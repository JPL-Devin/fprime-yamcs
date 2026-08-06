package org.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.protobuf.TransferState;

import org.fprime.yamcs.packet.CfdpChecksum;
import org.fprime.yamcs.packet.CfdpPdu;

/**
 * Reassembles class-1 CFDP downlink transactions (Metadata / File Data×N /
 * EOF) into complete files, validates the CFDP modular checksum, and stores
 * the result in a YAMCS bucket (optionally mirroring to a local directory).
 *
 * <p>Supports one in-flight transaction at a time. An EOF with a non-zero
 * condition code cancels the in-flight transaction.
 */
public class CfdpDownlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CfdpDownlinkHandler.class);

    /**
     * Maximum completed reassemblies awaiting storage. Bounds the memory a
     * fast (or spoofed) TM stream can pin in queued reassembly buffers when
     * bucket writes are slower than downlinks complete.
     */
    static final int MAX_PENDING_STORES = 4;

    /** Supplies the API-level transfer record for a new downlink transaction. */
    public interface TransferResolver {
        FprimeFileTransfer resolve(String sourceFileName, String destinationFileName,
                                   int fileSize);
    }

    private final Bucket bucket;
    private final Path mirrorDir;
    private final int maxFileSize;
    private final TransferResolver transferResolver;
    private final TransferEventListener listener;
    private final Executor storageExecutor;

    // In-flight transaction reassembly. Null means idle. Guarded by the
    // handler's monitor (all packet entry points are synchronized).
    private Reassembly inflight;
    private long inflightLastActivity;
    private final AtomicInteger pendingStores = new AtomicInteger();

    private static final class Reassembly {
        final int transactionSeq;
        final String destinationFileName;
        final byte[] buffer;
        final int declaredSize;
        int bytesReceived;
        final FprimeFileTransfer transfer;

        Reassembly(int transactionSeq, String dst, int size, FprimeFileTransfer transfer) {
            this.transactionSeq = transactionSeq;
            this.destinationFileName = dst;
            this.declaredSize = size;
            this.buffer = new byte[size];
            this.transfer = transfer;
        }
    }

    /**
     * @param bucket           destination bucket for reassembled files
     * @param mirrorDir        optional local mirror directory (null disables)
     * @param maxFileSize      largest Metadata-declared file size accepted,
     *                         in bytes; bounds the reassembly buffer
     * @param transferResolver supplies the API transfer record for a Metadata
     * @param listener         receives transfer state changes
     * @param storageExecutor  executor on which the completed file is written
     *                         to the bucket/mirror, keeping blocking storage
     *                         I/O off the TM stream subscriber thread
     */
    public CfdpDownlinkHandler(Bucket bucket, Path mirrorDir, int maxFileSize,
                               TransferResolver transferResolver,
                               TransferEventListener listener,
                               Executor storageExecutor) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize must be positive");
        }
        this.bucket = bucket;
        this.mirrorDir = mirrorDir == null ? null : mirrorDir.normalize();
        this.maxFileSize = maxFileSize;
        this.transferResolver = transferResolver;
        this.listener = listener;
        this.storageExecutor = storageExecutor;
    }

    /** Process one CFDP PDU found at {@code offset} within {@code bytes}. */
    public synchronized void handlePdu(byte[] bytes, int offset) {
        try {
            CfdpPdu.Header header = CfdpPdu.decodeHeader(bytes, offset);
            if (header.type == CfdpPdu.Type.FILE_DATA) {
                handleFileData(bytes, header);
                return;
            }
            int directive = CfdpPdu.directiveCode(bytes, header);
            switch (directive) {
                case CfdpPdu.DIRECTIVE_METADATA:
                    handleMetadata(bytes, header);
                    break;
                case CfdpPdu.DIRECTIVE_EOF:
                    handleEof(bytes, header);
                    break;
                default:
                    LOG.warn("Ignoring CFDP directive 0x{} on tx {}",
                            Integer.toHexString(directive), header.transactionSeq);
            }
        } catch (Exception e) {
            // Drop undecodable PDUs rather than failing the in-flight
            // transaction: garbage on the CFDP APID must not abort an
            // unrelated healthy transfer. A dead transaction is reclaimed
            // by the expireInflight sweeper.
            LOG.error("Dropping undecodable CFDP PDU: {}", e.getMessage());
        }
    }

    /**
     * Fail the in-flight transaction if no PDU for it has arrived within
     * {@code maxAgeMs}. Called periodically by the owning service so a
     * Metadata never followed by EOF cannot pin its reassembly buffer
     * forever.
     */
    public synchronized void expireInflight(long maxAgeMs) {
        if (inflight != null && System.currentTimeMillis() - inflightLastActivity > maxAgeMs) {
            LOG.warn("CFDP transaction {} stalled for over {} ms; failing",
                    inflight.transactionSeq, maxAgeMs);
            failInflight("transaction stalled: no PDU received within " + maxAgeMs + " ms");
        }
    }

    private void handleMetadata(byte[] bytes, CfdpPdu.Header header) {
        if (inflight != null) {
            LOG.warn("Got Metadata while transaction {} in progress; dropping previous",
                    inflight.transactionSeq);
            failInflight("superseded by new Metadata");
        }
        CfdpPdu.Metadata md = CfdpPdu.decodeMetadata(bytes, header);
        LOG.info("CFDP downlink Metadata: tx={} size={} src={} dst={}",
                header.transactionSeq, md.fileSize,
                ObjectNames.forLog(md.sourceFileName), ObjectNames.forLog(md.destinationFileName));
        if (md.fileSize < 0 || md.fileSize > maxFileSize) {
            LOG.error("Metadata declares file size {} outside [0, {}]; rejecting",
                    md.fileSize, maxFileSize);
            // Resolve and fail immediately so a pending startDownload()
            // transfer gets the real rejection reason instead of waiting
            // for the timeout sweeper.
            FprimeFileTransfer rejected = transferResolver.resolve(
                    md.sourceFileName, md.destinationFileName, 0);
            String reason = String.format(
                    "Metadata declares file size %d outside [0, %d]",
                    md.fileSize, maxFileSize);
            rejected.setFailureReason(reason);
            rejected.setState(TransferState.FAILED);
            listener.stateChanged(rejected);
            listener.verifierAck(rejected, AckStatus.NOK, reason);
            return;
        }

        FprimeFileTransfer transfer = transferResolver.resolve(
                md.sourceFileName, md.destinationFileName, md.fileSize);
        transfer.setState(TransferState.RUNNING);
        inflight = new Reassembly(header.transactionSeq, md.destinationFileName,
                md.fileSize, transfer);
        inflightLastActivity = System.currentTimeMillis();
        listener.stateChanged(transfer);
        listener.verifierAck(transfer, AckStatus.PENDING,
                String.format("receiving %d bytes from %s", md.fileSize, md.sourceFileName));
    }

    private void handleFileData(byte[] bytes, CfdpPdu.Header header) {
        if (inflight == null) {
            LOG.warn("Got File Data with no in-flight transaction; dropping");
            return;
        }
        if (header.transactionSeq != inflight.transactionSeq) {
            LOG.warn("Got File Data for tx {} while reassembling tx {}; dropping",
                    header.transactionSeq, inflight.transactionSeq);
            return;
        }
        CfdpPdu.FileData data = CfdpPdu.decodeFileData(bytes, header);
        // Long arithmetic: offset is wire-controlled and int addition could
        // wrap negative and slip past the comparison.
        if (data.offset < 0 || (long) data.offset + data.dataSize > inflight.declaredSize) {
            LOG.error("File Data would overflow file: offset={} dataSize={} declared={}",
                    data.offset, data.dataSize, inflight.declaredSize);
            failInflight("overflow in File Data PDU");
            return;
        }
        System.arraycopy(bytes, data.dataStart, inflight.buffer, data.offset, data.dataSize);
        inflightLastActivity = System.currentTimeMillis();
        // Clamp: duplicate or overlapping File Data offsets must not inflate
        // the reported progress past the declared file size.
        inflight.bytesReceived = (int) Math.min((long) inflight.declaredSize,
                (long) inflight.bytesReceived + data.dataSize);

        inflight.transfer.setTransferredSize(inflight.bytesReceived);
        listener.stateChanged(inflight.transfer);
    }

    private void handleEof(byte[] bytes, CfdpPdu.Header header) {
        if (inflight == null) {
            LOG.warn("Got EOF with no in-flight transaction");
            return;
        }
        if (header.transactionSeq != inflight.transactionSeq) {
            LOG.warn("Got EOF for tx {} while reassembling tx {}; dropping",
                    header.transactionSeq, inflight.transactionSeq);
            return;
        }
        CfdpPdu.Eof eof = CfdpPdu.decodeEof(bytes, header);
        if (eof.conditionCode != CfdpPdu.CONDITION_NO_ERROR) {
            LOG.warn("CFDP transaction {} ended with condition code {}",
                    header.transactionSeq, eof.conditionCode);
            failInflight("transaction cancelled: condition code " + eof.conditionCode);
            return;
        }
        if (eof.fileSize != inflight.declaredSize) {
            failInflight(String.format("EOF file size %d != Metadata file size %d",
                    eof.fileSize, inflight.declaredSize));
            return;
        }
        int computed = CfdpChecksum.of(inflight.buffer);
        if (computed != eof.checksum) {
            failInflight(String.format("checksum mismatch: expected 0x%08x got 0x%08x",
                    eof.checksum, computed));
            return;
        }

        String objectName;
        try {
            objectName = ObjectNames.sanitize(inflight.destinationFileName);
        } catch (IllegalArgumentException e) {
            LOG.error("Rejecting unsafe destination file name '{}': {}",
                    ObjectNames.forLog(inflight.destinationFileName), e.getMessage());
            failInflight("unsafe destination path: " + e.getMessage());
            return;
        }
        // Hand storage off so blocking bucket/mirror I/O never stalls the TM
        // stream subscriber thread. Clear the in-flight slot first: the
        // reassembly is complete and the next Metadata may arrive immediately.
        // Bound the storage backlog so a fast TM stream cannot pin unbounded
        // memory in queued reassembly buffers.
        if (pendingStores.get() >= MAX_PENDING_STORES) {
            failInflight("storage backlog: " + MAX_PENDING_STORES
                    + " completed transfers already awaiting bucket writes");
            return;
        }
        Reassembly completed = inflight;
        inflight = null;
        pendingStores.incrementAndGet();
        try {
            storageExecutor.execute(() -> {
                try {
                    store(completed, objectName);
                } finally {
                    pendingStores.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            pendingStores.decrementAndGet();
            String reason = "storage executor rejected write: " + e.getMessage();
            completed.transfer.setFailureReason(reason);
            completed.transfer.setState(TransferState.FAILED);
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.NOK, reason);
        }
    }

    private void store(Reassembly completed, String objectName) {
        try {
            bucket.putObjectAsync(objectName, "application/octet-stream",
                    Map.of(), completed.buffer).join();
            LOG.info("CFDP downlink COMPLETE: {} ({} bytes) -> bucket {}",
                    objectName, completed.bytesReceived, bucket.getName());

            mirrorToDirectory(objectName, completed.buffer);

            completed.transfer.setTransferredSize(completed.bytesReceived);
            completed.transfer.setState(TransferState.COMPLETED);
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.OK,
                    String.format("delivered %d bytes to bucket %s/%s",
                            completed.bytesReceived, bucket.getName(), objectName));
        } catch (Exception e) {
            LOG.error("Failed to store file in bucket", e);
            String reason = "bucket write failed: " + e.getMessage();
            completed.transfer.setFailureReason(reason);
            completed.transfer.setState(TransferState.FAILED);
            listener.stateChanged(completed.transfer);
            listener.verifierAck(completed.transfer, AckStatus.NOK, reason);
        }
    }

    private void mirrorToDirectory(String objectName, byte[] content) {
        if (mirrorDir == null) {
            return;
        }
        try {
            Path mirrorPath = mirrorDir.resolve(objectName).normalize();
            if (!mirrorPath.startsWith(mirrorDir)) {
                LOG.warn("Refusing to mirror {} outside {}", objectName, mirrorDir);
                return;
            }
            Files.createDirectories(mirrorPath.getParent());
            Files.write(mirrorPath, content);
            LOG.info("Mirrored downlink file to {}", mirrorPath);
        } catch (IOException e) {
            LOG.warn("Failed to mirror file to {}: {}", mirrorDir, e.getMessage());
        }
    }

    private void failInflight(String reason) {
        if (inflight == null) {
            return;
        }
        inflight.transfer.setFailureReason(reason);
        inflight.transfer.setState(TransferState.FAILED);
        listener.stateChanged(inflight.transfer);
        listener.verifierAck(inflight.transfer, AckStatus.NOK, reason);
        inflight = null;
    }
}
