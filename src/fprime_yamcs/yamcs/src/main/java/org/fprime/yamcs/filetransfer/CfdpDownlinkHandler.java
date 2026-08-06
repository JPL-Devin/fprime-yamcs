package org.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

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

    // In-flight transaction reassembly. Null means idle. Only touched from
    // the TM stream subscriber thread.
    private Reassembly inflight;

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
     */
    public CfdpDownlinkHandler(Bucket bucket, Path mirrorDir, int maxFileSize,
                               TransferResolver transferResolver,
                               TransferEventListener listener) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize must be positive");
        }
        this.bucket = bucket;
        this.mirrorDir = mirrorDir == null ? null : mirrorDir.normalize();
        this.maxFileSize = maxFileSize;
        this.transferResolver = transferResolver;
        this.listener = listener;
    }

    /** Process one CFDP PDU found at {@code offset} within {@code bytes}. */
    public void handlePdu(byte[] bytes, int offset) {
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
            LOG.error("Error processing CFDP PDU", e);
            failInflight("PDU processing error: " + e.getMessage());
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
                header.transactionSeq, md.fileSize, md.sourceFileName, md.destinationFileName);
        if (md.fileSize < 0 || md.fileSize > maxFileSize) {
            LOG.error("Metadata declares file size {} outside [0, {}]; ignoring",
                    md.fileSize, maxFileSize);
            return;
        }

        FprimeFileTransfer transfer = transferResolver.resolve(
                md.sourceFileName, md.destinationFileName, md.fileSize);
        transfer.setState(TransferState.RUNNING);
        inflight = new Reassembly(header.transactionSeq, md.destinationFileName,
                md.fileSize, transfer);
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
        inflight.bytesReceived += data.dataSize;

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
                    inflight.destinationFileName, e.getMessage());
            failInflight("unsafe destination path: " + e.getMessage());
            return;
        }
        try {
            bucket.putObjectAsync(objectName, "application/octet-stream",
                    Map.of(), inflight.buffer).join();
            LOG.info("CFDP downlink COMPLETE: {} ({} bytes) -> bucket {}",
                    objectName, inflight.bytesReceived, bucket.getName());

            mirrorToDirectory(objectName, inflight.buffer);

            inflight.transfer.setTransferredSize(inflight.bytesReceived);
            inflight.transfer.setState(TransferState.COMPLETED);
            listener.stateChanged(inflight.transfer);
            listener.verifierAck(inflight.transfer, AckStatus.OK,
                    String.format("delivered %d bytes to bucket %s/%s",
                            inflight.bytesReceived, bucket.getName(), objectName));
            inflight = null;
        } catch (Exception e) {
            LOG.error("Failed to store file in bucket", e);
            failInflight("bucket write failed: " + e.getMessage());
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
