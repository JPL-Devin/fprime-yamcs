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
import org.fprime.yamcs.packet.FilePacket;

/**
 * Reassembles {@code Fw::FilePacket} downlink sequences
 * (START / DATA×N / END, or CANCEL) into complete files, validates the CFDP
 * modular checksum, and stores the result in a YAMCS bucket (optionally
 * mirroring to a local directory).
 *
 * <p>Supports one in-flight transfer at a time, matching the F´
 * {@code Svc::FileDownlink} protocol which is strictly sequential.
 */
public class FilePacketDownlinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePacketDownlinkHandler.class);

    /**
     * Supplies the API-level transfer record to attach to a newly started
     * downlink — either a pending {@code startDownload()} transfer keyed by
     * destination path, or a freshly created record for unsolicited
     * downlinks.
     */
    public interface TransferResolver {
        FprimeFileTransfer resolve(String sourcePath, String destinationPath, int fileSize);
    }

    private final Bucket bucket;
    private final Path mirrorDir;
    private final TransferResolver transferResolver;
    private final TransferEventListener listener;
    private final int maxFileSize;

    // In-flight downlink reassembly. Null means idle. Only touched from the
    // TM stream subscriber thread.
    private Reassembly inflight;

    private static final class Reassembly {
        final String sourcePath;
        final String destinationPath;
        final byte[] buffer;
        final int declaredSize;
        int bytesReceived;
        final FprimeFileTransfer transfer;

        Reassembly(String src, String dst, int size, FprimeFileTransfer transfer) {
            this.sourcePath = src;
            this.destinationPath = dst;
            this.declaredSize = size;
            this.buffer = new byte[size];
            this.transfer = transfer;
        }
    }

    /**
     * @param bucket           destination bucket for reassembled files
     * @param mirrorDir        optional local mirror directory (null disables)
     * @param maxFileSize      largest START-declared file size accepted, in
     *                         bytes; bounds the reassembly buffer allocation
     *                         so a corrupt/malicious START cannot exhaust
     *                         ground-server memory
     * @param transferResolver supplies the API transfer record for a START
     * @param listener         receives transfer state changes
     */
    public FilePacketDownlinkHandler(Bucket bucket, Path mirrorDir, int maxFileSize,
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

    /**
     * Process one descriptor-prefixed {@code Fw::FilePacket} found at
     * {@code offset} within {@code bytes}.
     */
    public void handleFilePacket(byte[] bytes, int offset) {
        try {
            FilePacket.Header header = FilePacket.decodeHeader(bytes, offset);
            if (header.type == null) {
                LOG.warn("Unknown FilePacket type {} at seq {}",
                        header.rawType, header.sequenceIndex);
                return;
            }
            switch (header.type) {
                case START:
                    handleStart(bytes, header);
                    break;
                case DATA:
                    handleData(bytes, header);
                    break;
                case END:
                    handleEnd(bytes, header);
                    break;
                case CANCEL:
                    handleCancel(header);
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error processing FilePacket", e);
            failInflight("packet processing error: " + e.getMessage());
        }
    }

    private void handleStart(byte[] bytes, FilePacket.Header header) {
        if (inflight != null) {
            LOG.warn("Got START while transfer in progress; dropping previous");
            failInflight("superseded by new START");
        }
        FilePacket.StartPayload start = FilePacket.decodeStart(bytes, header.payloadOffset);
        LOG.info("File transfer START: seq={} size={} src={} dst={}",
                header.sequenceIndex, start.fileSize, start.sourcePath, start.destinationPath);
        if (start.fileSize < 0 || start.fileSize > maxFileSize) {
            LOG.error("START declares file size {} outside [0, {}]; ignoring",
                    start.fileSize, maxFileSize);
            return;
        }

        FprimeFileTransfer transfer = transferResolver.resolve(
                start.sourcePath, start.destinationPath, start.fileSize);
        transfer.setState(TransferState.RUNNING);
        inflight = new Reassembly(start.sourcePath, start.destinationPath, start.fileSize, transfer);
        listener.stateChanged(transfer);
        listener.verifierAck(transfer, AckStatus.PENDING,
                String.format("receiving %d bytes from %s", start.fileSize, start.sourcePath));
    }

    private void handleData(byte[] bytes, FilePacket.Header header) {
        if (inflight == null) {
            LOG.warn("Got DATA seq={} with no in-flight transfer; dropping", header.sequenceIndex);
            return;
        }
        FilePacket.DataPayload data = FilePacket.decodeData(bytes, header.payloadOffset);
        // Long arithmetic: byteOffset is wire-controlled and int addition
        // could wrap negative and slip past the comparison.
        if (data.byteOffset < 0
                || (long) data.byteOffset + data.dataSize > inflight.declaredSize) {
            LOG.error("DATA packet would overflow file: byteOffset={} dataSize={} declared={}",
                    data.byteOffset, data.dataSize, inflight.declaredSize);
            failInflight("overflow in DATA packet");
            return;
        }
        System.arraycopy(bytes, data.dataStart, inflight.buffer, data.byteOffset, data.dataSize);
        inflight.bytesReceived += data.dataSize;

        inflight.transfer.setTransferredSize(inflight.bytesReceived);
        listener.stateChanged(inflight.transfer);
    }

    private void handleEnd(byte[] bytes, FilePacket.Header header) {
        if (inflight == null) {
            LOG.warn("Got END seq={} with no in-flight transfer", header.sequenceIndex);
            return;
        }
        int receivedChecksum = FilePacket.decodeEndChecksum(bytes, header.payloadOffset);
        int computed = CfdpChecksum.of(inflight.buffer);
        if (computed != receivedChecksum) {
            LOG.error("Checksum mismatch on transfer {}: received=0x{} computed=0x{}",
                    inflight.destinationPath,
                    Integer.toHexString(receivedChecksum),
                    Integer.toHexString(computed));
            failInflight(String.format(
                    "checksum mismatch: expected 0x%08x got 0x%08x",
                    receivedChecksum, computed));
            return;
        }

        String objectName;
        try {
            objectName = sanitizeObjectName(inflight.destinationPath);
        } catch (IllegalArgumentException e) {
            LOG.error("Rejecting unsafe destination path '{}': {}",
                    inflight.destinationPath, e.getMessage());
            failInflight("unsafe destination path: " + e.getMessage());
            return;
        }
        try {
            // putObjectAsync returns a CompletableFuture<Void>; block on it so
            // a single COMPLETE/FAILED line is logged per transfer instead of
            // racing the next packet on the stream.
            bucket.putObjectAsync(objectName, "application/octet-stream",
                    Map.of(), inflight.buffer).join();
            LOG.info("File transfer COMPLETE: {} ({} bytes) -> bucket {}",
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

    private void handleCancel(FilePacket.Header header) {
        if (inflight != null) {
            LOG.warn("File transfer CANCELLED at seq {} (was: {})",
                    header.sequenceIndex, inflight.destinationPath);
            failInflight("cancelled by spacecraft");
        } else {
            LOG.warn("Got CANCEL seq={} with no in-flight transfer", header.sequenceIndex);
        }
    }

    /**
     * Turn the wire-supplied destination path into a bucket object key:
     * strip the leading '/', and reject empty or '..'-bearing paths so a
     * corrupt or malicious START cannot address objects outside the bucket
     * namespace (or, via the mirror, outside the mirror directory).
     */
    static String sanitizeObjectName(String destinationPath) {
        String name = destinationPath.startsWith("/")
                ? destinationPath.substring(1)
                : destinationPath;
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty destination path");
        }
        for (String segment : name.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(
                        "path contains empty, '.' or '..' segment: " + destinationPath);
            }
        }
        return name;
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

    /**
     * Mark the in-flight downlink as failed, push the failure to its API
     * transfer, and clear the in-flight state. Used by every error path in
     * the downlink state machine.
     */
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
