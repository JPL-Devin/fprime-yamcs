package org.fprime.yamcs.filetransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.protobuf.TransferState;

import org.fprime.yamcs.packet.CfdpChecksum;
import org.fprime.yamcs.packet.FilePacket;
import org.fprime.yamcs.packet.SpacePacket;

/**
 * Uplinks a file as an {@code Fw::FilePacket} START / DATA×N / END sequence,
 * wrapping each packet in a CCSDS space packet on the file APID and handing
 * it to an {@link UplinkTransport}.
 */
public class FilePacketUplinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilePacketUplinkHandler.class);

    private final UplinkTransport transport;
    private final int fileApid;
    private final int chunkSize;
    private final TransferEventListener listener;

    public FilePacketUplinkHandler(UplinkTransport transport, int fileApid, int chunkSize,
                                   TransferEventListener listener) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        this.transport = transport;
        this.fileApid = fileApid;
        this.chunkSize = chunkSize;
        this.listener = listener;
    }

    /**
     * Run the uplink to completion, updating the transfer record as it
     * progresses. Intended to run on a dedicated executor.
     */
    public void run(FprimeFileTransfer transfer, byte[] content) {
        try {
            transfer.setStartTime(System.currentTimeMillis());
            LOG.info("Uplink START: id={} bucket={} object={} -> {} ({} bytes)",
                    transfer.getId(), transfer.getBucketName(), transfer.getObjectName(),
                    transfer.getRemotePath(), content.length);

            int seq = 0;
            send(FilePacket.encodeStart(seq, content.length,
                    transfer.getObjectName(), transfer.getRemotePath()));

            // DATA×N — update transferredSize after each chunk so the UI
            // progress bar animates.
            for (int offset = 0; offset < content.length; offset += chunkSize) {
                int len = Math.min(chunkSize, content.length - offset);
                seq++;
                send(FilePacket.encodeData(seq, offset, content, offset, len));
                transfer.setTransferredSize(offset + len);
                listener.stateChanged(transfer);
            }

            seq++;
            send(FilePacket.encodeEnd(seq, CfdpChecksum.of(content)));

            transfer.setTransferredSize(content.length);
            transfer.setState(TransferState.COMPLETED);
            LOG.info("Uplink COMPLETE: id={} object={} ({} bytes)",
                    transfer.getId(), transfer.getObjectName(), content.length);
        } catch (Exception e) {
            LOG.error("Uplink FAILED: id={} object={}",
                    transfer.getId(), transfer.getObjectName(), e);
            transfer.setFailureReason(e.getMessage() != null ? e.getMessage() : e.toString());
            transfer.setState(TransferState.FAILED);
        } finally {
            listener.stateChanged(transfer);
        }
    }

    private void send(byte[] filePacket) throws Exception {
        // The sequence count field is patched by the link's command
        // postprocessor; zero is a placeholder.
        transport.send(SpacePacket.wrapTelecommand(filePacket, fileApid, 0));
    }
}
