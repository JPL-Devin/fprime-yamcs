package org.fprime.yamcs.tctm;

import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.tctm.CcsdsSeqCountFiller;
import org.yamcs.tctm.CommandPostprocessor;
import org.yamcs.utils.ByteArrayUtils;

import org.fprime.yamcs.packet.SpacePacket;

/**
 * Command postprocessor for F´ telecommands: patches the CCSDS space packet
 * length and sequence count in the command binary before it reaches the link.
 *
 * <p>Configured on a TC link, e.g.:
 *
 * <pre>
 * dataLinks:
 *   - name: udp-out
 *     class: org.yamcs.tctm.UdpTcDataLink
 *     stream: tc_realtime
 *     commandPostprocessorClassName: org.fprime.yamcs.tctm.FprimeCommandPostprocessor
 * </pre>
 */
public class FprimeCommandPostprocessor implements CommandPostprocessor {

    private final CcsdsSeqCountFiller seqFiller = new CcsdsSeqCountFiller();
    private CommandHistoryPublisher commandHistory;

    // Constructor used when this postprocessor is used without YAML configuration
    public FprimeCommandPostprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    // Constructor used when this postprocessor is used with YAML configuration
    // (commandPostprocessorClassArgs)
    public FprimeCommandPostprocessor(String yamcsInstance, YConfiguration config) {
    }

    // Called by Yamcs during initialization
    @Override
    public void setCommandHistoryPublisher(CommandHistoryPublisher commandHistory) {
        this.commandHistory = commandHistory;
    }

    // Called by Yamcs *after* a command was submitted, but *before* the link
    // handles it. Must return the (possibly modified) packet binary.
    @Override
    public byte[] process(PreparedCommand pc) {
        byte[] binary = pc.getBinary();
        if (binary == null || binary.length < SpacePacket.PRIMARY_HEADER_LEN + 1) {
            commandHistory.publishAck(pc.getCommandId(),
                    CommandHistoryPublisher.AcknowledgeSent_KEY,
                    System.currentTimeMillis(),
                    CommandHistoryPublisher.AckStatus.NOK,
                    "Command binary shorter than a CCSDS space packet");
            return null; // drop the command
        }
        if (binary.length > SpacePacket.PRIMARY_HEADER_LEN + SpacePacket.MAX_PAYLOAD_LEN) {
            commandHistory.publishAck(pc.getCommandId(),
                    CommandHistoryPublisher.AcknowledgeSent_KEY,
                    System.currentTimeMillis(),
                    CommandHistoryPublisher.AckStatus.NOK,
                    "Command binary exceeds the CCSDS maximum packet length");
            return null; // drop the command
        }

        // Set the CCSDS packet data length field (minus one per the SPP protocol)
        ByteArrayUtils.encodeUnsignedShort(
                binary.length - SpacePacket.PRIMARY_HEADER_LEN - 1, binary,
                SpacePacket.LENGTH_FIELD_OFFSET);

        // Set CCSDS sequence count
        int seqCount = seqFiller.fill(binary);

        // Publish the sequence count to Command History. This has no special
        // meaning to Yamcs, but shows how to store custom per-command data.
        commandHistory.publish(pc.getCommandId(), "ccsds-seqcount", seqCount);

        // Since the binary was modified, update it in Command History too.
        commandHistory.publish(pc.getCommandId(), PreparedCommand.CNAME_BINARY, binary);

        return binary;
    }
}
