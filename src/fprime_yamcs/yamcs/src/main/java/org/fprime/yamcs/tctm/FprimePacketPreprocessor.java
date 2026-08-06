package org.fprime.yamcs.tctm;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractPacketPreprocessor;

import org.fprime.yamcs.packet.SpacePacket;

/**
 * Packet preprocessor for F´ telemetry: verifies CCSDS sequence continuity
 * per APID and extracts the F´ time tag as the packet generation time.
 *
 * <p>Configured on a TM link, e.g.:
 *
 * <pre>
 * dataLinks:
 *   - name: udp-in
 *     class: org.yamcs.tctm.UdpTmDataLink
 *     stream: tm_realtime
 *     packetPreprocessorClassName: org.fprime.yamcs.tctm.FprimePacketPreprocessor
 * </pre>
 */
public class FprimePacketPreprocessor extends AbstractPacketPreprocessor {

    private final Map<Integer, AtomicInteger> seqCounts = new HashMap<>();

    // F´ type widths, from FpConfig / Fw::Time serialization.
    private static final int FwPacketDescriptorType_SIZE = 2;
    private static final int FwTlmPacketizeIdType_SIZE = 2;
    private static final int FwTimeBaseStoreType_SIZE = 2;
    private static final int FwTimeContextStoreType_SIZE = 1;
    private static final int FwEventIdType_SIZE = 4;

    private static final int TLM_TIME_TAG_OFFSET = SpacePacket.PRIMARY_HEADER_LEN
            + FwPacketDescriptorType_SIZE + FwTlmPacketizeIdType_SIZE
            + FwTimeBaseStoreType_SIZE + FwTimeContextStoreType_SIZE;

    private static final int EVENT_TIME_TAG_OFFSET = SpacePacket.PRIMARY_HEADER_LEN
            + FwPacketDescriptorType_SIZE + FwEventIdType_SIZE
            + FwTimeBaseStoreType_SIZE + FwTimeContextStoreType_SIZE;

    // APIDs
    private static final int APID_EVENT = 2; // default F´ APID for events
    private static final int APID_TLM_PKT = 4; // default F´ APID for telemetry packets

    // Constructor used when this preprocessor is used without YAML configuration
    public FprimePacketPreprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    // Constructor used when this preprocessor is used with YAML configuration
    // (packetPreprocessorClassArgs)
    public FprimePacketPreprocessor(String yamcsInstance, YConfiguration config) {
        super(yamcsInstance, config);
    }

    @Override
    public TmPacket process(TmPacket packet) {
        byte[] bytes = packet.getPacket();

        if (bytes.length < SpacePacket.PRIMARY_HEADER_LEN) {
            eventProducer.sendWarning("SHORT_PACKET",
                    "Short packet received, length: " + bytes.length
                            + "; minimum required length is "
                            + SpacePacket.PRIMARY_HEADER_LEN + " bytes.");
            // Returning null drops the packet.
            return null;
        }

        // Verify continuity for a given APID based on the CCSDS sequence counter
        int apidseqcount = SpacePacket.packetIdAndSequence(bytes);
        int apid = (apidseqcount >> 16) & 0x07FF;
        int seq = apidseqcount & 0x3FFF;
        AtomicInteger ai = seqCounts.computeIfAbsent(apid, k -> new AtomicInteger());
        int oldseq = ai.getAndSet(seq);

        if (((seq - oldseq) & 0x3FFF) != 1) {
            eventProducer.sendWarning("SEQ_COUNT_JUMP",
                    "Sequence count jump for APID: " + apid + " old seq: " + oldseq
                            + " newseq: " + seq);
        }

        int timeTagOffset = 0;
        // Find time tags depending on APID
        if (apid == APID_EVENT) {
            timeTagOffset = EVENT_TIME_TAG_OFFSET;
        } else if (apid == APID_TLM_PKT) {
            timeTagOffset = TLM_TIME_TAG_OFFSET;
        }
        // Leap second handling, see
        // https://docs.yamcs.org/yamcs-server-manual/general/time/
        int leapSecondsOffset = 38;
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int timeSec = bb.getInt(timeTagOffset) + leapSecondsOffset;
        int timeUsec = bb.getInt(timeTagOffset + 4); // seconds field is 4 bytes wide
        long packetGenerationTime = (timeSec * 1000L) + (timeUsec / 1000L);

        packet.setGenerationTime(packetGenerationTime);

        // Use the full 32 bits, so that both APID and the count are included.
        // Yamcs uses this attribute to uniquely identify the packet (together
        // with the generation time).
        packet.setSequenceCount(apidseqcount);

        return packet;
    }
}
