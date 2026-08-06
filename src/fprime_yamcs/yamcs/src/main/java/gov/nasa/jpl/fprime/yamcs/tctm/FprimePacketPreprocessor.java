package gov.nasa.jpl.fprime.yamcs.tctm;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractPacketPreprocessor;
import org.yamcs.utils.TimeEncoding;

import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

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
 *     packetPreprocessorClassName: gov.nasa.jpl.fprime.yamcs.tctm.FprimePacketPreprocessor
 * </pre>
 */
public class FprimePacketPreprocessor extends AbstractPacketPreprocessor {

    // ConcurrentHashMap: links normally invoke process() from a single
    // thread, but nothing enforces that, and the map is cheap to harden.
    private final Map<Integer, AtomicInteger> seqCounts = new ConcurrentHashMap<>();

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

    // Impossible 14-bit sequence value marking a freshly seeded counter.
    private static final int FIRST_PACKET_SENTINEL = -1;

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
        // computeIfAbsent makes the first-packet seed atomic; the sentinel
        // marks it so link start does not raise a spurious jump.
        AtomicInteger ai = seqCounts.computeIfAbsent(apid,
                k -> new AtomicInteger(FIRST_PACKET_SENTINEL));
        int oldseq = ai.getAndSet(seq);
        if (oldseq != FIRST_PACKET_SENTINEL && ((seq - oldseq) & 0x3FFF) != 1) {
            eventProducer.sendWarning("SEQ_COUNT_JUMP",
                    "Sequence count jump for APID: " + apid + " old seq: " + oldseq
                            + " newseq: " + seq);
        }

        // Find time tags depending on APID. APIDs without a known F´ time
        // tag layout (file packets, unknown packets) get local reception
        // time instead of misparsing arbitrary payload bytes as a time tag.
        int timeTagOffset = -1;
        if (apid == APID_EVENT) {
            timeTagOffset = EVENT_TIME_TAG_OFFSET;
        } else if (apid == APID_TLM_PKT) {
            timeTagOffset = TLM_TIME_TAG_OFFSET;
        }
        if (timeTagOffset >= 0 && bytes.length >= timeTagOffset + 8) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            // The F´ seconds field is an unsigned U32; mask so values past
            // 2038-01-19 do not wrap negative.
            long timeSec = bb.getInt(timeTagOffset) & 0xFFFFFFFFL;
            long timeUsec = bb.getInt(timeTagOffset + 4) & 0xFFFFFFFFL;
            // F´ time tags are Unix (UTC) epoch seconds; TimeEncoding applies
            // YAMCS's maintained TAI-UTC leap-second table.
            packet.setGenerationTime(TimeEncoding.fromUnixMillisec(
                    (timeSec * 1000L) + (timeUsec / 1000L)));
        } else {
            if (timeTagOffset >= 0) {
                eventProducer.sendWarning("SHORT_PACKET",
                        "Packet on APID " + apid + " too short for time tag (length "
                                + bytes.length + "); using local time");
            }
            packet.setGenerationTime(TimeEncoding.getWallclockTime());
        }

        // Use the full 32 bits, so that both APID and the count are included.
        // Yamcs uses this attribute to uniquely identify the packet (together
        // with the generation time).
        packet.setSequenceCount(apidseqcount);

        return packet;
    }
}
