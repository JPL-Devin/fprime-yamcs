package org.fprime.yamcs.packet;

import java.nio.ByteBuffer;

/**
 * CCSDS Space Packet (CCSDS 133.0-B) primary header helpers shared by the
 * F´ TM/TC processing and file transfer code.
 */
public final class SpacePacket {

    /** CCSDS Space Packet primary header length in bytes. */
    public static final int PRIMARY_HEADER_LEN = 6;

    /** Maximum packet data field length (16-bit length field, minus-one encoding). */
    public static final int MAX_PAYLOAD_LEN = 65536;

    /** Offset of the 16-bit packet data length field in the primary header. */
    public static final int LENGTH_FIELD_OFFSET = 4;

    private SpacePacket() {
    }

    /** Extract the 11-bit APID from a raw space packet. */
    public static int apid(byte[] packet) {
        return (((packet[0] & 0xFF) << 8) | (packet[1] & 0xFF)) & 0x07FF;
    }

    /** Extract the combined 32-bit packet-id + sequence-control words. */
    public static int packetIdAndSequence(byte[] packet) {
        return ByteBuffer.wrap(packet).getInt(0);
    }

    /**
     * Wrap a payload in a CCSDS space packet with the telecommand type flag
     * set, no secondary header, and standalone sequence flags.
     *
     * @param payload  packet data field content, 1 to {@link #MAX_PAYLOAD_LEN} bytes
     * @param apid     11-bit application process identifier
     * @param seqCount 14-bit sequence count
     * @throws IllegalArgumentException if the payload cannot be represented
     *         in the 16-bit length field
     */
    public static byte[] wrapTelecommand(byte[] payload, int apid, int seqCount) {
        if (payload.length < 1 || payload.length > MAX_PAYLOAD_LEN) {
            throw new IllegalArgumentException(
                    "payload length " + payload.length + " outside [1, " + MAX_PAYLOAD_LEN + "]");
        }
        ByteBuffer bb = ByteBuffer.allocate(PRIMARY_HEADER_LEN + payload.length);
        // Word 0: 3b version(0) | 1b type(1 = TC) | 1b secHdr(0) | 11b APID
        int packetId = (1 << 12) | (apid & 0x07FF);
        bb.putShort((short) packetId);
        // Word 1: 2b seqFlags (0b11 = standalone) | 14b seqCount
        int seqCtrl = (0b11 << 14) | (seqCount & 0x3FFF);
        bb.putShort((short) seqCtrl);
        // Word 2: 16b data length, minus one per the SPP convention
        bb.putShort((short) (payload.length - 1));
        bb.put(payload);
        return bb.array();
    }
}
