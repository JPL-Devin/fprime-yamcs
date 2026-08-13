package gov.jpl.nasa.fprime.keymgmt;

import java.nio.ByteBuffer;

/** CCSDS Space Packet construction helpers. */
public final class SpacePackets {

    public static final int PRIMARY_HEADER_LENGTH = 6;
    /** Max payload such that the 16-bit length field (len-1) does not overflow. */
    public static final int MAX_PAYLOAD_LENGTH = 65536;

    private SpacePackets() {
    }

    /** Build a TC space packet (version 0, no secondary header, standalone). */
    public static byte[] buildTcPacket(int apid, int seqCount, byte[] payload) {
        if (payload.length == 0 || payload.length > MAX_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("Invalid payload length: " + payload.length);
        }
        ByteBuffer bb = ByteBuffer.allocate(PRIMARY_HEADER_LENGTH + payload.length);
        // Word 0: 3b version(0) | 1b type(1 = TC) | 1b secHdr(0) | 11b APID
        bb.putShort((short) ((1 << 12) | (apid & 0x07FF)));
        // Word 1: 2b seqFlags (0b11 = standalone) | 14b seqCount
        bb.putShort((short) ((0b11 << 14) | (seqCount & 0x3FFF)));
        // Word 2: 16b data length (number of payload bytes minus one)
        bb.putShort((short) (payload.length - 1));
        bb.put(payload);
        return bb.array();
    }
}
