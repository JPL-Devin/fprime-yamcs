package gov.jpl.nasa.fprime.keymgmt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds KEM Establishment packet payloads. The ML-KEM-768 ciphertext
 * (1088 bytes) exceeds typical TC frame limits, so it is fragmented:
 *
 * <pre>version(1) | masterKeyId(2) | fragIndex(1) | fragCount(1) | fragment</pre>
 *
 * The recipient reassembles fragments 0..fragCount-1 in order to recover
 * the full ciphertext, then decapsulates to obtain the shared secret.
 */
public final class KemPacketBuilder {

    /** Wire format version of the KEM Establishment packet payload. */
    public static final byte VERSION = 1;
    public static final int HEADER_LENGTH = 5;

    private KemPacketBuilder() {
    }

    /** Split the ciphertext into space-packet payloads of at most fragmentSize data bytes. */
    public static List<byte[]> buildFragments(int masterKeyId, byte[] ciphertext, int fragmentSize) {
        if (fragmentSize < 1) {
            throw new IllegalArgumentException("fragmentSize must be positive");
        }
        int fragCount = (ciphertext.length + fragmentSize - 1) / fragmentSize;
        if (fragCount > 255) {
            throw new IllegalArgumentException("Too many fragments: " + fragCount);
        }
        List<byte[]> fragments = new ArrayList<>(fragCount);
        for (int i = 0; i < fragCount; i++) {
            int offset = i * fragmentSize;
            int length = Math.min(fragmentSize, ciphertext.length - offset);
            ByteBuffer bb = ByteBuffer.allocate(HEADER_LENGTH + length);
            bb.put(VERSION);
            bb.putShort((short) masterKeyId);
            bb.put((byte) i);
            bb.put((byte) fragCount);
            bb.put(ciphertext, offset, length);
            fragments.add(bb.array());
        }
        return fragments;
    }
}
