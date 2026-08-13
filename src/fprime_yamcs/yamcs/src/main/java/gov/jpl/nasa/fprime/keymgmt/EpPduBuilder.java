package gov.jpl.nasa.fprime.keymgmt;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Builds SDLS Extended Procedures PDUs per CCSDS 355.1-B-1, using the
 * Annex D "Baseline Implementation Mode" field sizes:
 *
 * <ul>
 *   <li>OTAR (tag 0x01): MasterKeyID(16b) | IV(96b) | {EncKeyID(16b),
 *       EncKey(256b)}xN | MAC(128b), AES-256-GCM under the master key (KEK)
 *   <li>Key Activation (tag 0x02): KeyID(16b)xN
 *   <li>Key Verification (tag 0x04): {KeyID(16b), Challenge(128b)}xN
 * </ul>
 *
 * <p>PDU header: 1 byte tag (type flag 0=command, user flag 0=CCSDS,
 * service group 00=key management, 4-bit procedure ID) followed by a
 * 16-bit length field expressed in bits (5.3.2.3).
 */
public final class EpPduBuilder {

    public static final int TAG_OTAR = 0x01;
    public static final int TAG_KEY_ACTIVATION = 0x02;
    public static final int TAG_KEY_DEACTIVATION = 0x03;
    public static final int TAG_KEY_VERIFICATION = 0x04;

    public static final int IV_LENGTH = 12;
    public static final int MAC_LENGTH = 16;
    public static final int CHALLENGE_LENGTH = 16;
    public static final int SESSION_KEY_LENGTH = 32;

    private static final SecureRandom RANDOM = new SecureRandom();

    private EpPduBuilder() {
    }

    /**
     * Build an OTAR Command PDU uploading one AES-256 session key,
     * authenticated-encrypted under the KEK with AES-256-GCM.
     *
     * @param masterKeyId  key ID of the master key (KEK) protecting the upload
     * @param kek          32-byte master key
     * @param sessionKeyId key ID to assign to the uploaded session key
     * @param sessionKey   32-byte AES-256 session key to upload
     */
    public static byte[] buildOtar(int masterKeyId, byte[] kek, int sessionKeyId, byte[] sessionKey)
            throws GeneralSecurityException {
        if (kek.length != SESSION_KEY_LENGTH) {
            throw new IllegalArgumentException("KEK must be 32 bytes, got " + kek.length);
        }
        if (sessionKey.length != SESSION_KEY_LENGTH) {
            throw new IllegalArgumentException("Session key must be 32 bytes, got " + sessionKey.length);
        }
        byte[] iv = new byte[IV_LENGTH];
        RANDOM.nextBytes(iv);

        // Upload Key Block plaintext: Key ID (16 bits) + key (256 bits)
        ByteBuffer plaintext = ByteBuffer.allocate(2 + SESSION_KEY_LENGTH);
        plaintext.putShort((short) sessionKeyId);
        plaintext.put(sessionKey);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(kek, "AES"),
                new GCMParameterSpec(MAC_LENGTH * 8, iv));
        // GCM output = ciphertext || tag; the tag is the PDU MAC field
        byte[] ctAndTag = cipher.doFinal(plaintext.array());

        ByteBuffer data = ByteBuffer.allocate(2 + IV_LENGTH + ctAndTag.length);
        data.putShort((short) masterKeyId);
        data.put(iv);
        data.put(ctAndTag);
        return withHeader(TAG_OTAR, data.array());
    }

    /** Build a Key Activation Command PDU for the given session key IDs. */
    public static byte[] buildKeyActivation(int... keyIds) {
        ByteBuffer data = ByteBuffer.allocate(2 * keyIds.length);
        for (int keyId : keyIds) {
            data.putShort((short) keyId);
        }
        return withHeader(TAG_KEY_ACTIVATION, data.array());
    }

    /**
     * Build a Key Verification Command PDU with a random 128-bit challenge.
     *
     * @return the PDU; the challenge is returned via {@code challengeOut}
     *         (16 bytes) so the caller can check the eventual reply
     */
    public static byte[] buildKeyVerification(int keyId, byte[] challengeOut) {
        if (challengeOut.length != CHALLENGE_LENGTH) {
            throw new IllegalArgumentException("Challenge buffer must be 16 bytes");
        }
        RANDOM.nextBytes(challengeOut);
        ByteBuffer data = ByteBuffer.allocate(2 + CHALLENGE_LENGTH);
        data.putShort((short) keyId);
        data.put(challengeOut);
        return withHeader(TAG_KEY_VERIFICATION, data.array());
    }

    /** Prepend the EP PDU header: tag byte + data field length in bits. */
    private static byte[] withHeader(int tag, byte[] data) {
        ByteBuffer pdu = ByteBuffer.allocate(3 + data.length);
        pdu.put((byte) tag);
        pdu.putShort((short) (data.length * 8));
        pdu.put(data);
        return pdu.array();
    }
}
