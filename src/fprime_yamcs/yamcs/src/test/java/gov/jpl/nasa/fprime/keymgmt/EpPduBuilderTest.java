package gov.jpl.nasa.fprime.keymgmt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;

public class EpPduBuilderTest {

    private static final SecureRandom RANDOM = new SecureRandom();

    @Test
    public void otarPduMatchesBaselineImplementationMode() throws Exception {
        byte[] kek = new byte[32];
        byte[] sessionKey = new byte[32];
        RANDOM.nextBytes(kek);
        RANDOM.nextBytes(sessionKey);

        byte[] pdu = EpPduBuilder.buildOtar(1, kek, 128, sessionKey);

        // Header: tag 0x01, length = 512 bits (Annex D: N*272 + 240, N=1)
        ByteBuffer bb = ByteBuffer.wrap(pdu);
        assertEquals(EpPduBuilder.TAG_OTAR, bb.get() & 0xFF);
        assertEquals(512, bb.getShort() & 0xFFFF);
        // Data field: masterKeyId(2) + IV(12) + encKeyBlock(34) + MAC(16)
        assertEquals(3 + 64, pdu.length);
        assertEquals(1, bb.getShort() & 0xFFFF);
    }

    @Test
    public void otarPduDecryptsToKeyIdAndSessionKey() throws Exception {
        byte[] kek = new byte[32];
        byte[] sessionKey = new byte[32];
        RANDOM.nextBytes(kek);
        RANDOM.nextBytes(sessionKey);

        byte[] pdu = EpPduBuilder.buildOtar(7, kek, 200, sessionKey);
        ByteBuffer bb = ByteBuffer.wrap(pdu, 3, pdu.length - 3);
        assertEquals(7, bb.getShort() & 0xFFFF);
        byte[] iv = new byte[12];
        bb.get(iv);
        byte[] ctAndTag = new byte[bb.remaining()];
        bb.get(ctAndTag);

        // Recipient-side processing: AES-256-GCM decrypt under the KEK
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(kek, "AES"), new GCMParameterSpec(128, iv));
        byte[] plaintext = cipher.doFinal(ctAndTag);

        ByteBuffer pt = ByteBuffer.wrap(plaintext);
        assertEquals(200, pt.getShort() & 0xFFFF);
        byte[] recovered = new byte[32];
        pt.get(recovered);
        assertArrayEquals(sessionKey, recovered);
    }

    @Test
    public void otarPduRejectsTampering() throws Exception {
        byte[] kek = new byte[32];
        byte[] pdu = EpPduBuilder.buildOtar(1, kek, 128, new byte[32]);
        pdu[20] ^= 0x01;

        ByteBuffer bb = ByteBuffer.wrap(pdu, 3, pdu.length - 3);
        bb.getShort();
        byte[] iv = new byte[12];
        bb.get(iv);
        byte[] ctAndTag = new byte[bb.remaining()];
        bb.get(ctAndTag);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(kek, "AES"), new GCMParameterSpec(128, iv));
        assertThrows(AEADBadTagException.class, () -> cipher.doFinal(ctAndTag));
    }

    @Test
    public void otarPduUsesFreshIvs() throws Exception {
        byte[] kek = new byte[32];
        byte[] key = new byte[32];
        byte[] pdu1 = EpPduBuilder.buildOtar(1, kek, 128, key);
        byte[] pdu2 = EpPduBuilder.buildOtar(1, kek, 128, key);
        byte[] iv1 = Arrays.copyOfRange(pdu1, 5, 17);
        byte[] iv2 = Arrays.copyOfRange(pdu2, 5, 17);
        assertFalse(Arrays.equals(iv1, iv2));
    }

    @Test
    public void otarRejectsBadKeyLengths() {
        assertThrows(IllegalArgumentException.class,
                () -> EpPduBuilder.buildOtar(1, new byte[16], 128, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> EpPduBuilder.buildOtar(1, new byte[32], 128, new byte[16]));
    }

    @Test
    public void keyActivationPdu() {
        byte[] pdu = EpPduBuilder.buildKeyActivation(128, 129);
        ByteBuffer bb = ByteBuffer.wrap(pdu);
        assertEquals(EpPduBuilder.TAG_KEY_ACTIVATION, bb.get() & 0xFF);
        assertEquals(32, bb.getShort() & 0xFFFF);
        assertEquals(128, bb.getShort() & 0xFFFF);
        assertEquals(129, bb.getShort() & 0xFFFF);
    }

    @Test
    public void keyVerificationPdu() {
        byte[] challenge = new byte[16];
        byte[] pdu = EpPduBuilder.buildKeyVerification(150, challenge);
        ByteBuffer bb = ByteBuffer.wrap(pdu);
        assertEquals(EpPduBuilder.TAG_KEY_VERIFICATION, bb.get() & 0xFF);
        // Annex D: N*18 octets = 144 bits for N=1
        assertEquals(144, bb.getShort() & 0xFFFF);
        assertEquals(150, bb.getShort() & 0xFFFF);
        byte[] embedded = new byte[16];
        bb.get(embedded);
        assertArrayEquals(challenge, embedded);
        assertFalse(Arrays.equals(challenge, new byte[16]));
    }
}
