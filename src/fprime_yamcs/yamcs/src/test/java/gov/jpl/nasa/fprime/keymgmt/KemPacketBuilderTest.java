package gov.jpl.nasa.fprime.keymgmt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

public class KemPacketBuilderTest {

    @Test
    public void fragmentsReassembleToCiphertext() throws Exception {
        byte[] ciphertext = new byte[MlKem768.CIPHERTEXT_LENGTH];
        for (int i = 0; i < ciphertext.length; i++) {
            ciphertext[i] = (byte) i;
        }
        List<byte[]> fragments = KemPacketBuilder.buildFragments(7, ciphertext, 512);
        assertEquals(3, fragments.size()); // ceil(1088 / 512)

        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < fragments.size(); i++) {
            ByteBuffer bb = ByteBuffer.wrap(fragments.get(i));
            assertEquals(KemPacketBuilder.VERSION, bb.get());
            assertEquals(7, bb.getShort() & 0xFFFF);
            assertEquals(i, bb.get() & 0xFF);
            assertEquals(3, bb.get() & 0xFF);
            byte[] data = new byte[bb.remaining()];
            bb.get(data);
            reassembled.write(data);
        }
        assertArrayEquals(ciphertext, reassembled.toByteArray());
    }

    @Test
    public void singleFragmentWhenItFits() {
        List<byte[]> fragments = KemPacketBuilder.buildFragments(1, new byte[100], 512);
        assertEquals(1, fragments.size());
        assertEquals(KemPacketBuilder.HEADER_LENGTH + 100, fragments.get(0).length);
    }

    @Test
    public void rejectsInvalidFragmentSize() {
        assertThrows(IllegalArgumentException.class,
                () -> KemPacketBuilder.buildFragments(1, new byte[100], 0));
    }
}
