package gov.jpl.nasa.fprime.keymgmt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

public class SpacePacketsTest {

    @Test
    public void primaryHeaderFields() {
        byte[] payload = new byte[] { 1, 2, 3, 4, 5 };
        byte[] packet = SpacePackets.buildTcPacket(0x20, 42, payload);
        assertEquals(6 + 5, packet.length);

        ByteBuffer bb = ByteBuffer.wrap(packet);
        int word0 = bb.getShort() & 0xFFFF;
        assertEquals(0, word0 >> 13);            // version
        assertEquals(1, (word0 >> 12) & 1);      // type = TC
        assertEquals(0, (word0 >> 11) & 1);      // no secondary header
        assertEquals(0x20, word0 & 0x7FF);       // APID
        int word1 = bb.getShort() & 0xFFFF;
        assertEquals(0b11, word1 >> 14);         // standalone
        assertEquals(42, word1 & 0x3FFF);        // sequence count
        assertEquals(4, bb.getShort() & 0xFFFF); // length = payload - 1
    }

    @Test
    public void sequenceCountWraps() {
        byte[] packet = SpacePackets.buildTcPacket(0x20, 0x4001, new byte[] { 0 });
        int word1 = ByteBuffer.wrap(packet).getShort(2) & 0xFFFF;
        assertEquals(1, word1 & 0x3FFF);
    }

    @Test
    public void rejectsEmptyPayload() {
        assertThrows(IllegalArgumentException.class,
                () -> SpacePackets.buildTcPacket(0x20, 0, new byte[0]));
    }
}
