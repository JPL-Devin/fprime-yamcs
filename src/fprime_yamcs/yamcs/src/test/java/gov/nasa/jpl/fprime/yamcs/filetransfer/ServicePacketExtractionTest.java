package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * TM-stream dispatch filters: APID matching, minimum-length short-circuit,
 * descriptor check, and trimming trailing frame padding to the
 * CCSDS-declared length before bytes reach the downlink handlers.
 */
public class ServicePacketExtractionTest {

    private static final int APID = 5;

    private static byte[] fakePdu() {
        byte[] pdu = new byte[16];
        for (int i = 0; i < pdu.length; i++) {
            pdu[i] = (byte) (0x40 + i);
        }
        return pdu;
    }

    private static byte[] filePacketSpacePacket() {
        byte[] pkt = FilePacket.encodeStart(0, 10, "/src", "/f");
        return SpacePacket.wrapTelecommand(pkt, APID, 0);
    }

    private static byte[] withPadding(byte[] packet, int padding) {
        return Arrays.copyOf(packet, packet.length + padding);
    }

    @Test
    public void bridgeExtractsPduFromSpacePacket() {
        byte[] pdu = fakePdu();
        byte[] packet = SpacePacket.wrapTelecommand(pdu, APID, 0);
        assertArrayEquals(pdu, CfdpSpacePacketBridge.extractPdu(packet, APID));
    }

    @Test
    public void bridgePaddingTrimmedToDeclaredLength() {
        byte[] pdu = fakePdu();
        byte[] padded = withPadding(SpacePacket.wrapTelecommand(pdu, APID, 0), 32);
        assertArrayEquals(pdu, CfdpSpacePacketBridge.extractPdu(padded, APID));
    }

    @Test
    public void bridgeWrongApidAndShortPacketsDropped() {
        byte[] packet = SpacePacket.wrapTelecommand(fakePdu(), APID, 0);
        assertNull(CfdpSpacePacketBridge.extractPdu(packet, APID + 1));
        assertNull(CfdpSpacePacketBridge.extractPdu(new byte[4], APID));
    }

    @Test
    public void bridgeStripsFileDescriptor() {
        // F´ CfdpManager frames PDUs behind the FW_PACKET_FILE descriptor.
        byte[] pdu = fakePdu();
        byte[] framed = new byte[FilePacket.DESCRIPTOR_LEN + pdu.length];
        framed[0] = (byte) (FilePacket.FILE_DESCRIPTOR >> 8);
        framed[1] = (byte) FilePacket.FILE_DESCRIPTOR;
        System.arraycopy(pdu, 0, framed, FilePacket.DESCRIPTOR_LEN, pdu.length);
        byte[] packet = SpacePacket.wrapTelecommand(framed, APID, 0);

        assertArrayEquals(pdu, CfdpSpacePacketBridge.extractPdu(packet, APID));
    }

    @Test
    public void bridgeWrapUnwrapRoundTrips() {
        byte[] pdu = fakePdu();
        byte[] packet = CfdpSpacePacketBridge.wrapPdu(pdu, APID, 7);
        assertEquals(APID, SpacePacket.apid(packet));
        assertEquals(packet.length, SpacePacket.declaredLength(packet));
        assertArrayEquals(pdu, CfdpSpacePacketBridge.extractPdu(packet, APID));
    }

    @Test
    public void filePacketOnConfiguredApidPassesThrough() {
        byte[] packet = filePacketSpacePacket();
        assertSame(packet, FprimeFilePacketService.extractFilePacket(packet, APID));
    }

    @Test
    public void filePacketPaddingTrimmedToDeclaredLength() {
        byte[] packet = filePacketSpacePacket();
        byte[] padded = withPadding(packet, 32);
        assertArrayEquals(packet, FprimeFilePacketService.extractFilePacket(padded, APID));
    }

    @Test
    public void filePacketWrongApidShortOrBadDescriptorDropped() {
        byte[] packet = filePacketSpacePacket();
        assertNull(FprimeFilePacketService.extractFilePacket(packet, APID + 1));
        assertNull(FprimeFilePacketService.extractFilePacket(new byte[4], APID));

        byte[] badDescriptor = packet.clone();
        badDescriptor[SpacePacket.PRIMARY_HEADER_LEN] = 0x7F;
        badDescriptor[SpacePacket.PRIMARY_HEADER_LEN + 1] = 0x7F;
        assertNull(FprimeFilePacketService.extractFilePacket(badDescriptor, APID));
    }
}
