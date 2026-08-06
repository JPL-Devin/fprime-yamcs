package org.fprime.yamcs.packet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class CfdpPduTest {

    @Test
    public void metadataRoundTrip() {
        byte[] pdu = CfdpPdu.encodeMetadata(1, 2, 42, 1234, "src.bin", "/dst/file.bin");
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.Type.FILE_DIRECTIVE, h.type);
        assertEquals(1, h.sourceEntityId);
        assertEquals(2, h.destinationEntityId);
        assertEquals(42, h.transactionSeq);
        assertEquals(CfdpPdu.DIRECTIVE_METADATA, CfdpPdu.directiveCode(pdu, h));
        CfdpPdu.Metadata md = CfdpPdu.decodeMetadata(pdu, h);
        assertEquals(1234, md.fileSize);
        assertEquals("src.bin", md.sourceFileName);
        assertEquals("/dst/file.bin", md.destinationFileName);
    }

    @Test
    public void fileDataRoundTrip() {
        byte[] content = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] pdu = CfdpPdu.encodeFileData(1, 2, 7, 100, content, 2, 4);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.Type.FILE_DATA, h.type);
        assertEquals(7, h.transactionSeq);
        CfdpPdu.FileData fd = CfdpPdu.decodeFileData(pdu, h);
        assertEquals(100, fd.offset);
        assertEquals(4, fd.dataSize);
        byte[] data = Arrays.copyOfRange(pdu, fd.dataStart, fd.dataStart + fd.dataSize);
        assertEquals(3, data[0]);
        assertEquals(6, data[3]);
    }

    @Test
    public void eofRoundTrip() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_NO_ERROR, 0xDEADBEEF, 500);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.DIRECTIVE_EOF, CfdpPdu.directiveCode(pdu, h));
        CfdpPdu.Eof eof = CfdpPdu.decodeEof(pdu, h);
        assertEquals(CfdpPdu.CONDITION_NO_ERROR, eof.conditionCode);
        assertEquals(0xDEADBEEF, eof.checksum);
        assertEquals(500, eof.fileSize);
    }

    @Test
    public void eofCancelConditionCode() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 9, CfdpPdu.CONDITION_CANCEL_REQUEST, 0, 0);
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertEquals(CfdpPdu.CONDITION_CANCEL_REQUEST, CfdpPdu.decodeEof(pdu, h).conditionCode);
    }

    @Test
    public void truncatedPdusRejected() {
        byte[] md = CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", "b");
        // Header truncated
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.decodeHeader(Arrays.copyOf(md, CfdpPdu.HEADER_LEN - 1), 0));
        // Data field truncated below the declared data field length
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.decodeHeader(Arrays.copyOf(md, md.length - 1), 0));
        // Metadata payload truncated (patch length field down, cut LV short)
        byte[] cut = Arrays.copyOf(md, CfdpPdu.HEADER_LEN + 7);
        cut[1] = 0;
        cut[2] = 7;
        CfdpPdu.Header h = CfdpPdu.decodeHeader(cut, 0);
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeMetadata(cut, h));
    }

    @Test
    public void unsupportedVersionRejected() {
        byte[] pdu = CfdpPdu.encodeEof(1, 2, 0, 0, 0, 0);
        pdu[0] = (byte) ((pdu[0] & 0x1F) | (0b011 << 5));
        assertThrows(IllegalArgumentException.class, () -> CfdpPdu.decodeHeader(pdu, 0));
    }

    @Test
    public void overlongFileNamesRejected() {
        String longName = "x".repeat(256);
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, 0, 10, longName, "b"));
        assertThrows(IllegalArgumentException.class,
                () -> CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", longName));
    }

    @Test
    public void encodedPdusAreUnacknowledgedClass1() {
        byte[] pdu = CfdpPdu.encodeMetadata(1, 2, 0, 10, "a", "b");
        CfdpPdu.Header h = CfdpPdu.decodeHeader(pdu, 0);
        assertTrue(!h.acknowledged);
    }
}
