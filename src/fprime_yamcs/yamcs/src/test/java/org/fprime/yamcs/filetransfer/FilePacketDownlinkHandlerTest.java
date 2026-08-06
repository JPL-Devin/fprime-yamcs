package org.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

import org.fprime.yamcs.packet.CfdpChecksum;
import org.fprime.yamcs.packet.FilePacket;

public class FilePacketDownlinkHandlerTest {

    private FakeBucket bucket;
    private RecordingListener listener;
    private FprimeFileTransfer lastResolved;

    @TempDir
    Path mirrorDir;

    @BeforeEach
    public void setup() {
        bucket = new FakeBucket();
        listener = new RecordingListener();
        lastResolved = null;
    }

    private FilePacketDownlinkHandler handler(int maxFileSize) {
        return new FilePacketDownlinkHandler(bucket, mirrorDir, maxFileSize,
                (src, dst, size) -> {
                    lastResolved = new FprimeFileTransfer(1, "fake", dst, src, size,
                            TransferDirection.DOWNLOAD, "test", false);
                    return lastResolved;
                }, listener);
    }

    private static void sendSequence(FilePacketDownlinkHandler h, String dst, byte[] content) {
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", dst), 0);
        int seq = 1;
        for (int off = 0; off < content.length; off += 100) {
            int len = Math.min(100, content.length - off);
            h.handleFilePacket(FilePacket.encodeData(seq++, off, content, off, len), 0);
        }
        h.handleFilePacket(FilePacket.encodeEnd(seq, CfdpChecksum.of(content)), 0);
    }

    @Test
    public void reassemblesAndStoresFile() throws IOException {
        byte[] content = new byte[350];
        new Random(1).nextBytes(content);
        FilePacketDownlinkHandler h = handler(1024);

        sendSequence(h, "/out/result.bin", content);

        assertArrayEquals(content, bucket.objects.get("out/result.bin"));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertEquals(content.length, lastResolved.getTransferredSize());
        assertArrayEquals(content, Files.readAllBytes(mirrorDir.resolve("out/result.bin")));
    }

    @Test
    public void checksumMismatchFailsTransfer() {
        byte[] content = new byte[10];
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, content.length, "/src", "/f"), 0);
        h.handleFilePacket(FilePacket.encodeData(1, 0, content, 0, content.length), 0);
        h.handleFilePacket(FilePacket.encodeEnd(2, CfdpChecksum.of(content) + 1), 0);

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("checksum mismatch"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void oversizeStartIgnored() {
        FilePacketDownlinkHandler h = handler(100);
        h.handleFilePacket(FilePacket.encodeStart(0, 101, "/src", "/f"), 0);
        assertNull(lastResolved);
    }

    @Test
    public void negativeDeclaredSizeIgnored() {
        FilePacketDownlinkHandler h = handler(100);
        h.handleFilePacket(FilePacket.encodeStart(0, -1, "/src", "/f"), 0);
        assertNull(lastResolved);
    }

    @Test
    public void dataOverflowFailsTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        byte[] data = new byte[8];
        h.handleFilePacket(FilePacket.encodeData(1, 5, data, 0, data.length), 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void overflowingByteOffsetFailsTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        // byteOffset near Integer.MAX_VALUE: int addition would wrap negative
        // and slip past a naive bounds check.
        byte[] pkt = FilePacket.encodeData(1, Integer.MAX_VALUE - 1, new byte[4], 0, 4);
        h.handleFilePacket(pkt, 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void truncatedDataPacketFailsTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        // DATA header claims 4 bytes but the packet carries none
        byte[] pkt = FilePacket.encodeData(1, 0, new byte[4], 0, 4);
        byte[] truncated = new byte[pkt.length - 4];
        System.arraycopy(pkt, 0, truncated, 0, truncated.length);
        h.handleFilePacket(truncated, 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("packet processing error"));
    }

    @Test
    public void traversalDestinationRejected() {
        byte[] content = new byte[4];
        FilePacketDownlinkHandler h = handler(1024);
        sendSequence(h, "/../escape.bin", content);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("unsafe destination path"));
        assertTrue(bucket.objects.isEmpty());
        assertFalse(Files.exists(mirrorDir.getParent().resolve("escape.bin")));
    }

    @Test
    public void cancelFailsInflightTransfer() {
        FilePacketDownlinkHandler h = handler(1024);
        h.handleFilePacket(FilePacket.encodeStart(0, 10, "/src", "/f"), 0);
        // CANCEL packet: descriptor + type 3 + seq
        byte[] cancel = ByteBuffer.allocate(FilePacket.minimumLength())
                .putShort((short) FilePacket.FILE_DESCRIPTOR)
                .put((byte) FilePacket.Type.CANCEL.value)
                .putInt(1)
                .array();
        h.handleFilePacket(cancel, 0);
        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("cancelled"));
    }

    @Test
    public void sanitizeObjectNameRejectsUnsafePaths() {
        assertEquals("a/b.bin", FilePacketDownlinkHandler.sanitizeObjectName("/a/b.bin"));
        assertEquals("b.bin", FilePacketDownlinkHandler.sanitizeObjectName("b.bin"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacketDownlinkHandler.sanitizeObjectName("/"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacketDownlinkHandler.sanitizeObjectName("/a/../b"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacketDownlinkHandler.sanitizeObjectName("../b"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacketDownlinkHandler.sanitizeObjectName("a//b"));
        assertThrows(IllegalArgumentException.class,
                () -> FilePacketDownlinkHandler.sanitizeObjectName("./b"));
    }
}
