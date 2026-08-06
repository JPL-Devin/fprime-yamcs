package org.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.junit.jupiter.api.Test;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

import org.fprime.yamcs.packet.CfdpChecksum;
import org.fprime.yamcs.packet.CfdpPdu;

public class CfdpDownlinkHandlerTest {

    private static final int LOCAL = 1;
    private static final int REMOTE = 2;

    private final FakeBucket bucket = new FakeBucket();
    private final RecordingListener listener = new RecordingListener();

    private FprimeFileTransfer lastResolved;

    private CfdpDownlinkHandler handler(int maxFileSize) {
        return new CfdpDownlinkHandler(bucket, null, maxFileSize,
                (src, dst, size) -> {
                    lastResolved = new FprimeFileTransfer(1, "fake", dst, src, size,
                            TransferDirection.DOWNLOAD, "CFDP", false);
                    return lastResolved;
                }, listener);
    }

    private void feed(CfdpDownlinkHandler h, byte[] pdu) {
        h.handlePdu(pdu, 0);
    }

    private void runTransaction(CfdpDownlinkHandler h, int tx, byte[] content,
                                String dst, int chunk) {
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, tx, content.length, "src.bin", dst));
        for (int off = 0; off < content.length; off += chunk) {
            int len = Math.min(chunk, content.length - off);
            feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, tx, off, content, off, len));
        }
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, tx, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), content.length));
    }

    @Test
    public void reassemblesAndStoresFile() {
        byte[] content = new byte[300];
        new Random(1).nextBytes(content);
        CfdpDownlinkHandler h = handler(1024);

        runTransaction(h, 7, content, "/data/out.bin", 100);

        assertArrayEquals(content, bucket.objects.get("data/out.bin"));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
        assertEquals(content.length, lastResolved.getTransferredSize());
    }

    @Test
    public void checksumMismatchFailsTransfer() {
        byte[] content = new byte[50];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, content.length, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, content.length));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content) + 1, content.length));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("checksum mismatch"));
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void oversizeMetadataIgnored() {
        CfdpDownlinkHandler h = handler(100);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 101, "s", "/d.bin"));
        assertEquals(null, lastResolved);
    }

    @Test
    public void fileDataOverflowFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // Offset beyond the declared size
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 8, content, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("overflow"));
    }

    @Test
    public void negativeOffsetFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        byte[] content = new byte[10];
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, Integer.MIN_VALUE, content, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
    }

    @Test
    public void cancelConditionCodeFailsTransfer() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_CANCEL_REQUEST, 0, 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(lastResolved.getFailuredReason().contains("cancelled"));
    }

    @Test
    public void eofSizeMismatchFailsTransfer() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 11));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
    }

    @Test
    public void mismatchedTransactionDataDropped() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/d.bin"));
        // File Data / EOF from another transaction must not disturb tx 1
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 2, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 2, CfdpPdu.CONDITION_NO_ERROR, 0, 10));
        assertEquals(TransferState.RUNNING, lastResolved.getTransferState());

        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));
        assertEquals(TransferState.COMPLETED, lastResolved.getTransferState());
    }

    @Test
    public void traversalDestinationRejected() {
        byte[] content = new byte[10];
        CfdpDownlinkHandler h = handler(1024);
        feed(h, CfdpPdu.encodeMetadata(REMOTE, LOCAL, 1, 10, "s", "/../evil.bin"));
        feed(h, CfdpPdu.encodeFileData(REMOTE, LOCAL, 1, 0, content, 0, 10));
        feed(h, CfdpPdu.encodeEof(REMOTE, LOCAL, 1, CfdpPdu.CONDITION_NO_ERROR,
                CfdpChecksum.of(content), 10));

        assertEquals(TransferState.FAILED, lastResolved.getTransferState());
        assertTrue(bucket.objects.isEmpty());
    }

    @Test
    public void malformedPduWithNoInflightIsIgnored() {
        CfdpDownlinkHandler h = handler(1024);
        feed(h, new byte[] { 0x20, 0, 0 });
        assertFalse(bucket.objects.containsKey("d.bin"));
    }
}
