package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.FileTransferFilter;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.TransferMonitor;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.FileTransferCapabilities;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;

public class AbstractFprimeFileTransferServiceTest {

    /** Minimal concrete service exposing the shared bookkeeping for test. */
    private static final class TestService extends AbstractFprimeFileTransferService {
        @Override
        protected void addCapabilities(FileTransferCapabilities.Builder builder) {
        }

        @Override
        public void fetchFileList(String source, String destination, String remotePath,
                java.util.Map<String, Object> options) {
        }

        @Override
        public FileTransfer startUpload(String source, Bucket bucket, String objectName,
                String destination, String destinationPath, TransferOptions options) {
            return null;
        }

        @Override
        public FileTransfer startDownload(String source, String sourcePath, String destination,
                Bucket bucket, String objectName, TransferOptions options) {
            return null;
        }

        @Override
        public void pause(FileTransfer transfer) {
        }

        @Override
        public void resume(FileTransfer transfer) {
        }

        @Override
        public void cancel(FileTransfer transfer) {
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }
    }

    private final TestService service = new TestService();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    public void shutdown() {
        executor.shutdownNow();
    }

    private FprimeFileTransfer transfer(TransferDirection direction) {
        return new FprimeFileTransfer(service.nextTransferId(), "bucket", "obj",
                "/remote", 10, direction, "TEST", false);
    }

    @Test
    public void transferIdsAreUnique() {
        assertTrue(service.nextTransferId() < service.nextTransferId());
    }

    @Test
    public void addTransferIsRetrievableById() {
        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.addTransfer(t);
        assertSame(t, service.getFileTransfer(t.getId()));
        assertNull(service.getFileTransfer(999_999L));
    }

    @Test
    public void getTransfersFiltersByDirectionStateAndLimit() {
        FprimeFileTransfer up = transfer(TransferDirection.UPLOAD);
        FprimeFileTransfer down = transfer(TransferDirection.DOWNLOAD);
        down.setState(TransferState.COMPLETED);
        service.addTransfer(up);
        service.addTransfer(down);

        assertEquals(2, service.getTransfers(null).size());

        FileTransferFilter byDirection = new FileTransferFilter();
        byDirection.direction = TransferDirection.DOWNLOAD;
        assertEquals(List.of(down), service.getTransfers(byDirection));

        FileTransferFilter byState = new FileTransferFilter();
        byState.states = List.of(TransferState.COMPLETED);
        assertEquals(List.of(down), service.getTransfers(byState));

        FileTransferFilter limited = new FileTransferFilter();
        limited.limit = 1;
        // Newest first (default descending): the limit keeps the most recent.
        assertEquals(List.of(down), service.getTransfers(limited));

        FileTransferFilter ascending = new FileTransferFilter();
        ascending.descending = false;
        assertEquals(List.of(up, down), service.getTransfers(ascending));
    }

    @Test
    public void getTransfersFiltersByEntityIdsAndTimeWindow() {
        FprimeFileTransfer a = transfer(TransferDirection.UPLOAD);
        a.setEntityIds(1L, 2L);
        FprimeFileTransfer b = transfer(TransferDirection.UPLOAD);
        b.setEntityIds(3L, 4L);
        service.addTransfer(a);
        service.addTransfer(b);

        FileTransferFilter byLocal = new FileTransferFilter();
        byLocal.localEntityId = 1L;
        assertEquals(List.of(a), service.getTransfers(byLocal));

        FileTransferFilter byRemote = new FileTransferFilter();
        byRemote.remoteEntityId = 4L;
        assertEquals(List.of(b), service.getTransfers(byRemote));

        FileTransferFilter futureWindow = new FileTransferFilter();
        futureWindow.start = a.getCreationTime() + 3_600_000L;
        assertEquals(List.of(), service.getTransfers(futureWindow));

        FileTransferFilter pastWindow = new FileTransferFilter();
        pastWindow.stop = a.getCreationTime() - 3_600_000L;
        assertEquals(List.of(), service.getTransfers(pastWindow));

        FileTransferFilter openWindow = new FileTransferFilter();
        openWindow.start = a.getCreationTime() - 3_600_000L;
        openWindow.stop = a.getCreationTime() + 3_600_000L;
        assertEquals(List.of(b, a), service.getTransfers(openWindow));
    }

    @Test
    public void fetchObjectTranslatesAsyncFailuresToIoException() throws Exception {
        FakeBucket bucket = new FakeBucket();
        bucket.objects.put("obj", new byte[] { 1, 2 });
        assertEquals(2, AbstractFprimeFileTransferService.fetchObject(bucket, "obj").length);

        bucket.failGets = true;
        java.io.IOException e = assertThrows(java.io.IOException.class,
                () -> AbstractFprimeFileTransferService.fetchObject(bucket, "obj"));
        assertTrue(e.getMessage().contains("read error"));
    }

    @Test
    public void terminalTransfersEvictedPastHistoryBound() {
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY + 5; i++) {
            FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
            t.setState(TransferState.COMPLETED);
            service.addTransfer(t);
        }
        assertEquals(AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY,
                service.getTransfers(null).size());
    }

    @Test
    public void runningTransfersSurviveEviction() {
        List<FprimeFileTransfer> running = new ArrayList<>();
        for (int i = 0; i < AbstractFprimeFileTransferService.MAX_TRANSFER_HISTORY + 5; i++) {
            FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
            if (i < 5) {
                t.setState(TransferState.RUNNING);
                running.add(t);
            } else {
                t.setState(TransferState.COMPLETED);
            }
            service.addTransfer(t);
        }
        for (FprimeFileTransfer t : running) {
            assertSame(t, service.getFileTransfer(t.getId()));
        }
    }

    @Test
    public void failNonTerminalTransfersFlipsOnlyNonTerminal() {
        FprimeFileTransfer queued = transfer(TransferDirection.UPLOAD);
        FprimeFileTransfer done = transfer(TransferDirection.DOWNLOAD);
        done.setState(TransferState.COMPLETED);
        service.addTransfer(queued);
        service.addTransfer(done);

        service.failNonTerminalTransfers("service stopped");

        assertEquals(TransferState.FAILED, queued.getTransferState());
        assertEquals("service stopped", queued.getFailuredReason());
        assertEquals(TransferState.COMPLETED, done.getTransferState());
    }

    @Test
    public void monitorsNotifiedAndThrowingMonitorIsolated() {
        List<FileTransfer> seen = new ArrayList<>();
        TransferMonitor bad = t -> {
            throw new IllegalStateException("boom");
        };
        TransferMonitor good = seen::add;
        service.registerTransferMonitor(bad);
        service.registerTransferMonitor(good);

        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.notifyStateChanged(t);
        assertEquals(List.of(t), seen);

        service.unregisterTransferMonitor(bad);
        service.unregisterTransferMonitor(good);
        service.notifyStateChanged(t);
        assertEquals(1, seen.size());
    }

    @Test
    public void failTransferSetsReasonAndState() {
        FprimeFileTransfer t = transfer(TransferDirection.UPLOAD);
        service.addTransfer(t);
        service.failTransfer(t, AckStatus.NOK, "no link");
        assertEquals(TransferState.FAILED, t.getTransferState());
        assertEquals("no link", t.getFailuredReason());
    }

    @Test
    public void uplinkBacklogIsBounded() throws Exception {
        Object gate = new Object();
        synchronized (gate) {
            // First task blocks the single worker; the rest sit in the queue.
            for (int i = 0; i < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS; i++) {
                service.submitUplink(executor, transfer(TransferDirection.UPLOAD), () -> {
                    synchronized (gate) {
                        // released when the test exits the synchronized block
                    }
                });
            }
            FprimeFileTransfer overflow = transfer(TransferDirection.UPLOAD);
            InvalidRequestException e = assertThrows(InvalidRequestException.class,
                    () -> service.submitUplink(executor, overflow, () -> {
                    }));
            assertTrue(e.getMessage().contains("backlog"));
        }
    }

    @Test
    public void uplinkSlotsReleasedAfterCompletion() throws Exception {
        for (int round = 0; round < 3; round++) {
            int submitted = 0;
            long deadline = System.currentTimeMillis() + 5000;
            // Fill the backlog bound in every round: slots freed by completed
            // tasks must become available again (finally-block decrements).
            while (submitted < AbstractFprimeFileTransferService.MAX_PENDING_UPLOADS) {
                try {
                    service.submitUplink(executor, transfer(TransferDirection.UPLOAD), () -> {
                    });
                    submitted++;
                } catch (InvalidRequestException e) {
                    assertTrue(System.currentTimeMillis() < deadline,
                            "uplink slots never released");
                    Thread.sleep(5);
                }
            }
            executor.submit(() -> {
            }).get();
        }
    }
}
