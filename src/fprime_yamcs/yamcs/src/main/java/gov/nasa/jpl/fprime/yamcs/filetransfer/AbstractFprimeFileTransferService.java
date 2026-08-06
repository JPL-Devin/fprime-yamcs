package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.Processor;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.buckets.Bucket;
import org.yamcs.buckets.BucketManager;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.CommandingManager;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.filetransfer.AbstractFileTransferService;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.FileTransferFilter;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.filetransfer.TransferMonitor;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.EntityInfo;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.TransferState;
import org.yamcs.security.User;
import org.yamcs.xtce.MetaCommand;

/**
 * Common scaffolding for F´ file transfer services: transfer bookkeeping,
 * monitor notification, verifier acks, remote file listings, and synthesis
 * of spacecraft commands (e.g. {@code FileDownlink.SendFile},
 * {@code FileManager.ListDirectory}) through a YAMCS processor.
 *
 * <p>Concrete services (Fw::FilePacket, CFDP, ...) supply the wire protocol:
 * how bytes get uplinked and how downlinked packets are reassembled.
 */
public abstract class AbstractFprimeFileTransferService extends AbstractFileTransferService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String LOCAL_ENTITY_NAME = "ground";
    public static final String REMOTE_ENTITY_NAME = "spacecraft";

    // Custom verifier key reported back to YAMCS command history so
    // operators see the transfer outcome on the triggering command entry
    // in the command stack. Appears as Verifier_FileTransfer_Status etc.
    private static final String VERIFIER_KEY =
            CommandHistoryPublisher.Verifier_KEY_PREFIX + "FileTransfer";

    /**
     * Maximum retained transfer records. Terminal (completed/failed)
     * transfers beyond this are evicted oldest-first so a chattering link
     * cannot grow ground-server memory without bound.
     */
    protected static final int MAX_TRANSFER_HISTORY = 1000;

    /**
     * Maximum uplink transfers queued behind the single uplink worker.
     * Each queued task pins its full file contents in memory, so this
     * mirrors the downlink storage-backlog bound.
     */
    protected static final int MAX_PENDING_UPLOADS = 4;

    private final AtomicLong transferIdSeq = new AtomicLong(1);
    private final AtomicInteger pendingUplinks = new AtomicInteger();
    private final Map<Long, FprimeFileTransfer> transfers = new ConcurrentHashMap<>();
    private final List<TransferMonitor> transferMonitors = new CopyOnWriteArrayList<>();

    /**
     * Callback handed to protocol handlers so they can report transfer
     * progress without the service exposing mutation entry points on its
     * public API.
     */
    protected final TransferEventListener transferListener = new TransferEventListener() {
        @Override
        public void stateChanged(FprimeFileTransfer transfer) {
            notifyStateChanged(transfer);
        }

        @Override
        public void verifierAck(FprimeFileTransfer transfer, AckStatus status, String message) {
            publishVerifierAck(transfer, status, message);
        }
    };

    // Resolved by resolveProcessor() for spacecraft command synthesis.
    protected Processor processor;
    protected CommandingManager commandingManager;
    protected CommandHistoryPublisher commandHistoryPublisher;
    protected User systemUser;

    protected RemoteFileListingHandler listingHandler =
            new RemoteFileListingHandler(REMOTE_ENTITY_NAME);

    // ------------------------------------------------------------------
    // Transfer bookkeeping
    // ------------------------------------------------------------------

    protected long nextTransferId() {
        return transferIdSeq.getAndIncrement();
    }

    protected void addTransfer(FprimeFileTransfer transfer) {
        transfers.put(transfer.getId(), transfer);
        evictOldTransfers();
    }

    private void evictOldTransfers() {
        if (transfers.size() <= MAX_TRANSFER_HISTORY) {
            return;
        }
        transfers.values().stream()
                .filter(t -> t.getTransferState() == TransferState.COMPLETED
                        || t.getTransferState() == TransferState.FAILED)
                .sorted(Comparator.comparingLong(FprimeFileTransfer::getId))
                .limit(transfers.size() - MAX_TRANSFER_HISTORY)
                .forEach(t -> transfers.remove(t.getId()));
    }

    @Override
    public List<FileTransfer> getTransfers(FileTransferFilter filter) {
        List<FileTransfer> all = new ArrayList<>(transfers.values());
        if (filter == null) {
            return all;
        }
        List<FileTransfer> result = new ArrayList<>();
        for (FileTransfer ft : all) {
            if (filter.direction != null && ft.getDirection() != filter.direction) {
                continue;
            }
            if (filter.states != null && !filter.states.isEmpty()
                    && !filter.states.contains(ft.getTransferState())) {
                continue;
            }
            if (filter.localEntityId != null
                    && !filter.localEntityId.equals(ft.getLocalEntityId())) {
                continue;
            }
            if (filter.remoteEntityId != null
                    && !filter.remoteEntityId.equals(ft.getRemoteEntityId())) {
                continue;
            }
            result.add(ft);
        }
        if (filter.limit > 0 && result.size() > filter.limit) {
            result = result.subList(0, filter.limit);
        }
        return result;
    }

    @Override
    public FileTransfer getFileTransfer(long id) {
        return transfers.get(id);
    }

    @Override
    public void registerTransferMonitor(TransferMonitor monitor) {
        transferMonitors.add(monitor);
    }

    @Override
    public void unregisterTransferMonitor(TransferMonitor monitor) {
        transferMonitors.remove(monitor);
    }

    /** Push a transfer state change to all registered monitors. */
    protected void notifyStateChanged(FprimeFileTransfer transfer) {
        for (TransferMonitor m : transferMonitors) {
            try {
                m.stateChanged(transfer);
            } catch (Exception e) {
                log.warn("Transfer monitor threw", e);
            }
        }
    }

    /**
     * Publish a verifier ack to the YAMCS command history entry for the
     * command that triggered this transfer, so operators drilling into the
     * command stack see the transfer's progress alongside the standard acks.
     *
     * <p>No-op if the transfer wasn't triggered by a synthesized command.
     */
    protected void publishVerifierAck(FprimeFileTransfer transfer, AckStatus status,
                                      String message) {
        if (commandHistoryPublisher == null) {
            return;
        }
        CommandId cmdId = transfer.getTriggeringCommandId();
        if (cmdId == null) {
            return;
        }
        try {
            commandHistoryPublisher.publishAck(cmdId, VERIFIER_KEY,
                    System.currentTimeMillis(), status, message);
        } catch (Exception e) {
            log.debug("Failed to publish verifier ack for transfer {}", transfer.getId(), e);
        }
    }

    /** Flip a transfer to FAILED and notify monitors and command history. */
    protected void failTransfer(FprimeFileTransfer transfer, AckStatus ackStatus, String reason) {
        transfer.setFailureReason(reason);
        transfer.setState(TransferState.FAILED);
        notifyStateChanged(transfer);
        publishVerifierAck(transfer, ackStatus, reason);
    }

    /**
     * Queue an uplink task, bounding the backlog: each queued task pins the
     * file contents in memory, so past {@link #MAX_PENDING_UPLOADS} the
     * request is rejected instead of queued. Registers the transfer and
     * notifies monitors before submission.
     */
    protected void submitUplink(ExecutorService uplinkExecutor,
                                FprimeFileTransfer transfer, Runnable task) {
        if (pendingUplinks.get() >= MAX_PENDING_UPLOADS) {
            throw new InvalidRequestException("uplink backlog: " + MAX_PENDING_UPLOADS
                    + " transfers already queued");
        }
        addTransfer(transfer);
        notifyStateChanged(transfer);
        pendingUplinks.incrementAndGet();
        try {
            uplinkExecutor.submit(() -> {
                try {
                    task.run();
                } finally {
                    pendingUplinks.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            pendingUplinks.decrementAndGet();
            failTransfer(transfer, AckStatus.NOK, "uplink executor rejected: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Flip every non-terminal (queued/running/paused) transfer to FAILED.
     * Called from {@code doStop()} so stopped services never leave transfers
     * stranded in a non-terminal state that eviction cannot reclaim.
     */
    protected void failNonTerminalTransfers(String reason) {
        for (FprimeFileTransfer t : transfers.values()) {
            TransferState s = t.getTransferState();
            if (s != TransferState.COMPLETED && s != TransferState.FAILED) {
                failTransfer(t, AckStatus.NOK, reason);
            }
        }
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    /** API-level local entity id; services with configured ids override. */
    protected long localApiEntityId() {
        return FprimeFileTransfer.GROUND_ENTITY_ID;
    }

    /** API-level remote entity id; services with configured ids override. */
    protected long remoteApiEntityId() {
        return FprimeFileTransfer.SPACECRAFT_ENTITY_ID;
    }

    @Override
    public List<EntityInfo> getLocalEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(localApiEntityId())
                .setName(LOCAL_ENTITY_NAME)
                .build());
    }

    @Override
    public List<EntityInfo> getRemoteEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(remoteApiEntityId())
                .setName(REMOTE_ENTITY_NAME)
                .build());
    }

    // ------------------------------------------------------------------
    // Spacecraft command synthesis
    // ------------------------------------------------------------------

    /**
     * Resolve the first processor of this instance for command synthesis.
     * Returns false (and logs) if no processor is available.
     */
    protected boolean resolveProcessor() {
        YamcsServerInstance ysi = YamcsServer.getServer().getInstance(yamcsInstance);
        this.processor = ysi == null ? null : ysi.getFirstProcessor();
        if (processor == null) {
            log.warn("No processor available; spacecraft command synthesis disabled");
            return false;
        }
        this.commandingManager = processor.getCommandingManager();
        this.commandHistoryPublisher = processor.getCommandHistoryPublisher();
        this.systemUser = YamcsServer.getServer().getSecurityStore().getSystemUser();
        return true;
    }

    /**
     * Find a MetaCommand by qualified name, or — when {@code configuredName}
     * is empty — by a qualified-name suffix (e.g. {@code "SendFile"}),
     * enabling auto-discovery against generated F´ dictionaries.
     */
    protected MetaCommand findCommand(String configuredName, String suffix) {
        if (processor == null) {
            return null;
        }
        String name = configuredName;
        if ((name == null || name.isEmpty()) && suffix != null && !suffix.isEmpty()) {
            List<String> names = new ArrayList<>();
            for (MetaCommand cmd : processor.getMdb().getMetaCommands()) {
                names.add(cmd.getQualifiedName());
            }
            List<String> candidates = suffixCandidates(names, suffix);
            if (candidates.size() > 1) {
                // Refuse ambiguous auto-discovery: which command wins would
                // depend on MDB iteration order, and the chosen command is
                // dispatched to the spacecraft. Require explicit config.
                log.warn("Multiple commands match suffix '{}': {}; refusing auto-discovery — "
                        + "configure the qualified command name explicitly", suffix, candidates);
                return null;
            }
            name = candidates.isEmpty() ? null : candidates.get(0);
            log.info("Auto-discovered command for suffix '{}': {}", suffix, name);
        }
        return name == null || name.isEmpty() ? null : processor.getMdb().getMetaCommand(name);
    }

    /**
     * Qualified names whose final segment equals {@code suffix}. Matching
     * is on a name-segment boundary so e.g. {@code AbortSendFile} is never
     * mistaken for {@code SendFile}.
     */
    static List<String> suffixCandidates(Collection<String> qualifiedNames, String suffix) {
        List<String> candidates = new ArrayList<>();
        for (String qn : qualifiedNames) {
            if (qn.endsWith("/" + suffix) || qn.endsWith("." + suffix)) {
                candidates.add(qn);
            }
        }
        return candidates;
    }

    /**
     * Fetch a bucket object with a bounded wait, translating async failures
     * into the API-facing exception types.
     */
    protected static byte[] fetchObject(Bucket bucket, String objectName) throws IOException {
        try {
            return bucket.getObjectAsync(objectName).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading '" + objectName
                    + "' from bucket " + bucket.getName(), e);
        } catch (TimeoutException e) {
            throw new IOException("Timed out reading '" + objectName
                    + "' from bucket " + bucket.getName(), e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IOException("Failed to read '" + objectName + "' from bucket "
                    + bucket.getName() + ": " + cause.getMessage(), cause);
        }
    }

    /**
     * Build and dispatch a spacecraft command, returning its CommandId.
     *
     * <p>Commands are dispatched as the YAMCS system user (matching the
     * built-in {@code org.yamcs.cfdp.CfdpService} pattern): a user granted
     * file-transfer privileges implicitly gains the authority to send the
     * configured transfer commands, bypassing per-user command authorization.
     * Deployments should gate file-transfer privileges accordingly.
     */
    protected CommandId dispatchCommand(MetaCommand command, Map<String, Object> args,
                                        String origin, int sequenceNumber) throws Exception {
        PreparedCommand pc = commandingManager.buildCommand(
                command, args, origin, sequenceNumber, systemUser);
        commandingManager.sendCommand(systemUser, pc);
        return pc.getCommandId();
    }

    protected Bucket getOrCreateBucket(String name) throws Exception {
        BucketManager bm = YamcsServer.getServer().getBucketManager();
        Bucket b = bm.getBucket(name);
        if (b == null) {
            log.info("Bucket {} not found, creating", name);
            b = bm.createBucket(name);
        }
        return b;
    }

    // ------------------------------------------------------------------
    // Remote file listing — delegated to RemoteFileListingHandler
    // ------------------------------------------------------------------

    @Override
    public ListFilesResponse getFileList(String localEntity, String remoteEntity,
                                         String remotePath, Map<String, Object> options) {
        return listingHandler.getFileList(normalizeDirName(remotePath));
    }

    @Override
    public void saveFileList(ListFilesResponse listing) {
        listingHandler.saveFileList(listing);
    }

    @Override
    public void registerRemoteFileListMonitor(RemoteFileListMonitor monitor) {
        listingHandler.registerMonitor(monitor);
    }

    @Override
    public void unregisterRemoteFileListMonitor(RemoteFileListMonitor monitor) {
        listingHandler.unregisterMonitor(monitor);
    }

    @Override
    public void notifyRemoteFileListMonitors(ListFilesResponse listing) {
        listingHandler.notifyMonitors(listing);
    }

    @Override
    public Set<RemoteFileListMonitor> getRemoteFileListMonitors() {
        return listingHandler.getMonitors();
    }

    protected static String normalizeDirName(String remotePath) {
        return (remotePath == null || remotePath.isEmpty()) ? "." : remotePath;
    }
}
