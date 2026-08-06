package org.fprime.yamcs.filetransfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
public abstract class AbstractFprimeFileTransferService extends AbstractFileTransferService
        implements TransferEventListener {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String LOCAL_ENTITY_NAME = "ground";
    public static final String REMOTE_ENTITY_NAME = "spacecraft";

    // Custom verifier key reported back to YAMCS command history so
    // operators see the transfer outcome on the triggering command entry
    // in the command stack. Appears as Verifier_FileTransfer_Status etc.
    private static final String VERIFIER_KEY =
            CommandHistoryPublisher.Verifier_KEY_PREFIX + "FileTransfer";

    private final AtomicLong transferIdSeq = new AtomicLong(1);
    private final Map<Long, FprimeFileTransfer> transfers = new ConcurrentHashMap<>();
    private final List<TransferMonitor> transferMonitors = new CopyOnWriteArrayList<>();

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

    @Override
    public void stateChanged(FprimeFileTransfer transfer) {
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
    @Override
    public void verifierAck(FprimeFileTransfer transfer, AckStatus status, String message) {
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
        stateChanged(transfer);
        verifierAck(transfer, ackStatus, reason);
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    @Override
    public List<EntityInfo> getLocalEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(FprimeFileTransfer.GROUND_ENTITY_ID)
                .setName(LOCAL_ENTITY_NAME)
                .build());
    }

    @Override
    public List<EntityInfo> getRemoteEntities() {
        return List.of(EntityInfo.newBuilder()
                .setId(FprimeFileTransfer.SPACECRAFT_ENTITY_ID)
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
        this.processor = ysi.getFirstProcessor();
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
        if (name == null || name.isEmpty()) {
            for (MetaCommand cmd : processor.getMdb().getMetaCommands()) {
                if (cmd.getQualifiedName().endsWith(suffix)) {
                    name = cmd.getQualifiedName();
                    break;
                }
            }
            log.info("Auto-discovered command for suffix '{}': {}", suffix, name);
        }
        return name == null ? null : processor.getMdb().getMetaCommand(name);
    }

    /** Build and dispatch a spacecraft command, returning its CommandId. */
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
