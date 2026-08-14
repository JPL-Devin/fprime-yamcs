package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.yamcs.InitException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.buckets.Bucket;
import org.yamcs.cfdp.CfdpService;
import org.yamcs.commanding.CommandingManager;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;
import org.yamcs.security.User;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.YarchDatabase;

/**
 * YAMCS's native {@link CfdpService} adapted for F´: all CFDP protocol
 * logic (transactions, retransmission, class-1/class-2, PDU codecs) is the
 * stock YAMCS implementation, consuming PDUs from {@code cfdp_in} /
 * {@code cfdp_out} via {@link CfdpSpacePacketBridge}. Only the F´ command
 * side is overridden:
 *
 * <ul>
 *   <li><b>Download initiation</b>: F´'s CfdpManager does not implement the
 *       CFDP proxy-put operation, so {@link #startDownload} synthesizes the
 *       configured F´ downlink command (e.g. {@code CfdpManager.SendFile})
 *       instead; the resulting spacecraft-initiated transaction is tracked
 *       natively as an incoming transfer.
 *   <li><b>Remote file listing</b>: F´ has no CFDP directory-listing
 *       support, so {@link #fetchFileList} synthesizes the F´
 *       {@code ListDirectory} command and {@link RemoteFileListingHandler}
 *       assembles the listing from the resulting F´ events (requires the
 *       {@code fprime-yamcs-events} publisher).
 * </ul>
 *
 * <p>Configured under {@code services:} after the bridge:
 * <pre>
 *   - class: gov.nasa.jpl.fprime.yamcs.filetransfer.FprimeCfdpService
 *     args:
 *       inStream: cfdp_in              # default
 *       outStream: cfdp_out            # default
 *       localEntities:
 *         - name: ground
 *           id: 1
 *       remoteEntities:
 *         - name: spacecraft
 *           id: 2
 *       incomingBucket: cfdpFiles
 *       entityIdLength: 1              # F´ CfdpManager uses one-byte ids
 *       sequenceNrLength: 1
 *       fileDownlinkCommand: ""        # qualified MDB name of the F´ CFDP
 *                                      # downlink command; startDownload is
 *                                      # rejected when empty
 *       sourceFileNameArg: sourceFileName
 *       destFileNameArg: destFileName
 *       downlinkCommandArgs:           # fixed values for remaining args
 *         channelId: 0                 # (example: F´ CfdpManager.SendFile)
 *       listDirectoryCommand: ""       # auto-discovered by "ListDirectory"
 *                                      # suffix when empty
 *       listDirDirNameArg: dirName
 *       eventsStream: events_realtime
 * </pre>
 */
public class FprimeCfdpService extends CfdpService {

    private static final String TRANSFER_TYPE = "CFDP";

    // Configuration
    private String fileDownlinkCommandName;
    private String sourceFileNameArg;
    private String destFileNameArg;
    private Map<String, Object> downlinkCommandArgs;
    private String listDirectoryCommandName;
    private String listDirDirNameArg;
    private String eventsStreamName;

    // Runtime
    private Stream eventsStream;
    private ExecutorService monitorNotifier;
    private ScheduledExecutorService listingSweeper;
    private RemoteFileListingHandler listingHandler;
    private Processor processor;
    private CommandingManager commandingManager;
    private User systemUser;
    MetaCommand fileDownlinkCommand;   // may be null if not in MDB
    MetaCommand listDirectoryCommand;  // may be null if not in MDB
    private final AtomicLong downloadIdSeq = new AtomicLong(1);

    @Override
    public Spec getSpec() {
        Spec spec = super.getSpec();
        spec.addOption("fileDownlinkCommand", OptionType.STRING).withDefault("");
        spec.addOption("sourceFileNameArg", OptionType.STRING).withDefault("sourceFileName");
        spec.addOption("destFileNameArg", OptionType.STRING).withDefault("destFileName");
        spec.addOption("downlinkCommandArgs", OptionType.ANY);
        spec.addOption("listDirectoryCommand", OptionType.STRING).withDefault("");
        spec.addOption("listDirDirNameArg", OptionType.STRING).withDefault("dirName");
        spec.addOption("eventsStream", OptionType.STRING).withDefault("events_realtime");
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config)
            throws InitException {
        // The stock default would instantiate a second, unconfigured
        // CfdpService as the file-listing delegate; point it at this class.
        if (CfdpService.class.getName().equals(
                config.getString("fileListingServiceClassName", null))) {
            Map<String, Object> m = new HashMap<>(config.toMap());
            m.put("fileListingServiceClassName", getClass().getName());
            config = YConfiguration.wrap(m);
        }
        super.init(yamcsInstance, serviceName, config);
        this.fileDownlinkCommandName = config.getString("fileDownlinkCommand", "");
        this.sourceFileNameArg = config.getString("sourceFileNameArg", "sourceFileName");
        this.destFileNameArg = config.getString("destFileNameArg", "destFileName");
        this.downlinkCommandArgs = config.containsKey("downlinkCommandArgs")
                ? config.getMap("downlinkCommandArgs") : Map.of();
        this.listDirectoryCommandName = config.getString("listDirectoryCommand", "");
        this.listDirDirNameArg = config.getString("listDirDirNameArg", "dirName");
        this.eventsStreamName = config.getString("eventsStream", "events_realtime");

        this.monitorNotifier = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "FprimeCfdpService-listing-notifier");
            t.setDaemon(true);
            return t;
        });
        this.listingHandler = new RemoteFileListingHandler(
                AbstractFprimeFileTransferService.REMOTE_ENTITY_NAME, monitorNotifier);
    }

    @Override
    protected void doStart() {
        if (resolveProcessor()) {
            this.fileDownlinkCommand = findCommand(fileDownlinkCommandName, null);
            this.listDirectoryCommand = findCommand(listDirectoryCommandName, "ListDirectory");
            if (!fileDownlinkCommandName.isEmpty() && fileDownlinkCommand == null) {
                log.warn("CFDP downlink command '{}' not found in MDB; "
                        + "startDownload() will fail", fileDownlinkCommandName);
            }
            if (listDirectoryCommand == null) {
                log.warn("ListDirectory command '{}' not found in MDB; "
                        + "fetchFileList will fail", listDirectoryCommandName);
            }
        }

        this.eventsStream = YarchDatabase.getInstance(yamcsInstance)
                .getStream(eventsStreamName);
        if (eventsStream != null) {
            eventsStream.addSubscriber(listingHandler);
            log.info("Subscribed to {} for remote file listings", eventsStreamName);
        } else {
            log.warn("{} stream not found; fetchFileList will not "
                    + "be able to collect results", eventsStreamName);
        }

        this.listingSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FprimeCfdpService-listing-sweeper");
            t.setDaemon(true);
            return t;
        });
        this.listingSweeper.scheduleWithFixedDelay(
                listingHandler::expireStaleListings, 5, 5, TimeUnit.SECONDS);

        super.doStart();
    }

    @Override
    protected void doStop() {
        if (listingSweeper != null) {
            listingSweeper.shutdownNow();
        }
        if (eventsStream != null) {
            eventsStream.removeSubscriber(listingHandler);
        }
        if (monitorNotifier != null) {
            monitorNotifier.shutdown();
        }
        super.doStop();
    }

    // ------------------------------------------------------------------
    // Download initiation via F´ command
    // ------------------------------------------------------------------

    /**
     * Dispatch the configured F´ downlink command and return a record of
     * the request. The spacecraft-initiated transaction that follows is
     * tracked natively by the base class as an incoming transfer.
     */
    @Override
    public FileTransfer startDownload(String sourceEntity, String sourcePath,
                                      String destEntity, Bucket destBucket,
                                      String destPath, TransferOptions options) {
        if (fileDownlinkCommand == null) {
            throw new InvalidRequestException(
                    "No F´ CFDP downlink command configured; "
                            + "only spacecraft-initiated downlinks are supported");
        }
        if (sourcePath == null || sourcePath.isEmpty()) {
            throw new InvalidRequestException("sourcePath (file on spacecraft) is required");
        }
        if (destPath == null || destPath.isEmpty()) {
            destPath = sourcePath.contains("/")
                    ? sourcePath.substring(sourcePath.lastIndexOf('/') + 1)
                    : sourcePath;
        }
        try {
            Map<String, Object> args = new HashMap<>(downlinkCommandArgs);
            args.put(sourceFileNameArg, sourcePath);
            args.put(destFileNameArg, destPath);
            dispatchCommand(fileDownlinkCommand, args, getClass().getSimpleName(),
                    (int) (downloadIdSeq.get() & 0x7FFFFFFF));
        } catch (Exception e) {
            throw new InvalidRequestException(
                    "Failed to dispatch downlink command: " + e.getMessage());
        }
        FprimeFileTransfer transfer = new FprimeFileTransfer(
                downloadIdSeq.getAndIncrement(),
                destBucket != null ? destBucket.getName() : null,
                destPath, sourcePath, -1, TransferDirection.DOWNLOAD,
                TRANSFER_TYPE, false);
        transfer.setStartTime(System.currentTimeMillis());
        transfer.setState(TransferState.RUNNING);
        return transfer;
    }

    // ------------------------------------------------------------------
    // Remote file listing via F´ ListDirectory events
    // ------------------------------------------------------------------

    @Override
    public void fetchFileList(String localEntity, String remoteEntity,
                              String remotePath, Map<String, Object> options) {
        if (listDirectoryCommand == null) {
            throw new InvalidRequestException(
                    "Remote file listing unavailable: ListDirectory command not found in MDB");
        }
        String dirName = normalizeDirName(remotePath);
        log.info("fetchFileList: requesting F´ listing of {}", dirName);
        listingHandler.beginListing(dirName);
        try {
            Map<String, Object> args = new HashMap<>();
            args.put(listDirDirNameArg, dirName);
            dispatchCommand(listDirectoryCommand, args,
                    getClass().getSimpleName() + "-listing", 0);
        } catch (Exception e) {
            log.error("fetchFileList({}): failed to dispatch ListDirectory command",
                    dirName, e);
            listingHandler.failListing(dirName);
        }
    }

    @Override
    public ListFilesResponse getFileList(String localEntity, String remoteEntity,
                                         String remotePath, Map<String, Object> options) {
        return listingHandler.getFileList(normalizeDirName(remotePath));
    }

    @Override
    public void saveFileList(ListFilesResponse listing) {
        if (listing != null
                && !listing.getRemotePath().equals(normalizeDirName(listing.getRemotePath()))) {
            listing = listing.toBuilder()
                    .setRemotePath(normalizeDirName(listing.getRemotePath()))
                    .build();
        }
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

    private static String normalizeDirName(String remotePath) {
        return (remotePath == null || remotePath.isEmpty()) ? "." : remotePath;
    }

    // ------------------------------------------------------------------
    // Spacecraft command synthesis
    // ------------------------------------------------------------------

    private boolean resolveProcessor() {
        YamcsServerInstance ysi = YamcsServer.getServer().getInstance(yamcsInstance);
        this.processor = ysi == null ? null : ysi.getFirstProcessor();
        if (processor == null) {
            log.warn("No processor available; spacecraft command synthesis disabled");
            return false;
        }
        this.commandingManager = processor.getCommandingManager();
        this.systemUser = YamcsServer.getServer().getSecurityStore().getSystemUser();
        return true;
    }

    /** Find a MetaCommand by qualified name, or by suffix when unconfigured. */
    private MetaCommand findCommand(String configuredName, String suffix) {
        if (processor == null) {
            return null;
        }
        String name = configuredName;
        if ((name == null || name.isEmpty()) && suffix != null && !suffix.isEmpty()) {
            List<String> names = new ArrayList<>();
            for (MetaCommand cmd : processor.getMdb().getMetaCommands()) {
                names.add(cmd.getQualifiedName());
            }
            List<String> candidates =
                    AbstractFprimeFileTransferService.suffixCandidates(names, suffix);
            if (candidates.size() > 1) {
                log.warn("Multiple commands match suffix '{}': {}; refusing auto-discovery — "
                        + "configure the qualified command name explicitly", suffix, candidates);
                return null;
            }
            name = candidates.isEmpty() ? null : candidates.get(0);
        }
        return name == null || name.isEmpty() ? null : processor.getMdb().getMetaCommand(name);
    }

    private void dispatchCommand(MetaCommand command, Map<String, Object> args,
                                 String origin, int sequenceNumber) throws Exception {
        PreparedCommand pc = commandingManager.buildCommand(
                command, args, origin, sequenceNumber, systemUser);
        commandingManager.sendCommand(systemUser, pc);
    }
}
