package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.buckets.Bucket;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.FileTransferCapabilities;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

import gov.nasa.jpl.fprime.yamcs.packet.CfdpPdu;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * Class-1 (unacknowledged) CFDP file transfer to and from the spacecraft,
 * carrying CFDP PDUs inside CCSDS space packets on a dedicated APID. This is
 * the CFDP counterpart of {@link FprimeFilePacketService} and shares its
 * transport and service infrastructure:
 *
 * <ul>
 *   <li><b>Uplink</b>: {@link #startUpload} reads a bucket object and hands
 *       it to {@link CfdpUplinkHandler}, which emits a Metadata / File
 *       Data×N / EOF PDU sequence through the configured
 *       {@link UplinkTransport}. Any YAMCS TC data link works — a CCSDS TC
 *       frame virtual channel (TM/TC pipeline) or a raw space packet link.
 *   <li><b>Downlink</b>: subscribes to a TM stream, filters for the CFDP
 *       APID, and delegates PDU reassembly, checksum verification, and
 *       bucket storage to {@link CfdpDownlinkHandler}. Downlinks are either
 *       unsolicited (spacecraft initiated) or triggered by
 *       {@link #startDownload} when a spacecraft downlink command is
 *       configured in the MDB.
 * </ul>
 *
 * <p>Protocol subset: class-1 (unacknowledged) transactions, small
 * (32-bit) file sizes, 1-byte entity ids, 2-byte transaction sequence
 * numbers, modular checksum. No retransmission, pause, resume, or cancel.
 *
 * <p>Configured under {@code services:} in the instance configuration:
 * <pre>
 *   - class: gov.nasa.jpl.fprime.yamcs.filetransfer.CfdpFileTransferService
 *     args:
 *       inStream: tm_realtime          # default
 *       bucket: cfdpFiles              # incoming bucket
 *       cfdpApid: 5                    # APID carrying CFDP PDUs
 *       localEntityId: 1               # ground CFDP entity id
 *       remoteEntityId: 2              # spacecraft CFDP entity id
 *       uplinkLink: UDP_TC_OUT.vc1     # YAMCS TC link to route through
 *       uplinkChunkSize: 128           # file bytes per File Data PDU
 *       interPacketDelayMs: 20         # pacing delay between uplink packets
 *       downlinkMirrorDir: /tmp/fprime-downlink  # local mirror (default)
 *       maxFileSize: 268435456         # downlink allocation cap in bytes
 *       fileDownlinkCommand: ""        # optional F´ command for startDownload
 *       sourceFileNameArg: sourceFileName  # downlink-command source-path argument name
 *       destFileNameArg: destFileName      # downlink-command destination-path argument name
 *       downloadTimeoutMs: 30000       # max wait for the spacecraft Metadata PDU
 * </pre>
 *
 * <p>Design note: this service deliberately does not reuse the built-in
 * {@code org.yamcs.cfdp.CfdpService}, which expects raw CFDP PDUs on
 * dedicated {@code cfdp_in}/{@code cfdp_out} streams. Here PDUs are
 * encapsulated in CCSDS space packets on a configurable APID and travel
 * through the ordinary F´ TM/TC pipelines (or a raw space-packet link) via
 * the shared {@link UplinkTransport}, so the F´ and CFDP services share one
 * uplink/downlink infrastructure.
 */
public class CfdpFileTransferService extends AbstractFprimeFileTransferService
        implements StreamSubscriber {

    // 256 MiB: bounds what a corrupt/malicious Metadata PDU can allocate.
    private static final int DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

    private static final int DEFAULT_CFDP_APID = 5;

    private static final String TRANSFER_TYPE = "CFDP";

    // A reassembly with no PDU activity for this long is failed by the
    // sweeper, releasing its buffer.
    private static final long INFLIGHT_STALL_TIMEOUT_MS = 60_000;

    // Configuration
    private String inStreamName;
    private String bucketName;
    private int cfdpApid;
    private int maxFileSize;
    private Path downlinkMirrorDir;
    private String uplinkLinkName;
    private int uplinkChunkSize;
    private long interPacketDelayMs;
    private int localEntityId;
    private int remoteEntityId;
    private String fileDownlinkCommandName;
    private String sourceFileNameArg;
    private String destFileNameArg;
    private long downloadTimeoutMs;

    // Runtime
    private Stream inStream;
    private CfdpDownlinkHandler downlinkHandler;
    private CfdpUplinkHandler uplinkHandler;
    private ExecutorService uplinkExecutor;
    // Storage worker: completed downlink files are written to the bucket and
    // mirror off the TM stream subscriber thread.
    private ExecutorService storageExecutor;
    private ScheduledExecutorService timeoutScheduler;
    private MetaCommand fileDownlinkCommand;   // may be null if not in MDB

    // Pending startDownload() transfers keyed by the destination file name
    // the spacecraft will echo in its Metadata PDU.
    private final Map<String, FprimeFileTransfer> pendingDownloadsByPath = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------
    // Spec / configuration
    // ------------------------------------------------------------------

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("inStream", OptionType.STRING).withDefault("tm_realtime");
        spec.addOption("bucket", OptionType.STRING).withDefault("cfdpFiles");
        spec.addOption("cfdpApid", OptionType.INTEGER).withDefault(DEFAULT_CFDP_APID);
        spec.addOption("localEntityId", OptionType.INTEGER).withDefault(1);
        spec.addOption("remoteEntityId", OptionType.INTEGER).withDefault(2);
        spec.addOption("maxFileSize", OptionType.INTEGER).withDefault(DEFAULT_MAX_FILE_SIZE);
        // Mirroring defaults on (matching FprimeFilePacketService); set to
        // "" to disable local filesystem mirroring of downlinked files.
        spec.addOption("downlinkMirrorDir", OptionType.STRING)
                .withDefault("/tmp/fprime-downlink");
        spec.addOption("uplinkLink", OptionType.STRING).withDefault("UDP_TC_OUT.vc1");
        spec.addOption("uplinkChunkSize", OptionType.INTEGER).withDefault(128);
        spec.addOption("interPacketDelayMs", OptionType.INTEGER).withDefault(20);
        // Optional: qualified name of a spacecraft command that initiates a
        // CFDP downlink of (sourceFileNameArg, destFileNameArg). When empty,
        // startDownload() is rejected and only unsolicited downlinks work.
        spec.addOption("fileDownlinkCommand", OptionType.STRING).withDefault("");
        spec.addOption("sourceFileNameArg", OptionType.STRING).withDefault("sourceFileName");
        spec.addOption("destFileNameArg", OptionType.STRING).withDefault("destFileName");
        spec.addOption("downloadTimeoutMs", OptionType.INTEGER).withDefault(30000);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config)
            throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.inStreamName = config.getString("inStream", "tm_realtime");
        this.bucketName = config.getString("bucket", "cfdpFiles");
        this.cfdpApid = config.getInt("cfdpApid", DEFAULT_CFDP_APID);
        this.localEntityId = config.getInt("localEntityId", 1);
        this.remoteEntityId = config.getInt("remoteEntityId", 2);
        this.maxFileSize = config.getInt("maxFileSize", DEFAULT_MAX_FILE_SIZE);
        String mirror = config.getString("downlinkMirrorDir", "/tmp/fprime-downlink");
        this.downlinkMirrorDir = mirror.isEmpty() ? null : Paths.get(mirror);
        this.uplinkLinkName = config.getString("uplinkLink", "UDP_TC_OUT.vc1");
        this.uplinkChunkSize = config.getInt("uplinkChunkSize", 128);
        this.interPacketDelayMs = config.getLong("interPacketDelayMs", 20L);
        this.fileDownlinkCommandName = config.getString("fileDownlinkCommand", "");
        this.sourceFileNameArg = config.getString("sourceFileNameArg", "sourceFileName");
        this.destFileNameArg = config.getString("destFileNameArg", "destFileName");
        this.downloadTimeoutMs = config.getLong("downloadTimeoutMs", 30000L);
        if (cfdpApid < 0 || cfdpApid > SpacePacket.MAX_APID) {
            throw new InitException("cfdpApid " + cfdpApid + " outside [0, "
                    + SpacePacket.MAX_APID + "]");
        }
        // Fail at startup, not on the first transfer: the codec encodes
        // one-byte entity ids.
        if (localEntityId < 0 || localEntityId > 0xFF
                || remoteEntityId < 0 || remoteEntityId > 0xFF) {
            throw new InitException("entity ids " + localEntityId + "/"
                    + remoteEntityId + " outside [0, 255]");
        }

        log.info("CfdpFileTransferService init: inStream={} bucket={} cfdpApid={}"
                + " entities={}->{} uplinkLink={} chunk={}B downlinkMirror={}",
                inStreamName, bucketName, cfdpApid, localEntityId, remoteEntityId,
                uplinkLinkName, uplinkChunkSize, downlinkMirrorDir);
    }

    @Override
    protected long localApiEntityId() {
        return localEntityId;
    }

    @Override
    protected long remoteApiEntityId() {
        return remoteEntityId;
    }

    @Override
    protected void addCapabilities(FileTransferCapabilities.Builder b) {
        b.setUpload(true)
         .setDownload(true)
         .setRemotePath(true)
         .setFileList(false)      // no listing protocol in class-1 CFDP
         .setHasTransferType(false)
         .setPauseResume(false);
    }

    // ------------------------------------------------------------------
    // Service lifecycle
    // ------------------------------------------------------------------

    @Override
    protected void doStart() {
        try {
            YarchDatabaseInstance yarch = YarchDatabase.getInstance(yamcsInstance);
            this.inStream = yarch.getStream(inStreamName);
            if (this.inStream == null) {
                notifyFailed(new IllegalStateException("Stream not found: " + inStreamName));
                return;
            }

            Bucket bucket = getOrCreateBucket(bucketName);

            UplinkTransport transport = TcLinkUplinkTransport.resolve(
                    yamcsInstance, uplinkLinkName, getClass().getSimpleName(),
                    interPacketDelayMs);
            this.uplinkHandler = new CfdpUplinkHandler(
                    transport, cfdpApid, uplinkChunkSize,
                    localEntityId, remoteEntityId, transferListener);
            this.storageExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "CfdpFileTransferService-storage");
                t.setDaemon(true);
                return t;
            });
            this.downlinkHandler = new CfdpDownlinkHandler(
                    bucket, downlinkMirrorDir, maxFileSize,
                    localEntityId, remoteEntityId,
                    this::resolveDownlinkTransfer, transferListener, storageExecutor);

            // Uplink worker: single-threaded so transactions serialize and
            // the PDU stream stays ordered.
            this.uplinkExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "CfdpFileTransferService-uplink");
                t.setDaemon(true);
                return t;
            });

            this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "CfdpFileTransferService-timeout");
                t.setDaemon(true);
                return t;
            });
            this.timeoutScheduler.scheduleWithFixedDelay(
                    this::sweepPendingDownloadTimeouts, 5, 5, TimeUnit.SECONDS);

            if (!fileDownlinkCommandName.isEmpty() && resolveProcessor()) {
                this.fileDownlinkCommand = findCommand(fileDownlinkCommandName, null);
                if (fileDownlinkCommand == null) {
                    log.warn("CFDP downlink command '{}' not found in MDB; "
                            + "startDownload() will fail", fileDownlinkCommandName);
                }
            }

            this.inStream.addSubscriber(this);
            log.info("CfdpFileTransferService started: subscribed to {}, APID {}",
                    inStreamName, cfdpApid);
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
        }
        if (uplinkExecutor != null) {
            uplinkExecutor.shutdownNow();
        }
        if (inStream != null) {
            inStream.removeSubscriber(this);
        }
        if (storageExecutor != null) {
            // Drain in-flight bucket writes before failing leftovers so a
            // finishing store and the shutdown sweep don't race on state.
            storageExecutor.shutdown();
            try {
                storageExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // No packets or uplink work can progress after the executors stop;
        // flip every remaining non-terminal transfer to FAILED so nothing
        // stays QUEUED/RUNNING forever.
        pendingDownloadsByPath.clear();
        failNonTerminalTransfers("service stopped");
        notifyStopped();
    }

    // ------------------------------------------------------------------
    // StreamSubscriber: called for every packet on the TM stream
    // ------------------------------------------------------------------

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        Object packetCol = tuple.getColumn("packet");
        if (!(packetCol instanceof byte[])) {
            return;
        }
        byte[] bytes = (byte[]) packetCol;
        if (bytes.length < SpacePacket.PRIMARY_HEADER_LEN + CfdpPdu.minimumLength()) {
            return;  // Too short to be a CFDP PDU; some other APID.
        }
        if (SpacePacket.apid(bytes) != cfdpApid) {
            return;  // Not a CFDP packet.
        }
        // Trim trailing padding beyond the CCSDS-declared length so it can
        // never be parsed as PDU content.
        int declared = SpacePacket.declaredLength(bytes);
        if (bytes.length > declared) {
            bytes = Arrays.copyOf(bytes, declared);
        }
        downlinkHandler.handlePdu(bytes, SpacePacket.PRIMARY_HEADER_LEN);
    }

    @Override
    public void streamClosed(Stream stream) {
        log.info("Stream {} closed", stream.getName());
    }

    /**
     * Attach an API-level transfer record to a newly started downlink: a
     * pending startDownload() transfer keyed by the destination file name,
     * or a fresh record for an unsolicited (spacecraft initiated)
     * transaction.
     */
    private FprimeFileTransfer resolveDownlinkTransfer(String sourceFileName,
                                                       String destinationFileName, int fileSize) {
        FprimeFileTransfer transfer = pendingDownloadsByPath.remove(destinationFileName);
        if (transfer == null) {
            transfer = new FprimeFileTransfer(nextTransferId(), bucketName,
                    destinationFileName, sourceFileName, fileSize,
                    TransferDirection.DOWNLOAD, TRANSFER_TYPE, false);
            transfer.setEntityIds(localEntityId, remoteEntityId);
            transfer.setStartTime(System.currentTimeMillis());
            addTransfer(transfer);
            log.info("Unsolicited CFDP downlink; created transfer record id={}",
                    transfer.getId());
        } else {
            transfer.setTotalSize(fileSize);
        }
        return transfer;
    }

    /**
     * Scheduled task: fail any pending download whose start time is older
     * than {@code downloadTimeoutMs} — the "spacecraft never sent a
     * Metadata PDU" case.
     */
    private void sweepPendingDownloadTimeouts() {
        downlinkHandler.expireInflight(INFLIGHT_STALL_TIMEOUT_MS);
        long now = System.currentTimeMillis();
        for (Map.Entry<String, FprimeFileTransfer> entry :
                new ArrayList<>(pendingDownloadsByPath.entrySet())) {
            FprimeFileTransfer t = entry.getValue();
            long age = now - t.getStartTime();
            if (age < downloadTimeoutMs) {
                continue;
            }
            if (!pendingDownloadsByPath.remove(entry.getKey(), t)) {
                continue;
            }
            log.warn("CFDP download timeout: id={} remotePath={} after {} ms — "
                    + "no Metadata PDU received", t.getId(), t.getRemotePath(), age);
            String reason = "timeout after " + age + " ms: no Metadata PDU for '"
                    + t.getRemotePath() + "'";
            t.setFailureReason(reason);
            t.setState(TransferState.FAILED);
            notifyStateChanged(t);
            publishVerifierAck(t, AckStatus.TIMEOUT, reason);
        }
    }

    // ------------------------------------------------------------------
    // FileTransferService: upload / download
    // ------------------------------------------------------------------

    @Override
    public FileTransfer startUpload(String sourceEntity, Bucket sourceBucket,
                                    String objectName, String destinationEntity,
                                    String remotePath, TransferOptions options)
            throws IOException {
        if (sourceBucket == null) {
            throw new InvalidRequestException("sourceBucket is required");
        }
        if (objectName == null || objectName.isEmpty()) {
            throw new InvalidRequestException("objectName is required");
        }
        // Bounded wait so a stalled storage backend cannot pin the API
        // thread indefinitely.
        byte[] content = fetchObject(sourceBucket, objectName);
        if (content == null) {
            throw new InvalidRequestException(
                    "No such object '" + objectName + "' in bucket " + sourceBucket.getName());
        }
        if (content.length > maxFileSize) {
            throw new InvalidRequestException("Object '" + objectName + "' is "
                    + content.length + " bytes, larger than maxFileSize " + maxFileSize);
        }

        String dest = (remotePath == null || remotePath.isEmpty()) ? objectName : remotePath;
        FprimeFileTransfer transfer = new FprimeFileTransfer(
                nextTransferId(), sourceBucket.getName(), objectName, dest,
                content.length, TransferDirection.UPLOAD, TRANSFER_TYPE, false);
        transfer.setEntityIds(localEntityId, remoteEntityId);
        submitUplink(uplinkExecutor, transfer, () -> uplinkHandler.run(transfer, content));
        return transfer;
    }

    @Override
    public FileTransfer startDownload(String sourceEntity, String sourcePath,
                                      String destEntity, Bucket destBucket,
                                      String destPath, TransferOptions options)
            throws IOException {
        if (fileDownlinkCommand == null) {
            throw new InvalidRequestException("No CFDP downlink command configured; "
                    + "only unsolicited (spacecraft initiated) downlinks are supported");
        }
        if (sourcePath == null || sourcePath.isEmpty()) {
            throw new InvalidRequestException("sourcePath (file on spacecraft) is required");
        }
        if (destBucket == null) {
            throw new InvalidRequestException("destBucket is required");
        }
        if (destPath == null || destPath.isEmpty()) {
            destPath = sourcePath.contains("/")
                    ? sourcePath.substring(sourcePath.lastIndexOf('/') + 1)
                    : sourcePath;
        }

        long id = nextTransferId();
        FprimeFileTransfer transfer = new FprimeFileTransfer(
                id, destBucket.getName(), destPath, sourcePath, -1,
                TransferDirection.DOWNLOAD, TRANSFER_TYPE, false);
        transfer.setEntityIds(localEntityId, remoteEntityId);
        transfer.setStartTime(System.currentTimeMillis());
        // Reject rather than overwrite: a displaced pending transfer could
        // never be resolved or timed out and would sit RUNNING forever.
        if (pendingDownloadsByPath.putIfAbsent(destPath, transfer) != null) {
            throw new InvalidRequestException(
                    "A download to '" + destPath + "' is already pending");
        }
        addTransfer(transfer);
        notifyStateChanged(transfer);

        try {
            Map<String, Object> args = new HashMap<>();
            args.put(sourceFileNameArg, sourcePath);
            args.put(destFileNameArg, destPath);
            CommandId cmdId = dispatchCommand(fileDownlinkCommand, args,
                    getClass().getSimpleName(), (int) (id & 0x7FFFFFFF));
            transfer.setTriggeringCommandId(cmdId);
            publishVerifierAck(transfer, AckStatus.SCHEDULED,
                    "waiting for spacecraft Metadata PDU");
            log.info("CFDP downlink START: id={} source={} (on spacecraft) -> bucket {}/{}",
                    id, sourcePath, destBucket.getName(), destPath);
        } catch (Exception e) {
            log.error("Failed to dispatch CFDP downlink command for transfer {}", id, e);
            pendingDownloadsByPath.remove(destPath, transfer);
            failTransfer(transfer, AckStatus.NOK, "command dispatch failed: " + e.getMessage());
            throw new IOException("Failed to dispatch downlink command: " + e.getMessage(), e);
        }
        return transfer;
    }

    @Override
    public void pause(FileTransfer transfer) {
        throw new UnsupportedOperationException("Pause not supported by class-1 CFDP");
    }

    @Override
    public void resume(FileTransfer transfer) {
        throw new UnsupportedOperationException("Resume not supported by class-1 CFDP");
    }

    @Override
    public void cancel(FileTransfer transfer) {
        throw new UnsupportedOperationException(
                "Cancel not supported; class-1 CFDP transactions are fire-and-forget");
    }

    @Override
    public void fetchFileList(String localEntity, String remoteEntity,
                              String remotePath, Map<String, Object> options) {
        // Matches the declared capability (setFileList(false)): reject
        // rather than silently no-op if a client calls it anyway.
        throw new InvalidRequestException("File listing is not supported by class-1 CFDP");
    }

    @Override
    public ListFilesResponse getFileList(String localEntity, String remoteEntity,
                                         String remotePath, Map<String, Object> options) {
        throw new InvalidRequestException("File listing is not supported by class-1 CFDP");
    }

    @Override
    public void saveFileList(ListFilesResponse listing) {
        throw new InvalidRequestException("File listing is not supported by class-1 CFDP");
    }
}
