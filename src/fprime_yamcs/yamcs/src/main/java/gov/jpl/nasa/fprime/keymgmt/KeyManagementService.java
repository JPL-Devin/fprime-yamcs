package gov.jpl.nasa.fprime.keymgmt;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.AbstractYamcsService;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.events.EventProducer;
import org.yamcs.events.EventProducerFactory;
import org.yamcs.http.HttpServer;
import org.yamcs.management.LinkManager;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.security.sdls.SdlsSecurityAssociation;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.ccsds.AbstractTcFrameLink;
import org.yamcs.tctm.ccsds.AbstractTmFrameLink;
import org.yamcs.tctm.ccsds.TcPacketHandler;

/**
 * Optional SDLS session-key management service (CCSDS 355.1-B-1 compliant
 * OTAR with an ML-KEM-768 master tier).
 *
 * <p>A rekey performs, in order:
 * <ol>
 *   <li>ML-KEM-768 encapsulation against the pre-loaded spacecraft public
 *       key; the shared secret becomes the transaction master key (KEK)
 *   <li>KEM Establishment space packets on {@code kemApid} carrying the
 *       (fragmented) ciphertext, so the spacecraft can decapsulate the
 *       same KEK
 *   <li>OTAR Command PDU (355.1-B-1 Annex D baseline mode) on
 *       {@code epApid} carrying a fresh AES-256 session key wrapped with
 *       AES-256-GCM under the KEK
 *   <li>Key Activation Command PDU
 *   <li>Key Verification Command PDU (best-effort; the flight side reply
 *       path is under development, keys report "unverified" until then)
 *   <li>Installation of the session key into the configured ground SDLS
 *       Security Associations so both ends stay synchronized
 * </ol>
 *
 * <p>Not enabled by default. Add to the instance configuration:
 * <pre>
 *   - class: gov.jpl.nasa.fprime.keymgmt.KeyManagementService
 *     args:
 *       publicKeyFile: etc/mlkem768-pub.pem
 *       uplinkLink: UDP_TC_OUT.vc1
 *       sdlsTargets:
 *         - link: UDP_TC_OUT
 *           spi: 1
 * </pre>
 */
public class KeyManagementService extends AbstractYamcsService {

    private static final SecureRandom RANDOM = new SecureRandom();

    // Configuration
    private Path publicKeyFile;
    private int kemApid;
    private int epApid;
    private String uplinkLinkName;
    private int kemFragmentSize;
    private int masterKeyId;
    private int firstSessionKeyId;
    private String opensslBinary;
    private int sdlsInstallDelayMs;
    private List<SdlsTarget> sdlsTargets;

    // Runtime
    private MlKem768 mlkem;
    private TcPacketHandler uplinkLink;
    private EventProducer eventProducer;
    private final KeyInventory inventory = new KeyInventory();
    private final AtomicInteger nextSessionKeyId = new AtomicInteger();
    private final AtomicInteger packetSeqCount = new AtomicInteger();
    private int uplinkCmdSeq = 0;
    private volatile RekeyStatus lastStatus;

    /** One ground SDLS Security Association to update on rekey. */
    static final class SdlsTarget {
        final String linkName;
        final short spi;

        SdlsTarget(String linkName, short spi) {
            this.linkName = linkName;
            this.spi = spi;
        }
    }

    /** Outcome of the most recent rekey attempt, for the status API. */
    public static final class RekeyStatus {
        public final boolean success;
        public final int sessionKeyId;
        public final String message;
        public final long timestampMillis;

        RekeyStatus(boolean success, int sessionKeyId, String message) {
            this.success = success;
            this.sessionKeyId = sessionKeyId;
            this.message = message;
            this.timestampMillis = System.currentTimeMillis();
        }
    }

    @Override
    public Spec getSpec() {
        Spec targetSpec = new Spec();
        targetSpec.addOption("link", OptionType.STRING).withRequired(true);
        targetSpec.addOption("spi", OptionType.INTEGER).withRequired(true);

        Spec spec = new Spec();
        spec.addOption("publicKeyFile", OptionType.STRING).withRequired(true);
        spec.addOption("kemApid", OptionType.INTEGER).withDefault(0x20);
        spec.addOption("epApid", OptionType.INTEGER).withDefault(0x21);
        spec.addOption("uplinkLink", OptionType.STRING).withDefault("UDP_TC_OUT.vc1");
        // KEM ciphertext bytes per packet; each packet must fit the TC frame limit
        spec.addOption("kemFragmentSize", OptionType.INTEGER).withDefault(512);
        spec.addOption("masterKeyId", OptionType.INTEGER).withDefault(1);
        // Annex D reserves session key IDs 0-127 for master keys
        spec.addOption("firstSessionKeyId", OptionType.INTEGER).withDefault(128);
        spec.addOption("opensslBinary", OptionType.STRING).withDefault("openssl");
        // Wait for queued rekey packets to be framed under the OLD key before
        // installing the new one into the ground SAs
        spec.addOption("sdlsInstallDelayMs", OptionType.INTEGER).withDefault(500);
        spec.addOption("sdlsTargets", OptionType.LIST).withElementType(OptionType.MAP)
                .withSpec(targetSpec).withDefault(List.of());
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        publicKeyFile = Paths.get(config.getString("publicKeyFile"));
        kemApid = config.getInt("kemApid", 0x20);
        epApid = config.getInt("epApid", 0x21);
        uplinkLinkName = config.getString("uplinkLink", "UDP_TC_OUT.vc1");
        kemFragmentSize = config.getInt("kemFragmentSize", 512);
        masterKeyId = config.getInt("masterKeyId", 1);
        firstSessionKeyId = config.getInt("firstSessionKeyId", 128);
        opensslBinary = config.getString("opensslBinary", "openssl");
        sdlsInstallDelayMs = config.getInt("sdlsInstallDelayMs", 500);
        nextSessionKeyId.set(firstSessionKeyId);

        sdlsTargets = new ArrayList<>();
        for (YConfiguration target : config.getConfigList("sdlsTargets")) {
            sdlsTargets.add(new SdlsTarget(target.getString("link"), (short) target.getInt("spi")));
        }

        mlkem = new MlKem768(opensslBinary);
        try {
            mlkem.verifyAvailable();
        } catch (Exception e) {
            throw new InitException("OpenSSL check failed: " + e.getMessage());
        }
    }

    @Override
    protected void doStart() {
        eventProducer = EventProducerFactory.getEventProducer(yamcsInstance, "KeyManagement", 10000);

        LinkManager linkManager = YamcsServer.getServer().getInstance(yamcsInstance).getLinkManager();
        Link link = linkManager.getLink(uplinkLinkName);
        if (!(link instanceof TcPacketHandler)) {
            notifyFailed(new IllegalStateException("Link " + uplinkLinkName
                    + (link == null ? " not found" : " is not a TcPacketHandler")));
            return;
        }
        uplinkLink = (TcPacketHandler) link;

        for (SdlsTarget target : sdlsTargets) {
            if (resolveSa(linkManager, target) == null) {
                notifyFailed(new IllegalStateException(
                        "No SDLS SA with SPI " + target.spi + " on link " + target.linkName));
                return;
            }
        }

        HttpServer httpServer = YamcsServer.getServer().getGlobalService(HttpServer.class);
        if (httpServer != null) {
            httpServer.addRoute("keymgmt", () -> new KeyManagementHandler(this));
        } else {
            log.warn("No HttpServer available; key management UI/API disabled");
        }
        notifyStarted();
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }

    /**
     * Execute a full session rekey. Serialized: concurrent triggers queue.
     *
     * @return the resulting status (also retrievable via {@link #getLastStatus})
     */
    public synchronized RekeyStatus rekey() {
        int sessionKeyId = nextSessionKeyId.getAndIncrement();
        byte[] sessionKey = new byte[EpPduBuilder.SESSION_KEY_LENGTH];
        MlKem768.Encapsulation encapsulation = null;
        try {
            // 1. Establish transaction KEK via ML-KEM-768
            encapsulation = mlkem.encapsulate(publicKeyFile);
            for (byte[] fragment : KemPacketBuilder.buildFragments(masterKeyId,
                    encapsulation.getCiphertext(), kemFragmentSize)) {
                sendPacket(kemApid, fragment);
            }

            // 2. OTAR: upload fresh session key under the KEK
            RANDOM.nextBytes(sessionKey);
            byte[] otar = EpPduBuilder.buildOtar(masterKeyId, encapsulation.getSharedSecret(),
                    sessionKeyId, sessionKey);
            sendPacket(epApid, otar);
            inventory.register(sessionKeyId);

            // 3. Activate
            sendPacket(epApid, EpPduBuilder.buildKeyActivation(sessionKeyId));
            inventory.transition(sessionKeyId, KeyInventory.KeyState.ACTIVE);

            // 4. Verify (best-effort: no reply path yet, stays "unverified")
            byte[] challenge = new byte[EpPduBuilder.CHALLENGE_LENGTH];
            sendPacket(epApid, EpPduBuilder.buildKeyVerification(sessionKeyId, challenge));

            // 5. Install into ground SDLS so both ends use the new key.
            // Let the queued rekey packets drain first: they must be framed
            // under the old key, which the spacecraft still holds.
            if (!sdlsTargets.isEmpty() && sdlsInstallDelayMs > 0) {
                Thread.sleep(sdlsInstallDelayMs);
            }
            LinkManager linkManager = YamcsServer.getServer().getInstance(yamcsInstance).getLinkManager();
            for (SdlsTarget target : sdlsTargets) {
                SdlsSecurityAssociation sa = resolveSa(linkManager, target);
                if (sa == null) {
                    throw new IllegalStateException(
                            "SDLS SA disappeared: SPI " + target.spi + " on " + target.linkName);
                }
                sa.setSecretKey(sessionKey.clone());
            }

            eventProducer.sendInfo(String.format(
                    "Session rekey complete: key ID %d uploaded, activated, and installed in %d ground SA(s)",
                    sessionKeyId, sdlsTargets.size()));
            lastStatus = new RekeyStatus(true, sessionKeyId, "Rekey complete (verification pending)");
        } catch (Exception e) {
            log.error("Rekey failed", e);
            eventProducer.sendWarning("Session rekey failed: " + e.getMessage());
            lastStatus = new RekeyStatus(false, sessionKeyId, "Rekey failed: " + e.getMessage());
        } finally {
            Arrays.fill(sessionKey, (byte) 0);
            if (encapsulation != null) {
                encapsulation.destroy();
            }
        }
        return lastStatus;
    }

    public KeyInventory getInventory() {
        return inventory;
    }

    public RekeyStatus getLastStatus() {
        return lastStatus;
    }

    private static SdlsSecurityAssociation resolveSa(LinkManager linkManager, SdlsTarget target) {
        Link link = linkManager.getLink(target.linkName);
        if (link instanceof AbstractTcFrameLink l) {
            return l.getSdls(target.spi);
        } else if (link instanceof AbstractTmFrameLink l) {
            return l.getSdls(target.spi);
        }
        return null;
    }

    /** Wrap the payload in a space packet and send it through the TC link. */
    private void sendPacket(int apid, byte[] payload) {
        byte[] spacePacket = SpacePackets.buildTcPacket(apid, packetSeqCount.getAndIncrement(), payload);
        CommandId cmdId = CommandId.newBuilder()
                .setGenerationTime(System.currentTimeMillis())
                .setOrigin("KeyManagementService")
                .setSequenceNumber(uplinkCmdSeq++)
                .setCommandName("KeyManagementService/keyPacket")
                .build();
        PreparedCommand pc = new PreparedCommand(cmdId);
        pc.setBinary(spacePacket);
        if (!uplinkLink.sendCommand(pc)) {
            throw new IllegalStateException("TC link " + uplinkLinkName + " rejected the packet");
        }
    }
}
