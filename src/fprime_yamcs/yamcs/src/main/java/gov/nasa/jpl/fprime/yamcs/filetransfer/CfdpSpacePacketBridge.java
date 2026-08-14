package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.AbstractYamcsService;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

import gov.nasa.jpl.fprime.yamcs.packet.FilePacket;
import gov.nasa.jpl.fprime.yamcs.packet.SpacePacket;

/**
 * Bridges CFDP PDUs between the F´ CCSDS space-packet pipelines and the
 * {@code cfdp_in}/{@code cfdp_out} streams consumed by YAMCS's native
 * {@link org.yamcs.cfdp.CfdpService}, so the built-in CFDP implementation
 * handles all protocol logic.
 *
 * <p><b>Downlink</b>: subscribes to a TM stream, filters for the CFDP APID,
 * strips the space-packet header (and the F´ {@code FW_PACKET_FILE}
 * descriptor CfdpManager prepends), and emits the raw PDU on
 * {@code cfdp_in}.
 *
 * <p><b>Uplink</b>: subscribes to {@code cfdp_out}, frames each PDU behind
 * the {@code FW_PACKET_FILE} descriptor inside a space packet on the CFDP
 * APID, and sends it through the configured {@link UplinkTransport}.
 *
 * <p>Creates {@code cfdp_in}/{@code cfdp_out} at init when absent, so list
 * this service <b>before</b> the CFDP service in the instance configuration:
 * <pre>
 *   - class: gov.nasa.jpl.fprime.yamcs.filetransfer.CfdpSpacePacketBridge
 *     args:
 *       tmStream: tm_realtime          # default
 *       cfdpApid: 16                   # must match the spacecraft CFDP APID
 *       uplinkLink: UDP_TC_OUT.vc1     # YAMCS TC link to route through
 *       interPacketDelayMs: 20         # pacing delay between uplink packets
 *       cfdpInStream: cfdp_in          # default
 *       cfdpOutStream: cfdp_out        # default
 * </pre>
 */
public class CfdpSpacePacketBridge extends AbstractYamcsService
        implements StreamSubscriber {

    // Outside the APIDs assigned by the F´ default ComCfg (0-5); must match
    // the spacecraft-side CFDP APID and be overridden if 16 is in use.
    private static final int DEFAULT_CFDP_APID = 16;

    // Shortest decodable CFDP PDU: 4-byte fixed header fields plus one-byte
    // entity ids and sequence number.
    static final int MIN_PDU_LEN = 7;

    private static final TupleDefinition CFDP_TDEF = new TupleDefinition();
    static {
        CFDP_TDEF.addColumn("gentime", DataType.TIMESTAMP);
        CFDP_TDEF.addColumn("entityId", DataType.LONG);
        CFDP_TDEF.addColumn("seqNum", DataType.INT);
        CFDP_TDEF.addColumn("pdu", DataType.BINARY);
    }

    // Configuration
    private String tmStreamName;
    private String cfdpInStreamName;
    private String cfdpOutStreamName;
    private int cfdpApid;
    private String uplinkLinkName;
    private long interPacketDelayMs;

    // Runtime
    private Stream tmStream;
    private Stream cfdpIn;
    private Stream cfdpOut;
    private UplinkTransport transport;
    private ExecutorService uplinkExecutor;
    private StreamSubscriber cfdpOutSubscriber;
    // CCSDS sequence count (14-bit, wraps; may be re-patched by a link
    // postprocessor).
    private final AtomicInteger seqCount = new AtomicInteger();

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("tmStream", OptionType.STRING).withDefault("tm_realtime");
        spec.addOption("cfdpInStream", OptionType.STRING).withDefault("cfdp_in");
        spec.addOption("cfdpOutStream", OptionType.STRING).withDefault("cfdp_out");
        spec.addOption("cfdpApid", OptionType.INTEGER).withDefault(DEFAULT_CFDP_APID);
        spec.addOption("uplinkLink", OptionType.STRING).withDefault("UDP_TC_OUT.vc1");
        spec.addOption("interPacketDelayMs", OptionType.INTEGER).withDefault(20);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config)
            throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.tmStreamName = config.getString("tmStream", "tm_realtime");
        this.cfdpInStreamName = config.getString("cfdpInStream", "cfdp_in");
        this.cfdpOutStreamName = config.getString("cfdpOutStream", "cfdp_out");
        this.cfdpApid = config.getInt("cfdpApid", DEFAULT_CFDP_APID);
        this.uplinkLinkName = config.getString("uplinkLink", "UDP_TC_OUT.vc1");
        this.interPacketDelayMs = config.getLong("interPacketDelayMs", 20L);
        if (cfdpApid < 0 || cfdpApid > SpacePacket.MAX_APID) {
            throw new InitException("cfdpApid " + cfdpApid + " outside [0, "
                    + SpacePacket.MAX_APID + "]");
        }

        // Created at init (not doStart) so the CFDP service listed after
        // this one finds them during its own init.
        try {
            YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);
            createStreamIfAbsent(ydb, cfdpInStreamName);
            createStreamIfAbsent(ydb, cfdpOutStreamName);
        } catch (Exception e) {
            throw new InitException("Failed to create CFDP streams", e);
        }
    }

    private static void createStreamIfAbsent(YarchDatabaseInstance ydb, String name)
            throws Exception {
        if (ydb.getStream(name) == null) {
            ydb.execute("create stream " + name + CFDP_TDEF.getStringDefinition());
        }
    }

    @Override
    protected void doStart() {
        try {
            YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);
            this.tmStream = ydb.getStream(tmStreamName);
            if (tmStream == null) {
                notifyFailed(new IllegalStateException("Stream not found: " + tmStreamName));
                return;
            }
            this.cfdpIn = ydb.getStream(cfdpInStreamName);
            this.cfdpOut = ydb.getStream(cfdpOutStreamName);

            this.transport = TcLinkUplinkTransport.resolve(
                    yamcsInstance, uplinkLinkName, getClass().getSimpleName(),
                    interPacketDelayMs);

            // Single-threaded so the outgoing PDU stream stays ordered.
            this.uplinkExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "CfdpSpacePacketBridge-uplink");
                t.setDaemon(true);
                return t;
            });

            this.cfdpOutSubscriber = new StreamSubscriber() {
                @Override
                public void onTuple(Stream stream, Tuple tuple) {
                    Object pdu = tuple.getColumn("pdu");
                    if (pdu instanceof byte[]) {
                        uplinkExecutor.submit(() -> uplinkPdu((byte[]) pdu));
                    }
                }

                @Override
                public void streamClosed(Stream stream) {
                    log.info("Stream {} closed", stream.getName());
                }
            };
            cfdpOut.addSubscriber(cfdpOutSubscriber);
            tmStream.addSubscriber(this);
            log.info("CfdpSpacePacketBridge started: {} <-> {}/{} on APID {}",
                    tmStreamName, cfdpInStreamName, cfdpOutStreamName, cfdpApid);
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        if (tmStream != null) {
            tmStream.removeSubscriber(this);
        }
        if (cfdpOut != null && cfdpOutSubscriber != null) {
            cfdpOut.removeSubscriber(cfdpOutSubscriber);
        }
        if (uplinkExecutor != null) {
            uplinkExecutor.shutdownNow();
        }
        notifyStopped();
    }

    // ------------------------------------------------------------------
    // Downlink: TM stream -> cfdp_in
    // ------------------------------------------------------------------

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        Object packetCol = tuple.getColumn("packet");
        if (!(packetCol instanceof byte[])) {
            return;
        }
        byte[] pdu = extractPdu((byte[]) packetCol, cfdpApid);
        if (pdu != null) {
            ArrayList<Object> cols = new ArrayList<>();
            cols.add(TimeEncoding.getWallclockTime());
            cols.add(0L);
            cols.add(0);
            cols.add(pdu);
            cfdpIn.emitTuple(new Tuple(CFDP_TDEF, cols));
        }
    }

    /**
     * Extract the raw CFDP PDU from a TM packet on {@code apid}: trim to the
     * CCSDS-declared length so trailing frame padding is never parsed as PDU
     * content, then skip the space-packet header and the optional F´
     * {@code FW_PACKET_FILE} descriptor. Returns null for non-CFDP packets.
     */
    static byte[] extractPdu(byte[] bytes, int apid) {
        if (bytes.length < SpacePacket.PRIMARY_HEADER_LEN + MIN_PDU_LEN) {
            return null;  // Too short to be a CFDP PDU; some other APID.
        }
        if (SpacePacket.apid(bytes) != apid) {
            return null;  // Not a CFDP packet.
        }
        int declared = SpacePacket.declaredLength(bytes);
        int end = Math.min(bytes.length, declared);
        int off = SpacePacket.PRIMARY_HEADER_LEN;
        if (end - off >= FilePacket.DESCRIPTOR_LEN + MIN_PDU_LEN
                && FilePacket.isFilePacket(bytes, off)) {
            off += FilePacket.DESCRIPTOR_LEN;
        }
        return Arrays.copyOfRange(bytes, off, end);
    }

    // ------------------------------------------------------------------
    // Uplink: cfdp_out -> space packet -> TC link
    // ------------------------------------------------------------------

    private void uplinkPdu(byte[] pdu) {
        try {
            transport.send(wrapPdu(pdu, cfdpApid,
                    seqCount.getAndUpdate(s -> (s + 1) & 0x3FFF)));
        } catch (Exception e) {
            log.error("Failed to uplink CFDP PDU ({} bytes)", pdu.length, e);
        }
    }

    /**
     * Frame a PDU behind the F´ {@code FW_PACKET_FILE} descriptor inside a
     * telecommand space packet; CfdpManager routes uplink by that descriptor.
     */
    static byte[] wrapPdu(byte[] pdu, int apid, int seq) {
        byte[] framed = new byte[FilePacket.DESCRIPTOR_LEN + pdu.length];
        framed[0] = (byte) (FilePacket.FILE_DESCRIPTOR >> 8);
        framed[1] = (byte) FilePacket.FILE_DESCRIPTOR;
        System.arraycopy(pdu, 0, framed, FilePacket.DESCRIPTOR_LEN, pdu.length);
        return SpacePacket.wrapTelecommand(framed, apid, seq);
    }
}
