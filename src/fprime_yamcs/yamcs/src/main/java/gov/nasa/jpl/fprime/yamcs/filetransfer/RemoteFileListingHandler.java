package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.google.protobuf.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.RemoteFile;
// Note: the events_realtime stream carries the *internal* Db.Event
// protobuf, NOT org.yamcs.protobuf.Event (the external API type).
// They are wire-incompatible classes with the same field names.
import org.yamcs.yarch.protobuf.Db.Event;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;

/**
 * Collects F´ {@code Svc::FileManager} directory-listing events from the
 * YAMCS events stream into {@link ListFilesResponse}s, backing the remote
 * file browser of the YAMCS File Transfer UI.
 *
 * <p>Mirrors CfdpService's pattern: an in-progress accumulator per directory
 * is flipped into the listing cache on the terminal event, notifying
 * {@link RemoteFileListMonitor} subscribers.
 */
public class RemoteFileListingHandler implements StreamSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteFileListingHandler.class);

    /**
     * Maximum entries retained per in-progress listing. Entries beyond this
     * are dropped (with a warning) so a misbehaving or spoofed event stream
     * cannot grow ground-server memory without bound.
     */
    static final int MAX_LISTING_ENTRIES = 100_000;

    /**
     * Age after which an in-progress listing whose terminal event never
     * arrived (dropped packet, F´ reset, ...) is expired and flipped to
     * failed, bounding how long an accumulator can live.
     */
    static final long LISTING_EXPIRY_MS = 5 * 60 * 1000L;

    /**
     * Maximum cached completed listings. Least-recently-used entries are
     * evicted past this, so listing many unique remote paths cannot grow
     * ground-server memory without bound.
     */
    static final int MAX_CACHED_LISTINGS = 64;

    private final String remoteEntityName;
    // Listing-monitor callbacks are dispatched here rather than on the
    // events-stream subscriber thread, so a slow monitor cannot stall
    // event processing (same pattern as transfer-monitor notification).
    private final Executor monitorNotifier;
    private final Map<String, ListingAccumulator> inProgressListings = new ConcurrentHashMap<>();
    private final Map<String, ListFilesResponse> fileListCache = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(
                        Map.Entry<String, ListFilesResponse> eldest) {
                    return size() > MAX_CACHED_LISTINGS;
                }
            });
    private final Set<RemoteFileListMonitor> monitors = new CopyOnWriteArraySet<>();

    public RemoteFileListingHandler(String remoteEntityName, Executor monitorNotifier) {
        this.remoteEntityName = remoteEntityName;
        this.monitorNotifier = monitorNotifier;
    }

    // ------------------------------------------------------------------
    // Listing lifecycle, called by the owning service
    // ------------------------------------------------------------------

    /**
     * Create/refresh the accumulator for a directory. If a prior listing was
     * in progress for the same path, it is discarded — the caller is asking
     * for a fresh view.
     */
    public void beginListing(String dirName) {
        expireStaleListings();
        // Bound concurrent accumulators (symmetric with MAX_CACHED_LISTINGS)
        // so repeated fetches of distinct paths cannot grow the map until
        // the expiry sweep. Serialized so concurrent callers cannot slip
        // past the bound (removals elsewhere only shrink the map).
        synchronized (inProgressListings) {
            if (!inProgressListings.containsKey(dirName)
                    && inProgressListings.size() >= MAX_CACHED_LISTINGS) {
                throw new InvalidRequestException("Too many listings in progress ("
                        + MAX_CACHED_LISTINGS + "); wait for one to complete or expire");
            }
            inProgressListings.put(dirName, new ListingAccumulator(dirName));
        }
    }

    /**
     * Expire in-progress listings older than {@link #LISTING_EXPIRY_MS},
     * flipping them to failed. Called opportunistically from
     * {@link #beginListing} and periodically by the owning service's
     * timeout sweeper.
     */
    public void expireStaleListings() {
        expireStaleListings(System.currentTimeMillis());
    }

    /** Clock-injectable variant of {@link #expireStaleListings()}. */
    void expireStaleListings(long now) {
        for (Map.Entry<String, ListingAccumulator> entry :
                new ArrayList<>(inProgressListings.entrySet())) {
            if (now - entry.getValue().createdAt > LISTING_EXPIRY_MS) {
                LOG.warn("Expiring stale listing of {} (no terminal event after {} ms)",
                        ObjectNames.forLog(entry.getKey()), LISTING_EXPIRY_MS);
                completeListing(entry.getKey(), "failed");
            }
        }
    }

    /** Terminate an in-progress listing as failed (e.g. command dispatch error). */
    public void failListing(String dirName) {
        completeListing(dirName, "failed");
    }

    public ListFilesResponse getFileList(String dirName) {
        return fileListCache.get(dirName);
    }

    public void saveFileList(ListFilesResponse listing) {
        if (listing == null) {
            return;
        }
        fileListCache.put(listing.getRemotePath(), listing);
    }

    public void registerMonitor(RemoteFileListMonitor monitor) {
        monitors.add(monitor);
    }

    public void unregisterMonitor(RemoteFileListMonitor monitor) {
        monitors.remove(monitor);
    }

    public Set<RemoteFileListMonitor> getMonitors() {
        return new HashSet<>(monitors);
    }

    public void notifyMonitors(ListFilesResponse listing) {
        List<RemoteFileListMonitor> recipients = new ArrayList<>(monitors);
        try {
            monitorNotifier.execute(() -> {
                for (RemoteFileListMonitor m : recipients) {
                    try {
                        m.receivedFileList(listing);
                    } catch (Exception e) {
                        LOG.warn("RemoteFileListMonitor threw", e);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.debug("Dropping listing notification after shutdown", e);
        }
    }

    // ------------------------------------------------------------------
    // Event stream subscriber — drives the listing state machine
    // ------------------------------------------------------------------

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        Object body = tuple.getColumn("body");
        if (!(body instanceof Event)) {
            return;
        }
        Event evt = (Event) body;
        String type = evt.getType();
        if (type == null) {
            return;
        }
        int dot = type.lastIndexOf('.');
        if (dot >= 0) {
            type = type.substring(dot + 1);
        }

        // Event arguments come from the structured Event.extra map populated
        // by fprime-yamcs-events.
        Map<String, String> extra = evt.getExtraMap();
        if (extra == null || extra.isEmpty()) {
            return;
        }
        String msg = evt.getMessage();

        try {
            switch (type) {
                case "DirectoryListing":
                    onDirectoryListing(extra);
                    break;
                case "DirectoryListingSubdir":
                    onDirectoryListingSubdir(extra);
                    break;
                case "ListDirectoryStarted":
                    // Informational — the accumulator was already created by
                    // beginListing(). Nothing to do.
                    break;
                case "ListDirectorySucceeded":
                    onListDirectoryTerminal(extra, true);
                    break;
                case "ListDirectoryError":
                    onListDirectoryTerminal(extra, false);
                    break;
                default:
                    // not a listing event
            }
        } catch (Exception e) {
            LOG.warn("Error processing event type={} msg={}",
                    ObjectNames.forLog(type), ObjectNames.forLog(msg), e);
        }
    }

    @Override
    public void streamClosed(Stream stream) {
        LOG.info("Stream {} closed", stream.getName());
    }

    private void onDirectoryListing(Map<String, String> extra) {
        String dir = extra.get("dirName");
        String file = extra.get("fileName");
        String sizeStr = extra.get("fileSize");
        if (dir == null || file == null || sizeStr == null) {
            return;
        }
        long size = Long.parseLong(sizeStr);
        ListingAccumulator acc = inProgressListings.get(dir);
        if (acc != null) {
            acc.addFile(file, size);
        }
    }

    private void onDirectoryListingSubdir(Map<String, String> extra) {
        String dir = extra.get("dirName");
        String subdir = extra.get("subdirName");
        if (dir == null || subdir == null) {
            return;
        }
        ListingAccumulator acc = inProgressListings.get(dir);
        if (acc != null) {
            acc.addSubdir(subdir);
        }
    }

    private void onListDirectoryTerminal(Map<String, String> extra, boolean succeeded) {
        String dir = extra.get("dirName");
        if (dir != null) {
            completeListing(dir, succeeded ? "completed" : "failed");
        }
    }

    /**
     * Build the final ListFilesResponse for a completed (or failed) listing,
     * move it into the cache, and notify monitors.
     */
    private void completeListing(String dir, String state) {
        ListingAccumulator acc = inProgressListings.remove(dir);
        if (acc == null) {
            LOG.debug("completeListing({}): no accumulator (listing not ours?)",
                    ObjectNames.forLog(dir));
            return;
        }
        ListFilesResponse response = acc.build(state);
        fileListCache.put(dir, response);
        LOG.info("Listing of {} {}: {} entries",
                ObjectNames.forLog(dir), state, response.getFilesCount());
        notifyMonitors(response);
    }

    /**
     * Collects file and subdirectory entries for a single in-progress
     * directory listing. Flipped to a ListFilesResponse when the terminal
     * event arrives.
     */
    private final class ListingAccumulator {
        private final String dirName;
        private final List<RemoteFile> entries = new ArrayList<>();
        final long createdAt = System.currentTimeMillis();

        ListingAccumulator(String dirName) {
            this.dirName = dirName;
        }

        synchronized void addFile(String name, long size) {
            if (!hasCapacity()) {
                return;
            }
            entries.add(RemoteFile.newBuilder()
                    .setName(name)
                    .setIsDirectory(false)
                    .setSize(size)
                    .build());
        }

        synchronized void addSubdir(String name) {
            if (!hasCapacity()) {
                return;
            }
            entries.add(RemoteFile.newBuilder()
                    .setName(name)
                    .setIsDirectory(true)
                    .setSize(0)
                    .build());
        }

        private boolean hasCapacity() {
            if (entries.size() >= MAX_LISTING_ENTRIES) {
                LOG.warn("Listing of {} exceeds {} entries; dropping further entries",
                        ObjectNames.forLog(dirName), MAX_LISTING_ENTRIES);
                return false;
            }
            return true;
        }

        synchronized ListFilesResponse build(String state) {
            long nowMs = System.currentTimeMillis();
            return ListFilesResponse.newBuilder()
                    .setRemotePath(dirName)
                    .setDestination(remoteEntityName)
                    .setState(state)
                    .setListTime(Timestamp.newBuilder()
                            .setSeconds(nowMs / 1000)
                            .setNanos((int) ((nowMs % 1000) * 1_000_000))
                            .build())
                    .addAllFiles(entries)
                    .build();
        }
    }
}
