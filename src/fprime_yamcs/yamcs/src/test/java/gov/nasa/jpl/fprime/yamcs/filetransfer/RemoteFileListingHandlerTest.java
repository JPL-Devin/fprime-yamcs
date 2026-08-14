package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.RemoteFile;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.protobuf.Db.Event;

public class RemoteFileListingHandlerTest {

    private final RemoteFileListingHandler handler = new RemoteFileListingHandler(Runnable::run);

    private static Tuple eventTuple(Event evt) {
        TupleDefinition tdef = new TupleDefinition();
        tdef.addColumn("body", DataType.protobuf(Event.class.getName()));
        return new Tuple(tdef, List.of(evt));
    }

    private void feedStructured(String type, Map<String, String> extra) {
        Event evt = Event.newBuilder()
                .setSource("test")
                .setSeqNumber(0)
                .setGenerationTime(0)
                .setReceptionTime(0)
                .setType(type)
                .setMessage("")
                .putAllExtra(extra)
                .build();
        handler.onTuple(null, eventTuple(evt));
    }

    @Test
    public void inProgressListingCountIsBounded() {
        for (int i = 0; i < RemoteFileListingHandler.MAX_CACHED_LISTINGS; i++) {
            handler.beginListing("/dir" + i, "fprime", "/dir" + i);
        }
        assertThrows(InvalidRequestException.class,
                () -> handler.beginListing("/overflow", "fprime", "/overflow"));
        // Refreshing an already-in-progress path is still allowed at the cap.
        handler.beginListing("/dir0", "fprime", "/dir0");
    }

    @Test
    public void listingAccumulatesAndCompletes() {
        handler.beginListing("/data", "fprime", "/data");
        feedStructured("DirectoryListing",
                Map.of("dirName", "/data", "fileName", "a.bin", "fileSize", "100"));
        feedStructured("DirectoryListingSubdir",
                Map.of("dirName", "/data", "subdirName", "sub"));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));

        ListFilesResponse listing = handler.getFileList("/data");
        assertEquals("completed", listing.getState());
        assertEquals(2, listing.getFilesCount());
        RemoteFile file = listing.getFiles(0);
        assertEquals("a.bin", file.getName());
        assertEquals(100, file.getSize());
        assertTrue(listing.getFiles(1).getIsDirectory());
    }

    @Test
    public void listingEchoesDestinationAndRequestedPath() {
        // yamcs-web only applies pushed listings whose destination/remotePath
        // match its dialog selection; root folder is requested as "".
        handler.beginListing(".", "fprime", "");
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "."));

        ListFilesResponse listing = handler.getFileList("");
        assertEquals("fprime", listing.getDestination());
        assertEquals("", listing.getRemotePath());
    }

    @Test
    public void fileNamesWithSpacesParse() {
        handler.beginListing("/data", "fprime", "/data");
        feedStructured("DirectoryListing",
                Map.of("dirName", "/data", "fileName", "my file.bin", "fileSize", "5"));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals("my file.bin", handler.getFileList("/data").getFiles(0).getName());
    }

    @Test
    public void eventsWithoutExtraIgnored() {
        handler.beginListing("/data", "fprime", "/data");
        Event evt = Event.newBuilder()
                .setSource("test")
                .setSeqNumber(0)
                .setGenerationTime(0)
                .setReceptionTime(0)
                .setType("DirectoryListing")
                .setMessage("[DirectoryListing] Directory /data: a.bin (3 bytes)")
                .build();
        handler.onTuple(null, eventTuple(evt));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals(0, handler.getFileList("/data").getFilesCount());
    }

    @Test
    public void errorTerminalFailsListing() {
        handler.beginListing("/data", "fprime", "/data");
        feedStructured("ListDirectoryError", Map.of("dirName", "/data", "status", "1"));
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void errorTerminalMatchesDirectoryNameWithSpaces() {
        handler.beginListing("/my data dir", "fprime", "/my data dir");
        feedStructured("ListDirectoryError", Map.of("dirName", "/my data dir", "status", "1"));
        assertEquals("failed", handler.getFileList("/my data dir").getState());
    }

    @Test
    public void cachedListingsAreBoundedLru() {
        for (int i = 0; i <= RemoteFileListingHandler.MAX_CACHED_LISTINGS; i++) {
            String dir = "/dir" + i;
            handler.beginListing(dir, "fprime", dir);
            feedStructured("ListDirectorySucceeded", Map.of("dirName", dir));
        }
        assertNull(handler.getFileList("/dir0"), "oldest cached listing must be evicted");
        assertEquals("completed", handler
                .getFileList("/dir" + RemoteFileListingHandler.MAX_CACHED_LISTINGS).getState());
    }

    @Test
    public void entriesForUnknownDirectoryIgnored() {
        handler.beginListing("/data", "fprime", "/data");
        feedStructured("DirectoryListing",
                Map.of("dirName", "/other", "fileName", "a.bin", "fileSize", "1"));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals(0, handler.getFileList("/data").getFilesCount());
        assertNull(handler.getFileList("/other"));
    }

    @Test
    public void incompleteExtraArgsIgnored() {
        handler.beginListing("/data", "fprime", "/data");
        feedStructured("DirectoryListing", Map.of("dirName", "/data"));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals(0, handler.getFileList("/data").getFilesCount());
    }

    @Test
    public void throwingMonitorDoesNotBlockOthers() {
        List<ListFilesResponse> seen = new ArrayList<>();
        RemoteFileListMonitor bad = l -> {
            throw new IllegalStateException("boom");
        };
        RemoteFileListMonitor good = seen::add;
        handler.registerMonitor(bad);
        handler.registerMonitor(good);

        handler.beginListing("/data", "fprime", "/data");
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals(1, seen.size());

        handler.unregisterMonitor(bad);
        handler.unregisterMonitor(good);
        assertTrue(handler.getMonitors().isEmpty());
    }

    @Test
    public void failListingFlipsInProgressListing() {
        handler.beginListing("/data", "fprime", "/data");
        handler.failListing("/data");
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void staleListingExpiresAsFailed() {
        handler.beginListing("/data", "fprime", "/data");
        handler.expireStaleListings(System.currentTimeMillis()
                + RemoteFileListingHandler.LISTING_EXPIRY_MS + 1);
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void freshListingSurvivesExpirySweep() {
        handler.beginListing("/data", "fprime", "/data");
        handler.expireStaleListings();
        assertNull(handler.getFileList("/data"), "listing still in progress");
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/data"));
        assertEquals("completed", handler.getFileList("/data").getState());
    }

    @Test
    public void saveFileListRoundTripsThroughCache() {
        ListFilesResponse listing = ListFilesResponse.newBuilder()
                .setRemotePath("/saved")
                .setState("success")
                .addFiles(RemoteFile.newBuilder().setName("a.bin").setSize(1))
                .build();
        handler.saveFileList(listing);
        assertEquals(listing, handler.getFileList("/saved"));

        handler.saveFileList(null);
        assertEquals(listing, handler.getFileList("/saved"), "null save must be a no-op");
    }
}
