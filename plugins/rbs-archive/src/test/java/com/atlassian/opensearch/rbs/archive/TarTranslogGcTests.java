/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link TarTranslogRemoteStoreStrategy#runTranslogGc}.
 *
 * <p>Uses an in-memory fake {@link TransferService} to avoid any I/O,
 * with TAR blob names generated via {@link TranslogArchivePathHelper} to
 * ensure name parsing is end-to-end correct.
 */
public class TarTranslogGcTests extends OpenSearchTestCase {

    private static final Logger LOG = LogManager.getLogger(TarTranslogGcTests.class);

    // ── Fake TransferService ──────────────────────────────────────────────────

    /**
     * Minimal in-memory TransferService that supports listFolders, listAll, deleteBlobs.
     * Blobs are stored as "day/minute/blobName".
     */
    private static final class FakeTransferService implements TransferService {
        // Blob store: path (joined with '/') → blob name → dummy data
        private final Map<String, Set<String>> blobs = new ConcurrentHashMap<>();

        void put(String dayDir, String minuteDir, String blobName) {
            String key = dayDir + "/" + minuteDir;
            blobs.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(blobName);
        }

        Set<String> allBlobs() {
            return blobs.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
        }

        private String pathKey(Iterable<String> path) {
            List<String> parts = new ArrayList<>();
            // strip leading "txlog" if present
            boolean seenTxlog = false;
            for (String p : path) {
                if (!seenTxlog && "txlog".equals(p)) { seenTxlog = true; continue; }
                parts.add(p);
            }
            return String.join("/", parts);
        }

        @Override public Set<String> listFolders(Iterable<String> path) {
            String prefix = pathKey(path);
            Set<String> result = new LinkedHashSet<>();
            for (String key : blobs.keySet()) {
                if (prefix.isEmpty()) {
                    // top-level: return day dirs
                    result.add(key.split("/")[0]);
                } else if (key.startsWith(prefix + "/")) {
                    String rest = key.substring((prefix + "/").length());
                    if (!rest.contains("/")) result.add(rest);
                }
            }
            return result;
        }

        @Override public Set<String> listAll(Iterable<String> path) {
            String key = pathKey(path);
            return blobs.getOrDefault(key, Set.of());
        }

        @Override public void deleteBlobs(Iterable<String> path, List<String> fileNames) {
            String key = pathKey(path);
            Set<String> set = blobs.get(key);
            if (set != null) {
                set.removeAll(fileNames);
                if (set.isEmpty()) blobs.remove(key);
            }
        }

        // ── Unimplemented (not needed for GC tests) ───────────────────────────
        @Override public void uploadBlob(String tp, org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot snap,
                                         Iterable<String> path, ActionListener<org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot> l,
                                         WritePriority prio) { fail("not expected"); }
        @Override public void uploadBlobs(Set<org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot> snaps,
                                          Map<Long, BlobPath> paths, ActionListener<org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot> l,
                                          WritePriority prio) { fail("not expected"); }
        @Override public void uploadBlob(org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot snap,
                                         Iterable<String> path, WritePriority prio) throws IOException { fail("not expected"); }
        @Override public void uploadBlob(InputStream is, Iterable<String> path, String fileName,
                                         WritePriority prio, ActionListener<Void> l) throws IOException { fail("not expected"); }
        @Override public void delete(Iterable<String> path) throws IOException { fail("not expected"); }
        @Override public void deleteAsync(String tp, Iterable<String> path, ActionListener<Void> l) { fail("not expected"); }
        @Override public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException { fail("not expected"); return null; }
        @Override public InputStream downloadBlob(Iterable<String> path, String fileName, long position, long length) throws IOException { fail("not expected"); return null; }
        @Override public org.opensearch.common.blobstore.InputStreamWithMetadata downloadBlobWithMetadata(Iterable<String> path, String fileName) throws IOException { fail("not expected"); return null; }
        @Override public void listFoldersAsync(String tp, Iterable<String> path, ActionListener<Set<String>> l) { fail("not expected"); }
        @Override public void listAllInSortedOrder(Iterable<String> path, String prefix, int limit, ActionListener<List<BlobMetadata>> l) { fail("not expected"); }
        @Override public void listAllInSortedOrderAsync(String tp, Iterable<String> path, String prefix, int limit, ActionListener<List<BlobMetadata>> l) { fail("not expected"); }
        @Override public void deleteBlobsAsync(String tp, Iterable<String> path, List<String> fileNames, ActionListener<Void> l) { fail("not expected"); }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Extracts the day-dir (yyyyMMdd) component from the given instant using
     * {@link TranslogArchivePathHelper#tarBlobDir} (which internally uses UTC).
     */
    private static String dayDir(Instant instant) {
        BlobPath dir = TranslogArchivePathHelper.tarBlobDir(new BlobPath(), instant);
        // tarBlobDir produces ["txlog", "yyyyMMdd", "HHmm"] — element index 1 is day
        String[] parts = dir.toArray();
        return parts[1]; // yyyyMMdd
    }

    private static String minuteDir(Instant instant) {
        BlobPath dir = TranslogArchivePathHelper.tarBlobDir(new BlobPath(), instant);
        String[] parts = dir.toArray();
        return parts[2]; // HHmm
    }

    private static String tarBlobName(Instant instant, String nodeId) {
        return TranslogArchivePathHelper.tarBlobName(instant, nodeId);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /** Blobs older than retention are deleted; recent blobs are kept. */
    public void testDeletesExpiredKeepsRecent() throws IOException {
        FakeTransferService ts = new FakeTransferService();
        BlobPath base = new BlobPath();

        Instant now = Instant.now();
        Duration retention = Duration.ofHours(2);

        // Old blob — 3 hours ago → expired (use distinct node IDs to avoid same-name collision)
        Instant old = now.minus(Duration.ofHours(3));
        String oldBlobName = tarBlobName(old, "aabb1122");
        ts.put(dayDir(old), minuteDir(old), oldBlobName);

        // Recent blob — 1 hour ago → keep (different node ID so blob name is always distinct)
        Instant recent = now.minus(Duration.ofHours(1));
        String recentBlobName = tarBlobName(recent, "ccdd3344");
        ts.put(dayDir(recent), minuteDir(recent), recentBlobName);

        TarTranslogRemoteStoreStrategy.runTranslogGc(ts, base, retention, LOG);

        Set<String> remaining = ts.allBlobs();
        assertEquals("Only 1 blob should remain", 1, remaining.size());
        assertFalse("Expired blob should be gone", remaining.contains(oldBlobName));
        assertTrue("Recent blob should survive", remaining.contains(recentBlobName));
    }

    /** Multiple blobs in same minute-dir — all expired → dir cleaned up. */
    public void testCleansUpEntireMinuteDir() throws IOException {
        FakeTransferService ts = new FakeTransferService();
        BlobPath base = new BlobPath();

        Instant now = Instant.now();
        Duration retention = Duration.ofHours(1);
        Instant old = now.minus(Duration.ofHours(5));

        ts.put(dayDir(old), minuteDir(old), tarBlobName(old, "node1"));
        ts.put(dayDir(old), minuteDir(old), tarBlobName(old.plusSeconds(1), "node2"));

        TarTranslogRemoteStoreStrategy.runTranslogGc(ts, base, retention, LOG);

        assertTrue("All blobs should be deleted", ts.allBlobs().isEmpty());
    }

    /** No blobs to delete → GC completes without error. */
    public void testNoOpWhenNothingExpired() throws IOException {
        FakeTransferService ts = new FakeTransferService();
        BlobPath base = new BlobPath();

        Instant now = Instant.now();
        Duration retention = Duration.ofHours(24);
        Instant recent = now.minus(Duration.ofMinutes(30));

        ts.put(dayDir(recent), minuteDir(recent), tarBlobName(recent, "aabb1122"));

        TarTranslogRemoteStoreStrategy.runTranslogGc(ts, base, retention, LOG);

        assertEquals("No blobs should be deleted", 1, ts.allBlobs().size());
    }

    /** Empty store → GC returns without error. */
    public void testNoOpWhenStoreEmpty() throws IOException {
        FakeTransferService ts = new FakeTransferService();
        TarTranslogRemoteStoreStrategy.runTranslogGc(ts, new BlobPath(), Duration.ofHours(1), LOG);
        assertTrue("Store should still be empty", ts.allBlobs().isEmpty());
    }

    /** null TransferService → skips silently. */
    public void testSkipsWhenTransferServiceNull() throws IOException {
        // Should not throw
        TarTranslogRemoteStoreStrategy.runTranslogGc(null, new BlobPath(), Duration.ofHours(1), LOG);
    }

    /** Non-TAR files in the hierarchy are not deleted. */
    public void testIgnoresNonTarFiles() throws IOException {
        FakeTransferService ts = new FakeTransferService();
        BlobPath base = new BlobPath();

        Instant now = Instant.now();
        Instant old = now.minus(Duration.ofHours(10));
        String dayDir = dayDir(old);
        String minuteDir = minuteDir(old);

        ts.put(dayDir, minuteDir, "README.txt");
        ts.put(dayDir, minuteDir, "metadata.json");

        TarTranslogRemoteStoreStrategy.runTranslogGc(ts, base, Duration.ofHours(1), LOG);

        // Non-TAR files should not be deleted
        Set<String> remaining = ts.allBlobs();
        assertTrue("README.txt should survive", remaining.contains("README.txt"));
        assertTrue("metadata.json should survive", remaining.contains("metadata.json"));
    }
}
