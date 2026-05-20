/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration-style tests for {@link TranslogBatchCollector} and the
 * {@link TarTranslogRemoteStoreStrategy#runArchiveGc} retention GC path.
 *
 * <p>Uses a real {@link FsBlobStore} backed by a temp directory so that uploads,
 * listing, and deletes hit the file system — no mocks for the storage layer.
 */
public class TranslogBatchCollectorTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private BlobStore blobStore;
    private TransferService transferService;
    private BlobPath basePath;

    private static final Logger LOG = LogManager.getLogger(TranslogBatchCollectorTests.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        blobStore = new FsBlobStore(1024, createTempDir(), false);
        transferService = new BlobStoreTransferService(blobStore, threadPool);
        // Use a unique base path per test to avoid cross-test pollution
        basePath = new BlobPath().add("base-" + randomAlphaOfLength(10));
    }

    /** Calls the static runTranslogGc directly with the real transferService. */
    private void runGc(Duration retention) throws IOException {
        TarTranslogRemoteStoreStrategy.runTranslogGc(transferService, basePath, retention, LOG);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Builds a minimal TAR blob (1 entry) with the given GC entries. */
    private static byte[] buildTar(String indexUUID, int shardId, long gen, List<TarArchiveBuilder.GcShardEntry> gcEntries)
        throws IOException {
        String tarPath = indexUUID + "/" + shardId + "/1/translog-" + gen + ".tlog";
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            TarArchiveBuilder.fromBytes(tarPath, ("gen-" + gen).getBytes(StandardCharsets.UTF_8))
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcEntries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        return out.toByteArray();
    }

    /** Uploads a TAR blob to the hierarchical archive path for the given instant. */
    private void uploadTar(Instant uploadTime, String nodeId, byte[] tarBytes) throws IOException {
        BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(basePath, uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, nodeId);
        transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(blobName, tarBytes, 0L), minutePath, WritePriority.HIGH);
    }

    /** Returns the count of TAR blobs under the txlog root. */
    private long countTarBlobs() throws IOException {
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(basePath);
        long count = 0;
        for (String dayDir : blobStore.blobContainer(txlogRoot).children().keySet()) {
            var dayContainer = blobStore.blobContainer(txlogRoot.add(dayDir));
            for (String minuteDir : dayContainer.children().keySet()) {
                var minuteContainer = blobStore.blobContainer(txlogRoot.add(dayDir).add(minuteDir));
                count += minuteContainer.listBlobs().keySet().stream().filter(n -> n.endsWith(".tar")).count();
            }
        }
        return count;
    }

    // ── Tests: timestamp-based GC (runArchiveGc / runTranslogGc) ─────────────

    /**
     * runArchiveGc uses pure timestamp-based deletion.
     * Old TAR (any GC entries) older than retention → DELETED regardless of checkpoint status.
     * This confirms runTranslogGc is timestamp-only (not checkpoint-gated).
     */
    public void testTimestampGcDeletesOldTarRegardlessOfCheckpoint() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // GC entry: maxSeqNo=10, globalCheckpoint=8 → checkpoint NOT covered (unsafe per scanner)
        // But runTranslogGc does timestamp-based only → should still delete it
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 5L, 10L, 8L));

        // Upload an old TAR (2 hours ago → past any retention window)
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
        uploadTar(twoHoursAgo, "node1", buildTar(indexUUID, 0, 5L, gcEntries));

        runGc(Duration.ofMinutes(1));

        assertEquals("Timestamp-based GC deletes old TARs regardless of checkpoint status", 0L, countTarBlobs());
    }

    /**
     * Retention GC: TAR where all shards are covered (globalCheckpoint >= maxSeqNo)
     * AND older than retention window → MUST be deleted.
     */
    public void testRetentionGcDeletesOldSafeTars() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);
        int shardId = 0;

        // GC entry: globalCheckpoint=10 >= maxSeqNo=10 → safe to delete
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, shardId, 5L, 10L, 10L));

        // Old TAR (2 hours ago)
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
        uploadTar(twoHoursAgo, "node1", buildTar(indexUUID, shardId, 5L, gcEntries));

        runGc(Duration.ofMinutes(1));

        assertEquals("Safe old TAR must be deleted by retention GC", 0L, countTarBlobs());
    }

    /**
     * Recent TARs must NOT be deleted regardless of checkpoint status.
     */
    public void testRetentionGcPreservesRecentTars() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // Safe entry
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 1L, 5L, 5L));

        // Recent TAR (10 seconds ago)
        Instant recent = Instant.now().minus(Duration.ofSeconds(10));
        uploadTar(recent, "node1", buildTar(indexUUID, 0, 1L, gcEntries));

        runGc(Duration.ofHours(1));

        assertEquals("Recent TAR must NOT be deleted by retention GC", 1L, countTarBlobs());
    }

    /**
     * Mixed scenario: one old safe TAR and one recent TAR.
     * Only the old safe one should be deleted.
     */
    public void testRetentionGcDeletesOldButPreservesRecent() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // Old TAR: 2 hours ago, safe (ckp >= maxSeq)
        List<TarArchiveBuilder.GcShardEntry> oldGc = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 1L, 5L, 5L));
        uploadTar(Instant.now().minus(Duration.ofHours(2)), "node1", buildTar(indexUUID, 0, 1L, oldGc));

        // Recent TAR: 30 seconds ago, safe
        List<TarArchiveBuilder.GcShardEntry> recentGc = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 6L, 10L, 10L));
        uploadTar(Instant.now().minus(Duration.ofSeconds(30)), "node1", buildTar(indexUUID, 0, 6L, recentGc));

        runGc(Duration.ofMinutes(1));

        assertEquals("Old safe TAR deleted, recent preserved → 1 remaining", 1L, countTarBlobs());
    }

    /**
     * GC with no archive blobs is a no-op (no errors thrown, count stays 0).
     */
    public void testRetentionGcOnEmptyArchiveIsNoop() throws Exception {
        TarTranslogRemoteStoreStrategy strategy = new TarTranslogRemoteStoreStrategy(
            new TranslogBatchCoordinator("node1", null, TimeValue.timeValueMinutes(1), 10)
        );
        // Should not throw
        strategy.runArchiveGc(basePath, Duration.ofMinutes(1));
        assertEquals("Empty archive: no blobs to delete", 0L, countTarBlobs());
    }

    /**
     * Multiple shards in same TAR: timestamp-based GC deletes the TAR if expired,
     * regardless of how many shards are in it.
     */
    public void testTimestampGcDeletesMultiShardTarIfExpired() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // Two shards: one covered, one not — timestamp GC doesn't care
        List<TarArchiveBuilder.GcShardEntry> gcEntries = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 1L, 5L, 5L),    // covered
            new TarArchiveBuilder.GcShardEntry(indexUUID, 1, 1L, 10L, 3L)    // not covered
        );

        uploadTar(Instant.now().minus(Duration.ofHours(2)), "node1", buildTar(indexUUID, 0, 1L, gcEntries));

        runGc(Duration.ofMinutes(1));

        assertEquals("Timestamp-based GC deletes expired TARs even with multi-shard content", 0L, countTarBlobs());
    }

    /**
     * GC of multiple old TARs in the same minute-dir: all safe ones deleted.
     */
    public void testRetentionGcDeletesMultipleTarsInSameMinute() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));

        for (int gen = 1; gen <= 3; gen++) {
            List<TarArchiveBuilder.GcShardEntry> gc = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, gen, gen + 2L, gen + 2L));
            // Use slightly different node IDs to get different blob names
            uploadTar(twoHoursAgo.plusSeconds(gen), "node" + gen, buildTar(indexUUID, 0, gen, gc));
        }

        runGc(Duration.ofMinutes(1));

        assertEquals("All 3 safe old TARs must be deleted", 0L, countTarBlobs());
    }

    // ── Tests: gc_idx lifecycle ────────────────────────────────────────────────

    /**
     * After scanning and deleting a safe old TAR, the gc_idx blob for that minute
     * should also be cleaned up.
     */
    public void testGcIdxBlobDeletedAfterMinuteCleaned() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // Old, safe TAR
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
        List<TarArchiveBuilder.GcShardEntry> gc = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 1L, 5L, 5L));
        uploadTar(twoHoursAgo, "node1", buildTar(indexUUID, 0, 1L, gc));

        runGc(Duration.ofMinutes(1));

        // After GC, there should be no TARs
        assertEquals("TAR deleted after safe GC", 0L, countTarBlobs());

        // The gc_idx blob for this minute should also be gone
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(basePath);
        String dayDir = TranslogArchivePathHelper.dayDir(twoHoursAgo);
        BlobPath gcDayPath = gcIdxRoot.add(dayDir);
        var gcIdxBlobs = blobStore.blobContainer(gcDayPath).listBlobs();
        assertEquals("gc_idx blob should be cleaned up after minute deletion", 0, gcIdxBlobs.size());
    }
}
