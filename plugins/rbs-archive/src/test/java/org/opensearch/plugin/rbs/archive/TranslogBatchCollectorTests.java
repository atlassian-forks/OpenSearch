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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration-style tests for {@link TranslogBatchCollector} and the
 * {@link TranslogArchiveGcScanner} checkpoint-aware GC path.
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

    // ── Tests: gc_idx lifecycle ────────────────────────────────────────────────

    /**
     * After scanning and deleting a safe old TAR, the gc_idx blob for that minute
     * should also be cleaned up.
     */
    /**
     * After the GC scanner marks a minute-dir as safe and deletes its TARs,
     * the corresponding gc_idx entry should also be removed.
     * This validates the evictAndDeleteIdx() path in scanDay().
     */
    public void testGcIdxBlobDeletedAfterMinuteCleaned() throws Exception {
        String indexUUID = "idx-" + randomAlphaOfLength(8);

        // Old, safe TAR: maxSeqNo=5, globalCheckpoint=5 → checkpoint already covered
        Instant twoHoursAgo = Instant.now().minus(java.time.Duration.ofHours(2));
        List<TarArchiveBuilder.GcShardEntry> gc = List.of(new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 1L, 5L, 5L));
        uploadTar(twoHoursAgo, "node1", buildTar(indexUUID, 0, 1L, gc));

        // Run the scanner (with no live indices — scanner treats them as deleted → safe)
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, basePath);
        scanner.scan(twoHoursAgo.plusSeconds(1), java.util.Collections.emptySet());

        // After GC scan, all safe TARs should be deleted
        assertEquals("TAR deleted after safe GC", 0L, countTarBlobs());

        // The gc_idx blob for this minute should also be gone
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(basePath);
        String dayDir = TranslogArchivePathHelper.dayDir(twoHoursAgo);
        BlobPath gcDayPath = gcIdxRoot.add(dayDir);
        var gcIdxBlobs = blobStore.blobContainer(gcDayPath).listBlobs();
        assertEquals("gc_idx blob should be cleaned up after minute deletion", 0, gcIdxBlobs.size());
    }
}
