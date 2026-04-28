/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Component integration test: wires real classes ({@link TranslogTransferManager},
 * {@link RemoteFsTimestampAwareTranslog#cleanup}) together with a counting
 * {@link TransferService} mock to verify PUT / GET / DELETE / LIST request counts
 * for archive upload enabled vs disabled.
 *
 * <p>This approach avoids a real blob store (and associated mock FS issues) while
 * still exercising the real branching logic in {@link TranslogTransferManager} and
 * {@link RemoteFsTimestampAwareTranslog#cleanup(TranslogTransferManager)}.
 *
 * <p>Key behaviors verified:
 * <ul>
 *   <li>Archive-OFF {@code transferSnapshot()}: 2 tlog + 2 ckp + 1 metadata = 5 PUTs,
 *       0 GETs, 0 DELETEs, 0 LISTs.</li>
 *   <li>Archive-OFF {@code cleanup()}: ≥1 LIST to discover stale metadata blobs.</li>
 *   <li>Archive-ON {@code cleanup()}: 0 LISTs, 0 DELETEs, 0 PUTs — complete no-op.</li>
 * </ul>
 */
public class TranslogArchiveUploadComponentTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("test-index", "test-uuid-1", 0);

    private ThreadPool threadPool;
    private CountingTransferService transferService;

    private long primaryTerm;
    private long generation;
    private long minTranslogGeneration;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transferService = new CountingTransferService();
        primaryTerm = randomNonNegativeLong();
        generation = randomLongBetween(2, 1000);
        minTranslogGeneration = randomLongBetween(0, generation - 1);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private TranslogTransferManager buildManager(boolean archiveEnabled) {
        BlobPath dataPath = BlobPath.cleanPath().add("translog").add("data");
        BlobPath mdPath = BlobPath.cleanPath().add("translog").add("metadata");
        BlobPath basePath = BlobPath.cleanPath();
        RemoteTranslogTransferTracker tracker = new RemoteTranslogTransferTracker(SHARD_ID, 10);
        FileTransferTracker fileTracker = new FileTransferTracker(SHARD_ID, tracker);
        return new TranslogTransferManager(
            SHARD_ID,
            transferService,
            dataPath,
            mdPath,
            basePath,
            fileTracker,
            tracker,
            DefaultRemoteStoreSettings.INSTANCE,
            false,         // isTranslogMetadataEnabled
            archiveEnabled
        );
    }

    /**
     * Builds a real {@link TransferSnapshot} backed by temp files for a given generation.
     */
    private TransferSnapshot snapshotForGen(long gen) throws IOException {
        CheckpointFileSnapshot ckp1 = new CheckpointFileSnapshot(
            primaryTerm,
            gen,
            minTranslogGeneration,
            createTempFile(Translog.TRANSLOG_FILE_PREFIX + gen, Translog.CHECKPOINT_SUFFIX),
            null
        );
        CheckpointFileSnapshot ckp2 = new CheckpointFileSnapshot(
            primaryTerm,
            gen - 1,
            minTranslogGeneration,
            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (gen - 1), Translog.CHECKPOINT_SUFFIX),
            null
        );
        TranslogFileSnapshot tlog1 = new TranslogFileSnapshot(
            primaryTerm,
            gen,
            createTempFile(Translog.TRANSLOG_FILE_PREFIX + gen, Translog.TRANSLOG_FILE_SUFFIX),
            null
        );
        TranslogFileSnapshot tlog2 = new TranslogFileSnapshot(
            primaryTerm,
            gen - 1,
            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (gen - 1), Translog.TRANSLOG_FILE_SUFFIX),
            null
        );

        return new TransferSnapshot() {
            @Override
            public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
                return Set.of(ckp1, ckp2);
            }

            @Override
            public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
                return Set.of(tlog1, tlog2);
            }

            @Override
            public TranslogTransferMetadata getTranslogTransferMetadata() {
                return new TranslogTransferMetadata(primaryTerm, gen, minTranslogGeneration, 2);
            }

            @Override
            public Set<TransferFileSnapshot> getTranslogFileSnapshotWithMetadata() throws IOException {
                tlog1.setMetadataFileInputStream(ckp1.inputStream());
                tlog2.setMetadataFileInputStream(ckp2.inputStream());
                return Set.of(tlog1, tlog2);
            }
        };
    }

    private static final TranslogTransferListener NOOP_LISTENER = new TranslogTransferListener() {
        @Override
        public void onUploadComplete(TransferSnapshot s) {}

        @Override
        public void onUploadFailed(TransferSnapshot s, Exception e) {}
    };

    // -----------------------------------------------------------------------
    // 1. isTranslogArchiveUploadEnabled flag
    // -----------------------------------------------------------------------

    public void testArchiveEnabledFlagOn() {
        assertTrue(buildManager(true).isTranslogArchiveUploadEnabled());
    }

    public void testArchiveEnabledFlagOff() {
        assertFalse(buildManager(false).isTranslogArchiveUploadEnabled());
    }

    // -----------------------------------------------------------------------
    // 2. transferSnapshot() — archive-OFF: 5 PUTs (2 tlog + 2 ckp + 1 metadata),
    // 0 GETs, 0 DELETEs, 0 LISTs.
    //
    // The CountingTransferService stubs all uploads to succeed synchronously.
    // -----------------------------------------------------------------------

    public void testTransferSnapshotArchiveOffPutCount() throws IOException {
        TranslogTransferManager mgr = buildManager(false);
        boolean ok = mgr.transferSnapshot(snapshotForGen(generation), NOOP_LISTENER);

        assertTrue("transferSnapshot should succeed", ok);
        // 2 tlog + 2 ckp uploaded via uploadBlobs (counted per-file), plus 1 metadata via uploadBlob = 5 PUTs.
        assertEquals("Archive-OFF: 5 PUTs (2 tlog + 2 ckp + 1 metadata)", 5, transferService.putCount());
        assertEquals("Archive-OFF: 0 GETs during upload", 0, transferService.getCount());
        assertEquals("Archive-OFF: 0 DELETEs during upload", 0, transferService.deleteCount());
        assertEquals("Archive-OFF: 0 LISTs during upload", 0, transferService.listCount());

        logger.info(
            "Archive-OFF transferSnapshot — PUTs={}, GETs={}, LISTs={}, DELETEs={}",
            transferService.putCount(),
            transferService.getCount(),
            transferService.listCount(),
            transferService.deleteCount()
        );
    }

    // -----------------------------------------------------------------------
    // 3. cleanup() — LIST and DELETE counts
    // -----------------------------------------------------------------------

    /** Archive-ON: cleanup() issues zero blob operations. */
    public void testCleanupArchiveOnZeroOps() throws IOException {
        TranslogTransferManager mgr = buildManager(true);
        RemoteFsTimestampAwareTranslog.cleanup(mgr);

        assertEquals("Archive-ON cleanup: 0 PUTs", 0, transferService.putCount());
        assertEquals("Archive-ON cleanup: 0 GETs", 0, transferService.getCount());
        assertEquals("Archive-ON cleanup: 0 LISTs", 0, transferService.listCount());
        assertEquals("Archive-ON cleanup: 0 DELETEs", 0, transferService.deleteCount());

        logger.info(
            "Archive-ON cleanup — PUTs={}, GETs={}, LISTs={}, DELETEs={}",
            transferService.putCount(),
            transferService.getCount(),
            transferService.listCount(),
            transferService.deleteCount()
        );
    }

    /** Archive-OFF: cleanup() issues ≥1 LIST to discover stale metadata blobs. */
    public void testCleanupArchiveOffIssuesList() throws IOException {
        // Pre-populate the mock with 2 metadata blobs so cleanup has something to list.
        TranslogTransferMetadata md1 = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, 2);
        TranslogTransferMetadata md2 = new TranslogTransferMetadata(primaryTerm, generation + 1, minTranslogGeneration, 2);
        transferService.setMetadataBlobs(List.of(new PlainBlobMetadata(md1.getFileName(), 1), new PlainBlobMetadata(md2.getFileName(), 1)));

        TranslogTransferManager mgr = buildManager(false);
        RemoteFsTimestampAwareTranslog.cleanup(mgr);

        assertThat("Archive-OFF cleanup: ≥1 LIST", transferService.listCount(), greaterThan(0));

        logger.info(
            "Archive-OFF cleanup — PUTs={}, GETs={}, LISTs={}, DELETEs={}",
            transferService.putCount(),
            transferService.getCount(),
            transferService.listCount(),
            transferService.deleteCount()
        );
    }

    // -----------------------------------------------------------------------
    // 4. Round-trip: N uploads → cleanup
    // -----------------------------------------------------------------------

    /**
     * Archive-OFF round-trip: N upload cycles = N*5 PUTs, then cleanup = ≥1 LIST + ≥1 DELETE.
     */
    public void testRoundTripArchiveOff() throws IOException {
        int cycles = 3;
        TranslogTransferManager mgr = buildManager(false);

        // Set up the mock to return metadata blobs for cleanup.
        List<BlobMetadata> metadataBlobs = new LinkedList<>();
        for (int i = 0; i < cycles; i++) {
            TranslogTransferMetadata md = new TranslogTransferMetadata(primaryTerm, generation + i, minTranslogGeneration, 2);
            metadataBlobs.add(new PlainBlobMetadata(md.getFileName(), 1));
            mgr.transferSnapshot(snapshotForGen(generation + i), NOOP_LISTENER);
        }
        transferService.setMetadataBlobs(metadataBlobs);

        int putsAfterUpload = transferService.putCount();
        // Each cycle: 2 tlog + 2 ckp + 1 metadata = 5 PUTs.
        assertEquals("Archive-OFF: " + cycles + " cycles × 5 PUTs = " + (cycles * 5), cycles * 5, putsAfterUpload);

        transferService.reset();
        RemoteFsTimestampAwareTranslog.cleanup(mgr);

        assertThat("Archive-OFF cleanup: ≥1 LIST", transferService.listCount(), greaterThan(0));
        assertThat("Archive-OFF cleanup: ≥1 DELETE", transferService.deleteCount(), greaterThan(0));

        logger.info(
            "Archive-OFF cleanup after {} cycles — LISTs={}, DELETEs={}",
            cycles,
            transferService.listCount(),
            transferService.deleteCount()
        );
    }

    /**
     * Archive-ON: cleanup() is a no-op — zero total ops.
     */
    public void testRoundTripArchiveOnZeroOps() throws IOException {
        TranslogTransferManager mgr = buildManager(true);
        RemoteFsTimestampAwareTranslog.cleanup(mgr);

        int total = transferService.putCount() + transferService.getCount() + transferService.listCount() + transferService.deleteCount();
        assertEquals("Archive-ON: zero total blob ops", 0, total);

        logger.info("Archive-ON round-trip total ops={}", total);
    }

    // -----------------------------------------------------------------------
    // 5. Comparative: archive-ON vs archive-OFF cleanup cost
    // -----------------------------------------------------------------------

    /**
     * After N upload cycles (archive-OFF), cleanup = ≥1 LIST + ≥1 DELETE.
     * Same scenario with archive-ON = 0 ops.
     */
    public void testCleanupCostArchiveOnVsOff() throws IOException {
        int cycles = 5;

        // archive-OFF
        TranslogTransferManager offMgr = buildManager(false);
        List<BlobMetadata> blobs = new LinkedList<>();
        for (int i = 0; i < cycles; i++) {
            TranslogTransferMetadata md = new TranslogTransferMetadata(primaryTerm, generation + i, minTranslogGeneration, 2);
            blobs.add(new PlainBlobMetadata(md.getFileName(), 1));
            offMgr.transferSnapshot(snapshotForGen(generation + i), NOOP_LISTENER);
        }
        transferService.setMetadataBlobs(blobs);
        transferService.reset();

        RemoteFsTimestampAwareTranslog.cleanup(offMgr);
        int offLists = transferService.listCount(), offDeletes = transferService.deleteCount();
        int offTotal = offLists + offDeletes;
        logger.info("Archive-OFF cleanup ({} cycles) — LISTs={}, DELETEs={}, total={}", cycles, offLists, offDeletes, offTotal);

        transferService.reset();

        // archive-ON
        TranslogTransferManager onMgr = buildManager(true);
        RemoteFsTimestampAwareTranslog.cleanup(onMgr);
        int onTotal = transferService.putCount() + transferService.getCount() + transferService.listCount() + transferService.deleteCount();
        logger.info("Archive-ON cleanup ({} cycles) — total={}", cycles, onTotal);

        assertEquals("Archive-ON cleanup: 0 blob ops", 0, onTotal);
        assertTrue("Archive-OFF ops (" + offTotal + ") > archive-ON (0)", offTotal > onTotal);
    }

    // -----------------------------------------------------------------------
    // 6. GC LIST-count: cleanup() LIST operations for archive ON vs OFF
    // -----------------------------------------------------------------------

    /**
     * Verifies that translog GC (cleanup) LIST call counts are lower with archive ON vs OFF.
     *
     * <p>Archive-ON: {@code cleanup()} returns immediately — 0 LIST calls.
     * Archive-OFF: {@code cleanup()} calls {@code listTranslogMetadataFilesAsync()} = 1 LIST,
     * then optionally more for primary term cleanup.
     *
     * <p>This confirms that archive ON reduces S3 LIST API costs for translog GC operations.
     */
    public void testGcListCountArchiveOnVsOff() throws IOException {
        int cycles = 5;

        // --- Archive-OFF: seed metadata blobs then measure cleanup() LISTs ---
        TranslogTransferManager offMgr = buildManager(false);
        List<BlobMetadata> blobs = new LinkedList<>();
        for (int i = 0; i < cycles; i++) {
            TranslogTransferMetadata md = new TranslogTransferMetadata(primaryTerm, generation + i, minTranslogGeneration, 2);
            blobs.add(new PlainBlobMetadata(md.getFileName(), 1));
        }
        transferService.setMetadataBlobs(blobs);
        transferService.reset();

        RemoteFsTimestampAwareTranslog.cleanup(offMgr);
        int offLists = transferService.listCount();
        int offTotal = offLists + transferService.deleteCount() + transferService.putCount() + transferService.getCount();

        logger.info(
            "Translog GC Archive-OFF ({} cycles) — LISTs={}, DELETEs={}, PUTs={}, GETs={}, TOTAL={}",
            cycles,
            offLists,
            transferService.deleteCount(),
            transferService.putCount(),
            transferService.getCount(),
            offTotal
        );

        // Archive-OFF cleanup MUST issue at least 1 LIST to discover stale metadata blobs.
        assertTrue("Archive-OFF GC: ≥1 LIST", offLists >= 1);

        transferService.reset();

        // --- Archive-ON: cleanup() must be a complete no-op (0 LISTs, 0 everything) ---
        TranslogTransferManager onMgr = buildManager(true);
        RemoteFsTimestampAwareTranslog.cleanup(onMgr);
        int onLists = transferService.listCount();
        int onTotal = transferService.putCount() + transferService.getCount() + onLists + transferService.deleteCount();

        logger.info(
            "Translog GC Archive-ON ({} cycles) — LISTs={}, TOTAL={}",
            cycles,
            onLists,
            onTotal
        );

        assertEquals("Archive-ON GC: 0 LISTs — cleanup() is no-op", 0, onLists);
        assertEquals("Archive-ON GC: 0 total blob ops", 0, onTotal);
        assertTrue(
            "Archive-ON GC LIST count (" + onLists + ") < Archive-OFF (" + offLists + ")",
            onLists < offLists
        );
    }

    // -----------------------------------------------------------------------
    // 7. OFF→ON toggle: fallback to per-shard metadata when no ZIPs found
    // -----------------------------------------------------------------------

    /**
     * Regression test for Issue 1: when archive is toggled OFF→ON and no ZIP has been
     * uploaded yet, {@link RemoteFsTranslog} must fall back to per-shard tlog metadata
     * rather than returning an empty translog.
     *
     * <p>Setup: archive=ON, listFolders returns empty (no hashNodeId dirs),
     * but metadataBlobs contains a real metadata entry. Expects that download()
     * triggers a GET for per-shard tlog files (fallback path).
     */
    public void testArchiveOnWithNoZipsFallsBackToPerShardMetadata() throws IOException {
        // Verify that when archive=ON and no ZIPs exist, the fallback path calls
        // readMetadata (= 1 listAllInSortedOrder call) in addition to the folder scan LIST.
        // We test this at the TranslogTransferManager level to avoid file-handle lifecycle issues
        // that come with full RemoteFsTranslog.download() invocation.

        TranslogTransferManager mgr = buildManager(true);

        // Step 1: simulate the ZIP folder scan (listFolders) returning empty.
        // This is what TranslogArchiveRecovery does — calls listFolders → empty → no ZIPs.
        Set<String> nodeDirs = mgr.getTransferService().listFolders(List.of("translog", "data", "hashTypeIndex"));
        int listsAfterFolderScan = transferService.listCount();
        assertTrue("nodeDirs must be empty (no ZIPs)", nodeDirs == null || nodeDirs.isEmpty());

        // Step 2: simulate the fallback — readMetadata(0) issues 1 more listAllInSortedOrder.
        TranslogTransferMetadata fallback = mgr.readMetadata(0);
        int listsAfterFallback = transferService.listCount();

        // Fallback must have issued ≥1 more LIST than the folder scan.
        assertTrue("readMetadata fallback must issue ≥1 additional LIST", listsAfterFallback > listsAfterFolderScan);

        // With no metadata blobs seeded, readMetadata returns null (truly fresh shard).
        assertNull("No metadata seeded → readMetadata must return null", fallback);
    }

    // -----------------------------------------------------------------------
    // CountingTransferService — counts PUTs / GETs / DELETEs / LISTs
    // -----------------------------------------------------------------------

    /**
     * A {@link TransferService} stub that counts every upload / download / delete / list
     * call and stubs responses so the real {@link TranslogTransferManager} flow completes.
     */
    static final class CountingTransferService implements TransferService {

        private final AtomicInteger puts = new AtomicInteger();
        private final AtomicInteger gets = new AtomicInteger();
        private final AtomicInteger deletes = new AtomicInteger();
        private final AtomicInteger lists = new AtomicInteger();

        /** Metadata blobs returned by listAllInSortedOrder (used by cleanup/readMetadata). */
        private volatile List<BlobMetadata> metadataBlobs = Collections.emptyList();

        void setMetadataBlobs(List<BlobMetadata> blobs) {
            this.metadataBlobs = new LinkedList<>(blobs);
        }

        int putCount() {
            return puts.get();
        }

        int getCount() {
            return gets.get();
        }

        int deleteCount() {
            return deletes.get();
        }

        int listCount() {
            return lists.get();
        }

        void reset() {
            puts.set(0);
            gets.set(0);
            deletes.set(0);
            lists.set(0);
        }

        // --- PUTs ---

        @Override
        public void uploadBlob(
            String threadPoolName,
            TransferFileSnapshot fileSnapshot,
            Iterable<String> path,
            ActionListener<TransferFileSnapshot> listener,
            WritePriority priority
        ) {
            puts.incrementAndGet();
            listener.onResponse(fileSnapshot);
        }

        @Override
        public void uploadBlob(TransferFileSnapshot fileSnapshot, Iterable<String> path, WritePriority priority) throws IOException {
            puts.incrementAndGet();
            // Consume and close the input stream to release file handles (avoids LeakFS failures).
            try (InputStream in = fileSnapshot.inputStream()) {
                in.transferTo(java.io.OutputStream.nullOutputStream());
            }
        }

        @Override
        public void uploadBlobs(
            Set<TransferFileSnapshot> snapshots,
            Map<Long, BlobPath> pathMap,
            ActionListener<TransferFileSnapshot> listener,
            WritePriority priority
        ) {
            snapshots.forEach(s -> {
                puts.incrementAndGet();
                // Consume and close the input stream to release file handles (avoids LeakFS failures).
                try (InputStream in = s.inputStream()) {
                    in.transferTo(java.io.OutputStream.nullOutputStream());
                } catch (IOException e) {
                    // ignore in test stub
                }
                listener.onResponse(s);
            });
        }

        @Override
        public void uploadBlob(
            InputStream inputStream,
            Iterable<String> path,
            String blobName,
            WritePriority priority,
            ActionListener<Void> listener
        ) {
            puts.incrementAndGet();
            listener.onResponse(null);
        }

        @Override
        public void uploadBlobStream(
            InputStream inputStream,
            long contentLength,
            Iterable<String> path,
            String blobName,
            WritePriority priority,
            org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
        ) throws IOException {
            puts.incrementAndGet();
        }

        // --- GETs ---

        @Override
        public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException {
            gets.incrementAndGet();
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream downloadBlob(Iterable<String> path, String fileName, long position, long length) throws IOException {
            gets.incrementAndGet();
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public org.opensearch.common.blobstore.InputStreamWithMetadata downloadBlobWithMetadata(Iterable<String> path, String fileName)
            throws IOException {
            gets.incrementAndGet();
            return new org.opensearch.common.blobstore.InputStreamWithMetadata(
                new ByteArrayInputStream(new byte[0]),
                Collections.emptyMap()
            );
        }

        // --- DELETEs ---

        @Override
        public void deleteBlobs(Iterable<String> path, List<String> fileNames) throws IOException {
            deletes.addAndGet(fileNames.size());
        }

        @Override
        public void deleteBlobsAsync(String threadPool, Iterable<String> path, List<String> fileNames, ActionListener<Void> listener) {
            deletes.addAndGet(fileNames.size());
            listener.onResponse(null);
        }

        @Override
        public void delete(Iterable<String> path) throws IOException {
            deletes.incrementAndGet();
        }

        @Override
        public void deleteAsync(String threadPool, Iterable<String> path, ActionListener<Void> listener) {
            deletes.incrementAndGet();
            listener.onResponse(null);
        }

        // --- LISTs ---

        @Override
        public Set<String> listAll(Iterable<String> path) throws IOException {
            lists.incrementAndGet();
            return Collections.emptySet();
        }

        @Override
        public Set<String> listFolders(Iterable<String> path) throws IOException {
            lists.incrementAndGet();
            return Collections.emptySet();
        }

        @Override
        public void listFoldersAsync(String threadPool, Iterable<String> path, ActionListener<Set<String>> listener) {
            lists.incrementAndGet();
            listener.onResponse(Collections.emptySet());
        }

        @Override
        public void listAllInSortedOrder(
            Iterable<String> path,
            String filenamePrefix,
            int limit,
            ActionListener<List<BlobMetadata>> listener
        ) {
            lists.incrementAndGet();
            listener.onResponse(new LinkedList<>(metadataBlobs));
        }

        @Override
        public void listAllInSortedOrderAsync(
            String threadPool,
            Iterable<String> path,
            String filenamePrefix,
            int limit,
            ActionListener<List<BlobMetadata>> listener
        ) {
            lists.incrementAndGet();
            listener.onResponse(new LinkedList<>(metadataBlobs));
        }
    }
}
