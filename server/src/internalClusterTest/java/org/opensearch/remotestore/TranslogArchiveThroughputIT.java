/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.remotestore.translogmetadata.mocks.MockFsMetadataSupportedRepositoryPlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Component test: verifies that translog archive upload does NOT degrade indexing throughput
 * compared to non-archive mode, even when S3 PUT latency is significant.
 *
 * <p>Root cause (pre-fix): {@code TranslogArchiveBatchCoordinator.submitAndWait()} blocked
 * the {@code TRANSLOG_SYNC} thread for {@code batchInterval + uploadTime} per cycle, effectively
 * doubling the sync cycle time and halving indexing throughput when {@code durability=REQUEST}.
 *
 * <p>Fix: {@code dispatchUnderLock()} now hands the batch off to a background upload thread
 * immediately, so the next collection cycle starts in parallel with the upload (pipelining).
 * Effective cycle ≈ {@code max(batchInterval, uploadTime)} instead of {@code batchInterval + uploadTime}.
 *
 * <p>This test injects latency into every translog repo blob write (simulating S3 RTT) and asserts
 * that archive throughput is within an acceptable ratio of non-archive throughput.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TranslogArchiveThroughputIT extends RemoteStoreBaseIntegTestCase {

    static final String LATENCY_FS_TYPE = "latency-fs";
    private static final String INDEX_ARCHIVE = "bench-translog-archive";
    private static final String INDEX_NO_ARCHIVE = "bench-translog-no-archive";
    private static final int NUM_SHARDS = 3;
    private static final int NUM_REPLICAS = 0;
    private static final int BULK_SIZE = 50;
    private static final int TOTAL_DOCS = 500;

    /**
     * Simulated blob write latency (ms) — large enough to expose blocking behaviour but
     * small enough to keep the test duration reasonable.
     */
    static final int WRITE_LATENCY_MS = 150;

    /**
     * Maximum acceptable throughput ratio (no-archive tps / archive tps).
     * Pre-fix: ~2.0x (archive blocked TRANSLOG_SYNC thread for batchInterval + uploadTime).
     * Post-fix target: ≤ 1.5x (collection and upload overlap via pipelining).
     */
    private static final double MAX_ACCEPTABLE_RATIO = 1.5;

    // -----------------------------------------------------------------------
    // Latency-injecting translog repository
    // -----------------------------------------------------------------------

    static class LatencyInjectingFsBlobContainer extends FsBlobContainer {
        LatencyInjectingFsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
            super(blobStore, blobPath, path);
        }

        private static void simulateLatency() {
            try {
                Thread.sleep(WRITE_LATENCY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            simulateLatency();
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            simulateLatency();
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    static class LatencyInjectingFsBlobStore extends FsBlobStore {
        LatencyInjectingFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly) throws IOException {
            super(bufferSizeInBytes, path, readonly);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            Path storagePath = this.path();
            for (String p : path.toArray()) {
                storagePath = storagePath.resolve(p);
            }
            try {
                Files.createDirectories(storagePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new LatencyInjectingFsBlobContainer(this, path, storagePath);
        }
    }

    public static class LatencyInjectingFsRepository extends FsRepository {

        static final String TYPE = LATENCY_FS_TYPE;

        public LatencyInjectingFsRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            String location = metadata.settings().get("location");
            Path path = environment.resolveRepoFile(location);
            return new LatencyInjectingFsBlobStore(bufferSize, path, false);
        }
    }

    public static class LatencyInjectingFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                LATENCY_FS_TYPE,
                metadata -> new LatencyInjectingFsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test infrastructure
    // -----------------------------------------------------------------------

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(
            MockFsRepositoryPlugin.class,
            MockFsMetadataSupportedRepositoryPlugin.class,
            LatencyInjectingFsRepositoryPlugin.class
        ).collect(Collectors.toList());
    }

    @Override
    public void setUp() throws Exception {
        segmentRepoPath = null;
        translogRepoPath = null;
        asyncUploadMockFsRepo = false;
        clusterSettingsSuppliedByTest = true;
        super.setUp();
    }

    /**
     * Wire segment repo to standard {@link MockFsRepositoryPlugin#TYPE} and
     * translog repo to {@link LatencyInjectingFsRepository} so that every translog blob PUT
     * incurs simulated latency — exposing the TRANSLOG_SYNC thread blocking issue.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsRepositoryPlugin.TYPE,    // segment repo — no artificial latency
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    LATENCY_FS_TYPE                 // translog repo — latency injected here
                )
            )
            .build();
    }

    private Settings indexSettings(boolean archiveEnabled) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(NUM_REPLICAS, NUM_SHARDS))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), archiveEnabled)
            // Short sync interval so translog syncs happen frequently — exposes blocking behaviour
            .put("index.translog.sync_interval", "100ms")
            .put("index.translog.durability", "REQUEST")
            .build();
    }

    // -----------------------------------------------------------------------
    // Test
    // -----------------------------------------------------------------------

    /**
     * Verifies that with the pipelining fix, translog archive upload does not degrade
     * indexing throughput by more than {@value #MAX_ACCEPTABLE_RATIO}x vs non-archive.
     *
     * <p>Pre-fix: archive throughput ≈ 50% of non-archive (ratio ~2.0×) because
     * {@code submitAndWait()} blocked the {@code TRANSLOG_SYNC} thread for the full
     * {@code batchInterval + uploadLatency} per cycle.
     *
     * <p>Post-fix: collection and upload pipeline → ratio ≤ {@value #MAX_ACCEPTABLE_RATIO}.
     */
    public void testArchiveThroughputWithinAcceptableRatioOfNoArchive() throws Exception {
        logger.info("--> starting cluster (cluster-manager + 1 data node)");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        logger.info("--> creating indices (archive ON and OFF)");
        assertAcked(prepareCreate(INDEX_NO_ARCHIVE).setSettings(indexSettings(false)));
        assertAcked(prepareCreate(INDEX_ARCHIVE).setSettings(indexSettings(true)));
        ensureGreen(INDEX_NO_ARCHIVE, INDEX_ARCHIVE);

        // warm-up: let translog coordinator register and upload threads stabilise
        logger.info("--> warm-up ({} docs each)", BULK_SIZE * 3);
        indexDocs(INDEX_NO_ARCHIVE, BULK_SIZE * 3);
        indexDocs(INDEX_ARCHIVE, BULK_SIZE * 3);
        Thread.sleep(500);

        // --- measured run: no-archive ---
        logger.info("--> [NO-ARCHIVE] measuring {} docs", TOTAL_DOCS);
        long noArchiveMs = measureIndexingMs(INDEX_NO_ARCHIVE, TOTAL_DOCS);
        double noArchiveTps = (double) TOTAL_DOCS / noArchiveMs * 1000.0;

        // --- measured run: archive ---
        logger.info("--> [ARCHIVE] measuring {} docs", TOTAL_DOCS);
        long archiveMs = measureIndexingMs(INDEX_ARCHIVE, TOTAL_DOCS);
        double archiveTps = (double) TOTAL_DOCS / archiveMs * 1000.0;

        double ratio = noArchiveTps / archiveTps;

        logger.info(
            String.format(
                Locale.ROOT,
                "%n%n========== TRANSLOG ARCHIVE THROUGHPUT TEST ==========%n"
                    + "  Setup          : 1 node, %d shards, %d replicas, sync_interval=100ms, durability=REQUEST%n"
                    + "  Simulated RTT  : %d ms per writeBlob (translog repo only)%n"
                    + "  Docs indexed   : %,d per run%n"
                    + "  NO-ARCHIVE     : %,d ms  →  %.1f docs/sec%n"
                    + "  ARCHIVE        : %,d ms  →  %.1f docs/sec%n"
                    + "  Ratio          : %.2fx  (no-archive/archive; >1 = archive is slower)%n"
                    + "  Max allowed    : %.1fx%n"
                    + "  Root cause     : submitAndWait() blocked TRANSLOG_SYNC thread for%n"
                    + "                   batchInterval + uploadTime per cycle (pre-fix).%n"
                    + "  Fix            : dispatchUnderLock() hands batch to background thread%n"
                    + "                   so next collection starts immediately (pipelining).%n"
                    + "======================================================%n",
                NUM_SHARDS,
                NUM_REPLICAS,
                WRITE_LATENCY_MS,
                TOTAL_DOCS,
                noArchiveMs,
                noArchiveTps,
                archiveMs,
                archiveTps,
                ratio,
                MAX_ACCEPTABLE_RATIO
            )
        );

        assertTrue(
            String.format(
                Locale.ROOT,
                "Translog archive throughput ratio %.2fx exceeds max allowed %.1fx. "
                    + "Archive indexed %,d docs in %d ms vs no-archive %,d ms. "
                    + "This indicates the pipelining fix is not working — TRANSLOG_SYNC thread "
                    + "is still blocking on upload during batch collection.",
                ratio,
                MAX_ACCEPTABLE_RATIO,
                TOTAL_DOCS,
                archiveMs,
                noArchiveMs
            ),
            ratio <= MAX_ACCEPTABLE_RATIO
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private long measureIndexingMs(String indexName, int totalDocs) {
        long start = System.nanoTime();
        indexDocs(indexName, totalDocs);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("[{}] indexed {} docs in {} ms", indexName, totalDocs, elapsedMs);
        return elapsedMs;
    }

    private void indexDocs(String indexName, int totalDocs) {
        int remaining = totalDocs;
        while (remaining > 0) {
            int batchSize = Math.min(BULK_SIZE, remaining);
            BulkRequest bulk = new BulkRequest();
            for (int i = 0; i < batchSize; i++) {
                bulk.add(
                    new IndexRequest(indexName).id(UUIDs.randomBase64UUID())
                        .source("field", randomAlphaOfLength(20), "ts", System.currentTimeMillis())
                );
            }
            BulkResponse resp = client().bulk(bulk).actionGet(TimeValue.timeValueSeconds(30));
            assertFalse("Bulk had failures: " + resp.buildFailureMessage(), resp.hasFailures());
            remaining -= batchSize;
        }
    }
}
