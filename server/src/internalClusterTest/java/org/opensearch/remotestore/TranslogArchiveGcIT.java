/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.translog.TranslogArchiveTimerThreadLeakFilter;
import org.opensearch.index.translog.TranslogArchiveCollector;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration test that verifies the translog archive GC end-to-end:
 * indexes some documents, waits for TAR blobs to appear, waits past the
 * retention window, then triggers GC and asserts TARs are deleted.
 *
 * <p>Uses very short GC interval (5s) and retention (10s) so the test
 * completes in under a minute. The safety buffer is zeroed out in setUp/tearDown
 * so the hardcoded 5-minute floor doesn't block deletion.
 */
@ThreadLeakFilters(filters = TranslogArchiveTimerThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TranslogArchiveGcIT extends BaseRemoteStoreRestoreIT {

    private static final String INDEX_NAME = "gc-test";

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // GC scheduler fires every 1 minute (min is now 1s)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL.getKey(), "1m")
            // Fast translog buffer so TARs are uploaded quickly
            .put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            // testGcSchedulerIndexesTarsAndDeletesThem: cluster retention = 1m
            // testGcPreservesRecentTarsWhenRetentionIsLong: overridden dynamically in that test
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_RETENTION.getKey(), "1m")
            .build();
    }

    /**
     * Verifies that the GC scheduler end-to-end:
     * <ol>
     *   <li>The GC scanner creates {@code gc_idx/} index files ({@code .idx}) for each scanned minute-dir.</li>
     *   <li>After the safety buffer expires, TARs are deleted automatically by the scheduled GC.</li>
     * </ol>
     *
     * <p>Setup: GC interval = 1m, safety buffer = 1.5m. After indexing and flushing:
     * <ul>
     *   <li>Wait for TARs to appear (upload phase).</li>
     *   <li>Use {@code assertBusy} with 3m timeout to wait for {@code gc_idx/} {@code .idx} files to appear
     *       (proves the GC scanner ran and built the index — fires ~1m after startup).</li>
     *   <li>Use {@code assertBusy} with 3m timeout to wait for all TARs to be deleted
     *       (proves the safety buffer expired and GC ran the deletion pass).</li>
     * </ul>
     */
    public void testGcSchedulerIndexesTarsAndDeletesThem() throws Exception {
        // archive_retention = 1m (the time gate). GC interval = 1m (set in nodeSettings).
        // TARs uploaded at t=0, retention expires at t=1m.
        // First GC run at t=1m: scans minute-dirs → writes .idx files (scanner phase).
        // Second GC run at t=2m: retention expired → deletes TARs (deletion phase).
        // assertBusy timeout = 3m to give plenty of headroom.

        // Single node: both cluster-manager and data. GC resolves transfer service from local shards.
        final String nodeName = internalCluster().startNode();
        ensureGreen();

        assertAcked(
            client().admin().indices().prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(remoteStoreIndexSettings(0, 1))
                        .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                        // Cluster retention is set to 1m in nodeSettings — no index-level override needed
                        .build()
                )
        );
        ensureGreen(INDEX_NAME);

        // Index 100 docs then flush so global checkpoint advances past all seqNos.
        // This ensures isSafeToDelete() Phase 1 passes (checkpoint ≥ maxSeqNo at upload time).
        for (int i = 0; i < 100; i++) {
            client().index(new IndexRequest(INDEX_NAME).source("field", "value-" + i)).actionGet();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Wait for TARs to appear (upload phase)
        assertBusy(() -> {
            List<String> tars = collectTarPaths(translogRepoPath);
            assertFalse("Expected at least one TAR to be uploaded under txlog/", tars.isEmpty());
        }, 30, java.util.concurrent.TimeUnit.SECONDS);

        int tarsAtStart = collectTarPaths(translogRepoPath).size();
        logger.info("=== TARs uploaded: {} ===", tarsAtStart);
        assertTrue("Expected at least one TAR before GC", tarsAtStart > 0);

        // ── Phase 1: assert GC scanner creates gc_idx/.idx files ──────────────
        // The GC scheduler fires every 1m. After the first fire it should scan the minute-dirs
        // and write .idx files under gc_idx/{day}/{minute}.idx
        logger.info("=== Waiting for gc_idx/ .idx files to appear (GC scanner phase, up to 3m) ===");
        assertBusy(() -> {
            List<String> idxFiles = collectIdxPaths(translogRepoPath);
            assertFalse(
                "Expected at least one gc_idx/ .idx file to be written by the GC scanner",
                idxFiles.isEmpty()
            );
            logger.info("gc_idx/ .idx files found: {}", idxFiles);
        }, 3, java.util.concurrent.TimeUnit.MINUTES);

        // ── Phase 2: assert GC deletes TARs after archive_retention expires ────
        // archive_retention = 1m. After the second GC run (~2m from start), all TARs should be deleted.
        logger.info("=== Waiting for all TARs to be deleted by GC (up to 3m) ===");
        assertBusy(() -> {
            List<String> remaining = collectTarPaths(translogRepoPath);
            assertEquals(
                "Expected all TARs to be deleted by GC, but " + remaining.size() + " remain: " + remaining,
                0,
                remaining.size()
            );
        }, 3, java.util.concurrent.TimeUnit.MINUTES);

        logger.info("=== GC end-to-end verified: TARs uploaded={}, all deleted ✅ ===", tarsAtStart);
    }

    /**
     * Verifies that GC preserves TARs when {@code archive_retention} is set to a long value (60m).
     *
     * <p>Since GC cutoff = {@code now - archive_retention}, recently-uploaded TARs ({@literal <} 60m old)
     * should not be deleted even when GC is triggered immediately.
     */
    public void testGcPreservesRecentTarsWhenRetentionIsLong() throws Exception {
        // Single node: both cluster-manager and data (GC requires local shards to resolve transfer service).
        internalCluster().startNode();
        ensureGreen();

        // Override cluster retention to 60m so recently-uploaded TARs are protected.
        // This overrides the 1m default set in nodeSettings().
        client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_RETENTION.getKey(), "60m")
                    .build()
            ).get();

        assertAcked(
            client().admin().indices().prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(remoteStoreIndexSettings(0, 1))
                        .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                        // Cluster retention dynamically updated to 60m above — no index-level override needed
                        .build()
                )
        );
        ensureGreen(INDEX_NAME);

        // Index some docs
        for (int i = 0; i < 20; i++) {
            client().index(new IndexRequest(INDEX_NAME).source("field", "value-" + i)).actionGet();
        }

        // Wait for TARs to appear
        assertBusy(() -> {
            assertFalse("Expected at least one TAR", collectTarPaths(translogRepoPath).isEmpty());
        }, 30, java.util.concurrent.TimeUnit.SECONDS);

        int tarsBeforeGc = collectTarPaths(translogRepoPath).size();
        assertTrue("Expected TARs before GC", tarsBeforeGc > 0);

        // Trigger GC immediately — retention is 60m so recently-uploaded TARs should NOT be deleted
        String clusterManagerNode = internalCluster().getMasterName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, clusterManagerNode);
        indicesService.getTranslogArchiveCollector().runRetentionForTesting();

        int tarsAfterGc = collectTarPaths(translogRepoPath).size();
        logger.info("TARs before GC: {}, after GC: {} (60m retention, should not delete)", tarsBeforeGc, tarsAfterGc);
        assertEquals(
            "GC should not have deleted recent TARs (60m archive_retention active), count changed from "
                + tarsBeforeGc + " to " + tarsAfterGc,
            tarsBeforeGc,
            tarsAfterGc
        );
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Collects all {@code *.idx} blob paths under {@code gc_idx/} in the translog repo.
     * Path structure: {@code gc_idx/{yyyyMMdd}/{HHmm}.idx}
     */
    private static List<String> collectIdxPaths(Path repoRoot) throws IOException {
        List<String> result = new ArrayList<>();
        Path gcIdxRoot = repoRoot.resolve("gc_idx");
        if (!Files.isDirectory(gcIdxRoot)) {
            return result;
        }
        try (DirectoryStream<Path> dayDirs = Files.newDirectoryStream(gcIdxRoot)) {
            for (Path dayDir : dayDirs) {
                if (!Files.isDirectory(dayDir)) continue;
                try (DirectoryStream<Path> idxFiles = Files.newDirectoryStream(dayDir, "*.idx")) {
                    for (Path idx : idxFiles) {
                        result.add(gcIdxRoot.relativize(idx).toString());
                    }
                }
            }
        }
        return result;
    }

    /**
     * Collects all {@code *.tar} blob paths under {@code txlog/} in the translog repo.
     * Path structure: {@code txlog/{yyyyMMdd}/{HHmm}/*.tar}
     */
    private static List<String> collectTarPaths(Path repoRoot) throws IOException {
        List<String> result = new ArrayList<>();
        Path txlogRoot = repoRoot.resolve("txlog");
        if (!Files.isDirectory(txlogRoot)) {
            return result;
        }
        try (DirectoryStream<Path> dayDirs = Files.newDirectoryStream(txlogRoot)) {
            for (Path dayDir : dayDirs) {
                if (!Files.isDirectory(dayDir)) continue;
                try (DirectoryStream<Path> minuteDirs = Files.newDirectoryStream(dayDir)) {
                    for (Path minuteDir : minuteDirs) {
                        if (!Files.isDirectory(minuteDir)) continue;
                        try (DirectoryStream<Path> blobs = Files.newDirectoryStream(minuteDir, "*.tar")) {
                            for (Path blob : blobs) {
                                result.add(txlogRoot.relativize(blob).toString());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }
}
