/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Integration test verifying that translog archive (TAR) upload mode works end-to-end.
 *
 * <p>Uses a single-node cluster with FS-backed repositories so blobs can be inspected on disk.
 * Two indices (10 shards each) run concurrently — one with {@code "tar"} strategy, one with
 * the default per-file strategy.
 *
 * <p>Verifies:
 * <ol>
 *   <li>TAR blobs appear in the translog repo for the archive-ON index.</li>
 *   <li>ZERO TARs appear for the archive-OFF (per-file) index.</li>
 *   <li>Per-file index: 1 {@code metadata__} blob per {@code .tlog} blob.</li>
 *   <li>After flush, {@code syncNeeded()} returns false for an empty generation —
 *       TAR count stays stable (no spurious uploads).</li>
 * </ol>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = { TranslogArchiveTimerThreadLeakFilter.class })
public class TranslogArchiveUploadIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_ARCHIVE_ON = "test-archive-on";
    private static final String INDEX_ARCHIVE_OFF = "test-archive-off";
    private static final int NUM_SHARDS = 10;
    private static final int NUM_DOCS = 200;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Load RbsArchivePlugin so the "tar" translog strategy is registered in the test cluster.
        return Stream.concat(super.nodePlugins().stream(), Stream.of(RbsArchivePlugin.class)).collect(Collectors.toList());
    }

    @Override
    public void setUp() throws Exception {
        // Use deterministic (non-async) FS repo so we can inspect files reliably.
        segmentRepoPath = null;
        translogRepoPath = null;
        asyncUploadMockFsRepo = false;
        super.setUp();
        // Start cluster-manager and data nodes in setUp() rather than inside each test method.
        // This ensures nodes are alive when the base @After teardown() runs its assertions.
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNode();
    }

    /**
     * Returns index settings for a remote-store-backed index with the given translog strategy.
     *
     * @param strategyName {@code "tar"} to enable archive upload; {@code ""} for per-file default.
     */
    private Settings indexSettings(String strategyName) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(0, NUM_SHARDS))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), strategyName)
            .put("index.translog.sync_interval", "100ms")
            .build();
    }

    /**
     * Verifies upload file types for two indices (10 shards each) with TAR strategy ON and OFF.
     *
     * <p>What we assert (structural correctness, not exact ratios):
     * <ul>
     *   <li><b>Archive-ON index</b>: at least 1 TAR appears in the translog repo.</li>
     *   <li><b>Archive-OFF index</b>: ZERO TARs — TARs are exclusively produced by archive-ON.</li>
     *   <li><b>Archive-OFF index</b>: at least 1 {@code .tlog} + 1 {@code metadata__} blob.</li>
     *   <li>1 {@code metadata__} blob per {@code .tlog} file (always uploaded as a pair).</li>
     * </ul>
     */
    public void testUploadedFileTypesWithArchiveOnAndOff() throws Exception {
        String archiveOnUuid = createArchiveIndex();
        String archiveOffUuid = createDefaultIndex();

        // Phase 1: Index into archive-ON ONLY — confirm TARs appear and archive-OFF has zero.
        indexDocuments(INDEX_ARCHIVE_ON, NUM_DOCS);

        assertBusy(
            () -> assertThat("Expected ≥1 TAR for archive-ON index", findBlobs(translogRepoPath, "*.tar"), not(empty())),
            30,
            TimeUnit.SECONDS
        );

        List<Path> onTars = findBlobs(translogRepoPath, "*.tar");
        List<Path> offTarsPhase1 = findBlobs(translogRepoPath, "*.tar", archiveOffUuid);

        logger.info("Phase 1 (archive-ON) — TARs: {}, archive-OFF TARs (must be 0): {}", onTars.size(), offTarsPhase1.size());

        assertThat("Archive-ON: ≥1 TAR uploaded", onTars, not(empty()));
        assertEquals("Archive-OFF: ZERO TARs at any point (TARs are exclusive to archive-ON)", 0, offTarsPhase1.size());

        // Phase 2: Index into archive-OFF, flush, assert ZERO TARs in its path.
        indexDocuments(INDEX_ARCHIVE_OFF, NUM_DOCS);
        flushAndRefresh(INDEX_ARCHIVE_OFF);

        assertBusy(
            () -> assertThat(
                "Expected ≥1 metadata__ blob for archive-OFF index",
                findMetadataBlobs(translogRepoPath, archiveOffUuid),
                not(empty())
            ),
            30,
            TimeUnit.SECONDS
        );

        List<Path> offTars = findBlobs(translogRepoPath, "*.tar", archiveOffUuid);
        List<Path> offTlogBlobs = findBlobs(translogRepoPath, "*.tlog", archiveOffUuid);
        List<Path> offMetaBlobs = findMetadataBlobs(translogRepoPath, archiveOffUuid);

        logger.info("Phase 2 (archive-OFF) — TARs: {}, .tlog: {}, metadata: {}", offTars.size(), offTlogBlobs.size(), offMetaBlobs.size());

        assertEquals("Archive-OFF: ZERO TARs in its path", 0, offTars.size());
        assertThat("Archive-OFF: ≥1 .tlog file", offTlogBlobs, not(empty()));
        assertThat("Archive-OFF: ≥1 metadata blob", offMetaBlobs, not(empty()));
        assertEquals("Archive-OFF: 1 metadata per .tlog (always uploaded as a pair)", offTlogBlobs.size(), offMetaBlobs.size());
    }

    /**
     * Verifies that with archive enabled and zero translog ops (after flush with no new writes),
     * {@code syncNeeded()} returns {@code false} and no additional TARs are uploaded.
     */
    public void testNoUploadWhenNoTranslogOpsWithArchiveEnabled() throws Exception {
        createArchiveIndex();

        indexDocuments(INDEX_ARCHIVE_ON, NUM_DOCS);

        // Wait until ≥1 TAR appears — confirms archive upload is working.
        assertBusy(() -> assertThat(findBlobs(translogRepoPath, "*.tar"), not(empty())), 30, TimeUnit.SECONDS);

        // Flush: commits ops → rolls generation to a new empty generation (0 ops).
        flushAndRefresh(INDEX_ARCHIVE_ON);
        final int tarCountAfterFlush = findBlobs(translogRepoPath, "*.tar").size();
        logger.info("TAR count after flush (baseline): {}", tarCountAfterFlush);

        // Trigger more sync cycles. If syncNeeded() is broken, new TARs would appear.
        flushAndRefresh(INDEX_ARCHIVE_ON);
        flushAndRefresh(INDEX_ARCHIVE_ON);

        int tarCountFinal = findBlobs(translogRepoPath, "*.tar").size();
        logger.info("TAR count final: baseline={}, final={}", tarCountAfterFlush, tarCountFinal);
        assertEquals("syncNeeded() fix: no TARs uploaded for empty generation after flush", tarCountAfterFlush, tarCountFinal);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Creates the archive-ON index and returns its UUID. */
    private String createArchiveIndex() {
        createIndex(INDEX_ARCHIVE_ON, indexSettings("tar"));
        ensureGreen(INDEX_ARCHIVE_ON);
        return client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_ARCHIVE_ON).getIndexUUID();
    }

    /** Creates the archive-OFF (per-file default) index and returns its UUID. */
    private String createDefaultIndex() {
        createIndex(INDEX_ARCHIVE_OFF, indexSettings(""));
        ensureGreen(INDEX_ARCHIVE_OFF);
        return client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_ARCHIVE_OFF).getIndexUUID();
    }

    private void indexDocuments(String indexName, int count) {
        BulkRequest bulk = new BulkRequest();
        for (int i = 0; i < count; i++) {
            bulk.add(new IndexRequest(indexName).id(UUIDs.randomBase64UUID()).source("field", randomAlphaOfLength(8)));
        }
        BulkResponse response = client().bulk(bulk).actionGet();
        assertFalse("Bulk indexing had failures", response.hasFailures());
    }

    /**
     * Walks {@code root} and collects all regular files whose name matches {@code glob},
     * optionally filtering to only paths that contain {@code indexUUID} as a path component.
     */
    private static List<Path> findBlobs(Path root, String glob, String indexUUID) throws IOException {
        List<Path> result = new ArrayList<>();
        if (!Files.exists(root)) return result;
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!root.getFileSystem().getPathMatcher("glob:**/" + glob).matches(file)) {
                    return FileVisitResult.CONTINUE;
                }
                if (indexUUID != null) {
                    boolean found = false;
                    for (Path component : root.relativize(file)) {
                        if (indexUUID.equals(component.toString())) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) return FileVisitResult.CONTINUE;
                }
                result.add(file);
                return FileVisitResult.CONTINUE;
            }
        });
        return result;
    }

    private static List<Path> findBlobs(Path root, String glob) throws IOException {
        return findBlobs(root, glob, null);
    }

    private static List<Path> findMetadataBlobs(Path root, String indexUUID) throws IOException {
        return findBlobs(root, "metadata__*", indexUUID);
    }
}
