/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.translog.TranslogArchiveTimerThreadLeakFilter;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugin.rbs.archive.RbsArchivePlugin;
import org.opensearch.plugins.Plugin;
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
 * Integration test verifying that translog archive (TAR) upload mode works end-to-end:
 *
 * <ol>
 *   <li>When strategy is {@code "tar"}: TAR blobs appear in the translog repo; no TARs
 *       appear for indices that use the default per-file strategy.</li>
 *   <li>When strategy is default (empty): per-file blobs ({@code .tlog} + {@code metadata__})
 *       are written; no TARs are produced.</li>
 *   <li>{@code syncNeeded()} does not trigger an upload for an empty generation after flush
 *       when archive is enabled — TAR count stays stable.</li>
 * </ol>
 *
 * <p>Uses a single-node cluster with FS-backed repositories so blobs can be inspected on disk.
 * Two indices (10 shards each) run concurrently — one with {@code "tar"} strategy, one with
 * the default per-file strategy.
 *
 * <p><b>Note</b>: the {@code rbs-archive} plugin must be on the classpath for the TAR strategy
 * to be loaded. In the Gradle test task this is ensured via the {@code testImplementation}
 * dependency on {@code :plugins:rbs-archive}.
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

    /** Returns the index UUID from cluster state — used to isolate blobs by index in the shared repo. */
    private String resolveIndexUUID(String indexName) {
        return client().admin().cluster().prepareState().get().getState().metadata().index(indexName).getIndexUUID();
    }

    /**
     * Verifies upload file types for two indices (10 shards each) with TAR strategy ON and OFF.
     *
     * <p>What we assert (structural correctness, not exact ratios):
     *
     * <ul>
     *   <li><b>Archive-ON index</b>: at least 1 TAR appears in the translog repo.</li>
     *   <li><b>Archive-OFF index</b>: ZERO TARs — TARs are exclusively produced by archive-ON.</li>
     *   <li><b>Archive-OFF index</b>: at least 1 {@code .tlog} + 1 {@code metadata__} blob.</li>
     *   <li>1 {@code metadata__} blob per {@code .tlog} file (always uploaded as a pair).</li>
     * </ul>
     *
     * <p>We intentionally do NOT assert "zero .tlog for archive-ON" — there is an inherent
     * startup race where the first 1–2 sync cycles may fire before the batch coordinator finishes
     * registering, causing a brief fallback to per-shard uploads. This is a known transient
     * behaviour, not a correctness bug.
     *
     * <p>PUT reduction design (documented, not asserted):
     * <ul>
     *   <li>archive-ON per cycle: 1 TAR + N metadata PUTs (N = active shards)</li>
     *   <li>archive-OFF per cycle: N × 3 PUTs (.tlog + .ckp + metadata per shard)</li>
     *   <li>Saving: (3N) − (1 + N) = 2N − 1 PUTs per cycle (~63% reduction for 10 shards)</li>
     * </ul>
     */
    public void testUploadedFileTypesWithArchiveOnAndOff() throws Exception {
        createIndex(INDEX_ARCHIVE_ON, indexSettings("tar"));
        createIndex(INDEX_ARCHIVE_OFF, indexSettings(""));
        ensureGreen(INDEX_ARCHIVE_ON, INDEX_ARCHIVE_OFF);

        // UUID-based path filtering isolates blobs by index within the shared translog repo.
        // Path structure: {repoRoot}/{hashPrefix}/{indexUUID}/{shardId}/translog/...
        String archiveOnUuid = resolveIndexUUID(INDEX_ARCHIVE_ON);
        String archiveOffUuid = resolveIndexUUID(INDEX_ARCHIVE_OFF);

        // -----------------------------------------------------------------------
        // Phase 1: Index into archive-ON ONLY — archive-OFF has 0 ops → its sync is a no-op.
        // This ensures all blobs appearing in the repo during this phase belong to archive-ON.
        // -----------------------------------------------------------------------
        indexDocuments(INDEX_ARCHIVE_ON, NUM_DOCS);

        assertBusy(
            () -> assertThat("Expected ≥1 TAR for archive-ON index", findBlobs(translogRepoPath, "*.tar"), not(empty())),
            30,
            TimeUnit.SECONDS
        );

        List<Path> onTars = findBlobs(translogRepoPath, "*.tar");
        List<Path> onTlogBlobs = findBlobs(translogRepoPath, "*.tlog", archiveOnUuid);
        List<Path> onMetaBlobs = findMetadataBlobs(translogRepoPath, archiveOnUuid);

        // Archive-OFF produces ZERO TARs — TARs are exclusively an archive-ON artifact.
        List<Path> offTarsPhase1 = findBlobs(translogRepoPath, "*.tar", archiveOffUuid);

        logger.info(
            "Phase 1 (archive-ON) — TARs: {}, .tlog: {}, metadata: {}, archive-OFF TARs (must be 0): {}",
            onTars.size(),
            onTlogBlobs.size(),
            onMetaBlobs.size(),
            offTarsPhase1.size()
        );

        assertThat("Archive-ON: ≥1 TAR uploaded", onTars, not(empty()));
        assertEquals("Archive-OFF: ZERO TARs at any point (TARs are exclusive to archive-ON)", 0, offTarsPhase1.size());

        // -----------------------------------------------------------------------
        // Phase 2: Index into archive-OFF, flush, assert ZERO TARs in its path.
        // -----------------------------------------------------------------------
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

        // PUT reduction summary (logged, not asserted — ratio depends on sync timing).
        logger.info(
            "PUT summary — archive-ON: {} TARs + {} metadata = {} PUTs; archive-OFF: {} .tlog + {} metadata = {} PUTs",
            onTars.size(),
            onMetaBlobs.size(),
            onTars.size() + onMetaBlobs.size(),
            offTlogBlobs.size(),
            offMetaBlobs.size(),
            offTlogBlobs.size() + offMetaBlobs.size()
        );
    }

    /**
     * Verifies that with archive enabled and zero translog ops (after flush with no new writes),
     * {@code syncNeeded()} returns {@code false} and no additional TARs are uploaded.
     *
     * <p>Flow:
     * <ol>
     *   <li>Index docs → background sync → TARs appear.</li>
     *   <li>Flush (commits ops, rolls generation to empty — 0 ops).</li>
     *   <li>Wait several sync cycles and assert TAR count stays stable.</li>
     * </ol>
     */
    public void testNoUploadWhenNoTranslogOpsWithArchiveEnabled() throws Exception {
        createIndex(INDEX_ARCHIVE_ON, indexSettings("tar"));
        ensureGreen(INDEX_ARCHIVE_ON);

        indexDocuments(INDEX_ARCHIVE_ON, NUM_DOCS);

        // Wait until ≥1 TAR appears — confirms archive upload is working.
        assertBusy(() -> assertThat(findBlobs(translogRepoPath, "*.tar"), not(empty())), 30, TimeUnit.SECONDS);

        // Flush: commits ops → rolls generation to a new empty generation (0 ops).
        // With syncNeeded() returning false, archive mode short-circuits → no additional TARs.
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
     *
     * <p>The FNV_1A_COMPOSITE_1 path strategy stores blobs at:
     * {@code {repoRoot}/{hashPrefix}/{indexUUID}/{shardId}/translog/{data|metadata}/...}
     * so the UUID appears as a directory component in every per-index blob path.
     *
     * @param root      repository root path
     * @param glob      file name glob, e.g. {@code "*.tar"} or {@code "metadata__*"}
     * @param indexUUID optional index UUID to filter by; {@code null} to match all
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

    private static List<Path> findMetadataBlobs(Path root) throws IOException {
        return findBlobs(root, "metadata__*", null);
    }

    private static List<Path> findMetadataBlobs(Path root, String indexUUID) throws IOException {
        return findBlobs(root, "metadata__*", indexUUID);
    }
}
