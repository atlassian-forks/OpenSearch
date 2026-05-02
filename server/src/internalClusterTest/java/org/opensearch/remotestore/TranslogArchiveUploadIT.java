/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.index.translog.TranslogArchiveTimerThreadLeakFilter;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for translog archive upload: index with archive upload enabled,
 * then restore from remote and verify doc count.
 */
@ThreadLeakFilters(filters = TranslogArchiveTimerThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TranslogArchiveUploadIT extends BaseRemoteStoreRestoreIT {

    /**
     * Index with archive upload enabled, stop primary, restore from remote, verify hit count.
     */
    public void testRestoreFromRemoteWithArchiveUploadEnabled() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        // Set buffer interval before data nodes start so TranslogArchiveCollector picks 50ms at doStart()
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Use emptyTranslog=false so last iteration leaves uncommitted ops for archive collector to upload
        Map<String, Long> indexStats = indexData(3, true, false, INDEX_NAME);
        waitForTranslogArchiveUpload();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true), future);
        future.actionGet();

        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Multi-shard index with archive upload: index data across multiple shards,
     * stop the primary node, restore from remote, verify all shards' data is recovered.
     */
    public void testRestoreMultiShardIndexWithArchiveUpload() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 3)) // 3 shards, 0 replicas
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(3, true, false, INDEX_NAME);
        waitForTranslogArchiveUpload();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true), future);
        future.actionGet();

        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Multi-node: primary uploads translog archives, stop primary,
     * restore from remote on surviving node → verify all data recovered.
     *
     * Unlike the single-node tests above, this proves archives uploaded by one node
     * are downloadable and usable by a different node for restore.
     */
    public void testRestoreOnDifferentNodeFromTranslogArchiveWhenPrimaryDown() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);

        // 1 shard, 0 replicas with archive enabled
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Index data and wait for archives
        Map<String, Long> indexStats = indexData(3, true, false, INDEX_NAME);
        waitForTranslogArchiveUpload();

        // Stop the primary node — shard goes red
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        // Restore on surviving data node using the archive
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true), future);
        future.actionGet();

        // Verify data restored on the surviving (different) node
        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Multi-shard multi-node: primary uploads translog archives for 3 shards,
     * stop primary → restore from remote on surviving node → all shards recovered.
     */
    public void testMultiShardRestoreOnDifferentNodeFromTranslogArchive() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);

        // 3 shards, 0 replicas with archive enabled
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 3))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(3, true, false, INDEX_NAME);
        waitForTranslogArchiveUpload();

        // Stop the primary node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        // Restore on surviving node
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true), future);
        future.actionGet();

        // All shards restored on different node
        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Index data in multiple batches with archives → stop primary → restore →
     * verify all batches' data recovered from multiple archive ZIPs.
     */
    public void testRestoreFromMultipleTranslogArchivesOnDifferentNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);

        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Multiple indexing iterations to create multiple archive ZIPs
        Map<String, Long> indexStats = indexData(5, true, false, INDEX_NAME);
        waitForTranslogArchiveUpload();

        // Stop primary → restore on different node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true), future);
        future.actionGet();

        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Restore + content verification: index docs with known IDs and field values,
     * archive-upload, kill primary, restore on different node, verify every doc by ID and content.
     *
     * This catches bugs that hitCount alone cannot: wrong-gen recovery, duplicate replay,
     * or cross-ZIP boundary loss.
     */
    public void testRestoreVerifiesDocumentContentNotJustCount() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);

        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Batch 1: flush → goes to segments, not translog
        Map<String, String> batch1 = indexKnownDocs(INDEX_NAME, 20);
        flushAndRefresh(INDEX_NAME);

        // Batch 2: NOT flushed → stays in translog → must be recovered from archive ZIP
        Map<String, String> batch2 = indexKnownDocs(INDEX_NAME, 20);

        waitForTranslogArchiveUpload();

        // Kill primary — batch2 only exists in the archive ZIP
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future1 = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true),
                future1
            );
        future1.actionGet();
        ensureGreen(org.opensearch.common.unit.TimeValue.timeValueSeconds(120), INDEX_NAME);

        // Verify ALL docs by ID and field value — catches count-correct but content-wrong bugs
        Map<String, String> all = new HashMap<>(batch1);
        all.putAll(batch2);
        verifyDocumentContent(INDEX_NAME, all);
    }

    /**
     * Same-node restart: primary restarts and must recover its own archive ZIPs from remote.
     * Verifies the hashNodeId path is correct (same node uploads and downloads its own ZIPs).
     */
    public void testSameNodeRestartRecoversFromArchive() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        String dataNode = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Flush batch 1 → segments
        Map<String, String> batch1 = indexKnownDocs(INDEX_NAME, 15);
        flushAndRefresh(INDEX_NAME);

        // Batch 2 stays in translog (not flushed) → must come from archive on restart
        Map<String, String> batch2 = indexKnownDocs(INDEX_NAME, 15);
        waitForTranslogArchiveUpload();

        // Restart the SAME node (not kill) — node comes back with same nodeId → reads its own hashNodeId dir
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback());
        ensureGreen(org.opensearch.common.unit.TimeValue.timeValueSeconds(120), INDEX_NAME);

        Map<String, String> all = new HashMap<>(batch1);
        all.putAll(batch2);
        verifyDocumentContent(INDEX_NAME, all);
    }

    /**
     * OFF→ON toggle IT: index with archive OFF, toggle archive ON, index more,
     * stop primary, restore → both batches must be present.
     *
     * Exercises Issue 1 fallback: at restore time, ZIP exists for batch2 but batch1's
     * ops were uploaded as per-shard tlog files (archive was OFF). Restore must recover all.
     */
    public void testRestoreAfterArchiveToggleOffToOn() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNodes(2);

        // Start with archive OFF
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), false)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Batch 1 with archive OFF — uploaded as per-shard tlog files
        Map<String, String> batch1 = indexKnownDocs(INDEX_NAME, 20);
        flushAndRefresh(INDEX_NAME);

        // Toggle archive ON
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true))
            .get();

        // Batch 2 with archive ON — uploaded as ZIP
        Map<String, String> batch2 = indexKnownDocs(INDEX_NAME, 20);
        waitForTranslogArchiveUpload();

        // Kill primary — batch2 only in ZIP, batch1 only in per-shard tlog
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        PlainActionFuture<RestoreRemoteStoreResponse> future3 = PlainActionFuture.newFuture();
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true).waitForCompletion(true),
                future3
            );
        future3.actionGet();
        ensureGreen(org.opensearch.common.unit.TimeValue.timeValueSeconds(120), INDEX_NAME);

        Map<String, String> all = new HashMap<>(batch1);
        all.putAll(batch2);
        verifyDocumentContent(INDEX_NAME, all);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Index {@code count} documents with known IDs and field values.
     * Returns a map of docId → fieldValue for later content verification.
     * Uses IMMEDIATE refresh so docs are searchable immediately (no flush needed for count checks).
     */
    private Map<String, String> indexKnownDocs(String indexName, int count) {
        Map<String, String> docs = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String id = UUIDs.randomBase64UUID();
            String value = randomAlphaOfLength(12);
            client().index(new IndexRequest(indexName).id(id).source("value", value)).actionGet();
            docs.put(id, value);
        }
        return docs;
    }

    /**
     * Verify every doc in {@code expected} exists with the correct field value.
     * Uses assertBusy to tolerate brief post-restore refresh lag.
     */
    private void verifyDocumentContent(String indexName, Map<String, String> expected) throws Exception {
        // Refresh to make all restored docs visible.
        client().admin().indices().prepareRefresh(indexName).get();
        // Verify count first — fast fail if obviously wrong.
        assertBusy(() -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), (long) expected.size()), 30, TimeUnit.SECONDS);
        // Verify each doc's content by ID — catches wrong-doc or missing-doc bugs.
        for (Map.Entry<String, String> e : expected.entrySet()) {
            GetResponse get = client().prepareGet(indexName, e.getKey()).get();
            assertTrue("Doc " + e.getKey() + " must exist after restore", get.isExists());
            assertEquals("Field value mismatch for doc " + e.getKey(), e.getValue(), get.getSourceAsMap().get("value"));
        }
    }

    /**
     * Dedicated test: verifies that uploaded TARs follow the exact hierarchical S3 path structure:
     * {@code txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar}
     *
     * <p>This catches regressions where the helper methods exist but are not wired into the actual
     * upload call (e.g. if someone accidentally uses the legacy flat path instead).
     */
    public void testTarUploadFollowsHierarchicalPathStructure() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "50ms").build()
            )
            .get();
        internalCluster().startDataOnlyNode();
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Flush first batch → goes to segments; second batch stays in translog → triggers archive upload
        indexKnownDocs(INDEX_NAME, 10);
        flushAndRefresh(INDEX_NAME);
        indexKnownDocs(INDEX_NAME, 10);
        waitForTranslogArchiveUpload();

        // Collect all uploaded TAR paths and assert each conforms to the spec.
        List<String> tarPaths = collectTarPaths(translogRepoPath);
        assertFalse("expected at least one TAR path to validate", tarPaths.isEmpty());
        for (String relPath : tarPaths) {
            assertTarPathStructure(relPath);
        }
    }

    /**
     * Wait until at least one translog archive TAR appears in the translog repo.
     * New path layout: txlog/{yyyyMMdd}/{HHmm}/*.tar
     */
    private void waitForTranslogArchiveUpload() throws Exception {
        assertBusy(
            () -> assertTrue(
                "expected at least one archive TAR under translog repo " + translogRepoPath,
                hasArchiveUnderRepo(translogRepoPath)
            ),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * Searches the repo root for any archive file (.tar or .zip) under the txlog/ directory.
     * New path: txlog/{day}/{minute}/*.tar
     */
    private static boolean hasArchiveUnderRepo(Path repoRoot) {
        AtomicBoolean found = new AtomicBoolean(false);
        try {
            Files.walkFileTree(repoRoot, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (found.get()) {
                        return FileVisitResult.TERMINATE;
                    }
                    String name = dir.getFileName().toString();
                    // New path: txlog/{day}/{minute}/*.tar  (2 levels deep from txlog)
                    if ("txlog".equals(name)) {
                        if (hasArchiveInDirOrChildren(dir, 2)) {
                            found.set(true);
                            return FileVisitResult.TERMINATE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            return false;
        }
        return found.get();
    }

    // Keep legacy name for backward compat with any callers
    private static boolean hasArchiveZipUnderRepo(Path repoRoot) {
        return hasArchiveUnderRepo(repoRoot);
    }

    /**
     * Check for any *.tar or *.zip archive under dir, recursing {@code levelsDeep} levels.
     *
     * @param dir        directory to search
     * @param levelsDeep number of directory levels to descend before looking for blobs
     */
    private static boolean hasArchiveInDirOrChildren(Path dir, int levelsDeep) {
        if (levelsDeep <= 0) {
            try (DirectoryStream<Path> blobs = Files.newDirectoryStream(dir, "*.tar")) {
                if (blobs.iterator().hasNext()) return true;
            } catch (IOException e) {
                return false;
            }
            try (DirectoryStream<Path> blobs = Files.newDirectoryStream(dir, "*.zip")) {
                if (blobs.iterator().hasNext()) return true;
            } catch (IOException e) {
                return false;
            }
            return false;
        }
        try (DirectoryStream<Path> childDirs = Files.newDirectoryStream(dir)) {
            for (Path child : childDirs) {
                if (Files.isDirectory(child) && hasArchiveInDirOrChildren(child, levelsDeep - 1)) {
                    return true;
                }
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    /**
     * Legacy: check for any *.zip under dir (kept for backward compat with old test helpers).
     */
    private static boolean hasZipInDirOrChildren(Path dir, int levelsToZip) {
        return hasArchiveInDirOrChildren(dir, levelsToZip);
    }

    /**
     * Collects all {@code *.tar} paths under {@code txlog/} in the repo root, returning each
     * as a relative path string: {@code txlog/{day}/{minute}/{blob}.tar}.
     */
    private static List<String> collectTarPaths(Path repoRoot) throws IOException {
        List<String> result = new ArrayList<>();
        Path txlogRoot = repoRoot.resolve("txlog");
        if (!Files.isDirectory(txlogRoot)) {
            return result;
        }
        // Walk: txlog/{day}/{minute}/*.tar
        try (DirectoryStream<Path> dayDirs = Files.newDirectoryStream(txlogRoot)) {
            for (Path dayDir : dayDirs) {
                if (!Files.isDirectory(dayDir)) continue;
                try (DirectoryStream<Path> minuteDirs = Files.newDirectoryStream(dayDir)) {
                    for (Path minuteDir : minuteDirs) {
                        if (!Files.isDirectory(minuteDir)) continue;
                        try (DirectoryStream<Path> blobs = Files.newDirectoryStream(minuteDir, "*.tar")) {
                            for (Path blob : blobs) {
                                // Build relative path: txlog/{day}/{minute}/{blob}
                                result.add("txlog/"
                                    + dayDir.getFileName() + "/"
                                    + minuteDir.getFileName() + "/"
                                    + blob.getFileName());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Asserts that a relative TAR path matches the exact hierarchical structure from the design:
     * <pre>txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar</pre>
     * where:
     * <ul>
     *   <li>{yyyyMMdd} — 8 digits</li>
     *   <li>{HHmm}     — 4 digits</li>
     *   <li>{ss}       — exactly 2 digits (00-59)</li>
     *   <li>{SSS}      — exactly 3 digits (000-999)</li>
     *   <li>{nodeIdShort} — exactly 8 alphanumeric chars</li>
     * </ul>
     *
     * @param relPath relative path like {@code txlog/20260502/1430/45.123.a3f7b2c1.tar}
     */
    private static void assertTarPathStructure(String relPath) {
        // Pattern: txlog/{8 digits}/{4 digits}/{2 digits}.{3 digits}.{8 alnum}.tar
        Pattern pattern = Pattern.compile(
            "txlog/\\d{8}/\\d{4}/\\d{2}\\.\\d{3}\\.[a-zA-Z0-9]{8}\\.tar"
        );
        assertTrue(
            "TAR path does not match expected structure 'txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar', got: " + relPath,
            pattern.matcher(relPath).matches()
        );
    }
}
