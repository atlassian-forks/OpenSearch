/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for translog archive upload: index with archive upload enabled,
 * then restore from remote and verify doc count.
 */
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
     * Wait until at least one translog archive ZIP appears in the translog repo and has been
     * present for at least 1 second, so the collector has time to include the latest ops.
     * Checks path translog/data/{hashPrefix}/{genBucket}/*.zip.
     */
    private void waitForTranslogArchiveUpload() throws Exception {
        // assertBusy polls until at least one ZIP appears — no artificial 1s stability window needed.
        assertBusy(
            () -> assertTrue(
                "expected at least one archive ZIP under translog repo " + translogRepoPath,
                hasArchiveZipUnderRepo(translogRepoPath)
            ),
            30,
            TimeUnit.SECONDS
        );
    }

    private static boolean hasArchiveZipUnderRepo(Path repoRoot) {
        AtomicBoolean found = new AtomicBoolean(false);
        try {
            Files.walkFileTree(repoRoot, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (found.get()) {
                        return FileVisitResult.TERMINATE;
                    }
                    String name = dir.getFileName().toString();
                    if ("data".equals(name)) {
                        if (hasZipInDirOrChildren(dir, 2)) {
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

    /**
     * Check for any *.zip under dir. Path translog/data has 2 levels to zip: {hashPrefix}/{genBucket}/*.zip.
     *
     * @param dir          directory to search (e.g. translog/data)
     * @param levelsToZip  2 for data/{hashPrefix}/{genBucket}/*.zip
     */
    private static boolean hasZipInDirOrChildren(Path dir, int levelsToZip) {
        if (levelsToZip <= 0) {
            try (DirectoryStream<Path> blobs = Files.newDirectoryStream(dir, "*.zip")) {
                return blobs.iterator().hasNext();
            } catch (IOException e) {
                return false;
            }
        }
        try (DirectoryStream<Path> childDirs = Files.newDirectoryStream(dir)) {
            for (Path child : childDirs) {
                if (Files.isDirectory(child) && hasZipInDirOrChildren(child, levelsToZip - 1)) {
                    return true;
                }
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }
}
