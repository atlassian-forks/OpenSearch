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
import org.opensearch.action.support.PlainActionFuture;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
     * Wait until at least one translog archive ZIP appears in the translog repo and has been
     * present for at least 1 second, so the collector has time to include the latest ops.
     * Checks path translog/data/{hashPrefix}/{genBucket}/*.zip.
     */
    private volatile long firstArchiveSeenTimeNanos = 0L;

    private void waitForTranslogArchiveUpload() throws Exception {
        firstArchiveSeenTimeNanos = 0L;
        boolean ok = waitUntil(() -> {
            if (hasArchiveZipUnderRepo(translogRepoPath) == false) {
                firstArchiveSeenTimeNanos = 0L;
                return false;
            }
            long now = System.nanoTime();
            if (firstArchiveSeenTimeNanos == 0L) {
                firstArchiveSeenTimeNanos = now;
                return false;
            }
            return (now - firstArchiveSeenTimeNanos) >= TimeUnit.SECONDS.toNanos(1);
        }, 30, TimeUnit.SECONDS);
        assertTrue("expected at least one archive ZIP under translog repo " + translogRepoPath + " (stable for 1s)", ok);
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
