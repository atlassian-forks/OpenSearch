/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.rbs.RbsTarPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.NodeRoles;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public final class RbsTarIntegrationIT extends RemoteStoreBaseIntegTestCase {
    private static final Logger log = LogManager.getLogger(RbsTarIntegrationIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(RbsTarPlugin.class);
        plugins.add(InspectableRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings remoteStoreRepoSettings() {
        return remoteStoreClusterSettings(
            REPOSITORY_NAME,
            segmentRepoPath,
            InspectableRepositoryPlugin.TYPE,
            REPOSITORY_2_NAME,
            translogRepoPath,
            InspectableRepositoryPlugin.TYPE
        );
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.remote_store.translog.bundle.max_wait_ms", 10L).build();
    }

    public void testSegmentTarUploadAndRestore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String indexName = "test-segment-tar-idx";

        // 1. First run with default strategy to see baseline counts
        BlobStoreStats.reset();
        final Settings defaultSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put("index.remote_store.segment.strategy", "default")
            .build();
        createIndex(indexName + "-default", defaultSettings);
        ensureGreen(indexName + "-default");

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(indexName + "-default").setId(String.valueOf(i)).setSource("field", "value").get();
        }
        flushAndRefresh(indexName + "-default");

        final int defaultPuts = BlobStoreStats.putCount.get();

        // 2. Run with rbs-tar strategy
        BlobStoreStats.reset();
        final Settings tarSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put("index.remote_store.segment.strategy", "rbs-tar")
            .build();
        createIndex(indexName + "-tar", tarSettings);
        ensureGreen(indexName + "-tar");

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(indexName + "-tar").setId(String.valueOf(i)).setSource("field", "value").get();
        }
        flushAndRefresh(indexName + "-tar");

        final int tarPuts = BlobStoreStats.putCount.get();

        log.info("Default Segment PUTs: {}, Tar Segment PUTs: {}", defaultPuts, tarPuts);
        assertTrue("Tar PUTs should be lower than default PUTs", tarPuts < defaultPuts);

        // Close and restore index to verify recovery path
        assertAcked(client().admin().indices().prepareClose(indexName + "-tar"));
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(indexName + "-tar").restoreAllShards(true),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indexName + "-tar");

        // Verify document count matches
        assertHitCount(client().prepareSearch(indexName + "-tar").setSize(0).get(), 20);
    }

    public void testTranslogBundlingAndSelfHealing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String indexName = "test-translog-tar-idx";
        final Settings settings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put("index.remote_store.translog.strategy", "rbs-tar")
            .put("index.translog.durability", "request")
            .build();
        createIndex(indexName, settings);
        ensureGreen(indexName);
        Thread.sleep(1000); // Allow any asynchronous shard initialization tasks to complete

        BlobStoreStats.reset();
        final List<ActionFuture<IndexResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("field", "value").execute());
        }
        for (final ActionFuture<IndexResponse> future : futures) {
            future.actionGet();
        }

        final IndexShard indexShard = getIndexShard(dataNode, indexName);
        // Translogs are synced automatically on operations with REQUEST durability.

        final int translogPuts = BlobStoreStats.putCount.get();
        final int translogLists = BlobStoreStats.listCount.get();
        log.info("RBS-Tar Translog Indexing PUTs: {}, LISTs: {}", translogPuts, translogLists);

        assertTrue("Translog PUTs should be low (got " + translogPuts + ")", translogPuts < 15);
        assertEquals("Translog LISTs should be zero during normal indexing", 0, translogLists);

        // Shut down cluster manager node to trigger failover/no-master scenario
        final String clusterManagerName = internalCluster().getClusterManagerName();
        final Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(clusterManagerName);
        internalCluster().stopCurrentClusterManagerNode();

        final Path tempRecoveryPath = createTempDir();
        final IndexSettings indexSettings = indexShard.indexSettings();

        final RepositoriesService reposService = internalCluster().getInstance(RepositoriesService.class, dataNode);
        final BlobStoreRepository repo = (BlobStoreRepository) reposService.repository(REPOSITORY_2_NAME);

        RemoteFsTranslog.download(
            repo,
            indexShard.shardId(),
            internalCluster().getInstance(ThreadPool.class, dataNode),
            tempRecoveryPath,
            indexSettings.getRemoteStorePathStrategy(),
            indexShard.getRemoteStoreSettings(),
            log,
            false,
            true,
            System.currentTimeMillis(),
            false,
            internalCluster().getInstance(IndicesService.class, dataNode).getRemoteStoreTranslogStrategies().get("rbs-tar")
        );

        boolean tlgExists = false;
        try (java.util.stream.Stream<Path> filesStream = Files.list(tempRecoveryPath)) {
            tlgExists = filesStream.anyMatch(p -> p.getFileName().toString().endsWith(".tlog"));
        }

        // Restart cluster manager to ensure clean cluster state for teardown
        internalCluster().startNode(
            Settings.builder().put(NodeRoles.nonDataNode(NodeRoles.clusterManagerNode())).put(clusterManagerDataPathSettings)
        );

        assertTrue("Restored translog files should exist in recovery directory", tlgExists);
    }

    public void testRegistryCleanupAndGC() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String indexName = "test-translog-gc-idx";
        final Settings settings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put("index.remote_store.translog.strategy", "rbs-tar")
            .put("index.translog.durability", "request")
            .build();
        createIndex(indexName, settings);
        ensureGreen(indexName);

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(indexName).setId("initial-" + i).setSource("field", "value").get();
        }
        final IndexShard indexShard = getIndexShard(dataNode, indexName);

        flushAndRefresh(indexName);

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(indexName).setId("new-" + i).setSource("field", "value").get();
        }
        flushAndRefresh(indexName);

        final NodeBundleRegistry registry = internalCluster().getClusterManagerNodeInstance(NodeBundleRegistry.class);
        final RepositoriesService reposService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        final ClusterService clusterService = internalCluster().getClusterManagerNodeInstance(ClusterService.class);

        // Manually advance the min generation on the master registry to mark old bundles as garbage
        registry.updateShardMinGen(indexShard.shardId().getIndex().getUUID(), indexShard.shardId().getId(), 100L);

        BlobStoreStats.reset();
        registry.runGarbageCollection(() -> reposService, clusterService);

        assertTrue(BlobStoreStats.deleteCount.get() > 0);
    }
}
