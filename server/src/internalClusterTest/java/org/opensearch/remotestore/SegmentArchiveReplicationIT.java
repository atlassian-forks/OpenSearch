/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration tests for segment archive upload and download across primary and replica nodes.
 *
 * These tests verify the critical invariants:
 *   1. Primary uploads segment files as ZIP archives (not individual files) to remote store
 *   2. Replica nodes can download and recover segment files from those archives
 *   3. Data integrity is preserved across the archive upload → remote store → archive download cycle
 *   4. Multiple refreshes produce multiple archives, all recoverable by replicas
 *   5. After primary failure, replica can still serve data recovered from archived segments
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentArchiveReplicationIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-segment-archive-replication";
    private static final int DOCS_PER_BATCH = 50;

    @Override
    public void setUp() throws Exception {
        // Force a fresh repo path per test so listSegmentArchives() cannot find ZIPs
        // written by a previous test in this class (segmentRepoPath is lazily initialized
        // in RemoteStoreBaseIntegTestCase and would otherwise be reused across tests).
        segmentRepoPath = null;
        translogRepoPath = null;
        super.setUp();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey(), true)
            .build();
    }

    /**
     * Test: Primary uploads archived segments, replica downloads and serves the same data.
     *
     * Flow:
     *   1. Start 2 data nodes (primary + replica)
     *   2. Create index with 1 shard, 1 replica, archive upload enabled, SEGMENT replication
     *   3. Index documents on primary → flush → refresh
     *   4. Assert archive ZIP blob was written to remote store
     *   5. Assert replica node (pinned via _local preference) serves the same document count
     *   6. Index more documents → flush → verify replica catches up
     */
    public void testReplicaDownloadsArchivedSegmentsFromPrimary() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Determine which node actually holds the replica (allocation is non-deterministic)
        String actualReplicaNode = getReplicaNodeName(INDEX_NAME, 0);

        // Index first batch and flush so archive upload is triggered
        indexDocuments(DOCS_PER_BATCH);
        flushAndRefresh(INDEX_NAME);

        // CRITICAL: assert an archive ZIP blob was actually written to the remote segment data path
        assertBusy(() -> {
            List<String> archives = listSegmentArchives();
            assertFalse("Primary must have uploaded at least one archive ZIP to remote store", archives.isEmpty());
        }, 30, TimeUnit.SECONDS);

        // Assert the replica shard is STARTED on its node (definitive role check via routing table)
        assertReplicaShardStarted(INDEX_NAME, 0, actualReplicaNode);

        // Assert replica node exclusively serves the data.
        // Preference.ONLY_LOCAL ("_only_local") routes only to the local node's shard copy;
        // it throws NoShardAvailableActionException if no local copy exists — unlike "_local"
        // which silently falls back to any node. See Preference.ONLY_LOCAL in OperationRouting.
        assertBusy(() -> {
            SearchResponse replicaResponse = client(actualReplicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local") // Preference.ONLY_LOCAL — fail if no local shard
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(replicaResponse, DOCS_PER_BATCH);
        }, 30, TimeUnit.SECONDS);

        // Index second batch
        indexDocuments(DOCS_PER_BATCH);
        flushAndRefresh(INDEX_NAME);

        // Verify replica catches up with second batch
        assertBusy(() -> {
            SearchResponse replicaResponse = client(actualReplicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local") // Preference.ONLY_LOCAL — fail if no local shard
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(replicaResponse, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: Multiple refreshes create multiple archives, all recoverable by replica.
     *
     * Flow:
     *   1. Start primary + replica
     *   2. Index small batches with refresh between each (creates multiple archive ZIPs)
     *   3. Assert archive count grows after each batch (each refresh = new archive)
     *   4. Force merge → verify data integrity on both primary and replica
     */
    public void testMultipleArchiveUploadsRecoverableByReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Determine which node actually holds the replica (allocation is non-deterministic)
        String actualReplicaNode = getReplicaNodeName(INDEX_NAME, 0);

        // Index 5 batches with flush between each → creates multiple archives
        int totalDocs = 0;
        for (int batch = 0; batch < 5; batch++) {
            indexDocuments(DOCS_PER_BATCH);
            totalDocs += DOCS_PER_BATCH;
            flushAndRefresh(INDEX_NAME);

            final int expectedDocs = totalDocs;
            final int batchNum = batch;

            // Assert archive count has grown (each flush with new segments should produce a new ZIP)
            assertBusy(() -> {
                List<String> archives = listSegmentArchives();
                assertTrue(
                    "Archive count should grow after batch " + batchNum + ", got: " + archives.size(),
                    archives.size() > batchNum  // at least one archive per batch
                );
            }, 30, TimeUnit.SECONDS);

            // Assert replica shard role on first batch (routing table confirms it's not primary)
            if (batch == 0) {
                assertReplicaShardStarted(INDEX_NAME, 0, actualReplicaNode);
            }

            // Assert replica exclusively serves the data (_only_local proves local shard copy exists)
            assertBusy(() -> {
                SearchResponse response = client(actualReplicaNode).prepareSearch(INDEX_NAME)
                    .setPreference("_only_local")
                    .setSize(0)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get();
                assertHitCount(response, expectedDocs);
            }, 30, TimeUnit.SECONDS);
        }

        // Force merge and verify data integrity on replica
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);

        final int finalTotalDocs = totalDocs;
        assertBusy(() -> {
            SearchResponse response = client(actualReplicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local")
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(response, finalTotalDocs);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: After primary node stops, replica (promoted to primary) still serves all data.
     *
     * This proves that archived segments downloaded by replica are complete and self-sufficient.
     *
     * Flow:
     *   1. Start primary + replica
     *   2. Index data → flush → assert archive blob exists
     *   3. Stop original primary node
     *   4. Assert replicaNode became the new primary via cluster routing state
     *   5. Verify promoted replica serves all documents (pinned to that node)
     *   6. Index more data on new primary → verify searchable
     */
    public void testReplicaServesDataAfterPrimaryFailure() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index data and flush to ensure archived segments are uploaded
        indexDocuments(DOCS_PER_BATCH * 2);
        flushAndRefresh(INDEX_NAME);

        // Assert archive blob was written before killing primary
        assertBusy(() -> {
            List<String> archives = listSegmentArchives();
            assertFalse("Archive ZIP must exist before primary failure", archives.isEmpty());
        }, 30, TimeUnit.SECONDS);

        // Verify cluster is fully green before stopping primary
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);

        // Stop original primary — replica should be promoted
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

        // Wait for cluster to stabilize (at least yellow = promoted replica is primary)
        ensureYellow(INDEX_NAME);

        // Assert that the replicaNode is now the primary shard holder
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertNotNull("Primary shard must be assigned", primary);
            assertTrue("Primary shard must be active", primary.active());
            String newPrimaryNodeId = primary.currentNodeId();
            String replicaNodeId = state.nodes().resolveNode(replicaNode).getId();
            assertEquals("replicaNode must have been promoted to primary", replicaNodeId, newPrimaryNodeId);
        }, 30, TimeUnit.SECONDS);

        // Verify promoted primary (former replica) serves all documents exclusively
        assertBusy(() -> {
            SearchResponse response = client(replicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local")
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(response, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);

        // Step 6 (was missing): Index more data on promoted primary → verify searchable
        indexDocuments(DOCS_PER_BATCH);
        flushAndRefresh(INDEX_NAME);

        assertBusy(() -> {
            SearchResponse response = client(replicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local")
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(response, DOCS_PER_BATCH * 3);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: Multi-shard index with archive upload — replicas recover all shards.
     *
     * Flow:
     *   1. Start 2 data nodes
     *   2. Create index with 3 shards, 1 replica, archive enabled
     *   3. Index data across all shards
     *   4. Assert archive ZIPs exist (one per shard that had new segments)
     *   5. Verify replica has all documents across all shards (node-pinned)
     */
    public void testMultiShardArchiveReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index enough data to touch all 3 shards
        int totalDocs = DOCS_PER_BATCH * 6; // 300 docs across 3 shards
        indexDocuments(totalDocs);
        flushAndRefresh(INDEX_NAME);

        // Assert archive ZIPs exist for the shards that uploaded
        assertBusy(() -> {
            List<String> archives = listSegmentArchives();
            assertFalse("At least one archive ZIP must exist after multi-shard upload", archives.isEmpty());
        }, 30, TimeUnit.SECONDS);

        // Verify all documents searchable on both nodes
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, totalDocs);
        }, 30, TimeUnit.SECONDS);

        // Index more and verify
        indexDocuments(DOCS_PER_BATCH * 3);
        flushAndRefresh(INDEX_NAME);
        int finalTotal = totalDocs + DOCS_PER_BATCH * 3;

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, finalTotal);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: New replica node joins cluster and recovers all archived segments.
     *
     * Flow:
     *   1. Start with 1 data node (primary only, 0 replicas)
     *   2. Index data → flush → assert archive blob exists on primary
     *   3. Add a second data node and increase replica count to 1
     *   4. New replica recovers segment files from archived ZIPs in remote store
     *   5. Verify new replica serves all documents (pinned via _local)
     */
    public void testNewReplicaRecoversFromArchivedSegments() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        // Start with 0 replicas
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index data on primary only
        indexDocuments(DOCS_PER_BATCH * 3);
        flushAndRefresh(INDEX_NAME);

        // Assert primary has written archives to remote store before replica joins
        assertBusy(() -> {
            List<String> archives = listSegmentArchives();
            assertFalse("Primary must have archived segments before replica joins", archives.isEmpty());
        }, 30, TimeUnit.SECONDS);

        // Add new data node and increase replicas to 1
        String newReplicaNode = internalCluster().startDataOnlyNode();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();

        // Wait for replica to fully recover from remote store (archived segments)
        ensureGreen(INDEX_NAME);

        // Assert new node holds a started replica shard (not primary)
        assertReplicaShardStarted(INDEX_NAME, 0, newReplicaNode);

        // Verify new replica exclusively serves all documents (_only_local proves local shard exists)
        assertBusy(() -> {
            SearchResponse response = client(newReplicaNode).prepareSearch(INDEX_NAME)
                .setPreference("_only_local")
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertHitCount(response, DOCS_PER_BATCH * 3);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Content verification: replica must serve each doc by ID with correct field value.
     *
     * Proves that archive download → decompression → segment recovery is byte-correct,
     * not just count-correct. A truncated or corrupt archive that happens to yield the
     * same doc count would still fail this test.
     */
    public void testReplicaDocumentContentMatchesPrimary() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        String actualReplicaNode = getReplicaNodeName(INDEX_NAME, 0);

        // Index known docs with tracked IDs and values
        Map<String, String> knownDocs = indexKnownDocs(30);
        flushAndRefresh(INDEX_NAME);

        // Wait for archive upload
        assertBusy(() -> assertFalse("Archive ZIP must exist", listSegmentArchives().isEmpty()), 30, TimeUnit.SECONDS);

        // Assert replica shard role
        assertReplicaShardStarted(INDEX_NAME, 0, actualReplicaNode);

        // Verify content on replica using _only_local GET — proves correct archive decompression
        verifyReplicaDocumentContent(actualReplicaNode, knownDocs);
    }

    /**
     * After primary failure, promoted replica must serve correct document content — not just count.
     * Enhances testReplicaServesDataAfterPrimaryFailure with per-doc content check.
     */
    public void testPromotedReplicaDocumentContentAfterPrimaryFailure() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        String replicaNode = getReplicaNodeName(INDEX_NAME, 0);

        // Index known docs, flush so replica recovers from archive
        Map<String, String> knownDocs = indexKnownDocs(30);
        flushAndRefresh(INDEX_NAME);

        assertBusy(() -> assertFalse("Archive must exist before kill", listSegmentArchives().isEmpty()), 30, TimeUnit.SECONDS);

        // Verify replica has correct content before kill
        verifyReplicaDocumentContent(replicaNode, knownDocs);

        // Kill primary — replica promoted
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        ensureYellow(INDEX_NAME);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertNotNull("Primary must be assigned", primary);
            assertTrue("Primary must be active", primary.active());
            assertEquals("replicaNode must be new primary", state.nodes().resolveNode(replicaNode).getId(), primary.currentNodeId());
        }, 30, TimeUnit.SECONDS);

        // After promotion, content must still be correct on the new primary (former replica)
        verifyReplicaDocumentContent(replicaNode, knownDocs);
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * Index {@code count} docs with known IDs and field values. Returns id → value map.
     * Does NOT flush — caller decides when to flush.
     */
    private Map<String, String> indexKnownDocs(int count) {
        Map<String, String> docs = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String id = UUIDs.randomBase64UUID();
            String value = randomAlphaOfLength(10);
            client().index(new IndexRequest(INDEX_NAME).id(id).source("value", value)).actionGet();
            docs.put(id, value);
        }
        return docs;
    }

    /**
     * Verify each doc exists on the given node's local shard with the correct field value.
     * Uses {@code _only_local} GET routed to {@code node} — proves the local shard copy has correct data.
     */
    private void verifyReplicaDocumentContent(String node, Map<String, String> expected) throws Exception {
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // Count check first — fast fail
        assertBusy(
            () -> assertHitCount(client(node).prepareSearch(INDEX_NAME).setPreference("_only_local").setSize(0).get(), expected.size()),
            30,
            TimeUnit.SECONDS
        );
        // Per-doc content check via GET routed to the specific node
        for (Map.Entry<String, String> e : expected.entrySet()) {
            GetResponse get = client(node).prepareGet(INDEX_NAME, e.getKey()).setPreference("_only_local").get();
            assertTrue("Doc " + e.getKey() + " must exist on replica " + node, get.isExists());
            assertEquals(
                "Field value mismatch on replica " + node + " for doc " + e.getKey(),
                e.getValue(),
                get.getSourceAsMap().get("value")
            );
        }
    }

    private void indexDocuments(int count) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < count; i++) {
            bulkRequest.add(new IndexRequest(INDEX_NAME).source("field", "value" + i, "num", i));
        }
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertFalse("Bulk indexing should succeed", response.hasFailures());
    }

    /**
     * Lists all segment archive ZIP blobs under the segment remote repository path.
     * Shared logic mirrors {@code SegmentArchiveRetentionIT#listSegmentArchives()}.
     */
    private List<String> listSegmentArchives() throws Exception {
        List<String> archives = new ArrayList<>();
        if (segmentRepoPath == null) {
            return archives;
        }
        Files.walkFileTree(segmentRepoPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.getFileName().toString().endsWith(".zip") && isUnderSegmentsDataPath(file)) {
                    archives.add(file.toString());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });
        return archives;
    }

    private boolean isUnderSegmentsDataPath(Path file) {
        Path p = file.getParent();
        while (p != null) {
            if ("segments".equals(p.getFileName() != null ? p.getFileName().toString() : "")) {
                return true;
            }
            p = p.getParent();
        }
        return false;
    }

    /**
     * Returns the node name holding the replica shard for the given index/shardId.
     * Allocation is non-deterministic — do not assume a specific node holds the replica.
     */
    private String getReplicaNodeName(String indexName, int shardId) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting replicaShard = state.routingTable()
            .index(indexName)
            .shard(shardId)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(s -> !s.primary())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No started replica shard for " + indexName + "/" + shardId));
        return state.nodes().get(replicaShard.currentNodeId()).getName();
    }

    /**
     * Asserts that the given node holds a STARTED, non-primary shard copy for the specified index/shard.
     *
     * <p>This is more definitive than {@code setPreference("_only_local")} alone:
     * <ul>
     *   <li>{@code _only_local} (see {@link org.opensearch.cluster.routing.Preference#ONLY_LOCAL}) routes
     *       exclusively to the local node's shard copy and throws {@code NoShardAvailableActionException}
     *       if absent — but it cannot distinguish primary from replica.</li>
     *   <li>This method checks the cluster routing table directly to assert the shard role
     *       ({@code !ShardRouting.primary()}) and state ({@code ShardRoutingState.STARTED}).</li>
     * </ul>
     * Used together, they guarantee data is served from a STARTED replica shard on the specified node.
     */
    private void assertReplicaShardStarted(String indexName, int shardId, String nodeName) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String nodeId = state.nodes().resolveNode(nodeName).getId();
        ShardRouting replicaShard = state.routingTable()
            .index(indexName)
            .shard(shardId)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(s -> nodeId.equals(s.currentNodeId()) && !s.primary())
            .findFirst()
            .orElse(null);
        assertNotNull("Node [" + nodeName + "] must hold a STARTED replica shard for " + indexName + "/" + shardId, replicaShard);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet());
        } catch (Exception e) {
            // Index may not exist if test failed before creation
            logger.warn("Could not delete index during tearDown: {}", e.getMessage());
        }
        super.tearDown();
    }
}
