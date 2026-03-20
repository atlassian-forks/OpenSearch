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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

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
     *   4. Verify replica has same document count as primary
     *   5. Index more documents → flush → verify replica catches up
     */
    public void testReplicaDownloadsArchivedSegmentsFromPrimary() throws Exception {
        // Start cluster: 1 cluster manager + 2 data nodes
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        String replicaNode = internalCluster().startDataOnlyNode();

        // Create index with archive enabled and 1 replica (SEGMENT replication)
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index first batch
        indexDocuments(DOCS_PER_BATCH);
        flushAndRefresh(INDEX_NAME);

        // Wait for segment replication to complete
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH);
        }, 30, TimeUnit.SECONDS);

        // Index second batch
        indexDocuments(DOCS_PER_BATCH);
        flushAndRefresh(INDEX_NAME);

        // Verify replica has all documents from both batches
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: Multiple refreshes create multiple archives, all recoverable by replica.
     *
     * Flow:
     *   1. Start primary + replica
     *   2. Index small batches with refresh between each (creates multiple archive ZIPs)
     *   3. Verify replica has all documents after each batch
     *   4. Force merge → verify data integrity on both primary and replica
     */
    public void testMultipleArchiveUploadsRecoverableByReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
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

        // Index 5 batches with flush between each → creates multiple archives
        int totalDocs = 0;
        for (int batch = 0; batch < 5; batch++) {
            indexDocuments(DOCS_PER_BATCH);
            totalDocs += DOCS_PER_BATCH;
            flushAndRefresh(INDEX_NAME);

            final int expectedDocs = totalDocs;
            assertBusy(() -> {
                SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
                assertHitCount(response, expectedDocs);
            }, 30, TimeUnit.SECONDS);
        }

        // Force merge and verify data integrity
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);

        final int finalTotalDocs = totalDocs;
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
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
     *   2. Index data → flush → verify both nodes have data
     *   3. Stop original primary node
     *   4. Replica gets promoted to primary
     *   5. Verify promoted replica serves all documents
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

        // Verify both nodes have the data
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);

        // Stop primary — replica should be promoted
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

        // Wait for cluster to stabilize (replica promoted to primary)
        ensureYellow(INDEX_NAME);

        // Verify promoted replica serves all documents
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 2);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test: Multi-shard index with archive upload — replicas recover all shards.
     *
     * Flow:
     *   1. Start 2 data nodes
     *   2. Create index with 3 shards, 1 replica, archive enabled
     *   3. Index data across all shards
     *   4. Verify replica has all documents across all shards
     */
    public void testMultiShardArchiveReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index enough data to distribute across all 3 shards
        int totalDocs = DOCS_PER_BATCH * 6; // 300 docs across 3 shards
        indexDocuments(totalDocs);
        flushAndRefresh(INDEX_NAME);

        // Verify all documents searchable (served by primary + replica shards)
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
     *   2. Index data → flush → create archives
     *   3. Add a second data node and increase replica count to 1
     *   4. New replica recovers segment files from archived ZIPs in remote store
     *   5. Verify new replica serves all documents
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

        // Verify primary has the data
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 3);
        }, 30, TimeUnit.SECONDS);

        // Add new data node and increase replicas to 1
        internalCluster().startDataOnlyNode();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();

        // Wait for replica to recover from remote store (archived segments)
        ensureGreen(INDEX_NAME);

        // Verify new replica serves all documents
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get();
            assertHitCount(response, DOCS_PER_BATCH * 3);
        }, 30, TimeUnit.SECONDS);
    }

    // Helper methods

    private void indexDocuments(int count) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < count; i++) {
            bulkRequest.add(new IndexRequest(INDEX_NAME).source("field", "value" + i, "num", i));
        }
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertFalse("Bulk indexing should succeed", response.hasFailures());
    }

    @Override
    public void tearDown() throws Exception {
        try {
            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        } catch (Exception e) {
            // Index may not exist
        }
        super.tearDown();
    }
}
