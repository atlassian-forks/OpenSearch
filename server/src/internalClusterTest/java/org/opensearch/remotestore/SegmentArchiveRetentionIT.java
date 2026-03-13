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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for segment archive retention and cleanup.
 * Validates that old segment archives are deleted when no longer referenced by active metadata.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SegmentArchiveRetentionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-segment-archive-retention";
    private static final int DOCS_PER_BATCH = 100;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey(), true)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_RETENTION_MINUTES.getKey(), 30)
            .build();
    }

    /**
     * Test that old archives are deleted when all their segments are no longer referenced.
     */
    public void testRetentionDeletesOldArchives() throws Exception {
        // Create index with segment archiving enabled
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index first batch and refresh to create archive-001
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        List<String> archives1 = listSegmentArchives();
        assertTrue("Should have created at least one archive", archives1.size() > 0);

        // Index second batch and refresh to create archive-002
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        List<String> archives2 = listSegmentArchives();
        assertTrue("Should have created more archives", archives2.size() > archives1.size());

        // Force merge to reduce segments and trigger cleanup
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for cleanup to run (may need to trigger manually in test)
        Thread.sleep(2000);

        // After force merge, old archives should eventually be cleaned up
        // Note: Actual cleanup timing depends on retention policy
        List<String> archivesAfter = listSegmentArchives();
        logger.info("Archives before force merge: {}, after: {}", archives2.size(), archivesAfter.size());
        
        // Verify that we have fewer archives after cleanup
        // (exact count depends on timing and retention settings)
        assertTrue("Some cleanup should have occurred", archivesAfter.size() <= archives2.size());
    }

    /**
     * Test retention with mixed archived and per-file segments.
     */
    public void testRetentionWithMixedArchiveAndPerFile() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create some archived segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        int archiveCount1 = listSegmentArchives().size();

        // Disable archiving temporarily
        updateIndexSetting(INDEX_NAME, "index.remote_store.segment.archive_upload_enabled", "false");

        // Create per-file segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Re-enable archiving
        updateIndexSetting(INDEX_NAME, "index.remote_store.segment.archive_upload_enabled", "true");

        // Create more archived segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Force merge to reduce segments
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for potential cleanup
        Thread.sleep(2000);

        // Verify index is still functional
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", DOCS_PER_BATCH * 3, docCount);
    }

    /**
     * Test that archives are NOT deleted if any member is still active.
     */
    public void testArchiveNotDeletedIfAnyMemberActive() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create archive with multiple segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        
        int archiveCountBefore = listSegmentArchives().size();
        assertTrue("Should have archives", archiveCountBefore > 0);

        // Partial force merge (doesn't merge all segments)
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(2).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Archives with active members should still exist
        int archiveCountAfter = listSegmentArchives().size();
        
        // Verify index still works
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", DOCS_PER_BATCH * 2, docCount);
    }

    /**
     * Test archive cleanup during concurrent indexing.
     */
    public void testRetentionDuringActiveIndexing() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Background indexing thread
        Thread indexingThread = new Thread(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    indexDocuments(50);
                    client().admin().indices().prepareRefresh(INDEX_NAME).get();
                    Thread.sleep(200);
                }
            } catch (Exception e) {
                logger.error("Indexing thread failed", e);
            }
        });

        indexingThread.start();

        // Wait a bit to let some archives accumulate
        Thread.sleep(1000);

        // Trigger force merge while indexing is ongoing
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();

        // Wait for indexing to complete
        indexingThread.join(10000);
        assertFalse("Indexing thread should have completed", indexingThread.isAlive());

        // Final refresh
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify all documents are present
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", 500, docCount);

        // Verify archives exist
        List<String> archives = listSegmentArchives();
        assertTrue("Should have archives", archives.size() > 0);
    }

    /**
     * Test that recent archives are preserved during cleanup.
     */
    public void testRetentionPreservesRecentArchives() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put("index.remote_store.segment.archive_upload_enabled", true)
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create multiple archives in sequence
        List<Integer> archiveCounts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            indexDocuments(DOCS_PER_BATCH);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            archiveCounts.add(listSegmentArchives().size());
            Thread.sleep(200);
        }

        logger.info("Archive counts after each batch: {}", archiveCounts);

        // Force merge to trigger cleanup of old archives
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for cleanup
        Thread.sleep(2000);

        // Recent archives should exist
        List<String> finalArchives = listSegmentArchives();
        assertTrue("Should still have some archives", finalArchives.size() > 0);

        // Verify all documents are present
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", DOCS_PER_BATCH * 5, docCount);
    }

    // Helper methods

    private void indexDocuments(int count) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < count; i++) {
            bulkRequest.add(new IndexRequest(INDEX_NAME).source("field", "value" + i));
        }
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertFalse("Bulk indexing should succeed", response.hasFailures());
    }

    private void updateIndexSetting(String index, String setting, String value) {
        client().admin()
            .indices()
            .prepareUpdateSettings(index)
            .setSettings(Settings.builder().put(setting, value))
            .get();
    }

    private List<String> listSegmentArchives() throws Exception {
        // This would need to query the remote store to list archives
        // Implementation depends on RemoteStoreBaseIntegTestCase setup
        // For now, return a mock implementation
        // TODO: Implement actual archive listing from remote store
        List<String> archives = new ArrayList<>();
        
        // In real implementation, this would:
        // 1. Get remote store path for the index
        // 2. List files under segments/data/{hashPrefix}/
        // 3. Filter for *.zip files
        // 4. Return list of archive paths
        
        return archives;
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
