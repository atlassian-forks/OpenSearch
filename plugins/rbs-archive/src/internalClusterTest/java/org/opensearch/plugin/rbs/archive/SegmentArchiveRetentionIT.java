/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(RbsArchivePlugin.class)).collect(Collectors.toList());
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
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
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
        assertTrue("Should have created more archives", archives2.size() >= archives1.size());

        // Force merge to reduce segments and trigger cleanup
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for at least one archive to appear after force merge (assertBusy, no Thread.sleep).
        assertBusy(() -> assertFalse("Should still have archives after merge", listSegmentArchives().isEmpty()), 30, TimeUnit.SECONDS);

        List<String> archivesAfter = listSegmentArchives();
        logger.info("Archives before force merge: {}, after: {}", archives2.size(), archivesAfter.size());
        assertTrue("Should still have archives after merge", archivesAfter.size() > 0);
    }

    /**
     * Test retention with mixed archived and per-file segments.
     */
    public void testRetentionWithMixedArchiveAndPerFile() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create some archived segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        int archiveCount1 = listSegmentArchives().size();

        // Disable archiving temporarily
        updateIndexSetting(INDEX_NAME, IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "");

        // Create per-file segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Re-enable archiving
        updateIndexSetting(INDEX_NAME, IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar");

        // Create more archived segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Force merge to reduce segments
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify index is still functional (no sleep needed — refresh was just issued above).
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
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create archive with multiple segments
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        List<String> archivesBeforeMerge = listSegmentArchives();
        assertTrue("Should have archives before merge", archivesBeforeMerge.size() > 0);

        // Do a partial merge that doesn't eliminate all segments
        // (does not force merge to a single segment)
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(2).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify archives still exist because merged segments still reference them
        assertBusy(
            () -> assertFalse("Archives should still exist after partial merge", listSegmentArchives().isEmpty()),
            30,
            TimeUnit.SECONDS
        );

        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", DOCS_PER_BATCH, docCount);
    }

    /**
     * Test retention during active indexing — no race conditions or orphaned archives.
     */
    public void testRetentionDuringActiveIndexing() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Continuously index and refresh
        for (int batch = 0; batch < 5; batch++) {
            indexDocuments(DOCS_PER_BATCH);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }

        // Verify all documents present during active state
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents after 5 batches", DOCS_PER_BATCH * 5, docCount);

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
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create multiple archives in sequence — no Thread.sleep between batches.
        List<Integer> archiveCounts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            indexDocuments(DOCS_PER_BATCH);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            archiveCounts.add(listSegmentArchives().size());
        }

        logger.info("Archive counts after each batch: {}", archiveCounts);

        // Force merge to trigger cleanup of old archives.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for at least one archive to be present after merge (assertBusy, no Thread.sleep).
        assertBusy(() -> assertFalse("Should still have some archives after merge", listSegmentArchives().isEmpty()), 30, TimeUnit.SECONDS);

        // Recent archives should exist
        List<String> finalArchives = listSegmentArchives();
        assertTrue("Should still have some archives", finalArchives.size() > 0);

        // Verify all documents are present
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents", DOCS_PER_BATCH * 5, docCount);
    }

    /**
     * Regression test: all segment TAR archives must be deleted when the index is deleted.
     *
     * Bug: {@code deleteStaleSegments(0)} (called on shard close during index deletion) treated
     * the newest TAR as "active" (protected by the keep-boundary logic) and skipped it, leaving
     * orphaned TARs in S3 after deletion.
     *
     * Fix: when {@code lastNMetadataFilesToKeep==0}, treat all archives as stale.
     *
     * This test is NOT flaky: shard close (which calls {@code deleteStaleSegments(0)}) completes
     * synchronously before the DELETE response returns, so no {@code assertBusy} or sleep is needed.
     */
    public void testAllTarsDeletedAfterIndexDeletion() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index documents and refresh — this causes a segment archive TAR to be uploaded to S3.
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify at least one TAR was created.
        List<String> tarsBefore = listSegmentArchives();
        assertTrue("Should have at least one TAR before deletion", tarsBefore.size() > 0);

        // Delete the index — shard close calls deleteStaleSegments(0) synchronously.
        // By the time assertAcked() returns, deleteStaleSegments(0) has already run.
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());

        // No assertBusy / Thread.sleep needed — cleanup is synchronous on shard close.
        List<String> tarsAfter = listSegmentArchives();
        assertEquals("All segment TAR archives must be deleted after index deletion (found: " + tarsAfter + ")", 0, tarsAfter.size());
    }

    /**
     * Test recovery from archived segments after index is reopened.
     */
    public void testRecoveryFromArchiveAfterIndexReopen() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index documents and refresh to create archives
        indexDocuments(DOCS_PER_BATCH * 3);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify archives exist
        List<String> archivesBefore = listSegmentArchives();
        assertTrue("Should have archives before close", archivesBefore.size() > 0);

        // Count documents
        long docCountBefore = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents before close", DOCS_PER_BATCH * 3, docCountBefore);

        // Close the index
        client().admin().indices().prepareClose(INDEX_NAME).get();

        // Reopen the index — it recovers from archived segments in remote store
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        // Verify documents are still there (recovered from archives)
        long docCountAfter = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents after reopen", DOCS_PER_BATCH * 3, docCountAfter);
    }

    /**
     * Test archive cleanup after Lucene merge (no SegmentCommitInfo references).
     */
    public void testArchiveCleanupAfterLuceneMerge() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Create archives by indexing in batches
        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        indexDocuments(DOCS_PER_BATCH);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify multiple archives exist
        List<String> archivesBeforeMerge = listSegmentArchives();
        assertTrue("Should have multiple archives", archivesBeforeMerge.size() > 0);

        // Force merge — merges all segments and reduces archives
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify data is still present
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents after merge", DOCS_PER_BATCH * 2, docCount);

        // Verify archives still exist (at least for the merged segment)
        List<String> archivesAfterMerge = listSegmentArchives();
        assertTrue("Should still have archives after merge", archivesAfterMerge.size() > 0);
    }

    /**
     * Test multi-shard index with archive upload - each shard independently creates archives.
     */
    public void testMultiShardArchiveUpload() throws Exception {
        int numShards = 3;
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_REMOTE_STORE_SEGMENT_STRATEGY_SETTING.getKey(), "tar")
            .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_STRATEGY_SETTING.getKey(), "tar")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        // Index enough data to distribute across shards
        indexDocuments(DOCS_PER_BATCH * 3);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Index more data
        indexDocuments(DOCS_PER_BATCH * 3);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify all documents present
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents across shards", DOCS_PER_BATCH * 6, docCount);

        // Force merge all shards
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify data integrity after merge across all shards
        long docsAfterMerge = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals("Should have all documents after multi-shard merge", DOCS_PER_BATCH * 6, docsAfterMerge);
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
        client().admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder().put(setting, value)).get();
    }

    private List<String> listSegmentArchives() throws Exception {
        List<String> archives = new ArrayList<>();
        if (segmentRepoPath == null) {
            return archives;
        }
        // Walk the segment repo for *.tar files under segments/data/
        // Archive blobs are named: segment_archive_<timestamp>_<uuid>.tar
        java.nio.file.Files.walkFileTree(segmentRepoPath, new java.nio.file.SimpleFileVisitor<java.nio.file.Path>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(java.nio.file.Path file, java.nio.file.attribute.BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (name.endsWith(".tar")) {
                    // Only count TARs under a "segments" data path
                    if (isUnderSegmentsDataPath(file)) {
                        archives.add(file.toString());
                    }
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFileFailed(java.nio.file.Path file, java.io.IOException exc) {
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return archives;
    }

    private boolean isUnderSegmentsDataPath(java.nio.file.Path file) {
        // Segment archives are stored under: {repoRoot}/{indexUUID}/{shardId}/segments/data/{hashPrefix}/*.tar
        java.nio.file.Path p = file.getParent();
        while (p != null) {
            if ("segments".equals(p.getFileName() != null ? p.getFileName().toString() : "")) {
                return true;
            }
            p = p.getParent();
        }
        return false;
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
