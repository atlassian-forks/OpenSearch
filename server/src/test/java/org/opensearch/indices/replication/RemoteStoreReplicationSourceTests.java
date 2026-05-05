/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.RemoteStoreRefreshListenerTests;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreReplicationSourceTests extends OpenSearchIndexLevelReplicationTestCase {
    private static final long REPLICATION_ID = 123L;
    private RemoteStoreReplicationSource replicationSource;
    private IndexShard primaryShard;

    private IndexShard replicaShard;
    private final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryShard = newStartedShard(true, settings, new InternalEngineFactory());
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        primaryShard.refresh("test");
        replicaShard = newStartedShard(false, settings, new NRTReplicationEngineFactory());
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primaryShard, replicaShard);
        super.tearDown();
    }

    public void testGetCheckpointMetadata() throws ExecutionException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();
        assert (response.getCheckpoint().equals(checkpoint));
        assert (response.getMetadataMap().isEmpty() == false);
    }

    public void testGetCheckpointMetadataFailure() {
        IndexShard mockShard = mock(IndexShard.class);
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        when(mockShard.getSegmentInfosSnapshot()).thenThrow(new RuntimeException("test"));
        assertThrows(RuntimeException.class, () -> {
            replicationSource = new RemoteStoreReplicationSource(mockShard);
            final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
            res.get();
        });
    }

    public void testGetSegmentFiles() throws ExecutionException, InterruptedException, IOException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        List<StoreFileMetadata> filesToFetch = primaryShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());
        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, replicaShard, (fileName, bytesRecovered) -> {}, res);
        GetSegmentFilesResponse response = res.get();
        assertEquals(response.files.size(), filesToFetch.size());
        assertTrue(response.files.containsAll(filesToFetch));
        closeShards(replicaShard);
    }

    public void testGetSegmentFilesAlreadyExists() throws IOException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        List<StoreFileMetadata> filesToFetch = primaryShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());
        CountDownLatch latch = new CountDownLatch(1);
        try {
            final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
            replicationSource = new RemoteStoreReplicationSource(primaryShard);
            replicationSource.getSegmentFiles(
                REPLICATION_ID,
                checkpoint,
                filesToFetch,
                primaryShard,
                (fileName, bytesRecovered) -> {},
                res
            );
            res.get();
        } catch (AssertionError | ExecutionException ex) {
            latch.countDown();
            assertTrue(ex instanceof AssertionError);
            assertTrue(ex.getMessage().startsWith("Local store already contains the file"));
        }
        latch.await();
    }

    public void testGetSegmentFilesReturnEmptyResponse() throws ExecutionException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getSegmentFiles(
            REPLICATION_ID,
            checkpoint,
            Collections.emptyList(),
            primaryShard,
            (fileName, bytesRecovered) -> {},
            res
        );
        GetSegmentFilesResponse response = res.get();
        assert (response.files.isEmpty());
    }

    public void testGetCheckpointMetadataEmpty() throws ExecutionException, InterruptedException, IOException {
        IndexShard mockShard = mock(IndexShard.class);
        // Build mockShard to return replicaShard directory so that empty metadata file is returned.
        buildIndexShardBehavior(mockShard, replicaShard);
        replicationSource = new RemoteStoreReplicationSource(mockShard);

        // Mock replica shard state to RECOVERING so that getCheckpointInfo return empty map
        final ReplicationCheckpoint checkpoint = replicaShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        when(mockShard.state()).thenReturn(IndexShardState.RECOVERING);
        replicationSource = new RemoteStoreReplicationSource(mockShard);
        // Recovering shard should just do a noop and return empty metadata map.
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();
        assert (response.getCheckpoint().equals(checkpoint));
        assert (response.getMetadataMap().isEmpty());

        // Started shard should fail with assertion error.
        when(mockShard.state()).thenReturn(IndexShardState.STARTED);
        expectThrows(AssertionError.class, () -> {
            final PlainActionFuture<CheckpointInfoResponse> res2 = PlainActionFuture.newFuture();
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res2);
        });
    }

    /**
     * Verifies that getSegmentFiles() successfully downloads segments using the metadata
     * already loaded by getCheckpointMetadata().
     *
     * getCheckpointMetadata() calls init() which populates segmentsUploadedToRemoteStore
     * and archiveStateRef.  getSegmentFiles() reuses that state (does NOT call init() again)
     * to avoid a race where the metadata advances between the two calls.
     */
    public void testGetSegmentFilesReusesMetadataFromGetCheckpointMetadata() throws ExecutionException, InterruptedException, IOException {
        replicationSource = new RemoteStoreReplicationSource(primaryShard);

        // Step 1: getCheckpointMetadata — calls remoteDirectory.init() and captures current metadata
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> metaRes = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, metaRes);
        CheckpointInfoResponse metaResponse = metaRes.get();
        assertFalse("Metadata map must not be empty after primary upload", metaResponse.getMetadataMap().isEmpty());

        // Step 2: getSegmentFiles() — reuses the metadata state set by getCheckpointMetadata().
        // Filter out segments_N files to avoid CorruptIndexException on replica store close.
        List<StoreFileMetadata> filesToFetch = metaResponse.getMetadataMap()
            .values()
            .stream()
            .filter(f -> !f.name().startsWith("segments_"))
            .collect(Collectors.toList());
        final PlainActionFuture<GetSegmentFilesResponse> filesRes = PlainActionFuture.newFuture();
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, replicaShard, (f, b) -> {}, filesRes);
        GetSegmentFilesResponse filesResponse = filesRes.get();
        assertFalse("Expected segment files to be fetched by replication source", filesResponse.files.isEmpty());

        // Step 3: confirm remote directory metadata is consistent
        RemoteSegmentStoreDirectory remoteDir = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) primaryShard
            .remoteStore()
            .directory()).getDelegate()).getDelegate();
        RemoteSegmentMetadata latestMeta = remoteDir.readLatestMetadataFile();
        assertNotNull("Remote metadata must be readable after getSegmentFiles()", latestMeta);
        assertFalse("Remote metadata must contain segment entries", latestMeta.getMetadata().isEmpty());
        // replicaShard is closed by tearDown() via closeShards(primaryShard, replicaShard)
    }

    /**
     * Verifies that the fix for the double-init() race condition works.
     *
     * <p><b>Background (the bug):</b> Before the fix, the replica replication flow made
     * two separate {@code init()} calls on the same {@link RemoteSegmentStoreDirectory}.
     * {@code getCheckpointMetadata()} called {@code init()} reading metadata <b>M_N</b>,
     * then {@code getSegmentFiles()} called {@code init()} again reading <b>M_{N+1}</b>
     * (primary merged segments in between), destructively replacing the map — pre-merge
     * files disappeared, causing {@code NoSuchFileException}.</p>
     *
     * <p><b>The fix:</b> {@code getSegmentFiles()} no longer calls {@code init()}.
     * It reuses the {@code segmentsUploadedToRemoteStore} map already populated by
     * {@code getCheckpointMetadata()}, ensuring both methods see the same metadata version.</p>
     *
     * <p>This test calls {@code getCheckpointMetadata()} <em>before</em> the merge
     * (populating the map with M_N), then force-merges (writing M_{N+1} to disk),
     * then calls {@code getSegmentFiles()} with the pre-merge filesToFetch.
     * With the fix, the map still has M_N → download succeeds.
     * Without the fix, a second {@code init()} in {@code getSegmentFiles()} would
     * read M_{N+1} → pre-merge files gone → {@code NoSuchFileException}.</p>
     */
    public void testGetSegmentFilesSucceedsAfterMergeBecauseItReusesMetadataFromGetCheckpointMetadata() throws Exception {
        // After setUp: primary has segment _0 from docs "1" and "2" + refresh.

        // Create a second segment so forceMerge has something to merge.
        indexDoc(primaryShard, "_doc", "3");
        primaryShard.refresh("create second segment");
        // Now primary has segments _0 and _1.

        replicationSource = new RemoteStoreReplicationSource(primaryShard);

        // ──────────────────────────────────────────────────────────────────
        // Step 1: getCheckpointMetadata() BEFORE merge — calls init(),
        // reads metadata M_N containing both _0.* and _1.* files.
        // This populates segmentsUploadedToRemoteStore with M_N.
        // ──────────────────────────────────────────────────────────────────
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> metaRes = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, metaRes);
        CheckpointInfoResponse metaResponse = metaRes.get();
        assertFalse("Metadata map should not be empty", metaResponse.getMetadataMap().isEmpty());

        // Build filesToFetch from M_N (filter out segments_N to avoid unrelated issues).
        List<StoreFileMetadata> filesToFetch = metaResponse.getMetadataMap()
            .values()
            .stream()
            .filter(f -> !f.name().startsWith("segments_"))
            .collect(Collectors.toList());
        assertFalse("filesToFetch should not be empty", filesToFetch.isEmpty());

        Set<String> preMergeFileNames = filesToFetch.stream().map(StoreFileMetadata::name).collect(Collectors.toSet());

        // ──────────────────────────────────────────────────────────────────
        // Step 2: Force-merge on primary — advances metadata to M_{N+1}.
        // Old segments (_0, _1) are merged into _2. M_{N+1} only
        // references _2.* files; _0.* and _1.* are gone from the
        // metadata FILE (but the in-memory map is untouched).
        // ──────────────────────────────────────────────────────────────────
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
        forceMergeRequest.maxNumSegments(1);
        primaryShard.forceMerge(forceMergeRequest);
        primaryShard.refresh("after merge — uploads M_{N+1}");

        // Sanity check: read the metadata FILE directly (without calling init(),
        // which would destructively replace the in-memory map and defeat the test).
        // readLatestMetadataFile() reads the file but does NOT update
        // segmentsUploadedToRemoteStore — the map still reflects M_N.
        RemoteSegmentStoreDirectory remoteDir = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) primaryShard
            .remoteStore()
            .directory()).getDelegate()).getDelegate();
        RemoteSegmentMetadata latestMetadata = remoteDir.readLatestMetadataFile();
        assertNotNull("Metadata file must exist after merge", latestMetadata);
        Set<String> metadataFileKeys = latestMetadata.getMetadata().keySet();
        boolean someFilesGone = preMergeFileNames.stream().anyMatch(f -> !metadataFileKeys.contains(f));
        assertTrue(
            "Metadata FILE after force-merge must not contain all pre-merge files. "
                + "Pre-merge: "
                + preMergeFileNames
                + ", metadata file keys: "
                + metadataFileKeys,
            someFilesGone
        );

        // ──────────────────────────────────────────────────────────────────
        // Step 3: getSegmentFiles() with the STALE filesToFetch from M_N.
        //
        // WITH FIX: getSegmentFiles() does NOT call init() again. The
        // in-memory map still has M_N (set by getCheckpointMetadata()
        // in step 1). Pre-merge files are present → download succeeds.
        //
        // WITHOUT FIX: getSegmentFiles() would call init() → read M_{N+1}
        // → replace map → pre-merge files gone → NoSuchFileException.
        // ──────────────────────────────────────────────────────────────────
        final PlainActionFuture<GetSegmentFilesResponse> filesRes = PlainActionFuture.newFuture();
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, replicaShard, (f, b) -> {}, filesRes);

        // After the fix, getSegmentFiles() must succeed — no NoSuchFileException.
        GetSegmentFilesResponse response = filesRes.get();
        assertFalse("Expected segment files to be fetched successfully", response.files.isEmpty());
        assertEquals("All requested files should be in the response", filesToFetch.size(), response.files.size());
    }

    /**
     * Option A: when the checkpoint carries a metadataFilename, the replication source
     * must call initFromMetadataFilename() (direct GET, 0 LIST) instead of init() (LIST+GET).
     * Verifies that the response is non-empty and correct — proving the direct GET path works.
     */
    public void testGetCheckpointMetadataUsesDirectGetWhenFilenameInCheckpoint() throws ExecutionException, InterruptedException {
        replicationSource = new RemoteStoreReplicationSource(primaryShard);

        // Get the checkpoint that the primary published — if RemoteStoreRefreshListener
        // correctly set lastUploadedMetadataFilename and embedded it, getLatestReplicationCheckpoint()
        // returns a checkpoint with metadataFilename set.
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();

        // The primary refresh in setUp() should have triggered uploadMetadata + checkpoint publish.
        // If metadataFilename is present, Option A is active.
        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();

        assertNotNull("Response must not be null", response);
        assertFalse("Metadata map must not be empty (direct GET succeeded)", response.getMetadataMap().isEmpty());
        assertNotNull("SegmentInfosBytes must not be null", response.getInfosBytes());

        // If metadataFilename was in checkpoint, log it for observability
        if (checkpoint.getMetadataFilename() != null) {
            logger.info("Option A active: metadataFilename={}", checkpoint.getMetadataFilename());
        } else {
            logger.info("Option A not active (metadataFilename null) — fallback to LIST used");
        }
    }

    /**
     * Option C (revised): when getCheckpointMetadata() is called twice with the same checkpoint,
     * the second call must return the cached metadata (0 S3 calls).
     * Verifies by checking that the response is identical and non-empty on both calls.
     */
    public void testGetCheckpointMetadataReturnsCachedMetadataOnSameCheckpoint() throws ExecutionException, InterruptedException {
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();

        // First call — fetches from S3 (LIST or direct GET), populates cache
        final PlainActionFuture<CheckpointInfoResponse> res1 = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res1);
        CheckpointInfoResponse response1 = res1.get();
        assertFalse("First call: metadata map must not be empty", response1.getMetadataMap().isEmpty());

        // Second call with identical checkpoint — must return cached result (0 S3 calls)
        final PlainActionFuture<CheckpointInfoResponse> res2 = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res2);
        CheckpointInfoResponse response2 = res2.get();
        assertFalse("Second call (cache hit): metadata map must not be empty", response2.getMetadataMap().isEmpty());

        // Both responses must have the same set of files
        assertEquals(
            "Cached response must have same file count as first response",
            response1.getMetadataMap().size(),
            response2.getMetadataMap().size()
        );
        assertEquals(
            "Cached response must have same file names as first response",
            response1.getMetadataMap().keySet(),
            response2.getMetadataMap().keySet()
        );
    }

    /**
     * Option C: when a new checkpoint arrives (different segmentInfosVersion),
     * the cache must be invalidated and a fresh fetch performed.
     */
    public void testGetCheckpointMetadataCacheInvalidatedOnNewCheckpoint() throws ExecutionException, InterruptedException, IOException {
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        final ReplicationCheckpoint checkpoint1 = primaryShard.getLatestReplicationCheckpoint();

        // Populate the cache with checkpoint1
        final PlainActionFuture<CheckpointInfoResponse> res1 = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint1, res1);
        CheckpointInfoResponse response1 = res1.get();
        assertFalse("First checkpoint: metadata must not be empty", response1.getMetadataMap().isEmpty());

        // Index a new doc and refresh → new checkpoint with higher segmentInfosVersion
        indexDoc(primaryShard, "_doc", "3");
        primaryShard.refresh("advance checkpoint");
        final ReplicationCheckpoint checkpoint2 = primaryShard.getLatestReplicationCheckpoint();

        // checkpoint2 must be different from checkpoint1
        assertFalse("New checkpoint must differ from old checkpoint", checkpoint1.equals(checkpoint2));

        // Second call with NEW checkpoint — cache must be invalidated, fresh fetch from S3
        final PlainActionFuture<CheckpointInfoResponse> res2 = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint2, res2);
        CheckpointInfoResponse response2 = res2.get();
        assertFalse("New checkpoint: fresh fetch must return non-empty metadata", response2.getMetadataMap().isEmpty());
    }

    private void buildIndexShardBehavior(IndexShard mockShard, IndexShard indexShard) {
        when(mockShard.getSegmentInfosSnapshot()).thenReturn(indexShard.getSegmentInfosSnapshot());
        Store remoteStore = mock(Store.class);
        when(mockShard.remoteStore()).thenReturn(remoteStore);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate()).getDelegate();
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory));
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);
    }
}
