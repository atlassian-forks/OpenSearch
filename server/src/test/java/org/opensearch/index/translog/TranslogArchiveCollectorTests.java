/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.archive.ArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper;
import org.opensearch.index.translog.transfer.archive.ArchiveEntry;
import org.opensearch.index.translog.transfer.archive.ZipCentralDirectoryParser;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TranslogArchiveCollectorTests extends OpenSearchTestCase {

    /** Count archive blobs only; ignores filesystem metadata (e.g. .DS_Store on macOS). */
    private static long zipBlobCount(Map<String, BlobMetadata> blobs) {
        return blobs.keySet().stream().filter(name -> name.endsWith(".zip")).count();
    }

    public void testBuildArchiveFromEntries() throws IOException {
        TranslogArchiveCollector collector = new TranslogArchiveCollector(mock(IndicesService.class));
        String path = "idx-uuid/0/1/translog-2.tlog";
        byte[] content = "translog bytes".getBytes(StandardCharsets.UTF_8);
        byte[] zipBytes = collector.buildArchiveFromEntries(Collections.singletonList(ArchiveBuilder.fromBytes(path, content)));
        assertTrue(zipBytes.length > 0);
        long tailStart = Math.max(0, zipBytes.length - 4096);
        byte[] tail = Arrays.copyOfRange(zipBytes, (int) tailStart, zipBytes.length);
        List<ArchiveEntry> entries = ZipCentralDirectoryParser.parse(tail, tailStart);
        assertThat(entries, hasSize(1));
        assertThat(entries.get(0).getPath(), equalTo(path));
        assertThat(entries.get(0).getDataLength(), equalTo((long) content.length));
        byte[] extracted = Arrays.copyOfRange(
            zipBytes,
            (int) entries.get(0).getDataOffset(),
            (int) (entries.get(0).getDataOffset() + entries.get(0).getDataLength())
        );
        assertArrayEquals(content, extracted);
    }

    /**
     * Integration-style test: upload archive to real FsBlobStore, then verify blob exists
     * and central directory at end of blob is parseable (Step 6 plan).
     */
    public void testUploadArchiveToBlobStoreAndParseCentralDirectory() throws IOException {
        String path = "idx-uuid/0/1/translog-2.tlog";
        byte[] content = "translog bytes".getBytes(StandardCharsets.UTF_8);
        TranslogArchiveCollector collector = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] zipBytes = collector.buildArchiveFromEntries(Collections.singletonList(ArchiveBuilder.fromBytes(path, content)));
        assertTrue(zipBytes.length > 0);

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64,
                "translog_zip",
                "idx-uuid",
                "node-1"
            );
            BlobPath archivePath = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashPrefix).add("0");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("batch1.zip", zipBytes, 0L), archivePath, WritePriority.HIGH);

            BlobContainer container = blobStore.blobContainer(archivePath);
            Map<String, BlobMetadata> blobs = container.listBlobs();
            assertThat(blobs, notNullValue());
            assertThat("one archive blob", zipBlobCount(blobs), equalTo(1L));
            String blobName = blobs.keySet().stream().filter(n -> n.endsWith(".zip")).findFirst().orElseThrow();
            assertThat(blobName, endsWith(".zip"));

            long size = blobs.get(blobName).length();
            int tailLen = (int) Math.min(size, 4096);
            long tailStartOffset = size - tailLen;
            byte[] tail;
            try (InputStream in = container.readBlob(blobName, tailStartOffset, tailLen)) {
                tail = in.readAllBytes();
            }
            List<ArchiveEntry> entries = ZipCentralDirectoryParser.parse(tail, tailStartOffset);
            assertThat(entries, hasSize(1));
            assertThat(entries.get(0).getPath(), equalTo(path));
            assertThat(entries.get(0).getDataLength(), equalTo((long) content.length));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Step 8: deleteArchivesOlderThanRetentionNewPath deletes archive when all entries are past retention.
     */
    public void testDeleteArchivesOlderThanRetentionDeletesWhenPastRetention() throws IOException {
        String pathPrefix = "idx-uuid/0/1/";
        byte[] tlogContent = "tlog".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "ckp".getBytes(StandardCharsets.UTF_8);
        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.tlog", tlogContent),
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.ckp", ckpContent)
        );
        TranslogArchiveCollector collector = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] zipBytes = collector.buildArchiveFromEntries(entries);

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath dataBase = new BlobPath().add(uniqueBase).add("translog").add("data");
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                "translog_zip",
                "idx-uuid",
                "node-1"
            );
            BlobPath genPath = dataBase.add(hashPrefix).add("0");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("old.zip", zipBytes, 0L), genPath, WritePriority.HIGH);
            Map<String, BlobMetadata> before = blobStore.blobContainer(genPath).listBlobs();
            assertThat(zipBlobCount(before), equalTo(1L));

            Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new java.util.HashMap<>();
            retention.put("idx-uuid/0", new ArchiveDeletionHelper.RetentionBounds(1L, 3L));
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetentionNewPath(transferService, dataBase, retention);
            assertThat(deleted, equalTo(1));
            Map<String, BlobMetadata> after = blobStore.blobContainer(genPath).listBlobs();
            assertThat(zipBlobCount(after), equalTo(0L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Step 8: deleteArchivesOlderThanRetentionNewPath keeps archive when not past retention.
     */
    public void testDeleteArchivesOlderThanRetentionKeepsWhenNotPastRetention() throws IOException {
        String pathPrefix = "idx-uuid/0/1/";
        byte[] content = "x".getBytes(StandardCharsets.UTF_8);
        TranslogArchiveCollector collector = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] zipBytes = collector.buildArchiveFromEntries(
            Collections.singletonList(ArchiveBuilder.fromBytes(pathPrefix + "translog-2.tlog", content))
        );

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath dataBase = new BlobPath().add(uniqueBase).add("translog").add("data");
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                "translog_zip",
                "idx-uuid",
                "node-1"
            );
            BlobPath genPath = dataBase.add(hashPrefix).add("0");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("keep.zip", zipBytes, 0L), genPath, WritePriority.HIGH);
            Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new java.util.HashMap<>();
            retention.put("idx-uuid/0", new ArchiveDeletionHelper.RetentionBounds(1L, 2L));
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetentionNewPath(transferService, dataBase, retention);
            assertThat(deleted, equalTo(0));
            Map<String, BlobMetadata> after = blobStore.blobContainer(genPath).listBlobs();
            assertThat(zipBlobCount(after), equalTo(1L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testGetEligibleShardIdsEmptyWhenNoIndices() {
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());
        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService);
        List<ShardId> shardIds = collector.getEligibleShardIds();
        assertThat(shardIds, hasSize(0));
    }

    public void testGetEligibleShardIdsReturnsShardsWithRemoteTranslogAndArchiveEnabled() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(indexService.getShardOrNull(0)).thenReturn(shard);
        when(indicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService);
        List<ShardId> shardIds = collector.getEligibleShardIds();
        assertThat(shardIds, hasSize(1));
        assertThat(shardIds.get(0), equalTo(shardId));
    }

    public void testGetEligibleShardIdsSkipsWhenArchiveUploadDisabled() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService);
        List<ShardId> shardIds = collector.getEligibleShardIds();
        assertThat(shardIds, hasSize(0));
    }

    public void testRunBatchUploadsArchiveWhenShardsHavePendingData() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        // Use real metadata so setArchiveBlobPath/setArchiveEntryOffsets actually store values
        org.opensearch.index.translog.transfer.TranslogTransferMetadata realMeta =
            new org.opensearch.index.translog.transfer.TranslogTransferMetadata(1L, 5L, 3L, 1, "node-1");
        realMeta.setGenerationToPrimaryTermMapper(new java.util.HashMap<>());
        TransferSnapshot mockSnapshot = mock(TransferSnapshot.class);
        when(mockSnapshot.getTranslogTransferMetadata()).thenReturn(realMeta);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot, () -> {});
            return null;
        }).when(shard).buildSnapshotForArchive(any());

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        assertThat(collector.getEligibleShardIds(), hasSize(1));
        collector.runBatchForTesting();

        org.mockito.ArgumentCaptor<BlobPath> pathCaptor = org.mockito.ArgumentCaptor.forClass(BlobPath.class);
        org.mockito.ArgumentCaptor<String> nameCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(transferService).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            pathCaptor.capture(),
            nameCaptor.capture(),
            eq(WritePriority.HIGH),
            eq(null)
        );
        assertThat(nameCaptor.getValue(), endsWith(".zip"));
        String uploadedPath = pathCaptor.getValue().buildAsString();
        // Archive is index+node scoped: path must start with repo-root, not contain shardId
        assertThat(uploadedPath, org.hamcrest.Matchers.startsWith("repo-root/translog/data/"));
        assertThat(
            "archive path must not contain shard id '0'",
            uploadedPath,
            org.hamcrest.Matchers.not(org.hamcrest.Matchers.matchesRegex(".*repo-root/[^/]+/[^/]+/0/.*"))
        );
        assertThat(uploadedPath, org.hamcrest.Matchers.containsString("translog"));
        assertThat(uploadedPath, org.hamcrest.Matchers.containsString("data"));

        verify(transferManager).uploadMetadata(mockSnapshot);

        // Verify archive metadata fields are populated on the real metadata object
        assertNotNull("archiveBlobPath should be set on metadata", realMeta.getArchiveBlobPath());
        assertThat(realMeta.getArchiveBlobPath(), endsWith(".zip"));
        assertThat(realMeta.getArchiveBlobPath(), org.hamcrest.Matchers.containsString("translog/data/"));
        assertNotNull("archiveEntryOffsets should be set on metadata", realMeta.getArchiveEntryOffsets());

        // Verify NO individual per-shard translog upload happened
        verify(transferManager, org.mockito.Mockito.never()).transferSnapshot(any(), any());
    }

    /**
     * RunBatch with two contributing shards: one ZIP upload, metadata upload for each shard.
     * (Code review: verifies metadata upload is invoked after successful archive upload.)
     */
    public void testRunBatchMultiShardOneZipAndMetadataPerShard() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId0 = new ShardId(metadata.getIndex(), 0);
        ShardId shardId1 = new ShardId(metadata.getIndex(), 1);

        TransferService transferService = mock(TransferService.class);
        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        // Use separate real metadata per shard so archive fields are actually stored
        org.opensearch.index.translog.transfer.TranslogTransferMetadata realMeta0 =
            new org.opensearch.index.translog.transfer.TranslogTransferMetadata(1L, 5L, 3L, 1, "node-1");
        realMeta0.setGenerationToPrimaryTermMapper(new java.util.HashMap<>());
        org.opensearch.index.translog.transfer.TranslogTransferMetadata realMeta1 =
            new org.opensearch.index.translog.transfer.TranslogTransferMetadata(1L, 5L, 3L, 1, "node-1");
        realMeta1.setGenerationToPrimaryTermMapper(new java.util.HashMap<>());

        TransferSnapshot mockSnapshot0 = mock(TransferSnapshot.class);
        TransferSnapshot mockSnapshot1 = mock(TransferSnapshot.class);
        when(mockSnapshot0.getTranslogTransferMetadata()).thenReturn(realMeta0);
        when(mockSnapshot0.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot0.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot1.getTranslogTransferMetadata()).thenReturn(realMeta1);
        when(mockSnapshot1.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot1.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard0 = mock(IndexShard.class);
        when(shard0.shardId()).thenReturn(shardId0);
        when(shard0.routingEntry()).thenReturn(routing);
        when(shard0.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard0.isSyncNeeded()).thenReturn(true);
        when(shard0.supportsArchiveSnapshot()).thenReturn(true);
        when(shard0.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard0.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard0.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot0, () -> {});
            return null;
        }).when(shard0).buildSnapshotForArchive(any());

        IndexShard shard1 = mock(IndexShard.class);
        when(shard1.shardId()).thenReturn(shardId1);
        when(shard1.routingEntry()).thenReturn(routing);
        when(shard1.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard1.isSyncNeeded()).thenReturn(true);
        when(shard1.supportsArchiveSnapshot()).thenReturn(true);
        when(shard1.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard1.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard1.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot1, () -> {});
            return null;
        }).when(shard1).buildSnapshotForArchive(any());

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard0);
        when(indexService.getShardOrNull(1)).thenReturn(shard1);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        assertThat(collector.getEligibleShardIds(), hasSize(2));
        collector.runBatchForTesting();

        verify(transferService).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            any(),
            any(),
            eq(WritePriority.HIGH),
            eq(null)
        );
        org.mockito.ArgumentCaptor<TransferSnapshot> snapshotCaptor = org.mockito.ArgumentCaptor.forClass(TransferSnapshot.class);
        verify(transferManager, org.mockito.Mockito.times(2)).uploadMetadata(snapshotCaptor.capture());
        assertThat(snapshotCaptor.getAllValues(), hasSize(2));
        assertThat(snapshotCaptor.getAllValues().get(0), equalTo(mockSnapshot0));
        assertThat(snapshotCaptor.getAllValues().get(1), equalTo(mockSnapshot1));

        // Verify archive metadata is populated on BOTH shards' metadata objects
        assertNotNull("shard0 archiveBlobPath should be set", realMeta0.getArchiveBlobPath());
        assertNotNull("shard1 archiveBlobPath should be set", realMeta1.getArchiveBlobPath());
        assertThat(realMeta0.getArchiveBlobPath(), endsWith(".zip"));
        assertThat(realMeta1.getArchiveBlobPath(), endsWith(".zip"));
        // Both shards reference the same archive blob
        assertEquals("both shards should reference same archive", realMeta0.getArchiveBlobPath(), realMeta1.getArchiveBlobPath());
        assertNotNull("shard0 archiveEntryOffsets should be set", realMeta0.getArchiveEntryOffsets());
        assertNotNull("shard1 archiveEntryOffsets should be set", realMeta1.getArchiveEntryOffsets());
        // Both share the same offsets map (same ZIP)
        assertEquals(realMeta0.getArchiveEntryOffsets(), realMeta1.getArchiveEntryOffsets());

        // Verify NO individual per-shard translog upload happened
        verify(transferManager, org.mockito.Mockito.never()).transferSnapshot(any(), any());
    }

    /**
     * Step 10: when archive upload fails and fallback is enabled, per-shard transferSnapshot is invoked.
     */
    public void testRunBatchFallbackToPerShardWhenArchiveUploadFails() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        doThrow(new IOException("simulated archive upload failure")).when(transferService)
            .uploadBlobStream(
                any(java.io.InputStream.class),
                org.mockito.ArgumentMatchers.anyLong(),
                any(),
                any(),
                eq(WritePriority.HIGH),
                eq(null)
            );

        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        TransferSnapshot mockSnapshot = mock(TransferSnapshot.class);
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(1L);
        when(mockSnapshot.getTranslogTransferMetadata()).thenReturn(mockMeta);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getTranslogFileSnapshots()).thenReturn(Collections.emptySet());

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot, () -> {});
            return null;
        }).when(shard).buildSnapshotForArchive(any());

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        when(remoteStoreSettings.getTranslogArchiveFallbackToPerShard()).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        assertThat(collector.getEligibleShardIds(), hasSize(1));
        collector.runBatchForTesting();

        verify(transferManager).transferSnapshot(eq(mockSnapshot), any());
    }

    /**
     * When archive upload fails and fallback is disabled, per-shard transferSnapshot is not invoked.
     * (Code review: test for fallback disabled when upload fails.)
     */
    public void testRunBatchNoFallbackWhenArchiveUploadFails() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        doThrow(new IOException("simulated archive upload failure")).when(transferService)
            .uploadBlobStream(
                any(java.io.InputStream.class),
                org.mockito.ArgumentMatchers.anyLong(),
                any(),
                any(),
                eq(WritePriority.HIGH),
                eq(null)
            );

        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        TransferSnapshot mockSnapshot = mock(TransferSnapshot.class);
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(1L);
        when(mockSnapshot.getTranslogTransferMetadata()).thenReturn(mockMeta);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getTranslogFileSnapshots()).thenReturn(Collections.emptySet());

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot, () -> {});
            return null;
        }).when(shard).buildSnapshotForArchive(any());

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        when(remoteStoreSettings.getTranslogArchiveFallbackToPerShard()).thenReturn(false);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        assertThat(collector.getEligibleShardIds(), hasSize(1));
        collector.runBatchForTesting();

        verify(transferManager, never()).transferSnapshot(any(), any());
    }

    /**
     * When archive upload fails, release callbacks are still invoked in finally (no snapshot/release leak).
     */
    public void testRunBatchReleaseCallbackInvokedOnUploadFailure() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        doThrow(new IOException("simulated archive upload failure")).when(transferService)
            .uploadBlobStream(
                any(java.io.InputStream.class),
                org.mockito.ArgumentMatchers.anyLong(),
                any(),
                any(),
                eq(WritePriority.HIGH),
                eq(null)
            );

        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        TransferSnapshot mockSnapshot = mock(TransferSnapshot.class);
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(1L);
        when(mockSnapshot.getTranslogTransferMetadata()).thenReturn(mockMeta);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getTranslogFileSnapshots()).thenReturn(Collections.emptySet());

        AtomicInteger releaseCount = new AtomicInteger(0);
        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<TransferSnapshot, Runnable> consumer = inv.getArgument(0);
            consumer.accept(mockSnapshot, releaseCount::incrementAndGet);
            return null;
        }).when(shard).buildSnapshotForArchive(any());

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        when(remoteStoreSettings.getTranslogArchiveFallbackToPerShard()).thenReturn(false);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        collector.runBatchForTesting();

        assertThat(releaseCount.get(), equalTo(1));
    }

    /**
     * runBatch runs runArchiveRetention first; when a shard provides retention bounds and an archive is past retention, it is deleted.
     */
    public void testRunBatchRunsArchiveRetentionAndDeletesPastRetention() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        String indexUuid = metadata.getIndex().getUUID();
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        String pathPrefix = indexUuid + "/0/1/";
        byte[] tlogContent = "tlog".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "ckp".getBytes(StandardCharsets.UTF_8);
        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.tlog", tlogContent),
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.ckp", ckpContent)
        );
        TranslogArchiveCollector collectorForBuild = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] zipBytes = collectorForBuild.buildArchiveFromEntries(entries);

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath baseTranslogPath = new BlobPath().add(uniqueBase);
            BlobPath dataBase = baseTranslogPath.add("translog").add("data");
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                "translog_zip",
                indexUuid,
                "node-1"
            );
            BlobPath genPath = dataBase.add(hashPrefix).add("0");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("old.zip", zipBytes, 0L), genPath, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(genPath).listBlobs()), equalTo(1L));

            TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
            when(transferManager.getTransferService()).thenReturn(transferService);
            when(transferManager.getArchiveBasePath()).thenReturn(baseTranslogPath);

            IndexShard shard = mock(IndexShard.class);
            when(shard.shardId()).thenReturn(shardId);
            when(shard.isRemoteTranslogEnabled()).thenReturn(true);
            when(shard.isSyncNeeded()).thenReturn(false);
            when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
            when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
            when(shard.indexSettings()).thenReturn(indexSettings);
            when(shard.getArchiveRetentionBounds()).thenReturn(Optional.of(new ArchiveDeletionHelper.RetentionBounds(1L, 3L)));

            IndexService indexService = mock(IndexService.class);
            when(indexService.getIndexSettings()).thenReturn(indexSettings);
            when(indexService.getShardOrNull(0)).thenReturn(shard);

            IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.indexService(any())).thenReturn(indexService);
            when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

            RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
            when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
            when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);

            TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
            collector.runBatchForTesting();

            Map<String, BlobMetadata> after = blobStore.blobContainer(genPath).listBlobs();
            assertThat("archive past retention should be deleted by runArchiveRetention", zipBlobCount(after), equalTo(0L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Non-primary shards are skipped during runBatch — only primary shards contribute to archive.
     */
    public void testRunBatchSkipsNonPrimaryShards() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(false); // replica shard
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        collector.runBatchForTesting();

        // Non-primary shard → no upload, no buildSnapshotForArchive
        verify(transferService, never()).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            any(),
            any(),
            any(WritePriority.class),
            any()
        );
        verify(shard, never()).buildSnapshotForArchive(any());
    }

    /**
     * When isSyncNeeded() returns false, shard has no pending data and is skipped (no upload).
     */
    public void testRunBatchSkipsShardWhenSyncNotNeeded() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(false); // no pending data
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        collector.runBatchForTesting();

        // Sync not needed → no upload, no buildSnapshotForArchive
        verify(transferService, never()).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            any(),
            any(),
            any(WritePriority.class),
            any()
        );
        verify(shard, never()).buildSnapshotForArchive(any());
    }

    /**
     * snapshotToEntries: converts a TransferSnapshot into archive entries with correct path prefixes.
     */
    public void testSnapshotToEntriesConvertsCorrectly() throws IOException {
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(3L);

        // Create temp files for snapshot
        java.nio.file.Path tlogFile = createTempFile("translog-5", ".tlog");
        java.nio.file.Files.write(tlogFile, "tlog content".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        java.nio.file.Path ckpFile = createTempFile("translog-5", ".ckp");
        java.nio.file.Files.write(ckpFile, "ckp content".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        FileSnapshot.TranslogFileSnapshot tlogSnapshot = new FileSnapshot.TranslogFileSnapshot(3L, 5L, tlogFile, null);
        FileSnapshot.CheckpointFileSnapshot ckpSnapshot = new FileSnapshot.CheckpointFileSnapshot(3L, 5L, 3L, ckpFile, null);

        TransferSnapshot snapshot = mock(TransferSnapshot.class);
        when(snapshot.getTranslogTransferMetadata()).thenReturn(mockMeta);
        when(snapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(java.util.Collections.singleton(tlogSnapshot));
        when(snapshot.getCheckpointFileSnapshots()).thenReturn(java.util.Collections.singleton(ckpSnapshot));

        java.util.List<ArchiveBuilder.ArchiveBuildEntry> entries = TranslogArchiveCollector.snapshotToEntries(snapshot, "idx-uuid/0");

        assertThat(entries, hasSize(2));
        // Verify paths include pathPrefix/primaryTerm/filename
        java.util.Set<String> paths = new java.util.HashSet<>();
        for (ArchiveBuilder.ArchiveBuildEntry entry : entries) {
            paths.add(entry.getPath());
        }
        assertTrue("should contain tlog path", paths.stream().anyMatch(p -> p.startsWith("idx-uuid/0/3/") && p.endsWith(".tlog")));
        assertTrue("should contain ckp path", paths.stream().anyMatch(p -> p.startsWith("idx-uuid/0/3/") && p.endsWith(".ckp")));
    }

    /**
     * deleteArchivesOlderThanRetentionNewPath with empty retention bounds map: no-op, no archives deleted.
     */
    public void testDeleteArchivesWithEmptyRetentionBoundsIsNoOp() throws IOException {
        String pathPrefix = "idx-uuid/0/1/";
        byte[] content = "x".getBytes(StandardCharsets.UTF_8);
        TranslogArchiveCollector collectorForBuild = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] zipBytes = collectorForBuild.buildArchiveFromEntries(
            Collections.singletonList(ArchiveBuilder.fromBytes(pathPrefix + "translog-2.tlog", content))
        );

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath dataBase = new BlobPath().add(uniqueBase).add("translog").add("data");
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                "translog_zip",
                "idx-uuid",
                "node-1"
            );
            BlobPath genPath = dataBase.add(hashPrefix).add("0");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("keep.zip", zipBytes, 0L), genPath, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(genPath).listBlobs()), equalTo(1L));

            // Empty retention map → no archives should be deleted
            Map<String, ArchiveDeletionHelper.RetentionBounds> emptyRetention = new java.util.HashMap<>();
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetentionNewPath(transferService, dataBase, emptyRetention);
            assertThat("no archives should be deleted with empty retention", deleted, equalTo(0));
            assertThat(zipBlobCount(blobStore.blobContainer(genPath).listBlobs()), equalTo(1L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Retention with mixed old/new ZIPs: cluster has both coordinator-uploaded and collector-uploaded ZIPs.
     * Archives whose entries are all past retention bounds are deleted; those with entries within bounds are kept.
     */
    public void testRetentionWithMixedOldNewZips() throws IOException {
        String indexUuid = "idx-uuid";
        String pathPrefix0 = indexUuid + "/0/1/";
        String pathPrefix1 = indexUuid + "/1/1/";

        // Old ZIP: gen 2 (past retention bounds of minGen=1, maxGen=3)
        byte[] oldContent = "old".getBytes(StandardCharsets.UTF_8);
        TranslogArchiveCollector collectorForBuild = new TranslogArchiveCollector(mock(IndicesService.class));
        byte[] oldZipBytes = collectorForBuild.buildArchiveFromEntries(
            Arrays.asList(
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-2.tlog", oldContent),
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-2.ckp", oldContent)
            )
        );

        // New ZIP: gen 10 (well beyond retention bounds, should be kept because it's newer)
        byte[] newContent = "new".getBytes(StandardCharsets.UTF_8);
        byte[] newZipBytes = collectorForBuild.buildArchiveFromEntries(
            Arrays.asList(
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-10.tlog", newContent),
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-10.ckp", newContent)
            )
        );

        // Mixed ZIP: gen 2 for shard 0 (past retention for shard 0) AND gen 10 for shard 1 (kept)
        byte[] mixedZipBytes = collectorForBuild.buildArchiveFromEntries(
            Arrays.asList(
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-2.tlog", oldContent),
                ArchiveBuilder.fromBytes(pathPrefix0 + "translog-2.ckp", oldContent),
                ArchiveBuilder.fromBytes(pathPrefix1 + "translog-10.tlog", newContent),
                ArchiveBuilder.fromBytes(pathPrefix1 + "translog-10.ckp", newContent)
            )
        );

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath dataBase = new BlobPath().add(uniqueBase).add("translog").add("data");
            String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
                "translog_zip",
                indexUuid,
                "node-1"
            );
            BlobPath genPath = dataBase.add(hashPrefix).add("0");

            // Upload all three ZIPs
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("old.zip", oldZipBytes, 0L), genPath, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("new.zip", newZipBytes, 0L), genPath, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("mixed.zip", mixedZipBytes, 0L), genPath, WritePriority.HIGH);
            assertThat("should have 3 ZIPs before retention", zipBlobCount(blobStore.blobContainer(genPath).listBlobs()), equalTo(3L));

            // Retention bounds: shard 0 → minGen=1, maxGen=3 (gen 2 is past retention)
            // shard 1 has no retention bounds → its entries are effectively always retained
            Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new java.util.HashMap<>();
            retention.put(indexUuid + "/0", new ArchiveDeletionHelper.RetentionBounds(1L, 3L));

            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetentionNewPath(transferService, dataBase, retention);

            Map<String, BlobMetadata> after = blobStore.blobContainer(genPath).listBlobs();
            long remainingZips = zipBlobCount(after);

            // Old ZIP (gen 2 only, shard 0 past retention) → deleted
            // New ZIP (gen 10, shard 0 not past retention for bounds [1,3]) → kept
            // Mixed ZIP has shard 1 data (no retention bounds for shard 1) → kept
            assertTrue("At least old.zip should be deleted", deleted >= 1);
            assertTrue("Some ZIPs should remain", remainingZips >= 1);
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * When a coordinator index is registered, runBatch skips upload (only retention runs).
     * This prevents double uploads when the school-bus coordinator handles uploads inline.
     */
    public void testRunBatchSkipsUploadWhenCoordinatorRegistered() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT.toString())
                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        BlobPath archiveBase = new BlobPath().add("repo-root");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getArchiveBasePath()).thenReturn(archiveBase);

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.isRemoteTranslogEnabled()).thenReturn(true);
        when(shard.isSyncNeeded()).thenReturn(true);
        when(shard.supportsArchiveSnapshot()).thenReturn(true);
        when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
        when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
        when(shard.indexSettings()).thenReturn(indexSettings);

        IndexService indexService = mock(IndexService.class);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any())).thenReturn(indexService);
        when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
        when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        ThreadPool threadPool = mock(ThreadPool.class);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        assertThat(collector.getEligibleShardIds(), hasSize(1));

        // Register coordinator for this index — collector should skip upload
        collector.registerCoordinatorIndex(metadata.getIndex().getUUID());
        collector.runBatchForTesting();

        // Verify NO upload happened (neither ZIP nor metadata nor individual files)
        verify(transferService, org.mockito.Mockito.never()).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            any(),
            any(),
            any(WritePriority.class),
            any()
        );
        verify(transferManager, org.mockito.Mockito.never()).uploadMetadata(any());
        verify(transferManager, org.mockito.Mockito.never()).transferSnapshot(any(), any());

        // buildSnapshotForArchive should NOT be called
        verify(shard, org.mockito.Mockito.never()).buildSnapshotForArchive(any());
    }
}
