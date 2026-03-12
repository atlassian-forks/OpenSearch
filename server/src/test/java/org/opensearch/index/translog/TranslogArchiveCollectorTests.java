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
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot("batch1.zip", zipBytes, 0L),
                archivePath,
                WritePriority.HIGH,
                null
            );

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
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("old.zip", zipBytes, 0L), genPath, WritePriority.HIGH, null);
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
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("keep.zip", zipBytes, 0L), genPath, WritePriority.HIGH, null);
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
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId = new ShardId(metadata.getIndex(), 0);

        TransferService transferService = mock(TransferService.class);
        BlobPath basePath = new BlobPath().add("base");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getRemoteDataTransferPath()).thenReturn(basePath);

        TransferSnapshot mockSnapshot = mock(TransferSnapshot.class);
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(1L);
        when(mockSnapshot.getTranslogTransferMetadata()).thenReturn(mockMeta);
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
        assertThat(pathCaptor.getValue().buildAsString(), org.hamcrest.Matchers.containsString("translog"));
        assertThat(pathCaptor.getValue().buildAsString(), org.hamcrest.Matchers.containsString("data"));

        verify(transferManager).uploadMetadataForArchiveSnapshot(mockSnapshot);
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
                    .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        ShardId shardId0 = new ShardId(metadata.getIndex(), 0);
        ShardId shardId1 = new ShardId(metadata.getIndex(), 1);

        TransferService transferService = mock(TransferService.class);
        BlobPath basePath = new BlobPath().add("base");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getRemoteDataTransferPath()).thenReturn(basePath);

        TransferSnapshot mockSnapshot0 = mock(TransferSnapshot.class);
        TransferSnapshot mockSnapshot1 = mock(TransferSnapshot.class);
        org.opensearch.index.translog.transfer.TranslogTransferMetadata mockMeta = mock(
            org.opensearch.index.translog.transfer.TranslogTransferMetadata.class
        );
        when(mockMeta.getPrimaryTerm()).thenReturn(1L);
        when(mockSnapshot0.getTranslogTransferMetadata()).thenReturn(mockMeta);
        when(mockSnapshot0.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(mockSnapshot0.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot1.getTranslogTransferMetadata()).thenReturn(mockMeta);
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
        verify(transferManager, org.mockito.Mockito.times(2)).uploadMetadataForArchiveSnapshot(snapshotCaptor.capture());
        assertThat(snapshotCaptor.getAllValues(), hasSize(2));
        assertThat(snapshotCaptor.getAllValues().get(0), equalTo(mockSnapshot0));
        assertThat(snapshotCaptor.getAllValues().get(1), equalTo(mockSnapshot1));
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

        BlobPath basePath = new BlobPath().add("base");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getRemoteDataTransferPath()).thenReturn(basePath);

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

        verify(transferManager).transferSnapshot(eq(mockSnapshot), any(), eq(null));
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

        BlobPath basePath = new BlobPath().add("base");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getRemoteDataTransferPath()).thenReturn(basePath);

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

        verify(transferManager, never()).transferSnapshot(any(), any(), any());
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

        BlobPath basePath = new BlobPath().add("base");
        TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
        when(transferManager.getTransferService()).thenReturn(transferService);
        when(transferManager.getRemoteDataTransferPath()).thenReturn(basePath);

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
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("old.zip", zipBytes, 0L), genPath, WritePriority.HIGH, null);
            assertThat(zipBlobCount(blobStore.blobContainer(genPath).listBlobs()), equalTo(1L));

            TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
            when(transferManager.getTransferService()).thenReturn(transferService);
            when(transferManager.getRemoteDataTransferPath()).thenReturn(baseTranslogPath);

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
}
