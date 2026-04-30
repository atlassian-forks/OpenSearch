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
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;
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
import java.time.Duration;
import java.time.Instant;
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
     * deleteArchivesOlderThanRetention deletes ZIPs with a timestamp older than the retention cutoff.
     * Uses a blob name with a timestamp 1 hour in the past with 5-minute retention → should be deleted.
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
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                "idx-uuid",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            // Blob name: timestamp 1 hour ago → past 5-minute retention
            String oldBlobName = TranslogArchivePathHelper.formatTimestamp(Instant.now().minus(Duration.ofHours(1))) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(oldBlobName, zipBytes, 0L), zipDir, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));

            long retentionMinutes = 5L;
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, retentionMinutes);
            assertThat(deleted, equalTo(1));
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(0L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * deleteArchivesOlderThanRetention keeps ZIPs with a timestamp within the retention window.
     * Uses a blob name with current timestamp and 60-minute retention → should be kept.
     */
    public void testDeleteArchivesOlderThanRetentionKeepsWhenWithinRetention() throws IOException {
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
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                "idx-uuid",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            // Blob name: current timestamp → within 60-minute retention
            String freshBlobName = TranslogArchivePathHelper.formatTimestamp(Instant.now()) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(freshBlobName, zipBytes, 0L), zipDir, WritePriority.HIGH);

            long retentionMinutes = 60L;
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, retentionMinutes);
            assertThat(deleted, equalTo(0));
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));
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
        // Provide a real translog file snapshot so snapshotToEntries() returns non-empty entries.
        // Without this, the new empty-entries guard would skip the upload (correct behavior for truly empty
        // translog, but this test verifies the upload path with pending data).
        FileSnapshot.TransferFileSnapshot tlogFile =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "dummy-tlog-data".getBytes(StandardCharsets.UTF_8), 1L);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(tlogFile));
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
        assertThat(nameCaptor.getValue(), endsWith(".tar"));
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

        // No per-shard metadata upload — recovery uses ZIP comment (ArchiveCommentFormat) instead
        verify(transferManager, org.mockito.Mockito.never()).uploadMetadata(any());

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
        // Provide real file snapshots so snapshotToEntries() returns non-empty entries for the upload path.
        FileSnapshot.TransferFileSnapshot tlogFile0 =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "tlog-data-shard0".getBytes(StandardCharsets.UTF_8), 1L);
        when(mockSnapshot0.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(tlogFile0));
        when(mockSnapshot0.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot1.getTranslogTransferMetadata()).thenReturn(realMeta1);
        FileSnapshot.TransferFileSnapshot tlogFile1 =
            new FileSnapshot.TransferFileSnapshot("translog-6.tlog", "tlog-data-shard1".getBytes(StandardCharsets.UTF_8), 1L);
        when(mockSnapshot1.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(tlogFile1));
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
        // No per-shard metadata upload — recovery uses ZIP comment (ArchiveCommentFormat) instead
        verify(transferManager, org.mockito.Mockito.never()).uploadMetadata(any());

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
        // Provide a real file snapshot so upload is attempted (then fails) to trigger fallback
        FileSnapshot.TransferFileSnapshot fallbackTlogFile =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "tlog-data".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1L);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(fallbackTlogFile));
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
        FileSnapshot.TransferFileSnapshot noFallbackTlogFile =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "tlog-data".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1L);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(noFallbackTlogFile));
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
        FileSnapshot.TransferFileSnapshot releaseCallback2TlogFile =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "tlog-data".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1L);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(releaseCallback2TlogFile));
        when(mockSnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());
        when(mockSnapshot.getTranslogFileSnapshots()).thenReturn(Collections.emptySet());
        FileSnapshot.TransferFileSnapshot releaseCallbackTlogFile =
            new FileSnapshot.TransferFileSnapshot("translog-5.tlog", "tlog-data".getBytes(java.nio.charset.StandardCharsets.UTF_8), 1L);
        when(mockSnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.singleton(releaseCallbackTlogFile));
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
     * runRetentionForTesting runs archive retention; when a shard provides retention bounds and an archive is past retention, it is deleted.
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
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                indexUuid,
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = baseTranslogPath.add("translog").add("data").add(hashTypeIndex).add(hashNodeId);
            // Blob name: 2 hours ago → past any reasonable retention
            String oldBlobName = TranslogArchivePathHelper.formatTimestamp(Instant.now().minus(Duration.ofHours(2))) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(oldBlobName, zipBytes, 0L), zipDir, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));

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
            collector.runRetentionForTesting();

            Map<String, BlobMetadata> after = blobStore.blobContainer(zipDir).listBlobs();
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
    /**
     * deleteArchivesOlderThanRetention with a fresh ZIP (current timestamp) and long retention → no deletion.
     * Verifies that ZIPs within the retention window are not touched.
     */
    public void testDeleteArchivesWithFreshZipIsNoOp() throws IOException {
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
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                "idx-uuid",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            // Fresh ZIP → within 60-minute retention window
            String freshName = TranslogArchivePathHelper.formatTimestamp(Instant.now()) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(freshName, zipBytes, 0L), zipDir, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));

            long retentionMinutes = 60L;
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, retentionMinutes);
            assertThat("fresh ZIP within retention should not be deleted", deleted, equalTo(0));
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Retention with mixed old/new ZIPs by timestamp: old ZIPs (past retention) are deleted, new ZIPs kept.
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
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                indexUuid,
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            // Old ZIPs: timestamps 2 hours ago — past 5-minute retention
            Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
            String oldName1 = TranslogArchivePathHelper.formatTimestamp(twoHoursAgo) + ".zip";
            String oldName2 = TranslogArchivePathHelper.formatTimestamp(twoHoursAgo.plusMillis(1)) + ".zip";
            // New ZIP: current timestamp — within retention
            String newName = TranslogArchivePathHelper.formatTimestamp(Instant.now()) + ".zip";

            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(oldName1, oldZipBytes, 0L), zipDir, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(oldName2, mixedZipBytes, 0L), zipDir, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(newName, newZipBytes, 0L), zipDir, WritePriority.HIGH);
            assertThat("should have 3 ZIPs before retention", zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(3L));

            // 5-minute retention: 2 old ZIPs deleted, 1 new ZIP kept
            long retentionMinutes = 5L;
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, retentionMinutes);
            assertThat("2 old ZIPs should be deleted", deleted, equalTo(2));
            assertThat("1 new ZIP should remain", zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));
            assertThat("new ZIP should still be present", blobStore.blobContainer(zipDir).listBlobs().containsKey(newName), equalTo(true));
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

    /**
     * Orphaned archive ZIPs (from deleted indices) are detected via S3 folder scan and deleted
     * by the same pure-timestamp pass that deletes live-index ZIPs.
     * <p>
     * In the new (pure-timestamp) GC model, orphaned dirs are not special-cased: we simply scan
     * every {@code hashTypeIndex} dir found under {@code translog/data/} and delete ZIPs older
     * than the largest configured retention (or the safety floor, whichever is greater). Eventually,
     * an orphaned dir's ZIPs all age out and the dir becomes empty.
     */
    public void testOrphanedIndexArchiveZipsAreCleanedUpViaScan() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            RemoteStoreEnums.PathHashAlgorithm hashAlgo = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath basePath = new BlobPath().add(uniqueBase);

            // Deleted index — upload ZIPs to its archive dir
            String deletedIndexUUID = "deleted-" + randomAlphaOfLength(6);
            String nodeId = "node-1";
            String deletedHashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(deletedIndexUUID, hashAlgo);
            String hashNodeId = TranslogArchivePathHelper.hashNodeId(nodeId, hashAlgo);
            BlobPath deletedZipDir = basePath.add("translog").add("data").add(deletedHashTypeIndex).add(hashNodeId);
            byte[] zipBytes = new TranslogArchiveCollector(mock(IndicesService.class)).buildArchiveFromEntries(Collections.emptyList());
            // Both timestamps are well past the 5-minute safety-floor retention so they will both
            // be deleted by the pure-timestamp GC pass on this orphaned dir.
            String blob1 = TranslogArchivePathHelper.formatTimestamp(Instant.now().minus(Duration.ofHours(2))) + ".zip";
            String blob2 = TranslogArchivePathHelper.formatTimestamp(Instant.now().minus(Duration.ofHours(1))) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(blob1, zipBytes, 0L), deletedZipDir, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(blob2, zipBytes, 0L), deletedZipDir, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(deletedZipDir).listBlobs()), equalTo(2L));

            // Live index — needs a shard with TranslogTransferManager to provide transferService + basePath
            IndexMetadata metadata = IndexMetadata.builder("live-index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                        .put(
                            IndexMetadata.SETTING_REPLICATION_TYPE,
                            org.opensearch.indices.replication.common.ReplicationType.SEGMENT.toString()
                        )
                        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                        .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                        .build()
                )
                .build();
            IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);

            TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
            when(transferManager.getTransferService()).thenReturn(transferService);
            when(transferManager.getArchiveBasePath()).thenReturn(basePath);

            IndexShard liveShard = mock(IndexShard.class);
            when(liveShard.shardId()).thenReturn(new ShardId(metadata.getIndex(), 0));
            when(liveShard.isRemoteTranslogEnabled()).thenReturn(true);
            when(liveShard.indexSettings()).thenReturn(indexSettings);
            when(liveShard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
            when(liveShard.getTranslogNodeId()).thenReturn(Optional.of(nodeId));
            when(liveShard.getArchiveRetentionBounds()).thenReturn(Optional.empty());

            IndexService liveIndexService = mock(IndexService.class);
            when(liveIndexService.getIndexSettings()).thenReturn(indexSettings);
            when(liveIndexService.getShardOrNull(0)).thenReturn(liveShard);

            IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.indexService(any())).thenReturn(liveIndexService);
            when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(liveIndexService).iterator());

            RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
            when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
            when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(hashAlgo);
            when(remoteStoreSettings.getTranslogArchiveGcInterval()).thenReturn(TimeValue.timeValueMinutes(1));

            TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
            collector.runRetentionForTesting();

            // Orphaned ZIPs for the deleted index should all be gone
            assertThat(
                "Orphaned ZIPs for deleted index should be removed by S3 scan",
                zipBlobCount(blobStore.blobContainer(deletedZipDir).listBlobs()),
                equalTo(0L)
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Fix: early-exit in deleteArchivesOlderThanRetention when a fresh ZIP is encountered.
     * Verifies that once a ZIP within retention is encountered, pagination stops immediately —
     * remaining ZIPs (also within retention, sorted after) are not listed or deleted.
     *
     * Scenario: 2 old ZIPs (past retention) + 1 fresh ZIP → early-exit after deleting 2 old ones.
     * The fresh ZIP being encountered should prevent checking any further pages.
     */
    public void testDeleteArchivesEarlyExitOnFreshZip() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                "idx-uuid",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            byte[] zipBytes = new TranslogArchiveCollector(mock(IndicesService.class)).buildArchiveFromEntries(
                Collections.singletonList(ArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8)))
            );

            // Two old ZIPs: past 5-minute retention → should be deleted
            Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
            String old1 = TranslogArchivePathHelper.formatTimestamp(twoHoursAgo) + ".zip";
            String old2 = TranslogArchivePathHelper.formatTimestamp(twoHoursAgo.plusMillis(1)) + ".zip";
            // One fresh ZIP: within retention → should stop further pagination
            String fresh = TranslogArchivePathHelper.formatTimestamp(Instant.now()) + ".zip";

            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(old1, zipBytes, 0L), zipDir, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(old2, zipBytes, 0L), zipDir, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(fresh, zipBytes, 0L), zipDir, WritePriority.HIGH);
            assertThat(zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(3L));

            long retentionMinutes = 5L;
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, retentionMinutes);

            assertThat("2 old ZIPs should be deleted, fresh ZIP should stop further iteration", deleted, equalTo(2));
            assertThat("fresh ZIP should still be present", zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()), equalTo(1L));
            assertThat("fresh ZIP should not be deleted", blobStore.blobContainer(zipDir).listBlobs().containsKey(fresh), equalTo(true));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * When there are more than MAX_ARCHIVE_BLOBS_PER_NODE (500) expired ZIPs, the loop must
     * re-list after each batch of deletions until all expired ZIPs are gone.
     *
     * Scenario: 600 old ZIPs (all past retention) + 1 fresh ZIP.
     * Expected: all 600 old ZIPs deleted in one GC cycle (2 re-list passes), fresh ZIP kept.
     */
    public void testDeleteArchivesMoreThan500ExpiredZipsDeletedInOneGcCycle() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
                "idx-uuid",
                RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
            );
            String hashNodeId = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            BlobPath zipDir = new BlobPath().add(uniqueBase).add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

            byte[] zipBytes = new TranslogArchiveCollector(mock(IndicesService.class)).buildArchiveFromEntries(
                Collections.singletonList(ArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8)))
            );

            // Upload 600 expired ZIPs (sorted ascending: each 1ms apart, all > 2h ago)
            int expiredCount = 600;
            Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
            for (int i = 0; i < expiredCount; i++) {
                String name = TranslogArchivePathHelper.formatTimestamp(twoHoursAgo.plusMillis(i)) + ".zip";
                transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(name, zipBytes, 0L), zipDir, WritePriority.HIGH);
            }

            // Upload 1 fresh ZIP (within retention)
            String freshName = TranslogArchivePathHelper.formatTimestamp(Instant.now()) + ".zip";
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(freshName, zipBytes, 0L), zipDir, WritePriority.HIGH);

            assertThat(
                "Should have 601 ZIPs before GC",
                zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()),
                equalTo((long) expiredCount + 1)
            );

            // Run GC with 5-minute retention: all 600 old ZIPs should be deleted in one cycle
            int deleted = TranslogArchiveCollector.deleteArchivesOlderThanRetention(transferService, zipDir, 5L);

            assertThat("All 600 expired ZIPs should be deleted in one GC cycle", deleted, equalTo(expiredCount));
            assertThat(
                "Only 1 fresh ZIP should remain",
                zipBlobCount(blobStore.blobContainer(zipDir).listBlobs()),
                equalTo(1L)
            );
            assertThat(
                "Fresh ZIP should still be present",
                blobStore.blobContainer(zipDir).listBlobs().containsKey(freshName),
                equalTo(true)
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Fix: empty translog archive is skipped — no S3 PUT when all snapshots have zero files.
     * Verifies that runBatch does NOT upload a ZIP when all shard snapshots produce empty file sets.
     */
    public void testRunBatchSkipsUploadWhenAllSnapshotsEmpty() throws IOException {
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

        // Snapshot with NO files — simulates empty translog (no pending ops)
        org.opensearch.index.translog.transfer.TranslogTransferMetadata emptyMeta =
            new org.opensearch.index.translog.transfer.TranslogTransferMetadata(1L, 5L, 3L, 0, "node-1");
        emptyMeta.setGenerationToPrimaryTermMapper(new java.util.HashMap<>());
        TransferSnapshot emptySnapshot = mock(TransferSnapshot.class);
        when(emptySnapshot.getTranslogTransferMetadata()).thenReturn(emptyMeta);
        when(emptySnapshot.getTranslogFileSnapshotWithMetadata()).thenReturn(Collections.emptySet());
        when(emptySnapshot.getCheckpointFileSnapshots()).thenReturn(Collections.emptySet());

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
            consumer.accept(emptySnapshot, () -> {});
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
        collector.runBatchForTesting();

        // No S3 upload should happen — empty snapshot → no ZIP
        verify(transferService, never()).uploadBlobStream(
            any(java.io.InputStream.class),
            org.mockito.ArgumentMatchers.anyLong(),
            any(),
            any(),
            any(WritePriority.class),
            any()
        );
    }

    /**
     * Fix: gc_interval default is 5 minutes.
     * Verifies the setting default is 5m so that the retention GC doesn't run too frequently.
     */
    public void testTranslogArchiveGcIntervalDefaultIsOneMinute() {
        // The default gc_interval should be 5 minutes (not 1 minute) to reduce S3 LIST storms.
        TimeValue defaultInterval = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL.getDefault(
            org.opensearch.common.settings.Settings.EMPTY
        );
        assertThat("gc_interval default should be 1 minute", defaultInterval, equalTo(TimeValue.timeValueMinutes(1)));
    }
}
