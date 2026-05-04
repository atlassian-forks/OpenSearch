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

import org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.TranslogArchiveGcScanner;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TranslogArchiveCollectorTests extends OpenSearchTestCase {

    private static final String UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String UUID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    /**
     * Integration test: TAR archive with GC entries where maxGen > globalCheckpoint should NOT be deleted.
     * The checkpoint gate must block deletion even when the blob is past the retention timestamp.
     */
    public void testCheckpointGateBlocksDeletionWhenMaxGenExceedsCheckpoint() throws IOException {
        // Build a TAR with GC entries: shard 0, gen 5..10 (maxGen=10)
        String indexUuid = "test-uuid";
        int shardIdInt = 0;
        String pathPrefix = indexUuid + "/" + shardIdInt + "/1/";
        byte[] tlogContent = "tlog".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "ckp".getBytes(StandardCharsets.UTF_8);

        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
            new TarArchiveBuilder.GcShardEntry(indexUuid, shardIdInt, 5L, 10L, 8L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            TarArchiveBuilder.fromBytes(pathPrefix + "translog-5.tlog", tlogContent),
            TarArchiveBuilder.fromBytes(pathPrefix + "translog-5.ckp", ckpContent)
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcEntries);
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        TarArchiveBuilder.build(baos, layout, entries);
        byte[] tarBytes = baos.toByteArray();

        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath basePath = new BlobPath().add(uniqueBase);

            // Upload TAR at a timestamp 2 hours ago (past retention)
            Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
            BlobPath tarDir = TranslogArchivePathHelper.tarBlobDir(basePath, twoHoursAgo);
            String blobName = TranslogArchivePathHelper.tarBlobName(twoHoursAgo, "node-1");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(blobName, tarBytes, 0L), tarDir, WritePriority.HIGH);
            assertThat(archiveBlobCount(blobStore.blobContainer(tarDir).listBlobs()), equalTo(1L));

            // Set up shard mock: globalCheckpoint = 8 → maxGen(10) > 8 → NOT safe
            IndexMetadata metadata = IndexMetadata.builder("test-index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo")
                        .put(IndexMetadata.SETTING_REPLICATION_TYPE,
                            org.opensearch.indices.replication.common.ReplicationType.SEGMENT.toString())
                        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                        .put(IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.getKey(), true)
                        .build()
                )
                .build();
            IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
            ShardId shardId = new ShardId(metadata.getIndex(), shardIdInt);

            TranslogTransferManager transferManager = mock(TranslogTransferManager.class);
            when(transferManager.getTransferService()).thenReturn(transferService);
            when(transferManager.getArchiveBasePath()).thenReturn(basePath);

            IndexShard shard = mock(IndexShard.class);
            when(shard.shardId()).thenReturn(shardId);
            when(shard.isRemoteTranslogEnabled()).thenReturn(true);
            when(shard.isSyncNeeded()).thenReturn(false);
            when(shard.getTranslogTransferManager()).thenReturn(Optional.of(transferManager));
            when(shard.getTranslogNodeId()).thenReturn(Optional.of("node-1"));
            when(shard.indexSettings()).thenReturn(indexSettings);
            when(shard.getArchiveRetentionBounds()).thenReturn(Optional.empty());
            // globalCheckpoint = 8 → maxGen(10) > 8 → must NOT delete
            when(shard.getLastSyncedGlobalCheckpoint()).thenReturn(8L);

            IndexService indexService = mock(IndexService.class);
            when(indexService.getIndexSettings()).thenReturn(indexSettings);
            when(indexService.getShardOrNull(shardIdInt)).thenReturn(shard);

            IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.indexService(any())).thenReturn(indexService);
            when(indicesService.iterator()).thenAnswer(inv -> Collections.singleton(indexService).iterator());

            RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
            when(remoteStoreSettings.getClusterRemoteTranslogBufferInterval()).thenReturn(TimeValue.timeValueMinutes(1));
            when(remoteStoreSettings.getPathHashAlgorithm()).thenReturn(RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
            when(remoteStoreSettings.getTranslogArchiveRetention()).thenReturn(TimeValue.timeValueMinutes(5));

            TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
            collector.runRetentionForTesting();

            // TAR must still exist — checkpoint gate blocked deletion
            Map<String, BlobMetadata> after = blobStore.blobContainer(tarDir).listBlobs();
            assertThat(
                "TAR with maxGen(10) > globalCheckpoint(8) must NOT be deleted",
                archiveBlobCount(after),
                equalTo(1L)
            );

            // Now upload a newer TAR for the same shard with globalCheckpoint=10 (covers maxGen=10).
            // The scanner will pick up checkpoint=10 from this TAR → rolling map advances → safe to delete.
            Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
            BlobPath newerTarDir = TranslogArchivePathHelper.tarBlobDir(basePath, oneHourAgo);
            String newerBlobName = TranslogArchivePathHelper.tarBlobName(oneHourAgo, "node-1");
            List<TarArchiveBuilder.GcShardEntry> newerGc = List.of(
                new TarArchiveBuilder.GcShardEntry(indexUuid, shardIdInt, 11L, 12L, 10L)
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> newerEntries = List.of(
                TarArchiveBuilder.fromBytes(pathPrefix + "translog-11.tlog", "newer".getBytes(StandardCharsets.UTF_8))
            );
            TarArchiveBuilder.TarLayout newerLayout = TarArchiveBuilder.computeLayout(newerEntries, newerGc);
            java.io.ByteArrayOutputStream newerBaos = new java.io.ByteArrayOutputStream();
            TarArchiveBuilder.build(newerBaos, newerLayout, newerEntries);
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot(newerBlobName, newerBaos.toByteArray(), 0L),
                newerTarDir, WritePriority.HIGH
            );

            // Second retention run: scanner sees newer TAR with checkpoint=10 → rolling map = 10
            // maxGen(10) ≤ rollingCheckpoint(10) → safe → old TAR deleted
            collector.runRetentionForTesting();

            Map<String, BlobMetadata> afterAdvanced = blobStore.blobContainer(tarDir).listBlobs();
            assertThat(
                "TAR with maxGen(10) ≤ rollingCheckpoint(10) must be deleted",
                archiveBlobCount(afterAdvanced),
                equalTo(0L)
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * When a minute-dir is not fully safe, per-TAR GC should delete only safe TARs and keep stuck ones.
     */
    public void testDeleteStuckMinutePartiallyDeletesOnlySafeTars() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base");
            Instant minuteTime = Instant.parse("2026-05-01T10:00:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String minuteKey = TranslogArchivePathHelper.dayDir(minuteTime) + "/" + TranslogArchivePathHelper.minuteDir(minuteTime);

            // Safe TAR: checkpoint already covers maxSeqNo (phase 1).
            List<TarArchiveBuilder.GcShardEntry> safeGc = List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 10L));
            List<TarArchiveBuilder.ArchiveBuildEntry> safeEntries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "safe".getBytes(StandardCharsets.UTF_8))
            );
            ByteArrayOutputStream safeOut = new ByteArrayOutputStream();
            TarArchiveBuilder.build(safeOut, TarArchiveBuilder.computeLayout(safeEntries, safeGc), safeEntries);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("00.100.nodeA.tar", safeOut.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            // Stuck TAR: checkpoint below maxSeqNo, no newer minute to advance rolling checkpoint.
            List<TarArchiveBuilder.GcShardEntry> stuckGc = List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 11L, 20L, 12L));
            List<TarArchiveBuilder.ArchiveBuildEntry> stuckEntries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/1/1/translog-2.tlog", "stuck".getBytes(StandardCharsets.UTF_8))
            );
            ByteArrayOutputStream stuckOut = new ByteArrayOutputStream();
            TarArchiveBuilder.build(stuckOut, TarArchiveBuilder.computeLayout(stuckEntries, stuckGc), stuckEntries);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("00.200.nodeB.tar", stuckOut.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals(1, deleted);

            Map<String, BlobMetadata> remaining = blobStore.blobContainer(minutePath).listBlobs();
            assertFalse("safe TAR must be deleted", remaining.containsKey("00.100.nodeA.tar"));
            assertTrue("stuck TAR must remain", remaining.containsKey("00.200.nodeB.tar"));
            assertTrue("minute-key must remain indexed while stuck TAR exists", scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * If all TARs in a stuck minute become safe, per-TAR GC should delete all and evict the minute key.
     */
    public void testDeleteStuckMinutePartiallyEvictsWhenAllTarsDeleted() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base");
            Instant minuteTime = Instant.parse("2026-05-01T10:01:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String minuteKey = TranslogArchivePathHelper.dayDir(minuteTime) + "/" + TranslogArchivePathHelper.minuteDir(minuteTime);

            List<TarArchiveBuilder.ArchiveBuildEntry> e1 = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "a".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> e2 = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/1/1/translog-1.tlog", "b".getBytes(StandardCharsets.UTF_8))
            );

            ByteArrayOutputStream out1 = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                out1,
                TarArchiveBuilder.computeLayout(e1, List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L))),
                e1
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("01.100.nodeA.tar", out1.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            ByteArrayOutputStream out2 = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                out2,
                TarArchiveBuilder.computeLayout(e2, List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 1L, 7L, 8L))),
                e2
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("01.200.nodeB.tar", out2.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals(2, deleted);
            assertEquals(0L, archiveBlobCount(blobStore.blobContainer(minutePath).listBlobs()));
            assertFalse("minute-key must be evicted once all TARs are gone", scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * If per-TAR delete fails, the minute must remain indexed and blobs must remain untouched.
     */
    public void testDeleteStuckMinutePartiallyKeepsMinuteWhenDeleteFails() throws Exception {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            BlobStoreTransferService realTransferService = new BlobStoreTransferService(blobStore, threadPool);
            TransferService transferService = org.mockito.Mockito.spy(realTransferService);

            BlobPath archiveBasePath = new BlobPath().add("base");
            Instant minuteTime = Instant.parse("2026-05-01T10:02:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String minuteKey = TranslogArchivePathHelper.dayDir(minuteTime) + "/" + TranslogArchivePathHelper.minuteDir(minuteTime);

            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TarArchiveBuilder.build(out, TarArchiveBuilder.computeLayout(entries, gcEntries), entries);
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot("02.100.nodeA.tar", out.toByteArray(), 0L),
                minutePath,
                WritePriority.HIGH
            );

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            doThrow(new IOException("simulated delete failure")).when(transferService).deleteBlobs(eq(minutePath), any(List.class));

            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals(0, deleted);
            assertEquals(1L, archiveBlobCount(blobStore.blobContainer(minutePath).listBlobs()));
            assertTrue("minute-key must remain indexed when delete fails", scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Multi-index minute: same shard id across indices must not collide; only safe index TAR is deleted.
     */
    public void testDeleteStuckMinutePartiallyWithMultipleIndicesSameShardId() throws Exception {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base");
            Instant minuteTime = Instant.parse("2026-05-01T10:03:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String minuteKey = TranslogArchivePathHelper.dayDir(minuteTime) + "/" + TranslogArchivePathHelper.minuteDir(minuteTime);

            List<TarArchiveBuilder.ArchiveBuildEntry> entriesA = List.of(
                TarArchiveBuilder.fromBytes("idx-a/0/1/translog-1.tlog", "a".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entriesB = List.of(
                TarArchiveBuilder.fromBytes("idx-b/0/1/translog-2.tlog", "b".getBytes(StandardCharsets.UTF_8))
            );

            // Index A (shard 0): safe in phase-1.
            ByteArrayOutputStream outA = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                outA,
                TarArchiveBuilder.computeLayout(entriesA, List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L))),
                entriesA
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("03.100.nodeA.tar", outA.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            // Index B (same shard 0): stuck (checkpoint below maxSeqNo).
            ByteArrayOutputStream outB = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                outB,
                TarArchiveBuilder.computeLayout(entriesB, List.of(new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 6L, 10L, 3L))),
                entriesB
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("03.200.nodeB.tar", outB.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals(1, deleted);

            Map<String, BlobMetadata> remaining = blobStore.blobContainer(minutePath).listBlobs();
            assertFalse("safe TAR for index A should be removed", remaining.containsKey("03.100.nodeA.tar"));
            assertTrue("stuck TAR for index B should remain", remaining.containsKey("03.200.nodeB.tar"));
            assertTrue("minute should remain indexed while one TAR is stuck", scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Deleted-index TARs should be removed in per-TAR GC when liveIndexUUIDs excludes that index.
     */
    public void testDeleteStuckMinutePartiallyWithLiveIndexFilterDeletesDeletedIndexTar() throws Exception {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base");
            Instant minuteTime = Instant.parse("2026-05-01T10:06:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String minuteKey = TranslogArchivePathHelper.dayDir(minuteTime) + "/" + TranslogArchivePathHelper.minuteDir(minuteTime);

            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("idx-a/0/1/translog-1.tlog", "a".getBytes(StandardCharsets.UTF_8))
            );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // Intentionally unsafe by checkpoint so only the live-index filter can make it deletable.
            TarArchiveBuilder.build(
                out,
                TarArchiveBuilder.computeLayout(entries, List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 0L))),
                entries
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("06.100.nodeA.tar", out.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(
                transferService,
                minutePath,
                minuteKey,
                scanner,
                Collections.singleton(UUID_B)
            );

            assertEquals(1, deleted);
            assertEquals(0L, archiveBlobCount(blobStore.blobContainer(minutePath).listBlobs()));
            assertFalse("minute key should be evicted after all TARs are deleted", scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Multi-index minute becomes fully safe after a newer TAR advances checkpoint for the stuck index.
     */
    public void testDeleteHierarchicalArchivesMultipleIndicesBecomesSafeAfterNewerMinute() throws Exception {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base");

            Instant minute1 = Instant.parse("2026-05-01T10:04:30Z");
            Instant minute2 = Instant.parse("2026-05-01T10:05:30Z");
            BlobPath minute1Path = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minute1);
            BlobPath minute2Path = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minute2);
            String minute1Key = TranslogArchivePathHelper.dayDir(minute1) + "/" + TranslogArchivePathHelper.minuteDir(minute1);

            List<TarArchiveBuilder.ArchiveBuildEntry> entriesA = List.of(
                TarArchiveBuilder.fromBytes("idx-a/0/1/translog-1.tlog", "a".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entriesB1 = List.of(
                TarArchiveBuilder.fromBytes("idx-b/0/1/translog-2.tlog", "b1".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entriesB2 = List.of(
                TarArchiveBuilder.fromBytes("idx-b/0/1/translog-3.tlog", "b2".getBytes(StandardCharsets.UTF_8))
            );

            // Minute1: index A is safe, index B stuck.
            ByteArrayOutputStream outA = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                outA,
                TarArchiveBuilder.computeLayout(entriesA, List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L))),
                entriesA
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("04.100.nodeA.tar", outA.toByteArray(), 0L), minute1Path, WritePriority.HIGH);

            ByteArrayOutputStream outB1 = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                outB1,
                TarArchiveBuilder.computeLayout(entriesB1, List.of(new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 6L, 10L, 3L))),
                entriesB1
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("04.200.nodeB.tar", outB1.toByteArray(), 0L), minute1Path, WritePriority.HIGH);

            // Minute2: newer TAR for index B advances checkpoint to cover minute1 maxSeqNo.
            ByteArrayOutputStream outB2 = new ByteArrayOutputStream();
            TarArchiveBuilder.build(
                outB2,
                TarArchiveBuilder.computeLayout(entriesB2, List.of(new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 11L, 12L, 12L))),
                entriesB2
            );
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("05.100.nodeB.tar", outB2.toByteArray(), 0L), minute2Path, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minute2); // scan both minutes

            assertThat("rolling checkpoints must track each index UUID independently", scanner.getRollingCheckpoints().keySet(), hasItems(UUID_A, UUID_B));

            // Cutoff only targets minute1; minute2 should be skipped as newer minute-dir.
            Instant cutoff = Instant.parse("2026-05-01T10:04:59Z");
            int deleted = TranslogArchiveCollector.deleteHierarchicalArchivesOlderThan(
                transferService,
                TranslogArchivePathHelper.txlogRootPath(archiveBasePath),
                cutoff,
                scanner
            );

            assertEquals("both minute1 TARs should be deleted once index B is covered", 2, deleted);
            assertEquals(0L, archiveBlobCount(blobStore.blobContainer(minute1Path).listBlobs()));
            assertEquals(1L, archiveBlobCount(blobStore.blobContainer(minute2Path).listBlobs()));
            assertFalse("minute1 key should be evicted after full cleanup", scanner.getInMemoryIndex().containsKey(minute1Key));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Boundary-minute cleanup should only delete parseable TARs older than cutoff, keeping fresh and malformed names.
     */
    public void testDeleteBlobsInDirWithCutoffDeletesOnlyOlderParseableTarNames() throws Exception {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath minutePath = new BlobPath().add("base").add("txlog").add("20260501").add("1000");

            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("00.001.nodeA.tar", new byte[] { 1 }, 0L), minutePath, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("59.999.nodeA.tar", new byte[] { 2 }, 0L), minutePath, WritePriority.HIGH);
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot("malformed.tar", new byte[] { 3 }, 0L), minutePath, WritePriority.HIGH);

            Instant cutoff = Instant.parse("2026-05-01T10:00:30Z");
            int deleted = TranslogArchiveCollector.deleteBlobsInDir(transferService, minutePath, cutoff);
            assertEquals(1, deleted);

            Map<String, BlobMetadata> remaining = blobStore.blobContainer(minutePath).listBlobs();
            assertThat(remaining.keySet(), hasItems("59.999.nodeA.tar", "malformed.tar"));
            assertFalse(remaining.containsKey("00.001.nodeA.tar"));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Becoming cluster-manager should schedule retention exactly once even if callback is invoked repeatedly.
     */
    public void testOnClusterManagerSchedulesRetentionOnce() {
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        ThreadPool threadPool = mock(ThreadPool.class);
        Scheduler.Cancellable cancellable = mock(Scheduler.Cancellable.class);
        TimeValue gcInterval = TimeValue.timeValueMinutes(1);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), eq(gcInterval), eq(ThreadPool.Names.TRANSLOG_TRANSFER))).thenReturn(
            cancellable
        );

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getTranslogArchiveGcInterval()).thenReturn(gcInterval);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        collector.onClusterManager();
        collector.onClusterManager();

        verify(threadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(gcInterval), eq(ThreadPool.Names.TRANSLOG_TRANSFER));
    }

    /**
     * Losing cluster-manager role should cancel the retention task that was scheduled on promotion.
     */
    public void testOffClusterManagerCancelsRetentionTask() {
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        ThreadPool threadPool = mock(ThreadPool.class);
        Scheduler.Cancellable cancellable = mock(Scheduler.Cancellable.class);
        TimeValue gcInterval = TimeValue.timeValueMinutes(1);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), eq(gcInterval), eq(ThreadPool.Names.TRANSLOG_TRANSFER))).thenReturn(
            cancellable
        );

        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getTranslogArchiveGcInterval()).thenReturn(gcInterval);

        TranslogArchiveCollector collector = new TranslogArchiveCollector(indicesService, threadPool, remoteStoreSettings);
        collector.onClusterManager();
        collector.offClusterManager();

        verify(cancellable, times(1)).cancel();
    }

    public void testTranslogArchiveGcIntervalDefaultIsTwoMinutes() {
        // The default gc_interval is 2 minutes for more aggressive GC (reduced from 10m).
        TimeValue defaultInterval = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL.getDefault(
            org.opensearch.common.settings.Settings.EMPTY
        );
        assertThat("gc_interval default should be 2 minutes", defaultInterval, equalTo(TimeValue.timeValueMinutes(2)));
    }

    /**
     * GC-001: When deleteStuckMinutePartially() finds an EMPTY minute-dir, it must call
     * evictAndDeleteIdx() — not just evict() — so the gc_idx blob is also deleted from S3.
     */
    public void testDeleteStuckMinutePartiallyEmptyDirDeletesGcIdxBlob() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base-" + randomAlphaOfLength(8));
            Instant minuteTime = Instant.parse("2026-05-01T10:10:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String dayDir = TranslogArchivePathHelper.dayDir(minuteTime);
            String minuteDir = TranslogArchivePathHelper.minuteDir(minuteTime);
            String minuteKey = dayDir + "/" + minuteDir;

            // Upload and scan a TAR to create the gc_idx entry, then delete the TAR manually
            // to simulate an already-empty minute-dir (e.g. prior partial GC run)
            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8))
            );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TarArchiveBuilder.build(out, TarArchiveBuilder.computeLayout(entries, List.of()), entries);
            String tarName = TranslogArchivePathHelper.tarBlobName(minuteTime, "node-a");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(tarName, out.toByteArray(), 0L), minutePath, WritePriority.HIGH);

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            // Verify gc_idx blob was created
            BlobPath gcIdxDayPath = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath).add(dayDir);
            assertTrue("gc_idx blob should exist before GC",
                blobStore.blobContainer(gcIdxDayPath).listBlobs().containsKey(minuteDir + ".idx"));

            // Now delete the TAR manually — minute-dir is now empty
            blobStore.blobContainer(minutePath).deleteBlobsIgnoringIfNotExists(Collections.singletonList(tarName));

            // deleteStuckMinutePartially on empty dir → should evictAndDeleteIdx (not just evict)
            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals("no TARs deleted from already-empty dir", 0, deleted);

            // gc_idx blob MUST be deleted (GC-001 fix)
            assertFalse("gc_idx blob should be deleted when minute-dir is empty",
                blobStore.blobContainer(gcIdxDayPath).listBlobs().containsKey(minuteDir + ".idx"));

            // Memory should be evicted
            assertFalse("minute-key should be evicted from memory",
                scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * GC-001: When deleteStuckMinutePartially() deletes ALL TARs in a stuck minute-dir,
     * it must call evictAndDeleteIdx() — not just evict() — so the gc_idx blob is deleted.
     */
    public void testDeleteStuckMinutePartiallyAllTarsDeletedDeletesGcIdxBlob() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base-" + randomAlphaOfLength(8));
            Instant minuteTime = Instant.parse("2026-05-01T10:11:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String dayDir = TranslogArchivePathHelper.dayDir(minuteTime);
            String minuteDir = TranslogArchivePathHelper.minuteDir(minuteTime);
            String minuteKey = dayDir + "/" + minuteDir;

            // Upload a safe TAR (checkpoint already covers maxSeqNo → phase 1 safe)
            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
                new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L) // checkpoint == maxSeqNo → safe
            );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TarArchiveBuilder.build(out, TarArchiveBuilder.computeLayout(entries, gcEntries), entries);
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot("10.100.nodeA.tar", out.toByteArray(), 0L),
                minutePath, WritePriority.HIGH
            );

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            BlobPath gcIdxDayPath = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath).add(dayDir);
            assertTrue("gc_idx blob should exist before GC",
                blobStore.blobContainer(gcIdxDayPath).listBlobs().containsKey(minuteDir + ".idx"));

            // deleteStuckMinutePartially → safe TAR deleted → minute-dir fully empty
            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);
            assertEquals("safe TAR should be deleted", 1, deleted);
            assertEquals("minute-dir should be empty", 0L,
                archiveBlobCount(blobStore.blobContainer(minutePath).listBlobs()));

            // gc_idx blob MUST be deleted (GC-001 fix)
            assertFalse("gc_idx blob should be deleted after all TARs gone",
                blobStore.blobContainer(gcIdxDayPath).listBlobs().containsKey(minuteDir + ".idx"));

            // Memory should be evicted
            assertFalse("minute-key should be evicted from memory",
                scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * GC-002: Non-TAR blobs in a stuck minute-dir must NOT block eviction.
     * They should be deleted (or excluded from remaining count) so GC can complete.
     */
    public void testDeleteStuckMinutePartiallyNonTarBlobDoesNotBlockEviction() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("base-" + randomAlphaOfLength(8));
            Instant minuteTime = Instant.parse("2026-05-01T10:12:30Z");
            BlobPath minutePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, minuteTime);
            String dayDir = TranslogArchivePathHelper.dayDir(minuteTime);
            String minuteDir = TranslogArchivePathHelper.minuteDir(minuteTime);
            String minuteKey = dayDir + "/" + minuteDir;

            // Upload one safe TAR
            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8))
            );
            List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
                new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L) // safe
            );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TarArchiveBuilder.build(out, TarArchiveBuilder.computeLayout(entries, gcEntries), entries);
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot("11.100.nodeA.tar", out.toByteArray(), 0L),
                minutePath, WritePriority.HIGH
            );

            // Also upload a stray non-TAR blob (e.g. partial upload artifact)
            transferService.uploadBlob(
                new FileSnapshot.TransferFileSnapshot("stray.tmp", "junk".getBytes(StandardCharsets.UTF_8), 0L),
                minutePath, WritePriority.HIGH
            );

            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minuteTime);

            // deleteStuckMinutePartially → safe TAR deleted, non-TAR blob handled
            int deleted = TranslogArchiveCollector.deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner);

            // Non-TAR blob should NOT permanently block eviction
            // (either deleted too, or at least not counted as a stuck TAR)
            assertFalse("minute-key must be evicted — non-TAR blob must not block GC",
                scanner.getInMemoryIndex().containsKey(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Verifies that after deleteHierarchicalArchivesOlderThan() deletes a minute-dir's TARs,
     * the corresponding gc_idx blob is also deleted from remote storage (hot-path cleanup).
     */
    public void testDeleteHierarchicalArchivesDeletesGcIdxBlobAfterMinuteDeletion() throws IOException {
        BlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
            String uniqueBase = "base-" + randomAlphaOfLength(12);
            BlobPath basePath = new BlobPath().add(uniqueBase);

            byte[] tarBytes = new TranslogArchiveCollector(mock(IndicesService.class)).buildArchiveFromEntries(
                Collections.singletonList(TarArchiveBuilder.fromBytes("idx-uuid/0/1/translog-1.tlog", "x".getBytes(StandardCharsets.UTF_8)))
            );

            // Old TAR: 2 hours ago → past cutoff → should be GC'd
            Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
            BlobPath oldDir = TranslogArchivePathHelper.tarBlobDir(basePath, twoHoursAgo);
            String oldName = TranslogArchivePathHelper.tarBlobName(twoHoursAgo, "node-x");
            transferService.uploadBlob(new FileSnapshot.TransferFileSnapshot(oldName, tarBytes, 0L), oldDir, WritePriority.HIGH);

            // Run scan() to populate gc_idx for this minute
            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, basePath);
            scanner.scan(twoHoursAgo);

            // Verify gc_idx blob was created
            String dayDir = TranslogArchivePathHelper.dayDir(twoHoursAgo);
            String minuteDir = TranslogArchivePathHelper.minuteDir(twoHoursAgo);
            BlobPath gcIdxDayPath = TranslogArchiveGcScanner.gcIdxRootPath(basePath).add(dayDir);
            Map<String, BlobMetadata> gcIdxBefore = blobStore.blobContainer(gcIdxDayPath).listBlobs();
            assertThat("gc_idx blob should exist before GC", gcIdxBefore.containsKey(minuteDir + ".idx"), equalTo(true));

            // Run hierarchical GC with scanner — old minute should be deleted along with its .idx
            BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(basePath);
            Instant cutoff = Instant.now().minus(Duration.ofMinutes(1));
            int deleted = TranslogArchiveCollector.deleteHierarchicalArchivesOlderThan(transferService, txlogRoot, cutoff, scanner);

            assertThat("old TAR should be deleted", deleted, equalTo(1));

            // gc_idx blob should also be deleted
            Map<String, BlobMetadata> gcIdxAfter = blobStore.blobContainer(gcIdxDayPath).listBlobs();
            assertThat("gc_idx blob should be deleted after minute-dir GC",
                gcIdxAfter.containsKey(minuteDir + ".idx"), equalTo(false));

            // Scanner memory should be evicted
            String minuteKey = dayDir + "/" + minuteDir;
            assertNull("minute-key should be evicted from scanner memory after GC",
                scanner.getInMemoryIndex().get(minuteKey));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Count TAR archive blobs (files ending in {@code .tar}) in a blob-container listing.
     * Used by retention tests to verify old minute-dirs are deleted while newer ones survive.
     */
    private static long archiveBlobCount(java.util.Map<String, ?> blobs) {
        return blobs.keySet().stream().filter(name -> name.endsWith(".tar")).count();
    }
}
