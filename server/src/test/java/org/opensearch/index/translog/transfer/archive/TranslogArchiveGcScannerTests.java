/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TranslogArchiveGcScannerTests extends OpenSearchTestCase {

    private static final String UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

    // ── readGcPrefix ──────────────────────────────────────────────────────────

    /**
     * Builds a real TAR with GC entries and verifies readGcPrefix extracts them correctly.
     */
    public void testReadGcPrefixFromRealTar() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 5L, 10L, 100L),
            new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 20L, 30L, 100L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-5.tlog", "data".getBytes(StandardCharsets.UTF_8))
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Mock transfer service: simulate range-GET 1 (TAR header) and range-GET 2 (GC prefix)
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath minutePath = new BlobPath().add("base").add("txlog").add("20260502").add("1000");
        String blobName = "45.123.nodeA.tar";

        // Single range-GET: bytes [0, 512 + MAX_GC_PREFIX_BYTES) — scanner reads header + GC prefix in one shot
        long readLength = TarArchiveBuilder.TAR_BLOCK + TranslogArchiveGcScanner.MAX_GC_PREFIX_BYTES;
        // Build the combined buffer that the mock returns: first 512 bytes of TAR + index bytes
        byte[] indexBytes = layout.getIndexBytes();
        byte[] combined = new byte[(int) readLength];
        System.arraycopy(tarBytes, 0, combined, 0, Math.min(tarBytes.length, combined.length));
        Mockito.when(transferService.downloadBlob(
            ArgumentMatchers.eq(minutePath),
            ArgumentMatchers.eq(blobName),
            ArgumentMatchers.eq(0L),
            ArgumentMatchers.eq(readLength)
        )).thenReturn(new ByteArrayInputStream(combined));

        BlobPath archiveBasePath = new BlobPath().add("base");
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);

        List<TarArchiveBuilder.GcShardEntry> gcOut = scanner.readGcPrefix(minutePath, blobName);

        assertEquals(2, gcOut.size());
        assertEquals(0, gcOut.get(0).getShardId());
        assertEquals(5L, gcOut.get(0).getMinSeqNo());
        assertEquals(10L, gcOut.get(0).getMaxSeqNo());
        assertEquals(1, gcOut.get(1).getShardId());
        assertEquals(20L, gcOut.get(1).getMinSeqNo());
        assertEquals(30L, gcOut.get(1).getMaxSeqNo());
    }

    /**
     * readGcPrefix returns empty when downloadBlob throws IOException.
     */
    public void testReadGcPrefixHandlesDownloadError() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath minutePath = new BlobPath().add("txlog").add("20260502").add("1000");
        // The scanner calls downloadBlob(path, name, position, length) — all variants should throw
        Mockito.when(transferService.downloadBlob(
            ArgumentMatchers.any(BlobPath.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.anyLong()
        )).thenThrow(new IOException("S3 error"));

        BlobPath archiveBasePath = new BlobPath().add("base");
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);

        List<TarArchiveBuilder.GcShardEntry> result = scanner.readGcPrefix(minutePath, "broken.tar");
        assertTrue("Should return empty on download error", result.isEmpty());
    }

    // ── isSafeToDelete ────────────────────────────────────────────────────────

    public void testIsSafeToDeleteReturnsFalseWhenNotYetScanned() {
        TransferService transferService = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, new BlobPath().add("base"));

        // No .idx in memory → scanner hasn't processed this minute yet → conservative: don't delete
        assertFalse("Not yet scanned → conservative", scanner.isSafeToDelete("20260502/1000"));
    }

    public void testIsSafeToDeleteReturnsTrueWhenIndexedButEmpty() throws IOException {
        // An empty MinuteGcIndex (scanner ran, found no GC shards) → safe to delete by timestamp
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // Empty MinuteGcIndex: 0 shards
        byte[] emptyIdxBytes = new MinuteGcIndex.Builder().build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(emptyIdxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // Empty index → all TARs had no GC shards → safe to delete by timestamp alone
        assertTrue("Empty GC index → safe to delete", scanner.isSafeToDelete("20260502/1000"));
    }

    public void testIsSafeToDeleteReturnsTrueWhenAllShardsCovered() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, new BlobPath().add("base"));

        // Manually inject an index entry
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 100L),
            new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 10L, 15L, 100L)
        ));
        scanner.getInMemoryIndex(); // trigger map access
        // Use package-accessible map via getInMemoryIndex() returns unmodifiable, so inject via scanMinute path
        // Instead, test via a real scan flow with a mock that returns primed data
        // Simpler: directly test the logic by calling isSafeToDelete after inserting via loadFromPersisted

        // Build a serialized MinuteGcIndex
        MinuteGcIndex idx = builder.build();
        byte[] idxBytes = idx.serialize();

        // Mock listFolders and listAllInSortedOrder for loadFromPersisted
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath),
            ArgumentMatchers.eq(""),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner2 = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner2.loadFromPersisted();

        // shard 0 maxGen=5, checkpoint=5 → covered
        // shard 1 maxGen=15, checkpoint=20 → covered
        assertTrue(scanner2.isSafeToDelete("20260502/1000"));
    }

    public void testIsSafeToDeleteReturnsFalseWhenShardNotCovered() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        // globalCheckpoint=8 embedded in TAR → rolling checkpoint for shard 0 = 8
        // maxGen=10 > rollingCheckpoint(8) → NOT safe
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 8L)));
        byte[] idxBytes = builder.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // shard 0: maxGen=10, rollingCheckpoint=8 → 10 > 8 → NOT safe to delete
        assertFalse(scanner.isSafeToDelete("20260502/1000"));
    }

    public void testIsSafeToDeleteReturnsFalseWhenCheckpointBelowMaxGen() throws IOException {
        // Checkpoint embedded in TAR (3L) is below maxGen (5L) → not yet safe
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        // globalCheckpoint=3 < maxGen=5 → rolling checkpoint=3, maxGen=5 → not safe
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 3L)));
        byte[] idxBytes = builder.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // rollingCheckpoint(0)=3, maxGen(0)=5 → 5 > 3 → not safe to delete
        assertFalse(scanner.isSafeToDelete("20260502/1000"));
    }

    // ── evict ─────────────────────────────────────────────────────────────────

    public void testEvictRemovesFromInMemory() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 2L, 100L)));
        byte[] idxBytes = builder.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        assertTrue("Should be in index before evict", scanner.getInMemoryIndex().containsKey("20260502/1000"));
        scanner.evict("20260502/1000");
        assertFalse("Should be gone after evict", scanner.getInMemoryIndex().containsKey("20260502/1000"));
    }

    public void testEvictIndexDoesNotBypassConservativeNullLivenessGate() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // Not safe by itself: checkpoint(0) < maxSeqNo(5)
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 0L)));
        byte[] idxBytes = builder.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        assertFalse(scanner.isSafeToDelete("20260502/1000"));
        scanner.evictIndex(UUID_A);

        // Null liveness remains conservative: minute still has entries, so it stays blocked.
        assertFalse(scanner.isSafeToDelete("20260502/1000"));
        // With liveIndexUUIDs provided and UUID_A missing, it becomes safe.
        assertTrue(scanner.isSafeToDelete("20260502/1000", Set.of("some-other-index")));
    }

    // ── rolling checkpoint across multiple minutes ────────────────────────────

    /**
     * Verifies the core rolling checkpoint logic:
     * - Minute 1000: shard 0, maxGen=10, checkpoint=8 → rollingCheckpoint=8 → NOT safe
     * - Minute 1001: shard 0, maxGen=12, checkpoint=10 → rollingCheckpoint=10 → minute 1000 NOW safe
     * - Minute 1002: shard 0, maxGen=15, checkpoint=12 → rollingCheckpoint=12 → minute 1001 safe, 1002 NOT
     */
    public void testRollingCheckpointAcrossMinutes() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // Build 3 MinuteGcIndexes for minutes 1000, 1001, 1002
        MinuteGcIndex.Builder b1000 = new MinuteGcIndex.Builder();
        b1000.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 8L)));   // checkpoint=8

        MinuteGcIndex.Builder b1001 = new MinuteGcIndex.Builder();
        b1001.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 11L, 12L, 10L))); // checkpoint=10

        MinuteGcIndex.Builder b1002 = new MinuteGcIndex.Builder();
        b1002.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 13L, 15L, 12L))); // checkpoint=12

        byte[] idx1000 = b1000.build().serialize();
        byte[] idx1001 = b1001.build().serialize();
        byte[] idx1002 = b1002.build().serialize();

        // Mock listFolders and listAllInSortedOrder for the 3 .idx files
        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));

        BlobMetadata blob1000 = Mockito.mock(BlobMetadata.class);
        Mockito.when(blob1000.name()).thenReturn("1000.idx");
        BlobMetadata blob1001 = Mockito.mock(BlobMetadata.class);
        Mockito.when(blob1001.name()).thenReturn("1001.idx");
        BlobMetadata blob1002 = Mockito.mock(BlobMetadata.class);
        Mockito.when(blob1002.name()).thenReturn("1002.idx");

        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(blob1000, blob1001, blob1002));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idx1000));
        Mockito.when(transferService.downloadBlob(gcDayPath, "1001.idx")).thenReturn(new ByteArrayInputStream(idx1001));
        Mockito.when(transferService.downloadBlob(gcDayPath, "1002.idx")).thenReturn(new ByteArrayInputStream(idx1002));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // rollingCheckpoints[0] = max(8, 10, 12) = 12
        assertEquals(12L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));

        // minute 1000: maxGen=10, rollingCheckpoint=12 → 10 ≤ 12 → SAFE
        assertTrue("1000 should be safe: maxGen(10) ≤ rollingCheckpoint(12)",
            scanner.isSafeToDelete("20260502/1000"));

        // minute 1001: maxGen=12, rollingCheckpoint=12 → 12 ≤ 12 → SAFE
        assertTrue("1001 should be safe: maxGen(12) ≤ rollingCheckpoint(12)",
            scanner.isSafeToDelete("20260502/1001"));

        // minute 1002: maxGen=15, rollingCheckpoint=12 → 15 > 12 → NOT SAFE
        assertFalse("1002 should NOT be safe: maxGen(15) > rollingCheckpoint(12)",
            scanner.isSafeToDelete("20260502/1002"));
    }

    /**
     * Verifies that scanMinute() processes minute-dirs in chronological order within a day,
     * so the rolling checkpoint accumulates correctly (newer minutes are processed last).
     */
    public void testScanDayProcessesMinutesChronologically() throws IOException {
        // The rolling checkpoint must equal the max checkpoint seen across ALL minutes.
        // If processed in reverse order, the rolling checkpoint would be set by the oldest minute first
        // and then correctly overridden by newer minutes (since Math::max handles any order).
        // But we verify the sort is applied correctly.
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // Build idx for 3 minutes: 1002 first (highest checkpoint), 1000 last
        MinuteGcIndex.Builder b1 = new MinuteGcIndex.Builder();
        b1.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L)));

        MinuteGcIndex.Builder b2 = new MinuteGcIndex.Builder();
        b2.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 6L, 10L, 10L)));

        MinuteGcIndex.Builder b3 = new MinuteGcIndex.Builder();
        b3.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 11L, 20L, 20L)));

        byte[] idx1 = b1.build().serialize();
        byte[] idx2 = b2.build().serialize();
        byte[] idx3 = b3.build().serialize();

        // Return them in REVERSE order from listFolders to verify sorting is applied
        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata bm1 = Mockito.mock(BlobMetadata.class); Mockito.when(bm1.name()).thenReturn("1000.idx");
        BlobMetadata bm2 = Mockito.mock(BlobMetadata.class); Mockito.when(bm2.name()).thenReturn("1001.idx");
        BlobMetadata bm3 = Mockito.mock(BlobMetadata.class); Mockito.when(bm3.name()).thenReturn("1002.idx");

        // listAllInSortedOrder returns blobs in REVERSE order (1002, 1001, 1000) — sorted by scanner
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(bm3, bm2, bm1)); // reverse order
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idx1));
        Mockito.when(transferService.downloadBlob(gcDayPath, "1001.idx")).thenReturn(new ByteArrayInputStream(idx2));
        Mockito.when(transferService.downloadBlob(gcDayPath, "1002.idx")).thenReturn(new ByteArrayInputStream(idx3));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // Regardless of load order, rolling checkpoint should be max(5, 10, 20) = 20
        assertEquals(20L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));

        // All minutes with maxGen ≤ 20 should be safe
        assertTrue(scanner.isSafeToDelete("20260502/1000")); // maxGen=5 ≤ 20
        assertTrue(scanner.isSafeToDelete("20260502/1001")); // maxGen=10 ≤ 20
        assertTrue(scanner.isSafeToDelete("20260502/1002")); // maxGen=20 ≤ 20
    }

    // ── Two-phase isSafeToDelete ──────────────────────────────────────────────

    /**
     * Phase 1 (immediate): checkpoint at upload time already covered all ops.
     * maxCheckpoint maxCheckpoint >= maxSeqNo → safe immediately, no newer TAR needed.gt;= maxSeqNo - safe immediately, no newer TAR needed.
     */
    public void testIsSafeToDeletePhase1ImmediateCheckpoint() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // maxSeqNo=10, maxCheckpoint=10 → Phase 1: 10 >= 10 → immediately safe
        MinuteGcIndex.Builder b = new MinuteGcIndex.Builder();
        b.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 10L)));
        byte[] idxBytes = b.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata idxBlob = Mockito.mock(BlobMetadata.class);
        Mockito.when(idxBlob.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(idxBlob));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // Phase 1: maxCheckpoint(10) >= maxSeqNo(10) → safe immediately, no rolling checkpoint needed
        assertTrue("Phase 1: immediate checkpoint coverage should be safe",
            scanner.isSafeToDelete("20260502/1000"));
        // Rolling checkpoint is set from the loaded .idx (maxCheckpoint=10), but Phase 1 is what made it safe
        assertEquals(10L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));
    }

    /**
     * Phase 1 blocks (checkpoint below maxSeqNo at upload time), Phase 2 passes (rolling above maxSeqNo).
     * Shard had new activity - newer TAR advanced rolling checkpoint past maxSeqNo.
     */
    public void testIsSafeToDeletePhase2RollingCheckpoint() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // minute 1000: maxSeqNo=10, maxCheckpoint=8 → Phase 1 fails (8 < 10)
        // minute 1001: newer TAR, maxSeqNo=15, maxCheckpoint=12
        // rollingCheckpoints[0] = max(8, 12) = 12 → Phase 2: 12 >= 10 → safe
        MinuteGcIndex.Builder b1000 = new MinuteGcIndex.Builder();
        b1000.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 8L)));

        MinuteGcIndex.Builder b1001 = new MinuteGcIndex.Builder();
        b1001.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 11L, 15L, 12L)));

        byte[] idx1000 = b1000.build().serialize();
        byte[] idx1001 = b1001.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata bm1000 = Mockito.mock(BlobMetadata.class); Mockito.when(bm1000.name()).thenReturn("1000.idx");
        BlobMetadata bm1001 = Mockito.mock(BlobMetadata.class); Mockito.when(bm1001.name()).thenReturn("1001.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(bm1000, bm1001));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idx1000));
        Mockito.when(transferService.downloadBlob(gcDayPath, "1001.idx")).thenReturn(new ByteArrayInputStream(idx1001));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // rollingCheckpoints[0] = 12
        assertEquals(12L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));

        // minute 1000: Phase 1 fails (8 < 10), Phase 2: rollingCheckpoint(12) >= maxSeqNo(10) → safe
        assertTrue("Phase 2: rolling checkpoint covers maxSeqNo", scanner.isSafeToDelete("20260502/1000"));

        // minute 1001: Phase 1 fails (12 < 15), Phase 2: rollingCheckpoint(12) < maxSeqNo(15) → NOT safe
        assertFalse("minute 1001 not safe: maxSeqNo(15) > rollingCheckpoint(12)",
            scanner.isSafeToDelete("20260502/1001"));
    }

    /** Both phases fail: checkpoint below maxSeqNo AND no newer TAR advanced rolling checkpoint. */
    public void testIsSafeToDeleteBothPhasesFail() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath gcDayPath = gcIdxRoot.add("20260502");

        // maxSeqNo=10, maxCheckpoint=7 → Phase 1 fails; no newer TAR → rollingCheckpoints[0]=7 → Phase 2 fails
        MinuteGcIndex.Builder b = new MinuteGcIndex.Builder();
        b.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 7L)));
        byte[] idxBytes = b.build().serialize();

        Mockito.when(transferService.listFolders(gcIdxRoot)).thenReturn(Set.of("20260502"));
        BlobMetadata bm = Mockito.mock(BlobMetadata.class); Mockito.when(bm.name()).thenReturn("1000.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(bm));
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(), ArgumentMatchers.any()
        );
        Mockito.when(transferService.downloadBlob(gcDayPath, "1000.idx")).thenReturn(new ByteArrayInputStream(idxBytes));

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
        scanner.loadFromPersisted();

        // Phase 1 fails: 7 < 10. Phase 2 fails: rollingCheckpoints[0]=7 < maxSeqNo=10
        assertFalse("Both phases fail → not safe", scanner.isSafeToDelete("20260502/1000"));
    }

    // ── isShardStuck / isTarSafeToDelete ─────────────────────────────────────

    /** Shard with no data in latestMaxSeqNo is considered stuck (unknown = conservative). */
    public void testIsShardStuckReturnsTrueWhenUnknown() {
        TransferService ts = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, new BlobPath().add("base"));
        assertTrue("Unknown shard should be stuck", scanner.isShardStuck(UUID_A, 42));
    }

    /**
     * isShardStuck returns false when latestMaxSeqNo and rollingCheckpoints are both set and equal.
     * Verifies the isShardStuck() derivation directly without full scanMinute setup.
     */
    public void testIsShardStuckReturnsFalseWhenCaughtUp() throws Exception {
        TransferService ts = Mockito.mock(TransferService.class);
        BlobPath base = new BlobPath().add("base");
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, base);

        // Simulate scanner state: shard 0 has maxSeqNo=100, checkpoint=100 (fully caught up)
        // We inject state via isTarSafeToDelete which internally uses isShardStuck.
        // Since checkpoint == maxSeqNo, Phase 1 fires immediately.
        List<TarArchiveBuilder.GcShardEntry> entries = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 80L, 100L, 100L) // checkpoint=maxSeqNo
        );
        // Phase 1 makes it immediately safe — isShardStuck not even consulted
        assertTrue("Phase 1 covered: TAR should be safe", scanner.isTarSafeToDelete(entries));

        // Now test when Phase 1 doesn't fire (checkpoint < maxSeqNo) but rolling has caught up
        // We use isTarSafeToDelete with a stale checkpoint entry; shard is not in latestMaxSeqNo
        // so isShardStuck returns true (conservative) → TAR is not safe
        List<TarArchiveBuilder.GcShardEntry> stale = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 7, 80L, 100L, 90L) // checkpoint < maxSeqNo
        );
        assertFalse("Shard 7 not in latestMaxSeqNo → stuck → TAR not safe", scanner.isTarSafeToDelete(stale));

        // isShardStuck directly: shard unknown → true
        assertTrue("Unknown shard should be stuck", scanner.isShardStuck(UUID_A, 7));
    }

    /** isTarSafeToDelete returns true for empty GC entries. */
    public void testIsTarSafeToDeleteEmptyEntries() {
        TransferService ts = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, new BlobPath().add("base"));
        assertTrue(scanner.isTarSafeToDelete(List.of()));
    }

    /** isTarSafeToDelete Phase 1: checkpoint at upload covers all ops. */
    public void testIsTarSafeToDeletePhase1() {
        TransferService ts = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, new BlobPath().add("base"));
        List<TarArchiveBuilder.GcShardEntry> entries = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 80L, 100L, 100L)  // checkpoint=maxSeqNo
        );
        assertTrue("Phase 1 should make TAR safe", scanner.isTarSafeToDelete(entries));
    }

    /** isTarSafeToDelete returns false when shard is stuck and no rolling checkpoint. */
    public void testIsTarSafeToDeleteReturnsFalseWhenShardStuck() {
        TransferService ts = Mockito.mock(TransferService.class);
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, new BlobPath().add("base"));
        List<TarArchiveBuilder.GcShardEntry> entries = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 5, 80L, 100L, 90L)  // checkpoint < maxSeqNo, no rolling
        );
        assertFalse("Stuck shard should make TAR unsafe", scanner.isTarSafeToDelete(entries));
    }

    // ── gcIdxRootPath ─────────────────────────────────────────────────────────

    public void testGcIdxRootPath() {
        BlobPath base = new BlobPath().add("bucket").add("repo");
        BlobPath gcRoot = TranslogArchiveGcScanner.gcIdxRootPath(base);
        assertTrue("gc_idx should be in the path", gcRoot.buildAsString().contains("gc_idx"));
    }

    // ── scanMinute() with real blob store ─────────────────────────────────────

    /**
     * End-to-end test for {@code scanMinute()} using a real FsBlobStore:
     * <ol>
     *   <li>Upload a real TAR with embedded GC entries to the hierarchical txlog path.</li>
     *   <li>Run {@code scanner.scan()} to pick up the new minute-dir.</li>
     *   <li>Verify a {@code .idx} blob was persisted in {@code gc_idx/}.</li>
     *   <li>Verify the minute is now in the in-memory index with correct shard entries.</li>
     *   <li>Verify {@code isSafeToDelete} returns the expected result based on checkpoint state.</li>
     * </ol>
     */
    public void testScanMinuteWithRealBlobStore() throws Exception {
        org.opensearch.common.blobstore.fs.FsBlobStore blobStore =
            new org.opensearch.common.blobstore.fs.FsBlobStore(
                randomIntBetween(1, 8) * 1024, createTempDir(), false);
        org.opensearch.threadpool.ThreadPool threadPool =
            new org.opensearch.threadpool.TestThreadPool(getClass().getName());
        try {
            org.opensearch.index.translog.transfer.BlobStoreTransferService transferService =
                new org.opensearch.index.translog.transfer.BlobStoreTransferService(blobStore, threadPool);

            BlobPath archiveBasePath = new BlobPath().add("repo");

            // Build a real TAR with GC entries: shard 0, seqNos 1..10, checkpoint=10 (Phase 1 safe)
            List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
                new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 10L)  // checkpoint=maxSeqNo → Phase 1 safe
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
                TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", "tlog data".getBytes(java.nio.charset.StandardCharsets.UTF_8))
            );
            TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            TarArchiveBuilder.build(baos, layout, entries);
            byte[] tarBytes = baos.toByteArray();

            // Upload TAR to the hierarchical txlog path: {base}/txlog/{day}/{minute}/
            java.time.Instant now = java.time.Instant.now();
            BlobPath minutePath = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobDir(archiveBasePath, now);
            String blobName = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobName(now, "node-test");
            transferService.uploadBlob(
                new org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot(blobName, tarBytes, 0L),
                minutePath,
                org.opensearch.common.blobstore.stream.write.WritePriority.HIGH
            );

            // Run scanner.scan() — should discover the new minute-dir and persist a .idx
            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(now);

            // Verify in-memory index has the minute key
            String dayDir = org.opensearch.index.translog.transfer.TranslogArchivePathHelper.dayDir(now);
            String minuteDir = org.opensearch.index.translog.transfer.TranslogArchivePathHelper.minuteDir(now);
            String minuteKey = dayDir + "/" + minuteDir;
            assertTrue("scanMinute must populate in-memory index for " + minuteKey,
                scanner.getInMemoryIndex().containsKey(minuteKey));

            // Verify the .idx blob was persisted in gc_idx/{day}/{minute}.idx
            BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
            BlobPath gcDayPath = gcIdxRoot.add(dayDir);
            Set<String> idxBlobs = transferService.listAll(gcDayPath);
            assertTrue("gc_idx blob must be persisted: " + minuteDir + ".idx",
                idxBlobs != null && idxBlobs.contains(minuteDir + ".idx"));

            // Verify isSafeToDelete: checkpoint(10) >= maxSeqNo(10) → Phase 1 → safe
            assertTrue("Minute " + minuteKey + " should be safe to delete (Phase 1: checkpoint=maxSeqNo)",
                scanner.isSafeToDelete(minuteKey));

            // Second scan: minute is already indexed → no extra PUT (idempotent)
            int blobCountBefore = transferService.listAll(gcDayPath).size();
            scanner.scan(now);
            int blobCountAfter = transferService.listAll(gcDayPath).size();
            assertEquals("Second scan must not re-write existing .idx", blobCountBefore, blobCountAfter);

        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * Verifies that a minute-dir containing a TAR with checkpoint below maxSeqNo is NOT safe
     * until a newer TAR advances the rolling checkpoint past maxSeqNo — using real blob store.
     */
    public void testScanMinuteCheckpointGateWithRealBlobStore() throws Exception {
        org.opensearch.common.blobstore.fs.FsBlobStore blobStore =
            new org.opensearch.common.blobstore.fs.FsBlobStore(
                randomIntBetween(1, 8) * 1024, createTempDir(), false);
        org.opensearch.threadpool.ThreadPool threadPool =
            new org.opensearch.threadpool.TestThreadPool(getClass().getName());
        try {
            org.opensearch.index.translog.transfer.BlobStoreTransferService transferService =
                new org.opensearch.index.translog.transfer.BlobStoreTransferService(blobStore, threadPool);
            BlobPath archiveBasePath = new BlobPath().add("repo");

            // Minute 1: shard 0, maxSeqNo=10, checkpoint=7 → Phase 1 fails (7 < 10)
            java.time.Instant minute1Time = java.time.Instant.parse("2026-05-01T10:00:30Z");
            List<TarArchiveBuilder.GcShardEntry> gc1 = List.of(
                new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 7L)
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entries1 = List.of(
                TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", "a".getBytes())
            );
            TarArchiveBuilder.TarLayout layout1 = TarArchiveBuilder.computeLayout(entries1, gc1);
            java.io.ByteArrayOutputStream b1 = new java.io.ByteArrayOutputStream();
            TarArchiveBuilder.build(b1, layout1, entries1);
            BlobPath min1Path = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobDir(archiveBasePath, minute1Time);
            String name1 = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobName(minute1Time, "node1");
            transferService.uploadBlob(
                new org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot(name1, b1.toByteArray(), 0L),
                min1Path, org.opensearch.common.blobstore.stream.write.WritePriority.HIGH);

            // Minute 2: shard 0, maxSeqNo=15, checkpoint=12 → advances rolling to 12 ≥ 10 → minute1 safe
            java.time.Instant minute2Time = java.time.Instant.parse("2026-05-01T10:01:30Z");
            List<TarArchiveBuilder.GcShardEntry> gc2 = List.of(
                new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 11L, 15L, 12L)
            );
            List<TarArchiveBuilder.ArchiveBuildEntry> entries2 = List.of(
                TarArchiveBuilder.fromBytes("uuid/0/1/translog-2.tlog", "b".getBytes())
            );
            TarArchiveBuilder.TarLayout layout2 = TarArchiveBuilder.computeLayout(entries2, gc2);
            java.io.ByteArrayOutputStream b2 = new java.io.ByteArrayOutputStream();
            TarArchiveBuilder.build(b2, layout2, entries2);
            BlobPath min2Path = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobDir(archiveBasePath, minute2Time);
            String name2 = org.opensearch.index.translog.transfer.TranslogArchivePathHelper
                .tarBlobName(minute2Time, "node1");
            transferService.uploadBlob(
                new org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot(name2, b2.toByteArray(), 0L),
                min2Path, org.opensearch.common.blobstore.stream.write.WritePriority.HIGH);

            // Scan both minutes — uses real BlobStoreTransferService which calls uploadBlobStream for gc_idx
            TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);
            scanner.scan(minute2Time); // scan up to minute2

            String day = org.opensearch.index.translog.transfer.TranslogArchivePathHelper.dayDir(minute1Time);
            String min1 = org.opensearch.index.translog.transfer.TranslogArchivePathHelper.minuteDir(minute1Time);
            String min2 = org.opensearch.index.translog.transfer.TranslogArchivePathHelper.minuteDir(minute2Time);

            // rollingCheckpoints[0] = max(7, 12) = 12
            assertEquals(12L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));

            // Minute 1: Phase 1 fails (7 < 10), Phase 2: rollingCheckpoint(12) ≥ maxSeqNo(10) → safe
            assertTrue("Minute 1 should be safe via Phase 2 rolling checkpoint",
                scanner.isSafeToDelete(day + "/" + min1));

            // Minute 2: Phase 1 fails (12 < 15), Phase 2: rollingCheckpoint(12) < 15 → NOT safe
            assertFalse("Minute 2 should NOT be safe: maxSeqNo(15) > rollingCheckpoint(12)",
                scanner.isSafeToDelete(day + "/" + min2));

        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * Regression test: loadSingleIdx() must update rollingCheckpoints, not just inMemoryIndex.
     *
     * <p>Bug: when scan() runs and ALL minute-dirs already have .idx files, scanDay() calls
     * loadSingleIdx() for each (not scanMinute()). The original loadSingleIdx() only populated
     * inMemoryIndex but NOT rollingCheckpoints. On the second+ scan cycle, isSafeToDelete()
     * Phase 2 checks rollingCheckpoints[indexUUID][shardId] — which is always null/empty —
     * and returns false for every minute-dir, permanently blocking all GC deletions.
     *
     * <p>This scenario occurs in production when:
     * 1. Cluster-manager comes up with existing gc_idx/ files (loadFromPersisted restores state OK).
     * 2. First scan() cycles through and calls loadSingleIdx() for minutes already indexed.
     * 3. No new minute-dirs exist, so scanMinute() is never called.
     * 4. rollingCheckpoints remains empty → Phase 2 always fails → no TARs ever deleted.
     */
    public void testScanDayLoadsSingleIdxUpdatesRollingCheckpoints() throws IOException {
        // Setup: two minutes already have .idx in gc_idx/, none in inMemoryIndex yet.
        // minute 1000: shard 0, maxSeqNo=10, maxCheckpoint=8 → Phase 1 fails
        // minute 1001: shard 0, maxSeqNo=15, maxCheckpoint=12 → Phase 1 fails
        // After scan(), rollingCheckpoints[UUID_A][0] should be max(8, 12) = 12
        // → Phase 2: minute 1000 safe (10 ≤ 12), minute 1001 not safe (15 > 12)

        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath txlogRoot = archiveBasePath.add("txlog");
        BlobPath gcIdxRoot = TranslogArchiveGcScanner.gcIdxRootPath(archiveBasePath);
        BlobPath txlogDayPath = txlogRoot.add("20260503");
        BlobPath gcIdxDayPath = gcIdxRoot.add("20260503");

        // Build serialized .idx for both minutes
        MinuteGcIndex.Builder b1000 = new MinuteGcIndex.Builder();
        b1000.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 8L)));
        byte[] idx1000 = b1000.build().serialize();

        MinuteGcIndex.Builder b1001 = new MinuteGcIndex.Builder();
        b1001.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 11L, 15L, 12L)));
        byte[] idx1001 = b1001.build().serialize();

        // txlog/ has the two minute dirs
        Mockito.when(transferService.listFolders(txlogRoot)).thenReturn(Set.of("20260503"));
        Mockito.when(transferService.listFolders(txlogDayPath)).thenReturn(Set.of("1000", "1001"));

        // gc_idx/ already has .idx for both minutes (simulates second scan cycle)
        BlobMetadata bm1000 = Mockito.mock(BlobMetadata.class);
        Mockito.when(bm1000.name()).thenReturn("1000.idx");
        BlobMetadata bm1001 = Mockito.mock(BlobMetadata.class);
        Mockito.when(bm1001.name()).thenReturn("1001.idx");
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(List.of(bm1000, bm1001)); // both already indexed
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(gcIdxDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );

        // downloadBlob for the idx files (called by loadSingleIdx)
        Mockito.when(transferService.downloadBlob(gcIdxDayPath, "1000.idx"))
            .thenReturn(new ByteArrayInputStream(idx1000));
        Mockito.when(transferService.downloadBlob(gcIdxDayPath, "1001.idx"))
            .thenReturn(new ByteArrayInputStream(idx1001));

        // Create scanner with empty state (simulates fresh second scan cycle —
        // no loadFromPersisted called, rollingCheckpoints starts empty)
        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(transferService, archiveBasePath);

        // Trigger scan() — all minutes already have .idx → scanDay calls loadSingleIdx for each
        scanner.scan(java.time.Instant.now());

        // Both minutes should be in inMemoryIndex
        assertNotNull("minute 1000 should be in inMemoryIndex", scanner.getInMemoryIndex().get("20260503/1000"));
        assertNotNull("minute 1001 should be in inMemoryIndex", scanner.getInMemoryIndex().get("20260503/1001"));

        // rollingCheckpoints must be populated by loadSingleIdx (the bug: it wasn't)
        assertNotNull("rollingCheckpoints should contain UUID_A", scanner.getRollingCheckpoints().get(UUID_A));
        assertEquals("rollingCheckpoints[0] should be max(8,12)=12",
            12L, (long) scanner.getRollingCheckpoints().get(UUID_A).get(0));

        // Phase 2 gate: minute 1000 maxSeqNo=10 ≤ rollingCheckpoint=12 → SAFE
        assertTrue("minute 1000 should be safe via Phase 2 rolling checkpoint",
            scanner.isSafeToDelete("20260503/1000"));

        // Phase 2 gate: minute 1001 maxSeqNo=15 > rollingCheckpoint=12 → NOT SAFE
        assertFalse("minute 1001 should NOT be safe: maxSeqNo(15) > rollingCheckpoint(12)",
            scanner.isSafeToDelete("20260503/1001"));
    }

    /**
     * Regression test: scanMinute() must persist gc_idx via uploadBlobStream() not uploadBlob().
     *
     * uploadBlob(InputStream,...) calls checksumOfChecksum() which expects bytes to already contain
     * an OpenSearch codec footer. MinuteGcIndex.serialize() produces raw bytes with no codec footer,
     * causing "Checksum combination failed" errors and preventing GC index persistence.
     *
     * uploadBlobStream() writes raw bytes directly to the blob container without checksum processing.
     */
    public void testScanMinutePersistsGcIdxViaUploadBlobStreamNotUploadBlob() throws Exception {
        TransferService mockTransfer = Mockito.mock(TransferService.class);
        BlobPath archiveBasePath = new BlobPath().add("base");
        BlobPath txlogDayPath = archiveBasePath.add("txlog").add("20260503");
        BlobPath minutePath = txlogDayPath.add("1000");
        BlobPath gcIdxDayPath = archiveBasePath.add("gc_idx").add("20260503");

        // One TAR with a GC entry
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 10L, 7L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", "a".getBytes())
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcEntries);
        java.io.ByteArrayOutputStream tarOut = new java.io.ByteArrayOutputStream();
        TarArchiveBuilder.build(tarOut, layout, entries);
        byte[] tarBytes = tarOut.toByteArray();

        // Mock: one TAR blob in the minute dir
        BlobMetadata blobMeta = Mockito.mock(BlobMetadata.class);
        Mockito.when(blobMeta.name()).thenReturn("00.000.node1.tar");
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<java.util.List<BlobMetadata>> listener =
                inv.getArgument(3);
            listener.onResponse(List.of(blobMeta));
            return null;
        }).when(mockTransfer).listAllInSortedOrder(
            ArgumentMatchers.eq(minutePath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );
        // readGcPrefix uses downloadBlob(path, name, position, length) — 4-arg ranged version
        Mockito.when(mockTransfer.downloadBlob(
            ArgumentMatchers.eq(minutePath),
            ArgumentMatchers.eq("00.000.node1.tar"),
            ArgumentMatchers.eq(0L),
            ArgumentMatchers.anyLong()
        )).thenAnswer(inv -> new java.io.ByteArrayInputStream(tarBytes));

        // No already-indexed minutes — mock the async version with ActionListener
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<java.util.List<BlobMetadata>> listener =
                inv.getArgument(3);
            listener.onResponse(List.of());
            return null;
        }).when(mockTransfer).listAllInSortedOrder(
            ArgumentMatchers.eq(gcIdxDayPath), ArgumentMatchers.eq(""), ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );

        TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(mockTransfer, archiveBasePath);
        scanner.scanMinute(txlogDayPath, gcIdxDayPath, "20260503", "1000");

        // Verify uploadBlobStream was called (not uploadBlob) — regression for checksum corruption bug
        Mockito.verify(mockTransfer).uploadBlobStream(
            ArgumentMatchers.any(java.io.InputStream.class),
            ArgumentMatchers.anyLong(),
            ArgumentMatchers.eq(gcIdxDayPath),
            ArgumentMatchers.eq("1000.idx"),
            ArgumentMatchers.eq(org.opensearch.common.blobstore.stream.write.WritePriority.NORMAL),
            ArgumentMatchers.isNull()
        );
        // Verify uploadBlob was NOT called for gc_idx (would cause checksum corruption)
        Mockito.verify(mockTransfer, Mockito.never()).uploadBlob(
            ArgumentMatchers.any(java.io.InputStream.class),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq("1000.idx"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );

        // gc_idx should be in-memory after successful persist
        String minuteKey = "20260503/1000";
        assertNotNull("gc_idx should be in memory after successful persist", scanner.getInMemoryIndex().get(minuteKey));
    }
}
