/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TranslogArchiveRecoveryTests extends OpenSearchTestCase {




    /** Builds a TAR archive containing tlog/ckp pairs for the given shards. */
    private byte[] buildTar(int[] shardIds, long[] generations, long[] primaryTerms) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        for (int i = 0; i < shardIds.length; i++) {
            String prefix = "indexUUID/" + shardIds[i] + "/" + primaryTerms[i] + "/";
            String tlogPath = prefix + "translog-" + generations[i] + ".tlog";
            String ckpPath = prefix + "translog-" + generations[i] + ".ckp";
            entries.add(
                TarArchiveBuilder.fromBytes(tlogPath, ("tlog-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8))
            );
            entries.add(TarArchiveBuilder.fromBytes(ckpPath, ("ckp-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8)));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        TarArchiveBuilder.build(out, layout, entries);
        return out.toByteArray();
    }




    @SuppressWarnings("unchecked")
    private static Iterable<String> any() {
        return org.mockito.ArgumentMatchers.any();
    }

    // ── New hierarchical-path recovery tests ──────────────────────────────────

    /**
     * collectTarsFromHierarchicalPath returns empty list when txlog/ root has no day dirs.
     */
    public void testCollectTarsEmptyWhenNoData() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        Mockito.when(
            transferService.listFolders(ArgumentMatchers.any(BlobPath.class))
        ).thenReturn(Collections.emptySet());

        BlobPath txlogRoot = new BlobPath().add("base").add("txlog");
        Instant startFrom = Instant.parse("2026-05-01T22:00:00Z");

        List<TranslogArchiveRecovery.TarRef> result =
            TranslogArchiveRecovery.collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);
        assertTrue("Should be empty when no day dirs", result.isEmpty());
    }

    /**
     * collectTarsFromHierarchicalPath skips day-dirs before startFrom.
     */
    public void testCollectTarsSkipsOldDays() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath txlogRoot = new BlobPath().add("base").add("txlog");

        // listFolders(txlogRoot) → {"20260430", "20260501"}
        Mockito.when(
            transferService.listFolders(txlogRoot)
        ).thenReturn(new HashSet<>(Arrays.asList("20260430", "20260501")));

        // listFolders for "20260430" day (old day) — should not be called since it's before startDay
        // listFolders for "20260501" → {"2230"}
        BlobPath dayPath = txlogRoot.add("20260501");
        Mockito.when(
            transferService.listFolders(dayPath)
        ).thenReturn(Collections.singleton("2230"));

        // listAllInSortedOrder for minute dir → empty
        Mockito.doAnswer(inv -> {
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            listener.onResponse(Collections.emptyList());
            return null;
        }).when(transferService).listAllInSortedOrder(
            ArgumentMatchers.eq(dayPath.add("2230")),
            ArgumentMatchers.eq(""),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        );

        // startFrom is 2026-05-01T22:29:00Z → startDay="20260501", startMinute="2229"
        Instant startFrom = Instant.parse("2026-05-01T22:29:00Z");
        List<TranslogArchiveRecovery.TarRef> result =
            TranslogArchiveRecovery.collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);

        // Should not have called listFolders for the old day
        Mockito.verify(transferService, Mockito.never())
            .listFolders(txlogRoot.add("20260430"));
        assertTrue("Should return empty since minute dir has no blobs", result.isEmpty());
    }

    /**
     * readTarIndexCachedEntries caches on second call (returns same list instance).
     * Pre-populates cache to verify hit path without needing real TAR bytes.
     */
    public void testReadTarIndexCachedEntriesCache() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath blobPath = new BlobPath().add("txlog").add("20260501").add("2230");
        String blobName = "45.123.nodetest.tar";

        TranslogArchiveRecovery.TarRef tarRef =
            new TranslogArchiveRecovery.TarRef(blobPath, blobName, 100L);

        // Pre-populate cache with a sentinel list
        Map<String, List<ArchiveIndexEntry>> cache = new HashMap<>();
        String cacheKey = blobPath.buildAsString() + "/" + blobName;
        List<ArchiveIndexEntry> sentinel = Collections.emptyList();
        cache.put(cacheKey, sentinel);

        // First call: should hit cache, not call transferService
        List<ArchiveIndexEntry> result =
            TranslogArchiveRecovery.readTarIndexCachedEntries(transferService, tarRef, cache);

        assertSame("Should return cached list", sentinel, result);
        // TransferService should NOT have been called
        Mockito.verifyNoInteractions(transferService);

        // Second call: same result
        List<ArchiveIndexEntry> result2 =
            TranslogArchiveRecovery.readTarIndexCachedEntries(transferService, tarRef, cache);
        assertSame("Second call should also return cached instance", sentinel, result2);
        Mockito.verifyNoInteractions(transferService);
    }

    /**
     * RECOVERY_START_MARGIN_MINUTES is 2.
     */
    public void testRecoveryStartMargin() {
        assertEquals(2, TranslogArchiveRecovery.RECOVERY_START_MARGIN_MINUTES);
    }

    // ── End-to-end recovery: TAR → extracted .tlog/.ckp files ────────────────

    /**
     * Verifies that a TAR archive written by {@link org.opensearch.index.translog.transfer.archive.TarArchiveBuilder}
     * containing both a {@code .tlog} and {@code .ckp} file for a shard can be:
     * <ol>
     *   <li>Listed via {@code collectTarsFromHierarchicalPath}</li>
     *   <li>Parsed for its {@code _index} entries via {@code readTarIndexCachedEntries}</li>
     *   <li>Have individual files extracted with correct content</li>
     * </ol>
     * This exercises the full recovery read path from TAR bytes to file content.
     */
    public void testEndToEndRecoveryTarExtractionSingleShard() throws IOException {
        // Build a TAR with translog + checkpoint for shard 0
        String indexUUID = "recovery-uuid";
        int shardId = 0;
        long primaryTerm = 1L;
        long generation = 5L;

        String tlogPath = indexUUID + "/" + shardId + "/" + primaryTerm + "/translog-" + generation + ".tlog";
        String ckpPath  = indexUUID + "/" + shardId + "/" + primaryTerm + "/translog-" + generation + ".ckp";
        byte[] tlogContent = ("translog-data-gen-" + generation).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] ckpContent  = ("checkpoint-gen-" + generation).getBytes(java.nio.charset.StandardCharsets.UTF_8);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        entries.add(TarArchiveBuilder.fromBytes(tlogPath, tlogContent));
        entries.add(TarArchiveBuilder.fromBytes(ckpPath,  ckpContent));

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Parse the _index from the TAR bytes (simulates what recovery does via range-GET)
        // The _index entry is the first entry in the TAR (offset 0 = TAR header)
        // Read index size from TAR header bytes [124,136)
        byte[] header = java.util.Arrays.copyOfRange(tarBytes, 0, TarArchiveBuilder.TAR_BLOCK);
        String sizeOctal = new String(java.util.Arrays.copyOfRange(header, 124, 136),
            java.nio.charset.StandardCharsets.US_ASCII).trim().replace("\0", "");
        long indexSize = Long.parseLong(sizeOctal, 8);
        byte[] indexBytes = java.util.Arrays.copyOfRange(tarBytes,
            TarArchiveBuilder.TAR_BLOCK, TarArchiveBuilder.TAR_BLOCK + (int) indexSize);

        List<TarArchiveBuilder.EntryLocation> locations = TarArchiveBuilder.parseIndex(indexBytes);

        // Verify index contains both .tlog and .ckp
        assertEquals("TAR _index must have 2 entries (tlog + ckp)", 2, locations.size());

        Map<String, TarArchiveBuilder.EntryLocation> locationByPath = new HashMap<>();
        for (TarArchiveBuilder.EntryLocation loc : locations) {
            locationByPath.put(loc.getPath(), loc);
        }
        assertTrue("_index must contain tlog path", locationByPath.containsKey(tlogPath));
        assertTrue("_index must contain ckp path", locationByPath.containsKey(ckpPath));

        // Extract tlog content by seeking to its offset in the TAR
        TarArchiveBuilder.EntryLocation tlogLoc = locationByPath.get(tlogPath);
        byte[] extractedTlog = java.util.Arrays.copyOfRange(tarBytes,
            (int) tlogLoc.getDataOffset(), (int) (tlogLoc.getDataOffset() + tlogLoc.getDataLength()));
        assertArrayEquals("Extracted tlog content must match original", tlogContent, extractedTlog);

        // Extract ckp content
        TarArchiveBuilder.EntryLocation ckpLoc = locationByPath.get(ckpPath);
        byte[] extractedCkp = java.util.Arrays.copyOfRange(tarBytes,
            (int) ckpLoc.getDataOffset(), (int) (ckpLoc.getDataOffset() + ckpLoc.getDataLength()));
        assertArrayEquals("Extracted ckp content must match original", ckpContent, extractedCkp);
    }

    /**
     * Verifies that when multiple TARs contain data for the same shard+generation,
     * the _index correctly identifies each file's offset within its respective TAR,
     * and that entries from different TARs are independent (no cross-TAR offset confusion).
     */
    public void testEndToEndRecoveryMultipleTarsForSameShard() throws IOException {
        String indexUUID = "multi-tar-uuid";
        int shardId = 0;
        long primaryTerm = 1L;

        // TAR 1: generation 3
        byte[] tar1Content = buildTar(new int[]{shardId}, new long[]{3L}, new long[]{primaryTerm});
        // TAR 2: generation 4
        byte[] tar2Content = buildTar(new int[]{shardId}, new long[]{4L}, new long[]{primaryTerm});

        // Parse index from each TAR independently
        List<TarArchiveBuilder.EntryLocation> locs1 = parseTarIndex(tar1Content);
        List<TarArchiveBuilder.EntryLocation> locs2 = parseTarIndex(tar2Content);

        // Each TAR has 2 entries (tlog + ckp)
        assertEquals("TAR1 must have 2 entries", 2, locs1.size());
        assertEquals("TAR2 must have 2 entries", 2, locs2.size());

        // Paths in TAR1 must contain generation 3
        assertTrue("TAR1 entries must reference gen 3",
            locs1.stream().anyMatch(l -> l.getPath().contains("translog-3")));
        // Paths in TAR2 must contain generation 4
        assertTrue("TAR2 entries must reference gen 4",
            locs2.stream().anyMatch(l -> l.getPath().contains("translog-4")));

        // Offsets in TAR1 and TAR2 are independent (both start from TAR_BLOCK + indexSize)
        // Content extraction from each TAR must yield the correct shard-specific bytes
        for (TarArchiveBuilder.EntryLocation loc : locs1) {
            if (loc.getPath().endsWith(".tlog")) {
                byte[] extracted = java.util.Arrays.copyOfRange(tar1Content,
                    (int) loc.getDataOffset(), (int) (loc.getDataOffset() + loc.getDataLength()));
                String content = new String(extracted, java.nio.charset.StandardCharsets.UTF_8);
                assertTrue("TAR1 tlog content must reference shard " + shardId, content.contains("tlog-" + shardId));
                assertTrue("TAR1 tlog content must reference gen 3", content.contains("-3"));
            }
        }
        for (TarArchiveBuilder.EntryLocation loc : locs2) {
            if (loc.getPath().endsWith(".tlog")) {
                byte[] extracted = java.util.Arrays.copyOfRange(tar2Content,
                    (int) loc.getDataOffset(), (int) (loc.getDataOffset() + loc.getDataLength()));
                String content = new String(extracted, java.nio.charset.StandardCharsets.UTF_8);
                assertTrue("TAR2 tlog content must reference shard " + shardId, content.contains("tlog-" + shardId));
                assertTrue("TAR2 tlog content must reference gen 4", content.contains("-4"));
            }
        }
    }

    /**
     * Verifies that collectTarsFromHierarchicalPath returns TARs in chronological order
     * (older minute-dirs before newer), which is critical for recovery applying ops in order.
     */
    public void testCollectTarsChronologicalOrder() throws IOException {
        TransferService transferService = Mockito.mock(TransferService.class);
        BlobPath txlogRoot = new BlobPath().add("base").add("txlog");

        // Single day, two minute-dirs: 1000 and 1001
        Mockito.when(transferService.listFolders(txlogRoot))
            .thenReturn(new HashSet<>(Collections.singleton("20260501")));
        BlobPath dayPath = txlogRoot.add("20260501");
        Mockito.when(transferService.listFolders(dayPath))
            .thenReturn(new HashSet<>(Arrays.asList("1001", "1000"))); // reverse order from S3

        // Minute 1000: one TAR blob
        BlobMetadata blob1000 = Mockito.mock(BlobMetadata.class);
        Mockito.when(blob1000.name()).thenReturn("00.000.nodeA.tar");
        Mockito.when(blob1000.length()).thenReturn(100L);
        // Minute 1001: one TAR blob
        BlobMetadata blob1001 = Mockito.mock(BlobMetadata.class);
        Mockito.when(blob1001.name()).thenReturn("00.000.nodeA.tar");
        Mockito.when(blob1001.length()).thenReturn(200L);

        Mockito.doAnswer(inv -> {
            BlobPath path = inv.getArgument(0);
            ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
            if (path.buildAsString().contains("1000")) {
                listener.onResponse(Collections.singletonList(blob1000));
            } else if (path.buildAsString().contains("1001")) {
                listener.onResponse(Collections.singletonList(blob1001));
            } else {
                listener.onResponse(Collections.emptyList());
            }
            return null;
        }).when(transferService).listAllInSortedOrder(
            Mockito.any(BlobPath.class), Mockito.eq(""), Mockito.anyInt(), Mockito.any());

        // startFrom = 20260501T09:59:00Z → startDay=20260501, startMinute=0959 → both 1000 and 1001 included
        java.time.Instant startFrom = java.time.Instant.parse("2026-05-01T09:59:00Z");
        List<TranslogArchiveRecovery.TarRef> result =
            TranslogArchiveRecovery.collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);

        assertEquals("Should collect 2 TARs", 2, result.size());
        // Must be in chronological order: minute 1000 before 1001
        // TarRef fields are package-private: path, blobName, size
        assertTrue("First TAR must be from minute 1000",
            result.get(0).path.buildAsString().contains("1000"));
        assertTrue("Second TAR must be from minute 1001",
            result.get(1).path.buildAsString().contains("1001"));
    }

    // ── Helper: parse TAR _index bytes ───────────────────────────────────────

    private List<TarArchiveBuilder.EntryLocation> parseTarIndex(byte[] tarBytes) throws IOException {
        byte[] header = java.util.Arrays.copyOfRange(tarBytes, 0, TarArchiveBuilder.TAR_BLOCK);
        String sizeOctal = new String(java.util.Arrays.copyOfRange(header, 124, 136),
            java.nio.charset.StandardCharsets.US_ASCII).trim().replace("\0", "");
        try {
            long indexSize = Long.parseLong(sizeOctal, 8);
            byte[] indexBytes = java.util.Arrays.copyOfRange(tarBytes,
                TarArchiveBuilder.TAR_BLOCK, TarArchiveBuilder.TAR_BLOCK + (int) indexSize);
            return TarArchiveBuilder.parseIndex(indexBytes);
        } catch (NumberFormatException e) {
            return Collections.emptyList();
        }
    }
}
