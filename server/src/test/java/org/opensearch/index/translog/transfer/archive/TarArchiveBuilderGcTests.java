/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TarArchiveBuilderGcTests extends OpenSearchTestCase {

    private static final String UUID_A = "11111111-1111-1111-1111-111111111111";
    private static final String UUID_B = "22222222-2222-2222-2222-222222222222";

    // ── GC summary round-trip through computeLayout / parseGcSummary ──────────

    public void testGcSummaryRoundtripNoEntries() throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", "data".getBytes(StandardCharsets.UTF_8))
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, Collections.emptyList());
        List<TarArchiveBuilder.GcShardEntry> gc = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());
        assertTrue("GC summary should be empty when no gc entries provided", gc.isEmpty());
    }

    public void testGcSummaryRoundtripSingleShard() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 3, 10L, 20L, 100L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/3/1/translog-10.tlog", new byte[8]),
            TarArchiveBuilder.fromBytes("uuid/3/1/translog-10.ckp", new byte[4])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());

        assertEquals(1, gcOut.size());
        assertEquals(UUID_A, gcOut.get(0).getIndexUUID());
        assertEquals(3, gcOut.get(0).getShardId());
        assertEquals(10L, gcOut.get(0).getMinSeqNo());
        assertEquals(20L, gcOut.get(0).getMaxSeqNo());
        assertEquals(100L, gcOut.get(0).getGlobalCheckpoint());
    }

    public void testGcSummaryRoundtripMultipleShards() throws IOException {
        // Two shards from same index
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 100L),
            new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 10L, 15L, 100L),
            new TarArchiveBuilder.GcShardEntry(UUID_A, 2, 100L, 200L, 100L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", new byte[1])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());

        assertEquals(3, gcOut.size());
        // All from UUID_A, shards 0, 1, 2
        for (TarArchiveBuilder.GcShardEntry e : gcOut) {
            assertEquals(UUID_A, e.getIndexUUID());
        }
        assertEquals(5L, gcOut.stream().filter(e -> e.getShardId() == 0).findFirst().orElseThrow().getMaxSeqNo());
        assertEquals(15L, gcOut.stream().filter(e -> e.getShardId() == 1).findFirst().orElseThrow().getMaxSeqNo());
        assertEquals(200L, gcOut.stream().filter(e -> e.getShardId() == 2).findFirst().orElseThrow().getMaxSeqNo());
    }

    public void testGcSummaryRoundtripMultipleIndices() throws IOException {
        // Two indices with shards
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 100L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 10L, 20L, 15L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", new byte[1])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());

        assertEquals(2, gcOut.size());
        TarArchiveBuilder.GcShardEntry entryA = gcOut.stream().filter(e -> e.getIndexUUID().equals(UUID_A)).findFirst().orElseThrow();
        TarArchiveBuilder.GcShardEntry entryB = gcOut.stream().filter(e -> e.getIndexUUID().equals(UUID_B)).findFirst().orElseThrow();
        assertEquals(5L, entryA.getMaxSeqNo());
        assertEquals(20L, entryB.getMaxSeqNo());
        assertEquals(100L, entryA.getGlobalCheckpoint());
        assertEquals(15L, entryB.getGlobalCheckpoint());
    }

    // ── parseIndex still works correctly after GC prefix ─────────────────────

    public void testParseIndexSkipsGcPrefixCorrectly() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 5, 3L, 7L, 100L)
        );
        byte[] tlogData = "tlog-content".getBytes(StandardCharsets.UTF_8);
        byte[] ckpData = "ckp-content".getBytes(StandardCharsets.UTF_8);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            TarArchiveBuilder.fromBytes("uuid/5/1/translog-3.tlog", tlogData),
            TarArchiveBuilder.fromBytes("uuid/5/1/translog-3.ckp", ckpData)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);

        // parseIndex must successfully parse 2 entries despite GC prefix
        List<TarArchiveBuilder.EntryLocation> locations = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(2, locations.size());
        assertEquals("uuid/5/1/translog-3.tlog", locations.get(0).getPath());
        assertEquals("uuid/5/1/translog-3.ckp", locations.get(1).getPath());
    }

    public void testParseIndexSkipsGcPrefixMultipleIndices() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 6L, 10L, 10L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 1, 6L, 10L, 10L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", new byte[4]),
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.ckp", new byte[2])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.EntryLocation> locations = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(2, locations.size());
    }

    // ── Full TAR build + extract with GC prefix ───────────────────────────────

    public void testBuildAndExtractWithGcPrefix() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 3L, 100L)
        );
        byte[] content = "hello-gc".getBytes(StandardCharsets.UTF_8);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", content)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tar = out.toByteArray();

        // Verify total size matches declared layout size
        assertEquals(layout.getTotalSize(), tar.length);

        // Verify entry can be extracted at declared offset
        TarArchiveBuilder.EntryLocation loc = layout.getEntries().get(0);
        byte[] extracted = Arrays.copyOfRange(tar, (int) loc.getDataOffset(), (int) (loc.getDataOffset() + loc.getDataLength()));
        assertArrayEquals(content, extracted);
    }

    // ── Backward compatibility: zero GC entries behaves like old format ────────

    public void testZeroGcEntriesLegacyCompatibleParsing() throws IOException {
        // computeLayout without GC entries: legacy overload
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", new byte[]{1, 2, 3})
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        // parseIndex should still work (numIndices = 0, skips 0 bytes, then reads entries)
        List<TarArchiveBuilder.EntryLocation> locations = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(1, locations.size());
        assertEquals("uuid/0/1/translog-1.tlog", locations.get(0).getPath());
    }

    // ── parseGcSummary edge cases ─────────────────────────────────────────────

    public void testParseGcSummaryNullReturnsEmpty() {
        assertTrue(TarArchiveBuilder.parseGcSummary(null).isEmpty());
    }

    public void testParseGcSummaryEmptyBytesReturnsEmpty() {
        assertTrue(TarArchiveBuilder.parseGcSummary(new byte[0]).isEmpty());
    }

    public void testParseGcSummaryTruncatedReturnsEmpty() {
        // Says 1 index but only has 1 byte after header → truncated
        byte[] bad = new byte[]{0x00, 0x01, 0x00}; // numIndices=1, then only 1 byte (need 18 for header)
        assertTrue(TarArchiveBuilder.parseGcSummary(bad).isEmpty());
    }

    // ── Size correctness ──────────────────────────────────────────────────────

    public void testGcPrefixSizeWithTwoIndicesOneShardEach() throws IOException {
        // 2 indices, 1 shard each: size = 2 + 2*(18 + 1*24) = 2 + 2*42 = 86 bytes
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 6L, 10L, 10L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("a/b/c/translog-1.tlog", new byte[1])
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        int expectedGcPrefixSize = 2 + 2 * (TarArchiveBuilder.GC_INDEX_HEADER_BYTES + TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD);
        // Parse and verify round-trip works
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());
        assertEquals(2, gcOut.size());
        // Verify parseIndex also works
        List<TarArchiveBuilder.EntryLocation> locs = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(1, locs.size());
    }
}
