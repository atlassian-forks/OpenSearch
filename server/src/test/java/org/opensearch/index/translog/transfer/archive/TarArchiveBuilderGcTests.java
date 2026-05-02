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
            new TarArchiveBuilder.GcShardEntry(3, 10L, 20L, 0xCAFE, 100L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/3/1/translog-10.tlog", new byte[8]),
            TarArchiveBuilder.fromBytes("uuid/3/1/translog-10.ckp", new byte[4])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());

        assertEquals(1, gcOut.size());
        assertEquals(3, gcOut.get(0).getShardId());
        assertEquals(10L, gcOut.get(0).getMinSeqNo());
        assertEquals(20L, gcOut.get(0).getMaxSeqNo());
        assertEquals(0xCAFE, gcOut.get(0).getUuidHash());
    }

    public void testGcSummaryRoundtripMultipleShards() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(0, 1L, 5L, 0x1111, 100L),
            new TarArchiveBuilder.GcShardEntry(1, 10L, 15L, 0x2222, 100L),
            new TarArchiveBuilder.GcShardEntry(2, 100L, 200L, 0x3333, 100L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", new byte[1])
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcIn);
        List<TarArchiveBuilder.GcShardEntry> gcOut = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());

        assertEquals(3, gcOut.size());
        assertEquals(0, gcOut.get(0).getShardId());
        assertEquals(5L, gcOut.get(0).getMaxSeqNo());
        assertEquals(1, gcOut.get(1).getShardId());
        assertEquals(15L, gcOut.get(1).getMaxSeqNo());
        assertEquals(2, gcOut.get(2).getShardId());
        assertEquals(200L, gcOut.get(2).getMaxSeqNo());
    }

    // ── parseIndex still works correctly after GC prefix ─────────────────────

    public void testParseIndexSkipsGcPrefixCorrectly() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
            new TarArchiveBuilder.GcShardEntry(5, 3L, 7L, 0xBEEF, 100L)
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

    // ── Full TAR build + extract with GC prefix ───────────────────────────────

    public void testBuildAndExtractWithGcPrefix() throws IOException {
        List<TarArchiveBuilder.GcShardEntry> gcIn = List.of(
            new TarArchiveBuilder.GcShardEntry(0, 1L, 3L, 0xABCD, 100L)
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
        // parseIndex should still work (numShardsGC = 0, skips 0 bytes, then reads entries)
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
        // Says 1 shard but only has 1 byte after header → truncated
        byte[] bad = new byte[]{0x00, 0x01, 0x00}; // numShards=1, then only 1 byte
        assertTrue(TarArchiveBuilder.parseGcSummary(bad).isEmpty());
    }
}
