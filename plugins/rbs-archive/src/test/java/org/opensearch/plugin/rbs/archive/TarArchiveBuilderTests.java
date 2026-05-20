/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.opensearch.plugin.rbs.archive.TarArchiveBuilder.TAR_BLOCK;

/**
 * Unit + roundtrip tests for {@link TarArchiveBuilder}.
 *
 * <p><b>Validation strategy — two independent parsers:</b>
 * <ol>
 *   <li><b>Own parser</b>: {@link TarSegmentParser} / {@link TarArchiveBuilder#parseIndex} —
 *       reads our binary index and resolves byte offsets. Validates that the builder and parser
 *       are consistent with each other (structural regression test).</li>
 *   <li><b>commons-compress {@code TarArchiveInputStream}</b> (test scope only) —
 *       an independent, widely-used TAR implementation. Any TAR that fails here is not a valid
 *       TAR regardless of what our own parser says. This catches bugs in the builder itself.</li>
 * </ol>
 */
public class TarArchiveBuilderTests extends OpenSearchTestCase {

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /** Builds a TAR archive into a byte array from the given entries. */
    private static byte[] buildToBytes(List<TarArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) layout.getTotalSize());
        TarArchiveBuilder.build(baos, layout, entries);
        return baos.toByteArray();
    }

    private static long pad512(long n) {
        return n == 0 ? 0 : ((n + TAR_BLOCK - 1) / TAR_BLOCK) * TAR_BLOCK;
    }

    /**
     * Parses the TAR bytes with commons-compress and returns a map of
     * entry name → entry content (byte array).
     * This is the independent ground-truth validator.
     */
    private static Map<String, byte[]> parseTarWithCommonsCompress(byte[] tar) throws IOException {
        Map<String, byte[]> result = new HashMap<>();
        try (TarArchiveInputStream tais = new TarArchiveInputStream(new ByteArrayInputStream(tar))) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                if (!tais.canReadEntryData(entry)) continue;
                byte[] data = tais.readAllBytes();
                result.put(entry.getName(), data);
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Independent TAR validity via commons-compress
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Verifies that the TAR produced by {@link TarArchiveBuilder} is a valid standard TAR
     * as recognized by commons-compress, which is the gold-standard independent parser.
     */
    public void testProducedTarIsValidStandardTar() throws IOException {
        byte[] content1 = "hello translog".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = new byte[1024];
        new Random(42).nextBytes(content2);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.tlog", content1),
            TarArchiveBuilder.fromBytes("uuid/0/1/translog-1.ckp", content2)
        );
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);

        // _index entry is always present
        assertTrue("commons-compress must see _index entry", parsed.containsKey("_index"));
        // data entries
        assertTrue("commons-compress must see translog-1.tlog", parsed.containsKey("uuid/0/1/translog-1.tlog"));
        assertTrue("commons-compress must see translog-1.ckp", parsed.containsKey("uuid/0/1/translog-1.ckp"));

        // Content must be byte-identical
        assertArrayEquals("tlog content must be byte-identical", content1, parsed.get("uuid/0/1/translog-1.tlog"));
        assertArrayEquals("ckp content must be byte-identical", content2, parsed.get("uuid/0/1/translog-1.ckp"));
    }

    public void testEmptyArchiveIsValidTar() throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Collections.emptyList();
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);
        // Only _index should appear
        assertTrue("_index present", parsed.containsKey("_index"));
        assertEquals("Only _index in empty archive", 1, parsed.size());
    }

    public void testSingleByteEntryIsValidTar() throws IOException {
        byte[] content = { 0x42 };
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes("tiny.bin", content));
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);
        assertTrue(parsed.containsKey("tiny.bin"));
        assertArrayEquals(content, parsed.get("tiny.bin"));
    }

    public void testExactly512ByteEntryIsValidTar() throws IOException {
        byte[] content = new byte[512];
        Arrays.fill(content, (byte) 'Z');
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes("exact512.bin", content));
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);
        assertTrue(parsed.containsKey("exact512.bin"));
        assertArrayEquals(content, parsed.get("exact512.bin"));
    }

    public void testLargeEntryIsValidTar() throws IOException {
        byte[] content = new byte[128 * 1024]; // 128 KB
        new Random(7).nextBytes(content);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes("large.bin", content));
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);
        assertTrue(parsed.containsKey("large.bin"));
        assertArrayEquals(content, parsed.get("large.bin"));
    }

    public void testManyEntriesAreValidTar() throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        Map<String, byte[]> expectedContents = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            byte[] data = ("content of entry " + i).getBytes(StandardCharsets.UTF_8);
            String path = "index-" + i + "/shard-0/1/translog-" + i + ".tlog";
            entries.add(TarArchiveBuilder.fromBytes(path, data));
            expectedContents.put(path, data);
        }

        byte[] tar = buildToBytes(entries);
        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);

        for (Map.Entry<String, byte[]> e : expectedContents.entrySet()) {
            assertTrue("Entry " + e.getKey() + " present", parsed.containsKey(e.getKey()));
            assertArrayEquals("Entry " + e.getKey() + " content matches", e.getValue(), parsed.get(e.getKey()));
        }
    }

    public void testFromPathEntryIsValidTar() throws IOException {
        byte[] data = "translog data from local file, with some extra content here".getBytes(StandardCharsets.UTF_8);
        java.nio.file.Path tmpFile = createTempDir().resolve("translog-5.tlog");
        java.nio.file.Files.write(tmpFile, data);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromPath("uuid/0/1/translog-5.tlog", tmpFile, data.length)
        );
        byte[] tar = buildToBytes(entries);

        Map<String, byte[]> parsed = parseTarWithCommonsCompress(tar);
        assertTrue(parsed.containsKey("uuid/0/1/translog-5.tlog"));
        assertArrayEquals("fromPath content is byte-identical in TAR", data, parsed.get("uuid/0/1/translog-5.tlog"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Index roundtrip (computeLayout → parseIndex) — own parser validation
    // ─────────────────────────────────────────────────────────────────────────

    public void testIndexRoundtripSingleEntry() throws IOException {
        byte[] content = "test data".getBytes(StandardCharsets.UTF_8);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes("myfile.txt", content));

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        List<TarArchiveBuilder.EntryLocation> parsed = TarArchiveBuilder.parseIndex(layout.getIndexBytes());

        assertEquals("One entry in parsed index", 1, parsed.size());
        assertEquals("Entry path", "myfile.txt", parsed.get(0).getPath());
        assertEquals("Entry length", content.length, parsed.get(0).getDataLength());
        assertTrue("Data offset must be > 512", parsed.get(0).getDataOffset() > TAR_BLOCK);
    }

    public void testIndexRoundtripMultipleEntries() throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        List<byte[]> contents = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            byte[] data = ("content of file " + i).getBytes(StandardCharsets.UTF_8);
            contents.add(data);
            entries.add(TarArchiveBuilder.fromBytes("dir/file-" + i + ".dat", data));
        }

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        List<TarArchiveBuilder.EntryLocation> parsed = TarArchiveBuilder.parseIndex(layout.getIndexBytes());

        assertEquals("All 5 entries in index", 5, parsed.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("dir/file-" + i + ".dat", parsed.get(i).getPath());
            assertEquals(contents.get(i).length, parsed.get(i).getDataLength());
        }
    }

    /**
     * This is the critical cross-check: the byte offsets in our {@code _index} must point
     * to the actual data bytes in the produced TAR. Any offset arithmetic bug fails here.
     */
    public void testIndexOffsetsMatchActualDataPosition() throws IOException {
        byte[] data1 = "AAAA".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "BBBBBBBB".getBytes(StandardCharsets.UTF_8);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("a.txt", data1),
            TarArchiveBuilder.fromBytes("b.txt", data2)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        byte[] tar = buildToBytes(entries);

        List<TarArchiveBuilder.EntryLocation> locs = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(2, locs.size());

        TarArchiveBuilder.EntryLocation loc1 = locs.get(0);
        assertEquals("a.txt", loc1.getPath());
        byte[] readBack1 = Arrays.copyOfRange(tar, (int) loc1.getDataOffset(), (int) (loc1.getDataOffset() + loc1.getDataLength()));
        assertArrayEquals("a.txt content at reported offset", data1, readBack1);

        TarArchiveBuilder.EntryLocation loc2 = locs.get(1);
        assertEquals("b.txt", loc2.getPath());
        byte[] readBack2 = Arrays.copyOfRange(tar, (int) loc2.getDataOffset(), (int) (loc2.getDataOffset() + loc2.getDataLength()));
        assertArrayEquals("b.txt content at reported offset", data2, readBack2);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: TarSegmentParser roundtrip (cross-validates index offsets)
    // ─────────────────────────────────────────────────────────────────────────

    public void testTarSegmentParserRoundtrip() throws IOException {
        byte[] seg1 = "segment file content 1".getBytes(StandardCharsets.UTF_8);
        byte[] seg2 = new byte[256];
        Arrays.fill(seg2, (byte) 0x42);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("_0.cfe", seg1),
            TarArchiveBuilder.fromBytes("_0.cfs", seg2)
        );

        byte[] tar = buildToBytes(entries);

        // TarSegmentParser reads: first 512 header → determine index size → read full head
        byte[] firstHeader = Arrays.copyOfRange(tar, 0, TAR_BLOCK);
        int headReadLen = TarSegmentParser.computeHeadReadLength(firstHeader);
        assertTrue("Head read length > TAR_BLOCK", headReadLen > TAR_BLOCK);
        assertTrue("Head read length <= tar.length", headReadLen <= tar.length);

        byte[] head = Arrays.copyOfRange(tar, 0, headReadLen);
        Map<String, SegmentArchiveEntry> segMap = TarSegmentParser.parseToMap(head);

        assertEquals("Two entries parsed", 2, segMap.size());
        assertTrue("_0.cfe in map", segMap.containsKey("_0.cfe"));
        assertTrue("_0.cfs in map", segMap.containsKey("_0.cfs"));

        // Validate actual content at reported offsets
        SegmentArchiveEntry entry1 = segMap.get("_0.cfe");
        byte[] actual1 = Arrays.copyOfRange(tar, (int) entry1.getOffset(), (int) (entry1.getOffset() + entry1.getLength()));
        assertArrayEquals("_0.cfe content matches", seg1, actual1);

        SegmentArchiveEntry entry2 = segMap.get("_0.cfs");
        byte[] actual2 = Arrays.copyOfRange(tar, (int) entry2.getOffset(), (int) (entry2.getOffset() + entry2.getLength()));
        assertArrayEquals("_0.cfs content matches", seg2, actual2);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: GC prefix roundtrip
    // ─────────────────────────────────────────────────────────────────────────

    public void testGcPrefixRoundtrip() throws IOException {
        String indexUUID = java.util.UUID.randomUUID().toString();
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
            new TarArchiveBuilder.GcShardEntry(indexUUID, 0, 0L, 100L, 95L),
            new TarArchiveBuilder.GcShardEntry(indexUUID, 1, 50L, 200L, 190L)
        );
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(
            TarArchiveBuilder.fromBytes("some/file.tlog", "data".getBytes(StandardCharsets.UTF_8))
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries, gcEntries);

        // Validate the TAR is still valid
        byte[] tar;
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) layout.getTotalSize());
            TarArchiveBuilder.build(baos, layout, entries);
            tar = baos.toByteArray();
        }
        Map<String, byte[]> parsedTar = parseTarWithCommonsCompress(tar);
        assertTrue("_index present even with GC prefix", parsedTar.containsKey("_index"));
        assertTrue("data entry present", parsedTar.containsKey("some/file.tlog"));

        // Validate GC data roundtrip
        List<TarArchiveBuilder.GcShardEntry> parsed = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());
        assertEquals("Two GC entries parsed", 2, parsed.size());

        assertEquals(indexUUID, parsed.get(0).getIndexUUID());
        assertEquals(0, parsed.get(0).getShardId());
        assertEquals(0L, parsed.get(0).getMinSeqNo());
        assertEquals(100L, parsed.get(0).getMaxSeqNo());
        assertEquals(95L, parsed.get(0).getGlobalCheckpoint());

        assertEquals(indexUUID, parsed.get(1).getIndexUUID());
        assertEquals(1, parsed.get(1).getShardId());
        assertEquals(50L, parsed.get(1).getMinSeqNo());
        assertEquals(200L, parsed.get(1).getMaxSeqNo());
        assertEquals(190L, parsed.get(1).getGlobalCheckpoint());
    }

    public void testGcPrefixWithMultipleIndices() throws IOException {
        String idx1 = java.util.UUID.randomUUID().toString();
        String idx2 = java.util.UUID.randomUUID().toString();
        List<TarArchiveBuilder.GcShardEntry> gcEntries = List.of(
            new TarArchiveBuilder.GcShardEntry(idx1, 0, 0L, 10L, 8L),
            new TarArchiveBuilder.GcShardEntry(idx2, 0, 100L, 200L, 190L),
            new TarArchiveBuilder.GcShardEntry(idx2, 1, 100L, 200L, 190L)
        );
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(
            List.of(TarArchiveBuilder.fromBytes("f.tlog", new byte[] { 1, 2, 3 })),
            gcEntries
        );
        List<TarArchiveBuilder.GcShardEntry> parsed = TarArchiveBuilder.parseGcSummary(layout.getIndexBytes());
        assertEquals(3, parsed.size());

        assertEquals(idx1, parsed.get(0).getIndexUUID());
        assertEquals(0, parsed.get(0).getShardId());

        assertEquals(idx2, parsed.get(1).getIndexUUID());
        assertEquals(idx2, parsed.get(2).getIndexUUID());
        assertEquals(0, parsed.get(1).getShardId());
        assertEquals(1, parsed.get(2).getShardId());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Padding and boundary conditions
    // ─────────────────────────────────────────────────────────────────────────

    public void testEntryOf1ByteIsPaddedTo512() throws IOException {
        byte[] content = new byte[] { 42 };
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes("tiny.bin", content));
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        byte[] tar = buildToBytes(entries);

        assertEquals(layout.getTotalSize(), tar.length);

        List<TarArchiveBuilder.EntryLocation> locs = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(1, locs.size());
        assertEquals(1L, locs.get(0).getDataLength());

        // The byte at the data offset must be 42; the rest of the 512-byte data block must be 0
        int dataOffset = (int) locs.get(0).getDataOffset();
        assertEquals(42, tar[dataOffset] & 0xFF);
        for (int i = dataOffset + 1; i < dataOffset + TAR_BLOCK; i++) {
            assertEquals("Padding byte " + i + " must be 0", 0, tar[i]);
        }

        // commons-compress must also see exactly 1 byte
        Map<String, byte[]> cc = parseTarWithCommonsCompress(tar);
        assertArrayEquals(content, cc.get("tiny.bin"));
    }

    public void testTotalSizeMatchesActualByteCount() throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(TarArchiveBuilder.fromBytes("file-" + i + ".bin", ("data " + i).getBytes(StandardCharsets.UTF_8)));
        }
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        byte[] tar = buildToBytes(entries);

        assertEquals("Layout totalSize must match actual byte count", layout.getTotalSize(), tar.length);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests: Path validation
    // ─────────────────────────────────────────────────────────────────────────

    public void testPathTooLongThrows() {
        // TAR name field is 100 bytes with the last byte reserved for '\0', so effective max is 99 bytes.
        // Both 100-char and 101-char paths must be rejected.
        String path100 = "a".repeat(100);
        expectThrows(
            IOException.class,
            () -> TarArchiveBuilder.computeLayout(List.of(TarArchiveBuilder.fromBytes(path100, new byte[] { 1 })))
        );

        String path101 = "a".repeat(101);
        expectThrows(
            IOException.class,
            () -> TarArchiveBuilder.computeLayout(List.of(TarArchiveBuilder.fromBytes(path101, new byte[] { 1 })))
        );
    }

    public void testPathAtMaxLengthIsAccepted() throws IOException {
        // 99-char path is the maximum that fits with a null terminator in the 100-byte TAR name field.
        String path99 = "a".repeat(99);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(TarArchiveBuilder.fromBytes(path99, new byte[] { 2 }));
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        assertNotNull(layout);
        List<TarArchiveBuilder.EntryLocation> locs = TarArchiveBuilder.parseIndex(layout.getIndexBytes());
        assertEquals(1, locs.size());
        assertEquals(path99, locs.get(0).getPath());

        // Also valid per commons-compress (independent ground-truth)
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) layout.getTotalSize());
        TarArchiveBuilder.build(baos, layout, entries);
        Map<String, byte[]> cc = parseTarWithCommonsCompress(baos.toByteArray());
        assertTrue("99-char path must be present in commons-compress parsed TAR", cc.containsKey(path99));
    }
}
