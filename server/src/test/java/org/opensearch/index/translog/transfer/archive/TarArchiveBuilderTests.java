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
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class TarArchiveBuilderTests extends OpenSearchTestCase {

    // ---- helpers ----

    private static TarArchiveBuilder.ArchiveBuildEntry entry(String path, byte[] content) {
        return TarArchiveBuilder.fromBytes(path, content);
    }

    private static byte[] extract(byte[] tarBytes, long dataOffset, long dataLength) {
        return Arrays.copyOfRange(tarBytes, (int) dataOffset, (int) (dataOffset + dataLength));
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new Random().nextBytes(bytes);
        return bytes;
    }

    // ---- tests ----

    public void testComputeLayoutSingleEntry() throws IOException {
        byte[] content = "hello translog".getBytes(StandardCharsets.UTF_8);
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(entry("shard/0/1/translog-1.tlog", content));

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);

        // total size must be a multiple of 512
        assertEquals(0, layout.getTotalSize() % TarArchiveBuilder.TAR_BLOCK);
        assertThat(layout.getTotalSize(), greaterThan(0L));
        assertThat(layout.getEntries(), hasSize(1));

        TarArchiveBuilder.EntryLocation loc = layout.getEntries().get(0);
        assertThat(loc.getDataLength(), equalTo((long) content.length));
        // data offset must be > 0 (after index entry header + content + padding)
        assertThat(loc.getDataOffset(), greaterThan(0L));
        // offset must be 512-aligned
        assertEquals(0, loc.getDataOffset() % TarArchiveBuilder.TAR_BLOCK);
    }

    public void testComputeLayoutMultipleEntries() throws IOException {
        byte[] c1 = "first content".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "second content longer".getBytes(StandardCharsets.UTF_8);
        byte[] c3 = new byte[1000];

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("uuid/0/1/translog-1.tlog", c1),
            entry("uuid/0/1/translog-1.ckp", c2),
            entry("uuid/0/1/translog-2.tlog", c3)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);

        assertThat(layout.getEntries(), hasSize(3));
        assertEquals(0, layout.getTotalSize() % TarArchiveBuilder.TAR_BLOCK);

        // Offsets must be strictly increasing and 512-aligned
        long prev = 0;
        for (TarArchiveBuilder.EntryLocation loc : layout.getEntries()) {
            assertThat(loc.getDataOffset(), greaterThan(prev));
            assertEquals(0, loc.getDataOffset() % TarArchiveBuilder.TAR_BLOCK);
            prev = loc.getDataOffset();
        }

        // Data lengths must match original sizes
        assertThat(layout.getEntries().get(0).getDataLength(), equalTo((long) c1.length));
        assertThat(layout.getEntries().get(1).getDataLength(), equalTo((long) c2.length));
        assertThat(layout.getEntries().get(2).getDataLength(), equalTo((long) c3.length));
    }

    public void testBuildAndParseIndex() throws IOException {
        byte[] c1 = "translog data here".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "checkpoint data".getBytes(StandardCharsets.UTF_8);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("uuid/0/1/translog-3.tlog", c1),
            entry("uuid/0/1/translog-3.ckp", c2)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);

        // Build the TAR
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Total size matches
        assertThat((long) tarBytes.length, equalTo(layout.getTotalSize()));

        // First 512 bytes = index header. Bytes 512..512+indexSize = index content.
        int indexSize = layout.getIndexBytes().length;
        byte[] indexBytes = Arrays.copyOfRange(tarBytes, TarArchiveBuilder.TAR_BLOCK, TarArchiveBuilder.TAR_BLOCK + indexSize);
        assertArrayEquals(layout.getIndexBytes(), indexBytes);

        // Parse index and verify paths and lengths
        List<TarArchiveBuilder.EntryLocation> parsed = TarArchiveBuilder.parseIndex(indexBytes);
        assertThat(parsed, hasSize(2));
        assertThat(parsed.get(0).getPath(), equalTo("uuid/0/1/translog-3.tlog"));
        assertThat(parsed.get(1).getPath(), equalTo("uuid/0/1/translog-3.ckp"));
        assertThat(parsed.get(0).getDataLength(), equalTo((long) c1.length));
        assertThat(parsed.get(1).getDataLength(), equalTo((long) c2.length));
    }

    public void testBuildAndExtractContent() throws IOException {
        byte[] c1 = "the actual translog bytes go here".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "checkpoint bytes".getBytes(StandardCharsets.UTF_8);

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("uuid/0/1/translog-5.tlog", c1),
            entry("uuid/0/1/translog-5.ckp", c2)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Extract using offsets from layout — simulating a byte-range GET
        byte[] extracted1 = extract(tarBytes, layout.getEntries().get(0).getDataOffset(), layout.getEntries().get(0).getDataLength());
        byte[] extracted2 = extract(tarBytes, layout.getEntries().get(1).getDataOffset(), layout.getEntries().get(1).getDataLength());

        assertArrayEquals(c1, extracted1);
        assertArrayEquals(c2, extracted2);
    }

    public void testLayoutIsDeterministic() throws IOException {
        byte[] c1 = new byte[100];
        byte[] c2 = new byte[200];
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("a/b/c.tlog", c1),
            entry("a/b/c.ckp", c2)
        );

        TarArchiveBuilder.TarLayout layout1 = TarArchiveBuilder.computeLayout(entries);
        TarArchiveBuilder.TarLayout layout2 = TarArchiveBuilder.computeLayout(entries);

        assertEquals(layout1.getTotalSize(), layout2.getTotalSize());
        assertArrayEquals(layout1.getIndexBytes(), layout2.getIndexBytes());
        for (int i = 0; i < layout1.getEntries().size(); i++) {
            assertEquals(layout1.getEntries().get(i).getDataOffset(), layout2.getEntries().get(i).getDataOffset());
            assertEquals(layout1.getEntries().get(i).getDataLength(), layout2.getEntries().get(i).getDataLength());
        }
    }

    public void testEmptyEntriesList() throws IOException {
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(List.of());
        // Should produce a valid TAR with just index + end-of-archive marker
        assertThat(layout.getTotalSize(), greaterThan(0L));
        assertEquals(0, layout.getTotalSize() % TarArchiveBuilder.TAR_BLOCK);
        assertThat(layout.getEntries(), hasSize(0));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, List.of());
        assertThat((long) out.toByteArray().length, equalTo(layout.getTotalSize()));
    }

    public void testLargeFile() throws IOException {
        byte[] largeContent = randomBytes(100 * 1024); // 100 KB

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = List.of(entry("uuid/0/1/translog-big.tlog", largeContent));
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        byte[] extracted = extract(tarBytes, layout.getEntries().get(0).getDataOffset(), layout.getEntries().get(0).getDataLength());
        assertArrayEquals(largeContent, extracted);
    }

    public void testTotalSizeMatchesActualOutput() throws IOException {
        // Randomised sizes to catch off-by-one padding errors
        byte[] c1 = randomBytes(randomIntBetween(1, 2000));
        byte[] c2 = randomBytes(randomIntBetween(1, 2000));
        byte[] c3 = randomBytes(randomIntBetween(1, 2000));

        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("s/0/1/t-1.tlog", c1),
            entry("s/0/1/t-1.ckp", c2),
            entry("s/0/1/t-2.tlog", c3)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);

        // Critical: predicted totalSize must EXACTLY match actual bytes written
        assertEquals(layout.getTotalSize(), (long) out.toByteArray().length);
    }

    public void testIndexOffsetsMatchActualBytePositions() throws IOException {
        // Verify parsed index offsets point to exact byte positions in the TAR
        byte[] c1 = randomBytes(randomIntBetween(50, 500));
        byte[] c2 = randomBytes(randomIntBetween(50, 500));
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            entry("x/0/1/t.tlog", c1),
            entry("x/0/1/t.ckp", c2)
        );

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.build(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Parse index from TAR bytes (as a reader would do)
        int indexSize = layout.getIndexBytes().length;
        byte[] indexBytes = Arrays.copyOfRange(tarBytes, TarArchiveBuilder.TAR_BLOCK, TarArchiveBuilder.TAR_BLOCK + indexSize);
        List<TarArchiveBuilder.EntryLocation> parsedLocations = TarArchiveBuilder.parseIndex(indexBytes);

        // Verify each entry can be extracted at the offset the index claims
        assertArrayEquals(c1, extract(tarBytes, parsedLocations.get(0).getDataOffset(), parsedLocations.get(0).getDataLength()));
        assertArrayEquals(c2, extract(tarBytes, parsedLocations.get(1).getDataOffset(), parsedLocations.get(1).getDataLength()));
    }
}
