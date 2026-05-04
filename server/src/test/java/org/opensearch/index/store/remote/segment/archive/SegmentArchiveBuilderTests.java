/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link SegmentArchiveBuilder} — TAR-based segment archive.
 */
public class SegmentArchiveBuilderTests extends OpenSearchTestCase {

    /**
     * Build a TAR archive from byte entries and verify the layout is correct.
     */
    public void testComputeLayoutAndBuild() throws IOException {
        String path1 = "_0.si";
        byte[] content1 = "segment info content".getBytes(StandardCharsets.UTF_8);
        String path2 = "_0.cfs";
        byte[] content2 = "compound file content".getBytes(StandardCharsets.UTF_8);
        String path3 = "_0.cfe";
        byte[] content3 = "compound file entries".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2),
            SegmentArchiveBuilder.fromBytes(path3, content3)
        );

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        assertThat("Layout total size must be positive", layout.getTotalSize(), greaterThan(0L));
        assertThat("Layout must have 3 entry locations", layout.getEntries().size(), equalTo(3));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        assertThat("TAR size must match layout total size", (long) tarBytes.length, equalTo(layout.getTotalSize()));
        assertThat("Must have 3 archive entries", archiveEntries.size(), equalTo(3));

        // Verify offsets are positive and lengths are correct
        for (Map.Entry<String, SegmentArchiveEntry> e : archiveEntries.entrySet()) {
            assertThat("Offset must be positive for " + e.getKey(), e.getValue().getOffset(), greaterThan(0L));
            assertThat("Length must match original for " + e.getKey(), e.getValue().getLength(), greaterThan(0L));
        }
        assertThat(archiveEntries.get(path1).getLength(), equalTo((long) content1.length));
        assertThat(archiveEntries.get(path2).getLength(), equalTo((long) content2.length));
        assertThat(archiveEntries.get(path3).getLength(), equalTo((long) content3.length));
    }

    /**
     * Range-read simulation: offsets from buildAndExtractOffsets allow byte-accurate extraction.
     */
    public void testOffsetsAllowRangeRead() throws IOException {
        String path1 = "_1.si";
        byte[] content1 = "segment info one".getBytes(StandardCharsets.UTF_8);
        String path2 = "_1.cfs";
        byte[] content2 = "compound file segment one with more data".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2)
        );

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Simulate range-read using the offsets
        SegmentArchiveEntry entry1 = archiveEntries.get(path1);
        assertThat(entry1, notNullValue());
        byte[] extracted1 = Arrays.copyOfRange(tarBytes, (int) entry1.getOffset(), (int) (entry1.getOffset() + entry1.getLength()));
        assertArrayEquals("Range-read must yield original content for " + path1, content1, extracted1);

        SegmentArchiveEntry entry2 = archiveEntries.get(path2);
        assertThat(entry2, notNullValue());
        byte[] extracted2 = Arrays.copyOfRange(tarBytes, (int) entry2.getOffset(), (int) (entry2.getOffset() + entry2.getLength()));
        assertArrayEquals("Range-read must yield original content for " + path2, content2, extracted2);
    }

    /**
     * Offsets from buildAndExtractOffsets match those parsed by TarSegmentParser.
     * This cross-validates the builder and parser are consistent.
     */
    public void testBuilderOffsetsMatchParserOffsets() throws IOException {
        Map<String, byte[]> files = new java.util.LinkedHashMap<>();
        files.put("_0.si", "segment info".getBytes(StandardCharsets.UTF_8));
        files.put("_0.cfs", new byte[100_000]);
        Arrays.fill(files.get("_0.cfs"), (byte) 0xBE);
        files.put("_0.cfe", "compound entries".getBytes(StandardCharsets.UTF_8));
        files.put("_1.si", new byte[5_000]);
        Arrays.fill(files.get("_1.si"), (byte) 0xEF);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = new java.util.ArrayList<>();
        for (Map.Entry<String, byte[]> f : files.entrySet()) {
            entries.add(SegmentArchiveBuilder.fromBytes(f.getKey(), f.getValue()));
        }

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> builderEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Parse via TarSegmentParser
        Map<String, SegmentArchiveEntry> parserEntries = TarSegmentParser.parseToMap(tarBytes);

        assertThat("Builder and parser must find same number of entries", builderEntries.size(), equalTo(parserEntries.size()));

        for (Map.Entry<String, SegmentArchiveEntry> be : builderEntries.entrySet()) {
            String name = be.getKey();
            SegmentArchiveEntry fromBuilder = be.getValue();
            SegmentArchiveEntry fromParser = parserEntries.get(name);
            assertNotNull("Parser must find entry: " + name, fromParser);

            assertThat("Offset mismatch for " + name, fromBuilder.getOffset(), equalTo(fromParser.getOffset()));
            assertThat("Length mismatch for " + name, fromBuilder.getLength(), equalTo(fromParser.getLength()));

            // Both offsets must extract the same correct content
            byte[] original = files.get(name);
            byte[] extractedViaBuilder = Arrays.copyOfRange(tarBytes,
                (int) fromBuilder.getOffset(), (int) (fromBuilder.getOffset() + fromBuilder.getLength()));
            byte[] extractedViaParser = Arrays.copyOfRange(tarBytes,
                (int) fromParser.getOffset(), (int) (fromParser.getOffset() + fromParser.getLength()));
            assertArrayEquals("Builder offset extraction mismatch for " + name, original, extractedViaBuilder);
            assertArrayEquals("Parser offset extraction mismatch for " + name, original, extractedViaParser);
        }
    }

    /**
     * fromDirectory: entry backed by a Lucene Directory file can be included in the archive.
     */
    public void testFromDirectoryEntry() throws IOException {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] fileContent = "hello from directory".getBytes(StandardCharsets.UTF_8);
        try (IndexOutput out2 = dir.createOutput("test.txt", IOContext.DEFAULT)) {
            out2.writeBytes(fileContent, fileContent.length);
        }

        SegmentArchiveBuilder.SegmentArchiveBuildEntry entry = SegmentArchiveBuilder.fromDirectory("test.txt", dir);
        assertThat("Path must match", entry.getPath(), equalTo("test.txt"));
        assertThat("Size must match file content length", entry.getSize(), equalTo((long) fileContent.length));
        assertNull("File-backed entry has no backing bytes", entry.getBackingBytes());

        // Build a TAR with this entry and verify range-read works
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = List.of(entry);
        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(baos, layout, entries);
        byte[] tarBytes = baos.toByteArray();

        SegmentArchiveEntry ae = archiveEntries.get("test.txt");
        assertThat(ae, notNullValue());
        byte[] extracted = Arrays.copyOfRange(tarBytes, (int) ae.getOffset(), (int) (ae.getOffset() + ae.getLength()));
        assertArrayEquals("Range-read must yield original content", fileContent, extracted);
    }

    /**
     * Single-file archive: verify the layout and range-read works for one file.
     */
    public void testSingleFileArchive() throws IOException {
        byte[] content = "only one file in the archive".getBytes(StandardCharsets.UTF_8);
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = List.of(
            SegmentArchiveBuilder.fromBytes("_0.si", content)
        );

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        assertThat("Single entry in archive", archiveEntries.size(), equalTo(1));
        SegmentArchiveEntry entry = archiveEntries.get("_0.si");
        assertThat(entry, notNullValue());
        byte[] extracted = Arrays.copyOfRange(tarBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
        assertArrayEquals(content, extracted);
    }
}
