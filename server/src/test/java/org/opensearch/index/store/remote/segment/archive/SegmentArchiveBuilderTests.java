/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.hamcrest.Matchers.equalTo;

/**
 * TDD Tests for {@link SegmentArchiveBuilder}
 */
public class SegmentArchiveBuilderTests extends OpenSearchTestCase {

    /**
     * Test: Build a segment archive with stored (no compression) format
     */
    public void testBuildSegmentArchiveStored() throws IOException {
        // Given: segment files with content
        String segmentPath1 = "_0.si";
        byte[] content1 = "segment info content".getBytes(StandardCharsets.UTF_8);

        String segmentPath2 = "_0.cfs";
        byte[] content2 = "compound file content".getBytes(StandardCharsets.UTF_8);

        String segmentPath3 = "_0.cfe";
        byte[] content3 = "compound file entries".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(segmentPath1, content1),
            SegmentArchiveBuilder.fromBytes(segmentPath2, content2),
            SegmentArchiveBuilder.fromBytes(segmentPath3, content3)
        );

        // When: building the archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.build(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Then: archive should be readable as ZIP with stored entries
        // ZIP overhead varies but should be small for stored format
        int totalContentSize = content1.length + content2.length + content3.length;
        assertTrue("ZIP size should be close to content size", zipBytes.length >= totalContentSize);
        assertTrue("ZIP overhead should be reasonable", zipBytes.length < totalContentSize + 500);

        // Verify ZIP structure: can read entries back
        ZipInputStream zis = new ZipInputStream(new java.io.ByteArrayInputStream(zipBytes));
        Map<String, byte[]> extractedEntries = extractZipEntries(zis);

        assertThat(extractedEntries.size(), equalTo(3));
        assertArrayEquals(content1, extractedEntries.get(segmentPath1));
        assertArrayEquals(content2, extractedEntries.get(segmentPath2));
        assertArrayEquals(content3, extractedEntries.get(segmentPath3));
    }

    /**
     * Test: Parse archive and extract offsets for range-read
     */
    public void testBuildAndExtractOffsets() throws IOException {
        // Given: segment files
        String path1 = "_1.si";
        byte[] content1 = "segment info one".getBytes(StandardCharsets.UTF_8);

        String path2 = "_1.cfs";
        byte[] content2 = "compound file segment one".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2)
        );

        // When: building archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Then: should return archive entries with offsets
        assertThat(archiveEntries.size(), equalTo(2));

        SegmentArchiveEntry entry1 = archiveEntries.get(path1);
        assertThat(entry1.getFilename(), equalTo(path1));
        assertThat(entry1.getLength(), equalTo((long) content1.length));
        assertTrue(entry1.getOffset() >= 0);
        assertTrue(entry1.getChecksum() != 0);

        SegmentArchiveEntry entry2 = archiveEntries.get(path2);
        assertThat(entry2.getFilename(), equalTo(path2));
        assertThat(entry2.getLength(), equalTo((long) content2.length));
        assertTrue(entry2.getOffset() >= 0);
        assertTrue(entry2.getChecksum() != 0);

        // Verify range-read: extract data using offsets
        byte[] extracted1 = Arrays.copyOfRange(zipBytes, (int) entry1.getOffset(), (int) (entry1.getOffset() + entry1.getLength()));
        byte[] extracted2 = Arrays.copyOfRange(zipBytes, (int) entry2.getOffset(), (int) (entry2.getOffset() + entry2.getLength()));

        assertArrayEquals(content1, extracted1);
        assertArrayEquals(content2, extracted2);
    }

    /**
     * Test: Archive with large segment files
     */
    public void testBuildLargeSegment() throws IOException {
        // Given: a large segment file
        String path = "_5.cfs";
        byte[] largeContent = new byte[10_000_000]; // 10 MB
        Arrays.fill(largeContent, (byte) 42);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(SegmentArchiveBuilder.fromBytes(path, largeContent));

        // When: building archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Then: large file should be stored without compression
        SegmentArchiveEntry entry = archiveEntries.get(path);
        assertThat(entry.getLength(), equalTo((long) largeContent.length));

        // ZIP stored format: archive size ≈ data size + overhead (no compression savings)
        assertTrue(zipBytes.length < largeContent.length + 1000); // Small overhead
    }

    /**
     * Test: Empty archive (no entries)
     */
    public void testBuildEmptyArchive() throws IOException {
        // Given: no entries
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList();

        // When: building empty archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);

        // Then: should produce minimal ZIP
        assertThat(archiveEntries.size(), equalTo(0));
        assertTrue(out.toByteArray().length > 0); // Even empty ZIP has structure
    }

    /**
     * Test: Archive entry is built from byte array
     */
    public void testSegmentArchiveBuildEntryFromBytes() throws IOException {
        // Given: byte content
        String path = "test.bin";
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        // When: creating build entry
        SegmentArchiveBuilder.SegmentArchiveBuildEntry entry = SegmentArchiveBuilder.fromBytes(path, content);

        // Then: entry properties are correct
        assertThat(entry.getPath(), equalTo(path));
        assertThat(entry.getSize(), equalTo((long) content.length));

        // Can read content
        byte[] readContent = new byte[content.length];
        try (var in = entry.getContent()) {
            int total = 0;
            while (total < content.length) {
                int r = in.read(readContent, total, content.length - total);
                if (r <= 0) break;
                total += r;
            }
        }
        assertArrayEquals(content, readContent);
    }

    // Helper: Extract all entries from ZIP
    private Map<String, byte[]> extractZipEntries(ZipInputStream zis) throws IOException {
        Map<String, byte[]> entries = new java.util.HashMap<>();
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            byte[] content = new byte[(int) entry.getSize()];
            int total = 0;
            while (total < content.length) {
                int r = zis.read(content, total, content.length - total);
                if (r <= 0) break;
                total += r;
            }
            entries.put(entry.getName(), content);
            zis.closeEntry();
        }
        return entries;
    }
}
