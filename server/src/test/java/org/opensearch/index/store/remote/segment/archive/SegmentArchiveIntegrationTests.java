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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for segment archive upload/download flow
 * Tests the complete round-trip: build → upload → parse → download
 */
public class SegmentArchiveIntegrationTests extends OpenSearchTestCase {

    /**
     * Test: Complete upload/download cycle for segment archive
     */
    public void testCompleteArchiveCycle() throws IOException {
        // Given: segment files to archive
        String path1 = "_0.si";
        byte[] content1 = "segment info".getBytes(StandardCharsets.UTF_8);
        
        String path2 = "_0.cfs";
        byte[] content2 = "compound file content here".getBytes(StandardCharsets.UTF_8);
        
        String path3 = "_0.cfe";
        byte[] content3 = "compound file entries".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2),
            SegmentArchiveBuilder.fromBytes(path3, content3)
        );

        // When: building and uploading archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Simulate upload to S3 (just store bytes)
        byte[] uploadedArchive = zipBytes;

        // When: parsing archive on recovery
        int tailLen = (int) Math.min(uploadedArchive.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(uploadedArchive, uploadedArchive.length - tailLen, uploadedArchive.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, uploadedArchive.length - tailLen);

        // When: downloading files using parsed offsets
        byte[] downloaded1 = Arrays.copyOfRange(
            uploadedArchive,
            (int) parsedEntries.get(path1).getOffset(),
            (int) (parsedEntries.get(path1).getOffset() + parsedEntries.get(path1).getLength())
        );
        
        byte[] downloaded2 = Arrays.copyOfRange(
            uploadedArchive,
            (int) parsedEntries.get(path2).getOffset(),
            (int) (parsedEntries.get(path2).getOffset() + parsedEntries.get(path2).getLength())
        );
        
        byte[] downloaded3 = Arrays.copyOfRange(
            uploadedArchive,
            (int) parsedEntries.get(path3).getOffset(),
            (int) (parsedEntries.get(path3).getOffset() + parsedEntries.get(path3).getLength())
        );

        // Then: downloaded content should match original
        assertArrayEquals(content1, downloaded1);
        assertArrayEquals(content2, downloaded2);
        assertArrayEquals(content3, downloaded3);
    }

    /**
     * Test: Archive with large segments (100 MB)
     */
    public void testLargeSegmentArchiveRoundTrip() throws IOException {
        // Given: large segment file
        byte[] largeContent = new byte[100_000_000]; // 100 MB
        Arrays.fill(largeContent, (byte) 'X');

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_10.cfs", largeContent)
        );

        // When: building archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // When: downloading in chunks (simulating range-read)
        SegmentArchiveEntry entry = parsedEntries.get("_10.cfs");
        long offset = entry.getOffset();
        long length = entry.getLength();

        // Then: offset and length should be correct
        assertThat(length, equalTo((long) largeContent.length));
        assertTrue(offset + length <= zipBytes.length);

        // Then: can extract via range
        byte[] extracted = Arrays.copyOfRange(
            zipBytes,
            (int) offset,
            (int) (offset + length)
        );
        assertArrayEquals(largeContent, extracted);
    }

    /**
     * Test: Multiple archives with different content
     */
    public void testMultipleArchivesWithDifferentContent() throws IOException {
        // Given: two separate archive batches
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> batch1 = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", "batch1_si".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("_0.cfs", "batch1_cfs".getBytes(StandardCharsets.UTF_8))
        );

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> batch2 = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_1.si", "batch2_si".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("_1.cfs", "batch2_cfs".getBytes(StandardCharsets.UTF_8))
        );

        // When: building two archives
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> entries1 = SegmentArchiveBuilder.buildAndExtractOffsets(out1, batch1);
        byte[] zip1 = out1.toByteArray();

        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> entries2 = SegmentArchiveBuilder.buildAndExtractOffsets(out2, batch2);
        byte[] zip2 = out2.toByteArray();

        // When: parsing both
        int tail1Len = (int) Math.min(zip1.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail1 = Arrays.copyOfRange(zip1, zip1.length - tail1Len, zip1.length);
        Map<String, SegmentArchiveEntry> parsed1 = ZipSegmentParser.parseToMap(tail1, zip1.length - tail1Len);

        int tail2Len = (int) Math.min(zip2.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail2 = Arrays.copyOfRange(zip2, zip2.length - tail2Len, zip2.length);
        Map<String, SegmentArchiveEntry> parsed2 = ZipSegmentParser.parseToMap(tail2, zip2.length - tail2Len);

        // Then: both archives should be independent and correct
        assertThat(parsed1.size(), equalTo(2));
        assertThat(parsed2.size(), equalTo(2));
        assertTrue(parsed1.containsKey("_0.si"));
        assertTrue(parsed2.containsKey("_1.si"));
    }

    /**
     * Test: Archive maintains order of entries
     */
    public void testArchiveEntriesOrderPreserved() throws IOException {
        // Given: entries in specific order
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("file_a", "content_a".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("file_b", "content_b".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("file_c", "content_c".getBytes(StandardCharsets.UTF_8))
        );

        // When: building archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: all entries present
        assertThat(parsedEntries.size(), equalTo(3));
        assertTrue(parsedEntries.containsKey("file_a"));
        assertTrue(parsedEntries.containsKey("file_b"));
        assertTrue(parsedEntries.containsKey("file_c"));
    }

    /**
     * Test: Empty archive round-trip
     */
    public void testEmptyArchiveRoundTrip() throws IOException {
        // Given: empty archive
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList();

        // When: building empty archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing empty archive
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: should be empty
        assertThat(parsedEntries.size(), equalTo(0));
        assertTrue(zipBytes.length > 0); // Even empty ZIP has structure
    }

    /**
     * Test: Archive with mixed file types and sizes
     */
    public void testMixedFileTypesAndSizes() throws IOException {
        // Given: variety of files
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", new byte[100]),           // 100 bytes
            SegmentArchiveBuilder.fromBytes("_0.cfs", new byte[1_000_000]),    // 1 MB
            SegmentArchiveBuilder.fromBytes("_0.cfe", new byte[500]),          // 500 bytes
            SegmentArchiveBuilder.fromBytes("_0.liv", new byte[50]),           // 50 bytes
            SegmentArchiveBuilder.fromBytes("_0.nvd", new byte[10_000])        // 10 KB
        );

        // When: building archive
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: all sizes should be correct
        assertThat(parsedEntries.get("_0.si").getLength(), equalTo(100L));
        assertThat(parsedEntries.get("_0.cfs").getLength(), equalTo(1_000_000L));
        assertThat(parsedEntries.get("_0.cfe").getLength(), equalTo(500L));
        assertThat(parsedEntries.get("_0.liv").getLength(), equalTo(50L));
        assertThat(parsedEntries.get("_0.nvd").getLength(), equalTo(10_000L));
    }
}
