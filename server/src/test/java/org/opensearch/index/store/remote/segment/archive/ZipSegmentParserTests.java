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
 * TDD Tests for {@link ZipSegmentParser} - parse ZIP archives for range-read recovery
 */
public class ZipSegmentParserTests extends OpenSearchTestCase {

    /**
     * Test: Parse a built segment archive to extract offsets
     */
    public void testParseSegmentArchiveExtractsOffsets() throws IOException {
        // Given: a built segment archive with known entries
        String path1 = "_0.si";
        byte[] content1 = "segment info".getBytes(StandardCharsets.UTF_8);
        
        String path2 = "_0.cfs";
        byte[] content2 = "compound file segment".getBytes(StandardCharsets.UTF_8);
        
        String path3 = "_0.cfe";
        byte[] content3 = "compound file entries".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2),
            SegmentArchiveBuilder.fromBytes(path3, content3)
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> builtEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing the archive tail
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: should extract all entries with correct offsets
        assertThat(parsedEntries.size(), equalTo(3));
        
        SegmentArchiveEntry parsed1 = parsedEntries.get(path1);
        assertThat(parsed1, notNullValue());
        assertThat(parsed1.getFilename(), equalTo(path1));
        assertThat(parsed1.getLength(), equalTo((long) content1.length));
        
        SegmentArchiveEntry parsed2 = parsedEntries.get(path2);
        assertThat(parsed2, notNullValue());
        assertThat(parsed2.getFilename(), equalTo(path2));
        assertThat(parsed2.getLength(), equalTo((long) content2.length));
        
        SegmentArchiveEntry parsed3 = parsedEntries.get(path3);
        assertThat(parsed3, notNullValue());
        assertThat(parsed3.getFilename(), equalTo(path3));
        assertThat(parsed3.getLength(), equalTo((long) content3.length));
    }

    /**
     * Test: Extract data using parsed offsets (range-read simulation)
     */
    public void testRangeReadUsingParsedOffsets() throws IOException {
        // Given: a segment archive with known content
        String path = "_5.cfs";
        byte[] originalContent = "large compound file content here".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path, originalContent)
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing and extracting
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: should be able to extract exact range
        SegmentArchiveEntry entry = parsedEntries.get(path);
        assertThat(entry, notNullValue());
        
        long offset = entry.getOffset();
        long length = entry.getLength();
        
        // Extract using offset/length
        byte[] extractedContent = Arrays.copyOfRange(
            zipBytes,
            (int) offset,
            (int) (offset + length)
        );
        
        assertArrayEquals(originalContent, extractedContent);
    }

    /**
     * Test: Handle empty archive tail (error case)
     */
    public void testParseTailTooShortThrows() {
        // Given: a tail that's too short to contain EOCD
        byte[] shortTail = new byte[10];

        // When: trying to parse
        IOException e = expectThrows(IOException.class, () -> ZipSegmentParser.parse(shortTail, 0));

        // Then: should throw with descriptive message
        assertTrue(e.getMessage().contains("Tail too short") || e.getMessage().contains("EOCD"));
    }

    /**
     * Test: Parse archive with single large file
     */
    public void testParseLargeSegmentFile() throws IOException {
        // Given: a large segment file
        String path = "_10.cfs";
        byte[] largeContent = new byte[5_000_000]; // 5 MB
        Arrays.fill(largeContent, (byte) 42);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path, largeContent)
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: should handle large file correctly
        SegmentArchiveEntry entry = parsedEntries.get(path);
        assertThat(entry, notNullValue());
        assertThat(entry.getLength(), equalTo((long) largeContent.length));
        
        // Verify extraction works
        byte[] extracted = Arrays.copyOfRange(
            zipBytes,
            (int) entry.getOffset(),
            (int) (entry.getOffset() + entry.getLength())
        );
        assertArrayEquals(largeContent, extracted);
    }

    /**
     * Test: Parse returns list and map with same entries
     */
    public void testParseListAndMapConsistency() throws IOException {
        // Given: built archive
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", "info".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("_0.cfs", "data".getBytes(StandardCharsets.UTF_8))
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing as list and map
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        
        List<SegmentArchiveEntry> parsedList = ZipSegmentParser.parse(tail, zipBytes.length - tailLen);
        Map<String, SegmentArchiveEntry> parsedMap = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: both should have same entries
        assertThat(parsedList, hasSize(2));
        assertThat(parsedMap.size(), equalTo(2));
        
        for (SegmentArchiveEntry entry : parsedList) {
            assertThat(parsedMap.get(entry.getFilename()), notNullValue());
            assertThat(parsedMap.get(entry.getFilename()).getOffset(), equalTo(entry.getOffset()));
            assertThat(parsedMap.get(entry.getFilename()).getLength(), equalTo(entry.getLength()));
        }
    }

    /**
     * Test: Parse archive with mixed file sizes
     */
    public void testParseMixedFileSizes() throws IOException {
        // Given: archive with files of varying sizes
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", new byte[100]),      // 100 bytes
            SegmentArchiveBuilder.fromBytes("_0.cfs", new byte[100000]),  // 100 KB
            SegmentArchiveBuilder.fromBytes("_0.cfe", new byte[500]),     // 500 bytes
            SegmentArchiveBuilder.fromBytes("_0.liv", new byte[1000])     // 1 KB
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: parsing
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Then: all files should be parseable with correct sizes
        assertThat(parsedEntries.size(), equalTo(4));
        assertThat(parsedEntries.get("_0.si").getLength(), equalTo(100L));
        assertThat(parsedEntries.get("_0.cfs").getLength(), equalTo(100000L));
        assertThat(parsedEntries.get("_0.cfe").getLength(), equalTo(500L));
        assertThat(parsedEntries.get("_0.liv").getLength(), equalTo(1000L));
    }
}
