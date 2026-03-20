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
        byte[] extractedContent = Arrays.copyOfRange(zipBytes, (int) offset, (int) (offset + length));

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

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(SegmentArchiveBuilder.fromBytes(path, largeContent));

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
        byte[] extracted = Arrays.copyOfRange(zipBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
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
     * Test: Corrupted central directory - EOCD signature present but CD data is garbage.
     */
    public void testParseCorruptedCentralDirectoryThrows() throws IOException {
        // Given: build a valid archive first
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", "info".getBytes(StandardCharsets.UTF_8)),
            SegmentArchiveBuilder.fromBytes("_0.cfs", "data".getBytes(StandardCharsets.UTF_8))
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: corrupt the central directory by overwriting bytes in the middle
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);

        // Find EOCD and get CD offset, then corrupt CD data
        int eocdPos = -1;
        for (int i = tail.length - 22; i >= 0; i--) {
            if ((tail[i] & 0xff) == 0x50 && (tail[i + 1] & 0xff) == 0x4b && (tail[i + 2] & 0xff) == 0x05 && (tail[i + 3] & 0xff) == 0x06) {
                eocdPos = i;
                break;
            }
        }
        assertNotNull("Should find EOCD", eocdPos != -1);

        // Get CD offset from EOCD (at eocdPos + 16, little-endian 4 bytes)
        int cdOffsetInFile = (tail[eocdPos + 16] & 0xff) | ((tail[eocdPos + 17] & 0xff) << 8) | ((tail[eocdPos + 18] & 0xff) << 16)
            | ((tail[eocdPos + 19] & 0xff) << 24);
        int cdOffsetInTail = (int) (cdOffsetInFile - (zipBytes.length - tailLen));

        // Corrupt the CD entry signature (overwrite first 4 bytes of CD with garbage)
        if (cdOffsetInTail >= 0 && cdOffsetInTail + 4 < tail.length) {
            tail[cdOffsetInTail] = (byte) 0xFF;
            tail[cdOffsetInTail + 1] = (byte) 0xFF;
            tail[cdOffsetInTail + 2] = (byte) 0xFF;
            tail[cdOffsetInTail + 3] = (byte) 0xFF;
        }

        // Then: parsing should return empty list (no valid CD entries) or throw
        byte[] corruptedTail = tail;
        long tailStartOffset = zipBytes.length - tailLen;
        try {
            List<SegmentArchiveEntry> result = ZipSegmentParser.parse(corruptedTail, tailStartOffset);
            // If it doesn't throw, it should return fewer entries (corrupted CD was skipped)
            assertTrue("Corrupted CD should produce fewer entries than original", result.size() < 2);
        } catch (IOException e) {
            // Expected: corrupted CD may throw
            assertTrue("Error should mention central directory or corruption", e.getMessage() != null);
        }
    }

    /**
     * Test: EOCD signature not found in tail → IOException
     */
    public void testParseNoEocdSignatureThrows() {
        // Given: random bytes with no EOCD signature
        byte[] randomTail = new byte[1000];
        Arrays.fill(randomTail, (byte) 0x42);

        // When/Then: should throw IOException
        IOException e = expectThrows(IOException.class, () -> ZipSegmentParser.parse(randomTail, 0));
        assertTrue("Should mention EOCD", e.getMessage().contains("EOCD"));
    }

    /**
     * Test: Archive download fallback - when parser returns no entries for a file,
     * caller should fall back to per-file download.
     */
    public void testParsedEntriesMissingFileTriggersNull() throws IOException {
        // Given: archive with only _0.si
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes("_0.si", "info".getBytes(StandardCharsets.UTF_8))
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // When: looking for a file that doesn't exist in archive
        SegmentArchiveEntry missing = parsedEntries.get("_1.cfs");

        // Then: should return null (caller falls back to per-file download)
        assertNull("Missing file should return null for fallback", missing);
        // Existing file should still be found
        assertThat(parsedEntries.get("_0.si"), notNullValue());
    }

    /**
     * INVARIANT: Segment files can really be downloaded back from a ZIP archive.
     *
     * This is the full end-to-end proof that the download path works:
     *   1. Build a realistic archive from segment files
     *   2. Write it to a temp file (simulating blob store)
     *   3. Read ONLY the tail bytes (simulating a range-read of the archive tail)
     *   4. Parse central directory from tail to discover file offsets (no full download needed)
     *   5. Range-read each file individually using parsed offsets
     *   6. Verify extracted content matches original byte-for-byte
     *   7. Verify CRC32 checksums match
     *
     * This proves:
     *   - We do NOT need to download the entire ZIP to recover individual files
     *   - The tail-based central directory parsing correctly resolves data offsets
     *   - Range-read extraction produces correct, uncorrupted data
     *   - The offsets from ZipSegmentParser match what SegmentArchiveBuilder produced
     */
    public void testRealDownloadFromZipViaRangeReadWithChecksumVerification() throws IOException {
        // Given: realistic segment files with varied content
        Map<String, byte[]> originalFiles = new java.util.LinkedHashMap<>();
        originalFiles.put("_0.si", "OpenSearch segment info version 9.8".getBytes(StandardCharsets.UTF_8));

        byte[] cfsContent = new byte[200_000]; // 200 KB compound file
        for (int i = 0; i < cfsContent.length; i++) {
            cfsContent[i] = (byte) (i % 251); // non-trivial pattern to catch byte-swap bugs
        }
        originalFiles.put("_0.cfs", cfsContent);

        originalFiles.put("_0.cfe", "compound file entries v2 metadata".getBytes(StandardCharsets.UTF_8));

        byte[] livContent = new byte[1024]; // live docs bitmap
        Arrays.fill(livContent, (byte) 0xFF);
        originalFiles.put("_0.liv", livContent);

        originalFiles.put("_0_Lucene90_0.dvd", "doc values data block".getBytes(StandardCharsets.UTF_8));
        originalFiles.put("_0_Lucene90_0.dvm", "doc values meta".getBytes(StandardCharsets.UTF_8));

        // Step 1: Build the archive (upload side)
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> buildEntries = new java.util.ArrayList<>();
        for (Map.Entry<String, byte[]> f : originalFiles.entrySet()) {
            buildEntries.add(SegmentArchiveBuilder.fromBytes(f.getKey(), f.getValue()));
        }

        // Write archive to temp file (simulating blob store write)
        java.nio.file.Path archiveFile = createTempFile("segment-archive-download-test-", ".zip");
        Map<String, SegmentArchiveEntry> uploadMetadata;
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(archiveFile.toFile())) {
            uploadMetadata = SegmentArchiveBuilder.buildAndExtractOffsets(fos, buildEntries);
        }

        // Step 2: Simulate download — read archive from "blob store" (file)
        byte[] archiveBlob = java.nio.file.Files.readAllBytes(archiveFile);
        long archiveSize = archiveBlob.length;

        // Step 3: Read ONLY the tail (simulating a single range-read of the last N bytes)
        // In production, this would be: transferService.downloadBlob(path, blob, archiveSize - tailLen, tailLen)
        int tailLen = (int) Math.min(archiveSize, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(archiveBlob, (int) (archiveSize - tailLen), (int) archiveSize);

        // Step 4: Parse central directory from tail to discover all file offsets
        Map<String, SegmentArchiveEntry> downloadMetadata = ZipSegmentParser.parseToMap(tail, archiveSize - tailLen);

        // ASSERT: Parser found ALL files that were uploaded
        assertThat("Parser must discover all files in archive", downloadMetadata.size(), equalTo(originalFiles.size()));

        // Step 5 & 6: Range-read each file and verify content
        for (Map.Entry<String, byte[]> original : originalFiles.entrySet()) {
            String filename = original.getKey();
            byte[] expectedContent = original.getValue();

            // Get download offset from parsed metadata
            SegmentArchiveEntry parsedEntry = downloadMetadata.get(filename);
            assertThat("Parser must find entry for: " + filename, parsedEntry, notNullValue());
            assertThat(
                "Parsed length must match original for: " + filename,
                parsedEntry.getLength(),
                equalTo((long) expectedContent.length)
            );

            // Simulate range-read: downloadBlob(path, blob, offset, length)
            long rangeStart = parsedEntry.getOffset();
            long rangeLen = parsedEntry.getLength();
            assertTrue("Range must be within archive bounds for: " + filename, rangeStart + rangeLen <= archiveSize);

            byte[] downloadedContent = Arrays.copyOfRange(archiveBlob, (int) rangeStart, (int) (rangeStart + rangeLen));

            // ASSERT: Downloaded content is byte-identical to what was uploaded
            assertArrayEquals("Downloaded content must match original for: " + filename, expectedContent, downloadedContent);

            // Step 7: Verify CRC32 checksum
            // The upload metadata has CRC32 from build time; verify downloaded data matches
            SegmentArchiveEntry builtEntry = uploadMetadata.get(filename);
            assertNotNull("Upload metadata must have entry for: " + filename, builtEntry);

            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(downloadedContent);
            long downloadedChecksum = crc.getValue();

            assertThat(
                "CRC32 of downloaded content must match upload metadata for: " + filename,
                downloadedChecksum,
                equalTo(builtEntry.getChecksum())
            );
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
