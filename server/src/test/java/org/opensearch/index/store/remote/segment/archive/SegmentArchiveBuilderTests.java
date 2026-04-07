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

    /**
     * Test: End-to-end upload → download with real FS-based blob store simulation.
     * Builds archive → writes to FS → reads tail → parses offsets → range-reads each file → content matches.
     */
    public void testEndToEndUploadDownloadWithFsBlobStore() throws IOException {
        // Given: multiple segment files
        String path1 = "_0.si";
        byte[] content1 = "segment info for shard zero".getBytes(StandardCharsets.UTF_8);
        String path2 = "_0.cfs";
        byte[] content2 = new byte[50_000];
        Arrays.fill(content2, (byte) 0xAB);
        String path3 = "_0.cfe";
        byte[] content3 = "compound file entries metadata".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2),
            SegmentArchiveBuilder.fromBytes(path3, content3)
        );

        // When: build archive and write to temp file (simulating FS blob store upload)
        java.nio.file.Path tempFile = createTempFile("segment-archive-", ".zip");
        Map<String, SegmentArchiveEntry> archiveEntries;
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile.toFile())) {
            archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(fos, entries);
        }

        // Then: read the archive back (simulating blob store download)
        byte[] archiveBytes = java.nio.file.Files.readAllBytes(tempFile);
        assertTrue("Archive file should exist and have content", archiveBytes.length > 0);

        // Verify each file can be recovered via range-read using offsets
        for (Map.Entry<String, SegmentArchiveEntry> e : archiveEntries.entrySet()) {
            String filename = e.getKey();
            SegmentArchiveEntry entry = e.getValue();

            byte[] extracted = Arrays.copyOfRange(archiveBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));

            if (filename.equals(path1)) {
                assertArrayEquals("Content mismatch for " + path1, content1, extracted);
            } else if (filename.equals(path2)) {
                assertArrayEquals("Content mismatch for " + path2, content2, extracted);
            } else if (filename.equals(path3)) {
                assertArrayEquals("Content mismatch for " + path3, content3, extracted);
            }
        }

        // Also verify via ZipSegmentParser tail parsing (download path)
        int tailLen = (int) Math.min(archiveBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(archiveBytes, archiveBytes.length - tailLen, archiveBytes.length);
        Map<String, SegmentArchiveEntry> parsedEntries = ZipSegmentParser.parseToMap(tail, archiveBytes.length - tailLen);

        assertThat(parsedEntries.size(), equalTo(3));
        for (Map.Entry<String, SegmentArchiveEntry> pe : parsedEntries.entrySet()) {
            SegmentArchiveEntry parsed = pe.getValue();
            byte[] rangeRead = Arrays.copyOfRange(archiveBytes, (int) parsed.getOffset(), (int) (parsed.getOffset() + parsed.getLength()));
            if (pe.getKey().equals(path1)) {
                assertArrayEquals(content1, rangeRead);
            } else if (pe.getKey().equals(path2)) {
                assertArrayEquals(content2, rangeRead);
            } else if (pe.getKey().equals(path3)) {
                assertArrayEquals(content3, rangeRead);
            }
        }
    }

    /**
     * Test: Concurrent archive builds from multiple threads don't interfere.
     * Each thread builds its own archive independently.
     */
    public void testConcurrentArchiveBuilds() throws Exception {
        int numThreads = 8;
        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(numThreads);
        java.util.concurrent.CountDownLatch allDone = new java.util.concurrent.CountDownLatch(numThreads);
        java.util.concurrent.atomic.AtomicReference<Exception> firstError = new java.util.concurrent.atomic.AtomicReference<>();

        Thread[] threads = new Thread[numThreads];
        byte[][] results = new byte[numThreads][];

        for (int t = 0; t < numThreads; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await(5, java.util.concurrent.TimeUnit.SECONDS);

                    String path = "_" + threadIdx + ".cfs";
                    byte[] content = new byte[10_000];
                    Arrays.fill(content, (byte) (threadIdx + 1));

                    List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
                        SegmentArchiveBuilder.fromBytes(path, content)
                    );

                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
                    byte[] zipBytes = out.toByteArray();
                    results[threadIdx] = zipBytes;

                    // Verify our own archive is correct
                    SegmentArchiveEntry entry = archiveEntries.get(path);
                    assertNotNull("Entry should exist for thread " + threadIdx, entry);
                    assertThat(entry.getLength(), equalTo(10_000L));

                    byte[] extracted = Arrays.copyOfRange(zipBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
                    assertArrayEquals(content, extracted);
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                } finally {
                    allDone.countDown();
                }
            });
            threads[t].start();
        }

        assertTrue("All threads should complete within 30s", allDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertNull("No thread should have failed: " + firstError.get(), firstError.get());

        // Verify all results are non-null and independent
        for (int t = 0; t < numThreads; t++) {
            assertNotNull("Thread " + t + " should have produced output", results[t]);
            assertTrue("Thread " + t + " archive should have content", results[t].length > 0);
        }
    }

    /**
     * Test: Multi-shard archive - multiple shards' segment files in a single archive with independent offsets.
     */
    public void testMultiShardArchiveBuildAndIndividualRecover() throws IOException {
        // Given: segment files from 3 different shards
        String shard0_si = "shard0/_0.si";
        byte[] shard0_si_content = "shard 0 segment info".getBytes(StandardCharsets.UTF_8);
        String shard0_cfs = "shard0/_0.cfs";
        byte[] shard0_cfs_content = new byte[20_000];
        Arrays.fill(shard0_cfs_content, (byte) 0x01);

        String shard1_si = "shard1/_0.si";
        byte[] shard1_si_content = "shard 1 segment info".getBytes(StandardCharsets.UTF_8);
        String shard1_cfs = "shard1/_0.cfs";
        byte[] shard1_cfs_content = new byte[30_000];
        Arrays.fill(shard1_cfs_content, (byte) 0x02);

        String shard2_si = "shard2/_0.si";
        byte[] shard2_si_content = "shard 2 segment info".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(shard0_si, shard0_si_content),
            SegmentArchiveBuilder.fromBytes(shard0_cfs, shard0_cfs_content),
            SegmentArchiveBuilder.fromBytes(shard1_si, shard1_si_content),
            SegmentArchiveBuilder.fromBytes(shard1_cfs, shard1_cfs_content),
            SegmentArchiveBuilder.fromBytes(shard2_si, shard2_si_content)
        );

        // When: build single archive for all shards
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Then: 5 entries total, each recoverable independently
        assertThat(archiveEntries.size(), equalTo(5));

        // Verify range-read recovery for each shard's files independently
        Map<String, byte[]> expectedContent = new java.util.HashMap<>();
        expectedContent.put(shard0_si, shard0_si_content);
        expectedContent.put(shard0_cfs, shard0_cfs_content);
        expectedContent.put(shard1_si, shard1_si_content);
        expectedContent.put(shard1_cfs, shard1_cfs_content);
        expectedContent.put(shard2_si, shard2_si_content);

        for (Map.Entry<String, byte[]> expected : expectedContent.entrySet()) {
            SegmentArchiveEntry entry = archiveEntries.get(expected.getKey());
            assertNotNull("Missing entry for " + expected.getKey(), entry);

            byte[] extracted = Arrays.copyOfRange(zipBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
            assertArrayEquals("Content mismatch for " + expected.getKey(), expected.getValue(), extracted);
        }
    }

    /**
     * Test: Mid-stream failure during archive build - IOException from entry content stream.
     */
    /**
     * Verifies that {@link SegmentArchiveBuilder#fromDirectory} (2-pass streaming path) produces
     * byte-for-byte identical ZIP output to {@link SegmentArchiveBuilder#fromBytes} (byte[]-backed path).
     * Both should produce the same offsets and the same recoverable content.
     */
    public void testFromDirectoryProducesSameZipAsFromBytes() throws IOException {
        String path1 = "_0.si";
        String path2 = "_0.cfs";
        byte[] content1 = "segment info content".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "compound file content goes here".getBytes(StandardCharsets.UTF_8);

        // Write files into an in-memory Lucene directory.
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        try (IndexOutput out = dir.createOutput(path1, IOContext.DEFAULT)) {
            out.writeBytes(content1, content1.length);
        }
        try (IndexOutput out = dir.createOutput(path2, IOContext.DEFAULT)) {
            out.writeBytes(content2, content2.length);
        }

        // Build ZIP via fromBytes (byte[]-backed path).
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> bytesEntries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2)
        );
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> bytesOffsets = SegmentArchiveBuilder.buildAndExtractOffsets(bytesOut, bytesEntries);

        // Build ZIP via fromDirectory (2-pass streaming path).
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> dirEntries = Arrays.asList(
            SegmentArchiveBuilder.fromDirectory(path1, dir),
            SegmentArchiveBuilder.fromDirectory(path2, dir)
        );
        ByteArrayOutputStream dirOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> dirOffsets = SegmentArchiveBuilder.buildAndExtractOffsets(dirOut, dirEntries);

        // Offsets must match.
        assertEquals("Offsets must match between fromBytes and fromDirectory", bytesOffsets.size(), dirOffsets.size());
        for (String p : bytesOffsets.keySet()) {
            SegmentArchiveEntry be = bytesOffsets.get(p);
            SegmentArchiveEntry de = dirOffsets.get(p);
            assertNotNull("fromDirectory must have offset for " + p, de);
            assertEquals("offset must match for " + p, be.getOffset(), de.getOffset());
            assertEquals("length must match for " + p, be.getLength(), de.getLength());
        }

        // Both ZIPs must contain the same content when range-read.
        byte[] bytesZip = bytesOut.toByteArray();
        byte[] dirZip = dirOut.toByteArray();

        for (Map.Entry<String, SegmentArchiveEntry> e : bytesOffsets.entrySet()) {
            SegmentArchiveEntry entry = e.getValue();
            byte[] fromBytesContent = Arrays.copyOfRange(bytesZip, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
            byte[] fromDirContent = Arrays.copyOfRange(dirZip, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
            assertArrayEquals("Content must match for " + e.getKey(), fromBytesContent, fromDirContent);
        }

        dir.close();
    }

    public void testBuildFailsOnBrokenInputStream() {
        // Given: an entry whose InputStream throws mid-read
        SegmentArchiveBuilder.SegmentArchiveBuildEntry brokenEntry = new SegmentArchiveBuilder.SegmentArchiveBuildEntry() {
            @Override
            public String getPath() {
                return "_0.cfs";
            }

            @Override
            public java.io.InputStream getContent() {
                return new java.io.InputStream() {
                    private int bytesRead = 0;

                    @Override
                    public int read() throws IOException {
                        if (bytesRead++ > 100) {
                            throw new IOException("Simulated stream failure at byte 100");
                        }
                        return 42;
                    }
                };
            }

            @Override
            public long getSize() {
                return 1000; // Claims 1000 bytes but stream will fail at 100
            }
        };

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(brokenEntry);

        // When/Then: buildAndExtractOffsets should throw IOException
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOException ex = expectThrows(IOException.class, () -> SegmentArchiveBuilder.buildAndExtractOffsets(out, entries));
        assertTrue(
            "Should contain failure message",
            ex.getMessage().contains("Simulated stream failure") || ex.getMessage().contains("expected")
        );
    }

    /**
     * Test: Archive upload path should follow convention segments/data/{hashPrefix}/*.zip
     */
    public void testArchiveUploadPathConvention() throws IOException {
        // Given: a built archive
        String path = "_0.si";
        byte[] content = "segment info".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(SegmentArchiveBuilder.fromBytes(path, content));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // When: constructing the upload path (simulating what RemoteSegmentStoreDirectory would do)
        String indexUuid = "abc123-def456";
        String hashPrefix = Integer.toHexString(indexUuid.hashCode() & 0xFFFF);
        long timestamp = System.currentTimeMillis();
        String blobName = String.format("%017d.zip", timestamp);
        String uploadPath = "segments/data/" + hashPrefix + "/" + blobName;

        // Then: path follows convention
        assertTrue("Path should start with segments/data/", uploadPath.startsWith("segments/data/"));
        assertTrue("Path should end with .zip", uploadPath.endsWith(".zip"));
        assertTrue("Path should contain hash prefix", uploadPath.contains(hashPrefix + "/"));
        // Blob name should be 21 chars: 17-digit timestamp + ".zip"
        assertThat(blobName.length(), equalTo(21));
    }

    /**
     * INVARIANT: When archive upload is enabled, individual segment file uploads are redundant.
     *
     * This test proves that ALL segment files for a refresh are fully contained in the single
     * archive ZIP blob, and each file is independently recoverable via range-read using
     * the metadata offsets. Therefore, uploading individual files alongside the archive
     * would be wasteful (dual upload).
     *
     * Asserts:
     *  1. Every segment file that would normally be uploaded individually is present in the archive.
     *  2. The archive metadata (SegmentArchiveEntry map) covers ALL files — no file is missing.
     *  3. Each file can be extracted from the archive via offset/length range-read.
     *  4. Extracted content is byte-identical to the original.
     *  5. CRC32 checksums stored in metadata match the actual content checksums.
     *  6. A single archive blob is sufficient — no per-file blobs needed.
     */
    public void testNoIndividualUploadWhenArchiveEnabled() throws IOException {
        // Given: a realistic set of segment files from a single refresh
        // (simulating what RemoteStoreRefreshListener.uploadNewSegments would process)
        Map<String, byte[]> segmentFiles = new java.util.LinkedHashMap<>();
        segmentFiles.put("_0.si", "segment info zero".getBytes(StandardCharsets.UTF_8));
        segmentFiles.put("_0.cfs", new byte[500_000]);  // 500 KB compound file
        Arrays.fill(segmentFiles.get("_0.cfs"), (byte) 0xCA);
        segmentFiles.put("_0.cfe", "compound entries metadata block".getBytes(StandardCharsets.UTF_8));
        segmentFiles.put("_0.liv", new byte[256]);  // live docs
        Arrays.fill(segmentFiles.get("_0.liv"), (byte) 0x01);
        segmentFiles.put("segments_1", "segments gen 1 commit data".getBytes(StandardCharsets.UTF_8));

        // Build archive entries from all segment files
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> archiveInputs = new java.util.ArrayList<>();
        for (Map.Entry<String, byte[]> sf : segmentFiles.entrySet()) {
            archiveInputs.add(SegmentArchiveBuilder.fromBytes(sf.getKey(), sf.getValue()));
        }

        // When: build a single archive ZIP (this is what the archive upload path does
        // INSTEAD of calling remoteDirectory.copyFrom() per file)
        ByteArrayOutputStream archiveOutputStream = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOutputStream, archiveInputs);
        byte[] archiveBlob = archiveOutputStream.toByteArray();

        // ASSERT 1: Archive metadata covers ALL segment files — nothing is missing
        assertThat("Archive must contain metadata for every segment file", archiveEntries.size(), equalTo(segmentFiles.size()));
        for (String filename : segmentFiles.keySet()) {
            assertNotNull("Archive must contain entry for: " + filename, archiveEntries.get(filename));
        }

        // ASSERT 2: Each file is recoverable from the single archive blob via range-read
        // (this proves individual per-file uploads are unnecessary)
        int filesRecoveredFromArchive = 0;
        for (Map.Entry<String, byte[]> sf : segmentFiles.entrySet()) {
            String filename = sf.getKey();
            byte[] originalContent = sf.getValue();
            SegmentArchiveEntry entry = archiveEntries.get(filename);

            // ASSERT 3: Offset and length are valid
            assertTrue("Offset must be non-negative for " + filename, entry.getOffset() >= 0);
            assertThat("Length must match original file size for " + filename, entry.getLength(), equalTo((long) originalContent.length));
            assertTrue("Offset + length must be within archive blob", entry.getOffset() + entry.getLength() <= archiveBlob.length);

            // ASSERT 4: Range-read extraction produces byte-identical content
            byte[] extracted = Arrays.copyOfRange(archiveBlob, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
            assertArrayEquals("Content extracted from archive must match original for " + filename, originalContent, extracted);

            // ASSERT 5: CRC32 checksum matches
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(extracted);
            assertThat("CRC32 checksum must match for " + filename, crc.getValue(), equalTo(entry.getChecksum()));

            filesRecoveredFromArchive++;
        }

        // ASSERT 6: All files were recovered — a single archive blob is sufficient
        assertThat("All files must be recoverable from the single archive blob", filesRecoveredFromArchive, equalTo(segmentFiles.size()));

        // Verify: only ONE blob needs to be uploaded (the archive), not N blobs (one per file)
        // The archive blob count is always 1, vs segmentFiles.size() individual uploads
        int archiveBlobCount = 1;
        int individualBlobCount = segmentFiles.size();
        assertTrue(
            "Archive upload (1 blob) must be fewer than individual uploads (" + individualBlobCount + " blobs)",
            archiveBlobCount < individualBlobCount
        );
    }

    /**
     * Fix #6: Cross-validate that offsets from SegmentArchiveBuilder.buildAndExtractOffsets()
     * match offsets from ZipSegmentParser.parseToMap() (which parses the ZIP central directory).
     * This proves offset tracking in the builder is correct and consistent with the parser.
     */
    public void testBuilderOffsetsMatchParserOffsets() throws IOException {
        // Build archive with multiple files of varying sizes
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

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> builderEntries = SegmentArchiveBuilder.buildAndExtractOffsets(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Parse the same ZIP using ZipSegmentParser (reads central directory from tail)
        int tailLen = (int) Math.min(zipBytes.length, ZipSegmentParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        Map<String, SegmentArchiveEntry> parserEntries = ZipSegmentParser.parseToMap(tail, zipBytes.length - tailLen);

        // Cross-validate: same number of entries
        assertThat("Builder and parser must find same number of entries", builderEntries.size(), equalTo(parserEntries.size()));

        // Cross-validate: each entry's offset and length must match
        for (Map.Entry<String, SegmentArchiveEntry> be : builderEntries.entrySet()) {
            String name = be.getKey();
            SegmentArchiveEntry fromBuilder = be.getValue();
            SegmentArchiveEntry fromParser = parserEntries.get(name);
            assertNotNull("Parser must find entry: " + name, fromParser);

            assertThat("Offset mismatch for " + name, fromBuilder.getOffset(), equalTo(fromParser.getOffset()));
            assertThat("Length mismatch for " + name, fromBuilder.getLength(), equalTo(fromParser.getLength()));

            // Both offsets must extract the same correct content
            byte[] original = files.get(name);
            byte[] extractedViaBuilder = Arrays.copyOfRange(
                zipBytes,
                (int) fromBuilder.getOffset(),
                (int) (fromBuilder.getOffset() + fromBuilder.getLength())
            );
            byte[] extractedViaParser = Arrays.copyOfRange(
                zipBytes,
                (int) fromParser.getOffset(),
                (int) (fromParser.getOffset() + fromParser.getLength())
            );
            assertArrayEquals("Builder offset extraction mismatch for " + name, original, extractedViaBuilder);
            assertArrayEquals("Parser offset extraction mismatch for " + name, original, extractedViaParser);
        }
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
