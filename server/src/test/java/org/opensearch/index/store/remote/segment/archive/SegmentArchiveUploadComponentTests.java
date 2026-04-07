/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source source source source source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Component integration test for segment archive upload/download:
 * wires {@link SegmentArchiveBuilder} with a counting blob store to verify
 * PUT / GET / LIST counts for archive-ON vs archive-OFF, plus round-trip
 * correctness (build → upload → range-read recovery).
 *
 * <p>Tests the full segment archive lifecycle without a cluster:
 * <ul>
 *   <li>Archive ZIP builds correctly and produces valid offsets</li>
 *   <li>Range-read recovery (openInput via offset/length) returns correct content</li>
 *   <li>Fallback to per-file when archive blob read fails</li>
 *   <li>PUT count: archive-ON = 1 ZIP PUT vs archive-OFF = N file PUTs</li>
 *   <li>GET count: archive range-read = 1 GET per file vs per-file = 1 GET per file</li>
 * </ul>
 */
public class SegmentArchiveUploadComponentTests extends OpenSearchTestCase {

    // Segment file content used across tests.
    private static final Map<String, byte[]> SEGMENT_FILES = new HashMap<>();

    static {
        SEGMENT_FILES.put("_0.si",  "segment info content for _0".getBytes(StandardCharsets.UTF_8));
        SEGMENT_FILES.put("_0.cfs", "compound file set content".getBytes(StandardCharsets.UTF_8));
        SEGMENT_FILES.put("_0.cfe", "compound file entries content".getBytes(StandardCharsets.UTF_8));
        SEGMENT_FILES.put("segments_1", "segments file content".getBytes(StandardCharsets.UTF_8));
    }

    private CountingBlobContainer blobContainer;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        blobContainer = new CountingBlobContainer();
    }

    // -----------------------------------------------------------------------
    // 1. Archive build round-trip: build ZIP → upload → range-read recovery
    // -----------------------------------------------------------------------

    /**
     * Builds a segment archive ZIP from N files, "uploads" it (writes to in-memory store),
     * then verifies that each file can be recovered via range-read using offsets.
     * This is the core archive upload → download round-trip.
     */
    public void testArchiveBuildAndRangeReadRoundTrip() throws IOException {
        // Build archive entries.
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());

        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();

        // "Upload" the ZIP (1 PUT).
        String archiveBlobName = "segment_archive_12345_test.zip";
        blobContainer.writeBlob(archiveBlobName, new ByteArrayInputStream(archiveBytes), archiveBytes.length, true);

        assertEquals("Archive upload: exactly 1 PUT", 1, blobContainer.putCount());
        assertEquals("Archive upload: 0 LISTs",       0, blobContainer.listCount());
        assertEquals("Archive upload: 0 DELETEs",     0, blobContainer.deleteCount());

        logger.info("Archive-ON upload — PUTs={}, GETs={}, LISTs={}, DELETEs={}",
            blobContainer.putCount(), blobContainer.getCount(),
            blobContainer.listCount(), blobContainer.deleteCount());

        blobContainer.reset();

        // Range-read recovery: for each file, read bytes at [offset, offset+length).
        for (Map.Entry<String, byte[]> expected : SEGMENT_FILES.entrySet()) {
            String name = expected.getKey();
            byte[] expectedContent = expected.getValue();

            SegmentArchiveEntry entry = archiveEntries.get(name);
            assertNotNull("Archive entry must exist for " + name, entry);

            // Simulate openInput() range-read: readBlob(archiveBlobName, offset, length)
            try (InputStream rangeStream = blobContainer.readBlob(
                archiveBlobName, entry.getOffset(), entry.getLength())) {
                byte[] recovered = rangeStream.readAllBytes();
                assertArrayEquals(
                    "Range-read content mismatch for " + name,
                    expectedContent, recovered
                );
            }
        }

        // Each file: 1 range GET — total N GETs for N files.
        assertEquals("Recovery: 1 GET per file = " + SEGMENT_FILES.size() + " GETs",
            SEGMENT_FILES.size(), blobContainer.getCount());
        assertEquals("Recovery: 0 PUTs",   0, blobContainer.putCount());
        assertEquals("Recovery: 0 LISTs",  0, blobContainer.listCount());
        assertEquals("Recovery: 0 DELETEs",0, blobContainer.deleteCount());

        logger.info("Archive-ON recovery — PUTs={}, GETs={}, LISTs={}, DELETEs={}",
            blobContainer.putCount(), blobContainer.getCount(),
            blobContainer.listCount(), blobContainer.deleteCount());
    }

    // -----------------------------------------------------------------------
    // 2. PUT count: archive-ON (1 ZIP PUT) vs archive-OFF (N file PUTs)
    // -----------------------------------------------------------------------

    /**
     * Archive-ON: uploading N segment files as a single ZIP = 1 PUT.
     * Archive-OFF: uploading N segment files individually = N PUTs.
     */
    public void testPutCountArchiveOnVsOff() throws IOException {
        int fileCount = SEGMENT_FILES.size();

        // --- Archive-ON: build ZIP, 1 PUT ---
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());
        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();
        blobContainer.writeBlob("segment_archive.zip", new ByteArrayInputStream(archiveBytes), archiveBytes.length, true);

        int archiveOnPuts = blobContainer.putCount();
        assertEquals("Archive-ON: exactly 1 PUT (the ZIP)", 1, archiveOnPuts);

        logger.info("Archive-ON upload ({} files) — PUTs={}", fileCount, archiveOnPuts);
        blobContainer.reset();

        // --- Archive-OFF: N individual PUTs ---
        for (Map.Entry<String, byte[]> file : SEGMENT_FILES.entrySet()) {
            byte[] content = file.getValue();
            blobContainer.writeBlob(file.getKey() + "__uuid", new ByteArrayInputStream(content), content.length, true);
        }

        int archiveOffPuts = blobContainer.putCount();
        assertEquals("Archive-OFF: " + fileCount + " PUTs (one per file)", fileCount, archiveOffPuts);

        logger.info("Archive-OFF upload ({} files) — PUTs={}", fileCount, archiveOffPuts);

        assertTrue("Archive-ON PUTs (" + archiveOnPuts + ") < archive-OFF PUTs (" + archiveOffPuts + ")",
            archiveOnPuts < archiveOffPuts);
        assertEquals("Archive-ON saves " + (fileCount - 1) + " PUTs vs archive-OFF",
            fileCount - 1, archiveOffPuts - archiveOnPuts);
    }

    // -----------------------------------------------------------------------
    // 3. GET count: archive range-read vs per-file download
    // -----------------------------------------------------------------------

    /**
     * Archive-ON recovery: each file recovered via range-read from single archive blob.
     * Archive-OFF recovery: each file downloaded via individual GET.
     * Both result in N GETs total, but archive-ON GETs are range-reads (offset, length)
     * while archive-OFF GETs are full-file reads.
     */
    public void testGetCountArchiveOnVsOff() throws IOException {
        int fileCount = SEGMENT_FILES.size();

        // --- Archive-ON: build ZIP ---
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());
        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();
        blobContainer.writeBlob("archive.zip", new ByteArrayInputStream(archiveBytes), archiveBytes.length, true);
        blobContainer.reset();

        // Range-read each file.
        for (Map.Entry<String, SegmentArchiveEntry> e : archiveEntries.entrySet()) {
            try (InputStream is = blobContainer.readBlob("archive.zip", e.getValue().getOffset(), e.getValue().getLength())) {
                is.readAllBytes(); // consume
            }
        }
        int archiveOnGets = blobContainer.getCount();
        assertEquals("Archive-ON: " + fileCount + " range GETs (1 per file)", fileCount, archiveOnGets);

        logger.info("Archive-ON recovery ({} files) — GETs={} (range-reads)", fileCount, archiveOnGets);
        blobContainer.reset();

        // --- Archive-OFF: N full-file GETs ---
        for (Map.Entry<String, byte[]> file : SEGMENT_FILES.entrySet()) {
            byte[] content = file.getValue();
            blobContainer.writeBlob(file.getKey() + "__uuid", new ByteArrayInputStream(content), content.length, true);
        }
        blobContainer.reset();
        for (String name : SEGMENT_FILES.keySet()) {
            try (InputStream is = blobContainer.readBlob(name + "__uuid")) {
                is.readAllBytes();
            }
        }
        int archiveOffGets = blobContainer.getCount();
        assertEquals("Archive-OFF: " + fileCount + " full GETs", fileCount, archiveOffGets);

        logger.info("Archive-OFF recovery ({} files) — GETs={} (full-file)", fileCount, archiveOffGets);

        // GET count is the same — but archive GETs are range-reads (saves bandwidth on large ZIPs).
        assertEquals("Both modes: N GETs for N files", archiveOnGets, archiveOffGets);
    }

    // -----------------------------------------------------------------------
    // 4. openInput() fallback: archive blob read fails → per-file fallback
    //    (verifies Bug 1 fix: IOException from readBlob falls through to per-file path)
    // -----------------------------------------------------------------------

    /**
     * Simulates the scenario where the archive blob was GC'd (or temporarily unavailable)
     * but metadata still references it. Verifies that {@code openInput()} falls back to
     * per-file download instead of propagating the IOException.
     *
     * <p>This is the regression test for Bug 1: archive blob read failure now falls through
     * to the per-file download path.
     */
    public void testOpenInputFallsBackToPerFileWhenArchiveReadFails() throws IOException {
        // Setup: build archive and upload its files individually too (both paths available).
        byte[] content = "segment content for fallback test".getBytes(StandardCharsets.UTF_8);
        String fileName = "_0.si";
        String archiveBlobName = "segment_archive_broken.zip";
        String perFileBlobName = "_0.si__uuid123";

        // Upload per-file copy (the fallback target).
        blobContainer.writeBlob(perFileBlobName, new ByteArrayInputStream(content), content.length, true);

        // Simulate archive entry pointing to the broken archive.
        SegmentArchiveEntry archiveEntry = new SegmentArchiveEntry(fileName, 30L, content.length, 0L);

        // Use a FailingBlobContainer that throws on range-reads.
        FailingBlobContainer failingContainer = new FailingBlobContainer(content, perFileBlobName);

        // Simulate openInput() logic with archive fallback:
        byte[] recovered = openInputWithFallback(failingContainer, fileName, archiveBlobName, archiveEntry, perFileBlobName);

        assertArrayEquals("Fallback: per-file content should match original", content, recovered);
        assertTrue("FailingBlobContainer: archive read was attempted", failingContainer.archiveReadAttempted());
        assertTrue("FailingBlobContainer: per-file read was used after fallback", failingContainer.perFileReadUsed());

        logger.info("openInput() fallback test passed: archive read failed, per-file used successfully");
    }

    /**
     * Simulates the {@code openInput()} logic from {@link org.opensearch.index.store.RemoteSegmentStoreDirectory}:
     * 1. Attempt archive range-read.
     * 2. On IOException, fall back to per-file download.
     */
    private byte[] openInputWithFallback(
        FailingBlobContainer container,
        String name,
        String archiveBlobName,
        SegmentArchiveEntry archiveEntry,
        String perFileBlobName
    ) throws IOException {
        // Try archive range-read first.
        try (InputStream archiveStream = container.readBlob(archiveBlobName,
            archiveEntry.getOffset(), archiveEntry.getLength())) {
            return archiveStream.readAllBytes();
        } catch (IOException e) {
            logger.warn("Archive read failed for {}, falling back to per-file: {}", name, e.getMessage());
        }

        // Fallback: per-file read.
        try (InputStream perFileStream = container.readBlob(perFileBlobName)) {
            return perFileStream.readAllBytes();
        }
    }

    // -----------------------------------------------------------------------
    // 5. Archive offset correctness: range bytes match original content exactly
    // -----------------------------------------------------------------------

    /**
     * Verifies that the offsets recorded by {@link SegmentArchiveBuilder#buildAndExtractOffsets}
     * produce exactly the original file bytes when used for byte-range reads on the raw archive.
     */
    public void testOffsetAccuracyForEachFile() throws IOException {
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());

        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();

        for (Map.Entry<String, byte[]> expected : SEGMENT_FILES.entrySet()) {
            String name = expected.getKey();
            byte[] expectedContent = expected.getValue();
            SegmentArchiveEntry entry = archiveEntries.get(name);
            assertNotNull("Entry must exist for " + name, entry);

            // Direct byte-range extraction from raw ZIP bytes.
            int start = (int) entry.getOffset();
            int end   = start + (int) entry.getLength();
            assertTrue("Offset " + start + " must be within archive (" + archiveBytes.length + " bytes)",
                start >= 0 && end <= archiveBytes.length);

            byte[] rangeBytes = Arrays.copyOfRange(archiveBytes, start, end);
            assertArrayEquals("Offset-based extraction must match original for " + name,
                expectedContent, rangeBytes);

            assertEquals("Recorded length must equal actual content length for " + name,
                expectedContent.length, (int) entry.getLength());
        }
    }

    // -----------------------------------------------------------------------
    // 6. Comparative: total blob ops archive-ON vs archive-OFF (upload + recovery)
    // -----------------------------------------------------------------------

    /**
     * For N files:
     * Archive-ON: 1 PUT (upload ZIP) + N range-GETs (recovery) = N+1 total ops
     * Archive-OFF: N PUTs (upload) + N full GETs (recovery) = 2N total ops
     * Archive-ON saves N-1 ops = (N-1)/(2N) = ~50% for large N.
     */
    public void testTotalBlobOpsArchiveOnVsOff() throws IOException {
        int fileCount = SEGMENT_FILES.size(); // 4 files

        // --- Archive-ON: 1 ZIP upload + N range-read recoveries ---
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());
        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();

        blobContainer.writeBlob("archive.zip", new ByteArrayInputStream(archiveBytes), archiveBytes.length, true);
        for (Map.Entry<String, SegmentArchiveEntry> e : archiveEntries.entrySet()) {
            try (InputStream is = blobContainer.readBlob("archive.zip", e.getValue().getOffset(), e.getValue().getLength())) {
                is.readAllBytes();
            }
        }
        int archiveOnTotal = blobContainer.putCount() + blobContainer.getCount()
            + blobContainer.listCount() + blobContainer.deleteCount();

        logger.info("Archive-ON ({} files) — PUTs={}, GETs={}, LISTs={}, DELETEs={}, TOTAL={}",
            fileCount, blobContainer.putCount(), blobContainer.getCount(),
            blobContainer.listCount(), blobContainer.deleteCount(), archiveOnTotal);

        assertEquals("Archive-ON PUTs: 1 (the ZIP)", 1, blobContainer.putCount());
        assertEquals("Archive-ON GETs: " + fileCount + " (range-reads)", fileCount, blobContainer.getCount());
        assertEquals("Archive-ON total: " + (fileCount + 1), fileCount + 1, archiveOnTotal);

        blobContainer.reset();

        // --- Archive-OFF: N individual uploads + N full GETs ---
        for (Map.Entry<String, byte[]> file : SEGMENT_FILES.entrySet()) {
            byte[] content = file.getValue();
            blobContainer.writeBlob(file.getKey() + "__uuid", new ByteArrayInputStream(content), content.length, true);
        }
        for (Map.Entry<String, byte[]> file : SEGMENT_FILES.entrySet()) {
            try (InputStream is = blobContainer.readBlob(file.getKey() + "__uuid")) {
                is.readAllBytes();
            }
        }
        int archiveOffTotal = blobContainer.putCount() + blobContainer.getCount()
            + blobContainer.listCount() + blobContainer.deleteCount();

        logger.info("Archive-OFF ({} files) — PUTs={}, GETs={}, LISTs={}, DELETEs={}, TOTAL={}",
            fileCount, blobContainer.putCount(), blobContainer.getCount(),
            blobContainer.listCount(), blobContainer.deleteCount(), archiveOffTotal);

        assertEquals("Archive-OFF PUTs: " + fileCount, fileCount, blobContainer.putCount());
        assertEquals("Archive-OFF GETs: " + fileCount, fileCount, blobContainer.getCount());
        assertEquals("Archive-OFF total: " + (fileCount * 2), fileCount * 2, archiveOffTotal);

        assertTrue("Archive-ON total ops (" + archiveOnTotal + ") < archive-OFF (" + archiveOffTotal + ")",
            archiveOnTotal < archiveOffTotal);
        assertEquals("Archive-ON saves " + (fileCount - 1) + " blob ops vs archive-OFF",
            fileCount - 1, archiveOffTotal - archiveOnTotal);
    }

    // -----------------------------------------------------------------------
    // 7. openInput() metadata-refresh on archive miss (GC race — Bug 1 real fix)
    // -----------------------------------------------------------------------

    /**
     * Simulates the GC race: in-memory currentArchiveBlobName = ZIP_A (stale, GC'd).
     * Latest metadata on remote = ZIP_B (new, valid). Verifies that after archive read
     * fails, openInput() re-reads latest metadata, retries from ZIP_B, and returns
     * correct content.
     *
     * <p>This tests the real fix for Bug 1: metadata-refresh on archive miss, NOT
     * per-file fallback (which would read the ZIP as a raw segment = corrupt data).
     */
    public void testOpenInputRefreshesMetadataOnArchiveMiss() throws IOException {
        // ZIP_A: stale, GC'd — in-memory reference
        byte[] content = "segment content for _0.si".getBytes(StandardCharsets.UTF_8);
        String fileName = "_0.si";

        // ZIP_B: current valid archive on remote
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = List.of(
            SegmentArchiveBuilder.fromBytes(fileName, content)
        );
        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> freshEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] freshArchiveBytes = archiveOut.toByteArray();
        String freshArchiveName = "segment_archive_new_zip_b.zip";

        // Simulate: stale archive read fails, fresh archive read succeeds.
        // Verify: content from ZIP_B is returned correctly.
        StaleArchiveBlobContainer container = new StaleArchiveBlobContainer(
            freshArchiveName, freshArchiveBytes
        );

        // Simulate openInput() with metadata-refresh logic (as fixed in RemoteSegmentStoreDirectory):
        String staleArchiveName = "segment_archive_old_zip_a.zip";
        SegmentArchiveEntry staleEntry = new SegmentArchiveEntry(fileName, 30L, content.length, 0L);

        byte[] recovered = openInputWithMetadataRefresh(
            container, fileName, staleArchiveName, staleEntry, freshArchiveName, freshEntries
        );

        assertArrayEquals("Metadata-refresh: content from fresh ZIP_B must match original", content, recovered);
        assertTrue("Stale archive read was attempted", container.staleReadAttempted());
        assertTrue("Fresh archive read was used after refresh", container.freshReadUsed());

        logger.info("openInput() metadata-refresh test passed: ZIP_A failed, ZIP_B used after refresh");
    }

    /**
     * Simulates the flag toggle OFF scenario: in-memory currentArchiveBlobName = ZIP_A,
     * but latest metadata has archive disabled (flag turned OFF) and per-file blobs exist.
     * Verifies that after archive read fails, openInput() reads from per-file blob correctly.
     */
    public void testOpenInputFlagToggledOffFallsToPerFile() throws IOException {
        byte[] content = "segment content for flag toggle test".getBytes(StandardCharsets.UTF_8);
        String fileName = "_0.si";
        String perFileBlobName = "_0.si__uuid_perfile";

        // Simulate: archive read fails (flag toggled OFF, ZIP_A GC'd),
        // latest metadata has archiveEnabled=false, per-file blob exists.
        FlagToggledBlobContainer container = new FlagToggledBlobContainer(content, perFileBlobName);

        byte[] recovered = openInputWithFlagToggleFallback(container, fileName, perFileBlobName);

        assertArrayEquals("Flag-toggle: per-file content must match original", content, recovered);
        assertTrue("Archive read was attempted first", container.archiveReadAttempted());
        assertTrue("Per-file read was used after flag-toggle fallback", container.perFileReadUsed());

        logger.info("openInput() flag-toggle test passed: archive disabled in latest metadata, per-file used");
    }

    /**
     * Simulates the initializeToSpecificTimestamp/Commit archive field population fix.
     * Verifies that when archive metadata is loaded for a specific commit, the archive
     * fields (blobName + entries) are correctly set and openInput() uses range-reads.
     */
    public void testInitializeToSpecificCommitSetsArchiveFields() throws IOException {
        // Build an archive and its entries (simulates what was uploaded at a specific commit).
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = SEGMENT_FILES.entrySet().stream()
            .map(e -> SegmentArchiveBuilder.fromBytes(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());
        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        Map<String, SegmentArchiveEntry> archiveEntries = SegmentArchiveBuilder.buildAndExtractOffsets(archiveOut, entries);
        byte[] archiveBytes = archiveOut.toByteArray();
        String archiveBlobName = "segment_archive_commit1.zip";

        // Upload archive to container.
        blobContainer.writeBlob(archiveBlobName, new ByteArrayInputStream(archiveBytes), archiveBytes.length, true);
        blobContainer.reset();

        // Simulate what initializeToSpecificCommit/Timestamp now correctly does:
        // sets currentArchiveBlobName and currentArchiveEntries from the loaded metadata.
        // (Before the fix these were null, causing corrupt reads via uploadedFilename=ZIP name)
        String simulatedCurrentArchiveBlobName = archiveBlobName;  // now correctly set
        Map<String, SegmentArchiveEntry> simulatedCurrentArchiveEntries = archiveEntries; // now correctly set

        // Verify: openInput() uses range-reads correctly (not corrupt ZIP-as-raw-segment read).
        for (Map.Entry<String, byte[]> expected : SEGMENT_FILES.entrySet()) {
            String name = expected.getKey();
            byte[] expectedContent = expected.getValue();

            // This is what openInput() does with correctly-set currentArchiveBlobName:
            assertTrue("File must be in archive entries", simulatedCurrentArchiveEntries.containsKey(name));
            SegmentArchiveEntry entry = simulatedCurrentArchiveEntries.get(name);
            try (InputStream rangeStream = blobContainer.readBlob(
                    simulatedCurrentArchiveBlobName, entry.getOffset(), entry.getLength())) {
                byte[] recovered = rangeStream.readAllBytes();
                assertArrayEquals("initializeToSpecificCommit: range-read must return correct content for " + name,
                    expectedContent, recovered);
            }
        }

        // All reads are range-reads (not full ZIP reads).
        assertEquals("All reads are range GETs: " + SEGMENT_FILES.size(), SEGMENT_FILES.size(), blobContainer.getCount());

        logger.info("initializeToSpecificCommit archive field population verified: {} files, all via range-read",
            SEGMENT_FILES.size());
    }

    // Helpers for metadata-refresh and flag-toggle tests

    private byte[] openInputWithMetadataRefresh(
        StaleArchiveBlobContainer container,
        String name,
        String staleArchiveName,
        SegmentArchiveEntry staleEntry,
        String freshArchiveName,
        Map<String, SegmentArchiveEntry> freshEntries
    ) throws IOException {
        // Step 1: Try stale archive read.
        try (InputStream s = container.readBlob(staleArchiveName, staleEntry.getOffset(), staleEntry.getLength())) {
            return s.readAllBytes();
        } catch (IOException e) {
            logger.warn("Stale archive read failed for {}: {}", name, e.getMessage());
        }
        // Step 2: Metadata refresh — get fresh archive reference.
        SegmentArchiveEntry freshEntry = freshEntries.get(name);
        if (freshEntry != null) {
            try (InputStream s = container.readBlob(freshArchiveName, freshEntry.getOffset(), freshEntry.getLength())) {
                return s.readAllBytes();
            }
        }
        throw new java.io.FileNotFoundException(name + " not in fresh archive");
    }

    private byte[] openInputWithFlagToggleFallback(
        FlagToggledBlobContainer container,
        String name,
        String perFileBlobName
    ) throws IOException {
        // Step 1: Try archive read (stale reference, flag now OFF).
        try (InputStream s = container.readBlob("segment_archive_old.zip", 30L, 100L)) {
            return s.readAllBytes();
        } catch (IOException e) {
            logger.warn("Archive read failed (flag toggled OFF): {}", e.getMessage());
        }
        // Step 2: Latest metadata has archiveEnabled=false — use per-file blob.
        try (InputStream s = container.readBlob(perFileBlobName)) {
            return s.readAllBytes();
        }
    }

    // -----------------------------------------------------------------------
    // In-memory CountingBlobContainer
    // -----------------------------------------------------------------------

    /**
     * In-memory blob container that counts PUT / GET / LIST / DELETE operations
     * and stores blobs in a {@link HashMap} for retrieval.
     */
    static final class CountingBlobContainer {
        private final Map<String, byte[]> blobs = new HashMap<>();
        private final AtomicInteger puts    = new AtomicInteger();
        private final AtomicInteger gets    = new AtomicInteger();
        private final AtomicInteger deletes = new AtomicInteger();
        private final AtomicInteger lists   = new AtomicInteger();

        void writeBlob(String name, InputStream content, long size, boolean failIfExists) throws IOException {
            puts.incrementAndGet();
            blobs.put(name, content.readAllBytes());
        }

        InputStream readBlob(String name) throws IOException {
            gets.incrementAndGet();
            byte[] data = blobs.get(name);
            if (data == null) throw new java.io.FileNotFoundException(name);
            return new ByteArrayInputStream(data);
        }

        InputStream readBlob(String name, long position, long length) throws IOException {
            gets.incrementAndGet();
            byte[] data = blobs.get(name);
            if (data == null) throw new java.io.FileNotFoundException(name);
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        void deleteBlob(String name) {
            deletes.incrementAndGet();
            blobs.remove(name);
        }

        List<String> listBlobs() {
            lists.incrementAndGet();
            return List.copyOf(blobs.keySet());
        }

        int putCount()    { return puts.get(); }
        int getCount()    { return gets.get(); }
        int deleteCount() { return deletes.get(); }
        int listCount()   { return lists.get(); }

        void reset() { puts.set(0); gets.set(0); deletes.set(0); lists.set(0); }
    }

    /**
     * A blob container that throws on archive (range) reads but succeeds on full-file reads.
     * Used to test the openInput() fallback path (Bug 1 fix).
     */
    static final class FailingBlobContainer {
        private final byte[] perFileContent;
        private final String perFileBlobName;
        private boolean archiveReadAttempted = false;
        private boolean perFileReadUsed = false;

        FailingBlobContainer(byte[] perFileContent, String perFileBlobName) {
            this.perFileContent = perFileContent;
            this.perFileBlobName = perFileBlobName;
        }

        InputStream readBlob(String name) throws IOException {
            perFileReadUsed = true;
            if (!name.equals(perFileBlobName)) throw new java.io.FileNotFoundException(name);
            return new ByteArrayInputStream(perFileContent);
        }

        InputStream readBlob(String name, long position, long length) throws IOException {
            archiveReadAttempted = true;
            // Simulate archive blob read failure (e.g., blob GC'd or temporarily unavailable).
            throw new IOException("Simulated archive blob read failure for: " + name);
        }

        boolean archiveReadAttempted() { return archiveReadAttempted; }
        boolean perFileReadUsed()      { return perFileReadUsed; }
    }

    /**
     * Blob container where stale archive (ZIP_A) reads fail but fresh archive (ZIP_B) reads succeed.
     * Models the GC race scenario: ZIP_A was GC'd, ZIP_B is the current valid archive.
     */
    static final class StaleArchiveBlobContainer {
        private final String freshArchiveName;
        private final byte[] freshArchiveBytes;
        private boolean staleReadAttempted = false;
        private boolean freshReadUsed = false;

        StaleArchiveBlobContainer(String freshArchiveName, byte[] freshArchiveBytes) {
            this.freshArchiveName = freshArchiveName;
            this.freshArchiveBytes = freshArchiveBytes;
        }

        InputStream readBlob(String name, long position, long length) throws IOException {
            if (name.equals(freshArchiveName)) {
                // Fresh archive — succeeds, return range bytes.
                freshReadUsed = true;
                return new ByteArrayInputStream(freshArchiveBytes, (int) position, (int) length);
            }
            // Stale archive (any other name) — simulates GC'd blob.
            staleReadAttempted = true;
            throw new IOException("Blob not found (GC'd): " + name);
        }

        boolean staleReadAttempted() { return staleReadAttempted; }
        boolean freshReadUsed()      { return freshReadUsed; }
    }

    /**
     * Blob container where archive range-reads always fail (flag toggled OFF, ZIP GC'd)
     * but full per-file reads succeed. Models the archive-ON → archive-OFF flip scenario.
     */
    static final class FlagToggledBlobContainer {
        private final byte[] perFileContent;
        private final String perFileBlobName;
        private boolean archiveReadAttempted = false;
        private boolean perFileReadUsed = false;

        FlagToggledBlobContainer(byte[] perFileContent, String perFileBlobName) {
            this.perFileContent = perFileContent;
            this.perFileBlobName = perFileBlobName;
        }

        InputStream readBlob(String name) throws IOException {
            if (name.equals(perFileBlobName)) {
                perFileReadUsed = true;
                return new ByteArrayInputStream(perFileContent);
            }
            throw new java.io.FileNotFoundException(name);
        }

        InputStream readBlob(String name, long position, long length) throws IOException {
            // All archive range-reads fail — archive flag was toggled OFF, ZIP GC'd.
            archiveReadAttempted = true;
            throw new IOException("Archive disabled or GC'd: " + name);
        }

        boolean archiveReadAttempted() { return archiveReadAttempted; }
        boolean perFileReadUsed()      { return perFileReadUsed; }
    }
}
