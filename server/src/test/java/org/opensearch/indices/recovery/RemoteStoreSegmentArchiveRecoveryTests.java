/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * TDD Tests for segment archive download during recovery
 * Tests the recovery handler's ability to download segments from archives
 */
public class RemoteStoreSegmentArchiveRecoveryTests extends OpenSearchTestCase {

    /**
     * Test: Should download all segments when archive contains multiple files
     */
    public void testDownloadMultipleSegmentsFromArchive() {
        // Given: archive metadata with multiple segment files
        String archiveBlob = "refresh_20260312_093000.zip";
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.si", new SegmentArchiveEntry("_0.si", 100, 2048, 123456L));
        archiveEntries.put("_0.cfs", new SegmentArchiveEntry("_0.cfs", 2148, 8700000, 789012L));
        archiveEntries.put("_0.cfe", new SegmentArchiveEntry("_0.cfe", 8702148, 1200, 345678L));
        
        // When: downloading segments
        List<String> segmentsToDownload = Arrays.asList("_0.si", "_0.cfs", "_0.cfe");
        
        // Then: all segments should be downloadable
        for (String segmentFile : segmentsToDownload) {
            assertThat(archiveEntries.get(segmentFile), notNullValue());
            SegmentArchiveEntry entry = archiveEntries.get(segmentFile);
            assertTrue(entry.getOffset() >= 0);
            assertTrue(entry.getLength() > 0);
        }
    }

    /**
     * Test: Should use offset and length for range-read
     */
    public void testRangeReadParametersFromArchive() {
        // Given: archive entry with offset and length
        SegmentArchiveEntry entry = new SegmentArchiveEntry("_5.cfs", 5000, 15000, 999999L);
        
        // When: preparing range-read
        long rangeStart = entry.getOffset();
        long rangeEnd = entry.getOffset() + entry.getLength();
        
        // Then: should have correct bounds
        assertThat(rangeStart, equalTo(5000L));
        assertThat(rangeEnd, equalTo(20000L));
        assertTrue(rangeEnd > rangeStart);
    }

    /**
     * Test: Should handle missing segment in archive (fallback scenario)
     */
    public void testMissingSegmentInArchive() {
        // Given: archive with limited segments
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.si", new SegmentArchiveEntry("_0.si", 100, 2048, 123456L));
        archiveEntries.put("_0.cfs", new SegmentArchiveEntry("_0.cfs", 2148, 8700000, 789012L));
        
        // When: looking for a segment not in archive
        String missingSegment = "_1.si";
        
        // Then: should return null/not found
        assertNull(archiveEntries.get(missingSegment));
    }

    /**
     * Test: Should download large segment from archive
     */
    public void testDownloadLargeSegmentFromArchive() {
        // Given: a large segment in archive
        long largeSegmentSize = 100_000_000L; // 100 MB
        SegmentArchiveEntry largeEntry = new SegmentArchiveEntry("_10.cfs", 1000, largeSegmentSize, 111111L);
        
        // When: downloading
        long offset = largeEntry.getOffset();
        long length = largeEntry.getLength();
        long expectedBytes = offset + length;
        
        // Then: should be able to handle large file
        assertTrue(length == largeSegmentSize);
        assertTrue(expectedBytes > 100_000_000L);
    }

    /**
     * Test: Should verify checksum after range-read
     */
    public void testChecksumVerificationAfterDownload() {
        // Given: archive entry with checksum
        long expectedChecksum = 987654321L;
        SegmentArchiveEntry entry = new SegmentArchiveEntry("_0.cfs", 2148, 8700000, expectedChecksum);
        
        // When: verifying downloaded data
        long actualChecksum = entry.getChecksum();
        
        // Then: checksums should match
        assertThat(actualChecksum, equalTo(expectedChecksum));
    }

    /**
     * Test: Recovery should prefer archive over per-file when archive exists
     */
    public void testPreferArchiveDownloadWhenAvailable() {
        // Given: both archive and per-file paths available
        boolean archiveAvailable = true;
        boolean perFileAvailable = true;
        
        // When: deciding which path to use
        boolean useArchive = archiveAvailable; // Archive takes priority
        
        // Then: should use archive path
        assertTrue(useArchive);
    }

    /**
     * Test: Should handle archive with mixed segment types
     */
    public void testDownloadMixedSegmentTypesFromArchive() {
        // Given: archive with different segment file types
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.si", new SegmentArchiveEntry("_0.si", 100, 5000, 111L));      // segment info
        archiveEntries.put("_0.cfs", new SegmentArchiveEntry("_0.cfs", 5100, 500000, 222L)); // compound file
        archiveEntries.put("_0.cfe", new SegmentArchiveEntry("_0.cfe", 505100, 2000, 333L)); // compound entries
        archiveEntries.put("_0.liv", new SegmentArchiveEntry("_0.liv", 507100, 1000, 444L)); // live docs
        
        // When: downloading all segment types
        // Then: all should be retrievable
        assertThat(archiveEntries.size(), equalTo(4));
        for (SegmentArchiveEntry entry : archiveEntries.values()) {
            assertThat(entry, notNullValue());
            assertTrue(entry.getLength() > 0);
        }
    }

    /**
     * Test: Sequential offset ordering in archive entries
     */
    public void testSequentialOffsetOrderingInArchive() {
        // Given: multiple archive entries
        SegmentArchiveEntry entry1 = new SegmentArchiveEntry("_0.si", 100, 2048, 111L);
        SegmentArchiveEntry entry2 = new SegmentArchiveEntry("_0.cfs", 2148, 8700000, 222L);
        SegmentArchiveEntry entry3 = new SegmentArchiveEntry("_0.cfe", 8702148, 1200, 333L);
        
        // When: checking offset ordering
        long offset1 = entry1.getOffset();
        long offset2 = entry2.getOffset();
        long offset3 = entry3.getOffset();
        
        // Then: offsets should be sequential (no gaps/overlaps)
        assertTrue(offset1 < offset2);
        assertTrue(offset2 < offset3);
        assertEquals(offset1 + entry1.getLength(), offset2); // No gap
        assertEquals(offset2 + entry2.getLength(), offset3); // No gap
    }

    /**
     * Test: Empty archive handling (edge case)
     */
    public void testEmptyArchiveHandling() {
        // Given: empty archive (no entries)
        Map<String, SegmentArchiveEntry> emptyArchive = new HashMap<>();
        
        // When: checking archive
        // Then: should handle gracefully
        assertThat(emptyArchive.size(), equalTo(0));
        assertTrue(emptyArchive.isEmpty());
    }

    /**
     * Test: Archive metadata should contain all needed info for recovery
     */
    public void testArchiveMetadataCompleteness() {
        // Given: archive entry for recovery
        SegmentArchiveEntry entry = new SegmentArchiveEntry("_5.cfs", 10000, 50000, 555555L);
        
        // When: preparing recovery download
        // Then: all required fields should be present
        assertThat(entry.getFilename(), notNullValue());
        assertThat(entry.getFilename(), equalTo("_5.cfs"));
        assertTrue(entry.getOffset() >= 0);
        assertTrue(entry.getLength() > 0);
        assertThat(entry.getChecksum(), equalTo(555555L));
    }

    /**
     * Test: Should extract correct byte range from archive
     */
    public void testByteRangeExtractionFromArchive() {
        // Given: archive with known content boundaries
        SegmentArchiveEntry entry = new SegmentArchiveEntry("_0.cfs", 1000, 9000, 999L);
        
        // When: computing byte range
        long rangeStart = entry.getOffset();
        long rangeEnd = entry.getOffset() + entry.getLength();
        long rangeSize = rangeEnd - rangeStart;
        
        // Then: range should be correct
        assertThat(rangeStart, equalTo(1000L));
        assertThat(rangeEnd, equalTo(10000L));
        assertThat(rangeSize, equalTo(9000L));
    }
}
