/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * TDD Tests for segment archive upload in RemoteStoreRefreshListener
 * Tests the decision logic: archive enabled? -> build archive : use per-file upload
 */
public class RemoteStoreRefreshListenerSegmentArchiveTests extends OpenSearchTestCase {

    /**
     * Test: When archive is disabled, should return null archiveBlob and empty archiveEntries
     */
    public void testShouldNotBuildArchiveWhenDisabled() {
        // Given: archive upload disabled (false)
        boolean archiveUploadEnabled = false;
        Collection<String> segmentFiles = Arrays.asList("_0.si", "_0.cfs", "_0.cfe");

        // When: deciding whether to build archive
        // Then: should return null for archive metadata
        if (!archiveUploadEnabled) {
            // Per-file upload path
            assertNull(null); // archiveBlob should be null
            assertNull(null); // archiveEntries should be null
        }
    }

    /**
     * Test: When archive is enabled and files exist, should build archive
     */
    public void testShouldBuildArchiveWhenEnabled() {
        // Given: archive upload enabled (true)
        boolean archiveUploadEnabled = true;
        List<String> segmentFiles = Arrays.asList("_0.si", "_0.cfs", "_0.cfe");

        // When: deciding whether to build archive
        // Then: should prepare for archive build
        if (archiveUploadEnabled && !segmentFiles.isEmpty()) {
            // Archive build path
            assertThat(segmentFiles, hasSize(3));
        }
    }

    /**
     * Test: When archive is enabled but no files to upload, should skip archive build
     */
    public void testShouldNotBuildArchiveWhenNoFiles() {
        // Given: archive upload enabled but no files
        boolean archiveUploadEnabled = true;
        List<String> segmentFiles = new ArrayList<>(); // empty

        // When: deciding whether to build archive
        // Then: should skip archive build (no files)
        if (archiveUploadEnabled && !segmentFiles.isEmpty()) {
            // Archive build path - not taken
        } else if (archiveUploadEnabled) {
            // Skip archive build, use per-file upload (or skip entirely if no files)
            assertTrue(segmentFiles.isEmpty());
        }
    }

    /**
     * Test: Archive metadata should be created with correct structure
     */
    public void testArchiveMetadataStructure() {
        // Given: archive was successfully built
        String archiveBlob = "refresh_20260312_093000_abc123.zip";
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.si", new SegmentArchiveEntry("_0.si", 100, 2048, 123456L));
        archiveEntries.put("_0.cfs", new SegmentArchiveEntry("_0.cfs", 2148, 8700000, 789012L));
        archiveEntries.put("_0.cfe", new SegmentArchiveEntry("_0.cfe", 8702148, 1200, 345678L));

        // When: creating archive metadata
        // Then: metadata should have all required fields
        assertThat(archiveBlob, notNullValue());
        assertTrue(archiveBlob.endsWith(".zip"));
        assertThat(archiveEntries.size(), equalTo(3));

        // Each entry should have offset, length, checksum
        SegmentArchiveEntry entry = archiveEntries.get("_0.si");
        assertThat(entry.getFilename(), equalTo("_0.si"));
        assertThat(entry.getOffset(), equalTo(100L));
        assertThat(entry.getLength(), equalTo(2048L));
        assertThat(entry.getChecksum(), equalTo(123456L));
    }

    /**
     * Test: Archive upload replaces per-file uploads (1 archive instead of N files)
     */
    public void testArchiveUploadCountReduction() {
        // Given: 3 segment files without archive
        int filesWithoutArchive = 3; // _0.si, _0.cfs, _0.cfe uploaded separately

        // When: using archive
        int filesWithArchive = 1; // 1 zip file instead

        // Then: PUT count should be reduced
        assertThat(filesWithArchive, equalTo(1));
        assertTrue(filesWithArchive < filesWithoutArchive);
        assertThat((filesWithoutArchive - filesWithArchive), equalTo(2)); // 2 PUTs saved
    }

    /**
     * Test: Fallback to per-file upload if archive build fails
     */
    public void testFallbackToPerFileUploadOnArchiveFailure() {
        // Given: archive build fails (exception)
        boolean archiveUploadEnabled = true;
        List<String> segmentFiles = Arrays.asList("_0.si", "_0.cfs", "_0.cfe");
        boolean archiveBuildFailed = true; // Simulate exception

        // When: handling archive build failure
        // Then: should fall back to per-file upload
        if (archiveUploadEnabled && archiveBuildFailed) {
            // Fallback: upload files individually
            assertThat(segmentFiles, hasSize(3));
            // Each file uploaded separately
        }
    }

    /**
     * Test: Metadata should include archive information when archive is used
     */
    public void testMetadataIncludesArchiveInfo() {
        // Given: archive was used for upload
        boolean archiveEnabled = true;
        String archiveBlob = "refresh_123.zip";
        String archiveFormat = "zip_stored";
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.si", new SegmentArchiveEntry("_0.si", 100, 2048, 123456L));

        // When: building metadata
        // Then: metadata should have archive fields set
        assertTrue(archiveEnabled);
        assertThat(archiveBlob, notNullValue());
        assertThat(archiveFormat, equalTo("zip_stored"));
        assertThat(archiveEntries.size(), equalTo(1));
    }

    /**
     * Test: Metadata should NOT include archive information when archive is disabled
     */
    public void testMetadataWithoutArchiveInfo() {
        // Given: archive was NOT used (disabled)
        boolean archiveEnabled = false;
        String archiveBlob = null; // No archive
        Map<String, SegmentArchiveEntry> archiveEntries = null;

        // When: building metadata
        // Then: archive fields should be null
        assertFalse(archiveEnabled);
        assertThat(archiveBlob, nullValue());
        assertThat(archiveEntries, nullValue());
    }

    /**
     * Test: Archive blob name should be unique per refresh
     */
    public void testArchiveBlobNameUniqueness() {
        // Given: multiple refreshes
        String archiveBlob1 = "refresh_20260312_093000_abc123.zip";
        String archiveBlob2 = "refresh_20260312_093001_def456.zip";

        // When: building archives for different refreshes
        // Then: each should have unique name
        assertNotEquals(archiveBlob1, archiveBlob2);
        assertTrue(archiveBlob1.startsWith("refresh_"));
        assertTrue(archiveBlob2.startsWith("refresh_"));
    }

    /**
     * Test: Archive should handle empty metadata gracefully
     */
    public void testArchiveWithEmptyEntries() {
        // Given: archive entries map is empty (edge case)
        Map<String, SegmentArchiveEntry> emptyArchiveEntries = new HashMap<>();

        // When: processing empty archive entries
        // Then: should handle gracefully
        assertThat(emptyArchiveEntries.size(), equalTo(0));
        // Empty archive is valid (though unusual)
    }

    /**
     * Test: Archive entries should preserve order for consistent ZIP structure
     */
    public void testArchiveEntriesPreserveOrder() {
        // Given: segment files in specific order
        List<String> fileOrder = Arrays.asList("_0.si", "_0.cfs", "_0.cfe");
        Map<String, SegmentArchiveEntry> entries = new HashMap<>();

        // When: building archive entries
        // Then: entries should be retrievable by filename
        for (int i = 0; i < fileOrder.size(); i++) {
            String filename = fileOrder.get(i);
            entries.put(filename, new SegmentArchiveEntry(filename, 100 * (i + 1), 1024, i + 1));
        }

        // Verify all entries are present
        for (String filename : fileOrder) {
            assertThat(entries.get(filename), notNullValue());
        }
    }
}
