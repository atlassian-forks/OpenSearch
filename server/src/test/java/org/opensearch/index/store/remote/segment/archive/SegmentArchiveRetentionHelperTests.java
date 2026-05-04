/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TDD tests for {@link SegmentArchiveRetentionHelper}.
 */
public class SegmentArchiveRetentionHelperTests extends OpenSearchTestCase {

    /**
     * Test: archive blob not referenced by any active metadata → stale → should be deleted.
     */
    public void testStaleArchiveIdentified() {
        Set<String> allBlobs = new HashSet<>(
            Arrays.asList(
                "segment_archive_1710000000000_abc123.tar",   // stale
                "segment_archive_1710000001000_def456.tar",   // active
                "_0.si__uuid1",                                // individual segment file (not archive)
                "_0.cfs__uuid2"                                // individual segment file (not archive)
            )
        );

        Set<String> activeUploaded = new HashSet<>(
            Arrays.asList(
                "segment_archive_1710000001000_def456.tar",   // referenced by metadata
                "_0.si__uuid1",
                "_0.cfs__uuid2"
            )
        );

        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(allBlobs, activeUploaded);

        assertEquals(1, stale.size());
        assertEquals("segment_archive_1710000000000_abc123.tar", stale.get(0));
    }

    /**
     * Test: archive blob still referenced → active → should NOT be deleted.
     */
    public void testActiveArchivePreserved() {
        Set<String> allBlobs = new HashSet<>(Arrays.asList("segment_archive_1710000001000_def456.tar"));

        Set<String> activeUploaded = new HashSet<>(Arrays.asList("segment_archive_1710000001000_def456.tar"));

        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(allBlobs, activeUploaded);

        assertTrue("Active archive should not be marked stale", stale.isEmpty());
    }

    /**
     * Test: mixed scenario — some archives stale, some active, plus individual files.
     */
    public void testMixedArchivesOnlyStaleDeleted() {
        Set<String> allBlobs = new HashSet<>(
            Arrays.asList(
                "segment_archive_1710000000000_aaa.tar",   // stale
                "segment_archive_1710000001000_bbb.tar",   // active
                "segment_archive_1710000002000_ccc.tar",   // stale
                "segment_archive_1710000003000_ddd.tar",   // active
                "_0.si__uuid1",                             // individual (not archive)
                "_1.cfs__uuid2",                            // individual (not archive)
                "segments_1__uuid3"                          // segment infos (not archive)
            )
        );

        Set<String> activeUploaded = new HashSet<>(
            Arrays.asList(
                "segment_archive_1710000001000_bbb.tar",
                "segment_archive_1710000003000_ddd.tar",
                "_0.si__uuid1",
                "_1.cfs__uuid2",
                "segments_1__uuid3"
            )
        );

        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(allBlobs, activeUploaded);

        assertEquals(2, stale.size());
        assertTrue(stale.contains("segment_archive_1710000000000_aaa.tar"));
        assertTrue(stale.contains("segment_archive_1710000002000_ccc.tar"));
        // Individual files should NOT be in stale list
        assertFalse(stale.contains("_0.si__uuid1"));
        assertFalse(stale.contains("_1.cfs__uuid2"));
    }

    /**
     * Test: no archive blobs at all → nothing to delete.
     */
    public void testNoArchiveBlobsReturnsEmpty() {
        Set<String> allBlobs = new HashSet<>(Arrays.asList("_0.si__uuid1", "_0.cfs__uuid2"));

        Set<String> activeUploaded = new HashSet<>(Arrays.asList("_0.si__uuid1", "_0.cfs__uuid2"));

        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(allBlobs, activeUploaded);
        assertTrue("No archives should mean nothing stale", stale.isEmpty());
    }

    /**
     * Test: empty data directory → nothing to delete.
     */
    public void testEmptyDataDirReturnsEmpty() {
        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(new HashSet<>(), new HashSet<>());
        assertTrue(stale.isEmpty());
    }

    /**
     * Test: null inputs handled gracefully.
     */
    public void testNullInputsHandled() {
        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(null, new HashSet<>());
        assertTrue(stale.isEmpty());
    }

    /**
     * Test: isArchiveBlob correctly identifies archive blobs.
     */
    public void testIsArchiveBlob() {
        assertTrue(SegmentArchiveRetentionHelper.isArchiveBlob("segment_archive_1710000000000_abc.tar"));
        assertTrue(SegmentArchiveRetentionHelper.isArchiveBlob("segment_archive_123.tar"));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob("_0.si__uuid1"));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob("segments_1__uuid2"));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob("metadata__primary__gen__uuid"));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob(null));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob(""));
        assertFalse(SegmentArchiveRetentionHelper.isArchiveBlob("segment_archive_no_zip_ext"));
    }

    /**
     * Test: extractUploadedFilename parses metadata value correctly.
     */
    public void testExtractUploadedFilename() {
        // Standard format: original::uploaded::checksum::length::version
        assertEquals(
            "segment_archive_123_abc.tar",
            SegmentArchiveRetentionHelper.extractUploadedFilename("_0.si::segment_archive_123_abc.tar::12345::100::9")
        );
        // Individual file format
        assertEquals("_0.si__uuid1", SegmentArchiveRetentionHelper.extractUploadedFilename("_0.si::_0.si__uuid1::12345::200::9"));
    }

    /**
     * Test: collectActiveUploadedFilenames aggregates across multiple metadata maps.
     */
    public void testCollectActiveUploadedFilenames() {
        Map<String, String> metadata1 = new HashMap<>();
        metadata1.put("_0.si", "_0.si::segment_archive_1000_aaa.tar::111::50::9");
        metadata1.put("_0.cfs", "_0.cfs::segment_archive_1000_aaa.tar::222::500::9");

        Map<String, String> metadata2 = new HashMap<>();
        metadata2.put("_0.si", "_0.si::segment_archive_2000_bbb.tar::333::50::9");
        metadata2.put("_1.si", "_1.si::_1.si__uuid1::444::60::9");

        Set<String> active = SegmentArchiveRetentionHelper.collectActiveUploadedFilenames(Arrays.asList(metadata1, metadata2));

        assertTrue(active.contains("segment_archive_1000_aaa.tar"));
        assertTrue(active.contains("segment_archive_2000_bbb.tar"));
        assertTrue(active.contains("_1.si__uuid1"));
        assertEquals(3, active.size()); // deduplicated: archive_1000 appears twice but as same uploaded name
    }

    /**
     * Test: all archives stale when no metadata references any archive.
     */
    public void testAllArchivesStaleWhenNoMetadataReferencesArchives() {
        Set<String> allBlobs = new HashSet<>(
            Arrays.asList("segment_archive_1000_aaa.tar", "segment_archive_2000_bbb.tar", "segment_archive_3000_ccc.tar")
        );

        // Active metadata only references individual files
        Set<String> activeUploaded = new HashSet<>(Arrays.asList("_0.si__uuid1", "_0.cfs__uuid2"));

        List<String> stale = SegmentArchiveRetentionHelper.findStaleArchiveBlobs(allBlobs, activeUploaded);
        assertEquals(3, stale.size());
    }
}
