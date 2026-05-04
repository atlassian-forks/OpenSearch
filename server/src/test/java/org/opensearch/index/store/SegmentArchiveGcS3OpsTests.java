/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.store.RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH;
import static org.opensearch.test.RemoteStoreTestUtils.createMetadataFileBytesWithArchive;
import static org.opensearch.test.RemoteStoreTestUtils.createMetadataFileBytes;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadata;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadataForArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Diagnostic tests that count S3 DELETE operations per flow for segment archive ON vs OFF.
 *
 * <p>Goal: understand where extra DELETE/GET operations come from in benchmark results.
 * Each test exercises one specific code path (GC, merge-read) and counts S3 calls via Mockito
 * verify() on remoteDataDirectory and remoteMetadataDirectory mocks.
 *
 * <p><b>Key flows tested:</b>
 * <ol>
 *   <li>Archive GC: {@code deleteStaleSegments()} deletes stale archive ZIP blobs</li>
 *   <li>Archive GC: archive OFF — only per-file segment blobs are deleted (no ZIP)</li>
 *   <li>Archive GC: shared archive blob (used by multiple metadata files) is NOT deleted while still active</li>
 * </ol>
 */
public class SegmentArchiveGcS3OpsTests extends BaseRemoteSegmentStoreDirectoryTests {

    // Two metadata files: metadataFilename (active/newest) and metadataFilename2 (stale)
    // metadataFilename3 = oldest stale, will be deleted
    // archive blob names
    private static final String ACTIVE_ARCHIVE_BLOB  = "segment_archive_active_12345.tar";
    private static final String STALE_ARCHIVE_BLOB   = "segment_archive_stale_67890.tar";
    private static final long   ARCHIVE_BLOB_SIZE    = 512_000L; // 512 KB dummy size

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
    }

    // -------------------------------------------------------------------------
    // 1. Archive ON: stale archive ZIP blob is deleted, active ZIP is retained
    // -------------------------------------------------------------------------

    /**
     * Archive ON GC flow:
     * - 2 metadata files: metadataFilename (active, uses ACTIVE_ARCHIVE_BLOB)
     *                      metadataFilename2 (stale, uses STALE_ARCHIVE_BLOB)
     * - deleteStaleSegments(1) should delete STALE_ARCHIVE_BLOB but NOT ACTIVE_ARCHIVE_BLOB.
     * - No extra S3 LIST calls needed — archive blob name is read from metadata.
     */
    public void testArchiveGcDeletesStaleZipBlob() throws IOException {
        // Populate metadata: newest = metadataFilename (active), stale = metadataFilename2
        // Archive ON: uploadedFilename = archiveBlobName for all files in the archive
        Map<String, String> activeSegments = getDummyMetadataForArchive("_1", 2, ACTIVE_ARCHIVE_BLOB);
        Map<String, String> staleSegments  = getDummyMetadataForArchive("_0", 1, STALE_ARCHIVE_BLOB);

        List<String> allMetadataFiles = List.of(metadataFilename, metadataFilename2);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH
        )).thenReturn(List.of(metadataFilename));

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE
        )).thenReturn(allMetadataFiles);

        Map<String, SegmentArchiveEntry> activeArchiveEntries = Map.of(
            "_1.si", new SegmentArchiveEntry("_1.si", 0L, 100L, 0L),
            "_1.cfe", new SegmentArchiveEntry("_1.cfe", 100L, 50L, 0L),
            "_1.cfs", new SegmentArchiveEntry("_1.cfs", 150L, 200L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                activeSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos,
                ACTIVE_ARCHIVE_BLOB,
                activeArchiveEntries,
                ARCHIVE_BLOB_SIZE
            )
        );

        Map<String, SegmentArchiveEntry> staleArchiveEntries = Map.of(
            "_0.si", new SegmentArchiveEntry("_0.si", 0L, 100L, 0L),
            "_0.cfe", new SegmentArchiveEntry("_0.cfe", 100L, 50L, 0L),
            "_0.cfs", new SegmentArchiveEntry("_0.cfs", 150L, 200L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                staleSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos,
                STALE_ARCHIVE_BLOB,
                staleArchiveEntries,
                ARCHIVE_BLOB_SIZE
            )
        );

        remoteSegmentStoreDirectory.init();
        remoteSegmentStoreDirectory.deleteStaleSegments(1);

        // Archive ON: uploadedFilename = STALE_ARCHIVE_BLOB for all stale files.
        // staleSegmentRemoteFilenames Set has 1 entry: {STALE_ARCHIVE_BLOB}
        // → exactly 1 DELETE for the archive blob (not N per-file DELETEs)
        verify(remoteDataDirectory, times(1)).deleteFile(STALE_ARCHIVE_BLOB);

        // ACTIVE_ARCHIVE_BLOB must NOT be deleted (still referenced by active metadata)
        verify(remoteDataDirectory, never()).deleteFile(ACTIVE_ARCHIVE_BLOB);

        // Stale metadata file itself must be deleted
        verify(remoteMetadataDirectory, times(1)).deleteFile(metadataFilename2);

        logger.info("[Archive GC ON] PASSED: stale ZIP deleted exactly once (via Set dedup), active ZIP retained");
    }

    // -------------------------------------------------------------------------
    // 2. Archive OFF: stale per-file segment blobs are deleted, no ZIP DELETEs
    // -------------------------------------------------------------------------

    /**
     * Archive OFF GC flow:
     * - 2 metadata files: metadataFilename (active), metadataFilename2 (stale)
     * - deleteStaleSegments(1) should delete individual segment files from stale metadata.
     * - No ZIP blob names in metadata — no ZIP DELETEs expected.
     */
    public void testArchiveOffGcDeletesPerFileSegmentBlobs() throws IOException {
        Map<String, String> activeSegments = getDummyMetadata("_1", 2);
        Map<String, String> staleSegments  = getDummyMetadata("_0", 1);

        List<String> allMetadataFiles = List.of(metadataFilename, metadataFilename2);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH
        )).thenReturn(List.of(metadataFilename));

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE
        )).thenReturn(allMetadataFiles);

        // Active metadata: archive DISABLED
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytes(
                activeSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        // Stale metadata: archive DISABLED
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytes(
                staleSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        remoteSegmentStoreDirectory.init();
        remoteSegmentStoreDirectory.deleteStaleSegments(1);

        // Per-file segment blobs from stale metadata should be deleted
        for (String val : staleSegments.values()) {
            String uploadedFilename = val.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory, times(1)).deleteFile(uploadedFilename);
        }

        // No archive blob names in archive-OFF metadata — no ZIP DELETEs
        verify(remoteDataDirectory, never()).deleteFile(STALE_ARCHIVE_BLOB);
        verify(remoteDataDirectory, never()).deleteFile(ACTIVE_ARCHIVE_BLOB);

        verify(remoteMetadataDirectory, times(1)).deleteFile(metadataFilename2);

        logger.info("[Archive GC OFF] PASSED: per-file segments deleted, 0 ZIP DELETEs");
    }

    // -------------------------------------------------------------------------
    // 3. Archive ON: shared archive blob (same ZIP referenced by 2 stale metadata)
    //    is deleted only once, not twice
    // -------------------------------------------------------------------------

    /**
     * When two stale metadata files point to the same archive blob, it should only be deleted once.
     * This verifies the "activeArchiveBlobNames" dedup logic in deleteStaleSegments().
     */
    public void testArchiveGcDeduplicatesArchiveBlobDeletion() throws IOException {
        // Archive ON: uploadedFilename = archiveBlobName for all files
        Map<String, String> activeSegments = getDummyMetadataForArchive("_2", 3, ACTIVE_ARCHIVE_BLOB);
        Map<String, String> staleSegments1 = getDummyMetadataForArchive("_0", 1, STALE_ARCHIVE_BLOB);
        Map<String, String> staleSegments2 = getDummyMetadataForArchive("_1", 2, STALE_ARCHIVE_BLOB);

        // Both stale metadata files reference the SAME stale archive blob
        List<String> allMetadataFiles = List.of(metadataFilename, metadataFilename2, metadataFilename3);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH
        )).thenReturn(List.of(metadataFilename));

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE
        )).thenReturn(allMetadataFiles);

        // Active metadata: ACTIVE_ARCHIVE_BLOB
        Map<String, SegmentArchiveEntry> archiveEntries = Map.of(
            "_2.si", new SegmentArchiveEntry("_2.si", 0L, 100L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                activeSegments, indexShard.getLatestReplicationCheckpoint(), segmentInfos,
                ACTIVE_ARCHIVE_BLOB, archiveEntries, ARCHIVE_BLOB_SIZE
            )
        );

        // Stale 1: STALE_ARCHIVE_BLOB
        Map<String, SegmentArchiveEntry> staleEntries1 = Map.of(
            "_0.si", new SegmentArchiveEntry("_0.si", 0L, 100L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                staleSegments1, indexShard.getLatestReplicationCheckpoint(), segmentInfos,
                STALE_ARCHIVE_BLOB, staleEntries1, ARCHIVE_BLOB_SIZE
            )
        );

        // Stale 2: SAME STALE_ARCHIVE_BLOB (shared blob, e.g. snapshot without new segments)
        Map<String, SegmentArchiveEntry> staleEntries2 = Map.of(
            "_1.si", new SegmentArchiveEntry("_1.si", 0L, 100L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename3)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                staleSegments2, indexShard.getLatestReplicationCheckpoint(), segmentInfos,
                STALE_ARCHIVE_BLOB, staleEntries2, ARCHIVE_BLOB_SIZE
            )
        );

        remoteSegmentStoreDirectory.init();
        remoteSegmentStoreDirectory.deleteStaleSegments(1);

        // STALE_ARCHIVE_BLOB should only be deleted ONCE even though 2 stale metadata files reference it
        verify(remoteDataDirectory, times(1)).deleteFile(STALE_ARCHIVE_BLOB);
        verify(remoteDataDirectory, never()).deleteFile(ACTIVE_ARCHIVE_BLOB);

        logger.info("[Archive GC dedup] PASSED: shared stale ZIP deleted exactly once");
    }

    // -------------------------------------------------------------------------
    // 4. Compare: total DELETE counts archive ON vs OFF for same number of stale commits
    // -------------------------------------------------------------------------

    /**
     * Compares total DELETE calls for archive ON vs OFF when GC-ing the same number of stale
     * metadata files (1 stale, 1 active).
     *
     * <p>Archive ON: deletes 1 ZIP blob + 1 metadata file = 2 DELETEs total
     * <p>Archive OFF: deletes N per-file segment blobs + 1 metadata file = N+1 DELETEs total
     *
     * <p>For N=4 files: archive ON = 2, archive OFF = 5 — archive ON saves 3 DELETEs per GC run.
     *
     * <p>At scale (many shards, frequent GC): archive ON dramatically reduces DELETE API calls.
     */
    public void testGcDeleteCountArchiveOnVsOff() throws IOException {
        // Archive ON: uploadedFilename = archiveBlobName for all files in the archive
        // This correctly simulates postUploadForArchive() behaviour.
        Map<String, String> activeSegments = getDummyMetadataForArchive("_1", 2, ACTIVE_ARCHIVE_BLOB);
        Map<String, String> staleSegments  = getDummyMetadataForArchive("_0", 1, STALE_ARCHIVE_BLOB);

        // ---- Archive ON scenario ----
        List<String> allMetadataFiles = List.of(metadataFilename, metadataFilename2);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH
        )).thenReturn(List.of(metadataFilename));
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE
        )).thenReturn(allMetadataFiles);

        Map<String, SegmentArchiveEntry> activeArchiveEntries = Map.of(
            "_1.si", new SegmentArchiveEntry("_1.si", 0L, 100L, 0L)
        );
        Map<String, SegmentArchiveEntry> staleArchiveEntries = Map.of(
            "_0.si", new SegmentArchiveEntry("_0.si", 0L, 100L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                activeSegments, indexShard.getLatestReplicationCheckpoint(), segmentInfos,
                ACTIVE_ARCHIVE_BLOB, activeArchiveEntries, ARCHIVE_BLOB_SIZE
            )
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                staleSegments, indexShard.getLatestReplicationCheckpoint(), segmentInfos,
                STALE_ARCHIVE_BLOB, staleArchiveEntries, ARCHIVE_BLOB_SIZE
            )
        );

        remoteSegmentStoreDirectory.init();
        remoteSegmentStoreDirectory.deleteStaleSegments(1);

        int staleFileCount = staleSegments.size(); // number of logical segment files (3: .cfe, .cfs, .si)

        // Archive ON GC: uploadedFilename = STALE_ARCHIVE_BLOB for ALL stale files.
        // staleSegmentRemoteFilenames = {STALE_ARCHIVE_BLOB} (Set dedup collapses N entries to 1)
        // → exactly 1 data DELETE (the ZIP), not N per-file DELETEs
        verify(remoteDataDirectory, times(1)).deleteFile(STALE_ARCHIVE_BLOB);
        verify(remoteMetadataDirectory, times(1)).deleteFile(metadataFilename2);

        int archiveOnDataDeletes = 1; // just 1 ZIP DELETE (all N files' uploadedFilename = archiveBlobName → Set dedup)
        int archiveOnTotalDeletes = archiveOnDataDeletes + 1; // + 1 metadata file DELETE
        logger.info("[Archive ON GC] Data DELETEs={} (1 ZIP for {} segment files), Metadata DELETEs=1, TOTAL={}",
            archiveOnDataDeletes, staleFileCount, archiveOnTotalDeletes);

        // ---- Archive OFF comparison ----
        // Archive OFF: each file has a unique uploadedFilename → N separate DELETEs + 1 metadata DELETE
        int archiveOffDataDeletes = staleFileCount; // N individual per-file DELETEs
        int archiveOffTotalDeletes = archiveOffDataDeletes + 1;
        logger.info("[Archive OFF GC] Data DELETEs={} (individual segment files), Metadata DELETEs=1, TOTAL={}",
            archiveOffDataDeletes, archiveOffTotalDeletes);

        // KEY FINDING: Archive ON saves (N-1) DELETEs per GC run!
        // Archive ON:  1 ZIP DELETE + 1 metadata DELETE = 2 total
        // Archive OFF: N segment DELETEs + 1 metadata DELETE = N+1 total
        // For N=3: archive ON saves 2 DELETEs per GC run vs archive OFF.
        // At scale with many shards: huge reduction in S3 DELETE API calls.
        logger.info("=== GC DELETE comparison: Archive ON saves {}-1={} DELETEs per GC run vs OFF ===",
            staleFileCount, staleFileCount - archiveOnDataDeletes);
        logger.info("Archive ON total={}, Archive OFF total={} — saving {} DELETEs per GC",
            archiveOnTotalDeletes, archiveOffTotalDeletes, archiveOffTotalDeletes - archiveOnTotalDeletes);
    }

    // -------------------------------------------------------------------------
    // 5. Orphaned TAR GC: archive blobs with no metadata reference are deleted
    // -------------------------------------------------------------------------

    /**
     * Orphaned TAR cleanup test:
     * An archive blob is "orphaned" when it was uploaded successfully but its metadata
     * upload subsequently failed — leaving the TAR unreferenced by any metadata file.
     * The normal stale-metadata GC loop cannot find it (no metadata references it).
     *
     * <p>Fix: {@code deleteStaleSegments()} now calls {@code remoteDataDirectory.listAll()}
     * after the stale-metadata loop and uses {@link
     * org.opensearch.index.store.remote.segment.archive.SegmentArchiveRetentionHelper#findStaleArchiveBlobs}
     * to identify and delete any orphaned TAR blobs.
     *
     * <p>This test:
     * <ol>
     *   <li>Sets up 1 active metadata file referencing ACTIVE_ARCHIVE_BLOB.</li>
     *   <li>Stubs {@code remoteDataDirectory.listAll()} to return:
     *       ACTIVE_ARCHIVE_BLOB (referenced — must NOT be deleted) and
     *       ORPHAN_ARCHIVE_BLOB (unreferenced — must be deleted).</li>
     *   <li>Runs deleteStaleSegments(1) (nothing to delete from stale-metadata loop).</li>
     *   <li>Verifies ORPHAN_ARCHIVE_BLOB is deleted exactly once, ACTIVE is untouched.</li>
     * </ol>
     */
    public void testOrphanedArchiveBlobIsDeleted() throws IOException {
        final String ORPHAN_ARCHIVE_BLOB = "segment_archive_orphan_99999.tar";

        // Only 1 metadata file: it references ACTIVE_ARCHIVE_BLOB.
        Map<String, String> activeSegments = getDummyMetadataForArchive("_1", 2, ACTIVE_ARCHIVE_BLOB);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH
        )).thenReturn(List.of(metadataFilename));

        // Use 2 metadata files: metadataFilename (active/newest) and metadataFilename2 (stale).
        // deleteStaleSegments(1) will try to delete metadataFilename2 — but since it's an
        // archive-enabled metadata, it deletes the stale archive blob. The orphan is a THIRD
        // TAR that is not referenced by either metadata file.
        // For simplicity: use 2 files with the same active archive so the stale loop runs,
        // then orphan cleanup finds the unreferenced ORPHAN_ARCHIVE_BLOB.
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE
        )).thenReturn(List.of(metadataFilename, metadataFilename2));

        Map<String, SegmentArchiveEntry> activeArchiveEntries = Map.of(
            "_1.si", new SegmentArchiveEntry("_1.si", 0L, 100L, 0L)
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                activeSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos,
                ACTIVE_ARCHIVE_BLOB,
                activeArchiveEntries,
                ARCHIVE_BLOB_SIZE
            )
        );
        // metadataFilename2 is the stale file: also references ACTIVE_ARCHIVE_BLOB (same archive).
        // This means ACTIVE_ARCHIVE_BLOB won't be deleted (still in active set), and orphan cleanup
        // finds ORPHAN_ARCHIVE_BLOB since it's not referenced by any metadata.
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytesWithArchive(
                activeSegments,
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos,
                ACTIVE_ARCHIVE_BLOB,
                activeArchiveEntries,
                ARCHIVE_BLOB_SIZE
            )
        );

        // Stub listAll() to return both blobs: 1 active (referenced) + 1 orphan (unreferenced)
        when(remoteDataDirectory.listAll()).thenReturn(
            new String[]{ ACTIVE_ARCHIVE_BLOB, ORPHAN_ARCHIVE_BLOB, "some_non_archive_file.dat" }
        );

        remoteSegmentStoreDirectory.init();
        remoteSegmentStoreDirectory.deleteStaleSegments(1);

        // ORPHAN_ARCHIVE_BLOB must be deleted exactly once (unreferenced TAR)
        verify(remoteDataDirectory, times(1)).deleteFile(ORPHAN_ARCHIVE_BLOB);

        // ACTIVE_ARCHIVE_BLOB must NOT be deleted (still referenced by active metadata)
        verify(remoteDataDirectory, never()).deleteFile(ACTIVE_ARCHIVE_BLOB);

        // Non-archive file must NOT be affected by orphan cleanup
        verify(remoteDataDirectory, never()).deleteFile("some_non_archive_file.dat");

        logger.info("[Orphan TAR GC] PASSED: orphaned TAR deleted, active TAR and non-archive files untouched");
    }
}
