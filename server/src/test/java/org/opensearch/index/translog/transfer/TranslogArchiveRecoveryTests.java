/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.transfer.archive.ArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.ArchiveCommentFormat;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TranslogArchiveRecoveryTests extends OpenSearchTestCase {

    public void testExtractEocdComment() throws IOException {
        // Build a real ZIP with comment
        String tlogPath = "indexUUID/0/1/translog-5.tlog";
        String ckpPath = "indexUUID/0/1/translog-5.ckp";
        byte[] tlogContent = "translog data".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "ckp data".getBytes(StandardCharsets.UTF_8);

        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(tlogPath, tlogContent),
            ArchiveBuilder.fromBytes(ckpPath, ckpContent)
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Extract comment
        String comment = TranslogArchiveRecovery.extractEocdComment(zipBytes);
        assertNotNull("EOCD comment should be found", comment);

        // Parse comment and verify entries
        List<ArchiveIndexEntry> parsed = ArchiveCommentFormat.parse(comment);
        assertFalse("Parsed entries should not be empty", parsed.isEmpty());

        // Find shard 0's entry
        ArchiveIndexEntry entry = parsed.stream().filter(e -> e.getShardId() == 0 && e.getGeneration() == 5).findFirst().orElse(null);
        assertNotNull("Should find shard 0 gen 5 entry", entry);
        assertEquals(1L, entry.getPrimaryTerm());

        // Verify offsets allow extraction
        byte[] extractedTlog = Arrays.copyOfRange(
            zipBytes,
            (int) entry.getTlogOffset(),
            (int) (entry.getTlogOffset() + entry.getTlogLength())
        );
        assertArrayEquals(tlogContent, extractedTlog);
        byte[] extractedCkp = Arrays.copyOfRange(zipBytes, (int) entry.getCkpOffset(), (int) (entry.getCkpOffset() + entry.getCkpLength()));
        assertArrayEquals(ckpContent, extractedCkp);
    }

    public void testExtractEocdCommentFromEmptyZip() throws IOException {
        // Build ZIP with empty entries list — no comment
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, Collections.emptyList());
        byte[] zipBytes = out.toByteArray();

        String comment = TranslogArchiveRecovery.extractEocdComment(zipBytes);
        // Empty comment or null is expected
        assertTrue("Empty ZIP should have null or empty comment", comment == null || comment.isEmpty());
    }

    public void testBinarySearchForGeneration() throws IOException {
        // Build 3 ZIPs with different generations
        byte[] zip1 = buildZip(new int[] { 0, 1 }, new long[] { 3, 3 }, new long[] { 1, 1 });
        byte[] zip2 = buildZip(new int[] { 0, 1 }, new long[] { 4, 4 }, new long[] { 1, 1 });
        byte[] zip3 = buildZip(new int[] { 0 }, new long[] { 5 }, new long[] { 1 });

        BlobPath path = new BlobPath().add("test");
        List<TranslogArchiveRecovery.ZipRef> zips = Arrays.asList(
            new TranslogArchiveRecovery.ZipRef(path, "zip1.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip2.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip3.zip")
        );

        // Mock transfer service
        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip1.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip1));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip2.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip2));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip3.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip3));

        // Search for shard 0, gen 4 → should find in zip2
        TranslogArchiveRecovery.ZipEntryLocation loc = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 4);
        assertNotNull("Should find gen 4 for shard 0", loc);
        assertEquals("zip2.zip", loc.blobName);
        assertEquals(4L, loc.entry.getGeneration());

        // Re-stub for fresh InputStreams
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip1.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip1));
        TranslogArchiveRecovery.ZipEntryLocation loc2 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 1, 3);
        assertNotNull("Should find gen 3 for shard 1", loc2);
        assertEquals("zip1.zip", loc2.blobName);

        // Search for shard 0, gen 5
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip3.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip3));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip2.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip2));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip1.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip1));
        TranslogArchiveRecovery.ZipEntryLocation loc3 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 5);
        assertNotNull("Should find gen 5 for shard 0", loc3);
        assertEquals("zip3.zip", loc3.blobName);

        // Search for non-existent gen
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip1.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip1));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip2.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip2));
        org.mockito.Mockito.when(transferService.downloadBlob(any(), org.mockito.ArgumentMatchers.eq("zip3.zip")))
            .thenReturn(new java.io.ByteArrayInputStream(zip3));
        TranslogArchiveRecovery.ZipEntryLocation loc4 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 99);
        assertNull("Should not find gen 99", loc4);
    }

    private byte[] buildZip(int[] shardIds, long[] generations, long[] primaryTerms) throws IOException {
        List<ArchiveBuilder.ArchiveBuildEntry> entries = new java.util.ArrayList<>();
        for (int i = 0; i < shardIds.length; i++) {
            String prefix = "indexUUID/" + shardIds[i] + "/" + primaryTerms[i] + "/";
            String tlogPath = prefix + "translog-" + generations[i] + ".tlog";
            String ckpPath = prefix + "translog-" + generations[i] + ".ckp";
            entries.add(
                ArchiveBuilder.fromBytes(tlogPath, ("tlog-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8))
            );
            entries.add(ArchiveBuilder.fromBytes(ckpPath, ("ckp-" + shardIds[i] + "-" + generations[i]).getBytes(StandardCharsets.UTF_8)));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        return out.toByteArray();
    }

    /**
     * Recovery with multiple ZIPs: generation range spans multiple ZIPs requiring binary search across ZIP boundaries.
     * Builds 5 ZIPs where generations are spread across them: zip1(gen1-2), zip2(gen3-4), zip3(gen5-6),
     * zip4(gen7-8), zip5(gen9-10). Binary search must locate gen across boundaries.
     */
    public void testRecoveryWithMultipleZipsSpanningBoundaries() throws IOException {
        byte[] zip1 = buildZip(new int[] { 0, 0 }, new long[] { 1, 2 }, new long[] { 1, 1 });
        byte[] zip2 = buildZip(new int[] { 0, 0 }, new long[] { 3, 4 }, new long[] { 1, 1 });
        byte[] zip3 = buildZip(new int[] { 0, 0 }, new long[] { 5, 6 }, new long[] { 1, 1 });
        byte[] zip4 = buildZip(new int[] { 0, 0 }, new long[] { 7, 8 }, new long[] { 1, 1 });
        byte[] zip5 = buildZip(new int[] { 0, 0 }, new long[] { 9, 10 }, new long[] { 1, 1 });

        BlobPath path = new BlobPath().add("test");
        List<TranslogArchiveRecovery.ZipRef> zips = Arrays.asList(
            new TranslogArchiveRecovery.ZipRef(path, "zip1.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip2.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip3.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip4.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip5.zip")
        );

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);

        // Helper to re-stub all ZIPs (InputStreams are consumed after each call)
        java.lang.Runnable stubAll = () -> {
            try {
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip1.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip1));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip2.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip2));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip3.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip3));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip4.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip4));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip5.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip5));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        // Test finding generations at ZIP boundaries
        // Gen 1 → should be in zip1
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc1 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 1);
        assertNotNull("Should find gen 1 for shard 0", loc1);
        assertEquals("zip1.zip", loc1.blobName);

        // Gen 4 → should be in zip2 (boundary: last gen in zip2)
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc4 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 4);
        assertNotNull("Should find gen 4 for shard 0", loc4);
        assertEquals("zip2.zip", loc4.blobName);

        // Gen 5 → should be in zip3 (boundary: first gen in zip3)
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc5 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 5);
        assertNotNull("Should find gen 5 for shard 0", loc5);
        assertEquals("zip3.zip", loc5.blobName);

        // Gen 10 → should be in zip5 (last ZIP, last gen)
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc10 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, 10);
        assertNotNull("Should find gen 10 for shard 0", loc10);
        assertEquals("zip5.zip", loc10.blobName);

        // Gen 11 → does not exist
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation locMissing = TranslogArchiveRecovery.binarySearchForGeneration(
            transferService,
            zips,
            0,
            11
        );
        assertNull("Should not find gen 11", locMissing);
    }

    /**
     * Recovery when shard absent from some ZIPs: binary search handles gaps where a shard
     * had no data during some batches. Shard 1 only appears in zip2 and zip4.
     */
    public void testRecoveryWhenShardAbsentFromSomeZips() throws IOException {
        // zip1: only shard 0
        byte[] zip1 = buildZip(new int[] { 0 }, new long[] { 1 }, new long[] { 1 });
        // zip2: both shards
        byte[] zip2 = buildZip(new int[] { 0, 1 }, new long[] { 2, 5 }, new long[] { 1, 1 });
        // zip3: only shard 0
        byte[] zip3 = buildZip(new int[] { 0 }, new long[] { 3 }, new long[] { 1 });
        // zip4: both shards
        byte[] zip4 = buildZip(new int[] { 0, 1 }, new long[] { 4, 6 }, new long[] { 1, 1 });
        // zip5: only shard 0
        byte[] zip5 = buildZip(new int[] { 0 }, new long[] { 5 }, new long[] { 1 });

        BlobPath path = new BlobPath().add("test");
        List<TranslogArchiveRecovery.ZipRef> zips = Arrays.asList(
            new TranslogArchiveRecovery.ZipRef(path, "zip1.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip2.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip3.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip4.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip5.zip")
        );

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);
        java.lang.Runnable stubAll = () -> {
            try {
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip1.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip1));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip2.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip2));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip3.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip3));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip4.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip4));
                org.mockito.Mockito.when(
                    transferService.downloadBlob(
                        org.mockito.ArgumentMatchers.any(BlobPath.class),
                        org.mockito.ArgumentMatchers.eq("zip5.zip")
                    )
                ).thenReturn(new java.io.ByteArrayInputStream(zip5));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        // Shard 1 gen 5 → should be in zip2 (shard 1 is absent from zip1, zip3, zip5)
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc1 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 1, 5);
        assertNotNull("Should find shard 1 gen 5 despite gaps", loc1);
        assertEquals("zip2.zip", loc1.blobName);
        assertEquals(5L, loc1.entry.getGeneration());

        // Shard 1 gen 6 → should be in zip4
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation loc2 = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 1, 6);
        assertNotNull("Should find shard 1 gen 6 despite gaps", loc2);
        assertEquals("zip4.zip", loc2.blobName);
        assertEquals(6L, loc2.entry.getGeneration());

        // Shard 1 gen 99 → does not exist anywhere
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation locMissing = TranslogArchiveRecovery.binarySearchForGeneration(
            transferService,
            zips,
            1,
            99
        );
        assertNull("Should not find non-existent gen 99 for shard 1", locMissing);

        // Shard 2 → never appears in any ZIP
        stubAll.run();
        TranslogArchiveRecovery.ZipEntryLocation locNonexistentShard = TranslogArchiveRecovery.binarySearchForGeneration(
            transferService,
            zips,
            2,
            1
        );
        assertNull("Should not find shard 2 which never appears", locNonexistentShard);
    }

    /**
     * Integration test: full upload → recovery cycle with real ZIP bytes.
     * Builds a ZIP with comment (as the coordinator would), then extracts files using EOCD comment
     * parsing and verifies the recovered content matches the original.
     */
    public void testFullUploadToRecoveryCycleWithRealZip() throws IOException {
        // Simulate what the coordinator uploads: 2 shards, each with tlog+ckp
        String indexUUID = "test-idx-uuid";
        byte[] shard0Tlog = "shard0 translog gen 5 content".getBytes(StandardCharsets.UTF_8);
        byte[] shard0Ckp = "shard0 checkpoint gen 5".getBytes(StandardCharsets.UTF_8);
        byte[] shard1Tlog = "shard1 translog gen 3 content here".getBytes(StandardCharsets.UTF_8);
        byte[] shard1Ckp = "shard1 checkpoint gen 3".getBytes(StandardCharsets.UTF_8);

        String shard0TlogPath = indexUUID + "/0/1/translog-5.tlog";
        String shard0CkpPath = indexUUID + "/0/1/translog-5.ckp";
        String shard1TlogPath = indexUUID + "/1/2/translog-3.tlog";
        String shard1CkpPath = indexUUID + "/1/2/translog-3.ckp";

        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(shard0TlogPath, shard0Tlog),
            ArchiveBuilder.fromBytes(shard0CkpPath, shard0Ckp),
            ArchiveBuilder.fromBytes(shard1TlogPath, shard1Tlog),
            ArchiveBuilder.fromBytes(shard1CkpPath, shard1Ckp)
        );

        // Step 1: Build ZIP with comment (upload side)
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        byte[] zipBytes = out.toByteArray();

        // Step 2: Extract EOCD comment (recovery side)
        String comment = TranslogArchiveRecovery.extractEocdComment(zipBytes);
        assertNotNull("EOCD comment must be present", comment);

        // Step 3: Parse comment to get index entries
        List<ArchiveIndexEntry> indexEntries = ArchiveCommentFormat.parse(comment);
        assertFalse("Parsed entries should not be empty", indexEntries.isEmpty());

        // Step 4: Recover shard 0 gen 5
        ArchiveIndexEntry shard0Entry = indexEntries.stream()
            .filter(e -> e.getShardId() == 0 && e.getGeneration() == 5)
            .findFirst()
            .orElse(null);
        assertNotNull("Should find shard 0 gen 5", shard0Entry);
        assertEquals(1L, shard0Entry.getPrimaryTerm());

        // Extract tlog+ckp content via offsets (simulating range-read)
        byte[] recoveredTlog0 = Arrays.copyOfRange(
            zipBytes,
            (int) shard0Entry.getTlogOffset(),
            (int) (shard0Entry.getTlogOffset() + shard0Entry.getTlogLength())
        );
        byte[] recoveredCkp0 = Arrays.copyOfRange(
            zipBytes,
            (int) shard0Entry.getCkpOffset(),
            (int) (shard0Entry.getCkpOffset() + shard0Entry.getCkpLength())
        );
        assertArrayEquals("Shard 0 tlog content mismatch", shard0Tlog, recoveredTlog0);
        assertArrayEquals("Shard 0 ckp content mismatch", shard0Ckp, recoveredCkp0);

        // Step 5: Recover shard 1 gen 3
        ArchiveIndexEntry shard1Entry = indexEntries.stream()
            .filter(e -> e.getShardId() == 1 && e.getGeneration() == 3)
            .findFirst()
            .orElse(null);
        assertNotNull("Should find shard 1 gen 3", shard1Entry);
        assertEquals(2L, shard1Entry.getPrimaryTerm());

        byte[] recoveredTlog1 = Arrays.copyOfRange(
            zipBytes,
            (int) shard1Entry.getTlogOffset(),
            (int) (shard1Entry.getTlogOffset() + shard1Entry.getTlogLength())
        );
        byte[] recoveredCkp1 = Arrays.copyOfRange(
            zipBytes,
            (int) shard1Entry.getCkpOffset(),
            (int) (shard1Entry.getCkpOffset() + shard1Entry.getCkpLength())
        );
        assertArrayEquals("Shard 1 tlog content mismatch", shard1Tlog, recoveredTlog1);
        assertArrayEquals("Shard 1 ckp content mismatch", shard1Ckp, recoveredCkp1);
    }

    /**
     * Simulate the real IT scenario: single shard uploads one generation per ZIP (one ZIP per sync).
     * Recovery must find ALL consecutive generations (5 through 10) across 6 separate ZIPs.
     * This is the exact pattern that failed in the integration test:
     *   "translog file doesn't exist with generation: 9 recovering from: 5 checkpoint: 10"
     */
    public void testRecoverAllConsecutiveGenerationsAcrossManyZips() throws IOException {
        // Each ZIP has exactly one generation for shard 0 (one per sync)
        byte[] zip1 = buildZip(new int[] { 0 }, new long[] { 5 }, new long[] { 1 });
        byte[] zip2 = buildZip(new int[] { 0 }, new long[] { 6 }, new long[] { 1 });
        byte[] zip3 = buildZip(new int[] { 0 }, new long[] { 7 }, new long[] { 1 });
        byte[] zip4 = buildZip(new int[] { 0 }, new long[] { 8 }, new long[] { 1 });
        byte[] zip5 = buildZip(new int[] { 0 }, new long[] { 9 }, new long[] { 1 });
        byte[] zip6 = buildZip(new int[] { 0 }, new long[] { 10 }, new long[] { 1 });

        BlobPath path = new BlobPath().add("test");
        List<TranslogArchiveRecovery.ZipRef> zips = Arrays.asList(
            new TranslogArchiveRecovery.ZipRef(path, "20260101000001000.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "20260101000002000.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "20260101000003000.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "20260101000004000.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "20260101000005000.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "20260101000006000.zip")
        );

        byte[][] zipBytes = { zip1, zip2, zip3, zip4, zip5, zip6 };
        String[] zipNames = {
            "20260101000001000.zip",
            "20260101000002000.zip",
            "20260101000003000.zip",
            "20260101000004000.zip",
            "20260101000005000.zip",
            "20260101000006000.zip" };

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);

        // Stub all ZIPs — use thenAnswer to return fresh InputStreams each time
        for (int i = 0; i < zipNames.length; i++) {
            final byte[] bytes = zipBytes[i];
            org.mockito.Mockito.when(
                transferService.downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq(zipNames[i]))
            ).thenAnswer(inv -> new java.io.ByteArrayInputStream(bytes));
        }

        // Verify binary search finds EVERY generation from 5 to 10
        for (long gen = 5; gen <= 10; gen++) {
            TranslogArchiveRecovery.ZipEntryLocation loc = TranslogArchiveRecovery.binarySearchForGeneration(transferService, zips, 0, gen);
            assertNotNull("Should find gen " + gen + " for shard 0", loc);
            assertEquals("Gen " + gen + " should be in correct ZIP", gen, loc.entry.getGeneration());
            // Each gen is in its own ZIP: gen5→zip1, gen6→zip2, ... gen10→zip6
            assertEquals("Gen " + gen + " should be in ZIP " + (gen - 4), zipNames[(int) (gen - 5)], loc.blobName);
        }
    }

    /**
     * Like above but with multiple shards per ZIP: each ZIP has one gen per shard.
     * Ensures all shards' generations are found.
     */
    public void testRecoverConsecutiveGenerationsMultipleShardsPerZip() throws IOException {
        // Each ZIP: shard 0 and shard 1 both get one gen (same gen number, different shard)
        byte[] zip1 = buildZip(new int[] { 0, 1 }, new long[] { 5, 5 }, new long[] { 1, 1 });
        byte[] zip2 = buildZip(new int[] { 0, 1 }, new long[] { 6, 6 }, new long[] { 1, 1 });
        byte[] zip3 = buildZip(new int[] { 0, 1 }, new long[] { 7, 7 }, new long[] { 1, 1 });

        BlobPath path = new BlobPath().add("test");
        List<TranslogArchiveRecovery.ZipRef> zips = Arrays.asList(
            new TranslogArchiveRecovery.ZipRef(path, "zip1.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip2.zip"),
            new TranslogArchiveRecovery.ZipRef(path, "zip3.zip")
        );

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);
        org.mockito.Mockito.when(
            transferService.downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("zip1.zip"))
        ).thenAnswer(inv -> new java.io.ByteArrayInputStream(zip1));
        org.mockito.Mockito.when(
            transferService.downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("zip2.zip"))
        ).thenAnswer(inv -> new java.io.ByteArrayInputStream(zip2));
        org.mockito.Mockito.when(
            transferService.downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("zip3.zip"))
        ).thenAnswer(inv -> new java.io.ByteArrayInputStream(zip3));

        // Verify all gens found for both shards
        for (int shard = 0; shard <= 1; shard++) {
            for (long gen = 5; gen <= 7; gen++) {
                TranslogArchiveRecovery.ZipEntryLocation loc = TranslogArchiveRecovery.binarySearchForGeneration(
                    transferService,
                    zips,
                    shard,
                    gen
                );
                assertNotNull("Should find shard " + shard + " gen " + gen, loc);
                assertEquals(gen, loc.entry.getGeneration());
            }
        }
    }

    /**
     * Verifies that readZipComment uses range-read (downloadBlob with offset/length)
     * instead of downloading the entire ZIP blob when size is known.
     */
    public void testReadZipCommentUsesRangeRead() throws IOException {
        byte[] zipBytes = buildZip(new int[] { 0 }, new long[] { 5 }, new long[] { 1 });
        long zipSize = zipBytes.length;

        BlobPath path = new BlobPath().add("test");
        TranslogArchiveRecovery.ZipRef zip = new TranslogArchiveRecovery.ZipRef(path, "test.zip", zipSize);

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);

        int tailLen = (int) Math.min(zipSize, TranslogArchiveRecovery.EOCD_TAIL_READ_SIZE);
        long tailOffset = zipSize - tailLen;
        byte[] tail = Arrays.copyOfRange(zipBytes, (int) tailOffset, (int) zipSize);
        org.mockito.Mockito.when(
            transferService.downloadBlob(
                org.mockito.ArgumentMatchers.any(BlobPath.class),
                org.mockito.ArgumentMatchers.eq("test.zip"),
                org.mockito.ArgumentMatchers.eq(tailOffset),
                org.mockito.ArgumentMatchers.eq((long) tailLen)
            )
        ).thenReturn(new java.io.ByteArrayInputStream(tail));

        List<ArchiveIndexEntry> entries = TranslogArchiveRecovery.readZipComment(transferService, zip);

        // Verify range-read was called
        org.mockito.Mockito.verify(transferService)
            .downloadBlob(
                org.mockito.ArgumentMatchers.any(BlobPath.class),
                org.mockito.ArgumentMatchers.eq("test.zip"),
                org.mockito.ArgumentMatchers.eq(tailOffset),
                org.mockito.ArgumentMatchers.eq((long) tailLen)
            );
        // Verify full download was NOT called
        org.mockito.Mockito.verify(transferService, org.mockito.Mockito.never())
            .downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("test.zip"));

        assertFalse("Should parse entries from range-read tail", entries.isEmpty());
        assertEquals(0, entries.get(0).getShardId());
        assertEquals(5L, entries.get(0).getGeneration());
    }

    /**
     * Verifies readZipComment handles ZIPs smaller than EOCD_TAIL_READ_SIZE
     * by reading the entire blob via range-read (offset=0, length=size).
     */
    public void testReadZipCommentSmallZip() throws IOException {
        byte[] zipBytes = buildZip(new int[] { 0 }, new long[] { 1 }, new long[] { 1 });
        long zipSize = zipBytes.length;
        assertTrue("Test ZIP should be smaller than tail read size", zipSize < TranslogArchiveRecovery.EOCD_TAIL_READ_SIZE);

        BlobPath path = new BlobPath().add("test");
        TranslogArchiveRecovery.ZipRef zip = new TranslogArchiveRecovery.ZipRef(path, "small.zip", zipSize);

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);
        org.mockito.Mockito.when(
            transferService.downloadBlob(
                org.mockito.ArgumentMatchers.any(BlobPath.class),
                org.mockito.ArgumentMatchers.eq("small.zip"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq(zipSize)
            )
        ).thenReturn(new java.io.ByteArrayInputStream(zipBytes));

        List<ArchiveIndexEntry> entries = TranslogArchiveRecovery.readZipComment(transferService, zip);
        assertFalse("Should parse entries from small ZIP", entries.isEmpty());
    }

    /**
     * Verifies readZipComment falls back to full download when size is unknown (-1).
     */
    public void testReadZipCommentFallsBackWhenSizeUnknown() throws IOException {
        byte[] zipBytes = buildZip(new int[] { 0 }, new long[] { 7 }, new long[] { 1 });

        BlobPath path = new BlobPath().add("test");
        // No size provided → uses 2-arg constructor, size = -1
        TranslogArchiveRecovery.ZipRef zip = new TranslogArchiveRecovery.ZipRef(path, "unknown.zip");

        TransferService transferService = org.mockito.Mockito.mock(TransferService.class);
        org.mockito.Mockito.when(
            transferService.downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("unknown.zip"))
        ).thenReturn(new java.io.ByteArrayInputStream(zipBytes));

        List<ArchiveIndexEntry> entries = TranslogArchiveRecovery.readZipComment(transferService, zip);

        // Verify full download WAS called (fallback)
        org.mockito.Mockito.verify(transferService)
            .downloadBlob(org.mockito.ArgumentMatchers.any(BlobPath.class), org.mockito.ArgumentMatchers.eq("unknown.zip"));

        assertFalse("Should still parse entries via fallback", entries.isEmpty());
        assertEquals(7L, entries.get(0).getGeneration());
    }

    @SuppressWarnings("unchecked")
    private static Iterable<String> any() {
        return org.mockito.ArgumentMatchers.any();
    }
}
