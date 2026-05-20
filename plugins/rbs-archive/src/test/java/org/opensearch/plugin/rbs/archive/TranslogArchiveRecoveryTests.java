/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TranslogArchiveRecovery}.
 *
 * <p>Uses a mock {@link TransferService} to simulate TAR blobs in the hierarchical
 * txlog path ({@code txlog/yyyyMMdd/HHmm/*.tar}).
 */
public class TranslogArchiveRecoveryTests extends OpenSearchTestCase {

    // ── helpers ───────────────────────────────────────────────────────────────

    private static final String INDEX_UUID = "test-index-uuid-abc";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1L;

    /**
     * Builds a TAR blob (using TarArchiveBuilder) containing the given generations for one shard.
     * Returns the raw bytes of the archive.
     */
    private static byte[] buildTarWithGenerations(String indexUUID, int shardId, long primaryTerm, long... generations) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new java.util.ArrayList<>();

        for (long gen : generations) {
            byte[] tlog = ("tlog-" + gen).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] ckp = ("ckp-" + gen).getBytes(java.nio.charset.StandardCharsets.UTF_8);

            String tlogName = indexUUID + "/" + shardId + "/" + primaryTerm + "/translog-" + gen + ".tlog";
            String ckpName = indexUUID + "/" + shardId + "/" + primaryTerm + "/translog-" + gen + ".ckp";
            entries.add(TarArchiveBuilder.fromBytes(tlogName, tlog));
            entries.add(TarArchiveBuilder.fromBytes(ckpName, ckp));
        }

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        TarArchiveBuilder.build(bos, layout, entries);
        return bos.toByteArray();
    }

    /**
     * Creates a mock TransferService that serves TAR blobs from the given map.
     * Key format: "dayDir/minuteDir/blobName"
     *
     * <p>The mock supports:
     * <ul>
     *   <li>{@code listFolders(txlogRoot)} → day dirs</li>
     *   <li>{@code listFolders(dayPath)} → minute dirs</li>
     *   <li>{@code listAllInSortedOrder(minutePath, ...)} → blob metadata</li>
     *   <li>{@code downloadBlob(minutePath, blobName, offset, length)} → range of TAR bytes</li>
     * </ul>
     */
    private static TransferService mockTransferService(
        BlobPath basePath,
        Map<String, byte[]> tarBlobs  // key = "dayDir/minuteDir/blobName"
    ) throws IOException {
        TransferService ts = mock(TransferService.class);

        // Group blobs by dayDir and minuteDir
        Map<String, Set<String>> dayToMinutes = new HashMap<>();
        Map<String, Map<String, byte[]>> minuteToBlobs = new HashMap<>();

        for (String key : tarBlobs.keySet()) {
            String[] parts = key.split("/", 3);
            String dayDir = parts[0];
            String minuteDir = parts[1];
            String blobName = parts[2];

            dayToMinutes.computeIfAbsent(dayDir, k -> new java.util.LinkedHashSet<>()).add(minuteDir);
            String minuteKey = dayDir + "/" + minuteDir;
            minuteToBlobs.computeIfAbsent(minuteKey, k -> new HashMap<>()).put(blobName, tarBlobs.get(key));
        }

        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(basePath);

        // listFolders(txlogRoot) → day dirs
        when(ts.listFolders(eq(txlogRoot))).thenReturn(dayToMinutes.keySet());

        for (Map.Entry<String, Set<String>> dayEntry : dayToMinutes.entrySet()) {
            String dayDir = dayEntry.getKey();
            BlobPath dayPath = txlogRoot.add(dayDir);

            // listFolders(dayPath) → minute dirs
            when(ts.listFolders(eq(dayPath))).thenReturn(dayEntry.getValue());

            for (String minuteDir : dayEntry.getValue()) {
                String minuteKey = dayDir + "/" + minuteDir;
                BlobPath minutePath = dayPath.add(minuteDir);
                Map<String, byte[]> blobs = minuteToBlobs.getOrDefault(minuteKey, Collections.emptyMap());

                // listAllInSortedOrder(minutePath, ...) → sorted BlobMetadata list
                List<BlobMetadata> blobMetas = new java.util.ArrayList<>();
                for (Map.Entry<String, byte[]> blobEntry : blobs.entrySet()) {
                    String name = blobEntry.getKey();
                    long size = blobEntry.getValue().length;
                    blobMetas.add(new PlainBlobMetadata(name, size));
                }
                blobMetas.sort((a, b) -> a.name().compareTo(b.name()));

                final List<BlobMetadata> finalMetas = blobMetas;
                doAnswer(inv -> {
                    @SuppressWarnings("unchecked")
                    org.opensearch.core.action.ActionListener<List<BlobMetadata>> listener = inv.getArgument(3);
                    listener.onResponse(finalMetas);
                    return null;
                }).when(ts).listAllInSortedOrder(eq(minutePath), anyString(), anyInt(), any());

                // downloadBlob(minutePath, blobName, offset, length) → range bytes
                for (Map.Entry<String, byte[]> blobEntry : blobs.entrySet()) {
                    String blobName = blobEntry.getKey();
                    byte[] data = blobEntry.getValue();
                    when(ts.downloadBlob(eq(minutePath), eq(blobName), anyLong(), anyLong())).thenAnswer(inv -> {
                        long offset = inv.getArgument(2);
                        long length = inv.getArgument(3);
                        int start = (int) offset;
                        int len = (int) Math.min(length, data.length - start);
                        return new ByteArrayInputStream(data, start, len);
                    });
                    // full blob download
                    when(ts.downloadBlob(eq(minutePath), eq(blobName))).thenReturn(new ByteArrayInputStream(data));
                }
            }
        }

        return ts;
    }

    private static BlobPath basePath() {
        return BlobPath.cleanPath().add("test-repo").add("indices").add(INDEX_UUID);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /** Single TAR with one generation — hierarchical recovery finds and extracts it. */
    public void testRecoversSingleGenerationFromHierarchicalPath() throws IOException {
        long gen = 5L;
        byte[] tar = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, gen);

        Instant uploadTime = Instant.parse("2026-05-19T10:30:00Z");
        String dayDir = TranslogArchivePathHelper.dayDir(uploadTime);
        String minuteDir = TranslogArchivePathHelper.minuteDir(uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, "node1");

        Map<String, byte[]> blobs = Map.of(dayDir + "/" + minuteDir + "/" + blobName, tar);
        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        Path location = createTempDir();
        Instant startFrom = uploadTime.minusSeconds(60); // scan from 1 min before upload

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            gen,
            gen,
            location,
            startFrom,
            null
        );

        assertTrue("Should find generation " + gen, found);
        Path tlogFile = location.resolve(Translog.getFilename(gen));
        Path ckpFile = location.resolve(Translog.getCommitCheckpointFileName(gen));
        assertTrue("tlog file should exist", Files.exists(tlogFile));
        assertTrue("ckp file should exist", Files.exists(ckpFile));
        assertEquals("tlog-" + gen, new String(Files.readAllBytes(tlogFile)));
        assertEquals("ckp-" + gen, new String(Files.readAllBytes(ckpFile)));
    }

    /** TAR with multiple generations — only the requested range is extracted. */
    public void testRecoversGenerationRange() throws IOException {
        byte[] tar = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, 10L, 11L, 12L);

        Instant uploadTime = Instant.parse("2026-05-19T10:30:00Z");
        String dayDir = TranslogArchivePathHelper.dayDir(uploadTime);
        String minuteDir = TranslogArchivePathHelper.minuteDir(uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, "node1");

        Map<String, byte[]> blobs = Map.of(dayDir + "/" + minuteDir + "/" + blobName, tar);
        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        Path location = createTempDir();
        Instant startFrom = uploadTime.minusSeconds(60);

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            10L,
            12L,
            location,
            startFrom,
            null
        );

        assertTrue("Should find all generations in range", found);
        for (long gen = 10L; gen <= 12L; gen++) {
            assertTrue("tlog-" + gen + " should exist", Files.exists(location.resolve(Translog.getFilename(gen))));
            assertTrue("ckp-" + gen + " should exist", Files.exists(location.resolve(Translog.getCommitCheckpointFileName(gen))));
        }
    }

    /** No TAR blobs → returns false (signals core to use per-file fallback). */
    public void testReturnsFalseWhenNoTarsExist() throws IOException {
        BlobPath base = basePath();
        TransferService ts = mock(TransferService.class);
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(base);
        when(ts.listFolders(eq(txlogRoot))).thenReturn(Collections.emptySet());

        Path location = createTempDir();
        Instant startFrom = Instant.now().minusSeconds(120);

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            1L,
            1L,
            location,
            startFrom,
            null
        );

        assertFalse("Should return false when no TAR archive exists", found);
    }

    /** Generation not in any TAR → returns false for that generation. */
    public void testReturnsFalseWhenGenerationMissing() throws IOException {
        long presentGen = 5L;
        long missingGen = 99L;
        byte[] tar = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, presentGen);

        Instant uploadTime = Instant.parse("2026-05-19T10:30:00Z");
        String dayDir = TranslogArchivePathHelper.dayDir(uploadTime);
        String minuteDir = TranslogArchivePathHelper.minuteDir(uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, "node1");

        Map<String, byte[]> blobs = Map.of(dayDir + "/" + minuteDir + "/" + blobName, tar);
        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        Path location = createTempDir();
        Instant startFrom = uploadTime.minusSeconds(60);

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            missingGen,
            missingGen,
            location,
            startFrom,
            null
        );

        assertFalse("Should return false when generation is not in any TAR", found);
    }

    /** Index cache is populated on first read and reused (no duplicate range-GETs). */
    public void testIndexCacheIsPopulatedAndReused() throws IOException {
        long gen = 7L;
        byte[] tar = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, gen);

        Instant uploadTime = Instant.parse("2026-05-19T10:30:00Z");
        String dayDir = TranslogArchivePathHelper.dayDir(uploadTime);
        String minuteDir = TranslogArchivePathHelper.minuteDir(uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, "node1");

        Map<String, byte[]> blobs = Map.of(dayDir + "/" + minuteDir + "/" + blobName, tar);
        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache();
        Path location1 = createTempDir();
        Path location2 = createTempDir();

        Instant startFrom = uploadTime.minusSeconds(60);

        // First call: populates cache
        boolean found1 = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            gen,
            gen,
            location1,
            startFrom,
            cache
        );
        assertTrue("First call should find generation", found1);
        assertEquals("Cache should have 1 entry after first call", 1, cache.size());

        // Second call: should hit cache (no extra downloads of the _index)
        boolean found2 = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            gen,
            gen,
            location2,
            startFrom,
            cache
        );
        assertTrue("Second call should also find generation", found2);
        assertEquals("Cache size should still be 1", 1, cache.size());

        // Verify _index was only downloaded once (the header + data = 2 range-GETs for index)
        // plus 2 range-GETs for tlog+ckp on each call = 2*1 + 2*2 = 6 total, index only twice
        // We verify the cache works by checking the index header is only read once per TAR
        BlobPath minutePath = TranslogArchivePathHelper.txlogRootPath(base).add(dayDir).add(minuteDir);
        // Each call: 1 range-GET for header (offset=0), 1 range-GET for index data, 2 range-GETs for tlog+ckp
        // With cache: 2nd call skips header+index → only 2 range-GETs for tlog+ckp
        verify(ts, times(1)).downloadBlob(eq(minutePath), eq(blobName), eq(0L), anyLong()); // header only once
    }

    /** TARs from before startFrom are skipped. */
    public void testSkipsTarsBeforeStartFrom() throws IOException {
        long gen = 3L;
        byte[] tar = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, gen);

        Instant uploadTime = Instant.parse("2026-05-19T10:30:00Z");
        String dayDir = TranslogArchivePathHelper.dayDir(uploadTime);
        String minuteDir = TranslogArchivePathHelper.minuteDir(uploadTime);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadTime, "node1");

        Map<String, byte[]> blobs = Map.of(dayDir + "/" + minuteDir + "/" + blobName, tar);
        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        Path location = createTempDir();
        // startFrom is AFTER the upload time — the TAR should be skipped
        Instant startFrom = uploadTime.plusSeconds(600); // 10 minutes later

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            gen,
            gen,
            location,
            startFrom,
            null
        );

        assertFalse("Should not find TAR that is before startFrom", found);
    }

    /** Recovery from multiple TAR blobs spanning two minute-dirs. */
    public void testRecoversFromMultipleMinuteDirs() throws IOException {
        Instant time1 = Instant.parse("2026-05-19T10:30:00Z");
        Instant time2 = Instant.parse("2026-05-19T10:31:00Z");
        long gen1 = 1L;
        long gen2 = 2L;

        byte[] tar1 = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, gen1);
        byte[] tar2 = buildTarWithGenerations(INDEX_UUID, SHARD_ID, PRIMARY_TERM, gen2);

        String dayDir = TranslogArchivePathHelper.dayDir(time1);
        String minuteDir1 = TranslogArchivePathHelper.minuteDir(time1);
        String minuteDir2 = TranslogArchivePathHelper.minuteDir(time2);
        String blob1 = TranslogArchivePathHelper.tarBlobName(time1, "n1");
        String blob2 = TranslogArchivePathHelper.tarBlobName(time2, "n1");

        Map<String, byte[]> blobs = new java.util.LinkedHashMap<>();
        blobs.put(dayDir + "/" + minuteDir1 + "/" + blob1, tar1);
        blobs.put(dayDir + "/" + minuteDir2 + "/" + blob2, tar2);

        BlobPath base = basePath();
        TransferService ts = mockTransferService(base, blobs);

        Path location = createTempDir();
        Instant startFrom = time1.minusSeconds(60);

        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            gen1,
            gen2,
            location,
            startFrom,
            null
        );

        assertTrue("Should find both generations across two minute-dirs", found);
        assertTrue(Files.exists(location.resolve(Translog.getFilename(gen1))));
        assertTrue(Files.exists(location.resolve(Translog.getFilename(gen2))));
    }

    /** txlog/ root raises NoSuchFileException → return false gracefully. */
    public void testHandlesMissingTxlogRoot() throws IOException {
        BlobPath base = basePath();
        TransferService ts = mock(TransferService.class);
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(base);
        when(ts.listFolders(eq(txlogRoot))).thenThrow(new java.nio.file.NoSuchFileException("txlog"));

        Path location = createTempDir();
        boolean found = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            ts,
            base,
            INDEX_UUID,
            SHARD_ID,
            1L,
            1L,
            location,
            Instant.now(),
            null
        );

        assertFalse("Should return false when txlog root does not exist", found);
    }
}
