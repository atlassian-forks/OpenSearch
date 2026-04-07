/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.archive.ArchiveCommentFormat;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Recovers translog files for a single shard from archive ZIPs using binary search on sorted ZIP blob names.
 * <p>
 * ZIP blob names are timestamps ({@code yyyyMMddHHmmssSSS.zip}), so S3 LIST returns them lexicographically
 * sorted. Generations are monotonically increasing, so binary search on ZIP comments (EOCD tail reads)
 * efficiently locates the ZIPs containing the required generation range.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchiveRecovery {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveRecovery.class);

    /** Maximum bytes to read from ZIP tail for EOCD comment (covers most comments). */
    static final int EOCD_TAIL_READ_SIZE = 8192;
    /** Minimum EOCD size (22 bytes fixed header). */
    private static final int EOCD_MIN_SIZE = 22;
    private static final int MAX_ZIPS_PER_BUCKET = 10_000;

    private TranslogArchiveRecovery() {}

    /**
     * Recover translog files for a specific shard from archive ZIPs, discovering the generation range
     * from the latest ZIP comments. Used when no per-shard metadata is available (coordinator mode).
     *
     * @param transferService  S3/blob transfer service
     * @param archiveBasePath  repository root path (no index/shard components)
     * @param indexUUID        index UUID
     * @param shardId          shard ID
     * @param location         local FS directory to write recovered files
     * @param pathHashAlgorithm hash algorithm for archive path
     * @throws IOException on S3 or I/O errors
     */
    public static void recover(
        TransferService transferService,
        BlobPath archiveBasePath,
        String indexUUID,
        int shardId,
        Path location,
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm
    ) throws IOException {
        logger.info("Recovering translog from archive (no metadata): index={} shard={}", indexUUID, shardId);

        String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(indexUUID, pathHashAlgorithm);
        // nodeId is not available in the no-metadata scan path, so we must LIST {hashTypeIndex}/
        // to find all hashNodeId dirs (one per node that uploaded for this index).
        BlobPath indexPath = archiveBasePath.add("translog").add("data").add(hashTypeIndex);
        Set<String> nodeDirs = transferService.listFolders(indexPath);
        if (nodeDirs == null || nodeDirs.isEmpty()) {
            logger.info("No archive node dirs found at {}; nothing to recover", indexPath.buildAsString());
            return;
        }

        List<ZipRef> allZips = new ArrayList<>();
        for (String nodeDir : nodeDirs) {
            BlobPath nodePath = indexPath.add(nodeDir);
            List<BlobMetadata> blobs = listBlobsSorted(transferService, nodePath, MAX_ZIPS_PER_BUCKET);
            for (BlobMetadata blob : blobs) {
                if (blob.name().endsWith(".zip")) {
                    allZips.add(new ZipRef(nodePath, blob.name(), blob.length()));
                }
            }
        }

        if (allZips.isEmpty()) {
            logger.info("No archive ZIPs found; nothing to recover");
            return;
        }

        allZips.sort((a, b) -> a.blobName.compareTo(b.blobName));

        // Scan ALL ZIPs to discover the full generation range for this shard.
        // In coordinator (school bus) mode, each ZIP typically has one generation per shard,
        // so we must scan across ZIPs to find the true min/max generation range.
        long minGeneration = Long.MAX_VALUE;
        long maxGeneration = Long.MIN_VALUE;
        boolean found = false;

        for (ZipRef zip : allZips) {
            List<ArchiveIndexEntry> entries = readZipComment(transferService, zip);
            for (ArchiveIndexEntry entry : entries) {
                if (entry.getShardId() == shardId) {
                    found = true;
                    minGeneration = Math.min(minGeneration, entry.getGeneration());
                    maxGeneration = Math.max(maxGeneration, entry.getGeneration());
                }
            }
        }

        if (!found) {
            logger.info("No entries for shard {} in any archive ZIP; nothing to recover", shardId);
            return;
        }

        logger.info(
            "Discovered generation range for shard {}: [{}-{}] across {} ZIPs",
            shardId,
            minGeneration,
            maxGeneration,
            allZips.size()
        );

        recover(transferService, archiveBasePath, indexUUID, shardId, minGeneration, maxGeneration, location, pathHashAlgorithm);
    }

    /**
     * Recover translog files for a specific shard from archive ZIPs.
     *
     * @param transferService  S3/blob transfer service
     * @param archiveBasePath  repository root path (no index/shard components)
     * @param indexUUID        index UUID
     * @param shardId          shard ID
     * @param minGeneration    minimum generation to recover (inclusive)
     * @param maxGeneration    maximum generation to recover (inclusive)
     * @param location         local FS directory to write recovered files
     * @param pathHashAlgorithm hash algorithm for archive path
     * @throws IOException on S3 or I/O errors
     */
    public static void recover(
        TransferService transferService,
        BlobPath archiveBasePath,
        String indexUUID,
        int shardId,
        long minGeneration,
        long maxGeneration,
        Path location,
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm
    ) throws IOException {
        logger.info("Recovering translog from archive: index={} shard={} gen=[{}-{}]", indexUUID, shardId, minGeneration, maxGeneration);

        String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(indexUUID, pathHashAlgorithm);
        // LIST {hashTypeIndex}/ to find all hashNodeId dirs (one per node that uploaded for this index).
        BlobPath indexPath = archiveBasePath.add("translog").add("data").add(hashTypeIndex);
        Set<String> nodeDirs = transferService.listFolders(indexPath);
        if (nodeDirs == null || nodeDirs.isEmpty()) {
            throw new IOException("No archive node dirs found at " + indexPath.buildAsString());
        }

        // Collect all ZIP names across node dirs (sorted by timestamp = sorted by name)
        List<ZipRef> allZips = new ArrayList<>();
        for (String nodeDir : nodeDirs) {
            BlobPath nodePath = indexPath.add(nodeDir);
            List<BlobMetadata> blobs = listBlobsSorted(transferService, nodePath, MAX_ZIPS_PER_BUCKET);
            for (BlobMetadata blob : blobs) {
                if (blob.name().endsWith(".zip")) {
                    allZips.add(new ZipRef(nodePath, blob.name(), blob.length()));
                }
            }
        }

        if (allZips.isEmpty()) {
            throw new IOException("No archive ZIPs found under " + indexPath.buildAsString());
        }

        // Sort by blob name (timestamp-based, lexicographic = chronological)
        allZips.sort((a, b) -> a.blobName.compareTo(b.blobName));

        // Find ZIPs containing the required generation range using binary search
        Map<Long, ZipEntryLocation> genToLocation = new HashMap<>();
        for (long gen = minGeneration; gen <= maxGeneration; gen++) {
            if (genToLocation.containsKey(gen)) {
                continue;
            }
            ZipEntryLocation loc = binarySearchForGeneration(transferService, allZips, shardId, gen);
            if (loc == null) {
                throw new IOException("Could not find generation " + gen + " for shard " + shardId + " in any archive ZIP");
            }
            genToLocation.put(gen, loc);
        }

        // Download each generation's files via range-read
        for (long gen = minGeneration; gen <= maxGeneration; gen++) {
            ZipEntryLocation loc = genToLocation.get(gen);
            downloadGeneration(transferService, loc, gen, location);
        }

        // Copy final checkpoint
        String commitCkp = Translog.getCommitCheckpointFileName(maxGeneration);
        Path commitCkpPath = location.resolve(commitCkp);
        if (Files.exists(commitCkpPath)) {
            Files.copy(commitCkpPath, location.resolve(Translog.CHECKPOINT_FILE_NAME));
        }

        logger.info(
            "Archive recovery complete: index={} shard={} gen=[{}-{}] zipsScanned={}",
            indexUUID,
            shardId,
            minGeneration,
            maxGeneration,
            allZips.size()
        );
    }

    /**
     * Binary search for the ZIP containing a specific generation for a specific shard.
     *
     * @return ZipEntryLocation with offsets, or null if not found
     */
    static ZipEntryLocation binarySearchForGeneration(
        TransferService transferService,
        List<ZipRef> zips,
        int shardId,
        long targetGeneration
    ) throws IOException {
        int lo = 0;
        int hi = zips.size() - 1;

        // Cache parsed comments to avoid re-reading
        Map<Integer, List<ArchiveIndexEntry>> commentCache = new HashMap<>();

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            List<ArchiveIndexEntry> entries = getOrReadComment(transferService, zips, mid, commentCache);

            // Find this shard's entries
            List<ArchiveIndexEntry> shardEntries = entries.stream().filter(e -> e.getShardId() == shardId).collect(Collectors.toList());

            if (shardEntries.isEmpty()) {
                // Shard not in this ZIP — generation didn't change at this time.
                // Search both directions from neighbors; try left first (older ZIPs more likely to have older gens)
                hi = mid - 1;
                continue;
            }

            long minGen = shardEntries.stream().mapToLong(ArchiveIndexEntry::getGeneration).min().orElse(Long.MAX_VALUE);
            long maxGen = shardEntries.stream().mapToLong(ArchiveIndexEntry::getGeneration).max().orElse(Long.MIN_VALUE);

            if (targetGeneration < minGen) {
                hi = mid - 1;
            } else if (targetGeneration > maxGen) {
                lo = mid + 1;
            } else {
                // Found! Look for the exact entry
                for (ArchiveIndexEntry entry : shardEntries) {
                    if (entry.getGeneration() == targetGeneration) {
                        return new ZipEntryLocation(zips.get(mid).path, zips.get(mid).blobName, entry);
                    }
                }
                // Generation is in range but exact entry not found — shouldn't happen
                throw new IOException(
                    "Generation "
                        + targetGeneration
                        + " in range ["
                        + minGen
                        + "-"
                        + maxGen
                        + "] but entry not found in ZIP "
                        + zips.get(mid).blobName
                );
            }
        }

        // Linear scan as fallback (handles gaps where shard was absent)
        for (int i = zips.size() - 1; i >= 0; i--) {
            List<ArchiveIndexEntry> entries = getOrReadComment(transferService, zips, i, commentCache);
            for (ArchiveIndexEntry entry : entries) {
                if (entry.getShardId() == shardId && entry.getGeneration() == targetGeneration) {
                    return new ZipEntryLocation(zips.get(i).path, zips.get(i).blobName, entry);
                }
            }
        }

        return null;
    }

    private static List<ArchiveIndexEntry> getOrReadComment(
        TransferService transferService,
        List<ZipRef> zips,
        int index,
        Map<Integer, List<ArchiveIndexEntry>> cache
    ) throws IOException {
        if (cache.containsKey(index)) {
            return cache.get(index);
        }
        List<ArchiveIndexEntry> entries = readZipComment(transferService, zips.get(index));
        cache.put(index, entries);
        return entries;
    }

    /**
     * Range-read the ZIP tail to extract the EOCD comment.
     * Uses range-read when blob size is known to avoid downloading the entire ZIP.
     */
    static List<ArchiveIndexEntry> readZipComment(TransferService transferService, ZipRef zip) throws IOException {
        byte[] tail;
        if (zip.size > 0) {
            // Range-read only the tail of the ZIP (EOCD + comment are at the end)
            int tailLen = (int) Math.min(zip.size, EOCD_TAIL_READ_SIZE);
            long tailOffset = zip.size - tailLen;
            try (InputStream tailStream = transferService.downloadBlob(zip.path, zip.blobName, tailOffset, tailLen)) {
                tail = tailStream.readAllBytes();
            }
        } else {
            // Fallback: blob size unknown, download full blob
            try (InputStream fullStream = transferService.downloadBlob(zip.path, zip.blobName)) {
                tail = fullStream.readAllBytes();
            }
        }
        String comment = extractEocdComment(tail);
        if (comment == null) {
            return Collections.emptyList();
        }
        return ArchiveCommentFormat.parse(comment);
    }

    /**
     * Extract EOCD comment from ZIP bytes by scanning backwards for the EOCD signature.
     */
    static String extractEocdComment(byte[] zipBytes) {
        // EOCD signature: 0x50 0x4b 0x05 0x06
        int len = zipBytes.length;
        for (int i = len - EOCD_MIN_SIZE; i >= Math.max(0, len - 65535 - EOCD_MIN_SIZE); i--) {
            if (zipBytes[i] == 0x50 && zipBytes[i + 1] == 0x4b && zipBytes[i + 2] == 0x05 && zipBytes[i + 3] == 0x06) {
                int commentLength = (zipBytes[i + 20] & 0xFF) | ((zipBytes[i + 21] & 0xFF) << 8);
                if (commentLength > 0 && i + EOCD_MIN_SIZE + commentLength <= len) {
                    return new String(zipBytes, i + EOCD_MIN_SIZE, commentLength, StandardCharsets.UTF_8);
                }
                return null;
            }
        }
        return null;
    }

    /**
     * Download tlog + ckp for a single generation from a ZIP via range-read.
     */
    private static void downloadGeneration(TransferService transferService, ZipEntryLocation loc, long generation, Path location)
        throws IOException {
        String tlogFilename = Translog.getFilename(generation);
        String ckpFilename = Translog.getCommitCheckpointFileName(generation);

        // Download tlog
        rangeReadToFile(
            transferService,
            loc.path,
            loc.blobName,
            loc.entry.getTlogOffset(),
            loc.entry.getTlogLength(),
            location.resolve(tlogFilename)
        );

        // Download ckp
        rangeReadToFile(
            transferService,
            loc.path,
            loc.blobName,
            loc.entry.getCkpOffset(),
            loc.entry.getCkpLength(),
            location.resolve(ckpFilename)
        );
    }

    private static void rangeReadToFile(
        TransferService transferService,
        BlobPath path,
        String blobName,
        long offset,
        long length,
        Path targetFile
    ) throws IOException {
        if (Files.exists(targetFile)) {
            Files.delete(targetFile);
        }
        try (InputStream in = transferService.downloadBlob(path, blobName, offset, length)) {
            Files.copy(in, targetFile);
        }
    }

    /**
     * Synchronously list blobs in sorted order at the given path.
     */
    private static List<BlobMetadata> listBlobsSorted(TransferService transferService, BlobPath path, int limit) throws IOException {
        SetOnce<List<BlobMetadata>> resultHolder = new SetOnce<>();
        SetOnce<Exception> errorHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        transferService.listAllInSortedOrder(
            path,
            "",
            limit,
            new LatchedActionListener<>(ActionListener.wrap(resultHolder::set, errorHolder::set), latch)
        );
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IOException("Timed out listing blobs at " + path.buildAsString());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted listing blobs at " + path.buildAsString(), e);
        }
        if (errorHolder.get() != null) {
            throw new IOException("Failed to list blobs at " + path.buildAsString(), errorHolder.get());
        }
        List<BlobMetadata> result = resultHolder.get();
        return result != null ? result : Collections.emptyList();
    }

    /** Reference to a ZIP blob in the archive. */
    static final class ZipRef {
        final BlobPath path;
        final String blobName;
        final long size;

        ZipRef(BlobPath path, String blobName) {
            this(path, blobName, -1);
        }

        ZipRef(BlobPath path, String blobName, long size) {
            this.path = path;
            this.blobName = blobName;
            this.size = size;
        }
    }

    /** Location of a generation's data within a specific ZIP blob. */
    static final class ZipEntryLocation {
        final BlobPath path;
        final String blobName;
        final ArchiveIndexEntry entry;

        ZipEntryLocation(BlobPath path, String blobName, ArchiveIndexEntry entry) {
            this.path = path;
            this.blobName = blobName;
            this.entry = entry;
        }
    }
}
