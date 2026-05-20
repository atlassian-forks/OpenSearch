/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.TransferService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Recovers translog files for a single shard from archive TARs stored in the hierarchical
 * txlog path ({@code txlog/{yyyyMMdd}/{HHmm}/*.tar}).
 *
 * <p><b>Algorithm:</b>
 * <ol>
 *   <li>Compute {@code startFrom = lastSegmentTimestamp - RECOVERY_START_MARGIN_MINUTES}</li>
 *   <li>LIST day-dirs under {@code txlog/} and skip dirs before {@code startFrom}.</li>
 *   <li>For each relevant minute-dir (≥ startFrom), list TAR blobs in sorted order.</li>
 *   <li>For each TAR, range-read its binary {@code _index} (offset 0→512 for header, then
 *       header-specified size for data), parse via {@link TarArchiveBuilder#parseIndex},
 *       and cache in {@link TranslogArchiveIndexCache}.</li>
 *   <li>Filter index entries by {@code indexUUID/shardId} path prefix.</li>
 *   <li>For each required generation, range-read tlog + ckp from the matching TAR.</li>
 * </ol>
 *
 * <p>Returns {@code false} to signal that core should fall back to per-file recovery when
 * no archive exists or required generations are not found.
 *
 * @opensearch.internal
 */
final class TranslogArchiveRecovery {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveRecovery.class);

    /** Max blobs to list per minute-dir during hierarchical recovery. */
    static final int MAX_BLOBS_PER_MINUTE_DIR = 5_000;

    /** Safety margin: scan this many minutes before lastSegmentTimestamp. */
    public static final int RECOVERY_START_MARGIN_MINUTES = 2;

    private TranslogArchiveRecovery() {}

    /**
     * Recovers translog files for a shard from the hierarchical TAR archive path.
     *
     * @param transferService      blob transfer service
     * @param archiveBasePath      repository base path (parent of {@code txlog/})
     * @param indexUUID            index UUID (used to filter TAR entries)
     * @param shardId              shard ID (used to filter TAR entries)
     * @param minGeneration        first generation to recover (inclusive)
     * @param maxGeneration        last generation to recover (inclusive)
     * @param location             local directory to write recovered files
     * @param lastSegmentTimestamp timestamp to start scanning from (minus margin)
     * @param cache                shared LRU index cache (may be null to disable caching)
     * @return {@code true} if all required generations were found and extracted;
     *         {@code false} to signal fallback to per-file recovery
     * @throws IOException on transfer errors
     */
    public static boolean recoverFromHierarchicalPath(
        TransferService transferService,
        BlobPath archiveBasePath,
        String indexUUID,
        int shardId,
        long minGeneration,
        long maxGeneration,
        Path location,
        Instant lastSegmentTimestamp,
        TranslogArchiveIndexCache cache
    ) throws IOException {
        Instant startFrom = lastSegmentTimestamp.minus(Duration.ofMinutes(RECOVERY_START_MARGIN_MINUTES));
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(archiveBasePath);

        logger.info(
            "Hierarchical recovery: index={} shard={} gen=[{}-{}] startFrom={}",
            indexUUID,
            shardId,
            minGeneration,
            maxGeneration,
            startFrom
        );

        // Collect all relevant TARs from minute-dirs at or after startFrom
        List<TarRef> tars = collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);
        if (tars.isEmpty()) {
            logger.info("No TAR blobs found in hierarchical path from {}, falling back", startFrom);
            return false;
        }

        // Path prefix to filter entries belonging to this shard: "indexUUID/shardId/"
        String shardPrefix = indexUUID + "/" + shardId + "/";

        // Build map: generation → TarEntryPair (tlog offset/length + ckp offset/length)
        Map<Long, TarEntryPair> genToLocation = new HashMap<>();

        for (TarRef tar : tars) {
            List<TarArchiveBuilder.EntryLocation> entries = readTarIndexCached(transferService, tar, cache);
            // Group by path for tlog↔ckp pairing
            Map<String, TarArchiveBuilder.EntryLocation> byPath = new HashMap<>();
            for (TarArchiveBuilder.EntryLocation loc : entries) {
                if (loc.getPath().startsWith(shardPrefix)) {
                    byPath.put(loc.getPath(), loc);
                }
            }
            // Find all .tlog entries and pair with .ckp
            for (TarArchiveBuilder.EntryLocation tlogLoc : entries) {
                String path = tlogLoc.getPath();
                if (!path.startsWith(shardPrefix) || !path.endsWith(".tlog")) continue;

                String ckpPath = path.substring(0, path.length() - 5) + ".ckp";
                TarArchiveBuilder.EntryLocation ckpLoc = byPath.get(ckpPath);

                long gen = parseGeneration(path);
                if (gen < 0) continue;
                if (gen < minGeneration || gen > maxGeneration) continue;

                // Most recent TAR wins for this generation
                genToLocation.put(gen, new TarEntryPair(tar, tlogLoc, ckpLoc));
            }
        }

        // Download all required generations
        boolean allFound = true;
        long effectiveMax = maxGeneration;

        if (minGeneration == Long.MIN_VALUE && maxGeneration == Long.MAX_VALUE) {
            // "Recover all" mode — download everything found
            for (Map.Entry<Long, TarEntryPair> e : genToLocation.entrySet()) {
                downloadGeneration(transferService, e.getValue(), e.getKey(), location);
            }
            effectiveMax = genToLocation.keySet().stream().mapToLong(Long::longValue).max().orElse(Long.MIN_VALUE);
        } else {
            for (long gen = minGeneration; gen <= maxGeneration; gen++) {
                TarEntryPair loc = genToLocation.get(gen);
                if (loc == null) {
                    logger.warn("Generation {} not found in hierarchical path for shard {}/{}", gen, indexUUID, shardId);
                    allFound = false;
                    continue;
                }
                downloadGeneration(transferService, loc, gen, location);
            }
        }

        // Copy highest-generation .ckp to translog.ckp for bootstrap
        if (effectiveMax != Long.MIN_VALUE && effectiveMax != Long.MAX_VALUE) {
            String commitCkp = Translog.getCommitCheckpointFileName(effectiveMax);
            Path commitCkpPath = location.resolve(commitCkp);
            if (Files.exists(commitCkpPath)) {
                Path globalCkp = location.resolve(Translog.CHECKPOINT_FILE_NAME);
                if (Files.exists(globalCkp)) Files.delete(globalCkp);
                Files.copy(commitCkpPath, globalCkp);
            }
        }

        logger.info(
            "Hierarchical recovery complete: index={} shard={} gen=[{}-{}] allFound={} tarsScanned={}",
            indexUUID,
            shardId,
            minGeneration,
            maxGeneration,
            allFound,
            tars.size()
        );
        return allFound;
    }

    /**
     * Collects all TAR blob references from the hierarchical txlog path that are at or after
     * {@code startFrom}, sorted lexicographically (= chronologically).
     */
    static List<TarRef> collectTarsFromHierarchicalPath(TransferService transferService, BlobPath txlogRoot, Instant startFrom)
        throws IOException {
        List<TarRef> result = new ArrayList<>();

        Set<String> dayDirs;
        try {
            dayDirs = transferService.listFolders(txlogRoot);
        } catch (FileNotFoundException | NoSuchFileException e) {
            logger.debug("Recovery: txlog root not found, falling back to per-file recovery");
            return result;
        } catch (IOException e) {
            logger.warn("Recovery: failed to list txlog root: {}", e.getMessage());
            return result;
        }
        if (dayDirs == null || dayDirs.isEmpty()) {
            return result;
        }

        String startDay = TranslogArchivePathHelper.dayDir(startFrom);
        String startMinute = TranslogArchivePathHelper.minuteDir(startFrom);

        for (String dayDir : dayDirs) {
            // Skip days entirely before startDay
            if (dayDir.compareTo(startDay) < 0) continue;

            BlobPath dayPath = txlogRoot.add(dayDir);
            Set<String> minuteDirs;
            try {
                minuteDirs = transferService.listFolders(dayPath);
            } catch (IOException e) {
                logger.warn("Recovery: failed to list day dir {}: {}", dayPath.buildAsString(), e.getMessage());
                continue;
            }
            if (minuteDirs == null || minuteDirs.isEmpty()) continue;

            for (String minuteDir : minuteDirs) {
                // For the start day, skip minute-dirs before startMinute
                if (dayDir.equals(startDay) && minuteDir.compareTo(startMinute) < 0) continue;

                BlobPath minutePath = dayPath.add(minuteDir);
                List<BlobMetadata> blobs;
                try {
                    blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                        f -> transferService.listAllInSortedOrder(minutePath, "", MAX_BLOBS_PER_MINUTE_DIR, f)
                    );
                } catch (IOException e) {
                    logger.warn("Recovery: failed to list minute dir {}: {}", minutePath.buildAsString(), e.getMessage());
                    continue;
                }
                if (blobs == null) continue;
                for (BlobMetadata blob : blobs) {
                    if (blob.name().endsWith(".tar")) {
                        result.add(new TarRef(minutePath, blob.name(), blob.length()));
                    }
                }
            }
        }

        // Sort lexicographically by full path (= chronologically for timestamp-named blobs)
        result.sort((a, b) -> {
            String ka = a.path.buildAsString() + "/" + a.blobName;
            String kb = b.path.buildAsString() + "/" + b.blobName;
            return ka.compareTo(kb);
        });
        return result;
    }

    /**
     * Reads the binary {@code _index} from a TAR blob using two range-GETs, with LRU caching.
     *
     * <p>TAR layout: [512B header][index bytes, padded to 512][...data entries...][2×512 EOF]
     * Step 1: range-GET bytes [0, 512) to read the TAR header and extract index size.
     * Step 2: range-GET bytes [512, 512+indexSize) to read the index bytes.
     * Step 3: parse via {@link TarArchiveBuilder#parseIndex}.
     */
    static List<TarArchiveBuilder.EntryLocation> readTarIndexCached(
        TransferService transferService,
        TarRef tar,
        TranslogArchiveIndexCache cache
    ) throws IOException {
        String pathStr = tar.path.buildAsString();

        // Check cache first
        if (cache != null) {
            List<TarArchiveBuilder.EntryLocation> cached = cache.get(pathStr, tar.blobName);
            if (cached != null) {
                logger.debug("TAR index cache hit: {}/{}", pathStr, tar.blobName);
                return cached;
            }
        }

        List<TarArchiveBuilder.EntryLocation> entries = readTarIndex(transferService, tar);

        // Populate cache
        if (cache != null) {
            cache.put(pathStr, tar.blobName, entries);
        }
        return entries;
    }

    /**
     * Reads and parses the binary {@code _index} from a TAR blob via two range-GETs.
     */
    static List<TarArchiveBuilder.EntryLocation> readTarIndex(TransferService transferService, TarRef tar) throws IOException {
        // Step 1: read the 512-byte TAR header for the _index entry to get its size
        byte[] header;
        try (InputStream hStream = transferService.downloadBlob(tar.path, tar.blobName, 0, TarArchiveBuilder.TAR_BLOCK)) {
            header = hStream.readAllBytes();
        }
        if (header.length < TarArchiveBuilder.TAR_BLOCK) {
            logger.warn("TAR {} too small for header ({} bytes)", tar.blobName, header.length);
            return Collections.emptyList();
        }

        // Parse octal size field from TAR header (bytes 124–135)
        String sizeOctal = new String(header, 124, 12, StandardCharsets.US_ASCII).trim().replace("\0", "");
        long indexDataSize;
        try {
            indexDataSize = Long.parseLong(sizeOctal, 8);
        } catch (NumberFormatException e) {
            logger.warn("TAR {} has invalid size field in header: '{}'", tar.blobName, sizeOctal);
            return Collections.emptyList();
        }

        if (indexDataSize <= 0 || indexDataSize > 512 * 1024) {
            logger.warn("TAR {} index size {} out of expected range", tar.blobName, indexDataSize);
            return Collections.emptyList();
        }

        // Step 2: range-GET the index bytes (immediately after the 512-byte header)
        byte[] indexBytes;
        try (InputStream iStream = transferService.downloadBlob(tar.path, tar.blobName, TarArchiveBuilder.TAR_BLOCK, indexDataSize)) {
            indexBytes = iStream.readAllBytes();
        }

        // Step 3: parse the binary index
        try {
            return TarArchiveBuilder.parseIndex(indexBytes);
        } catch (Exception e) {
            logger.warn("Failed to parse TAR index from {}: {}", tar.blobName, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Downloads tlog + ckp for a single generation from a TAR via range-reads.
     */
    private static void downloadGeneration(TransferService transferService, TarEntryPair loc, long generation, Path location)
        throws IOException {
        String tlogFilename = Translog.getFilename(generation);
        String ckpFilename = Translog.getCommitCheckpointFileName(generation);

        rangeReadToFile(
            transferService,
            loc.tar.path,
            loc.tar.blobName,
            loc.tlogLoc.getDataOffset(),
            loc.tlogLoc.getDataLength(),
            location.resolve(tlogFilename)
        );

        if (loc.ckpLoc != null) {
            rangeReadToFile(
                transferService,
                loc.tar.path,
                loc.tar.blobName,
                loc.ckpLoc.getDataOffset(),
                loc.ckpLoc.getDataLength(),
                location.resolve(ckpFilename)
            );
        }
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
     * Parses the translog generation from a TAR entry path.
     * Path format: {@code {uuid}/{shardId}/{primaryTerm}/translog-{generation}.tlog}
     *
     * @return the parsed generation, or -1 if the path cannot be parsed
     */
    static long parseGeneration(String path) {
        // Extract filename (last component)
        int lastSlash = path.lastIndexOf('/');
        String filename = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
        // filename: translog-{generation}.tlog
        if (!filename.startsWith("translog-") || !filename.endsWith(".tlog")) {
            return -1;
        }
        try {
            return Long.parseLong(filename.substring("translog-".length(), filename.length() - ".tlog".length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    /** Reference to a TAR blob in the archive. */
    static final class TarRef {
        final BlobPath path;
        final String blobName;
        final long size;

        TarRef(BlobPath path, String blobName, long size) {
            this.path = path;
            this.blobName = blobName;
            this.size = size;
        }
    }

    /** Paired tlog + ckp entry locations within a specific TAR. */
    static final class TarEntryPair {
        final TarRef tar;
        final TarArchiveBuilder.EntryLocation tlogLoc;
        final TarArchiveBuilder.EntryLocation ckpLoc; // may be null if not found

        TarEntryPair(TarRef tar, TarArchiveBuilder.EntryLocation tlogLoc, TarArchiveBuilder.EntryLocation ckpLoc) {
            this.tar = tar;
            this.tlogLoc = tlogLoc;
            this.ckpLoc = ckpLoc;
        }
    }
}
