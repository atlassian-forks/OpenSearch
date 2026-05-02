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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    /** Minimum EOCD size (22 bytes fixed header). */
    private static final int MAX_ZIPS_PER_BUCKET = 10_000;

    /** Max blobs to list per minute-dir during hierarchical recovery. */
    private static final int MAX_BLOBS_PER_MINUTE_DIR = 5_000;
    /** Safety margin: start scanning this many minutes before lastSegmentTimestamp. */
    public static final int RECOVERY_START_MARGIN_MINUTES = 2;

    private TranslogArchiveRecovery() {}

    // ── New hierarchical-path recovery ────────────────────────────────────────

    /**
     * Recovers translog files for a shard from the new hierarchical TAR path
     * ({@code {base}/txlog/{day}/{minute}/*.tar}), starting from a timestamp-bounded scan.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Compute start = {@code lastSegmentTimestamp - RECOVERY_START_MARGIN_MINUTES}</li>
     *   <li>LIST day-dirs under {@code txlog/} from today and yesterday if spanning midnight.</li>
     *   <li>For each relevant minute-dir (≥ start), LIST blobs and read each TAR's binary
     *       {@code _index} (cached by {@link TranslogArchiveIndexCache}).</li>
     *   <li>Filter entries by {@code indexUUID/shardId} path prefix.</li>
     *   <li>For each generation in [{@code minGeneration}, {@code maxGeneration}], range-read
     *       the tlog + ckp data from the matching TAR.</li>
     * </ol>
     *
     * @param transferService       blob transfer service
     * @param archiveBasePath       repository base path (the root, not txlog/)
     * @param indexUUID             index UUID (used to filter TAR entries)
     * @param shardId               shard ID (used to filter TAR entries)
     * @param minGeneration         first generation to recover (inclusive)
     * @param maxGeneration         last generation to recover (inclusive)
     * @param location              local directory to write recovered files
     * @param lastSegmentTimestamp  timestamp of the last successful segment upload;
     *                              recovery scans from {@code lastSegmentTimestamp - margin}
     * @param cache                 shared LRU index cache (may be null to disable caching)
     * @return true if all required generations were recovered, false if some were missing
     *         (caller should fall back to the legacy recovery path)
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
            indexUUID, shardId, minGeneration, maxGeneration, startFrom
        );

        // Collect all relevant TARs from minute-dirs at or after startFrom
        List<ZipRef> tars = collectTarsFromHierarchicalPath(transferService, txlogRoot, startFrom);
        if (tars.isEmpty()) {
            logger.info("No TAR blobs found in hierarchical path from {}, falling back", startFrom);
            return false;
        }

        // Build a map: generation → TarRef + ArchiveIndexEntry
        // Use a call-local cache (blobKey → entries) to avoid re-reading the same TAR index
        // when recovering multiple shards from the same TAR.
        Map<String, List<ArchiveIndexEntry>> entryCache = new HashMap<>();
        Map<Long, ZipEntryLocation> genToLocation = new HashMap<>();

        for (ZipRef tar : tars) {
            List<ArchiveIndexEntry> entries = readTarIndexCachedEntries(transferService, tar, entryCache);
            for (ArchiveIndexEntry entry : entries) {
                if (entry.getShardId() != shardId) continue;
                long generation = entry.getGeneration();
                if (generation < minGeneration || generation > maxGeneration) continue;
                // Keep the latest TAR that has this generation (most recent wins)
                genToLocation.put(generation, new ZipEntryLocation(tar.path, tar.blobName, entry));
            }
        }

        // Download all required generations
        boolean allFound = true;
        for (long gen = minGeneration; gen <= maxGeneration; gen++) {
            ZipEntryLocation loc = genToLocation.get(gen);
            if (loc == null) {
                logger.warn("Generation {} not found in hierarchical path for shard {}", gen, shardId);
                allFound = false;
                continue;
            }
            downloadGeneration(transferService, loc, gen, location);
        }

        if (allFound && maxGeneration >= minGeneration) {
            // Copy final checkpoint
            String commitCkp = Translog.getCommitCheckpointFileName(maxGeneration);
            Path commitCkpPath = location.resolve(commitCkp);
            if (Files.exists(commitCkpPath)) {
                Path globalCkp = location.resolve(Translog.CHECKPOINT_FILE_NAME);
                if (Files.exists(globalCkp)) Files.delete(globalCkp);
                Files.copy(commitCkpPath, globalCkp);
            }
        }

        logger.info(
            "Hierarchical recovery complete: index={} shard={} gen=[{}-{}] allFound={} tarsScanned={}",
            indexUUID, shardId, minGeneration, maxGeneration, allFound, tars.size()
        );
        return allFound;
    }

    /**
     * Collects all TAR blob references from the hierarchical txlog path that are at or after
     * {@code startFrom}. Returns them sorted lexicographically (= chronologically).
     *
     * @param transferService blob service
     * @param txlogRoot       path to {@code txlog/} root
     * @param startFrom       only include TARs in minute-dirs ≥ this timestamp
     * @return sorted list of TAR blob references
     */
    static List<ZipRef> collectTarsFromHierarchicalPath(
        TransferService transferService,
        BlobPath txlogRoot,
        Instant startFrom
    ) throws IOException {
        List<ZipRef> result = new ArrayList<>();

        Set<String> dayDirs;
        try {
            dayDirs = transferService.listFolders(txlogRoot);
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
                        result.add(new ZipRef(minutePath, blob.name(), blob.length()));
                    }
                }
            }
        }

        // Sort by (dayDir + minuteDir + blobName) — lexicographic = chronological
        result.sort((a, b) -> {
            String ka = a.path.buildAsString() + "/" + a.blobName;
            String kb = b.path.buildAsString() + "/" + b.blobName;
            return ka.compareTo(kb);
        });
        return result;
    }

    /**
     * Reads the TAR binary {@code _index} from the blob, using the per-blob cache if available.
     * Caches {@link ArchiveIndexEntry} objects (not {@link TarArchiveBuilder.EntryLocation},
     * which is package-private).
     *
     * <p>The {@link TranslogArchiveIndexCache} is typed to {@code List<TarArchiveBuilder.EntryLocation>}
     * for the general case; here we use a separate in-call map to avoid the type mismatch.
     * The cache key is still {@code (pathStr, blobName)} for coherence.
     *
     * @param transferService blob service
     * @param tar             TAR blob reference
     * @param entryCache      call-local cache: blobKey → List&lt;ArchiveIndexEntry&gt;
     * @return list of {@link ArchiveIndexEntry} from the TAR's {@code _index}
     */
    static List<ArchiveIndexEntry> readTarIndexCachedEntries(
        TransferService transferService,
        ZipRef tar,
        Map<String, List<ArchiveIndexEntry>> entryCache
    ) throws IOException {
        String cacheKey = tar.path.buildAsString() + "/" + tar.blobName;
        if (entryCache.containsKey(cacheKey)) {
            return entryCache.get(cacheKey);
        }
        List<ArchiveIndexEntry> entries = readTarIndex(transferService, tar);
        entryCache.put(cacheKey, entries);
        return entries;
    }

    // ── End new hierarchical-path recovery ────────────────────────────────────

    /**
     * Reads the TAR index from the HEAD of a .tar archive blob (byte-range GET of first two TAR entries).
     *
     * <p>TAR layout: [512B header][index data padded to 512][...data entries...][2*512 EOF marker]
     * The index is always at offset 512 with a known size (from the TAR header), so we can read it
     * with a bounded byte-range GET without knowing the total archive size.
     */
    static List<ArchiveIndexEntry> readTarIndex(TransferService transferService, ZipRef ref) throws IOException {
        // Step 1: read the first 512 bytes (TAR header for the index entry) to get index size
        byte[] header;
        try (InputStream hStream = transferService.downloadBlob(ref.path, ref.blobName, 0, TarArchiveBuilder.TAR_BLOCK)) {
            header = hStream.readAllBytes();
        }
        if (header.length < TarArchiveBuilder.TAR_BLOCK) {
            logger.warn("TAR {} too small for header ({} bytes)", ref.blobName, header.length);
            return Collections.emptyList();
        }

        // Parse size field from TAR header (bytes 124-135, octal ASCII)
        String sizeOctal = new String(header, 124, 12, StandardCharsets.US_ASCII).trim().replace("\0", "");
        long indexDataSize;
        try {
            indexDataSize = Long.parseLong(sizeOctal, 8);
        } catch (NumberFormatException e) {
            logger.warn("TAR {} has invalid size field in header: '{}'", ref.blobName, sizeOctal);
            return Collections.emptyList();
        }

        // Sanity check: index = GC prefix (up to 2 + 4096*32 = ~128KB) + entry index (~312KB max at 2000 shards).
        // Allow up to 512KB to be safe. Values far below 512 or above 512KB indicate corruption.
        if (indexDataSize <= 0 || indexDataSize > 512 * 1024) {
            logger.warn("TAR {} index size {} out of expected range", ref.blobName, indexDataSize);
            return Collections.emptyList();
        }

        // Step 2: read the index data (immediately after the 512-byte header)
        byte[] indexBytes;
        try (InputStream iStream = transferService.downloadBlob(ref.path, ref.blobName, TarArchiveBuilder.TAR_BLOCK, (int) indexDataSize)) {
            indexBytes = iStream.readAllBytes();
        }

        // Step 3: parse the binary index into ArchiveIndexEntry list
        List<TarArchiveBuilder.EntryLocation> locations;
        try {
            locations = TarArchiveBuilder.parseIndex(indexBytes);
        } catch (Exception e) {
            logger.warn("Failed to parse TAR index from {}: {}", ref.blobName, e.getMessage());
            return Collections.emptyList();
        }

        // Step 4: convert EntryLocation pairs (tlog+ckp) into ArchiveIndexEntry objects
        return parseTarLocationsToArchiveEntries(locations);
    }

    /**
     * Converts a list of TAR {@link TarArchiveBuilder.EntryLocation} entries into {@link ArchiveIndexEntry} objects.
     * Expects entries in pairs: (tlog, ckp) for each generation, in the path format:
     * {@code {uuid}/{shardId}/{primaryTerm}/translog-{generation}.tlog}
     * {@code {uuid}/{shardId}/{primaryTerm}/translog-{generation}.ckp}
     */
    static List<ArchiveIndexEntry> parseTarLocationsToArchiveEntries(List<TarArchiveBuilder.EntryLocation> locations) {
        List<ArchiveIndexEntry> result = new ArrayList<>();
        // Group by generation key: uuid/shardId/primaryTerm/generation
        Map<String, TarArchiveBuilder.EntryLocation> byPath = new HashMap<>();
        for (TarArchiveBuilder.EntryLocation loc : locations) {
            byPath.put(loc.getPath(), loc);
        }

        // Find all .tlog entries and pair with .ckp
        for (TarArchiveBuilder.EntryLocation tlogLoc : locations) {
            String path = tlogLoc.getPath();
            if (!path.endsWith(".tlog")) continue;

            // Path format: {uuid}/{shardId}/{primaryTerm}/translog-{generation}.tlog
            // Checkpoint path: same prefix but .ckp extension
            String ckpPath = path.substring(0, path.length() - 5) + ".ckp";

            TarArchiveBuilder.EntryLocation ckpLoc = byPath.get(ckpPath);

            // Parse path components
            String[] parts = path.split("/");
            if (parts.length < 4) continue;
            try {
                int shardId = Integer.parseInt(parts[parts.length - 3]);
                long primaryTerm = Long.parseLong(parts[parts.length - 2]);
                // filename: translog-{generation}.tlog
                String filename = parts[parts.length - 1];
                long generation = Long.parseLong(
                    filename.replace("translog-", "").replace(".tlog", "")
                );

                long ckpOffset = ckpLoc != null ? ckpLoc.getDataOffset() : 0;
                long ckpLength = ckpLoc != null ? ckpLoc.getDataLength() : 0;

                result.add(new ArchiveIndexEntry(
                    shardId, primaryTerm, generation,
                    tlogLoc.getDataOffset(), tlogLoc.getDataLength(),
                    ckpOffset, ckpLength
                ));
            } catch (NumberFormatException e) {
                logger.warn("Could not parse TAR entry path: {}", path);
            }
        }
        return result;
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
