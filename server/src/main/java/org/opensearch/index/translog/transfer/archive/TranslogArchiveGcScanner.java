/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scans translog TAR archives in the hierarchical txlog path and builds per-minute GC index files.
 *
 * <p><b>Algorithm (per scan cycle)</b>:
 * <ol>
 *   <li>LIST {@code gc_idx/{day}/} to find already-scanned minute-dirs.</li>
 *   <li>LIST {@code txlog/{day}/{minute}/} for each minute-dir that does not yet have a {@code .idx}.</li>
 *   <li>For each TAR blob: issue two range-GETs to read only the GC summary prefix from {@code _index}.</li>
 *   <li>Merge per-shard {@code {minGen, maxGen}} across all TARs in the minute.</li>
 *   <li>PUT {@code gc_idx/{day}/{HHmm}.idx} with the merged index.</li>
 *   <li>Load the result into the in-memory {@link #inMemoryIndex}.</li>
 * </ol>
 *
 * <p><b>GC decision</b>: a minute-dir is safe to delete when, for every shard in its index,
 * {@code maxGen(shard) ≤ globalCheckpoint(shard)}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchiveGcScanner {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveGcScanner.class);

    /** Sub-directory under the archive base where GC index files are stored. */
    public static final String GC_IDX_DIR = "gc_idx";

    /** Max TAR blobs to list per minute-dir (safety cap). */
    private static final int MAX_TARS_PER_MINUTE = 50_000;

    /** Max .idx files to list per day. */
    private static final int MAX_IDX_PER_DAY = 10_000;

    /**
     * Upper bound for the GC prefix read in one range-GET:
     * {@code 2 + 4096 * GC_SUMMARY_BYTES_PER_SHARD = 2 + 4096 * 32 = 131,074 bytes ≈ 128 KB}.
     * S3 charges per request, not per byte — this is effectively free.
     * 4,096 shards/node is a generous upper bound covering any realistic cluster configuration.
     */
    static final int MAX_GC_PREFIX_BYTES = 2 + 4096 * TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD;

    private final TransferService transferService;
    private final BlobPath archiveBasePath;

    /**
     * In-memory GC index: {@code "yyyyMMdd/HHmm" → MinuteGcIndex}.
     * Populated by {@link #scan(Instant)} and queried by {@link #isSafeToDelete}.
     */
    private final Map<String, MinuteGcIndex> inMemoryIndex = new ConcurrentHashMap<>();

    /**
     * Rolling per-shard checkpoint: {@code shardId → max(lastSyncedGlobalCheckpoint)} across all scanned TARs.
     * Updated in chronological minute order so the max is always the latest known value.
     * Used in Phase 2 of {@link #isSafeToDelete} and {@link #isTarSafeToDelete}.
     */
    private final Map<Integer, Long> rollingCheckpoints = new ConcurrentHashMap<>();

    /**
     * Latest max seqNo ever observed per shard across all scanned minute-dirs.
     * Used by {@link #isShardStuck}: a shard is stuck when its rolling checkpoint
     * has not yet advanced past its latest observed maxSeqNo.
     * Bounded by cluster topology (total shards in cluster).
     */
    private final Map<Integer, Long> latestMaxSeqNo = new ConcurrentHashMap<>();

    // Note: No per-TAR GC cache is maintained.
    // During deleteStuckMinutePartially(), GC prefix is re-read via range-GET for each TAR.
    // This avoids storing ~160KB per TAR (2,000 shards × 80 bytes) in memory for all scanned
    // minutes (which would be ~14.8GB at steady state). Since stuck minutes are rare (only
    // during node outages), the extra range-GETs cost ~$2.64 one-time when the node recovers —
    // a small price to avoid catastrophic memory growth.

    public TranslogArchiveGcScanner(TransferService transferService, BlobPath archiveBasePath) {
        this.transferService = transferService;
        this.archiveBasePath = archiveBasePath;
    }

    /**
     * Returns the in-memory GC index (for testing / external queries).
     */
    public Map<String, MinuteGcIndex> getInMemoryIndex() {
        return Collections.unmodifiableMap(inMemoryIndex);
    }

    // ── Scan ──────────────────────────────────────────────────────────────────

    /**
     * Scans new minute-dirs under {@code txlog/} that do not yet have a {@code .idx} in {@code gc_idx/},
     * builds the merged GC index, persists it, and loads it into memory.
     *
     * @param now current time (used to determine which day dirs to scan)
     */
    public void scan(Instant now) {
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(archiveBasePath);
        BlobPath gcIdxRoot = gcIdxRootPath(archiveBasePath);

        try {
            Set<String> dayDirs = transferService.listFolders(txlogRoot);
            if (dayDirs == null || dayDirs.isEmpty()) return;

            for (String dayDir : dayDirs) {
                scanDay(txlogRoot, gcIdxRoot, dayDir);
            }
        } catch (IOException e) {
            logger.warn("GC scanner: failed to list txlog root: {}", e.getMessage());
        }
    }

    /**
     * Loads the existing {@code .idx} files from {@code gc_idx/} into memory.
     * Called on cluster-manager startup to restore in-memory state without re-scanning TARs.
     */
    public void loadFromPersisted() {
        BlobPath gcIdxRoot = gcIdxRootPath(archiveBasePath);
        try {
            Set<String> dayDirs = transferService.listFolders(gcIdxRoot);
            if (dayDirs == null || dayDirs.isEmpty()) return;

            for (String dayDir : dayDirs) {
                BlobPath dayPath = gcIdxRoot.add(dayDir);
                List<BlobMetadata> idxBlobs;
                try {
                    idxBlobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                        f -> transferService.listAllInSortedOrder(dayPath, "", MAX_IDX_PER_DAY, f)
                    );
                } catch (IOException e) {
                    logger.warn("GC scanner: failed to list gc_idx day dir {}: {}", dayDir, e.getMessage());
                    continue;
                }
                if (idxBlobs == null) continue;

                for (BlobMetadata blob : idxBlobs) {
                    if (!blob.name().endsWith(".idx")) continue;
                    String minuteKey = minuteKeyFromIdxBlob(dayDir, blob.name());
                    try {
                        byte[] bytes;
                        try (InputStream is = transferService.downloadBlob(dayPath, blob.name())) {
                            bytes = is.readAllBytes();
                        }
                        MinuteGcIndex idx = MinuteGcIndex.deserialize(bytes);
                        inMemoryIndex.put(minuteKey, idx);
                        // Restore rolling checkpoints from persisted state
                        for (MinuteGcIndex.ShardRange shard : idx.shards()) {
                            rollingCheckpoints.merge(shard.getShardId(), shard.getMaxCheckpoint(), Math::max);
                        }
                    } catch (IOException e) {
                        logger.warn("GC scanner: failed to load idx {}/{}: {}", dayDir, blob.name(), e.getMessage());
                    }
                }
            }
            logger.info("GC scanner: loaded {} minute-indexes from gc_idx/", inMemoryIndex.size());
        } catch (IOException e) {
            logger.warn("GC scanner: failed to list gc_idx root: {}", e.getMessage());
        }
    }

    /**
     * Checks if a minute-dir is safe to delete using a two-phase check.
     *
     * <p><b>Phase 1 — Immediate check</b> (no newer TAR needed):
     * For each shard in this minute, if {@code entry.maxCheckpoint ≥ entry.maxSeqNo} at upload time,
     * the ops were already committed to remote segments when the TAR was written. Safe immediately.
     *
     * <p><b>Phase 2 — Rolling check</b> (newer TAR advanced the checkpoint):
     * For shards not yet safe by Phase 1, check {@code rollingCheckpoints[shard] ≥ maxSeqNo}.
     * The rolling checkpoint is the max globalCheckpoint ever seen across all scanned TARs,
     * including newer minutes.
     *
     * <p>This two-phase design correctly handles idle shards (Phase 1) and active shards
     * where the checkpoint advanced in a later minute (Phase 2) — without requiring any
     * extra S3 uploads.
     *
     * <p>Special cases:
     * <ul>
     *   <li>{@code idx == null}: not yet scanned → {@code false} (conservative)</li>
     *   <li>{@code idx.size() == 0}: scanned, no GC shards → {@code true} (safe)</li>
     * </ul>
     *
     * @param minuteKey {@code "yyyyMMdd/HHmm"} key identifying the minute-dir
     * @return true if all shards in this minute have their ops durably committed
     */
    public boolean isSafeToDelete(String minuteKey) {
        MinuteGcIndex idx = inMemoryIndex.get(minuteKey);
        if (idx == null) {
            return false; // Not yet scanned — conservatively hold
        }
        if (idx.size() == 0) {
            return true; // Scanned, no GC shards → safe
        }
        for (MinuteGcIndex.ShardRange shard : idx.shards()) {
            // Phase 1: immediate — checkpoint at upload time already covered all ops
            if (shard.getMaxCheckpoint() >= shard.getMaxSeqNo()) {
                continue; // This shard is immediately safe
            }
            // Phase 2: rolling — a newer TAR has since advanced the checkpoint past maxSeqNo
            Long latestCheckpoint = rollingCheckpoints.get(shard.getShardId());
            if (latestCheckpoint == null || shard.getMaxSeqNo() > latestCheckpoint) {
                return false; // Still waiting for checkpoint to advance
            }
        }
        return true;
    }

    /**
     * Returns true if a shard is currently "stuck" — its rolling checkpoint has not yet
     * advanced past its latest observed maxSeqNo across all scanned minute-dirs.
     *
     * <p>A stuck shard means at least one TAR containing it cannot yet be safely deleted.
     * Used for per-TAR granularity: TARs whose shards are all non-stuck can be deleted
     * even when their minute-dir contains other stuck shards from a different node.
     *
     * @param shardId shard identifier
     * @return true if the shard's checkpoint has not caught up to its latest observed seqNo
     */
    public boolean isShardStuck(int shardId) {
        Long checkpoint = rollingCheckpoints.get(shardId);
        Long maxSeq = latestMaxSeqNo.get(shardId);
        if (checkpoint == null || maxSeq == null) return true; // unknown → conservative
        return checkpoint < maxSeq;
    }

    /**
     * Checks if a specific TAR blob is safe to delete using the two-phase check applied
     * to each shard in the TAR's GC entries.
     *
     * <p>This enables per-TAR granularity: even when a minute-dir is not entirely safe
     * (because one node's shard is stuck), individual TARs from other nodes whose shards
     * are all safe can still be deleted immediately.
     *
     * @param gcEntries GC shard entries from this specific TAR's {@code _index} prefix
     * @return true if all shards in this TAR have their ops durably committed
     */
    public boolean isTarSafeToDelete(List<TarArchiveBuilder.GcShardEntry> gcEntries) {
        if (gcEntries.isEmpty()) return true; // No GC shards → safe
        for (TarArchiveBuilder.GcShardEntry e : gcEntries) {
            // Phase 1: immediate — checkpoint at upload time already covered all ops
            if (e.getGlobalCheckpoint() >= e.getMaxSeqNo()) continue;
            // Phase 2: rolling — checkpoint advanced in a later minute
            if (isShardStuck(e.getShardId())) return false;
        }
        return true;
    }

    /**
     * Returns the rolling checkpoint map (for testing / inspection).
     */
    public Map<Integer, Long> getRollingCheckpoints() {
        return Collections.unmodifiableMap(rollingCheckpoints);
    }

    /**
     * Returns the latestMaxSeqNo map (for testing / inspection).
     */
    public Map<Integer, Long> getLatestMaxSeqNo() {
        return Collections.unmodifiableMap(latestMaxSeqNo);
    }

    /**
     * Removes a minute-key from the in-memory index
     * (called after successful deletion of the minute-dir or all its TARs).
     */
    public void evict(String minuteKey) {
        inMemoryIndex.remove(minuteKey);
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void scanDay(BlobPath txlogRoot, BlobPath gcIdxRoot, String dayDir) {
        BlobPath dayTxlogPath = txlogRoot.add(dayDir);
        BlobPath dayGcIdxPath = gcIdxRoot.add(dayDir);

        // Find already-indexed minute-dirs for this day
        Set<String> alreadyIndexed = getAlreadyIndexedMinutes(dayGcIdxPath);

        // List all minute-dirs in txlog for this day
        Set<String> minuteDirs;
        try {
            minuteDirs = transferService.listFolders(dayTxlogPath);
        } catch (IOException e) {
            logger.warn("GC scanner: failed to list txlog day dir {}: {}", dayDir, e.getMessage());
            return;
        }
        if (minuteDirs == null || minuteDirs.isEmpty()) return;

        // Sort minute-dirs in chronological order (HHmm lexicographic = chronological).
        // Critical for rolling checkpoint correctness: newer minutes must be processed last
        // so rollingCheckpoints accumulates the maximum checkpoint seen up to the latest minute.
        List<String> sortedMinuteDirs = new ArrayList<>(minuteDirs);
        Collections.sort(sortedMinuteDirs);

        for (String minuteDir : sortedMinuteDirs) {
            String minuteKey = dayDir + "/" + minuteDir;
            if (alreadyIndexed.contains(minuteDir)) {
                // Already scanned: ensure loaded in memory (also updates rollingCheckpoints)
                if (!inMemoryIndex.containsKey(minuteKey)) {
                    loadSingleIdx(dayGcIdxPath, dayDir, minuteDir);
                }
                continue;
            }
            scanMinute(dayTxlogPath, dayGcIdxPath, dayDir, minuteDir);
        }
    }

    private Set<String> getAlreadyIndexedMinutes(BlobPath dayGcIdxPath) {
        try {
            List<BlobMetadata> idxBlobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                f -> transferService.listAllInSortedOrder(dayGcIdxPath, "", MAX_IDX_PER_DAY, f)
            );
            if (idxBlobs == null) return Collections.emptySet();
            Set<String> result = ConcurrentHashMap.newKeySet();
            for (BlobMetadata b : idxBlobs) {
                if (b.name().endsWith(".idx")) {
                    // strip ".idx" → minute string e.g. "1000"
                    result.add(b.name().substring(0, b.name().length() - 4));
                }
            }
            return result;
        } catch (IOException e) {
            logger.debug("GC scanner: no gc_idx day dir yet ({}): {}", dayGcIdxPath.buildAsString(), e.getMessage());
            return Collections.emptySet();
        }
    }

    private void loadSingleIdx(BlobPath dayGcIdxPath, String dayDir, String minuteDir) {
        String blobName = minuteDir + ".idx";
        String minuteKey = dayDir + "/" + minuteDir;
        try {
            byte[] bytes;
            try (InputStream is = transferService.downloadBlob(dayGcIdxPath, blobName)) {
                bytes = is.readAllBytes();
            }
            inMemoryIndex.put(minuteKey, MinuteGcIndex.deserialize(bytes));
        } catch (IOException e) {
            logger.warn("GC scanner: failed to load {}: {}", blobName, e.getMessage());
        }
    }

    /**
     * Scans all TARs in one minute-dir, merges their GC summaries, persists the result,
     * and loads it into the in-memory index.
     */
    void scanMinute(BlobPath dayTxlogPath, BlobPath dayGcIdxPath, String dayDir, String minuteDir) {
        BlobPath minutePath = dayTxlogPath.add(minuteDir);
        String minuteKey = dayDir + "/" + minuteDir;

        // LIST all TAR blobs in this minute-dir
        List<BlobMetadata> tarBlobs;
        try {
            tarBlobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                f -> transferService.listAllInSortedOrder(minutePath, "", MAX_TARS_PER_MINUTE, f)
            );
        } catch (IOException e) {
            logger.warn("GC scanner: failed to list minute dir {}/{}: {}", dayDir, minuteDir, e.getMessage());
            return;
        }
        if (tarBlobs == null || tarBlobs.isEmpty()) return;

        // Merge GC summaries from all TARs.
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        for (BlobMetadata blob : tarBlobs) {
            if (!blob.name().endsWith(".tar")) continue;
            List<TarArchiveBuilder.GcShardEntry> gcEntries = readGcPrefix(minutePath, blob.name());
            builder.merge(gcEntries);
            // Update latestMaxSeqNo for per-TAR stuck detection (isShardStuck)
            for (TarArchiveBuilder.GcShardEntry e : gcEntries) {
                latestMaxSeqNo.merge(e.getShardId(), e.getMaxSeqNo(), Math::max);
            }
        }

        // Always build and persist the .idx — even if empty (no GC entries found in any TAR).
        // An absent .idx unambiguously means "not yet scanned"; an empty .idx means
        // "scanned, all TARs had zero GC shards → safe to delete by timestamp".
        if (builder.isEmpty()) {
            logger.debug("GC scanner: no GC entries in minute {}/{} — persisting empty .idx", dayDir, minuteDir);
        }

        MinuteGcIndex idx = builder.build();

        // Persist .idx blob synchronously using latch
        String idxBlobName = minuteDir + ".idx";
        byte[] idxBytes = idx.serialize();
        try {
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Exception> uploadErr = new java.util.concurrent.atomic.AtomicReference<>();
            transferService.uploadBlob(
                new java.io.ByteArrayInputStream(idxBytes),
                dayGcIdxPath,
                idxBlobName,
                org.opensearch.common.blobstore.stream.write.WritePriority.NORMAL,
                new org.opensearch.core.action.ActionListener<Void>() {
                    @Override public void onResponse(Void v) { latch.countDown(); }
                    @Override public void onFailure(Exception e) { uploadErr.set(e); latch.countDown(); }
                }
            );
            latch.await();
            if (uploadErr.get() != null) {
                throw new IOException("Failed to upload gc_idx blob", uploadErr.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("GC scanner: interrupted persisting gc_idx {}/{}", dayDir, idxBlobName);
            return; // Don't load in-memory without durable persistence — caller will retry next cycle
        } catch (IOException e) {
            logger.warn("GC scanner: failed to persist gc_idx {}/{}: {}", dayDir, idxBlobName, e.getMessage());
            return; // Don't load in-memory: "absent .idx = not scanned" invariant must hold
        }

        // Only update in-memory state after successful persistence.
        // This preserves the invariant: absent .idx means "not yet scanned".
        inMemoryIndex.put(minuteKey, idx);

        // Update rolling checkpoints with this minute's per-shard maxCheckpoint.
        // Called after persistence to ensure durability-memory consistency.
        for (MinuteGcIndex.ShardRange shard : idx.shards()) {
            rollingCheckpoints.merge(shard.getShardId(), shard.getMaxCheckpoint(), Math::max);
        }

        logger.debug("GC scanner: indexed minute {}/{} with {} shards", dayDir, minuteDir, idx.size());
    }

    /**
     * Reads the GC summary prefix from a single TAR blob using one range-GET.
     *
     * <p>S3 charges per request, not per byte — reading extra bytes is free.
     * We fetch the TAR header (512B) + GC prefix in a single range-GET:
     * <pre>
     *   bytes [0, 512 + MAX_GC_PREFIX_BYTES) → TAR header + GC prefix
     * </pre>
     * The TAR header gives us the true index size; we parse the GC prefix
     * directly from the same buffer without a second round-trip.
     *
     * @param minutePath blob path to the minute-dir
     * @param blobName   TAR blob name
     * @return list of GC shard entries, or empty on any error
     */
    public List<TarArchiveBuilder.GcShardEntry> readGcPrefix(BlobPath minutePath, String blobName) {
        try {
            // Single range-GET: TAR header (512B) + enough bytes for the GC prefix.
            // GC prefix max size = 2 + MAX_SHARDS_PER_NODE * GC_SUMMARY_BYTES_PER_SHARD.
            // We use a generous upper bound; extra bytes cost nothing on S3.
            long readLength = TarArchiveBuilder.TAR_BLOCK + MAX_GC_PREFIX_BYTES;
            byte[] buf;
            try (InputStream is = transferService.downloadBlob(minutePath, blobName, 0L, readLength)) {
                buf = is.readAllBytes();
            }

            if (buf.length < TarArchiveBuilder.TAR_BLOCK) {
                return Collections.emptyList();
            }

            // Parse index data size from TAR header size field (bytes 124-135, octal ASCII)
            String sizeOctal = new String(buf, 124, 12, StandardCharsets.US_ASCII).trim().replace("\0", "");
            long indexDataSize;
            try {
                indexDataSize = Long.parseLong(sizeOctal, 8);
            } catch (NumberFormatException e) {
                logger.debug("GC scanner: invalid TAR header size in {}: '{}'", blobName, sizeOctal);
                return Collections.emptyList();
            }
            if (indexDataSize < 2) {
                return Collections.emptyList();
            }

            // Extract the GC prefix bytes from the combined buffer (starts at offset 512)
            int gcPrefixOffset = TarArchiveBuilder.TAR_BLOCK;
            int gcPrefixAvailable = buf.length - gcPrefixOffset;
            if (gcPrefixAvailable < 2) {
                return Collections.emptyList();
            }
            byte[] gcPrefixBytes = Arrays.copyOfRange(buf, gcPrefixOffset, buf.length);

            return TarArchiveBuilder.parseGcSummary(gcPrefixBytes);
        } catch (IOException e) {
            logger.debug("GC scanner: failed to read GC prefix from {}: {}", blobName, e.getMessage());
            return Collections.emptyList();
        }
    }

    // ── Path helpers ──────────────────────────────────────────────────────────

    /**
     * Returns the root path for GC index files: {@code {archiveBasePath}/gc_idx/}.
     */
    public static BlobPath gcIdxRootPath(BlobPath archiveBasePath) {
        return archiveBasePath.add(GC_IDX_DIR);
    }

    /**
     * Builds the minute key {@code "yyyyMMdd/HHmm"} from a day string and idx blob name.
     *
     * @param dayDir  e.g. "20260502"
     * @param idxBlob e.g. "1000.idx"
     * @return e.g. "20260502/1000"
     */
    static String minuteKeyFromIdxBlob(String dayDir, String idxBlob) {
        String minute = idxBlob.endsWith(".idx") ? idxBlob.substring(0, idxBlob.length() - 4) : idxBlob;
        return dayDir + "/" + minute;
    }
}
