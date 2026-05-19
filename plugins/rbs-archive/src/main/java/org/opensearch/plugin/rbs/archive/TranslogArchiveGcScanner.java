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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.transfer.TransferService;







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
 */
final class TranslogArchiveGcScanner {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveGcScanner.class);

    /** Sub-directory under the archive base where GC index files are stored. */
    public static final String GC_IDX_DIR = "gc_idx";

    /** Max TAR blobs to list per minute-dir (safety cap). */
    private static final int MAX_TARS_PER_MINUTE = 50_000;

    /** Max .idx files to list per day. */
    private static final int MAX_IDX_PER_DAY = 10_000;

    /**
     * Upper bound for the GC prefix read in one range-GET (index-grouped format):
     * {@code 2 (numIndices) + maxIndices * (18 (header) + maxShardsPerIndex * 28 (per shard))}.
     * Assuming max 512 indices/node and max 8 shards/index = 4096 shards/node:
     * {@code 2 + 512 * (18 + 8 * 28) = 2 + 512 * 242 = 123,906 bytes ≈ 121 KB}.
     * Conservative upper bound: use 2 + 4096 * (18 + 28) to handle worst case (1 shard/index).
     * S3 charges per request, not per byte — this is effectively free.
     */
    static final int MAX_GC_PREFIX_BYTES = 2 + 4096 * (TarArchiveBuilder.GC_INDEX_HEADER_BYTES + TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD);

    private final TransferService transferService;
    private final BlobPath archiveBasePath;

    /**
     * In-memory GC index: {@code "yyyyMMdd/HHmm" → MinuteGcIndex}.
     * Populated by {@link #scan(Instant)} and queried by {@link #isSafeToDelete}.
     */
    private final Map<String, MinuteGcIndex> inMemoryIndex = new ConcurrentHashMap<>();

    /**
     * Rolling per-(indexUUID, shardId) checkpoint:
     * {@code indexUUID → shardId → max(lastSyncedGlobalCheckpoint)} across all scanned TARs.
     * Updated in chronological minute order so the max is always the latest known value.
     * Used in Phase 2 of {@link #isSafeToDelete} and {@link #isTarSafeToDelete}.
     * Two-level map avoids collisions between different indices that share the same shard ID.
     */
    private final Map<String, Map<Integer, Long>> rollingCheckpoints = new ConcurrentHashMap<>();

    /**
     * Latest max seqNo ever observed per (indexUUID, shardId) across all scanned minute-dirs.
     * Used by {@link #isShardStuck}: a shard is stuck when its rolling checkpoint
     * has not yet advanced past its latest observed maxSeqNo.
     * Bounded by cluster topology (total shards in cluster).
     */
    private final Map<String, Map<Integer, Long>> latestMaxSeqNo = new ConcurrentHashMap<>();

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

        // Only scan today and yesterday: all older data has already been GC-deleted,
        // so there is nothing to index there. This avoids an unbounded S3 LIST over
        // all historical day-dirs (which could be 30+ dirs if the cluster was long-running).
        String today = TranslogArchivePathHelper.dayDir(now);
        String yesterday = TranslogArchivePathHelper.dayDir(now.minus(java.time.Duration.ofDays(1)));

        for (String dayDir : new String[]{ yesterday, today }) {
            try {
                scanDay(txlogRoot, gcIdxRoot, dayDir);
            } catch (Exception e) {
                logger.warn("GC scanner: failed to scan txlog day dir {}: {}", dayDir, e.getMessage());
            }
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
                        // Restore rolling checkpoints from persisted state (2-level: indexUUID → shardId)
                        for (String indexUUID : idx.indexUUIDs()) {
                            Map<Integer, Long> indexCheckpoints = rollingCheckpoints
                                .computeIfAbsent(indexUUID, k -> new ConcurrentHashMap<>());
                            for (MinuteGcIndex.ShardRange shard : idx.shards(indexUUID).values()) {
                                indexCheckpoints.merge(shard.getShardId(), shard.getMaxCheckpoint(), Math::max);
                            }
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
     * Uses {@code null} liveIndexUUIDs (conservative: no liveness check, same as before).
     *
     * @param minuteKey {@code "yyyyMMdd/HHmm"} key identifying the minute-dir
     * @return true if all shards in this minute have their ops durably committed
     */
    public boolean isSafeToDelete(String minuteKey) {
        return isSafeToDelete(minuteKey, null);
    }

    /**
     * Checks if a minute-dir is safe to delete using a two-phase check, with optional
     * index liveness information.
     *
     * <p>For each (indexUUID, shard) entry in the minute:
     * <ul>
     *   <li>If {@code liveIndexUUIDs} is non-null and does NOT contain the indexUUID →
     *       index was deleted → this shard's entry is unconditionally safe (skip).</li>
     *   <li><b>Phase 1</b>: {@code maxCheckpoint ≥ maxSeqNo} at upload time → ops in remote segments → safe.</li>
     *   <li><b>Phase 2</b>: {@code rollingCheckpoints[indexUUID][shardId] ≥ maxSeqNo} → newer TAR confirmed → safe.</li>
     *   <li>Otherwise → hold (not safe yet).</li>
     * </ul>
     *
     * <p>Special cases:
     * <ul>
     *   <li>{@code idx == null}: not yet scanned → {@code false} (conservative)</li>
     *   <li>{@code idx.isEmpty()}: scanned, no GC shards → {@code true} (safe)</li>
     * </ul>
     *
     * @param minuteKey      {@code "yyyyMMdd/HHmm"} key identifying the minute-dir
     * @param liveIndexUUIDs set of index UUIDs currently alive in cluster state; null → skip liveness check
     * @return true if all shard entries in this minute are safe to delete
     */
    public boolean isSafeToDelete(String minuteKey, Set<String> liveIndexUUIDs) {
        MinuteGcIndex idx = inMemoryIndex.get(minuteKey);
        if (idx == null) {
            return false; // Not yet scanned — conservatively hold
        }
        if (idx.isEmpty()) {
            return true; // Scanned, no GC shards → safe
        }
        for (String indexUUID : idx.indexUUIDs()) {
            // If index is gone from cluster state, all its shard data is unconditionally safe
            if (liveIndexUUIDs != null && !liveIndexUUIDs.contains(indexUUID)) {
                continue;
            }
            for (MinuteGcIndex.ShardRange shard : idx.shards(indexUUID).values()) {
                // Phase 1: immediate — checkpoint at upload time already covered all ops
                if (shard.getMaxCheckpoint() >= shard.getMaxSeqNo()) {
                    continue;
                }
                // Phase 2: rolling — a newer TAR has since advanced the checkpoint past maxSeqNo
                Map<Integer, Long> indexCheckpoints = rollingCheckpoints.get(indexUUID);
                Long latestCheckpoint = indexCheckpoints != null ? indexCheckpoints.get(shard.getShardId()) : null;
                if (latestCheckpoint == null || shard.getMaxSeqNo() > latestCheckpoint) {
                    return false; // Still waiting for checkpoint to advance
                }
            }
        }
        return true;
    }

    /**
     * Returns true if a shard is currently "stuck" — its rolling checkpoint has not yet
     * advanced past its latest observed maxSeqNo across all scanned minute-dirs.
     *
     * @param indexUUID index UUID (required to disambiguate shards across indices)
     * @param shardId   shard identifier
     * @return true if the shard's checkpoint has not caught up to its latest observed seqNo
     */
    public boolean isShardStuck(String indexUUID, int shardId) {
        Map<Integer, Long> indexCheckpoints = rollingCheckpoints.get(indexUUID);
        Map<Integer, Long> indexMaxSeqNos = latestMaxSeqNo.get(indexUUID);
        if (indexCheckpoints == null || indexMaxSeqNos == null) return true; // unknown → conservative
        Long checkpoint = indexCheckpoints.get(shardId);
        Long maxSeq = indexMaxSeqNos.get(shardId);
        if (checkpoint == null || maxSeq == null) return true;
        return checkpoint < maxSeq;
    }

    /**
     * Checks if a specific TAR blob is safe to delete.
     * Uses {@code null} liveIndexUUIDs (conservative: no liveness check).
     */
    public boolean isTarSafeToDelete(List<TarArchiveBuilder.GcShardEntry> gcEntries) {
        return isTarSafeToDelete(gcEntries, null);
    }

    /**
     * Checks if a specific TAR blob is safe to delete using the two-phase check applied
     * to each shard in the TAR's GC entries, with optional index liveness information.
     *
     * <p>For each shard entry:
     * <ul>
     *   <li>If {@code liveIndexUUIDs} non-null and index not in set → deleted → unconditionally safe (skip).</li>
     *   <li>Phase 1: {@code globalCheckpoint ≥ maxSeqNo} → safe.</li>
     *   <li>Phase 2: {@code !isShardStuck(indexUUID, shardId)} → rolling checkpoint covered → safe.</li>
     *   <li>Otherwise → hold.</li>
     * </ul>
     *
     * @param gcEntries      GC shard entries from this specific TAR's {@code _index} prefix
     * @param liveIndexUUIDs set of index UUIDs currently alive in cluster state; null → skip liveness check
     * @return true if all shards in this TAR have their ops durably committed
     */
    public boolean isTarSafeToDelete(List<TarArchiveBuilder.GcShardEntry> gcEntries, Set<String> liveIndexUUIDs) {
        if (gcEntries.isEmpty()) return true; // No GC shards → safe
        for (TarArchiveBuilder.GcShardEntry e : gcEntries) {
            // Deleted index: unconditionally safe (no recovery possible)
            if (liveIndexUUIDs != null && !liveIndexUUIDs.contains(e.getIndexUUID())) {
                continue;
            }
            // Phase 1: immediate — checkpoint at upload time already covered all ops
            if (e.getGlobalCheckpoint() >= e.getMaxSeqNo()) continue;
            // Phase 2: rolling — checkpoint advanced in a later minute
            if (isShardStuck(e.getIndexUUID(), e.getShardId())) return false;
        }
        return true;
    }

    /**
     * Returns the rolling checkpoint map (for testing / inspection).
     */
    public Map<String, Map<Integer, Long>> getRollingCheckpoints() {
        return Collections.unmodifiableMap(rollingCheckpoints);
    }

    /**
     * Returns the latestMaxSeqNo map (for testing / inspection).
     */
    public Map<String, Map<Integer, Long>> getLatestMaxSeqNo() {
        return Collections.unmodifiableMap(latestMaxSeqNo);
    }

    /**
     * Removes a minute-key from the in-memory index
     * (called after successful deletion of the minute-dir or all its TARs).
     */
    public void evict(String minuteKey) {
        inMemoryIndex.remove(minuteKey);
    }

    /**
     * Evicts a minute-key from the in-memory index and deletes the corresponding
     * {@code gc_idx/{dayDir}/{minuteDir}.idx} blob from remote storage.
     *
     * <p>The S3 delete is best-effort: if it fails, the eviction from memory still proceeds
     * and a warning is logged. The orphaned {@code .idx} file will be cleaned up on the next
     * reconciliation pass in {@link #scanDay}.
     *
     * @param dayDir    e.g. {@code "20260502"}
     * @param minuteDir e.g. {@code "1000"}
     */
    public void evictAndDeleteIdx(String dayDir, String minuteDir) {
        String minuteKey = dayDir + "/" + minuteDir;
        inMemoryIndex.remove(minuteKey);
        BlobPath gcIdxDayPath = gcIdxRootPath(archiveBasePath).add(dayDir);
        String idxBlobName = minuteDir + ".idx";
        try {
            transferService.deleteBlobs(gcIdxDayPath, List.of(idxBlobName));
            logger.debug("GC scanner: deleted gc_idx {}/{}", dayDir, idxBlobName);
        } catch (IOException e) {
            logger.warn("GC scanner: failed to delete gc_idx {}/{}: {}", dayDir, idxBlobName, e.getMessage());
        }
    }

    /**
     * Evicts all in-memory state for a deleted index.
     * Called when an index is deleted so stale checkpoint data doesn't block GC
     * of future indices that reuse the same shard IDs.
     *
     * @param indexUUID the UUID of the deleted index
     */
    public void evictIndex(String indexUUID) {
        rollingCheckpoints.remove(indexUUID);
        latestMaxSeqNo.remove(indexUUID);
        // Note: inMemoryIndex (MinuteGcIndex) entries are keyed by minute-key, not indexUUID.
        // Those will be cleaned up naturally when the minute-dir is deleted via GC.
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

        // Reconciliation: delete orphaned gc_idx entries whose txlog minute-dir no longer exists.
        // This happens when GC deletes all TARs in a minute but the .idx blob was not cleaned up
        // (e.g. due to a transient failure or a prior code version that didn't delete .idx on evict).
        // Batch all orphan deletes into a single deleteBlobs() call (GC-003).
        Set<String> txlogMinutes = new java.util.HashSet<>(sortedMinuteDirs);
        List<String> orphanBlobs = new ArrayList<>();
        for (String indexedMinute : alreadyIndexed) {
            if (!txlogMinutes.contains(indexedMinute)) {
                logger.debug("GC scanner: orphaned gc_idx {}/{}.idx (no txlog minute-dir) — queued for deletion", dayDir, indexedMinute);
                inMemoryIndex.remove(dayDir + "/" + indexedMinute);
                orphanBlobs.add(indexedMinute + ".idx");
            }
        }
        if (!orphanBlobs.isEmpty()) {
            BlobPath gcIdxDayPath = gcIdxRootPath(archiveBasePath).add(dayDir);
            try {
                transferService.deleteBlobs(gcIdxDayPath, orphanBlobs);
                logger.debug("GC scanner: batch-deleted {} orphaned gc_idx blobs in {}", orphanBlobs.size(), dayDir);
            } catch (IOException e) {
                logger.warn("GC scanner: failed to batch-delete orphaned gc_idx blobs in {}: {}", dayDir, e.getMessage());
            }
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
            MinuteGcIndex idx = MinuteGcIndex.deserialize(bytes);
            inMemoryIndex.put(minuteKey, idx);
            // Update rolling checkpoints — required for isSafeToDelete Phase 2.
            // Without this, scanDay() calls that hit "already indexed" minutes via loadSingleIdx()
            // would populate inMemoryIndex but leave rollingCheckpoints empty, causing Phase 2
            // to always return false and blocking all GC deletions on the second and subsequent
            // scan cycles (when all minutes already have .idx files).
            for (String indexUUID : idx.indexUUIDs()) {
                Map<Integer, Long> indexCheckpoints = rollingCheckpoints
                    .computeIfAbsent(indexUUID, k -> new ConcurrentHashMap<>());
                for (MinuteGcIndex.ShardRange shard : idx.shards(indexUUID).values()) {
                    indexCheckpoints.merge(shard.getShardId(), shard.getMaxCheckpoint(), Math::max);
                }
            }
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
            // Update latestMaxSeqNo for per-TAR stuck detection (isShardStuck), keyed by (indexUUID, shardId)
            for (TarArchiveBuilder.GcShardEntry e : gcEntries) {
                latestMaxSeqNo.computeIfAbsent(e.getIndexUUID(), k -> new ConcurrentHashMap<>())
                    .merge(e.getShardId(), e.getMaxSeqNo(), Math::max);
            }
        }

        // Always build and persist the .idx — even if empty (no GC entries found in any TAR).
        // An absent .idx unambiguously means "not yet scanned"; an empty .idx means
        // "scanned, all TARs had zero GC shards → safe to delete by timestamp".
        if (builder.isEmpty()) {
            logger.debug("GC scanner: no GC entries in minute {}/{} — persisting empty .idx", dayDir, minuteDir);
        }

        MinuteGcIndex idx = builder.build();

        // Persist .idx blob synchronously using uploadBlobStream (raw bytes, no codec checksum wrapping).
        // uploadBlob() calls checksumOfChecksum() which expects bytes to already contain an OpenSearch
        // codec footer — our raw serialized index bytes do not, causing checksum corruption errors.
        // uploadBlobStream() writes directly to the blob container without any checksum processing.
        String idxBlobName = minuteDir + ".idx";
        byte[] idxBytes = idx.serialize();
        try {
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            transferService.uploadBlob(
                new java.io.ByteArrayInputStream(idxBytes),
                dayGcIdxPath,
                idxBlobName,
                org.opensearch.common.blobstore.stream.write.WritePriority.NORMAL,
                future
            );
            future.actionGet();
        } catch (IOException e) {
            logger.warn("GC scanner: failed to persist gc_idx {}/{}: {}", dayDir, idxBlobName, e.getMessage());
            return; // Don't load in-memory: "absent .idx = not scanned" invariant must hold
        }

        // Only update in-memory state after successful persistence.
        // This preserves the invariant: absent .idx means "not yet scanned".
        inMemoryIndex.put(minuteKey, idx);

        // Update rolling checkpoints with this minute's per-(indexUUID, shardId) maxCheckpoint.
        // Called after persistence to ensure durability-memory consistency.
        for (String indexUUID : idx.indexUUIDs()) {
            Map<Integer, Long> indexCheckpoints = rollingCheckpoints
                .computeIfAbsent(indexUUID, k -> new ConcurrentHashMap<>());
            for (MinuteGcIndex.ShardRange shard : idx.shards(indexUUID).values()) {
                indexCheckpoints.merge(shard.getShardId(), shard.getMaxCheckpoint(), Math::max);
            }
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
