/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Per-minute merged GC index: tracks the min/maxSeqNo and max globalCheckpoint for each
 * (indexUUID, shardId) pair across all TARs in one minute-dir.
 *
 * <p>Built by {@link TranslogArchiveGcScanner} after merging GC summaries from all TAR blobs in a
 * minute-dir. Persisted to {@code gc_idx/{day}/{HHmm}.idx} and held in memory on the cluster-manager.
 *
 * <p>Binary serialization format ({@code .idx} file) mirrors the TAR GC prefix (index-grouped):
 * <pre>
 *   [2 bytes: numIndices (unsigned short)]
 *   Per index:
 *     [8 bytes: indexUUID most-significant bits  (long)]
 *     [8 bytes: indexUUID least-significant bits (long)]
 *     [2 bytes: numShards (unsigned short)]
 *     Per shard (28 bytes):
 *       [4 bytes: shardId]
 *       [8 bytes: minSeqNo]
 *       [8 bytes: maxSeqNo]
 *       [8 bytes: maxCheckpoint]
 * </pre>
 *
 */
final class MinuteGcIndex {

    /** Bytes per shard entry in the serialized format (matches TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD = 28). */
    static final int BYTES_PER_SHARD = TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD;

    /** Per-index header bytes in serialized format: 16 UUID + 2 numShards (matches GC_INDEX_HEADER_BYTES = 18). */
    static final int INDEX_HEADER_BYTES = TarArchiveBuilder.GC_INDEX_HEADER_BYTES;

    /** Merged shard entries: indexUUID → shardId → ShardRange */
    private final Map<String, Map<Integer, ShardRange>> shards;

    public MinuteGcIndex(Map<String, Map<Integer, ShardRange>> shards) {
        // Make deeply unmodifiable
        Map<String, Map<Integer, ShardRange>> immutable = new HashMap<>(shards.size());
        for (Map.Entry<String, Map<Integer, ShardRange>> e : shards.entrySet()) {
            immutable.put(e.getKey(), Collections.unmodifiableMap(new HashMap<>(e.getValue())));
        }
        this.shards = Collections.unmodifiableMap(immutable);
    }

    /** Returns all index UUIDs tracked in this minute. */
    public Set<String> indexUUIDs() {
        return shards.keySet();
    }

    /** Returns all shard ranges for a specific index, or empty map if index not present. */
    public Map<Integer, ShardRange> shards(String indexUUID) {
        return shards.getOrDefault(indexUUID, Collections.emptyMap());
    }

    /** Returns the shard range for a specific (indexUUID, shardId), or null if not present. */
    public ShardRange get(String indexUUID, int shardId) {
        Map<Integer, ShardRange> indexShards = shards.get(indexUUID);
        return indexShards != null ? indexShards.get(shardId) : null;
    }

    /** Returns all shard ranges across all indices (flattened). */
    public Collection<ShardRange> allShards() {
        List<ShardRange> result = new ArrayList<>();
        for (Map<Integer, ShardRange> indexShards : shards.values()) {
            result.addAll(indexShards.values());
        }
        return result;
    }

    /** Total number of shards tracked across all indices in this minute. */
    public int size() {
        int total = 0;
        for (Map<Integer, ShardRange> indexShards : shards.values()) {
            total += indexShards.size();
        }
        return total;
    }

    /** Returns true if no shards are tracked (empty minute). */
    public boolean isEmpty() {
        return shards.isEmpty();
    }

    /**
     * Merged generation range + max known global checkpoint for one (indexUUID, shardId) pair
     * in one minute-dir, accumulated across all TAR blobs in that minute.
     */
    public static final class ShardRange {
        private final String indexUUID;
        private final int shardId;
        private final long minSeqNo;
        private final long maxSeqNo;
        private final long maxCheckpoint;

        public ShardRange(String indexUUID, int shardId, long minSeqNo, long maxSeqNo, long maxCheckpoint) {
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.maxCheckpoint = maxCheckpoint;
        }

        public String getIndexUUID() { return indexUUID; }
        public int getShardId() { return shardId; }
        public long getMinSeqNo() { return minSeqNo; }
        public long getMaxSeqNo() { return maxSeqNo; }
        /** Max globalCheckpoint seen across all TARs in this minute for this (index, shard). */
        public long getMaxCheckpoint() { return maxCheckpoint; }

        /** Merge with another range for the same (indexUUID, shardId): expand seqNo range, take max checkpoint. */
        public ShardRange merge(ShardRange other) {
            return new ShardRange(
                indexUUID,
                shardId,
                Math.min(minSeqNo, other.minSeqNo),
                Math.max(maxSeqNo, other.maxSeqNo),
                Math.max(maxCheckpoint, other.maxCheckpoint)
            );
        }
    }

    // ── Builder ────────────────────────────────────────────────────────────────

    /**
     * Mutable builder: accumulates GC entries from multiple TARs in one minute-dir.
     */
    public static final class Builder {
        /** indexUUID → shardId → ShardRange */
        private final Map<String, Map<Integer, ShardRange>> shards = new HashMap<>();

        /**
         * Merges a list of {@link TarArchiveBuilder.GcShardEntry} from one TAR into this builder.
         */
        public void merge(List<TarArchiveBuilder.GcShardEntry> gcEntries) {
            for (TarArchiveBuilder.GcShardEntry entry : gcEntries) {
                String indexUUID = entry.getIndexUUID();
                int shardId = entry.getShardId();
                ShardRange incoming = new ShardRange(
                    indexUUID,
                    shardId,
                    entry.getMinSeqNo(),
                    entry.getMaxSeqNo(),
                    entry.getGlobalCheckpoint()
                );
                shards.computeIfAbsent(indexUUID, k -> new HashMap<>())
                    .merge(shardId, incoming, ShardRange::merge);
            }
        }

        /** Builds the immutable {@link MinuteGcIndex}. */
        public MinuteGcIndex build() {
            return new MinuteGcIndex(shards);
        }

        public boolean isEmpty() {
            return shards.isEmpty();
        }
    }

    // ── Serialization ──────────────────────────────────────────────────────────

    /**
     * Serializes this index to bytes for storage as a {@code .idx} blob.
     * Format mirrors the GC prefix in the TAR {@code _index} (index-grouped, same layout).
     */
    public byte[] serialize() {
        int numIndices = shards.size();
        // Compute total size: 2 (numIndices) + per-index: 18 header + numShards * 28
        int totalSize = 2;
        for (Map<Integer, ShardRange> indexShards : shards.values()) {
            totalSize += INDEX_HEADER_BYTES + indexShards.size() * BYTES_PER_SHARD;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putShort((short) numIndices);
        for (Map.Entry<String, Map<Integer, ShardRange>> indexEntry : shards.entrySet()) {
            long uuidMsb, uuidLsb;
            try {
                UUID uuid = UUID.fromString(indexEntry.getKey());
                uuidMsb = uuid.getMostSignificantBits();
                uuidLsb = uuid.getLeastSignificantBits();
            } catch (IllegalArgumentException e) {
                UUID uuid = UUID.nameUUIDFromBytes(indexEntry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                uuidMsb = uuid.getMostSignificantBits();
                uuidLsb = uuid.getLeastSignificantBits();
            }
            buf.putLong(uuidMsb);
            buf.putLong(uuidLsb);
            Map<Integer, ShardRange> indexShards = indexEntry.getValue();
            buf.putShort((short) indexShards.size());
            for (ShardRange r : indexShards.values()) {
                buf.putInt(r.getShardId());
                buf.putLong(r.getMinSeqNo());
                buf.putLong(r.getMaxSeqNo());
                buf.putLong(r.getMaxCheckpoint());
            }
        }
        return buf.array();
    }

    /**
     * Deserializes a {@link MinuteGcIndex} from bytes previously written by {@link #serialize()}.
     *
     * @param bytes raw bytes from a {@code .idx} blob
     * @return parsed index, or empty index on malformed input
     */
    public static MinuteGcIndex deserialize(byte[] bytes) {
        if (bytes == null || bytes.length < 2) {
            return new MinuteGcIndex(Collections.emptyMap());
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int numIndices = buf.getShort() & 0xFFFF;
        if (numIndices == 0) {
            return new MinuteGcIndex(Collections.emptyMap());
        }
        Map<String, Map<Integer, ShardRange>> result = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            if (buf.remaining() < INDEX_HEADER_BYTES) {
                return new MinuteGcIndex(Collections.emptyMap()); // truncated
            }
            long uuidMsb = buf.getLong();
            long uuidLsb = buf.getLong();
            String indexUUID = new UUID(uuidMsb, uuidLsb).toString();
            int numShards = buf.getShort() & 0xFFFF;
            if (buf.remaining() < numShards * BYTES_PER_SHARD) {
                return new MinuteGcIndex(Collections.emptyMap()); // truncated
            }
            Map<Integer, ShardRange> indexShards = new HashMap<>(numShards);
            for (int s = 0; s < numShards; s++) {
                int shardId = buf.getInt();
                long minSeqNo = buf.getLong();
                long maxSeqNo = buf.getLong();
                long maxCheckpoint = buf.getLong();
                indexShards.put(shardId, new ShardRange(indexUUID, shardId, minSeqNo, maxSeqNo, maxCheckpoint));
            }
            result.put(indexUUID, indexShards);
        }
        return new MinuteGcIndex(result);
    }
}
