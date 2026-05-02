/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-minute merged GC index: tracks the min/maxGen for each shard across all TARs in one minute-dir.
 *
 * <p>Built by {@link TranslogArchiveGcScanner} after merging GC summaries from all TAR blobs in a
 * minute-dir. Persisted to {@code gc_idx/{day}/{HHmm}.idx} and held in memory on the cluster-manager.
 *
 * <p>Binary serialization format ({@code .idx} file):
 * <pre>
 *   [2 bytes: numShards (unsigned short)]
 *   Per shard:
 *     [4 bytes: shardId]
 *     [8 bytes: minGen]
 *     [8 bytes: maxGen]
 *     [4 bytes: uuidHash]
 *   = 18 bytes/shard + 2 byte header
 * </pre>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class MinuteGcIndex {

    /**
     * Bytes per shard entry in the serialized {@code .idx} file.
     * Layout: [4 shardId][8 minGen][8 maxGen][4 uuidHash][8 maxCheckpoint] = 32 bytes.
     */
    static final int BYTES_PER_SHARD = TarArchiveBuilder.GC_SUMMARY_BYTES_PER_SHARD;

    /** Merged shard entries: shardId → ShardRange */
    private final Map<Integer, ShardRange> shards;

    public MinuteGcIndex(Map<Integer, ShardRange> shards) {
        this.shards = Collections.unmodifiableMap(shards);
    }

    /** Returns all shard ranges in this minute index. */
    public Collection<ShardRange> shards() {
        return shards.values();
    }

    /** Returns the shard range for the given shardId, or null if not present. */
    public ShardRange get(int shardId) {
        return shards.get(shardId);
    }

    /** Number of shards tracked in this minute. */
    public int size() {
        return shards.size();
    }

    /**
     * Merged generation range + max known global checkpoint for one shard in one minute-dir,
     * accumulated across all TAR blobs in that minute.
     *
     * <p>{@code maxCheckpoint} is the maximum {@code globalCheckpoint} seen across all TARs
     * in this minute for this shard. It represents the latest checkpoint observed at upload
     * time within this minute, used by the scanner's rolling checkpoint map.
     */
    @ExperimentalApi
    public static final class ShardRange {
        private final int shardId;
        private final long minSeqNo;
        private final long maxSeqNo;
        private final int uuidHash;
        private final long maxCheckpoint;

        public ShardRange(int shardId, long minSeqNo, long maxSeqNo, int uuidHash, long maxCheckpoint) {
            this.shardId = shardId;
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.uuidHash = uuidHash;
            this.maxCheckpoint = maxCheckpoint;
        }

        public int getShardId() { return shardId; }
        public long getMinSeqNo() { return minSeqNo; }
        public long getMaxSeqNo() { return maxSeqNo; }
        public int getUuidHash() { return uuidHash; }
        /** Max globalCheckpoint seen across all TARs in this minute for this shard. */
        public long getMaxCheckpoint() { return maxCheckpoint; }

        /** Merge with another range for the same shard: expand gen range, take max checkpoint. */
        public ShardRange merge(ShardRange other) {
            return new ShardRange(
                shardId,
                Math.min(minSeqNo, other.minSeqNo),
                Math.max(maxSeqNo, other.maxSeqNo),
                uuidHash,
                Math.max(maxCheckpoint, other.maxCheckpoint)
            );
        }
    }

    // ── Builder ────────────────────────────────────────────────────────────────

    /**
     * Mutable builder: accumulates GC entries from multiple TARs in one minute-dir.
     */
    public static final class Builder {
        private final Map<Integer, ShardRange> shards = new HashMap<>();

        /**
         * Merges a list of {@link TarArchiveBuilder.GcShardEntry} from one TAR into this builder.
         */
        public void merge(java.util.List<TarArchiveBuilder.GcShardEntry> gcEntries) {
            for (TarArchiveBuilder.GcShardEntry entry : gcEntries) {
                ShardRange incoming = new ShardRange(
                    entry.getShardId(), entry.getMinSeqNo(), entry.getMaxSeqNo(),
                    entry.getUuidHash(), entry.getGlobalCheckpoint()
                );
                shards.merge(entry.getShardId(), incoming, ShardRange::merge);
            }
        }

        /** Builds the immutable {@link MinuteGcIndex}. */
        public MinuteGcIndex build() {
            return new MinuteGcIndex(new HashMap<>(shards));
        }

        public boolean isEmpty() {
            return shards.isEmpty();
        }
    }

    // ── Serialization ──────────────────────────────────────────────────────────

    /**
     * Serializes this index to bytes for storage as a {@code .idx} blob.
     * Format mirrors the GC prefix in the TAR {@code _index} (same layout, reusable parser).
     */
    public byte[] serialize() {
        int numShards = shards.size();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(2 + numShards * BYTES_PER_SHARD);
        buf.putShort((short) numShards);
        for (ShardRange r : shards.values()) {
            buf.putInt(r.getShardId());
            buf.putLong(r.getMinSeqNo());
            buf.putLong(r.getMaxSeqNo());
            buf.putInt(r.getUuidHash());
            buf.putLong(r.getMaxCheckpoint());
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
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
        int numShards = buf.getShort() & 0xFFFF;
        int required = 2 + numShards * BYTES_PER_SHARD;
        if (bytes.length < required) {
            return new MinuteGcIndex(Collections.emptyMap());
        }
        Map<Integer, ShardRange> result = new HashMap<>(numShards);
        for (int i = 0; i < numShards; i++) {
            int shardId = buf.getInt();
            long minSeqNo = buf.getLong();
            long maxSeqNo = buf.getLong();
            int uuidHash = buf.getInt();
            long maxCheckpoint = buf.getLong();
            result.put(shardId, new ShardRange(shardId, minSeqNo, maxSeqNo, uuidHash, maxCheckpoint));
        }
        return new MinuteGcIndex(result);
    }
}
