/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MinuteGcIndexTests extends OpenSearchTestCase {

    private static final String UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String UUID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    // ── Builder / merge ───────────────────────────────────────────────────────

    public void testBuilderSingleShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        List<TarArchiveBuilder.GcShardEntry> entries = List.of(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 5L, 10L, 100L)
        );
        builder.merge(entries);

        MinuteGcIndex idx = builder.build();
        assertEquals(1, idx.size());
        MinuteGcIndex.ShardRange r = idx.get(UUID_A, 0);
        assertNotNull(r);
        assertEquals(UUID_A, r.getIndexUUID());
        assertEquals(0, r.getShardId());
        assertEquals(5L, r.getMinSeqNo());
        assertEquals(10L, r.getMaxSeqNo());
        assertEquals(100L, r.getMaxCheckpoint());
    }

    public void testBuilderMergesTwoTarsForSameShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        // TAR 1: shard 0, seqNo 5-10, checkpoint 8
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 5L, 10L, 8L)));
        // TAR 2: shard 0, seqNo 8-15, checkpoint 12 (overlapping)
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 8L, 15L, 12L)));

        MinuteGcIndex idx = builder.build();
        assertEquals(1, idx.size());
        MinuteGcIndex.ShardRange r = idx.get(UUID_A, 0);
        assertNotNull(r);
        assertEquals(5L, r.getMinSeqNo());   // min(5, 8) = 5
        assertEquals(15L, r.getMaxSeqNo());  // max(10, 15) = 15
        assertEquals(12L, r.getMaxCheckpoint()); // max(8, 12) = 12
    }

    public void testBuilderMultipleShardsSameIndex() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 3L, 3L),
            new TarArchiveBuilder.GcShardEntry(UUID_A, 1, 10L, 20L, 20L)
        ));
        MinuteGcIndex idx = builder.build();
        assertEquals(2, idx.size());
        assertEquals(3L, idx.get(UUID_A, 0).getMaxSeqNo());
        assertEquals(20L, idx.get(UUID_A, 1).getMaxSeqNo());
    }

    public void testBuilderMultipleIndices() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 5L, 5L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 10L, 20L, 20L)
        ));
        MinuteGcIndex idx = builder.build();
        assertEquals(2, idx.size());
        assertEquals(2, idx.indexUUIDs().size());
        assertTrue(idx.indexUUIDs().contains(UUID_A));
        assertTrue(idx.indexUUIDs().contains(UUID_B));
        assertEquals(5L, idx.get(UUID_A, 0).getMaxSeqNo());
        assertEquals(20L, idx.get(UUID_B, 0).getMaxSeqNo());
    }

    public void testBuilderSameShardIdDifferentIndices() {
        // shard 0 of UUID_A and shard 0 of UUID_B must not collide
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(UUID_A, 0, 1L, 50L, 30L),
            new TarArchiveBuilder.GcShardEntry(UUID_B, 0, 100L, 200L, 150L)
        ));
        MinuteGcIndex idx = builder.build();
        MinuteGcIndex.ShardRange a = idx.get(UUID_A, 0);
        MinuteGcIndex.ShardRange b = idx.get(UUID_B, 0);
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(50L, a.getMaxSeqNo());
        assertEquals(200L, b.getMaxSeqNo());
        assertEquals(30L, a.getMaxCheckpoint());
        assertEquals(150L, b.getMaxCheckpoint());
    }

    public void testBuilderEmptyMerge() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(Collections.emptyList());
        assertTrue(builder.isEmpty());
    }

    // ── Serialize / deserialize roundtrip ─────────────────────────────────────

    public void testSerializeDeserializeEmpty() {
        MinuteGcIndex idx = new MinuteGcIndex(Collections.emptyMap());
        byte[] bytes = idx.serialize();
        MinuteGcIndex roundtripped = MinuteGcIndex.deserialize(bytes);
        assertEquals(0, roundtripped.size());
        assertTrue(roundtripped.isEmpty());
    }

    public void testSerializeDeserializeOneShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, 7, 100L, 200L, 180L)));
        MinuteGcIndex idx = builder.build();

        byte[] bytes = idx.serialize();
        MinuteGcIndex roundtripped = MinuteGcIndex.deserialize(bytes);

        assertEquals(1, roundtripped.size());
        MinuteGcIndex.ShardRange r = roundtripped.get(UUID_A, 7);
        assertNotNull(r);
        assertEquals(UUID_A, r.getIndexUUID());
        assertEquals(7, r.getShardId());
        assertEquals(100L, r.getMinSeqNo());
        assertEquals(200L, r.getMaxSeqNo());
        assertEquals(180L, r.getMaxCheckpoint());
    }

    public void testSerializeDeserializeMultipleShards() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        for (int i = 0; i < 5; i++) {
            builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_A, i, i * 10L, i * 10L + 5L, (long)(i * 10 + 5))));
        }
        for (int i = 0; i < 3; i++) {
            builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(UUID_B, i, i * 100L, i * 100L + 50L, (long)(i * 100 + 50))));
        }
        MinuteGcIndex idx = builder.build();
        byte[] bytes = idx.serialize();
        MinuteGcIndex roundtripped = MinuteGcIndex.deserialize(bytes);

        assertEquals(8, roundtripped.size());
        for (int i = 0; i < 5; i++) {
            MinuteGcIndex.ShardRange r = roundtripped.get(UUID_A, i);
            assertNotNull("Missing UUID_A shard " + i, r);
            assertEquals(i * 10L, r.getMinSeqNo());
            assertEquals(i * 10L + 5L, r.getMaxSeqNo());
        }
        for (int i = 0; i < 3; i++) {
            MinuteGcIndex.ShardRange r = roundtripped.get(UUID_B, i);
            assertNotNull("Missing UUID_B shard " + i, r);
            assertEquals(i * 100L, r.getMinSeqNo());
        }
    }

    public void testDeserializeTruncatedBytesReturnsEmpty() {
        MinuteGcIndex result = MinuteGcIndex.deserialize(new byte[]{0x00}); // only 1 byte, needs 2
        assertEquals(0, result.size());
    }

    public void testDeserializeNullBytesReturnsEmpty() {
        MinuteGcIndex result = MinuteGcIndex.deserialize(null);
        assertEquals(0, result.size());
    }

    // ── ShardRange merge ──────────────────────────────────────────────────────

    public void testShardRangeMerge() {
        MinuteGcIndex.ShardRange a = new MinuteGcIndex.ShardRange(UUID_A, 0, 5L, 15L, 15L);
        MinuteGcIndex.ShardRange b = new MinuteGcIndex.ShardRange(UUID_A, 0, 3L, 20L, 20L);
        MinuteGcIndex.ShardRange merged = a.merge(b);
        assertEquals(3L, merged.getMinSeqNo());
        assertEquals(20L, merged.getMaxSeqNo());
        assertEquals(20L, merged.getMaxCheckpoint());
    }

    // ── minuteKeyFromIdxBlob ──────────────────────────────────────────────────

    public void testMinuteKeyFromIdxBlob() {
        assertEquals("20260502/1000", TranslogArchiveGcScanner.minuteKeyFromIdxBlob("20260502", "1000.idx"));
        assertEquals("20260502/2359", TranslogArchiveGcScanner.minuteKeyFromIdxBlob("20260502", "2359.idx"));
    }
}
