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

    // ── Builder / merge ───────────────────────────────────────────────────────

    public void testBuilderSingleShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        List<TarArchiveBuilder.GcShardEntry> entries = List.of(
            new TarArchiveBuilder.GcShardEntry(0, 5L, 10L, 0xABCD, 100L)
        );
        builder.merge(entries);

        MinuteGcIndex idx = builder.build();
        assertEquals(1, idx.size());
        MinuteGcIndex.ShardRange r = idx.get(0);
        assertNotNull(r);
        assertEquals(0, r.getShardId());
        assertEquals(5L, r.getMinSeqNo());
        assertEquals(10L, r.getMaxSeqNo());
        assertEquals(0xABCD, r.getUuidHash());
    }

    public void testBuilderMergesTwoTarsForSameShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        // TAR 1: shard 0, gen 5-10
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(0, 5L, 10L, 0x1111, 100L)));
        // TAR 2: shard 0, gen 8-15  (overlapping)
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(0, 8L, 15L, 0x1111, 100L)));

        MinuteGcIndex idx = builder.build();
        assertEquals(1, idx.size());
        MinuteGcIndex.ShardRange r = idx.get(0);
        assertEquals(5L, r.getMinSeqNo());   // min(5, 8) = 5
        assertEquals(15L, r.getMaxSeqNo());  // max(10, 15) = 15
    }

    public void testBuilderMultipleShards() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(Arrays.asList(
            new TarArchiveBuilder.GcShardEntry(0, 1L, 3L, 0xAAA, 100L),
            new TarArchiveBuilder.GcShardEntry(1, 10L, 20L, 0xBBB, 100L)
        ));
        MinuteGcIndex idx = builder.build();
        assertEquals(2, idx.size());
        assertEquals(3L, idx.get(0).getMaxSeqNo());
        assertEquals(20L, idx.get(1).getMaxSeqNo());
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
    }

    public void testSerializeDeserializeOneShard() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(7, 100L, 200L, 0xDEAD, 100L)));
        MinuteGcIndex idx = builder.build();

        byte[] bytes = idx.serialize();
        MinuteGcIndex roundtripped = MinuteGcIndex.deserialize(bytes);

        assertEquals(1, roundtripped.size());
        MinuteGcIndex.ShardRange r = roundtripped.get(7);
        assertNotNull(r);
        assertEquals(7, r.getShardId());
        assertEquals(100L, r.getMinSeqNo());
        assertEquals(200L, r.getMaxSeqNo());
        assertEquals(0xDEAD, r.getUuidHash());
    }

    public void testSerializeDeserializeMultipleShards() {
        MinuteGcIndex.Builder builder = new MinuteGcIndex.Builder();
        for (int i = 0; i < 10; i++) {
            builder.merge(List.of(new TarArchiveBuilder.GcShardEntry(i, i * 10L, i * 10L + 5L, i, (long)(i * 10 + 5))));
        }
        MinuteGcIndex idx = builder.build();
        byte[] bytes = idx.serialize();
        MinuteGcIndex roundtripped = MinuteGcIndex.deserialize(bytes);

        assertEquals(10, roundtripped.size());
        for (int i = 0; i < 10; i++) {
            MinuteGcIndex.ShardRange r = roundtripped.get(i);
            assertNotNull(r);
            assertEquals(i * 10L, r.getMinSeqNo());
            assertEquals(i * 10L + 5L, r.getMaxSeqNo());
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
        MinuteGcIndex.ShardRange a = new MinuteGcIndex.ShardRange(0, 5L, 15L, 0x1, 15L);
        MinuteGcIndex.ShardRange b = new MinuteGcIndex.ShardRange(0, 3L, 20L, 0x1, 20L);
        MinuteGcIndex.ShardRange merged = a.merge(b);
        assertEquals(3L, merged.getMinSeqNo());
        assertEquals(20L, merged.getMaxSeqNo());
    }

    // ── minuteKeyFromIdxBlob ──────────────────────────────────────────────────

    public void testMinuteKeyFromIdxBlob() {
        assertEquals("20260502/1000", TranslogArchiveGcScanner.minuteKeyFromIdxBlob("20260502", "1000.idx"));
        assertEquals("20260502/2359", TranslogArchiveGcScanner.minuteKeyFromIdxBlob("20260502", "2359.idx"));
    }
}
