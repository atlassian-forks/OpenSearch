/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * One shard's entry in the archive index (ZIP comment): (shardId, primaryTerm, generation) and byte ranges for .tlog and .ckp.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class ArchiveIndexEntry {

    private final int shardId;
    private final long primaryTerm;
    private final long generation;
    private final long tlogOffset;
    private final long tlogLength;
    private final long ckpOffset;
    private final long ckpLength;

    public ArchiveIndexEntry(
        int shardId,
        long primaryTerm,
        long generation,
        long tlogOffset,
        long tlogLength,
        long ckpOffset,
        long ckpLength
    ) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.tlogOffset = tlogOffset;
        this.tlogLength = tlogLength;
        this.ckpOffset = ckpOffset;
        this.ckpLength = ckpLength;
    }

    public int getShardId() {
        return shardId;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getTlogOffset() {
        return tlogOffset;
    }

    public long getTlogLength() {
        return tlogLength;
    }

    public long getCkpOffset() {
        return ckpOffset;
    }

    public long getCkpLength() {
        return ckpLength;
    }

    public String key() {
        return shardId + "," + primaryTerm + "," + generation;
    }

    /** Serialize to one CSV line: shardId,primaryTerm,gen,tlogOff,tlogLen,ckpOff,ckpLen */
    public String toLine() {
        return shardId + "," + primaryTerm + "," + generation + "," + tlogOffset + "," + tlogLength + "," + ckpOffset + "," + ckpLength;
    }

    /**
     * Parse a line from the comment (same format as toLine()).
     *
     * @return parsed entry or null if line is invalid
     */
    public static ArchiveIndexEntry fromLine(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        String[] parts = line.split(",", -1);
        if (parts.length != 7) {
            return null;
        }
        try {
            int shardId = Integer.parseInt(parts[0]);
            long primaryTerm = Long.parseLong(parts[1]);
            long generation = Long.parseLong(parts[2]);
            long tlogOffset = Long.parseLong(parts[3]);
            long tlogLength = Long.parseLong(parts[4]);
            long ckpOffset = Long.parseLong(parts[5]);
            long ckpLength = Long.parseLong(parts[6]);
            return new ArchiveIndexEntry(shardId, primaryTerm, generation, tlogOffset, tlogLength, ckpOffset, ckpLength);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArchiveIndexEntry that = (ArchiveIndexEntry) o;
        return shardId == that.shardId && primaryTerm == that.primaryTerm && generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, primaryTerm, generation);
    }
}
