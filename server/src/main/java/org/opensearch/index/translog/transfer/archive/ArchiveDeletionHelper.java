/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Helper for deciding if a translog archive ZIP is safe to delete based on retention bounds per shard.
 * Path convention inside archive: {@code indexUUID/shardId/primaryTerm/translog-<gen>.tlog|.ckp}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class ArchiveDeletionHelper {

    private ArchiveDeletionHelper() {}

    /**
     * Retention bounds for a shard: do not delete generations >= minGenerationToKeep in this primary term,
     * and do not delete any files from primary terms >= minPrimaryTermToKeep.
     */
    @ExperimentalApi
    public static final class RetentionBounds {
        private final long minPrimaryTermToKeep;
        private final long minGenerationToKeep;

        public RetentionBounds(long minPrimaryTermToKeep, long minGenerationToKeep) {
            this.minPrimaryTermToKeep = minPrimaryTermToKeep;
            this.minGenerationToKeep = minGenerationToKeep;
        }

        public long getMinPrimaryTermToKeep() {
            return minPrimaryTermToKeep;
        }

        public long getMinGenerationToKeep() {
            return minGenerationToKeep;
        }
    }

    /**
     * Parsed entry from an archive member path: shard key (indexUUID/shardId), primary term, generation.
     */
    @ExperimentalApi
    public static final class ParsedEntry {
        private final String shardKey;
        private final long primaryTerm;
        private final long generation;

        public ParsedEntry(String shardKey, long primaryTerm, long generation) {
            this.shardKey = shardKey;
            this.primaryTerm = primaryTerm;
            this.generation = generation;
        }

        public String getShardKey() {
            return shardKey;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public long getGeneration() {
            return generation;
        }
    }

    /**
     * Parse member path to shard key and (primaryTerm, generation). Path format: indexUUID/shardId/primaryTerm/filename.
     * Filename: translog-&lt;gen&gt;.tlog or translog-&lt;gen&gt;.ckp.
     *
     * @return empty if path does not match the convention
     */
    public static Optional<ParsedEntry> parsePath(String path) {
        return ArchivePathParser.parseMemberPath(path)
            .map(p -> new ParsedEntry(p.getIndexUUID() + "/" + p.getShardId(), p.getPrimaryTerm(), p.getGeneration()));
    }

    /**
     * Returns true if every entry in the archive is deletable under the given retention bounds.
     * An entry (primaryTerm, generation) is deletable if primaryTerm &lt; minPrimaryTermToKeep for that shard,
     * or (primaryTerm == minPrimaryTermToKeep and generation &lt; minGenerationToKeep).
     * If an entry cannot be parsed or has no retention bounds, the archive is not deletable (conservative).
     */
    public static boolean isArchiveDeletable(List<ArchiveEntry> entries, Map<String, RetentionBounds> retentionByShard) {
        if (entries == null || entries.isEmpty()) {
            return true;
        }
        if (retentionByShard == null || retentionByShard.isEmpty()) {
            return false;
        }
        for (ArchiveEntry entry : entries) {
            Optional<ParsedEntry> parsed = parsePath(entry.getPath());
            if (parsed.isEmpty()) {
                return false;
            }
            ParsedEntry p = parsed.get();
            RetentionBounds bounds = retentionByShard.get(p.getShardKey());
            if (bounds == null) {
                return false;
            }
            boolean entryDeletable = p.getPrimaryTerm() < bounds.getMinPrimaryTermToKeep()
                || (p.getPrimaryTerm() == bounds.getMinPrimaryTermToKeep() && p.getGeneration() < bounds.getMinGenerationToKeep());
            if (!entryDeletable) {
                return false;
            }
        }
        return true;
    }
}
