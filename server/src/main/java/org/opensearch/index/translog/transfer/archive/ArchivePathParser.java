/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * Shared parser for archive member paths: {@code indexUUID/shardId/primaryTerm/translog-<gen>.tlog|.ckp}.
 * Used by {@link ArchiveCommentFormat} and {@link ArchiveDeletionHelper} to avoid duplicated logic.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class ArchivePathParser {

    private ArchivePathParser() {}

    /**
     * Result of parsing a member path. Contains indexUUID, shardId, primaryTerm, generation, and whether
     * the file is a .tlog (true) or .ckp (false).
     */
    @ExperimentalApi
    public static final class ParsedMemberPath {
        private final String indexUUID;
        private final int shardId;
        private final long primaryTerm;
        private final long generation;
        private final boolean isTlog;

        public ParsedMemberPath(String indexUUID, int shardId, long primaryTerm, long generation, boolean isTlog) {
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.isTlog = isTlog;
        }

        public String getIndexUUID() {
            return indexUUID;
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

        public boolean isTlog() {
            return isTlog;
        }
    }

    /**
     * Parse member path to (indexUUID, shardId, primaryTerm, generation, isTlog). Path format:
     * indexUUID/shardId/primaryTerm/translog-&lt;gen&gt;.tlog or .ckp.
     *
     * @return empty if path does not match the convention
     */
    public static Optional<ParsedMemberPath> parseMemberPath(String path) {
        if (path == null || path.isEmpty()) {
            return Optional.empty();
        }
        String[] parts = path.split("/");
        if (parts.length != 4) {
            return Optional.empty();
        }
        String indexUUID = parts[0];
        int shardId;
        long primaryTerm;
        try {
            shardId = Integer.parseInt(parts[1]);
            primaryTerm = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        String filename = parts[3];
        long generation = parseGenerationFromFilename(filename);
        if (generation < 0) {
            return Optional.empty();
        }
        boolean isTlog = filename.endsWith(".tlog");
        if (!isTlog && !filename.endsWith(".ckp")) {
            return Optional.empty();
        }
        return Optional.of(new ParsedMemberPath(indexUUID, shardId, primaryTerm, generation, isTlog));
    }

    /**
     * Parse generation from filename translog-&lt;gen&gt;.tlog or translog-&lt;gen&gt;.ckp.
     *
     * @return generation, or -1 if filename does not match
     */
    public static long parseGenerationFromFilename(String filename) {
        if (filename == null || !filename.startsWith("translog-")) {
            return -1;
        }
        int start = 9;
        int end;
        if (filename.endsWith(".tlog")) {
            end = filename.length() - 5;
        } else if (filename.endsWith(".ckp")) {
            end = filename.length() - 4;
        } else {
            return -1;
        }
        if (end <= start) {
            return -1;
        }
        try {
            return Long.parseLong(filename.substring(start, end));
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
