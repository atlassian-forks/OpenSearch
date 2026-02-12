/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Format for the archive index stored in the ZIP EOCD comment.
 * Optional first line: {@code indexUUID=<uuid>} for retention (so comment can be used for deletable check).
 * Then one line per shard: shardId,primaryTerm,gen,tlogOffset,tlogLength,ckpOffset,ckpLength
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class ArchiveCommentFormat {

    private static final String INDEX_UUID_PREFIX = "indexUUID=";

    private ArchiveCommentFormat() {}

    /**
     * Serialize index entries to comment string (one line per entry, newline-separated).
     */
    public static String serialize(List<ArchiveIndexEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append(entries.get(i).toLine());
        }
        return sb.toString();
    }

    /**
     * Serialize with indexUUID prefix so retention can use comment for deletable check without parsing CD.
     */
    public static String serializeWithIndex(String indexUUID, List<ArchiveIndexEntry> entries) {
        if (indexUUID == null || indexUUID.isEmpty()) {
            return serialize(entries);
        }
        return INDEX_UUID_PREFIX + indexUUID + "\n" + serialize(entries);
    }

    /**
     * Parse comment string to list of index entries. Skips optional first line {@code indexUUID=...}.
     */
    public static List<ArchiveIndexEntry> parse(String comment) {
        List<ArchiveIndexEntry> result = new ArrayList<>();
        if (comment == null || comment.isEmpty()) {
            return result;
        }
        String body = comment;
        if (comment.startsWith(INDEX_UUID_PREFIX)) {
            int firstNewline = comment.indexOf('\n');
            body = firstNewline >= 0 ? comment.substring(firstNewline + 1) : "";
        }
        for (String line : body.split("\n")) {
            ArchiveIndexEntry entry = ArchiveIndexEntry.fromLine(line.trim());
            if (entry != null) {
                result.add(entry);
            }
        }
        return result;
    }

    /**
     * Parse comment that may include indexUUID prefix. Returns indexUUID (or null) and list of entries.
     * Used by retention to build full paths for isArchiveDeletable.
     */
    public static ParseResult parseWithIndex(String comment) {
        if (comment == null || comment.isEmpty()) {
            return new ParseResult(null, new ArrayList<>());
        }
        String indexUUID = null;
        String body = comment;
        if (comment.startsWith(INDEX_UUID_PREFIX)) {
            int firstNewline = comment.indexOf('\n');
            if (firstNewline >= 0) {
                indexUUID = comment.substring(INDEX_UUID_PREFIX.length(), firstNewline).trim();
                body = comment.substring(firstNewline + 1);
            } else {
                indexUUID = comment.substring(INDEX_UUID_PREFIX.length()).trim();
                body = "";
            }
        }
        List<ArchiveIndexEntry> entries = new ArrayList<>();
        for (String line : body.split("\n")) {
            ArchiveIndexEntry entry = ArchiveIndexEntry.fromLine(line.trim());
            if (entry != null) {
                entries.add(entry);
            }
        }
        return new ParseResult(indexUUID, entries);
    }

    /**
     * Result of parsing comment with optional indexUUID.
     */
    @ExperimentalApi
    public static final class ParseResult {
        private final String indexUUID;
        private final List<ArchiveIndexEntry> entries;

        public ParseResult(String indexUUID, List<ArchiveIndexEntry> entries) {
            this.indexUUID = indexUUID;
            this.entries = entries != null ? new ArrayList<>(entries) : new ArrayList<>();
        }

        public String getIndexUUID() {
            return indexUUID;
        }

        public List<ArchiveIndexEntry> getEntries() {
            return entries;
        }
    }

    /**
     * Convert index entries (from comment) to ArchiveEntry list for isArchiveDeletable. Requires indexUUID.
     * Two entries per shard: .tlog and .ckp path (offset/length are 0 since only path is used for deletable check).
     */
    public static List<ArchiveEntry> toArchiveEntriesForRetention(String indexUUID, List<ArchiveIndexEntry> indexEntries) {
        if (indexUUID == null || indexEntries == null || indexEntries.isEmpty()) {
            return new ArrayList<>();
        }
        List<ArchiveEntry> out = new ArrayList<>();
        for (ArchiveIndexEntry e : indexEntries) {
            String prefix = indexUUID + "/" + e.getShardId() + "/" + e.getPrimaryTerm() + "/";
            out.add(new ArchiveEntry(prefix + "translog-" + e.getGeneration() + ".tlog", 0L, 0L));
            out.add(new ArchiveEntry(prefix + "translog-" + e.getGeneration() + ".ckp", 0L, 0L));
        }
        return out;
    }

    /**
     * Parse comment and return map keyed by "shardId,primaryTerm,generation" for lookup.
     */
    public static Map<String, ArchiveIndexEntry> parseToMap(String comment) {
        Map<String, ArchiveIndexEntry> map = new HashMap<>();
        for (ArchiveIndexEntry entry : parse(comment)) {
            map.put(entry.key(), entry);
        }
        return map;
    }

    /**
     * One (path, offset, length) record when building the archive.
     */
    @ExperimentalApi
    public static final class PathOffsetLength {
        private final String path;
        private final long offset;
        private final long length;

        public PathOffsetLength(String path, long offset, long length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }

        public String getPath() {
            return path;
        }

        public long getOffset() {
            return offset;
        }

        public long getLength() {
            return length;
        }
    }

    /**
     * Build index entries from (path, offset, length) list. Path format: indexUUID/shardId/primaryTerm/translog-N.tlog or .ckp.
     * Groups by (shardId, primaryTerm, gen) and produces one ArchiveIndexEntry per shard.
     *
     * @return list of index entries (one per shard); empty if paths don't match or grouping fails
     */
    public static List<ArchiveIndexEntry> fromPathOffsetList(List<PathOffsetLength> pathOffsets) {
        // key = shardId,primaryTerm,gen -> { tlogOff, tlogLen, ckpOff, ckpLen }
        Map<String, long[]> byShard = new HashMap<>();
        for (PathOffsetLength po : pathOffsets) {
            Optional<ArchivePathParser.ParsedMemberPath> parsed = ArchivePathParser.parseMemberPath(po.getPath());
            if (parsed.isEmpty()) {
                continue;
            }
            ArchivePathParser.ParsedMemberPath p = parsed.get();
            String key = p.getShardId() + "," + p.getPrimaryTerm() + "," + p.getGeneration();
            long[] vals = byShard.computeIfAbsent(key, k -> new long[] { -1, -1, -1, -1 });
            if (p.isTlog()) {
                vals[0] = po.getOffset();
                vals[1] = po.getLength();
            } else {
                vals[2] = po.getOffset();
                vals[3] = po.getLength();
            }
        }
        List<ArchiveIndexEntry> result = new ArrayList<>();
        for (Map.Entry<String, long[]> e : byShard.entrySet()) {
            String key = e.getKey();
            long[] v = e.getValue();
            if (v[0] < 0 || v[1] < 0 || v[2] < 0 || v[3] < 0) {
                continue;
            }
            String[] parts = key.split(",", -1);
            if (parts.length != 3) {
                continue;
            }
            try {
                int shardId = Integer.parseInt(parts[0]);
                long primaryTerm = Long.parseLong(parts[1]);
                long generation = Long.parseLong(parts[2]);
                result.add(new ArchiveIndexEntry(shardId, primaryTerm, generation, v[0], v[1], v[2], v[3]));
            } catch (NumberFormatException ex) {
                // skip
            }
        }
        return result;
    }
}
