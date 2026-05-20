/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Builds a TAR archive with a binary index entry as the first file.
 * <p>
 * Unlike ZIP, TAR headers precede each entry, so all byte offsets are fully deterministic
 * from file sizes alone — no content reads required before streaming begins. This enables
 * true single-pass streaming to S3 with a known {@code Content-Length}.
 * <p>
 * <b>Archive layout:</b>
 * <pre>
 *   [512-byte TAR header: name="_index"]
 *   [index bytes: binary offset table, padded to 512-byte boundary]
 *   [512-byte TAR header: name="uuid/shardId/primaryTerm/translog-N.tlog"]
 *   [tlog bytes, padded to 512-byte boundary]
 *   [512-byte TAR header: name="uuid/shardId/primaryTerm/translog-N.ckp"]
 *   [ckp bytes, padded to 512-byte boundary]
 *   ... (repeat for each shard)
 *   [two 512-byte zero blocks: end-of-archive marker]
 * </pre>
 *
 * <b>Index format (binary, fixed-size per entry):</b>
 * <pre>
 *   [4 bytes: num_entries (big-endian int)]
 *   Per entry:
 *     [2 bytes: path_len (big-endian short)]
 *     [path_len bytes: UTF-8 path]
 *     [8 bytes: data_offset (big-endian long, absolute from archive start)]
 *     [8 bytes: data_length (big-endian long, unpadded)]
 * </pre>
 *
 * <b>Recovery:</b> Range-read bytes [512, 512+index_size) to get the index,
 * then range-read each entry's [data_offset, data_offset+data_length) directly.
 * The index is always at a fixed offset (512 bytes from start) — no need to know
 * total archive size (unlike ZIP which stores the index at the tail).
 *
 */
final class TarArchiveBuilder {

    /** TAR block size — all headers and padded data are multiples of this. */
    public static final int TAR_BLOCK = 512;

    /** Name of the index entry (always first in the archive). */
    static final String INDEX_ENTRY_NAME = "_index";

    /**
     * GC summary prefix layout (embedded at the head of the binary {@code _index}),
     * grouped by index UUID to avoid repeating the UUID per shard:
     * <pre>
     *   [2 bytes: numIndices (unsigned short, big-endian)]
     *   Per index:
     *     [8 bytes: indexUUID most-significant bits  (long)]
     *     [8 bytes: indexUUID least-significant bits (long)]
     *     [2 bytes: numShards (unsigned short, big-endian)]
     *     Per shard (24 bytes):
     *       [4 bytes: shardId           (int)]
     *       [8 bytes: minSeqNo          (long)]
     *       [8 bytes: maxSeqNo          (long)]
     *       [8 bytes: globalCheckpoint  (long, value at TAR upload time)]
     * </pre>
     * Per-index header overhead: {@code 16 + 2 = 18 bytes}.
     * Per-shard overhead: {@code 4 (shardId) + 8 (minSeqNo) + 8 (maxSeqNo) + 8 (globalCheckpoint) = 28 bytes}.
     */
    public static final int GC_SUMMARY_BYTES_PER_SHARD = 28;

    /** Per-index header size in the GC prefix: 16 bytes UUID + 2 bytes numShards. */
    public static final int GC_INDEX_HEADER_BYTES = 18;

    /**
     * Immutable GC summary entry for one shard, embedded in each TAR's {@code _index}.
     *
     * <p>{@code globalCheckpoint} is the value known to the data node at TAR upload time.
     * The GC scanner takes the {@code max(globalCheckpoint)} across all TARs in all minute-dirs
     * to obtain the latest known checkpoint for each shard, which is then used to decide
     * whether old minute-dirs are safe to delete.
     */
    public static final class GcShardEntry {
        private final String indexUUID;
        private final int shardId;
        private final long minSeqNo;
        private final long maxSeqNo;
        private final long globalCheckpoint;

        /**
         * @param indexUUID        full index UUID string (used as grouping key, avoids hash collisions)
         * @param shardId          shard identifier
         * @param minSeqNo         minimum sequence number of ops in this TAR's translog files for this shard
         * @param maxSeqNo         maximum sequence number of ops in this TAR's translog files for this shard
         * @param globalCheckpoint global checkpoint (seqNo) on the data node at TAR upload time;
         *                         same numeric space as minSeqNo/maxSeqNo
         */
        public GcShardEntry(String indexUUID, int shardId, long minSeqNo, long maxSeqNo, long globalCheckpoint) {
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.globalCheckpoint = globalCheckpoint;
        }

        public String getIndexUUID() {
            return indexUUID;
        }

        public int getShardId() {
            return shardId;
        }

        /** Minimum sequence number of ops in this TAR's translog files for this shard (seqNo space). */
        public long getMinSeqNo() {
            return minSeqNo;
        }

        /** Maximum sequence number of ops in this TAR's translog files for this shard (seqNo space). */
        public long getMaxSeqNo() {
            return maxSeqNo;
        }

        /** Global checkpoint (seqNo) on the data node at the time this TAR was uploaded. */
        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }
    }

    private TarArchiveBuilder() {}

    /**
     * Represents a pre-computed TAR archive layout: total size and per-entry offsets.
     * Computed from file sizes alone — no content reads required.
     */
    public static final class TarLayout {
        /** Total archive size in bytes (including end-of-archive marker). */
        private final long totalSize;
        /** Pre-serialized binary index bytes (the content of the first TAR entry). */
        private final byte[] indexBytes;
        /** Per-entry (path, dataOffset, dataLength) in archive order. */
        private final List<EntryLocation> entries;

        TarLayout(long totalSize, byte[] indexBytes, List<EntryLocation> entries) {
            this.totalSize = totalSize;
            this.indexBytes = indexBytes;
            this.entries = Collections.unmodifiableList(entries);
        }

        public long getTotalSize() {
            return totalSize;
        }

        public byte[] getIndexBytes() {
            return indexBytes;
        }

        public List<EntryLocation> getEntries() {
            return entries;
        }
    }

    /**
     * Byte location of a data entry within the archive.
     */
    public static final class EntryLocation {
        private final String path;
        private final long dataOffset;
        private final long dataLength;

        EntryLocation(String path, long dataOffset, long dataLength) {
            this.path = path;
            this.dataOffset = dataOffset;
            this.dataLength = dataLength;
        }

        public String getPath() {
            return path;
        }

        public long getDataOffset() {
            return dataOffset;
        }

        public long getDataLength() {
            return dataLength;
        }
    }

    /**
     * Computes the TAR layout (total size and all entry offsets) from entry paths and sizes only.
     * No file content is read — this is pure arithmetic.
     *
     * @param entries ordered list of (path, size) for all data entries (NOT including the index entry)
     * @return layout with pre-serialized index and total archive size
     * @throws IOException if path length exceeds TAR 100-byte limit
     */
    public static TarLayout computeLayout(List<ArchiveBuildEntry> entries) throws IOException {
        return computeLayout(entries, Collections.emptyList());
    }

    /**
     * Computes the TAR layout, embedding a GC summary prefix at the head of the {@code _index}.
     *
     * <p>The {@code _index} binary layout with GC prefix:
     * <pre>
     *   GC SUMMARY PREFIX (index-grouped):
     *     [2 bytes: numIndices]
     *     Per index:
     *       [16 bytes: indexUUID as 2 longs (msb+lsb)]
     *       [2 bytes: numShards]
     *       Per shard: [4 shardId][8 minSeqNo][8 maxSeqNo][8 globalCheckpoint] = 24 bytes
     *   FULL ENTRY INDEX:
     *     [4 bytes: numEntries]
     *     Per entry: [2 pathLen][pathLen path][8 dataOffset][8 dataLength]
     * </pre>
     *
     * @param entries    data entries to pack into the TAR
     * @param gcEntries  GC summary entries (one per shard represented in this TAR); may be empty
     * @return computed layout with embedded GC prefix
     */
    public static TarLayout computeLayout(List<ArchiveBuildEntry> entries, List<GcShardEntry> gcEntries) throws IOException {
        // Step 1: collect paths and sizes
        List<String> paths = new ArrayList<>(entries.size());
        List<Long> sizes = new ArrayList<>(entries.size());
        for (ArchiveBuildEntry e : entries) {
            String path = e.getPath();
            // TAR name field is 100 bytes but the last byte must be '\0' (null terminator),
            // so the effective maximum usable path length is 99 bytes.
            if (path.getBytes(StandardCharsets.UTF_8).length > 99) {
                throw new IOException("TAR path too long (max 99 bytes): " + path);
            }
            paths.add(path);
            sizes.add(e.getSize());
        }

        // Step 2: compute GC prefix size (index-grouped format)
        // 2 (numIndices) + per-index: 18 (header) + numShards * 24
        // Group gcEntries by indexUUID
        java.util.LinkedHashMap<String, List<GcShardEntry>> byIndex = new java.util.LinkedHashMap<>();
        for (GcShardEntry gc : gcEntries) {
            byIndex.computeIfAbsent(gc.getIndexUUID(), k -> new ArrayList<>()).add(gc);
        }
        int numIndices = byIndex.size();
        int gcPrefixSize = 2; // numIndices
        for (List<GcShardEntry> shards : byIndex.values()) {
            gcPrefixSize += GC_INDEX_HEADER_BYTES + shards.size() * GC_SUMMARY_BYTES_PER_SHARD;
        }

        int numEntries = paths.size();
        int entryIndexSize = 4; // num_entries header
        for (String path : paths) {
            entryIndexSize += 2 + path.getBytes(StandardCharsets.UTF_8).length + 8 + 8;
        }
        int indexDataSize = gcPrefixSize + entryIndexSize;

        // Step 3: compute size of index TAR entry (header + padded data)
        long indexEntrySize = TAR_BLOCK + pad512(indexDataSize); // 512 header + padded data

        // Step 4: compute per-entry offsets (all data entries come after the index entry)
        long currentOffset = indexEntrySize;
        List<EntryLocation> locations = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            long dataOffset = currentOffset + TAR_BLOCK; // skip the 512-byte header
            long dataLength = sizes.get(i);
            locations.add(new EntryLocation(paths.get(i), dataOffset, dataLength));
            currentOffset = dataOffset + pad512(dataLength);
        }

        // Step 5: total size = end of last entry + 2 * 512 (end-of-archive marker)
        long totalSize = currentOffset + 2L * TAR_BLOCK;

        // Step 6: serialize the binary index (now we know all offsets)
        byte[] indexBytes = serializeIndex(paths, locations, gcEntries);
        assert indexBytes.length == indexDataSize : "index size mismatch: " + indexBytes.length + " vs " + indexDataSize;

        return new TarLayout(totalSize, indexBytes, locations);
    }

    /**
     * Streams the TAR archive to {@code out} in a single sequential pass.
     * File content is read lazily during streaming — peak memory is one file at a time.
     *
     * @param out     target output stream (e.g. pipe to S3 upload)
     * @param layout  pre-computed layout from {@link #computeLayout}
     * @param entries data entries in the same order as used to compute {@code layout}
     */
    public static void build(OutputStream out, TarLayout layout, List<ArchiveBuildEntry> entries) throws IOException {
        // Write index entry
        writeTarHeader(out, INDEX_ENTRY_NAME, layout.getIndexBytes().length);
        out.write(layout.getIndexBytes());
        writePadding(out, layout.getIndexBytes().length);

        // Write each data entry
        List<EntryLocation> locations = layout.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            ArchiveBuildEntry entry = entries.get(i);
            long dataLength = locations.get(i).getDataLength();
            writeTarHeader(out, entry.getPath(), dataLength);
            // Stream content lazily — no full pre-load into memory
            byte[] backing = entry.getBackingBytes();
            if (backing != null) {
                out.write(backing);
            } else {
                try (InputStream in = entry.getContent()) {
                    byte[] buf = new byte[65536];
                    long remaining = dataLength;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        int read = in.read(buf, 0, toRead);
                        if (read <= 0) break;
                        out.write(buf, 0, read);
                        remaining -= read;
                    }
                }
            }
            writePadding(out, dataLength);
        }

        // End-of-archive: two 512-byte zero blocks
        byte[] eoa = new byte[2 * TAR_BLOCK];
        out.write(eoa);
    }

    // ── Index serialization ────────────────────────────────────────────────────

    private static byte[] serializeIndex(List<String> paths, List<EntryLocation> locations, List<GcShardEntry> gcEntries) {
        // Group by indexUUID (preserve insertion order)
        java.util.LinkedHashMap<String, List<GcShardEntry>> byIndex = new java.util.LinkedHashMap<>();
        for (GcShardEntry gc : gcEntries) {
            byIndex.computeIfAbsent(gc.getIndexUUID(), k -> new ArrayList<>()).add(gc);
        }
        int numIndices = byIndex.size();
        int gcPrefixSize = 2; // numIndices
        for (List<GcShardEntry> shards : byIndex.values()) {
            gcPrefixSize += GC_INDEX_HEADER_BYTES + shards.size() * GC_SUMMARY_BYTES_PER_SHARD;
        }

        int numEntries = paths.size();
        int entryIndexSize = 4;
        List<byte[]> pathBytes = new ArrayList<>(numEntries);
        for (String path : paths) {
            byte[] pb = path.getBytes(StandardCharsets.UTF_8);
            pathBytes.add(pb);
            entryIndexSize += 2 + pb.length + 8 + 8;
        }

        ByteBuffer buf = ByteBuffer.allocate(gcPrefixSize + entryIndexSize);

        // Write GC summary prefix (index-grouped)
        buf.putShort((short) numIndices);
        for (java.util.Map.Entry<String, List<GcShardEntry>> indexEntry : byIndex.entrySet()) {
            // Write 16-byte UUID (msb + lsb). For non-standard UUID strings (e.g. "_na_" in tests),
            // fall back to a deterministic 128-bit hash of the string.
            long uuidMsb, uuidLsb;
            try {
                java.util.UUID uuid = java.util.UUID.fromString(indexEntry.getKey());
                uuidMsb = uuid.getMostSignificantBits();
                uuidLsb = uuid.getLeastSignificantBits();
            } catch (IllegalArgumentException e) {
                // Non-standard UUID: use name-based UUID (type 3) as a deterministic fallback
                java.util.UUID uuid = java.util.UUID.nameUUIDFromBytes(indexEntry.getKey().getBytes(StandardCharsets.UTF_8));
                uuidMsb = uuid.getMostSignificantBits();
                uuidLsb = uuid.getLeastSignificantBits();
            }
            buf.putLong(uuidMsb);
            buf.putLong(uuidLsb);
            List<GcShardEntry> shards = indexEntry.getValue();
            buf.putShort((short) shards.size());
            for (GcShardEntry gc : shards) {
                buf.putInt(gc.getShardId());
                buf.putLong(gc.getMinSeqNo());
                buf.putLong(gc.getMaxSeqNo());
                buf.putLong(gc.getGlobalCheckpoint());
            }
        }

        // Write full entry index
        buf.putInt(numEntries);
        for (int i = 0; i < numEntries; i++) {
            byte[] pb = pathBytes.get(i);
            buf.putShort((short) pb.length);
            buf.put(pb);
            buf.putLong(locations.get(i).getDataOffset());
            buf.putLong(locations.get(i).getDataLength());
        }
        return buf.array();
    }

    /**
     * Parses the binary entry index from {@code _index} bytes, skipping the GC summary prefix.
     * The GC prefix uses the index-grouped format; we skip it by reading the structure.
     *
     * @param indexBytes raw index bytes (content of the first TAR entry)
     * @return list of (path, dataOffset, dataLength) in order
     */
    public static List<EntryLocation> parseIndex(byte[] indexBytes) throws IOException {
        if (indexBytes == null || indexBytes.length < 2) {
            return Collections.emptyList();
        }
        ByteBuffer buf = ByteBuffer.wrap(indexBytes);
        // Skip GC summary prefix (index-grouped format):
        // [2 bytes: numIndices]
        // Per index: [16 bytes UUID][2 bytes numShards][numShards * 24 bytes]
        int numIndices = buf.getShort() & 0xFFFF;
        for (int i = 0; i < numIndices; i++) {
            buf.getLong(); // UUID msb
            buf.getLong(); // UUID lsb
            int numShards = buf.getShort() & 0xFFFF;
            buf.position(buf.position() + numShards * GC_SUMMARY_BYTES_PER_SHARD);
        }

        if (buf.remaining() < 4) {
            return Collections.emptyList();
        }
        int numEntries = buf.getInt();
        List<EntryLocation> result = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            short pathLen = buf.getShort();
            byte[] pathBuf = new byte[pathLen];
            buf.get(pathBuf);
            String path = new String(pathBuf, StandardCharsets.UTF_8);
            long dataOffset = buf.getLong();
            long dataLength = buf.getLong();
            result.add(new EntryLocation(path, dataOffset, dataLength));
        }
        return result;
    }

    /**
     * Parses only the GC summary prefix from {@code _index} bytes (index-grouped format).
     * This is a fast path for the GC scanner: returns one {@link GcShardEntry} per shard
     * across all indices, with the {@code indexUUID} populated from the group header.
     *
     * @param indexBytes raw index bytes (GC prefix only, or full _index)
     * @return list of {@link GcShardEntry} from the GC prefix (all indices flattened)
     */
    public static List<GcShardEntry> parseGcSummary(byte[] indexBytes) {
        if (indexBytes == null || indexBytes.length < 2) {
            return Collections.emptyList();
        }
        ByteBuffer buf = ByteBuffer.wrap(indexBytes);
        int numIndices = buf.getShort() & 0xFFFF;
        if (numIndices == 0) {
            return Collections.emptyList();
        }
        List<GcShardEntry> result = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            if (buf.remaining() < GC_INDEX_HEADER_BYTES) {
                return Collections.emptyList(); // truncated
            }
            long uuidMsb = buf.getLong();
            long uuidLsb = buf.getLong();
            String indexUUID = new java.util.UUID(uuidMsb, uuidLsb).toString();
            int numShards = buf.getShort() & 0xFFFF;
            if (buf.remaining() < numShards * GC_SUMMARY_BYTES_PER_SHARD) {
                return Collections.emptyList(); // truncated
            }
            for (int s = 0; s < numShards; s++) {
                int shardId = buf.getInt();
                long minSeqNo = buf.getLong();
                long maxSeqNo = buf.getLong();
                long globalCheckpoint = buf.getLong();
                result.add(new GcShardEntry(indexUUID, shardId, minSeqNo, maxSeqNo, globalCheckpoint));
            }
        }
        return result;
    }

    // ── TAR header writing ─────────────────────────────────────────────────────

    /**
     * Writes a POSIX UStar-compatible TAR header for a regular file.
     * Only the fields required for extraction are populated:
     *   name, size, typeflag, and checksum.
     */
    static void writeTarHeader(OutputStream out, String name, long size) throws IOException {
        byte[] header = new byte[TAR_BLOCK];

        // name (100 bytes, null-terminated)
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int nameLen = Math.min(nameBytes.length, 99);
        System.arraycopy(nameBytes, 0, header, 0, nameLen);
        header[nameLen] = 0; // null-terminate

        // file permissions (mode): 0000644 → octal
        fillOctal(header, 100, 8, 0644L);
        // uid, gid: 0
        fillOctal(header, 108, 8, 0L);
        fillOctal(header, 116, 8, 0L);
        // file size (12 bytes octal)
        fillOctal(header, 124, 12, size);
        // last modification time (12 bytes octal): use current time
        fillOctal(header, 136, 12, System.currentTimeMillis() / 1000L);
        // type flag: '0' = regular file
        header[156] = '0';
        // UStar magic + version
        byte[] magic = "ustar  \0".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(magic, 0, header, 257, Math.min(magic.length, 8));

        // Checksum: sum of all bytes treating checksum field as 8 spaces
        Arrays.fill(header, 148, 156, (byte) ' ');
        int checksum = 0;
        for (byte b : header) {
            checksum += (b & 0xFF);
        }
        fillOctal(header, 148, 8, checksum);
        // TAR spec: last byte of checksum field is space, second-to-last is NUL
        header[154] = 0;
        header[155] = ' ';

        out.write(header);
    }

    /** Pad output to next 512-byte boundary after {@code dataLength} bytes. */
    static void writePadding(OutputStream out, long dataLength) throws IOException {
        int remainder = (int) (dataLength % TAR_BLOCK);
        if (remainder != 0) {
            out.write(new byte[TAR_BLOCK - remainder]);
        }
    }

    /** Round {@code n} up to next multiple of 512. */
    static long pad512(long n) {
        return ((n + TAR_BLOCK - 1) / TAR_BLOCK) * TAR_BLOCK;
    }

    /** Write {@code value} as a null-terminated octal string in {@code buf[off..off+len)}. */
    private static void fillOctal(byte[] buf, int off, int len, long value) {
        String octal = Long.toOctalString(value);
        // Pad left with zeros, leave room for null terminator
        int digits = len - 1;
        if (octal.length() > digits) {
            octal = octal.substring(octal.length() - digits);
        }
        Arrays.fill(buf, off, off + len, (byte) '0');
        byte[] octalBytes = octal.getBytes(StandardCharsets.US_ASCII);
        int start = off + digits - octalBytes.length;
        System.arraycopy(octalBytes, 0, buf, start, octalBytes.length);
        buf[off + len - 1] = 0; // null-terminate
    }

    // ── Entry model ──────────────────────────────────────────────────────────

    /**
     * A single entry to be packed into a TAR archive.
     * Each entry provides a path (name), content stream, and byte length.
     */
    public interface ArchiveBuildEntry {

        /** Entry path inside the archive (e.g. {@code "indexUUID/shardId/1/translog-3.tlog"}). */
        String getPath();

        /** Opens a fresh stream over the entry's content. */
        InputStream getContent() throws IOException;

        /** Byte length of the content. */
        long getSize();

        /**
         * Returns the raw backing bytes for this entry, or {@code null} if not available.
         * When non-null, builders may reuse this array directly to avoid an extra allocation.
         */
        default byte[] getBackingBytes() {
            return null;
        }
    }

    /**
     * Creates an {@link ArchiveBuildEntry} from a path and a pre-loaded byte array.
     *
     * @param path    entry path inside the archive
     * @param content raw bytes
     * @return an immutable entry backed by {@code content}
     */
    public static ArchiveBuildEntry fromBytes(String path, byte[] content) {
        return new ArchiveBuildEntry() {
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public InputStream getContent() {
                return new ByteArrayInputStream(content);
            }

            @Override
            public long getSize() {
                return content.length;
            }

            @Override
            public byte[] getBackingBytes() {
                return content;
            }
        };
    }

    /**
     * Creates a lazy streaming {@link ArchiveBuildEntry} from a {@link java.nio.file.Path}.
     * <p>
     * Unlike {@link #fromBytes}, this variant does <b>not</b> read the file into memory at
     * creation time. The file is opened and streamed lazily when {@link ArchiveBuildEntry#getContent()}
     * is called during TAR construction. This is the preferred factory when building a TAR from
     * local translog files — it keeps peak memory bounded to the TAR pipe buffer size (256 KB)
     * rather than the total size of all files in the batch.
     *
     * @param path    the entry path inside the TAR (e.g. {@code indexUUID/0/1/translog-3.tlog})
     * @param file    the local file to stream
     * @param size    the file size in bytes (must be accurate — used for the TAR header)
     * @return a lazy {@link ArchiveBuildEntry}
     */
    public static ArchiveBuildEntry fromPath(String path, java.nio.file.Path file, long size) {
        return new ArchiveBuildEntry() {
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public InputStream getContent() throws IOException {
                return java.nio.file.Files.newInputStream(file);
            }

            @Override
            public long getSize() {
                return size;
            }

            @Override
            public byte[] getBackingBytes() {
                return null; // not backed by bytes — streaming only
            }
        };
    }
}
