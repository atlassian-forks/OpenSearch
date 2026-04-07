/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Builds a ZIP archive with compression method "stored" (no compression).
 * Central directory is written at the end by ZipOutputStream (no separate manifest).
 * Path convention for members: {@code indexUUID/shardId/primaryTerm/filename}.
 *
 * @opensearch.internal
 */
public final class ArchiveBuilder {

    /**
     * Writes a ZIP (stored) archive to {@code out} from the given entries.
     * Each entry's path is used as the ZIP member name; content is read from the stream.
     *
     * @param out     target stream (e.g. blob upload stream)
     * @param entries ordered list of (path, content, size)
     * @throws IOException on read/write or invalid entry
     */
    public static void build(OutputStream out, Iterable<ArchiveBuildEntry> entries) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            for (ArchiveBuildEntry entry : entries) {
                addStoredEntry(zos, entry);
            }
        }
    }

    /**
     * Computes the byte size of the ZIP that would be produced by {@link #buildWithComment} without buffering.
     * Used for streaming upload (build once to a pipe with known size). When using streaming upload, the ZIP
     * is built twice: once here (size pass) and once in {@link #buildWithComment} to the upload stream. This
     * is intentional so the blob store receives an exact content length.
     *
     * @param entries ordered list of (path, content, size)
     * @return total bytes that buildWithComment would write
     */
    public static long computeSizeWithComment(Iterable<ArchiveBuildEntry> entries) throws IOException {
        return computeSizeAndOffsetsWithComment(entries).getSize();
    }

    /**
     * Result holding both the computed ZIP size and the per-entry path offsets.
     */
    public static final class SizeAndOffsets {
        private final long size;
        private final List<ArchiveCommentFormat.PathOffsetLength> offsets;

        SizeAndOffsets(long size, List<ArchiveCommentFormat.PathOffsetLength> offsets) {
            this.size = size;
            this.offsets = offsets;
        }

        public long getSize() {
            return size;
        }

        public List<ArchiveCommentFormat.PathOffsetLength> getOffsets() {
            return offsets;
        }
    }

    /**
     * Computes the byte size and per-entry offsets of the ZIP that would be produced by {@link #buildWithComment}.
     * Used for streaming upload with known size and for populating archive recovery metadata.
     */
    public static SizeAndOffsets computeSizeAndOffsetsWithComment(Iterable<ArchiveBuildEntry> entries) throws IOException {
        CountingOutputStream counter = new CountingOutputStream(new NullOutputStream());
        List<ArchiveCommentFormat.PathOffsetLength> offsets = buildWithCommentAndOffsets(counter, entries);
        return new SizeAndOffsets(counter.getCount(), offsets);
    }

    /**
     * Builds a ZIP (stored) with an index in the EOCD comment. Records (path, offset, length) per entry,
     * builds index from paths (indexUUID/shardId/primaryTerm/translog-N.tlog|.ckp), and sets comment before close.
     *
     * @param out     target stream (e.g. blob upload stream)
     * @param entries ordered list of (path, content, size)
     * @throws IOException on read/write or invalid entry
     */
    public static void buildWithComment(OutputStream out, Iterable<ArchiveBuildEntry> entries) throws IOException {
        buildWithCommentAndOffsets(out, entries);
    }

    /**
     * Builds a ZIP with EOCD comment and returns the per-entry path offsets for archive-based recovery.
     * Callers can use the returned offsets to populate metadata with exact byte ranges for range-reads.
     *
     * @return list of (path, offset, length) for each entry in the archive
     */
    public static List<ArchiveCommentFormat.PathOffsetLength> buildWithCommentAndOffsets(
        OutputStream out,
        Iterable<ArchiveBuildEntry> entries
    ) throws IOException {
        CountingOutputStream countingOut = new CountingOutputStream(out);
        List<ArchiveCommentFormat.PathOffsetLength> pathOffsets = new ArrayList<>();
        try (ZipOutputStream zos = new ZipOutputStream(countingOut)) {
            for (ArchiveBuildEntry entry : entries) {
                addStoredEntryWithOffset(zos, entry, countingOut, pathOffsets);
            }
            List<ArchiveIndexEntry> indexEntries = ArchiveCommentFormat.fromPathOffsetList(pathOffsets);
            String indexUUID = pathOffsets.isEmpty() ? null : extractIndexUUID(pathOffsets.get(0).getPath());
            String comment = ArchiveCommentFormat.serializeWithIndex(indexUUID, indexEntries);
            zos.setComment(comment);
        }
        return pathOffsets;
    }

    private static String extractIndexUUID(String memberPath) {
        if (memberPath == null) {
            return null;
        }
        int i = memberPath.indexOf('/');
        return i > 0 ? memberPath.substring(0, i) : null;
    }

    private static void addStoredEntryWithOffset(
        ZipOutputStream zos,
        ArchiveBuildEntry entry,
        CountingOutputStream countingOut,
        List<ArchiveCommentFormat.PathOffsetLength> pathOffsets
    ) throws IOException {
        String path = entry.getPath();
        long size = entry.getSize();
        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Invalid size for entry " + path + ": " + size);
        }
        int len = (int) size;
        // Re-use the backing bytes directly if available to avoid a second allocation.
        byte[] buf = entry.getBackingBytes();
        if (buf == null) {
            buf = new byte[len];
            try (InputStream in = entry.getContent()) {
                int total = 0;
                while (total < len) {
                    int r = in.read(buf, total, len - total);
                    if (r <= 0) break;
                    total += r;
                }
                if (total != len) {
                    throw new IOException("Entry " + path + ": expected " + len + " bytes, got " + total);
                }
            }
        }
        CRC32 crc = new CRC32();
        crc.update(buf, 0, len);
        ZipEntry ze = new ZipEntry(path);
        ze.setMethod(ZipEntry.STORED);
        ze.setSize(len);
        ze.setCompressedSize(len);
        ze.setCrc(crc.getValue());
        zos.putNextEntry(ze);
        long dataOffset = countingOut.getCount();
        zos.write(buf, 0, len);
        long dataLength = countingOut.getCount() - dataOffset;
        pathOffsets.add(new ArchiveCommentFormat.PathOffsetLength(path, dataOffset, dataLength));
        zos.closeEntry();
    }

    private static void addStoredEntry(ZipOutputStream zos, ArchiveBuildEntry entry) throws IOException {
        String path = entry.getPath();
        long size = entry.getSize();
        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Invalid size for entry " + path + ": " + size);
        }
        int len = (int) size;
        // Re-use the backing bytes directly if available to avoid a second allocation.
        byte[] buf = entry.getBackingBytes();
        if (buf == null) {
            buf = new byte[len];
            try (InputStream in = entry.getContent()) {
                int total = 0;
                while (total < len) {
                    int r = in.read(buf, total, len - total);
                    if (r <= 0) break;
                    total += r;
                }
                if (total != len) {
                    throw new IOException("Entry " + path + ": expected " + len + " bytes, got " + total);
                }
            }
        }
        CRC32 crc = new CRC32();
        crc.update(buf, 0, len);
        ZipEntry ze = new ZipEntry(path);
        ze.setMethod(ZipEntry.STORED);
        ze.setSize(len);
        ze.setCompressedSize(len);
        ze.setCrc(crc.getValue());
        zos.putNextEntry(ze);
        zos.write(buf, 0, len);
        zos.closeEntry();
    }

    /**
     * One entry to add to the archive: path, content stream, and size.
     *
     * @opensearch.internal
     */
    @ExperimentalApi
    public interface ArchiveBuildEntry {

        String getPath();

        InputStream getContent() throws IOException;

        long getSize();

        /**
         * Returns the raw backing bytes for this entry, or null if not available.
         * When non-null, ArchiveBuilder will re-use this array directly instead of
         * allocating a second buffer — halving per-entry heap usage during ZIP assembly.
         */
        default byte[] getBackingBytes() {
            return null;
        }
    }

    /**
     * Build entry from a path and byte array.
     */
    public static ArchiveBuildEntry fromBytes(String path, byte[] content) {
        return new ArchiveBuildEntry() {
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public InputStream getContent() {
                return new BytesArray(content).streamInput();
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
}
