/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parses the binary {@code _index} entry from the HEAD of a segment TAR archive to resolve
 * path → (data offset, data length) for range-read recovery.
 * <p>
 * Unlike ZIP (which stores the central directory at the tail and requires knowing the total
 * blob size), the TAR {@code _index} is always at a fixed position:
 * <ul>
 *   <li>Bytes [0, 512): first TAR header (for the {@code _index} entry).</li>
 *   <li>Bytes [512, 512 + indexDataLength): the binary index payload.</li>
 * </ul>
 * Recovery reads {@code [0, 512 + padded(indexDataLength))} — no LIST operation needed.
 * <p>
 * The {@code _index} binary format is defined by {@link TarArchiveBuilder}:
 * <pre>
 *   GC SUMMARY PREFIX (empty for segments — no GC entries):
 *     [2 bytes: numIndices = 0]
 *   FULL ENTRY INDEX:
 *     [4 bytes: numEntries (big-endian int)]
 *     Per entry:
 *       [2 bytes: path_len (big-endian short)]
 *       [path_len bytes: UTF-8 path]
 *       [8 bytes: data_offset (big-endian long)]
 *       [8 bytes: data_length (big-endian long)]
 * </pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public final class TarSegmentParser {

    /** Bytes to read from the archive head to cover the TAR header + index payload. */
    public static final int TAR_HEADER_SIZE = TarArchiveBuilder.TAR_BLOCK;

    private TarSegmentParser() {}

    /**
     * Parses the {@code _index} payload from the HEAD of a segment TAR archive.
     *
     * @param head bytes read from the start of the archive blob; must include the full
     *             512-byte TAR header plus at least {@code indexDataLength} bytes of payload
     * @return map of filename → SegmentArchiveEntry (offset, length) for range-read recovery
     * @throws IOException if the header is missing, truncated, or the index is corrupted
     */
    public static Map<String, SegmentArchiveEntry> parseToMap(byte[] head) throws IOException {
        if (head == null || head.length < TAR_HEADER_SIZE) {
            throw new IOException("TAR head buffer too short (need at least " + TAR_HEADER_SIZE + " bytes, got "
                + (head == null ? 0 : head.length) + ")");
        }

        // Read the index data length from the TAR header (octal ASCII in bytes 124..135)
        long indexDataLength = parseTarSize(head, 0);
        if (indexDataLength < 0) {
            throw new IOException("Invalid TAR index entry size: " + indexDataLength);
        }

        int indexStart = TAR_HEADER_SIZE;
        int indexEnd = (int) (indexStart + indexDataLength);

        if (head.length < indexEnd) {
            throw new IOException(
                "TAR head buffer does not contain full _index payload: need " + indexEnd + " bytes, got " + head.length
            );
        }

        byte[] indexBytes = new byte[(int) indexDataLength];
        System.arraycopy(head, indexStart, indexBytes, 0, (int) indexDataLength);

        // Delegate parsing to TarArchiveBuilder (handles GC prefix skip + entry index)
        List<TarArchiveBuilder.EntryLocation> locations = TarArchiveBuilder.parseIndex(indexBytes);

        Map<String, SegmentArchiveEntry> result = new TreeMap<>();
        for (TarArchiveBuilder.EntryLocation loc : locations) {
            result.put(loc.getPath(), new SegmentArchiveEntry(loc.getPath(), loc.getDataOffset(), loc.getDataLength(), 0L));
        }
        return result;
    }

    /**
     * Computes how many bytes must be read from the start of the archive to cover
     * the TAR header plus the full padded {@code _index} payload.
     * <p>
     * This is the number of bytes callers should request via a range-GET starting at offset 0.
     *
     * @param head the first {@value #TAR_HEADER_SIZE} bytes of the archive (just the header)
     * @return total bytes to read: 512 + padded(indexDataLength)
     * @throws IOException if the header is too short or the size field is invalid
     */
    public static int computeHeadReadLength(byte[] head) throws IOException {
        if (head == null || head.length < TAR_HEADER_SIZE) {
            throw new IOException("TAR header buffer too short");
        }
        long indexDataLength = parseTarSize(head, 0);
        if (indexDataLength < 0) {
            throw new IOException("Invalid TAR index entry size: " + indexDataLength);
        }
        long padded = ((indexDataLength + TarArchiveBuilder.TAR_BLOCK - 1) / TarArchiveBuilder.TAR_BLOCK) * TarArchiveBuilder.TAR_BLOCK;
        return (int) (TAR_HEADER_SIZE + padded);
    }

    /**
     * Reads the file size field from a TAR header at the given offset.
     * The size is stored as a null-terminated octal ASCII string in bytes [124, 136).
     *
     * @param buf    buffer containing the TAR header
     * @param offset byte offset of the start of the 512-byte TAR header within {@code buf}
     * @return file size in bytes
     * @throws IOException if the size field cannot be parsed
     */
    static long parseTarSize(byte[] buf, int offset) throws IOException {
        // TAR size field: bytes 124..135 (12 bytes), null-terminated octal ASCII
        int sizeOffset = offset + 124;
        if (buf.length < sizeOffset + 12) {
            throw new IOException("Buffer too short to read TAR size field");
        }
        StringBuilder sb = new StringBuilder(12);
        for (int i = sizeOffset; i < sizeOffset + 12; i++) {
            byte b = buf[i];
            if (b == 0 || b == ' ') break;
            sb.append((char) b);
        }
        try {
            return Long.parseLong(sb.toString().trim(), 8);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid TAR size field: '" + sb + "'", e);
        }
    }
}
