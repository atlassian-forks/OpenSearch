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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parses ZIP end-of-central-directory (EOCD) and central directory from the tail bytes of a segment archive
 * to resolve path → (data offset, data length) for range download. No full-archive read needed.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public final class ZipSegmentParser {

    private static final int EOCD_SIG = 0x06054b50;
    private static final int CD_ENTRY_SIG = 0x02014b50;
    private static final int LOCAL_HEADER_SIZE = 30;
    private static final int EOCD_MIN_SIZE = 22;

    /** Max bytes to read from end of archive for EOCD + central directory (65535 + 22). */
    public static final int MAX_ZIP_TAIL_BYTES = 65557;

    private ZipSegmentParser() {}

    /**
     * Parse the last N bytes of the archive (must include full EOCD and central directory).
     *
     * @param tail         last bytes of the archive (e.g. from range-read at end of blob)
     * @param tailStartOffset offset of tail[0] in the archive (i.e. archiveSize - tail.length)
     * @return list of segment archive entries in order (path → data offset, data length)
     * @throws IOException if EOCD not found or central directory corrupted
     */
    public static List<SegmentArchiveEntry> parse(byte[] tail, long tailStartOffset) throws IOException {
        if (tail.length < EOCD_MIN_SIZE) {
            throw new IOException("Tail too short for EOCD (need at least " + EOCD_MIN_SIZE + " bytes)");
        }

        int eocdPos = findEocd(tail);
        int cdSize = getInt(tail, eocdPos + 12);
        int cdOffsetInFile = getInt(tail, eocdPos + 16);
        int cdOffsetInTail = (int) (cdOffsetInFile - tailStartOffset);

        if (cdOffsetInTail < 0 || cdOffsetInTail + cdSize > tail.length) {
            throw new IOException(
                "Central directory not fully in tail: cdOffset="
                    + cdOffsetInFile
                    + ", cdSize="
                    + cdSize
                    + ", tailStart="
                    + tailStartOffset
                    + ", tailLen="
                    + tail.length
            );
        }

        return parseCentralDirectory(tail, cdOffsetInTail, cdSize, tailStartOffset + cdOffsetInTail);
    }

    /**
     * Same as {@link #parse(byte[], long)} but returns a map path → entry for lookup.
     *
     * @param tail         last bytes of the archive
     * @param tailStartOffset offset of tail[0] in the archive
     * @return map of filename → SegmentArchiveEntry for range-download recovery
     * @throws IOException if EOCD not found or central directory corrupted
     */
    public static Map<String, SegmentArchiveEntry> parseToMap(byte[] tail, long tailStartOffset) throws IOException {
        List<SegmentArchiveEntry> list = parse(tail, tailStartOffset);
        Map<String, SegmentArchiveEntry> map = new TreeMap<>();
        for (SegmentArchiveEntry e : list) {
            map.put(e.getFilename(), e);
        }
        return map;
    }

    /**
     * Find the EOCD (End of Central Directory) signature in the tail bytes.
     * EOCD is always at or before the last 65557 bytes of a ZIP file.
     */
    private static int findEocd(byte[] tail) throws IOException {
        for (int i = tail.length - EOCD_MIN_SIZE; i >= 0; i--) {
            if (getInt(tail, i) == EOCD_SIG) {
                return i;
            }
        }
        throw new IOException("EOCD signature not found in tail");
    }

    /**
     * Parse central directory entries from the tail buffer.
     * Each entry provides: path, offset (in archive), length.
     */
    private static List<SegmentArchiveEntry> parseCentralDirectory(byte[] buf, int cdStart, int cdSize, long cdStartInFile)
        throws IOException {
        List<SegmentArchiveEntry> entries = new ArrayList<>();
        int pos = cdStart;
        int end = cdStart + cdSize;

        while (pos + 46 <= end) {
            if (getInt(buf, pos) != CD_ENTRY_SIG) {
                break;
            }

            // Read central directory entry header
            int method = getShort(buf, pos + 10) & 0xffff;
            int compressedSize = getInt(buf, pos + 20);
            int filenameLen = getShort(buf, pos + 28) & 0xffff;
            int extraLen = getShort(buf, pos + 30) & 0xffff;
            int commentLen = getShort(buf, pos + 32) & 0xffff;
            int localHeaderOffset = getInt(buf, pos + 42);

            // Verify compression method is "stored" (0)
            if (method != 0) {
                throw new IOException("Unsupported compression method " + method + " (expected 0 stored)");
            }

            pos += 46;

            // Check bounds
            if (pos + filenameLen + extraLen + commentLen > end) {
                throw new IOException("Central directory entry overflows");
            }

            // Extract filename
            String path = new String(buf, pos, filenameLen, StandardCharsets.UTF_8);
            pos += filenameLen + extraLen + commentLen;

            // Calculate data offset: local file header offset + local header size + filename length + extra field
            long dataOffset = localHeaderOffset + LOCAL_HEADER_SIZE + filenameLen + extraLen;

            // Create archive entry with offset, length, and checksum (0 for now - can be added later)
            entries.add(new SegmentArchiveEntry(path, dataOffset, compressedSize, 0L));
        }

        return entries;
    }

    /**
     * Read a 4-byte little-endian integer from buffer at offset.
     */
    private static int getInt(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8) | ((b[off + 2] & 0xff) << 16) | ((b[off + 3] & 0xff) << 24);
    }

    /**
     * Read a 2-byte little-endian short from buffer at offset.
     */
    private static int getShort(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8);
    }
}
