/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parses ZIP end-of-central-directory (EOCD) and central directory from the tail bytes of an archive
 * to resolve path → (data offset, data length) for range download. No full-archive read.
 *
 * @opensearch.internal
 */
public final class ZipCentralDirectoryParser {

    private static final int EOCD_SIG = 0x06054b50;
    private static final int CD_ENTRY_SIG = 0x02014b50;
    private static final int LOCAL_HEADER_SIZE = 30;
    private static final int EOCD_MIN_SIZE = 22;

    /** Max bytes to read from end of archive for EOCD + central directory (65535 + 22). */
    public static final int MAX_ZIP_TAIL_BYTES = 65557;

    private ZipCentralDirectoryParser() {}

    /**
     * Parse the last N bytes of the archive (must include full EOCD and full central directory).
     *
     * @param tail         last bytes of the archive (e.g. from range-read at end of blob)
     * @param tailStartOffset offset of tail[0] in the archive (i.e. archiveSize - tail.length)
     * @return list of entries in order (path → data offset, data length)
     */
    public static List<ArchiveEntry> parse(byte[] tail, long tailStartOffset) throws IOException {
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
     * Read the ZIP EOCD comment from the tail bytes. Comment length is at EOCD+20 (2 bytes).
     *
     * @param tail last bytes of the archive (must contain full EOCD + comment)
     * @return comment string, or "" if comment length is 0
     */
    public static String getComment(byte[] tail) throws IOException {
        if (tail.length < EOCD_MIN_SIZE) {
            throw new IOException("Tail too short for EOCD");
        }
        int eocdPos = findEocd(tail);
        int commentLen = getShort(tail, eocdPos + 20) & 0xffff;
        if (commentLen == 0) {
            return "";
        }
        if (eocdPos + 22 + commentLen > tail.length) {
            throw new IOException("Comment overflows tail");
        }
        return new String(tail, eocdPos + 22, commentLen, StandardCharsets.UTF_8);
    }

    /**
     * Same as {@link #parse(byte[], long)} but returns a map path → entry for lookup.
     */
    public static Map<String, ArchiveEntry> parseToMap(byte[] tail, long tailStartOffset) throws IOException {
        List<ArchiveEntry> list = parse(tail, tailStartOffset);
        Map<String, ArchiveEntry> map = new TreeMap<>();
        for (ArchiveEntry e : list) {
            map.put(e.getPath(), e);
        }
        return map;
    }

    private static int findEocd(byte[] tail) throws IOException {
        for (int i = tail.length - EOCD_MIN_SIZE; i >= 0; i--) {
            if (getInt(tail, i) == EOCD_SIG) {
                return i;
            }
        }
        throw new IOException("EOCD signature not found in tail");
    }

    private static List<ArchiveEntry> parseCentralDirectory(byte[] buf, int cdStart, int cdSize, long cdStartInFile) throws IOException {
        List<ArchiveEntry> entries = new ArrayList<>();
        int pos = cdStart;
        int end = cdStart + cdSize;
        while (pos + 46 <= end) {
            if (getInt(buf, pos) != CD_ENTRY_SIG) {
                break;
            }
            int method = getShort(buf, pos + 10) & 0xffff;
            int compressedSize = getInt(buf, pos + 20);
            int filenameLen = getShort(buf, pos + 28) & 0xffff;
            int extraLen = getShort(buf, pos + 30) & 0xffff;
            int commentLen = getShort(buf, pos + 32) & 0xffff;
            int localHeaderOffset = getInt(buf, pos + 42);
            if (method != 0) {
                throw new IOException("Unsupported compression method " + method + " (expected 0 stored)");
            }
            pos += 46;
            if (pos + filenameLen + extraLen + commentLen > end) {
                throw new IOException("Central directory entry overflows");
            }
            String path = new String(buf, pos, filenameLen, StandardCharsets.UTF_8);
            pos += filenameLen + extraLen + commentLen;
            long dataOffset = localHeaderOffset + LOCAL_HEADER_SIZE + filenameLen + extraLen;
            entries.add(new ArchiveEntry(path, dataOffset, compressedSize));
        }
        return entries;
    }

    private static int getInt(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8) | ((b[off + 2] & 0xff) << 16) | ((b[off + 3] & 0xff) << 24);
    }

    private static int getShort(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8);
    }
}
