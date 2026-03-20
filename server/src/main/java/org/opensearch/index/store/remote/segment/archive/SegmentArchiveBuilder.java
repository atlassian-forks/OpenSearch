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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Builds a ZIP archive with compression method "stored" (no compression) for segment files.
 * Central directory is written at the end by ZipOutputStream (no separate manifest).
 * Provides offset/length information for range-read recovery.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public final class SegmentArchiveBuilder {

    /**
     * Writes a ZIP (stored) archive to {@code out} from the given entries.
     * Each entry's path is used as the ZIP member name; content is read from the stream.
     *
     * @param out     target stream (e.g. blob upload stream)
     * @param entries ordered list of (path, content, size)
     * @throws IOException on read/write or invalid entry
     */
    public static void build(OutputStream out, Iterable<SegmentArchiveBuildEntry> entries) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            for (SegmentArchiveBuildEntry entry : entries) {
                addStoredEntry(zos, entry);
            }
        }
    }

    /**
     * Builds a ZIP (stored) archive and returns a map of offsets for each entry.
     * Used to populate {@link SegmentArchiveEntry} for metadata storage.
     *
     * @param out     target stream (e.g. blob upload stream)
     * @param entries ordered list of (path, content, size)
     * @return map of filename → SegmentArchiveEntry with offset/length/checksum
     * @throws IOException on read/write or invalid entry
     */
    public static Map<String, SegmentArchiveEntry> buildAndExtractOffsets(OutputStream out, Iterable<SegmentArchiveBuildEntry> entries)
        throws IOException {
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        CountingOutputStream countingOut = new CountingOutputStream(out);

        try (ZipOutputStream zos = new ZipOutputStream(countingOut)) {
            for (SegmentArchiveBuildEntry entry : entries) {
                addStoredEntryWithOffset(zos, entry, countingOut, archiveEntries);
            }
        }

        return archiveEntries;
    }

    /**
     * Adds a single entry to the ZIP with stored (no compression) format.
     * Records the offset and length for later range-read recovery.
     */
    private static void addStoredEntryWithOffset(
        ZipOutputStream zos,
        SegmentArchiveBuildEntry entry,
        CountingOutputStream countingOut,
        Map<String, SegmentArchiveEntry> offsets
    ) throws IOException {
        String path = entry.getPath();
        long size = entry.getSize();

        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Invalid size for entry " + path + ": " + size);
        }

        int len = (int) size;
        byte[] buf = new byte[len];

        // Read entry content
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

        // Calculate CRC32
        CRC32 crc = new CRC32();
        crc.update(buf, 0, len);

        // Create ZIP entry with stored format
        ZipEntry ze = new ZipEntry(path);
        ze.setMethod(ZipEntry.STORED);
        ze.setSize(len);
        ze.setCompressedSize(len);
        ze.setCrc(crc.getValue());

        zos.putNextEntry(ze);
        long dataOffset = countingOut.getCount();

        // Write data
        zos.write(buf, 0, len);
        long dataLength = countingOut.getCount() - dataOffset;

        zos.closeEntry();

        // Record offset and length for recovery
        offsets.put(path, new SegmentArchiveEntry(path, dataOffset, dataLength, crc.getValue()));
    }

    /**
     * Adds a single entry to the ZIP with stored format (simple version without offset tracking).
     */
    private static void addStoredEntry(ZipOutputStream zos, SegmentArchiveBuildEntry entry) throws IOException {
        String path = entry.getPath();
        long size = entry.getSize();

        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Invalid size for entry " + path + ": " + size);
        }

        int len = (int) size;
        byte[] buf = new byte[len];

        // Read entry content
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

        // Calculate CRC32
        CRC32 crc = new CRC32();
        crc.update(buf, 0, len);

        // Create ZIP entry with stored format
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
     * Creates a build entry from a byte array.
     */
    public static SegmentArchiveBuildEntry fromBytes(String path, byte[] content) {
        return new SegmentArchiveBuildEntry() {
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
        };
    }

    /**
     * Interface for entries to be added to the segment archive.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.14.0")
    public interface SegmentArchiveBuildEntry {
        /**
         * Path within the archive (e.g., "_0.si", "_0.cfs", "_0.cfe")
         */
        String getPath();

        /**
         * Content stream. Caller is responsible for closing.
         */
        InputStream getContent();

        /**
         * Size of the content in bytes.
         */
        long getSize();
    }
}
