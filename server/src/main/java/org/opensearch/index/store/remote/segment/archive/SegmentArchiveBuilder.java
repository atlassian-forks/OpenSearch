/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
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

// CountingOutputStream tracks bytes written so we can record per-entry offsets.
// Defined as a package-private static inner class below.

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

    /** Small buffer size for 2-pass CRC streaming (file-backed entries). */
    private static final int STREAM_BUF_SIZE = 8 * 1024;

    /**
     * Adds a single entry to the ZIP with stored (no compression) format.
     * Records the offset and length for later range-read recovery.
     * <p>
     * Strategy:
     * <ul>
     *   <li>If {@link SegmentArchiveBuildEntry#getBackingBytes()} is non-null (byte[]-backed entry):
     *       re-use the existing array directly — no second allocation.</li>
     *   <li>Otherwise (file/stream-backed entry): 2-pass streaming —
     *       pass 1 reads with an 8KB buffer to compute CRC32,
     *       pass 2 streams content directly into ZipOutputStream.
     *       Peak per-entry heap: 8KB (stream buffer) only.</li>
     * </ul>
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

        byte[] backingBytes = entry.getBackingBytes();
        final CRC32 crc = new CRC32();

        if (backingBytes != null) {
            // Byte[]-backed: compute CRC on existing array, no second allocation.
            crc.update(backingBytes, 0, len);
            ZipEntry ze = storedEntry(path, len, crc.getValue());
            zos.putNextEntry(ze);
            long dataOffset = countingOut.getCount();
            zos.write(backingBytes, 0, len);
            long dataLength = countingOut.getCount() - dataOffset;
            zos.closeEntry();
            offsets.put(path, new SegmentArchiveEntry(path, dataOffset, dataLength, crc.getValue()));
        } else {
            // File/stream-backed: 2-pass streaming. Pass 1: CRC only (8KB buffer).
            byte[] buf = new byte[STREAM_BUF_SIZE];
            try (InputStream in = entry.getContent()) {
                int r;
                while ((r = in.read(buf)) != -1) {
                    crc.update(buf, 0, r);
                }
            }
            // Pass 2: write to ZIP stream.
            ZipEntry ze = storedEntry(path, len, crc.getValue());
            zos.putNextEntry(ze);
            long dataOffset = countingOut.getCount();
            try (InputStream in = entry.getContent()) {
                int r;
                while ((r = in.read(buf)) != -1) {
                    zos.write(buf, 0, r);
                }
            }
            long dataLength = countingOut.getCount() - dataOffset;
            zos.closeEntry();
            offsets.put(path, new SegmentArchiveEntry(path, dataOffset, dataLength, crc.getValue()));
        }
    }

    /**
     * Adds a single entry to the ZIP with stored format (simple version without offset tracking).
     * Uses the same byte[]-backed / 2-pass-streaming strategy as {@link #addStoredEntryWithOffset}.
     */
    private static void addStoredEntry(ZipOutputStream zos, SegmentArchiveBuildEntry entry) throws IOException {
        String path = entry.getPath();
        long size = entry.getSize();

        if (size < 0 || size > Integer.MAX_VALUE) {
            throw new IOException("Invalid size for entry " + path + ": " + size);
        }
        int len = (int) size;

        byte[] backingBytes = entry.getBackingBytes();
        final CRC32 crc = new CRC32();

        if (backingBytes != null) {
            crc.update(backingBytes, 0, len);
            ZipEntry ze = storedEntry(path, len, crc.getValue());
            zos.putNextEntry(ze);
            zos.write(backingBytes, 0, len);
            zos.closeEntry();
        } else {
            byte[] buf = new byte[STREAM_BUF_SIZE];
            // Pass 1: CRC
            try (InputStream in = entry.getContent()) {
                int r;
                while ((r = in.read(buf)) != -1) {
                    crc.update(buf, 0, r);
                }
            }
            // Pass 2: write
            ZipEntry ze = storedEntry(path, len, crc.getValue());
            zos.putNextEntry(ze);
            try (InputStream in = entry.getContent()) {
                int r;
                while ((r = in.read(buf)) != -1) {
                    zos.write(buf, 0, r);
                }
            }
            zos.closeEntry();
        }
    }

    /** Helper to build a STORED ZipEntry with all required fields set. */
    private static ZipEntry storedEntry(String path, long size, long crcValue) {
        ZipEntry ze = new ZipEntry(path);
        ze.setMethod(ZipEntry.STORED);
        ze.setSize(size);
        ze.setCompressedSize(size);
        ze.setCrc(crcValue);
        return ze;
    }

    /**
     * Computes the exact byte size of the ZIP that would be produced by
     * {@link #buildAndExtractOffsets} for the given entries, without buffering.
     * Used to provide the exact content-length for streaming blob upload.
     * For file-backed entries, this performs the 2-pass CRC read (pass 1 only).
     */
    public static long computeSize(Iterable<SegmentArchiveBuildEntry> entries) throws IOException {
        CountingOutputStream counter = new CountingOutputStream(NullOutputStream.INSTANCE);
        buildAndExtractOffsets(counter, entries);
        return counter.getCount();
    }

    /**
     * Creates a file-backed build entry from a Lucene {@link Directory}.
     * Each call to {@code getContent()} opens a fresh {@link IndexInput} (required for 2-pass streaming).
     * {@code getBackingBytes()} returns null, triggering the 2-pass streaming path in the ZIP builder.
     */
    public static SegmentArchiveBuildEntry fromDirectory(String path, Directory directory) throws IOException {
        long size = directory.fileLength(path);
        return new SegmentArchiveBuildEntry() {
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public InputStream getContent() throws IOException {
                IndexInput input = directory.openInput(path, IOContext.DEFAULT);
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        try {
                            return input.readByte() & 0xFF;
                        } catch (java.io.EOFException e) {
                            return -1;
                        }
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        long remaining = input.length() - input.getFilePointer();
                        if (remaining <= 0) return -1;
                        int toRead = (int) Math.min(len, remaining);
                        input.readBytes(b, off, toRead);
                        return toRead;
                    }

                    @Override
                    public void close() throws IOException {
                        input.close();
                    }
                };
            }

            @Override
            public long getSize() {
                return size;
            }

            // null → triggers 2-pass streaming in addStoredEntry*
            @Override
            public byte[] getBackingBytes() {
                return null;
            }
        };
    }

    /**
     * Creates a build entry from a byte array.
     * Returns the backing array via {@link SegmentArchiveBuildEntry#getBackingBytes()}
     * so the ZIP builder can re-use it directly without a second allocation.
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

            @Override
            public byte[] getBackingBytes() {
                return content;
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
         * For file-backed entries, each call must return a fresh stream from the start.
         */
        InputStream getContent() throws IOException;

        /**
         * Size of the content in bytes.
         */
        long getSize();

        /**
         * Returns the raw backing bytes for this entry, or null if not available.
         * When non-null, the ZIP builder re-uses this array directly for CRC and write,
         * avoiding a second allocation. File-backed entries return null and use 2-pass
         * streaming instead.
         */
        default byte[] getBackingBytes() {
            return null;
        }
    }
}
