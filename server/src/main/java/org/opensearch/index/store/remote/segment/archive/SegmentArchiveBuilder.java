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
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a TAR archive for segment files.
 * <p>
 * The archive layout follows {@link TarArchiveBuilder}:
 * <ul>
 *   <li>First entry: {@code _index} — binary offset table (all offsets pre-computed from file sizes).</li>
 *   <li>Remaining entries: one TAR entry per segment file.</li>
 * </ul>
 * <p>
 * Key advantage over ZIP: all byte offsets are known before any file content is read,
 * enabling true single-pass streaming to S3 with a known {@code Content-Length}.
 * No CRC pre-pass required.
 * <p>
 * Recovery reads the {@code _index} from the HEAD of the blob (fixed offset 512),
 * then issues a single range-GET per segment file — no need to know the total blob size.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public final class SegmentArchiveBuilder {

    private SegmentArchiveBuilder() {}

    /**
     * Computes the TAR layout (all offsets, total size) from entry metadata only.
     * No file content is read — pure arithmetic.
     *
     * @param entries ordered list of segment entries
     * @return pre-computed layout with total archive size and serialised index bytes
     * @throws IOException if any path exceeds the TAR 100-byte name limit
     */
    public static TarArchiveBuilder.TarLayout computeLayout(List<SegmentArchiveBuildEntry> entries) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> tarEntries = toTarEntries(entries);
        return TarArchiveBuilder.computeLayout(tarEntries, Collections.emptyList());
    }

    /**
     * Streams a TAR archive to {@code out} and returns a map of per-file
     * {@link SegmentArchiveEntry} (offset + length) for metadata storage.
     * <p>
     * Callers must pre-compute the layout via {@link #computeLayout} and pass the
     * same {@code entries} list (same order). The layout already contains the index bytes
     * and all offsets.
     *
     * @param out     target stream (e.g. blob upload stream via pipe)
     * @param layout  pre-computed TAR layout
     * @param entries ordered list of segment entries (same order as used to compute layout)
     * @return map of filename → SegmentArchiveEntry with offset/length for range-read recovery
     * @throws IOException on I/O failure
     */
    public static Map<String, SegmentArchiveEntry> buildAndExtractOffsets(
        OutputStream out,
        TarArchiveBuilder.TarLayout layout,
        List<SegmentArchiveBuildEntry> entries
    ) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> tarEntries = toTarEntries(entries);
        TarArchiveBuilder.build(out, layout, tarEntries);

        // Map each entry's path → SegmentArchiveEntry using the pre-computed locations
        Map<String, SegmentArchiveEntry> result = new HashMap<>(layout.getEntries().size());
        for (TarArchiveBuilder.EntryLocation loc : layout.getEntries()) {
            result.put(loc.getPath(), new SegmentArchiveEntry(loc.getPath(), loc.getDataOffset(), loc.getDataLength(), 0L));
        }
        return result;
    }

    /**
     * Creates a build entry backed by a file in the given {@link Directory}.
     * The entry's content stream is opened lazily on each call to {@link SegmentArchiveBuildEntry#getContent()}.
     *
     * @param path      segment filename (e.g. {@code "_0.si"})
     * @param directory store directory containing the file
     * @return a file-backed build entry
     * @throws IOException if the file cannot be stat'd for its length
     */
    public static SegmentArchiveBuildEntry fromDirectory(String path, Directory directory) throws IOException {
        final long size;
        try (IndexInput probe = directory.openInput(path, IOContext.READONCE)) {
            size = probe.length();
        }

        return new SegmentArchiveBuildEntry() {
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public InputStream getContent() throws IOException {
                final IndexInput input = directory.openInput(path, IOContext.READONCE);
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
        };
    }

    /**
     * Creates a build entry from a byte array.
     *
     * @param path    entry path inside the archive
     * @param content raw bytes
     * @return an immutable entry backed by {@code content}
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

    // ── Internal helpers ─────────────────────────────────────────────────────

    /** Adapts {@link SegmentArchiveBuildEntry} list to {@link TarArchiveBuilder.ArchiveBuildEntry} list. */
    private static List<TarArchiveBuilder.ArchiveBuildEntry> toTarEntries(List<SegmentArchiveBuildEntry> entries) {
        List<TarArchiveBuilder.ArchiveBuildEntry> out = new ArrayList<>(entries.size());
        for (SegmentArchiveBuildEntry e : entries) {
            out.add(new TarArchiveBuilder.ArchiveBuildEntry() {
                @Override
                public String getPath() { return e.getPath(); }

                @Override
                public InputStream getContent() throws IOException { return e.getContent(); }

                @Override
                public long getSize() { return e.getSize(); }

                @Override
                public byte[] getBackingBytes() { return e.getBackingBytes(); }
            });
        }
        return out;
    }

    // ── Public entry interface ────────────────────────────────────────────────

    /**
     * Interface for entries to be added to the segment archive.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.14.0")
    public interface SegmentArchiveBuildEntry {
        /**
         * Path within the archive (e.g., {@code "_0.si"}, {@code "_0.cfs"}).
         */
        String getPath();

        /**
         * Content stream. For file-backed entries, each call must return a fresh stream from the start.
         */
        InputStream getContent() throws IOException;

        /**
         * Size of the content in bytes.
         */
        long getSize();

        /**
         * Returns the raw backing bytes for this entry, or {@code null} if not available.
         * When non-null, TAR builder can write directly from the array.
         */
        default byte[] getBackingBytes() {
            return null;
        }
    }
}
