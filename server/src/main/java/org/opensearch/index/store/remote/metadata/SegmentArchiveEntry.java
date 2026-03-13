/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata for a single segment file within a segment archive (ZIP).
 * Contains offset and length information for range-read recovery.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class SegmentArchiveEntry {
    private final String filename;
    private final long offset;
    private final long length;
    private final long checksum;

    public SegmentArchiveEntry(String filename, long offset, long length, long checksum) {
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.checksum = checksum;
    }

    public String getFilename() {
        return filename;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long getChecksum() {
        return checksum;
    }

    /**
     * Write entry to IndexOutput
     */
    public void write(IndexOutput out) throws IOException {
        out.writeString(filename);
        out.writeLong(offset);
        out.writeLong(length);
        out.writeLong(checksum);
    }

    /**
     * Read entry from IndexInput
     */
    public static SegmentArchiveEntry read(IndexInput in) throws IOException {
        String filename = in.readString();
        long offset = in.readLong();
        long length = in.readLong();
        long checksum = in.readLong();
        return new SegmentArchiveEntry(filename, offset, length, checksum);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentArchiveEntry that = (SegmentArchiveEntry) o;
        return offset == that.offset
            && length == that.length
            && checksum == that.checksum
            && Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, offset, length, checksum);
    }

    @Override
    public String toString() {
        return "SegmentArchiveEntry{"
            + "filename='"
            + filename
            + '\''
            + ", offset="
            + offset
            + ", length="
            + length
            + ", checksum="
            + checksum
            + '}';
    }
}
