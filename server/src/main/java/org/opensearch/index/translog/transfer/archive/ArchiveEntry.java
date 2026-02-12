/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Immutable entry describing one file inside an archive (path and byte range for range-read).
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class ArchiveEntry {

    private final String path;
    private final long dataOffset;
    private final long dataLength;

    public ArchiveEntry(String path, long dataOffset, long dataLength) {
        this.path = Objects.requireNonNull(path);
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
    }

    public String getPath() {
        return path;
    }

    /** Start offset of file data in the archive blob (after local file header). */
    public long getDataOffset() {
        return dataOffset;
    }

    /** Length of file data in the archive (compressed size; for stored = uncompressed). */
    public long getDataLength() {
        return dataLength;
    }
}
