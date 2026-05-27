/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.io.InputStream;

/**
 * Represents a segment file located in the remote store, allowing block-level
 * byte range streaming.
 */
@ExperimentalApi
public interface RemoteSegmentFile {

    /**
     * The logical Lucene name of the segment file (e.g., "_0.cfs").
     */
    String getName();

    /**
     * The physical size of the segment file in bytes.
     */
    long getLength();

    /**
     * The checksum value of the segment file.
     */
    String getChecksum();

    /**
     * Opens a read stream for a slice of the segment file.
     *
     * @param position the starting byte offset (0 to start from the beginning)
     * @param length   the number of bytes to read
     * @return a standard Java InputStream for the byte slice
     * @throws IOException if a physical I/O or network failure occurs
     */
    InputStream openStream(long position, long length) throws IOException;
}
