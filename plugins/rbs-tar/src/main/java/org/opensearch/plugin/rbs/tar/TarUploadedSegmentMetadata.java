/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.io.IOException;
import java.io.InputStream;

/**
 * Metadata for a segment file uploaded as part of a tar bundle.
 * Supports reading a byte range of the segment by delegating to a range read
 * of the parent tar file.
 */
public final class TarUploadedSegmentMetadata extends UploadedSegmentMetadata {
    private final String tarFilename;
    private final long tarOffset;
    private final RemoteDirectory remoteDataDirectory;

    public TarUploadedSegmentMetadata(
        String name,
        String tarFilename,
        long tarOffset,
        String checksum,
        long length,
        RemoteDirectory remoteDataDirectory
    ) {
        super(name, tarFilename + "#" + tarOffset, checksum, length);
        this.tarFilename = tarFilename;
        this.tarOffset = tarOffset;
        this.remoteDataDirectory = remoteDataDirectory;
    }

    public String getTarFilename() {
        return tarFilename;
    }

    public long getTarOffset() {
        return tarOffset;
    }

    @Override
    public InputStream openStream(long position, long length) throws IOException {
        return remoteDataDirectory.getBlobContainer().readBlob(tarFilename, tarOffset + position, length);
    }
}
