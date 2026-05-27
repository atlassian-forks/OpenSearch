/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.io.IOException;
import java.io.InputStream;

/**
 * Default implementation of {@link UploadedSegmentMetadata} that performs range reads.
 */
public final class DefaultUploadedSegmentMetadata extends UploadedSegmentMetadata {
    private final RemoteDirectory remoteDataDirectory;

    public DefaultUploadedSegmentMetadata(
        String name,
        String uploadedFilename,
        String checksum,
        long length,
        RemoteDirectory remoteDataDirectory
    ) {
        super(name, uploadedFilename, checksum, length);
        this.remoteDataDirectory = remoteDataDirectory;
    }

    @Override
    public InputStream openStream(long position, long length) throws IOException {
        return remoteDataDirectory.getBlobContainer().readBlob(getUploadedFilename(), position, length);
    }
}
