/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A {@link FsBlobStore} subclass that returns {@link CountingFsBlobContainer} instances,
 * enabling operation counting (LIST/GET/PUT/DELETE) for testing purposes.
 */
public class CountingFsBlobStore extends FsBlobStore {

    public CountingFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly) throws IOException {
        super(bufferSizeInBytes, path, readonly);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new CountingFsBlobContainer(this, path, buildAndCreate(path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create CountingFsBlobContainer", e);
        }
    }

    /**
     * Expose the buildAndCreate method which is protected in FsBlobStore.
     * We need this to construct the filesystem path for the container.
     */
    @Override
    protected synchronized Path buildAndCreate(BlobPath path) throws IOException {
        return super.buildAndCreate(path);
    }
}
