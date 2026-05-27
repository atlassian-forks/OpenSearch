/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class InspectableFsBlobContainer extends FsBlobContainer {
    public InspectableFsBlobContainer(final FsBlobStore blobStore, final BlobPath blobPath, final Path path) {
        super(blobStore, blobPath, path);
    }

    @Override
    public void writeBlob(final String blobName, final InputStream inputStream, final long blobSize, final boolean failIfAlreadyExists)
        throws IOException {
        BlobStoreStats.putCount.incrementAndGet();
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        BlobStoreStats.getCount.incrementAndGet();
        return super.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(final String blobName, final long position, final long length) throws IOException {
        BlobStoreStats.getCount.incrementAndGet();
        return super.readBlob(blobName, position, length);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        BlobStoreStats.listCount.incrementAndGet();
        return super.listBlobs();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(final String blobNamePrefix) throws IOException {
        BlobStoreStats.listCount.incrementAndGet();
        return super.listBlobsByPrefix(blobNamePrefix);
    }

    @Override
    public DeleteResult delete() throws IOException {
        BlobStoreStats.deleteCount.incrementAndGet();
        return super.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        BlobStoreStats.deleteCount.incrementAndGet();
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }
}
