/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.OpenSearchException;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.nio.file.Path;

public final class InspectableFsBlobStore extends FsBlobStore {
    public InspectableFsBlobStore(final int bufferSizeInBytes, final Path path, final boolean readonly) throws IOException {
        super(bufferSizeInBytes, path, readonly);
    }

    @Override
    public BlobContainer blobContainer(final BlobPath path) {
        try {
            return new InspectableFsBlobContainer(this, path, buildAndCreate(path));
        } catch (final IOException ex) {
            throw new OpenSearchException("failed to create blob container", ex);
        }
    }
}
