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
import org.opensearch.common.io.IndexIOStreamHandler;

import java.io.IOException;

/**
 * Handler for {@link RemoteSegmentMetadata}
 *
 * @opensearch.internal
 */
public class RemoteSegmentMetadataHandler implements IndexIOStreamHandler<RemoteSegmentMetadata> {

    private static final ThreadLocal<Integer> VERSION_CONTEXT = ThreadLocal.withInitial(() -> RemoteSegmentMetadata.CURRENT_VERSION);

    /**
     * Sets the version context for reading metadata
     * @param version the version to use for reading
     */
    public static void setVersionContext(int version) {
        VERSION_CONTEXT.set(version);
    }

    /**
     * Gets the current version context
     * @return the version context
     */
    public static int getVersionContext() {
        return VERSION_CONTEXT.get();
    }

    /**
     * Clears the version context
     */
    public static void clearVersionContext() {
        VERSION_CONTEXT.remove();
    }

    /**
     * Reads metadata content from metadata file input stream and parsed into {@link RemoteSegmentMetadata}
     * @param indexInput metadata file input stream with {@link IndexInput#getFilePointer()} pointing to metadata content
     * @return {@link RemoteSegmentMetadata}
     */
    @Override
    public RemoteSegmentMetadata readContent(IndexInput indexInput) throws IOException {
        return RemoteSegmentMetadata.read(indexInput, getVersionContext());
    }

    /**
     * Writes metadata to file output stream
     * @param indexOutput metadata file input stream
     * @param content {@link RemoteSegmentMetadata} from which metadata content would be generated
     */
    @Override
    public void writeContent(IndexOutput indexOutput, RemoteSegmentMetadata content) throws IOException {
        content.write(indexOutput);
    }
}
