/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.List;

/**
 * Rate-limited remote object operations available to a segment blob layout.
 *
 * <p>This facade is the only remote-object access point exposed to layouts. It keeps repository implementation
 * details, download throttling, upload throttling, encryption handling, and multi-stream upload behavior in core.
 * Layouts must not bypass it with direct blob-container reads or writes.
 *
 * <p>The {@code length} supplied to read methods belongs to the requested logical range. It can differ from the
 * complete underlying object length when a layout stores files in a shared archive.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class RemoteSegmentBlobStore {

    private final RemoteDirectory remoteDirectory;

    RemoteSegmentBlobStore(RemoteDirectory remoteDirectory) {
        this.remoteDirectory = remoteDirectory;
    }

    /**
     * Copies one local object to remote storage using the selected upload priority and encryption metadata.
     *
     * @return {@code true} when the upload completes asynchronously and invokes {@code listener}; {@code false} when
     *         the caller must use the synchronous fallback
     */
    public boolean copyFrom(
        Directory from,
        String source,
        String destination,
        IOContext context,
        Runnable postUpload,
        ActionListener<Void> listener,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) throws IOException {
        return remoteDirectory.copyFrom(from, source, destination, context, postUpload, listener, lowPriorityUpload, cryptoMetadata);
    }

    /**
     * Copies one local object synchronously for repositories without asynchronous multi-stream support.
     */
    public void copyFrom(Directory from, String source, String destination, IOContext context) throws IOException {
        remoteDirectory.copyFrom(from, source, destination, context);
    }

    /**
     * Opens a complete remote object through the configured download rate limiter.
     */
    public IndexInput openInput(String location, long length, IOContext context) throws IOException {
        return remoteDirectory.openInput(location, length, context);
    }

    /**
     * Opens exactly one remote byte range through the configured download rate limiter.
     */
    public IndexInput openBlockInput(String location, long position, long length, long logicalLength, IOContext context)
        throws IOException {
        return remoteDirectory.openBlockInput(location, position, length, logicalLength, context);
    }

    /**
     * Deletes one physical remote object.
     */
    public void deleteFile(String location) throws IOException {
        remoteDirectory.deleteFile(location);
    }

    /**
     * Deletes a batch of physical remote objects.
     */
    public void deleteFiles(List<String> locations) throws IOException {
        remoteDirectory.deleteFiles(locations);
    }
}
