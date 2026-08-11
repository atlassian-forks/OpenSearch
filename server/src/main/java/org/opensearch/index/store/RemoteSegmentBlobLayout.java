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
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Defines the physical representation of logical remote segment files.
 *
 * <p>Core selects the logical files for each existing refresh-triggered upload and retains ownership of their
 * checksums, uploaded-segment metadata, checkpoint publication, retries, and translog retention. A layout must only
 * decide how those already selected files are persisted and read from the remote store. For example, the built-in
 * layout stores each logical file as one remote object, while an extension can store every file in the selected batch
 * in one immutable archive.
 *
 * <p>The location returned from {@link #writeBatch(WriteContext, ActionListener)} is opaque to core. Core records it
 * in {@code UploadedSegmentMetadata.uploadedFilename} and supplies it unchanged to read and deletion calls. A layout
 * must therefore keep the location self-describing for as long as retained remote metadata can reference it.
 *
 * <p>Implementations must not change upload scheduling, publish metadata or checkpoints, delete metadata files, or
 * make translog-retention decisions. They must also treat every successful remote object as immutable. These limits
 * make a layout usable by recovery, snapshots, cleanup, and normal refresh uploads without replacing the remote store
 * directory or its lifecycle.
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface RemoteSegmentBlobLayout {

    /**
     * Persists every logical file selected for one upload batch.
     *
     * <p>The listener must receive exactly one opaque location for every name in {@link WriteContext#getFiles()} only
     * after the complete batch is durable in remote storage. Returning a partial map is an error. On failure, the
     * listener must fail without returning locations. Core then leaves its uploaded-file cache, remote metadata,
     * checkpoint, and translog-retention state unchanged.
     */
    void writeBatch(WriteContext context, ActionListener<Map<String, String>> listener);

    /**
     * Opens a logical segment file from its opaque physical location.
     *
     * <p>{@code logicalLength} is the length recorded in segment metadata. It is not necessarily the size of the
     * underlying remote object. A shared archive layout must return an input whose length is the logical file length.
     */
    IndexInput openInput(String physicalLocation, long logicalLength, IOContext context) throws IOException;

    /**
     * Opens a range within a logical segment file from its opaque physical location.
     *
     * <p>The returned input represents exactly {@code length} bytes beginning at {@code position}. Implementations
     * should use {@link RemoteSegmentBlobStore#openBlockInput(String, long, long, long, IOContext)} so remote download
     * rate limits remain in effect and unrelated archive entries are not downloaded.
     */
    IndexInput openBlockInput(String physicalLocation, long position, long length, long logicalLength, IOContext context)
        throws IOException;

    /**
     * Releases one logical file after core removes its immediate reference.
     *
     * <p>A one-file-per-object layout can delete its object eagerly. A shared-object layout must not delete the shared
     * object here because another logical file can still reference it. It should defer physical deletion to
     * {@link #deleteUnreferenced(Collection, Set)}.
     */
    void releaseLogicalFile(String physicalLocation) throws IOException;

    /**
     * Deletes physical data that no retained segment metadata still references.
     *
     * <p>Core computes the complete stale and active location sets from retained metadata before invoking this method.
     * A shared-object layout must compare locations at its physical-object granularity and delete an object only when
     * no active location points into it. Implementations must not keep separate persisted reference counts.
     */
    void deleteUnreferenced(Collection<String> stalePhysicalLocations, Set<String> activePhysicalLocations) throws IOException;

    /**
     * Immutable input for one complete refresh-selected upload batch.
     *
     * <p>The source directory contains the logical files in {@link #getFiles()}. The collection is ordered so a layout
     * can produce deterministic archive entries. {@link #getRemoteStore()} exposes only rate-limited object operations;
     * layouts must use it instead of accessing a repository blob container directly. The remaining values preserve the
     * I/O, priority, shard, and encryption context selected by core.
     */
    @ExperimentalApi
    final class WriteContext {
        private final Directory sourceDirectory;
        private final Collection<String> files;
        private final RemoteSegmentBlobStore remoteStore;
        private final ShardId shardId;
        private final IOContext ioContext;
        private final boolean lowPriorityUpload;
        private final CryptoMetadata cryptoMetadata;

        public WriteContext(
            Directory sourceDirectory,
            Collection<String> files,
            RemoteSegmentBlobStore remoteStore,
            ShardId shardId,
            IOContext ioContext,
            boolean lowPriorityUpload,
            CryptoMetadata cryptoMetadata
        ) {
            this.sourceDirectory = sourceDirectory;
            this.files = files;
            this.remoteStore = remoteStore;
            this.shardId = shardId;
            this.ioContext = ioContext;
            this.lowPriorityUpload = lowPriorityUpload;
            this.cryptoMetadata = cryptoMetadata;
        }

        public Directory getSourceDirectory() {
            return sourceDirectory;
        }

        public Collection<String> getFiles() {
            return files;
        }

        public RemoteSegmentBlobStore getRemoteStore() {
            return remoteStore;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public IOContext getIoContext() {
            return ioContext;
        }

        public boolean isLowPriorityUpload() {
            return lowPriorityUpload;
        }

        public CryptoMetadata getCryptoMetadata() {
            return cryptoMetadata;
        }
    }
}
