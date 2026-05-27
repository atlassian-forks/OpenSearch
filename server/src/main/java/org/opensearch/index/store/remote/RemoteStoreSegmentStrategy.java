/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Strategy interface for remote segment store uploads, metadata management, and GC.
 * Implementations control how segment data and metadata are stored in the remote store
 * (e.g. per-file blobs or multi-shard TAR bundles).
 */
@ExperimentalApi
public interface RemoteStoreSegmentStrategy {

    /**
     * Uploads segment files to the remote store.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId         shard ID
     * @param context         files to upload, local store directory, checkpoint, and encryption config
     * @param listener        upload lifecycle callbacks
     * @throws IOException on I/O error
     */
    void upload(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, UploadContext context, UploadListener listener)
        throws IOException;

    /**
     * Uploads segment metadata after a successful {@link #upload}.
     *
     * <p>Strategies that embed metadata atomically in their data blob (e.g. TAR {@code index.bin})
     * should leave this as a no-op. Strategies that write separate metadata blobs
     * (e.g. {@code metadata__xxx} files) must override it.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId         shard ID
     * @param context         metadata content and upload parameters
     * @throws IOException on I/O error
     */
    default void uploadMetadata(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, MetadataUploadContext context)
        throws IOException {}

    /**
     * Deletes stale segments no longer referenced by active commits.
     *
     * @param remoteDirectory  remote segment store directory
     * @param shardId          shard ID
     * @param minCommitsToKeep minimum number of recent commits to retain
     * @throws IOException on I/O error
     */
    void deleteStaleSegments(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, int minCommitsToKeep) throws IOException;

    /**
     * Deletes a single segment file from the remote store.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId         shard ID
     * @param name            file name to delete
     * @throws IOException on I/O error
     */
    void deleteFile(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, String name) throws IOException;

    /**
     * Returns a {@link MetadataReader} for discovering and parsing segment metadata.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId         shard ID
     * @return metadata reader for this strategy
     */
    MetadataReader getMetadataReader(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId);

    /**
     * Upload parameters passed to {@link #upload} on every refresh.
     *
     * @param segmentFiles      active segment files; {@code toUpload=true} means PUT to remote,
     *                          {@code false} means already present (register only)
     * @param storeDirectory    local Lucene directory to read file bytes from
     * @param checkpoint        replication checkpoint (primary term, seqno) for labelling
     * @param isLowPriorityUpload {@code true} for background/merge uploads; may trigger throttling
     * @param cryptoMetadata    server-side encryption config; {@code null} if not enabled
     */
    @ExperimentalApi
    record UploadContext(Collection<SegmentFile> segmentFiles, Directory storeDirectory, ReplicationCheckpoint checkpoint,
        boolean isLowPriorityUpload, CryptoMetadata cryptoMetadata) {

        /**
         * A segment file in an upload context.
         *
         * @param name     local Lucene file name (e.g. {@code _0.cfs})
         * @param toUpload {@code true} if the file must be uploaded; {@code false} if already remote
         */
        @ExperimentalApi
        public record SegmentFile(String name, boolean toUpload) {
        }
    }

    /**
     * Upload lifecycle callbacks per segment file and for the overall batch.
     */
    @ExperimentalApi
    interface UploadListener {
        /** Called when upload of {@code file} starts. */
        void onUploadStart(String file);

        /** Called when upload of {@code file} succeeds. */
        void onUploadSuccess(String file);

        /** Called when upload of {@code file} fails with {@code ex}. */
        void onUploadFailure(String file, Exception ex);

        /** Called when all files in the batch have uploaded successfully. */
        void onAllUploadsSuccess();

        /** Called when the batch upload fails with {@code ex}. */
        void onAllUploadsFailure(Exception ex);
    }

    /**
     * Metadata upload parameters passed to {@link #uploadMetadata}.
     *
     * <p>TAR bundle strategies may ignore all fields — their {@code uploadMetadata} is a no-op.
     * Per-file strategies use every field to construct the {@code metadata__xxx} blob.
     *
     * <p><b>Note</b>: {@code CatalogSnapshot} is an internal engine type; external plugin
     * implementors using the default no-op {@code uploadMetadata} need not interact with it.
     *
     * @param activeSegmentFiles           segment file names active at this commit; used to build
     *                                     the file→remote-name mapping in the metadata blob
     * @param catalogSnapshot              Lucene catalog snapshot for generation, format versions,
     *                                     and {@code SegmentInfos} serialization
     * @param storeDirectory               local staging directory for writing the metadata file
     *                                     before copying to remote (deleted afterwards)
     * @param translogGeneration           translog generation at upload time; embedded in the
     *                                     metadata filename for point-in-time restore correlation
     * @param checkpoint                   replication checkpoint; embedded in filename and metadata
     * @param nodeId                       uploading node ID; used for dual-primary write detection
     * @param catalogSnapshotToCommitSerializer serializes {@code CatalogSnapshot} to
     *                                          {@code SegmentInfos} bytes stored in the metadata blob;
     *                                          must not be {@code null} for real uploads
     */
    @ExperimentalApi
    record MetadataUploadContext(Collection<String> activeSegmentFiles, CatalogSnapshot catalogSnapshot, Directory storeDirectory,
        long translogGeneration, ReplicationCheckpoint checkpoint, String nodeId, CheckedFunction<
            CatalogSnapshot,
            byte[],
            IOException> catalogSnapshotToCommitSerializer) {
    }

    /**
     * Discovers and parses segment metadata from the remote store.
     * Implementations control the metadata naming scheme and format.
     */
    @ExperimentalApi
    interface MetadataReader {

        /**
         * Reads the latest available segment metadata.
         *
         * @return latest metadata, or {@code null} if not found
         * @throws IOException on I/O error
         */
        RemoteSegmentMetadata readMetadata() throws IOException;

        /**
         * Reads metadata for a specific primary term and generation.
         *
         * @return metadata, or {@code null} if not found
         * @throws IOException on I/O error
         */
        RemoteSegmentMetadata readMetadata(long primaryTerm, long generation) throws IOException;

        /**
         * Reads metadata pinned at or before the given timestamp.
         *
         * @return metadata, or {@code null} if not found
         * @throws IOException on I/O error
         */
        RemoteSegmentMetadata readMetadata(long timestamp) throws IOException;

        /**
         * Reads metadata from a specific physical filename.
         *
         * @return metadata, or {@code null} if not found
         * @throws IOException on I/O error
         */
        RemoteSegmentMetadata readMetadata(String filename) throws IOException;

        /**
         * Reads the {@code count} most recent metadata files.
         *
         * @return map of filename → metadata
         * @throws IOException on I/O error
         */
        Map<String, RemoteSegmentMetadata> readLatestNMetadata(int count) throws IOException;

        /**
         * Returns the physical metadata filename for a given primary term and generation.
         *
         * @throws IOException on I/O or discovery error
         */
        String getMetadataFilename(long primaryTerm, long generation) throws IOException;
    }
}
