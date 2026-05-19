/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.GcDecision;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;

/**
 * SPI for the translog remote store lifecycle: upload, download, and stale-blob GC.
 *
 * <p>Core calls these methods per shard. The implementation decides how to fulfil each
 * request — it may upload immediately or defer to any internal coordination mechanism
 * (e.g. node-level batching). Core has no knowledge of the implementation strategy.
 *
 * <p>All three operations should be provided together because they are tightly coupled:
 * <ul>
 *   <li>Upload format determines what download must read.</li>
 *   <li>Upload layout determines which GC LIST calls (if any) make sense.</li>
 * </ul>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface TranslogRemoteStoreStrategy {

    /**
     * Upload a translog snapshot to remote storage.
     *
     * <p>The implementation may upload files immediately, defer to a node-level coordinator,
     * or apply any other batching/compression strategy. Core blocks on this call until the
     * upload is acknowledged or fails.
     *
     * @param transferSnapshot   the snapshot of translog files to upload
     * @param listener           notified on success or failure
     * @param transferService    the shard's transfer service (may be used for blob I/O)
     * @param shardId            identifies the shard (index UUID + shard number); passed so node-level
     *                           batching strategies can correlate uploads without a separate init() call
     * @param repositoryBasePath the translog repository base path; lets the strategy compute its target
     *                           blob path without holding a reference to the repository object
     * @return {@code true} if the snapshot was uploaded successfully
     * @throws IOException if the upload fails
     */
    boolean upload(
        TransferSnapshot transferSnapshot,
        TranslogTransferListener listener,
        TransferService transferService,
        ShardId shardId,
        BlobPath repositoryBasePath
    ) throws IOException;

    /**
     * Download translog files for the given primary term and generation.
     *
     * <p>Core calls this when recovering translog from remote storage. The strategy may return
     * {@code false} to let core fall back to its default per-file download
     * ({@link org.opensearch.index.translog.transfer.TranslogTransferManager#downloadTranslog}).
     *
     * <p>A TAR-based implementation would scan archive blobs for the requested generation and
     * extract the matching {@code .tlog}/{@code .ckp} files to {@code location}.
     *
     * @param primaryTerm        the primary term of the generation to recover
     * @param generation         the generation to recover
     * @param location           local directory to write the recovered files to
     * @param transferService    transfer service for blob I/O; same repository as used at upload time
     * @param shardId            identifies the shard (used for path construction)
     * @param repositoryBasePath translog repository base path
     * @return {@code true} if the generation was found and downloaded; {@code false} to fall back
     * @throws IOException if the download fails
     */
    boolean download(
        long primaryTerm,
        long generation,
        Path location,
        TransferService transferService,
        ShardId shardId,
        BlobPath repositoryBasePath
    ) throws IOException;

    /**
     * Decide how to handle stale translog blob cleanup for this shard.
     *
     * <p>Called during per-shard translog trimming. The default returns
     * {@link GcDecision#USE_DEFAULT}, which runs the standard OpenSearch per-file
     * LIST + delete logic. Implementations that use a different blob layout (e.g. archive
     * blobs containing multiple shards) should return {@link GcDecision#SKIP} to suppress
     * the per-shard LIST call; they are responsible for their own GC lifecycle.
     *
     * @param minPrimaryTerm  minimum primary term to retain
     * @param minGeneration   minimum generation to retain
     * @return a {@link GcDecision} instructing core how to proceed
     * @throws IOException if the decision cannot be made
     */
    default GcDecision resolveStaleTranslogBlobs(long minPrimaryTerm, long minGeneration) throws IOException {
        return GcDecision.USE_DEFAULT;
    }
}
