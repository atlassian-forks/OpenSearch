/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.remote.GcDecision;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Composite SPI for the full translog remote store lifecycle: upload, download, and GC.
 * <p>
 * All three operations must be provided together because they are tightly coupled:
 * <ul>
 *   <li>If upload batches generations into TAR archives, download must read from those archives.</li>
 *   <li>If upload creates archive blobs, GC must avoid issuing LIST calls for per-file blobs that don't exist.</li>
 * </ul>
 * <p>
 * <strong>GC note</strong>: The TAR archive plugin returns {@link GcDecision#SKIP} from
 * {@link #resolveStaleTranslogBlobs} to suppress the per-shard LIST call (expensive on S3).
 * Archive blob GC is instead performed by a node-level background task registered via
 * {@link org.opensearch.plugins.Plugin#createComponents}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface TranslogRemoteStoreStrategy {

    /**
     * Upload a translog snapshot to remote storage.
     *
     * @param transferSnapshot the snapshot of translog files to upload
     * @param listener         listener notified on success/failure
     * @return {@code true} if the snapshot was uploaded successfully
     * @throws IOException if the upload fails
     */
    boolean upload(TransferSnapshot transferSnapshot, TranslogTransferListener listener) throws IOException;

    /**
     * Download translog files for the given primary term and generation.
     *
     * @param primaryTerm the primary term of the translog to recover
     * @param generation  the generation to recover
     * @param location    the local path to write the recovered files to
     * @return {@code true} if the generation was found and downloaded
     * @throws IOException if the download fails
     */
    boolean download(long primaryTerm, long generation, Path location) throws IOException;

    /**
     * Decide how to handle stale translog blob cleanup for this shard.
     * <p>
     * Called during per-shard translog trimming instead of the default LIST + delete logic.
     *
     * @param minPrimaryTerm  minimum primary term to retain
     * @param minGeneration   minimum generation to retain; all older entries are candidates for deletion
     * @return a {@link GcDecision} instructing core how to proceed with cleanup
     * @throws IOException if the decision cannot be made
     */
    default GcDecision resolveStaleTranslogBlobs(long minPrimaryTerm, long minGeneration) throws IOException {
        return GcDecision.USE_DEFAULT;
    }
}
