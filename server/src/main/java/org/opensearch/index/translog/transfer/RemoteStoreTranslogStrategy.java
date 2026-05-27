/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Strategy interface for translog snapshot transfer and download.
 */
@ExperimentalApi
public interface RemoteStoreTranslogStrategy {

    /**
     * Instructs core how to handle its per-generation remote translog GC.
     *
     * <p>Strategies that store multiple generations in a single bundle blob (e.g. TAR node
     * bundles) must return {@link GcDecision#SKIP} to prevent core from issuing DELETE calls
     * against per-generation paths that do not exist in the remote store. Without this, core
     * will flood S3/remote-store with 404-producing DELETE requests on every flush cycle
     * (one per stale generation per shard), paying API costs for operations that do nothing.
     *
     * <p>When {@link GcDecision#SKIP} is returned, the plugin is fully responsible for GC
     * (e.g. via {@code NodeBundleRegistry.runGarbageCollection()}).
     *
     * <p>The default returns {@link GcDecision#USE_DEFAULT}, preserving the existing
     * per-generation DELETE behaviour for strategies that store one file per generation.
     *
     * @return {@link GcDecision#SKIP} if core should skip remote GC for this strategy,
     *         {@link GcDecision#USE_DEFAULT} to proceed with core's per-generation deletes
     */
    default GcDecision resolveStaleTranslogBlobs() {
        return GcDecision.USE_DEFAULT;
    }

    /**
     * Controls whether core's per-generation remote translog GC runs.
     */
    @ExperimentalApi
    enum GcDecision {
        /** Plugin owns GC — core must not issue per-generation DELETE calls. */
        SKIP,
        /** Proceed with core's default per-generation DELETE behaviour. */
        USE_DEFAULT
    }

    /**
     * Transfers a translog snapshot to the remote translog store.
     *
     * @param shardId the shard ID
     * @param transferSnapshot the translog snapshot to transfer
     * @param listener listener to handle transfer completion and failure events
     * @param cryptoMetadata encryption metadata for secure uploads
     * @return true if the transfer was successful, false otherwise
     * @throws IOException in case of I/O error during transfer
     */
    /**
     * Returns strategies to try in order if {@link #downloadRange} cannot find all requested
     * generations. This enables safe rolling upgrades when a shard's strategy changes
     * (e.g. from {@code "default"} per-file to {@code "tar"} bundles) — old blobs written by
     * the previous strategy are still recoverable via the fallback chain.
     *
     * <p>Core calls each fallback strategy's {@link #downloadRange} for any generation not yet
     * found, stopping when all generations are satisfied or the chain is exhausted.
     *
     * <p>The default returns an empty list (no fallback), which is correct for the common case
     * where the strategy is stable and all blobs were written by the same strategy.
     *
     * @return ordered list of fallback strategies; may be empty
     */
    default java.util.List<RemoteStoreTranslogStrategy> fallbackStrategies() {
        return java.util.Collections.emptyList();
    }

    boolean transferSnapshot(
        ShardId shardId,
        TransferSnapshot transferSnapshot,
        TranslogTransferListener listener,
        CryptoMetadata cryptoMetadata
    ) throws IOException;

    /**
     * Downloads all translog files in the given generation range in a single pass.
     *
     * <p>Implementations must download all translog and checkpoint files for generations
     * [{@code minGeneration}..{@code maxGeneration}] that are present in
     * {@code generationToPrimaryTerm}. Strategies that store multiple generations in a
     * single bundle blob (e.g. TAR node bundles) should perform one in-memory registry
     * lookup and batch the range-GETs. Strategies that store one file per generation
     * (e.g. the default per-file strategy) may issue one download call per generation.
     *
     * @param shardId the shard ID
     * @param minGeneration first generation to recover (inclusive)
     * @param maxGeneration last generation to recover (inclusive)
     * @param generationToPrimaryTerm map from generation number to primary term
     * @param location the local directory path to write recovered files to
     * @throws IOException if a download error occurs
     */
    void downloadRange(ShardId shardId, long minGeneration, long maxGeneration, Map<String, String> generationToPrimaryTerm, Path location)
        throws IOException;

}
