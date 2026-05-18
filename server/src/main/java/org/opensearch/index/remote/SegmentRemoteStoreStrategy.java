/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Composite SPI for the full segment remote store lifecycle: upload, download, and GC.
 * <p>
 * All three operations must be provided together because they are tightly coupled:
 * <ul>
 *   <li>If upload bundles files into a TAR archive, download must issue range-GETs.</li>
 *   <li>If upload creates archive blobs, GC must know how to clean them up.</li>
 * </ul>
 * A plugin implementing this interface provides a consistent, complete strategy.
 * <p>
 * <strong>GC note</strong>: The TAR archive plugin returns {@link GcDecision#SKIP} from
 * {@link #resolveStaleBlobs} to suppress the per-shard LIST call (expensive on S3).
 * Archive blob GC is instead performed by a node-level background task registered via
 * {@link org.opensearch.plugins.Plugin#createComponents}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface SegmentRemoteStoreStrategy {

    /**
     * Upload new segment files to remote storage.
     *
     * @param files           the logical filenames to upload (e.g. {@code _0.cfe}, {@code _0.cfs})
     * @param sizeMap         map of filename → file size in bytes
     * @param storeDirectory  local shard store directory
     * @param remoteDirectory remote segment store directory to register uploaded files into
     * @param shard           the index shard owning these segments
     * @param listener        completion listener; called with {@code null} on success or an exception on failure
     */
    void upload(
        Collection<String> files,
        Map<String, Long> sizeMap,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        IndexShard shard,
        ActionListener<Void> listener
    );

    /**
     * Open an {@link IndexInput} that reads the segment file described by {@code metadata}.
     * <p>
     * For per-file uploads, this issues a standard blob GET.
     * For TAR archive uploads, this issues a range-GET at the file's offset within the archive blob.
     *
     * @param name     the logical segment filename (e.g. {@code _0.cfe})
     * @param metadata upload metadata describing where the file lives in remote storage
     * @return an {@link IndexInput} positioned at byte 0 of the segment data
     * @throws IOException if the file cannot be read
     */
    IndexInput openInput(String name, UploadedSegmentMetadata metadata) throws IOException;

    /**
     * Decide how to handle stale blob cleanup for this shard.
     * <p>
     * Called during per-shard GC instead of the default LIST-all-blobs logic.
     *
     * @param activeUploadedFilenames the set of uploaded filenames referenced by active segment metadata
     * @param latestMetadataGeneration the metadata generation of the latest committed segment info
     * @return a {@link GcDecision} instructing core how to proceed with cleanup
     * @throws IOException if the decision cannot be made
     */
    default GcDecision resolveStaleBlobs(Set<String> activeUploadedFilenames, long latestMetadataGeneration) throws IOException {
        return GcDecision.USE_DEFAULT;
    }
}
