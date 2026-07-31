/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.ChannelFactory;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Proves that the same subclass-based extension point used for segment bundling
 * ({@code RemoteSegmentStoreDirectory}) generalizes to translog: overrides exactly one construction seam
 * ({@link #createTranslogTransferManager}) to swap in {@link BundlingTranslogTransferManager}, changing
 * nothing else about how {@code RemoteFsTranslog} works.
 * <p>
 * Deliberately narrower in scope than a production translog implementation would need to be:
 * <ul>
 *   <li>Batches uploads only within this one shard (a full implementation would batch across every shard on
 *       a node, which needs a node-level registry and a way for other nodes/replicas to discover which node
 *       holds a given shard's bundle; not attempted here).</li>
 *   <li>No durable pointer is written anywhere, so a fresh replica or a restart cannot recover files that
 *       only exist inside a bundle blob. This is the same correctness gap raised for segments during design
 *       review, compounded by translog's stricter durability requirement.</li>
 * </ul>
 * Exists to validate the mechanism, not to be adopted as-is.
 */
public class BatchingRemoteFsTranslog extends RemoteFsTranslog {

    public BatchingRemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier startedPrimarySupplier,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        TranslogOperationHelper translogOperationHelper,
        boolean isServerSideEncryptionEnabled
    ) throws IOException {
        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            blobStoreRepository,
            threadPool,
            startedPrimarySupplier,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            translogOperationHelper,
            (ChannelFactory) FileChannel::open,
            isServerSideEncryptionEnabled
        );
    }

    @Override
    protected TranslogTransferManager createTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker tracker,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled,
        boolean isServerSideEncryptionEnabled
    ) throws IOException {
        // Duplicates the small amount of path/transfer-service derivation that
        // RemoteFsTranslog.buildTranslogTransferManager does, so core needs no further changes beyond the
        // createTranslogTransferManager seam itself.
        return TranslogPathSupport.buildBundlingTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            tracker,
            pathStrategy,
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            isServerSideEncryptionEnabled
        );
    }
}
