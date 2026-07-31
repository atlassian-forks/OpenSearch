/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * Duplicates the small amount of blob-path/transfer-service derivation from
 * {@code RemoteFsTranslog.buildTranslogTransferManager}. That method returns a concrete
 * {@code TranslogTransferManager}, not the pieces needed to build a subclass instead, so there is no way to
 * reuse it directly without either changing its return type (a bigger core change) or duplicating this here.
 * Kept as a standalone helper so the duplication is visible and easy to delete if core ever gets a proper
 * {@code protected} builder hook instead.
 */
final class TranslogPathSupport {

    private TranslogPathSupport() {}

    static BundlingTranslogTransferManager buildBundlingTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker tracker,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled,
        boolean isServerSideEncryptionEnabled
    ) {
        String indexUUID = shardId.getIndex().getUUID();
        String shardIdStr = String.valueOf(shardId.id());
        RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(DATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath dataPath = pathStrategy.generatePath(dataPathInput);
        RemoteStorePathStrategy.ShardDataPathInput mdPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(METADATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
        BlobStoreTransferService transferService = new BlobStoreTransferService(
            blobStoreRepository.blobStore(isServerSideEncryptionEnabled),
            threadPool
        );
        return new BundlingTranslogTransferManager(
            shardId,
            transferService,
            dataPath,
            mdPath,
            fileTransferTracker,
            tracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled
        );
    }
}
