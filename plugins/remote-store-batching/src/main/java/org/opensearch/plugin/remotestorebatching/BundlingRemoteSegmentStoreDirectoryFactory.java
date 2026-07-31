/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Identical to {@link RemoteSegmentStoreDirectoryFactory} except for the final construction call, which
 * returns a {@link BundlingRemoteSegmentStoreDirectory} instead of the default. All path/repository/
 * lock-manager resolution, including the pluggable-data-format branching for {@code dataDirectory}, is
 * inherited unchanged rather than duplicated, so this factory tracks that resolution automatically as it
 * evolves upstream instead of needing to be kept in sync by hand.
 */
public class BundlingRemoteSegmentStoreDirectoryFactory extends RemoteSegmentStoreDirectoryFactory {

    public BundlingRemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix
    ) {
        super(repositoriesService, threadPool, segmentsPathFixedPrefix);
    }

    @Override
    protected RemoteSegmentStoreDirectory createDirectory(
        RemoteDirectory dataDirectory,
        RemoteDirectory metadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        Map<String, String> pendingDownloadMergedSegments
    ) throws IOException {
        return new BundlingRemoteSegmentStoreDirectory(
            dataDirectory,
            metadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingDownloadMergedSegments
        );
    }
}
