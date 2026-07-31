/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Mirrors {@code RemoteBlobStoreInternalTranslogFactory} but constructs a {@link BatchingRemoteFsTranslog}
 * instead of the default {@code RemoteFsTranslog}. See {@link BatchingRemoteFsTranslog} for what this covers
 * and does not cover.
 */
public class BatchingTranslogFactory implements TranslogFactory {

    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final ThreadPool threadPool;
    private final RemoteStoreSettings remoteStoreSettings;

    public BatchingTranslogFactory(
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        RemoteStoreSettings remoteStoreSettings
    ) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier
    ) throws IOException {
        return newTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            startedPrimarySupplier,
            TranslogOperationHelper.DEFAULT
        );
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier,
        TranslogOperationHelper translogOperationHelper
    ) throws IOException {
        String repositoryName = config.getIndexSettings().getRemoteStoreTranslogRepository();
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(repositoryName);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
        RemoteTranslogTransferTracker tracker = new RemoteTranslogTransferTracker(config.getShardId(), 20);
        return new BatchingRemoteFsTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            (BlobStoreRepository) repository,
            threadPool,
            startedPrimarySupplier,
            tracker,
            remoteStoreSettings,
            translogOperationHelper,
            false
        );
    }
}
