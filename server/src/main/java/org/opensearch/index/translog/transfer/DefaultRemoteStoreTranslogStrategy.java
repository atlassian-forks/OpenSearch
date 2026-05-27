/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Default implementation of RemoteStoreTranslogStrategy which delegates to TranslogTransferManager.
 */
public final class DefaultRemoteStoreTranslogStrategy implements RemoteStoreTranslogStrategy {

    private final TranslogTransferManager translogTransferManager;

    public DefaultRemoteStoreTranslogStrategy(final TranslogTransferManager translogTransferManager) {
        this.translogTransferManager = translogTransferManager;
    }

    @Override
    public boolean transferSnapshot(
        final ShardId shardId,
        final TransferSnapshot transferSnapshot,
        final TranslogTransferListener listener,
        final CryptoMetadata cryptoMetadata
    ) throws IOException {
        return translogTransferManager.transferSnapshot(transferSnapshot, listener, cryptoMetadata);
    }

    @Override
    public void downloadRange(
        final ShardId shardId,
        final long minGeneration,
        final long maxGeneration,
        final Map<String, String> generationToPrimaryTerm,
        final Path location
    ) throws IOException {
        for (long gen = maxGeneration; gen >= minGeneration; gen--) {
            final String genStr = Long.toString(gen);
            final String primaryTerm = generationToPrimaryTerm.get(genStr);
            if (primaryTerm == null) {
                continue;
            }
            translogTransferManager.downloadTranslog(primaryTerm, genStr, location);
        }
    }
}
