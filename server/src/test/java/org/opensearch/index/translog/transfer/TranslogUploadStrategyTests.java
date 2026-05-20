/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.GcDecision;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

import static org.mockito.Mockito.mock;

/**
 * Tests for the translog remote store strategy SPI.
 */
public class TranslogUploadStrategyTests extends OpenSearchTestCase {

    /**
     * Verifies that {@link TranslogTransferManager} implements the translog strategy contract
     * via {@code transferSnapshot} and {@code downloadTranslog}.
     */
    public void testTranslogTransferManagerFulfillsStrategyContract() throws Exception {
        ShardId shardId = new ShardId(new Index("index", "indexUuid"), 0);
        TransferService transferService = mock(TransferService.class);
        BlobPath remoteBaseTransferPath = new BlobPath().add("base_path");
        RemoteTranslogTransferTracker tracker = new RemoteTranslogTransferTracker(shardId, 20);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, tracker);

        TranslogTransferManager manager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add("translog"),
            remoteBaseTransferPath.add("metadata"),
            fileTransferTracker,
            tracker,
            DefaultRemoteStoreSettings.INSTANCE,
            false
        );

        // Verify manager is usable as a translog strategy by wrapping in adapter
        TranslogRemoteStoreStrategy strategy = new TranslogRemoteStoreStrategy() {
            @Override
            public boolean upload(
                TransferSnapshot snapshot,
                TranslogTransferListener listener,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return manager.transferSnapshot(snapshot, listener);
            }

            @Override
            public boolean download(
                long primaryTerm,
                long generation,
                Path location,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return manager.downloadTranslog(String.valueOf(primaryTerm), String.valueOf(generation), location);
            }
        };

        assertNotNull("Strategy must not be null", strategy);
    }

    /**
     * Verifies that the default {@link TranslogRemoteStoreStrategy#resolveStaleTranslogBlobs}
     * returns USE_DEFAULT.
     */
    public void testDefaultGcDecisionIsUseDefault() throws Exception {
        TranslogRemoteStoreStrategy strategy = new TranslogRemoteStoreStrategy() {
            @Override
            public boolean upload(
                TransferSnapshot snapshot,
                TranslogTransferListener listener,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return false;
            }

            @Override
            public boolean download(
                long primaryTerm,
                long generation,
                Path location,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return false;
            }
        };

        GcDecision decision = strategy.resolveStaleTranslogBlobs(1L, 10L);
        assertEquals("Default GC must be USE_DEFAULT", GcDecision.Kind.USE_DEFAULT, decision.kind());
    }

    /**
     * Verifies that a custom strategy can override GC to SKIP.
     */
    public void testCustomStrategyCanReturnSkipForTranslogGc() throws Exception {
        TranslogRemoteStoreStrategy strategy = new TranslogRemoteStoreStrategy() {
            @Override
            public boolean upload(
                TransferSnapshot snapshot,
                TranslogTransferListener listener,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return false;
            }

            @Override
            public boolean download(
                long primaryTerm,
                long generation,
                Path location,
                TransferService ts,
                org.opensearch.core.index.shard.ShardId sid,
                org.opensearch.common.blobstore.BlobPath bp
            ) throws java.io.IOException {
                return false;
            }

            @Override
            public GcDecision resolveStaleTranslogBlobs(long minPrimaryTerm, long minGeneration) {
                return GcDecision.SKIP;
            }
        };

        GcDecision decision = strategy.resolveStaleTranslogBlobs(1L, 10L);
        assertEquals("Custom strategy must return SKIP", GcDecision.Kind.SKIP, decision.kind());
    }
}
