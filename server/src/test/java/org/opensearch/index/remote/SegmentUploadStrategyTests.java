/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

/**
 * Tests for the segment remote store strategy SPI.
 */
public class SegmentUploadStrategyTests extends OpenSearchTestCase {

    /**
     * Verifies that {@link DefaultSegmentRemoteStoreStrategy} completes immediately on empty file list.
     */
    public void testDefaultStrategyEmptyFilesCompletesImmediately() {
        RemoteSegmentTransferTracker tracker = mock(RemoteSegmentTransferTracker.class);
        DefaultSegmentRemoteStoreStrategy strategy = new DefaultSegmentRemoteStoreStrategy(tracker);

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean failed = new AtomicBoolean(false);

        strategy.upload(
            java.util.List.of(),
            Map.of(),
            null,
            null,
            null,
            ActionListener.wrap(v -> completed.set(true), e -> failed.set(true))
        );

        assertTrue("Empty upload must complete immediately", completed.get());
        assertFalse("Empty upload must not fail", failed.get());
    }

    /**
     * Verifies that {@link DefaultSegmentRemoteStoreStrategy} returns USE_DEFAULT for GC.
     */
    public void testDefaultStrategyGcDecisionIsUseDefault() throws Exception {
        RemoteSegmentTransferTracker tracker = mock(RemoteSegmentTransferTracker.class);
        DefaultSegmentRemoteStoreStrategy strategy = new DefaultSegmentRemoteStoreStrategy(tracker);

        GcDecision decision = strategy.resolveStaleBlobs(java.util.Set.of(), 0L);
        assertEquals("Default strategy must use default GC", GcDecision.Kind.USE_DEFAULT, decision.kind());
    }

    /**
     * Verifies that a custom {@link SegmentRemoteStoreStrategy} can override GC to SKIP.
     */
    public void testCustomStrategyCanReturnSkipForGc() throws Exception {
        SegmentRemoteStoreStrategy customStrategy = new SegmentRemoteStoreStrategy() {
            @Override
            public void upload(
                java.util.Collection<String> files,
                Map<String, Long> sizeMap,
                org.apache.lucene.store.Directory store,
                org.opensearch.index.store.RemoteSegmentStoreDirectory remote,
                org.opensearch.index.shard.IndexShard shard,
                ActionListener<Void> listener
            ) {
                listener.onResponse(null);
            }

            @Override
            public org.apache.lucene.store.IndexInput openInput(
                String name,
                org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public GcDecision resolveStaleBlobs(java.util.Set<String> active, long gen) {
                return GcDecision.SKIP;
            }
        };

        GcDecision decision = customStrategy.resolveStaleBlobs(java.util.Set.of(), 0L);
        assertEquals("Custom strategy must return SKIP", GcDecision.Kind.SKIP, decision.kind());
    }

    /**
     * Verifies that {@link GcDecision#deleteBlobs} carries the blob names correctly.
     */
    public void testGcDecisionDeleteBlobsCarriesNames() {
        java.util.Set<String> blobs = java.util.Set.of("blob1", "blob2");
        GcDecision decision = GcDecision.deleteBlobs(blobs);
        assertEquals("DELETE_BLOBS kind", GcDecision.Kind.DELETE_BLOBS, decision.kind());
        assertEquals("Must carry blob names", blobs, decision.blobNames());
    }
}
