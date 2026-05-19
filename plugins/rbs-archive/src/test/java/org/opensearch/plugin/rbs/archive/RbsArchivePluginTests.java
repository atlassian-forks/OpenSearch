/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link RbsArchivePlugin}.
 */
public class RbsArchivePluginTests extends OpenSearchTestCase {

    private final RbsArchivePlugin plugin = new RbsArchivePlugin();

    /**
     * Verifies that the plugin provides a non-null {@link SegmentRemoteStoreStrategy}
     * and that it is a {@link TarSegmentRemoteStoreStrategy}.
     */
    public void testGetSegmentStrategyReturnsTarStrategy() {
        SegmentRemoteStoreStrategy strategy = plugin.getSegmentStrategy();
        assertNotNull("Plugin must provide a SegmentRemoteStoreStrategy", strategy);
        assertTrue(
            "Plugin segment strategy must be TarSegmentRemoteStoreStrategy",
            strategy instanceof TarSegmentRemoteStoreStrategy
        );
    }

    /**
     * Verifies that the translog strategy returns null (uses default per-file behavior).
     * This is intentional until TAR translog bundling is fully implemented.
     */
    public void testGetTranslogStrategyReturnsNullForDefault() {
        TranslogRemoteStoreStrategy strategy = plugin.getTranslogStrategy();
        assertNull(
            "Plugin translog strategy must be null (use default) until TAR translog is implemented",
            strategy
        );
    }

    /**
     * Verifies that {@link TarSegmentRemoteStoreStrategy} is a valid {@link SegmentRemoteStoreStrategy}.
     */
    public void testTarSegmentStrategyImplementsInterface() {
        TarSegmentRemoteStoreStrategy strategy = new TarSegmentRemoteStoreStrategy();
        assertNotNull("TarSegmentRemoteStoreStrategy must not be null", strategy);
        assertTrue(
            "TarSegmentRemoteStoreStrategy must implement SegmentRemoteStoreStrategy",
            strategy instanceof SegmentRemoteStoreStrategy
        );
    }

    /**
     * Verifies that the TAR segment strategy completes immediately when given an empty file list.
     */
    public void testTarStrategyWithEmptyFilesCompletesImmediately() {
        TarSegmentRemoteStoreStrategy strategy = new TarSegmentRemoteStoreStrategy();

        java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicBoolean failed = new java.util.concurrent.atomic.AtomicBoolean(false);

        org.opensearch.core.action.ActionListener<Void> listener = org.opensearch.core.action.ActionListener.wrap(
            v -> completed.set(true),
            e -> failed.set(true)
        );

        strategy.upload(java.util.List.of(), java.util.Map.of(), null, null, null, listener);

        assertTrue("Empty upload must complete immediately", completed.get());
        assertFalse("Empty upload must not fail", failed.get());
    }

    /**
     * Verifies constants on {@link TarSegmentRemoteStoreStrategy} are set correctly.
     */
    public void testTarStrategyConstants() {
        assertEquals("MAX_ARCHIVE_BYTES must be 256 MB", 256 * 1024 * 1024L, TarSegmentRemoteStoreStrategy.MAX_ARCHIVE_BYTES);
    }

    /**
     * Verifies that TAR strategy returns SKIP for GC (not USE_DEFAULT).
     */
    public void testTarStrategyGcDecisionIsSkip() throws Exception {
        TarSegmentRemoteStoreStrategy strategy = new TarSegmentRemoteStoreStrategy();
        org.opensearch.index.remote.GcDecision decision = strategy.resolveStaleBlobs(java.util.Set.of(), 0L);
        assertEquals(
            "TAR strategy must return SKIP to suppress per-shard LIST call",
            org.opensearch.index.remote.GcDecision.Kind.SKIP,
            decision.kind()
        );
    }
}
