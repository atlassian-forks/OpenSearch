/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link RbsArchivePlugin}.
 */
public class RbsArchivePluginTests extends OpenSearchTestCase {

    private final RbsArchivePlugin plugin = new RbsArchivePlugin();

    /**
     * Verifies that the plugin reports the strategy name "tar" — used for per-index opt-in.
     */
    public void testGetStrategyNameReturnsTar() {
        assertEquals("Strategy name must be 'tar'", "tar", plugin.getStrategyName());
    }

    /**
     * Verifies that the plugin provides a non-null {@link SegmentRemoteStoreStrategy}
     * and that it is a {@link TarSegmentRemoteStoreStrategy}.
     */
    public void testGetSegmentStrategyReturnsTarStrategy() {
        SegmentRemoteStoreStrategy strategy = plugin.getSegmentStrategy();
        assertNotNull("Plugin must provide a SegmentRemoteStoreStrategy", strategy);
        assertTrue("Plugin segment strategy must be TarSegmentRemoteStoreStrategy", strategy instanceof TarSegmentRemoteStoreStrategy);
    }

    /**
     * Verifies that the translog strategy returns null before {@code createComponents()} is called.
     * The strategy is wired lazily by the collector's {@code doStart()}.
     */
    public void testGetTranslogStrategyReturnsNullBeforeCreateComponents() {
        // Before createComponents(), tarTranslogStrategy is not yet initialized
        TranslogRemoteStoreStrategy strategy = plugin.getTranslogStrategy();
        assertNull("Plugin translog strategy must be null before createComponents() is called", strategy);
    }

    /**
     * Verifies that upload() with a null coordinator (simulating collector stopped or not yet started)
     * returns false and calls onUploadFailed instead of throwing NPE.
     */
    public void testNullCoordinatorUploadReturnsFalseAndCallsOnUploadFailed() throws Exception {
        // Strategy with null coordinator simulates collector-not-started or collector-stopped state
        TarTranslogRemoteStoreStrategy strategy = new TarTranslogRemoteStoreStrategy(null);

        java.util.concurrent.atomic.AtomicBoolean failed = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);

        org.opensearch.index.translog.transfer.listener.TranslogTransferListener listener =
            new org.opensearch.index.translog.transfer.listener.TranslogTransferListener() {
                public void onUploadComplete(org.opensearch.index.translog.transfer.TransferSnapshot snapshot) {
                    completed.set(true);
                }

                public void onUploadFailed(org.opensearch.index.translog.transfer.TransferSnapshot snapshot, Exception ex) {
                    failed.set(true);
                }
            };

        // Build a minimal snapshot via Mockito so all abstract methods are covered.
        // Use non-empty file sets so we get past the empty-files fast path.
        // TranslogFileSnapshot opens a FileChannel — must close it after the test.
        var tlogPath = createTempFile("translog", ".tlog");
        java.nio.file.Files.writeString(tlogPath, "dummy");
        var tlogSnap = new org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot(1L, 1L, tlogPath, null);
        try {
            var meta = new org.opensearch.index.translog.transfer.TranslogTransferMetadata(1L, 1L, 1L, 1);

            var snapshot = org.mockito.Mockito.mock(org.opensearch.index.translog.transfer.TransferSnapshot.class);
            org.mockito.Mockito.when(snapshot.getTranslogFileSnapshots()).thenReturn(java.util.Set.of(tlogSnap));
            org.mockito.Mockito.when(snapshot.getCheckpointFileSnapshots()).thenReturn(java.util.Set.of());
            org.mockito.Mockito.when(snapshot.getTranslogTransferMetadata()).thenReturn(meta);

            var shardId = new org.opensearch.core.index.shard.ShardId("idx", "uuid", 0);
            var basePath = new org.opensearch.common.blobstore.BlobPath().add("base");

            boolean result = strategy.upload(snapshot, listener, null, shardId, basePath);

            assertFalse("Upload with null coordinator must return false", result);
            assertTrue("onUploadFailed must be called when coordinator is null", failed.get());
            assertFalse("onUploadComplete must NOT be called when coordinator is null", completed.get());
        } finally {
            tlogSnap.close(); // releases the FileChannel to avoid LeakFS classMethod failure
        }
    }

    /**
     * Verifies that setCoordinator() properly wires a new coordinator into the strategy,
     * allowing upload to succeed after the collector starts.
     */
    public void testSetCoordinatorWiresNewCoordinator() throws Exception {
        TarTranslogRemoteStoreStrategy strategy = new TarTranslogRemoteStoreStrategy(null);
        assertNull("Coordinator should be null initially", strategy.getCoordinator());

        TranslogBatchCoordinator coordinator = new TranslogBatchCoordinator(
            "node-test",
            strategy,
            TimeValue.timeValueMillis(200),
            Integer.MAX_VALUE
        );
        try {
            strategy.setCoordinator(coordinator);
            assertEquals("Coordinator should be set after setCoordinator()", coordinator, strategy.getCoordinator());

            // Also verify clearing works
            strategy.setCoordinator(null);
            assertNull("Coordinator should be null after setCoordinator(null)", strategy.getCoordinator());
        } finally {
            // close() calls timerExecutor.shutdownNow(); wait for the thread to die to avoid
            // the randomized-testing "leaked thread" check failing the class-method pseudo-test.
            coordinator.close();
        }
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
