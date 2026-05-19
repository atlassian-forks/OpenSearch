/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Duration;

/**
 * Node-singleton lifecycle component for translog archive upload and GC.
 *
 * <p>Created by {@link org.opensearch.plugins.Plugin#createComponents} when the RBS archive
 * plugin is loaded. Owns the {@link TranslogBatchCoordinator} and registers itself as a
 * cluster-manager listener to run archive retention GC only on the elected cluster-manager.
 *
 * <p>This class contains <b>no TAR or archive format knowledge</b> — all format-specific
 * work is delegated to {@code TranslogRemoteStoreStrategy}:
 * <ul>
 *   <li>{@code TarTranslogRemoteStoreStrategy#uploadBatch} — called by the coordinator per batch.</li>
 *   <li>{@code TarTranslogRemoteStoreStrategy#runArchiveGc} — called by the GC task on the
 *       cluster-manager to delete expired archive blobs.</li>
 * </ul>
 *
 */
final class TranslogBatchCollector extends AbstractLifecycleComponent implements LocalNodeClusterManagerListener {

    private static final Logger logger = LogManager.getLogger(TranslogBatchCollector.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TarTranslogRemoteStoreStrategy strategy;
    private final TimeValue gcInterval;
    private final Duration retentionAge;
    /** Checkpoint-aware GC scanner — lazily initialized when TransferService and basePath become available. */
    private volatile TranslogArchiveGcScanner gcScanner;
    private final String nodeId;

    private volatile TranslogBatchCoordinator coordinator;
    private volatile Scheduler.Cancellable gcTask;

    // Settings
    private final TimeValue archiveMaxWait;
    private final int archiveThreshold;

    public TranslogBatchCollector(
        String nodeId,
        ClusterService clusterService,
        ThreadPool threadPool,
        TarTranslogRemoteStoreStrategy strategy,
        TimeValue archiveMaxWait,
        int archiveThreshold,
        TimeValue gcInterval,
        Duration retentionAge
    ) {
        this.nodeId = nodeId;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.strategy = strategy;
        this.archiveMaxWait = archiveMaxWait;
        this.archiveThreshold = archiveThreshold;
        this.gcInterval = gcInterval;
        this.retentionAge = retentionAge;
    }

    @Override
    protected void doStart() {
        coordinator = new TranslogBatchCoordinator(
            nodeId,
            strategy,
            archiveMaxWait,
            archiveThreshold
        );
        // Register as cluster-manager listener to start/stop GC task on leadership changes
        clusterService.addLocalNodeClusterManagerListener(this);
        logger.info("TranslogBatchCollector started (maxWait={}, threshold={})", archiveMaxWait, archiveThreshold);
    }

    @Override
    protected void doStop() {
        // LocalNodeClusterManagerListener is automatically removed when the lifecycle component closes
        cancelGcTask();
        TranslogBatchCoordinator c = coordinator;
        if (c != null) {
            c.close();
            coordinator = null;
        }
        logger.info("TranslogBatchCollector stopped");
    }

    @Override
    protected void doClose() {
        // Nothing beyond doStop
    }

    // ── LocalNodeClusterManagerListener ──────────────────────────────────────

    @Override
    public void onClusterManager() {
        logger.info("TranslogBatchCollector: became cluster-manager, starting retention GC (interval={})", gcInterval);
        gcTask = threadPool.scheduleWithFixedDelay(this::runGcSafe, gcInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    public void offClusterManager() {
        logger.info("TranslogBatchCollector: no longer cluster-manager, stopping retention GC");
        cancelGcTask();
    }

    private void cancelGcTask() {
        Scheduler.Cancellable t = gcTask;
        if (t != null) {
            t.cancel();
            gcTask = null;
        }
    }

    private synchronized void initGcScannerIfNeeded() {
        if (gcScanner != null) return;
        TransferService ts = coordinator != null ? coordinator.getLastKnownTransferService() : null;
        BlobPath basePath = strategy.getLastKnownBasePath();
        if (ts == null || basePath == null) {
            logger.debug("TranslogBatchCollector GC: no TransferService/basePath available yet, skipping scanner init");
            return;
        }
        gcScanner = new TranslogArchiveGcScanner(ts, basePath);
        try {
            gcScanner.loadFromPersisted();
        } catch (Exception e) {
            logger.warn("TranslogBatchCollector GC scanner: failed to load persisted state: {}", e.getMessage());
        }
        logger.info("TranslogBatchCollector: GC scanner initialised (basePath={})", basePath.buildAsString());
    }

    private void runGcSafe() {
        try {
            initGcScannerIfNeeded();
            TranslogArchiveGcScanner scanner = gcScanner;
            if (scanner == null) {
                // No uploads yet — nothing to GC
                return;
            }
            // Phase 1: scan for new minute-dirs and build .idx files
            scanner.scan(java.time.Instant.now());

            // Phase 2: delete expired minute-dirs (both checkpoint-aware and timestamp fallback)
            strategy.runArchiveGc(strategy.getLastKnownBasePath(), retentionAge);
        } catch (Exception e) {
            logger.warn("Translog archive GC failed", e);
        }
    }

    // ── Coordinator access ────────────────────────────────────────────────────

    /**
     * Returns the active coordinator, or {@code null} if the collector is stopped.
     * Used by {@code TarTranslogRemoteStoreStrategy} to submit shard data to the batch.
     */
    TranslogBatchCoordinator getCoordinator() {
        return coordinator;
    }
}
