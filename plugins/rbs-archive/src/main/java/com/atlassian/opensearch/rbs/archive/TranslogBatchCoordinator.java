/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.translog.transfer.TransferService;
// TranslogShardBatch is in same plugin package

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Node-scoped synchronous batch coordinator for translog archive uploads (the "shared taxi" model).
 *
 * <p>A single instance lives for the lifetime of the node (created by {@link TranslogBatchCollector},
 * injected into each shard's {@code RemoteFsTranslog} via {@code TranslogRemoteStoreStrategy}).
 * All primary shards on the node — regardless of index — submit their translog data here and share
 * a single archive upload per batch window.
 *
 * <p>Multiple shard sync threads call {@link #submitAndWait} to add their translog data to the
 * current batch. The batch dispatches when either:
 * <ul>
 *   <li>The number of pending shards reaches {@code archiveThreshold} (taxi full → depart early), or</li>
 *   <li>{@code archiveMaxWait} ms have elapsed since the <b>first</b> shard arrived (timer expires).</li>
 * </ul>
 * After each upload the wait clock resets — the next batch starts fresh on the next arriving shard.
 * All waiting threads are released only after the upload completes (or fails), preserving the
 * durability guarantee: the client's indexing response is not sent until data is in S3.
 *
 * <p>Bytes-based early dispatch ({@link #MAX_BATCH_BYTES}) is a safety bound against OOM.
 *
 * <p>Archive building and upload is fully delegated to the {@code TarTranslogRemoteStoreStrategy#uploadBatch}
 * SPI method — this coordinator contains <b>no TAR or blob format knowledge</b>.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class TranslogBatchCoordinator {

    private static final Logger logger = LogManager.getLogger(TranslogBatchCoordinator.class);

    private static final int UPLOAD_RETRY_MAX_ATTEMPTS = 3;

    /**
     * Maximum total uncompressed bytes across all shard entries in one batch.
     * Prevents OOM when many large shards accumulate in a single batch window.
     */
    static final long MAX_BATCH_BYTES = 128L * 1024 * 1024;

    private final String nodeId;
    /** Archive base path — repository base; the strategy will compute the full archive path from this. */
    /** Package-private for test injection and verification. */
    volatile TarTranslogRemoteStoreStrategy strategy;
    private final long archiveMaxWaitMillis;
    private final int archiveThreshold;

    // ── Current batch state ── guarded by lock ────────────────────────────────
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * Wakes up waiting shards when: (a) first-arrival timer fires, (b) threshold reached,
     * or (c) coordinator closes.
     */
    private final Condition batchReady = lock.newCondition();
    private final Condition batchComplete = lock.newCondition();

    /**
     * Accumulated shard data for the current batch.
     * Keyed by "{indexUUID}:{shardId}" to distinguish same shard-ID on different indices.
     */
    private final Map<String, TranslogShardBatch> pendingShards = new HashMap<>();
    /** Running total of file bytes in the current batch. */
    private long pendingBatchBytes;
    /**
     * The {@link TransferService} captured from the first shard that joins this batch.
     * All shards in a batch share the same repository, so any shard's service is valid for the upload.
     */
    private TransferService batchTransferService;
    /**
     * The repository base path captured from the first shard in this batch.
     * All shards in the same node share the same translog repository, so one is sufficient.
     */
    private BlobPath batchBasePath;
    /** Error from the most recent dispatch, if any. */
    private volatile IOException dispatchError;
    /** Whether a dispatch is currently in progress. */
    private boolean dispatching;
    /** Whether this coordinator has been closed. */
    private volatile boolean closed;
    /**
     * Timer executor that fires once per batch when the first shard arrives.
     * Replaced per batch — null when no batch is in progress.
     */
    private final ScheduledExecutorService timerExecutor;
    /** Whether a per-batch timer task is currently scheduled. Reset to {@code false} on each dispatch. */
    private boolean firstArrivalTimerScheduled;

    /**
     * @param nodeId              this node's ID (used in archive blob names to avoid multi-node conflicts)
     * @param strategy            the plugin-provided strategy that builds and uploads the archive
     * @param archiveMaxWait      maximum wait for additional shards before dispatching a batch
     * @param archiveThreshold    number of pending shards that triggers early dispatch
     */
    public TranslogBatchCoordinator(
        String nodeId,
        TarTranslogRemoteStoreStrategy strategy,
        TimeValue archiveMaxWait,
        int archiveThreshold
    ) {
        this.nodeId = nodeId;
        this.strategy = strategy;
        this.archiveMaxWaitMillis = archiveMaxWait.millis();
        this.archiveThreshold = archiveThreshold;
        this.pendingBatchBytes = 0;
        this.closed = false;
        this.firstArrivalTimerScheduled = false;

        // Single-threaded scheduler: fires the "max wait" timeout for the current batch.
        this.timerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "translog-archive-batch-timer-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Submits this shard's translog data to the current batch and blocks until the batch upload
     * completes (success or failure).
     *
     * <p>If this shard is a duplicate within the same batch window (same index+shard), the newer
     * submission overwrites the older one — only the most recent snapshot is included.
     *
     * @param shardBatch the translog files for one shard ready to be archived
     * @throws IOException if the batch upload fails (propagated to the calling shard's sync thread)
     */
    /** Sets or replaces the strategy after construction. Used to break the circular reference
     *  between coordinator and strategy during plugin init. */
    public void setStrategy(TarTranslogRemoteStoreStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Returns the {@link TransferService} captured from the most recent batch dispatch,
     * or {@code null} if no batch has been dispatched yet.
     * Used by {@link TarTranslogRemoteStoreStrategy#runArchiveGc} to list/delete blobs.
     */
    public TransferService getLastKnownTransferService() {
        return batchTransferService;
    }

    public void submitAndWait(TranslogShardBatch shardBatch, TransferService transferService, BlobPath repositoryBasePath) throws IOException {
        if (closed) {
            throw new IOException("TranslogBatchCoordinator is closed");
        }

        String key = shardBatch.getIndexUUID() + ":" + shardBatch.getShardId();

        lock.lock();
        try {
            if (closed) {
                throw new IOException("TranslogBatchCoordinator is closed");
            }

            // If this shard was already pending (e.g., two syncs before a batch dispatches),
            // subtract the old byte count before overwriting.
            TranslogShardBatch previous = pendingShards.get(key);
            if (previous != null) {
                pendingBatchBytes -= previous.totalBytes();
            }
            pendingShards.put(key, shardBatch);
            pendingBatchBytes += shardBatch.totalBytes();

            // Capture the transfer service from the first arriving shard (all share the same repo).
            if (batchTransferService == null) {
                batchTransferService = transferService;
            }
            if (batchBasePath == null) {
                batchBasePath = repositoryBasePath;
            }

            // Capture the latch BEFORE we signal; all waiters will wait on the same latch
            // so they all wake up when the current dispatch completes.
            dispatchError = null;

            // Schedule the first-arrival timer on the first shard in this batch.
            if (!firstArrivalTimerScheduled && !dispatching) {
                firstArrivalTimerScheduled = true;
                timerExecutor.schedule(this::onTimerExpiry, archiveMaxWaitMillis, TimeUnit.MILLISECONDS);
                logger.debug("Archive batch timer started ({} ms), first shard: {}:{}", archiveMaxWaitMillis,
                    shardBatch.getIndexUUID(), shardBatch.getShardId());
            }

            // Early dispatch conditions: threshold reached OR byte cap exceeded
            boolean thresholdReached = pendingShards.size() >= archiveThreshold;
            boolean bytesCapReached = pendingBatchBytes >= MAX_BATCH_BYTES;

            if ((thresholdReached || bytesCapReached) && !dispatching) {
                logger.debug("Archive batch dispatch triggered: threshold={} byteCap={} shards={}",
                    thresholdReached, bytesCapReached, pendingShards.size());
                dispatchBatch();
            }

            // Wait until our batch's upload completes (dispatching flag cleared + batchComplete signal)
            while (pendingShards.containsKey(key) || dispatching) {
                batchComplete.await();
                if (closed) {
                    throw new IOException("TranslogBatchCoordinator closed while waiting for batch");
                }
            }

            if (dispatchError != null) {
                throw new IOException("Archive batch upload failed for shard " + key, dispatchError);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for archive batch upload (shard=" + key + ")", e);
        } finally {
            lock.unlock();
        }
    }

    /** Called by the timer when {@code archiveMaxWait} elapses after the first shard arrived. */
    private void onTimerExpiry() {
        lock.lock();
        try {
            if (closed || pendingShards.isEmpty() || dispatching) {
                return;
            }
            logger.debug("Archive batch timer expired, dispatching {} pending shard(s)", pendingShards.size());
            dispatchBatch();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drains {@link #pendingShards} and dispatches the batch upload synchronously while holding
     * the lock — other submitters wait on {@link #batchComplete} and are released when we signal.
     *
     * <p>MUST be called with {@link #lock} held.
     */
    private void dispatchBatch() {
        // Drain the current batch
        List<TranslogShardBatch> batch = new ArrayList<>(pendingShards.values());
        pendingShards.clear();
        pendingBatchBytes = 0;
        dispatching = true;
        firstArrivalTimerScheduled = false; // next batch gets a fresh timer

        // Capture and reset the transfer service for this batch
        TransferService dispatchTransferService = batchTransferService;
        batchTransferService = null; // next batch will get a fresh service reference
        BlobPath dispatchBasePath = batchBasePath;
        batchBasePath = null;

        // Release the lock while uploading — other shards can still call submitAndWait and
        // will see dispatching=true, so they'll join and wait on batchComplete.
        lock.unlock();
        IOException uploadError = null;
        try {
            uploadBatchWithRetry(batch, dispatchBasePath, dispatchTransferService);
        } catch (IOException e) {
            uploadError = e;
            logger.warn(() -> new ParameterizedMessage("Archive batch upload failed ({} shards)", batch.size()), e);
        } finally {
            lock.lock();
            dispatching = false;
            dispatchError = uploadError;
            batchComplete.signalAll();
        }
    }

    /**
     * Delegates the actual archive building and upload to {@code TarTranslogRemoteStoreStrategy#uploadBatch}.
     * Retries up to {@link #UPLOAD_RETRY_MAX_ATTEMPTS} times on failure.
     */
    private void uploadBatchWithRetry(List<TranslogShardBatch> batch, BlobPath basePath, TransferService transferService) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            try {
                strategy.uploadBatch(batch, basePath, transferService);
                logger.debug("Archive batch uploaded: shards={}", batch.size());
                return; // success
            } catch (IOException e) {
                lastFailure = e;
                final int attemptNum = attempt + 1;
                logger.warn(
                    () -> new ParameterizedMessage("Archive upload attempt {} of {} failed (shards={})",
                        attemptNum, UPLOAD_RETRY_MAX_ATTEMPTS, batch.size()),
                    e
                );
            }
        }
        throw new IOException(
            "Archive upload failed after " + UPLOAD_RETRY_MAX_ATTEMPTS + " attempts (shards=" + batch.size() + ")",
            lastFailure
        );
    }

    /**
     * Stops this coordinator. Signals all waiting shard threads so they can exit and
     * shuts down the timer executor cleanly.
     */
    public void close() {
        closed = true;
        timerExecutor.shutdownNow();
        lock.lock();
        try {
            batchReady.signalAll();
            batchComplete.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Returns this node's ID. */
    public String getNodeId() {
        return nodeId;
    }

    /** Returns current number of pending shards (for testing). */
    int getPendingShardCount() {
        lock.lock();
        try {
            return pendingShards.size();
        } finally {
            lock.unlock();
        }
    }
}
