/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;
import org.opensearch.index.translog.transfer.archive.ArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-index synchronous batch coordinator for translog archive uploads (the "shared car" model).
 * <p>
 * Multiple shard sync threads call {@link #submitAndWait} to add their translog data to the current batch.
 * The batch dispatches when either:
 * <ul>
 *   <li>The number of pending shards reaches {@code archiveThreshold} (car seats full → depart early), or</li>
 *   <li>{@code archiveMaxWait} has elapsed since the <b>first</b> shard arrived (waiting too long → depart).</li>
 * </ul>
 * After each upload the wait clock resets — the next batch starts fresh on the next arriving shard.
 * All waiting threads are released only after the upload completes, preserving the
 * durability guarantee that the client's indexing response is not sent until data is remotely persisted.
 * <p>
 * Bytes-based early dispatch ({@link #MAX_BATCH_BYTES}) is retained as a safety bound against OOM.
 * <p>
 * <b>Path layout:</b> {@code repoBase/translog/data/{hashTypeIndex}/{hashNodeId}/{timestamp}.zip}
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class TranslogArchiveBatchCoordinator {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveBatchCoordinator.class);

    /** Global coordinator registry keyed by indexUUID. Looked up by RemoteFsTranslog at upload time. */
    private static final java.util.concurrent.ConcurrentHashMap<String, TranslogArchiveBatchCoordinator> COORDINATORS =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Register a coordinator for an index. */
    public static void register(TranslogArchiveBatchCoordinator coordinator) {
        COORDINATORS.put(coordinator.getIndexUUID(), coordinator);
    }

    /** Unregister the coordinator for an index. */
    public static void unregister(String indexUUID) {
        TranslogArchiveBatchCoordinator coordinator = COORDINATORS.remove(indexUUID);
        if (coordinator != null) {
            coordinator.close();
        }
    }



    /** Look up the coordinator for an index. Returns null if not registered. */
    public static TranslogArchiveBatchCoordinator get(String indexUUID) {
        return COORDINATORS.get(indexUUID);
    }

    private static final int PIPE_BUFFER_BYTES = 256 * 1024;
    private static final int UPLOAD_RETRY_MAX_ATTEMPTS = 2;

    /**
     * Maximum total uncompressed bytes across all shard entries in one batch.
     * Prevents OOM when many large shards accumulate in a single batch window.
     */
    static final long MAX_BATCH_BYTES = 128L * 1024 * 1024;

    private final String indexUUID;
    private final String nodeId;
    private final BlobPath archiveBasePath;
    private final RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm;
    private final long archiveMaxWaitMillis;
    private final int archiveThreshold;
    private final long uploadTimeoutMillis;

    // Current batch state — guarded by lock
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * Condition used to wake up waiting shards when: (a) the first-arrival timer fires,
     * (b) the threshold is reached, or (c) the coordinator closes.
     */
    private final Condition batchReady = lock.newCondition();
    private final Condition batchComplete = lock.newCondition();

    /** Accumulated shard data for current batch. */
    private final Map<Integer, ShardArchiveData> pendingShards = new HashMap<>();
    /** Running total of entry bytes in the current batch. */
    private long pendingBatchBytes;
    /** Latch released when the current batch upload completes (or fails). */
    private volatile CountDownLatch dispatchLatch;
    /** Error from the most recent dispatch, if any. */
    private volatile IOException dispatchError;
    /** Whether a dispatch is currently in progress. */
    private boolean dispatching;
    /** Whether this coordinator has been closed. */
    private volatile boolean closed;
    /**
     * Timer executor used to enforce archiveMaxWait after the first shard arrives.
     * Replaced per batch — null when no batch is in progress (no shards pending).
     */
    private final ScheduledExecutorService timerExecutor;
    /**
     * Whether a per-batch timer task is currently scheduled.
     * Reset to false on each dispatch so the next batch starts a fresh timer.
     */
    private boolean firstArrivalTimerScheduled;

    /**
     * Data submitted by one shard for inclusion in the batch ZIP.
     */
    @ExperimentalApi
    public static final class ShardArchiveData {
        private final int shardId;
        private final long primaryTerm;
        private final long generation;
        private final long minTranslogGeneration;
        private final List<ArchiveBuilder.ArchiveBuildEntry> entries;

        public ShardArchiveData(
            int shardId,
            long primaryTerm,
            long generation,
            long minTranslogGeneration,
            List<ArchiveBuilder.ArchiveBuildEntry> entries
        ) {
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.minTranslogGeneration = minTranslogGeneration;
            this.entries = entries;
        }

        public int getShardId() {
            return shardId;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public long getGeneration() {
            return generation;
        }

        public long getMinTranslogGeneration() {
            return minTranslogGeneration;
        }

        public List<ArchiveBuilder.ArchiveBuildEntry> getEntries() {
            return entries;
        }
    }

    public TranslogArchiveBatchCoordinator(
        String indexUUID,
        String nodeId,
        BlobPath archiveBasePath,
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm,
        TimeValue archiveMaxWait,
        int archiveThreshold
    ) {
        this.indexUUID = indexUUID;
        this.nodeId = nodeId;
        this.archiveBasePath = archiveBasePath;
        this.pathHashAlgorithm = pathHashAlgorithm;
        this.archiveMaxWaitMillis = archiveMaxWait.millis();
        this.archiveThreshold = archiveThreshold;
        this.uploadTimeoutMillis = Math.max(archiveMaxWait.millis() * 10, 30_000);
        this.dispatchLatch = new CountDownLatch(1);
        this.closed = false;
        this.firstArrivalTimerScheduled = false;

        // Shared single-thread scheduler; fires once per batch when the first shard arrives.
        this.timerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "translog-archive-batch-timer-" + indexUUID);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Stops the timer executor when this coordinator is unregistered.
     * Called from {@link #unregister(String)}.
     * Signals any waiting threads and shuts down the timer cleanly.
     */
    public void close() {
        this.closed = true;
        // Shut down the timer executor immediately — no new scheduled tasks will fire.
        this.timerExecutor.shutdownNow();
        // Signal any submitAndWait() callers that are waiting on batchReady so they can exit.
        lock.lock();
        try {
            batchReady.signalAll();
        } finally {
            lock.unlock();
        }
        // Wait briefly for the executor thread to fully stop.
        try {
            this.timerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Submit shard data to the current batch and block until the batch ZIP is uploaded.
     * Called by the shard's sync thread. Returns only after the ZIP is durably persisted in remote store.
     *
     * <p>Dispatch triggers when either:
     * <ul>
     *   <li>Pending shard count reaches {@code archiveThreshold} (seats full → depart immediately), or</li>
     *   <li>The first-arrival timer expires after {@code archiveMaxWait} ms, or</li>
     *   <li>Total batch bytes reach {@link #MAX_BATCH_BYTES}.</li>
     * </ul>
     *
     * @param shardData the shard's translog data for this batch
     * @param transferService the transfer service to use for upload
     * @throws IOException if the upload fails
     */
    public void submitAndWait(ShardArchiveData shardData, TransferService transferService) throws IOException {
        CountDownLatch myLatch;

        lock.lock();
        try {
            pendingShards.put(shardData.getShardId(), shardData);
            long shardBytes = shardData.getEntries().stream().mapToLong(ArchiveBuilder.ArchiveBuildEntry::getSize).sum();
            pendingBatchBytes += shardBytes;
            myLatch = dispatchLatch;

            // Schedule first-arrival timer on the very first shard in this batch.
            // Subsequent shards in the same batch reuse the already-running timer.
            if (!firstArrivalTimerScheduled && !dispatching) {
                firstArrivalTimerScheduled = true;
                timerExecutor.schedule(() -> {
                    if (closed) return;
                    lock.lock();
                    try {
                        batchReady.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }, archiveMaxWaitMillis, TimeUnit.MILLISECONDS);
                logger.debug(
                    "First shard arrived for index {}, scheduled batch dispatch in {} ms",
                    indexUUID,
                    archiveMaxWaitMillis
                );
            }

            // Dispatch early if threshold or byte limit reached
            boolean thresholdReached = pendingShards.size() >= archiveThreshold;
            boolean byteLimitReached = pendingBatchBytes >= MAX_BATCH_BYTES;
            if ((thresholdReached || byteLimitReached) && !dispatching) {
                logger.debug(
                    "Dispatching early for index {}: shards={} threshold={} bytes={} byteLimit={}",
                    indexUUID,
                    pendingShards.size(),
                    archiveThreshold,
                    pendingBatchBytes,
                    byteLimitReached
                );
                dispatchUnderLock(transferService);
            } else {
                // Wait for the first-arrival timer or an early dispatch signal
                try {
                    batchReady.await(archiveMaxWaitMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for batch dispatch", e);
                }
                // Dispatch if we are the first to wake and batch not yet dispatched
                if (myLatch == dispatchLatch && !dispatching && !pendingShards.isEmpty()) {
                    dispatchUnderLock(transferService);
                }
            }
        } finally {
            lock.unlock();
        }

        // Wait for the background upload thread to complete
        try {
            if (!myLatch.await(uploadTimeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IOException("Timed out waiting for archive batch upload");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for batch upload", e);
        }

        IOException error = dispatchError;
        if (error != null) {
            throw new IOException("Archive batch upload failed", error);
        }
    }

    /**
     * No-op: kept for API compatibility. Dispatch is now driven by first-arrival timer and threshold.
     */
    public void timerDispatch(TransferService transferService) {
        // no-op — dispatch is now triggered inside submitAndWait via first-arrival timer
    }

    /**
     * Must be called under lock. Hands the current batch off to a background upload thread and
     * resets the coordinator state immediately so that the next batch can start accumulating
     * without waiting for the upload to complete (pipelining).
     * <p>
     * Resets {@code firstArrivalTimerScheduled} so the next batch schedules a fresh timer
     * when its first shard arrives.
     * <p>
     * Callers still wait for the upload to finish via {@code dispatchLatch.await()} in
     * {@link #submitAndWait}, preserving {@code durability=REQUEST} correctness.
     */
    private void dispatchUnderLock(TransferService transferService) {
        dispatching = true;
        Map<Integer, ShardArchiveData> batch = new HashMap<>(pendingShards);
        CountDownLatch currentLatch = dispatchLatch;

        // Reset state for next batch immediately — callers can start accumulating while upload runs
        pendingShards.clear();
        pendingBatchBytes = 0;
        dispatchError = null;
        dispatchLatch = new CountDownLatch(1);
        dispatching = false;
        firstArrivalTimerScheduled = false;  // next batch will schedule its own first-arrival timer
        batchComplete.signalAll();

        // Hand off upload to a background thread — lock is NOT held during upload.
        // dispatchError is set BEFORE countDown so submitAndWait() sees it after latch.await().
        Thread uploadThread = new Thread(() -> {
            IOException uploadException = null;
            try {
                uploadBatch(batch, transferService);
            } catch (IOException e) {
                uploadException = e;
                logger.warn(() -> new ParameterizedMessage("Archive batch upload failed for index {}", indexUUID), e);
            } finally {
                // Set error before releasing latch so submitAndWait() sees it after await()
                dispatchError = uploadException;
                currentLatch.countDown();
            }
        }, "translog-archive-upload-" + indexUUID);
        uploadThread.setDaemon(true);
        uploadThread.start();
    }

    /**
     * Build and upload the ZIP archive for a batch.
     */
    private void uploadBatch(Map<Integer, ShardArchiveData> batch, TransferService transferService) throws IOException {
        // Collect all entries
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        for (ShardArchiveData data : batch.values()) {
            allEntries.addAll(data.getEntries());
        }

        if (allEntries.isEmpty()) {
            logger.trace("No entries to upload for index {}", indexUUID);
            return;
        }

        // Compute path: translog/data/{hashTypeIndex}/{hashNodeId}/
        String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(indexUUID, pathHashAlgorithm);
        String hashNodeId = TranslogArchivePathHelper.hashNodeId(nodeId, pathHashAlgorithm);
        BlobPath archivePath = archiveBasePath.add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

        // Compute TAR layout (deterministic from file sizes alone — no content reads needed)
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries);
        long contentLength = layout.getTotalSize();

        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            String blobName = TranslogArchivePathHelper.tarBlobNameFromCurrentTime();
            try (PipedOutputStream pos = new PipedOutputStream(); PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)) {
                AtomicReference<IOException> uploadError = new AtomicReference<>();
                java.util.concurrent.CountDownLatch uploadLatch = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "archive-upload-" + indexUUID);
                    t.setDaemon(true);
                    return t;
                });
                executor.submit(() -> {
                    try {
                        transferService.uploadBlobStream(pis, contentLength, archivePath, blobName, WritePriority.HIGH, null);
                    } catch (IOException e) {
                        uploadError.set(e);
                    } finally {
                        uploadLatch.countDown();
                    }
                });

                TarArchiveBuilder.build(pos, layout, allEntries);
                pos.close();

                if (!uploadLatch.await(uploadTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                    throw new IOException("Archive upload timed out for index " + indexUUID);
                }
                executor.shutdown();
                if (uploadError.get() != null) {
                    throw uploadError.get();
                }

                logger.debug(
                    "Archive uploaded path={} blob={} entries={} size={}",
                    archivePath.buildAsString(),
                    blobName,
                    allEntries.size(),
                    contentLength
                );
                return; // success
            } catch (IOException e) {
                lastFailure = e;
                final int attemptNum = attempt + 1;
                logger.warn(() -> new ParameterizedMessage("Archive upload attempt {} failed for index {}", attemptNum, indexUUID), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during archive upload for index " + indexUUID, e);
            }
        }
        throw new IOException("Archive upload failed after " + UPLOAD_RETRY_MAX_ATTEMPTS + " attempts for index " + indexUUID, lastFailure);
    }

    /** Returns the index UUID this coordinator manages. */
    public String getIndexUUID() {
        return indexUUID;
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
