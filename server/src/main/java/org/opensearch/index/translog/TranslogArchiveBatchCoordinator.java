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

import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.time.Instant;
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
 * Node-scoped synchronous batch coordinator for translog archive uploads (the "shared car" model).
 * <p>
 * A single instance lives for the lifetime of the node (created by {@link TranslogArchiveCollector},
 * injected via {@link TranslogConfig}). All primary shards on the node — regardless of index —
 * submit their translog data here and share a single TAR upload per batch window.
 * <p>
 * Multiple shard sync threads call {@link #submitAndWait} to add their translog data to the current batch.
 * The batch dispatches when either:
 * <ul>
 *   <li>The number of pending shards reaches {@code archiveThreshold} (car full → depart early), or</li>
 *   <li>{@code archiveMaxWait} ms have elapsed since the <b>first</b> shard arrived (timer expires).</li>
 * </ul>
 * After each upload the wait clock resets — the next batch starts fresh on the next arriving shard.
 * All waiting threads are released only after the upload completes, preserving the
 * durability guarantee that the client's indexing response is not sent until data is in S3.
 * <p>
 * Bytes-based early dispatch ({@link #MAX_BATCH_BYTES}) is a safety bound against OOM.
 * <p>
 * <b>Path layout:</b> {@code repoBase/txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar}
 * <p>
 * <b>Lifecycle:</b> Created by {@link TranslogArchiveCollector} on node start, closed on node stop.
 * Not tied to any individual index — all archive-enabled indices share this instance.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class TranslogArchiveBatchCoordinator {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveBatchCoordinator.class);

    private static final int PIPE_BUFFER_BYTES = 256 * 1024;
    private static final int UPLOAD_RETRY_MAX_ATTEMPTS = 2;

    /**
     * Maximum total uncompressed bytes across all shard entries in one batch.
     * Prevents OOM when many large shards accumulate in a single batch window.
     */
    static final long MAX_BATCH_BYTES = 128L * 1024 * 1024;

    private volatile String nodeId;
    /** Archive base path — set lazily via {@link #initArchiveBasePath} on first shard submission. */
    private volatile BlobPath archiveBasePath;
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

    /**
     * Accumulated shard data for current batch.
     * Keyed by "{indexUUID}:{shardId}" to correctly distinguish same shard IDs across different indices.
     */
    private final Map<String, ShardArchiveData> pendingShards = new HashMap<>();
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
        /** Index UUID — carried here because the coordinator is node-scoped (not per-index). */
        private final String indexUUID;
        private final int shardId;
        private final long primaryTerm;
        private final long generation;
        private final long minTranslogGeneration;
        private final List<TarArchiveBuilder.ArchiveBuildEntry> entries;
        /** Lowest seqNo in this batch (local checkpoint at upload time). */
        private final long minSeqNo;
        /** Highest seqNo assigned in this batch. */
        private final long maxSeqNo;
        /** Last synced global checkpoint — used by GC scanner to decide when TAR is safe to delete. */
        private final long globalCheckpoint;

        public ShardArchiveData(
            String indexUUID,
            int shardId,
            long primaryTerm,
            long generation,
            long minTranslogGeneration,
            List<TarArchiveBuilder.ArchiveBuildEntry> entries
        ) {
            this(indexUUID, shardId, primaryTerm, generation, minTranslogGeneration, entries, -1L, -1L, -1L);
        }

        public ShardArchiveData(
            String indexUUID,
            int shardId,
            long primaryTerm,
            long generation,
            long minTranslogGeneration,
            List<TarArchiveBuilder.ArchiveBuildEntry> entries,
            long minSeqNo,
            long maxSeqNo,
            long globalCheckpoint
        ) {
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.minTranslogGeneration = minTranslogGeneration;
            this.entries = entries;
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.globalCheckpoint = globalCheckpoint;
        }

        public String getIndexUUID() {
            return indexUUID;
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

        public List<TarArchiveBuilder.ArchiveBuildEntry> getEntries() {
            return entries;
        }

        public long getMinSeqNo() {
            return minSeqNo;
        }

        public long getMaxSeqNo() {
            return maxSeqNo;
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        /** Unique key within a node-scoped batch: prevents collisions between same shardId on different indices. */
        public String batchKey() {
            return indexUUID + ":" + shardId;
        }
    }

    /**
     * @param nodeId             this node's ID (used in TAR blob names to avoid multi-node conflicts)
     * @param archiveBasePath    base blob path for the translog archive repo
     * @param pathHashAlgorithm  path hashing algorithm for blob routing
     * @param archiveMaxWait     maximum time to wait for additional shards before dispatching a batch
     * @param archiveThreshold   number of pending shards that triggers early dispatch
     */
    public TranslogArchiveBatchCoordinator(
        String nodeId,
        BlobPath archiveBasePath,
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm,
        TimeValue archiveMaxWait,
        int archiveThreshold
    ) {
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
            Thread t = new Thread(r, "translog-archive-batch-timer-node");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Stops the timer executor when this coordinator is unregistered.
     * Called from {@link #close()}.
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
            pendingShards.put(shardData.batchKey(), shardData);
            long shardBytes = shardData.getEntries().stream().mapToLong(TarArchiveBuilder.ArchiveBuildEntry::getSize).sum();
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
                    "First shard arrived (node-level batch), scheduled dispatch in {} ms (pending={})",
                    archiveMaxWaitMillis,
                    pendingShards.size()
                );
            }

            // Dispatch early if threshold or byte limit reached
            boolean thresholdReached = pendingShards.size() >= archiveThreshold;
            boolean byteLimitReached = pendingBatchBytes >= MAX_BATCH_BYTES;
            if ((thresholdReached || byteLimitReached) && !dispatching) {
                logger.debug(
                    "Dispatching early (node-level): shards={} threshold={} bytes={} byteLimit={}",
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
     * Sets the archive base path and node ID from the calling shard on first submission.
     * Called by {@link RemoteFsTranslog} before {@link #submitAndWait} so the coordinator
     * uses the correct base path and real node ID (avoiding clusterService.localNode() calls
     * which are forbidden on cluster-state applier threads).
     * Thread-safe: both fields are volatile and only set once (first-write-wins is fine since all
     * shards on the same node share the same repo base path and node ID).
     */
    public void initArchiveBasePath(BlobPath repoBasePath, String nodeId) {
        if (this.archiveBasePath == null) {
            this.archiveBasePath = repoBasePath;
        }
        if (this.nodeId == null || this.nodeId.equals("local")) {
            this.nodeId = nodeId;
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
        Map<String, ShardArchiveData> batch = new HashMap<>(pendingShards);
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
                logger.warn(
                    () -> new ParameterizedMessage("Archive batch upload failed (node-level, {} shards)", batch.size()),
                    e
                );
            } finally {
                // Set error before releasing latch so submitAndWait() sees it after await()
                dispatchError = uploadException;
                currentLatch.countDown();
            }
        }, "translog-archive-upload-node");
        uploadThread.setDaemon(true);
        uploadThread.start();
    }

    /**
     * Build and stream-upload a TAR archive for a node-level batch.
     * <p>
     * All TAR entries are streamed through a {@link PipedOutputStream} → {@link PipedInputStream}
     * pipe directly into the S3 upload — no full file content is held in memory simultaneously.
     * GC summary entries are embedded per shard, using the per-shard {@code indexUUID} carried
     * in {@link ShardArchiveData} (required since this coordinator is node-scoped, not per-index).
     */
    private void uploadBatch(Map<String, ShardArchiveData> batch, TransferService transferService) throws IOException {
        // Collect all TAR entries and GC summary entries from all shards in this batch.
        List<TarArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        List<TarArchiveBuilder.GcShardEntry> gcEntries = new ArrayList<>(batch.size());
        for (ShardArchiveData data : batch.values()) {
            allEntries.addAll(data.getEntries());
            // Embed GC summary in the TAR so the GC scanner can determine when it is safe to delete.
            // indexUUID comes from ShardArchiveData (not a class-level field) because this coordinator
            // is node-scoped and serves shards from multiple indices simultaneously.
            if (data.getMinSeqNo() >= 0) {
                gcEntries.add(new TarArchiveBuilder.GcShardEntry(
                    data.getIndexUUID(),  // per-shard indexUUID — correct for multi-index batches
                    data.getShardId(),
                    data.getMinSeqNo(),
                    data.getMaxSeqNo(),
                    data.getGlobalCheckpoint()
                ));
            }
        }

        if (allEntries.isEmpty()) {
            logger.trace("Node-level batch had no TAR entries to upload (all shards empty)");
            return;
        }

        // Compute TAR layout (pure arithmetic from sizes — no file content reads)
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries, gcEntries);
        long contentLength = layout.getTotalSize();

        // Hierarchical path: txlog/{yyyyMMdd}/{HHmm}/
        Instant uploadInstant = Instant.now();
        BlobPath archivePath = TranslogArchivePathHelper.tarBlobDir(archiveBasePath, uploadInstant);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadInstant, nodeId);

        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            // Pipe: TAR builder thread writes → upload thread reads and streams to S3.
            // The pipe buffer (256 KB) is the only in-memory buffer — peak memory is bounded.
            try (
                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)
            ) {
                // Use a volatile field instead of AtomicReference — CountDownLatch provides
                // the happens-before guarantee, making AtomicReference unnecessary overhead.
                final IOException[] uploadError = { null };
                final java.util.concurrent.CountDownLatch uploadLatch = new java.util.concurrent.CountDownLatch(1);

                // Reuse a raw Thread (not a new ExecutorService per attempt) to avoid thread pool
                // creation overhead and the associated resource leak on timeout/exception paths.
                Thread uploadThread = new Thread(() -> {
                    try {
                        transferService.uploadBlobStream(pis, contentLength, archivePath, blobName, WritePriority.HIGH, null);
                    } catch (IOException e) {
                        uploadError[0] = e;
                    } finally {
                        uploadLatch.countDown(); // happens-before: uploadError[0] visible after await()
                    }
                }, "translog-archive-upload-attempt-" + attempt);
                uploadThread.setDaemon(true);
                uploadThread.start();

                // Stream TAR bytes into the pipe — builder blocks when the pipe buffer is full,
                // creating natural backpressure. No full-batch buffering occurs.
                TarArchiveBuilder.build(pos, layout, allEntries);
                pos.close();

                if (!uploadLatch.await(uploadTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    uploadThread.interrupt();
                    throw new IOException(
                        String.format("Archive upload timed out after %d ms (shards=%d)", uploadTimeoutMillis, batch.size())
                    );
                }
                if (uploadError[0] != null) {
                    throw uploadError[0];
                }

                logger.debug(
                    "Archive uploaded path={} blob={} shards={} entries={} size={}",
                    archivePath.buildAsString(),
                    blobName,
                    batch.size(),
                    allEntries.size(),
                    contentLength
                );
                return; // success
            } catch (IOException e) {
                lastFailure = e;
                final int attemptNum = attempt + 1;
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Archive upload attempt {} of {} failed (shards={})",
                        attemptNum, UPLOAD_RETRY_MAX_ATTEMPTS, batch.size()
                    ),
                    e
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during archive upload (shards=" + batch.size() + ")", e);
            }
        }
        throw new IOException(
            "Archive upload failed after " + UPLOAD_RETRY_MAX_ATTEMPTS + " attempts (shards=" + batch.size() + ")",
            lastFailure
        );
    }

    /** Returns this node's ID. Used by tests to verify node-scoped coordinator identity. */
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
