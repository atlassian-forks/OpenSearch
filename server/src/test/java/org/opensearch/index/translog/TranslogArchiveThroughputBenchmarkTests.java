/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Local throughput benchmark for translog archive upload.
 *
 * Simulates the genLock contention pattern:
 *   - N "shards" each protected by a ReentrantReadWriteLock
 *   - "Indexing" threads hold readLock briefly per operation
 *   - "Archive" thread acquires ALL writeLocks, builds TAR/ZIP, "uploads" (Thread.sleep),
 *     then releases ALL locks
 *
 * Run with:
 *   cd .tmp/opensearch-rbs
 *   ./gradlew :server:test \
 *     --tests "org.opensearch.index.translog.TranslogArchiveThroughputBenchmark" \
 *     --info -x javadoc
 */
package org.opensearch.index.translog;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Throughput benchmark: measures indexing docs/sec under translog archive upload lock contention.
 * <p>
 * Not a correctness test — asserts nothing, just prints results to stdout.
 */
public class TranslogArchiveThroughputBenchmarkTests extends OpenSearchTestCase {

    // ─── Benchmark parameters ──────────────────────────────────────────────

    /** Duration of each benchmark run in milliseconds */
    private static final long RUN_DURATION_MS = 10_000;

    /** Translog archive flush interval (ms) — matches cluster setting */
    private static final long BUFFER_INTERVAL_MS = 650;

    /** Simulated S3 upload latency (ms) — vary to test impact */
    private static final int[] FAKE_UPLOAD_LATENCIES_MS = { 20, 50, 100 };

    /** Number of primary shards per node — matches benchmark cluster config */
    private static final int[] SHARD_COUNTS = { 1, 10, 20 };

    /** Number of indexing threads per shard */
    private static final int INDEXING_THREADS_PER_SHARD = 1;

    /** Time each indexing thread holds the readLock (µs) — simulates translog write */
    private static final int INDEX_LOCK_HOLD_US = 100;  // 100µs = typical translog write

    // ───────────────────────────────────────────────────────────────────────

    public void testThroughputBenchmark() throws InterruptedException {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println(  "║     Translog Archive Throughput Benchmark (Lock Contention)  ║");
        System.out.println(  "╚══════════════════════════════════════════════════════════════╝");
        System.out.println("Buffer interval: " + BUFFER_INTERVAL_MS + "ms | Run duration: " + RUN_DURATION_MS + "ms\n");
        System.out.printf("%-10s %-15s %-18s %-15s %-15s%n",
            "Shards", "Upload (ms)", "Docs/sec", "Lock busy%", "Uploads done");
        System.out.println("─".repeat(75));

        for (int shards : SHARD_COUNTS) {
            for (int latencyMs : FAKE_UPLOAD_LATENCIES_MS) {
                BenchmarkResult result = runBenchmark(shards, latencyMs);
                System.out.printf("%-10d %-15d %-18.0f %-15.1f %-15d%n",
                    shards,
                    latencyMs,
                    result.docsPerSecond,
                    result.lockBusyPercent,
                    result.uploadsCompleted);
            }
            System.out.println();
        }
        System.out.println("Done.");
    }

    private BenchmarkResult runBenchmark(int numShards, int uploadLatencyMs) throws InterruptedException {
        // One lock per shard (models genLock per shard translog)
        List<ReentrantReadWriteLock> locks = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            locks.add(new ReentrantReadWriteLock(true)); // fair
        }

        AtomicLong docsIndexed = new AtomicLong(0);
        AtomicLong lockBusyNanos = new AtomicLong(0);   // total nanos indexing threads waited
        AtomicLong totalNanos = new AtomicLong(0);       // total indexing thread-time
        AtomicLong uploadsCompleted = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);

        // ── Indexing threads ─────────────────────────────────────────────────
        int numIndexingThreads = numShards * INDEXING_THREADS_PER_SHARD;
        List<Thread> indexingThreads = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int t = 0; t < numIndexingThreads; t++) {
            final int shardIdx = t % numShards;
            final ReentrantReadWriteLock lock = locks.get(shardIdx);
            Thread thread = new Thread(() -> {
                try { startLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                while (running.get()) {
                    long before = System.nanoTime();
                    lock.readLock().lock();
                    long locked = System.nanoTime();
                    try {
                        // Simulate translog write — hold lock briefly
                        spinBusyUs(INDEX_LOCK_HOLD_US);
                        docsIndexed.incrementAndGet();
                    } finally {
                        lock.readLock().unlock();
                    }
                    long after = System.nanoTime();
                    lockBusyNanos.addAndGet(locked - before);  // time waiting to acquire
                    totalNanos.addAndGet(after - before);       // total time per op
                }
            }, "indexing-shard-" + shardIdx + "-" + t);
            thread.setDaemon(true);
            indexingThreads.add(thread);
        }

        // ── Archive thread (models TranslogArchiveCollector.runBatchForIndex) ─
        Thread archiveThread = new Thread(() -> {
            try { startLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            while (running.get()) {
                long batchStart = System.nanoTime();
                // 1. Acquire ALL write locks (sequential, same order → no deadlock)
                for (ReentrantReadWriteLock lock : locks) {
                    lock.writeLock().lock();
                }
                try {
                    // 2. Simulate TAR build (sub-ms, negligible)
                    // 3. Simulate S3 upload (the real bottleneck)
                    Thread.sleep(uploadLatencyMs);
                    uploadsCompleted.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } finally {
                    // 4. Release all locks
                    for (ReentrantReadWriteLock lock : locks) {
                        lock.writeLock().unlock();
                    }
                }
                // 5. Wait for next buffer interval
                long elapsed = (System.nanoTime() - batchStart) / 1_000_000L;
                long sleepMs = BUFFER_INTERVAL_MS - elapsed;
                if (sleepMs > 0) {
                    try { Thread.sleep(sleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                }
            }
        }, "archive-collector");
        archiveThread.setDaemon(true);

        // ── Start all threads ────────────────────────────────────────────────
        for (Thread t : indexingThreads) t.start();
        archiveThread.start();

        startLatch.countDown();  // fire!
        Thread.sleep(RUN_DURATION_MS);
        running.set(false);

        for (Thread t : indexingThreads) t.join(1000);
        archiveThread.interrupt();
        archiveThread.join(1000);

        double docsPerSecond = docsIndexed.get() * 1000.0 / RUN_DURATION_MS;
        double lockBusyPercent = totalNanos.get() > 0
            ? (100.0 * lockBusyNanos.get()) / totalNanos.get()
            : 0;

        return new BenchmarkResult(docsPerSecond, lockBusyPercent, uploadsCompleted.get());
    }

    /**
     * Busy-wait for approximately {@code micros} microseconds without sleeping.
     * Used to simulate translog write holding the readLock.
     */
    private void spinBusyUs(int micros) {
        long deadline = System.nanoTime() + micros * 1_000L;
        while (System.nanoTime() < deadline) {
            // spin
        }
    }

    private static class BenchmarkResult {
        final double docsPerSecond;
        final double lockBusyPercent;
        final long uploadsCompleted;

        BenchmarkResult(double docsPerSecond, double lockBusyPercent, long uploadsCompleted) {
            this.docsPerSecond = docsPerSecond;
            this.lockBusyPercent = lockBusyPercent;
            this.uploadsCompleted = uploadsCompleted;
        }
    }
}
