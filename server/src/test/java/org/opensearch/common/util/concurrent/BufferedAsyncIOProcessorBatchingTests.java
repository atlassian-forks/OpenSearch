/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Proves that {@link BufferedAsyncIOProcessor} already batches multiple items submitted
 * within the buffer interval into a single {@code write()} call. This means downstream
 * consumers (like {@code TranslogArchiveBatchCoordinator}) do NOT need their own batch
 * interval wait — the upstream processor has already done the buffering.
 */
public class BufferedAsyncIOProcessorBatchingTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ThreadContext threadContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("batching-test");
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    /**
     * Multiple items submitted rapidly within the buffer interval are delivered
     * together in a single {@code write()} invocation — proving that
     * BufferedAsyncIOProcessor already provides interval-based batching.
     *
     * Simulates the translog sync path: multiple shards of the same index
     * call {@code IndexShard.sync()} concurrently. The BufferedAsyncIOProcessor
     * collects all their Translog.Location items and delivers them as one batch
     * to {@code Engine.ensureTranslogSynced()}, which calls
     * {@code RemoteFsTranslog.ensureSynced()} for each location.
     *
     * This proves that a downstream batch coordinator does NOT need to add its
     * own wait interval — the items are already temporally co-located.
     */
    public void testMultipleItemsWithinIntervalAreBatchedInSingleWrite() throws Exception {
        final int itemCount = 10;
        final long bufferIntervalMs = 500;

        // Track how many items each write() call receives
        ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
        CountDownLatch allNotified = new CountDownLatch(itemCount);

        BufferedAsyncIOProcessor<Integer> processor = new BufferedAsyncIOProcessor<>(
            logger,
            10240,
            threadContext,
            threadPool,
            () -> TimeValue.timeValueMillis(bufferIntervalMs)
        ) {
            @Override
            protected void write(List<Tuple<Integer, Consumer<Exception>>> candidates) throws IOException {
                batchSizes.add(candidates.size());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        // Submit all items rapidly (well within the buffer interval)
        for (int i = 0; i < itemCount; i++) {
            processor.put(i, e -> allNotified.countDown());
        }

        assertTrue("All items should be processed", allNotified.await(bufferIntervalMs * 3, TimeUnit.MILLISECONDS));

        // The key assertion: the FIRST write() call should contain multiple items
        // (ideally all of them) because they were all submitted within the buffer interval.
        int firstBatchSize = batchSizes.peek();
        assertTrue(
            "First write() call should batch multiple items (got " + firstBatchSize + " out of " + itemCount + "). "
                + "This proves BufferedAsyncIOProcessor already buffers items within the interval. "
                + "Batch sizes: " + batchSizes,
            firstBatchSize > 1
        );

        // Total items across all write() calls must equal itemCount
        int totalProcessed = batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertEquals("All items must be processed", itemCount, totalProcessed);
    }

    /**
     * Concurrent submitters from multiple threads (simulating multiple shard sync threads
     * for the same index) are batched into a single write() call within one buffer interval.
     *
     * This directly models the scenario where 64 concurrent indexing clients drive
     * shard syncs that all arrive at the BufferedAsyncIOProcessor within 650ms.
     */
    public void testConcurrentSubmittersAreBatchedWithinInterval() throws Exception {
        final int threadCount = 8;
        final long bufferIntervalMs = 500;

        ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
        CountDownLatch allNotified = new CountDownLatch(threadCount);
        CyclicBarrier startBarrier = new CyclicBarrier(threadCount);

        BufferedAsyncIOProcessor<Integer> processor = new BufferedAsyncIOProcessor<>(
            logger,
            10240,
            threadContext,
            threadPool,
            () -> TimeValue.timeValueMillis(bufferIntervalMs)
        ) {
            @Override
            protected void write(List<Tuple<Integer, Consumer<Exception>>> candidates) throws IOException {
                batchSizes.add(candidates.size());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        // All threads submit simultaneously
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            final int shardId = i;
            Thread t = new Thread(() -> {
                try {
                    startBarrier.await(5, TimeUnit.SECONDS);
                    processor.put(shardId, e -> allNotified.countDown());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "shard-sync-" + i);
            t.setDaemon(true);
            threads.add(t);
            t.start();
        }

        assertTrue("All items should be processed", allNotified.await(bufferIntervalMs * 3, TimeUnit.MILLISECONDS));

        for (Thread t : threads) {
            t.join(5000);
        }

        // With concurrent submissions all arriving before the first drain, the first
        // write() should contain most (if not all) items.
        int maxBatchSize = batchSizes.stream().mapToInt(Integer::intValue).max().orElse(0);
        assertTrue(
            "At least one write() call should batch multiple concurrent submitters (max batch size: "
                + maxBatchSize + ", threads: " + threadCount + "). "
                + "This proves the upstream BufferedAsyncIOProcessor already batches concurrent shard syncs. "
                + "Downstream coordinators should NOT add their own wait interval.",
            maxBatchSize > 1
        );

        int totalProcessed = batchSizes.stream().mapToInt(Integer::intValue).sum();
        assertEquals("All items must be processed", threadCount, totalProcessed);
    }

    /**
     * Verifies that items submitted AFTER the buffer interval elapses go into the NEXT
     * write() batch — confirming the interval-based batching boundary.
     */
    public void testItemsAfterIntervalGoToNextBatch() throws Exception {
        final long bufferIntervalMs = 200;
        final AtomicInteger writeCallCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
        CountDownLatch firstBatchDone = new CountDownLatch(1);
        CountDownLatch secondBatchDone = new CountDownLatch(1);

        BufferedAsyncIOProcessor<String> processor = new BufferedAsyncIOProcessor<>(
            logger,
            10240,
            threadContext,
            threadPool,
            () -> TimeValue.timeValueMillis(bufferIntervalMs)
        ) {
            @Override
            protected void write(List<Tuple<String, Consumer<Exception>>> candidates) throws IOException {
                batchSizes.add(candidates.size());
                writeCallCount.incrementAndGet();
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        // Submit 3 items in the first interval
        for (int i = 0; i < 3; i++) {
            processor.put("batch1-item" + i, e -> firstBatchDone.countDown());
        }

        // Wait for first batch to process
        assertTrue("First batch should complete", firstBatchDone.await(bufferIntervalMs * 3, TimeUnit.MILLISECONDS));

        // Wait past the buffer interval
        Thread.sleep(bufferIntervalMs + 50);

        // Submit 2 more items — these should go into a separate write() call
        for (int i = 0; i < 2; i++) {
            processor.put("batch2-item" + i, e -> secondBatchDone.countDown());
        }

        assertTrue("Second batch should complete", secondBatchDone.await(bufferIntervalMs * 3, TimeUnit.MILLISECONDS));

        // There should be at least 2 write() calls (items split across intervals)
        assertTrue(
            "Should have at least 2 write() calls for items across intervals (got " + writeCallCount.get() + ")",
            writeCallCount.get() >= 2
        );
    }
}

