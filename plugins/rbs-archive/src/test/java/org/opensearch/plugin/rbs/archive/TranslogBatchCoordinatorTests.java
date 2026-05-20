/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link TranslogBatchCoordinator}: flush triggers, concurrency, retry logic,
 * and failure propagation. Ported and adapted from the feature branch's
 * {@code TranslogArchiveBatchCoordinatorTests}.
 */
public class TranslogBatchCoordinatorTests extends OpenSearchTestCase {

    /** All coordinators created during a test — closed in tearDown to stop timer threads. */
    private final List<TranslogBatchCoordinator> coordinatorsToClose = new ArrayList<>();

    @After
    public void closeCoordinators() {
        for (TranslogBatchCoordinator c : coordinatorsToClose) {
            c.close();
        }
        coordinatorsToClose.clear();
    }

    private static final String NODE_ID = "test-node-id";
    private static final BlobPath BASE_PATH = BlobPath.cleanPath().add("repo-root");
    private static final String INDEX_UUID = "test-index-uuid";

    /**
     * Creates a coordinator with a mock strategy. The strategy's {@code uploadBatch()} does
     * nothing by default (no exception = success). Tests that need to verify upload count
     * or inject failures can configure the mock further.
     *
     * @param batchInterval timer-based flush interval
     * @param threshold     shard-count threshold for early dispatch; use {@code Integer.MAX_VALUE}
     *                      to disable early dispatch and rely only on the timer
     */
    private TranslogBatchCoordinator createCoordinator(TimeValue batchInterval, int threshold) {
        TarTranslogRemoteStoreStrategy strategy = mock(TarTranslogRemoteStoreStrategy.class);
        TranslogBatchCoordinator c = new TranslogBatchCoordinator(NODE_ID, strategy, batchInterval, threshold);
        // Note: setStrategy() not called again — constructor already wires it
        coordinatorsToClose.add(c);
        return c;
    }

    /** Convenience overload: threshold=Integer.MAX_VALUE (timer-only dispatch). */
    private TranslogBatchCoordinator createCoordinator(TimeValue batchInterval) {
        return createCoordinator(batchInterval, Integer.MAX_VALUE);
    }

    private TranslogShardBatch shardBatch(int shardId, String content) throws IOException {
        Path tlogFile = createTempFile("translog-5-shard" + shardId, ".tlog");
        Files.write(tlogFile, content.getBytes(StandardCharsets.UTF_8));
        Path ckpFile = createTempFile("translog-5-shard" + shardId, ".ckp");
        Files.write(ckpFile, "ckp".getBytes(StandardCharsets.UTF_8));

        String tlogRemote = INDEX_UUID + "/" + shardId + "/1/translog-5.tlog";
        String ckpRemote = INDEX_UUID + "/" + shardId + "/1/translog-5.ckp";
        List<TranslogShardBatch.BatchFile> files = new ArrayList<>();
        files.add(new TranslogShardBatch.BatchFile(tlogFile, tlogRemote, Files.size(tlogFile)));
        files.add(new TranslogShardBatch.BatchFile(ckpFile, ckpRemote, Files.size(ckpFile)));
        return new TranslogShardBatch(INDEX_UUID, shardId, 1L, 5L, 3L, files, 100L, 200L, 150L);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /** Single shard submits; timer fires; strategy.uploadBatch() called exactly once. */
    public void testSingleShardSubmitAndDispatch() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        coordinator.submitAndWait(shardBatch(0, "shard0 data"), transferService, BASE_PATH);

        verify(coordinator.strategy, times(1)).uploadBatch(any(), any(), any());
    }

    /** Two shards submitted concurrently → bundled into single uploadBatch call. */
    public void testMultipleShardsSingleBatch() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);

        CountDownLatch bothDone = new CountDownLatch(2);
        AtomicReference<Exception> error = new AtomicReference<>();

        Thread t0 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shardBatch(0, "shard0"), transferService, BASE_PATH);
            } catch (Exception e) {
                error.set(e);
            } finally {
                bothDone.countDown();
            }
        });

        Thread t1 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shardBatch(1, "shard1"), transferService, BASE_PATH);
            } catch (Exception e) {
                error.set(e);
            } finally {
                bothDone.countDown();
            }
        });

        t0.start();
        t1.start();

        // Both threads return when the timer fires
        assertTrue("Both threads must complete within 5 s", bothDone.await(5, TimeUnit.SECONDS));
        assertNull("No errors expected", error.get());

        // ONE batch upload for both shards
        verify(coordinator.strategy, times(1)).uploadBatch(any(), any(), any());
    }

    /**
     * Coordinator with a very long timer and no submissions does not call uploadBatch.
     * Tests idle state correctness.
     */
    public void testNoUploadWhenNoShardsSubmitted() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMinutes(1));

        // Sleep briefly to ensure no spurious dispatch
        Thread.sleep(10);

        verify(coordinator.strategy, times(0)).uploadBatch(any(), any(), any());
    }

    /** Threshold-based early dispatch: N shards ≥ threshold → upload without waiting for timer. */
    public void testThresholdEarlyDispatch() throws Exception {
        int threshold = 3;
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMinutes(10), threshold);
        TransferService transferService = mock(TransferService.class);

        AtomicInteger uploadCount = new AtomicInteger(0);
        doAnswer(inv -> {
            uploadCount.incrementAndGet();
            return null;
        }).when(coordinator.strategy).uploadBatch(any(), any(), any());

        // Submit exactly 'threshold' shards concurrently
        List<Thread> threads = new ArrayList<>();
        CountDownLatch allDone = new CountDownLatch(threshold);
        CyclicBarrier barrier = new CyclicBarrier(threshold);

        for (int i = 0; i < threshold; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                try {
                    barrier.await(); // all start simultaneously
                    coordinator.submitAndWait(shardBatch(idx, "content-" + idx), transferService, BASE_PATH);
                } catch (Exception e) {
                    // expected if interrupt
                } finally {
                    allDone.countDown();
                }
            });
            threads.add(t);
            t.start();
        }

        assertTrue("All shard threads must complete within 10 s", allDone.await(10, TimeUnit.SECONDS));
        // Threshold dispatch should have triggered exactly once
        assertEquals("Threshold dispatch should have triggered exactly once", 1, uploadCount.get());
    }

    /** Upload retry: strategy throws IOException twice, succeeds on 3rd attempt. */
    public void testUploadRetryCount() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(inv -> {
            int n = callCount.incrementAndGet();
            if (n < 3) throw new IOException("Simulated upload failure attempt " + n);
            return null;
        }).when(coordinator.strategy).uploadBatch(any(), any(), any());

        // With UPLOAD_RETRY_MAX_ATTEMPTS = 3, should succeed on 3rd attempt
        coordinator.submitAndWait(shardBatch(0, "data"), transferService, BASE_PATH);
        assertEquals("Should have retried exactly 3 times total", 3, callCount.get());
    }

    /** Upload exhausts all 3 retries → IOException propagates to caller. */
    public void testUploadFailurePropagates() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        doThrow(new IOException("Persistent failure")).when(coordinator.strategy).uploadBatch(any(), any(), any());

        IOException ex = expectThrows(
            IOException.class,
            () -> coordinator.submitAndWait(shardBatch(0, "data"), transferService, BASE_PATH)
        );
        assertNotNull(ex);
    }

    /**
     * Failure propagates to ALL shards waiting on the same batch — not just the dispatch thread.
     */
    public void testUploadFailurePropagatestoMultipleWaiters() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);

        doThrow(new IOException("Batch failed")).when(coordinator.strategy).uploadBatch(any(), any(), any());

        int numWaiters = 4;
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch allDone = new CountDownLatch(numWaiters);

        for (int i = 0; i < numWaiters; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    coordinator.submitAndWait(shardBatch(idx, "shard-" + idx), transferService, BASE_PATH);
                } catch (IOException e) {
                    failureCount.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            }).start();
        }

        assertTrue("All waiters must finish within 5 s", allDone.await(5, TimeUnit.SECONDS));
        assertEquals("All waiting shards should have received the failure", numWaiters, failureCount.get());
    }

    /** submitAndWait after close() throws IOException immediately. */
    public void testSubmitAfterCloseThrows() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMinutes(1));
        TransferService transferService = mock(TransferService.class);
        coordinator.close();

        expectThrows(IOException.class, () -> coordinator.submitAndWait(shardBatch(0, "data"), transferService, BASE_PATH));
    }

    /**
     * Duplicate shard within same batch window: second submission overwrites first.
     * Only ONE translog-5.tlog entry must appear in the uploaded batch (not two).
     */
    public void testDuplicateShardOverwrittenInBatch() throws Exception {
        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);

        AtomicReference<List<TranslogShardBatch>> capturedBatch = new AtomicReference<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<TranslogShardBatch> batch = inv.getArgument(0);
            capturedBatch.set(new ArrayList<>(batch));
            return null;
        }).when(coordinator.strategy).uploadBatch(any(), any(), any());

        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Exception> err = new AtomicReference<>();

        // Two submits for the SAME shard 0 simultaneously → only the later one should be in the batch
        for (int i = 0; i < 2; i++) {
            final int seq = i;
            new Thread(() -> {
                try {
                    coordinator.submitAndWait(shardBatch(0, "content-v" + seq), transferService, BASE_PATH);
                } catch (Exception e) {
                    err.set(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNull(err.get());

        // The batch should contain exactly 1 entry for shard 0 (duplicate was overwritten)
        assertNotNull(capturedBatch.get());
        long shard0Count = capturedBatch.get().stream().filter(b -> b.getShardId() == 0 && INDEX_UUID.equals(b.getIndexUUID())).count();
        assertEquals("Duplicate shard submission must be deduplicated", 1, shard0Count);
    }

    /**
     * Two independent coordinators do not share state — each uploads exactly once.
     */
    public void testTwoCoordinatorsAreIndependent() throws Exception {
        TranslogBatchCoordinator c1 = createCoordinator(TimeValue.timeValueMillis(1));
        TranslogBatchCoordinator c2 = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService ts = mock(TransferService.class);

        c1.submitAndWait(shardBatch(0, "data"), ts, BASE_PATH);
        c2.submitAndWait(shardBatch(0, "data"), ts, BASE_PATH);

        // Each coordinator should have called uploadBatch exactly once
        verify(c1.strategy, times(1)).uploadBatch(any(), any(), any());
        verify(c2.strategy, times(1)).uploadBatch(any(), any(), any());
    }

    /**
     * While one batch is uploading, a new shard can submit to the NEXT batch and also complete.
     * This tests the double-buffer (pipeline) behaviour.
     */
    public void testNextBatchCollectionStartsWhileUploadInProgress() throws Exception {
        CountDownLatch uploadStarted = new CountDownLatch(1);
        CountDownLatch releaseUpload = new CountDownLatch(1);

        TranslogBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        AtomicInteger uploadCount = new AtomicInteger(0);
        doAnswer(inv -> {
            int n = uploadCount.incrementAndGet();
            if (n == 1) {
                uploadStarted.countDown();
                releaseUpload.await(5, TimeUnit.SECONDS);
            }
            return null;
        }).when(coordinator.strategy).uploadBatch(any(), any(), any());

        // First shard: triggers upload (which blocks)
        Thread t1 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shardBatch(0, "shard0"), transferService, BASE_PATH);
            } catch (Exception e) {
                logger.error("t1 error", e);
            }
        });
        t1.start();

        // Wait for upload to start, then submit second shard for the NEXT batch
        assertTrue(uploadStarted.await(5, TimeUnit.SECONDS));

        Thread t2 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shardBatch(1, "shard1"), transferService, BASE_PATH);
            } catch (Exception e) {
                logger.error("t2 error", e);
            }
        });
        t2.start();

        // Release the first upload
        releaseUpload.countDown();

        t1.join(5000);
        t2.join(5000);

        // Both batches should have been uploaded (total 2 calls)
        assertEquals("Should have uploaded 2 batches (pipeline)", 2, uploadCount.get());
    }
}
