/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;
import org.opensearch.index.translog.transfer.archive.ArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TranslogArchiveBatchCoordinatorTests extends OpenSearchTestCase {

    private TranslogArchiveBatchCoordinator createCoordinator(TimeValue batchInterval) {
        return new TranslogArchiveBatchCoordinator(
            "test-index-uuid",
            "test-node-id",
            new BlobPath().add("repo-root"),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1,
            batchInterval
        );
    }

    private TranslogArchiveBatchCoordinator.ShardArchiveData createShardData(int shardId, String content) {
        String path = "test-index-uuid/" + shardId + "/1/translog-5.tlog";
        String ckpPath = "test-index-uuid/" + shardId + "/1/translog-5.ckp";
        List<ArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        entries.add(ArchiveBuilder.fromBytes(path, content.getBytes(StandardCharsets.UTF_8)));
        entries.add(ArchiveBuilder.fromBytes(ckpPath, "ckp".getBytes(StandardCharsets.UTF_8)));
        return new TranslogArchiveBatchCoordinator.ShardArchiveData(shardId, 1L, 5L, 3L, entries);
    }

    public void testSingleShardSubmitAndDispatch() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        TranslogArchiveBatchCoordinator.ShardArchiveData shardData = createShardData(0, "shard0 data");

        // The batch interval is 1ms, so the shard's submission should trigger dispatch
        coordinator.submitAndWait(shardData, transferService);

        // Verify single ZIP upload happened
        verify(transferService).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            any(BlobPath.class),
            anyString(),
            eq(WritePriority.HIGH),
            eq(null)
        );
    }

    public void testMultipleShardsSingleZip() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);
        AtomicInteger uploadCount = new AtomicInteger(0);

        TranslogArchiveBatchCoordinator.ShardArchiveData shard0 = createShardData(0, "shard0 data");
        TranslogArchiveBatchCoordinator.ShardArchiveData shard1 = createShardData(1, "shard1 data");

        // Submit shard 0 first (won't dispatch yet — interval not elapsed)
        // Then use timerDispatch to trigger after both are submitted
        CountDownLatch bothSubmitted = new CountDownLatch(2);
        AtomicReference<Exception> error = new AtomicReference<>();

        Thread t0 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shard0, transferService);
                bothSubmitted.countDown();
            } catch (Exception e) {
                error.set(e);
                bothSubmitted.countDown();
            }
        });

        Thread t1 = new Thread(() -> {
            try {
                coordinator.submitAndWait(shard1, transferService);
                bothSubmitted.countDown();
            } catch (Exception e) {
                error.set(e);
                bothSubmitted.countDown();
            }
        });

        t0.start();
        t1.start();

        // Wait a bit for both to submit, then trigger dispatch
        Thread.sleep(60);
        coordinator.timerDispatch(transferService);

        assertTrue("Both threads should complete", bothSubmitted.await(5, TimeUnit.SECONDS));
        assertNull("No errors expected", error.get());

        // Only ONE zip upload should have happened (both shards bundled)
        verify(transferService, times(1)).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            any(BlobPath.class),
            anyString(),
            eq(WritePriority.HIGH),
            eq(null)
        );
    }

    public void testTimerDispatchWithNoPending() {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMinutes(1));
        TransferService transferService = mock(TransferService.class);

        // Should be a no-op
        coordinator.timerDispatch(transferService);
        assertEquals(0, coordinator.getPendingShardCount());
    }

    public void testEmptyEntriesNoUpload() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        // Empty entries
        TranslogArchiveBatchCoordinator.ShardArchiveData emptyData = new TranslogArchiveBatchCoordinator.ShardArchiveData(
            0,
            1L,
            5L,
            3L,
            new ArrayList<>()
        );

        coordinator.submitAndWait(emptyData, transferService);

        // No upload should happen with empty entries
        verify(transferService, times(0)).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            any(BlobPath.class),
            anyString(),
            any(WritePriority.class),
            any()
        );
    }

    public void testUploadPathContainsTranslogData() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        coordinator.submitAndWait(createShardData(0, "test"), transferService);

        org.mockito.ArgumentCaptor<BlobPath> pathCaptor = org.mockito.ArgumentCaptor.forClass(BlobPath.class);
        org.mockito.ArgumentCaptor<String> nameCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(transferService).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            pathCaptor.capture(),
            nameCaptor.capture(),
            eq(WritePriority.HIGH),
            eq(null)
        );

        String path = pathCaptor.getValue().buildAsString();
        assertThat(path, org.hamcrest.Matchers.startsWith("repo-root/translog/data/"));
        assertThat(nameCaptor.getValue(), org.hamcrest.Matchers.endsWith(".zip"));

        // Verify the second path component is hashTypeIndex and third is hashNodeId.
        String expectedHashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(
            "test-index-uuid",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        String expectedHashNodeId = TranslogArchivePathHelper.hashNodeId(
            "test-node-id",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        assertThat(path, org.hamcrest.Matchers.containsString("translog/data/" + expectedHashTypeIndex + "/" + expectedHashNodeId + "/"));
    }

    public void testUploadPathUsesHashNodeIdNotGenBucket() throws Exception {
        // Regression test: upload path must use hash(nodeId) not a hardcoded bucket like "0".
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        coordinator.submitAndWait(createShardData(0, "hello"), transferService);

        org.mockito.ArgumentCaptor<BlobPath> pathCaptor = org.mockito.ArgumentCaptor.forClass(BlobPath.class);
        verify(transferService).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            pathCaptor.capture(),
            anyString(),
            eq(WritePriority.HIGH),
            eq(null)
        );

        String path = pathCaptor.getValue().buildAsString();
        String expectedHashNodeId = TranslogArchivePathHelper.hashNodeId(
            "test-node-id",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );

        // Must contain hashNodeId — not a bare numeric bucket like "/0/"
        assertThat("Path must contain hashNodeId", path, org.hamcrest.Matchers.containsString(expectedHashNodeId));
        assertFalse("Path must NOT use bare bucket '0'", path.endsWith("/0/") || path.contains("/0/" + expectedHashNodeId));
    }

    /**
     * Static registry: register, get, unregister lifecycle.
     */
    public void testStaticRegistryLifecycle() {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMinutes(1));
        String uuid = coordinator.getIndexUUID();

        // Initially not registered
        assertNull("coordinator should not be registered yet", TranslogArchiveBatchCoordinator.get(uuid));

        // Register
        TranslogArchiveBatchCoordinator.register(coordinator);
        assertSame("get should return registered coordinator", coordinator, TranslogArchiveBatchCoordinator.get(uuid));

        // Unregister
        TranslogArchiveBatchCoordinator.unregister(uuid);
        assertNull("coordinator should be unregistered", TranslogArchiveBatchCoordinator.get(uuid));

        // Unregister again is no-op
        TranslogArchiveBatchCoordinator.unregister(uuid);
        assertNull(TranslogArchiveBatchCoordinator.get(uuid));
    }

    /**
     * Static registry: registering a second coordinator for the same index replaces the first.
     */
    public void testStaticRegistryReplacesExisting() {
        TranslogArchiveBatchCoordinator c1 = createCoordinator(TimeValue.timeValueMinutes(1));
        TranslogArchiveBatchCoordinator c2 = createCoordinator(TimeValue.timeValueMinutes(2));
        String uuid = c1.getIndexUUID();

        TranslogArchiveBatchCoordinator.register(c1);
        assertSame(c1, TranslogArchiveBatchCoordinator.get(uuid));

        TranslogArchiveBatchCoordinator.register(c2);
        assertSame("second register should replace", c2, TranslogArchiveBatchCoordinator.get(uuid));

        TranslogArchiveBatchCoordinator.unregister(uuid);
    }

    /**
     * Upload failure triggers exactly UPLOAD_RETRY_MAX_ATTEMPTS (2) upload attempts before giving up.
     */
    public void testUploadRetryCountIsExactlyTwo() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        org.mockito.Mockito.doThrow(new IOException("simulated failure"))
            .when(transferService)
            .uploadBlobStream(any(InputStream.class), anyLong(), any(BlobPath.class), anyString(), eq(WritePriority.HIGH), eq(null));

        AtomicReference<Exception> caughtError = new AtomicReference<>();
        Thread submitter = new Thread(() -> {
            try {
                coordinator.submitAndWait(createShardData(0, "retry test"), transferService);
            } catch (IOException e) {
                caughtError.set(e);
            }
        }, "retry-submitter");
        submitter.start();
        submitter.join(30_000);

        assertNotNull("Should have caught IOException", caughtError.get());

        // Verify exactly 2 upload attempts (UPLOAD_RETRY_MAX_ATTEMPTS = 2)
        verify(transferService, times(2)).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            any(BlobPath.class),
            anyString(),
            eq(WritePriority.HIGH),
            eq(null)
        );
    }

    /**
     * Concurrent coordinator dispatch under load: many shards submit rapidly across many threads.
     * Verifies that all threads complete without error and only a small number of ZIP uploads happen
     * (ideally one if all shards land in the same batch window).
     */
    public void testConcurrentDispatchUnderLoad() throws Exception {
        int numShards = 8;
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);
        AtomicInteger uploadCount = new AtomicInteger(0);
        // Count uploads (thread-safe)
        org.mockito.Mockito.doAnswer(inv -> {
            uploadCount.incrementAndGet();
            return null;
        })
            .when(transferService)
            .uploadBlobStream(any(InputStream.class), anyLong(), any(BlobPath.class), anyString(), eq(WritePriority.HIGH), eq(null));

        CyclicBarrier barrier = new CyclicBarrier(numShards);
        CountDownLatch allDone = new CountDownLatch(numShards);
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < numShards; i++) {
            final int shardId = i;
            new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    coordinator.submitAndWait(createShardData(shardId, "shard" + shardId + " data"), transferService);
                } catch (Exception e) {
                    firstError.compareAndSet(null, e);
                } finally {
                    allDone.countDown();
                }
            }, "shard-submit-" + shardId).start();
        }

        assertTrue("All threads should complete within timeout", allDone.await(30, TimeUnit.SECONDS));
        assertNull("No errors expected, got: " + firstError.get(), firstError.get());
        // At least one upload happened, but could be more if batches split across intervals
        assertTrue("At least one upload should have happened", uploadCount.get() >= 1);
        // Should be far fewer uploads than shards (batching works)
        assertTrue("Uploads (" + uploadCount.get() + ") should be fewer than shards (" + numShards + ")", uploadCount.get() < numShards);
    }

    /**
     * Coordinator failure: upload throws IOException. All waiting threads should receive
     * the error wrapped in an IOException.
     */
    public void testCoordinatorUploadFailurePropagates() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(1));
        TransferService transferService = mock(TransferService.class);

        // Simulate an upload that throws immediately
        org.mockito.Mockito.doThrow(new IOException("simulated S3 failure"))
            .when(transferService)
            .uploadBlobStream(any(InputStream.class), anyLong(), any(BlobPath.class), anyString(), eq(WritePriority.HIGH), eq(null));

        AtomicReference<Exception> caughtError = new AtomicReference<>();
        Thread submitter = new Thread(() -> {
            try {
                coordinator.submitAndWait(createShardData(0, "failure test"), transferService);
            } catch (IOException e) {
                caughtError.set(e);
            }
        }, "failure-submitter");
        submitter.start();
        submitter.join(30_000);
        assertFalse("Submitter thread should have completed", submitter.isAlive());

        // The coordinator retries UPLOAD_RETRY_MAX_ATTEMPTS (2) times, then wraps in IOException
        assertNotNull("Submitter should have caught an IOException", caughtError.get());
        assertThat(
            caughtError.get().getMessage(),
            org.hamcrest.Matchers.anyOf(
                org.hamcrest.Matchers.containsString("failed"),
                org.hamcrest.Matchers.containsString("Failed"),
                org.hamcrest.Matchers.containsString("upload")
            )
        );

        // Verify upload was attempted (coordinator retries)
        verify(transferService, org.mockito.Mockito.atLeastOnce()).uploadBlobStream(
            any(InputStream.class),
            anyLong(),
            any(BlobPath.class),
            anyString(),
            eq(WritePriority.HIGH),
            eq(null)
        );
    }

    /**
     * Pipelining: while batch N is uploading, the coordinator should immediately accept
     * submissions for batch N+1 — i.e. the collection window for batch N+1 starts as soon
     * as batch N is handed off to the upload thread, NOT after the upload completes.
     *
     * If upload takes U ms and batch interval is B ms, then two consecutive cycles should
     * complete in approximately max(B, U) + B, NOT 2*(B + U).
     *
     * We verify this by injecting a slow upload (300ms) with a short batch interval (50ms).
     * Without pipelining: 2 cycles = 2 * (50 + 300) = 700ms minimum.
     * With pipelining:    2 cycles = (50 + 300) + 50 = 400ms (upload and next collection overlap).
     */
    public void testNextBatchCollectionStartsWhileUploadInProgress() throws Exception {
        int batchIntervalMs = 50;
        int uploadDelayMs = 300;
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(batchIntervalMs));
        TransferService transferService = mock(TransferService.class);

        // Slow upload: blocks for uploadDelayMs
        org.mockito.Mockito.doAnswer(inv -> {
            Thread.sleep(uploadDelayMs);
            return null;
        }).when(transferService).uploadBlobStream(any(), anyLong(), any(), anyString(), any(), any());

        CountDownLatch bothDone = new CountDownLatch(2);
        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicReference<Long> firstSubmitEndMs = new AtomicReference<>();
        AtomicReference<Long> secondSubmitEndMs = new AtomicReference<>();

        // Batch 1: submit shard 0, wait for it to complete
        Thread batch1 = new Thread(() -> {
            try {
                coordinator.submitAndWait(createShardData(0, "batch1"), transferService);
                firstSubmitEndMs.set(System.currentTimeMillis());
            } catch (Exception e) {
                error.set(e);
            } finally {
                bothDone.countDown();
            }
        }, "batch1-thread");

        // Batch 2: submit shard 1 shortly after batch 1 starts (while upload of batch 1 is in progress)
        Thread batch2 = new Thread(() -> {
            try {
                Thread.sleep(batchIntervalMs + 10); // start after batch 1 has been dispatched
                coordinator.submitAndWait(createShardData(1, "batch2"), transferService);
                secondSubmitEndMs.set(System.currentTimeMillis());
            } catch (Exception e) {
                error.set(e);
            } finally {
                bothDone.countDown();
            }
        }, "batch2-thread");

        long start = System.currentTimeMillis();
        batch1.start();
        batch2.start();

        assertTrue("Both batches should complete", bothDone.await(10, TimeUnit.SECONDS));
        long totalMs = System.currentTimeMillis() - start;
        assertNull("No errors expected: " + error.get(), error.get());

        // With pipelining: batch2 collection starts while batch1 uploads
        // total ≈ (batchInterval + uploadDelay) + batchInterval = 400ms
        // Without pipelining: total ≈ (batchInterval + uploadDelay) * 2 = 700ms
        long pipelinedBound = (batchIntervalMs + uploadDelayMs) + batchIntervalMs + 100; // 400ms + slack
        assertTrue(
            "With pipelining total time "
                + totalMs
                + "ms should be < "
                + pipelinedBound
                + "ms (non-pipelined would be ~"
                + 2 * (batchIntervalMs + uploadDelayMs)
                + "ms)",
            totalMs < pipelinedBound
        );

        // Both uploads happened (two separate batches)
        verify(transferService, times(2)).uploadBlobStream(any(), anyLong(), any(), anyString(), any(), any());
    }

    /**
     * Coordinator failure propagates to multiple waiting threads: when upload fails,
     * all threads that submitted to the same batch receive the error.
     */
    public void testCoordinatorFailurePropagesToMultipleWaitingThreads() throws Exception {
        TranslogArchiveBatchCoordinator coordinator = createCoordinator(TimeValue.timeValueMillis(50));
        TransferService transferService = mock(TransferService.class);

        org.mockito.Mockito.doThrow(new IOException("simulated failure"))
            .when(transferService)
            .uploadBlobStream(any(InputStream.class), anyLong(), any(BlobPath.class), anyString(), eq(WritePriority.HIGH), eq(null));

        int numShards = 3;
        CountDownLatch allDone = new CountDownLatch(numShards);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numShards; i++) {
            final int shardId = i;
            new Thread(() -> {
                try {
                    coordinator.submitAndWait(createShardData(shardId, "fail-data-" + shardId), transferService);
                } catch (IOException e) {
                    errorCount.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            }, "fail-shard-" + shardId).start();
        }

        // Force dispatch after threads have submitted
        Thread.sleep(60);
        coordinator.timerDispatch(transferService);

        assertTrue("All threads should complete", allDone.await(30, TimeUnit.SECONDS));
        // All threads that were in the same batch should have received the error
        assertTrue("At least one thread should get the error", errorCount.get() >= 1);
    }
}
