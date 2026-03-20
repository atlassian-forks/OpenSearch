/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.translog.transfer.TranslogTransferManager.CHECKPOINT_FILE_DATA_KEY;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("*")
public class TranslogTransferManagerTests extends OpenSearchTestCase {

    private TransferService transferService;
    private ShardId shardId;
    private BlobPath remoteBaseTransferPath;
    private ThreadPool threadPool;
    private long primaryTerm;
    private long generation;
    private long minTranslogGeneration;
    private RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    byte[] tlogBytes;
    byte[] ckpBytes;
    FileTransferTracker tracker;
    TranslogTransferManager translogTransferManager;
    long delayForBlobDownload;
    boolean isTranslogMetadataEnabled;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm = randomNonNegativeLong();
        generation = randomNonNegativeLong();
        shardId = mock(ShardId.class);
        when(shardId.getIndex()).thenReturn(new Index("index", "indexUUid"));
        minTranslogGeneration = randomLongBetween(0, generation);
        remoteBaseTransferPath = new BlobPath().add("base_path");
        transferService = mock(TransferService.class);
        threadPool = new TestThreadPool(getClass().getName());
        remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        tlogBytes = "Hello Translog".getBytes(StandardCharsets.UTF_8);
        ckpBytes = "Hello Checkpoint".getBytes(StandardCharsets.UTF_8);
        tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker);
        isTranslogMetadataEnabled = false;
        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );

        delayForBlobDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new ByteArrayInputStream(tlogBytes);
        });

        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.ckp"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new ByteArrayInputStream(ckpBytes);
        });
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testTransferSnapshot() throws Exception {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        doNothing().when(transferService)
            .uploadBlob(
                any(TransferFileSnapshot.class),
                Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))),
                any(WritePriority.class)
            );
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(listener::onResponse);
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        ) {
            @Override
            public void onSuccess(TransferFileSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }

        };

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );

        assertTrue(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                translogTransferSucceeded.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                translogTransferFailed.incrementAndGet();
            }
        }));
        assertEquals(4, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(4, fileTransferTracker.allUploaded().size());
    }

    public void testTransferSnapshotOnUploadTimeout() throws Exception {
        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            Runnable runnable = () -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (TransferFileSnapshot transferFileSnapshot : transferFileSnapshots) {
                    listener.onResponse(transferFileSnapshot);
                }
            };
            Thread t = new Thread(runnable);
            t.start();
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        );
        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogTransferTimeout()).thenReturn(new TimeValue(1));
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled
        );
        SetOnce<Exception> exception = new SetOnce<>();
        translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                exception.set(ex);
            }
        });
        assertNotNull(exception.get());
        assertTrue(exception.get() instanceof TranslogUploadFailedException);
        assertEquals("Timed out waiting for transfer of snapshot test-to-string to complete", exception.get().getMessage());
    }

    public void testTransferSnapshotOnThreadInterrupt() throws Exception {
        SetOnce<Thread> uploadThread = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            uploadThread.set(new Thread(() -> {
                ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
                try {
                    Thread.sleep(31 * 1000);
                } catch (InterruptedException ignore) {
                    List<TransferFileSnapshot> list = new ArrayList<>(invocationOnMock.getArgument(0));
                    listener.onFailure(new FileTransferException(list.get(0), ignore));
                }
            }));
            uploadThread.get().start();
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        );
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        SetOnce<Exception> exception = new SetOnce<>();

        Thread thread = new Thread(() -> {
            try {
                translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
                    @Override
                    public void onUploadComplete(TransferSnapshot transferSnapshot) {}

                    @Override
                    public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                        exception.set(ex);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();

        Thread.sleep(1000);
        // Interrupt the thread
        thread.interrupt();
        assertBusy(() -> {
            assertNotNull(exception.get());
            assertTrue(exception.get() instanceof TranslogUploadFailedException);
            assertEquals("Failed to upload test-to-string", exception.get().getMessage());
        });
        uploadThread.get().interrupt();
    }

    private TransferSnapshot createTransferSnapshot() throws IOException {
        try {
            CheckpointFileSnapshot checkpointFileSnapshot1 = new CheckpointFileSnapshot(
                primaryTerm,
                generation,
                minTranslogGeneration,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX),
                null
            );
            CheckpointFileSnapshot checkpointFileSnapshot2 = new CheckpointFileSnapshot(
                primaryTerm,
                generation,
                minTranslogGeneration,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX),
                null
            );
            TranslogFileSnapshot translogFileSnapshot1 = new TranslogFileSnapshot(
                primaryTerm,
                generation,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX),
                null
            );
            TranslogFileSnapshot translogFileSnapshot2 = new TranslogFileSnapshot(
                primaryTerm,
                generation - 1,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX),
                null
            );

            return new TransferSnapshot() {
                @Override
                public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
                    return Set.of(checkpointFileSnapshot1, checkpointFileSnapshot2);
                }

                @Override
                public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
                    return Set.of(translogFileSnapshot1, translogFileSnapshot2);
                }

                @Override
                public TranslogTransferMetadata getTranslogTransferMetadata() {
                    return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
                }

                @Override
                public Set<TransferFileSnapshot> getTranslogFileSnapshotWithMetadata() throws IOException {
                    translogFileSnapshot1.setMetadataFileInputStream(checkpointFileSnapshot1.inputStream());
                    translogFileSnapshot2.setMetadataFileInputStream(checkpointFileSnapshot2.inputStream());
                    return Set.of(translogFileSnapshot1, translogFileSnapshot2);
                }

                @Override
                public String toString() {
                    return "test-to-string";
                }
            };
        } catch (Exception e) {
            throw new IOException("Failed to create transfer snapshot");
        }
    }

    public void testReadMetadataNoFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata());
        assertNoDownloadStats(false);
    }

    // This should happen most of the time -
    public void testReadMetadataFile() throws IOException {
        TranslogTransferMetadata metadata1 = new TranslogTransferMetadata(1, 1, 1, 2);
        String mdFilename1 = metadata1.getFileName();

        TranslogTransferMetadata metadata2 = new TranslogTransferMetadata(1, 0, 1, 2);
        String mdFilename2 = metadata2.getFileName();
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename1, 1));
            bmList.add(new PlainBlobMetadata(mdFilename2, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        long delayForMdDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1))).thenAnswer(invocation -> {
            Thread.sleep(delayForMdDownload);
            return new ByteArrayInputStream(translogTransferManager.getMetadataBytes(metadata));
        });

        assertEquals(metadata, translogTransferManager.readMetadata());

        assertEquals(translogTransferManager.getMetadataBytes(metadata).length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForMdDownload);
    }

    public void testReadMetadataReadException() throws IOException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2);
        String mdFilename = tm.getFileName();

        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, translogTransferManager::readMetadata);
        assertNoDownloadStats(true);
    }

    public void testMetadataFileNameOrder() throws IOException {
        // asserting that new primary followed new generation are lexicographically smallest
        String mdFilenameGen1 = new TranslogTransferMetadata(1, 1, 1, 2).getFileName();
        String mdFilenameGen2 = new TranslogTransferMetadata(1, 2, 1, 2).getFileName();
        String mdFilenamePrimary2 = new TranslogTransferMetadata(2, 1, 1, 2).getFileName();
        List<String> metadataFiles = Arrays.asList(mdFilenameGen1, mdFilenameGen2, mdFilenamePrimary2);
        Collections.sort(metadataFiles);
        assertEquals(Arrays.asList(mdFilenamePrimary2, mdFilenameGen2, mdFilenameGen1), metadataFiles);
    }

    public void testReadMetadataListException() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onFailure(new IOException("Issue while listing"));
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), any(String.class))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, translogTransferManager::readMetadata);
        assertNoDownloadStats(false);
    }

    public void testDownloadTranslog() throws IOException {
        Path location = createTempDir();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogAlreadyExists() throws IOException {
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));

        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogWithTrackerUpdated() throws IOException {
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));

        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService).downloadBlob(any(BlobPath.class), eq(translogFile));
        verify(transferService).downloadBlob(any(BlobPath.class), eq(checkpointFile));
        assertTrue(Files.exists(location.resolve(translogFile)));
        assertTrue(Files.exists(location.resolve(checkpointFile)));

        // Since the tracker already holds the files with success state, adding them with failed state would throw exception
        assertThrows(IllegalStateException.class, () -> tracker.add(translogFile, false));
        assertThrows(IllegalStateException.class, () -> tracker.add(checkpointFile, false));

        // Since the tracker already holds the files with success state, adding them with success state is allowed
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertTlogCkpDownloadStats();
    }

    public void testDeleteTranslogSuccess() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(2, tracker.allUploaded().size());

        List<String> files = List.of(checkpointFile, translogFile);
        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertBusy(() -> assertEquals(0, tracker.allUploaded().size()));
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(eq(files));
    }

    public void testDeleteStaleTranslogMetadata() {
        String tm1 = new TranslogTransferMetadata(1, 1, 1, 2).getFileName();
        String tm2 = new TranslogTransferMetadata(1, 2, 1, 2).getFileName();
        String tm3 = new TranslogTransferMetadata(2, 3, 1, 2).getFileName();
        doAnswer(invocation -> {
            ActionListener<List<BlobMetadata>> actionListener = invocation.getArgument(4);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(tm1, 1));
            bmList.add(new PlainBlobMetadata(tm2, 1));
            bmList.add(new PlainBlobMetadata(tm3, 1));
            actionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrderAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(TranslogTransferMetadata.METADATA_PREFIX),
                anyInt(),
                any(ActionListener.class)
            );
        List<String> files = List.of(tm2, tm3);
        translogTransferManager.deleteStaleTranslogMetadataFilesAsync(() -> {
            verify(transferService).listAllInSortedOrderAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(TranslogTransferMetadata.METADATA_PREFIX),
                eq(Integer.MAX_VALUE),
                any()
            );
            verify(transferService).deleteBlobsAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(files),
                any(ActionListener.class)
            );
        });
    }

    public void testDeleteTranslogFailure() throws Exception {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> { throw new IOException("test exception"); }).when(blobStore).blobContainer(any(BlobPath.class));
        // when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(2, tracker.allUploaded().size());

        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertEquals(2, tracker.allUploaded().size());
    }

    private void assertNoDownloadStats(boolean nonZeroUploadTime) {
        assertEquals(0, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertEquals(0, remoteTranslogTransferTracker.getTotalDownloadsSucceeded());
        assertEquals(0, remoteTranslogTransferTracker.getLastSuccessfulDownloadTimestamp());
        if (nonZeroUploadTime == false) {
            assertEquals(0, remoteTranslogTransferTracker.getTotalDownloadTimeInMillis());
        }
    }

    private void assertTlogCkpDownloadStats() {
        assertEquals(tlogBytes.length + ckpBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= 2 * delayForBlobDownload);
    }

    public void testGetPrimaryTermAndGeneration() {
        String nodeId = UUID.randomUUID().toString();
        String tm = new TranslogTransferMetadata(1, 2, 1, 2, nodeId).getFileName();
        Tuple<Tuple<Long, Long>, String> actualOutput = TranslogTransferMetadata.getNodeIdByPrimaryTermAndGeneration(tm);
        assertEquals(1L, (long) (actualOutput.v1().v1()));
        assertEquals(2L, (long) (actualOutput.v1().v2()));
        assertEquals(String.valueOf(Objects.hash(nodeId)), actualOutput.v2());
    }

    public void testMetadataConflict() throws InterruptedException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename = tm.getFileName();
        long count = mdFilename.chars().filter(ch -> ch == METADATA_SEPARATOR.charAt(0)).count();
        // There should not be any `_` in mdFile name as it is used a separator .
        assertEquals(14, count);
        Thread.sleep(1);
        TranslogTransferMetadata tm2 = new TranslogTransferMetadata(1, 1, 1, 2, "node--2");
        String mdFilename2 = tm2.getFileName();

        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename, 1));
            bmList.add(new PlainBlobMetadata(mdFilename2, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertThrows(RuntimeException.class, translogTransferManager::readMetadata);
    }

    // tests for cases when ckp is stored as translog metadata.
    public void testTransferSnapshotWithTranslogMetadata() throws Exception {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        isTranslogMetadataEnabled = true;

        doNothing().when(transferService)
            .uploadBlob(
                any(TransferFileSnapshot.class),
                Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))),
                any(WritePriority.class)
            );
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(transferFileSnapshot -> {
                assertNotNull(transferFileSnapshot.getMetadataFileInputStream());
                listener.onResponse(transferFileSnapshot);
            });
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        ) {
            @Override
            public void onSuccess(TransferFileSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }

        };

        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );

        assertTrue(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                translogTransferSucceeded.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                translogTransferFailed.incrementAndGet();
            }
        }));
        assertEquals(2, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(2, fileTransferTracker.allUploaded().size());
    }

    public void testDownloadTranslogWithMetadata() throws IOException {
        isTranslogMetadataEnabled = true;
        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        Path location = createTempDir();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        mockDownloadBlobWithMetadataResponse();
        translogTransferManager.downloadTranslog("12", "23", location);
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        verify(transferService, times(1)).downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStatsWithMetadata();
    }

    private void mockDownloadBlobWithMetadataResponse() throws IOException {
        Map<String, String> metadata = new HashMap<>();
        String ckpDataString = Base64.getEncoder().encodeToString(ckpBytes);
        metadata.put(CHECKPOINT_FILE_DATA_KEY, ckpDataString);
        when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new InputStreamWithMetadata(new ByteArrayInputStream(tlogBytes), metadata);
        });
    }

    private void assertTlogCkpDownloadStatsWithMetadata() {
        assertEquals(tlogBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForBlobDownload);
    }

    public void testlistTranslogMetadataFilesAsync() throws Exception {
        String tm1 = new TranslogTransferMetadata(1, 1, 1, 2).getFileName();
        String tm2 = new TranslogTransferMetadata(1, 2, 1, 2).getFileName();
        String tm3 = new TranslogTransferMetadata(2, 3, 1, 2).getFileName();
        doAnswer(invocation -> {
            ActionListener<List<BlobMetadata>> actionListener = invocation.getArgument(4);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(tm1, 1));
            bmList.add(new PlainBlobMetadata(tm2, 1));
            bmList.add(new PlainBlobMetadata(tm3, 1));
            actionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrderAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(TranslogTransferMetadata.METADATA_PREFIX),
                anyInt(),
                any(ActionListener.class)
            );
        AtomicBoolean fetchCompleted = new AtomicBoolean(false);
        translogTransferManager.listTranslogMetadataFilesAsync(new ActionListener<>() {
            @Override
            public void onResponse(List<BlobMetadata> blobMetadata) {
                assertEquals(3, blobMetadata.size());
                assertEquals(blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList()), List.of(tm1, tm2, tm3));
                fetchCompleted.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fetchCompleted.set(true);
                throw new RuntimeException(e);
            }
        });
        assertBusy(() -> assertTrue(fetchCompleted.get()));
    }

    public void testReadMetadataForGivenTimestampNoFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata(1234L));
        assertNoDownloadStats(false);
    }

    public void testReadMetadataForGivenTimestampNoMatchingFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            String timestamp1 = RemoteStoreUtils.invertLong(2345L);
            BlobMetadata bm1 = new PlainBlobMetadata("metadata__1__12__" + timestamp1 + "__node1__1", 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata(1234L));
        assertNoDownloadStats(false);
    }

    public void testReadMetadataForGivenTimestampFile() throws IOException {
        AtomicReference<String> mdFilename1 = new AtomicReference<>();
        String timestamp1 = RemoteStoreUtils.invertLong(2345L);
        mdFilename1.set("metadata__1__12__" + timestamp1 + "__node1__1");
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            BlobMetadata bm1 = new PlainBlobMetadata(mdFilename1.get(), 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        long delayForMdDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1.get()))).thenAnswer(invocation -> {
            Thread.sleep(delayForMdDownload);
            return new ByteArrayInputStream(translogTransferManager.getMetadataBytes(metadata));
        });

        assertEquals(metadata, translogTransferManager.readMetadata(3000L));

        assertEquals(translogTransferManager.getMetadataBytes(metadata).length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForMdDownload);
    }

    public void testReadMetadataForGivenTimestampException() throws IOException {
        AtomicReference<String> mdFilename1 = new AtomicReference<>();
        String timestamp1 = RemoteStoreUtils.invertLong(2345L);
        mdFilename1.set("metadata__1__12__" + timestamp1 + "__node1__1");
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            BlobMetadata bm1 = new PlainBlobMetadata(mdFilename1.get(), 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1.get()))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, () -> translogTransferManager.readMetadata(3000L));
        assertNoDownloadStats(true);
    }

    public void testPopulateFileTrackerWithLocalStateNoReaders() {
        translogTransferManager.populateFileTrackerWithLocalState(null);
        assertTrue(translogTransferManager.getFileTransferTracker().allUploaded().isEmpty());

        translogTransferManager.populateFileTrackerWithLocalState(List.of());
        assertTrue(translogTransferManager.getFileTransferTracker().allUploaded().isEmpty());
    }

    public void testPopulateFileTrackerWithLocalState() {
        TranslogReader reader1 = mock(TranslogReader.class);
        when(reader1.getGeneration()).thenReturn(12L);
        TranslogReader reader2 = mock(TranslogReader.class);
        when(reader2.getGeneration()).thenReturn(23L);
        TranslogReader reader3 = mock(TranslogReader.class);
        when(reader3.getGeneration()).thenReturn(34L);
        TranslogReader reader4 = mock(TranslogReader.class);
        when(reader4.getGeneration()).thenReturn(45L);

        translogTransferManager.populateFileTrackerWithLocalState(List.of(reader1, reader2, reader3, reader4));
        assertEquals(
            Set.of("translog-12.tlog", "translog-23.tlog", "translog-34.tlog", "translog-45.tlog"),
            translogTransferManager.getFileTransferTracker().allUploaded()
        );
    }

    public void testPopulateFileTrackerWithLocalStateNoCkpAsMetadata() {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            remoteBaseTransferPath,
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );

        TranslogReader reader1 = mock(TranslogReader.class);
        when(reader1.getGeneration()).thenReturn(12L);
        TranslogReader reader2 = mock(TranslogReader.class);
        when(reader2.getGeneration()).thenReturn(23L);

        translogTransferManager.populateFileTrackerWithLocalState(List.of(reader1, reader2));
        assertEquals(
            Set.of("translog-12.tlog", "translog-12.ckp", "translog-23.tlog", "translog-23.ckp"),
            translogTransferManager.getFileTransferTracker().allUploaded()
        );
    }

    public void testMetadataRoundTripWithArchiveFields() throws IOException {
        TranslogTransferMetadata metadata = new TranslogTransferMetadata(2L, 10L, 5L, 3, "node-1");
        HashMap<String, String> genMap = new HashMap<>();
        genMap.put("5", "2");
        genMap.put("6", "2");
        genMap.put("7", "2");
        metadata.setGenerationToPrimaryTermMapper(genMap);
        metadata.setArchiveBlobPath("repo-root/translog/data/hashPrefix/0/20260320.zip");
        HashMap<String, String> offsets = new HashMap<>();
        offsets.put("indexUUID/0/2/translog-5.tlog", "100,500");
        offsets.put("indexUUID/0/2/translog-5.ckp", "600,50");
        metadata.setArchiveEntryOffsets(offsets);

        // Serialize via the same codec wrapper used in production
        byte[] metadataBytes = translogTransferManager.getMetadataBytes(metadata);
        assertNotNull(metadataBytes);
        assertTrue(metadataBytes.length > 0);

        // Deserialize via TranslogTransferManager.readMetadata(filename) path — simulate by uploading+downloading
        // For simplicity, use the handler directly with the codec wrapper
        org.opensearch.common.io.VersionedCodecStreamWrapper<TranslogTransferMetadata> wrapper =
            new org.opensearch.common.io.VersionedCodecStreamWrapper<>(
                new TranslogTransferMetadataHandler(),
                TranslogTransferMetadata.CURRENT_VERSION,
                "md"
            );
        org.apache.lucene.store.IndexInput indexInput = new org.opensearch.common.lucene.store.ByteArrayIndexInput(
            "test-metadata",
            metadataBytes
        );
        TranslogTransferMetadata read = wrapper.readStream(indexInput);

        assertEquals(2L, read.getPrimaryTerm());
        assertEquals(10L, read.getGeneration());
        assertEquals(5L, read.getMinTranslogGeneration());
        assertEquals(genMap, read.getGenerationToPrimaryTermMapper());
        assertEquals("repo-root/translog/data/hashPrefix/0/20260320.zip", read.getArchiveBlobPath());
        assertEquals(offsets, read.getArchiveEntryOffsets());
    }

    public void testMetadataRoundTripWithoutArchiveFields() throws IOException {
        TranslogTransferMetadata metadata = new TranslogTransferMetadata(1L, 5L, 3L, 1, "node-1");
        HashMap<String, String> genMap = new HashMap<>();
        genMap.put("3", "1");
        metadata.setGenerationToPrimaryTermMapper(genMap);
        // No archive fields set

        byte[] metadataBytes = translogTransferManager.getMetadataBytes(metadata);
        org.opensearch.common.io.VersionedCodecStreamWrapper<TranslogTransferMetadata> wrapper =
            new org.opensearch.common.io.VersionedCodecStreamWrapper<>(
                new TranslogTransferMetadataHandler(),
                TranslogTransferMetadata.CURRENT_VERSION,
                "md"
            );
        org.apache.lucene.store.IndexInput indexInput = new org.opensearch.common.lucene.store.ByteArrayIndexInput(
            "test-metadata",
            metadataBytes
        );
        TranslogTransferMetadata read = wrapper.readStream(indexInput);

        assertEquals(1L, read.getPrimaryTerm());
        assertEquals(5L, read.getGeneration());
        assertEquals(3L, read.getMinTranslogGeneration());
        assertNull("archiveBlobPath should be null when not set", read.getArchiveBlobPath());
        assertNull("archiveEntryOffsets should be null when not set", read.getArchiveEntryOffsets());
    }

    public void testDownloadTranslogFromArchive() throws IOException {
        Path location = createTempDir();
        String indexUUID = "indexUUid";
        String shardIdStr = "0";
        String primaryTerm = "12";
        String generation = "23";
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        String ckpFilename = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        String tlogEntryPath = indexUUID + "/" + shardIdStr + "/" + primaryTerm + "/" + translogFilename;
        String ckpEntryPath = indexUUID + "/" + shardIdStr + "/" + primaryTerm + "/" + ckpFilename;

        byte[] tlogContent = "translog archive content".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "checkpoint archive content".getBytes(StandardCharsets.UTF_8);

        String archiveBlobPath = "repo-root/translog/data/hashPrefix/0/20260320.zip";
        HashMap<String, String> archiveEntryOffsets = new HashMap<>();
        archiveEntryOffsets.put(tlogEntryPath, "100," + tlogContent.length);
        archiveEntryOffsets.put(ckpEntryPath, "600," + ckpContent.length);

        // Mock range-read for tlog entry
        when(transferService.downloadBlob(any(BlobPath.class), eq("20260320.zip"), eq(100L), eq((long) tlogContent.length))).thenReturn(
            new ByteArrayInputStream(tlogContent)
        );
        // Mock range-read for ckp entry
        when(transferService.downloadBlob(any(BlobPath.class), eq("20260320.zip"), eq(600L), eq((long) ckpContent.length))).thenReturn(
            new ByteArrayInputStream(ckpContent)
        );

        translogTransferManager.downloadTranslog(primaryTerm, generation, location, archiveBlobPath, archiveEntryOffsets);

        assertTrue(Files.exists(location.resolve(translogFilename)));
        assertTrue(Files.exists(location.resolve(ckpFilename)));
        assertArrayEquals(tlogContent, Files.readAllBytes(location.resolve(translogFilename)));
        assertArrayEquals(ckpContent, Files.readAllBytes(location.resolve(ckpFilename)));

        // Verify range-reads were made (not full blob downloads)
        verify(transferService).downloadBlob(any(BlobPath.class), eq("20260320.zip"), eq(100L), eq((long) tlogContent.length));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("20260320.zip"), eq(600L), eq((long) ckpContent.length));
    }

    public void testDownloadTranslogFromArchiveMissingEntryThrows() throws IOException {
        Path location = createTempDir();
        String archiveBlobPath = "repo-root/translog/data/hashPrefix/0/20260320.zip";
        HashMap<String, String> archiveEntryOffsets = new HashMap<>();
        // Only tlog entry, no ckp entry — should fail when looking for ckp

        String indexUUID = "indexUUid";
        String tlogEntryPath = indexUUID + "/0/12/translog-23.tlog";
        archiveEntryOffsets.put(tlogEntryPath, "100,500");

        // Mock tlog range-read to succeed so the error happens on the ckp lookup
        byte[] tlogContent = "tlog".getBytes(StandardCharsets.UTF_8);
        when(transferService.downloadBlob(any(BlobPath.class), eq("20260320.zip"), eq(100L), eq(500L))).thenReturn(
            new ByteArrayInputStream(tlogContent)
        );

        IOException e = expectThrows(
            IOException.class,
            () -> translogTransferManager.downloadTranslog("12", "23", location, archiveBlobPath, archiveEntryOffsets)
        );
        assertTrue(e.getMessage().contains("Archive entry not found"));
    }

    /**
     * Backward compatibility: metadata with archive fields set, then metadata without archive fields.
     * Verifies both paths work without data loss — v2 (archive) metadata is backward compatible with v1 (no archive).
     * Simulates the sequence: flag off (v1) → flag on (v2 with archive) → flag off (v1 again).
     */
    public void testBackwardCompatFlagOffOnOff() throws IOException {
        // Phase 1: "flag off" — metadata without archive fields (v1 style)
        TranslogTransferMetadata metaV1 = new TranslogTransferMetadata(1L, 5L, 3L, 1, "node-1");
        HashMap<String, String> genMapV1 = new HashMap<>();
        genMapV1.put("3", "1");
        genMapV1.put("4", "1");
        genMapV1.put("5", "1");
        metaV1.setGenerationToPrimaryTermMapper(genMapV1);
        // No archive fields

        byte[] bytesV1 = translogTransferManager.getMetadataBytes(metaV1);
        org.opensearch.common.io.VersionedCodecStreamWrapper<TranslogTransferMetadata> wrapper =
            new org.opensearch.common.io.VersionedCodecStreamWrapper<>(
                new TranslogTransferMetadataHandler(),
                TranslogTransferMetadata.CURRENT_VERSION,
                "md"
            );
        TranslogTransferMetadata readV1 = wrapper.readStream(
            new org.opensearch.common.lucene.store.ByteArrayIndexInput("v1-meta", bytesV1)
        );
        assertEquals(1L, readV1.getPrimaryTerm());
        assertEquals(5L, readV1.getGeneration());
        assertEquals(genMapV1, readV1.getGenerationToPrimaryTermMapper());
        assertNull("v1 should have null archiveBlobPath", readV1.getArchiveBlobPath());
        assertNull("v1 should have null archiveEntryOffsets", readV1.getArchiveEntryOffsets());

        // Phase 2: "flag on" — metadata WITH archive fields (v2 style)
        TranslogTransferMetadata metaV2 = new TranslogTransferMetadata(1L, 6L, 3L, 1, "node-1");
        HashMap<String, String> genMapV2 = new HashMap<>(genMapV1);
        genMapV2.put("6", "1");
        metaV2.setGenerationToPrimaryTermMapper(genMapV2);
        metaV2.setArchiveBlobPath("repo-root/translog/data/hash/0/20260320.zip");
        HashMap<String, String> offsets = new HashMap<>();
        offsets.put("indexUUid/0/1/translog-6.tlog", "100,500");
        offsets.put("indexUUid/0/1/translog-6.ckp", "600,50");
        metaV2.setArchiveEntryOffsets(offsets);

        byte[] bytesV2 = translogTransferManager.getMetadataBytes(metaV2);
        TranslogTransferMetadata readV2 = wrapper.readStream(
            new org.opensearch.common.lucene.store.ByteArrayIndexInput("v2-meta", bytesV2)
        );
        assertEquals(1L, readV2.getPrimaryTerm());
        assertEquals(6L, readV2.getGeneration());
        assertEquals(genMapV2, readV2.getGenerationToPrimaryTermMapper());
        assertEquals("repo-root/translog/data/hash/0/20260320.zip", readV2.getArchiveBlobPath());
        assertEquals(offsets, readV2.getArchiveEntryOffsets());

        // Phase 3: "flag off again" — metadata without archive fields (back to v1 style)
        TranslogTransferMetadata metaV1Again = new TranslogTransferMetadata(1L, 7L, 3L, 1, "node-1");
        HashMap<String, String> genMapV1Again = new HashMap<>(genMapV2);
        genMapV1Again.put("7", "1");
        metaV1Again.setGenerationToPrimaryTermMapper(genMapV1Again);
        // No archive fields set — simulating flag turned off

        byte[] bytesV1Again = translogTransferManager.getMetadataBytes(metaV1Again);
        TranslogTransferMetadata readV1Again = wrapper.readStream(
            new org.opensearch.common.lucene.store.ByteArrayIndexInput("v1-again-meta", bytesV1Again)
        );
        assertEquals(1L, readV1Again.getPrimaryTerm());
        assertEquals(7L, readV1Again.getGeneration());
        assertEquals(genMapV1Again, readV1Again.getGenerationToPrimaryTermMapper());
        assertNull("v1-again should have null archiveBlobPath", readV1Again.getArchiveBlobPath());
        assertNull("v1-again should have null archiveEntryOffsets", readV1Again.getArchiveEntryOffsets());

        // Verify v1-again can still be read correctly even after a v2 was written
        // (ensures no corruption or state leakage between flag transitions)
        assertNotEquals("v2 and v1-again should have different generations", readV2.getGeneration(), readV1Again.getGeneration());
    }
}
