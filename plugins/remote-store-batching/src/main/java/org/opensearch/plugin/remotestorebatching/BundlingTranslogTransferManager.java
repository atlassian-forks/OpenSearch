/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.RemoteStoreSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single-shard only: coalesces multiple concurrent {@link #transferSnapshot} calls into one blob write
 * instead of one blob per generation's files.
 * <p>
 * Known gaps, not solved here (see the class javadoc on {@link BatchingRemoteFsTranslog} for why): no durable
 * pointer is written, so {@code downloadTranslog}/recovery is unmodified and will not find files that only
 * exist inside a bundle blob after this JVM exits. This class demonstrates the write-side mechanism only,
 * proving the same subclass-based extension point that works for segments also works for translog. It is not
 * a production-ready implementation.
 */
public class BundlingTranslogTransferManager extends TranslogTransferManager {

    private static final Logger logger = LogManager.getLogger(BundlingTranslogTransferManager.class);
    private static final long BATCH_WINDOW_MILLIS = 20L;

    private final Object lock = new Object();
    private List<PendingUpload> pending = new ArrayList<>();
    private boolean flushScheduled = false;

    public BundlingTranslogTransferManager(
        ShardId shardId,
        TransferService transferService,
        BlobPath remoteDataTransferPath,
        BlobPath remoteMetadataTransferPath,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled
    ) {
        super(
            shardId,
            transferService,
            remoteDataTransferPath,
            remoteMetadataTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled
        );
    }

    private static final class PendingUpload {
        final TransferSnapshot snapshot;
        final TranslogTransferListener listener;
        final CountDownLatch doneLatch = new CountDownLatch(1);
        volatile boolean success;

        PendingUpload(TransferSnapshot snapshot, TranslogTransferListener listener) {
            this.snapshot = snapshot;
            this.listener = listener;
        }
    }

    @Override
    public boolean transferSnapshot(TransferSnapshot transferSnapshot, TranslogTransferListener translogTransferListener, CryptoMetadata cryptoMetadata)
        throws IOException {
        PendingUpload upload = new PendingUpload(transferSnapshot, translogTransferListener);
        boolean isLeader;
        synchronized (lock) {
            pending.add(upload);
            isLeader = flushScheduled == false;
            flushScheduled = true;
        }
        if (isLeader) {
            try {
                Thread.sleep(BATCH_WINDOW_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            List<PendingUpload> batch;
            synchronized (lock) {
                batch = pending;
                pending = new ArrayList<>();
                flushScheduled = false;
            }
            flushBatch(batch);
        }
        try {
            upload.doneLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return upload.success;
    }

    private void flushBatch(List<PendingUpload> batch) {
        try {
            List<SegmentBundleFormat.NamedContent> contents = new ArrayList<>();
            for (PendingUpload upload : batch) {
                for (TransferFileSnapshot f : upload.snapshot.getCheckpointFileSnapshots()) {
                    contents.add(wrap(f));
                }
                for (TransferFileSnapshot f : upload.snapshot.getTranslogFileSnapshots()) {
                    contents.add(wrap(f));
                }
            }
            byte[] bundleBytes = SegmentBundleFormat.build(contents);
            String blobName = "translog_bundle_" + UUIDs.base64UUID();

            CountDownLatch writeLatch = new CountDownLatch(1);
            AtomicReference<Exception> failure = new AtomicReference<>();
            transferService.uploadBlob(
                new ByteArrayInputStream(bundleBytes),
                remoteDataTransferPath,
                blobName,
                WritePriority.NORMAL,
                ActionListener.wrap(r -> writeLatch.countDown(), e -> {
                    failure.set(e);
                    writeLatch.countDown();
                })
            );
            writeLatch.await();
            logger.debug(
                "Bundled {} translog generation uploads into one blob {} ({} bytes)",
                batch.size(),
                blobName,
                bundleBytes.length
            );

            for (PendingUpload upload : batch) {
                notifyOne(upload, failure.get());
            }
        } catch (Exception e) {
            logger.warn("Exception while bundling translog uploads, failing this batch", e);
            for (PendingUpload upload : batch) {
                notifyOne(upload, e);
            }
        }
    }

    private static void notifyOne(PendingUpload upload, Exception failure) {
        try {
            if (failure == null) {
                upload.listener.onUploadComplete(upload.snapshot);
                upload.success = true;
            } else {
                upload.listener.onUploadFailed(upload.snapshot, failure);
                upload.success = false;
            }
        } catch (IOException e) {
            logger.warn("Exception notifying translog transfer listener", e);
            upload.success = false;
        } finally {
            upload.doneLatch.countDown();
        }
    }

    private static SegmentBundleFormat.NamedContent wrap(TransferFileSnapshot f) {
        return new SegmentBundleFormat.NamedContent() {
            @Override
            public String getName() {
                return f.getName();
            }

            @Override
            public byte[] getBytes() {
                try (java.io.InputStream in = f.inputStream()) {
                    return in.readAllBytes();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        };
    }
}
