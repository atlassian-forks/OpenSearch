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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.index.remote.GcDecision;
// TranslogBatchCoordinator is in same plugin package
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.TransferSnapshot;
// TranslogArchivePathHelper is in same plugin package
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
// TranslogShardBatch is in same plugin package
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * TAR-based translog remote store strategy using node-level batch upload.
 *
 * <p>Implements the "shared taxi" model: instead of uploading one blob per shard per sync,
 * each shard's translog files are packaged as a {@link TranslogShardBatch} and submitted to
 * the node-scoped {@link TranslogBatchCoordinator}. The coordinator waits for additional
 * shards (up to the configured threshold or max-wait timeout) and then dispatches a single TAR
 * blob containing all pending shards' translog files in one S3 PUT.
 *
 * <p>This class owns all TAR format knowledge — the coordinator itself is TAR-agnostic.
 *
 * @see TranslogBatchCoordinator
 * @see TranslogBatchCollector
 */
public class TarTranslogRemoteStoreStrategy implements TranslogRemoteStoreStrategy {

    private static final Logger logger = LogManager.getLogger(TarTranslogRemoteStoreStrategy.class);

    private static final int PIPE_BUFFER_BYTES = 256 * 1024;

    private final TranslogBatchCoordinator coordinator;
    public TarTranslogRemoteStoreStrategy(TranslogBatchCoordinator coordinator) {
        this.coordinator = coordinator;
    }



    // ── TranslogRemoteStoreStrategy: batch model ─────────────────────────────

        public boolean supportsNodeBatching() {
        return true;
    }

    /**
     * Packages this shard's translog snapshot into a {@link TranslogShardBatch} and submits
     * it to the node-level coordinator. Blocks until the coordinator's batch is uploaded
     * (or fails), then notifies the listener.
     */
    @Override
    public boolean upload(TransferSnapshot snapshot, TranslogTransferListener listener, TransferService transferService,
            ShardId shardId, BlobPath repositoryBasePath) throws IOException {
        List<TranslogShardBatch.BatchFile> files = new ArrayList<>();
        String iUUID = shardId.getIndex().getUUID();
        int sId = shardId.id();

        long primaryTerm = snapshot.getTranslogTransferMetadata().getPrimaryTerm();

        // Collect tlog files
        for (var snap : snapshot.getTranslogFileSnapshots()) {
            String remoteName = iUUID + "/" + sId + "/" + primaryTerm + "/" + snap.getName();
            files.add(new TranslogShardBatch.BatchFile(snap.getPath(), remoteName, snap.getContentLength()));
        }
        // Collect checkpoint files
        for (var snap : snapshot.getCheckpointFileSnapshots()) {
            String remoteName = iUUID + "/" + sId + "/" + primaryTerm + "/" + snap.getName();
            files.add(new TranslogShardBatch.BatchFile(snap.getPath(), remoteName, snap.getContentLength()));
        }

        if (files.isEmpty()) {
            listener.onUploadComplete(snapshot);
            return true;
        }

        var meta = snapshot.getTranslogTransferMetadata();
        TranslogShardBatch batch = new TranslogShardBatch(
            iUUID, sId,
            meta.getPrimaryTerm(), meta.getGeneration(), meta.getMinTranslogGeneration(),
            files
        );

        try {
            coordinator.submitAndWait(batch, transferService, repositoryBasePath);
            listener.onUploadComplete(snapshot);
            return true;
        } catch (IOException e) {
            listener.onUploadFailed(snapshot, e);
            return false;
        }
    }

    /**
     * Builds a TAR archive from all shard batches and uploads it as a single blob.
     * Called by the coordinator when the batch window closes.
     *
     * <p>Uses a {@link PipedInputStream}/{@link PipedOutputStream} pair to stream the TAR
     * bytes directly to the upload thread — no full-batch in-memory buffering.
     *
     * @param batch           the per-shard snapshots assembled for this batch
     * @param basePath        the repository base path for constructing the archive blob path
     * @param transferService the shard's transfer service (passed by coordinator; all shards share a repo)
     */
        public void uploadBatch(List<TranslogShardBatch> batch, BlobPath basePath, TransferService transferService) throws IOException {
        // Build all ArchiveBuildEntry objects from the batch
        List<TarArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        List<TarArchiveBuilder.GcShardEntry> gcEntries = new ArrayList<>();

        for (TranslogShardBatch shardBatch : batch) {
            for (TranslogShardBatch.BatchFile file : shardBatch.getFiles()) {
                Path localPath = file.getLocalPath();
                long sizeBytes = file.getSizeBytes();
                String remoteName = file.getRemoteName();
                allEntries.add(TarArchiveBuilder.fromPath(remoteName, localPath, sizeBytes));
            }
            gcEntries.add(new TarArchiveBuilder.GcShardEntry(
                shardBatch.getIndexUUID(),
                shardBatch.getShardId(),
                shardBatch.getMinSeqNo(),
                shardBatch.getMaxSeqNo(),
                shardBatch.getGlobalCheckpoint()
            ));
        }

        if (allEntries.isEmpty()) {
            logger.debug("TAR translog batch: no entries, skipping upload");
            return;
        }

        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries, gcEntries);
        long contentLength = layout.getTotalSize();

        Instant uploadInstant = Instant.now();
        BlobPath archivePath = TranslogArchivePathHelper.tarBlobDir(basePath, uploadInstant);
        String blobName = TranslogArchivePathHelper.tarBlobName(uploadInstant, coordinator.getNodeId());

        // Stream the TAR bytes via pipe into the upload thread — no full-batch buffering.
        final IOException[] uploadError = { null };

        try (
            PipedOutputStream pos = new PipedOutputStream();
            PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)
        ) {
            PlainActionFuture<Void> uploadFuture = PlainActionFuture.newFuture();

            Thread uploadThread = new Thread(() -> {
                try {
                    transferService.uploadBlob(pis, archivePath, blobName, WritePriority.HIGH, uploadFuture);
                } catch (IOException e) {
                    uploadError[0] = e;
                    uploadFuture.onFailure(e);
                }
            }, "tar-translog-upload");
            uploadThread.setDaemon(true);
            uploadThread.start();

            // Write TAR bytes into the pipe (blocks when pipe buffer is full — natural backpressure)
            TarArchiveBuilder.build(pos, layout, allEntries);
            pos.close();

            // Wait for upload to complete
            try {
                uploadFuture.get();
            } catch (Exception e) {
                throw new IOException("TAR translog upload failed: " + blobName, e.getCause() != null ? e.getCause() : e);
            }
        }

        if (uploadError[0] != null) {
            throw uploadError[0];
        }

        logger.debug("TAR translog batch uploaded: path={} blob={} shards={} entries={} size={}",
            archivePath.buildAsString(), blobName, batch.size(), allEntries.size(), contentLength);
    }

    // ── Per-shard lifecycle methods (download + GC) ───────────────────────────

    @Override
    public boolean download(long primaryTerm, long generation, Path location) throws IOException {
        // TODO: implement range-GET from archive TAR blob
        // For now, fall back to default (per-file) download
        return false;
    }

    @Override
    public GcDecision resolveStaleTranslogBlobs(long minPrimaryTerm, long minGeneration) throws IOException {
        // Archive GC is handled by runArchiveGc() at the node level (cluster-manager only).
        // Suppress per-shard LIST calls entirely.
        return GcDecision.SKIP;
    }

        public void runArchiveGc(BlobPath basePath, Duration retentionAge) throws IOException {
        // TODO: port TranslogArchiveGcScanner from feature branch to handle timestamp-based
        // deletion of expired minute-directories under basePath/txlog/
        logger.debug("TAR translog archive GC: retention={} (not yet implemented)", retentionAge);
    }
}
