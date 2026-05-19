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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.GcDecision;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    /** Node-level LRU cache for parsed TAR {@code _index} entries, shared across all recovery calls. */
    private final TranslogArchiveIndexCache indexCache;
    /**
     * Tracks the repository base path from the most recent upload() call.
     * Used by {@link #runArchiveGc} when no per-call basePath is available.
     * Null until the first upload occurs.
     */
    private volatile BlobPath lastKnownBasePath;

    public TarTranslogRemoteStoreStrategy(TranslogBatchCoordinator coordinator) {
        this(coordinator, new TranslogArchiveIndexCache());
    }

    TarTranslogRemoteStoreStrategy(TranslogBatchCoordinator coordinator, TranslogArchiveIndexCache indexCache) {
        this.coordinator = coordinator;
        this.indexCache = indexCache;
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

    /** Returns the last repository base path captured from an upload, or {@code null} if no upload has completed yet. */
    public BlobPath getLastKnownBasePath() {
        return lastKnownBasePath;
    }

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

    /**
     * Recovers a translog generation from archive TARs using the hierarchical path recovery
     * ({@link TranslogArchiveRecovery#recoverFromHierarchicalPath}).
     *
     * <p>Uses the node-level {@link TranslogArchiveIndexCache} to avoid re-reading TAR index
     * blobs when multiple shards recover from the same TAR (common during rolling restarts).
     *
     * <p>The {@code lastSegmentTimestamp} passed to recovery is {@code Instant.now()} because
     * at download time we don't have segment upload timestamps — the 2-minute safety margin in
     * {@link TranslogArchiveRecovery#RECOVERY_START_MARGIN_MINUTES} ensures we scan enough TARs.
     *
     * @return {@code true} if the generation was found and extracted from an archive TAR;
     *         {@code false} to signal that core should use the per-file fallback
     */
    @Override
    public boolean download(
        long primaryTerm,
        long generation,
        Path location,
        TransferService transferService,
        ShardId shardId,
        BlobPath repositoryBasePath
    ) throws IOException {
        // Capture basePath for GC (GC runs on the cluster-manager which may not be uploading).
        lastKnownBasePath = repositoryBasePath;

        return TranslogArchiveRecovery.recoverFromHierarchicalPath(
            transferService,
            repositoryBasePath,
            shardId.getIndex().getUUID(),
            shardId.id(),
            generation,
            generation,
            location,
            java.time.Instant.now(),
            indexCache
        );
    }

    static boolean extractGenerationFromTarStream(InputStream is, String tlogName, String ckpName, Path targetDir)
        throws IOException {
        final int BLOCK = TarArchiveBuilder.TAR_BLOCK;
        byte[] header = new byte[BLOCK];
        boolean tlogFound = false, ckpFound = false;
        while (true) {
            if (readFully(is, header) < BLOCK) break;
            boolean allZero = true;
            for (byte b : header) { if (b != 0) { allZero = false; break; } }
            if (allZero) break;
            int nameEnd = 0;
            while (nameEnd < 100 && header[nameEnd] != 0) nameEnd++;
            String entryName = new String(header, 0, nameEnd, java.nio.charset.StandardCharsets.US_ASCII);
            String baseName = entryName.contains("/") ? entryName.substring(entryName.lastIndexOf('/') + 1) : entryName;
            int sizeEnd = 136;
            while (sizeEnd > 124 && header[sizeEnd - 1] == 0) sizeEnd--;
            String sizeStr = new String(header, 124, sizeEnd - 124, java.nio.charset.StandardCharsets.US_ASCII).trim();
            long dataSize = sizeStr.isEmpty() ? 0 : Long.parseLong(sizeStr, 8);
            long paddedSize = ((dataSize + BLOCK - 1) / BLOCK) * BLOCK;
            if (baseName.equals(tlogName) || baseName.equals(ckpName)) {
                Path dest = targetDir.resolve(baseName);
                try (java.io.OutputStream out = Files.newOutputStream(dest)) {
                    byte[] buf = new byte[8192];
                    long remaining = dataSize;
                    while (remaining > 0) {
                        int r = is.read(buf, 0, (int) Math.min(buf.length, remaining));
                        if (r < 0) throw new java.io.EOFException();
                        out.write(buf, 0, r);
                        remaining -= r;
                    }
                }
                is.skip(paddedSize - dataSize);
                if (baseName.equals(tlogName)) tlogFound = true; else ckpFound = true;
                if (tlogFound && ckpFound) return true;
            } else {
                is.skip(paddedSize);
            }
        }
        return tlogFound && ckpFound;
    }

    private static int readFully(InputStream is, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int r = is.read(buf, total, buf.length - total);
            if (r < 0) return total;
            total += r;
        }
        return total;
    }

        public void runArchiveGc(BlobPath basePath, Duration retentionAge) throws IOException {
        // Prefer the explicitly passed basePath; fall back to the one captured from the last upload.
        BlobPath effectiveBase = (basePath != null) ? basePath : lastKnownBasePath;
        if (effectiveBase == null) {
            logger.debug("TAR translog archive GC: no basePath known yet (no upload has completed), skipping");
            return;
        }
        if (retentionAge == null || retentionAge.isNegative() || retentionAge.isZero()) {
            logger.warn("TAR translog archive GC: invalid retentionAge={}, skipping", retentionAge);
            return;
        }
        runTranslogGc(coordinator.getLastKnownTransferService(), effectiveBase, retentionAge, logger);
    }

    /**
     * Scans the txlog archive hierarchy and deletes TAR blobs (and empty parent dirs) older than
     * {@code retentionAge}. Package-private for unit testing.
     *
     * <p>Walk order: {@code txlog/ → yyyyMMdd/ → HHmm/ → *.tar}
     * <p>A blob is expired when {@code now - blobTimestamp > retentionAge}.
     * <p>After deleting all expired blobs in a minute-dir, the minute-dir itself is removed if empty.
     * After processing all minute-dirs in a day-dir, the day-dir is removed if empty.
     *
     * @param transferService blob store I/O
     * @param basePath        repository base path (txlog root is {@code basePath/txlog/})
     * @param retentionAge    blobs older than this are eligible for deletion
     * @param log             caller's logger
     */
    static void runTranslogGc(
        TransferService transferService,
        BlobPath basePath,
        Duration retentionAge,
        org.apache.logging.log4j.Logger log
    ) throws IOException {
        if (transferService == null) {
            log.debug("TAR translog archive GC: no TransferService available yet, skipping");
            return;
        }
        Instant cutoff = Instant.now().minus(retentionAge);
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(basePath);

        Set<String> dayDirs;
        try {
            dayDirs = transferService.listFolders(txlogRoot);
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            log.debug("TAR translog archive GC: txlog root not present, nothing to clean up");
            return;
        }

        for (String dayDir : dayDirs) {
            BlobPath dayPath = txlogRoot.add(dayDir);
            Set<String> minuteDirs;
            try {
                minuteDirs = transferService.listFolders(dayPath);
            } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                continue;
            }

            for (String minuteDir : minuteDirs) {
                BlobPath minutePath = dayPath.add(minuteDir);
                Set<String> blobs;
                try {
                    blobs = transferService.listAll(minutePath);
                } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                    continue;
                }

                // Delete expired TAR blobs
                List<String> toDelete = new ArrayList<>();
                for (String blob : blobs) {
                    if (!blob.endsWith(".tar")) continue;
                    TranslogArchivePathHelper.parseTarBlobTimestamp(dayDir, minuteDir, blob)
                        .filter(ts -> ts.isBefore(cutoff))
                        .ifPresent(ts -> {
                            toDelete.add(blob);
                            log.debug("TAR translog archive GC: deleting expired blob {}/{}/{} (ts={}, cutoff={})",
                                dayDir, minuteDir, blob, ts, cutoff);
                        });
                }
                if (!toDelete.isEmpty()) {
                    transferService.deleteBlobs(minutePath, toDelete);
                }

                // Remove empty minute-dir (best-effort)
                try {
                    Set<String> remaining = transferService.listAll(minutePath);
                    if (remaining.isEmpty()) {
                        // Some stores support directory deletion via an empty delete; try deleting the sentinel
                        transferService.deleteBlobs(dayPath, List.of(minuteDir));
                    }
                } catch (Exception ignored) {
                    // Directory cleanup is best-effort; ignore errors
                }
            }

            // Remove empty day-dir (best-effort)
            try {
                Set<String> remainingMinutes = transferService.listFolders(dayPath);
                if (remainingMinutes.isEmpty()) {
                    transferService.deleteBlobs(txlogRoot, List.of(dayDir));
                }
            } catch (Exception ignored) {
                // Best-effort
            }
        }
        log.debug("TAR translog archive GC: scan complete (cutoff={})", cutoff);
    }
}
