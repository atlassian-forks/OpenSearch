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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.archive.ArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.ArchiveCommentFormat;
import org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Node-level collector for translog archive upload. Runs a scheduled task at buffer_interval;
 * collects from eligible shards that have pending data, builds one ZIP (stored) per batch, uploads one blob.
 * <p>
 * <b>Archive path layout</b>: Blobs live at {@code repoBasePath/translog/data/{hashTypeIndex}/{hashNodeId}/{yyyyMMddHHmmssSSS}.zip}.
 * {@code repoBasePath} is the repository root (no index-UUID or shard-ID components); archives are index+node scoped.
 * Inside each ZIP, member paths are {@code indexUUID/shardId/primaryTerm/translog-<gen>.tlog} or {@code .ckp}.
 * See {@link org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper} for path parsing and retention.
 * <p>
 * <b>Retention</b>: {@link #deleteArchivesOlderThanRetention} deletes archive ZIPs older than the configured retention age.
 * is past the shard's retention bounds (no partial delete).
 * <p>
 * <b>Memory</b>: Peak memory for an archive upload is proportional to the total size of all primary shards'
 * translog and checkpoint files in the batch, because {@code snapshotToEntries} and the archive build hold
 * entry content in memory.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchiveCollector extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveCollector.class);

    /** Max archive blobs to consider per node per retention run; pagination re-lists until fewer returned. */
    private static final int MAX_ARCHIVE_BLOBS_PER_NODE = 500;
    /** Max blob names to pass per deleteBlobs call when batching retention deletes. */
    private static final int RETENTION_DELETE_BATCH_SIZE = 100;
    /** Skip parsing ZIPs whose blob name timestamp is newer than this (retention hint). */
    private static final int RETENTION_SKIP_ZIPS_NEWER_THAN_MINUTES = 60;
    /** File type in hash input for translog archive path. */
    private static final String TRANSLOG_ARCHIVE_FILE_TYPE = "translog_zip";
    /** Gen bucket size; path is translog/data/{hashPrefix}/{genBucket}. */
    public static final int GEN_BUCKET_SIZE = 100;

    /** Locks for serializing upload per (indexUUID, nodeId). Key: indexUUID + "|" + nodeId. */
    private static final ConcurrentHashMap<String, Object> UPLOAD_LOCKS = new ConcurrentHashMap<>();

    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final RemoteStoreSettings remoteStoreSettings;
    private volatile Scheduler.Cancellable scheduledTask;
    /** Set of index UUIDs that use coordinator-based (school bus) uploads. */
    private final Set<String> coordinatorEnabledIndices = ConcurrentHashMap.newKeySet();

    public TranslogArchiveCollector(IndicesService indicesService) {
        this(indicesService, null, null);
    }

    public TranslogArchiveCollector(IndicesService indicesService, ThreadPool threadPool, RemoteStoreSettings remoteStoreSettings) {
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    /**
     * Shard IDs on this node that have remote translog and archive upload enabled.
     */
    public List<ShardId> getEligibleShardIds() {
        List<ShardId> out = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            if (indexService.getIndexSettings().isRemoteTranslogStoreEnabled() == false) {
                continue;
            }
            if (indexService.getIndexSettings().isTranslogArchiveUploadEnabled() == false) {
                continue;
            }
            for (int i = 0; i < indexService.getIndexSettings().getNumberOfShards(); i++) {
                IndexShard shard = indexService.getShardOrNull(i);
                if (shard != null && shard.isRemoteTranslogEnabled()) {
                    out.add(shard.shardId());
                }
            }
        }
        return out;
    }

    /**
     * Build a ZIP (stored) archive from the given entries; returns bytes for tests.
     */
    public byte[] buildArchiveFromEntries(Iterable<ArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.build(out, entries);
        return out.toByteArray();
    }

    /** Maximum bytes for all entries from a single snapshot to prevent OOM. */
    private static final long MAX_SNAPSHOT_ARCHIVE_BYTES = 128 * 1024 * 1024L;

    /**
     * Convert a transfer snapshot to archive entries (path = pathPrefix/primaryTerm/name). Reads file content into memory.
     *
     * @throws IOException if total content exceeds MAX_SNAPSHOT_ARCHIVE_BYTES
     */
    public static List<ArchiveBuilder.ArchiveBuildEntry> snapshotToEntries(TransferSnapshot snapshot, String pathPrefix)
        throws IOException {
        long primaryTerm = snapshot.getTranslogTransferMetadata().getPrimaryTerm();
        String prefix = pathPrefix + "/" + primaryTerm + "/";
        List<ArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        long totalBytes = 0;
        for (FileSnapshot.TransferFileSnapshot file : snapshot.getTranslogFileSnapshotWithMetadata()) {
            totalBytes += file.getContentLength();
            if (totalBytes > MAX_SNAPSHOT_ARCHIVE_BYTES) {
                throw new IOException("Snapshot archive size " + totalBytes + " exceeds limit " + MAX_SNAPSHOT_ARCHIVE_BYTES);
            }
            entries.add(streamEntry(prefix + file.getName(), file));
        }
        for (FileSnapshot.TransferFileSnapshot file : snapshot.getCheckpointFileSnapshots()) {
            totalBytes += file.getContentLength();
            if (totalBytes > MAX_SNAPSHOT_ARCHIVE_BYTES) {
                throw new IOException("Snapshot archive size " + totalBytes + " exceeds limit " + MAX_SNAPSHOT_ARCHIVE_BYTES);
            }
            entries.add(streamEntry(prefix + file.getName(), file));
        }
        return entries;
    }

    private static ArchiveBuilder.ArchiveBuildEntry streamEntry(String path, FileSnapshot.TransferFileSnapshot file) throws IOException {
        long size = file.getContentLength();
        byte[] content = new byte[(int) size];
        try (InputStream in = file.inputStream()) {
            int total = 0;
            while (total < size) {
                int r = in.read(content, total, (int) (size - total));
                if (r <= 0) break;
                total += r;
            }
            if (total != size) {
                throw new IOException("Expected " + size + " bytes, got " + total);
            }
        }
        return ArchiveBuilder.fromBytes(path, content);
    }

    @Override
    protected void doStart() {
        if (threadPool != null && remoteStoreSettings != null) {
            TimeValue interval = remoteStoreSettings.getClusterRemoteTranslogBufferInterval();
            scheduledTask = threadPool.scheduleWithFixedDelay(this::runBatch, interval, ThreadPool.Names.TRANSLOG_TRANSFER);
        }
    }

    @Override
    protected void doStop() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
        }
    }

    @Override
    protected void doClose() {}

    private void runBatch() {
        runArchiveRetention();
        // When the school-bus batch coordinator is active, archive uploads are handled inline
        // with the translog sync flow (via TranslogArchiveBatchCoordinator). The collector only
        // performs retention cleanup. Skip the upload path to avoid double uploads.
        if (isCoordinatorBasedUploadEnabled()) {
            return;
        }
        List<ShardId> eligible = getEligibleShardIds();
        if (eligible.isEmpty()) {
            return;
        }
        List<IndexShard> shardsWithPending = new ArrayList<>();
        for (ShardId sid : eligible) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard != null && shard.routingEntry() != null && shard.routingEntry().primary() && shard.isSyncNeeded()) {
                shardsWithPending.add(shard);
            }
        }
        if (shardsWithPending.isEmpty()) {
            return;
        }
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = remoteStoreSettings != null
            ? remoteStoreSettings.getPathHashAlgorithm()
            : RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;

        Map<String, List<IndexShard>> byIndex = new HashMap<>();
        for (IndexShard shard : shardsWithPending) {
            String indexUUID = shard.indexSettings().getIndexMetadata().getIndexUUID();
            byIndex.computeIfAbsent(indexUUID, k -> new ArrayList<>()).add(shard);
        }

        for (Map.Entry<String, List<IndexShard>> e : byIndex.entrySet()) {
            String indexUUID = e.getKey();
            List<IndexShard> indexShards = e.getValue();
            Optional<TranslogTransferManager> transferManagerOpt = indexShards.get(0).getTranslogTransferManager();
            if (transferManagerOpt.isEmpty()) continue;
            TranslogTransferManager transferManager = transferManagerOpt.get();
            String nodeId = indexShards.get(0).getTranslogNodeId().orElse("unknown");
            String lockKey = indexUUID + "|" + nodeId;
            Object lock = UPLOAD_LOCKS.computeIfAbsent(lockKey, k -> new Object());
            synchronized (lock) {
                runBatchForIndex(
                    transferManager.getTransferService(),
                    transferManager.getArchiveBasePath(),
                    indexUUID,
                    nodeId,
                    indexShards,
                    pathHashAlgorithm
                );
            }
        }
    }

    private void runBatchForIndex(
        TransferService transferService,
        BlobPath basePath,
        String indexUUID,
        String nodeId,
        List<IndexShard> shardsWithPending,
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm
    ) {
        List<TransferSnapshot> snapshots = new ArrayList<>();
        List<Runnable> releases = new ArrayList<>();
        List<String> pathPrefixes = new ArrayList<>();
        List<IndexShard> contributingShards = new ArrayList<>();
        collectSnapshotsFromShards(shardsWithPending, snapshots, releases, pathPrefixes, contributingShards);
        if (snapshots.isEmpty()) {
            return;
        }
        String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(indexUUID, pathHashAlgorithm);
        String hashNodeId = TranslogArchivePathHelper.hashNodeId(nodeId, pathHashAlgorithm);
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        try {
            for (int i = 0; i < snapshots.size(); i++) {
                allEntries.addAll(snapshotToEntries(snapshots.get(i), pathPrefixes.get(i)));
            }
            uploadArchiveNewPathAndMetadata(
                transferService,
                basePath,
                hashTypeIndex,
                hashNodeId,
                snapshots,
                contributingShards,
                allEntries
            );
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Failed to build or upload translog archive for index {}", indexUUID), ex);
            runFallbackIfEnabled(contributingShards, snapshots);
        } finally {
            releaseSnapshots(snapshots, releases);
        }
    }

    /**
     * Runs archive retention: collects retention bounds from eligible shards and deletes archive blobs
     * that are fully past retention. Uses the same timer as runBatch (no separate schedule).
     */
    private void runArchiveRetention() {
        List<ShardId> eligible = getEligibleShardIds();
        if (eligible.isEmpty()) {
            return;
        }
        // Collect one representative shard per index for retention bounds and transfer manager.
        // Key: indexUUID → (retentionBounds, transferManager, nodeId)
        Map<String, ArchiveDeletionHelper.RetentionBounds> retentionByIndex = new HashMap<>();
        Map<String, TranslogTransferManager> transferManagerByIndex = new HashMap<>();
        Map<String, String> nodeIdByIndex = new HashMap<>();

        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = remoteStoreSettings != null
            ? remoteStoreSettings.getPathHashAlgorithm()
            : RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;

        for (ShardId sid : eligible) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) continue;
            String indexUUID = shard.indexSettings().getIndexMetadata().getIndexUUID();
            shard.getArchiveRetentionBounds().ifPresent(bounds -> retentionByIndex.put(indexUUID, bounds));
            if (!transferManagerByIndex.containsKey(indexUUID)) {
                shard.getTranslogTransferManager().ifPresent(tm -> {
                    transferManagerByIndex.put(indexUUID, tm);
                    shard.getTranslogNodeId().ifPresent(nid -> nodeIdByIndex.put(indexUUID, nid));
                });
            }
        }
        if (retentionByIndex.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ArchiveDeletionHelper.RetentionBounds> e : retentionByIndex.entrySet()) {
            String indexUUID = e.getKey();
            ArchiveDeletionHelper.RetentionBounds bounds = e.getValue();
            TranslogTransferManager tm = transferManagerByIndex.get(indexUUID);
            String nodeId = nodeIdByIndex.get(indexUUID);
            if (tm == null || nodeId == null) continue;
            String hashTypeIndex = TranslogArchivePathHelper.hashTypeIndex(indexUUID, pathHashAlgorithm);
            String hashNodeId = TranslogArchivePathHelper.hashNodeId(nodeId, pathHashAlgorithm);
            BlobPath zipDir = tm.getArchiveBasePath().add("translog").add("data").add(hashTypeIndex).add(hashNodeId);
            try {
                deleteArchivesOlderThanRetention(tm.getTransferService(), zipDir, bounds);
            } catch (IOException ex) {
                logger.warn("Archive retention delete failed for index {}: {}", indexUUID, ex.getMessage());
            }
        }
    }

    /**
     * Deletes archive ZIPs under {@code zipDir} that are older than the configured retention age.
     * Uses timestamp-only deletion (parsed from blob name) — no range-reads or generation checks needed.
     * The retention age minimum (5 min) ensures no live ZIP is deleted prematurely.
     *
     * <p>Path: {@code translog/data/{hashTypeIndex}/{hashNodeId}/{yyyyMMddHHmmssSSS}.zip}
     *
     * @param transferService blob transfer service
     * @param zipDir          direct path to the node's ZIP directory (no intermediate LISTs needed)
     * @param bounds          retention bounds containing the effective retention age
     * @return number of ZIPs deleted
     */
    static int deleteArchivesOlderThanRetention(
        TransferService transferService,
        BlobPath zipDir,
        ArchiveDeletionHelper.RetentionBounds bounds
    ) throws IOException {
        long effectiveRetentionMinutes = bounds.getEffectiveRetentionMinutes();
        Instant retentionCutoff = Instant.now().minus(Duration.ofMinutes(effectiveRetentionMinutes));
        int deleted = 0;
        boolean morePages;
        do {
            List<BlobMetadata> blobs;
            try {
                blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                    f -> transferService.listAllInSortedOrder(zipDir, "", MAX_ARCHIVE_BLOBS_PER_NODE, f)
                );
            } catch (IOException e) {
                logger.warn("List archive ZIPs failed at {}: {}", zipDir.buildAsString(), e.getMessage());
                break;
            }
            if (blobs == null || blobs.isEmpty()) {
                break;
            }
            List<String> toDelete = new ArrayList<>();
            for (BlobMetadata blob : blobs) {
                String name = blob.name();
                if (name == null || !name.endsWith(".zip")) {
                    continue;
                }
                // Delete if blob timestamp < retentionCutoff (i.e. NOT after cutoff)
                boolean withinRetention = TranslogArchivePathHelper.parseBlobNameTimestamp(name)
                    .filter(ts -> ts.isAfter(retentionCutoff))
                    .isPresent();
                if (!withinRetention) {
                    toDelete.add(name);
                }
            }
            for (int i = 0; i < toDelete.size(); i += RETENTION_DELETE_BATCH_SIZE) {
                int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, toDelete.size());
                List<String> batch = toDelete.subList(i, end);
                try {
                    transferService.deleteBlobs(zipDir, new ArrayList<>(batch));
                    deleted += batch.size();
                    logger.debug(
                        "Deleted {} archive ZIPs older than {} minutes from {}",
                        batch.size(),
                        effectiveRetentionMinutes,
                        zipDir.buildAsString()
                    );
                } catch (IOException e) {
                    logger.warn(
                        "Failed to delete archive batch ({} blobs) from {}: {}",
                        batch.size(),
                        zipDir.buildAsString(),
                        e.getMessage()
                    );
                }
            }
            morePages = blobs.size() >= MAX_ARCHIVE_BLOBS_PER_NODE;
        } while (morePages);
        return deleted;
    }

    private void collectSnapshotsFromShards(
        List<IndexShard> shardsWithPending,
        List<TransferSnapshot> snapshots,
        List<Runnable> releases,
        List<String> pathPrefixes,
        List<IndexShard> contributingShards
    ) {
        for (IndexShard shard : shardsWithPending) {
            if (shard.supportsArchiveSnapshot() == false) {
                continue;
            }
            String pathPrefix = shard.indexSettings().getIndexMetadata().getIndexUUID() + "/" + shard.shardId().id();
            try {
                shard.buildSnapshotForArchive((snapshot, release) -> {
                    snapshots.add(snapshot);
                    releases.add(release);
                    pathPrefixes.add(pathPrefix);
                    contributingShards.add(shard);
                });
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Failed to build snapshot for archive from shard {}", shard.shardId()), e);
            }
        }
    }

    private static final int PIPE_BUFFER_BYTES = 256 * 1024;
    /** Max attempts for archive upload on blob name collision or transient failure. */
    private static final int UPLOAD_RETRY_MAX_ATTEMPTS = 3;
    /** Sleep between upload retries (ms). */
    private static final int UPLOAD_RETRY_SLEEP_MS = 25;

    private void uploadArchiveNewPathAndMetadata(
        TransferService transferService,
        BlobPath basePath,
        String hashTypeIndex,
        String hashNodeId,
        List<TransferSnapshot> snapshots,
        List<IndexShard> contributingShards,
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries
    ) throws IOException {
        // Path: translog/data/{hashTypeIndex}/{hashNodeId}/{timestamp}.zip
        // hashTypeIndex = hash("translog_zip|{indexUUID}") — same for all nodes, used for per-index GC
        // hashNodeId = hash(nodeId) — unique per node, isolates writes for S3 partition safety
        BlobPath archivePath = basePath.add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

        // Pre-compute size and capture per-entry offsets for archive-based recovery metadata.
        // Since the ZIP uses STORED (no compression), offsets are deterministic across builds.
        ArchiveBuilder.SizeAndOffsets sizeAndOffsets = ArchiveBuilder.computeSizeAndOffsetsWithComment(allEntries);
        List<ArchiveCommentFormat.PathOffsetLength> entryOffsets = sizeAndOffsets.getOffsets();

        AtomicReference<String> uploadedBlobName = new AtomicReference<>();
        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            String blobName = TranslogArchivePathHelper.blobNameFromCurrentTime();
            long contentLength = sizeAndOffsets.getSize();
            try (PipedOutputStream pos = new PipedOutputStream(); PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)) {
                AtomicReference<IOException> uploadError = new AtomicReference<>();
                java.util.concurrent.CountDownLatch uploadLatch = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "translog-archive-upload");
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
                try {
                    ArchiveBuilder.buildWithComment(pos, allEntries);
                } finally {
                    pos.close();
                }
                try {
                    uploadLatch.await();
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for archive upload", e);
                }
                executor.shutdown();
                if (uploadError.get() != null) {
                    lastFailure = uploadError.get();
                    if (attempt < UPLOAD_RETRY_MAX_ATTEMPTS - 1) {
                        try {
                            Thread.sleep(UPLOAD_RETRY_SLEEP_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted during upload retry", e);
                        }
                        continue;
                    }
                    throw lastFailure;
                }
            } catch (IOException e) {
                lastFailure = e;
                if (attempt < UPLOAD_RETRY_MAX_ATTEMPTS - 1) {
                    try {
                        Thread.sleep(UPLOAD_RETRY_SLEEP_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during upload retry", ie);
                    }
                    continue;
                }
                throw e;
            }
            logger.debug(
                "Archive uploaded path={} hashTypeIndex={} hashNodeId={} blob={}",
                archivePath.buildAsString(),
                hashTypeIndex,
                hashNodeId,
                blobName
            );
            uploadedBlobName.set(blobName);
            break;
        }
        // Build archive entry offsets map for recovery metadata: entryPath → "offset,length"
        Map<String, String> archiveEntryOffsetsMap = new HashMap<>();
        for (ArchiveCommentFormat.PathOffsetLength pol : entryOffsets) {
            archiveEntryOffsetsMap.put(pol.getPath(), pol.getOffset() + "," + pol.getLength());
        }
        String fullArchiveBlobPath = archivePath.buildAsString() + uploadedBlobName.get();

        int metadataSuccessCount = 0;
        for (int i = 0; i < contributingShards.size(); i++) {
            final int shardIndex = i;
            Optional<TranslogTransferManager> managerOpt = contributingShards.get(i).getTranslogTransferManager();
            if (managerOpt.isPresent()) {
                try {
                    // Populate archive location in metadata for archive-based recovery
                    snapshots.get(i).getTranslogTransferMetadata().setArchiveBlobPath(fullArchiveBlobPath);
                    snapshots.get(i).getTranslogTransferMetadata().setArchiveEntryOffsets(archiveEntryOffsetsMap);
                    managerOpt.get().uploadMetadata(snapshots.get(i));
                    metadataSuccessCount++;
                } catch (IOException e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Failed to upload metadata for archive snapshot from shard {}",
                            contributingShards.get(shardIndex).shardId()
                        ),
                        e
                    );
                }
            }
        }

        // If no metadata was uploaded successfully, the ZIP blob is orphaned — attempt cleanup.
        if (metadataSuccessCount == 0 && uploadedBlobName.get() != null) {
            logger.warn("All metadata uploads failed; attempting to delete orphaned archive blob {}", uploadedBlobName.get());
            try {
                transferService.deleteBlobs(archivePath, java.util.Collections.singletonList(uploadedBlobName.get()));
                logger.info("Deleted orphaned archive blob {}", uploadedBlobName.get());
            } catch (IOException deleteEx) {
                logger.warn("Failed to delete orphaned archive blob {}: {}", uploadedBlobName.get(), deleteEx.getMessage());
            }
        }
    }

    private void runFallbackIfEnabled(List<IndexShard> contributingShards, List<TransferSnapshot> snapshots) {
        if (remoteStoreSettings == null || remoteStoreSettings.getTranslogArchiveFallbackToPerShard() == false) {
            return;
        }
        TranslogTransferListener noOpListener = new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                logger.warn(() -> new ParameterizedMessage("Fallback per-shard upload failed"), ex);
            }
        };
        for (int i = 0; i < contributingShards.size(); i++) {
            final int shardIndex = i;
            Optional<TranslogTransferManager> managerOpt = contributingShards.get(i).getTranslogTransferManager();
            if (managerOpt.isPresent()) {
                try {
                    managerOpt.get().transferSnapshot(snapshots.get(i), noOpListener);
                } catch (IOException io) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Fallback per-shard upload failed for shard {}",
                            contributingShards.get(shardIndex).shardId()
                        ),
                        io
                    );
                }
            }
        }
    }

    private void releaseSnapshots(List<TransferSnapshot> snapshots, List<Runnable> releases) {
        for (int i = 0; i < snapshots.size(); i++) {
            try {
                if (snapshots.get(i) instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) snapshots.get(i)).close();
                    } catch (Exception e) {
                        logger.warn(() -> new ParameterizedMessage("Failed to close snapshot"), e);
                    }
                }
            } finally {
                releases.get(i).run();
            }
        }
    }

    /**
     * Register an index as using coordinator-based (school bus) upload.
     * When registered, the collector skips upload for that index — only retention runs.
     */
    public void registerCoordinatorIndex(String indexUUID) {
        coordinatorEnabledIndices.add(indexUUID);
    }

    /**
     * Unregister an index from coordinator-based upload (e.g. on index deletion).
     */
    public void unregisterCoordinatorIndex(String indexUUID) {
        coordinatorEnabledIndices.remove(indexUUID);
    }

    /**
     * Returns true if the coordinator-based (school bus) upload model is active for any index.
     * When true, the collector only runs retention — uploads are handled inline by
     * {@link TranslogArchiveBatchCoordinator} in the sync path.
     */
    private boolean isCoordinatorBasedUploadEnabled() {
        return !coordinatorEnabledIndices.isEmpty();
    }

    /**
     * Runs one batch synchronously; for unit tests only.
     */
    void runBatchForTesting() {
        runBatch();
    }
}
