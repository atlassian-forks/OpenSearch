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
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
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
import java.util.HashSet;
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
    /** Separate scheduled task for archive retention GC — runs at retention interval, not buffer interval. */
    private volatile Scheduler.Cancellable retentionScheduledTask;
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
            TimeValue uploadInterval = remoteStoreSettings.getClusterRemoteTranslogBufferInterval();
            scheduledTask = threadPool.scheduleWithFixedDelay(this::runBatch, uploadInterval, ThreadPool.Names.TRANSLOG_TRANSFER);
            // Retention GC runs on a separate schedule (default 1 min, configurable via
            // cluster.remote_store.translog.archive.gc_interval) to avoid expensive S3 LIST
            // calls every buffer_interval (650ms).
            TimeValue retentionInterval = remoteStoreSettings.getTranslogArchiveGcInterval();
            retentionScheduledTask = threadPool.scheduleWithFixedDelay(
                this::runArchiveRetention,
                retentionInterval,
                ThreadPool.Names.TRANSLOG_TRANSFER
            );
        }
    }

    @Override
    protected void doStop() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
        }
        if (retentionScheduledTask != null) {
            retentionScheduledTask.cancel();
            retentionScheduledTask = null;
        }
    }

    @Override
    protected void doClose() {}

    private void runBatch() {
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
            // Skip upload if there are no actual entries — all snapshots were empty (no translog ops).
            // Uploading an empty ZIP wastes S3 PUTs and creates stale objects that GC must clean up.
            // Note: releaseSnapshots is called in the finally block below, so we just return here.
            if (allEntries.isEmpty()) {
                logger.debug("Skipping translog archive upload: all snapshots are empty (no translog files to archive)");
                return;
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
     * Pure-timestamp-based archive GC.
     * <p>
     * For each live primary shard on this node we know:
     *   - the index UUID (and so the {@code hashTypeIndex} S3 directory)
     *   - the node ID (and so the {@code hashNodeId} S3 directory belonging to this node)
     *   - the configured {@code translog.archive_retention} (clamped to {@link ArchiveDeletionHelper#MIN_RETENTION_SAFETY_BUFFER_MINUTES})
     * <p>
     * Algorithm:
     * <ol>
     *   <li>List {@code translog/data/} → all {@code hashTypeIndex} directories present in S3.</li>
     *   <li>For each {@code hashTypeIndex}, scan ONLY this node's subfolder
     *       ({@code hashNodeId(localNodeId)}). Other nodes' subfolders belong to peers and they
     *       run their own GC for those.</li>
     *   <li>Page through ZIPs in lexicographic (= chronological by {@code yyyyMMddHHmmssSSS} prefix)
     *       order; build the expired list in memory; <b>early-exit pagination</b> when a ZIP within
     *       retention is encountered.</li>
     *   <li>Delete the expired list in batches of {@link #RETENTION_DELETE_BATCH_SIZE}.</li>
     * </ol>
     * <p>
     * <b>Pure timestamp-based:</b> no per-shard generation/primaryTerm checks. Orphaned-index
     * directories are not special-cased — their ZIPs simply age past retention and get deleted by
     * the normal pass on a future cycle.
     */
    private void runArchiveRetention() {
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = remoteStoreSettings != null
            ? remoteStoreSettings.getPathHashAlgorithm()
            : RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;

        // 1. Discover live indices on this node + a TransferService/basePath/nodeId/retention to use.
        Map<String, Long> retentionMinutesByIndex = new HashMap<>();
        TransferService anyTransferService = null;
        BlobPath anyBasePath = null;
        String localNodeId = null;
        long defaultRetentionMinutes = ArchiveDeletionHelper.MIN_RETENTION_SAFETY_BUFFER_MINUTES;

        for (ShardId sid : getEligibleShardIds()) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) continue;
            String indexUUID = shard.indexSettings().getIndexMetadata().getIndexUUID();
            long retentionMinutes = Math.max(
                shard.indexSettings().getTranslogArchiveRetention().getMinutes(),
                ArchiveDeletionHelper.MIN_RETENTION_SAFETY_BUFFER_MINUTES
            );
            retentionMinutesByIndex.put(indexUUID, retentionMinutes);
            // Use the largest configured retention as the default for orphaned-index dirs we encounter,
            // so we never accidentally delete a ZIP newer than ANY live index's retention.
            if (retentionMinutes > defaultRetentionMinutes) {
                defaultRetentionMinutes = retentionMinutes;
            }
            if (anyTransferService == null) {
                Optional<TranslogTransferManager> tmOpt = shard.getTranslogTransferManager();
                Optional<String> nodeIdOpt = shard.getTranslogNodeId();
                if (tmOpt.isPresent() && nodeIdOpt.isPresent()) {
                    anyTransferService = tmOpt.get().getTransferService();
                    anyBasePath = tmOpt.get().getArchiveBasePath();
                    localNodeId = nodeIdOpt.get();
                }
            }
        }

        // No archive-enabled live shards on this node → nothing to clean up here.
        if (anyTransferService == null || anyBasePath == null || localNodeId == null) {
            return;
        }

        // 2. List all hashTypeIndex directories present in S3 (one S3 LIST).
        BlobPath archiveDataPath = anyBasePath.add("translog").add("data");
        Set<String> s3HashTypeDirs;
        try {
            s3HashTypeDirs = anyTransferService.listFolders(archiveDataPath);
        } catch (IOException e) {
            logger.warn("Failed to list archive data dirs: {}", e.getMessage());
            return;
        }

        String hashNodeIdLocal = TranslogArchivePathHelper.hashNodeId(localNodeId, pathHashAlgorithm);

        // 3. For each hashTypeIndex dir present in S3, scan THIS node's subfolder and delete by age.
        //    Use the index's configured retention if it's still live; otherwise use the largest
        //    retention seen on this node so we stay safe.
        for (String hashTypeDir : s3HashTypeDirs) {
            // Find which live index (if any) maps to this hashTypeDir.
            Long retentionMinutes = null;
            for (Map.Entry<String, Long> e : retentionMinutesByIndex.entrySet()) {
                if (TranslogArchivePathHelper.hashTypeIndex(e.getKey(), pathHashAlgorithm).equals(hashTypeDir)) {
                    retentionMinutes = e.getValue();
                    break;
                }
            }
            if (retentionMinutes == null) {
                retentionMinutes = defaultRetentionMinutes;  // orphaned dir → safe default
            }
            BlobPath zipDir = archiveDataPath.add(hashTypeDir).add(hashNodeIdLocal);
            try {
                deleteArchivesOlderThanRetention(anyTransferService, zipDir, retentionMinutes);
            } catch (IOException ex) {
                logger.warn("Archive retention delete failed for {}: {}", zipDir.buildAsString(), ex.getMessage());
            }
        }
    }

    /**
     * Pure-timestamp-based deletion of expired archive ZIPs in {@code zipDir}.
     * <p>
     * <b>Algorithm:</b>
     * <ol>
     *   <li>Page through ZIPs in {@code zipDir} (size {@link #MAX_ARCHIVE_BLOBS_PER_NODE}, sorted
     *       ascending by name = ascending by {@code yyyyMMddHHmmssSSS} timestamp).</li>
     *   <li>Build {@code expired} list in memory by parsing each ZIP's filename timestamp and
     *       comparing against {@code now - retentionMinutes}.</li>
     *   <li>Stop paginating as soon as we hit a ZIP whose timestamp is within retention — every
     *       subsequent ZIP (in any page) is even newer and therefore also within retention.</li>
     *   <li>Delete all collected expired ZIPs in batches of {@link #RETENTION_DELETE_BATCH_SIZE}.</li>
     * </ol>
     * <p>
     * <b>No generation/primaryTerm checks.</b> Decision is purely the ZIP filename timestamp vs the
     * retention cutoff.
     *
     * @param transferService blob transfer service
     * @param zipDir          this node's ZIP directory ({@code translog/data/{hashTypeIndex}/{hashNodeId}})
     * @param retentionMinutes retention age in minutes (caller is expected to clamp to safety floor)
     * @return number of ZIPs deleted
     * @throws IOException if the initial listing fails irrecoverably
     */
    static int deleteArchivesOlderThanRetention(
        TransferService transferService,
        BlobPath zipDir,
        long retentionMinutes
    ) throws IOException {
        Instant retentionCutoff = Instant.now().minus(Duration.ofMinutes(retentionMinutes));
        int deleted = 0;

        // Outer loop: re-LIST after each deletion batch. Because deletions actually remove the
        // ZIPs from S3, the next listAllInSortedOrder() returns the NEXT block of ZIPs (sorted
        // ascending by name = timestamp). We stop when:
        //   - LIST returns fewer than MAX_ARCHIVE_BLOBS_PER_NODE → no more pages, OR
        //   - the current page contains a ZIP within retention → all remaining blobs are newer.
        while (true) {
            List<BlobMetadata> blobs;
            try {
                blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                    f -> transferService.listAllInSortedOrder(zipDir, "", MAX_ARCHIVE_BLOBS_PER_NODE, f)
                );
            } catch (IOException e) {
                logger.warn("List archive ZIPs failed at {}: {}", zipDir.buildAsString(), e.getMessage());
                throw e;
            }
            if (blobs == null || blobs.isEmpty()) {
                return deleted;
            }

            // Walk page; collect expired with early-exit on first within-retention ZIP.
            List<String> expired = new ArrayList<>(blobs.size());
            boolean hitWithinRetention = false;
            for (BlobMetadata blob : blobs) {
                String name = blob.name();
                if (!TranslogArchivePathHelper.isArchiveBlob(name)) {
                    continue;
                }
                Optional<Instant> tsOpt = TranslogArchivePathHelper.parseBlobNameTimestamp(name);
                if (tsOpt.isEmpty()) {
                    continue;  // unparseable name — leave alone
                }
                if (tsOpt.get().isAfter(retentionCutoff)) {
                    hitWithinRetention = true;
                    break;  // all remaining blobs in this page (and any next page) are newer
                }
                expired.add(name);
            }

            // Delete expired blobs from this page in batches of RETENTION_DELETE_BATCH_SIZE.
            for (int i = 0; i < expired.size(); i += RETENTION_DELETE_BATCH_SIZE) {
                int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, expired.size());
                List<String> batch = expired.subList(i, end);
                try {
                    transferService.deleteBlobs(zipDir, new ArrayList<>(batch));
                    deleted += batch.size();
                } catch (IOException e) {
                    logger.warn(
                        "Failed to delete archive batch ({} blobs) from {}: {}",
                        batch.size(),
                        zipDir.buildAsString(),
                        e.getMessage()
                    );
                    // If deletion fails, abort the loop to avoid re-listing the same blobs forever.
                    return deleted;
                }
            }
            if (deleted > 0) {
                logger.debug(
                    "Deleted {} archive ZIPs older than {} minutes from {} (page deletions={}, hitWithinRetention={})",
                    deleted,
                    retentionMinutes,
                    zipDir.buildAsString(),
                    expired.size(),
                    hitWithinRetention
                );
            }

            // Termination conditions:
            //   1. Hit a within-retention ZIP → no need to look further.
            //   2. Page was not full → S3 has nothing more to offer in this dir.
            if (hitWithinRetention) {
                return deleted;
            }
            if (blobs.size() < MAX_ARCHIVE_BLOBS_PER_NODE) {
                return deleted;
            }
            // Otherwise: we deleted a full page of expired ZIPs; loop and re-LIST to get the next batch.
        }
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
        // Use TAR format by default; fall back to ZIP if feature flag disabled.
        boolean useTar = remoteStoreSettings == null || remoteStoreSettings.isTranslogArchiveUseTar();
        if (!useTar) {
            uploadArchiveZip(transferService, basePath, hashTypeIndex, hashNodeId, snapshots, contributingShards, allEntries);
            return;
        }

        // Path: translog/data/{hashTypeIndex}/{hashNodeId}/{timestamp}.tar
        // hashTypeIndex = hash("translog_zip|{indexUUID}") — same for all nodes, used for per-index GC
        // hashNodeId = hash(nodeId) — unique per node, isolates writes for S3 partition safety
        BlobPath archivePath = basePath.add("translog").add("data").add(hashTypeIndex).add(hashNodeId);

        // TAR streaming: compute layout from file sizes alone (no content reads).
        // All offsets and total size are determined before any data is read — enabling true
        // single-pass streaming to S3 with exact Content-Length.
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries);
        long contentLength = layout.getTotalSize();

        AtomicReference<String> uploadedBlobName = new AtomicReference<>();
        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            String blobName = TranslogArchivePathHelper.tarBlobNameFromCurrentTime();
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
                    // Single-pass streaming: reads each file once, lazily, directly to the pipe.
                    // Peak memory = one file read buffer (64 KB), not all shard translog bytes.
                    TarArchiveBuilder.build(pos, layout, allEntries);
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
                "TAR archive uploaded path={} hashTypeIndex={} hashNodeId={} blob={} size={}",
                archivePath.buildAsString(),
                hashTypeIndex,
                hashNodeId,
                blobName,
                contentLength
            );
            uploadedBlobName.set(blobName);
            break;
        }
        logger.debug(
            "TAR archive batch uploaded: path={} blob={} shards={}",
            archivePath.buildAsString(),
            uploadedBlobName.get(),
            contributingShards.size()
        );
    }

    /**
     * Fallback ZIP-based archive upload (legacy format). Used when {@code use_tar=false}.
     * Unlike the TAR path, this builds the ZIP twice: once to compute size/offsets, once to upload.
     */
    private void uploadArchiveZip(
        TransferService transferService,
        BlobPath basePath,
        String hashTypeIndex,
        String hashNodeId,
        List<TransferSnapshot> snapshots,
        List<IndexShard> contributingShards,
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries
    ) throws IOException {
        BlobPath archivePath = basePath.add("translog").add("data").add(hashTypeIndex).add(hashNodeId);
        ArchiveBuilder.SizeAndOffsets sao = ArchiveBuilder.computeSizeAndOffsetsWithComment(allEntries);
        long contentLength = sao.getSize();
        String blobName = TranslogArchivePathHelper.blobNameFromCurrentTime();
        try (PipedOutputStream pos = new PipedOutputStream(); PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)) {
            AtomicReference<IOException> uploadError = new AtomicReference<>();
            java.util.concurrent.CountDownLatch uploadLatch = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "translog-archive-zip-upload");
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
                throw new IOException("Interrupted while waiting for ZIP archive upload", e);
            }
            executor.shutdown();
            if (uploadError.get() != null) {
                throw uploadError.get();
            }
        }
        logger.debug(
            "ZIP archive uploaded path={} blob={} shards={}",
            archivePath.buildAsString(),
            blobName,
            contributingShards.size()
        );
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
     * Orphaned archive ZIPs for deleted indices are detected via S3 folder scan in the GC.
     */
    public void registerCoordinatorIndex(String indexUUID) {
        coordinatorEnabledIndices.add(indexUUID);
    }

    /**
     * Unregister an index from coordinator-based upload (e.g. on index deletion).
     * Orphaned ZIPs are detected and cleaned up by the GC via S3 folder scan.
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
     * Runs one upload batch synchronously; for unit tests only.
     */
    void runBatchForTesting() {
        runBatch();
    }

    /**
     * Runs archive retention GC synchronously; for unit tests only.
     */
    void runRetentionForTesting() {
        runArchiveRetention();
    }
}
