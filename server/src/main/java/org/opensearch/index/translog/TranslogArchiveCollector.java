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
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Node-level collector for translog archive upload. Runs a scheduled task at buffer_interval;
 * collects from ALL eligible shards across ALL indices on this node, builds one TAR per batch
 * (per-node grouping), uploads one blob per cycle.
 *
 * <p><b>New archive path layout</b>:
 * <pre>
 *   {base}/txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar
 * </pre>
 * Inside each TAR, member paths are {@code indexUUID/shardId/primaryTerm/translog-N.tlog|.ckp}.
 * The TAR's binary {@code _index} entry allows range-read recovery per shard.
 *
 * <p><b>Retention GC</b>: runs only on the elected cluster-manager node (via
 * {@link LocalNodeClusterManagerListener}). Pure timestamp-based: entire minute-directories
 * older than the configured retention age are deleted. Default retention: 2 hours.
 * Default GC interval: 10 minutes.
 *
 * <p><b>Memory</b>: Peak memory is proportional to the total size of all primary shards'
 * translog files in the batch.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchiveCollector extends AbstractLifecycleComponent implements LocalNodeClusterManagerListener {

    private static final Logger logger = LogManager.getLogger(TranslogArchiveCollector.class);

    /** Max blobs to list per page during retention GC. */
    private static final int MAX_ARCHIVE_BLOBS_PER_PAGE = 1000;
    /** Max blob names to pass per deleteBlobs call when batching deletes. */
    private static final int RETENTION_DELETE_BATCH_SIZE = 100;

    /** Node-level upload lock — one TAR upload at a time per node. */
    private static final Object NODE_UPLOAD_LOCK = new Object();

    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final RemoteStoreSettings remoteStoreSettings;
    private final ClusterService clusterService;
    private volatile Scheduler.Cancellable scheduledTask;
    /** Retention GC task — only runs when this node is the elected cluster-manager. */
    private volatile Scheduler.Cancellable retentionScheduledTask;
    /** Set of index UUIDs that use coordinator-based (school bus) uploads. */
    private final Set<String> coordinatorEnabledIndices = ConcurrentHashMap.newKeySet();

    public TranslogArchiveCollector(IndicesService indicesService) {
        this(indicesService, null, null, null);
    }

    public TranslogArchiveCollector(IndicesService indicesService, ThreadPool threadPool, RemoteStoreSettings remoteStoreSettings) {
        this(indicesService, threadPool, remoteStoreSettings, null);
    }

    public TranslogArchiveCollector(
        IndicesService indicesService,
        ThreadPool threadPool,
        RemoteStoreSettings remoteStoreSettings,
        ClusterService clusterService
    ) {
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.remoteStoreSettings = remoteStoreSettings;
        this.clusterService = clusterService;
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
    public byte[] buildArchiveFromEntries(List<TarArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);
        TarArchiveBuilder.build(out, layout, entries);
        return out.toByteArray();
    }

    /** Maximum bytes for all entries from a single snapshot to prevent OOM. */
    private static final long MAX_SNAPSHOT_ARCHIVE_BYTES = 128 * 1024 * 1024L;

    /**
     * Convert a transfer snapshot to archive entries (path = pathPrefix/primaryTerm/name). Reads file content into memory.
     *
     * @throws IOException if total content exceeds MAX_SNAPSHOT_ARCHIVE_BYTES
     */
    public static List<TarArchiveBuilder.ArchiveBuildEntry> snapshotToEntries(TransferSnapshot snapshot, String pathPrefix)
        throws IOException {
        long primaryTerm = snapshot.getTranslogTransferMetadata().getPrimaryTerm();
        String prefix = pathPrefix + "/" + primaryTerm + "/";
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
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

    private static TarArchiveBuilder.ArchiveBuildEntry streamEntry(String path, FileSnapshot.TransferFileSnapshot file) throws IOException {
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
        return TarArchiveBuilder.fromBytes(path, content);
    }

    @Override
    protected void doStart() {
        if (threadPool != null && remoteStoreSettings != null) {
            TimeValue uploadInterval = remoteStoreSettings.getClusterRemoteTranslogBufferInterval();
            scheduledTask = threadPool.scheduleWithFixedDelay(this::runBatch, uploadInterval, ThreadPool.Names.TRANSLOG_TRANSFER);
            // Register as cluster-manager listener so GC only runs on the elected master.
            if (clusterService != null) {
                clusterService.addLocalNodeClusterManagerListener(this);
            }
        }
    }

    @Override
    protected void doStop() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
        }
        // GC task is cancelled via offClusterManager; cancel here too as a safety net.
        if (retentionScheduledTask != null) {
            retentionScheduledTask.cancel();
            retentionScheduledTask = null;
        }
    }

    @Override
    protected void doClose() {}

    // ── LocalNodeClusterManagerListener ───────────────────────────────────────

    /**
     * Called when this node becomes the elected cluster-manager.
     * Starts the archive retention GC scheduled task.
     */
    @Override
    public void onClusterManager() {
        if (threadPool != null && remoteStoreSettings != null && retentionScheduledTask == null) {
            TimeValue gcInterval = remoteStoreSettings.getTranslogArchiveGcInterval();
            logger.info("Became cluster-manager: starting translog archive GC (interval={})", gcInterval);
            retentionScheduledTask = threadPool.scheduleWithFixedDelay(
                this::runArchiveRetention,
                gcInterval,
                ThreadPool.Names.TRANSLOG_TRANSFER
            );
        }
    }

    /**
     * Called when this node loses cluster-manager status.
     * Stops the archive retention GC scheduled task.
     */
    @Override
    public void offClusterManager() {
        if (retentionScheduledTask != null) {
            logger.info("Lost cluster-manager: stopping translog archive GC");
            retentionScheduledTask.cancel();
            retentionScheduledTask = null;
        }
    }

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

        // Collect all pending primary shards across ALL indices on this node (per-node grouping).
        List<IndexShard> shardsWithPending = new ArrayList<>();
        TransferService transferService = null;
        BlobPath basePath = null;
        String nodeId = null;
        for (ShardId sid : eligible) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null || shard.routingEntry() == null || !shard.routingEntry().primary() || !shard.isSyncNeeded()) {
                continue;
            }
            shardsWithPending.add(shard);
            // Capture transfer service + basePath from the first shard that has one.
            if (transferService == null) {
                Optional<TranslogTransferManager> tmOpt = shard.getTranslogTransferManager();
                Optional<String> nodeIdOpt = shard.getTranslogNodeId();
                if (tmOpt.isPresent() && nodeIdOpt.isPresent()) {
                    transferService = tmOpt.get().getTransferService();
                    basePath = tmOpt.get().getArchiveBasePath();
                    nodeId = nodeIdOpt.get();
                }
            }
        }

        if (shardsWithPending.isEmpty() || transferService == null || basePath == null || nodeId == null) {
            return;
        }

        // Single node-level lock: one TAR upload at a time for this node.
        synchronized (NODE_UPLOAD_LOCK) {
            runBatchForNode(transferService, basePath, nodeId, shardsWithPending);
        }
    }

    private void runBatchForNode(
        TransferService transferService,
        BlobPath basePath,
        String nodeId,
        List<IndexShard> shardsWithPending
    ) {
        List<TransferSnapshot> snapshots = new ArrayList<>();
        List<Runnable> releases = new ArrayList<>();
        List<String> pathPrefixes = new ArrayList<>();
        List<IndexShard> contributingShards = new ArrayList<>();
        collectSnapshotsFromShards(shardsWithPending, snapshots, releases, pathPrefixes, contributingShards);
        if (snapshots.isEmpty()) {
            return;
        }
        List<TarArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        try {
            for (int i = 0; i < snapshots.size(); i++) {
                allEntries.addAll(snapshotToEntries(snapshots.get(i), pathPrefixes.get(i)));
            }
            if (allEntries.isEmpty()) {
                logger.debug("Skipping translog archive upload: all snapshots are empty (no translog files to archive)");
                return;
            }
            uploadArchiveNewPath(transferService, basePath, nodeId, snapshots, contributingShards, allEntries);
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Failed to build or upload translog archive for node {}", nodeId), ex);
            runFallbackIfEnabled(contributingShards, snapshots);
        } finally {
            releaseSnapshots(snapshots, releases);
        }
    }

    /**
     * Pure-timestamp-based archive GC using the new hierarchical path.
     *
     * <p>Algorithm (only runs on the elected cluster-manager):
     * <ol>
     *   <li>LIST {@code {base}/txlog/} → day directories.</li>
     *   <li>For each day dir: if the entire day is newer than the retention cutoff → skip (bail out).</li>
     *   <li>If the entire day is older → delete the day directory entirely.</li>
     *   <li>If it's the boundary day → LIST minute-dirs, apply the same bail-out logic per minute.</li>
     *   <li>Delete expired minute-dirs entirely; for the boundary minute, delete individual blobs.</li>
     * </ol>
     *
     * <p>This is O(directories) not O(files): at steady state with 2h retention only ~120 minute-dirs
     * exist, fitting in 1 LIST page.
     */
    private void runArchiveRetention() {
        // Determine the retention duration from any live shard's config.
        TransferService anyTransferService = null;
        BlobPath anyBasePath = null;
        long retentionMinutes = ArchiveDeletionHelper.MIN_RETENTION_SAFETY_BUFFER_MINUTES;

        for (ShardId sid : getEligibleShardIds()) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) continue;
            long shardRetention = Math.max(
                shard.indexSettings().getTranslogArchiveRetention().getMinutes(),
                ArchiveDeletionHelper.MIN_RETENTION_SAFETY_BUFFER_MINUTES
            );
            if (shardRetention > retentionMinutes) {
                retentionMinutes = shardRetention;
            }
            if (anyTransferService == null) {
                Optional<TranslogTransferManager> tmOpt = shard.getTranslogTransferManager();
                if (tmOpt.isPresent()) {
                    anyTransferService = tmOpt.get().getTransferService();
                    anyBasePath = tmOpt.get().getArchiveBasePath();
                }
            }
        }

        if (anyTransferService == null || anyBasePath == null) {
            return;
        }

        Instant cutoff = Instant.now().minus(Duration.ofMinutes(retentionMinutes));
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(anyBasePath);

        try {
            deleteHierarchicalArchivesOlderThan(anyTransferService, txlogRoot, cutoff);
        } catch (IOException ex) {
            logger.warn("Archive retention GC failed: {}", ex.getMessage());
        }
    }

    /**
     * Hierarchical GC for {@code txlog/{day}/{minute}/} path structure.
     * Deletes entire day/minute directories if all their content is older than {@code cutoff}.
     * For the boundary minute, deletes individual blob files older than {@code cutoff}.
     *
     * @param transferService blob service
     * @param txlogRoot       path to the {@code txlog/} root
     * @param cutoff          delete blobs/dirs with timestamp before this instant
     * @return total number of blobs deleted
     */
    static int deleteHierarchicalArchivesOlderThan(
        TransferService transferService,
        BlobPath txlogRoot,
        Instant cutoff
    ) throws IOException {
        int deleted = 0;

        // List day directories under txlog/
        Set<String> dayDirs;
        try {
            dayDirs = transferService.listFolders(txlogRoot);
        } catch (IOException e) {
            logger.warn("GC: failed to list txlog root {}: {}", txlogRoot.buildAsString(), e.getMessage());
            return deleted;
        }
        if (dayDirs == null || dayDirs.isEmpty()) {
            return deleted;
        }

        for (String dayDir : dayDirs) {
            // Day dir format: yyyyMMdd — end of day = dayDir + "2359"
            // If the start of the day is newer than cutoff (dayDir >= cutoffDay), bail out.
            // If the end of the day is older than cutoff (dayDir < cutoffDay), delete entire day dir.
            String cutoffDay = TranslogArchivePathHelper.dayDir(cutoff);
            int dayCmp = dayDir.compareTo(cutoffDay);
            if (dayCmp > 0) {
                // Entire day is newer than cutoff — skip
                continue;
            }
            BlobPath dayPath = txlogRoot.add(dayDir);
            if (dayCmp < 0) {
                // Entire day is before the cutoff day — delete all minute-dirs in it
                deleted += deleteAllMinuteDirs(transferService, dayPath);
                continue;
            }

            // Boundary day (dayCmp == 0): enumerate minute-dirs and apply per-minute logic
            Set<String> minuteDirs;
            try {
                minuteDirs = transferService.listFolders(dayPath);
            } catch (IOException e) {
                logger.warn("GC: failed to list day dir {}: {}", dayPath.buildAsString(), e.getMessage());
                continue;
            }
            if (minuteDirs == null || minuteDirs.isEmpty()) {
                continue;
            }

            String cutoffMinute = TranslogArchivePathHelper.minuteDir(cutoff);
            for (String minuteDir : minuteDirs) {
                int minCmp = minuteDir.compareTo(cutoffMinute);
                BlobPath minutePath = dayPath.add(minuteDir);
                if (minCmp > 0) {
                    // Minute is newer than cutoff — skip
                    continue;
                }
                if (minCmp < 0) {
                    // Entire minute is before the cutoff — delete entire minute dir
                    deleted += deleteBlobsInDir(transferService, minutePath, null /* all blobs */);
                    continue;
                }
                // Boundary minute: delete only blobs with timestamp < cutoff
                deleted += deleteBlobsInDir(transferService, minutePath, cutoff);
            }
        }
        return deleted;
    }

    /**
     * Deletes all minute-level directories (and their blobs) under a day path.
     */
    private static int deleteAllMinuteDirs(TransferService transferService, BlobPath dayPath) throws IOException {
        Set<String> minuteDirs;
        try {
            minuteDirs = transferService.listFolders(dayPath);
        } catch (IOException e) {
            logger.warn("GC: failed to list day dir {}: {}", dayPath.buildAsString(), e.getMessage());
            return 0;
        }
        if (minuteDirs == null || minuteDirs.isEmpty()) {
            return 0;
        }
        int deleted = 0;
        for (String minuteDir : minuteDirs) {
            deleted += deleteBlobsInDir(transferService, dayPath.add(minuteDir), null /* all blobs */);
        }
        return deleted;
    }

    /**
     * Deletes blobs in a single minute directory.
     *
     * @param minutePath the minute-level BlobPath
     * @param cutoff     if non-null, only delete blobs whose timestamp is before this instant;
     *                   if null, delete all blobs in the directory
     * @return number of blobs deleted
     */
    static int deleteBlobsInDir(TransferService transferService, BlobPath minutePath, Instant cutoff) {
        // Extract day and minute from the path to reconstruct blob timestamps
        String[] pathParts = minutePath.buildAsString().split("/");
        // Path: .../txlog/{day}/{minute}  → last 2 non-empty segments are minute, day
        String dayDirStr = pathParts.length >= 2 ? pathParts[pathParts.length - 2] : null;
        String minuteDirStr = pathParts.length >= 1 ? pathParts[pathParts.length - 1] : null;

        List<BlobMetadata> blobs;
        try {
            blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                f -> transferService.listAllInSortedOrder(minutePath, "", MAX_ARCHIVE_BLOBS_PER_PAGE, f)
            );
        } catch (IOException e) {
            logger.warn("GC: failed to list minute dir {}: {}", minutePath.buildAsString(), e.getMessage());
            return 0;
        }
        if (blobs == null || blobs.isEmpty()) {
            return 0;
        }

        List<String> toDelete = new ArrayList<>(blobs.size());
        for (BlobMetadata blob : blobs) {
            String name = blob.name();
            if (cutoff == null) {
                // Delete all blobs
                toDelete.add(name);
            } else {
                // Delete only blobs older than cutoff
                Optional<Instant> tsOpt = TranslogArchivePathHelper.parseTarBlobTimestamp(dayDirStr, minuteDirStr, name);
                if (tsOpt.isPresent() && tsOpt.get().isBefore(cutoff)) {
                    toDelete.add(name);
                }
                // Blobs not parseable or newer than cutoff: skip
            }
        }

        int deleted = 0;
        for (int i = 0; i < toDelete.size(); i += RETENTION_DELETE_BATCH_SIZE) {
            int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, toDelete.size());
            List<String> batch = toDelete.subList(i, end);
            try {
                transferService.deleteBlobs(minutePath, new ArrayList<>(batch));
                deleted += batch.size();
            } catch (IOException e) {
                logger.warn("GC: failed to delete batch in {}: {}", minutePath.buildAsString(), e.getMessage());
            }
        }
        if (deleted > 0) {
            logger.debug("GC: deleted {} blobs from {}", deleted, minutePath.buildAsString());
        }
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

    private void uploadArchiveNewPath(
        TransferService transferService,
        BlobPath basePath,
        String nodeId,
        List<TransferSnapshot> snapshots,
        List<IndexShard> contributingShards,
        List<TarArchiveBuilder.ArchiveBuildEntry> allEntries
    ) throws IOException {
        // New hierarchical path: {base}/txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar
        Instant now = Instant.now();
        BlobPath archivePath = TranslogArchivePathHelper.tarBlobDir(basePath, now);

        // TAR streaming: compute layout from file sizes alone (no content reads).
        // All offsets and total size are determined before any data is read — enabling true
        // single-pass streaming to S3 with exact Content-Length.
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries);
        long contentLength = layout.getTotalSize();

        AtomicReference<String> uploadedBlobName = new AtomicReference<>();
        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            String blobName = TranslogArchivePathHelper.tarBlobName(now, nodeId);
            try (PipedOutputStream pos = new PipedOutputStream(); PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)) {
                AtomicReference<IOException> uploadError = new AtomicReference<>();
                CountDownLatch uploadLatch = new CountDownLatch(1);
                ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
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
                "TAR archive uploaded path={} nodeId={} blob={} size={}",
                archivePath.buildAsString(),
                nodeId,
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

    /**
     * Backward-compatible adapter for tests that used the old flat-dir GC API.
     * Deletes blobs in {@code zipDir} whose legacy timestamp (from blob name) is older than
     * {@code now - retentionMinutes}. Used by unit tests that set up blobs directly in a flat dir.
     *
     * @param transferService  blob transfer service
     * @param zipDir           flat blob directory (path ending at the container level)
     * @param retentionMinutes retention cutoff in minutes
     * @return number of blobs deleted
     * @throws IOException on list failure
     */
    static int deleteArchivesOlderThanRetention(
        TransferService transferService,
        BlobPath zipDir,
        long retentionMinutes
    ) throws IOException {
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(retentionMinutes));
        // Use listAllInSortedOrder directly (flat dir, old blob names)
        List<BlobMetadata> blobs;
        blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
            f -> transferService.listAllInSortedOrder(zipDir, "", MAX_ARCHIVE_BLOBS_PER_PAGE, f)
        );
        if (blobs == null || blobs.isEmpty()) {
            return 0;
        }
        List<String> toDelete = new ArrayList<>();
        for (BlobMetadata blob : blobs) {
            String name = blob.name();
            Optional<Instant> tsOpt = TranslogArchivePathHelper.parseBlobNameTimestamp(name);
            if (tsOpt.isPresent() && tsOpt.get().isBefore(cutoff)) {
                toDelete.add(name);
            }
        }
        int deleted = 0;
        for (int i = 0; i < toDelete.size(); i += RETENTION_DELETE_BATCH_SIZE) {
            int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, toDelete.size());
            transferService.deleteBlobs(zipDir, new ArrayList<>(toDelete.subList(i, end)));
            deleted += end - i;
        }
        return deleted;
    }
}
