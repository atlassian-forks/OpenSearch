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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.transfer.FileSnapshot;
import org.opensearch.index.translog.transfer.TransferService;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogArchivePathHelper;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.TranslogArchiveGcScanner;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    /**
     * Checkpoint-aware GC scanner — lazily created when this node becomes cluster-manager.
     * Null when not the cluster-manager.
     */
    private volatile TranslogArchiveGcScanner gcScanner;
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
            // Initialise the checkpoint-aware GC scanner.
            // We bootstrap it eagerly with any available transfer service so it can load
            // persisted .idx state before the first GC cycle runs.
            initGcScannerIfNeeded();

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
     * Stops the archive retention GC scheduled task and clears the GC scanner.
     */
    @Override
    public void offClusterManager() {
        if (retentionScheduledTask != null) {
            logger.info("Lost cluster-manager: stopping translog archive GC");
            retentionScheduledTask.cancel();
            retentionScheduledTask = null;
        }
        gcScanner = null;
    }

    /**
     * Initialises {@link #gcScanner} from any available shard's transfer service.
     * Synchronized to prevent a check-then-act race when called concurrently
     * (e.g. from onClusterManager and runArchiveRetention at the same time).
     */
    private synchronized void initGcScannerIfNeeded() {
        if (gcScanner != null) return;
        for (ShardId sid : getEligibleShardIds()) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) continue;
            Optional<TranslogTransferManager> tmOpt = shard.getTranslogTransferManager();
            if (tmOpt.isPresent()) {
                TransferService ts = tmOpt.get().getTransferService();
                BlobPath base = tmOpt.get().getArchiveBasePath();
                TranslogArchiveGcScanner scanner = new TranslogArchiveGcScanner(ts, base);
                try {
                    scanner.loadFromPersisted();
                } catch (Exception e) {
                    logger.warn("GC scanner: failed to load persisted state: {}", e.getMessage());
                }
                gcScanner = scanner;
                logger.info("GC scanner initialised from shard {}", sid);
                return;
            }
        }
        logger.debug("GC scanner: no eligible shard with transfer service found yet");
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
        List<TarArchiveBuilder.GcShardEntry> gcEntries = new ArrayList<>(contributingShards.size());
        try {
            for (int i = 0; i < snapshots.size(); i++) {
                allEntries.addAll(snapshotToEntries(snapshots.get(i), pathPrefixes.get(i)));

                // Build GC summary entry for this shard embedding seqNo range + last synced global checkpoint.
                //
                // IMPORTANT: All values are in sequence number (seqNo) space — NOT translog generation space.
                // Translog generation numbers are file counters and are NOT comparable to seqNos.
                //
                // We embed getLastSyncedGlobalCheckpoint() (the globalCheckpoint from the last Lucene
                // segment commit / translog sync), NOT getLastKnownGlobalCheckpoint() (the in-memory
                // replicated checkpoint). The synced checkpoint is what the remote segment store has
                // committed — if checkpoint >= maxSeqNo, the ops are in uploaded segments and the translog
                // is no longer needed for recovery (two-phase isSafeToDelete Phase 1 check).
                IndexShard shard = contributingShards.get(i);
                org.opensearch.index.seqno.SeqNoStats seqNoStats = shard.seqNoStats();
                long minSeqNo = seqNoStats.getLocalCheckpoint();   // local checkpoint: ops confirmed processed
                long maxSeqNo = seqNoStats.getMaxSeqNo();          // highest seqNo assigned in this batch
                // Use last SYNCED checkpoint: the checkpoint written to the last committed Lucene segment.
                // This is the checkpoint the remote segment store has durably uploaded — safe for GC decisions.
                long syncedGlobalCheckpoint = shard.getLastSyncedGlobalCheckpoint();
                String indexUUID = shard.indexSettings().getIndexMetadata().getIndexUUID();
                gcEntries.add(new TarArchiveBuilder.GcShardEntry(indexUUID, shard.shardId().id(), minSeqNo, maxSeqNo, syncedGlobalCheckpoint));
            }
            if (allEntries.isEmpty()) {
                logger.debug("Skipping translog archive upload: all snapshots are empty (no translog files to archive)");
                return;
            }
            uploadArchiveNewPath(transferService, basePath, nodeId, snapshots, contributingShards, allEntries, gcEntries);
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Failed to build or upload translog archive for node {}", nodeId), ex);
            runFallbackIfEnabled(contributingShards, snapshots);
        } finally {
            releaseSnapshots(snapshots, releases);
        }
    }

    /**
     * Checkpoint-aware archive GC using the hierarchical txlog path.
     *
     * <p>The checkpoint gate is the primary safety mechanism — a minute-dir is deleted only when
     * all shard seqNos in that minute are confirmed committed to remote segments (via the two-phase
     * {@link TranslogArchiveGcScanner#isSafeToDelete} check). The configurable retention window
     * ({@code index.remote_store.translog.archive_retention}) adds an additional time gate:
     * minute-dirs younger than the retention age are never deleted even if the checkpoint gate passes.
     *
     * <p>Algorithm (only runs on the elected cluster-manager):
     * <ol>
     *   <li>Resolve transfer service + base path from any eligible shard.</li>
     *   <li>Compute retention cutoff = {@code now - archiveRetention} from that shard's index settings.</li>
     *   <li>Ensure {@link #gcScanner} is initialised; run {@code scanner.scan()} to index new minute-dirs.</li>
     *   <li>For every minute-dir in txlog/ that is older than the retention cutoff:
     *       ask {@code scanner.isSafeToDelete()} — delete only if checkpoint gate passes.</li>
     *   <li>On successful deletion: evict the minute-key from the scanner's in-memory index.</li>
     * </ol>
     */
    private void runArchiveRetention() {
        // TARs are node-level batches containing ops from ALL shards on the node (across all indices).
        // GC runs at cluster level and deletes entire minute-dirs. The retention window is therefore
        // a cluster-level setting (cluster.remote_store.translog.archive.retention), not per-index.
        TransferService anyTransferService = null;
        BlobPath anyBasePath = null;

        for (ShardId sid : getEligibleShardIds()) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) continue;
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) continue;
            Optional<TranslogTransferManager> tmOpt = shard.getTranslogTransferManager();
            if (tmOpt.isPresent()) {
                anyTransferService = tmOpt.get().getTransferService();
                anyBasePath = tmOpt.get().getArchiveBasePath();
                break;
            }
        }

        if (anyTransferService == null || anyBasePath == null) {
            return;
        }

        // Ensure the GC scanner is ready (may be null if no shards existed at onClusterManager time).
        initGcScannerIfNeeded();
        TranslogArchiveGcScanner scanner = gcScanner;

        // Run the scanner to index any new minute-dirs before making deletion decisions.
        // Checkpoints are embedded in each TAR's GC summary — no need to query local shards here.
        if (scanner != null) {
            try {
                scanner.scan(Instant.now());
            } catch (Exception e) {
                logger.warn("GC scanner: scan failed: {}", e.getMessage());
            }
        }

        // Build liveIndexUUIDs from cluster state — authoritative, works on dedicated master with no local shards.
        // Indices absent from cluster state have been deleted; their TARs are unconditionally safe to delete.
        Set<String> liveIndexUUIDs = null;
        if (clusterService != null) {
            liveIndexUUIDs = new HashSet<>();
            for (IndexMetadata meta : clusterService.state().metadata()) {
                liveIndexUUIDs.add(meta.getIndexUUID());
            }
        }

        // Retention cutoff: cluster.remote_store.translog.archive.retention (default 5m).
        // Combined with the checkpoint gate (isSafeToDelete), TARs are deleted only when:
        //   1. The minute-dir is older than the cluster retention window (time gate)
        //   2. All shard checkpoints in the minute are committed to remote segments (checkpoint gate)
        long retentionMillis = remoteStoreSettings.getTranslogArchiveRetention().millis();
        Instant retentionCutoff = Instant.now().minus(Duration.ofMillis(retentionMillis));
        BlobPath txlogRoot = TranslogArchivePathHelper.txlogRootPath(anyBasePath);

        try {
            deleteHierarchicalArchivesOlderThan(anyTransferService, txlogRoot, retentionCutoff, scanner, liveIndexUUIDs);
        } catch (IOException ex) {
            logger.warn("Archive retention GC failed: {}", ex.getMessage());
        }
    }

    /**
     * Hierarchical GC for {@code txlog/{day}/{minute}/} path structure.
     *
     * <p>For each expired minute-dir (older than {@code cutoff}):
     * <ol>
     *   <li>Ask {@code scanner.isSafeToDelete(minuteKey)}: the scanner uses its rolling
     *       per-shard checkpoint map (populated from embedded GC summaries in TARs) to verify
     *       all shard generations are covered. If scanner is null or not yet indexed → skip.</li>
     *   <li>Delete all blobs in the minute-dir (or just blobs older than cutoff for boundary minute).</li>
     *   <li>Evict from scanner's in-memory index.</li>
     * </ol>
     *
     * @param transferService blob service
     * @param txlogRoot       path to the {@code txlog/} root
     * @param cutoff          delete blobs/dirs with timestamp before this instant
     * @param scanner         checkpoint-aware GC scanner; may be null (skips checkpoint gate)
     * @return total number of blobs deleted
     */
    static int deleteHierarchicalArchivesOlderThan(
        TransferService transferService,
        BlobPath txlogRoot,
        Instant cutoff,
        TranslogArchiveGcScanner scanner
    ) throws IOException {
        return deleteHierarchicalArchivesOlderThan(transferService, txlogRoot, cutoff, scanner, null);
    }

    /**
     * Backward-compatible overload — no scanner, timestamp-only.
     */
    static int deleteHierarchicalArchivesOlderThan(
        TransferService transferService,
        BlobPath txlogRoot,
        Instant cutoff
    ) throws IOException {
        return deleteHierarchicalArchivesOlderThan(transferService, txlogRoot, cutoff, null, null);
    }

    /**
     * Hierarchical GC for {@code txlog/{day}/{minute}/} path structure.
     *
     * <p>For each expired minute-dir (older than {@code cutoff}):
     * <ol>
     *   <li>Ask {@code scanner.isSafeToDelete(minuteKey, liveIndexUUIDs)}: deleted-index shards are
     *       unconditionally safe; live-index shards use the two-phase checkpoint check.</li>
     *   <li>Delete all blobs in the minute-dir (or just blobs older than cutoff for boundary minute).</li>
     *   <li>Evict from scanner's in-memory index.</li>
     * </ol>
     *
     * @param transferService blob service
     * @param txlogRoot       path to the {@code txlog/} root
     * @param cutoff          delete blobs/dirs with timestamp before this instant
     * @param scanner         checkpoint-aware GC scanner; may be null (skips checkpoint gate)
     * @param liveIndexUUIDs  set of index UUIDs currently alive in cluster state; null → skip liveness check
     * @return total number of blobs deleted
     */
    static int deleteHierarchicalArchivesOlderThan(
        TransferService transferService,
        BlobPath txlogRoot,
        Instant cutoff,
        TranslogArchiveGcScanner scanner,
        Set<String> liveIndexUUIDs
    ) throws IOException {
        int deleted = 0;

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

        String cutoffDay = TranslogArchivePathHelper.dayDir(cutoff);
        String cutoffMinute = TranslogArchivePathHelper.minuteDir(cutoff);

        for (String dayDir : dayDirs) {
            int dayCmp = dayDir.compareTo(cutoffDay);
            if (dayCmp > 0) {
                continue; // Entire day is newer than cutoff — skip
            }

            BlobPath dayPath = txlogRoot.add(dayDir);
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

            for (String minuteDir : minuteDirs) {
                if (dayCmp == 0 && minuteDir.compareTo(cutoffMinute) > 0) {
                    continue; // Minute is newer than cutoff — skip
                }

                String minuteKey = dayDir + "/" + minuteDir;
                BlobPath minutePath = dayPath.add(minuteDir);

                // Checkpoint safety gate via rolling checkpoint map embedded in TARs.
                // scanner == null → no gate (timestamp-only fallback, whole-minute delete).
                if (scanner != null && !scanner.isSafeToDelete(minuteKey, liveIndexUUIDs)) {
                    // Minute-dir is not entirely safe — attempt per-TAR granularity deletion.
                    // Safe TARs (from non-stuck shards) can be deleted individually even if the
                    // minute-dir still holds a few stuck TARs from a down node.
                    int partialDeleted = deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner, liveIndexUUIDs);
                    deleted += partialDeleted;
                    // Don't evict — the minute-key stays in memory until all TARs are gone.
                    continue;
                }

                if (dayCmp == 0 && minuteDir.compareTo(cutoffMinute) == 0) {
                    deleted += deleteBlobsInDir(transferService, minutePath, cutoff);
                } else {
                    deleted += deleteBlobsInDir(transferService, minutePath, null);
                }

                if (scanner != null) {
                    scanner.evict(minuteKey);
                }
            }
        }
        return deleted;
    }

    /**
     * Per-TAR granularity deletion for a stuck minute-dir.
     *
     * <p>When a minute-dir is not entirely safe to delete (because at least one shard is stuck),
     * we iterate over each TAR blob and delete only those whose GC entries are all safe.
     * This prevents a single stuck node from holding back 9,000+ safe TARs from other nodes.
     *
     * <p>Per-TAR GC entries are read from the scanner's in-memory cache (populated during
     * {@code scanMinute()}). If not cached (e.g. minute loaded from persisted .idx on restart),
     * we fall back to a range-GET of the TAR's GC prefix — same cost as the initial scan.
     *
     * <p>If all TARs in the minute-dir are deleted, the minute-key is evicted from the scanner.
     *
     * @param transferService blob service
     * @param minutePath      path to the minute-dir
     * @param minuteKey       {@code "yyyyMMdd/HHmm"} key
     * @param scanner         GC scanner (never null when this method is called)
     * @return number of TARs deleted in this call
     */
    static int deleteStuckMinutePartially(
        TransferService transferService,
        BlobPath minutePath,
        String minuteKey,
        TranslogArchiveGcScanner scanner
    ) {
        return deleteStuckMinutePartially(transferService, minutePath, minuteKey, scanner, null);
    }

    static int deleteStuckMinutePartially(
        TransferService transferService,
        BlobPath minutePath,
        String minuteKey,
        TranslogArchiveGcScanner scanner,
        Set<String> liveIndexUUIDs
    ) {
        // List remaining TAR blobs in the minute-dir
        List<BlobMetadata> remainingBlobs;
        try {
            remainingBlobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                f -> transferService.listAllInSortedOrder(minutePath, "", MAX_ARCHIVE_BLOBS_PER_PAGE, f)
            );
        } catch (IOException e) {
            logger.warn("GC per-TAR: failed to list stuck minute {}: {}", minuteKey, e.getMessage());
            return 0;
        }
        if (remainingBlobs == null || remainingBlobs.isEmpty()) {
            // Minute-dir is already empty — evict
            scanner.evict(minuteKey);
            return 0;
        }

        // Re-read GC prefix for each TAR to determine per-TAR safety.
        // We do NOT cache per-TAR GC entries in memory because that would require
        // ~160KB per TAR × 9,231 TARs/minute = ~14.8GB at steady state — unacceptable.
        // This path is only taken for stuck minutes (rare: only during node outages).
        // The extra range-GETs cost ~$2.64 one-time when the stuck node recovers.
        List<String> safeToDelete = new ArrayList<>();
        int remaining = 0;
        for (BlobMetadata blob : remainingBlobs) {
            String blobName = blob.name();
            if (!blobName.endsWith(".tar")) {
                // Non-TAR blobs: keep
                remaining++;
                continue;
            }

            // Re-read GC prefix via range-GET (1 GET per TAR — same cost as initial scan)
            List<TarArchiveBuilder.GcShardEntry> gcEntries = scanner.readGcPrefix(minutePath, blobName);

            if (scanner.isTarSafeToDelete(gcEntries, liveIndexUUIDs)) {
                safeToDelete.add(blobName);
            } else {
                remaining++;
            }
        }

        // Delete safe TARs in batches
        int deleted = 0;
        for (int i = 0; i < safeToDelete.size(); i += RETENTION_DELETE_BATCH_SIZE) {
            int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, safeToDelete.size());
            List<String> batch = safeToDelete.subList(i, end);
            try {
                transferService.deleteBlobs(minutePath, new ArrayList<>(batch));
                deleted += batch.size();
            } catch (IOException e) {
                logger.warn("GC per-TAR: failed to delete batch in {}: {}", minutePath.buildAsString(), e.getMessage());
                remaining += batch.size(); // Count as still remaining on failure
            }
        }

        // If all TARs are now gone, evict the minute-key from memory
        if (remaining == 0 && deleted > 0) {
            scanner.evict(minuteKey);
            logger.debug("GC per-TAR: minute {} fully cleaned after per-TAR pass", minuteKey);
        } else if (deleted > 0) {
            logger.debug("GC per-TAR: deleted {} safe TARs from stuck minute {}, {} still stuck", deleted, minuteKey, remaining);
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
        List<TarArchiveBuilder.ArchiveBuildEntry> allEntries,
        List<TarArchiveBuilder.GcShardEntry> gcEntries
    ) throws IOException {
        // New hierarchical path: {base}/txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar
        Instant now = Instant.now();
        BlobPath archivePath = TranslogArchivePathHelper.tarBlobDir(basePath, now);

        // TAR streaming: compute layout with embedded GC summary prefix.
        // The GC scanner reads these entries (minGen, maxGen, globalCheckpoint per shard)
        // via a single range-GET to build its rolling checkpoint map — no separate upload needed.
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(allEntries, gcEntries);
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
     * Also evicts the index from the GC scanner's in-memory state so stale checkpoint data
     * doesn't block GC for future indices that reuse the same shard IDs.
     */
    public void unregisterCoordinatorIndex(String indexUUID) {
        coordinatorEnabledIndices.remove(indexUUID);
        TranslogArchiveGcScanner scanner = gcScanner;
        if (scanner != null) {
            scanner.evictIndex(indexUUID);
        }
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
     * Runs archive retention GC synchronously; for integration tests only.
     */
    public void runRetentionForTesting() {
        runArchiveRetention();
    }

}
