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
import org.opensearch.index.translog.transfer.archive.ArchiveEntry;
import org.opensearch.index.translog.transfer.archive.ZipCentralDirectoryParser;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.opensearch.action.support.PlainActionFuture;
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
 * <b>Archive path layout</b>: Blobs live at {@code basePath/translog/data/{hashTypeIndex}/{hashNodeId}/{yyyyMMddHHmmssSSS}.zip}.
 * Inside each ZIP, member paths are {@code indexUUID/shardId/primaryTerm/translog-<gen>.tlog} or {@code .ckp}.
 * See {@link org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper} for path parsing and retention.
 * <p>
 * <b>Retention</b>: {@link #deleteArchivesOlderThanRetentionNewPath} deletes an archive blob only when every member
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

    /**
     * Build a ZIP (stored) archive to a file on disk; used for upload to keep memory small.
     */
    private static void buildArchiveToFile(Path path, Iterable<ArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            ArchiveBuilder.build(out, entries);
        }
    }

    /**
     * Build a ZIP (stored) with index in EOCD comment to a file; used for upload on new path.
     */
    private static void buildArchiveToFileWithComment(Path path, Iterable<ArchiveBuilder.ArchiveBuildEntry> entries) throws IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            ArchiveBuilder.buildWithComment(out, entries);
        }
    }

    /**
     * Convert a transfer snapshot to archive entries (path = pathPrefix/primaryTerm/name). Reads file content into memory.
     */
    public static List<ArchiveBuilder.ArchiveBuildEntry> snapshotToEntries(TransferSnapshot snapshot, String pathPrefix)
        throws IOException {
        long primaryTerm = snapshot.getTranslogTransferMetadata().getPrimaryTerm();
        String prefix = pathPrefix + "/" + primaryTerm + "/";
        List<ArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        for (FileSnapshot.TransferFileSnapshot file : snapshot.getTranslogFileSnapshotWithMetadata()) {
            entries.add(streamEntry(prefix + file.getName(), file));
        }
        for (FileSnapshot.TransferFileSnapshot file : snapshot.getCheckpointFileSnapshots()) {
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
                    transferManager.getRemoteDataTransferPath(),
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
        long minGenerationInZip = Long.MAX_VALUE;
        for (TransferSnapshot s : snapshots) {
            long minGen = s.getTranslogTransferMetadata().getMinTranslogGeneration();
            if (minGen >= 0 && minGen < minGenerationInZip) {
                minGenerationInZip = minGen;
            }
        }
        long genBucket = minGenerationInZip != Long.MAX_VALUE ? minGenerationInZip / GEN_BUCKET_SIZE : 0L;
        String hashPrefix = RemoteStoreEnums.PathHashAlgorithm.hashForTranslogArchive(
            pathHashAlgorithm,
            TRANSLOG_ARCHIVE_FILE_TYPE,
            indexUUID,
            nodeId
        );
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries = new ArrayList<>();
        try {
            for (int i = 0; i < snapshots.size(); i++) {
                allEntries.addAll(snapshotToEntries(snapshots.get(i), pathPrefixes.get(i)));
            }
            uploadArchiveNewPathAndMetadata(transferService, basePath, hashPrefix, genBucket, snapshots, contributingShards, allEntries);
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
        Map<String, ArchiveDeletionHelper.RetentionBounds> retentionByShard = new HashMap<>();
        TranslogTransferManager transferManager = null;
        for (ShardId sid : eligible) {
            IndexService indexService = indicesService.indexService(sid.getIndex());
            if (indexService == null) {
                continue;
            }
            IndexShard shard = indexService.getShardOrNull(sid.id());
            if (shard == null) {
                continue;
            }
            shard.getArchiveRetentionBounds()
                .ifPresent(
                    bounds -> retentionByShard.put(shard.indexSettings().getIndexMetadata().getIndexUUID() + "/" + sid.id(), bounds)
                );
            if (transferManager == null) {
                Optional<TranslogTransferManager> opt = shard.getTranslogTransferManager();
                if (opt.isPresent()) {
                    transferManager = opt.get();
                }
            }
        }
        if (retentionByShard.isEmpty() || transferManager == null) {
            return;
        }
        try {
            BlobPath dataPath = transferManager.getRemoteDataTransferPath().add("translog").add("data");
            deleteArchivesOlderThanRetentionNewPath(transferManager.getTransferService(), dataPath, retentionByShard);
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("Archive retention delete failed"), e);
        }
    }

    /**
     * Deletes archive blobs under {@code translog/data/{hashPrefix}/{genBucket}/} when every member is past
     * retention. Walks hashPrefix → genBucket → {@code *.zip}; range-reads each ZIP tail; uses comment when
     * present else central directory parse to decide if the archive is deletable.
     *
     * @param transferService  service to list, read, and delete
     * @param dataBasePath     base path (e.g. remoteDataTransferPath.add("translog").add("data"))
     * @param retentionByShard map from shard key (indexUUID/shardId) to retention bounds
     * @return number of archive blobs deleted
     */
    public static int deleteArchivesOlderThanRetentionNewPath(
        TransferService transferService,
        BlobPath dataBasePath,
        Map<String, ArchiveDeletionHelper.RetentionBounds> retentionByShard
    ) throws IOException {
        if (retentionByShard == null || retentionByShard.isEmpty()) {
            return 0;
        }
        Set<String> hashPrefixes;
        try {
            hashPrefixes = transferService.listFolders(dataBasePath);
        } catch (IOException e) {
            logger.trace("List translog/data failed: {}", e.getMessage());
            return 0;
        }
        if (hashPrefixes == null || hashPrefixes.isEmpty()) {
            return 0;
        }
        int deleted = 0;
        for (String hashPrefix : hashPrefixes) {
            BlobPath hashPath = dataBasePath.add(hashPrefix);
            Set<String> genBuckets;
            try {
                genBuckets = transferService.listFolders(hashPath);
            } catch (IOException e) {
                logger.trace("List translog/data/{} failed: {}", hashPrefix, e.getMessage());
                continue;
            }
            if (genBuckets == null || genBuckets.isEmpty()) {
                continue;
            }
            for (String genBucket : genBuckets) {
                BlobPath genPath = hashPath.add(genBucket);
                deleted += deleteArchivesInPathOlderThanRetention(transferService, genPath, retentionByShard);
            }
        }
        return deleted;
    }

    /** Lists *.zip in nodePath, range-reads tail per ZIP, and deletes those whose entries are all past retention. */
    private static int deleteArchivesInPathOlderThanRetention(
        TransferService transferService,
        BlobPath nodePath,
        Map<String, ArchiveDeletionHelper.RetentionBounds> retentionByShard
    ) throws IOException {
        int deleted = 0;
        boolean morePages;
        do {
            List<BlobMetadata> blobs;
            try {
                blobs = PlainActionFuture.<List<BlobMetadata>, IOException>get(
                    f -> transferService.listAllInSortedOrder(nodePath, "", MAX_ARCHIVE_BLOBS_PER_NODE, f)
                );
            } catch (IOException e) {
                logger.trace("List blobs failed: {}", e.getMessage());
                break;
            }
            if (blobs == null || blobs.isEmpty()) {
                break;
            }
            Instant retentionCutoff = Instant.now().minus(Duration.ofMinutes(RETENTION_SKIP_ZIPS_NEWER_THAN_MINUTES));
            List<String> toDelete = new ArrayList<>();
            for (BlobMetadata blob : blobs) {
                String name = blob.name();
                if (name == null || !name.endsWith(".zip")) {
                    continue;
                }
                if (TranslogArchivePathHelper.parseBlobNameTimestamp(name).filter(ts -> ts.isAfter(retentionCutoff)).isPresent()) {
                    continue;
                }
                long size = blob.length();
                if (size < 22) {
                    continue;
                }
                int tailLen = (int) Math.min(size, ZipCentralDirectoryParser.MAX_ZIP_TAIL_BYTES);
                long tailStartOffset = size - tailLen;
                byte[] tail;
                try (InputStream in = transferService.downloadBlob(nodePath, name, tailStartOffset, tailLen)) {
                    tail = in.readAllBytes();
                } catch (IOException e) {
                    logger.trace("Range-read tail of archive {} failed: {}", name, e.getMessage());
                    continue;
                }
                List<ArchiveEntry> entries = null;
                try {
                    String comment = ZipCentralDirectoryParser.getComment(tail);
                    ArchiveCommentFormat.ParseResult parsed = ArchiveCommentFormat.parseWithIndex(comment);
                    if (parsed.getIndexUUID() != null && !parsed.getEntries().isEmpty()) {
                        entries = ArchiveCommentFormat.toArchiveEntriesForRetention(parsed.getIndexUUID(), parsed.getEntries());
                    }
                } catch (IOException ignored) {}
                if (entries == null || entries.isEmpty()) {
                    try {
                        entries = ZipCentralDirectoryParser.parse(tail, tailStartOffset);
                    } catch (IOException e) {
                        logger.trace("Parse central directory of {} failed: {}", name, e.getMessage());
                        continue;
                    }
                }
                if (ArchiveDeletionHelper.isArchiveDeletable(entries, retentionByShard)) {
                    toDelete.add(name);
                }
            }
            for (int i = 0; i < toDelete.size(); i += RETENTION_DELETE_BATCH_SIZE) {
                int end = Math.min(i + RETENTION_DELETE_BATCH_SIZE, toDelete.size());
                List<String> batch = toDelete.subList(i, end);
                try {
                    transferService.deleteBlobs(nodePath, new ArrayList<>(batch));
                    deleted += batch.size();
                    for (String name : batch) {
                        logger.debug("Deleted archive {} (past retention)", name);
                    }
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("Failed to delete archive batch ({} blobs)", batch.size()), e);
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
        String hashPrefix,
        long genBucket,
        List<TransferSnapshot> snapshots,
        List<IndexShard> contributingShards,
        List<ArchiveBuilder.ArchiveBuildEntry> allEntries
    ) throws IOException {
        BlobPath archivePath = basePath.add("translog").add("data").add(hashPrefix).add(String.valueOf(genBucket));

        IOException lastFailure = null;
        for (int attempt = 0; attempt < UPLOAD_RETRY_MAX_ATTEMPTS; attempt++) {
            String blobName = TranslogArchivePathHelper.blobNameFromCurrentTime();
            long contentLength = ArchiveBuilder.computeSizeWithComment(allEntries);
            try (PipedOutputStream pos = new PipedOutputStream(); PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER_BYTES)) {
                AtomicReference<IOException> uploadError = new AtomicReference<>();
                Thread uploadThread = new Thread(() -> {
                    try {
                        transferService.uploadBlobStream(pis, contentLength, archivePath, blobName, WritePriority.HIGH, null);
                    } catch (IOException e) {
                        uploadError.set(e);
                    }
                }, "translog-archive-upload");
                uploadThread.start();
                try {
                    ArchiveBuilder.buildWithComment(pos, allEntries);
                } finally {
                    pos.close();
                }
                try {
                    uploadThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for archive upload", e);
                }
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
                "Archive uploaded path={} hashPrefix={} genBucket={} blob={}",
                archivePath.buildAsString(),
                hashPrefix,
                genBucket,
                blobName
            );
            break;
        }
        for (int i = 0; i < contributingShards.size(); i++) {
            final int shardIndex = i;
            Optional<TranslogTransferManager> managerOpt = contributingShards.get(i).getTranslogTransferManager();
            if (managerOpt.isPresent()) {
                try {
                    managerOpt.get().uploadMetadata(snapshots.get(i));
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
     * Runs one batch synchronously; for unit tests only.
     */
    void runBatchForTesting() {
        runBatch();
    }
}
