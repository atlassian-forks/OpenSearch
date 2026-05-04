/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogArchiveRecovery;
import org.opensearch.index.translog.transfer.TranslogCheckpointTransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.index.translog.transfer.archive.ArchiveDeletionHelper;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * A Translog implementation which syncs local FS with a remote store
 * The current impl uploads translog , ckp and metadata to remote store
 * for every sync, post syncing to disk. Post that, a new generation is
 * created.
 *
 * @opensearch.internal
 */
public class RemoteFsTranslog extends Translog {

    private final Logger logger;
    protected final TranslogTransferManager translogTransferManager;
    protected final FileTransferTracker fileTransferTracker;
    protected final BooleanSupplier startedPrimarySupplier;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private volatile long maxRemoteTranslogGenerationUploaded;

    private volatile long minSeqNoToKeep;

    // min generation referred by last uploaded translog
    protected volatile long minRemoteGenReferenced;

    // clean up translog folder uploaded by previous primaries once
    protected final SetOnce<Boolean> olderPrimaryCleaned = new SetOnce<>();

    protected static final int REMOTE_DELETION_PERMITS = 2;
    private static final int DOWNLOAD_RETRIES = 2;

    // Semaphore used to allow only single remote generation to happen at a time
    protected final Semaphore remoteGenerationDeletionPermits = new Semaphore(REMOTE_DELETION_PERMITS);

    // These permits exist to allow any inflight background triggered upload.
    private static final int SYNC_PERMIT = 1;
    private final Semaphore syncPermit = new Semaphore(SYNC_PERMIT);
    protected final AtomicBoolean pauseSync = new AtomicBoolean(false);
    private final boolean isTranslogMetadataEnabled;

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier startedPrimarySupplier,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        logger = Loggers.getLogger(getClass(), shardId);
        this.startedPrimarySupplier = startedPrimarySupplier;
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        isTranslogMetadataEnabled = indexSettings().isTranslogMetadataEnabled();
        this.translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            indexSettings().getRemoteStorePathStrategy(),
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            indexSettings().isTranslogArchiveUploadEnabled()
        );
        try {
            if (config.downloadRemoteTranslogOnInit()) {
                download(translogTransferManager, location, logger, config.shouldSeedRemote(), 0);
            }
            Checkpoint checkpoint = readCheckpoint(location);
            logger.info("Downloaded data from remote translog till maxSeqNo = {}", checkpoint.maxSeqNo);
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                String errorMsg = String.format(Locale.ROOT, "%s at least one reader must be recovered", shardId);
                logger.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            if (config.downloadRemoteTranslogOnInit() == false) {
                translogTransferManager.populateFileTrackerWithLocalState(this.readers);
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep
                // on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    // visible for testing
    RemoteTranslogTransferTracker getRemoteTranslogTracker() {
        return remoteTranslogTransferTracker;
    }

    public static void download(
        Repository repository,
        ShardId shardId,
        ThreadPool threadPool,
        Path location,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        Logger logger,
        boolean seedRemote,
        boolean isTranslogMetadataEnabled,
        boolean isTranslogArchiveUploadEnabled,
        long timestamp
    ) throws IOException {
        assert repository instanceof BlobStoreRepository : String.format(
            Locale.ROOT,
            "%s repository should be instance of BlobStoreRepository",
            shardId
        );
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        // We use a dummy stats tracker to ensure the flow doesn't break.
        // TODO: To be revisited as part of https://github.com/opensearch-project/OpenSearch/issues/7567
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 1000);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            pathStrategy,
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            isTranslogArchiveUploadEnabled
        );
        RemoteFsTranslog.download(translogTransferManager, location, logger, seedRemote, timestamp);
        logger.trace(remoteTranslogTransferTracker.toString());
    }

    // Visible for testing
    static void download(TranslogTransferManager translogTransferManager, Path location, Logger logger, boolean seedRemote, long timestamp)
        throws IOException {
        /*
        In Primary to Primary relocation , there can be concurrent upload and download of translog.
        While translog files are getting downloaded by new primary, it might hence be deleted by the primary
        Hence we retry if tlog/ckp files are not found .

        This doesn't happen in last download , where it is ensured that older primary has stopped modifying tlog data.
         */
        IOException ex = null;
        for (int i = 0; i <= DOWNLOAD_RETRIES; i++) {
            boolean success = false;
            long startTimeMs = System.currentTimeMillis();
            try {
                downloadOnce(translogTransferManager, location, logger, seedRemote, timestamp);
                success = true;
                return;
            } catch (FileNotFoundException | NoSuchFileException e) {
                // continue till download retries
                ex = e;
            } finally {
                logger.trace("downloadOnce success={} timeElapsed={}", success, (System.currentTimeMillis() - startTimeMs));
            }
        }
        logger.info("Exhausted all download retries during translog/checkpoint file download");
        throw ex;
    }

    private static void downloadOnce(
        TranslogTransferManager translogTransferManager,
        Path location,
        Logger logger,
        boolean seedRemote,
        long timestamp
    ) throws IOException {
        logger.debug("Downloading translog files from remote");
        RemoteTranslogTransferTracker statsTracker = translogTransferManager.getRemoteTranslogTransferTracker();
        long prevDownloadBytesSucceeded = statsTracker.getDownloadBytesSucceeded();
        long prevDownloadTimeInMillis = statsTracker.getTotalDownloadTimeInMillis();

        TranslogTransferMetadata translogMetadata = translogTransferManager.readMetadata(timestamp);
        if (translogMetadata != null) {
            if (Files.notExists(location)) {
                Files.createDirectories(location);
            }

            // Delete translog files on local before downloading from remote
            for (Path file : FileSystemUtils.files(location)) {
                Files.delete(file);
            }

            // Check if metadata has archive info (new school-bus model) or legacy per-shard info.
            String archiveBlobPath = translogMetadata.getArchiveBlobPath();
            Map<String, String> archiveEntryOffsets = translogMetadata.getArchiveEntryOffsets();

            if (archiveBlobPath != null && archiveEntryOffsets != null) {
                // Archive mode with explicit offsets: range-read from ZIP using metadata offsets.
                Map<String, String> generationToPrimaryTermMapper = translogMetadata.getGenerationToPrimaryTermMapper();
                try {
                    for (long i = translogMetadata.getGeneration(); i >= translogMetadata.getMinTranslogGeneration(); i--) {
                        String generation = Long.toString(i);
                        translogTransferManager.downloadTranslog(
                            generationToPrimaryTermMapper.get(generation),
                            generation,
                            location,
                            archiveBlobPath,
                            archiveEntryOffsets
                        );
                    }
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // A generation's entry was not found in the metadata's ZIP — it may live in an
                    // older ZIP (multi-cycle scenario). Fall back to 8-param ZIP scan with gen range.
                    logger.info(
                        "Archive entry not found in metadata TAR, falling back to TAR scan gen=[{}-{}]",
                        translogMetadata.getMinTranslogGeneration(),
                        translogMetadata.getGeneration()
                    );
                    IOUtils.rm(FileSystemUtils.files(location));
                    String indexUUID = translogTransferManager.getShardId().getIndex().getUUID();
                    recoverFromArchiveWithGenRange(
                        translogTransferManager,
                        indexUUID,
                        location,
                        logger,
                        translogMetadata.getMinTranslogGeneration(),
                        translogMetadata.getGeneration()
                    );
                }
            } else {
                // No archive offsets — try per-shard download first.
                // If archive mode is enabled but offsets are missing, fall back to ZIP scan on FileNotFoundException.
                Map<String, String> generationToPrimaryTermMapper = translogMetadata.getGenerationToPrimaryTermMapper();
                try {
                    for (long i = translogMetadata.getGeneration(); i >= translogMetadata.getMinTranslogGeneration(); i--) {
                        String generation = Long.toString(i);
                        translogTransferManager.downloadTranslog(generationToPrimaryTermMapper.get(generation), generation, location);
                    }
                } catch (FileNotFoundException | NoSuchFileException e) {
                    if (translogTransferManager.isTranslogArchiveUploadEnabled()) {
                        // Per-shard files not found — archive mode, fall back to ZIP range-read.
                        logger.info(
                            "Per-shard files not found in archive mode, falling back to TAR scan gen=[{}-{}]",
                            translogMetadata.getMinTranslogGeneration(),
                            translogMetadata.getGeneration()
                        );
                        IOUtils.rm(FileSystemUtils.files(location));
                        String indexUUID = translogTransferManager.getShardId().getIndex().getUUID();
                        recoverFromArchiveWithGenRange(
                            translogTransferManager,
                            indexUUID,
                            location,
                            logger,
                            translogMetadata.getMinTranslogGeneration(),
                            translogMetadata.getGeneration()
                        );
                    } else {
                        throw e;
                    }
                }
            }

            logger.info(
                "Downloaded translog and checkpoint files from={} to={}",
                translogMetadata.getMinTranslogGeneration(),
                translogMetadata.getGeneration()
            );

            statsTracker.recordDownloadStats(prevDownloadBytesSucceeded, prevDownloadTimeInMillis);

            // Copy latest generation .ckp to translog.ckp for flows that depend on its existence.
            // recoverFromArchiveWithGenRange copies it internally; for direct downloads, do it here.
            Path commitCkpForCopy = location.resolve(Translog.getCommitCheckpointFileName(translogMetadata.getGeneration()));
            if (Files.exists(commitCkpForCopy)) {
                Path destCkp = location.resolve(Translog.CHECKPOINT_FILE_NAME);
                if (Files.exists(destCkp)) {
                    Files.delete(destCkp);
                }
                Files.copy(commitCkpForCopy, destCkp);
            }
        } else {
            // No metadata found.
            // Use the settings flag (not in-memory coordinator registry) to decide between
            // archive ZIP scan and legacy local-cleanup. The flag is reliable at recovery time
            // (before coordinator re-registers after node restart), unlike the registry.
            if (translogTransferManager.isTranslogArchiveUploadEnabled() && seedRemote == false) {
                logger.info("Archive enabled, no metadata found: attempting TAR scan recovery");
                String indexUUID = translogTransferManager.getShardId().getIndex().getUUID();
                recoverFromArchive(
                    translogTransferManager,
                    indexUUID,
                    location,
                    seedRemote,
                    logger,
                    statsTracker,
                    prevDownloadBytesSucceeded,
                    prevDownloadTimeInMillis
                );
            } else {
                // No metadata and archive disabled (or seedRemote): fresh/empty shard path.
                logger.debug("No translog files found on remote, checking local filesystem for cleanup");
                if (FileSystemUtils.exists(location.resolve(CHECKPOINT_FILE_NAME))) {
                    final Checkpoint checkpoint = readCheckpoint(location);
                    if (seedRemote) {
                        logger.debug("Remote migration ongoing. Retaining the translog on local, skipping clean-up");
                    } else if (isEmptyTranslog(checkpoint) == false) {
                        logger.debug("Translog files exist on local without any metadata in remote, cleaning up these files");
                        Translog.createEmptyTranslog(location, translogTransferManager.getShardId(), checkpoint);
                    } else {
                        logger.debug("Empty translog on local, skipping clean-up");
                    }
                }
            }
        }
        logger.debug("downloadOnce execution completed");
    }

    private static boolean isEmptyTranslog(Checkpoint checkpoint) {
        return checkpoint.generation == checkpoint.minTranslogGeneration
            && checkpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED
            && checkpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED
            && checkpoint.numOps == 0;
    }

    /**
     * Archive TAR recovery with explicit generation range (when metadata provides minGen/maxGen).
     * Uses {@link TranslogArchiveRecovery#recoverFromHierarchicalPath} to scan the hierarchical txlog/ path.
     * Writes files directly to {@code location} (not via temp dir).
     */
    private static void recoverFromArchiveWithGenRange(
        TranslogTransferManager translogTransferManager,
        String indexUUID,
        Path location,
        Logger logger,
        long minGeneration,
        long maxGeneration
    ) throws IOException {
        if (Files.notExists(location)) {
            Files.createDirectories(location);
        }
        for (Path file : FileSystemUtils.files(location)) {
            Files.delete(file);
        }
        // Try new hierarchical TAR path first (txlog/{day}/{minute}/ structure).
        // Fall back to old ZIP path if nothing found (backward compatibility).
        // Use Instant.now() as the segment timestamp so we scan from (now - 2 min) forward.
        // If nothing found in the recent window, the ZIP fallback handles older data.
        boolean recoveredFromTar = TranslogArchiveRecovery.recoverFromHierarchicalPath(
            translogTransferManager.getTransferService(),
            translogTransferManager.getArchiveBasePath(),
            indexUUID,
            translogTransferManager.getShardId().id(),
            minGeneration,
            maxGeneration,
            location,
            Instant.now(),
            null
        );
        logger.info(
            "Archive gen-range recovery complete for shard {} gen=[{}-{}] found={}",
            translogTransferManager.getShardId(),
            minGeneration,
            maxGeneration,
            recoveredFromTar
        );
    }

    /**
     * Attempts recovery from archive TARs via {@link TranslogArchiveRecovery}.
     * Uses a sibling temp directory to avoid destroying local translog files if no TARs are found (fresh shard).
     * On success, moves recovered files into {@code location} and records download stats.
     * On fresh shard (no TARs), falls through to local cleanup (create empty translog if needed).
     */
    private static void recoverFromArchive(
        TranslogTransferManager translogTransferManager,
        String indexUUID,
        Path location,
        boolean seedRemote,
        Logger logger,
        RemoteTranslogTransferTracker statsTracker,
        long prevDownloadBytesSucceeded,
        long prevDownloadTimeInMillis
    ) throws IOException {
        Path tempRecoveryDir = location.resolveSibling("translog_archive_recovery_tmp");
        if (Files.exists(tempRecoveryDir)) {
            IOUtils.rm(tempRecoveryDir);
        }
        Files.createDirectories(tempRecoveryDir);
        try {
            // Try new hierarchical TAR path (txlog/{day}/{minute}/) first.
            // Use epoch + 2min as segment timestamp so we scan from epoch forward = all archives.
            // Fall back to legacy ZIP scan if no TARs found.
            boolean recoveredFromTar = TranslogArchiveRecovery.recoverFromHierarchicalPath(
                translogTransferManager.getTransferService(),
                translogTransferManager.getArchiveBasePath(),
                indexUUID,
                translogTransferManager.getShardId().id(),
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                tempRecoveryDir,
                // Epoch + 2min so startFrom = epoch, meaning scan all archives
                Instant.EPOCH.plus(Duration.ofMinutes(2)),
                null
            );
            if (!recoveredFromTar) {
                logger.warn("No TAR archives found in hierarchical path for shard {}", translogTransferManager.getShardId());
            }
            if (FileSystemUtils.exists(tempRecoveryDir.resolve(CHECKPOINT_FILE_NAME))) {
                if (Files.notExists(location)) {
                    Files.createDirectories(location);
                }
                for (Path file : FileSystemUtils.files(location)) {
                    Files.delete(file);
                }
                for (Path recoveredFile : FileSystemUtils.files(tempRecoveryDir)) {
                    Files.move(recoveredFile, location.resolve(recoveredFile.getFileName().toString()));
                }
                statsTracker.recordDownloadStats(prevDownloadBytesSucceeded, prevDownloadTimeInMillis);
                logger.info("TAR recovery succeeded for shard {}", translogTransferManager.getShardId());
            } else {
                // No ZIPs found. This can happen when archive was just toggled ON (OFF→ON) and no ZIP
                // has been uploaded yet — per-shard tlog files still exist in remote.
                // Fall back to per-shard metadata download (+1 LIST) before treating as fresh shard.
                // This prevents data loss in the one-time transition window after enabling archive.
                logger.info(
                    "No archive TARs found for shard {}, attempting per-shard metadata fallback (OFF→ON toggle)",
                    translogTransferManager.getShardId()
                );
                TranslogTransferMetadata fallbackMetadata = translogTransferManager.readMetadata(0);
                if (fallbackMetadata != null) {
                    logger.info(
                        "Per-shard metadata found, recovering from per-shard tlog files gen=[{}-{}]",
                        fallbackMetadata.getMinTranslogGeneration(),
                        fallbackMetadata.getGeneration()
                    );
                    if (Files.notExists(location)) {
                        Files.createDirectories(location);
                    }
                    for (Path file : FileSystemUtils.files(location)) {
                        Files.delete(file);
                    }
                    Map<String, String> generationToPrimaryTermMapper = fallbackMetadata.getGenerationToPrimaryTermMapper();
                    for (long i = fallbackMetadata.getGeneration(); i >= fallbackMetadata.getMinTranslogGeneration(); i--) {
                        translogTransferManager.downloadTranslog(
                            generationToPrimaryTermMapper.get(Long.toString(i)),
                            Long.toString(i),
                            location
                        );
                    }
                    statsTracker.recordDownloadStats(prevDownloadBytesSucceeded, prevDownloadTimeInMillis);
                    Path commitCkp = location.resolve(Translog.getCommitCheckpointFileName(fallbackMetadata.getGeneration()));
                    if (Files.exists(commitCkp)) {
                        Path destCkp = location.resolve(Translog.CHECKPOINT_FILE_NAME);
                        if (Files.exists(destCkp)) Files.delete(destCkp);
                        Files.copy(commitCkp, destCkp);
                    }
                    logger.info("Per-shard fallback recovery succeeded for shard {}", translogTransferManager.getShardId());
                } else {
                    // Truly fresh shard — no TARs and no per-shard metadata.
                    logger.info("No archive TARs and no per-shard metadata (fresh shard), checking local filesystem for cleanup");
                    if (FileSystemUtils.exists(location.resolve(CHECKPOINT_FILE_NAME))) {
                        final Checkpoint checkpoint = readCheckpoint(location);
                        if (seedRemote) {
                            logger.debug("Remote migration ongoing. Retaining the translog on local, skipping clean-up");
                        } else if (isEmptyTranslog(checkpoint) == false) {
                            logger.debug("Translog files exist on local without any remote archive, cleaning up these files");
                            Translog.createEmptyTranslog(location, translogTransferManager.getShardId(), checkpoint);
                        } else {
                            logger.debug("Empty translog on local, skipping clean-up");
                        }
                    }
                }
            }
        } finally {
            IOUtils.rm(tempRecoveryDir);
        }
    }

    public static TranslogTransferManager buildTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker tracker,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled,
        boolean isTranslogArchiveUploadEnabled
    ) {
        assert Objects.nonNull(pathStrategy);
        String indexUUID = shardId.getIndex().getUUID();
        String shardIdStr = String.valueOf(shardId.id());
        RemoteStorePathStrategy.PathInput dataPathInput = RemoteStorePathStrategy.PathInput.builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(DATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath dataPath = pathStrategy.generatePath(dataPathInput);
        RemoteStorePathStrategy.PathInput mdPathInput = RemoteStorePathStrategy.PathInput.builder()
            .basePath(blobStoreRepository.basePath())
            .indexUUID(indexUUID)
            .shardId(shardIdStr)
            .dataCategory(TRANSLOG)
            .dataType(METADATA)
            .fixedPrefix(remoteStoreSettings.getTranslogPathFixedPrefix())
            .build();
        BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
        BlobStoreTransferService transferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool);
        return new TranslogTransferManager(
            shardId,
            transferService,
            dataPath,
            mdPath,
            blobStoreRepository.basePath(),
            fileTransferTracker,
            tracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            isTranslogArchiveUploadEnabled
        );
    }

    @Override
    public boolean ensureSynced(Location location) throws IOException {
        assert location.generation <= current.getGeneration();
        if (location.generation == current.getGeneration()) {
            ensureOpen();
            return prepareAndUpload(primaryTermSupplier.getAsLong(), location.generation);
        }
        return false;
    }

    @Override
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        prepareAndUpload(primaryTermSupplier.getAsLong(), null);
    }

    private boolean prepareAndUpload(Long primaryTerm, Long generation) throws IOException {
        // During primary relocation, both the old and new primary have engine created with RemoteFsTranslog and having
        // ReplicationTracker.primaryMode() as true. However, before we perform the `internal:index/shard/replication/segments_sync`
        // action which re-downloads the segments and translog on the new primary. We are ensuring 2 things here -
        // 1. Using startedPrimarySupplier, we prevent the new primary to do pre-emptive syncs
        // 2. Using syncPermits, we prevent syncs at the desired time during primary relocation.
        if (startedPrimarySupplier.getAsBoolean() == false || syncPermit.tryAcquire(SYNC_PERMIT) == false) {
            logger.debug("skipped uploading translog for {} {} syncPermits={}", primaryTerm, generation, syncPermit.availablePermits());
            // NO-OP
            return false;
        }
        long maxSeqNo = -1;
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    if (closed.get() == false) {
                        maxSeqNo = getMaxSeqNo();
                    }
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                    }
                    assert writeLock.isHeldByCurrentThread() : "Write lock must be held before we acquire the read lock";
                    // Here we are downgrading the write lock by acquiring the read lock and releasing the write lock
                    // This ensures that other threads can still acquire the read locks while also protecting the
                    // readers and writer to not be mutated any further.
                    readLock.acquire();
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            } else if (generation < current.getGeneration()) {
                return false;
            }
        }

        assert readLock.isHeldByCurrentThread() == true;
        try (Releasable ignored = readLock; Releasable ignoredGenLock = deletionPolicy.acquireTranslogGen(getMinFileGeneration())) {
            // Do we need remote writes in sync fashion ?
            // If we don't , we should swallow FileAlreadyExistsException while writing to remote store
            // and also verify for same during primary-primary relocation
            // Writing remote in sync fashion doesn't hurt as global ckp update
            // is not updated in remote translog except in primary to primary recovery.
            if (generation == null) {
                if (closed.get() == false) {
                    return upload(primaryTerm, current.getGeneration() - 1, maxSeqNo);
                } else {
                    return upload(primaryTerm, current.getGeneration(), maxSeqNo);
                }
            } else {
                return upload(primaryTerm, generation, maxSeqNo);
            }
        }
    }

    private boolean upload(long primaryTerm, long generation, long maxSeqNo) throws IOException {
        // When archive upload is enabled, submit data to the node-scoped batch coordinator.
        // The coordinator is a singleton for the node (not per-index), obtained via TranslogConfig
        // to avoid static lookup. It bundles all shards across ALL indices into a single TAR and
        // blocks until the upload completes (durability=REQUEST preserved).
        // Obtain the node-scoped coordinator from the static singleton (set during node start).
        // This avoids threading TranslogArchiveCollector through 5 constructor layers while
        // remaining safe: there is exactly one collector per JVM (OpenSearch node process).
        TranslogArchiveCollector archiveCollector = TranslogArchiveCollector.getInstance();
        TranslogArchiveBatchCoordinator archiveBatchCoordinator = archiveCollector != null
            ? archiveCollector.getNodeCoordinator()
            : null;
        if (indexSettings().isTranslogArchiveUploadEnabled() && archiveBatchCoordinator != null) {
            try {
                // Provide the repo base path to the coordinator on first use so uploads and recovery
                // use the same root path (blobStoreRepository.basePath()).
                archiveBatchCoordinator.initArchiveBasePath(translogTransferManager.getArchiveBasePath());
                logger.trace("submitting to archive batch coordinator for primary term {} generation {}", primaryTerm, generation);
                List<TarArchiveBuilder.ArchiveBuildEntry> entries = buildArchiveEntries(primaryTerm, generation);
                // Provide seqNo stats so the batch coordinator can embed GC entries in the TAR.
                // The GC scanner uses these to determine when it is safe to delete old TARs.
                long minSeqNo = getMinUnreferencedSeqNoInSegments(globalCheckpointSupplier.getAsLong());
                long globalCheckpoint = globalCheckpointSupplier.getAsLong();
                TranslogArchiveBatchCoordinator.ShardArchiveData shardData = new TranslogArchiveBatchCoordinator.ShardArchiveData(
                    shardId.getIndex().getUUID(),  // indexUUID — required since coordinator is node-scoped
                    shardId.id(),
                    primaryTerm,
                    generation,
                    getMinFileGeneration(),
                    entries,
                    minSeqNo,
                    maxSeqNo,
                    globalCheckpoint
                );
                archiveBatchCoordinator.submitAndWait(shardData, translogTransferManager.getTransferService());
                // No per-shard metadata upload — recovery uses TranslogArchiveRecovery to find
                // and download translog files directly from archive ZIPs via binary search.
                maxRemoteTranslogGenerationUploaded = generation;
                minRemoteGenReferenced = getMinFileGeneration();
                return true;
            } finally {
                syncPermit.release(SYNC_PERMIT);
            }
        }
        logger.trace("uploading translog for primary term {} generation {}", primaryTerm, generation);
        try (
            TranslogCheckpointTransferSnapshot transferSnapshotProvider = new TranslogCheckpointTransferSnapshot.Builder(
                primaryTerm,
                generation,
                location,
                readers,
                Translog::getCommitCheckpointFileName,
                config.getNodeId()
            ).build()
        ) {
            return translogTransferManager.transferSnapshot(
                transferSnapshotProvider,
                new RemoteFsTranslogTransferListener(generation, primaryTerm, maxSeqNo)
            );
        } finally {
            syncPermit.release(SYNC_PERMIT);
        }

    }

    /**
     * Maximum total bytes for a single shard's translog archive entries.
     * Translog files are typically small (KB-MB), but a guard prevents OOM on pathological cases.
     */
    private static final long MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES = 128 * 1024 * 1024L;

    /**
     * Builds archive entries (tlog + ckp) for the given generation from the current readers.
     * Entry paths follow the pattern: {indexUUID}/{shardId}/{primaryTerm}/{filename}
     *
     * @throws IOException if files exceed MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES or I/O fails
     */
    private List<TarArchiveBuilder.ArchiveBuildEntry> buildArchiveEntries(long primaryTerm, long generation) throws IOException {
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>();
        String indexUUID = shardId.getIndex().getUUID();
        String prefix = indexUUID + "/" + shardId.id() + "/" + primaryTerm + "/";

        String tlogFilename = Translog.getFilename(generation);
        String ckpFilename = Translog.getCommitCheckpointFileName(generation);

        Path tlogFile = location.resolve(tlogFilename);
        Path ckpFile = location.resolve(ckpFilename);

        // Use lazy streaming entries (fromPath) — no file content read into heap here.
        // TarArchiveBuilder.build() streams each file through the TAR pipe during upload,
        // keeping peak memory bounded to the pipe buffer (256 KB) regardless of file sizes.
        long totalBytes = 0;
        if (Files.exists(tlogFile)) {
            long tlogSize = Files.size(tlogFile);
            totalBytes += tlogSize;
            if (totalBytes > MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES) {
                throw new IOException(
                    "Translog archive entry size "
                        + totalBytes
                        + " exceeds limit "
                        + MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES
                        + " for shard "
                        + shardId
                        + " gen "
                        + generation
                );
            }
            entries.add(TarArchiveBuilder.fromPath(prefix + tlogFilename, tlogFile, tlogSize));
        }
        if (Files.exists(ckpFile)) {
            long ckpSize = Files.size(ckpFile);
            totalBytes += ckpSize;
            if (totalBytes > MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES) {
                throw new IOException(
                    "Translog archive entry size "
                        + totalBytes
                        + " exceeds limit "
                        + MAX_TRANSLOG_ARCHIVE_ENTRY_BYTES
                        + " for shard "
                        + shardId
                        + " gen "
                        + generation
                );
            }
            entries.add(TarArchiveBuilder.fromPath(prefix + ckpFilename, ckpFile, ckpSize));
        }

        return entries;
    }

    public void buildSnapshotForArchive(BiConsumer<TransferSnapshot, Runnable> consumer) throws IOException {
        if (startedPrimarySupplier.getAsBoolean() == false || syncPermit.tryAcquire(SYNC_PERMIT) == false) {
            return;
        }
        Releasable genLock = null;
        Releasable readLockRef = null;
        try {
            long maxSeqNo = -1;
            long snapshotGeneration;
            try (Releasable ignored = writeLock.acquire()) {
                try {
                    if (closed.get() == false) {
                        maxSeqNo = getMaxSeqNo();
                    }
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    snapshotGeneration = current.getGeneration();
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                    }
                    assert writeLock.isHeldByCurrentThread();
                    readLockRef = readLock.acquire();
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            }
            long primaryTerm = primaryTermSupplier.getAsLong();
            genLock = deletionPolicy.acquireTranslogGen(getMinFileGeneration());
            final Releasable genLockForRelease = genLock;
            TranslogCheckpointTransferSnapshot snapshot = new TranslogCheckpointTransferSnapshot.Builder(
                primaryTerm,
                snapshotGeneration,
                location,
                readers,
                Translog::getCommitCheckpointFileName,
                config.getNodeId()
            ).build();
            if (readLockRef != null) {
                readLockRef.close();
                readLockRef = null;
            }
            Runnable release = () -> {
                Releasables.close(genLockForRelease);
                syncPermit.release(SYNC_PERMIT);
            };
            consumer.accept(snapshot, release);
        } catch (Exception e) {
            Releasables.close(genLock);
            if (readLockRef != null) {
                Releasables.close(readLockRef);
            }
            syncPermit.release(SYNC_PERMIT);
            throw e;
        }
    }

    public TranslogTransferManager getTranslogTransferManager() {
        return translogTransferManager;
    }

    /**
     * Returns retention bounds for archive deletion: do not delete archives containing entries with
     * primary term >= minPrimaryTermToKeep or (primary term == minPrimaryTermToKeep and generation >= minGenerationToKeep).
     * Used by the node-level archive collector to build retentionByShard for deleteArchivesOlderThanRetention.
     */
    public Optional<ArchiveDeletionHelper.RetentionBounds> getArchiveRetentionBounds() {
        long minPrimaryTermToKeep;
        try (ReleasableLock ignored = readLock.acquire()) {
            if (readers.isEmpty()) {
                minPrimaryTermToKeep = primaryTermSupplier.getAsLong();
            } else {
                minPrimaryTermToKeep = readers.stream().map(BaseTranslogReader::getPrimaryTerm).min(Long::compare).get();
            }
        } catch (AlreadyClosedException e) {
            return Optional.empty();
        }
        long minGen = minRemoteGenReferenced - indexSettings().getRemoteTranslogExtraKeep();
        if (minGen < 0) {
            minGen = 0;
        }
        // Use the dedicated archive retention setting (default 10 min, min 5 min safety buffer).
        // See IndexSettings.INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_RETENTION_SETTING.
        long retentionMinutes = indexSettings().getTranslogArchiveRetention().getMinutes();
        return Optional.of(new ArchiveDeletionHelper.RetentionBounds(minPrimaryTermToKeep, minGen, retentionMinutes));
    }

    // Visible for testing
    public Set<String> allUploaded() {
        return fileTransferTracker.allUploaded();
    }

    private boolean syncToDisk() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.sync();
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    @Override
    public void sync() throws IOException {
        if (syncToDisk() || syncNeeded()) {
            prepareAndUpload(primaryTermSupplier.getAsLong(), null);
        }
    }

    /**
     * Returns <code>true</code> if an fsync and/or remote transfer is required to ensure durability of the translogs operations or it's metadata.
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            if (current.syncNeeded()) {
                return true;
            }
            // In archive mode an empty generation has no translog data to ZIP — skip to avoid wasted PUT requests.
            if (indexSettings().isTranslogArchiveUploadEnabled()) {
                return false;
            }
            return maxRemoteTranslogGenerationUploaded + 1 < this.currentFileGeneration() && current.totalOperations() == 0;
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose() : shardId
            + "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        try (ReleasableLock lock = writeLock.acquire()) {
            if (closed.compareAndSet(false, true)) {
                try {
                    sync();
                } finally {
                    logger.debug("translog closed");
                    closeFilesIfNoPendingRetentionLocks();
                }
            }
        }
    }

    protected long getMinReferencedGen() throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        long minReferencedGen = Math.min(
            deletionPolicy.minTranslogGenRequired(readers, current),
            minGenerationForSeqNo(minSeqNoToKeep, current, readers)
        );

        assert minReferencedGen >= getMinFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] but the lowest gen available is ["
            + getMinFileGeneration()
            + "]";
        assert minReferencedGen <= currentFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] which is higher than the current generation ["
            + currentFileGeneration()
            + "]";
        return minReferencedGen;
    }

    protected void setMinSeqNoToKeep(long seqNo) {
        if (seqNo < this.minSeqNoToKeep) {
            throw new IllegalArgumentException(
                shardId + " min seq number required can't go backwards: " + "current [" + this.minSeqNoToKeep + "] new [" + seqNo + "]"
            );
        }
        this.minSeqNoToKeep = seqNo;
    }

    @Override
    protected Releasable drainSync() {
        try {
            if (syncPermit.tryAcquire(SYNC_PERMIT, 1, TimeUnit.MINUTES)) {
                boolean result = pauseSync.compareAndSet(false, true);
                assert result && syncPermit.availablePermits() == 0;
                logger.info("All inflight remote translog syncs finished and further syncs paused");
                return Releasables.releaseOnce(() -> {
                    syncPermit.release(SYNC_PERMIT);
                    boolean wasSyncPaused = pauseSync.getAndSet(false);
                    assert syncPermit.availablePermits() == SYNC_PERMIT : "Available permits is " + syncPermit.availablePermits();
                    assert wasSyncPaused : "RemoteFsTranslog sync was not paused before re-enabling it";
                    logger.info("Resumed remote translog sync back on relocation failure");
                });
            } else {
                throw new TimeoutException("Timeout while acquiring all permits");
            }
        } catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException("Failed to acquire all permits", e);
        }
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {
        // clean up local translog files and updates readers
        super.trimUnreferencedReaders();

        // When archive upload is enabled, individual translog files are not uploaded to remote,
        // so there is nothing to clean up. Archive retention is handled by TranslogArchiveCollector.
        if (indexSettings().isTranslogArchiveUploadEnabled()) {
            return;
        }

        // This is to ensure that after the permits are acquired during primary relocation, there are no further modification on remote
        // store.
        if (startedPrimarySupplier.getAsBoolean() == false || pauseSync.get()) {
            return;
        }

        // Since remote generation deletion is async, this ensures that only one generation deletion happens at a time.
        // Remote generations involves 2 async operations - 1) Delete translog generation files 2) Delete metadata files
        // We try to acquire 2 permits and if we can not, we return from here itself.
        if (remoteGenerationDeletionPermits.tryAcquire(REMOTE_DELETION_PERMITS) == false) {
            return;
        }

        // cleans up remote translog files not referenced in latest uploaded metadata.
        // This enables us to restore translog from the metadata in case of failover or relocation.
        Set<Long> generationsToDelete = new HashSet<>();
        for (long generation = minRemoteGenReferenced - 1 - indexSettings().getRemoteTranslogExtraKeep(); generation >= 0; generation--) {
            if (fileTransferTracker.uploaded(Translog.getFilename(generation)) == false) {
                break;
            }
            generationsToDelete.add(generation);
        }
        if (generationsToDelete.isEmpty() == false) {
            deleteRemoteGeneration(generationsToDelete);
            translogTransferManager.deleteStaleTranslogMetadataFilesAsync(remoteGenerationDeletionPermits::release);
            deleteStaleRemotePrimaryTerms();
        } else {
            remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
        }
    }

    /**
     * Deletes remote translog and metadata files asynchronously corresponding to the generations.
     * @param generations generations to be deleted.
     */
    private void deleteRemoteGeneration(Set<Long> generations) {
        translogTransferManager.deleteGenerationAsync(
            primaryTermSupplier.getAsLong(),
            generations,
            remoteGenerationDeletionPermits::release
        );
    }

    /**
     * This method must be called only after there are valid generations to delete in trimUnreferencedReaders as it ensures
     * implicitly that minimum primary term in latest translog metadata in remote store is the current primary term.
     * <br>
     * This will also delete all stale translog metadata files from remote except the latest basis the metadata file comparator.
     */
    private void deleteStaleRemotePrimaryTerms() {
        // The deletion of older translog files in remote store is on best-effort basis, there is a possibility that there
        // are older files that are no longer needed and should be cleaned up. In here, we delete all files that are part
        // of older primary term.
        if (olderPrimaryCleaned.trySet(Boolean.TRUE)) {
            if (readers.isEmpty()) {
                logger.trace("Translog reader list is empty, returning from deleteStaleRemotePrimaryTerms");
                return;
            }
            // First we delete all stale primary terms folders from remote store
            long minimumReferencedPrimaryTerm = readers.stream().map(BaseTranslogReader::getPrimaryTerm).min(Long::compare).get();
            translogTransferManager.deletePrimaryTermsAsync(minimumReferencedPrimaryTerm);
        }
    }

    public static void cleanup(
        Repository repository,
        ShardId shardId,
        ThreadPool threadPool,
        RemoteStorePathStrategy pathStrategy,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled
    ) throws IOException {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        // We use a dummy stats tracker to ensure the flow doesn't break.
        // TODO: To be revisited as part of https://github.com/opensearch-project/OpenSearch/issues/7567
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 1000);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            pathStrategy,
            remoteStoreSettings,
            isTranslogMetadataEnabled,
            false
        );
        // clean up all remote translog files
        translogTransferManager.deleteTranslogFiles();
    }

    protected void onDelete() {
        ClusterService.assertClusterOrClusterManagerStateThread();
        // clean up all remote translog files
        translogTransferManager.delete();
    }

    // Visible for testing
    boolean isRemoteGenerationDeletionPermitsAvailable() {
        return remoteGenerationDeletionPermits.availablePermits() == REMOTE_DELETION_PERMITS;
    }

    /**
     * TranslogTransferListener implementation for RemoteFsTranslog
     *
     * @opensearch.internal
     */
    private class RemoteFsTranslogTransferListener implements TranslogTransferListener {

        /**
         * Generation for the translog
         */
        private final long generation;

        /**
         * Primary Term for the translog
         */
        private final long primaryTerm;

        private final long maxSeqNo;

        RemoteFsTranslogTransferListener(long generation, long primaryTerm, long maxSeqNo) {
            this.generation = generation;
            this.primaryTerm = primaryTerm;
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
            maxRemoteTranslogGenerationUploaded = generation;
            minRemoteGenReferenced = getMinFileGeneration();
            logger.debug(
                "Successfully uploaded translog for primary term = {}, generation = {}, maxSeqNo = {}",
                primaryTerm,
                generation,
                maxSeqNo
            );
        }

        @Override
        public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
            if (ex instanceof IOException) {
                throw (IOException) ex;
            } else {
                throw (RuntimeException) ex;
            }
        }
    }

    @Override
    public long getMinUnreferencedSeqNoInSegments(long minUnrefCheckpointInLastCommit) {
        return minSeqNoToKeep;
    }

    // Visible for testing
    int availablePermits() {
        return syncPermit.availablePermits();
    }

    /**
     * Checks whether or not the shard should be flushed based on translog files.
     * This checks if number of translog files breaches the threshold count determined by
     * {@code cluster.remote_store.translog.max_readers} setting
     * @return {@code true} if the shard should be flushed
     */
    @Override
    protected boolean shouldFlush() {
        int maxRemoteTlogReaders = translogTransferManager.getMaxRemoteTranslogReadersSettings();
        if (maxRemoteTlogReaders == -1) {
            return false;
        }
        return readers.size() >= maxRemoteTlogReaders;
    }
}
