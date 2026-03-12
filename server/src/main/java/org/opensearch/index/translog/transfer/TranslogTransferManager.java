/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.archive.ArchiveCommentFormat;
import org.opensearch.index.translog.transfer.archive.ArchiveEntry;
import org.opensearch.index.translog.transfer.archive.ArchiveIndexEntry;
import org.opensearch.index.translog.transfer.archive.TranslogArchivePathHelper;
import org.opensearch.index.translog.transfer.archive.ZipCentralDirectoryParser;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

/**
 * The class responsible for orchestrating the transfer of a {@link TransferSnapshot} via a {@link TransferService}
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class TranslogTransferManager {

    private final ShardId shardId;
    private final TransferService transferService;
    private final BlobPath remoteDataTransferPath;
    private final BlobPath remoteMetadataTransferPath;
    private final FileTransferTracker fileTransferTracker;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private final RemoteStoreSettings remoteStoreSettings;
    private static final int METADATA_FILES_TO_FETCH = 10;
    /** Max archive blobs to list per node when scanning for download; same as retention run. */
    private static final int MAX_ARCHIVE_BLOBS_PER_NODE = 500;
    /** Gen bucket size; must match TranslogArchiveCollector (translog/data/{hashPrefix}/{genBucket}). */
    private static final int GEN_BUCKET_SIZE = 100;
    // Flag to include checkpoint file data as translog file metadata during upload/download
    private final boolean isTranslogMetadataEnabled;
    final static String CHECKPOINT_FILE_DATA_KEY = "ckp-data";

    private final Logger logger;

    private static final VersionedCodecStreamWrapper<TranslogTransferMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new TranslogTransferMetadataHandler(),
        TranslogTransferMetadata.CURRENT_VERSION,
        TranslogTransferMetadata.METADATA_CODEC
    );

    public TranslogTransferManager(
        ShardId shardId,
        TransferService transferService,
        BlobPath remoteDataTransferPath,
        BlobPath remoteMetadataTransferPath,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        boolean isTranslogMetadataEnabled
    ) {
        this.shardId = shardId;
        this.transferService = transferService;
        this.remoteDataTransferPath = remoteDataTransferPath;
        this.remoteMetadataTransferPath = remoteMetadataTransferPath;
        this.fileTransferTracker = fileTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.remoteStoreSettings = remoteStoreSettings;
        this.isTranslogMetadataEnabled = isTranslogMetadataEnabled;
    }

    public RemoteTranslogTransferTracker getRemoteTranslogTransferTracker() {
        return remoteTranslogTransferTracker;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public TransferService getTransferService() {
        return transferService;
    }

    public BlobPath getRemoteDataTransferPath() {
        return remoteDataTransferPath;
    }

    /**
     * Reads the latest N translog metadata files from remote store using filename parsing.
     *
     * @param count Number of metadata files to read
     * @return Map of filename to parsed TranslogTransferMetadata
     * @throws IOException if the fetch or parsing fails
     */
    public Map<String, TranslogTransferMetadata> readLatestNMetadataFiles(int count) throws IOException {
        List<BlobMetadata> metadataFiles = transferService.listAllInSortedOrder(
            remoteMetadataTransferPath,
            TranslogTransferMetadata.METADATA_PREFIX,
            count
        );

        Map<String, TranslogTransferMetadata> result = new java.util.LinkedHashMap<>();
        for (BlobMetadata metadata : metadataFiles) {
            String fileName = metadata.name();
            try {
                TranslogTransferMetadata meta = readMetadata(fileName);
                result.put(fileName, meta);
            } catch (Exception e) {
                logger.error("Failed to read translog metadata file ", e);
            }
        }

        return result;
    }

    public boolean transferSnapshot(
        TransferSnapshot transferSnapshot,
        TranslogTransferListener translogTransferListener,
        org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
    ) throws IOException {
        List<Exception> exceptionList = new ArrayList<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        Set<TransferFileSnapshot> toUpload = new HashSet<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        long metadataBytesToUpload;
        long metadataUploadStartTime;
        long uploadStartTime;
        long prevUploadBytesSucceeded = remoteTranslogTransferTracker.getUploadBytesSucceeded();
        long prevUploadTimeInMillis = remoteTranslogTransferTracker.getTotalUploadTimeInMillis();

        try {
            if (isTranslogMetadataEnabled) {
                toUpload.addAll(fileTransferTracker.exclusionFilter(transferSnapshot.getTranslogFileSnapshotWithMetadata()));
            } else {
                toUpload.addAll(fileTransferTracker.exclusionFilter(transferSnapshot.getTranslogFileSnapshots()));
                toUpload.addAll(fileTransferTracker.exclusionFilter((transferSnapshot.getCheckpointFileSnapshots())));
            }
            if (toUpload.isEmpty()) {
                logger.trace("Nothing to upload for transfer");
                return true;
            }

            fileTransferTracker.recordBytesForFiles(toUpload);
            captureStatsBeforeUpload();
            final CountDownLatch latch = new CountDownLatch(toUpload.size());
            LatchedActionListener<TransferFileSnapshot> latchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(fileTransferTracker::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Exception during transfer for file {}",
                            ((FileTransferException) ex).getFileSnapshot().getName()
                        ),
                        ex
                    );
                    FileTransferException e = (FileTransferException) ex;
                    TransferFileSnapshot file = e.getFileSnapshot();
                    fileTransferTracker.onFailure(file, ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            Map<Long, BlobPath> blobPathMap = new HashMap<>();
            toUpload.forEach(
                fileSnapshot -> blobPathMap.put(
                    fileSnapshot.getPrimaryTerm(),
                    remoteDataTransferPath.add(String.valueOf(fileSnapshot.getPrimaryTerm()))
                )
            );

            uploadStartTime = System.nanoTime();
            // TODO: Ideally each file's upload start time should be when it is actually picked for upload
            // https://github.com/opensearch-project/OpenSearch/issues/9729
            fileTransferTracker.recordFileTransferStartTime(uploadStartTime);
            transferService.uploadBlobs(toUpload, blobPathMap, latchedActionListener, WritePriority.HIGH);

            try {
                if (latch.await(remoteStoreSettings.getClusterRemoteTranslogTransferTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                    Exception ex = new TranslogUploadFailedException(
                        "Timed out waiting for transfer of snapshot " + transferSnapshot + " to complete"
                    );
                    exceptionList.forEach(ex::addSuppressed);
                    throw ex;
                }
            } catch (InterruptedException ex) {
                Exception exception = new TranslogUploadFailedException("Failed to upload " + transferSnapshot, ex);
                exceptionList.forEach(exception::addSuppressed);
                Thread.currentThread().interrupt();
                throw exception;
            }
            if (exceptionList.isEmpty()) {
                TransferFileSnapshot tlogMetadata = prepareMetadata(transferSnapshot);
                metadataBytesToUpload = tlogMetadata.getContentLength();
                remoteTranslogTransferTracker.addUploadBytesStarted(metadataBytesToUpload);
                metadataUploadStartTime = System.nanoTime();
                try {
                    transferService.uploadBlob(tlogMetadata, remoteMetadataTransferPath, WritePriority.HIGH);
                } catch (Exception exception) {
                    remoteTranslogTransferTracker.addUploadTimeInMillis((System.nanoTime() - metadataUploadStartTime) / 1_000_000L);
                    remoteTranslogTransferTracker.addUploadBytesFailed(metadataBytesToUpload);
                    // outer catch handles capturing stats on upload failure
                    throw new TranslogUploadFailedException("Failed to upload " + tlogMetadata.getName(), exception);
                }

                remoteTranslogTransferTracker.addUploadTimeInMillis((System.nanoTime() - metadataUploadStartTime) / 1_000_000L);
                remoteTranslogTransferTracker.addUploadBytesSucceeded(metadataBytesToUpload);
                captureStatsOnUploadSuccess(prevUploadBytesSucceeded, prevUploadTimeInMillis);
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            } else {
                Exception ex = new TranslogUploadFailedException("Failed to upload " + exceptionList.size() + " files during transfer");
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Transfer failed for snapshot {}", transferSnapshot), ex);
            captureStatsOnUploadFailure();
            translogTransferListener.onUploadFailed(transferSnapshot, ex);
            return false;
        }
    }

    /**
     * Adds relevant stats to the tracker when an upload is started
     */
    private void captureStatsBeforeUpload() {
        remoteTranslogTransferTracker.incrementTotalUploadsStarted();
        // TODO: Ideally each file's byte uploads started should be when it is actually picked for upload
        // https://github.com/opensearch-project/OpenSearch/issues/9729
        remoteTranslogTransferTracker.addUploadBytesStarted(fileTransferTracker.getTotalBytesToUpload());
    }

    /**
     * Adds relevant stats to the tracker when an upload is successfully completed
     */
    private void captureStatsOnUploadSuccess(long prevUploadBytesSucceeded, long prevUploadTimeInMillis) {
        remoteTranslogTransferTracker.setLastSuccessfulUploadTimestamp(System.currentTimeMillis());
        remoteTranslogTransferTracker.incrementTotalUploadsSucceeded();
        long totalUploadedBytes = remoteTranslogTransferTracker.getUploadBytesSucceeded() - prevUploadBytesSucceeded;
        remoteTranslogTransferTracker.updateUploadBytesMovingAverage(totalUploadedBytes);
        long uploadDurationInMillis = remoteTranslogTransferTracker.getTotalUploadTimeInMillis() - prevUploadTimeInMillis;
        remoteTranslogTransferTracker.updateUploadTimeMovingAverage(uploadDurationInMillis);
        if (uploadDurationInMillis > 0) {
            remoteTranslogTransferTracker.updateUploadBytesPerSecMovingAverage((totalUploadedBytes * 1_000L) / uploadDurationInMillis);
        }
    }

    /**
     * Adds relevant stats to the tracker when an upload has failed
     */
    private void captureStatsOnUploadFailure() {
        remoteTranslogTransferTracker.incrementTotalUploadsFailed();
    }

    public boolean downloadTranslog(String primaryTerm, String generation, Path location) throws IOException {
        logger.trace(
            "Downloading translog files with: Primary Term = {}, Generation = {}, Location = {}",
            primaryTerm,
            generation,
            location
        );
        if (downloadFromArchive(primaryTerm, generation, location)) {
            return true;
        }
        String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        if (isTranslogMetadataEnabled == false) {
            // Download Checkpoint file, translog file from remote to local FS
            downloadToFS(ckpFileName, location, primaryTerm, false);
            downloadToFS(translogFilename, location, primaryTerm, false);
        } else {
            // Download translog.tlog file with object metadata from remote to local FS
            Map<String, String> metadata = downloadToFS(translogFilename, location, primaryTerm, true);
            try {
                assert metadata != null && !metadata.isEmpty() && metadata.containsKey(CHECKPOINT_FILE_DATA_KEY);
                recoverCkpFileUsingMetadata(metadata, location, generation, translogFilename);
            } catch (Exception e) {
                throw new IOException("Failed to recover checkpoint file from remote", e);
            }
        }
        return true;
    }

    private RemoteStoreEnums.PathHashAlgorithm getPathHashAlgorithm() {
        return remoteStoreSettings != null
            ? remoteStoreSettings.getPathHashAlgorithm()
            : RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;
    }

    /**
     * Try to discover and download translog from archive when no metadata exists in remote.
     * Lists archive (translog/data/{hashTypeIndex}/{hashNodeId}), finds candidate ZIP(s),
     * uses comment or CD to get latest (primaryTerm, generation) for this shard, then downloads.
     *
     * @param location local path to write .tlog and .ckp
     * @return true if download succeeded, false otherwise
     */
    public boolean tryDownloadFromArchiveOnly(Path location) throws IOException {
        String indexUUID = shardId.getIndex().getUUID();
        BlobPath dataBase = remoteDataTransferPath.add("translog").add("data");
        List<ZipTail> candidates = listZipTailsAll(dataBase);

        long bestPrimaryTerm = -1L;
        long bestGeneration = -1L;
        ZipTail bestZip = null;
        ArchiveIndexEntry bestIndexEntry = null;
        Map<String, ArchiveEntry> bestCdMap = null;

        for (ZipTail c : candidates) {
            byte[] tail = c.tail;
            long tailStart = c.tailStartOffset;
            ArchiveIndexEntry indexEntry = null;
            Map<String, ArchiveEntry> cdMap = null;

            try {
                String comment = ZipCentralDirectoryParser.getComment(tail);
                if (comment != null && !comment.isEmpty()) {
                    Map<String, ArchiveIndexEntry> indexMap = ArchiveCommentFormat.parseToMap(comment);
                    for (ArchiveIndexEntry e : indexMap.values()) {
                        if (e.getShardId() == shardId.id()) {
                            long pt = e.getPrimaryTerm();
                            long gen = e.getGeneration();
                            if (pt > bestPrimaryTerm || (pt == bestPrimaryTerm && gen > bestGeneration)) {
                                bestPrimaryTerm = pt;
                                bestGeneration = gen;
                                bestZip = c;
                                bestIndexEntry = e;
                                bestCdMap = null;
                            }
                        }
                    }
                    continue;
                }
                cdMap = ZipCentralDirectoryParser.parseToMap(tail, tailStart);
            } catch (IOException e) {
                try {
                    cdMap = ZipCentralDirectoryParser.parseToMap(tail, tailStart);
                } catch (IOException e2) {
                    logger.trace("Parse archive {} failed: {}", c.blobName, e2.getMessage());
                    continue;
                }
            }
            if (cdMap != null) {
                String pathPrefix = indexUUID + "/" + shardId.id() + "/";
                for (String path : cdMap.keySet()) {
                    if (path.startsWith(pathPrefix) && path.endsWith(".tlog")) {
                        int secondSlash = path.indexOf('/', pathPrefix.length());
                        if (secondSlash > 0) {
                            String termStr = path.substring(pathPrefix.length(), secondSlash);
                            String filePart = path.substring(secondSlash + 1);
                            if (filePart.startsWith("translog-") && filePart.endsWith(".tlog")) {
                                try {
                                    long pt = Long.parseLong(termStr);
                                    long gen = Long.parseLong(filePart.substring(9, filePart.length() - 5));
                                    String ckpPath = pathPrefix + pt + "/translog-" + gen + ".ckp";
                                    if (cdMap.containsKey(ckpPath)
                                        && (pt > bestPrimaryTerm || (pt == bestPrimaryTerm && gen > bestGeneration))) {
                                        bestPrimaryTerm = pt;
                                        bestGeneration = gen;
                                        bestZip = c;
                                        bestIndexEntry = null;
                                        bestCdMap = cdMap;
                                    }
                                } catch (NumberFormatException ignored) {}
                            }
                        }
                    }
                }
            }
        }

        if (bestZip == null || bestPrimaryTerm < 0) {
            return false;
        }

        long gen = bestGeneration;
        downloadTlogCkpFromZipTail(bestZip, bestIndexEntry, bestCdMap, bestPrimaryTerm, gen, indexUUID, location);
        logger.debug("Downloaded translog from archive (no metadata) primaryTerm={} generation={}", bestPrimaryTerm, gen);
        return true;
    }

    /**
     * Downloads .tlog and .ckp for the given generation from the ZIP to the local path. Uses indexEntry
     * offsets when non-null, otherwise looks up entries in cdMap by path.
     */
    private void downloadTlogCkpFromZipTail(
        ZipTail zip,
        ArchiveIndexEntry indexEntry,
        Map<String, ArchiveEntry> cdMap,
        long primaryTerm,
        long gen,
        String indexUUID,
        Path location
    ) throws IOException {
        String translogFilename = Translog.getFilename(gen);
        String ckpFileName = Translog.getCommitCheckpointFileName(gen);
        long downloadStartTime = System.nanoTime();
        long bytesRead = 0;
        try {
            if (indexEntry != null) {
                downloadRangeToFS(
                    zip.blobPath,
                    zip.blobName,
                    indexEntry.getTlogOffset(),
                    indexEntry.getTlogLength(),
                    location.resolve(translogFilename)
                );
                bytesRead += indexEntry.getTlogLength();
                downloadRangeToFS(
                    zip.blobPath,
                    zip.blobName,
                    indexEntry.getCkpOffset(),
                    indexEntry.getCkpLength(),
                    location.resolve(ckpFileName)
                );
                bytesRead += indexEntry.getCkpLength();
            } else {
                String pathPrefix = indexUUID + "/" + shardId.id() + "/" + primaryTerm + "/";
                ArchiveEntry tlogEntry = cdMap.get(pathPrefix + translogFilename);
                ArchiveEntry ckpEntry = cdMap.get(pathPrefix + ckpFileName);
                downloadRangeToFS(tlogEntry, zip.blobPath, zip.blobName, location.resolve(translogFilename));
                bytesRead += tlogEntry.getDataLength();
                downloadRangeToFS(ckpEntry, zip.blobPath, zip.blobName, location.resolve(ckpFileName));
                bytesRead += ckpEntry.getDataLength();
            }
        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
            remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesRead);
        }
        fileTransferTracker.add(translogFilename, true);
        fileTransferTracker.add(ckpFileName, true);
    }

    private static final class ZipTail {
        final BlobPath blobPath;
        final String blobName;
        final byte[] tail;
        final long tailStartOffset;

        ZipTail(BlobPath blobPath, String blobName, byte[] tail, long tailStartOffset) {
            this.blobPath = blobPath;
            this.blobName = blobName;
            this.tail = tail;
            this.tailStartOffset = tailStartOffset;
        }
    }

    /**
     * Lists all *.zip under translog/data: hashPrefix → genBucket → *.zip. For tryDownloadFromArchiveOnly.
     */
    private List<ZipTail> listZipTailsAll(BlobPath dataBase) {
        Set<String> hashPrefixes;
        try {
            hashPrefixes = transferService.listFolders(dataBase);
        } catch (IOException e) {
            logger.trace("List translog/data failed: {}", e.getMessage());
            return new ArrayList<>();
        }
        if (hashPrefixes == null || hashPrefixes.isEmpty()) {
            return new ArrayList<>();
        }
        List<ZipTail> out = new ArrayList<>();
        for (String hashPrefix : hashPrefixes) {
            BlobPath hashPath = dataBase.add(hashPrefix);
            Set<String> genBuckets;
            try {
                genBuckets = transferService.listFolders(hashPath);
            } catch (IOException e) {
                logger.trace("List genBuckets under {} failed: {}", hashPrefix, e.getMessage());
                continue;
            }
            if (genBuckets == null || genBuckets.isEmpty()) {
                continue;
            }
            for (String genBucket : genBuckets) {
                collectZipTails(dataBase.add(hashPrefix).add(genBucket), out);
            }
        }
        out.sort(
            Comparator.comparing(
                (ZipTail z) -> TranslogArchivePathHelper.parseBlobNameTimestamp(z.blobName).orElse(null),
                Comparator.nullsLast(Comparator.reverseOrder())
            )
        );
        return out;
    }

    /**
     * Lists *.zip under translog/data/{hashPrefix}/{genBucket} for all hashPrefixes. For downloadFromArchive.
     */
    private List<ZipTail> listZipTailsInGenBucket(BlobPath dataBase, long genBucket) {
        Set<String> hashPrefixes;
        try {
            hashPrefixes = transferService.listFolders(dataBase);
        } catch (IOException e) {
            logger.trace("List translog/data failed: {}", e.getMessage());
            return new ArrayList<>();
        }
        if (hashPrefixes == null || hashPrefixes.isEmpty()) {
            return new ArrayList<>();
        }
        List<ZipTail> out = new ArrayList<>();
        String genBucketStr = String.valueOf(genBucket);
        for (String hashPrefix : hashPrefixes) {
            collectZipTails(dataBase.add(hashPrefix).add(genBucketStr), out);
        }
        return out;
    }

    private void collectZipTails(BlobPath path, List<ZipTail> out) {
        List<BlobMetadata> blobs;
        try {
            blobs = transferService.listAllInSortedOrder(path, "", MAX_ARCHIVE_BLOBS_PER_NODE);
        } catch (IOException e) {
            logger.trace("List blobs under {} failed: {}", path.buildAsString(), e.getMessage());
            return;
        }
        if (blobs == null) {
            return;
        }
        for (BlobMetadata blob : blobs) {
            String name = blob.name();
            if (name == null || !name.endsWith(".zip")) {
                continue;
            }
            long size = blob.length();
            if (size < 22) {
                continue;
            }
            int tailLen = (int) Math.min(size, ZipCentralDirectoryParser.MAX_ZIP_TAIL_BYTES);
            long tailStartOffset = size - tailLen;
            byte[] tail;
            try (InputStream in = transferService.downloadBlob(path, name, tailStartOffset, tailLen)) {
                tail = in.readAllBytes();
            } catch (IOException e) {
                logger.trace("Range-read tail of {} failed: {}", name, e.getMessage());
                continue;
            }
            out.add(new ZipTail(path, name, tail, tailStartOffset));
        }
    }

    /**
     * Try to download this (primaryTerm, generation) from a translog archive (ZIP).
     * Uses path translog/data/{hashTypeIndex}/{hashNodeId} with comment-based lookup then CD fallback.
     */
    private boolean downloadFromArchive(String primaryTerm, String generation, Path location) throws IOException {
        return downloadFromArchiveNewPath(primaryTerm, generation, location);
    }

    private boolean downloadFromArchiveNewPath(String primaryTerm, String generation, Path location) throws IOException {
        long gen = Long.parseLong(generation);
        long genBucket = gen / GEN_BUCKET_SIZE;
        BlobPath dataBase = remoteDataTransferPath.add("translog").add("data");
        List<ZipTail> candidates = listZipTailsInGenBucket(dataBase, genBucket);
        return downloadFromArchiveWithCandidates(candidates, primaryTerm, generation, location);
    }

    /**
     * Iterates candidate ZIPs (newest first), parses comment then CD, and downloads tlog+ckp for the given
     * (primaryTerm, generation) if found. Returns true when the first matching ZIP is downloaded.
     */
    private boolean downloadFromArchiveWithCandidates(List<ZipTail> candidates, String primaryTerm, String generation, Path location)
        throws IOException {
        long gen = Long.parseLong(generation);
        String translogFilename = Translog.getFilename(gen);
        String ckpFileName = Translog.getCommitCheckpointFileName(gen);
        String indexUUID = shardId.getIndex().getUUID();
        String pathPrefix = indexUUID + "/" + shardId.id() + "/" + primaryTerm + "/";
        String tlogPathInZip = pathPrefix + translogFilename;
        String ckpPathInZip = pathPrefix + ckpFileName;
        String indexKey = shardId.id() + "," + primaryTerm + "," + generation;

        for (ZipTail c : candidates) {
            byte[] tail = c.tail;
            long tailStartOffset = c.tailStartOffset;
            BlobPath nodePath = c.blobPath;
            String name = c.blobName;

            ArchiveIndexEntry indexEntry = null;
            Map<String, ArchiveEntry> cdMap = null;
            try {
                String comment = ZipCentralDirectoryParser.getComment(tail);
                if (comment != null && !comment.isEmpty()) {
                    Map<String, ArchiveIndexEntry> indexMap = ArchiveCommentFormat.parseToMap(comment);
                    indexEntry = indexMap.get(indexKey);
                }
                if (indexEntry == null) {
                    cdMap = ZipCentralDirectoryParser.parseToMap(tail, tailStartOffset);
                }
            } catch (IOException e) {
                try {
                    cdMap = ZipCentralDirectoryParser.parseToMap(tail, tailStartOffset);
                } catch (IOException e2) {
                    continue;
                }
            }
            if (indexEntry != null) {
                downloadTlogCkpFromZipTail(c, indexEntry, null, Long.parseLong(primaryTerm), gen, indexUUID, location);
                logger.trace("Downloaded generation {} from archive {}", generation, name);
                return true;
            }
            if (cdMap != null) {
                ArchiveEntry tlogEntry = cdMap.get(tlogPathInZip);
                ArchiveEntry ckpEntry = cdMap.get(ckpPathInZip);
                if (tlogEntry != null && ckpEntry != null) {
                    downloadTlogCkpFromZipTail(c, null, cdMap, Long.parseLong(primaryTerm), gen, indexUUID, location);
                    logger.trace("Downloaded generation {} from archive (CD) {}", generation, name);
                    return true;
                }
            }
        }
        return false;
    }

    private void downloadRangeToFS(BlobPath blobPath, String blobName, long offset, long length, Path localFile) throws IOException {
        deleteFileIfExists(localFile);
        try (InputStream in = transferService.downloadBlob(blobPath, blobName, offset, length)) {
            Files.copy(in, localFile);
        }
    }

    private void downloadRangeToFS(ArchiveEntry entry, BlobPath archivePath, String archiveBlobName, Path localFile) throws IOException {
        deleteFileIfExists(localFile);
        try (InputStream in = transferService.downloadBlob(archivePath, archiveBlobName, entry.getDataOffset(), entry.getDataLength())) {
            Files.copy(in, localFile);
        }
    }

    /**
     * Process the provided metadata and tries to recover translog.ckp file to the FS.
     */
    private void recoverCkpFileUsingMetadata(Map<String, String> metadata, Path location, String generation, String fileName)
        throws IOException {

        String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        Path filePath = location.resolve(ckpFileName);
        // Here, we always override the existing file if present.
        deleteFileIfExists(filePath);

        String ckpDataBase64 = metadata.get(CHECKPOINT_FILE_DATA_KEY);
        if (ckpDataBase64 == null) {
            logger.error("Error processing metadata for translog file: {}", fileName);
            throw new IllegalStateException(
                "Checkpoint file data key " + CHECKPOINT_FILE_DATA_KEY + " is expected but not found in metadata for file: " + fileName
            );
        }
        byte[] ckpFileBytes = Base64.getDecoder().decode(ckpDataBase64);
        Files.write(filePath, ckpFileBytes);
    }

    private Map<String, String> downloadToFS(String fileName, Path location, String primaryTerm, boolean withMetadata) throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        // We need to change this logic when we introduce incremental download
        deleteFileIfExists(filePath);

        Map<String, String> metadata = null;
        boolean downloadStatus = false;
        long bytesToRead = 0, downloadStartTime = System.nanoTime();
        try {
            if (withMetadata) {
                try (
                    InputStreamWithMetadata inputStreamWithMetadata = transferService.downloadBlobWithMetadata(
                        remoteDataTransferPath.add(primaryTerm),
                        fileName
                    )
                ) {
                    InputStream inputStream = inputStreamWithMetadata.getInputStream();
                    metadata = inputStreamWithMetadata.getMetadata();

                    bytesToRead = inputStream.available();
                    Files.copy(inputStream, filePath);
                    downloadStatus = true;
                }
            } else {
                try (InputStream inputStream = transferService.downloadBlob(remoteDataTransferPath.add(primaryTerm), fileName)) {
                    bytesToRead = inputStream.available();
                    Files.copy(inputStream, filePath);
                    downloadStatus = true;
                }
            }
        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
            if (downloadStatus) {
                remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
            }
        }

        // Mark in FileTransferTracker so that the same files are not uploaded at the time of translog sync
        fileTransferTracker.add(fileName, true);
        return metadata;
    }

    private void deleteFileIfExists(Path filePath) throws IOException {
        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }
    }

    public TranslogTransferMetadata readMetadata() throws IOException {
        SetOnce<TranslogTransferMetadata> metadataSetOnce = new SetOnce<>();
        SetOnce<IOException> exceptionSetOnce = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<List<BlobMetadata>> latchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(blobMetadataList -> {
                if (blobMetadataList.isEmpty()) return;
                RemoteStoreUtils.verifyNoMultipleWriters(
                    blobMetadataList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
                    TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
                );
                String filename = blobMetadataList.get(0).name();
                boolean downloadStatus = false;
                long downloadStartTime = System.nanoTime(), bytesToRead = 0;
                try (InputStream inputStream = transferService.downloadBlob(remoteMetadataTransferPath, filename)) {
                    // Capture number of bytes for stats before reading
                    bytesToRead = inputStream.available();
                    IndexInput indexInput = new ByteArrayIndexInput("metadata file", inputStream.readAllBytes());
                    metadataSetOnce.set(metadataStreamWrapper.readStream(indexInput));
                    downloadStatus = true;
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("Exception while reading metadata file: {}", filename), e);
                    exceptionSetOnce.set(e);
                } finally {
                    remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
                    logger.debug("translogMetadataDownloadStatus={}", downloadStatus);
                    if (downloadStatus) {
                        remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
                    }
                }
            }, e -> {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                logger.error(() -> new ParameterizedMessage("Exception while listing metadata files"), e);
                exceptionSetOnce.set((IOException) e);
            }),
            latch
        );

        try {
            transferService.listAllInSortedOrder(
                remoteMetadataTransferPath,
                TranslogTransferMetadata.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH,
                latchedActionListener
            );
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Exception while reading/downloading metadafile", e);
        }

        if (exceptionSetOnce.get() != null) {
            throw exceptionSetOnce.get();
        }

        return metadataSetOnce.get();
    }

    private TransferFileSnapshot prepareMetadata(TransferSnapshot transferSnapshot) throws IOException {
        Map<String, String> generationPrimaryTermMap = transferSnapshot.getTranslogFileSnapshots().stream().map(s -> {
            assert s instanceof TranslogFileSnapshot;
            return (TranslogFileSnapshot) s;
        })
            .collect(
                Collectors.toMap(
                    snapshot -> String.valueOf(snapshot.getGeneration()),
                    snapshot -> String.valueOf(snapshot.getPrimaryTerm())
                )
            );
        TranslogTransferMetadata translogTransferMetadata = transferSnapshot.getTranslogTransferMetadata();
        translogTransferMetadata.setGenerationToPrimaryTermMapper(new HashMap<>(generationPrimaryTermMap));

        return new TransferFileSnapshot(
            translogTransferMetadata.getFileName(),
            getMetadataBytes(translogTransferMetadata),
            translogTransferMetadata.getPrimaryTerm()
        );
    }

    /**
     * Uploads only the translog transfer metadata for a snapshot. Used after a node-level archive upload
     * so recovery can discover generations via existing metadata path and then download from archive.
     */
    public void uploadMetadataForArchiveSnapshot(TransferSnapshot transferSnapshot) throws IOException {
        TransferFileSnapshot meta = prepareMetadata(transferSnapshot);
        transferService.uploadBlob(meta, remoteMetadataTransferPath, WritePriority.HIGH, null);
    }

    /**
     * Get the metadata bytes for a {@link TranslogTransferMetadata} object
     *
     * @param metadata The object to be parsed
     * @return Byte representation for the given metadata
     */
    public byte[] getMetadataBytes(TranslogTransferMetadata metadata) throws IOException {
        byte[] metadataBytes;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "translog transfer metadata " + metadata.getPrimaryTerm(),
                    metadata.getFileName(),
                    output,
                    TranslogTransferMetadata.BUFFER_SIZE
                )
            ) {
                metadataStreamWrapper.writeStream(indexOutput, metadata);
            }
            metadataBytes = BytesReference.toBytes(output.bytes());
        }

        return metadataBytes;
    }

    /**
     * This method handles deletion of multiple generations for a single primary term. The deletion happens for translog
     * and metadata files.
     *
     * @param primaryTerm primary term where the generations will be deleted.
     * @param generations set of generation to delete.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    public void deleteGenerationAsync(long primaryTerm, Set<Long> generations, Runnable onCompletion) {
        List<String> translogFiles = new ArrayList<>();
        generations.forEach(generation -> {
            // Add .ckp and .tlog file to translog file list which is located in basePath/<primaryTerm>
            String ckpFileName = Translog.getCommitCheckpointFileName(generation);
            String translogFileName = Translog.getFilename(generation);
            if (isTranslogMetadataEnabled == false) {
                translogFiles.addAll(List.of(ckpFileName, translogFileName));
            } else {
                translogFiles.add(translogFileName);
            }
        });
        // Delete the translog and checkpoint files asynchronously
        deleteTranslogFilesAsync(primaryTerm, translogFiles, onCompletion);
    }

    /**
     * Deletes all primary terms from remote store that are more than the given {@code minPrimaryTermToKeep}. The caller
     * of the method must ensure that the value is lesser than equal to the minimum primary term referenced by the remote
     * translog metadata.
     *
     * @param minPrimaryTermToKeep all primary terms below this primary term are deleted.
     */
    public void deletePrimaryTermsAsync(long minPrimaryTermToKeep) {
        logger.info("Deleting primary terms from remote store lesser than {}", minPrimaryTermToKeep);
        transferService.listFoldersAsync(ThreadPool.Names.REMOTE_PURGE, remoteDataTransferPath, new ActionListener<>() {
            @Override
            public void onResponse(Set<String> folders) {
                Set<Long> primaryTermsInRemote = folders.stream().filter(folderName -> {
                    try {
                        Long.parseLong(folderName);
                        return true;
                    } catch (Exception ignored) {
                        // NO-OP
                    }
                    return false;
                }).map(Long::parseLong).collect(Collectors.toSet());
                Set<Long> primaryTermsToDelete = primaryTermsInRemote.stream()
                    .filter(term -> term < minPrimaryTermToKeep)
                    .collect(Collectors.toSet());
                primaryTermsToDelete.forEach(term -> deletePrimaryTermAsync(term));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Exception occurred while getting primary terms from remote store", e);
            }
        });
    }

    public Set<Long> listPrimaryTermsInRemote() throws IOException {
        Set<String> primaryTermsStr = transferService.listFolders(remoteDataTransferPath);
        if (primaryTermsStr != null) {
            return primaryTermsStr.stream().filter(folderName -> {
                try {
                    Long.parseLong(folderName);
                    return true;
                } catch (NumberFormatException ignored) {
                    return false;
                }
            }).map(Long::parseLong).collect(Collectors.toSet());
        }
        return new HashSet<>();
    }
    /**
     * Handles deletion of all translog files associated with a primary term.
     *
     * @param primaryTerm primary term.
     */
    private void deletePrimaryTermAsync(long primaryTerm) {
        transferService.deleteAsync(
            ThreadPool.Names.REMOTE_PURGE,
            remoteDataTransferPath.add(String.valueOf(primaryTerm)),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info("Deleted primary term {}", primaryTerm);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(new ParameterizedMessage("Exception occurred while deleting primary term {}", primaryTerm), e);
                }
            }
        );
    }

    /**
     * Deletes all the translog content related to the underlying shard.
     */
    public void delete() {
        // Delete the translog data content from the remote store.
        delete(remoteDataTransferPath);
        // Delete the translog metadata content from the remote store.
        delete(remoteMetadataTransferPath);
    }

    private void delete(BlobPath path) {
        // cleans up all the translog contents in async fashion for the given path
        transferService.deleteAsync(ThreadPool.Names.REMOTE_PURGE, path, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("Deleted all remote translog data at path={}", path);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(new ParameterizedMessage("Exception occurred while cleaning translog at path={}", path), e);
            }
        });
    }

    public void deleteStaleTranslogMetadataFilesAsync(Runnable onCompletion) {
        try {
            transferService.listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteMetadataTransferPath,
                TranslogTransferMetadata.METADATA_PREFIX,
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        List<String> sortedMetadataFiles = blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList());
                        if (sortedMetadataFiles.size() <= 1) {
                            logger.trace("Remote Metadata file count is {}, so skipping deletion", sortedMetadataFiles.size());
                            onCompletion.run();
                            return;
                        }
                        List<String> metadataFilesToDelete = sortedMetadataFiles.subList(1, sortedMetadataFiles.size());
                        logger.trace("Deleting remote translog metadata files {}", metadataFilesToDelete);
                        deleteMetadataFilesAsync(metadataFilesToDelete, onCompletion);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Exception occurred while listing translog metadata files from remote store", e);
                        onCompletion.run();
                    }
                }
            );
        } catch (Exception e) {
            logger.error("Exception occurred while listing translog metadata files from remote store", e);
            onCompletion.run();
        }
    }

    public void deleteTranslogFiles() throws IOException {
        transferService.delete(remoteMetadataTransferPath);
        transferService.delete(remoteDataTransferPath);
    }

    /**
     * Deletes list of translog files asynchronously using the {@code REMOTE_PURGE} threadpool.
     *
     * @param primaryTerm primary term of translog files.
     * @param files       list of translog files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteTranslogFilesAsync(long primaryTerm, List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteDataTransferPath.add(String.valueOf(primaryTerm)),
                files,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        fileTransferTracker.delete(files);
                        logger.trace("Deleted translogs for primaryTerm={} files={}", primaryTerm, files);
                        onCompletion.run();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onCompletion.run();
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Exception occurred while deleting translog for primaryTerm={} files={}",
                                primaryTerm,
                                files
                            ),
                            e
                        );
                    }
                }
            );
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }

    /**
     * Deletes metadata files asynchronously using the {@code REMOTE_PURGE} threadpool. On success or failure, runs {@code onCompletion}.
     *
     * @param files list of metadata files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteMetadataFilesAsync(List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(ThreadPool.Names.REMOTE_PURGE, remoteMetadataTransferPath, files, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    onCompletion.run();
                    logger.trace("Deleted remote translog metadata files {}", files);
                }

                @Override
                public void onFailure(Exception e) {
                    onCompletion.run();
                    logger.error(new ParameterizedMessage("Exception occurred while deleting remote translog metadata files {}", files), e);
                }
            });
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }

    public int getMaxRemoteTranslogReadersSettings() {
        return this.remoteStoreSettings.getMaxRemoteTranslogReaders();
    }
}
