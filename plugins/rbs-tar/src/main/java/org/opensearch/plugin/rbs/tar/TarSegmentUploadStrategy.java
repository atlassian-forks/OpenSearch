/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.RemoteSegmentFile;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A segment upload strategy that packages segment files of a refresh batch into a single tar archive.
 * The first entry of the archive is always "index.bin".
 */
public final class TarSegmentUploadStrategy implements RemoteStoreSegmentStrategy {

    private static final Logger logger = LogManager.getLogger(TarSegmentUploadStrategy.class);

    public TarSegmentUploadStrategy() {}

    public static String getBundleFilename(
        long primaryTerm,
        long generation,
        long translogGeneration,
        long uploadCounter,
        int metadataVersion,
        String nodeId,
        long creationTimestamp
    ) {
        return String.join(
            "__",
            "rbs_bundle",
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(translogGeneration),
            RemoteStoreUtils.invertLong(uploadCounter),
            String.valueOf(Objects.hash(nodeId)),
            RemoteStoreUtils.invertLong(creationTimestamp),
            metadataVersion + ".tar"
        );
    }

    public static long getTimestamp(String filename) {
        String[] filenameTokens = filename.split("__");
        String tsToken = filenameTokens[filenameTokens.length - 2];
        return RemoteStoreUtils.invertLong(tsToken);
    }

    public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
        String[] tokens = filename.split("__");
        if (tokens.length < 8) {
            return null;
        }
        String primaryTermAndGen = String.join("__", tokens[1], tokens[2], tokens[3]);
        return new Tuple<>(primaryTermAndGen, tokens[5]);
    }

    private static int parseOctal(byte[] buffer, int offset, int length) {
        long value = 0;
        int end = offset + length;
        int start = offset;
        while (start < end && (buffer[start] == ' ' || buffer[start] == '0')) {
            start++;
        }
        for (int i = start; i < end; i++) {
            byte b = buffer[i];
            if (b == 0 || b == ' ' || b == '\t') {
                break;
            }
            if (b < '0' || b > '7') {
                throw new IllegalArgumentException("Invalid octal digit: " + (char) b);
            }
            value = (value << 3) + (b - '0');
        }
        return (int) value;
    }

    @Override
    public void upload(
        RemoteSegmentStoreDirectory remoteDirectory,
        ShardId shardId,
        RemoteStoreSegmentStrategy.UploadContext context,
        RemoteStoreSegmentStrategy.UploadListener listener
    ) throws IOException {
        final List<String> filesToUpload = context.segmentFiles()
            .stream()
            .filter(RemoteStoreSegmentStrategy.UploadContext.SegmentFile::toUpload)
            .map(RemoteStoreSegmentStrategy.UploadContext.SegmentFile::name)
            .collect(Collectors.toList());
        final List<String> activeSegmentFiles = context.segmentFiles()
            .stream()
            .map(RemoteStoreSegmentStrategy.UploadContext.SegmentFile::name)
            .collect(Collectors.toList());
        final Directory storeDirectory = context.storeDirectory();
        final ReplicationCheckpoint checkpoint = context.checkpoint();
        final boolean isLowPriorityUpload = context.isLowPriorityUpload();
        final CryptoMetadata cryptoMetadata = context.cryptoMetadata();
        final RemoteDirectory remoteDataDirectory = (RemoteDirectory) remoteDirectory.getDelegate();

        if (filesToUpload.isEmpty()) {
            listener.onAllUploadsSuccess();
            return;
        }

        final long creationTimestamp = System.currentTimeMillis();
        final String bundleUuid = UUIDs.randomBase64UUID();
        final long uploadCounter = remoteDirectory.getMetadataUploadCounter().incrementAndGet();
        final String bundleName = getBundleFilename(
            checkpoint.getPrimaryTerm(),
            checkpoint.getSegmentsGen(),
            0L,
            uploadCounter,
            RemoteSegmentMetadata.CURRENT_VERSION,
            "rbs_node",
            creationTimestamp
        );
        final String remoteTarName = remoteDirectory.getNewRemoteSegmentFilename(bundleName);

        // Circular size / offset calculation for index.bin
        int indexBinSize = 2048; // starting guess
        Map<String, Long> fileOffsets = new HashMap<>();
        byte[] indexBinBytes = null;

        for (int iter = 0; iter < 3; iter++) {
            int indexBinPadding = (512 - (indexBinSize % 512)) % 512;
            long currentOffset = 512L + indexBinSize + indexBinPadding;
            fileOffsets.clear();
            for (final String file : filesToUpload) {
                currentOffset += 512L;
                fileOffsets.put(file, currentOffset);
                final long length = storeDirectory.fileLength(file);
                final long filePadding = (512L - (length % 512L)) % 512L;
                currentOffset += length + filePadding;
            }

            final Map<String, RemoteSegmentFile> uploadedSegmentsMap = new HashMap<>();
            for (final String file : activeSegmentFiles) {
                if (filesToUpload.contains(file)) {
                    final String checksum = remoteDirectory.getChecksumOfLocalFile(storeDirectory, file);
                    final long length = storeDirectory.fileLength(file);
                    final TarUploadedSegmentMetadata meta = new TarUploadedSegmentMetadata(
                        file,
                        remoteTarName,
                        fileOffsets.get(file),
                        checksum,
                        length,
                        remoteDataDirectory
                    );
                    meta.setWrittenByMajor(org.apache.lucene.util.Version.LATEST.major);
                    uploadedSegmentsMap.put(file, meta);
                } else {
                    final RemoteSegmentFile existing = remoteDirectory.getSegmentsUploadedToRemoteStore().get(file);
                    if (existing == null) {
                        throw new NoSuchFileException(file);
                    }
                    uploadedSegmentsMap.put(file, existing);
                }
            }

            final RemoteSegmentMetadata remoteMetadata = new RemoteSegmentMetadata(uploadedSegmentsMap, new byte[0], checkpoint);

            final VersionedCodecStreamWrapper<RemoteSegmentMetadata> wrapper = new VersionedCodecStreamWrapper<>(
                new RemoteSegmentMetadataHandlerFactory(),
                RemoteSegmentMetadata.VERSION_ONE,
                RemoteSegmentMetadata.CURRENT_VERSION,
                RemoteSegmentMetadata.METADATA_CODEC
            );
            final ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
            try (IndexOutput indexOutput = new ByteBuffersIndexOutput(byteBuffersIndexOutput, "index.bin", "index.bin")) {
                wrapper.writeStream(indexOutput, remoteMetadata);
            }
            indexBinBytes = byteBuffersIndexOutput.toArrayCopy();

            if (indexBinBytes.length == indexBinSize) {
                break;
            }
            indexBinSize = indexBinBytes.length;
        }

        // Write tar archive to temporary local file
        final IndexOutput indexOutput = storeDirectory.createOutput(bundleName, IOContext.DEFAULT);
        final OutputStream os = new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                indexOutput.writeByte((byte) b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                indexOutput.writeBytes(b, off, len);
            }

            @Override
            public void close() throws IOException {
                indexOutput.close();
            }
        };

        try (TarOutputStream tos = new TarOutputStream(os)) {
            tos.putNextEntry("index.bin", indexBinBytes.length);
            tos.write(indexBinBytes);
            tos.closeEntry();

            final byte[] buf = new byte[8192];
            for (final String file : filesToUpload) {
                final long length = storeDirectory.fileLength(file);
                tos.putNextEntry(file, length);
                try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.READONCE)) {
                    long remaining = length;
                    while (remaining > 0) {
                        final int toRead = (int) Math.min(buf.length, remaining);
                        indexInput.readBytes(buf, 0, toRead);
                        tos.write(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
                tos.closeEntry();
            }
        }

        // Notify upload starts
        for (final String file : filesToUpload) {
            listener.onUploadStart(file);
        }

        final byte[] finalIndexBinBytes = indexBinBytes;
        final Map<String, Long> finalFileOffsets = fileOffsets;
        final Runnable postUploadRunner = () -> {
            try {
                for (final String file : activeSegmentFiles) {
                    if (filesToUpload.contains(file)) {
                        final String checksum = remoteDirectory.getChecksumOfLocalFile(storeDirectory, file);
                        final long length = storeDirectory.fileLength(file);
                        final TarUploadedSegmentMetadata metadata = new TarUploadedSegmentMetadata(
                            file,
                            remoteTarName,
                            finalFileOffsets.get(file),
                            checksum,
                            length,
                            remoteDataDirectory
                        );
                        metadata.setWrittenByMajor(org.apache.lucene.util.Version.LATEST.major);
                        remoteDirectory.addUploadedSegment(file, metadata);
                    } else {
                        // Keep or ensure present
                        final UploadedSegmentMetadata existing = remoteDirectory.getSegmentsUploadedToRemoteStore().get(file);
                        if (existing != null) {
                            remoteDirectory.addUploadedSegment(file, existing);
                        }
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException("Exception in post-upload segment registration", e);
            }
        };

        final ActionListener<Void> wrapListener = ActionListener.wrap(resp -> {
            for (final String file : filesToUpload) {
                listener.onUploadSuccess(file);
            }
            try {
                storeDirectory.deleteFile(bundleName);
            } catch (final IOException e) {
                // ignore
            }
            listener.onAllUploadsSuccess();
        }, ex -> {
            for (final String file : filesToUpload) {
                listener.onUploadFailure(file, ex);
            }
            try {
                storeDirectory.deleteFile(bundleName);
            } catch (final IOException e) {
                // ignore
            }
            listener.onAllUploadsFailure(ex);
        });

        final Directory remoteDataDir = remoteDirectory.getDelegate();
        boolean uploaded = false;
        if (remoteDataDir instanceof RemoteDirectory) {
            uploaded = ((RemoteDirectory) remoteDataDir).copyFrom(
                storeDirectory,
                bundleName,
                remoteTarName,
                IOContext.DEFAULT,
                postUploadRunner,
                wrapListener,
                isLowPriorityUpload,
                cryptoMetadata
            );
        }
        if (uploaded == false) {
            try {
                remoteDataDir.copyFrom(storeDirectory, bundleName, remoteTarName, IOContext.DEFAULT);
                postUploadRunner.run();
                wrapListener.onResponse(null);
            } catch (final Exception e) {
                wrapListener.onFailure(e);
            }
        }
    }

    @Override
    public void uploadMetadata(
        RemoteSegmentStoreDirectory remoteDirectory,
        ShardId shardId,
        RemoteStoreSegmentStrategy.MetadataUploadContext context
    ) throws IOException {
        // NO-OP: Tar strategy uploads everything atomically in upload(...)
    }

    public RemoteSegmentMetadata init(RemoteSegmentStoreDirectory remoteDirectory, RemoteDirectory remoteDataDirectory) throws IOException {
        logger.debug("Start initialisation of remote segment metadata");

        final List<String> bundleFiles = remoteDataDirectory.listFilesByPrefixInLexicographicOrder(
            "rbs_bundle__",
            RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
        );

        RemoteStoreUtils.verifyNoMultipleWriters(bundleFiles, TarSegmentUploadStrategy::getNodeIdByPrimaryTermAndGen);

        if (bundleFiles.isEmpty()) {
            logger.trace("No bundle file found, this can happen for new index with no data uploaded to remote segment store");
            return null;
        }

        final String latestBundle = bundleFiles.get(0);
        logger.trace("Reading latest bundle file {}", latestBundle);
        return readMetadata(remoteDirectory, remoteDataDirectory, latestBundle);
    }

    private RemoteSegmentMetadata postProcessMetadata(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteDataDirectory,
        RemoteSegmentMetadata rawMetadata
    ) {
        if (rawMetadata == null) {
            return null;
        }
        Map<String, RemoteSegmentFile> processedMap = new HashMap<>();
        for (Map.Entry<String, RemoteSegmentFile> entry : rawMetadata.getMetadata().entrySet()) {
            UploadedSegmentMetadata raw = (UploadedSegmentMetadata) entry.getValue();
            final String[] parts = raw.getUploadedFilename().split("#");
            final String tarFilename = parts[0];
            final long offset = Long.parseLong(parts[1]);
            TarUploadedSegmentMetadata processed = new TarUploadedSegmentMetadata(
                raw.getOriginalFilename(),
                tarFilename,
                offset,
                raw.getChecksum(),
                raw.getLength(),
                remoteDataDirectory
            );
            processed.setWrittenByMajor(raw.getWrittenByMajor());
            processedMap.put(entry.getKey(), processed);
        }
        return new RemoteSegmentMetadata(processedMap, rawMetadata.getSegmentInfosBytes(), rawMetadata.getReplicationCheckpoint());
    }

    @Override
    public void deleteStaleSegments(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, int minCommitsToKeep) throws IOException {
        if (minCommitsToKeep == -1) {
            return;
        }

        final RemoteDirectory remoteDataDirectory = (RemoteDirectory) remoteDirectory.getDelegate();
        final List<String> allBundles = remoteDataDirectory.listFilesByPrefixInLexicographicOrder("rbs_bundle__", Integer.MAX_VALUE);

        if (allBundles.size() <= minCommitsToKeep) {
            return;
        }

        if (minCommitsToKeep != 0 && RemoteStoreUtils.isPinnedTimestampStateStale()) {
            logger.warn("Skipping remote segment store garbage collection as last fetch of pinned timestamp is stale");
            return;
        }

        final Tuple<Long, Set<Long>> pinnedTimestampsState = RemoteStorePinnedTimestampService.getPinnedTimestamps();
        final Set<Long> pinnedTimestamps = new HashSet<>(pinnedTimestampsState.v2());
        pinnedTimestamps.add(pinnedTimestampsState.v1());

        final Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            allBundles,
            pinnedTimestamps,
            remoteDirectory.getMetadataFilePinnedTimestampMap(),
            TarSegmentUploadStrategy::getTimestamp,
            TarSegmentUploadStrategy::getNodeIdByPrimaryTermAndGen
        );

        final Set<String> allLockFiles = new HashSet<>(implicitLockedFiles);
        try {
            allLockFiles.addAll(
                ((RemoteStoreMetadataLockManager) remoteDirectory.getMetadataLockManager()).fetchLockedMetadataFiles("rbs_bundle")
            );
        } catch (final Exception e) {
            logger.error("Exception while fetching segment metadata lock files, skipping deleteStaleSegments", e);
            return;
        }

        final List<String> activeBundles = new ArrayList<>();
        final List<String> bundlesEligibleToDelete = new ArrayList<>();

        for (int i = 0; i < allBundles.size(); i++) {
            final String bundle = allBundles.get(i);
            if (i < minCommitsToKeep || allLockFiles.contains(bundle)) {
                activeBundles.add(bundle);
            } else {
                bundlesEligibleToDelete.add(bundle);
            }
        }

        if (bundlesEligibleToDelete.isEmpty()) {
            return;
        }

        // For GC purposes we only need the file→bundle mapping from index.bin.
        // Use readIndexBinOnly() instead of the full readMetadata() to avoid the
        // extra range-GET for segments_N bytes (only needed during shard init, not GC).
        final Set<String> referencedBundles = new HashSet<>(activeBundles);
        for (final String activeBundle : activeBundles) {
            try {
                final Map<String, RemoteSegmentFile> fileMap = readIndexBinOnly(remoteDataDirectory, activeBundle);
                for (final RemoteSegmentFile segMeta : fileMap.values()) {
                    final String[] parts = ((UploadedSegmentMetadata) segMeta).getUploadedFilename().split("#");
                    referencedBundles.add(parts[0]);
                }
            } catch (final Exception e) {
                logger.warn("Failed to read index.bin from active bundle: " + activeBundle + ", assuming it is needed", e);
            }
        }

        final List<String> bundlesToDelete = new ArrayList<>();
        for (final String bundle : bundlesEligibleToDelete) {
            if (!referencedBundles.contains(bundle)) {
                bundlesToDelete.add(bundle);
            }
        }

        if (bundlesToDelete.isEmpty() == false) {
            logger.info("Deleting stale bundles: {}", bundlesToDelete);
            remoteDataDirectory.deleteFiles(bundlesToDelete);

            final Map<String, UploadedSegmentMetadata> activeSegments = remoteDirectory.getSegmentsUploadedToRemoteStore();
            final List<String> toRemove = new ArrayList<>();
            for (final Map.Entry<String, UploadedSegmentMetadata> entry : activeSegments.entrySet()) {
                final String[] parts = entry.getValue().getUploadedFilename().split("#");
                if (bundlesToDelete.contains(parts[0])) {
                    toRemove.add(entry.getKey());
                }
            }
            for (final String file : toRemove) {
                remoteDirectory.removeUploadedSegment(file);
            }
        }
    }

    @Override
    public void deleteFile(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, String name) throws IOException {
        remoteDirectory.removeUploadedSegment(name);
    }

    public String getMetadataFileForCommit(RemoteDirectory remoteDataDirectory, long primaryTerm, long generation) throws IOException {
        final String prefix = String.join(
            "__",
            "rbs_bundle",
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation)
        );
        final List<String> bundleFiles = remoteDataDirectory.listFilesByPrefixInLexicographicOrder(prefix, 1);
        if (bundleFiles.isEmpty()) {
            throw new NoSuchFileException(
                "Bundle file is not present for given primary term " + primaryTerm + " and generation " + generation
            );
        }
        return bundleFiles.get(0);
    }

    /**
     * Reads only the file→bundle mapping from {@code index.bin} inside a TAR bundle blob.
     *
     * <p>This is a lightweight alternative to {@link #readMetadata} for use in GC paths where
     * {@code segmentInfosBytes} (requiring an extra range-GET for the {@code segments_N} file)
     * is not needed. One range-GET suffices: offset 0 for the TAR header + index.bin data.
     *
     * @return map of segment filename → {@link RemoteSegmentFile} with bundle path and byte offset
     */
    Map<String, RemoteSegmentFile> readIndexBinOnly(RemoteDirectory remoteDataDirectory, String filename) throws IOException {
        final long fileLength = remoteDataDirectory.fileLength(filename);
        final int initialFetchSize = (int) Math.min(65536, fileLength);
        final byte[] firstBlock;
        try (InputStream inputStream = remoteDataDirectory.getBlobContainer().readBlob(filename, 0, initialFetchSize)) {
            firstBlock = inputStream.readAllBytes();
        }

        final int indexBinSize = parseOctal(firstBlock, 124, 12);
        final byte[] indexBinBytes;
        if (indexBinSize + 512 <= initialFetchSize) {
            indexBinBytes = new byte[indexBinSize];
            System.arraycopy(firstBlock, 512, indexBinBytes, 0, indexBinSize);
        } else {
            try (InputStream inputStream = remoteDataDirectory.getBlobContainer().readBlob(filename, 512, indexBinSize)) {
                indexBinBytes = inputStream.readAllBytes();
            }
        }

        final VersionedCodecStreamWrapper<RemoteSegmentMetadata> wrapper = new VersionedCodecStreamWrapper<>(
            new RemoteSegmentMetadataHandlerFactory(),
            RemoteSegmentMetadata.VERSION_ONE,
            RemoteSegmentMetadata.CURRENT_VERSION,
            RemoteSegmentMetadata.METADATA_CODEC
        );
        final RemoteSegmentMetadata rawMetadata = wrapper.readStream(new ByteArrayIndexInput(filename, indexBinBytes));

        // Post-process to bind TarUploadedSegmentMetadata (offset from uploadedFilename "tarName#offset")
        final Map<String, RemoteSegmentFile> processedMap = new HashMap<>();
        for (final Map.Entry<String, RemoteSegmentFile> entry : rawMetadata.getMetadata().entrySet()) {
            final UploadedSegmentMetadata raw = (UploadedSegmentMetadata) entry.getValue();
            final String[] parts = raw.getUploadedFilename().split("#");
            final String tarFilename = parts[0];
            final long offset = Long.parseLong(parts[1]);
            final TarUploadedSegmentMetadata processed = new TarUploadedSegmentMetadata(
                raw.getOriginalFilename(),
                tarFilename,
                offset,
                raw.getChecksum(),
                raw.getLength(),
                null // remoteDataDirectory not needed — only used for byte-offset lookup during reads
            );
            processed.setWrittenByMajor(raw.getWrittenByMajor());
            processedMap.put(entry.getKey(), processed);
        }
        return processedMap;
    }

    public RemoteSegmentMetadata readMetadata(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteDataDirectory,
        String filename
    ) throws IOException {
        final long fileLength = remoteDataDirectory.fileLength(filename);
        final int initialFetchSize = (int) Math.min(65536, fileLength);
        byte[] firstBlock;
        try (InputStream inputStream = remoteDataDirectory.getBlobContainer().readBlob(filename, 0, initialFetchSize)) {
            firstBlock = inputStream.readAllBytes();
        }

        final int indexBinSize = parseOctal(firstBlock, 124, 12);
        byte[] indexBinBytes;
        if (indexBinSize + 512 <= initialFetchSize) {
            indexBinBytes = new byte[indexBinSize];
            System.arraycopy(firstBlock, 512, indexBinBytes, 0, indexBinSize);
        } else {
            try (InputStream inputStream = remoteDataDirectory.getBlobContainer().readBlob(filename, 512, indexBinSize)) {
                indexBinBytes = inputStream.readAllBytes();
            }
        }

        final VersionedCodecStreamWrapper<RemoteSegmentMetadata> wrapper = new VersionedCodecStreamWrapper<>(
            new RemoteSegmentMetadataHandlerFactory(),
            RemoteSegmentMetadata.VERSION_ONE,
            RemoteSegmentMetadata.CURRENT_VERSION,
            RemoteSegmentMetadata.METADATA_CODEC
        );
        final RemoteSegmentMetadata remoteMetadata = wrapper.readStream(new ByteArrayIndexInput(filename, indexBinBytes));

        // Post process to bind TarUploadedSegmentMetadata
        final RemoteSegmentMetadata processedMetadata = postProcessMetadata(remoteDirectory, remoteDataDirectory, remoteMetadata);

        // Retrieve actual segmentInfosBytes by reading the segments_N file from this or other bundles
        final Map<String, RemoteSegmentFile> metadataMap = processedMetadata.getMetadata();
        String segmentsNFile = null;
        for (final String file : metadataMap.keySet()) {
            if (file.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS)) {
                segmentsNFile = file;
                break;
            }
        }

        byte[] segmentInfosBytes = new byte[0];
        if (segmentsNFile != null) {
            final UploadedSegmentMetadata segmentsMeta = (UploadedSegmentMetadata) metadataMap.get(segmentsNFile);
            final long length = segmentsMeta.getLength();
            try (InputStream segmentsStream = segmentsMeta.openStream(0, length)) {
                segmentInfosBytes = segmentsStream.readAllBytes();
            }
        }

        return new RemoteSegmentMetadata(metadataMap, segmentInfosBytes, processedMetadata.getReplicationCheckpoint());
    }

    public RemoteSegmentMetadata initializeToSpecificTimestamp(
        RemoteDirectory remoteDataDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        long timestamp
    ) throws IOException {
        final List<String> bundleFiles = remoteDataDirectory.listFilesByPrefixInLexicographicOrder("rbs_bundle__", Integer.MAX_VALUE);
        final Set<String> lockedBundleFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            bundleFiles,
            Set.of(timestamp),
            TarSegmentUploadStrategy::getTimestamp,
            TarSegmentUploadStrategy::getNodeIdByPrimaryTermAndGen,
            true
        );
        if (lockedBundleFiles.isEmpty()) {
            return null;
        }
        if (lockedBundleFiles.size() > 1) {
            throw new IOException("Expected exactly one bundle file matching timestamp: " + timestamp + " but got " + lockedBundleFiles);
        }
        final String bundleFile = lockedBundleFiles.iterator().next();
        return readMetadata(remoteDirectory, remoteDataDirectory, bundleFile);
    }

    public Map<String, RemoteSegmentMetadata> readLatestNMetadataFiles(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteDataDirectory,
        int count
    ) throws IOException {
        final Map<String, RemoteSegmentMetadata> metadataMap = new java.util.LinkedHashMap<>();
        final List<String> bundleFiles = remoteDataDirectory.listFilesByPrefixInLexicographicOrder("rbs_bundle__", count);
        for (final String file : bundleFiles) {
            metadataMap.put(file, readMetadata(remoteDirectory, remoteDataDirectory, file));
        }
        return metadataMap;
    }

    @Override
    public RemoteStoreSegmentStrategy.MetadataReader getMetadataReader(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId) {
        return new TarRemoteSegmentMetadataReader(remoteDirectory, shardId);
    }

    private class TarRemoteSegmentMetadataReader implements RemoteStoreSegmentStrategy.MetadataReader {
        private final RemoteSegmentStoreDirectory remoteDirectory;
        private final RemoteDirectory remoteDataDirectory;
        private final ShardId shardId;

        public TarRemoteSegmentMetadataReader(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId) {
            this.remoteDirectory = remoteDirectory;
            this.remoteDataDirectory = (RemoteDirectory) remoteDirectory.getDelegate();
            this.shardId = shardId;
        }

        @Override
        public RemoteSegmentMetadata readMetadata() throws IOException {
            return init(remoteDirectory, remoteDataDirectory);
        }

        @Override
        public RemoteSegmentMetadata readMetadata(long primaryTerm, long generation) throws IOException {
            String metadataFile = getMetadataFileForCommit(remoteDataDirectory, primaryTerm, generation);
            return TarSegmentUploadStrategy.this.readMetadata(remoteDirectory, remoteDataDirectory, metadataFile);
        }

        @Override
        public RemoteSegmentMetadata readMetadata(long timestamp) throws IOException {
            return initializeToSpecificTimestamp(remoteDataDirectory, remoteDirectory, timestamp);
        }

        @Override
        public RemoteSegmentMetadata readMetadata(String filename) throws IOException {
            return TarSegmentUploadStrategy.this.readMetadata(remoteDirectory, remoteDataDirectory, filename);
        }

        @Override
        public Map<String, RemoteSegmentMetadata> readLatestNMetadata(int count) throws IOException {
            return readLatestNMetadataFiles(remoteDirectory, remoteDataDirectory, count);
        }

        @Override
        public String getMetadataFilename(long primaryTerm, long generation) throws IOException {
            return getMetadataFileForCommit(remoteDataDirectory, primaryTerm, generation);
        }
    }
}
