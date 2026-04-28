/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.lockmanager.RemoteStoreCommitLevelLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandler;
import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.index.store.remote.segment.archive.SegmentArchiveRetentionHelper;
import org.opensearch.index.store.remote.segment.archive.ZipSegmentParser;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.opensearch.common.blobstore.BlobMetadata;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A RemoteDirectory extension for remote segment store. We need to make sure we don't overwrite a segment file once uploaded.
 * In order to prevent segment overwrite which can occur due to two primary nodes for the same shard at the same time,
 * a unique suffix is added to the uploaded segment file. This class keeps track of filename of segments stored
 * in remote segment store vs filename in local filesystem and provides the consistent Directory interface so that
 * caller will be accessing segment files in the same way as {@code FSDirectory}. Apart from storing actual segment files,
 * remote segment store also keeps track of refresh checkpoints as metadata in a separate path which is handled by
 * another instance of {@code RemoteDirectory}.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public final class RemoteSegmentStoreDirectory extends FilterDirectory implements RemoteStoreCommitLevelLockManager {

    /**
     * Each segment file is uploaded with unique suffix.
     * For example, _0.cfe in local filesystem will be uploaded to remote segment store as _0.cfe__gX7bNIIBrs0AUNsR2yEG
     */
    public static final String SEGMENT_NAME_UUID_SEPARATOR = "__";

    /**
     * remoteDataDirectory is used to store segment files at path: cluster_UUID/index_UUID/shardId/segments/data
     */
    private final RemoteDirectory remoteDataDirectory;
    /**
     * remoteMetadataDirectory is used to store metadata files at path: cluster_UUID/index_UUID/shardId/segments/metadata
     */
    private final RemoteDirectory remoteMetadataDirectory;

    private final RemoteStoreLockManager mdLockManager;

    private final Map<Long, String> metadataFilePinnedTimestampMap;

    private final ThreadPool threadPool;

    /**
     * Keeps track of local segment filename to uploaded filename along with other attributes like checksum.
     * This map acts as a cache layer for uploaded segment filenames which helps avoid calling listAll() each time.
     * It is important to initialize this map on creation of RemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    /**
     * Immutable snapshot of archive state (blob name + entries).
     * Held in a single AtomicReference so both fields are always updated atomically,
     * preventing a reader from seeing a new blob name with old entries or vice versa.
     * A null reference means no archive is active.
     */
    static final class ArchiveState {
        final String blobName;
        final Map<String, SegmentArchiveEntry> entries;
        /** Total byte length of the archive ZIP blob; -1 if unknown (old metadata). */
        final long blobLength;

        ArchiveState(String blobName, Map<String, SegmentArchiveEntry> entries) {
            this(blobName, entries, -1L);
        }

        ArchiveState(String blobName, Map<String, SegmentArchiveEntry> entries, long blobLength) {
            this.blobName = blobName;
            this.entries = entries;
            this.blobLength = blobLength;
        }
    }

    private final java.util.concurrent.atomic.AtomicReference<ArchiveState> archiveStateRef =
        new java.util.concurrent.atomic.AtomicReference<>(null);

    /**
     * LRU cache for central-directory entries of *older* archive blobs.
     * Allows {@link #readFileFromArchiveBlob} to use a range-GET for the
     * file data instead of downloading the entire ZIP.
     *
     * <p>Keyed by archive blob name; value is the map of filename →
     * {@link SegmentArchiveEntry} parsed from that blob's central directory.
     * Bounded to {@value #ARCHIVE_INDEX_CACHE_SIZE} entries so memory stays
     * proportional to active segments, not the full history.
     */
    static final int ARCHIVE_INDEX_CACHE_SIZE = 20;

    /** Max bytes to range-read from the tail of a ZIP for central-directory parsing. */
    private static final int ZIP_TAIL_BYTES = 65_536 + 22; // max EOCD search range + EOCD record

    @SuppressWarnings("serial")
    private final Map<String, Map<String, SegmentArchiveEntry>> archiveIndexCache =
        Collections.synchronizedMap(
            new LinkedHashMap<String, Map<String, SegmentArchiveEntry>>(
                ARCHIVE_INDEX_CACHE_SIZE + 1, 0.75f, true   // accessOrder=true → LRU
            ) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Map<String, SegmentArchiveEntry>> eldest) {
                    return size() > ARCHIVE_INDEX_CACHE_SIZE;
                }
            }
        );

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandler(),
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private static final Logger staticLogger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);

    private final Logger logger;

    /**
     * AtomicBoolean that ensures only one staleCommitDeletion activity is scheduled at a time.
     * Visible for testing
     */
    protected final AtomicBoolean canDeleteStaleCommits = new AtomicBoolean(true);

    /**
     * Tracks the last time (epoch ms) a segment GC run was dispatched.
     * Used to rate-limit GC frequency via {@code cluster.remote_store.segment.metadata.gc.min_interval}.
     * Visible for testing.
     */
    protected final AtomicLong lastSegmentGcRunTimeMs = new AtomicLong(0);

    private final AtomicLong metadataUploadCounter = new AtomicLong(0);

    public static final int METADATA_FILES_TO_FETCH = 10;

    public RemoteSegmentStoreDirectory(
        RemoteDirectory remoteDataDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId
    ) throws IOException {
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.logger = Loggers.getLogger(getClass(), shardId);
        init();
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     * As this cache is specific to an instance of RemoteSegmentStoreDirectory, it is possible that cache becomes stale
     * if another instance of RemoteSegmentStoreDirectory is used to upload/delete segment files.
     * It is caller's responsibility to call init() again to ensure that cache is properly updated.
     *
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata init() throws IOException {
        logger.debug("Start initialisation of remote segment metadata");
        RemoteSegmentMetadata remoteSegmentMetadata = readLatestMetadataFile();
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
            if (remoteSegmentMetadata.isArchiveEnabled()) {
                ArchiveState state = new ArchiveState(remoteSegmentMetadata.getArchiveBlob(), remoteSegmentMetadata.getArchiveEntries(), remoteSegmentMetadata.getArchiveBlobLength());
                archiveStateRef.set(state);
                logger.debug(
                    "Archive metadata loaded: blob={}, entries={}",
                    state.blobName,
                    state.entries != null ? state.entries.size() : 0
                );
            } else {
                archiveStateRef.set(null);
            }
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
            archiveStateRef.set(null);
        }
        logger.debug("Initialisation of remote segment metadata completed");
        return remoteSegmentMetadata;
    }

    /**
     * Initializes the cache to a specific commit which keeps track of all the segment files uploaded to the
     * remote segment store.
     * this is currently used to restore snapshots, where we want to copy segment files from a given commit.
     * TODO: check if we can return read only RemoteSegmentStoreDirectory object from here.
     *
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata initializeToSpecificCommit(long primaryTerm, long commitGeneration, String acquirerId) throws IOException {
        String metadataFilePrefix = MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, commitGeneration);
        String metadataFile = ((RemoteStoreMetadataLockManager) mdLockManager).fetchLockedMetadataFile(metadataFilePrefix, acquirerId);
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
            archiveStateRef.set(
                remoteSegmentMetadata.isArchiveEnabled()
                    ? new ArchiveState(remoteSegmentMetadata.getArchiveBlob(), remoteSegmentMetadata.getArchiveEntries(), remoteSegmentMetadata.getArchiveBlobLength())
                    : null
            );
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
            archiveStateRef.set(null);
        }
        return remoteSegmentMetadata;
    }

    /**
     * Initializes the remote segment metadata to a specific timestamp.
     *
     * @param timestamp The timestamp to initialize the remote segment metadata to.
     * @return The RemoteSegmentMetadata object corresponding to the specified timestamp, or null if no metadata file is found for that timestamp.
     * @throws IOException If an I/O error occurs while reading the metadata file.
     */
    public RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );
        Set<String> lockedMetadataFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            Set.of(timestamp),
            MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen,
            true
        );
        if (lockedMetadataFiles.isEmpty()) {
            return null;
        }
        if (lockedMetadataFiles.size() > 1) {
            throw new IOException(
                "Expected exactly one metadata file matching timestamp: " + timestamp + " but got " + lockedMetadataFiles
            );
        }
        String metadataFile = lockedMetadataFiles.iterator().next();
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
            archiveStateRef.set(
                remoteSegmentMetadata.isArchiveEnabled()
                    ? new ArchiveState(remoteSegmentMetadata.getArchiveBlob(), remoteSegmentMetadata.getArchiveEntries(), remoteSegmentMetadata.getArchiveBlobLength())
                    : null
            );
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
            archiveStateRef.set(null);
        }
        return remoteSegmentMetadata;
    }

    /**
     * Read the latest metadata file to get the list of segments uploaded to the remote segment store.
     * We upload a metadata file per refresh, but it is not unique per refresh. Refresh metadata file is unique for a given commit.
     * The format of refresh metadata filename is: refresh_metadata__PrimaryTerm__Generation__UUID
     * Refresh metadata files keep track of active segments for the shard at the time of refresh.
     * In order to get the list of segment files uploaded to the remote segment store, we need to read the latest metadata file.
     * Each metadata file contains a map where
     * Key is - Segment local filename and
     * Value is - local filename::uploaded filename::checksum
     *
     * @return Map of segment filename to uploaded filename with checksum
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata readLatestMetadataFile() throws IOException {
        RemoteSegmentMetadata remoteSegmentMetadata = null;

        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            METADATA_FILES_TO_FETCH
        );

        RemoteStoreUtils.verifyNoMultipleWriters(metadataFiles, MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        if (metadataFiles.isEmpty() == false) {
            String latestMetadataFile = metadataFiles.get(0);
            logger.trace("Reading latest Metadata file {}", latestMetadataFile);
            remoteSegmentMetadata = readMetadataFile(latestMetadataFile);
        } else {
            logger.trace("No metadata file found, this can happen for new index with no data uploaded to remote segment store");
        }

        return remoteSegmentMetadata;
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(metadataFilename)) {
            byte[] metadataBytes = inputStream.readAllBytes();
            return metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes));
        }
    }

    /**
     * Metadata of a segment that is uploaded to remote segment store.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.3.0")
    public static class UploadedSegmentMetadata {
        // Visible for testing
        static final String SEPARATOR = "::";

        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;
        private final long length;

        /**
         * The Lucene major version that wrote the original segment files.
         * As part of the Lucene version compatibility check, this version information stored in the metadata
         * will be used to skip downloading the segment files unnecessarily
         * if they were written by an incompatible Lucene version.
         */
        private int writtenByMajor;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.join(
                SEPARATOR,
                originalFilename,
                uploadedFilename,
                checksum,
                String.valueOf(length),
                String.valueOf(writtenByMajor)
            );
        }

        public String getChecksum() {
            return this.checksum;
        }

        public long getLength() {
            return this.length;
        }

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            String[] values = uploadedFilename.split(SEPARATOR);
            UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(values[0], values[1], values[2], Long.parseLong(values[3]));
            if (values.length < 5) {
                staticLogger.error("Lucene version is missing for UploadedSegmentMetadata: " + uploadedFilename);
            }

            metadata.setWrittenByMajor(Integer.parseInt(values[4]));

            return metadata;
        }

        public String getOriginalFilename() {
            return originalFilename;
        }

        public void setWrittenByMajor(int writtenByMajor) {
            if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR) {
                this.writtenByMajor = writtenByMajor;
            } else {
                throw new IllegalArgumentException(
                    "Lucene major version supplied ("
                        + writtenByMajor
                        + ") is incorrect. Should be between Version.LATEST ("
                        + Version.LATEST.major
                        + ") and Version.MIN_SUPPORTED_MAJOR ("
                        + Version.MIN_SUPPORTED_MAJOR
                        + ")."
                );
            }
        }
    }

    /**
     * Contains utility methods that provide various parts of metadata filename along with comparator
     * Each metadata filename is of format: PREFIX__PrimaryTerm__Generation__UUID
     */
    public static class MetadataFilenameUtils {
        public static final String SEPARATOR = "__";
        public static final String METADATA_PREFIX = "metadata";

        static String getMetadataFilePrefixForCommit(long primaryTerm, long generation) {
            return String.join(
                SEPARATOR,
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation)
            );
        }

        // Visible for testing
        public static String getMetadataFilename(
            long primaryTerm,
            long generation,
            long translogGeneration,
            long uploadCounter,
            int metadataVersion,
            String nodeId,
            long creationTimestamp
        ) {
            return String.join(
                SEPARATOR,
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation),
                RemoteStoreUtils.invertLong(translogGeneration),
                RemoteStoreUtils.invertLong(uploadCounter),
                String.valueOf(Objects.hash(nodeId)),
                RemoteStoreUtils.invertLong(creationTimestamp),
                String.valueOf(metadataVersion)
            );
        }

        public static String getMetadataFilename(
            long primaryTerm,
            long generation,
            long translogGeneration,
            long uploadCounter,
            int metadataVersion,
            String nodeId
        ) {
            return getMetadataFilename(
                primaryTerm,
                generation,
                translogGeneration,
                uploadCounter,
                metadataVersion,
                nodeId,
                System.currentTimeMillis()
            );
        }

        // Visible for testing
        static long getPrimaryTerm(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[1]);
        }

        // Visible for testing
        static long getGeneration(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[2]);
        }

        public static long getTimestamp(String filename) {
            String[] filenameTokens = filename.split(SEPARATOR);
            return RemoteStoreUtils.invertLong(filenameTokens[filenameTokens.length - 2]);
        }

        public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
            String[] tokens = filename.split(SEPARATOR);
            if (tokens.length < 8) {
                // For versions < 2.11, we don't have node id.
                return null;
            }
            String primaryTermAndGen = String.join(SEPARATOR, tokens[1], tokens[2], tokens[3]);

            String nodeId = tokens[5];
            return new Tuple<>(primaryTermAndGen, nodeId);
        }

    }

    /**
     * Returns list of all the segment files uploaded to remote segment store till the last refresh checkpoint.
     * Any segment file that is uploaded without corresponding metadata file will not be visible as part of listAll().
     * We chose not to return cache entries for listAll as cache can have entries for stale segments as well.
     * Even if we plan to delete stale segments from remote segment store, it will be a periodic operation.
     *
     * @return segment filenames stored in remote segment store
     * @throws IOException if there were any failures in reading the metadata file
     */
    @Override
    public String[] listAll() throws IOException {
        return readLatestMetadataFile().getMetadata().keySet().toArray(new String[0]);
    }

    /**
     * Delete segment file from remote segment store.
     *
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException if the file exists but could not be deleted.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            remoteDataDirectory.deleteFile(remoteFilename);
            segmentsUploadedToRemoteStore.remove(name);
        }
    }

    /**
     * Returns the byte length of a segment file in the remote segment store.
     *
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException         in case of I/O error
     * @throws NoSuchFileException if the file does not exist in the cache or remote segment store
     */
    @Override
    public long fileLength(String name) throws IOException {
        if (segmentsUploadedToRemoteStore.containsKey(name)) {
            return segmentsUploadedToRemoteStore.get(name).getLength();
        }
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            return remoteDataDirectory.fileLength(remoteFilename);
        } else {
            throw new NoSuchFileException(name);
        }
    }

    /**
     * Creates and returns a new instance of {@link RemoteIndexOutput} which will be used to copy files to the remote
     * segment store.
     *
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return remoteDataDirectory.createOutput(getNewRemoteSegmentFilename(name), context);
    }

    /**
     * Opens a stream for reading an existing file and returns {@link RemoteIndexInput} enclosing the stream.
     *
     * @param name the name of an existing file.
     * @throws IOException         in case of I/O error
     * @throws NoSuchFileException if the file does not exist either in cache or remote segment store
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        // Atomically snapshot the archive state to avoid torn reads between blobName and entries.
        ArchiveState archiveState = archiveStateRef.get();
        if (archiveState != null && archiveState.entries != null && archiveState.entries.containsKey(name)) {
            SegmentArchiveEntry archiveEntry = archiveState.entries.get(name);
            logger.trace(
                "Opening {} from archive {} at offset={} length={}",
                name,
                archiveState.blobName,
                archiveEntry.getOffset(),
                archiveEntry.getLength()
            );
            try (
                InputStream archiveStream = remoteDataDirectory.getBlobContainer()
                    .readBlob(archiveState.blobName, archiveEntry.getOffset(), archiveEntry.getLength())
            ) {
                return new ByteArrayIndexInput(name, archiveStream.readAllBytes());
            } catch (IOException archiveEx) {
                // Archive read failed (blob GC'd while in-memory state was stale, or transient error).
                // Re-read latest metadata to get fresh archive reference and retry atomically.
                logger.warn(
                    "Archive read failed for {} from blob {} (offset={} length={}), refreshing metadata: {}",
                    name,
                    archiveState.blobName,
                    archiveEntry.getOffset(),
                    archiveEntry.getLength(),
                    archiveEx.getMessage()
                );
                try {
                    RemoteSegmentMetadata fresh = readLatestMetadataFile();
                    if (fresh != null && fresh.isArchiveEnabled()) {
                        ArchiveState freshState = new ArchiveState(fresh.getArchiveBlob(), fresh.getArchiveEntries(), fresh.getArchiveBlobLength());
                        archiveStateRef.set(freshState); // atomic update for subsequent callers
                        SegmentArchiveEntry freshEntry = freshState.entries != null ? freshState.entries.get(name) : null;
                        if (freshEntry != null) {
                            logger.debug("Retrying {} from refreshed archive blob {}", name, freshState.blobName);
                            try (
                                InputStream retryStream = remoteDataDirectory.getBlobContainer()
                                    .readBlob(freshState.blobName, freshEntry.getOffset(), freshEntry.getLength())
                            ) {
                                return new ByteArrayIndexInput(name, retryStream.readAllBytes());
                            }
                        }
                    } else if (fresh != null) {
                        // Archive flag toggled OFF — switch to per-file path atomically.
                        archiveStateRef.set(null);
                        UploadedSegmentMetadata perFile = fresh.getMetadata().get(name);
                        if (perFile != null) {
                            logger.debug("Archive disabled, reading {} as per-file blob {}", name, perFile.uploadedFilename);
                            return remoteDataDirectory.openInput(perFile.uploadedFilename, perFile.getLength(), context);
                        }
                    }
                } catch (IOException metaEx) {
                    logger.warn("Metadata refresh also failed for {}: {}", name, metaEx.getMessage());
                }
                throw new NoSuchFileException(name + " (archive unavailable and metadata refresh failed or file not found)");
            }
        }

        // Per-file download path.
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            // Guard: if remoteFilename is an archive blob, the file lives inside a ZIP —
            // we must NOT open the raw archive as a regular segment file (that would read
            // ZIP container bytes instead of the individual file's content).
            // This happens when archiveState.entries only covers the latest archive but the
            // file was uploaded in an older archive blob.
            if (SegmentArchiveRetentionHelper.isArchiveBlob(remoteFilename)) {
                logger.debug(
                    "File {} maps to archive blob {} but was not in current archiveState entries; "
                        + "extracting from ZIP via streaming read",
                    name,
                    remoteFilename
                );
                // Pass the known blob length from metadata if available, to avoid a LIST call in readFileFromArchiveBlob.
                // archiveStateRef may refer to a different (newer) archive, so check if any older archive in the LRU cache
                // has stored the length. We rely on the metadata's archiveBlobLength stored at upload time.
                // For now use -1 (unknown) — the length will be stored in metadata when the archive was uploaded
                // with the archiveBlobLength field, but for older archives it falls back to LIST.
                // TODO: thread archiveBlobLength through UploadedSegmentMetadata for per-file lookup.
                return readFileFromArchiveBlob(name, remoteFilename);
            }
            long fileLength = fileLength(name);
            return remoteDataDirectory.openInput(remoteFilename, fileLength, context);
        } else {
            throw new NoSuchFileException(name);
        }
    }

    /**
     * Extracts a single file from an older archive ZIP blob using range-GETs.
     *
     * <p><b>Strategy (2 S3 GETs on cache miss, 1 S3 GET on cache hit):</b>
     * <ol>
     *   <li>Look up the archive's central-directory map in {@link #archiveIndexCache} (LRU, up to
     *       {@value #ARCHIVE_INDEX_CACHE_SIZE} blobs).</li>
     *   <li>On <em>cache miss</em>: range-read the last {@value ZIP_TAIL_BYTES} bytes of the blob to
     *       parse the ZIP central directory via {@link ZipSegmentParser}, populate the cache.</li>
     *   <li>Use the cached {@link SegmentArchiveEntry} to range-read just the target file's bytes.</li>
     * </ol>
     *
     * <p>This replaces the previous full-blob download, reducing S3 bytes transferred from
     * {@code archiveSize} to at most {@code ZIP_TAIL_BYTES + fileSize} per cache-miss call,
     * and to just {@code fileSize} on a cache hit.
     *
     * @param name            the local segment filename to extract (e.g., {@code _a_Lucene90_0.dvm})
     * @param archiveBlobName the remote archive blob name
     * @return an {@link IndexInput} backed by the extracted file bytes
     * @throws NoSuchFileException if the file is not found inside the archive
     * @throws IOException         on I/O failure
     */
    private IndexInput readFileFromArchiveBlob(String name, String archiveBlobName) throws IOException {
        return readFileFromArchiveBlob(name, archiveBlobName, -1L);
    }

    private IndexInput readFileFromArchiveBlob(String name, String archiveBlobName, long knownBlobLength) throws IOException {
        // Step 1: resolve central-directory entries (cache hit = 0 S3 GETs, miss = 1–2 range GETs)
        Map<String, SegmentArchiveEntry> entries = archiveIndexCache.get(archiveBlobName);
        if (entries == null) {
            final long blobLength;
            if (knownBlobLength > 0) {
                // Blob length known from metadata — skip LIST entirely (0 extra S3 ops)
                blobLength = knownBlobLength;
                logger.debug("Using stored blobLength={} for archive blob {}, skipping LIST", blobLength, archiveBlobName);
            } else {
                // Blob length unknown — do 1 LIST to get the size
                Map<String, BlobMetadata> blobs =
                    remoteDataDirectory.getBlobContainer().listBlobsByPrefix(archiveBlobName);
                BlobMetadata meta = blobs.get(archiveBlobName);
                if (meta == null) {
                    throw new NoSuchFileException(archiveBlobName + " (blob not found)");
                }
                blobLength = meta.length();
            }

            // Range-read the tail of the ZIP to parse its central directory.
            long tailOffset = Math.max(0, blobLength - ZIP_TAIL_BYTES);
            long tailLength = blobLength - tailOffset;
            final byte[] tail;
            try (InputStream tailStream = remoteDataDirectory.getBlobContainer()
                .readBlob(archiveBlobName, tailOffset, tailLength)) {
                tail = tailStream.readAllBytes();
            }
            entries = ZipSegmentParser.parseToMap(tail, tailOffset);
            if (entries != null) {
                archiveIndexCache.put(archiveBlobName, entries);
                logger.debug("Cached central directory for archive blob {} ({} entries)", archiveBlobName, entries.size());
            }
        }

        // Step 2: range-read just the target file's bytes (1 S3 GET)
        if (entries != null) {
            SegmentArchiveEntry entry = entries.get(name);
            if (entry != null) {
                logger.debug(
                    "Range-reading {} from archive {} at offset={} length={}",
                    name, archiveBlobName, entry.getOffset(), entry.getLength()
                );
                try (InputStream fileStream = remoteDataDirectory.getBlobContainer()
                    .readBlob(archiveBlobName, entry.getOffset(), entry.getLength())) {
                    return new ByteArrayIndexInput(name, fileStream.readAllBytes());
                }
            }
        }

        // Fallback: full blob download (only if ZipSegmentParser couldn't parse the central directory)
        logger.warn(
            "ZipSegmentParser failed for archive blob {}; falling back to full blob download for {}",
            archiveBlobName, name
        );
        final byte[] archiveBytes;
        try (InputStream blobStream = remoteDataDirectory.getBlobContainer().readBlob(archiveBlobName)) {
            archiveBytes = blobStream.readAllBytes();
        }
        try (ZipInputStream zis = new ZipInputStream(
            new ByteArrayInputStream(archiveBytes))) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                if (name.equals(zipEntry.getName())) {
                    return new ByteArrayIndexInput(name, zis.readAllBytes());
                }
            }
        }
        throw new NoSuchFileException(name + " (not found inside archive blob " + archiveBlobName + ")");
    }

    /**
     * Copies a file from the source directory to a remote based on multi-stream upload support.
     * If vendor plugin supports uploading multiple parts in parallel, <code>BlobContainer#writeBlobByStreams</code>
     * will be used, else, the legacy {@link RemoteSegmentStoreDirectory#copyFrom(Directory, String, String, IOContext)}
     * will be called.
     *
     * @param from     The directory for the file to be uploaded
     * @param src      File to be uploaded
     * @param context  IOContext to be used to open IndexInput of file during remote upload
     * @param listener Listener to handle upload callback events
     */
    public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener) {
        copyFrom(from, src, context, listener, false);
    }

    /**
     * Copies a file from the source directory to a remote based on multi-stream upload support.
     * If vendor plugin supports uploading multiple parts in parallel, <code>BlobContainer#writeBlobByStreams</code>
     * will be used, else, the legacy {@link RemoteSegmentStoreDirectory#copyFrom(Directory, String, String, IOContext)}
     * will be called.
     *
     * @param from     The directory for the file to be uploaded
     * @param src      File to be uploaded
     * @param context  IOContext to be used to open IndexInput of file during remote upload
     * @param listener Listener to handle upload callback events
     */
    public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) {
        try {
            final String remoteFileName = getNewRemoteSegmentFilename(src);
            boolean uploaded = remoteDataDirectory.copyFrom(from, src, remoteFileName, context, () -> {
                try {
                    postUpload(from, src, remoteFileName, getChecksumOfLocalFile(from, src));
                } catch (IOException e) {
                    throw new RuntimeException("Exception in segment postUpload for file " + src, e);
                }
            }, listener, lowPriorityUpload);
            if (uploaded == false) {
                copyFrom(from, src, src, context);
                listener.onResponse(null);
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", src), e);
            listener.onFailure(e);
        }
    }

    /**
     * This acquires a lock on a given commit by creating a lock file in lock directory using {@code FileLockInfo}
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @param acquirerId  Lock Acquirer ID which wants to acquire lock on the commit.
     * @throws IOException         will be thrown in case i) listing file failed or ii) Writing the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void acquireLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.acquire(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Releases a lock which was acquired on given segment commit.
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @param acquirerId  Acquirer ID for which lock needs to be released.
     * @throws IOException         will be thrown in case i) listing lock files failed or ii) deleting the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void releaseLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.release(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Checks if a specific commit have any corresponding lock file.
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @return True if there is at least one lock for given primary term and generation.
     * @throws IOException         will be thrown in case listing lock files failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public Boolean isLockAcquired(long primaryTerm, long generation) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        return isLockAcquired(metadataFile);
    }

    // Visible for testing
    Boolean isLockAcquired(String metadataFile) throws IOException {
        return mdLockManager.isAcquired(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).build());
    }

    // Visible for testing
    String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
            1
        );

        if (metadataFiles.isEmpty()) {
            throw new NoSuchFileException(
                "Metadata file is not present for given primary term " + primaryTerm + " and generation " + generation
            );
        }
        if (metadataFiles.size() != 1) {
            throw new IllegalStateException(
                "there should be only one metadata file for given primary term "
                    + primaryTerm
                    + "and generation "
                    + generation
                    + " but found "
                    + metadataFiles.size()
            );
        }
        return metadataFiles.get(0);
    }

    private void postUpload(Directory from, String src, String remoteFilename, String checksum) throws IOException {
        UploadedSegmentMetadata segmentMetadata = new UploadedSegmentMetadata(src, remoteFilename, checksum, from.fileLength(src));
        segmentsUploadedToRemoteStore.put(src, segmentMetadata);
    }

    /**
     * Registers a segment file as uploaded via archive. The remote filename is the archive blob name,
     * and the file's actual data is at a specific offset/length within the archive.
     */
    public void postUploadForArchive(String src, String archiveBlobName, SegmentArchiveEntry entry, Directory from) throws IOException {
        String checksum = getChecksumOfLocalFile(from, src);
        UploadedSegmentMetadata segmentMetadata = new UploadedSegmentMetadata(src, archiveBlobName, checksum, entry.getLength());
        segmentsUploadedToRemoteStore.put(src, segmentMetadata);
    }

    /**
     * Copies an existing src file from directory from to a non-existent file dest in this directory.
     * Once the segment is uploaded to remote segment store, update the cache accordingly.
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        String remoteFilename = getNewRemoteSegmentFilename(dest);
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        postUpload(from, src, remoteFilename, getChecksumOfLocalFile(from, src));
    }

    /**
     * Checks if the file exists in the uploadedSegments cache and the checksum matches.
     * It is important to match the checksum as the same segment filename can be used for different
     * segments due to a concurrency issue.
     *
     * @param localFilename filename of segment stored in local filesystem
     * @param checksum      checksum of the segment file
     * @return true if file exists in cache and checksum matches.
     */
    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename)
            && segmentsUploadedToRemoteStore.get(localFilename).checksum.equals(checksum);
    }

    /**
     * Upload metadata file
     *
     * @param segmentFiles         segment files that are part of the shard at the time of the latest refresh
     * @param segmentInfosSnapshot SegmentInfos bytes to store as part of metadata file
     * @param storeDirectory instance of local directory to temporarily create metadata file before upload
     * @param translogGeneration translog generation
     * @param replicationCheckpoint ReplicationCheckpoint of primary shard
     * @param nodeId node id
     * @throws IOException in case of I/O error while uploading the metadata file
     */
    public void uploadMetadata(
        Collection<String> segmentFiles,
        SegmentInfos segmentInfosSnapshot,
        Directory storeDirectory,
        long translogGeneration,
        ReplicationCheckpoint replicationCheckpoint,
        String nodeId
    ) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(),
                segmentInfosSnapshot.getGeneration(),
                translogGeneration,
                metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION,
                nodeId
            );
            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(metadataFilename, IOContext.DEFAULT)) {
                    Map<String, Integer> segmentToLuceneVersion = getSegmentToLuceneVersion(segmentFiles, segmentInfosSnapshot);
                    Map<String, String> uploadedSegments = new HashMap<>();
                    for (String file : segmentFiles) {
                        if (segmentsUploadedToRemoteStore.containsKey(file)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.originalFilename));
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file);
                        }
                    }

                    ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
                    segmentInfosSnapshot.write(
                        new ByteBuffersIndexOutput(byteBuffersIndexOutput, "Snapshot of SegmentInfos", "SegmentInfos")
                    );
                    byte[] segmentInfoSnapshotByteArray = byteBuffersIndexOutput.toArrayCopy();

                    metadataStreamWrapper.writeStream(
                        indexOutput,
                        new RemoteSegmentMetadata(
                            RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments),
                            segmentInfoSnapshotByteArray,
                            replicationCheckpoint
                        )
                    );
                }
                storeDirectory.sync(Collections.singleton(metadataFilename));
                remoteMetadataDirectory.copyFrom(storeDirectory, metadataFilename, metadataFilename, IOContext.DEFAULT);
            } finally {
                tryAndDeleteLocalFile(metadataFilename, storeDirectory);
            }
        }
    }

    /**
     * Upload metadata file with archive fields.
     * This overload includes the archive blob name and per-file archive entries (offset, length, checksum).
     */
    public void uploadMetadata(
        Collection<String> segmentFiles,
        SegmentInfos segmentInfosSnapshot,
        Directory storeDirectory,
        long translogGeneration,
        ReplicationCheckpoint replicationCheckpoint,
        String nodeId,
        String archiveBlobName,
        Map<String, SegmentArchiveEntry> archiveEntries
    ) throws IOException {
        uploadMetadata(segmentFiles, segmentInfosSnapshot, storeDirectory, translogGeneration,
            replicationCheckpoint, nodeId, archiveBlobName, archiveEntries, -1L);
    }

    public void uploadMetadata(
        Collection<String> segmentFiles,
        SegmentInfos segmentInfosSnapshot,
        Directory storeDirectory,
        long translogGeneration,
        ReplicationCheckpoint replicationCheckpoint,
        String nodeId,
        String archiveBlobName,
        Map<String, SegmentArchiveEntry> archiveEntries,
        long archiveBlobLength
    ) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(),
                segmentInfosSnapshot.getGeneration(),
                translogGeneration,
                metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION,
                nodeId
            );
            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(metadataFilename, IOContext.DEFAULT)) {
                    Map<String, Integer> segmentToLuceneVersion = getSegmentToLuceneVersion(segmentFiles, segmentInfosSnapshot);
                    Map<String, String> uploadedSegments = new HashMap<>();
                    for (String file : segmentFiles) {
                        if (segmentsUploadedToRemoteStore.containsKey(file)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.originalFilename));
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file);
                        }
                    }

                    ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
                    segmentInfosSnapshot.write(
                        new ByteBuffersIndexOutput(byteBuffersIndexOutput, "Snapshot of SegmentInfos", "SegmentInfos")
                    );
                    byte[] segmentInfoSnapshotByteArray = byteBuffersIndexOutput.toArrayCopy();

                    RemoteSegmentMetadata meta = new RemoteSegmentMetadata(
                        RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments),
                        segmentInfoSnapshotByteArray,
                        replicationCheckpoint,
                        true,
                        archiveBlobName,
                        "zip_stored",
                        archiveEntries
                    );
                    if (archiveBlobLength > 0) {
                        meta.setArchiveBlobLength(archiveBlobLength);
                    }
                    metadataStreamWrapper.writeStream(indexOutput, meta);
                }
                storeDirectory.sync(Collections.singleton(metadataFilename));
                remoteMetadataDirectory.copyFrom(storeDirectory, metadataFilename, metadataFilename, IOContext.DEFAULT);
            } finally {
                tryAndDeleteLocalFile(metadataFilename, storeDirectory);
            }
        }
    }

    /**
     * Parses the provided SegmentInfos to retrieve a mapping of the provided segment files to
     * the respective Lucene major version that wrote the segments
     *
     * @param segmentFiles         List of segment files for which the Lucene major version is needed
     * @param segmentInfosSnapshot SegmentInfos instance to parse
     * @return Map of the segment file to its Lucene major version
     */
    private Map<String, Integer> getSegmentToLuceneVersion(Collection<String> segmentFiles, SegmentInfos segmentInfosSnapshot) {
        Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
        for (SegmentCommitInfo segmentCommitInfo : segmentInfosSnapshot) {
            SegmentInfo info = segmentCommitInfo.info;
            Set<String> segFiles = info.files();
            for (String file : segFiles) {
                segmentToLuceneVersion.put(file, info.getVersion().major);
            }
        }

        for (String file : segmentFiles) {
            if (segmentToLuceneVersion.containsKey(file) == false) {
                if (file.equals(segmentInfosSnapshot.getSegmentsFileName())) {
                    segmentToLuceneVersion.put(file, segmentInfosSnapshot.getCommitLuceneVersion().major);
                } else {
                    // Fallback to the Lucene major version of the respective segment's .si file
                    String segmentInfoFileName = RemoteStoreUtils.getSegmentName(file) + ".si";
                    segmentToLuceneVersion.put(file, segmentToLuceneVersion.get(segmentInfoFileName));
                }
            }
        }

        return segmentToLuceneVersion;
    }

    /**
     * Try to delete file from local store. Fails silently on failures
     *
     * @param filename: name of the file to be deleted
     */
    private void tryAndDeleteLocalFile(String filename, Directory directory) {
        try {
            logger.debug("Deleting file: " + filename);
            directory.deleteFile(filename);
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.trace("Exception while deleting. Missing file : " + filename, e);
        } catch (IOException e) {
            logger.warn("Exception while deleting: " + filename, e);
        }
    }

    private String getChecksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    private String getExistingRemoteFilename(String localFilename) {
        if (segmentsUploadedToRemoteStore.containsKey(localFilename)) {
            return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
        } else {
            return null;
        }
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(SEGMENT_NAME_UUID_SEPARATOR)[0];
    }

    // Visible for testing
    public Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return Collections.unmodifiableMap(this.segmentsUploadedToRemoteStore);
    }

    // Visible for testing
    Set<String> getMetadataFilesToFilterActiveSegments(
        final int lastNMetadataFilesToKeep,
        final List<String> sortedMetadataFiles,
        final Set<String> lockedMetadataFiles
    ) {
        // the idea here is for each deletable md file, we can consider the segments present in non-deletable md file
        // before this and non-deletable md file after this to compute the active segment files.
        // For ex:
        // lastNMetadataFilesToKeep = 3
        // sortedMetadataFiles = [m1, m2, m3, m4, m5, m6(locked), m7(locked), m8(locked), m9(locked), m10]
        // lockedMetadataFiles = m6, m7, m8, m9
        // then the returned set will be (m3, m6, m9)
        final Set<String> metadataFilesToFilterActiveSegments = new HashSet<>();
        for (int idx = lastNMetadataFilesToKeep; idx < sortedMetadataFiles.size(); idx++) {
            if (lockedMetadataFiles.contains(sortedMetadataFiles.get(idx)) == false) {
                String prevMetadata = (idx - 1) >= 0 ? sortedMetadataFiles.get(idx - 1) : null;
                String nextMetadata = (idx + 1) < sortedMetadataFiles.size() ? sortedMetadataFiles.get(idx + 1) : null;

                if (prevMetadata != null && (lockedMetadataFiles.contains(prevMetadata) || idx == lastNMetadataFilesToKeep)) {
                    // if previous metadata of deletable md is locked, add it to md files for active segments.
                    metadataFilesToFilterActiveSegments.add(prevMetadata);
                }
                if (nextMetadata != null && lockedMetadataFiles.contains(nextMetadata)) {
                    // if next metadata of deletable md is locked, add it to md files for active segments.
                    metadataFilesToFilterActiveSegments.add(nextMetadata);
                }
            }
        }
        return metadataFilesToFilterActiveSegments;
    }

    /**
     * Delete stale segment and metadata files
     * One metadata file is kept per commit (refresh updates the same file). To read segments uploaded to remote store,
     * we just need to read the latest metadata file.
     * Assumptions:
     * (1) if a segment file is not present in a md file, it will never be present in any md file created after that, and
     * (2) if (md1, md2, md3) are in sorted order, it is not possible that a segment file will be in md1 and md3 but not in md2.
     * <p>
     * for each deletable md file, segments present in non-deletable md file before this and non-deletable md file
     * after this are sufficient to compute the list of active or non-deletable segment files referenced by a deletable
     * md file
     *
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     * @throws IOException in case of I/O error while reading from / writing to remote segment store
     */
    public void deleteStaleSegments(int lastNMetadataFilesToKeep) throws IOException {
        if (lastNMetadataFilesToKeep == -1) {
            logger.info(
                "Stale segment deletion is disabled if cluster.remote_store.index.segment_metadata.retention.max_count is set to -1"
            );
            return;
        }

        List<String> sortedMetadataFileList = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );
        if (sortedMetadataFileList.size() <= lastNMetadataFilesToKeep) {
            logger.debug(
                "Number of commits in remote segment store={}, lastNMetadataFilesToKeep={}",
                sortedMetadataFileList.size(),
                lastNMetadataFilesToKeep
            );
            return;
        }

        // Check last fetch status of pinned timestamps. If stale, return.
        if (RemoteStoreUtils.isPinnedTimestampStateStale()) {
            logger.warn("Skipping remote segment store garbage collection as last fetch of pinned timestamp is stale");
            return;
        }

        Tuple<Long, Set<Long>> pinnedTimestampsState = RemoteStorePinnedTimestampService.getPinnedTimestamps();

        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            sortedMetadataFileList,
            pinnedTimestampsState.v2(),
            metadataFilePinnedTimestampMap,
            MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        final Set<String> allLockFiles = new HashSet<>(implicitLockedFiles);

        try {
            allLockFiles.addAll(
                ((RemoteStoreMetadataLockManager) mdLockManager).fetchLockedMetadataFiles(MetadataFilenameUtils.METADATA_PREFIX)
            );
        } catch (Exception e) {
            logger.error("Exception while fetching segment metadata lock files, skipping deleteStaleSegments", e);
            return;
        }

        List<String> metadataFilesEligibleToDelete = new ArrayList<>(
            sortedMetadataFileList.subList(lastNMetadataFilesToKeep, sortedMetadataFileList.size())
        );

        // Along with last N files, we need to keep files since last successful run of scheduler
        long lastSuccessfulFetchOfPinnedTimestamps = pinnedTimestampsState.v1();
        metadataFilesEligibleToDelete = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFilesEligibleToDelete,
            MetadataFilenameUtils::getTimestamp,
            lastSuccessfulFetchOfPinnedTimestamps
        );

        List<String> metadataFilesToBeDeleted = metadataFilesEligibleToDelete.stream()
            .filter(metadataFile -> allLockFiles.contains(metadataFile) == false)
            .collect(Collectors.toList());

        logger.debug(
            "metadataFilesEligibleToDelete={} metadataFilesToBeDeleted={}",
            metadataFilesEligibleToDelete,
            metadataFilesToBeDeleted
        );

        Map<String, UploadedSegmentMetadata> activeSegmentFilesMetadataMap = new HashMap<>();
        Set<String> activeSegmentRemoteFilenames = new HashSet<>();
        // Collect active archive blob names directly from active metadata — no extra LIST needed.
        Set<String> activeArchiveBlobNames = new HashSet<>();

        final Set<String> metadataFilesToFilterActiveSegments = getMetadataFilesToFilterActiveSegments(
            lastNMetadataFilesToKeep,
            sortedMetadataFileList,
            allLockFiles
        );

        for (String metadataFile : metadataFilesToFilterActiveSegments) {
            RemoteSegmentMetadata activeMeta = readMetadataFile(metadataFile);
            Map<String, UploadedSegmentMetadata> segmentMetadataMap = activeMeta.getMetadata();
            activeSegmentFilesMetadataMap.putAll(segmentMetadataMap);
            activeSegmentRemoteFilenames.addAll(
                segmentMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet())
            );
            if (activeMeta.isArchiveEnabled() && activeMeta.getArchiveBlob() != null) {
                activeArchiveBlobNames.add(activeMeta.getArchiveBlob());
            }
        }
        Set<String> deletedSegmentFiles = new HashSet<>();
        for (String metadataFile : metadataFilesToBeDeleted) {
            RemoteSegmentMetadata staleMeta = readMetadataFile(metadataFile);
            Map<String, UploadedSegmentMetadata> staleSegmentFilesMetadataMap = staleMeta.getMetadata();
            Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values()
                .stream()
                .map(metadata -> metadata.uploadedFilename)
                .collect(Collectors.toSet());
            AtomicBoolean deletionSuccessful = new AtomicBoolean(true);
            staleSegmentRemoteFilenames.stream()
                .filter(file -> activeSegmentRemoteFilenames.contains(file) == false)
                .filter(file -> deletedSegmentFiles.contains(file) == false)
                .forEach(file -> {
                    try {
                        remoteDataDirectory.deleteFile(file);
                        deletedSegmentFiles.add(file);
                        if (!activeSegmentFilesMetadataMap.containsKey(getLocalSegmentFilename(file))) {
                            segmentsUploadedToRemoteStore.remove(getLocalSegmentFilename(file));
                        }
                    } catch (NoSuchFileException e) {
                        logger.info("Segment file {} corresponding to metadata file {} does not exist in remote", file, metadataFile);
                    } catch (IOException e) {
                        deletionSuccessful.set(false);
                        logger.warn(
                            "Exception while deleting segment file {} corresponding to metadata file {}. Deletion will be re-tried",
                            file,
                            metadataFile
                        );
                    }
                });
            // Note: when archive is ON, UploadedSegmentMetadata.uploadedFilename = archiveBlobName for ALL files
            // in that archive. The Set deduplication above means the archive ZIP is deleted exactly once
            // via the staleSegmentRemoteFilenames loop — no explicit archive blob deletion needed here.
            if (deletionSuccessful.get()) {
                logger.debug("Deleting stale metadata file {} from remote segment store", metadataFile);
                remoteMetadataDirectory.deleteFile(metadataFile);
            }
        }
        logger.debug("deletedSegmentFiles={}", deletedSegmentFiles);
    }

    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep) {
        deleteStaleSegmentsAsync(lastNMetadataFilesToKeep, 0, ActionListener.wrap(r -> {}, e -> {}));
    }

    /**
     * Delete stale segment and metadata files asynchronously, with a minimum GC interval guard.
     *
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     * @param minIntervalMs            minimum milliseconds that must elapse between consecutive GC runs;
     *                                 {@code 0} disables rate-limiting and preserves the original behaviour
     */
    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep, long minIntervalMs) {
        deleteStaleSegmentsAsync(lastNMetadataFilesToKeep, minIntervalMs, ActionListener.wrap(r -> {}, e -> {}));
    }

    /**
     * Delete stale segment and metadata files asynchronously, with a minimum GC interval guard.
     * <p>
     * When {@code minIntervalMs > 0}, the GC is skipped if less than {@code minIntervalMs} have elapsed
     * since the last dispatch. The underlying {@link #deleteStaleSegments(int)} algorithm is unchanged —
     * it still performs a full S3 LIST and correctly identifies stale files each time it runs.
     * Skipping only delays cleanup; it never causes orphaned files or incorrect deletions.
     *
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     * @param minIntervalMs            minimum milliseconds between GC runs; {@code 0} = no rate-limit
     * @param listener                 callback on completion or failure
     */
    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep, long minIntervalMs, ActionListener<Void> listener) {
        if (minIntervalMs > 0 && (threadPool.absoluteTimeInMillis() - lastSegmentGcRunTimeMs.get()) < minIntervalMs) {
            logger.trace("Skipping segment GC — min interval {}ms not yet elapsed since last run", minIntervalMs);
            return;
        }
        if (canDeleteStaleCommits.compareAndSet(true, false)) {
            try {
                threadPool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
                    try {
                        lastSegmentGcRunTimeMs.set(threadPool.absoluteTimeInMillis());
                        deleteStaleSegments(lastNMetadataFilesToKeep);
                        listener.onResponse(null);
                    } catch (Exception e) {
                        logger.error(
                            "Exception while deleting stale commits from remote segment store, will retry delete post next commit",
                            e
                        );
                        listener.onFailure(e);
                    } finally {
                        canDeleteStaleCommits.set(true);
                    }
                });
            } catch (Exception e) {
                logger.error("Exception occurred while scheduling deleteStaleCommits", e);
                canDeleteStaleCommits.set(true);
                listener.onFailure(e);
            }
        }
    }

    public static void remoteDirectoryCleanup(
        RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
        String remoteStoreRepoForIndex,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy
    ) {
        try {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = (RemoteSegmentStoreDirectory) remoteDirectoryFactory.newDirectory(
                remoteStoreRepoForIndex,
                indexUUID,
                shardId,
                pathStrategy
            );
            remoteSegmentStoreDirectory.deleteStaleSegments(0);
            remoteSegmentStoreDirectory.deleteIfEmpty();
        } catch (Exception e) {
            staticLogger.error("Exception occurred while deleting directory", e);
        }
    }

    /*
    Tries to delete shard level directory if it is empty
    Return true if it deleted it successfully
     */
    private boolean deleteIfEmpty() throws IOException {
        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            1
        );
        if (metadataFiles.size() != 0) {
            logger.info("Remote directory still has files, not deleting the path");
            return false;
        }

        try {
            remoteDataDirectory.delete();
            remoteMetadataDirectory.delete();
            mdLockManager.delete();
        } catch (Exception e) {
            logger.error("Exception occurred while deleting directory", e);
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        deleteStaleSegmentsAsync(0, 0, ActionListener.wrap(r -> deleteIfEmpty(), e -> logger.error("Failed to cleanup remote directory")));
    }
}
