/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import java.nio.file.Path;
import java.util.List;

/**
 * Immutable snapshot of one shard's translog data ready for inclusion in a node-level batch upload.
 *
 * <p>Produced by {@code TranslogRemoteStoreStrategy#upload} when the strategy returns
 * {@code batching is supported}: rather than uploading immediately, the shard packages
 * its translog files into this type and hands it off to the node-scoped batch coordinator.
 *
 * <p>The coordinator collects these from multiple shards/indices and then calls
 * {@code TarTranslogRemoteStoreStrategy#uploadBatch} with the full list when the batch is ready.
 *
 * <p>All file references use the local filesystem path so the batch uploader can stream them
 * lazily — no in-memory buffering of the raw bytes is required until upload begins.
 *
 */
final class TranslogShardBatch {

    /** Index UUID — required because coordinators are node-scoped (not per-index). */
    private final String indexUUID;
    private final int shardId;
    private final long primaryTerm;
    private final long generation;
    private final long minTranslogGeneration;
    /** Files to include in the batch upload — tlog and ckp files for this shard. */
    private final List<BatchFile> files;

    /** The lowest sequence number present in these translog files (-1 if unknown). */
    private final long minSeqNo;
    /** The highest sequence number present in these translog files (-1 if unknown). */
    private final long maxSeqNo;
    /** The global checkpoint at upload time (-1 if unknown). */
    private final long globalCheckpoint;

    public TranslogShardBatch(
        String indexUUID,
        int shardId,
        long primaryTerm,
        long generation,
        long minTranslogGeneration,
        List<BatchFile> files,
        long minSeqNo,
        long maxSeqNo,
        long globalCheckpoint
    ) {
        this.indexUUID = indexUUID;
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.files = List.copyOf(files);
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.globalCheckpoint = globalCheckpoint;
    }

    /** Convenience constructor for cases where seq-no metadata is not yet known. */
    public TranslogShardBatch(
        String indexUUID,
        int shardId,
        long primaryTerm,
        long generation,
        long minTranslogGeneration,
        List<BatchFile> files
    ) {
        this(indexUUID, shardId, primaryTerm, generation, minTranslogGeneration, files, -1L, -1L, -1L);
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public int getShardId() {
        return shardId;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public List<BatchFile> getFiles() {
        return files;
    }

    public long getMinSeqNo() {
        return minSeqNo;
    }

    public long getMaxSeqNo() {
        return maxSeqNo;
    }

    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    /** Total uncompressed bytes across all files in this shard batch. */
    public long totalBytes() {
        long total = 0;
        for (BatchFile f : files) {
            total += f.getSizeBytes();
        }
        return total;
    }

    /**
     * One file to include in the batch. The {@link #getRemoteName()} is the path that will be used
     * inside the archive blob (e.g. {@code indexUUID/shardId/primaryTerm/translog-N.tlog}).
     */
    public static final class BatchFile {
        private final Path localPath;
        private final String remoteName;
        private final long sizeBytes;

        public BatchFile(Path localPath, String remoteName, long sizeBytes) {
            this.localPath = localPath;
            this.remoteName = remoteName;
            this.sizeBytes = sizeBytes;
        }

        public Path getLocalPath() {
            return localPath;
        }

        public String getRemoteName() {
            return remoteName;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }
    }
}
