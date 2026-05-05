/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is remote store.
 *
 * @opensearch.internal
 */
public class RemoteStoreReplicationSource implements SegmentReplicationSource {

    private static final Logger logger = LogManager.getLogger(RemoteStoreReplicationSource.class);

    private final IndexShard indexShard;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    /**
     * Option C (revised): cache the last successfully fetched metadata to avoid redundant S3 calls
     * when the checkpoint hasn't changed (i.e. no new segments since last replication cycle).
     * Guarded by the fact that ReplicationCheckpoint equality covers primaryTerm + segmentInfosVersion,
     * so a new primary term or new segments always produces a different checkpoint → cache miss → fresh fetch.
     */
    private volatile ReplicationCheckpoint lastFetchedCheckpoint = null;
    private volatile RemoteSegmentMetadata lastFetchedMetadata = null;

    public RemoteStoreReplicationSource(IndexShard indexShard) {
        this.indexShard = indexShard;
        FilterDirectory remoteStoreDirectory = (FilterDirectory) indexShard.remoteStore().directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();
    }

    @Override
    public void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    ) {
        Map<String, StoreFileMetadata> metadataMap;
        try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = indexShard.getSegmentInfosSnapshot()) {
            final Version version = segmentInfosSnapshot.get().getCommitLuceneVersion();
            final RemoteSegmentMetadata mdFile = getRemoteSegmentMetadata(checkpoint);
            // During initial recovery flow, the remote store might not
            // have metadata as primary hasn't uploaded anything yet.
            if (mdFile == null && indexShard.state().equals(IndexShardState.STARTED) == false) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, Collections.emptyMap(), null));
                return;
            }
            assert mdFile != null : "Remote metadata file can't be null if shard is active " + indexShard.state();
            metadataMap = mdFile.getMetadata()
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        e -> e.getKey(),
                        e -> new StoreFileMetadata(
                            e.getValue().getOriginalFilename(),
                            e.getValue().getLength(),
                            Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
                            version,
                            null
                        )
                    )
                );
            listener.onResponse(new CheckpointInfoResponse(mdFile.getReplicationCheckpoint(), metadataMap, mdFile.getSegmentInfosBytes()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        try {
            if (filesToFetch.isEmpty()) {
                listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                return;
            }
            logger.debug("Downloading segment files from remote store {}", filesToFetch);

            // Do NOT call remoteDirectory.init() here. getCheckpointMetadata() already
            // called init() which populated segmentsUploadedToRemoteStore and archiveStateRef
            // from the same metadata version used to compute filesToFetch. A second init()
            // would read the LATEST metadata file, which may have advanced (e.g. due to a
            // merge on the primary) and no longer contain entries for files in filesToFetch,
            // causing NoSuchFileException during download.
            if (!remoteDirectory.getSegmentsUploadedToRemoteStore().isEmpty()) {
                final Directory storeDirectory = indexShard.store().directory();
                final Collection<String> directoryFiles = List.of(storeDirectory.listAll());
                final List<String> toDownloadSegmentNames = new ArrayList<>();
                for (StoreFileMetadata fileMetadata : filesToFetch) {
                    String file = fileMetadata.name();
                    assert directoryFiles.contains(file) == false : "Local store already contains the file " + file;
                    toDownloadSegmentNames.add(file);
                }
                indexShard.getFileDownloader()
                    .downloadAsync(
                        cancellableThreads,
                        remoteDirectory,
                        new ReplicationStatsDirectoryWrapper(storeDirectory, fileProgressTracker),
                        toDownloadSegmentNames,
                        ActionListener.map(listener, r -> new GetSegmentFilesResponse(filesToFetch))
                    );
            } else {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        } catch (IOException | RuntimeException e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void cancel() {
        this.cancellableThreads.cancel("Canceled by target");
    }

    @Override
    public String getDescription() {
        return "RemoteStoreReplicationSource";
    }

    /**
     * Fetch remote segment metadata for the given checkpoint, applying two optimizations:
     *
     * <p><b>Option C (revised) — checkpoint cache:</b> If the incoming checkpoint equals the last
     * successfully fetched checkpoint, return the cached {@link RemoteSegmentMetadata} immediately
     * (0 S3 LIST, 0 S3 GET). Same checkpoint means same {@code primaryTerm + segmentInfosVersion},
     * i.e. no new segments since the last cycle.
     *
     * <p><b>Option A — direct GET via known filename:</b> If the checkpoint carries a
     * {@link ReplicationCheckpoint#getMetadataFilename()}, skip the S3 LIST and call
     * {@link RemoteSegmentStoreDirectory#initFromMetadataFilename(String)} instead of
     * {@link RemoteSegmentStoreDirectory#init()} (0 LIST, 1 GET).
     *
     * <p>Falls back to {@link RemoteSegmentStoreDirectory#init()} (1 LIST + 1 GET) when neither
     * optimization applies (e.g. first fetch after startup, or checkpoint from an older primary).
     *
     * @param checkpoint the replication checkpoint received from the primary
     * @return the fetched (or cached) segment metadata
     * @throws IOException on S3 read failure
     */
    private RemoteSegmentMetadata getRemoteSegmentMetadata(ReplicationCheckpoint checkpoint) throws IOException {
        // Option C: return cached metadata if checkpoint hasn't advanced
        if (checkpoint.equals(lastFetchedCheckpoint) && lastFetchedMetadata != null) {
            logger.trace(
                "Returning cached segment metadata for unchanged checkpoint primaryTerm={} segmentInfosVersion={}",
                checkpoint.getPrimaryTerm(),
                checkpoint.getSegmentInfosVersion()
            );
            return lastFetchedMetadata;
        }

        AtomicReference<RemoteSegmentMetadata> mdFile = new AtomicReference<>();
        String metadataFilename = checkpoint.getMetadataFilename();
        if (metadataFilename != null) {
            // Option A: direct GET — primary told us the exact filename, no LIST needed
            logger.trace("Fetching segment metadata via direct GET (filename={})", metadataFilename);
            cancellableThreads.executeIO(() -> mdFile.set(remoteDirectory.initFromMetadataFilename(metadataFilename)));
        } else {
            // Fallback: LIST + GET (old behavior — no filename in checkpoint)
            logger.trace("Fetching segment metadata via LIST (no filename in checkpoint)");
            cancellableThreads.executeIO(() -> mdFile.set(remoteDirectory.init()));
        }

        RemoteSegmentMetadata result = mdFile.get();
        // Update cache on success (result may be null during initial recovery)
        if (result != null) {
            lastFetchedCheckpoint = checkpoint;
            lastFetchedMetadata = result;
        }
        return result;
    }
}
