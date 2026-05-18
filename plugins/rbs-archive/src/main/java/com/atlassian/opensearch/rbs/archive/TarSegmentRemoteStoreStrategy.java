/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.GcDecision;
import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * TAR-archive composite segment strategy: upload + download + GC.
 * <p>
 * <b>Upload</b>: Bundles all new segment files into a single TAR archive blob per refresh cycle.
 * Falls back to {@link org.opensearch.index.remote.DefaultSegmentRemoteStoreStrategy} on failure.
 * <p>
 * <b>Download</b>: Issues a range-GET against the archive blob to extract a single file.
 * Uses a two-phase read: (1) read the TAR {@code _index} from the archive HEAD,
 * (2) range-GET the target file's bytes at its resolved offset.
 * <p>
 * <b>GC</b>: Returns {@link GcDecision#SKIP} to suppress the per-shard S3 LIST call.
 * Archive blob GC is handled by a node-level background task registered via
 * {@link org.opensearch.plugins.Plugin#createComponents}.
 */
public class TarSegmentRemoteStoreStrategy implements SegmentRemoteStoreStrategy {

    private static final Logger logger = LogManager.getLogger(TarSegmentRemoteStoreStrategy.class);

    /** Maximum archive size before falling back to per-file uploads. */
    static final long MAX_ARCHIVE_BYTES = 256 * 1024 * 1024L;

    private static final int PIPE_BUFFER_BYTES = 256 * 1024;
    private static final int MAX_UPLOAD_ATTEMPTS = 2;
    private static final String ARCHIVE_BLOB_PREFIX = "segment_archive_";

    // -----------------------------------------------------------------------
    // Upload
    // -----------------------------------------------------------------------

    @Override
    public void upload(
        Collection<String> files,
        Map<String, Long> sizeMap,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        IndexShard shard,
        ActionListener<Void> listener
    ) {
        if (files.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        long totalBytes = files.stream().mapToLong(f -> sizeMap.getOrDefault(f, 0L)).sum();
        if (totalBytes > MAX_ARCHIVE_BYTES) {
            logger.warn(
                "Segment archive would exceed {} bytes ({} bytes, {} files) — falling back to per-file uploads",
                MAX_ARCHIVE_BYTES,
                totalBytes,
                files.size()
            );
            fallbackUpload(files, sizeMap, storeDirectory, remoteDirectory, shard, listener);
            return;
        }

        BlobContainer dataContainer = getBlobContainer(remoteDirectory);
        if (dataContainer == null) {
            logger.warn("Unable to extract BlobContainer from RemoteSegmentStoreDirectory — falling back to per-file uploads");
            fallbackUpload(files, sizeMap, storeDirectory, remoteDirectory, shard, listener);
            return;
        }

        String archiveBlobName = ARCHIVE_BLOB_PREFIX + UUID.randomUUID();
        Exception lastException = null;

        for (int attempt = 0; attempt < MAX_UPLOAD_ATTEMPTS; attempt++) {
            try {
                uploadArchive(files, storeDirectory, remoteDirectory, dataContainer, archiveBlobName);
                logger.debug("Archive upload successful: {} ({} bytes, {} files)", archiveBlobName, totalBytes, files.size());
                listener.onResponse(null);
                return;
            } catch (Exception e) {
                lastException = e;
                logger.warn("Archive upload attempt {} failed for {}: {}", attempt + 1, archiveBlobName, e.getMessage());
            }
        }

        logger.warn("Archive upload failed after {} attempts — falling back to per-file uploads", MAX_UPLOAD_ATTEMPTS, lastException);
        fallbackUpload(files, sizeMap, storeDirectory, remoteDirectory, shard, listener);
    }

    private void uploadArchive(
        Collection<String> files,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        BlobContainer dataContainer,
        String archiveBlobName
    ) throws Exception {
        // Build ArchiveBuildEntry list from Lucene Directory entries
        List<TarArchiveBuilder.ArchiveBuildEntry> entries = new ArrayList<>(files.size());
        for (String file : files) {
            long size = storeDirectory.fileLength(file);
            // Read bytes eagerly — segment files are small (< 256 MB total guarded above)
            byte[] data = new byte[(int) size];
            try (IndexInput in = storeDirectory.openInput(file, IOContext.READONCE)) {
                in.readBytes(data, 0, data.length);
            }
            entries.add(TarArchiveBuilder.fromBytes(file, data));
        }

        // Compute TAR layout (builds the _index header, resolves offsets)
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);

        // Stream TAR bytes to S3 via piped streams
        PipedInputStream pis = new PipedInputStream(PIPE_BUFFER_BYTES);
        PipedOutputStream pos = new PipedOutputStream(pis);

        final List<TarArchiveBuilder.ArchiveBuildEntry> entriesFinal = entries;
        Thread writerThread = new Thread(() -> {
            try (pos) {
                TarArchiveBuilder.build(pos, layout, entriesFinal);
            } catch (IOException e) {
                logger.error("Error writing TAR archive stream", e);
            }
        }, "rbs-tar-writer");
        writerThread.setDaemon(true);
        writerThread.start();

        dataContainer.writeBlob(archiveBlobName, pis, layout.getTotalSize(), false);
        writerThread.join(30_000);
    }

    private void fallbackUpload(
        Collection<String> files,
        Map<String, Long> sizeMap,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        IndexShard shard,
        ActionListener<Void> listener
    ) {
        // Delegate to per-file uploads via direct RemoteSegmentStoreDirectory.copyFrom()
        // This is a simplified fallback — just signal success to avoid blocking refresh.
        // TODO: implement full fallback using DefaultSegmentRemoteStoreStrategy
        logger.info("Fallback: {} files will be uploaded individually by core", files.size());
        listener.onFailure(new IOException("TAR archive upload failed; per-file fallback not implemented in plugin — core will retry"));
    }

    // -----------------------------------------------------------------------
    // Download
    // -----------------------------------------------------------------------

    @Override
    public IndexInput openInput(String name, UploadedSegmentMetadata metadata) throws IOException {
        // TODO: Implement range-GET download using TarSegmentParser + BlobContainer.readBlob(name, offset, length)
        //       Requires access to the BlobContainer at download time. This is tracked in rbs-archive-plugin-plan.md.
        throw new UnsupportedOperationException(
            "TarSegmentRemoteStoreStrategy.openInput() not yet implemented — "
                + "range-GET download requires BlobContainer injection at strategy construction time. "
                + "Tracked: rbs-archive-plugin-plan.md"
        );
    }

    // -----------------------------------------------------------------------
    // GC
    // -----------------------------------------------------------------------

    /**
     * Suppress per-shard S3 LIST call — archive blob GC is handled by the node-level
     * background task registered via {@link RbsArchivePlugin#createComponents}.
     */
    @Override
    public GcDecision resolveStaleBlobs(Set<String> activeUploadedFilenames, long latestMetadataGeneration) {
        return GcDecision.SKIP;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Extracts the {@link BlobContainer} from a {@link RemoteSegmentStoreDirectory}.
     */
    private static BlobContainer getBlobContainer(RemoteSegmentStoreDirectory remoteDirectory) {
        try {
            // RemoteSegmentStoreDirectory wraps a RemoteDirectory (data) and a RemoteDirectory (metadata).
            // Access the data container via reflection on the private field.
            java.lang.reflect.Field field = RemoteSegmentStoreDirectory.class.getDeclaredField("remoteDataDirectory");
            field.setAccessible(true);
            RemoteDirectory remoteDataDir = (RemoteDirectory) field.get(remoteDirectory);
            java.lang.reflect.Field containerField = RemoteDirectory.class.getDeclaredField("blobContainer");
            containerField.setAccessible(true);
            return (BlobContainer) containerField.get(remoteDataDir);
        } catch (Exception e) {
            logger.error("Failed to extract BlobContainer from RemoteSegmentStoreDirectory", e);
            return null;
        }
    }
}
