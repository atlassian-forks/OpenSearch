/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Bundles every file from one refresh into a single remote blob instead of uploading each file separately:
 * one PUT per refresh instead of one PUT per file. Overrides the three extension points added to core for
 * this purpose: the batch-upload entry point, the read path, and GC.
 * <p>
 * Everything else, including the metadata file format, locking, and listing, is inherited unchanged. Bundle
 * blob GC ({@link #deleteRemoteFiles}) is recomputed fresh from the active/stale sets each run, the same way
 * core's own per-file GC already works, so it needs no extra state and survives restarts correctly.
 */
public class BundlingRemoteSegmentStoreDirectory extends RemoteSegmentStoreDirectory {

    private static final Logger logger = LogManager.getLogger(BundlingRemoteSegmentStoreDirectory.class);
    static final String BUNDLE_BLOB_PREFIX = "segment_bundle_";

    public BundlingRemoteSegmentStoreDirectory(
        RemoteDirectory remoteDataDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        Map<String, String> pendingDownloadMergedSegments
    ) throws IOException {
        super(remoteDataDirectory, remoteMetadataDirectory, mdLockManager, threadPool, shardId, pendingDownloadMergedSegments);
    }

    @Override
    public void copyFrom(
        Directory from,
        Collection<String> files,
        IOContext context,
        Function<String, ActionListener<Void>> perFileListenerFactory,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) {
        if (files.isEmpty()) {
            return;
        }
        // Build a per-file listener map up front so every file gets exactly one callback, matching the contract
        // RemoteStoreUploaderService relies on (stats tracking, GroupedActionListener countdown) regardless of
        // whether the batch bundles cleanly or falls back.
        Map<String, ActionListener<Void>> listeners = new LinkedHashMap<>();
        for (String src : files) {
            listeners.put(src, perFileListenerFactory.apply(src));
        }

        try {
            List<SegmentBundleFormat.NamedContent> contents = new ArrayList<>(files.size());
            Map<String, String> checksums = new LinkedHashMap<>();
            for (String src : files) {
                byte[] bytes = readFileBytes(from, src);
                checksums.put(src, checksumOfLocalFile(from, src));
                contents.add(new SegmentBundleFormat.NamedContent() {
                    @Override
                    public String getName() {
                        return src;
                    }

                    @Override
                    public byte[] getBytes() {
                        return bytes;
                    }
                });
            }

            byte[] bundleBytes = SegmentBundleFormat.build(contents);
            String bundleBlobName = BUNDLE_BLOB_PREFIX + UUIDs.base64UUID();
            BlobContainer blobContainer = remoteDataDirectory.getBlobContainer();
            blobContainer.writeBlob(bundleBlobName, new java.io.ByteArrayInputStream(bundleBytes), bundleBytes.length, true);

            SegmentBundleFormat.HeaderProbe probe = SegmentBundleFormat.probeHeader(bundleBytes);
            for (String src : files) {
                SegmentBundleFormat.Entry entry = probe.entries.get(src);
                String pointer = SegmentBundleFormat.encodePointer(bundleBlobName, entry.offset, entry.length);
                postUpload(from, src, pointer, checksums.get(src));
                listeners.get(src).onResponse(null);
            }
            logger.debug("Bundled {} segment files into one blob {} ({} bytes)", files.size(), bundleBlobName, bundleBytes.length);
        } catch (Exception e) {
            logger.warn("Exception while bundling segment files, failing this batch", e);
            for (ActionListener<Void> listener : listeners.values()) {
                listener.onFailure(e);
            }
        }
    }

    private static byte[] readFileBytes(Directory from, String name) throws IOException {
        try (IndexInput input = from.openInput(name, IOContext.READONCE)) {
            byte[] bytes = new byte[(int) input.length()];
            input.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    // Duplicated from RemoteSegmentStoreDirectory's private getChecksumOfLocalFile rather than requiring core
    // to widen that method's visibility for one plugin-side caller.
    private static String checksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.READONCE)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename == null) {
            throw new NoSuchFileException(name);
        }
        if (SegmentBundleFormat.isBundlePointer(remoteFilename)) {
            String blobName = SegmentBundleFormat.pointerBlobName(remoteFilename);
            long offset = SegmentBundleFormat.pointerOffset(remoteFilename);
            long length = SegmentBundleFormat.pointerLength(remoteFilename);
            try (InputStream in = remoteDataDirectory.getBlobContainer().readBlob(blobName, offset, length)) {
                return new ByteArrayIndexInput(name, in.readAllBytes());
            }
        }
        return super.openInput(name, context);
    }

    // No deleteFile() override: the default implementation calls remoteDataDirectory.deleteFile(remoteFilename),
    // which is built on deleteBlobsIgnoringIfNotExists — for a bundle pointer that's a literal blob name that
    // never existed, so it silently no-ops. The map entry still gets removed, which is correct: the shared
    // bundle blob is never touched here regardless, so no other file's data is at risk.

    @Override
    protected void deleteRemoteFiles(List<String> staleUploadedFilenames, Set<String> activeUploadedFilenames) throws IOException {
        List<String> toDelete = new ArrayList<>();
        Set<String> staleBundleBlobNames = new LinkedHashSet<>();

        for (String uploadedFilename : staleUploadedFilenames) {
            if (SegmentBundleFormat.isBundlePointer(uploadedFilename)) {
                staleBundleBlobNames.add(SegmentBundleFormat.pointerBlobName(uploadedFilename));
            } else {
                toDelete.add(uploadedFilename);
            }
        }

        for (String bundleBlobName : staleBundleBlobNames) {
            boolean stillReferenced = activeUploadedFilenames.stream()
                .filter(SegmentBundleFormat::isBundlePointer)
                .anyMatch(active -> SegmentBundleFormat.pointerBlobName(active).equals(bundleBlobName));
            if (stillReferenced == false) {
                // Every file that was ever bundled into this blob is now stale — safe to delete the whole
                // blob. Recomputed from the active/stale sets this GC run already built, no counters kept.
                toDelete.add(bundleBlobName);
            } else {
                logger.debug("Bundle {} still has at least one active file, keeping it this GC run", bundleBlobName);
            }
        }

        remoteDataDirectory.deleteFiles(toDelete);
    }
}
