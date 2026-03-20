/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper for cleaning up stale segment archive ZIP blobs that are no longer referenced
 * by any active metadata file. An archive blob is stale if its name does not appear
 * as the {@code uploadedFilename} in any active metadata entry.
 * <p>
 * Archive blob names follow the pattern: {@code segment_archive_<timestamp>_<uuid>.zip}
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class SegmentArchiveRetentionHelper {

    private static final Logger logger = LogManager.getLogger(SegmentArchiveRetentionHelper.class);

    /** Prefix for segment archive blobs. */
    public static final String SEGMENT_ARCHIVE_PREFIX = "segment_archive_";

    private SegmentArchiveRetentionHelper() {}

    /**
     * Identifies stale segment archive blobs in the data directory.
     * An archive blob is stale if it is not referenced by any active segment metadata.
     *
     * @param allBlobsInDataDir all blob names in the remote data directory (from listAll/listBlobs)
     * @param activeUploadedFilenames set of uploaded filenames currently referenced by active metadata files
     * @return list of archive blob names that are stale and can be deleted
     */
    public static List<String> findStaleArchiveBlobs(Set<String> allBlobsInDataDir, Set<String> activeUploadedFilenames) {
        if (allBlobsInDataDir == null || allBlobsInDataDir.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> staleArchives = new ArrayList<>();
        for (String blobName : allBlobsInDataDir) {
            if (isArchiveBlob(blobName) && !activeUploadedFilenames.contains(blobName)) {
                staleArchives.add(blobName);
            }
        }
        return staleArchives;
    }

    /**
     * Collects the set of uploaded filenames that are actively referenced by any of the given
     * metadata maps. Each metadata map entry is filename → "original::uploaded::checksum::length::version".
     *
     * @param metadataMaps list of metadata maps (from reading each metadata file)
     * @return set of uploaded filenames currently in use
     */
    public static Set<String> collectActiveUploadedFilenames(List<Map<String, String>> metadataMaps) {
        return metadataMaps.stream()
            .flatMap(m -> m.values().stream())
            .map(SegmentArchiveRetentionHelper::extractUploadedFilename)
            .collect(Collectors.toSet());
    }

    /**
     * Extracts the uploaded filename from a metadata value string.
     * Format: "original::uploaded::checksum::length::version"
     */
    static String extractUploadedFilename(String metadataValue) {
        String[] parts = metadataValue.split("::");
        if (parts.length >= 2) {
            return parts[1];
        }
        return metadataValue;
    }

    /**
     * Returns true if the blob name matches the segment archive naming convention.
     */
    public static boolean isArchiveBlob(String blobName) {
        return blobName != null && blobName.startsWith(SEGMENT_ARCHIVE_PREFIX) && blobName.endsWith(".zip");
    }
}
