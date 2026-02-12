/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * Helpers for translog archive path: hash(type, indexUUID), hash(nodeId), and blob name.
 * Path layout: translog/data/{hashTypeIndex}/{hashNodeId}/{yyyymmddhhmmssMS}.zip
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchivePathHelper {

    private static final String FILE_TYPE_TRANSLOG_ZIP = "translog_zip";
    private static final DateTimeFormatter BLOB_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    private TranslogArchivePathHelper() {}

    /**
     * First path component: hash(fileType, indexUUID). List this dir to get all node dirs for the index.
     */
    public static String hashTypeIndex(String indexUUID, RemoteStoreEnums.PathHashAlgorithm algorithm) {
        return RemoteStoreUtils.hashStringForPath(FILE_TYPE_TRANSLOG_ZIP + "|" + indexUUID, algorithm);
    }

    /**
     * Second path component: hash(nodeId).
     */
    public static String hashNodeId(String nodeId, RemoteStoreEnums.PathHashAlgorithm algorithm) {
        return RemoteStoreUtils.hashStringForPath(nodeId, algorithm);
    }

    /**
     * Blob name: yyyyMMddHHmmssSSS.zip (timestamp with ms; single value for performance hint and collision avoidance).
     */
    public static String blobNameFromCurrentTime() {
        return BLOB_TIMESTAMP_FORMAT.format(Instant.now()) + ".zip";
    }

    /**
     * Parse blob name to timestamp for retention hint. Returns empty if name is not yyyyMMddHHmmssSSS.zip.
     */
    public static java.util.Optional<Instant> parseBlobNameTimestamp(String blobName) {
        if (blobName == null || !blobName.endsWith(".zip")) {
            return java.util.Optional.empty();
        }
        String base = blobName.substring(0, blobName.length() - 4);
        if (base.length() != 17) {
            return java.util.Optional.empty();
        }
        try {
            return java.util.Optional.of(Instant.from(BLOB_TIMESTAMP_FORMAT.parse(base)));
        } catch (DateTimeParseException e) {
            return java.util.Optional.empty();
        }
    }
}
