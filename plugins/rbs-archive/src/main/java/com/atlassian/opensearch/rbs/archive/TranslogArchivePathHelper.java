/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Optional;

/**
 * Helpers for translog archive path layout and blob name generation.
 *
 * <p><b>Hierarchical path layout</b> (per-node, all indices combined):
 * <pre>
 *   {base}/txlog/{yyyyMMdd}/{HHmm}/{ss}.{SSS}.{nodeIdShort}.tar
 * </pre>
 * Example: {@code txlog/20260501/2230/45.123.a3f7b2c1.tar}
 *
 * <ul>
 *   <li>{@code txlog/} — fixed top-level prefix</li>
 *   <li>{@code yyyyMMdd/} — UTC day directory</li>
 *   <li>{@code HHmm/} — UTC minute directory (~120 per 2h window at steady state)</li>
 *   <li>{@code ss.SSS.{nodeIdShort}.tar} — blob: seconds, milliseconds, short node id</li>
 * </ul>
 *
 * <p><b>GC</b>: enumerate minute-dirs under day, compare path timestamp to cutoff,
 * delete entire expired dirs. At steady state (2h retention) there are only ~120 minute-dirs
 * = 1 LIST page for GC.
 *
 * <p><b>Restore</b>: LIST minute-dirs from {@code lastSegmentTimestamp - margin}, then list
 * blobs within those dirs.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchivePathHelper {

    /** Top-level directory prefix for the new hierarchical path. */
    public static final String TXLOG_PREFIX = "txlog";

    /** Formatter for the day-level directory: {@code yyyyMMdd}. */
    private static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    /** Formatter for the minute-level directory: {@code HHmm}. */
    private static final DateTimeFormatter MINUTE_FORMAT = DateTimeFormatter.ofPattern("HHmm", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    /** Formatter for the blob name time prefix: {@code ss.SSS} (seconds + ms within the minute). */
    private static final DateTimeFormatter BLOB_NAME_TIME_FORMAT = DateTimeFormatter.ofPattern("ss.SSS", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    /** Formatter for parsing concatenated day+minute+blob timestamp: {@code yyyyMMddHHmmssSSS}. */
    private static final DateTimeFormatter FULL_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    /** Length of the short node ID appended to blob names. */
    private static final int NODE_ID_SHORT_LEN = 8;

    private TranslogArchivePathHelper() {}

    // ── New hierarchical path helpers ─────────────────────────────────────────

    /**
     * Returns the day-level directory name: {@code yyyyMMdd}.
     */
    public static String dayDir(Instant instant) {
        return DAY_FORMAT.format(instant);
    }

    /**
     * Returns the minute-level directory name: {@code HHmm}.
     */
    public static String minuteDir(Instant instant) {
        return MINUTE_FORMAT.format(instant);
    }

    /**
     * Returns the blob name for a new TAR archive: {@code ss.SSS.{nodeIdShort}.tar}.
     * Example: {@code 45.123.a3f7b2c1.tar}
     *
     * @param instant  upload time (UTC)
     * @param nodeId   raw node ID (first alphanumeric chars are used as short ID)
     */
    public static String tarBlobName(Instant instant, String nodeId) {
        return BLOB_NAME_TIME_FORMAT.format(instant) + "." + shortNodeId(nodeId) + ".tar";
    }

    /**
     * Returns the blob directory {@link BlobPath} for a new TAR upload:
     * {@code {base}/txlog/{yyyyMMdd}/{HHmm}/}.
     *
     * @param base    repository base path
     * @param instant upload time (UTC)
     */
    public static BlobPath tarBlobDir(BlobPath base, Instant instant) {
        return base.add(TXLOG_PREFIX).add(dayDir(instant)).add(minuteDir(instant));
    }

    /**
     * Returns the day-level path for listing minute-dirs:
     * {@code {base}/txlog/{yyyyMMdd}/}.
     * Use {@code listFolders} on this to enumerate all minute-dirs for the given day.
     *
     * @param base    repository base path
     * @param instant the reference instant (day is extracted)
     */
    public static BlobPath txlogDayPath(BlobPath base, Instant instant) {
        return base.add(TXLOG_PREFIX).add(dayDir(instant));
    }

    /**
     * Returns the txlog root path: {@code {base}/txlog/}.
     */
    public static BlobPath txlogRootPath(BlobPath base) {
        return base.add(TXLOG_PREFIX);
    }

    /**
     * Parses the upload {@link Instant} from the hierarchical path components.
     * Components: day dir ({@code yyyyMMdd}), minute dir ({@code HHmm}),
     * blob name ({@code ss.SSS.nodeIdShort.tar}).
     *
     * <p>Returns {@link Optional#empty()} if any component cannot be parsed.
     *
     * @param dayDirStr    e.g. {@code "20260501"}
     * @param minuteDirStr e.g. {@code "2230"}
     * @param blobName     e.g. {@code "45.123.a3f7b2c1.tar"}
     */
    public static Optional<Instant> parseTarBlobTimestamp(String dayDirStr, String minuteDirStr, String blobName) {
        if (dayDirStr == null || minuteDirStr == null || blobName == null) {
            return Optional.empty();
        }
        if (!blobName.endsWith(".tar")) {
            return Optional.empty();
        }
        // blob name format: ss.SSS.nodeIdShort.tar  → split on '.'
        String[] parts = blobName.split("\\.", -1);
        // Expected: ["ss", "SSS", "nodeIdShort", "tar"] = 4 parts
        if (parts.length < 4) {
            return Optional.empty();
        }
        // Parse timestamp from component parts: yyyyMMdd (day) + HHmm (minute) + ss.SSS (blob)
        // Combine into a single yyyyMMddHHmmssSSS string and parse with a prebuilt formatter.
        // Format: dayDir=yyyyMMdd, minuteDir=HHmm, parts[0]=ss, parts[1]=SSS
        String tsStr = dayDirStr + minuteDirStr + parts[0] + parts[1]; // 17 chars: yyyyMMddHHmmssSSS
        if (tsStr.length() != 17) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.from(FULL_TIMESTAMP_FORMAT.parse(tsStr)));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns true if the blob name matches the new TAR format:
     * {@code ss.SSS.{nodeIdShort}.tar} (exactly 4 dot-separated segments).
     */
    public static boolean isNewTarBlob(String blobName) {
        if (blobName == null || !blobName.endsWith(".tar")) {
            return false;
        }
        String[] parts = blobName.split("\\.", -1);
        // ss.SSS.nodeIdShort.tar → ["ss", "SSS", nodeIdShort, "tar"] = 4 parts
        return parts.length == 4 && parts[0].length() == 2 && parts[1].length() == 3;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Returns a short identifier derived from the node ID: first {@value #NODE_ID_SHORT_LEN}
     * alphanumeric characters of the node ID. Falls back to {@code "00000000"} if the node ID
     * is null or has fewer than {@value #NODE_ID_SHORT_LEN} alphanumeric chars.
     */
    static String shortNodeId(String nodeId) {
        if (nodeId == null) {
            return "00000000";
        }
        StringBuilder sb = new StringBuilder(NODE_ID_SHORT_LEN);
        for (int i = 0; i < nodeId.length() && sb.length() < NODE_ID_SHORT_LEN; i++) {
            char c = nodeId.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                sb.append(c);
            }
        }
        while (sb.length() < NODE_ID_SHORT_LEN) {
            sb.append('0');
        }
        return sb.toString();
    }
}
