/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {
    private static final int MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS = 100;

    /**
     * Used to specify the default translog buffer interval for remote store backed indexes.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        IndexSettings.MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls minimum number of metadata files to keep in remote segment store.
     * {@code value < 1} will disable deletion of stale segment metadata files.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        -1,
        v -> {
            if (v == 0) {
                throw new IllegalArgumentException(
                    "Value 0 is not allowed for this setting as it would delete all the data from remote segment store"
                );
            }
        },
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls timeout value while uploading translog and checkpoint files to remote translog
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.transfer_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(30),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * This setting is used to set the remote store blob store path type strategy. This setting is effective only for
     * remote store enabled cluster.
     */
    @ExperimentalApi
    public static final Setting<RemoteStoreEnums.PathType> CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.index.path.type",
        RemoteStoreEnums.PathType.FIXED.toString(),
        RemoteStoreEnums.PathType::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * This setting is used to disable uploading translog.ckp file as metadata to translog.tlog. This setting is effective only for
     * repositories that supports metadata read and write with metadata and is applicable for only remote store enabled clusters.
     */
    @ExperimentalApi
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_METADATA = Setting.boolSetting(
        "cluster.remote_store.index.translog.translog_metadata",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED = Setting.boolSetting(
        "cluster.remote_store.server_side_encryption",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When true, if translog archive upload fails for a batch, fall back to per-shard upload for that batch.
     * When false, fail and rely on retry (no fallback).
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_FALLBACK_TO_PER_SHARD = Setting.boolSetting(
        "cluster.remote_store.translog.archive.fallback_to_per_shard_upload",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls whether segment archive upload is globally enabled at cluster level.
     * Index-level setting still required to enable per index.
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED = Setting.boolSetting(
        "cluster.remote_store.segment.archive.enabled",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls fallback behavior when segment archive upload fails.
     * When true, falls back to per-file upload on archive failure.
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE = Setting.boolSetting(
        "cluster.remote_store.segment.archive.fallback_to_per_file_upload",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls whether translog archives are written in TAR format (true) or ZIP format (false).
     * TAR uses a streaming single-pass build with a head-index, eliminating the double-build overhead
     * of the ZIP format and significantly improving indexing throughput.
     * Default: true (TAR). Set to false to fall back to ZIP for compatibility.
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_USE_TAR = Setting.boolSetting(
        "cluster.remote_store.translog.archive.use_tar",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls how often the translog archive retention GC runs (i.e. how frequently stale ZIPs are deleted).
     * This is the timer repeat interval — separate from the data retention age.
     * Default: 1 minute. Minimum: 1 minute.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.translog.archive.gc_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(1),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Minimum interval between consecutive segment metadata GC runs per shard.
     * Segment GC is triggered after every refresh, but with high refresh rates this leads to excessive S3 LIST calls.
     * Setting a minimum interval rate-limits how often the GC actually executes while keeping the algorithm correct —
     * the full S3 LIST + stale file deletion still runs each time, just not more often than this interval.
     * Set to {@code 0s} to restore the original behaviour (GC after every refresh).
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_SEGMENT_METADATA_GC_MIN_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.segment.metadata.gc.min_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(0),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * This setting is used to set the remote store blob store path hash algorithm strategy. This setting is effective only for
     * remote store enabled cluster. This setting will come to effect if the {@link #CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING}
     * is either {@code HASHED_PREFIX} or {@code HASHED_INFIX}.
     */
    @ExperimentalApi
    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING = new Setting<>(
        "cluster.remote_store.index.path.hash_algorithm",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls the maximum referenced remote translog files. If breached the shard will be flushed.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_MAX_TRANSLOG_READERS = Setting.intSetting(
        "cluster.remote_store.translog.max_readers",
        1000,
        -1,
        v -> {
            if (v != -1 && v < MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS) {
                throw new IllegalArgumentException("Cannot set value lower than " + MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS);
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls timeout value while uploading segment files to remote segment store
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.segment.transfer_timeout",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueMinutes(10),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls pinned timestamp feature enablement
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED = Setting.boolSetting(
        "cluster.remote_store.pinned_timestamps.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Controls pinned timestamp scheduler interval
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamps.scheduler_interval",
        TimeValue.timeValueMinutes(3),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * Controls allowed timestamp values to be pinned from past
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamps.lookback_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /**
     * Controls the fixed prefix for the translog path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.translog.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    /**
     * Controls the fixed prefix for the segments path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.segments.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;
    private volatile TimeValue clusterRemoteTranslogTransferTimeout;
    private volatile TimeValue clusterRemoteSegmentTransferTimeout;
    private volatile RemoteStoreEnums.PathType pathType;
    private volatile RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm;
    private volatile int maxRemoteTranslogReaders;
    private volatile boolean isTranslogMetadataEnabled;
    private volatile boolean isClusterServerSideEncryptionRepoEnabled;
    private volatile boolean translogArchiveFallbackToPerShard;
    private volatile boolean clusterRemoteStoreSegmentArchiveEnabled;
    private volatile boolean clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    private volatile TimeValue translogArchiveGcInterval;
    private volatile boolean translogArchiveUseTar;
    private volatile TimeValue segmentMetadataGcMinInterval;
    private static volatile boolean isPinnedTimestampsEnabled;
    private static volatile TimeValue pinnedTimestampsSchedulerInterval;
    private static volatile TimeValue pinnedTimestampsLookbackInterval;
    private final String translogPathFixedPrefix;
    private final String segmentsPathFixedPrefix;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );

        clusterRemoteTranslogTransferTimeout = CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteTranslogTransferTimeout
        );

        pathType = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING, this::setPathType);

        isTranslogMetadataEnabled = clusterSettings.get(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA, this::setTranslogMetadataEnabled);

        pathHashAlgorithm = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING, this::setPathHashAlgorithm);

        maxRemoteTranslogReaders = CLUSTER_REMOTE_MAX_TRANSLOG_READERS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_MAX_TRANSLOG_READERS, this::setMaxRemoteTranslogReaders);

        clusterRemoteSegmentTransferTimeout = CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteSegmentTransferTimeout
        );

        pinnedTimestampsSchedulerInterval = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL.get(settings);
        pinnedTimestampsLookbackInterval = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL.get(settings);
        isPinnedTimestampsEnabled = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.get(settings);

        translogPathFixedPrefix = CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(settings);
        segmentsPathFixedPrefix = CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(settings);

        translogArchiveFallbackToPerShard = CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_FALLBACK_TO_PER_SHARD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_FALLBACK_TO_PER_SHARD,
            this::setTranslogArchiveFallbackToPerShard
        );

        clusterRemoteStoreSegmentArchiveEnabled = CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED,
            this::setClusterRemoteStoreSegmentArchiveEnabled
        );

        clusterRemoteStoreSegmentArchiveFallbackToPerFile = CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE,
            this::setClusterRemoteStoreSegmentArchiveFallbackToPerFile
        );

        translogArchiveGcInterval = CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_GC_INTERVAL, this::setTranslogArchiveGcInterval);

        translogArchiveUseTar = CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_USE_TAR.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_USE_TAR, this::setTranslogArchiveUseTar);

        segmentMetadataGcMinInterval = CLUSTER_REMOTE_STORE_SEGMENT_METADATA_GC_MIN_INTERVAL.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_STORE_SEGMENT_METADATA_GC_MIN_INTERVAL,
            this::setSegmentMetadataGcMinInterval
        );
    }

    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    private void setMinRemoteSegmentMetadataFiles(int minRemoteSegmentMetadataFiles) {
        this.minRemoteSegmentMetadataFiles = minRemoteSegmentMetadataFiles;
    }

    public int getMinRemoteSegmentMetadataFiles() {
        return this.minRemoteSegmentMetadataFiles;
    }

    public TimeValue getClusterRemoteTranslogTransferTimeout() {
        return clusterRemoteTranslogTransferTimeout;
    }

    public TimeValue getClusterRemoteSegmentTransferTimeout() {
        return clusterRemoteSegmentTransferTimeout;
    }

    private void setClusterRemoteTranslogTransferTimeout(TimeValue clusterRemoteTranslogTransferTimeout) {
        this.clusterRemoteTranslogTransferTimeout = clusterRemoteTranslogTransferTimeout;
    }

    private void setClusterRemoteSegmentTransferTimeout(TimeValue clusterRemoteSegmentTransferTimeout) {
        this.clusterRemoteSegmentTransferTimeout = clusterRemoteSegmentTransferTimeout;
    }

    @ExperimentalApi
    public RemoteStoreEnums.PathType getPathType() {
        return pathType;
    }

    @ExperimentalApi
    public RemoteStoreEnums.PathHashAlgorithm getPathHashAlgorithm() {
        return pathHashAlgorithm;
    }

    private void setPathType(RemoteStoreEnums.PathType pathType) {
        this.pathType = pathType;
    }

    private void setTranslogMetadataEnabled(boolean isTranslogMetadataEnabled) {
        this.isTranslogMetadataEnabled = isTranslogMetadataEnabled;
    }

    public boolean isTranslogMetadataEnabled() {
        return isTranslogMetadataEnabled;
    }

    private void setPathHashAlgorithm(RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm) {
        this.pathHashAlgorithm = pathHashAlgorithm;
    }

    public int getMaxRemoteTranslogReaders() {
        return maxRemoteTranslogReaders;
    }

    private void setMaxRemoteTranslogReaders(int maxRemoteTranslogReaders) {
        this.maxRemoteTranslogReaders = maxRemoteTranslogReaders;
    }

    public static TimeValue getPinnedTimestampsSchedulerInterval() {
        return pinnedTimestampsSchedulerInterval;
    }

    public static TimeValue getPinnedTimestampsLookbackInterval() {
        return pinnedTimestampsLookbackInterval;
    }

    // Visible for testing
    public static void setPinnedTimestampsLookbackInterval(TimeValue pinnedTimestampsLookbackInterval) {
        RemoteStoreSettings.pinnedTimestampsLookbackInterval = pinnedTimestampsLookbackInterval;
    }

    public static boolean isPinnedTimestampsEnabled() {
        return isPinnedTimestampsEnabled;
    }

    public String getTranslogPathFixedPrefix() {
        return translogPathFixedPrefix;
    }

    public String getSegmentsPathFixedPrefix() {
        return segmentsPathFixedPrefix;
    }

    public boolean isTranslogArchiveFallbackToPerShard() {
        return translogArchiveFallbackToPerShard;
    }

    public boolean getTranslogArchiveFallbackToPerShard() {
        return translogArchiveFallbackToPerShard;
    }

    public boolean isClusterRemoteStoreSegmentArchiveEnabled() {
        return clusterRemoteStoreSegmentArchiveEnabled;
    }

    public boolean isClusterRemoteStoreSegmentArchiveFallbackToPerFile() {
        return clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    }

    /**
     * Returns the translog archive GC interval — how often the retention GC timer fires.
     * Distinct from the data retention age ({@code index.remote_store.translog.archive_retention}).
     */
    public TimeValue getTranslogArchiveGcInterval() {
        return translogArchiveGcInterval != null ? translogArchiveGcInterval : TimeValue.timeValueMinutes(1);
    }

    private void setTranslogArchiveGcInterval(TimeValue translogArchiveGcInterval) {
        this.translogArchiveGcInterval = translogArchiveGcInterval;
    }

    /**
     * Returns true if translog archives should be written in TAR format (single-pass streaming),
     * false to use the legacy ZIP format.
     */
    public boolean isTranslogArchiveUseTar() {
        return translogArchiveUseTar;
    }

    private void setTranslogArchiveUseTar(boolean translogArchiveUseTar) {
        this.translogArchiveUseTar = translogArchiveUseTar;
    }

    public TimeValue getSegmentMetadataGcMinInterval() {
        return segmentMetadataGcMinInterval;
    }

    private void setSegmentMetadataGcMinInterval(TimeValue segmentMetadataGcMinInterval) {
        this.segmentMetadataGcMinInterval = segmentMetadataGcMinInterval;
    }

    private void setTranslogArchiveFallbackToPerShard(boolean translogArchiveFallbackToPerShard) {
        this.translogArchiveFallbackToPerShard = translogArchiveFallbackToPerShard;
    }

    private void setClusterRemoteStoreSegmentArchiveEnabled(boolean clusterRemoteStoreSegmentArchiveEnabled) {
        this.clusterRemoteStoreSegmentArchiveEnabled = clusterRemoteStoreSegmentArchiveEnabled;
    }

    private void setClusterRemoteStoreSegmentArchiveFallbackToPerFile(boolean clusterRemoteStoreSegmentArchiveFallbackToPerFile) {
        this.clusterRemoteStoreSegmentArchiveFallbackToPerFile = clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    }

}
