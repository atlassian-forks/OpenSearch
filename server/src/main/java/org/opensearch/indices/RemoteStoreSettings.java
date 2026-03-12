/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.remote.RemoteStoreEnums;

import java.nio.ByteBuffer;
import java.util.Base64;

public class RemoteStoreSettings {

    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        TimeValue.timeValueMillis(200),
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        1,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.transfer_timeout",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<TimeValue> CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.segment.transfer_timeout",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<RemoteStoreEnums.PathType> CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.path.type",
        RemoteStoreEnums.PathType.FIXED.name(),
        RemoteStoreEnums.PathType::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING = new Setting<>(
        "cluster.remote_store.path.hash_algorithm",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.name(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_METADATA = Setting.boolSetting(
        "cluster.remote_store.translog.metadata.enabled",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Integer> CLUSTER_REMOTE_MAX_TRANSLOG_READERS = Setting.intSetting(
        "cluster.remote_store.translog.max_readers",
        10,
        1,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED = Setting.boolSetting(
        "cluster.remote_store.server_side_encryption",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamp.scheduler_interval",
        TimeValue.timeValueMinutes(1),
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamp.lookback_interval",
        TimeValue.timeValueHours(24),
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED = Setting.boolSetting(
        "cluster.remote_store.pinned_timestamp.enabled",
        false,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<String> CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.translog.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    public static final Setting<String> CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.segments.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_ARCHIVE_FALLBACK_TO_PER_SHARD = Setting.boolSetting(
        "cluster.remote_store.translog.archive.fallback_to_per_shard_upload",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED = Setting.boolSetting(
        "cluster.remote_store.segment.archive.enabled",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE = Setting.boolSetting(
        "cluster.remote_store.segment.archive.fallback_to_per_file_upload",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;
    private volatile TimeValue clusterRemoteTranslogTransferTimeout;
    private volatile TimeValue clusterRemoteSegmentTransferTimeout;
    private volatile RemoteStoreEnums.PathType pathType;
    private volatile RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm;
    private volatile int maxRemoteTranslogReaders;
    private volatile boolean isTranslogMetadataEnabled;
    private static volatile boolean isPinnedTimestampsEnabled;
    private static volatile TimeValue pinnedTimestampsSchedulerInterval;
    private static volatile TimeValue pinnedTimestampsLookbackInterval;
    private final String translogPathFixedPrefix;
    private final String segmentsPathFixedPrefix;
    private volatile boolean translogArchiveFallbackToPerShard;
    private volatile boolean clusterRemoteStoreSegmentArchiveEnabled;
    private volatile boolean clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    private volatile boolean isClusterServerSideEncryptionRepoEnabled;

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

        isClusterServerSideEncryptionRepoEnabled = CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED, this::setClusterServerSideEncryptionEnabled);

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
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
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

    public RemoteStoreEnums.PathType getPathType() {
        return pathType;
    }

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

    private void setClusterServerSideEncryptionEnabled(boolean isClusterServerSideEncryptionRepoEnabled) {
        this.isClusterServerSideEncryptionRepoEnabled = isClusterServerSideEncryptionRepoEnabled;
    }

    public boolean isClusterServerSideEncryptionRepoEnabled() {
        return isClusterServerSideEncryptionRepoEnabled;
    }

    public static boolean isPinnedTimestampsEnabled() {
        return isPinnedTimestampsEnabled;
    }

    public static TimeValue getPinnedTimestampsSchedulerInterval() {
        return pinnedTimestampsSchedulerInterval;
    }

    public static TimeValue getPinnedTimestampsLookbackInterval() {
        return pinnedTimestampsLookbackInterval;
    }

    public String getTranslogPathFixedPrefix() {
        return translogPathFixedPrefix;
    }

    public String getSegmentsPathFixedPrefix() {
        return segmentsPathFixedPrefix;
    }

    private void setTranslogArchiveFallbackToPerShard(boolean translogArchiveFallbackToPerShard) {
        this.translogArchiveFallbackToPerShard = translogArchiveFallbackToPerShard;
    }

    public boolean isTranslogArchiveFallbackToPerShard() {
        return translogArchiveFallbackToPerShard;
    }

    private void setClusterRemoteStoreSegmentArchiveEnabled(boolean clusterRemoteStoreSegmentArchiveEnabled) {
        this.clusterRemoteStoreSegmentArchiveEnabled = clusterRemoteStoreSegmentArchiveEnabled;
    }

    public boolean isClusterRemoteStoreSegmentArchiveEnabled() {
        return clusterRemoteStoreSegmentArchiveEnabled;
    }

    private void setClusterRemoteStoreSegmentArchiveFallbackToPerFile(boolean clusterRemoteStoreSegmentArchiveFallbackToPerFile) {
        this.clusterRemoteStoreSegmentArchiveFallbackToPerFile = clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    }

    public boolean isClusterRemoteStoreSegmentArchiveFallbackToPerFile() {
        return clusterRemoteStoreSegmentArchiveFallbackToPerFile;
    }

    public static String longToUrlBase64(long value) {
        byte[] hashBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        String base64Str = Base64.getUrlEncoder().encodeToString(hashBytes);
        return base64Str.substring(0, base64Str.length() - 1);
    }
}
