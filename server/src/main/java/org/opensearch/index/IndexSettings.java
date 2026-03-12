/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.merge.MergeOnFlushMergePolicy;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.translog.Translog;
import org.opensearch.ingest.IngestService;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * This class encapsulates all index level settings and provides a convenient way to access them.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexSettings {

    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>(
        "index.shard.check_on_startup",
        "false",
        (s) -> {
            switch (s) {
                case "false":
                case "true":
                case "checksum":
                    return s;
                default:
                    throw new IllegalArgumentException("unknown value for [index.shard.check_on_startup] must be one of [true, false, checksum]");
            }
        },
        Property.IndexScope,
        Property.ReadOnly
    );

    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "index.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.flush_threshold_size",
        new ByteSizeValue(512, ByteSizeValue.ByteUnit.MB),
        new ByteSizeValue(100, ByteSizeValue.ByteUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeValue.ByteUnit.BYTES),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the frequency of the translog fsync operation.
     */
    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.sync_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(100),
        Property.IndexScope
    );

    /**
     * Controls the durability of the translog operations.
     */
    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING = Setting.enumSetting(
        Translog.Durability.class,
        "index.translog.durability",
        Translog.Durability.REQUEST,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum size of a single translog generation file.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.generation_threshold_size",
        new ByteSizeValue(64, ByteSizeValue.ByteUnit.MB),
        new ByteSizeValue(1, ByteSizeValue.ByteUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeValue.ByteUnit.BYTES),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls how long translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted.
     */
    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING = Setting.timeSetting(
        "index.translog.retention.age",
        TimeValue.timeValueHours(12),
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls how many translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.retention.size",
        new ByteSizeValue(512, ByteSizeValue.ByteUnit.MB),
        new ByteSizeValue(-1, ByteSizeValue.ByteUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeValue.ByteUnit.BYTES),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING = Setting.intSetting(
        "index.translog.retention.total_files",
        100,
        0,
        Setting.Property.IndexScope
    );

    public static final Setting<TimeValue> INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING = Setting.timeSetting(
        "index.soft_deletes.retention_lease.period",
        TimeValue.timeValueHours(12),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting(
        "index.max_refresh_listeners",
        1000,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting(
        "index.max_slices_per_scroll",
        1024,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> MAX_SLICES_PER_PIT = Setting.intSetting(
        "index.max_slices_per_pit",
        1024,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> MAX_REGEX_LENGTH_SETTING = Setting.intSetting(
        "index.max_regex_length",
        1000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> DEFAULT_PIPELINE = new Setting<>(
        "index.default_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> FINAL_PIPELINE = new Setting<>(
        "index.final_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING = Setting.boolSetting(
        "index.warmer.enabled",
        true,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING = Setting.simpleString(
        "index.remote_store.translog.repository",
        Property.IndexScope,
        Property.Final
    );

    public static final Setting<TimeValue> INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "index.remote_store.translog.buffer_interval",
        TimeValue.timeValueMillis(200),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING = Setting.intSetting(
        "index.remote_store.translog.keep_extra_gen",
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING = Setting.boolSetting(
        "index.remote_store.translog.archive_upload_enabled",
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_REMOTE_STORE_SEGMENT_ARCHIVE_UPLOAD_ENABLED_SETTING = Setting.boolSetting(
        "index.remote_store.segment.archive_upload_enabled",
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    private final Index index;
    private final Settings nodeSettings;
    private final Settings settings;
    private final Logger logger;
    private final IndexScopedSettings scopedSettings;
    private final IndexMetadata indexMetadata;

    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue translogFlushThresholdSize;
    private volatile ByteSizeValue generationThresholdSize;
    private volatile TimeValue translogRetentionAge;
    private volatile ByteSizeValue translogRetentionSize;
    private volatile long softDeleteRetentionOperations;
    private volatile long retentionLeaseMillis;
    private volatile boolean isRemoteStoreEnabled;
    private volatile String remoteStoreTranslogRepository;
    private volatile TimeValue remoteTranslogUploadBufferInterval;
    private int remoteTranslogKeepExtraGen;
    private volatile boolean translogArchiveUploadEnabled;
    private volatile boolean segmentArchiveUploadEnabled;
    private volatile boolean isTranslogMetadataEnabled;
    private final RemoteStorePathStrategy remoteStorePathStrategy;

    public IndexSettings(IndexMetadata indexMetadata, Settings nodeSettings) {
        this(indexMetadata, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    public IndexSettings(IndexMetadata indexMetadata, Settings nodeSettings, IndexScopedSettings scopedSettings) {
        this.indexMetadata = indexMetadata;
        this.index = indexMetadata.getIndex();
        this.nodeSettings = nodeSettings;
        this.settings = indexMetadata.getSettings();
        this.logger = Loggers.getLogger(getClass(), index);
        this.scopedSettings = scopedSettings.copy(settings, indexMetadata);

        this.refreshInterval = INDEX_REFRESH_INTERVAL_SETTING.get(settings);
        this.translogFlushThresholdSize = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.get(settings);
        this.generationThresholdSize = INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.get(settings);
        this.translogRetentionAge = INDEX_TRANSLOG_RETENTION_AGE_SETTING.get(settings);
        this.translogRetentionSize = INDEX_TRANSLOG_RETENTION_SIZE_SETTING.get(settings);
        this.softDeleteRetentionOperations = settings.getAsLong("index.soft_deletes.retention.operations", 0L);
        this.retentionLeaseMillis = INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis();
        
        this.isRemoteStoreEnabled = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings);
        this.remoteStoreTranslogRepository = INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(settings);
        this.remoteTranslogUploadBufferInterval = INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        this.remoteTranslogKeepExtraGen = INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.get(settings);
        this.translogArchiveUploadEnabled = INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING.get(settings);
        this.segmentArchiveUploadEnabled = INDEX_REMOTE_STORE_SEGMENT_ARCHIVE_UPLOAD_ENABLED_SETTING.get(settings);
        
        this.isTranslogMetadataEnabled = settings.getAsBoolean("index.remote_store.translog.metadata.enabled", true);
        this.remoteStorePathStrategy = RemoteStorePathStrategy.parse(settings);

        this.scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING, this::setGenerationThresholdSize);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_AGE_SETTING, this::setTranslogRetentionAge);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_SIZE_SETTING, this::setTranslogRetentionSize);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING, this::setRemoteTranslogUploadBufferInterval);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING, this::setRemoteTranslogKeepExtraGen);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_REMOTE_STORE_TRANSLOG_ARCHIVE_UPLOAD_ENABLED_SETTING, this::setTranslogArchiveUploadEnabled);
        this.scopedSettings.addSettingsUpdateConsumer(INDEX_REMOTE_STORE_SEGMENT_ARCHIVE_UPLOAD_ENABLED_SETTING, this::setSegmentArchiveUploadEnabled);
    }

    public Index getIndex() {
        return index;
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }

    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    private void setRefreshInterval(TimeValue refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public ByteSizeValue getTranslogFlushThresholdSize() {
        return translogFlushThresholdSize;
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue translogFlushThresholdSize) {
        this.translogFlushThresholdSize = translogFlushThresholdSize;
    }

    public ByteSizeValue getGenerationThresholdSize() {
        return generationThresholdSize;
    }

    private void setGenerationThresholdSize(ByteSizeValue generationThresholdSize) {
        this.generationThresholdSize = generationThresholdSize;
    }

    public TimeValue getTranslogRetentionAge() {
        return translogRetentionAge;
    }

    private void setTranslogRetentionAge(TimeValue translogRetentionAge) {
        this.translogRetentionAge = translogRetentionAge;
    }

    public ByteSizeValue getTranslogRetentionSize() {
        return translogRetentionSize;
    }

    private void setTranslogRetentionSize(ByteSizeValue translogRetentionSize) {
        this.translogRetentionSize = translogRetentionSize;
    }

    public long getSoftDeleteRetentionOperations() {
        return softDeleteRetentionOperations;
    }

    private void setSoftDeleteRetentionOperations(long ops) {
        this.softDeleteRetentionOperations = ops;
    }

    public long getRetentionLeaseMillis() {
        return retentionLeaseMillis;
    }

    private void setRetentionLeaseMillis(TimeValue retentionLeaseMillis) {
        this.retentionLeaseMillis = retentionLeaseMillis.millis();
    }

    public boolean isRemoteStoreEnabled() {
        return isRemoteStoreEnabled;
    }

    public String getRemoteStoreTranslogRepository() {
        return remoteStoreTranslogRepository;
    }

    public TimeValue getRemoteTranslogUploadBufferInterval() {
        return remoteTranslogUploadBufferInterval;
    }

    private void setRemoteTranslogUploadBufferInterval(TimeValue interval) {
        this.remoteTranslogUploadBufferInterval = interval;
    }

    public int getRemoteTranslogKeepExtraGen() {
        return remoteTranslogKeepExtraGen;
    }

    private void setRemoteTranslogKeepExtraGen(int extraGen) {
        this.remoteTranslogKeepExtraGen = extraGen;
    }

    public boolean isTranslogArchiveUploadEnabled() {
        return translogArchiveUploadEnabled;
    }

    private void setTranslogArchiveUploadEnabled(boolean enabled) {
        this.translogArchiveUploadEnabled = enabled;
    }

    public boolean isSegmentArchiveUploadEnabled() {
        return segmentArchiveUploadEnabled;
    }

    private void setSegmentArchiveUploadEnabled(boolean enabled) {
        this.segmentArchiveUploadEnabled = enabled;
    }

    public boolean isTranslogMetadataEnabled() {
        return isTranslogMetadataEnabled;
    }

    public RemoteStorePathStrategy getRemoteStorePathStrategy() {
        return remoteStorePathStrategy;
    }
}
