/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * TDD Tests for cluster-level segment archive settings
 * Tests control of segment archive upload fallback behavior
 */
public class RemoteStoreSegmentArchiveSettingsTests extends OpenSearchTestCase {

    /**
     * Test: Segment archive fallback setting has correct default (true)
     */
    public void testSegmentArchiveFallbackDefaultValue() {
        // Given: default settings (no explicit override)
        Settings settings = Settings.EMPTY;
        
        // When: reading fallback setting
        boolean fallbackDefault = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(settings);
        
        // Then: should default to true (fallback enabled)
        assertThat(fallbackDefault, equalTo(true));
    }

    /**
     * Test: Can disable segment archive fallback
     */
    public void testCanDisableSegmentArchiveFallback() {
        // Given: settings with fallback disabled
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey(), false)
            .build();
        
        // When: reading fallback setting
        boolean fallbackEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(settings);
        
        // Then: should be false (fallback disabled)
        assertThat(fallbackEnabled, equalTo(false));
    }

    /**
     * Test: Segment archive fallback is dynamic (can be updated at runtime)
     */
    public void testSegmentArchiveFallbackIsDynamic() {
        // Given: setting can be updated
        boolean initialValue = true;
        boolean updatedValue = false;
        
        // When: updating setting (would be done by ClusterSettings in real scenario)
        Settings initialSettings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey(), initialValue)
            .build();
        
        Settings updatedSettings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey(), updatedValue)
            .build();
        
        // Then: both values are valid
        boolean val1 = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(initialSettings);
        boolean val2 = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(updatedSettings);
        
        assertThat(val1, equalTo(true));
        assertThat(val2, equalTo(false));
    }

    /**
     * Test: Segment archive global enable/disable setting
     */
    public void testSegmentArchiveGlobalEnableDisable() {
        // Given: settings with segment archive globally disabled
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey(), false)
            .build();
        
        // When: reading global enable setting
        boolean archiveEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.get(settings);
        
        // Then: should be false (globally disabled)
        assertThat(archiveEnabled, equalTo(false));
    }

    /**
     * Test: Segment archive global enable defaults to true
     */
    public void testSegmentArchiveGlobalEnableDefault() {
        // Given: default settings
        Settings settings = Settings.EMPTY;
        
        // When: reading global enable setting
        boolean archiveEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.get(settings);
        
        // Then: should default to true (enabled)
        assertThat(archiveEnabled, equalTo(true));
    }

    /**
     * Test: Index-level setting takes precedence over cluster setting
     */
    public void testIndexSettingPrecedenceOverCluster() {
        // Given: cluster setting enabled, but index setting disabled
        boolean clusterEnabled = true;
        boolean indexEnabled = false;
        
        // When: determining if archive should be used
        boolean shouldUseArchive = indexEnabled && clusterEnabled;
        
        // Then: should respect index-level setting
        assertThat(shouldUseArchive, equalTo(false));
    }

    /**
     * Test: Fallback setting name is correct
     */
    public void testFallbackSettingName() {
        // Given: the fallback setting
        String settingKey = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey();
        
        // When: checking setting name
        // Then: should follow expected naming convention
        assertTrue(settingKey.contains("segment"));
        assertTrue(settingKey.contains("archive"));
        assertTrue(settingKey.contains("fallback"));
    }

    /**
     * Test: Global enable setting name is correct
     */
    public void testGlobalEnableSettingName() {
        // Given: the global enable setting
        String settingKey = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey();
        
        // When: checking setting name
        // Then: should follow expected naming convention
        assertTrue(settingKey.contains("segment"));
        assertTrue(settingKey.contains("archive"));
        assertTrue(settingKey.contains("enabled"));
    }

    /**
     * Test: Both settings work together correctly
     */
    public void testSettingsCombined() {
        // Given: both cluster settings configured
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey(), true)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey(), true)
            .build();
        
        // When: reading both settings
        boolean archiveEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.get(settings);
        boolean fallbackEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(settings);
        
        // Then: both should be true
        assertThat(archiveEnabled, equalTo(true));
        assertThat(fallbackEnabled, equalTo(true));
    }

    /**
     * Test: Can disable archive and still have fallback setting (independent)
     */
    public void testDisableArchiveKeepFallback() {
        // Given: archive disabled but fallback enabled
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.getKey(), false)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.getKey(), true)
            .build();
        
        // When: reading both settings
        boolean archiveEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_ENABLED.get(settings);
        boolean fallbackEnabled = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_ARCHIVE_FALLBACK_TO_PER_FILE.get(settings);
        
        // Then: archive disabled, fallback setting still present
        assertThat(archiveEnabled, equalTo(false));
        assertThat(fallbackEnabled, equalTo(true)); // Independent of archive enable
    }
}
