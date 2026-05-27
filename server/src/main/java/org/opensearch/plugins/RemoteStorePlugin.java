/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;
import org.opensearch.index.translog.transfer.RemoteStoreTranslogStrategy;

import java.util.Collections;
import java.util.Map;

/**
 * A plugin that provides custom strategies for Remote Blob Store (RBS) operations.
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface RemoteStorePlugin {

    /**
     * The {@link RemoteStoreSegmentStrategy} mappings for this plugin.
     *
     * @return a map from segment strategy type key to a strategy implementation
     */
    default Map<String, RemoteStoreSegmentStrategy> getRemoteStoreSegmentStrategies() {
        return Collections.emptyMap();
    }

    /**
     * The {@link RemoteStoreTranslogStrategy} mappings for this plugin.
     *
     * @return a map from translog strategy type key to a strategy implementation
     */
    default Map<String, RemoteStoreTranslogStrategy> getRemoteStoreTranslogStrategies() {
        return Collections.emptyMap();
    }
}
