/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.RemoteSegmentBlobLayoutFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A plugin that contributes physical remote segment blob layouts.
 *
 * <p>Implement this interface only to provide the storage format for selected remote segment upload batches. Do not
 * provide a replacement directory factory, upload scheduler, translog implementation, or repository access path.
 * Core keeps those responsibilities so all remote-store workflows use the same layout contract.
 *
 * <p>Every data node that can allocate an index using a plugin layout must install the plugin. If it is absent, layout
 * resolution fails instead of opening the index with the default layout.
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface RemoteSegmentBlobLayoutPlugin {

    /**
     * Returns factories keyed by their immutable index-setting names.
     *
     * <p>Keys must exactly match {@link RemoteSegmentBlobLayoutFactory#getName()}. The name {@code default} is
     * reserved for the built-in layout and plugin registration of that name is rejected.
     */
    default Map<String, RemoteSegmentBlobLayoutFactory> getRemoteSegmentBlobLayouts() {
        return Collections.emptyMap();
    }
}
