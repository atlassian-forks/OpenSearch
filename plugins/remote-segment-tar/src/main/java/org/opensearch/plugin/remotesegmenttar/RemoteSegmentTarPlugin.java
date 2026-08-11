/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import org.opensearch.index.store.RemoteSegmentBlobLayoutFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RemoteSegmentBlobLayoutPlugin;

import java.util.Map;

/**
 * Registers the {@code tar} physical layout for remote segment batches.
 *
 * <p>The plugin does not provide a remote directory, upload scheduler, or translog implementation. Core still selects
 * the refresh batch and owns metadata and checkpoint publication. This plugin only turns that selected batch into one
 * immutable archive and translates the locations recorded by core back into remote byte ranges.
 */
public class RemoteSegmentTarPlugin extends Plugin implements RemoteSegmentBlobLayoutPlugin {

    @Override
    public Map<String, RemoteSegmentBlobLayoutFactory> getRemoteSegmentBlobLayouts() {
        // The registry rejects duplicate names and requires this plugin on each node that opens a TAR-layout shard.
        return Map.of(TarRemoteSegmentBlobLayoutFactory.NAME, new TarRemoteSegmentBlobLayoutFactory());
    }
}
