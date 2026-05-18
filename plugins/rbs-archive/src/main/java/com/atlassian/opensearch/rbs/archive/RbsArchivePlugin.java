/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RemoteStorePlugin;

/**
 * RBS Archive Plugin — provides TAR-based remote store strategies for OpenSearch Remote Backed Storage.
 * <p>
 * When loaded, this plugin replaces the default per-file segment and translog upload behavior
 * with TAR-archive bundling, significantly reducing the number of S3 API calls per upload cycle.
 * <p>
 * The plugin provides complete lifecycle strategies for both segments and translog:
 * <ul>
 *   <li><b>Segment strategy</b>: bundles files into TAR archives per refresh cycle;
 *       downloads via range-GET; defers GC to {@code createComponents()} background task.</li>
 *   <li><b>Translog strategy</b>: {@code null} (use default per-file) until TAR translog
 *       batching is fully implemented.</li>
 * </ul>
 */
public class RbsArchivePlugin extends Plugin implements RemoteStorePlugin {

    @Override
    public SegmentRemoteStoreStrategy getSegmentStrategy() {
        return new TarSegmentRemoteStoreStrategy();
    }

    /**
     * Translog TAR batching is not yet fully implemented — returns {@code null} to use the
     * default per-file translog behavior. Full TAR translog batching (including recovery and GC)
     * will be added in a follow-up.
     */
    @Override
    public TranslogRemoteStoreStrategy getTranslogStrategy() {
        return null; // use default per-file behavior
    }
}
