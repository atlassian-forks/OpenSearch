/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.plugins.RemoteStorePlugin;

import java.util.List;

/**
 * Node-level holder for composite remote store strategies resolved from plugins.
 * <p>
 * Populated at node startup from {@link org.opensearch.plugins.PluginsService#filterPlugins(Class)}
 * and injected into components that need strategy resolution.
 * <p>
 * If no {@link RemoteStorePlugin} is loaded, both strategies are {@code null} and
 * callers fall back to built-in per-file behavior.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class RemoteStoreStrategyProvider {

    private static final Logger logger = LogManager.getLogger(RemoteStoreStrategyProvider.class);

    /** A no-op provider — all callers use built-in defaults. */
    public static final RemoteStoreStrategyProvider NOOP = new RemoteStoreStrategyProvider(null, null);

    private final SegmentRemoteStoreStrategy segmentStrategy;
    private final TranslogRemoteStoreStrategy translogStrategy;

    private RemoteStoreStrategyProvider(
        SegmentRemoteStoreStrategy segmentStrategy,
        TranslogRemoteStoreStrategy translogStrategy
    ) {
        this.segmentStrategy = segmentStrategy;
        this.translogStrategy = translogStrategy;
    }

    /**
     * Build a provider by discovering strategies from loaded {@link RemoteStorePlugin} plugins.
     * If multiple plugins are present, the first one wins and a warning is logged.
     *
     * @param plugins the list of loaded plugins implementing {@link RemoteStorePlugin}
     * @return a configured provider (or {@link #NOOP} if no plugin is loaded)
     */
    public static RemoteStoreStrategyProvider fromPlugins(List<RemoteStorePlugin> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            logger.debug("No RemoteStorePlugin found — using built-in default strategies");
            return NOOP;
        }
        if (plugins.size() > 1) {
            logger.warn(
                "Multiple RemoteStorePlugin implementations found ({}); using the first: {}",
                plugins.size(),
                plugins.get(0).getClass().getName()
            );
        }
        RemoteStorePlugin plugin = plugins.get(0);
        SegmentRemoteStoreStrategy segmentStrategy = plugin.getSegmentStrategy();
        TranslogRemoteStoreStrategy translogStrategy = plugin.getTranslogStrategy();
        logger.info(
            "RemoteStorePlugin [{}] registered — segmentStrategy={}, translogStrategy={}",
            plugin.getClass().getName(),
            segmentStrategy != null ? segmentStrategy.getClass().getSimpleName() : "default",
            translogStrategy != null ? translogStrategy.getClass().getSimpleName() : "default"
        );
        return new RemoteStoreStrategyProvider(segmentStrategy, translogStrategy);
    }

    /**
     * Returns the plugin-provided segment strategy, or {@code null} (use built-in defaults).
     */
    public SegmentRemoteStoreStrategy getSegmentStrategy() {
        return segmentStrategy;
    }

    /**
     * Returns the plugin-provided translog strategy, or {@code null} (use built-in defaults).
     */
    public TranslogRemoteStoreStrategy getTranslogStrategy() {
        return translogStrategy;
    }
}
