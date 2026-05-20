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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Node-level registry for composite remote store strategies resolved from plugins.
 * <p>
 * Populated at node startup from {@link org.opensearch.plugins.PluginsService#filterPlugins(Class)}
 * and injected into components that need strategy resolution.
 * <p>
 * Strategies are looked up by name (e.g. {@code "tar"}) which must match the value of
 * {@code index.remote_store.segment.strategy} or {@code index.remote_store.translog.strategy}
 * on an index. Installing a plugin does NOT automatically activate its strategy — an operator
 * must explicitly configure the per-index setting.
 * <p>
 * If no name is configured for an index (empty string), both strategies return {@code null}
 * and callers fall back to built-in per-file behavior.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class RemoteStoreStrategyProvider {

    private static final Logger logger = LogManager.getLogger(RemoteStoreStrategyProvider.class);

    /** A no-op provider — all callers use built-in defaults. */
    public static final RemoteStoreStrategyProvider NOOP = new RemoteStoreStrategyProvider(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList()
    );

    /** Map of strategy name → segment strategy, in plugin registration order. */
    private final Map<String, SegmentRemoteStoreStrategy> segmentStrategies;

    /** Map of strategy name → translog strategy, in plugin registration order. */
    private final Map<String, TranslogRemoteStoreStrategy> translogStrategies;

    /**
     * All translog strategies in registration order, used for fallback chain during recovery.
     * Does NOT include the null/default entry.
     */
    private final List<TranslogRemoteStoreStrategy> allTranslogStrategies;

    /**
     * Names of all registered plugins in registration order.
     */
    private final List<String> registeredNames;

    private RemoteStoreStrategyProvider(
        Map<String, SegmentRemoteStoreStrategy> segmentStrategies,
        Map<String, TranslogRemoteStoreStrategy> translogStrategies,
        List<TranslogRemoteStoreStrategy> allTranslogStrategies,
        List<String> registeredNames
    ) {
        this.segmentStrategies = segmentStrategies;
        this.translogStrategies = translogStrategies;
        this.allTranslogStrategies = allTranslogStrategies;
        this.registeredNames = registeredNames;
    }

    /**
     * Build a provider registry by discovering strategies from loaded {@link RemoteStorePlugin} plugins.
     * Multiple plugins with distinct {@link RemoteStorePlugin#getStrategyName()} values are all registered.
     * Plugins with an empty name or duplicate names are skipped with a warning.
     *
     * @param plugins the list of loaded plugins implementing {@link RemoteStorePlugin}
     * @return a configured provider (or {@link #NOOP} if no plugin is loaded)
     */
    public static RemoteStoreStrategyProvider fromPlugins(List<RemoteStorePlugin> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            logger.debug("No RemoteStorePlugin found — using built-in default strategies");
            return NOOP;
        }
        Map<String, SegmentRemoteStoreStrategy> segMap = new LinkedHashMap<>();
        Map<String, TranslogRemoteStoreStrategy> tlogMap = new LinkedHashMap<>();
        List<TranslogRemoteStoreStrategy> allTlog = new ArrayList<>();
        List<String> names = new ArrayList<>();

        for (RemoteStorePlugin plugin : plugins) {
            String name = plugin.getStrategyName();
            if (name == null || name.isEmpty()) {
                logger.warn("RemoteStorePlugin [{}] returned empty getStrategyName() — skipping",
                    plugin.getClass().getName());
                continue;
            }
            if (segMap.containsKey(name) || tlogMap.containsKey(name)) {
                logger.warn("RemoteStorePlugin [{}] duplicate strategy name '{}' — skipping",
                    plugin.getClass().getName(), name);
                continue;
            }
            SegmentRemoteStoreStrategy seg = plugin.getSegmentStrategy();
            TranslogRemoteStoreStrategy tlog = plugin.getTranslogStrategy();
            if (seg != null) segMap.put(name, seg);
            if (tlog != null) {
                tlogMap.put(name, tlog);
                allTlog.add(tlog);
            }
            names.add(name);
            logger.info(
                "RemoteStorePlugin [{}] registered as '{}' — segmentStrategy={}, translogStrategy={}",
                plugin.getClass().getName(), name,
                seg != null ? seg.getClass().getSimpleName() : "none",
                tlog != null ? tlog.getClass().getSimpleName() : "none"
            );
        }
        if (names.isEmpty()) return NOOP;
        return new RemoteStoreStrategyProvider(
            Collections.unmodifiableMap(segMap),
            Collections.unmodifiableMap(tlogMap),
            Collections.unmodifiableList(allTlog),
            Collections.unmodifiableList(names)
        );
    }

    /**
     * Returns the segment strategy for the given strategy name, or {@code null} if the name
     * is empty/unknown (caller should use built-in per-file behavior).
     *
     * @param strategyName the value of {@code index.remote_store.segment.strategy}
     */
    public SegmentRemoteStoreStrategy segmentStrategyFor(String strategyName) {
        if (strategyName == null || strategyName.isEmpty()) return null;
        SegmentRemoteStoreStrategy s = segmentStrategies.get(strategyName);
        if (s == null) {
            logger.warn("Segment strategy '{}' not found in registry — using per-file default", strategyName);
        }
        return s;
    }

    /**
     * Returns the translog strategy for the given strategy name, or {@code null} if the name
     * is empty/unknown (caller should use built-in per-file behavior).
     *
     * @param strategyName the value of {@code index.remote_store.translog.strategy}
     */
    public TranslogRemoteStoreStrategy translogStrategyFor(String strategyName) {
        if (strategyName == null || strategyName.isEmpty()) return null;
        TranslogRemoteStoreStrategy s = translogStrategies.get(strategyName);
        if (s == null) {
            logger.warn("Translog strategy '{}' not found in registry — using per-file default", strategyName);
        }
        return s;
    }

    /**
     * Returns all registered translog strategies EXCEPT the one with the given name,
     * in registration order. Used for the recovery fallback chain: try current strategy
     * first, then try each other registered strategy, then default per-file.
     *
     * @param currentStrategyName the name of the strategy already tried (may be null/empty)
     */
    public List<TranslogRemoteStoreStrategy> otherTranslogStrategies(String currentStrategyName) {
        if (allTranslogStrategies.isEmpty()) return Collections.emptyList();
        List<TranslogRemoteStoreStrategy> others = new ArrayList<>();
        for (Map.Entry<String, TranslogRemoteStoreStrategy> e : translogStrategies.entrySet()) {
            if (!e.getKey().equals(currentStrategyName)) {
                others.add(e.getValue());
            }
        }
        return Collections.unmodifiableList(others);
    }

    /**
     * Returns all registered translog strategies in registration order.
     * Used for recovery fallback when no current strategy is known.
     */
    public List<TranslogRemoteStoreStrategy> allTranslogStrategies() {
        return allTranslogStrategies;
    }

    /**
     * Returns the plugin-provided segment strategy, or {@code null} (use built-in defaults).
     * @deprecated Use {@link #segmentStrategyFor(String)} with per-index setting instead.
     */
    @Deprecated
    public SegmentRemoteStoreStrategy getSegmentStrategy() {
        // Backward compat: return first registered, or null
        return segmentStrategies.isEmpty() ? null : segmentStrategies.values().iterator().next();
    }

    /**
     * Returns the plugin-provided translog strategy, or {@code null} (use built-in defaults).
     * @deprecated Use {@link #translogStrategyFor(String)} with per-index setting instead.
     */
    @Deprecated
    public TranslogRemoteStoreStrategy getTranslogStrategy() {
        // Backward compat: return first registered, or null
        return translogStrategies.isEmpty() ? null : translogStrategies.values().iterator().next();
    }

    /** Returns the names of all registered strategy plugins, in registration order. */
    public List<String> getRegisteredNames() {
        return registeredNames;
    }
}
