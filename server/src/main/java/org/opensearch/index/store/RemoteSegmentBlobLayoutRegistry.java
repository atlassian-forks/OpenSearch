/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves remote segment blob layouts registered by the node.
 *
 * <p>The registry always supplies the built-in {@value #DEFAULT_LAYOUT_NAME} layout. Plugin layout names must be
 * unique and cannot replace that built-in behavior. Resolution deliberately fails for an unknown name rather than
 * falling back to the default layout. This prevents a node without an installed layout plugin from interpreting an
 * existing index with the wrong physical format.
 *
 * @opensearch.api
 */
@ExperimentalApi
public final class RemoteSegmentBlobLayoutRegistry {

    public static final String DEFAULT_LAYOUT_NAME = "default";

    private final Map<String, RemoteSegmentBlobLayoutFactory> factories;

    /**
     * Creates a registry from named plugin contributions and verifies that each map key matches its factory name.
     */
    public RemoteSegmentBlobLayoutRegistry(Map<String, RemoteSegmentBlobLayoutFactory> pluginFactories) {
        this(pluginFactories.values());
        pluginFactories.forEach((name, factory) -> {
            if (name.equals(factory.getName()) == false) {
                throw new IllegalArgumentException("Remote segment blob layout key [" + name + "] does not match factory name");
            }
        });
    }

    /**
     * Creates a registry from plugin factory instances and rejects duplicate names eagerly during node startup.
     */
    public RemoteSegmentBlobLayoutRegistry(Collection<RemoteSegmentBlobLayoutFactory> pluginFactories) {
        factories = new HashMap<>();
        factories.put(DEFAULT_LAYOUT_NAME, new DefaultRemoteSegmentBlobLayoutFactory());
        pluginFactories.forEach(factory -> {
            String name = factory.getName();
            if (DEFAULT_LAYOUT_NAME.equals(name)) {
                throw new IllegalArgumentException("Remote segment blob layout [default] is reserved");
            }
            if (factories.putIfAbsent(name, factory) != null) {
                throw new IllegalArgumentException("Duplicate remote segment blob layout [" + name + "]");
            }
        });
    }

    /**
     * Resolves {@code name}, or fails with a clear error when the corresponding plugin is not installed.
     */
    public RemoteSegmentBlobLayoutFactory getFactory(String name) {
        RemoteSegmentBlobLayoutFactory factory = factories.get(name);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown remote segment blob layout [" + name + "]");
        }
        return factory;
    }
}
