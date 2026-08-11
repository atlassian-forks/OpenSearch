/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;

/**
 * Creates one remote segment blob layout for a shard.
 *
 * <p>Factories are registered once per node and are resolved from the immutable index layout setting. The returned
 * layout instance is owned by one {@link RemoteSegmentStoreDirectory}. It may retain shard-local configuration, but it
 * must not retain cross-shard reference counts or replace the directory lifecycle.
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface RemoteSegmentBlobLayoutFactory {

    /**
     * Returns the unique layout name used by the index setting.
     */
    String getName();

    /**
     * Creates the layout using the rate-limited remote store facade for {@code shardId}.
     */
    RemoteSegmentBlobLayout create(RemoteSegmentBlobStore remoteStore, ShardId shardId);
}
