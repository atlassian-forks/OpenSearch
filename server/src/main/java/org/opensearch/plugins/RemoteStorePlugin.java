/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;

/**
 * Plugin SPI for replacing the default Remote Backed Storage (RBS) upload, download,
 * and GC strategies with custom implementations.
 * <p>
 * Plugins implement this interface to provide TAR-archive-based (or other) strategies
 * for both segment and translog remote store operations. All three lifecycle operations
 * (upload, download, GC) are bundled into a single composite strategy per data type
 * to ensure consistency — it is not possible to override only upload without also
 * providing a compatible download and GC implementation.
 * <p>
 * If a method returns {@code null}, the built-in default behavior is used for that
 * data type. At most one {@link RemoteStorePlugin} may be loaded per node;
 * loading multiple implementations will result in a warning and only the first is used.
 *
 * @opensearch.spi
 */
@ExperimentalApi
public interface RemoteStorePlugin {

    /**
     * Returns the composite segment remote store strategy (upload + download + GC),
     * or {@code null} to use the built-in per-file behavior.
     */
    default SegmentRemoteStoreStrategy getSegmentStrategy() {
        return null;
    }

    /**
     * Returns the composite translog remote store strategy (upload + download + GC),
     * or {@code null} to use the built-in per-file behavior.
     */
    default TranslogRemoteStoreStrategy getTranslogStrategy() {
        return null;
    }
}
