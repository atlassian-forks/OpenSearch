/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentBlobLayout;
import org.opensearch.index.store.RemoteSegmentBlobLayoutFactory;
import org.opensearch.index.store.RemoteSegmentBlobStore;

/**
 * Creates the shard-local TAR layout selected by {@code index.remote_store.segment.blob_layout=tar}.
 *
 * <p>The factory receives the core-owned, rate-limited remote store facade. It deliberately does not receive a blob
 * container, so the plugin cannot bypass repository encryption, download throttling, or upload throttling.
 */
public final class TarRemoteSegmentBlobLayoutFactory implements RemoteSegmentBlobLayoutFactory {

    public static final String NAME = "tar";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public RemoteSegmentBlobLayout create(RemoteSegmentBlobStore remoteStore, ShardId shardId) {
        // The layout has no mutable shared state. Metadata remains the source of truth for archive references.
        return new TarRemoteSegmentBlobLayout(remoteStore);
    }
}
