/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.core.index.shard.ShardId;

final class DefaultRemoteSegmentBlobLayoutFactory implements RemoteSegmentBlobLayoutFactory {

    @Override
    public String getName() {
        return RemoteSegmentBlobLayoutRegistry.DEFAULT_LAYOUT_NAME;
    }

    @Override
    public RemoteSegmentBlobLayout create(RemoteSegmentBlobStore remoteStore, ShardId shardId) {
        return new DefaultRemoteSegmentBlobLayout(remoteStore);
    }
}
