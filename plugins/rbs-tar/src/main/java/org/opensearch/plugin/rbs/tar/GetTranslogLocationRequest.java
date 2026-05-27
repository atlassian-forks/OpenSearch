/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public final class GetTranslogLocationRequest extends ClusterManagerNodeRequest<GetTranslogLocationRequest> {
    private final String indexUuid;
    private final int shardId;
    private final long generation;

    public GetTranslogLocationRequest(final String indexUuid, final int shardId, final long generation) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.generation = generation;
    }

    public GetTranslogLocationRequest(final StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readInt();
        this.generation = in.readLong();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeInt(shardId);
        out.writeLong(generation);
    }

    public String getIndexUuid() {
        return indexUuid;
    }

    public int getShardId() {
        return shardId;
    }

    public long getGeneration() {
        return generation;
    }
}
