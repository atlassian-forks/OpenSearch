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
import java.util.ArrayList;
import java.util.List;

public final class NodeBundleReportRequest extends ClusterManagerNodeRequest<NodeBundleReportRequest> {
    private final String nodeId;
    private final String bundlePath;
    private final long timestamp;
    private final List<NodeBundleRegistry.ShardReport> shards;

    public NodeBundleReportRequest(
        final String nodeId,
        final String bundlePath,
        final long timestamp,
        final List<NodeBundleRegistry.ShardReport> shards
    ) {
        this.nodeId = nodeId;
        this.bundlePath = bundlePath;
        this.timestamp = timestamp;
        this.shards = shards;
    }

    public NodeBundleReportRequest(final StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.bundlePath = in.readString();
        this.timestamp = in.readLong();
        final int shardCount = in.readVInt();
        this.shards = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            final String indexUuid = in.readString();
            final int shardId = in.readInt();
            final long term = in.readLong();
            final long minGen = in.readLong();
            final int fileCount = in.readVInt();
            final List<NodeBundleRegistry.FileReport> files = new ArrayList<>(fileCount);
            for (int j = 0; j < fileCount; j++) {
                final long gen = in.readLong();
                final boolean isTlg = in.readBoolean();
                final long offset = in.readLong();
                final long length = in.readLong();
                files.add(new NodeBundleRegistry.FileReport(gen, isTlg, offset, length));
            }
            this.shards.add(new NodeBundleRegistry.ShardReport(indexUuid, shardId, term, minGen, files));
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeString(bundlePath);
        out.writeLong(timestamp);
        out.writeVInt(shards.size());
        for (final NodeBundleRegistry.ShardReport shard : shards) {
            out.writeString(shard.indexUuid);
            out.writeInt(shard.shardId);
            out.writeLong(shard.primaryTerm);
            out.writeLong(shard.minRemoteGenReferenced);
            out.writeVInt(shard.files.size());
            for (final NodeBundleRegistry.FileReport file : shard.files) {
                out.writeLong(file.generation);
                out.writeBoolean(file.isTlg);
                out.writeLong(file.offset);
                out.writeLong(file.length);
            }
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getBundlePath() {
        return bundlePath;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<NodeBundleRegistry.ShardReport> getShards() {
        return shards;
    }
}
