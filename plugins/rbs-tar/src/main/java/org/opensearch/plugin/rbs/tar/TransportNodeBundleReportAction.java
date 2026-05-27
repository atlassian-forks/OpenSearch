/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public final class TransportNodeBundleReportAction extends TransportClusterManagerNodeAction<
    NodeBundleReportRequest,
    NodeBundleReportResponse> {

    private final NodeBundleRegistry registry;

    @Inject
    public TransportNodeBundleReportAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final NodeBundleRegistry registry
    ) {
        super(
            NodeBundleReportAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            NodeBundleReportRequest::new,
            indexNameExpressionResolver
        );
        this.registry = registry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected NodeBundleReportResponse read(final StreamInput in) throws IOException {
        return new NodeBundleReportResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(final NodeBundleReportRequest request, final ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final NodeBundleReportRequest request,
        final ClusterState state,
        final ActionListener<NodeBundleReportResponse> listener
    ) {
        try {
            registry.registerBundle(request.getNodeId(), request.getBundlePath(), request.getTimestamp(), request.getShards());
            listener.onResponse(new NodeBundleReportResponse(true));
        } catch (final Exception e) {
            listener.onFailure(e);
        }
    }
}
