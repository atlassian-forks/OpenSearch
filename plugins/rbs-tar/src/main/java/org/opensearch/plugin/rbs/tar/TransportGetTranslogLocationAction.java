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
import java.util.List;

public final class TransportGetTranslogLocationAction extends TransportClusterManagerNodeAction<
    GetTranslogLocationRequest,
    GetTranslogLocationResponse> {

    private final NodeBundleRegistry registry;

    @Inject
    public TransportGetTranslogLocationAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final NodeBundleRegistry registry
    ) {
        super(
            GetTranslogLocationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetTranslogLocationRequest::new,
            indexNameExpressionResolver
        );
        this.registry = registry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetTranslogLocationResponse read(final StreamInput in) throws IOException {
        return new GetTranslogLocationResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(final GetTranslogLocationRequest request, final ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final GetTranslogLocationRequest request,
        final ClusterState state,
        final ActionListener<GetTranslogLocationResponse> listener
    ) {
        try {
            final List<NodeBundleRegistry.FileLocation> locations = registry.getTranslogLocations(
                request.getIndexUuid(),
                request.getShardId(),
                request.getGeneration()
            );
            listener.onResponse(new GetTranslogLocationResponse(locations));
        } catch (final Exception e) {
            listener.onFailure(e);
        }
    }
}
