/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.remote.SegmentRemoteStoreStrategy;
// TranslogBatchCoordinator is in same plugin package
// TranslogBatchCollector is in same plugin package
import org.opensearch.index.translog.transfer.TranslogRemoteStoreStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RemoteStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * RBS Archive Plugin — provides TAR-based remote store strategies for OpenSearch Remote Backed Storage.
 *
 * <p>When loaded, this plugin replaces the default per-file segment and translog upload behavior
 * with TAR-archive bundling, significantly reducing the number of S3 API calls per upload cycle.
 *
 * <p>Plugin provides:
 * <ul>
 *   <li><b>Segment strategy</b>: {@link TarSegmentRemoteStoreStrategy} — bundles files into TAR
 *       archives per refresh cycle; downloads via range-GET; defers GC to a background task.</li>
 *   <li><b>Translog strategy</b>: {@link TarTranslogRemoteStoreStrategy} — "shared taxi" node-level
 *       batching: all primary shards submit their translog snapshots to a node-scoped
 *       {@link TranslogBatchCoordinator} which uploads a single TAR blob per batch window.</li>
 * </ul>
 *
 * <p>The {@link TranslogBatchCollector} lifecycle component is registered via {@link #createComponents}
 * so that it starts/stops with the node and participates in cluster-manager GC scheduling.
 */
public class RbsArchivePlugin extends Plugin implements RemoteStorePlugin {

    // Default settings — will be made cluster-level settings in a follow-up
    static final TimeValue DEFAULT_ARCHIVE_MAX_WAIT = TimeValue.timeValueMillis(200);
    static final int DEFAULT_ARCHIVE_THRESHOLD = 10;
    static final TimeValue DEFAULT_GC_INTERVAL = TimeValue.timeValueMinutes(10);
    static final Duration DEFAULT_RETENTION_AGE = Duration.ofHours(2);

    /** Shared node-level TAR translog strategy — created once, shared across all shards. */
    private TarTranslogRemoteStoreStrategy tarTranslogStrategy;

    @Override
    public SegmentRemoteStoreStrategy getSegmentStrategy() {
        return new TarSegmentRemoteStoreStrategy();
    }

    /**
     * Returns the TAR translog strategy that uses node-level batching via
     * {@link TranslogBatchCoordinator}. The coordinator itself is wired in
     * {@link #createComponents} and injected into the strategy.
     *
     * <p>Returns {@code null} until {@link #createComponents} has been called
     * (i.e., before node start). Shards created before the collector starts will use
     * the default per-file behavior.
     */
    @Override
    public TranslogRemoteStoreStrategy getTranslogStrategy() {
        return tarTranslogStrategy;
    }

    /**
     * Creates the node-level {@link TranslogBatchCollector} lifecycle component.
     *
     * <p>The collector owns the {@link TranslogBatchCoordinator} and registers
     * itself as a cluster-manager listener for GC scheduling. It is returned here so
     * that OpenSearch manages its lifecycle (start/stop with the node).
     *
     * <p><b>Note</b>: This is called before shards are created, so {@link #tarTranslogStrategy}
     * is set here and will be seen by all subsequently created shard translog instances.
     */
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        String nodeId = clusterService.localNode().getId();

        // Build coordinator and strategy together, breaking the circular reference:
        // strategy → coordinator → strategy is solved by a two-step init:
        //   1. Create coordinator with a null-strategy placeholder
        //   2. Create strategy with the coordinator reference
        //   3. Wire the strategy back into the coordinator via setStrategy()
        TranslogBatchCoordinator coordinator = new TranslogBatchCoordinator(
            nodeId,
            null, // strategy wired below via setStrategy()
            DEFAULT_ARCHIVE_MAX_WAIT,
            DEFAULT_ARCHIVE_THRESHOLD
        );
        tarTranslogStrategy = new TarTranslogRemoteStoreStrategy(coordinator);
        coordinator.setStrategy(tarTranslogStrategy);

        // The collector manages the coordinator lifecycle (start/stop with the node) and
        // schedules GC on the elected cluster-manager.
        TranslogBatchCollector collector = new TranslogBatchCollector(
            nodeId,
            clusterService,
            threadPool,
            tarTranslogStrategy,
            DEFAULT_ARCHIVE_MAX_WAIT,
            DEFAULT_ARCHIVE_THRESHOLD,
            DEFAULT_GC_INTERVAL,
            DEFAULT_RETENTION_AGE
        );

        return List.of(collector);
    }
}
