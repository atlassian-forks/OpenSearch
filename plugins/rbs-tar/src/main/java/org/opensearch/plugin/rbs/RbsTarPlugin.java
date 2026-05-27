/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;
import org.opensearch.index.translog.transfer.RemoteStoreTranslogStrategy;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.plugin.rbs.tar.GetTranslogLocationAction;
import org.opensearch.plugin.rbs.tar.NodeBundleRegistry;
import org.opensearch.plugin.rbs.tar.NodeBundleReportAction;
import org.opensearch.plugin.rbs.tar.NodeTranslogUploadQueue;
import org.opensearch.plugin.rbs.tar.TarSegmentUploadStrategy;
import org.opensearch.plugin.rbs.tar.TarTranslogUploadStrategy;
import org.opensearch.plugin.rbs.tar.TransportGetTranslogLocationAction;
import org.opensearch.plugin.rbs.tar.TransportNodeBundleReportAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RemoteStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Main plugin class for RBS Tar.
 */
public final class RbsTarPlugin extends Plugin implements RemoteStorePlugin, ActionPlugin {
    private static final Logger logger = LogManager.getLogger(RbsTarPlugin.class);

    public static final Setting<Long> BUNDLE_MAX_WAIT_MS_SETTING = Setting.longSetting(
        "cluster.remote_store.translog.bundle.max_wait_ms",
        500L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> BUNDLE_MIN_SHARDS_SETTING = Setting.intSetting(
        "cluster.remote_store.translog.bundle.min_shards",
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> BUNDLE_MAX_BYTES_SETTING = Setting.byteSizeSetting(
        "cluster.remote_store.translog.bundle.max_bytes",
        new ByteSizeValue(10, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> INDEX_COMMIT_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.index.commit_interval",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final DelegatingTranslogStrategy delegatingTranslogStrategy = new DelegatingTranslogStrategy();
    private NodeBundleRegistry registry;
    private NodeTranslogUploadQueue queue;
    private TarTranslogUploadStrategy translogStrategy;

    private volatile ThreadPool threadPool;
    private volatile Supplier<RepositoriesService> reposServiceSupplier;
    private volatile ClusterService clusterService;
    private volatile Scheduler.Cancellable commitFuture;

    private synchronized void updateCommitInterval(final TimeValue newInterval) {
        if (commitFuture != null) {
            commitFuture.cancel();
        }
        if (threadPool != null && clusterService != null && reposServiceSupplier != null) {
            commitFuture = threadPool.scheduleWithFixedDelay(() -> {
                if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                    registry.commitIndex(reposServiceSupplier, clusterService);
                }
            }, newInterval, ThreadPool.Names.GENERIC);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(BUNDLE_MAX_WAIT_MS_SETTING, BUNDLE_MIN_SHARDS_SETTING, BUNDLE_MAX_BYTES_SETTING, INDEX_COMMIT_INTERVAL_SETTING);
    }

    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return List.of(
            new ActionHandler<>(NodeBundleReportAction.INSTANCE, TransportNodeBundleReportAction.class),
            new ActionHandler<>(GetTranslogLocationAction.INSTANCE, TransportGetTranslogLocationAction.class)
        );
    }

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
        this.registry = new NodeBundleRegistry();
        this.queue = new NodeTranslogUploadQueue(clusterService, threadPool, repositoriesServiceSupplier, client);
        this.translogStrategy = new TarTranslogUploadStrategy(queue, repositoriesServiceSupplier, clusterService, client);
        this.delegatingTranslogStrategy.setDelegate(translogStrategy);

        this.threadPool = threadPool;
        this.reposServiceSupplier = repositoriesServiceSupplier;
        this.clusterService = clusterService;

        // Schedule periodic commits of registry index to S3 on coordination node
        final TimeValue initialInterval = clusterService.getClusterSettings().get(INDEX_COMMIT_INTERVAL_SETTING);
        updateCommitInterval(initialInterval);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_COMMIT_INTERVAL_SETTING, this::updateCommitInterval);

        // Schedule periodic garbage collection of translog bundles on coordination node
        threadPool.scheduleWithFixedDelay(() -> {
            if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                registry.runGarbageCollection(repositoriesServiceSupplier, clusterService);
            }
        }, TimeValue.timeValueMinutes(5), ThreadPool.Names.GENERIC);

        // Listen to cluster manager elections for failover recovery
        clusterService.addListener(event -> {
            boolean wasClusterManager = event.previousState().nodes().isLocalNodeElectedClusterManager();
            boolean isClusterManager = event.state().nodes().isLocalNodeElectedClusterManager();
            if (isClusterManager && wasClusterManager == false) {
                threadPool.generic().execute(() -> {
                    try {
                        registry.recoverIndexOnFailover(repositoriesServiceSupplier, clusterService);
                    } catch (Exception e) {
                        logger.error("Failed to recover NodeBundleRegistry index on master failover", e);
                    }
                });
            }
        });

        List<Object> components = new ArrayList<>();
        components.add(registry);
        components.add(queue);
        components.add(translogStrategy);
        return components;
    }

    @Override
    public Map<String, RemoteStoreSegmentStrategy> getRemoteStoreSegmentStrategies() {
        return Collections.singletonMap("rbs-tar", new TarSegmentUploadStrategy());
    }

    @Override
    public Map<String, RemoteStoreTranslogStrategy> getRemoteStoreTranslogStrategies() {
        return Collections.singletonMap("rbs-tar", delegatingTranslogStrategy);
    }

    private static final class DelegatingTranslogStrategy implements RemoteStoreTranslogStrategy {
        private volatile RemoteStoreTranslogStrategy delegate;

        void setDelegate(final RemoteStoreTranslogStrategy delegate) {
            this.delegate = delegate;
        }

        @Override
        public RemoteStoreTranslogStrategy.GcDecision resolveStaleTranslogBlobs() {
            final RemoteStoreTranslogStrategy d = delegate;
            if (d == null) {
                return RemoteStoreTranslogStrategy.GcDecision.USE_DEFAULT;
            }
            return d.resolveStaleTranslogBlobs();
        }

        @Override
        public boolean transferSnapshot(
            final ShardId shardId,
            final TransferSnapshot transferSnapshot,
            final TranslogTransferListener listener,
            final CryptoMetadata cryptoMetadata
        ) throws IOException {
            final RemoteStoreTranslogStrategy d = delegate;
            if (d == null) {
                throw new IOException("TarTranslogUploadStrategy is not initialized yet");
            }
            return d.transferSnapshot(shardId, transferSnapshot, listener, cryptoMetadata);
        }

        @Override
        public void downloadRange(
            final ShardId shardId,
            final long minGeneration,
            final long maxGeneration,
            final java.util.Map<String, String> generationToPrimaryTerm,
            final Path location
        ) throws IOException {
            final RemoteStoreTranslogStrategy d = delegate;
            if (d == null) {
                throw new IOException("TarTranslogUploadStrategy is not initialized yet");
            }
            d.downloadRange(shardId, minGeneration, maxGeneration, generationToPrimaryTerm, location);
        }

    }
}
