/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Proves that both segment and translog remote-store uploads can be bundled from a plugin, through minimal
 * core extension points (a subclassable {@code RemoteSegmentStoreDirectory} for segments, a subclassable
 * {@code RemoteFsTranslog} plus one new {@code EnginePlugin} hook for translog) rather than the broader
 * Strategy-interface SPI from the original RFC (opensearch-project/OpenSearch#21850).
 * <p>
 * Segment bundling ({@link BundlingRemoteSegmentStoreDirectory}) is complete, including GC of bundle blobs.
 * Translog bundling ({@link BatchingRemoteFsTranslog}) is a narrower proof of the mechanism only; see its
 * javadoc for what is not solved. Registration below is global (every remote-store index on the node), not
 * yet index-settings-selectable; that is a scoping choice for this implementation, not a limitation of the
 * underlying core hooks.
 */
public class RemoteStoreBatchingPlugin extends Plugin implements IndexStorePlugin, EnginePlugin {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private ThreadPool threadPool;
    private RemoteStoreSettings remoteStoreSettings;

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
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.remoteStoreSettings = new RemoteStoreSettings(clusterService.getSettings(), clusterService.getClusterSettings());
        return Collections.emptyList();
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(
            RemoteSegmentStoreDirectoryFactory.PLUGGABLE_REMOTE_DIRECTORY_FACTORY_KEY,
            new BundlingRemoteSegmentStoreDirectoryFactory(
                repositoriesServiceSupplier,
                threadPool,
                remoteStoreSettings.getSegmentsPathFixedPrefix()
            )
        );
    }

    @Override
    public Optional<TranslogFactory> getCustomTranslogFactory(IndexSettings indexSettings) {
        if (indexSettings.isRemoteTranslogStoreEnabled() == false) {
            return Optional.empty();
        }
        return Optional.of(new BatchingTranslogFactory(repositoriesServiceSupplier, threadPool, remoteStoreSettings));
    }
}
