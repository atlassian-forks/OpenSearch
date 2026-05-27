/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.util.Collections;
import java.util.Map;

public final class InspectableRepositoryPlugin extends Plugin implements RepositoryPlugin {
    public static final String TYPE = "inspectable_fs";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        final Environment env,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            TYPE,
            metadata -> new InspectableRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );
    }
}
