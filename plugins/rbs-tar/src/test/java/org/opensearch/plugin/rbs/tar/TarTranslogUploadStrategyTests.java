/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.RemoteStoreTranslogStrategy;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NodeBundleRegistry, NodeTranslogUploadQueue, and TarTranslogUploadStrategy.
 */
public class TarTranslogUploadStrategyTests extends OpenSearchTestCase {

    public void testRegistrySerializationAndLookup() throws IOException {
        final NodeBundleRegistry registry = new NodeBundleRegistry();

        final List<NodeBundleRegistry.FileReport> files = new ArrayList<>();
        files.add(new NodeBundleRegistry.FileReport(1L, true, 512, 100));
        files.add(new NodeBundleRegistry.FileReport(1L, false, 1124, 50));

        final List<NodeBundleRegistry.ShardReport> shards = new ArrayList<>();
        shards.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files));

        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", 1716645371000L, shards);

        // Verify lookup
        final List<NodeBundleRegistry.FileLocation> locations = registry.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(2, locations.size());
        assertEquals("txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", locations.get(0).bundlePath);
        assertTrue(locations.get(0).isTlg);
        assertEquals(512, locations.get(0).offset);
        assertEquals(100, locations.get(0).length);

        assertFalse(locations.get(1).isTlg);
        assertEquals(1124, locations.get(1).offset);
        assertEquals(50, locations.get(1).length);

        // Serialize and Deserialize roundtrip
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            registry.writeTo(dos, 3L); // Term 3
        }

        final byte[] data = baos.toByteArray();
        final NodeBundleRegistry deserialized = new NodeBundleRegistry();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            deserialized.readFrom(dis);
        }

        // Verify deserialized lookup
        final List<NodeBundleRegistry.FileLocation> deserializedLocs = deserialized.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(2, deserializedLocs.size());
        assertEquals("txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", deserializedLocs.get(0).bundlePath);
        assertTrue(deserializedLocs.get(0).isTlg);
        assertEquals(512, deserializedLocs.get(0).offset);
        assertEquals(100, deserializedLocs.get(0).length);
    }

    public void testRegistryGarbageCollection() {
        final NodeBundleRegistry registry = new NodeBundleRegistry();

        final List<NodeBundleRegistry.FileReport> files1 = new ArrayList<>();
        files1.add(new NodeBundleRegistry.FileReport(1L, true, 512, 100));

        final List<NodeBundleRegistry.FileReport> files2 = new ArrayList<>();
        files2.add(new NodeBundleRegistry.FileReport(2L, true, 512, 100));

        final List<NodeBundleRegistry.ShardReport> shards1 = new ArrayList<>();
        shards1.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files1));

        final List<NodeBundleRegistry.ShardReport> shards2 = new ArrayList<>();
        shards2.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, files2));

        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_1.tar", 1000L, shards1);
        registry.registerBundle("node-1", "txlog_node_bundles/node-1/2026/05/25/bundle_2.tar", 2000L, shards2);

        // Set min remote generation referenced to 2. This makes bundle_1 garbage, but bundle_2 remains active!
        registry.updateShardMinGen("index-uuid", 0, 2L);

        final List<NodeBundleRegistry.FileLocation> locs1 = registry.getTranslogLocations("index-uuid", 0, 1L);
        assertEquals(1, locs1.size());

        final List<NodeBundleRegistry.FileLocation> locs2 = registry.getTranslogLocations("index-uuid", 0, 2L);
        assertEquals(1, locs2.size());
    }

    // --- downloadRange() tests ---

    public void testDownloadRangeFromMasterBatchesRangeGets() throws Exception {
        // Verifies that downloadRange() fetches all gens in the range via a single
        // registry lookup per gen (in-memory) + one range-GET per file — no full scans.
        final ClusterService clusterService = mockClusterServiceWithMaster();
        final RepositoriesService reposService = mock(RepositoriesService.class);
        final BlobStoreRepository repo = mock(BlobStoreRepository.class);
        final BlobStore blobStore = mock(BlobStore.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);

        when(reposService.repository("mock-repo")).thenReturn(repo);
        when(repo.basePath()).thenReturn(new BlobPath());
        when(repo.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        // gen 1: .tlog + .ckp; gen 2: .tlog + .ckp — all in different bundles
        final byte[] gen1Tlg = "gen1-tlg".getBytes(StandardCharsets.UTF_8);
        final byte[] gen1Ckp = "gen1-ckp".getBytes(StandardCharsets.UTF_8);
        final byte[] gen2Tlg = "gen2-tlg".getBytes(StandardCharsets.UTF_8);
        final byte[] gen2Ckp = "gen2-ckp".getBytes(StandardCharsets.UTF_8);

        doAnswer(invocation -> {
            final long offset = invocation.getArgument(1);
            final long length = invocation.getArgument(2);
            if (offset == 100 && length == gen1Tlg.length) return new ByteArrayInputStream(gen1Tlg);
            if (offset == 200 && length == gen1Ckp.length) return new ByteArrayInputStream(gen1Ckp);
            if (offset == 300 && length == gen2Tlg.length) return new ByteArrayInputStream(gen2Tlg);
            if (offset == 400 && length == gen2Ckp.length) return new ByteArrayInputStream(gen2Ckp);
            return new ByteArrayInputStream(new byte[0]);
        }).when(blobContainer).readBlob(anyString(), anyLong(), anyLong());

        final Client client = mock(Client.class);
        doAnswer(invocation -> {
            final GetTranslogLocationRequest req = invocation.getArgument(1);
            final ActionListener<GetTranslogLocationResponse> listener = invocation.getArgument(2);
            final List<NodeBundleRegistry.FileLocation> locs = new ArrayList<>();
            if (req.getGeneration() == 1L) {
                locs.add(new NodeBundleRegistry.FileLocation(true, "bundle_1.tar", 100, gen1Tlg.length, 1L));
                locs.add(new NodeBundleRegistry.FileLocation(false, "bundle_1.tar", 200, gen1Ckp.length, 1L));
            } else if (req.getGeneration() == 2L) {
                locs.add(new NodeBundleRegistry.FileLocation(true, "bundle_2.tar", 300, gen2Tlg.length, 2L));
                locs.add(new NodeBundleRegistry.FileLocation(false, "bundle_2.tar", 400, gen2Ckp.length, 2L));
            }
            listener.onResponse(new GetTranslogLocationResponse(locs));
            return null;
        }).when(client).execute(eq(GetTranslogLocationAction.INSTANCE), any(), any());

        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(null, () -> reposService, clusterService, client);
        final Path tempDir = createTempDir();
        final ShardId shardId = new ShardId(new Index("test-index", "index-uuid"), 0);

        final Map<String, String> genToPrimaryTerm = new HashMap<>();
        genToPrimaryTerm.put("1", "1");
        genToPrimaryTerm.put("2", "1");
        strategy.downloadRange(shardId, 1L, 2L, genToPrimaryTerm, tempDir);

        assertEquals("gen1-tlg", new String(Files.readAllBytes(tempDir.resolve("translog-1.tlog")), StandardCharsets.UTF_8));
        assertEquals("gen1-ckp", new String(Files.readAllBytes(tempDir.resolve("translog-1.ckp")), StandardCharsets.UTF_8));
        assertEquals("gen2-tlg", new String(Files.readAllBytes(tempDir.resolve("translog-2.tlog")), StandardCharsets.UTF_8));
        assertEquals("gen2-ckp", new String(Files.readAllBytes(tempDir.resolve("translog-2.ckp")), StandardCharsets.UTF_8));
    }

    public void testDownloadRangeSkipsGensNotInMap() throws Exception {
        // downloadRange() must skip generations not present in generationToPrimaryTerm
        // (they may have already been trimmed and are not needed for recovery).
        final ClusterService clusterService = mockClusterServiceWithMaster();
        final RepositoriesService reposService = mock(RepositoriesService.class);
        final BlobStoreRepository repo = mock(BlobStoreRepository.class);
        final BlobStore blobStore = mock(BlobStore.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);

        when(reposService.repository("mock-repo")).thenReturn(repo);
        when(repo.basePath()).thenReturn(new BlobPath());
        when(repo.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        final byte[] gen3Tlg = "gen3-tlg".getBytes(StandardCharsets.UTF_8);
        doAnswer(invocation -> {
            final long offset = invocation.getArgument(1);
            if (offset == 500) return new ByteArrayInputStream(gen3Tlg);
            return new ByteArrayInputStream(new byte[0]);
        }).when(blobContainer).readBlob(anyString(), anyLong(), anyLong());

        final Client client = mock(Client.class);
        doAnswer(invocation -> {
            final GetTranslogLocationRequest req = invocation.getArgument(1);
            final ActionListener<GetTranslogLocationResponse> listener = invocation.getArgument(2);
            final List<NodeBundleRegistry.FileLocation> locs = new ArrayList<>();
            if (req.getGeneration() == 3L) {
                locs.add(new NodeBundleRegistry.FileLocation(true, "bundle_3.tar", 500, gen3Tlg.length, 3L));
            }
            listener.onResponse(new GetTranslogLocationResponse(locs));
            return null;
        }).when(client).execute(eq(GetTranslogLocationAction.INSTANCE), any(), any());

        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(null, () -> reposService, clusterService, client);
        final Path tempDir = createTempDir();
        final ShardId shardId = new ShardId(new Index("test-index", "index-uuid"), 0);

        // Range is [1..3] but only gen 3 is in the map — gens 1 and 2 should be silently skipped
        final Map<String, String> genToPrimaryTerm = new HashMap<>();
        genToPrimaryTerm.put("3", "1");
        strategy.downloadRange(shardId, 1L, 3L, genToPrimaryTerm, tempDir);

        assertFalse(Files.exists(tempDir.resolve("translog-1.tlog")));
        assertFalse(Files.exists(tempDir.resolve("translog-2.tlog")));
        assertTrue(Files.exists(tempDir.resolve("translog-3.tlog")));
    }

    public void testDownloadRangeSelfHealingFallbackWhenNoMaster() throws Exception {
        // When master is unavailable, downloadRange() falls back to self-healing scan for each gen.
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        final DiscoveryNode localNode = mock(DiscoveryNode.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getClusterManagerNode()).thenReturn(null); // No master
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.getId()).thenReturn("node-1");
        doAnswer(invocation -> {
            final java.util.function.Consumer<DiscoveryNode> action = invocation.getArgument(0);
            action.accept(localNode);
            return null;
        }).when(discoveryNodes).forEach(any());

        final IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", org.opensearch.Version.CURRENT)
                    .put("index.remote_store.enabled", true)
                    .put("index.remote_store.translog.repository", "mock-repo")
            )
            .build();
        when(metadata.iterator()).thenReturn(Collections.singletonList(indexMetadata).iterator());

        final RepositoriesService reposService = mock(RepositoriesService.class);
        final BlobStoreRepository repo = mock(BlobStoreRepository.class);
        final BlobStore blobStore = mock(BlobStore.class);
        final BlobContainer blobContainer = mock(BlobContainer.class);

        when(reposService.repository("mock-repo")).thenReturn(repo);
        when(repo.basePath()).thenReturn(new BlobPath());
        when(repo.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        // Setup a bundle blob that contains gen 1
        final List<NodeBundleRegistry.FileReport> fileReports = new ArrayList<>();
        fileReports.add(new NodeBundleRegistry.FileReport(1L, true, 2000, 8));
        fileReports.add(new NodeBundleRegistry.FileReport(1L, false, 2500, 8));
        final List<NodeBundleRegistry.ShardReport> shardReports = new ArrayList<>();
        shardReports.add(new NodeBundleRegistry.ShardReport("index-uuid", 0, 1L, 1L, fileReports));
        final byte[] indexBinData = serializeIndexBinHelper("node-1", shardReports);
        final byte[] headerBytes = buildTarHeaderForIndexBin(indexBinData);

        final Map<String, org.opensearch.common.blobstore.BlobMetadata> blobsMap = new HashMap<>();
        blobsMap.put("bundle_1.tar", mock(org.opensearch.common.blobstore.BlobMetadata.class));
        when(blobContainer.listBlobs()).thenReturn(blobsMap);
        doAnswer(invocation -> {
            final long offset = invocation.getArgument(1);
            final long length = invocation.getArgument(2);
            if (offset == 0 && length == 1024) return new ByteArrayInputStream(headerBytes);
            if (offset == 512 && length == indexBinData.length) return new ByteArrayInputStream(indexBinData);
            if (offset == 2000 && length == 8) return new ByteArrayInputStream("gen1-tlg".getBytes(StandardCharsets.UTF_8));
            if (offset == 2500 && length == 8) return new ByteArrayInputStream("gen1-ckp".getBytes(StandardCharsets.UTF_8));
            return new ByteArrayInputStream(new byte[0]);
        }).when(blobContainer).readBlob(anyString(), anyLong(), anyLong());

        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(
            null,
            () -> reposService,
            clusterService,
            mock(Client.class)
        );
        final Path tempDir = createTempDir();
        final ShardId shardId = new ShardId(new Index("test-index", "index-uuid"), 0);

        final Map<String, String> genToPrimaryTerm = new HashMap<>();
        genToPrimaryTerm.put("1", "1");
        strategy.downloadRange(shardId, 1L, 1L, genToPrimaryTerm, tempDir);

        assertEquals("gen1-tlg", new String(Files.readAllBytes(tempDir.resolve("translog-1.tlog")), StandardCharsets.UTF_8));
        assertEquals("gen1-ckp", new String(Files.readAllBytes(tempDir.resolve("translog-1.ckp")), StandardCharsets.UTF_8));
    }

    public void testResolveStaleTranslogBlobsReturnsSkip() {
        // TarTranslogUploadStrategy must return SKIP so core never issues per-generation
        // DELETE calls against paths that don't exist (bundles contain multiple shards/gens).
        final TarTranslogUploadStrategy strategy = new TarTranslogUploadStrategy(
            null,
            () -> mock(RepositoriesService.class),
            mock(ClusterService.class),
            mock(Client.class)
        );
        assertEquals(RemoteStoreTranslogStrategy.GcDecision.SKIP, strategy.resolveStaleTranslogBlobs());
    }

    // --- helpers ---

    private ClusterService mockClusterServiceWithMaster() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getClusterManagerNode()).thenReturn(mock(DiscoveryNode.class));

        final IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", org.opensearch.Version.CURRENT)
                    .put("index.remote_store.enabled", true)
                    .put("index.remote_store.translog.repository", "mock-repo")
            )
            .build();
        when(metadata.iterator()).thenReturn(Collections.singletonList(indexMetadata).iterator());
        return clusterService;
    }

    private byte[] buildTarHeaderForIndexBin(final byte[] indexBinData) {
        final byte[] headerBytes = new byte[1024];
        System.arraycopy("index.bin".getBytes(StandardCharsets.UTF_8), 0, headerBytes, 0, 9);
        final String octalSizeStr = String.format(Locale.ROOT, "%011o", indexBinData.length);
        System.arraycopy(octalSizeStr.getBytes(StandardCharsets.UTF_8), 0, headerBytes, 124, 11);
        return headerBytes;
    }

    private byte[] serializeIndexBinHelper(final String nodeId, final List<NodeBundleRegistry.ShardReport> shardReports)
        throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeBytes("TTRI");
            dos.writeByte(1); // Version 1
            final byte[] nodeBytes = nodeId.getBytes(StandardCharsets.UTF_8);
            dos.writeShort(nodeBytes.length);
            dos.write(nodeBytes);

            dos.writeShort(shardReports.size());
            for (final NodeBundleRegistry.ShardReport shard : shardReports) {
                final byte[] uuidBytes = shard.indexUuid.getBytes(StandardCharsets.UTF_8);
                dos.writeShort(uuidBytes.length);
                dos.write(uuidBytes);
                dos.writeInt(shard.shardId);
                dos.writeLong(shard.primaryTerm);
                dos.writeLong(shard.minRemoteGenReferenced);

                dos.writeShort(shard.files.size());
                for (final NodeBundleRegistry.FileReport file : shard.files) {
                    final String name = "translog-" + file.generation + (file.isTlg ? ".tlog" : ".ckp");
                    final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                    dos.writeShort(nameBytes.length);
                    dos.write(nameBytes);
                    dos.writeLong(file.generation);
                    dos.writeLong(file.offset);
                    dos.writeLong(file.length);
                }
            }
        }
        return baos.toByteArray();
    }
}
