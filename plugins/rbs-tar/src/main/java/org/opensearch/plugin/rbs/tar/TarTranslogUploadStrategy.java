/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.translog.transfer.RemoteStoreTranslogStrategy;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class TarTranslogUploadStrategy implements RemoteStoreTranslogStrategy {
    private static final Logger logger = LogManager.getLogger(TarTranslogUploadStrategy.class);

    private final NodeTranslogUploadQueue queue;
    private final Supplier<RepositoriesService> reposSupplier;
    private final ClusterService clusterService;
    private final Client client;

    public TarTranslogUploadStrategy(
        final NodeTranslogUploadQueue queue,
        final Supplier<RepositoriesService> reposSupplier,
        final ClusterService clusterService,
        final Client client
    ) {
        this.queue = queue;
        this.reposSupplier = reposSupplier;
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    public boolean transferSnapshot(
        final ShardId shardId,
        final TransferSnapshot transferSnapshot,
        final TranslogTransferListener listener,
        final CryptoMetadata cryptoMetadata
    ) throws IOException {
        try {
            final CompletableFuture<Void> future = queue.enqueue(shardId, transferSnapshot);
            future.get(120, TimeUnit.SECONDS);
            listener.onUploadComplete(transferSnapshot);
            return true;
        } catch (final Exception e) {
            listener.onUploadFailed(transferSnapshot, e);
            return false;
        }
    }

    @Override
    public RemoteStoreTranslogStrategy.GcDecision resolveStaleTranslogBlobs() {
        // TAR bundles store multiple shards' generations in a single blob — per-generation
        // DELETE paths don't exist. Return SKIP so core never issues those 404-producing calls.
        // GC is owned entirely by NodeBundleRegistry.runGarbageCollection() on the master.
        return RemoteStoreTranslogStrategy.GcDecision.SKIP;
    }

    @Override
    public void downloadRange(
        final ShardId shardId,
        final long minGeneration,
        final long maxGeneration,
        final Map<String, String> generationToPrimaryTerm,
        final Path location
    ) throws IOException {
        final DiscoveryNode masterNode = clusterService.state().nodes().getClusterManagerNode();
        final String repoName = masterNode != null ? NodeBundleRegistry.findRemoteTranslogRepository(clusterService) : null;

        Files.createDirectories(location);

        for (long gen = minGeneration; gen <= maxGeneration; gen++) {
            if (generationToPrimaryTerm.containsKey(Long.toString(gen)) == false) {
                continue;
            }

            List<NodeBundleRegistry.FileLocation> locations = null;
            if (repoName != null) {
                try {
                    final PlainActionFuture<GetTranslogLocationResponse> future = PlainActionFuture.newFuture();
                    client.execute(
                        GetTranslogLocationAction.INSTANCE,
                        new GetTranslogLocationRequest(shardId.getIndex().getUUID(), shardId.getId(), gen),
                        future
                    );
                    locations = future.actionGet(30, TimeUnit.SECONDS).getLocations();
                } catch (final Exception e) {
                    logger.warn("downloadRange: failed to get location for gen {} from master, falling back to self-healing", gen, e);
                }
            }

            if (locations != null && locations.isEmpty() == false) {
                // Fast path: registry has exact byte offsets — issue targeted range-GETs
                final BlobStoreRepository repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);
                for (final NodeBundleRegistry.FileLocation loc : locations) {
                    final String fileName = "translog-" + gen + (loc.isTlg ? ".tlog" : ".ckp");
                    final String[] parts = loc.bundlePath.split("/");
                    BlobPath blobPath = repo.basePath();
                    for (int i = 0; i < parts.length - 1; i++) {
                        blobPath = blobPath.add(parts[i]);
                    }
                    final BlobContainer container = repo.blobStore().blobContainer(blobPath);
                    try (InputStream is = container.readBlob(parts[parts.length - 1], loc.offset, loc.length)) {
                        Files.copy(is, location.resolve(fileName), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            } else if (locations != null && locations.isEmpty()) {
                // Master knows about this shard but has no locations — generation is missing
                throw new FileNotFoundException("Translog generation " + gen + " not found in remote store");
            } else {
                // No master or transport failure — self-heal by scanning bundle blobs
                logger.info("downloadRange: no master info for gen {}, starting self-healing scan", gen);
                runSelfHealingRecovery(shardId, gen, location);
            }
        }
    }

    private void runSelfHealingRecovery(final ShardId shardId, final long gen, final Path location) throws IOException {
        final List<String> nodeIds = new java.util.ArrayList<>();
        nodeIds.add(clusterService.localNode().getId());
        if (clusterService.state() != null && clusterService.state().nodes() != null) {
            clusterService.state().nodes().forEach(node -> {
                if (nodeIds.contains(node.getId()) == false) {
                    nodeIds.add(node.getId());
                }
            });
        }
        logger.debug("runSelfHealingRecovery for gen {}: nodeIds={}", gen, nodeIds);

        final String repoName = NodeBundleRegistry.findRemoteTranslogRepository(clusterService);
        final BlobStoreRepository repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);

        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        boolean foundTlg = false;
        boolean foundCkp = false;

        for (int i = 0; i < 5; i++) {
            final ZonedDateTime dt = now.minusMinutes(i);
            final String timePrefix = String.format(
                Locale.ROOT,
                "year_%04d/month_%02d/day_%02d/hour_%02d/minute_%02d",
                dt.getYear(),
                dt.getMonthValue(),
                dt.getDayOfMonth(),
                dt.getHour(),
                dt.getMinute()
            );

            for (final String nodeId : nodeIds) {
                BlobPath bundleDir = repo.basePath().add("txlog_node_bundles").add(nodeId);
                for (final String part : timePrefix.split("/")) {
                    bundleDir = bundleDir.add(part);
                }
                final BlobContainer bundleContainer = repo.blobStore().blobContainer(bundleDir);

                final Map<String, BlobMetadata> blobs;
                try {
                    blobs = bundleContainer.listBlobs();
                    logger.debug("runSelfHealingRecovery listed blobs in dir {}: count={}", bundleDir.buildAsString(), blobs.size());
                } catch (final IOException e) {
                    continue;
                }

                for (final String name : blobs.keySet()) {
                    if (name.startsWith("bundle_") && name.endsWith(".tar")) {
                        try {
                            final byte[] headerBytes = new byte[1024];
                            try (InputStream is = bundleContainer.readBlob(name, 0, 1024)) {
                                int read = 0;
                                while (read < headerBytes.length) {
                                    final int r = is.read(headerBytes, read, headerBytes.length - read);
                                    if (r == -1) break;
                                    read += r;
                                }
                            }

                            final long entrySize = parseEntrySizeFromHeader(headerBytes);
                            logger.debug("runSelfHealingRecovery parsed bundle {} entrySize={}", name, entrySize);
                            if (entrySize > 0) {
                                final byte[] indexBinBytes = new byte[(int) entrySize];
                                try (InputStream is = bundleContainer.readBlob(name, 512, entrySize)) {
                                    int readPayload = 0;
                                    while (readPayload < indexBinBytes.length) {
                                        final int r = is.read(indexBinBytes, readPayload, indexBinBytes.length - readPayload);
                                        if (r == -1) break;
                                        readPayload += r;
                                    }
                                }

                                final List<NodeBundleRegistry.ShardReport> shards = deserializeIndexBin(indexBinBytes);
                                for (final NodeBundleRegistry.ShardReport shard : shards) {
                                    if (shard.indexUuid.equals(shardId.getIndex().getUUID()) && shard.shardId == shardId.getId()) {
                                        logger.debug(
                                            "runSelfHealingRecovery found matching shard in bundle {}: UUID={}, shardId={}",
                                            name,
                                            shard.indexUuid,
                                            shard.shardId
                                        );
                                        for (final NodeBundleRegistry.FileReport file : shard.files) {
                                            if (file.generation == gen) {
                                                final String fileName = "translog-" + gen + (file.isTlg ? ".tlog" : ".ckp");
                                                final Path filePath = location.resolve(fileName);
                                                Files.createDirectories(location);
                                                try (InputStream is = bundleContainer.readBlob(name, file.offset, file.length)) {
                                                    Files.copy(is, filePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                                                }
                                                logger.debug(
                                                    "runSelfHealingRecovery successfully downloaded {} from bundle {} at offset {}, length {}",
                                                    fileName,
                                                    name,
                                                    file.offset,
                                                    file.length
                                                );
                                                if (file.isTlg) foundTlg = true;
                                                else foundCkp = true;
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (final Exception e) {
                            logger.warn("Exception in runSelfHealingRecovery scanning bundle " + name, e);
                        }
                    }
                }
            }

            if (foundTlg && foundCkp) {
                logger.info("Self-healing recovered generation {} successfully", gen);
                return;
            }
        }

        if (!foundTlg || !foundCkp) {
            throw new IOException("Failed to self-heal recover translog files for generation " + gen);
        }
    }

    private long parseEntrySizeFromHeader(final byte[] header) {
        if (header[124] == (byte) 0x80) { // Base-256
            long size = 0;
            for (int i = 1; i < 12; i++) {
                size = (size << 8) + (header[124 + i] & 0xFF);
            }
            return size;
        } else { // Octal ASCII
            long size = 0;
            int start = 124;
            while (start < 124 + 12 && header[start] == ' ') {
                start++;
            }
            for (int i = start; i < 124 + 12; i++) {
                final byte b = header[i];
                if (b == 0 || b == ' ') break;
                size = (size * 8) + (b - '0');
            }
            return size;
        }
    }

    private List<NodeBundleRegistry.ShardReport> deserializeIndexBin(final byte[] data) throws IOException {
        final List<NodeBundleRegistry.ShardReport> reports = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            final byte[] magicBytes = new byte[4];
            dis.readFully(magicBytes);
            final String magic = new String(magicBytes, "US-ASCII");
            if (magic.equals("TTRI") == false) {
                throw new IOException("Invalid index.bin magic: " + magic);
            }
            final byte version = dis.readByte();
            final short nodeIdLen = dis.readShort();
            final byte[] nodeIdBytes = new byte[nodeIdLen];
            dis.readFully(nodeIdBytes);

            final int shardCount = dis.readUnsignedShort();
            for (int i = 0; i < shardCount; i++) {
                final short indexUuidLen = dis.readShort();
                final byte[] indexUuidBytes = new byte[indexUuidLen];
                dis.readFully(indexUuidBytes);
                final String indexUuid = new String(indexUuidBytes, "UTF-8");
                final int shardId = dis.readInt();
                final long primaryTerm = dis.readLong();
                final long minRemoteGenReferenced = dis.readLong();

                final int fileCount = dis.readUnsignedShort();
                final List<NodeBundleRegistry.FileReport> files = new ArrayList<>();
                for (int j = 0; j < fileCount; j++) {
                    final short nameLen = dis.readShort();
                    final byte[] nameBytes = new byte[nameLen];
                    dis.readFully(nameBytes);
                    final String name = new String(nameBytes, "UTF-8");
                    final long gen = dis.readLong();
                    final long offset = dis.readLong();
                    final long length = dis.readLong();
                    final boolean isTlg = name.endsWith(".tlog");
                    files.add(new NodeBundleRegistry.FileReport(gen, isTlg, offset, length));
                }

                final NodeBundleRegistry.ShardReport sr = new NodeBundleRegistry.ShardReport(
                    indexUuid,
                    shardId,
                    primaryTerm,
                    minRemoteGenReferenced,
                    files
                );
                reports.add(sr);
            }
        }
        return reports;
    }

    private boolean runSelfHealingRecoveryForAllGenerations(final ShardId shardId, final Path location) throws IOException {
        final List<String> nodeIds = new java.util.ArrayList<>();
        nodeIds.add(clusterService.localNode().getId());
        if (clusterService.state() != null && clusterService.state().nodes() != null) {
            clusterService.state().nodes().forEach(node -> {
                if (nodeIds.contains(node.getId()) == false) {
                    nodeIds.add(node.getId());
                }
            });
        }
        logger.debug("runSelfHealingRecoveryForAllGenerations: nodeIds={}", nodeIds);

        final String repoName = NodeBundleRegistry.findRemoteTranslogRepository(clusterService);
        final BlobStoreRepository repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);

        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        final List<DownloadTask> tasks = new ArrayList<>();
        long maxGen = -1;

        for (int i = 0; i < 15; i++) {
            final ZonedDateTime dt = now.minusMinutes(i);
            final String timePrefix = String.format(
                Locale.ROOT,
                "year_%04d/month_%02d/day_%02d/hour_%02d/minute_%02d",
                dt.getYear(),
                dt.getMonthValue(),
                dt.getDayOfMonth(),
                dt.getHour(),
                dt.getMinute()
            );

            for (final String nodeId : nodeIds) {
                BlobPath bundleDir = repo.basePath().add("txlog_node_bundles").add(nodeId);
                for (final String part : timePrefix.split("/")) {
                    bundleDir = bundleDir.add(part);
                }
                final BlobContainer bundleContainer = repo.blobStore().blobContainer(bundleDir);

                final Map<String, BlobMetadata> blobs;
                try {
                    blobs = bundleContainer.listBlobs();
                    logger.debug(
                        "runSelfHealingRecoveryForAllGenerations listed blobs in dir {}: count={}",
                        bundleDir.buildAsString(),
                        blobs.size()
                    );
                } catch (final IOException e) {
                    continue;
                }

                for (final String name : blobs.keySet()) {
                    if (name.startsWith("bundle_") && name.endsWith(".tar")) {
                        try {
                            final byte[] headerBytes = new byte[1024];
                            try (InputStream is = bundleContainer.readBlob(name, 0, 1024)) {
                                int read = 0;
                                while (read < headerBytes.length) {
                                    final int r = is.read(headerBytes, read, headerBytes.length - read);
                                    if (r == -1) break;
                                    read += r;
                                }
                            }

                            final long entrySize = parseEntrySizeFromHeader(headerBytes);
                            logger.debug("runSelfHealingRecoveryForAllGenerations parsed bundle {} entrySize={}", name, entrySize);
                            if (entrySize > 0) {
                                final byte[] indexBinBytes = new byte[(int) entrySize];
                                try (InputStream is = bundleContainer.readBlob(name, 512, entrySize)) {
                                    int readPayload = 0;
                                    while (readPayload < indexBinBytes.length) {
                                        final int r = is.read(indexBinBytes, readPayload, indexBinBytes.length - readPayload);
                                        if (r == -1) break;
                                        readPayload += r;
                                    }
                                }

                                final List<NodeBundleRegistry.ShardReport> shards = deserializeIndexBin(indexBinBytes);
                                for (final NodeBundleRegistry.ShardReport shard : shards) {
                                    if (shard.indexUuid.equals(shardId.getIndex().getUUID()) && shard.shardId == shardId.getId()) {
                                        logger.debug(
                                            "runSelfHealingRecoveryForAllGenerations found matching shard in bundle {}: UUID={}, shardId={}",
                                            name,
                                            shard.indexUuid,
                                            shard.shardId
                                        );
                                        for (final NodeBundleRegistry.FileReport file : shard.files) {
                                            final String fileName = "translog-" + file.generation + (file.isTlg ? ".tlog" : ".ckp");
                                            final Path filePath = location.resolve(fileName);
                                            if (file.generation > maxGen) {
                                                maxGen = file.generation;
                                            }
                                            tasks.add(new DownloadTask(bundleContainer, name, file.offset, file.length, filePath));
                                            logger.debug(
                                                "runSelfHealingRecoveryForAllGenerations added download task for {} from bundle {} at offset {}, length {}",
                                                fileName,
                                                name,
                                                file.offset,
                                                file.length
                                            );
                                        }
                                    }
                                }
                            }
                        } catch (final Exception e) {
                            logger.warn("Exception in runSelfHealingRecoveryForAllGenerations scanning bundle " + name, e);
                        }
                    }
                }
            }
        }

        if (maxGen != -1) {
            if (Files.exists(location)) {
                FileSystemUtils.deleteSubDirectories(location);
                for (final Path file : FileSystemUtils.files(location)) {
                    Files.delete(file);
                }
            } else {
                Files.createDirectories(location);
            }

            for (final DownloadTask task : tasks) {
                try (InputStream is = task.container.readBlob(task.blobName, task.offset, task.length)) {
                    Files.copy(is, task.filePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }

            final String commitCkpName = "translog-" + maxGen + ".ckp";
            if (Files.exists(location.resolve(commitCkpName))) {
                Files.copy(
                    location.resolve(commitCkpName),
                    location.resolve("translog.ckp"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING
                );
            }
            return true;
        }
        return false;
    }

    private static final class DownloadTask {
        final BlobContainer container;
        final String blobName;
        final long offset;
        final long length;
        final Path filePath;

        DownloadTask(BlobContainer container, String blobName, long offset, long length, Path filePath) {
            this.container = container;
            this.blobName = blobName;
            this.offset = offset;
            this.length = length;
            this.filePath = filePath;
        }
    }
}
