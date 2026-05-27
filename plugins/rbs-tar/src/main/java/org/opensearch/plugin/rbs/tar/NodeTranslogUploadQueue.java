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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.plugin.rbs.RbsTarPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Singleton node-level queue that aggregates translog snapshot transfers across shards,
 * bundles them into tar archives, uploads them, and coordinates master node report ACK.
 */
public final class NodeTranslogUploadQueue extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(NodeTranslogUploadQueue.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Supplier<RepositoriesService> reposSupplier;
    private final Client client;

    private final List<QueueEntry> queue = new ArrayList<>();
    private final Thread workerThread;
    private volatile boolean closed = false;

    public NodeTranslogUploadQueue(
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Supplier<RepositoriesService> reposSupplier,
        final Client client
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.reposSupplier = reposSupplier;
        this.client = client;

        this.workerThread = new Thread(this::runWorker, "rbs-translog-bundler");
        this.workerThread.setDaemon(true);
        this.workerThread.start();
    }

    public CompletableFuture<Void> enqueue(final ShardId shardId, final TransferSnapshot snapshot) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (queue) {
            if (closed) {
                future.completeExceptionally(new IOException("Queue is closed"));
                return future;
            }
            queue.add(new QueueEntry(shardId, snapshot, future));
            queue.notifyAll();
        }
        return future;
    }

    private void runWorker() {
        while (!closed) {
            final List<QueueEntry> batch = new ArrayList<>();
            try {
                synchronized (queue) {
                    while (queue.isEmpty() && !closed) {
                        queue.wait();
                    }
                    if (closed) {
                        break;
                    }

                    // Await threshold or wait time
                    final long maxWaitMs = clusterService.getClusterSettings().get(RbsTarPlugin.BUNDLE_MAX_WAIT_MS_SETTING);
                    final long startTime = System.currentTimeMillis();
                    while (true) {
                        final long elapsed = System.currentTimeMillis() - startTime;
                        final long remaining = maxWaitMs - elapsed;
                        if (remaining <= 0 || isThresholdReached()) {
                            break;
                        }
                        queue.wait(remaining);
                    }

                    batch.addAll(queue);
                    queue.clear();
                }

                if (batch.isEmpty() == false) {
                    processBatch(batch);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                failBatch(batch, e);
                break;
            } catch (final Exception e) {
                logger.error("Exception in translog bundle worker", e);
                failBatch(batch, e);
            }
        }
    }

    private boolean isThresholdReached() {
        final int minShards = clusterService.getClusterSettings().get(RbsTarPlugin.BUNDLE_MIN_SHARDS_SETTING);
        final ByteSizeValue maxBytesSetting = clusterService.getClusterSettings().get(RbsTarPlugin.BUNDLE_MAX_BYTES_SETTING);
        final long maxBytes = maxBytesSetting.getBytes();

        final Set<ShardId> shards = new HashSet<>();
        long totalBytes = 0;
        for (final QueueEntry entry : queue) {
            shards.add(entry.shardId);
            totalBytes += getSnapshotBytes(entry.snapshot);
        }
        return shards.size() >= minShards || totalBytes >= maxBytes;
    }

    private long getSnapshotBytes(final TransferSnapshot snapshot) {
        long bytes = 0;
        try {
            for (final TransferFileSnapshot file : snapshot.getTranslogFileSnapshots()) {
                bytes += file.getContentLength();
            }
            for (final TransferFileSnapshot file : snapshot.getCheckpointFileSnapshots()) {
                bytes += file.getContentLength();
            }
        } catch (final IOException e) {
            // ignore
        }
        return bytes;
    }

    private void processBatch(final List<QueueEntry> batch) {
        try {
            final String nodeId = clusterService.localNode().getId();
            final String bundleUuid = UUIDs.randomBase64UUID();

            final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            final String timePrefix = String.format(
                Locale.ROOT,
                "year_%04d/month_%02d/day_%02d/hour_%02d/minute_%02d",
                now.getYear(),
                now.getMonthValue(),
                now.getDayOfMonth(),
                now.getHour(),
                now.getMinute()
            );
            final String bundlePath = "txlog_node_bundles/" + nodeId + "/" + timePrefix + "/bundle_" + bundleUuid + ".tar";

            // Build structural components to calculate entry sizes
            final List<NodeBundleRegistry.ShardReport> shardReports = new ArrayList<>();
            final Map<TransferFileSnapshot, FileOffsetInfo> fileOffsetMap = new HashMap<>();

            // 1. First Pass: Calculate index.bin size and offsets mathematically
            final long tempIndexBinSize = calculateIndexBinSize(nodeId, batch);
            long currentOffset = 512 + tempIndexBinSize + getPadding(tempIndexBinSize);

            for (final QueueEntry entry : batch) {
                final List<NodeBundleRegistry.FileReport> fileReports = new ArrayList<>();
                final List<TransferFileSnapshot> files = new ArrayList<>();
                files.addAll(entry.snapshot.getTranslogFileSnapshots());
                files.addAll(entry.snapshot.getCheckpointFileSnapshots());

                for (final TransferFileSnapshot file : files) {
                    final long len = file.getContentLength();
                    final long payloadOffset = currentOffset + 512;
                    fileReports.add(
                        new NodeBundleRegistry.FileReport(file.getGeneration(), file.getName().endsWith(".tlog"), payloadOffset, len)
                    );
                    fileOffsetMap.put(file, new FileOffsetInfo(payloadOffset, len));
                    currentOffset = payloadOffset + len + getPadding(len);
                }

                final long minRemoteGenReferenced = entry.snapshot.getTranslogTransferMetadata().getMinTranslogGeneration();
                final long globalCheckpoint = entry.snapshot.getTranslogTransferMetadata().getGlobalCheckpoint();

                shardReports.add(
                    new NodeBundleRegistry.ShardReport(
                        entry.shardId.getIndex().getUUID(),
                        entry.shardId.getId(),
                        entry.snapshot.getTranslogTransferMetadata().getPrimaryTerm(),
                        minRemoteGenReferenced,
                        globalCheckpoint,
                        fileReports
                    )
                );
            }

            // 2. Real serialization of index.bin
            final byte[] indexBinData = serializeIndexBin(nodeId, shardReports);

            // 3. Chain input streams to create the tar on-the-fly without a local temp file
            final List<InputStream> streams = new ArrayList<>();
            // index.bin entry
            streams.add(new ByteArrayInputStream(TarOutputStream.buildHeader("index.bin", indexBinData.length)));
            streams.add(new ByteArrayInputStream(indexBinData));
            final long indexPaddingLen = getPadding(indexBinData.length);
            if (indexPaddingLen > 0) {
                streams.add(new ByteArrayInputStream(new byte[(int) indexPaddingLen]));
            }

            // segment/translog files entries
            for (final QueueEntry entry : batch) {
                final List<TransferFileSnapshot> files = new ArrayList<>();
                files.addAll(entry.snapshot.getTranslogFileSnapshots());
                files.addAll(entry.snapshot.getCheckpointFileSnapshots());

                for (final TransferFileSnapshot file : files) {
                    final long len = file.getContentLength();
                    streams.add(new ByteArrayInputStream(TarOutputStream.buildHeader(file.getName(), len)));
                    streams.add(file.inputStream());
                    final long filePaddingLen = getPadding(len);
                    if (filePaddingLen > 0) {
                        streams.add(new ByteArrayInputStream(new byte[(int) filePaddingLen]));
                    }
                }
            }

            // EOF records (two blocks of 512 bytes)
            streams.add(new ByteArrayInputStream(new byte[1024]));

            final long uploadSize = currentOffset + 1024;

            // 4. S3 Upload
            final String repoName = NodeBundleRegistry.findRemoteTranslogRepository(clusterService);
            if (repoName == null) {
                throw new IOException("No translog repository configured");
            }
            final BlobStoreRepository repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);
            BlobPath basePath = repo.basePath();
            final String[] pathParts = bundlePath.split("/");
            for (int i = 0; i < pathParts.length - 1; i++) {
                basePath = basePath.add(pathParts[i]);
            }
            final BlobContainer container = repo.blobStore().blobContainer(basePath);
            final String targetFileName = pathParts[pathParts.length - 1];

            try (InputStream is = new SequenceInputStream(Collections.enumeration(streams))) {
                container.writeBlob(targetFileName, is, uploadSize, true);
            }

            // 5. TCP Report to Master and await ACK
            final DiscoveryNode masterNode = clusterService.state().nodes().getClusterManagerNode();
            if (masterNode == null) {
                throw new IOException("No elected cluster-manager node found to report translog bundle");
            }

            final NodeBundleReportRequest reportRequest = new NodeBundleReportRequest(
                nodeId,
                bundlePath,
                System.currentTimeMillis(),
                shardReports
            );

            final PlainActionFuture<NodeBundleReportResponse> reportFuture = PlainActionFuture.newFuture();
            client.execute(NodeBundleReportAction.INSTANCE, reportRequest, reportFuture);

            final NodeBundleReportResponse response = reportFuture.actionGet(30, TimeUnit.SECONDS);
            if (response.isAcknowledged()) {
                // Success! Release all futures
                for (final QueueEntry entry : batch) {
                    entry.future.complete(null);
                }
            } else {
                throw new IOException("Master did not acknowledge translog report");
            }

        } catch (final Exception e) {
            logger.error("Failed to bundle and upload translogs", e);
            failBatch(batch, e);
        }
    }

    private void failBatch(final List<QueueEntry> batch, final Exception e) {
        for (final QueueEntry entry : batch) {
            entry.future.completeExceptionally(e);
        }
    }

    private long getPadding(final long size) {
        final long remainder = size % 512;
        return remainder == 0 ? 0 : 512 - remainder;
    }

    private long calculateIndexBinSize(final String nodeId, final List<QueueEntry> batch) throws IOException {
        // Construct a mock byte output to find the exact size of index.bin
        final List<NodeBundleRegistry.ShardReport> mockReports = new ArrayList<>();
        for (final QueueEntry entry : batch) {
            final List<NodeBundleRegistry.FileReport> mockFiles = new ArrayList<>();
            final List<TransferFileSnapshot> files = new ArrayList<>();
            files.addAll(entry.snapshot.getTranslogFileSnapshots());
            files.addAll(entry.snapshot.getCheckpointFileSnapshots());

            for (final TransferFileSnapshot file : files) {
                mockFiles.add(new NodeBundleRegistry.FileReport(file.getGeneration(), file.getName().endsWith(".tlog"), 0, 0));
            }
            mockReports.add(new NodeBundleRegistry.ShardReport(entry.shardId.getIndex().getUUID(), entry.shardId.getId(), 0, 0, mockFiles));
        }
        return serializeIndexBin(nodeId, mockReports).length;
    }

    private byte[] serializeIndexBin(final String nodeId, final List<NodeBundleRegistry.ShardReport> shardReports) throws IOException {
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

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected synchronized void doClose() throws IOException {
        closed = true;
        synchronized (queue) {
            queue.notifyAll();
        }
        try {
            workerThread.join(5000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class QueueEntry {
        final ShardId shardId;
        final TransferSnapshot snapshot;
        final CompletableFuture<Void> future;

        QueueEntry(final ShardId shardId, final TransferSnapshot snapshot, final CompletableFuture<Void> future) {
            this.shardId = shardId;
            this.snapshot = snapshot;
            this.future = future;
        }
    }

    private static final class FileOffsetInfo {
        final long offset;
        final long length;

        FileOffsetInfo(final long offset, final long length) {
            this.offset = offset;
            this.length = length;
        }
    }
}
