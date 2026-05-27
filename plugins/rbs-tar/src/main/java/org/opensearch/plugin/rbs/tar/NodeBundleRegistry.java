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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * NodeBundleRegistry manages the active translog bundles in the cluster.
 * It uses a flat parallel-array layout to minimize memory footprint.
 */
public final class NodeBundleRegistry {
    private static final Logger logger = LogManager.getLogger(NodeBundleRegistry.class);
    private static final String MAGIC_INDEX = "IDXI";
    private static final byte VERSION = 1;

    // Dictionaries for mapping strings to short/int tokens to optimize heap
    private final List<String> indexUuidTable = new ArrayList<>();
    private final Map<String, Short> indexUuidToToken = new HashMap<>();

    private final List<String> nodeIdTable = new ArrayList<>();
    private final Map<String, Short> nodeIdToToken = new HashMap<>();

    private final List<String> bundlePaths = new ArrayList<>();
    private final Map<String, Integer> bundlePathToId = new HashMap<>();

    // Flat parallel arrays for files in bundles
    private int fileCount = 0;
    private int[] fileBundleId = new int[1024];
    private short[] fileIndexToken = new short[1024];
    private int[] fileShardId = new int[1024];
    private long[] filePrimaryTerm = new long[1024];
    private long[] fileGeneration = new long[1024];
    private long[] fileOffset = new long[1024];
    private long[] fileLength = new long[1024];
    private boolean[] fileIsTlg = new boolean[1024];

    // Shard GC thresholds: a bundle is safe to delete only when ALL its generations are below
    // min(minRemoteGenReferenced, globalCheckpoint+1) for every shard it contains.
    private final Map<String, Map<Integer, Long>> shardMinGenMap = new HashMap<>();          // Index UUID -> Shard ID ->
                                                                                             // minRemoteGenReferenced
    private final Map<String, Map<Integer, Long>> shardGlobalCheckpointMap = new HashMap<>(); // Index UUID -> Shard ID -> globalCheckpoint

    private boolean dirty = false;
    private long lastCommitTimestamp = 0;

    public synchronized void registerBundle(
        final String nodeId,
        final String bundlePath,
        final long timestamp,
        final List<ShardReport> shards
    ) {
        final short nodeToken = getOrCreateNodeToken(nodeId);
        final int bundleId = getOrCreateBundleId(bundlePath);

        for (final ShardReport shard : shards) {
            final short indexToken = getOrCreateIndexToken(shard.indexUuid);

            // Record/update the min remote generation referenced
            shardMinGenMap.computeIfAbsent(shard.indexUuid, k -> new HashMap<>()).put(shard.shardId, shard.minRemoteGenReferenced);
            // Record/update the global checkpoint (replication safety boundary)
            if (shard.globalCheckpoint != Long.MAX_VALUE) {
                shardGlobalCheckpointMap.computeIfAbsent(shard.indexUuid, k -> new HashMap<>()).put(shard.shardId, shard.globalCheckpoint);
            }

            for (final FileReport file : shard.files) {
                ensureCapacity(fileCount + 1);
                fileBundleId[fileCount] = bundleId;
                fileIndexToken[fileCount] = indexToken;
                fileShardId[fileCount] = shard.shardId;
                filePrimaryTerm[fileCount] = shard.primaryTerm;
                fileGeneration[fileCount] = file.generation;
                fileOffset[fileCount] = file.offset;
                fileLength[fileCount] = file.length;
                fileIsTlg[fileCount] = file.isTlg;
                fileCount++;
            }
        }
        dirty = true;
    }

    public synchronized int getFileCount() {
        return fileCount;
    }

    public synchronized List<FileLocation> getTranslogLocations(final String indexUuid, final int shardId, final long generation) {
        final Short indexToken = indexUuidToToken.get(indexUuid);
        if (indexToken == null) {
            return Collections.emptyList();
        }

        final List<FileLocation> locations = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            if (fileIndexToken[i] == indexToken.shortValue()
                && fileShardId[i] == shardId
                && (generation == -1 || fileGeneration[i] == generation)) {
                final String bundlePath = bundlePaths.get(fileBundleId[i]);
                locations.add(new FileLocation(fileIsTlg[i], bundlePath, fileOffset[i], fileLength[i], fileGeneration[i]));
            }
        }
        return locations;
    }

    public synchronized void updateShardMinGen(final String indexUuid, final int shardId, final long minGen) {
        shardMinGenMap.computeIfAbsent(indexUuid, k -> new HashMap<>()).put(shardId, minGen);
        dirty = true;
    }

    public synchronized boolean isDirty() {
        return dirty;
    }

    public synchronized void clearDirty() {
        this.dirty = false;
    }

    public synchronized long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    private short getOrCreateNodeToken(final String nodeId) {
        Short token = nodeIdToToken.get(nodeId);
        if (token == null) {
            token = (short) nodeIdTable.size();
            nodeIdTable.add(nodeId);
            nodeIdToToken.put(nodeId, token);
        }
        return token;
    }

    private short getOrCreateIndexToken(final String indexUuid) {
        Short token = indexUuidToToken.get(indexUuid);
        if (token == null) {
            token = (short) indexUuidTable.size();
            indexUuidTable.add(indexUuid);
            indexUuidToToken.put(indexUuid, token);
        }
        return token;
    }

    private int getOrCreateBundleId(final String bundlePath) {
        Integer id = bundlePathToId.get(bundlePath);
        if (id == null) {
            id = bundlePaths.size();
            bundlePaths.add(bundlePath);
            bundlePathToId.put(bundlePath, id);
        }
        return id;
    }

    private void ensureCapacity(final int minCapacity) {
        if (minCapacity > fileBundleId.length) {
            final int newCapacity = Math.max(fileBundleId.length * 2, minCapacity);
            fileBundleId = Arrays.copyOf(fileBundleId, newCapacity);
            fileIndexToken = Arrays.copyOf(fileIndexToken, newCapacity);
            fileShardId = Arrays.copyOf(fileShardId, newCapacity);
            filePrimaryTerm = Arrays.copyOf(filePrimaryTerm, newCapacity);
            fileGeneration = Arrays.copyOf(fileGeneration, newCapacity);
            fileOffset = Arrays.copyOf(fileOffset, newCapacity);
            fileLength = Arrays.copyOf(fileLength, newCapacity);
            fileIsTlg = Arrays.copyOf(fileIsTlg, newCapacity);
        }
    }

    public synchronized void writeTo(final DataOutputStream out, final long term) throws IOException {
        out.writeBytes(MAGIC_INDEX);
        out.writeByte(VERSION);
        out.writeLong(term);

        // Node ID Dict
        out.writeShort(nodeIdTable.size());
        for (final String nodeId : nodeIdTable) {
            out.writeUTF(nodeId);
        }

        // Index UUID Dict
        out.writeShort(indexUuidTable.size());
        for (final String indexUuid : indexUuidTable) {
            out.writeUTF(indexUuid);
        }

        // Bundle Paths Dict
        out.writeInt(bundlePaths.size());
        for (final String path : bundlePaths) {
            out.writeUTF(path);
        }

        // Shard Min Gen Map
        out.writeShort(shardMinGenMap.size());
        for (final Map.Entry<String, Map<Integer, Long>> indexEntry : shardMinGenMap.entrySet()) {
            final short indexToken = getOrCreateIndexToken(indexEntry.getKey());
            out.writeShort(indexToken);
            final Map<Integer, Long> shardMap = indexEntry.getValue();
            out.writeShort(shardMap.size());
            for (final Map.Entry<Integer, Long> shardEntry : shardMap.entrySet()) {
                out.writeInt(shardEntry.getKey());
                out.writeLong(shardEntry.getValue());
            }
        }

        // File Arrays
        out.writeInt(fileCount);
        for (int i = 0; i < fileCount; i++) {
            out.writeInt(fileBundleId[i]);
            out.writeShort(fileIndexToken[i]);
            out.writeInt(fileShardId[i]);
            out.writeLong(filePrimaryTerm[i]);
            out.writeLong(fileGeneration[i]);
            out.writeLong(fileOffset[i]);
            out.writeLong(fileLength[i]);
            out.writeBoolean(fileIsTlg[i]);
        }
    }

    public synchronized void readFrom(final DataInputStream in) throws IOException {
        final byte[] magicBytes = new byte[4];
        in.readFully(magicBytes);
        final String magic = new String(magicBytes, "US-ASCII");
        if (magic.equals(MAGIC_INDEX) == false) {
            throw new IOException("Invalid index magic: " + magic);
        }
        final byte version = in.readByte();
        if (version != VERSION) {
            throw new IOException("Unsupported index version: " + version);
        }
        final long term = in.readLong(); // Raft term

        // Node ID Dict
        nodeIdTable.clear();
        nodeIdToToken.clear();
        final int nodeCount = in.readUnsignedShort();
        for (int i = 0; i < nodeCount; i++) {
            final String nodeId = in.readUTF();
            nodeIdTable.add(nodeId);
            nodeIdToToken.put(nodeId, (short) i);
        }

        // Index UUID Dict
        indexUuidTable.clear();
        indexUuidToToken.clear();
        final int indexCount = in.readUnsignedShort();
        for (int i = 0; i < indexCount; i++) {
            final String indexUuid = in.readUTF();
            indexUuidTable.add(indexUuid);
            indexUuidToToken.put(indexUuid, (short) i);
        }

        // Bundle Paths Dict
        bundlePaths.clear();
        bundlePathToId.clear();
        final int pathCount = in.readInt();
        for (int i = 0; i < pathCount; i++) {
            final String path = in.readUTF();
            bundlePaths.add(path);
            bundlePathToId.put(path, i);
        }

        // Shard Min Gen Map
        shardMinGenMap.clear();
        final int shardMinGenIndexCount = in.readUnsignedShort();
        for (int i = 0; i < shardMinGenIndexCount; i++) {
            final short indexToken = in.readShort();
            final String indexUuid = indexUuidTable.get(indexToken);
            final Map<Integer, Long> shardMap = new HashMap<>();
            final int shardCount = in.readUnsignedShort();
            for (int j = 0; j < shardCount; j++) {
                final int shardId = in.readInt();
                final long minGen = in.readLong();
                shardMap.put(shardId, minGen);
            }
            shardMinGenMap.put(indexUuid, shardMap);
        }

        // File Arrays
        fileCount = in.readInt();
        ensureCapacity(fileCount);
        for (int i = 0; i < fileCount; i++) {
            fileBundleId[i] = in.readInt();
            fileIndexToken[i] = in.readShort();
            fileShardId[i] = in.readInt();
            filePrimaryTerm[i] = in.readLong();
            fileGeneration[i] = in.readLong();
            fileOffset[i] = in.readLong();
            fileLength[i] = in.readLong();
            fileIsTlg[i] = in.readBoolean();
        }
    }

    public synchronized void runGarbageCollection(final Supplier<RepositoriesService> reposSupplier, final ClusterService clusterService) {
        final String repoName = findRemoteTranslogRepository(clusterService);
        if (repoName == null) {
            return;
        }

        final BlobStoreRepository repo;
        try {
            repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);
        } catch (final Exception e) {
            logger.warn("Failed to obtain translog repository during GC", e);
            return;
        }

        final BlobContainer container = repo.blobStore().blobContainer(repo.basePath());

        // Clean up shard maps of deleted indices
        final Set<String> activeIndexUuids = new HashSet<>();
        for (final IndexMetadata indexMetadata : clusterService.state().metadata().indices().values()) {
            activeIndexUuids.add(indexMetadata.getIndexUUID());
        }
        shardMinGenMap.keySet().retainAll(activeIndexUuids);
        shardGlobalCheckpointMap.keySet().retainAll(activeIndexUuids);

        // Find all active bundle IDs and which ones are entirely garbage
        final Set<Integer> activeBundleIds = new HashSet<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            activeBundleIds.add(fileBundleId[i]);
        }

        final Set<Integer> garbageBundleIds = new HashSet<>(activeBundleIds);

        // A bundle is NOT garbage if any of its files have gen >= safeDeleteThreshold.
        // safeDeleteThreshold = min(minRemoteGenReferenced, globalCheckpoint + 1):
        // - minRemoteGenReferenced: primary has flushed below this gen to Lucene
        // - globalCheckpoint + 1: all replicas have acknowledged up to globalCheckpoint
        // Both signals must agree before we consider a generation safe to delete.
        for (int i = 0; i < fileCount; i++) {
            final int bundleId = fileBundleId[i];
            final String indexUuid = indexUuidTable.get(fileIndexToken[i]);
            final int shardId = fileShardId[i];
            final long gen = fileGeneration[i];

            final Map<Integer, Long> shardMap = shardMinGenMap.get(indexUuid);
            final long minGen = (shardMap != null && shardMap.containsKey(shardId)) ? shardMap.get(shardId) : Long.MAX_VALUE;

            final Map<Integer, Long> gcpMap = shardGlobalCheckpointMap.get(indexUuid);
            final long globalCheckpoint = (gcpMap != null && gcpMap.containsKey(shardId)) ? gcpMap.get(shardId) : Long.MAX_VALUE - 1;
            // globalCheckpoint + 1 = first gen that is NOT yet fully replicated
            final long replicationSafeThreshold = globalCheckpoint == Long.MAX_VALUE - 1 ? Long.MAX_VALUE : globalCheckpoint + 1;

            final long safeDeleteThreshold = Math.min(minGen, replicationSafeThreshold);

            if (gen >= safeDeleteThreshold) {
                // File is still needed — bundle must not be deleted
                garbageBundleIds.remove(bundleId);
            }
        }

        if (garbageBundleIds.isEmpty() == false) {
            final List<String> pathsToDelete = new ArrayList<>();
            for (final int garbageId : garbageBundleIds) {
                pathsToDelete.add(bundlePaths.get(garbageId));
            }
            logger.info("Found {} garbage translog bundles to clean up: {}", garbageBundleIds.size(), pathsToDelete);

            try {
                container.deleteBlobsIgnoringIfNotExists(pathsToDelete);
                // Shift elements in flat arrays to remove files from deleted bundles
                removeBundles(garbageBundleIds);
                dirty = true;
            } catch (final IOException e) {
                logger.error("Failed to delete garbage translog bundles from remote store", e);
            }
        }
    }

    private void removeBundles(final Set<Integer> bundleIdsToRemove) {
        int writeIdx = 0;
        for (int readIdx = 0; readIdx < fileCount; readIdx++) {
            if (bundleIdsToRemove.contains(fileBundleId[readIdx])) {
                continue;
            }
            if (writeIdx != readIdx) {
                fileBundleId[writeIdx] = fileBundleId[readIdx];
                fileIndexToken[writeIdx] = fileIndexToken[readIdx];
                fileShardId[writeIdx] = fileShardId[readIdx];
                filePrimaryTerm[writeIdx] = filePrimaryTerm[readIdx];
                fileGeneration[writeIdx] = fileGeneration[readIdx];
                fileOffset[writeIdx] = fileOffset[readIdx];
                fileLength[writeIdx] = fileLength[readIdx];
                fileIsTlg[writeIdx] = fileIsTlg[readIdx];
            }
            writeIdx++;
        }
        fileCount = writeIdx;
    }

    public synchronized void commitIndex(final Supplier<RepositoriesService> reposSupplier, final ClusterService clusterService) {
        if (dirty == false) {
            return;
        }

        final String repoName = findRemoteTranslogRepository(clusterService);
        if (repoName == null) {
            return;
        }

        final BlobStoreRepository repo;
        try {
            repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);
        } catch (final Exception e) {
            logger.warn("Failed to obtain translog repository for commitIndex", e);
            return;
        }

        final long term = clusterService.state().term();
        final long timestamp = System.currentTimeMillis();
        final String fileName = String.format(Locale.ROOT, "index_term_%d_%d.idx", term, timestamp);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writeTo(dos, term);
        } catch (final IOException e) {
            logger.error("Failed to serialize NodeBundleRegistry", e);
            return;
        }

        final byte[] data = baos.toByteArray();
        final BlobPath idxPath = repo.basePath().add("txlog_bundles_indices");
        final BlobContainer container = repo.blobStore().blobContainer(idxPath);

        try {
            container.writeBlob(fileName, new ByteArrayInputStream(data), data.length, true);
            lastCommitTimestamp = timestamp;
            dirty = false;
            logger.info("Successfully committed NodeBundleRegistry index file: {}", fileName);

            // Maintain N-version (last 2 versions) retention model: delete older ones
            final Map<String, BlobMetadata> blobs = container.listBlobs();
            final List<IdxFile> idxFiles = new ArrayList<>();
            for (final String name : blobs.keySet()) {
                if (name.startsWith("index_term_") && name.endsWith(".idx")) {
                    try {
                        final String[] parts = name.substring("index_term_".length(), name.length() - ".idx".length()).split("_");
                        final long fileTerm = Long.parseLong(parts[0]);
                        final long fileTimestamp = Long.parseLong(parts[1]);
                        idxFiles.add(new IdxFile(name, fileTerm, fileTimestamp));
                    } catch (final Exception ex) {
                        // ignore malformed
                    }
                }
            }

            // Sort files by timestamp ascending
            idxFiles.sort(Collections.reverseOrder()); // newest first
            if (idxFiles.size() > 2) {
                final List<String> toDelete = new ArrayList<>();
                for (int i = 2; i < idxFiles.size(); i++) {
                    toDelete.add(idxFiles.get(i).name);
                }
                logger.info("Deleting older registry index files: {}", toDelete);
                container.deleteBlobsIgnoringIfNotExists(toDelete);
            }

        } catch (final IOException e) {
            logger.error("Failed to upload NodeBundleRegistry index to S3", e);
        }
    }

    public synchronized void recoverIndexOnFailover(
        final Supplier<RepositoriesService> reposSupplier,
        final ClusterService clusterService
    ) {
        final String repoName = findRemoteTranslogRepository(clusterService);
        if (repoName == null) {
            logger.info("No remote translog repository found. NodeBundleRegistry starting empty.");
            return;
        }

        final BlobStoreRepository repo;
        try {
            repo = (BlobStoreRepository) reposSupplier.get().repository(repoName);
        } catch (final Exception e) {
            logger.warn("Failed to obtain translog repository for failover recovery", e);
            return;
        }

        final BlobPath idxPath = repo.basePath().add("txlog_bundles_indices");
        final BlobContainer container = repo.blobStore().blobContainer(idxPath);

        final long currentTerm = clusterService.state().term();
        IdxFile newestIdx = null;

        try {
            final Map<String, BlobMetadata> blobs = container.listBlobs();
            for (final String name : blobs.keySet()) {
                if (name.startsWith("index_term_") && name.endsWith(".idx")) {
                    try {
                        final String[] parts = name.substring("index_term_".length(), name.length() - ".idx".length()).split("_");
                        final long fileTerm = Long.parseLong(parts[0]);
                        final long fileTimestamp = Long.parseLong(parts[1]);

                        // Term fencing: accept only files with term <= currentTerm
                        if (fileTerm <= currentTerm) {
                            if (newestIdx == null || fileTimestamp > newestIdx.timestamp) {
                                newestIdx = new IdxFile(name, fileTerm, fileTimestamp);
                            }
                        }
                    } catch (final Exception ex) {
                        // ignore malformed
                    }
                }
            }
        } catch (final IOException e) {
            logger.warn("Failed to list cluster index files from S3. Failover scan may start from empty.", e);
        }

        long lastIdxTime = 0;
        if (newestIdx != null) {
            logger.info("Restoring NodeBundleRegistry from: {}", newestIdx.name);
            try (InputStream is = container.readBlob(newestIdx.name); DataInputStream dis = new DataInputStream(is)) {
                readFrom(dis);
                lastIdxTime = newestIdx.timestamp;
                lastCommitTimestamp = lastIdxTime;
                dirty = false;
            } catch (final IOException e) {
                logger.error("Failed to read NodeBundleRegistry from S3", e);
            }
        }

        // Perform parallel delta scan for the delta window (from lastIdxTime - 1 min to current time + 1 min)
        final long startTime = Math.max(0, lastIdxTime - 60000);
        final long endTime = System.currentTimeMillis() + 60000;
        runDeltaScan(repo, clusterService, startTime, endTime);
    }

    private void runDeltaScan(
        final BlobStoreRepository repo,
        final ClusterService clusterService,
        final long startTimeMs,
        final long endTimeMs
    ) {
        logger.info("Running delta scan on S3 bundles between timestamp {} and {}", startTimeMs, endTimeMs);

        // Generate list of all minute directories in the window
        final List<ZonedDateTime> minutes = new ArrayList<>();
        ZonedDateTime start = Instant.ofEpochMilli(startTimeMs).atZone(ZoneOffset.UTC).withSecond(0).withNano(0);
        final ZonedDateTime end = Instant.ofEpochMilli(endTimeMs).atZone(ZoneOffset.UTC).withSecond(0).withNano(0);
        while (start.isAfter(end) == false) {
            minutes.add(start);
            start = start.plusMinutes(1);
        }

        // Get active nodes in the cluster
        final Set<String> activeNodeIds = new HashSet<>();
        clusterService.state().nodes().forEach(node -> activeNodeIds.add(node.getId()));

        // Scan S3 for each active node and minute directory
        for (final String nodeId : activeNodeIds) {
            for (final ZonedDateTime dt : minutes) {
                final String timePrefix = String.format(
                    Locale.ROOT,
                    "year_%04d/month_%02d/day_%02d/hour_%02d/minute_%02d",
                    dt.getYear(),
                    dt.getMonthValue(),
                    dt.getDayOfMonth(),
                    dt.getHour(),
                    dt.getMinute()
                );
                BlobPath bundleDir = repo.basePath().add("txlog_node_bundles").add(nodeId);
                for (final String part : timePrefix.split("/")) {
                    bundleDir = bundleDir.add(part);
                }
                final BlobContainer bundleContainer = repo.blobStore().blobContainer(bundleDir);

                try {
                    final Map<String, BlobMetadata> blobs = bundleContainer.listBlobs();
                    for (final String name : blobs.keySet()) {
                        if (name.startsWith("bundle_") && name.endsWith(".tar")) {
                            final String fullBundlePath = "txlog_node_bundles/" + nodeId + "/" + timePrefix + "/" + name;
                            if (bundlePathToId.containsKey(fullBundlePath) == false) {
                                // Delta bundle not in the index! Range GET first 1KB of this bundle to retrieve index.bin metadata
                                logger.info("Found delta bundle not in index: {}", fullBundlePath);
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

                                    // Parse the ustar header of the first entry (index.bin)
                                    // Header starts with name (100 bytes), then size at offset 124 (12 bytes)
                                    final long entrySize = parseEntrySizeFromHeader(headerBytes);
                                    if (entrySize > 0) {
                                        // Read the entire index.bin entry payload (which follows the 512-byte header)
                                        final int indexBinOffset = 512;
                                        final byte[] indexBinBytes = new byte[(int) entrySize];
                                        try (InputStream is = bundleContainer.readBlob(name, indexBinOffset, entrySize)) {
                                            int readPayload = 0;
                                            while (readPayload < indexBinBytes.length) {
                                                final int r = is.read(indexBinBytes, readPayload, indexBinBytes.length - readPayload);
                                                if (r == -1) break;
                                                readPayload += r;
                                            }
                                        }

                                        // Deserialize the shards and files details
                                        final List<ShardReport> shards = deserializeIndexBin(indexBinBytes);
                                        registerBundle(nodeId, fullBundlePath, dt.toInstant().toEpochMilli(), shards);
                                        logger.info("Restored delta bundle from S3 header: {}", fullBundlePath);
                                    }
                                } catch (final Exception e) {
                                    logger.error("Failed to parse delta bundle header for " + fullBundlePath, e);
                                }
                            }
                        }
                    }
                } catch (final Exception e) {
                    // ignore if directory doesn't exist
                }
            }
        }
    }

    private long parseEntrySizeFromHeader(final byte[] header) {
        // Size field is at offset 124, length 12
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

    private List<ShardReport> deserializeIndexBin(final byte[] data) throws IOException {
        final List<ShardReport> reports = new ArrayList<>();
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
                final long minRemoteGenReferenced = dis.readLong(); // Added field to report minRemoteGenReferenced

                final int fileCount = dis.readUnsignedShort();
                final List<FileReport> files = new ArrayList<>();
                for (int j = 0; j < fileCount; j++) {
                    final short nameLen = dis.readShort();
                    final byte[] nameBytes = new byte[nameLen];
                    dis.readFully(nameBytes);
                    final String name = new String(nameBytes, "UTF-8");
                    final long gen = dis.readLong();
                    final long offset = dis.readLong();
                    final long length = dis.readLong();
                    final boolean isTlg = name.endsWith(".tlog");
                    files.add(new FileReport(gen, isTlg, offset, length));
                }

                final ShardReport sr = new ShardReport(indexUuid, shardId, primaryTerm, minRemoteGenReferenced, files);
                reports.add(sr);
            }
        }
        return reports;
    }

    public static String findRemoteTranslogRepository(final ClusterService clusterService) {
        for (final IndexMetadata indexMetadata : clusterService.state().metadata()) {
            if (indexMetadata.getSettings().getAsBoolean("index.remote_store.enabled", false)) {
                final String repo = indexMetadata.getSettings().get("index.remote_store.translog.repository");
                if (repo != null) {
                    return repo;
                }
            }
        }
        return null;
    }

    private static final class IdxFile implements Comparable<IdxFile> {
        final String name;
        final long term;
        final long timestamp;

        IdxFile(final String name, final long term, final long timestamp) {
            this.name = name;
            this.term = term;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(final IdxFile o) {
            return Long.compare(this.timestamp, o.timestamp);
        }
    }

    public static final class ShardReport {
        public final String indexUuid;
        public final int shardId;
        public final long primaryTerm;
        public final long minRemoteGenReferenced;
        /**
         * The global checkpoint at upload time — all replicas have acknowledged generations
         * up to this point. A generation is safe to GC only when it is below BOTH
         * {@code minRemoteGenReferenced} (primary flush boundary) AND
         * {@code globalCheckpoint + 1} (replication boundary).
         * Defaults to {@link Long#MAX_VALUE} when unknown (conservative — won't GC early).
         */
        public final long globalCheckpoint;
        public final List<FileReport> files;

        public ShardReport(
            final String indexUuid,
            final int shardId,
            final long primaryTerm,
            final long minRemoteGenReferenced,
            final List<FileReport> files
        ) {
            this(indexUuid, shardId, primaryTerm, minRemoteGenReferenced, Long.MAX_VALUE, files);
        }

        public ShardReport(
            final String indexUuid,
            final int shardId,
            final long primaryTerm,
            final long minRemoteGenReferenced,
            final long globalCheckpoint,
            final List<FileReport> files
        ) {
            this.indexUuid = indexUuid;
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.minRemoteGenReferenced = minRemoteGenReferenced;
            this.globalCheckpoint = globalCheckpoint;
            this.files = files;
        }
    }

    public static final class FileReport {
        public final long generation;
        public final boolean isTlg;
        public final long offset;
        public final long length;

        public FileReport(final long generation, final boolean isTlg, final long offset, final long length) {
            this.generation = generation;
            this.isTlg = isTlg;
            this.offset = offset;
            this.length = length;
        }
    }

    public static final class FileLocation {
        public final boolean isTlg;
        public final String bundlePath;
        public final long offset;
        public final long length;
        public final long generation;

        public FileLocation(final boolean isTlg, final String bundlePath, final long offset, final long length, final long generation) {
            this.isTlg = isTlg;
            this.bundlePath = bundlePath;
            this.offset = offset;
            this.length = length;
            this.generation = generation;
        }
    }
}
