# RFC: Pluggable Remote Store Strategies

## Motivation

OpenSearch Remote-Backed Storage (RBS) uploads every Lucene segment file and every translog
generation individually to object storage. Under high-throughput indexing this produces a very
large number of PUT and LIST API calls — the primary contributor to S3 cost.

A typical production cluster (200 shards, 1-second refresh) generates roughly:

| Operation | Requests/month |
|-----------|---------------|
| Segment PUTs | ~2.6 billion |
| Translog PUTs | ~2.6 billion |
| **Total S3 API cost** | **~$26,000 / month** |

The upload logic is currently inlined in `RemoteStoreUploaderService` and `RemoteFsTranslog`.
Optimising it requires invasive changes to core engine code and cannot be deployed as a plugin
or enabled per-index without forking OpenSearch.

## Proposed Solution

Introduce two strategy interfaces in OpenSearch core and expose them through the existing plugin
framework. Custom strategies can then control how segment and translog data are physically stored,
downloaded, and garbage-collected — without any changes to core.

### Extension Points

#### 1. `RemoteStorePlugin` — plugin registration

```java
@ExperimentalApi
public interface RemoteStorePlugin {
    // Return a map of strategy-name → implementation.
    // "default" is always registered by core; plugins add additional keys.
    default Map<String, RemoteStoreSegmentStrategy>  getRemoteStoreSegmentStrategies()  { … }
    default Map<String, RemoteStoreTranslogStrategy> getRemoteStoreTranslogStrategies() { … }
}
```

#### 2. `RemoteStoreSegmentStrategy` — segment lifecycle

```java
@ExperimentalApi
public interface RemoteStoreSegmentStrategy {

    // Upload segment files for one refresh cycle.
    // UploadContext carries the file list, local Directory, checkpoint, and crypto config.
    // UploadListener receives per-file and batch success/failure callbacks.
    void upload(RemoteSegmentStoreDirectory dir, ShardId shard,
                UploadContext ctx, UploadListener listener) throws IOException;

    // Upload the metadata blob after a successful upload().
    // Default is a no-op — strategies that embed metadata inside the data blob (e.g. TAR
    // index.bin) do not need a separate metadata file.
    default void uploadMetadata(RemoteSegmentStoreDirectory dir, ShardId shard,
                                MetadataUploadContext ctx) throws IOException {}

    // Delete segment files no longer referenced by any active commit.
    void deleteStaleSegments(RemoteSegmentStoreDirectory dir, ShardId shard,
                             int minCommitsToKeep) throws IOException;

    void deleteFile(RemoteSegmentStoreDirectory dir, ShardId shard, String name) throws IOException;

    // Return a reader that can discover and parse segment metadata.
    // Default strategy uses the metadata__<term>_<gen>_<nodeId> naming convention.
    // TAR strategy reads index.bin from the start of each bundle via a single range-GET.
    MetadataReader getMetadataReader(RemoteSegmentStoreDirectory dir, ShardId shard);
}
```

`RemoteSegmentFile` is a companion interface that abstracts range reads on a remote segment file,
decoupling the core from Lucene's `IndexInput` so strategies can implement range-GETs directly
against their own storage layout (e.g. a byte slice inside a TAR blob).

#### 3. `RemoteStoreTranslogStrategy` — translog lifecycle

```java
@ExperimentalApi
public interface RemoteStoreTranslogStrategy {

    // Upload one translog snapshot. Returns true on success.
    boolean transferSnapshot(ShardId shard, TransferSnapshot snapshot,
                             TranslogTransferListener listener,
                             CryptoMetadata crypto) throws IOException;

    // Download all generations in [minGen..maxGen] to `location`.
    // Batch strategies (e.g. TAR) perform one master round-trip then issue targeted
    // range-GETs. Per-file strategies loop over generationToPrimaryTerm and call
    // downloadTranslog() once per generation.
    void downloadRange(ShardId shard, long minGen, long maxGen,
                       Map<String, String> generationToPrimaryTerm,
                       Path location) throws IOException;

    // Controls whether core's per-generation remote DELETE loop runs.
    // Return SKIP when generations are stored in shared bundles — the DELETE paths
    // do not exist, so core would flood S3 with 404-producing requests on every flush.
    // Plugin owns GC when SKIP is returned.
    default GcDecision resolveStaleTranslogBlobs() { return GcDecision.USE_DEFAULT; }

    // Fallback chain tried when downloadRange() cannot satisfy all generations.
    // Enables safe rolling upgrades: declare DefaultRemoteStoreTranslogStrategy here
    // so blobs written by the old strategy remain recoverable after a strategy switch.
    default List<RemoteStoreTranslogStrategy> fallbackStrategies() { return emptyList(); }

    enum GcDecision { SKIP, USE_DEFAULT }
}
```

### Settings

Two new index-level settings select the active strategy per index:

```
index.remote_store.segment.strategy   (default: "default")
index.remote_store.translog.strategy  (default: "default")
```

### Lifecycle

1. **Node startup** — `IndicesService` collects strategy maps from all plugins and registers
   the built-in `"default"` implementations.
2. **Index open** — `IndexModule` / `IndexService` resolve the strategy names from index settings
   against the registry and inject singleton instances into each shard.
3. **Upload** — `RemoteStoreUploaderService` calls `strategy.upload()` + `strategy.uploadMetadata()`
   on every refresh; `RemoteFsTranslog` calls `strategy.transferSnapshot()` on every sync.
4. **Recovery** — `RemoteFsTranslog.downloadOnce()` calls `strategy.downloadRange()`, then tries
   each strategy in `fallbackStrategies()` for any generations not yet found.
5. **GC** — `trimUnreferencedReaders()` and `deleteStaleRemotePrimaryTerms()` check
   `resolveStaleTranslogBlobs()` before issuing remote DELETEs.

## Reference Implementation: `rbs-tar` Plugin

The `plugins/rbs-tar` module ships two strategy implementations that demonstrate the full power
of the extension points.

### `TarSegmentUploadStrategy`

Packs all segment files for one refresh into a single TAR blob. The first TAR entry is always
`index.bin` — a compact binary manifest mapping each file name to its byte offset and length
inside the archive. This means:

- **1 PUT per refresh** instead of N (80–90% reduction).
- **Reads** resolve to a single S3 Range GET per file, addressed by offset + length from `index.bin`.
- **GC** reads `index.bin` only (skipping `segments_N`) via `readIndexBinOnly()`, saving one
  range-GET per active bundle per flush.

### `TarTranslogUploadStrategy`

Batches translog snapshots from all shards on a node into a single TAR bundle per flush window
via `NodeTranslogUploadQueue`. Key behaviours:

- **`transferSnapshot()`** enqueues the snapshot; a worker thread drains the queue and uploads
  one bundle covering all shards' pending generations.
- **`downloadRange()`** fetches all `FileLocation` entries from the master `NodeBundleRegistry`
  in one round-trip, then issues one range-GET per TAR file — no per-generation master calls.
- **`resolveStaleTranslogBlobs()`** returns `GcDecision.SKIP`; GC is owned by
  `NodeBundleRegistry.runGarbageCollection()` using `min(minRemoteGenReferenced, globalCheckpoint+1)`
  as the safe deletion threshold, ensuring replicas are not starved.
- **`fallbackStrategies()`** declares `DefaultRemoteStoreTranslogStrategy` so per-file blobs
  written before a strategy switch remain recoverable.

### Cost Impact

| Metric | Default | TAR bundle |
|--------|---------|-----------|
| Segment PUTs/month | ~2.6 B | ~290 M (−89%) |
| Translog PUTs/month | ~2.6 B | ~52 M (−98%) |
| **Total API cost** | **~$26,000** | **~$1,700** |

## Alternatives Considered

### 1. Hardcoding optimisations in OpenSearch core

Implement TAR bundling or similar compression directly in the core engine. Rejected because it
couples a specific format to the release cycle, blocks community experimentation, and makes the
core significantly harder to test and maintain.

### 2. Custom BlobStore / storage gateway layer

Intercept writes at the `BlobStore` level (e.g. an S3-backed FUSE mount or a custom repository
plugin). Rejected because the BlobStore layer has no visibility into Lucene refresh boundaries or
primary term checkpoints — it cannot know which files belong to a single flush batch or when it is
safe to bundle them.

### 3. External key-value store for translog (Valkey / Redis)

Store translog generations in an in-memory KV store instead of S3. Rejected because RAM storage
is orders of magnitude more expensive than S3, async replication cannot match S3's 11-nines
durability, and it introduces an external operational dependency.

### 4. Shared filesystem (EFS / NFS)

Mount a shared network filesystem as the translog store. Rejected because EFS costs ~$0.30/GB-month
vs S3's ~$0.023/GB-month, NFS mounts introduce locking and reliability risks under network
partitions, and throughput-provisioning costs scale with write volume.

### 5. Pull-based ingestion (no-op translog)

A fundamentally different architecture where indexing nodes do not maintain a translog at all.
Instead, documents are ingested from a durable upstream log (e.g. Kafka) and segments are rebuilt
on recovery by re-replaying the log from a known checkpoint.

This would eliminate translog PUT costs entirely and simplify the recovery path significantly.
However, it is not viable as a near-term solution for two reasons:

1. **Segment upload cost remains.** Every refresh still produces segment files that must be pushed
   to remote store. The per-file PUT cost (the larger of the two cost drivers at scale) is
   unchanged by removing the translog.
2. **Not universally applicable.** Pull-based ingestion requires all data to flow through an
   ordered, replayable upstream log. The majority of OpenSearch deployments use direct REST
   indexing (`_bulk`, `_index`) with no upstream log, making this an opt-in architecture shift
   rather than a general solution. Migrating existing clusters would be a significant operational
   undertaking with no clear migration path for already-indexed data.

The pluggable strategy approach is orthogonal to pull-based ingestion: clusters that do adopt
pull-based ingestion could implement a `RemoteStoreTranslogStrategy` that is a no-op (or
returns immediately from `transferSnapshot()`), while still benefiting from `TarSegmentUploadStrategy`
to address segment costs.

## Open Questions

- **Dynamic strategy switching**: switching `index.remote_store.*.strategy` on a live index
  requires careful handling of mixed-layout shards. The `fallbackStrategies()` chain covers
  recovery, but segment GC across mixed layouts needs further design.
- **Settings validation**: the cluster coordinator should verify that all nodes have the named
  strategy plugin loaded before approving a settings change.
- **Metrics**: per-strategy upload latency, bundle size distribution, and GC cycle counters
  should be exposed via the existing remote store stats API.
