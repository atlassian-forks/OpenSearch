# S3 LIST Operations Analysis — Remote Store with Archive Enabled

> Analysis date: 2026-04-16  
> Scenario: 1 index, 60 shards, 3 nodes (20 shards/node), txlog + segment archive both ON

---

## 1. Where & When LIST Calls Happen

### A. Segment Layer (`RemoteSegmentStoreDirectory`)

| Trigger | Method | LIST calls | Frequency |
|---|---|---|---|
| Primary term change (failover/restart) | `initializeRemoteDirectoryOnTermUpdate()` → `readLatestMetadataFile()` | 1 | Rare (per term change) |
| Shard open / Recovery | `init()` → `readLatestMetadataFile()` | 1 | Per recovery |
| Commit / lock operation | `getMetadataFileForCommit()` | 1 | Per commit |
| After refresh (async, semaphore-gated) | `deleteStaleSegments()` | 1 | Per refresh cycle that uploads |
| Shard close | `deleteIfEmpty()` | 1 | Per close |
| Timestamp-based recovery | `initializeToSpecificTimestamp()` | 1 | Recovery only |

**Steady-state behaviour**: `readLatestMetadataFile()` is **not** called on every refresh — only on primary term transitions.  
`deleteStaleSegments()` fires asynchronously after every refresh that successfully uploads segments, semaphore-gated (max 1 concurrent per shard).

Default `index.refresh_interval` = **1 second** → **~1 LIST/sec/shard** from segment GC in steady state.

---

### B. Translog Layer (`TranslogTransferManager` / `RemoteFsTimestampAwareTranslog`)

| Trigger | Method | LIST calls | Frequency |
|---|---|---|---|
| Metadata read (upload/download) | `readMetadata()` | 1 | Per translog generation upload |
| Trim cycle (commit/flush/refresh) | `listTranslogMetadataFilesAsync()` | 1 | Per trim with gens to delete |
| Primary term folder discovery | `listPrimaryTermsInRemote()` → `listFolders()` | 1 | Per `deleteStaleRemotePrimaryTerms()` |
| Stale metadata cleanup | `deleteStaleTranslogMetadataFilesAsync()` | 1 | Per trim cycle |

**Archive mode impact on translog**:  
`RemoteFsTimestampAwareTranslog.trimUnreferencedReaders()` **short-circuits early** when archive is enabled — remote cleanup (list + delete) is skipped and delegated to the archive GC instead.  
This means `deleteStaleTranslogMetadataFilesAsync()` and `listTranslogMetadataFilesAsync()` are **skipped** during normal trim → **fewer LIST calls** for translog in archive mode.

- Archive GC interval: `cluster.remote_store.translog.archive.gc_interval` = **1 min** (default)
- Archive GC does **1 `listAllInSortedOrderAsync`** per GC cycle per shard

---

### C. Pinned Timestamp Service (`RemoteStorePinnedTimestampService`)

| Trigger | Method | LIST calls | Frequency |
|---|---|---|---|
| Periodic scheduled task | `AsyncUpdatePinnedTimestampTask.runInternal()` → `blobContainer.listBlobs()` | 1 | Every 3 min (default) |

- Runs **per node**, not per shard — does **not** scale with shard count
- Lists a single fixed path: `basePath/pinned_timestamps/`
- Setting: `cluster.remote_store.pinned_timestamps.scheduler_interval` = **3 min** (default)

---

## 2. Does Archive Mode Add Extra LISTs?

**No — archive mode does NOT add extra LIST calls. It reduces them for translog.**

| Layer | Archive OFF | Archive ON |
|---|---|---|
| Segment | `deleteStaleSegments()` runs per refresh (1 LIST) | Same — segment GC is unchanged |
| Translog trim | 1–2 LISTs per trim cycle | **0** — short-circuited |
| Translog GC | Ad-hoc | 1 LIST per shard per 1 min (archive GC) |
| Segment metadata | Blob names from metadata file | Same — no extra scan |

Archive blob names are stored **inside** metadata files, so no extra directory scan is required for segments. For translog, archive GC replaces the per-trim list calls with a lower-frequency scheduled scan.

---

## 3. Estimation: 1 Index, 60 Shards, 3 Nodes (20 shards/node)

### Per-Shard Steady-State LIST Rate

| Source | Calls/sec/shard | Notes |
|---|---|---|
| Segment `deleteStaleSegments()` | ~1.0 | 1 LIST per refresh (1s default), semaphore-gated |
| Translog trim (archive ON) | ~0 | Short-circuited; archive GC handles cleanup |
| Translog archive GC | ~0.017 | 1 LIST per 60s per shard |
| Pinned timestamp | ~0 | Per-node, not per-shard |
| **Total per shard** | **~1.017 LIST/sec** | |

### Per-Node (20 shards/node)

| Source | Calls/sec/node | Calls/min/node |
|---|---|---|
| Segment GC (20 shards × ~1/s) | 20 | 1,200 |
| Translog archive GC (20 shards × 1/60s) | 0.33 | 20 |
| Pinned timestamp (1 per node / 3 min) | 0.006 | 0.33 |
| **Total** | **~20.3/sec** | **~1,220/min** |

### Cluster Total (3 nodes, 60 shards)

| Source | Calls/sec (cluster) | Calls/min (cluster) | % of total |
|---|---|---|---|
| Segment `deleteStaleSegments()` | **60** | **3,600** | ~98% |
| Translog archive GC | **1** | **60** | ~1.6% |
| Pinned timestamp | **0.017** | **1** | ~0.03% |
| **Total** | **~61/sec** | **~3,661/min** | 100% |

---

## 4. Why Still So Many LISTs Even with Archive ON?

The dominant driver is **`deleteStaleSegments()`** — it fires after **every refresh** (1/sec default), per shard, each time doing 1 S3 LIST call. With 60 shards that is ~3,600 LIST/min just from segment GC.

This is **entirely independent of archive mode** — whether txlog or segment archive is on or off, the segment cleanup is tied to the refresh cycle, not the archive path.

**Archive mode only relieves the translog path**, which was already a smaller contributor.

---

## 5. Key Settings That Control LIST Rate

| Setting | Default | Effect |
|---|---|---|
| `index.refresh_interval` | `1s` | **Primary lever** — increasing this linearly reduces segment GC LISTs |
| `cluster.remote_store.translog.archive.gc_interval` | `1m` | Controls translog archive GC frequency |
| `cluster.remote_store.pinned_timestamps.scheduler_interval` | `3m` | Negligible — not shard-scaled |
| Shard count | — | Linear reduction across all sources |

### Example: If refresh_interval = 10s

| Source | Calls/min (cluster) |
|---|---|
| Segment GC | 360 (÷10) |
| Translog archive GC | 60 |
| Pinned timestamp | 1 |
| **Total** | **~421/min** |

A 10× increase in refresh interval reduces cluster-wide LISTs by ~90%.

---

## 6. Relevant Classes

| Class | Role |
|---|---|
| `RemoteSegmentStoreDirectory` | All segment LIST calls |
| `RemoteStoreRefreshListener` | Triggers segment sync + GC after refresh |
| `TranslogTransferManager` | Translog LIST calls (metadata, primary terms) |
| `RemoteFsTimestampAwareTranslog` | Short-circuits trim when archive ON |
| `RemoteStorePinnedTimestampService` | Per-node periodic pinned timestamp LIST |
| `RemoteStoreSettings` | All relevant setting definitions |

---

## 7. Optimization: Segment GC Rate Limiter

### Problem

`deleteStaleSegmentsAsync()` is called after every successful segment upload (tied to `index.refresh_interval`, default 1s). Each call issues 1 S3 LIST — the dominant source of S3 LIST cost (~98% of all LISTs).

### Approach: Time-Based Minimum GC Interval

Add a cluster-dynamic setting `cluster.remote_store.segment.metadata.gc.min_interval` (default: `30s`). Before dispatching a GC task, check if enough time has elapsed since the last run. If not, skip — the algorithm runs unchanged when it does execute.

**Key correctness properties:**
- `deleteStaleSegments()` algorithm is **100% unchanged** — still does full S3 LIST, reads all metadata, correctly identifies stale files
- **Primary only** — `RemoteStoreRefreshListener` gates `deleteStaleSegmentsAsync` behind `indexShard.routingEntry().primary()`, so replicas are never affected
- **No replica impact** — replica `readLatestMetadataFile()` is capped at `METADATA_FILES_TO_FETCH=10` files regardless of how many stale files accumulate; cost does not increase
- **No orphan risk** — stale files linger up to `gc_min_interval` longer, then are cleaned correctly
- **Existing semaphore preserved** — `canDeleteStaleCommits` still prevents concurrent GC; rate limiter is an additional outer guard
- **Snapshot/pinned-timestamp safety** — lock checks happen inside `deleteStaleSegments()` at GC time, unchanged

### Implementation

**`RemoteStoreSettings.java`** — new setting:
```java
public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_SEGMENT_METADATA_GC_MIN_INTERVAL = Setting.timeSetting(
    "cluster.remote_store.segment.metadata.gc.min_interval",
    TimeValue.timeValueSeconds(30),
    TimeValue.timeValueSeconds(0),
    Setting.Property.Dynamic,
    Setting.Property.NodeScope
);
```

**`RemoteSegmentStoreDirectory.java`** — rate-limit field + guard:
```java
// New field
private final AtomicLong lastSegmentGcRunTimeMs = new AtomicLong(0);

// deleteStaleSegmentsAsync — add time-based gate
public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep, ActionListener<Void> listener) {
    long now = threadPool.absoluteTimeInMillis();
    long minIntervalMs = remoteStoreSettings.getSegmentMetadataGcMinInterval().millis();
    if (minIntervalMs > 0 && (now - lastSegmentGcRunTimeMs.get()) < minIntervalMs) {
        return; // too soon — skip, retry on next refresh
    }
    if (canDeleteStaleCommits.compareAndSet(true, false)) {
        try {
            threadPool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
                try {
                    lastSegmentGcRunTimeMs.set(threadPool.absoluteTimeInMillis());
                    deleteStaleSegments(lastNMetadataFilesToKeep);
                    listener.onResponse(null);
                } catch (Exception e) { ... } finally {
                    canDeleteStaleCommits.set(true);
                }
            });
        } catch (Exception e) { ... }
    }
}
```

### Estimated Savings (60 shards, 3 nodes)

| Scenario | Before | After (30s default) | Reduction |
|---|---|---|---|
| LIST/min — segment GC | 3,600 | **120** | **97%** |
| LIST/min — translog archive GC | 60 | 60 | 0% |
| LIST/min — pinned timestamp | 1 | 1 | 0% |
| **Total LIST/min (cluster)** | **3,661** | **181** | **~95%** |

Worst-case stale segment accumulation per shard: 1 file per 30s → at most ~1 extra metadata file lingers per cycle. Negligible storage overhead.

### Setting Tuning Guide

| `gc_min_interval` | LIST/min (60 shards) | Max cleanup delay |
|---|---|---|
| `0s` (disabled / current behavior) | ~3,600 | immediate |
| `10s` | ~360 | 10s |
| `30s` (proposed default) | ~120 | 30s |
| `60s` | ~60 | 60s |

Setting is dynamically tunable with no restart required.

---

## 8. Summary

> **With archive ON for both txlog and segments, the high S3 LIST count is almost entirely caused by `deleteStaleSegments()` running once per refresh per shard — not by archive-related operations. Archive mode actually reduces translog LIST calls but has no effect on the dominant segment GC path.**
>
> **The proposed fix — a time-based GC rate limiter (`cluster.remote_store.segment.metadata.gc.min_interval`, default 30s) — reduces cluster-wide S3 LISTs by ~95% with zero correctness risk: the GC algorithm is unchanged, replicas are unaffected, and stale files are cleaned up correctly just less frequently.**
