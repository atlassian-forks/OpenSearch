# Translog Archive GC — Full Design Document

## 1. Background and Problem

OpenSearch remote-backed shards write translog operations continuously. Without archiving, each shard
uploads individual `.tlog` and `.ckp` files to S3 — generating **millions of S3 PUT requests per day**
at enormous cost. The archive-on path batches these into TAR files (one per node per 650ms), dramatically
reducing S3 API calls.

Once translog ops are committed to remote segments, the archive TARs are no longer needed for recovery.
They must be deleted to avoid unbounded S3 storage growth.

**The GC challenge**:
- Pure timestamp GC (delete if age > N hours) is unsafe and wasteful
  - Unsafe: a node down for >2h could lose data if TARs are deleted before recovery
  - Wasteful: holds TARs for 2h even when they're safe to delete in minutes
- The cluster-manager (responsible for GC) does **not** host shards and cannot query per-shard checkpoints
- GC decisions must be made without expensive per-shard transport calls to data nodes

**Solution**: Embed checkpoint information directly in each TAR at upload time. The cluster-manager
reads this embedded data during scanning and makes correct GC decisions autonomously.

---

## 2. Three Concepts: Generation, SeqNo, GlobalCheckpoint

These are **distinct** — confusing them causes bugs.

| Concept | Space | Example | Meaning |
|---|---|---|---|
| **Translog generation** | File counter | 42, 47 | Which translog FILE (not comparable to seqNo) |
| **Sequence number (seqNo)** | Op counter | 15,847 | Individual operation ordering |
| **GlobalCheckpoint** | SeqNo space | 15,843 | Highest seqNo replicated to all shard copies |

```
Translog file gen=47 contains seqNos [15,800 .. 15,847]
globalCheckpoint = 15,843 → gen 47 still needed (15,847 not yet covered)
globalCheckpoint = 15,847 → gen 47 can be deleted
```

**What we embed in TARs**: seqNo ranges + `lastSyncedGlobalCheckpoint` (NOT generation numbers,
NOT `getLastKnownGlobalCheckpoint`).

**Why `lastSyncedGlobalCheckpoint`**: This is the checkpoint from the **last committed Lucene segment**
that was durably uploaded to the remote segment store. In remote-store clusters, segment commits are
uploaded before the checkpoint advances. So `lastSyncedGlobalCheckpoint >= maxSeqNo` means all ops
are in remote segments — the TAR is safe to delete.

`getLastKnownGlobalCheckpoint()` (in-memory replicated checkpoint) can be ahead of what's actually
in remote segments and must NOT be used for GC decisions.

---

## 3. S3 Folder Structure

```
{repo-root}/
  txlog/                                     ← translog archive root
    20260502/                                ← day dir (yyyyMMdd)
      1000/                                  ← minute dir (HHmm)
        45.123.nodeA.tar                     ← TAR: uploaded every 650ms per node
        46.456.nodeA.tar
        45.789.nodeB.tar
        ...  (~9,231 TARs per minute at 100 nodes)
      1001/
        ...
  gc_idx/                                    ← GC journal (new)
    20260502/
      1000.idx                               ← merged GC index per minute (~64KB)
      1001.idx
      ...
```

**Key path invariants**:
- `txlog/{day}/{minute}/` is a flat dir of `.tar` blobs
- `gc_idx/{day}/{minute}.idx` is a single blob per minute, written after all TARs are scanned
- Absent `.idx` = "not yet scanned" (scanner hasn't processed this minute)
- Empty `.idx` (0 shards) = "scanned, no GC data found" → safe to delete by timestamp alone

---

## 4. TAR Internal Structure: `_index` Binary Format

Each TAR contains a special first entry named `_index` with this binary layout:

```
_index binary layout:
  ┌─ GC SUMMARY PREFIX ─────────────────────────────────────────────────────┐
  │ [2 bytes: numShardsGC (unsigned short, big-endian)]                     │
  │ Per shard (32 bytes = GC_SUMMARY_BYTES_PER_SHARD):                      │
  │   [4 bytes:  shardId]                                                   │
  │   [8 bytes:  minSeqNo]  ← first seqNo of ops for this shard in this TAR│
  │   [8 bytes:  maxSeqNo]  ← last  seqNo of ops for this shard in this TAR│
  │   [4 bytes:  indexUUID hash (first 4 bytes, for shard identity)]        │
  │   [8 bytes:  lastSyncedGlobalCheckpoint at upload time]                 │
  └─────────────────────────────────────────────────────────────────────────┘
  ┌─ FULL ENTRY INDEX (recovery path only) ─────────────────────────────────┐
  │ [4 bytes: numEntries]                                                   │
  │ Per entry: [2 pathLen][pathLen path][8 dataOffset][8 dataLength]        │
  └─────────────────────────────────────────────────────────────────────────┘
```

**Constants**:
- `GC_SUMMARY_BYTES_PER_SHARD = 32`
- `MAX_GC_PREFIX_BYTES = 2 + 4096 * 32 = 131,074 bytes ≈ 128KB`
  (generous upper bound: supports up to 4,096 shards/node)

**GC prefix size** at 2,000 shards/node: `2 + 2,000 × 32 = 64,002 bytes ≈ 64KB`

**Where it's written** (`TranslogArchiveCollector.runBatchForNode()`):
```java
// Per contributing shard:
long minSeqNo = shard.seqNoStats().getLocalCheckpoint();
long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
long checkpoint = shard.getLastSyncedGlobalCheckpoint();  // NOT getLastKnownGlobalCheckpoint()
int uuidHash = shard.indexSettings().getIndexMetadata().getIndexUUID().hashCode();
gcEntries.add(new GcShardEntry(shard.shardId().id(), minSeqNo, maxSeqNo, uuidHash, checkpoint));
TarArchiveBuilder.computeLayout(allEntries, gcEntries);  // embeds GC prefix
```

---

## 5. GC Journal: `.idx` File Format

Written to `gc_idx/{day}/{HHmm}.idx` after scanning all TARs in a minute-dir.
Format mirrors the TAR GC prefix exactly (same layout, reusable parser):

```
.idx binary layout:
  [2 bytes: numShards]
  Per shard (32 bytes):
    [4 shardId][8 minSeqNo][8 maxSeqNo][4 uuidHash][8 maxCheckpoint]
  = 2 + numShards * 32 bytes
```

**Values are the MERGED result** across all TARs in the minute:
- `minSeqNo` = `min(minSeqNo)` across all TARs for this shard
- `maxSeqNo` = `max(maxSeqNo)` across all TARs for this shard
- `maxCheckpoint` = `max(lastSyncedGlobalCheckpoint)` across all TARs for this shard

**Size**: At 2,000 shards: `2 + 2,000 × 32 = 64,002 bytes ≈ 64KB`

**Always written** — even if the minute had zero GC shards (empty `.idx` = 2 bytes).
This preserves the invariant: absent `.idx` = not yet scanned.

---

## 6. GC Cluster-Manager Scheduled Task

Runs on the **elected cluster-manager only**, every `GC_INTERVAL` (default 10 minutes).

### Lifecycle

```
onClusterManager():
  initGcScannerIfNeeded() [synchronized]
  scanner.loadFromPersisted()         ← reads gc_idx/ blobs, rebuilds in-memory state
  start retentionScheduledTask (10 min interval)

offClusterManager():
  gcScanner = null

runArchiveRetention() [every 10 min]:
  initGcScannerIfNeeded() [synchronized]  ← lazy init if no shards at onClusterManager time
  scanner.scan(now)                        ← Phase 1: index new minute-dirs
  deleteHierarchicalArchivesOlderThan()   ← Phase 2: delete safe TARs
```

### Phase 1: Scanner (`scanner.scan()`)

**Purpose**: Read GC prefix from new TARs, merge into per-minute `.idx`, update in-memory state.

**For each new minute-dir** (not yet having a `.idx`):

```
Step 1: LIST txlog/{day}/                → find day dirs
Step 2: LIST gc_idx/{day}/               → find already-indexed minutes
Step 3: For each unindexed minute-dir (sorted chronologically):
  Step 3a: LIST txlog/{day}/{HHmm}/      → get TAR blob names
  Step 3b: For each TAR blob:
    1 range-GET [0, 512 + MAX_GC_PREFIX_BYTES)
      → bytes [0,512): TAR header → parse _index entry size
      → bytes [512,...): GC prefix → parse numShardsGC + per-shard entries
  Step 3c: Merge across all TARs:
    shardId → {min(minSeqNo), max(maxSeqNo), max(checkpoint)}
  Step 3d: PUT gc_idx/{day}/{HHmm}.idx  ← persist merged index
  Step 3e: ONLY AFTER successful PUT:
    inMemoryIndex[minuteKey] = mergedIndex
    rollingCheckpoints[shardId] = max(rollingCheckpoints[shardId], maxCheckpoint)
    latestMaxSeqNo[shardId] = max(latestMaxSeqNo[shardId], maxSeqNo)

Minute-dirs processed in SORTED (chronological) ORDER within each day:
  → ensures rolling checkpoint accumulates in increasing order
```

**Single range-GET per TAR** (S3 charges per request, not per byte):
- Reading 128KB costs same as reading 512B
- Fetch TAR header + GC prefix in one request
- No second round-trip needed

### Phase 2: GC Deletion

**Safety buffer**: Never delete a minute-dir newer than `MIN_RETENTION_SAFETY_BUFFER_MINUTES` (5 min).
This protects brand-new minute-dirs that haven't been scanned yet.

**For each minute-dir older than 5 min**:

```
if isSafeToDelete(minuteKey):
  deleteAllTarsInMinuteDir()     ← fast path: whole-minute delete
  scanner.evict(minuteKey)
else:
  // future: per-TAR granularity (see Section 9)
  skip entire minute-dir (conservative)
```

---

## 7. Two-Phase `isSafeToDelete()` Logic

```java
boolean isSafeToDelete(String minuteKey):
  idx = inMemoryIndex[minuteKey]
  if idx == null: return false          // Not yet scanned → conservative hold
  if idx.size() == 0: return true       // Scanned, no GC shards → safe by timestamp

  for each ShardRange shard in idx:
    // Phase 1 (immediate): checkpoint at upload time already covered all ops
    // Safe: if the data node's last synced checkpoint already covered maxSeqNo,
    // the remote segments had these ops durably committed when the TAR was written.
    if shard.maxCheckpoint >= shard.maxSeqNo:
      continue  // this shard is immediately safe

    // Phase 2 (rolling): a newer TAR advanced the rolling checkpoint past maxSeqNo
    // Safe: some newer TAR from this shard showed a higher checkpoint.
    Long latestCheckpoint = rollingCheckpoints[shard.shardId]
    if latestCheckpoint == null || shard.maxSeqNo > latestCheckpoint:
      return false  // still waiting for checkpoint to advance

  return true
```

**Decision table**:

| State | Phase 1 | Phase 2 | Decision | Reason |
|---|---|---|---|---|
| `idx == null` | — | — | **hold** | Not yet scanned |
| `idx.size() == 0` | — | — | **delete** | No GC data → safe |
| `maxCheckpoint >= maxSeqNo` | ✅ | — | **delete** | Ops in remote segments at upload time |
| `maxCheckpoint < maxSeqNo`, rolling covers | — | ✅ | **delete** | Newer TAR confirmed checkpoint advanced |
| `maxCheckpoint < maxSeqNo`, rolling missing | — | ❌ | **hold** | Checkpoint hasn't caught up |
| `maxCheckpoint < maxSeqNo`, rolling below | — | ❌ | **hold** | Checkpoint still below maxSeqNo |

**Why this handles idle shards** (Phase 1):
When a shard has no writes after minute M, no newer TARs will advance the rolling checkpoint.
But at upload time, the shard's checkpoint likely already covered all its ops (idle = ops committed).
Phase 1 catches this without requiring any newer TAR.

---

## 8. In-Memory State: What's Cached and Memory Usage

### Data Structures (all on cluster-manager)

```
inMemoryIndex: ConcurrentHashMap<String, MinuteGcIndex>
  Key:   "yyyyMMdd/HHmm" (minute key)
  Value: MinuteGcIndex { shardId → ShardRange{minSeqNo, maxSeqNo, maxCheckpoint} }

rollingCheckpoints: ConcurrentHashMap<Integer, Long>
  Key:   shardId
  Value: max(lastSyncedGlobalCheckpoint) seen across ALL scanned TARs

latestMaxSeqNo: ConcurrentHashMap<Integer, Long>    [future: for per-TAR deletion]
  Key:   shardId
  Value: max(maxSeqNo) seen across all minutes
```

### Memory Sizing (steady state — 2h retention window)

```
inMemoryIndex:
  Entries:  120 minute-dirs (2h / 1min)
  Per entry: 2,000 shards × ~80 bytes (Java object) = 160KB
  Total:    120 × 160KB = 19.2MB

rollingCheckpoints:
  Entries:  max shards in cluster = 2,000 shards/node × 100 nodes = 200,000
  Per entry: 8B key + 8B value + ~32B overhead = 48 bytes
  Total:    200,000 × 48 = 9.6MB

latestMaxSeqNo:
  Same size as rollingCheckpoints = 9.6MB (future)

Total in-memory: ~29MB (or ~19MB without latestMaxSeqNo)
```

### Memory During Node Outage (stuck shards)

With 50 nodes down for 30 days:
- Stuck minute-dirs accumulate at rate of GC cycle (10 min = new minute-dirs not deleted)
- `inMemoryIndex` grows at rate: 10 new minutes/GC cycle
- After 30 days: `30 × 24 × 6 = 4,320 additional minutes × 160KB = 691MB` ← could be large

**Mitigation**: When a shard's minute-dir is stuck for too long, the scanner still writes `.idx`
and loads it into memory, but GC skips deletion. The in-memory index grows proportionally to the
stuck duration. On recovery, the rolled shards become safe and entire stuck minute-dirs are deleted.

For extremely long outages (>7 days), the cluster-manager has enough RAM (typically 64GB) to hold
this data: `10,080 min × 160KB = 1.6GB` — manageable.

### S3 Cost for `.idx` Files

```
PUT .idx:    10 new minutes/cycle × 144 cycles/day = 1,440 PUTs/day → $0.007/day
GET .idx:    loaded once on master restart (120 files × 1 GET) → $0.00005
DELETE .idx: 1,440 deletes/day when minute-dir is fully cleaned → ~$0
```

---

## 9. Per-TAR Granularity: Implemented

### Problem with Current Whole-Minute Deletion

If shard 5 (on nodeX) is stuck, ALL 9,231 TARs in a minute-dir are held — even though 9,230 of
them (from the other 99 nodes) are safe. During a 6h nodeX outage:

```
Current:   hold 360 minute-dirs × 9,231 TARs = 3.3M TARs ≈ 1TB storage
Per-TAR:   hold 360 minute-dirs × 1 stuck TAR = 360 TARs ≈ 112MB storage
Savings:   99.99% storage reduction during outage
```

### Implementation: Shard-Level Stuck Tracking

**Key insight**: Track which **shards** are stuck, not which TARs. Memory bounded by cluster topology.

```
New state in TranslogArchiveGcScanner:
  latestMaxSeqNo: Map<shardId, Long>     // max(maxSeqNo) ever seen across all TARs
  perTarGcCache:  Map<minuteKey,         // per-TAR GC entries (populated during scanMinute)
                      Map<blobName,
                          List<GcShardEntry>>>

isShardStuck(shardId):
  checkpoint = rollingCheckpoints[shardId]   // null → unknown → true (conservative)
  maxSeq     = latestMaxSeqNo[shardId]       // null → unknown → true (conservative)
  return checkpoint == null || maxSeq == null || checkpoint < maxSeq

isTarSafeToDelete(gcEntries):
  for each entry in gcEntries:
    if entry.maxCheckpoint >= entry.maxSeqNo: continue    // Phase 1
    if isShardStuck(entry.shardId): return false          // Phase 2
  return true
```

### Per-TAR Deletion Flow (in `deleteStuckMinutePartially`)

```
For stuck minute-dir M (isSafeToDelete(M) == false):
  1. LIST txlog/{day}/{minute}/              → get remaining TAR blobs (1 LIST)
  2. For each TAR blob:
     a. Read GcShardEntry list:
        → from perTarGcCache (free — already in memory from scanMinute)
        → or re-read GC prefix via range-GET if not cached (master restart)
     b. if isTarSafeToDelete(gcEntries): add to delete batch
     c. else: count as remaining (stuck)
  3. DELETE safe TARs in batches
  4. if remaining == 0: evict minuteKey from inMemoryIndex
```

### Memory Analysis

```
latestMaxSeqNo: Map<shardId, Long>
  200,000 shards × 48 bytes = 9.6MB    ← bounded by cluster topology, never grows unboundedly

No per-TAR cache maintained.
Reason: caching GC entries for all scanned TARs would require:
  92,310 TARs/cycle × 2,000 shards × ~80 bytes = 14.8GB ← catastrophic

Instead: re-read GC prefix via range-GET at deletion time for stuck minutes only.
Cost of re-reads: stuck minutes are rare; ~$2.64 one-time when node recovers.
This is the correct tradeoff: zero steady-state memory vs negligible extra cost during outages.
```

### Storage Savings During Node Outage

```
6h outage, 1 node down (1 stuck TAR/minute):
  Current (whole-minute): hold 360 × 9,231 = 3.3M TARs ≈ 1TB storage
  Per-TAR:               hold 360 × 1      = 360  TARs ≈ 112MB storage
  Savings: 99.99%
```

---

## 10. Master Restart Recovery

On cluster-manager restart, rebuild in-memory state from persisted `.idx` files:

```
Phase 1: LIST gc_idx/{day}/             → find all day dirs (1 LIST)
Phase 2: For each day dir:
  LIST gc_idx/{day}/                    → list .idx blobs (1 LIST per day)
  For each .idx blob:
    GET gc_idx/{day}/{HHmm}.idx         → download and deserialize (1 GET)
    inMemoryIndex[minuteKey] = idx
    rollingCheckpoints[s] = max(rolling[s], idx.maxCheckpoint(s))

Cost: ~120 GETs × 64KB = 7.7MB data transfer → $0.00005
Latency: <1 second (parallel GETs)
```

**Why this is fast**: we read `.idx` files (one per minute, 64KB each), NOT individual TARs
(9,231 per minute, range-GET per TAR). The `.idx` files act as a write-ahead summary — pay once
during scanning, free forever after.

---

## 11. S3 Cost Model

### Cluster Parameters

| Parameter | Value |
|---|---|
| Data nodes | 100 |
| Shards/node | 2,000 |
| TAR interval | 650ms/node |
| TARs/minute (cluster) | 100 × (60/0.65) = 9,231 |
| TARs/day | 9,231 × 60 × 24 = 13.3M |
| GC interval | 10 min (144 cycles/day) |
| New minute-dirs/cycle | 10 |
| S3 pricing (us-east-1) | PUT $0.005/1K · GET $0.0004/1K · LIST $0.005/1K · DELETE $0.00004/1K |

### Cost Breakdown

#### TAR Upload (data nodes, every 650ms)
| Operation | Count/day | Cost/day |
|---|---|---|
| PUT TARs | 13.3M | $66.50 |

#### Scanner Phase (cluster-manager, every 10 min)
| Operation | Count/day | Notes |
|---|---|---|
| LIST day-dirs (txlog/ root) | 144 | 1 LIST/cycle |
| LIST gc_idx/{day}/ | 144 | 1 LIST/cycle |
| LIST minute-dirs (10 pages × 10 min-dirs) | 14,400 | 10 LIST pages per minute-dir |
| **Range-GET TAR GC prefix** | **13.3M** | **1 GET per TAR — dominant cost** |
| PUT .idx | 1,440 | 1 per new minute-dir |

| Operation | Cost/day |
|---|---|
| LISTs (all) | $0.073 |
| **Range-GETs** | **$5.32** |
| PUT .idx | $0.007 |
| **Scanner subtotal** | **$5.40** |

#### GC Deletion Phase (cluster-manager, every 10 min)
| Operation | Count/day | Cost/day |
|---|---|---|
| LIST minute-dirs for deletion | 14,400 | $0.072 |
| DELETE TARs | 13.3M | $0.53 |
| DELETE .idx | 1,440 | ~$0 |
| GET .idx (load new) | 1,440 | $0.001 |
| **GC subtotal** | | **$0.60** |

#### Master Restart (one-time)
| Operation | Cost |
|---|---|
| LIST + GET 120 .idx files | ~$0.00005 |

#### Total
| Component | Daily cost |
|---|---|
| TAR uploads | $66.50 |
| Scanner | $5.40 |
| GC deletion | $0.60 |
| **TOTAL** | **$72.50/day** |
| Instance cost (100 × r7g.4xlarge @ 50%) | $1,028/day |
| **Journal overhead as % of compute** | **0.54%** |

### Why 1 GET per TAR (not 2)

S3 charges per request, not per byte. Reading 128KB costs the same as reading 512B.
Single range-GET `[0, 512 + MAX_GC_PREFIX_BYTES)` fetches TAR header + GC prefix atomically.
Saves 13.3M GETs/day vs 2-GET design → saves **$5.32/day**.

---

## 12. Correctness and Safety Properties

### Key Invariants

1. **Absent `.idx` = not scanned**: scanner always writes `.idx` (even empty) before updating memory
2. **Empty `.idx` = safe by timestamp**: scanned, all TARs had 0 GC shards
3. **Memory updated only after durable PUT**: if PUT fails → `return` (no memory update)
4. **Chronological processing**: minute-dirs sorted within a day so rolling max is correct
5. **`initGcScannerIfNeeded()` synchronized**: prevents double-init race on cluster-manager
6. **5-minute safety buffer**: protects brand-new minute-dirs not yet scanned by scanner

### `getLastSyncedGlobalCheckpoint()` Safety

In remote-store clusters, Lucene segment commits are uploaded to the remote segment store
**before** the checkpoint advances. Therefore:
- `lastSyncedGlobalCheckpoint >= maxSeqNo` → ops are in remote segments → TAR deletable
- No risk of "checkpoint said safe but remote segments don't have it yet"

`getLastKnownGlobalCheckpoint()` (in-memory replicated checkpoint) must NOT be used —
it can be ahead of what's actually uploaded to remote segment store.

### Recovery Safety

`TranslogArchiveRecovery.readTarIndex()` calls `TarArchiveBuilder.parseIndex()` which correctly
skips the GC prefix:
```java
int numShardsGC = buf.getShort() & 0xFFFF;
buf.position(buf.position() + numShardsGC * GC_SUMMARY_BYTES_PER_SHARD);
// ... then reads entry index normally ...
```

The `indexDataSize > 65536` guard in `readTarIndex()` has been updated to `> 512 * 1024`
to accommodate the larger GC prefix (up to 64KB at 2,000 shards/node).

---

## 13. Implementation Files

| File | Role |
|---|---|
| `TarArchiveBuilder.java` | `GcShardEntry` (32B/shard); `computeLayout(entries, gcEntries)`; `serializeIndex()` with GC prefix; `parseIndex()` skips GC prefix; `parseGcSummary()` fast path for scanner |
| `MinuteGcIndex.java` | `ShardRange{shardId, minSeqNo, maxSeqNo, uuidHash, maxCheckpoint}`; `Builder.merge()`; `serialize()`/`deserialize()` |
| `TranslogArchiveGcScanner.java` | `readGcPrefix()` (1 GET); `scanMinute()` (always writes .idx, updates only after persist); `rollingCheckpoints` map; two-phase `isSafeToDelete()`; `loadFromPersisted()` restores state; chronological minute ordering; `getRollingCheckpoints()` for testing |
| `TranslogArchiveCollector.java` | `gcEntries` built with `getLastSyncedGlobalCheckpoint()`; synchronized `initGcScannerIfNeeded()`; `runArchiveRetention()` uses 5-min safety buffer; `deleteHierarchicalArchivesOlderThan()` calls `scanner.isSafeToDelete()` |
| `TranslogArchiveRecovery.java` | `recoverFromHierarchicalPath()`; `readTarIndex()` with corrected 512KB guard; `parseIndex()` correctly skips GC prefix |
| `ArchiveDeletionHelper.java` | `MIN_RETENTION_SAFETY_BUFFER_MINUTES = 5`; `RetentionBounds` for archive-off/on interaction |

## 14. Test Coverage

| Test Class | What It Covers |
|---|---|
| `TarArchiveBuilderGcTests` | GC prefix roundtrip; `parseIndex` backward compat; full TAR build with GC; edge cases (null, truncated, empty) |
| `MinuteGcIndexTests` | Builder merge (multiple TARs); serialize/deserialize roundtrip; `ShardRange.merge()` correctness |
| `TranslogArchiveGcScannerTests` | `readGcPrefix` (1 GET); two-phase `isSafeToDelete` (Phase1/Phase2/both-fail); empty index safe; not-scanned conservative; rolling checkpoint across 3 minutes; chronological order correctness |
| `TranslogArchiveCollectorTests` | Checkpoint-gate blocks deletion (embedded checkpoint=8 < maxSeqNo=10); advances on newer TAR (embedded checkpoint=10); existing retention and upload tests |
| `TranslogArchiveRecoveryTests` | Index parsing with GC prefix; recovery from hierarchical path |

---

## 15. Per-TAR Granularity Deletion (Implemented)

**Problem**: A single stuck shard blocks deletion of ALL 9,231 TARs in a minute-dir.

**Solution**: Track `latestMaxSeqNo[shardId]` alongside `rollingCheckpoints[shardId]`.
A shard is stuck if `rollingCheckpoints[s] < latestMaxSeqNo[s]`.
For stuck minute-dirs, re-LIST and delete individual TARs that contain no stuck shards.

**Memory**: Only `latestMaxSeqNo` map needed (200K entries × 48B = 9.6MB — bounded by cluster topology).
No per-TAR caching required.

**Storage savings during node outage**:
- Current: hold all 9,231 TARs/minute even for one stuck shard
- Per-TAR: hold only the 1 TAR/minute from the stuck node (~99.99% storage reduction)

**When to implement**: When storage costs during node outages become significant in production.
