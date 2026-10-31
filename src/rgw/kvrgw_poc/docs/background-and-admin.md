# Background Tasks & Admin Operations

## Background Threads

The C++ backend runs three background threads alongside the gRPC server:

| Thread | Class | Source | Interval |
|--------|-------|--------|----------|
| Sweeper | `Sweeper` | `sweeper.cpp` | `KVRGW_SWEEPER_INTERVAL_SEC` (default 10s) |
| GC Worker | `GcWorker` | `gc_worker.cpp` | `KVRGW_GC_INTERVAL_SEC` (default 10s) |
| Admin Server | `AdminServer` | `admin_server.cpp` | Event-driven (Unix socket) |

All threads share a `std::atomic<bool> stop_flag` — set by SIGINT/SIGTERM for clean shutdown. Background workers use best-effort error handling: FDB and DataStore errors are skipped (no exceptions), and the worker continues on the next entry.

---

## Sweeper

**Purpose:** Crash recovery for incomplete STORAGE-tier uploads. Moves stale P:O entries to G:O so GcWorker can clean up orphaned blobs.

**When it matters:** If the backend crashes between Phase 2 (blob written) and Phase 3 (commit), the P:O entry remains in FDB and the blob is orphaned on disk. The sweeper finds these and moves them to GC.

**Algorithm:**

```
loop (every interval_sec):
  range_scan(P: prefix for this shard) → all P:O entries
  for each P:O entry:
    parse PoValueHeader → get created_at_unix
    if (now - created_at_unix < min_age_sec): skip (still in-flight)
    txn {
      move_po_to_go(P:O parts, po_value)
    }
```

**`move_po_to_go`** (in `gc_value.cpp`):
```
txn {
  get(P:O) → if missing: already cleaned, return
  get(S:O) → if exists and ref_tag matches: Phase 3 already committed, just del(P:O)
  else:
    put(G:O, GcValueHeader{chunk=STORAGE, object_size=estimated_size, mtime=0})
    del(P:O)
}
```

**Group P:O handling (batch mode):**

`sweep_once()` detects group P:O entries by checking category `'G'` at key offset 13 (vs `'O'` for regular P:O). For group entries:
1. Parse group value (`parse_group_po_value`) to get per-entry ref_tags and sizes
2. Check age against `min_age_sec`
3. Create a single group G:O entry (`make_group_go_key` + `make_group_gc_value`) and delete the group P:O in one FDB transaction
4. Flags = 0 in the group G:O value because: Phase 3 never committed → no shared data, no tags (bypass batch), no annotations (AWS spec: separate API call on committed objects)

**Config:**
- `KVRGW_SWEEPER_INTERVAL_SEC` — scan interval (default 10s)
- `KVRGW_SWEEPER_MIN_AGE_SEC` — minimum P:O age before considering it stale (default 60s)

**Note:** Only STORAGE-tier objects use P:O. INLINE and KV_STORE objects never create P:O entries (single-txn atomic writes).

---

## GC Worker

**Purpose:** Processes G:O entries — frees storage-tier blobs, deletes D: entries for KV_STORE objects, and removes the G:O key.

**Algorithm:**

```
loop (every interval_sec, unless suspended):
  range_scan(G: prefix) → all G:O entries
  for each G:O entry:
    parse GoKeyParts (from key) → bucket_id, ref_tag, size_tier
    parse GcValueHeader (from value) → chunk.type, flags, object_size, mtime
    shared = (flags & kFlagSharedData)

    switch (chunk.type):
      CHUNK_STORAGE / CHUNK_STORAGE_REF:
        if shared:
          r_key = make_r_key(ref_tag)
          txn {
            r_val = get(R:<ref_tag>)
            if r_val.ref_count > 1:
              put(R:<ref_tag>, count - 1)  // decrement only — other copies still live
              del(G:O)
              commit; continue
            del(R:<ref_tag>)               // last reference — remove R: entry
          }
        rate_allow(blob_bytes) → if throttled: return
        data_store.remove(ref_tag)
        txn { del(G:O); if flags & kFlagExternalTags: range_clear(C:prefix) }

      CHUNK_CHILD_D / CHUNK_CHILD_D_REF:
        rate_allow(0)
        reconstruct D: key from (bucket_id, d_size_tier(object_size), ref_tag, mtime)
        txn {
          decrement_or_del_child_d(tr, d_key, object_size, shared)
          del(G:O)
          if (flags & kFlagExternalTags): range_clear(C:<ref_tag>, C:<ref_tag>\xFF)
        }

      CHUNK_INLINE:
        rate_allow(0)
        txn { del(G:O); if flags & kFlagExternalTags: range_clear(C:prefix) }
        // inline data was already gone when O: was deleted
        // G:O only exists if object had annotations
```

**GC entry sources (`must_defer_to_gc` conditions):**
- `move_object_to_g` — defers to G:O when: CHUNK_STORAGE/STORAGE_REF, or `has_external_annotations()`, or `kv_store_coalescing` enabled. All other cases (INLINE, CHILD_D, CHILD_D_REF, external tags, extended attrs) are cleaned inline in the same transaction.
- `displace_old_object` (SUSPENDED) — same `must_defer_to_gc` logic for V:<kNullVersion> entries
- `DeleteObjectVersion` Case 2 — GC for the deleted current version's data
- `CopyObject` — displacement of the destination object on copy-overwrite (same as PUT overwrite)
- Sweeper — stale P:O entries moved to G:O

**Shared data (kFlagSharedData):** When a G:O entry has the shared flag set, the GcWorker decrements the ref_count (in R: for STORAGE, in D: suffix for CHILD_D) before freeing. The blob/D: entry is only deleted when count reaches 1 (last reference). This prevents double-free of data shared across multiple S3 objects via CopyObject.

**Shared helper:** `decrement_or_del_child_d(tr, d_key, object_size, shared)` in `ref_count_ops.cpp` — used by both the inline cleanup path (move_object_to_g) and the GcWorker. If not shared: unconditional del(D:). If shared: read D:, parse ref_count suffix, decrement or delete. D: ref-count logic is extracted into `decrement_or_del_child_d(tr, d_key, object_size, shared)` in `ref_count_ops.cpp` — shared by both `move_object_to_g` (inline cleanup) and `GcWorker`.

**Child KV cleanup (kFlagExternalTags):** When a G:O entry has the external tags flag set, the GcWorker issues `range_clear(C:<ref_tag>, C:<ref_tag>\xFF)` in the same transaction as the G:O deletion. This removes all child KV entries (tags, future annotations) for the ref_tag. The range_clear is a no-op if no children exist. Only performed on full deletions — ref-count decrements skip child cleanup since other references still exist.

**Group G:O handling (batch GC):**

The GcWorker detects group G:O entries in the G: scan (category `'G'` at key offset 14, vs `'O'` for regular entries). For group entries:
1. Parse group G:O value (`parse_group_gc_value`) to get per-entry ref_tags, chunk types, flags, and sizes
2. Iterate entries within the group; rate-limit per entry
3. Call `data_store.remove(ref_tag)` for each entry's ref_tag
4. After all entries processed, delete the group G:O key in one FDB transaction

**Rate limiting:**
- `max_objects_per_sec` — caps G:O entries processed per second
- `max_mb_per_sec` — caps blob bytes freed per second (STORAGE only)
- Resets per 1-second window

**Config:** Via admin socket (see below) or env vars at startup.

---

## Admin Server

**Purpose:** Runtime configuration changes without restart. Listens on a Unix domain socket (`/tmp/kvrgw-admin-{i}.sock`).

**Protocol:** Line-based text. Client sends one line, server responds with one line, then closes connection.

**Wire format:**
```
echo "GET" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-0.sock
```

### GC Policy Commands

| Command | Description | Example |
|---------|-------------|---------|
| `SET key=value ...` | Stage new GC policy | `SET suspended=1 interval_sec=5` |
| `GET` | Read active + pending policy | Returns all fields + ages |
| `QUERY` | Read active_age only | `ACTIVE_AGE=7` |

**SET fields:** `suspended` (0/1), `interval_sec`, `max_objects_per_sec`, `max_mb_per_sec`

**Response:** `OK HANDLE=N` on success, `BUSY` if previous change not yet applied, `ERROR <msg>` on bad input.

### Tier Config Commands

| Command | Description | Example |
|---------|-------------|---------|
| `set-tier-config key=value ...` | Stage new tier config | `set-tier-config max_inline=512 max_kv_store=16384` |
| `get-tier-config` | Read active tier config | Returns all fields + ages |
| `query-tier-config` | Read active_age only | `ACTIVE_AGE=3` |

**set-tier-config fields:** `max_inline`, `max_kv_store`, `kv_store_coalescing` (0/1/true/false), `batch_size` (int, default 1), `batch_timeout_us` (int, default 1000), `batch_threads` (int, default 8)

**Invariant enforced:** `max_kv_store > max_inline` (unless both are 0). Rejected with `ERROR invariant: ...`

### Double-Buffer Pattern

Both GC policy and tier config use the same state management pattern (`GcConfigState` / `TierConfigState`):

1. `try_stage(new_config)` — writes to pending slot, returns handle
2. Background thread calls `maybe_apply_pending()` on each iteration — swaps pending → active
3. `wait-applied --handle N` (via `gc_ctl.sh`) — polls until `active_age >= N`

This ensures in-flight operations complete with the old config before the new one takes effect. No locks on the hot path — `active_copy()` is a mutex-guarded read (fast, no contention under normal load).

---

## gc_ctl Tool

**Binary:** `build/gc_ctl`
**Wrapper:** `scripts/gc_ctl.sh` (sets env vars, calls binary)

### Inspect Commands (read FDB directly)

| Command | Description |
|---------|-------------|
| `count` | Pending G:O entries, total_bytes, missing_blobs |
| `count-by-tier` | Per-tier breakdown |
| `list [--limit N]` | List G:O entries with ref_tag, tier, blob_bytes |
| `list-by-size <min> <max> [--limit N]` | Filter by size range |
| `raw-get <key-hex>` | Read raw binary value from FDB (stdout) |
| `raw-set <key-hex>` | Write raw binary value to FDB (stdin) |

### Admin Commands (via Unix socket)

| Command | Description |
|---------|-------------|
| `set-gc-config key=value ...` | Stage GC policy change |
| `get-gc-config` | Read current GC config |
| `query-active-age` | Poll active_age |
| `wait-applied --handle N [--max-gap N] [--poll-sec N]` | Block until applied |

---

## Startup Sequence

```
main():
  1. Parse env vars (socket path, data root, intervals, tier config)
  2. Load tier config: YAML file → env overrides → defaults
  3. fdb_select_api_version + fdb_setup_network + network thread
  4. KvStore::create() → expected (exit on failure)
     - KvStore API includes `run_transaction` template (not yet used by background callers)
  5. allocate_rgw_id() → expected (exit on failure)
  6. Create: RefTagGenerator, DataStore, TierConfigState, KvRgwServiceImpl
     - Bucket cache uses 3-second TTL (`kBucketCacheTtl`)
  7. Create: GcConfigState (initial GC policy from env)
  8. Start threads: Sweeper, GcWorker, AdminServer (run() returns bool)
  9. Start gRPC server on Unix socket
  10. Wait for SIGINT/SIGTERM → set stop_flag → join threads → shutdown
```

No exceptions in the startup path. All errors checked via return values.

---

## ErrInsertion (Fault Injection)

**Purpose:** White-box testing of crash-recovery paths. Allows injecting faults at specific code points via admin socket commands, without recompilation.

**Class:** `ErrInsertion` in `err_insertion.hpp/cpp`. Data member of `KvRgwServiceImpl`.

**Layout:** Two-level cache-friendly structure:
- `flags_[kFaultTypeCount]` — hot; one byte per fault type (0 = inactive). `is_error_active(FaultType)` is inline with `[[likely]]` on the zero branch.
- `FaultPayload payload_[kFaultTypeCount]` — cold; only consulted when the corresponding flag is non-zero.

**Modes:**
- Fixed (0x01) — always active once set
- Counter-based/periodic (0x02) — activates every N-th check
- Time-based/periodic (0x03) — activates every N microseconds

Burst support: consecutive activations per trigger.

**FaultType enum:**
- `kAbortAfterBatchPhase2` — abort `commit_batch` after Phase 2 disk writes
- `kAbortAfterSinglePhase2` — abort single-mode PUT after Phase 2
- `kAbortSweeperAfterPutGo` — abort sweeper after writing G:O
- `kAbortGcWorkerMidGroup` — abort GcWorker mid-group processing

**Admin commands:**
- `set-error <name> [period=N] [burst=N] [interval_us=N]`
- `clear-error <name>`

**White-box test:** `scripts/test_white_box.sh` — injects fault → PUT fails after Phase 2 → verifies orphan blob exists → sweeper + GcWorker clean up.

---

## Shutdown

1. Signal handler sets `stop_flag = true`
2. gRPC server shutdown (drains in-flight RPCs)
3. Background threads check `stop_flag` each iteration and exit
4. Network thread: `fdb_stop_network()` + join
5. Clean exit
