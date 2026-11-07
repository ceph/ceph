# GC admin socket and runtime policy

The C++ backend exposes a **Unix admin socket** per instance (`/tmp/kvrgw-admin-{i}.sock`, override with `KVRGW_ADMIN_SOCKET`) for GC control. With multiple backends, [`scripts/gc_ctl.sh`](scripts/gc_ctl.sh) fans out `set-gc-config` / `wait-applied` to every admin socket. Per-instance **HANDLE** values may differ; `wait-applied` waits on each instance’s own handle. `get-gc-config` requires **policy fields** (`suspended`, `interval_sec`, rate limits) to match across instances, and each instance to be internally consistent (`pending_age` ∈ `{active_age, active_age+1}`). **Ages may differ between instances.**

## Policy struct (`GcPolicy`)

| Field | Meaning |
|-------|---------|
| `suspended` | When true, GC worker does not process `G:O` entries |
| `interval_sec` | Sleep between GC passes when not suspended |
| `max_objects_per_sec` | Cap objects removed per second (`0` = unlimited) |
| `max_mb_per_sec` | Cap MiB removed per second (`0` = unlimited) |

Startup defaults from env: `KVRGW_GC_INTERVAL_SEC` (default 10), `KVRGW_GC_MAX_OBJECTS_PER_SEC`, `KVRGW_GC_MAX_MB_PER_SEC` (default 0).

## Double buffering (active / pending)

- **`active_policy`** + **`active_age`**
- **`pending_policy`** + **`pending_age`**
- **In sync:** `active_age == pending_age` — no staged update.
- **Staged:** `pending_age == active_age + 1` — GC has not applied yet.

### SetGcConfig (admin `SET` command)

```
if (active_age != pending_age) → BUSY (client retries)
write pending_policy
pending_age++
return handle = pending_age
```

Admin returns immediately; ACK means **staged**, not active.

### GC worker

Before each **`G:O` entry** (between atomic steps only, not mid-FDB txn):

```
if (active_age != pending_age):
  copy pending_policy → active_policy
  active_age = pending_age
process entry using active_policy (rate limits, etc.)
```

When **`suspended`**, the worker uses a **100ms** idle loop (not `interval_sec`) and breaks out of the inter-pass sleep early if active policy fields change, so a long suspend interval (e.g. 3600) does not block resume.

### Client poll (`QUERY`)

Server returns **`active_age`** only. Client holds **`handle`** from Set.

Loop while `active_age == handle - 1` (sleep ~1s, re-query).

When loop exits:

| Condition | Result |
|-----------|--------|
| `active_age == handle` | **Done** |
| `active_age < handle - 1` | **Error** (invariant; fail test) |
| `active_age > handle + MAX_GAP` | **Error** (fail test) |
| `active_age > handle` (else) | **Expired** — retry full Set + poll |

**Tests (solo):** `MAX_GAP = 1`; should never see `active_age > handle`. **Parallel config test:** `MAX_GAP = N` for N concurrent sessions.

### GetGcConfig (`GET` command)

Returns **`active_age`** and full **`active_policy`**. After poll Done, client verifies **`active_age == handle`** and every field matches the Set request.

## Wire protocol (line-oriented, one request per connection)

**SET** — space-separated `key=value`:

```
SET suspended=1 interval_sec=3600 max_objects_per_sec=0 max_mb_per_sec=0
→ OK HANDLE=3
→ BUSY
→ ERROR ...
```

**QUERY**

```
→ ACTIVE_AGE=2
```

**GET**

```
→ CONFIG active_age=3 pending_age=3 suspended=1 interval_sec=3600 max_objects_per_sec=0 max_mb_per_sec=0
```

## Fault Injection Commands

Admin socket commands for the ErrInsertion framework:

**set-error** — activate a fault injection point:

```
set-error <name> [period=N] [burst=N] [interval_us=N]
→ OK
→ ERROR unknown fault: <name>
```

`name` = fault type name (e.g. `kAbortAfterBatchPhase2`, `kAbortAfterSinglePhase2`, `kAbortSweeperAfterPutGo`, `kAbortGcWorkerMidGroup`). Without options: fixed mode (always active). With `period=N`: counter-based periodic. With `interval_us=N`: time-based periodic. `burst=N`: consecutive activations per trigger.

**clear-error** — deactivate a fault:

```
clear-error <name>
→ OK
→ ERROR unknown fault: <name>
```

## CLI

```bash
./scripts/gc_ctl.sh set-gc-config suspended=1 interval_sec=3600
./scripts/gc_ctl.sh query-active-age
./scripts/gc_ctl.sh get-gc-config
./scripts/gc_ctl.sh wait-applied --handle 3 --max-gap 1
./scripts/gc_ctl.sh count          # FDB G:O scan (unchanged)
```

## Delete → G queue test

1. Set `suspended=1` via admin; wait-applied; get-gc-config verify.
2. Upload **1000** objects (100B–8MiB, log-uniform); suspend GC throughout.
3. Delete in random batches of **1–256** (`DeleteMulti` or single `s3 rm` when batch=1).
4. After **each batch**, verify (only STORAGE tier objects > 8KB appear in G:O — INLINE and KV_STORE objects are cleaned in-txn):
   - `gc_ctl count` — entries, total bytes (from G:O value), no missing blobs
   - `gc_ctl list --limit 2000` — one row per STORAGE-tier deleted object; unique ref_tags; each `size_tier` matches `tier_from_size(blob_bytes)` and tier byte range
   - `gc_ctl count-by-tier` — per-tier count/bytes match deleted histogram (STORAGE only)
   - `gc_ctl list-by-size` — per-tier range counts match
   - GET on batch keys fails
5. Resume GC (`suspended=0`).

Size reporting: `gc_ctl count` reads `object_size` from the binary G:O value (14B `GcValueHeader`: chunk type + flags + size + mtime) for all chunk types. For STORAGE entries, cross-checks against actual blob file size on disk (mismatch = corruption). The `flags` byte includes `kFlagSharedData` (0x04) for entries whose data is ref-counted via CopyObject — the GcWorker decrements ref_counts before freeing. It also includes `kFlagExternalTags` (0x08) for entries with external C:T child KV entries — the GcWorker issues `range_clear` to clean children on full deletion (see [background-and-admin.md](background-and-admin.md)).

Script: `./scripts/test_gc_delete_queue.sh` (env: `KVRGW_GC_DELETE_SEED`, `KVRGW_GC_DELETE_COUNT`).
