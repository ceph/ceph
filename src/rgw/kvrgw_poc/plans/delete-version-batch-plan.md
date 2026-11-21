# delete_multi() — Versioned Batch Optimization Plan

## Overview

The `objects` path in `KvRgwServiceImpl::delete_multi()` currently processes each
`(key, version_id)` entry serially — one transaction per entry — by calling
`delete_object_version()` in a loop. This is unnecessarily slow: each call opens its
own transaction, does its own bucket lookup, and issues independent FDB round-trips.

This plan replaces the serial loop with a batched, region-based implementation:

1. **Sort** the `objects` list by `(key ASC, version_id ASC)`.
2. **Group** into per-key **regions** (contiguous entries sharing the same key).
3. For each region, select a **read strategy** based on region shape.
4. Execute a **single transaction** per region: issue all reads concurrently where
   possible, apply all mutations, commit once.
5. Fall back to serial `delete_object_version()` per entry on repeated commit failure.

---

## Codebase Context

### Relevant files

| File | Role |
|------|------|
| `backend/src/service_impl.cpp` | Main implementation — all changes here |
| `backend/src/service_impl.hpp` | Declarations — add `VersionRegion`, `VersionDeleteContext`, new methods |
| `backend/src/keys.hpp` / `keys.cpp` | Key construction — out-param overloads already exist |
| `backend/src/object_value.hpp` | `ovh_*` zero-copy accessors, `ObjectValueHeader` |
| `backend/src/kv_store.hpp` | `FdbGetHolder`, `FdbRangeHolder`, `KvTransaction` API |

### Key existing patterns to follow

The non-versioned delete refactor (`delete_multi_try_commit` / `delete_apply`) is the
reference implementation. Follow these patterns exactly:

**Zero-copy reads — use `FdbGetHolder` / `FdbRangeHolder`, never `kv_wait_get`:**
```cpp
// Issue async get — stores FdbFuture (movable)
FdbFuture f_obj = tr.kv_async_get(obj_key.view());

// At resolution time — construct holder on-the-fly (non-movable, non-copyable)
FdbGetHolder holder(std::move(f_obj));
fdb_error_t err = holder.wait();
if (holder.present()) {
    std::string_view raw = holder.value();  // zero-copy, into FDB buffer
    const ObjectValueHeader* h = ovh_ptr(raw);
}

// Range scan — use FdbRangeHolder
FdbFuture f_scan = tr.kv_async_get_range(start.view(), false, end.view(), limit);
FdbRangeHolder scan(std::move(f_scan));
scan.wait();
for (const auto& [k, v] : scan) { /* k and v are string_view into FDB buffer */ }
bool sentinel_hit = scan.more() || scan.count() == limit;
```

**Zero-copy header access — use `ovh_*` accessors, never raw field access:**
```cpp
const ObjectValueHeader* h = ovh_ptr(raw_value);   // returns nullptr if too short
version_id_t vid  = ovh_version_id(h);              // reads big-endian correctly
version_id_t nvid = ovh_next_vid(h);
uint64_t     size = ovh_size(h);
uint32_t     mtime= ovh_last_modified_sec(h);
std::string_view ref = ovh_ref_tag(h);
bool is_dm = ovh_is_delete_marker(h);
```

**In-place key construction — use out-param overloads, never copy:**
```cpp
KeyBuf key_buf;                                          // stack-allocated, 1100 bytes
make_object_key(bucket_id, object_name, key_buf);        // writes in-place
make_v_key(bucket_id, object_name, vid, key_buf);        // writes in-place
make_go_key(sc, si, bucket_id, ref_tag, size, key_buf);  // writes in-place
make_d_key(bucket_id, st, ref_tag, mtime, key_buf);      // writes in-place
make_ct_key(bucket_id, ref_tag, key_buf);                // writes in-place
make_c_prefix(bucket_id, ref_tag, key_buf);              // writes in-place
tr.kv_put(key_buf.view(), value);   // pass string_view, no copy
tr.kv_del(key_buf.view());
tr.kv_range_clear(begin.view(), end_str);  // end must be std::string (see below)
```

**No dynamic allocation on hot path:**
- `FdbGetHolder` and `FdbRangeHolder` are non-movable, non-copyable — cannot be stored
  in containers. Store `FdbFuture` in structs; construct holders at resolution time.
- Pre-allocate per-region context arrays on the stack or as `unique_ptr[]` outside the
  retry loop (see `delete_multi` pattern: `unique_ptr<DeleteContext[]> ctx_buf(new DeleteContext[kChunkSize])`).
- Use `std::span<>` to pass pre-allocated arrays into functions.
- Use `out->emplace_back(...)` with the `DeleteMultiKeyOutcome` constructor overload —
  no intermediate structs.
- `kv_range_clear` end bound requires `std::string` (unavoidable); compute once.

**`FdbFuture` is movable but `FdbGetHolder`/`FdbRangeHolder` are not:**
```cpp
// OK — store FdbFuture in struct, move it in:
struct VersionDeleteContext {
    FdbFuture f_S;      // movable — stored until resolution
    FdbFuture f_V;      // movable
    FdbFuture f_scan1;  // movable
    FdbFuture f_scan2;  // movable
    KeyBuf    s_key;    // in-place, no heap
    KeyBuf    v_key;    // in-place, no heap
};
// At resolution time:
FdbGetHolder s_holder(std::move(ctx.f_S));   // non-movable, constructed on-the-fly
```

**`DeleteMultiKeyOutcome` constructor:**
```cpp
// Defined in service_impl.hpp:
DeleteMultiKeyOutcome(Status s, std::string k, std::string ec,
                      bool dm = false, version_id_t dm_vid = {})
out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted, std::string(key), "",
                  created_dm, dm_version_id);
```

### Existing reference: delete_apply()
`delete_apply()` in `service_impl.cpp:~1676` shows the full zero-copy pattern for
reading an object, checking conditions, calling `displace_old_object`, writing a
Delete-Marker, and using `ovh_*` accessors. Study this before implementing Axis 2.

### Existing reference: delete_multi_try_commit()
`delete_multi_try_commit()` in `service_impl.cpp:~1778` shows the full batch pattern:
pre-allocated `ctxs` span, pipeline all reads, verify bucket once, apply all mutations,
commit once, resize `out` on failure.

---

## Caller Context

The Go frontend (`backend_kvrgw.go::DeleteObjects`) always passes **either** `keys`
or `objects` — never both simultaneously. `objects` is used when any entry in the S3
`DeleteObjects` request has a `VersionId`; in that case ALL entries go into `objects`,
including those without a `VersionId` (which arrive as `version_id = nullopt`).

A `nullopt` entry means "delete whatever is current" — identical semantics to
`DeleteObject` on a versioned bucket.

---

## Key Invariants

- Version IDs are assigned in **descending** order: newer versions have **smaller**
  raw values. `kFirstVersionIdRaw = 0xFFFFFFFE` is the oldest; `kNullVersionRaw =
  0xFFFFFFFF` is the null sentinel.
- `S:{key}` holds the **current** (newest, smallest `version_id`) version. It is
  **not** part of the `V:` key space.
- `V:{key}{vid}` holds non-current (displaced) versions. FDB key order within a key's
  `V:` prefix is ascending by raw `vid` bytes — i.e. **newer versions appear first**
  in a forward scan (smaller raw value = earlier in key order).
- `S:.next_vid` (`ovh_next_vid(h)`) points to the most recently displaced version
  (the newest `V:` entry for this key, if any).
- A given `version_id` exists in exactly one of `S:` or `V:` — never both.

---

## Region Definition

A **region** is the set of all `objects` entries sharing the same `key`, after sorting.
Within a region, entries are ordered by `version_id` ascending (newer first, since
newer = smaller raw value). `nullopt` entries target `S:` directly and are handled
separately from explicit version entries.

Let:
- `region_vids` = set of explicit `version_id` values in the region (excludes nullopt)
- `vid_min` = smallest (newest) explicit version_id in region
- `vid_max` = largest (oldest) explicit version_id in region
- `has_nullopt` = region contains at least one nullopt entry
- `is_contiguous` = `vid_max - vid_min + 1 == region_vids.size()` (no gaps in the
  version ID sequence within the window)

"Delete all versions" naturally produces a contiguous region — no special API needed.

---

## Read Strategy Selection

```
if region.size == 1 AND has_nullopt:
    Strategy A — concurrent f_S + full-range scan1

elif region.size == 1 AND explicit vid:
    Strategy B — optimistic point get (f_V first, f_S only if not found)

elif is_contiguous:
    Strategy C — range scan + range-clear optimization

else:
    Strategy D — N concurrent point gets
```

---

## Strategy A — size=1, nullopt (unversioned delete)

Issue concurrently at transaction open:
```
KeyBuf s_key;  make_object_key(bucket_id, object_name, s_key);
KeyBuf v_start;  make_v_key(bucket_id, object_name, version_id_t{0}, v_start);
KeyBuf v_end;    make_v_key(bucket_id, object_name, version_id_t{kFirstVersionIdRaw}, v_end);

f_S     = tr.kv_async_get(s_key.view())
f_scan1 = tr.kv_async_get_range(v_start.view(), v_end.view(), limit=1)
```

`f_scan1` covers the full `V:` range at limit=1 — returns the newest non-current
version (promotion candidate) in parallel with `f_S`. No sequential dependency.

**Apply:**
```
FdbGetHolder s_holder(std::move(f_S));
FdbRangeHolder scan1(std::move(f_scan1));
s_holder.wait();  scan1.wait();

if NOT s_holder.present():
    no-op (key doesn't exist)
    return

// S: present — delete it
const ObjectValueHeader* h = ovh_ptr(s_holder.value());
write G: (make_go_key out-param) if h->chunk.type indicates data
kv_del(s_key.view())

if scan1.count() > 0:
    const auto& [pk, pv] = *scan1.begin()   // newest V: entry = promotion candidate
    // patch next_vid from current h->next_vid, write promoted value to S:
    kv_put(s_key.view(), promoted_raw)
    kv_del(pk)   // pk is string_view into FDB buffer — valid until scan1 destroyed
else:
    write Delete-Marker at s_key.view()
```

---

## Strategy B — size=1, explicit version_id (optimistic)

Issue only — assume non-current (lives in `V:`):
```
KeyBuf v_key;  make_v_key(bucket_id, object_name, vid, v_key);
f_V = tr.kv_async_get(v_key.view())
```

**Apply:**
```
FdbGetHolder v_holder(std::move(f_V));
v_holder.wait();

if v_holder.present():
    // Non-current version found in V: — common case
    const ObjectValueHeader* h = ovh_ptr(v_holder.value());
    write G: if has data
    kv_del(v_key.view())
    // f_S never issued — done

else:
    // Not in V: — may be current (S:) or not exist
    KeyBuf s_key;  make_object_key(bucket_id, object_name, s_key);
    f_S = tr.kv_async_get(s_key.view())    // sequential second read
    FdbGetHolder s_holder(std::move(f_S));
    s_holder.wait();

    if s_holder.present() AND ovh_version_id(ovh_ptr(s_holder.value())) == vid:
        // Current version — Axis 2 logic (promote/DM, same as Strategy C Axis 2)
    else:
        no-op (version doesn't exist)
```

Best case (non-current, common): **1 read total, no heap**.
Worst case (current): **2 sequential reads**.

---

## Strategy C — contiguous region, size >= 2

Issue concurrently at transaction open:
```
KeyBuf s_key;    make_object_key(bucket_id, object_name, s_key);
KeyBuf scan_start;  make_v_key(bucket_id, object_name, vid_min - 1, scan_start);
KeyBuf scan_end;    make_v_key(bucket_id, object_name, kFirstVersionId, scan_end);
const int limit = std::min(kMaxKeysPerScan, static_cast<int>(region.size) + 2);

f_S     = tr.kv_async_get(s_key.view())
f_scan1 = tr.kv_async_get_range(scan_start.view(), scan_end.view(), limit)
```

`f_scan1` starts just below `vid_min` (one step older) and scans toward the oldest
version. Since the region is contiguous, no interleaved survivors should exist within
`[vid_min, vid_max]`. Scan finds older survivors (promotion candidates) and confirms
range-clear is safe.

After resolving `f_S`, conditionally issue `f_scan2`:
```
FdbGetHolder s_holder(std::move(f_S));  s_holder.wait();
const ObjectValueHeader* s_hdr = s_holder.present() ? ovh_ptr(s_holder.value()) : nullptr;

s_targeted = has_nullopt OR (s_hdr AND ovh_version_id(s_hdr) ∈ region_vids)

if s_targeted AND s_hdr AND ovh_next_vid(s_hdr) < vid_min - 1:
    // Gap between S:.next_vid and scan1 start — survivors may exist in gap
    KeyBuf gap_start;  make_v_key(bucket_id, object_name, ovh_next_vid(s_hdr), gap_start);
    KeyBuf gap_end;    make_v_key(bucket_id, object_name, vid_min - 1, gap_end);
    f_scan2 = tr.kv_async_get_range(gap_start.view(), gap_end.view(), limit=1)
    // f_scan2 finds nearest survivor newer than the region (takes priority for promotion)
```

### Axis 1 — Non-Current V: Cleanup

```
FdbRangeHolder scan1(std::move(f_scan1));  scan1.wait();
sentinel_hit = scan1.more() || scan1.count() == limit

extras = scan1 entries whose vid ∉ region_vids
interleaved = extras where vid_min < vid < vid_max   // should be empty (contiguous)
older_survivors = extras where vid > vid_max          // sorted: first = newest = closest

if interleaved.empty():
    // Safe — one range mutation instead of N individual deletes
    KeyBuf rc_start;  make_v_key(bucket_id, object_name, vid_min - 1, rc_start);
    KeyBuf rc_end_k;  make_v_key(bucket_id, object_name, vid_max + 1, rc_end_k);
    const std::string rc_end(rc_end_k.view());   // unavoidable std::string for range_clear end
    tr.kv_range_clear(rc_start.view(), rc_end);
else:
    // Unexpected interleaved survivors (concurrent write race?) — per-entry fallback
    KeyBuf del_key;
    for each vid in region_vids where vid ≠ ovh_version_id(s_hdr):
        make_v_key(bucket_id, object_name, vid, del_key);
        tr.kv_del(del_key.view());

// GC writes for non-current region entries with data
KeyBuf gc_key;
for each scan1 entry in region_vids (found in V:):
    const ObjectValueHeader* vh = ovh_ptr(entry.value);
    if vh->chunk.type indicates data:
        make_go_key(shard_count, shard_id, bucket_id, ovh_ref_tag(vh), ovh_size(vh), gc_key);
        tr.kv_put(gc_key.view(), make_gc_value(...));
```

### Axis 2 — Current Version (S:) Handling

```
if NOT s_targeted:
    return   // S: survives untouched

// S: is being deleted
const ObjectValueHeader* s_hdr = ovh_ptr(s_holder.value());
KeyBuf gc_key;
if s_hdr->chunk.type indicates data:
    make_go_key(..., gc_key);
    tr.kv_put(gc_key.view(), make_gc_value(...));

// Resolve promotion candidate — newest surviving version wins
// Priority 1: f_scan2 result (newer-than-region survivors)
// Priority 2: older_survivors.first() from scan1 (older-than-region survivors)
// Priority 3: none → write Delete-Marker

std::string_view promote_key{};    // key into FDB buffer — zero-copy
std::string_view promote_val{};

if f_scan2 issued:
    FdbRangeHolder scan2(std::move(f_scan2));  scan2.wait();
    if scan2.count() > 0:
        auto [pk, pv] = *scan2.begin();
        promote_key = pk;  promote_val = pv;

if promote_key.empty() AND NOT older_survivors.empty() AND NOT sentinel_hit:
    auto [pk, pv] = older_survivors.front();
    promote_key = pk;  promote_val = pv;

// Promotion-skip: if promote candidate is itself in region_vids → Axis 1 deletes it
if NOT promote_key.empty():
    version_id_t cand_vid = ovh_version_id(ovh_ptr(promote_val));
    if cand_vid ∈ region_vids:
        promote_key = {};   // skip — write DM instead

if NOT promote_key.empty():
    // Promote: patch next_vid from s_hdr, write to S:
    // Must copy promoted raw value to patch next_vid field (one memcpy of OValueBuf size)
    OValueBuf pbuf;
    // copy promote_val bytes into pbuf, patch hdr.next_vid = ovh_next_vid(s_hdr)
    tr.kv_put(s_key.view(), pbuf.view());
    tr.kv_del(promote_key);   // promote_key is string_view into FDB buffer
else:
    // Write Delete-Marker
    ObjectValue dm;
    dm.hdr.flags = ObjectValue::kFlagFenced;
    auto ids = compute_new_version(bucket_state.versioning_state, s_hdr);
    dm.hdr.version_id = ids.version_id;
    dm.hdr.next_vid   = ids.next_vid;
    dm.hdr.chunk.type = CHUNK_INLINE;
    OValueBuf dm_buf;
    write_object_value(dm_buf, dm);
    tr.kv_put(s_key.view(), dm_buf.view());

// nullopt + explicit-vid interaction:
// If has_nullopt AND ovh_version_id(s_hdr) ∈ region_vids:
//   Axis 1 already handles V: delete for that vid — no displace needed.
```

---

## Strategy D — non-contiguous region, size >= 2

Issue concurrently at transaction open:
```
KeyBuf s_key;  make_object_key(bucket_id, object_name, s_key);
f_S = tr.kv_async_get(s_key.view())

// Per-entry V: point gets — use pre-allocated VersionDeleteContext[] array
// (allocated once outside the retry loop, reused across regions)
for i in 0..region.size:
    KeyBuf v_key;  make_v_key(bucket_id, object_name, region_vids[i], ctxs[i].v_key);
    ctxs[i].f_V = tr.kv_async_get(ctxs[i].v_key.view());
```

**Apply:**
```
// Resolve all V: point gets
KeyBuf gc_key;
for i in 0..region.size:
    FdbGetHolder v_holder(std::move(ctxs[i].f_V));
    v_holder.wait();
    if v_holder.present():
        write G: if has data (make_go_key out-param into gc_key)
        tr.kv_del(ctxs[i].v_key.view())
    // else: not in V:, may be at S:, handled by Axis 2

// Axis 2 — same logic as Strategy C Axis 2
// f_scan2 uses full range [f_S.next_vid, kFirstVersionIdRaw], limit=1
FdbGetHolder s_holder(std::move(f_S));  s_holder.wait();
// ... same s_targeted / promote / DM logic
```

No range-clear — individual `kv_del` per entry. No wasted range scan even for keys
with thousands of versions.

---

## Commit, Retry, Fallback

```
commit transaction

on success:
    append DeleteMultiKeyOutcome(Deleted/Error) per entry to out (emplace_back)

on retriable conflict:
    retry entire region (re-issue all reads, re-apply)
    max retries = kMaxRetries (same as delete_object_version: 3)

on non-retriable error OR threshold exceeded:
    fall back: call delete_object_version() per entry serially
    (existing correct implementation — no duplication of logic)
```

---

## Memory / Allocation Rules

| Resource | Rule |
|----------|------|
| `KeyBuf` (1100 bytes) | Always stack-allocated or in pre-allocated array; never `new KeyBuf` |
| `FdbFuture` | Stored in `VersionDeleteContext` struct; moved into holder at resolution |
| `FdbGetHolder` / `FdbRangeHolder` | Constructed on-the-fly at resolution site; never stored |
| `VersionDeleteContext[]` | Allocated once as `unique_ptr<VersionDeleteContext[]>` outside retry loop |
| `string_view` from FDB | Valid until holder destroyed — do not outlive the holder |
| `OValueBuf` for promoted value | Stack-allocated; one memcpy to patch `next_vid` |
| Range-clear end bound | One `std::string` per range-clear call (unavoidable) |
| `out->emplace_back(...)` | Use `DeleteMultiKeyOutcome` constructor overload directly |

---

## Performance Characteristics

| Strategy | Reads | Mutations | When |
|----------|-------|-----------|------|
| A (nullopt, size=1) | 2 concurrent | 1 del + optional G: + promote/DM | Unversioned single delete |
| B (explicit, size=1) | 1 optimistic; 2 sequential if current | 1 del + optional G: | Single version delete |
| C (contiguous, size≥2) | 2 concurrent + optional scan2 | 1 range-clear + optional G:/promote/DM | Dense/contiguous batch |
| D (non-contiguous, size≥2) | N+1 concurrent + optional scan2 | N del + optional G:/promote/DM | Sparse/fragmented batch |

**Pathological case (Strategy D):** a key with 10,000 versions and a region of 3
random versions pays exactly 4 reads (`f_S` + 3 point gets) — no wasted range scan.
Still reduces N transactions → 1 transaction per region regardless of strategy.

**"Delete all versions"** is naturally a contiguous region → Strategy C → single
`kv_range_clear` + 2 reads. No special API needed.

---

## Sub-Tasks

### Sub-Task 1 — Sort and Region-Split
**Intent:** Establish the sorted, grouped structure all subsequent sub-tasks depend on.

**Todo:**
1. Sort `objects` by `(key ASC, version_id ASC)` — nullopt sorts as `version_id_t{0}`
   (newest, smallest raw value) for sort purposes; handled specially in Axis 2.
2. Define `VersionRegion`:
   ```cpp
   struct VersionRegion {
       std::string_view key;
       std::span<const DeleteMultiObjectRef> entries;
       version_id_t vid_min{};
       version_id_t vid_max{};
       bool has_nullopt{false};
       bool is_contiguous{false};
   };
   ```
3. Write region iterator that walks sorted list and yields one `VersionRegion` per
   unique key, computing `vid_min`, `vid_max`, `has_nullopt`, `is_contiguous`.
4. Since `objects` is `std::span<const DeleteMultiObjectRef>` (cannot sort in-place),
   build a `std::vector<size_t> sorted_idx` index sorted by `(key, version_id)` and
   iterate regions via the index.

**Relevant files:** `service_impl.hpp` (add `VersionRegion`), `service_impl.cpp:~3089`

**Status:** `[ ] pending`

---

### Sub-Task 2 — Strategy A: nullopt, size=1
**Intent:** Unversioned single-key delete — concurrent `f_S` + full-range scan1.

**Todo:**
1. Issue `f_S` + `f_scan1([vid=0, vid=kFirstVersionIdRaw], limit=1)` concurrently.
2. Resolve via `FdbGetHolder` / `FdbRangeHolder` (zero-copy).
3. Apply: absent → no-op; present → GC write (if data) + promote from scan1 or write DM.
4. All key construction via out-param overloads into stack `KeyBuf`.

**Status:** `[ ] pending`

---

### Sub-Task 3 — Strategy B: explicit vid, size=1
**Intent:** Optimistic point get — skip `f_S` entirely for the common non-current case.

**Todo:**
1. Issue only `f_V = tr.kv_async_get(v_key.view())`.
2. Resolve via `FdbGetHolder`; if present → `kv_del` + optional GC, done.
3. If not present → issue `f_S` sequentially; resolve; if `S:.version_id == vid`
   → Axis 2 (promote/DM); else no-op.
4. Axis 2 shared with Strategy C — extract as helper or inline.

**Status:** `[ ] pending`

---

### Sub-Task 4 — Strategy C: contiguous, size≥2
**Intent:** Range scan + range-clear optimization for contiguous regions.

**Todo:**
1. Issue `f_S` + `f_scan1([vid_min-1, kFirstVersionIdRaw], limit=min(kMaxKeysPerScan, size+2))`
   concurrently.
2. Resolve `f_S` via `FdbGetHolder`; if gap (`next_vid < vid_min - 1`) and `S:` targeted
   → issue `f_scan2([next_vid, vid_min-1], limit=1)`.
3. Axis 1: resolve `f_scan1` via `FdbRangeHolder`; classify extras/interleaved/sentinel;
   `kv_range_clear` or per-entry `kv_del` fallback; GC writes.
4. Axis 2: GC write for `S:`; resolve `f_scan2` if issued; resolve promotion candidate
   (scan2 priority 1, older_survivors priority 2); promotion-skip check; `kv_put`
   promoted value (patch `next_vid`) or write Delete-Marker.
5. nullopt + explicit-vid interaction (skip displace when both present).
6. All key construction via out-param overloads; reuse single `KeyBuf key_buf` across
   mutations where slots are non-overlapping.

**Relevant files:**
- `kv_store.hpp:271,283,290` (`kv_async_get_range`, `FdbRangeHolder`, `kv_range_clear`)
- `object_value.hpp` (`ovh_ptr`, `ovh_version_id`, `ovh_next_vid`, `ovh_ref_tag`, `ovh_size`)
- `service_impl.cpp:~1676` (`delete_apply` — reference for DM construction)
- `service_impl.hpp:556` (`compute_new_version(ObjectValueHeader*)`)

**Status:** `[ ] pending`

---

### Sub-Task 5 — Strategy D: non-contiguous, size≥2
**Intent:** N concurrent point gets for sparse/fragmented regions — no wasted range scan.

**Todo:**
1. Pre-allocate `VersionDeleteContext[]` array (size = max region size) outside retry
   loop — reuse across regions within same `delete_version_try_commit` call.
   ```cpp
   struct VersionDeleteContext {
       FdbFuture f_V;
       KeyBuf    v_key;
   };
   ```
2. Issue `f_S` + `f_V[i]` for all explicit vids concurrently.
3. Resolve each `f_V[i]` via `FdbGetHolder`; `kv_del` + GC if found; skip if not.
4. Axis 2: same as Strategy C but `f_scan2` uses full range
   `[f_S.next_vid, kFirstVersionIdRaw], limit=1`.

**Status:** `[ ] pending`

---

### Sub-Task 6 — Commit, Retry, Fallback
**Intent:** Commit per-region transaction; retry on conflict; serial fallback on failure.

**Todo:**
1. Wrap region logic in retry loop (`kMaxRetries = 3`).
2. On retriable conflict (`is_retriable(ec)`) → retry; re-issue all reads, re-apply.
3. On threshold exceeded → serial `delete_object_version()` per entry in region.
4. On success → `out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted, ...)` per entry.
5. On failure → `out->emplace_back(DeleteMultiKeyOutcome::Status::Error, ...)` per entry.
6. Track `base = out->size()` before apply; `out->resize(base)` on commit failure
   (same pattern as `delete_multi_try_commit`).

**Relevant files:** `service_impl.cpp:2893` (`delete_object_version`); `is_retriable()`

**Status:** `[ ] pending`

---

### Sub-Task 7 — Wire into delete_multi()
**Intent:** Replace the serial `objects` loop with the new region-based path.

**Todo:**
1. Add to `KvRgwServiceImpl` in `service_impl.hpp`:
   ```cpp
   bool delete_version_try_commit(
       tenant_id_t tenant_id,
       const std::string& bucket_name,
       bucket_id_t bucket_id,
       const VersionRegion& region,
       std::vector<DeleteMultiKeyOutcome>* out);
   ```
2. Replace the `if (!objects.empty())` serial loop in `delete_multi()` with:
   - Build sorted index (Sub-Task 1).
   - Iterate regions; call `delete_version_try_commit` per region.
   - On failure → serial fallback per entry.
3. Remove the TBD comment on the old loop.
4. `delete_object_version()` is retained unchanged as the serial fallback.

**Relevant files:** `service_impl.cpp:~3119`; `service_impl.hpp`

**Status:** `[ ] pending`

---

## Constants

| Name | Value | Meaning |
|------|-------|---------|
| `kNullVersionRaw` | `0xFFFFFFFF` | Null/sentinel version (`constants.hpp`) |
| `kFirstVersionIdRaw` | `0xFFFFFFFE` | Oldest possible version (`constants.hpp`) |
| `kMaxKeysPerScan` | TBD (~100) | Cap on scan1 limit — guards against dense version chains |
| `kMaxRetries` | `3` | Max transaction retry attempts before serial fallback |

---

## Files Affected

| File | Change |
|------|--------|
| `backend/src/service_impl.hpp` | Add `VersionRegion`, `VersionDeleteContext`, `delete_version_try_commit` |
| `backend/src/service_impl.cpp` | New batch path; replace serial `objects` loop in `delete_multi()` |
| `backend/src/keys.hpp` / `keys.cpp` | No new changes expected (out-param overloads already exist) |

---

## Appendix — Delete Bucket Contents (Future Work)

> **Status:** Design only. Implementation deferred until delete-multi versioned batch is complete.

### Overview

A new `delete_bucket_contents()` operation that efficiently wipes all objects from a
bucket using pipelined range scans, batched GC writes (`GroupGcValue`), and
`kv_range_clear`. Two sequential phases: V: domain first, then O: domain.

### Infrastructure Already Available

| Component | Location |
|-----------|----------|
| `GroupGcEntry` / `GroupGcValue` | `gc_value.hpp` |
| `make_group_gc_value` / `parse_group_gc_value` | `gc_value.hpp/.cpp` |
| `make_group_go_key` | `keys.hpp` |
| GC worker `GroupGcValue` handling | `gc_worker.cpp:124-147` |
| `kMaxBatchSize = 16` | `constants.hpp` |
| `kv_range_clear` | `kv_store.hpp:290` |
| `FdbRangeHolder` (zero-copy range scan) | `kv_store.hpp:283` |

### Pipelined Scan Loop (applies to both Phase 1 and Phase 2)

```
issue f_scan[0] = kv_async_get_range(domain_start, domain_end, limit=1000)
consecutive_failures = 0

loop:
    wait f_scan[i]
    if scan[i].count() == 0: break

    last_key = scan[i].back().key
    if scan[i].count() == limit OR scan[i].more():
        issue f_scan[i+1] = kv_async_get_range(last_key+1, domain_end, limit=1000)
        // ↑ in flight while current batch is processed

    // Process current batch (while f_scan[i+1] is in flight):
    for attempt in 0..kMaxRetries:
        open txn
        accumulator = GroupGcEntry[kMaxBatchSize], count=0
        for each entry in scan[i]:
            h = ovh_ptr(entry.value)
            if has_data(h):
                accumulator[count++] = {ovh_ref_tag(h), h->chunk, h->flags, ovh_size(h)}
                if count == kMaxBatchSize:
                    make_group_go_key(bucket_id, accumulator[0].ref_tag, gc_key)
                    kv_put(gc_key.view(), make_group_gc_value(accumulator, count))
                    count = 0
        if count > 0:
            make_group_go_key(bucket_id, accumulator[0].ref_tag, gc_key)
            kv_put(gc_key.view(), make_group_gc_value(accumulator, count))
        kv_range_clear(scan[i].first().key, last_key+1)
        if commit() succeeds:
            consecutive_failures = 0
            break
        if attempt == kMaxRetries - 1:
            // retry exhausted — skip range, do NOT kv_range_clear
            had_failure = true
            consecutive_failures++
            if consecutive_failures >= kMaxConsecutiveFailures:
                return ERROR  // abort entire operation

    if scan[i].count() < limit AND NOT scan[i].more(): break
```

### Phase 1 — V: domain

```
domain_start = make_v_prefix(bucket_id).view()
domain_end   = prefix_range_end(domain_start)
```

Run pipelined scan loop above.

### Phase 2 — O: domain

```
domain_start = make_object_prefix(bucket_id).view()
domain_end   = prefix_range_end(domain_start)
```

Run pipelined scan loop above.

### Bucket Removal (post-Phase-2)

After both phases complete, in a single transaction:
```
f_scan_V = kv_async_get_range(V:{bucket}[start], V:{bucket}[end], limit=1)
f_scan_O = kv_async_get_range(O:{bucket}[start], O:{bucket}[end], limit=1)
// both concurrent
wait both

if had_failure OR scan_V.count() > 0 OR scan_O.count() > 0:
    // data remains — do not remove bucket
    return INCOMPLETE
else:
    kv_del(B:{bucket_key})
    invalidate_bucket_cache(tenant_id, bucket_name)
    return OK
```

If `INCOMPLETE`, caller may retry the entire operation or report partial completion.

### Error Handling Constants

| Name | Value | Meaning |
|------|-------|---------|
| `kScanPageSize` | `1000` | Entries per range scan page |
| `kMaxRetries` | `3` | Per-range transaction retry limit |
| `kMaxConsecutiveFailures` | `5` | Consecutive range failures before abort |

### GC Worker Note

The GC worker already handles `GroupGcValue` (lines 124–147 of `gc_worker.cpp`) but
only calls `data_store_.remove()` — it does not handle `CHUNK_CHILD_D` /
`CHUNK_INLINE` cases within the group. This must be fixed before `delete_bucket_contents`
goes into production.

### Files Affected (future)

| File | Change |
|------|--------|
| `backend/src/service_impl.hpp` | Add `delete_bucket_contents()` |
| `backend/src/service_impl.cpp` | Implement pipelined scan loop |
| `backend/src/gc_worker.cpp` | Handle all chunk types in `GroupGcValue` path |

---

## Appendix B — Delete Object With All Versions (Future Work)

> **Status:** Design only. Implementation deferred until delete-multi versioned batch is complete.

### Overview

A new `delete_all_versions(key, leave_dm)` operation that removes all `V:` entries
for a specific key and handles the `O:` entry, using the same pipelined drain logic
as `delete_bucket_contents` (Appendix A).

### Shared Infrastructure — `drain_v_range()`

Both `delete_all_versions` and `delete_bucket_contents` Phase 1 use an identical V:
drain loop. Extract as a single reusable helper:

```cpp
KvrgwErrorCode drain_v_range(
    KvStore&        store,
    bucket_id_t     bucket_id,
    std::string_view v_start,     // inclusive scan start
    std::string_view v_end,       // exclusive scan end
    bool*           had_failure); // out: true if any range was skipped
```

Same constants as Appendix A: `kScanPageSize=1000`, `kMaxBatchSize=16`,
`kMaxRetries=3`, `kMaxConsecutiveFailures=5`. Same overlap-scan loop, same GC
aggregation, same error handling rules.

**Callers:**

| Caller | `v_start` / `v_end` |
|--------|---------------------|
| `delete_all_versions(key)` | `make_v_prefix(bucket_id, key)` → `prefix_range_end` |
| `delete_bucket_contents()` Phase 1 | `make_v_prefix(bucket_id)` → `prefix_range_end` |

### Algorithm

**Step 1 — Fast-path probe (single txn):**
```
f_V = kv_async_get_range(v_start, v_end, limit = kScanPageSize + 1)
f_O = kv_async_get(O:{key})
// both concurrent

wait both

if f_V.count() <= kScanPageSize AND NOT f_V.more():
    // all versions fit in one txn — fast path
    aggregate f_V entries into GroupGcValue batches → kv_put G:V entries
    kv_range_clear(v_start, v_end)
    apply O: logic (see below)
    commit → done

else:
    // slow path — drain V: domain iteratively
    drain_v_range(bucket_id, v_start, v_end, &had_failure)
    // then confirmation txn (Step 2)
```

**Step 2 — Confirmation txn (slow path only):**
```
f_V_confirm = kv_async_get_range(v_start, v_end, limit = kScanPageSize + 1)
f_O         = kv_async_get(O:{key})
// both concurrent

wait both

if had_failure OR (f_V_confirm.count() > kScanPageSize OR f_V_confirm.more()):
    return INCOMPLETE  // do not touch O:

// V: is clean — aggregate any remaining entries + range-clear + apply O:
aggregate f_V_confirm entries → GC
kv_range_clear(v_start, v_end)
apply O: logic (see below)
commit
```

**O: entry logic (`leave_dm` flag):**
```
if f_O.present():
    h = ovh_ptr(f_O.value())
    if NOT ovh_is_delete_marker(h):
        write G: entry for O: (data needs GC)
    if leave_dm:
        write Delete-Marker at O:{key}   // key remains logically deleted, visible in list-versions
    else:
        kv_del(O:{key})                  // key fully absent from namespace
else:
    NOP
```

| `leave_dm` | Use case |
|-----------|----------|
| `true` | S3 `DeleteObjects` semantics — DM left in place |
| `false` | Full wipe — bucket drain, lifecycle hard-delete |

### Files Affected (future)

| File | Change |
|------|--------|
| `backend/src/service_impl.hpp` | Add `drain_v_range()`, `delete_all_versions()` |
| `backend/src/service_impl.cpp` | Implement both; share `drain_v_range` with `delete_bucket_contents` |
