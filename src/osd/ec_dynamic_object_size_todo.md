# EC Dynamic Per-Object Chunk Size — Unimplemented Work

This file records everything discussed but not yet implemented for the
`FLAG_DYNAMIC_OBJECT_SIZE` feature on the `variable_sized_objects` branch.
It is intended as a starting point for a future AI or developer picking up
this work.

The companion design document is:
`doc/dev/osd_internals/erasure_coding/dynamic_object_size.rst`

All deferred call sites in the OSD carry the comment
`// TODO(dynamic-object-size)` — run `git grep 'TODO(dynamic-object-size)'`
to find them all.

---

## 1. Direct reads and split reads

This is the largest unimplemented area. The design is fully specified in
Section 7 of the design doc. The pieces must land roughly together.

### 1a. `Objecter::Op::ec_object_size_hint` field

**File:** `src/osdc/Objecter.h` — `struct Op`

**What is missing:** `Objecter::Op` has no field for an object-size hint.
Without it, `SplitOp::create()` cannot derive the correct per-object chunk
size and always falls back to sending reads to the primary, losing the
split-read performance benefit.

**What to do:**

Add to `struct Op`:
```cpp
uint64_t ec_object_size_hint = 0;  // 0 = unknown; object size in bytes
```

In `SplitOp::create()` (`src/osdc/SplitOp.cc`), after the `pg_pool_t*`
is validated, add:
```cpp
uint64_t ec_chunk_size = 0;
if (pi->allows_dynamic_object_size() && op->ec_object_size_hint != 0) {
    ECUtil::stripe_info_base_t tmp_sinfo(ec_impl, pi);
    ec_chunk_size = tmp_sinfo.chunk_size_for_hint(
        op->ec_object_size_hint,
        tmp_sinfo.get_max_dynamic_chunk_size());
}
```

This requires adding `#include "osd/ECUtil.h"` to `SplitOp.cc` (it already
transitively depends on `osd/osd_types.h` and `osd/ECTypes.h`).

Thread `ec_chunk_size` into:
- `ECSplitOp::init_read()` — shard selection arithmetic
- `ECStripeView` constructor — chunk size used for stripe iteration
- `ECSplitOp::assemble_buffer_read()` — buffer assembly
- `ECSplitOp::assemble_buffer_sparse_read()` — sparse buffer assembly
- `prepare_single_op()` — single-chunk direct-read routing

When `ec_object_size_hint == 0` and the pool has `FLAG_DYNAMIC_OBJECT_SIZE`,
`SplitOp::create()` must return `false` immediately (fall through to primary
routing). This is **Option D** from the design discussion — correct but loses
the split optimisation. See item 6 for the future **Option B** upgrade.

---

### 1b. librados API: `set_ec_object_size_hint` on read operations

**Files:** `src/include/rados/librados.hpp`, `src/include/rados/librados.h`,
`src/librados/librados.cc`

**What is missing:** `ObjectReadOperation` has no method for an object-size
hint. `set_alloc_hint` exists only on `ObjectWriteOperation` and serves a
different purpose.

**What to do:**

Add to `ObjectReadOperation` in `librados.hpp`:
```cpp
void set_ec_object_size_hint(uint64_t object_size);
```

Add to the C API in `librados.h`:
```c
void rados_read_op_set_ec_object_size_hint(
    rados_read_op_t op, uint64_t object_size);
```

Implement in `librados.cc` to store the value on the `ObjectOperation` and
transfer it to `Objecter::Op::ec_object_size_hint` inside
`prepare_read_op()` / `read()`.

---

### 1c. neorados API: `ec_object_size_hint` modifier on `ReadOp`

**Files:** `src/include/neorados/RADOS.hpp`, `src/neorados/RADOS.cc`

**What is missing:** `neorados::ReadOp` has no fluent modifier for an
object-size hint.

**What to do:**

Add to `ReadOp` in `RADOS.hpp`, following the pattern of `balance_reads()`:
```cpp
ReadOp& ec_object_size_hint(uint64_t object_size) &;
ReadOp&& ec_object_size_hint(uint64_t object_size) && {
    return std::move(ec_object_size_hint(object_size));
}
```

Implement in `RADOS.cc` to store the value and forward it to
`Objecter::Op::ec_object_size_hint` through the existing `RADOS::execute()`
path.

---

### 1d. RGW call-site wiring

**Files:** `src/rgw/rgw_common.h` (`prepare_read()`, `prepare_op_for_read()`
are the likely injection points)

**What is missing:** RGW does not supply the object content-length when
building read operations, even though it always has this information from its
SAL / manifest metadata (`rgw_obj_size`).

**What to do:**

At each RGW call site that constructs a `neorados::ReadOp` with
`balance_reads()`, add the hint where the object size is already known:
```cpp
// Before
op.balance_reads().read(off, len, &bl);

// After
op.balance_reads()
  .ec_object_size_hint(obj_size)   // from rgw_obj_size / manifest
  .read(off, len, &bl);
```

When the object size is not yet known (e.g. pre-manifest fetch, listing),
omit the hint — the op falls back to primary routing automatically.

---

### 1e. `objects_read_local()` — missing `ec_chunk_size` parameter

**Files:** `src/osd/PGBackend.h`, `src/osd/ECBackend.h`,
`src/osd/ECBackend.cc:1273`, `src/osd/ECSwitch.h:277`,
`src/osd/PrimaryLogPG.cc:6001`

**What is missing:** `ECBackend::objects_read_local()` calls
`extent_to_shard_extent(off, len)` with no `chunk_size` argument, always
using pool-default geometry. This computes a wrong shard-local file offset
for objects with a non-default `oi.ec_chunk_size`. The caller
(`PrimaryLogPG`) has `oi.ec_chunk_size` in scope and passes it to every
other EC read path but omits it here.

**What to do:**

1. Add `uint64_t ec_chunk_size = 0` parameter to the virtual in
   `PGBackend.h`:
   ```cpp
   virtual int objects_read_local(const hobject_t &hoid,
       uint64_t off, uint64_t len, uint32_t op_flags,
       ceph::buffer::list *bl, uint64_t ec_chunk_size = 0) = 0;
   ```

2. In `ECBackend::objects_read_local()` forward it:
   ```cpp
   auto [shard_offset, shard_len] =
       extent_to_shard_extent(off, len, ec_chunk_size);
   ```

3. In `PrimaryLogPG` pass `oi.ec_chunk_size`:
   ```cpp
   pgbackend->objects_read_local(
       soid, op.extent.offset, op.extent.length,
       op.flags, &osd_op.outdata, oi.ec_chunk_size);
   ```

4. Update `ECSwitch::objects_read_local()` to forward the new parameter to
   the optimized backend. `ECBackendL` and `ReplicatedBackend` should accept
   but ignore it.

---

### 1f. Encode `ec_chunk_size` in `MOSDOpReply`

**File:** `src/messages/MOSDOpReply.h`

**What is missing:** `MOSDOpReply` carries no `ec_chunk_size` field. The
Objecter cannot learn the correct per-object chunk size from a reply, which
is needed to prime the hint cache on retry and for the `-EAGAIN` mismatch
path (item 1g).

**What to do:**

- Add `uint64_t ec_chunk_size = 0;` as a member of `MOSDOpReply`.
- Bump `HEAD_VERSION` from 8 to 9.
- In `encode_payload()`: encode the field after the existing version-8 fields
  when non-zero (zero means "not applicable" so old clients are unaffected).
- In `decode_payload()`: decode when `header.version >= 9`.
- In `PrimaryLogPG` reply construction: populate the field from
  `oi.ec_chunk_size` for direct-read replies on dynamic pools.
- In `Objecter::handle_osd_op_reply()` (`src/osdc/Objecter.cc:3814`): read
  the field from the reply and store it in `op->ec_chunk_size`.

---

### 1g. OSD mismatch detection and `-EAGAIN` (prerequisite for Option B)

**File:** `src/osd/PrimaryLogPG.cc` — `ec_direct_read()` branch (~line 6000)

**What is missing:** The OSD does not detect when the `ec_chunk_size`
embedded in an incoming direct-read op (derived by the Objecter from the
caller's hint) differs from `oi.ec_chunk_size`, and does not return
`-EAGAIN` in that case.

**What to do:**

Before calling `objects_read_local()`, compare the op's `ec_chunk_size`
against `oi.ec_chunk_size` (treating 0 as pool-default on both sides). On
mismatch, return `-EAGAIN` and populate the reply's `ec_chunk_size` field
(item 1f) with the correct value so the Objecter can retry with the right
geometry.

This item is a prerequisite for **Option B** (item 6) and also makes hint
mismatches due to stale hints or pool upgrades self-healing.

---

## 2. Rollback visitors

**File:** `src/osd/PGBackend.cc` — two locations marked
`TODO(dynamic-object-size)` (~lines 217 and 302)

**What is missing:**

`PGBackend::RollbackVisitor` translates a `pg_log_entry_t` rollback
description into an `ObjectStore::Transaction`.  Two of its visitor
overrides are broken for objects with a non-default `ec_chunk_size`:

- `append()` (~line 219) — calls
  `object_size_to_shard_size(old_size, shard, /*chunk_size=*/0)`, resolving
  to pool-default geometry, so `rollback_append()` truncates to the wrong
  shard offset.
- `rollback_extents()` (~line 304) — same bug: wrong shard boundaries and
  extent lengths cause `rollback_extents()` to clone the wrong byte range.

The root cause is that `ObjectModDesc::append()` encodes only `old_size`,
and the version-3 `ObjectModDesc::rollback_extents()` encodes
`gen, extents, object_size, shards` — but neither record stores
`ec_chunk_size`.  The correct value is the chunk size the object had
*before* the write being rolled back (`WritePlanObj::chunk_size`, which
equals `sinfo.effective_chunk_size(obc->obs.oi)` evaluated before the
transaction updates the OI).

Three options were analysed (full discussion in Section 6 of
`doc/dev/osd_internals/erasure_coding/dynamic_object_size.rst`):

### Option A — Store `ec_chunk_size` in the mod_desc records *(recommended)*

**Changes required:**

1. **`src/osd/osd_types.h`** — `ObjectModDesc`:
   - `append(uint64_t old_size)` → `append(uint64_t old_size, uint64_t ec_chunk_size = 0)`; bump `ENCODE_START` to version 2, encode `ec_chunk_size` after `old_size`.
   - Version-3 `rollback_extents(gen, extents, object_size, shards)` → add `uint64_t ec_chunk_size = 0` parameter; bump `ENCODE_START` to version 4, encode `ec_chunk_size` after `shards`.
   - `Visitor::append(uint64_t old_size)` → `Visitor::append(uint64_t old_size, uint64_t ec_chunk_size)`.
   - `Visitor::rollback_extents(gen, extents, object_size, shards)` → add `uint64_t ec_chunk_size` parameter.

2. **`src/osd/osd_types.cc`** — `ObjectModDesc::visit()`:
   - `APPEND` case: when `struct_v >= 2` decode `ec_chunk_size` after `size`; pass it to `visitor->append(size, ec_chunk_size)`.
   - `ROLLBACK_EXTENTS` case: when `struct_v >= 4` decode `ec_chunk_size` after `shards`; pass it to `visitor->rollback_extents(gen, extents, object_size, shards, ec_chunk_size)`.

3. **`src/osd/ECTransaction.cc`**:
   - Line 809 (`mod_desc.append`): pass the pre-write chunk size.  The pre-write chunk size is `sinfo.get_base().effective_chunk_size(obc->obs.oi)` — must be captured *before* the `oi.ec_chunk_size = plan.chunk_size` update at line 1174 mutates the cached OI.  In practice this is fine because `Generate::append_and_rollback_info()` (which records `mod_desc.append`) runs before `Generate::written_shards()` (which writes back `oi.ec_chunk_size`).  Concretely: `entry->mod_desc.append(ECUtil::align_next(plan.orig_size), obc ? obc->obs.oi.ec_chunk_size : 0)`.
   - Lines 1138–1142 (`mod_desc.rollback_extents`): same value; `entry->mod_desc.rollback_extents(entry->version.version, rollback_extents, ECUtil::align_next(plan.orig_size), rollback_shards, obc ? obc->obs.oi.ec_chunk_size : 0)`.

4. **`src/osd/PGBackend.cc`** — `RollbackVisitor`:
   - `append(uint64_t old_size, uint64_t ec_chunk_size)`: replace `chunk_size = 0` argument with `ec_chunk_size` in the `object_size_to_shard_size` call.
   - `rollback_extents(gen, extents, object_size, shards, uint64_t ec_chunk_size)`: same replacement.
   - Remove the two `TODO(dynamic-object-size)` comments.

5. **All other `Visitor` subclasses** that override `append()` or
   `rollback_extents()` — `TrimmerPostRemove`, `Trimmer`, `DumpVisitor`
   in `osd_types.cc`, and any in tests — must add the new parameters
   (they can ignore `ec_chunk_size`).

**Backward compatibility:** Old PG log records with version 1 `APPEND` or
version 3 `ROLLBACK_EXTENTS` will decode `ec_chunk_size = 0`, falling back
to the pool default — correct for non-dynamic pools and for objects written
before the feature was enabled.

### Option B — Extract `ec_chunk_size` from the co-recorded `SETATTRS` entry *(no format change, fragile)*

When a write is planned, the old OI (with the pre-write `ec_chunk_size`) is
captured in `xattr_rollback` and encoded as a `SETATTRS` record in the
same `mod_desc` buffer, immediately before the `APPEND` /
`ROLLBACK_EXTENTS` record.  `RollbackVisitor` can stash the chunk size seen
in the old `OI_ATTR` during `setattrs()` and consume it in `append()` /
`rollback_extents()`.

**Changes required:**

1. Add `uint64_t old_ec_chunk_size = 0` member to `RollbackVisitor`.
2. In `setattrs()`: if `attrs` contains `OI_ATTR`, decode the `object_info_t`
   and set `old_ec_chunk_size = oi.ec_chunk_size`.
3. In `append()` and `rollback_extents()`: pass `old_ec_chunk_size` to
   `object_size_to_shard_size()`.

**Caveat:** Relies on the implicit ordering invariant that `SETATTRS` always
precedes `APPEND`/`ROLLBACK_EXTENTS` in the `mod_desc` buffer.  If no
`SETATTRS` is present (e.g. write that doesn't update attrs), `old_ec_chunk_size`
stays zero and the bug silently persists.  Add a `ceph_assert` that
`SETATTRS` was seen before `APPEND` for dynamic pools, or document the
assumption explicitly.

### Option C — Read the old OI from the stashed object *(not viable)*

For the `rmobject` rollback path the stashed object exists on-disk with the
correct OI.  For `APPEND` rollback no stash exists.  Adding a synchronous
OSD-store read inside the visitor is architecturally wrong.  **Do not implement.**

---

## 3. Deep scrub stride

**File:** `src/osd/ECBackend.cc:1612` — marked `TODO(dynamic-object-size)`

**What is missing:** `ECBackend::be_deep_scrub()` aligns its read stride to
`sinfo.get_default_chunk_size()` regardless of the object being scrubbed.
For objects with a larger per-object chunk size this may produce a
non-chunk-aligned stride.

**What to do:** Source the per-object chunk size from the object's OI or
attrs at the point the scrub iterates, and use
`sinfo.for_object_chunk_size(ec_chunk_size)` to compute the aligned stride.

---

## 4. Sub-chunk encode path

**File:** `src/osd/ECBackend.cc:537` — marked `TODO(dynamic-object-size)`

**What is missing:** The sub-chunk encode path uses
`sinfo.get_default_chunk_size()` for sub-chunk offset arithmetic. This is
wrong for objects with a non-default chunk size.

**What to do:** Identify the object's `oi.ec_chunk_size` at the call site
and pass it through so that `sinfo.for_object_chunk_size(ec_chunk_size)` is
used.

---

## 5. Crimson OSD

**File:** `src/crimson/osd/ec_backend.cc` — marked `TODO(dynamic-object-size)`

**What is missing:** Crimson always passes `chunk_size == 0` (pool default)
to `objects_read_and_reconstruct`. It does not source the per-object chunk
size from the OI and does not support the dynamic-object-size feature.

**What to do:** Apply the same OI-lookup-and-thread pattern to the Crimson
EC read path that was applied to the classic OSD. This is substantial work
requiring familiarity with the Crimson EC backend architecture and is
explicitly out of scope for the first pass.

---

## 6. Option B: speculative split for unknown-hint reads (future enhancement)

**File:** `src/osdc/SplitOp.cc` — `SplitOp::create()` and
`src/osdc/Objecter.cc:2397` — `op_post_split_op_complete()`

**What is missing:** When `ec_object_size_hint == 0` the current
implementation (Option D) sends the op to the primary, losing the split-read
benefit for callers that do not know the object size.

The preferred future upgrade (**Option B**) would instead speculatively send
the split using pool-default chunk geometry and rely on the OSD returning
`-EAGAIN` with the correct chunk size when the geometry is wrong. The
Objecter retries with the now-known chunk size; subsequent reads use it
directly from the cache.

**Prerequisites:** Items 1f and 1g must be complete first.

**What to do:**

1. In `SplitOp::create()`, when `ec_object_size_hint == 0` on a dynamic
   pool, proceed with `ec_chunk_size = 0` (pool default) rather than
   returning `false`. The split will be speculative.

2. In `op_post_split_op_complete()`, when the retry is triggered by
   `-EAGAIN` *and* the reply carried a non-zero `ec_chunk_size` (item 1f),
   store it in `op->ec_object_size_hint` before resubmitting, so the retry
   goes through `SplitOp::create()` with the correct geometry rather than
   falling through to the primary.

**Design discussion:** Section 7 of the design doc,
"When the hint is unknown: design alternatives", Option B vs Option D.

---

## 7. Revisit `chunk_size_for_hint()` — power-of-two constraint and alternative strategies

**File:** `src/osd/ECUtil.h` — `stripe_info_base_t::chunk_size_for_hint()`

**Why this needs revisiting:**

The current implementation always rounds up to the next power of two.  The
comment in the function says _"the geometry maths assumes this"_, but an audit
of `stripe_info_t` shows that is not true: every geometry helper
([`ro_offset_to_shard_offset`](src/osd/ECUtil.h),
[`object_size_to_shard_size`](src/osd/ECUtil.h),
[`shard_offset_to_ro_offset`](src/osd/ECUtil.h), etc.) uses only integer
`/` and `%` against `chunk_size` and `stripe_width`; no bitwise shift or
mask is ever applied to `chunk_size` itself.  The only hard alignment
requirement on shard sizes is the 4 KiB `EC_ALIGN_SIZE` applied by
`align_next()`, which is independent of the chunk size being a power of two.

The power-of-two rule therefore appears to be a convenience heuristic that
was promoted to a hard constraint without justification.  This matters because
rounding up to the next power of two causes significant space amplification:
an object whose per-shard size is just over a power-of-two boundary pays up
to 2× the storage of one that falls just under it.

**Alternative strategies to evaluate:**

### A. Power of two (current)
Round `ceil(hint / k)` up to the next power of two.

- **Pro:** Simple; natural doubling steps mean chunk size grows slowly relative
  to object size; easy mental model.
- **Con:** Up to 2× space amplification at power-of-two boundaries.  An object
  whose hint is `default_chunk * k + 1` bytes pays for a chunk that is twice
  as large as actually needed.  For write workloads that cluster near these
  boundaries the average wasted space is ~25% (E[amplification] across the
  uniform interval [2^n, 2^(n+1)] = 1.5 / 2 ≈ 75% utilisation).
- **Alignment:** Passes the `EC_ALIGN_SIZE` alignment requirement trivially
  (powers of two ≥ 4096 are always 4096-aligned).

### B. `EC_ALIGN_SIZE`-aligned round-up (minimum constraint only)
Round `ceil(hint / k)` up to the next multiple of `EC_ALIGN_SIZE` (4 KiB).

- **Pro:** Minimal padding; chunk size tracks actual object size very closely;
  no pathological 2× jumps.
- **Con:** Produces a very large number of distinct chunk sizes, which
  complicates monitoring, capacity modelling, and any future feature that
  must enumerate possible geometries.  Stale-hint scenarios (item 2 above)
  are more likely to produce a chunk that is too small, forcing more stripes.
- **Alignment:** Satisfies `align_next()` by construction.

### C. Fixed set of allowed sizes (pool-configurable ladder)
Define a monotone sequence of allowed chunk sizes, e.g.
`{default, 2×default, 4×default, …, max}`, or an operator-specified list.
`chunk_size_for_hint()` picks the smallest size in the ladder that fits
`ceil(hint / k)`.

- **Pro:** Bounded number of distinct geometries; operator can tune the
  ladder to the workload; predictable capacity planning.
- **Con:** Adds a pool configuration knob; ladder must be validated
  (monotone, each entry `EC_ALIGN_SIZE`-aligned, each entry ≤ `max`).
  Default ladder must be chosen carefully.
- **Alignment:** Ladder validation ensures it.

### D. `EC_ALIGN_SIZE`-aligned round-up, capped at a small set of "nice" sizes
A middle ground: round up to the next multiple of `EC_ALIGN_SIZE`, then snap
to the next value in a small implicit ladder (e.g. multiples of 64 KiB, or
powers-of-two-of-64 KiB).  Keeps the number of distinct sizes small without
the full 2× penalty.

- **Pro:** Bounded geometry diversity; much less amplification than pure
  power of two; no new pool knobs.
- **Con:** The snap boundaries are arbitrary and may still cause noticeable
  amplification near snap points.

**Recommended next step:**

Before choosing a strategy, verify empirically whether the power-of-two
constraint is actually required anywhere in the encode/decode path (ISA-L,
Jerasure plugin interfaces) or in the sub-chunk path
(`src/osd/ECBackend.cc:537`).  If the plugins impose no such requirement,
document that explicitly in the comment and consider migrating to **Strategy C**
(configurable ladder, defaulting to powers of two) so the constraint can be
loosened without a flag day.

**Additional concerns raised during review (see also item #2 in the shortcomings
analysis):**

- The chunk size chosen on the _first_ write is permanent (`oi.ec_chunk_size`
  is set once and never updated).  A bad hint from a small first write
  forces all future writes — even those that grow the object dramatically —
  into a small chunk size for the object's entire lifetime.  This is
  independent of which rounding strategy is chosen, but any new strategy
  must consider whether re-chunking on later writes is desirable.
- `max_chunk_size < default_chunk_size` is not validated; the two clamps
  silently return a value that violates the max.
- The `max_chunk_size` default parameter in the function signature silently
  bypasses the pool-configured value for any call site that omits the
  argument.
