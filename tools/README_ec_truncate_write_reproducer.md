# EC Truncate+Write Bug Reproducer via RBD Copyup

Reproducer scripts for bugs fixed in `wip-truncate-write-io-sequence` (commit
`51d8c5c489b`).  These bugs were previously believed to be triggerable only by
CLS-sparsify operations.  The scripts below demonstrate that normal RBD clone
copyup operations can produce the same truncate+write RADOS ops that expose
the bugs.

Tracker: https://tracker.ceph.com/issues/77276

## Two reproducer scripts

| Script | Bug triggered | Symptom |
|--------|--------------|---------|
| `rbd_discard_workload_parity_corruption.py` | Parity overwrite (line 296: `=` vs `.insert()`) | `data_digest_mismatch` on deep scrub |
| `rbd_discard_workload_shard_sizes.py` | Parity overwrite + shard-resize skip (line 761) | `obj_size_info_mismatch` / `failed to pick suitable object info` on normal scrub; OSD crash on read |

Both scripts require a source-code hack to `CopyupRequest::send()` (described
below) and must be run with `--initial-length 4194304` to populate the parent
image objects.

---

## 1. Source-code hack: CopyupRequest coalescing window

The bug requires two librbd operations (a discard and a write) to coalesce
into a single `CopyupRequest` so that both contribute ops to the same RADOS
write.  Normally, the Python RBD API is synchronous: `image.discard()` blocks
until the copyup completes, so the subsequent `image.write()` can never
overlap with it.

The scripts use `aio_discard()` + `aio_write()` to dispatch both operations
concurrently.  However, the copyup's parent-read begins almost immediately, so
there is a tight race.  To guarantee coalescing, add a 5-second delay at the
start of `CopyupRequest::send()`:

```cpp
// src/librbd/io/CopyupRequest.cc  —  CopyupRequest<I>::send()
template <typename I>
void CopyupRequest<I>::send() {
  // Delay the parent read by 5 seconds to widen the coalescing window:
  // other write ops that hit the same object_no during this interval will
  // call append_request() and piggy-back onto this copyup instead of
  // triggering an independent one.  The window closes when
  // handle_read_from_parent() calls disable_append_requests().
  auto timer = std::make_shared<boost::asio::steady_timer>(
    m_image_ctx->asio_engine->get_io_context(),
    std::chrono::seconds(5));
  timer->async_wait([this, timer](const boost::system::error_code&) {
    read_from_parent();
  });
}
```

This is a test-only hack.  It delays the parent read by 5 seconds, keeping the
`CopyupRequest` in the "accepting appends" state so both the discard and write
requests are appended to `m_pending_requests` before the copyup proceeds.

Rebuild the OSDs after applying this change.

## 2. What the scripts do

### Common setup (both scripts)

1. **Create a base RBD image** in the replicated metadata pool, backed by the
   EC data pool.

2. **Write 4 MiB to each of 4 regions** of the parent image (fills 4 RADOS
   objects with data).  This ensures the parent objects are non-zero so that
   `m_copyup_is_zero = false` during copyup.

3. **Snapshot and protect** the parent image.

4. **Create a clone** from the snapshot.

5. **Create a snapshot on the clone**.  This makes
   `snapc.snaps` non-empty, which sets `deep_copyup = true` inside
   `CopyupRequest::copyup()`.  When deep_copyup is true, the copyup and the
   pending write operations are sent as **two separate RADOS ops**:
   - `copyup_op` (empty snap context) — creates the clone object with parent data
   - `write_op` (clone's snap context) — applies the truncate + write to the
     now-existing object

   This is the critical step.  The `write_op` arrives at an object with
   `orig_size = 4194304` (4 MiB, from the copyup), which exercises the buggy
   code paths in `ECTransaction::WritePlanObj`.

6. **Issue async discard + write** using `aio_discard()` and `aio_write()`.
   Both operations hit the same RADOS object.  Both get `-ENOENT` on their
   initial `write_object()` attempt (the clone object doesn't exist yet), and
   both call `copyup()`.  The first creates a `CopyupRequest`; the second
   appends to it via `append_request()`.  After the 5-second coalescing
   window, both are flushed:
   - The discard contributes `ObjectDiscardRequest::add_write_ops()` →
     `truncate(offset)`
   - The write contributes `ObjectWriteRequest::add_write_ops()` →
     `write(offset, data)`

### Script 1: `rbd_discard_workload_parity_corruption.py`

**Workload shape per region (4 MiB object):**
- Discard: last 64 KiB (offset 4128768, length 65536) → `truncate(4128768)`
- Write: first 4 KiB (offset 0, length 4096) → `write(0, 4096)`

**RADOS ops generated (visible in OSD logs):**

First, both the discard and write are sent directly, both fail with ENOENT:
```
osd_op [stat, truncate 4128768]                          → -ENOENT
osd_op [stat, set-alloc-hint, write 0~4096]              → -ENOENT
```

Then, after the 5-second coalescing window, two ops from the deep_copyup path:
```
osd_op [call rbd.sparse_copyup, set-alloc-hint]  snapc 0=[]      (copyup_op)
osd_op [truncate 4128768, write 0~4096]          snapc 6=[6]     (write_op)
```

**EC write plan produced for the write_op:**
```
will_write: {0:[0~4096], 5:[823296~4096]}
orig_size: 4194304   projected_size: 4128768
```

Shard 5 (parity) should have `[0~4096, 823296~4096]` but has only
`[823296~4096]` because line 296 (`will_write[shard] = truncate_write`)
overwrote the data write's parity component.

**Scrub result:** deep scrub reports `data_digest_mismatch` — the parity at
shard offset 0 is stale.

### Script 2: `rbd_discard_workload_shard_sizes.py`

**Workload shape per region (4 MiB object):**
- Discard: from 64 KiB to end (offset 65536, length 4128768) → `truncate(65536)`
- Write: last 4 KiB (offset 4190208, length 4096) → `write(4190208, 4096)`

The truncate slashes the object to 64 KiB, then the write extends it back to
4 MiB.  The resulting `projected_size == orig_size == 4194304`.

**RADOS ops generated:**
```
osd_op [stat, truncate 65536]                            → -ENOENT
osd_op [stat, set-alloc-hint, write 4190208~4096]        → -ENOENT
osd_op [call rbd.sparse_copyup, set-alloc-hint]  snapc 0=[]      (copyup_op)
osd_op [truncate 65536, write 4190208~4096]      snapc 6=[6]     (write_op)
```

**EC write plan produced for the write_op:**
```
will_write: {3:[835584~4096], 5:[12288~4096]}
orig_size: 4194304   projected_size: 4194304
invalidates_cache: 0
```

Three bugs combine here:

1. **Bug 4 (parity overwrite):** Shard 5's `will_write` should include
   `[835584~4096]` (the data write's parity), but `will_write[shard] =
   truncate_write` at line 296 replaced it with `[12288~4096]`.  The parity
   shard is only written up to offset 16384.

2. **Bug 5 (shard-resize skip):** `orig_size == projected_size`, so the
   shard-resize code at line 761 (`if (plan.orig_size < plan.projected_size)`)
   does not run.  Shards that were truncated down are never extended back up.

3. **Bug 1 (cache invalidation):** `invalidates_cache` is `0` despite a
   truncate being present.  `projected_size < orig_size` is false because
   `projected_size == orig_size`.

**Resulting shard sizes on disk:**

| Shard | Actual size | Expected size | Status |
|-------|------------|---------------|--------|
| 0     | 16384      | 839680        | SHORT  |
| 1     | 12288      | 839680        | SHORT  |
| 2     | 12288      | 839680        | SHORT  |
| 3     | 839680     | 839680        | OK (has the data write) |
| 4     | 12288      | 835584        | SHORT  |
| 5     | 16384      | 839680        | SHORT (parity) |

**Scrub result:** normal scrub (not deep) reports `obj_size_info_mismatch` on
5 of 6 shards and `failed to pick suitable object info` because every primary
shard's on-disk size disagrees with its OI attribute.

**Read result:** reading from any offset beyond the truncated shard sizes
crashes the OSD with `FAILED ceph_assert(r == 0)` at `ECCommon.cc:687` during
`handle_sub_read_reply`.

---

## 3. Cluster prerequisites

Build Ceph from the branch **without** the fix (i.e. before cherry-picking
`51d8c5c489b`), with the `CopyupRequest::send()` hack applied.  Then set up
a vstart cluster:

```bash
cd /work/ceph/build

# Stop any previous cluster
../src/stop.sh

# Start a 6-OSD vstart cluster
OSD=6 MDS=0 MON=1 MGR=1 ../src/vstart.sh --debug --new -x --localhost \
  -o 'debug_osd = 20'

source vstart_environment.sh

# Set cluster flags
ceph osd set noout
ceph osd set noscrub
ceph osd set nodeep-scrub

# Create EC profile: k=5, m=1, stripe_unit=4K
ceph osd erasure-code-profile set reedsol \
  plugin=isa k=5 m=1 technique=reed_sol_van \
  stripe_unit=4K crush-failure-domain=osd

# Create pools
ceph osd pool create rbd_erasure erasure reedsol
ceph osd pool create rbd_replicated replicated

# Enable RBD on both pools
ceph osd pool application enable rbd_erasure rbd
ceph osd pool application enable rbd_replicated rbd
ceph osd pool set rbd_erasure allow_ec_overwrites true
rbd pool init rbd_erasure
rbd pool init rbd_replicated
```

## 4. Running the scripts

### Parity corruption (deep scrub detection)

```bash
python3 tools/rbd_discard_workload_parity_corruption.py --initial-length 4194304
```

Then trigger a deep scrub on the affected PGs.  Use the `block_name_prefix`
printed by the script to find PGs in the OSD logs, then:

```bash
ceph osd unset noscrub
ceph osd unset nodeep-scrub
ceph pg deep-scrub <PG_ID>
# Wait ~15 seconds
ceph health detail
```

Expected output includes:
```
OSD_SCRUB_ERRORS: N scrub errors
PG_DAMAGED: Possible data damage: M pgs inconsistent
```

`rados list-inconsistent-obj <PG_ID>` will show `data_digest_mismatch`.

### Shard size mismatch (normal scrub detection / OSD crash)

```bash
python3 tools/rbd_discard_workload_shard_sizes.py --initial-length 4194304
```

Then trigger a **normal** scrub:

```bash
ceph osd unset noscrub
ceph pg scrub <PG_ID>
# Wait ~15 seconds
ceph health detail
```

Expected output includes:
```
OSD_SCRUB_ERRORS: N scrub errors
PG_DAMAGED: Possible data damage: M pgs inconsistent
```

`rados list-inconsistent-obj <PG_ID>` will show `obj_size_info_mismatch` on
5 of 6 shards, and the OSD logs will contain:
```
failed to pick suitable object info
```

Alternatively, reading from the corrupted clone at an offset beyond 64 KiB
will crash the primary OSD:
```
FAILED ceph_assert(r == 0) at src/osd/ECCommon.cc:687
```
