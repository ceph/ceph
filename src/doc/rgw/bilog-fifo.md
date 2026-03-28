
Introduce a FIFO-backed bilog backend as an alternative to the
existing InIndex backend. Each bucket index shard gets a dedicated FIFO object
(`<shard_oid>.bilog`) for its bilog. Selection is per-bucket and recorded in
`bucket_info.layout.logs`.

The FIFO backend uses `neorados::cls::fifo::FIFO` (the same implementation
used by the datalog). Bilog push, list, trim, and marker cursor semantics
are modelled after the datalog pattern.

Design:

### 1. Backend selection

Backends are selected at bucket-creation time via the config option
`rgw_default_bucket_bilog_type` (`inindex` / `fifo`. default is `inindex`).
The chosen type is stored as a `BucketLogType` in `bucket_info.layout.logs[0]`.
Once written, the bucket uses that backend for its lifetime until explicitly migrated.

### 2. Per-shard FIFO objects

One FIFO object per bucket index shard, with OID `<shard_oid>.bilog`. This
matches the sharding of the index itself and avoids a single-FIFO bottleneck
per bucket. Shard routing for writes uses `RGWSI_BucketIndex_RADOS::
bucket_shard_index(key, num_shards)`.

### 3. CLS layer bypass

For FIFO buckets the CLS-level bilog write is suppressed by setting
`log_op = false` in the op struct. The FIFO push is performed by the RGW
layer before the CLS op completes.

### 4. Ordering for crash-safety

The FIFO entry is pushed **before** the CLS bucket index op. This prevents the
worst-case scenario: a crash after the CLS op but before the FIFO push would
leave the secondary permanently unable to see the change.

**Object Write Op**
This is weaker than InIndex, where `write_entry` and `log_index_operation` are
both called inside the same CLS method (e.g. `cls_rgw_bucket_complete_op`).
A crash rolls back the entire transaction. With FIFO, a crash between the push and the
CLS op leaves a bilog entry that claims an object was added, but the bucket
index has no corresponding entry.
When the secondary zone processes such an entry it calls `HeadObject`
on the source, which returns 404. Sync process treats `-ENOENT` as a non-fatal
ignorable error (`ignore_sync_error()` in `rgw_data_sync.cc`) - it silently skips
the entry and advances the sync marker. No data is lost on the secondary.

**OLH operations (`LINK_OLH`, `UNLINK_INSTANCE`)**
The FIFO entry carries the `olh_epoch`. On the secondary the
`cls_rgw_bucket_link_olh` handler calls `start_modify(candidate_epoch)`, which
discards the op silently if `candidate_epoch < olh.epoch`.
If RGW crashes after the FIFO push (epoch=N, `LINK_OLH` for `foo#v1`)
but before the `link_olh` op, and the client never retries, the primary's
OLH pointer stays at the previous version while the secondary advances to `foo#v1`.
Epoch comparison cannot detect this: epoch=N is the highest epoch the secondary
has seen for this object, so `start_modify(N)` applies correctly from its perspective.

So, FIFO-first means the secondary can be *ahead* of the primary with a
diverged OLH pointer that epoch comparison has no mechanism to reverse.
We issue `link_olh` / `unlink_instance` before the FIFO push. This is safer. 
They will eventually converge upon a retry or a new FIFO push on secondary.


### 5. Dispatch: `with_bilog<>`

A central template method `RGWRados::with_bilog<OpType>()` selects the
appropriate handler at the call site:

```
is_inindex && log_data   → OpIssuer(log_data=true)  + BILogNopHandler
is_inindex && !log_data  → OpIssuer(log_data=false) + BILogNopHandler
is_fifo   && log_data    → OpIssuer(log_data=false) + RGWBILogUpdateBatch
is_fifo   && !log_data   → OpIssuer(log_data=false) + BILogNopHandler
```

`BILogNopHandler` is a NOP with the same interface as
`RGWBILogUpdateBatch`.

### 6. Write batching (current state)

Each operation flushes one entry to the FIFO immediately. This adds one OSD
round-trip per bucket write. Batching multiple writes is deferred for now.

### 7. Marker and cursor format
FIFO list markers are raw strings (e.g. `"00000001"`). For multi-shard
buckets, the service layer builds markers of the form
`<shard_id>#<raw_marker>` using `build_bucket_index_marker()`, matching the
InIndex format.

Multi-generation markers use the format
`G{gen:020}@{inner_marker}` from `rgw_log_backing.h`.

---

## Implementation Status

- `BucketLogType::FIFO` - new enum value in `rgw_bucket_layout.h`
- `LazyFIFO` - per-shard FIFO wrapper with lazy init in `rgw_log_backing.h`
- `RGWBILogFIFO` - multiple shard FIFO push/list/trim in `rgw_bilog.h/.cc`
- `RGWBILogUpdateBatch` - per-request write helper with shard routing and async flush
- `RGWSI_BILog_RADOS` - service layer changes (InIndex + FIFO) in `svc_bilog_rados.h/.cc`
- `BILogNopHandler` - NOP handler for InIndex and disabled-log paths
- `with_bilog<>` - dispatch template in `RGWRados`
- `cls_rgw_bi_log_related_op` - base struct for CLS ops with typed OpIssuer classes
- `cls_obj_complete_op` - template calls `with_bilog<>` internally
- `bucket_index_link_olh` - rewritten with `with_bilog<CLSRGWLinkOLH<>>`
- `bucket_index_unlink_instance` - rewritten with `with_bilog<CLSRGWUnlinkInstance>`
- `apply_olh_log` - FIFO entry pushed via `with_bilog<void>` at `need_to_link` condition
- `check_disk_state` - FIFO entry pushed via `with_bilog<void>`. `suggest_flag` split
- FIFO batch cache - `shared_ptr<RGWBILogFIFO>` cached in `RGWRados` by `(bucket_id, gen)`
- New bucket FIFO start with `init_default_bucket_layout()` with configurable `rgw_default_bucket_bilog_type`
- `commit_target_layout` preserves FIFO backend type through reshard
- basic `CORO_TEST_F` tests in `test/rgw/test_bilog.cc`

## Remaining Work

### InIndex → FIFO migration
Existing buckets created before `rgw_default_bucket_bilog_type = fifo` remain
on InIndex indefinitely. Migration requires something similar to multi-generation
 `log_list` cursor that walks [InIndex, FIFO] in order using cursorgen used
 by datalog.

### Write batching

Currently one FIFO push per bucket operation, Batching multiple entries into a single `fifo_push` call could be beneficial at high write rates. can it be
made crash-consistent?

###  Functional testing

- multisite sync verification with FIFO-backed buckets
- sync catch-up after zone outage
- reshard of FIFO bucket with sync veriification
- radosgw-admin bilog commands (`list`, `trim`, `status`)
- upgrade path: InIndex bucket continues working after upgrade