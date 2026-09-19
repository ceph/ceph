# Batch PUT Design

## Motivation

Single PUT commits one FDB transaction per object. At 128 threads × ~16ms per PUT = ~8K IOPS for 8KB objects. FDB storage servers are far from saturated (CPU avg 0.36). The bottleneck is per-object commit overhead.

`put-multi` (direct 10/txn, no queue) proved 137K IOPS is achievable — the FDB cluster can handle 10x more work if we batch commits. The current batch queue adds coordination overhead (mutex, promise/future, wait time) that eats the commit amortization gains.

Goal: design a batch PUT that eliminates redundant work and maximizes parallelism within a single FDB transaction.

---

## Two classes of PUT

Separation is about the **coordination entry only** — everything else is shared.

### Class A — KV-only (inline + child-D)

- Data lives entirely in FDB (O: value or D: entry)
- Single FDB transaction, no coordination entry needed
- No crash-recovery concern — if txn fails, nothing is written

### Class B — Cross-domain (storage tier)

- Spans FDB (metadata) + filesystem (blob)
- Coordination entry (P:O) required: must be durable in FDB BEFORE blob write starts
- If process crashes after blob write but before metadata commit → sweeper finds P:O, cleans orphan blob
- Batch mode: all Class B entries in a batch share one group coordination entry

---

## Shared optimizations (both classes)

### Pipelined reads for all entries

All `async_get(S:O)` for N entries + all `async_get(B:)` for unique buckets are issued before any are resolved. FDB batches them into one network round-trip internally. The first `wait()` blocks for the round-trip; all subsequent waits return instantly (already resolved).

### Bucket read deduplication

Not limited to one bucket per batch. A batch of 10 ops might span 5 unique buckets:

- Scan batch entries, build unique bucket set (fixed stack array, linear scan)
- Issue `async_get(B:)` for each unique bucket only
- Resolve each unique bucket once, store result in the array
- Each entry looks up its bucket via linear scan

A 10-op batch with 1 bucket: 1 bucket read. With 5 unique buckets: 5 bucket reads. Both in one FDB round-trip alongside all S:O reads.

---

## Phase 1 — Coordination entry (Class B only)

Simple blocking writes. P:O must be committed before any disk write starts (correctness: if process crashes after disk write, sweeper needs P:O to find and clean the orphan blob).

### Single mode

```
put_phase1_single(bucket_id, object_name, ref_tag):
  po_key = make_po_key(bucket_id, object_name, ref_tag)
  store.set(po_key, po_value)       // blocking — one txn, one commit
```

One P:O key per object. Standard crash-recovery anchor.

### Batch mode — group coordination entry

```
put_phase1_batch(bucket_id, group_ref_tag, ref_tags[N]):
  group_po_key = make_group_po_key(bucket_id, group_ref_tag)
  group_po_value = [count 2B][ref_tag_0 12B]...[ref_tag_N 12B][created_at 4B]
  store.set(group_po_key, group_po_value)   // blocking — one txn, one commit
```

One group P:O key covers all N entries. One commit instead of N. Sweeper processes the group atomically (follow-up task).

---

## Phase 2 — Storage-tier data write (Class B only)

Standalone function called by both single-mode and batch-mode wrappers. Simple blocking disk write:

```
storage_write(ref_tag, data):
  data_store.write(ref_tag, data)   // blocking
```

Safe to proceed because Phase 1 committed — sweeper can find and clean any orphaned blobs if process crashes here.

In batch mode, called N times sequentially (or parallelized across threads in future).

---

## Phase 3 — Metadata commit (shared, all tiers)

Same code path for single and batch, Class A and Class B. The only phase using async FDB futures.

### Step 1 — Issue all reads (one FDB round-trip)

```
for each unique bucket (not yet verified):
  f_bkt[b] = async_get(B:b)
for each entry:
  f_obj[i] = async_get(S:O_i)
if Class B (single):
  f_po = async_get(P:O)
if Class B (batch):
  f_group_po = async_get(group_P:O)
```

All issued before any wait. One network round-trip regardless of N.

### Step 2 — Resolve and apply

```
for each unique bucket:
  wait(f_bkt[b]) → verify access → store in verified array
for each entry:
  wait(f_obj[i]) → check conditions, displace old, put(S:O_i)
  if child-D: put(D:_i, data)
if Class B (single): wait(f_po) → verify exists → del(P:O)
if Class B (batch): wait(f_group_po) → verify exists → del(group_P:O)
commit()
```

---

## Wrappers

### Single-mode (batch_size=1)

```
Class A (inline/child-D):
  put_object_single_txn:
    begin_transaction
    put_prepare (async_get B: + S:O)
    put_finalize (wait, apply, put S:O / D:)
    commit

Class B (storage tier):
  select_storage_tier:
    put_phase1_single (store.set P:O — blocking)
    storage_write (data_store.write — blocking)
    put_object_single_txn:
      begin_transaction
      put_prepare (async_get B: + S:O + P:O)
      put_finalize (wait, apply, put S:O, del P:O)
      commit
```

### Batch-mode (batch_size=N)

```
Batch thread drains N entries from queue:

Class B subset (if any):
  put_phase1_batch (one group P:O — blocking store.set)
  for each Class B entry: storage_write (blocking)

All N entries (Class A + Class B together):
  begin_transaction
  for each entry: put_prepare (async_get S:O + B: for unique buckets)
  if Class B: async_get(group_P:O)
  for each entry: put_finalize (wait, apply, put S:O / D:)
  if Class B: wait(group_P:O) → del(group_P:O)
  commit
```

---

## Data structures (zero heap allocation on hot path)

```cpp
struct PutContext {
  FdbFuture f_bkt;                // valid only if has_bucket_future
  FdbFuture f_obj;                // always valid
  FdbFuture f_po;                 // storage tier single: P:O existence check
  bool has_bucket_future{false};
  bool is_storage_tier{false};
  KeyBuf object_key;              // 1100B stack buffer
};

struct VerifiedBucket {
  uint32_t tenant_id;
  const std::string* bucket_name;  // points into existing string (no alloc)
  BucketState state;
};

static constexpr int kMaxBatchSize = 16;
// Stack-allocated in commit_batch:
VerifiedBucket verified[kMaxBatchSize];
int verified_count = 0;
PutContext ctxs[kMaxBatchSize];
```

---

## Sweeper impact (follow-up task)

Group P:O entries need sweeper support:
1. Parse ref_tag vector from value
2. For each ref_tag: check if committed S:O exists with matching ref_tag
3. Clean orphan blobs, delete group P:O

Not implemented in this change — acceptable for testing (always `--clean` between runs).

---

## Expected performance

| Mode | Current | Projected |
|------|---------|-----------|
| Single 8KB PUT | 7.6K IOPS | ~7.6K (no change — zero overhead from refactor) |
| Batch=5 8KB PUT | 7.3K IOPS | ~15-20K (one P:O commit, pipelined reads, one metadata commit) |
| Batch=10 8KB PUT | 7.2K IOPS | ~25-30K (same benefits, larger batch) |
| put-multi 128B (ceiling) | 137K IOPS | — (theoretical max, no queue) |

---

## Implementation instructions

### What to remove

From `backend/src/kv_store.hpp` and `backend/src/kv_store.cpp`:
- `KvCommitFuture` struct (entire definition)
- `KvTransaction::commit_async()` method
- `KvStore::set_async()` method

From `backend/src/service_impl.hpp`:
- `KvCommitFuture* po_commit` from `PutContext`
- `std::optional<KvCommitFuture> po_commit` from `BatchCommitEntry`
- `KvCommitFuture* po_commit_future` from `PutInTxnParams`
- `std::string object_key_buf` and `std::string bucket_key` from `PutContext`
- `std::unordered_map` parameter from `put_finalize` signature

### What to add/change

In `backend/src/service_impl.hpp`:
- `PutContext` rewritten with `KeyBuf object_key` (stack buffer) — see data structures above
- `VerifiedBucket` struct — see data structures above
- `put_finalize` signature: `(KvTransaction& tr, PutContext& ctx, PutInTxnParams& params, VerifiedBucket* verified, int& verified_count, uint8_t* out_vs)`
- `put_phase1_batch` declaration

In `backend/src/service_impl.cpp`:
- `put_phase1_batch` implementation — builds group P:O key+value, calls `store_.set()` (blocking)
- `put_prepare` — uses `KeyBuf` (no heap), issues async_get(B:) only if `need_bucket`
- `put_finalize` — linear scan of `VerifiedBucket` array instead of map lookup; if `has_bucket_future` verify and append to array
- `put_object_in_txn` — `VerifiedBucket verified[1]; int vc = 0;` on stack
- `commit_batch` — full rewrite per batch-mode flow above: separate Class B phase1+phase2, then shared phase3 with stack arrays
- `select_storage_tier` — revert to blocking `store_.set(P:O)` (remove `set_async` call)
- `put_object_route` batch path — remove `po_commit_for_entry` and `set_async` usage, storage entries handled by `commit_batch` internally

### Group P:O key format

```
Key:   P:<shard_count 2B><shard_id 2B><bucket_id 8B><G 1B><group_ref_tag 12B>
       (category byte 'G' distinguishes from regular P:O which uses 'O')
Value: [count 2B BE][ref_tag_0 12B][ref_tag_1 12B]...[ref_tag_N 12B][created_at 4B BE]
```

Use existing `KeyHeaderS` with category `'G'` and variable tail = `group_ref_tag`. The `group_ref_tag` can be the first entry's ref_tag (arbitrary unique identifier for the group).

### Key helper

```cpp
KeyBuf make_group_po_key(const std::string& bucket_id, const std::string& group_ref_tag);
std::string make_group_po_value(const std::vector<std::string_view>& ref_tags, uint32_t created_at);
```

Add to `backend/src/keys.cpp` / `backend/src/gc_value.cpp` as appropriate.

### Build and test commands

```bash
cd /home/gbenhano/kv_poc/backend/build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)
cd /home/gbenhano/kv_poc && ./scripts/reload.sh --clean
cd /home/gbenhano/kv_poc && ./scripts/run_test_plan.sh --quick
cd /home/gbenhano/kv_poc && ./scripts/run_ceph_rgw_tests.sh
```

All tests must pass 100%. If any fail, stop and investigate.

### Existing source file locations

- `backend/src/service_impl.cpp` — all PUT logic (put_prepare ~line 1340, put_finalize ~line 1360, put_object_in_txn ~line 1501, commit_batch ~line 267, select_storage_tier ~line 1567, put_object_route ~line 1614)
- `backend/src/service_impl.hpp` — PutContext, PutInTxnParams, BatchCommitEntry, method declarations
- `backend/src/kv_store.hpp` — KvStore, KvTransaction, FdbFuture, KvCommitFuture (to remove)
- `backend/src/kv_store.cpp` — set_async, commit_async implementations (to remove)
- `backend/src/keys.cpp` + `backend/src/keys.hpp` — key construction helpers (make_po_key, KeyBuf, KeyHeaderS)
- `backend/src/gc_value.cpp` — make_po_value, parse_po_value
- `backend/src/key_buf.hpp` — KeyBuf struct definition (1100B stack buffer)
