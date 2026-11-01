# Batch Commit Mode

PUT batch coalescing: multiple S3 PutObject operations committed in a single FDB transaction to reduce per-object commit overhead.

## Design

### Queue

`BatchCommitQueue` holds a `std::deque<BatchCommitEntry>`. Callers enqueue entries and receive a `std::future<KvrgwErrorCode>` that resolves when the batch commits (or fails).

### Thread Pool

N committer threads (default 8, configurable via `KVRGW_BATCH_THREADS`) run the `BatchCommitQueue::run()` loop. Each thread independently drains entries from the shared deque and commits a batch.

### Wait Logic

Each committer thread blocks until one of:
- `pending_.size() >= batch_size` -- full batch ready
- The oldest entry in the deque has waited `>= batch_timeout_us` -- partial flush

Sleep uses the remaining time until the oldest entry expires, not a fixed interval. This prevents unnecessary latency on the oldest entry while still allowing the batch to fill.

### Conflict Set

Before committing, the thread scans the front of the deque and builds a per-batch conflict set (`unordered_set<string>` keyed on `bucket_name/object_name`). If the same object key appears twice, the duplicate is pushed to the back of the deque. This avoids FDB write-write conflicts within a single transaction.

Conflict pushbacks are tracked in `BatchStats::conflict_pushbacks`.

### Commit

`commit_batch()` runs a three-phase protocol for all entries in the batch:

**Phase 1 — Group P:O (Class B / storage tier only):**
One group coordination entry covers all storage-tier entries in the batch. Single blocking `store_.set()`.
```
Key:   P:<shard_count 2B><shard_id 2B><bucket_id 8B><G 1B><group_ref_tag 12B>
Value: [count 2B BE][ref_tag 12B + size 8B BE]×N[created_at 4B BE]
       (per-entry = 20 bytes; stores estimated_size from PUT request)
```
Category byte `'G'` distinguishes group P:O from regular P:O (which uses `'O'`). Functions: `make_group_po_value(const GroupPoEntry*, size_t, uint32_t)`, `parse_group_po_value(string_view) → optional<GroupPoValue>`.

`kMaxBatchSize` (16) is defined in `constants.hpp`.

**Phase 2 — Disk writes (Class B only):**
Sequential `data_store.write()` for each storage-tier entry. Safe because Phase 1 committed — sweeper can find and clean orphan blobs.

**Phase 3 — Metadata commit (all entries):**
Single FDB transaction with pipelined reads:
1. Stack-allocated arrays: `PutContext ctxs[kMaxBatchSize]`, `VerifiedBucket verified[kMaxBatchSize]`
2. Bucket read deduplication: linear scan of issued buckets (no `unordered_set`)
3. For each entry: `put_prepare()` issues async reads (S:O + B: for unique buckets)
4. If Class B: `async_get(group_P:O)`
5. For each entry: `put_finalize()` resolves futures, applies mutations (linear scan of `VerifiedBucket` array, no `unordered_map`)
6. If Class B: `wait(group_P:O)` → verify exists → `del(group_P:O)`
7. Commit; on retriable error (200–206): retry up to 10 times with backoff
8. On permanent failure: set error code on all futures
9. On success: set `KVRGW_ERR_OK` on all futures

### Notification

`enqueue()` only notifies the condition variable when `pending_.size() >= batch_size`. This prevents waking threads for every single entry; partial batches drain via the timeout path.

## Configuration

### Environment Variables

| Env var | Default | Description |
|---------|---------|-------------|
| `KVRGW_BATCH_SIZE` | 1 | Objects per FDB transaction. 1 = batch disabled |
| `KVRGW_BATCH_TIMEOUT_US` | 1000 | Max wait (microseconds) before flushing a partial batch |
| `KVRGW_BATCH_THREADS` | 8 | Number of committer threads in the pool |

### Admin Socket

```
echo "set-tier-config batch_size=32 batch_timeout_us=500" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-0.sock
```

Changes take effect after the double-buffer applies (`maybe_apply_pending()`). The batch queue is stopped and restarted with the new thread count if `batch_size` transitions to/from 1. Note: in `--perf` mode, the batch queue only starts if `KVRGW_BATCH_SIZE > 1` is set at launch or `set-batch` is used at runtime.

### Perf Driver

```
perf> set-batch size=32 timeout=500 threads=8
```

Immediately applies the new config and restarts the batch queue.

## Integration with PUT Flow

`put_object_route()` is the shared entry point for all PUT operations (both gRPC `PutObject` handler and perf driver `put_worker`). When `batch_size > 1` and the request has no tags, it routes to the batch queue:

```
put_object_route(req, data, data_len):
  tc = active tier config

  if batch_size > 1 && !tags:
    select tier (INLINE / KV_STORE / STORAGE) — set chunk.type only
    enqueue BatchCommitEntry → batch_queue (no P:O or disk write by caller)
    wait on future → return status
    (batch worker handles Phase 1 group P:O + Phase 2 disk writes + Phase 3 metadata commit)

  else (batch_size == 1 or has tags):
    if zero-byte: INLINE → put_object_single_txn
    else: select_storage_tier → single-txn or three-phase
```

All three tiers (INLINE, KV_STORE, STORAGE) go through batch when enabled. The caller (producer thread) only classifies the tier and enqueues — **all I/O (Phase 1 group P:O, Phase 2 disk writes, Phase 3 metadata commit) is performed by the batch worker thread**. This means producer threads do zero FDB or disk work in batch mode; they block on `future.get()` until the batch worker commits.

### Bucket Rotation (burst)

The `burst=N` parameter (default 1) controls how producer threads distribute objects across buckets: `bucket_names[(seq / burst_size) % num_buckets]`. With `burst=1`, every object goes to the next bucket round-robin. With `burst=5`, 5 consecutive objects go to the same bucket before rotating. This reduces per-batch B: bucket verification reads when using multiple buckets, since consecutive entries in the batch queue are more likely to share a bucket.

Requests with tags bypass batching because `PreparedTags` is defined in the .cpp and cannot be stored in `BatchCommitEntry` (header visibility limitation). This is an implementation constraint, not a design requirement — `put_object_in_txn` handles tags correctly within a transaction.

### Zero-allocation hot path

`commit_batch` uses stack-allocated arrays for all per-entry state:
- `PutContext ctxs[kMaxBatchSize]` — FDB futures + KeyBuf (1100B stack buffer per entry, no heap strings)
- `VerifiedBucket verified[kMaxBatchSize]` — bucket verification cache (linear scan, no `unordered_map`)
- `std::string bucket_ids[kMaxBatchSize]` — resolved bucket IDs
- Bucket dedup via stack array + linear scan (no `unordered_set`)

## Performance Characteristics

- **Throughput**: Batching amortizes FDB commit latency across N objects. For small inline objects (128B), expect ~50% throughput improvement. For larger objects (4KB D-tier, 8KB storage-tier), gains are smaller or negative due to increased per-transaction work offsetting commit savings.
- **Latency**: Individual PUT latency increases by up to `batch_timeout_us` (waiting for batch to fill). At default 1000us, p99 PUT latency adds ~1ms in the worst case.
- **Conflict rate**: The per-batch conflict set prevents intra-batch conflicts. Inter-batch conflicts (same key written by two different batches) are handled by FDB's normal retry loop.
- **Thread scaling**: More threads allow more concurrent batches. Diminishing returns past the number of FDB storage processes.
- **Stats**: Use `batch-stats` in the perf driver to observe batch_commits, avg/min/max batch_size, avg_queue_size, avg/min/max wait_us, conflict_pushbacks, and txn failure counters. `avg_queue_size` shows the average deque depth when a worker extracts entries — indicates backpressure.

## Delete-Multi: Same Principle

`DeleteMulti` uses the same commit amortization approach as PUT batching — 10 keys per FDB transaction with pipelined reads. Measured result: **2.6x IOPS improvement** over single-key delete (54K vs 20.7K IOPS for 1M 8KB objects at c=128). The commit cost is amortized across 10 deletions while pipelined reads (B: + 10×S:O) complete in a single FDB round-trip.
