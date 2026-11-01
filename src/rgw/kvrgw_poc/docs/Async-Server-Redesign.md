# Async Server Redesign

## Summary

Replace the current blocking model (producer thread blocks on future until batch worker commits) with a fully async model where:
- Producers (gRPC handlers, perf driver) enqueue requests non-blocking
- Workers pull from two queues (batch, single), execute, send response directly to client, free slot
- No `std::future`/`std::promise`, no reply queue, no per-request heap allocation

## Context Table

Pre-allocated array of 4096 fixed-size slots (2KB each). Each slot is a packed binary struct holding the complete request lifecycle state:

- Fixed fields: flags, op_type, state, tier, tenant_id, ref_tag, etag, size, error_code, version_id
- References: data_ptr + data_len (into data pool), client_handle (gRPC responder or perf stats ptr)
- Variable fields: bucket_name (len 1B, max 63B), object_name (len 2B), content_type (len 1B, max 255B), tags/metadata tail
- All inline — no pointers to external strings

Slot allocation: free-list (stack of available indices). No free slot = QUEUE_FULL → reject with 503 (clients auto-backoff) + increment `queue_full_rejects` stat counter.

If request doesn't fit in 2KB → reject with `KVRGW_ERR_VALUE_TOO_LARGE` + increment `slot_overflow_rejects` stat counter. Spillover/linking is TBD.

Note: large tag payloads (external C:T) are stored in the data pool, not inline in the slot. The slot holds only a pointer + encoded size. In practice, typical requests (key < 256B, content-type < 64B, inline tags < 256B) fit well within 2KB.

## Data Pool

Pre-allocated buffer for object data larger than ~128B (which fits inline in the slot).

Interface:
```
void* allocate_data_buffer(size_t size);   // nullptr if full
void  free_data_buffer(void* ptr);
```

Trivial implementation for now (free-list of fixed chunks or simple bump allocator). Better allocator later — API is stable.

Ownership:
- PUT: producer allocates (fills with body) → worker frees after commit
- GET: worker allocates (fills from FDB/disk) → producer frees after sending to client

## Request Queues

Two queues, entries are lightweight (slot index + routing info, no data copy):

- **Batch queue**: entries that can be grouped (N per FDB txn). Worker extracts up to batch_size, commits together.
- **Single queue**: entries that must commit alone (1 per txn). Worker extracts one, commits.

Caller classifies at enqueue time and routes to appropriate queue.

## Worker Pool

- Thread count = number of assigned CPU cores
- Generic workers pull from either queue
- All operations non-blocking — workers never block on FDB or disk I/O

### Worker Event Loop

Each worker has its own `epoll` instance with four event sources (per-worker, no sharing):

- `eventfd_requests` — shared; new work in request queue. Registered with `EPOLLONESHOT` to avoid thundering herd — only one worker wakes per event. After extracting entries, worker re-arms the eventfd.
- `eventfd_iouring` — per-worker; io_uring CQE ready on this worker's ring
- `eventfd_fdb` — per-worker; FDB future resolved for a future owned by this worker
- `timerfd` — per-worker; fires when oldest batch entry exceeds batch_timeout_us

Worker wakes on any event, processes all ready state machines from its own pending list, goes back to sleep.

Note: per-worker eventfds and io_uring instances keep the model simple (no cross-thread data access, no thundering herd). Future revision may move to a shared model with work-stealing to allow load balancing between workers when some are idle and others are overloaded.

### FDB Async Integration

FDB C API `fdb_future_set_callback()` fires on the FDB network thread when a future resolves. Each FDB future is tagged with the owning worker's `eventfd_fdb`. The callback writes 1 to that specific worker's eventfd — no thundering herd. Worker then scans its own pending futures list to find which resolved.

### Disk I/O (io_uring)

Each worker has its own io_uring instance (separate SQ/CQ rings, no sharing). All file operations submitted to the worker's own ring. Completions signal via per-worker `eventfd_iouring`.

### Batch Timeout

Worker sets `timerfd` to fire at the deadline of the oldest entry in the batch queue. When timerfd fires: extract partial batch (even if < batch_size), process it. Reset timerfd after extraction.

### State Machine Per Request

Each context slot has a `state` field. Workers advance state on each wake:

**Batch PUT (N entries — leader slot tracks group state):**

The first slot extracted becomes the "leader." Leader slot stores: group state, FDB txn handle, member slot indices (up to batch_size), Phase 1/3 FDB futures, `pending_futures` counter (atomic uint8_t — decremented by each FDB callback; worker only advances state when counter reaches 0). Member slots hold individual entry data only.

1. `PHASE1_COMMIT_ISSUED` — group P:O async commit submitted (storage-tier only)
2. `PHASE1_COMPLETE` — commit resolved → submit io_uring writes for blobs (one per storage-tier member)
3. `PHASE2_IO_ISSUED` — disk writes submitted to io_uring
4. `PHASE2_COMPLETE` — all blob writes confirmed → open new FDB txn, issue async reads (B: + N×S:O)
5. `PHASE3_READS_ISSUED` — FDB reads in flight
6. `PHASE3_READS_COMPLETE` — reads resolved → apply mutations, issue async commit
7. `PHASE3_COMMIT_ISSUED` — commit in flight
8. `PHASE3_DONE` — commit confirmed → send responses for all members, free all slots + data buffers

On conflict at step 8: discard old txn, create new txn, retry from step 5. Max 10 retries.

Note: FDB transactions have a 5-second time limit. In async mode, time between txn creation (step 4) and commit (step 7) may be longer than in the blocking model if the worker interleaves other work. Monitor `transaction_too_old` (error 1007) rate — if elevated, workers should prioritize advancing in-progress batches over extracting new work.

**Batch DELETE (10 entries):**
1. `READS_ISSUED` — async reads (B: + 10×S:O)
2. `READS_COMPLETE` — resolve, apply move_to_G mutations
3. `COMMIT_ISSUED` — async commit
4. `DONE` — send responses, free slots

**GET:**
1. `READ_ISSUED` — async Get(S:O)
2. `READ_COMPLETE` — resolve, determine tier. Copy data to safe buffer: INLINE data (≤256B) copied into slot variable area; KV_STORE data (≤4KB) copied into data pool. FDB future destroyed after copy.
3. If STORAGE: `DISK_READ_ISSUED` — io_uring read submitted (data read into data pool buffer)
4. `DISK_READ_COMPLETE` → initiate async gRPC response send
5. `RESPONSE_SEND_ISSUED` → waiting for gRPC send completion
6. `DONE` → gRPC send confirmed, free slot + data buffer
7. If INLINE/KV_STORE: data already copied at step 2 → initiate send, then step 5-6

**LIST:**
1. `READS_ISSUED` — async Get(B:) + async range_scan(S:O prefix) (parallel)
2. `READS_COMPLETE` — resolve B: (access check), process scan results, send response, free slot

No reply queue. Worker completes the request end-to-end at the final state.

## gRPC Model (async)

- Async gRPC server (completion queue based)
- On new request: parse headers + body → allocate slot → fill context → enqueue to batch/single queue → register next request tag on completion queue (immediately ready for next client)
- Worker finishes RPC: initiates async gRPC response write, tags with slot index
- Dedicated gRPC completion poller thread: calls `cq->Next()`, gets send-complete events, frees slot + data buffer
- For PUT responses (no body): worker can use sync Finish (fast, no data to stream) and free slot immediately

Note: async gRPC is the most complex part of this design (manual lifecycle, completion queue tags, request/response state). Implementation order: perf driver async first (validates the worker pool + context table), then gRPC sync (existing model, validates the queue plumbing), then gRPC async last.

## Perf Driver Model

- Producer loop: generate request → allocate slot → fill → enqueue. If QUEUE_FULL: wait on `eventfd_slots_freed` until slots available, retry.
- Worker finishes: increments stats atomically via pointer in context slot, frees slot, writes 1 to `eventfd_slots_freed` (wakes perf driver if blocked).
- No separate reply processing — worker does it inline.

## Backpressure

- Context table full (no free slot) = QUEUE_FULL
- gRPC: return 503 Service Unavailable → S3 clients retry with exponential backoff
- Perf driver: caller sleeps until slots free up (worker completions release slots)
- nginx: can enforce `client_max_body_size` to bound max data per request

## Zero-Copy Flow (PUT)

1. gRPC/perf: receive body → allocate data buffer from pool → copy body in (one copy from network/source)
2. Allocate context slot → fill fixed+variable fields → store data_ptr in slot
3. Push slot index to batch/single queue (no data movement)
4. Worker reads slot by index → accesses data via ptr → commits to FDB / writes to disk
5. Worker sends response via client_handle → frees data buffer → frees slot

## Zero-Copy Flow (GET)

1. Request arrives → allocate context slot → fill params → push to single queue
2. Worker reads slot → loads object from FDB/disk → allocates data buffer → fills it
3. Worker stores data_ptr in slot → sends response to client (streams from data buffer)
4. After send complete: frees data buffer → frees slot

## Not in Scope (TBD)

- Spillover mode for requests exceeding 2KB slot
- CPU pinning for worker threads
- Lock-free queue implementation (mutex + CV for now)
- Async gRPC streaming for large GET responses
- Data pool size-class allocator
- io_uring SQE batching (submit multiple ops per syscall)
- FDB future grouping (multiple futures per eventfd wake)

## Concurrency Model

### Slot Ownership

Each slot transitions through owners. Only one thread touches a slot at any time:
- `FREE` → producer takes ownership (allocate from free-list)
- `QUEUED` → slot index in a queue; no one reads/writes the slot body
- `PROCESSING` → worker owns the slot (extracted from queue; reads params, writes results, issues I/O)
- `SENDING` → gRPC completion poller owns the slot (response in flight to client)
- `FREE` → worker or gRPC completion handler releases (free-list push)

No locks per slot. Ownership is implicit and serial:

| Transition | Who | How | Lock held |
|---|---|---|---|
| FREE → owned by producer | Producer thread | free-list pop | free-list mutex |
| Producer fills slot | Producer thread | direct write | none (exclusive owner) |
| Slot → QUEUED | Producer thread | queue push | queue mutex |
| QUEUED → PROCESSING | Worker thread | queue extract | queue mutex |
| Worker processes slot | Worker thread | direct read/write | none (exclusive owner) |
| Worker chains batch | Worker thread | sets next_in_batch on each slot | none (all extracted, exclusively owned) |
| PROCESSING → SENDING (GET) | Worker thread | initiates async gRPC send | none |
| SENDING → FREE (GET) | gRPC poller thread | send-complete event | free-list mutex |
| PROCESSING → FREE (PUT) | Worker thread | response sent, slot released | free-list mutex |

No contention on slot data ever. The only shared-state mutexes:
- Queue mutex (batch or single): protects deque during push/extract
- Free-list mutex: protects available-slot stack

### Batch Linking

Each slot has a `next_in_batch` field (uint16_t, 0xFFFF = end of chain). Worker locks queue, extracts up to batch_size entries, chains them via `next_in_batch`. First extracted slot is the batch leader — holds group state (FDB txn, FDB futures, io_uring tracking). Member slots are accessed via the chain.

### Queue Locking

- Batch queue: mutex + notify on `eventfd_requests`
- Single queue: same mutex (or separate mutex if contention is high)
- Free-list: separate mutex (or atomic CAS stack for lock-free)

### FDB Callback Thread Safety

`fdb_future_set_callback` fires on the FDB network thread. The callback only writes to an eventfd (atomic 8-byte write) — no shared state access. The worker thread reads from the eventfd and then accesses the future. No race: the callback signals, the worker reacts.

Each worker tracks its own pending FDB futures in a local list (no sharing between workers). When eventfd fires, the worker scans its own list for resolved futures.

### io_uring Thread Safety

Each worker has its own io_uring instance (separate SQ/CQ rings). No sharing between workers. Each worker submits to its own ring and reaps from its own CQ.

## Error Handling

- FDB retriable errors (conflict 1020, etc.): retry from appropriate state (re-issue reads for txn ops)
- FDB permanent errors (key too large, txn too large): set error_code in slot, send error response, free slot
- io_uring write failure: set error_code, send error response, free slot + data buffer. Stale P:O cleaned by sweeper.
- io_uring read failure (GET): send 500 to client, free slot
- Slot overflow (>2KB): reject at enqueue time, never enters queue
- Data pool exhausted: reject at enqueue time with 503 (same as QUEUE_FULL)

## Shutdown

1. Stop accepting new requests (gRPC server shutdown, perf driver stop flag)
2. Drain request queues: workers continue processing until both queues empty
3. Wait for all in-flight state machines to reach terminal state (pending FDB commits resolve, pending io_uring ops complete)
4. Close io_uring instances
5. FDB network shutdown
6. Free pools (context table, data pool)

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `KVRGW_CTX_SLOTS` | 4096 | Context table size (max in-flight requests) |
| `KVRGW_CTX_SLOT_SIZE` | 2048 | Bytes per slot |
| `KVRGW_DATA_POOL_MB` | 256 | Data pool total size |
| `KVRGW_WORKER_THREADS` | (cores) | Worker thread count |
| `KVRGW_BATCH_SIZE` | 5 | Max entries per batch FDB txn |
| `KVRGW_BATCH_TIMEOUT_US` | 1000 | Max wait before flushing partial batch |

## Data Structures

### Typed IDs

```cpp
using tenant_id_t = uint32_t;
using bucket_id_t = uint64_t;

class ref_tag_t {
  uint32_t rgw_id_;
  uint64_t seq_id_;
public:
  void serialize(uint8_t* out) const;       // host → 12B BE wire
  static ref_tag_t deserialize(const uint8_t* src);
  std::string to_hex() const;               // blob filenames
  static ref_tag_t from_hex(std::string_view);
};

class __attribute__((packed)) etag_t {
  uint8_t bytes_[16];
  uint16_t part_count_{0};
public:
  void serialize(uint8_t* out) const;
  void deserialize(const uint8_t* src);
  std::string to_hex() const;               // S3 ETag header ("hex" or "hex-N")
  static etag_t from_hex(std::string_view); // parse If-Match header
  uint16_t part_count() const;
  bool is_multipart() const;
};
static_assert(sizeof(etag_t) == 18);

enum class ChunkType : uint8_t {
  INLINE = 0,
  CHILD_D = 1,
  STORAGE = 2,
};
static_assert(sizeof(ChunkType) == 1);

enum class ObjFlags : uint8_t {
  // bit definitions TBD
};
static_assert(sizeof(ObjFlags) == 1);
```

### Condition Types

```cpp
class __attribute__((packed)) cond_flags_t {
  uint8_t bits_{0};
public:
  static constexpr uint8_t kIfMatch           = 0x01;
  static constexpr uint8_t kIfNoneMatch       = 0x02;
  static constexpr uint8_t kIfModifiedSince   = 0x04;
  static constexpr uint8_t kIfUnmodifiedSince = 0x08;
  static constexpr uint8_t kHasMtime          = 0x10;
  static constexpr uint8_t kHasSize           = 0x20;
  static constexpr uint8_t kEtagIsStar        = 0x40;

  bool has_any() const;
  bool if_match() const;
  bool if_none_match() const;
  bool if_modified_since() const;
  bool if_unmodified_since() const;
  bool has_mtime() const;
  bool has_size() const;
  bool etag_is_star() const;
  void set_if_match();
  void set_if_none_match();
  void set_if_modified_since();
  void set_if_unmodified_since();
  void set_has_size();
  void set_etag_star();
  void clear();
};
static_assert(sizeof(cond_flags_t) == 1);

struct __attribute__((packed)) Condition {
  cond_flags_t flags{};     // 1B
  uint8_t _pad{0};          // 1B
  etag_t etag{};            // 18B
  uint32_t mtime{0};        // 4B
  uint64_t size{0};         // 8B
};
static_assert(sizeof(Condition) == 32);
```

### BatchCommitEntry Layout

Unified buffer design. All variable-length fields packed into a single `buffer[]`. Condition placed after the buffer — reclaimable when `has_condition == 0` (buffer logically extends by 32B).

```
Offset  Field                         Size
------  -----                         ----
[0]     uint64_t object_size          8
[8]     void* data_ptr                8
[16]    void* tags_ptr                8
[24]    steady_clock::time_point      8
[32]    bucket_id_t (uint64_t)        8
[40]    tenant_id_t (uint32_t)        4
[44]    last_modified_sec             4
[48]    last_modified_nsec            4
[52]    data_len                      4
[56]    ref_tag_t                     12  (8-byte aligned)
[68]    object_name_len               2
[70]    tags_payload_len              2
[72]    etag_t                        18  packed
[90]    ChunkType                     1
[91]    ObjFlags                      1
[92]    content_type_len              1
[93]    tags_count                    1
[94]    bucket_name_len               2
[96]    has_condition                  1
[97]    _pad                          1
[98]    buffer[1728]                  1728
[1826]  Condition                     32  packed (reclaimable)
[1858]  std::promise<KvrgwErrorCode>  ~32 (cold, tail)
[~1890] -- end --
```

Buffer internal layout (packed sequentially, lengths from header fields):
```
buffer[0 .. object_name_len)                 — object_name
buffer[object_name_len .. +bucket_name_len)  — bucket_name
buffer[.. +content_type_len)                 — content_type
buffer[.. +tags_payload_len)                 — inline_tags
buffer[.. remainder)                         — inline data
```

When `has_condition == 0`: Condition 32B reclaimable, logical buffer = 1760B.
When `has_condition == 1`: Condition valid at fixed offset, buffer = 1728B.

Inline data capacity = `buffer_size - object_name_len - bucket_name_len - content_type_len - tags_payload_len`. If object fits in leftover, no `data_ptr` needed. Otherwise `data_ptr` points to data pool.

`std::promise` at tail — heap-allocating anyway, kept away from hot fields. Removed when gRPC goes fully async.

### InFlightBatch (per-thread)

```cpp
struct InFlightBatch {
  uint8_t phase;              // 0=EMPTY, 1=PHASE1, 2=PHASE2, 3=PHASE3
  uint8_t step;               // WORKING=0, COMMIT_ISSUED=1, IO_ISSUED=2
  uint8_t entry_count;
  uint8_t storage_entry_count;
  uint8_t p3_attempt;
  bucket_id_t group_bucket_id;
  // group_ref_tag derived from storage_entries[0].ref_tag

  BatchCommitEntry entries[kMaxBatchSize];
  GroupPoEntry storage_entries[kMaxBatchSize];

  std::unique_ptr<KvTransaction> txn;       // single, reused P1 then P3
  FdbFuture commit_future;                  // single, reused

  void reset();
};
```

- `storage_entry_count > 0` implies storage tier (no separate bool)
- Single `txn` + `commit_future` — P1 and P3 never coexist

### WorkerState (per-thread)

```cpp
struct WorkerState {
  static constexpr int kMaxInflight = 16;
  InFlightBatch slots[kMaxInflight];
};
```

Each worker thread owns its own `WorkerState`. No shared inflight arrays — only the request queue is shared (mutex-protected).

## Stat Counters

| Counter | Description |
|---------|-------------|
| `queue_full_rejects` | Requests rejected due to no free context slots |
| `slot_overflow_rejects` | Requests rejected due to >2KB metadata |
| `data_pool_exhausted` | Requests rejected due to no free data buffers |
| `batch_commits` | FDB transactions committed (batch path) |
| `single_commits` | FDB transactions committed (single path) |
| `fdb_retries` | FDB conflict retries across all ops |
| `iouring_writes` | Disk writes submitted |
| `iouring_reads` | Disk reads submitted |
| `iouring_errors` | io_uring operation failures |
| `slots_in_use` | Current occupied slots (gauge) |
| `data_pool_in_use_mb` | Current data pool usage (gauge) |