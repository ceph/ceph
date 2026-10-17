# Async Refactor — Session Summary

## Primary Request and Intent

Transition the KV-RGW backend to a non-blocking, asynchronous processing model with strong typing. Completed in this session:

1. Async batch workers with per-thread WorkerState and InFlightBatch state machine
2. Typed IDs: `tenant_id_t`, `bucket_id_t`, `version_id_t` — wired everywhere, zero raw types
3. Typed enums: `ChunkType`, `VersioningState` — wired everywhere
4. `etag_t` class with ObjectValue accessors
5. `cond_flags_t` + `Condition` struct defined (not yet wired into runtime)
6. `version_id_t` hex format (backend + Go frontend)
7. Fixed batch PUT version_id propagation bug (BatchPutResult)
8. Fixed V: key scan prefix bug (ListObjectVersions)
9. Fixed operator precedence bug in storage-tier disk writes

## Test Results

- Quick: PASSED (4/4)
- Fast: PASSED (3/3)
- Medium: PASSED (7/7) including batch tiers
- S3: 204/204 passed
- Perf: 51,464 IOPS aggregate (3 instances, batch_size=10, 8KB, 60s)

## What's Done

- `typed_ids.hpp/cpp` — all typed ID classes and enums
- `keys.hpp/cpp` — all key functions use `bucket_id_t`, `version_id_t`
- `service_impl.hpp/cpp` — async workers, typed IDs everywhere
- `object_value.hpp/cpp` — `bucket_id_t`, `version_id_t`, `VersioningState`, `etag_t` accessors
- `frontend/backend_kvrgw.go` — hex version_id format
- `perf_driver.cpp`, `sweeper.cpp`, `gc_worker.cpp`, `gc_ctl.cpp` — all updated

## What Remains

- `etag_t` + `Condition` wired into condition checking (replace string if_match/if_none_match)
- `ref_tag_t` class (replace RefTag = std::array)
- `ObjFlags` enum (flags field is bitfield, needs operator overloads)
- BatchCommitEntry unified buffer layout
- WorkerState::kMaxInflight = 16 (currently 4)
- eventfd-based wake (eliminate 50us polling)

## Key Technical Concepts

- **Asynchronous Processing**: Non-blocking operations, overlapping I/O and computation.
- **Fault Injection**: `ErrInsertion` framework for white-box testing.
- **Typed IDs**: Replacing `std::string` with native types (`uint64_t`, `uint32_t`) and custom packed structs (`ref_tag_t`, `etag_t`, `cond_flags_t`).
- **Zero-Copy**: Minimizing data movement by using shared buffers and slot-based processing.
- **Memory Pools**: Pre-allocated context table and data pool to avoid runtime heap allocations.
- **Batching**: Grouping multiple operations into a single FDB transaction.
- **FDB Asynchronous API**: Using `fdb_transaction_commit` and `fdb_future` for non-blocking FDB operations.
- **io_uring**: Linux asynchronous I/O interface for non-blocking disk operations.
- **Event-Driven Architecture**: Workers using `epoll_wait` on multiple event sources (`eventfd_requests`, `eventfd_iouring`, `eventfd_fdb`, `timerfd`).
- **State Machines**: Managing complex multi-step operations (like batch PUT/DELETE) as state transitions within context slots.
- **Backpressure**: Mechanisms (QUEUE_FULL, 503 HTTP status) to manage overload.
- **Producer-Consumer Model**: Callers enqueue requests, workers process them.
- **RefTag**: 12-byte unique identifier for object data.
- **Group P:O/G:O**: Batching mechanism for crash recovery and GC.
- **Conditional Requests**: S3 `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `If-Match-Size` headers.

## Files and Code Sections

- **`poc_as_built.md`**: Updated to mention ErrInsertion, RefTag type, group G:O infrastructure, updated group P:O format, and fault injection.
- **`architecture.md`**: Updated to include ErrInsertion in backend description and mention RefTag type in key files.
- **`cpp-data-structures.md`**: Updated with RefTag type alias, `GroupPoEntry`, `GroupPoValue`, `GroupGcEntry`, `GroupGcValue` structs, `ErrInsertion` class layout, and `GroupPoKeyParts`/`GroupGoKeyParts` to Parse Result Structs. `kMaxBatchSize` moved to `constants.hpp`.
- **`s3-operations-code.md`**: Updated to mention fault injection points in `PutObject` paths.
- **`batch_mode.md`**: Updated "Phase 1 — Group P:O" with new value format including per-entry sizes, and noted `kMaxBatchSize` moved to `constants.hpp`.
- **`background-and-admin.md`**: Updated Sweeper section for group P:O handling, GC Worker for group G:O handling, and added ErrInsertion/fault injection section.
- **`gc_admin.md`**: Added `set-error` and `clear-error` commands to the admin socket protocol.
- **`TEST_PLAN.md`**: Added white-box test phase (`scripts/test_white_box.sh`).
- **`backend/src/err_insertion.hpp`**: New file — `FaultType` enum, `FaultPayload` struct, `ErrInsertion` class.
- **`backend/src/err_insertion.cpp`**: New file — implements `ErrInsertion` methods.
- **`backend/tests/err_insertion_test.cpp`**: New file — unit tests for `ErrInsertion`.
- **`backend/CMakeLists.txt`**: Modified to add `err_insertion.cpp` and test binary.
- **`backend/src/admin_server.hpp`**: Modified to include `err_insertion.hpp` and add `ErrInsertion*` to constructor.
- **`backend/src/admin_server.cpp`**: Modified to parse and dispatch `set-error` and `clear-error` commands.
- **`backend/src/main.cpp`**: Modified to pass `&service.err_insertion()` to `AdminServer`.
- **`backend/src/service_impl.hpp`**: Modified — `ErrInsertion` member, `BatchCommitEntry` uses `RefTag` and `std::optional<PreparedTags>`, typed function signatures.
- **`backend/src/service_impl.cpp`**: Modified — removed `PreparedTags` struct, updated `put_object_route()`, `commit_batch()`, fault injection points, `BatchCommitQueue::run()` rewritten for async.
- **`backend/src/kv_store.hpp`**: Modified — `FdbFuture::is_ready()`, `KvTransaction::commit_async()`, `KvTransaction::resolve_commit()`.
- **`backend/src/kv_store.cpp`**: Implemented `commit_async()` and `resolve_commit()`.
- **`backend/src/ref_tag.hpp`**: Defined `RefTag` as `std::array<uint8_t, kRefTagSize>`, `ref_tag_view()` helper.
- **`backend/src/ref_tag.cpp`**: Updated `RefTagGenerator::next()` to return `RefTag`.
- **`backend/src/keys.hpp`**: Updated parse structs to use `RefTag`. Added `GroupPoKeyParts`, `GroupGoKeyParts`.
- **`backend/src/keys.cpp`**: Updated parse functions. Implemented group key parsers.
- **`backend/src/gc_value.hpp`**: Defined group structs. Updated signatures.
- **`backend/src/gc_value.cpp`**: Implemented group value serialize/parse functions.
- **`backend/src/gc_worker.cpp`**: Modified to use `ref_tag_view` and process `GroupGoKeyParts`.
- **`backend/tools/gc_ctl.cpp`**: Updated to use `ref_tag_view`.
- **`backend/src/constants.hpp`**: Moved `kMaxBatchSize` here.
- **`scripts/test_white_box.sh`**: New — batch Phase 2 abort fault injection test.
- **`scripts/test_batch_tiers.sh`**: New — batch PUT tiers integration test.
- **`new_frontend_model.md`**: Knowledge base of the session.
- **`Async-Server-Redesign.md`**: Full async server redesign plan.

## Errors and Fixes

- **Compile errors after RefTag refactor**: Wrapped `RefTag` instances with `ref_tag_view()` helper at all call sites.
- **Linker errors for `ErrInsertion`**: Added `err_insertion.cpp` to CMakeLists.txt.
- **`test_white_box.sh` fault not firing**: Changed to target instance 0 directly via `s3cmd`.
- **`test_white_box.sh` unexpected blob count (7 vs 1)**: Added `--max-retries=0` to `s3cmd`.
- **`test_batch_tiers.sh` missing `KeyCount`**: Changed to count `Contents` array length.
- **`test_batch_tiers.sh` low `avg_batch_size`**: Increased `batch_timeout_us` from 1000 to 10000.
- **Backend segfaults after async worker rewrite**: Shared mutable `inflight_[]` state accessed by multiple threads. Band-aid: reduced to 1 worker thread. Correct fix (pending): per-thread `WorkerState`.

## InFlightBatch Design (Latest)

```cpp
struct InFlightBatch {
    uint8_t phase;           // 1, 2, or 3
    uint8_t step;            // enum: kStepIdle, kStepCommitPending, kStepStorageWrite, ...
    uint8_t entry_count;
    uint8_t storage_entry_count;
    uint8_t p3_attempt;
    bool    has_storage_tier;

    bucket_id_t  group_bucket_id;
    ref_tag_t    group_ref_tag;

    BatchCommitEntry entries[kMaxBatchSize];
    GroupPoEntry     storage_entries[kMaxBatchSize];

    std::unique_ptr<KvTransaction> txn;
    FdbFuture commit_future;
};
```

`BatchCommitEntry` uses typed IDs (`tenant_id_t`, `bucket_id_t`, `ref_tag_t`, `etag_t`), `Condition` struct, inline char arrays for `bucket_name`, `content_type`, `object_name`, and inline data buffers.

## Pending Tasks (Incremental Plan)

1. **Step 1**: Rewrite batch worker loop for async commit (InFlightBatch, polling, max_inflight)
2. **Step 1**: Add `commit_async()` to KvStore/KvTransaction (IN PROGRESS — implemented but segfault with multiple workers)
3. **Step 1**: Build + test
4. **Step 2a**: GET parallel — async reads, poll futures
5. **Step 2b**: DELETE parallel — async reads + async commit
6. **Step 2c**: CopyObject parallel — async reads + async commit
7. **Step 2d**: LIST parallel — async Get(B:) + range_scan

## Reference Documents

A new agent should load:
- `new_frontend_model.md` — detailed knowledge base
- `Async-Server-Redesign.md` — full async server redesign plan
- `docs/batch_mode.md` — batch mode documentation
- `docs/background-and-admin.md` — sweeper/GC/admin docs
- `docs/cpp-data-structures.md` — C++ struct definitions
- `docs/architecture.md` — system architecture
- `TEST_PLAN.md` — test plan
