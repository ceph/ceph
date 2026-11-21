# Session Knowledge Base — Aug 26-27, 2026

## What Was Done This Session

### 1. ErrInsertion Fault Injection Framework
- **Files**: `backend/src/err_insertion.hpp`, `backend/src/err_insertion.cpp`, `backend/tests/err_insertion_test.cpp`
- Cache-friendly two-level layout: `flags_[]` (hot, 1 byte per fault) + `FaultPayload payload_[]` (cold)
- `is_error_active(FaultType)` inline with `[[likely]]`, zero-cost when no faults active
- Modes: fixed (always fire), counter-based (periodic every Nth call), time-based (periodic every N microseconds)
- Burst support: trigger fires N consecutive times per activation
- Admin socket commands: `set-error <name> [period=N] [burst=N] [interval_us=N]`, `clear-error <name>`
- FaultType enum: `kAbortAfterBatchPhase2`, `kAbortAfterSinglePhase2`, `kAbortSweeperAfterPutGo`, `kAbortGcWorkerMidGroup`
- Wired into admin_server.cpp and injection points in service_impl.cpp
- Unit test: 10/10 pass

### 2. RefTag Type Cleanup (Step 0)
- `using RefTag = std::array<uint8_t, 12>` in `ref_tag.hpp`
- `ref_tag_view(const RefTag&) → std::string_view` helper
- `RefTagGenerator::next()` returns `RefTag` (was `std::string`)
- Changed: `PoKeyParts`, `GoKeyParts`, `DKeyParts` parse structs use `RefTag`
- Changed: `BatchCommitEntry::ref_tag`, `PutObjectRequest::ref_tag`, `PutInTxnParams::ref_tag`
- All callers in service_impl.cpp, perf_driver.cpp, gc_worker.cpp, gc_ctl.cpp fixed

### 3. Group GC Infrastructure
- **Group P:O value format**: `[count 2B BE][ref_tag 12B + size 8B BE]×N[created_at 4B BE]` (20B per entry)
- **Group G:O key**: `G:<size_tier 1B><shard 4B><bucket_id 8B><G 1B><group_ref_tag 12B>` (category 'G')
- **Group G:O value**: `[count 2B BE][ref_tag 12B + chunk 1B + flags 1B + size 8B BE]×N` (22B per entry)
- Structs: `GroupPoEntry`, `GroupPoValue`, `GroupGcEntry`, `GroupGcValue` (all use `std::array`, stack-allocated)
- Keys: `parse_group_po_key()`, `make_group_go_key()`, `parse_group_go_key()`
- **Sweeper**: detects group P:O (category 'G'), creates group G:O, deletes group P:O in single txn
- **GcWorker**: detects group G:O, iterates entries, removes blobs, deletes key
- `kMaxBatchSize` moved to `constants.hpp`

### 4. Batch PUT with Tags
- `PreparedTags` moved from service_impl.cpp (file-local) to `tag_value.hpp`
- `std::optional<PreparedTags> tags` added to `BatchCommitEntry`
- Removed `!req.tags` gate from batch routing — tagged PUTs now batch
- Default `batch_size` changed from 1 to 5 in `TierConfig`

### 5. Tests Added
- `scripts/test_white_box.sh` — fault injection integration test (single Phase 2 abort)
- `scripts/test_batch_tiers.sh` — 3 tiers × 1000 objects via s5cmd, verifies avg_batch_size >= 3
- Phase 5b (batch tiers) added to medium test suite in `run_test_plan.sh`
- White-box test phase added to TEST_PLAN.md

### 6. Async Batch Worker (INCOMPLETE — has bug)
- `commit_async()` and `FdbFuture::is_ready()` added to `kv_store.hpp/cpp`
- `BatchCommitQueue::run()` rewritten with `InFlightBatch` state machine
- **BUG**: shared `inflight_[]` array accessed by multiple worker threads → segfault
- **Band-aid applied**: reduced to single worker thread (`start()` spawns 1 thread regardless of param)
- **Correct fix needed**: per-thread `WorkerState` with private inflight slots (see plan below)
- Reload was in progress; backends may need `reload.sh --clean` to restart

## Commits Made

```
1be0430 Group GC infrastructure + ErrInsertion fault injection framework (29 files)
017e16a Batch PUT with tags support + batch_size=5 default (5 files)
```

The async worker rewrite is NOT committed (unstable, has the threading bug).

## Current Code State

- Build passes (single-thread band-aid)
- Medium test plan: 6/6 pass (before async rewrite; after rewrite needs verification)
- S3 tests: 204/204 pass (before async rewrite)
- Async rewrite introduced segfaults on multi-thread — single-thread workaround in place but untested fully

## Plans and Design Docs

| File | Purpose |
|------|---------|
| `Async-Server-Redesign.md` | Full async server vision (context table, data pool, io_uring, eventfd) |
| `.cursor/plans/Async Workers Incremental-593ac721.plan.md` | Incremental async steps (current work) |
| `.cursor/plans/Group GC Sweeper-593ac721.plan.md` | Group GC + sweeper (completed) |
| `.cursor/plans/Batch Tags Default-593ac721.plan.md` | Batch tags + default (completed) |

## Next Steps (from Async Workers Incremental plan)

### Immediate: Fix async batch worker
1. Revert to per-thread `WorkerState` model (each thread owns its own `InFlightBatch slots[16]`)
2. Multiple threads safe (no shared inflight state, only shared queue mutex)
3. Build + reload --clean + run --medium + s3 tests → 100% pass

### Then: Steps 2a-2d (parallel single ops)
- 2a: GET parallel
- 2b: DELETE parallel
- 2c: CopyObject parallel
- 2d: LIST parallel

## Key Design Decisions (this session)

1. **No dynamic allocation on hot path** — fixed arrays, stack buffers, pre-allocated pools
2. **Typed IDs**: `bucket_id_t` (uint64_t), `tenant_id_t` (uint32_t), `ref_tag_t` (class: rgw_id + seq_id, serialize/deserialize/to_hex), `etag_t` (packed class: 16B + part_count), `cond_flags_t` (packed 1B bitfield), `Condition` (packed 32B struct)
3. **Per-thread worker state** — no shared inflight arrays, only shared queue (mutex-protected)
4. **Phase/Step tracking** — `uint8_t phase` (EMPTY/1/2/3) + `uint8_t step` (WORKING/COMMIT_ISSUED/IO_ISSUED)
5. **Single txn+future reused** — P1 and P3 never coexist, one field serves both
6. **group_ref_tag removed** — derived from `storage_entries[0].ref_tag`
7. **is_storage_tier removed** — check `chunk_type == CHUNK_STORAGE` directly
8. **Flags = 0 for sweeper G:O entries** — no shared data, no tags, no annotations (proven by AWS spec)

## User Rules

1. Do not implement without explicit "implement the plan now"
2. User makes all design decisions
3. Short concise answers, don't volunteer unrequested info
4. User drives discussion
5. No fixed answer menus (no AskQuestion tool)
6. Never hide failures
7. Before agent mode: repeat instructions, state what to exec, wait for approve/deny
8. `\` = newline escape
9. No unsolicited flowcharts
10. Avoid dynamic allocation; prefer stack; small count → std::array over vector
11. Prefer caller-owned buffers passed in over callee heap allocation
