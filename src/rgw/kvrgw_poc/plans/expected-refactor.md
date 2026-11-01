# Plan: Remove Exceptions from FDB Path — std::expected Refactor

## Overview

Replace all exception-based error handling in the FDB path with `std::expected<T, fdb_error_t>` (C++23). No exceptions on the hot path. Native FDB error codes propagated to callers.

## Prerequisite

- All current tests pass (10/10 slow, 14/14 chaos verified)
- No code changes pending

## Steps

### 1. Upgrade to C++23

File: `backend/CMakeLists.txt`
```
set(CMAKE_CXX_STANDARD 23)
```

### 2. Rewrite fdb_blocking.hpp/cpp

File: `backend/src/fdb_blocking.hpp`, `backend/src/fdb_blocking.cpp`

- `transaction_get` → `std::expected<std::optional<std::string>, fdb_error_t>`
- `transaction_commit` → `std::expected<void, fdb_error_t>`
- `transaction_get_range` → `std::expected<std::vector<KeyValue>, fdb_error_t>`
- Add: `constexpr bool is_retriable(fdb_error_t code) { return code == 1020; }`
- Keep `check_error()` only for FDB setup (non-critical path: `fdb_select_api_version`, `fdb_create_database`)

### 3. Rewrite kv_store.hpp/cpp

File: `backend/src/kv_store.hpp`, `backend/src/kv_store.cpp`

**KvTransaction:**
- `get(key)` → `std::expected<std::optional<std::string>, fdb_error_t>`
- `commit()` → `std::expected<void, fdb_error_t>`
- `range_scan(start, end, limit)` → `std::expected<std::vector<RangeScanResult>, fdb_error_t>`
- `put(key, value)` — stays `void` (mutation queued, fails at commit)
- `del(key)` — stays `void` (mutation queued, fails at commit)
- Remove `atomic_add()` (replaced by transactional read-modify-write already)
- Remove `abort()` (unused)

**KvStore:**
- `get(key)` → `std::expected<std::optional<std::string>, fdb_error_t>`
- `range_scan(start, end, limit)` → `std::expected<std::vector<RangeScanResult>, fdb_error_t>`
- `allocate_rgw_id()` → `std::expected<uint32_t, fdb_error_t>`
- Remove `put()`/`del()` on KvStore (only used via transactions)

### 4. Remove is_fdb_conflict()

File: `backend/src/service_impl.cpp`

Delete:
```cpp
bool is_fdb_conflict(const std::exception& ex) { ... }
```

Replace with `fdb::is_retriable(err)` at each retry site.

### 5. Rewrite retry loops in service_impl.cpp

8 retry loops to update:

| Line | Function | Pattern |
|------|----------|---------|
| 234 | AddTenant | try/catch → expected |
| 455 | put_object_phase3 | try/catch → expected |
| 519 | CreateBucket | try/catch → expected |
| 584 | put_object_single_txn | try/catch → expected |
| 388 | delete_multi_try_commit | try/catch → expected |
| 679 | PutObject (select_storage_tier Phase 1) | try/catch → expected |
| 1062 | DeleteMulti batching | try/catch → expected |
| 1115 | DeleteBucket | try/catch → expected |

New pattern:
```cpp
for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
  auto tr = store_.begin_transaction();
  auto val = tr->get(key.view());
  if (!val) return grpc::Status(grpc::StatusCode::INTERNAL, fdb_get_error(val.error()));
  // ... mutations ...
  auto rc = tr->commit();
  if (rc) break;  // success
  if (!fdb::is_retriable(rc.error())) {
    return grpc::Status(grpc::StatusCode::INTERNAL, fdb_get_error(rc.error()));
  }
  // retriable — continue loop
}
```

### 6. Top-level RPC handlers

Lines: 679, 757, 836, 862, 894, 1036, 1062, 1115

Remove `try { ... } catch (const std::exception& ex) { return INTERNAL; }` wrappers. Errors propagate via return values. If a non-retriable FDB error reaches the RPC handler, map to `grpc::INTERNAL` with `fdb_get_error(code)`.

### 7. Sweeper (backend/src/sweeper.cpp)

Lines 25, 69 — replace `catch(...)` with:
```cpp
auto rc = tr->commit();
if (!rc) continue;  // skip entry, best-effort
```

### 8. GcWorker (backend/src/gc_worker.cpp)

Line 134 — replace `catch(...)` with expected checks:
```cpp
auto tr = store_.begin_transaction();
tr->del(row.key);
auto rc = tr->commit();
if (!rc) continue;  // skip, best-effort
```

### 9. allocate_rgw_id (backend/src/kv_store.cpp)

Line 173 — rewrite retry loop:
```cpp
std::expected<uint32_t, fdb_error_t> KvStore::allocate_rgw_id() {
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr = begin_transaction();
    auto val = tr->get(key.view());
    if (!val) return std::unexpected(val.error());
    // ... read counter, put incremented ...
    auto rc = tr->commit();
    if (rc) return static_cast<uint32_t>(next);
    if (!fdb::is_retriable(rc.error())) return std::unexpected(rc.error());
  }
  return std::unexpected(1020);  // exhausted retries
}
```

Caller in `main.cpp`:
```cpp
auto rgw_id = store.allocate_rgw_id();
if (!rgw_id) { std::cerr << fdb_get_error(rgw_id.error()); return 1; }
```

### 10. gc_ctl (backend/tools/gc_ctl.cpp)

Lines 550, 567 — replace try/catch with:
```cpp
auto rows = store.range_scan(prefix.view(), end, limit);
if (!rows) { std::cerr << fdb_get_error(rows.error()); return 1; }
```

### 11. Remove all FDB-path throws

After all callers are updated, remove:
- `throw std::runtime_error(...)` from `fdb_blocking.cpp` (only `check_error` for setup remains)
- `throw std::runtime_error("negative atomic counter value")` from `kv_store.cpp`
- `throw std::runtime_error("allocate_rgw_id: max retries exceeded")` from `kv_store.cpp`

### 12. Verify

- Build with C++23
- Run `--quick` test (build + unit + reload + smoke)
- Run full slow tier (10/10)
- Run chaos tier (14/14)
- Fix simple bugs (typos, missed call sites, wrong error code)
- Do NOT redesign or change any design decisions — only implement the plan as specified

## Rules

- **Do not make design decisions.** Any design question goes to the user.
- **Do not change existing behavior.** Same retry counts, same error mappings, same logic — only the error propagation mechanism changes.
- **Fix simple bugs only.** Compile errors, wrong variable names, missed `.error()` calls. If a fix requires a design choice, stop and ask.

## Files touched

| File | Changes |
|------|---------|
| `CMakeLists.txt` | C++23 |
| `fdb_blocking.hpp` | Return types → expected |
| `fdb_blocking.cpp` | Remove throws, return unexpected |
| `kv_store.hpp` | API → expected |
| `kv_store.cpp` | Implement expected, remove atomic_add/abort |
| `service_impl.cpp` | All 8 retry loops + 8 RPC handlers + remove is_fdb_conflict |
| `gc_worker.cpp` | Expected checks |
| `sweeper.cpp` | Expected checks |
| `gc_value.cpp` | move_po_to_go expected |
| `gc_ctl.cpp` | Expected checks |
| `main.cpp` | allocate_rgw_id expected |
| `kv_range_test.cpp` | Update test for expected API |

## Not in scope (this plan)

- Validation throws (keys.cpp, ref_tag.cpp, object_value.cpp) — separate plan
- Admin server socket exceptions — separate plan (startup only)

## Phase 2: DataStore I/O (after Phase 1 passes all tests)

If Phase 1 (FDB path) runs clean (14/14 chaos), proceed to DataStore I/O refactor:

### DataStore throws to remove

File: `backend/src/data_store.cpp`

| Line | Function | Throw |
|------|----------|-------|
| 33 | write | "test filter dropped write" |
| 38 | write | "failed to open data file for write" |
| 42 | write | "failed to write data file" |
| 51 | read_all | "failed to stat data file" |
| 58 | read | "test filter dropped read" |
| 67 | read | "failed to open data file for read" |
| 72 | read | "failed to seek data file" |
| 76 | read | "failed to tell data file size" |
| 80 | read | "read offset past end of data file" |
| 88 | read | "failed to seek data file" |
| 94 | read | "short read from data file" |
| 101 | remove | "test filter dropped remove" |

### New API

```cpp
class DataStore {
 public:
  std::expected<void, std::error_code> write(std::string_view ref_tag, std::string_view data);
  std::expected<std::string, std::error_code> read_all(std::string_view ref_tag) const;
  std::expected<std::string, std::error_code> read(std::string_view ref_tag, uint64_t offset, uint64_t length) const;
  std::expected<void, std::error_code> remove(std::string_view ref_tag);
};
```

### Callers to update

- `service_impl.cpp` — `data_store_.write()` in select_storage_tier Phase 2; `data_store_.read()` in GetObject
- `gc_worker.cpp` — `data_store_.remove()` in CHUNK_STORAGE branch
- `gc_ctl.cpp` — `blob_file_size()` (already handles missing file gracefully)

### Error mapping

- DataStore write failure → `grpc::INTERNAL` (PutObject fails)
- DataStore read failure → `grpc::INTERNAL` (GetObject fails)
- DataStore remove failure in GC → log and skip (best-effort, same as today)

### Error-injection test: `data_store_error_test.cpp`

Pure unit test (no FDB, no gRPC). Exercises every DataStore error path via real filesystem triggers — no mocks needed.

**Approach:** create a DataStore rooted in a temp directory, then trigger real I/O errors and verify the correct `std::error_code` comes back in the expected.

| # | Operation | Trigger | Assert |
|---|-----------|---------|--------|
| 1 | write success | normal write | `has_value()` |
| 2 | write open fail | root dir is read-only (`chmod 444`) | `!has_value()`, `.error()` is a real errno |
| 3 | read success full | write then read_all | data matches |
| 4 | read success partial | write, then read(offset=5, length=3) | correct slice |
| 5 | read missing file | read non-existent ref_tag | `!has_value()` |
| 6 | read_all missing file | read_all non-existent ref_tag | `!has_value()` |
| 7 | read offset past end | write 10 bytes, read(offset=20, length=1) | `!has_value()` |
| 8 | remove success | write then remove | `has_value()` |
| 9 | remove idempotent | remove non-existent ref_tag | `has_value()` (ENOENT is OK) |
| 10 | test filter write | filter returns false | `!has_value()` |
| 11 | test filter read | filter returns false | `!has_value()` |
| 12 | test filter remove | filter returns false | `!has_value()` |

**Error-code preservation rules** (verified by assert):
- Every non-success result carries a non-zero `std::error_code`
- No error is silently swallowed or mapped to a different category
- `remove()` on ENOENT returns success (idempotent — same as today)

**Caller propagation** (verified by red-team review, same as Phase 1):
- `service_impl.cpp` PutObject: `data_store_.write()` error → `grpc::INTERNAL`
- `service_impl.cpp` GetObject: `data_store_.read()` error → `grpc::INTERNAL`
- `gc_worker.cpp`: `data_store_.remove()` error → skip and continue (best-effort)

**Build:** link `data_store.cpp` + `ref_tag.cpp` + test. No FDB, no gRPC. Add to CMakeLists.txt as `data_store_error_test`.

### Same rules apply

- Do not redesign. Only change error propagation.
- Fix simple bugs only. Design questions go to user.
- Build + load --clean + full chaos after completion.
