# Error Codes Refactor

**Status: COMPLETE**

## Problem

All FDB errors map to `grpc::StatusCode::INTERNAL` via `fdb_internal()`, losing the original error code. Internal functions use `grpc::Status` even though they never cross a gRPC boundary. Retry logic cannot distinguish retriable from permanent errors.

**Bugs fixed:**
1. `is_retriable()` only checked FDB 1020 — missed 1037/1009/1007 under high load
2. FDB read errors inside transactions (e.g. `kv_wait_get`) were never retried — the string-matching retry path never worked

---

## Design

### 1. KvrgwErrorCode enum in `proto/kvrgw.proto`

Enum name: `KvrgwErrorCode`. All values prefixed `KVRGW_ERR_*`. Detailed per-value documentation (trigger conditions, retry category).

```protobuf
enum KvrgwErrorCode {
  KVRGW_ERR_OK = 0;

  // --- S3 errors (100-109) ---

  // The specified object key does not exist in the bucket.
  // Returned by GET, HEAD, DELETE when the target S3 object is missing.
  KVRGW_ERR_NO_SUCH_KEY = 100;

  // The specified bucket does not exist for this tenant.
  // Returned when bucket lookup (B: key) yields no result.
  KVRGW_ERR_NO_SUCH_BUCKET = 101;

  // A bucket with this name already exists for this tenant.
  // Returned by CreateBucket when B: key is already present.
  KVRGW_ERR_BUCKET_ALREADY_EXISTS = 102;

  // The bucket contains objects or pending uploads and cannot be deleted.
  // Returned by DeleteBucket when S:O or P:O scan finds entries.
  KVRGW_ERR_BUCKET_NOT_EMPTY = 103;

  // A conditional write precondition failed (if-match, if-none-match, mtime, size).
  // Returned by PUT, DELETE, COPY when ETag/mtime/size condition is not met.
  KVRGW_ERR_PRECONDITION_FAILED = 104;

  // Access denied by bucket policy (access_flags bitmask).
  // Returned when check_access or verify_bucket_in_txn denies the operation.
  KVRGW_ERR_ACCESS_DENIED = 105;

  // Invalid request parameter (e.g. content-type too long, invalid bucket name chars).
  KVRGW_ERR_INVALID_ARGUMENT = 106;

  // Bucket name violates DNS-compliant naming rules.
  KVRGW_ERR_INVALID_BUCKET_NAME = 107;

  // The specified version ID does not exist for this object.
  KVRGW_ERR_NO_SUCH_VERSION = 108;

  // The tenant name is not registered (T: key missing).
  // Returned when tenant_id_for_name() cannot resolve the tenant.
  KVRGW_ERR_NO_SUCH_TENANT = 109;

  // --- FDB retriable, definitely NOT committed (200-206) ---
  // Category: is_retriable_idempotent() = true
  // Safe to retry unconditionally — the transaction did NOT commit.

  // FDB write-write conflict. Another transaction wrote the same key range.
  // FDB error 1020. Most common retriable error under concurrent writes.
  KVRGW_ERR_FDB_CONFLICT = 200;

  // FDB storage server does not have recent mutations yet.
  // FDB error 1037. Occurs when a storage server is catching up after lag.
  KVRGW_ERR_FDB_PROCESS_BEHIND = 201;

  // FDB read requested a version that does not yet exist.
  // FDB error 1009. Occurs under high commit rates when GRV returns a stale version.
  KVRGW_ERR_FDB_FUTURE_VERSION = 202;

  // FDB transaction has been alive too long (5s default).
  // FDB error 1007. Retry with a fresh transaction.
  KVRGW_ERR_FDB_TRANSACTION_TOO_OLD = 203;

  // FDB could not commit; transaction definitely not applied.
  // FDB error 1020 variant via predicate. Distinct from commit_unknown.
  KVRGW_ERR_FDB_NOT_COMMITTED = 204;

  // FDB cluster version changed (coordinator failover or recovery).
  // FDB error 1039. Retry with a fresh transaction.
  KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED = 206;

  // --- FDB retriable, commit outcome UNKNOWN (205) ---
  // Category: is_retriable_not_idempotent() = true
  // The commit MAY have succeeded. Safe to retry only if the operation is idempotent.

  // FDB commit result unknown — network issue after commit sent.
  // FDB error 1021. The transaction may or may not have been applied.
  // All POC operations are idempotent, so retry is safe here.
  KVRGW_ERR_FDB_COMMIT_UNKNOWN = 205;

  // --- FDB permanent (300-302) ---
  // Not retriable. Operation must fail.

  // FDB key exceeds maximum size (10KB).
  // FDB error 2102. Indicates a bug in key construction.
  KVRGW_ERR_FDB_KEY_TOO_LARGE = 300;

  // FDB value exceeds maximum size (100KB).
  // FDB error 2103. Object too large for inline/D-tier storage.
  KVRGW_ERR_FDB_VALUE_TOO_LARGE = 301;

  // FDB transaction exceeds size limit (10MB).
  // FDB error 2101. Too many mutations in one transaction.
  KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE = 302;

  // --- Internal errors (400-405) ---
  // Not retriable. Indicates bugs or system-level failures.

  // Catch-all for unmapped FDB errors or unexpected failures.
  KVRGW_ERR_INTERNAL = 400;

  // KV value could not be parsed (corrupt or schema mismatch).
  // Indicates data corruption — should never occur in normal operation.
  KVRGW_ERR_CORRUPT_VALUE = 401;

  // Bucket ID in transaction does not match cached value (stale cache race).
  KVRGW_ERR_BUCKET_ID_MISMATCH = 402;

  // Value exceeds the allowed size for its storage tier.
  KVRGW_ERR_VALUE_TOO_LARGE = 403;

  // Transaction conflict retry limit exceeded (10 attempts).
  KVRGW_ERR_TRANSACTION_CONFLICT = 404;

  // Maximum retry attempts exhausted for a retriable error.
  KVRGW_ERR_MAX_RETRIES_EXCEEDED = 405;
}
```

### 2. Response proto change — error_code in every response message

Every gRPC response message gets a `KvrgwErrorCode error_code` field. Backend always returns `grpc::Status::OK`. Application errors travel in the response body, not via gRPC status codes.

```protobuf
message PutObjectResponse {
  KvrgwErrorCode error_code = 1;
  string etag = 2;
  string version_id = 3;
}

message GetObjectResponse {
  KvrgwErrorCode error_code = 1;
  oneof payload { ... }
}
```

**Error model:**
- Application errors (key not found, access denied, precondition failed) are always known before data streaming starts. The first response message carries the error_code.
- Transport failures (disk I/O mid-stream, network drop, crash) break the gRPC stream. The S3 client detects truncation via content-length mismatch or connection reset. No typed error code needed for these — they're not application errors.

**Frontend behavior:**
- Unary RPCs: check `resp.ErrorCode` — if not `KVRGW_ERR_OK`, map to Go error for versitygw
- Streaming RPCs (GetObject): check first message's `error_code` — if not OK, return error immediately. If OK, stream data. Mid-stream stream breaks propagate as io errors (existing behavior).

### 3. Three retry category helpers (switch-based, no range checks)

```cpp
// Definitely NOT committed — safe to retry unconditionally
bool is_retriable_idempotent(KvrgwErrorCode c) {
  switch (c) {
    case KVRGW_ERR_FDB_CONFLICT:
    case KVRGW_ERR_FDB_PROCESS_BEHIND:
    case KVRGW_ERR_FDB_FUTURE_VERSION:
    case KVRGW_ERR_FDB_TRANSACTION_TOO_OLD:
    case KVRGW_ERR_FDB_NOT_COMMITTED:
    case KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED:
      return true;
    default:
      return false;
  }
}

// Commit outcome unknown — safe only if operation is idempotent
bool is_retriable_not_idempotent(KvrgwErrorCode c) {
  return c == KVRGW_ERR_FDB_COMMIT_UNKNOWN;
}

// Union — "can I retry this at all?"
bool is_retriable(KvrgwErrorCode c) {
  return is_retriable_idempotent(c) || is_retriable_not_idempotent(c);
}
```

### 4. FDB error constants (no magic numbers)

```cpp
namespace kvrgw::fdb {
  constexpr fdb_error_t kConflict              = 1020;
  constexpr fdb_error_t kProcessBehind         = 1037;
  constexpr fdb_error_t kFutureVersion         = 1009;
  constexpr fdb_error_t kTransactionTooOld     = 1007;
  constexpr fdb_error_t kCommitUnknownResult   = 1021;
  constexpr fdb_error_t kClusterVersionChanged = 1039;
  constexpr fdb_error_t kKeyTooLarge           = 2102;
  constexpr fdb_error_t kValueTooLarge         = 2103;
  constexpr fdb_error_t kTransactionTooLarge   = 2101;
}
```

### 5. `fdb_to_error()` replaces `fdb_internal()`

```cpp
KvrgwErrorCode fdb_to_error(fdb_error_t err) {
  switch (err) {
    case fdb::kConflict:              return KVRGW_ERR_FDB_CONFLICT;
    case fdb::kProcessBehind:         return KVRGW_ERR_FDB_PROCESS_BEHIND;
    case fdb::kFutureVersion:         return KVRGW_ERR_FDB_FUTURE_VERSION;
    case fdb::kTransactionTooOld:     return KVRGW_ERR_FDB_TRANSACTION_TOO_OLD;
    case fdb::kCommitUnknownResult:   return KVRGW_ERR_FDB_COMMIT_UNKNOWN;
    case fdb::kClusterVersionChanged: return KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED;
    case fdb::kKeyTooLarge:           return KVRGW_ERR_FDB_KEY_TOO_LARGE;
    case fdb::kValueTooLarge:         return KVRGW_ERR_FDB_VALUE_TOO_LARGE;
    case fdb::kTransactionTooLarge:   return KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE;
    default:                          return KVRGW_ERR_INTERNAL;
  }
}
```

### 6. `kvrgw_strerror(KvrgwErrorCode)` — short runtime strings

Returns a static one-liner for logs and admin output. Used for human display only — never for error identification or parsing.

### 7. Internal function signatures change

All internal functions return `KvrgwErrorCode` or `std::expected<T, KvrgwErrorCode>` instead of `grpc::Status`. The ~50 `fdb_internal()` call sites in `service_impl.cpp` become `fdb_to_error()`. `grpc::Status` is removed from the codebase entirely (no `to_grpc_status()` needed — backend always returns gRPC OK).

### 8. ErrorStats counters + admin socket

```cpp
struct ErrorStats {
  std::atomic<int64_t> counts[KvrgwErrorCode_ARRAYSIZE]{};

  void record(KvrgwErrorCode code) {
    counts[static_cast<int>(code)].fetch_add(1, std::memory_order_relaxed);
  }

  int64_t get(KvrgwErrorCode code) const {
    return counts[static_cast<int>(code)].load(std::memory_order_relaxed);
  }

  void reset() {
    for (auto& c : counts) c.store(0, std::memory_order_relaxed);
  }
};
```

Sparse array (406 slots, ~3.2KB) — O(1) index by enum value, no hash overhead.

Admin commands: `GET error-stats`, `RESET error-stats`.

### 9. Go frontend error mapping

Frontend reads `resp.ErrorCode` (proto enum, no string parsing) and maps to Go errors for versitygw:

```go
func mapErrorCode(code pb.KvrgwErrorCode) error {
  switch code {
  case pb.KVRGW_ERR_OK:
    return nil
  case pb.KVRGW_ERR_NO_SUCH_KEY:
    return s3err.GetAPIError(s3err.ErrNoSuchKey)
  case pb.KVRGW_ERR_NO_SUCH_BUCKET:
    return s3err.GetAPIError(s3err.ErrNoSuchBucket)
  case pb.KVRGW_ERR_BUCKET_ALREADY_EXISTS:
    return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
  case pb.KVRGW_ERR_BUCKET_NOT_EMPTY:
    return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
  case pb.KVRGW_ERR_PRECONDITION_FAILED:
    return s3err.GetAPIError(s3err.ErrPreconditionFailed)
  case pb.KVRGW_ERR_ACCESS_DENIED:
    return s3err.GetAPIError(s3err.ErrAccessDenied)
  case pb.KVRGW_ERR_INVALID_ARGUMENT:
    return s3err.GetAPIError(s3err.ErrInvalidArgument)
  default:
    return s3err.GetAPIError(s3err.ErrInternalError)
  }
}
```

Replaces the current `mapGrpcErr()` which switches on `grpc::StatusCode`.

---

## Scope

- All error returns in `service_impl.cpp` — every `grpc::Status` becomes `KvrgwErrorCode`
- `fdb_internal()` removed, replaced by `fdb_to_error()`
- `grpc::Status` removed from all RPC handlers — backend always returns gRPC OK with error_code in response
- `kvrgw_strerror()` for human display only (logs, admin socket)
- Proto enum + error_code field in all response messages
- Go frontend uses `mapErrorCode()` on the proto enum directly (no string parsing)
- Retry logic uses `is_retriable(KvrgwErrorCode)` — no string matching, no grpc code guessing

## Not in scope (now)

- Structured error details (google.rpc.Status rich model)
- Error codes for multipart, encryption, lifecycle (not implemented)
- Structured logging system (separate `logging_design.md`)
- Mid-stream transport failure handling (S3 client detects via content-length mismatch — not an application error)

---

## Files touched

- `proto/kvrgw.proto` — KvrgwErrorCode enum + error_code field in all response messages
- `backend/src/error_codes.hpp` (new) — FDB constants, `fdb_to_error`, `is_retriable*`, `kvrgw_strerror`
- `backend/src/error_codes.cpp` (new) — implementations
- `backend/src/service_impl.cpp` — replace all `fdb_internal()` calls, remove grpc::Status returns, set error_code in responses
- `backend/src/service_impl.hpp` — ErrorStats struct, updated function signatures
- `backend/src/kv_store.hpp` — remove old `is_retriable(fdb_error_t)`
- `backend/src/admin_server.cpp` — `GET/RESET error-stats` commands
- `backend/src/perf_driver.cpp` — use new error types in retry loops
- `frontend/backend_kvrgw.go` — replace `mapGrpcErr()` with `mapErrorCode()` reading proto enum
- `backend/CMakeLists.txt` — add `error_codes.cpp`

---

## Implementation order

1. Add KvrgwErrorCode enum to proto + error_code field in all response messages, regenerate C++ and Go
2. Create `error_codes.hpp/cpp` with FDB constants, `fdb_to_error()`, `kvrgw_strerror()`, three `is_retriable*()` helpers (switch-based)
3. Add ErrorStats struct with admin socket `GET/RESET error-stats`
4. Refactor RPC handlers: return gRPC OK + set error_code in response. Remove `grpc::Status` error returns.
5. Refactor internal functions bottom-up: return `KvrgwErrorCode` / `std::expected<T, KvrgwErrorCode>`
6. Update retry loops to use `is_retriable(KvrgwErrorCode)` — delete old `is_retriable(fdb_error_t)`
7. Update Go frontend: replace `mapGrpcErr()` with `mapErrorCode()` reading proto enum directly

---

## Verification protocol

### After each implementation step (steps 1–7)

```bash
# Build
make -j

# Clean reload
./scripts/reload.sh --clean

# Test suites (cumulative — each includes prior)
./scripts/run_test_plan.sh --fast
./scripts/run_test_plan.sh --quick
./scripts/run_test_plan.sh --medium

# S3 compatibility tests
./scripts/run_ceph_rgw_tests.sh
```

**Pass criteria:**
- All tests must pass 100%
- Check `GET error-stats` via admin socket after each suite
- Note retriable error counts (KVRGW_ERR_FDB_CONFLICT, KVRGW_ERR_FDB_PROCESS_BEHIND, etc.) — keep record but do not stop on them
- Stop immediately on any non-retriable error count > 0 or any test failure

### After final step — full performance suite

Start FDB metrics collection in a separate terminal:

```bash
mkdir -p perf-results/error-codes-refactor/metrics
scripts/collect_fdb_metrics.sh perf-results/error-codes-refactor/metrics 2
```

Run the perf driver:

```bash
FDB_CLUSTER_FILE=.fdb/fdb.cluster \
  KVRGW_MAX_KV_STORE=4096 \
  KVRGW_MAX_INLINE=256 \
  ./build/kv-rgw-backend --perf /tmp/kvrgw-perf.sock data
```

Execute full perf suite:

```
# --- Unversioned ---
perf> create-buckets count=4
perf> put c=128 tiers=128,4096,8192 duration=300
perf> batch-stats
perf> get c=128
perf> put-overwrite c=128 tiers=128,4096,8192
perf> copy c=128
perf> delete c=128
perf> delete-buckets

# --- Versioned ---
perf> create-buckets count=4 mode=versioned
perf> put c=128 tiers=128,4096,8192 duration=300
perf> put-overwrite-versioned c=128 tiers=128,4096,8192 versions=3
perf> delete-version c=128 versions=3
perf> delete c=128
perf> delete-buckets
```

**Record:**
- `GET error-stats` — full snapshot of all error counters
- `batch-stats` — batch queue performance counters
- FDB metrics from `collect_fdb_metrics.sh` (CPU, txn/sec, durability lag)
- IOPS and latency from perf driver output
- Compare results to baseline in `thread_and_profiling.md` — no regression expected (refactor is logic-only, no data path changes)

---

## Results

### Performance (batch_size=1, c=128, clean FDB)

No regression. Full results recorded in [thread_and_profiling.md](thread_and_profiling.md#error-codes-refactor--performance-validation).

| Operation | 128B IOPS | 4KB IOPS | 8KB IOPS |
|-----------|-----------|----------|----------|
| PUT | 20,583 | 12,562 | 10,716 |
| PUT-OVERWRITE | 19,111 | 6,085 | 10,021 |
| COPY | 18,382 | 7,288 | 15,780 |
| DELETE | 26,661 | 12,137 | 23,459 |

PUT 8KB isolated (clean FDB): **10,967 IOPS**, 0 errors.

### Error stats

- `KVRGW_ERR_FDB_FUTURE_VERSION` (FDB 1009): ~0.02% rate under sustained heavy write load. All retriable — retried successfully.
- Pre-refactor bug: 1009 was misclassified as non-retriable, causing hard failures under load. Now correctly classified as `is_retriable_idempotent()`.
- All other error counters: 0 on clean workloads.
