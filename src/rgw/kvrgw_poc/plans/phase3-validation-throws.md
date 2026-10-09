# Plan: Phase 3 — Remove remaining throws

## Inventory

31 throw sites remain across 4 groups:

### Group A: Validation on hot path (8 throws)

Data read from FDB is corrupt or programmer passed wrong-size input.

| # | File | Line | Throw | Trigger |
|---|------|------|-------|---------|
| A1 | service_impl.cpp | 326 | "invalid object key for move-to-G" | `parse_object_key` returned nullopt — corrupt key |
| A2 | service_impl.cpp | 366 | "corrupt object value" | `parse_object_value` returned nullopt — corrupt value |
| A3 | object_value.cpp | 84 | "bucket_id must be 8 bytes" | caller passed wrong-size bucket_id |
| A4 | keys.cpp | 72 | "invalid L namespace type" | caller passed bad type char |
| A5 | keys.cpp | 75 | "invalid L namespace name length" | caller passed empty or >64 name |
| A6 | keys.cpp | 316 | "invalid bucket value" | bucket value from FDB < 8 bytes |
| A7 | ref_tag.cpp | 31,43,59,73 | ref_tag size/hex errors | caller passed wrong-size ref_tag |
| A8 | tenant_value.cpp | 24,33 | "tenant value too short" | truncated value from FDB |

### Group B: Admin server startup (4 throws)

| # | File | Line | Throw |
|---|------|------|-------|
| B1 | admin_server.cpp | 180 | "admin socket failed" |
| B2 | admin_server.cpp | 187 | "admin socket path too long" |
| B3 | admin_server.cpp | 193 | "admin bind failed" |
| B4 | admin_server.cpp | 197 | "admin listen failed" |

Currently caught by `main.cpp:152` → "fatal:" → exit.

### Group C: gc_ctl CLI (15 throws)

Arg parsing, admin socket, response parsing. All caught by `gc_ctl.cpp:557,651` → "gc_ctl failed:" → exit.

### Group D: FDB setup (1 throw)

| # | File | Line | Throw |
|---|------|------|-------|
| D1 | fdb_blocking.cpp | 34 | `check_error()` for `fdb_create_database` |

Called from `KvStore()` constructor. Caught by `main.cpp:152`.

---

## Proposed approach per group

### Group A — hot path validation

Split into two sub-categories:

**A-data (A1, A2, A6, A8): corrupt data from FDB**

These fire when FDB returns data that doesn't parse. Convert to return-value errors:
- `move_object_to_g` (A1): already called inside txn helpers that return `grpc::Status` or `expected`. Change to return `bool` — false means "corrupt key, skip". Callers log and return INTERNAL.
- `delete_object_in_txn` (A2): already returns `expected<void, fdb_error_t>`. Change to return INTERNAL via `grpc::Status` or a sentinel error. Cannot use `fdb_error_t` — not an FDB error.
- `extract_bucket_id` (A6): change to return `std::optional<std::string>`. Callers already check bucket values before calling, but add a check.
- `tenant_value.cpp` (A8): `read_uint32_be`/`read_int64_be` are only called from `parse_tenant_value` which already size-checks. These throws are unreachable. Convert to assert or return 0.

**A-prog (A3, A4, A5, A7): programmer error**

These validate inputs that are always controlled by our own code. They should never fire in a correct build. Convert to `assert()` — crash immediately on bug, no exception overhead. Same severity as throw but cheaper and clearer intent.

### Group B — admin server startup

Change `AdminServer::run()` to return `bool` (false = startup failed). `main.cpp` checks the return and logs/exits. No exceptions needed.

### Group C — gc_ctl CLI

Keep as-is. This is a short-lived CLI tool, not the backend hot path. Throw + catch at main is standard C++ CLI error handling. Converting to return codes adds verbosity for no benefit.

### Group D — FDB setup

Change `check_error` to a non-throwing helper:
```cpp
std::optional<std::string> error_string(fdb_error_t code);
```

Change `KvStore` constructor to a factory:
```cpp
static std::expected<KvStore, fdb_error_t> KvStore::create();
```

`main.cpp` checks the result and exits on error.

---

## Design decisions

1. **A1 (`move_object_to_g` corrupt key):** log to stderr, return `bool` (false = corrupt entry, fenced). Caller returns `grpc::INTERNAL`. Server stays up.
2. **A2 (`delete_object_in_txn` corrupt value):** defer — requires error-type redesign. Keep throw for now.
3. **A3–A8 (programmer error + unreachable):** convert to `assert()`.
4. **Group C (gc_ctl):** keep as-is — standard CLI pattern, not hot path.

---

## Files touched

| File | Changes |
|------|---------|
| `service_impl.cpp` | A1, A2 — return errors instead of throw |
| `object_value.cpp` | A3 — assert |
| `keys.cpp` | A4, A5 — assert; A6 — return optional |
| `ref_tag.cpp` | A7 — assert |
| `tenant_value.cpp` | A8 — assert (unreachable) |
| `admin_server.hpp/cpp` | B — run() returns bool |
| `main.cpp` | B, D — check returns |
| `fdb_blocking.hpp/cpp` | D — remove check_error, add factory |
| `kv_store.hpp/cpp` | D — KvStore::create() factory |

Not touched: `gc_ctl.cpp` (keep as-is).

## Verify

- Build
- Run unit tests (all 7 including fdb_error_test, data_store_error_test)
- `grep -r 'throw std::' backend/src/` → only gc_ctl and (if kept) gc_ctl
- Run --slow (10/10)
- Run --chaos (14/14)
