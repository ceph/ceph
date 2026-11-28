# Architecture Overview

## Components

```
┌─────────────────────────────────────────────────────────┐
│  S3 Clients (aws cli, s3cmd, s5cmd, any S3 SDK)         │
└───────────────────────────┬─────────────────────────────┘
                            │ HTTP (S3 protocol)
                            ▼
┌─────────────────────────────────────────────────────────┐
│  HTTP Gateway (nginx, :9080)                            │
│  Round-robin upstream to N frontends                    │
└───────┬───────────┬───────────┬─────────────────────────┘
        │           │           │  HTTP
        ▼           ▼           ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Frontend 0  │ │ Frontend 1  │ │ Frontend 2  │
│ (Go, :9081) │ │ (Go, :9082) │ │ (Go, :9083) │
│ versitygw   │ │ versitygw   │ │ versitygw   │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘
       │ cgo / C ABI   │ cgo / C ABI   │ cgo / C ABI
       │ libkvrgw.so   │ libkvrgw.so   │ libkvrgw.so
       ▼               ▼               ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Backend 0   │ │ Backend 1   │ │ Backend 2   │
│ (in-process)│ │ (in-process)│ │ (in-process)│
│ C++ lib     │ │ C++ lib     │ │ C++ lib     │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘
       │               │               │
       └───────────────┼───────────────┘
                       │ FDB client (shared cluster)
                       ▼
         ┌───────────────────────┐
         │  FoundationDB         │
         │  (metadata + D: data  │
         │   + R: ref-counts     │
         │   + C: child KV)      │
         └───────────────────────┘
                       +
         ┌───────────────────────┐
         │  Local Filesystem     │
         │  (STORAGE tier blobs) │
         └───────────────────────┘
```

## Frontend (Go) — temporary

**Binary:** `build/kv-rgw-frontend`

The frontend is a thin HTTP-to-C-ABI adapter built on [versitygw](https://github.com/versity/versitygw):

- **versitygw** handles S3 XML protocol parsing, request routing, AWS Signature V4 validation, error formatting, conditional headers (If-Match/If-None-Match/If-Modified-Since), and response serialization
- **versitygw IAM** authenticates requests via SigV4; supports multiple users via file-based IAMDir (`iam/users.json`)
- **versitygw bucket policy** evaluator: before each operation, calls `be.GetBucketPolicy()` and evaluates Principal/Action/Resource against the authenticated user. Root user bypasses all policy checks.
- **KvRgwBackend** implements the versitygw `backend.Backend` interface by translating each S3 operation into C ABI calls (`kvrgw_*`) on in-process `libkvrgw.so`
- Each frontend process loads **one** backend (`KvRgwServiceImpl` in the same process)
- **ACLs disabled** (`DisableACLs: true`) — bucket policies are the sole access control mechanism

The frontend does NO metadata storage, no caching, no business logic beyond S3 protocol translation (version ID string mapping, response field formatting) and IAM/policy evaluation (handled by versitygw). It is stateless. See [frontend-migration.md](frontend-migration.md) for the full Go vs C++ split analysis.

**Tenant model:** `tenantForCtx()` always returns the configured tenant (`KVRGW_TENANT_NAME`, default `kv-poc`). All users share one bucket namespace. Multi-tenant with user-to-tenant mapping is a follow-up.

Key files:
- `frontend/backend_kvrgw.go` — KvRgwBackend implementation, version ID translation, delete marker signaling
- `frontend/main.go` — startup, cgo backend start, versitygw registration, IAMDir/DisableACLs config
- `proto/kvrgw.proto` — protobuf messages (`KvrgwErrorCode` and request/response types)
- `iam/users.json` — file-based IAM accounts (alt user for policy testing)

## Backend (C++)

**Library:** `build/libkvrgw.so` (S3). **Binary:** `build/kv-rgw-backend` (`--perf` only).

The backend is the single source of truth for metadata and data operations:

- **KvRgwServiceImpl** — S3 operation implementation (PutObject, GetObject, DeleteObject, ListObjects, CreateBucket, etc.)
- **KvStore** — FDB transaction wrapper (`kv_get`, `kv_put`, `kv_del`, `kv_range_scan`, `kv_range_clear`, `kv_async_get`/`kv_wait_get` for pipelining, `begin_transaction`, `allocate_rgw_id`, `run_transaction` template). All operations return `std::expected` (C++23) — no exceptions on the hot path.
- **DataStore** — abstract interface for blob storage (STORAGE tier). Two implementations: `FileDataStore` (local FS `data/<ref_tag_hex>`) for production, `PerfDataStore` (no-op I/O with configurable sleep) for `--perf` mode. Returns `std::error_code` — no exceptions.
- **RefTagGenerator** — unique 12-byte write-instance identifiers (`rgw_id(4B) + seq_id(8B)`)

Background threads:
- **Sweeper** — scans P: namespace; moves stale P:O entries to G:O (crash recovery for incomplete uploads)
- **GcWorker** — scans G: namespace; deletes blobs (STORAGE) or D: entries (KV_STORE)
- **AdminServer** — Unix socket for runtime config (GC policy, tier config, error stats, fault injection)
- **ErrorStats** — atomic counters per `KvrgwErrorCode`; queryable via admin socket (`get-error-stats`, `reset-error-stats`)
- **ErrInsertion** — fault injection framework; two-level cache-friendly layout (`flags_[]` hot + `FaultPayload[]` cold); controlled via admin socket (`set-error`, `clear-error`); modes: fixed, counter-based, time-based

Error model: the C ABI returns `KvrgwErrorCode`; protobuf enums name those codes. Internal functions return `KvrgwErrorCode` or `std::expected<T, KvrgwErrorCode>`. FDB errors mapped via `fdb_to_error()` using named constants in `kvrgw::fdb::` namespace.

Key files:
- `backend/src/service_impl.cpp` — all S3 operation logic
- `backend/src/error_codes.hpp/cpp` — `KvrgwErrorCode` helpers (`fdb_to_error`, `is_retriable*`, `kvrgw_strerror`), FDB named constants
- `backend/src/keys.cpp` + `key_buf.hpp` — KV key construction (zero-allocation KeyBuf)
- `backend/src/ref_tag.hpp` — `using RefTag = std::array<uint8_t, 12>`; `ref_tag_view()` helper
- `backend/src/object_value.hpp` — packed binary structs for all KV values
- `backend/src/gc_value.cpp` — G:O and P:O value handling (including group P:O / group G:O)
- `backend/src/bucket_policy.hpp/cpp` — `read_bucket_state`, `check_access`, `verify_bucket_in_txn`, `parse_policy_flags`
- `backend/src/ref_count.hpp` — R: and D: ref-count helpers (CopyObject data sharing)
- `backend/src/tag_value.hpp/cpp` — tag payload encoding/decoding (binary packed), TagSet type
- `backend/src/gc_worker.cpp` — background GC (with ref_count decrement for shared data)
- `backend/src/sweeper.cpp` — background crash recovery
- `backend/src/admin_server.cpp` — runtime config via Unix socket
- `backend/src/tier_config_state.cpp` — double-buffer tier config (online change)
- `backend/src/err_insertion.hpp/cpp` — fault injection (ErrInsertion class, FaultType enum)

## Communication

| Path | Protocol | Transport |
|------|----------|-----------|
| Client → GW | HTTP (S3) | TCP :9080 |
| GW → Frontend | HTTP | TCP :9081-908N |
| Frontend → Backend | C ABI (cgo) | in-process `libkvrgw.so` |
| Backend → FDB | FDB C API | FDB network thread (TCP to fdbserver) |
| Backend → Filesystem | POSIX (`FileDataStore`) | local FS (`KVRGW_DATA` directory); `PerfDataStore` in `--perf` mode |
| Admin tool → Backend | line protocol | Unix socket `/tmp/kvrgw-admin-{i}.sock` |

## Socket pairing

Each instance `i` (0-based) has:
- Frontend listens on HTTP `:9081+i` and runs `KvRgwServiceImpl` in-process
- Admin socket `/tmp/kvrgw-admin-{i}.sock` (`gc_ctl`)

The HTTP GW (nginx on :9080) round-robins across all frontend HTTP ports.

## Data flow: PutObject

```
Client --HTTP PUT--> GW --HTTP--> Frontend (versitygw parses S3 request)
  Frontend calls C ABI PutObject (metadata + body buffer)
  Backend: put_object_route(PutObjectRequest, data, data_len)
    Tier selection:
      if size == 0 or size <= max_inline: → INLINE (data in O: value)
      if size <= max_kv_store:            → KV_STORE (data in D: entry)
      else:                               → STORAGE (blob on filesystem)

    Routing (batch_size > 1 and no tags):
      Caller classifies tier and enqueues BatchCommitEntry → BatchCommitQueue.
      Batch worker thread handles everything:
        Phase 1: group P:O write (one FDB commit for all storage-tier entries)
        Phase 2: data_store.write() for each storage-tier entry
        Phase 3: single FDB txn with pipelined reads for all entries
      Per-batch conflict set prevents duplicate keys; conflicts pushed to back of deque.

    Routing (batch_size == 1 or has tags):
      INLINE/KV_STORE: put_object_single_txn (single FDB txn)
      STORAGE: Phase 1 (P:O blocking) → Phase 2 (blob) → Phase 3 (put_object_phase3)

  Backend returns etag
Frontend returns HTTP 200 with ETag header
```

`put_object_route()` is also called by the perf driver's `put_worker` -- same code path as production.
See [batch_mode.md](batch_mode.md) for batch queue design and configuration.

## Data flow: GetObject

```
Client --HTTP GET--> GW --HTTP--> Frontend
  Frontend calls gRPC GetObject:
    Backend reads O: from FDB
    if INLINE:    → data from O: value
    if KV_STORE:  → data from D: entry (FDB)
    if STORAGE:   → data from local filesystem blob
    Backend streams response (metadata + 64KB chunks)
  Frontend writes HTTP response with Content-Length, ETag, Last-Modified
```

## Data flow: CopyObject

```
Client --HTTP PUT (x-amz-copy-source)--> GW --HTTP--> Frontend
  Frontend parses copy-source header, calls gRPC CopyObject:
    Backend (single FDB transaction):
      Read src O: (or V:<vid>) → get metadata + chunk descriptor
      Check preconditions (if_match, if_none_match, dst conditionals)
      Data sharing (no bytes copied for D:/STORAGE):
        INLINE:      byte-copy inline data into new O: value
        CHILD_D:     increment ref_count suffix in D: entry, dst uses CHILD_D_REF
        STORAGE:     create/increment R:<ref_tag> entry, dst uses STORAGE_REF
        Tag handling:
          if !replace_metadata: copy inline tags or read/write C:T entry
          if replace_metadata: no tags (AWS REPLACE strips metadata)
      Displace old destination if overwrite (→ G:O)
      Write new dst O: entry
      Commit
    Backend returns etag + last_modified + version_ids
  Frontend returns HTTP 200 with CopyObjectResult XML
```
