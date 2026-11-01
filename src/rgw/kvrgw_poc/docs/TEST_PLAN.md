# KV-RGW Test Plan

**As-built reference:** [poc_as_built.md](poc_as_built.md)

## Rules

1. **100% pass required.** Any failure means the run failed. Do not assume code works until every step passes.
2. **Report every failure.** The runner executes all phases and prints a failure summary at the end. No silent skips.
3. **Listing invariants (no hard-coded key names):**
   - **Order:** `KV(n) < KV(n+1)` for every consecutive pair in a list result.
   - **Count:** `list_count == upload_count` (full-list tests); stress polls allow `list_count <= upload_count` until complete.
4. **Count mismatch = bug.** If `list_count ≠ upload_count` on a finished upload, stop investigation there.
5. **No speculation.** Without evidence, assume bad code.
6. **Multiple clients.** Full-list tests verify via **both** `aws s3api` and `s3cmd ls` (s3cmd uses ListObjects v1 + `delimiter=/`).

Shared verifier: [`scripts/list_verify.py`](scripts/list_verify.py)

| Subcommand | Use |
|------------|-----|
| `aws` | Full object list via `list-objects-v2`; count + strict key order |
| `s3cmd` | Full object list via `s3cmd ls`; count + strict key order |
| `aws-buckets` | Full bucket list via `list-buckets`; count + strict name order + optional CreationDate |
| `pages` | Verify keys collected in paginated `page-*.json` files |

## How to run

```bash
./scripts/run_test_plan.sh           # full plan (same as --slow): phases 0–9 (~45 min)
./scripts/run_test_plan.sh --fast    # build, unit, reload + fast integration tests (~1 min)
./scripts/run_test_plan.sh --quick   # --fast + smoke (~2 min)
./scripts/run_test_plan.sh --medium  # --fast + smoke + byte-range + list boundary (~4 min)
./scripts/run_test_plan.sh --slow    # --medium + 64K/1K stress + gc delete queue (~45 min)
./scripts/run_test_plan.sh --chaos   # --slow + chaos tests (requires N=3, ~1 hour)
./scripts/reload.sh [--clean] [N]    # N instances (default from .run/instances); GW :9080
./scripts/reload.sh --clean 3       # 3 instances + shared FDB/data
./scripts/install_lb.sh              # nginx for GW
./scripts/start_multi.sh [N]         # start N pairs + GW (no rebuild)
./scripts/test_multi_rgw_rr.sh       # RR GET test (runs when KVRGW_INSTANCES>1)
./scripts/smoke_test.sh             # reload --clean 1 + M1/M2 checks via GW
./scripts/smoke_test.sh --skip-reload # after reload phase in test plan
./scripts/test_head_bucket.sh
./scripts/test_l_namespace_fdb.sh
./scripts/test_corrupted_etag.sh
./scripts/test_byte_range_get.sh
./scripts/test_list_pagination.sh
./scripts/test_list_stress.sh       # list-objects stress (--skip-reload after reload)
./scripts/test_list_buckets_stress.sh
./scripts/test_gc_admin.sh
./scripts/test_gc_delete_queue.sh
./scripts/test_bucket_policy.go              # bucket policy enforcement (6 scenarios)
./scripts/test_kill_single_instance.sh      # N>=3: kill one instance, run integration suite
./scripts/test_power_cycle_upload_easy.sh   # N=3: 64K upload, restart, verify, 2nd bucket
./scripts/test_power_cycle_upload_hard.sh   # N=3: kill during aws list page2; s5cmd ls; verify both
./scripts/verify_bucket_all.sh BUCKET       # aws list + s5cmd ls + s5cmd sync + diff vs /tmp/dedup
```

Exit code 0 only if **all** phases pass. Exit code 1 with a printed failure list otherwise.

Phases **11–12** use `--skip-reload` (servers from earlier phases must stay up). Phase **3** (`smoke_test.sh`) performs its own clean FDB wipe when run standalone.

## Fast vs slow tests

| Tier | Phase / script | Typical duration | Notes |
|------|----------------|------------------|-------|
| **Fast** | 0 build | ~10 s | cmake + go build |
| **Fast** | 1 unit tests | ~5 s | `object_value_test`, `l_key_test`, `data_store_test`, `gc_config_state_test`, `kv_range_test` (+ live FDB error injection), `fdb_error_test` (32 mock tests), `data_store_error_test` (9 FS error tests) |
| **Fast** | reload fast suite | ~20 s | head-bucket, corrupted-etag, L namespace FDB, gc admin — **every `reload.sh`** |
| **Fast** | 4 head bucket | ~5 s | (in reload fast suite) |
| **Fast** | 5 corrupted etag | ~5 s | (in reload fast suite) |
| **Fast** | 6 L namespace FDB | ~3 s | (in reload fast suite) |
| **Fast** | 12 gc admin API | ~10 s | (in reload fast suite) |
| **Medium** | 2 reload (clean) | ~30–60 s | rebuild, start, S3 verify |
| **Medium** | 3 smoke | ~30 s | CRUD + GC poll |
| **Medium** | 6 byte range GET | ~30 s | 4 MiB object, 10 ranges |
| **Medium** | 7 list boundary | ~1 min | 1024 keys, aws + s3cmd |
| **Slow** | 8 pagination | ~5–10 min | 64K keys, paginated list |
| **Slow** | 9 list stress | ~10–15 min | 64K upload + concurrent listing |
| **Slow** | 10 list buckets stress | ~5–10 min | 1K buckets × 10 objects |
| **Slow** | 12 gc delete queue | ~15–20 min | 1000 objects, G:O verification |
| **Slow** | 10 bucket policy | ~5 min | 6 policy enforcement scenarios |

**Tier flags** (cumulative; default = `--slow`):

| Flag | Phases | ~Duration |
|------|--------|-----------|
| `--fast` | 0–2 | ~1 min |
| `--quick` | 0–3 | ~2 min |
| `--medium` | 0–5 | ~4 min |
| `--slow` / default | 0–10 | ~45 min |
| `--chaos` | 0–14 | ~1 hour (N=3) |

**Chaos tier** = `--slow` + kill-instance + power-cycle easy + power-cycle hard + white-box fault injection. Requires `./scripts/reload.sh --clean 3` first (registers N in `.run/instances`). Bucket policy phase runs in `--slow`.

**Quick plan** = `--quick`. **Full plan** = `--slow` (default).

## Phase summary

| Phase | Name | Runner |
|-------|------|--------|
| 0 | Build | `run_test_plan.sh` |
| 1 | Unit tests | `run_test_plan.sh` |
| 2 | Reload (clean + fast tests) | `reload.sh --clean` |
| 3 | Smoke | `smoke_test.sh` |
| 4 | Byte-range GET | `test_byte_range_get.sh` |
| 5 | List boundary (1024) | inline in `run_test_plan.sh` |
| 6 | Pagination (64K) | `test_list_pagination.sh` |
| 7 | List stress (64K upload) | `test_list_stress.sh --skip-reload` |
| 8 | ListBuckets stress (1K) | `test_list_buckets_stress.sh --skip-reload` |
| 9 | GC delete queue | `test_gc_delete_queue.sh` |
| 10 | Bucket policy | `test_bucket_policy.go` |
| 11 | Kill instance (N=3) | `test_kill_single_instance.sh` |
| 12 | Power-cycle easy | `test_power_cycle_upload_easy.sh` |
| 13 | Power-cycle hard | `test_power_cycle_upload_hard.sh` |
| 14 | White-box (fault injection) | `test_white_box.sh` |

Phases **11–14** run only with `--chaos` and require **`KVRGW_INSTANCES=3`** (phase 14 also works with N=1). Phase 11 leaves a degraded cluster; phases 12–13 call `reload.sh --clean 3` first if servers are not healthy.

**Fast tests** (head-bucket, corrupted-etag, L namespace FDB, gc admin) run inside **every** `reload.sh`, not as separate plan phases.

**Corrupted-etag test** (`test_corrupted_etag.sh`): Uses `gc_ctl raw-get`/`raw-set` to read/modify/write binary O: values via the FDB C API. Variant A corrupts etag byte at offset 16 in the O: value; variant B corrupts byte 0 of the blob file. Both verify the backend serves data but reports an MD5 mismatch to the client.

---

## Phase 0 — Build

| Step | Command | Pass criteria |
|------|---------|---------------|
| 0.1 | `cmake --build build -j$(nproc)` | Exit 0 |
| 0.2 | `go build -o build/kv-rgw-frontend ./frontend` | Exit 0 |

**On failure:** print compiler/linker output; do not run later phases.

---

## Phase 1 — Unit tests

| Step | Command | Pass criteria |
|------|---------|---------------|
| 1.1 | `./build/object_value_test` | Prints `passed`, exit 0 (includes `BucketValueHeader` 17B layout + policy parser test) |
| 1.2 | `./build/data_store_test` | Prints `passed`, exit 0 |
| 1.3 | `./build/kv_range_test` | Prints `passed`, exit 0 (requires FDB); includes live FDB error injection (conflict 1020, transaction_too_old 1007, key_too_large 2102) |
| 1.4 | `./build/gc_config_state_test` | Prints `passed`, exit 0 |
| 1.5 | `./build/fdb_error_test` | 32 tests, exit 0; link-time mock of FDB C API; verifies `std::expected` error-code propagation through fdb_blocking → kv_store → begin_transaction/get/commit/range_scan/allocate_rgw_id |
| 1.6 | `./build/data_store_error_test` | 9 tests, exit 0; real filesystem error injection (missing file, read-only dir, offset past end, idempotent remove) |

**On failure:** report test name and stderr; fix before integration tests.

---

## Phase 2 — Server startup

| Step | Command | Pass criteria |
|------|---------|---------------|
| 2.1 | `./scripts/reload.sh --clean` | Exit 0; FDB/backend/frontend up; S3 check ok |

`reload.sh --clean` also runs backend unit tests during `kvrgw_build()`. On success, `kvrgw_verify()` exercises:

- `s3cmd` path: mb, put, ls, get, info (`?acl`), del, rb
- **ListBuckets mini-test** (`kvrgw_test_list_buckets`): create 4 buckets, `aws s3api list-buckets` (CreationDate), `s3cmd ls`

**On failure:** report `.logs/backend.log` and `.logs/frontend.log` tail.

---

## Phase 3 — Smoke (M1 + M2)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 3.1 | `./scripts/smoke_test.sh` | Exit 0; PUT/GET/HEAD, Range GET, ls, rm, overwrite, rb, ListBuckets |

**On failure:** report which milestone step failed.

---

## Phase 4 — HeadBucket

| Step | Command | Pass criteria |
|------|---------|---------------|
| 4.1 | `./scripts/test_head_bucket.sh` | Exit 0 |

Steps:

1. `head-bucket` on non-existent bucket → **404**
2. `s3 mb`; `head-bucket` → **200**
2.5. Read FDB **`T` + tenant_name** (`KVRGW_TENANT_NAME`, default `kv-poc`); parse `tenant_id` from value
3. `s3 rb`; `head-bucket` → **404**
4. `s3 mb` (warms cache); **FDB `clear` on `B` key** (simulate other RGW); `head-bucket` → **404**

Step 4 requires `BucketExists` to always `Get(B)` in FDB and refresh/invalidate cache (not serve stale cache hits).

---

## Phase 5 — Byte-range GET (4 MiB uint32 object)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 5.1 | `./scripts/test_byte_range_get.sh` | Exit 0 |

Object layout: 4 MiB = 1,048,576 little-endian `uint32` values `0 .. 1_048_575`. Ten random `(offset, read_size)` pairs. Verification splits each response into: first partial word (1–3 B), middle full `uint32` compare, last partial word (1–3 B). Backend uses `DataStore::read(ref_tag, offset, length)` — does not load the full blob for range requests.

---

## Phase 6 — List boundary (1024 keys, page-size 1000)

Catches off-by-one continuation token bugs at page boundaries.

| Step | Action | Pass criteria |
|------|--------|---------------|
| 6.1 | Create 1024 files under `/tmp/kv-test-2k-*` | `upload_count == 1024` |
| 6.2 | `s5cmd sync` → test bucket | Upload exit 0 |
| 6.3 | `list_verify.py aws` | Count = upload_count; strict order |
| 6.4 | `list_verify.py s3cmd` | Count = upload_count; strict order |
| 6.5 | Delete bucket | Cleanup ok |

**On failure:** print count and first ordering violation from `list_verify.py`.

---

## Phase 7 — Pagination scale (64K + 1K)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 7.1 | `./scripts/test_list_pagination.sh` | Exit 0 |

Includes:

- bucket1: 65536 objects, paginated aws (max-keys 500/1000; gofakes3 caps HTTP max-keys at 1000)
- bucket2: 1000 objects with `other-` prefix
- Count = upload count; strict key order; aws + s3cmd full listings

**On failure:** report bucket, client, expected vs actual count.

---

## Phase 8 — List stress (64K parallel upload + concurrent listing)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 8.1 | `./scripts/test_list_stress.sh` | Exit 0 |

- `s5cmd sync /tmp/dedup/ s3://bucket1/` runs in parallel with repeated full listings
- Each poll: strict order; `list_count <= 65536`
- Stop when `list_count == 65536`; backend/frontend must stay alive (no crash/segfault)

**On failure:** report poll count, last list count, `.logs/backend.log` tail.

---

## Phase 9 — ListBuckets stress (1K buckets + concurrent 64K upload)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 9.1 | `./scripts/test_list_buckets_stress.sh` | Exit 0 |

- Create **1000** buckets (`lb-stress-0001` … `lb-stress-1000`); `list_verify.py aws-buckets` — exact count, strict name order, CreationDate
- Put **10** objects into each bucket; repeat ListBuckets — same 1000 buckets
- `s5cmd sync /tmp/dedup/ s3://bucket1/` (65536 objects) runs in parallel with repeated ListBuckets polls
- Each poll: same 1000 buckets; backend/frontend must stay alive

Env: `KVRGW_LB_BUCKET_COUNT`, `KVRGW_LB_OBJECTS_PER_BUCKET`, `KVRGW_LB_PREFIX`, `KVRGW_LB_PARALLEL`.

**On failure:** report poll count and s5cmd sync exit status.

---

## Phase 10 — GC admin API

| Step | Command | Pass criteria |
|------|---------|---------------|
| 10.1 | `./scripts/test_gc_admin.sh` | Exit 0 |

- Admin unix socket: `SET` / `QUERY` / `GET`, `wait-applied`, `get-gc-config` field verify
- Suspend/resume GC; parallel config sessions (parallel section uses `MAX_GAP=2`; sequential waits use `MAX_GAP=1`)

See [`gc_admin.md`](gc_admin.md).

---

## Phase 11 — GC delete queue (suspend + gc_ctl)

| Step | Command | Pass criteria |
|------|---------|---------------|
| 11.1 | `./scripts/test_gc_delete_queue.sh` | Exit 0 |

- Suspend GC via admin; upload **1000** objects (100B–8MiB, log-uniform sizes)
- Delete in random batches of **1–256** keys (`DeleteMulti` or single `s3 rm` when batch=1)
- Only **STORAGE tier** objects (> 8KB) create G:O entries; INLINE and KV_STORE objects are cleaned in the delete transaction
- After **each** batch: `gc_ctl count`, full `list`, `count-by-tier`, `list-by-size` match deleted STORAGE-tier set
- Every `:G:O` entry: `size_tier` matches `tier_from_size(blob_bytes)`; unique ref_tags; GET fails on deleted keys
- `gc_ctl` reads `object_size` from binary G:O value; cross-checks against blob file for STORAGE entries

Env: `KVRGW_GC_DELETE_SEED`, `KVRGW_GC_DELETE_COUNT` (default 1000).

**Tools:** `gc_ctl raw-get <key-hex>` / `gc_ctl raw-set <key-hex>` for binary KV manipulation in tests (FDB C API, no fdbcli text parsing).

---

## Phase 10 — Bucket policy enforcement

| Step | Command | Pass criteria |
|------|---------|---------------|
| 10.1 | `go run scripts/test_bucket_policy.go` (needs N≥2; hits `:9081` and `:9082`) | Exit 0 |

Scenarios tested:

- **Deny write:** PutBucketPolicy with kDenyWrite → PUT/DELETE/DELETE-multi rejected (AccessDenied), GET/HEAD/LIST pass
- **Deny list:** kDenyList → LIST rejected, other ops pass
- **Deny read:** kDenyRead → GET/HEAD rejected after TTL expiry, other ops pass
- **Cross-instance TTL:** stale cache allows op; TTL expires → deny; refresh-before-reject → allow after DeleteBucketPolicy
- **Deny DeleteBucket:** kDenyDeleteBucket → DELETE bucket rejected
- **No-policy sanity:** all ops pass when no policy is set

---

## Phase 13 — Post-run report

The runner prints:

```
TEST PLAN SUMMARY
=================
PASS: 0 build
PASS: 1 unit tests
FAIL: 6 list boundary (1024) — FAIL: count 1023 != expected 1024
...
Result: FAILED (11 passed, 1 failed)
```

Full plan: **10 phases (0–9)** plus summary. `--quick` runs phases **0–3**.

---

## Prerequisites

- FDB running (`./scripts/start_fdb.sh`)
- Tools: `aws`, `s5cmd`, `s3cmd`, `python3`, `curl`
- Config: `s3cmd.cfg` in repo root

## Ceph-RGW S3 Compatibility Tests

**204 tests passing** from the ceph/s3-tests suite (32 deselected: ACL-dependent, RGW-specific headers, bucket ownership controls, advanced bucket-policy conditions, IAM-user-dependent) (`~/clean/ceph/src/test/rgw/s3-tests`). Covers: bucket CRUD, object CRUD, listing (v1/v2, prefix, delimiter, pagination, special chars), byte-range, versioning (enable/suspend, versions, delete markers, promotion, multi-delete), conditional GET/PUT/DELETE/COPY, CopyObject (same/cross-bucket, versioned, canned-acl), atomic writes, bucket naming validation, concurrent versioned operations.

```bash
./scripts/run_ceph_rgw_tests.sh
```

Full test list and run command: [ceph_rgw_tests.md](ceph_rgw_tests.md)

**Requires:** System running on `:9080` (any tier reload). Uses `s3tests.conf` for credentials (root + alt user via IAMDir).

---

## Phase 14 — White-box (fault injection)

|| Step | Command | Pass criteria |
||------|---------|---------------|
|| 14.1 | `./scripts/test_white_box.sh` | Exit 0 |

Fault injection via admin socket → PUT fails after Phase 2 → verifies orphan blob exists on disk → sweeper moves stale P:O to G:O → GcWorker removes blob and deletes G:O key. Uses `set-error` / `clear-error` admin commands to activate/deactivate `kAbortAfterSinglePhase2` and `kAbortAfterBatchPhase2` fault types.

---

## Not in scope (POC)

- Full AWS conformance suite
- Multipart upload
- CopyObject
- Object tagging, encryption, object lock, lifecycle
- Sweeper crash / DataStore filter driver test (design instrumentation exists; no automated test script yet)
- Full AWS IAM / ACL — bucket-level policy (4-mode access_flags) is **in scope**; per-user/per-object ACL and full IAM policy language remain out of scope
