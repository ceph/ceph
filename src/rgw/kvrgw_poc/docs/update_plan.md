# Documentation Update Plan

Updates needed to reflect bucket cache conformance and bucket policy features.

---

## poc_as_built.md

- **Binary data structures table**: B value is now 17B (bucket_id 8 + created_at 8 + access_flags 1) + variable-length policy JSON.
- **Supported S3 operations**: Add PutBucketPolicy, GetBucketPolicy, DeleteBucketPolicy RPCs.
- **Key schema**: Update B value description to 17B header + policy JSON.
- **PutObject Phase 1**: Now a blind `store_.set(P:O)` — no transaction, no get(B).
- **PutObject Phase 3 / single-txn**: Add `verify_bucket_in_txn(B, kDenyWrite)` inside txn.
- **GetObject**: Add `check_access(kDenyRead)` with refresh-before-reject.
- **HeadObject**: Same — `check_access(kDenyRead)`.
- **ListObjects**: Reads B fresh via `read_bucket_state()` (no cache). Continuation token embeds bucket_id. Check `kDenyList`.
- **DeleteObject / DeleteMulti**: `verify_bucket_in_txn(B, kDenyWrite)` in each txn.
- **DeleteBucket**: `check_access(kDenyDeleteBucket)` + fresh read.
- **Internal features**: Document 3-second cache TTL, `read_bucket_state()` replaces `resolve_bucket_id`.
- **Error handling**: Frontend maps `codes.PermissionDenied` → S3 AccessDenied.
- **Source modules**: Add `bucket_policy.cpp`, `run_transaction` in KvStore.

---

## architecture.md

- **Backend (C++) KvStore**: Add `run_transaction` template and `set()` (blind write) to API description.
- **Backend key files**: Mention `read_bucket_state`, `check_access`, `verify_bucket_in_txn`, `bucket_policy.hpp/cpp`.
- **Frontend (Go)**: Note PermissionDenied mapping; mention policy RPCs are gRPC-only (not routed through gofakes3).
- **Data flow PutObject**: Phase 1 is blind write; Phase 3 includes verify_bucket_in_txn.

---

## cpp-data-structures.md

- **Constants**: Add `kBucketCacheTtl = 3s`, `kDenyRead/Write/List/DeleteBucket` flag constants.
- **BucketValueHeader**: Extend to 17B — add `uint8_t access_flags` at offset 16. Variable-length policy JSON follows.
- **BucketValue (parsed)**: Add `access_flags` and `policy_json` fields.
- **BucketCacheEntry**: Add `access_flags` field.
- **New structs**: `BucketState` (returned by `read_bucket_state`), `TxnStats`, `TxnRetryPolicy`.
- **New functions**: `read_bucket_state`, `check_access`, `parse_policy_flags`, `extract_access_flags`.
- **Removed**: `resolve_bucket_id` (replaced by `read_bucket_state`).

---

## s3-operations-code.md

- **PutObject Phase 1**: Blind write — `get_bucket_id_cached` + `store_.set(P:O)`. No transaction.
- **PutObject Phase 3**: `verify_bucket_in_txn(*tr, tenant, bucket, id, kDenyWrite)` before object ops.
- **put_object_single_txn**: Same verify call replaces inline get(B) + extract.
- **PutObject entry (PutObject RPC)**: Soft `check_access(kDenyWrite)` before reading data stream.
- **GetObject**: `check_access(kDenyRead)` after cache lookup.
- **HeadObject**: Same.
- **ListObjects**: `read_bucket_state()` (fresh), check `kDenyList`, token = `base64(bucket_id + key)`.
- **DeleteObject**: `check_access(kDenyWrite)` soft + `verify_bucket_in_txn(kDenyWrite)` hard.
- **DeleteMulti**: Same pattern per chunk.
- **DeleteBucket**: `check_access(kDenyDeleteBucket)` via `read_bucket_state`.
- **BucketExists**: Now uses `read_bucket_state` (returns full state).
- **Common helpers table**: Replace `resolve_bucket_id` with `read_bucket_state`; add `check_access`, `verify_bucket_in_txn`, `parse_policy_flags`.
- **New section**: PutBucketPolicy / GetBucketPolicy / DeleteBucketPolicy operation walkthroughs.

---

## background-and-admin.md

- **Startup sequence**: Mention bucket cache uses 3-second TTL (`kBucketCacheTtl`).
- **KvStore API**: Note `run_transaction` template available (not yet used by existing callers).

No other changes — GC, sweeper, admin socket behavior unchanged.

---

## gc_admin.md

No changes needed. GC/sweeper semantics unaffected by cache conformance or policy features.

---

## TEST_PLAN.md

- **Phase summary table**: Add new phase "10 bucket policy" (or renumber).
- **New phase**: Bucket policy enforcement via `scripts/test_bucket_policy.go`:
  - Deny write: PUT/DELETE/DELETE-multi rejected, GET/HEAD/LIST pass
  - Deny list: LIST rejected, others pass
  - Deny read: GET/HEAD rejected after TTL, others pass
  - Cross-instance TTL: stale cache allows, TTL expires → deny, refresh-before-reject → allow
  - Deny DeleteBucket: DELETE bucket rejected
  - No-policy sanity: all ops pass
- **Phase 1 unit tests**: Note `BucketValueHeader` is now 17B; add policy parser test if added.
- **Not-in-scope note**: Clarify that bucket-level policy (4-mode flags) is now in scope; full AWS IAM/ACL remains out of scope.
