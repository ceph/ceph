# Tenant-id & `:L:` namespace — full suite findings

**Run:** `./scripts/run_test_plan.sh` (2026-08-06)  
**Result:** **PASSED 13/13 phases** (~50 min)

Phases exercised tenant resolution (`T` + name → id → `B` prefix), `AddTenant`, multi-bucket stress, and FDB-direct tamper tests (HeadBucket, corrupted ETag).

---

## Bugs fixed (this session)

| Issue | Fix |
|-------|-----|
| `reload.sh` without `--clean` skipped `AddTenant` | Always call `kvrgw_add_tenant` after server start |
| `kvrgw_add_tenant` / smoke failed on existing tenant (HTTP 409) | Accept **200** or **409** |
| `test_head_bucket.sh` FDB value parse could leave trailing `\n` in JSON path | `rstrip("\n")` before `rstrip("'")` (same as etag test) |

---

## Design holes (for owner decision)

### 1. Base doc vs POC `:T:` key layout

[Listing-and-Key-Scheme.md](Listing-and-Key-Scheme.md) still describes **`[T 1B][tenant_id 4B]`** (5-byte fixed key). POC as-built uses **`T + tenant_name`** (variable key) with value `{ tenant_id 4B, created_at 8B }`.

**Risk:** Tools/docs that build `T` keys from numeric id will miss POC rows.

### 2. No migration for `:L:` counter key format (not shard migration)

This is **not** online shard resharding / re-keying of `S:O` rows. It means: when the **counter key format** changed (legacy `\x00…_counter` / ASCII `L:BUCKET:ID` → binary `L N bucket_id`), there is **no in-place FDB rename**. POC relies on **`reload.sh --clean`** to wipe metadata. Restart without clean leaves orphan old counter keys and fresh `L N *` cells at 1 — a **metadata consistency** concern, not a listing shard migration issue.

### 3. Bucket counter width

`L N bucket_id` is uint64; allocated **bucket_id** is 8-byte BE uint64. **64-bit space is sufficient** — overflow is not a practical concern (removed from open issues).

### 4. `L I` (string→id maps) not implemented

Documented for M2.5 only. No runtime validation that stray `L I` keys are rejected on write paths (they are inert until something reads them).

### 5. `:L:` FDB assertions

**Added:** `scripts/test_l_namespace_fdb.sh` (test plan phase 6) — verifies `L N tenant_id`, `L N bucket_id` (before/after mb), `L N rgw_id` in FDB.

### 6. `reload.sh` without `--clean` after code deploy

User workflow `./scripts/reload.sh` (no clean) now re-adds tenant (409 ok) but **does not reset `L N` counters**. Safe if FDB state matches code; unsafe if counters were wiped independently of `T`/`B` rows.

### 7. Resolved: counter type byte

Renamed **`L` type `C` → `N`** (numeric counter) to avoid confusion with `S`/`Z` category `C` (child entries). **`I`** unchanged for id maps.

---

## Verified OK (no action)

- Tenant resolution chain in tests: `T+name → tenant_id (4B) → B key → bucket_id (8B) → S:O key`
- `AddTenant` txn: `atomic_add(L N tenant_id)` + `Put(T+name)`
- `CreateBucket` txn: `atomic_add(L N bucket_id)` + `Put(B)`
- Boot: `atomic_add(L N rgw_id)` logged as `rgw_id=N` in reload report
- Corrupted ETag tests (KV etag + blob tamper) pass with current key layout
- 1K bucket / 64K object stress with `tenant_name` on all gRPC paths

---

## Suggested follow-ups (optional)

1. POC delta banner on Listing-and-Key-Scheme `T` section (name-keyed key).
2. FDB inspection test: after `AddTenant` + `CreateBucket`, `get` on `L N tenant_id` and `L N bucket_id`.
3. Migration policy doc: when `--clean` is required vs in-place upgrade.
