# PutObject and DeleteBucket protocol

## Problem

Object metadata is keyed by `bucket_id`, resolved once from `B <tenant_id> <bucket_name>`. Without coordination, DeleteBucket could race with in-flight PUT and leave orphan `:O:` entries. **`P:O` is the commit token** — Phase 3 must verify it still exists before writing `:O:`.

---

**Bucket cache:** `(tenant_id, bucket_name) → bucket_id`. Write paths `Get(B)` in Phase 1 only. Phase 3 gates on `get(P:O)`, not `Get(B)`. Invalidate cache on DeleteBucket.

---

## DeleteBucket

### Pre-scan (no transaction)

1. RangeScan `:O:` for this bucket, limit 1 → if hit, `BucketNotEmpty`.
2. RangeScan `:P:` for this bucket, limit 1001 → if count > 1000, refuse (`BucketTooActive`).

### FDB transaction

1. RangeScan `:P:` for this bucket (limit 1000) → for each `P:O`: `put(G:O)` + `delete(P:O)` via `move_po_to_go()`.
2. RangeScan `:O:` limit 1 → if hit, `BucketNotEmpty`.
3. `Get(B)` → if missing, success (idempotent).
4. Delete `B`. Commit.
5. Invalidate local bucket cache.

In-flight PUT is force-aborted in step 1. PUT Phase 3 missing-`P:O` path returns NoSuchBucket or InternalFailure.

---

## PutObject — Phase 1 FDB transaction

1. Generate `ref_tag`.
2. `Get(B)` → if missing, invalidate cache, `NoSuchBucket`.
3. `Put(P:O)` — value: `estimated_size`, `content_type_hint`, `created_at_unix`.
4. Commit.

---

## PutObject — Phase 2 (no KV)

Write blob to storage tier by `ref_tag`; compute etag.

---

## PutObject — Phase 3 FDB transaction

**If `P:O` exists** — normal commit:

1. If `:O:` exists with same ref_tag → delete `P:O`; commit (orphan cleanup).
2. Else if `:O:` exists (overwrite) → move old `:O:` to `G:O`.
3. Write `:O:`; delete `P:O`. Commit.

**If `P:O` missing** — txn retry / failure path:

1. If `:O:` has this ref_tag → success (already committed).
2. Else if `B` missing → `NoSuchBucket`.
3. Else → `InternalFailure` (sweeper or bucket-delete aborted upload).

---

## Sweeper — stale P:O

For each aged `P:O`:

- If committed `:O:` with same ref_tag → delete `P:O` only.
- Else → txn `put(G:O)` + `delete(P:O)`; GcWorker frees blob from `G:O`.

---

## Concurrent delete vs in-flight PUT

DeleteBucket pre-scans empty `:O:`, then force-aborts all `:P:` in one txn. PUT Phase 3 missing-`P:O` path reports NoSuchBucket or InternalFailure — no orphan `:O:` committed after bucket delete begins.
