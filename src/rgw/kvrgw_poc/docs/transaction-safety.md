# Transaction Safety — All S3 Operations

This document lists every KV transaction in the S3 operations layer, shows the base transaction (FDB — fully safe via read-write conflict detection), and documents TiKV-specific modifications and open issues.

---

## Notation

- `get(K)` — read key K
- `put(K, V)` — write key K with value V
- `delete(K)` — remove key K
- `RangeScan(prefix)` — read all keys matching prefix
- All operations within `txn { ... commit }` are atomic

---

## Base Transactions (FDB)

All transactions below are safe on FDB. Read conflict ranges detect concurrent writes to any key read within the transaction; write conflict ranges detect concurrent reads of any key written.

### PUT Phase 1 — Record intent

```
txn {
  get(B) → if null or delete-pending → NoSuchBucket
  put(P:O, {ref_tag, object_name, estimated_size, created_at})
  commit
}
```

### PUT Phase 3 — Commit metadata

```
txn {
  get(P:O) → if null → abort
  get(O:) → read existing entry (null for new objects)
  If overwrite (O: exists, different ref_tag): put(G:O, stripped_old)
  put(O:, new_value)
  delete(P:O)
  commit
}
```

### DELETE (non-versioned)

```
txn {
  get(O:) → if null → success (idempotent)
  put(G:O, {ref_tag, chunk_pointers})
  delete(O:)
  commit
}
```

### DELETE without version-id (versioned — create delete marker)

```
txn {
  get(O:) → read current version_id
  if O: exists: put(V:<current_vid>, old_value)
  put(O:, fenced_delete_marker)
  commit
}
```

### DELETE with version-id — Case 1 (target in `:V:`)

```
txn {
  get(O:) → verify target ≠ current
  get(V:<target_vid>) → read entry
  put(G:V, {ref_tag, ...}) [if target has data]
  delete(V:<target_vid>)
  commit
}
```

### DELETE with version-id — Case 2 (target is current — promotion)

```
txn {
  get(O:) → verify target = current
  RangeScan(V:<object_name>, limit=1) → find latest version to promote
  put(O:, promoted_value) [or delete O: if no V: exists]
  delete(V:<promoted_vid>)
  put(G:O, {old_ref_tag, ...}) [if old O: had data]
  commit
}
```

### Delete All Versions (vendor extension)

```
txn {
  get(O:) → read ref_tag
  put(G:F, {ref_tag, object_name})
  delete(O:)
  commit
}
```

### PutObjectTagging

```
txn {
  get(O:) → read current value
  put(O:, updated_value) [inline tags]
  — or —
  put(O:, mark_external) + put(C:<ref_tag>T, tag_data) [external tags]
  commit
}
```

### PutObjectAcl

```
txn {
  get(O:) → read current value
  put(O:, updated_acl)
  commit
}
```

### PutObjectAnnotation

Phase 3 (with storage-tier data):
```
txn {
  get(P:A) → if null → abort
  get(O:) → if null → abort; verify ref_tag match; read annotation_count
  get(C:<ref_tag>A<key>) → check if overwrite; if old has data: put(G:A, old_data)
  put(C:<ref_tag>A<key>, new_value)
  delete(P:A)
  put(O:, annotation_count + 1)  // or same count if overwrite
  commit
}
```

### DeleteObjectAnnotation

```
txn {
  get(O:) → if null → abort; read ref_tag, annotation_count
  get(C:<ref_tag>A<key>) → if null → success (idempotent)
  If old annotation has storage data: put(G:A, {anno_ref_tag, chunk_pointers})
  delete(C:<ref_tag>A<key>)
  put(O:, annotation_count - 1)
  commit
}
```

### InitiateMultipartUpload

```
txn {
  get(B) → if null or delete-pending → NoSuchBucket
  put(M:<object_name><ref_tag><0>, head_metadata)
  put(P:M, {ref_tag, object_name, upload_metadata})
  commit
}
```

### UploadPart Phase 1 — Register intent

```
txn {
  get(M:<object_name><ref_tag><0>) → verify head exists
  get(M:<object_name><ref_tag><part_number>) → check for re-upload
  put(M:<object_name><ref_tag><part_number>, {state=pending, ...})
  commit
}
```

### UploadPart Phase 3 — Commit part (blind write)

```
put(M:<object_name><ref_tag><part_number>, {state=committed, etag, size, ...})
```

No transaction — single blind write.

### CompleteMultipartUpload Phase 1 — Fence upload

```
txn {
  get(M:<object_name><ref_tag><0>) → read head, verify exists
  put(P:M, {head_metadata, client_part_list, completion_marker})
  delete(M:<object_name><ref_tag><0>)
  commit
}
```

### CompleteMultipartUpload Phase 3 — Commit final object

```
txn {
  get(O:) [if overwrite: read old value]
  put(O:, final_manifest)
  put(G:M, {ref_tag, part_list, MIXED})
  delete(P:M)
  put(G:O, old_value) [if overwrite]
  commit
}
```

### AbortMultipartUpload

```
txn {
  get(M:<object_name><ref_tag><0>) → verify head exists
  put(G:M, {ref_tag, object_name, DEEP})
  delete(M:<object_name><ref_tag><0>)
  delete(P:M)
  commit
}
```

### DeleteBucket

```
txn {
  RangeScan(:P:, limit 1000) → collect P:O entries
  For each P:O: put(G:O, {...}) + delete(P:O)
  RangeScan(:M: part_number=0, limit 1000) → collect heads
  Policy check: abort or delete M: heads + put(G:M, DEEP)
  RangeScan(:O:, limit 1) → if hit → BucketNotEmpty
  get(B) → if null → success
  delete(B)
  commit
}
```

### Sweeper (P:O / P:A)

```
txn {
  get(P:O) → if null → skip
  put(G:O, {ref_tag, bucket_id, object_name})
  delete(P:O)
  commit
}
```

---

## TiKV Safety Classification

TiKV uses snapshot isolation with **write-write conflict detection only**. Read-write conflicts are NOT detected. RangeScans do NOT create conflict ranges.

### Safe on TiKV as-is

These transactions serialize via write-write conflicts on shared keys (`:O:`, `P:O`, `:M:` head, `B`):

| Transaction | Conflict key | Concurrent operations detected |
|---|---|---|
| PUT Phase 3 vs Sweeper | `P:O` (both delete) | ✓ |
| PUT Phase 3 vs PUT Phase 3 (same object) | `:O:` (both write) | ✓ |
| PUT Phase 3 vs DELETE (same object) | `:O:` (both write) | ✓ |
| PUT Phase 3 vs CompleteMultipartUpload Phase 3 | `:O:` (both write) | ✓ |
| DELETE-without-version-id vs any `:O:` writer | `:O:` (all write) | ✓ |
| DELETE Case 2 vs any `:O:` writer | `:O:` (all write) | ✓ |
| Delete-All vs any `:O:` writer | `:O:` (all write) | ✓ |
| Case 1 vs Case 1 (same `:V:` target) | `:V:<target>` (both write) | ✓ |
| Case 1 vs Case 2 (same `:V:` key) | `:V:<key>` (both write) | ✓ |
| Complete Phase 1 vs Abort (same upload) | `:M:` head (both delete) | ✓ |
| Complete Phase 1 vs Complete Phase 1 | `:M:` head (both delete) | ✓ |
| PutObjectTagging (inline) / PutObjectAcl | `:O:` (both write) | ✓ |
| Sweeper vs Sweeper (same P:O) | `P:O` (both delete) | ✓ |
| DeleteBucket vs PUT Phase 3 | `P:O` (both delete) | ✓ |
| CreateBucket vs CreateBucket (same name) | `B` (both write) | ✓ |

### Modified for TiKV — functional writes and `last_active`

Some transactions modify derived KVs (`:V:`, `:C:`, `:M:` parts) without natively writing the parent (`:O:` or `:M:` head). On TiKV, a concurrent operation writing the parent goes undetected. The fix depends on the operation type:

**Functional count fields (preferred — same code on FDB and TiKV):**

Annotations and tags maintain count fields in O: that serve both a data purpose and provide conflict detection:

| Transaction | Derived key written | O: field written | Conflict via |
|---|---|---|---|
| PutObjectAnnotation (new) | `:C:A` | `annotation_count + 1` | Write-write on O: |
| PutObjectAnnotation (overwrite) | `:C:A` | `annotation_count` (unchanged) | Write-write on O: |
| DeleteObjectAnnotation | delete `:C:A` | `annotation_count - 1` | Write-write on O: |
| PutObjectTagging (all cases) | `:C:T` (if external) | `tag_count` | Write-write on O: |
| DeleteObjectTagging | delete `:C:T` | `tag_count = 0` | Write-write on O: |

These use identical transactions on FDB and TiKV — no backend-specific code. See [child-kv-operations.md](child-kv-operations.md).

**`last_active` field (TiKV only — for operations with no functional O: write):**

| Transaction | Derived key written | Parent `last_active` updated |
|---|---|---|
| DELETE with version-id Case 1 | `:V:<target>` deleted | `:O:` `last_active` |
| UploadPart Phase 1 | `:M:<part>` written | `:M:` head `last_active` (TBD — see open issues) |

The `last_active` field:
- 32-bit unsigned integer, delta from `mtime` in 100ms ticks (~13.6 years range)
- Updated on TiKV only — FDB relies on read conflict ranges
- Creates write-write conflict with concurrent parent writers
- Also useful for observability: detects stale/inactive entries

**Note on UploadPart:** UploadPart Phase 1 reads the `:M:` head but writes only a part entry. On TiKV, Complete/Abort deleting the head goes undetected. Adding `last_active` to the head would provide conflict detection BUT serializes all parallel part uploads (write-write on head for every part). This trade-off is under consideration — currently listed as an open issue (#3 below) with sweeper cleanup as the mitigation.

### Unsafe on TiKV — open issues

These transactions involve shared-key readers that cannot afford a touch-write (would serialize high-frequency concurrent operations). No transactional fix exists without pessimistic locking or protocol changes.

#### 1. PUT Phase 1 vs DeleteBucket

| Aspect | Detail |
|---|---|
| **Root cause** | PUT Phase 1 reads `B` but doesn't write it. DeleteBucket writes (deletes) `B`. No write-write conflict. |
| **Why no touch-write** | Writing `B` in Phase 1 would serialize ALL PUTs to the same bucket — unacceptable throughput impact. |
| **Mitigation** | pend-delete workaround: separate transaction sets `delete-pending` flag on `B` before DeleteBucket. Short delay allows in-flight Phase 1s to drain. See [bucket_delete.md](bucket_delete.md). |
| **Residual risk** | Extreme scheduling delay on a Phase 1 transaction (started before pend-delete, commits after delay). Results in orphaned `O:` entries — detectable by periodic scans. |

#### 2. InitiateMultipartUpload vs DeleteBucket

| Aspect | Detail |
|---|---|
| **Root cause** | Same as #1 — Initiate reads `B`, DeleteBucket deletes `B`. No write-write conflict. |
| **Why no touch-write** | Same — would serialize all upload initiations per bucket. |
| **Mitigation** | Same pend-delete workaround. |
| **Residual risk** | Same class — orphaned `:M:` head + `P:M` for a deleted bucket. Sweeper cleans. |

#### 3. UploadPart Phase 1 vs Complete/Abort

| Aspect | Detail |
|---|---|
| **Root cause** | UploadPart reads `:M:` head but writes only the part entry (`:M:<part>`). Complete/Abort deletes head. Different keys — no write-write conflict. |
| **Why no touch-write** | Writing `:M:` head on every UploadPart would serialize ALL parallel part uploads to the same multipart upload — defeats the purpose of parallel uploads. |
| **Mitigation** | Accepted by design. The `G:M` cleanup loop (triggered by Complete or Abort) range-scans `M:<ref_tag>*` in multiple passes, catching late blind writes from in-flight Phase 3s. |
| **Residual risk** | A Phase 3 blind write can land AFTER the G:M cleanup loop completes and deletes the G:M entry. This creates a permanent orphaned `:M:` entry (pointing to already-freed storage data). No discovery mechanism exists for these entries — the head is gone (LC can't find it), G:M is gone (cleanup loop won't re-run), no per-part P: entry (sweeper can't find it). |

**Accepted trade-off:** These orphaned `:M:` entries are invisible KV garbage — no client can read them (head is gone), no data corruption (storage data was already freed), and the `:M:` domain is short-lived by nature (holds only parts mid-upload or forgotten entries). This is no worse than orphaned part objects in the current RADOS model (which accumulate from the same race and are only discoverable via expensive full-pool scans like `rgw-orphan-list`).

**Future option:** A periodic background scan of `:M:` entries (per bucket or global) can detect headless part entries and remove them. This scan is inexpensive because the `:M:` domain is small — it contains only parts of active or recently-completed uploads, not long-lived object data.

---

## Summary

| Category | Count | TiKV status |
|---|---|---|
| Safe as-is | 15+ pairs | Write-write conflicts on shared keys |
| Modified (functional counts) | 5 operations (annotations, tags) | `annotation_count` / `tag_count` in O: — identical code on FDB and TiKV |
| Modified (`last_active`) | 1 operation (DELETE Case 1) | TiKV-only touch-write to O: |
| Unsafe (open issues) | 3 races | Mitigated via pend-delete or sweeper |

### Cost per PUT (hot path)

| Phase | Operations |
|---|---|
| Phase 1 | 1 read (B) + 1 write (P:O) |
| Phase 2 | Storage-tier write (no KV) |
| Phase 3 | 2 reads (P:O, O:) + 1–2 writes (O:, optionally G:O on overwrite) + 1 delete (P:O) |
| **Total** | **3 reads + 2–3 writes + 1 delete across 2 transactions** |

All operations are fully safe on FDB. On TiKV, derived-KV gaps are closed by functional count fields (preferred) or `last_active` (for versioned operations with no functional O: write). The remaining 3 open issues are structural (shared-key reader pattern) and are mitigated by application-level protocols with negligible residual risk.
