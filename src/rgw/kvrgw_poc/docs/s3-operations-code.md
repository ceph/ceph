# S3 Operations — Code Walkthrough

How each S3 operation is implemented in the C++ backend (`service_impl.cpp`). All operations resolve `tenant_name` → `tenant_id` via cache/FDB lookup before proceeding. FDB operations return `std::expected<T, fdb_error_t>` (C++23); DataStore operations return `std::error_code` — no exceptions on the hot path. RPC handlers always return `grpc::Status::OK`; application errors are communicated via `KvrgwErrorCode error_code` in the response proto. FDB errors are mapped via `fdb_to_error()` to `KvrgwErrorCode`; retriable errors (200–206) are retried, permanent errors (300+) fail immediately.

---

## PutObject

**S3:** `PUT /bucket/key` | **gRPC:** streaming `PutObject`

**Entry:** line 675

1. Read metadata message (bucket, key, content-type, content-length, if_match, if_none_match, tags, user metadata)
2. Resolve tenant_id; validate content_type length (≤ 255)
2b. If tags present: `prepare_tags(proto_tags)` → `PreparedTags` (validate, sort, encode)
3. Soft `check_access(kDenyWrite)` before reading data stream (early reject)
4. Generate ref_tag (`ref_tags_.next()` → 12B: `rgw_id(4) + seq(8)`)
5. If `if_match` or `if_none_match` set → build `PutCondition` struct
5. Stream data chunks; compute MD5 etag
6. Build `ObjectValue` header (ref_tag, etag, size, timestamps, content_type)
7. Build `PutObjectRequest` struct → call `put_object_route(req, data, data_len)`

**`put_object_route`** — shared entry point for gRPC `PutObject` and perf driver `put_worker`:

```
put_object_route(PutObjectRequest& req, data, data_len):
  tc = tier_config_state_.active_copy()

  if batch_size > 1 && !tags:
    select tier by size → set chunk.type (no P:O or disk write here)
    enqueue BatchCommitEntry → batch_queue_.enqueue()
    wait on future → return status/etag
    (batch worker handles Phase 1 group P:O + Phase 2 disk writes + Phase 3 metadata commit)
      ▸ Fault injection point: ErrInsertion kAbortAfterBatchPhase2 — aborts after Phase 2 disk writes, before Phase 3

  else (batch_size == 1 or has tags):
    if zero-byte: INLINE → put_object_single_txn
    else: select_storage_tier
```

**`select_storage_tier`** (line 631):

```
if size <= max_inline:
    chunk.type = INLINE
    inline_data = data
    → put_object_single_txn

if size <= max_kv_store:
    chunk.type = KV_STORE
    → put_object_single_txn

else:
    chunk.type = STORAGE
    → Phase 1: blocking store_.set(P:O)
    → Phase 2: data_store.write(ref_tag, data)
      ▸ Fault injection point: ErrInsertion kAbortAfterSinglePhase2 — aborts after blob write, before Phase 3
    → Phase 3: put_object_phase3
```

**`put_object_single_txn`** (line 573):
```
txn (retry 10x on conflict) {
  verify_bucket_in_txn(B, kDenyWrite)   // bucket must exist + not deny writes
  get(S:O)
  if PutCondition set:
    check_put_condition(existing, cond) → FAILED_PRECONDITION on mismatch
  if exists: move_object_to_g(existing)
  if PreparedTags: apply_tags_to_value(object, tags, tr, bucket_id, ref_tag)
  put(S:O, OValueBuf wire format)
  if KV_STORE: put(D:key, data)
  commit
}
```

**`put_object_phase3`** (line 442):
```
txn (retry 10x) {
  verify_bucket_in_txn(B, kDenyWrite)   // bucket must exist + not deny writes
  get(P:O) → if missing: check if already committed (idempotent) or abort
  get(S:O)
  if PutCondition set:
    check_put_condition(existing, cond) → FAILED_PRECONDITION on mismatch
  if exists with same ref_tag: del(P:O), return OK
  if exists with different ref_tag: move_object_to_g(existing)
  if PreparedTags: apply_tags_to_value(object, tags, tr, bucket_id, ref_tag)
  put(S:O, OValueBuf)
  del(P:O)
  commit
}
```

**`put_object_in_txn`** — single-mode wrapper: allocates `VerifiedBucket[1]` on stack, calls `put_prepare` + `put_finalize`.

**`put_prepare`** — issues async FDB reads (B: if `need_bucket`, S:O always, P:O if single-mode storage tier). Uses `KeyBuf` for object key (zero heap allocation).

**`put_finalize`** — resolves futures, applies mutations:
```
put_finalize(tr, ctx, params, VerifiedBucket* verified, verified_count, out_vs):
  if has_bucket_future: resolve → verify access → add to verified array
  else: linear scan verified array for cached result
  if is_storage_tier:
    if f_po set (single mode): verify P:O exists → del(P:O)
    else (batch mode): skip (group P:O handled by commit_batch)
  resolve S:O → check conditions → displace old → compute version
  put(S:O, OValueBuf)
  if KV_STORE: put(D:key, data)
```

`skip_bucket_verify` is true when the calling batch has already verified the bucket for a prior entry in the same transaction. `is_storage_tier` is true when P:O was written in Phase 1 and needs to be deleted in the same commit.

---

## GetObject

**S3:** `GET /bucket/key` | **gRPC:** streaming `GetObject`

**Entry:** line 753

1. Resolve bucket_id (cached)
2. `check_access(kDenyRead)` — refresh-before-reject on cache deny
3. `load_object_with_data(bucket_id, key)`:
   - Snapshot transaction: `get(S:O)` → parse ObjectValueHeader
   - If INLINE: data = `inline_data` from O: value
   - If KV_STORE: reconstruct D: key → `get(D:key)` → data
   - If STORAGE: data loaded separately below
3. If STORAGE: `data_store.read(ref_tag, offset, length)`
4. Byte-range handling: resolve `Range` header → slice body
5. Stream response: metadata message + 64KB data chunks

---

## HeadObject

**S3:** `HEAD /bucket/key` | **gRPC:** `HeadObject`

**Entry:** line 832

1. Resolve bucket_id (cached)
2. `check_access(kDenyRead)` — refresh-before-reject on cache deny
3. `load_object(bucket_id, key)` → `get(S:O)` → parse header
4. Return metadata only (etag, size, last_modified, content_type, tags_count). No data read.

---

## DeleteObject

**S3:** `DELETE /bucket/key` | **gRPC:** `DeleteObject`

**Entry:** line 1029

Conditional fields (optional): `if_match` (ETag), `if_match_last_modified_time` (unix seconds), `if_match_size` + `has_if_match_size` (distinguishes size=0 from not-set).

Three building-block functions replace the former monolithic `delete_object_in_txn`:

```
check_access(kDenyWrite)                  // soft (cached, refresh-before-reject)
txn {
  // --- delete_prepare(tr, tenant_id, bucket, key, need_bucket=true) ---
  kv_async_get(B:)                        // issue FDB read (non-blocking)
  kv_async_get(S:O)                       // issue FDB read (non-blocking)

  // --- delete_verify_bucket(tr, ctx, need_bucket=true) ---
  kv_wait_get(B:) → verify bucket exists + kDenyWrite

  // --- delete_apply(tr, ctx, bucket_state, cond) ---
  kv_wait_get(S:O) → if missing: return (idempotent)
  if conditionals set:
    check if_match against stored etag → FAILED_PRECONDITION on mismatch
    check if_match_last_modified_time against stored mtime → FAILED_PRECONDITION
    check if_match_size against stored size → FAILED_PRECONDITION
  move_object_to_g(S:O value)  // or single-txn cleanup

  commit
}
```

`delete_single` is a convenience wrapper that calls all three building blocks in sequence.

**`move_object_to_g`** (line 456):
```
must_defer_to_gc = CHUNK_STORAGE or CHUNK_STORAGE_REF
                   or has_external_annotations()
                   or kv_store_coalescing
if must_defer_to_gc:
    kv_put(G:O, GcValueHeader{chunk, flags, object_size, mtime})
    kv_del(S:O)
else:
    if CHUNK_CHILD_D:     decrement_or_del_child_d(D:, size, shared)
    if CHUNK_CHILD_D_REF: decrement_or_del_child_d(D:owner, size, shared)
    if kFlagExternalTags: kv_del(C:T)
    if kFlagExtendedAttrs: kv_range_clear(C:prefix)
    kv_del(S:O)
```

---

## DeleteMulti

**S3:** `POST /bucket?delete` | **gRPC:** `DeleteMulti`

**Entry:** line 1053

1. Soft `check_access(kDenyWrite)` (cached, refresh-before-reject)
2. Batch keys into chunks of 10
3. For each chunk: single FDB transaction with pipelined reads (1 round-trip for B: + N×S:O):
   ```
   txn {
     // Issue all async reads up front
     delete_prepare(tr, tenant_id, bucket, key[0], need_bucket=true)
       → kv_async_get(B:) + kv_async_get(S:O[0])
     for key[1..N-1]:
       delete_prepare(tr, tenant_id, bucket, key[i], need_bucket=false)
         → kv_async_get(S:O[i]) only

     // Resolve bucket once
     delete_verify_bucket(tr, ctx[0], need_bucket=true)
       → kv_wait_get(B:) → verify access

     // Apply each delete (resolves S:O futures one by one)
     for each key:
       delete_apply(tr, ctx[i], bucket_state, cond)
     commit
   }
   ```
4. On chunk commit failure: retry; after 3 failures switch to single-key mode (`delete_single`)
5. Returns per-key success/error list

---

## ListObjects

**S3:** `GET /bucket?list-type=2` | **gRPC:** `ListObjects`

**Entry:** line 890

1. `read_bucket_state()` — fresh FDB read (no cache)
2. Check `kDenyList` → PermissionDenied if set
3. Build S:O prefix for range scan; continuation token = `base64(bucket_id + key)`
4. Paginated loop:
   - `range_scan(start, end, batch_limit)` — batch_limit = max_keys × 10 (over-fetch for delimiter collapsing)
   - For each row: parse ObjectValueHeader → extract key, size, etag, last_modified
   - Apply prefix/delimiter filtering (CommonPrefixes)
   - Track `budget` (max_keys) and `continuation_token` (base64 of last key)
4. Return object list + truncation flag + next token

---

## CreateBucket

**S3:** `PUT /bucket` | **gRPC:** `CreateBucket`

**Entry:** line 507

```
for attempt in 0..9:
  tr = begin_transaction()   → if error: retry (1020) or return INTERNAL
  get(B:key) → if error: retry/INTERNAL; if exists: ALREADY_EXISTS
  bid = get(L:N:bucket_id counter)  → read-modify-write
  put(L:N:bucket_id, bid + 1)
  put(B:key, BucketValueHeader{bucket_id=bid+1, created_at})
  commit → if error: retry (1020) or return INTERNAL
```

Counter is transactional read-modify-write — no wasted IDs on conflict. ID 0 is never assigned (NULL sentinel).

---

## DeleteBucket

**S3:** `DELETE /bucket` | **gRPC:** `DeleteBucket`

**Entry:** line 1108

1. `read_bucket_state()` — fresh FDB read; `check_access(kDenyDeleteBucket)` → PermissionDenied if set
2. Pre-scan: `range_scan(S:O prefix, limit=1)` — if non-empty → BucketNotEmpty
3. Pre-scan: `range_scan(P: bucket prefix)` — count pending uploads
4. Transaction:
   ```
   txn {
     re-check S:O empty (limit 1)
     for each P:O entry: move_po_to_go (force-abort pending uploads)
     get(B:key) → if missing: NOT_FOUND
     del(B:key)
     commit
   }
   ```
4. Invalidate bucket cache

---

## ListBuckets

**S3:** `GET /` | **gRPC:** `ListBuckets`

**Entry:** line 858

1. Build B: prefix for tenant_id
2. `range_scan(B:prefix, B:prefix_end, 0)` — all buckets for tenant
3. For each row: parse BucketValueHeader → extract bucket_name (from key), created_at
4. Return list sorted by key (lexicographic bucket name order — guaranteed by KV scan)

---

## BucketExists (HeadBucket)

**S3:** `HEAD /bucket` | **gRPC:** internal `BucketExists`

**Entry:** line 548

1. `read_bucket_state(tenant_id, bucket_name)` — always reads FDB `get(B:key)` (returns full state)
2. Never serves stale cache for HEAD
3. Returns exists=true/false + bucket_id + access_flags

---

## Common Helpers

| Helper | Used by | Returns | Purpose |
|--------|---------|---------|---------|
| `read_bucket_state` | BucketExists, List, DeleteBucket | `expected<optional<BucketState>, fdb_error_t>` | Fresh FDB Get(B) → full state (replaces `resolve_bucket_id`) |
| `check_access` | GET, HEAD, DELETE, PUT entry | `KvrgwErrorCode` | Cache-first access check; refresh-before-reject on deny |
| `verify_bucket_in_txn` | PUT single-txn, Phase 3, DELETE, DeleteMulti | `expected<void, fdb_error_t>` | In-txn bucket existence + policy check |
| `fdb_to_error` | All FDB error sites | `KvrgwErrorCode` | Maps `fdb_error_t` to `KvrgwErrorCode` (replaces `fdb_internal`) |
| `is_retriable` | Retry loops | `bool` | Switch-based check if `KvrgwErrorCode` is retriable (200–206) |
| `parse_policy_flags` | PutBucketPolicy | `uint8_t` | JSON policy → access_flags bitmask |
| `get_bucket_id_cached` | GET, HEAD, PUT Phase 1 | `expected<optional<string>, fdb_error_t>` | Cache-first bucket_id lookup |
| `tenant_id_for_name` | All RPCs | `KvrgwErrorCode` | tenant_name → tenant_id resolution |
| `move_object_to_g` | PUT (overwrite), DELETE | `bool` (false = corrupt key, fenced) | Single-txn cleanup or defer to GC |
| `decrement_or_del_child_d` | move_object_to_g, GcWorker | `void` | Decrement D: ref_count or delete; shared helper in `ref_count_ops.cpp` |
| `delete_prepare` | DELETE, DeleteMulti | `void` | Issue kv_async_get(B:) + kv_async_get(S:O) — non-blocking |
| `delete_verify_bucket` | DELETE, DeleteMulti | `expected<void, fdb_error_t>` | kv_wait_get(B:), verify access |
| `delete_apply` | DELETE, DeleteMulti | `expected<void, fdb_error_t>` | kv_wait_get(S:O), conditional check, move_to_G |
| `delete_single` | DELETE, DeleteMulti (fallback) | `expected<void, fdb_error_t>` | Calls prepare + verify + apply in one wrapper |
| `load_object` | HEAD | `expected<optional<ObjectValue>, fdb_error_t>` | Read + parse O: value (metadata only) |
| `load_object_with_data` | GET | `expected<optional<LoadResult>, fdb_error_t>` | Read O: + fetch data from appropriate tier |
| `write_object_value` | PUT single-txn, Phase 3 | `bool` | Build OValueBuf wire format |
| `check_put_condition` | PUT single-txn, Phase 3 | `KvrgwErrorCode` | Enforce if_match/if_none_match against existing O: |
| `check_delete_condition` | DELETE, DeleteVersion | `KvrgwErrorCode` | Enforce if_match + mtime + size against target entry |
| `prepare_tags` | PutObject, PutObjectTagging | `expected<PreparedTags, KvrgwErrorCode>` | Validate proto tags (count, key/value lengths, aggregate size), sort, encode |
| `apply_tags_to_value` | PutObject, PutObjectTagging | `void` | Set tags_count + inline tags or kFlagExternalTags + C:T write |
| `encode_tag_payload` / `decode_tag_payload` | PutObjectTagging, GetObjectTagging, CopyObject | `string` / `TagSet` | Binary tag serialization |

---

## PutBucketPolicy

**S3:** `PUT /bucket?policy` | **gRPC:** `PutBucketPolicy`

1. Resolve tenant_id, bucket_name
2. `read_bucket_state()` — verify bucket exists
3. `parse_policy_flags(policy_json)` → compute `access_flags` bitmask
4. Write B: value = `BucketValueHeader{bucket_id, created_at, access_flags}` + policy_json
5. Invalidate local bucket cache entry

---

## GetBucketPolicy

**S3:** `GET /bucket?policy` | **gRPC:** `GetBucketPolicy`

1. Resolve tenant_id, bucket_name
2. `read_bucket_state()` → return `policy_json` (empty string if no policy)

---

## DeleteBucketPolicy

**S3:** `DELETE /bucket?policy` | **gRPC:** `DeleteBucketPolicy`

1. Resolve tenant_id, bucket_name
2. `read_bucket_state()` — verify bucket exists
3. Write B: value = `BucketValueHeader{bucket_id, created_at, access_flags=0}` (no policy JSON)
4. Invalidate local bucket cache entry

---

## PutBucketVersioning

**S3:** `PUT /bucket?versioning` | **gRPC:** `PutBucketVersioning`

1. Resolve tenant_id
2. Read B: value in transaction
3. Parse status string ("Enabled" or "Suspended") → `VersioningState` enum
4. Write B: value with updated `versioning_state` byte (offset 17)
5. Invalidate bucket cache

---

## GetBucketVersioning

**S3:** `GET /bucket?versioning` | **gRPC:** `GetBucketVersioning`

1. `read_bucket_state()` → return status string ("Enabled", "Suspended", or "")

---

## Versioning Helpers

### compute_new_version(versioning_state, old_object)

```
ENABLED:
  if old_object exists: {old.next_vid, old.next_vid - 1}
  else:                 {kFirstVersionId, kFirstVersionId - 1}
SUSPENDED:
  if old_object exists: {kNullVersion, old.next_vid}
  else:                 {kNullVersion, kFirstVersionId}
DISABLED:
  {kNullVersion, kFirstVersionId}
```

### displace_old_object(tr, versioning_state, object_key, old_object)

```
ENABLED:
  put V:<old_vid> = old_object (preserve as historical version)
SUSPENDED:
  if old.vid != kNullVersion: put V:<old_vid> (preserve real version)
  else: move_object_to_g (destroy existing null)
  then: if V:<kNullVersion> exists → GC it (one-null-version rule)
DISABLED:
  move_object_to_g (destroy)
```

### set_object_metadata (version_id gating)

Report version_id only when versioning was ever active:
```
if !(vid == kNullVersion && next_vid == kFirstVersionId):
  metadata.version_id = to_string(vid)
```

### PutObject version_id response

Only report when bucket has versioning ENABLED (not suspended, not disabled):
```
if (vs == VERSIONING_ENABLED):
  response.version_id = to_string(object_value.version_id)
```

---

## DeleteObject (versioned buckets)

**S3:** `DELETE /bucket/key` | **gRPC:** `DeleteObject`

When versioning is ENABLED or SUSPENDED:
1. Read existing O: entry
2. `displace_old_object()` — move old to V: or GC
3. `compute_new_version()` — get new vid/next_vid
4. Write delete marker to O: (flags = kFlagFenced, chunk = INLINE, no data)
5. Return `is_delete_marker=true, version_id=<dm_vid>`

When versioning is DISABLED:
1. `move_object_to_g()` — destroy the object
2. Delete O: entry

---

## DeleteObjectVersion

**S3:** `DELETE /bucket/key?versionId=X` | **gRPC:** `DeleteObjectVersion`

Conditional fields (optional): same as DeleteObject (`if_match`, `if_match_last_modified_time`, `if_match_size` + `has_if_match_size`). Checked against the target version's metadata.

**Case 1 — target is non-current (in V:):**
1. Look up `V:<key><target_vid>`
2. If conditionals set: check against V: entry metadata → FAILED_PRECONDITION on mismatch
2. If found: create G: entry for its data, delete V: entry
3. If not found: return OK (idempotent)

**Case 2 — target is current (in O:):**
1. Scan `V:<key>\x00` prefix (limit=1) to find promotion candidate
2. If found: promote to O: (copy V: value, set next_vid from current), delete V: entry
3. If not found: delete O: entry (key completely gone)
4. GC current version's data (D: or G: entry)

---

## CopyObject

**S3:** `PUT /bucket/key` with `x-amz-copy-source` | **gRPC:** `CopyObject`

**Entry:** line 2242

Single FDB transaction — no P:O needed (no storage-tier write).

```
1. Resolve tenant_id, src_bucket_id, dst_bucket_id
2. verify_bucket_in_txn(dst_B, kDenyWrite)
3. Read source: get(src S:O) or get(V:<vid>) if src_version_id set
   → if missing or delete marker: NOT_FOUND
4. Source conditionals:
   if if_match set: check src.etag → FAILED_PRECONDITION on mismatch
   if if_none_match set: check src.etag → FAILED_PRECONDITION on match
5. Read destination: get(dst S:O) → may be null (new object)
6. Destination conditionals:
   if dst_if_match set: check dst.etag → FAILED_PRECONDITION on mismatch
   if dst_if_none_match set: check dst.etag → FAILED_PRECONDITION on match
7. Self-copy guard (same bucket + same key + no versionId):
   if replace_metadata: in-place metadata update (rewrite O: with new content_type); return
   else: reject (InvalidRequest)
8. Build new ObjectValue (copy src fields; override content_type + metadata if replace_metadata)
9. Data sharing by tier:
   CHUNK_INLINE:
     byte-copy inline_data into new ObjectValue
   CHUNK_CHILD_D:
     read D: entry for src; write D: with ref_count=2 (flags-past-data suffix)
     set kFlagSharedData on src O:; dst chunk = CHUNK_CHILD_D_REF
   CHUNK_CHILD_D_REF:
     read D: entry (owner's); increment existing ref_count
     dst chunk = CHUNK_CHILD_D_REF (same owner bucket_id + ref_tag)
   CHUNK_STORAGE:
     create R:<src_ref_tag> with count=2 + chunk_descriptor
     set kFlagSharedData on src O:; dst chunk = CHUNK_STORAGE_REF
   CHUNK_STORAGE_REF:
     read R:<data_ref_tag>; increment count
     dst chunk = CHUNK_STORAGE_REF
10. Tag handling:
    if !replace_metadata && src.tags_count > 0:
      if src has inline tags: copy tags to new_value
      if src has external tags: get(C:T src_ref_tag) → put(C:T dst_ref_tag, payload)
        set kFlagExternalTags + tags_count on new_value
    if replace_metadata: no tags (AWS REPLACE strips metadata)
11. Displace old destination if overwrite:
    displace_old_object(versioning_state, dst_existing)
12. compute_new_version → assign version_id/next_vid to dst
13. put(dst S:O, new ObjectValue)
14. commit
15. Return etag, last_modified, version_id, copy_source_version_id
```

---

## ListObjectVersions

**S3:** `GET /bucket?versions` | **gRPC:** `ListObjectVersions`

1. Scan O: range (all current versions)
2. Scan V: range (all historical versions)
3. Merge into single list, sort by (object_name ASC, is_current DESC, version_id ASC)
4. Apply key_marker filter, paginate with max_keys
5. Set `next_key_marker` = last returned key, `next_version_id_marker` = last returned vid
6. Return versions + delete markers with is_latest flag

---

## PutObjectTagging

**S3:** `PUT /bucket/key?tagging` | **gRPC:** `PutObjectTagging`

1. Resolve tenant_id, bucket_id (cached)
2. `check_access(kDenyWrite)`
3. Validate: tags.size() ≤ 10, keys 1–128B, values 0–256B, aggregate ≤ 5120B
4. Sort tags lexicographically by key
5. Encode tag payload; determine inline vs external

```
txn (retry 10x) {
  verify_bucket_in_txn(B, kDenyWrite)
  get(S:O) → NOT_FOUND if missing or delete marker
  Parse existing ObjectValue (tags_count, external flag, ref_tag)

  if new fits inline:
    Rebuild O: with inline tags, set tags_count
    put(S:O)
    if was external: del(C:T)
  else (external):
    Rebuild O: with kFlagExternalTags, set tags_count, no inline tags
    put(S:O)
    put(C:T, encoded_payload)
  commit
}
```

All cases write O: (tags_count field) for write-write conflict detection.

---

## GetObjectTagging

**S3:** `GET /bucket/key?tagging` | **gRPC:** `GetObjectTagging`

1. Resolve tenant_id, bucket_id (cached)
2. `check_access(kDenyRead)`

```
snapshot txn {
  get(S:O) → NOT_FOUND if missing or delete marker
  if tags_count == 0: return empty TagSet
  if inline (!kFlagExternalTags): decode inline tags from O: tail
  if external (kFlagExternalTags): get(C:T ref_tag) → decode payload
  return tags (sorted by key — pre-sorted at PUT time)
}
```

---

## DeleteObjectTagging

**S3:** `DELETE /bucket/key?tagging` | **gRPC:** `DeleteObjectTagging`

1. Resolve tenant_id, bucket_id (cached)
2. `check_access(kDenyWrite)`

```
txn (retry 10x) {
  verify_bucket_in_txn(B, kDenyWrite)
  get(S:O) → NOT_FOUND if missing or delete marker
  if kFlagExternalTags set: del(C:T)
  Rebuild O: with tags_count=0, clear kFlagExternalTags
  put(S:O)
  commit
}
```

---

## Delete Marker Signaling

When HeadObject or GetObject encounters a delete marker (current version or specific version):
- Backend returns `error_code = KVRGW_ERR_NO_SUCH_KEY` with `error_detail = "DeleteMarker:<vid>:<mtime>"`
- Frontend calls `parseDeleteMarkerDetail(resp.GetErrorDetail())` to extract version ID and mtime
- Frontend returns `(*HeadObjectOutput{LastModified: mtime}, ErrNoSuchKey)`
- versitygw framework detects non-nil result + error → adds `x-amz-delete-marker: true` header
