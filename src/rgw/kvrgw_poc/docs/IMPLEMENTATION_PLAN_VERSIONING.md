# Versioning Implementation Plan

## Core principle: Adapt-or-Abort at commit time

Every write reads B fresh inside the transaction, branches on versioning state. The cache gives a soft early check; the transaction enforces truth. `verify_bucket_in_txn` returns `expected<VerifiedBucket, Status>` — on failure the txn aborts immediately.

---

## Data structure changes

### 1. `BucketValueHeader` — 18 bytes (was 17)

```cpp
struct BucketValueHeader {
  uint8_t bucket_id[8];
  int64_t created_at_unix;
  uint8_t access_flags;
  uint8_t versioning_state;  // 0=Disabled, 1=Enabled, 2=Suspended
};
```

### 2. `ObjectValueHeader` — 60 bytes (was 52)

```cpp
struct ObjectValueHeader {
  // ... existing 52 bytes ...
  uint32_t version_id;   // NO_VERSION=0, NULL_ID=1, real >= 2
  uint32_t next_vid;     // next available (decrements from 0xFFFFFFFF)
};

static constexpr uint8_t kFlagFenced = 0x02;
```

### 3. Constants

```cpp
inline constexpr uint32_t kNoVersion       = 0;
inline constexpr uint32_t kNullId          = 1;
inline constexpr uint32_t kFirstVersionId  = 0xFFFFFFFF;
inline constexpr char     kCategoryVersion = 'V';

enum VersioningState : uint8_t {
  VERSIONING_DISABLED  = 0,
  VERSIONING_ENABLED   = 1,
  VERSIONING_SUSPENDED = 2,
};
```

### 4. V: key layout

```
[S 1B][shard_count 2B][shard_id 2B][bucket_id 8B][V 1B][object_name][version_id 4B BE]
```

Builder: `make_v_key(bucket_id, object_name, version_id)`.

---

## Bucket cache changes

```cpp
struct VerifiedBucket {
  std::string bucket_id;
  VersioningState versioning;
};
// returned as expected<VerifiedBucket, fdb_error_t>
```

`BucketCacheEntry` and `BucketState` gain a `versioning_state` field. `read_bucket_state()` parses it from the B value. `verify_bucket_in_txn()` returns the versioning state along with the bucket_id so the caller can branch.

---

## Shared displacement functions

```cpp
// Moves old O: to V: (versioned) or G:O (unversioned/null-vid)
void displace_old_object(
    KvTransaction& tr,
    VersioningState state,
    const ObjectValue& old_o,
    std::string_view bucket_id);

// Determines vid and next_vid for the new O: entry
struct NewVersionIds { uint32_t version_id; uint32_t next_vid; };

NewVersionIds compute_new_version(
    VersioningState state,
    const ObjectValue* old_o);  // null if no existing O:
```

### `displace_old_object` logic

| State | old_o.vid | Action |
|---|---|---|
| Disabled | any | `put(G:O, old)` — GC reclaims |
| Enabled/Suspended | `>= 2` (real) | `put(V:<old_vid>, old)` — preserve |
| Enabled/Suspended | `NULL_ID` or `NO_VERSION` | `put(G:O, old)` — null never goes to V: |

### `compute_new_version` logic

| State | old_o | Result |
|---|---|---|
| Disabled | any | `{NO_VERSION, kFirstVersionId}` |
| Enabled | exists | `{old.next_vid, old.next_vid - 1}` |
| Enabled | absent | `{kFirstVersionId, kFirstVersionId - 1}` |
| Suspended | exists | `{NULL_ID, old.next_vid}` |
| Suspended | absent | `{NULL_ID, kFirstVersionId}` |

---

## PUT — `put_object_single_txn` (Tier 1 + 2)

```
txn {
  vb = verify_bucket_in_txn(tr, bucket_name, kDenyWrite)
  if (!vb) return vb.error()                    // 404 or 403 → abort

  old = get(S:O)
  if (old && old.is_pending()) return CONFLICT   // incomplete upload guard

  if (old) displace_old_object(tr, vb->versioning, *old, vb->bucket_id)

  ids = compute_new_version(vb->versioning, old ? &*old : nullptr)
  new_hdr.version_id = ids.version_id
  new_hdr.next_vid   = ids.next_vid
  new_hdr.flags      = 0                        // not fenced

  put(S:O, new_value)
  if (tier == KV_STORE) put(D:key, data)

  err = commit(tr)
  if (err) return err                           // retry or propagate
  return OK with version_id
}
```

## PUT — `put_object_phase3` (Tier 3)

```
txn {
  vb = verify_bucket_in_txn(tr, bucket_name, kDenyWrite)
  if (!vb) return vb.error()

  old = get(S:O)
  pending = get(P:O)
  if (!pending) return CONFLICT                  // phase3 without phase1

  if (old) displace_old_object(tr, vb->versioning, *old, vb->bucket_id)

  ids = compute_new_version(vb->versioning, old ? &*old : nullptr)
  new_hdr.version_id = ids.version_id
  new_hdr.next_vid   = ids.next_vid
  new_hdr.flags      = 0

  put(S:O, new_value_from_pending)
  del(P:O)

  err = commit(tr)
  if (err) return err
  return OK with version_id
}
```

---

## DELETE without version-id

```
txn {
  vb = verify_bucket_in_txn(tr, bucket_name, kDenyWrite)
  if (!vb) return vb.error()

  old = get(S:O)

  switch (vb->versioning) {

  case DISABLED:
    if (!old) return OK                          // idempotent
    displace_old_object(tr, DISABLED, *old, vb->bucket_id)
    del(S:O)
    break

  case ENABLED:
    if (old) displace_old_object(tr, ENABLED, *old, vb->bucket_id)
    ids = compute_new_version(ENABLED, old ? &*old : nullptr)
    dm_hdr.version_id = ids.version_id
    dm_hdr.next_vid   = ids.next_vid
    dm_hdr.flags      = kFlagFenced              // delete marker
    dm_hdr.size       = 0
    dm_hdr.ref_tag    = {}
    put(S:O, dm_hdr)                             // fenced DM replaces O:
    break

  case SUSPENDED:
    if (old) displace_old_object(tr, SUSPENDED, *old, vb->bucket_id)
    ids = compute_new_version(SUSPENDED, old ? &*old : nullptr)
    dm_hdr.version_id = ids.version_id           // == NULL_ID
    dm_hdr.next_vid   = ids.next_vid
    dm_hdr.flags      = kFlagFenced
    dm_hdr.size       = 0
    dm_hdr.ref_tag    = {}
    put(S:O, dm_hdr)
    break
  }

  err = commit(tr)
  if (err) return err
  return OK with dm_hdr.version_id               // 0 for DISABLED
}
```

---

## GET / HEAD

```
object = get(S:O)
if (!object) return 404

if (object->is_delete_marker()) {
  return 404 with headers:
    x-amz-delete-marker: true
    x-amz-version-id: object->version_id
}

// normal path — return data
response.version_id = object->version_id         // 0 means omit header
```

### GET with version-id

```
object = get(S:O)
if (object && object->version_id == requested_vid) {
  if (object->is_delete_marker()) return 405
  return data
}

// not current — check V:
v_entry = get(V:<requested_vid>)
if (!v_entry) return 404
if (v_entry->is_delete_marker()) return 405
return data from v_entry
```

---

## ListObjectsV2

One-line addition to existing scan loop:

```
if (entry.is_delete_marker()) continue;          // skip fenced
```

---

## DELETE with version-id (Step 8)

```
txn {
  current = get(S:O)

  if (current && current->version_id == target_vid) {
    // Case 2: removing the current version — promote
    latest_v = range_scan(V:<bucket_id><object_name>, limit=1, reverse)
    if (latest_v) {
      promoted = latest_v.value
      promoted.next_vid = current->next_vid       // inherit counter
      put(S:O, promoted)
      del(V:<latest_v.key>)
    } else {
      del(S:O)                                   // no versions left
    }
    if (current->has_data()) put(G:O, *current)  // GC reclaims blob
  } else {
    // Case 1: removing a non-current version
    v_entry = get(V:<target_vid>)
    if (!v_entry) return OK                      // idempotent
    if (v_entry->has_data()) put(G:O, *v_entry)  // GC reclaims blob
    del(V:<target_vid>)
  }

  err = commit(tr)
  if (err) return err
  return OK
}
```

No `verify_bucket_in_txn` needed — mechanical regardless of versioning state.

---

## ListObjectVersions (Step 9)

Merge-scan O: + V: by `(object_name, version_id DESC)`. O: entry always sorts first for its object. Return type indicator (Version vs DeleteMarker) per entry.

---

## PutBucketVersioning

```
txn {
  b = get(B:<bucket_name>)
  if (!b) return 404

  old_state = b->versioning_state
  // Disabled → Enabled|Suspended: OK
  // Enabled → Suspended: OK
  // Suspended → Enabled: OK
  // Any → Disabled: INVALID (AWS doesn't allow)
  if (new_state == DISABLED) return INVALID_ARGUMENT

  b->versioning_state = new_state
  put(B:<bucket_name>, b)

  err = commit(tr)
  if (err) return err
  invalidate_cache(bucket_name)
  return OK
}
```

---

## GcWorker (Step 10)

Existing `process_g_o_entry` already frees blobs via ref_tag. Extension: when a G:O entry originated from a versioned displacement, it may reference a blob — same cleanup path. No new logic needed beyond ensuring G:O entries from `displace_old_object` carry the correct ref_tag.

---

## Implementation order

1. **Data structures** — extend `BucketValueHeader` (18B), `ObjectValueHeader` (60B), add V: key builder, add constants
2. **Bucket state** — `VersioningState` in cache/state, `verify_bucket_in_txn` returns it, `PutBucketVersioning` RPC
3. **Displacement + version-id helpers** — `displace_old_object`, `compute_new_version` as shared functions
4. **PUT** — wire universal branching into `put_object_single_txn` and `put_object_phase3`
5. **DELETE without vid** — branch on state, write fenced DM
6. **GET / HEAD** — fenced check, version-id lookup
7. **ListObjectsV2** — skip fenced entries
8. **DELETE with vid** — Case 1 + Case 2 promotion
9. **ListObjectVersions** — O: + V: merge scan
10. **GcWorker** — handle V: entries in G:O cleanup

Steps 1–7 are the critical path. Steps 8–9 are additive. Step 10 extends existing GC.

The big architectural win: steps 3–5 share a single displacement function. No duplicated version logic across PUT, DELETE, and (future) CompleteMultipartUpload. The bucket-state branch is the only decision point, and it happens once per commit transaction.

---

## Execution protocol

- Build and run relevant tests from [TEST_PLAN_VERSIONING.md](TEST_PLAN_VERSIONING.md) after each step before moving to the next.
- Fix simple bugs inline; do not proceed if tests fail.
- Document any deviation from this plan in [CHANGES_VERSIONING.md](CHANGES_VERSIONING.md) for review.
- After all steps complete, run the full test suite including chaos (`./scripts/run_test_plan.sh`).
