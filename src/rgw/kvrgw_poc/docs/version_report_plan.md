# Version-ID Response Fix Plan

## Problem

PUT, GET, HEAD, DELETE responses must include `x-amz-version-id` based on **bucket versioning state**, not object value alone.

Current code uses `if (version_id != 0)` which misses the suspended case (version_id=0 should return `"null"`).

## S3 Spec

| Bucket State | Response Header |
|---|---|
| Disabled (never enabled) | No `x-amz-version-id` |
| Enabled | `x-amz-version-id: <numeric>` |
| Suspended | `x-amz-version-id: null` |

## Changes Required

### 1. Proto: `DeleteObjectResponse`

```protobuf
message DeleteObjectResponse {
  bool is_delete_marker = 1;
  string version_id = 2;
}
```

### 2. C++ `PutObject` handler (service_impl.cpp ~line 919)

**Before:**
```cpp
if (object_value.hdr.version_id != 0) {
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
```

**After:**
```cpp
if (versioning_state != VERSIONING_DISABLED) {
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
```

Requires propagating `versioning_state` from `put_object_single_txn`/`put_object_phase3` back to the PutObject handler (same pattern as version_id — via reference or output param).

### 3. C++ `DeleteObject` handler

- Propagate delete-marker's version_id out of the txn loop
- Propagate versioning_state out of the txn loop
- After commit:
```cpp
if (versioning_state != VERSIONING_DISABLED) {
    response->set_is_delete_marker(created_dm);
    response->set_version_id(std::to_string(dm_version_id));
}
```

### 4. C++ `GetObject` / `HeadObject` responses

Currently: metadata response only has etag/size/mtime/content_type.

Option A (simple): Add `version_id` to `ObjectMetadata` proto message:
```protobuf
message ObjectMetadata {
  string etag = 1;
  uint64 size = 2;
  int64 last_modified_unix = 3;
  string content_type = 4;
  string version_id = 5;
}
```

The C++ handler sets it from `object_value.hdr.version_id`. But this doesn't account for bucket state (disabled bucket should not report version_id).

Option B (bucket-state aware): GET/HEAD use cached bucket state (3s TTL). Add `versioning_state` to the cached `BucketIdEntry` struct and check before setting version_id in the response.

**Recommendation: Option A** — set `version_id` from the stored object unconditionally. The Go frontend already converts 0→omit. For disabled buckets where no version was ever assigned, the object has version_id=0 which maps to empty/omit. For suspended buckets where version_id=0 was explicitly assigned, the Go frontend returns `"null"`. The ambiguity (disabled vs suspended with version_id=0) is acceptable because:
- Objects created while disabled always have version_id=0
- Objects created while suspended also have version_id=0
- In both cases, if you later enable versioning and GET the object, AWS returns `"null"` for the pre-existing object

So Option A is actually correct for all cases.

### 5. Go frontend translation (already done)

```go
// version_id from proto → S3 response
"" or "0" when bucket disabled → omit header (versionIDFromProto returns "")
"0" when bucket suspended     → "null" (versionIDToExternal(0) returns "null")
non-zero                      → numeric string
```

**Issue with Option A**: The Go frontend can't distinguish "disabled with 0" from "suspended with 0" using only the version_id value. Both are 0.

**Resolution**: For GET/HEAD, always include version_id in ObjectMetadata. The Go frontend returns it as `"null"` for 0 and numeric for non-zero. This matches AWS behavior: once versioning has been enabled on a bucket (even if later suspended), GET always returns a version-id header. For buckets that were NEVER versioned, all objects have version_id=0 and the frontend omits the header. But if versioning was enabled then suspended, objects may have version_id=0 AND the header should be present.

**Final recommendation**: Use Option B. Add a `bool versioned` field to ObjectMetadata that indicates whether the bucket has ever had versioning enabled. The C++ GET/HEAD handler knows this from cached state.

### 6. Summary of C++ work

| File | Change |
|---|---|
| `proto/kvrgw.proto` | Add fields to `DeleteObjectResponse`; add `version_id` + `versioned` to `ObjectMetadata` |
| `service_impl.cpp` PutObject | Propagate `versioning_state`, check != disabled |
| `service_impl.cpp` DeleteObject | Propagate DM version_id + versioning_state, set response fields |
| `service_impl.cpp` GetObject | Set `version_id` in metadata from object_value; set `versioned` from cached bucket state |
| `service_impl.cpp` HeadObject | Same as GetObject |
| `service_impl.hpp` | Update signatures for put_object_single_txn/put_object_phase3 to propagate versioning_state |

### 7. Go frontend work (after C++ changes)

| File | Change |
|---|---|
| `frontend/backend_kvrgw.go` DeleteObject | Read `resp.GetIsDeleteMarker()` + `resp.GetVersionId()`, set on `*s3.DeleteObjectOutput` |
| `frontend/backend_kvrgw.go` GetObject | Read `meta.GetVersionId()`, include in response if `meta.GetVersioned()` |
| `frontend/backend_kvrgw.go` HeadObject | Same as GetObject |

### 8. Ceph s3-tests expected outcome after fix

All 14 versioning tests should pass:
- 7 currently passing ✓
- 5 fail on missing DeleteObject version_id → fixed by #3
- 2 fail on null version handling → fixed by #2 + #4

---

## Appendix: C++ Changes Already Made (Steps 2-4)

### Proto changes

| Field | Message | Purpose |
|---|---|---|
| `string version_id = 6` | `GetObjectRequest` | Pass ?versionId from S3 GET to backend |
| `string version_id = 2` | `PutObjectResponse` | Return assigned version_id after PUT |
| `string version_id = 4` | `HeadObjectRequest` | Pass ?versionId from S3 HEAD to backend |
| `repeated DeleteMultiObject objects = 4` | `DeleteMultiRequest` | Batch delete with per-object version_id |
| `message DeleteMultiObject { key, version_id }` | (new message) | Key+version pair for batch delete |

### service_impl.hpp

- `put_object_single_txn` — signature changed from `const ObjectValue&` to `ObjectValue&` (allows propagating version_id back)
- `put_object_phase3` — same signature change

### service_impl.cpp — PutObject

**Location:** `put_object_single_txn` (line ~769) and `put_object_phase3` (line ~631)

**Change:** After `compute_new_version` assigns `versioned_value.hdr.version_id = ids.version_id`, also propagate back to caller:
```cpp
new_value.hdr.version_id = ids.version_id;
```

**Location:** `PutObject` handler (line ~919)

**Change:** Set version_id on response after successful write:
```cpp
response->set_etag(object_value.etag_display());
if (object_value.hdr.version_id != 0) {
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
```

### service_impl.cpp — GetObject

**Location:** After access check (line ~939)

**Change:** Added versioned-object retrieval. If `request->version_id()` is non-empty:
1. Parse to `uint32_t target_vid`
2. Check current object (O: key) — if `hdr.version_id == target_vid`, use it
3. Otherwise, look up `make_v_key(bucket_id, key, target_vid)` in V: key space
4. Load data (inline, KV-store, or storage tier)
5. Proceed with loaded object for range handling + streaming

Falls back to existing latest-version path when version_id is empty.

### service_impl.cpp — HeadObject

**Location:** After access check (line ~1069)

**Change:** Same version-lookup pattern as GetObject:
1. If `request->version_id()` is non-empty, parse to target_vid
2. Check O: key, then V: key
3. Return metadata from the matched version

### service_impl.cpp — DeleteMulti

**Location:** `DeleteMulti` handler (line ~1613)

**Change:** Added support for `request->objects()` field (new proto field 4):
```cpp
if (request->objects_size() > 0) {
    for each object:
        if version_id set → call DeleteObjectVersion internally
        else → call delete_multi_one_key (existing path)
    return;
}
// fallback: existing keys-only path
```

### Design notes

- **No KV model changes** — same FDB key layout (O:, V:, B:, T:, D:, G: prefixes)
- **No new RPCs** — reused existing `DeleteObjectVersion` internally from `DeleteMulti`
- **Backward compatible** — new proto fields are additive; old clients/binaries ignore them
- **Version ID format** — uint32 stored big-endian in FDB keys; represented as decimal string in proto/S3 responses; "null" = version_id 0 in S3 spec

