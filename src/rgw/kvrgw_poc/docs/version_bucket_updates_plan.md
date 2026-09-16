# Version Bucket Updates — Implementation Plan

## Summary

Implement `~/nextgen/version_bucket_updates.md` across C++ backend, Go frontend, and proto layer.

Core change: merge `kNoVersion=0` and `kNullVersionId=1` into `NULL_VERSION = max_uint32`.

---

## Version ID Mapping (internal ↔ wire ↔ S3)

| Internal (uint32) | Proto (string) | S3 API (string) | Meaning |
|---|---|---|---|
| `0xFFFFFFFF` (max_uint32) | `"4294967295"` | `"null"` | Null version (unversioned or suspended) |
| `0xFFFFFFFE` (max_uint32-1) | `"4294967294"` | `"4294967294"` | First real version |
| `0xFFFFFFFD` (max_uint32-2) | `"4294967293"` | `"4294967293"` | Second real version |
| ... decreasing ... | ... | ... | Newer versions get smaller IDs |
| `0` | never used | never used | Reserved (unused after migration) |

**Go translation rules:**
- S3 `"null"` → proto `"4294967295"` (when sending to C++)
- Proto `"4294967295"` → S3 `"null"` (when returning to client)
- Proto `""` or absent → omit `x-amz-version-id` header (versioning disabled)
- All other values pass through as-is

---

## C++ Changes

### constants.hpp

```cpp
// Remove:
inline constexpr uint32_t kNoVersion      = 0;
inline constexpr uint32_t kNullVersionId  = 1;

// Replace with:
inline constexpr uint32_t kNullVersion    = 0xFFFFFFFF;  // max_uint32
inline constexpr uint32_t kFirstVersionId = 0xFFFFFFFE;  // max_uint32 - 1
```

### service_impl.cpp — compute_new_version

```cpp
switch (versioning_state) {
case VERSIONING_ENABLED:
    if (old_o) {
        return {old_o->hdr.next_vid, old_o->hdr.next_vid - 1};
    }
    return {kFirstVersionId, kFirstVersionId - 1};
case VERSIONING_SUSPENDED:
    if (old_o) {
        return {kNullVersion, old_o->hdr.next_vid};
    }
    return {kNullVersion, kFirstVersionId};
case VERSIONING_DISABLED:
default:
    return {kNullVersion, kFirstVersionId};
}
```

### service_impl.cpp — displace_old_object

```cpp
if (versioning_state == VERSIONING_ENABLED) {
    // ALL old entries → V: (including NULL_VERSION)
    put(V:<old_vid>, old)
} else if (versioning_state == VERSIONING_SUSPENDED) {
    if (old_o.vid != kNullVersion) {
        put(V:<old_vid>, old)  // preserve real versions
    } else {
        move_object_to_g(old)  // destroy existing null
    }
    // Also destroy V:<NULL_VERSION> if it exists (one-null-version rule)
    if (V:<key><kNullVersion> exists) {
        move V:<key><kNullVersion> to G:
    }
} else {  // disabled
    move_object_to_g(old)
}
```

### service_impl.cpp — PutObject response

```cpp
// After successful write:
if (object_value.hdr.version_id != kNullVersion || object_value.hdr.next_vid != kFirstVersionId) {
    // Bucket has/had versioning — report version_id
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
// When disabled: vid=kNullVersion AND next_vid=kFirstVersionId → don't report
```

Wait — simpler: always report if bucket state != disabled. But we don't have bucket state at response time. Alternative: use the same heuristic but with new constant:
```cpp
// If version_id is kNullVersion and next_vid is kFirstVersionId → fresh unversioned, don't report
// Otherwise → report (versioning was active at some point)
if (!(object_value.hdr.version_id == kNullVersion && object_value.hdr.next_vid == kFirstVersionId)) {
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
```

Actually simpler: propagate versioning_state from the txn. See section below.

### service_impl.cpp — Propagate versioning_state from put functions

Add `uint8_t* out_versioning_state` parameter to `put_object_single_txn` and `put_object_phase3`:
```cpp
grpc::Status put_object_single_txn(..., ObjectValue& new_value, ..., uint8_t* out_versioning_state);
```

Inside, after `verify_bucket_in_txn`:
```cpp
if (out_versioning_state) *out_versioning_state = vb->versioning_state;
```

In PutObject handler:
```cpp
uint8_t vs = VERSIONING_DISABLED;
auto status = put_object_single_txn(..., &vs);
// or select_storage_tier(..., &vs);

if (vs != VERSIONING_DISABLED) {
    response->set_version_id(std::to_string(object_value.hdr.version_id));
}
```

### service_impl.cpp — DeleteObject response

Already done: returns `dm_version_id`. With new constants, suspended DM gets `vid=kNullVersion=0xFFFFFFFF`. Go translates to "null". Correct.

### service_impl.cpp — set_object_metadata (GetObject/HeadObject)

```cpp
void set_object_metadata(ObjectMetadata* metadata, const ObjectValue& value) {
    metadata->set_etag(value.etag_display());
    metadata->set_size(value.hdr.size);
    metadata->set_last_modified_unix(value.hdr.last_modified_sec);
    metadata->set_content_type(value.content_type);
    // Report version_id if versioning was ever active
    if (!(value.hdr.version_id == kNullVersion && value.hdr.next_vid == kFirstVersionId)) {
        metadata->set_version_id(std::to_string(value.hdr.version_id));
    }
}
```

### service_impl.cpp — DeleteObjectVersion

No change needed for Case 2 promotion logic — with `NULL_VERSION=max_uint32`, the null version sorts LAST in V: scan. Promotion always picks the smallest key (most recent real version). Null version is never accidentally promoted over a real version.

### service_impl.cpp — ListObjectVersions

No sorting change needed. V: entries with `kNullVersion=max_uint32` naturally sort last in big-endian byte order. The merge logic already puts O: first, then V: entries in scan order.

---

## Go Frontend Changes

### frontend/backend_kvrgw.go — Version ID translation

```go
const nullVersionInternal = "4294967295"  // max_uint32

func versionIDToInternal(s string) string {
    if s == "null" {
        return nullVersionInternal
    }
    return s
}

func versionIDToExternal(vid uint32) string {
    if vid == 0xFFFFFFFF {
        return "null"
    }
    return fmt.Sprintf("%d", vid)
}

func versionIDFromProto(s string) string {
    if s == "" {
        return ""
    }
    if s == nullVersionInternal {
        return "null"
    }
    return s
}
```

Update all callers of `versionIDToExternal` — currently converts `0 → "null"`. Change to `0xFFFFFFFF → "null"`.

### frontend/backend_kvrgw.go — GetObject/HeadObject

When metadata has `version_id` set:
- `"4294967295"` → include `VersionId: "null"` in response
- Other values → include as-is
- Empty/absent → omit header

---

## Migration

Existing FDB data has objects with `version_id=0` (old kNoVersion) and `version_id=1` (old kNullVersionId). Options:

**Option A: Online migration** — on first read, if vid ∈ {0, 1} and next_vid == max_uint32, treat as NULL_VERSION. Lazy-rewrite on next PUT.

**Option B: Clean start** — wipe data (`reload.sh --clean`). Acceptable for POC.

**Recommendation: Option B** for now. Note in test scripts that `--clean` is required after this change.

---

## Test Impact

- All existing versioning tests need `--clean` reload
- `versionIDToExternal(0)` calls in Go need updating to use new constant
- `test_version_get_put.sh` version IDs will change from `4294967295, 4294967294, ...` to `4294967294, 4294967293, ...` (first real version is now max_uint32-1)
- Null version in `test_list_object_versions.sh` Case 6 will report vid="null" (mapped from max_uint32)

---

## Files to Change

| File | Changes |
|---|---|
| `backend/src/constants.hpp` | Replace kNoVersion/kNullVersionId with kNullVersion/kFirstVersionId |
| `backend/src/service_impl.cpp` | compute_new_version, displace_old_object, PutObject response, set_object_metadata |
| `backend/src/service_impl.hpp` | Add out_versioning_state param to put functions |
| `frontend/backend_kvrgw.go` | Update translation constants (0xFFFFFFFF = "null") |
| `scripts/test_version_get_put.sh` | Update expected version IDs |
| `scripts/test_list_object_versions.sh` | Update expected null version value |

---

## Execution Steps (for agent)

After all code changes are made, execute these steps in order:

### Step 1: Build

```bash
cd /home/gbenhano/kv_poc
cd frontend && PATH=$PATH:~/go/bin protoc --go_out=pb --go_opt=paths=source_relative \
  --go-grpc_out=pb --go-grpc_opt=paths=source_relative -I ../proto ../proto/kvrgw.proto
go build -o ../build/kv-rgw-frontend .
cd ..
cmake --build build -j$(nproc)
```

Both must compile with zero errors.

### Step 2: Reload --clean

```bash
pkill -f kv-rgw-frontend; pkill -f kv-rgw-backend; pkill nginx; sleep 3
bash scripts/reload.sh --clean 1
```

Must show `S3: ok` and `Fast tests: ok`.

### Step 3: Run our versioning tests

```bash
bash scripts/test_version_get_put.sh
bash scripts/test_delete_bucket_versioning.sh
bash scripts/test_list_object_versions.sh
```

All three must PASS. If version IDs in test assertions changed (due to new constants), update the test scripts accordingly — the tests validate behavior not specific numeric IDs.

### Step 4: Run ceph-rgw versioning tests

```bash
cd ~/clean/ceph/src/test/rgw/s3-tests
S3TEST_CONF=/home/gbenhano/kv_poc/s3tests.conf python3.11 -m pytest \
  s3tests/functional/test_s3.py -k "test_versioning_bucket_create_suspend or \
  test_versioning_obj_create_read_remove or \
  test_versioning_obj_create_read_remove_head or \
  test_versioning_stack_delete_markers or \
  test_versioning_obj_plain_null_version_removal or \
  test_versioning_obj_plain_null_version_overwrite or \
  test_versioning_obj_plain_null_version_overwrite_suspended or \
  test_versioning_obj_suspend_versions or \
  test_versioning_obj_create_versions_remove_all or \
  test_versioning_obj_create_versions_remove_special_names or \
  test_versioning_obj_list_marker or \
  test_versioning_multi_object_delete or \
  test_versioning_multi_object_delete_with_marker or \
  test_versioning_multi_object_delete_with_marker_create" --tb=line -q
```

Target: **14/14 pass, 0 failures**.

The `test_versioning_obj_list_marker` teardown error (BucketNotEmpty) is a test cleanup issue — if it appears as ERROR but not FAILED, it's acceptable. The functional test itself passes.

### Step 5: Run --quick regression

```bash
pkill -f kv-rgw-frontend; pkill -f kv-rgw-backend; pkill nginx; sleep 3
bash scripts/run_test_plan.sh --quick
```

Must show PASSED (4/4 phases).

### Step 6: Report

Report results in this format:
```
--quick:            PASS/FAIL (N/N phases)
test_version_get_put:     PASS/FAIL
test_delete_bucket_versioning: PASS/FAIL
test_list_object_versions:     PASS/FAIL (N/N cases)
ceph-rgw versioning:      N passed, N failed, N errors
```
