# Ceph-RGW S3 Tests — Full Classification (760 tests)

## Summary

| Category | Count | Status |
|---|---|---|
| **Currently passing** | 114 | In `run_ceph_rgw_tests.sh` |
| **Should work (untested)** | ~80 | No missing features; need verification run |
| **Needs small fix** | ~40 | Missing: user metadata, conditional edge cases, delimiter pagination |
| **ACL/access control** | 88 | Needs real ACL implementation (currently stubbed) |
| **Bucket policy (advanced)** | 20 | Needs full IAM policy language |
| **Multipart upload** | 63 | Not implemented |
| **Encryption (SSE)** | 59 | Not implemented |
| **Object lock/retention** | 48 | Not implemented |
| **Lifecycle** | 47 | Not implemented |
| **Copy object** | 42 | Not implemented |
| **Post object (form upload)** | 40 | Not implemented |
| **Tagging** | 25 | Not implemented |
| **CORS** | 14 | Not implemented |
| **Presigned URLs** | 12 | Untested (may partially work via versitygw) |
| **Bucket logging** | 113 | Not implemented |
| **Checksum** | 7 | Not implemented |
| **Object attributes** | 8 | Not implemented |
| **Restore/Glacier** | 5 | Not implemented |
| **Ownership controls** | 5 | Stubbed, not enforced |
| **Other (torrent, IAM)** | 2 | Not applicable |

---

## Category 1: Not Supported (no plans for POC)

### Multipart Upload (63 tests)
`test_multipart_*`, `test_abort_multipart_*`, `test_list_multipart_*`

### Encryption (59 tests)
`test_*_sse_*`, `test_*_enc_*`, `test_*_kms_*`

### Object Lock / Retention (48 tests)
`test_*_lock_*`, `test_*_retention_*`, `test_*_governance_*`

### Lifecycle (47 tests)
`test_lifecycle_*`, `test_*_expiration_*`

### Copy Object (42 tests) — NEXT TO IMPLEMENT
`test_*_copy_*`

Will be implemented next. Includes server-side copy (same bucket / cross-bucket), copy with metadata replacement, copy with version ID source.

### Post Object / Form Upload (40 tests)
`test_post_object_*`

### Tagging (25 tests)
`test_*_tagging_*`, `test_*_tags_*`

### CORS (14 tests)
`test_*_cors_*`

### Bucket Logging (113 tests)
`test_*_logging_*`, `test_bucket_log_*`

### Checksum (7 tests)
`test_*_checksum_*`

### Object Attributes (8 tests)
`test_*_object_attributes*`

### Restore/Glacier (5 tests)
`test_*_restore_*`

---

## Category 2: Should Work — Needs Verification Run (~80 tests)

These use only features we support. Need a test run to confirm. Grouped by subcategory:

### Listing tests not yet in suite (~20)
- `test_bucket_list_delimiter_unreadable` / `test_bucket_listv2_delimiter_unreadable`
- `test_bucket_list_prefix_unreadable` / `test_bucket_listv2_prefix_unreadable`
- `test_bucket_list_unordered` / `test_bucket_listv2_unordered`
- `test_bucket_list_maxkeys_invalid`
- `test_bucket_list_return_data_versioning` (may need GetObjectAcl)

### Bucket CRUD (~10)
- `test_bucket_notexist`
- `test_bucket_get_location`
- `test_bucket_create_naming_bad_*` (expect errors for invalid names)
- `test_bucket_create_naming_dns_*`

### Object write/read (~15)
- `test_atomic_write_1mb/4mb/8mb`
- `test_atomic_read_1mb/4mb/8mb`
- `test_atomic_dual_write_1mb/4mb/8mb`
- `test_object_raw_get` / `test_object_raw_authenticated`

### Versioning (~10)
- `test_versioned_concurrent_object_create_and_remove`
- `test_versioned_concurrent_object_create_concurrent_remove`

### Delete operations (~10)
- Various `test_delete_object_*` with version conditions
- `test_multi_object_delete_*` variants

### Range (~5)
- `test_ranged_request_invalid_range`
- `test_ranged_request_response_code` variants already passing

---

## Category 3: Needs Small Fix (~40 tests)

### User metadata — `x-amz-meta-*` (5 tests)
Need to persist and return custom headers. Requires proto field + backend storage for variable metadata map.
- `test_object_set_get_metadata_none_to_good`
- `test_object_set_get_metadata_none_to_empty`
- `test_object_set_get_metadata_overwrite_to_empty`
- `test_object_set_get_unicode_metadata`
- `test_object_set_get_non_utf8_metadata`

### Response headers — Cache-Control, Expires (2 tests)
Need to store and return these standard HTTP headers.
- `test_object_write_cache_control`
- `test_object_write_expires`

### Conditional GET/PUT edge cases (6 tests)
Versitygw framework handles conditions but some paths fail. Need investigation.
- `test_get_object_ifmatch_failed` (412 on ETag mismatch)
- `test_get_object_ifnonematch_good` (304 on ETag match)
- `test_get_object_ifmodifiedsince_failed` (304 when not modified)
- `test_get_object_ifunmodifiedsince_good` (200 expected, gets error)
- `test_put_object_ifmatch_nonexisted_failed` (412 when no object)
- `test_put_object_ifnonmatch_overwrite_existed_failed` (412 when exists)

### Delimiter + pagination (4 tests)
Continuation token format issue for ListObjects v1 with non-"/" delimiters.
- `test_bucket_list_delimiter_prefix`
- `test_bucket_listv2_delimiter_prefix`
- `test_bucket_list_delimiter_prefix_underscore`
- `test_bucket_listv2_delimiter_prefix_underscore`

### Bucket recreate behavior (1 test)
- `test_bucket_recreate_not_overriding` — expects objects to survive bucket re-creation

---

## Category 4: ACL / Access Control (88 tests)

Currently stubbed (always FULL_CONTROL to owner). Real implementation requires:
- Per-object ACL storage
- Canned ACL support (private, public-read, etc.)
- Grant parsing and evaluation
- Anonymous access handling
- Alt-user / cross-account testing

### Examples:
- `test_access_bucket_*` (private, publicread, publicreadwrite)
- `test_bucket_acl_*`
- `test_object_acl_*`
- `test_bucket_acl_grant_*`

---

## Category 5: Presigned URLs (12 tests)

May partially work since versitygw handles signature validation. Untested.
- `test_object_raw_get_x_amz_expires_*`
- `test_object_raw_put_authenticated_expired`

---

## Category 6: Ownership Controls (5 tests)

Currently stubbed (always BucketOwnerEnforced). Need real enforcement.
- `test_create_bucket_bucket_owner_enforced`
- `test_create_bucket_bucket_owner_preferred`
- `test_create_bucket_object_writer`
- `test_put_bucket_ownership_*`
- `test_bucket_create_delete_bucket_ownership`

---

## Category 2 Verification Results

Ran ~70 tests from Category 2. Results:
- **32 passed** — added to suite (total now 142)
- **60 failed** — classified below

### Verified as Unsupported

| Test | Reason |
|---|---|
| `test_head_bucket_usage` (×2) | RGW-specific `X-Rgw-Object-Count`/`X-Rgw-Bytes-Used` headers |
| `test_object_content_encoding_aws_chunked` | AWS chunked transfer encoding (nginx/versitygw limitation) |
| `test_100_continue` | `Expect: 100-continue` auth check happens after 100 response (nginx behavior) |
| `test_expected_bucket_owner` | `x-amz-expected-bucket-owner` header not supported |
| `test_bucket_list_unordered` (×2) | `unordered` param not recognized |
| `test_object_raw_*` (14) | Raw HTTP presigned URL tests — signature handling differences |
| `test_bucket_create_naming_bad_*` (9) | No bucket name validation (we accept all names) |

### Fixable (small effort)

| Test | Fix Needed |
|---|---|
| `test_list_buckets_paginated` | Add `max-buckets` pagination to ListBuckets |
| `test_bucket_recreate_not_overriding` | Make CreateBucket idempotent (return OK if same owner) |
| `test_object_delete_key_bucket_gone` | Bucket cache staleness after delete |

### Conditional Delete/PUT — IMPLEMENTED (13 passing, 4 RGW deviations)

Implemented per AWS spec. 13 tests pass. 4 tests follow RGW's legacy behavior (pre-conditional-delete era) where DELETE on missing/DM + If-Match returns 204 instead of 404/412:

- `test_delete_object_if_match` — expects 204 for missing+IfMatch (AWS: 404)
- `test_delete_object_current_if_match` — expects 204 for DM+IfMatch:* (AWS: 412)
- `test_delete_object_if_match_last_modified_time` — same
- `test_delete_object_if_match_size` — same

Remaining 9 multi-delete tests need versitygw framework support (rejects conditional params on batch delete):

- `test_delete_object_if_match` (×3 variants: etag, last_modified_time, size)
- `test_delete_object_current_if_match` (×3)
- `test_delete_object_version_if_match` (×3)
- `test_delete_objects_if_match` (×3)
- `test_delete_objects_current_if_match` (×3)
- `test_delete_objects_version_if_match` (×3)
- `test_put_object_if_match` / `test_put_current_object_if_match` / `test_put_object_current_if_match`
- `test_put_current_object_if_none_match`

---

## Next Steps (priority order)

1. **Conditional Delete/PUT** — in progress, unlocks 22 tests
2. **CopyObject** — next major feature, unlocks 42 tests
3. **User metadata** (Category 3) — unlocks 5 tests, prerequisite for CopyObject metadata handling
4. **ACL** (Category 4) — large feature, unlocks 88 tests
