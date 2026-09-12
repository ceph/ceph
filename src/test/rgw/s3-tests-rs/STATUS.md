# s3-tests-rs — Porting Status (2026-06-08)

## Summary

The Python s3-tests suite (`s3tests/functional/`) contains ~976 test
functions across 7 files.  The Rust suite contains ~708 tests in
`tests/test_s3/` plus additional tests in `test_iam.rs` and
`test_sns.rs`.  Approximately 129 tests from the main Python file
(`test_s3.py`) have not been ported.  80 tests in the Rust suite are
new (no Python equivalent).

The separate Python files contribute additional unported coverage:

| Python file | Tests | Ported to Rust |
|-------------|-------|----------------|
| test_s3.py | 757 | ~628 (129 remaining) |
| test_iam.py | 90 | ~26 (partial) |
| test_headers.py | 48 | ~21 (partial, merged into headers.rs) |
| test_s3select.py | 38 | 0 |
| test_sts.py | 37 | 0 |
| test_sns.py | 5 | 4 |

## Unported tests from test_s3.py by category

### Bucket logging (56 tests)

Cleanup, concurrent flush, key filter, notupdating, request_id, and
additional configuration variants not yet ported.  Most require the
bucket logging rollover infrastructure.

### Encryption — SSE-KMS / SSE-S3 (30 tests)

Bucket-level encryption config (GET/PUT/DELETE), encrypted copy,
encrypted transfer, SSE-KMS policy conditions, and multipart with
SSE-KMS.  The Rust suite covers SSE-C but is missing the KMS/S3
server-side variants.

Tests include:
- `test_put_bucket_encryption_{kms,s3}` — bucket default encryption
- `test_get_bucket_encryption_{kms,s3}` — read back config
- `test_delete_bucket_encryption_{kms,s3}` — remove config
- `test_sse_{kms,s3}_*` — object-level SSE-KMS/S3 operations
- `test_encrypted_transfer_*` — end-to-end encrypted PUT/GET
- `test_copy_enc`, `test_copy_part_enc` — encrypted CopyObject

### Bucket policy (8 tests)

- `test_bucket_policy_different_tenant` — cross-tenant policy
- `test_bucket_policy_tenanted_bucket` — tenanted bucket naming
- `test_bucket_policy_deny_self_denied_policy{,_confirm_header}`
- `test_bucket_policy_put_obj_kms_noenc` — KMS policy with no enc
- `test_bucket_policy_put_obj_{kms_s3,s3_kms}` — mixed enc policies
- `test_bucket_policy_set_condition_operator_end_with_IfExists`
- `test_head_object_404_with_policy_prefix`

### Object attributes (4 tests)

The `GetObjectAttributes` API (newer S3 operation):
- `test_get_multipart_object_attributes`
- `test_get_paginated_multipart_object_attributes`
- `test_get_multipart_checksum_object_attributes`
- `test_get_sse_c_encrypted_object_attributes`

### Conditional PUT (8 tests)

- `test_put_object_ifmatch_{good,failed}`
- `test_put_object_ifmatch_{nonexisted_failed,overwrite_existed_good}`
- `test_put_object_ifnonmatch_{good,failed}`
- `test_put_object_ifnonmatch_{nonexisted_good,overwrite_existed_failed}`

Note: some of these may overlap with the Rust `conditional` module
under different names.  Needs verification.

### Multipart edge cases (3 tests)

- `test_multipart_resend_first_finishes_last` — part reorder
- `test_multipart_upload_complete_without_create` — complete w/o init
- `test_multipart_upload_resend_part` — part resend idempotency

### Usage / stats (2 tests)

- `test_account_usage`
- `test_head_bucket_usage`

### Atomic writes (3 tests)

- `test_atomic_conditional_write_1mb`
- `test_atomic_dual_conditional_write_1mb`
- `test_atomic_write_bucket_gone`

### Other (15 tests)

- `test_100_continue_error_retry` — 100-Continue handling
- `test_bucket_head_extended` — extended HEAD response
- `test_object_copy_versioning_multipart_upload` — versioned MPU copy
- `test_object_requestid_matches_header_on_error` — RequestId check
- `test_ranged_request_return_trailing_bytes_response_code`
- `test_ranged_request_skip_leading_bytes_response_code`
- `test_read_through` — read-through cache
- `test_restore_noncur_obj` — glacier restore (non-current)
- `test_restore_object_permanent` — glacier restore (permanent)
- `test_restore_object_temporary` — glacier restore (temporary)
- `test_versioning_stack_delete_merkers` — stacked delete markers

## Entirely unported Python files

### test_sts.py (37 tests)

STS AssumeRole, AssumeRoleWithWebIdentity, GetSessionToken.
Requires OIDC IdP (Keycloak) integration.  None ported.

### test_s3select.py (38 tests)

S3 Select (SQL queries over objects).  Requires the s3select
module to be built and enabled.  None ported.

### test_iam.py (90 tests, ~26 ported)

IAM user policies.  Partially ported to `test_iam.rs`.  The Rust
SDK has an XML element ordering issue that blocks 7 tests (see
`docs/sdk-limitations.md`).  Remaining ~64 tests not yet ported.

### test_headers.py (48 tests, ~21 ported)

SigV2 header validation.  Partially ported to `headers.rs`.
Remaining ~27 tests not yet ported.

## Porting priorities for nsfs/posix drivers

For filesystem-backed drivers, the most valuable unported tests are:

1. **Conditional PUT** (8 tests) — may already be covered under
   different names, but should be verified
2. **Multipart edge cases** (3 tests) — resend/reorder resilience
   exercises driver-level idempotency
3. **Object attributes** (4 tests) — newer API, exercises metadata
   paths
4. **Atomic writes** (3 tests) — concurrent write correctness

Lower priority (require infrastructure not available in nsfs):
- Encryption (30) — needs KMS; SSE-C is already covered
- Bucket logging (56) — not implemented in nsfs
- STS (37) — needs OIDC IdP
- S3 Select (38) — needs s3select module
- Glacier restore (3) — needs cloud tiering
