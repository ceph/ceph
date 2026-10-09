# SDK Limitations and Workarounds

The Rust AWS SDK (`aws-sdk-s3`, `aws-sdk-iam`, `aws-sdk-sts`) lacks some escape
hatches that Python's boto3 provides. This document catalogs the limitations
encountered during test translation and the strategies used to overcome them.

## Resolved Limitations

### 1. Missing `Content-Length: 0` on UploadPartCopy

**Problem:** RGW requires `Content-Length: 0` on `UploadPartCopy`. The Rust SDK
omits it, causing `411 MissingContentLength`.

**Solution:** `customize().mutate_request()` interceptor injects the header
before signing. Applied to the `multipart_copy()` helper in `src/fixtures.rs`
and all inline `upload_part_copy()` call sites.

**Lesson:** When mutating request bodies/headers via interceptor, always update
`Content-Length` to match — otherwise the server reads past EOF and the SDK
retries for ~200s.

### 2. If-Match / If-None-Match on PutObject

**Original claim:** SDK lacks conditional write support.

**Correction:** The SDK (aws-sdk-s3 v1.132.0+) natively supports `if_match()`
and `if_none_match()` on `PutObject`. 33 conditional tests in
`tests/test_s3/conditional.rs` use the native API.

### 3. Custom Header / Query Parameter Injection

**Problem:** Python uses `client.meta.events.register('before-call.s3.*', ...)`
to inject arbitrary headers and query parameters. No Rust equivalent.

**Solution:** Two approaches depending on the test:
- **`customize().mutate_request()`** — for standard SDK operations that need
  extra headers or query params (e.g., `allow-unordered=true`, `read-stats`
  on listing/head operations). Used in bucket_list.rs (22 sites),
  bucket_logging.rs (14), bucket_policy.rs (14), multipart.rs (16), and others.
- **`sigv2_request()` via reqwest** — for tests that need full header control.
  The `tests/test_s3/headers.rs` module (21 tests) uses raw HTTP with manual
  SigV2 signing, matching the Python tests' approach.

### 4. Type-Safe Serialization (Invalid Value Injection)

**Problem:** Negative tests need to send malformed enum variants or bad date
formats. The SDK enforces type safety at the builder level.

**Solution:** Two patterns:
- `Type::from("invalid_value")` creates an SDK `Unknown` variant that serializes
  as-is (used for lifecycle `ExpirationStatus::from("enabled")`).
- `customize().mutate_request()` replaces values in the serialized XML body
  before signing (used for invalid dates like `20200101` instead of ISO format).

### 5. SigV2 Signing

**Problem:** The Rust SDK only supports SigV4. Python has ~25 `auth_aws2` tests.

**Solution:** Manual SigV2 implementation in `src/http.rs`: `sigv2_sign()`,
`sigv2_canonicalize_headers()`, `sigv2_canonicalize_resource()`,
`sigv2_request()`, and `presign_v2()`. Uses HMAC-SHA1 via the `hmac` and `sha1`
crates, sends requests via `reqwest`. The `tests/test_s3/headers.rs` module
uses this for all 21 header injection tests.

### 6. POST Object (Form Upload)

**Problem:** POST Object uses multipart form encoding, not the standard SDK
`PutObject` operation.

**Solution:** `reqwest` multipart form builder in `src/http.rs` (`PostForm`,
`post_object_form()`). 37 tests in `tests/test_s3/post_object.rs` use this,
testing policies, conditions, ACLs, and error cases.

### 7. GetObjectAttributes 403

**Problem:** Rust SDK's `GetObjectAttributes` triggered 403 from RGW while
boto3 succeeded on the same cluster.

**Solution:** Resolved — the test now passes without workarounds. Root cause
was likely an SDK version issue or request construction difference that was
fixed in a later SDK release.

### 8. SSE-KMS / SSE-S3 Encryption

**Problem:** Encryption tests were blocked on KMS infrastructure (Vault or GKLM).

**Solution:** HashiCorp Vault sidecar (`vault-vstart.sh`) provides a dev-mode
transit backend. 48 encryption tests cover SSE-S3 (default and explicit),
SSE-KMS, and SSE-C. Config sections `[s3 kms]` and `[s3 main]` carry the
key IDs.

## Open Limitations

### IAM XML Element Order

**Problem:** RGW returns IAM XML with `<ResponseMetadata>` before the result
element. The `aws-sdk-iam` parser expects the result element first, causing
parse failures on 200 OK responses.

**Affected tests:** 1 (`test_iam_user_policy_allow_actions` — ignored with
`VERIFY` prefix).

**Fix options:** Fix RGW XML emission order (preferred), or use raw HTTP
with manual XML parsing for affected IAM operations.

### Bucket Name Validation (Tenant-Qualified Names)

**Problem:** Cross-tenant bucket access uses colon-delimited names
(`tenant:bucketname`). The SDK rejects colons in bucket names.

**Status:** Not yet needed — no cross-tenant tests are currently translated.
When needed, `customize().mutate_request()` can override the bucket name in
the URI after validation.

### Whitespace Object Keys

**Problem:** The SDK rejects empty or whitespace-only object keys at the
builder level.

**Status:** Affected edge cases (key `" "`) were dropped from translations
of `test_versioning_obj_create_versions_remove_special_names` and
`test_bucket_create_special_key_names`. Could be resolved with an interceptor
that bypasses key validation.

### No Raw Response Header Access

**Problem:** Some tests need HTTP response headers not modeled in SDK output
structs (`content-range`, `x-rgw-object-count`, `x-amz-expiration`).

**Status:** Partially mitigated. `src/http.rs` provides `RequestHeaderCapture`
for capturing request headers via `customize()`. Response header access for
tests that need it uses `signed_request()` which returns the raw
`reqwest::Response`.

## Workaround Strategy Summary

| Layer | Mechanism | Use case | Tests using it |
|-------|-----------|----------|----------------|
| 1 | `customize().mutate_request()` | Header/query injection, XML body mutation | ~90 call sites across 11 modules |
| 2 | `reqwest` + manual SigV4 | Full request control, raw response headers | `signed_request()`, `signed_request_with_creds()` |
| 2 | `reqwest` + manual SigV2 | SigV2 auth tests, header injection tests | `sigv2_request()`, headers.rs (21 tests) |
| 2 | `reqwest` multipart form | POST Object | `post_object_form()`, post_object.rs (37 tests) |
| 3 | Feature-gated ignores | Backend-specific failures | `#[cfg_attr(feature = "fails_on_nsfs", ignore)]` |
