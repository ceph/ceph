# SDK Limitations — Tests Blocked by Rust AWS SDK Constraints

This document tracks S3 test scenarios that cannot be translated using the Rust AWS SDK
(`aws-sdk-s3`, `aws-sdk-iam`, `aws-sdk-sts`) as-is. Each entry identifies the limitation,
the affected tests, and what would be needed to unblock them.

The Python boto3 SDK provides escape hatches (event hooks, raw header injection, bucket
name validation bypass) that the Rust SDK does not expose directly. Where the Rust SDK
does offer a mechanism (the `Intercept` trait), we have not yet implemented it.

## 1. ~~Missing `Content-Length: 0` on UploadPartCopy~~ (RESOLVED)

**Problem:** RGW requires a `Content-Length: 0` header on `UploadPartCopy` requests.
The Rust SDK omits this header entirely. RGW rejects the request with `411 MissingContentLength`.

**Resolution:** Fixed via `customize().mutate_request()` interceptor that injects
`Content-Length: 0` header before signing. Applied to both the `multipart_copy()`
helper in `src/fixtures.rs` and all inline `upload_part_copy()` call sites.
6 previously-ignored tests now pass. Committed in `de55405`.

**Key lesson:** When mutating request bodies/headers via interceptor, always update
`Content-Length` to match the new body length — otherwise the server reads past EOF
and drops the connection, causing the SDK to retry on transient errors for ~200s.

---

## 2. ~~No `If-Match` / `If-None-Match` on PutObject~~ (NOT BLOCKED)

**Original claim:** The Rust SDK's `PutObject` builder does not expose `if_match()` or
`if_none_match()` methods.

**Correction:** The Rust SDK (aws-sdk-s3 v1.132.0) *does* support `if_match()` and
`if_none_match()` on `PutObject`. These tests were already translated in
`tests/test_s3/conditional.rs` using native SDK methods — no interceptor needed.
This entry was based on an earlier, incorrect assessment.

---

## 3. IAM XML Element Order

**Problem:** RGW returns IAM XML responses with `<ResponseMetadata>` before the result
element (e.g., `<GetUserPolicyResult>`). The `aws-sdk-iam` XML parser expects the result
element first. The HTTP response is 200 OK, but the SDK fails to parse the body.

**Affected tests (4):**
- `test_iam_user_policy_list` — test_iam.rs:118
- `test_iam_user_policy_get` — test_iam.rs:164
- `test_iam_user_policy_get_invalid_policy` — test_iam.rs:197
- `test_iam_user_policy_get_multiple` — test_iam.rs:231

**Python equivalent:** boto3 handles both element orderings.

**Fix options:**
1. Fix RGW to emit the result element first (server-side fix).
2. Patch `aws-sdk-iam` XML parsing to be order-independent (SDK upstream PR).
3. Use a raw HTTP client (`reqwest`) and parse IAM XML manually for these tests.

**Unblocks:** 4 currently ignored tests, plus future IAM policy tests that read
response bodies.

---

## 4. Custom Header Injection (General)

**Problem:** Many Python tests use `client.meta.events.register('before-call.s3.*', ...)`
to inject arbitrary HTTP headers (e.g., `Transfer-Encoding: chunked`,
`x-amz-metadata-directive`, `x-amz-acl`, `x-amz-grant-full-control`). The Rust SDK
has no equivalent inline event hook.

**Affected test categories:**
- `test_headers.py` — all 48 tests require header injection
- `test_object_write_with_chunked_transfer_encoding` — test_s3.py:1592
- `test_bucket_policy_put_obj_copy_source_meta` — test_s3.py:12358 (injects
  `x-amz-metadata-directive`)
- `test_bucket_policy_put_obj_acl` — test_s3.py:12408 (injects `x-amz-acl`)
- `test_bucket_policy_put_obj_grant` — test_s3.py:12448 (injects
  `x-amz-grant-full-control`)
- `test_head_bucket_usage` — test_s3.py:1110 (injects `read-stats` query param)
- `test_head_bucket_extended` — test_s3.py:3362 (injects `read-stats` query param)
- `test_bucket_list_maxkeys_invalid` — test_s3.py:1234 (injects `&max-keys=blah`
  query param via event hook)
- `test_bucket_list_unordered` / `test_bucket_listv2_unordered` — test_s3.py:1127/1189
  (injects `&allow-unordered=true` query param — RGW extension)

**Fix:** Build a reusable interceptor framework:
- A `get_client_with_headers(headers: HashMap<String, String>)` factory that returns
  a client pre-configured with an interceptor injecting the given headers.
- Or a thread-local / `Arc<Mutex<>>` approach where test code sets headers before each call.
- For query parameter injection, the interceptor's `modify_before_signing` can also
  append to the request URI.

**Unblocks:** ~48 header tests, plus ~8 policy/listing tests requiring header or
query param injection.

---

## 5. Bucket Name Validation Bypass (Tenant-Qualified Names)

**Problem:** Cross-tenant bucket access in RGW uses colon-delimited names
(e.g., `"tenant:bucketname"` or `":bucketname"`). boto3 allows this by unregistering
its `validate_bucket_name` handler. The Rust SDK validates bucket names and rejects
colons.

**Affected tests (~2+):**
- `test_bucket_policy_different_tenant` — test_s3.py:11331
- `test_bucket_policy_tenanted_bucket` — test_s3.py:11397

**Fix:** SDK interceptor that overrides the bucket name in the HTTP request URI after
validation, or use `reqwest` directly for these tests.

**Unblocks:** Cross-tenant policy tests.

---

## 6. Type-Safe Serialization (Invalid Value Injection)

**Problem:** Some tests verify server-side rejection of invalid values — malformed
enum variants, bad date formats, etc. The Rust SDK enforces type safety at the
builder level: enums only accept valid variants, dates are always serialized as
valid ISO 8601. The malformed request never reaches the server.

**Affected tests:**
- `test_object_lock_put_obj_lock_invalid_mode` — test_s3.py:13024
  (sends `Mode: "abc"` and `Mode: "governance"` — case-sensitive rejection)
- `test_object_lock_put_obj_lock_invalid_status` — test_s3.py:13054
  (sends `ObjectLockEnabled: "Disabled"`)
- `test_object_lock_put_obj_retention_invalid_mode` — test_s3.py:13142
  (sends `Mode: "governance"` and `Mode: "abc"` for retention — SDK uses enum)
- `test_object_lock_put_legal_hold_invalid_status` — test_s3.py:13462
  (sends `Status: "abc"` for legal hold — SDK uses enum)
- ~~`test_lifecycle_set_invalid_date`~~ — test_s3.py:9050 **(RESOLVED)**
  Sends date string `'20200101'` instead of ISO format. Fixed via
  `customize().mutate_request()` interceptor that replaces the valid ISO date
  in the serialized XML body before signing. Committed in `5057692`.
- ~~`test_lifecycle_invalid_status`~~ — test_s3.py:9012 **(RESOLVED)**
  Sends `Status: "enabled"` / `"invalid"`. Fixed using `ExpirationStatus::from("enabled")`
  which creates an `Unknown` variant that serializes as-is. Committed in `cf82644`.

**Fix (for remaining tests):** Use `customize().mutate_request()` to replace values in the
serialized XML body before signing (proven approach). For enum-based tests,
`Type::from("invalid_value")` creates an `Unknown` variant. For type-constrained
fields (dates, integers), mutate the XML body string directly.

**Partially unblocked:** 2 of 6 tests resolved. Remaining 4 (object lock tests)
can use the same interceptor pattern.

---

## 7. SigV2 Signing

**Problem:** The Rust AWS SDK only supports SigV4. The Python suite has ~25 tests
marked `@pytest.mark.auth_aws2` that test SigV2 signing.

**Affected tests (~25):** All `auth_aws2`-marked tests in test_s3.py.

**Fix options:**
1. Implement a custom SigV2 signer using the SDK's `Intercept` trait.
2. Use `reqwest` with a manual SigV2 implementation.
3. Accept that SigV2 is deprecated and skip these tests.

**Unblocks:** ~25 SigV2 tests.

---

## 8. Object Key Validation (Empty/Whitespace Keys)

**Problem:** The Rust SDK validates that the `key` field is non-empty before sending
the request. A key consisting of a single space `" "` is rejected by the SDK's
`PutObjectEndpointParamsInterceptor` as a missing field.

**Affected tests:**
- `test_versioning_obj_create_versions_remove_special_names` — test_s3.py:7990
  (uses key `" "` among others — we dropped it from our translation)
- `test_bucket_create_special_key_names` — test_s3.py:5466
  (uses key `" "` among others — we dropped it from our translation)

**Fix:** SDK interceptor that bypasses key validation, or `reqwest` for edge-case
key tests.

**Unblocks:** Space-as-key and other whitespace key edge cases (~2 partial tests).

---

## 9. GetObjectAttributes Returns 403

**Problem:** The Rust SDK's `GetObjectAttributes` request triggers a 403 AccessDenied
response from RGW, even though the same operation succeeds with boto3 on the same
cluster. This is likely due to a difference in how the Rust SDK constructs and signs
the request (possibly incorrect URI canonicalization or missing headers).

**Affected tests (1+):**
- `test_get_object_attributes` — object_ops.rs (ignored, VERIFY)
- `test_get_multipart_object_attributes` — not yet translated
- `test_get_paginated_multipart_object_attributes` — not yet translated
- `test_get_single_multipart_object_attributes` — not yet translated
- `test_get_checksum_object_attributes` — not yet translated
- `test_get_versioned_object_attributes` — not yet translated
- `test_get_multipart_checksum_object_attributes` — not yet translated

**Fix:** Investigate the raw HTTP request differences between the Rust SDK and boto3
for `GetObjectAttributes`. May require an interceptor to fix headers/URI, or an
upstream SDK bug report.

**Unblocks:** ~7 object attribute tests.

---

## 10. No Raw Response Header Access

**Problem:** boto3 exposes raw HTTP response headers via
`response['ResponseMetadata']['HTTPHeaders']`, allowing tests to inspect
`content-range`, `x-rgw-object-count`, `x-rgw-bytes-used`, `x-amz-expiration`,
`x-amz-delete-marker`, `etag` (on error responses), and other headers. The Rust
SDK's typed output structs (`GetObjectOutput`, `HeadObjectOutput`, etc.) only
expose a subset of these as first-class fields. Headers not modeled in the output
struct are inaccessible without dropping down to raw HTTP.

**Affected tests (~15+):**
- `test_ranged_request_response_code` et al. — test_s3.py:7539-7598
  (assert `content-range` header format)
- `test_ranged_big_request_response_code` — test_s3.py:7557
  (assert `content-range` on 8MB ranged GET)
- `test_head_bucket_extended` — test_s3.py:3362
  (assert `x-rgw-object-count`, `x-rgw-bytes-used`)
- `test_lifecycle_expiration_header_*` — test_s3.py:9100+ (check `x-amz-expiration`)
- `test_object_raw_response_headers` — already translated but cannot verify
  all header values the Python test checks

**Fix:** Use the SDK's `customize()` API or `Intercept` trait to capture the raw
`http::Response` before deserialization. Alternatively, use `reqwest` for tests
that need raw header inspection.

**Unblocks:** ~15 tests that assert specific response header values.

---

## Summary

| # | Limitation | Fix approach | Status |
|---|---|---|---|
| 1 | Content-Length on UploadPartCopy | SDK interceptor | **RESOLVED** — 6 tests unblocked |
| 2 | If-Match/If-None-Match on PutObject | SDK interceptor | **NOT BLOCKED** — SDK supports natively |
| 3 | IAM XML element order | RGW fix or raw HTTP | Open — 4 tests |
| 4 | Custom header/query injection | Interceptor framework | Open — ~56 tests |
| 5 | Tenant-qualified bucket names | Interceptor or raw HTTP | Open — ~2 tests |
| 6 | Type-safe serialization (enums, dates) | Interceptor body mutation | **Partially resolved** — 2 of 6 |
| 7 | SigV2 signing | Custom signer or raw HTTP | Open — ~25 tests |
| 8 | Whitespace key validation | Interceptor bypass | Open — ~2 tests |
| 9 | GetObjectAttributes 403 | Investigate SDK request | Open — ~7 tests |
| 10 | No raw response header access | Interceptor or reqwest | Open — ~15 tests |
| | **Total remaining** | | **~115** |

Items 4 and 10 share a common solution: the `customize().mutate_request()` pattern
(proven in items 1 and 6). A systematic rollout would unblock ~70 more tests.

---

## Bypass Strategy: Three Layers

The limitations above fall into a spectrum from "send a slightly wrong value" to
"send an entirely custom S3 operation." Three approaches cover this spectrum,
layered from lightest to heaviest:

### Layer 1: SDK Interceptors (`Intercept` trait) — per-request mutation

The Rust AWS SDK provides the `Intercept` trait (`aws-smithy-runtime-api`) with
hooks at each stage of request processing:

- `modify_before_serialization` — mutate the SDK input type before XML generation
- `modify_before_signing` — mutate the serialized HTTP request (body, headers, URI)
  before SigV4 signs it — **mutations are included in the signature**
- `modify_before_transmit` — mutate after signing — useful for headers excluded
  from signing, but body changes will break signature verification

**Best for:** negative tests (invalid dates, malformed enums, missing fields),
custom header injection (`If-Match`, `Content-Length: 0`), query parameter
injection (`allow-unordered`, `read-stats`).

**Pattern:** Let the SDK build a valid request, then surgically edit the
serialized XML body or headers at `modify_before_signing`. For example, to send
an invalid lifecycle date:

1. Build a valid `LifecycleExpiration` with a real `DateTime`
2. Register an interceptor that replaces `<Date>2020-01-01T00:00:00Z</Date>`
   with `<Date>20200101</Date>` in the XML body
3. The SDK signs the mutated body, sends it, server rejects with 400

This is the direct Rust analogue of Python boto3's
`client.meta.events.register('before-call.s3.*', handler)` pattern.

**Covers:** Limitations 1, 2, 4, 5, 6, 8 (~80 tests).

### Layer 2: Raw HTTP + `aws-sigv4` — full request control

For cases where interceptor-based mutation is awkward (entirely custom XML
schemas, non-standard operations, binary protocols), bypass the SDK's operation
layer entirely:

- Use the `aws-sigv4` crate (already a transitive dependency) for SigV4 signing
- Construct XML bodies by hand (templates or builders)
- Send via `reqwest` (already in our dependency tree)

The existing `src/http.rs` already provides `raw_request`, `raw_request_body`,
and `post_object_form`. Adding a `signed_raw_request` function that handles
SigV4 would complete this layer.

**Best for:** entirely custom S3 operations, SigV2 tests (with a manual SigV2
implementation), tests needing raw response header access.

**Covers:** Limitations 3, 7, 9, 10 (~50 tests).

### Layer 3: Cargo `[patch]` — compile-time type extension

For production-style S3 dialect extensions (adding new operations or fields to
the SDK's type system), use Cargo's `[patch.crates-io]` to substitute specific
SDK crates with local modified copies. This is the approach used by the RGW
team's S3-Vectors project to integrate LanceDB APIs — the upstream Rust
project's types are extended at build time without maintaining a fork.

```toml
[patch.crates-io]
aws-sdk-s3 = { path = "patches/aws-sdk-s3" }
```

**Best for:** adding genuinely new S3 operations (e.g., `PutVectorIndex`),
relaxing type constraints across many tests, or when interceptor-based mutation
becomes too fragile across SDK version upgrades.

**Trade-off:** patches must be rebased when the SDK version is bumped. Worth it
for permanent dialect extensions; overkill for one-off negative tests.

### Recommended adoption order

1. **Interceptors first** — implement a shared `XmlBodyMutator` and
   `HeaderInjector` interceptor. This is the highest-leverage, lowest-risk
   change and unblocks the most tests immediately.

2. **SigV4-signed raw HTTP second** — add `signed_raw_request` to `src/http.rs`
   for the cases interceptors can't reach (custom operations, raw header
   inspection, SigV2).

3. **`[patch]` last** — reserve for if/when we need to test genuinely new S3
   operations, not just malformed versions of existing ones.

---

## 5. Payload Signing Mode Coverage

**Background:** The S3 protocol supports four `x-amz-content-sha256` modes:

| Mode | Value |
|------|-------|
| Signed payload | `<inline SHA256 hash>` |
| Unsigned payload | `UNSIGNED-PAYLOAD` |
| Streaming signed + trailer | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` |
| Streaming unsigned + trailer | `STREAMING-UNSIGNED-PAYLOAD-TRAILER` |

**SDK knobs:**
- `RequestChecksumCalculation::WhenRequired` — disables automatic trailing checksums
- `.customize().disable_payload_signing()` — sets `UNSIGNED-PAYLOAD`
- `.checksum_algorithm(ChecksumAlgorithm::Crc32)` — requests explicit checksum
- `ByteStream::read_from().path(file)` — creates a streaming (file-backed) body

**Actual behavior over HTTP (our test cluster):**

| Body type | Checksums | disable_payload_signing | x-amz-content-sha256 |
|-----------|-----------|-------------------------|----------------------|
| `Vec<u8>` | WhenSupported | no | inline SHA256 hash |
| `Vec<u8>` | WhenSupported | yes | `UNSIGNED-PAYLOAD` |
| `Vec<u8>` | WhenRequired | no | inline SHA256 hash |
| `Vec<u8>` | WhenRequired | yes | `UNSIGNED-PAYLOAD` |
| file-backed | WhenSupported | no | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` |
| file-backed | WhenSupported | yes | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (!) |
| file-backed | WhenRequired | no | `UNSIGNED-PAYLOAD` |
| file-backed | WhenRequired | yes | `UNSIGNED-PAYLOAD` |

Key observations:
1. **`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`** is reachable with file-backed
   streaming bodies and `WhenSupported` checksums. The checksum interceptor overrides
   `disable_payload_signing()` in this configuration.
2. **`STREAMING-UNSIGNED-PAYLOAD-TRAILER`** is NOT reachable over HTTP with the
   current Rust SDK — the checksum interceptor always upgrades to signed streaming.
   The Java and Go SDK suites cover this mode.
3. **`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`** (without `-TRAILER`) is NOT reachable —
   the SDK either uses the trailer variant or falls back to non-streaming.

**What we test:** 17 tests in `tests/test_s3/signing.rs` covering three of the
four modes: inline SHA256, `UNSIGNED-PAYLOAD`, and
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`. Tests use both `Vec<u8>` and
file-backed streaming bodies, with various checksum configurations, and verify
round-trip integrity via GET.

---

*This document should be updated whenever a new SDK limitation is discovered during
translation. Use `git log --all -p -- docs/sdk-limitations.md` to review the history
of additions.*
