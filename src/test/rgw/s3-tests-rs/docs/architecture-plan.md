# Architecture — s3-tests-rs

## Context

Rust translation of the [Ceph s3-tests](https://github.com/ceph/s3-tests)
Python test suite (~970 tests, pytest/boto3), plus tests ported from Ceph-tree
Python suites (bucket notifications, bucket logging, etc.). Goals: performance
(10x faster parallel execution), type safety, and comprehensive S3 API
conformance testing for any production RGW backend (rados, posix, nsfs).

Current state: 827 tests across 22 modules (808 active + 19 ignored).

## Stack

- **`aws-sdk-s3`** / `aws-sdk-iam` / `aws-sdk-sts` — Rust-native async AWS SDK (tokio)
- **`reqwest`** — raw HTTP for POST Object, SigV2 requests, header tests, and
  presigned URL verification
- **`cargo-nextest`** — test runner with per-test process isolation and parallelism
- **`configparser`** — parses the same `s3tests.conf` INI format as the Python suite

## Project Layout

```
s3-tests-rs/
  Cargo.toml
  src/
    lib.rs              # re-exports
    config.rs           # INI config → S3TestConfig singleton (OnceCell<Arc<>>)
    client.rs           # ~20 client factory functions (S3, IAM, STS)
    cleanup.rs          # nuke_bucket, nuke_prefixed_buckets
    fixtures.rs         # get_new_bucket, setup/teardown, multipart helpers
    error.rs            # expect_s3_err!, assert_s3_err! macros
    random.rs           # generate_random (chunked random data)
    policy.rs           # JSON policy builder
    http.rs             # raw HTTP, SigV4/SigV2 signing, presigned URLs, CORS
  tests/
    test_s3/            # 18 modules, 723 tests:
      acl.rs            #   57 (canned ACLs, grants, cross-user, bucket ownership)
      bucket_list.rs    #   94 (list v1/v2, delimiters, prefixes, unordered)
      bucket_logging.rs #   59 (logging config, flush, auth type, ACL)
      bucket_policy.rs  #   43 (allow/deny, conditions, public access block)
      conditional.rs    #   33 (If-Match/None-Match/Modified/Unmodified on GET and PUT)
      copy.rs           #   18 (same/cross bucket, versioned, conditional, metadata)
      cors.rs           #   23 (set/delete, wildcard, headers, multi-rules)
      encryption.rs     #   48 (SSE-S3 default/explicit, SSE-KMS, SSE-C)
      headers.rs        #   21 (raw header injection via SigV2 reqwest)
      lifecycle.rs      #   45 (rules, expiration, noncurrent, filters, tags)
      multipart.rs      #   42 (upload, abort, list, copy-part, sizes, metadata)
      object_lock.rs    #   39 (retention, legal hold, governance bypass, versioning)
      object_ops.rs     #   99 (CRUD, range, metadata, checksums, attributes)
      post_object.rs    #   37 (form-post via reqwest, policies, conditions)
      signing.rs        #   17 (payload signing modes, streaming, checksums)
      stress.rs         #    2 (cache stress, concurrent put/delete/list)
      tagging.rs        #   16 (bucket/object tags, multipart, head tag count)
      versioning.rs     #   30 (create/suspend, delete markers, multi-delete, ACL)
    test_iam/           # 6 modules, 73 tests:
      user.rs           #   18 (user CRUD, path, boundary conditions)
      user_policy.rs    #   26 (attach/detach, allow/deny bucket and object actions)
      group.rs          #    8 (group CRUD, membership, policy)
      role.rs           #   10 (role CRUD, assume-role policy)
      oidc.rs           #    5 (OIDC provider CRUD)
      sts.rs            #    6 (AssumeRole, cross-account, WebIdentity)
    test_rgw/           # 2 modules, 5 tests:
      admin.rs          #    0 (admin ops — placeholder)
      quota.rs          #    5 (user/bucket quota)
    test_sns.rs         #   26 (Kafka delivery, filters, metadata, lifecycle events)
```

## Key Design Decisions

### Config — reuse the same INI file
Parses `s3tests.conf` (same format, same env var `S3TEST_CONF`) via the
`configparser` crate. Global singleton (`OnceCell<Arc<S3TestConfig>>`). Both
Python and Rust suites run against the same config — no separate setup needed.

### Client factories — builder pattern
One internal `build_s3_client(access_key, secret_key)` helper, then ~20 public
functions (`get_client()`, `get_alt_client()`, `get_iam_root_client()`, etc.)
that pull credentials from config. `force_path_style(true)` + `endpoint_url()`
for RGW compatibility. Path style is configurable via `[DEFAULT] path_style`.

### Bucket names — atomic counter
`AtomicU64` replaces Python's `itertools.count()`. Unique per process, no
locking needed under nextest's process-per-test model.

### Cleanup — explicit async, not Drop
`TestGuard::setup()` at test start, dropped at test end. `nuke_bucket()` deletes
all versions/delete-markers in batches of 128, handles object lock governance
bypass with retention waits.

### Error testing — macros
```rust
expect_s3_err!(client.put_object()...send().await) → S3ErrorInfo { status, error_code }
assert_s3_err!(expr, 404, "NoSuchBucket") → asserts both, returns S3ErrorInfo
```
Extracts HTTP status + S3 error code from `SdkError<E>` via `ProvideErrorMetadata`.

### SDK workarounds — three layers
The Rust AWS SDK lacks some escape hatches that boto3 provides (raw header
injection, bucket name validation bypass, SigV2 signing). The suite uses three
strategies to work around these, from lightest to heaviest:

1. **`customize().mutate_request()`** — intercept the serialized HTTP request
   before signing. Used extensively for `Content-Length: 0` injection on
   `UploadPartCopy`, query parameter injection (`allow-unordered`, `read-stats`),
   XML body mutation (invalid dates/enums for negative tests), and custom header
   injection on standard SDK operations.

2. **Raw HTTP via `reqwest` + manual SigV4/SigV2** — `src/http.rs` provides
   `signed_request()` (SigV4) and `sigv2_request()` (SigV2) for tests that need
   full control: POST Object form uploads, SigV2 auth tests, raw header injection
   tests, presigned URL tests, and CORS preflight verification.

3. **Feature-gated conditional ignores** — `#[cfg_attr(feature = "fails_on_nsfs", ignore)]`
   for tests that pass on rados but fail on a specific backend due to unimplemented
   features (e.g., bucket logging on nsfs).

### Marker mapping
- Module paths provide category grouping (nextest filters: `test(encryption::)`)
- `#[ignore = "reason"]` for unconditional skips (RGW bugs, missing infra)
- `#[cfg_attr(feature = "...", ignore)]` for backend-specific skips
- `nextest.toml` filter sets for marker-equivalent groups

### Concurrency tests
Python's `threading.Thread` → `tokio::spawn` with `JoinSet`.

## Source Mapping

| Rust module | Python source |
|---|---|
| `src/config.rs` | `s3tests/functional/__init__.py` (config sections) |
| `src/client.rs` | `s3tests/functional/__init__.py` (client factories) |
| `src/cleanup.rs` | `s3tests/functional/__init__.py` (nuke_bucket, setup/teardown) |
| `src/fixtures.rs` | `s3tests/functional/__init__.py` (bucket creation helpers) |
| `src/error.rs` | `s3tests/functional/utils.py` (error extraction) |
| `src/http.rs` | raw HTTP, SigV2 — no direct Python equivalent |
| `tests/test_s3/*.rs` | `s3tests/functional/test_s3.py` (752 tests) |
| `tests/test_s3/headers.rs` | `s3tests/functional/test_headers.py` (48 tests) |
| `tests/test_iam/*.rs` | `s3tests/functional/test_iam.py` (90 tests) |
| `tests/test_sns.rs` | `src/test/rgw/bucket_notification/test_bn.py` (87 tests) |

## Sidecar Infrastructure

The suite uses `rgw-vstart.sh` and companion scripts to stand up local
services needed by specific test modules:

| Sidecar | Script | Tests enabled |
|---------|--------|---------------|
| Keycloak | `keycloak-vstart.sh` | OIDC, WebIdentity (STS) |
| HashiCorp Vault | `vault-vstart.sh` | SSE-KMS, SSE-S3 encryption |
| Kafka | `kafka-vstart.sh` | SNS notification delivery |

Future sidecars may include Keystone (service tokens), Elasticsearch
(metadata sync), and D4N (distributed cache).

## Verification

```bash
S3TEST_CONF=/path/to/s3tests.conf cargo nextest run
```

For backend-specific runs:
```bash
cargo nextest run --features fails_on_nsfs    # skip nsfs-unsupported tests
cargo nextest run --features fails_on_dbstore  # skip dbstore-unsupported tests
```

Stress tests are ignored by default; run explicitly:
```bash
cargo nextest run -E 'test(/^stress::/)' --run-ignored=all --test-threads=1
```
