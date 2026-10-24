# s3-tests-rs

Rust translation of the [Ceph s3-tests](https://github.com/ceph/s3-tests) S3 API
compatibility test suite. Targets Ceph RGW (and any S3-compatible endpoint) using
the Rust-native AWS SDK.

**Current status:** 767 tests across 19 modules (719 active + 48 ignored).

## Stack

- **[aws-sdk-s3](https://crates.io/crates/aws-sdk-s3)** / `aws-sdk-iam` / `aws-sdk-sts` — async Rust AWS SDK (tokio)
- **[cargo-nextest](https://nexte.st/)** — test runner with per-test process isolation and parallelism
- **`#[tokio::test]`** — async test harness

## Prerequisites

1. **Rust toolchain** (stable, 2021 edition)
2. **cargo-nextest**:
   ```bash
   cargo install --locked cargo-nextest
   ```
3. **A running S3-compatible endpoint** (Ceph RGW via `vstart.sh`, or AWS, or another implementation)
4. **A config file** — same INI format as the Python suite. Copy `s3tests.conf.SAMPLE`
   from this repo and edit it for your cluster.

## Quick start

```bash
# 1. Copy and edit the sample config
cp s3tests.conf.SAMPLE my-s3tests.conf
$EDITOR my-s3tests.conf

# 2. Export it
export S3TEST_CONF=/path/to/my-s3tests.conf

# 3. Run all tests
cargo nextest run

# 4. See results
```

The suite runs a cleanup step automatically before the first test (via a
nextest setup script). This deletes any leftover buckets from prior runs.

## Configuration

The config file uses INI format, identical to the Python s3-tests. See
`s3tests.conf.SAMPLE` for a fully-commented example. The key sections are:

| Config section | Required | Purpose |
|---|---|---|
| `[DEFAULT]` | yes | `host`, `port`, `is_secure`, `ssl_verify` |
| `[fixtures]` | yes | `bucket prefix` template (`{random}` is replaced with random chars) |
| `[s3 main]` | yes | Primary test user: `access_key`, `secret_key`, `display_name`, `user_id`, `email` |
| `[s3 alt]` | yes | Secondary user for cross-user ACL/policy tests |
| `[s3 tenant]` | yes | Tenanted user for multi-tenancy tests |
| `[iam]` | yes | IAM service credentials for user-policy tests |
| `[iam root]` | yes | IAM account root user |
| `[iam alt root]` | yes | IAM account root in a different account |
| `[s3 cloud]` | no | Cloud-tier transition endpoint (if testing cloud transitions) |

### Optional settings in `[s3 main]`

```ini
# KMS key IDs for SSE-KMS tests (requires Vault or GKLM)
kms_keyid = 01234567-89ab-cdef-0123-456789abcdef

# Storage classes for transition tests
storage_classes = "LUKEWARM, FROZEN"

# Lifecycle debug interval — controls how fast LC processes rules (seconds)
# Must match vstart.sh: -o rgw_lc_debug_interval=10
lc_debug_interval = 10
```

### RGW vstart.sh options

For a full-featured test run, start the cluster with:

```bash
MON=1 OSD=1 MDS=0 MGR=1 RGW=1 ../src/vstart.sh -n -d \
  -o rgw_lc_debug_interval=10 \
  -o rgw_bucket_logging_object_roll_time=5 \
  -o rgw_sts_key=abcdefghijklmnop \
  -o rgw_s3_auth_use_sts=true
```

With `vstart.sh`, all required users (`[s3 main]`, `[s3 alt]`, `[s3 tenant]`,
`[iam]`, `[iam root]`, `[iam alt root]`) are created automatically with the
credentials shown in `s3tests.conf.SAMPLE`.

### Storage classes (for lifecycle transition tests)

Lifecycle transition tests require storage classes to be created in the
cluster. These must match the `storage_classes` setting in `s3tests.conf`:

```bash
# Add to zonegroup
sudo radosgw-admin zonegroup placement add \
  --rgw-zonegroup default --placement-id default-placement \
  --storage-class LUKEWARM
sudo radosgw-admin zonegroup placement add \
  --rgw-zonegroup default --placement-id default-placement \
  --storage-class FROZEN

# Add to zone (must also have entries here, or RGW reports
# "non existent dest placement" at lifecycle processing time)
sudo radosgw-admin zone placement add \
  --rgw-zone default --placement-id default-placement \
  --storage-class LUKEWARM --data-pool default.rgw.buckets.data
sudo radosgw-admin zone placement add \
  --rgw-zone default --placement-id default-placement \
  --storage-class FROZEN --data-pool default.rgw.buckets.data

sudo radosgw-admin period update --commit
```

Then restart RGW. Verify with:

```bash
sudo radosgw-admin zonegroup placement list
sudo radosgw-admin zone placement list
```

The corresponding `s3tests.conf` setting:

```ini
[s3 main]
storage_classes = "LUKEWARM, FROZEN"
```

## Running tests

### Environment variable

All commands below assume `S3TEST_CONF` is set:

```bash
export S3TEST_CONF=/path/to/s3tests.conf
```

### Run everything

```bash
# All active tests (excludes #[ignore]'d tests)
cargo nextest run

# Including ignored tests (e.g., SSE-KMS tests you've now configured)
cargo nextest run --run-ignored all
```

### Run a specific category (nextest profiles)

Profiles are defined in `.config/nextest.toml`. Each runs only the tests in
the named module:

```bash
cargo nextest run -P bucket-list       # ListObjects v1/v2, delimiters, prefixes
cargo nextest run -P object-ops        # CRUD, range, metadata, bucket naming
cargo nextest run -P multipart         # multipart upload/abort/list/copy
cargo nextest run -P versioning        # versioning, delete markers, null versions
cargo nextest run -P copy              # CopyObject, metadata, cross-bucket
cargo nextest run -P acl               # bucket/object ACLs, grants, cross-user
cargo nextest run -P tagging           # bucket/object tags, limits
cargo nextest run -P conditional       # if-match, if-modified-since, etc.
cargo nextest run -P encryption        # SSE-S3, SSE-C (many need SSE config)
cargo nextest run -P bucket-policy     # bucket policies, tag conditions
cargo nextest run -P object-lock       # lock config, retention, legal hold
cargo nextest run -P cors              # CORS configuration
cargo nextest run -P lifecycle         # lifecycle rules, expiration
cargo nextest run -P post-object       # POST (form-based) uploads
cargo nextest run -P bucket-logging    # bucket logging (needs rollover time)
cargo nextest run -P headers           # SigV2 header validation tests
cargo nextest run -P signing           # payload signing mode matrix
cargo nextest run -P sns               # SNS topic and notification config
cargo nextest run -P iam               # IAM user policies
cargo nextest run -P basic             # bucket-list + object-ops + copy + conditional + cors + acl
cargo nextest run -P all               # everything, including infra-dependent
```

### Run a specific test

```bash
# By full name
cargo nextest run -E 'test(=bucket_list::test_bucket_list_empty)'

# By substring
cargo nextest run -E 'test(~versioning_obj_suspend)'

# All tests in a module
cargo nextest run -E 'test(multipart::)'

# Combine filters
cargo nextest run -E 'test(lifecycle::) & test(~expiration)'
```

### Run ignored tests

Tests are `#[ignore]`'d when they require infrastructure not universally
available (KMS, OIDC, etc.). Once you've set up the dependency:

```bash
# Run only ignored tests in a category
cargo nextest run -P encryption --run-ignored ignored-only

# Run both active and ignored
cargo nextest run -P encryption --run-ignored all
```

### Debugging a single test

`cargo nextest` runs tests in subprocesses, so it can't attach a debugger.
Use `cargo test` with `--no-run` to build the test binary, then run it under
`rust-gdb`:

```bash
# Build without running — prints the test binary path
S3TEST_CONF=../s3tests.conf cargo test --test test_s3 lifecycle::test_lifecycle_empty_filter --no-run

# Run under rust-gdb (use the binary path from the build output)
S3TEST_CONF=../s3tests.conf rust-gdb --args ./target/debug/deps/test_s3-HASH lifecycle::test_lifecycle_empty_filter
```

In gdb:
```
b test_lifecycle_empty_filter
r
```

Replace `HASH` with the actual hash suffix printed by `--no-run`, and
substitute the test name you want to debug.

### Fast-fail on server crash

When testing against a local RGW that may crash (e.g., an in-development SAL
backend), set `S3TEST_FAST_FAIL=1` to abort the run immediately when the
server becomes unreachable:

```bash
S3TEST_FAST_FAIL=1 cargo nextest run --no-fail-fast
```

Each test does a TCP connect check before running. On the first unreachable
test, a global flag is set and all subsequent tests panic instantly without
attempting a connection. This avoids wasting minutes sending requests at a
dead server.

### Manual cleanup

If tests are interrupted or you want to purge leftover buckets manually:

```bash
cargo run --example cleanup
```

### Backend-specific test filtering

Tests known to fail on specific backends are conditionally compiled using
cargo features, mirroring the Python suite's `@pytest.mark.fails_on_*` markers.

```bash
# Default: skip tests that fail on RGW (most common target)
cargo nextest run

# Running against AWS: skip AWS-known-failures instead
cargo nextest run --no-default-features --features fails_on_aws

# Running against dbstore
cargo nextest run --no-default-features --features fails_on_dbstore

# Running against nsfs (hierarchical filesystem namespace)
cargo nextest run --no-default-features --features fails_on_nsfs

# No filtering — run everything regardless of backend
cargo nextest run --no-default-features
```

| Feature | Default | Tests skipped | Notes |
|---|---|---|---|
| `fails_on_rgw` | **yes** | ~8 | RGW-specific failures |
| `fails_on_aws` | no | ~130 | Behaviors that differ from AWS (includes bucket logging) |
| `fails_on_dbstore` | no | ~178 | Features not supported by dbstore |
| `fails_on_nsfs` | no | 2 | Hierarchical namespace: file-as-prefix conflict (see below) |

#### nsfs file-as-prefix limitation

The nsfs driver maps S3 object keys to real filesystem directory
hierarchies — the `/` delimiter in a key creates actual directories.
This exposes the POSIX directory namespace constraint: a name in a
directory is either a file or a subdirectory, not both.  `PUT foo/bar`
creates file `bar` inside directory `foo`, but a subsequent
`PUT foo/bar/xyzzy` requires `bar` to be a directory, which fails
with `ENOTDIR`.

Other filesystem-backed drivers (e.g. posixdriver) avoid this by
flattening key paths.  Filesystems with richer namespace semantics
(resource forks, alternate data streams) could also sidestep this, but
the standard POSIX API does not expose such mechanisms.  The noobaa
nsfs implementation, which uses the same hierarchical mapping, has the
same limitation.  Tests tagged `fails_on_nsfs` exercise key patterns
that trigger this conflict.

## Test counts by module

| Module | Active | Ignored | Total |
|---|---|---|---|
| `bucket_list` | 94 | 0 | 94 |
| `object_ops` | 87 | 7 | 94 |
| `acl` | 51 | 6 | 57 |
| `object_lock` | 39 | 0 | 39 |
| `post_object` | 37 | 0 | 37 |
| `multipart` | 35 | 1 | 36 |
| `bucket_policy` | 36 | 5 | 41 |
| `lifecycle` | 40 | 0 | 40 |
| `conditional` | 33 | 0 | 33 |
| `versioning` | 29 | 0 | 29 |
| `encryption` | 24 | 10 | 34 |
| `test_iam` | 19 | 7 | 26 |
| `cors` | 23 | 0 | 23 |
| `headers` | 13 | 8 | 21 |
| `signing` | 17 | 0 | 17 |
| `copy` | 15 | 3 | 18 |
| `tagging` | 16 | 0 | 16 |
| `bucket_logging` | 107 | 1 | 108 |
| `test_sns` | 4 | 0 | 4 |
| **Total** | **719** | **48** | **767** |

## Infrastructure constraints

Some S3 features require external services not present in a minimal test cluster.
Tests that need them are `#[ignore]`'d with a reason string.

| Dependency | Required for | # ignored |
|---|---|---|
| **KMS (Vault or GKLM)** | SSE-KMS encryption | 9 |
| **OIDC IdP (Keycloak)** | STS AssumeRoleWithWebIdentity | 0 (not yet translated) |
| **Kafka broker** | S3/SNS bucket notifications | 0 (not yet translated) |
| **Virtual-host DNS** | Virtual-hosted bucket URLs | 0 (not yet translated) |
| **Bucket logging rollover** | Bucket logging flush/ACL/auth tests | 3 |
| **Bucket logging** | Bucket logging tests | 1 |

Tests marked `#[ignore = "VERIFY: ..."]` fail both in this suite *and* in
the Python s3-tests on the same cluster. These are pre-existing RGW behavior
differences, not translation bugs.

## Known SDK/RGW interop issues

1. **Multipart copy + Content-Length**: RGW requires `Content-Length: 0` on
   `UploadPartCopy`. The Rust SDK omits it. **Resolved** via `customize().mutate_request()`
   interceptor.

2. **IAM XML element order**: RGW returns `<ResponseMetadata>` before
   `<GetUserPolicyResult>`. The Rust SDK parser expects the result first.
   Requests succeed (HTTP 200) but parsing fails. (7 IAM tests ignored)

3. **Transfer-Encoding chunked**: The Rust SDK's aws-chunked signing
   conflicts with `Transfer-Encoding: chunked`. (1 test ignored)

See `docs/sdk-limitations.md` for the full list and bypass strategies
(interceptors, raw HTTP + SigV4, cargo patch).

## Project layout

```
s3-tests-rs/
  s3tests.conf.SAMPLE       # example config (same format as Python suite)
  src/                       # shared library infrastructure
    config.rs                #   INI config parser (reuses s3tests.conf)
    client.rs                #   S3/IAM/STS client factories
    cleanup.rs               #   bucket cleanup (versioned, object-lock aware)
    fixtures.rs              #   bucket/object creation, multipart upload helpers
    error.rs                 #   S3ErrorInfo extraction, assert macros
    http.rs                  #   raw HTTP, SigV2/V4 signing, POST form uploads
    policy.rs                #   JSON policy builder for bucket policy tests
    random.rs                #   chunked random data generator
  tests/
    test_s3/                 # S3 API tests (18 modules, plus test_sns and test_iam)
      bucket_list.rs         #   list objects v1/v2, delimiters, prefixes, pagination
      object_ops.rs          #   CRUD, range requests, metadata, bucket naming
      acl.rs                 #   bucket/object ACLs, grant permissions, cross-user
      multipart.rs           #   multipart upload/abort/list, copy, error cases
      versioning.rs          #   versioning, delete markers, null versions
      lifecycle.rs           #   lifecycle rules, expiration (days/date/versioning)
      object_lock.rs         #   lock config, retention, legal hold
      copy.rs                #   copy object, metadata, cross-bucket, ACL
      tagging.rs             #   bucket/object tagging, limits, modify
      encryption.rs          #   SSE-S3, SSE-C (single + multipart)
      conditional.rs         #   conditional GET (if-match, if-modified-since, etc.)
      bucket_policy.rs       #   bucket policies, tag conditions
      cors.rs                #   CORS configuration (includes SigV2 presigned tests)
      headers.rs             #   SigV2 header validation (auth_aws2)
      signing.rs             #   payload signing mode matrix (4 modes × body sizes)
      post_object.rs         #   POST (form-based) uploads, policies, redirects
      bucket_logging.rs      #   bucket logging config, key formats, ACL/auth
    test_iam.rs              # IAM user policy tests
    test_sns.rs              # SNS topic CRUD and notification config
  examples/
    cleanup.rs               # manual bucket cleanup binary
  .config/
    nextest.toml             # nextest profiles and setup script
  docs/
    architecture-plan.md     # original architecture design
    translation-status.md    # module-by-module translation status
    sdk-limitations.md       # SDK limitations and bypass strategies
```

## Relationship to Python s3-tests

This project reuses the same `s3tests.conf` configuration file format and
the same RGW user setup as the Python s3-tests. Both suites can run against
the same cluster simultaneously (each uses its own random bucket prefix).

The nextest profiles roughly correspond to pytest markers from the original
suite. Test names are preserved where possible for traceability.

See `docs/translation-status.md` for a detailed mapping of Rust modules to
their Python source files and line numbers.
