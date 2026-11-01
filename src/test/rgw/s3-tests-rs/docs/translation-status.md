# Translation Status

## Scope

This suite aims to provide comprehensive S3 API conformance testing against
any production RGW backend (rados, posix, nsfs). All applicable S3 operations
are in scope, including SigV2 edge cases, subsystems like S3 Select, multisite,
and admin operations. Infrastructure sidecars (Keycloak, Vault, Kafka, etc.)
are added as needed. Tests for features being retired (e.g., tenants) may be
excluded.

## Sources

### Python s3-tests (primary)

```
Repository: https://github.com/ceph/s3-tests
Commit:     c29bf103f62df65ed342ca4efb1f6d41a77e81e7
Date:       2026-03-24
```

Source for test_s3, test_iam, test_headers, and test_sts modules.

### Ceph-tree Python test suites

| File | Tests | Description |
|------|------:|-------------|
| `src/test/rgw/bucket_notification/test_bn.py` | 87 | Bucket notification delivery (Kafka, AMQP, HTTP) |
| `src/test/rgw/bucket_logging/test_bucket_logging.py` | 16 | Bucket logging admin API |
| `src/test/rgw/lua/test_lua.py` | 8 | Lua scripting hooks on S3 operations |
| `src/test/rgw/dedup/test_dedup.py` | 50 | Object deduplication |
| `src/test/rgw/rgw_multi/tests.py` | 119 | Multisite/multizone sync |
| `src/test/rgw/rgw_multi/tests_az.py` | 12 | Archive zone replication |
| `src/test/rgw/rgw_multi/tests_es.py` | 3 | Elasticsearch metadata sync |
| `qa/workunits/rgw/test_rgw_admin_pagination.py` | 8 | Admin API bucket listing pagination |
| `qa/workunits/rgw/test-keystone-service-token.py` | 4 | Keystone service token auth |
| `qa/workunits/rgw/test_rgw_d4n.py` | 2 | D4N distributed cache |
| `qa/workunits/rgw/test_rgw_reshard.py` | 1 | Bucket resharding |

### New tests (no Python equivalent)

Tests written during posix/nsfs development for account support (group CRUD,
account roles, cross-account STS, quota), signing mode coverage, checksums,
CORS presigned URLs, stress tests, and encryption size variants.

## Current Coverage

827 tests across 22 modules (808 active + 19 ignored).

### test_s3 — 723 tests (18 modules)

| Module | Tests | Ignored | Coverage |
|--------|------:|--------:|----------|
| object_ops | 99 | 3 | CRUD, range, metadata, checksums (SHA256/CRC64), object attributes, multi-delete, special keys |
| bucket_list | 94 | 0 | List v1/v2, delimiters, prefixes, maxkeys, markers, fetchowner, continuation, unordered |
| bucket_logging | 59 | 1 | Logging config, flush, auth type, ACL, object ACL, roll time |
| acl | 57 | 6 | Canned ACLs, grants, cross-user, bucket ownership controls, MaxBuckets |
| encryption | 48 | 0 | SSE-S3 default/explicit, SSE-KMS, SSE-C, multipart, copy, range GET |
| lifecycle | 45 | 0 | Rules, expiration (days/date), noncurrent, filters, tags, delete markers |
| bucket_policy | 43 | 3 | Allow/deny, conditions, tags, public access block, copy-source, multipart |
| multipart | 42 | 0 | Upload, abort, list, copy-part, sizes, metadata, overwrite |
| object_lock | 39 | 0 | Retention, legal hold, governance bypass, versioning, delete-with-hold |
| post_object | 37 | 0 | Form upload via reqwest, policies, conditions, ACLs |
| conditional | 33 | 0 | If-Match/None-Match/Modified/Unmodified on GET and PUT |
| versioning | 30 | 0 | Create/suspend, null versions, delete markers, multi-delete, copy, ACL |
| cors | 23 | 0 | Set/delete, wildcard origin, headers, multi-rules, preflight |
| headers | 21 | 0 | Raw header injection via SigV2/reqwest (from test_headers.py) |
| copy | 18 | 1 | Same/cross bucket, versioned, conditional, metadata directive |
| signing | 17 | 0 | Payload signing modes, streaming, checksums |
| tagging | 16 | 0 | Bucket/object tags, max kv, multipart tagging, head tag count |
| stress | 2 | 3 | Bucket cache stress, concurrent put/delete/list (run manually) |

### test_iam — 73 tests (6 modules)

| Module | Tests | Ignored | Coverage |
|--------|------:|--------:|----------|
| user_policy | 26 | 1 | Attach/detach, allow/deny bucket and object actions |
| user | 18 | 0 | User CRUD, path, boundary conditions |
| role | 10 | 0 | Role CRUD, assume-role policy |
| group | 8 | 0 | Group CRUD, membership, policy |
| sts | 6 | 0 | AssumeRole, cross-account, WebIdentity (Keycloak) |
| oidc | 5 | 0 | OIDC provider CRUD |

### test_rgw — 5 tests

| Module | Tests | Coverage |
|--------|------:|----------|
| quota | 5 | User/bucket quota enforcement |

### test_sns — 26 tests

Kafka notification delivery, S3 event filters (prefix/suffix/metadata/tags),
lifecycle event notifications, multipart notifications, delete marker events.
Ported from `test_bn.py`.

## Translation Reconciliation

Cross-checked against git history in both the standalone s3-tests-rs repo
and the Ceph `-misc` branch. Test names were kept 1:1 with the Python
originals except for 8 confirmed renames (case normalization, typo fixes,
and KMS test restructuring).

### test_s3.py — 752 Python tests

- **647** exact name matches with Rust
- **8** confirmed renames (covered in Rust under different names)
- **97** unported
- Rust test_s3/ also contains tests from test_headers.py (21) and new
  tests without Python equivalents (signing, checksums, stress, ownership, etc.)

### test_iam.py — 90 Python tests

- **65** exact name matches
- **25** unported (cross-account ACL/policy (17), account roles (4),
  account summary (2), other (2))
- **8** Rust-only (group CRUD, additional OIDC tests)

### test_headers.py — 48 Python tests

- **21** exact name matches, **27** unported, **0** Rust-only

### test_sts.py — 37 Python tests

- **1** exact match (`test_assume_role_with_web_identity`)
- **36** unported (session policies (12), WebIdentity tag variants (11),
  GetCallerIdentity (3), assume-role allow/deny/expiry (5), other (5))
- **5** Rust-only (cross-account role tests structured differently)

### test_s3select.py — 38 Python tests

- **0** ported

### test_bn.py — 87 tests (Ceph tree)

- **26** ported to `test_sns.rs` (Kafka delivery, topic CRUD, filters,
  account topics, lifecycle events)
- **34** intentionally deferred: persistent queue (17), AMQP (12),
  multisite migration (2), SSL/mTLS (2), idle timeout (1)
- **27** remaining portable: HTTP endpoint delivery (~8), topic CRUD
  variants (~10), CloudEvents, opaque data, list topics, other (~9)

## Unported from s3-tests Repo

### test_s3.py (97 tests)

| Category | Count | Tests |
|----------|------:|-------|
| Bucket logging | 55 | Cleanup (24), concurrent flush/disable/update (16), config update (8), key filter (2), notupdating (4), wildcard policy (1) |
| Encryption (KMS/S3) | 16 | KMS multipart (3), KMS POST object (2), SSE-S3 default multipart/HEAD/POST (3), KMS HEAD/read-declare/not-declared/no-key (4), encrypted copy (2), enc conflict (2) |
| Conditional PUT | 8 | `test_put_object_ifmatch_{good,failed,nonexisted_failed,overwrite_existed_good}`, `test_put_object_ifnonmatch_{good,failed,nonexisted_good,overwrite_existed_failed}` |
| Restore / transition | 4 | Glacier restore (3), encrypted lifecycle transition (1) |
| Bucket policy | 3 | KMS encryption policy variants |
| Tenant | 2 | `test_bucket_policy_different_tenant`, `test_bucket_policy_tenanted_bucket` (retiring) |
| Object attributes | 2 | `test_get_multipart_checksum_object_attributes`, `test_get_sse_c_encrypted_object_attributes` |
| Usage / stats | 1 | `test_head_bucket_extended` (x-rgw-object-count header) |
| Other | 6 | `test_object_copy_versioning_multipart_upload`, `test_object_requestid_matches_header_on_error`, `test_ranged_request_*_response_code` (2), `test_read_through`, `test_bucket_head_extended` |

### test_iam.py (25 tests)

Cross-account bucket ACL/policy tests (17), account role tests (4),
account summary (2), `test_verify_allow_iam_actions` (1),
`test_same_account_role_policy_allow` (1).

### test_headers.py (27 tests)

Remaining SigV2 header validation edge cases.

### test_sts.py (36 tests)

Session policy tests (12), WebIdentity tag/resource variants (11),
GetCallerIdentity (3), assume-role allow/deny/expiry (5), other (5).

### test_s3select.py (38 tests)

Entire module — requires S3 Select support on the backend.

### s3-tests repo totals

| Source | Total | Ported | Unported |
|--------|------:|-------:|---------:|
| test_s3.py | 752 | 655 | 97 |
| test_iam.py | 90 | 65 | 25 |
| test_headers.py | 48 | 21 | 27 |
| test_sts.py | 37 | 1 | 36 |
| test_s3select.py | 38 | 0 | 38 |
| **Total** | **965** | **742** | **223** |

## Unported from Ceph-Tree Suites

### test_bn.py — bucket notifications (27 remaining portable + 34 deferred)

Already partially ported (26 Kafka tests in test_sns.rs). Remaining work:

| Category | Count | Sidecar needed | Notes |
|----------|------:|----------------|-------|
| HTTP endpoint delivery | ~8 | None (in-process HTTP server) | Basic delivery, CloudEvents, multi-delete, opaque data, lifecycle |
| Topic CRUD variants | ~10 | None | Admin topics, permissions, list topics, topic update, name validation |
| AMQP delivery | 12 | RabbitMQ | Same code path as Kafka; lower priority |
| Persistent queue | 17 | RADOS | Queue persistence in object store; rados-only |
| SSL/mTLS | 2 | Cert infra + RabbitMQ | Fragile setup |
| Multisite migration | 2 | Multi-zone cluster | Topic migration across zones |
| Idle timeout | 1 | None | 300s wait; tests infra behavior not S3 semantics |

### test_bucket_logging.py — bucket logging admin (16 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Logging info/list/flush, cleanup on delete/disable, config updates, multiple sources | None | Uses `radosgw-admin` CLI for some operations; the s3-tests `test_s3.py` bucket_logging tests (55 unported) overlap — reconciliation needed to avoid duplication |

### test_lua.py — Lua scripting (8 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Script management, put/copy hooks, entropy, access log, request interruption | None | Requires Lua support compiled into RGW (`--with-radosgw-lua-packages`); tests S3 operations with pre/post-request Lua hooks |

### test_dedup.py — object deduplication (50 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Dedup basic/incremental/dry-run, multipart, tenants, CLI operations, REST pause/resume, filter by bucket/storage-class | None (uses `radosgw-admin` + `ceph-dedup-tool`) | Heavy use of CLI tools and RADOS-level verification; may need admin API wrappers in `src/http.rs` |

### test_rgw_admin_pagination.py — admin API pagination (8 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Bucket list with/without stats, max-entries, marker, pagination | None | Uses raw HTTP against the admin API with HMAC-SHA1 signing; `src/http.rs` already has SigV2 support |

### test-keystone-service-token.py — Keystone auth (4 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| List containers, expired token, service token fallback | Keystone | Swift API (not S3); needs a Keystone identity provider sidecar |

### rgw_multi/tests.py — multisite sync (119 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Object/bucket sync, versioning, ACL, CORS, policy, delete markers, incremental sync, datalog, zone management, bucket sync enable/disable, sync policy, pubsub | Multi-zone RGW cluster | Largest single suite; requires orchestrating multiple RGW instances with zone/zonegroup config; likely a separate test binary or profile |

### rgw_multi/tests_az.py — archive zones (12 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Archive zone replication, versioning, bucket rename, object version preservation | Multi-zone RGW cluster + archive zone | Extension of multisite; same infrastructure requirements |

### rgw_multi/tests_es.py — Elasticsearch sync (3 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Object search, custom metadata search, bucket config | Elasticsearch | Metadata sync to ES; needs ES sidecar |

### test_rgw_d4n.py — D4N cache (2 tests)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Small/large object caching | D4N cache layer | Tests cache hit/miss behavior through S3 API |

### test_rgw_reshard.py — bucket resharding (1 test)

| What | Sidecar needed | Notes |
|------|----------------|-------|
| Bucket reshard operation | None (rados-only) | Uses `radosgw-admin` to trigger reshard; verifies data integrity through S3 API |

### Ceph-tree totals

| Source | Total | Ported | Remaining |
|--------|------:|-------:|----------:|
| test_bn.py | 87 | 26 | 61 |
| test_bucket_logging.py | 16 | 0 | 16 |
| test_lua.py | 8 | 0 | 8 |
| test_dedup.py | 50 | 0 | 50 |
| test_rgw_admin_pagination.py | 8 | 0 | 8 |
| test-keystone-service-token.py | 4 | 0 | 4 |
| rgw_multi/tests.py | 119 | 0 | 119 |
| rgw_multi/tests_az.py | 12 | 0 | 12 |
| rgw_multi/tests_es.py | 3 | 0 | 3 |
| test_rgw_d4n.py | 2 | 0 | 2 |
| test_rgw_reshard.py | 1 | 0 | 1 |
| **Total** | **310** | **26** | **284** |

## Ignored Tests (19)

| Count | Category | Notes |
|------:|----------|-------|
| 6 | ACL grant enforcement | Also fails in Python — pre-existing RGW issue |
| 3 | Stress tests | Manual-run only (`--run-ignored=all --test-threads=1`) |
| 3 | Object ops | Unreadable key (1), bucket logging support (1), chunked transfer (1) |
| 2 | Bucket policy | SSE config required (1), Content-Length on copy-part (1) |
| 1 | Bucket logging | Automatic rollover timing unreliable |
| 1 | Bucket policy | Tag-conditional policy (also fails in Python) |
| 1 | Copy | Cross-user copy access (also fails in Python) |
| 1 | IAM | Invalid policy version (also fails in Python) |
| 1 | Stress | Concurrent version stress |
