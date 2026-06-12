# S3-Tests Rust Translation — Status

## Overview

This project translates the [Ceph s3-tests](https://github.com/ceph/s3-tests) Python test suite
(pytest/boto3) into Rust. See [README.md](../README.md) for setup and usage.

The Python suite has ~970 tests across 7 files. This translation currently covers ~35.8%.

## Python Baseline

The translation is based on the following commit of the upstream Python s3-tests:

```
Repository: https://github.com/ceph/s3-tests
Branch:     master
Commit:     c29bf103f62df65ed342ca4efb1f6d41a77e81e7
Date:       2026-03-24
Subject:    Merge pull request #700 from pritha-srivastava/wip-rgw-sts-expired-creds
```

Recent commits at the time of this snapshot (newest first):

```
c29bf10 Merge pull request #700 from pritha-srivastava/wip-rgw-sts-expired-creds
9712efd Merge pull request #722 from cbodley/wip-74399
06e2c57 Merge pull request #720 from nbalacha/wip-nbalacha-74322
9e60e5e Merge pull request #704 from sungjoon-koh/wip-part-cksum
236ec2e Merge pull request #699 from sungjoon-koh/wip-s3-part-get-etag
120a731 Merge pull request #710 from nbalacha/wip-nbalacha-71365
```

To identify Python tests added after this baseline:

```bash
cd /path/to/python/s3-tests
git log --oneline c29bf10..HEAD -- s3tests/functional/
```

## Current Coverage

| Module | Tests | Python Source | Notes |
|---|---|---|---|
| `bucket_list` | 78 | test_s3.py | List v1/v2, delimiters, prefixes, maxkeys, markers, fetchowner, continuation, anonymous, prefix+delimiter combos, versioned listing |
| `object_ops` | 73 | test_s3.py | CRUD, range requests, metadata, multi-delete, bucket naming, concurrent ACL, anon access, response headers, ACL mtime, special keys, bucket head/delete, checksums (SHA256/CRC64), object attributes |
| `object_lock` | 28 | test_s3.py | Lock config, retention (put/get/versionid/increase/shorten/bypass/override), legal hold, delete-with-hold/retention, suspend versioning, enable-after-create, invalid days/years, mode changes |
| `acl` | 27 | test_s3.py | Bucket/object canned ACLs (public-read/write/authenticated-read/bucket-owner-read/full-control), grant userid, cross-user, verify-owner, private-to-private, revoke-all |
| `versioning` | 22 | test_s3.py | Create/suspend, null versions, delete markers (nonversioned/versioned/suspended/expiration), multi-delete, copy version, ACL, list marker, create-read-remove, remove-all, special names, atomic upload version-id |
| `bucket_policy` | 20 | test_s3.py | Allow/deny, cross-bucket, tag conditions, public policy status, v2 ACL deny, multipart, copy-source condition, notprincipal, public access block, ownership controls |
| `lifecycle` | 18 | test_s3.py | Set/get/delete rules, noncurrent, filter, expiration (days/date/versioning/header/tags), id-too-long, same-id, days0, deletemarker |
| `multipart` | 17 | test_s3.py | Upload (empty/small/contents/sizes/metadata), abort, list, missing part, incorrect etag, overwrite, abort-not-found, list-multiple |
| `test_iam` | 16 | test_iam.py | User policy CRUD, allow/deny bucket actions, allow/deny object actions |
| `copy` | 15 | test_s3.py | Same/diff bucket, zero/16M, content-type, metadata, self-copy, cross-user, canned ACL, versioned bucket, conditional copy |
| `tagging` | 12 | test_s3.py | Bucket/object tagging, max kv size, excess key/val, modify, put-with-tags, multipart tagging, head tag count |
| `encryption` | 10 | test_s3.py | SSE-S3 explicit + default upload (all 8 ignored — need KMS) |
| `conditional` | 7 | test_s3.py | If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since on GET |
| `cors` | 4 | test_s3.py | Set/delete, wildcard origin, headers, multi-rules |
| **Total** | **347** | | **~35.8% of ~970** |

## Ignored/Skipped Tests (22 total)

Tests are ignored with a reason string indicating the category.

| Count | Ignore prefix | Category | Notes |
|---|---|---|---|
| 8 | `requires SSE configuration` | Missing infra | Need KMS (Vault/GKLM) backend |
| 3 | `RGW returns 411 MissingContentLength` | SDK/RGW interop | Need SDK interceptor for Content-Length: 0 |
| 4 | `VERIFY: ...RGW ACL grant enforcement` | Pre-existing failure | Also fails in Python s3-tests on same cluster |
| 1 | `VERIFY: ...RGW cross-user copy access` | Pre-existing failure | Also fails in Python s3-tests on same cluster |
| 1 | `VERIFY: ...RGW tag-conditional policy` | Pre-existing failure | Also fails in Python s3-tests on same cluster |
| 4 | `VERIFY: ...RGW XML element order` | SDK/RGW interop | RGW IAM XML order breaks aws-sdk-iam parser |
| 1 | `VERIFY: ...GetObjectAttributes` | SDK/RGW interop | RGW returns 403 on Rust SDK GetObjectAttributes (passes in Python) |

The `VERIFY:` prefix marks tests that need further investigation — these may be
translation bugs, RGW bugs, or expected behavior differences. They were verified
against the Python suite on the same cluster to distinguish from translation errors.

## Looking Ahead

### Straightforward to translate (no special infra)

| Category | Est. remaining | Notes |
|---|---|---|
| object_ops | ~40 | More delete, head, write, ACL variants, presigned URLs |
| acl | ~50 | Access permission matrix, header ACL grants |
| bucket_policy | ~20 | More condition operators, deny policies |
| IAM (test_iam.py) | ~74 | Role policies, account tests (need `iam_root` fixture) |
| tagging | ~13 | Versioned tagging, ACL-gated tagging |
| versioning | ~3 | Concurrent scenarios |
| lifecycle | ~28 | Tag-based expiration, noncurrent expiration, size filters |
| object_lock | ~10 | Multi-delete with retention, compliance mode |

### Requires external infrastructure or SDK work

| Category | Est. remaining | Dependency |
|---|---|---|
| encryption (SSE-KMS) | ~57 | KMS: Vault or GKLM |
| POST object | ~36 | `reqwest` crate (not external, just not yet added) |
| bucket logging | ~116 | RGW logging infrastructure |
| test_headers.py | 48 | SDK interceptor for header injection |
| test_sts.py (WebIdentity) | ~27 | OIDC IdP (Keycloak) |
| test_s3select.py | 38 | S3 Select support |
| test_sns.py | 4 | Kafka broker |
| PUT conditional writes | ~10 | SDK interceptor for If-Match on PutObject |
| Multipart copy | 3 | SDK interceptor for Content-Length: 0 |
| SigV2 tests | ~25 | Custom signing (SDK doesn't support SigV2) |
| Virtual-host bucket URLs | varies | Custom DNS infrastructure |

See [sdk-limitations.md](sdk-limitations.md) for a detailed inventory of ~115
tests blocked by Rust AWS SDK constraints.

### Recommended next priorities

1. **Implement SDK interceptor pattern** — unblocks multipart copy (3), PUT conditionals (~10),
   and headers tests (48). Establishes reusable infrastructure.
2. **Expand object_ops** (+20-30) — biggest single-module gap
3. **Expand IAM** (+15-20) — role policy tests
4. **Add POST object module** — add `reqwest` dependency, enables ~36 tests
5. **Expand lifecycle** — tag-based and size-filter expiration scenarios
