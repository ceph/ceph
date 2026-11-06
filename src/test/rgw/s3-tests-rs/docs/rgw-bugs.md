# RGW Bugs Discovered During Test Translation

Issues found during s3-tests-rs development that appear to be RGW server bugs
(not SDK issues or test translation errors).

## Open

### PostBucketLogging 500 when objects exist before logging is enabled

**Symptom:** `POST /{bucket}?logging` (Ceph extension flush) returns HTTP 500
`UnknownError` when the source bucket contained objects before
`PutBucketLogging` was called.

**Reproduction:**

```
1. CreateBucket src, CreateBucket log
2. PutBucketPolicy on log (allow logging.s3.amazonaws.com)
3. PutObject to src (key="testkey")           ← object exists BEFORE logging
4. PutBucketLogging on src → log              ← enable logging
5. GetObject from src
6. POST src?logging                           ← 500 UnknownError
```

If step 3 is moved after step 4 (object created after logging enabled),
`PostBucketLogging` returns 200 normally.

**Workaround:** Tests reorder setup so objects are created after logging is
enabled. The test semantics are preserved since the tests verify log record
content (AuthType, ACLRequired), not whether logging captures pre-existing
objects.

**RGW version:** main branch (May 2026).

## Resolved

### RGW returns 404 instead of 400 for unreadable object key

Test `test_object_write_read_unreadable` — RGW returns 404 NotFound for a
key containing `\x0a` where the expected behavior is 400 BadRequest. The
Python test also fails on the same cluster. Test is ignored with `VERIFY`
prefix pending investigation.

### ACL grant enforcement edge cases

Four ACL tests (`test_bucket_acl_grant_*_read` variants) fail on both Rust
and Python suites on the same cluster. These appear to be pre-existing RGW
issues with how ACL grants interact with the bucket ownership model. Tests
are ignored with `VERIFY` prefix.
