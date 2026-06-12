# RGW Bugs Discovered During Test Translation

Issues found during s3-tests-rs development that appear to be RGW server bugs
(not SDK issues or test translation errors). Each entry includes reproduction
steps and evidence.

## 1. `PostBucketLogging` returns 500 when objects exist before logging is enabled

**Discovered:** 2026-05-16

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

If step 3 is moved after step 4 (object created AFTER logging enabled),
`PostBucketLogging` returns 200 normally.

**Impact:** Any test that creates objects before enabling logging and then
calls `PostBucketLogging` to flush will fail. Affects: `test_bucket_logging_bucket_auth_type`,
`test_bucket_logging_bucket_acl_required`, `test_bucket_logging_object_acl_required`.

**Workaround:** Reorder test setup so objects are created after logging is
enabled. The test semantics are preserved since we're testing log record
content (AuthType, ACLRequired), not whether logging captures pre-existing
objects.

**Python suite behavior:** The Python tests also fail on this cluster — Python
doesn't have the SDK extensions installed, so `_flush_logs` falls back to
`time.sleep(5.5)` + dummy put, but no log objects appear within the timeout.
The root cause may be the same issue manifesting differently.

**Additional evidence:**

- HTTP 500 with `UnknownError` indicates an unhandled error path in RGW,
  not an intentional rejection. A valid PostBucketLogging request should
  never produce a 500.
- The Python tests are structured to create objects before enabling logging
  — the test authors expected this ordering to work. This is the natural
  pattern (enable logging on an existing bucket with data).
- The operations being logged (GET, LIST by alt user) occur *after*
  PutBucketLogging. S3 logging records operations, not object creation.
  These operations should produce log records regardless of when the
  objects were created.
- The sleep-based fallback path (no PostBucketLogging, just wait for
  rollover) also produces zero log records. This suggests the logging
  subsystem itself fails to initialize properly when the bucket has
  pre-existing objects — not just PostBucketLogging crashing, but no
  log records generated at all for post-enable operations.

**RGW version:** Built from current main branch (2026-05-16 checkout).
