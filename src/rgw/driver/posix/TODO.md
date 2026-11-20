# POSIX Driver — Known Issues and TODO

## Known test failures

### test_multi_objectv2_delete ordering artifact

`object_ops::test_multi_objectv2_delete` fails when run in sequence
with other tests (e.g., via `cargo nextest run -E 'test(/^object_ops::/)'`)
but passes when run alone.  The failure is `assertion failed:
response.contents().is_empty()` — objects remain visible after
DeleteObjects.

Root cause appears to be stale versioning state from a prior test
leaking into the bucket cache.  The v1 variant
(`test_multi_object_delete`) passes reliably.

Note: posix developers currently test primarily with the ceph-s3
test suite (boto3-based).  This issue was found with s3tests-rs
(`qa/workunits/rgw/s3tests-rs`), the Rust S3 compatibility suite.

### BucketCache LRU race under concurrent load

Shared with nsfs — see `src/rgw/driver/nsfs/TODO.md` "Known issues"
section for full analysis with backtraces.  Reproduces on posix during
the s3tests-rs `bucket_list` and `multipart` modules, which create
many buckets rapidly.

Reproducer: `qa/workunits/rgw/s3tests-rs/tests/test_s3/stress.rs`
(marked `#[ignore]`).
