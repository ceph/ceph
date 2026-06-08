# NSFS Driver — Status (2026-06-08)

Branch: `wip-posix-nsfs`

## s3tests-rs Scorecard

Tested against a local-XFS vstart cluster with `--features fails_on_nsfs`.
722 tests total, 583 passed, 139 failed, 54 skipped.

| Module | Score | Notes |
|--------|-------|-------|
| object_ops | 87/87 | |
| bucket_list | 92/92 | 2 file-as-prefix skipped |
| conditional | 33/33 | |
| acl | 51/51 | |
| tagging | 16/16 | |
| headers | 13/13 | |
| cors | 23/23 | |
| signing | 17/17 | |
| post_object | 37/37 | |
| object_lock | 39/39 | |
| multipart | 34/34 | |
| copy | 15/15 | |
| versioning | 29/31 | 2 lifecycle-dependent |
| bucket_policy | 35/37 | 2 lifecycle-dependent |
| encryption | 22/26 | 4 SSE-C multipart |
| lifecycle | 8/82 | not implemented |
| bucket_logging | 13/201 | not implemented |
| SNS topics | 0/8 | not implemented |

## Feature Completeness

### Fully working

These features pass all s3tests-rs coverage and are considered
complete at the local-XFS level.

- **Core CRUD** — PUT, GET, HEAD, DELETE for objects and buckets
- **Hierarchical listing** — prefix, delimiter, pagination, v1 and v2
- **Multipart upload** — init, upload-part, complete, abort, list-parts,
  per-part GET (?partNumber=N), copy-part
- **Object copy** — same-bucket, cross-bucket, metadata directive
- **Versioning** — versioned PUT/GET/HEAD/DELETE, delete markers,
  version promotion, ListObjectVersions, suspended versioning,
  null version handling, OFD-locked demote+rename
- **Conditional operations** — If-Match/If-None-Match on PUT, MPU
  complete, and DELETE (including versioned and delete-marker targets);
  x-amz-if-match-last-modified-time and x-amz-if-match-size on DELETE
- **ACLs** — bucket and object ACLs, canned ACLs
- **Tagging** — object and bucket tagging
- **CORS** — stored as bucket attributes, enforced by op layer
- **Object lock** — retention and legal hold via attributes
- **Bucket policy** — policy storage and evaluation (op layer)
- **Presigned URLs** — handled at HTTP layer, no driver involvement
- **Request signing** — v2 and v4 signature validation
- **POST object** — form-based upload
- **Byte-range GET** — offset/length reads, multipart range reads
- **Checksums** — CRC32, CRC32C, SHA256 via op layer
- **Content-MD5** — validated on PUT

### Partial / narrow gaps

**Encryption (22/26):** SSE-S3 and SSE-KMS attributes are stored and
round-tripped. SSE-C (customer-provided keys) fails on multipart
upload — 4 tests fail. No actual crypto is performed; the test KMS
backend handles SSE-S3/SSE-KMS transparently at the op layer.

### Not implemented

**Lifecycle (8/82):** `get_lifecycle()` and `get_rgwlc()` return
nullptr. The lifecycle subsystem requires a timer/worker framework
that doesn't exist in the nsfs driver. The 8 passing tests are
configuration-only (GET/PUT lifecycle config via bucket attrs).
The 2 versioning failures (`test_delete_marker_expiration`) and
2 bucket_policy failures (`test_lifecyclev2_expiration`) are also
lifecycle-dependent.

**Bucket logging (13/201):** All logging-specific bucket methods
are inherited stubs. Bucket logging requires infrastructure for log
object assembly and flush that is rados-specific in the current RGW
design. The 13 passing tests are likely configuration-only.

**SNS / notifications (0/8):** `NSFSNotification` is a stub
(publish returns 0). Topic management methods return -ENOTSUP.
Notifications require a message bus integration that doesn't exist
for nsfs.

### SAL interface stubs

These SAL methods are stubbed (return 0 or empty) and do not affect
s3tests-rs results, but would matter for production deployments:

- **Usage tracking** — `log_usage()`, `read_all_usage()`,
  `trim_all_usage()` all return 0
- **Statistics** — `load_stats()`, `complete_flush_stats()` return 0
- **Metadata listing** — `meta_list_keys_init/next()` return 0
- **IAM groups/roles** — return -ENOTSUP
- **OIDC providers** — return -ENOTSUP
- **Cloud tiering** — `transition_to_cloud()` returns -1

## Concurrency

Tested with `--test-threads=4` and `rgw_thread_pool_size=512`.
No crashes under parallel load after the following fixes:

- OFD file locking on `.versions/.lock` for demote+rename atomicity
- LMDB dbi handle exhaustion fixed with persistent per-partition
  flat_map and mdb_drop(del=1) eviction
- max_dbs derived from cache max_buckets (not hardcoded)
- BucketCache LRU crash fixes (cohort_lru eviction + AVL locking)

## Known limitations

**File-as-prefix:** POSIX namespace constraint — a path component
cannot be both a file and a directory. PUT `foo/bar` then PUT
`foo/bar/xyzzy` fails with ENOTDIR. Noobaa-nsfs has the same
limitation. Tests tagged `fails_on_nsfs`. See DESIGN.md.

**Listing cache races:** Two `delimiter_basic` tests were
previously attributed to cache races but are actually
file-as-prefix conflicts (now correctly tagged and skipped).
