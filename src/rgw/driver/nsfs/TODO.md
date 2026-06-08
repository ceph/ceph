# NSFS Driver — Status and TODO

Branch: `wip-posix-nsfs`

## Completed — Milestone 1 (basic I/O + layout)

| Phase | Commit | Summary |
|-------|--------|---------|
| 0 | `637531d` | Test baseline — 16 unit tests against nsfs clone |
| 1 | `3cce418` | Strip versioning — ~740 lines removed |
| 2 | `bbfd863` | Hierarchical path mapping — `resolve_path()`, decomposed openat |
| — | `69801f4` | s3cmd integration test (`test_nsfs_s3.sh`) |
| 3+4 | `fb4d11b` | Recursive listing + DELETE with dir cleanup |
| 5 | `a58a6c1` | `user.nsfs.*` xattr scheme with prefix swap |
| — | `4975171` | xattr on-disk verification tests |
| 6 | `faefadd` | Multipart with reflink assembly (`copy_file_range`) |
| — | `0bbe7bf` | Fix chown/copy_object for hierarchical paths |
| — | `e8285f4` | Style: drop redundant nsfs:: qualifications |
| — | `c46421f` | Sideloading: synthesize etag/content-type for external files |
| — | `3c3304c` | Add DESIGN.md and TODO.md |
| — | `9dd9f86` | Directory objects via .folder sentinel |

## Test Coverage

- **Unit tests:** 35 (`bin/unittest_rgw_nsfs_driver --create --delete --verbose`)
- **Integration tests:** ~60 tests via `test_nsfs_s3.sh` with s3cmd + awscli
  - PUT/GET/LIST/DELETE: flat + hierarchical
  - Multipart via `aws s3api`: create, upload-part, complete, content verify
  - Hierarchical copy, sideloaded files, directory objects
  - xattr naming verification

## Milestone 2 — Core S3 I/O path completeness

Goal: audit and fill gaps in S3 operations that RGW supports, focusing
on the I/O path. Most of these should already work from the posix fork;
the audit identifies what our nsfs adaptations (hierarchical paths,
xattr prefix swap, url_encode removal) may have broken.

### 1. Conditional operations — VERIFIED
If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since.
Op layer compares against etag/mtime from `get_obj_attrs()`.
`NSFSReadOp::prepare()` handles all four conditions correctly.

### 2. Byte-range GET — VERIFIED
`NSFSReadOp::iterate()` honors ofs/end via `File::read()` + `lseek()`.
Multipart range reads route through `MPDirectory::read()` with
per-part offset adjustment.

### 3. Content-MD5 validation on PUT — VERIFIED
Op layer computes MD5, stores in `attrs[RGW_ATTR_ETAG]` before
calling `Writer::complete()`. Flows through `write_attrs()` →
`make_xattr_name()` → `user.nsfs.rgw.etag` on disk.

### 4. Additional checksums (CRC32, CRC32C, SHA256) — VERIFIED
Op layer stores `RGW_ATTR_CKSUM` in attrs map before calling
`Writer::complete()`. Multipart parts store per-part cksum in
`NSFSUploadPartInfo`. `get_info()` restores `cksum_type`/`cksum_flags`.
Op layer reconstructs composite via `try_sum_part_cksums()` →
`part->get_cksum()`.

### 5. Object metadata round-trip — VERIFIED
`user.rgw.*` → `user.nsfs.rgw.*` prefix swap via
`make_xattr_name()`/`parse_xattr_name()` is bijective. All RGW_ATTR_*
keys (content-type, content-encoding, content-disposition, cache-control,
x-amz-meta-*) round-trip correctly.

### 6. S3 Object Tagging — VERIFIED
`RGW_ATTR_TAGS` = `"user.rgw.x-amz-tagging"` flows through standard
attr storage. No special driver handling needed.

### 7. ACLs — VERIFIED
`RGW_ATTR_ACL` = `"x-rgw-acl"` stored via attrs. Maps to
`user.nsfs.x-rgw-acl` on disk. Bucket ACL persisted in
`NSFSBucket::set_acl()`.

### 8. Object Lock / Retention / Legal Hold — VERIFIED
`RGW_ATTR_OBJECT_RETENTION` and `RGW_ATTR_OBJECT_LEGAL_HOLD` flow
through standard attrs. Multipart `upload_info` carries
retention/legal-hold through init → meta xattr → get_info() cycle.

### 9. Presigned URLs — VERIFIED
Path encoding handled at HTTP layer (rgw_auth_s3.cc). Store receives
decoded keys. `decode_obj_key()` does no url_decode — keys are literal
filesystem paths. No encoding mismatch possible.

## Empirical validation

The code audit above confirms no regressions from the nsfs adaptations.
Empirical validation uses s3-tests-rs (`~/debug/clone-of-s3-tests-rs`),
a Rust port of the s3-tests suite, pointed at a vstart nsfs cluster.

Relevant test modules and coverage:

| Module | Tests | Covers items |
|--------|-------|-------------|
| `conditional.rs` | 33 | 1 |
| `headers.rs` | 21 | 2, 3, 5 |
| `object_ops.rs` | 98 | 1, 2, 3, 5, 9 |
| `copy.rs` | 18 | 5 |
| `multipart.rs` | 36 | 3, 4 |
| `tagging.rs` | 16 | 6 |
| `acl.rs` | 60 | 7 |
| `object_lock.rs` | 39 | 8 |

## Branch housekeeping

The branch has several `fixup!` commits that need squashing before merge:

```
git rebase -i --autosquash <base>
```

The fixup commits target:
- `rgw/posix: replace POSIXManifest with per-part size xattrs` — encode_attr/decode_raw_attr + uint16_t
- `rgw/nsfs: replace NSFSManifest with per-part size xattrs` — same
- `rgw/posix: fix use-after-free in get_meta_obj` — pin_bucket + write_attrs stale removal
- `rgw/nsfs: fix use-after-free in get_meta_obj` — same

Posix-only commits (get_ent, conditional PUT/DELETE, per-part xattrs,
get_meta_obj, write_attrs) are structured for cherry-picking to Ali's
posixdriver development baseline if needed.

## Completed — Milestone 3 (S3 Versioning)

Follows the noobaa nsfs `.versions/` layout so the on-disk structure can
exploit GPFS `gpfs_linkatif`/`gpfs_unlinkat` primitives in a future phase.
Version IDs are deterministic from stat (`mtime-{base36_ns}-ino-{base36}`),
serving as the serialized CAS comparison value.

Design rationale and noobaa analysis: see DESIGN.md.

| Phase | Summary | Status |
|-------|---------|--------|
| 1 | Version ID helpers, `.versions/` path utils, safe-link/safe-unlink CAS, xattr constants | done |
| 2 | Versioned PUT — demote current to `.versions/`, OFD-locked demote+rename | done |
| 3 | Versioned DELETE — delete marker creation, specific version deletion, promotion | done |
| 4 | Version-aware GET/HEAD — `_find_version_path`, delete marker detection | done |
| 5 | ListObjectVersions — enumerate `.versions/` in fill_cache, LMDB custom comparator | done |
| 6 | Bucket versioning state — set_bucket_versioning persistence, auto-create `.versions/` | done |

Additional fixes applied during milestone 3:
- Conditional checks for versioned delete (DM targets, version-specific targets)
- OFD file locking for demote+rename atomicity (replaces CAS retry loop)
- Cache invalidation after versioned PUT/MPU/copy
- LMDB dbi handle exhaustion fix (persistent dbi map, mdb_drop eviction)
- max_dbs derived from cache max_buckets

## Resolved issues

### BucketCache LRU crashes (fixed)

Three crash signatures in cohort_lru under concurrent multi-bucket
workloads — SIGABRT in evict_block, SIGSEGV in AVL traversal, SIGSEGV
in string comparison. Fixed in commit `26fb45c`.


### LMDB dbi handle exhaustion (fixed)

`mdb_dbi_close()` does not reclaim slots. Fixed with persistent
per-partition `flat_map<string, MDBDbi>` and `mdb_drop(del=1)`
eviction. max_dbs derived from `rgw_nsfs_cache_max_buckets`.
Commits `24554c6`, `fab6ed1`.

## Open work

See STATUS.md for the current s3tests-rs scorecard and feature
completeness assessment.

### Lifecycle

Not implemented. `get_lifecycle()`/`get_rgwlc()` return nullptr.
Requires a timer/worker framework. Accounts for 74 lifecycle test
failures plus 2 versioning and 2 bucket_policy failures that depend
on lifecycle execution.

### Bucket logging

Not implemented. Requires log object assembly and flush
infrastructure. 188 test failures.

### SNS / notifications

`NSFSNotification` is a stub. Topic management returns -ENOTSUP.
8 test failures.

### SSE-C multipart

4 test failures. Encryption attributes are stored but no actual
SSE-C crypto is performed during multipart upload assembly.

### Atomicity / locking design review

OFD locks on `.versions/.lock` serialize versioned PUT/MPU demote+rename.
This is correct but coarse. A broader review of locking granularity and
its interaction with the BucketCache inotify invalidation path is
warranted. See DESIGN.md cache invalidation section.

## Future milestones (out of scope for now)

- GPFS integration (`gpfs_linkatif`, `gpfs_unlinkat`, `O_TMPFILE`, fd pre-staging)
- Handle cache (LRU for Directory objects)
- `force_md5_etag` config option
- Bucket lifecycle management
- Bucket logging
- SNS / notifications
