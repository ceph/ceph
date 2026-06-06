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

### 1. Conditional operations
If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since on
GET/PUT/DELETE. Relies on `RGW_ATTR_ETAG` and mtime from
`get_obj_attrs()`. Includes sideloaded-file synthesized etags.

- **Status:** needs audit
- **Risk:** low — op layer does the comparisons, store just provides attrs

### 2. Byte-range GET
Range header support. `NSFSReadOp::iterate()` receives ofs/end params
and must pass them through to `File::read()`.

- **Status:** needs audit
- **Risk:** low — inherited from posix, unlikely broken

### 3. Content-MD5 validation on PUT
Client sends Content-MD5 header, op layer validates against computed MD5.
Store just needs to store the etag correctly via `Writer::complete()`.

- **Status:** needs audit
- **Risk:** low — op layer handles validation

### 4. Additional checksums (CRC32, CRC32C, SHA256)
Newer S3 feature. Op layer computes checksums during upload. Store
persists `RGW_ATTR_CKSUM` through attrs. Needs xattr round-trip
verification.

- **Status:** needs audit
- **Risk:** medium — newer feature, may have assumptions about store capabilities

### 5. Object metadata round-trip
x-amz-meta-*, content-type, content-encoding, content-disposition,
cache-control. Stored as `RGW_ATTR_PREFIX + header` attrs. Must
round-trip through `make_xattr_name()`/`parse_xattr_name()` correctly.

- **Status:** needs audit
- **Risk:** medium — the xattr prefix swap is the most likely breakage point

### 6. S3 Object Tagging
GetObjectTagging, PutObjectTagging, DeleteObjectTagging. Tags stored
as `RGW_ATTR_TAGS`. Needs `get_obj_attrs()`/`set_obj_attrs()` to
handle tag attrs correctly.

- **Status:** needs audit
- **Risk:** low — pure attr storage

### 7. ACLs
At minimum, canned ACLs on PUT. `RGW_ATTR_ACL` storage. Check
`NSFSObject::set_acl()`, `get_acl()`.

- **Status:** needs audit
- **Risk:** low — inherited from posix

### 8. Object Lock / Retention / Legal Hold
`RGW_ATTR_OBJECT_RETENTION`, `RGW_ATTR_OBJECT_LEGAL_HOLD`. Primarily
attr storage — the op layer enforces the semantics, the store just
persists and returns the attrs.

- **Status:** needs audit
- **Risk:** low — pure attr storage, but multipart complete() already
  threads retention/legal-hold through

### 9. Presigned URLs
Should work if GET/PUT work correctly. Main risk is path-encoding
issues from our url_encode elimination — presigned URL signatures
include the object key, and if the key contains characters that
need encoding, there could be signature mismatches.

- **Status:** needs audit
- **Risk:** medium — path encoding is a known area of change

## Approach

For each item: read the relevant code paths, identify gaps, write a
test that exercises the feature via the integration test, fix any
breakage found. Items that pass without changes get documented as
verified; items that need fixes get committed individually.

## Future milestones (out of scope for now)

- Versioning (`.versions/` directory scheme)
- GPFS integration (`gpfs_linkat`, `O_TMPFILE`, conditional link/unlink)
- Handle cache (LRU for Directory objects)
- `force_md5_etag` config option
- Bucket lifecycle management
