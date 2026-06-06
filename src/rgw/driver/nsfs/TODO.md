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

## Future milestones (out of scope for now)

- Versioning (`.versions/` directory scheme)
- GPFS integration (`gpfs_linkat`, `O_TMPFILE`, conditional link/unlink)
- Handle cache (LRU for Directory objects)
- `force_md5_etag` config option
- Bucket lifecycle management
