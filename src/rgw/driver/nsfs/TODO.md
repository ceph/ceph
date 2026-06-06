# NSFS Driver — Status and TODO

Branch: `wip-posix-nsfs`

## Completed

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

## Test Coverage

- **Unit tests:** 31 (`bin/unittest_rgw_nsfs_driver --create --delete --verbose`)
- **Integration tests:** ~50 tests via `test_nsfs_s3.sh` with s3cmd + awscli
  - PUT/GET/LIST/DELETE: flat + hierarchical
  - Multipart via `aws s3api`: create, upload-part, complete, content verify
  - Hierarchical copy
  - Sideloaded file GET/HEAD/LIST with synthesized metadata
  - xattr naming verification

## TODO

### Near-term

- [ ] Directory objects — keys ending in `/`, `.folder` sentinel for body
- [ ] Handle cache — LRU cache for intermediate Directory objects from
      `resolve_path()`, modeled on RGWFileHandle (performance)
- [ ] Bucket lifecycle — creation/deletion via SAL, bucket listing

### Medium-term

- [ ] Versioning — `.versions/` directory scheme per noobaa spec
- [ ] Object Lock — retention/legal-hold xattrs
- [ ] GPFS integration — `gpfs_linkat` splice, `O_TMPFILE`, conditional
      link/unlink, batch xattr reads

### Low priority / nice-to-have

- [ ] `force_md5_etag` config option — compute real MD5 on upload for
      environments that need it (matches noobaa's per-bucket/account flag)
- [ ] Compression support in multipart path (currently `#if 0`)
- [ ] Clean up rgw_mime to use function-local static instead of init/cleanup
