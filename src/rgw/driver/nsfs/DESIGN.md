# NSFS Driver — Design and Architecture

The nsfs SAL backend maps S3 objects to a real POSIX filesystem hierarchy,
tracking the noobaa nsfs specification for compatibility with GPFS
deployments.

## Filesystem Layout

- Bucket = a directory under the nsfs root
- Object key = literal filesystem path (no URL-encoding)
  - `dir1/dir2/file.txt` → real `dir1/dir2/file.txt` on disk
- Multipart staging = `.multipart_<upload_id>/` at bucket root
- Directory objects (future) = real directory + `.folder` sentinel

## Path Resolution

`resolve_path()` decomposes an object key into per-component `openat()`
calls, creating intermediate directories as needed. Returns a leaf
directory fd + leaf filename. This avoids PATH_MAX limits and enables
O_NOFOLLOW per component.

The dir_chain vector owns intermediate Directory objects. On DELETE,
reverse-walk removes empty parents.

## xattr Scheme

Two-tier prefix swap via `make_xattr_name()` / `parse_xattr_name()`:

| RGW internal key    | On-disk xattr            |
|---------------------|--------------------------|
| `user.rgw.etag`     | `user.nsfs.rgw.etag`     |
| `user.rgw.acl`      | `user.nsfs.rgw.acl`      |
| `owner`             | `user.nsfs.owner`        |
| `object_type`       | `user.nsfs.object_type`  |
| `mp_upload`         | `user.nsfs.mp_upload`    |

The conversion is bijective — no lookup tables.

## Sideloading (External Filesystem Interop)

Files created outside RGW (cp, rsync, NFS, GPFS tools) work as S3
objects without any registration step:

- **ETag:** synthesized from stat as `mtime-<base36>-ino-<base36>`.
  The dash prevents S3 SDKs from MD5-validating. Matches noobaa format.
- **Content-Type:** extension-based lookup via system mime.types.
  Falls back to `application/octet-stream`.
- **Owner:** defaults to "unknown" in listings, requesting user on GET.
- **Size:** always from stat.

RGW-created files have stored etag/content-type xattrs which take
precedence. Synthesis is strictly a fallback.

## Multipart Upload

Parts are written to `.multipart_<upload_id>/part-NNNNN` files.
CompleteMultipartUpload assembles parts into a single regular file via
`copy_file_range()` (reflink on XFS/Btrfs, kernel fallback on others),
then `renameat()` to the final hierarchical path.

`assemble_parts()` is the single integration point for future GPFS
`gpfs_linkat` splice substitution.

## Bucket Cache

Uses `BucketCache<NSFSDriver, NSFSBucket>` (lmdb) from the posix driver
template. `fill_cache()` recursively traverses the directory tree,
accumulating path prefixes to reconstruct full object keys.

## GPFS Integration Surface

Portable Linux equivalents are used throughout, with clean substitution
points for GPFS:

| Mechanism                     | Current (portable)                | Future (GPFS)          |
|-------------------------------|-----------------------------------|------------------------|
| Multipart assembly            | `copy_file_range()`               | `gpfs_linkat` splice   |
| Atomic write publish          | temp file + `rename()`            | `O_TMPFILE` + `gpfs_linkatif` |
| Race-safe unlink              | stat-before-unlink                | `gpfs_unlinkat` with fd verify |
| Batch xattr read              | per-attr `fgetxattr`              | `gpfs_fcntl` batch     |

## Key Files

- `rgw_sal_nsfs.h` — driver header, FSEnt hierarchy, SAL class declarations
- `rgw_sal_nsfs.cc` — all implementation
- `nsfsDB.{h,cc}` — user database (sqlite-based)
- `../../rgw_mime.{h,cc}` — shared mime type lookup (extracted for cross-driver use)

## Test Infrastructure

- Unit tests: `src/test/rgw/test_rgw_nsfs_driver.cc`
  - Run: `bin/unittest_rgw_nsfs_driver --create --delete --verbose`
- Integration tests: `src/test/rgw/test_nsfs_s3.sh`
  - Run: `../src/test/rgw/test_nsfs_s3.sh --verbose`
  - Self-contained: starts vstart cluster, inline credentials, no sudo
