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

### File-as-prefix limitation

Because `/` in an object key maps to real directory boundaries, the
POSIX directory namespace constraint applies: a name in a directory is
either a file or a subdirectory, not both.  If key `foo/bar` exists as
a regular file, a subsequent PUT of `foo/bar/xyzzy` fails with
`ENOTDIR` — `resolve_path` cannot open `bar` as a directory.

Other filesystem-backed drivers (e.g. posixdriver) avoid this by
flattening key paths.  Filesystems with richer namespace semantics
(resource forks, alternate data streams) could also sidestep this, but
the standard POSIX API does not expose such mechanisms.

The noobaa nsfs implementation has the same constraint.  Their
`_create_path()` (native_fs_utils.js) does `mkdir` on each path
component and catches `EEXIST`/`EISDIR`, but lets `ENOTDIR`
propagate.  `translate_error_codes` maps `ENOTDIR` to
`NO_SUCH_OBJECT` (S3 404) — misleading, since the conflict is
structural, not a missing object.  Noobaa's `.folder` sentinel
mechanism is unrelated: it handles directory objects (keys ending
in `/`), not file-as-prefix conflicts.

S3 tests that exercise overlapping key prefixes are tagged
`fails_on_nsfs` and skipped via `--features fails_on_nsfs`.

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

### Per-part metadata on the assembled object

Two xattrs are stored on the final file at completion time:

| Attr name                 | Encoding              | Purpose                               |
|---------------------------|-----------------------|---------------------------------------|
| `multipart_part_count`    | encoded uint16\_t     | `x-amz-mp-parts-count` header        |
| `multipart_part_sizes`    | encoded vector\<u64\> | GET/HEAD `?partNumber=N` byte slicing |

`ReadOp::prepare()` uses `part_sizes` to compute the byte offset and
length within the assembled file for a requested part number, then
adjusts `obj_size` and applies the offset in `read()`/`iterate()`.
For non-multipart objects, `partNumber=1` returns the whole body
(matching rados behavior for Java SDK compatibility).

### Differences from noobaa nsfs

The assembly strategy matches noobaa: concatenate parts into one file,
delete the staging directory.  However:

- **noobaa does not store per-part sizes on the assembled object.**
  Part metadata (size, offset, etag) lives on temporary part files
  during upload and is deleted after completion.
- **noobaa does not support GET `?partNumber=N`.**  Our per-part size
  xattrs are an extension that enables this S3 feature.
- **xattr naming** follows the same `user.nsfs.*` prefix-swap scheme.

### GPFS xattr sizing consideration

The encoded `vector<uint64_t>` grows linearly with part count (~8
bytes per part plus encoding framing).  For the S3 maximum of 10,000
parts this is ~80 KB — well within GPFS's standard POSIX xattr limit
(256 KB per attr) but exceeds the 216-byte buffer that noobaa's native
`gpfs_fcntl` batch API allocates (`GPFS_XATTR_BUFFER_SIZE` in
`fs_napi.cpp`).

Our GPFS integration surface (see table below) currently targets
standard POSIX `fgetxattr`/`fsetxattr`, which handles the full range.
If we later adopt noobaa's `gpfs_fcntl` batch path for performance,
this xattr will need either a larger buffer or a packed representation
(e.g. raw little-endian u64 array without DENC framing).

## Bucket Cache

Uses `BucketCache<NSFSDriver, NSFSBucket>` (lmdb) from the posix driver
template. `fill_cache()` recursively traverses the directory tree,
accumulating path prefixes to reconstruct full object keys.

### Inotify watcher (`rgw_nsfs_inotify`)

The bucket cache can optionally use inotify to detect files created or
removed in bucket directories by processes outside radosgw (sideloading).
This is **disabled by default** (`rgw_nsfs_inotify = false`) because in
single-process deployments every filesystem change made by radosgw
itself generates a redundant inotify event, adding lock contention and
wasted I/O.  Enable it only when external tools (cp, rsync, NFS, GPFS
tools) modify files that should appear as S3 objects without a PUT.

The corresponding posix-backend option is `rgw_posix_inotify`.
`rgw-vstart.sh` accepts `--with-inotify` to enable it at dev-cluster
start.

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
