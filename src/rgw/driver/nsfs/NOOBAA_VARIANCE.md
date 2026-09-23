# NooBaa NSFS vs Ceph NSFS: On-Disk Variance Analysis

## Purpose

Our Ceph NSFS driver was created to provide an S3-compatible filesystem
backend inspired by NooBaa's NSFS.  This document catalogs concrete
on-disk format differences that would prevent a live migration from a
running NooBaa NSFS deployment to our Ceph NSFS driver (or vice versa),
traces the history of design decisions and where implementation diverged
from intent, and identifies corrective actions.

The focus is on **multipart upload** state, where the divergence is
most consequential for in-flight operations, but the xattr and encoding
divergence affects all objects.

---

## 1. xattr namespace and encoding

### Prefix mapping

NooBaa uses `user.noobaa.*` as its internal prefix.  Our driver uses
`user.nsfs.*`.  The prefix swap is implemented in `make_xattr_name()`
/ `parse_xattr_name()` (`rgw_sal_nsfs.cc:72-89`):

```cpp
static const std::string NSFS_XATTR_PREFIX = "user.nsfs.";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs.rgw.";
static const std::string RGW_ATTR_PFX = "user.rgw.";

static inline std::string make_xattr_name(const std::string& key) {
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}
```

NooBaa's equivalent (`namespace_fs.js:73-88`):

```javascript
const XATTR_NOOBAA_INTERNAL_PREFIX = 'user.noobaa.';
const XATTR_CONTENT_TYPE = XATTR_NOOBAA_INTERNAL_PREFIX + 'content_type';
const XATTR_VERSION_ID   = XATTR_NOOBAA_INTERNAL_PREFIX + 'version_id';
// ... etc
```

### Value encoding

NooBaa stores all xattr values as **UTF-8 strings** — numbers are
decimal strings, structured data is JSON.  Our driver uses **Ceph
binary encoding** (`ENCODE_START`/`ENCODE_FINISH` with bufferlist
serialization).  These are completely incompatible wire formats.

### Complete attribute mapping

| Attribute | NooBaa xattr | Ceph NSFS xattr | Value format |
|-----------|-------------|-----------------|--------------|
| ETag | `user.content_md5` (no `noobaa.` prefix!) | `user.nsfs.rgw.etag` | NooBaa: hex string; Ceph: binary blob |
| Content-Type | `user.noobaa.content_type` | `user.nsfs.rgw.content_type` | NooBaa: string; Ceph: encoded string |
| Content-Encoding | `user.noobaa.content_encoding` | `user.nsfs.rgw.content_encoding` | same divergence |
| Version ID | `user.noobaa.version_id` | `user.nsfs.version_id` | NooBaa: string; Ceph: encoded |
| Delete marker | `user.noobaa.delete_marker` | `user.nsfs.delete_marker` | NooBaa: string; Ceph: encoded |
| Dir content | `user.noobaa.dir_content` | N/A (we use `.folder` sentinel differently) | — |
| Object tags | `user.noobaa.tag.<tagkey>` (one per tag) | `user.nsfs.rgw.x-amz-tagging` (single blob) | completely different structure |
| Legal hold | `user.noobaa.legal_hold` | `user.nsfs.rgw.obj-legal-hold-status` | different key and encoding |
| Retention mode | `user.noobaa.retention_mode` | `user.nsfs.rgw.obj-retention` | different key and encoding |
| Retention date | `user.noobaa.retention_date` | (embedded in retention blob) | — |
| Non-current timestamp | `user.noobaa.non_current_timestamp` | `user.nsfs.non_current_timestamp` | NooBaa: string; Ceph: encoded |
| User metadata | `user.<key>` (raw, no prefix) | `user.nsfs.rgw.<key>` | NooBaa: passthrough; Ceph: prefix-swapped |
| ACL | (not stored as xattr by NooBaa) | `user.nsfs.x-rgw-acl` | Ceph-specific |
| Object type | (inferred from stat) | `user.nsfs.object_type` | Ceph-specific enum |
| Multipart part count | (not stored on final) | `user.nsfs.multipart_part_count` | Ceph: encoded uint16 |
| Multipart part sizes | (not stored on final) | `user.nsfs.multipart_part_sizes` | Ceph: encoded vector<uint64_t> |
| GPFS DMAPI | `dmapi.IBM*` (4 attrs) | N/A (not yet integrated) | — |
| GPFS encryption | `gpfs.Encryption` | N/A (not yet integrated) | — |

**Impact:** A completed NooBaa object's xattrs are unreadable by our
driver.  Our driver would treat NooBaa objects as having no metadata
(no content-type, no etag, no user metadata, no tags).  The reverse
is equally true.

### Sideloaded file etag synthesis

Both drivers synthesize etags for files created outside S3 (cp, rsync,
NFS).  Both use a `mtime-ino` format that prevents S3 SDKs from
MD5-validating.  Our DESIGN.md states this "matches noobaa format" —
this is one of the few areas of deliberate compatibility.

### Bucket cache etag vs xattr etag

NooBaa's `_get_etag(stat)` (namespace_fs.js:2881) first checks the
`user.content_md5` xattr and returns the real MD5 digest when present;
the mtime-ino synthesis is only a fallback for sideloaded files.  This
means S3-created objects (including multipart uploads with composite
`hash-N` etags) always return the correct etag to clients.

Our NSFS driver uses `synthesize_etag(stx)` (mtime-ino) unconditionally
when populating bucket listing cache entries — including for S3-created
objects that have a proper etag stored in `user.nsfs.rgw.etag`.  This
means LIST results may return a synthetic etag that differs from the
actual object etag returned by HEAD/GET.  The correct behavior is to
read the etag from xattrs when present and fall back to synthesis only
for sideloaded files, matching NooBaa's precedence.

**Corrective action:** Audit all `synthesize_etag` call sites in cache
population paths (`fill_cache`, `add_entry` in multipart complete, copy,
versioned demote) and prefer the xattr-stored etag when available.

---

## 2. Multipart staging directory layout

### Directory structure comparison

**NooBaa:**
```
<bucket>/.noobaa_bucket_temp_dir/multipart-uploads/<obj_id>/
├── create-params.json        ← CreateMultipartUpload parameters (JSON)
└── data/
    ├── 5242880               ← shared data file for all 5MB parts
    └── 3145728               ← data file for the last (smaller) part
```

**Ceph NSFS:**
```
<bucket>/.multipart_<upload_id>/
├── .meta                     ← NSFSMPObj (binary-encoded xattr)
├── part-00001                ← 5MB, individual file
├── part-00002                ← 5MB, individual file
├── part-00003                ← 5MB, individual file
└── part-00004                ← 3MB, individual file
```

### Key differences

| Aspect | NooBaa NSFS | Ceph NSFS |
|--------|-------------|-----------|
| **Staging dir location** | `<bucket>/.noobaa_bucket_temp_dir/multipart-uploads/<id>/` | `<bucket>/.multipart_<upload_id>/` |
| **Upload ID source** | `<obj_id>` (directory name) | `MULTIPART_UPLOAD_ID_PREFIX + random` |
| **Upload params storage** | `create-params.json` — JSON file | `.meta` file with `NSFSMPObj` as binary xattr `user.nsfs.mp_upload` |
| **Part data file naming** | `data/<size>` — named by part size in bytes | `part-NNNNN` — zero-padded part number |
| **Part data file model** | **Shared file per unique size** — multiple parts of the same size written at different offsets within one file | **One file per part** — each part is a separate file |
| **Part metadata** | Individual string xattrs per part file | Single binary xattr per part file |

### NooBaa per-part xattrs (individual string-valued attributes)

```
user.noobaa.part_offset  = "0"                                  (decimal string)
user.noobaa.part_size    = "5242880"                             (decimal string)
user.noobaa.part_etag    = "d41d8cd98f00b204e9800998ecf8427e"    (hex string)
```

Three separate xattr keys, all string values.  `part_offset` records
the byte position of this part's data within the shared data file.

### Ceph NSFS per-part xattr (single binary blob)

```
user.nsfs.mp_upload = <ENCODE_START(2,1) num etag mtime cksum ENCODE_FINISH>
```

`NSFSUploadPartInfo { uint32_t num, string etag, real_time mtime,
optional<Cksum> cksum }`.  Note:

- No `size` field (recovered from `statx`; adding as struct_v 3 is
  planned — safe, backward-compatible).
- No `offset` field (not needed because each part is a separate file).
- `mtime` field exists (NooBaa does not store per-part mtime).
- `cksum` field exists (NooBaa does not store per-part checksums).

### Discovery implications

Our driver discovers in-flight uploads by scanning for `.multipart_*`
directories (`list_multiparts()` in `rgw_sal_nsfs.cc`).  NooBaa scans
for `.noobaa_bucket_temp_dir/multipart-uploads/*/`.  Neither driver
would discover the other's in-flight uploads.

---

## 3. Multipart data file model — the critical architectural divergence

### NooBaa: shared data file per part size

NooBaa's `upload_multipart` (`namespace_fs.js:1843`) stores part data
in files named by their **size**, not by part number.  The path is
determined by `_get_part_data_path({ ...params, size: part_size })`.

When a 100-part upload uses 5MB parts (the common case), all 100
parts are written to a single file `data/5242880` at sequential
offsets.  Each part's metadata xattr records `part_offset` (byte
position) and `part_size`.

The `part_size_to_fd_map` in `complete_object_upload` tracks how many
unique sizes have been seen.  The optimization exploits this:

```javascript
// namespace_fs.js:2000-2004
if (part_size_to_fd_map.size === 1 && !is_non_continuous_upload) {
    if (num === multiparts.length) {
        // All parts are the same size AND continuous (1,2,3,...,N).
        // The shared data file IS the final object.
        await nb_native().fs.link(fs_context, data_part_path, upload_path);
        break;  // ZERO COPY — hard link, no data movement at all
    } else {
        // Not the last part yet — accumulate size, continue
        prev_part_size = part_size;
        total_size += part_size;
        continue;
    }
}
```

When the last part has a different size (the typical case — last part
is smaller), there are exactly 2 unique sizes.  NooBaa handles this
with a copy of the prefix file + append of the last part
(`namespace_fs.js:2010-2023`).

When part sizes vary more (re-uploaded parts with different sizes, or
non-continuous part numbering), NooBaa falls back to `copy_bytes` —
userspace read/write through a buffer pool (`namespace_fs.js:2027`).

### Ceph NSFS: one file per part

Our `NSFSMultipartWriter` creates each part as a separate file:
`part-NNNNN` under the staging directory.  Part data is written via
`part_file->write(offset, data, dpp, null_yield)`.

At complete time, `assemble_parts()` (`rgw_sal_nsfs.cc:523-589`)
iterates all parts:

```cpp
for (uint16_t n = 1; n <= num_parts; ++n) {
    std::string part_name = MP_OBJ_PART_PFX + fmt::format("{:0>5}", n);
    int part_fd = openat(dir_fd, part_name.c_str(), O_RDONLY);
    // ... stat for size ...
    while (remaining > 0) {
        ssize_t copied = copy_file_range(part_fd, &in_off, out_fd, &out_off,
                                          remaining, 0);
        if (copied < 0 && (errno == EXDEV || errno == ENOSYS || errno == EOPNOTSUPP)) {
            // fallback to read/write with 64KB buffer
        }
    }
}
```

`copy_file_range()` may use reflink (CoW) on XFS and Btrfs, achieving
block-level zero-copy.  On ext4 and GPFS it falls back to a kernel
data copy, which is faster than userspace copy but still moves data.

**We cannot use the NooBaa link optimization because our data model
(one-file-per-part) precludes it.**  You cannot hard-link N separate
files into one output file.

---

## 4. The "linkat splice" — design intent vs. implementation reality

### What our DESIGN.md says

`src/rgw/driver/nsfs/DESIGN.md` lines 82-87:

> Parts are written to `.multipart_<upload_id>/part-NNNNN` files.
> CompleteMultipartUpload assembles parts into a single regular file via
> `copy_file_range()` (reflink on XFS/Btrfs, kernel fallback on others),
> then `renameat()` to the final hierarchical path.
>
> `assemble_parts()` is the single integration point for future GPFS
> `gpfs_linkat` splice substitution.

And the GPFS integration surface table (DESIGN.md lines 156-159):

| Mechanism | Current (portable) | Future (GPFS) |
|-----------|-------------------|---------------|
| Multipart assembly | `copy_file_range()` | `gpfs_linkat` splice |
| Atomic write publish | temp file + `rename()` | `O_TMPFILE` + `gpfs_linkatif` |
| Race-safe unlink | stat-before-unlink | `gpfs_unlinkat` with fd verify |
| Batch xattr read | per-attr `fgetxattr` | `gpfs_fcntl` batch |

### What `gpfs_linkat` actually is (from gpfs.h:1253-1276)

```
NAME:        gpfs_linkat()

FUNCTION:    Link file to a directory name.

             Same interface as the linkat(2) system call and
             with similar functionality with these differences:
              - When newpath specifies an existing file, it is
                replaced;
              - AT_EMPTY_PATH does not require CAP_DAC_READ_SEARCH.
```

`gpfs_linkat` is an enhanced `linkat()`.  It creates or replaces
**hard links**.  It does NOT concatenate file data, splice file
content, or merge files.

`gpfs_linkatif` (`gpfs.h:1218-1249`) adds atomic replace-with-inode-
verification — a compare-and-swap link.

Neither of these is a data-concatenation operation.

### Search for GPFS data concatenation primitives

A thorough search of `gpfs.h` and `gpfs_fcntl.h` found no
file-data-concatenation, file-splice, or file-merge API:

- `gpfs_fcntl` with `GPFS_FCNTL_RESTRIPE_*` — changes block layout
  and placement policies, does not concatenate files
- `gpfs_fcntl` with `GPFS_FCNTL_GET_XATTR` / `GPFS_FCNTL_SET_XATTR`
  — batch xattr operations
- No `GPFS_FCNTL_CONCAT`, `GPFS_FCNTL_SPLICE`, `GPFS_FCNTL_MERGE`,
  or similar operation exists in the headers

**GPFS does not expose a file-data-concatenation syscall in its
public API** (at least not in the version captured in the NooBaa
source tree).

### Where `gpfs_linkat` IS used correctly

Our `fs_strategy.cc` uses `gpfs_linkat` and `gpfs_linkatif` for
operations where they are the right tool:

- `GPFSStrategy::link_temp_file()` (line 525): `gpfs_linkat(fd, "",
  AT_FDCWD, filepath, AT_EMPTY_PATH)` — links an O_TMPFILE anonymous
  fd into the filesystem.  Uses `AT_EMPTY_PATH` which POSIX `linkat`
  requires `CAP_DAC_READ_SEARCH` for, but GPFS does not.

- `GPFSStrategy::safe_link()` (line 552): `gpfs_linkatif(src_fd, "",
  AT_FDCWD, filepath, AT_EMPTY_PATH, replace_fd)` — atomic
  compare-and-swap link for versioned PUT demote.

- `GPFSStrategy::safe_unlink()` (line 580): `gpfs_unlinkat(fd,
  filepath, delete_fd)` — verified unlink.

These are the correct GPFS-enhanced link/unlink operations.  They are
NOT used for multipart assembly.

### Where `assemble_parts()` stands

`assemble_parts()` (`rgw_sal_nsfs.cc:523-589`) is:

- A **file-local static function** — not a method on any class
- **NOT part of the `FSStrategy` virtual interface** — `grep -n
  'assemble' fs_strategy.h` returns nothing
- **NOT dispatched through any strategy** — called directly from
  `NSFSMultipartUpload::complete()`

The TODO.md lists GPFS integration as "future milestones (out of
scope for now)" and includes:

> GPFS integration (`gpfs_linkatif`, `gpfs_unlinkat`, `O_TMPFILE`,
> fd pre-staging)

Note: this TODO item does not mention multipart assembly specifically.

### The confusion — reconstructing the design intent

The phrase "gpfs_linkat splice" in DESIGN.md appears to conflate two
distinct concepts:

1. **GPFS-enhanced `linkat`** — `gpfs_linkat`/`gpfs_linkatif`, which
   provide replace-on-exist and CAS semantics beyond POSIX `linkat`.
   We already use these correctly for atomic publish and versioned
   demote.

2. **Zero-copy multipart assembly** — NooBaa achieves this via its
   shared-data-file-per-size model + POSIX `link()`.  The zero-copy
   comes from the **data model** (writing all same-sized parts to one
   file), not from any special syscall.  On GPFS, `gpfs_linkat` can
   substitute for `link()` with its replace-on-exist benefit, but the
   prerequisite is the shared-data-file model.

The prior Claude instance that authored DESIGN.md appears to have:

1. Correctly identified NooBaa's link optimization from the NooBaa
   deep-dive analysis (see `nsfs-deepdive.md:283-296`).
2. Correctly identified `gpfs_linkat` as the GPFS-enhanced `linkat`.
3. Combined them into "gpfs_linkat splice" as aspirational shorthand
   for "someday we'll do zero-copy assembly on GPFS."
4. **Never implemented the shared-data-file model** that is the actual
   prerequisite — the part-write path creates one-file-per-part.
5. Left `assemble_parts()` outside the FSStrategy interface, so even
   if we added GPFS dispatch, there's no virtual method to override.

The deep-dive doc (`nsfs-deepdive.md:284`) also uses the phrase
"linkat splice into temp output" to describe NooBaa's behavior,
which is actually `fs.link()` of the shared data file — a POSIX
hard link, not a GPFS-specific splice operation.

---

## 5. Assembly mechanism comparison

| Step | NooBaa NSFS | Ceph NSFS |
|------|-------------|-----------|
| **Part data model** | Shared file per unique size, offset-tracked | One file per part |
| **Assembly (common: all same size)** | `link()` the shared file — **true zero copy** | `copy_file_range()` per part — kernel copy or reflink |
| **Assembly (2 sizes: all-same + last)** | `link()` prefix file, copy last part | `copy_file_range()` per part — same |
| **Assembly (mixed sizes)** | `copy_bytes` — userspace r/w through buffer pool | `copy_file_range()` per part — same |
| **Assembly function** | inline in `complete_object_upload()` (~80 lines JS) | `assemble_parts()` (file-local static, 67 lines C++) |
| **FSStrategy dispatch** | N/A (JS, no strategy layer) | NOT dispatched — not in FSStrategy interface |
| **Output file** | `<mpu_path>/final` | `<staging_dir>/.assembled` |
| **Publish mechanism** | `_finish_upload()` → `linkatif` or rename | `renameat()` to final path |
| **Cleanup** | `folder_delete(mpu_path)` | `delete_directory(staging_dir)` |
| **Per-part GET after complete** | Not supported | Supported via `part_sizes` xattr on assembled file |

---

## 6. Completed object format divergence

| Aspect | NooBaa NSFS | Ceph NSFS |
|--------|-------------|-----------|
| **Regular PUT** | Single file with `user.noobaa.*` xattrs | Single file with `user.nsfs.*` xattrs |
| **Multipart result** | Single file (linked or assembled) | Single file (assembled via `copy_file_range`) |
| **Part sizes on final** | Not stored | `user.nsfs.multipart_part_sizes` (vector<uint64_t>, binary-encoded) |
| **Part count on final** | Not stored (derivable from etag `-N` suffix) | `user.nsfs.multipart_part_count` (uint16, binary-encoded) |
| **GET ?partNumber=N** | Not supported | Supported via part_sizes xattr byte slicing |
| **Object type marker** | Inferred from stat (dir vs file) | `user.nsfs.object_type` (binary-encoded enum: FILE, DIRECTORY, MULTIPART, VERSIONED, SYMLINK) |

---

## 7. Versioning compatibility

Both drivers use a `.versions/` subdirectory for version storage.
Both compute version IDs deterministically from stat fields.  But:

- **xattr names** differ (`user.noobaa.version_id` vs
  `user.nsfs.version_id`)
- **Version ID format** may differ (both use mtime+ino but possibly
  different base encoding)
- **CAS primitives** differ: NooBaa uses `gpfs_linkatif` (with
  inode verification) or POSIX safe-link/safe-unlink with mtime+ino
  CAS; our driver uses OFD file locking on `.versions/.lock`
- **Delete marker** representation differs in xattr name and encoding

The `.versions/` directory layout is structurally similar but not
directly interoperable.

---

## 8. Migration implications

### NooBaa → Ceph NSFS

- **Completed objects:** Invisible — our driver cannot parse
  `user.noobaa.*` xattrs.  Objects appear to have no metadata (no
  content-type, no etag, no user metadata, no tags).  A migration
  tool would need to re-write all xattrs from NooBaa format to Ceph
  NSFS format.
- **In-flight multipart uploads:** Completely invisible — different
  staging directory locations, naming, and data file models.  Must
  be completed or aborted on NooBaa before migration.
- **Versioning:** `.versions/` directory structure is similar but
  xattr divergence means version metadata is unreadable.  Version
  IDs may not match.

### Ceph NSFS → NooBaa

Same issues in reverse.

### Minimum viable migration path

If NooBaa on-disk compatibility is a goal:

1. **xattr translation layer** — read both prefixes on ingest, write
   one on output.  Handle encoding divergence (binary vs string).
2. **Require in-flight multipart completion** before migration — no
   cross-driver multipart interop is feasible without the shared
   data-file model.
3. **Version ID reconciliation** — verify format compatibility or
   accept version discontinuity at migration boundary.

A more complete approach would be native dual-format support, but
the encoding divergence (binary vs string) makes this expensive.

---

## 9. Carrying the link optimization forward

NooBaa's same-size link optimization is elegant and applicable to any
POSIX filesystem.  To carry it forward into our driver:

### Prerequisites

1. **Shared data file per size** — change the part-write path from
   one-file-per-part to one-file-per-unique-size.  The writer opens
   or creates `data/<size>` and writes at the correct offset.  This
   requires tracking the current write offset per unique size.

2. **Per-part offset tracking** — add `offset` to
   `NSFSUploadPartInfo` (struct_v 4, or combined with the `size`
   addition as struct_v 3).  The offset records where this part's
   data begins within the shared file.

3. **Wire `assemble_parts()` through FSStrategy** — make it a virtual
   method so GPFSStrategy can override with `gpfs_linkat`.

### Assembly logic

```
if (unique_sizes == 1 && parts_are_continuous) {
    // Common case: all parts same size
    link(data_file, output_path);    // or gpfs_linkat on GPFS
} else if (unique_sizes == 2 && parts_are_continuous) {
    // Typical case: all same except last part
    link(prefix_data_file, output_path);
    // append last part data
} else {
    // Rare case: mixed sizes or non-continuous
    copy_file_range() per part, as today
}
```

### Relationship to MultipartCache

The in-memory MultipartCache naturally stores per-part sizes in
`MultipartPartInfo.size`.  This could detect the same-size case
early and set a flag on the cache entry, avoiding the need to scan
part metadata at complete time.

### Filesystem considerations

- **XFS/Btrfs with reflink:** `copy_file_range()` already achieves
  block-level zero-copy via CoW.  The link optimization provides no
  additional benefit on these filesystems.
- **ext4:** No reflink support.  `copy_file_range()` does a full
  kernel data copy.  The link optimization would be a significant
  win.
- **GPFS:** No reflink in the standard API.  The link optimization
  via `gpfs_linkat` would be the primary zero-copy path.

---

## 10. Summary of corrective actions

### DESIGN.md updates needed

1. The GPFS integration surface table entry "gpfs_linkat splice"
   should be revised to accurately describe the prerequisite:
   "Shared-data-file model + `link()`/`gpfs_linkat`"

2. The phrase "gpfs_linkat splice" should be replaced with more
   precise language wherever it appears, since `gpfs_linkat` is an
   enhanced `linkat()`, not a data-concatenation operation.

### Code changes to evaluate

1. **NSFSUploadPartInfo struct_v 3** — add `size` field (safe,
   backward-compatible, already planned for MultipartCache work).

2. **Shared-data-file model** — significant architectural change to
   the part-write path.  Independent of MultipartCache but could
   be informed by it.

3. **FSStrategy::assemble_parts()** — make `assemble_parts` a virtual
   method on `FSStrategy` so GPFS can override the assembly path.

4. **xattr compatibility layer** — if NooBaa migration is a goal,
   design a read-both-write-one xattr layer.  Scope TBD.

### Documentation

This variance document should move to `src/rgw/driver/nsfs/` alongside
DESIGN.md and TODO.md once the analysis is reviewed and the corrective
actions are prioritized.
