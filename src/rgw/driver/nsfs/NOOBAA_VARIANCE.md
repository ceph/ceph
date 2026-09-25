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

## Which NooBaa this describes

Every claim here about NooBaa's behaviour was read from **noobaa-core
`68ca22d33` (master, 2026-05-27)**, checked out at
`~/dev/noobaa-core`.  Cite that revision, not "NooBaa", when relying on
anything below.

This matters more than it usually would.  NooBaa is under active
development, and Spectrum Scale has its own aspiration toward
high-fidelity S3 -- so the claims most likely to go stale are precisely
the ones this document uses to describe what NooBaa *cannot* do:  that it
has no owner record, no S3 ACL vocabulary, and stores no part count or
part sizes on a completed object.  Absences are what a fidelity PR fills
in.  The on-disk conventions in the other direction -- the `.folder`
sentinel, the version path layout, the staging tree -- have data written
in that shape in the field and are far less likely to move.

Re-read the revision before treating an absence here as a design
premise.

### Upstream survey, 2026-09-24

`origin/master` is **377 commits** ahead of our snapshot and its head is
one day old.  What that changed, for the claims here:

**The xattr key set is unchanged.**  The only difference between
`68ca22d33` and `origin/master` in the constants block is a refactor
introducing `XATTR_RETENTION_PREFIX`, which yields the same two retention
keys.  No ACL, owner, or part-count attribute has appeared.  So the
absences this document relies on still hold in merged code.

**But fidelity is actively being closed**, in exactly those areas:
object lock (#9881, #9896, #10051), bucket policy and ARNs (#10052),
conditional request metadata (`eadecc66e`), and MPU error semantics --
`a91d56f99` makes CompleteMultipartUpload surface InvalidPart rather than
InternalError, which is the same conformance point we were failing until
`e7512f634c2`.  Treat "NooBaa cannot" as a statement with a date on it.

**#10049 does NOT affect us**, despite its title.  "MPU performance
improvement - Defer parts DB ops" touches `object_services/*` and
`md_store.js` -- the containerized metadata store -- and not
`namespace_fs.js`.  The NSFS staging layout section 2 describes is
unaffected, and S5's target does not move.

**#10071 is the one to read.**  "Implement atomic if-none-match for POSIX
and GPFS" changes `namespace_fs.js` to publish with
`linkfileat(..., should_not_override=true)`, so a put-if-absent fails
with EEXIST instead of racing, and it ships a five-writer concurrency
test.  It also *removes* entries from the Ceph s3-tests pending lists --
NooBaa measures NSFS fidelity against the same suite we do.

That last one reflects back on us.  `FSStrategy::link_temp_file()`
publishes with `gpfs_linkat(AT_EMPTY_PATH)`, which atomically *replaces*;
we have no link-if-absent variant on the publish path, and
`GPFSStrategy::clone_file(excl)` still does faccessat-then-act, which its
own comment admits narrows the race without closing it.  We already bind
and use `gpfs_linkatif` for CAS links (`fs_strategy.cc:689`), so the
primitive is in hand.  Whether our If-None-Match path has the race they
are closing is worth checking directly.

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
decimal strings, structured data is JSON.

Our side is **not one thing**, and the original claim here — that we use
Ceph binary encoding throughout, making the wire formats "completely
incompatible" — was too strong.  `write_x_attr()` writes the bufferlist's
raw bytes, and what is in the bufferlist depends on the attribute and on
the code path that filled it:

- **Plain strings.**  `RGW_ATTR_ETAG` holds bare hex;  on disk an object
  carries `user.nsfs.rgw.etag = "e14316878dab30f766e8d8119fdc92da"` as 32
  ASCII bytes, verified by `getfattr -e hex` (2026-09-23).  Content type
  and user metadata are strings too.  These differ from NooBaa in the
  *key* only.
- **Ceph-encoded structures.**  `RGW_ATTR_ACL`, retention, and the
  multipart part-size vector really are `ENCODE_START` payloads, and
  those need a codec or -- better -- to disappear behind a question on
  XattrStrategy, as ownership already has.
- **Counted strings including the terminator.**  RGW's convention for
  request metadata is `bl.append(v.c_str(), v.size() + 1)`
  (`rgw_op.h:2478`, `rgw_op.cc:3861`, `rgw_op.cc:5545`), so the trailing
  NUL is part of the value and lands on disk.  NooBaa writes no
  terminator.

**And we do not apply that convention uniformly.**  The librgw paths
append `xattr.val.len` verbatim (`rgw_file.cc:1493`, `:1519`), so the same
key holds `round` (5 bytes) when set over NFS and `round\0` (6 bytes) when
set by an S3 PUT.  Two consequences beyond migration:  a `noobaa` format
must *normalize* values rather than rename them, and an object PUT over S3
and read back with `getxattr` over NFS hands the client a trailing NUL.

**Etag quoting is a related, recurring hazard.**  nsfs stored the etag
*with its surrounding quotes* until `15386ec4a26` ("rgw/nsfs: store etags
as bare hex"), and `856e91963c4` / `79f77ca016a` had to unquote on the
comparison side in nsfs and posix.  The quotes belong to the HTTP
representation;  anything reasoning about the stored value has to assume
they may reappear.

### Complete attribute mapping

| Attribute | NooBaa xattr | Ceph NSFS xattr | Value format |
|-----------|-------------|-----------------|--------------|
| ETag | `user.content_md5` (no `noobaa.` prefix!) | `user.nsfs.rgw.etag` | both bare hex strings -- the key differs, the value does not (see above) |
| Content-Type | `user.noobaa.content_type` | `user.nsfs.rgw.content_type` | both raw strings -- key only (verified, see below) |
| Content-Encoding | `user.noobaa.content_encoding` | `user.nsfs.rgw.content_encoding` | both raw strings -- key only |
| Version ID | `user.noobaa.version_id` | `user.nsfs.version_id` | **identical**:  `mtime-<b36>-ino-<b36>`, sentinel `null` |
| Delete marker | `user.noobaa.delete_marker` | `user.nsfs.delete_marker` | **identical**:  the literal `"true"`, written only when true, never cleared |
| Dir content | `user.noobaa.dir_content` (marker *in addition to* `.folder`) | N/A | see section 1.1 -- the sentinel is shared, this marker is not |
| Object tags | `user.noobaa.tag.<tagkey>` (one per tag) | `user.nsfs.rgw.x-amz-tagging` (single blob) | completely different structure |
| Legal hold | `user.noobaa.legal_hold` | `user.nsfs.rgw.obj-legal-hold-status` | different key and encoding |
| Retention mode | `user.noobaa.retention_mode` | `user.nsfs.rgw.obj-retention` | different key and encoding |
| Retention date | `user.noobaa.retention_date` | (embedded in retention blob) | — |
| Non-current timestamp | `user.noobaa.non_current_timestamp` | `user.nsfs.non_current_timestamp` | **identical**:  decimal milliseconds since the epoch |
| User metadata | `user.<key>` (raw, no prefix) | `user.nsfs.rgw.<key>` | NooBaa: passthrough, no terminator; Ceph: prefix-swapped, terminator present or not depending on the writing path |
| ACL | (not stored as xattr by NooBaa) | `user.nsfs.rgw.acl` | Ceph-specific, ceph-encoded |
| Object type | (inferred from stat) | `user.nsfs.object_type` | Ceph-specific enum |
| Multipart part count | (not stored on final) | `user.nsfs.multipart_part_count` | Ceph: encoded uint16 |
| Multipart part sizes | (not stored on final) | `user.nsfs.multipart_part_sizes` | Ceph: encoded vector<uint64_t> |
| GPFS DMAPI | `dmapi.IBM*` (4 attrs) | N/A (not yet integrated) | — |
| GPFS encryption | `gpfs.Encryption` | N/A (not yet integrated) | — |

**Impact:** A completed NooBaa object's xattrs are largely unreadable by
our driver -- but far less uniformly than this document claimed twice
over.  Our driver would treat NooBaa objects as having no metadata, and
the reverse is equally true, yet the *reason* is the key in most rows and
the value in only a few.

### What is actually ceph-encoded (verified 2026-09-25)

This document said "Ceph: encoded" for five rows.  That was wrong for all
five, and it mattered:  it made the value-format problem look four times
larger than it is.

The plain scalars are raw bytes on our side too.  Version id is written
and read with a bare `fgetxattr` and compared as a `string_view`
(`is_null_version_fd`, `rgw_sal_nsfs.cc:308`);  the etag likewise
(`etag_from_fd`, `:326`);  content type is
`bl.append(mime.data(), mime.size())` (`:6753`);  the non-current
timestamp is `std::to_string(millis)` written straight to the attribute
(`:1088`);  the delete marker is presence, read into an 8-byte buffer
nobody parses (`:1734`).  RGW passes attributes as `bufferlist`, which
looks like an envelope, and `decode_attr()` sits nearby -- but for a
scalar the bufferlist just holds the bytes.

What *is* ceph-encoded is the structured set:  the ACL
(`RGWAccessControlPolicy`), `object_type`, the multipart part count and
sizes, `bucket_info`, and the object-lock blobs.  Every one of those is
either ours-only or differs in kind, so none of them was ever going to
rename cleanly.

So the value-format work in S5 is three things, not eight:  object tags,
the object-lock family, and the user-metadata terminator.

### Three rows are already byte-identical

**Version id.**  `_get_version_id_by_stat()`
(`namespace_fs.js:126-129`) returns
`'mtime-' + mtimeNsBigint.toString(36) + '-ino-' + ino.toString(36)`, and
`NULL_VERSION_ID = 'null'` (`:90`).  Ours is the same string and the same
sentinel (`rgw_sal_nsfs.cc:93`, `:634`).  Only the key differs.

**Non-current timestamp.**  Theirs is `String(Date.now())`
(`namespace_fs.js:2735`), ours is `duration_cast<milliseconds>` of the
system clock (`rgw_sal_nsfs.cc:1083`).  Same unit, same epoch.

**Delete marker.**  Both write the literal `"true"`
(`namespace_fs.js:2712`, `rgw_sal_nsfs.cc:8315`), both write it only when
the marker is being created, and neither ever clears it --
`_clear_user_xattr` is never called with `XATTR_DELETE_MARKER` anywhere
in their tree.  A boolean whose only value is `"true"` is a presence
flag, which is why our presence test and their `=== 'true'` both give the
right answer against either side's trees.

There is no transition that would write `"false"`, because a delete
marker is a whole file in `.versions/` rather than a state on a file:
promotion unlinks the marker instead of un-marking it.  The attribute
that *does* describe a mutable state is the non-current timestamp, and
there both projects remove it on promotion rather than falsify it
(`namespace_fs.js:2758`, `rgw_sal_nsfs.cc:549`) -- arrived at
independently, and the same answer.

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

## 1.1 Object naming

Not previously covered here, and it is where the next round of work lands,
so it is recorded with a decision per row.

**Ordinary keys are already identical.**  A key which does not begin with
`_`, does not end in `/` and carries no namespace is stored verbatim by
both, byte for byte.  That is the overwhelming majority of objects.

**Leading underscore — we diverge, and ours is the accident.**
`get_key_fname()` goes through `rgw_obj_key::get_index_key_name()`
(`rgw_obj_types.h:192`), which doubles a leading underscore:  the key
`_foo` becomes the file `__foo`, where NooBaa writes `_foo`.  The doubling
exists because a rados bucket index shares a keyspace with entries spelled
`_<ns>_<name>`, so a key beginning `_` would collide.  A directory has no
such keyspace, so on a filesystem it buys nothing and makes the object
misnamed to the other gateway.

It is symmetric -- `parse_raw_oid()` (`rgw_obj_types.h:285`) undoes it --
and that symmetry is load-bearing:  writing `_foo` bare while still
parsing with `parse_raw_oid()` would send `_foo_bar` down the namespace
branch and mis-split it into namespace `foo`, name `bar`.  Both halves
have to change together, which is why they now live on one object
(`PathStrategy`).

**Decided 2026-09-23:  drop the doubling for nsfs, in favour of NooBaa
compatibility.**  Nothing is in the field.  Scoped to
`src/rgw/driver/nsfs/`;  the posix driver keeps its own naming and is not
touched.

**Directory objects — we already agree, except when empty.**  An earlier
revision of this document said we "use the `.folder` sentinel
differently".  That is wrong.  NooBaa uses the same sentinel with the same
spelling:  `config.NSFS_FOLDER_OBJECT_NAME = '.folder'` (`config.js:848`),
appended to any key ending in `/` (`namespace_fs.js:2662`), and stripped
back off in listings (`:944`).  A `.folder` we write is listed correctly
by NooBaa.

The real divergence is one optimization.  `XATTR_DIR_CONTENT` is an
*additional* marker on the directory holding the content length, and for
an **empty** directory object with versioning disabled
`_create_empty_dir_content()` writes the metadata as xattrs on the
directory and then unlinks `.folder`.  When the marker is not `'0'` the
content is read from `.folder`, as we do.

So the asymmetry is one-way:  NooBaa's empty directory object has no
sentinel and is invisible to us;  ours keeps a zero-length `.folder` and
stays visible to them.

**Decided 2026-09-23:  follow NooBaa.**  Recognising
`user.noobaa.dir_content` on read is what stops an empty directory object
being invisible, and is separable from whether we adopt the
unlink-on-empty behaviour on write -- that second half is the only part
which changes what we produce.

**Namespaced keys** (`_<ns>_<name>`, prefixed with `.`) are multipart
staging metadata.  They belong to `MPUStrategy`, whose divergence is
section 2.

---

## 1.2 Bucket-level state lives outside the tree

Sections 1 and 1.1 are about objects.  Bucket-level state is a different
problem, and the more consequential one.

**NooBaa keeps all of it in a config store, not in the data tree.**  The
NC bucket record (`src/server/system_services/schemas/nsfs_bucket_schema.js`)
holds:

    owner_account, bucket_owner, tag, versioning, path, creation_date,
    s3_policy, encryption, website, force_md5_etag, logging,
    lifecycle_configuration_rules, notifications, object_lock_configuration

That is a JSON file under their config directory, read, parsed and
schema-validated per access by `config_fs.js`.  In the non-containerized
deployment -- the one Scale ships -- it is not a serialization of
anything;  it IS the runtime representation.  Containerized NooBaa uses
PostgreSQL instead, through `system_store.js`.

**We keep the same facts in `user.nsfs.bucket_info` and the bucket
directory's attributes**, which is why our tree is self-describing and
theirs is not:  move a NooBaa bucket's path and it is separated from its
own configuration, while ours travels with the directory.

**The consequence for a NooBaa-format bucket.**  Our S3 interface has no
*feature* deficit -- RGW-on-nsfs implements versioning, bucket policy,
lifecycle, notifications and object lock, and the suites exercise them.
The deficit is that on an unmarked tree there is nowhere to put any of
it.  It is not a list of missing features;  it is the whole bucket-level
surface, unreachable for want of a place to write.  Object-level state
is the opposite case:  retention, legal hold, tags, content type and
etag are xattrs on both sides, so they are a naming-and-encoding problem
the strategies can solve.

So serving an unmarked bucket faithfully requires reading their config
store, and handing a tree back requires writing it.  That adapter, not
the strategies, is the real cost of the unmarked profile.  It is a
migration-lifetime adapter and not an architecture:  RGW has durable,
transactional metadata on every backend already, and a second store for
the same facts is the one-fact-in-two-places failure this document keeps
finding.

## 1.3 S3 Vectors:  same engine, different binding

Both projects are building S3 Vectors, and both embed **LanceDB** --
noobaa-core carries `@lancedb/lancedb` as a direct dependency with a REST
surface under `src/endpoint/vector/` and its own
`nsfs_vector_bucket_schema` in the config store.  Earlier reading
suggested they intended a passthrough to other implementations;  current
master does not do that.

The difference is where the facility lives.  RGW embeds LanceDB in the
gateway, converged and resident on whichever backend the instance runs.
A NooBaa vector bucket is bound to a namespace store -- there is a commit
preventing deletion of an NSS while a vector bucket uses it.

Two things follow.  Whether the two write compatible Lance datasets is a
concrete question nobody has looked at, and it belongs in this document
once someone does.  And a vector bucket writes structures plain NSFS
knows nothing about, which makes it a marked-profile feature by
construction -- a whole bucket *type* rather than an extra file beside an
object, and a better argument for the marker than the shadow tree is.

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

5. **FSIO default ACL** — the FSIO positional-IO path stamps a
   default `RGW_ATTR_ACL` (as `user.nsfs.x-rgw-acl`) on newly
   created shadow objects using Ceph binary encoding
   (`RGWAccessControlPolicy::encode`).  NooBaa does not store ACLs
   as xattrs at all.  This is a new divergence introduced by the
   FSIO work — any future noobaa compatibility layer must handle
   or ignore this attribute.

### Documentation

This variance document should move to `src/rgw/driver/nsfs/` alongside
DESIGN.md and TODO.md once the analysis is reviewed and the corrective
actions are prioritized.
