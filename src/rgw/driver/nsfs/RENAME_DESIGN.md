# Rename: why it is hard, and why nsfs can do it anyway

## Purpose

`rgw_rename()` follows the NFS operation, whose contract is atomicity
with respect to other operations "where possible".  RGW cannot honour
that in general, and the reason is structural rather than an
implementation shortcoming.  This document records the structural
argument, the property that exempts nsfs from it, what the existing tests
were really telling us, and the semantics chosen for it.

Handle identity is the axis underneath most of what follows and is
treated separately in [HANDLE_IDENTITY.md](HANDLE_IDENTITY.md) — including
the measured result that a filesystem's own persistent handle *is*
rename-invariant, which would remove the constraint in §5.3.

Sibling documents: [DESIGN.md](DESIGN.md),
[ETAG_STRATEGY.md](ETAG_STRATEGY.md),
[NOOBAA_VARIANCE.md](NOOBAA_VARIANCE.md).

---

## 1. Why S3 cannot rename

S3 has no rename operation, and the absence is not an oversight.  A key
is an opaque string; the `/` in `a/b/c.txt` is a display convention that
`ListObjects` exposes through `prefix` and `delimiter`.  There is no
directory to move.

So renaming what a client sees as a directory means re-keying every
object beneath it.  That is unbounded work — a prefix may cover millions
of objects — and it cannot be made atomic, because there is no single
object whose mutation commits the whole change.  `RGWLibFS::rename()`
says as much in its own comment:

```
/* forbid renaming of directories (unreasonable at scale) */
```

and returns `-EPERM` for a directory.  For a file it does a
`RGWCopyObjRequest` followed by an unlink — O(size), not atomic, and it
loses the object's identity.

On rados this is the worst case.  The bucket index *is* the namespace, so
re-keying means rewriting N index entries, and version ids are stored
data, so preserving object identity across the move means deliberately
rewriting identity.  Development work moving namespace metadata into an
external store is expected to change this, at which point rados could
rename a metadata subtree — see §6.

---

## 2. Why nsfs is different

`resolve_parent_dir()` (`rgw_sal_nsfs.cc:768`) walks the key on `/` and
creates a real `Directory` for each component.  An S3 key `a/b/c.txt` is
genuinely `<bucket>/a/b/c.txt` on the filesystem.

The property that matters is stronger than "we have directories":

**An object's key is derived from its position in the tree, not stored
anywhere.**  `fill_cache()` composes it — `decode_obj_key(path_prefix +
get_name())` (`:1325`) — and no on-disk attribute contains a path.  The
nsfs xattr set is `bucket_info`, `mp_upload`, `object_type`,
`multipart_part_count`, `multipart_part_sizes`, `version_id`,
`delete_marker`, `non_current_timestamp`; the RGW attrs beside them
(etag, ACL, content-type) are equally position-independent.

Therefore moving a directory re-keys everything beneath it with no
per-object work.  One `renameat()`:

- **atomic**, by the filesystem's own guarantee, against concurrent
  operations on that name;
- **O(1)** in the number of objects beneath, where rados is O(N).

**That holds for a prefix, not for a single versioned object.**  nsfs puts
an object's history beside it rather than inside it —

```
<bucket>/photo.jpg                    <- current
<bucket>/.versions/photo.jpg_<verid>  <- older versions
```

— so renaming one versioned object is the leaf plus N version entries:
N+1 renames, and the set is *not* atomic.  Interrupted midway it leaves
versions orphaned under the old name.  Still cheap, since nothing but
metadata moves, but the atomicity claim above belongs to prefix rename
only.

| case | cost | atomic |
|---|---|---|
| unversioned object | 1 rename | yes |
| prefix / directory | 1 rename | yes |
| versioned object | 1 + N versions | **no** |

This is a consequence of our layout, not of versioned rename as such.
posix is heading for objects-as-directories, where an object's versions
live *inside* the object's own directory — one `renameat` moves the object
and its whole history atomically, and the row above disappears.  Noted as
context rather than a proposal:  rgw-standalone owns that schema, and
whether nsfs should follow is a separate question from rename.

Prefix fanout is planned there too — splitting wide directories by prefix
— and whether that preserves single-`renameat` prefix rename depends on
where the split lands relative to the logical directory.  Deliberately not
designed here;  treat it as an unknown that will need answering when it
arrives.

Three further properties fall out without special handling:

- `.versions/` and `.shadow/` are created with `mkdirat` on the
  directory's own descriptor (`:570`, `:692`), so they are children and
  move with it.
- **Version ids and non-MD5 etags survive.**  Both are
  `mtime-<base36>-ino-<base36>`, and renaming a directory touches neither
  its children's mtimes nor their inodes.  Object identity is preserved
  for free, which on rados would have to be reconstructed.
- The `.folder` sentinel carrying a directory's own attributes moves with
  the directory.

**posix caveat.**  In this tree posix derives keys from `path_prefix`
the same way and stores no key on disk, so the same approach would work.
The rgw-standalone direction — objects-as-directories with fanout —
may remove that property if names are hashed into fanout buckets, in
which case the hierarchy stops being the index.  Confirm with
rgw-standalone before assuming posix can or cannot follow.

---

## 3. What the tests said, and what was actually wrong

An earlier revision of this note reported all three tests in
`ceph_test_librgw_file_rename` failing on nsfs on a clean root, and
concluded that the v1 create sequence had been broken by FSIO — that
`rgw_open`/`rgw_write`/`rgw_close` no longer worked and the fix was a
compatibility decision about the v1 API.

**That was wrong.**  Rename works today for files, cross-bucket included;
the suite was reporting on itself.  Fixed in "test: open for write and for
create when creating an object".

`make_object()` called `rgw_open(fs, fh, 0 /* posix flags */, 0)` — that
is `O_RDONLY` with no `RGW_OPEN_FLAG_CREATE`.  On a driver with an FSIO
view `rgw_open()` opens the file for real, so it returned `-ENOENT`;
`rgw_write()` then had no open and returned `-EPERM`; nothing was written;
and every rename failed `-ENOENT` on a source that did not exist.
`librgw_file_nfsns.cc` has always passed
`O_RDWR` with `RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE`, and passes.

Two things made this hard to see, and both are the same shape:

- `make_object()` assigned four results to one `ret` and returned the
  last, so it reported `rgw_close()`'s status and swallowed the two
  failures that mattered.
- `RGWLibFS::rename()`'s step 0 declares its own `int rc` inside the
  `case 0:` block, shadowing the function's `rc`, which was initialised to
  `-EINVAL`.  A failed copy therefore returns `-EINVAL` and the real errno
  is visible only at `debug_rgw=1`.  **Still unfixed** — two lines, no
  design content.

The suite was also not re-run safe:  fixed names, `-EEXIST` from
`rgw_mkdir` on a second run, three failures for that instead.  Also fixed.

### 3.1 What the green suite does not tell you

It asserts only that the destination resolves.  Nothing checks that the
source is gone, that content survived, or that the etag or version id is
unchanged — so a rename that copied and forgot to delete passes all three.
That matters more now that it is green, because green invites trust.  Any
rename work wants those assertions first;  inode equality across the move
is the direct control that distinguishes a real rename from
copy-then-delete.

### 3.2 An unwired flag, noticed on the way

`RGWFileHandle` carries `FLAG_CREATING` with `creating()`,
`open_for_create()` and `clear_creating()` (`rgw_file_int.h:327, 730,
795, 800`), and nothing in the tree calls any of them.  It is not the
mechanism above — `RGW_OPEN_FLAG_CREATE` is, and it works — so this is a
separate question:  whether handle *state* ("this handle names an object
that does not exist yet") expresses something the per-open request flag
does not, or whether it is redundant and should go.  Unresolved;  recorded
so it is not mistaken for the cause of anything.

## 4. The semantic question

This is the part that implementation cannot decide.  S3 has no rename, so
what does an S3 client see afterwards — particularly under versioning,
where the objects being moved have version history?

**(a) Namespace operation, transparent to S3.**  Objects appear at the
new keys with version history intact, because version ids survive the
move.  The only option that keeps atomicity and O(1).  But no S3
implementation moves version history, so an S3 client observes something
the API cannot express.  A deliberate, documented divergence — the same
category as the `check_empty` decision.

**(b) Copy plus delete-marker.**  New keys receive new versions, old keys
receive delete markers.  Exactly correct S3 semantics, and it discards
every advantage in §2.  This is today's behaviour.

**(c) Refuse under versioning.**  Rename on unversioned buckets only,
`-EPERM` otherwise.  Honest, cheap, and leaves versioned buckets — the
configuration this driver is being built for — without the operation.

### 4.1 What (a) means concretely

An S3 client sees the object leave the old key and appear at the new one,
carrying its versions:  `ListObjectVersions` at the new key returns the
same versions with the **same version ids**, since ids are
`mtime-<base36>-ino-<base36>` and a rename changes neither.

Cross-bucket follows a rule rather than a fudge:  history can only move
somewhere able to hold it.

- source has no history -> any target;
- source has history -> target **versioned or suspended**:  history moves;
- source has history -> target **unversioned**:  refuse, *unless* the
  caller asks for it explicitly.

Suspended qualifies because such a bucket still holds non-current
versions, it merely stops minting new ones.

The explicit case is worth having rather than a flat refusal:  flattening
an object to its current version is a legitimate thing to want, and what
makes it dangerous is doing it *silently*.  So gate it on a flag —
`rgw_rename()` already takes a `uint32_t flags` and defines only
`RGW_RENAME_FLAG_NONE`, so this is additive with no ABI change.  With the
flag the move slices the history off, keeping the current version;
without it the rename fails rather than discarding anything.

The granularity that falls out is the right one.  `rename(2)` has no such
flag, so **no client can ask for slicing on a particular `mv`** — an NFS
user moving an object between buckets gets an error, never quiet history
loss.  But it is not unreachable from NFS either:  Ganesha has places to
bind policy, the Export block among them, so an administrator can decide
that a given export slices, once and deliberately, and the FSAL passes the
flag.  Account-scoped properties would give a second binding point when
they exist.

That is the correct shape for this:  a standing administrative decision
rather than a per-operation one, made by someone who can see what the
export is for, and never inferred from a client's `mv`.

**Recommendation: (a), stated as a divergence.**  For a gateway whose
primary interface is NFS, the filesystem's semantics are the contract and
the S3 view is derived.  (b) is what we have and it is not worth keeping.
(c) is defensible as a first step if (a) is contentious, and it is
strictly on the way to (a).

---

## 5. What was built

Landed 2026-09-11 as a single cut in seven commits — SAL seam, cache
primitives, nsfs implementation, `rgw_file`, tests, and recovery.  Phasing
was considered (knob default-off, unversioned before versioned) and
rejected:  it would have created a transitional state where unversioned
objects moved and versioned ones copied, which is a worse inconsistency
than landing the novel machinery with the seam.

1. **A rename path in the driver.**  `Object::rename()` on the SAL,
   `-ENOTSUP` by default;  nsfs implements it as `renameat()` between
   resolved parent descriptors, gated on `FSStrategy::can_rename()` (so
   GPFS declines) and on `rgw_nsfs_enable_rename`.  Cross-bucket is the
   same call — buckets are top-level directories.  `EXDEV` is an error,
   not a copy fallback that would silently reintroduce O(size).
2. **Listing-cache rekey, not invalidation.**  The first cut dropped the
   whole bucket's cache.  That was wrong:  a rename touches N+1 entries and
   dropping the bucket costs a rebuild unbounded in bucket size.  Because
   keys sort `name '\0' instance`, a key and all its versions are
   contiguous, so `BucketCache::rename_entries()` rekeys them in one
   bounded cursor walk per chunk.  Nor are the entries re-described from
   disk:  a rename changes the key and nothing else, so re-reading each
   object would duplicate `fill_cache()`'s job and risk describing it
   differently than before.  Chunked at 1024 rather than buffered whole,
   since a key's version count is client-controlled.
3. **Invalidation** — see §5.2.  Unchanged in design;  the handle is
   rekeyed in the `FHCache` rather than retired, and `set_times()` still
   moves both parents' change attributes, which is what makes a Linux
   client re-resolve.
4. **The open-file refusal is kept.**  Whether it extends to a subtree is
   still open;  see §5.3.
5. **Directory rename remains `-EPERM`.**  It is the prefix case, and §6
   argues the prefix interface should not be derived from the single-object
   one.  A directory object resolves to `nsfs::Directory` and
   `NSFSObject::rename()` returns `-ENOTSUP` for it deliberately.

### 5.1 Concurrency and crash atomicity

Two separate problems.  Concurrency is about what another caller may
*observe* mid-move;  crash atomicity is about what is left behind.

**Why ordering alone is not enough.**  Listing enumerates `.versions/` by
readdir and emits entries whether or not the leaf exists — required,
because after the current version is deleted the rest must still list, so
it cannot change.  Therefore orphan entries at the destination are visible
as versions of a key that is not otherwise there, and the source lists with
shrinking history while the move runs.  Suppressing both sides would need
listing to synthesise entries from wherever they physically are:  a journal
with redirect.  Rejected as disproportionate.

**Chosen — sequential atomicity.**  Rename holds the per-directory version
lock (`FSStrategy::version_lock`, already used by `copy_object`,
`publish` and `delete_obj`) for the duration.  Listing does **not** take it.
So a caller who races a rename may observe a torn state and a caller who
does not, never does.  That is precisely the guarantee the existing version
paths already give — listing does not lock against demote or promote today
— so rename introduces no new weakness and costs nothing on the listing
hot path.  Within the lock the ordering still matters:  **versions first,
leaf last**, so the leaf rename is the commit point and the object is
resolvable at exactly one key throughout.

**Rejected — atomicity by mutual exclusion.**  Lock both directories and
have versioned *listing* take the lock too, so no concurrent caller can
observe an intermediate state.  Strictly stronger, and not chosen because
it makes every versioned listing block behind any in-flight version
mutation — a real cost on a hot path and the first thing a reviewer would
ask about — and because two-directory locking needs a canonical order (by
inode) or a reverse-direction rename deadlocks against it.  Worth
revisiting only with a measurement of the listing cost;  it is the upgrade
path and the chosen model does not block it.

**The intent record.**  One xattr on the *source* `.versions/` directory,
written before the first move and cleared after the leaf lands:

    user.nsfs.rename_intent = leaf \0 tenant \0 bucket \0 key \0 flags

Two xattr operations for the whole move, whatever the version count.  It
carries *what the move was*, never how far it got — recovery derives the
state from the filesystem, which is cheaper and cannot go stale:

    leaf still at the source  -> never committed -> ROLL BACK
                                 (return any versions found at the
                                  destination)
    leaf at the destination   -> committed       -> ROLL FORWARD
                                 (send the history still behind after it,
                                  or drop it if the move was sliced)

**The leaf's location is the commit record**, which falls out of
versions-first-leaf-last and is what makes the record lightweight.

The slice flag has to be recorded even though nothing else about the move
does.  The same on-disk state — leaf committed, history still at the source
— means "forward this" for an ordinary move and "delete this" for a sliced
one, and only the record can say which.

Present at both keys, or at neither, is not a state the sequence can
produce:  `renameat()` is atomic and the version lock excludes the other
mutations.  It means something outside the driver has been at the directory
— most plausibly a client recreating the source key after the crash.
Recovery refuses and keeps the record rather than guess, because destroying
real data is worse than staying stuck.

**When recovery runs.**  At version-lock acquisition, in
`version_lock_recovering()`, which all eight lock sites now go through.  So
the next write to *any* object in that directory repairs it — not only the
next rename, which might never come.  A failed recovery does not fail the
caller:  the record may describe a different object, and refusing every
write to a directory because one object's move is stuck is worse than the
delta.  `rename()` is the exception and refuses, because there is one
record per directory and writing a new one would lose the only pointer to
the stranded versions.

Read paths deliberately do not repair.  `Directory::fill_cache()` notices
the record and says so in the log, but does not take the version lock:
that would put a mutation on the read path and create a
bucket-cache-entry-lock -> version-lock ordering that exists nowhere else.
The cost is that a listing can show the delta until the next write to that
directory.  A background sweep is not in scope.

### 5.2 Invalidation

What exists today, and it is deliberate:  `rename()` calls `set_times(t)`
on **both** `src_fh` and `dst_fh`, moving ctime/mtime/atime on the two
directory handles.  A Linux NFS client watches a directory's change
attribute, so moving it is what makes the client drop cached readdir and
lookup results and re-resolve — which is how a client detects a moved
object.  This is the mechanism;  it is not incidental.

The upcall is a separate surface.  `RGWFileHandle::invalidate()` fires
`fs->invalidate_cb(arg, fh_hk)`, registered through
`rgw_register_invalidate()`, and tells the consumer to drop a cached
handle.  Today its only caller is the GC path (`rgw_file.cc:1607`), on
readdir expiry, guarded so it does not fire mid-readdir-cycle.  `rename()`
does not use it.

**The handle is derived from the path, and that is the crux.**  Lookup is
by `fh_key`, a hash tuple rather than a name comparison — but the tuple is

    fh_hk.bucket = XXH64(tenant : bucket)
    fh_hk.object = XXH64(tenant : full-path-from-bucket-root)

`make_fhk()` composes the object half through `make_key_name()`, which is
`full_object_name() + "/" + name`.  So the key is position-dependent even
though nothing compares names.

`rgw_fh_hk` is not private bookkeeping.  It is public API
(`include/rados/rgw_file.h:51`), it *is* `struct rgw_file_handle`'s
identity — the header calls it a "content-addressable hash" — and
`rgw_lookup_handle()` takes one.  That is the path by which a consumer
turns a wire NFS filehandle back into a live handle.

Two consequences, and the first corrects a natural assumption:

- **Clients are not automatically fine.**  In a real filesystem an NFS
  filehandle names the inode, so it survives a rename;  here it is
  content-addressed over the path, so a rename *changes the object's
  filehandle*.  A client holding the old one gets nothing back from
  `rgw_lookup_handle()`.  This is why the invalidation work was
  load-bearing rather than a nicety:  the handles genuinely do go stale,
  and the change-attribute bump is what drives the client to re-resolve by
  name before it tries to use one.
- **Directory rename multiplies it.**  Every descendant's
  `full_object_name()` changes, so every descendant's key changes — for
  cached handles inside librgw and for whatever filehandles clients are
  holding.  Any client with files open beneath the subtree is exposed to
  ESTALE unless the rename drives invalidation across the whole moved
  subtree.

The addressing scheme should stay as it is;  making keys
position-independent would be a far larger change and is not wanted.  The
implication is that rename must *deliberately move handles* — re-key each
cached handle beneath the subtree and drive the invalidations that make
clients re-look-up — rather than assume anything survives on its own.

This also refines the cost claim in §2.  The filesystem work is O(1):  one
`renameat()`, no per-object I/O.  The bookkeeping is O(cached handles
beneath the subtree), which is bounded by the handle cache rather than by
the number of objects, and is memory-only.  That is still categorically
better than rados's O(N) index rewrite, but it is not free, and it is the
part that needs care.

### 5.3 Why open files are refused — the guard is correct

An earlier revision of this note called the `-EPERM` on open files an
artefact of copy-then-unlink with no semantic behind it.  That is wrong,
and the reason follows directly from §5.2.

**What an NFS client has open is a filehandle**, and the filehandle is
exactly the bits that change when the object moves, because it is a hash
of the path.  There is no transformation the client can follow:

- it holds the *old* bits, and nothing pushes it new ones;
- for an already-open file it has no name to re-resolve from — under v4 it
  may have opened by handle and never known one;
- `ESTALE` on an open descriptor is fatal to the application.  Unlike a
  failed lookup, there is no recovery path.

So re-keying an open handle repairs librgw's internal view while leaving
the client's reference dead.  It fixes the half that does not matter.
Refusing the rename is the correct behaviour for a content-addressed
handle, and the guard should stay.

**This is the real constraint on directory rename**, more than any
bookkeeping cost.  Moving a subtree changes the filehandle of every object
beneath it, so it cannot be done while any of them are open without
handing those clients ESTALE.  The options are:

- **Refuse when anything beneath is open.**  Extends the existing
  file-level guard to a subtree.  librgw knows its own open handles, so
  the test is a bounded scan;  the cost is that a single open file
  anywhere beneath blocks the rename.
- **Accept ESTALE for open files under a moved subtree**, and document it.
  Defensible only if directory rename is rare and the exposure is stated.
- **Stop deriving handles from the path**, which is a much larger change
  and is not wanted — see below for why it is not merely a matter of
  taste.

### 5.3.1 Why the handles are content-addressed at all

An NFS filehandle must be *persistent*:  a client may present one after a
server restart, and the server has to resolve it.  Hashing the path makes
a handle reconstructible with no persistent handle table — that is what
the "content-addressable hash" in the public header buys, and it is a
real architectural property rather than a convenience.

The cost is precisely the rename problem:  a handle that is derived from
where an object *is* cannot survive the object moving.  Making handles
stable across renames means introducing a durable handle-to-object
mapping — which is the same external-metadata-store direction that would
let rados rename a subtree (§6).  The two problems converge on the same
answer, which is an argument for not solving this one locally in a way
that has to be undone later.

### 5.4 Tests

`ceph_test_librgw_file_rename`'s three original tests only asserted that the
destination resolves.  None checked that the source was gone, that content
survived, or that the etag or version id was unchanged — so a rename that
copied and failed to delete would have passed all three.

The net is now in `ceph_test_librgw_file_write2` (`OPEN2.RENAME_*`), which
runs through open2 against a filesystem-backed driver:  source absent
afterwards, content identical, etag unchanged, **inode equality across the
move** — the control that distinguishes `renameat` from copy-then-delete —
cross-bucket, over an existing key, directory refused, open file refused,
history moved with the object, a versioned source refused into a bucket
that cannot hold history, slicing when asked, rollback when a move fails
partway, and the three recovery cases below.

Two of these are boundary markers rather than ordinary assertions, and are
meant to go red when the design moves:

- `RENAME_PRESERVES_INODE` asserts rename is a move and not a copy.  It is
  achievable on any filesystem backend, and on a future metadata-backed
  rados by rebinding a name;  it is not achievable on rados today.  The
  suite is scoped to filesystem layouts already, but this should gate on a
  driver capability once one exists.
- `RENAME_REFUSES_DIRECTORY` records where the design stops.  nsfs *could*
  do prefix rename in one `renameat`;  when that lands this test should be
  inverted rather than deleted.

Recovery needs an injected failure that leaves the intent record, because
the ordinary error paths all clean up after themselves:
`inject-rename-fail-after` takes `abandon=versions|leaf`, which gives up
without rolling back — the state a crash leaves, and the only state the
recovery path can be tested against.  The three tests trigger recovery with
an *unrelated write* into the same directory rather than by retrying the
rename, since a retry would pass whether or not recovery ran.  Verified to
fail with recovery disabled:  those three and nothing else.

---

## 6. Generality, and why the interface shape is the risk

The external metadata schema being designed for rados introduces a
**name-to-object-id indirection**, precisely so that objects can be renamed
without moving data — rename becomes a remapping of names onto ids.  So
rename in that world is not a filesystem operation at all, and NFS rename
will be required there.

Two things follow, and they pull in opposite directions.

### 6.1 The right place to prototype, the wrong place to generalise from

nsfs is the only backend that can do **prefix** rename cheaply today:  one
`renameat` moves a whole subtree, because the keys beneath it re-derive
(§2).  That makes it the natural place to build and exercise the operation,
and it is a genuine precursor to the metadata work rather than a detour.

But in full generality the metadata feature is **prefix rename**, and the
SAL interface for that is probably *not* "rename one object".  Single-object
rename is the degenerate case of a prefix operation, not the foundation of
one — build the easy case first and the general case arrives bolted onto
something shaped wrong.  Two specific traps:

- **Shape.**  If the eventual rados mechanism is a name-to-id rebinding,
  then the primitive that generalises is closer to *rebind this name (or
  name prefix) to this location* than to *rename this file*.  nsfs fulfils
  a rebind with `renameat`;  metadata-rados fulfils it with an index
  update.  An interface modelled on the filesystem call would not survive
  the translation.
- **Guarantees.**  The complexities genuinely differ and do not converge.
  Prefix rename is one call on nsfs and O(n-objects) name updates under the
  indirection.  An earlier revision of this document concluded that the
  interface should therefore let a driver *report* what it can guarantee —
  atomic, or linear and interruptible, or unsupported.  **That is retired.**
  The goal for the metadata case is prefix rename atomic *from the caller's
  view*, by whatever mechanism;  a backend limitation reported upward is
  exactly the leak the interface must not have.  What remains true is the
  narrower warning:  validating a prefix interface against nsfs alone
  invites encoding nsfs's single-call implementation into the contract.

We will not know for some time whether an operation designed now is the
right one.  That argues for the capability seam used elsewhere in this
note, and against freezing a signature early.

### 6.2 What generalises regardless

The *semantics* in §4 do:  if rename is a namespace operation that carries
history, that is a property of RGW's filesystem-like namespaces generally,
and one argument covers nsfs, posix and a later metadata-rados alike.  The
divergence from S3 is then not an nsfs-local special case.

---

## 7. Status

**Implemented for nsfs, provisionally** — see §6 for why the interface
shape, not the implementation, is the risk.  Landed 2026-09-11;  `posix`
declines through `can_rename()` and so does GPFS.

§4 is settled:  (a), history moves with the object, cross-bucket gated on
the target being able to hold it, slicing available as export policy.
Slicing is reachable through `RGW_RENAME_FLAG_SLICE_VERSIONS`, which
`rgw_file` drives from its own configuration;  nothing further is needed to
bind it to export policy.

Two things were found on the way, neither planned.  The first was
pre-existing:  `publish()` declared its `DemoteResult` inside the block that
takes the version lock, so the result died there and the listing cache never
flipped the demoted entry from current to non-current — two `IsLatest=true`
for one key over S3.  It was hidden because the first listing of a bucket
fills from the store and derives the flags correctly, and the test whose
comment says it reads the incremental cache deliberately was always the
first lister, so it always got the rebuild it meant to avoid.  The second
was the measurement that overwrite changes the inode, which parked the
whole open-by-handle direction;  see HANDLE_IDENTITY.md.

### 7.1 What is not done

- **Directory (prefix) rename.**  §6 argues it should not be derived from
  the single-object interface, so this is a decision outstanding rather
  than a missing implementation.  Deferred to a later level-up of this work,
  along with **whether the open-file refusal should extend to a subtree**
  (§5.3), which only matters once prefix rename exists.
- **`FLAG_CREATING`** patches the same seam late identity would (§3.2).  If
  open-by-handle is ever unparked, the inference should be *replaced* by it
  rather than living alongside it — two mechanisms for one window is how
  they drift.
- **A background sweep** for intent records.  Not in scope:  recovery runs
  at the next version-lock acquisition on the directory, and a listing
  reports the delta in the log meanwhile (§5.1).
- **The intermediate commits were not individually built.**  Dependency
  order makes each compilable by construction, which is reasoning rather
  than a test.
