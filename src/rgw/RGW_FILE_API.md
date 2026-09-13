# librgw's file API, for a consumer that wants the whole contract

## Purpose

`include/rados/rgw_file.h` has accumulated two generations of interface.  The
older one works and is not going away;  the newer one is what a consumer
should be written against, and the difference is not stylistic — several
semantics are simply inexpressible in the older shape.

This document is about how to use the surface, not how it is implemented.
Every entry point is covered, with its intended use and the context needed to
call it correctly;  §11 works the other way round, from NFS and FSAL
operations to the sequences that serve them.

Where behaviour depends on the backend it says so.  Implementation notes live
beside the nsfs driver: [DESIGN.md](driver/nsfs/DESIGN.md) for the driver,
[HANDLE_IDENTITY.md](driver/nsfs/HANDLE_IDENTITY.md) for what a filehandle
names and for how long, [RENAME_DESIGN.md](driver/nsfs/RENAME_DESIGN.md) for
rename, [ETAG_STRATEGY.md](driver/nsfs/ETAG_STRATEGY.md) for the ETag option.

**Scope note.**  The subject is the `rgw_file` contract, which is
driver-agnostic;  the document lives here rather than under a driver because
that is what it describes.  Statements qualified "on a filesystem-backed
driver" hold for nsfs and are intended to hold for posix;  rados implements
the namespace but not the positional I/O model.

Interface version is `LIBRGW_FILE_VER_MAJOR.MINOR.EXTRA` = 1.4.1
(`LIBRGW_FILE_VERSION_CODE`).  See §13.

---

## 1. Two generations, and which you want

**The older shape keeps open state on the handle.**  `rgw_open()` takes a
`struct rgw_file_handle*` and returns nothing but a status;  `rgw_read()`,
`rgw_write()` and `rgw_close()` also take the handle.  There is at most one
open per handle, and librgw owns it.  This exists because NFSv3 has no open
and returns no token to the ULP: the protocol gives you a filehandle and a
sequence of reads and writes, and somebody has to infer where the open began
and ended.  librgw does that inference — it keeps the open on the handle and
reclaims it from `rgw_close()`, or from an idle timer if no close arrives.

**The newer shape hands the open back to you.**  `rgw_open2()` returns an
opaque `rgw_open_fd`, and `rgw_readv()`, `rgw_writev()`, `rgw_reopen2()` and
`rgw_close2()` take that rather than the handle.  Several opens on one object
coexist, each with its own access mode, and the object's lifecycle is driven
by them rather than guessed at.

**Write new consumers against the second.**  Four things are only available
there:

- More than one open per object, with distinct modes — the first shape cannot
  represent a reader and a writer on one handle.
- An access-mode change without closing (`rgw_reopen2()`), which NFSv4 OPEN
  upgrade and downgrade need.
- A create disposition and the attributes to create with (§6), which is the
  difference between one call and a sequence that publishes an empty object
  first.
- Correct last-writer semantics, because the count of write opens is real
  rather than inferred.

The first shape remains supported and is not deprecated;  an NFSv3 server has
no way to use anything else.  Passing `RGW_OPEN_FLAG_V3` (alias
`RGW_OPEN_FLAG_STATELESS`) says so explicitly.

---

## 2. The namespace

`struct rgw_fs` carries a `root_fh`, and the shape below it is not uniform:

**At the root, names are buckets.**  `rgw_mkdir()` on `root_fh` creates a
bucket and enforces S3 bucket naming — so it rejects names an ordinary
`mkdir(2)` would accept, and a consumer should expect `-EINVAL` for a
perfectly good POSIX directory name.  `rgw_unlink()` on `root_fh` removes a
bucket, together with the hidden object that may be carrying its Unix
attributes.

**Below the root, directories are pseudo-directories.**  They are a view over
key prefixes, not first-class objects.  `rgw_mkdir()` there is cheap and
`rgw_unlink()` there removes the prefix marker.  A consumer must not assume a
directory has an independent existence: it may appear because an object was
created under it and disappear when that object goes.

**Files are objects.**  A file's name is its key relative to the bucket root,
so `photos/vacation/pic.jpg` is one object in one bucket and not three
nested things.

**Unlink of a file is an S3 DELETE.**  On a versioned bucket that means a
delete marker, not the removal of history — so a `rgw_unlink()` followed by a
`rgw_lookup()` reports absence while the previous versions remain.  A
consumer implementing POSIX unlink semantics over a versioned bucket should
know it is not reclaiming space.

**Symbolic links exist as a type.**  `rgw_symlink()` creates a handle of type
`RGW_FS_TYPE_SYMBOLIC_LINK` and `rgw_readlink()` reads the target back.  They
are an NFS-facing convenience;  an S3 client sees an ordinary object, with no
notion that it is a link.

---

## 3. Mount, teardown, and the upcall

**`rgw_mount()`** takes a uid and an access key pair and produces a
`struct rgw_fs` rooted at the whole namespace — all buckets the credential
can see.

**`rgw_mount2()`** additionally takes a `root` path, and is what an export of
a subtree wants: the resulting `root_fh` is that path rather than the bucket
list.  Prefer it whenever the export is not the whole namespace, because it
removes the need for the consumer to police paths above its export.

**`rgw_umount()`** releases the mount.  Any handle obtained from it is dead
afterwards, and — this is the operationally important part — filehandles do
not survive it (§4).  A consumer that persists filehandles across its own
restart is persisting things that will not resolve.

**`rgw_statfs()`** fills a `struct rgw_statvfs`.  It reports real backing
capacity on a filesystem-backed driver.  Treat the numbers as advisory: an
object store has no single answer for free inodes, and a quota may bind long
before capacity does.

**`rgw_register_invalidate()`** registers a callback taking an `rgw_fh_hk`,
through which librgw tells the consumer to drop a cached handle.  Register it
at mount time, before any lookup.  It fires from two places, and the second
matters more than the first:

- **handle reclaim**, when a cached handle expires;
- **rename**, because the client's filehandle is a hash of the old path and
  after the move names nothing.  Re-keying repairs librgw's bookkeeping, not
  the consumer's.  A consumer that caches handles and does not register this
  will hold handles resolving to the wrong object, or to nothing, after a
  rename.

The upcall is *not* how a filesystem client learns about a rename.  That
happens through the change attributes on both parent directories, which
`rgw_rename()` moves and which a Linux NFS client watches to decide when to
re-resolve.  The upcall is for the consumer's own cache.

It is also the seam through which cross-instance cache invalidation,
delegation recall and lease revocation would arrive.  Those would want a
reason code and an acknowledgement, which the present signature does not
carry.

---

## 4. Handles: what they name, how long, and how to get one

### 4.1 What a handle is

`struct rgw_fh_hk` is a 128-bit pair — `{ bucket, object }` — and is what an
NFS server puts on the wire as a filehandle.  Both halves are hashes of
names: the bucket half of the tenant and bucket, the object half of the full
path from the bucket root.

**It is content-addressed, not durable.**  There is no persistent handle
table and nothing to reconstruct a handle from, so a handle resolves only
while the object it names is still cached.  It does not survive eviction and
does not survive a remount.  `HANDLE_IDENTITY.md` explains why the obvious
improvement — deriving handles from filesystem identity — is parked rather
than merely unimplemented, and `ceph_test_librgw_file_fhcache` pins the
current behaviour so the limitation is not rediscovered.

### 4.2 `rgw_lookup()` — by name, from a parent

The ordinary path resolution.  The flags are worth taking seriously, because
three of them change what the call does rather than merely how it reports.

`RGW_LOOKUP_FLAG_CREATE` mints a handle for a name that need not exist, and
records that the caller intends to create it.  **It creates nothing** — no
object, no filesystem state.  That is deliberate: an empty object becoming
S3-visible because somebody looked up a name would be wrong.  Pair it with
`rgw_open2()` and a createmode (§5).

`RGW_LOOKUP_FLAG_DIR` and `RGW_LOOKUP_FLAG_FILE` are a *type hint*.  A name
in this namespace can be ambiguous — a prefix and an object can share it —
and the hint says which the caller means, avoiding a wrong-type resolution.
Pass one when you know, from a readdir entry or from the operation's context.

`RGW_LOOKUP_FLAG_RCB` says "this lookup is being made from inside a readdir
callback".  It is a performance contract, not a decoration.  It relaxes the
exact-match requirement, and where `rgw_nfs_s3_fast_attrs` is enabled it
permits librgw to return interpolated owner, group and mode instead of
stat-ing each entry — which is the difference between one backend round trip
per directory and one per dirent.  A consumer doing readdir-plus should pass
it;  a consumer that needs true per-object attributes should not.

Each successful lookup returns a reference (§4.4).

### 4.3 `rgw_lookup_handle()` — by handle, from the wire

Given the 128 bits a client presented, this returns the handle they name.  It
is the reverse direction from `rgw_lookup()`: no parent, no name, just the
opaque bytes.

**Why a server cannot avoid it.**  Every NFS operation arrives carrying a
filehandle, not a path — that is the protocol's central design choice, and it
is what makes the namespace stateless from the server's point of view.  A
server therefore needs a way to get from bytes to object with nothing else in
hand.  In ganesha that way is the `fsal_export` operation `create_handle()`,
called whenever a filehandle arrives for which mdcache holds no entry, and
FSAL_RGW implements it with this call (`FSAL/FSAL_RGW/export.c`, in
`create_handle()`).  The FSAL's opaque wire handle *is* a
`struct rgw_fh_hk` — `create_handle()` rejects any other length — so there is
nothing in it to resolve a path from, and a reverse lookup keyed on the
handle is the only possibility.

It gets reached more often than "after a restart" suggests:  mdcache eviction
under memory pressure, an NFSv4 `PUTFH` for a handle the server has not
touched lately, and on NFSv3 essentially any operation, since v3 has no open
state to pin an object with.

**What makes it different from the filesystem equivalent.**  The analogue is
`open_by_handle_at(2)`, and the contrast is the point rather than a detail:
that call resolves a handle the kernel minted earlier, across a remount,
because the filesystem holds durable identity.  This one is **a cache lookup
and nothing more**.  The key is a one-way hash of the path, so if the entry is
gone there is nothing to rebuild from — it fails on eviction, and always
after a remount.

That failure is not a condition to retry;  it is the definition of `ESTALE`,
and FSAL_RGW maps it straight through (`rgw2fsal_error(-ESTALE)`).

**The consequence worth understanding**, because it is a protocol outcome
rather than an implementation wrinkle:  there are two caches in series, and
the outer one is usually the larger.  mdcache can evict an entry, call
`create_handle()`, and get `ESTALE` from librgw for an object that exists and
has not changed — nothing was renamed, nothing was deleted, the handle simply
aged out of the inner cache.  A durable handle scheme would remove that
class of `ESTALE` entirely, which is what `HANDLE_IDENTITY.md` is about and
why it matters more than its "optimisation" framing suggests.

Practical guidance:

- Every operation reached by filehandle rather than by name must be prepared
  for `ESTALE` at any moment, not only after a restart.
- The window is bounded by librgw's cache residency, which the consumer does
  not control.  Do not try to keep handles alive by holding references
  indefinitely;  that trades staleness for unbounded memory.
- A handle obtained this way carries a reference, like any other (§4.4), and
  `create_handle()`'s own pattern is the one to copy:  resolve, `rgw_getattr()`
  to confirm the object is really there, then build the consumer-side handle.

### 4.4 `rgw_fh_rele()` — giving a reference back

Every handle from `rgw_lookup()` or `rgw_lookup_handle()` carries a
reference, released with `rgw_fh_rele()`.  Releasing a reference is not the
same as removing the handle from the cache — the mapping outlives the
reference, which is why a handle can still resolve after you have released
it, and why eviction rather than release is what makes it stale.

`RGW_CLOSE_FLAG_RELE` on `rgw_close()`/`rgw_close2()` does the release as
part of the close, which is usually what a consumer wants.

One asymmetry to know about: a stateless (v3) open arms an idle timer holding
a reference of its own, released when it fires.  So on that path a handle
stays pinned for `rgw_nfs_stateless_finalize_secs` — five minutes by default,
thirty in a versioned bucket — after the close.  That is the price of
inferring a close the protocol never sends.  A v2 consumer closing explicitly
does not pay it.

---

## 5. The canonical create sequence

```c
struct rgw_file_handle* fh = NULL;
rgw_lookup(fs, parent_fh, name, &fh, NULL, 0, RGW_LOOKUP_FLAG_CREATE);

struct stat st = { .st_mode = 0644, .st_uid = uid, .st_gid = gid };
struct stat out;
struct rgw_open_args args = {
    .version    = RGW_OPEN_ARGS_V1,
    .size       = sizeof(args),
    .createmode = RGW_CREATEMODE_GUARDED,
    .attr_mask  = RGW_SETATTR_MODE|RGW_SETATTR_UID|RGW_SETATTR_GID,
    .attrs      = &st,
    .attrs_out  = &out,
};

rgw_open_fd ofd = NULL;
rgw_open2(fs, fh, &ofd, O_RDWR, RGW_OPEN_FLAG_NONE, &args);
rgw_writev(ofd, iov, iov_cnt, offset, &nwritten, RGW_WRITE_FLAG_NONE);
rgw_close2(ofd, RGW_CLOSE_FLAG_NONE);
rgw_fh_rele(fs, fh, RGW_FH_RELE_FLAG_NONE);
```

Three alternatives look reasonable and are wrong.

**Do not use `rgw_create()` if you are going to use `rgw_open2()`.**
`rgw_create()` performs a PUT — the object exists, published and empty,
before anything is written.  A subsequent open then finds an existing object
and forks a copy-on-write view of it rather than creating.  `rgw_open2()`
with a createmode subsumes it.  `rgw_create()` remains for consumers that
genuinely want a create with no open, and for the v1 shape.

**Do not set attributes before the open.**  `rgw_setattr()` on a handle with
no live open goes through the object path, and on a nonexistent object that
path *materialises* it with a zero-length PUT.  The result is the same empty
published object.  Attributes belong in `rgw_open_args`.

**Do not expect anything to be visible before the close.**  §8.

---

## 6. Creating: `createmode` in detail

`rgw_open_args.createmode` governs when it is anything other than
`RGW_CREATEMODE_NONE`.  With `args == NULL`, or with
`createmode == RGW_CREATEMODE_NONE`, behaviour is what it always was:
`RGW_OPEN_FLAG_CREATE` and `O_EXCL` decide.

| mode | meaning |
|---|---|
| `RGW_CREATEMODE_NONE` | do not create;  the legacy flags still apply |
| `RGW_CREATEMODE_UNCHECKED` | create if absent, open if present, apply the attributes either way |
| `RGW_CREATEMODE_GUARDED` | create;  `-EEXIST` if it exists |
| `RGW_CREATEMODE_EXCLUSIVE` | guarded, and the attributes carry a retransmission verifier |
| `RGW_CREATEMODE_EXCLUSIVE41` | as above, with the verifier separate from the attributes |

**`UNCHECKED` does not truncate.**  It means "create if absent, do not fail
if present".  A client wanting the object emptied says so with
`RGW_SETATTR_SIZE` in the attributes, or `O_TRUNC` in `posix_flags`.  The
opposite reading is tempting and would silently destroy an existing object on
an ordinary create-open.

**Exclusive create needs no verifier field**, which is the counterintuitive
part.  In the ganesha model the FSAL folds the 8-byte verifier into the
attribute set itself — two `uint32_t` halves written to `atime.tv_sec` and
`mtime.tv_sec` — and compares those fields back to recognise a
retransmission.  So the verifier arrives as ordinary attributes and the
initial-attribute path carries it.  Two obligations follow:

- Supply `RGW_SETATTR_ATIME|RGW_SETATTR_MTIME` with the verifier halves, and
  do not expect librgw to substitute server time.  They are stored byte-exact
  and must come back byte-exact, because the comparison is `tv_sec` equality.
- Expect the verifier to survive only until the first write, which moves
  mtime as it would on any filesystem.  A retransmitted create arrives before
  the client's writes, so this suffices — but the verifier is not a durable
  property of the object.

**Replay versus conflict is the consumer's decision.**  All three guarded
modes return `-EEXIST` when the object exists.  Telling "my own
retransmission" from "somebody else's object" means reading the stored
attributes back and comparing, which is what the FSAL's own `check_verifier`
operation is for.  librgw stores faithfully and reports `-EEXIST`;  it does
not guess.

**`attrs_out`**, if supplied, is filled from the resulting object, saving a
`rgw_getattr()` — everything in it was just written.

**ACLs are reserved.**  `args.acl`, `acl_len` and `acl_encoding` exist so the
structure need not change again, and a non-empty ACL is refused with
`-ENOTSUP` — refused rather than ignored, because a consumer that believes it
set an ACL must not be told the open succeeded.  The encoding enum
anticipates NFSv4 XDR, POSIX.1e and RGW's native policy.

**A size in the initial attributes** is applied as a data operation, after
any `O_TRUNC`, so an explicit size wins over the flag.

---

## 7. Reading and writing

**`rgw_readv()` / `rgw_writev()`** take the `rgw_open_fd`, an `iovec` array
and an explicit offset.  They are positional — there is no file position to
share between calls — and synchronous, returning when the work is done.  A
short write is reported through `bytes_written`;  a consumer must check it
rather than assume the whole `iovec` went.

**`rgw_read()` / `rgw_write()`** are the v1 equivalents, taking the handle
and a single buffer.  They operate on the open librgw is maintaining for the
handle, creating one if none exists.

**`rgw_readlink()`** reads a symlink target, with the same shape as
`rgw_read()`.

**`rgw_reopen2()`** changes an open's access mode without returning it.  The
directions are not symmetric.  An upgrade to write establishes the object's
mutable view, since a reader may be bound directly to the published object.
A downgrade that gives up the last write intent **publishes**, exactly as
closing it would — giving up write intent and closing are the same event as
far as finalization is concerned.  A consumer implementing NFSv4 OPEN
downgrade should expect the object to become visible at that point.

**`rgw_truncate()`** sets a file's size directly, given a handle.  It is the
same operation `rgw_setattr()` performs for `RGW_SETATTR_SIZE` and exists as
a call of its own for callers that have nothing else to set.  It returns
`-EISDIR` on a directory.  It is a data operation: it affects the mutable
view, and like any write it does not publish.

**`rgw_fsync()`** and **`rgw_commit()`** are durability, not visibility.
`rgw_commit()` is the NFSv3 COMMIT shape, taking an offset and length so a
consumer can commit the range a client asked about.  Neither publishes:  a
committed object is still invisible to S3 until the last write open is
returned.  A consumer that treats COMMIT as "the data is now readable by
others" will be wrong — that is what the close is for.

---

## 8. Closing, and what finalization means

**Finalization is about S3 visibility, not about what this interface's own
callers see.**  That distinction is the single most important thing in this
document, and getting it backwards leads to exactly the wrong expectations.

There is one mutable view of an object — the NFS view — and every caller of
this API shares it.  Readers and writers on that view see the same bytes at
the same time, as POSIX requires.  A write is visible to a subsequent read
immediately;  there is no isolation between them and none is intended.
`reclone()` exists to preserve that: when a writer arrives on a handle
currently bound to the published object, it establishes the mutable view and
`dup2()`s it onto the shared descriptor, so readers already open **follow the
writer** rather than being stranded on stale content.

What the **last write open being returned** does is make that view the
published object, which is what an S3 client sees.  So:

- `rgw_close2()` on the last write open publishes.  An S3 GET before that
  point sees the *previous* object and never a partially written one — that
  atomicity is for the object-store consumer.  An NFS reader has been seeing
  the new bytes all along.
- Concurrent writers share the one view.  Their bytes are indistinguishable,
  and whichever closes last publishes everything all of them wrote.
- `rgw_commit()` and `rgw_fsync()` make the view durable without publishing
  (§7).  Publishing on COMMIT would put a partial object into S3, which is
  precisely why it does not.
- `rgw_close()` is the v1 form, closing whatever open the handle carries.
- `RGW_CLOSE_FLAG_RELE` releases the handle reference along with the open.
- `RGW_CLOSE_FLAG_DETACH` **is accepted and currently does nothing.**  Its
  semantics are settled and documented in the header — it declines to
  finalize on this close's account, in the sense of `pthread_detach()` — but
  it is unimplemented.  Do not depend on it.  It is not a transactional
  abort, and the API offers none: one writer cannot withdraw bytes another
  may have interleaved.

---

## 9. Attributes

**`rgw_getattr()` / `rgw_setattr()`** work in terms of `struct stat` plus an
`RGW_SETATTR_*` mask over mode, uid, gid, mtime, atime, ctime and size.

Two things surprise people.

*Unix attributes are stored attributes, not inode state.*  On nsfs the mode,
owner and group live in an object attribute;  the mode of whatever file backs
the object is whatever the driver created.  A create with mode 0600 produces
an object that *reports* 0600 — do not expect it on the backing file.  Times
are the exception: because they carry the exclusive-create verifier, a create
applies them to the backing inode too.

*A size change is a data operation*, applied before the attribute pass rather
than with it.

Prefer attributes at create (§6) over a subsequent `rgw_setattr()`, for the
reason in §5.

**Extended attributes** have four calls.  `rgw_setxattrs()` and
`rgw_rmxattrs()` set and remove over an `rgw_xattrlist` of name/value pairs;
`rgw_getxattrs()` fetches named ones, delivering through a callback because
values are variable-length;  `rgw_lsxattrs()` enumerates.  Note that RGW's
own internal attributes are namespaced and hidden from this interface, so a
consumer will not see — and must not try to set — the attributes carrying
Unix state, ETags or ACL policy.

There is **no ACL entry point** in this interface at all.

---

## 10. Namespace operations

**`rgw_mkdir()`** — bucket at the root, pseudo-directory below it (§2).

**`rgw_unlink()`** — bucket at the root, S3 DELETE below it, which on a
versioned bucket means a delete marker (§2).

**`rgw_symlink()`** — creates a link-typed object;  `rgw_readlink()` reads
it.

**`rgw_rename()`** takes the two parent handles and the two names, following
`rename(2)`.  On a driver that can do it this is a real rename — the object
and its version history move without the data being copied — and the handle
is re-keyed in place rather than retired, with both parents' change
attributes moved so a Linux NFS client re-resolves.  It refuses a directory,
and it refuses an open file.  `RGW_RENAME_FLAG_SLICE_VERSIONS` moves only the
current version and discards history, which is needed to move a versioned
object into a bucket that cannot hold versions;  the header notes it is
intended to be bound as export policy rather than requested per operation,
since `rename(2)` gives a client no way to ask.  A driver that cannot rename
returns `-ENOTSUP` and the caller falls back.  See `RENAME_DESIGN.md`.

**`rgw_readdir2()` is the one to use.**  It continues an enumeration from a
*name*, which is what a protocol without a cookie verifier actually needs.

**`rgw_readdir()`** continues from a numeric offset, and **`rgw_dirent_offset()`**
projects the offset of a given name — which is the call to reach for when a
client hands back a cookie and the consumer must resume.  Both are built on
offsets that are a 64-bit hash of the dirent name: there is no collision
handling and no verifier.  Do not persist them, and do not treat them as
stable across a mutation of the directory.  A real directory-position cookie
is outstanding work.

---

## 11. Scenarios

Working the other way round, from the operation a consumer is implementing.

### 11.1 NFSv4 OPEN, `UNCHECKED4`, file may exist

`rgw_lookup(FLAG_CREATE)`, then `rgw_open2()` with
`createmode = RGW_CREATEMODE_UNCHECKED`, the client's attributes in
`args.attrs`, and `posix_flags` carrying the access mode.  If the client
asked for size 0, include `RGW_SETATTR_SIZE` — do not rely on the mode to
truncate.  Fill `args.attrs_out` and answer the OPEN's attribute request from
it rather than issuing a `rgw_getattr()`.

### 11.2 NFSv4 OPEN, `GUARDED4`

As above with `RGW_CREATEMODE_GUARDED`.  `-EEXIST` maps to `NFS4ERR_EXIST`
directly;  there is no replay case to consider.

### 11.3 NFSv4 OPEN, `EXCLUSIVE4` or `EXCLUSIVE4_1`

As above with the corresponding createmode, and with the verifier already
folded into atime/mtime in `args.attrs` (§6).  On `-EEXIST`, do not report
the error yet: read the object's attributes and compare the verifier — that
is the FSAL's `check_verifier` — and treat a match as success on the existing
object.  Do not re-apply attributes or truncate on that path;  a replay must
be idempotent.

For `EXCLUSIVE4_1` the client's attributes arrive alongside the verifier, so
they go in the same `args.attrs` and are applied at create, which is the
whole reason the attributes are carried in the open rather than set
afterwards.

### 11.4 NFSv4 OPEN upgrade and downgrade

`rgw_reopen2()` on the existing `rgw_open_fd`.  Note that a downgrade
relinquishing the last write intent publishes (§7), so the object becomes
S3-visible at the downgrade rather than at the eventual CLOSE.

### 11.5 NFSv4 CLOSE

`rgw_close2()`.  If the consumer is also done with the handle, pass
`RGW_CLOSE_FLAG_RELE` rather than a separate `rgw_fh_rele()`.

### 11.6 NFSv3 CREATE, then WRITE, then COMMIT

There is no open to hold, so this is the v1 shape.  `rgw_lookup(FLAG_CREATE)`
records the intent;  `rgw_open()` supplies what that intent implies even if
the caller passes no flags (§14);  `rgw_write()` writes;  `rgw_commit()`
answers COMMIT for durability.  Visibility still waits for the close — or,
if no close arrives, for the idle timer, which is why that timer exists.

A v3 consumer should pass `RGW_OPEN_FLAG_V3` so librgw knows the close is
inferred rather than promised.

### 11.7 PUTFH — resolving a filehandle from the wire

`rgw_lookup_handle()` (§4.3).  Failure is `ESTALE`, not a retryable
condition.  This is the path that makes handle lifetime a protocol concern
rather than an implementation detail.

### 11.8 LOOKUP

`rgw_lookup()` without `FLAG_CREATE`.  Pass a type hint if the operation's
context gives you one, and `RGW_LOOKUP_FLAG_RCB` only from inside a readdir
callback.

### 11.9 READDIR and READDIRPLUS

`rgw_readdir2()`, continuing from the last name returned.  For READDIRPLUS,
pass `RGW_LOOKUP_FLAG_RCB` on the per-entry lookups so librgw may return
interpolated attributes instead of stat-ing every entry (§4.2) — the
difference is one backend round trip per directory rather than per dirent.
If the client hands back a numeric cookie, `rgw_dirent_offset()` projects it,
with the caveats in §10.

### 11.10 A reader while a writer is active

Nothing special is required, and the reader **sees the writer's bytes as they
land** — the same semantics an ordinary filesystem gives.  Both are on the one
mutable view;  a reader already open when the writer arrives is carried onto
it by the `dup2()` in `reclone()` rather than being left on the old content.

Do not look for isolation here and do not offer it to clients.  There is no
snapshot, no copy for readers, and no notion of a torn read to protect against
— two NFS clients on one export see each other's writes exactly as two
processes on a local filesystem do.  A consumer that needs more than that has
to arbitrate, and this interface gives it nothing to arbitrate with (§12).

The atomicity that does exist is on the other side: an S3 client sees the
previous object until the last write open is returned, and then the new one
(§8).

**That holds within one librgw instance.**  The sharing is per-instance, not
global, and the boundary is worth knowing because the behaviour differs on
each side of it:

- There is one `RGWFileHandle` per name per instance, so every open in an
  instance — reader or writer — lands on the same mutable view.  That is what
  makes the paragraph above true, and it is also why `rgw_getattr()` reports
  the writer's live size and mtime to every caller in that instance:  the
  handle has a live view and `stat()` refreshes from it.
- A reader in a *different* instance which opened before the mutable view
  existed is bound to the published object, and nothing carries it across —
  `reclone()`'s `dup2()` reaches only its own instance's descriptor.  It
  continues reading the object it opened, and still does after the writer
  publishes, since its descriptor references the replaced inode.  It picks up
  the new content only by reopening.
- `rgw_getattr()` in that instance is stale for the same reason.  With no open
  of its own, `stat()` returns what the handle cached at lookup rather than
  re-resolving, and NFS GETATTR arrives on a filehandle and so never re-runs
  the lookup.  By the model's own standard that is wrong — the mutable view is
  shared through the filesystem, so a second instance ought to report the
  writer's current size — and it is outstanding work rather than a designed
  boundary.

A single-instance export therefore gets POSIX semantics as described.  A
multi-instance export over one filesystem does not yet, and a consumer should
not promise it.

### 11.11 Two clients writing one file

Both get write opens on one shared mutable view.  Their bytes interleave and
cannot be attributed, and whichever closes last publishes the result.  A
consumer needing exclusion has to provide it: this interface has no locking
and no share reservations (§12).  That is the single largest gap for an
SMB-style consumer, and the reason `FSAL_O_DENY_*` cannot be honoured today.

### 11.12 RENAME

`rgw_rename()`.  Expect `-EPERM` for a directory and a refusal for an open
file — an NFS client is entitled to rename an open file, so a consumer may
need to report this as a limitation rather than pass it through.  Ensure
`rgw_register_invalidate()` is registered, or the consumer's cached handle
for the old name will survive the move (§3).

### 11.13 REMOVE on a versioned bucket

`rgw_unlink()` creates a delete marker.  The name stops resolving, space is
not reclaimed, and a subsequent create of the same name is a new version
rather than a fresh object.  A consumer presenting POSIX semantics should be
explicit about this in its own documentation.

### 11.14 Server restart

Every filehandle issued before the restart is stale, because there is no
persistent handle table (§4.1).  A consumer cannot pre-warm or migrate them.
This is the case that most often surprises, since a filehandle is
contractually supposed to be durable.

---

## 12. What is not here

Stated plainly so nobody looks for it:

- **No locking of any kind** — no advisory locks, no mandatory locks, no
  share reservations, no delegations.  A single-consumer deployment is
  assumed.
- **No hard links.**
- **No `fallocate`, no `SEEK_DATA`/`SEEK_HOLE`, no copy- or clone-range** —
  the last is worth revisiting, since filesystem backends have reflink.
- **No ACL interface**, at create or otherwise.
- **No asynchronous I/O.**  `readv`/`writev` complete before returning.
- **No prefix or directory rename.**
- **`RGW_CLOSE_FLAG_DETACH` is unimplemented** (§8).

---

## 13. How this interface is versioned

`LIBRGW_FILE_VER_MINOR` tracks additions and is currently 4.  It is *not*
bumped on every change: this interface has one active consumer, developed in
step with it, so the existing symbol evolves rather than a parallel one being
added.  The minor is bumped when something outside that work needs to
distinguish.

`struct rgw_open_args` is versioned in its own right — `version` and `size`,
checked on every call, with a mismatch returning `-EINVAL`.  That is not for
third-party compatibility;  it is because the consumer and librgw are built
from separate trees at different times, and a clean rejection beats reading a
field the caller never set.  Zero the structure, set `version` and `size`,
fill in what you need.

---

## 14. Pitfalls, and the reason for each

**A v1 sequence gets its create flags inferred.**  `lookup(FLAG_CREATE)` then
`rgw_open(fh, 0, 0)` — no access mode, no create flag — worked on a backend
where the open was bookkeeping, because the write transaction created the
object at close.  Where the open is a real open, that same sequence is a
read-only open of something that does not exist.  librgw therefore supplies
what the recorded create intent implies.  A v2 consumer passes explicit flags
and never relies on this;  it exists for compatibility.

**`rgw_setattr()` on a handle with no live open can create an object.**  §5.
Use `rgw_open_args.attrs`.

**mtime is reported from the mutable view while an open is live.**  `getattr`
refreshes size and mtime from the current view whenever there is one, so a
caller-supplied mtime is visible only where the driver has also put it on the
backing inode — which a create does, precisely so the exclusive-create
verifier reads back.

**A filehandle is not durable.**  §4.1.  Handle `ESTALE` everywhere a handle
arrives from outside.

**Readdir offsets are name hashes.**  §10.  Do not persist them.

**Nothing publishes until the last write open is returned.**  A consumer that
opens for write, writes, and waits for the object to appear over S3 without
closing will wait forever — or, on the stateless path, for the idle timer.

**`rgw_mkdir()` at the root enforces S3 bucket names.**  A legal POSIX
directory name may be rejected;  that is not a bug to work around.
