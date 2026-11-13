# Handle identity: what an `rgw_fh_hk` names

## Purpose

`rgw_file`'s file handle is a hash of an object's *path*.  That choice is
the reason rename is hard (see [RENAME_DESIGN.md](RENAME_DESIGN.md)), and
it is a question in its own right — larger than rename and separable from
it.  This document records what the handle is today, what a
filesystem-backed driver could use instead, what that costs, and what an
abstraction has to provide for it to be worth doing.

**Status: parked.**  The scheme §2 proposes needs an object's identity to
survive an overwrite, and §7 records the measurement showing it does not.
This is analysis to resume from, not a plan in progress — see §8.

**Scope note.**  The subject is `rgw_file`'s handle contract, which is
driver-agnostic; the document lives here because this is where the working
notes are, not because it is nsfs-specific.  If anything, the likely first
consumer is posix — see §3.

---

## 1. What the handle is today

`rgw_fh_hk` is public API (`include/rados/rgw_file.h:51`), it is
`struct rgw_file_handle`'s identity, the header describes it as a
"content-addressable hash", and `rgw_lookup_handle()` accepts one.  It is
the bits a consumer turns into a wire NFS filehandle.

It is 128 bits, two `uint64_t`:

```
fh_hk.bucket = XXH64(tenant ":" bucket)
fh_hk.object = XXH64(tenant ":" full-path-from-bucket-root)
```

`make_fhk()` composes the object half via `make_key_name()`, which is
`full_object_name() + "/" + name`, and `full_object_name()` includes the
bucket segment with a leading `/`.  A child inherits its parent's bucket
half unchanged.  So for `bucket1/randy/foo`:

| handle | bucket half | object half |
|---|---|---|
| `bucket1` | `XXH64(t:bucket1)` | `XXH64(t:bucket1)` |
| `randy` | inherited | `XXH64(t:/bucket1/randy)` |
| `foo` | inherited | `XXH64(t:/bucket1/randy/foo)` |

Two observations follow.

**Any rename changes the handle.**  Including a rename within one
directory: `mv bucket1/randy/foo bucket1/randy/bar` yields
`XXH64(t:/bucket1/randy/bar)`.  The only rename-invariant part is the
bucket half, which is not the half that identifies the object.

**The allocation is lopsided.**  64 bits of hash for a bucket name is
sized for a namespace that does not exist — buckets per tenant number in
the thousands.  The bucket name is also present in *both* halves, since
the object path includes it.  Meanwhile the object half, which has to
distinguish every object in the store, gets the same 64 bits.

### 1.1 The salt is per-export, and it is load-bearing

The salt is named `tenant` but is `get_fs()->get_user()->user_id.to_str()`
— the user id.  Each mount authenticates as a user, so it is effectively
**per-export**, not per-tenant in the RGW sense.

It is not decoration.  It arose from a deployment running two exports with
different access/secret key pairs over overlapping data, which needed the
two to be isolated from each other.  The salt makes the two exports'
key spaces disjoint, so a handle minted by one means nothing to the other.

It works because `rgw_lookup_handle()` performs no authorization check:

```cpp
RGWFileHandle* rgw_fh = fs->lookup_handle(*fh_hk);
if (! rgw_fh) { return -ENOENT; }
```

It is a cache lookup on that mount's `RGWLibFS` and nothing else, so the
isolation rests entirely on the two key spaces being disjoint.

**Decided: isolation does not belong in the handle key.**  It needs a real
authorization check at resolve time.  Encoding it in the key was a way to
make one deployment work, not a design, and it should be replaced rather
than carried forward — independently of anything below.

That is also how Ganesha's other drivers work:  a handle identifies an
object, and the export decides who may resolve it.  Together with §2.2 it
means this direction converges on established practice on both axes —
derivation and isolation — rather than inventing either.

### 1.2 Content-addressed, but not persistent

An NFS filehandle is supposed to be **durable**:  a client may present one
after a server restart and the server has to resolve it.  Hashing the path
looks like it buys that without a handle table anywhere.

**It does not.**  `RGWLibFS::lookup_handle()` is a cache lookup —
`fh_cache.find_latch()`, and on a miss it logs "handle lookup failed" and
returns nullptr, bar a special case for the root handle.  There is no
reconstruction and there cannot be:  XXH64 is one-way, so the bits name an
object only while an `RGWFileHandle` for it is still in the cache.

So a filehandle does not survive a restart of the NFS server that issued
it, and does not even survive LRU eviction under pressure.  In both cases
`FSAL_RGW::create_handle()` gets nothing back and returns `ESTALE`
(`nfs-ganesha/src/FSAL/FSAL_RGW/export.c:282`), with the object entirely
unchanged.  `ceph_test_librgw_file_fhcache` asserts both, each paired with
a positive control so a broken resolution path cannot be mistaken for the
property under test.

This inverts the usual argument for leaving the scheme alone.  The
durability that content-addressing appears to provide is not there, so a
filesystem-derived handle would not be trading it away — it would be
**adding** durability librgw does not currently have, since
`open_by_handle_at()` resolves after a restart.  Whatever else the
transition in §5 costs, this is not on the list.

---

## 2. The filesystem already has a persistent handle

`name_to_handle_at(2)` and `open_by_handle_at(2)` are exactly the
mechanism knfsd resolves NFS filehandles with.  Measured on the xfs data
root under this build:

```
handle_bytes=12  handle_type=129  bytes=0afe2b0000000000 a06d0e0d
                                        ^ino (64, LE)     ^gen (32)
after rename: IDENTICAL -- handle is rename-invariant
```

- **96 bits**, `XFS_FILEID_INO64_GEN`: a 64-bit inode plus a 32-bit
  generation.
- **Rename-invariant for files _and_ directories** — verified by renaming
  each and re-deriving.  A directory rename does not disturb the handle of
  the directory or, by construction, of anything beneath it.
- The **generation number is the correctness component**.  It is what
  stops a stale handle resolving to a *different* file after inode reuse.
  Dropping it to save bits reintroduces exactly the aliasing bug knfsd's
  gen field exists to prevent.

### 2.1 The bit budget works

96 bits of `(ino, gen)` fits inside the existing 128-bit `rgw_fh_hk` with
32 bits left for a bucket discriminator — no widening of the public
struct.  That is the re-apportionment §1's lopsidedness invites.

An inode-derived handle is identical across exports for the same file, so
the per-export salt cannot survive — which §1.1 has already settled: the
isolation moves to an authorization check and the bits are freed rather
than re-spent.

The caveat used to be **fsid**.  `(ino, gen)` is unique within one
filesystem, so per-account filesystem roots would need an fsid component
too and 128 bits would get tight.  **That is retired.**  The per-account
namespace feature — implemented somewhat as an afterthought in v1 — is
provisionally dropped:  which paths are reachable by which NFS clients and
users is to be controlled by strong authentication, not by mount
permissions.  One data root makes `(ino, gen)` globally unique, so 96 bits
suffice and the remaining 32 are free, which is what a fallback
discriminator needs.  This is the same move as the export-isolation
decision in §1.1:  from structural separation to authorization.

### 2.2 The capability is not a concern

`open_by_handle_at()` requires `CAP_DAC_READ_SEARCH`.  Measured:

```
open_by_handle_at FAILED: Operation not permitted (euid=1000)
```

`setcap cap_dac_read_search=ep` on the binary is the mechanism, and file
capabilities survive exec, so no part of this needs to run as root.

The more useful observation is *which* process resolves handles.  It is
the librgw consumer — an NFS server — not `radosgw`.  **nfs-ganesha on VFS
is entirely built on open-by-handle**, so a Ganesha exporting a local
filesystem already runs with this capability;  the requirement is
satisfied in exactly the deployment that would use this, and asks nothing
new of it.

That also says something about fit.  A handle-based librgw is not an
imposition on Ganesha's model — it *is* Ganesha's model.  The present
path-hash scheme is the thing that diverges from how FSAL_VFS works.

That matters because `CAP_DAC_READ_SEARCH` is broad — it bypasses read
and search permission checks process-wide — and granting it to a
network-facing HTTP daemon would be a genuine expansion of blast radius.
Granting it to an NFS server that must already resolve handles is not a
new posture.  `radosgw` serving S3 never resolves a filehandle and needs
nothing.

Two practical notes rather than design ones:  file capabilities are lost
whenever the binary is replaced, so a build-test loop has to reapply them
after every link;  and in a containerised deployment the capability must
be in the container's bounding set, not merely set on the file.

---

## 3. Why this is worth doing now, and for whom

First, a correction to the framing this document originally used.  An NFS
filehandle is contractually a fixed mapping from opaque bytes to a
particular file.  NFSv4.1 introduced **volatile filehandles**, which appear
to have been intended for exactly our situation — but the industry did not
adopt them, and Linux clients, which is to say nearly all of them, refuse
them.  So we cannot *declare* our handles volatile;  we are obliged to
present them as persistent whether they are or not.

Open-by-handle is therefore not an optimisation.  It is the only honest way
to keep a promise we have no choice but to make, and rename-invariance is a
consequence of that rather than the goal.

Three further propositions, in increasing order of how much they justify.

**It is a precursor.**  NFS rename will be required once the external
metadata schema lands, and that schema introduces a name-to-object-id
indirection (see `RENAME_DESIGN.md` §6).  Doing handle identity properly on
a filesystem first is how the shape gets learned, on the one backend where
the operations are cheap enough to iterate on.  Not merely "implementation
experience" — the handle is the thing rename breaks, so a durable handle is
the precondition for rename meaning anything.

**The nsfs work stands on its own.**  Making librgw-nfs efficient on nsfs,
and later posix, is the current work and is valuable as a demonstration
regardless of what follows it.

**And there is a concrete consumer.**  A standalone deployment on **xfs —
the flash appliance** — which could take advantage of this if it became
interested in NFS export.  *(Assumed to be the posix driver rather than
nsfs;  worth confirming, though see below — it matters less than it first
appears.)*

Note what that consumer is *not*:  it is xfs, not GPFS.  The handle scheme
would arguably generalise to GPFS, which nsfs also backs, but rgw-nfs on
Spectrum Scale is unlikely to be productized — so GPFS-specific work here
is fair to carry and unlikely to be exploited, and should not be used to
justify anything on its own.

And NFS is expected to come to posix in due course regardless.  So posix
is a **first-class consumer of this abstraction, not a hypothetical one**,
whichever driver the appliance turns out to run.  That settles the shape
of the work: the abstraction cannot encode nsfs assumptions and cannot be
built by reaching into nsfs internals, even though nsfs is where it would
be exercised first.  rgw-standalone owns posix, so it has to be something
they can adopt without our having modified their driver — and something
that still fits once NFS arrives there.

---

## 4. The abstraction

`rgw_file` is driver-agnostic, so the only honest form is a
**driver-supplied derivation of the object half**, behind the same kind of
capability flag proposed for directory rename:

- **rados** — path hash, i.e. today's behaviour.  Not a stopgap: rados has
  no inode and no `name_to_handle_at` to borrow, and object identity *is*
  the key.  This is a structural absence, not unfinished work.
- **nsfs, posix** — the filesystem's persistent handle.
- **later, rados** — whatever the external metadata store supplies, through
  the same seam rather than a new one.

For the abstraction to be worth insisting on rather than hacking around,
it should:

1. keep filesystem specifics out of `rgw_file` — the driver returns
   opaque identity bits and answers whether it can resolve them;
2. be explicit about handle lifetime, and prefer durability where the
   driver can supply it (§1.2) -- the current scheme offers none, so this
   is a floor to raise rather than a property to protect;
3. state its uniqueness scope explicitly (per filesystem, per data root,
   per cluster), because §2.1 turns on it;
4. carry a generation or equivalent, so a stale handle cannot alias a
   different object (§2);
5. be adoptable by posix without nsfs-specific plumbing (§3);
6. rely on an authorization check for export isolation, not on the shape
   of the key (§1.1).

---

## 5. What it does not solve

**The transition is a flag day.**  Changing the derivation invalidates
every outstanding filehandle: clients holding one across the upgrade get
`ESTALE` — the same failure this work is meant to remove, relocated to
upgrade time.  `fh_key` carries a `uint32_t version`, but it lives in the
encoded `RGW_ATTR_UNIX_KEY1` attribute, not in the 128-bit wire tuple, so
a server has no in-band way to recognise an old-format handle and resolve
it compatibly.  Any re-derivation therefore wants a deliberate migration
story, and the bit-allocation argument should not make it look cheap.

**It does not solve lookup-for-create by itself.**
`rgw_lookup(RGW_LOOKUP_FLAG_CREATE)` mints a handle for an object that has
no inode, so there is no filesystem handle to derive from — and it cannot be
patched by changing the handle later, since that is precisely the change
this scheme exists to make impossible.

Three options were considered.  *Create at lookup* was rejected:  a failed
follow-up leaves a stray object, and a zero-length object becoming
S3-visible on a mere lookup is wrong.  *Alias* — register the FS-derived
handle as an additional name at creation — was rejected too:  registering a
second name in our own table is easy, but cooperating cleanly and
efficiently with nfs-ganesha's fileid management is not.  *Late identity* —
assign `fh_hk` when the object comes into existence, and let nothing emit it
before — is the choice.

What makes that settle rather than merely defer the problem is that it is
contained to FSAL_RGW, which is ours to evolve.  Verified:  every
`construct_handle()` call site runs only after the object demonstrably
exists — `handle.c:104` (lookup then getattr, bailing if either fails),
`handle.c:340` (after `rgw_mkdir`), `handle.c:1046` (after `rgw_create`),
`export.c:201`, `export.c:291`, `main.c:345` (the export root, which exists
by definition).  `handle.c:104` is the notable one:  even if a creating
lookup returned a handle for a nonexistent object, the following
`rgw_getattr` fails and `construct_handle()` is never reached.  The
invariant is *enforced* by a gate already present, not merely observed.

So no `fsal_api` change and no Ganesha core change:  the pre-create window
never escapes librgw, and late identity is an internal ordering concern.  An
**ephemeral handle state** in the FSAL contract — a handle that must not be
keyed or wired, guaranteed to resolve into a cache key by the time the
create succeeds, discarded if it fails — is not needed, but it remains the
right framing if some future path does hand out a pre-create handle, and it
would be reusable by any backend that assigns identity at creation,
including a metadata-schema rados.

**It does not make rename free.**  It makes the handle survive one, which
removes the ESTALE exposure that currently justifies refusing to rename
open files, and removes the O(cached handles) re-keying.  The listing-cache
invalidation and the S3 semantic question in `RENAME_DESIGN.md` §4 are
untouched.

---

## 6. Relationship to the metadata work

A durable handle-to-object mapping is where the future metadata work is
going, and it is what would let rados rename a prefix atomically.  This is
the same property arriving early on drivers that can get it from the
filesystem for free — not a competing design.  Which is the argument for
building the seam now and letting rados fill it in later, rather than
solving it locally in a way that has to be undone.

---

## 7. Overwrite changes the inode, and that parks this

Measured 2026-09-11 while specifying the fileid contract.  An S3 PUT over an
existing key writes a temp file and links it into place, so the object gets a
**new inode on every overwrite**:

    after first  PUT: ino=2959671
    after second PUT: ino=3015189  size=10   (content genuinely replaced)

The FSIO path should do the same by construction — an existing object's
shadow is a CoW clone, so a new inode, and `publish()` renames it over the
leaf — though only the S3 side was measured.  (An earlier attempt to show
this with `write2` was inconclusive and withdrawn:  the test unlinks its
object, so the second run created it fresh and xfs reused the just-freed
inode.  Same number, different reason.)

This is close to fatal for §2.  An inode-derived filehandle would break not
only on rename but whenever *anyone* overwrites the object — including an S3
client overwriting something an NFS client holds a handle to.  Rename is
rare and explicit;  overwrite is ordinary traffic.  And today's path-hash
handle *does* survive an overwrite, because the path does not change.  So an
inode-derived handle would be a **regression** on this axis, not an
improvement.

### 7.1 The rescue that nearly worked

`XFS_IOC_START_COMMIT` / `XFS_IOC_COMMIT_RANGE` (`xfs_fs.h:1081-1082`)
commits file1's contents into file2 if file2 still has the same
inode/mtime/ctime, moving file2's old contents to file1, `-EBUSY` on
mismatch, restartable after a crash.  That is `publish()`, as a filesystem
primitive.

Measured working, on a filesystem formatted `-i exchange=1`:

    before:  leaf ino=131 "A"      shadow ino=132 "BBBB"
    after:   leaf ino=131 "BBBB"   shadow ino=132 "A"    leaf inode PRESERVED

on a `FICLONERANGE` clone of the leaf — the shadow model's exact shape.  It
would have given identity preserved across overwrite, the displaced content
landing in the shadow (which is exactly what becomes the non-current
version), a kernel CAS replacing `demote_current_version()`'s hand-rolled
retry, and crash restart owned by the filesystem.  Requirements: kernel
6.13+, and the `exchange=1` feature, set at format with `-i exchange=1`
(the inode option group, *not* `-m`) and retrofittable with
`xfs_admin -O exchange=1`.

### 7.2 Why it is foregone: xattrs do not move

Measured on the same filesystem:

    before:  leaf xattr=leaf-side    shadow xattr=shadow-side
    after:   leaf xattr=leaf-side    shadow xattr=shadow-side   (unchanged)

Content swaps;  xattrs stay with their inodes.  And no attr-fork exchange is
exposed to userspace — the ioctl family is 129/130/131 with flags TO_EOF,
DSYNC, DRY_RUN and FILE1_WRITTEN, and nothing else.  The kernel exchanges
attr forks internally for online repair, but not as an operation.

So `publish()` would invert to "commit the content, then stamp the leaf with
new metadata and the shadow with old", leaving a window where the leaf has
new content and stale metadata.  Today's rename-publish installs content and
metadata together atomically, so this is a loss:

    |                     | rename-publish | commit-range          |
    |---------------------|----------------|-----------------------|
    | identity on overwrite | changes      | preserved             |
    | content + metadata  | atomic         | split                 |
    | crash restart       | ours           | the filesystem's      |
    | portability         | any fs         | xfs 6.13+, exchange=1 |

**The decisive item is the version id, not the etag.**  Under
`rgw_non_md5_etag` the etag is `mtime-ino` and therefore derivable, so it
would be automatically correct after a swap with nothing stored to tear.
The version id has no such escape:  it is stored *precisely* to be stable,
and on publish the old version id must travel with the old content into
`.versions/` — which the exchange cannot do, and which cannot be re-derived
on the shadow because the exchange updates both files' mtimes.

XFS "METADIR" is not the answer, checked so nobody re-chases it:  per
`mkfs.xfs(8)` it moves XFS's *own* metadata (realtime bitmaps, quota files)
into a hidden inode tree rather than fixed superblock slots.  It does not
touch the attr fork, where user xattrs live.  The thing that would solve
this is smaller — a `COMMIT_RANGE` flag meaning "exchange the attr forks
along with the data".  That is a general need for anyone doing atomic file
replacement with associated xattrs, not a Ceph-specific one.

### 7.3 Scope

None of this touches rename:  `renameat()` preserves the inode and the
xattrs travel with it, since they are on the inode being moved.  The
commit-range material bears only on **publish** — whether identity survives
an overwrite.

    rename implementation   unaffected;  implemented, see RENAME_DESIGN.md
    CoW / shadow publish    unaffected;  stays rename-based
    fileid redesign         PARKED

---

## 8. Status

**Parked, not deferred.**  §2's scheme needs identity stable across
overwrite, and §7 shows it is not.  Without an attr-fork exchange the
options are write-in-place (which destroys atomic publish), a stored id plus
an index (which is the handle table this scheme exists to avoid), or
accepting breakage on write (a regression on today's behaviour).  So this
waits on one of two things, neither of them ours:

- an attr-fork exchange facility in the exchange-range API (§7.2), or
- the external metadata schema supplying durable object ids (§6).

The rest of the document stands as the analysis to resume from.  Two
questions that were open are now closed:  per-account filesystem roots are
provisionally dropped, which retires the fsid caveat (§2.1), and
lookup-for-create is settled by late identity, which is contained to
FSAL_RGW (§5).  What remains open is the handle-format migration (§5), which
only matters once this is unparked.

What is *not* parked is the negative result being recorded rather than
rediscovered:  `ceph_test_librgw_file_fhcache` pins the current handle's
lifetime — that it resolves while cached, survives a release, and goes stale
on eviction and across a remount — with assertions that say in their failure
messages that this document needs revisiting if they ever pass.
