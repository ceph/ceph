# Positional, copy-on-write access for NFS over RGW

## Purpose

An NFS write cannot be expressed as an S3 PUT.  It is positional, repeated,
and carries no statement that it is finished.  This document records the model
that reconciles the two — what it promises, which conflicts it resolves and
how, and which of the reconciliations are forced by the protocols rather than
chosen.  None of it is derivable from the code;  it is the reasoning the code
implements.

Companion documents: [RGW_FILE_API.md](RGW_FILE_API.md) for how a consumer
uses the resulting interface, and under `driver/nsfs/`,
[DESIGN.md](driver/nsfs/DESIGN.md) for the driver,
[RENAME_DESIGN.md](driver/nsfs/RENAME_DESIGN.md),
[HANDLE_IDENTITY.md](driver/nsfs/HANDLE_IDENTITY.md) and
[ETAG_STRATEGY.md](driver/nsfs/ETAG_STRATEGY.md) for the questions that
turned out to be separable from this one.

---

## 1. The model

NFS and S3 are two **views** of an object.  Both are permanent, stable and
global.  The difference is that the NFS view is mutable under ordinary NFS
semantics, and that mutation is **confined to the NFS namespace until it is
finalized** — published into S3.

Per object there is at most **one active unpublished shadow**, and any number
of clients rendezvous on it.  A shadow is durable and shared.  It is not a
per-client snapshot and not a staging temp file:  two NFS clients writing one
object see each other's bytes exactly as two processes on a local filesystem
do, because they are on the same shadow.

What finalization buys is not isolation between NFS callers — they have none
and want none — but atomicity for the *S3* reader, who sees the previous
object until the shadow replaces it and never a partially written one.

### Terminology, because two of these were got wrong first

The per-object mutable state is a **shadow**, in code and in prose, matching
`.shadow/` and `shadow_fd`.  **View** is reserved for the protocol-level
projection: "the NFS view", "the S3 view".  **Clone** is wrong for the
abstraction — it names the nsfs reflink mechanism, and a GPFS clone is a
different concrete thing.

Publishing is triggered by **the return of the last write open**:
`file::write_opens` reaching zero, whether by a close or by a downgrade.  An
earlier draft called this "emptying the write cohort".  That was a coinage for
something `close2()` already expressed, and `cohort` is a proper noun in this
tree — `cohort::lru` is named for CohortFS — so a reader met one token meaning
two unrelated things.

---

## 2. Decisions

**Readers never create a shadow.**  A read open binds to the published
object.  When a writer arrives, `reclone()` forks the shadow and `dup2`s it
onto the shared descriptor, so readers already open follow it immediately
without reopening.  There are no autonomous histories for whoever arrived
first.  This is why `FSIOObject` carries a three-state binding —
`PUBLISHED`, `SHADOW`, `SHADOW_PUBLISHED` — rather than a `published` bool.

**Finalization is last-writer-wins.**  That is the conflict rule, and it
covers writer against writer.  It does not cover a reader-created shadow,
which is a second and independent reason readers must not fork.

**Unlink is POSIX.**  The shadow's name is dropped immediately
(`FSIOObject::discard()`), open descriptors keep operating on it, it is never
published, and the filesystem reclaims the storage at last close.  A fresh
open after an unlink finds no entry and creates a new shadow rather than
inheriting the deleted object's content.

**COMMIT is fsync, not publish.**  NFS COMMIT is a durability operation.  The
shadow is already durable and already visible to NFS, so publishing on COMMIT
would put a partial object into S3.  `rgw_fsync` matters more than COMMIT
here, since FSAL_RGW calls it for every stable write.

**The finalize timer is an S3-visibility SLA, not a correctness bound.**
Nothing is at risk while an unclosed stateless writer waits: the data is
durable and NFS-visible.  This is why the old
`rgw_nfs_write_completion_interval_s`, which in the previous model *completed*
an in-flight write after ten seconds, is actively harmful here — it would
publish a partial object mid-transfer.  It is replaced by
`rgw_nfs_stateless_finalize_secs`, an idle timer which **publishes without
closing**:  releasing descriptors is the consumer's business, and a resuming
writer simply reclones.

**Stateless opens are librgw-owned.**  `rgw_open_fd` is a bare `Open*` with no
generation, so a token handed out that librgw's reaper could free would
dangle.  The stateless `Open` is therefore kept on the `RGWFileHandle`
(`file::global_open`) and never exposed;  a v3 consumer drives it through the
handle-based calls, which continue to work unchanged.

---

## 3. Versioned buckets

A versioned bucket turns "when do we publish" from a latency question into a
correctness one, because every publish mints a version.

**NFSv3 has no completion signal at all.**  It has no OPEN and no CLOSE, and
none of LOOKUP, ACCESS, GETATTR, SETATTR, READ, WRITE, COMMIT, CREATE, REMOVE
or RENAME means "I am finished".

**COMMIT is not that signal, and is an anti-signal.**  The application never
sends COMMIT;  the Linux client emits it from its own page-cache management —
writeback aging, memory pressure, close-to-open, an application `fsync`,
`sync`, `msync`, `O_SYNC` writes, and before lock operations.  It reports the
state of the client's cache, not the application's intent.  Publishing on
COMMIT would mint a version every writeback interval during a long write, and
once per `fsync` for a database or log writer — strictly worse than
quiescence, which at least correlates with the client having stopped.

**So quiescence is the only place application intent enters the protocol, and
it enters as absence rather than as a message.**  Version granularity is
therefore quiescence granularity under v3.  That is intrinsic, not an
implementation defect, and it is the same shape as the rule elsewhere in this
model:  v4 says when it is done, v3 does not, so the close is inferred — and
on a versioned bucket the cost of inferring is a surplus version rather than a
held descriptor.

This is v3-only.  The idle timer arms only for stateless opens, and a v4 open
is never the global open, so there is exactly one publish per open.

**The exposure is narrower than it first appears**, because
`arm_stateless_timer()` re-arms on every `writev`:

- a continuous writer at any rate never fires the timer, so one version.  A
  slow bulk copy over a slow link is not affected.
- a bursty writer with gaps longer than the interval gets one version per
  burst.

**Unlink in a versioned bucket writes a delete marker**, as S3 requires, so the
name stops resolving while the history remains.  A consumer presenting POSIX
unlink semantics over such a bucket should know it is not reclaiming space.

**Suspended versioning** keeps the versions a bucket already holds and stops
minting new ones.

---

## 4. The listing cache

Two paths can invalidate the LMDB bucket-listing cache, and only one of them
is allowed to be load-bearing.

Explicit invalidation — and, where the change is describable, a rekey rather
than a drop — is the correctness path for mutations the gateway performs
itself.  inotify is **advisory**:  it exists to notice an external actor
modifying the directory underneath us, and must never be trusted for our own
consistency.

The reasons are concrete.  inotify watches the top-level bucket directory and
not `.versions/`, so version demotes and deletes are invisible to it.  Its
events can be coalesced, reordered or dropped under pressure, and the window
between an overflow and its delivery is uncontrolled.  An ADD event re-reads
the object from disk, so a late ADD can re-inject an entry that an explicit
invalidation and refill had already removed.  And nothing serialises
incremental event delivery against an in-flight fill.

---

## 5. Scope

**nsfs is the prototype vehicle**, being the cleanest backend and having
reflink.  Nobody deploys NFS-over-RGW on GPFS — ganesha runs there directly
through FSAL_GPFS, and RGW's role is the S3 view — so GPFS multi-head
coherence is not a question this backend has to answer.

**Multi-instance is in scope.**  Several ganesha/librgw instances are expected
over a single local filesystem, on an appliance.  There RGW is the source of
truth, NFS-over-RGW is the only path, and the instances must agree.  The
shared medium is the filesystem itself:  coherent inodes and xattrs, and
OFD/POSIX locks across processes on one host.

So the "one active unpublished shadow" invariant eventually needs a real
arbiter, and the view state which today lives in per-process memory —
`binding`, `published`, `write_opens` — has to move into the shadow.  That is
not built.  What is built is careful not to foreclose it;  §6 says where the
edges are.

**Scale-out is RADOS**, through a distributed copy-on-write extent primitive
and handle tracking built from scratch, gated to rgw_file.  The details are
deliberately unsettled.  One design consequence must survive the translation,
though:  the shadow should stay a **separately addressable thing** rather than
a state bit on the head object.  On nsfs, confinement is enforced purely by
naming — `.shadow/` is not in the S3 listing — so no read path needs to know
about a flag.  If RADOS were to make it head-object state, the two backends
would stop being isomorphic.

---

## 6. What is built, and what is not

The work was sequenced by what blocks a usable FSAL rather than by what is
interesting.

**Built.**  The shadow directory and the FSIO object model;  binding,
attach-or-create and stateless opens;  exclusive fork with join-the-winner;
positional read and write;  truncate;  `rgw_reopen2` with publish on the
downgrade that returns the last write open;  attributes and xattrs resolved
against the view a caller is on;  `cluster_stat` and `statfs`;  lookup
resolved against the positional view rather than an S3 GET, so an unpublished
object is findable by name;  versioning over the NFS write path;  a real
rename;  and a create disposition with initial attributes on `open2`.

**Deliberately declared rather than built.**  No hard links.  No `fallocate`,
`SEEK_DATA`/`SEEK_HOLE` or copy/clone-range — the last worth revisiting, since
the backends have reflink.  I/O is synchronous.  Readdir cookies are a hash of
the name, with no collision handling and no verifier;  a real
directory-position cookie is outstanding.  `RGW_CLOSE_FLAG_DETACH` has settled
semantics and no implementation, so that the meaning is fixed before a caller
depends on a guess.

**Designed and deferred until a consumer forces them.**  Advisory and
mandatory locking, with two consumers wanting different semantics, arbitration
across instances, and a revocable lease.  Delegations.  An extended upcall
surface — handle invalidation is already upcalled through
`rgw_register_invalidate()`, but delegation recall, cross-instance
invalidation and lease revocation all want a reason code and an
acknowledgement the present signature does not carry.

**Known edges, with the shape of the fix but not the fix.**  Cross-instance
coherence is the large one:  the mutable view is shared through the filesystem
by name, but a reader in another instance which opened before the shadow
existed stays bound to the published object, and `rgw_getattr()` there returns
what the handle cached at lookup, because GETATTR arrives on a filehandle and
never re-runs the lookup.  Within one instance both are correct.  The
rendezvous path likewise has no cross-instance arbitration:  building a shadow
away from its name closed the window for the *creator*, but two instances
adopting one shadow still coordinate through nothing.
