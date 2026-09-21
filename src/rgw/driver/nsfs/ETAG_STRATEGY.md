# Selectable ETag strategy: `rgw_non_md5_etag`

## Purpose

`rgw_non_md5_etag` makes RGW skip the content MD5 that normally becomes
an object's ETag, substituting a unique dashed token.  This document
records what the option covers after the completion work, what it
demonstrably costs in S3 semantics, and the design questions the current
shape leaves unanswered.

It exists because those questions are the ones an upstream reviewer will
ask, and because the answer to "should this be on?" is probably not a
per-cluster performance judgement at all.  See §5.

Sibling documents: [DESIGN.md](DESIGN.md) for the driver,
[NOOBAA_VARIANCE.md](NOOBAA_VARIANCE.md) for where our on-disk format
diverges from NooBaa's.

---

## 1. What the option does

`rgw_non_md5_etag` is a **daemon-wide boolean**, `level: advanced`, with
`flags: [runtime]` — it can be flipped on a running gateway without a
restart.  That last property matters more than it looks; see §3.2.

When set, the body is never hashed and the ETag becomes one of two
dashed tokens:

| backend | form | source |
|---|---|---|
| nsfs | `mtime-<base36>-ino-<base36>` | the object file's own `statx` |
| everything else | `mtime-<ns>-req-<req-id>` | wall clock plus the request id |

The dash is load-bearing rather than cosmetic.  An AWS SDK reads a dash
in an ETag as "this is a multipart ETag, do not validate the body against
it", so the dash is what stops clients failing an integrity check that
the gateway is no longer performing.

nsfs uses the same string it gives version ids, so on a versioned nsfs
bucket **ETag equals versionId**.  That is asserted by
`OPEN2.VER_PUBLISHED_ETAG_TRACKS_VERSION_ID` in
`src/test/librgw_file_write2.cc`, and it holds for a non-obvious reason:
`publish()` computes the ETag *before* the `renameat()` and the version
id *after* it.  The two agree only because setting an xattr updates
ctime rather than mtime, and a rename moves the name rather than the
file.  If either assumption stops holding, the strings drift apart
silently.

### 1.1 Coverage

The option originally reached only `RGWPutObj::execute()`.  Every writer
that digests a body now honours it:

| path | scope |
|---|---|
| `RGWPutObj::execute()` | S3 PUT, UploadPart — all backends |
| `RGWPostObj::execute()` | browser/form POST upload — all backends |
| `RGWBulkUploadOp::handle_file()` | Swift bulk upload — all backends |
| `NSFSFSIOObject::publish()` | every NFS/FSIO write, at last-writer close |
| `NSFSObject::generate_etag()` | first read of a file with no ETag xattr |

The nsfs pair are the ones a request never passes through, and
`publish()` is the largest single win available: it `pread`s the whole
shadow file back through a 64 KiB buffer purely to digest it, a second
full pass over bytes the client has just written.

Deliberately unchanged: `RGWGetObj`'s DLO/SLO manifest hashing and the
Swift SLO manifest sum digest segment *ETag strings*, not object bodies.
They cost nothing and work unchanged on dashed values.

**Not covered: `POSIXObject::generate_etag()`.**  posix has no
`publish()`, so a sideloaded first read is its only gap.  Closing it is
not a drive-by: outside a request there is no req id, so the only unique
value in scope is the inode — which §1's table says posix does not use.
Doing it properly means lifting the `mtime-ino` formatter to
`rgw_common` beside `rgw_make_opaque_etag()`, amending that documented
contract, and a posix validation run.

### 1.2 Multipart

The composite ETag stays `<32hex>-<parts>` in both modes.  MPU complete
folds each part ETag to 16 bytes via `rgw_part_etag_to_digest()` and
hashes the digests as it always did.  The fold decodes a leading 32 hex
digits when present, and otherwise MD5s the ETag string itself.  A token
can never be mistaken for a classic ETag, because both forms begin
`mtime-` and neither `m` nor `t` is a hex digit.

The consequence is that the composite is a hash of per-write tokens, so
it is **not reproducible across runs** — it cannot be pinned to a
literal in a test or an external record.

---

## 2. What it costs, measured

These are not predictions.  Each is a test in `s3-tests-rs` that failed
against a gateway with the option on and had to be taught the difference:

| behaviour lost | evidence |
|---|---|
| ETag equals Content-MD5 | `object_ops::test_object_write_check_etag` |
| ETag survives an identical rewrite | `conditional::test_put_object_if_match` |
| …for multipart | `conditional::test_multipart_put_object_if_match` |
| composite ETag is reproducible | `multipart::test_multipart_reupload_checksum_and_etag` |
| a part ETag stays valid across re-upload | `encryption::test_multipart_sse_c_get_part` |

The second and third are the sharp ones.  An ETag that is a digest is
*stable*: rewriting identical bytes reproduces it, so a conditional
request holding an older ETag still succeeds.  A per-write token has no
such property, and `If-Match` against a superseded value fails with 412.
Any client that caches an ETag and later conditions on it is affected,
and nothing in the protocol tells it the rules changed.

The last row surfaced a genuine bug in the suite rather than a semantic
loss — a helper re-uploaded a part and then sent Complete with the
*first* upload's ETag.  MD5 had masked it, because re-sending identical
bytes reproduces the digest.  Worth recording as a class: **a digest
ETag hides staleness bugs that a unique ETag exposes.**

The full nsfs suite scores identically in both modes once those five are
conditioned — 648/649 baseline, 76/76 `librgw_file_write2`, the single
failure being the unrelated `test_multipart_complete_over_1000_parts`.

---

## 3. Open design questions

### 3.1 Granularity

The option is daemon-wide.  It is not per-bucket, per-user, per-tenant or
per-storage-class.  A gateway cannot serve a tenant who needs
S3-interoperable ETags alongside one who wants the throughput.  For a
multi-tenant Ceph deployment that is close to disqualifying.

### 3.2 Mixed-mode buckets

Because the flag is runtime-flippable, a single bucket can hold objects
of both styles with nothing to reconcile them:

- `If-Match` behaves like a digest for pre-flip objects and like a token
  for post-flip ones, in the same bucket, with no way for a client to
  tell which it is talking to.
- `LIST` returns two ETag shapes.
- An MPU whose parts straddle a flip produces a composite folding both
  classic and token part ETags.  This is well-defined, but it means the
  composite's meaning depends on *when* each part was uploaded.

Nothing detects or reports this.  The gateway has no notion of an ETag
epoch.

### 3.3 Cost, unmeasured here

We have not measured the saving ourselves;  Mark Kogan observed it and
owns testing for non-regression.  What is worth recording is that the
figure is not one number.  An inline PUT hash overlaps with I/O the
gateway is doing anyway, whereas `publish()` is a *separate* full read of
data that was just written -- so the two paths have materially different
profiles, and `publish()` may well show a larger saving than the S3 path
rather than the same one.  A single percentage quoted for "the option"
should be read as applying to whichever path was measured.

### 3.4 Integrity posture

`Content-MD5` is accepted and not verified when the option is on.  The
gateway is silently declining to check something the client asked it to
check.  Whether that should instead be an error, and whether the
suppression should be visible in the response, is unresolved.

---

## 4. NooBaa lineage

This is not a novel idea; it is the NooBaa NSFS default arriving in RGW.
NooBaa ships `config.NSFS_CALCULATE_MD5 = false`, and its `_get_etag()`
prefers a stored xattr and otherwise returns a version-id-shaped token.
For NooBaa, the synthetic ETag *is* the ETag — not a degraded mode but
the defined behaviour, which is where its throughput advantage over a
hashing gateway comes from.

That is the important observation: in the NooBaa lineage the token is
authoritative, and clients are built against it.  In Ceph object storage
the MD5 ETag is the contract, and the token breaks it.  The same
mechanism is a defined behaviour in one product and a violation in the
other.

---

## 5. Where this probably lands

For most users of Ceph object storage it is hard to argue this is more
than a performance hack for local benchmarking — §2 lists real S3
semantics it removes, §3.1 says it cannot be scoped to the tenants
willing to accept that, and §3.2 says it can be turned on mid-flight
leaving a bucket in two states at once.  Presented as a general-purpose
tuneable it should expect a hostile review, and the reviewer would be
right.

The splitting function is more likely **deployment identity than
performance preference**.  Spectrum Scale / GPFS ship this ETag form as
their *defined* behaviour, inherited from the NooBaa lineage in §4;
their clients are built against it and the token is the contract.  A
Ceph object deployment is the opposite case: MD5 is the contract, and
today there is no reason to change that.

If that framing is right, the implication is that the selector should
not be a global performance toggle at all.  It should be scoped to the
thing that actually differs — the backend or the deployment — so that
"Spectrum Scale behaves as Spectrum Scale" is expressible without also
offering "Ceph object silently stops computing MD5" as a tuning knob.
That is a different option shape than a cluster-wide bool, and it is the
question to settle before this goes upstream.

---

## 6. Status

Implemented and validated on nsfs in both modes -- 648/649 baseline and
77/77 `librgw_file_write2` with the option on and off alike.

Open: posix `generate_etag()` (§1.1), which waits on a posix review.

The option's *shape* — granularity, mixed-mode buckets, and whether the
selector should be deployment identity rather than a performance toggle
(§3.1, §3.2, §5), plus the integrity posture (§3.4) — is deferred to a later
level-up.  It should be settled before the option is presented as generally
useful, but it is a design decision rather than outstanding work on this
branch.  Performance measurement is Mark's (§3.3).
