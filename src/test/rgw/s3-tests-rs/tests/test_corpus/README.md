# The conversion corpus

A modality of this suite, not a separate tool.  It writes a set of S3
artifacts, records what S3 says about them, records what the filesystem
says about them, and asserts the properties that hold of any S3
implementation.

Comparing two gateways is a later pass.  What this produces is the
substrate for that comparison.

The design notes are `docs/NSFS_CORPUS.md` in the nsfs design set;
this file is the interface.  The end-to-end validation workflow this
feeds — stand NooBaa up, generate and ingest, hand off to RGW, verify
— is `../../../adoption/WORKFLOW.md`.

---

## Running it

```sh
S3TEST_CONF=~/dev/rgw/s3tests.conf \
CORPUS_OUT=/tmp/corpus \
CORPUS_DATA_ROOT=<build>/dev/rgw/nsfs/root \
  cargo test --test test_corpus -- --test-threads=1
```

| variable | meaning |
|---|---|
| `CORPUS_TIER` | `coverage` (default), `interaction`, `scale`.  Axes return early for a tier they do not implement |
| `CORPUS_OUT` | where captures are written.  Default `target/corpus`, which is build output |
| `CORPUS_DATA_ROOT` | the gateway's data root.  **Set only when the run is local to it.**  Without it the disk half is not taken and the S3 half still works |

`--test-threads=1` is not required but makes the per-axis progress
readable.  The axes use their own buckets and do not collide.

**Run it against a clean tree.**  Names are derived from parameters so
that manifests diff, which means a second run writes the same keys to
the same buckets.  Use `rgw-vstart.sh --clean`.  One axis — bucket
configuration — is explicitly idempotent because it clears each
configuration before applying it, but that is a property of that axis
and not of the corpus.

---

## What writes the artifacts

Every artifact here is written through S3, by whichever gateway the
run is pointed at.  That is the only generator this has.

A second one is designed and **not built**:  artifacts written
straight to the filesystem, which no gateway produced, plus bulk for
the scale tier.  Its purpose is provenance an S3 request cannot
supply — a directory object in their layout, a name a gateway would
reject.  `docs/NSFS_CORPUS.md` §1 is the design.

---

## The two captures

### `Manifest` — what S3 says

```rust
let mut m = Manifest::new();
m.object("key", "size", "128");
m.bucket(&b, "versioning", "Enabled");
std::fs::write(path, m.render())?;
```

One fact per line, `scope \t name \t field \t value`, sorted bytewise
and deduplicated, every component percent-encoded so a value can never
introduce a tab or a newline.  The comparison is `diff`, and a
difference names the fact rather than the object.

`fact()` is the general form; `object()` and `bucket()` are the two
scopes in use.

### `Inventory` — what S3 cannot say

```rust
let mut inv = Inventory::new();
inv.walk(&bucket_dir)?;
std::fs::write(path, inv.render())?;
let problems = inv.check_object_modes();
```

Mode, type, uid, gid, size and **extended attribute names** for every
entry under a root, same line discipline.  No timestamps and no inode
numbers: they differ on every run and carry nothing the comparison
wants.

Three defects this half found that the manifest structurally could
not: a zero-length object at mode 0700, every completed multipart
object at 0700, and conversion leaving NooBaa-spelled attributes on
version entries.

Most axes want the one-call form, which writes the file and returns
what the disk-level invariants object to:

```rust
let problems = inventory::capture(&b, &dir, &format!("{tier}-keys"));
```

`capture()` returns an empty list when `CORPUS_DATA_ROOT` is unset — a
remote run must not fail an invariant it cannot measure.

**`walk()` records what is *under* a root, not the root itself.**  A
bucket's configuration lives on the bucket directory, so that needs
`root_xattr_names()` or `root_xattr_digest()`.  Prefer the digest form
when asking whether something was persisted: a configuration folded
into an existing blob adds no attribute *name*, and a names-only
comparison reads that as "stored nowhere".

## The parameter space

`names.rs` and `shapes.rs` hold the axes.  Each is an enum with a
`label()` or `key()`, and most carry a `rationale()` saying why that
value is in the set — a named value should be able to explain itself.

| | |
|---|---|
| `KeyClass` | 15 key shapes: reserved names, the length ceiling, a key that is also a prefix |
| `SizeClass`, `MetaClass`, `TagClass` | object body, user metadata, tags |
| `PartCount`, `PartPattern`, `UploadState` | multipart.  **512 and 513** are named, not generated |
| `CopySource`, `CopyDest`, `Directive` | copy |
| `LockClass` | object lock, and `RETAIN_UNTIL_SECS` |

The `*_COVERAGE` constants are the coverage tier: every value of every
axis at least once, **not** the cross product.  The product is the
interaction tier's job.

`bucket(tier, label)` composes the bucket name, so every bucket this
produces is identifiable as corpus output.

---

## Adding an axis

1. Add the shape enum to `shapes.rs`, with a `label()` and a
   `rationale()` where the value needs defending, plus a
   `*_COVERAGE` constant.
2. Add `<axis>.rs` with one `#[tokio::test] async fn corpus_coverage_<axis>()`.
3. Declare it in `mod.rs`.
4. Return early unless `CORPUS_TIER` is a tier you implement.
5. Record both captures; assert reference-free properties only.

### Write the axis to exercise what you are suspicious of

The point is not to produce artifacts; it is to reach the code that
handles them.  Several defects lived behind a *warm listing cache* — a
listing that misses refills from the store and derives every field
correctly, so it reads the same whatever the write path put in the
cache.  The multipart and copy axes therefore list their buckets
*before* their writes complete, deliberately.

An axis that passes has only tested the branches it reached.  The copy
axis passed for a while without a versioned destination, and the branch
it was written to settle was never executed.

### Assert only what holds of any S3 implementation

There is no golden manifest and there will not be one: it would record
what this implementation happened to do, enshrining the first run's
defects as the expected result.  Assert instead that a key the gateway
accepted is listed, that a version id it reported is findable, that a
size matches the bytes written, that nothing appears twice.

The gateway's error code is recorded like any other **fact**, and the
axis carries on.  Only a broken invariant fails the axis — a listing
that disagrees with what was written, a version id that cannot be
found.
Whether a backend supports a feature is stated by the suite's
`fails_on_<backend>` gates, not by this corpus.

### Two cautions, both learned the hard way

**Check the request before believing the answer.**  This corpus has
produced two false positives, and both were the instrument's fault.
The logging step created a target bucket with no policy, got the
correct 403 that S3 requires, and reported the feature as absent.  A
names-only attribute diff reported request-payment as unstored when it
folds into an existing blob.  When an axis finds a defect on its first
run, re-read what it asked for.

**Prefer fixed values to varying ones.**  A moving timestamp or a
generated id makes every manifest differ from the last for a reason
that is not a change in behaviour.  `RETAIN_UNTIL_SECS` is a fixed
date for this reason.  Where a value cannot be fixed — an
inode-derived version id, a retention date computed from an object's
own mtime — record it as a fact and do not compare it.

---

## What this is not

Two things are **deliberately** absent.  Both have a revisit
condition, so neither is an oversight to correct by adding one.

**Concurrency.**  No axis generates concurrent traffic.  A corpus is a
static artifact set, and names derived from parameters are what make
two runs against two gateways diff line for line;  mixing concurrent
traffic in would cost exactly that.  Conversion *while serving* does
need it, and it belongs to C2b and C7 rather than here.  Revisit when
C7 is specified.  `docs/NSFS_CORPUS.md` §8.

**A golden manifest.**  There is no expected copy in the repository to
diff against, and there should not be:  it would record what this
implementation happened to do, enshrining whatever defects were in the
first run's output as the expected result.  What an axis asserts
instead are properties true of any S3 implementation, and comparisons
are between two captures — never against a file in the tree.
