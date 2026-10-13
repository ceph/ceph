# The corpus validation workflow, end to end

**Who this is for.**  A developer running the adoption validation, or
taking it over part-built.  Stage letters like C2b are
`docs/NSFS_ADOPTION_WORK_PLAN.md`'s.

This is the sequence and nothing else;  each step names the document
that owns its detail rather than restating it.

Four steps.  **The first three run today.  The fourth is not built** —
step 4 says what is missing.

```
  1. stand NooBaa up        standup.rs up && standup.rs serve && standup.rs iam-user
  2. generate and ingest    standup.rs capture
  3. hand off to RGW        standup.rs stop, then adopt and capture twice (§3)
  4. verify                 not built -- C2b
```

Everything is driven from `src/test/rgw/adoption/standup.rs`.
`standup.rs list` prints the steps and the settings;  `ADOPTION_WORK`
is required and has no default, because it holds the worktree, their
config store, the data root and the captures.

---

## 1.  Stand NooBaa up

```sh
cd src/test/rgw/adoption
./standup.rs up          # pin, worktree, patch, build, config
./standup.rs serve       # their endpoint, HTTP, on 6001;  IAM, HTTPS, on 7005
./standup.rs iam-user    # an IAM user in the account, through their IAM API
./standup.rs store-tests # the nsfs unit tests that read a real NooBaa store, against this one
```

`up` is the slow one — their dependency tree and their native addon,
tens of minutes on a first run.  Everything after it is seconds.

**What this produces.**  A pinned NooBaa NSFS gateway serving a data
root, with an account, an anonymous account and a bucket their own CLI
created, and an IAM user in that account their IAM API created — records *they* wrote,
rather than records composed from our reading of their schema.  Their
CLI cannot address an IAM user, which is why `iam-user` goes through
the API.  NooBaa shows the IAM user's secret once;  `iam-user` keeps it
in `$ADOPTION_WORK/iam-user-credentials`.

`store-tests` runs the four nsfs unit tests that read a real config
store (`*AStoreOnDisk*`), with `NSFS_NOOBAA_STORE` pointed at this one.
Nothing else sets that variable, so without this step they skip.  It
needs `ceph_build`, and it fails if any of the four skip rather than
run.  Like any run of the nsfs unit binary, it must not run beside
another.

`README.md` beside this file owns the detail — why source and not a
container, why a `--detach`ed worktree, why the dependency tree is
pinned on our side, and the one patch their native addon needs.

## 2.  Generate and ingest

```sh
./standup.rs capture
```

Runs the conversion corpus against their endpoint and keeps what it
produced in `$ADOPTION_WORK/capture/noobaa/` — one manifest and one
inventory per axis, plus `noobaa.log` for the whole run.

**What this produces.**  Artifacts in *their* on-disk format, written
by their gateway, and a record of what S3 said about each one.  The
artifacts are the ingest;  the captures are the baseline the fourth
step compares against.

Three things about reading the output:

* **A failing axis is the finding, not a broken step.**  The corpus
  asserts what the nsfs driver should do, and theirs is a different
  implementation.  Every axis writes its artifacts before it asserts,
  so a failed axis still leaves a complete pair.
* **No artifacts at all is a real failure**, and the step fails on it:
  it means no axis reached a write, so read `noobaa.log` from the top.
* `capture` sets `CORPUS_OBJECT_MODE=any` and
  `--features fails_on_noobaa`.  The first because the corpus
  otherwise requires every object to be `0600`, which is a statement
  about our driver;  the second because three things their
  implementation does not do would otherwise be indistinguishable from
  real disagreements.

The corpus itself is owned by
`../s3-tests-rs/tests/test_corpus/README.md` (the interface) and
`docs/NSFS_CORPUS.md` in the nsfs design set (the reasoning, including
the two capture formats in §7).

## 3.  Hand off to RGW

```sh
./standup.rs stop                      # leaves the tree
./standup.rs import                    # RGW over the tree;  identities and bucket records imported
./standup.rs capture rgw-unconverted   # the corpus in observe mode
./standup.rs adopt                     # the same gateway;  every bucket adopted
./standup.rs capture rgw-converted     # the corpus in observe mode again
```

**Sequential, never concurrent.**  NooBaa writes, stops, then RGW opens
the same directory.  That is the production story — drain, then upgrade
— and `adopt` refuses while their endpoint is still running rather than
warning about it.

`adopt` runs `rgw-vstart.sh` with three things that matter:

| | |
|---|---|
| `--data-root` | the tree NooBaa wrote.  Mandatory:  without it RGW serves its own build directory and the run measures nothing |
| `--noobaa-config-root` | their config store, so RGW can read what they wrote |
| `--adoption-mode command` | the driver's default, said explicitly:  nothing is adopted until asked |

Then `adopt` asks, through Admin Ops as an operator would
(`NSFS_ADOPTION.md` §7):  the `adoption` driver hint with
`identities=true` imports the store's accounts, users and keys;  then
each bucket NooBaa holds a record of is adopted into the shared
profile, by the hint with `bucket=<name>`, except
`corpus-coverage-copy`, which goes through `PUT /admin/nsfs/profile`.
Adopting a bucket imports its NooBaa record and rewrites its tree.
`ACCOUNT_IMPORT.md` §6.1 owns what the import does, in order.

**What this produces.**  An RGW gateway serving the tree NooBaa wrote,
with that deployment's accounts, users, identity rows and bucket state
imported.

`adopt` also grants the imported IAM user the `nsfs=read` cap, through
Admin Ops as vstart's `[iam]` user unless `rgw_admin_key` says
otherwise:  the corpus needs it to ask what the IAM user's requests run
as.

`import` and `adopt` produce the two RGW runs C2b compares
(`docs/NSFS_ADOPTION_WORK_PLAN.md`).  `import` imports the identities
and then each bucket's NooBaa record, with the `adoption` hint's
`import=<bucket>`:  RGW's bucket state is written onto the bucket
directory and the objects are left as NooBaa wrote them.  `adopt`
adopts every bucket, which imports its record again and converts its
tree;  `corpus-coverage-copy` goes through the profile endpoint.  When
the `adoption` hint reports that a gateway already serves this tree,
`adopt` uses it rather than restarting:  a restart empties the userdb
and would re-import the identities under new ids, so the two captures
would see different owners.

Each capture runs the corpus in observe mode.  Besides the axes,
it checks the imported identities (`test_corpus/identity.rs`):  the
account's user is its root, and the IAM user has no identity row of
its own and runs as its account's (`ACCOUNT_IMPORT.md` §2.8).  Its
capture conf carries the IAM user as `[s3 alt]` and the Admin Ops key
as `[iam]` for that.

## 4.  Verify — **not built**

This is C2b of `docs/NSFS_ADOPTION_WORK_PLAN.md`, and it is the one
step with no procedure yet.  Two things are missing, and they are
separate:

**An observe-only corpus mode.**  Every axis today *writes* its
artifacts and then records what S3 says.  The fourth step needs the
second half without the first:  re-read the artifacts NooBaa created,
now through RGW, and capture what RGW says about them.  Running the
corpus again as it stands would rewrite them in our format and destroy
the thing under test.

**The comparison.**  C2b is a *three-way* diff, because RGW makes two
passes:

```
capture/noobaa   what their gateway said      (step 2, exists)
capture/rgw      what RGW says, adopted       (needs observe-only)
capture/rgw-conv what RGW says, converted     (needs observe-only + C7)
```

The formats are built for this:  both captures are line-oriented, one
fact per line, sorted bytewise, so the comparison is `diff` and a
difference names the *fact* rather than the object
(`docs/NSFS_CORPUS.md` §7).  What does not exist is the statement of
which differences are **expected** — an ETag that cannot survive a
migration is not a defect, and a lost ACL is.  Without that list a
diff is noise.

**Until it exists**, the first three steps are worth running on their
own.  Step 2's capture is a measurement of their on-disk format that
nothing else produces, and step 3 either serves the tree or reports why
it cannot.  The first full run found that NooBaa's completed-multipart
ETag is `mtime-<base36>-ino-<base36>` rather than a digest with a part
count, that they answer `501 NotImplemented` to
PutObjectLockConfiguration at the pinned commit, and that our own
bucket-configuration axis was looking for configuration only where
*this* driver stores it.

---

## Which document owns what

| | |
|---|---|
| `README.md` (here) | the stand-up:  pinning, the worktree, their build, the config store |
| this file | the sequence, and what each step produces |
| `../s3-tests-rs/tests/test_corpus/README.md` | running the corpus, the capture types, adding an axis |
| `docs/NSFS_CORPUS.md` | why the corpus is shaped as it is;  §7 the two formats |
| `docs/NSFS_ADOPTION.md` | the adoption model:  §7 the modes, §8 the permission rules, §9 how the work is scheduled |
| `docs/ACCOUNT_IMPORT.md` | the import:  §6.1 what it does in order, §6.2 failure and re-running |
| `docs/NSFS_ADOPTION_WORK_PLAN.md` | the stages, and which are done |
| `docs/RGW_FS_DEV_GUIDE.md` §6 | the same ground for a developer who is not doing adoption work |

The `docs/` set is the nsfs design notes, held outside this tree.
