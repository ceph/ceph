# nsfs adoption:  standing NooBaa up

`standup.rs` brings a pinned NooBaa NSFS gateway up over a data root,
so the conversion corpus can be run against *their* implementation and
the result compared with RGW.

It is the workflow driver and tests nothing itself.  The corpus is in
`../s3-tests-rs/tests/test_corpus/`.

## Why this exists

`docs/NSFS_ADOPTION_WORK_PLAN.md` §4, in the nsfs design set:  until
this exists, every control in the adoption work is composed from *our
reading* of NooBaa's schema, so a passing stage is evidence about our
reading rather than about NooBaa's output.  The corpus's nine axes
validate RGW against S3.  Nothing yet validates RGW against NooBaa,
because nothing has written a NooBaa tree.

## Running it

```sh
cargo +nightly -Zscript standup.rs list      # the steps, and the settings
cargo +nightly -Zscript standup.rs pin       # one step
cargo +nightly -Zscript standup.rs up        # every built step
```

**`-Zscript` is nightly-only** and there is no stable equivalent.  On a
fresh machine:  `rustup toolchain install nightly`.  Stable cargo
cannot run this file.

Every setting is an environment variable with a default;  `list`
prints them.

| | |
|---|---|
| `ADOPTION_NOOBAA_SRC` | the developer's noobaa-core clone.  **Read only** |
| `ADOPTION_PIN` | the commit to stand up |
| `ADOPTION_WORK` | **required.**  the worktree, the NooBaa config, the data root, logs, captures |
| `ADOPTION_DATA` | the tree both gateways serve;  defaults to `$ADOPTION_WORK/data` |
| `ADOPTION_PORT` | the NooBaa endpoint |
| `ADOPTION_CEPH_BUILD` | **required for `adopt`.**  the ceph build directory |
| `ADOPTION_SUITE` | the s3-tests-rs checkout;  defaults to a sibling of this driver |
| `ADOPTION_TIER` | which corpus tier `capture` runs |

## What `capture` produces

`capture` points the conversion corpus at the NooBaa endpoint and keeps
what it produced, per axis, under `$ADOPTION_WORK/capture/noobaa`:

```
coverage-keys.manifest     what S3 says -- one fact per line, sorted
coverage-keys.inventory    what the disk says -- modes, ownership, xattr names
...                        one pair per axis
noobaa.log                 the whole run
```

**The side is in the path, never in the artifacts.**  The comparison
between two gateways is `diff`, so a fact recording which gateway
produced it would differ on every line and the diff would stop saying
anything.  `capture/noobaa` here;  `capture/rgw` is where the run
after `adopt` belongs.

**A failing axis is the finding, not a broken step.**  The corpus
asserts what *our* driver should do.  NooBaa is a different
implementation, and a disagreement is the whole reason to capture.
Each axis writes its manifest and takes its inventory before it
asserts, so a failed axis still leaves a complete pair behind;  the
run is `--no-fail-fast` and a non-zero exit is reported rather than
propagated.  What `capture` does fail on is producing **no** artifacts,
because that means no axis got as far as writing one.

Two things it sets that a normal corpus run does not:

* `CORPUS_OBJECT_MODE=any`.  The corpus checks that every object is
  published `0600`, which is a statement about the nsfs driver rather than
  about S3.  Left strict, every object NooBaa wrote is reported
  as an offender and the differences worth reading are buried.
* a generated `s3tests.conf`, at `$ADOPTION_WORK/s3tests-noobaa.conf`.  The
  suite's own config points at whatever vstart last brought up, and a
  capture that quietly ran against RGW while claiming to have
  run against NooBaa is the worst failure this step could have.  It
  carries all six identity sections the suite's loader demands, with
  placeholders for the five the corpus does not use -- deliberately
  not copies of main's credential, since one NC account serves this
  tree and an axis reaching for a second identity should fail rather
  than silently act as the first.

## Decisions

**Source, not a container.**  Containers are not helpful during
development, and the data root has to be a plain directory that RGW
can open afterwards *as the same uid* — which is the permission
surface the conversion work already spent a long time on.  A container
would pin NooBaa's whole toolchain, which is the stronger answer for a
released version we must re-check later;  it is not worth the cost
now.

**Sequential handoff against one tree.**  NooBaa writes, stops, then
RGW opens the same directory.  That is the adoption story — drain,
then upgrade — and it is what `docs/NSFS_CORPUS.md` already assumes.
Both gateways serving at once is a separate feature and must not be
smuggled in here.

**A git worktree at the pin, never a checkout.**  Reading noobaa-core's source
at some other commit is something this project does constantly, and
`standup.rs` must not disturb the clone's HEAD or index to do its job.
The worktree is `--detach`ed, because leaving a named branch in
someone else's clone is a side effect nobody asked for.

**Steps are individually runnable and idempotent.**  A stand-up that
only runs end to end is one you cannot debug.  Running a step twice
should be a no-op, not an error.

## The pin is a hypothesis

`08d063f86` is *"Bumping version from 5.22.0 to 5.23.0"* — the **start**
of the window in which noobaa-core carried the 5.23 version string.
Whether IBM Storage Scale ships that surface is an open question with
the Scale team;  the standing estimate is 85% that it is the window
start rather than something later in it.  5.23 has no branch and no
tag, so there is nothing more precise to point at.

`standup.rs pin` says so on every run, which is cheaper than someone
rediscovering it.  See `feedback_derive_from_shipped_noobaa` and
`project_scale_team_questions` in the design notes.

## Three things this driver got wrong first, now settled

Agreed and built 2026-10-08.  Kept because each was a reasoning error
rather than a typo, and the reasoning is what would recur.

**A prefix has to be a word.**  This driver's environment variables
were `IX_WORK`, `IX_PIN` and so on, and `IX` was glossed as
*interchange* wherever it appeared.  Glossing a token is not the fix:
two letters carry no lexical structure, so a reader has to be told
every time rather than being able to read it.  They are
`ADOPTION_WORK` and `ADOPTION_PIN` now, and the gloss is gone
because nothing needs glossing.  Config keys in `adoption.conf` are
bare -- `work`, `pin` -- and always were.

**Config should direct where things live, and there should be no
invented default.**  `work` is where a gigabyte of worktree goes *and*
where the config store and data root live -- the artifacts this
exercise produces.  The first version defaulted to
`~/.cache/nsfs-interchange`, which was wrong twice:  it put the
deliverable somewhere nominally disposable, and it chose the location
without being asked.  The driver takes the location from config
or from the environment and **stops** if neither supplies one.

**XDG cache semantics do not control this.**  I reached for them to
justify the default;  this work is explicit, and a location given on
purpose needs no appeal to a general convention.  The reasoning, not
just the path, was the mistake.

---

## NooBaa's lock does not `npm ci`, so the tree is pinned here

`npm ci` is the right verb:  it installs a lockfile exactly, refuses
when the lock and `package.json` disagree, and never rewrites the
lock.  `npm install` is permitted to resolve differently and to
rewrite the lock to match — which it did, the first time this driver
was written with the wrong verb, leaving a modified
`package-lock.json` in the worktree and a dependency tree that was not
the pinned one.

**But `npm ci` cannot succeed at `08d063f86`.**  That commit's
`package-lock.json` is incomplete:  it is missing `@emnapi/core` and
`@emnapi/runtime`, which `@napi-rs/wasm-runtime` requires, and carries
a stale `@emnapi/wasi-threads`.  `package.json` declares neither
`engines` nor `packageManager`, so there is no npm version to select
that would make an incomplete lock complete.  This is a property of
NooBaa's commit.

So the dependency tree is pinned **here** instead.  The first run
resolves it with `npm install` and records the result in `locks/`;
every later run copies that lock in and uses `npm ci`, which is exact.
The recorded lock is committed alongside this driver.  Their
`package-lock.json` is restored afterwards, so the worktree stays
identical to the pin, and `build` checks that before and after.

A container would not have helped.  An image bakes in whatever one
arbitrary resolution produced — the same resolution, without recording
that it made a choice.

What remains outside any lock is narrow:  `node` itself, and the
compilation of native modules against the host toolchain.

## Status

Every step is built.  `up` runs `pin`, `worktree`, `patch`, `build`
and `config`;  `serve`, `capture`, `stop` and `adopt` are each run
deliberately, because they start and stop gateways over a tree that is
the artifact, and chaining them past a failure is how a tree gets
destroyed rather than handed over.

What is **not** here is the comparison pass.  `capture/noobaa` exists;
re-reading those same artifacts over RGW after `adopt`, and
diffing the two, is the next thing and is a corpus question rather
than a driver one -- a re-read observes, where every axis today
writes.
