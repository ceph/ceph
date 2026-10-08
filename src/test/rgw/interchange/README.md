# nsfs interchange:  standing NooBaa up

`standup.rs` brings a pinned NooBaa NSFS gateway up over a data root,
so the conversion corpus can be run against *their* implementation and
the result compared with ours.

It is the workflow driver and tests nothing itself.  The corpus is in
`../s3-tests-rs/tests/test_corpus/`.

## Why this exists

`docs/NSFS_ADOPTION_WORK_PLAN.md` §4, in the nsfs design set:  until
this exists, every control in the adoption work is composed from *our
reading* of NooBaa's schema, so a passing stage is evidence about our
reading rather than about their output.  The corpus's seven axes
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
| `IX_NOOBAA_SRC` | the developer's noobaa-core clone.  **Read only** |
| `IX_PIN` | the commit to stand up |
| `IX_WORK` | scratch:  the worktree, their config, logs |
| `IX_DATA` | the tree both gateways serve |
| `IX_PORT` | their endpoint |

## Decisions

**Source, not a container.**  Containers are not helpful during
development, and the data root has to be a plain directory that RGW
can open afterwards *as the same uid* — which is the permission
surface the conversion work already spent a long time on.  A container
would pin their whole toolchain, which is the stronger answer for a
released version we must re-check later;  it is not worth the cost
now.

**Sequential handoff against one tree.**  NooBaa writes, stops, then
RGW opens the same directory.  That is the adoption story — drain,
then upgrade — and it is what `docs/NSFS_CORPUS.md` already assumes.
Both gateways serving at once is a separate feature and must not be
smuggled in here.

**A git worktree at the pin, never a checkout.**  Reading their source
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

## Their lock does not `npm ci`, so the tree is pinned here

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
their commit.

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

`list` marks what is built.  `pin`, `worktree` and `build` work;
`config`, `serve`, `capture` and `stop` are not written — that is where
the design discussion had reached.
