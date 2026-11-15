# AGENTS.md

Guidance for AI coding agents working in the Ceph repository. Ceph is a large
C++/Python monorepo (RADOS, RBD, CephFS, RGW, MGR, cephadm, dashboard). Human
contributor docs remain the source of truth; this file summarises the rules
that are easy to miss and links to the canonical documents. A nearer
`AGENTS.md` describes the directory it sits in. This file is the process
policy for the whole tree.

- Contribution rules: [`SubmittingPatches.rst`](SubmittingPatches.rst),
  [`SubmittingPatches-backports.rst`](SubmittingPatches-backports.rst),
  [`CONTRIBUTING.rst`](CONTRIBUTING.rst)
- Coding style: [`CodingStyle`](CodingStyle), [`.clang-format`](.clang-format)
- Detailed build, test, and troubleshooting notes:
  [`.github/copilot-instructions.md`](.github/copilot-instructions.md)
- Developer guide: [`doc/dev/developer_guide/`](doc/dev/developer_guide/)
- Security reports: [`SECURITY.md`](SECURITY.md)

## Rules

- Act only on the human's say-so. An agent may push, comment, label, or
  retrigger CI on the human's behalf only when the human has asked for that
  specific action. Do not send anything the human has not seen. Maintainers
  treat what an agent posts as the human's own words.
- Target `main`. Fixes land in `main` first and are cherry-picked to stable
  branches (`squid`, `tentacle`, ...) with `git cherry-pick -x`; never open a
  feature PR against a stable branch.
- Never stage submodule pointer changes (`src/BLAKE3`, `src/googletest`,
  `ceph-object-corpus`, ... see `.gitmodules`) unless the task is a submodule
  bump. Use `git add <paths>`, not `git add .` or `git add -A`.
- Keep changes scoped to one subsystem and one logical change per commit. Do
  not mix refactors, formatting churn, or drive-by fixes with functional
  changes; put a required refactor in its own commit.
- New code must be LGPL-2.1/LGPL-3.0 compatible. Do not copy GPL code, and do
  not add new third-party dependencies or vendored code without asking first.
- Do not change on-disk or on-wire encodings (`ENCODE_START`/`DECODE_START`,
  `struct_v`) casually. Bumping `struct_v` is not enough. A new decoder can
  read old data, but during an upgrade old daemons and clients still have to
  decode, and sometimes re-encode, what new ones send. Gate a new field on a
  feature bit or on `require_osd_release`, so it is encoded only once every
  peer understands it. Mixed-version clusters and upgrades must keep working.
  Corpus checks live in `ceph-object-corpus` and `src/test/encoding/`.
- Configuration options are defined in `src/common/options/*.yaml.in`, not in
  C++ headers. Add or change options there, and reference them from the docs
  with `.. confval:: <name>`.
- User-visible changes (CLI flags, command output, defaults, health warnings,
  behaviour) must update the docs under `doc/` in the same PR. Notable or
  breaking changes also need an entry in [`PendingReleaseNotes`](PendingReleaseNotes).
- Every directory under `qa/suites/` must contain a `.qa` symlink to `../.qa/`;
  CI enforces this with `src/script/verify-qa`.
- Do not edit generated files or build output (`build/`, dashboard
  `dist/`, `node_modules/`); change the source and regenerate.
- Do not hide a failure to make a test pass. Do not add an entry to a QA log
  ignorelist, loosen an assert, or catch and drop an error unless that change
  is the fix, and then explain why in the commit message.

## Fixing Bugs

- For a non-trivial bug, reproduce it first with a unit test, a standalone
  test, or vstart before writing a fix. If it does not reproduce, say so
  and stop rather than guessing at a fix.
- Show that the test fails without the fix and passes with it.
- State what was not built or tested.
- If the bug may be a vulnerability, stop. Follow [`SECURITY.md`](SECURITY.md):
  mail security@ceph.io, and put nothing about it in a public tracker, pull
  request, or commit message.

## Commits And Pull Requests

- Commit title: `<subsystem>: <imperative description>`, at most 72
  characters, e.g. `osd: make the ClassHandler::mutex private`,
  `mgr/dashboard: fix silence form validation`, `doc/rados: fix a typo`.
  Use the path-style prefix for subdirectories (`mgr/cephadm`, `qa/tasks`,
  `rgw/sts`). Changes only under `doc/` start with `doc`.
- The commit body explains *why*, not only *what*.
- Trailers, in this order:
  ```
  Fixes: https://tracker.ceph.com/issues/12345
  Signed-off-by: Real Name <email@example.com>
  ```
  `Fixes:` may also reference the SHA1 of the commit that introduced a
  regression. Sign off with `git commit -s` (DCO); real names only.
- Do not add `Co-authored-by` for an AI tool. That trailer names the tool as
  an author. There is no separate attribution trailer: say which tools were
  used, and how, in the commit message or the pull request description.
- Agents must not add a `Signed-off-by` on the human's behalf, invent tracker
  numbers, or fabricate test results. Leave placeholders and say so.
- PR titles use the same `<subsystem>: <description>` form; they feed the
  release notes. Backport PR titles are prefixed with the branch:
  `squid: osd: ...`.
- Fill in the checklist in
  [`.github/pull_request_template.md`](.github/pull_request_template.md)
  (tracker, component impact, docs, tests). CI checks it.

## Build

The commands and the failure modes are in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md). Short
version:

```bash
git submodule update --init --recursive --recommend-shallow
./install-deps.sh
./do_cmake.sh            # Debug build into ./build; fails if build/ exists
cd build && ninja -j3    # ~2.5 GB RAM per job; lower -j if cc1plus is killed
ninja <target>           # build only what you touched, e.g. ceph-osd, radosgw
```

- Extra CMake flags go through `ARGS`, e.g.
  `ARGS="-DWITH_RADOSGW=OFF" ./do_cmake.sh`. Use `RelWithDebInfo` for any
  performance measurement; Debug builds are several times slower.
- A full build needs tens of GB of disk and a long time. Prefer building the
  targets affected by the change.
- Containerised builds: `src/script/build-with-container.py`.

## Test

How to run ctest, make check, standalone tests, vstart, tox, and the
dashboard frontend is in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md). In
short: `unittest_*` targets run under ctest, `ceph_test_*` binaries do not,
`./run-make-check.sh` is the full check Jenkins runs, and a local cluster is
`ninja vstart` plus `src/vstart.sh`. Python components use tox in their own
directory.

- A feature behind a config option or pool flag that defaults to off is not
  covered by existing suites. Enable it in at least one client suite
  (rbd, fs, rgw) and one upgrade suite under `qa/suites/`, or link a tracker
  for doing so, and state in the PR which suites exercise it.
- Bug fixes should come with a test that reproduces the bug. Integration tests
  live in `qa/` (teuthology suites); they cannot be run locally in full.

## Code Style

- C++: follow `CodingStyle` (Google style with Ceph changes): 2-space indent,
  `snake_case` functions, `CamelCase` classes, member names with an `m_`
  prefix or with none at all, `ALL_CAPS` constants and enums, braces on every
  conditional, `#pragma once`. Format only the lines you change with
  `clang-format`; never reformat whole files.
- Use Ceph's logging (`dout`/`ldout`, `derr`) and subsystem debug levels, not
  `std::cout`. Respect existing locking and `ceph::mutex` conventions.
- Python: PEP 8, type hints for new code, must pass the component's `mypy`
  and `flake8` tox environments.
- Docs: reStructuredText under `doc/`. See
  [`doc/dev/documenting.rst`](doc/dev/documenting.rst). Build with
  `admin/build-doc`.

## Ask First

- Changes to encodings, the messenger protocol, OSD/MON on-disk formats,
  or cluster upgrade behaviour.
- New dependencies, submodule bumps, or changes to `install-deps.sh`,
  packaging (`ceph.spec.in`, `debian/`), or CI workflows.
- Large cross-subsystem refactors.
