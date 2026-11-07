# AGENTS.md

Guidance for AI coding agents working in the Ceph repository. Ceph is a large
C++/Python monorepo (RADOS, RBD, CephFS, RGW, MGR, cephadm, dashboard). Human
contributor docs remain the source of truth; this file summarises the rules
that are easy to miss and links to the canonical documents.

- Contribution rules: [`SubmittingPatches.rst`](SubmittingPatches.rst),
  [`SubmittingPatches-backports.rst`](SubmittingPatches-backports.rst),
  [`CONTRIBUTING.rst`](CONTRIBUTING.rst)
- Coding style: [`CodingStyle`](CodingStyle), [`.clang-format`](.clang-format)
- Detailed build, test, and troubleshooting notes:
  [`.github/copilot-instructions.md`](.github/copilot-instructions.md)
- Developer guide: [`doc/dev/developer_guide/`](doc/dev/developer_guide/)

## Rules

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
  `struct_v`) without bumping versions and keeping old decoders working:
  mixed-version clusters and upgrades must keep working. Corpus checks live in
  `ceph-object-corpus` and `src/test/encoding/`.
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
- Agents must not add a `Signed-off-by` on the human's behalf, invent tracker
  numbers, or fabricate test results. Leave placeholders and say so.
- PR titles use the same `<subsystem>: <description>` form; they feed the
  release notes. Backport PR titles are prefixed with the branch:
  `squid: osd: ...`.
- Fill in the checklist in
  [`.github/pull_request_template.md`](.github/pull_request_template.md)
  (tracker, component impact, docs, tests). CI checks it.

## Build

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
- A full build needs tens of GB of disk and a long time. Prefer building and
  testing the specific targets affected by your change.
- Containerised builds: `src/script/build-with-container.py`.

## Test

- C++ unit tests (`unittest_*` targets) run via ctest:
  `cd build && ninja unittest_foo && ctest -R unittest_foo -V`.
  `ceph_test_*` binaries are not run by ctest and must be invoked manually.
- Full "make check": `./run-make-check.sh` (slow; Jenkins runs it on every PR).
- Standalone tests: `cd build && ../qa/run-standalone.sh <script>`.
- Local dev cluster: `cd build && ninja vstart &&
  ../src/vstart.sh -d -n -x --localhost` then `./bin/ceph -s`; stop with
  `../src/stop.sh`.
- Python components use tox in their own directory:
  - `src/pybind/mgr`: `tox -e py3,mypy,flake8,check-black,check-isort`
    (fix formatting with `tox -e format-black,format-isort`)
  - `src/cephadm`: `tox -e flake8,mypy,check-black,py3`
  - `src/ceph-volume`, `src/python-common`, `qa`: see each `tox.ini`
- Dashboard frontend (`src/pybind/mgr/dashboard/frontend`):
  `npm ci`, `npm run lint`, `npm run fix`, `npx jest <path>`; use
  `npm run test:ci` for a full run.
- Bug fixes should come with a test that reproduces the bug. Integration tests
  live in `qa/` (teuthology suites); they cannot be run locally in full.

## Code Style

- C++: follow `CodingStyle` (Google style with Ceph changes): 2-space indent,
  `snake_case` functions, `CamelCase` classes, `m_` member prefix,
  `ALL_CAPS` constants and enums, braces on every conditional,
  `#pragma once`. Format only the lines you change with `clang-format`;
  never reformat whole files.
- Use Ceph's logging (`dout`/`ldout`, `derr`) and subsystem debug levels, not
  `std::cout`. Respect existing locking and `ceph::mutex` conventions.
- Python: PEP 8, type hints for new code, must pass the component's `mypy`
  and `flake8` tox environments.
- Docs: reStructuredText under `doc/`, following the Google developer
  documentation style guide. Build with `admin/build-doc`.

## Ask First

- Changes to encodings, the messenger protocol, OSD/MON on-disk formats,
  or cluster upgrade behaviour.
- New dependencies, submodule bumps, or changes to `install-deps.sh`,
  packaging (`ceph.spec.in`, `debian/`), or CI workflows.
- Large cross-subsystem refactors.
