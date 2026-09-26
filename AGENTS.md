# Ceph Agent Instructions

Ceph is a distributed storage system (object/block/file). Repo: https://github.com/ceph/ceph

Per-component AGENTS.md files will expand on component-specific rules. This file covers repo-wide conventions.

## Key docs (read before working in these areas)

- `README.md` — build, test, vstart cluster quickstart
- `CodingStyle` — C++/Python/JS style rules
- `SubmittingPatches.rst` — commit format, PR rules for `main`
- `SubmittingPatches-backports.rst` — backport workflow, cherry-pick rules, releng-audit CI
- `CONTRIBUTING.rst` — governance, contribution overview
- `.github/pull_request_template.md` — PR checklist (fill out for every PR)

## Repo layout

- `src/` — all source; major subdirs: `mon/ osd/ mds/ mgr/ rgw/ client/ common/ librados/ librbd/ cls/ pybind/ test/ script/`
- `qa/` — integration tests (teuthology)
- `doc/` — RST documentation
- `build/` — created by `do_cmake.sh` (not in git)

## Build

```
git submodule update --init --recursive --recommend-shallow
./install-deps.sh && apt install python3-routes   # Debian/Ubuntu
./do_cmake.sh          # creates build/ (Debug by default)
cd build && ninja -j3  # ~2.5 GB RAM per job
```

- `ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" ./do_cmake.sh` for non-debug
- `build/` must not exist before running `do_cmake.sh`
- Never commit submodule changes unless intentionally updating them; use `git add <specific-files>`

## Test

```
cd build
ctest -j$(nproc)                   # unit tests (unittest_* targets)
ninja check -j$(nproc)             # same + deps
../qa/run-standalone.sh            # broader; capture output to file
```

- `ceph_test_*` targets must be run manually (not via ctest)
- Test logs: `build/Testing/Temporary/`
- Jenkins CI: comment `jenkins test make check` on a PR

## Development cluster

```
cd build && ninja vstart
../src/vstart.sh --debug --new -x --localhost --bluestore
./bin/ceph -s
../src/stop.sh
```

## Coding style

**C++** (see `CodingStyle` and `.clang-format`):
- Google C++ guide as baseline with Ceph overrides
- Functions: `snake_case()` — Classes: `CamelCase` — Members: `m_foo` — Constants/Enums: `ALL_CAPS`
- 2-space indent, no tabs; always braces on conditionals; `#pragma once`
- New code only; don't reformat unrelated old code

**Python**: PEP-8. Run `tox` in `src/pybind/`, `src/cephadm/`, `src/ceph-volume/`, `src/python-common/`, or `qa/` as relevant.

**JS/TS** (mgr/dashboard): Angular style guide + ESLint + Prettier.

## Commit messages

```
<subsystem>: <imperative summary, ≤72 chars>

<body: what and why>

Fixes: https://tracker.ceph.com/issues/NNNNN
Signed-off-by: Name <email>
```

- Always `git commit -s`
- Subsystem = component path prefix: `osd`, `mgr/dashboard`, `doc/rados`, etc.
- PR title follows same `subsystem: description` format

## PRs targeting `main`

- Target `main`, never stable branches directly
- Fill the checklist in `.github/pull_request_template.md`
- Update docs for any user-facing change
- License: LGPL 2.1/3.0 compatible (no GPL in most areas; check `COPYING`)
- For backport requests: open a tracker issue at https://tracker.ceph.com with `Backport:` field

## Backport PRs (targeting stable branches)

Full rules in `SubmittingPatches-backports.rst`. Summary:
- Fix `main` first, then cherry-pick with `git cherry-pick -x`
- PR title: `<branch>: <subsystem>: <description>` (e.g. `squid: osd: fix foo`)
- Set milestone to the stable branch name
- Do NOT modify the original commit message; only add a `Conflicts:` block after the `(cherry picked from ...)` line documenting every manually changed file
- The `releng-audit` CI enforces these rules and will block the PR on violations
- Do NOT merge backport PRs yourself

## Critical pitfalls

- Missing submodules → `git submodule update --init --recursive`
- OOM during build → reduce with `ninja -j2` or `ninja -j1`
- `build/` already exists → `rm -rf build` then re-run `do_cmake.sh`
- Python import errors → `apt install python3-routes`
- Tests need `ulimit -n 4096` and working `hostname --fqdn`
