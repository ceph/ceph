# Ceph Codebase Navigation Guide

## Project Overview

Ceph is a distributed storage system providing three storage interfaces on a single unified cluster: **object storage** (RADOS/RGW), **block storage** (RBD), and **file storage** (CephFS). The cluster is managed by a set of daemons:

- **MON** (Monitor) — maintains cluster consensus via Paxos, manages cluster maps (OSDMap, MonMap, MDSMap, FSMap)
- **OSD** (Object Storage Daemon) — stores objects, handles replication/erasure-coding, recovery, scrubbing
- **MDS** (Metadata Server) — manages the CephFS filesystem namespace
- **RGW** (RADOS Gateway) — provides S3 and Swift-compatible HTTP object storage APIs
- **MGR** (Manager) — hosts Python plugin modules for monitoring, orchestration, and management

The codebase is primarily C++20 with Python for management tooling. The build system is CMake.

## Repository Layout

| Directory | Purpose |
|-----------|---------|
| `src/` | All source code — daemons, libraries, and tools |
| `qa/` | Integration test suites (Teuthology) and standalone test scripts |
| `doc/` | Sphinx documentation (user-facing and developer) |
| `cmake/` | CMake build modules and dependency finders |
| `monitoring/` | Grafana dashboards, Prometheus alert rules, SNMP MIBs |
| `container/` | Container/Docker build configurations |
| `debian/` | Debian/Ubuntu packaging |
| `systemd/` | systemd unit files |
| `examples/` | Example configurations |
| `man/` | Man pages |
| `admin/` | Administration utilities |

## Source Code Map (`src/`)

| Directory | What It Is | Guide |
|-----------|-----------|-------|
| `src/osd/` | Object Storage Daemon — PGs, replication, EC, peering, recovery | [AGENTS.md](src/osd/AGENTS.md) |
| `src/mon/` | Monitor — Paxos consensus, cluster state management | [AGENTS.md](src/mon/AGENTS.md) |
| `src/mds/` | Metadata Server — CephFS namespace, distributed locking | [AGENTS.md](src/mds/AGENTS.md) |
| `src/rgw/` | RADOS Gateway — S3/Swift HTTP API (largest subsystem) | [AGENTS.md](src/rgw/AGENTS.md) |
| `src/mgr/` | Manager — Python plugin host, metrics, orchestration | [AGENTS.md](src/mgr/AGENTS.md) |
| `src/crimson/` | Next-gen async OSD using Seastar framework | [AGENTS.md](src/crimson/AGENTS.md) |
| `src/librados/` | RADOS client library (C and C++ API) | [AGENTS.md](src/librados/AGENTS.md) |
| `src/librbd/` | RBD (block device) client library | [AGENTS.md](src/librbd/AGENTS.md) |
| `src/common/` | Core shared utilities (bufferlist, mutexes, formatters, config) | [AGENTS.md](src/common/AGENTS.md) |
| `src/include/` | Shared headers (encoding, types, Context, feature bits) | [AGENTS.md](src/include/AGENTS.md) |
| `src/msg/` | Messaging layer (async networking, protocol v1/v2) | [AGENTS.md](src/msg/AGENTS.md) |
| `src/os/` | ObjectStore abstraction (BlueStore, MemStore) | [AGENTS.md](src/os/AGENTS.md) |
| `src/crush/` | CRUSH placement algorithm | [AGENTS.md](src/crush/AGENTS.md) |
| `src/erasure-code/` | Erasure coding plugin framework | [AGENTS.md](src/erasure-code/AGENTS.md) |
| `src/cls/` | Server-side RADOS classes (custom object ops on OSD) | [AGENTS.md](src/cls/AGENTS.md) |
| `src/pybind/mgr/` | Python manager modules (dashboard, cephadm, etc.) | [AGENTS.md](src/pybind/mgr/AGENTS.md) |
| `src/tools/` | CLI tools (crushtool, osdmaptool, rados, rbd) | [AGENTS.md](src/tools/AGENTS.md) |
| `src/test/` | C++ unit tests (GTest/GMock) | [AGENTS.md](src/test/AGENTS.md) |
| `qa/` | Integration tests (Teuthology suites, standalone scripts) | [AGENTS.md](qa/AGENTS.md) |
| `src/client/` | CephFS FUSE client | |
| `src/osdc/` | Client-side Objecter — sends OSD operations | |
| `src/neorados/` | New async RADOS API (boost::asio) | |
| `src/auth/` | Authentication (CephX) | |
| `src/kv/` | Key-value store abstraction (RocksDB wrapper) | |
| `src/global/` | Global initialization and signal handling | |
| `src/log/` | Logging infrastructure | |
| `src/compressor/` | Compression plugins (zstd, snappy, lz4, zlib) | |
| `src/blk/` | Block device abstraction | |
| `src/messages/` | 176 message type headers (one per inter-daemon message) | |
| `src/cephadm/` | Container-based cluster deployment tool | |
| `src/ceph-volume/` | LVM-based OSD provisioning tool | |

### Vendored Third-Party (do NOT modify)
`src/rocksdb/`, `src/seastar/`, `src/fmt/`, `src/zstd/`, `src/xxHash/`, `src/BLAKE3/`, `src/arrow/`, `src/googletest/`, `src/json_spirit/`, `src/spdk/`, `src/isa-l/`, `src/c-ares/`, `src/utf8proc/`, `src/dmclock/`, `src/lss/`, `src/breakpad/`, `src/jaegertracing/`, `src/s3select/`, `src/qatlib/`, `src/qatzip/`, `src/uadk/`, `src/libkmip/`, `src/blkin/`

## Daemon Entry Points

| Daemon | Source File | Binary |
|--------|-----------|--------|
| OSD | `src/ceph_osd.cc` | `ceph-osd` |
| Monitor | `src/ceph_mon.cc` | `ceph-mon` |
| MDS | `src/ceph_mds.cc` | `ceph-mds` |
| Manager | `src/ceph_mgr.cc` | `ceph-mgr` |
| RGW | `src/rgw/rgw_main.cc` | `radosgw` |
| Crimson OSD | `src/crimson/osd/main.cc` | `crimson-osd` |

## Build System

```bash
./install-deps.sh              # Install build prerequisites
./do_cmake.sh                  # Configure (creates build/ directory)
cd build && ninja              # Build (or make -jN)
./run-make-check.sh            # Full build + unit tests
```

- CMake modules in `cmake/modules/` handle dependency detection
- Config options defined in YAML files under `src/common/options/` (e.g., `osd.yaml.in`, `rgw.yaml.in`)
- `compile_commands.json` in the repo root or build dir for IDE/clangd integration

## Coding Conventions

Based on the project's `CodingStyle` file:

- **Indentation**: 2 spaces, no tabs
- **Functions**: `snake_case()` (not CamelCase)
- **Classes**: `CamelCase` for full classes; `lower_case_t` for simple structs
- **Members**: `m_` prefix or no prefix (not trailing `_`)
- **Constants/Enums**: `UPPER_CASE`
- **Conditionals**: always use braces, body on next line
- **Parameters**: inputs first, then outputs. Use `const&` for inputs, pointers for outputs
- **Constructors**: mark single-arg constructors `explicit`
- **Header guards**: `#pragma once` is acceptable
- **Python**: PEP-8
- **Commit messages**: `subsystem: imperative description` (e.g., `osd: fix PG creation race`). AI agents **MUST NOT** add `Signed-off-by` tags — only humans can certify DCO compliance.

## Rules for AI Coding Agents

### Code Formatting
The repository has a `.clang-format` file at the project root. **All new and modified C++ code must conform to this format.** Key points from the config:
- Based on Google style with Ceph-specific overrides
- C++20 standard
- 2-space indentation, no tabs
- Column limit: 80 (soft), penalty-based line breaking
- Braces: opening brace on same line for classes/structs/control, but **after newline for function definitions**
- Pointer/reference alignment: left (`int* p`, not `int *p`)
- Constructor initializer lists: break after colon, one per line if they don't fit
- Includes are sorted and regrouped by category (local, project, boost, system)

When writing new code, follow these formatting rules. When editing existing code, format only the lines you change.

### Do Not Reformat Existing Code
**Never make whitespace-only or style-only changes to code you are not otherwise modifying.** Reformatting untouched code causes unnecessary git conflicts with other developers' branches. If you see poorly formatted existing code, leave it alone unless you are already making functional changes to those specific lines.

### Minimize Diff Noise
Keep diffs clean and minimal:
- Only change lines that are necessary for the functional change
- Do not re-indent surrounding code blocks unless the change requires it
- Do not reorganize includes in files where you are only changing a few lines
- Do not add or remove blank lines in code you are not modifying

### Comments
Comments should explain **why**, not **what**. Assume developers have AI tools to explain what code does.

**Good:** edge cases, non-obvious invariants, workarounds for specific bugs, constraints from other subsystems.
**Bad:** narrating code line by line, changelog-style ("NOW does X"), describing the current task/PR ("Added for the backfill fix"). When in doubt, leave the comment out.

### Documentation
If your change affects user-visible behavior (CLI commands, config options, APIs, admin operations), you **must** update the corresponding documentation in `doc/`. Documentation in `doc/` is for **users and operators**, not developers. It should:
- Cover use cases for advanced users and administrators
- Explain what changed, how to use it, and any migration steps
- Use practical examples showing real commands and expected output
- Not include internal implementation details, code structure explanations, or developer notes

Developer-facing information belongs in code comments, commit messages, or `doc/dev/` — not in the user-facing docs.

### Design Documents
Complex changes and major new features **must** include a design document in `doc/dev/`. An agent may be asked to write or iterate on a design document — editing it based on human developer feedback is expected and encouraged during the design phase.

However, once implementation begins, the design document becomes the agreed specification. **Never modify a design document during implementation without explicit user permission.** If implementation reveals a design flaw, stop and raise it with the user rather than silently changing the design to match what you built.

### Git Commit Standards

#### Sign-off
All commits must include a `Signed-off-by` line, but **AI agents MUST NOT add `Signed-off-by` tags.** Only humans can legally certify the Developer Certificate of Origin (DCO). The human submitter is responsible for:
- Reviewing all AI-generated code
- Ensuring compliance with licensing requirements
- Adding their own `Signed-off-by` tag to certify the DCO
- Taking full responsibility for the contribution

#### AI Attribution
All AI-generated or AI-assisted code must include an `Assisted-by` tag so human reviewers know which parts of the change need careful scrutiny.

```
Assisted-by: AI-tool-or-model [optional-tooling]
```

### Commit Structure and PR Preparation
During development, commit regularly so progress is preserved. Working commits do not need to be polished.

When asked to prepare for a PR, restructure commits to tell a **story for human reviewers**: preparatory refactoring first, then tests/reproducers, then the main fix, then cleanup. Each commit should be reviewable in isolation. Never mix unrelated changes. A simple fix can be one commit; a complex feature may need several. Pushing a PR is always a human decision.

### Build and Test
- **Build**: `cd build && ninja` (never use `make`). **Never clean the build directory without explicit user permission.**
- **Test**: `cd build && ninja osd_unittests` — baseline mandatory check. If unsure whether to skip, ask the user.

### Agent Workflow
1. Understand the code before modifying it — use `compile_commands.json` with clangd if available
2. Write code following `.clang-format` and `CodingStyle` — format only lines you change
3. Build with `ninja`, run `osd_unittests`, never clean without permission
4. Add an `Assisted-by` tag to commit messages — **never** add a `Signed-off-by` tag; only the human developer may do so

## Navigation Quick Reference

| If you need to... | Look in... |
|---|---|
| Add a config option | `src/common/options/*.yaml.in` |
| Add a CLI command | `src/mon/MonCommands.h` (mon) or mgr module `@CLICommand` |
| Add a new message type | `src/messages/` + register constant in `src/include/ceph_fs.h` |
| Understand data placement | `src/crush/CrushWrapper.h` |
| Understand PG operations | `src/osd/PrimaryLogPG.cc` |
| Add a mgr module | Copy `src/pybind/mgr/hello/` |
| Add a RADOS class | Copy `src/cls/hello/` |
| Understand serialization | `src/include/encoding.h` (`ENCODE_START`/`ENCODE_FINISH`) |
| Understand the wire protocol | `src/msg/async/ProtocolV2.h` |
| Work on BlueStore | `src/os/bluestore/BlueStore.h` |
| Debug an ObjectStore offline | `src/tools/ceph_objectstore_tool.cc` |

## Key Interdependencies

```
                    ┌──────────┐
                    │  common/ │  (used by everything)
                    │ include/ │
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         ┌────▼───┐ ┌───▼────┐ ┌──▼───┐
         │  msg/  │ │  os/   │ │crush/│
         └───┬────┘ └───┬────┘ └──┬───┘
             │          │         │
    ┌────────┼──────────┼─────────┘
    │        │          │
┌───▼──┐ ┌──▼──┐ ┌────▼───┐ ┌─────┐
│ osd/ │ │mon/ │ │  mds/  │ │mgr/ │
└──┬───┘ └─────┘ └────────┘ └─────┘
   │
   │  ┌──────────┐  ┌──────────┐  ┌──────┐
   └──│erasure-  │  │librados/ │  │ rgw/ │
      │code/     │  └────┬─────┘  └──┬───┘
      └──────────┘       │           │
                    ┌────▼─────┐     │
                    │ librbd/  │◄────┘
                    └──────────┘
```

Note: `rgw/` depends on `librados/` (not on `os/` or `crush/` directly). `librados/` depends on `msg/`, `os/`, and `crush/` transitively, but `rgw/` does not.
