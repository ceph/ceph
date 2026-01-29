# Ceph Coding Agent Instructions

## Repository Overview

Ceph is a scalable distributed storage system providing object, block, and file storage in a unified platform. The codebase is approximately 281MB with 177 CMakeLists.txt files across the project.

**Primary Languages & Technologies:**
- C++ (main codebase - follows Google C++ Style Guide with modifications)
- Python 3 (management tools, tests)
- CMake (build system using Ninja)
- Bash (scripts and utilities)

**Key Components:**
- **MON (Monitor)**: Cluster membership and state (`src/mon/`)
- **OSD (Object Storage Daemon)**: Data storage and replication (`src/osd/`)
- **MDS (Metadata Server)**: CephFS metadata (`src/mds/`)
- **MGR (Manager)**: Cluster management and monitoring (`src/mgr/`)
- **RGW (RADOS Gateway)**: Object storage API (S3/Swift) (`src/rgw/`)
- **Client Libraries**: librados, librbd, libcephfs (`src/librados/`, `src/librbd/`, etc.)

## Critical Build Requirements

### Prerequisites (MUST run before building)
```bash
# 1. Initialize submodules (REQUIRED - will fail without this)
git submodule update --init --recursive --recommend-shallow --progress

# 2. Install dependencies
./install-deps.sh

# 3. For Ubuntu/Debian, also install (verified necessary):
apt install python3-routes
```

### Build Process (Standard Development Build)
```bash
# From repository root:
./do_cmake.sh
cd build
ninja -j3  # Use -j3 or lower to avoid OOM; each job needs ~2.5GB RAM
```

**IMPORTANT BUILD NOTES:**
- `do_cmake.sh` creates **Debug builds** by default (if `.git` exists). Debug builds are 5x slower than release builds.
- For performance testing, always use: `ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" ./do_cmake.sh`
- The `build/` directory must NOT exist before running `do_cmake.sh` (script will exit with error)
- Build can take 40-60GB disk space. Ensure adequate space before starting.
- Memory: Plan for ~2.5GB RAM per ninja job. Use `-j<N>` to limit jobs on constrained systems.
- If you see `g++: fatal error: Killed signal terminated program cc1plus`, reduce ninja jobs.

### Running Tests

**Unit Tests (ctest):**
```bash
cd build
ninja  # Ensure build is complete first
ctest -j$(nproc)  # Run all tests in parallel

# To run specific tests:
ctest -R <regex_pattern>

# For verbose output:
ctest -V -R <pattern>
```

**Note:** Targets starting with `unittest_*` are run by ctest. Targets starting with `ceph_test_*` must be run manually.

**Make Check (Full Test Suite):**
```bash
# From repository root:
./run-make-check.sh

# Or manually:
cd build
ninja check -j$(nproc)
```

**Prerequisites for make check:**
- `ulimit -n` must be >= 1024 (script sets this automatically)
- `hostname --fqdn` must work (will fail otherwise)
- Sufficient file descriptors and AIO resources

**Test logs on failure:** Located in `build/Testing/Temporary/`

### Development Cluster (vstart)

Start a local test cluster for development:
```bash
cd build
ninja vstart  # Builds minimal required components
../src/vstart.sh --debug --new -x --localhost --bluestore
./bin/ceph -s  # Check cluster status

# Test commands:
./bin/rbd create foo --size 1000
./bin/rados -p foo bench 30 write

# Stop cluster:
../src/stop.sh
```

**vstart Options:**
- `--new` or `-n`: Start fresh cluster (use `-n` to preserve data between restarts)
- `--debug` or `-d`: Enable debug logging
- `--localhost`: Bind to localhost only
- `--bluestore`: Use BlueStore backend (recommended)

## Coding Standards

### C++ Code (src/)
- Follow Google C++ Style Guide with Ceph modifications (see `CodingStyle` file)
- **Naming:**
  - Functions: `use_underscores()` (NOT CamelCase)
  - Classes: `CamelCase` with `m_` prefix for members
  - Structs (data containers): `lowercase_t`
  - Constants: `ALL_CAPS` (NOT kCamelCase)
  - Enums: `ALL_CAPS`
- **Formatting:**
  - Indent: 2 spaces (NO tabs)
  - Always use braces for conditionals, even one-liners
  - No spaces inside conditionals: `if (foo)` not `if ( foo )`
- **Headers:** Use `#pragma once` (preferred over include guards)
- Use `.clang-format` for C++ code formatting

### Python Code
- Follow PEP-8 strictly for new code
- Multiple `tox.ini` files exist for Python linting:
  - `src/pybind/tox.ini`
  - `src/cephadm/tox.ini`
  - `src/ceph-volume/tox.ini`
  - `src/python-common/tox.ini`
  - `qa/tox.ini`

### Commit Messages
- **Title Format:** `<subsystem>: <imperative mood description>` (max 72 chars)
  - Examples: `mon: add perf counter for finisher`, `doc/mgr: fix typo`
- **Body:** Explain "what" and "why", not just "what"
- **Required:** `Signed-off-by: Your Name <email@example.com>` (use `git commit -s`)
- **Optional:** `Fixes: http://tracker.ceph.com/issues/XXXXX` (before Signed-off-by)

## Common Build Patterns

### CMake Options (via ARGS)
```bash
# Performance build:
ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" ./do_cmake.sh

# Without RADOS Gateway:
ARGS="-DWITH_RADOSGW=OFF" ./do_cmake.sh

# Use system Boost:
ARGS="-DWITH_SYSTEM_BOOST=ON" ./do_cmake.sh

# Enable ccache/sccache (auto-detected if available):
# Already handled by do_cmake.sh

# Custom compiler:
ARGS="-DCMAKE_C_COMPILER=gcc-12 -DCMAKE_CXX_COMPILER=g++-12" ./do_cmake.sh
```

### Building Specific Targets
```bash
cd build
ninja <target_name>  # Build only specific target
ninja install        # Install vstart cluster
```

## Container Builds

Use `src/script/build-with-container.py` for isolated builds:

```bash
# Build on CentOS 9:
./src/script/build-with-container.py -d centos9 -b build.centos9 -e build

# Build on Ubuntu 22.04:
./src/script/build-with-container.py -d ubuntu22.04 -b build.u2204 -e build

# Run tests in container:
./src/script/build-with-container.py -e tests

# Interactive shell:
./src/script/build-with-container.py -e interactive
```

## CI/CD Workflows

**GitHub Actions Workflows (`.github/workflows/`):**
- `pr-checklist.yml`: Validates PR checklist completion
- `check-license.yml`: Ensures no GPL code in certain areas
- `pr-check-deps.yml`: Dependency validation
- Other workflows for backports, triage, etc.

**Jenkins CI:**
- Triggered by PRs or comment: `jenkins test make check`
- Runs full `run-make-check.sh` on Sepia Lab infrastructure
- Results posted back to GitHub PR

## Directory Structure

**Essential Paths:**
```
ceph/
├── src/                    # All source code
│   ├── mon/               # Monitor daemon
│   ├── osd/               # OSD daemon
│   ├── mds/               # MDS daemon
│   ├── mgr/               # Manager daemon
│   ├── rgw/               # RADOS Gateway
│   ├── client/            # Client-side code
│   ├── common/            # Common utilities
│   ├── msg/               # Messaging layer
│   ├── auth/              # Authentication
│   ├── cls/               # Object classes
│   ├── librados/          # RADOS client library
│   ├── librbd/            # RBD client library
│   ├── pybind/            # Python bindings
│   ├── test/              # Unit tests
│   ├── script/            # Build and utility scripts
│   └── vstart.sh          # Development cluster script
├── qa/                     # Integration test suites (teuthology)
├── doc/                    # Documentation (RST format)
├── cmake/                  # CMake modules
├── build/                  # Build directory (created by do_cmake.sh)
├── CMakeLists.txt          # Root CMake configuration
├── do_cmake.sh             # CMake wrapper script
├── install-deps.sh         # Dependency installation
├── run-make-check.sh       # Full test suite runner
└── .clang-format           # C++ formatting rules
```

## Common Pitfalls & Solutions

### Build Failures
1. **"No such file or directory" for submodule files:**
   - **Solution:** Run `git submodule update --init --recursive --recommend-shallow`
   
2. **"g++: fatal error: Killed":**
   - **Solution:** Out of memory. Use `ninja -j2` or `ninja -j1`

3. **"'build' dir already exists":**
   - **Solution:** `rm -rf build` then re-run `do_cmake.sh`

4. **Python module import errors:**
   - **Solution:** Run `apt install python3-routes` (often missed in install-deps.sh)

### Test Failures
1. **"ulimit -n too small":**
   - **Solution:** `ulimit -n 4096` before running tests
   
2. **"hostname --fqdn" fails:**
   - **Solution:** Fix system hostname configuration

3. **Temp files accumulate in /tmp:**
   - **Solution:** `rm -fr /tmp/ceph-asok.*` between test runs

## Documentation

- Build documentation: `admin/build-doc` (requires packages from `doc_deps.deb.txt`)
- Documentation source: `doc/` directory (RST format)
- Follow Google Developer Documentation Style Guide for doc changes
- Doc changes must accompany user-facing functionality changes

## Submitting Changes

**Required for all PRs:**
1. Sign commits: `git commit -s`
2. Follow commit message format (see above)
3. Update documentation for user-facing changes
4. PRs target `main` branch (not stable branches)
5. All code must be LGPL 2.1/3.0 compatible (NO GPL in most areas)

**Recommended before submitting:**
```bash
# Build and verify:
./install-deps.sh
./do_cmake.sh
cd build && ninja

# Run tests (optional but recommended):
ctest -R <relevant_tests>
```

## Quick Reference

**Environment Variables:**
- `BUILD_DIR`: Override build directory (default: `build`)
- `CEPH_GIT_DIR`: Path to ceph git checkout (default: `..`)
- `ARGS`: Pass additional CMake arguments to do_cmake.sh

**Important Files:**
- `CONTRIBUTING.rst`: Contribution guidelines
- `SubmittingPatches.rst`: Patch submission process
- `CodingStyle`: Coding standards
- `README.md`: General project information
- `.clang-format`: C++ code formatting rules

---

**Trust these instructions first.** Only search for additional information if these instructions are incomplete or incorrect. The Ceph build system is complex but well-documented. When in doubt, consult README.md, SubmittingPatches.rst, or ask for guidance.
