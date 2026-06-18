# src/test/ — C++ Unit Tests

## Purpose

This directory contains C++ unit tests using the **Google Test (GTest)** and **Google Mock (GMock)** frameworks. Tests generally mirror the source tree structure — tests for subsystem `foo` are in `test/foo/`.

## Organization

### Test File Naming
- `test_*.cc` — standalone tests
- `unittest_*.cc` — unit tests linked against GTest
- Test files are registered in `CMakeLists.txt` via `add_ceph_test()` or `add_ceph_unittest()`

### Major Test Subdirectories
| Subdirectory | Tests For |
|---|---|
| `osd/` | OSD, PG, PGBackend, PGLog, EC, peering |
| `mon/` | Monitor, Paxos, PGMap |
| `mds/` | MDS metadata operations |
| `msgr/` | Messaging layer |
| `crimson/` | Crimson async engine |
| `librados/` | RADOS client library |
| `librbd/` | RBD image library |
| `client/` | CephFS client |
| `objectstore/` | BlueStore and ObjectStore |
| `erasure-code/` | Erasure coding plugins |
| `compressor/` | Compression plugins |
| `encoding/` | Encoding roundtrip tests |
| `common/` | Common utilities |
| `cls_rbd/`, `cls_lock/`, `cls_log/`, `cls_rgw/`, etc. | RADOS class tests |
| `journal/` | RBD journal tests |
| `admin_socket/` | Admin socket tests |
| `cli/` | CLI output tests |

## Building and Running Tests

```bash
cd build && ninja unittest_foo && ./bin/unittest_foo   # Build and run one test
GTEST_FILTER="Suite.Name" ./bin/unittest_foo           # Filter test cases
ctest -j$(nproc)                                       # Run all registered tests
```

## Patterns and Idioms

Standard GTest/GMock. Tests use `TEST_F` with fixture classes. Register in `CMakeLists.txt` via `add_ceph_unittest()`. Many tests use `MemStore` (in-memory ObjectStore) to avoid disk I/O.

## Navigation Hints

- To find tests for a specific subsystem: look in `test/<subsystem>/`
- To add a new test: create a `test_*.cc` or `unittest_*.cc` file in the appropriate subdirectory and register it in `CMakeLists.txt`
- To understand test fixtures for OSD tests: see `test/osd/` for PGBackend and PGLog test fixtures
- For encoding roundtrip tests: see `test/encoding/`
- For integration-level tests: see `qa/` ([../../qa/AGENTS.md](../../qa/AGENTS.md))

## Gotchas

- Some tests require a running cluster or specific hardware. These are typically skipped in unit test runs and handled by the QA infrastructure instead.
- Test binaries are built in `build/bin/`. Use `ninja <test_name>` to build a specific test without building everything.
- `run-make-check.sh` does a full build plus tests. It's slow — for iterative development, build and run individual tests.
- Some tests use `GTEST_SKIP()` for conditional execution. Check test output for skipped tests.
- The `cls_*` test directories test both the server-side class code and the client-side helper libraries.
