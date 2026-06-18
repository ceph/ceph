# qa/ — Integration Testing Infrastructure

## Purpose

The `qa/` directory contains Ceph's integration testing infrastructure. There are two main approaches: **Teuthology** (distributed cluster testing on real or virtual machines) and **standalone tests** (shell scripts that run on a single node using vstart clusters). Unit tests are separate — see [../src/test/AGENTS.md](../src/test/AGENTS.md).

## Key Files and Directories

### Standalone Tests
| Path | Purpose |
|------|---------|
| `standalone/` | Shell-based tests that run on a single node using vstart clusters |
| `standalone/ceph-helpers.sh` | ~80KB utility library — functions for starting/stopping daemons, waiting for cluster health, creating pools, etc. All standalone tests source this. |

### Teuthology Suites
| Path | Purpose |
|------|---------|
| `suites/` | Test suite definitions (YAML-based). Each suite is a directory tree; YAML fragments are composed to create test configurations. |
| `tasks/` | Python task implementations (~134 files). Each task is a Python class that teuthology executes on test nodes. |
| `workunits/` | Self-contained test scripts that run on test nodes (assume a running cluster). |
| `clusters/` | Predefined cluster configurations (YAML) specifying node count and roles. |
| `config/` | Test configuration fragments. |
| `overrides/` | Configuration overrides for test matrix generation. |
| `distros/` | Distribution-specific test configurations. |
| `machine_types/` | Machine type configurations for test infrastructure. |

### Major Test Suites (`suites/`)
| Suite | Tests |
|---|---|
| `rados/` | Core RADOS object storage (28 subdirectories) |
| `rbd/` | RBD block device (22 subdirectories) |
| `rgw/` | RGW S3/Swift gateway (24 subdirectories) |
| `fs/` / `cephfs/` | CephFS filesystem (28 subdirectories) |
| `crimson-rados/` | Crimson OSD |
| `smoke/` | Quick smoke tests |
| `upgrade/` | Cluster upgrade tests |
| `perf-basic/` | Performance benchmarks |
| `nvmeof/` | NVMe-oF gateway tests |

### Key Task Files (`tasks/`)
| File | Purpose |
|------|---------|
| `ceph.py` | Core cluster control — start/stop daemons, health checks |
| `cephadm.py` | Container-based deployment testing |
| `ceph_test_case.py` | Base test class for Python-based tests |
| `cephfs/` | CephFS-specific tasks (60 modules) |

## Patterns and Idioms

### Standalone Test Pattern
```bash
#!/bin/bash
source $(dirname $0)/../ceph-helpers.sh

function run() {
    local dir=$1
    shift
    setup $dir || return 1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    wait_for_clean || return 1

    # ... test logic ...

    teardown $dir || return 1
}

main my-test "$@"
```

### Teuthology Suite Composition
YAML fragments in the suite directory tree are composed to create test configurations:
- Single test: `suites/foo/test.yaml`
- Combined tests: `suites/foo/bar/+` (combines fragments a.yaml, b.yaml, c.yaml)
- Test matrix: `suites/foo/%` (creates all combinations)
- Random selection: `suites/foo/$` (picks one randomly)

### Teuthology Task Pattern
```python
from teuthology.task import Task

class MyTask(Task):
    def setup(self):
        # Cluster setup
        pass

    def begin(self):
        # Run test
        pass

    def end(self):
        # Cleanup
        pass
```

## Navigation Hints

- To write a quick single-node test: create a shell script under `standalone/` using `ceph-helpers.sh`
- To understand the `ceph-helpers.sh` API: read the function list at the top of the file
- To add a teuthology test: create a YAML suite definition under `suites/` and implement tasks in `tasks/`
- To run standalone tests locally: `qa/standalone/<category>/test_name.sh`
- To understand how teuthology composes tests: look at the directory structure under any suite

## Gotchas

- `ceph-helpers.sh` is ~80KB and central to all standalone tests. Changes to it can break many tests.
- Teuthology suites generate a combinatorial matrix of tests. A seemingly small change to a suite definition can produce hundreds of additional test runs.
- Workunits assume a running cluster. They do not set up or tear down clusters themselves.
- Standalone tests create temporary vstart clusters. They need the Ceph binaries to be built and available in PATH.
- Some QA tests are very long-running (hours). Check the suite's estimated run time before launching.
