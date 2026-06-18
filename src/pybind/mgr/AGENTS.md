# src/pybind/mgr/ — Python Manager Modules

## Purpose

This directory contains the Python modules that run inside the Manager daemon (ceph-mgr). There are 50+ modules providing web UI, monitoring, orchestration, CLI commands, and various management features. Each module is a directory containing a `module.py` (or equivalent) that defines a class inheriting from `MgrModule`.

The C++ hosting infrastructure for these modules is in `src/mgr/` — see [../../mgr/AGENTS.md](../../mgr/AGENTS.md).

## Key Files (Framework)

| File | Role |
|------|------|
| `mgr_module.py` | `MgrModule` base class — defines the full interface for modules (options, commands, health checks, notifications, etc.). Read this first. |
| `mgr_util.py` | Shared utility functions used across modules. |
| `object_format.py` | Output formatting utilities (JSON, YAML, text). |

## Module Directory

### Key Modules
| Module | Purpose |
|---|---|
| `dashboard/` | Web UI (Angular frontend + Python REST backend). Largest module, has its own build system. |
| `cephadm/` | Container-based cluster deployment and lifecycle management. |
| `orchestrator/` | Orchestrator abstraction layer (interface for cephadm/rook). |
| `prometheus/` | Prometheus metrics exporter. |
| `balancer/` | PG balancer — distributes PGs evenly across OSDs. |
| `pg_autoscaler/` | Automatic PG count management. |
| `volumes/` | CephFS volume/subvolume management. |
| `rbd_support/` | RBD management commands. |
| `hello/` | **Template module** — copy this to create new modules. |

There are 50+ modules total. Browse the directory listing for the full set.

## Patterns and Idioms

### Module Structure
Every module has a class inheriting `MgrModule` (defined in `mgr_module.py`). Use `hello/` as a template. Key patterns:
- **Commands**: use `@CLICommand` decorator (preferred) or `COMMANDS` class attribute (older)
- **Cluster state**: `self.get_osdmap()`, `self.get('health')`, `self.get('pg_summary')`
- **Health checks**: `self.set_health_checks({...})`
- **Config**: `self.get_module_option('name')` / `self.set_module_option('name', val)`
- **Continuous service**: implement `serve()` with `self.event.wait()` for interruptible sleeping (not `time.sleep()`)

## Dependencies

- `mgr_module.py` — the MgrModule base class (in this directory)
- `src/mgr/` — C++ hosting infrastructure
- Various Python standard library and third-party packages

## Navigation Hints

- To create a new module: copy `hello/` and modify
- To understand the MgrModule API: read `mgr_module.py` — it has comprehensive docstrings
- To add a CLI command to an existing module: use the `@CLICommand` decorator
- To expose a health check: use `self.set_health_checks()`
- To understand the dashboard: see `dashboard/` (it has its own frontend build system using Angular)
- To understand orchestrator interface: read `orchestrator/_interface.py`

## Gotchas

- **Dashboard** is by far the largest module. It has its own Angular frontend, build system, and REST API framework. Treat it as a sub-project.
- Modules run in the Manager daemon's Python interpreter. Thread safety with the C++ GIL layer is handled by the hosting code, but be careful with multi-threaded Python code within a module.
- `serve()` runs in its own thread. Use `self.event.wait()` for interruptible sleeping — do not use `time.sleep()`.
- Module config options persist in the monitor store. Changes are visible cluster-wide.
- Not all modules are enabled by default. Check `ceph mgr module ls` for active modules.
