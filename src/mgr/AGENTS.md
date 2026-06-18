# src/mgr/ — Manager Daemon

## Purpose

The Manager daemon (ceph-mgr) hosts Python plugin modules that provide cluster monitoring, orchestration, CLI commands, and management features. The C++ code in this directory handles the daemon lifecycle, Python module loading, GIL management, and bridges between Python modules and the cluster's C++ infrastructure.

The actual module implementations (dashboard, cephadm, prometheus, pg_autoscaler, etc.) live in `src/pybind/mgr/` — see [../pybind/mgr/AGENTS.md](../pybind/mgr/AGENTS.md). This directory contains only the C++ hosting infrastructure.

## Key Files

| File | Role |
|------|------|
| `Mgr.h/cc` | Main active manager daemon class. |
| `MgrStandby.h/cc` | Standby manager — waits for promotion to active. |
| `DaemonServer.h/cc` | Handles connections from other daemons, routes commands, collects reports. |
| `ActivePyModules.h/cc` | Manages active Python module instances — dispatches notifications, health checks. |
| `StandbyPyModules.h/cc` | Manages Python modules while in standby mode. |
| `PyModule.h/cc` | Wrapper for a single Python module — loading, initialization. |
| `PyModuleRegistry.h/cc` | Discovery and loading of all Python modules from the filesystem. |
| `BaseMgrModule.h/cc` | C++ base class exposed to Python — defines the `ceph_module` C extension interface. |
| `BaseMgrStandbyModule.h/cc` | C++ base for standby-mode Python module interface. |
| `Gil.h/cc` | Python GIL management utilities — `SafeThreadState`, `GilGuard`. Critical for thread safety. |
| `MgrClient.h/cc` | Client-side MGR connection — used by other daemons to send perf stats and reports. |
| `ClusterState.h/cc` | Cached cluster state (OSDMap, health, PGMap) for Python modules. |
| `DaemonState.h/cc` | Per-daemon state tracking (perf counters, health). |
| `ServiceMap.h/cc` | Service discovery — tracks registered services (RGW, iSCSI, NFS, etc.). |
| `PyFormatter.h/cc` | Bridge between Ceph's Formatter classes and Python objects. |
| `PyOSDMap.h/cc` | OSDMap exposed to Python. |
| `MgrCommands.h` | Command definitions for the manager itself. |

## Patterns and Idioms

### GIL Management
The most critical pattern in this subsystem. C++ and Python code run on different threads, and the Python GIL must be managed carefully:

```cpp
// Acquire GIL before calling Python:
Gil gil(py_module->pMyThreadState);
// ... call Python ...
// GIL released when `gil` goes out of scope
```

Always acquire the GIL before calling into Python, release before calling into C++. See `Gil.h` for `SafeThreadState` and `GilGuard`.

### Module Commands
Python modules define commands via:
- `COMMANDS` class attribute (older pattern, list of command descriptors)
- `@CLICommand` decorator (newer pattern, annotated methods)

Commands are registered with the manager, which routes CLI requests to the appropriate module.

### Python Module Lifecycle
1. `PyModuleRegistry` scans `src/pybind/mgr/` for modules (directories with `module.py`)
2. `PyModule` wraps each module, loading its Python class
3. `ActivePyModules` instantiates and runs modules when the mgr is active
4. Modules receive notifications (map changes, daemon reports) via callbacks

## Dependencies

Uses `common/`, `msg/`, `mon/` (MonClient), `osd/OSDMap.h`, embedded CPython. Module code is in `src/pybind/mgr/`.

## Navigation Hints

- To understand how a CLI command reaches a Python module: `DaemonServer::handle_command()` → `ActivePyModules::handle_command()`
- To understand how Python modules access cluster state: see `BaseMgrModule.cc` for the `ceph_module` methods exposed to Python
- To add a new module: create a new directory under `src/pybind/mgr/` following the `hello/` template
- To understand perf counter collection: see `DaemonServer::handle_report()`

## Gotchas

- **GIL deadlocks** are a real risk. The most common pattern: holding a C++ lock, acquiring GIL, calling Python code that tries to call back into C++ and acquire the same lock.
- Manager has active/standby HA. Only the active manager runs Python modules at full functionality.
- `Py*` wrappers expose C++ objects to Python with manual reference counting — be careful with ownership.
