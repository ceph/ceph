# src/pybind/mgr/ — Python Manager Modules

50+ modules. Each is a directory with a `module.py` defining a class inheriting `MgrModule` (defined in `mgr_module.py`). Use `hello/` as a template.

The C++ hosting infrastructure is in `src/mgr/`.

## Key patterns

- **Continuous service**: implement `serve()` using `self.event.wait()` — never `time.sleep()` (not interruptible).
- **Commands**: use `@CLICommand` decorator (preferred over `COMMANDS` class attribute).
- **Config**: persists in the monitor store, visible cluster-wide.

**Dashboard** is a sub-project with its own Angular frontend and build system — treat it separately.
