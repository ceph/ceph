# src/mgr/ — Manager Daemon

The C++ code here is the **hosting infrastructure only**. Python module implementations (dashboard, cephadm, prometheus, etc.) live in `src/pybind/mgr/`.

**GIL management** is the most critical pattern. Acquire the GIL before calling into Python, release before calling into C++. See `Gil.h` for `SafeThreadState` and `GilGuard`. **GIL deadlocks** are a real risk: the common pattern is holding a C++ lock, acquiring GIL, then calling Python code that tries to call back into C++ and re-acquire the same lock.

Manager has active/standby HA. Only the active manager runs Python modules at full functionality.
