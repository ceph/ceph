# src/common/ — Shared Utilities

Core library used by every subsystem (~330 files): config, threading, logging, perf counters, data structures.

Config options defined in `options/*.yaml.in`. `CephContext` (`cct`) is the global context object (config, logging, perf counters) — most classes receive it as a constructor param.

Use `ceph::mutex`/`ceph::make_mutex()` instead of `std::mutex` for lockdep in debug builds. `bufferlist` is in `include/buffer.h`, not here.
