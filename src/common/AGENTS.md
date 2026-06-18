# src/common/ — Shared Utilities

## Purpose

The `common/` directory is the shared utility library used by every Ceph subsystem. It contains ~330+ files providing core infrastructure: configuration management, threading primitives, serialization helpers, data structures, logging, performance counters, cryptography, and more.

Almost every `.cc` file in Ceph includes something from `common/`. When looking for a utility function, check here first before writing a new one.

## Key Files

### Configuration
| File | Role |
|------|------|
| `options/` | YAML config option definitions (`osd.yaml.in`, `rgw.yaml.in`, etc.) |
| `options/build_options.cc` | Compiled config option registry |
| `config.cc/h` | Config parsing and management |
| `ceph_context.cc/h` | `CephContext` — the global context object holding config, logging, perf counters |
| `config_cacher.h` | Thread-safe config value caching |

### Threading and Synchronization
| File | Role |
|------|------|
| `ceph_mutex.h` | Mutex wrappers with optional lockdep checking in debug builds |
| `WorkQueue.h` | Thread pool work queue (ThreadPool + WorkQueue) |
| `Finisher.h/cc` | Callback completion queue — runs `Context` callbacks in a dedicated thread |
| `Timer.h/cc` | `SafeTimer` — schedule callbacks after a delay |
| `AsyncReserver.h` | Async reservation system (used for backfill/recovery slot management) |
| `Throttle.h/cc` | Rate limiting / backpressure (token bucket) |
| `Cond.h` | Condition variable wrapper |
| `RWLock.h` | Reader-writer lock wrapper |

### Data Structures
| File | Role |
|------|------|
| `hobject.h/cc` | `hobject_t` and `ghobject_t` — hash-based object identifiers |
| `Formatter.h/cc` | Output formatting base class |
| `JSONFormatter.h` | JSON output formatting |
| `XMLFormatter.h` | XML output formatting |
| `HTMLFormatter.h` | HTML output formatting |
| `TableFormatter.h` | Table output formatting |
| `PrioritizedQueue.h` | Priority queue with strict and proportional queues |
| `PriorityCache.h` | Priority-based cache manager |
| `cohort_lru.h` | Thread-safe LRU cache |
| `RefCountedObj.h` | Intrusive reference-counted base class |

### Logging
| File | Role |
|------|------|
| `dout.h` | `dout(level)` / `dendl` macros |
| `subsys.h` | Logging subsystem definitions (ceph_subsys_osd, etc.) |
| `LogClient.h/cc` | Centralized log message forwarding to MON |
| `LogEntry.h` | Log entry structure |

### Performance
| File | Role |
|------|------|
| `perf_counters.h/cc` | Performance counter infrastructure (counters viewable via admin socket) |
| `TrackedOp.h/cc` | Operation tracking with slow-op warnings |
| `HeartbeatMap.h/cc` | Thread heartbeat monitoring |
| `admin_socket.h/cc` | Unix socket for runtime introspection |

### Crypto and Checksums
| File | Role |
|------|------|
| `ceph_crypto.h/cc` | Cryptographic function wrappers |
| `Checksummer.h` | Checksum computation (crc32c, xxhash) |
| `CDC.h` / `FastCDC.h` / `FixedCDC.h` | Content-defined chunking for deduplication |

### Other Utilities
| File | Role |
|------|------|
| `ceph_argparse.h/cc` | Command-line argument parsing |
| `common_init.h/cc` | Daemon initialization sequence |
| `BackTrace.h/cc` | Stack trace capture |
| `SubProcess.h/cc` | Child process management |
| `armor.h/cc` | Base64 encoding/decoding |
| `blkdev.h/cc` | Block device utility functions |
| `PluginRegistry.h/cc` | Generic plugin loading framework |
| `EventTrace.h/cc` | LTTng/USDT tracing |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `options/` | YAML config option definitions and build scripts |
| `async/` | Async completion utilities |
| `io_exerciser/` | I/O pattern testing utilities |
| `json/` | JSON parsing helpers |

## Patterns and Idioms

### CephContext
Every daemon and library creates a `CephContext` (usually abbreviated `cct`). It holds configuration, the logging subsystem, and perf counters. Almost every class receives `cct` as a constructor parameter. Access config values via `cct->_conf->get_val<T>("name")` or the config observer pattern.

### Finisher Pattern
Use `Finisher::queue(Context*)` when a callback needs to run on a dedicated thread rather than the caller's.

### Mutex Guidelines
Use `ceph::mutex` / `ceph::make_mutex("name")` instead of `std::mutex` — enables lockdep checking in debug builds.

### TrackedOp
Long-running operations should inherit from `TrackedOp` for automatic slow-op warnings.

## Dependencies

Foundational library — depends on `include/`, `log/`, and external libraries (boost, fmt, openssl). Everything else depends on `common/`.

## Navigation Hints

- Need to add a config option? Edit the appropriate YAML file in `options/` (e.g., `osd.yaml.in` for OSD options)
- Need a thread pool? Use `WorkQueue.h` (or `ShardedThreadPool` for sharded work)
- Need to format output (JSON/XML/table)? Use `Formatter.h` and its subclasses
- Need rate limiting? Use `Throttle.h`
- Need an operation timer? Use `TrackedOp.h`
- Need a perf counter? See `perf_counters.h` and look at existing examples

## Gotchas

- `hobject_t` hash ordering is critical for PG splitting and CRUSH. Never change hash functions.
- `bufferlist` is defined in `include/buffer.h`, not in `common/`. But `common/` has the buffer instrumentation.
- Not everything in `common/` is widely used. Some utilities are only used by one or two subsystems.
- `Finisher` callbacks run on the Finisher's thread. If your callback acquires locks, beware of deadlocks with the caller.
- `SafeTimer` callbacks run with the timer's lock held. Keep them short or dispatch to a Finisher.
