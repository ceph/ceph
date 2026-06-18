# src/crimson/ — Next-Generation Async OSD (Seastar)

## Purpose

Crimson is the next-generation OSD implementation built on the **Seastar** asynchronous framework. It provides a run-to-completion, shard-per-core concurrency model — no mutexes, no thread pools, no blocking I/O. Each CPU core runs an independent reactor with its own event loop.

Crimson is under active development and is **not yet production-ready**. It shares some code with the classic OSD (PeeringState, PGLog, OSDMap, CRUSH) but reimplements the I/O path, networking, and storage layers using Seastar futures.

## Key Files

### OSD Core (`osd/`)
| File | Role |
|------|------|
| `osd/osd.h/cc` | Crimson OSD daemon — initialization, map handling, PG management. |
| `osd/main.cc` | Crimson OSD entry point (`crimson-osd` binary). |
| `osd/pg.h/cc` | Crimson PG — async PG implementation using futures. |
| `osd/pg_backend.h/cc` | Abstract backend for Crimson PG I/O. |
| `osd/replicated_backend.h/cc` | Replicated pool I/O using Seastar futures. |
| `osd/ec_backend.h/cc` | Erasure code backend for Crimson. |
| `osd/osd_operation.h/cc` | Operation tracking and scheduling for Crimson ops. |
| `osd/ops_executer.h/cc` | Executes RADOS operations within Crimson. |
| `osd/object_context.h/cc` | Object context management (replaces classic ObjectContext). |
| `osd/recovery_backend.h/cc` | Recovery implementation using futures. |

### Common Utilities (`common/`)
| File | Role |
|------|------|
| `common/errorator.h` | **Errorator** — type-safe error handling replacing exceptions. Core Crimson pattern. |
| `common/interruptible_future.h` | Interruptible futures — can be cancelled when PG interval changes. |
| `common/gated.h` | Gate for tracking outstanding operations. |
| `common/operation.h` | Operation tracking framework. |

### Storage (`os/`)
| File | Role |
|------|------|
| `os/seastore/` | **SeaStore** — new object store optimized for NVMe, fully async. |
| `os/alienstore/` | **AlienStore** — bridge to run classic BlueStore inside Crimson (wraps blocking calls). |
| `os/cyanstore/` | **CyanStore** — in-memory store for testing. |

### Networking (`net/`)
| File | Role |
|------|------|
| `net/Messenger.h` | Crimson Messenger built on Seastar networking. |
| `net/Connection.h` | Crimson async connection. |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `osd/` | Crimson OSD implementation (async PG, backends, operations) |
| `os/seastore/` | SeaStore — new async object store for NVMe |
| `os/alienstore/` | Bridge to classic BlueStore |
| `os/cyanstore/` | In-memory testing store |
| `common/` | Crimson-specific utilities (errorator, interruptible_future, etc.) |
| `net/` | Seastar networking layer |
| `mon/` | Crimson monitor client |
| `mgr/` | Crimson manager client |
| `auth/` | Crimson auth handlers |
| `admin/` | Admin socket for Crimson |
| `tools/` | Crimson-specific tools |

## Patterns and Idioms

### Errorator
Type-safe error handling that replaces exceptions (which are expensive in Seastar):
```cpp
using read_errorator = crimson::errorator<
  crimson::ct_error::enoent,
  crimson::ct_error::input_output_error>;

read_errorator::future<bufferlist> read(...);
```
The return type statically encodes which errors can occur. Callers must handle all error cases.

### Interruptible Futures
Most PG operations use `interruptible_future` instead of plain `seastar::future`. These can be interrupted when a PG interval change invalidates ongoing work:
```cpp
using interruptible_future = crimson::interruptible::interruptible_future<IOInterruptCondition>;
```

### Shared Code with Classic OSD
Crimson shares these components with the classic OSD:
- `PeeringState` — peering state machine
- `PGLog` — operation log
- `OSDMap` / `CRUSHWrapper` — placement
- `osd_types.h` — type definitions

But the I/O path, threading model, and storage layer are completely different.

### No Blocking Allowed
Crimson code must never block. No `std::mutex`, no `std::condition_variable`, no blocking I/O. Everything is expressed as `seastar::future<>` chains. AlienStore exists specifically to wrap classic BlueStore's blocking calls in a separate thread pool.

## Dependencies

Uses `src/seastar/` (vendored Seastar framework). Shares `osd/PeeringState.h`, `osd/osd_types.h`, `osd/OSDMap.h`, `crush/` with the classic OSD. Uses non-blocking parts of `common/` and `include/`.

## Navigation Hints

- To understand the Crimson I/O path: `osd/ops_executer.cc` → `pg_backend.cc` → `replicated_backend.cc`
- To understand SeaStore: start at `os/seastore/seastore.h`
- To understand errorator: read `common/errorator.h` — it's the foundation of all Crimson error handling
- To understand how classic code is bridged: see `os/alienstore/`

## Gotchas

- **Never add blocking code** to Crimson. No `std::mutex`, `std::condition_variable`, `sleep()`, blocking I/O, or standard library threading headers.
- `seastar::future<>` vs `crimson::interruptible_future<>` — most PG operations use the interruptible variant. Using the wrong one can cause hangs or missed interruptions.
- Crimson is not production-ready. Expect incomplete features and active churn.
- SeaStore is experimental. AlienStore (wrapping BlueStore) is the practical storage backend for Crimson testing.
- Errorator requires explicit error type listing at compile time. Adding a new error type to a function signature can cascade through many callers.
- Seastar's shard-per-core model means each core has its own copy of state. Cross-core communication uses `seastar::submit_to()`.
