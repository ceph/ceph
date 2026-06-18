# src/cls/ — RADOS Classes

## Purpose

RADOS classes (cls) are **server-side plugins** that execute atomically on OSD nodes alongside RADOS operations. Each class provides domain-specific operations that run close to the data — avoiding round-trips between client and OSD. For example, `cls_rbd` manages RBD image metadata, `cls_rgw` manages bucket indices, and `cls_lock` provides distributed locking.

Classes register methods via the `objclass` API (`src/objclass/objclass.h`). Server-side code operates only on the local object — it cannot access the network or other objects.

## Key Files

Each class follows the same directory structure:
- `cls_<name>.cc` — server-side implementation (runs on OSD)
- `cls_<name>_client.cc/h` — client-side helper library (used by librados callers)
- `cls_<name>_types.h/cc` — shared type definitions
- `cls_<name>_ops.h` — operation definitions

The `objclass` API is defined in:
- `src/objclass/objclass.h` — stable C API for writing class methods

## Directory Structure (Class Implementations)

| Subdirectory | Purpose | Used By |
|---|---|---|
| `rbd/` | RBD image metadata (headers, snapshots, object map, mirroring) | librbd |
| `rgw/` | RGW bucket index operations | RGW |
| `rgw_gc/` | RGW garbage collection | RGW |
| `lock/` | Distributed object locking | librbd, RGW |
| `log/` | Append-only log on objects | RGW (metadata log) |
| `journal/` | Journaling support | librbd mirroring |
| `refcount/` | Reference counting on objects | RGW |
| `queue/` | Object-based queue | RGW notifications |
| `2pc_queue/` | Two-phase commit queue | RGW notifications |
| `fifo/` | FIFO queue on objects | RGW |
| `cas/` | Content-addressable storage (chunk dedup) | Dedup feature |
| `cmpomap/` | Compare omap values atomically | Various |
| `numops/` | Numeric operations on object data | Various |
| `otp/` | One-time password | RGW MFA |
| `user/` | User metadata storage | RGW |
| `version/` | Object versioning | Various |
| `sem_set/` | Semaphore set | Various |
| `cephfs/` | CephFS-specific operations | MDS |
| `lua/` | Lua scripting engine | User-defined classes |
| `hello/` | **Example/template class** — copy this to create new classes | Documentation |
| `sdk/` | SDK examples | Documentation |
| `timeindex/` | Time-indexed operations | Various |

## Patterns and Idioms

### Writing a Class
Copy `hello/` as a template. Server-side methods receive `cls_method_context_t hctx` and use `cls_cxx_*` functions (`cls_cxx_read`, `cls_cxx_write`, `cls_cxx_map_get_val`, etc.) to operate on the local object. Methods are registered in `__cls_init()` via `cls_register_cxx_method()`. Client-side helpers wrap `ioctx.exec()` calls. See `src/objclass/objclass.h` for the full server-side API.

## Dependencies

API defined in `src/objclass/objclass.h`. Client helpers use `librados`. Classes loaded by OSD via `src/osd/ClassHandler.h`.

## Navigation Hints

- To create a new RADOS class, copy the `hello/` directory structure
- To understand how a specific cls method works, read the server-side `.cc` file
- To call a cls method from client code, use the `cls_<name>_client.h` helpers
- To understand how the OSD loads and executes classes, see `src/osd/ClassHandler.h/cc`

## Gotchas

- Server-side code **cannot access the network**. It can only read/write the local object via `cls_cxx_*` functions.
- Server-side code runs atomically — the entire method completes or fails as one operation.
- The client-side helpers and server-side implementation must use the same encoding format. Keep type definitions in the shared `_types.h` file.
- `cls_rbd` is the most complex class — it manages the full RBD image metadata schema. Changes to it affect RBD format compatibility.
- Classes are loaded as shared libraries by the OSD. Build system registration is in `src/cls/CMakeLists.txt`.
