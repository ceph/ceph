# src/librbd/ — RBD Client Library

## Purpose

The `librbd` library provides RADOS Block Device (RBD) functionality — virtual block devices stored as striped objects in RADOS. It supports snapshots, cloning, journaling (for mirroring), encryption, image groups, live migration, and client-side caching.

The library uses an **async request pattern** where operations are `AsyncRequest` subclasses with internal state machines. The central object is `ImageCtx`, which holds all state for an open RBD image.

## Key Files

| File | Role |
|------|------|
| `ImageCtx.h/cc` | Central context object for an open RBD image. Holds pointers to all subsystems. |
| `internal.h/cc` | Core operations: create, open, close, resize, snap_create, flatten, etc. |
| `Operations.h/cc` | High-level image operation wrappers. |
| `ImageState.h/cc` | Image state machine (open, close, refresh). |
| `ImageWatcher.h/cc` | Watch/notify for image-level events (resize, snap, lock changes). |
| `ExclusiveLock.h/cc` | Exclusive lock management — ensures single-writer semantics. |
| `Journal.h/cc` | Image journal — records all mutations for mirroring. |
| `MirroringWatcher.h/cc` | Mirroring state change notifications. |
| `ObjectMap.h/cc` | Object existence bitmap — tracks which RADOS objects exist for fast reads. |
| `Types.h/cc` | RBD type definitions. |
| `Utils.h/cc` | Utility functions. |
| `AsioEngine.h/cc` | Boost.Asio based executor for async operations. |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `io/` | I/O path — read, write, discard, flush, copy-on-write |
| `operation/` | Image-level operations (resize, snap_create, flatten, rename) |
| `image/` | Image lifecycle management (open, close, refresh) |
| `mirror/` | Mirroring operations (enable, disable, promote, demote) |
| `cache/` | Client-side caching (including persistent writeback cache) |
| `crypto/` | Image encryption (LUKS1, LUKS2) |
| `journal/` | Journal implementation for mirroring |
| `object_map/` | Object existence map operations |
| `managed_lock/` | Distributed lock management internals |
| `exclusive_lock/` | Exclusive lock state machine |
| `deep_copy/` | Image deep copy operations |
| `migration/` | Live image migration |
| `trash/` | Trash (deferred deletion) management |
| `watcher/` | Watch/notify base framework |
| `group/` | Image group (consistency group) operations |
| `api/` | Public API implementation |
| `plugin/` | Plugin framework (e.g., cache plugins) |

## Patterns and Idioms

### Async Request Pattern
Most operations are implemented as `AsyncRequest` subclasses with state machines:
```cpp
class MyRequest : public AsyncRequest<ImageCtx> {
  void send() override;      // Start the operation
  // Internal states call each other via Context callbacks
};
```

### Context Callbacks
All async operations use `Context` callbacks for completion. State transitions within a request chain contexts together.

### ImageCtx
The central hub — holds configuration, I/O engine, journal, lock state, object map, and caching layer. Almost every class in librbd receives an `ImageCtx*`.

### Object Striping
RBD images are striped across multiple RADOS objects. Each object holds `object_size` bytes (default 4MB). Object names follow the pattern `rbd_data.<image_id>.<object_number>`.

## Dependencies

Uses `librados/` for RADOS I/O, `common/`, `cls/rbd/` (metadata), `cls/lock/` (locking), `journal/` (mirroring). Public API in `include/rbd/`.

## Navigation Hints

- To understand the read/write path: see `io/ImageDispatchSpec.h` → `io/ObjectDispatch.h` → `io/ObjectRequest.h`
- To understand snapshotting: see `operation/SnapshotCreateRequest.h`
- To understand cloning (copy-on-write): see `io/CopyupRequest.h`
- To understand mirroring: see `mirror/` and `journal/`
- To add a new image operation: create an `AsyncRequest` subclass in `operation/`

## Gotchas

- The async request pattern means operations span many callbacks. Tracing a single operation requires following the chain of `Context::complete()` calls through multiple states.
- `ImageCtx` is not thread-safe by itself. Access is coordinated through the exclusive lock and image-level locks.
- RBD uses `cls_rbd` (server-side class) heavily for metadata operations. Client-side helpers are in `cls/rbd/cls_rbd_client.h`.
- The journal is optional — it's only enabled when mirroring is active. But when enabled, every write goes through the journal.
- Object map is also optional. When enabled, it must be kept in sync with actual object existence.
