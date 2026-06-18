# src/librados/ — RADOS Client Library

## Purpose

`librados` provides the C and C++ client API for RADOS object storage. Applications use it to create pools, read/write objects, manage snapshots, and perform atomic operations. Internally, it wraps the **Objecter** (`src/osdc/Objecter.h`), which handles the actual OSD communication, OSDMap management, and request routing.

This library is used by RGW, RBD, CephFS clients, and any external application that needs direct RADOS access.

## Key Files

| File | Role |
|------|------|
| `librados_cxx.cc` | C++ API implementation (`librados::Rados`, `librados::IoCtx`). |
| `librados_c.cc` | C API implementation (`rados_t`, `rados_ioctx_t`). |
| `RadosClient.h/cc` | Internal client implementation — manages connections, MonClient, Objecter. |
| `IoCtxImpl.h/cc` | I/O context (pool handle) implementation — per-pool state and operations. |
| `AioCompletionImpl.h/cc` | Async I/O completion implementation. |
| `ObjectOperationImpl.h/cc` | Object operation builder implementation. |
| `ListObjectImpl.h/cc` | Object listing/enumeration implementation. |
| `librados_util.h/cc` | Internal utility functions. |
| `snap_set_diff.h/cc` | Snapshot set difference calculation. |

### Related Files (outside this directory)
| File | Role |
|------|------|
| `osdc/Objecter.h/cc` | The RADOS request engine — maps operations to OSDs, handles retries, manages OSDMap. |
| `neorados/RADOS.hpp` | New async RADOS API using boost::asio (modern replacement). |
| `include/rados/librados.h` | Public C API header. |
| `include/rados/librados.hpp` | Public C++ API header. |

## Patterns and Idioms

### Core Patterns
- **Sync**: `ioctx.write("obj", bl, len, offset)`
- **Async**: create `AioCompletion`, call `ioctx.aio_write(...)`, wait/poll
- **Compound ops**: batch multiple operations atomically via `ObjectWriteOperation`/`ObjectReadOperation`
- **Lifecycle**: `Rados::init()` → `connect()` → `ioctx_create(pool)` → operations → cleanup

## Dependencies

Core dependency is `osdc/Objecter.h` (the actual request engine). Also uses `mon/MonClient.h`, `msg/`, `common/`. Public API in `include/rados/`.

## Navigation Hints

- To understand how a RADOS operation reaches an OSD: `IoCtxImpl::operate()` → `Objecter::mutate()` → `Objecter::_op_submit()` → sends `MOSDOp` to the primary OSD
- To add a new RADOS operation: add the op constant to `include/rados.h`, add builder method to the `ObjectOperation` class, implement in `OSD::do_osd_ops()`
- For the new async API, see `src/neorados/RADOS.hpp`

## Gotchas

- `librados` is the C/C++ wrapper. The actual networking and routing logic is in `osdc/Objecter`, not here.
- The C API (`librados.h`) and C++ API (`librados.hpp`) are separate wrappers around the same `RadosClient` implementation.
- `neorados` is the modern replacement API. New internal Ceph code should prefer it over the classic `librados` API.
- Pool handles (`IoCtx`) are not thread-safe. Create one per thread or protect with external synchronization.
- The Objecter automatically handles OSD failures and OSDMap changes (re-routing operations to new primaries).
