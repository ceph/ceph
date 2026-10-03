# src/cls/ — RADOS Classes

RADOS classes are **server-side plugins** that execute atomically on OSD nodes. Classes register methods via the `objclass` API (`src/objclass/objclass.h`). Server-side code operates only on the local object — it cannot access the network or other objects.

To create a new class, copy the `hello/` directory. Server-side methods receive `cls_method_context_t hctx` and use `cls_cxx_*` functions to operate on the local object. Methods are registered in `__cls_init()` via `cls_register_cxx_method()`.

The client-side helpers and server-side implementation must use the same encoding format. Keep type definitions in the shared `_types.h` file.

`cls_rbd` manages the full RBD image metadata schema — changes to it affect RBD format compatibility.
