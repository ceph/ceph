# src/librbd/ — RBD Client Library

RBD images use an **async request + state machine pattern** — operations are `AsyncRequest` subclasses chaining `Context` callbacks. Tracing a single operation requires following `Context::complete()` calls through multiple states.

The journal is optional — only enabled when mirroring is active. When enabled, every write goes through the journal.

Object map is optional. When enabled, it must be kept in sync with actual object existence.

`ImageCtx` is not thread-safe by itself. Access is coordinated through the exclusive lock and image-level locks.

RBD uses `cls_rbd` heavily for metadata. Client-side helpers are in `cls/rbd/cls_rbd_client.h`.
