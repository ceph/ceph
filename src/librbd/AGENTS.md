# src/librbd/ — RBD Client Library

RBD images use an **async request + state machine pattern** — operations are `AsyncRequest` subclasses chaining `Context` callbacks. Tracing a single operation requires following `Context::complete()` calls through multiple states.

The journal is optional. Journal-based mirroring requires it; snapshot-based mirroring does not. Journaling can also be enabled independently of mirroring. When enabled, every write goes through the journal.

Object map is optional. When enabled, it must be kept in sync with actual object existence.

RBD uses `cls_rbd` heavily for metadata. Client-side helpers are in `cls/rbd/cls_rbd_client.h`.
