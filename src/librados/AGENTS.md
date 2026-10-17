# src/librados/ — RADOS Client Library

`librados` wraps the **Objecter** (`src/osdc/Objecter.h`), which handles the actual OSD communication, OSDMap management, and request routing. The C and C++ APIs are separate wrappers around the same `RadosClient` implementation.

`neorados` is the modern replacement API. New internal Ceph code should prefer it over classic `librados`.

Pool handles (`IoCtx`) are not thread-safe. Create one per thread or protect with external synchronization.
