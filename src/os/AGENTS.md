# src/os/ — ObjectStore Abstraction

`ObjectStore` is the abstract interface for all OSD on-disk storage. Production implementation is **BlueStore** (`bluestore/`); **MemStore** (`memstore/`) is for tests.

A `coll_t` maps 1:1 to a Placement Group. The `ch` (collection handle) must be obtained via `open_collection()` before use — it caches metadata.

`queue_transaction` is asynchronous. Callbacks fire on a separate thread. FileStore is removed — ignore any old references to it.
