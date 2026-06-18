# src/os/bluestore/ — BlueStore

## Purpose

BlueStore is Ceph's **production object store backend**. It stores data directly on raw block devices, bypassing the filesystem layer entirely. It uses **RocksDB** for metadata (via a thin filesystem called BlueFS), and pluggable allocators for managing free space on the block device.

The architecture layers: objects (onodes) → blobs → extents on disk. Each object has metadata (onode) stored in RocksDB, which points to blobs that reference disk extents where the actual data lives.

## Key Files

| File | Role |
|------|------|
| `BlueStore.h/cc` | Main implementation (~30K lines). Inherits from `ObjectStore`. Contains all CRUD operations, cache management, GC, and transaction handling. |
| `BlueFS.h/cc` | Minimal filesystem for RocksDB — manages WAL and SST files on raw block device. |
| `bluestore_types.h/cc` | Core on-disk types: `bluestore_onode_t`, `bluestore_blob_t`, `bluestore_extent_ref_map_t`, `bluestore_pextent_t`. |
| `bluefs_types.h/cc` | BlueFS on-disk format types. |
| `Allocator.h/cc` | Free-space allocator interface. |
| `AvlAllocator.h/cc` | AVL tree-based allocator. |
| `BitmapAllocator.h/cc` | Bitmap-based allocator. |
| `BtreeAllocator.h/cc` | B-tree allocator. |
| `Btree2Allocator.h/cc` | Second-generation B-tree allocator. |
| `HybridAllocator.h/cc` | Hybrid allocator (combines strategies). |
| `StupidAllocator.h/cc` | Simple first-fit allocator (legacy, still used). |
| `FreelistManager.h/cc` | Persistent free-space tracking (survives restarts). |
| `Writer.h/cc` | Write path optimization. |
| `Compression.h/cc` | Compression integration (selects and applies compressor). |
| `bluestore_tool.cc` | `ceph-bluestore-tool` — offline debugging and repair tool. |

## Patterns and Idioms

### Onode → Blob → Extent Hierarchy
```
Onode (per-object metadata in RocksDB)
  └── Blob (chunk of data, possibly compressed/checksummed)
       └── PExtent (physical extent on block device: offset + length)
```

- **Onode**: metadata for one object. Stored in RocksDB. Points to blobs.
- **Blob**: a contiguous chunk of object data. May be compressed, checksummed, shared (for clones).
- **PExtent** (`bluestore_pextent_t`): physical location on disk (offset + length).

### Write Strategies
- **Direct write**: for large writes — data written directly to allocated extents.
- **Deferred write**: for small overwrites — data written to WAL first, then applied later. Avoids read-modify-write of large blocks.

### Transaction Handling
1. Caller creates `ObjectStore::Transaction` with operations
2. BlueStore stages the transaction: allocates space, prepares RocksDB batch
3. Data written to block device
4. RocksDB metadata updated atomically
5. Completion callbacks fired

### Cache Management
BlueStore maintains two caches:
- **Onode cache**: recently accessed object metadata
- **Buffer cache**: recently read data blocks
Both are size-limited and configurable via `bluestore_cache_size`.

## Dependencies

Implements `os/ObjectStore.h`. Uses `kv/` (RocksDB), `blk/` (block devices), `common/`, `include/`.

## Navigation Hints

- To understand how a write is handled: search for `BlueStore::_do_write()` in `BlueStore.cc`
- To understand how data is read: search for `BlueStore::read()` in `BlueStore.cc`
- To understand allocator behavior: read the allocator you're interested in (default is usually `BitmapAllocator` or `StupidAllocator`)
- To understand BlueFS (RocksDB filesystem): `BlueFS.h/cc`
- To debug BlueStore issues: use `ceph-bluestore-tool` (source in `bluestore_tool.cc`)
- For offline fsck: `ceph-bluestore-tool fsck`

## Gotchas

- `BlueStore.cc` is ~30K lines — one of the largest files in Ceph. Search by function name rather than reading linearly.
- BlueStore writes directly to raw block devices. A bug in allocation or extent mapping can corrupt all data on the device.
- The deferred write mechanism adds complexity to the write path. Small writes have a different code path from large writes.
- BlueFS and BlueStore share the same block device but manage different regions. Understanding the boundary is important for space management.
- Allocator choice affects performance characteristics. Different allocators have different fragmentation and speed trade-offs.
- The onode/blob/extent model means that reading a single byte of object data may require: RocksDB lookup → extent map resolution → block device read → decompression. Understanding this chain is necessary for performance work.
