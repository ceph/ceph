# src/os/ — ObjectStore Abstraction

## Purpose

The `os/` directory defines the `ObjectStore` abstract interface — the contract for all on-disk object storage in Ceph. Every OSD stores its data through this interface. The production implementation is **BlueStore** (`bluestore/`); **MemStore** (`memstore/`) exists for testing.

The ObjectStore API provides atomic transactions, collection (PG) management, object read/write, extended attributes, and omap (key-value per object) operations.

## Key Files

| File | Role |
|------|------|
| `ObjectStore.h` | Abstract base class defining the full storage interface. This is the contract that all backends implement. |
| `Transaction.h` | `ObjectStore::Transaction` — atomic batch of mutations (write, setattr, omap_setkeys, clone, etc.) |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `bluestore/` | BlueStore — production backend, stores data directly on raw block devices. See [bluestore/AGENTS.md](bluestore/AGENTS.md) |
| `memstore/` | MemStore — in-memory backend for unit tests. `MemStore.h/cc`, `PageSet.h` |
| `fs/` | Filesystem metadata utilities |

## Patterns and Idioms

### Transaction Model
All storage mutations are batched into a `Transaction`:
```cpp
ObjectStore::Transaction t;
t.write(coll, oid, offset, length, bl);
t.setattr(coll, oid, "attr_name", val_bl);
t.omap_setkeys(coll, oid, keys);
store->queue_transaction(ch, std::move(t), on_applied, on_committed);
```

Transactions are atomic — either all operations apply or none do. The `on_committed` callback fires when the transaction is durable.

### Collections
A `coll_t` represents a collection, which maps 1:1 to a Placement Group. Objects belong to exactly one collection.

### Object Naming
Objects are identified by `ghobject_t` (which contains a `hobject_t` plus generation and shard_id for EC). The ObjectStore maps these to its internal storage format.

### Read Path
```cpp
store->read(ch, oid, offset, length, &bl);
store->getattr(ch, oid, "attr_name", &val_bl);
store->omap_get_values(ch, oid, keys, &out);
```

## Dependencies

Uses `include/`, `common/`. BlueStore additionally uses `kv/` (RocksDB) and `blk/` (block device).

## Navigation Hints

- To understand what operations a Transaction supports, read `Transaction.h`
- To understand how BlueStore implements storage, see [bluestore/AGENTS.md](bluestore/AGENTS.md)
- To understand how the OSD uses ObjectStore, see `src/osd/PrimaryLogPG.cc` (specifically `do_osd_ops()`)
- For testing, use `MemStore` — it requires no disk setup

## Gotchas

- `ObjectStore` implementations must guarantee transaction atomicity. This is non-trivial for BlueStore (which uses a WAL).
- The `ch` parameter (collection handle) must be obtained via `open_collection()` before use. It caches metadata.
- `queue_transaction` is asynchronous. The callbacks fire on a separate thread.
- There used to be a FileStore backend (deprecated, removed). Some old comments may reference it.
