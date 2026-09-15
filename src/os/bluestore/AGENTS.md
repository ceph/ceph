# src/os/bluestore/ — BlueStore

BlueStore stores data directly on raw block devices (no filesystem). Uses RocksDB via **BlueFS** for metadata.

Object model: `Onode` (metadata in RocksDB) → `Blob` (data chunk, possibly compressed) → `PExtent` (physical offset+length on disk).

`BlueStore.cc` is ~30K lines — search by function name. A bug in allocation or extent mapping can corrupt all data on the device.

**Deferred writes** handle small overwrites (writes to WAL first, applied later). Small and large writes have different code paths.

BlueFS and BlueStore share the same block device but manage different regions.
