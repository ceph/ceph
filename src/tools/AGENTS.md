# src/tools/ — CLI Tools

## Purpose

This directory contains the source code for Ceph's command-line tools — used for cluster administration, debugging, and offline manipulation of data stores. Some tools operate on a running cluster; others operate directly on offline data structures.

## Key Files

### Map and Configuration Tools
| File | Tool Binary | Purpose |
|------|------------|---------|
| `crushtool.cc` | `crushtool` | Compile, decompile, test, and simulate CRUSH maps |
| `monmaptool.cc` | `monmaptool` | Create and edit monitor maps |
| `osdmaptool.cc` | `osdmaptool` | Create, edit, and test OSD maps |
| `ceph_conf.cc` | `ceph-conf` | Query ceph.conf configuration values |
| `ceph_authtool.cc` | `ceph-authtool` | Create and manage CephX authentication keys |

### Offline Store Tools
| File | Tool Binary | Purpose |
|------|------------|---------|
| `ceph_objectstore_tool.cc/h` | `ceph-objectstore-tool` | Offline ObjectStore manipulation — import/export objects, repair metadata, list PGs |
| `ceph_monstore_tool.cc` | `ceph-monstore-tool` | Offline monitor store manipulation — inspect and modify monitor database |
| `ceph_kvstore_tool.cc` | `ceph-kvstore-tool` | RocksDB key-value store inspection and manipulation |
| `kvstore_tool.h/cc` | (library) | Shared KV store tool logic |

### Recovery and Repair
| File | Tool Binary | Purpose |
|------|------------|---------|
| `rebuild_mondb.h/cc` | (library) | Monitor database reconstruction from OSD data |
| `RadosDump.h/cc` | (library) | RADOS object dumping utilities |

### Client CLI Tools (subdirectories)
| Subdirectory | Tool Binary | Purpose |
|---|---|---|
| `rados/` | `rados` | RADOS object storage CLI — put/get/bench/pool management |
| `rbd/` | `rbd` | RBD image management CLI — create/map/snap/mirror |
| `cephfs/` | `cephfs-top`, `cephfs-mirror` | CephFS monitoring and mirroring tools |
| `rbd_mirror/` | `rbd-mirror` | RBD mirror daemon — replicates images between clusters |
| `rbd_nbd/` | `rbd-nbd` | Map RBD images as NBD devices |
| `rbd_wnbd/` | `rbd-wnbd` | Map RBD images as Windows block devices |
| `immutable_object_cache/` | `ceph-immutable-object-cache` | Shared read-only cache daemon |
| `ceph-dencoder/` | `ceph-dencoder` | Encoding/decoding test tool — roundtrip test all serializable types |
| `neorados/` | `neorados` | New RADOS CLI using the neorados API |

### Other Tools
| File | Tool Binary | Purpose |
|------|------------|---------|
| `psim.cc` | `psim` | Placement group simulator — test PG distribution |
| `scratchtoolpp.cc` | `scratchtoolpp` | RADOS test/scratch tool |

## Navigation Hints

- To work on the `rados` CLI: see `rados/rados.cc`
- To work on the `rbd` CLI: see `rbd/rbd.cc`
- To understand offline OSD repair: see `ceph_objectstore_tool.cc`
- To test encoding compatibility: use `ceph-dencoder` (source in `ceph-dencoder/`)
- To simulate CRUSH placement: use `crushtool --test`

## Gotchas

- `ceph_objectstore_tool` directly opens the OSD's ObjectStore. The OSD must be stopped first — running it on a live OSD will corrupt data.
- `ceph-dencoder` is used in encoding compatibility tests. Adding a new encoded type requires registering it with the dencoder.
- The `rados` and `rbd` CLI tools have many subcommands implemented across multiple source files in their subdirectories.
- The RBD mirror daemon (`rbd_mirror/`) is a long-running daemon, not just a CLI tool, despite living in `tools/`.
