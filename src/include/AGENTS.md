# src/include/ — Shared Header Files

## Purpose

The `include/` directory contains fundamental header files used across all Ceph subsystems. This is where core type definitions, the serialization framework, the `bufferlist` data container, callback infrastructure, protocol constants, and public API headers live.

When you see a type like `epoch_t`, `bufferlist`, `Context`, or `hobject_t` and need to understand it, look here first.

## Key Files

### Serialization
| File | Role |
|------|------|
| `encoding.h` | The `ENCODE_START`/`ENCODE_FINISH`/`DECODE_START`/`DECODE_FINISH` macros and `WRITE_CLASS_ENCODER`. This is the primary serialization framework. |
| `denc.h` | `DENC` — a newer, more efficient serialization framework. Types that use it get zero-copy decoding. Used for hot-path types. |

### Data Containers
| File | Role |
|------|------|
| `buffer.h` | `bufferlist`, `bufferptr`, `buffer::list::iterator` — the universal data container in Ceph |
| `buffer_fwd.h` | Forward declarations for buffer types |

### Callbacks
| File | Role |
|------|------|
| `Context.h` | `Context` base class (async callback), `C_SaferCond` (sync waiter), `LambdaContext`. Every async completion in Ceph uses this. |

### Type Definitions
| File | Role |
|------|------|
| `types.h` | Common typedefs: `epoch_t`, `version_t`, `tid_t`, `snapid_t`, `ceph_tid_t` |
| `ceph_fs.h` | On-wire protocol constants, `MSG_*` message type numbers, inode/dentry flags |
| `ceph_features.h` | Feature bit definitions for version negotiation between daemons |
| `CompatSet.h` | Feature compatibility sets for on-disk format versioning |
| `rados.h` | RADOS operation constants (`CEPH_OSD_OP_READ`, `CEPH_OSD_OP_WRITE`, etc.) |
| `ceph_hash.h` | Hash functions for object naming (rjenkins) |
| `msgr.h` | Messenger constants and address types |
| `neorados/RADOS.hpp` | New async RADOS API header |

### Data Structures
| File | Role |
|------|------|
| `interval_set.h` | Set of non-overlapping intervals — used for tracking extent ranges |
| `elist.h` | Embeddable (intrusive) doubly-linked list |
| `mempool.h` | Memory pool tracking — categorizes memory usage by subsystem |
| `compact_map.h` / `compact_set.h` | Space-efficient map/set for small collections |
| `btree_map.h` | B-tree based map (from `cpp-btree/`) |
| `filepath.h` | CephFS path representation |

### Public API Headers
| Subdirectory | Contents |
|---|---|
| `rados/` | `librados.h`, `librados.hpp` — public RADOS API |
| `rbd/` | `librbd.h`, `librbd.hpp` — public RBD API |
| `cephfs/` | `libcephfs.h`, `ceph_ll_client.h` — public CephFS API |

## Patterns and Idioms

### Encoding/Decoding
Every on-wire and on-disk structure uses versioned encoding defined in `encoding.h`:
```cpp
void encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);  // (current_version, compat_version, bufferlist)
  encode(field1, bl);
  encode(field2, bl);
  encode(field3, bl);      // added in v2
  ENCODE_FINISH(bl);
}
void decode(bufferlist::const_iterator& p) {
  DECODE_START(2, p);
  decode(field1, p);
  decode(field2, p);
  if (struct_v >= 2) {
    decode(field3, p);     // v2 addition — guarded for backwards compat
  }
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(MyType)
```
When adding a field, bump the version and guard the decode with `if (struct_v >= N)`. `denc.h` provides a newer zero-copy variant for performance-critical types — don't mix both for the same type.

### bufferlist
`bufferlist` is a scatter-gather list of memory buffers — the universal data container. Not contiguous: `bl.c_str()` may copy. Use iterators for decoding: `auto it = bl.cbegin(); decode(obj, it)`.

### Context Callbacks
`Context` (in `Context.h`) is the async callback base class. Use `LambdaContext` for lambdas, `C_SaferCond` for synchronous waiting.

## Dependencies

Most fundamental layer — depends only on external libraries (boost, fmt). Everything in `src/` depends on `include/`.

## Navigation Hints

- Need to find a message type constant? Search `ceph_fs.h` for `MSG_` or `msgr.h` for `CEPH_MSG_`
- Need to understand what RADOS ops exist? See `rados.h` for `CEPH_OSD_OP_*`
- Need to check feature compatibility? See `ceph_features.h`
- Need to understand the encode/decode version for a type? Find its `ENCODE_START` call

## Gotchas

- `encoding.h` vs `denc.h`: most types use `encoding.h`. `denc.h` is for performance-critical types that benefit from bounded-size encoding. Don't mix them for the same type.
- `bufferlist` is NOT a contiguous buffer. Operations that assume contiguity (e.g., casting to a char*) require `bl.c_str()` which may copy.
- Feature bits in `ceph_features.h` are a limited resource. New features should prefer capability negotiation over adding feature bits.
- `MSG_*` constants must be unique. When adding a new message type, choose the next available number.
