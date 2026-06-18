# src/erasure-code/ — Erasure Coding Framework

## Purpose

This directory provides the pluggable erasure coding (EC) framework. It defines the `ErasureCodeInterface` that all EC plugins implement, plus a base class with shared utilities and a plugin loader. Multiple EC algorithms are available as subdirectory plugins, each optimized for different use cases.

Erasure coding splits data into `k` data chunks and generates `m` coding (parity) chunks. Any `k` of the `k+m` chunks can reconstruct the original data, providing storage efficiency compared to replication.

## Key Files

| File | Role |
|------|------|
| `ErasureCodeInterface.h` | Pure virtual interface — defines `encode()`, `decode()`, `minimum_to_decode()`, chunk mapping |
| `ErasureCode.h/cc` | Base class with shared utility methods (chunk size calculations, default implementations) |
| `ErasureCodePlugin.h/cc` | Plugin loading infrastructure (`ErasureCodePluginRegistry`) |

## Directory Structure (Plugins)

| Subdirectory | Algorithm | Best For |
|---|---|---|
| `jerasure/` | Reed-Solomon via Jerasure library | Default plugin, general purpose |
| `isa/` | Intel ISA-L optimized Reed-Solomon | Intel CPUs with SIMD (fastest) |
| `lrc/` | Locally Repairable Codes | Reducing recovery network traffic (adds local parity groups) |
| `clay/` | CLAY codes | Optimal repair bandwidth (minimum data transfer for single-chunk repair) |
| `shec/` | Shingled Erasure Code | Reduced parity overhead |
| `consistency/` | Consistency checking | Testing/verification |

## Patterns and Idioms

### Plugin Registration
Each plugin registers itself via `ErasureCodePluginRegistry`:
```cpp
ErasureCodePluginRegistry::instance().add("jerasure", new ErasureCodePluginJerasure());
```

### Core Interface
```cpp
// Encode: k data chunks → k+m chunks (data + coding)
int encode(const set<int> &want_to_encode,
           const bufferlist &in,
           map<int, bufferlist> *encoded);

// Decode: any k chunks → reconstruct missing chunks
int decode(const set<int> &want_to_read,
           const map<int, bufferlist> &chunks,
           map<int, bufferlist> *decoded);

// What chunks are needed to read a given set?
int minimum_to_decode(const set<int> &want_to_read,
                      const set<int> &available,
                      map<int, vector<pair<int,int>>> *minimum);
```

### Profile Configuration
EC plugins are configured via a profile (key-value map):
```
plugin=jerasure technique=reed_sol_van k=4 m=2
```

## OSD-Side EC Backends

This directory provides the codec plugins. The OSD-side I/O path that *uses* these codecs has two implementations (see `src/osd/AGENTS.md` for details):

- **FastEC** (`src/osd/ECBackend.cc`, `ECCommon.cc`) — the active development target. All new work should target FastEC unless explicitly told otherwise.
- **Classic/Legacy EC** (`src/osd/ECBackendL.cc`, `ECCommonL.cc`) — original implementation, kept for stability. Likely to be deleted. Do not add features here.

`src/osd/ECSwitch.h` selects between them at runtime based on pool configuration.

## Dependencies

Uses `include/`, `common/`. Used by `src/osd/ECBackend.cc` and `src/osd/ECCommon.cc`.

## Navigation Hints

- To understand how the OSD uses EC, see `src/osd/ECBackend.h` (FastEC) or `src/osd/ECBackendL.h` (legacy)
- To add a new EC plugin, create a new subdirectory following the `jerasure/` pattern
- To test EC behavior, use `ceph_erasure_code_benchmark` (in `src/test/erasure-code/`)
- EC pool configuration is managed by OSDMonitor — see `src/mon/OSDMonitor.cc`

## Gotchas

- EC chunk ordering matters. Chunk 0..k-1 are data, k..k+m-1 are coding. The mapping between chunks and OSD shards is defined by `shard_id_t`.
- The `minimum_to_decode()` method is critical for read optimization — it tells the OSD which shards to read from to minimize I/O.
- ISA-L (`isa/`) requires Intel CPUs. On non-Intel hardware, jerasure is used automatically.
- CLAY codes minimize repair bandwidth but have higher encode/decode CPU cost.
