# src/crush/ — CRUSH Placement Algorithm

## Purpose

CRUSH (Controlled Replication Under Scalable Hashing) is Ceph's data placement algorithm. It deterministically maps objects to OSDs using a hierarchical cluster topology (racks, hosts, devices) and configurable placement rules. CRUSH avoids centralized lookup tables — any node can compute placement given the CRUSH map.

The core algorithm is implemented in C (`mapper.c`, `crush.c`, `hash.c`) for performance. A C++ wrapper (`CrushWrapper`) provides the high-level API used by the rest of Ceph.

## Key Files

| File | Role |
|------|------|
| `CrushWrapper.h/cc` | C++ wrapper around the C CRUSH code. Primary API for all CRUSH operations: rule creation, bucket management, placement computation. |
| `mapper.c/h` | Core CRUSH mapping algorithm in C — `crush_do_rule()` computes placement |
| `crush.c/h` | CRUSH data structures: buckets, rules, weights (C code) |
| `hash.c/h` | CRUSH hash functions (rjenkins) — deterministic and stable |
| `builder.c/h` | Programmatic CRUSH map construction (add buckets, items, adjust weights) |
| `CrushCompiler.h/cc` | Text-format CRUSH map compiler/decompiler (human-readable format) |
| `CrushTester.h/cc` | CRUSH map testing — simulate placement and measure distribution |
| `CrushTreeDumper.h` | Dump CRUSH hierarchy as tree for visualization |
| `CrushLocation.h/cc` | OSD location detection (reads `crush_location` from config or hardware) |
| `types.h` | CRUSH type definitions |
| `grammar.h` | Parser grammar for CRUSH map text format |

## Patterns and Idioms

### Placement Computation
```cpp
// Map object to OSDs:
vector<int> osds;
crush.do_rule(rule_id, x, osds, num_replicas, weights);
// x is typically pg_id.ps() (placement seed)
```

### CRUSH Rule Structure
Rules are sequences of steps:
1. `take(root)` — select a starting bucket
2. `chooseleaf_firstn(N, type)` — choose N leaves of the given type (e.g., host)
3. `emit` — output the selected devices

### Bucket Types
- `uniform` — all items same weight
- `list` — linked list (legacy)
- `tree` — balanced binary tree
- `straw` / `straw2` — straw-drawing (default, fairest distribution)

## Dependencies

Self-contained — depends only on `include/`. Used by `osd/`, `mon/`, `osdc/`, `crimson/`.

## Navigation Hints

- To understand how pools map to CRUSH rules, see `OSDMap::_pg_to_up_acting_osds()`
- To modify CRUSH placement behavior, start with `mapper.c:crush_do_rule()`
- To add a new bucket type, modify `crush.h` and `mapper.c`
- To test CRUSH distribution, use `crushtool --test` (source in `src/tools/crushtool.cc`)

## Gotchas

- **Hash stability is critical**: changing hash functions changes data placement across the entire cluster, causing massive data migration. Never modify `hash.c` without understanding the full impact.
- CRUSH rules are order-sensitive. The sequence of steps (take, choose, emit) defines behavior.
- The C code uses raw pointers and manual memory management. The C++ `CrushWrapper` handles ownership.
- `straw2` is the recommended bucket algorithm. `straw` (v1) has known fairness issues with weight changes.
- CRUSH weights are in units of 0x10000 (65536 = 1.0 TiB). Integer arithmetic, not floating point.
