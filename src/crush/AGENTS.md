# src/crush/ — CRUSH Placement Algorithm

CRUSH deterministically maps objects to OSDs using a hierarchical cluster topology and configurable placement rules. The core algorithm is in C (`mapper.c`) for performance; `CrushWrapper` is the C++ API.

**Hash stability is critical**: changing `hash.c` changes data placement cluster-wide, causing massive data migration.

CRUSH weights are integer units of 0x10000 (65536 = 1.0 TiB) — not floating point.

`straw2` is the recommended bucket algorithm. `straw` (v1) has known fairness issues with weight changes.
