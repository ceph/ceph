# RGW Dedup: Two-Pass Fan-Out Ingress Design

## Problem

The current ingress design allocates one 2 MB output buffer per MD5 shard
simultaneously. Memory scales linearly with shard count:

| MD5 Shards | Ingress Memory |
|------------|----------------|
| 512        | 1 GB           |
| 2,048      | 4 GB           |
| 4,095      | ~8 GB          |

This caps practical scalability. Supporting trillions of objects requires
thousands of shards, which requires tens of gigabytes just for ingress
buffers — even though only a small fraction are active at any moment.

## Core Idea

Replace the single-pass fan-out with a **two-pass external fan-out**. Instead
of opening all N shard buffers at once, open only B buffers at a time and
perform 2 passes through the data.

- **B** = `allocation_size / PER_SHARD_BUFFER_SIZE` (concurrent output streams)
- **Single pass** when `num_md5_shards <= B` (small systems, no overhead)
- **Two passes** when `num_md5_shards > B`, covering up to B² shards

A single memory allocation is used for **both** ingress buffers and the dedup
hash table, repurposed between phases.

## Memory Allocation

A single buffer (`raw_mem`) is allocated once per dedup cycle. During
ingress, the buffer is partitioned into `B = allocation / 2 Mi` output
stream buffers. Afterwards the same buffer is repurposed as the per-shard
hash table.

The allocation size is **configuration-driven** via the YAML option
`rgw_dedup_min_mem_allocation_mb`. This sets the *minimum* allocation;
if the object count requires more shards than the minimum can support
(in the B² model), the system doubles the allocation until B² is sufficient,
up to the hard cap of 2048 MB.

### YAML Configuration

```yaml
rgw_dedup_min_mem_allocation_mb:
  type: uint
  default: 64        # 64 MB
  min: 8             # 8 MB
  max: 2048          # 2 Gi
  desc: >
    Minimum memory allocation (in MB) for dedup ingress buffers and hash table.
    The actual allocation may be higher if the object count requires more
    shards than B² can provide at this size. The system doubles the allocation
    until B² suffices or the 2048 MB cap is reached. Systems exceeding
    the cap are rejected.
```

### Allocation Logic

```
B = allocation / PER_SHARD_BUFFER_SIZE       (PER_SHARD_BUFFER_SIZE = 2 Mi)
Single-pass:  num_md5_shards <= B            (one buffer per shard)
Two-pass:     num_md5_shards <= B²           (B groups of B shards)
```

Starting from `rgw_dedup_min_mem_allocation_mb`:
1. Compute `B = alloc / 2 Mi`.
2. If `num_md5_shards <= B²` → use this allocation.
3. Otherwise, double the allocation and repeat.
4. If allocation exceeds 2048 MB → reject (system too large).

### Headroom

Object count headroom is **1.25x** (25%). Because memory is no longer
proportional to shard count, the system can afford more generous headroom
without a proportional memory penalty.

```cpp
obj_count = obj_count + (obj_count / 4);  // 1.25x headroom
```

### Scaling Table

Memory is the single allocation size. Table-Slots = allocation / 32
(hash table entries). `B` = allocation / 2 Mi (concurrent ingress
output buffers). All units use strict power-of-2 math
(K = 1024, M = 1 Mi, G = 1 Gi, T = 1 Ti).

| MB   | Slots | B    | obj-raw | B²   | obj-raw |
|------|-------|------|---------|------|---------|
| 8    | 256K  | 4    | 1M      | 16   | 4M      |
| 16   | 512K  | 8    | 4M      | 64   | 32M     |
| 32   | 1M    | 16   | 16M     | 256  | 256M    |
| 64   | 2M    | 32   | 64M     | 1K   | 2G      |
| 128  | 4M    | 64   | 256M    | 4K   | 16G     |
| 256  | 8M    | 128  | 1G      | 16K  | 128G    |
| 512  | 16M   | 256  | 4G      | 64K  | 1T      |
| 1024 | 32M   | 512  | 16G     | 256K | 8T      |
| 2048 | 64M   | 1024 | 64G     | 1M   | 64T     |

*Max Shards (B²) is capped at 1,048,575 (1M − 1) — the maximum MD5 shard ID
supported by the `%05X` OID format (`0xFFFFF`). The last row hits this
ceiling (1024² = 1M, clamped to 1M − 1). Table uses 2 Mi slabs
(`DISK_BLOCK_COUNT = 256`).

Below the shard ceiling, doubling memory yields **8x** object capacity (B
doubles → B² quadruples shards, and slots/shard doubles → 4× × 2× = 8×).
Above the ceiling, doubling memory yields only **2×** (slots/shard doubles).

### Hash Table Density

At the designed operating point (with 1.25x headroom), the hash table load
factor is:

```
load = raw_objects_per_shard / slots_per_shard
     = (raw_obj_count / num_md5_shards) / (raw_mem_size / 32)
```

With 1.25x headroom the worst-case load factor is ~80%, yielding ~5 average
probes per lookup with linear probing.

## Phases

### Single-Pass Flow (num_md5_shards <= B)
```
setup() → ingress(BI → S slabs) → work_shards_barrier → dedup → md5_shards_barrier → cleanup
```

### Two-Pass Flow (num_md5_shards > B)
```
setup() → per-worker { phase1(BI → CS) + phase2(CS → S) } → work_shards_barrier → dedup → md5_shards_barrier → cleanup
```

Each worker processes its assigned BI shard in two internal phases
without inter-worker coordination between phases. Only a single
`work_shards_barrier` is needed at the end of ingress.

**Crash recovery:** Follows the current model. The barrier detects
timed-out members and proceeds without them. Any incomplete coarse or
final slabs from crashed workers are simply missing data — the dedup
phase runs on whatever was successfully written. The dedup pool is
deleted at cycle end (`safe_pool_delete`), cleaning up all slab objects
(both `CS` and `S` prefixes).

When `num_md5_shards <= B`, the two-pass logic is skipped entirely — the
system routes records directly to final S slabs, identical to today's
single-pass behavior. Most small-to-medium systems will take this path.

For testing, set `rgw_dedup_min_mem_allocation_mb = 8` to force a small
allocation (B=4), which triggers the two-pass path even on small systems.

### Groups

Groups are **contiguous ranges** of B shards:

```
G = ceil(num_md5_shards / B)

group 0:  shards [0      .. B-1]
group 1:  shards [B      .. 2B-1]
group 2:  shards [2B     .. 3B-1]
  ...
group G-1: shards [(G-1)*B .. num_md5_shards-1]   (last group may be smaller)
```

### Phase 1: Coarse Ingress (BI -> CS slabs)

Each worker scans its assigned BI shard and fans out records into G
coarse-group slabs (CS prefix):

- Allocate **G output buffers** from raw_mem (G <= B, so fits)
- For each record:
  `md5_shard = md5_low % num_md5_shards`
  `group_id  = md5_shard / B`
- Write record to `CS` slab for `group_id` via `get_coarse_slab_name(group_id)`
- After scanning all BI entries, flush all G buffers

### Phase 2: Fine Fan-Out (CS -> S slabs)

After phase 1 completes, the same worker iterates over each group and
re-fans its CS slabs into final per-shard S slabs:

For each group `g` in [0, G):
1. Allocate **B output buffers** for the B md5 shards in group g
   (buffer `i` maps to md5 shard `g*B + i`; last group may use fewer)
2. Read back CS slabs for (worker_id, group g) from RADOS
3. For each record:
   `md5_shard  = md5_low % num_md5_shards`
   `buffer_idx = md5_shard - g * B`
4. Write record to S slab via buffer at `buffer_idx`
5. Flush output buffers (writes final S slabs)
6. Delete CS slabs for this group

Memory: reuses the same raw_mem allocation. Phase 2 processes one group
at a time, needing at most B output buffers (exactly the allocation size).

### Dedup Phase (unchanged)

The same `raw_mem` buffer is repurposed as the hash table. Each MD5 shard is
processed one at a time, loading its slabs into the hash table to find
duplicates.

## OID Format Changes

### Current Formats

| Object Type       | Format                        | Max           | Size  |
|-------------------|-------------------------------|---------------|-------|
| Slab              | `SLB.%03X.%02X.%04X`         | SLB.FFF.FF.FFFF | 16 B |
| MD5 shard token   | `MD5.SHRD.TK.` + `%03x`      | MD5.SHRD.TK.fff | 16 B |
| Worker shard token| `WRK.SHRD.TK.` + `%03x`      | WRK.SHRD.TK.fff | 16 B |

### New Formats

| Object Type        | Format                       | Max             | Size  |
|--------------------|------------------------------|-----------------|-------|
| Slab               | `S%05X.%02X.%05X`            | SFFFFF.FF.FFFFF | 16 B  |
| MD5 shard token    | `MD5.TK.` + `%05x`           | MD5.TK.fffff    | 13 B  |
| Worker shard token | `WRK.TK.` + `%05x`           | WRK.TK.fffff    | 13 B  |
| Coarse group slab  | `CS%03X.%02X.%06X`           | CSFFF.FF.FFFFFF | 16 B  |

All fit within the existing `BUFF_SIZE = 16` byte limit.

Field capacities (final slabs `S`):

| Field      | Hex Digits | Max Value             | Purpose                    |
|------------|------------|-----------------------|----------------------------|
| md5_shard  | 5          | 0xFFFFF = 1,048,575   | ~1M MD5 shards             |
| worker     | 2          | 0xFF = 255            | Matches MAX_WORK_SHARD     |
| slab_seq   | 5          | 0xFFFFF = 1,048,575   | ~1M slabs per (shard, worker) |

Field capacities (coarse slabs `CS`):

| Field      | Hex Digits | Max Value             | Purpose                    |
|------------|------------|-----------------------|----------------------------|
| group_id   | 3          | 0xFFF = 4,095         | B is at most 1,024         |
| worker     | 2          | 0xFF = 255            | Matches MAX_WORK_SHARD     |
| slab_seq   | 6          | 0xFFFFFF = 16,777,215 | Matches 24-bit slab_id in disk_rec_id_t |

### Coarse Group Slab Naming

Pass 1 writes coarse-group slabs using prefix `CS` instead of `S`:

```
CS{group_id:03X}.{worker:02X}.{slab_seq:06X}
```

The 6-hex `slab_seq` matches the 24-bit `slab_id` field in `disk_rec_id_t`
(max 16M), so the OID and the in-memory address have identical capacity.
These slabs are transient — deleted after pass 2 processes each group.

## disk_rec_id_t (replaces disk_block_id_t)

The hash table `value_t` is restructured. `disk_block_id_t` (32-bit, pointed
to a block) is replaced by `disk_rec_id_t` (48-bit, points directly to a
record on disk). The freed space comes from shrinking `count` from `uint16_t`
to `uint8_t` (capped at `MAX_COPIES_PER_OBJ = 128`, fits in 8 bits) and
absorbing `rec_id` into the address.

### Current layout: `value_t` (8 bytes)

```
  disk_block_id_t block_idx;   // 32 bits (uint32_t)
  uint16_t        count;       // 16 bits
  uint8_t         rec_id;      //  8 bits
  uint8_t         flags;       //  8 bits
```

`disk_block_id_t` (32 bits):
```
 31      24 23               8 7       0
+----------+------------------+----------+
| work_shard |    slab_id     | block_off |
|  8 bits    |   16 bits      |  8 bits   |
+----------+------------------+----------+
```

### New layout: `value_t` (8 bytes)

```
  disk_rec_id_t rec_addr;     // 48 bits (struct, 6 bytes)
  uint8_t       count;        //  8 bits (was uint16_t, 128 cap fits)
  uint8_t       flags;        //  8 bits (unchanged)
```

`disk_rec_id_t` (48 bits) -- packed bit-field struct, in-memory only:
```cpp
struct __attribute__((packed)) disk_rec_id_t {
    work_shard_t  work_shard : 8;   // byte 0
    uint32_t      slab_id    : 24;  // bytes 1-3
    uint16_t      block_id   : 9;   // byte 4 + 1 bit
    uint8_t       rec_id     : 6;   // 6 bits
    uint8_t       rsv        : 1;   // reserved
};
```

Constructors take `(work_shard, slab_id, block_id)` or
`(work_shard, slab_id, block_id, rec_id)`. Fields are accessed directly
as struct members. No endianness conversion needed.

| Field      | Bits | Max Value      | Purpose                                |
|------------|------|----------------|----------------------------------------|
| work_shard | 8    | 255            | Worker ID (unchanged)                  |
| slab_id    | 24   | 16,777,215     | Slab index within (shard, worker)      |
| block_id   | 9    | 511            | Block within slab (256 at 2MB, 512 at 4MB) |
| rec_id     | 6    | 63             | Record within block (cap: MAX_REC_IN_BLOCK = 32) |
| reserved   | 1    | —              | Future use                             |

`sizeof(table_entry_t)` remains **32 bytes** (24-byte key + 8-byte value).
No hash table density change.

### disk_block_header_t

The on-disk block header stores only a `uint16_t block_idx` (block index
within the slab, 0..DISK_BLOCK_COUNT-1) instead of the full
`disk_rec_id_t`. Saves 4 bytes per block header (6B -> 2B), since
worker and slab identity are already encoded in the slab's OID.

```cpp
struct __attribute__((packed)) disk_block_header_t {
    uint16_t  offset;                     // write cursor / magic
    uint16_t  rec_count;
    uint16_t  block_idx;                  // was: disk_rec_id_t block_id (6B -> 2B)
    uint16_t  rec_offsets[MAX_REC_IN_BLOCK];
};
```

`block_idx` is serialized with `HTOCEPH_16`/`CEPHTOH_16` (it IS on disk).

### disk_block_seq_t

The write-path state `d_seq_number` (a flat counter conflating slab
identity and block position) is split into two fields:

```cpp
uint32_t  d_slab_id   = 0;   // slab index, incremented by flush()
uint16_t  d_block_id  = 0;   // block index within slab, reset by slab_reset()
```

`store_slab()` and `load_slab()` take `slab_id` directly (not a composite
`seq_number`).

`add_record()` takes a `disk_rec_id_t*` out-param and constructs the full
address inline: `disk_rec_id_t(d_worker_id, d_slab_id, d_block_id, rec_id)`.
The old `record_info_t` wrapper struct was removed — `rec_id` is already
embedded in the `disk_rec_id_t` bit-field and does not need a separate field.

### Slab Size Options

The 9-bit `block_id` supports up to 512 blocks per slab, future-proofing
for 4 MB slabs. The current implementation uses 2 MB slabs (256 blocks,
8 of 9 bits used).

**2 MB slabs (current):** `DISK_BLOCK_COUNT = 256`

| Allocation | B | Max Shards (B²) | Slots/Shard | Max Raw Objects (÷1.25) |
|------------|---|------------------|-------------|-------------------------|
| 128 MB     | 64  | 4,096          | 4M          | 13.7G                   |
| 256 MB     | 128 | 16,384         | 8M          | 110G                    |
| 512 MB     | 256 | 65,536         | 16M         | 880G                    |
| 1,024 MB   | 512 | 262,144        | 33M         | 7.04T                   |

**4 MB slabs (future option):** `DISK_BLOCK_COUNT = 512`

| Allocation | B | Max Shards (B²) | Slots/Shard | Max Raw Objects (÷1.25) |
|------------|---|------------------|-------------|-------------------------|
| 128 MB     | 32  | 1,024          | 4M          | 3.4G                    |
| 256 MB     | 64  | 4,096          | 8M          | 27.5G                   |
| 512 MB     | 128 | 16,384         | 16M         | 220G                    |
| 1,024 MB   | 256 | 65,536         | 33M         | 1.76T                   |

4 MB slabs halve B (and quarter B²) at the same memory, but produce half as
many RADOS objects — fewer I/O round-trips. Better for very large systems
where I/O ops are the bottleneck. Switching is a single constant change
(`DISK_BLOCK_COUNT`); the `disk_rec_id_t` layout supports both.

## MAX_WORK_SHARD (unchanged at 255)

`MAX_WORK_SHARD` stays at **255**. The 8-bit `work_shard` field in
`disk_rec_id_t` supports 255 workers, which is sufficient — 255 RGW workers
can cover all BI shards (up to Ceph's 1,999 resharding cap) using modulo
arithmetic (`current_shard = worker_id; current_shard += num_work_shards`).

Growing beyond 255 workers would require widening `disk_rec_id_t`, adding
more token coordination overhead (write/read/compare-swap for each worker
token), with diminishing returns — each of the 255 workers already handles
~8 BI shards in the worst case.

```cpp
num_work_shards = std::min(num_md5_shards, MAX_WORK_SHARD);
// MAX_WORK_SHARD = 255
```

## Slab Capacity Verification

### Final slabs (`S%05X.%02X.%05X`)

The 5-hex `slab_seq` in the OID (max 1,048,575 = ~1M) is tighter than
the 24-bit `slab_id` in `disk_rec_id_t` (max 16M). Each (md5_shard,
worker) pair can store:

```
1,048,576 slabs × 256 blocks = 268M blocks  (2MB slabs)
1,048,576 slabs × 512 blocks = 537M blocks  (4MB slabs)
```

Worst realistic case: 1T objects, 8192 md5 shards, 255 workers:

```
per_pair = 1.25T / (8192 × 255) = 598,572 records
slabs needed = 598,572 / 256 = 2,339 slabs  (2MB)
slabs needed = 598,572 / 512 = 1,170 slabs  (4MB)
```

Well within 1M. Massive headroom for final per-shard slabs.

### Coarse slabs (`CS%03X.%02X.%06X`)

The 6-hex `slab_seq` (max 16,777,215 = 16M = 2^24) matches the 24-bit
`slab_id` in `disk_rec_id_t` exactly — no wasted bits, no mismatch.

Worst realistic case: 56T objects, B = 1024 groups, 255 workers:

```
per_pair = 1.25 × 56T / (1024 × 255) = 2^28 records
slabs needed = 2^28 / 256 = 2^20 = ~1M slabs  (2MB)
```

2^24 capacity / 2^20 needed = **16x headroom**. Even at the maximum
supported scale, the coarse slab_seq is never the bottleneck.

## Data Structure Changes Summary

### `rgw_dedup_utils.h`

```
MAX_WORK_SHARD:       255       (unchanged)
WORK_SHARD_HARD_LIMIT: 0xFF    (unchanged)
MAX_MD5_SHARD:        2048   → 8192  (or higher, up to 1M with %05X OIDs)
MD5_SHARD_HARD_LIMIT: 0xFFF  → 0xFFFFF
md5_shard_t:          uint16_t → uint32_t (for >65535 shards)
work_shard_t:         uint16_t (unchanged, 255 fits)
```

### `rgw_dedup_store.h`

```
disk_block_id_t (32 bits) → disk_rec_id_t (48 bits, packed bit-field struct):
  work_shard: 8 bits, slab_id: 24 bits, block_id: 9 bits,
  rec_id: 6 bits, reserved: 1 bit
  In-memory only, no endianness conversion. Direct field access.
  Constructors: (ws, slab_id, block_id) and (ws, slab_id, block_id, rec_id)

disk_block_header_t::block_id → block_idx:
  disk_rec_id_t (6B) → uint16_t (2B), saves 4 bytes per block header
  Serialized with HTOCEPH_16/CEPHTOH_16

disk_block_seq_t::d_seq_number → d_slab_id + d_block_id:
  uint32_t d_slab_id (slab index, incremented by flush)
  uint16_t d_block_id (block index within slab, reset by slab_reset)

disk_block_seq_t::add_record():
  record_info_t removed — out-param is now disk_rec_id_t* directly
  rec_id is embedded in disk_rec_id_t, no separate field needed

store_slab() / load_slab(): parameter seq_number → slab_id

table_entry_t::value_t:
  disk_rec_id_t rec_addr (6 bytes) + count (uint8_t) + flags (uint8_t)
  sizeof(value_t) = 8 bytes (unchanged)
  sizeof(table_entry_t) = 32 bytes (unchanged)

disk_block_seq_t::d_coarse:
  bool d_coarse = false;  // set by activate(coarse=true) for Phase 1
  flush() dispatches to store_coarse_slab() when d_coarse is true,
  store_slab() otherwise.

disk_block_array_t:
  d_disk_arr: std::vector<disk_block_seq_t>  (dynamically sized)
  mode_t enum: SINGLE_PASS, PHASE1_COARSE, PHASE2_FINE
  d_fan_out_B, d_group_id, d_mode: routing state

  Constructor (single-pass):
    disk_block_array_t(dpp, raw_mem, raw_mem_size, worker_id,
                       p_stats, num_md5_shards)
    num_buffers = num_md5_shards, route: md5_shard = md5_low % N

  Constructor (two-pass):
    disk_block_array_t(dpp, raw_mem, raw_mem_size, worker_id,
                       p_stats, num_md5_shards, fan_out_B, group_id, mode)
    Phase 1: num_buffers = G = ceil(N/B), route: idx = md5_shard / B
    Phase 2: num_buffers = B (or less for last group),
             route: idx = md5_shard - group_id * B

  get_shard_block_seq(md5_low):
    md5_shard = md5_low % num_md5_shards
    SINGLE_PASS:    idx = md5_shard
    PHASE1_COARSE:  idx = md5_shard / B
    PHASE2_FINE:    idx = md5_shard - group_id * B

store_coarse_slab() / load_coarse_slab(): (new)
  Same as store_slab() / load_slab() but use get_coarse_slab_name(group_id)
  for CS-prefix OIDs.

SLAB_NAME_FORMAT: "SLB.%03X.%02X.%04X" → "S%05X.%02X.%05X"
COARSE_SLAB_NAME_FORMAT: (new) "CS%03X.%02X.%06X"
```

### `rgw_dedup_cluster.h`

```
MD5_SHARD_PREFIX:    "MD5.SHRD.TK." → "MD5.TK."
WORKER_SHARD_PREFIX: "WRK.SHRD.TK." → "WRK.TK."
shard_token_oid::set_shard:
  parameter: uint16_t → uint32_t
  format:    "%03x"   → "%05x"

d_completed_md5[MAX_MD5_SHARD] → dynamically allocated (vector)
d_completed_workers[MAX_WORK_SHARD] → d_completed_workers[256] (unchanged)
d_num_completed_md5:  uint16_t → uint32_t

get_next_work_shard_token() / get_next_md5_shard_token():
  return type: work_shard_t / md5_shard_t → shard_t (uint32_t)
  sentinel:    NULL_WORK_SHARD / NULL_MD5_SHARD → NULL_SHARD (0xFFFFFFFF)
```

`all_shard_tokens_completed()` is already prefix-parameterized — it works
for `WRK.TK.` and `MD5.TK.` without code changes.

### `rgw_dedup.cc`

```
YAML config: rgw_dedup_min_mem_allocation_mb (min 8, default 64, max 2048)

calc_fan_out_params(obj_count, min_alloc_mb, &out):
  Unified function replaces calc_num_md5_shards() and calc_mem_allocation().
  1. Apply 1.25x headroom: obj_count += obj_count / 4
  2. Walk scaling table rows (8, 16, 32, ..., 2048 MB):
     - num_slots = alloc / 32, shards = ceil(obj_count / num_slots)
     - B = alloc / 2Mi
     - First row where shards <= B² → needed_alloc
  3. actual_alloc = max(needed_alloc, min_alloc_mb)
  4. Recompute shards, B with actual_alloc
  5. num_groups = 0 if shards <= B (single-pass), else ceil(shards / B)
  Returns -EOVERFLOW if no row fits (system too large).

Background::setup():
  - Reads d_min_mem_allocation_mb from config
  - Calls calc_fan_out_params() → d_raw_mem_size, d_fan_out_B, d_num_groups,
    num_md5_shards, num_work_shards

Background::f_ingress_work_shard():
  - Branches on d_num_groups:
    d_num_groups == 0 → objects_ingress_single_work_shard() (single-pass)
    d_num_groups >  0 → phase1_coarse_ingress() then phase2_fine_fan_out()

Background::phase1_coarse_ingress():
  - Same BI scan loop as objects_ingress_single_work_shard()
  - Uses disk_block_array_t in PHASE1_COARSE mode (G buffers)
  - Routes records by group_id = md5_shard / B

Background::phase2_fine_fan_out():
  - For each group g in [0, G):
    1. Create disk_block_array_t in PHASE2_FINE mode (B buffers)
    2. Read CS slabs via load_coarse_slab() for (worker_id, group g)
    3. Deserialize blocks/records, route to S slabs (buffer_idx = md5_shard - g*B)
    4. Flush S slabs, delete CS slabs for this group

Background::run():
  - Allocates d_raw_mem_size bytes (computed in setup)
  - Both paths use WRK.TK. tokens + work_shards_barrier
```

### `rgw_dedup_epoch.h`

No serialization format change needed. `num_md5_shards` is already `uint32_t`
in `dedup_epoch_t`.

## Example: 1 Trillion Objects

```
Raw objects:            1T
Headroom (1.25x):       1.25T
MD5 shards:             8192
Work shards:            min(8192, 255) = 255
Config min alloc:       64 MB (default)
Doubling: 64→128→256→512→1024 MB (B=512, B²=256K > 8192) → alloc = 1024 MB
B (output streams):     512
num_groups:             ceil(8192 / 512) = 16
Slots/shard:            32M
Per-worker:             phase 1: BI → CS slabs, phase 2: CS → S slabs
Peak memory:            1024 MB (single allocation, repurposed)
```

## Dynamic Array Allocation

All arrays currently sized by compile-time maximums (`MAX_MD5_SHARD`,
`MAX_WORK_SHARD`) must become dynamically allocated, sized to runtime values.
This prevents stack overflow for large configurations and avoids wasting
memory for small ones.

| Array | Current | New |
|-------|---------|-----|
| `d_completed_md5[MAX_MD5_SHARD]` | Fixed 4095 bytes on heap (in `cluster`) | `std::vector<uint8_t>(num_md5_shards)` |
| `d_completed_workers[MAX_WORK_SHARD]` | Fixed 255 bytes on heap | Fixed `d_completed_workers[256]` — unchanged |
| `d_disk_arr[MAX_MD5_SHARD]` | Fixed on stack in `disk_block_array_t` | `std::vector<disk_block_seq_t>(B)` where B = fan-out buffer count |

Benefits:
- Small system (4 shards, 4 workers): allocates 4+4 entries, not 1M+256
- Large system (8192 shards, 255 workers): allocates exactly what is needed
- Eliminates stack overflow risk from `disk_block_array_t` (was ~80 KB, could
  grow to megabytes with higher MAX values)
- `d_disk_arr` is sized to B (the fan-out buffer count), not num_md5_shards,
  since during ingress only B streams are open simultaneously

## Resolved Design Decisions

1. **Per-worker two-phase ingress (no GRP tokens)**: Each worker processes
   its assigned BI shard in two internal phases without inter-worker
   coordination between phases. Phase 1: BI → CS slabs (coarse ranges).
   Phase 2: load CS slabs by range and output into final S slabs. Only
   a single `work_shards_barrier` is needed at the end of ingress.

2. **Config-driven memory model**: The allocation starts from
   `rgw_dedup_min_mem_allocation_mb` (default 64 MB) and doubles until
   B² >= num_md5_shards. Systems exceeding 2048 MB are rejected.

3. **Routing formula**: Coarse routing uses contiguous ranges
   (`group_id = md5_shard / B`), mapping each group to a contiguous
   block of B shards. The last group may be smaller if N is not
   divisible by B. Fine routing within a group uses
   `buffer_idx = md5_shard - group_id * B`.

4. **Crash recovery**: No new phase tracking in `dedup_epoch_t`. Follows the
   current model: barriers detect timed-out/crashed members and proceed
   without them. Partial data from crashed workers is simply missing. The
   dedup pool is deleted on successful cycle end (`safe_pool_delete`),
   cleaning up all transient slabs.

5. **d_completed_md5**: Dynamically allocated as `std::vector<uint8_t>` at
   runtime, resolving the fixed-array issue for up to 1M MD5 shards.

6. **MAX_WORK_SHARD stays at 255**: 255 workers cover all BI shards (up to
   1,999) via modulo arithmetic. Growing beyond 255 would require widening
   `disk_rec_id_t` and adding token coordination overhead with diminishing
   returns. The 8-bit `work_shard` field is retained.

9. **disk_rec_id_t (replaces disk_block_id_t)**: Widened from 32 to 48 bits.
   Implemented as a packed bit-field struct (in-memory only, no endianness).
   `rec_id` absorbed into the address (was a separate `value_t` field).
   `count` shrunk from `uint16_t` to `uint8_t` (128 cap fits).
   `slab_id` widened from 16 to 24 bits (max 16M, eliminates overflow
   concern). `block_id` widened from 8 to 9 bits (supports future 4 MB
   slabs). Constructors take `(slab_id, block_id)` instead of composite
   `seq_number`. `sizeof(table_entry_t)` unchanged at 32 bytes.

10. **Coarse slab_seq overflow (resolved)**: The coarse slab format
    `CS%03X.%02X.%06X` uses 6 hex digits for `slab_seq` (max 16M = 2^24),
    matching the 24-bit `slab_id` in `disk_rec_id_t`. At the worst
    supported scale (56T objects, B = 1024, 255 workers), each
    (group, worker) pair needs ~2^20 slabs — well within 2^24 capacity
    (16x headroom). Final slabs use 5-hex `slab_seq` (max ~1M), also
    far above worst-case needs.

## Open Items

1. **Minimum allocation floor**: Set at 64 MB (32 buffers) or 128 MB (64
   buffers)? Lower floor means smaller systems use less memory but may need
   2 passes earlier.

2. **Testing**: Need integration tests for the two-pass path, verifying that
   records survive the coarse → fine fan-out without loss or duplication.
