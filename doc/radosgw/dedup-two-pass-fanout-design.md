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
Two-pass:     num_md5_shards <= B²           (balanced sqrt split)
  G = ceil(sqrt(num_md5_shards))             (number of groups)
  shards_per_group = ceil(N / G)             (shards per group)
  Both G and shards_per_group <= B
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

Groups use a **balanced sqrt split**: both the number of groups and the
number of shards per group are approximately sqrt(N), giving symmetric
buffer usage across both phases.

```
S = shards_per_group = ceil(N / G)
G = ceil(sqrt(N))

group 0:  shards [0      .. S-1]
group 1:  shards [S      .. 2S-1]
group 2:  shards [2S     .. 3S-1]
  ...
group G-1: shards [(G-1)*S .. num_md5_shards-1]   (last group may be smaller)

Both G and S are <= B (guaranteed since N <= B²).
```

### Phase 1: Coarse Ingress (BI -> CS slabs)

Each worker scans its assigned BI shard and fans out records into G
coarse-group slabs (CS prefix):

- Allocate **G output buffers** from raw_mem (G <= B, so fits)
- For each record:
  `md5_shard = md5_low % num_md5_shards`
  `group_id  = md5_shard / shards_per_group`
- Write record to `CS` slab for `group_id` via `get_coarse_slab_name(group_id)`
- After scanning all BI entries, flush all G buffers

### Phase 2: Fine Fan-Out (CS -> S slabs)

After phase 1 completes, the same worker iterates over each group and
re-fans its CS slabs into final per-shard S slabs:

For each group `g` in [0, G):
1. Allocate **S output buffers** for the shards in group g
   (buffer `i` maps to md5 shard `g*S + i`; last group may use fewer)
2. Read back CS slabs for (worker_id, group g) from RADOS
3. For each record:
   `md5_shard  = md5_low % num_md5_shards`
   `buffer_idx = md5_shard - g * shards_per_group`
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
