# FDB Performance & Scale Testing

## Goal

Characterize the KV-based RGW model's IOPS, latency, and scaling behavior on FoundationDB. Compare against the RADOS model to understand limitations and advantages. Results must be extrapolatable to production sizing.

---

## Hardware Allocation (112 cores / 376 GB / 9 NVMe)

### Actual hardware

- **CPU:** 2x Intel Xeon Platinum 8276M (28 cores/socket, HT) = 112 logical CPUs
- **RAM:** 376 GB
- **NVMe:** 8x 7.3 TB INTEL SSDPE2KX080T8 + 1x 699 GB INTEL SSDPE21K750GA

### FDB drive allocation

| Role | Device | Partition | Size | Mount point |
|---|---|---|---|---|
| FDB storage server 0 | nvme4n1 | p1 | 7.3 TB | /mnt/fdb0 |
| FDB storage server 1 | nvme8n1 | p1 | 7.3 TB | /mnt/fdb1 |
| FDB storage server 2 | nvme9n1 | p1 | 7.3 TB | /mnt/fdb2 |
| FDB log server 0 | nvme1n1 | p3 | 80 GB | /mnt/fdb-log0 |
| FDB log server 1 | nvme7n1 | p3 | 80 GB | /mnt/fdb-log1 |
| FDB log server 2 + coordinator | nvme5n1 | new LV (80 GB) | 80 GB | /mnt/fdb-log2 |

### Process allocation

| Role | Instances | Cores | RAM |
|---|---|---|---|
| FDB storage server | 3 | 3 x 4 = 12 | 3 x 8 GB = 24 GB |
| FDB transaction log + stateless | 3 | 3 x 4 = 12 | 3 x 12 GB = 36 GB |
| KV-RGW backend | 3 | 3 x 8 = 24 | 3 x 16 GB = 48 GB |
| KV-RGW frontend | 3 | 3 x 4 = 12 | 3 x 2 GB = 6 GB |
| KVRGW_DATA | — | — | directory (no dedicated drive) |
| OS / test driver / headroom | — | 52 | 262 GB |

**Redundancy:** `triple` — 3 copies per write. 3 log servers satisfy triple requirement.

**Drive constraints:** 6 of 9 NVMe drives are allocated to Ceph (not running). Log servers use unused partitions on Ceph drives. Storage servers use 3 dedicated drives.

**Extrapolation model:** "3 backend instances on this machine. A production cluster with 12 backends needs 4x the machines."

**Known optimistic biases (document in results):**
- **Page cache:** 262 GB free RAM caches the entire dataset in memory, inflating read latency/IOPS. Mitigated by dropping page cache between test runs, but warm-cache runs will be unrealistically fast.
- **No shard distribution cost:** Triple replication with 3 storage servers = every server holds all shards. No cross-shard routing overhead. In production with more servers, shard placement adds coordination cost that is absent here.

---

## Phase 1 — Blackbox (End-to-End S3)

Standard S3 load generators hitting nginx:9080. Measures the full stack: HTTP parsing, SigV4, gRPC, backend FDB transactions, data tier I/O.

### Object size tiers

| Tier | Size | Storage | KV ops per PUT |
|---|---|---|---|
| Inline | 128B | Embedded in O: value | 1 txn (2R + 1W) |
| D: (child KV) | 4KB | Separate D: entry | 1 txn (2R + 2W) |
| Storage | 16KB, 64KB | Filesystem blob | P:O + blob + commit txn (3R + 2W) |

### Test matrix

| Test | Measure | Parameters |
|---|---|---|
| PUT IOPS per tier | ops/sec, latency | 128B, 4KB, 16KB, 64KB. Concurrency: 16, 64, 256, 1024 |
| GET IOPS per tier | ops/sec, latency | Same sizes. Pre-loaded |
| DELETE IOPS | ops/sec | Bulk delete per tier |
| Mixed workload | IOPS, latency | 50/30/10/10 PUT/GET/LIST/DELETE, per tier |
| LIST latency | p50/p99/max | Page sizes 100, 1000. Under idle + under write load |
| Versioned PUT per tier | ops/sec, latency | Same sizes with versioning enabled |
| Versioned LIST | ListObjectVersions latency | Page scan cost |
| Versioned DELETE | ops/sec | DeleteObjectVersion |

**Dataset:** ~1M objects (moderate). Scaling to extreme sizes is Phase 2 only.

**Deliverable:** CSV/JSON results, gnuplot scripts, comparison baseline.

### Phase 1 Results

Tier config: `KVRGW_MAX_INLINE=256`, `KVRGW_MAX_KV_STORE=4096`. Tool: `warp` (MinIO), 30s per test, 3 runs averaged. Page cache dropped between runs. 3 backend instances, 3 frontend instances, nginx GW on :9080.

**PUT IOPS (ops/sec) — non-versioned**

| Size | Tier | c=16 | c=64 | c=256 |
|---|---|---|---|---|
| 128B | INLINE | 4,110 | 9,320 | 10,400 |
| 4KB | D: child KV | 3,700 | 7,650 | 8,550 |
| 8KB | STORAGE | 2,825 | 6,850 | 8,880 |

**PUT latency (p50 / p99) — non-versioned**

| Size | c=16 | c=64 | c=256 |
|---|---|---|---|
| 128B | 3.9ms / 6ms | 6.6ms / 12ms | 23ms / 33ms |
| 4KB | 4.1ms / 7ms | 8.1ms / 13ms | 28ms / 48ms |
| 8KB | 5.4ms / 11ms | 9.1ms / 22ms | 27ms / 62ms |

**GET IOPS (ops/sec)**

| Size | c=16 | c=64 | c=256 |
|---|---|---|---|
| 128B | 9,090 | 12,900 | 12,250 |
| 4KB | 8,250 | 12,000 | 11,300 |
| 8KB | 8,600 | 11,700 | 11,070 |

**GET latency (p50 / p99)**

| Size | c=16 | c=64 | c=256 |
|---|---|---|---|
| 128B | 1.7ms / 3ms | 4.9ms / 7ms | 20ms / 26ms |
| 4KB | 1.9ms / 3ms | 5.2ms / 8ms | 22ms / 32ms |
| 8KB | 1.8ms / 4ms | 5.4ms / 7ms | 23ms / 29ms |

**LIST (objects/sec, c=16):** ~285,000 obj/sec across all sizes (page cache warm)

**Mixed workload (c=64, 50/30/10/10 PUT/GET/DEL/STAT)**

| Size | Total ops/sec | PUT p50 | GET p50 |
|---|---|---|---|
| 128B | 10,990 | 6.8ms | 4.2ms |
| 4KB | 9,290 | 7.9ms | 5.0ms |
| 8KB | 8,900 | 9.3ms | 4.1ms |

**Versioned PUT IOPS (ops/sec) — versioning enabled**

| Size | c=16 | c=64 | c=256 |
|---|---|---|---|
| 128B | 4,110 | 9,330 | 10,920 |
| 4KB | 3,670 | 7,540 | 8,470 |
| 8KB | 2,810 | 6,800 | 9,130 |

**Versioning overhead:** Within noise (<2%). The extra V: displacement write does not measurably impact throughput at these concurrency levels.

**Observations:**
- PUT peaks at c=256 (~10K ops/sec INLINE, ~8.9K STORAGE)
- GET peaks at c=64 (~13K ops/sec INLINE), slight degradation at c=256 due to contention
- STORAGE tier PUT is ~15-30% slower than INLINE (filesystem I/O overhead)
- GET performance is similar across tiers (page cache effect — all data served from memory)
- LIST is dominated by page cache; 285K obj/sec is not representative of cold-cache production

**Raw data:** `perf-results/phase1/`, `perf-results/phase1-run2/`, `perf-results/phase1-run3/`, `perf-results/versioning/`

### End-to-End Latency Breakdown (c=64)

Instrumented via `fdb_latency.hpp` — thread-local per-request accumulator measures FDB time and disk I/O within the C++ backend. End-to-end S3 latency from warp. Non-backend overhead derived as: S3 total - backend total. This includes Go frontend, nginx, gRPC framing, and client-side network — not just Go processing.

**PutObject — full stack latency allocation**

| Size | S3 total (ms) | Non-backend (ms) | Non-backend % | FDB (ms) | FDB % | C++ other (ms) | C++ % |
|---|---|---|---|---|---|---|---|
| 128B | 6.8 | 4.5 | 66% | 2.1 | 31% | 0.2 | 3% |
| 4KB | 8.4 | 5.1 | 61% | 3.2 | 38% | 0.2 | 2% |
| 8KB | 9.1 | 4.1 | 45% | 4.5 | 49% | 0.5 | 5% |

**GetObject — full stack latency allocation**

| Size | S3 total (ms) | Non-backend (ms) | Non-backend % | FDB (ms) | FDB % | C++ other (ms) | C++ % |
|---|---|---|---|---|---|---|---|
| 128B | 4.9 | 4.3 | 88% | 0.4 | 9% | 0.2 | 3% |
| 4KB | 5.2 | 4.4 | 85% | 0.7 | 13% | 0.1 | 2% |
| 8KB | 5.4 | 4.8 | 89% | 0.4 | 7% | 0.2 | 4% |

**ListObjects (c=16)**

| S3 total (ms) | Non-backend (ms) | Non-backend % | FDB (ms) | FDB % |
|---|---|---|---|---|
| 34.5 | 33.5 | 97% | 0.9 | 3% |

**DeleteMulti (batch) — full stack latency allocation (c=64, 256K objects)**

| Size | S3 total (ms) | Non-backend (ms) | Non-backend % | FDB (ms) | FDB % | C++ other (ms) | C++ % |
|---|---|---|---|---|---|---|---|
| 128B | 237 | 3 | 1% | 233 | 98% | 0.6 | 0% |
| 4KB | 466 | 10 | 2% | 455 | 98% | 0.8 | 0% |
| 8KB | 313 | 15 | 5% | 298 | 95% | 0.8 | 0% |

**DeleteMulti per-object cost** (batch ~300 objects, 10 keys/txn):

| Size | Per-object total (ms) | Per-object FDB (ms) | obj/sec |
|---|---|---|---|
| 128B | 0.78 | 0.78 | 27,078 |
| 4KB | 1.52 | 1.52 | 13,515 |
| 8KB | 0.99 | 0.99 | 20,590 |

DeleteMulti is 95-98% FDB — Go/nginx overhead is negligible because one HTTP request triggers hundreds of FDB deletes. Per-object FDB cost (0.78-1.52ms) is the fair comparison against RADOS per-object delete cost, since in the RADOS model DeleteMulti only saves HTTP round-trips — each object delete is an independent OSD operation regardless of batching.

### FDB Internal Breakdown (C++ backend only)

Within the C++ backend, FDB dominates — 90-95% of backend time is FDB calls.

**PutObject — FDB % of C++ backend RPC time**

| Size | Backend (us) | FDB get (us) | FDB commit (us) | FDB % of backend |
|---|---|---|---|---|
| 128B | 2,282 | 808 | 1,293 | 92% |
| 4KB | 3,339 | 1,605 | 1,565 | 95% |
| 8KB | 4,980 | 1,388 | 3,079 | 90% |

**Versioned PutObject** — same latency profile as non-versioned (no measurable overhead).

### FDB System Metrics (during c=64 benchmark)

Collected via pidstat, iostat, and `fdbcli status json` during instrumented runs.

**FDB server CPU and memory:**

| Role | CPU % | RSS (MB) |
|---|---|---|
| Storage server (x3) | ~65% each | ~3,000 each |
| Log server (x3) | ~33% each | ~155 each |
| Stateless (x3) | 17-66% | ~100-150 each |

**FDB internal metrics:**

| Metric | Value |
|---|---|
| Commit latency (mean) | 1.2 ms |
| Commit latency (max) | 6.8 ms |
| Committed txns/sec | 13,739 |
| Reads/sec | 35,432 |
| Writes/sec | 20,612 |
| Conflicts (total) | 581 (negligible) |
| NVMe disk busy | 1-2% |
| Performance limited by | "workload" (FDB not saturated) |

### Bottleneck Analysis

**Non-backend overhead (Go frontend + nginx + gRPC + client network) is the dominant latency contributor:**
- **PUT:** 45-66% of total S3 latency, depending on object size
- **GET:** 85-89% of total S3 latency — FDB read is only 0.4ms, non-backend overhead dominates
- **LIST:** 97% — FDB scan is sub-millisecond, nearly all time is outside the C++ backend

Note: "Non-backend" includes Go frontend processing, nginx proxying, gRPC framing/serialization, and client-side network. Isolating Go processing alone requires Go-side instrumentation.

**FDB is significant but secondary:**
- PUT commit latency (1.2ms floor) is irreducible with `triple` replication
- FDB's share grows with object size (more data to commit) — from 31% at 128B to 49% at 8KB
- For GET, FDB is a minor factor (~9%) — the bottleneck is entirely outside the backend

**C++ backend non-FDB work is negligible** — 2-5% across all operations. Key building, value packing, protobuf handling are well-optimized. For STORAGE tier (8KB), disk I/O is now instrumented separately.

**FDB has headroom:**
- NVMe disks at 1-2% utilization — not I/O bound
- Storage server CPU at 65% — not CPU saturated
- FDB reports "not saturated by workload" — more concurrency could increase aggregate IOPS

**Implications:**
- Replacing the Go frontend (planned) would eliminate ~4.5ms non-backend overhead, roughly doubling PUT IOPS and tripling GET IOPS
- After Go removal, FDB commit latency (1.2ms) becomes the dominant cost for writes
- Versioning adds no measurable overhead to any layer

**Raw metrics:** `perf-results/phase1-instrumented/metrics/`

---

## Phase 2 — Simulation (FDB-Direct C++ Driver)

### Architecture

Custom C++ binary that:
- Reuses POC key builders (`make_object_key`, `make_d_key`, `make_go_key`, etc.)
- Reuses value builders (`ObjectValueHeader`, `OValueBuf`, `write_object_value`)
- Calls FDB C API directly (same transaction patterns as the backend)
- Multi-threaded: N worker threads, each with own FDB client instance

### Phase 2A — Full simulation (all tiers, no data transfer)

All tiers simulated with correct KV structure but no actual data payload. Measures the metadata transaction overhead per tier.

- Inline (128B): O: value with empty inline_data placeholder
- D: tier (4KB): O: + D: entry (empty KV, correct key structure)
- Storage tier: P:O + nop blob stub + O: commit (no filesystem I/O)

### Phase 2B — Pure KV simulation (metadata-only tiers)

Only operations entirely within FDB — isolates pure FDB transaction performance.

- Inline (128B): O: value only — pure single-write transaction rate
- D: tier (4KB): O: + D: — pure two-write transaction rate
- No storage tier (P:O + phase3 adds noise even with nop stub)

### Operations simulated

| Operation | FDB pattern | KV reads | KV writes |
|---|---|---|---|
| PUT (inline) | single txn: get(B) + get(O:) + put(O:) | 2 | 1 |
| PUT (D: tier) | single txn: get(B) + get(O:) + put(O:) + put(D:) | 2 | 2 |
| PUT (storage) | set(P:O) + nop + txn: get(P:O) + get(B) + get(O:) + put(O:) + del(P:O) | 3 | 2 |
| PUT (versioned) | txn: get(B) + get(O:) + put(V:old) + put(O:new) | 2 | 2-3 |
| GET (inline) | get(O:) | 1 | 0 |
| GET (D: tier) | get(O:) + get(D:) | 2 | 0 |
| GET (version) | get(V:key+vid) | 1 | 0 |
| LIST (page) | get(B) + range_scan(O: prefix, limit) | 1+N | 0 |
| ListObjectVersions | range_scan(O:) + range_scan(V:) + merge | 2+N | 0 |
| DELETE | txn: get(B) + get(O:) + put(G:O) + del(O:) | 2 | 2 |
| DELETE (versioned) | txn: get(B) + get(O:) + put(V:old) + put(O:DM) | 2 | 2-3 |
| DeleteObjectVersion | txn: get(V:) + del(V:) + optional promote | 1-2 | 1-2 |

---

## Test Scenarios (Phase 2)

### 1. IOPS ceiling (single operation type)

- Pure PUT (inline 128B): find max transactions/sec
- Pure PUT versioned (same key, accumulate versions): txn/sec vs version depth
- Pure GET (pre-loaded): find max reads/sec
- Pure GET specific version: reads/sec on V: entries
- Pure LIST (page=1000): find max scans/sec
- Pure ListObjectVersions (page=1000): scan/merge cost

### 2. Mixed workload

- Configurable ratio: `--put-pct 50 --get-pct 30 --list-pct 10 --delete-pct 10`
- Ramp concurrency until IOPS plateaus or FDB conflict rate > 5%
- Same mix with versioning enabled vs disabled

### 3. Scaling tests

- **Objects-per-bucket:** 1M, 10M, 100M, 1B keys in one bucket; measure PUT IOPS degradation
- **Bucket count:** 1K, 100K, 1M, 10M buckets; measure ListBuckets and per-bucket PUT/GET latency
- **Key fanout:** range_scan with varying prefix selectivity

### 4. Conflict behavior

- N threads writing to same bucket (hot bucket contention)
- Measure conflict retry rate vs IOPS
- Compare: 1 bucket vs 100 buckets vs 10000 buckets for same total write rate

### 5. Listing at scale

- 1B objects in bucket; paginated LIST from start to end
- 10M buckets; ListBuckets full scan
- Measure: pages/sec, latency per page
- Compare to RADOS cls_list performance numbers

### 6. Versioning at scale

- Single key with 1K, 10K, 100K versions; measure PUT latency (displacement cost)
- ListObjectVersions on 1M objects x 10 versions each; page scan time
- DeleteObjectVersion on deep stacks; promotion cost
- Version accumulation: 100K keys x 100 versions = 10M V: entries; overall system impact

---

## Key Metrics

| Metric | Source |
|---|---|
| Transactions/sec (committed) | Driver counters |
| Conflicts/sec (FDB error 1020) | Driver counters |
| Latency histogram (p50, p95, p99, max) | HDR histogram per operation type |
| FDB storage_queue, log_queue | `fdbcli --exec "status json"` |
| FDB data_distribution | `fdbcli --exec "status json"` |
| NVMe utilization | `iostat -x 1` per drive |

---

## Source Files (new)

| File | Purpose |
|---|---|
| `backend/perf/fdb_perf_driver.cpp` | Main driver, thread pool, workload config, CLI |
| `backend/perf/workload.hpp` | Operation generators (PUT/GET/LIST/DELETE/versioned) |
| `backend/perf/metrics.hpp` | HDR histogram, throughput counters, CSV export |
| `backend/perf/nop_data_store.hpp` | No-op data store stub (configurable latency) |

**Reuses from POC:** `keys.cpp`, `object_value.cpp`, `kv_store.cpp`, `fdb_blocking.cpp`, `ref_count.hpp`, `tag_value.cpp`

---

## Comparison with RADOS

| Dimension | KV model (FDB) | RADOS model | Expected advantage |
|---|---|---|---|
| PUT latency | 1 FDB txn (2-3 KV ops) | OSD write + cls_rgw index update | KV: lower (no separate index update) |
| GET latency | 1-2 KV reads | OSD read | Similar (both single-hop) |
| LIST | Range scan on ordered keys | cls_rgw bucket index shard scan + merge | KV: better ordering, no shard merge |
| Bucket scaling | B: prefix scan (ordered) | Separate bucket instance pool | KV: O(1) per bucket, no pool overhead |
| Conflict under load | FDB MVCC + retry | OSD-level locking | KV: finer granularity, but retry cost |
| Object count limit | FDB key space (~10^12) | Per-shard OSD limits, resharding | KV: no resharding needed |
| Versioning depth | V: ordered entries, O(1) displacement | cls_versioning per-shard | KV: no shard split on deep versioning |

---

## FDB Cluster Setup

### Deployment model

6 Docker containers with `--network=host` (no bridge overhead, localhost communication):

- **3 storage containers** — one per dedicated NVMe drive, each runs one `fdbserver` with `class = storage`
- **3 log containers** — one per log partition/LV, each runs two `fdbserver` processes: one with `class = log`, one with `class = stateless` (candidate for coordinator, commit proxy, GRV proxy, resolver)

Each container gets CPU pinning and memory limits for isolation. FDB auto-recruits stateless processes from the log containers.

**`--network=host`:** All containers use host networking — no Docker bridge overhead. FDB processes communicate via localhost, matching production latency characteristics.

**`--locality_machineid`:** Each container sets a unique `locality_machineid` (e.g. `fdb-storage0`, `fdb-log0`) so FDB treats them as separate fault domains. Without this, FDB sees all processes on one machine and refuses to satisfy `triple` replication.

### foundationdb.conf (per container)

Each container has its own `foundationdb.conf` with unique ports and `locality_machineid`.

fdb-storage0 (`/mnt/fdb0/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4500]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = storage
locality_machineid = fdb-storage0
```

fdb-storage1 (`/mnt/fdb1/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4501]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = storage
locality_machineid = fdb-storage1
```

fdb-storage2 (`/mnt/fdb2/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4502]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = storage
locality_machineid = fdb-storage2
```

fdb-log0 (`/mnt/fdb-log0/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4503]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = log
locality_machineid = fdb-log0

[fdbserver.4506]
datadir = /data/data-stateless/$ID
logdir = /data/logs
memory = 4GiB
class = stateless
locality_machineid = fdb-log0
```

fdb-log1 (`/mnt/fdb-log1/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4504]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = log
locality_machineid = fdb-log1

[fdbserver.4507]
datadir = /data/data-stateless/$ID
logdir = /data/logs
memory = 4GiB
class = stateless
locality_machineid = fdb-log1
```

fdb-log2 (`/mnt/fdb-log2/foundationdb.conf`):
```ini
[fdbmonitor]

[general]
cluster-file = /data/fdb.cluster
restart-delay = 5

[fdbserver.4505]
datadir = /data/data/$ID
logdir = /data/logs
memory = 8GiB
class = log
locality_machineid = fdb-log2

[fdbserver.4508]
datadir = /data/data-stateless/$ID
logdir = /data/logs
memory = 4GiB
class = stateless
locality_machineid = fdb-log2
```

Port assignment: storage servers 4500–4502, log servers 4503–4505, stateless 4506–4508. Each log container runs two fdbserver processes (one log + one stateless). `restart-delay = 5` for fast crash detection during perf tests.

### NVMe mounts (host side)

```
/dev/nvme4n1p1  → /mnt/fdb0      (storage server 0, 7.3 TB, label fdb-disk9)
/dev/nvme8n1p1  → /mnt/fdb1      (storage server 1, 7.3 TB, label fdb-disk6)
/dev/nvme9n1p1  → /mnt/fdb2      (storage server 2, 7.3 TB, label fdb-disk8)
/dev/nvme1n1p3  → /mnt/fdb-log0  (log server 0, 80 GB, on Ceph drive)
/dev/nvme7n1p3  → /mnt/fdb-log1  (log server 1, 80 GB, on Ceph drive)
/dev/vg_nvme/lv_fdblog → /mnt/fdb-log2  (log server 2 + coordinator, 80 GB LV, on nvme5n1)
```

Mount options: `ext4`, `noatime,nodiscard`

**No online TRIM:** `discard` causes synchronous TRIM latency spikes that skew p99/max numbers. Run `sudo fstrim /mnt/fdb*` and `sudo fstrim /mnt/fdb-log*` manually before each test run instead.

Container volume mapping: host `/mnt/fdb{0,1,2}` and `/mnt/fdb-log{0,1,2}` → container `/data`.

### NUMA-aware CPU pinning

2-socket system (2x Xeon 8276M) = 2 NUMA nodes. Cross-socket memory access adds ~100ns latency. Before deployment, check which NUMA node each NVMe drive is on:

```bash
cat /sys/block/nvme4n1/device/device/numa_node   # storage0
cat /sys/block/nvme8n1/device/device/numa_node   # storage1
cat /sys/block/nvme9n1/device/device/numa_node   # storage2
```

Pin each container's CPU and memory to the same NUMA node as its NVMe drive using `--cpuset-cpus` and `--cpuset-mems` in Docker, or `numactl --cpunodebind=N --membind=N`.

### CPU frequency pinning

Lock CPU frequency for reproducible results. Turbo Boost (2.2 GHz base → 4.0 GHz max) varies with thermal state and active core count, causing run-to-run variance.

```bash
sudo cpupower frequency-set -g performance
# Optional: disable turbo for maximum reproducibility (lower absolute numbers)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
```

### Initialize cluster

```bash
fdbcli --exec "configure new single ssd"
fdbcli --exec "configure triple"
fdbcli --exec "coordinators auto"
```

### Pre-test preparation

```bash
# Drop page cache between test runs to avoid inflated read numbers
# (262 GB free RAM would otherwise cache entire dataset in memory)
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# TRIM all FDB drives (run once before each test, not during)
sudo fstrim /mnt/fdb0 /mnt/fdb1 /mnt/fdb2
sudo fstrim /mnt/fdb-log0 /mnt/fdb-log1 /mnt/fdb-log2
```
