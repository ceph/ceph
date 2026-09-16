# Thread Configuration & Profiling Results

## FDB Cluster Configuration

6 storage servers (2 per NVMe), 3 log servers, 3 stateless (commit proxy, GRV proxy, resolver).

| Container | Port | CPUset | Role |
|-----------|------|--------|------|
| fdb-storage0 | 4500 | 0-1 | storage |
| fdb-storage0b | 4509 | 2-3 | storage |
| fdb-storage1 | 4501 | 4-5 | storage |
| fdb-storage1b | 4510 | 6-7 | storage |
| fdb-storage2 | 4502 | 8-9 | storage |
| fdb-storage2b | 4511 | 10-11 | storage |
| fdb-log0 | 4503 | 12-15 | log |
| fdb-log1 | 4504 | 16-19 | log |
| fdb-log2 | 4505 | 20-23 | log |
| fdb-stateless0 | 4506 | 12-15 | stateless |
| fdb-stateless1 | 4507 | 16-19 | stateless |
| fdb-stateless2 | 4508 | 20-23 | stateless |

## Tier Configuration

- `KVRGW_MAX_INLINE=256` (objects <= 256B → INLINE tier)
- `KVRGW_MAX_KV_STORE=4096` (objects <= 4KB → D-tier; > 4KB → storage tier)

## Test Environment

- 112-core machine, Rocky Linux 9
- FDB 7.x, triple redundancy
- All tests use a single unversioned bucket unless noted
- Perf driver runs in-process (no Go frontend, no gRPC overhead)

---

## Non-Batch Baseline (batch_size=1)

### 3 Storage Servers (original config, 4 cores each)

| Tier | c= | Duration | IOPS | FDB % | SS CPU avg | SS CPU max |
|------|-----|----------|------|-------|-----------|-----------|
| 128B | 128 | 100s | 17,173 | 99.8% | 0.75 | 0.96 |
| 4KB | 128 | 100s | — | — | — | — |
| 8KB | 128 | 100s | — | — | — | — |

### 6 Storage Servers (shared 4-core cpusets)

| Tier | c= | Duration | IOPS | FDB % | SS CPU avg | SS CPU max |
|------|-----|----------|------|-------|-----------|-----------|
| 128B | 128 | 100s | 20,492 | 99.8% | 0.26 | 0.41 |

### 6 Storage Servers (dedicated 2-core cpusets)

| Tier | c= | Duration | IOPS | FDB % | SS CPU avg | SS CPU max |
|------|-----|----------|------|-------|-----------|-----------|
| 128B | 128 | 100s | 19,437 | 99.8% | 0.25 | 0.40 |

### 6 Storage Servers — Full Tier Sweep (clean FDB, 100s)

| Tier | c= | IOPS | Avg Total (us) | FDB Get (us) | FDB Commit (us) | Disk (us) |
|------|-----|------|----------------|--------------|-----------------|-----------|
| 128B | 128 | 18,181 | 7,035 | 4,810 | 2,210 | 0 |
| 4KB | 128 | 13,874 | 9,212 | 6,763 | 2,430 | 0 |
| 8KB | 128 | 8,209 | 15,571 | 7,931 | 6,517 | 1,095 |

FDB stats: txn/sec avg=10,873, max=19,242. SS CPU avg=0.54, max=0.95.

---

## Batch Mode Results

### Batch 10x16 (batch_size=10, threads=16, c=112, clean FDB, 100s)

| Tier | IOPS | vs Non-batch | Avg Total (us) | Avg Wait (us) |
|------|------|-------------|----------------|---------------|
| 128B | 27,156 | **+49%** | 4,113 | 352 |
| 4KB | 12,114 | -13% | 9,225 | 352 |
| 8KB | 7,347 | -10% | 15,222 | 352 |

Batch stats: batch_commits=502,583, avg_batch_size=9.3, max_wait_us=4,188.
FDB stats: txn/sec avg=4,951, max=8,955. SS CPU avg=0.57, max=0.93.

### 8KB Zero-Disk (batch_size=10, threads=16, c=128, PerfDataStore sim_write=0, 60s)

| Tier | IOPS | vs Real Disk | Avg Total (us) | Avg Wait (us) | Avg Queue Size |
|------|------|-------------|----------------|---------------|----------------|
| 8KB | 25,522 | **+182%** (vs 9,043) | 4,969 | 323 | 11.8 |

Batch stats: batch_commits=156,651, avg_batch_size=9.8, max_wait_us=8,378.
Confirms storage-tier bottleneck is entirely disk I/O, not FDB.

### Batch 5x32 (batch_size=5, threads=32, c=96, clean FDB, 100s)

| Tier | IOPS | vs Non-batch | Avg Total (us) | Avg Wait (us) |
|------|------|-------------|----------------|---------------|
| 128B | 27,273 | **+50%** | 3,511 | 224 |
| 4KB | 14,628 | +5% | 6,641 | 224 |
| 8KB | 7,746 | -6% | 12,371 | 224 |

Batch stats: batch_commits=1,028,348, avg_batch_size=4.8, max_wait_us=2,219.
FDB stats: txn/sec avg=6,850, max=10,276. SS CPU avg=0.58, max=0.95.

### Thread Sweep (30s, batch_size=10, timeout=1000us)

| Threads | Push c= | 128B IOPS | 4KB IOPS | 8KB IOPS | avg_wait_us | max_wait_us |
|---------|---------|-----------|----------|----------|-------------|-------------|
| 8 | 120 | 17,668 | 13,156 | 7,798 | 2,732 | 20,392 |
| **16** | **112** | **19,774** | **15,316** | **7,427** | **353** | **2,864** |
| 32 | 96 | 18,204 | 14,242 | 7,118 | 318 | 1,702 |
| 64 | 64 | 14,971 | 12,379 | 5,848 | 371 | 1,697 |

Best overall: **16 threads** for 128B/4KB, **32 threads** for lowest wait.

---

## put-multi (Direct Batch, No Queue)

10 objects per FDB transaction, each thread manages its own transaction. No queue coordination.

| Tier | c= | Duration | IOPS | Note |
|------|-----|----------|------|------|
| 128B | 128 | 60s | 137,482 | Theoretical max (no queue overhead) |

---

## Delete Operations

| Operation | Objects | c= | IOPS | Avg Latency (us) |
|-----------|---------|-----|------|-------------------|
| delete (single) | 1,082,276 | 128 | 29,524 | 4,333 |
| delete-multi (10/txn, parallel) | 1,066,723 | 128 | 41,045 | 31,155 |
| delete-multi (10/txn, serial, old) | 1,943,657 | 128 | 3,082 | 323,883 |

---

## Key Observations

1. **FDB storage server CPU is the ceiling** — single-threaded event loop saturates at 0.95 cores regardless of batch mode or thread count.

2. **Doubling storage servers (3→6) gave +19% IOPS** — less than expected 2x because other components (commit proxy, log servers) become constraints.

3. **Batch mode helps small objects most** — 128B sees +50% from commit amortization. 4KB/8KB see diminishing/negative returns because per-transaction work grows (more keys written per commit).

4. **put-multi shows the theoretical ceiling** — 137K IOPS with direct 10/txn proves the batch queue overhead (mutex, cv, promise/future) accounts for the gap between 27K (batch queue) and 137K (direct).

5. **8 batch threads bottleneck** (avg_wait=2.7ms, max=20ms). **16 threads** is optimal balance. **32+ threads** reduce wait but starve push threads.

6. **Zero txn conflicts/failures** in all tests — clean workload with unique object names per thread.

7. **PerfDataStore isolates FDB from disk** — `--perf` mode uses `PerfDataStore` (no-op I/O with configurable sleep via `set-sim-disk-write-us`). Eliminates filesystem variance from measurements, enabling clean comparison of batch commit overhead vs FDB transaction cost.

8. **Batch size 10 is optimal** — tested 1/5/8/9/10/11/16 on single instance (8KB, PerfDataStore, c=128, 16 threads). batch=10 consistently leads at ~25K IOPS. batch=5 is queue-starved (avg_queue=38, wait=3ms). batch=16 has diminishing returns from larger FDB transactions.

9. **Producer thread count matters** — c=128 is needed to keep the batch queue saturated. c=64 drops to 34K aggregate (3 instances), c=32 to 22K. The queue needs enough backpressure to fill batches without timeout delays.

10. **More buckets hurt performance** — tested 1/16/32/64/128 buckets per instance. 1 bucket is fastest (47.7K aggregate). More buckets add B: verification reads per batch and increase FDB key range tracking overhead without improving SS distribution. The `burst=N` parameter mitigates this by sending N consecutive objects to the same bucket before rotating, reducing per-batch B: reads.

11. **FDB SS CPU is the cluster ceiling** — at 48K aggregate IOPS, hottest SS hits 0.98 cores. FDB reports "limited by workload" but individual SS are saturated. Adding more SS processes is the scaling path.

12. **FDB resource minimums** — SS, log, and stateless all run on 1 core + 1GiB FDB memory + 256MiB cache with no performance regression. Actual memory usage: SS ~340MB, log ~290MB, stateless ~200MB.

---

## Batch Size Sweep (single instance, 8KB, PerfDataStore, c=128, 16 threads, 60s)

| batch_size | IOPS | vs batch=1 | avg_total_us | avg_queue_size | avg_wait_us |
|-----------|------|-----------|-------------|----------------|-------------|
| 1 | 10,832 | baseline | 11,793 | — | — |
| 5 | 16,180 | +49% | 7,887 | 38.3 | 3,055 |
| 8 | 21,838 | +102% | 5,838 | 10.4 | 235 |
| 9 | 20,360 | +88% | 6,248 | 10.9 | 283 |
| **10** | **25,522** | **+136%** | **4,969** | **11.8** | **323** |
| 11 | 20,083 | +85% | 6,333 | 12.6 | 308 |
| 16 | 20,164 | +86% | 6,304 | 18.0 | 277 |

---

## Multi-Instance Scaling (8KB, batch=10, PerfDataStore, 16 threads, 120s)

| Instances | SS count | Aggregate IOPS | Per-instance | SS CPU max | SS CPU avg | txn/sec | Queue MB |
|-----------|---------|---------------|-------------|-----------|-----------|---------|---------|
| 1 | 6 | 25,522 | 25,522 | 0.47 | 0.18 | 4,876 | 86 |
| 3 | 6 | 47,749 | 15,916 | 0.98 | 0.75 | 9,204 | 388 |
| 6 | 6 | 47,742 | 7,957 | 0.98 | 0.72 | 8,531 | 775 |
| 3 | 3 | 40,709 | 13,570 | 0.98 | 0.67 | 8,345 | 529 |
| 3 | 12 | 52,912 | 17,637 | 0.98 | 0.47 | 10,066 | 277 |

3→6 instances adds nothing — FDB SS is already saturated. 12 SS gives +11% over 6 SS but diminishing returns (hottest SS still 0.98). FDB key distribution is uneven — 2-3 SS carry most of the load regardless of total SS count.

---

## Producer Thread Sweep (3 instances, 6 SS, batch=10, 8KB, PerfDataStore, 120s)

| Producers (c=) | Aggregate IOPS | avg_queue_size | SS CPU max |
|----------------|---------------|----------------|-----------|
| 32 | 22,146 | 9.1 | 0.55 |
| 64 | 34,243 | 10.6 | 0.62 |
| 128 | 47,749 | 12.5 | 0.98 |

c=128 is necessary to saturate the cluster. c=32 underfeeds the batch queue.

---

## FDB Resource Minimization

All configs use 1 core per container, `-m 1GiB --cache_memory 256MiB`, 2G container mem_limit.

| Config | Aggregate IOPS | SS CPU max | Notes |
|--------|---------------|-----------|-------|
| Original (2-core SS, 8G FDB, 4-core log/stateless) | 46,204 | 0.98 | Baseline |
| First shrink (1-core log/stateless) | 44,995 | 0.98 | No regression |
| Minimal (1-core all, 1G FDB, 256M cache) | 47,749 | 0.98 | No regression |

Minimal config frees 100 of 112 cores and 72G of DRAM for other uses.

---

## Post-Test Diagnostics

After any perf test, run `HEALTH` first to check if FDB kept up with the workload:

```bash
./scripts/display_perf_results.sh perf-results/<dir>/ HEALTH
```

This shows one line per FDB server with input/durable ratio, data lag trend, CPU, and flags (`[HOTSPOT]`, `[SATURATED]`, `[FALLING_BEHIND]`). If any server shows problems, drill into `SS`, `LOG`, or `SL` for detailed per-server metrics.

All summary selectors (KVRGW, SS, LOG, SL) use a state machine to filter samples: IDLE → RAMPUP → ACTIVE → DRAIN. Only ACTIVE phase samples are included in stats. Warmup samples (default 3, configurable via `--warmup N`) and collection artifacts (zero-IOPS samples mid-test) are excluded automatically.

---

## Configuration Recommendations

| Workload | batch_size | timeout_us | threads | Push c= |
|----------|-----------|------------|---------|---------|
| Small objects (<= 256B) | 10 | 1000 | 16 | 128 |
| Mixed (128B-4KB) | 10 | 1000 | 16 | 128 |
| Large objects (8KB+) | 10 | 1000 | 16 | 128 |
| Testing/debugging | 1 | — | — | 128 |

Recommended `burst=` setting: match `batch_size` (e.g. `burst=10` with `batch_size=10`) when using multiple buckets to minimize per-batch bucket verification reads.

batch=10 with 16 threads is optimal across all tiers when using PerfDataStore. With real disk, storage tier is disk-bound regardless of batch config.

---

## FDB Cluster Configuration System

The FDB cluster topology is managed through a three-file system:

- **`System-Resources.md`** — one-time machine inventory with hard limits (max 48 cores, max 48 GB DRAM for FDB). Lists allowed NVMe drives/partitions.
- **`FDB-Default-Config.txt`** — human-editable intent: role counts and per-container resources. The default profile used by reload/load scripts.
- **`scripts/gen_fdb_config.sh`** — validates config against resource limits, assigns CPU cores from NUMA topology, derives cache_memory (50% of MEM) and mem_limit (2x MEM). Generates `FDB-Default-Config.md` with resolved core IDs and mount mappings. Port ranges: SS 4500-4511, LOG 4520-4522, SL 4530-4532.
- **`scripts/gen_docker_compose.sh`** — reads the generated `.md` and produces `fdb-cluster/docker-compose.yml`. Validates mount paths against `System-Resources.md` allowlist and cpuset/cores consistency.

Additional profiles (e.g. `FDB-12SS-Config.txt`) can be created and fed to the generator to produce matching `.md` files for specific test scenarios.

### Steady-State Validation (single instance, 3 SS, 600s)

A 600-second run with 1 instance (c=128, batch=10, 8KB, PerfDataStore) on 3 SS confirmed stable steady state:
- IOPS: 21.8K median, <7% deviation
- SS CPU: 0.74-0.81, `in/dur=1.0x` (input equals durable rate)
- LOG queue: flat at 107 MB (not growing)
- Zero errors, zero data_lag
- FDB ratekeeper throttling at ~190 MB `limiting_queue` — normal proactive back-pressure at 0.76 CPU, not degradation

---

## Error Codes Refactor — Performance Validation

Post-refactor perf run (batch_size=1, clean FDB, c=128). Confirms no regression from the `KvrgwErrorCode` refactor — logic-only change, no data path differences.

### PUT 8KB isolated (clean FDB, 100s)

| Tier | IOPS | Errors | Retries |
|------|------|--------|---------|
| 8KB | 10,967 | 0 | 0 |

### Full Suite (per-tier isolated, clean FDB, c=128, 100s PUT duration)

| Operation | 128B IOPS | 4KB IOPS | 8KB IOPS |
|-----------|-----------|----------|----------|
| PUT | 20,583 | 12,562 | 10,716 |
| PUT-OVERWRITE | 19,111 | 6,085 | 10,021 |
| COPY | 18,382 | 7,288 | 15,780 |
| DELETE | 26,661 | 12,137 | 23,459 |

Versioned (128B, 30s PUT + 3 versions):

| Operation | IOPS |
|-----------|------|
| PUT | 19,949 |
| PUT-OVERWRITE-VERSIONED | 18,713 |
| DELETE-VERSION | 18,100 |
| DELETE | 25,147 |

### Error observations

- `KVRGW_ERR_FDB_FUTURE_VERSION` (FDB 1009) occurs under sustained heavy write load at ~0.02% rate. All instances are retriable and succeed on retry. Pre-refactor, 1009 was misclassified as non-retriable and caused hard failures.
- 128B and 8KB tiers: 0 errors across all operations on clean FDB.
- 4KB tier under sustained overwrite load: occasional `FDB_FUTURE_VERSION` retries due to FDB storage server lag — benign under the new retry classification.

---

## Delete Single vs Delete-Multi Comparison

1M objects (8KB tier, STORAGE), c=128, clean FDB, direct perf driver (no S3/gRPC). `count=N` mode — no listing overhead.

| Mode | Ops | IOPS | Avg Latency | Keys/Commit |
|------|-----|------|-------------|-------------|
| delete (single) | 1,000,000 | 20,692 | 6,139 us/op | 1 |
| delete-multi (10/txn) | 1,000,000 | 54,006 | 21,994 us/batch | 10 |

**Delete-multi is 2.6x faster** — commit amortization (10 deletes per FDB transaction) with pipelined reads (B: + 10×S:O in one round-trip). Zero errors on both runs.
