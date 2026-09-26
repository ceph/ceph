# Multi-Instance Perf Test Plan

## Goal

Determine whether the 25K IOPS ceiling (batch=10, 16 workers, PerfDataStore) is client-side or FDB cluster-side. Run 3 independent backend instances against the same FDB cluster, each writing to separate buckets. If the bottleneck is client-side, aggregate should approach 75K IOPS.

## Setup

- FDB cluster: already running (6 SS, 3 logs, 3 stateless)
- `reload.sh --clean --perf` to wipe FDB and build
- PerfDataStore (zero disk latency) on all instances
- Tier config: max_inline=256, max_kv_store=4096
- Batch: size=10, timeout=1000us, threads=16

## Instances

| Instance | Socket | Data dir | Bucket | Push threads |
|----------|--------|----------|--------|-------------|
| 0 | /tmp/kvrgw-perf-0.sock | data-0 | perf-bucket-0 | c=128 |
| 1 | /tmp/kvrgw-perf-1.sock | data-1 | perf-bucket-1 | c=128 |
| 2 | /tmp/kvrgw-perf-2.sock | data-2 | perf-bucket-2 | c=128 |

Each instance gets its own rgw_id (from transactional L:N counter). No key conflicts — each writes to its own bucket with unique thread-scoped object names.

## Test Sequence

1. `reload.sh --clean --perf` — wipe FDB, build, wait healthy
2. Start FDB metrics collector (2s interval)
3. Launch 3 instances in parallel, each piped:
   ```
   create-buckets buckets=1
   set-batch size=10 timeout=1000 threads=16
   set-sim-disk-write-us 0
   put c=128 tiers=8192 duration=120
   batch-stats
   quit
   ```
4. Wait for all 3 to complete
5. Stop metrics collector
6. Parse per-instance IOPS + batch-stats
7. Parse FDB time-series (CPU, txn/sec, queue, durability lag, limited_by)

## Duration

120 seconds per run. Longer than 60s to capture steady-state and observe any FDB degradation over time.

## Metrics to Capture

### Per-instance (from perf driver output)
- IOPS
- avg_total_us
- avg_batch_size
- avg_queue_size
- avg_wait_us
- errors / retries

### FDB cluster (from metrics collector)
- txn committed/sec (expect ~15K if 3×5K)
- reads/sec, writes/sec
- ss_cpu avg/max (expect ~0.90 if linear scaling)
- log_cpu max
- proxy_cpu max (commit proxy — most likely bottleneck)
- ss_disk_busy
- durability_lag_seconds
- queue_MB (growth = FDB falling behind)
- limited_by (will change from "workload" to actual bottleneck name)
- conflict rate (should stay 0 — separate buckets)

## Expected Outcomes

### Scenario A: Client-side bottleneck (likely)
- Aggregate ~65-75K IOPS (near-linear scaling)
- FDB CPU climbs but no single component saturates
- queue_MB stable
- limited_by stays "workload"

### Scenario B: FDB bottleneck — commit proxy
- Aggregate plateaus at ~50-60K IOPS
- proxy_cpu_max approaches 1.0
- limited_by changes to "commit_proxy"
- Individual instance IOPS drops below 25K

### Scenario C: FDB bottleneck — storage server
- ss_cpu_max hits 0.95
- queue_MB grows unbounded
- limited_by changes to "storage_server_write_queue"
- durability_lag increases

## Output

- `perf-results/multi-instance/instance-{0,1,2}.log` — per-instance perf output
- `perf-results/multi-instance/metrics/` — FDB continuous metrics
- `perf-results/multi-instance/fdb_timeseries.txt` — parsed time-series
