# Performance Testing

## Overview

The perf driver is an interactive CLI built into the backend binary. The `put` command calls `put_object_route()` — the same shared entry point as the production gRPC `PutObject` handler, ensuring batch mode, tier selection, and all FDB operations are identical. GET and DELETE commands call internal methods (`load_object_with_data`, `delete_single`) directly via friend access, bypassing the gRPC handler layer but using the same FDB transaction logic.

## Setup

### 1. Build with perf mode

```bash
./scripts/reload.sh --clean --perf
```

This wipes FDB state, rebuilds the binary, waits for FDB healthy, then exits. No frontend, nginx, or test suite. The `--clean` flag is recommended for baseline benchmarks. Omit it to keep existing FDB data.

### 2. Launch the driver

```bash
FDB_CLUSTER_FILE=.fdb/fdb.cluster \
  KVRGW_MAX_KV_STORE=4096 \
  KVRGW_MAX_INLINE=256 \
  ./build/kv-rgw-backend --perf /tmp/kvrgw-perf.sock data
```

In `--perf` mode, the backend uses `PerfDataStore` instead of `FileDataStore`. `PerfDataStore` performs no disk I/O — writes and reads are no-ops (or configurable sleeps via `set-sim-disk-write-us` / `set-sim-disk-read-us`). This isolates FDB and batch queue performance from filesystem variance.

The driver resolves (or auto-creates) the tenant and enters an interactive prompt:

```
=== KV-RGW Perf Driver (interactive) ===
tenant=kv-poc
Commands: create-buckets, put, put-multi, get, delete, delete-multi,
          delete-buckets, list-buckets, list-objects, set-batch, quit
Example: put c=128 tiers=128,4096,8192 duration=300

perf>
```

## Shared Code Architecture

The perf driver uses `put_object_route()` -- the same entry point as the gRPC `PutObject` handler. This ensures batch mode, tier selection, overwrite handling, and all FDB operations are identical between perf and production.

```
gRPC PutObject handler
  └─> put_object_route(PutObjectRequest, data, data_len)
        ├── batch_size > 1 → BatchCommitQueue
        └── batch_size == 1 → select_storage_tier / put_object_single_txn

perf driver put_worker
  └─> put_object_route(PutObjectRequest, data, data_len)
        └── (same routing)
```

GET and DELETE workers call `load_object_with_data()` and `delete_single()` directly.

## DataStore Abstraction

The backend uses a `DataStore` abstract interface for blob storage. Two implementations:

| Implementation | Used when | Behavior |
|---|---|---|
| `FileDataStore` | Normal mode (no `--perf`) | Real file I/O to `KVRGW_DATA` directory |
| `PerfDataStore` | `--perf` mode | No disk I/O; configurable sleep per read/write |

`PerfDataStore` allows isolating FDB transaction performance from disk I/O variance. Simulated latency is configured at runtime via `set-sim-disk-write-us` and `set-sim-disk-read-us` commands (see below). Both implementations share the same `DataStore` virtual interface — all service code (PUT, GET, GC) is unaware of which backend is active.

## Commands

### Common Parameters

Parameters vary by command:

| Parameter | Used by | Default | Description |
|-----------|---------|---------|-------------|
| `c=N` | put, put-multi, get, delete, delete-multi, copy, put-overwrite, put-overwrite-versioned, delete-version | 64 | Concurrency (threads) |
| `duration=N` | put, put-multi | 30 | Run time in seconds (mutually exclusive with count=) |
| `count=N` | put, delete, delete-multi | 0 | Exact object count (mutually exclusive with duration=). For delete: generates keys directly without listing. |
| `tiers=S1,S2,...` | put, put-multi, put-overwrite, put-overwrite-versioned | (required) | Object sizes in bytes (determines tier) |
| `buckets=N` | create-buckets | 1 | Number of buckets to create |
| `mode=X` | create-buckets | none | Versioning mode: `none`, `versioned`, `suspended` |
| `versions=N` | put-overwrite-versioned, delete-version | (required) | Number of versions per key |
| `burst=N` | put | 1 | Objects sent to current bucket before rotating to next |

Note: `get`, `copy`, `put-overwrite`, `put-overwrite-versioned`, `delete-version`, `list-objects`, `list-buckets`, `delete-buckets` operate on all existing buckets/objects — they do not take `duration=` or `count=`. The `delete` and `delete-multi` commands support `count=N` for direct key generation (no listing) when the key pattern matches the PUT output.

### create-buckets

Create test buckets with optional versioning mode.

```
perf> create-buckets count=4 mode=none
```

### put

PUT objects using `put_object_route()`. Routes through batch queue when `batch_size > 1`.

```
perf> put c=128 tiers=128,4096,8192 duration=300
```

Each tier value is an object size in bytes. Objects are distributed round-robin across buckets using burst rotation. Object names are `perf/<thread_id>/<instance_id>_<seq>` (e.g. `perf/0000/02_000000000042`). The instance ID is set via `KVRGW_INSTANCE_ID` env var, ensuring multi-instance runs produce unique keys.

Each thread maintains per-bucket sequence counters starting at 0, growing contiguously. After the timed test, a padding phase aligns all threads and buckets to a global max sequence, producing a uniform layout: `total_objects = threads × global_max_seq × buckets`. No shared atomic counters are used on the PUT hot path.

The `burst=N` parameter controls bucket rotation: with `burst=1` (default), every object goes to the next bucket. With `burst=5`, 5 consecutive objects go to the same bucket before rotating. This reduces per-batch B: verification reads when using multiple buckets.

### put-multi

Direct 10-objects-per-transaction PUT without the batch queue. Each thread opens its own FDB transaction, calls `put_object_in_txn()` 10 times, then commits. Intended to measure theoretical maximum FDB batching throughput (no queue coordination overhead).

```
perf> put-multi c=64 tiers=128 duration=60
```

### get

List all buckets, list all objects in each bucket, then GET every object. Runs to completion (not time-bounded).

```
perf> get c=64
```

### delete / delete-multi

List all objects, then delete them. `delete` uses one FDB transaction per object; `delete-multi` batches 10 keys per transaction with c=N parallel workers. Both run to completion.

```
perf> delete c=64
perf> delete-multi c=64
```

### copy

Copy every pre-populated object within the same bucket (`obj_X` → `COPY_X`). One copy per object, runs to completion. Pre-populate with `put` first.

```
perf> copy c=128
```

### put-overwrite

Overwrite every pre-populated object once with new data. Exercises the `displace_old_object` / `move_object_to_g` path. Runs to completion.

```
perf> put-overwrite c=128 tiers=128
```

### put-overwrite-versioned

Overwrite every pre-populated object N times on a versioned bucket, creating N versions per key. Bucket must be created with `create-buckets mode=versioned`. Runs to completion.

```
perf> put-overwrite-versioned c=128 tiers=128 versions=3
```

### delete-version

Delete one random version per key. Must run immediately after `put-overwrite-versioned` — no other commands in between. Each thread picks a random version index `r` in `[0, versions)` and deletes version `0xFFFFFFFE - r`. Runs to completion.

```
perf> delete-version c=128 versions=3
```

### delete-buckets

Remove test buckets (must be empty).

```
perf> delete-buckets
```

### list-buckets

List all buckets with timing.

```
perf> list-buckets
```

### list-objects

List objects per bucket with timing.

```
perf> list-objects
```

### list-test

Post-test verification: lists all expected objects per bucket using `service.ListObjects()` (1000 keys per page), verifies each key matches the expected key scheme in lexicographic order, and profiles per-page latency.

Keys are generated on the fly using `KeyIterator` — a zero-allocation iterator with a fixed stack buffer that produces keys in `<prefix>/<thread_id>/<instance_id>_<seq>` order.

Parameters are read from the metadata file (`KVRGW_METADATA_FILE`): prefix, threads, instances, per-instance max_seq.

```
perf> list-test
perf> list-test --blind
perf> list-test --max-pages=100
perf> list-test --blind --max-pages=50
perf> list-test --progress=200
perf> list-test --quiet
```

- Default: validate keys + time pages. Fail-fast on mismatch.
- `--blind`: time pages only, skip key validation. Pure listing throughput benchmark.
- `--max-pages=N`: stop after N pages (N × 1000 keys). For quick profiling.
- `--progress=N`: print a progress line every N pages (default 100) to stderr. Format: `progress: pages=XXXX.XK keys=XXXX.XM elapsed=XXXX.Xs (cumul keys/s, pages/s) interval: keys/s`. Pages always in K, keys always in M, elapsed in seconds — all right-aligned fixed-width. Rates are raw numbers. Interval rate covers the period since the previous progress line.
- `--quiet`: suppress progress lines on stderr.
- `--ryw-cache=enabled|disabled`: control the FDB client-side Read-Your-Writes cache for listing scans. Default is `disabled` (optimized — skips pointless cache allocation on forward-only scans). Set to `enabled` to restore the old behavior for A/B comparison. See `KvStore::range_scan` docs in `cpp-data-structures.md`.

### run_list_test.sh

Standalone script to run `list-test` against a completed test's results directory. Reads `test_metadata.txt`, launches a perf driver with the correct prefix and metadata, runs the verification, and exits.

```bash
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/ --blind
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/ --blind --max-pages=100
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/ --progress=200
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/ --quiet
./scripts/run_list_test.sh perf-results/FDB-Config1_Perf-Test1_20260831_090000/ --ryw-cache=enabled
```

No dependency on the original test run — just point at any results directory with a valid `test_metadata.txt`. FDB must still be running.

### Key Prefix

Each test run generates a unique prefix: SHA-256 of `<Perf-Test-name>:<FDB-Config-name>:<timestamp>`, last 6 bytes as 12 hex chars. Passed via `KVRGW_KEY_PREFIX` env var. Enables multiple test runs without FDB clean.

### Metadata File

Created in results directory: `test_metadata.txt`. Contains prefix, instances, threads, buckets, tiers, burst, duration, and per-instance `max_seq_<id>=<value>`. Used by `list-test` for verification parameters.

### set-batch

Change batch config at runtime. Immediately restarts the batch queue.

```
perf> set-batch size=32 timeout=500 threads=8
```

### set-sim-disk-write-us / set-sim-disk-read-us

Configure simulated disk latency per operation (perf mode only). Default is 0 (no sleep, instant return).

```
perf> set-sim-disk-write-us 1000
sim_disk_write_us=1000
perf> set-sim-disk-read-us 500
sim_disk_read_us=500
perf> get-sim-disk
sim_disk_write_us=1000 sim_disk_read_us=500
```

Use `set-sim-disk-write-us 0` to measure pure FDB/batch overhead with zero disk cost. Use realistic values (e.g. 1000 for local NVMe, 5000 for NFS) to model specific storage backends.

Changes take effect immediately (atomic store) — can be adjusted between or during test runs.

### batch-stats

Print batch statistics and reset counters.

```
perf> batch-stats
```

Output:

```
batch_commits=1234 entries_batched=39488 conflict_pushbacks=12
avg_batch_size=32.0 min_batch_size=8 max_batch_size=32
avg_queue_size=45.2 avg_wait_us=450 min_wait_us=23 max_wait_us=1002
txn_retries=3 txn_hard_failures=0 txn_max_retries_exceeded=0
```

| Field | Description |
|-------|-------------|
| `batch_commits` | Number of FDB transactions committed by batch threads |
| `entries_batched` | Total PUT entries committed across all batches |
| `conflict_pushbacks` | Duplicate object keys deferred to back of deque |
| `avg/min/max_batch_size` | Entries per commit transaction |
| `avg_queue_size` | Average deque depth when a worker extracts entries (backpressure indicator) |
| `avg/min/max_wait_us` | Time from oldest entry enqueue to commit start |
| `txn_retries` | FDB conflict retries (error 1020) across all operations |
| `txn_hard_failures` | Non-retriable FDB errors |
| `txn_max_retries_exceeded` | Transactions that exhausted all retry attempts |

Counters reset after each `batch-stats` call.

### Admin Socket: Error Stats

Query per-error-code counters from the backend via the admin socket:

```bash
echo "get-error-stats" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-0.sock
echo "reset-error-stats" | socat - UNIX-CONNECT:/tmp/kvrgw-admin-0.sock
```

Output is a sparse list of `KvrgwErrorCode_Name=count` pairs (non-zero only). `reset-error-stats` dumps the current values then zeros all counters.

## Batch Mode Testing

Typical workflow:

```
perf> set-batch size=32 timeout=1000 threads=8
perf> put c=128 tiers=128,4096 duration=60
perf> batch-stats
perf> set-batch size=1
perf> put c=128 tiers=128,4096 duration=60
```

Compare IOPS and latency between batched and non-batched runs at the same concurrency and tier mix.

## Overwrite / Copy / Version Benchmark Workflow

```
perf> create-buckets count=1
perf> put c=128 tiers=128 duration=30
perf> put-overwrite c=128 tiers=128
perf> copy c=128
perf> delete c=128
perf> delete-buckets

perf> create-buckets count=1 mode=versioned
perf> put c=128 tiers=128 duration=30
perf> put-overwrite-versioned c=128 tiers=128 versions=3
perf> delete-version c=128 versions=3
perf> delete c=128
perf> delete-buckets
```

## Delete Single vs Multi Comparison Workflow

Uses `count=N` for exact object counts and list-free deletion:

```
perf> create-buckets count=1
perf> put c=128 tiers=8192 count=1000000
perf> delete c=128 count=1000000
perf> delete-buckets

perf> create-buckets count=1
perf> put c=128 tiers=8192 count=1000000
perf> delete-multi c=128 count=1000000
perf> delete-buckets
```

`count=` mode generates keys directly (`perf/<thread_id>/<seq>`) without listing — eliminates range scan noise from the measurement. Requires the same `c=` value as the PUT so thread_id ranges match.

## Output Format

After each `put` / `get` / `delete` run, the driver prints:

```
=== PUT 8KB ===
  ops=123456 errors=0 retries=3 IOPS=4115 elapsed=30.0s
  Latency breakdown:
    PutObject count=123456 avg_total_us=243 avg_fdb_us=198 fdb_pct=81.5% avg_disk_us=0 disk_pct=0.0% avg_get_us=45 avg_commit_us=120
```

When errors or retries are non-zero, a per-error-code breakdown is printed:

```
  Error breakdown: KVRGW_ERR_FDB_FUTURE_VERSION=3 KVRGW_ERR_FDB_CONFLICT=1
```

- `ops` -- successful operations
- `errors` -- non-retriable failures (permanent error codes 300+, 400+)
- `retries` -- retriable errors that were retried (200–206)
- `avg_total_us` -- wall-clock time per operation
- `avg_fdb_us` -- time inside FDB calls (get + put + del + commit + scan)
- `fdb_pct` -- FDB time as percentage of total
- `avg_disk_us` -- DataStore I/O time (STORAGE tier only)
- `avg_get_us` / `avg_commit_us` -- FDB sub-operation breakdown

Error classification: `BenchResult` uses `err_counts[KvrgwErrorCode_ARRAYSIZE]` with `record_error(KvrgwErrorCode)`. Each error is counted by its specific proto enum name. Retriable errors (200–206) increment both `retries` and their specific counter; non-retriable errors increment `errors`.

## Admin Socket: Live Stats (hz-style)

The admin socket supports live polling during a running `put` command. The admin server thread runs independently of the perf driver stdin, so stats can be queried while a test is in progress.

| Command | Response | Notes |
|---------|----------|-------|
| `get-batch-stats` | Cumulative batch counters | No reset on read |
| `get-latency` | Per-op-type latency breakdown | Cumulative |
| `get-error-stats` | Per-error-code counters | Cumulative |
| `reset-error-stats` | Dumps + zeros error counters | |
| `reset-latency` | Zeros latency counters | |

Batch stats are cumulative — the external collector diffs consecutive samples to compute per-interval rates (entries/sec, commits/sec, avg wait per interval). FDB `status json` provides instantaneous `hz` rates natively.

In `--perf` mode, the admin socket path is set via `KVRGW_ADMIN_SOCKET` env var (e.g., `/tmp/kvrgw-admin-perf.sock`). For multi-instance tests, each instance uses a separate socket.

## Raw Data File Format

Every test run produces a self-describing raw data file. The file contains all information needed to understand, reproduce, and compare the test.

### Header

Lines prefixed with `#`. Machine-parseable key-value pairs — no free text. Complete enough to reproduce the test without human memory.

```
# test_type: PUT
# test_start: 2026-08-26T06:30:00Z
# test_duration_sec: 120
# object_size_bytes: 8192
# tier: STORAGE
# data_store: PerfDataStore
# sim_disk_write_us: 0
# sim_disk_read_us: 0
# rgw_instances: 3
# rgw_producer_threads: 128
# rgw_batch_size: 10
# rgw_batch_timeout_us: 1000
# rgw_batch_threads: 16
# buckets_per_instance: 1
# fdb_storage_servers: 6
# fdb_log_servers: 3
# fdb_stateless: 3
# fdb_cores_per_ss: 1
# fdb_memory_per_process: 1GiB
# fdb_cache_memory: 256MiB
# fdb_redundancy: triple
# fdb_storage_engine: ssd-2
# sample_interval_sec: 2
# columns: timestamp,elapsed_s,fdb_txn_committed_hz,...,i0_entries_hz,...
```

### Data rows

Fixed CSV columns, one row per sample interval. Every row has the same fields in the same order. Missing values use `0` or `-1`, never empty. Parseable with `awk`, `pandas.read_csv(comment='#')`, or any CSV tool.

### Reproducibility

The header is a test specification. A replay script can parse it and reconstruct the exact test:
1. Read config from header (`# rgw_instances`, `# rgw_batch_size`, etc.)
2. Generate perf driver commands
3. Set env vars, launch instances, collect with same sample interval
4. Output a new raw file

After a code change, replay the same test and diff the data rows to detect regressions.

### Data mining

Across hundreds of raw files:
- `grep "^# test_type: PUT" *.csv` — find all PUT tests
- `grep "^# fdb_storage_servers: 12" *.csv` — find all 12-SS runs
- `grep "^# rgw_batch_size: 10" *.csv | xargs ...` — aggregate all batch=10 results
- Files are self-contained — no external metadata needed

File naming: `perf-results/csv/raw_<timestamp>.csv`

## FDB Cluster Configuration System

The FDB cluster is configured through a three-file system:

- **`System-Resources.md`** — one-time machine inventory: CPU cores, NVMe drives, RAM, and hard limits on what FDB may use (max 48 cores, max 48 GB DRAM). Created once, updated only when hardware changes. Must be approved before use.
- **`FDB-Default-Config.txt`** — human-editable intent file declaring role counts and resources per container (e.g. 6 SS at 1 core/1 GB each). Edit this to change FDB topology for different test profiles.
- **`scripts/gen_fdb_config.sh`** — reads both files, validates the config against resource limits, assigns actual core IDs and NVMe mount points, and outputs a fully resolved `FDB-Default-Config.md`.

Storage-server count must be one of {1, 3, 6, 9, 12} — SS processes are distributed round-robin across the 3 NVMe drives. Any other count is rejected. Port ranges: SS 4500-4511, LOG 4520-4522, SL 4530-4532.

`scripts/gen_docker_compose.sh` reads the generated `.md` and produces `fdb-cluster/docker-compose.yml`. Every parameter is binding: container name, port, cpuset, memory, mount, zone, machine ID, FDB `-m` and `--cache_memory` flags. Mount paths are validated against `System-Resources.md` allowlist.

`FDB-Config.txt` also supports FDB cluster settings:

```
throttle: enable
storage_hard_limit_mb: 500
```

- `throttle`: `enable` or `disable` (default: disable). Enables FDB automatic per-tag transaction throttling.
- `storage_hard_limit_mb`: positive integer in MB (default: 1500). Sets `--knob_storage_hard_limit_bytes` on SS processes — the cluster stops accepting writes when any SS queue exceeds this. Lower values (e.g. 500) prevent deep queue buildup under heavy load.

The runner applies `throttle enable` via fdbcli after cluster init when configured.

To create additional test profiles, copy `FDB-Default-Config.txt` to a new name (e.g. `FDB-12SS-Config.txt`), edit the counts/resources, and run:

```bash
./scripts/gen_fdb_config.sh FDB-12SS-Config.txt
```

This produces `FDB-12SS-Config.md` with the resolved mappings. `FDB-Default-Config.md` is used by the reload/load scripts unless another config is explicitly specified.

## Perf Test Definition System

A perf test is fully specified by a definition file (`Perf-Test-XXX.txt`) that references an FDB cluster config. The flow:

```
FDB-Config-XXX.txt → gen_fdb_config.sh → FDB-Config-XXX.md ─┐
                                                              ├→ run_perf_from_def.sh → results/
Perf-Test-XXX.txt  → gen_perf_test.sh  → Perf-Test-XXX.md ──┘         │
                                                                       ↓
                                                         display_perf_results.sh
```

### Perf Test Definition Format

`Perf-Test-XXX.txt` contains key-value pairs:

Lines starting with `#` are comments. Trailing whitespace on values is ignored.

```
instances: 3
mode: perf
clean: yes
batch_size: 10
batch_timeout_us: 1000
batch_threads: 8
concurrency: 128
buckets: 16
burst: 5
tiers: 8192
duration: 120
version_state: none
sim_disk_write_us: 0
sim_disk_read_us: 0
sample_interval: 2
fdb_stats: full
kvrgw_stats: full
```

- `mode`: `perf` (PerfDataStore, no disk I/O) or `normal` (FileDataStore)
- `clean`: `yes` (wipe FDB before test) or `no`
- `version_state`: `none`, `versioned`, or `suspended` (required) — bucket versioning mode passed to `create-buckets`
- `duration` and `count` are mutually exclusive
- `fdb_stats` / `kvrgw_stats`: `full` or `none`

### Stats Macros

**`fdb_stats: full`** collects:
- Transaction rates (txn/sec, reads/sec, writes/sec, conflict/sec)
- Per-process CPU utilization (SS, log, proxy)
- Per-process memory usage (RSS, FDB internal)
- Disk latency (read/write per SS)
- Log queue size / durability lag
- Log growth (current size, delta KB/MB, delta %)
- Storage queue size
- Data distribution (bytes stored per SS, key-range balance)

**`kvrgw_stats: full`** collects per instance:
- Latency per op type: PutObject, GetObject, HeadObject, DeleteObject, DeleteMulti, DeleteBucket, CreateBucket, ListBuckets, ListObjects, ListObjVersions, DeleteObjVersion, CopyObject, PutObjTagging, GetObjTagging, DelObjTagging, BucketExists, PutBucketPolicy, GetBucketPolicy, DelBucketPolicy, PutBucketVer, GetBucketVer (each with count, avg_total_us, avg_fdb_us, fdb_pct, sub-op breakdown)
- Batch stats: batch_commits, entries_batched, conflict_pushbacks, avg/min/max batch_size, avg queue_size, avg/min/max wait_us
- Transaction health: txn_retries, txn_hard_failures, txn_max_retries_exceeded
- Error stats: per-error-code counters (non-zero only)
- Host stats: per-process CPU/IO (pidstat), disk utilization (iostat), process memory

### gen_perf_test.sh

Validates a `Perf-Test-XXX.txt` (required fields, types, mutual exclusivity, referenced FDB config exists), expands macros, and generates `Perf-Test-XXX.md`.

```bash
./scripts/gen_perf_test.sh Perf-Test-Default.txt
```

### run_perf_from_def.sh

Executes a test from the generated `.md` files. Both arguments are required. Reloads FDB if `clean: yes`, launches instances, starts stats collectors, runs workload, writes results.

```bash
./scripts/run_perf_from_def.sh Perf-Test-Default.md FDB-Default-Config.md
```

### Results Directory

Output directory combines both config names:

```
perf-results/<fdb-config-name>_<perf-test-name>_<timestamp>/
  instance-00_<timestamp>.log       # perf driver stdout/stderr
  instance-00_<timestamp>.stats     # per-interval KVRGW stats
  instance-01_<timestamp>.log
  instance-01_<timestamp>.stats
  ...
  fdb_stats_<timestamp>.log         # per-interval FDB stats
  host_stats_<timestamp>.log        # pidstat, iostat, memory
  Perf-Test-XXX.md                  # copy of test definition
  FDB-Config-XXX.md                 # copy of cluster config
```

### display_perf_results.sh

Analyze results with selectors and deviation threshold:

```bash
./scripts/display_perf_results.sh perf-results/<dir>/ HEALTH
./scripts/display_perf_results.sh perf-results/<dir>/ KVRGW SS --deviation 15
```

Selectors:

- `HEALTH` — FDB cluster health summary. One line per server showing input/durable ratio, data lag, CPU, and flags (`[OK]`, `[HOTSPOT]`, `[SATURATED]`, `[FALLING_BEHIND]`, `[DRAINING]`, `[IDLE]`). `[HOTSPOT]` requires both `in/dur > 2.0x` and `cpu > 0.25` — filters out FDB heartbeat noise during drain. Shows LOAD and DRAIN phases. Drain is detected post-hoc by the display script from SS `input_hz` data (2 consecutive zero-input samples after active phase) — no runtime overhead, no collector-side detection. Modes:
  - Default: last 2 samples of LOAD + last 2 of DRAIN
  - `--long`: exponential timeline (1s, 2s, 4s, 8s...) + last 2 before drain
  - `--long --compact`: same but aggregated by role
  - `--linear --interval N`: every Nth sample + last 2 before drain
  - All modes: if a selected sample has no data (fdbcli timeout), scans forward to next valid sample
- `KVRGW` — per-instance table: IOPS, latency, wait_us, queue_size, errors. Uses record-based parsing with state machine filtering (IDLE→RAMPUP→ACTIVE→DRAIN). Only ACTIVE phase samples with nonzero IOPS are included in stats — startup warmup and collection artifacts are excluded. Use `--warmup N` to set rampup samples (default 3). Use `--long` for full breakdown.
- `SS` — per-storage-server: cpu, mem, input/durable rates, data lag, stored bytes. Filters to ACTIVE phase (skips warmup). Use `--long` for full rate + accumulation tables with LOAD/DRAIN phases.
- `LOG` — per-log-server: cpu, mem, input/durable rates, queue disk/mem. Filters to ACTIVE phase.
- `SL` — per-stateless-server: cpu, mem. Plus cluster QoS. Filters to ACTIVE phase.
- `FDB` — gnuplot graphs for Storage Queues & Durability, TLog Queue Growth, Memory & Disk Headroom.

`HEALTH` is the recommended first-run diagnostic after a test.

## Scripts

### run_perf_test.sh

Configurable test runner. Reloads FDB (unless `--no-reload`), launches N instances, collects stats every `--sample-interval` seconds, outputs a raw CSV.

```bash
./scripts/run_perf_test.sh \
  --description "baseline 6SS" \
  --instances 3 \
  --concurrency 128 \
  --batch-size 10 \
  --object-size 8192 \
  --duration 120
```

All parameters:

| Flag | Default | Description |
|------|---------|-------------|
| `--description TEXT` | (empty) | Free-text label for this run |
| `--instances N` | 1 | RGW backend instances |
| `--concurrency N` | 128 | Producer threads per instance |
| `--batch-size N` | 10 | Batch queue size |
| `--batch-timeout N` | 1000 | Batch timeout (us) |
| `--batch-threads N` | 16 | Batch worker threads |
| `--object-size N` | 8192 | Object size (bytes) |
| `--buckets N` | 1 | Buckets per instance |
| `--duration N` | 60 | Test duration (seconds) |
| `--sim-write-us N` | 0 | Simulated disk write latency (us) |
| `--sim-read-us N` | 0 | Simulated disk read latency (us) |
| `--sample-interval N` | 2 | Stats sampling interval (seconds) |
| `--output-dir DIR` | perf-results/csv | Output directory |
| `--no-reload` | (reload) | Skip `reload.sh --clean --perf` |

### replay_test.sh

Reproduce a test from a raw file's header. Parses all config from the `# key: value` lines and calls `run_perf_test.sh` with matching parameters.

```bash
./scripts/replay_test.sh perf-results/csv/raw_20260826_063000.csv "after async refactor"
```

Adds `# based_on:` to the new file's header for traceability.

### display_test.sh

Human-readable summary of a raw file: config, final results (IOPS, errors, batch stats), and time-series min/avg/max for FDB and per-instance metrics including per-interval latency.

```bash
./scripts/display_test.sh perf-results/csv/raw_20260826_063000.csv
```

### compare_runs.sh

Side-by-side comparison of two runs. Shows config differences, aggregate metrics with delta percentages, and final results from both.

```bash
./scripts/compare_runs.sh perf-results/csv/raw_baseline.csv perf-results/csv/raw_after_change.csv
```

### Per-interval metrics collected

Each sample row includes:

| Source | Fields |
|--------|--------|
| FDB status json | txn_hz, reads_hz, writes_hz, conflict_hz, ss_cpu_avg, ss_cpu_max, log_cpu_max, proxy_cpu_max, queue_mb, durability_lag_s |
| Admin get-batch-stats | entries_hz, commits_hz, interval_wait_us, interval_queue_size |
| Admin get-latency | interval_latency_us (per-interval end-to-end PUT latency) |
| Admin get-error-stats | error count |

FDB fields are instantaneous `hz` rates. KVRGW fields are per-interval deltas computed from cumulative counters.

## FDB Metrics Collection

### External collector (continuous sampling)

Run in a separate terminal before starting the test:

```bash
mkdir -p perf-results/my-test/metrics
scripts/collect_fdb_metrics.sh perf-results/my-test/metrics 2
```

This samples every 2 seconds: FDB status JSON (CPU, memory, txn rates per process), pidstat CPU/IO, iostat, memory. Kill after the test completes (Ctrl-C or `kill`).

Files produced:
- `fdb_status.log` — FDB self-reported metrics (CPU per storage/log/proxy, txn/sec, durability lag)
- `pidstat_cpu.log` / `pidstat_io.log` — per-process host CPU/IO
- `iostat.log` — disk utilization
- `memory.log` — process memory

### Latency stats (shutdown dump)

On shutdown or SIGINT, the backend dumps a full latency breakdown to stderr via `LatencyStats::dump()`. This includes per-operation-type counters and FDB sub-operation timing (get, put, del, commit, scan).

Transaction failure stats (`txn_retries`, `txn_hard_failures`, `txn_max_retries_exceeded`) are accumulated across all operations (batch and non-batch) and reported in `batch-stats` output and the shutdown dump.
