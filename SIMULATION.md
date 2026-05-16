# Crimson SeaStore multi-OSD simulation

A self-contained harness for exercising the crimson SeaStore object store
under controllable, isolated multi-OSD conditions. The simulation lives
entirely under `qa/standalone/crimson/` and pairs with optional
`WITH_SEASTORE_WAF_COUNTERS` perf counters in `src/crimson/os/seastore/`.

The original purpose was Write Amplification (WAF) measurement, but the
same setup reproduces cleaner-saturation bugs (e.g. the leak fixed in
the upstream commit "crimson/os/seastore: fix cleaner space leak from
shadowed result list") and is generally useful as a deterministic
multi-OSD playground for crimson development.

## Architecture

```
  qa/standalone/crimson/
  ├── setup_osd_emul.sh     per-OSD backing-device provisioning
  ├── start_multi_osd.sh    end-to-end cluster bring-up
  ├── stop_multi_osd.sh     teardown
  ├── test_multi_osd.sh     fio randwrite driver with stall watchdog
  ├── waf_bench.fio         fio job file (rados ioengine)
  ├── run_waf_bench.sh      one-shot WAF benchmark orchestration
  ├── waf_report.py         WAF summary builder (asok + fio JSON)
  ├── waf_plot.py           optional matplotlib WAF-over-time plot
  └── test-waf-bench.sh     standalone CI-style WAF self-test
```

### Backing-device modes

`setup_osd_emul.sh` provisions one backing device per OSD. Two modes:

- **memory** (kernel `null_blk`) — fastest, but each instance is RAM-backed
  and limited to a few GiB before kernel allocation fails. Used for total
  cluster sizes ≤ ~8 GiB.
- **file** (`losetup` over a sparse file with 4 KiB sector size) — slower
  but scales to whatever the underlying filesystem can hold. Used for
  total cluster sizes > ~8 GiB.

The mode is auto-selected from total requested size; override with
`--backing=memory|file|auto`. Each OSD dir gets a `device_path` and
`backing_mode` marker so teardown knows what to release.

### Cluster bring-up

`start_multi_osd.sh <NUM_OSDS> <SIZE_GB> <BASE_DIR>` runs five stages
in order:

```
  0. cleanup     (always — kills leftover procs, removes null_blk /
                  loop devices and $BUILD/dev)
  1. preflight   (sanity-check no stale state remains after cleanup)
  2. devices     (setup_osd_emul.sh provisions N backing devices)
  3. vstart      (mon, mgr, N crimson-osds; wait up+active;
                  vstart output redirected to vstart.log)
  4. pool        (create configurable workload pool; default waf-test)
  5. balancer    (enable upmap balancer; wait osd df pg counts to
                  converge; --no-balancer skips this stage)
```

The cleanup at step 0 is unconditional — every invocation starts from
a clean slate. `stop_multi_osd.sh` is the standalone teardown helper
called by the test runners on exit.

### Per-OSD config

`crimson/osd` reads a per-OSD `[osd.N]` ceph.conf section so each OSD
opens its own backing device path. `vstart.sh` writes those sections
from `--seastore-devs`. The `--null-blk` flag only fills empty slots
to avoid clobbering explicit per-OSD device paths.

### Workload driver

`test_multi_osd.sh` drives fio (1 MiB block size by default, configurable
`--bs` / `--rw`) against the pool. It samples `seastore_waf` perf
counters via asok every `--period` seconds, runs a stall watchdog that
kills fio when counters stop advancing for `--stall-multi × --period`
seconds, and detects OSD crashes by combining `ceph status` with asok
responsiveness probes.

### WAF perf counters (optional)

When the binary is built with `-DWITH_SEASTORE_WAF_COUNTERS=ON` (default
ON for Debug+Crimson, OFF otherwise), SeaStore exposes:

- `l_seastore_bytes_user_written` — incremented per committed logical
  write with the user-visible payload size (deferred to the commit
  callback so retried-on-conflict transactions are not double-counted).
- `l_seastore_bytes_device_written` — incremented from `report_stats`
  with the per-shard device write delta.

A 10 s seastar timer emits a periodic `[WAF]` log line. Both counters
are visible through the OSD admin socket (`perfcounters_dump
seastore_waf`). When the option is OFF the symbols, the timer, the
per-write increment, and the report_stats hook all compile out — zero
runtime cost in production builds.

## Build

```sh
# Debug build with WAF counters (default-on):
cmake -B build -DWITH_CRIMSON=ON -DCMAKE_BUILD_TYPE=Debug
ninja -C build crimson-osd

# Release build without WAF counters:
cmake -B build -DWITH_CRIMSON=ON -DCMAKE_BUILD_TYPE=Release
ninja -C build crimson-osd

# Explicit override of the default:
cmake -B build -DWITH_CRIMSON=ON -DWITH_SEASTORE_WAF_COUNTERS=ON ...
```

## Command-line examples

### Canonical reproducer — 70%-full random write

The headline command for reproducing cleaner-saturation bugs. Brings up
a 2-OSD cluster with 32 GiB per OSD, then runs fio randwrite that
covers 70% of the cluster as its address space and writes 20× the
cluster size in total volume:

```sh
SIZE=64; OSDS=2
qa/standalone/crimson/start_multi_osd.sh --no-balancer $OSDS $((SIZE/OSDS)) build/dev && \
qa/standalone/crimson/test_multi_osd.sh --jobs 1 --size $((SIZE * 70 / 100))g --iosize $((SIZE * 20))g --rw randwrite
```

`start_multi_osd.sh` cleans up any prior run on its own, so the same
command can be re-run repeatedly with no manual teardown.

### WAF benchmark (small, end-to-end self-test)

The minimal CI-style self-test — brings up 2 OSDs (memory-backed), runs
a short bench, checks WAF is under the configured ceiling, tears down.

```sh
qa/standalone/crimson/test-waf-bench.sh --num-osds 2 --size-gb 2 --runtime 30 --waf-max 10
```

### WAF benchmark (full orchestration)

The reusable WAF measurement entry point. Each run produces a
`waf_report.txt` in the bench output dir.

```sh
qa/standalone/crimson/run_waf_bench.sh \
  --num-osds 2 --size-gb 32 --runtime 300 \
  --num-jobs 2 --bench-size 8g --bench-nrfiles 64
```

### File-backed mode for large clusters

For sizes above what `null_blk` can hold, force file-backed mode
explicitly. The launcher will create `$BASE_DIR/osdN/backing.img`
sparse files and bind them via `losetup` with 4 KiB sector size:

```sh
qa/standalone/crimson/start_multi_osd.sh --backing=file 4 64 build/dev
```

### Inspecting WAF perf counters live

While a cluster is up:

```sh
build/bin/ceph -c build/ceph.conf tell osd.0 perfcounters_dump seastore_waf
build/bin/ceph -c build/ceph.conf tell osd.1 perfcounters_dump seastore_waf
```

### Teardown

Cleanup happens automatically at the start of every
`start_multi_osd.sh` invocation. To tear down manually:

```sh
qa/standalone/crimson/stop_multi_osd.sh
```
