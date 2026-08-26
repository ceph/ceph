# Crimson vs Classic Scrub Test Scripts

Comparison between `osd-scrub-test.sh` (classic) and `osd-scrub-test-crimson.sh` (crimson).

---

## Test Coverage

| Test | Classic | Crimson | Notes |
|------|---------|---------|-------|
| `TEST_scrub_test` | ✅ | ✅ | Identical logic |
| `TEST_interval_changes` | ✅ | ✅ | Identical logic |
| `TEST_scrub_abort` | ✅ | ✅ | Different scrub-start detection (see §4) |
| `TEST_deep_scrub_abort` | ✅ | ✅ | Different scrub-start detection (see §4) |
| `TEST_scrub_permit_time` | ✅ | ✅ | Identical logic |
| `TEST_pg_dump_objects_scrubbed` | ✅ | ✅ | Identical logic |
| `TEST_dump_scrub_schedule` | ✅ | ✅ | Different config-set method (see §3) |
| `TEST_just_deep_scrubs` | ✅ | ✅ | Different cluster setup (see §6) |
| `TEST_abort_periodic_for_operator` | ✅ | ✅ | Different config-set + reservations dump (see §7) |
| `NO_scrub_extended_sleep` | ✅ (disabled) | ❌ | BlueStore-only feature; omitted in crimson |

---

## Differences

### 1. OSD Backend and Startup

- Classic uses `run_osd` with BlueStore (default objectstore).
- Crimson uses `run_crimson_osd` with `--osd_objectstore=seastore`.
- Crimson calls `apply_crimson_config` after `run_mon` to inject required mon/osd
  config options (e.g. `mon_allow_pool_size_one`, `osd_scrub_load_threshold`).
- Crimson adds to `CEPH_ARGS`:
  ```
  --ms-bind-msgr2=true --ms-bind-msgr1=false
  --osd_pool_default_crimson=true
  --osd_pool_default_pg_autoscale_mode=off
  ```

### 2. `run()` Loop — Test Error Handling

| Classic | Crimson |
|---------|---------|
| First failure calls `return 1` immediately | Collects all pass/fail results, continues remaining tests, prints a summary table |

### 3. `ceph tell` vs `ceph pg` / `ceph config set`

Crimson does not support `ceph tell <pgid>` for scrub scheduling or
`ceph tell osd.* config set` for global config changes.

| Classic | Crimson |
|---------|---------|
| `ceph tell $pgid schedule-scrub` | `ceph pg $pgid schedule-scrub` |
| `ceph tell $pgid schedule-deep-scrub` | `ceph pg $pgid schedule-deep-scrub` |
| `ceph tell $pgid deep-scrub` | `ceph pg deep-scrub $pgid` |
| `ceph tell osd.* config set osd_X Y` | `ceph config set osd osd_X Y` |

### 4. `_scrub_abort` — Scrub-Start Detection

Classic polls the monitor's `pg dump` which can lag behind:
```bash
ceph pg dump pgs | grep ^$pgid | grep -q "scrubbing"
```

Crimson queries the primary OSD directly to avoid stat-propagation delays:
```bash
ceph tell osd.$primary pg $pgid query | jq -r '.state' | grep -q "scrubbing"
```

Crimson also passes extra retry flags to the OSD so blocked scrubs auto-retry:
```
--osd_scrub_retry_after_noscrub=1
--osd_scrub_retry_pg_state=2
--osd_scrub_retry_delay=2
```

### 5. `wait_initial_scrubs` — Completion Detection

Classic relies on `last_scrub_duration` from `pg dump` with a 20 s timeout:
```bash
not_done=$(ceph pg dump pgs --format=json-pretty | \
  jq '.pg_stats | map(select(.last_scrub_duration == 0)) | ...' | wc -l)
```

Crimson uses three methods in priority order with a 120 s timeout (stats
propagation is slower in Crimson):
1. Check PG state for `"scrubbing"` via `ceph pg $pg query`.
2. Check `scrub_metrics.session_metrics.successful_cnt` via `ceph pg $pg scrub_metrics`.
3. Fallback: check `info.stats.scrub_duration` via `ceph tell osd.$id pg $pg query`.

### 6. `TEST_just_deep_scrubs` — Cluster Setup

Classic uses the shared helper `standard_scrub_cluster` which internally
calls `run_osd`.

Crimson inlines the cluster setup manually, calling `run_crimson_osd` directly
with seastore-specific arguments and `apply_crimson_config`.

### 7. `TEST_abort_periodic_for_operator` — Config and Reservations Dump

| Operation | Classic | Crimson |
|-----------|---------|---------|
| Per-OSD config change | `ceph tell osd.* config set ...` | `ceph config set osd ...` |
| Dump scrub reservations | `ceph tell osd.2 dump_scrub_reservations` | `ceph --admin-daemon $(get_asok_path osd.$id) dump_scrub_reservations` (per-OSD admin socket; `ceph tell` not available in Crimson) |

### 8. `perf_counters` vs `dump_scrub_metrics`

Classic appends OSD scrub performance counters at the end of each test:
```bash
ceph tell osd.$osd counter dump | jq 'with_entries(select(.key | startswith("osd_scrub")))'
```

Crimson uses the scrub metrics command instead, since Crimson exposes scrub
stats through a different interface:
```bash
ceph pg $pgid scrub_metrics
```

### 9. Missing Test: `NO_scrub_extended_sleep`

This test validates `osd_scrub_extended_sleep` with a week-day scrub window.
It is present in the classic script (disabled with the `NO_` prefix) and
**entirely absent** from the crimson script. The test depends on
`--bluestore_cache_autotune`, which is not applicable to SeaStore/Crimson.
