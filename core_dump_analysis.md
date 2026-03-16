# Ceph OSD Core Dump Analysis

## Summary
All 6 ceph-osd processes (OSDs 0-5) crashed in the build directory. Analysis reveals **two distinct crash patterns**:

---

## Crash Pattern 1: SIGABRT - std::out_of_range Exception (OSDs 0 & 2)

### Affected OSDs
- **OSD 0** (`core.559327`)
- **OSD 2** (`core.562544`)

### Root Cause
**`std::out_of_range` exception in [`PrimaryLogPG::get_internal_versions()`](src/osd/PrimaryLogPG.cc:16113)**

### Detailed Analysis

The crash occurred when trying to access a shard version that doesn't exist in the `shard_versions` map:

```cpp
// At src/osd/PrimaryLogPG.cc:16113
out->at(shard) = version;  // std::map::at() throws if key doesn't exist
```

### The Problem

From the core dump analysis and code inspection:

1. **Object info has 3 shard versions** in `shard_versions` map:
   - Shard 1: version 78, epoch 49
   - Shard 2: version 78, epoch 49
   - Shard 3: version 78, epoch 49

2. **Acting set has 5 shards**:
   - OSD 0, shard 0
   - OSD 1, shard 1
   - OSD 3, shard 4
   - OSD 4, shard 5
   - OSD 5, shard 3

3. **The code tried to access shard 2** from the output map, but:
   - The output map was initialized with acting shards (0, 1, 4, 5, 3)
   - Shard 2 is NOT in the acting set
   - When iterating over `shard_versions` (which contains shards 1, 2, 3), it tried to do `out->at(shard_id=2)`
   - **Shard 2 doesn't exist in the output map → `std::out_of_range` exception thrown**

### Root Cause Analysis

The bug is in [`PrimaryLogPG::get_internal_versions()`](src/osd/PrimaryLogPG.cc:16109-16113):

```cpp
if (is_primary()) {
  for (const auto& shard : acting_shards) {
    (*out)[shard.shard] = obc->obs.oi.version;  // Initialize with current version
  }
  for (const auto& [shard, version] : obc->obs.oi.shard_versions) {
    out->at(shard) = version;  // BUG: assumes shard exists in out map
  }
}
```

**What `shard_versions` represents:**

From [`ECTransaction.cc:897-910`](src/osd/ECTransaction.cc:897-910), `shard_versions` tracks **non-primary shards that have stale data** during EC partial writes:

- In erasure-coded pools with partial writes enabled, some shards are designated as "non-primary" (cannot become primary)
- When a partial write occurs, only some shards are updated
- Non-primary shards that were NOT written to get an entry in `shard_versions` with their old version
- This tracks which shards have outdated data and need recovery

**The scenario that causes the crash:**

1. An EC pool has partial writes enabled with non-primary shards (e.g., shards 1, 2, 3)
2. A partial write occurs, updating some but not all shards
3. Shard 2 is marked as having stale data in `shard_versions` (version 78)
4. Later, the acting set changes due to OSD failure/rebalancing
5. The new acting set no longer includes shard 2 (now has shards 0, 1, 3, 4, 5)
6. `get_internal_versions()` is called via `CEPH_OSD_OP_GET_INTERNAL_VERSIONS`
7. The function initializes `out` map with only the current acting shards
8. Then it tries to update entries from `shard_versions`, including shard 2
9. **Crash**: `out->at(2)` throws because shard 2 isn't in the acting set

### Stack Trace
```
#12 std::__throw_out_of_range(char const*)
#13 std::map<shard_id_t, eversion_t>::at() at stl_map.h:553
#14 PrimaryLogPG::get_internal_versions() at PrimaryLogPG.cc:16113
#15 PrimaryLogPG::do_osd_ops() at PrimaryLogPG.cc:6596
#16 PrimaryLogPG::prepare_transaction() at PrimaryLogPG.cc:9049
#17 PrimaryLogPG::execute_ctx() at PrimaryLogPG.cc:4293
```

### Signal
- **SIGABRT** (Aborted) - C++ exception caused abort

---

## Crash Pattern 2: SIGILL - Illegal Instruction (OSDs 1, 3, 4, 5)

### Affected OSDs
- **OSD 1** (`core.560901`)
- **OSD 3** (`core.564120`)
- **OSD 4** (`core.565779`)
- **OSD 5** (`core.567480`)

### Root Cause
**SIGILL (Illegal Instruction)** during shutdown sequence

### Stack Trace
```
#0  __pthread_kill_implementation()
#1  raise()
#2  reraise_fatal() at signal_handler.cc:93
#3  handle_oneshot_fatal_signal() at signal_handler.cc:372
#4  <signal handler called>
#5  __futex_abstimed_wait_common()
#6  pthread_cond_wait()
#7  ceph::condition_variable_debug::wait()
#8  AsyncMessenger::wait() at AsyncMessenger.cc:806
#9  main() at ceph_osd.cc:779
```

### Analysis
These OSDs received **SIGILL (signal 4)** while waiting in the main thread during shutdown. This appears to be a **secondary failure** - likely these OSDs were killed externally (possibly by a test harness or watchdog) after the primary crashes occurred.

---

## Conclusion

### Primary Issue
**Bug in [`PrimaryLogPG::get_internal_versions()`](src/osd/PrimaryLogPG.cc:16113)**: The function attempts to access shard IDs from `object_info.shard_versions` that may not exist in the acting set, causing an `std::out_of_range` exception.

### Secondary Issue  
The remaining OSDs (1, 3, 4, 5) were terminated with SIGILL, likely by external intervention after the cluster became unstable due to the primary crashes.

### Recommendation

Fix the bug in [`PrimaryLogPG::get_internal_versions()`](src/osd/PrimaryLogPG.cc:16113) to handle cases where `shard_versions` contains shards not in the current acting set.

**Proposed fix:**

```cpp
if (is_primary()) {
  for (const auto& shard : acting_shards) {
    (*out)[shard.shard] = obc->obs.oi.version;
  }
  for (const auto& [shard, version] : obc->obs.oi.shard_versions) {
    // Only update if shard is in the acting set
    auto it = out->find(shard);
    if (it != out->end()) {
      it->second = version;
    }
    // Shards not in acting set are ignored - they're tracked in shard_versions
    // for recovery purposes but aren't relevant for current operations
  }
}
```

**Why this happens:**

This is a race condition between:
1. **EC partial write tracking** - which records stale shard versions in `shard_versions`
2. **Acting set changes** - which can remove OSDs/shards from the active set

The `shard_versions` map persists in object metadata even after the acting set changes, causing the crash when `get_internal_versions()` assumes all shards in `shard_versions` are also in the current acting set.