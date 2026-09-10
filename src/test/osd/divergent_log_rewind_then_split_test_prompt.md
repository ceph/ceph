# Test Case Prompt: DivergentLogRewindThenSplit

## Bug reference

Tracker: https://tracker.ceph.com/issues/68649
Sepia run: `lflores-2026-07-17_23:28:16-rados-wip-lflores-testing-3-2026-07-17-1417-distro-default-trial/374634`

## Bug summary

When a replica's log is rewound to empty by `proc_replica_log` (because it
held divergent entries), and the replica is left with a non-empty `pg_missing`
set, `reset_complete_to()` returns early without updating `info.last_complete`.
As a result `last_complete == last_update` even though objects are missing.

If a PG split then occurs before recovery completes, the child PG inherits
this state. The child sends a `pg_notify_t` to the primary carrying
`last_complete == last_update`. The primary's `GetMissing` fast-path
(`pi.last_update == pi.last_complete && pi.last_update == info.last_update`)
fires, calls `peer_missing[replica].clear()`, and proceeds to activate the
replica as fully up-to-date. The primary then marks the PG `active+clean` and
forwards live client writes (clone operations) to the replica. The replica
crashes with ENOENT in BlueStore because it is missing the head object.

## Precise preconditions

1. A replica shard has entries in its log that are **divergent** from the
   authoritative log (it received recovery pushes in a prior interval that were
   later superseded).
2. `proc_replica_log` runs on the primary, detects the divergence, rewinds the
   replica's log to empty, and produces `peer_missing[replica] = {obj_head,
   obj_clone}`.
3. Recovery of those 2 missing objects begins (`activate - not complete,
   missing(2)`) but does **not** complete before the next event.
4. A PG split fires (`pg_num` increases). The parent PG has
   `(log_tail, last_update] == (v, v]` (empty log) and `missing(2)`.
5. `split_into()` copies the empty log + missing set to the child. The child's
   `reset_complete_to()` call no-ops (empty log early-return), leaving
   `child->info.last_complete == child->info.last_update`.
6. The child peers. Its `pg_notify_t` carries `last_complete == last_update`.
   The primary's `GetMissing` fast-path fires and clears
   `peer_missing[replica]`.
7. The primary activates, marks the PG `active+clean`, and forwards a
   client write (which includes a clone of `head`) to the replica.
8. The replica crashes: ENOENT in `BlueStore::_txc_add_transaction` because
   the `head` object does not exist on the shard.

## Test case to implement

Add to `TestECFailoverWithPeering` in
`src/test/osd/TestECFailoverWithPeering.cc`:

```
TEST_P(TestECFailoverWithPeering, DivergentLogRewindThenSplit)
```

### Step-by-step scenario

Use `osd_async_recovery_min_cost = 0` to force async recovery.

**Phase 1 — Write two objects so the replica has log entries**

```
create_and_write_verify("obj_head", pattern_a);   // v1
create_and_write_verify("obj_clone", pattern_a);  // v2
```

Both objects are on all shards. PG is `active+clean`.

**Phase 2 — Mark target replica down, write new versions, bring it back**

Mark shard `target = 1` down.

```
mark_osd_down(target);
write_verify("obj_head",  0, pattern_b, data_size);   // v3, only on 0, 2..k+m-1
write_verify("obj_clone", 0, pattern_b, data_size);   // v4, only on 0, 2..k+m-1
mark_osd_up(target);
```

After `mark_osd_up`, peering runs. `proc_replica_log` detects that the replica
at `target` has v1 and v2 for the two objects (which are divergent from v3/v4),
rewinds the replica's log to empty, and produces `peer_missing[target] = {obj_head, obj_clone}`.
Do **not** call `run_recovery_and_verify_callbacks` yet — leave recovery incomplete.

At this point assert that:
- `get_peering_state(0)->get_peer_missing()` contains a non-empty entry for
  the shard corresponding to `target`.
- `get_peering_state(target)->get_pg_log().get_log().log.empty()` is true.
- `get_peering_state(target)->get_pg_log().get_missing().num_missing() == 2`.
- `get_peering_state(target)->get_info().last_complete` is **less than**
  `get_peering_state(target)->get_info().last_update`.
  *(This is the assertion that currently FAILS due to the bug — last_complete
  wrongly equals last_update.)*

**Phase 3 — Simulate a PG split by forcing a new peering interval**

The test framework cannot directly change `pg_num`, but we can approximate the
effect of the split by forcing a new peering interval on just the target shard
while it still has the empty log + missing(2) state.  Concretely:

- Call `advance_epoch()` (or `new_epoch()`) to bump the osdmap epoch.
- This triggers `start_peering_interval` on the child shard, which calls
  `reset_complete_to` again on the empty log — reproducing the no-op that
  leaves `last_complete == last_update`.
- Allow peering messages to flow so the primary receives the new `pg_notify_t`.

Then assert:
- The primary's `GetMissing` did **not** clear `peer_missing[target]`, i.e.
  `get_peering_state(0)->get_peer_missing()` still has a non-empty entry for
  the target shard.
  *(This assertion currently FAILS — `peer_missing` is incorrectly cleared.)*

**Phase 4 — Write to obj_head (which requires a clone), verify no crash**

```
write_verify("obj_head", 0, pattern_c, data_size);  // triggers clone of head on all shards
```

Without the fix, this write issues a `clone` sub-transaction to the target
shard, which hits ENOENT and aborts. With the fix, the write either:
- Is blocked until recovery completes (the missing set is respected), or
- Causes recovery to be initiated before forwarding the write.

Assert the write completes without crashing the shard.

**Phase 5 — Complete recovery and verify data**

```
run_recovery_and_verify_callbacks("obj_head",  target, pattern_b);
run_recovery_and_verify_callbacks("obj_clone", target, pattern_b);
```

Assert all shards are clean after recovery.

### Assertions summary

| # | Assertion | Passes without fix? |
|---|-----------|---------------------|
| 1 | After proc_replica_log: target log is empty | Yes |
| 2 | After proc_replica_log: target missing(2) | Yes |
| 3 | After proc_replica_log: target `last_complete < last_update` | **No** (bug) |
| 4 | After new interval: primary `peer_missing[target]` non-empty | **No** (bug) |
| 5 | Write with clone completes without crash | **No** (bug) |
| 6 | Recovery completes, all shards clean | **No** (bug) |

### Notes on fixture capabilities

- `mark_osd_down(shard)` / `mark_osd_up(shard)` trigger full peering.
- `advance_epoch()` bumps the osdmap and triggers `start_peering_interval`.
- `get_peering_state(shard)->get_pg_log().get_log().log.empty()` checks log.
- `get_peering_state(shard)->get_pg_log().get_missing()` reads the missing set.
- `get_peering_state(0)->get_peer_missing()` returns the primary's view of
  peer missing sets.
- `set_config("osd_async_recovery_min_cost", "0")` forces async recovery so
  that recovery does not run automatically before the split simulation.
- Restore `osd_async_recovery_min_cost` to `"100"` at the end of the test.

### Expected result

**Without fix:** assertion 3 fails (last_complete == last_update after
proc_replica_log rewind to empty log), and if that assertion is removed the
write in phase 4 crashes with ENOENT in BlueStore.

**With fix:** all assertions pass, the write completes safely, and recovery
successfully brings the target shard up to date.
