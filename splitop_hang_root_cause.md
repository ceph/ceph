# SplitOp Hang Root Cause Analysis

## Executive Summary

The hang in `ceph_test_rados` is caused by a bug in [`Objecter::_calc_target()`](src/osdc/Objecter.cc:3081) when handling SplitOp sub-operations with `FORCE_OSD` flag after OSD map changes. When a forced OSD disappears from the acting set, the code incorrectly attempts to redrive the operation to a different PG shard, causing OSDs to silently drop the misdirected operations. This leaves the parent SplitOp waiting forever for sub-operations that will never complete.

## The Bug

### What Happens

1. **Parent operation (tid 30165)** is created for an EC read and assigned to splitop_session (osd=-2)
2. **SplitOp creates sub-operations** targeting specific shards with `FORCE_OSD` flag:
   - tid 30166 → shard 0 (osd.0) with force_osd=0
   - tid 30167 → shard 1 (osd.1) with force_osd=1  ← **This one fails**
   - tid 30168 → shard 4 (osd.4) with force_osd=4
3. **OSD map changes** occur (OSDs killed, PG remapping)
4. **tid 30167 is retried** but osd.1 is no longer in the acting set for shard 1
5. **`_calc_target()` incorrectly handles this**:
   - Detects forced osd.1 not in acting set
   - Clears `FORCE_OSD` flag and tries to redrive to primary
   - **Changes PG from 2.2s1 to 2.2s0** (wrong shard!)
   - Sends operation to osd.1 but for wrong PG shard (2.2s0 instead of 2.2s1)
6. **OSD.1 correctly drops the misdirected operation** without sending a reply
7. **Parent operation hangs forever** waiting for tid 30167 that will never complete

### Evidence from Logs

**Original sub-operation created correctly:**
```
Line 1720135: SplitOp::sent_op pool=2 pgid=2.2s1 osd=1 force_osd=1 balance_reads=0
Line 1720143: _op_submit tid 30167 osd.1
Line 1720144: _send_op 30167 to 2.2s1 on osd.1  ✓ Correct!
```

**After retry, sent to wrong PG shard:**
```
11:54:14.434: _send_op 30167 to 2.2s0 on osd.1  ✗ Wrong shard!
```

**OSD correctly drops misdirected operation:**
```
11:54:12.638 osd.1: changed after 103, dropping osd_op(client.4317.0:30167 2.2s1 ... e103)
11:54:14.457 osd.1: misdirected, dropping (e106 at 2.2s0)
11:54:53.781 osd.1: no pg, shouldn't exist e136, dropping (2.2s4)
```

## Root Cause in Code

The bug is in [`Objecter::_calc_target()`](src/osdc/Objecter.cc:3245-3277):

```cpp
if (t->flags & CEPH_OSD_FLAG_FORCE_OSD) {
  bool osd_in_acting = false;
  for (auto acting_osd : t->acting) {
    if (acting_osd == t->osd) {
      osd_in_acting = true;
      break;
    }
  }
  if (!osd_in_acting) {
    if (t->flags & CEPH_OSD_FLAG_FAIL_ON_EAGAIN) {
      // Return error to trigger -EAGAIN
      ldout(cct, 10) << __func__ << " forced osd." << t->osd
                     << " not in acting set " << t->acting
                     << ", FAIL_ON_EAGAIN set, returning POOL_DNE to trigger -EAGAIN"
                     << dendl;
      t->osd = -1;
      return RECALC_OP_TARGET_POOL_DNE;
    } else {
      // ← BUG IS HERE!
      ldout(cct, 10) << __func__ << " forced osd." << t->osd
                     << " not in acting set " << t->acting
                     << ", clearing direct read flags and redriving to primary"
                     << dendl;
      t->flags &= ~CEPH_OSD_FLAGS_DIRECT_READ;
      t->flags &= ~CEPH_OSD_FLAG_FORCE_OSD;  // ← Clears FORCE_OSD!
      // Falls through to recalculate target, which changes PG shard!
    }
  }
}
```

### The Problem

When `FORCE_OSD` is set but the forced OSD is not in the acting set:

1. **If `FAIL_ON_EAGAIN` is set**: Returns `RECALC_OP_TARGET_POOL_DNE` which triggers -EAGAIN ✓ Correct
2. **If `FAIL_ON_EAGAIN` is NOT set**: Clears `FORCE_OSD` and redrives ✗ **BUG!**

**Why this is wrong for SplitOp:**

- SplitOp sub-operations have `FORCE_OSD` set to target specific EC shards
- They do NOT have `FAIL_ON_EAGAIN` set (that's only for the parent)
- When the forced OSD disappears, clearing `FORCE_OSD` causes the code to recalculate the target
- This changes the PG shard (e.g., from 2.2s1 to 2.2s0)
- The operation is then sent to the wrong PG shard and gets dropped by the OSD
- **No reply is sent**, so the parent SplitOp hangs forever

## The Fix

When `FORCE_OSD` is set and the forced OSD is not in the acting set, we must **always** fail the operation with -EAGAIN, regardless of whether `FAIL_ON_EAGAIN` is set. This allows:

1. The sub-operation to fail back to the parent SplitOp
2. The parent SplitOp to handle the failure via [`SplitOp::complete()`](src/osdc/SplitOp.cc:497)
3. The parent operation to be retried or failed appropriately

### Proposed Fix

In [`Objecter::_calc_target()`](src/osdc/Objecter.cc:3256-3276), change the logic to:

```cpp
if (!osd_in_acting) {
  // When a forced OSD disappears from the acting set, we must fail the
  // operation with -EAGAIN. This is critical for SplitOp sub-operations
  // which target specific EC shards - we cannot redrive them to a different
  // shard as that would send the operation to the wrong PG.
  ldout(cct, 10) << __func__ << " forced osd." << t->osd
                 << " not in acting set " << t->acting
                 << ", returning POOL_DNE to trigger -EAGAIN"
                 << dendl;
  t->osd = -1;
  return RECALC_OP_TARGET_POOL_DNE;
}
```

This ensures that:
- Operations with `FORCE_OSD` always fail with -EAGAIN when the forced OSD disappears
- SplitOp sub-operations properly fail back to the parent
- The parent can retry or fail the entire operation
- No operations are sent to wrong PG shards

## Impact

This bug affects:
- Erasure-coded pools with SplitOp enabled (balance_reads=1)
- Scenarios where OSD map changes occur during operation execution
- Any operation using `FORCE_OSD` flag without `FAIL_ON_EAGAIN`

The hang is reproducible with `ceph_test_rados` when:
1. Using erasure-coded pools (k=3, m=2)
2. SplitOp feature enabled
3. OSD map churn (OSDs killed/restarted)
4. Multiple OSDs killed simultaneously

## Related Code

- [`Objecter::_calc_target()`](src/osdc/Objecter.cc:3081) - Where the bug occurs
- [`SplitOp::prepare_single_op()`](src/osdc/SplitOp.cc:849) - Sets `FORCE_OSD` on sub-operations
- [`SplitOp::complete()`](src/osdc/SplitOp.cc:497) - Handles sub-operation completion
- [`Objecter::op_post_split_op_complete()`](src/osdc/Objecter.cc:2379) - Handles split op retry