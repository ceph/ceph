# Ceph Test RADOS Hang Analysis

## Executive Summary

The `ceph_test_rados` hung during a stress test performing 400,000 operations on 128 objects with 4 concurrent operations. The hang occurred due to **false-positive OSD failure detection** causing a PG to enter an extended peering state, blocking client I/O operations.

## Timeline of Events

### 13:23:04 - Test Started
- Test configuration: 400,000 operations, 128 objects, max 4 in-flight operations
- Object size: 2000 bytes, write stride: 200-400 bytes

### 13:35:17.020 - First Stuck Operation
- **Client operation tid 59827**: Read on object 96 submitted to OSD.4
- Object: `aainscowukibmcom-san-ceph2-rocky10-fulllatest668978-96`
- PG: 2.6s2

### 13:35:17.024 - OSD.2 False-Positive Failure
```
Monitor daemon marked osd.2 down, but it is still running
map e527 wrongly marked me down at e527
```
- OSD.2 was incorrectly marked down by the monitor at epoch 527
- OSD.2 was still running and processing requests

### 13:35:17.113 - PG Peering Disruption
- PG 2.6s2 on OSD.4 detected acting set change:
  - **Before**: `[1,0,4,5,3,2]` (all OSDs present)
  - **After**: `[1,0,4,5,3,2147483647]` (OSD.2 replaced with NONE)
- PG entered "unknown NOTIFY" state with `pruub` (peering request up until blocked)
- State: `pruub=14.889929771s` indicating peering blocked for ~15 seconds

### 13:35:17.183 - Client Operation Requeued
- Operation tid 59827 requeued with latency 0.162419s
- PG state: `unknown NOTIFY pruub 797.033020020s@`
- Operation stuck waiting for PG to complete peering

### 13:35:18.213 - Last Requeue
- Operation tid 59827 requeued again with latency 1.191841s
- After this, the operation was never completed or requeued again

### 13:35:18.809 - PG Recovered
- PG 2.6s2 transitioned to `Started/ReplicaActive` state
- Acting set restored: `[1,0,4,5,3,2]`
- **However, the stuck client operations were never resumed**

### 13:35:44.850 - Additional Stuck Operations
- **tid 60293**: Read on object 56, submitted to OSD.3
- **tid 60295**: Read on object 56, submitted to OSD.1
- Both operations started lagging at 13:35:59

### 13:35:44.852 - Test Hung
- Test output shows: `waiting on 4`
- Last operation: `11932: read oid 56 snap -1`
- Test never progressed beyond this point

### 13:36:19.253 - Laggy Operations Detected
```
client.4317.objecter  tid 60295 on osd.1 is laggy
client.4317.objecter  tid 60293 on osd.3 is laggy
client.4317.objecter  tid 59827 on osd.4 is laggy
```

## Root Cause Analysis

### Primary Issue: False-Positive OSD Failure Detection

1. **Monitor incorrectly marked OSD.2 as down** at epoch 527
   - OSD.2 was still running and processing requests
   - This triggered unnecessary PG peering across the cluster

2. **PG Peering Blocked Client I/O**
   - When PG 2.6s2 entered peering state, it stopped processing client operations
   - Operations were requeued but never completed after PG recovered
   - The PG state machine failed to resume pending operations

3. **Operation Leak/Lost Wake-up**
   - Client operations tid 59827, 60293, and 60295 were never completed
   - After PG recovered to active state, these operations were not resumed
   - This suggests a bug in the operation requeue/resume logic during peering recovery

### Secondary Issues

1. **Erasure Coded Pool Complexity**
   - PG 2.6s2 is an EC pool with acting set `[1,0,4,5,3,2]`
   - EC pools require more complex coordination during peering
   - The `pruub` (peering request up until blocked) indicates the replica was waiting for primary

2. **No Timeout or Recovery Mechanism**
   - Client operations remained stuck indefinitely
   - No automatic retry or timeout mechanism kicked in
   - Test hung waiting for operations that would never complete

## Evidence

### Client Log (client.admin.668978.log)
```
2026-03-13T13:35:17.020 - tid 59827 submitted to osd.4
2026-03-13T13:35:29.243 - tid 59827 marked as laggy
2026-03-13T13:35:44.850 - tid 60293 submitted to osd.3
2026-03-13T13:35:44.851 - tid 60295 submitted to osd.1
2026-03-13T13:35:59.249 - All three operations marked as laggy
```

### OSD.4 Log (osd.4.log)
```
2026-03-13T13:35:17.022 - Operation received, PG state: active+remapped
2026-03-13T13:35:17.113 - PG peering restart due to acting set change
2026-03-13T13:35:17.115 - PG state: unknown NOTIFY pruub 797.033020020s@
2026-03-13T13:35:17.183 - Operation requeued, latency 0.162419s
2026-03-13T13:35:18.213 - Operation requeued, latency 1.191841s
2026-03-13T13:35:18.809 - PG recovered to ReplicaActive
```

### OSD.2 Log (osd.2.log)
```
2026-03-13T13:35:17.024 - Monitor daemon marked osd.2 down, but it is still running
2026-03-13T13:35:17.024 - map e527 wrongly marked me down at e527
```

## Impact

- **Test Hang**: ceph_test_rados hung indefinitely
- **Lost Operations**: 3+ client operations permanently stuck
- **No Recovery**: System did not self-recover despite PG becoming active

## Recommendations

### Immediate Actions

1. **Investigate False-Positive Detection**
   - Review monitor logs to understand why OSD.2 was incorrectly marked down
   - Check for heartbeat timeouts, network issues, or monitor bugs
   - Examine monitor configuration: `mon_osd_down_out_interval`, `mon_osd_report_timeout`

2. **Fix Operation Resume Logic**
   - **Critical Bug**: Operations requeued during peering are not resumed after PG recovery
   - Review [`PrimaryLogPG::requeue_ops()`](src/osd/PrimaryLogPG.cc) and [`ECBackend::handle_sub_write_reply()`](src/osd/ECBackend.cc)
   - Ensure operations in `waiting_for_peered` queue are properly woken up

3. **Add Operation Timeout**
   - Implement client-side timeout for stuck operations
   - Add automatic retry mechanism for operations stuck in peering

### Code Areas to Investigate

1. **PG State Machine** ([`src/osd/PeeringState.cc`](src/osd/PeeringState.cc))
   - Review transition from `NOTIFY` to `Active` state
   - Verify operation resume logic in `activate()` method
   - Check `requeue_ops()` is called after peering completes

2. **EC Backend** ([`src/osd/ECBackend.cc`](src/osd/ECBackend.cc))
   - Review how EC sub-operations are tracked during peering
   - Verify sub-read operations are properly resumed
   - Check `waiting_state` and `waiting_commit` queues

3. **Objecter** ([`src/osdc/Objecter.cc`](src/osdc/Objecter.cc))
   - Review laggy operation detection and handling
   - Verify operations are retried when marked laggy
   - Check `should_resend` logic for stuck operations

4. **Monitor OSD Failure Detection** ([`src/mon/OSDMonitor.cc`](src/mon/OSDMonitor.cc))
   - Review heartbeat processing logic
   - Check for race conditions in failure detection
   - Verify `mark_down()` logic doesn't create false positives

### Testing Recommendations

1. **Reproduce the Issue**
   - Run ceph_test_rados with similar configuration
   - Inject OSD failure detection events
   - Monitor for stuck operations

2. **Add Test Coverage**
   - Test operation resume after PG peering
   - Test EC pool operations during OSD failures
   - Test false-positive failure detection scenarios

3. **Stress Testing**
   - Run extended stress tests with concurrent operations
   - Monitor for operation leaks
   - Check for memory leaks in operation queues

## Conclusion

The hang was caused by a **false-positive OSD failure detection** that triggered PG peering, which in turn exposed a **bug in the operation resume logic**. When the PG recovered from peering, pending client operations were not properly resumed, causing them to hang indefinitely.

This is a **critical bug** that can cause data plane hangs in production systems during transient OSD failures or false-positive failure detection events.

### Priority: **CRITICAL**
### Severity: **HIGH** - Can cause indefinite client I/O hangs

## Files Analyzed

- `/work/ceph/build/out/for_bob/ceph_test_rados.log` (11M)
- `/work/ceph/build/out/for_bob/client.admin.668978.log` (781M)
- `/work/ceph/build/out/for_bob/osd.4.log` (2.4G)
- `/work/ceph/build/out/for_bob/osd.2.log` (1.9G)