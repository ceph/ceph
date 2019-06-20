========================
 Placement Group States
========================

When checking a cluster's status (e.g., running ``ceph -w`` or ``ceph -s``), 
Ceph will report on the status of the placement groups. A placement group has 
one or more states. The optimum state for placement groups in the placement group
map is ``active + clean``. 

*creating*
  Ceph is still creating the placement group.

*activating*
  The placement group is peered but not yet active.

*active*
  Ceph will process requests to the placement group.

*clean*
  Ceph replicated all objects in the placement group the correct number of times.

*down*
  A replica with necessary data is down, so the placement group is offline.

*scrubbing*
  Ceph is checking the placement group metadata for inconsistencies.

*deep*
  Ceph is checking the placement group data against stored checksums.

*degraded*
  Ceph has not replicated some objects in the placement group the correct number of times yet.

*inconsistent*
  Ceph detects inconsistencies in the one or more replicas of an object in the placement group
  (e.g. objects are the wrong size, objects are missing from one replica *after* recovery finished, etc.).

*peering*
  The placement group is undergoing the peering process

*repair*
  Ceph is checking the placement group and repairing any inconsistencies it finds (if possible).

*recovering*
  Ceph is migrating/synchronizing objects and their replicas.

*forced_recovery*
  High recovery priority of that PG is enforced by user.

*recovery_wait*
  The placement group is waiting in line to start recover.

*recovery_toofull*
  A recovery operation is waiting because the destination OSD is over its
  full ratio.

*recovery_unfound*
  Recovery stopped due to unfound objects.

*backfilling*
  Ceph is scanning and synchronizing the entire contents of a placement group
  instead of inferring what contents need to be synchronized from the logs of
  recent operations. Backfill is a special case of recovery.

*forced_backfill*
  High backfill priority of that PG is enforced by user.

*backfill_wait*
  The placement group is waiting in line to start backfill.

*backfill_toofull*
  A backfill operation is waiting because the destination OSD is over
  the backfillfull ratio.

*backfill_unfound*
  Backfill stopped due to unfound objects.

*incomplete*
  Ceph detects that a placement group is missing information about
  writes that may have occurred, or does not have any healthy
  copies. If you see this state, try to start any failed OSDs that may
  contain the needed information. In the case of an erasure coded pool
  temporarily reducing min_size may allow recovery.

*stale*
  The placement group is in an unknown state - the monitors have not received
  an update for it since the placement group mapping changed.

*remapped*
  The placement group is temporarily mapped to a different set of OSDs from what
  CRUSH specified.

*undersized*
  The placement group has fewer copies than the configured pool replication level.

*peered*
  The placement group has peered, but cannot serve client IO due to not having
  enough copies to reach the pool's configured min_size parameter.  Recovery
  may occur in this state, so the pg may heal up to min_size eventually.

*snaptrim*
  Trimming snaps.

*snaptrim_wait*
  Queued to trim snaps.

*snaptrim_error*
  Error stopped trimming snaps.

*unknown*
  The ceph-mgr hasn't yet received any information about the PG's state from an
  OSD since mgr started up.
