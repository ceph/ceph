========================
 Placement Group States
========================

When checking a cluster's status (e.g., running ``ceph -w`` or ``ceph -s``), 
Ceph will report on the status of the placement groups. A placement group has 
one or more states. The optimum state for placement groups in the placement group
map is ``active + clean``. 

*Creating*
  Ceph is still creating the placement group.

*Active*
  Ceph will process requests to the placement group.

*Clean*
  Ceph replicated all objects in the placement group the correct number of times.

*Down*
  A replica with necessary data is down, so the placement group is offline.

*Replay*
  The placement group is waiting for clients to replay operations after an OSD crashed.

*Splitting*
  Ceph is splitting the placement group into multiple placement groups. (functional?)

*Scrubbing*
  Ceph is checking the placement group for inconsistencies.

*Degraded*
  Ceph has not replicated some objects in the placement group the correct number of times yet.

*Inconsistent*
  Ceph detects inconsistencies in the one or more replicas of an object in the placement group
  (e.g. objects are the wrong size, objects are missing from one replica *after* recovery finished, etc.).

*Peering*
  The placement group is undergoing the peering process

*Repair*
  Ceph is checking the placement group and repairing any inconsistencies it finds (if possible).

*Recovering*
  Ceph is migrating/synchronizing objects and their replicas.

*Backfill*
  Ceph is scanning and synchronizing the entire contents of a placement group
  instead of inferring what contents need to be synchronized from the logs of
  recent operations. *Backfill* is a special case of recovery.

*Wait-backfill*
  The placement group is waiting in line to start backfill.

*Backfill-toofull*
  A backfill operation is waiting because the destination OSD is over its
  full ratio.

*Incomplete*
  Ceph detects that a placement group is missing information about
  writes that may have occurred, or does not have any healthy
  copies. If you see this state, try to start any failed OSDs that may
  contain the needed information or temporarily adjust min_size to
  allow recovery.

*Stale*
  The placement group is in an unknown state - the monitors have not received
  an update for it since the placement group mapping changed.

*Remapped*
  The placement group is temporarily mapped to a different set of OSDs from what
  CRUSH specified.

*Undersized*
  The placement group fewer copies than the configured pool replication level.

*Peered*
  The placement group has peered, but cannot serve client IO due to not having
  enough copies to reach the pool's configured min_size parameter.  Recovery
  may occur in this state, so the pg may heal up to min_size eventually.
