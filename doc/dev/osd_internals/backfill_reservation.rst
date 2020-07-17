====================
Backfill Reservation
====================

When a new osd joins a cluster, all pgs containing it must eventually backfill
to it.  If all of these backfills happen simultaneously, it would put excessive
load on the osd. osd_max_backfills limits the number of outgoing or
incoming backfills on a single node. The maximum number of outgoing backfills is
osd_max_backfills. The maximum number of incoming backfills is
osd_max_backfills. Therefore there can be a maximum of osd_max_backfills * 2
simultaneous backfills on one osd.

Each OSDService now has two AsyncReserver instances: one for backfills going
from the osd (local_reserver) and one for backfills going to the osd
(remote_reserver).  An AsyncReserver (common/AsyncReserver.h) manages a queue
by priority of waiting items and a set of current reservation holders.  When a
slot frees up, the AsyncReserver queues the Context* associated with the next
item on the highest priority queue in the finisher provided to the constructor.

For a primary to initiate a backfill, it must first obtain a reservation from
its own local_reserver.  Then, it must obtain a reservation from the backfill
target's remote_reserver via a MBackfillReserve message. This process is
managed by substates of Active and ReplicaActive (see the substates of Active
in PG.h).  The reservations are dropped either on the Backfilled event, which
is sent on the primary before calling recovery_complete and on the replica on
receipt of the BackfillComplete progress message), or upon leaving Active or
ReplicaActive.

It's important that we always grab the local reservation before the remote
reservation in order to prevent a circular dependency.

We want to minimize the risk of data loss by prioritizing the order in
which PGs are recovered.  A user can override the default order by using
force-recovery or force-backfill. A force-recovery at priority 255 will start
before a force-backfill at priority 254.

If a recovery is needed because a PG is below min_size a base priority of 220
is used. The number of OSDs below min_size of the pool is added, as well as a
value relative to the pool's recovery_priority.  The total priority is limited
to 253. Under ordinary circumstances a recovery is prioritized at 180 plus a
value relative to the pool's recovery_priority.  The total priority is limited
to 219.

If a backfill is needed because the number of acting OSDs is less than min_size,
a priority of 220 is used.  The number of OSDs below min_size of the pool is
added as well as a value relative to the pool's recovery_priority.  The total
priority is limited to 253.  If a backfill is needed because a PG is undersized,
a priority of 140 is used.  The number of OSDs below the size of the pool is
added as well as a value relative to the pool's recovery_priority.  The total
priority is limited to 179.  If a backfill is needed because a PG is degraded,
a priority of 140 is used.  A value relative to the pool's recovery_priority is
added.  The total priority is limited to 179.  Under ordinary circumstances a
backfill is priority of 100 is used.  A value relative to the pool's
recovery_priority is added.  The total priority is limited to 139.


Description             Base priority   Maximum priority
-----------             -------------   ----------------
Backfill                100             139
Degraded Backfill       140             179
Recovery                180             219
Inactive Recovery       220             253
Inactive Backfill       220             253
force-backfill          254
force-recovery          255
