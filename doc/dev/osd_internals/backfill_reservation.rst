====================
Backfill Reservation
====================

When a new OSD joins a cluster all PGs with it in their acting sets must
eventually backfill.  If all of these backfills happen simultaneously
they will present excessive load on the OSD:  the "thundering herd"
effect.

The ``osd_max_backfills`` tunable limits the number of outgoing or
incoming backfills that are active on a given OSD. Note that this limit is
applied separately to incoming and to outgoing backfill operations.
Thus there can be as many as ``osd_max_backfills * 2`` backfill operations
in flight on each OSD.  This subtlety is often missed, and Ceph
operators can be puzzled as to why more ops are observed than expected.

Each ``OSDService`` now has two AsyncReserver instances: one for backfills going
from the OSD (``local_reserver``) and one for backfills going to the OSD
(``remote_reserver``).  An ``AsyncReserver`` (``common/AsyncReserver.h``)
manages a queue by priority of waiting items and a set of current reservation
holders.  When a slot frees up, the ``AsyncReserver`` queues the ``Context*``
associated with the next item on the highest priority queue in the finisher
provided to the constructor.

For a primary to initiate a backfill it must first obtain a reservation from
its own ``local_reserver``.  Then it must obtain a reservation from the backfill
target's ``remote_reserver`` via a ``MBackfillReserve`` message. This process is
managed by sub-states of ``Active`` and ``ReplicaActive`` (see the sub-states
of ``Active`` in PG.h).  The reservations are dropped either on the ``Backfilled``
event, which is sent on the primary before calling ``recovery_complete``
and on the replica on receipt of the ``BackfillComplete`` progress message),
or upon leaving ``Active`` or ``ReplicaActive``.

It's important to always grab the local reservation before the remote
reservation in order to prevent a circular dependency.

We minimize the risk of data loss by prioritizing the order in
which PGs are recovered.  Admins can override the default order by using
``force-recovery`` or ``force-backfill``. A ``force-recovery`` with op
priority ``255`` will start before a ``force-backfill`` op at priority ``254``.

If recovery is needed because a PG is below ``min_size`` a base priority of
``220`` is used. This is incremented by the number of OSDs short of the pool's
``min_size`` as well as a value relative to the pool's ``recovery_priority``.
The resultant priority is capped at ``253`` so that it does not confound forced
ops as described above. Under ordinary circumstances a recovery op is
prioritized at ``180`` plus a value relative to the pool's ``recovery_priority``.
The resultant priority is capped at ``219``.

If backfill is needed because the number of acting OSDs is less than
the pool's ``min_size``, a priority of ``220`` is used.  The number of OSDs
short of the pool's ``min_size`` is added as well as a value relative to
the pool's ``recovery_priority``.  The total priority is limited to ``253``.

If backfill is needed because a PG is undersized,
a priority of ``140`` is used.  The number of OSDs below the size of the pool is
added as well as a value relative to the pool's ``recovery_priority``.  The
resultant priority is capped at ``179``.  If a backfill op is
needed because a PG is degraded, a priority of ``140`` is used.  A value
relative to the pool's ``recovery_priority`` is added.  The resultant priority
is capped at ``179`` .  Under ordinary circumstances a
backfill op priority of ``100`` is used.  A value relative to the pool's
``recovery_priority`` is added.  The total priority is capped at ``139``.

.. list-table:: Backfill and Recovery op priorities
   :widths: 20 20 20
   :header-rows: 1

   * - Description
     - Base priority
     - Maximum priority
   * - Backfill
     - 100
     - 139
   * - Degraded Backfill
     - 140
     - 179
   * - Recovery
     - 180
     - 219
   * - Inactive Recovery
     - 220
     - 253
   * - Inactive Backfill
     - 220
     - 253
   * - force-backfill
     - 254
     -
   * - force-recovery
     - 255
     -

