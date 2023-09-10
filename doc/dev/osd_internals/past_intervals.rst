=================================
OSDMap Trimming and PastIntervals
=================================


PastIntervals
-------------

There are two situations where we need to consider the set of all acting-set
OSDs for a PG back to some epoch ``e``:

 * During peering, we need to consider the acting set for every epoch back to
   ``last_epoch_started``, the last epoch in which the PG completed peering and
   became active.
   (see :doc:`/dev/osd_internals/last_epoch_started` for a detailed explanation)
 * During recovery, we need to consider the acting set for every epoch back to
   ``last_epoch_clean``, the last epoch at which all of the OSDs in the acting
   set were fully recovered, and the acting set was full.

For either of these purposes, we could build such a set by iterating backwards
from the current OSDMap to the relevant epoch.  Instead, we maintain a structure
PastIntervals for each PG.

An ``interval`` is a contiguous sequence of OSDMap epochs where the PG mapping
didn't change.  This includes changes to the acting set, the up set, the
primary, and several other parameters fully spelled out in
PastIntervals::check_new_interval.

Maintenance and Trimming
------------------------

The PastIntervals structure stores a record for each ``interval`` back to
last_epoch_clean.  On each new ``interval`` (See AdvMap reactions,
PeeringState::should_restart_peering, and PeeringState::start_peering_interval)
each OSD with the PG will add the new ``interval`` to its local PastIntervals.
Activation messages to OSDs which do not already have the PG contain the
sender's PastIntervals so that the recipient needn't rebuild it.  (See
PeeringState::activate needs_past_intervals).

PastIntervals are trimmed in two places.  First, when the primary marks the
PG clean, it clears its past_intervals instance
(PeeringState::try_mark_clean()).  The replicas will do the same thing when
they receive the info (See PeeringState::update_history).

The second, more complex, case is in PeeringState::start_peering_interval.  In
the event of a "map gap", we assume that the PG actually has gone clean, but we
haven't received a pg_info_t with the updated ``last_epoch_clean`` value yet.
To explain this behavior, we need to discuss OSDMap trimming.

OSDMap Trimming
---------------

OSDMaps are created by the Monitor quorum and gossiped out to the OSDs.  The
Monitor cluster also determines when OSDs (and the Monitors) are allowed to
trim old OSDMap epochs.  For the reasons explained above in this document, the
primary constraint is that we must retain all OSDMaps back to some epoch such
that all PGs have been clean at that or a later epoch (min_last_epoch_clean).
(See OSDMonitor::get_trim_to).

The Monitor quorum determines min_last_epoch_clean through MOSDBeacon messages
sent periodically by each OSDs.  Each message contains a set of PGs for which
the OSD is primary at that moment as well as the min_last_epoch_clean across
that set.  The Monitors track these values in OSDMonitor::last_epoch_clean.

There is a subtlety in the min_last_epoch_clean value used by the OSD to
populate the MOSDBeacon.  OSD::collect_pg_stats invokes PG::with_pg_stats to
obtain the lec value, which actually uses
pg_stat_t::get_effective_last_epoch_clean() rather than
info.history.last_epoch_clean.  If the PG is currently clean,
pg_stat_t::get_effective_last_epoch_clean() is the current epoch rather than
last_epoch_clean -- this works because the PG is clean at that epoch and it
allows OSDMaps to be trimmed during periods where OSDMaps are being created
(due to snapshot activity, perhaps), but no PGs are undergoing ``interval``
changes.

Back to PastIntervals
---------------------

We can now understand our second trimming case above.  If OSDMaps have been
trimmed up to epoch ``e``, we know that the PG must have been clean at some epoch
>= ``e`` (indeed, **all** PGs must have been), so we can drop our PastIntevals.

This dependency also pops up in PeeringState::check_past_interval_bounds().
PeeringState::get_required_past_interval_bounds takes as a parameter
oldest epoch, which comes from OSDSuperblock::cluster_osdmap_trim_lower_bound.
We use cluster_osdmap_trim_lower_bound rather than a specific osd's oldest map
because we don't necessarily trim all MOSDMap::cluster_osdmap_trim_lower_bound.
In order to avoid doing too much work at once we limit the amount of osdmaps
trimmed using ``osd_target_transaction_size`` in OSD::trim_maps().
For this reason, a specific OSD's oldest map can lag behind
OSDSuperblock::cluster_osdmap_trim_lower_bound
for a while.

See https://tracker.ceph.com/issues/49689 for an example.

OSDSuperblock::maps
-------------------

The OSDSuperblock holds an epoch interval set that represents the OSDMaps
that are stored by the OSD. Each OSDMap epoch range that was handled
is added to the set.
Once an osdmap is trimmed, it will be erased from the set.
As a result, the set's lower bound represent the oldest map that is
stored. While the upper bound represents the newest map.

The ``interval_set`` data structure supports non-contiguous epoch intervals
which may occur in "map gap" events. Before using this data structure,
``oldest_map`` and ``newest_map`` epochs were stored in the OSDSuperblock.
However, holding a single and contiguous epoch range imposed constraints which
may have resulted in an OSDMap leak.

See: https://tracker.ceph.com/issues/61962
