.. _mds-qos:

============================
MDS Quality of Service (QoS)
============================

The Metadata Server can throttle client metadata requests using the
dmClock scheduler, the same algorithm family used by the OSD mClock
scheduler. When the feature is enabled, each CephFS subvolume receives
its own QoS allocation, so a single noisy workload cannot monopolize
MDS capacity at the expense of other subvolumes.

.. note:: MDS QoS is a recent addition and is disabled by default.
   Evaluate it in a test environment before relying on it in
   production.

How it works
============

When QoS is enabled, incoming client metadata requests (for example
``lookup``, ``getattr``, ``mkdir``, and ``create``) pass through a
dmClock queue before the MDS processes them. Requests are grouped by
subvolume: the scheduler derives the QoS bucket from each session's
mount root, following the standard ``/volumes/<group>/<subvolume>``
layout, so all client sessions that mount the same subvolume share one
allocation.

Each subvolume's allocation is described by three values:

- **reservation**: the minimum request rate guaranteed to the
  subvolume, served before any spare capacity is distributed.
- **weight**: the subvolume's proportional share of capacity that
  remains after all reservations have been met.
- **limit**: the maximum request rate the subvolume may consume.
  Requests beyond the limit are held until capacity is available.

Rates are cost-weighted rather than raw operation counts: read-like
operations (``lookup``, ``getattr``, ``readdir``, ``open``) have a cost
of 1, and mutating operations (``create``, ``mkdir``, ``rename``,
``unlink``, ``setattr``) have a cost of 3.

Enabling QoS
============

Enable the scheduler on all MDS daemons:

.. prompt:: bash #

   ceph config set mds mds_dmclock_enable true

The setting takes effect on active MDS daemons and requires that the
reservation, weight, and limit defaults are all greater than zero,
which is the case out of the box. The defaults apply to every
subvolume that has no explicit override and can be changed at runtime:

.. prompt:: bash #

   ceph config set mds mds_dmclock_reservation 1000
   ceph config set mds mds_dmclock_weight 1000
   ceph config set mds mds_dmclock_limit 1000

See :confval:`mds_dmclock_enable`, :confval:`mds_dmclock_reservation`,
:confval:`mds_dmclock_weight`, and :confval:`mds_dmclock_limit`.

Per-subvolume settings
======================

Individual subvolumes can be given their own values through the MDS
admin interface. Values are integers, each must be at least 1, and the
reservation must not exceed the limit:

.. prompt:: bash #

   ceph tell mds.<id> qos set <subvolume-path> <reservation> <weight> <limit>

To inspect a subvolume's current settings and statistics, including
throttle counts and session counts:

.. prompt:: bash #

   ceph tell mds.<id> qos get <subvolume-path>

To revert a subvolume to the configured defaults:

.. prompt:: bash #

   ceph tell mds.<id> qos rm <subvolume-path>

To dump the QoS state of all subvolumes known to an MDS:

.. prompt:: bash #

   ceph tell mds.<id> dump qos

Example
=======

Suppose the subvolume mounted at ``/volumes/_nogroup/build`` runs a
batch job that floods the MDS with file creations, starving an
interactive workload in ``/volumes/_nogroup/home``. To cap the batch
subvolume and guarantee the interactive one a minimum rate:

.. prompt:: bash #

   ceph tell mds.0 qos set /volumes/_nogroup/build 100 100 500
   ceph tell mds.0 qos set /volumes/_nogroup/home 1000 2000 10000

The ``build`` subvolume is now limited to 500 cost-weighted requests
per second, while ``home`` is guaranteed 1000 and receives a larger
share of any spare capacity.
