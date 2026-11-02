.. _pgcalc:

=======
PG Calc
=======

The PG calculator suggests a ``pg_num`` value for each pool. It works from
the share of the data that the pool is expected to hold, the pool's size or
erasure-code profile, the number of OSDs that the pool uses, and a target
number of PGs per OSD.

The :ref:`PG autoscaler <pg-autoscaler>` is on by default for new pools and
adjusts ``pg_num`` as pools fill. Many clusters running recent releases do
fine with the PG autoscaler once :confval:`mon_target_pg_per_osd` is raised to
at least 200, which is its default beginning with the Umbrella release. This
calculator is still useful for internalizing how these values interact, and
where the PG autoscaler is infeasible or undesirable. To give the autoscaler
the calculator's information instead, set each pool's ``target_size_ratio`` to
its %Data. Leave it unset for CephFS metadata and RGW index pools, which the
autoscaler already weights more heavily.

Using the calculator
====================

#. Choose a use case. Each one fills the table with typical pools.
#. Choose the failure domain and CRUSH root for the CRUSH rules and
   erasure-code profiles that the generated commands create.
#. Adjust each pool: its application, whether it is replicated or erasure
   coded, its size or *k* and *m*, and the remaining values. Click the Size,
   OSD #, or Target PGs per OSD heading to set that value for every pool.
#. Click **Add pool** to add a pool, and the × at the start of a row to remove
   one.
#. Click **Generate commands** for commands that create the pools.

.. raw:: html

   <div id="pgcalc-root"></div>
   <script src="../../../_static/js/pgcalc.js"></script>

Fields
======

Failure domain and CRUSH root
   The CRUSH bucket type that each PG spreads its shards across,
   and the CRUSH root to choose them from. These go into the CRUSH rules and
   erasure-code profiles in the generated commands.

Pool name
   The name of the pool. The use cases give the usual names.

Application
   The application that uses the pool: ``rbd``, ``rgw``, ``cephfs``, or a
   name of your own for a pool that is accessed directly through librados.

Type
   Replicated or erasure coded. The calculator warns when an RGW index or
   metadata pool, or a CephFS metadata pool, is erasure coded: a future release
   may support this, but most deployments should stick with replication.

Size or k + m
   The number of replicas of a replicated pool, or the *k* data chunks and *m*
   coding chunks of an erasure-coded pool, where *k* is at least 2 and *m* at
   least 1. The calculation uses *k+m*. The
   calculator marks a pool amber when it has fewer than three replicas, or
   *m=1*: expect data unavailability during maintenance and data loss when
   component failures overlap. See :ref:`ecpool` for more about *m=1*. It
   also warns when *k* is above 8, which slows writes and rebuilds while each
   step up in *k* saves less space than the last.

OSD #
   The number of OSDs that hold the pool's PGs. This is usually every OSD in
   the cluster, but it is fewer when the pool's CRUSH rule selects a subset of
   them, for example a device class. Each PG puts each of its shards on its
   own OSD, in its own failure domain, so the calculator marks a pool red when
   OSD # is less than its size or *k+m*, and leaves it out of the generated
   commands.

%Data
   The share of the data on those OSDs that the pool is expected to hold. The
   use cases give examples.

Target PGs per OSD
   200 suits most clusters. Beginning with the Umbrella release, it is the
   default value of :confval:`mon_target_pg_per_osd`, the number of PG
   replicas per OSD that the PG autoscaler aims for. Use 300 if the number of
   OSDs is expected to grow, by up to double, in the foreseeable future. The
   monitors refuse to create pools or to raise ``pg_num`` past
   :confval:`mon_max_pg_per_osd` PG replicas per OSD, which is 500 by default
   beginning with the Umbrella release, and 250 in earlier releases.

How the suggested PG count is calculated
========================================

#. The calculator starts from this value, rounded down to a whole number:

   .. code-block:: none

      (Target PGs per OSD) x (OSD #) x (%Data / 100) / (Size, or k+m)

#. It rounds that value to the nearest power of two. If the nearest power of
   two is more than 25% below the value, it uses the next higher power of two.
#. The result is never less than a minimum that is meant to give every OSD at
   least one PG of every pool. The minimum is (OSD #) / (Size, or k+m), rounded
   down, plus one. It is rounded to a power of two in the same way, and
   doubled if it is still less than OSD #.

The aim is enough PGs to spread data evenly across the OSDs, without so many
PGs per OSD that recovery and backfill suffer. Empty or idle pools don't help
spread data, but their PGs still use memory and CPU.

For simplicity, the calculator assumes that all of a pool's OSDs are the same
size. CRUSH places PGs in proportion to OSD weight, so when OSD sizes vary,
the number of PG replicas on each OSD varies with them, and the largest OSDs
hold the most. Set :confval:`mon_max_pg_per_osd` generously enough to allow
for them: an OSD refuses to create PGs beyond
:confval:`osd_max_pg_per_osd_hard_ratio` (3 by default) times that value.

The generated commands
======================

The commands create, in order:

- A replicated CRUSH rule, and an erasure-code profile and CRUSH rule for each
  *k* and *m* in the table, all with the chosen failure domain and CRUSH root.
- Each pool, with its suggested ``pg_num`` and its CRUSH rule. A replicated
  pool also gets its size.
- ``allow_ec_overwrites`` on erasure-coded RBD and CephFS pools, which need
  it.
- The pool's application: ``rbd pool init`` for a replicated RBD pool, and
  ``ceph osd pool application enable`` for the others.

Review the commands before you run them. The PG autoscaler may change these
``pg_num`` values later unless you set each pool's ``pg_autoscale_mode`` to
``off``. CephFS pools still need ``ceph fs new``, and an erasure-coded RBD pool
holds image data only, with image metadata in a replicated pool.
