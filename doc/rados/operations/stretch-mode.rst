.. _stretch_mode:

================
Stretch Clusters
================

A stretch cluster is a method of implementing synchronous mirroring of data
between two zones (usually data centers). One Ceph cluster spans both zones,
and while both are up, each write is stored in both zones before it is
acknowledged. The data stays available when one zone fails, or when the link
between the zones fails.

There are three types of stretch cluster:

* **Stretch mode**, the main type, which this page describes. Pools are
  stretched by setting their ``num_zones`` to 2, and the Monitors handle zone
  failures. Stretch mode is restricted to two zones, and every stretched pool
  uses the same two zones. It can be configured in two ways, described below.
* **Individual stretch pools** (``ceph osd pool stretch set``). They are more
  flexible, for example over three zones, but more manual: interventions may
  be necessary after a zone failure. See :ref:`individual_stretch_pools`.
* **Erasure-coded pools spread across zones**: an erasure-coded pool with a
  large ``m``, whose CRUSH MSR rule spreads its shards over three or more
  zones. Like individual stretch pools, they are an advanced configuration,
  and interventions may be necessary after a zone failure. See
  :ref:`ec_pools_spread_across_zones`.

You cannot mix stretch mode and individual stretch pools in one cluster.

Stretch mode can be configured in two ways. Both give the same result, and
Ceph does not record which one was used:

* **Pool by pool**, by setting ``num_zones`` to 2 on each pool to be
  stretched, when it is created or later. The first stretched pool puts the
  cluster in stretch mode. See `Setting Up a Two-Zone Cluster`_.
* **All pools at once**, with ``ceph mon enable_stretch_mode``. It sets
  ``num_zones`` to 2 on every pool, and changes the pool creation defaults so
  that new pools are stretched too. It is a shortcut to the same stretch
  mode, not another type of stretch cluster; see `Converting an Existing
  Cluster to Stretch Mode`_.

In stretch mode:

* **Each zone holds all the data.** A stretched pool keeps a full set of
  erasure-coded shards, or of replicas, in each zone, so either zone can serve
  I/O on its own.
* **Each zone runs its own Monitors.** A further Monitor, the *tiebreaker*,
  sits at a third site, holds no data, and determines which zone remains
  active if the two zones lose contact.
* **The Monitors handle zone failures.** When a zone fails, the cluster enters
  *degraded* mode and serves I/O from the surviving zone. When the failed zone
  returns, the cluster recovers it and then returns to normal mode
  automatically.

The following terms are used throughout this documentation:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Term
     - Meaning
   * - Zone
     - One of the two sites the data is split over, usually a data center: a
       CRUSH bucket, usually of type ``datacenter``
   * - Tiebreaker
     - A Monitor at a third site; it picks the surviving zone in a netsplit
   * - Stretched pool
     - A pool with ``num_zones`` > 1: it keeps copies in more than one zone
       (currently the maximum is 2)
   * - Copies in each zone
     - What a stretched pool keeps in every zone: a full set of ``k+m``
       shards for an erasure-coded pool, or ``replica`` copies for a
       replicated pool (3 by default, or 2 after
       ``ceph mon enable_stretch_mode``). Either zone can serve I/O on its
       own.
   * - ``min_size``
     - How many of a zone's shards or copies must be available **in that
       zone** before a PG serves I/O. It counts out of ``k+m`` or
       ``replica``, never out of ``size``. The default is ``k+1`` for an
       erasure-coded pool, or ``k`` when ``m`` is 1 (5 of 6 shards for a 4+2
       pool), and ``replica`` minus half of ``replica``, rounded down, for a
       replicated pool (1 of 2 copies, 2 of 3). See `How min_size Works`_.
   * - ``size``
     - The total over all zones, worked out from what each zone keeps:
       ``(k+m) × num_zones`` (12 for a 4+2 pool), or
       ``replica × num_zones`` (6 by default, or 4 after
       ``ceph mon enable_stretch_mode``).
   * - Cluster stretch flags
     - The cluster-wide state, set while any pool is stretched;
       ``ceph mon dump`` and ``ceph osd dump`` show it
   * - Stretch mode
     - The main type of stretch cluster, which this page describes: pools
       stretched with ``num_zones`` 2, and the Monitors handling zone
       failures. A cluster is in stretch mode while at least one pool has
       ``num_zones`` 2.
   * - Individual stretch pool
     - An advanced type of stretch cluster, usually for three-zone
       configurations, and an alternative to stretch mode. You cannot mix the
       use of stretch mode and individual stretch pools. See
       :ref:`individual_stretch_pools`.

A cluster can mix stretched pools with ordinary one-zone pools.
Erasure-coded and replicated pools can both be stretched. A stretched
erasure-coded pool uses EC optimizations (FastEC), which a pool created with
two zones gets automatically.


Prerequisites
=============

Verify the following before setup. The setup commands refuse a cluster that
does not meet these requirements.

* **Two zones for the OSDs, and only two.** The limit of two is a current
  restriction that will be lifted in a future release.
* **Zones of similar weight.** The two zones' total CRUSH weights must not
  differ by more than 10% of the lighter zone's weight; stretching is refused
  otherwise. Once the cluster is stretched, a larger difference raises
  ``STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE`` instead. To override this limit,
  see `Overriding the Zone Weight Limit`_.
* **Five Monitors:** two in each zone, and the tiebreaker at a third site.

  * The tiebreaker can run on a cloud-hosted VM. Size it like the other
    Monitors; see :ref:`hardware-cpu-memory`.
  * Its network latency to the zones can be higher than the latency between
    the zones.

* **If you intend to use HDDs, we recommend erasure-coded pools with m ≥ 2
  (not enforced).** See `Recommended Configuration on HDDs`_.
* **EC optimizations enabled on erasure-coded pools**: see
  :ref:`rados_ops_erasure_coding_optimizations`. A pool created with two zones
  gets them automatically; an existing pool needs them before it can be
  stretched.


.. _creating_stretch_pools:

Setting Up a Two-Zone Cluster
=============================

Four steps take a cluster from separate zones to a stretch cluster: describe
the zones to CRUSH, place the Monitors, create a pool with two zones, then
check. The examples use data centers as the zones, called ``dc1`` and
``dc2``, the tiebreaker's site ``dc3``, and the Monitors ``a`` to ``e``.

#. **Put each zone in the CRUSH map**, and move each host into its zone:

   .. prompt:: bash #

      ceph osd crush add-bucket dc1 datacenter
      ceph osd crush add-bucket dc2 datacenter
      ceph osd crush move dc1 root=default
      ceph osd crush move dc2 root=default
      ceph osd crush move {host} datacenter=dc1

   Repeat the last command for every host. Do not create a bucket for
   ``dc3``: the tiebreaker's site holds no OSDs.

#. **Set the CRUSH location of each Monitor:**

   .. prompt:: bash #

      ceph mon set_location a datacenter=dc1
      ceph mon set_location b datacenter=dc1
      ceph mon set_location c datacenter=dc2
      ceph mon set_location d datacenter=dc2
      ceph mon set_location e datacenter=dc3

#. **Create a pool with two zones.** An erasure-coded pool:

   .. prompt:: bash #

      ceph osd pool create myecpool --pool_type erasure --k 4 --m 2 --num_zones 2

   This keeps a full set of ``k+m`` = 6 shards in each zone. With the default
   ``min_size`` of 5 (``k+1``), each zone needs 5 of its 6 shards for the PG
   to serve I/O. (The total ``size`` is 12.) The pool is created with EC
   optimizations (FastEC) enabled. A replicated pool:

   .. prompt:: bash #

      ceph osd pool create mypool --num_zones 2 --replica 2

   This keeps ``replica`` 2, two copies, in each zone. With ``min_size`` 1,
   each zone needs 1 of its 2 copies. (The total ``size`` is 4.)

   The first pool with two zones puts the cluster in stretch mode: the
   Monitors switch the election strategy to ``connectivity`` (see
   :ref:`changing_monitor_elections`), and the one Monitor outside ``dc1``
   and ``dc2`` becomes the tiebreaker.

   Other pools, including ``.mgr``, stay in one zone. Stretch one with
   ``ceph osd pool set {pool-name} num_zones 2``, or stretch every pool at
   once with ``ceph mon enable_stretch_mode`` (see `Converting an Existing
   Cluster to Stretch Mode`_). An existing erasure-coded pool must have EC
   optimizations enabled before it can be stretched.

#. **Verify the configuration:**

   * ``ceph mon dump`` shows ``stretch_mode_enabled 1`` and the tiebreaker
     Monitor.
   * ``ceph osd pool get myecpool num_zones`` and
     ``ceph osd pool get mypool num_zones`` print 2.
   * ``ceph -s`` shows every PG ``active+clean`` and no stretch health
     warnings.

If a step is refused, the error message states the reason. Common causes are
a Monitor without a location, CRUSH weights more than 10% apart, or a pool
default overridden in a Monitor's ``ceph.conf``.


Running a Stretch Cluster
=========================

The Monitors handle a zone failure and its return automatically. Operators
monitor the health checks below and keep the tiebreaker Monitor available.

**The link between the zones fails (netsplit).** The tiebreaker picks the zone
with the better connection to it, and I/O continues in that zone. The other
zone stops serving I/O until the link is restored.

**A zone fails.** When every OSD and Monitor in one zone becomes unreachable,
the cluster enters *degraded* mode and raises ``DEGRADED_STRETCH_MODE``.

* Stretched pools continue to serve I/O from the copies in the surviving
  zone. Only that zone counts now: each PG needs ``min_size`` of its copies
  there. `How min_size Works`_ shows when a PG stops serving I/O.
* PGs report ``undersized``; this is expected. Ceph does not create additional
  copies in the surviving zone.
* One-zone pools behave as in any cluster: those in the lost zone are
  unavailable.

**The zone returns.** The cluster enters *recovery* mode and raises
``RECOVERING_STRETCH_MODE``. PGs peer with the zone that stayed up and recover
the returning one. When every PG is clean, the cluster returns to normal mode
automatically and requires both zones to peer again.

Monitor the following health checks:

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Health check
     - Meaning
   * - ``DEGRADED_STRETCH_MODE``
     - One zone is down; I/O runs in the other
   * - ``RECOVERING_STRETCH_MODE``
     - The lost zone is back and being recovered
   * - ``INCORRECT_NUM_BUCKETS_STRETCH_MODE``
     - The CRUSH map no longer has exactly two zones with OSDs
   * - ``STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE``
     - The zones' CRUSH weights differ by more than 10%
   * - ``NONEXISTENT_MON_CRUSH_LOC_STRETCH_MODE``
     - A Monitor's location names a bucket that is not in the CRUSH map
   * - ``POOL_DEFAULT_CONFIG_OVERRIDE``
     - A Monitor-level setting overrides a pool creation default that stretch
       mode set (see :ref:`pool-default-config-override`)

Operational notes:

* OSDs communicate only with the Monitors in their own zone, never with the
  tiebreaker.
* Every new Monitor needs a location. Deployment tools that start Monitors
  directly must pass ``--set-crush-location datacenter={dc}`` to
  ``ceph-mon`` instead of running ``ceph mon set_location``. The option takes
  a single ``bucket=loc`` pair, and its bucket type must be the zone type.
* To replace a failed tiebreaker, deploy a new Monitor at the third site and
  run ``ceph mon set_new_tiebreaker mon.{name}``; then remove the old one.
  The command refuses a Monitor in the same location as the other Monitors.
* If the cluster does not move to the next mode automatically, the
  transition can be forced; see `Forcing a Stretch Mode`_.


Single-Zone Pools
=================

A stretch cluster can also hold pools that live in one zone. Create one with
``num_zones`` 1 and name its zone with ``--root``. An erasure-coded pool:

.. prompt:: bash #

   ceph osd pool create localecpool --pool_type erasure --k 4 --m 2 --num_zones 1 --root dc1

A replicated pool:

.. prompt:: bash #

   ceph osd pool create localpool --num_zones 1 --root dc1

A single-zone pool takes no part in degraded or recovery mode: it is
unavailable while its zone is down. Without ``--root``, the pool uses the
default CRUSH root and spreads its copies over both zones without the failure
handling of stretch mode, so always name the zone.


Pool Defaults
=============

Setting the pool creation defaults is the preferred way to make new pools
stretched. ``ceph osd pool create`` takes every parameter that is not given on
the command line from the defaults, so with two zones as the default, each
new pool is stretched without extra options, including pools that Rook or
other tools create.

Change the defaults with ``ceph osd pool default set``. The command takes the
same parameters as ``ceph osd pool create``, and it checks that a pool created
with the resulting defaults would be a valid configuration, refusing defaults
that fail.

For erasure-coded pools:

.. prompt:: bash #

   ceph osd pool default set --pool_type erasure --k 4 --m 2 --num_zones 2 --zone_failure_domain datacenter

For replicated pools:

.. prompt:: bash #

   ceph osd pool default set --replica 2 --num_zones 2

``ceph osd pool default get`` shows the current defaults and where each one
comes from. :ref:`pool-creation-defaults` describes both commands and every
parameter.

* The command never changes an existing pool. To stretch existing pools, see
  `Changing the Number of Zones of a Pool`_: use
  ``ceph osd pool set {pool-name} num_zones 2`` for each pool, or
  ``ceph mon enable_stretch_mode`` for all of them.
* For an erasure-coded pool, ``--k`` and ``--m`` set the shards in each zone:
  with ``--k 4 --m 2 --num_zones 2``, a new pool has 6 shards in each zone and
  a ``size`` of 12. For a replicated pool, ``--replica`` is the number of
  copies in each zone: with ``--replica 2 --num_zones 2``, a new pool has a
  ``size`` of 4. ``--replica`` is stored in :confval:`osd_pool_default_size`,
  which keeps that name for backward compatibility.
* The command checks the parameters as ``ceph osd pool create`` would and
  refuses a default that fails, for example ``--num_zones 2`` on a cluster
  that cannot be stretched.
* Avoid setting or overriding the pool creation defaults manually, in
  ``ceph.conf`` or in a ``mon`` section of the configuration. See
  :ref:`pool-creation-defaults-directly`.


Changing the Number of Zones of a Pool
======================================

To stretch an existing pool, set its ``num_zones`` to 2:

.. prompt:: bash #

   ceph osd pool set mypool num_zones 2

* An erasure-coded pool keeps a full set of ``k+m`` shards in each zone; a
  replicated pool keeps ``replica`` copies in each zone. ``min_size`` is reset
  to its default, counted in each zone: ``k+1`` shards, or 1 of 2 copies.
* An erasure-coded pool must have EC optimizations enabled first:
  ``ceph osd pool set {pool-name} allow_ec_optimizations true``.
* ``--replica {count}`` sets the number of replicas in each zone; without it,
  the pool creation default applies.
* The pool gets a new CRUSH rule. It is built from ``--zone_failure_domain``,
  ``--osd_failure_domain``, ``--root`` and ``--class`` when they are given,
  and otherwise from the pool creation defaults set with
  ``ceph osd pool default set`` (see :ref:`pool-creation-defaults`), not from
  the pool's previous rule. An erasure-coded pool takes its root, OSD failure
  domain and device class from its erasure code profile instead. The previous
  rule is removed if no other pool uses it, even if it was written by hand. To
  use a custom rule, give it with ``--crush_rule``.
* Setting the pool's current ``num_zones`` again leaves the pool unchanged.
* The first pool set to two zones puts the cluster in stretch mode, as
  described in the setup steps.

To move an erasure-coded pool back into one zone, use pool migration
(``ceph osd pool migrate``) to copy it to a new pool with one zone. To move a
replicated pool back, set its ``num_zones`` to 1. See `Disabling Stretch
Mode`_.

To change the number of replicas in each zone of a replicated pool, run
``ceph osd pool set {pool-name} replica {count}`` (1 to 10). This recalculates
``min_size`` and replaces any custom value.


Converting an Existing Cluster to Stretch Mode
==============================================

``ceph mon enable_stretch_mode`` and ``ceph mon disable_stretch_mode`` change
every pool at once, and change the pool creation defaults to match, so that
new pools follow. To stretch or unstretch pools one at a time instead, set
their ``num_zones``: see `Setting Up a Two-Zone Cluster`_, `Pool Defaults`_
and `Changing the Number of Zones of a Pool`_.

``ceph mon enable_stretch_mode`` converts an existing cluster in one step:

.. prompt:: bash #

   ceph mon enable_stretch_mode --dividing_bucket datacenter

It is equivalent to changing the pool creation defaults with
``ceph osd pool default set``, then converting each existing pool into a
stretched pool:

.. prompt:: bash #

   ceph osd pool default set --replica 2 --num_zones 2 --zone_failure_domain datacenter
   ceph osd pool set {pool-name} num_zones 2 --zone_failure_domain datacenter
   ceph osd pool set {pool-name} replica 2

The second command is run for every pool, and the third for every replicated
pool. Unlike those commands, ``enable_stretch_mode`` changes every pool or
none. It also takes ``--new_crush_rule {rule}``, which gives every replicated
pool that rule instead of a generated one, and ``--tiebreaker_mon {mon}``
(see `Manually Specifying the Tiebreaker`_).

On a cluster with no pools, the command configures the Monitors and the pool
creation defaults, and the first stretched pool (normally ``.mgr``) sets the
cluster stretch flags. Afterwards:

* **The cluster stretch flags follow the pools.** They are set while at least
  one pool has ``num_zones`` 2, and they are what make the Monitors handle a
  zone failure. When the last stretched pool is deleted or set to one zone,
  the flags clear and the tiebreaker is released.
* **Ceph does not record how a pool became stretched.** After
  ``enable_stretch_mode``, each pool can be changed on its own, and stretched
  and one-zone pools can be mixed.
* **The command can be run again.** A second run stretches any pools created
  in the meantime with ``--num_zones 1``.

To turn stretch mode off, see `Disabling Stretch Mode`_.


Upgrading from Earlier Releases
===============================

Stretch mode does the same job as in earlier releases, but the purpose of
``ceph mon enable_stretch_mode`` and ``ceph mon disable_stretch_mode`` has
changed subtly. In earlier releases they switched the cluster into and out of
stretch mode, and were the only way to stretch pools. Now stretch mode follows
the pools: the cluster is in stretch mode while any pool has ``num_zones`` 2,
however it was stretched. The two commands set ``num_zones`` on every pool at
once, and change the pool creation defaults to match so that new pools follow.
After ``enable_stretch_mode``, each pool can still be changed on its own. The
differences:

.. list-table::
   :header-rows: 1
   :widths: 24 38 38

   * -
     - Earlier releases
     - Now
   * - How a pool is stretched
     - Only ``ceph mon enable_stretch_mode``, for every pool
     - Per pool with ``num_zones``, or every pool with
       ``enable_stretch_mode``
   * - CRUSH rule
     - Write one by hand and pass it to the command
     - Ceph makes a rule for each pool; a hand-written rule is optional
   * - ``min_size``
     - A total over both zones (2 of 4)
     - Per zone (1 of 2 in each)
   * - Degraded mode
     - The Monitors halve each pool's ``min_size``
     - ``min_size`` stays as it is, because it already counts one zone
   * - Pool creation defaults
     - Ignored; every pool gets ``mon_stretch_pool_size``
     - Used: :confval:`osd_pool_default_size` counts the replicas in each
       zone
   * - Erasure-coded pools
     - Not supported
     - Supported, as FastEC pools
   * - Mixing stretched and one-zone pools
     - Not possible
     - Possible

An existing stretch cluster needs no action to upgrade. Until the upgrade is
committed it behaves exactly as the earlier release did, including when a
zone fails mid-upgrade; the new per-pool behaviour starts at the commit.

The upgrade has three stages:

#. **Monitors, then OSDs, move to the new release.** Every daemon treats pools
   as the earlier release does: ``min_size`` is a total over both zones, and
   the Monitors halve it while a zone is down and restore it afterwards. Until
   the commit:

   * ``ceph mon enable_stretch_mode`` and ``ceph mon disable_stretch_mode``
     work as in the earlier release. ``enable_stretch_mode`` still needs a
     hand-written CRUSH rule, and it fails while an individual stretch pool
     exists.
   * New pools get one zone. In stretch mode, a new replicated pool gets the
     stretch CRUSH rule and ``mon_stretch_pool_size`` copies, as before.
   * Creating a pool with ``--num_zones 2``, changing a pool's ``num_zones``,
     ``ceph osd pool default set``, and creating an erasure-coded pool in
     stretch mode are refused.
   * ``ceph osd pool set {pool-name} replica {count}`` sets the total number
     of copies, like ``size``: ``replica 2`` leaves two copies in all, not two
     in each zone.
   * A pool whose CRUSH rule picks a device class stops serving I/O while a
     zone is down, as in the earlier release, until the zone returns.

#. **The commit:** ``ceph osd require-osd-release vampire``. It is refused
   until every Monitor, and every OSD that is up, runs the new release. In
   this one OSDMap update the Monitors give each pool in stretch mode two
   zones, with half its copies in each, and ``min_size`` becomes a count in
   each zone: 2 of 4 becomes 1 of 2 in each zone. An odd value rounds up, so
   3 of 4 becomes 2 of 2 in each zone: stricter, never weaker. If a zone is
   down at the commit, the value is worked out from
   ``mon_stretch_pool_min_size``, so it is right once the zone returns. Every
   PG of a stretch pool re-peers once. The Monitors also set the pool creation
   defaults for stretched pools, in the ``global`` section of the
   configuration database: :confval:`osd_pool_default_num_zones` to ``2``,
   :confval:`osd_pool_default_zone_failure_domain` to the dividing bucket
   type, and :confval:`osd_pool_default_size` to half of
   ``mon_stretch_pool_size``.

   Only a replicated pool whose CRUSH rule places half of an even number of
   copies in each zone can be split this way. Any other pool, for example one
   whose ``size`` was set to 3, keeps one zone and the rules of earlier
   releases, and ``ceph mon disable_stretch_mode`` leaves it stretched.
   ``STRETCH_MODE_POOL_NOT_CONVERTED`` names such pools before the commit,
   while they can still be changed, and after it.

#. **After the commit,** everything on this page applies. OSDs of the earlier
   release can no longer start, on any cluster, stretched or not: upgrade any
   OSD that was down during the commit, for example in a lost zone, before it
   returns. With ``--yes-i-really-mean-it`` the commit goes ahead while older
   OSDs are up; they are marked down and cannot rejoin until they are
   upgraded.

Required actions by deployment type:

.. list-table::
   :header-rows: 1
   :widths: 20 50 30

   * - Deployment
     - Must change
     - Can now
   * - **Rook or other orchestrators**
     - Nothing in Rook's default configuration: Rook's stretch cluster
       commands behave as before. Two settings change meaning. A pool default
       such as ``osd pool default size`` in ``rook-config-override`` (a
       Monitor's local configuration) makes ``enable_stretch_mode`` fail, so
       remove it. And :confval:`osd_pool_default_size` in
       ``spec.cephConfig`` now counts copies in each zone for pools created
       without a size; Rook's own pools always pass ``--size 4``.
     - Create pools with ``--num_zones`` and ``--replica``; drop
       hand-written stretch rules
   * - **A cluster in stretch mode**
     - Nothing, unless ``STRETCH_MODE_POOL_NOT_CONVERTED`` names a pool:
       change it before the commit, for example with
       ``ceph osd pool set {pool-name} size 4`` if its rule places two copies
       in each zone. If a Monitor's ``ceph.conf`` or a ``mon`` section sets
       :confval:`osd_pool_default_size`, remove it after the commit:
       ``POOL_DEFAULT_CONFIG_OVERRIDE`` says so.
     - Mix stretched and one-zone pools; stretch erasure-coded pools
   * - **Individual stretch pools**
     - Nothing. They keep ``num_zones`` 1 and a ``min_size`` counted over all
       zones. Their PGs re-peer once at the commit, and from then on OSDs
       count a zone once even if the CRUSH rule reaches it through two device
       classes.
     - See :ref:`individual_stretch_pools`
   * - **A cluster that is not stretched**
     - Nothing
     - Stretch it as in `Setting Up a Two-Zone Cluster`_

After the commit, :confval:`osd_pool_default_size` means copies **in each
zone** for a stretched pool. For a one-zone pool that is the same as before.

What Has Not Changed
--------------------

A stretch cluster still behaves as it did in earlier releases when a zone
fails. What changed is how pools are stretched and how ``min_size`` is
counted, not how the cluster handles failures.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Area
     - Same as in earlier releases
   * - Layout
     - Two zones of similar CRUSH weight; five Monitors, two in each zone and
       a tiebreaker at a third site; the ``connectivity`` election strategy
   * - Copies
     - 2 copies of each object in each zone by default, 4 in all
   * - When I/O stops
     - With the defaults, a PG serves I/O while each zone holds at least one
       copy. While a zone is down, one copy in the surviving zone is enough.
   * - Failure handling
     - The same three modes (normal, degraded, recovery), switched
       automatically by the Monitors; the same netsplit handling by the
       tiebreaker
   * - Health checks
     - ``DEGRADED_STRETCH_MODE``, ``RECOVERING_STRETCH_MODE``,
       ``INCORRECT_NUM_BUCKETS_STRETCH_MODE``,
       ``NONEXISTENT_MON_CRUSH_LOC_STRETCH_MODE``,
       ``STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE``
   * - Commands
     - ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode`` still
       stretch and unstretch the whole cluster.
       ``ceph mon set_new_tiebreaker``,
       ``ceph osd force_recovery_stretch_mode`` and
       ``force_healthy_stretch_mode`` are unchanged.
   * - Tuning options
     - ``mon_stretch_cluster_recovery_ratio``,
       ``mon_stretch_recovery_min_wait``,
       ``mon_warn_on_degraded_stretch_mode``,
       ``mon_stretch_max_bucket_weight_delta``
   * - Individual stretch pools
     - ``ceph osd pool stretch set`` and ``unset`` work as before


.. _reducing_num_zones:

Disabling Stretch Mode
======================

For a Single Pool
-----------------

**Erasure-coded pools.** Use pool migration (``ceph osd pool migrate``) to
move the data to a new pool with one zone. The data remains available while
it is copied. To unstretch an erasure-coded pool in place instead, see
`Unstretching an Erasure-Coded Pool Without Migration`_.

**Replicated pools.** Set ``num_zones`` to 1 and name the zone that keeps the
pool with ``--root``:

.. prompt:: bash #

   ceph osd pool set mypool num_zones 1 --root dc1

The pool keeps its existing copies, in either zone, while its data moves into
``dc1``. Without ``--root``, the pool uses the default CRUSH root and stays
spread over both zones, without the failure handling of stretch mode.

For Every Pool
--------------

Like ``ceph mon enable_stretch_mode``, this command changes every pool at
once. To unstretch one pool at a time, use the commands above.

``ceph mon disable_stretch_mode --yes-i-really-mean-it`` unstretches every
pool at once:

* Replicated pools get 3 replicas.
* Erasure-coded pools are taken offline during the transition. To keep them
  available, migrate them first (see `For a Single Pool`_).
* The pool creation defaults are reset to 1 zone with 3 replicas.

Afterwards, remove any OSDs that are no longer required.

Deleting the last stretched pool, or setting it to one zone, also clears the
cluster stretch flags. Ceph then treats the cluster as no longer stretched:
the Monitors stop handling zone failures, and pools on the default CRUSH root
are distributed across the OSDs of both zones without regard to zones. The
pool creation defaults are not changed in that case: if they still specify 2
zones, the next pool created with them is stretched again.

While a Zone Is Down or Recovering
----------------------------------

A pool can be unstretched while the cluster is in degraded or recovery mode:

* For a replicated pool, name the surviving zone with ``--root``, for example
  ``ceph osd pool set mypool num_zones 1 --root dc1``. Every PG then maps to
  OSDs that are up, and the pool remains available. Without ``--root``, PGs
  that the new rule maps only to the lost zone's OSDs remain unavailable until
  those OSDs are marked ``out``.
* An erasure-coded pool keeps the shards of one zone for each PG. PGs whose
  shards are kept in the lost zone remain unavailable until that zone
  returns.

``ceph mon disable_stretch_mode`` can also be run in degraded mode, but the
command is currently refused in recovery mode. To leave recovery mode first,
see `Forcing a Stretch Mode`_.


Troubleshooting
===============

Most problems show up as a refused command, a PG that stops serving I/O, or a
mode that does not change. Every refusal names its reason in the error
message.

A Command to Stretch a Pool or the Cluster Is Refused
-----------------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * - The error names
     - Cause
     - What to do
   * - Zone weights that differ
     - The two zones' CRUSH weights are more than 10% apart
     - Balance the zones, or see `Overriding the Zone Weight Limit`_
   * - A Monitor without a location
     - Every Monitor needs a CRUSH location
     - ``ceph mon set_location {mon} datacenter={dc}``
   * - A setting "in the local configuration of mon.X"
     - A pool creation default is set in a Monitor's ``ceph.conf`` (with
       Rook, ``rook-config-override``), environment or command line, which
       overrides the defaults that stretching writes
     - Remove it there, and set defaults with ``ceph osd pool default set``
   * - Too few hosts in a zone for the replica count
     - Each zone needs at least ``replica`` hosts, one copy per host
     - Add hosts, lower ``--replica``, or choose another
       ``--osd_failure_domain``
   * - EC optimizations
     - The erasure-coded pool does not have FastEC enabled
     - ``ceph osd pool set {pool-name} allow_ec_optimizations true``
   * - An individual stretch pool
     - Individual stretch pools and stretched pools cannot be combined
     - Revert them with ``ceph osd pool stretch unset`` first
   * - The number of zones
     - The CRUSH map does not have exactly two zones that hold OSDs
     - Give OSDs to two zones only; the tiebreaker's site holds none
   * - The upgrade commit
     - Until the upgrade is committed, pools keep one zone, pool creation
       defaults cannot be set, and in stretch mode erasure-coded pools cannot
       be created
     - Finish the upgrade and run ``ceph osd require-osd-release vampire``

PGs Stop Serving I/O
--------------------

.. list-table::
   :header-rows: 1
   :widths: 34 33 33

   * - What you see
     - Cause
     - What to do
   * - PGs stop although one zone is healthy, and
       ``ceph osd dump | grep stretch`` shows ``degraded_stretch_mode 0``
     - The cluster is still in normal mode, where both zones count. It
       enters degraded mode only when every OSD and Monitor of the failed
       zone is down.
     - Bring the zone back, or wait until all of its OSDs and Monitors are
       marked down
   * - PGs stop while a zone is down (degraded mode)
     - Fewer than ``min_size`` copies are left in the surviving zone
     - Check ``ceph osd pool get {pool-name} min_size`` (a count per zone)
       and bring the surviving zone's OSDs back; see `How min_size Works`_
   * - Before the upgrade is committed, a pool with a device-class CRUSH
       rule stops while a zone is down
     - The earlier release's behaviour, kept until the commit
     - Bring the zone back, or commit the upgrade
   * - Writes stop although one zone has free space
     - The lighter zone's OSDs are full; a stretched pool needs room in both
       zones
     - Add capacity to that zone
   * - One zone stops serving I/O when the link between the zones fails
     - Expected: the tiebreaker keeps the zone with the better connection
     - Restore the link; ``ceph mon dump`` shows the tiebreaker

Other Problems
--------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - What you see
     - Cause
     - What to do
   * - The cluster stays in recovery mode
     - Some PGs are not clean yet; recovery mode ends
       ``mon_stretch_recovery_min_wait`` seconds after every PG is clean
     - ``ceph pg stat`` and ``ceph pg ls`` show the PGs still recovering;
       force normal mode only after checking every PG can peer (see
       `Forcing a Stretch Mode`_)
   * - ``POOL_DEFAULT_CONFIG_OVERRIDE``
     - A pool creation default is set in a ``mon`` section or a Monitor's
       local configuration, overriding the global value
     - Remove it there (see :ref:`pool-default-config-override`)
   * - New pools are not stretched
     - The pool creation defaults say one zone
     - ``ceph osd pool default get`` shows each default and where it comes
       from; ``ceph osd pool default set --num_zones 2``
   * - ``ceph osd ok-to-stop`` refuses to stop all the OSDs of a zone
     - ``min_size`` counts per zone, so stopping a whole zone in normal mode
       would stop PGs
     - Stop OSDs in smaller batches, for example one host per zone at a time
   * - ``ceph osd pool set {pool-name} size`` is refused on a stretched pool
     - ``size`` is worked out from the zones
     - Use ``ceph osd pool set {pool-name} replica {count}``


Advanced Options
================

The following options are not required for a typical deployment.

How min_size Works
------------------

For a stretched pool, ``min_size`` counts shards or copies in each zone, out
of that zone's ``k+m`` shards or ``replica`` copies. A PG serves I/O only
while every zone that counts has at least ``min_size`` of them available. The
cluster's mode decides which zones count; ``min_size`` itself never changes.

.. list-table::
   :header-rows: 1
   :widths: 31 23 23 23

   * - Pool
     - In each zone
     - Default ``min_size``
     - Allowed ``min_size``
   * - Erasure-coded 4+2
     - 6 shards
     - 5
     - 4 to 6
   * - Erasure-coded 2+1
     - 3 shards
     - 2
     - 2 to 3
   * - Replicated, ``replica`` 2
     - 2 copies
     - 1
     - 1 to 2
   * - Replicated, ``replica`` 3
     - 3 copies
     - 2
     - 1 to 3

The erasure-coded default is ``k+1``, or ``k`` when ``m`` is 1. The
replicated default is ``replica`` minus half of ``replica``, rounded down,
unless :confval:`osd_pool_default_min_size` is set. The allowed range is ``k``
to ``k+m``, or 1 to ``replica``.

When a PG serves I/O, for the default pools above:

.. list-table::
   :header-rows: 1
   :widths: 16 20 32 32

   * - Mode
     - Zones that count
     - Erasure-coded 4+2, ``min_size`` 5
     - Replicated, ``replica`` 2, ``min_size`` 1
   * - Normal
     - Both
     - Each zone needs 5 of its 6 shards. Losing 2 shards in one zone stops
       the PG.
     - Each zone needs 1 of its 2 copies. If either zone has none, the PG
       stops, even though the other zone has both.
   * - Degraded (a zone is down)
     - The surviving zone only
     - 5 of the 6 shards in the surviving zone
     - 1 of the 2 copies in the surviving zone is enough; the PG stops if
       both are lost
   * - Recovery (the lost zone is back)
     - The surviving zone only, until every PG is clean
     - As in degraded mode
     - As in degraded mode
   * - Netsplit
     - The zone the tiebreaker keeps
     - As in degraded mode, once the other zone's OSDs are marked down
     - As in degraded mode, once the other zone's OSDs are marked down

* In normal mode both zones must meet ``min_size``. A PG that loses every copy
  in one zone stops until those copies return, or until the Monitors declare
  the whole zone down and enter degraded mode.
* ``ceph osd pool set {pool-name} min_size {count}`` sets the count in each
  zone. ``ceph osd pool set {pool-name} replica {count}``, and changing
  ``num_zones``, reset it to the default.
* ``ceph osd pool get {pool-name} min_size`` and ``ceph osd pool ls detail``
  print the count in each zone. ``ls detail`` shows the zones and the copies
  in each zone before it, so a default replicated pool shows
  ``size 4 num_zones 2 replica 2 min_size 1``.
* One-zone pools and individual stretch pools count ``min_size`` over all
  their copies.
* A larger ``min_size`` means every acknowledged write is held by more OSDs,
  but PGs stop sooner. With ``replica`` 2 and ``min_size`` 1, a PG in degraded
  mode accepts writes held by one OSD. For more protection, use ``replica`` 3
  with ``min_size`` 2.

Recommended Configuration on HDDs
---------------------------------

On HDDs, we recommend double redundancy in each zone: each zone should be able
to lose two of a PG's copies or shards without losing data. HDDs take a long
time to rebuild, both after an OSD fails and when a zone returns after an
outage, so a second failure before recovery finishes is more likely. While
one zone is down, the other zone holds the only copies of the data.

We recommend erasure-coded pools with m ≥ 2: erasure-coded configurations are
usually more affordable than replicated ones. A replicated pool gets double
redundancy with ``replica`` 3 in each zone (``--num_zones 2 --replica 3``).

Inspecting a Stretched Pool
---------------------------

``ceph osd pool stretch show {pool-name}`` prints the stretch settings of a
stretched pool: its peering values (``peering_crush_bucket_count``,
``peering_crush_bucket_target``, ``peering_crush_bucket_barrier``), CRUSH
rule, ``size`` and ``min_size``. It also works for an individual stretch
pool, and reports a pool that is not stretched.

``ceph osd pool get {pool-name} num_zones`` and
``ceph osd pool get {pool-name} replica`` show the zone and replica counts.

CRUSH Rules
-----------

Ceph generates a CRUSH rule for each pool with two zones, so a custom rule is
optional:

* An erasure-coded pool gets a rule that chooses 2 buckets of the zone type,
  then all ``k+m`` shards in each, one per host. For example, for ``ecpool``
  created with ``--k 4 --m 2``::

     rule ecpool {
         type erasure
         step set_chooseleaf_tries 5
         step set_choose_tries 100
         step take default
         step choose firstn 2 type datacenter
         step chooseleaf indep 6 type host
         step emit
     }

* A replicated pool gets a rule that chooses 2 buckets of the zone type, then
  the pool's replicas in each, one per host. For example, for ``mypool`` with
  2 replicas in each zone::

     rule mypool {
         type replicated
         step take default
         step choose firstn 2 type datacenter
         step chooseleaf firstn 2 type host
         step emit
     }

* ``--zone_failure_domain``, ``--osd_failure_domain``, ``--root`` and
  ``--class`` on ``ceph osd pool create`` or
  ``ceph osd pool set {pool-name} num_zones 2`` change the generated rule: the
  zone bucket type, the bucket type for the copies within a zone (default
  ``host``), the CRUSH root, and the device class.
* Changing ``replica`` on a two-zone pool assigns a rule named
  ``{pool-name}-replica-{count}``.
* Setting a pool to one zone assigns the default replicated rule, or a
  one-zone rule for an erasure-coded pool. Replaced rules are removed when no
  other pool references them.
* ``ceph mon disable_stretch_mode {rule} --yes-i-really-mean-it`` moves every
  replicated pool to ``{rule}`` instead of the default rule.

To use a custom rule:

* Specify ``--crush_rule {rule}`` on ``ceph osd pool create`` or on
  ``ceph osd pool set {pool-name} num_zones 2``, or later with
  ``ceph osd pool set {pool-name} crush_rule {rule}``.
* ``ceph mon enable_stretch_mode --new_crush_rule {rule}`` gives every
  replicated pool the same rule, which must place 2 copies in each zone.
* Prefer ``step take default`` then ``step choose firstn 0 type datacenter``.
  A rule with one ``take`` step per zone makes ``MAX AVAIL`` report the space
  of one zone only, not of both.
* A rule can pick one device class, for example
  ``step take default class ssd``.

To write a rule, get and decompile the CRUSH map, add the rule with an unused
``id``, then compile and set the map (see :ref:`rados-crush-map-edits`):

.. prompt:: bash #

   ceph osd getcrushmap > crush.map.bin
   crushtool -d crush.map.bin -o crush.map.txt
   crushtool -c crush.map.txt -o crush2.map.bin
   ceph osd setcrushmap -i crush2.map.bin

Moving a stretched pool to another device class or failure domain needs a
hand-written rule and ``ceph osd pool set {pool-name} crush_rule {rule}``.

**Where the primaries are.** Each PG's primary OSD receives the PG's writes,
and its reads unless the client uses localized reads. The CRUSH rule decides
which zone holds the primaries:

* A rule that chooses the zones, as the generated rules do
  (``step choose firstn 2 type datacenter``) or
  ``step choose firstn 0 type datacenter`` does, orders the zones differently
  for each PG. Each zone holds the primaries of about half the PGs, so the
  primary load is spread over both zones. A write to a PG whose primary is in
  the other zone crosses the link between the zones twice: from the client to
  the primary, and from the primary to the copies in the client's zone.
* A rule with one ``take`` step per zone puts every primary in the zone it
  takes first. Use it when the clients run in one zone, so that their writes
  reach a primary in their own zone. That zone's OSDs then do all the primary
  work, ``MAX AVAIL`` reports one zone's space, and if that zone fails the
  primaries move to the other zone. For example, to keep the primaries of a
  replicated pool in ``dc1``::

     rule mypool_primary_dc1 {
         type replicated
         step take dc1
         step chooseleaf firstn 2 type host
         step emit
         step take dc2
         step chooseleaf firstn 2 type host
         step emit
     }

The primary of an erasure-coded pool is one of the first zone's shards.

Changing the Zone Failure Domain
--------------------------------

By default, zones are CRUSH buckets of type ``datacenter``, as set by
:confval:`osd_pool_default_zone_failure_domain`. To use another bucket type,
for example ``room`` or ``rack``:

* For all new pools, set the pool creation default with
  ``ceph osd pool default set --zone_failure_domain {type}``.
* For one pool, pass ``--zone_failure_domain {type}`` to
  ``ceph osd pool create`` or to
  ``ceph osd pool set {pool-name} num_zones 2``.
* For an erasure-coded pool, ``--zone_failure_domain`` takes precedence, then
  the erasure code profile's ``crush-zone-failure-domain``, then the pool
  creation default.

The Monitors' CRUSH locations must use the same bucket type. Changing the
failure domain of a pool that is already stretched requires a hand-written
rule (see `CRUSH Rules`_).

Manually Specifying the Tiebreaker
----------------------------------

By default, the tiebreaker is the one Monitor whose location is outside the
two zones, for example outside ``dc1`` and ``dc2``. To specify it explicitly,
add ``--tiebreaker_mon`` to ``ceph mon enable_stretch_mode``:

.. prompt:: bash #

   ceph mon enable_stretch_mode --dividing_bucket datacenter --tiebreaker_mon e

Once the cluster is in stretch mode, ``ceph mon set_new_tiebreaker
mon.{name}`` changes the tiebreaker, for example to replace a failed one.

Tuning Degraded and Recovery Mode
---------------------------------

The following Monitor options control the transitions between modes:

.. list-table::
   :header-rows: 1
   :widths: 40 10 50

   * - Option
     - Default
     - Effect
   * - ``mon_stretch_cluster_recovery_ratio``
     - ``0.6``
     - The fraction of OSDs that must be up for a degraded cluster to enter
       recovery mode (0.51 to 1)
   * - ``mon_stretch_recovery_min_wait``
     - ``15``
     - The time in seconds that the Monitors wait in recovery mode before
       treating fully healthy PGs as evidence that the cluster can return to
       normal mode
   * - ``mon_warn_on_degraded_stretch_mode``
     - ``true``
     - Whether degraded mode raises the ``DEGRADED_STRETCH_MODE`` health
       warning

Set them with ``ceph config set mon {option} {value}``.

Forcing a Stretch Mode
----------------------

The Monitors move the cluster between normal, degraded and recovery mode
automatically. If a transition does not happen, or must happen early, it can
be forced.

**Forcing recovery mode.** In degraded mode, the cluster enters recovery mode
when the lost zone returns. To enter recovery mode before that:

.. prompt:: bash #

   ceph osd force_recovery_stretch_mode --yes-i-really-mean-it

**Forcing normal mode.** In recovery mode, the cluster returns to normal mode
when the PGs are healthy. To return to normal mode before that:

.. prompt:: bash #

   ceph osd force_healthy_stretch_mode --yes-i-really-mean-it

The command fails if the cluster is not in recovery mode. Forcing normal mode
before every PG has recovered can leave PGs unable to peer and data
unavailable; use it only after verifying that every PG can peer. It also
clears the ``RECOVERING_STRETCH_MODE`` health warning, and allows
``ceph mon disable_stretch_mode``, which is refused in recovery mode.

Overriding the Zone Weight Limit
--------------------------------

``mon_stretch_max_bucket_weight_delta`` sets how far apart the two zones'
CRUSH weights can be, as a fraction of the lighter zone's weight. The default
is ``0.1`` (10%). Raising it is an override of a safety check, not a
supported configuration change:

.. prompt:: bash #

   ceph config set mon mon_stretch_max_bucket_weight_delta 0.25

The limit is checked when the cluster enters stretch mode, and it sets the
threshold for the ``STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE`` health warning
afterwards.

Side effects of zones with unequal weights:

* **Capacity.** A stretched pool keeps a full copy in each zone, so its usable
  capacity is limited by the lighter zone. The extra capacity of the heavier
  zone is available only to one-zone pools.
* **Full OSDs.** The lighter zone's OSDs reach ``nearfull`` and ``full``
  first. When they are full, writes to every stretched pool stop, even though
  the heavier zone still has free space.
* **Load.** The lighter zone serves the same writes and recovery traffic with
  fewer OSDs, so each of its OSDs carries more load and client latency can
  rise.
* **Recovery.** When the lighter zone returns after a failure, it receives a
  full copy of the changed data on fewer OSDs, so recovery mode lasts longer.
* **Hidden drift.** A higher limit also raises the threshold of
  ``STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE``, so the health warning no longer
  reports zones that drift further apart, for example after OSD failures or
  additions in one zone.

Unstretching an Erasure-Coded Pool Without Migration
----------------------------------------------------

To unstretch an erasure-coded pool in place, set ``num_zones`` to 1 and name
the zone that keeps the pool with ``--root``:

.. prompt:: bash #

   ceph osd pool set myecpool num_zones 1 --root dc1 --yes-i-really-mean-it

The pool is taken offline during the transition, and
``--yes-i-really-mean-it`` is required because PGs can become unavailable.
Each PG keeps only the shards of its first zone, shards ``0`` to ``k+m-1``,
and its ``min_size`` becomes ``k + min(1, m - 1)``. CRUSH chooses the first
zone for each PG, so each zone is the first zone of about half the PGs. The
other zone's shards are deleted once the PG is clean; until then they are a
source for recovery.

* If every PG is ``active+clean``, no PG becomes unavailable.
* If a zone is lost, a PG whose first zone survived stays available as long
  as at least ``min_size`` of its OSDs in that zone are up. A PG whose first
  zone is the lost zone, about half of them, is unavailable until that zone
  returns. It then recovers every write made in between from the other zone's
  shards.
* A PG also stays unavailable, even with every OSD up, if fewer than
  ``min_size`` of the shards it kept are complete and up to date. That
  happens to shards that were still being backfilled, as in recovery mode,
  and to shards whose zone was lost for more writes than the PG log holds.
  Setting ``num_zones`` back to ``2`` makes such PGs available, even while a
  zone is still lost; reduce the pool again once every PG is
  ``active+clean``.
* As for a replicated pool, a PG is unavailable while its new rule maps it
  only to OSDs that are down.

The Monitors do not check for these conditions before they make the change.
To keep the data available, wait until every OSD is up and every PG is
``active+clean``, or use pool migration instead (see `Disabling Stretch
Mode`_).

.. _stretch_mode_perf_counters:

Measuring Traffic Between Zones
-------------------------------

OSDs count the traffic they send to the other zone: write fan-out, recovery
pushes and remote reads. A primary OSD counts the sub-operations that it sends
to OSDs in another zone, for erasure-coded and replicated pools alike. Read
the counters with ``ceph tell osd.{id} perf dump``.

Write fan-out counters track shard data sent to replica OSDs in remote zones:

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Counter
     - Description
   * - ``cross_zone_write_ops``
     - Number of cross-zone sub-write operations issued.
   * - ``cross_zone_write_bytes``
     - Total bytes of shard data sent to OSDs in the remote zone during
       write fan-out.
   * - ``cross_zone_write_latency``
     - Average round-trip latency for cross-zone sub-write operations.
   * - ``cross_zone_write_latency_bytes_histogram``
     - Histogram of the latency and size of cross-zone sub-write operations.

Recovery push counters track shard data sent to remote-zone OSDs during PG
recovery:

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Counter
     - Description
   * - ``cross_zone_recovery_push_ops``
     - Number of cross-zone recovery push operations issued.
   * - ``cross_zone_recovery_push_bytes``
     - Total bytes pushed to remote-zone OSDs during recovery.
   * - ``cross_zone_recovery_push_latency``
     - Average round-trip latency for cross-zone recovery push
       operations.
   * - ``cross_zone_recovery_push_latency_bytes_histogram``
     - Histogram of the latency and size of cross-zone recovery pushes.

Remote-zone read counters track data fetched from remote-zone OSDs:
erasure-coded shards read when the primary's zone cannot serve a read, and
objects that a replicated pool's primary pulls during recovery:

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Counter
     - Description
   * - ``cross_zone_subread_ops``
     - Number of cross-zone sub-read and pull operations issued.
   * - ``cross_zone_subread_sent_bytes``
     - Total bytes that erasure coded sub-reads requested from remote-zone
       OSDs.
   * - ``cross_zone_subread_recv_bytes``
     - Total bytes of data received from remote-zone OSDs in response
       to cross-zone read requests.
   * - ``cross_zone_subread_latency``
     - Average round-trip latency for cross-zone sub-read operations.
   * - ``cross_zone_subread_latency_bytes_histogram``
     - Histogram of the latency and size of cross-zone sub-read operations.


More Than Two Zones
===================

Stretch mode is limited to two zones. Over three or more zones, there are two
advanced types of stretch cluster: an individual stretch pool for a replicated
pool, or an erasure-coded pool spread across the zones by hand. See
:ref:`individual_stretch_pools`. The Monitors do not handle zone failures for
them, so interventions may be necessary after a zone failure.
