.. _individual_stretch_pools:

========================
Individual Stretch Pools
========================

.. important:: **Use stretch mode instead where possible.** See
   :ref:`stretch_mode`. Individual stretch pools and erasure-coded pools
   spread across zones are advanced configurations with no automatic failure
   handling: the operator must provide the CRUSH rules and the automation that
   responds to zone failures.

Setting individual stretch pool attributes allows specific pools to be
distributed across two or more zones. This is done by running
``ceph osd pool stretch set`` on each pool; see
:ref:`setting_values_for_a_stretch_pool`.

Use stretch mode when there are exactly two zones. Use an individual stretch
pool only when a particular replicated pool must span more than two zones.
For an erasure-coded pool over three or more zones, spread its shards with a
CRUSH MSR rule instead: see `Erasure-Coded Pools Spread Across Zones`_.

The intent is to support three zones in stretch mode (``num_zones`` 3) in a
future release, then to deprecate individual stretch pools, with an upgrade
path, and later to remove them.

Individual stretch pools and stretch mode cannot be combined. While any pool
is stretched, ``ceph osd pool stretch set`` and
``ceph osd pool stretch unset`` fail. While an individual stretch pool
exists, the following fail until the pool is reverted with
``ceph osd pool stretch unset``:

* creating a pool with two zones, whether from ``--num_zones 2`` or from
  :confval:`osd_pool_default_num_zones`;
* ``ceph osd pool set {pool-name} num_zones 2``;
* ``ceph mon enable_stretch_mode``.


Comparison with Stretch Mode
============================

.. list-table::
   :header-rows: 1
   :widths: 16 28 28 28

   * -
     - Stretched pool (``num_zones`` 2)
     - Individual stretch pool
     - Erasure-coded pool spread across zones
   * - Zones
     - Exactly two, plus a tiebreaker site
     - Two or more
     - Three or more
   * - Netsplit
     - I/O carries on in the zone the tiebreaker picks
     - I/O stops until the netsplit ends
     - No tiebreaker, as for an individual stretch pool
   * - A zone fails
     - Degraded mode: I/O carries on in the other zone
     - Nothing changes automatically: the pool's ``min_size`` and the zones
       its PGs need stay as they are
     - Nothing changes automatically; I/O continues while the shards left
       reach ``min_size``
   * - Who responds to failures
     - The Monitors
     - The operator, often with automation
     - The operator, often with automation
   * - ``min_size`` counts
     - Copies in each zone
     - Copies in all zones together
     - Shards in all zones together
   * - Erasure-coded pools
     - Yes, FastEC
     - No, replicated only
     - Erasure-coded only, with an MSR rule


Operator Responsibilities
=========================

Ceph does not manage an individual stretch pool, or an erasure-coded pool
spread across zones, after it is configured. The operator is responsible for:

* **CRUSH rules.** An individual stretch pool needs a hand-written rule that
  places the required number of copies in each zone. An erasure-coded pool
  spread across zones gets an MSR rule from its erasure code profile, or a
  hand-written one.
* **Monitor placement.** The Monitors need CRUSH locations and the
  ``connectivity`` election strategy.
* **Failure handling.** When a zone fails or returns, Ceph does not change the
  pool's ``size``, ``min_size`` or peering values. External scripts or
  automation must set them to values that keep the pool available, and
  restore them afterwards.
* **Marking OSDs out.** After the loss of a zone, the down OSDs may need to be
  marked ``out`` manually (see `Limitations`_).


Requirements
============

* The same topology in every zone: the same number of hosts and OSDs.
* A network round-trip latency of no more than 10 ms between the zones that
  hold OSDs.
* A replicated pool: ``ceph osd pool stretch set`` refuses erasure-coded
  pools. For those, see `Erasure-Coded Pools Spread Across Zones`_.


Configuring a Three-Zone Pool
=============================

The example below uses three zones, ``dc1``, ``dc2`` and ``dc3``, with two
copies in each zone: ``size`` 6 and ``min_size`` 3.

#. **Add each zone to the CRUSH map** and move each host into its zone:

   .. prompt:: bash #

      ceph osd crush add-bucket dc1 datacenter
      ceph osd crush move dc1 root=default
      ceph osd crush move {host} datacenter=dc1

   Repeat for ``dc2`` and ``dc3``, and for every host.

#. **Set the location of each Monitor** to the zone it runs in:

   .. prompt:: bash #

      ceph mon set_location a datacenter=dc1
      ceph mon set_location b datacenter=dc2
      ceph mon set_location c datacenter=dc3

   Repeat for every Monitor.

#. **Switch the Monitor election strategy** to ``connectivity``:

   .. prompt:: bash #

      ceph mon set election_strategy connectivity

   See :ref:`changing_monitor_elections` for details.

#. **Write a CRUSH rule** that places two copies in each zone. Get and
   decompile the CRUSH map, add the rule, then compile and inject the map:

   .. prompt:: bash #

      ceph osd getcrushmap > crush.map.bin
      crushtool -d crush.map.bin -o crush.map.txt
      # add the rule below to crush.map.txt
      crushtool -c crush.map.txt -o crush2.map.bin
      ceph osd setcrushmap -i crush2.map.bin

   ::

      rule 3az_rule {
          id 1
          type replicated
          step take default
          step choose firstn 3 type datacenter
          step chooseleaf firstn 2 type host
          step emit
      }

   Use a rule ID that is not already in use. Check the result with
   ``ceph osd crush rule dump 3az_rule``.

#. **Apply the stretch values** to an existing replicated pool:

   .. prompt:: bash #

      ceph osd pool stretch set pool01 2 3 datacenter 3az_rule 6 3

   Check them with ``ceph osd pool stretch show pool01``.

The arguments of ``ceph osd pool stretch set {pool-name}
{peering_crush_bucket_count} {peering_crush_bucket_target}
{peering_crush_bucket_barrier} {crush_rule} {size} {min_size}``:

.. list-table::
   :header-rows: 1
   :widths: 30 50 20

   * - Argument
     - Meaning
     - Example
   * - ``pool-name``
     - An existing replicated pool; the command does not create one
     - ``pool01``
   * - ``peering_crush_bucket_count``
     - The number of distinct zones that a PG's acting set must span before
       the PG can peer
     - ``2``
   * - ``peering_crush_bucket_target``
     - Used with ``size`` to limit the number of OSDs from one zone in a PG's
       acting set
     - ``3``
   * - ``peering_crush_bucket_barrier``
     - The CRUSH bucket type of the zones
     - ``datacenter``
   * - ``crush_rule``
     - The hand-written rule; its type must match the pool type
     - ``3az_rule``
   * - ``size``, ``min_size``
     - Copies in total, and copies needed for I/O, counted over all zones
     - ``6``, ``3``

``--replica {count}`` with ``--min_size {count}`` can replace ``size`` and
``min_size``. ``--yes-i-really-mean-it`` is required when the count or target
exceeds the number of zones in the CRUSH map.


Handling Zone Failures
======================

With the example values, the loss of one zone leaves each PG with four copies
in two zones. This meets ``min_size`` 3 and ``peering_crush_bucket_count`` 2,
so I/O continues.

Ceph does not adjust the pool for any further failure. To keep the pool
available after the loss of a second zone, external automation must lower
``min_size`` and ``peering_crush_bucket_count`` with
``ceph osd pool stretch set``, and restore the original values when the zones
return. The down OSDs may also need to be marked ``out`` manually (see
`Limitations`_).


.. _ec_pools_spread_across_zones:

Erasure-Coded Pools Spread Across Zones
=======================================

An erasure-coded pool with a large ``m`` can survive the loss of a zone on its
own, without stretch mode or an individual stretch pool. Spread its shards
over three or more zones, with ``m`` large enough that the shards left after
one zone is lost still reach ``min_size``. CRUSH MSR rules suit this
configuration: an MSR rule places a fixed number of shards in each zone (see
:ref:`rados-crush-msr-rules`). The pool is an ordinary erasure-coded pool, so
Ceph does not handle zone failures for it: as for an individual stretch pool,
see `Operator Responsibilities`_ and `Limitations`_.

For example, ``k=4 m=5`` over three data centers puts 3 shards in each. After
one data center is lost, 6 shards remain, above the ``min_size`` of 5
(``k+1``), so the pool keeps serving I/O and tolerates one more OSD failure.
It uses 2.25 times the raw space of the data:

.. prompt:: bash #

   ceph osd erasure-code-profile set spread3 k=4 m=5 crush-failure-domain=datacenter crush-num-failure-domains=3 crush-osds-per-failure-domain=3
   ceph osd pool create ecspread erasure spread3

``m`` must be at least the number of shards in each zone plus one. With less,
the PGs stop serving I/O when a zone is lost: ``k=6 m=3`` over three zones
keeps 6 shards, below its ``min_size`` of 7.

Compared with stretch mode:

* Reads cannot be served from one zone: a read needs ``k`` shards, more than
  one zone holds.
* The high ``m`` makes overwrites expensive, so block and file workloads with
  many overwrites perform poorly. Object workloads can still perform well.
* Every write sends shards to every zone, and recovery after a zone returns
  crosses the links between the zones.
* The rule that the profile generates picks each zone's shards by OSD, so
  several can land on one host. To separate them by host, write the rule by
  hand.
* MSR rules are new in Squid, so clients must be Squid or later.
* The Monitors must still keep a quorum when one zone fails, for example with
  Monitors in each of the three zones.


Limitations
===========

Individual stretch pools must be replicated pools:
``ceph osd pool stretch set`` refuses erasure-coded pools.

To continue after a failure, a Ceph cluster needs a quorum of Monitors: more
than half of them must be up and in contact. A three-zone configuration
therefore does not tolerate the loss of more than one zone, unless the Monitor
configuration is adjusted between the failure of the first zone and the
second. To tolerate the loss of one zone and one host, you need three Monitors
in each zone.

Individual stretch pools do not support I/O during a netsplit between two or
more zones. The cluster remains accessible for basic Ceph commands, but I/O
is unavailable until the netsplit is resolved. In stretch mode, by contrast,
the tiebreaker Monitor isolates one zone and the other continues to serve
I/O.

Ceph is designed to tolerate multiple component failures. However, if more
than 25% of the OSDs in the cluster go down, Ceph may stop marking OSDs
``out``, which prevents rebalancing and can leave PGs ``inactive``. This
behavior is controlled by :confval:`mon_osd_min_in_ratio`. The default value
is ``0.75``: at least 75% of the OSDs in the cluster must be ``in`` for any
additional OSDs to be marked ``out``. This setting prevents too many OSDs from
being marked ``out``, which could cause cascading failures and a thundering
herd of data movement, with substantial client impact and long recovery times
when the OSDs return to service.

This is particularly relevant for three-zone clusters, which are designed to
survive the loss of a full zone (a third of the hosts). With the default
:confval:`mon_osd_min_in_ratio`, the down OSDs are never marked ``out``, so
PGs stay ``peered`` and I/O stops even though the surviving zones have enough
capacity to recover. Marking the down OSDs ``out`` manually restores service
in that situation.


Reverting to an Ordinary Pool
=============================

.. prompt:: bash #

   ceph osd pool stretch unset {pool-name} {crush_rule} {size} {min_size}

The command removes the stretch values and gives the pool the CRUSH rule,
``size`` and ``min_size`` provided. The command fails while any pool in the
cluster is stretched with ``num_zones``.
