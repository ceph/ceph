========================================================
Design Document: Ceph Erasure Coded (EC) Stretch Cluster
========================================================

.. sectnum::

.. note::

   **Phased Delivery (R1, R2, …)**

   Sections are marked *R1* or *Later Release*. A section without a mark belongs to R1 unless
   its parent is marked *Later Release*. R1 delivers:

   - **Pools over zones**: ``num_zones`` and ``replica`` on every pool, replicated or erasure coded,
     with generated CRUSH rules and pool creation defaults (:ref:`Pool Configuration
     <ecsc-pool-configuration>`).
   - **Stretch mode that follows the pools**, for replicated and erasure coded pools (:ref:`Stretch
     Mode <ecsc-stretch-mode>`).
   - **Zone-local direct reads**: good path only; any failure redirects to the Primary (:ref:`Read
     Path <ecsc-read-path>`).
   - **Writes via the Primary**: all write I/O crosses the inter-zone link; no Zone Primary encoding
     (:ref:`Write Path <ecsc-write-path>`).
   - **Recovery from the Primary**: the Primary reads all data and writes all remote-zone shards; no
     inter-zone bandwidth optimization (:ref:`Recovery <ecsc-recovery>`).

   The labels R1, R2 and so on are **incremental PR delivery milestones**. They do *not* map
   one-to-one to Ceph named releases: several phases may land in one release, or one phase may span
   releases. See :ref:`Implementation Order <ecsc-implementation-order>` for the order of the work.

.. important::

   **Scope and Applicability**

   The erasure coded parts of this design are an **extension to Fast EC** (FastEC) only. There
   is no attempt to support legacy EC: an erasure coded pool over more than one zone must be a
   FastEC pool. The replicated-pool parts, the pool creation defaults and the lifetime of EC
   profiles and CRUSH rules apply to every pool and do not need FastEC.


.. _ecsc-intent-and-summary:

Intent and Summary
------------------

The goal of this feature is an erasure coded (EC) configuration for a Ceph cluster spread over
several zones (a stretch cluster), built on FastEC. A *zone* is a group of OSDs in one CRUSH bucket,
typically a data center, and the *inter-zone link* connects the zones (:ref:`Zones and the
Inter-zone Link <ecsc-zones-and-the-inter-zone-link>`).

Ceph stretch clusters rely on replicated pools to keep data available across zones. Erasure
coding stores data more efficiently, but an EC pool whose shards CRUSH spreads over the zones
of a stretch cluster causes problems. Consider a two-zone cluster with a pool of ``k=4, m=6``,
five shards in each zone. The high parity count survives the loss of a whole zone, but the
configuration has three inefficiencies:

1. **Write bandwidth waste**: every write distributes all ``k+m`` chunks across both zones.
   Parity crosses the inter-zone link, and the traffic grows with the number of shards.
2. **Reads must cross zones**: a read needs at least ``k`` chunks, which in general span both
   zones, so every read pays the inter-zone latency.
3. **Recovery is bound by the inter-zone link**: after a zone failure, the lost chunks are
   rebuilt from chunks read across the link, which puts the whole recovery on the most
   constrained network path.

This design creates EC stripes of ``k+m`` shards and replicates each stripe ``num_zones`` times,
once per zone, across a CRUSH bucket type such as ``datacenter``. Each zone holds a complete,
independent copy of every stripe, so the raw space used is ``num_zones × (k+m)/k`` times the data:
for example 3 for two zones of ``k=4, m=2``, where a replicated pool with two copies in each of two
zones uses 4. EC keeps the copy within each zone small, and the copies in the other zones give
zone-level redundancy. The target design reads and recovers within a zone and
uses the inter-zone link only to replicate writes. R1 serves reads within a zone; writes and
recovery cross the link through the Primary (:ref:`Write Path <ecsc-write-path>` and :ref:`Recovery
<ecsc-recovery>`), and later releases reduce that traffic.

The design is capable of N zones. Diagrams and examples show the common two-zone high-availability
case, and R1 supports at most two zones (:ref:`Overview <ecsc-overview>`).


.. _ecsc-terminology-and-pool-fields:

Terminology and Pool Fields
---------------------------

.. _ecsc-zones-and-the-inter-zone-link:

Zones and the Inter-zone Link
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A *zone* is a group of OSDs within one CRUSH failure domain: typically a data center, but it can
be any CRUSH bucket type (for example ``rack`` or ``room``). That bucket type is the pool's
*zone type*, also called its zone failure domain. What marks a zone boundary is that traffic
crossing it costs more: higher latency, lower bandwidth, or both.

The *inter-zone link* is the network connection between the zones, for example the dedicated
link between data centers. It is usually the most bandwidth-constrained and latency-sensitive
path in the cluster. The design minimizes the traffic on it, even at the cost of extra
zone-local reads.

.. _ecsc-logical-diagram-two-zones:

Logical Diagram (Two Zones)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The diagram shows an erasure coded pool with ``num_zones`` 2, ``k=2`` and ``m=1`` over two data
centers. ``m=1`` does not give a high level of redundancy: like a replicated pool with
``replica`` 2 over two zones, the pool survives the loss of a zone, but one more failure then
leaves it with no redundancy. It may still be what a cluster wants.

.. ditaa::

  +-------------------+             +-------------------+
  |      Client A     |             |      Client B     |
  +---------+---------+             +---------+---------+
            |                                 |
            v                                 v
  +---------------------------------------------------+
  |             Logical Replication Layer             |
  |   (num_zones 2, zone_failure_domain datacenter)   |
  +--------------------------+------------------------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
  +-----------+---------+       +-----------+-----------+
  |                     |       |                       |
  |    Data Center A    |       |     Data Center B     |
  |      (Zone 0)       |       |       (Zone 1)        |
  |                     |       |                       |
  +--+-------+-------+--+       +---+-------+-------+---+
     |       |       |              |       |       |
     v       v       v              v       v       v
  +-----+ +-----+ +-----+        +-----+ +-----+ +-----+
  | OSD | | OSD | | OSD |        | OSD | | OSD | | OSD |
  |Shard| |Shard| |Shard|        |Shard| |Shard| |Shard|
  |  0  | |  1  | |  2  |        |  3  | |  4  | |  5  |
  +-^---+ +-----+ +-----+        +-^---+ +-----+ +-----+
    |                              |
 Primary                         Zone
                                 Primary

.. _ecsc-key-definitions:

Key Definitions
~~~~~~~~~~~~~~~

**Primary**
  The primary OSD in the acting set of a placement group (PG). It coordinates writes and reads
  for the PG. (Standard Ceph terminology.)

**Shard**
  The position of a chunk in the pool's acting set, numbered ``0`` to ``num_zones × (k+m) - 1``.
  Zone ``z`` holds shards ``z × (k+m)`` to ``(z+1) × (k+m) - 1``, the zone's *block*. In the
  diagram, zone 0 holds shards 0, 1 and 2, and zone 1 holds shards 3, 4 and 5.

**Relative shard**
  A shard's position within its zone's block, ``0`` to ``k+m-1``. Shard ``s + z × (k+m)`` is
  relative shard ``s`` of zone ``z``. The shards with the same relative shard in different zones
  hold the same chunk.

**Zone numbering**
  The zones of an erasure coded pool are numbered ``0`` to ``num_zones - 1`` by the position of
  their block in the acting set. This document always numbers them so. The numbers belong to a PG's
  acting set, not to CRUSH buckets: the rule chooses the zones in a different order for different
  PGs, so one PG's zone 0 can be another PG's zone 1, and a change of the CRUSH map can change which
  bucket is a PG's zone 0. A replicated acting set is not in zone order (:ref:`Zone-Aware Replica
  Split Reads <ecsc-zone-aware-replica-split-reads>`).

**Zone-local** and **remote-zone**
  An OSD or shard is zone-local to another if both are in the same zone, and remote-zone
  otherwise. For example: "when recovering data in zone 0, a remote-zone shard may be read".

**Zone Primary** (*Later Release*)
  The first primary-capable shard (:ref:`Primary-Capable and Non-Primary Shards
  <ecsc-primary-capable-and-non-primary-shards>`) of a zone in the acting set. In later releases it
  encodes, decodes and recovers within its zone to save inter-zone bandwidth. R1 has no Zone
  Primary: the Primary does all of this.

.. _ecsc-pool-fields:

Pool Fields
~~~~~~~~~~~

Every pool, replicated or erasure coded, has these fields in ``pg_pool_t``. ``num_zones`` and
``replica`` are new; the others exist already, and this design changes the meaning of
``min_size``.

==================================  ====================================================
Field                               Meaning
==================================  ====================================================
``num_zones``                       The number of zones, each holding a full copy of the
                                    data; 1 for a pool that is not over zones
``replica``                         The copies in each zone of a replicated pool
``size``                            The shards of each PG: ``num_zones × replica`` or
                                    ``num_zones × (k+m)``; at most 128, so that every
                                    shard id fits in a ``shard_id_t``
``min_size``                        The number of OSDs that must be up in *each zone* for
                                    I/O (below)
``erasure_code_profile``            The name of the pool's EC profile (:ref:`Erasure Code Profiles
                                    <ecsc-erasure-code-profiles>`)
``ec_data_shard_count``,            ``k`` and ``m`` of an erasure coded pool
``ec_coding_shard_count``
``nonprimary_shards``               The shards of a FastEC pool that cannot become
                                    Primary (:ref:`Primary-Capable and Non-Primary Shards
                                    <ecsc-primary-capable-and-non-primary-shards>`)
``crush_rule``                      The pool's CRUSH rule (:ref:`CRUSH Placement
                                    <ecsc-crush-placement>`)
``peering_crush_bucket_count``,     The stretch values that peering uses, set only on a
                                    stretched pool; ``peering_crush_bucket_barrier`` is the
                                    zone type (:ref:`Stretched Pools and Stretch Mode
                                    <ecsc-stretched-pools-and-stretch-mode>` and
                                    :ref:`peering_crush_bucket Values
                                    <ecsc-peering-crush-bucket-values>`)
``peering_crush_bucket_target``,
``peering_crush_bucket_barrier``,
``peering_crush_mandatory_member``
==================================  ====================================================

The zone type is not a pool field of its own. A stretched pool records it in
``peering_crush_bucket_barrier``, and an EC profile can record it as ``crush-zone-failure-domain``
(:ref:`Placement Options <ecsc-placement-options>`).

**min_size per zone**

``min_size`` counts the OSDs needed in each zone, not in the whole acting set; for a pool with one
zone the two are the same. For an erasure coded pool it is from ``k`` to ``k+m``, so each zone
tolerates ``k+m-min_size`` failed OSDs. For a replicated pool it is from 1 to ``replica``, so each
zone tolerates ``replica-min_size`` failed OSDs. See :ref:`min_size in Each State
<ecsc-min-size-in-each-state>` for how peering counts the zones in each stretch mode state, and
:ref:`Configuration Options <ecsc-configuration-options>` for the defaults.

.. _ecsc-stretched-pools-and-stretch-mode:

Stretched Pools and Stretch Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A pool is *stretched* when its ``peering_crush_bucket_count`` is set. Peering then requires an
acting set that spans that many buckets of the zone type (:ref:`Stretch Set Validation
<ecsc-stretch-set-validation>`).

*Stretch mode* is a state of the whole cluster, recorded in the MonMap and the OSDMap. In stretch
mode the Monitors use the connectivity election strategy with a tiebreaker Monitor outside the
zones, and the OSDMonitor moves the stretched pools between the healthy, degraded and recovery
states when a zone fails and returns (:ref:`Stretch Mode <ecsc-stretch-mode>`). Stretch mode is
enabled and disabled in lockstep with the pools: it is enabled when any pool's ``num_zones`` is
set to 2, and disabled when the last stretched pool is deleted or its ``num_zones`` is set back
to 1. A pool gets ``num_zones`` 2 at creation (:ref:`ceph osd pool create
<ecsc-ceph-osd-pool-create>`), with ``ceph osd pool set <pool> num_zones 2`` (:ref:`num_zones
<ecsc-num-zones>`), or from ``ceph mon enable_stretch_mode`` (:ref:`Global Stretch Mode Commands
<ecsc-global-stretch-mode-commands>`).

*Global stretch mode* names the commands ``ceph mon enable_stretch_mode`` and ``ceph mon
disable_stretch_mode``. In earlier releases they switched a separate cluster mode (:ref:`Global
Stretch Mode in Earlier Releases <ecsc-global-stretch-mode-in-earlier-releases>`). In this design
they change the pool creation defaults and the existing pools (:ref:`Global Stretch Mode Commands
<ecsc-global-stretch-mode-commands>`). Whether a pool is stretched is decided only by the pool.

An *individual stretch pool* is a pool given stretch values with ``ceph osd pool stretch set``,
which works only while stretch mode is off, or by an earlier release's ``stretch set``
(:ref:`Individual Stretch Pools <ecsc-individual-stretch-pools>`). It behaves exactly as in earlier
releases, including after an upgrade: its ``num_zones`` stays 1, its ``replica`` equals its
``size``, and its ``min_size`` counts the copies in all zones together (:ref:`Encoding
<ecsc-encoding>`). Individual stretch pools are a legacy feature for a pool over two zones. A pool
over three or more zones still needs them, because ``num_zones`` is 1 or 2 (:ref:`Overview
<ecsc-overview>`); the intent is to replace them with ``num_zones`` 3 in a future release.
They cannot be combined with stretch mode, whether a pool over zones enters it
or, before the upgrade is committed, the legacy ``ceph mon enable_stretch_mode`` does (:ref:`Global
Stretch Mode in Earlier Releases <ecsc-global-stretch-mode-in-earlier-releases>`): stretch mode does
not manage individual stretch pools, cannot be enabled while one exists (:ref:`Entering Stretch
Mode <ecsc-entering-stretch-mode>`), and ``ceph mon disable_stretch_mode`` does not change them
(:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>`).


.. _ecsc-approaches-considered-but-rejected:

Approaches Considered but Rejected
----------------------------------

We evaluated and rejected two alternative designs.

.. _ecsc-multi-layered-backends:

Multi-layered Backends
~~~~~~~~~~~~~~~~~~~~~~

This approach reused the existing ``ReplicationBackend`` to manage the top-level replication,
with EC as a secondary layer.

- *Reason for rejection*: it needs complex, multi-layered peering logic in which the replication
  layer interacts with the EC layer, and the correctness of these interactions is hard to
  ensure. The front-end and back-end interfaces of the two back ends also differ significantly
  (for example, support for synchronous reads), which would need substantial refactoring.
- *Advantage of the chosen design*: the bespoke approach minimizes changes to the peering state
  machine and offers better potential for recovery in complex failure scenarios.

.. _ecsc-lrc-locally-repairable-codes-plugin:

LRC (Locally Repairable Codes) Plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach configured the existing LRC plugin to provide multi-zone EC semantics. LRC is
designed for locality-aware erasure coding, but it does not address the problems that this
design solves:

- *Reason for rejection*:

  1. **Writes still cross zones**: LRC distributes all chunks (data, local parity and global
     parity) across the full CRUSH topology. Every write sends chunks over the inter-zone link,
     with no bandwidth saving over standard EC.
  2. **Reads still cross zones**: reading the data needs ``k`` data chunks, which are spread
     across all zones. LRC has no way for a single zone to serve reads on its own.
  3. **Recovery remains Primary-centralized**: Ceph EC coordinates all recovery from the
     Primary. LRC defines local repair groups at the coding level, but the EC back end would
     still have to delegate recovery to remote zones, which is the same complexity as this
     design.

- *Advantage of the chosen design*: a complete ``k+m`` stripe in each zone enables zone-local
  reads and zone-local single-OSD recovery without a special coding scheme, and limits the
  inter-zone link to write replication. It achieves better locality than LRC with less
  infrastructure complexity.


.. _ecsc-pool-configuration:

Pool Configuration
------------------

``ceph osd pool create`` creates a pool over zones in one command. Before this design a
stretched pool needed several commands (create the pool, set its copies, stretch it), and an
erasure coded pool needed a profile first. Now:

* Every parameter of ``ceph osd pool create`` takes its default from a configuration option, so a
  pool can be created with its name alone (:ref:`Pool Creation Defaults
  <ecsc-pool-creation-defaults>`).
* An erasure coded pool that names no profile gets one of its own, built from ``--k``, ``--m`` and
  the CRUSH options ``--root``, ``--zone_failure_domain``, ``--osd_failure_domain`` and
  ``--class`` (:ref:`Erasure Code Profiles <ecsc-erasure-code-profiles>`).
* The CRUSH rule of a pool over zones is generated from the pool's parameters (:ref:`CRUSH Placement
  <ecsc-crush-placement>`).
* ``ceph osd pool set <pool> num_zones`` and ``replica`` change an existing pool in the same way
  (:ref:`Changing a Pool <ecsc-changing-a-pool>`).

The positional arguments of ``ceph osd pool create`` stay. The new parameters, like those of
``ceph osd pool set`` and ``ceph osd pool default set``, are given by name only.

.. _ecsc-ceph-osd-pool-create:

ceph osd pool create
~~~~~~~~~~~~~~~~~~~~

.. _ecsc-syntax:

Syntax
^^^^^^

.. code-block:: text

   ceph osd pool create {pool_name}
        [--pg_num <pg_num>]
        [--pgp_num <pgp_num>]
        [--pool_type <replicated|erasure>]
        [--expected_num_objects <expected_num_objects>]

        # CRUSH placement options (mutually exclusive)
        [ --rule <crush_rule_name> |
          [--root <crush_root>] [--zone_failure_domain <zone_failure_domain>]
          [--osd_failure_domain <osd_failure_domain>] [--class <device_class>] ]

        # Replicated pools
        [--replica <replica>]
        [--size <size>]

        # Erasure coded pools
        [--k <num_data_shards>]
        [--m <num_coding_shards>]
        [--erasure_code_profile <profile_name>]

        # Topology and redundancy
        [--num_zones <num_zones>]
        [--min_size <min_size>]

        # Autoscaling and general options
        [--autoscale_mode <on|off|warn>]
        [--pg_num_min <pg_num_min>]
        [--pg_num_max <pg_num_max>]
        [--bulk]
        [--target_size_bytes <target_size_bytes>]
        [--target_size_ratio <target_size_ratio>]
        [--crimson]
        [--force_pg_limit]
        [--yes_i_really_mean_it]

The legacy positional syntax is kept for backward compatibility:

.. code-block:: text

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] [replicated] \
            [crush_rule_name] [expected_num_objects]

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] erasure \
            [erasure_code_profile] [crush_rule_name] [expected_num_objects] \
            [--autoscale_mode=<on,off,warn>]

It can be combined with the named parameters that do not conflict with it. For example,
``--pg_num`` cannot be given with its positional counterpart.

.. _ecsc-parameters:

Parameters
^^^^^^^^^^

A parameter that is not given takes its default from the option in :ref:`Configuration Options
<ecsc-configuration-options>`.

**--pool_type**: ``replicated`` or ``erasure``.

**--num_zones**: the zones, each holding a full copy of the pool: 1 or 2. Two zones make the pool
stretched (:ref:`Stretched Pools and Stretch Mode <ecsc-stretched-pools-and-stretch-mode>`) and need
what :ref:`Pools over Zones <ecsc-pools-over-zones>` lists.

**--replica**: replicated pools: the copies in each zone, 1 to 10.

**--size**: replicated pools, legacy: the copies of a single-zone pool, 1 to 10. It is ignored,
rather than refused, for an erasure coded pool. Without ``--num_zones`` or ``--replica``, a size
of a pool over the default zones that is a multiple of ``osd_pool_default_num_zones`` gives the
pool ``size / num_zones`` replicas, whatever ``osd_pool_default_size`` is: Rook creates every
pool of a stretch cluster with ``--size 4``, which earlier releases accept in stretch mode, and
re-applies any ``osd_pool_default_size`` its user configured.

**--k**, **--m**: erasure coded pools: the data and coding shards in each zone, set in the pool's
own profile (:ref:`Generated Profiles <ecsc-generated-profiles>`).

**--erasure_code_profile**: erasure coded pools: an existing profile to use instead of one of the
pool's own (:ref:`How a Pool Gets its Profile <ecsc-how-a-pool-gets-its-profile>`). ``default``
names no profile.

**--min_size**: the OSDs needed in each zone for I/O (:ref:`Pool Fields <ecsc-pool-fields>`). An
erasure coded pool's default is ``k + min(1, m-1)``.

**--rule**: an existing CRUSH rule to use (:ref:`The CRUSH Rule of a Pool
<ecsc-the-rule-of-a-pool>`). ``none`` is not special here: it fails with "specified rule none
doesn't exist".

**--zone_failure_domain**, **--osd_failure_domain**, **--root**, **--class**: the CRUSH options: the
zone type, the bucket type over which copies or shards are spread within a zone, the CRUSH root, and
a device class (needed only when the cluster mixes device classes). See :ref:`Placement Options
<ecsc-placement-options>` for where their values come from.

The other parameters keep their meaning: ``--pg_num``; ``--pgp_num``, kept for backward
compatibility and not to be modified; ``--expected_num_objects``, to pre-split placement groups;
the autoscaler's ``--autoscale_mode``, ``--pg_num_min``, ``--pg_num_max``, ``--bulk``,
``--target_size_bytes`` and ``--target_size_ratio``; ``--crimson``, for Crimson OSDs
(experimental); ``--force_pg_limit``, to exceed ``mon_max_pg_per_osd``; and
``--yes_i_really_mean_it``, to allow a pool name that begins with a dot, such as ``.rgw.root``.
``--yes_i_really_mean_it`` also allows, for a pool in one zone, a profile ``stripe_unit`` that is
not a multiple of 4096 (:ref:`How a Pool Gets its Profile <ecsc-how-a-pool-gets-its-profile>`).

.. _ecsc-checks:

Checks
^^^^^^

If a pool of that name exists, ``ceph osd pool create`` stops at once, whatever else is given: it
succeeds with "pool '<pool>' already exists" when the pool has the type asked for (``--pool_type``,
else ``osd_pool_default_type``), and fails with ``EINVAL``, "pool '<pool>' cannot change to type
<type>", otherwise. This also holds before the upgrade commit, before which a pool that does not
exist is created as earlier releases create it (:ref:`Before the Commit <ecsc-before-the-commit>`). A
named profile that does not exist fails next, with ``ENOENT``, "erasure code profile '<name>' does not
exist". The following combinations are then refused with ``EINVAL``, before anything is created:

* ``--erasure_code_profile`` with ``--k`` or ``--m``: "cannot specify both erasure_code_profile
  and k/m parameters".
* Only one of ``--k`` and ``--m``: "erasure_code_profile requires both k and m".
* ``--erasure_code_profile`` with a CRUSH option: "cannot specify both erasure_code_profile and
  crush parameters (crush_root, zone_failure_domain, osd_failure_domain, crush_device_class)".
* ``--rule`` with a CRUSH option: "cannot specify both crush rule and crush parameters (...)".
* ``--k`` or ``--m`` for a replicated pool: "cannot specify k/m parameters for replicated
  pools".
* ``--replica`` for an erasure coded pool: "replica is only applicable to replicated pools".
* A CRUSH option for a replicated pool with ``num_zones`` 1: "crush parameters (...) require
  num_zones > 1 for a replicated pool".
* ``--size`` with ``--replica``: "cannot specify both 'size' and 'replica' parameters; use
  'replica' and 'num_zones' for new pools".

The values that result, given or default, are then checked:

* ``num_zones`` from 1 to 2: "num_zones must be from 1 to 2".
* Replicated pools: ``--size`` from 1 to 10 ("pool size must be between 1 and 10") and only with
  one zone ("cannot specify 'size' with num_zones > 1; use 'replica' parameter instead", or with
  the default ``num_zones`` "cannot specify 'size' with num_zones > 1 unless it is a multiple of
  num_zones 2 (osd_pool_default_num_zones); use 'replica' parameter instead");
  otherwise ``replica`` from 1 to 10 ("replica must be between 1 and 10"); ``min_size`` from 1
  to ``replica`` ("pool min_size must be between 1 and replica, which is set to <n>").
* Erasure coded pools: ``k`` at least 2 ("k=<k> must be >= 2"), ``m`` at least 1 ("m=<m> must be
  >= 1"), ``k+m`` at most 127 ("(k+m)=<k+m> must be <= 127", as shard ids are 8-bit signed
  integers), and ``num_zones × (k+m)`` at most 128 ("a pool can have at most 128 OSDs, but <n>
  zones of k+m=<k+m> need <size>"). An LRC profile skips these checks, because the LRC plugin
  checks its own ``k``, ``m`` and ``l`` and adds local parity chunks. ``m`` greater than ``k``
  is allowed; the plugin can reject values when it normalizes the profile (ISA rejects ``m``
  greater than 32). A given ``min_size`` must be from ``k`` to ``k+m`` ("pool min_size must be
  between <k> and <k+m> (k + m)"), and, LRC included, from the plugin's data chunk count to its
  chunk count ("pool min_size must be between <data chunks> and <chunks>").
* ``pg_num`` and ``pgp_num``. ``--crimson`` needs ``ceph osd set-allow-crimson``. A crimson
  pool's autoscale mode is ``off`` unless one is given, and a given mode other than ``off`` is
  refused. ``--crimson=false`` cannot opt out of ``osd_pool_default_crimson``, and a crimson
  erasure coded pool always gets FastEC.
* The CRUSH options that are used name an existing CRUSH type ("'<type>' is not a CRUSH bucket
  type"), root ("CRUSH root '<root>' does not exist") and device class ("device class '<class>'
  does not exist").
* With ``num_zones`` 2: the requirements of :ref:`Pools over Zones <ecsc-pools-over-zones>`.

An error caused by a default names the option that gave the value, for example "num_zones must
be from 1 to 2 (osd_pool_default_num_zones)". A failed stretch mode check names
``osd_pool_default_num_zones`` when ``num_zones`` is the default, else
``osd_pool_default_zone_failure_domain`` when the zone type is. An error of building or checking
the rule of a pool over zones names the options that gave the values the rule was built or
checked with, for example "zone dc1 has only 2 items of type host; replica 3 per zone requires
at least 3 hosts (osd_pool_default_size, osd_pool_default_zone_failure_domain,
osd_pool_default_osd_failure_domain, osd_pool_default_root)".

The ``ceph`` CLI refuses ``--k`` below 2, ``--m`` below 1, ``--num_zones``, ``--replica`` and
``--min_size`` below 1 with its own message; the messages above reach other callers. A
``--size`` of 0 counts as not given, as in earlier releases, where it holds the place of a
positional argument. As in earlier releases, pool creation makes no check for a single copy.

.. _ecsc-size-min-size-and-shards:

Size, min_size and Shards
^^^^^^^^^^^^^^^^^^^^^^^^^

At creation the Monitor sets:

* For a replicated pool: ``replica`` from ``--replica``, else ``--size`` for a single-zone pool,
  else ``osd_pool_default_size``; ``size`` to ``num_zones × replica``; and ``min_size`` to
  ``--min_size``, else the default for ``replica`` copies (:ref:`Configuration Options
  <ecsc-configuration-options>`).
* For an erasure coded pool, from the plugin named by its profile:

  - ``size`` to ``num_zones`` × the plugin's chunk count (``k+m`` for jerasure and ISA);
  - ``min_size`` to ``--min_size``, else ``k + min(1, m-1)``. It is not scaled by ``num_zones``:
    it applies to each zone's block of ``k+m`` shards;
  - ``ec_data_shard_count`` and ``ec_coding_shard_count`` to ``k`` and ``m``;
  - ``nonprimary_shards``, when FastEC is enabled: raw shards ``1`` to ``k-1`` (through the
    plugin's chunk mapping) in every zone, that is shard ``s + (k+m) × zone``. For ``k=4``,
    ``m=2`` and two zones this is ``{1,2,3,7,8,9}``;
  - ``allow_ec_optimizations`` (FastEC): always for a pool over zones (:ref:`Pools over Zones
    <ecsc-pools-over-zones>`), and for a single-zone pool when
    ``osd_pool_default_flag_ec_optimizations`` is set and the profile allows it;
  - ``stripe_width`` from the stripe unit: ``osd_pool_erasure_code_stripe_unit`` if set, else 16
    KiB for a plugin with FastEC support when the pool is over zones or
    ``osd_pool_default_flag_ec_optimizations`` is set, else 4 KiB, and the profile's
    ``stripe_unit`` over any of these.

.. _ecsc-pools-over-zones:

Pools over Zones
^^^^^^^^^^^^^^^^

A pool with ``num_zones`` 2 needs:

* **The upgrade committed** (:ref:`Before the Commit <ecsc-before-the-commit>`), so
  ``require_osd_release`` is always recent enough for FastEC.
* **FastEC**, for an erasure coded pool. Creation enables ``allow_ec_optimizations`` and fails
  if it cannot, with "Multi-zone erasure coded pools require FastEC support. The erasure code
  profile '<profile>' does not support FastEC: <reason> Please use a FastEC-compatible profile
  (e.g., plugin=jerasure technique=reed_sol_van, or plugin=isa)." The reason is "ec
  optimizations not currently supported for pool profile." for a plugin without FastEC support
  (ISA supports it with any technique and Jerasure only with ``reed_sol_van``; SHEC, CLAY and
  LRC do not), or "stripe_unit must be divisible by 4096 to enable ec optimizations". (A plugin
  whose FastEC support is marked experimental would get "This is experimental for use in test
  and development and is not a supported configuration."; no plugin is so marked today.) LRC
  also refuses ``num_zones`` greater than 1.
* **A cluster that can enter stretch mode, or a pool that can join it** (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`).
* **A CRUSH rule that places the pool in its zones**: in ``num_zones`` buckets of the zone type,
  with ``replica`` or ``k+m`` items in each (:ref:`The CRUSH Rule of a Pool
  <ecsc-the-rule-of-a-pool>`).

Creating the first such pool enables stretch mode (:ref:`Entering Stretch Mode
<ecsc-entering-stretch-mode>`).

.. _ecsc-erasure-code-profiles:

Erasure Code Profiles
~~~~~~~~~~~~~~~~~~~~~

Every erasure coded pool, stretched or not, has an EC profile. Three things are new: unless a
pool names a profile, it gets a profile of its own; the ``default`` profile becomes a template
that no pool uses; and a profile is deleted with the last pool that uses it. The user
documentation describes the same behavior in :ref:`erasure-code-profile-lifecycle`.

.. _ecsc-profiles-pool-references-and-the-option:

Profiles, Pool References and the Option
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **An EC profile** is a named set of ``key=value`` strings: ``plugin``, ``k``, ``m``,
  ``technique``, the ``crush-*`` placement keys and any plugin keys. It is kept in the OSDMap, not
  in the configuration. ``ceph osd erasure-code-profile set`` creates one, normalized by its plugin;
  ``ceph osd pool create`` generates one (:ref:`Generated Profiles <ecsc-generated-profiles>`); and
  the cluster is created with one named ``default`` (:ref:`The default Profile
  <ecsc-the-default-profile>`).
* **A pool's reference**, ``pg_pool_t::erasure_code_profile``, names a profile in the same
  OSDMap. At creation the Monitor derives ``size``, ``min_size``, ``stripe_width``, the shard
  counts and the CRUSH rule from the profile and stores them in the pool. Only later changes of
  the pool read the profile again: ``ceph osd pool set <pool> num_zones`` (size, ``min_size``,
  generated rule and ``nonprimary_shards``), ``min_size`` of an erasure coded pool (its range)
  and ``allow_ec_optimizations``. OSDs build a PG's EC backend from the profile in the OSDMap of
  the epoch in which they instantiate the PG. A profile that changes while pools refer to it
  therefore no longer matches what those pools stored, and their data is decoded wrongly. Many
  pools can refer to one profile.
* **The option** ``osd_pool_default_erasure_code_profile`` is a configuration string in the same
  ``key=value`` syntax. It is not a profile: it is not in the OSDMap and no pool refers to it.
  Its built-in value is ``plugin=isa technique=reed_sol_van k=2 m=2`` since Tentacle (jerasure
  before). It keeps its role in earlier releases: it is read only to create ``default`` when the
  cluster is created or ``default`` is missing, and to fill in the keys that
  ``ceph osd erasure-code-profile set`` is not given (a different ``plugin`` discards them).
  Nothing writes it, and changing it does not change what a new pool gets; changing ``default``
  does. Making the option the default of new pools would change what upgraded clusters create,
  so it is left to a later change.

.. _ecsc-the-default-profile:

The default Profile
^^^^^^^^^^^^^^^^^^^

``default`` holds what a pool gets when it does not name a profile: ``k``, ``m``, the plugin and its
keys, and the ``crush-*`` keys. It is a template. Pools get copies of it (:ref:`Generated Profiles
<ecsc-generated-profiles>`), and from the upgrade commit no pool refers to it (:ref:`At the Commit
<ecsc-at-the-commit>`), so changing it affects only pools created afterwards:

* ``ceph osd pool default set --k <k> --m <m>`` changes its ``k`` and ``m`` (:ref:`ceph osd pool
  default set and get <ecsc-ceph-osd-pool-default-set-and-get>`), and ``ceph osd pool default get``
  shows them.
* ``ceph osd erasure-code-profile set default ...`` replaces it, with no flags since no pool uses
  it: the keys not given come from ``osd_pool_default_erasure_code_profile`` (:ref:`Profile Commands
  and Changes <ecsc-profile-commands-and-changes>`), so every key, ``k`` and ``m`` included, must be
  given again.
* If ``default`` does not exist, for example after ``ceph osd erasure-code-profile rm default``,
  the next command that needs it re-creates it from ``osd_pool_default_erasure_code_profile``.
  ``ceph osd pool create`` and ``ceph osd pool default set`` store the option as written;
  ``ceph osd crush rule create-erasure`` stores it normalized, with the ``crush-*`` keys filled
  in.

.. _ecsc-how-a-pool-gets-its-profile:

How a Pool Gets its Profile
^^^^^^^^^^^^^^^^^^^^^^^^^^^

An erasure coded pool takes its profile from one of two sources:

* **Generated** (no profile named): a profile of the pool's own, ``<pool_name>-k<k>-m<m>``, built
  from ``default``, ``--k`` and ``--m`` if given, and the CRUSH options given (:ref:`Generated
  Profiles <ecsc-generated-profiles>`).
* **Named** (``--erasure_code_profile <name>`` or the positional equivalent, with a name other
  than ``default``): an existing profile, normally created with
  ``ceph osd erasure-code-profile set``, used as it is and possibly shared with other pools. The
  command does not create it. If it does not exist, the command fails with "erasure code profile
  '<name>' does not exist".

``--erasure_code_profile default`` names no profile: the pool gets a generated profile. Because
the option is given, ``--k``, ``--m`` and the CRUSH options are refused with it, as with a named
profile. Scripts that create pools with the positional ``erasure default`` keep working, but the
pool's ``erasure_code_profile`` is the generated name.

A profile from either source can be used with ``num_zones`` greater than 1: pools over zones do not
have to be created from ``--k`` and ``--m``. When the Monitor generates the pool's rule, the
profile's CRUSH keys and ``num_zones`` build it (:ref:`The CRUSH Rule of a Pool
<ecsc-the-rule-of-a-pool>`). ``num_zones`` is set per pool and is never part of a profile; a
``num_zones`` key set with ``ceph osd erasure-code-profile set`` is ignored.

A profile from either source whose ``stripe_unit`` is not a multiple of 4096, which only ``ceph osd
erasure-code-profile set --force`` accepts, is refused: such a stripe unit is not wanted for new
pools ("stripe_unit should be a multiple of 4096 bytes for best performance, not <stripe_unit>
(erasure code profile '<name>'); only ceph osd pool create --yes-i-really-mean-it accepts it").
``--yes-i-really-mean-it`` allows it for a pool in one zone. A pool over zones is a FastEC pool,
which needs a multiple of 4096, so it is refused even then ("stripe_unit should be a multiple of
4096 bytes, not <stripe_unit> (erasure code profile '<name>'): a pool over zones is a FastEC pool,
which needs one"). ``ceph osd pool default set`` refuses defaults that give such a profile.

.. _ecsc-generated-profiles:

Generated Profiles
^^^^^^^^^^^^^^^^^^

Without a named profile, the Monitor builds the contents of the pool's profile:

1. It copies every key of ``default``, re-creating ``default`` first if it does not exist.
2. It sets ``k`` and ``m`` from ``--k`` and ``--m``, if given.
3. It records the CRUSH options given on the command line:

   - ``--root`` as ``crush-root``
   - ``--zone_failure_domain`` as ``crush-zone-failure-domain``
   - ``--osd_failure_domain`` as ``crush-osd-failure-domain`` if ``default`` has that key,
     otherwise as ``crush-failure-domain``
   - ``--class`` as ``crush-device-class``

4. The plugin normalizes the contents. This fills in unset keys (``crush-root=default``,
   ``crush-failure-domain=host`` and so on, but not ``crush-zone-failure-domain``) and can
   rewrite others (ISA switches ``reed_sol_van`` to ``cauchy`` for some ``k`` and ``m``). If
   normalization fails, the command fails and nothing is proposed.

The profile is named ``<pool_name>-k<k>-m<m>``, with the plugin's data and coding chunk counts.
The Monitor looks for that name in the committed OSDMap:

* **Not found**: the Monitor creates the profile in its own OSDMap update and retries the
  command. The CRUSH rule and then the pool follow in later updates.
* **Found, same contents** (every key and value): the profile is used. This is the retry above,
  a profile left by a failed create with the same options, or the profile of a pool renamed away
  from this name, which the two pools then share.
* **Found, different contents**: the command fails with ``EEXIST``, "EC profile
  '<pool_name>-k<k>-m<m>' already exists with different contents; remove it with ceph osd
  erasure-code-profile rm <pool_name>-k<k>-m<m>, or use it with --erasure_code_profile
  <pool_name>-k<k>-m<m>".

The contents include the CRUSH options, so a profile that is used always agrees with the command
line. No command-line option sets the plugin, technique or other plugin keys of a generated
profile; they come from ``default``. A pool that needs other values names a profile.

Pool names may contain characters that ``ceph osd erasure-code-profile get`` and ``rm`` do not
accept from the ``ceph`` CLI (it allows only ``[A-Za-z0-9-_.]``). The generated profile of such
a pool can then only be removed by deleting the pool.

.. _ecsc-profile-lifetime:

Profile Lifetime
^^^^^^^^^^^^^^^^

When an erasure coded pool is deleted, with ``ceph osd pool delete`` or through librados, the
Monitor deletes its profile in the same OSDMap update as the pool, unless:

* the profile is ``default``; or
* another erasure coded pool uses it, as the pending OSDMap update leaves the pools: pools being
  created count, and pools being deleted in the same update do not.

This applies to every profile, generated or set with ``ceph osd erasure-code-profile set``. Only
erasure coded pools count as users. The command output does not mention the deletion; the Monitor
logs it at ``debug_mon`` 10. The pool's CRUSH rule is removed independently (:ref:`Rule Lifetime
<ecsc-rule-lifetime>`).

``ceph osd pool create`` waits for the pending update to commit when the profile it uses is
being removed in that update, and then finds it gone: a named profile fails as missing, and a
generated one is generated again. ``ceph osd erasure-code-profile set`` waits in the same way.

``ceph osd pool create`` commits the profile it generates in a first OSDMap update, and then runs
again to create the pool. If the creation then fails, for example on a CRUSH topology error, a
``--rule`` that does not exist or does not place the pool in its zones, stretch mode validation, the
FastEC check or the PG limit, the command removes the profile, unless a pool uses it, as it removes
the rule it generated (:ref:`Rule Lifetime <ecsc-rule-lifetime>`). ``default``, which the command
creates again when it is missing, stays.

No other change removes a profile. A profile stays when:

* no pool has ever used it, for example a profile that was set but never used;
* ``mon_fake_pool_delete`` is set and the deletion is faked: the pool is only renamed to
  ``<name>.<id>.DELETED``, and the profile and the rule both stay.

OSDs store a deleted pool's profile with its final ``pg_pool_t``, so deleting the profile in the
same epoch as the pool does not affect PG removal.

.. _ecsc-profile-commands-and-changes:

Profile Commands and Changes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``ceph osd erasure-code-profile set`` changes only in flags and the wait in :ref:`Profile Lifetime
  <ecsc-profile-lifetime>`. The keys not given come from ``osd_pool_default_erasure_code_profile``
  (a different ``plugin`` discards them). A profile has ``crush-zone-failure-domain`` only if it
  is given: normalization does not add it, as the Monitor resolves the zone type (:ref:`Placement
  Options <ecsc-placement-options>`). Like ``crush-osd-failure-domain``, that key is not checked
  here: a bad value fails when a pool over zones is created with the profile
  ("'<type>' is not a CRUSH bucket type") or when a rule is built from it ("unknown type <type>").
  Setting an existing profile to identical contents succeeds without a change. Different contents
  replace a profile that no pool uses, committed or pending, such as ``default`` or a profile set
  before its first pool, with no flags; ``--force`` is needed only for a ``stripe_unit`` that is not
  a multiple of 4096.
* ``ceph osd erasure-code-profile rm`` waits while a pool in the pending update uses the
  profile, and fails with ``EBUSY`` while a committed erasure coded pool uses it. Removing a profile
  that does not exist succeeds. ``default`` can be removed when no pool uses it, which is always
  the case after the upgrade commit; ``ceph osd pool create``, ``ceph osd pool default set`` and
  ``ceph osd crush rule create-erasure`` re-create it when needed.
* ``ceph osd erasure-code-profile get`` and ``ls`` show only committed profiles; ``get`` of a
  deleted profile fails with ``ENOENT``.
* A pool's profile cannot be replaced. ``erasure_code_profile`` can be read with ``ceph osd pool
  get`` but is not a ``ceph osd pool set`` variable. The one exception is the upgrade commit, which
  moves pools that refer to ``default`` to a copy of it (:ref:`At the Commit <ecsc-at-the-commit>`).
* ``ceph osd erasure-code-profile set <name> ... --yes-i-really-mean-it`` overwrites a profile that
  pools use; without the flag it fails with ``EPERM`` and names the pools. The pools keep the values
  derived from the old profile, while OSDs and a later ``num_zones`` change use the new one
  (:ref:`Profiles, Pool References and the Option <ecsc-profiles-pool-references-and-the-option>`),
  so this is unsafe.
* ``ceph osd pool rename`` does not rename a generated profile or the CRUSH rule named after the
  pool. A pool created later with the old name reuses the rule, and the profile if its contents
  are the same; both are then shared by the two pools.

.. _ecsc-change-of-behavior:

Change of Behavior
^^^^^^^^^^^^^^^^^^^

* A profile is deleted with the last pool that uses it, where earlier releases kept it until
  ``ceph osd erasure-code-profile rm``. Scripts that set a profile once and reuse it after
  deleting its pools must set it again before each reuse. Scripts that remove the profile after
  deleting the pool keep working, because removing a missing profile succeeds.
* A pool created without a named profile, or with ``default``, gets a profile of its own, such
  as ``mypool-k2-m2``, and ``ceph osd erasure-code-profile ls`` lists one profile for each such
  pool. After the upgrade commit no pool refers to ``default``. Scripts that expect ``default``
  as a pool's profile must change, and so must scripts that create a profile of the generated
  name in advance to give a pool other plugin keys: that fails with ``EEXIST`` unless the contents
  are the same, and such a profile is used by naming it. Changing ``default`` no longer changes
  existing pools, which was unsafe. A new pool gets the same plugin, ``k``, ``m`` and other keys
  as in earlier releases.

.. _ecsc-crush-placement:

CRUSH Placement
~~~~~~~~~~~~~~~

.. _ecsc-placement-options:

Placement Options
^^^^^^^^^^^^^^^^^

A generated CRUSH rule uses four placement values: the root, the zone type, the OSD failure
domain within a zone, and the device class. This is the one rule for where they come from when a
pool or stretch mode command generates a rule for a pool.

* **Replicated pool**: the command-line option (``--root``, ``--zone_failure_domain``,
  ``--osd_failure_domain``, ``--class``), else its default option (``osd_pool_default_root``,
  ``osd_pool_default_zone_failure_domain``, ``osd_pool_default_osd_failure_domain``,
  ``osd_pool_default_class``).
* **Erasure coded pool, root, OSD failure domain and device class**: the command-line option, else
  the pool's profile (``crush-root``; ``crush-osd-failure-domain``, or ``crush-failure-domain``
  without it; ``crush-device-class``). A generated profile records the options given at creation,
  and normalization fills in the others (:ref:`Generated Profiles <ecsc-generated-profiles>`).
  ``osd_pool_default_root``, ``osd_pool_default_osd_failure_domain`` and ``osd_pool_default_class``
  do not apply to erasure coded pools.
* **Erasure coded pool, zone type**: ``--zone_failure_domain``, else the
  ``crush-zone-failure-domain`` of the pool's profile, else
  ``osd_pool_default_zone_failure_domain``. At pool creation only a profile named with
  ``--erasure_code_profile`` counts. Normalization does not fill the key in, so a profile has it
  only when it is given. A generated profile records the zone type only when
  ``--zone_failure_domain`` is given; otherwise a ``crush-zone-failure-domain`` copied from
  ``default`` is not used at creation.

This applies to ``ceph osd pool create``, to ``ceph osd pool set <pool> num_zones 2``
(:ref:`num_zones <ecsc-num-zones>`) and to ``ceph mon enable_stretch_mode``, which gives its
``dividing_bucket`` as ``--zone_failure_domain`` (:ref:`Global Stretch Mode Commands
<ecsc-global-stretch-mode-commands>`). A single-zone erasure coded rule, at creation or when a pool
goes to one zone, is built from the profile's keys alone and has no zone type. A single-zone
replicated pool does not generate a rule (:ref:`The CRUSH Rule of a Pool
<ecsc-the-rule-of-a-pool>`), and ``ceph osd pool set <pool> replica`` copies the values of the
pool's current rule (:ref:`replica <ecsc-replica>`).

.. _ecsc-the-rule-of-a-pool:

The CRUSH Rule of a Pool
^^^^^^^^^^^^^^^^^^^^^^^^

``ceph osd pool create`` gives a pool the first of these that applies:

1. **The rule named with** ``--rule``. It must exist ("specified rule <rule> doesn't exist") and
   is used unchanged: ``num_zones`` and the profile's CRUSH keys are not applied to it.
2. **A single-zone replicated pool** takes the *default replicated rule*: the rule that
   ``osd_pool_default_crush_rule`` names, or, while that option is -1, the first replicated
   rule. If the option names a rule that no longer exists, there is no default replicated rule,
   and creating a single-zone replicated pool (librados included), ``num_zones 1`` of a
   replicated pool and ``disable_stretch_mode`` fail with "No suitable CRUSH rule exists, check
   'osd pool default crush \*' config options". The rule is not checked further, so a
   non-replicated rule set with ``ceph config set`` makes the creation fail.
3. **For any other pool, the default rule** ``osd_pool_default_crush_rule``, when no CRUSH option is
   given and the rule suits the pool: it is of the pool's type and, for a pool over zones, places
   the pool as described below. A default rule that does not suit is skipped, not refused. ``ceph
   osd pool default set`` accepts only a replicated rule (:ref:`ceph osd pool default set and get
   <ecsc-ceph-osd-pool-default-set-and-get>`), so an erasure coded pool uses the default rule only
   if ``ceph config set`` made it an erasure rule.
4. **A rule of the generated name**: ``<pool>`` for a replicated pool over zones and for an erasure
   coded pool. A single-zone erasure coded pool that gives no named profile, ``--k``, ``--m`` or
   CRUSH option (:ref:`Parameters <ecsc-parameters>`) shares the rule ``erasure-code`` instead, as
   pools with the ``default`` profile do in earlier releases. The first such pool builds it, so
   later pools keep that placement even after the ``crush-*`` keys of ``default`` change; a pool
   that needs the new keys gives ``--rule`` or the CRUSH options. ``k`` and ``m`` do not matter,
   because a single-zone rule chooses as many OSDs as the pool's size. A committed rule of the
   generated name is reused. Otherwise the Monitor builds it in its own OSDMap update and retries
   the command.

A generated rule over zones takes the root, chooses ``num_zones`` buckets of the zone type (``choose
firstn``) and in each ``replica`` (``chooseleaf firstn``) or ``k+m`` (``chooseleaf indep``) OSD
failure domains, with the values of :ref:`Placement Options <ecsc-placement-options>`; CRUSH itself
does not change. When the OSD failure domain is ``osd``, the rule chooses the OSDs with ``choose``
instead, as a single-zone rule does: ``chooseleaf`` of type ``osd`` can return OSDs that are out.
The root must have exactly ``num_zones`` buckets of the zone type, each with at
least ``replica`` or ``k+m`` OSD failure domains that contain an OSD ("number of zones <n> for type
<type> is not equal to num_failure_domains <num_zones>"). The profile keys
``crush-num-failure-domains``, ``crush-num-osd-failure-domains`` and
``crush-osds-per-failure-domain`` apply only to single-zone rules, which are built from the
profile's keys as in earlier releases.

**Placement check.** Whenever a pool over zones gets a rule, the rule (given, default, or an
existing rule of the generated name) must be of the pool's type ("crush rule <id> type does not
match pool"; "CRUSH rule type does not match pool" for ``num_zones``, and "crush rule <rule> is not
a replicated rule" for ``enable_stretch_mode``) and must place the pool in ``num_zones`` buckets of
the zone type: it chooses ``num_zones`` of them or takes them one by one, and covers exactly
``num_zones`` of them ("crush rule <rule> does not place the pool in <num_zones> buckets of type
<type>"). It must also place the pool's share, ``replica`` or ``k+m`` items, in each ("crush rule
<rule> does not place <n> items in each <type>"). This applies to pool creation, ``ceph osd pool set
<pool> num_zones 2`` (:ref:`num_zones <ecsc-num-zones>`), ``ceph osd pool set <pool> crush_rule``
(:ref:`Other Pool Settings <ecsc-other-pool-settings>`) and ``ceph mon enable_stretch_mode``
(:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>`), but not to ``ceph osd
pool stretch set`` (:ref:`Individual Stretch Pools <ecsc-individual-stretch-pools>`). A pool joining
stretch mode also needs the checks of :ref:`Entering Stretch Mode <ecsc-entering-stretch-mode>`. A
single-zone pool's rule is checked only for its type.

An erasure rule over zones can also be built with a command:

.. code-block:: text

   ceph osd crush rule create-erasure <name> [<profile>] [<num_zones>]

``create-erasure`` builds an erasure rule from the profile's keys (``default`` if no profile is
given). With ``num_zones`` greater than 1 its zone type is the profile's
``crush-zone-failure-domain`` if the profile has one, else ``osd_pool_default_zone_failure_domain``,
as for a pool created with the profile.

There is no command for a replicated rule over zones: every path that stretches a replicated pool
generates one when it is given none, so only a rule shared by several pools, or one over three or
more zones for an individual stretch pool, is written by hand in the CRUSH map, as in earlier
releases.

*Later Release*: while there are stretched pools, adding or moving a CRUSH bucket in a way that
changes the number of zones will need ``--yes-i-really-mean-it``, because it can break the
pools' placement.

.. _ecsc-rule-lifetime:

Rule Lifetime
^^^^^^^^^^^^^

A CRUSH rule is removed automatically as soon as no pool uses it, whoever created it, in the same
OSDMap update as the change that leaves it unused: ``ceph osd pool delete`` of a pool of any type,
or any change of a pool's rule (``ceph osd pool set <pool> crush_rule``, ``num_zones`` and
``replica``, ``ceph osd pool stretch set`` and ``unset``, ``ceph mon enable_stretch_mode`` and
``disable_stretch_mode``). Pools count as the pending update leaves them: a pool created with the
rule, or moved to it, in the same update keeps it, and a pool deleted or moved away does not; a pool
deleted after a change of its rule releases the rule it has in that update. Before the upgrade
commit the global stretch mode commands run as in earlier releases and remove no rule (:ref:`Before
the Commit <ecsc-before-the-commit>`).

Two kinds of rule stay:

* **The default replicated rule** (:ref:`The CRUSH Rule of a Pool <ecsc-the-rule-of-a-pool>`, item
  2), as it is when a rule would be removed. While ``osd_pool_default_crush_rule`` names a rule, for
  example after ``ceph osd pool default set --rule``, that rule is kept and ``replicated_rule`` is
  not: it goes when its last pool, such as ``.mgr``, moves to another rule. Rule ids are reused
  lowest first, so after ``--rule none`` the first replicated rule can be a rule generated for a
  pool over zones.
* **A rule created by hand that no pool has used yet**, for example with
  ``ceph osd crush rule create-replicated``. Only a change that leaves a rule unused removes it,
  so such a rule stays until ``ceph osd crush rule rm`` removes it or a pool is given it.

A command that generates a rule commits it in a first OSDMap update, and then runs again to give it
to the pool. If the command then fails, it removes the rules it generated, unless a pool uses them,
and replies with its error once the removal is committed. Pool creation through librados does the
same (``<pool>``, :ref:`Pools Created through librados <ecsc-pools-created-through-librados>`). The
commands are:

* ``ceph osd pool create`` (``<pool>`` or ``erasure-code``), for example on the PG limit, stretch
  mode validation or the FastEC check (:ref:`Profile Lifetime <ecsc-profile-lifetime>`);
* ``ceph osd pool set <pool> num_zones`` (``<pool>``, ``<pool>-stretch`` or
  ``<pool>-single-zone``), for example on the 128-OSD limit, the PG limit or the stretch mode
  checks;
* ``ceph osd pool set <pool> replica`` (``<pool>-replica-<n>``), for example on the PG limit;
* ``ceph mon enable_stretch_mode`` (``<pool>-stretch``, and without ``new_crush_rule`` ``<pool>``
  and ``<pool>-replica-2``) and ``disable_stretch_mode`` (``<pool>-single-zone``), which commit
  the rules of all the pools they change in a first update, when a later rule cannot be built or
  a pool change fails its checks.

The command removes only the rules that it created. A rule that already had the name it
generates, for example one created by hand, is used, and stays if the command fails. The leader
Monitor keeps the list of rules with the command while the command runs again; if the leader
changes in between, the client sends the command to the new leader, which does not know the
rules, and they stay.

A command that names a rule that the pending update removes, or that would reuse such a rule
because it has the name the command generates, treats the rule as gone: it waits for the update
to commit, and then a named rule fails as missing and a generated name is built again. This
covers every command that gives a pool a rule (``ceph osd pool create``,
``ceph osd pool set <pool> crush_rule``, ``num_zones`` and ``replica``,
``ceph osd pool stretch set`` and ``unset``, ``ceph mon disable_stretch_mode``),
``ceph osd pool default set --rule``, and ``ceph osd crush rule create-simple``,
``create-replicated`` and ``create-erasure``. ``ceph mon enable_stretch_mode`` looks a rule it is
given up in the pending update, so it fails at once.

In earlier releases only the rule of a deleted erasure coded pool was removed. This design
removes every unused rule, as it removes every unused profile, so that the rules that the
Monitors generate (``<pool>``, ``erasure-code``, ``<pool>-stretch``, ``<pool>-single-zone`` and
``<pool>-replica-<n>``) do not accumulate: ``pg_pool_t::crush_rule`` has 8 bits, so a cluster
can have at most 256 rules. An operator who wants to keep a rule while no pool uses it must
create it again when it is next needed.

.. _ecsc-pools-confined-to-one-zone:

Pools Confined to One Zone
^^^^^^^^^^^^^^^^^^^^^^^^^^

A stretch cluster can also host pools that do not need a copy in every zone, for example because
the application provides the redundancy. Such a pool has ``num_zones`` 1 and a rule that keeps
it in one zone, so it uses zone-local storage without the latency or bandwidth of the inter-zone
link. Unlike two independent clusters, the loss of the inter-zone link then cuts that zone's
pools off from the rest of the cluster.

An erasure coded pool is confined with ``--root``. In a cluster with the data centers ``DC1`` and
``DC2``, ``--root DC1`` generates a rule that starts with ``take DC1``, so all of the pool's data
stays in ``DC1``. ``--num_zones 1`` must be given when ``osd_pool_default_num_zones`` is 2, as it is
after ``ceph mon enable_stretch_mode``. A replicated pool with ``num_zones`` 1 refuses ``--root``
(:ref:`Checks <ecsc-checks>`); it is given a rule that takes the bucket, with ``--rule``.

.. _ecsc-pool-creation-defaults:

Pool Creation Defaults
~~~~~~~~~~~~~~~~~~~~~~

Every parameter of ``ceph osd pool create`` that is not given takes its default from a
configuration option, and ``ceph osd pool default`` sets and shows those defaults. A preferred
pool configuration can therefore be set in advance, so that creating a pool needs only its name.

.. _ecsc-configuration-options:

Configuration Options
^^^^^^^^^^^^^^^^^^^^^

=========================  =========================================  ==================
Parameter                  Option                                     Built-in value
=========================  =========================================  ==================
``--pool_type``            ``osd_pool_default_type``                  ``replicated``
``--num_zones``            ``osd_pool_default_num_zones``             1
``--replica``              ``osd_pool_default_size``                  3
``--min_size``             ``osd_pool_default_min_size``              0: computed (below)
``--rule``                 ``osd_pool_default_crush_rule``            -1: none
``--zone_failure_domain``  ``osd_pool_default_zone_failure_domain``   ``datacenter``
``--osd_failure_domain``   ``osd_pool_default_osd_failure_domain``    ``host``
``--root``                 ``osd_pool_default_root``                  ``default``
``--class``                ``osd_pool_default_class``                 empty: any class
``--k``, ``--m``           \-                                         the ``default`` EC profile
                                                                      (:ref:`The default Profile
                                                                      <ecsc-the-default-profile>`)
``--pg_num``               ``osd_pool_default_pg_num``
``--pgp_num``              ``osd_pool_default_pgp_num``
``--autoscale_mode``       ``osd_pool_default_pg_autoscale_mode``
``--bulk``                 ``osd_pool_default_flag_bulk``             ``false``
``--crimson``              ``osd_pool_default_crimson``               ``false``
=========================  =========================================  ==================

The options for ``num_zones``, the zone type, the OSD failure domain, the root and the class are
new. ``replica`` keeps the option of earlier releases, ``osd_pool_default_size``.

Notes:

* **Replicas.** ``osd_pool_default_size`` counts the replicas in each zone: it is the default
  ``replica`` of a pool, not its ``size``. It keeps its name for backward compatibility, since
  scripts, deployment tools and other projects set it, and for a single-zone pool the two are the
  same. ``ceph osd pool default set --replica``, the global stretch mode commands, and the upgrade
  commit of a cluster in global stretch mode (:ref:`At the Commit <ecsc-at-the-commit>`) write it.
  Giving the option a per-zone meaning is safe for clusters upgraded from global stretch mode:
  earlier releases do not use ``osd_pool_default_size`` while global stretch mode is enabled,
  because pools get ``mon_stretch_pool_size`` instead, and read it only when enabling (pools must
  start at it) and disabling (pools return to it). A pool reports its total ``size``: 3 replicas in
  2 zones give ``size`` 6. A two-zone replicated pool created without the defaults of global stretch
  mode (:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>`) therefore gets 3
  replicas per zone, not 2.
* **Readers of the size default.** Code that needs the default total size computes
  ``osd_pool_default_num_zones × replicas``: the ``TOO_FEW_OSDS`` health check (``PGMap``) and
  the mgr's ``default_pool_size()``, which the rook module uses. ``OSDMap::build_simple``
  creates single-zone pools and uses the replicas per zone.
* **min_size.** ``osd_pool_default_min_size`` applies to each zone. 0 gives ``replica -
  floor(replica / 2)``; a non-zero value is capped at ``replica``. An explicit value therefore
  applies in every zone: with 2 replicas per zone, ``osd_pool_default_min_size`` 2 makes both copies
  in each zone necessary for I/O (see :ref:`At the Commit <ecsc-at-the-commit>`). Erasure coded
  pools do not use it.
* **Erasure coded pools.** The default ``k`` and ``m`` are not configuration options: they are kept
  in the ``default`` EC profile in the OSDMap. ``osd_pool_default_erasure_code_profile`` keeps its
  role in earlier releases and is not one of the pool creation defaults (:ref:`Profiles, Pool
  References and the Option <ecsc-profiles-pool-references-and-the-option>`). The placement options
  apply as in :ref:`Placement Options <ecsc-placement-options>`.
* **Legacy options.** ``mon_stretch_pool_size`` (4) and ``mon_stretch_pool_min_size`` (2) are used
  only by the global stretch mode of earlier releases, before the upgrade commit (:ref:`Global
  Stretch Mode in Earlier Releases <ecsc-global-stretch-mode-in-earlier-releases>`), and
  ``mon_stretch_pool_size`` once more at the commit (:ref:`At the Commit <ecsc-at-the-commit>`).
* **No hidden overrides.** Global stretch mode no longer overrides the defaults behind the
  configuration. ``ceph mon enable_stretch_mode`` writes ``osd_pool_default_num_zones``,
  ``osd_pool_default_size`` and ``osd_pool_default_zone_failure_domain`` to the configuration
  database (:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>`), where ``ceph
  osd pool default get`` shows them.

.. _ecsc-ceph-osd-pool-default-set-and-get:

ceph osd pool default set and get
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

   ceph osd pool default set
        [--pool_type <replicated|erasure>]
        [--num_zones <num_zones>]
        [--rule <rule> |
         [--zone_failure_domain <type>] [--osd_failure_domain <type>]
         [--root <crush_root>] [--class <device_class>]]
        [--replica <replica>] [--min_size <min_size>]
        [--k <k>] [--m <m>]
        [--pg_num <pg_num>] [--pgp_num <pgp_num>]
        [--autoscale_mode <on|off|warn>] [--bulk[=false]] [--crimson[=false]]
        [--yes_i_really_mean_it]

   ceph osd pool default get

The parameters are those of ``ceph osd pool create`` except the legacy ``--size``, given as flags
with the same names, so a default is set with the flag that overrides it when a pool is created.
Two of them differ from the pool properties of ``ceph osd pool set`` and ``get``, and from the
options: ``--rule`` is the property ``crush_rule`` (``osd_pool_default_crush_rule``), and
``--autoscale_mode`` is ``pg_autoscale_mode`` (``osd_pool_default_pg_autoscale_mode``).
``--replica`` writes ``osd_pool_default_size``. ``get`` shows each parameter with its option.

``set`` never changes an existing pool, and never enables or disables stretch mode. Every value
always has a default, so there is nothing to clear: ``set`` replaces a value, also with its
built-in value. One built-in value cannot be given: the empty device class (any class); ``ceph
config rm global osd_pool_default_class`` restores it. Parameters that
are not given are left as they are. ``set`` and ``get`` are commands of the configuration
Monitor and need ``config`` capabilities, although ``--k`` and ``--m`` change the OSDMap.

* ``set`` writes each given parameter to its option (:ref:`Configuration Options
  <ecsc-configuration-options>`) in the ``global`` section of the configuration database. There is
  no ``--size``: the command is new, and ``--replica`` writes ``osd_pool_default_size``, which
  counts the replicas in each zone.
* ``--k`` and ``--m`` set ``k`` and ``m`` in the ``default`` profile in the OSDMap, re-creating it
  first if it does not exist, in the same Monitor proposal as the options, so that a ``set`` is
  applied whole or not at all. ``default`` is not normalized by this. ``set`` has no
  ``--erasure_code_profile``: a pool that names a profile takes its ``k``, ``m``, plugin keys, root,
  OSD failure domain and class from the profile, not from the defaults (:ref:`Placement Options
  <ecsc-placement-options>`), and the other keys of ``default`` are changed with ``ceph osd
  erasure-code-profile set default`` (:ref:`The default Profile <ecsc-the-default-profile>`).
* ``--rule`` names an existing replicated rule ("rule '<rule>' is not a replicated rule"), because
  single-zone replicated pools take the default rule without a suitability check (:ref:`The CRUSH
  Rule of a Pool <ecsc-the-rule-of-a-pool>`), and stores the rule's id. It is refused together with
  the CRUSH options. Pools that use the default rule do not use the CRUSH option defaults, although
  creating a replicated pool over zones still checks them. ``--rule none`` stores -1: no default
  rule.
* ``set`` checks the parameters that result as ``ceph osd pool create`` checks them (:ref:`Checks
  <ecsc-checks>`), for a pool of the default type and for each pool type whose own parameters it is
  given (``--k`` or ``--m``: erasure; ``--replica``: replicated), using the cluster
  where a check needs it: CRUSH types, roots and classes, plugins, and whether the cluster can be
  stretched. For example, ``--num_zones 2`` needs exactly two buckets of the zone type, Monitors in
  both and one tiebreaker Monitor outside them (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`). A ``min_size`` default is checked against the replicas per zone
  when replicated pools are checked. ``set`` does not make the checks that pool creation makes
  against the pools and the profile: the zone type against stretch mode's, individual stretch pools,
  and FastEC support. Pool creation still makes them.
* ``--replica 1`` also needs ``mon_allow_pool_size_one`` and
  ``--yes-i-really-mean-it``, as ``ceph osd pool set <pool> size`` does.
* ``set`` fails with ``EINVAL`` if a value it writes would not take effect on the Monitors: if the
  option is set in a ``mon`` or ``mon.<id>`` section, or a masked ``global`` section, of the
  configuration database ("<option> is set in section <section> of the configuration database,
  which overrides the global value for monitors"), or in the local configuration of the Monitor
  that handles the command ("<option> is set in the local configuration of mon.<name> (file,
  environment or command line), which overrides the configuration database"). Other Monitors'
  local configuration is not checked. The health warning ``POOL_DEFAULT_CONFIG_OVERRIDE``
  reports an override that appears later, when the option also has a ``global`` value that it
  hides or differs from.
* If any check fails, nothing is written.
* Boolean parameters are given as ``--bulk`` or ``--bulk=false``.
* ``get`` shows every default that ``ceph osd pool create`` would use now, with the option and
  the source of each value (``-f json`` for machine-readable output). ``k`` and ``m`` come from
  the ``default`` profile, with the source ``osdmap``, or from
  ``osd_pool_default_erasure_code_profile`` while ``default`` does not exist. The rule is shown
  by name, or as ``none``. Ceph's ``get`` commands differ (``ceph osd pool get`` needs a
  variable or ``all``, ``ceph config get`` takes an optional key, ``ceph fs get`` shows
  everything); this one shows everything.

``set`` leaves out these ``ceph osd pool create`` parameters:

* ``<pool_name>``, ``--expected_num_objects``, ``--pg_num_min``, ``--pg_num_max``,
  ``--target_size_bytes`` and ``--target_size_ratio`` describe one pool: its name, its expected
  content and its autoscaler bounds. They are not cluster-wide preferences.
* ``--force_pg_limit`` and ``--yes_i_really_mean_it`` override a check for one command. They are
  not settings.

The shape of the command departs from Ceph convention, which suits a command that is rarely used:

* Ceph ``set`` commands usually take one variable and value
  (``ceph osd pool set <pool> <var> <val>``, ``ceph config set <who> <name> <value>``) or
  ``key=value`` pairs (``ceph osd erasure-code-profile set``). ``ceph osd pool default set`` takes
  the flags of ``ceph osd pool create`` instead, so that one command sets several defaults and
  checks them together.
* As it takes the flags of ``ceph osd pool create``, it takes their names: ``--replica``,
  ``--rule`` and ``--autoscale_mode``, not the ``size``, ``crush_rule`` and ``pg_autoscale_mode``
  of ``ceph osd pool set``.
* The command duplicates ``ceph config set`` and ``get`` for these options. Its value is the
  self-consistency checks and the pool create syntax.

.. _ecsc-how-pool-creation-uses-the-defaults:

How Pool Creation Uses the Defaults
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ceph osd pool create`` and ``ceph osd pool default set`` build and check the parameters in the
same four steps: load the defaults; use the erasure code profile, the named one or ``default``;
apply the command line; check the values that result. Which parameters may be given together
differs only where pool creation describes one pool and ``set`` the defaults of every pool:
``--k`` alone changes the default profile, but a pool needs both ``--k`` and ``--m``.

Every default has a sensible built-in value, so a cluster needs no ``set`` before its first pool.
The options can still be changed with ``ceph config set`` and ``rm``, which skip the checks of
``set``. Pool creation therefore checks the defaults that it uses as it checks given parameters, and
names the option at fault (:ref:`Checks <ecsc-checks>`).

.. _ecsc-changing-a-pool:

Changing a Pool
~~~~~~~~~~~~~~~

.. _ecsc-num-zones:

num_zones
^^^^^^^^^

.. code-block:: text

   ceph osd pool set <pool> num_zones <n> [--replica <n>] [--crush_rule <rule>]
        [--zone_failure_domain <type>] [--root <root>]
        [--osd_failure_domain <type>] [--class <class>]
        [--yes_i_really_mean_it]

This command changes ``num_zones`` between 1 and 2 (see *Changing Per-Pool Zone and Replica Counts*
in the stretch mode user documentation). ``ceph mon enable_stretch_mode`` and
``disable_stretch_mode`` run it on the pools they change (:ref:`Global Stretch Mode Commands
<ecsc-global-stretch-mode-commands>`). None of the parameters is required: one that is not given
takes its default. They are refused with every other ``ceph osd pool set`` variable ("--<option> is
only for 'ceph osd pool set <pool> num_zones'"), which would ignore them. With the pool's current
``num_zones`` the command does nothing and succeeds, whatever else is given. Otherwise it is refused
while the pool has the ``nosizechange`` flag ("pool size change is disabled; you must unset
nosizechange flag for the pool first"). The standard recovery process then performs all the
expansion or contraction; no manual data migration is needed. The pool's ``pg_temp``,
``primary_temp`` and ``pg_upmap`` entries name an OSD for each shard of the old size. The
``pg_temp`` and ``primary_temp`` entries are dropped in the same update, and a ``pg_temp`` that an
OSD asks for with the old size is ignored. ``ceph pg repeer`` and ``ceph osd pg-temp`` size and
check theirs against the pool as the pending update leaves it. A ``pg_upmap`` entry is dropped
when the size grows, which moves some more data, and is cut to the new size when it shrinks: the
positions that stay, the first zone's for a pool going to one zone, keep their OSDs and their data
does not move.

Going to two zones:

* The pool must not be stretched already (an erasure coded pool stretched by an earlier release's
  ``stretch set`` is unstretched first with ``ceph osd pool stretch unset``, :ref:`Individual
  Stretch Pools <ecsc-individual-stretch-pools>`), and it must be able to join stretch mode, or the
  cluster to enter it (:ref:`Entering Stretch Mode <ecsc-entering-stretch-mode>`).
* A replicated pool gets ``--replica``, else ``osd_pool_default_size``, copies in each zone,
  from 1 to 10.
* An erasure coded pool must be a FastEC pool. A legacy EC pool can never be stretched, so the
  change is refused: "pool '<pool>' is a legacy EC pool, which cannot be stretched; convert it
  to FastEC first with 'ceph osd pool set <pool> allow_ec_optimizations true'". Neither this
  command nor ``enable_stretch_mode`` converts it.
* The pool gets the rule named with ``--crush_rule``, which must exist, for any pool type; otherwise
  a generated rule, ``<pool>`` for a replicated pool and ``<pool>-stretch`` for an erasure coded
  pool, whose placement values follow :ref:`Placement Options <ecsc-placement-options>`. Unlike pool
  creation, ``num_zones`` does not use ``osd_pool_default_crush_rule``. The rule, given or
  generated, must pass the placement check of :ref:`The CRUSH Rule of a Pool
  <ecsc-the-rule-of-a-pool>`.
* ``size`` becomes ``2 × replica`` or ``2 × (k+m)``, at most 128 OSDs, and ``min_size`` is reset to
  its per-zone default (:ref:`Configuration Options <ecsc-configuration-options>`).
* The ``peering_crush_bucket_*`` fields are set (:ref:`peering_crush_bucket Values
  <ecsc-peering-crush-bucket-values>`), the ``nonprimary_shards`` are recomputed, and stretch mode
  is enabled if this is the first stretched pool (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`).

Going to one zone (only a stretched pool):

* An erasure coded pool needs ``--yes-i-really-mean-it``, checked before anything is changed or
  created: "num_zones 1 takes erasure coded pool '<pool>' out of stretch mode and can leave PGs
  unavailable; see "Reducing the Number of Zones" in the Stretch Clusters documentation. Pool
  migration can move the data to a pool in one zone instead. Pass --yes-i-really-mean-it to
  proceed anyway." (EPERM). A replicated pool keeps any copy, so it needs no confirmation.
* A replicated PG keeps any of its copies, so a replicated pool behaves as before. An erasure coded
  PG keeps the first ``k+m`` positions, the shards of its first zone, whichever zone that is. The
  Monitor makes no check of where those are: a PG with fewer than the new ``min_size`` of them on
  up OSDs, or with no up OSD in its new mapping to be its primary, is unavailable until its OSDs
  are up. In degraded stretch mode, a PG whose first zone survived goes active once ``min_size``
  of its OSDs there are up, and one whose first zone is lost waits for that zone. It then
  recovers the objects written meanwhile from the shards of the zone that is no longer part of
  the pool (:ref:`Primary-Centric Recovery <ecsc-primary-centric-recovery>`).
* That recovery needs the PG active, so it needs ``min_size`` kept shards that are complete and
  whose logs reach the authoritative log, which may be a stray's. A kept shard that was being
  backfilled, or whose zone was lost for more writes than the log holds, needs backfill, and an
  inactive PG cannot backfill: with too few others the PG stays ``incomplete`` with every OSD up.
  ``num_zones 2`` makes the other zone's shards members again, so the PG goes active and
  backfills.
* A replicated pool gets ``--replica``, else ``osd_pool_default_size``, copies, from 1 to 10,
  and the default ``min_size`` for them. A single copy needs ``mon_allow_pool_size_one`` and
  ``--yes-i-really-mean-it``, as ``ceph osd pool set <pool> size 1`` does. Its rule is the one
  named with ``--crush_rule``, which must exist, else the default replicated rule.
* An erasure coded pool gets ``size`` ``k+m`` and ``min_size`` ``k + min(1, m-1)``. Its rule is
  the one named with ``--crush_rule``, which must exist, else ``<pool>-single-zone``, built from
  the profile's keys.
* ``--root``, ``--zone_failure_domain``, ``--osd_failure_domain`` and ``--class`` are accepted
  but not used.
* The ``peering_crush_*`` fields are cleared, with no mandatory member (``CRUSH_ITEM_NONE``), and
  the ``nonprimary_shards`` are recomputed. If this is the last stretched pool and stretch mode is
  enabled, stretch mode is disabled (:ref:`Leaving Stretch Mode <ecsc-leaving-stretch-mode>`).

In both directions the old rule is removed if no other pool uses it (:ref:`Rule Lifetime
<ecsc-rule-lifetime>`), and a rule that is being removed is waited for.

.. _ecsc-replica:

replica
^^^^^^^

``ceph osd pool set <pool> replica <n>`` sets the replicas in each zone of a replicated pool,
and its ``size`` to ``num_zones × n``. It has the limits of ``ceph osd pool set <pool> size``:
``n`` from 1 to 10, refused while the pool has the ``nosizechange`` flag, and a single copy
(``n`` 1 on a single-zone pool) needs ``mon_allow_pool_size_one`` and
``--yes-i-really-mean-it``. Like ``ceph osd pool set <pool> size``, it resets ``min_size`` to its
default for ``n``. An erasure coded pool refuses it ("replica is only applicable to replicated
pools").

On a two-zone pool a new ``n`` gives the pool a rule named ``<pool>-replica-<n>``, built with
the root, zone type and OSD failure domain of its current rule. A device class is kept because a
rule with a class takes the class's shadow root. The current rule must take one root, or each
zone in turn under a common parent, as the rule in the stretch mode user documentation does;
the parent is then the root and the zones' bucket type the zone type ("pool CRUSH rule must take
one root, or each zone under one root, to change replica" otherwise).

.. _ecsc-other-pool-settings:

Other Pool Settings
^^^^^^^^^^^^^^^^^^^

* ``ceph osd pool set <pool> size`` is refused for an erasure coded pool ("can not change the
  size of an erasure-coded pool") and for a pool over zones ("cannot set 'size' for stretch
  pools (num_zones=<n>); use 'replica' parameter instead"). An erasure coded pool's ``size``
  changes only with its ``num_zones``. As in earlier releases, setting ``size`` resets
  ``min_size`` to its default for the new size.
* ``ceph osd pool set <pool> min_size`` takes the OSDs needed in each zone: from 1 to
  ``replica`` for a replicated pool ("pool min_size must be between 1 and replica, which is set
  to <n>"), from ``k`` to ``k+m`` for an erasure coded pool ("pool min_size must be between
  k=<k> and k+m=<k+m> (inclusively)").
* ``ceph osd pool set <pool> crush_rule`` takes an existing rule of the pool's type. For a pool over
  zones the rule must also pass the placement check of :ref:`The CRUSH Rule of a Pool
  <ecsc-the-rule-of-a-pool>`, with the pool's zone type.
* ``ceph osd pool stretch set`` and ``unset`` are for individual stretch pools (:ref:`Individual
  Stretch Pools <ecsc-individual-stretch-pools>`).
* ``ceph osd pool set`` takes ``--replica``, ``--crush_rule`` and the CRUSH options only with
  ``num_zones`` (:ref:`num_zones <ecsc-num-zones>`).

.. _ecsc-pools-created-through-librados:

Pools Created through librados
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

librados creates pools with ``POOL_OP_CREATE``; RGW uses it to create pools on demand. Such a pool
is a replicated pool built from the defaults, as ``ceph osd pool create <pool>`` builds one:
``osd_pool_default_num_zones`` zones with ``osd_pool_default_size`` copies in each. Its rule is
the one the request names, which must place a pool over zones in its zones (:ref:`The CRUSH Rule of
a Pool <ecsc-the-rule-of-a-pool>`). Otherwise a single-zone pool gets the default replicated rule,
and a pool over zones gets the default rule if it suits the pool, else a rule generated for it,
``<pool>``, which is committed in a first OSDMap update before the request is retried. If the
creation then fails, the generated rule is removed (:ref:`Rule Lifetime <ecsc-rule-lifetime>`).

``ceph mon enable_stretch_mode`` and the upgrade commit of a cluster in global stretch mode set the
defaults to two zones (:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>` and
:ref:`At the Commit <ecsc-at-the-commit>`), so librados then stretches its pools as ``ceph osd pool
create`` does. A cluster in stretch mode only because a pool was given ``num_zones`` 2 keeps its
single-zone defaults, and librados creates single-zone pools there. Earlier releases stretched these
pools in global stretch mode, with ``mon_stretch_pool_size`` copies over the stretch rule. Before
the upgrade commit, librados creates pools as earlier releases do, in global stretch mode too
(:ref:`Before the Commit <ecsc-before-the-commit>`).

.. _ecsc-examples:

Examples
~~~~~~~~

**Example 1: Replicated pool**. A 3-way replicated pool with 128 placement groups:

.. code-block:: bash

   ceph osd pool create my_rep_pool --pool_type replicated --replica 3 --pg_num 128

**Example 2: Erasure coded pool**. ``k=4, m=2`` (a size of 6 shards) with 64 placement groups:

.. code-block:: bash

   ceph osd pool create my_ec_pool --pool_type erasure --k 4 --m 2 --pg_num 64

**Example 3: Stretched replicated pool**. A replicated pool over two data centers, with 2
replicas in each (``size`` 4):

.. code-block:: bash

   ceph osd pool create stretch_rep --pool_type replicated --replica 2 \
        --zone_failure_domain datacenter --num_zones 2

**Example 4: Stretched erasure coded pool**. An erasure coded pool over the two racks of a
cluster, with ``k=4, m=2`` in each rack, for a ``size`` of 12 shards:

.. code-block:: bash

   ceph osd pool create stretch_ec --pool_type erasure --k 4 --m 2 \
        --zone_failure_domain rack --num_zones 2

**Example 5: Erasure coded pool in one data center**. A pool confined to the data center ``DC1``
(:ref:`Pools Confined to One Zone <ecsc-pools-confined-to-one-zone>`):

.. code-block:: bash

   ceph osd pool create dc1_ec --pool_type erasure --k 4 --m 2 --root DC1 --num_zones 1

**Example 6: Bulk erasure coded pool with an autoscaling limit**:

.. code-block:: bash

   ceph osd pool create bulk_ec --pool_type erasure --k 6 --m 3 --autoscale_mode on \
        --pg_num_min 128 --bulk


.. _ecsc-stretch-mode:

Stretch Mode
------------

.. _ecsc-overview:

Overview
~~~~~~~~

Stretch mode (:ref:`Stretched Pools and Stretch Mode <ecsc-stretched-pools-and-stretch-mode>`)
follows global stretch mode of earlier releases as closely as possible and reuses the OSDMonitor
infrastructure. Replicated and erasure coded pools are handled alike. The first pool that gets
``num_zones`` 2 enables stretch mode, and the last stretched pool to go disables it; ``ceph mon
enable_stretch_mode`` and ``disable_stretch_mode`` do so by changing the pools (:ref:`Global Stretch
Mode Commands <ecsc-global-stretch-mode-commands>`). Individual stretch pools are not managed by
stretch mode (:ref:`Individual Stretch Pools <ecsc-individual-stretch-pools>`).

R1 supports two zones. ``ceph osd pool create``, ``ceph osd pool default set`` and ``ceph osd pool
set <pool> num_zones`` refuse a ``num_zones`` greater than 2, and stretch mode needs exactly two
buckets of the zone type. Only individual stretch pools can span more than two zones. Three-zone
stretch mode is described where it affects the design, and is later work (:ref:`Later Release:
Additional Enhancements <ecsc-later-release-additional-enhancements>`).

For each pool over zones:

- **CRUSH rule**: the rule given, else a generated stretch rule (:ref:`The CRUSH Rule of a Pool
  <ecsc-the-rule-of-a-pool>`).
- **min_size**: per zone (:ref:`Pool Fields <ecsc-pool-fields>`). The degraded, recovery and healthy
  transitions never change it. Entering or leaving stretch mode through ``num_zones`` resets it to
  its per-zone default (:ref:`num_zones <ecsc-num-zones>`).
- **Peering**: a stretch-aware acting set calculation (:ref:`Peering <ecsc-peering>`), with the
  ``peering_crush_bucket_*`` values that the Monitor changes in the transitions
  (:ref:`peering_crush_bucket Values <ecsc-peering-crush-bucket-values>`).

.. mermaid::

   stateDiagram-v2
       [*] --> Healthy: first pool with num_zones 2
       Healthy --> Degraded: zone failure detected
       Degraded --> Recovery: failed zone returns
       Recovery --> Healthy: all PGs clean
       Degraded --> Recovery: force_recovery_stretch_mode CLI
       Recovery --> Healthy: force_healthy_stretch_mode CLI
       Healthy --> [*]: last stretched pool deleted or set to num_zones 1, or disable_stretch_mode
       Degraded --> [*]: last stretched pool deleted or set to num_zones 1, or disable_stretch_mode
       Recovery --> [*]: last stretched pool deleted or set to num_zones 1

.. _ecsc-entering-stretch-mode:

Entering Stretch Mode
~~~~~~~~~~~~~~~~~~~~~

Stretch mode is enabled by the first pool that gets ``num_zones`` 2: at creation
(``OSDMonitor::prepare_new_pool``), with ``ceph osd pool set <pool> num_zones 2``
(``prepare_command_pool_set_num_zones``), or with ``ceph mon enable_stretch_mode``. The command
is refused unless:

- the CRUSH map has exactly two buckets of the zone type, and their weights differ by no more
  than ``mon_stretch_max_bucket_weight_delta`` times the lighter one's;
- every Monitor has a CRUSH location at the zone type, both zones have a Monitor, and exactly
  one Monitor is outside them (or ``ceph mon enable_stretch_mode`` names the tiebreaker);
- the Monitors in quorum support the connectivity election strategy;
- no other pool has stretch values, counting the pool changes pending in the same epoch. While
  stretch mode is off, such a pool is an individual stretch pool (:ref:`Individual Stretch Pools
  <ecsc-individual-stretch-pools>`), and stretch mode would take over its stretch values. The
  command fails with "pool '<pool>' is a legacy individual stretch pool; legacy individual stretch
  pools and stretch mode cannot be combined, so unstretch it first with 'ceph osd pool stretch
  unset'". An individual stretch pool has one zone, so ``stretch unset`` is what clears its stretch
  values.

Enabling stretch mode changes:

- **MonMap** (``MonmapMonitor::try_enable_stretch_mode``): the election strategy becomes
  connectivity, the Monitor outside both zones becomes ``tiebreaker_mon`` and is added to
  ``disallowed_leaders``, and ``stretch_mode_enabled`` is set.
- **OSDMap** (``OSDMonitor::try_enable_stretch_mode``): ``stretch_mode_enabled``,
  ``stretch_bucket_count = 2`` and ``stretch_mode_bucket`` (the zone type) are set, and the
  degraded and recovering flags are cleared.
- **Pool**: ``peering_crush_bucket_count`` and ``peering_crush_bucket_target`` are 2,
  ``peering_crush_bucket_barrier`` is the zone type, and there is no
  ``peering_crush_mandatory_member`` (:ref:`peering_crush_bucket Values
  <ecsc-peering-crush-bucket-values>`).

Once both maps have committed, every Monitor engages stretch mode and drops OSD sessions from
outside its own zone (``Monitor::try_engage_stretch_mode``). The leader then looks for a zone
whose Monitors are all out of quorum, as it does after an election, so a zone lost while stretch
mode was off puts the cluster in degraded stretch mode. ``ceph osd pool create`` and
``ceph osd pool set`` make every check before they change either map, so a refused command
changes neither. They wait until the MonmapMonitor is writeable, and then commit the MonMap and
OSDMap changes in one Paxos round, as ``ceph osd pool delete`` does when it leaves stretch mode
(:ref:`Leaving Stretch Mode <ecsc-leaving-stretch-mode>`).

A later pool with ``num_zones`` 2, created or set, joins the current state without changing it
(``validate_stretch_mode_new_pool``). Its zone type must be the OSDMap's ``stretch_mode_bucket``
("CRUSH rule <id> is stretched across <type> instead of <type>"), and its rule must cover the
same two zone buckets as the other stretched pools ("CRUSH rule <id> covers <n> <type> buckets,
but stretch mode requires exactly 2", "CRUSH rule <id> uses different <type> buckets than
configured for stretch mode"). In degraded or recovery stretch mode the new pool keeps its full
``size`` and ``peering_crush_bucket_target``, and gets the degraded
``peering_crush_bucket_count`` and the mandatory member that the other stretched pools have, so
that it can go active in the surviving zone. The healthy transition restores it with them. The
CRUSH map must still have exactly two buckets of the pool's zone type, but their weights are not
checked again: a pool joins even when the zones' weights have drifted apart since stretch mode
was entered.

The joining checks use stretch mode and the pools as the pending OSDMap update leaves them, so
several pools can be created in one OSDMap update: the first enters stretch mode, and the others
join it, checked against it, and do not change its zone type. A pool that joins in the update of a
pending transition gets that transition's ``peering_crush_bucket_count`` and mandatory member.

.. _ecsc-degraded-recovery-and-healthy:

Degraded, Recovery and Healthy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The transitions are those of global stretch mode in earlier releases. They change the stretched
pools' ``peering_crush_bucket_count`` and ``peering_crush_mandatory_member``
(:ref:`peering_crush_bucket Values <ecsc-peering-crush-bucket-values>`), and every transition
gives every stretched pool a forced op resend, so clients resend their operations
(``trigger_degraded_stretch_mode``, ``trigger_recovery_stretch_mode``,
``trigger_healthy_stretch_mode``). They change the ``min_size`` of a pool over zones never; that of
a stretched pool with one zone, whose ``min_size`` counts all its copies, as earlier releases do:
halved when a zone is lost and ``mon_stretch_pool_min_size`` once healthy. Before the upgrade
commit every stretched pool has one zone (:ref:`Before the Commit <ecsc-before-the-commit>`).

- **Healthy to Degraded**: the leader finds a zone, other than the tiebreaker's, with no Monitor
  in quorum and all of its OSDs down (``Monitor::maybe_go_degraded_stretch_mode``). The OSDMap
  gets ``degraded_stretch_mode = 1``. Every stretched pool gets
  ``peering_crush_bucket_count = 1`` and the surviving zone as
  ``peering_crush_mandatory_member``. The MonMap lists the failed zone's Monitors in
  ``stretch_marked_down_mons``, so they cannot lead.
- **Degraded to Recovery**: when a new OSDMap brings OSDs up, the ratio of up OSDs exceeds
  ``mon_stretch_cluster_recovery_ratio`` and every zone, and the tiebreaker's location, has a
  Monitor in quorum; or with ``ceph osd force_recovery_stretch_mode --yes-i-really-mean-it``.
  The OSDMap gets ``recovering_stretch_mode = 1``; the pools' stretch values are not changed.
  The command is refused with ``EINVAL`` outside degraded stretch mode ("the cluster is not in
  degraded stretch mode"), once recovering ("the cluster is already in recovery stretch mode"),
  and while a zone or the tiebreaker's location has no Monitor in quorum ("monitor buckets
  <buckets> are down; recovery stretch mode needs all monitor buckets up"), which is usual while
  the failed zone is down (https://github.com/ceph/ceph/pull/72421; earlier releases aborted
  the leader on an assertion there).
- **Recovery to Healthy**: once ``mon_stretch_recovery_min_wait`` has passed and no PG is
  degraded, inactive or unknown; or with
  ``ceph osd force_healthy_stretch_mode --yes-i-really-mean-it``, which is refused outside
  recovery stretch mode and does not check the PGs. Its reply "Triggering healthy stretch mode"
  does not mean the transition happened: it waits while the PG statistics cannot be read, and is
  dropped if the pending MonMap disables stretch mode. Both flags are cleared, every stretched
  pool gets ``peering_crush_bucket_count = 2`` back and no mandatory member, and
  ``stretch_marked_down_mons`` is emptied.

Recovery stretch mode never returns to Degraded: a zone that fails again is not detected while
the cluster is in recovery stretch mode.

.. _ecsc-min-size-in-each-state:

min_size in Each State
~~~~~~~~~~~~~~~~~~~~~~

``min_size`` is per zone (:ref:`Pool Fields <ecsc-pool-fields>`). Peering interprets it according to
the stretch mode state rather than the Monitor changing it on each transition
(``OSDMap::stretch_num_acting_below_min_size``, :ref:`Stretch Set Validation
<ecsc-stretch-set-validation>`):

- In healthy stretch mode every zone needs at least ``min_size`` OSDs in the acting set.
- In degraded and recovery stretch mode only the surviving zone, the mandatory member, is
  counted.

A partial failure within a zone can take that zone below ``min_size`` and stop I/O. Removing the
rest of the failed zone by hand moves the cluster to degraded stretch mode, which can bring the
pool back, because the surviving zone meets ``min_size``. Nothing promotes a partial zone
failure to a whole zone failure automatically.

**Example**: ``k=2``, ``m=1``, ``num_zones`` 2 (``size`` 6). ``min_size`` is 2 at creation and
stays 2 in every state. In healthy stretch mode I/O stops if either zone has fewer than 2 shards
active; in degraded and recovery stretch mode only the surviving zone needs 2.

.. _ecsc-leaving-stretch-mode:

Leaving Stretch Mode
~~~~~~~~~~~~~~~~~~~~

Stretch mode is disabled when the last stretched pool goes:

- ``ceph osd pool delete`` of the last pool with ``peering_crush_bucket_count`` set, counting
  the pool changes pending in the same epoch: a stretched pool being created keeps stretch mode
  enabled, and of two stretched pools deleted together the second disables it
  (``OSDMonitor::_prepare_remove_pool``, ``is_last_stretch_pool``). This applies only after the
  upgrade commit, and while both the MonMap and the OSDMap are in stretch mode. The deletion is
  retried until the MonmapMonitor is writeable, and both maps change in one Paxos round.
- ``ceph osd pool set <pool> num_zones 1`` on the last stretched pool, counting the pool changes
  pending in the same epoch, while stretch mode is enabled or being enabled (:ref:`num_zones
  <ecsc-num-zones>`); or ``ceph mon disable_stretch_mode``, which sets every stretched pool to one
  zone (:ref:`Global Stretch Mode Commands <ecsc-global-stretch-mode-commands>`).

Disabling stretch mode changes:

- **MonMap** (``MonmapMonitor::clear_stretch_mode_state``): ``stretch_mode_enabled`` is cleared,
  and ``tiebreaker_mon``, ``disallowed_leaders`` and ``stretch_marked_down_mons`` are emptied.
  Every disallowed leader is removed, including any added with
  ``ceph mon add disallowed_leader``. The election strategy stays connectivity.
- **OSDMap**: ``stretch_mode_enabled``, ``stretch_bucket_count``, ``stretch_mode_bucket``,
  ``degraded_stretch_mode`` and ``recovering_stretch_mode`` are cleared.

The Monitors then disengage stretch mode and stop checking which zone an OSD session comes from.
No other pool changes, as none is stretched.

Deleting the last stretched pool or setting it to ``num_zones`` 1 does not check the stretch
mode state, so stretch mode is left in the same way from Healthy, Degraded or Recovery. This is
intended: no stretched pool is left to protect. In Degraded or Recovery, the degraded and
recovering flags and ``stretch_marked_down_mons`` are cleared without a transition to Healthy,
while the failed zone may still be down. ``ceph mon disable_stretch_mode`` is refused in
Recovery. An erasure coded pool set to one zone while a zone is down can leave PGs unavailable,
which is why that needs ``--yes-i-really-mean-it`` (:ref:`num_zones <ecsc-num-zones>`).

.. _ecsc-peering-crush-bucket-values:

peering_crush_bucket Values
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing ``pg_pool_t`` fields are used as in global stretch mode of earlier releases:

====================================  =================================================
Field                                 Value for a pool over zones
====================================  =================================================
``peering_crush_bucket_count``        The minimum number of zone buckets in the acting
                                      set: ``num_zones`` in healthy stretch mode, 1 in
                                      degraded and recovery
``peering_crush_bucket_target``       The target number of zone buckets, for the
                                      ``bucket_max`` calculation: ``num_zones``, not
                                      changed by the transitions
``peering_crush_bucket_barrier``      The zone type, for example ``datacenter``
``peering_crush_mandatory_member``    The zone bucket that must be represented: the
                                      surviving zone in degraded and recovery, none
                                      (``CRUSH_ITEM_NONE``) otherwise
====================================  =================================================

Example values for ``num_zones`` 2, ``k=2``, ``m=1`` (``size`` 6):

==================  ============  =============  ==========  ================
Stretch state       bucket_count  bucket_target  bucket_max  mandatory_member
==================  ============  =============  ==========  ================
Healthy             2             2              3           NONE
Degraded            1             2              3           surviving zone
Recovery            1             2              3           surviving zone
Healthy (restored)  2             2              3           NONE
==================  ============  =============  ==========  ================

``bucket_max = ceil(size / bucket_target)``, here ``6 / 2 = 3 = k+m``. The transitions do not
change ``bucket_target``, so this holds in every state and prevents more than one copy of each
shard in a zone, also in the surviving zone while the other is down.

.. _ecsc-global-stretch-mode-commands:

Global Stretch Mode Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ceph mon enable_stretch_mode [<tiebreaker_mon>] [<new_crush_rule>] <dividing_bucket>`` and ``ceph
mon disable_stretch_mode [<crush_rule>] [--yes-i-really-mean-it]`` keep their syntax, except that
``new_crush_rule``, required before the upgrade commit as in earlier releases, is optional after
it. Instead of
switching a separate mode (:ref:`Global Stretch Mode in Earlier Releases
<ecsc-global-stretch-mode-in-earlier-releases>`), they set the pool creation defaults below and
change pools through ``num_zones`` (:ref:`num_zones <ecsc-num-zones>`), which enables or disables
stretch mode (:ref:`Entering Stretch Mode <ecsc-entering-stretch-mode>` and :ref:`Leaving Stretch
Mode <ecsc-leaving-stretch-mode>`). They work with replicated and erasure coded pools, metadata
pools such as ``.mgr`` included, and keep no state: afterwards each pool can be changed on its own,
and local and stretched pools can be mixed. A cluster that uses global stretch mode can therefore
upgrade and then create local pools, stretched erasure coded pools or both. For a mixture, setting
``num_zones`` 2 on the pools that need it and giving every parameter at pool creation achieves the
same without changing the defaults.

==========================================  =========================  ==========================
Default                                     ``enable_stretch_mode``    ``disable_stretch_mode``
==========================================  =========================  ==========================
``osd_pool_default_num_zones``              2                          1
``osd_pool_default_size``                   2                          3
``osd_pool_default_zone_failure_domain``    ``dividing_bucket``        unchanged
==========================================  =========================  ==========================

**enable_stretch_mode** changes the pools that are not stretched as stretch mode needs:

* A pool with ``num_zones`` 1 gets ``num_zones`` 2 (:ref:`num_zones <ecsc-num-zones>`). A replicated
  pool gets 2 replicas per zone and ``new_crush_rule``, or without it a generated ``<pool>`` rule
  with ``dividing_bucket`` as its zone type, as ``num_zones`` generates one. An erasure coded pool
  gets a generated ``<pool>-stretch`` rule with ``dividing_bucket`` as its zone type, so its
  ``size`` doubles and its ``min_size`` is reset.
* A replicated pool with ``num_zones`` 2 and other than 2 replicas per zone gets 2 replicas,
  ``new_crush_rule``, or without it a generated ``<pool>-replica-2`` rule over ``dividing_bucket``
  with the root, OSD failure domain and class of the pool defaults, and the default ``min_size``
  for 2. As with ``num_zones``, it is refused while the pool has ``nosizechange``, and the rule must
  pass the placement check of :ref:`The CRUSH Rule of a Pool <ecsc-the-rule-of-a-pool>` for it.

It is refused unless:

* ``dividing_bucket`` is a CRUSH type and ``new_crush_rule``, if given, a replicated rule with a
  ``take`` step that covers exactly two buckets of that type, as a pool joining stretch mode must
  (:ref:`Entering Stretch Mode <ecsc-entering-stretch-mode>`);
* no legacy EC pool exists (its owner converts it first with ``ceph osd pool set <pool>
  allow_ec_optimizations true``);
* no individual stretch pool exists (:ref:`Entering Stretch Mode <ecsc-entering-stretch-mode>`);
* the defaults that result pass the checks of ``ceph osd pool default set`` (:ref:`ceph osd pool
  default set and get <ecsc-ceph-osd-pool-default-set-and-get>`) and nothing overrides them;
* the Monitors and the OSDMap can enter stretch mode (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`); and
* every pool change passes the checks of :ref:`num_zones <ecsc-num-zones>`, ``nosizechange``
  included.

A refusal changes no pool, default or Monitor state, and removes the rules the command generated
(:ref:`Rule Lifetime <ecsc-rule-lifetime>`). Nothing has to be prepared with ``ceph osd pool
default`` first.

On a cluster with no pools the command enters stretch mode in the MonMap alone: the tiebreaker,
given or chosen, its exclusion from leadership and the connectivity election strategy, together
with the stretched defaults. The OSDMap's cluster stretch flags follow with the first pool over
two zones, normally ``.mgr``, which the Manager creates from those defaults; until then the
Monitors do not engage stretch mode, as ``Monitor::try_engage_stretch_mode()`` waits for both
maps, and with no pool there is nothing for degraded stretch mode to protect. Earlier releases set
both maps at once, but the first pool then had to name the stretch rule, which they record nowhere.
While only the MonMap is in stretch mode:

* the first pool over two zones, from ``ceph osd pool create`` or ``ceph osd pool set <pool>
  num_zones 2``, keeps the MonMap as it is and must suit its tiebreaker: its zone type is checked
  against the Monitors' locations with that tiebreaker, so a pool over another bucket type than the
  command named is refused ("Failed to validate monitor stretch mode: ...");
* ``ceph osd pool stretch set`` and ``unset`` are refused, as in stretch mode, so no individual
  stretch pool can keep the first pool out;
* ``disable_stretch_mode`` and ``enable_stretch_mode`` work as with pools.

**disable_stretch_mode** changes the pools with ``num_zones`` greater than 1 to one zone
(:ref:`num_zones <ecsc-num-zones>`). These are all the pools that stretch mode stretched.

* A replicated pool gets 3 replicas and ``crush_rule`` if given, else the default replicated rule.
* An erasure coded pool gets a generated ``<pool>-single-zone`` rule, ``size`` ``k+m`` and the
  default ``min_size``.
* Pools with one zone are not changed, individual stretch pools and erasure coded pools with
  stretch values from an earlier release included: a replicated pool created with ``num_zones`` 1
  while the defaults were stretched keeps its 2 replicas, and is raised with ``ceph osd pool set
  <pool> replica 3``.
* The command writes ``osd_pool_default_size`` 3, whatever it was before
  ``enable_stretch_mode``.
* The command needs ``--yes-i-really-mean-it``, which it passes to each pool (:ref:`num_zones
  <ecsc-num-zones>`); a pool's refusal names the pool.
* It is refused in recovery stretch mode ("stretch mode is currently recovering and cannot be
  disabled"; run ``ceph osd force_healthy_stretch_mode`` first).
* ``crush_rule``, if given, must be a replicated rule that no stretched replicated pool uses.

**Both commands**: erasure coded pools get rules of their own, which are committed in a first OSDMap
update before the command is retried. Then the pool, MonMap and configuration changes commit
together in one Paxos round, all pools or none. A command that fails removes the rules it generated
(:ref:`Rule Lifetime <ecsc-rule-lifetime>`).

**Arguments**

* ``dividing_bucket`` is the CRUSH bucket type that splits the cluster into its two zones, for
  example ``datacenter``. It becomes the default zone type.
* ``tiebreaker_mon`` is a legacy argument. Without it, the Monitors pick the one Monitor outside
  both zones, and enabling is refused if there is none or more than one. It is still accepted,
  and used as in earlier releases, so that existing callers keep working and a cluster with more
  than one Monitor outside the zones can name its tiebreaker. Once stretch mode is enabled, a
  repeated ``enable_stretch_mode`` ignores it; ``ceph mon set_new_tiebreaker`` changes the
  tiebreaker. Being the first positional argument, it can only be left out when the other two
  are given by name:
  ``ceph mon enable_stretch_mode --new_crush_rule <rule> --dividing_bucket <type>``, or
  ``ceph mon enable_stretch_mode --dividing_bucket <type>``.
  ``enable_stretch_mode <rule> <type>`` is refused by the command line parser ("Invalid command:
  missing required parameter dividing_bucket(<string>)").
* ``new_crush_rule`` and ``crush_rule``, when given, go to the replicated pools that the commands
  change, as ``--crush_rule`` would (:ref:`num_zones <ecsc-num-zones>`); otherwise each pool gets a
  rule generated for it. Erasure coded pools get their generated rules, which ``ceph osd pool set <pool> crush_rule`` can change afterwards. A rule that
  the changes leave unused, for example ``new_crush_rule`` after ``disable_stretch_mode``, is
  removed (:ref:`Rule Lifetime <ecsc-rule-lifetime>`).

**Running the commands again**

Either command changes the MonMap when it enables or disables stretch mode, which makes the
Monitors bootstrap and run any command still waiting for its reply again. A second run must
therefore succeed, and its reply must not contradict the first:

* ``enable_stretch_mode`` sets the defaults again and stretches the pools that are not stretched
  yet. It replies "stretch mode is enabled", with "; <n> pool(s) changed" when it changed any.
* ``disable_stretch_mode`` with no pool over zones and the defaults already one zone with 3
  replicas succeeds with "stretch mode is disabled" and changes nothing. If only the defaults
  remain stretched, it resets them and replies "stretch mode was already disabled; the pool
  creation defaults are now one zone with 3 replicas". Otherwise it replies "stretch mode is
  disabled; <n> pool(s) changed".

Compared with earlier releases (:ref:`Global Stretch Mode in Earlier Releases
<ecsc-global-stretch-mode-in-earlier-releases>`), both commands can be run again (earlier releases
reply "stretch mode is already engaged" or "stretch mode is already disabled").
``enable_stretch_mode`` accepts replicated pools of any size, and every replicated pool ends with 2
replicas per zone: a local pool of size 4 or 1 becomes 2 × 2, and a two-zone pool with 3 replicas
per zone drops to 2. It also stretches erasure coded pools; this is intended, although each erasure
coded pool's raw usage doubles and every changed pool backfills.

While the defaults are stretched, a local pool created with ``--num_zones 1`` also gets 2 replicas,
unless ``--replica`` is given. This is intended: one replica default serves both kinds of pool,
without an option of its own for stretched pools.

.. _ecsc-global-stretch-mode-in-earlier-releases:

Global Stretch Mode in Earlier Releases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In earlier releases ``ceph mon enable_stretch_mode`` switched a separate cluster mode:

* Only replicated pools: enabling refuses erasure coded pools, and pools without the default
  ``size`` and ``min_size`` unless they already use the rule passed.
* Enabling gives every pool ``mon_stretch_pool_size`` (4) copies, a ``min_size`` of
  ``mon_stretch_pool_min_size`` (2, counted over both zones), the rule passed and the stretch
  values. While stretch mode is enabled, a new pool gets the same size and ``min_size`` (another
  size is refused) and the rule that most stretched pools use, so once they are all deleted pool
  creation fails with "No suitable CRUSH rule exists"; creating an erasure coded pool is
  refused.
* Degraded stretch mode halves each stretched pool's ``min_size``, and the healthy transition
  restores it from ``mon_stretch_pool_min_size``.
* ``ceph mon disable_stretch_mode`` resets every pool, to ``osd_pool_default_size`` and its
  default ``min_size``, and to the given rule, else the default replicated rule.
* ``stretch_mode_enabled`` in the MonMap and the OSDMap records the mode.

Before the upgrade commit, upgraded Monitors behave in this way too, pool creation and the halving of
``min_size`` included. See :ref:`Before the Commit <ecsc-before-the-commit>` for every difference
before the commit.

.. _ecsc-individual-stretch-pools:

Individual Stretch Pools
~~~~~~~~~~~~~~~~~~~~~~~~

``ceph osd pool stretch set`` and ``unset`` (``prepare_command_pool_stretch_set``,
``prepare_command_pool_stretch_unset``) make an individual stretch pool. They take the pool's
stretch values (``peering_crush_bucket_*``, ``crush_rule``, ``size`` and ``min_size``) from the
command line. An individual stretch pool keeps ``num_zones`` 1 and ``replica`` equal to ``size``,
and its ``min_size`` counts all its copies, exactly as in earlier releases: it is not a pool over
zones in the sense of this design, and stretch mode does not manage it.

An erasure coded pool is stretched and unstretched only with
``ceph osd pool set <pool> num_zones <n>``, which works out all of these values, and the shards
that cannot be Primary, from the zone count. Two ways to stretch an erasure coded pool would
have to agree on every one of them, so:

- ``stretch set`` refuses every erasure coded pool: "osd pool stretch set is not supported for
  EC pools; use 'ceph osd pool set <pool> num_zones <N>' instead".
- ``stretch unset`` would refuse an erasure coded pool with ``num_zones`` greater than 1 in the same
  way, but such a pool exists only in stretch mode, where both commands are refused first (below).
  An erasure coded pool with ``num_zones`` 1 has stretch values only if an earlier release's
  ``stretch set`` gave them (:ref:`Pools from Earlier Releases <ecsc-pools-from-earlier-releases>`).
  ``stretch unset`` clears them only with ``size`` ``k+m`` and a ``min_size`` from ``k`` to ``k+m``,
  which also repairs a ``size`` that the earlier ``stretch set`` changed.

For every pool type:

- Both commands are refused while stretch mode is enabled, or the changes pending in the same epoch
  enable it: stretch mode sets the stretch values of its pools itself. Conversely, stretch mode is
  not enabled while an individual stretch pool exists (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`).
- ``stretch set``'s ``min_size`` keeps its meaning in earlier releases, a total over all zones,
  from 1 to ``size``; 0 gives ``peering_crush_bucket_count``. The pool stores it unchanged, and
  peering counts it over the whole acting set, as for any stretched pool with one zone
  (:ref:`Stretch Set Validation <ecsc-stretch-set-validation>`).
- ``stretch set`` takes ``--replica`` (``size`` is the number of zones the rule spans ×
  ``replica``) or ``size``, but not both ("cannot specify both 'size' and 'replica' parameters;
  ..."); ``stretch unset`` takes either as the pool's one-zone ``size``. ``size`` stays the
  earlier releases' positional argument.
- ``peering_crush_bucket_count`` and ``peering_crush_bucket_target`` cannot exceed the number of
  zones the rule spans ("peering_crush_bucket_count (<n>) cannot exceed the number of <type> zones
  (<m>) spanned by crush rule <rule>"), and so cannot exceed the cluster's buckets of that type
  either. ``--yes-i-really-mean-it``, which allowed more than the cluster's buckets in earlier
  releases, is still accepted but no longer overrides this. ``stretch set`` does not set a mandatory
  member, and does not make the placement check of :ref:`The CRUSH Rule of a Pool
  <ecsc-the-rule-of-a-pool>`.
- ``stretch unset`` sets ``num_zones`` to 1.

.. _ecsc-network-partition-handling:

Network Partition Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing Ceph stretch cluster mechanism handles network partitions as follows:

- **Monitor-based zone election**: the Monitors are distributed between the zones, plus a
  tiebreaker location. When the inter-zone link is severed, the Monitors elect which zone
  continues to operate.
- **OSD failure reports**: OSDs heartbeat each other, and the surviving zone's OSDs report the
  failures. In stretch mode an OSD talks only to the Monitors of its own zone, so OSDs in the
  losing zone get no new maps and stop processing I/O when their read leases expire.
- **Lease-based read gating**: a lease decides whether OSDs may process reads. The surviving
  zone waits for the lease to expire before new writes are processed, so that the losing zone
  serves no stale reads during the transition.

More complex failures may also need to be treated as zone failures:

- **Asymmetric network splits**: OSDs in a remote zone may still reach a zone-local Monitor
  although the Monitors have decided on a zone failover.
- **Partial zone failures**: the loss of a rack or a subset of OSDs within a zone may warrant
  treating the zone as failed, for example if the remaining shards fall below the per-zone
  ``min_size`` (:ref:`min_size in Each State <ecsc-min-size-in-each-state>`).

.. _ecsc-online-osds-in-offline-zones:

Online OSDs in Offline Zones — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a zone is marked offline but some of its OSDs are still reachable, they must be removed from
the up set presented to the Objecter and the peering state machine. The mechanism ties to the
per-zone ``min_size`` (:ref:`min_size in Each State <ecsc-min-size-in-each-state>`):

- **Removal**: if fewer than ``min_size`` OSDs of a zone are in the up set, the zone's remaining
  OSDs are removed as well. This forces a zone failover and prevents partial-zone I/O. Removal from
  the up set alone is not enough: ``calc_ec_acting_stretch`` also serves a zone block from
  ``acting`` and strays that hold its shards (:ref:`Acting Set Calculation
  <ecsc-acting-set-calculation>`), so it must also skip holders in an offline zone.
- **Reintegration**: when enough OSDs return to meet the per-zone threshold, they are added back
  to the up set. Standard peering resynchronizes the data; reintegration needs no new recovery
  code.


.. _ecsc-read-path:

Read Path
---------

.. _ecsc-read-from-the-primary:

Read from the Primary — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard read path has the client contact the Primary OSD. The Primary reads from OSDs in
its own zone, and reconstructs the stripe from that zone's other shards when a zone-local shard
is missing, down, being backfilled or returns a medium error (for example an unreadable sector).
Only when its zone has too few readable shards to decode does it add remote-zone copies of the
relative shards that the zone lacks; a read that fails with a medium error is retried in the
same way. A zone without enough shards to reconstruct locally is likely to be taken offline by
stretch mode, but this fallback keeps the data available in transient states.

The reads of a read-modify-write (:ref:`Primary Encodes All Shards
<ecsc-primary-encodes-all-shards>`) and of recovery (:ref:`Primary-Centric Recovery
<ecsc-primary-centric-recovery>`) choose shards in the same way.

.. _ecsc-ec-direct-reads-over-zones:

EC Direct Reads over Zones — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

EC direct reads (:doc:`direct_reads`) let a client read data shards directly from the OSDs that hold
them, bypassing the Primary on the good path. For a pool over zones, the read mode flag of the
operation decides which zone each data shard is read from
(:ref:`ecsc-localized-reads-ceph-osd-flag-localize-reads` and
:ref:`ecsc-balanced-reads-ceph-osd-flag-balance-reads`), and any failure falls back to the Primary
(:ref:`Common Behavior <ecsc-common-behavior>`).

A client gets these reads only when all of these hold: it sets the read flag, per operation or
with ``rados_replica_read_policy`` ``localize`` or ``balance`` (with both flags, split and
direct reads use ``LOCALIZE_READS``); for ``LOCALIZE_READS`` it has a ``crush_location``; the
pool has the ``split_reads`` flag, which the Monitor sets on replicated and FastEC pools that
are not Crimson pools, from ``require_osd_release`` Umbrella; and the pool has no cache or dedup
tier. Snapshot reads, zero-length reads and unsupported operation types are not split. For
example, a client in the Primary's zone that sets ``CEPH_OSD_FLAG_LOCALIZE_READS`` reads from
that zone only:

.. mermaid::

   sequenceDiagram
       title Direct Read — Primary Zone

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard

       C->>ZS: Direct Read
       activate ZS
       ZS->>C: Complete
       deactivate ZS

       C->>ZS: Direct Read
       activate ZS
       ZS->>C: -EAGAIN
       deactivate ZS
       C->>P: Full Read
       activate P
       P->>ZS: Read
       activate ZS
       ZS->>P: Read Done
       deactivate ZS
       P->>C: Done
       deactivate P

.. note::

   The acting set of an erasure coded pool is positional, and the stretch CRUSH rule keeps it in
   zone order::

       acting[0 .. zone_size-1]             → zone 0
       acting[zone_size .. 2*zone_size-1]   → zone 1
       …

   A missing OSD is ``CRUSH_ITEM_NONE``, which keeps the positions, so the shard is
   ``rel_shard + zone_index × zone_size``. The arithmetic is the same for a single-zone pool
   (``zone_size == size``, ``zone_index == 0``), so no separate code path is needed. A read with
   only ``LOCALIZE_READS`` set is split only when ``num_zones`` is greater than 1; on a
   single-zone erasure coded pool it goes to the Primary.

.. _ecsc-localized-reads-ceph-osd-flag-localize-reads:

Localized Reads (``CEPH_OSD_FLAG_LOCALIZE_READS``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All data shards of an operation are read from the **same zone**, the one nearest to the client.

- **Zone selection**: the client scores each zone by its first OSD in the acting set that is not
  ``CRUSH_ITEM_NONE``, by the CRUSH distance to the client
  (``CrushWrapper::get_common_ancestor_distance()``). The nearest zone wins, and ties go to the
  lower-numbered zone. If no zone can be scored (``crush_location`` unset, no representative, no
  common ancestor, or an acting set shorter than the pool's size), zone 0 is used, which is also
  right for a single-zone pool.
- **Shard selection**: each data chunk is read from the chosen zone. If any of those OSDs is
  unavailable (absent from the acting set or not ``exists()``), the split read is aborted and the
  operation goes to the Primary, not to another zone. Later releases redirect to the Zone Primary
  (:ref:`Read from the Zone Primary <ecsc-read-from-the-zone-primary>`).

.. _ecsc-balanced-reads-ceph-osd-flag-balance-reads:

Balanced Reads (``CEPH_OSD_FLAG_BALANCE_READS``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each data shard ``rel_shard`` (``0`` to ``k-1``) of an operation is read from a **randomly
chosen zone**: one chosen uniformly from the zones whose OSD for that shard is available (inside
the acting set and ``exists()``), independently for each shard. The shard is
``rel_shard + zone × zone_size``. If no zone has an available OSD for a shard, the split read is
aborted and the operation goes to the Primary. This spreads the read load across all zones
without knowing where the client is, so the client needs no ``crush_location``.

.. _ecsc-common-behavior:

Common Behavior
^^^^^^^^^^^^^^^^

- **Direct read execution**: the client sends each shard read directly to the OSD. As the direct
  reads design describes, a read may overtake an in-flight write. An OSD that has an uncommitted
  write for the object (one that could still be rolled back), or has the object in its missing set,
  rejects the read with ``-EAGAIN``; an object that does not exist gives ``-ENOENT``. A data access
  error, such as a media error, also sends the read to the Primary, which reconstructs the data
  (:ref:`Read from the Primary <ecsc-read-from-the-primary>`).
- **Strict ordering**: an operation that needs strict ordering (``rwordered``) is never sent as
  a direct read: the client clears its balance and localize flags and sends it to the Primary.
  An OSD that receives such an operation as a direct read refuses it with ``-EINVAL``.
- **Object version**: a read within one chunk is not split. It goes whole, as a direct read, to
  the chosen zone's OSD, even when it needs the object version (``objver``) or carries other
  operations (for example ``stat``, ``getxattr`` or a class call). A split read that covers more
  than one chunk sends a sub-read to the Primary, which may be in the other zone, when it needs
  the version or carries other operations, and always until ``require_osd_release`` is past
  Umbrella: until then the shards' versions are checked against the Primary's. After that the
  shards' own versions are compared (direct reads design).
- **Failure handling (R1)**: an operation that fails as a direct read, for example with ``-EAGAIN``,
  is sent again to the Primary. Later releases send it to the Zone Primary (:ref:`Read from the Zone
  Primary <ecsc-read-from-the-zone-primary>`).

.. _ecsc-read-from-the-zone-primary:

Read from the Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1 the Primary handles every failure. In later releases an operation that cannot be processed
as a direct read is sent to the Zone Primary of the client's zone instead, which reconstructs
the data from the zone-local shards.

A conflict with an uncommitted write is still handled by the Primary, as the Zone Primary must
also reject an operation if an uncommitted write exists for the object.

- **Zone-local recovery**: a read sent to a Zone Primary is served by recovering data from OSDs
  in the Zone Primary's zone only.
- **Zone degradation**: a zone without enough redundancy to reconstruct data locally should be
  taken offline to clients, rather than serve reads that need the inter-zone link.
- **``-EAGAIN``**: the Zone Primary returns ``-EAGAIN`` only in short-lived transient
  conditions.

.. _ecsc-safe-shard-identification-and-synchronous-recovery:

Safe Shard Identification and Synchronous Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Zone Primary does not know by itself which shards are consistent and safe to read. Peering
is extended to tell it.

- **Synchronous Recovery Set**: the peering Activate message is extended with a "Synchronous
  Recovery Set", which lists the shards that are being recovered and may not be consistent yet.

  .. note::

     The peering state machine has a transition from Active+Clean to Recovery, so recovery can
     start without a new peering interval. This needs further investigation, to keep the
     Synchronous Recovery Set correct across such transitions.

- **Consistency guarantee**: within an epoch the set can only shrink, as shards complete
  recovery. It is treated as binary: either the full recovery set is active, or it is empty. The
  Zone Primary is therefore *conservatively stale*: it may reject reads it could safely serve,
  but never reads an inconsistent shard.
- **Initial behavior**:

  - The Zone Primary does not read shards in the Synchronous Recovery Set.
  - If the remaining shards cannot reconstruct the data, the Zone Primary returns ``-EAGAIN``.
  - Recovery messages tell the Zone Primary when recovery completes, which clears the set.

- **Subsequent enhancement**:

  - The Zone Primary can ask for permission to read a shard in the Synchronous Recovery Set.
  - The request triggers recovery of that object, if it is not complete, before the read is
    allowed.

.. _ecsc-zone-aware-replica-split-reads:

Zone-Aware Replica Split Reads — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section applies to **replicated pools** only. It does not need FastEC. It extends
   ``ReplicaSplitOp`` in ``src/osdc/SplitOp.cc``.

``ReplicaSplitOp`` divides a large read across replicas to run in parallel. With
``CEPH_OSD_FLAG_BALANCE_READS`` the chunks go round-robin to all available replicas, from a
randomly chosen start. With only ``CEPH_OSD_FLAG_LOCALIZE_READS`` set on a single-zone pool, the
read is not split: it goes to the nearest replica by CRUSH locality
(``Objecter::_calc_target``).

For a replicated pool over zones, ``CEPH_OSD_FLAG_LOCALIZE_READS`` restricts the sub-reads to
replicas in the **local zone**, the zone nearest to the client. A replicated acting set is not in
zone order: down OSDs are shifted out of it, and primary affinity moves the Primary to the front.
Each replica's zone is therefore its CRUSH ancestor of the zone type
(``peering_crush_bucket_barrier``), not its position in the acting set. The local zone is the zone
of the replica nearest to the client by CRUSH distance
(``CrushWrapper::get_common_ancestor_distance()``); a tie goes to the replica that comes first in
the acting set. All sub-reads go to replicas of that zone, however many replicas the other zones
have.

If no replica can be scored (``crush_location`` unset, or no common ancestor), or fewer than two
replicas of the chosen zone are available, the read is not split. It is sent as an ordinary
localized read, to the nearest replica, which redirects it to the Primary with
``-EAGAIN`` when it cannot serve it.

.. _ecsc-zone-aware-read-enhancements:

Zone-Aware Read Enhancements — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These enhancements to the erasure coded and replicated zone-aware reads are designed but not
needed for R1:

- **Zone Primary fallback (EC and replicated)**: a failed localized read, erasure coded direct read
  or replicated split read, is sent to the local Zone Primary rather than the Primary (:ref:`Read
  from the Zone Primary <ecsc-read-from-the-zone-primary>`). This avoids cross-zone latency on the
  fallback path when the client's zone is healthy but a single shard is briefly unavailable.
- **Per-zone min_size**: R1 enforces the per-zone ``min_size`` in peering only (:ref:`min_size in
  Each State <ecsc-min-size-in-each-state>`). The split reads should also consult the per-zone
  availability before choosing a zone for localized reads, so that they do not read from a zone that
  the Monitor has found to be below ``min_size``.
- **Zone-aware balanced reads for replicated pools**: ``BALANCE_READS`` on a replicated pool is
  restricted to zone-local replicas when enough of them are available, combining load
  distribution with locality.

.. _ecsc-kernel-rbd-client:

Kernel RBD Client — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The kernel RBD client (``krbd``) has its own EC direct read path, independent of ``librados``.
For erasure coded pools over zones, it must understand the ``num_zones × (k+m)`` shard layout to
find and target the shards in the client's zone. Without this, ``krbd`` may read shards in a
remote zone, losing the locality benefit. The change is small (the shard selection accounts for
the zone stride), but it is a kernel change and follows the kernel release cycle, independently
of Ceph releases.


.. _ecsc-write-path:

Write Path
----------

This section describes how writes are handled, in particular the read-modify-write (RMW)
sequences and the replication of write transactions to remote zones.

.. _ecsc-primary-encodes-all-shards:

Primary Encodes All Shards — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1 the Primary coordinates every write.

- **Mechanism**: the Primary encodes the ``k+m`` shards once and sends a write to every OSD in
  the acting set, including recovery and backfill targets, in every zone: ``num_zones × (k+m)``
  shard writes.
- **RMW reads**: the read part of a read-modify-write is done by the Primary, zone-local first, as
  in :ref:`Read from the Primary <ecsc-read-from-the-primary>`.
- **Inter-zone traffic**: all coded shard data, parity included, crosses the inter-zone link.
  This is not bandwidth-optimal, but it needs no Zone Primary and no new write path beyond
  extending the shard fan-out to the larger acting set.

.. _ecsc-replicate-transaction-preferred-path:

Replicate Transaction (Preferred Path) — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. mermaid::

   sequenceDiagram
       title Write to Primary

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard
       participant ZP as Zone Primary
       participant RS as Remote-zone Shard

       C->>P: Submit Op
       activate P

       P->>ZP: Replicate
       activate ZP

       note over P: Replicate message must contain<br/>data and which shards are to be updated.<br/>When OSDs are recovering, the primary<br/>may need to update remote-zone shards directly.

       P->>ZS: SubRead (cache)
       activate ZS
       ZS->>P: SubReadReply
       deactivate ZS

       P->>ZS: SubWrite
       activate ZS
       ZS->>P: SubWriteReply
       deactivate ZS

       ZP->>RS: SubRead (cache)
       activate RS
       RS->>ZP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>ZP: SubWriteReply
       deactivate RS

       ZP->>P: ReplicateDone
       deactivate ZP

       P->>C: Complete
       deactivate P

This mechanism minimizes the inter-zone bandwidth, which is often the most constrained resource
of a stretch cluster.

- **Mechanism**: the replicate message extends the existing sub-write message. Instead of
  sending coded shard data to every OSD in the remote zone, the Primary sends a copy of the
  *PGBackend transaction* (the raw write data and metadata, before EC encoding) to the Zone
  Primary.
- **Role of the Zone Primary**: the Zone Primary processes the transaction through its own
  ECTransaction pipeline, largely unmodified code, to generate the coded shards for its zone.
  For a write smaller than a full stripe this includes reading its zone-local shards to
  calculate the parity update. It then fans the shard writes out to the OSDs of its zone. Parity
  is never sent over the inter-zone link; each zone computes its own.
- **Completion**: the Zone Primary replies with the existing sub-write reply once all its
  zone-local shard writes are durable.
- **Crash handling**: if the Zone Primary crashes during the fan-out, the partial writes on
  remote-zone shards are rolled back by the existing peering process. No new crash recovery
  mechanism is needed.
- **Benefit**: the inter-zone traffic is one transaction message per remote zone, carrying only
  the raw data, not ``k+m`` coded chunks.

.. _ecsc-client-writes-to-the-zone-primary:

Client Writes to the Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This needs Replicate Transaction (:ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`), because the Zone Primary must be able to process
transactions and fan out shard writes in its zone.

.. mermaid::

   sequenceDiagram
       title Write to Zone Primary

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard
       participant RC as Remote Client
       participant ZP as Zone Primary
       participant RS as Remote-zone Shard

       RC->>ZP: Write op
       activate ZP

       ZP->>P: Replicate
       activate P
       P->>ZP: Permission to write

       note over ZP: There is potential for pre-emptive caching here,<br/>but it adds complexity and mostly does not actually<br/>help performance, as the limiting factor is actually<br/>the Remote-zone write.

       ZP->>RS: SubRead (cache)
       activate RS
       RS->>ZP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>ZP: SubWriteReply
       deactivate RS

       P->>ZS: SubRead (cache)
       activate ZS
       ZS->>P: SubReadReply
       deactivate ZS

       P->>ZS: SubWrite
       activate ZS
       ZS->>P: SubWriteReply
       deactivate ZS

       P->>ZP: write done
       ZP->>RC: Complete

       note over ZP: The write complete message is permitted<br/>as soon as all SubWriteReply messages have<br/>been received by the remote. It is shown<br/>here to demonstrate that it is required to<br/>complete processing on the Primary.

       ZP->>P: Remote-zone write complete
       deactivate P
       deactivate ZP

       P->>P: PG Idle.

       P->>ZP: DummyOp
       activate P
       activate ZP

       P->>ZS: DummyOp
       activate ZS
       ZS->>P: Done
       deactivate ZS

       ZP->>P: Done
       deactivate P

       note over ZP: Message ordering means that remote<br/>primary does not need to wait for<br/>dummy ops to complete

       ZP->>RS: DummyOp
       activate RS
       RS->>ZP: Done
       deactivate RS
       deactivate ZP

A client writes to its local Zone Primary rather than sending the data across the inter-zone link to
the Primary. The data then crosses the link once (Zone Primary to Primary) rather than twice (client
to Primary to Zone Primary, as in :ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`). Unlike :ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`, where the Primary starts the transaction and
replicates it outward, the Zone Primary receives the client write and coordinates with the Primary,
which keeps the global order.

- **Mechanism**:

  1. The client sends the write to its local **Zone Primary**, which stashes a copy of the data
     but does not yet fan out to zone-local shards.
  2. The Zone Primary replicates the *PGBackend transaction* (raw write data, before EC
     encoding) to the **Primary**.
  3. The Primary processes the transaction as usual through its ECTransaction pipeline: cache reads,
     encoding and the shard writes to its zone-local OSDs. Where it would replicate the data to the
     originating Zone Primary (as in :ref:`Replicate Transaction (Preferred Path)
     <ecsc-replicate-transaction-preferred-path>`), it sends a **write-permission message** instead,
     because that Zone Primary already has the data. Any *other* Zone Primaries (with ``num_zones``
     greater than 2) get the normal Replicate Transaction (:ref:`Replicate Transaction (Preferred
     Path) <ecsc-replicate-transaction-preferred-path>`).
  4. On write permission, the Zone Primary processes the transaction through its ECTransaction
     pipeline and fans the coded shard writes out to the OSDs of its zone.
  5. Once the Primary's own writes are durable and every other zone has confirmed durability,
     the Primary sends a **completion message** to the originating Zone Primary. The Zone
     Primary replies to the client once it has the completion message and its own writes are
     durable.

- **Write ordering**: writes stay strictly ordered across all zones, with the Primary as the
  single authority for PG log sequencing:

  - The Primary sequences the write in step 3. The write-permission message carries the sequence
    number, so writes that reach the Primary directly and through a Zone Primary are globally
    ordered.
  - Writes from several Zone Primaries (``num_zones`` greater than 2) are serialized by the
    Primary's normal sequencing; existing PG log ordering is enough.
  - The Zone Primary does not fan out to its zone-local shards until it gets write permission,
    so its shard writes happen in the global order.

- **Benefit**: for a client in a remote zone, the write data crosses the inter-zone link once, from
  the Zone Primary to the Primary. This halves the inter-zone traffic of :ref:`Replicate Transaction
  (Preferred Path) <ecsc-replicate-transaction-preferred-path>` (client to Primary to Zone Primary),
  and is far below R1 (:ref:`Primary Encodes All Shards <ecsc-primary-encodes-all-shards>`), which
  sends every coded shard across the link. The client also sees local write latency for the first
  acknowledgement, and the data is never sent back to the originating Zone Primary: the Primary
  sends only small permission and completion messages.
- **Failure handling**: if the Zone Primary is unavailable, the client writes to the Primary
  (:ref:`Primary Encodes All Shards <ecsc-primary-encodes-all-shards>` or :ref:`Replicate
  Transaction (Preferred Path) <ecsc-replicate-transaction-preferred-path>`, whichever is
  available). If the Zone Primary loses the Primary mid-transaction, it returns an error to the
  client and discards the stashed data; no zone-local shard write has been issued, because write
  permission never arrived.
- **Possible later enhancement: forward to other Zone Primaries** (``num_zones`` greater than 2).
  This may not be implemented. With more than two zones, the originating Zone Primary could send
  the transaction to the other Zone Primaries in parallel with the Primary, which would shorten
  their write latency. The Primary would still send write permission to every Zone Primary to
  keep the global order, but the data would already be there. The write-permission message from
  the Primary could overtake the data from the originating Zone Primary, so a unique transaction
  id would tie the two messages together at the receiving Zone Primary.

.. _ecsc-hybrid-approach:

Hybrid Approach — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This needs Replicate Transaction (:ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`). Complex failures may need a combination of Replicate
Transaction (:ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`), client writes to the Zone Primary (:ref:`Client
Writes to the Zone Primary <ecsc-client-writes-to-the-zone-primary>`) and the Primary encoding all
shards (:ref:`Primary Encodes All Shards <ecsc-primary-encodes-all-shards>`). For example, when one
remote zone is healthy and another degraded, the Primary may use Replicate Transaction for the
healthy zone and write the shards of the degraded zone directly, in the same transaction.


.. _ecsc-recovery:

Recovery
--------

The Primary OSD coordinates all recovery.

.. _ecsc-primary-centric-recovery:

Primary-Centric Recovery — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1 the Primary performs all recovery, without Zone Primaries and without minimizing
inter-zone bandwidth.

- **Reads**: the Primary reads the shards it needs to reconstruct the missing ones, from its own
  zone first and from remote zones only when its zone cannot supply them (:ref:`Read from the
  Primary <ecsc-read-from-the-primary>`).
- **Writes**: it decodes once and pushes the missing shards to their OSDs in every zone, its own
  and the remote ones.
- **Trade-off**: this may send more data over the inter-zone link than necessary, but it uses
  the existing recovery infrastructure with little change.
- **After going to one zone**: the shards of the zone that the pool no longer has, shard IDs ``k+m``
  and up, stay on their OSDs as strays until the PG is clean. They remain sources for their
  relative shards, whatever the pool's ``num_zones``: an object that only they hold at its newest
  version is recovered from them. A copy of a nonprimary shard is still a nonprimary shard
  (``pg_pool_t::is_nonprimary_shard()`` tests the relative shard), so the partial writes it
  skipped, which the primary's pwlc records, move its ``last_update`` as they do the shard's.

.. _ecsc-cost-based-recovery-planning:

Cost-Based Recovery Planning — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When an object (and its clones) needs recovery, the Primary assesses and plans its recovery.
This uses the existing reactive, object-by-object recovery rather than a new distributed
orchestration layer. The goal is to **minimize inter-zone bandwidth**: every cross-zone transfer
is expensive, so the Primary chooses, for each object, the strategy that sends the fewest bytes
across zone boundaries.

- **Assess**: for each zone, the Primary determines how many consistent chunks are available and
  how many are missing.
- **Choose by cost**: the Primary compares the inter-zone cost of the viable strategies and
  chooses the cheapest. The main trade-off is between:

  1. *Zone Primary recovers in its zone with remote-zone reads*: the Zone Primary reads the few
     shards it lacks from the Primary's zone and reconstructs locally.
  2. *Primary reconstructs and pushes*: the Primary reconstructs the object and pushes the
     missing shards to the remote zone.

  The best choice depends on how many shards each zone is missing.

- **Example**: ``k=6, m=2``, zone 0 has all 8 shards and zone 1 has only 5 (3 missing):

  - *Option A (Zone Primary reads remotely)*: the Zone Primary of zone 1 has 5 of the 6 chunks
    it needs and reads 1 from zone 0 to reconstruct its 3 missing shards. **Cost: 1 shard across
    the inter-zone link.**
  - *Option B (Primary pushes)*: the Primary reconstructs the 3 missing shards in zone 0 and
    pushes them to zone 1. **Cost: 3 shards across the inter-zone link.**

  Option A is cheaper. The algorithm compares the cross-zone transfers of each strategy and
  chooses the minimum. Where they are equal, it breaks the tie by zone-local read bandwidth or
  the total number of operations. (Later, the priorities may adapt, for example to optimize for
  read count rather than inter-zone bandwidth on fast links to slow rotational media, by
  auto-tuning or configuration. That is beyond the scope of this design.)

  .. note::

     The detailed algorithm needs further design work. The principle is to count the cross-zone
     shard transfers of each strategy and choose the minimum.

- **Plan construction**: for each zone, the Primary builds a "Recovery Plan" message that says
  which OSDs to read the available chunks from and which OSDs to write the recovered chunks to.
  Where the plan includes cross-zone reads, it says which remote-zone shards to fetch.

.. _ecsc-delegated-remote-zone-recovery:

Delegated Remote-zone Recovery — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This needs Cost-Based Recovery Planning (:ref:`Cost-Based Recovery Planning
<ecsc-cost-based-recovery-planning>`). The Primary sends a per-object Recovery Plan to the Zone
Primary, or other peers, which recover the object (and its clones) in their zone with the given read
and write sets. If the plan includes cross-zone reads, the Zone Primary fetches those shards before
it reconstructs. Because this ties into the existing object recovery lifecycle, a remote-zone peer
that fails during the plan simply drops the recovery operation. The Primary detects the failure
through peering or a timeout, and assesses the object again with a new plan.

.. _ecsc-fallback-strategies:

Fallback Strategies — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These strategies are part of Cost-Based Recovery (:ref:`Cost-Based Recovery Planning
<ecsc-cost-based-recovery-planning>`).

.. _ecsc-remote-zone-fallback-push-recovery:

Remote-zone Fallback (Push Recovery)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a remote zone cannot recover locally, and the cost analysis finds that reconstructing at the
Primary and pushing is cheapest:

1. The Primary reconstructs the object, from whatever valid shards the cluster has.
2. The Primary pushes the object data to the Zone Primary.
3. The push says which shards of the remote zone must be written.

.. _ecsc-primary-zone-fallback:

Primary Zone Fallback
^^^^^^^^^^^^^^^^^^^^^

If the Primary's own zone lacks the chunks to recover locally, the Primary reads the shards it needs
from remote zones, as in R1 (:ref:`Primary-Centric Recovery <ecsc-primary-centric-recovery>`). With
cost-based planning, this is chosen only when it needs fewer cross-zone transfers than any
alternative.


.. _ecsc-pg-log-handling:

PG Log Handling
---------------

This section describes how PG log entries are kept across replicated EC stripes, building on the
FastEC design for primary-capable and non-primary shards.

.. _ecsc-primary-capable-and-non-primary-shards:

Primary-Capable and Non-Primary Shards — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FastEC distinguishes two classes of shard:

- **Primary-capable shards** keep a full copy of the PG log, so that they can take over as
  Primary.
- **Non-primary shards** keep a reduced PG log, without the entries of writes that did not touch
  that shard.

For a pool over zones, the following shards are **primary-capable**:

- the first data shard of each zone;
- all coding (parity) shards of each zone.

All other data shards are non-primary shards with reduced logs (``nonprimary_shards``, :ref:`Size,
min_size and Shards <ecsc-size-min-size-and-shards>`).

.. note::

   As for a single-zone FastEC pool, ``pg_temp`` lists all primary-capable shards in shard order
   (zone 0, then zone 1) ahead of the shards that are not primary-capable
   (``OSDMap::pgtemp_primaryfirst``).

.. _ecsc-independent-log-generation-at-the-zone-primary:

Independent Log Generation at the Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log generation at the Zone Primary is needed only with Replicate Transaction writes (:ref:`Replicate
Transaction (Preferred Path) <ecsc-replicate-transaction-preferred-path>`). In R1 the Primary sends
encoded shards directly, and generates every PG log entry and sends it with the sub-writes, as for
any FastEC pool.

To reduce latency, the replicate transaction message (:ref:`Replicate Transaction (Preferred Path)
<ecsc-replicate-transaction-preferred-path>`) is sent to the Zone Primary *before* the cache read
completes on the Primary. The Zone Primary therefore cannot receive a ready-made log entry from the
Primary: it generates its own PG log entry from the transaction data. The Primary and the Zone
Primary produce equivalent entries from the same transaction, independently in each zone.

.. _ecsc-log-entry-compatibility-and-upgrade-safety:

Log Entry Compatibility and Upgrade Safety — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``require_osd_release`` in the OSDMap sets the minimum release that OSDs can run, and so the log
entry format that must be used. No new versioning mechanism is needed: the existing release
gating constrains log entry compatibility in mixed-version clusters.

Because the Zone Primary generates its own log entry from the transaction, it must populate the
object context (OBC) to get the old version of the attributes, including ``object_info_t`` for
the old size. This is another reason why the Zone Primary must be a **primary-capable shard**:
only those keep the full PG log and the OBC state needed to build log entries correctly.

**Debug mode for log entry equivalence**

A debug mode compares the log entries generated by the Primary and the Zone Primary for each
transaction. When it is enabled, the Primary includes its log entry in the replicate transaction
message, and the Zone Primary asserts that its own entry is identical. The mode is **enabled by
default in teuthology** to catch divergence early. It is a runtime configuration option for
production debugging, disabled by default in production because of the extra message overhead.

.. warning::

   If a future release changes how PG log entries are derived from transactions, the debug
   equivalence mode is an automated safety net: any divergence between the Primary's and the
   Zone Primary's log entries shows in CI at once.


.. _ecsc-peering:

Peering — *R1*
--------------

.. _ecsc-acting-set-calculation:

Acting Set Calculation
~~~~~~~~~~~~~~~~~~~~~~

``calc_replicated_acting_stretch`` works at the CRUSH bucket level for any pool type, but an
erasure coded acting set has **shard semantics**: position ``i`` must serve shard ``i``.
``calc_ec_acting_stretch`` therefore extends ``calc_ec_acting`` with the CRUSH bucket awareness
of the stretch path.

Objects are stored per shard, so an OSD is usable for position ``i`` only if it holds shard
``i`` itself. When ``num_zones`` is greater than 1, all positions of a zone block are served
from one CRUSH zone, and no two blocks share a zone. Assignments of zones to blocks are compared
in this order:

1. one whose shards can recover the PG, if any can;
2. as in ``calc_ec_acting``, the one that fills the most positions ``i`` from a usable ``up[i]``
   or ``acting[i]``, then from ``up[i]``, so strays only fill positions that ``up`` and
   ``acting`` cannot;
3. the one whose zones hold the most of their blocks' shards, counting a zone only if its own
   shards can recover the PG;
4. the one that holds the most shards;
5. the one that uses the most ``acting`` OSDs;
6. the first found, with zones in ascending CRUSH bucket id.

A block therefore keeps being served from where its data is (for example from ``acting`` after
``up`` swaps the zone blocks) while the ``up`` OSDs are backfilled, and moves to the ``up`` OSDs
once they hold it.

For each position ``i`` (0 to ``num_zones × (k+m) - 1``), considering only OSDs in the CRUSH
zone chosen for its block:

1. Prefer ``up[i]`` if usable.
2. Otherwise prefer ``acting[i]`` if usable.
3. Otherwise search the strays for an OSD with shard ``i``.
4. If no usable OSD is found, leave ``CRUSH_ITEM_NONE``.

An ``up[i]`` that is not chosen for position ``i`` is backfilled only if its block would be
served from ``up[i]``'s zone once the ``up`` OSDs hold their shards. That goal is the same zone
choice made over every ``up[i]``, the usable ``acting[i]`` and the chosen positions, which count
as the acting set because they are what ``acting`` becomes. Other strays are left out, so calls
restricted to ``up`` and ``acting``, such as the one from ``Recovered``, reach the same goal. An
``up[i]`` that already holds shard ``i`` is backfilled too: outside the acting set it gets no
writes, and the log may be trimmed past it before ``Recovered``. A completed backfill target
therefore always joins the new acting set, as with ``calc_ec_acting``, and ``choose_acting``
never finds ``want == acting`` with fewer backfill targets. An ``up[i]`` placed in a zone that
its block will not be served from, for example by a ``pg-upmap-items`` entry into another zone,
is not backfilled, and the ``pg_temp`` stays.

CRUSH ``bucket_max`` constraints apply: no zone may contribute more than ``size /
peering_crush_bucket_target`` OSDs (:ref:`peering_crush_bucket Values
<ecsc-peering-crush-bucket-values>`).

.. _ecsc-async-recovery:

Async Recovery
~~~~~~~~~~~~~~

``choose_async_recovery_replicated`` and ``choose_async_recovery_ec`` take an OSD out of the acting
set only if the acting set left without it could still go active. For a stretched pool, replicated
or erasure coded, that set must pass ``stretch_set_can_peer()`` and keep each zone at the per-zone
``min_size``, counted as in :ref:`Stretch Set Validation <ecsc-stretch-set-validation>`. An erasure
coded pool also keeps shard identity: the OSD's position becomes ``CRUSH_ITEM_NONE``, and the shards
left must still be able to recover the PG.

.. _ecsc-stretch-set-validation:

Stretch Set Validation
~~~~~~~~~~~~~~~~~~~~~~

``stretch_set_can_peer`` is the same for erasure coded and replicated pools: the acting set must
span at least ``peering_crush_bucket_count`` zones and include the
``peering_crush_mandatory_member``, if one is set. Zones are compared by their CRUSH bucket, so
a rule that takes its zones with a device class (``step take <zone> class <class>``) works.
Erasure coded pools need no extra check here.

Each zone's OSDs are checked against the per-zone ``min_size`` instead
(``OSDMap::stretch_num_acting_below_min_size``): in healthy stretch mode every zone needs at least
``min_size`` OSDs, and in degraded and recovery stretch mode only the surviving zone, the mandatory
member, is counted (:ref:`min_size in Each State <ecsc-min-size-in-each-state>`). Two layouts are
counted over the whole acting set, which needs ``min_size × num_zones`` OSDs: a stretched pool with
one zone, that is an individual stretch pool, including an erasure coded pool that an earlier
release's ``stretch set`` gave stretch values (:ref:`Pools from Earlier Releases
<ecsc-pools-from-earlier-releases>`), and a pool over three zones with a
``peering_crush_bucket_count`` of 2, for a later release. The zones counted are all
the buckets of the zone type that have OSDs under the rule's ``take`` roots, not only those the
acting set uses, so the "exactly ``num_zones`` zone buckets" of :ref:`The CRUSH Rule of a Pool
<ecsc-the-rule-of-a-pool>` matters for peering too. A shadow bucket of a device class counts as
its zone (``get_non_shadow_id()``), so a rule that takes a zone with two device classes counts
it once.


.. _ecsc-scrub-behavior:

Scrub Behavior — *R1*
----------------------

Scrub changes only in how deep scrub verifies the CRCs that each shard reports. Deep scrub must:

1. **Verify the coding CRC** within each zone's stripe, where the EC plugin supports it. This is
   current behavior, extended to the copies in every zone. It reaches the data shards and the
   first parity shard.
2. **Verify cross-zone CRC equivalence**: the shards with the same relative shard in different zones
   must have the same CRC. For example, shard 0 in zone 0 must match shard 3 in zone 1 in the
   diagram of :ref:`Logical Diagram (Two Zones) <ecsc-logical-diagram-two-zones>`.

When the copies of a shard differ, scrub reports each copy that differs from a reference copy.
The reference is chosen by three tests in turn: its zone's stripe passes the coding CRC check,
the most zones hold its CRC, and its zone comes first. If a differing copy passes the first two
tests as well as the reference, for example a copy of a parity shard that the coding CRC check
does not reach, nothing shows which copy is good. Scrub then reports both copies, and repair
rebuilds both from the other shards.

The coding CRC check needs a plugin that can encode and decode CRCs
(``get_ec_supports_crc_encode_decode()``); the cross-zone comparison does not. ISA supports it
with ``reed_sol_van``, and with ``cauchy`` only when ``m`` is 1; normalization switches ISA from
``reed_sol_van`` to ``cauchy`` when ``m`` is greater than 4, or 4 with ``k`` greater than 21.
jerasure's FastEC technique, ``reed_sol_van``, does not support it. A pool over zones without
the coding check still gets the cross-zone comparison, whose reference is then the CRC that the
most zones hold: in a pool over two zones, scrub reports both copies of a shard that differ.

Scrub never reads data to the Primary: it collects and compares the CRCs that each OSD reports.
Inter-zone bandwidth is therefore not a significant concern, and scrub stays Primary-centric for
pools over zones.


.. _ecsc-migration:

Migration — *R1*
----------------

Migration from an existing EC pool to an erasure coded pool over zones uses the **Pool
Migration** design, which is implemented separately. This design defines no separate migration
mechanism.

An existing FastEC pool can also be stretched in place with ``ceph osd pool set <pool> num_zones 2``
(:ref:`num_zones <ecsc-num-zones>`). The standard recovery process then backfills the new zone.


.. _ecsc-upgrade-and-backward-compatibility:

Upgrade and Backward Compatibility — *R1*
-----------------------------------------

During a rolling upgrade, OSDs of the earlier release and of this release peer the same PGs. Both
must judge every interval of a stretch pool alike: if one release records as "could not have gone
active" an interval in which the other took writes, a later peering can skip that interval and
lose acknowledged writes. So until the upgrade is committed, every daemon reads and judges every
pool as the earlier release does, and the Monitors keep the earlier release's behavior. The commit
is one OSDMap update, which sets the OSDMap flag ``stretch_zones``. From that epoch on the rules of
this design apply, and only OSDs of this release take part.

.. _ecsc-release-gate:

Release Gate
~~~~~~~~~~~~

* **The commit** is the OSDMap update that raises ``require_osd_release`` to
  ``OSDMonitor::NUM_ZONES_RELEASE``, the release that ships this design. That update sets the
  OSDMap flag ``stretch_zones`` (``CEPH_OSDMAP_STRETCH_ZONES``, ``OSDMap::stretch_zones()``). New
  clusters start with it. Nothing clears it and nothing else sets it, so an OSDMap has the flag
  exactly when it is from the commit on.
* **Everything else keys on the flag**, not on ``require_osd_release``. A cluster that commits on a
  release that carries this design as a backport reaches a later release with the flag already
  set, and the later release never treats its pools as uncommitted. Only ``NUM_ZONES_RELEASE``
  differs between such releases.
* **The OSD feature bit** ``OSD_STRETCH_ZONES`` marks the daemons and clients that implement this
  design. It tells apart releases that share a release feature bit, such as Umbrella and a backport
  of this design to it.

``NUM_ZONES_RELEASE`` is ``vampire``. A backport of this design to Umbrella sets it to
``umbrella``; a cluster that commits there reaches Vampire with the flag already set.

.. _ecsc-encoding:

Encoding
~~~~~~~~

* **New fields get a new version.** ``replica`` and ``num_zones`` are struct version 34 of
  ``pg_pool_t``, written only for peers with ``OSD_STRETCH_ZONES``. Umbrella peers get version
  33, earlier ones their own.
* **The canonical encoding switches at the commit.** ``OSDMap::get_encoding_features()`` drops
  ``OSD_STRETCH_ZONES`` while the map lacks the flag. Until the commit every full OSDMap and
  incremental is therefore written without ``num_zones`` and ``replica``, every daemon decodes the
  same pools from the same bytes, and an earlier release re-encodes them to the same bytes, so the
  full-map CRCs agree.
* **Decode never converts.** An older version decodes with ``num_zones`` 1, ``replica`` equal to
  ``size`` and ``min_size`` as written: a total over all copies, as the earlier release reads it.
  Only the commit converts pools (:ref:`At the Commit <ecsc-at-the-commit>`).
* **Encode in an older format writes the older meaning**: ``min_size`` is written as the per-zone
  ``min_size`` × ``num_zones``. After the commit only clients without ``OSD_STRETCH_ZONES`` get the
  older format; they see a pool over zones as one zone with that total.
* **Only representable states before the commit.** Until the commit every pool has ``num_zones`` 1.
  The commands that could change that are refused until then (:ref:`Before the Commit
  <ecsc-before-the-commit>`).

.. _ecsc-maps-profiles-and-configuration:

Maps, Profiles and Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **OSDMap**: ``stretch_mode_enabled``, ``stretch_bucket_count``, ``stretch_mode_bucket`` and the
  degraded and recovering state keep their encoding. Their meaning changes at the commit, from
  "global stretch mode is enabled" to "stretch mode is enabled because a pool was given
  ``num_zones`` 2" (:ref:`Stretched Pools and Stretch Mode <ecsc-stretched-pools-and-stretch-mode>`;
  individual stretch pools do not enable it). Only the Monitors use the stretch mode state. The
  flag ``stretch_zones`` records the commit (:ref:`Release Gate <ecsc-release-gate>`).
* **MonMap**: ``stretch_mode_enabled``, the tiebreaker, the disallowed leaders and the
  stretch-marked-down Monitors keep their encoding and meaning.
* **EC profiles** keep their encoding. Pools that refer to ``default`` are moved to copies of it at
  the commit (:ref:`At the Commit <ecsc-at-the-commit>`).
* **Configuration**: the pool creation defaults are converted at the commit (:ref:`At the Commit
  <ecsc-at-the-commit>`).
* **Messages and log entries** that later releases add are gated by ``require_osd_release`` in the
  same way (:ref:`Log Entry Compatibility and Upgrade Safety
  <ecsc-log-entry-compatibility-and-upgrade-safety>`).

.. _ecsc-peering-across-the-commit:

Peering across the Commit
~~~~~~~~~~~~~~~~~~~~~~~~~

Each rule is judged from the OSDMap of the epoch being judged, so every daemon gives the same
answer for the same epoch:

* **What** ``min_size`` **counts** follows ``num_zones``: a total over all copies for one zone, the
  copies in each zone for more (``OSDMap::stretch_num_acting_below_min_size()``). Before the commit
  every pool has one zone.
* **How buckets are counted** follows the flag. Without it, ``stretch_set_can_peer()`` and
  ``calc_replicated_acting_stretch()`` count as earlier releases do: each shadow bucket of a
  device-class rule (``dc1~ssd``) is a bucket of its own, and a hole in an erasure coded acting set
  counts as a bucket. From the commit on they count real buckets (``get_non_shadow_id()``) and skip
  holes, for individual stretch pools too.
* **The commit starts a new interval** for every stretch pool
  (``PastIntervals::is_new_interval()``), so that no interval is judged under both rules. Every
  change of ``num_zones`` also starts one.

.. _ecsc-before-the-commit:

Before the Commit
~~~~~~~~~~~~~~~~~

Before the commit, upgraded and earlier Monitors may be in quorum together, and the upgraded
Monitors keep the earlier release's behavior where this list does not say otherwise. Refusals that
last until the commit are ``EPERM``, with messages that end "until the upgrade is committed with
'ceph osd require-osd-release <release>'".

* **Pools are created as earlier releases create them**, by ``ceph osd pool create`` and by
  librados (``POOL_OP_CREATE``, which components such as RGW use on demand). In global stretch mode
  a new pool is a replicated stretch pool on the rule that most stretched pools use, with
  ``mon_stretch_pool_size`` copies and a ``min_size`` of ``mon_stretch_pool_min_size`` over all of
  them, halved in degraded stretch mode; another size, and an erasure coded pool, are refused, as
  earlier releases refuse them. The commit converts these pools like any other (:ref:`At the Commit
  <ecsc-at-the-commit>`). Every pool is created in one zone, whatever
  ``osd_pool_default_num_zones`` says. Only what earlier releases cannot express is refused: a
  ``num_zones`` other than 1 given on the command line ("a pool with num_zones <n> cannot be
  created until ...").
* **num_zones** cannot be changed: any use of ``num_zones`` on ``ceph osd pool set`` is an error,
  even with the value 1 ("num_zones cannot be changed until ...").
* ``ceph osd pool default set`` is refused ("ceph osd pool default set is refused until ...").
  ``ceph osd pool default get`` shows the defaults at any time; before the commit they are the
  values that the commit may still convert.
* Deleting the last stretched pool does not disable stretch mode (:ref:`Leaving Stretch Mode
  <ecsc-leaving-stretch-mode>`).
* ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode`` behave as in earlier releases
  (:ref:`Global Stretch Mode in Earlier Releases <ecsc-global-stretch-mode-in-earlier-releases>`):
  enabling gives each pool ``mon_stretch_pool_size`` copies, a ``min_size`` of
  ``mon_stretch_pool_min_size`` over all of them, and one zone. ``enable_stretch_mode`` is also
  refused while an individual stretch pool exists (:ref:`Entering Stretch Mode
  <ecsc-entering-stretch-mode>`), which earlier releases allowed. An earlier-release Monitor that
  leads the quorum still allows it.
* The degraded and healthy transitions halve and restore the ``min_size`` of every stretch pool
  whose ``min_size`` is a total, that is, one with one zone, as earlier releases do. Whichever
  release leads, the OSDMap therefore holds the same values (:ref:`Degraded, Recovery and Healthy
  <ecsc-degraded-recovery-and-healthy>`).
* ``ceph osd pool stretch set`` and ``unset`` follow :ref:`Individual Stretch Pools
  <ecsc-individual-stretch-pools>`.
* These are not gated on the release, so an earlier-release Monitor behaves differently: a profile
  is deleted with its last pool (:ref:`Profile Lifetime <ecsc-profile-lifetime>`); an unused rule is
  removed on ``ceph osd pool delete``, ``ceph osd pool set <pool> crush_rule`` and ``replica``, and
  ``stretch set`` and ``unset``, and a failed command removes the rules it generated (:ref:`Rule
  Lifetime <ecsc-rule-lifetime>`); and commands wait for a pending profile or rule removal.

No operator action is needed for safety at any point of the upgrade, including a zone outage
during it: every daemon judges every epoch before the commit by the earlier release's rules.

.. _ecsc-at-the-commit:

At the Commit
~~~~~~~~~~~~~

``ceph osd require-osd-release <release>`` commits the upgrade. As for any release, it is
refused until every Monitor runs the release, and, unless ``--yes-i-really-mean-it`` is given,
until every up OSD has the release's feature bit and ``OSD_STRETCH_ZONES`` and while no OSD is up.
While an OSD boot is pending in the same update it waits for that update. After the commit the
cluster cannot be downgraded. The commit's own OSDMap update, after the release's other
first-epoch steps and from the pools they leave pending:

1. Sets ``stretch_zones``. From this epoch on OSDMaps are written in the new format.
2. Converts each pool of global stretch mode that has its copies evenly in two zones: replicated,
   with an even ``size``, ``peering_crush_bucket_target`` 2, the stretch mode bucket as its barrier,
   and a rule that places ``size / 2`` copies in each zone. It gets ``num_zones`` 2, ``replica``
   ``size / 2`` and a per-zone ``min_size`` that never accepts writes with fewer copies than before:

   - healthy: ``min_size / 2``, rounded up;
   - degraded or recovering: ``mon_stretch_pool_min_size / 2``, rounded up, because the pool's
     ``min_size`` was halved and becoming healthy no longer restores it. A pool created while
     degraded, which has no mandatory member, gets the surviving zone, so that it stays active.

   ``min_size`` is at least 1 and at most ``replica``. Other stretch pools keep one zone and their
   ``min_size``: individual stretch pools, and pools of global stretch mode with an odd ``size``,
   another bucket target or another placement. ``STRETCH_MODE_POOL_NOT_CONVERTED`` names the
   pools of global stretch mode that keep one zone, before the commit, while they can still be
   changed, and after it, as ``disable_stretch_mode`` leaves them stretched.
3. Forces an op resend on every stretch pool, setting all three resend epochs, which the kernel
   client needs. The peering rules change for every stretch pool, so its PGs start a new interval
   that clients cannot see, and replicas drop the reads that clients sent before it.
4. Marks down every up OSD without ``OSD_STRETCH_ZONES``, which is only possible with
   ``--yes-i-really-mean-it``.
5. If the committed OSDMap has ``stretch_mode_enabled`` set, the cluster is in global stretch
   mode, because earlier releases set it only with ``ceph mon enable_stretch_mode``. The
   Monitors then convert the pool creation defaults once, so that the size and zone defaults in
   effect are kept:

   - ``osd_pool_default_num_zones`` is set to 2 and ``osd_pool_default_zone_failure_domain`` to
     the type of the OSDMap's ``stretch_mode_bucket``;
   - half of ``mon_stretch_pool_size`` (2 by default) goes to ``osd_pool_default_size``;
   - ``mon_stretch_pool_min_size`` is ignored, since ``min_size`` defaults per zone. An explicit
     ``osd_pool_default_min_size`` applies in each zone (:ref:`Configuration Options
     <ecsc-configuration-options>`): a cluster that sets it to 2, as is common for 3-copy pools,
     gives every new 2-replica-per-zone pool a ``min_size`` of 2 in each zone, where earlier
     releases gave stretched pools 1 in each zone.

   Only the ``global`` section is written. A value that overrides it for the Monitors (a
   ``mon``, ``mon.<id>`` or masked section, or a Monitor's local configuration) is left as it
   is. Earlier releases ignored ``osd_pool_default_size`` in global stretch mode, so after the
   commit such a value gives new pools that many copies in each zone, and
   ``POOL_DEFAULT_CONFIG_OVERRIDE`` reports it. The commit neither rewrites those sections, which
   their owner set deliberately, nor refuses to proceed over them; it cannot reach a ``ceph.conf``
   or a value an orchestrator writes back (Rook re-applies ``spec.cephConfig`` on every
   reconcile). The release notes and the stretch mode documentation say to remove such a value.

   The default rule is not kept: earlier releases gave a new pool the rule that most stretched pools
   used, whereas a new two-zone replicated pool now gets a generated ``<pool>`` rule (:ref:`The
   CRUSH Rule of a Pool <ecsc-the-rule-of-a-pool>`), and ``osd_pool_default_crush_rule`` is not set.
   Stretch mode itself stays enabled. The configuration change is proposed separately from the
   OSDMap change, so a commit that is lost after it can leave the defaults converted while the
   cluster is still before the commit; a retry proposes the same values.
6. Every erasure coded pool that refers to ``default`` gets a profile of its own,
   ``<pool_name>-k<k>-m<m>`` (``k`` and ``m`` are the plugin's data and coding chunk counts),
   holding exactly the keys and values of ``default``, without normalization. If the Monitor
   cannot load the plugin to count the chunks, the copy is named ``<pool_name>-default``
   instead. If the name already holds other contents, a suffix ``-<n>`` is added; if it holds
   the same contents, that profile is used and shared. The new profile and the pool's new
   ``erasure_code_profile`` are in the same OSDMap update, and the change is logged to the
   cluster log. ``default`` and ``osd_pool_default_erasure_code_profile`` are not changed, so
   new pools get what they got before the upgrade.

   The copy is exactly what the OSDs already use, so it cannot fail and the pool does not
   change: OSDs build the same EC backend from it, and ``size``, ``min_size``, ``stripe_width``,
   the shard counts and the CRUSH rule are kept. OSDs follow a pool's reference only when they
   instantiate a PG, in the OSDMap of that epoch, and the profile name is not part of the
   interval check. Running PGs keep their backend, and a PG loaded at an earlier epoch gets
   ``default`` from that epoch's map. The copies are made for the pools as the commit's OSDMap
   update leaves them: a pool created earlier in that update gets one, and a pool deleted in it none. ``ceph osd pool create`` and
   ``ceph osd pool delete`` that come after the commit in its update wait for it to commit.
7. The Monitors switch to the new behavior. Pool creation and ``num_zones`` changes are allowed,
   ``ceph osd pool default set`` works, the stretch mode commands behave as in :ref:`Global Stretch
   Mode Commands <ecsc-global-stretch-mode-commands>`, and ``stretch_mode_enabled`` follows the
   pools.

From the commit on every OSD has ``OSD_STRETCH_ZONES``. An OSD of an earlier release would judge
the new epochs by its own rules and keep those judgments on disk, even while its boot is refused,
and would act on them once upgraded. So ``OSDMap::get_features()`` requires the bit of OSDs, and
an OSD without it cannot connect to a Monitor or another OSD; ``preprocess_boot`` and
``prepare_boot`` refuse its boot, also while the commit is pending; and no Monitor or OSD sends it
an OSDMap. It can boot once it runs this release. Clients never need the bit. A client without it
works out the shard of an op to an optimized erasure coded pool with ``pg_temp`` as if the pool had
one zone, so for a pool over zones the OSD recovers the client's position in primary-first order
and replaces the shard (``OSDMap::undo_pre_stretch_zones_primaryfirst()``), as it does for clients
older than Tentacle.

.. _ecsc-pools-from-earlier-releases:

Pools from Earlier Releases
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A pool with ``num_zones`` 1 is fully backward compatible. An erasure coded pool with one zone has
  a standard ``k+m`` acting set, with no new on-disk format and no new wire messages, so existing
  OSDs and clients handle it as a conventional EC pool. If it refers to ``default``, it refers to a
  copy of it after the commit (:ref:`At the Commit <ecsc-at-the-commit>`).
* A replicated pool of global stretch mode keeps one zone and its ``min_size`` over all copies until
  the commit, which gives it two zones and a per-zone ``min_size`` (:ref:`At the Commit
  <ecsc-at-the-commit>`). It keeps its custom CRUSH rule. With the ``size`` 4 and ``min_size`` 2 of
  global stretch mode, each zone needs 1 replica, as before; ``size`` 4 with ``min_size`` 3 needs 2
  in each zone.
* A replicated individual stretch pool keeps ``num_zones`` 1, ``replica`` equal to ``size`` and its
  ``min_size`` over all its copies. It peers as in the earlier release until the commit, and from
  the commit on counts its buckets as :ref:`Peering across the Commit
  <ecsc-peering-across-the-commit>` says (:ref:`Individual Stretch Pools
  <ecsc-individual-stretch-pools>`).
* An erasure coded individual stretch pool, which only an earlier release's ``stretch set`` can
  make, keeps one zone. It is decoded with ``num_zones`` 1 and its ``min_size`` unchanged, and peers
  as a single-zone erasure coded pool (``calc_ec_acting``) that needs ``min_size`` shards in its
  whole acting set, while ``stretch_set_can_peer`` still applies its ``peering_crush_bucket_*``
  values. ``ceph osd pool stretch unset`` clears them (:ref:`Individual Stretch Pools
  <ecsc-individual-stretch-pools>`), after which ``num_zones`` can be changed.
* No data migration is needed. Pools with ``num_zones`` greater than 1 bring a larger acting set
  (``num_zones × (k+m)`` shards), new CRUSH rules and, in later releases, new inter-OSD
  messages. That is why they can only be created after the commit.


.. _ecsc-inter-zone-statistics:

Inter-Zone Statistics — *R1*
----------------------------

Operators need to see how the inter-zone link is used, to check that pools over zones deliver
their locality benefit and to plan capacity. In R1 each OSD has perf counters of the
sub-operations of stretched pools that it sends to OSDs in another zone, for erasure coded and
replicated pools alike (:ref:`Cross-Zone Performance Counters <stretch_mode_perf_counters>`):

- **Write fan-out**: ``cross_zone_write_ops``, ``cross_zone_write_bytes``,
  ``cross_zone_write_latency`` and ``cross_zone_write_latency_bytes_histogram``.
- **Recovery push**: ``cross_zone_recovery_push_ops``, ``cross_zone_recovery_push_bytes``,
  ``cross_zone_recovery_push_latency`` and ``cross_zone_recovery_push_latency_bytes_histogram``.
- **Remote-zone reads**, erasure coded sub-reads and replicated pulls: ``cross_zone_subread_ops``,
  ``cross_zone_subread_sent_bytes`` (requested by erasure coded sub-reads),
  ``cross_zone_subread_recv_bytes``, ``cross_zone_subread_latency`` and
  ``cross_zone_subread_latency_bytes_histogram``.

Each latency counter is an average of the round-trip latency, and each histogram counts the
sub-operations by latency and size. The counters use the existing perf counter infrastructure
and are queried with ``ceph tell osd.N perf dump``.

*Later Release*: statistics per pool, for example with ``ceph osd pool stats``; latency
percentiles (p50, p95, p99), to detect link degradation; and, for pools with direct reads, the
ratio of reads served in the client's zone to those that fell back to the Primary across a zone
boundary. A high remote ratio shows a locality problem.

.. note::

   A sub-operation is counted as cross-zone by comparing the zone of the source OSD with the
   zone of the destination OSD: their CRUSH ancestors of the zone type. The fan-out does not
   otherwise need the zones, so the statistics add these lookups for each sub-operation of a
   stretched pool.


.. _ecsc-implementation-order:

Implementation Order
--------------------

This section gives the phased implementation plan, broken into single-sprint stories.

.. _ecsc-r1-read-optimized-stretch-cluster:

R1: Read-Optimized Stretch Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

R1 delivers an erasure coded stretch cluster with zone-local reads, comparable to what
replicated stretch clusters provide. All writes and recovery cross the inter-zone link through
the Primary.

1. **CRUSH rule generation for pools over zones**: generate rules from ``num_zones`` and the
   placement options, and check that a rule places ``k+m`` shards in each of ``num_zones`` zones
   (:ref:`CRUSH Placement <ecsc-crush-placement>`).
2. **Pool parameters** ``num_zones`` **and** ``replica``: accept and check them in ``ceph osd pool
   create`` and ``ceph osd pool set`` (:ref:`ceph osd pool create <ecsc-ceph-osd-pool-create>` and
   :ref:`Changing a Pool <ecsc-changing-a-pool>`). ``num_zones`` is never part of the EC profile; a
   generated profile records the zone type as ``crush-zone-failure-domain`` only when it is given
   (:ref:`Placement Options <ecsc-placement-options>`).
3. **Primary-capable shard selection**: the first data shard and all parity shards of each zone are
   primary-capable (:ref:`Primary-Capable and Non-Primary Shards
   <ecsc-primary-capable-and-non-primary-shards>`).
4. **Stretch mode and peering**, in sub-stories:

   a. **Pool creation defaults, EC profiles and global stretch mode commands** (OSDMonitor): a
      configuration option default for every pool creation parameter, ``ceph osd pool default`` to
      set and show them, a profile of its own for every erasure coded pool, and ``ceph mon
      enable_stretch_mode``/``disable_stretch_mode`` that change the defaults and the pools,
      replicated and erasure coded (:ref:`Erasure Code Profiles <ecsc-erasure-code-profiles>`,
      :ref:`Pool Creation Defaults <ecsc-pool-creation-defaults>` and :ref:`Global Stretch Mode
      Commands <ecsc-global-stretch-mode-commands>`).
   b. **Stretch mode transitions** (OSDMonitor): fill the
      ``if (is_replicated()) { ... } else { /* not supported */ }`` gaps of the stretch mode code
      for erasure coded pools, with degraded, recovery and healthy transitions that change
      ``peering_crush_bucket_count`` and ``peering_crush_mandatory_member``, not ``min_size``
      (:ref:`Degraded, Recovery and Healthy <ecsc-degraded-recovery-and-healthy>`).
   c. **EC peering with stretch constraints** (PeeringState): ``calc_ec_acting_stretch``,
      respecting shard identity and ``bucket_max``, and the async recovery checks (:ref:`Peering
      <ecsc-peering>`).
   d. **is_recoverable and is_readable**: ``ECRecPred`` and ``ECReadPred`` map each shard of the
      larger shard set to its relative shard; the per-zone ``min_size`` is checked by
      ``OSDMap::stretch_num_acting_below_min_size`` (:ref:`Stretch Set Validation
      <ecsc-stretch-set-validation>`).

5. **Primary write fan-out to all shards**: the Primary encodes and sends ``num_zones × (k+m)``
   shard writes to all OSDs of the acting set (:ref:`Primary Encodes All Shards
   <ecsc-primary-encodes-all-shards>`).
6. **Direct reads with Primary fallback**: zone-aware EC direct reads and replicated split reads;
   every failure goes to the Primary (:ref:`EC Direct Reads over Zones
   <ecsc-ec-direct-reads-over-zones>` and :ref:`Zone-Aware Replica Split Reads
   <ecsc-zone-aware-replica-split-reads>`).
7. **Recovery**:

   a. **Single-OSD recovery**: the Primary recovers a missing OSD in any zone, reading its own
      zone first and remote zones only when its zone cannot supply the shards (:ref:`Read from the
      Primary <ecsc-read-from-the-primary>` and :ref:`Primary-Centric Recovery
      <ecsc-primary-centric-recovery>`).
   b. **Full-zone recovery**: after a zone is lost, the Primary reads its surviving zone,
      encodes the full stripe and pushes all ``k+m`` shards to the recovering zone's OSDs. This also
      covers several failed OSDs in one zone (:ref:`Primary-Centric Recovery
      <ecsc-primary-centric-recovery>`).

8. **Scrub**: cross-zone CRC equivalence of corresponding shards (:ref:`Scrub Behavior
   <ecsc-scrub-behavior>`).
9. **Network partition and zone failover**: the existing Monitor-based zone election and OSD
   heartbeats work with the erasure coded acting set (:ref:`Network Partition Handling
   <ecsc-network-partition-handling>`).
10. **End-to-end two-zone integration**: read, write, recovery, scrub and zone failover for
    ``num_zones`` 2 (:ref:`Testing Strategy <ecsc-testing-strategy>`).
11. **Inter-zone statistics**: OSD perf counters of cross-zone operations, bytes and latency
    (:ref:`Inter-Zone Statistics <ecsc-inter-zone-statistics>`).
12. **Release gate**: the upgrade commit is ``ceph osd require-osd-release vampire`` and sets the
    OSDMap flag ``stretch_zones`` (:ref:`Release Gate <ecsc-release-gate>`).

.. _ecsc-later-release-recovery-bandwidth-optimization:

Later Release: Recovery Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce the inter-zone bandwidth of recovery, in priority order.

1. **Cost-based recovery planning**: compare the cross-zone transfer cost of each recovery strategy
   (:ref:`Cost-Based Recovery Planning <ecsc-cost-based-recovery-planning>`).
2. **Recovery Plan message and Zone Primary execution**: delegated recovery within a zone
   (:ref:`Delegated Remote-zone Recovery <ecsc-delegated-remote-zone-recovery>`).
3. **Push recovery fallback**: the Primary reconstructs the object and pushes it to the Zone Primary
   with shard-write instructions (:ref:`Remote-zone Fallback (Push Recovery)
   <ecsc-remote-zone-fallback-push-recovery>`).
4. **Cost-based choice of remote-zone reads**: choose by cost between the Primary reading
   remote-zone shards, which R1 always does when its zone lacks them, and delegating to a Zone
   Primary (:ref:`Primary Zone Fallback <ecsc-primary-zone-fallback>`).

.. _ecsc-later-release-write-bandwidth-optimization:

Later Release: Write Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce the inter-zone bandwidth of writes, in priority order.

1. **Replicate Transaction message**: send raw write data to the Zone Primary, which encodes locally
   (:ref:`Replicate Transaction (Preferred Path) <ecsc-replicate-transaction-preferred-path>`).
2. **Independent PG log generation at the Zone Primary**, with OBC population and the debug
   equivalence mode (:ref:`Independent Log Generation at the Zone Primary
   <ecsc-independent-log-generation-at-the-zone-primary>` and :ref:`Log Entry Compatibility and
   Upgrade Safety <ecsc-log-entry-compatibility-and-upgrade-safety>`).
3. **Client writes to the Zone Primary**, with the write-permission and completion messages
   (:ref:`Client Writes to the Zone Primary <ecsc-client-writes-to-the-zone-primary>`).
4. **Hybrid write path** for zones in different health states (:ref:`Hybrid Approach
   <ecsc-hybrid-approach>`).

.. _ecsc-later-release-additional-enhancements:

Later Release: Additional Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories improve resilience and flexibility, at a lower priority than the bandwidth work.

1. **Read from the Zone Primary**, with the Synchronous Recovery Set (:ref:`Read from the Zone
   Primary <ecsc-read-from-the-zone-primary>`).
2. **Direct read failures to the Zone Primary** instead of the Primary (:ref:`Read from the Zone
   Primary <ecsc-read-from-the-zone-primary>` and :ref:`Zone-Aware Read Enhancements
   <ecsc-zone-aware-read-enhancements>`).
3. **Online OSDs in offline zones** (:ref:`Online OSDs in Offline Zones
   <ecsc-online-osds-in-offline-zones>`).
4. **CRUSH changes with stretched pools**: require ``--yes-i-really-mean-it`` for bucket changes
   that change the number of zones (:ref:`The CRUSH Rule of a Pool <ecsc-the-rule-of-a-pool>`).
5. **Three-zone stretch mode**: lift the two-zone limits of pool creation, ``num_zones`` and
   stretch mode, and handle three zones in the Monitors' tiebreaker logic and the degraded,
   recovery and healthy transitions. ``num_zones`` 3 is intended to replace individual stretch
   pools (:ref:`Individual Stretch Pools <ecsc-individual-stretch-pools>`).
6. **Three-zone integration**: full integration and real-world testing of three-zone
   configurations.
7. **More inter-zone statistics**: per-pool statistics, latency percentiles and the local versus
   remote read ratio (:ref:`Inter-Zone Statistics <ecsc-inter-zone-statistics>`).

.. _ecsc-testing-strategy:

Testing Strategy
~~~~~~~~~~~~~~~~

1. **Unit tests**: two-zone and three-zone configurations, to validate the core peering and
   recovery logic for N zones.
2. **Two zones, R1**: full end-to-end testing of every read, write, recovery and failure
   scenario.
3. **Three zones, later release**: full integration and real-world testing, with the three-zone
   stretch mode story (:ref:`Later Release: Additional Enhancements
   <ecsc-later-release-additional-enhancements>`).


.. _ecsc-user-documentation:

User Documentation
------------------

The stretch mode user documentation (``doc/rados/operations/stretch-mode.rst``) is rewritten for
this design, from this outline:

1. **Introduction**: what a stretch cluster is, and why and when to use one.

   - An explanation of stretch mode.
   - The *Stretch Cluster Issues* section, reworked to explain why stretching a pool by hand is
     hard and stretch mode should be used instead.
   - Part of the *Limitations* section, for example that there are only two zones.
   - References to the asynchronous replication alternatives for block, file and object.
   - A note that stretch clusters were redesigned in Vampire.
   - A note that ``ceph osd pool stretch`` is not the same as a stretch cluster.

2. **Configuration prerequisites**: Monitors at three sites, including the tiebreaker Monitor,
   and the CRUSH zones.

3. **Creating pools**:

   - ``ceph osd pool create`` examples for replicated and erasure coded pools.
   - Mixing local and stretched pools.
   - What happens automatically when ``num_zones`` is 2: a suitable CRUSH rule over the zones is
     written (unless one is given), stretch mode is enabled, the tiebreaker Monitor is
     configured and the Monitors switch to the connectivity election strategy.

4. **Modifying pools**: changing ``num_zones`` and ``replica``, and changing ``min_size``, which
   applies per zone.

5. **Enabling and disabling stretch mode** (global stretch mode):

   - Example commands.
   - What they do: set the configuration options so that future pools are stretched by default,
     and change the existing pools.
   - Setting the pool creation defaults without changing existing pools (:ref:`ceph osd pool default
     set and get <ecsc-ceph-osd-pool-default-set-and-get>`).
   - For a mixture of pool types, setting ``num_zones`` to 2 on the existing pools and giving
     every parameter when creating new pools may be simpler and achieves the same thing.

6. **Other commands**: replacing a failed tiebreaker Monitor, forcing recovery stretch mode and
   forcing healthy stretch mode.

7. **CRUSH rules**:

   - The Monitors write the CRUSH rule when a stretched pool is created or its ``num_zones``
     changes, but a pool can be given its own rule instead. A rule that no pool uses any more is
     removed, whoever created it.
   - Examples of the generated rules, explaining how primaries are placed.
   - An example of an alternative rule that puts all primaries in the same zone.
   - Device classes: a rule that takes its zones with a device class works with stretch mode, also
     in degraded stretch mode (:ref:`Stretch Set Validation <ecsc-stretch-set-validation>`).

8. **Cross-zone performance counters** (:ref:`Inter-Zone Statistics <ecsc-inter-zone-statistics>`),
   named as ``perf dump`` shows them.

Other user documentation that mentions ``osd_pool_default_size`` changes with it
(:ref:`Configuration Options <ecsc-configuration-options>`): its reference entry
(``doc/rados/configuration/pool-pg-config-ref.rst``), ``doc/rados/operations/health-checks.rst`` and
``doc/rados/troubleshooting/troubleshooting-pg.rst``.
