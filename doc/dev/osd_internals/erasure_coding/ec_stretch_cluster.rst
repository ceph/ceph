========================================================
Design Document: Ceph Erasure Coded (EC) Stretch Cluster
========================================================

.. note::

   **Phased Delivery (R1, R2, …)**

   This document distinguishes between **R1** and **later release** work.
   Sections and sub-sections are annotated accordingly. R1 delivers:

   - **Zone-local direct reads** (good path only; any failure redirects to the
     Primary)
   - **Writes via Primary** (all write IO traverses the inter-zone link; no
     Zone Primary encoding)
   - **Recovery from Primary** (Primary reads all data and writes all remote-zone
     shards; no inter-zone bandwidth optimization)

   The labels R1, R2, etc. refer to **incremental PR delivery milestones** —
   they do *not* map one-to-one to Ceph named releases. Multiple delivery
   phases may land within a single Ceph release, or a single phase may span
   releases depending on development progress.

   The implementation order is detailed in Section 14.

.. important::

   **Scope and Applicability**

   This design is an **extension to Fast EC** (Fast Erasure Coding) only. We make
   **no attempt to support legacy EC** implementations with this feature. All
   functionality described in this document requires Fast EC to be enabled.


1. Intent & High-Level Summary
-------------------------------

The primary goal of this feature is to support a Replicated Erasure Coded (EC)
configuration within a multi-zone Ceph cluster (Stretch Cluster), building upon
the Fast EC infrastructure.

.. note::

   Throughout this document, a *zone* refers to a group of OSDs within a single
   CRUSH failure domain — typically a data center, but it could be any CRUSH
   bucket type (e.g., rack, room). The key characteristic of a zone boundary is
   that traffic crossing it incurs a significant performance penalty: higher
   latency and/or lower bandwidth compared to intra-zone communication.

   The term *inter-zone link* refers to the network connection between these
   failure domains (e.g., the dedicated link between data centers). This is
   typically the most bandwidth-constrained and latency-sensitive path in the
   cluster. There is a strong desire to minimize inter-zone link traffic, even
   to the extent of issuing additional zone-local reads to avoid sending data across
   this link.

Currently, Ceph Stretch clusters typically rely on Replica to ensure data
availability across zones (e.g., data centers). While Erasure Coding provides
storage efficiency, applying a standard EC profile naively across a stretch
cluster introduces significant operational problems.

Consider a 2-zone stretch cluster using a standard EC profile of ``k=4, m=6``.
While the high parity count provides sufficient fault tolerance to survive a
full zone loss, this configuration suffers from three fundamental
inefficiencies:

1. **Write Bandwidth Waste**: Every write must distribute all ``k+m`` chunks
   across both zones. The coding (parity) shards are unnecessarily duplicated
   over the inter-zone link, consuming bandwidth that scales linearly with the
   number of shards.
2. **Reads Must Cross Zones**: Serving a read requires retrieving at least ``k``
   chunks, which in general will span both zones. Every read operation incurs
   inter-zone link latency.
3. **Recovery is Inter-Zone Link Bound**: After a zone failure, recovering the
   lost chunks requires reading surviving chunks across the inter-zone link,
   placing the entire recovery burden on the most constrained network path.

This feature introduces a hybrid approach to eliminate these problems: creating
Erasure Coded stripes (defined by ``k + m``) and replicating those stripes
``zones`` times across a specified topology level (e.g., Data Center). Each zone
holds a complete, independent copy of the EC stripe, meaning reads and recovery
can be performed entirely within a single zone. This combines the zone-local storage
efficiency of EC with the zone-level redundancy of a stretch cluster, while
minimizing inter-zone link usage to write replication only.

2. Proposed Configuration (CLI) Changes
---------------------------------------

To support this topology, the EC Pool configuration interface will be expanded 
directly within the pool creation command.  We intend to address a number of
issues with the current CLI design:

* Create-then-set flow.  A typical setup procedure of a pool requires multiple CLI
  commands (e.g. create replica, then set num copies, then set stretched, etc... )
* Remove the need to create an EC profile before the pool: ``--k`` and ``--m`` generate one
  for the pool (Section 2.1.5). An existing profile can still be used, for multi-zone pools
  too; this stays supported.

The current CLI behaviour of accepting either positional or non-positional arguments
will be maintained, however no positional arguments will be added for the new
functionality.  

Supporting stretch EC pools will require changes to several CLI commands. The design 
document will focus on just the changes that will be made to the pool create CLI 
to give an idea of how the new CLI will work. Similar changes will be made to the 
CLIs that allow modification of a pool. Section 2.3 describes the pool creation
defaults and the changes to the CLIs that control stretch mode.



2.1 ceph osd pool create
~~~~~~~~~~~~~~~~~~~~~~~~

The ceph osd pool create command will be extended to become a parameterized command.

For backward compatibility, the positional arguments will be maintained and can be
specified along side the new paramaters. 

2.1.1 Full command syntax
^^^^^^^^^^^^^^^^^^^^^^^^^

The full command syntax is listed here. Refer to the later sections for pool-type specific details.

.. code-block:: text

   ceph osd pool create {pool_name}
        [--pg_num <pg_num>]
        [--pgp_num <pgp_num>]
        [--pool_type <replicated|erasure>]
        [--expected_num_objects <expected_num_objects>]

        # CRUSH Placement Options (Mutually Exclusive)
        [ --rule <crush_rule_name> |
          [--root <crush_root>] [--zone_failure_domain <zone_failure_domain>] [--osd_failure_domain <osd_failure_domain>] [--class <device_class>] ]

        # Replica-Specific Options
        [--size <size>]
        [--replica <replica>]

        # Erasure-Specific Options
        [--k <num_data_shards>]
        [--m <num_coding_shards>]
        [ --erasure_code_profile <profile_name> ]

        # Topology and Redundancy
        [--num_zones <num_zones>]

        # Autoscaling and General Config
        [--autoscale_mode <on|off|warn>]
        [--pg_num_min <pg_num_min>]
        [--pg_num_max <pg_num_max>]
        [--bulk]
        [--target_size_bytes <target_size_bytes>]
        [--target_size_ratio <target_size_ratio>]
        [--crimson]
        [--yes_i_really_mean_it]

2.1.2 Legacy positional syntax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following syntax, taken from the current docs, will be maintained for backward compatibility:

.. code-block:: text

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] [replicated] \
            [crush_rule_name] [expected_num_objects]

.. code-block:: text

   ceph osd pool create {pool_name} [{pg_num} [{pgp_num}]] erasure \
            [erasure_code_profile] [crush_rule_name] [expected_num_objects] [--autoscale_mode=<on,off,warn>]

This syntax can be used with the new parameters which do not directly conflict.  For example --pg_num cannot 
be used with its positional counterpart. 

2.1.3 Basic Parameters
^^^^^^^^^^^^^^^^^^^^^^

These are the primary parameters required for standard deployments.

**--pool_type** (or positional equivalent)
  - *Definition*: The type of pool to create.
  - *Values*: ``replicated`` or ``erasure``.
  - *Default Value*: Derived from ``osd_pool_default_type`` if omitted, but highly recommended to specify.

**--size**
  - *Definition*: The number of OSDs the data is striped over for each object. (Replica only)
  - *Note*: Parameter will be ignored, rather than rejected for EC pools, for backward compatibility.

**--k**
  - *Definition*: Within a zone, the number of OSDs the data is striped over for each object. (EC only)

**--m**
  - *Definition*: Within a zone, the number of OSDs the coding shards are striped over. (EC only)

**--num_zones**
  - *Definition*: For a stretched cluster configuration defines the number of zones, each which store a full replica of the pool. (EC or Replica)
  - *Default Value*: ``osd_pool_default_num_zones`` (``1``; Section 2.3)
  - *Behavior*: Setting this to >1 creates a stretched pool.
     A non-stretched pool achieves redundancy across OSDs.  A stretched pool creates redundancy
     across ``num_zones``.
  - *Pool Size*: For an EC pool, the resulting pool ``size`` is ``num_zones × (k + m)``. For a
    replicated pool created with ``num_zones`` greater than 1 and without ``--size``, it is
    ``num_zones × replica``, with ``--replica`` defaulting to ``osd_pool_default_replica``
    (Section 2.3.2).


2.1.4 Advanced Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^

These parameters are intended for advanced users and offer finer control over the cluster layout.
For a replicated pool, ``--root``, ``--zone_failure_domain``, ``--osd_failure_domain`` and
``--class`` take effect only with ``num_zones`` greater than 1 and are rejected otherwise
(Section 2.1.5).

**--zone_failure_domain**
  - *Definition*: The CRUSH bucket type over which zone-redundancy is achieved.
  - *Default Value*: ``osd_pool_default_zone_failure_domain`` (``datacenter``; Section 2.3.2)
  - *Purpose*: This is the CRUSH bucket type that defines a zone.
  - *Note*: Mutually exclusive with ``--rule``

**--osd_failure_domain**
  - *Definition*: The CRUSH bucket type over which OSD-redundancy is achieved.
  - *Default Value*: ``host``
  - *Note*: Mutually exclusive with ``--rule``

**--class**
  - *Definition*: Restrict placement to devices of a specific class (e.g., ``ssd`` or ``hdd``), using the CRUSH device class names in the CRUSH map.
  - *Purpose*: Only required if a cluster has a mixture of different classes of OSD.
  - *Note*: Mutually exclusive with ``--rule``

**--root**
  - *Definition*: The root of the CRUSH tree to use.  (R2 only)
  - *Default Value*: The cluster root (``default``).
  - *Topology and Validation*: Specifying the CRUSH root to use (defaults to ``default``), the CRUSH level for a zone (defaults to ``datacenter``), and the number of zones (defaults to ``1``) is sufficient to define the pool's placement:

    - Example 1: In a cluster with 2 datacenters, specifying ``--num_zones 2`` will create a stretch pool across the 2 datacenters.
    - Example 2: In a cluster with 2 datacenters, specifying ``--root DC1`` will create a pool completely contained in DC1.
    - Validation: If there are N datacenters with the same root and you specify a number of zones M != N, the command will fail because the specified number of zones is different from the number of zones in the CRUSH hierarchy.
    - Custom Rules: If users want to use a subset of zones (e.g., a special 3-datacenter configuration), they must specify a custom CRUSH rule. A custom CRUSH rule is mutually exclusive with specifying the CRUSH root and/or CRUSH level.

  - *Operational Restrictions*: When there are stretch pools, adding or moving a CRUSH bucket that impacts the number of zones for the pool will require a ``yes-i-really-mean-it`` flag, as this is liable to break things.
  - *Note*: Mutually exclusive with ``--rule``

**--rule**
  - *Definition*: Use this CRUSH rule, instead of an auto-generated rule.
  - *Purpose*: Create a bespoke CRUSH rule for advanced use cases not covered by the auto rule generation above.
  - *Note*: Mutually exclusive with ``--root``, ``--osd_failure_domain`` and ``--zone_failure_domain``

**--erasure_code_profile**
  - *Definition*: An existing EC profile to use, instead of one generated from ``--k`` and ``--m``.
  - *Note*: Can be used with any ``--num_zones``, multi-zone configurations included. This is
    permanent, not a transitional measure. A profile defines ``k``, ``m``, the plugin and its
    keys, and the CRUSH options (its ``crush-*`` keys), so the parameters it defines cannot be
    given again: ``--k``, ``--m``, ``--root``, ``--zone_failure_domain``,
    ``--osd_failure_domain`` and ``--class`` are refused with it rather than overriding the
    profile, and their defaults (Section 2.3.2) are not applied. See Section 2.1.5.

**--replica**
  - *Definition*: For replicated pools, the number of replicas within each zone. (Replica only)
  - *Default Value*: ``osd_pool_default_replica`` (3; Section 2.3.2)

.. note::

   The following parameters are existing, standard pool configuration options included here for completeness.

**--pg_num**
  - *Definition*: The total number of placement groups for the pool.

**--pgp_num**
  - *Definition*: The total number of placement groups for placement purposes.
  - *Note*: This should never be modified. Purely here for backward compatibility.

**--expected_num_objects**
  - *Definition*: The expected number of objects for this pool, used to pre-split placement groups at pool creation.

**--autoscale_mode**
  - *Definition*: The auto-scaling mode for placement groups.
  - *Values*: ``on``, ``off``, or ``warn``.

**--pg_num_min**
  - *Definition*: The minimum number of placement groups when auto-scaling is active.

**--pg_num_max**
  - *Definition*: The maximum number of placement groups when auto-scaling is active.

**--bulk**
  - *Definition*: Flags the pool as a "bulk" pool to pre-allocate more placement groups automatically.

**--target_size_bytes**
  - *Definition*: The expected total size of the pool in bytes, used to guide PG autoscaling.

**--target_size_ratio**
  - *Definition*: The expected ratio of the cluster's total capacity this pool will consume, used for PG autoscaling.

**--crimson**
  - *Definition*: Flags the pool to run on Crimson OSD.
  - *Note*: Crimson OSD is experimental.

**--yes_i_really_mean_it**
  - *Definition*: Internal safety override flag. In the context of pool creation, it is specifically used to allow the creation of hidden or system-reserved pools whose names begin with a dot (e.g., ``.rgw.root``).


2.1.5 Erasure Code Profiles
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Every erasure coded pool, stretched or not, has an EC profile. The pool records its name in
``erasure_code_profile``. The profile supplies the plugin, ``k``, ``m``, technique and the
``crush-*`` keys used to build the pool's CRUSH rule. Two things are
new: ``ceph osd pool create`` can generate the profile from ``--k`` and ``--m``, and a profile
is deleted with the last pool that uses it. The user documentation describes the same
behaviour in :ref:`erasure-code-profile-lifecycle`.

**How a pool gets its profile**

An erasure coded pool takes its profile from one of three sources:

* **Generated** (``--k`` and ``--m``): the profile ``<pool_name>-k<k>-m<m>``, created if it does
  not exist (see *Generated profiles* below).
* **Named** (``--erasure_code_profile <name>`` or the positional equivalent): an existing
  profile, normally created with ``ceph osd erasure-code-profile set``. The command does not
  create the profile. If it does not exist, the command fails with "erasure code profile
  '<name>' does not exist".
* **Default** (neither): the ``default`` profile. If ``default`` does not exist, for example
  after ``ceph osd erasure-code-profile rm default``, it is created from
  ``osd_pool_default_erasure_code_profile`` as written, without plugin normalization.
  ``--erasure_code_profile default`` has the same effect.

A profile from any of these sources can be used with ``--num_zones`` greater than 1, and this
stays supported: multi-zone pools do not have to be created from ``--k`` and ``--m``. The
profile's CRUSH keys and ``num_zones`` build the pool's rule (*CRUSH rule* below).

**Profile reuse** (planned): EC still needs a profile for every pool, but
``ceph osd pool create`` creates as few profiles as it can. A pool that does not name a profile
uses an existing profile whose contents equal what it needs (``--k``, ``--m`` and the CRUSH
options applied to ``osd_pool_default_erasure_code_profile``), whatever that profile is
called. A new profile is generated only when none matches. A pool that gives neither
``--k``/``--m`` nor a profile then follows the current ``osd_pool_default_erasure_code_profile``,
rather than the ``default`` profile that was created from it once. The rules below describe
today's behaviour.

**Parameter combinations**

A named profile defines ``k``, ``m`` and the CRUSH options, so none of them can be given with
``--erasure_code_profile``. For an erasure pool, ``ceph osd pool create`` rejects these with
EINVAL:

* ``--erasure_code_profile`` with ``--k`` or ``--m``: "cannot specify both
  erasure_code_profile and k/m parameters".
* Only one of ``--k`` and ``--m``: "erasure_code_profile requires both k and m".
* ``--erasure_code_profile`` with any of ``--root``, ``--zone_failure_domain``,
  ``--osd_failure_domain`` or ``--class``: "cannot specify both erasure_code_profile and crush
  parameters (crush_root, zone_failure_domain, osd_failure_domain, crush_device_class)".
* Any of those CRUSH options without ``--k``/``--m``, even with a value equal to the default:
  "crush parameters (crush_root, zone_failure_domain, osd_failure_domain, crush_device_class)
  require k and m". Without ``--k``/``--m`` the pool uses the shared ``default`` profile,
  which cannot record per-pool options.
* ``k`` less than 2: "k=<k> must be >= 2". ``k+m`` greater than 127: "(k+m)=<k+m> must be
  <= 127", because shard ids are 8-bit signed integers.

For any pool type:

* ``--rule`` with any of ``--root``, ``--zone_failure_domain``, ``--osd_failure_domain`` or
  ``--class``: "cannot specify both crush rule and crush parameters (crush_root,
  zone_failure_domain, osd_failure_domain, crush_device_class)".
* ``--num_zones`` less than 1: "num_zones must be >= 1".

For a replicated pool, ``--k`` or ``--m`` is rejected: "cannot specify k/m parameters for
replicated pools". A replicated pool with ``num_zones`` 1 uses the default replicated rule, or the
rule given with ``--rule``, so any of ``--root``, ``--zone_failure_domain``,
``--osd_failure_domain`` or ``--class`` is rejected for it rather than ignored: "crush parameters
(crush_root, zone_failure_domain, osd_failure_domain, crush_device_class) require num_zones > 1
for a replicated pool".

``--rule`` may be combined with ``--k``/``--m`` or with ``--erasure_code_profile``. ``m``
greater than ``k`` is allowed. The plugin can reject further values when it normalizes the
profile (for example, ISA rejects ``m`` greater than 32). ``m`` at least 1 is enforced only
by the ``ceph`` CLI.

The existing-pool check runs first: if a pool of that name and type already exists, the
command succeeds with "pool '<pool>' already exists", whatever profile options are given.

**Generated profiles**

With ``--k`` and ``--m`` the monitor looks for a profile named ``<pool_name>-k<k>-m<m>`` in the
committed OSDMap.

* **Not found**: the monitor copies every key of ``osd_pool_default_erasure_code_profile``
  (default ``plugin=isa technique=reed_sol_van k=2 m=2``), sets ``k`` and ``m``, and records
  the CRUSH options given on the command line:

  - ``--root`` as ``crush-root``
  - ``--zone_failure_domain`` as ``crush-zone-failure-domain``
  - ``--osd_failure_domain`` as ``crush-osd-failure-domain`` if the default profile has that
    key, otherwise as ``crush-failure-domain``
  - ``--class`` as ``crush-device-class``

  The plugin then normalizes the profile. This fills in unset keys (``crush-root=default``,
  ``crush-zone-failure-domain=datacenter``, ``crush-failure-domain=host`` and so on) and can
  rewrite others (ISA switches ``reed_sol_van`` to ``cauchy`` for some ``k`` and ``m``). If
  normalization fails, the command fails and nothing is proposed.
  The profile is committed in its own OSDMap update and the command is retried. The CRUSH rule
  and then the pool follow in later updates.
* **Found, same** ``k`` **and** ``m``: the profile is used as it is. Only the ``k`` and ``m``
  values are compared, so a profile created in advance with
  ``ceph osd erasure-code-profile set`` keeps its own plugin, technique and ``crush-*`` keys.
* **Found, different** ``k`` **or** ``m``: the command fails with EEXIST, "EC profile
  '<pool_name>-k<k>-m<m>' already exists with different k/m parameters".

Each CRUSH option given on the command line must then equal the matching profile key
(``crush-osd-failure-domain``, else ``crush-failure-domain``, for ``--osd_failure_domain``). A
missing key counts as different. Otherwise the command fails with EINVAL, "EC profile
'<profile>' already exists with different crush parameters than specified
(crush_root/zone_failure_domain/osd_failure_domain/device_class)". A newly generated profile
always matches, so this only fails for a reused profile.

No command-line option sets the plugin, technique or other plugin keys of a generated
profile. They come from ``osd_pool_default_erasure_code_profile``, or from a profile
of the generated name created in advance. ``num_zones`` is not stored in the profile.

Pool names may contain characters that ``ceph osd erasure-code-profile get`` and ``rm`` do not
accept from the ``ceph`` CLI (it allows only ``[A-Za-z0-9-_.]``). The generated profile of such
a pool can then only be removed by deleting the pool.

**How the profile shapes the pool**

At creation the monitor loads the plugin named by the profile and sets:

* ``size`` to ``num_zones`` × the plugin's chunk count (``k+m`` for jerasure and ISA).
  ``--size`` is ignored for EC pools.
* ``min_size`` to ``k + min(1, m-1)``, not scaled by ``num_zones``: it applies to each zone's
  block of ``k+m`` shards (Section 11.2).
* ``ec_data_shard_count`` and ``ec_coding_shard_count`` to ``k`` and ``m``.
* ``nonprimary_shards``, when FastEC is enabled: raw shards ``1`` to ``k-1`` (through the
  plugin's chunk mapping) in every zone, that is shard ``s + (k+m) × zone``. For ``k=4``,
  ``m=2`` and two zones this is ``{1,2,3,7,8,9}``.

**CRUSH rule**

Without ``--rule`` the rule is named after the pool. Single-zone pools with the ``default``
profile are the exception: they share the ``erasure-code`` rule. If a committed rule of that name already exists it is reused, without checking that it
fits ``k+m``, ``num_zones`` or the CRUSH options. Otherwise the plugin builds it:

* ``num_zones`` greater than 1: a stretch rule that takes ``num_zones`` buckets of the zone type
  under the root (``choose firstn <num_zones>``), then ``k+m`` OSD failure domains
  (``chooseleaf_indep``) in each. The root, zone type,
  OSD failure domain type and device class are the command-line values, else the profile's
  ``crush-root``, ``crush-zone-failure-domain``, ``crush-osd-failure-domain`` (or
  ``crush-failure-domain``) and ``crush-device-class``. ``crush-num-failure-domains``,
  ``crush-num-osd-failure-domains`` and ``crush-osds-per-failure-domain`` are ignored. The root must have exactly ``num_zones``
  buckets of the zone type, each with at least ``k+m`` OSD failure domains that contain an OSD;
  otherwise the command fails, for example with "number of zones <n> for type <type> is not
  equal to num_failure_domains <num_zones>". LRC rejects ``num_zones`` greater than 1.
* ``num_zones`` equal to 1: the rule is built from the profile's keys only, as before. The
  command-line values take effect because the generated profile records them.
  ``--zone_failure_domain`` has no effect on such a rule.

With ``--rule`` the named rule must already exist ("specified rule <rule> doesn't exist") and is
used unchanged: ``num_zones`` and the profile's CRUSH keys are not applied to it.
``ceph osd crush rule create-erasure <name> [<profile>] [<num_zones>]`` builds the same kind of
rule from the profile's keys alone.

**Multi-zone pools**

* An erasure pool with ``num_zones`` greater than 1 must have FastEC. Creation enables
  ``allow_ec_optimizations`` and fails if that fails, with "Multi-zone erasure coded pools
  require FastEC support. The erasure code profile '<profile>' does not support FastEC:
  <reason> Please use a FastEC-compatible profile (e.g., plugin=jerasure
  technique=reed_sol_van, or plugin=isa)." The reason is one of: ``require_osd_release`` older
  than tentacle; a plugin without FastEC support (ISA supports it with any technique and
  jerasure only with ``reed_sol_van``; shec, clay and LRC do not); a plugin whose FastEC support
  is marked experimental.
* A multi-zone pool is created from ``--k``/``--m`` or from a profile. With ``--k``/``--m`` its
  plugin and technique come from ``osd_pool_default_erasure_code_profile``, or from a profile of
  the generated name created in advance. With neither ``--k``/``--m`` nor
  ``--erasure_code_profile``, it uses the ``default`` profile and, unlike a single-zone pool, a
  rule of its own named after the pool.
* This FastEC check at creation is the only release check today. Section 15 gates
  ``num_zones`` greater than 1 on ``require_osd_release``, which is not implemented yet.

**num_zones is set per pool**

``num_zones`` is set on every pool, at creation with ``--num_zones`` (default
``osd_pool_default_num_zones``, Section 2.3.2), and is never part of an erasure code profile.
``ceph osd pool get <pool> num_zones`` shows it, and ``ceph osd pool set <pool> num_zones <n>``
changes it (Section 13.2). A ``num_zones`` key set in a profile with
``ceph osd erasure-code-profile set`` is ignored.

**Changes after creation**

* ``ceph osd pool set <pool> size`` is refused for EC pools ("can not change the size of an
  erasure-coded pool"). An EC pool's ``size`` changes only with its ``num_zones`` (Section
  13.2), which ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode`` also change
  (Section 2.3.4).
* ``ceph osd pool stretch set`` is refused for EC pools, and so is ``ceph osd pool stretch
  unset`` for an EC pool with ``num_zones > 1``: an EC pool is stretched and unstretched only
  with ``num_zones`` (Section 13.2). Both are also refused while stretch mode is enabled
  (Section 11.4.1).
* A pool's profile cannot be replaced. ``erasure_code_profile`` can be read with
  ``ceph osd pool get`` but is not a ``ceph osd pool set`` variable.
* ``ceph osd erasure-code-profile set <name> ... --force --yes-i-really-mean-it`` overwrites a
  profile even if pools use it. The pools are not updated: ``size``, ``min_size``, the shard
  counts and ``nonprimary_shards`` keep their values, while OSDs
  build a PG's EC backend from the profile in the OSDMap they load, so this is unsafe.
* ``ceph osd pool rename`` does not rename a generated profile or the CRUSH rule named after
  the pool. A pool created later with the old name and the same ``k`` and ``m`` reuses both,
  which are then shared by the two pools.

**Lifetime**

When an erasure coded pool is deleted, with ``ceph osd pool delete`` or through librados, the
monitor deletes its profile in the same OSDMap update as the pool, unless:

* the profile is ``default``;
* another erasure coded pool in the committed OSDMap uses it; or
* a pool in the pending OSDMap update uses it. The pending update holds pools being created and
  pending changes to existing pools.

This applies to every profile, whether ``ceph osd pool create`` generated it or it was created
with ``ceph osd erasure-code-profile set``. Only erasure coded pools count as users. The command
output does not mention the deletion; the monitor logs the decision at ``debug_mon`` 10.

The pool's CRUSH rule is removed in the same update if no other pool in the committed OSDMap
uses it, as it was before this change. The two decisions are independent: the rule check counts
pools of any type but not pending pools, while the profile check counts only erasure coded pools
but includes pending ones. One can be removed while the other is kept.

Only a pool deletion removes a profile automatically. A profile stays when:

* no pool has ever used it, for example a profile that was set but never used;
* ``mon_fake_pool_delete`` is set and the deletion is faked: the pool is only renamed to
  ``<name>.<id>.DELETED``, and the profile and the rule both stay;
* ``ceph osd pool create`` fails after the generated profile was committed, for example on a
  CRUSH topology error, a ``--rule`` that does not exist, stretch mode validation, the FastEC
  check or the PG limit. The profile stays, and so does the rule named after the pool if the failure came
  after the rule was created. A retry with the same ``k``, ``m`` and CRUSH options reuses both.
  A retry with different CRUSH options fails until the profile is removed. A retry with a
  different ``k`` or ``m`` generates a new profile but reuses the leftover rule without checking
  it. Remove them with ``ceph osd erasure-code-profile rm`` and ``ceph osd crush rule rm``.

OSDs store a deleted pool's profile with its final ``pg_pool_t``, so deleting the profile in the
same epoch as the pool does not affect PG removal.

**Profile commands**

* ``ceph osd erasure-code-profile set`` is unchanged. Keys are merged onto
  ``osd_pool_default_erasure_code_profile`` (a different ``plugin`` discards the default keys),
  ``crush-failure-domain`` must name an existing CRUSH type, and the plugin normalizes the
  profile. ``crush-osd-failure-domain`` and ``crush-zone-failure-domain`` are not checked here;
  a bad value fails at rule creation with "unknown type <type>" if a new rule uses the key, and
  is never reported otherwise. Setting an existing
  profile to identical contents succeeds without a change. Different contents need ``--force``
  and ``--yes-i-really-mean-it``. Normalization now also adds ``crush-zone-failure-domain``
  (default ``datacenter``), so ``ceph osd erasure-code-profile get`` shows that key.
* ``ceph osd erasure-code-profile rm`` waits while a pool in the pending update uses the profile,
  and fails with EBUSY, "<pool> pool(s) are using the erasure code profile '<name>'", while a
  committed erasure coded pool uses it. Removing a profile that does not exist succeeds (return
  code 0) with "erasure-code-profile <name> does not exist". ``default`` can be removed when no
  pool uses it; ``ceph osd pool create`` and ``ceph osd crush rule create-erasure`` re-create it
  when needed.
* ``ceph osd erasure-code-profile get`` and ``ls`` show only committed profiles. ``get`` of a
  profile deleted with its last pool fails with ENOENT, "unknown erasure code profile '<name>'".

**Change of behaviour**

In earlier releases a profile created with ``ceph osd erasure-code-profile set`` remained until
``ceph osd erasure-code-profile rm`` removed it, and could be used again for later pools. Now it
is deleted with the last pool that uses it. Creating a pool with a deleted profile fails with
"erasure code profile '<name>' does not exist". Scripts and tests that set a profile once and reuse it after deleting
its pools must set it again before each reuse. Scripts that run
``ceph osd erasure-code-profile rm`` after deleting the pool keep working, because removing a
missing profile succeeds, but the ``rm`` no longer does anything.

2.2 Examples
~~~~~~~~~~~~

The following are examples of how the new parameterized ``ceph osd pool create`` command simplifies pool creation across different topologies.

**Example 1: Basic Replicated Pool**
Create a standard 3-way replicated pool containing 128 placement groups:

.. code-block:: bash

   ceph osd pool create my_rep_pool --pool_type replicated --size 3 --pg_num 128

**Example 2: Standard Erasure Coded Pool**
Create an erasure-coded pool using a ``k=4, m=2`` configuration (yielding a size of 6 shards) with 64 placement groups:

.. code-block:: bash

   ceph osd pool create my_ec_pool --pool_type erasure --k 4 --m 2 --pg_num 64

**Example 3: Stretched Replicated Pool**
Create a replicated pool that spans across two datacenters, achieving a total size of 4 (2 replicas in each datacenter):

.. code-block:: bash

   ceph osd pool create stretch_rep --pool_type replicated --size 4 --zone_failure_domain datacenter --num_zones 2

**Example 4: Stretched Erasure Coded Pool**
Create an erasure-coded pool stretched across two racks. Using a ``k=4, m=2`` configuration per zone across 2 zones creates a total pool size of 12 shards (4 data and 2 coding per rack):

.. code-block:: bash

   ceph osd pool create stretch_ec --pool_type erasure --k 4 --m 2 --zone_failure_domain rack --num_zones 2

**Example 5: Single Datacenter Erasure Coded Pool**
Create an EC pool confined entirely to a specific datacenter using the ``--root`` parameter:

.. code-block:: bash

   ceph osd pool create dc1_ec --pool_type erasure --k 4 --m 2 --root DC1

**Example 6: Bulk Erasure Coded Pool with Autoscaling Limits**
Create an EC pool where the system automatically scales the PG count but enforces a minimum boundary, marking it as a bulk pool:

.. code-block:: bash

   ceph osd pool create bulk_ec --pool_type erasure --k 6 --m 3 --autoscale_mode on --pg_num_min 128 --bulk

2.3 Pool Creation Defaults and Global Stretch Mode Commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Global stretch mode is refactored. ``ceph mon enable_stretch_mode`` and
``ceph mon disable_stretch_mode`` stay. Instead of switching a separate mode with its own
flag, options and pool restrictions, they change the pool creation defaults and the existing
pools. Whether a pool is stretched is decided only by its own
``num_zones``, and every ``ceph osd pool create`` parameter takes its default from a
configuration option. As a result:

* ``enable_stretch_mode`` and ``disable_stretch_mode`` work with replicated and EC pools,
  including metadata pools such as ``.mgr``;
* clusters that use global stretch mode can upgrade and then create local pools, stretched EC
  pools or both;
* a preferred pool configuration can be set in advance, so creating a pool needs only its name.

2.3.1 Global Stretch Mode Before the Refactor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Only replicated pools with two zones: EC pool creation is refused and every new pool must
  have ``num_zones`` 2. On main, enabling also refuses existing EC pools. stretchy-C accepts
  them, but only if the one rule passed is an EC rule, so a cluster with both replicated and EC
  pools cannot be enabled.
* Pool settings are fixed. The replica count is ``mon_global_stretch_pool_replica``, and the
  zone failure domain is the stretch bucket type. ``--size``, or a ``--replica`` or
  ``--min_size`` other than the global value, is refused at creation, and so are later changes
  to ``size``, ``min_size``, ``replica`` or ``num_zones`` and ``ceph osd pool stretch unset``.
* Enabling gives every existing pool the CRUSH rule passed to the command. A new pool without
  ``--rule`` gets a generated rule on stretchy-C. On main it takes the rule that most stretch
  pools use, so once they are all deleted pool creation fails with "No suitable CRUSH rule
  exists".
* ``global_stretch_mode_enabled`` in the MonMap records the mode. Earlier releases record it as
  ``stretch_mode_enabled`` in the MonMap and the OSDMap.
* ``ceph mon disable_stretch_mode`` resets every pool, EC pools included, to
  ``osd_pool_default_size`` and to the given rule, else the default replicated rule.
* Halving a replicated pool's ``min_size`` in degraded stretch mode, and restoring it from
  ``mon_stretch_pool_min_size``, was removed when ``min_size`` became per zone (Section 11.2).

2.3.2 Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Every ``ceph osd pool create`` parameter takes its default from a generic
``osd_pool_default_*`` option. These stretch-specific options are dropped or renamed:

.. list-table::
   :header-rows: 1

   * - Option
     - Change
   * - ``osd_pool_stretch_default_replica``
     - Dropped
   * - ``mon_global_stretch_pool_replica``
     - Dropped
   * - ``mon_stretch_pool_size``
     - Dropped
   * - ``mon_stretch_pool_min_size``
     - Dropped
   * - ``default_crush_zone_failure_domain``
     - Renamed to ``osd_pool_default_zone_failure_domain``
   * - ``osd_pool_default_size``
     - Legacy: still accepted, as the old name of ``osd_pool_default_replica``

Notes:

* ``osd_pool_default_replica`` (3) is the number of copies per zone, the default of
  ``--replica``. It replaces ``osd_pool_default_size``, which becomes its legacy name, and the
  dropped replica and size options. A replicated pool's ``size`` is ``num_zones`` × replicas,
  and a pool still reports that total: with ``osd_pool_default_replica`` 3 and ``num_zones`` 2,
  ``ceph osd pool get <pool> size`` reports 6. For a single-zone pool the replica count is the
  ``size``, as before. Without ``ceph mon enable_stretch_mode`` (Section 2.3.4), a two-zone
  replicated pool therefore defaults to 3 replicas per zone (``size`` 6) instead of 2.
* Giving the old option a per-zone meaning is safe for existing stretch clusters. On main,
  stretch mode does not use ``osd_pool_default_size`` while it is enabled, because pools get
  ``mon_stretch_pool_size`` instead. It is read only on enabling (pools must start at it) and
  disabling (pools return to it).
* The user documentation that mentions ``osd_pool_default_size`` needs updating to match. That
  covers its reference entry (``doc/rados/configuration/pool-pg-config-ref.rst``) and pages
  such as ``doc/rados/operations/stretch-mode.rst``, ``doc/rados/operations/health-checks.rst``
  and ``doc/rados/troubleshooting/troubleshooting-pg.rst``.
* ``osd_pool_default_min_size`` gives ``min_size`` per zone (Section 11.2) and replaces
  ``mon_stretch_pool_min_size``.
* Global stretch mode no longer overrides ``osd_pool_default_num_zones`` (1) with 2, or
  ``osd_pool_default_zone_failure_domain`` (``datacenter``) with the stretch bucket type.
* New options ``osd_pool_default_osd_failure_domain`` (``host``), ``osd_pool_default_root``
  (``default``) and ``osd_pool_default_class`` (any class) replace defaults that are built in
  today. EC pools take the root and OSD failure domain from their profile (Section 2.1.5).
* ``osd_pool_default_crush_rule`` (set with ``--rule``, Section 2.3.3) is used by a pool that
  gives no ``--rule``, if the rule suits the pool's type. Otherwise the pool gets a generated
  rule. Today it is used only by single-zone replicated pools.
* Code that reads ``osd_pool_default_size`` as a pool's total size must change to
  ``num_zones`` × ``osd_pool_default_replica``: the ``TOO_FEW_OSDS`` health check (``PGMap``), the
  mgr's check for enough OSDs (``mgr_module.py``), the rook module's replica count and
  ``OSDMap::build_simple``.
* ``ceph osd pool set <pool> num_zones 2`` uses the same defaults (Section 13.2).
* What an upgrade does with the dropped options is in Section 2.3.5.

2.3.3 Setting the Defaults
^^^^^^^^^^^^^^^^^^^^^^^^^^

``ceph osd pool default`` sets and shows the pool creation defaults of Section 2.3.2. It
never changes an existing pool. Every value always has a default, and the defaults are kept
consistent with each other, so there is nothing to clear: ``set`` replaces a value, including
with its built-in value.

.. code-block:: text

   ceph osd pool default set
        [--pool_type <replicated|erasure>]
        [--num_zones <num_zones>]
        [--rule <rule> |
         [--zone_failure_domain <type>] [--osd_failure_domain <type>]
         [--root <crush_root>] [--class <device_class>]]
        [--replica <replica> | --size <size>] [--min_size <min_size>]
        [--erasure_code_profile <profile> | [--k <k>] [--m <m>]]
        [--pg_num <pg_num>] [--pgp_num <pgp_num>]
        [--autoscale_mode <on|off|warn>] [--bulk <true|false>] [--crimson <true|false>]

   ceph osd pool default get

* ``set`` writes each given parameter to ``osd_pool_default_<parameter>`` in the ``global``
  section of the configuration database, with these exceptions: ``--pool_type`` sets
  ``osd_pool_default_type``, ``--size`` sets ``osd_pool_default_replica`` (see below),
  ``--autoscale_mode`` sets ``osd_pool_default_pg_autoscale_mode``, ``--bulk`` sets
  ``osd_pool_default_flag_bulk``, ``--rule`` sets ``osd_pool_default_crush_rule``, ``--k``
  and ``--m`` set ``k`` and ``m`` in
  ``osd_pool_default_erasure_code_profile``, and ``--erasure_code_profile`` copies the keys of
  an existing profile into ``osd_pool_default_erasure_code_profile``. That option is kept as
  today; because it holds a copy, the default does not depend on the named profile still
  existing. As with ``ceph osd pool create``, ``--erasure_code_profile`` is refused together
  with ``--k``, ``--m`` or a CRUSH option the profile defines. The three write the same option,
  so the stored defaults cannot disagree. ``--rule`` is likewise refused together with
  ``--zone_failure_domain``, ``--osd_failure_domain``, ``--root`` or ``--class``. While a
  default rule is set, pools that use it ignore the CRUSH option defaults. ``--rule none``
  returns to generated rules.
* ``set`` makes the checks that ``ceph osd pool create`` would make with the resulting
  defaults, for a pool of the default type and for each pool type whose own parameters it
  is given, so a default that would make pool creation fail is refused. For example,
  ``--num_zones`` must be at least 1, and 2 needs a cluster that can be stretched: exactly
  two buckets of the zone failure domain type, monitors in both and one tiebreaker monitor
  outside them (Section 11.1). ``--zone_failure_domain`` must be an existing CRUSH type.
  ``--replica 1`` or ``--size 1`` needs ``mon_allow_pool_size_one`` and
  ``--yes-i-really-mean-it``.
  ``--crimson`` needs crimson to be allowed on the cluster (``ceph osd set-allow-crimson``).
  A crimson pool's autoscale mode is ``off`` unless one is given. Boolean parameters are given
  as ``--bulk`` or ``--bulk=false``.
* ``set`` also fails if a value it writes would not take effect on the monitors, because a
  ``mon``-section value in the configuration database or a monitor's local configuration file
  overrides it. If such an override appears later, a health warning reports it (name to be
  decided, for example ``POOL_DEFAULT_OVERRIDDEN``).
* If any check fails, nothing is written. Parameters that are not given are left as they are.
* ``get`` shows every default that ``ceph osd pool create`` would use now, and where each
  value comes from, with ``-f json`` for machine-readable output. Ceph's ``get`` commands are
  not consistent. ``ceph osd pool get`` needs a variable or ``all``, ``ceph config get`` takes
  an optional key, and ``ceph fs get`` and ``ceph osd erasure-code-profile get`` show
  everything. This command shows everything.

``--size`` is a legacy parameter, kept because scripts and other projects use ``--size`` and
``osd_pool_default_size``. It sets ``osd_pool_default_replica``, whose legacy name is
``osd_pool_default_size`` (Section 2.3.2), so it means the total number of copies only for a
single-zone pool. ``--size`` is therefore refused together with ``--replica``, and with a
``num_zones``, given or default, greater than 1.

``set`` leaves out these ``ceph osd pool create`` parameters:

* ``<pool_name>``, ``--expected_num_objects``, ``--pg_num_min``, ``--pg_num_max``,
  ``--target_size_bytes`` and ``--target_size_ratio`` describe one pool: its name, its
  expected content and its autoscaler bounds. They are not cluster-wide preferences.
* ``--force_pg_limit`` and ``--yes_i_really_mean_it`` override a check for one command. They
  are not settings.

Setting ``--num_zones 1`` makes future pools single-zone and leaves the ``num_zones`` of
existing pools alone. ``set`` never enables or disables stretch mode, which follows only from
the pools (Section 2.3.4).

**How the defaults are used.** ``ceph osd pool create`` takes every parameter it is not given
from these defaults. Every default has a sensible built-in value (3 replicas, one zone, the
``k`` and ``m`` of ``osd_pool_default_erasure_code_profile``, and so on), so a cluster needs
no ``set`` before its first pool create. Users can still change the options directly with
``ceph config set`` and ``ceph config rm``, which skips the checks of ``set``; this is not
blocked. Pool create therefore checks the defaults it uses as it checks given parameters. It
refuses an inconsistent combination with an error that names the default options at fault.

Pool create and ``set`` build and check the parameters in the same four steps: load the
defaults; use the erasure code profile, the named one or the default; apply the command line;
check the values that result, using the cluster where a check needs it (CRUSH types, roots and
classes, plugins, whether the cluster can be stretched). Which parameters may be given
together is checked on the command line itself, and differs only where pool create describes
one pool and ``set`` the defaults of every pool: for example ``--k`` alone changes the default
profile, but a pool needs both ``--k`` and ``--m``. A ``min_size`` default is not checked,
because pool create limits it to the pool's size.

.. note::

   **Review required** for the shape of ``ceph osd pool default``:

   * Ceph ``set`` commands usually take one variable and value (``ceph osd pool set <pool>
     <var> <val>``, ``ceph config set <who> <name> <value>``) or ``key=value`` pairs
     (``ceph osd erasure-code-profile set``). This command takes the flags of
     ``ceph osd pool create`` instead, a departure from Ceph convention.
   * The parameter names follow ``ceph osd pool create`` (``--replica``, ``--rule``,
     ``--autoscale_mode``) rather than ``ceph osd pool set`` (``size``, ``crush_rule``,
     ``pg_autoscale_mode``). This choice may be reversed.
   * The command duplicates ``ceph config set`` and ``get`` for these options. Its value is
     the self-consistency checks and the pool create syntax.

2.3.4 Enabling and Disabling Stretch Mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ceph mon enable_stretch_mode [<tiebreaker_mon>] <new_crush_rule> <dividing_bucket>`` and
``ceph mon disable_stretch_mode [<crush_rule>] [--yes-i-really-mean-it]`` keep their command
syntax. They change every existing pool, metadata pools such as ``.mgr`` included.
``enable_stretch_mode`` sets a replicated pool to ``num_zones`` 2 and 2 replicas per zone, and
an EC pool to ``num_zones`` 2 with no other configuration changed. ``disable_stretch_mode``
sets every pool to ``num_zones`` 1, and a replicated pool to 3 replicas: it sets
``osd_pool_default_replica`` to 3, the built-in default, whatever it was before
``enable_stretch_mode``. Each pool is changed
as ``ceph osd pool set <pool> num_zones`` changes it (Section 13.2), which also enables or
disables stretch mode (Section 11.4.2). The replica counts come from ``osd_pool_default_replica``,
which each command sets first (2 on enable, 3 on disable; table below). A legacy EC pool can
never be stretched, so ``enable_stretch_mode`` refuses to run, and changes nothing, if any
legacy EC pool exists. Converting such a pool to FastEC is a manual step (Section 13.2). As
on main, ``disable_stretch_mode`` is refused in recovery stretch mode. Run
``ceph osd force_healthy_stretch_mode`` first.

Nothing has to be prepared with ``ceph osd pool default`` before ``enable_stretch_mode``.
Every default has a sensible built-in value (Section 2.3.3). ``enable_stretch_mode`` changes
only the stretch defaults in the table below: replicas per zone to 2, ``num_zones`` to 2 and
the zone failure domain to ``dividing_bucket``. Every other default keeps its value. It
stretches the existing EC pools too, with rules generated for them; the only prerequisite for
an EC pool is FastEC. The default pool configuration that results must pass the same checks
as ``ceph osd pool default set``, so that the next ``ceph osd pool create`` works. If it
would not pass, ``enable_stretch_mode`` is refused and changes nothing.

Differences from ``enable_stretch_mode`` and ``disable_stretch_mode`` on main:

* ``enable_stretch_mode`` can be run again. Main refuses it while stretch mode is enabled
  ("stretch mode is already engaged"). It now sets the defaults again and stretches the pools
  that are not stretched yet.
* ``enable_stretch_mode`` accepts replicated pools of any size. Main refuses unless every
  replicated pool has the default ``size`` and ``min_size``. Every replicated pool now ends
  with 2 replicas per zone: a local pool of size 4 or 1 becomes 2 × 2, and a two-zone pool
  with 3 replicas per zone drops to 2.
* ``enable_stretch_mode`` stretches EC pools, which main refuses. This is intended: each EC
  pool's raw usage doubles (``num_zones × (k + m)`` shards), and every changed pool
  backfills.
* While the defaults are stretched, a replicated pool created with ``--num_zones 1`` also
  gets ``osd_pool_default_replica`` (2) replicas, where it got 3 before.

They also change these pool creation defaults:

.. list-table::
   :header-rows: 1

   * -
     - ``enable_stretch_mode``
     - ``disable_stretch_mode``
   * - ``osd_pool_default_num_zones``
     - 2
     - 1
   * - ``osd_pool_default_replica`` (replicas per zone)
     - 2
     - 3
   * - ``osd_pool_default_zone_failure_domain``
     - The ``dividing_bucket`` argument
     - Unchanged

.. warning::

   **Major open issue**: one replica default serves both stretched and local pools. Setting
   ``osd_pool_default_replica`` to 2 gives local pools 2 replicas while stretch mode is
   enabled. Separate defaults for stretched and local pools may be needed after all (Section
   2.3.6).

The arguments are used as follows:

* ``dividing_bucket`` is the CRUSH bucket type that splits the cluster into its two zones, for
  example ``datacenter``. It becomes the default zone failure domain.
* ``tiebreaker_mon`` is a legacy argument. Without it, the monitors pick the one monitor
  outside both zones (Section 11.4.2). It is still accepted, and used as today, so that
  existing callers keep working and a cluster with more than one monitor outside the zones
  can still name its tiebreaker.
* ``new_crush_rule`` is given to every replicated pool that ``enable_stretch_mode`` changes,
  and ``crush_rule``, if given, to every replicated pool that ``disable_stretch_mode`` changes,
  as ``--crush_rule`` would be (Section 13.2); see the warning below. They are checked as
  today: ``new_crush_rule`` must be a replicated rule with a ``take`` step, stretched across
  the ``dividing_bucket`` type and covering exactly two zones, and ``crush_rule`` must be a
  replicated rule that differs from each pool's current rule. EC pools get their generated
  rules. ``ceph osd pool set <pool> crush_rule`` changes an EC pool's rule afterwards.

.. warning::

   **Review required**: ``new_crush_rule`` and ``crush_rule`` apply to replicated pools only.
   Today ``enable_stretch_mode`` gives the rule to every pool, and ``disable_stretch_mode``
   gives every pool its ``crush_rule`` or the default replicated rule. After this change EC
   pools ignore these arguments and get their own rules.

``enable_stretch_mode`` and ``disable_stretch_mode`` keep no state. Afterwards each pool can be
changed on its own, and local and stretched pools, replicated or EC, can be mixed. For a
mixture, setting ``num_zones`` to 2 on the pools that need it and giving every parameter when
creating new pools achieves the same without changing the defaults.

2.3.5 Upgrade
^^^^^^^^^^^^^

Section 15 describes the upgrade and its commit, when ``require_osd_release`` is raised. At the
commit the pool creation defaults that were in effect are kept:

* Released versions record global stretch mode as ``stretch_mode_enabled`` in the MonMap and
  the OSDMap; it is not a configuration option. If it is enabled, ``osd_pool_default_num_zones``
  is set to 2 and ``osd_pool_default_zone_failure_domain`` to the type of the OSDMap's
  ``stretch_mode_bucket``. Stretch mode itself stays enabled.
* In global stretch mode, half of ``mon_stretch_pool_size`` (2 by default) goes to
  ``osd_pool_default_replica``. ``mon_stretch_pool_min_size`` is ignored, since ``min_size``
  defaults per zone.
* ``osd_pool_stretch_default_replica``, ``mon_global_stretch_pool_replica`` and
  ``default_crush_zone_failure_domain`` exist only on stretchy-C. They were never released,
  so there is nothing to convert.
* After the conversion, the readers of ``osd_pool_default_size`` listed in Section 2.3.2 see
  the per-zone value. ``TOO_FEW_OSDS`` only moves a health warning, the mgr's check matters
  only before ``.mgr`` exists, and ``OSDMap::build_simple`` runs only when a cluster is
  created. The rook module is the one functional risk: it passes ``osd_pool_default_size`` to
  Rook as a pool's replica count, so it must change in the same release.

2.3.6 Open Questions
^^^^^^^^^^^^^^^^^^^^

* **One replica default for stretched and local pools** (major). ``osd_pool_default_replica``
  counts replicas per zone for every pool, so the stretched value 2 also applies to local
  pools while stretch mode is enabled. Separate defaults for stretched and local pools would
  avoid this, at the cost of a stretch-specific option.


1. Approaches Considered but Rejected
-------------------------------------

We evaluated and rejected the following two alternative designs:

3.1 Multi-layered Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved re-using the existing ``ReplicationBackend`` to manage
the top-level replication, with EC acting as a secondary layer.

- *Reason for Rejection*: This would require complex, multi-layered peering
  logic where the replication layer interacts with the EC layer. Ensuring the
  correctness of these interactions is difficult. Additionally, the front-end
  and back-end interfaces of the two back ends differ significantly (e.g.,
  support for synchronous reads), which would require substantial refactoring.
- *Advantage of Proposed Solution*: Our chosen bespoke approach minimizes
  changes to the peering state machine and offers better potential for recovery
  during complex failure scenarios.

3.2 LRC (Locally Repairable Codes) Plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach involved configuring the existing LRC plugin to provide multi-zone
EC semantics. While LRC is designed for locality-aware erasure coding, it does
not address the core problems this design aims to solve:

- *Reason for Rejection*:

  1. **Writes still cross zones**: LRC distributes all chunks (data, local
     parity, and global parity) across the full CRUSH topology. Every write
     operation sends chunks over the inter-zone link, offering no bandwidth
     savings over standard EC.
  2. **Reads still cross zones**: Reading the original data requires ``k`` data
     chunks, which are spread across all zones. LRC provides no mechanism for a
     single zone to independently serve reads.
  3. **Recovery remains primary-centralized**: In the current Ceph EC
     infrastructure, all recovery operations are coordinated by the Primary OSD.
     While LRC defines local repair groups at the coding level, the EC back end
     would still need to be extended to delegate recovery execution to remote
     zones — the same complexity required by this proposal.

- *Advantage of Proposed Solution*: The replicated EC stripe approach places a
  complete ``(k+m)`` set at each zone, which inherently enables zone-local
  reads, zone-local single-OSD recovery (without any special coding scheme), and
  limits inter-zone link usage to write replication. It achieves better locality
  than LRC with less infrastructure complexity.


4. Configuration Logic (Preliminary)
--------------------------------------

The interactions between these parameters imply a hierarchy in the CRUSH
rule generation:

- **Top Level**: The CRUSH rule selects ``zones`` buckets of type
  ``zone`` (e.g., select 2 data centers).
- **Lower Level**: Inside each selected ``zone``, the CRUSH rule
  selects ``k+m`` OSDs to store the chunks.

This leverages standard CRUSH mechanisms — no changes to CRUSH
itself are required.


5. Multi-Zone & Topology Considerations
-----------------------------------------

While this architecture is capable of supporting N-zone configurations,
specific attention is given to the common 2-zone High Availability (HA) use
case.

- **Reference Diagrams**: For clarity, architectural diagrams and examples
  within this design documentation will primarily depict a 2-zone HA
  configuration (``--num-zones 2``, with 2 data centers).
- **Logical Scalability**: The design is logically N-way capable. The parameter
  ``num_zones`` is not limited to 2; the system supports any valid CRUSH topology where
  ``num_zones`` failure domains exist.
- **Testing Strategy**: Testing will follow a phased approach:

  1. **Unit Tests (Initial)**: The unit test framework will include tests for
     both 2-zone (``--num-zones 2``) and 3-zone (``num_zones=3``) configurations, validating the
     core peering and recovery logic for N-way topologies.
  2. **Full 2-Zone Testing (Initial Release)**: 2-zone configurations will be
     fully tested end-to-end for the initial release, covering all read, write,
     recovery, and failure scenarios.
  3. **Full 3-Zone Testing (Later Release)**: Full integration and real-world
     testing of 3-zone configurations will be deferred to a subsequent release.


5.1 Single-Zone Pools
~~~~~~~~~~~~~~~~~~~~~

The use case for "single-zone" pools is a Ceph cluster which is split across multiple data centers, but the redundancy is provided by the application. Here, the user can specify a pool which is restricted to a single data center. It should be noted that this means that loss of an inter-zone link will lead to an entire zone being lost (something that would not be the case for two independent clusters).


6. Topologies and Terminology
-------------------------------

To illustrate the relationship between the replication layer, the EC layer, and
the physical topology, we define the following terms and visualization.

6.1 Logical Diagram (2-Zone HA)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following diagram illustrates a cluster configured with ``r=2``, ``k=2``,
``m=1`` spanning two Data Centers.

.. ditaa::
  +-------------------+             +-------------------+
  |      Client A     |             |      Client B     |
  +---------+---------+             +---------+---------+
            |                                 |          
            v                                 v          
  +---------------------------------------------------+
  |             Logical Replication Layer             |
  |           (--num-zones 2, zone_failure_domain=DC)          |
  +--------------------------+------------------------+
                             |                           
              +--------------+--------------+            
              |                             |            
              v                             v            
  +-----------+---------+       +-----------+-----------+
  |                     |       |                       |
  |                     |       |                       |
  |    Data Center A    |       |     Data Center B     |
  |  (Replica/Zone 1)   |       |   (Replica/Zone 2)    |
  |                     |       |                       |
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

6.2 Key Definitions
~~~~~~~~~~~~~~~~~~~~

**Primary**
  The primary OSD in the acting set for a Placement Group (PG). This OSD is
  responsible for coordinating writes and reads for the PG.
  (Standard Ceph terminology.)

**Shard**
  The globally unique position of a chunk within the pool's acting set. Shards
  are numbered ``0`` through ``num_zones × (k + m) - 1``. In the diagram above, Zone A
  holds Shards 0, 1, 2 and Zone B holds Shards 3, 4, 5.

**Zone-local**
  An OSD or shard is zone-local if it shares the same ``zone`` as another
  OSD or shard.

**Zone Primary**
  An internal EC convention. The Zone Primary is the first primary-capable
  shard in the acting set that resides in a given zone.

  These zone primaries will be used in (post-R1) stretch clusters to perform
  local-to-zone erasure coding.  The intent is to minimize inter-zone bandwidth
  requirements. 

**Remote-zone Shard**
  A relative term referring to a shard in a different zone.  For example: "When 
  recovering data in Zone A, a remote-zone shard may be used to read data"


7. Read Path & Recovery Strategies
----------------------------------

This architecture supports multiple read strategies to optimize for locality
and handle failure scenarios. The strategies are presented in order of
increasing complexity: starting with the baseline read-from-Primary path,
then layering direct-read optimizations on top.

7.1 Read from Primary — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard read path involves the client contacting the Primary OSD.

- **Local Priority**: The Primary will prioritize reading from local-to-zone OSDs
  (within its own ``zone``) to serve the request or recover data
  (reconstruct the stripe). This minimizes inter-zone link traffic.

7.1.1 Zone-local Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^

When the Primary cannot serve a read from a single zone-local shard (e.g., because
a shard is missing or degraded), it reconstructs the data from the remaining
zone-local shards within its ``zone``.

7.1.2 Zone-Local Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^

If the Primary cannot reconstruct data using only zone-local shards, it may issue
direct reads to remote-zone OSDs. This cross-zone read path serves two purposes:

- **Backfill / Recovery**: When a zone-local OSD is down or being backfilled, the
  Primary can read the corresponding shard from the remote zone to recover the
  missing data. It is likely that a zone with insufficient EC chunks to
  reconstruct locally would be taken offline by the stretch cluster monitor
  logic, but this fallback ensures availability during transient states.
- **Medium Error Recovery**: If a zone-local OSD returns a medium error (e.g.,
  unreadable sector), the Primary can recover the affected data by reading from
  remote-zone shards and reconstructing locally.

7.2 Direct Reads — Primary Zone — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This feature extends the existing "EC Direct Reads" capability (documented
separately) to the stretch cluster topology. A client co-located with the
Primary can read data directly from a zone-local shard, bypassing the Primary
entirely on the good path. **No new code is required** — this is the standard
EC Direct Read path operating over stretch-cluster shard numbering. Any failure
falls back to the Primary read path (Section 7.1).

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

- **Failure Handling & Redirection (R1)**:

  Any failure encountered during a direct read — whether a missing shard,
  ``-EAGAIN`` rejection, or medium error — results in the client redirecting
  the operation to the **Primary** (regardless of the Primary's location).

7.3 Direct Reads — Zone-Aware — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The client selects which zone to read each data shard from based on the
read mode flag set on the operation. Two modes are supported:

.. note::

   The stretch CRUSH rule guarantees that the acting array is zone-contiguous::

       acting[0 .. zone_size-1]             → zone 0
       acting[zone_size .. 2*zone_size-1]   → zone 1
       …

   so the absolute raw shard index is ``rel_shard + zone_index × zone_size``.
   This arithmetic is the same for non-stretch pools (``zone_size == pool.size``,
   ``zone_index == 0``), so no separate code path is needed.

7.3.1 Localized Reads (``CEPH_OSD_FLAG_LOCALIZE_READS``) — *R1*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All data shards for a single operation are read from the **same zone** — the one
nearest to the client.

- **Zone Selection**: The client scores each zone by picking one arbitrary OSD
  from that zone's acting-set range and computing its CRUSH locality distance
  to the client via ``CrushWrapper::get_common_ancestor_distance()``. The zone
  with the lowest (nearest) distance wins. Ties are broken in favour of the
  lower-indexed zone. If no zone can be scored (``crush_location`` unset, all
  representatives absent, or no common ancestor), zone 0 is used as the
  default, which is also the correct behaviour for non-stretch pools.

- **Shard Selection**: For each required data chunk, the client selects the
  corresponding shard from the chosen zone. Every shard in the operation comes
  from the same zone. If any shard's OSD is unavailable (absent from the acting
  set or not ``exists()``), the split read is aborted and the operation falls
  back to the Primary. There is no cross-zone fallback.

- **Unavailable Zone**: If any required shard in the chosen zone is unavailable,
  the client does not attempt a direct read and instead directs the operation to
  the Primary. In later releases this will be improved to redirect to the local
  Zone Primary instead.

7.3.2 Balanced Reads (``CEPH_OSD_FLAG_BALANCE_READS``) — *R1*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each data shard for a single operation is read from a **randomly chosen zone**,
selected independently for each shard. This spreads read load across all zones
without requiring knowledge of the client's physical location.

- **Per-Shard Zone Selection**: For each required data shard ``rel_shard``
  (``0`` to ``k-1``), a zone is chosen uniformly at random from the available
  zones. The selection for each shard is independent; different shards in the
  same operation may come from different zones.

- **Shard Selection**: The absolute raw shard is
  ``rel_shard + random_zone × zone_size``. If the chosen zone's OSD for a given
  shard is unavailable (absent from the acting set or not ``exists()``), the
  split read is aborted and the operation falls back to the Primary.

- **No Locality Requirement**: Unlike Localized Reads, this mode does not
  require ``crush_location`` to be set on the client. It is suitable as a
  general-purpose load-balancing read strategy across all zones.

7.3.3 Common Behaviour
^^^^^^^^^^^^^^^^^^^^^^^

The following applies to both Localized and Balanced read modes:

- **Direct Read Execution**: The client sends each shard read directly to the
  target OSD. As detailed in the ``ec_direct_reads.rst`` design, it is generally
  acceptable for reads to overtake in-flight writes. However, the OSD is aware
  if it has an "uncommitted" write (a write that could potentially be rolled
  back) for the requested object. If such a write exists, or if the client's
  operation requires strict ordering (e.g. the ``rwordered`` flag is set), the
  OSD rejects the operation with ``-EAGAIN``. Data access errors (e.g., a media
  error on the underlying storage) also cause the OSD to reject the operation.

- **Failure Handling & Redirection (R1)**:

  An ``-EAGAIN`` will cause the client to retry the op. For R1, the op will be
  redriven to the Primary. R2 will redrive the op to the Zone Primary.

7.4 Read from Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the R1 release, the primary will handle all failures. In later releases,
an op which cannot be processed as a direct read will be directed at the
zone primary instead. The zone primary will attempt to reconstruct the
read data from the zone-local shards directly.

A conflict due to an uncommitted write will continue to be handled by
the primary, as the zone primary must also reject an op if an
uncommitted write exists for that object.

- **Zone-local Recovery**: A read directed to a Zone Primary will attempt to serve
  the request by recovering data using only OSDs within the same data center
  (the remote zone).
- **Zone Degradation**: If a zone has insufficient redundancy to reconstruct
  data locally, that zone should be taken offline to clients rather than serving
  reads that would require inter-zone link access.
- **``-EAGAIN`` Behavior**: The Zone Primary will return ``-EAGAIN`` only in
  short-lived transient conditions.


7.4.1 Safe Shard Identification & Synchronous Recovery — *Later Release*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, the Zone Primary does not inherently know which shards are
consistent and safe to read. To manage this, the Peering process will be
enhanced.

- **Synchronous Recovery Set**: The existing peering Activate message will be
  extended to include a "Synchronous Recovery Set". This set identifies shards
  that are currently undergoing recovery and may not yet be consistent.

  .. note::

     The peering state machine has a transition from Active+Clean to Recovery,
     which implies that recovery can start without a new peering interval.
     This needs further investigation to ensure the Synchronous Recovery Set
     is correctly maintained across such transitions.

- **Consistency Guarantee**: Within any single epoch, the Synchronous Recovery
  Set can only reduce over time (shards are removed as they complete recovery).
  The set is treated as binary — either the full recovery set is active, or it
  is empty. This ensures the Zone Primary is *conservatively stale*: it may
  reject reads it could safely serve, but will never read from an inconsistent
  shard.

- **Initial Behavior**:

  - Shards listed in the Synchronous Recovery Set will not be used for reads
    by the Zone Primary.
  - If the remaining available shards are insufficient to reconstruct the data,
    the Zone Primary will return ``-EAGAIN``.
  - Recovery messages will signal the Zone Primary when recovery completes,
    clearing the set.

- **Subsequent Enhancement**:

  - A mechanism will be added to allow the Zone Primary to request permission
    to read from a shard within the Synchronous Recovery Set.
  - This request will trigger the necessary recovery for that specific object
    (if not already complete) before the read is permitted.


7.5 Zone-Aware Replica Split Ops — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section applies to **replicated pools** only. It is not specific to EC
   and does not require Fast EC. The mechanism described here is an enhancement
   to the existing ``ReplicaSplitOp`` implementation in
   ``src/osdc/SplitOp.cc``.

The existing ``ReplicaSplitOp`` path (see :class:`ReplicaSplitOp`) divides
large read operations across replicas for parallel execution. When the
``CEPH_OSD_FLAG_BALANCE_READS`` flag is set, chunks are distributed round-robin
across all available replicas, starting from a randomly selected replica for
load balancing. When the ``CEPH_OSD_FLAG_LOCALIZE_READS`` flag is set on a
non-stretch replica pool today, there is no zone-awareness — the replica
selection is still effectively unconstrained.

For stretched replicated pools (``zones > 1``), when
``CEPH_OSD_FLAG_LOCALIZE_READS`` is set, the ``ReplicaSplitOp`` restricts
all sub-read shards to replicas that reside within the **local zone** — the
zone nearest to the client.

**Mechanism — Shard Selection**

The zone is selected using the same closeness-score algorithm already employed
by the EC split path:

1. For each zone in the pool's acting set, one representative OSD is sampled
   (the first non-``CRUSH_ITEM_NONE`` entry in that zone's acting-set range).
2. The representative OSD's CRUSH locality distance to the client is computed
   via ``CrushWrapper::get_common_ancestor_distance()``.
3. The zone with the **lowest** (nearest) closeness score wins. Ties are broken
   in favour of the lower-indexed zone. If no zone can be scored
   (``crush_location`` unset, all representatives absent, or no common
   ancestor), zone 0 is used as the default — which is also the correct
   behaviour for non-stretch pools.
4. All sub-read shards are then selected **exclusively** from the OSDs that
   belong to the chosen zone. Shards from other zones are not used, regardless
   of how many replicas are available there.

If any OSD in the chosen zone is unavailable (absent from the acting set or
not ``exists()``), the split-read is aborted and the operation falls back to
the Primary, exactly as the existing failure path works today.

**Non-Stretch Pools**

For non-stretch replica pools (``zones == 1``), the behaviour is unchanged:
``LOCALIZE_READS`` is treated identically to ``BALANCE_READS`` for split ops,
because all replicas share the same zone.

**Relationship to EC Zone-Aware Reads**

This mechanism is the replica analogue of the EC zone-aware direct-read path
described in Section 7.3.1. Both share the same zone-selection algorithm
(``get_common_ancestor_distance`` / lowest closeness score), and both fall back
to the Primary on any shard unavailability. The shared implementation lives in
the ``SplitOp::local_zone_for_acting_set()`` static helper in ``SplitOp.cc``,
reused by both ``ECSplitOp`` and ``ReplicaSplitOp``.


7.6 Zone-Aware Read Enhancements — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following enhancements to the EC and replica zone-aware read paths are
fully designed but are not required for R1 and will land in a later release.

- **Zone Primary Fallback (EC and Replica)**: A failed localized read —
  whether an EC direct read or a replica split op — will be redirected to the
  local Zone Primary rather than the global Primary, consistent with the
  improvements described in Section 7.4. This avoids cross-zone latency on
  the fallback path when the client's zone is healthy but a single shard is
  temporarily unavailable.

- **Per-Zone ``min_size`` Interaction**: Once per-zone ``min_size`` enforcement
  (Section 11.2) is implemented, both the EC split-op path and the replica
  split-op path should also consult the per-zone availability state before
  choosing a zone for localized reads. This prevents routing reads to a zone
  that the monitor has already determined to be below its minimum shard
  threshold.

- **Zone-Aware Balanced Reads for Replica Pools**: The ``BALANCE_READS``
  implementation for replica pools will be extended to restrict balanced reads
  to zone-local replicas when sufficient replicas are available, combining load
  distribution with locality.


8. Write Path & Transaction Handling
------------------------------------

This section details how write operations are managed, specifically focusing on
Read-Modify-Write (RMW) sequences and the replication of write transactions to
remote zones.

8.1 Direct-to-OSD Writes (Primary Encodes All Shards) — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, all writes are coordinated exclusively by the Primary.

- **Mechanism**: The Primary generates all ``num_zones × (k + m)`` coded shard writes
  and sends individual write operations directly to every OSD in the acting
  set, including those in remote zones.
- **RMW Reads**: The "read" portion of any Read-Modify-Write cycle is performed
  locally by the Primary, strictly adhering to the logic defined in Section 7
  (Read Path & Recovery Strategies).
- **Inter-zone traffic**: This sends all coded shard data (including parity)
  over the inter-zone link. While this is not bandwidth-optimal, it requires
  no Zone Primary involvement in write processing and therefore no new
  write-path code beyond extending the existing shard fan-out to the larger
  acting set.

8.2 Replicate Transaction (Preferred Path) — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. R1 sends all coded
   shards directly from the Primary.

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

       P->>RP: Replicate
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
       RS->>RP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       ZP->>P: ReplicateDone
       deactivate ZP

       P->>C: Complete
       deactivate P

This mechanism is designed to minimize inter-zone link bandwidth usage, which
is often the most constrained resource in stretch clusters.

- **Mechanism**: The replicate message is an extension of the existing
  sub-write message. Instead of sending individually coded shard data to every
  OSD in the remote zone, the Primary sends a copy of the *PGBackend
  transaction* (i.e., the raw write data and metadata, prior to EC encoding) to
  the Zone Primary.
- **Role of Zone Primary**: Upon receipt, the Zone Primary processes the
  transaction through its local ECTransaction pipeline — largely unmodified
  code — to generate the coded shards for its zone. For writes smaller than a
  full stripe, this includes the Zone Primary issuing reads to its zone-local
  shards so that the parity update can be calculated. It then fans out the shard
  writes to the zone-local OSDs within its ``zone``. This means coding
  (parity) data is never sent over the inter-zone link; it is computed
  independently at each zone.
- **Completion**: The Zone Primary responds using the existing sub-write-reply
  message once all zone-local shard writes are durable.
- **Crash Handling**: If the Zone Primary crashes mid-fan-out, any partial
  writes on remote-zone shards are rolled back by the existing peering process. No
  new crash-recovery mechanisms are required.
- **Benefit**: This reduces inter-zone link traffic to a single transaction
  message per remote zone, carrying only the raw data — not the ``k+m`` coded
  chunks.

8.3 Client Writes Direct to Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. It requires Replicate
   Transaction (Section 8.2) as a prerequisite, since the Zone Primary must
   be able to process transactions and fan out shard writes locally.

.. mermaid::

   sequenceDiagram
       title Write to Zone Primary

       participant C as Client
       participant P as Primary
       participant ZS as Zone-local Shard
       participant RC as Remote Client
       participant ZP as Zone Primary
       participant RS as Remote-zone Shard

       RC->>RP: Write op
       activate ZP

       ZP->>P: Replicate
       activate P
       P->>RP: Permission to write

       note over ZP: There is potential for pre-emptive caching here,<br/>but it adds complexity and mostly does not actually<br/>help performance, as the limiting factor is actually<br/>the Remote-zone write.

       ZP->>RS: SubRead (cache)
       activate RS
       RS->>RP: SubReadReply
       deactivate RS

       ZP->>RS: SubWrite
       activate RS
       RS->>RP: SubWriteReply
       deactivate RS

       P->>ZS: SubRead (cache)
       activate ZS
       ZS->>P: SubReadReply
       deactivate ZS

       P->>ZS: SubWrite
       activate ZS
       ZS->>P: SubWriteReply
       deactivate ZS

       P->>RP: write done
       ZP->>RC: Complete

       note over ZP: The write complete message is permitted<br/>as soon as all SubWriteReply messages have<br/>been received by the remote. It is shown<br/>here to demonstrate that it is required to<br/>complete processing on the Primary.

       ZP->>P: Remote-zone write complete
       deactivate P
       deactivate ZP

       P->>P: PG Idle.

       P->>RP: DummyOp
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
       RS->>RP: Done
       deactivate RS
       deactivate ZP

This mechanism allows a client to write to its local Zone Primary rather than
sending data over the inter-zone link to the Primary. The key benefit is that
the write data crosses the inter-zone link only once (Zone Primary → Primary)
rather than twice (client → Primary → Zone Primary). Unlike Section 8.2 where
the Primary initiates the transaction and replicates it outward, here the
Zone Primary receives the client write directly and coordinates with the
Primary to maintain global ordering while avoiding redundant data transfer.

- **Mechanism**:

  1. The client sends the write to its local **Zone Primary**. The Zone
     Primary stashes a local copy of the write data but does not yet fan out
     to zone-local shards.
  2. The Zone Primary replicates the *PGBackend transaction* (raw write data,
     prior to EC encoding) to the **Primary**.
  3. The Primary processes the transaction as normal through its local
     ECTransaction pipeline, performing cache reads, encoding, and fanning out
     shard writes to its zone-local OSDs. When the Primary reaches the step where it
     would normally replicate data to the originating Zone Primary (as in
     Section 8.2), it instead sends a **write-permission message** to that
     Zone Primary, since the Zone Primary already holds the data. Writes to
     any *other* Zone Primaries (in an ``num_zones > 2`` configuration) proceed via
     the normal Replicate Transaction path (Section 8.2).
  4. Upon receiving write-permission, the Zone Primary processes the
     transaction through its local ECTransaction pipeline, generating and
     fanning out the coded shard writes to the zone-local OSDs within its
     ``zone``.
  5. Once the Primary's own local writes are durable and all other zones have
     confirmed durability, the Primary sends a **completion message** to the
     originating Zone Primary. The Zone Primary then responds to the client
     once the completion message is received and its own local writes are also
     durable.

- **Write Ordering**: Strict write ordering must be maintained across all zones.
  Since the Primary remains the single authority for PG log sequencing:

  - The Primary sequences the write as part of its normal processing in step 3.
    The write-permission message carries the assigned sequence number, ensuring
    that writes arriving at the Primary directly and writes arriving via a
    Zone Primary are globally ordered.
  - Writes from multiple Zone Primaries (in an ``num_zones > 2`` configuration) are
    serialized through the Primary's normal sequencing mechanism — no special
    handling is required beyond the existing PG log ordering.
  - The Zone Primary does not fan out to zone-local shards until it receives the
    write-permission message, guaranteeing that zone-local shard writes occur in
    the correct global order.

- **Benefit**: For workloads where the client is co-located with a remote zone,
  write data traverses the inter-zone link exactly once (Zone Primary →
  Primary transaction replication). This halves the inter-zone bandwidth
  compared to R1 (client → Primary → Zone Primary), and is equivalent to
  Section 8.2 but with the added advantage that the client experiences local
  write latency for the initial acknowledgement. Crucially, the data is never
  sent back to the originating Zone Primary — the Primary only sends
  lightweight permission and completion messages.

- **Failure Handling**: If the Zone Primary is unavailable, the client falls
  back to writing directly to the Primary (Section 8.1 or 8.2, depending on
  what is available). If the Primary is unreachable from the Zone Primary
  mid-transaction, the Zone Primary returns an error to the client and any
  stashed data is discarded (no zone-local shard writes have been issued, since
  write-permission was never received).

- **R3 Enhancement: Forward to Other Zone Primaries (``num_zones > 2``)**:

  .. note::

     This enhancement is a potential R3 feature and may not be implemented.

  In topologies with more than two zones, the originating Zone Primary could
  forward the transaction to the other Zone Primaries in parallel with
  sending it to the Primary. This would improve write latency for those zones
  by allowing them to receive the data directly from a peer Zone Primary
  rather than waiting for the Primary to replicate outward. The Primary would
  still issue write-permission messages to all Zone Primaries to maintain
  global ordering, but the data transfer to additional zones would already be
  complete, reducing the critical path.

  *Complexity / Review Note*: There is a known race condition here. The
  write-permission message originating from the Primary might overtake the
  data transfer sent by the originating Zone Primary. To handle this, a
  unique transaction ID will be required to definitively tie these two
  independent messages together at the receiving Zone Primary.

8.4 Hybrid Approach — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Replicate Transaction (Section 8.2). Deferred to a later release.

It is acknowledged that complex failure scenarios may require a combination of
Replicate Transaction, Client-via-Remote-Primary, and Direct-to-OSD approaches.

- **Example**: In a partial failure where one remote zone is healthy and another
  is degraded, the Primary may use Replicate Transaction for the healthy zone
  while simultaneously using Direct-to-OSD for the degraded zone within the
  same transaction lifecycle.


9. Recovery Logic
------------------

All recovery operations are centralized and coordinated by the Primary OSD.

9.1 Primary-Centric Recovery — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In R1, the Primary performs all recovery without delegating to Zone
Primaries and without attempting to minimize inter-zone bandwidth.

- **Zone-local Recovery**: The Primary recovers missing shards in its own
  ``zone`` using available zone-local chunks, exactly as standard EC
  recovery.
- **Remote-zone Recovery**: For every remote ``zone``, the Primary reads
  all shards required to reconstruct any missing data, encodes the missing
  shards locally, and pushes them directly to the target remote-zone OSDs.
- **No Zone Primary Coordination**: The Zone Primary plays no role in
  recovery in R1. All cross-zone data flows from or to the Primary.
- **Trade-off**: This approach may send more data over the inter-zone link than
  necessary, but it avoids the complexity of remote-delegated recovery and uses
  existing recovery infrastructure with minimal modification.

9.2 Cost-Based Recovery Planning — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This section is deferred to a later release. R1 uses Primary-centric
   recovery (Section 9.1) for all scenarios.

When the system identifies an individual object (including its clones) requiring
recovery, the Primary executes a per-object assessment and planning phase. This
leverages the existing, reactive, object-by-object recovery mechanism rather
than introducing a broad distributed orchestration layer. The overriding goal
is to **minimize inter-zone link bandwidth** — every cross-zone transfer is
expensive, so the Primary must dynamically choose the recovery strategy for
each object that results in the fewest bytes crossing zone boundaries.

- **Assess Capabilities**: For each ``zone``, the Primary
  determines how many consistent chunks are available locally and how many are
  missing.
- **Cost-Based Plan Selection**: The Primary evaluates the inter-zone bandwidth
  cost of each viable recovery strategy and selects the cheapest option. The
  key trade-off is between:

  1. *Zone Primary performs zone-local recovery with remote-zone reads* — the Zone
     Primary reads a small number of missing shards from the Primary's zone
     and reconstructs locally.
  2. *Primary reconstructs and pushes* — the Primary reconstructs the full
     object and pushes the missing shards to the remote zone.

  The optimal choice depends on how many shards each zone is missing.

- **Example**: Consider a ``6+2`` (k=6, m=2) configuration where Zone A has 8
  good shards and Zone B has only 5 good shards (3 missing). Two options:

  - *Option A (Zone Primary reads remotely)*: The Zone Primary at Zone B
    needs only **1 remote-zone read** (it has 5 of the 6 required chunks locally
    and fetches 1 from Zone A) to reconstruct the 3 missing shards locally.
    **Cost: 1 shard across the inter-zone link.**
  - *Option B (Primary pushes)*: The Primary reconstructs the 3 missing shards
    at Zone A and pushes them to Zone B. **Cost: 3 shards across the
    inter-zone link.**

  Option A is clearly cheaper. A general algorithm should compare the
  cross-zone transfer cost for each strategy and choose the minimum.
  Where inter-zone bandwidth consumption is equal, the algorithm should tie-break
  by optimizing for zone-local read bandwidth or the total number of operations.

  (In the future, the system may adapt these priorities — e.g. explicitly
  optimizing for read count rather than inter-zone bandwidth on high-bandwidth
  links to slower rotational media — whether through auto-tuning or operator
  configuration. However, exploring alternative optimization modes is beyond the
  scope of this design.)

  .. note::

     The detailed algorithm for optimal recovery planning requires further
     design work. The general principle is: count the number of cross-zone
     shard transfers each strategy would require and choose the minimum.

- **Plan Construction**: For each domain, the Primary constructs a "Recovery
  Plan" message containing specific instructions detailing which OSDs must be
  used to read the available chunks and which OSDs are the targets to write the
  recovered chunks. Where cross-zone reads are part of the plan, the message
  specifies which remote-zone shards to fetch.

9.3 Delegated Remote-zone Recovery — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Requires Cost-Based Recovery Planning (Section 9.2). Deferred to a later
   release.

- **Remote-zone Recovery (Plan-Based)**: The Primary sends a per-object "Recovery Plan"
  to the Zone Primary (or appropriate peers), instructing them to execute the
  recovery for that object (and clones) locally within their domain using the
  provided read/write set. If the plan includes cross-zone reads, the Zone Primary
  fetches the specified shards before reconstructing. Because this ties directly
  into the existing object recovery lifecycle, any failure of a remote-zone peer
  during the plan simply drops the recovery op. The Primary detects the failure
  via standard peering or timeout mechanisms and will naturally re-assess and
  generate a new plan for the object.

9.4 Fallback Strategies — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   These strategies are part of the Cost-Based Recovery system (Section 9.2)
   and are deferred to a later release.

9.4.1 Remote-zone Fallback (Push Recovery)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a remote zone cannot perform zone-local recovery, and the cost-based analysis
determines that Primary-side reconstruction and push is the cheapest option:

1. The Primary reconstructs the full object locally (using whatever valid shards
   are available across the cluster).
2. The Primary pushes the full object data to the Zone Primary.
3. This push is accompanied by a set of instructions specifying which shards in
   the remote zone must be written.

9.4.2 Primary Domain Fallback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the Primary's own domain lacks sufficient chunks to recover locally:

1. The Primary is permitted to read required shards from remote zones to perform
   the reconstruction.
2. While this crosses zone boundaries, the cost-based planning ensures it is
   only chosen when it results in fewer cross-zone transfers than any
   alternative.


10. PG Log Handling — *R1*
---------------------------

This section describes how PG log entries are managed across replicated EC
stripes, building on the FastEC design for primary-capable and non-primary
shards.

10.1 Primary-Capable vs. Non-Primary Shards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing FastEC design distinguishes between two classes of shard:

- **Primary-capable shards**: Maintain a full copy of the PG log, enabling
  them to take over as Primary if needed.
- **Non-primary shards**: Maintain a reduced PG log, omitting entries where no
  write was performed to that specific shard.

For replicated EC stretch clusters, the non-primary shard selection algorithm
will be extended. The following shards will be classified as **primary-capable**
(i.e., they maintain a full PG log):

- The first data shard in each replica (per ``zone``).
- All coding (parity) shards in each replica.

All remaining data shards will be non-primary shards with reduced logs.

.. note::

   Implementation detail: it needs to be determined whether ``pg_temp`` should
   be used to arrange all primary-capable shards (local, then remote) ahead of
   non-primary-capable shards in the acting set ordering.

10.2 Independent Log Generation at Zone Primary — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Log generation at the Zone Primary is only needed when Replicate
   Transaction writes (Section 8.2) are implemented. For R1, where the
   Primary sends pre-encoded shards directly, PG log entries are generated
   solely by the Primary and distributed with the sub-write messages as per
   existing behavior.

For latency optimization, the replicate transaction message (see Section 8.2)
will be sent to the Zone Primary *before* the cache read has completed on the
Primary. This means the Zone Primary cannot receive a pre-built log entry from
the Primary — it must generate its own PG log entry independently from the
transaction data.

Both the Primary and Zone Primary will produce equivalent log entries from the
same transaction, but they are generated independently at each zone.

10.3 Log Entry Compatibility & Upgrade Safety — *Later Release*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``osdmap.requires_osd_release`` field dictates the minimum code level that
OSDs can run, and therefore determines the log entry format that must be used.
No new versioning mechanism is required — the existing release gating already
constrains log entry compatibility across mixed-version clusters.

Because the Zone Primary generates its own log entry from the transaction
data, it must populate the Object-Based Context (OBC) to obtain the old
version of attributes (including ``object_info_t`` for old size). This is an
additional reason the Zone Primary must be a **primary-capable shard** — only
primary-capable shards maintain the full PG log and OBC state needed for
correct log entry construction.

**Debug Mode for Log Entry Equivalence**

A debug mode will be implemented that compares the log entries generated by the
Primary and Zone Primary for each transaction. When enabled, the Primary will
include its generated log entry in the replicate transaction message, and the
Zone Primary will assert that its independently generated entry is identical.
This mode will be **enabled by default in teuthology testing** to catch any
divergence early. It will be available as a runtime configuration option for
production debugging but disabled by default in production due to the
additional message overhead.

.. warning::

   If a future release changes how PG log entries are derived from
   transactions, the debug equivalence mode provides an automated safety net.
   Any divergence between Primary and Zone Primary log entries will be caught
   immediately in CI.


11. Peering & Stretch Mode — *R1*
-----------------------------------

This section covers the peering, ``min_size``, and stretch-mode integration
required for replicated EC pools. The design follows the existing replica
stretch cluster pattern as closely as possible, with EC-specific adaptations.

.. important::

   **R1 scope is simple two-zone (--num-zones 2).** Three-zone (``num_zones=3``) details are
   described for design completeness but will be implemented in a later
   release.

11.1 Pool Lifecycle
~~~~~~~~~~~~~~~~~~~~~

A pool with ``num_zones > 1``, replicated or EC, operates in stretch mode. Stretch mode is
enabled when such a pool is created or a pool's ``num_zones`` is set to 2. Global stretch
mode (``ceph mon enable_stretch_mode`` and ``disable_stretch_mode``) changes the pool creation
defaults and every existing pool (Section 2.3).

- **CRUSH rule**: A stretch rule generated for the pool's ``num_zones``, unless ``--rule``
  gives one.
- **min_size**: Set at pool creation; never automatically changed by the
  monitor during stretch mode state transitions. Users may still adjust it
  manually via ``ceph osd pool set <pool> min_size <value>``.
- **Peering**: Uses stretch-aware acting set calculation with parameters
  adjusted by the monitor during state transitions.
- **Failure**: Degraded/recovery/healthy stretch mode transitions managed by
  OSDMonitor; ``min_size`` is not modified.

**Prerequisites for ``num_zones > 1`` Pool Creation**

.. list-table::
   :header-rows: 1

   * - Requirement
     - Reason
   * - The upgrade is committed (``require_osd_release`` raised to the release with
       this design)
     - Ensures all OSDs understand extended acting-set semantics (Section 15)
   * - Monitors in both zones and a tiebreaker monitor in a third location, each
       with a CRUSH location at the zone failure domain
     - Creating the pool enables stretch mode if it is not enabled yet, and
       ``num_zones > 1`` pools need the stretch mode state machine for zone
       failover (Sections 2.3.4 and 11.3.1)
   * - ``zone_failure_domain`` must match the stretch mode failure domain once
       stretch mode is enabled
     - The CRUSH rule must align with the stretch cluster topology

11.2 Minimum PG Size Semantics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The user defines a ``min_size`` for a single zone, which implicitly specifies
the number of failures the pool will tolerate. For an EC pool, this is in the
range ``K`` to ``K+M``, defining a tolerance of ``0`` to ``M`` failures. For a
replica pool, this is in the range ``1`` to ``size``, defining a tolerance of
``0`` to ``size - min_size`` failures. 

Let the number of tolerated failures derived from this setup be denoted as **F**.

If num_zones > 1, then this setting is dynamically *interpreted*
according to the cluster's stretch mode (Healthy, Degraded, Recovery). EC pools 
with multiple zones will interpret ``min_size`` this way,
rather than actively modifying the ``min_size`` setting whenever a stretch
mode transition occurs.

11.2.1 Per-Zone Min-Size Interpretations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- There may be up to ``F`` OSD failures *in each individual zone*.
- A partial failure within a zone may drop a pool below its ``min_size``
  requirement and cause I/O to stop. Manually removing the rest of the failed
  zone will cause a transition to **Degraded Stretch Mode**, which might be
  sufficient to bring the pool back online because the ``min_size`` requirement
  is now met by the surviving zone. There will be no automation to promote a
  partial zone failure to a whole zone failure.

11.3 Stretch Mode State Machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first pool with ``num_zones = 2`` enables stretch mode and the last one to
go disables it. ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode``
do so by changing every pool's ``num_zones`` (Section 2.3.4). The degraded,
recovery and healthy transitions are those of the replica stretch mode state
machine, leveraging the existing OSDMonitor infrastructure. ``min_size`` is set
at pool creation and is never mutated by the OSDMonitor upon stretch mode
transitions.

.. mermaid::

   stateDiagram-v2
       [*] --> Healthy: first pool with num_zones 2
       Healthy --> Degraded: zone failure detected
       Degraded --> Recovery: failed zone returns
       Recovery --> Healthy: all PGs clean
       Degraded --> Recovery: force_recovery_stretch_mode CLI
       Recovery --> Healthy: force_healthy_stretch_mode CLI
       Healthy --> [*]: last stretch pool deleted or set to num_zones 1
       Degraded --> [*]: last stretch pool deleted or set to num_zones 1
       Recovery --> [*]: last stretch pool deleted or set to num_zones 1

**Concrete example — K=2, M=1, --num-zones 2 (size=6):**

``min_size`` is set to ``2`` at creation and remains ``2`` in every stretch state.
I/O will stop if either zone has less than ``2`` shards active.

11.3.1 Entering Stretch Mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Stretch mode is enabled by the first pool that gets ``num_zones = 2``: at
creation (``ceph osd pool create ... --num_zones 2``,
``OSDMonitor::prepare_new_pool``), with ``ceph osd pool set <pool> num_zones 2``
(``prepare_command_pool_set_num_zones``, Section 13.2), or with ``ceph mon
enable_stretch_mode``, which sets every pool to two zones (Section 2.3.4). The
command is refused unless:

- the CRUSH map has exactly two buckets of the zone failure domain type, and
  their weights differ by no more than ``mon_stretch_max_bucket_weight_delta``
  times the lighter one's;
- every monitor has a CRUSH location at the zone failure domain, both zones
  have a monitor, and exactly one monitor is outside them (or ``ceph mon
  enable_stretch_mode`` names the tiebreaker);
- the monitors in quorum support the connectivity election strategy.

Enabling stretch mode changes:

- **MonMap** (``MonmapMonitor::try_enable_stretch_mode``): the election
  strategy becomes connectivity, the monitor outside both zones becomes
  ``tiebreaker_mon`` and is added to ``disallowed_leaders``, and
  ``stretch_mode_enabled`` is set.
- **OSDMap** (``OSDMonitor::try_enable_stretch_mode``):
  ``stretch_mode_enabled``, ``stretch_bucket_count = 2`` and
  ``stretch_mode_bucket`` (the zone failure domain type) are set, and the
  degraded and recovering flags are cleared.
- **Pool**: ``peering_crush_bucket_count`` and ``peering_crush_bucket_target``
  are 2, ``peering_crush_bucket_barrier`` is the zone failure domain type and
  there is no ``peering_crush_mandatory_member`` (Section 11.6).

Once both maps have committed, every monitor engages stretch mode and drops OSD
sessions from outside its own zone (``Monitor::try_engage_stretch_mode``).
``osd pool set`` commits the MonMap and OSDMap changes in one Paxos round, but
it stages the MonMap change before it checks the zone weights. ``osd pool
create`` makes every check first, but proposes the MonMap change on its own, so
that change can commit first. The monitors then call an election, and the
resent command finds the MonMap already in stretch mode and makes only the
OSDMap and pool changes.

A later pool with ``num_zones = 2``, created or set, joins the current state,
which may be degraded or recovering, without changing it (Section 11.4.2). Its
zone failure domain must be the OSDMap's ``stretch_mode_bucket`` and its rule
must cover the same zone buckets as the other stretch pools
(``validate_stretch_mode_new_pool``).

11.3.2 Degraded, Recovery and Healthy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Healthy to Degraded**: the leader finds a zone, other than the
  tiebreaker's, with no monitor in quorum and all of its OSDs down
  (``Monitor::maybe_go_degraded_stretch_mode``). The OSDMap gets
  ``degraded_stretch_mode = 1``. Every stretch pool gets
  ``peering_crush_bucket_count = 1`` and the surviving zone as
  ``peering_crush_mandatory_member``. The MonMap lists the failed zone's
  monitors in ``stretch_marked_down_mons``, so they cannot lead.
- **Degraded to Recovery**: when a new OSDMap brings OSDs up, the ratio of up
  OSDs exceeds ``mon_stretch_cluster_recovery_ratio`` and every zone, and the
  tiebreaker's location, has a monitor in quorum; or with ``ceph osd
  force_recovery_stretch_mode``. The OSDMap gets
  ``recovering_stretch_mode = 1``. Pools are not changed.
- **Recovery to Healthy**: once ``mon_stretch_recovery_min_wait`` has passed and
  no PG is degraded, inactive or unknown; or with ``ceph osd
  force_healthy_stretch_mode``. Both flags are cleared, every stretch pool gets
  ``peering_crush_bucket_count = 2`` and no mandatory member back, and
  ``stretch_marked_down_mons`` is emptied.

11.3.3 Leaving Stretch Mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Stretch mode is disabled when the last stretch pool goes:

- ``ceph osd pool delete`` of the last pool with ``peering_crush_bucket_count``
  and ``peering_crush_bucket_target`` set (``OSDMonitor::_prepare_remove_pool``,
  ``is_last_stretch_pool``). This applies only while both the MonMap and the
  OSDMap are in stretch mode. The deletion is retried until the MonmapMonitor
  is writeable, and both maps change in one Paxos round.
- ``ceph osd pool set <pool> num_zones 1`` on the last stretch pool, counting
  the pool changes pending in the same epoch, or ``ceph mon
  disable_stretch_mode``, which sets every stretch pool to one zone (Section
  2.3.4). The pool gets a single-zone CRUSH rule (``--crush_rule``, otherwise
  the default replicated rule or a new ``<pool>-single-zone`` EC rule), and its
  ``peering_crush_*`` fields are cleared. A replicated pool gets ``--replica``
  or ``osd_pool_default_replica`` copies and the default ``min_size`` for them;
  an EC pool gets ``size`` K+M and ``min_size`` K + min(1, M-1).

Disabling stretch mode changes:

- **MonMap** (``MonmapMonitor::clear_stretch_mode_state``):
  ``stretch_mode_enabled`` is cleared, and ``tiebreaker_mon``,
  ``disallowed_leaders`` and ``stretch_marked_down_mons`` are emptied. Every
  disallowed leader is removed, including any added with ``ceph mon add
  disallowed_leader``. The election strategy stays connectivity.
- **OSDMap**: ``stretch_mode_enabled``, ``stretch_bucket_count``,
  ``stretch_mode_bucket``, ``degraded_stretch_mode`` and
  ``recovering_stretch_mode`` are cleared.

The monitors then disengage stretch mode and stop checking which zone an OSD
session comes from. No other pool changes, as none is a stretch pool.

Deleting the last stretch pool or setting it to ``num_zones 1`` does not check
the stretch mode state, so stretch mode is left the same way from Healthy,
Degraded or Recovery. In Degraded or Recovery, the degraded and recovering flags
and ``stretch_marked_down_mons`` are cleared without a transition to Healthy,
while the failed zone may still be down. ``ceph mon disable_stretch_mode`` is
refused in Recovery.

.. note::

   **Open questions for reviewers**

   - Should leaving stretch mode in Degraded or Recovery be refused, as ``ceph
     mon disable_stretch_mode`` is refused in Recovery? Or is leaving at once
     intended, because no stretch pool is left to protect?
   - Entering can leave the MonMap in stretch mode with no stretch pool: when
     ``osd pool set`` is refused for uneven zone weights, or when the client
     does not resend ``osd pool create``. Should both commit the MonMap change
     only with the pool, in the same Paxos round, as ``osd pool delete`` does?

11.4 OSDMonitor Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing OSDMonitor stretch mode code contains
``if (is_replicated()) { ... } else { /* not supported */ }`` patterns in
several places. These gaps must be filled for EC pools with ``num_zones > 1``.

**11.4.1 Pool Stretch Set / Unset** (``prepare_command_pool_stretch_set``,
``prepare_command_pool_stretch_unset``)

``ceph osd pool stretch set`` and ``unset`` are main's commands for an individual
stretch pool. They take the pool's stretch values (``peering_crush_bucket_*``,
``crush_rule``, ``size`` and ``min_size``) from the command line. An EC pool is
stretched and unstretched only with ``ceph osd pool set <pool> num_zones <n>``
(Section 13.2), which works out all of these, and the shards that cannot be
primary, from the zone count. Two ways to stretch an EC pool would have to agree
on every one of these values, so:

- ``stretch set`` refuses every EC pool, with ``osd pool stretch set is not
  supported for EC pools; use 'ceph osd pool set <pool> num_zones <N>' instead``.
- ``stretch unset`` refuses an EC pool with ``num_zones > 1`` in the same way,
  pointing to ``num_zones 1``. An EC pool with ``num_zones = 1`` has stretch
  values only if an older release's ``stretch set`` gave them. As on main,
  ``stretch unset`` clears them only with ``size`` K+M and a ``min_size`` from K
  to K+M, which also repairs a ``size`` that the older ``stretch set`` changed.
- Both are refused for every pool type while stretch mode is enabled: stretch
  mode sets the stretch values of its pools itself.

**11.4.2 Enable/Disable Stretch Mode** (``try_enable_stretch_mode``,
``try_disable_stretch_mode``)

Stretch mode is enabled with the first pool that gets ``num_zones = 2`` and
disabled when the last one goes (Sections 11.3.1 and 11.3.3). Replicated and EC
pools are accepted alike. The tiebreaker monitor is the one monitor outside both
zones, or the monitor named by ``enable_stretch_mode``'s legacy
``tiebreaker_mon`` argument. Without that argument enabling is refused if there
is no monitor outside both zones, or more than one. Each pool with ``num_zones =
2`` has ``peering_crush_bucket_count``, ``peering_crush_bucket_target`` and
``peering_crush_bucket_barrier`` set (same values as replica, Section 11.6).

Creating a pool with ``num_zones = 2`` while stretch mode is already enabled
configures only the new pool; it does not change the cluster's stretch mode
state. In degraded or recovery stretch mode the new pool keeps its full ``size``
and ``peering_crush_bucket_target``, as the existing stretch pools do. It is
given the degraded ``peering_crush_bucket_count`` and the
``peering_crush_mandatory_member`` that the existing stretch pools have (Section
11.6), so it can go active in the surviving zone, and the healthy transition
(11.4.4) restores it with them.

**11.4.3 Degraded Stretch Mode** (``trigger_degraded_stretch_mode``)

``min_size`` is **not modified** for any pool. Only
``peering_crush_bucket_count`` and ``peering_crush_mandatory_member`` are
updated.

**11.4.4 Healthy Stretch Mode** (``trigger_healthy_stretch_mode``)

``min_size`` is **not modified** for any pool. ``peering_crush_bucket_count``
is restored and ``peering_crush_mandatory_member`` is cleared (Section 11.6).

**11.4.5 Recovery Stretch Mode** (``trigger_recovery_stretch_mode``)

No changes required — does not modify ``min_size``.

11.5 Peering State Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**11.5.1 Acting Set Calculation**

``calc_replicated_acting_stretch`` is pool-type agnostic at the CRUSH bucket
level, but for EC pools the acting set has **shard semantics** — position
``i`` must serve shard ``i``. A new ``calc_ec_acting_stretch`` function (or
an extension of the existing ``calc_ec_acting``) is required.

Objects are stored per absolute shard, so an OSD is usable for position ``i``
only if it holds shard ``i`` itself. When ``num_zones > 1``, all positions of a
zone block are served from one CRUSH zone, and no two blocks share a zone.
The blocks are given the zones that together serve the most distinct relative
shards, up to ``K``, so the PG stays recoverable if any assignment keeps it
so. A zone holding fewer than ``K`` of a block's shards cannot serve it on its
own, so ties go to the zones that together hold the most of their shards,
counting a zone only if it holds at least ``K`` of its block's shards, then to
the assignment that uses the most ``up`` OSDs in such zones, then holds the
most shards, then uses the most ``up`` OSDs, then the most ``acting`` OSDs. A
block therefore keeps being served from where its data is (for example from
``acting`` after ``up`` swaps the zone blocks) while the ``up`` OSDs are
backfilled, and moves to the ``up`` OSDs once they hold it.

Algorithm for each position ``i`` (0 to ``num_zones×(K+M)−1``), considering
only OSDs in the CRUSH zone chosen for its block:

1. Prefer ``up[i]`` if usable
2. Otherwise prefer ``acting[i]`` if usable
3. Otherwise search strays for an OSD with shard ``i``
4. If no usable OSD found, leave as ``CRUSH_ITEM_NONE``

An ``up[i]`` not chosen for position ``i`` is backfilled only if its block
would be served from ``up[i]``'s zone once the ``up`` OSDs hold their shards.
That goal is the same zone choice made over every ``up[i]``, the usable
``acting[i]`` and the chosen positions (other strays are left out, so calls
restricted to ``up`` and ``acting``, such as the one from ``Recovered``, reach
the same goal). An ``up[i]`` that already holds shard ``i`` is backfilled too:
outside the acting set it gets no writes, and the log may be trimmed past it
before ``Recovered``. A completed backfill target therefore always joins the
new acting set, as it does with ``calc_ec_acting``, and ``choose_acting`` never
finds ``want == acting`` with fewer backfill targets. An ``up[i]`` placed in a
zone its block will not be served from, for example by a ``pg-upmap-items``
entry into another zone, is not backfilled and the ``pg_temp`` stays.

CRUSH ``bucket_max`` constraints apply: no zone may contribute more than
``size / peering_crush_bucket_target`` OSDs.

.. note::

   This is essentially ``calc_ec_acting`` with the additional CRUSH bucket
   awareness from the stretch path.

**11.5.2 Async Recovery**

``choose_async_recovery_replicated`` checks ``stretch_set_can_peer()`` before
removing an OSD from async recovery. An EC stretch variant must respect shard
identity: removing an OSD must not cause any zone to drop below K shards.

**11.5.3 Stretch Set Validation** (``stretch_set_can_peer``)

For EC pools, additionally verify:

- The acting set includes at least K shards in at least one surviving zone
- In healthy mode, all zones have ``K+M`` shards

11.6 Relationship to ``peering_crush_bucket_*`` Fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing ``pg_pool_t`` fields remain unchanged:

.. list-table::
   :header-rows: 1

   * - Field
     - Purpose
     - EC Stretch Usage
   * - ``peering_crush_bucket_count``
     - Min distinct CRUSH buckets in acting set
     - zones (zones) in healthy; reduced during degraded
   * - ``peering_crush_bucket_target``
     - Target CRUSH buckets for ``bucket_max`` calc
     - zones; not changed by the stretch mode transitions
   * - ``peering_crush_bucket_barrier``
     - CRUSH type level (e.g., datacenter)
     - Same as replica — the failure domain
   * - ``peering_crush_mandatory_member``
     - CRUSH bucket that must be represented
     - Set to surviving zone during degraded mode

**Example values for --num-zones 2, K=2, M=1 (size=6):**

.. list-table::
   :header-rows: 1

   * - Stretch State
     - bucket_count
     - bucket_target
     - bucket_max
     - mandatory_member
   * - Healthy
     - 2
     - 2
     - 3
     - NONE
   * - Degraded
     - 1
     - 2
     - 3
     - surviving_site
   * - Recovery
     - 1
     - 2
     - 3
     - surviving_site
   * - Healthy (restored)
     - 2
     - 2
     - 3
     - NONE

``bucket_max = ceil(size / bucket_target)`` — ``6 / 2 = 3 = K+M``. The stretch
mode transitions do not change ``bucket_target``, so this holds in every state
and naturally prevents more than one copy of each shard per zone, also in the
surviving zone while a zone is down.

11.7 Network Partition Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing Ceph stretch cluster mechanism handles network partitions as
follows:

- **MON-Based Zone Election**: The MONs are distributed between zones (plus a
  tie-break zone). When the inter-zone link is severed, the MONs elect which
  zone continues to operate.
- **OSD Heartbeat Discovery**: OSDs heartbeat the MONs and will discover if
  they are attached to the losing zone. OSDs on the losing zone stop
  processing IO.
- **Lease-Based Read Gating**: A lease determines whether OSDs are permitted to
  process read IOs. The surviving zone must wait for the lease to expire before
  new writes can be processed, ensuring no stale reads are served by the losing
  zone during the transition.

This mechanism does not preclude more complex failure scenarios that may also
need to be treated as zone failures:

- **Asymmetric Network Splits**: OSDs at a remote zone may be able to
  communicate with a zone-local MON even though the MONs have determined a zone
  failover has occurred.
- **Partial Zone Failures**: Loss of a rack or subset of OSDs within a zone may
  warrant treating the zone as failed (e.g., if the remaining shards fall below
  the per-zone ``min_size`` threshold defined in Section 11.2).

11.8 Online OSDs in Offline Zones
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a zone is marked as offline but individual OSDs within that zone remain
reachable, they must be removed from the up set that is presented to the
Objecter and peering state machine. The mechanism ties to the ``min_size``
logic described in Section 11.2:

- **Removal**: If fewer than ``k`` OSDs from a zone are present in the up set,
  the remaining OSDs for that zone are also removed. This forces a zone
  failover and prevents partial-zone IO. Removal from the up set alone is not
  enough: ``calc_ec_acting_stretch`` also serves a zone block from ``acting``
  and strays that hold its shards (Section 11.5.1), so it must also skip
  holders in an offline zone.
- **Reintegration**: When enough OSDs return to meet the per-zone threshold,
  they are added back into the up set. Standard peering will handle resyncing
  data — no new recovery code is required for reintegration.


12. Scrub Behavior — *R1*
-----------------------------

The only modification required to scrub is how it verifies the CRCs received
from each shard during deep scrub. The deep scrub process must:

1. **Verify the coding CRC**, where the EC plugin supports it. This is current
   behavior but must be extended to cover the replicated shards (i.e.,
   verifying the coding relationship within each zone's stripe independently).
2. **Verify cross-replica CRC equivalence**: corresponding shards in each
   replica must have the same CRC. For example, SiteShard 0 at Zone A must
   match SiteShard 0 at Zone B.

Scrub never reads data to the Primary — it only collects and compares CRCs
reported by each OSD. This means inter-zone bandwidth is not a significant
concern, and a primary-centric scrub process continues to make sense for
replicated EC stretch clusters.


13. Migration Path
-------------------

13.1 Pool Migration — *R1*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Migration from an existing EC pool to a replicated EC stretch cluster pool will
leverage the **Pool Migration** design, which is being implemented for the
Umbrella release. This document does not define a separate migration mechanism.

13.2 In-Place Replica Count Modification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ceph osd pool set <pool> num_zones <n>`` changes ``num_zones`` for an existing pool,
between 1 and 2 (see *Changing Per-Pool Zone and Replica Counts* in the stretch mode user
documentation). ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode`` run it on every
pool (Section 2.3.4). ``num_zones`` is set per pool, not in the profile (Section 2.1.5). The
change:

* sets ``size`` to ``num_zones × (k + m)`` for an EC pool. For a replicated pool going to two
  zones it is ``num_zones`` × replicas per zone (``--replica``, else
  ``osd_pool_default_replica``); going to one zone it is ``osd_pool_default_replica``;
* resets ``min_size`` to its per-zone default (Section 11.2);
* gives the pool a CRUSH rule for the new ``num_zones``: the one named with
  ``--crush_rule``, else a generated rule. Going to two zones, the generated rule uses
  ``--zone_failure_domain``, ``--root``, ``--osd_failure_domain`` and ``--class``, else their
  defaults (Section 2.3.2), and an EC pool's rule is named ``<pool>-stretch``. Going to one
  zone, a replicated pool gets the default replicated rule and an EC pool the rule
  ``<pool>-single-zone``. The old rule is removed if no other pool uses it;
* for an EC pool going to two zones, requires FastEC. A legacy EC pool can never be
  stretched, so the change is refused. Its owner can convert the pool first with
  ``ceph osd pool set <pool> allow_ec_optimizations true``; neither this change nor
  ``enable_stretch_mode`` does that for them. (Today the change turns FastEC on itself.);
* sets or clears the ``peering_crush_bucket_*`` fields, and enables or disables stretch mode
  for the first or last pool with two zones (Section 11.4.2).

Today ``--zone_failure_domain`` is required for two zones, and for a replicated pool also
``--replica`` and ``--osd_failure_domain``; they are to take the defaults instead. The standard
recovery process then performs all necessary expansion (or contraction) to match the new
configuration — no manual data migration is required.


14. Implementation Order
-------------------------

This section defines the phased implementation plan, broken into single-sprint
stories.

14.1 R1 — Read-Optimized Stretch Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

R1 delivers a functional EC stretch cluster with zone-local read optimization
(comparable to what Replica stretch clusters provide today). All writes and
recovery traverse the inter-zone link via the Primary.

1. **CRUSH Rule Generation for Replicated EC**
   Implement automatic CRUSH rule creation from the ``num_zones`` and
   ``zone_failure_domain`` pool parameters (Section 4). Validate that the acting
   set places ``k+m`` shards per ``num_zones``.

2. **Pool Parameters: ``num_zones`` and ``zone_failure_domain``**
   Extend pool creation and configuration to accept and validate ``num_zones``
   and ``zone_failure_domain``. Wire these into ``ceph osd pool`` commands
   (Section 2). Neither is part of the EC profile.

3. **Primary-Capable Shard Selection for Replicated EC**
   Extend the non-primary shard selection algorithm to designate the first data
   shard and all parity shards per replica as primary-capable (Section 10.1).

4. **Stretch Mode and Peering for Replicated EC**
   Broken into the following sub-stories (see Sections 11.1–11.6):

   a. **Pool Creation Defaults and Global Stretch Mode Commands** (OSDMonitor):
      Refactor global stretch mode into configuration option defaults for every
      pool creation parameter, ``ceph osd pool default`` to set and show them, and
      ``mon enable_stretch_mode``/``disable_stretch_mode`` commands that change
      the defaults and every existing pool, replicated and EC (Section 2.3).
   c. **Stretch Mode Transitions for EC** (OSDMonitor): Implement
      degraded/recovery/healthy transitions. Update
      ``peering_crush_bucket_count`` and ``peering_crush_mandatory_member``
      on zone failure/recovery; ``min_size`` is not modified
      (Sections 11.4.3–5).
   d. **EC Peering with Stretch Constraints** (PeeringState): Create
      ``calc_ec_acting_stretch`` to respect both shard identity and CRUSH
      ``bucket_max`` constraints. Extend async recovery checks
      (Section 11.5).
   e. **is_recoverable / is_readable for Stretch EC**: Verify and extend
      ``ECRecPred`` and ``ECReadPred`` to account for the larger shard set
      and per-zone constraints (Section 11.5.3).

5. **Primary Write Fan-Out to All Shards (Direct-to-OSD Writes)**
   Extend the Primary write path to encode and distribute ``num_zones × (k + m)``
   shard writes directly to all OSDs in the acting set (Section 8.1).

6. **Direct-to-OSD Reads with Primary Fallback**
   Extend EC Direct Reads to the stretch topology: clients read locally and
   redirect all failures to the Primary (Sections 7.2, 7.3).

7a. **Single-OSD Recovery (Within-Zone)**
    Extend recovery so the Primary can recover a single missing OSD within any
    replica. The Primary reads shards from the affected replica, reconstructs
    the missing shard, and pushes it to the replacement OSD (Section 9.1).

7b. **Full-Zone Recovery**
    Extend recovery to handle full-zone loss: the Primary reads from its local
    (surviving) replica, encodes the full stripe, and pushes all ``k+m`` shards
    to the recovering zone's OSDs. Also covers multi-OSD failures within a
    single zone (Section 9.1).

8. **Scrub CRC Verification for Replicated Shards**
   Extend deep scrub to verify cross-replica CRC equivalence for corresponding
   shards (Section 12).

9. **Network Partition and Zone Failover Integration**
   Validate the existing MON-based zone election and OSD heartbeat mechanisms
   work correctly with the replicated EC acting set (Section 11.7).

10. **End-to-End 2-Zone Integration Testing**
    Full integration test suite covering read, write, recovery, scrub, and
    zone-failover scenarios for a 2-zone (``--num-zones 2``) configuration.

11. **Inter-Zone Statistics**
    Add perf counters tracking cross-zone bytes, operation counts, latency
    histograms, and zone-local vs remote-zone-zone read ratios to give operators visibility
    into inter-zone link utilization (Section 17).

12. **User Documentation**
    Rewrite the stretch mode user documentation to the outline in Section 19.

14.2 Later Release — Recovery Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-zone link bandwidth consumed during recovery, in
priority order.

1. **Cost-Based Recovery Planning**
   Implement the assessment and plan-selection algorithm that compares
   cross-zone transfer costs for each recovery strategy (Section 9.2).

2. **Recovery Plan Message and Zone Primary Execution**
   Define the "Recovery Plan" message format and implement Zone Primary
   execution of delegated recovery within its ``zone``
   (Section 9.3).

3. **Push Recovery Fallback**
   Implement the path where the Primary reconstructs the full object and
   pushes to the Zone Primary with shard-write instructions (Section 9.4.1).

4. **Primary Domain Fallback (Cross-Zone Read for Zone-local Recovery)**
   Allow the Primary to read shards from remote zones when its own domain
   lacks sufficient chunks, only when cost-based planning selects it
   (Section 9.4.2).

14.3 Later Release — Write Bandwidth Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories reduce inter-zone link bandwidth consumed during writes, in
priority order.

1. **Replicate Transaction Message**
   Implement the replicate transaction message that sends raw write data to
   the Zone Primary instead of pre-encoded shards. The Zone Primary
   encodes locally (Section 8.2).

2. **Independent PG Log Generation at Zone Primary**
   Enable the Zone Primary to generate its own PG log entries from the
   replicate transaction, including OBC population and the debug equivalence
   mode (Sections 10.2, 10.3).

3. **Client Writes Direct to Zone Primary**
   Enable clients to write to their local Zone Primary, which stashes the
   data, replicates to the Primary, and fans out locally only after receiving
   write-permission. Includes the write-permission/completion message
   mechanism to maintain global consistency (Section 8.3).

4. **Hybrid Write Path (Replicate + Direct-to-OSD)**
   Support mixed write strategies within a single transaction when zones are
   in different health states (Section 8.4).

14.4 Later Release — Additional Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These stories improve resilience and flexibility but are lower priority than
bandwidth optimizations.

1. **Read from Zone Primary**
   Enable clients to send reads to their local Zone Primary for local
   reconstruction, including the Synchronous Recovery Set mechanism
   (Sections 7.4, 7.4.1).

2. **Direct-to-OSD Failure Redirection to Zone Primary**
   Upgrade direct-read failure handling to redirect to the local Zone
   Primary instead of the global Primary (Section 7.5).

3. **Online OSDs in Offline Zones Handling**
   Implement removal of residual online OSDs from the up set when their zone
   is offline (Section 11.8).

4. **3-Zone (``num_zones=3``) Full Integration Testing**
   Full integration and real-world testing of 3-zone configurations
   (Section 5).


15. Upgrade & Backward Compatibility
--------------------------------------

The upgrade relies on Ceph's encoding conventions. Code that uses a pool's ``num_zones`` never
checks ``require_osd_release``: the encoding carries the compatibility, and only the commands
of Section 15.3 check the release. Raising ``require_osd_release`` to the release with this
design commits the upgrade (Section 15.4). This replaces the OSD feature bit that this section
proposed before.

15.1 Encoding
~~~~~~~~~~~~~

* **Encode follows** ``require_osd_release``. The monitors encode every OSDMap with
  ``OSDMap::get_encoding_features()``. That drops the ``SERVER_<release>`` feature bits of
  releases newer than ``require_osd_release``, and ``pg_pool_t::encode`` picks its struct
  version from the features. Until the commit, every OSDMap is therefore written in the
  earlier release's format, which earlier daemons can read and which allows a downgrade.
* **New fields get a new version.** ``num_zones`` and ``replica`` are encoded only in the
  struct version of the release that ships this design, gated on that release's feature bit.
  On main, version 33 belongs to Umbrella (``shard_mapping`` and the EC shard counts).
  stretchy-C appends ``replica`` and ``num_zones`` to it, which is right only if this design
  ships in Umbrella. Otherwise they move to version 34, gated on the next release's feature
  bit, which main does not define yet.
* **Decode derives what an older version lacks.** For an older version, ``num_zones`` is
  ``peering_crush_bucket_target`` for a stretch pool and 1 otherwise. ``replica`` is
  ``size / num_zones``, and the per-zone ``min_size`` is ``min_size / num_zones``. Every pool
  in memory therefore has ``num_zones`` and ``replica``, whichever format it was read from.
* **Encode in an older format writes the older meaning,** so that decode, encode and decode
  again give the same pool. ``size`` is already the total. ``min_size`` must be written as
  the total, the per-zone ``min_size`` × ``num_zones``. Today stretchy-C writes the per-zone
  value, so each decode of the older format halves it again (2, 1, 0); this must be fixed.
* **Only representable states before the commit.** Until the commit a pool must stay
  representable in the older format. Its ``num_zones`` must equal
  ``peering_crush_bucket_target``, or be 1 for a pool that is not stretched, and no EC pool
  may have ``num_zones`` greater than 1. The commands that could break this are refused until
  then (Section 15.3).

15.2 Maps and Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **OSDMap:** ``stretch_mode_enabled``, ``stretch_bucket_count``, ``stretch_mode_bucket`` and
  the degraded and recovering state keep their encoding. Their meaning changes at the commit,
  from "global stretch mode is enabled" to "a pool has ``num_zones`` greater than 1". Only the
  monitors read them; the OSDs do not.
* **MonMap:** ``stretch_mode_enabled``, the tiebreaker, the disallowed leaders and the
  stretch-marked-down monitors keep their encoding. stretchy-C's
  ``global_stretch_mode_enabled`` (MonMap encoding version 11) was never released. It is
  removed before release, without compatibility code.
* **Configuration:** the pool creation defaults are converted at the commit (Section 2.3.5).
* **Messages and log entries** that later releases add are gated by ``require_osd_release``
  in the same way (Section 10.3).

15.3 During the Upgrade
~~~~~~~~~~~~~~~~~~~~~~~

Before the commit, the configuration still holds the values from before the upgrade, the
cluster can still be downgraded, and upgraded and earlier monitors may be in quorum together.
The monitors therefore keep the earlier release's behaviour, so that a command gives the same
result whichever monitor handles it:

* Pool creation is refused, both ``ceph osd pool create`` and pool creation through librados
  (``POOL_OP_CREATE``). The error asks for ``require_osd_release`` to be raised. Pool creation
  is rare, and refusing it avoids sizing a pool from configuration values that have not been
  converted yet.
* Any use of ``num_zones`` on ``ceph osd pool set`` is an error, even with the value 1.
* ``ceph mon enable_stretch_mode`` and ``disable_stretch_mode`` behave as on main, and
  ``ceph osd pool default set`` is refused. ``ceph osd pool default get`` shows the defaults
  at any time; before the commit they are the values that the commit may still convert.
* Existing pools keep working. Upgraded daemons decode a stretched pool with ``num_zones`` 2,
  and the monitors keep writing it in the earlier format.

.. note::

   **Review required**: refusing pool creation until the upgrade is committed also stops
   components that create pools on demand. For example, RGW creates pools through librados
   (``rgw_tools.cc``). A cluster whose ``require_osd_release`` is left unraised cannot create
   pools at all.

15.4 At the Commit
~~~~~~~~~~~~~~~~~~

``ceph osd require-osd-release <release>`` commits the upgrade. As for any release, it is
refused until every monitor runs the release and every up OSD has its feature bit, unless it is
forced, and after it the cluster cannot be downgraded. With the commit:

1. The next OSDMap is written in the new format, including ``num_zones`` and ``replica``. No
   pool is rewritten: the new format only stores what every daemon already derived.
2. If ``stretch_mode_enabled`` is set, the cluster is in global stretch mode, because earlier
   releases set it only with ``ceph mon enable_stretch_mode``. The monitors then convert the
   pool creation defaults once (Section 2.3.5).
3. The monitors switch to the new behaviour. Pool creation and ``num_zones`` changes are
   allowed, ``ceph osd pool default`` works, the stretch mode commands behave as in Section
   2.3.4, and ``stretch_mode_enabled`` means that a pool is stretched.

15.5 Pools from Earlier Releases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A pool with ``num_zones`` 1 is fully backward compatible. Replicated EC with one zone produces
  a standard ``(k+m)`` acting set, with no new on-disk format and no new wire messages, so
  existing OSDs and clients handle it as a conventional EC pool.
* A stretched replicated pool from an earlier release is decoded with ``num_zones`` set to
  ``peering_crush_bucket_target``, ``replica`` to ``size / num_zones`` and ``min_size``
  counted per zone, and keeps its custom CRUSH rule. Its behaviour does not change.
* No data migration is needed. Pools with ``num_zones`` greater than 1 bring a larger acting
  set (``num_zones × (k + m)`` shards), new CRUSH rules and, in later releases, new inter-OSD
  messages. That is why they can only be created after the commit.

16. Kernel Changes
-------------------

The kernel RBD client (``krbd``) implements its own EC direct read path,
independent of the userspace ``librados`` client. For replicated EC pools, the
``krbd`` direct read logic must be updated to understand the ``num_zones × (k + m)``
shard layout so that it can identify and target zone-local shards within the
client's ``zone``. Without this change, ``krbd`` may attempt direct
reads to shards in a remote zone, negating the locality benefit.

The required modification is small — the shard selection logic needs to account
for the replica stride when choosing which shard to read from — but it is a
kernel-side change and therefore follows the kernel release cycle independently
of the Ceph userspace releases.


17. Inter-Zone Statistics — *R1*
----------------------------------

Operators need visibility into inter-zone link utilization to validate that
the stretch EC design is delivering its locality benefits and to plan capacity.
The following statistics will be exposed per-pool and per-OSD:

- **Cross-zone bytes sent / received**: Total bytes transferred to/from OSDs in
  remote zones, broken down by operation type:

  - Write fan-out (shard data sent to remote zone)
  - Recovery push (shard data sent during recovery)
  - remote-zone read (shard data read from remote zone for reconstruction or
    medium-error recovery)

- **Cross-zone operation counts**: Number of cross-zone sub-operations, broken
  down by type (sub-write, sub-read, recovery push).

- **Cross-zone latency**: Histogram (p50 / p95 / p99) of round-trip latency for
  cross-zone sub-operations, useful for detecting link degradation.

- **local vs remote zone read ratio**: For direct-read-enabled pools, the fraction of
  reads served locally versus those that fell back to the Primary across a zone
  boundary. A high remote-zone ratio indicates a locality problem.

These counters will be exposed through the existing ``ceph perf`` counter
infrastructure and will be queryable via ``ceph tell osd.N perf dump`` and
the Ceph Manager dashboard. They should also be available at the pool level
via ``ceph osd pool stats``.

.. note::

   The inter-zone classification relies on comparing the CRUSH location of the
   source OSD with the destination OSD's ``zone``. This comparison is
   already performed during shard fan-out; the statistics layer adds only
   counter increments on the existing code path.


18. Non-Redundant Pool Zone Affinity
------------------------------------

A stretch cluster topology may additionally host workloads that do not require
multi-zone redundancy, or where redundancy is handled at a higher application
layer. To accommodate this, a non-redundant pool can be configured with zone
affinity. 

This is achieved by setting up the pool to use a specific CRUSH root. When creating the pool, you set the ``num_zones`` to ``1`` (the default) and pass a ``crush_root`` parameter targeting a specific datacenter bucket or zone bucket within your CRUSH hierarchy.

Implementation Details
~~~~~~~~~~~~~~~~~~~~~~

When configuring a pool with affinity to a specific zone, the system generates a 
CRUSH rule that performs a standard ``take <crush_root>`` operation on the 
designated zone bucket.

For instance, if a cluster has two datacenters defined in CRUSH as ``DC1`` and 
``DC2``, configuring a pool with ``crush_root=DC1`` and ``num_zones=1`` will prompt the 
system to generate a CRUSH rule that starts with ``take DC1``, ensuring all 
data for that pool resides completely within that datacenter. This allows non-redundant 
applications to leverage zone-local storage without incurring the latency or bandwidth 
costs of crossing the inter-zone link.


19. User Documentation
----------------------

The stretch mode user documentation (``doc/rados/operations/stretch-mode.rst``)
will be rewritten to this outline, which is still to be agreed:

1. **Introduction**: what a stretch cluster is, and why and when to use one.

   - An explanation of stretch mode.
   - The *Stretch Cluster Issues* section, reworked to explain why stretching a
     pool by hand is hard and stretch mode should be used instead.
   - Part of the *Limitations* section, for example that there are only two
     zones.
   - References to the asynchronous replication alternatives for block, file
     and object.
   - A note that stretch clusters were redesigned in the Vampire release.
   - A note that ``ceph osd pool stretch`` is not the same as a stretch
     cluster.

2. **Configuration prerequisites**: monitors at three sites, including the
   tiebreaker monitor, and the CRUSH zones.

3. **Creating pools**:

   - ``ceph osd pool create`` examples for replicated and EC pools.
   - Mixing local and stretched pools.
   - What happens automatically when ``num_zones`` is 2: a suitable multi-zone
     CRUSH rule is written (unless one is given), stretch mode is enabled, the
     tiebreaker monitor is configured and the monitors switch to the
     connectivity election strategy.

4. **Modifying pools**: changing ``num_zones``, and changing ``min_size``, which
   applies per zone.

5. **Enabling and disabling stretch mode** (global stretch mode):

   - Example commands.
   - What they do: set the configuration options so that future pools are
     stretched by default, and change the existing pools to stretched.
   - Setting the pool creation defaults without changing existing pools
     (Section 2.3.3).
   - For a mixture of pool types, setting ``num_zones`` to 2 on the existing
     pools and giving every parameter when creating new pools may be simpler
     and achieves the same thing.

6. **Other commands**: replacing a failed tiebreaker monitor, forcing recovery
   stretch mode and forcing healthy stretch mode.

7. **CRUSH rules**:

   - The monitors write the CRUSH rule when a stretched pool is created or its
     ``num_zones`` changes, but a pool can be given its own rule instead.
   - Examples of the generated rules, explaining how primaries are placed.
   - An example of an alternative rule that puts all primaries in the same
     zone.
   - Device classes: the current documentation says that they do not work with
     stretch mode. Check whether that has been fixed and document it here.
