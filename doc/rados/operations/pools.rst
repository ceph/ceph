.. _rados_pools:

=======
 Pools
=======
Pools are logical partitions that are used to store RADOS objects.

Pools provide:

- **Resilience**: It is possible to plan for the number of OSDs that may
  fail in parallel without data being unavailable or lost. If your cluster
  uses replicated pools, the number of OSDs that can fail in parallel without
  data loss is one less than the number of replicas, and the number that can
  fail without data becoming unavailable is usually two.
  
  For example: a typical configuration stores three replicas
  (copies) of each RADOS object (that is: ``size = 3``), but you can configure
  the number of replicas on a per-pool basis. For `erasure-coded pools
  <../erasure-code>`_, resilience is defined as the number of coding (aka parity) chunks 
  (for example, ``m = 2`` in the default erasure code profile).

- **Placement Groups**: The :ref:`autoscaler <pg-autoscaler>` sets the number
  of placement groups (PGs) for the pool. In a typical configuration, the
  target number of PGs is approximately one-hundred and fifty PGs per OSD. This
  provides reasonable balancing without consuming excessive computing
  resources. When setting up multiple pools, set an appropriate number of PGs
  for each pool and for the cluster as a whole. Each PG belongs to a specific
  pool: when multiple pools use the same OSDs, make sure that the **sum** of PG
  replicas per OSD is in the desired PG-per-OSD target range. See :ref:`Setting
  the Number of Placement Groups <setting the number of placement groups>` for
  instructions on how to manually set the number of placement groups per pool
  (this procedure works only when the autoscaler is not used). A handy calculator
  for various scenarios and combinations of pools, replicas, and target PG
  replicas per OSD is available for guidance at :ref:`pgcalc`.

- **CRUSH Rules**: When data is stored in a pool, the placement of PGs and object
  replicas (or chunks/shards, in the case of erasure-coded pools) in your
  cluster is governed by CRUSH rules. Custom CRUSH rules can be created for a
  pool if the default rule does not fit your use case.

- **Snapshots**: The command ``ceph osd pool mksnap`` creates a snapshot of a
  pool.

Pool Names
==========

Pool names beginning with ``.`` are reserved for use by Ceph's internal
operations. Do not create or manipulate pools with these names.


List Pools
==========

There are multiple ways to list the pools in your cluster.

To list just your cluster's pool names (e.g. when scripting), execute:

.. prompt:: bash $

   ceph osd pool ls

::

   .rgw.root
   default.rgw.log
   default.rgw.control
   default.rgw.meta

To list your cluster's pools with the pool number, run the following command:

.. prompt:: bash $

   ceph osd lspools

::

   1 .rgw.root
   2 default.rgw.log
   3 default.rgw.control
   4 default.rgw.meta

To list your cluster's pools with additional information, execute:

.. prompt:: bash $

   ceph osd pool ls detail

::

   pool 1 '.rgw.root' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 32 pgp_num 32 autoscale_mode on last_change 19 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 2 'default.rgw.log' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 32 pgp_num 32 autoscale_mode on last_change 21 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 3 'default.rgw.control' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 32 pgp_num 32 autoscale_mode on last_change 23 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 4 'default.rgw.meta' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 128 pgp_num 128 autoscale_mode on last_change 25 flags hashpspool stripe_width 0 pg_autoscale_bias 4 application rgw read_balance_score 4.00

To retrieve even more information, you can execute this command with the ``--format`` (or ``-f``) option and the ``json``, ``json-pretty``, ``xml`` or ``xml-pretty`` value.

.. _createpool:

Creating a Pool
===============

Before creating a pool, consult `Pool, PG and CRUSH Config Reference`_. The
Ceph central configuration database contains a default setting
(namely, ``osd_pool_default_pg_num``) that determines the number of PGs assigned
to a new pool if no specific value has been specified. It is possible to change
this value from its default. For more on the subject of setting the number of
PGs per pool, see `setting the number of placement groups`_.

.. note:: In Luminous and later releases, each pool must be associated with the
   application that will be using the pool. For more information, see
   `Associating a Pool with an Application`_ below.

To create a pool, run one of the following commands:

.. prompt:: bash $

    ceph osd pool create {pool-name} [{pg-num} [{pgp-num}]] [replicated] \
             [crush-rule-name] [expected-num-objects]

or:

.. prompt:: bash $

    ceph osd pool create {pool-name} [{pg-num} [{pgp-num}]] erasure \
             [erasure-code-profile] [crush-rule-name] [expected_num_objects] [--autoscale-mode=<on,off,warn>]

For a brief description of the elements of the above commands, consult the
following:

.. describe:: {pool-name}

   The name of the pool. It must be unique.

   :Type: String
   :Required: Yes.

.. describe:: {pg-num}

   The total number of PGs in the pool. For details on calculating an
   appropriate number, see :ref:`placement groups`. The default value is
   NOT suitable for most systems.

  :Type: Integer
  :Required: Yes.
  :Default: 32

.. describe:: {pgp-num}

   The total number of PGs for placement purposes. This **should be equal to
   the total number of PGs**, except briefly while ``pg_num`` is being
   increased or decreased. Note that in releases beginning with Nautilus one
   generally does not change ``pgp_num`` directly:  Ceph will automatically and
   incrementally scale ``pgp_num`` for a given pool when ``pg_num`` for that pool
   has been adjusted.  Adjustments to ``pg_num`` may be either made by the
   PG autoscaler, or if the autoscaler is disabled for a given pool by a manual
   setting via the CLI or dashboard.

  :Type: Integer
  :Required: Yes. If no value has been specified in the command, then the default value is used (unless a different value has been set in Ceph configuration).
  :Default: 32

.. describe:: {replicated|erasure}

   The pool's data protection strategy. This can be either ``replicated``
   (like RAID1 and RAID10) ``erasure (a kind
   of `generalized parity RAID <../erasure-code>`_ strategy like RAID6 but
   more flexible).  A 
   ``replicated`` pool yields less usable capacity for a given amount of
   raw storage but is suitable for all Ceph components and use cases.
   An ``erasure`` (EC) pool often yields more usable capacity than replication
   for a given amount of underlying raw storage, but is only suitable for 
   a subset of Ceph components and use cases.  Depending on the workload and
   the specific profile, EC usually requires more failure domains than
   replication and provides decreased performance, but may tolerate a greater
   number of overlapping drive or host failures.

  :Type: String
  :Required: No.
  :Default: replicated

.. describe:: [crush-rule-name]

   The name of the CRUSH rule to use for this pool. The specified rule must
   already exist, otherwise the command will fail.

   :Type: String
   :Required: No.
   :Default: For ``replicated`` pools, it is by default the rule specified by the :confval:`osd_pool_default_crush_rule` configuration option. This rule must exist.  For ``erasure`` pools, it is the ``erasure-code`` rule if the ``default`` `erasure code profile`_ is used or the ``{pool-name}`` rule  if not. This rule will be created implicitly if it doesn't already exist.

.. describe:: [erasure-code-profile=profile]

   For ``erasure`` pools only. Instructs Ceph to use the specified `erasure
   code profile`_. This profile must be an existing profile as defined via
   the dashboard or invoking ``osd erasure-code-profile set``.  Note that
   changes to the EC profile of a pool after creation do *not* take effect.
   To change the EC profile of an existing pool one must modify the pool to
   use a different CRUSH rule defined with the desired profile.

  :Type: String
  :Required: No.

.. _erasure code profile: ../erasure-code-profile

.. describe:: --autoscale-mode=<on,off,warn>

   - ``on``: the Ceph cluster will autotune changes to the number of PGs in the pool based on actual usage.
   - ``warn``: the Ceph cluster will recommend changes to the number of PGs in the pool based on actual usage.
   - ``off``: refer to :ref:`placement groups` for more information.

  :Type: String
  :Required: No.
  :Default: The default behavior is determined by the :confval:`osd_pool_default_pg_autoscale_mode` option.

.. describe:: [expected-num-objects]

   The expected number of RADOS objects for this pool. By setting this value and
   you arrange for PG splitting to occur at the time of pool creation and
   avoid the latency impact that accompanies runtime folder splitting.

   :Type: Integer
   :Required: No.
   :Default: 0, no splitting at the time of pool creation.

.. _associate-pool-to-application:

Associating a Pool with an Application
======================================

Each pool must be associated with an application before it can be used. Pools
that are intended for use with CephFS and pools that are created automatically
by RGW are associated automatically. Pools that are intended for use with RBD
should be initialized via the dashboard or the ``rbd`` CLI tool (see `Block Device Commands`_ for
more information).

For unusual use cases you can associate a free-form application name to a
pool by running the following command:

.. prompt:: bash $

   ceph osd pool application enable {pool-name} {application-name}

.. note:: CephFS uses the application name ``cephfs``, RBD uses the
   application name ``rbd``, and RGW uses the application name ``rgw``.

Setting Pool Quotas
===================

To set quotas for the maximum number of bytes or the maximum number of
RADOS objects per pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool set-quota {pool-name} [max_objects {obj-count}] [max_bytes {bytes}]

For example:

.. prompt:: bash $

   ceph osd pool set-quota data max_objects 10000

To remove a quota, set its value to ``0``.  Note that you may set a quota only
for bytes or only for RADOS objects, or you can set both.


Deleting a Pool
===============

To delete a pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool delete {pool-name} [{pool-name} --yes-i-really-really-mean-it]

To remove a pool, you must set the ``mon_allow_pool_delete`` flag to ``true``
in central configuration, otherwise the Ceph  monitors will refuse to remove
pools.

For more information, see `Monitor Configuration`_.

.. _Monitor Configuration: ../../configuration/mon-config-ref

If there are custom CRUSH rules that are no longer in use or needed, consider
deleting those rules.

.. prompt:: bash $

   ceph osd pool get {pool-name} crush_rule

For example, if the custom rule is "123", check all pools to see whether they
use the rule by running the following command:

.. prompt:: bash $

    ceph osd dump | grep "^pool" | grep "crush_rule 123"

If no pools use this custom rule, then it is safe to delete the rule from the
cluster.

Similarly, if there are users with permissions restricted to a pool that no
longer exists, consider deleting those users by running commands of the
following forms:

.. prompt:: bash $

    ceph auth ls | grep -C 5 {pool-name}
    ceph auth del {user}


Renaming a Pool
===============

To rename a pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool rename {current-pool-name} {new-pool-name}

If you rename a pool for which an authenticated user has per-pool capabilities,
you must update the user's capabilities ("caps") to refer to the new pool name.


Showing Pool Statistics
=======================

To show a pool's utilization statistics, run the following command:

.. prompt:: bash $

   rados df

To obtain I/O information for a specific pool or for all pools, run a command
of the following form:

.. prompt:: bash $

   ceph osd pool stats [{pool-name}]


Making a Snapshot of a Pool
===========================

To make a snapshot of a pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool mksnap {pool-name} {snap-name}

Removing a Snapshot of a Pool
=============================

To remove a snapshot of a pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool rmsnap {pool-name} {snap-name}

.. _setpoolvalues:

Setting Pool Values
===================

To assign values to a pool's configuration attributes, run a command of the following
form:

.. prompt:: bash $

   ceph osd pool set {pool-name} {key} {value}

You may set values for the following keys:

.. _compression_algorithm:

.. describe:: compression_algorithm
   
   :Description: Sets the inline compression algorithm used in storing data on the underlying BlueStore back end. This key's setting overrides the global setting :confval:`bluestore_compression_algorithm`.
   :Type: String
   :Valid Settings: ``lz4``, ``snappy``, ``zlib``, ``zstd``

.. describe:: compression_mode
   
   :Description: Sets the policy for inline compression when storing data on the underlying BlueStore back end. This key's setting overrides the global setting :confval:`bluestore_compression_mode`.
   :Type: String
   :Valid Settings: ``none``, ``passive``, ``aggressive``, ``force``

.. describe:: compression_min_blob_size

   
   :Description: Sets the minimum size for the compression of chunks: that is, chunks smaller than this are not compressed.  This key's setting overrides the following global settings:
   
   * :confval:`bluestore_compression_min_blob_size` 
   * :confval:`bluestore_compression_min_blob_size_hdd`
   * :confval:`bluestore_compression_min_blob_size_ssd`

   :Type: Unsigned Integer


.. describe:: compression_max_blob_size
   
   :Description: Sets the maximum size for chunks: that is, chunks larger than this are broken into smaller blobs no larger than this size before compression is performed.
   :Type: Unsigned Integer

.. _size:

.. describe:: size
   
   :Description: Sets the number of replicas for objects in the pool. For further details, see `Setting the Number of RADOS Object Replicas`_. This may be set only for ``replicated`` pools. EC pools will _report_ a ``size`` equal to K+M but this value may not be directly _set_.
   :Type: Integer

.. _min_size:

.. describe:: min_size
   
   :Description: Sets the minimum number of active replicas (or shards) required for PGs to be active and thus for I/O operations to proceed.  For further details, see `Setting the Number of RADOS Object Replicas`_.  For erasure-coded pools, this should be set to a value greater than ``K``. If I/O is allowed with only ``K`` shards available, there will be no redundancy and data will be lost in the event of an additional, permanent OSD failure. For more information, see `Erasure Code <../erasure-code>`_
   :Type: Integer
   :Version: ``0.54`` and above

.. _pg_num:

.. describe:: pg_num
   
   :Description: Specifies the total number of PGs for the given pool.  Note that the PG autoscaler, if enabled for a given pool, may override a value manually assigned.
   :Type: Integer
   :Valid Range: ``0`` to ``mon_max_pool_pg_num``. If set to ``0``, the value of ``osd_pool_default_pg_num`` will be used. 

.. _pgp_num:

.. describe:: pgp_num
   
   :Description: Sets the effective number of PGs to use when calculating data placement.  When running a Ceph release beginning with Nautilus, admins do not generally set this value explicitly: Ceph automatically and incrementally scales it up or down to match ``pg_num``.
   :Type: Integer
   :Valid Range: Between ``1`` and the current value of ``pg_num``.

.. _crush_rule:

.. describe:: crush_rule
   
   :Description: Sets the CRUSH rule that Ceph uses to map the pool's RADOS objects to appropriate OSDs.
   :Type: String

.. _allow_ec_overwrites:

.. describe:: allow_ec_overwrites
   
   :Description: Determines whether writes to an erasure-coded pool are allowed to update only part of a RADOS object. This allows CephFS and RBD to use an EC (erasure-coded) pool for user data (but not for metadata). For more details, see `Erasure Coding with Overwrites`_.
   :Type: Boolean

   .. versionadded:: 12.2.0
   
.. describe:: hashpspool

   :Description: Sets or unsets the ``HASHPSPOOL`` flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag

.. _nodelete:

.. describe:: nodelete

   :Description: Sets or unsets the ``NODELETE`` flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _nopgchange:

.. describe:: nopgchange

   :Description: Sets or unsets the ``NOPGCHANGE`` flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _nosizechange:

.. describe:: nosizechange

   :Description: Sets or unsets the ``NOSIZECHANGE`` flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _bulk:

.. describe:: bulk

   :Description: Sets or unsets the ``BULK`` flag on a given pool.
   :Type: Boolean
   :Valid Range: ``true``/``1`` sets flag, ``false``/``0`` unsets flag

.. _write_fadvise_dontneed:

.. describe:: write_fadvise_dontneed

   :Description: Sets or unsets the ``WRITE_FADVISE_DONTNEED`` flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _noscrub:

.. describe:: noscrub

   :Description: Sets or unsets the ``NOSCRUB`` flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _nodeep-scrub:

.. describe:: nodeep-scrub

   :Description: Sets or unsets the ``NODEEP_SCRUB`` flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _target_max_bytes:

.. describe:: target_max_bytes
   
   :Description: Ceph will begin flushing or evicting objects when the
                 ``max_bytes`` threshold is triggered and the deprecated cache tier
		 functionality is in use.
   :Type: Integer
   :Example: ``1000000000000``  #1-TB

.. _target_max_objects:

.. describe:: target_max_objects
   
   :Description: Ceph will begin flushing or evicting objects when the
                 ``max_objects`` threshold is triggered and the deprecated cache tier
		 functionality is in use.
   :Type: Integer
   :Example: ``1000000`` #1M objects

.. _fast_read:

.. describe:: fast_read
   
   :Description: For erasure-coded pools, if this flag is turned ``on``, the
                 read request issues "sub reads" to all shards, and then waits
                 until it receives enough shards to decode before it serves 
                 the client. If *jerasure* or *isa* erasure plugins are in 
                 use, then after the first *K* replies have returned, the 
                 client's request is served immediately using the data decoded 
                 from these replies. This approach sacrifices resources in 
                 exchange for better performance. This flag is supported only 
                 for erasure-coded pools.
   :Type: Boolean 
   :Defaults: ``0``

.. _scrub_min_interval:

.. describe:: scrub_min_interval
   
   :Description: Sets the minimum interval (in seconds) between successive shallow / light scrubs of the pool's PGs when the load is low. If the default value of ``0`` is in effect, then the value of ``osd_scrub_min_interval`` from central config is used.

   :Type: Double
   :Default: ``0``

.. _scrub_max_interval:

.. describe:: scrub_max_interval
   
   :Description: Sets the maximum interval (in seconds) between successive shallow / light scrubs of the pool's PGs regardless of cluster load. If the value of ``scrub_max_interval`` is ``0``, then the value ``osd_scrub_max_interval`` from central config is used.

   :Type: Double
   :Default: ``0``

.. _deep_scrub_interval:

.. describe:: deep_scrub_interval
   
   :Description: Sets the interval (in seconds) for successive pool deep scrubs of the pool's PGs. If the value of ``deep_scrub_interval`` is ``0``, the value ``osd_deep_scrub_interval`` from central config is used.

   :Type: Double
   :Default: ``0``

.. _recovery_priority:

.. describe:: recovery_priority
   
   :Description: Setting this value adjusts a pool's computed reservation priority. This value must be in the range ``-10`` to ``10``. Any pool assigned a negative value will be given a lower priority than any new pools, so users are directed to assign negative values to low-priority pools.

   :Type: Integer
   :Default: ``0``


.. _recovery_op_priority:

.. describe:: recovery_op_priority
   
   :Description: Sets the recovery operation priority for a specific pool's PGs. This overrides the general priority determined by :confval:`osd_recovery_op_priority`.

   :Type: Integer
   :Default: ``0``


Getting Pool Values
===================

To get the value for a given pool's key, run a command of the following form:

.. prompt:: bash $

   ceph osd pool get {pool-name} {key}


You may get values of the following keys:


``size``

:Description: See size_.

:Type: Integer


``min_size``

:Description: See min_size_.

:Type: Integer
:Version: ``0.54`` and above


``pg_num``

:Description: See pg_num_.

:Type: Integer


``pgp_num``

:Description: See pgp_num_.

:Type: Integer
:Valid Range: Equal to or less than ``pg_num``.


``crush_rule``

:Description: See crush_rule_.


``target_max_bytes``

:Description: See target_max_bytes_.

:Type: Integer


``target_max_objects``

:Description: See target_max_objects_.

:Type: Integer


``fast_read``

:Description: See fast_read_.

:Type: Boolean


``scrub_min_interval``

:Description: See scrub_min_interval_.

:Type: Double


``scrub_max_interval``

:Description: See scrub_max_interval_.

:Type: Double


``deep_scrub_interval``

:Description: See deep_scrub_interval_.

:Type: Double


``allow_ec_overwrites``

:Description: See allow_ec_overwrites_.

:Type: Boolean


``recovery_priority``

:Description: See recovery_priority_.

:Type: Integer


``recovery_op_priority``

:Description: See recovery_op_priority_.

:Type: Integer


Setting the Number of RADOS Object Replicas
===========================================

To set the number of data replicas to maintain for a given replicated pool, run a command of the
following form:

.. prompt:: bash $

   ceph osd pool set {poolname} size {num-replicas}

.. important:: The ``{num-replicas}`` argument includes the primary object
   itself.  For example, if you want there to be two replicas of the object in
   addition to the original object (for a total of three instances of the
   object) specify ``3`` by running the following command:

.. prompt:: bash $

   ceph osd pool set data size 3

You may independently run a command like the above for each desired pool. 

.. Note:: A PG might accept I/O in degraded mode with fewer than ``pool
   size`` replicas. To set a minimum number of replicas required for I/O, you
   should use the ``min_size`` setting.  For example, you might run the
   following command:

.. prompt:: bash $

   ceph osd pool set data min_size 2

This command ensures that no object in the data pool will receive I/O if it has
fewer than ``min_size`` (in this case, two) replicas.  Note that setting ``size``
to ``2`` or ``min_size`` to ``1`` in production risks data loss and should only
be done in certain emergency situations, and then only temporarily.


Getting the Number of Object Replicas
=====================================

To get the number of object replicas, run the following command:

.. prompt:: bash $

   ceph osd dump | grep 'replicated size'

Ceph will list pools and highlight the ``replicated size`` attribute.  By
default, Ceph maintains three replicas or copies, for a size of ``3``).

Managing pools that are flagged with ``--bulk``
===============================================
See :ref:`managing_bulk_flagged_pools`.

Setting values for a stretch pool
=================================
To set values for a stretch pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool stretch set {pool-name} {peering_crush_bucket_count} {peering_crush_bucket_target} {peering_crush_bucket_barrier} {crush_rule} {size} {min_size} [--yes-i-really-mean-it]

Here are the break downs of the arguments:

.. describe:: {pool-name}

   The name of the pool. It must be an existing pool: this command doesn't create a new pool.

   :Type: String
   :Required: Yes.

.. describe:: {peering_crush_bucket_count}

   This value is used along with ``peering_crush_bucket_barrier`` to determined whether the set of
   OSDs in the chosen acting set can peer with each other, based on the number of distinct
   buckets there are in the acting set.

   :Type: Integer
   :Required: Yes.

.. describe:: {peering_crush_bucket_target}
   
   This value is used along with ``peering_crush_bucket_barrier`` and ``size`` to calculate
   the value ``bucket_max`` which limits the number of OSDs in the same bucket chosen
   to be in the acting set of a PG.
   
   :Type: Integer
   :Required: Yes.

.. describe:: {peering_crush_bucket_barrier}
      
   The type of CRUSH bucket the pool's PGs are spread among, e.g., ``rack``, ``row``, or ``datacenter``.

   :Type: String
   :Required: Yes.

.. describe:: {crush_rule}
      
   The CRUSH rule to use for the pool. The type of pool must match the type of the CRUSH rule
   (``replicated`` or ``erasure``).

   :Type: String
   :Required: Yes.

.. describe:: {size}
         
   The number of replicas for RADOS objects (and thus PGs) in the pool.
   
   :Type: Integer
   :Required: Yes.

.. describe:: {min_size}
            
   The minimum number of replicas that must be active for IO operations to be
   serviced.

   :Type: Integer
   :Required: Yes.

.. describe:: {--yes-i-really-mean-it}
   
      This flag is required to confirm that you really want to bypass
      safety checks and set the values for a pool, e.g,
      when you are trying to set ``peering_crush_bucket_count`` or 
      ``peering_crush_bucket_target`` to be more than the number of buckets in the crush map.
   
      :Type: Flag
      :Required: No.

.. _setting_values_for_a_stretch_pool:

Unsetting values for a stretch pool
===================================
To move the pool back to non-stretch, run a command of the following form:

.. prompt:: bash $

   ceph osd pool stretch unset {pool-name}

Here are the breakdowns of the arguments:

.. describe:: {pool-name}

   The name of the pool. It must be an existing pool that is stretched,
   i.e., set with the command `ceph osd pool stretch set`.

   :Type: String
   :Required: Yes.

Showing values of a stretch pool
================================
To show values for a stretch pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool stretch show {pool-name}

Here are the break downs of the argument:

.. describe:: {pool-name}

   The name of the pool. It must be an existing pool that is stretched,
   i.e., it has already been set with the command `ceph osd pool stretch set`.

   :Type: String
   :Required: Yes.

.. _Pool, PG and CRUSH Config Reference: ../../configuration/pool-pg-config-ref
.. _Bloom Filter: https://en.wikipedia.org/wiki/Bloom_filter
.. _setting the number of placement groups: ../placement-groups#set-the-number-of-placement-groups
.. _Erasure Coding with Overwrites: ../erasure-code#erasure-coding-with-overwrites
.. _Block Device Commands: ../../../rbd/rados-rbd-cmds/#create-a-block-device-pool
.. _pgcalc: ../pgcalc

