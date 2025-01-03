.. _rados_pools:

=======
 Pools
=======
Pools are logical partitions that are used to store objects.

Pools provide:

- **Resilience**: It is possible to set the number of OSDs that are allowed to
  fail without any data being lost. If your cluster uses replicated pools, the
  number of OSDs that can fail without data loss is equal to the number of
  replicas.
  
  For example: a typical configuration stores an object and two replicas
  (copies) of each RADOS object (that is: ``size = 3``), but you can configure
  the number of replicas on a per-pool basis. For `erasure-coded pools
  <../erasure-code>`_, resilience is defined as the number of coding chunks
  (for example, ``m = 2`` in the default **erasure code profile**).

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
  (this procedure works only when the autoscaler is not used).

- **CRUSH Rules**: When data is stored in a pool, the placement of the object
  and its replicas (or chunks, in the case of erasure-coded pools) in your
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

There are multiple ways to get the list of pools in your cluster.

To list just your cluster's pool names (good for scripting), execute:

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

   pool 1 '.rgw.root' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 1 pgp_num 1 autoscale_mode on last_change 19 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 2 'default.rgw.log' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 1 pgp_num 1 autoscale_mode on last_change 21 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 3 'default.rgw.control' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 1 pgp_num 1 autoscale_mode on last_change 23 flags hashpspool stripe_width 0 application rgw read_balance_score 4.00
   pool 4 'default.rgw.meta' replicated size 3 min_size 1 crush_rule 0 object_hash rjenkins pg_num 1 pgp_num 1 autoscale_mode on last_change 25 flags hashpspool stripe_width 0 pg_autoscale_bias 4 application rgw read_balance_score 4.00

To get even more information, you can execute this command with the ``--format`` (or ``-f``) option and the ``json``, ``json-pretty``, ``xml`` or ``xml-pretty`` value.

.. _createpool:

Creating a Pool
===============

Before creating a pool, consult `Pool, PG and CRUSH Config Reference`_. The
Ceph central configuration database in the monitor cluster contains a setting
(namely, ``pg_num``) that determines the number of PGs per pool when a pool has
been created and no per-pool value has been specified. It is possible to change
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

    ceph osd pool create {pool-name} [{pg-num} [{pgp-num}]]   erasure \
             [erasure-code-profile] [crush-rule-name] [expected_num_objects] [--autoscale-mode=<on,off,warn>]

For a brief description of the elements of the above commands, consult the
following:

.. describe:: {pool-name}

   The name of the pool. It must be unique.

   :Type: String
   :Required: Yes.

.. describe:: {pg-num}

   The total number of PGs in the pool. For details on calculating an
   appropriate number, see :ref:`placement groups`. The default value ``8`` is
   NOT suitable for most systems.

  :Type: Integer
  :Required: Yes.
  :Default: 8

.. describe:: {pgp-num}

   The total number of PGs for placement purposes. This **should be equal to
   the total number of PGs**, except briefly while ``pg_num`` is being
   increased or decreased. 

  :Type: Integer
  :Required: Yes. If no value has been specified in the command, then the default value is used (unless a different value has been set in Ceph configuration).
  :Default: 8

.. describe:: {replicated|erasure}

   The pool type. This can be either **replicated** (to recover from lost OSDs
   by keeping multiple copies of the objects) or **erasure** (to achieve a kind
   of `generalized parity RAID <../erasure-code>`_ capability).  The
   **replicated** pools require more raw storage but can implement all Ceph
   operations. The **erasure** pools require less raw storage but can perform
   only some Ceph tasks and may provide decreased performance.

  :Type: String
  :Required: No.
  :Default: replicated

.. describe:: [crush-rule-name]

   The name of the CRUSH rule to use for this pool. The specified rule must
   exist; otherwise the command will fail.

   :Type: String
   :Required: No.
   :Default: For **replicated** pools, it is the rule specified by the :confval:`osd_pool_default_crush_rule` configuration variable. This rule must exist.  For **erasure** pools, it is the ``erasure-code`` rule if the ``default`` `erasure code profile`_ is used or the ``{pool-name}`` rule  if not. This rule will be created implicitly if it doesn't already exist.

.. describe:: [erasure-code-profile=profile]

   For **erasure** pools only. Instructs Ceph to use the specified `erasure
   code profile`_. This profile must be an existing profile as defined by **osd
   erasure-code-profile set**.

  :Type: String
  :Required: No.

.. _erasure code profile: ../erasure-code-profile

.. describe:: --autoscale-mode=<on,off,warn>

   - ``on``: the Ceph cluster will autotune or recommend changes to the number of PGs in your pool based on actual usage.
   - ``warn``: the Ceph cluster will autotune or recommend changes to the number of PGs in your pool based on actual usage.
   - ``off``: refer to :ref:`placement groups` for more information.

  :Type: String
  :Required: No.
  :Default: The default behavior is determined by the :confval:`osd_pool_default_pg_autoscale_mode` option.

.. describe:: [expected-num-objects]

   The expected number of RADOS objects for this pool. By setting this value and
   assigning a negative value to **filestore merge threshold**, you arrange
   for the PG folder splitting to occur at the time of pool creation and
   avoid the latency impact that accompanies runtime folder splitting.

   :Type: Integer
   :Required: No.
   :Default: 0, no splitting at the time of pool creation.

.. _associate-pool-to-application:

Associating a Pool with an Application
======================================

Pools need to be associated with an application before they can be used. Pools
that are intended for use with CephFS and pools that are created automatically
by RGW are associated automatically. Pools that are intended for use with RBD
should be initialized with the ``rbd`` tool (see `Block Device Commands`_ for
more information).

For other cases, you can manually associate a free-form application name to a
pool by running the following command.:

.. prompt:: bash $

   ceph osd pool application enable {pool-name} {application-name}

.. note:: CephFS uses the application name ``cephfs``, RBD uses the
   application name ``rbd``, and RGW uses the application name ``rgw``.

Setting Pool Quotas
===================

To set pool quotas for the maximum number of bytes and/or the maximum number of
RADOS objects per pool, run the following command:

.. prompt:: bash $

   ceph osd pool set-quota {pool-name} [max_objects {obj-count}] [max_bytes {bytes}]

For example:

.. prompt:: bash $

   ceph osd pool set-quota data max_objects 10000

To remove a quota, set its value to ``0``.


Deleting a Pool
===============

To delete a pool, run a command of the following form:

.. prompt:: bash $

   ceph osd pool delete {pool-name} [{pool-name} --yes-i-really-really-mean-it]

To remove a pool, you must set the ``mon_allow_pool_delete`` flag to ``true``
in the monitor's configuration. Otherwise, monitors will refuse to remove
pools.

For more information, see `Monitor Configuration`_.

.. _Monitor Configuration: ../../configuration/mon-config-ref

If there are custom rules for a pool that is no longer needed, consider
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

To assign values to a pool's configuration keys, run a command of the following
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
   
   :Description: Sets the policy for the inline compression algorithm used in storing data on the underlying BlueStore back end. This key's setting overrides the global setting :confval:`bluestore_compression_mode`.
   :Type: String
   :Valid Settings: ``none``, ``passive``, ``aggressive``, ``force``

.. describe:: compression_min_blob_size

   
   :Description: Sets the minimum size for the compression of chunks: that is, chunks smaller than this are not compressed.  This key's setting overrides the following global settings:
   
   * :confval:`bluestore_compression_min_blob_size` 
   * :confval:`bluestore_compression_min_blob_size_hdd`
   * :confval:`bluestore_compression_min_blob_size_ssd`

   :Type: Unsigned Integer


.. describe:: compression_max_blob_size
   
   :Description: Sets the maximum size for chunks: that is, chunks larger than this are broken into smaller blobs of this size before compression is performed.
   :Type: Unsigned Integer

.. _size:

.. describe:: size
   
   :Description: Sets the number of replicas for objects in the pool. For further details, see `Setting the Number of RADOS Object Replicas`_. Replicated pools only.
   :Type: Integer

.. _min_size:

.. describe:: min_size
   
   :Description: Sets the minimum number of replicas required for I/O.  For further details, see `Setting the Number of RADOS Object Replicas`_.  For erasure-coded pools, this should be set to a value greater than 'k'. If I/O is allowed at the value 'k', then there is no redundancy and data will be lost in the event of a permanent OSD failure. For more information, see `Erasure Code <../erasure-code>`_
   :Type: Integer
   :Version: ``0.54`` and above

.. _pg_num:

.. describe:: pg_num
   
   :Description: Sets the effective number of PGs to use when calculating data placement.
   :Type: Integer
   :Valid Range: ``0`` to ``mon_max_pool_pg_num``. If set to ``0``, the value of ``osd_pool_default_pg_num`` will be used. 

.. _pgp_num:

.. describe:: pgp_num
   
   :Description: Sets the effective number of PGs to use when calculating data placement.
   :Type: Integer
   :Valid Range: Between ``1`` and the current value of ``pg_num``.

.. _crush_rule:

.. describe:: crush_rule
   
   :Description: Sets the CRUSH rule that Ceph uses to map object placement within the pool.
   :Type: String

.. _allow_ec_overwrites:

.. describe:: allow_ec_overwrites
   
   :Description: Determines whether writes to an erasure-coded pool are allowed to update only part of a RADOS object. This allows CephFS and RBD to use an EC (erasure-coded) pool for user data (but not for metadata). For more details, see `Erasure Coding with Overwrites`_.
   :Type: Boolean

   .. versionadded:: 12.2.0
   
.. describe:: hashpspool

   :Description: Sets and unsets the HASHPSPOOL flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag

.. _nodelete:

.. describe:: nodelete

   :Description: Sets and unsets the NODELETE flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _nopgchange:

.. describe:: nopgchange

   :Description: Sets and unsets the NOPGCHANGE flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _nosizechange:

.. describe:: nosizechange

   :Description: Sets and unsets the NOSIZECHANGE flag on a given pool.
   :Type: Integer
   :Valid Range: 1 sets flag, 0 unsets flag
   :Version: Version ``FIXME``

.. _bulk:

.. describe:: bulk

   :Description: Sets and unsets the bulk flag on a given pool.
   :Type: Boolean
   :Valid Range: ``true``/``1`` sets flag, ``false``/``0`` unsets flag

.. _write_fadvise_dontneed:

.. describe:: write_fadvise_dontneed

   :Description: Sets and unsets the WRITE_FADVISE_DONTNEED flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _noscrub:

.. describe:: noscrub

   :Description: Sets and unsets the NOSCRUB flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _nodeep-scrub:

.. describe:: nodeep-scrub

   :Description: Sets and unsets the NODEEP_SCRUB flag on a given pool.
   :Type: Integer
   :Valid Range: ``1`` sets flag, ``0`` unsets flag

.. _target_max_bytes:

.. describe:: target_max_bytes
   
   :Description: Ceph will begin flushing or evicting objects when the
                 ``max_bytes`` threshold is triggered.
   :Type: Integer
   :Example: ``1000000000000``  #1-TB

.. _target_max_objects:

.. describe:: target_max_objects
   
   :Description: Ceph will begin flushing or evicting objects when the
                 ``max_objects`` threshold is triggered.
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
   
   :Description: Sets the minimum interval (in seconds) for successive scrubs of the pool's PGs when the load is low. If the default value of ``0`` is in effect, then the value of ``osd_scrub_min_interval`` from central config is used.

   :Type: Double
   :Default: ``0``

.. _scrub_max_interval:

.. describe:: scrub_max_interval
   
   :Description: Sets the maximum interval (in seconds) for scrubs of the pool's PGs regardless of cluster load. If the value of ``scrub_max_interval`` is ``0``, then the value ``osd_scrub_max_interval`` from central config is used.

   :Type: Double
   :Default: ``0``

.. _deep_scrub_interval:

.. describe:: deep_scrub_interval
   
   :Description: Sets the interval (in seconds) for pool “deep” scrubs of the pool's PGs. If the value of ``deep_scrub_interval`` is ``0``, the value ``osd_deep_scrub_interval`` from central config is used.

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

To get a value from a pool's key, run a command of the following form:

.. prompt:: bash $

   ceph osd pool get {pool-name} {key}


You may get values from the following keys:


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

To set the number of data replicas on a replicated pool, run a command of the
following form:

.. prompt:: bash $

   ceph osd pool set {poolname} size {num-replicas}

.. important:: The ``{num-replicas}`` argument includes the primary object
   itself.  For example, if you want there to be two replicas of the object in
   addition to the original object (for a total of three instances of the
   object) specify ``3`` by running the following command:

.. prompt:: bash $

   ceph osd pool set data size 3

You may run the above command for each pool. 

.. Note:: An object might accept I/Os in degraded mode with fewer than ``pool
   size`` replicas. To set a minimum number of replicas required for I/O, you
   should use the ``min_size`` setting.  For example, you might run the
   following command:

.. prompt:: bash $

   ceph osd pool set data min_size 2

This command ensures that no object in the data pool will receive I/O if it has
fewer than ``min_size`` (in this case, two) replicas.


Getting the Number of Object Replicas
=====================================

To get the number of object replicas, run the following command:

.. prompt:: bash $

   ceph osd dump | grep 'replicated size'

Ceph will list pools and highlight the ``replicated size`` attribute.  By
default, Ceph creates two replicas of an object (a total of three copies, for a
size of ``3``).

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

   The name of the pool. It must be an existing pool, this command doesn't create a new pool.

   :Type: String
   :Required: Yes.

.. describe:: {peering_crush_bucket_count}

   The value is used along with peering_crush_bucket_barrier to determined whether the set of
   OSDs in the chosen acting set can peer with each other, based on the number of distinct
   buckets there are in the acting set.

   :Type: Integer
   :Required: Yes.

.. describe:: {peering_crush_bucket_target}
   
   This value is used along with peering_crush_bucket_barrier and size to calculate
   the value bucket_max which limits the number of OSDs in the same bucket from getting chose to be in the acting set of a PG.
   
   :Type: Integer
   :Required: Yes.

.. describe:: {peering_crush_bucket_barrier}
      
   The type of bucket a pool is stretched across, e.g., rack, row, or datacenter.

   :Type: String
   :Required: Yes.

.. describe:: {crush_rule}
      
   The crush rule to use for the stretch pool. The type of pool must match the type of crush_rule
   (replicated or erasure).

   :Type: String
   :Required: Yes.

.. describe:: {size}
         
   The number of replicas for objects in the stretch pool.
   
   :Type: Integer
   :Required: Yes.

.. describe:: {min_size}
            
   The minimum number of replicas required for I/O in the stretch pool.

   :Type: Integer
   :Required: Yes.

.. describe:: {--yes-i-really-mean-it}
   
      This flag is required to confirm that you really want to by-pass
      the safety checks and set the values for a stretch pool, e.g,
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

Here are the break downs of the argument:

.. describe:: {pool-name}

   The name of the pool. It must be an existing pool that is stretched,
   i.e., it has already been set with the command `ceph osd pool stretch set`.

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
