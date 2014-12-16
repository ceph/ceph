=======
 Pools
=======

When you first deploy a cluster without creating a pool, Ceph uses the default
pools for storing data. A pool provides you with:

- **Resilience**: You can set how many OSD are allowed to fail without losing data.
  For replicated pools, it is the desired number of copies/replicas of an object. 
  A typical configuration stores an object and one additional copy
  (i.e., ``size = 2``), but you can determine the number of copies/replicas.
  For `erasure coded pools <../erasure-code>`_, it is the number of coding chunks
  (i.e. ``m=2`` in the **erasure code profile**)
  
- **Placement Groups**: You can set the number of placement groups for the pool.
  A typical configuration uses approximately 100 placement groups per OSD to 
  provide optimal balancing without using up too many computing resources. When 
  setting up multiple pools, be careful to ensure you set a reasonable number of
  placement groups for both the pool and the cluster as a whole. 

- **CRUSH Rules**: When you store data in a pool, a CRUSH ruleset mapped to the 
  pool enables CRUSH to identify a rule for the placement of the object 
  and its replicas (or chunks for erasure coded pools) in your cluster. 
  You can create a custom CRUSH rule for your pool.
  
- **Snapshots**: When you create snapshots with ``ceph osd pool mksnap``, 
  you effectively take a snapshot of a particular pool.
  
- **Set Ownership**: You can set a user ID as the owner of a pool. 

To organize data into pools, you can list, create, and remove pools. 
You can also view the utilization statistics for each pool.

List Pools
==========

To list your cluster's pools, execute:: 

	ceph osd lspools

The default pools include:

- ``data``
- ``metadata``
- ``rbd``


.. _createpool:

Create a Pool
=============

Before creating pools, refer to the `Pool, PG and CRUSH Config Reference`_.
Ideally, you should override the default value for the number of placement
groups in you Ceph configuration file, as the default is NOT ideal. 
For example:: 

	osd pool default pg num = 100
	osd pool default pgp num = 100

To create a pool, execute:: 

	ceph osd pool create {pool-name} {pg-num} [{pgp-num}] [replicated] \
             [crush-ruleset-name]
	ceph osd pool create {pool-name} {pg-num}  {pgp-num}   erasure \
             [erasure-code-profile] [crush-ruleset-name]

Where: 

``{pool-name}``

:Description: The name of the pool. It must be unique.
:Type: String
:Required: Yes. Picks up default or Ceph configuration value if not specified.

``{pg-num}``

:Description: The total number of placement groups for the pool. See `Placement
              Groups`_  for details on calculating a suitable number. The 
              default value ``8`` is NOT suitable for most systems.

:Type: Integer
:Required: Yes
:Default: 8

``{pgp-num}``

:Description: The total number of placement groups for placement purposes. This
              **should be equal to the total number of placement groups**, except 
              for placement group splitting scenarios.

:Type: Integer
:Required: Yes. Picks up default or Ceph configuration value if not specified.
:Default: 8

``{replicated|erasure}``

:Description: The pool type which may either be **replicated** to
              recover from lost OSDs by keeping multiple copies of the
              objects or **erasure** to get a kind of
              `generalized RAID5 <../erasure-code>`_ capability.
              The **replicated** pools require more
              raw storage but implement all Ceph operations. The
              **erasure** pools require less raw storage but only
              implement a subset of the available operations.

:Type: String
:Required: No. 
:Default: replicated

``[crush-ruleset-name]``

:Description: The name of the crush ruleset for this pool. If specified ruleset
              doesn't exist, the creation of **replicated** pool will fail with
              -ENOENT. But **replicated** pool will create a new erasure 
              ruleset with specified name.

:Type: String
:Required: No. 
:Default: "erasure-code" for **erasure pool**. Pick up Ceph configuraion variable
          **osd_pool_default_crush_replicated_ruleset** for **replicated** pool.


``[erasure-code-profile=profile]``

.. _erasure code profile: ../erasure-code-profile

:Description: For **erasure** pools only. Use the `erasure code profile`_. It
              must be an existing profile as defined by 
              **osd erasure-code-profile set**.

:Type: String
:Required: No. 

When you create a pool, set the number of placement groups to a reasonable value
(e.g., ``100``). Consider the total number of placement groups per OSD too.
Placement groups are computationally expensive, so performance will degrade when
you have many pools with many placement groups (e.g., 50 pools with 100
placement groups each). The point of diminishing returns depends upon the power
of the OSD host.

See `Placement Groups`_ for details on calculating an appropriate number of
placement groups for your pool.

.. _Placement Groups: ../placement-groups


Set Pool Quotas
===============

You can set pool quotas for the maximum number of bytes and/or the maximum 
number of objects per pool. ::

	ceph osd pool set-quota {pool-name} [max_objects {obj-count}] [max_bytes {bytes}] 

For example:: 

	ceph osd pool set-quota data max_objects 10000

To remove a quota, set its value to ``0``.


Delete a Pool
=============

To delete a pool, execute::

	ceph osd pool delete {pool-name} [{pool-name} --yes-i-really-really-mean-it]

	
If you created your own rulesets and rules for a pool you created,  you should
consider removing them when you no longer need your pool.  If you created users
with permissions strictly for a pool that no longer exists, you should consider
deleting those users too.


Rename a Pool
=============

To rename a pool, execute:: 

	ceph osd pool rename {current-pool-name} {new-pool-name}

If you rename a pool and you have per-pool capabilities for an authenticated 
user, you must update the user's capabilities (i.e., caps) with the new pool
name. 

.. note:: Version ``0.48`` Argonaut and above.

Show Pool Statistics
====================

To show a pool's utilization statistics, execute:: 

	rados df
	

Make a Snapshot of a Pool
=========================

To make a snapshot of a pool, execute:: 

	ceph osd pool mksnap {pool-name} {snap-name}	
	
.. note:: Version ``0.48`` Argonaut and above.


Remove a Snapshot of a Pool
===========================

To remove a snapshot of a pool, execute:: 

	ceph osd pool rmsnap {pool-name} {snap-name}

.. note:: Version ``0.48`` Argonaut and above.	

.. _setpoolvalues:


Set Pool Values
===============

To set a value to a pool, execute the following:: 

	ceph osd pool set {pool-name} {key} {value}
	
You may set values for the following keys: 

``size``

:Description: Sets the number of replicas for objects in the pool. 
              See `Set the Number of Object Replicas`_ for further details. 
              Replicated pools only.

:Type: Integer

``min_size``

:Description: Sets the minimum number of replicas required for I/O.  
              See `Set the Number of Object Replicas`_ for further details. 
              Replicated pools only.

:Type: Integer
:Version: ``0.54`` and above

``crash_replay_interval``

:Description: The number of seconds to allow clients to replay acknowledged, 
              but uncommitted requests.
              
:Type: Integer


``pgp_num``

:Description: The effective number of placement groups to use when calculating 
              data placement.

:Type: Integer
:Valid Range: Equal to or less than ``pg_num``.


``crush_ruleset``

:Description: The ruleset to use for mapping object placement in the cluster.
:Type: Integer


``hashpspool``

:Description: Set/Unset HASHPSPOOL flag on a given pool.
:Type: Integer
:Valid Range: 1 sets flag, 0 unsets flag
:Version: Version ``0.48`` Argonaut and above.	


``hit_set_type``

:Description: Enables hit set tracking for cache pools.
              See `Bloom Filter`_ for additional information.

:Type: String
:Valid Settings: ``bloom``, ``explicit_hash``, ``explicit_object``
:Default: ``bloom``. Other values are for testing.

``hit_set_count``

:Description: The number of hit sets to store for cache pools. The higher 
              the number, the more RAM consumed by the ``ceph-osd`` daemon.

:Type: Integer
:Valid Range: ``1``. Agent doesn't handle > 1 yet.


``hit_set_period``

:Description: The duration of a hit set period in seconds for cache pools. 
              The higher the number, the more RAM consumed by the 
              ``ceph-osd`` daemon.

:Type: Integer
:Example: ``3600`` 1hr


``hit_set_fpp``

:Description: The false positive probability for the ``bloom`` hit set type.
              See `Bloom Filter`_ for additional information.

:Type: Double
:Valid Range: 0.0 - 1.0
:Default: ``0.05``


``cache_target_dirty_ratio``

:Description: The percentage of the cache pool containing modified (dirty) 
              objects before the cache tiering agent will flush them to the
              backing storage pool.
              
:Type: Double
:Default: ``.4``


``cache_target_full_ratio``

:Description: The percentage of the cache pool containing unmodified (clean)
              objects before the cache tiering agent will evict them from the
              cache pool.
             
:Type: Double
:Default: ``.8``


``target_max_bytes``

:Description: Ceph will begin flushing or evicting objects when the 
              ``max_bytes`` threshold is triggered.
              
:Type: Integer
:Example: ``1000000000000``  #1-TB


``target_max_objects`` 

:Description: Ceph will begin flushing or evicting objects when the 
              ``max_objects`` threshold is triggered.

:Type: Integer
:Example: ``1000000`` #1M objects


``cache_min_flush_age``

:Description: The time (in seconds) before the cache tiering agent will flush 
              an object from the cache pool to the storage pool.
              
:Type: Integer
:Example: ``600`` 10min 


``cache_min_evict_age``

:Description: The time (in seconds) before the cache tiering agent will evict
              an object from the cache pool.
              
:Type: Integer
:Example: ``1800`` 30min



Get Pool Values
===============

To get a value from a pool, execute the following:: 

	ceph osd pool get {pool-name} {key}
	
You may get values for the following keys: 

``size``

:Description: Gets the number of replicas for objects in the pool. 
              See `Set the Number of Object Replicas`_ for further details. 
              Replicated pools only.

:Type: Integer

``min_size``

:Description: Gets the minimum number of replicas required for I/O.  
              See `Set the Number of Object Replicas`_ for further details. 
              Replicated pools only.

:Type: Integer
:Version: ``0.54`` and above

``crash_replay_interval``

:Description: The number of seconds to allow clients to replay acknowledged, 
              but uncommitted requests.
              
:Type: Integer


``pgp_num``

:Description: The effective number of placement groups to use when calculating 
              data placement.

:Type: Integer
:Valid Range: Equal to or less than ``pg_num``.


``crush_ruleset``

:Description: The ruleset to use for mapping object placement in the cluster.
:Type: Integer


``hit_set_type``

:Description: Enables hit set tracking for cache pools.
              See `Bloom Filter`_ for additional information.

:Type: String
:Valid Settings: ``bloom``, ``explicit_hash``, ``explicit_object``

``hit_set_count``

:Description: The number of hit sets to store for cache pools. The higher 
              the number, the more RAM consumed by the ``ceph-osd`` daemon.

:Type: Integer


``hit_set_period``

:Description: The duration of a hit set period in seconds for cache pools. 
              The higher the number, the more RAM consumed by the 
              ``ceph-osd`` daemon.

:Type: Integer


``hit_set_fpp``

:Description: The false positive probability for the ``bloom`` hit set type.
              See `Bloom Filter`_ for additional information.

:Type: Double


``cache_target_dirty_ratio``

:Description: The percentage of the cache pool containing modified (dirty) 
              objects before the cache tiering agent will flush them to the
              backing storage pool.
              
:Type: Double


``cache_target_full_ratio``

:Description: The percentage of the cache pool containing unmodified (clean)
              objects before the cache tiering agent will evict them from the
              cache pool.
             
:Type: Double


``target_max_bytes``

:Description: Ceph will begin flushing or evicting objects when the 
              ``max_bytes`` threshold is triggered.
              
:Type: Integer


``target_max_objects`` 

:Description: Ceph will begin flushing or evicting objects when the 
              ``max_objects`` threshold is triggered.

:Type: Integer


``cache_min_flush_age``

:Description: The time (in seconds) before the cache tiering agent will flush 
              an object from the cache pool to the storage pool.
              
:Type: Integer


``cache_min_evict_age``

:Description: The time (in seconds) before the cache tiering agent will evict
              an object from the cache pool.
              
:Type: Integer


Set the Number of Object Replicas
=================================

To set the number of object replicas on a replicated pool, execute the following:: 

	ceph osd pool set {poolname} size {num-replicas}

.. important:: The ``{num-replicas}`` includes the object itself.
   If you want the object and two copies of the object for a total of 
   three instances of the object, specify ``3``.
   
For example:: 

	ceph osd pool set data size 3

You may execute this command for each pool. **Note:** An object might accept 
I/Os in degraded mode with fewer than ``pool size`` replicas.  To set a minimum
number of required replicas for I/O, you should use the ``min_size`` setting.
For example::

  ceph osd pool set data min_size 2

This ensures that no object in the data pool will receive I/O with fewer than
``min_size`` replicas.


Get the Number of Object Replicas
=================================

To get the number of object replicas, execute the following:: 

	ceph osd dump | grep 'replicated size'
	
Ceph will list the pools, with the ``replicated size`` attribute highlighted.
By default, ceph Creates two replicas of an object (a total of three copies, or 
a size of 3).



.. _Pool, PG and CRUSH Config Reference: ../../configuration/pool-pg-config-ref
.. _Bloom Filter: http://en.wikipedia.org/wiki/Bloom_filter
