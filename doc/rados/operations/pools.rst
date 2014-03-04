=======
 Pools
=======

When you first deploy a cluster without creating a pool, Ceph uses the default
pools for storing data. A pool provides you with:

- **Resilience**: You can set how many OSD are allowed to fail without loosing data.
  For replicated pools, it is the desired number of copies/replicas of an object. 
  A typical configuration stores an object and one additional copy
  (i.e., ``size = 2``), but you can determine the number of copies/replicas.
  For erasure coded pools, it is the number of coding chunks
  (i.e. ``erasure-code-m=2``)
  
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

	ceph osd pool create {pool-name} {pg-num} [{pgp-num}] [replicated]
	ceph osd pool create {pool-name} {pg-num}  {pgp-num}   erasure \
             [{crush_ruleset=ruleset}] \
             [{erasure-code-directory=directory}] \
             [{erasure-code-plugin=plugin}] \
             [{erasure-code-k=data-chunks}] \
             [{erasure-code-m=coding-chunks}] \
             [{key=value} ...]

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
              objects or **erasure** to get a kind of generalized
              RAID5 capability. The **replicated** pools require more
              raw storage but implement all Ceph operations. The
              **erasure** pools require less raw storage but only
              implement a subset of the available operations.

:Type: String
:Required: No. 
:Default: replicated

``{crush_ruleset=ruleset}``

:Description: For **erasure** pools only. Set the name of the CRUSH
              **ruleset**. It must be an existing ruleset matching
              the requirements of the underlying erasure code plugin. 

:Type: String
:Required: No. 

``{erasure-code-directory=directory}``

:Description: For **erasure** pools only. Set the **directory** name
              from which the erasure code plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``{erasure-code-plugin=plugin}``

:Description: For **erasure** pools only. Use the erasure code **plugin**
              to compute coding chunks and recover missing chunks.

:Type: String
:Required: No. 
:Default: jerasure

``{erasure-code-k=data-chunks}``

:Description: For **erasure** pools using the **jerasure** plugin
              only. Each object is split in **data-chunks** parts,
              each stored on a different OSD.

:Type: Integer
:Required: No. 
:Default: 4

``{erasure-code-m=coding-chunks}``

:Description: For **erasure** pools using the **jerasure** plugin
              only. Compute **coding chunks** for each object and
              store them on different OSDs. The number of coding
              chunks is also the number of OSDs that can be down
              without losing data.

:Type: Integer
:Required: No. 
:Default: 2

``{key=value}``

:Description: For **erasure** pools, the semantic of the remaining
              key/value pairs is defined by the erasure code plugin.
              For **replicated** pools, the key/value pairs are
              ignored.

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

:Description: Sets the number of replicas for objects in the pool. See `Set the Number of Object Replicas`_ for further details. Replicated pools only.
:Type: Integer

``min_size``

:Description: Sets the minimum number of replicas required for io.  See `Set the Number of Object Replicas`_ for further details. Replicated pools only.
:Type: Integer

.. note:: Version ``0.54`` and above

``crash_replay_interval``

:Description: The number of seconds to allow clients to replay acknowledged, but uncommitted requests. 
:Type: Integer


``pgp_num``

:Description: The effective number of placement groups to use when calculating data placement. 
:Type: Integer
:Valid Range: Equal to or less than ``pg_num``.


``crush_ruleset``

:Description: The ruleset to use for mapping object placement in the cluster.
:Type: Integer

``hashpspool``

:Description: Set/Unset HASHPSPOOL flag on a given pool.
:Type: Integer
:Valid Range: 1 sets flag, 0 unsets flag


.. note:: Version ``0.48`` Argonaut and above.	


Get Pool Values
===============

To set a value to a pool, execute the following:: 

	ceph osd pool get {pool-name} {key}
	

``pg_num``

:Description: The number of placement groups for the pool.
:Type: Integer


``pgp_num``

:Description: The effective number of placement groups to use when calculating data placement. 
:Type: Integer
:Valid Range: Equal to or less than ``pg_num``.


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

	ceph osd dump | grep 'rep size'
	
Ceph will list the pools, with the ``rep size`` attribute highlighted.
By default, ceph Creates one replica of an object (a total of two copies, or 
a size of 2).



.. _Pool, PG and CRUSH Config Reference: ../../configuration/pool-pg-config-ref
