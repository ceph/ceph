===============
 Cache Tiering
===============

A cache tier provides Ceph Clients with better I/O performance for a subset of
the data stored in a backing storage tier. Cache tiering involves creating a
pool of relatively fast/expensive storage devices (e.g., solid state drives)
configured to act as a cache tier, and a backing pool of either erasure-coded
or relatively slower/cheaper devices configured to act as an economical storage
tier. The Ceph objecter handles where to place the objects and the tiering
agent determines when to flush objects from the cache to the backing storage
tier. So the cache tier and the backing storage tier are completely transparent 
to Ceph clients.


.. ditaa:: 
           +-------------+
           | Ceph Client |
           +------+------+
                  ^
     Tiering is   |  
    Transparent   |              Faster I/O
        to Ceph   |           +---------------+
     Client Ops   |           |               |   
                  |    +----->+   Cache Tier  |
                  |    |      |               |
                  |    |      +-----+---+-----+
                  |    |            |   ^ 
                  v    v            |   |   Active Data in Cache Tier
           +------+----+--+         |   |
           |   Objecter   |         |   |
           +-----------+--+         |   |
                       ^            |   |   Inactive Data in Storage Tier
                       |            v   |
                       |      +-----+---+-----+
                       |      |               |
                       +----->|  Storage Tier |
                              |               |
                              +---------------+
                                 Slower I/O


The cache tiering agent handles the migration of data between the cache tier 
and the backing storage tier automatically. However, admins have the ability to
configure how this migration takes place. There are two main scenarios: 

- **Writeback Mode:** When admins configure tiers with ``writeback`` mode, Ceph
  clients write data to the cache tier and receive an ACK from the cache tier.
  In time, the data written to the cache tier migrates to the storage tier
  and gets flushed from the cache tier. Conceptually, the cache tier is 
  overlaid "in front" of the backing storage tier. When a Ceph client needs 
  data that resides in the storage tier, the cache tiering agent migrates the
  data to the cache tier on read, then it is sent to the Ceph client. 
  Thereafter, the Ceph client can perform I/O using the cache tier, until the 
  data becomes inactive. This is ideal for mutable data (e.g., photo/video 
  editing, transactional data, etc.).

- **Read-only Mode:** When admins configure tiers with ``readonly`` mode, Ceph
  clients write data to the backing tier. On read, Ceph copies the requested
  object(s) from the backing tier to the cache tier. Stale objects get removed
  from the cache tier based on the defined policy. This approach is ideal 
  for immutable data (e.g., presenting pictures/videos on a social network, 
  DNA data, X-Ray imaging, etc.), because reading data from a cache pool that 
  might contain out-of-date data provides weak consistency. Do not use 
  ``readonly`` mode for mutable data.

And the modes above are accomodated to adapt different configurations:

- **Read-forward Mode:** this mode is the same as the ``writeback`` mode
  when serving write requests. But when Ceph clients is trying to read objects
  not yet copied to the cache tier, Ceph **forward** them to the backing tier by
  replying with a "redirect" message. And the clients will instead turn to the
  backing tier for the data. If the read performance of the backing tier is on
  a par with that of its cache tier, while its write performance or endurance
  falls far behind, this mode might be a better choice.

- **Read-proxy Mode:** this mode is similar to ``readforward`` mode: both
  of them do not promote/copy the data when the requested object does not
  exist in the cache tier. But instead of redirecting the Ceph clients to the
  backing tier when cache misses, the cache tier reads from the backing tier
  on behalf of the clients. Under some circumstances, this mode can help to
  reduce the latency.

Since all Ceph clients can use cache tiering, it has the potential to 
improve I/O performance for Ceph Block Devices, Ceph Object Storage, 
the Ceph Filesystem and native bindings.


Setting Up Pools
================

To set up cache tiering, you must have two pools. One will act as the 
backing storage and the other will act as the cache.


Setting Up a Backing Storage Pool
---------------------------------

Setting up a backing storage pool typically involves one of two scenarios: 

- **Standard Storage**: In this scenario, the pool stores multiple copies
  of an object in the Ceph Storage Cluster.

- **Erasure Coding:** In this scenario, the pool uses erasure coding to 
  store data much more efficiently with a small performance tradeoff.

In the standard storage scenario, you can setup a CRUSH ruleset to establish 
the failure domain (e.g., osd, host, chassis, rack, row, etc.). Ceph OSD 
Daemons perform optimally when all storage drives in the ruleset are of the 
same size, speed (both RPMs and throughput) and type. See `CRUSH Maps`_ 
for details on creating a ruleset. Once you have created a ruleset, create 
a backing storage pool. 

In the erasure coding scenario, the pool creation arguments will generate the
appropriate ruleset automatically. See `Create a Pool`_ for details.

In subsequent examples, we will refer to the backing storage pool 
as ``cold-storage``.


Setting Up a Cache Pool
-----------------------

Setting up a cache pool follows the same procedure as the standard storage
scenario, but with this difference: the drives for the cache tier are typically
high performance drives that reside in their own servers and have their own
ruleset.  When setting up a ruleset, it should take account of the hosts that
have the high performance drives while omitting the hosts that don't. See
`Placing Different Pools on Different OSDs`_ for details.


In subsequent examples, we will refer to the cache pool as ``hot-storage`` and
the backing pool as ``cold-storage``.

For cache tier configuration and default values, see 
`Pools - Set Pool Values`_.


Creating a Cache Tier
=====================

Setting up a cache tier involves associating a backing storage pool with
a cache pool ::

	ceph osd tier add {storagepool} {cachepool}

For example ::

	ceph osd tier add cold-storage hot-storage

To set the cache mode, execute the following::

	ceph osd tier cache-mode {cachepool} {cache-mode}

For example:: 

	ceph osd tier cache-mode hot-storage writeback

The cache tiers overlay the backing storage tier, so they require one
additional step: you must direct all client traffic from the storage pool to 
the cache pool. To direct client traffic directly to the cache pool, execute 
the following:: 

	ceph osd tier set-overlay {storagepool} {cachepool}

For example:: 

	ceph osd tier set-overlay cold-storage hot-storage


Configuring a Cache Tier
========================

Cache tiers have several configuration options. You may set
cache tier configuration options with the following usage:: 

	ceph osd pool set {cachepool} {key} {value}

See `Pools - Set Pool Values`_ for details.


Target Size and Type
--------------------

Ceph's production cache tiers use a `Bloom Filter`_ for the ``hit_set_type``::

	ceph osd pool set {cachepool} hit_set_type bloom

For example::

	ceph osd pool set hot-storage hit_set_type bloom

The ``hit_set_count`` and ``hit_set_period`` define how much time each HitSet
should cover, and how many such HitSets to store. ::

	ceph osd pool set {cachepool} hit_set_count 1
	ceph osd pool set {cachepool} hit_set_period 3600
	ceph osd pool set {cachepool} target_max_bytes 1000000000000

Binning accesses over time allows Ceph to determine whether a Ceph client
accessed an object at least once, or more than once over a time period 
("age" vs "temperature").

The ``min_read_recency_for_promote`` defines how many HitSets to check for the
existence of an object when handling a read operation. The checking result is
used to decide whether to promote the object asynchronously. Its value should be
between 0 and ``hit_set_count``. If it's set to 0, the object is always promoted.
If it's set to 1, the current HitSet is checked. And if this object is in the
current HitSet, it's promoted. Otherwise not. For the other values, the exact
number of archive HitSets are checked. The object is promoted if the object is
found in any of the most recent ``min_read_recency_for_promote`` HitSets.

A similar parameter can be set for the write operation, which is
``min_write_recency_for_promote``. ::

	ceph osd pool set {cachepool} min_read_recency_for_promote 1
	ceph osd pool set {cachepool} min_write_recency_for_promote 1

.. note:: The longer the period and the higher the
   ``min_read_recency_for_promote``/``min_write_recency_for_promote``, the more
   RAM the ``ceph-osd`` daemon consumes. In particular, when the agent is active
   to flush or evict cache objects, all ``hit_set_count`` HitSets are loaded
   into RAM.


Cache Sizing
------------

The cache tiering agent performs two main functions: 

- **Flushing:** The agent identifies modified (or dirty) objects and forwards
  them to the storage pool for long-term storage.
  
- **Evicting:** The agent identifies objects that haven't been modified 
  (or clean) and evicts the least recently used among them from the cache.


Absolute Sizing
~~~~~~~~~~~~~~~

The cache tiering agent can flush or evict objects based upon the total number
of bytes or the total number of objects. To specify a maximum number of bytes,
execute the following::

	ceph osd pool set {cachepool} target_max_bytes {#bytes}

For example, to flush or evict at 1 TB, execute the following::

	ceph osd pool set hot-storage target_max_bytes 1099511627776


To specify the maximum number of objects, execute the following::

	ceph osd pool set {cachepool} target_max_objects {#objects}

For example, to flush or evict at 1M objects, execute the following::

	ceph osd pool set hot-storage target_max_objects 1000000

.. note:: Ceph is not able to determine the size of a cache pool automatically, so
   the configuration on the absolute size is required here, otherwise the
   flush/evict will not work. If you specify both limits, the cache tiering
   agent will begin flushing or evicting when either threshold is triggered.

.. note:: All client requests will be blocked only when  ``target_max_bytes`` or
   ``target_max_objects`` reached

Relative Sizing
~~~~~~~~~~~~~~~

The cache tiering agent can flush or evict objects relative to the size of the
cache pool(specified by ``target_max_bytes`` / ``target_max_objects`` in
`Absolute sizing`_).  When the cache pool consists of a certain percentage of
modified (or dirty) objects, the cache tiering agent will flush them to the
storage pool. To set the ``cache_target_dirty_ratio``, execute the following::

	ceph osd pool set {cachepool} cache_target_dirty_ratio {0.0..1.0}

For example, setting the value to ``0.4`` will begin flushing modified
(dirty) objects when they reach 40% of the cache pool's capacity:: 

	ceph osd pool set hot-storage cache_target_dirty_ratio 0.4

When the dirty objects reaches a certain percentage of its capacity, flush dirty
objects with a higher speed. To set the ``cache_target_dirty_high_ratio``::

	ceph osd pool set {cachepool} cache_target_dirty_high_ratio {0.0..1.0}

For example, setting the value to ``0.6`` will begin aggressively flush dirty objects
when they reach 60% of the cache pool's capacity. obviously, we'd better set the value
between dirty_ratio and full_ratio::

	ceph osd pool set hot-storage cache_target_dirty_high_ratio 0.6

When the cache pool reaches a certain percentage of its capacity, the cache
tiering agent will evict objects to maintain free capacity. To set the 
``cache_target_full_ratio``, execute the following:: 

	ceph osd pool set {cachepool} cache_target_full_ratio {0.0..1.0}

For example, setting the value to ``0.8`` will begin flushing unmodified
(clean) objects when they reach 80% of the cache pool's capacity:: 

	ceph osd pool set hot-storage cache_target_full_ratio 0.8


Cache Age
---------

You can specify the minimum age of an object before the cache tiering agent 
flushes a recently modified (or dirty) object to the backing storage pool::

	ceph osd pool set {cachepool} cache_min_flush_age {#seconds}

For example, to flush modified (or dirty) objects after 10 minutes, execute 
the following:: 

	ceph osd pool set hot-storage cache_min_flush_age 600

You can specify the minimum age of an object before it will be evicted from
the cache tier::

	ceph osd pool {cache-tier} cache_min_evict_age {#seconds}

For example, to evict objects after 30 minutes, execute the following:: 

	ceph osd pool set hot-storage cache_min_evict_age 1800


Removing a Cache Tier
=====================

Removing a cache tier differs depending on whether it is a writeback 
cache or a read-only cache.


Removing a Read-Only Cache
--------------------------

Since a read-only cache does not have modified data, you can disable
and remove it without losing any recent changes to objects in the cache. 

#. Change the cache-mode to ``none`` to disable it. :: 

	ceph osd tier cache-mode {cachepool} none

   For example:: 

	ceph osd tier cache-mode hot-storage none

#. Remove the cache pool from the backing pool. ::

	ceph osd tier remove {storagepool} {cachepool}

   For example::

	ceph osd tier remove cold-storage hot-storage



Removing a Writeback Cache
--------------------------

Since a writeback cache may have modified data, you must take steps to ensure 
that you do not lose any recent changes to objects in the cache before you 
disable and remove it.


#. Change the cache mode to ``forward`` so that new and modified objects will 
   flush to the backing storage pool. ::

	ceph osd tier cache-mode {cachepool} forward

   For example:: 

	ceph osd tier cache-mode hot-storage forward


#. Ensure that the cache pool has been flushed. This may take a few minutes::

	rados -p {cachepool} ls

   If the cache pool still has objects, you can flush them manually. 
   For example::

	rados -p {cachepool} cache-flush-evict-all


#. Remove the overlay so that clients will not direct traffic to the cache. ::

	ceph osd tier remove-overlay {storagetier}

   For example::

	ceph osd tier remove-overlay cold-storage


#. Finally, remove the cache tier pool from the backing storage pool. ::

	ceph osd tier remove {storagepool} {cachepool} 

   For example::

	ceph osd tier remove cold-storage hot-storage


.. _Create a Pool: ../pools#create-a-pool
.. _Pools - Set Pool Values: ../pools#set-pool-values
.. _Placing Different Pools on Different OSDs: ../crush-map/#placing-different-pools-on-different-osds
.. _Bloom Filter: http://en.wikipedia.org/wiki/Bloom_filter
.. _CRUSH Maps: ../crush-map
.. _Absolute Sizing: #absolute-sizing
