===============
 Cache Tiering
===============

.. warning:: Cache tiering was deprecated in the Reef release and has lacked
   a maintainer for a long time. It will be removed in a future release without
   notice. **Do not deploy new cache tiers.** Migrate existing cache
   tier deployments as soon as possible.

.. note:: This documentation is retained for existing deployments only.
   If you need to remove a cache tier, see :ref:`cache-tiering-removal`.
   Some community members have adopted ``dm-cache`` (the Linux kernel's
   device-mapper cache target) as a block-level caching alternative, though
   this is not an officially supported or endorsed configuration.

A cache tier provides Ceph clients with better I/O performance for a subset of
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


The cache tiering agent flushes modified objects from the cache tier to the
backing storage tier and evicts unmodified objects from the cache tier. The
OSDs of the cache tier decide when to promote objects from the backing storage
tier into the cache tier. You choose how the tiers behave by setting the
``cache-mode``:

- **writeback** mode: After ``ceph osd tier set-overlay``, Ceph clients send
  all reads and writes for the backing storage pool to the cache tier. A
  request for an object that is in the cache tier is completed by the cache
  tier, which also acknowledges the writes. A request for an object that is not
  in the cache tier is proxied to the backing storage tier, and the object
  might then be promoted into the cache tier. Promotion depends on the cache
  pool's ``min_read_recency_for_promote`` value for reads and its
  ``min_write_recency_for_promote`` value for writes (see
  `Target Size and Type`_). If the cache pool has no HitSets, or if the
  operation requires promotion, the object is promoted before the request is
  served. This mode is well-suited for mutable data (for example, photo or
  video editing, or transactional data).

  The ``osd_tier_default_cache_min_read_recency_for_promote`` and
  ``osd_tier_default_cache_min_write_recency_for_promote`` options set the two
  recency values only when a cache tier is created with
  ``ceph osd tier add-cache``.

- **readproxy** mode: Reads of objects that are not in the cache tier are
  proxied to the base tier, and those objects are not promoted unless an
  operation requires it. A write to an object that is not in the cache tier
  first promotes the object into the cache tier, with no HitSet check.

- **proxy** mode: Reads and writes of objects that are not in the cache tier
  are proxied to the base tier, and the objects are not promoted unless an
  operation requires it (for example, a partial write when the base pool is
  erasure-coded). Objects that are already in the cache tier are still read
  and written there. Use this mode to drain a ``writeback`` cache before you
  remove it (see :ref:`cache-tiering-removal`).

Other cache modes are:

- **readonly** promotes objects to the cache on read operations only; write
  operations are forwarded to the base tier. This mode is intended for
  read-only workloads that do not require consistency to be enforced by the
  storage system. (**Warning**: when objects are updated in the base tier,
  Ceph makes **no** attempt to sync these updates to the corresponding objects
  in the cache. Since this mode is considered experimental, a
  ``--yes-i-really-mean-it`` option must be passed in order to enable it.)

- **none** is used to completely disable caching.


A word of caution
=================

Cache tiering will *degrade* performance for most workloads.  Users should use
extreme caution before using this feature.

* *Workload dependent*: Whether a cache will improve performance is
  highly dependent on the workload.  Because there is a cost
  associated with moving objects into or out of the cache, it can only
  be effective when there is a *large skew* in the access pattern in
  the data set, such that most of the requests touch a small number of
  objects.  The cache pool should be large enough to capture the
  working set for your workload to avoid thrashing.

* *Difficult to benchmark*: Most benchmarks that users run to measure
  performance will show terrible performance with cache tiering, in
  part because very few of them skew requests toward a small set of
  objects, it can take a long time for the cache to "warm up," and
  because the warm-up cost can be high.

* *Usually slower*: For workloads that are not cache tiering-friendly,
  performance is often slower than a normal RADOS pool without cache
  tiering enabled.

* *librados object enumeration*: The librados-level object enumeration
  API is not meant to be coherent in the presence of the cache.  If
  your application is using librados directly and relies on object
  enumeration, cache tiering will probably not work as expected.
  (This is not a problem for RGW, RBD, or CephFS.)

* *Complexity*: Enabling cache tiering means that a lot of additional
  machinery and complexity within the RADOS cluster is being used.
  This increases the probability that you will encounter a bug in the system
  that other users have not yet encountered and will put your deployment at a
  higher level of risk.

Known Good Workloads
--------------------

* *RGW time-skewed*: If the RGW workload is such that almost all read
  operations are directed at recently written objects, a simple cache
  tiering configuration that destages recently written objects from
  the cache to the base tier after a configurable period can work
  well.

Known Bad Workloads
-------------------

The following configurations are *known to work poorly* with cache
tiering.

* *RBD with replicated cache and erasure-coded base*: This is a common
  request, but usually does not perform well.  Even reasonably skewed
  workloads still send some small writes to cold objects. When the base tier
  is erasure-coded, the cache tier promotes the whole object (4 MiB by default
  for RBD) into the cache before it applies a small (often 4 KiB) write, even
  if ``allow_ec_overwrites`` is enabled on the base pool.  Only a handful of
  users have successfully deployed this configuration, and it only works for
  them because their data is extremely cold (backups) and they are not in
  any way sensitive to performance.

* *RBD with replicated cache and base*: RBD with a replicated base
  tier does better than when the base is erasure coded, but it is
  still highly dependent on the amount of skew in the workload, and
  very difficult to validate.  The user will need to have a good
  understanding of their workload and will need to tune the cache
  tiering parameters carefully.


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

In the standard storage scenario, you can setup a CRUSH rule to establish 
the failure domain (e.g., osd, host, chassis, rack, row, etc.). Ceph OSD 
Daemons perform optimally when all storage drives in the rule are of the 
same size, speed (both RPMs and throughput) and type. See :ref:`rados-crush-map`
for details on creating a rule. Once you have created a rule, create 
a backing storage pool. 

In the erasure coding scenario, the pool creation arguments will generate the
appropriate rule automatically. See :ref:`createpool` for details.

In subsequent examples, we will refer to the backing storage pool 
as ``cold-storage``.


Setting Up a Cache Pool
-----------------------

Setting up a cache pool follows the same procedure as the standard storage
scenario, but with this difference: the drives for the cache tier are typically
high performance drives that reside in their own servers and have their own
CRUSH rule.  When setting up such a rule, it should take account of the hosts
that have the high performance drives while omitting the hosts that don't. See
:ref:`CRUSH device classes <device_classes>` for details.


In subsequent examples, we will refer to the cache pool as ``hot-storage`` and
the backing pool as ``cold-storage``.


Creating a Cache Tier
=====================

Setting up a cache tier involves associating a backing storage pool with
a cache pool:

.. prompt:: bash $

   ceph osd tier add {storagepool} {cachepool}

For example:

.. prompt:: bash $

   ceph osd tier add cold-storage hot-storage

To set the cache mode, execute the following:

.. prompt:: bash $

   ceph osd tier cache-mode {cachepool} {cache-mode}

For example:

.. prompt:: bash $

   ceph osd tier cache-mode hot-storage writeback

The cache tiers overlay the backing storage tier, so they require one
additional step: you must direct all client traffic from the storage pool to 
the cache pool. To direct client traffic directly to the cache pool, execute 
the following:

.. prompt:: bash $

   ceph osd tier set-overlay {storagepool} {cachepool}

For example:

.. prompt:: bash $

   ceph osd tier set-overlay cold-storage hot-storage


Configuring a Cache Tier
========================

Cache tiers have several configuration options. You may set
cache tier configuration options with the following usage:

.. prompt:: bash $

   ceph osd pool set {cachepool} {key} {value}
   
See :ref:`setpoolvalues` for details.


Target Size and Type
--------------------

Ceph's production cache tiers use a `Bloom Filter`_ for the ``hit_set_type``:

.. prompt:: bash $

   ceph osd pool set {cachepool} hit_set_type bloom

For example:

.. prompt:: bash $

   ceph osd pool set hot-storage hit_set_type bloom

The ``hit_set_count`` and ``hit_set_period`` define how many such HitSets to
store, and how much time each HitSet should cover:

.. prompt:: bash $

   ceph osd pool set {cachepool} hit_set_count 12
   ceph osd pool set {cachepool} hit_set_period 14400
   ceph osd pool set {cachepool} target_max_bytes 1000000000000

.. note:: A larger ``hit_set_count`` results in more RAM consumed by
          the ``ceph-osd`` process.

Binning accesses over time allows Ceph to determine whether a Ceph client
accessed an object at least once, or more than once over a time period 
("age" vs "temperature").

The ``min_read_recency_for_promote`` defines how many HitSets to check for the
existence of an object when handling a read operation. The checking result is
used to decide whether to promote the object asynchronously. Its value should be
between 0 and ``hit_set_count``. If it's set to 0, the object is promoted
without a HitSet check. If it's set to 1, the object is promoted only if it is
in the current HitSet. For a higher value N, the object is promoted only if it
is in each of the N most recent HitSets, counting the current one. In every
case, promotion is also subject to the OSD's promotion rate limits
(``osd_tier_promote_max_objects_sec`` and ``osd_tier_promote_max_bytes_sec``).

A similar parameter can be set for the write operation, which is
``min_write_recency_for_promote``:

.. prompt:: bash $

   ceph osd pool set {cachepool} min_read_recency_for_promote 2
   ceph osd pool set {cachepool} min_write_recency_for_promote 2

.. note:: The longer the ``hit_set_period`` and the higher the
   ``hit_set_count``, the more RAM the ``ceph-osd`` daemon uses. For each
   placement group of the cache pool, the primary OSD keeps up to
   ``hit_set_count`` recent archived HitSets in memory, and it loads all of
   them while the agent is evicting objects.


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
execute the following:

.. prompt:: bash $

   ceph osd pool set {cachepool} target_max_bytes {#bytes}

For example, to flush or evict at 1 TiB, run the following command:

.. prompt:: bash $

   ceph osd pool set hot-storage target_max_bytes 1099511627776

To specify the maximum number of objects, execute the following:

.. prompt:: bash $

   ceph osd pool set {cachepool} target_max_objects {#objects}

For example, to flush or evict at 1M objects, execute the following:

.. prompt:: bash $

   ceph osd pool set hot-storage target_max_objects 1000000

.. note:: Ceph is not able to determine the size of a cache pool automatically, so
   the configuration on the absolute size is required here, otherwise the
   flush/evict will not work. If you specify both limits, the cache tiering
   agent will begin flushing or evicting when either threshold is triggered.

.. note:: When the cache pool goes over ``target_max_bytes`` or
   ``target_max_objects``, writes to objects that are not in the cache tier
   are blocked in ``writeback`` and ``readproxy`` modes until the agent has
   evicted enough objects. Requests for objects that are already in the cache
   tier continue, and reads of objects that are not in the cache tier are
   proxied to the base tier instead of being blocked, unless the read requires
   promotion.

Relative Sizing
~~~~~~~~~~~~~~~

The cache tiering agent can flush or evict objects relative to the size of the
cache pool(specified by ``target_max_bytes`` / ``target_max_objects`` in
`Absolute sizing`_).  When the cache pool consists of a certain percentage of
modified (or dirty) objects, the cache tiering agent will flush them to the
storage pool. To set the ``cache_target_dirty_ratio``, execute the following:

.. prompt:: bash $

   ceph osd pool set {cachepool} cache_target_dirty_ratio {0.0..1.0}

For example, setting the value to ``0.4`` will begin flushing modified
(dirty) objects when they reach 40% of the cache pool's capacity:

.. prompt:: bash $

   ceph osd pool set hot-storage cache_target_dirty_ratio 0.4

When the dirty objects reaches a certain percentage of its capacity, flush dirty
objects with a higher speed. To set the ``cache_target_dirty_high_ratio``:

.. prompt:: bash $

   ceph osd pool set {cachepool} cache_target_dirty_high_ratio {0.0..1.0}

For example, setting the value to ``0.6`` will begin aggressively flush dirty
objects when they reach 60% of the cache pool's capacity. Obviously, we'd
better set the value between dirty_ratio and full_ratio:

.. prompt:: bash $

   ceph osd pool set hot-storage cache_target_dirty_high_ratio 0.6

When the cache pool reaches a certain percentage of its capacity, the cache
tiering agent will evict objects to maintain free capacity. To set the 
``cache_target_full_ratio``, execute the following:

.. prompt:: bash $

   ceph osd pool set {cachepool} cache_target_full_ratio {0.0..1.0}

For example, setting the value to ``0.8`` makes the agent begin evicting
unmodified (clean) objects when the cache pool reaches 80% of its target size:

.. prompt:: bash $

   ceph osd pool set hot-storage cache_target_full_ratio 0.8


Cache Age
---------

You can specify the minimum age of an object before the cache tiering agent 
flushes a recently modified (or dirty) object to the backing storage pool:

.. prompt:: bash $

   ceph osd pool set {cachepool} cache_min_flush_age {#seconds}

For example, to flush modified (or dirty) objects after 10 minutes, execute the
following:

.. prompt:: bash $

   ceph osd pool set hot-storage cache_min_flush_age 600

You can specify the minimum age of an object before it will be evicted from the
cache tier:

.. prompt:: bash $

   ceph osd pool set {cache-tier} cache_min_evict_age {#seconds}

For example, to evict objects after 30 minutes, execute the following:

.. prompt:: bash $

   ceph osd pool set hot-storage cache_min_evict_age 1800


.. _cache-tiering-removal:

Removing a Cache Tier
=====================

Removing a cache tier differs depending on whether it is a writeback
cache or a read-only cache.


Removing a Read-Only Cache
--------------------------

Since a read-only cache does not have modified data, you can disable
and remove it without losing any recent changes to objects in the cache. 

#. Remove the overlay so that clients will not direct traffic to the cache:

   .. prompt:: bash $

      ceph osd tier rm-overlay {storagepool}

   For example:

   .. prompt:: bash $

      ceph osd tier rm-overlay cold-storage

#. Change the cache mode to ``none`` to disable it:

   .. prompt:: bash $

      ceph osd tier cache-mode {cachepool} none

   For example:

   .. prompt:: bash $

      ceph osd tier cache-mode hot-storage none

#. Remove the cache pool from the backing pool:

   .. prompt:: bash $

      ceph osd tier rm {storagepool} {cachepool}

   For example:

   .. prompt:: bash $

      ceph osd tier rm cold-storage hot-storage


Removing a Writeback Cache
--------------------------

Since a writeback cache may have modified data, you must take steps to ensure 
that you do not lose any recent changes to objects in the cache before you 
disable and remove it.


#. Change the cache mode to ``proxy`` so that the cache tier stops promoting
   objects, except for operations that require it:

   .. prompt:: bash $

      ceph osd tier cache-mode {cachepool} proxy

   For example: 

   .. prompt:: bash $

      ceph osd tier cache-mode hot-storage proxy


#. Flush and evict all objects from the cache pool:

   .. prompt:: bash $

      rados -p {cachepool} cache-flush-evict-all

#. Check that the cache pool is empty. If objects remain, run the previous
   command again:

   .. prompt:: bash $

      rados -p {cachepool} ls --all


#. Remove the overlay so that clients will not direct traffic to the cache:

   .. prompt:: bash $

      ceph osd tier rm-overlay {storagepool}

   For example:

   .. prompt:: bash $

      ceph osd tier rm-overlay cold-storage


#. Finally, remove the cache tier pool from the backing storage pool:

   .. prompt:: bash $

      ceph osd tier rm {storagepool} {cachepool}

   For example:

   .. prompt:: bash $

      ceph osd tier rm cold-storage hot-storage

Troubleshooting Unfound Objects
===============================
Under certain circumstances, restarting OSDs may result in unfound objects.

Here is an example of unfound objects appearing during an upgrade from Ceph
14.2.6 to Ceph 14.2.7::

   2/543658058 objects unfound (0.000%)
   pg 19.12 has 1 unfound objects
   pg 19.2d has 1 unfound objects
   
   Possible data damage: 2 pgs recovery_unfound
   pg 19.12 is active+recovery_unfound+undersized+degraded+remapped, acting [299,310], 1 unfound
   pg 19.2d is active+recovery_unfound+undersized+degraded+remapped, acting [290,309], 1 unfound

The output of ``ceph pg 19.12 list_unfound`` included the following::

   {
       "num_missing": 1,
       "num_unfound": 1,
       "objects": [
           {
               "oid": {
                   "oid": "hit_set_19.12_archive_2020-02-25 13:43:50.256316Z_2020-02-25 13:43:50.325825Z",
                   "key": "",
                   "snapid": -2,
                   "hash": 18,
                   "max": 0,
                   "pool": 19,
                   "namespace": ".ceph-internal"
               },
               "need": "3312398'55868341",
               "have": "0'0",
               "flags": "none",
               "locations": []
           }
       ],
       "more": false
   }

Field reports indicate that the unfound objects can be deleted with no
adverse effects, provided that the objects belong to the
``.ceph-internal`` namespace and have names of the form
``hit_set_<PGID>_archive``. These are hit set archives maintained
internally by cache tiering, not user data. Pawel Stefanski reported
that deleting missing or unfound objects is safe as long as the objects
are a part of ``.ceph-internal::hit_set_PGID_archive``.

Members of the upstream Ceph community have reported this issue on
releases ranging from 14.2.4 through 17.2.7.


.. _Bloom Filter: https://en.wikipedia.org/wiki/Bloom_filter
