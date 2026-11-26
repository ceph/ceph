==============
Priority Cache
==============

A cache
-------

It is an obvious concept to preserve for potential future reuses something that was costly to obtain.
A cache in a software system is a component that trades processing for storage. A typical cache has a fixed capacity
that it cannot exceed. Cache's internal logic decides which items are preserved and which have to go.

Multiple caches
---------------

Complex systems can have multiple caches for different types of data.
When caches are consuming different kinds of resources for their storage there is no problem.
But usually the resource for storage is the same - host memory.

Optimization problem
--------------------

The problem to solve is how to split available memory :math:`Mem_{avail} = \sum_{k=0}^{n} mem_{k}`
between the caches so the performance gain :math:`Gain = \sum_{k=0}^{n} cache_{k}(mem_{k})` is maximum.
In abstract, the metric for gain could be anything, but in practice always time is used.
The problem is absurdly complex as:

- optimum is an elusive target that depends on workload
- :math:`cache_{k}()` functions are not necessarily monotonic or continous 
- different caches can affect each other (draining one cache can make other less used)

Priority Cache
--------------

Priority Cache is a clever way to abstract away difficulties and get the robust, working solution.
There is no gain function, there is no need for complicated math.
Instead, there is a list of 12 priorities: PRI0 .. PRI10 and extra one PRI11=LAST.

A priority cache is assigned a memory quota as a single value. It operates and learns from the patterns.
Later, when asked the cache can distingush between its urgently needed capacity and an additional capacity
cache can benefit from, but less urgent.

When cache is assigned 1000M, it can use entire memory indiscriminately.
Queried, the cache splits the memory into priorities.

+----------+------+------+------+------+------+------+------+
| assigned | PRI0 | PRI1 | PRI2 | PRI3 | PRI4 |  ... | LAST |
+==========+======+======+======+======+======+======+======+
|    1000M | 500M | 300M | 100M |  50M |   0M |   0M |  50M |
+----------+------+------+------+------+------+------+------+

Data in PRI0 is more important than in PRI1, in PRI1 more important than in PRI2, etc.
It means that the cache has determined it needs 500M to work,
additional 300M to work a bit better, and additonal 100M for even more.
The LAST means that the cache is using the memory, but it does not affect its performance.

The function used to set up memory limit for the cache:

.. code-block:: cpp

   int64_t PriorityCache::commit_cache_size(uint64_t total_cache)

Cache's split of assigned memory into priority ranges is retrieved with:

.. code-block:: cpp

   int64_t get_cache_bytes(PriorityCache::Priority pri)


.. note::

  Following function play only support role.
  Priority Cache Manager uses it as temporary storage when decising how to split memory between caches.

  .. code-block:: cpp

    void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes)
    void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes)

Priority Cache Cooperation
--------------------------

Each Priority Cache (a specialization of `PriorityCache::PriCache`) is an independent entity.
The way how they group portions of held memory into the priorities is theoretically fully independent decision.
In practice those priorities meet at Priority Cache Manager that will balance sizes for the best performance.
Two independent caches A and B should have a common agreement what constitutes PRIx memory priority.
The goal is to prevent cache A from hoarding memory at high priorities that would be better utilized by cache B.

Priority Caches in Bluestore
----------------------------

BlueStore employs 4 instances of PriCache.

.. list-table::
  :widths: 15 65
  :header-rows: 1

  * - Cache
    - Description
  * - meta_cache
    - The cache that holds decoded Onodes. Since it was observed that decoding of Onodes
      is relatively costly operation this cache is usually the largest.
  * - data_cache
    - The cache for disk data. It has two roles. First, it keeps live data that has not
      need stored to the disk (deferred writes), or very short time for regular writes.
      Second, it works as a regular cache for disk data.
      In the second role it is the least useful cache of them all.
  * - kv_cache
    - The cache for RocksDB sst tables. The cache extents RocksDB - required interface with
      Priority Cache features to have dynamica control over the capacity.
  * - kv_onode_cache
    - Similar to kv_cache, but dedicated for keys that code Onodes. It is intentionally small,
      as BlueStore will never ask for a key if the corresponding Onode is loaded and resides in meta_cache.

Age Binning
-----------

Calculating capacities assigned to priorities in Priority Caches is difficult.
BlueStore caches are solving this by assuming LRU behaviour for the purpose of tracking capacities.
The trick is to create list of bins, each representing cache utilization through time.
The head of bin list is `now`, and the tail is the oldest tracked past.
When cache inserts a new element, the capacity of the element adds to the head bin.
When cache retrieves element, it subtracts the capacity of the element from the bin it was tracked in
and adds the capacity to the head bin.

Repeatedly, after predefined period of time, the new head bin is created and all other bins are shifted.
The period of bin shifting is defined by:

.. confval:: bluestore_cache_age_bin_interval

Once cache activity is nicely sorted into historical bins, the capacities can be chopped into priorities
for Priority Cache purposes.
The most recently used data forms PRI1; a bit older data forms PRI2; even older data forms PRI3, and so on.
The autor of the documentation does not know why PRI0 is not used at all.
The actual lengths that form the per-priority capacity requests are defined by:

.. confval:: bluestore_cache_age_bins_meta

The fact that all BlueStore's Priority Caches (**bluestore_cache_age_bins_kv**,
**bluestore_cache_age_bins_meta**, **bluestore_cache_age_bins_data**) share the same age bins to priority
mappings means the Priority Cache Manager treats them as equally important.
It is the respective cache hunger for capacity that determines the received share of capacity.
**bluestore_cache_age_bins_kv_onode** is an outlier here, as it is asking for whatever capacity is left.

.. note::

  The settings of age bin duration and mappings of age to priorities have not undergone a scrutiny they deserve.
  It is very likely that there are better settings that give Priority Cache infrastructure better perfromance.
  Also, a concept of automatic, dynamic adjustment of the values should be researched.

.. note::

  Most likely assigning "0 0 0 0 0 0 0 0 0 720" to **bluestore_cache_age_bins_kv_onode** is a mistake.
  Under heavy cache utilization size of the cache goes down to minimim allowed 64MB. This is known to be
  significantly too small for large RocksDB compactions.

Priority Cache Manager
----------------------

The role of the manager is to collect information from individual Priority Caches and share capacity
in a way that maximizes expected performance.

The algorithm is simple. Iterate over priority levels, from highest PRI0 to lowerst PRI10. At each priority,
try to assign the caches the capacity they require. In a typical scenario, it can simply do it.
If a cache had it, it can get it back.

When available memory shrinks, some caches are not getting their requested capacity at some priority.
When available memory expands, all caches are getting their requested capacities and then some extra on priority LAST.

A trap of perfection
--------------------

At the first glance assigning all available memory to caches is a good idea.
That is not really the case.

Imagine Priority Cache Manager and its caches reaching stable point. A stable point is defined here as
the condition that PRI0 to PRI11 capacity requests are fulfilled and LAST capacity is 0.
At such conditions the caches can no longer transfer capacity between them. The size of each cache is fixed,
for as long as the condition persists. And in default configuration that is 720 seconds.

Example: Imagine stable point achieved after intense read-write workload on object data.
There is a lot of Onodes loaded, and some of the very latest data is still cached.

.. table:: Stable point

  +----------+----------+------+------+------+------+------+------+------+
  | cache    | assigned | PRI1 | PRI2 | PRI3 | PRI4 | ...  | PRI10| LAST |
  +==========+==========+======+======+======+======+======+======+======+
  | kv_onode |       64 |    0 |    0 |    0 |    0 |      |   64 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | kv       |       64 |   40 |   24 |    0 |    0 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | meta     |     2000 | 1500 |  500 |    0 |    0 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | data     |     2000 | 2000 |    0 |    0 |    0 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+

Should the system suddenly switch to retrieving OMAPs, there is just 64M in **kv-cache**
and Priority Cache Manager cannot give more.
Its only after significant amount of time, about 720s with the default settings that the **meta-cache**
and **data-cache** content would time-out:

.. table:: Data times out

  +----------+----------+------+------+------+------+------+------+------+
  | cache    | assigned | PRI1 | PRI2 |  ... | PRI6 | ...  | PRI10| LAST |
  +==========+==========+======+======+======+======+======+======+======+
  | kv_onode |       64 |    0 |    0 |      |    0 |      |   64 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | kv       |       64 |   64 |    0 |      |    0 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | meta     |     2000 |    0 |    0 |      | 2000 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+
  | data     |     2000 |    0 |    0 |      | 2000 |      |    0 |    0 |
  +----------+----------+------+------+------+------+------+------+------+

And in the next cycle Priority Cache Manager can assign extra memory according to the default split ratios.

.. table:: Cache balancing revived

  +----------+----------+------+------+------+------+------+------+-------------+
  | cache    | assigned | PRI1 | PRI2 |  ... | PRI6 | ...  | PRI10|        LAST |
  +==========+==========+======+======+======+======+======+======+=============+
  | kv_onode |       64 |    0 |    0 |      |    0 |      |   64 | 4128 * 0.04 |
  +----------+----------+------+------+------+------+------+------+-------------+
  | kv       |       64 |   64 |    0 |      |    0 |      |    0 | 4128 * 0.45 |
  +----------+----------+------+------+------+------+------+------+-------------+
  | meta     |     2000 |    0 |    0 |      | 2000 |      |    0 | 4128 * 0.45 |
  +----------+----------+------+------+------+------+------+------+-------------+
  | data     |     2000 |    0 |    0 |      | 2000 |      |    0 | 4128 * 0.06 |
  +----------+----------+------+------+------+------+------+------+-------------+

Worse-is-better
---------------

The scenario presented in previous section is real, but improbable with current BlueStore Priority Caches.
The critical factor that saves the day is the fact that caches: **kv-onode**, **kv**, **meta** and **data**
are all sharded. The fact that those caches are sharded means that whatever capacity Manager assigns to them,
they split it evenly between the shards. In effect the most utilized shards are trimmed at the expense of
the less utilized ones. And that trimming gives something to distribute at LAST priority.
