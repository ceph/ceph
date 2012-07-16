=========================
 Tuning Placement Groups
=========================

Purpose
-------

Placement groups (PGs) are shards or fragments of a logical object pool.  Their function is to spread the responsibility for storing objects across the nodes of the cluster.  Generally speaking, a large number of placement groups will ensure that load is spread evenly, at the expensive of tracking/management overhead.  A small number of placement groups can result in non-optimal distribution of load (e.g., some OSDs with significantly more or less data than the others).

Each pool as a ``pg_num`` property that indicates the number of PGs it is fragmented into.  The total number of PG copies in the system is the sum of the ``pg_num`` value times the replication factor for each pool.


Optimal total PG count
----------------------

A rule of thumb is to shoot for a total PG count that is on the order of 50-100 PGs per OSD in the system.  Anything within an order of magnitude of that target will do reasonably well in terms of variation between utilization.  Thus, if your system have 900 OSDs, you probably want somewhere between 30,000 and 90,000 PG copies.  If your replication factor is 3x, then you want your ``pg_num`` values to add to something between 10,000 and 30,000.  16,384 is a nice round number (powers of two are marginally better).

If your system tends to be underpowered, choosing a lower ``pg_num`` will ease system load at the expense of load balance.

If you expect the cluster to grow in size, you may want to choose a larger ``pg_num`` that will remain within a good range as the number of OSDs increases.


Multiple pools
--------------

If you have a single pool of objects in the system, choosing ``pg_num`` is easy::

  total_pg_copies = num_osds * 100
  total_pgs = total_pg_copes / replication_factor
  lone_pool_pg_num = total_pgs

If you have multiple pools, you need to be a bit careful about how each pool's ``pg_num`` value is chosen so that they add up to a reasonable total.  Generally speaking, each pool's ``pg_num`` should be roughly proportional to the number of objects you expect the pool to contain.


Splitting/merging PGs
---------------------

Ceph will soon support the ability to dynamically adjust the number of PGs in a pool after it has been created.  Existing PGs will be "split" into smaller fragments or "merged" into larger ones and then redistributed.  Until then, the ``pg_num`` pool property can only be set when the pool is created.

