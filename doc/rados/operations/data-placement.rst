=========================
 Data Placement Overview
=========================

Ceph stores, replicates and rebalances data objects across a RADOS cluster
dynamically.  With many different users storing objects in different pools for
different purposes on countless OSDs, Ceph operations require some data
placement planning.  The main data placement planning concepts in Ceph include: 

- **Pools:** Ceph stores data within pools, which are logical groups for storing
  objects. Pools manage the number of placement groups, the number of replicas,
  and the ruleset for the pool. To store data in a pool, you must have
  an authenticated user with permissions for the pool. Ceph can snapshot pools. 
  See `Pools`_ for additional details.
  
- **Placement Groups:** Ceph maps objects to placement groups (PGs). 
  Placement groups (PGs) are shards or fragments of a logical object pool
  that place objects as a group into OSDs. Placement groups reduce the amount 
  of per-object metadata when Ceph stores the data in OSDs. A larger number of 
  placement groups (e.g., 100 per OSD) leads to better balancing. See 
  `Placement Groups`_ for additional details.

- **CRUSH Maps:**  CRUSH is a big part of what allows Ceph to scale without 
  performance bottlenecks, without limitations to scalability, and without a 
  single point of failure. CRUSH maps provide the physical topology of the 
  cluster to the CRUSH algorithm to determine where the data for an object 
  and its replicas should be stored, and how to do so across failure domains 
  for added data safety among other things. See `CRUSH Maps`_ for additional
  details.
  
When you initially set up a test cluster, you can use the default values. Once
you begin planning for a large Ceph cluster, refer to pools, placement groups
and CRUSH for data placement operations. If you find some aspects challenging,
`Inktank`_ provides excellent  premium support for Ceph.

.. _Pools: ../pools
.. _Placement Groups: ../placement-groups
.. _CRUSH Maps: ../crush-map
.. _Inktank: http://www.inktank.com