=========================
 Data Placement Overview
=========================

Ceph stores, replicates, and rebalances data objects across a RADOS cluster
dynamically. Because different users store objects in different pools for
different purposes on many OSDs, Ceph operations require a certain amount of
data- placement planning. The main data-placement planning concepts in Ceph
include:

- **Pools:** Ceph stores data within pools, which are logical groups used for
  storing objects. Pools manage the number of placement groups, the number of
  replicas, and the CRUSH rule for the pool. To store data in a pool, it is
  necessary to be an authenticated user with permissions for the pool. Ceph is
  able to make snapshots of pools. For additional details, see `Pools`_.

- **Placement Groups:** Ceph maps objects to placement groups. Placement
  groups (PGs) are shards or fragments of a logical object pool that place
  objects as a group into OSDs. Placement groups reduce the amount of
  per-object metadata that is necessary for Ceph to store the data in OSDs. A
  greater number of placement groups (for example, 100 PGs per OSD as compared
  with 50 PGs per OSD) leads to better balancing. For additional details, see
  :ref:`placement groups`.

- **CRUSH Maps:**  CRUSH plays a major role in allowing Ceph to scale while
  avoiding certain pitfalls, such as performance bottlenecks, limitations to
  scalability, and single points of failure. CRUSH maps provide the physical
  topology of the cluster to the CRUSH algorithm, so that it can determine both
  (1) where the data for an object and its replicas should be stored and (2)
  how to store that data across failure domains so as to improve data safety.
  For additional details, see `CRUSH Maps`_.

- **Balancer:** The balancer is a feature that automatically optimizes the
  distribution of placement groups across devices in order to achieve a
  balanced data distribution, in order to maximize the amount of data that can
  be stored in the cluster, and in order to evenly distribute the workload
  across OSDs.

It is possible to use the default values for each of the above components.
Default values are recommended for a test cluster's initial setup. However,
when planning a large Ceph cluster, values should be customized for
data-placement operations with reference to the different roles played by
pools, placement groups, and CRUSH.

.. _Pools: ../pools
.. _CRUSH Maps: ../crush-map
.. _Balancer: ../balancer
