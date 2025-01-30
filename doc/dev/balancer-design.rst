============================
Balancing in Ceph
============================

Introduction
============

In distributed storage systems like Ceph, it is important to balance write and read requests for optimal performance. Write balancing ensures fast storage
and replication of data in a cluster, while read balancing ensures quick access and retrieval of data in a cluster. Both types of balancing are important
in distributed systems for different reasons.

Upmap Balancing
==========================

Importance in a Cluster
-----------------------

Capacity balancing is a functional requirement. A system like Ceph is as full as its fullest device: When one device is full, the system can not serve write
requests anymore, and Ceph loses its function. To avoid filling up devices, we want to balance capacity across the devices in a fair way. Each device should
get a capacity proportional to its size so all devices have the same fullness level. From a performance perspective, capacity balancing creates fair share
workloads on the OSDs for write requests.

Capacity balancing is expensive. The operation (changing the mapping of pgs) requires data movement by definition, which takes time. During this time, the
performance of the system is reduced.

In Ceph, we can balance the write performance if all devices are homogeneous (same size and performance).

How to Balance Capacity in Ceph
-------------------------------

See :ref:`upmap` for more information.

Read Balancing
==============

Unlike capacity balancing, read balancing is not a strict requirement for Ceph’s functionality. Instead, it is a performance requirement, as it helps the system
“work” better. The overall goal is to ensure each device gets its fair share of primary OSDs so read requests are distributed evenly across OSDs in the cluster.
Unbalanced read requests lead to bad performance because of reduced overall cluster bandwidth.

Read balancing is cheap. Unlike capacity balancing, there is no data movement involved. It is just a metadata operation, where the osdmap is updated to change
which participating OSD in a pg is primary. This operation is fast and has no impact on the cluster performance (except improved performance when the operation
completes – almost immediately).

In Ceph, we can balance the read performance if all devices are homogeneous (same size and performance). However, in future versions, the read balancer can be improved
to achieve overall cluster performance in heterogeneous systems.

How to Balance Reads in Ceph
----------------------------
See :ref:`read_balancer` for more information.

Also, see the Cephalocon 2023 talk `New Read Balancer in Ceph <https://www.youtube.com/watch?v=AT_cKYaQzcU/>`_ for a demonstration of the offline version
of the read balancer.

Plans for the Next Version
--------------------------

1. Improve behavior for heterogeneous OSDs in a pool
