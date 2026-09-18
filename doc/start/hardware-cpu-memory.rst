.. _hardware-cpu-memory:

=======================
 CPU and Memory Sizing
=======================

.. meta::
   :description: How much CPU and RAM each Ceph daemon needs, and the settings that control OSD, Monitor, and MDS memory use.
   :ceph-page-type: reference
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

CPU and RAM per daemon. Bare minimums, and what counts as a core with
hyperthreading, are on :ref:`minimum-hardware`.

CPU
===

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Daemon
     - CPU recommendation
   * - OSD
     - 1 thread minimum, 3 recommended per HDD OSD; 4 minimum, 6 recommended
       per NVMe OSD (before replication; see :ref:`minimum-hardware` for
       caveats). Select for IOPS (I/O operations per second) per core, not
       for cores per :term:`OSD`.
   * - Monitor, Manager
     - Modest; 2 cores minimum.
   * - MDS
     - CPU-intensive and single-threaded. Performs best with a high clock
       rate (GHz). Does not need many cores unless the host also runs other
       services, such as SSD OSDs for the CephFS metadata pool.
   * - RGW
     - May co-reside with Monitor and Manager services if the nodes have
       sufficient resources.

Run non-Ceph CPU-intensive processes, for example OpenStack Nova, on
separate hosts, not on Monitor and Manager nodes, to avoid resource
contention.

Memory
======

General rules:

- More RAM is better.
- The figures below describe a single daemon of a given type. A server
  needs at least the sum of the needs of the daemons it hosts, plus
  resources for logs and other operating system components.
- A server needs more RAM at startup, when components fail or are added,
  and while the cluster rebalances. Allow headroom past what a calm period
  on a small initial cluster shows.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Daemon
     - RAM recommendation
   * - Monitor and Manager
     - See the table below. Consider tuning :confval:`mon_osd_cache_size`
       and :confval:`rocksdb_cache_size`.
   * - OSD
     - :confval:`osd_memory_target` defaults to 4 GiB per :term:`BlueStore`
       OSD. Total server RAM should be greater than
       (number of OSDs * ``osd_memory_target`` * 2) (headroom for the OS,
       other daemons, and recovery). A 1U server with 8 to 10 OSDs is well
       provisioned with 128 GiB.

       Enable :confval:`osd_memory_target_autotune` to help avoid running
       out of memory (OOM) under heavy load or when non-OSD daemons migrate
       onto a node. See the OSD Memory Target table below.
   * - MDS
     - Depends on :confval:`mds_cache_memory_limit` (default 4 GiB) plus
       overhead. The floor is on the :ref:`minimum-hardware` page.

Monitor and Manager RAM by Cluster Size
=======================================

.. list-table::
   :header-rows: 1
   :widths: 70 30

   * - Cluster size
     - RAM
   * - Very small clusters
     - 32 GiB
   * - Up to about 300 OSDs
     - 64 GiB
   * - More than about 300 OSDs, or clusters that will grow to that size
     - 128 GiB

OSD Memory Target
=================

:confval:`osd_memory_target` sets the amount of memory that a BlueStore OSD
attempts to consume.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Value
     - Effect
   * - Below 2 GiB
     - Not recommended. Ceph may fail to keep memory consumption under
       2 GiB, and extremely slow performance is likely.
   * - 2 GiB to 4 GiB
     - Typically works but may degrade performance. Metadata may need to be
       read from disk during I/O unless the active data set is relatively
       small.
   * - 4 GiB
     - The default. Chosen for typical use cases to balance RAM cost and OSD
       performance.
   * - Above 4 GiB
     - Can improve performance with many small objects, or with large data
       sets of 256 GiB per OSD or more. This is especially true with fast
       NVMe OSDs.
   * - HDD OSDs: 6 GiB
     - An effective target of at least 6 GiB helps mitigate slow requests
       on HDD OSDs.

.. important:: Reclamation is best effort; budget at least 20 percent RAM
   above the sum of :confval:`osd_memory_target` values.

.. tip:: Do not configure swap for hosts that run Ceph daemons; a crashed
   daemon recovers faster than one that is swapping.

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`minimum-hardware`
- :ref:`Storage Devices <hardware-storage-devices>`
- :ref:`Network Sizing <hardware-networks>`
