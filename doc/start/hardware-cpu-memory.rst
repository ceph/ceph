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

This page lists the CPU and RAM recommendations for each Ceph daemon type.
For the bare minimum per daemon, and for what counts as a core when
hyperthreading is enabled, see :ref:`minimum-hardware`. For the principles
behind these numbers, see :ref:`hardware-recommendations`.

CPU
===

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Daemon
     - CPU recommendation
   * - OSD
     - Enough processing power to run the :term:`RADOS` service, calculate
       data placement with :term:`CRUSH`, replicate data, and maintain its
       own copy of the cluster map. Select for IOPS (I/O operations per
       second) per core rather than for cores per :term:`OSD`. For threads
       per OSD by drive type, see :ref:`minimum-hardware`.
   * - Monitor, Manager
     - Modest. Monitor and Manager nodes do not have heavy CPU demands.
   * - MDS
     - CPU-intensive and single-threaded. Performs best with a high clock
       rate (GHz). Does not need many cores unless the host also runs other
       services, such as SSD OSDs for the CephFS metadata pool.
   * - RGW
     - May co-reside with Monitor and Manager services if the nodes have
       sufficient resources.

Run non-Ceph CPU-intensive processes, for example OpenStack Nova, on
separate hosts, not on Monitor and Manager nodes, to avoid resource
contention. If a host must run both, provide enough processing power for
both.

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
     - Usage scales with the size of the cluster and peaks at boot, during
       topology changes, and during recovery. Plan for peak usage.

       .. list-table::
          :header-rows: 1
          :widths: 70 30

          * - Cluster size
            - RAM
          * - Very small clusters
            - 32 GiB
          * - Modest clusters, up to about 300 OSDs
            - 64 GiB
          * - More than about 300 OSDs, or clusters that will grow to that
              size
            - 128 GiB

       Consider tuning :confval:`mon_osd_cache_size` and
       :confval:`rocksdb_cache_size`.
   * - OSD
     - :confval:`osd_memory_target` defaults to 4 GiB per :term:`BlueStore`
       OSD. Total server RAM should be greater than
       (number of OSDs * ``osd_memory_target`` * 2). This allows for the
       OS, administrative tasks such as monitoring and metrics, other Ceph
       daemons, and increased consumption during recovery. A 1U server with
       8 to 10 OSDs is well provisioned with 128 GiB.

       Enable :confval:`osd_memory_target_autotune` to help avoid running
       out of memory (OOM) under heavy load or when non-OSD daemons migrate
       onto a node. See the OSD Memory Target table below.
   * - MDS
     - Depends on :confval:`mds_cache_memory_limit` (default 4 GiB) plus
       overhead. The floor is on the :ref:`minimum-hardware` page.

OSD Memory Target
=================

BlueStore uses its own memory to cache data rather than the operating
system's page cache. :confval:`osd_memory_target` sets the amount of memory
that a BlueStore OSD attempts to consume.

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
