.. _minimum-hardware:

=============================
 Minimum Hardware per Daemon
=============================

.. meta::
   :description: The minimum CPU, memory, storage, and network resources for each Ceph daemon.
   :ceph-page-type: reference
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

Ceph can run on inexpensive commodity hardware. Small production clusters
and development clusters can run successfully with modest hardware. In this
table, cores means threads when hyperthreading (HT) is enabled. For Ceph, HT
is almost always advantageous. Each modern physical x64 CPU core typically
provides two logical CPU threads; other CPU architectures may vary.

There are many factors that influence resource choices. The minimum
resources that suffice for one purpose will not necessarily suffice for
another. A sandbox cluster with one :term:`OSD` built on a laptop with
VirtualBox or on a trio of Raspberry Pis will get by with fewer resources
than a production deployment with a thousand OSDs serving five thousand RBD
clients.

.. warning:: Use enterprise-quality storage media for production workloads.

Additional insights into resource planning for production clusters are
found in :ref:`hardware-recommendations` and elsewhere within this
documentation.

+--------------+----------------+------------------------------------------+
|  Process     | Criteria       | Bare Minimum and Recommended             |
+==============+================+==========================================+
| ``ceph-osd`` | Processor      | - 1 min, 3 recommended threads per HDD   |
|              |                |   OSD. 4, 6 respectively for NVMe SSD    |
|              |                |   OSDs.                                  |
|              |                |                                          |
|              |                | * Results are before replication.        |
|              |                | * Results may vary across CPU and drive  |
|              |                |   models and Ceph configuration:         |
|              |                |   (erasure coding, compression, etc)     |
|              |                | * ARM processors specifically may        |
|              |                |   require more cores for performance.    |
|              |                | * SSD OSDs, especially NVMe, will        |
|              |                |   benefit from additional cores per OSD. |
|              |                | * Actual performance depends on many     |
|              |                |   factors including drives, net, and     |
|              |                |   client throughput and latency.         |
|              |                |   Benchmarking is highly recommended.    |
|              +----------------+------------------------------------------+
|              | RAM            | >= 4 GiB per OSD; see :ref:`CPU and      |
|              |                | Memory Sizing <hardware-cpu-memory>`     |
|              +----------------+------------------------------------------+
|              | Storage Drives | 1x storage drive per OSD in most cases.  |
|              |                | PCIe Gen 4+ SSDs larger than 30 TB may   |
|              |                | benefit from being split into two or     |
|              |                | more OSDs.                               |
|              +----------------+------------------------------------------+
|              | DB/WAL offload |  1x SSD partition per HDD OSD            |
|              | (optional)     |  4-5x HDD OSDs per DB/WAL SATA SSD       |
|              |                |  <= 15 HDD OSDs per DB/WAL NVMe SSD      |
|              +----------------+------------------------------------------+
|              | Network        | 1x 1 Gb/s; see :ref:`Network Sizing      |
|              |                | <hardware-networks>` for the             |
|              |                | recommendation                           |
+--------------+----------------+------------------------------------------+
| ``ceph-mon`` | Processor      | - 2 cores minimum                        |
|              +----------------+------------------------------------------+
|              | RAM            |  >= 5 GB per daemon (large / production  |
|              |                |  clusters need more)                     |
|              +----------------+------------------------------------------+
|              | Storage        |  100 GB per daemon, SSD strongly urged   |
|              +----------------+------------------------------------------+
|              | Network        | 1x 1 Gb/s; see :ref:`Network Sizing      |
|              |                | <hardware-networks>` for the             |
|              |                | recommendation                           |
+--------------+----------------+------------------------------------------+
| ``ceph-mds`` | Processor      | - 2 cores minimum, higher freq is        |
|              |                |   better than more cores                 |
|              +----------------+------------------------------------------+
|              | RAM            |  >= 8 GiB per daemon                     |
|              +----------------+------------------------------------------+
|              | Network        | 1x 1 Gb/s; see :ref:`Network Sizing      |
|              |                | <hardware-networks>` for the             |
|              |                | recommendation                           |
+--------------+----------------+------------------------------------------+

.. tip:: When running an OSD node with a single storage drive, create a
   partition for your OSD that is separate from the partition
   containing the OS. Use separate drives for the OS and for OSD storage.

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`os-recommendations`
