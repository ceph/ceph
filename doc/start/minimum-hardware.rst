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

The smallest configuration that each daemon runs with. Cores means threads
when hyperthreading is enabled. Production clusters need more; see
:ref:`hardware-recommendations`.

+--------------+----------------+------------------------------------------+
|  Process     | Criteria       | Minimum (recommended where shown)        |
+==============+================+==========================================+
| ``ceph-osd`` | Processor      | 1 thread minimum, 3 recommended per HDD  |
|              |                | OSD; 4 minimum, 6 recommended per NVMe   |
|              |                | SSD OSD.                                 |
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
|              | Network        | 1 Gb/s minimum; 10 Gb/s recommended      |
|              |                | (:ref:`Network Sizing                    |
|              |                | <hardware-networks>`)                    |
+--------------+----------------+------------------------------------------+
| ``ceph-mon`` | Processor      | 2 cores minimum                          |
|              +----------------+------------------------------------------+
|              | RAM            |  >= 5 GB per daemon (large / production  |
|              |                |  clusters need more)                     |
|              +----------------+------------------------------------------+
|              | Storage        |  100 GB per daemon, SSD strongly urged   |
|              +----------------+------------------------------------------+
|              | Network        | 1 Gb/s minimum; 10 Gb/s recommended      |
|              |                | (:ref:`Network Sizing                    |
|              |                | <hardware-networks>`)                    |
+--------------+----------------+------------------------------------------+
| ``ceph-mds`` | Processor      | 2 cores minimum, higher freq is better   |
|              |                | than more cores                          |
|              +----------------+------------------------------------------+
|              | RAM            |  >= 8 GiB per daemon                     |
|              +----------------+------------------------------------------+
|              | Network        | 1 Gb/s minimum; 10 Gb/s recommended      |
|              |                | (:ref:`Network Sizing                    |
|              |                | <hardware-networks>`)                    |
+--------------+----------------+------------------------------------------+

Thread counts are before replication and vary with CPU and drive model,
erasure coding, and compression; ARM CPUs may need more cores. Benchmark.

.. warning:: Use enterprise-quality storage media for production workloads.

.. tip:: Use a separate drive for the OS. If a node has only one drive, put
   the OSD on its own partition. See
   :ref:`Storage Devices <hardware-storage-devices>`.

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`os-recommendations`
