.. _hardware-recommendations:

==========================
 Hardware Recommendations
==========================

.. meta::
   :description: The principles behind choosing CPUs, memory, storage devices, and networks for a Ceph cluster.
   :ceph-page-type: concept
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

Ceph runs on commodity hardware. Every hardware choice balances three
things: failure domains, cost, and performance. This page gives the
principles that drive those choices. The pages listed under Additional
Resources give the numbers.

No two clusters have the same requirements. Treat the recommendations as
general guidelines, and benchmark before you buy.

.. tip:: The `Ceph blog`_ is often an excellent source of information on Ceph
   performance.

Failure Domains
===============

A failure domain is any component whose loss prevents access to one or more
:term:`OSDs <OSD>` or other Ceph daemons. Examples are a stopped daemon on a
host, a failed storage drive, an OS crash, a malfunctioning NIC, a failed
power supply, a network outage, and a power outage.

Placing many responsibilities in few failure domains reduces cost. Isolating
every potential failure domain adds cost. You must balance the two risks.

These principles keep failure domains small:

- Ceph daemons, and the processes that use Ceph, are spread across many
  hosts.
- A host runs Ceph daemons of one type, and is configured for that type.
- Processes that use the cluster, such as OpenStack, OpenNebula, CloudStack,
  or Kubernetes, run on separate hosts.
- More, smaller nodes are safer than fewer, denser nodes: when a host with a
  large share of the cluster's capacity fails, recovery can push OSDs past
  the full ratio (:confval:`mon_osd_full_ratio`), and Ceph halts operations
  to prevent data loss.

Cost and Performance
====================

Balance cost against performance, and more subtly, against risk. These
principles apply to every component:

- CPUs are chosen for IOPS (I/O operations per second) per core, not for
  cores per OSD; Metadata Servers (:term:`MDS`) are single-threaded and want
  a high clock rate.
- More RAM is better: a server needs the sum of its daemons' peak needs, at
  startup, during failures and additions, and while rebalancing, plus
  headroom for the OS, logs, and monitoring.
- The operating system, each OSD, and any WAL+DB (write-ahead log and
  metadata database) have their own drives.
- Monitor databases, CephFS metadata, and RGW index and log pools belong on
  enterprise-class SSDs even when bulk data lives on HDDs; see
  :ref:`Storage Devices <hardware-storage-devices>`.
- Production clusters use enterprise-class drives with power loss
  protection.
- Cost is judged by total cost of ownership, not by price per terabyte:
  larger HDDs cost less per terabyte but deliver fewer IOPS per TB, and when
  RAID HBAs, chassis, and data center space are counted, SSDs often cost
  less overall.
- Drives are benchmarked before a significant purchase, and again to choose
  the write cache setting.

.. _hardware-recommendations-networks:

Networks
========

Network bandwidth must carry client traffic plus replication and recovery
traffic. Provision network bandwidth generously; see
:ref:`Network Sizing <hardware-networks>` for the numbers.

A faster network recovers faster. Fast recovery shortens the time during
which a second failure could make data unavailable or lost. The larger the
cluster, the more often OSDs fail, so this matters more as the cluster
grows.

Bonding links across two switches keeps one switch from being a failure
domain for the host; see :ref:`hardware-networks` for bonding guidance.
Management traffic, including BMC (baseboard management controller)
traffic, is a load of its own and belongs on a separate out-of-band network;
see :ref:`hardware-networks`.

Minimum Hardware Recommendations
================================

The bare minimum per daemon is listed on :ref:`minimum-hardware`.

Additional Resources
====================

- :ref:`CPU and Memory Sizing <hardware-cpu-memory>`
- :ref:`Storage Devices <hardware-storage-devices>`
- :ref:`Network Sizing <hardware-networks>`
- :ref:`Minimum Hardware per Daemon <minimum-hardware>`
- :ref:`os-recommendations`
- :ref:`ceph-cluster-components`

.. _Ceph blog: https://ceph.io/en/news/blog/
