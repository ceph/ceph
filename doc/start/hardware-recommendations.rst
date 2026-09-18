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

Every hardware choice balances failure domains, cost, and performance. This
page gives the principles. :ref:`Minimum Hardware per Daemon
<minimum-hardware>`, :ref:`CPU and Memory Sizing <hardware-cpu-memory>`,
:ref:`Storage Devices <hardware-storage-devices>`, and :ref:`Network Sizing
<hardware-networks>` give the numbers. No two clusters are alike: benchmark
before you buy.

Keep Failure Domains Small
==========================

A failure domain is any component whose loss prevents access to one or more
:term:`OSDs <OSD>` or other Ceph daemons. Examples are a stopped daemon on a
host, a failed storage drive, an OS crash, a malfunctioning NIC, a failed
power supply, a network outage, and a power outage.

Fewer failure domains cost less; isolating every one costs more.

These principles keep failure domains small:

- Ceph daemons are spread across many hosts.
- A host runs Ceph daemons of one type, and is configured for that type.
- Processes that use the cluster, such as OpenStack, OpenNebula, CloudStack,
  or Kubernetes, run on separate hosts.
- More, smaller nodes are safer than fewer, denser nodes: when a host with a
  large share of the cluster's capacity fails, recovery can push OSDs past
  the full ratio (:confval:`mon_osd_full_ratio`), and Ceph halts operations
  to prevent data loss.

Balance Cost, Performance, and Risk
===================================

These principles apply to every component:

- CPUs are chosen for IOPS (I/O operations per second) per core, not for
  cores per OSD; Metadata Servers (:term:`MDS`) are single-threaded and want
  a high clock rate.
- More RAM is better; size for peak use, not for a calm period. See
  :ref:`CPU and Memory Sizing <hardware-cpu-memory>`.
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
traffic. A faster network shortens recovery, and so the window in which a
second failure can lose data; the larger the cluster, the more often that
window opens. Bond links across two switches so that one switch is not a
failure domain for the host, and put management and BMC (baseboard
management controller) traffic on a separate out-of-band network. Numbers
and bonding guidance: :ref:`Network Sizing <hardware-networks>`.

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
- `Ceph blog`_ (performance articles)

.. _Ceph blog: https://ceph.io/en/news/blog/
