==========================
 Hardware Recommendations
==========================

Ceph runs on commodity hardware and a Linux operating system over a TCP/IP
network. The hardware recommendations for different processes/daemons differ
considerably.

* **OSDs:** OSD hosts should have ample data storage in the form of a hard drive 
  or a RAID. Ceph OSDs run the RADOS service, calculate data placement with 
  CRUSH, and maintain their own copy of the cluster map. Therefore, OSDs 
  should have a reasonable amount of processing power.

* **Monitors:** Ceph monitor hosts require enough disk space for the cluster map, 
  but usually do not encounter heavy loads. Monitor hosts do not need to be 
  very powerful.
  
* **Metadata Servers:** Ceph metadata servers distribute their load. However, 
  metadata servers must be capable of serving their data quickly. Metadata 
  servers should have strong processing capability and plenty of RAM.

.. note:: If you are not using the Ceph File System, you do not need a meta data server.

Minimum Hardware Recommendations
================================

Ceph can run on inexpensive commodity hardware. Small production clusters
and development clusters can run successfully with modest hardware.

+--------------+----------------+------------------------------------+
|  Process     | Criteria       | Minimum Recommended                |
+==============+================+====================================+
| ``ceph-osd`` | Processor      |  64-bit AMD-64/i386 dual-core      |
|              +----------------+------------------------------------+
|              | RAM            |  500 MB per daemon                 |
|              +----------------+------------------------------------+
|              | Volume Storage |  1-disk or RAID per daemon         |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               |
+--------------+----------------+------------------------------------+
| ``ceph-mon`` | Processor      |  64-bit AMD-64/i386                |
|              +----------------+------------------------------------+
|              | RAM            |  1 GB per daemon                   |
|              +----------------+------------------------------------+
|              | Disk Space     |  10 GB per daemon                  |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               |
+--------------+----------------+------------------------------------+
| ``ceph-mds`` | Processor      |  64-bit AMD-64/i386 quad-core      |
|              +----------------+------------------------------------+
|              | RAM            |  1 GB minimum per daemon           |
|              +----------------+------------------------------------+
|              | Disk Space     |  1 MB per daemon                   |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               |
+--------------+----------------+------------------------------------+

.. important: If you are running an OSD with a single disk, create a
   partition for your volume storage that is separate from the partition
   containing the OS. Generally, we recommend separate disks for the
   OS and the volume storage.

Production Cluster Example
==========================

Production clusters for petabyte scale data storage may also use commodity
hardware, but should have considerably more memory, processing power and data
storage to account for heavy traffic loads.

A recent (2012) Ceph cluster project is using two fairly robust hardware
configurations for Ceph OSDs, and a lighter configuration for monitors.

+----------------+----------------+------------------------------------+
|  Configuration | Criteria       | Minimum Recommended                |
+================+================+====================================+
| Dell PE R510   | Processor      |  2 64-bit quad-core Xeon CPUs      |
|                +----------------+------------------------------------+
|                | RAM            |  16 GB                             |
|                +----------------+------------------------------------+
|                | Volume Storage |  8-2TB drives. 1-OS 7-Storage      |
|                +----------------+------------------------------------+
|                | Client Network |  2-1GB Ethernet NICs               |
|                +----------------+------------------------------------+
|                | OSD Network    |  2-1GB Ethernet NICs               |
|                +----------------+------------------------------------+
|                | NIC Mgmt.      |  2-1GB Ethernet NICs               |
+----------------+----------------+------------------------------------+
| Dell PE R515   | Processor      |  1 hex-core Opteron CPU            |
|                +----------------+------------------------------------+
|                | RAM            |  16 GB                             |
|                +----------------+------------------------------------+
|                | Volume Storage |  12-3TB drives. Storage            |
|                +----------------+------------------------------------+
|                | OS Storage     |  1-500GB drive. Operating System.  |
|                +----------------+------------------------------------+
|                | Client Network |  2-1GB Ethernet NICs               |
|                +----------------+------------------------------------+
|                | OSD Network    |  2-1GB Ethernet NICs               |
|                +----------------+------------------------------------+
|                | NIC Mgmt.      |  2-1GB Ethernet NICs               |
+----------------+----------------+------------------------------------+





