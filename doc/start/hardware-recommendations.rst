.. _hardware-recommendations:

==========================
 Hardware Recommendations
==========================

Ceph was designed to run on commodity hardware, which makes building and
maintaining petabyte-scale data clusters economically feasible. 
When planning out your cluster hardware, you will need to balance a number 
of considerations, including failure domains and potential performance
issues. Hardware planning should include distributing Ceph daemons and 
other processes that use Ceph across many hosts. Generally, we recommend 
running Ceph daemons of a specific type on a host configured for that type 
of daemon. We recommend using other hosts for processes that utilize your 
data cluster (e.g., OpenStack, CloudStack, etc).


.. tip:: Check out the `Ceph blog`_ too.


CPU
===

CephFS metadata servers are CPU intensive, so they should have significant
processing power (e.g., quad core or better CPUs) and benefit from higher clock
rate (frequency in GHz). Ceph OSDs run the :term:`RADOS` service, calculate
data placement with :term:`CRUSH`, replicate data, and maintain their own copy of the
cluster map. Therefore, OSD nodes should have a reasonable amount of processing
power. Requirements vary by use-case; a starting point might be one core per
OSD for light / archival usage, and two cores per OSD for heavy workloads such
as RBD volumes attached to VMs.  Monitor / manager nodes do not have heavy CPU
demands so a modest processor can be chosen for them.  Also consider whether the
host machine will run CPU-intensive processes in addition to Ceph daemons. For
example, if your hosts will run computing VMs (e.g., OpenStack Nova), you will
need to ensure that these other processes leave sufficient processing power for
Ceph daemons. We recommend running additional CPU-intensive processes on
separate hosts to avoid resource contention.


RAM
===

Generally, more RAM is better.  Monitor / manager nodes for a modest cluster
might do fine with 64GB; for a larger cluster with hundreds of OSDs 128GB
is a reasonable target.  There is a memory target for BlueStore OSDs that
defaults to 4GB.  Factor in a prudent margin for the operating system and
administrative tasks (like monitoring and metrics) as well as increased
consumption during recovery:  provisioning ~8GB per BlueStore OSD
is advised.

Monitors and managers (ceph-mon and ceph-mgr)
---------------------------------------------

Monitor and manager daemon memory usage generally scales with the size of the
cluster.  Note that at boot-time and during topology changes and recovery these
daemons will need more RAM than they do during steady-state operation, so plan
for peak usage. For very small clusters, 32 GB suffices. For clusters of up to,
say, 300 OSDs go with 64GB. For clusters built with (or which will grow to)
even more OSDs you should provision 128GB. You may also want to consider
tuning the following settings:

* :confval:`mon_osd_cache_size`
* :confval:`rocksdb_cache_size`


Metadata servers (ceph-mds)
---------------------------

The metadata daemon memory utilization depends on how much memory its cache is
configured to consume.  We recommend 1 GB as a minimum for most systems.  See
:confval:`mds_cache_memory_limit`.


Memory
======

Bluestore uses its own memory to cache data rather than relying on the
operating system page cache.  In bluestore you can adjust the amount of memory
the OSD attempts to consume with the :confval:`osd_memory_target` configuration
option.

- Setting the :confval:`osd_memory_target` below 2GB is typically not
  recommended (it may fail to keep the memory that low and may also cause
  extremely slow performance.

- Setting the memory target between 2GB and 4GB typically works but may result
  in degraded performance: metadata may be read from disk during IO unless the
  active data set is relatively small.

- 4GB is the current default :confval:`osd_memory_target` size.  This default
  was chosen for typical use cases, and is intended to balance memory
  requirements and OSD performance for typical use cases.

- Setting the :confval:`osd_memory_target` higher than 4GB can improve
  performance when there many (small) objects or large (256GB/OSD or more) data
  sets are processed.

.. important:: OSD memory autotuning is "best effort". Although the OSD may
   unmap memory to allow the kernel to reclaim it, there is no guarantee that
   the kernel will actually reclaim freed memory within a specific time
   frame. This is especially true in older versions of Ceph where transparent
   huge pages can prevent the kernel from reclaiming memory that was freed from
   fragmented huge pages. Modern versions of Ceph disable transparent huge
   pages at the application level to avoid this, but that does not
   guarantee that the kernel will immediately reclaim unmapped memory. The OSD
   may still at times exceed its memory target. We recommend budgeting 
   approximately 20% extra memory on your system to prevent OSDs from going OOM
   (**O**\ut **O**\f **M**\emory) during temporary spikes or due to delay in
   the kernel reclaiming freed pages. That 20% value might be more or less than
   needed, depending on the exact configuration of the system.

When using the legacy FileStore backend, the page cache is used for caching
data, so no tuning is normally needed, and the OSD memory consumption is
generally related to the number of PGs per daemon in the system.


Data Storage
============

Plan your data storage configuration carefully. There are significant cost and
performance tradeoffs to consider when planning for data storage. Simultaneous
OS operations and simultaneous requests from multiple daemons for read and
write operations against a single drive can slow performance.

Hard Disk Drives
----------------

OSDs should have plenty of storage drive space for object data. We recommend a
minimum disk drive size of 1 terabyte. Consider the cost-per-gigabyte advantage
of larger disks. We recommend dividing the price of the disk drive by the
number of gigabytes to arrive at a cost per gigabyte, because larger drives may
have a significant impact on the cost-per-gigabyte. For example, a 1 terabyte
hard disk priced at $75.00 has a cost of $0.07 per gigabyte (i.e., $75 / 1024 =
0.0732). By contrast, a 3 terabyte disk priced at $150.00 has a cost of $0.05
per gigabyte (i.e., $150 / 3072 = 0.0488). In the foregoing example, using the
1 terabyte disks would generally increase the cost per gigabyte by
40%--rendering your cluster substantially less cost efficient.

.. tip:: Running multiple OSDs on a single SAS / SATA drive
   is **NOT** a good idea.  NVMe drives, however, can achieve
   improved performance by being split into two or more OSDs.

.. tip:: Running an OSD and a monitor or a metadata server on a single 
   drive is also **NOT** a good idea.

.. tip:: With spinning disks, the SATA and SAS interface increasingly
   becomes a bottleneck at larger capacities. See also the `Storage Networking 
   Industry Association's Total Cost of Ownership calculator`_.


Storage drives are subject to limitations on seek time, access time, read and
write times, as well as total throughput. These physical limitations affect
overall system performance--especially during recovery. We recommend using a
dedicated (ideally mirrored) drive for the operating system and software, and
one drive for each Ceph OSD Daemon you run on the host (modulo NVMe above).
Many "slow OSD" issues (when they are not attributable to hardware failure)
arise from running an operating system and multiple OSDs on the same drive.

It is technically possible to run multiple Ceph OSD Daemons per SAS / SATA
drive, but this will lead to resource contention and diminish overall
throughput.

To get the best performance out of Ceph, run the following on separate drives:
(1) operating systems, (2) OSD data, and (3) BlueStore db.  For more
information on how to effectively use a mix of fast drives and slow drives in
your Ceph cluster, see the `block and block.db`_ section of the Bluestore
Configuration Reference.

Solid State Drives
------------------

Ceph performance can be improved by using solid-state drives (SSDs). This
reduces random access time and reduces latency while accelerating throughput. 

SSDs cost more per gigabyte than do hard disk drives, but SSDs often offer
access times that are, at a minimum, 100 times faster than hard disk drives.
SSDs avoid hotspot issues and bottleneck issues within busy clusters, and
they may offer better economics when TCO is evaluated holistically.

SSDs do not have moving mechanical parts, so they are not necessarily subject
to the same types of limitations as hard disk drives. SSDs do have significant
limitations though. When evaluating SSDs, it is important to consider the
performance of sequential reads and writes.

.. important:: We recommend exploring the use of SSDs to improve performance. 
   However, before making a significant investment in SSDs, we **strongly
   recommend** reviewing the performance metrics of an SSD and testing the
   SSD in a test configuration in order to gauge performance. 

Relatively inexpensive SSDs may appeal to your sense of economy. Use caution.
Acceptable IOPS are not the only factor to consider when selecting an SSD for
use with Ceph. 

SSDs have historically been cost prohibitive for object storage, but emerging
QLC drives are closing the gap, offering greater density with lower power
consumption and less power spent on cooling. HDD OSDs may see a significant
performance improvement by offloading WAL+DB onto an SSD.

To get a better sense of the factors that determine the cost of storage, you
might use the `Storage Networking Industry Association's Total Cost of
Ownership calculator`_

Partition Alignment
~~~~~~~~~~~~~~~~~~~

When using SSDs with Ceph, make sure that your partitions are properly aligned.
Improperly aligned partitions suffer slower data transfer speeds than do
properly aligned partitions. For more information about proper partition
alignment and example commands that show how to align partitions properly, see
`Werner Fischer's blog post on partition alignment`_.

CephFS Metadata Segregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

One way that Ceph accelerates CephFS file system performance is by segregating
the storage of CephFS metadata from the storage of the CephFS file contents.
Ceph provides a default ``metadata`` pool for CephFS metadata. You will never
have to create a pool for CephFS metadata, but you can create a CRUSH map
hierarchy for your CephFS metadata pool that points only to SSD storage media.
See :ref:`CRUSH Device Class<crush-map-device-class>` for details.


Controllers
-----------

Disk controllers (HBAs) can have a significant impact on write throughput.
Carefully consider your selection of HBAs to ensure that they do not create a
performance bottleneck. Notably, RAID-mode (IR) HBAs may exhibit higher latency
than simpler "JBOD" (IT) mode HBAs. The RAID SoC, write cache, and battery
backup can substantially increase hardware and maintenance costs. Some RAID
HBAs can be configured with an IT-mode "personality".

.. tip:: The `Ceph blog`_ is often an excellent source of information on Ceph
   performance issues. See `Ceph Write Throughput 1`_ and `Ceph Write 
   Throughput 2`_ for additional details.


Benchmarking
------------

BlueStore opens block devices in O_DIRECT and uses fsync frequently to ensure
that data is safely persisted to media. You can evaluate a drive's low-level
write performance using ``fio``. For example, 4kB random write performance is
measured as follows:

.. code-block:: console

  # fio --name=/dev/sdX --ioengine=libaio --direct=1 --fsync=1 --readwrite=randwrite --blocksize=4k --runtime=300

Write Caches
------------

Enterprise SSDs and HDDs normally include power loss protection features which
use multi-level caches to speed up direct or synchronous writes.  These devices
can be toggled between two caching modes -- a volatile cache flushed to
persistent media with fsync, or a non-volatile cache written synchronously.

These two modes are selected by either "enabling" or "disabling" the write
(volatile) cache.  When the volatile cache is enabled, Linux uses a device in
"write back" mode, and when disabled, it uses "write through".

The default configuration (normally caching enabled) may not be optimal, and
OSD performance may be dramatically increased in terms of increased IOPS and
decreased commit_latency by disabling the write cache.

Users are therefore encouraged to benchmark their devices with ``fio`` as
described earlier and persist the optimal cache configuration for their
devices.

The cache configuration can be queried with ``hdparm``, ``sdparm``,
``smartctl`` or by reading the values in ``/sys/class/scsi_disk/*/cache_type``,
for example:

.. code-block:: console

  # hdparm -W /dev/sda

  /dev/sda:
   write-caching =  1 (on)

  # sdparm --get WCE /dev/sda
      /dev/sda: ATA       TOSHIBA MG07ACA1  0101
  WCE           1  [cha: y]
  # smartctl -g wcache /dev/sda
  smartctl 7.1 2020-04-05 r5049 [x86_64-linux-4.18.0-305.19.1.el8_4.x86_64] (local build)
  Copyright (C) 2002-19, Bruce Allen, Christian Franke, www.smartmontools.org

  Write cache is:   Enabled

  # cat /sys/class/scsi_disk/0\:0\:0\:0/cache_type
  write back

The write cache can be disabled with those same tools:

.. code-block:: console

  # hdparm -W0 /dev/sda

  /dev/sda:
   setting drive write-caching to 0 (off)
   write-caching =  0 (off)

  # sdparm --clear WCE /dev/sda
      /dev/sda: ATA       TOSHIBA MG07ACA1  0101
  # smartctl -s wcache,off /dev/sda
  smartctl 7.1 2020-04-05 r5049 [x86_64-linux-4.18.0-305.19.1.el8_4.x86_64] (local build)
  Copyright (C) 2002-19, Bruce Allen, Christian Franke, www.smartmontools.org

  === START OF ENABLE/DISABLE COMMANDS SECTION ===
  Write cache disabled

Normally, disabling the cache using ``hdparm``, ``sdparm``, or ``smartctl``
results in the cache_type changing automatically to "write through". If this is
not the case, you can try setting it directly as follows. (Users should note
that setting cache_type also correctly persists the caching mode of the device
until the next reboot):

.. code-block:: console

  # echo "write through" > /sys/class/scsi_disk/0\:0\:0\:0/cache_type

  # hdparm -W /dev/sda

  /dev/sda:
   write-caching =  0 (off)

.. tip:: This udev rule (tested on CentOS 8) will set all SATA/SAS device cache_types to "write
  through":

  .. code-block:: console

    # cat /etc/udev/rules.d/99-ceph-write-through.rules
    ACTION=="add", SUBSYSTEM=="scsi_disk", ATTR{cache_type}:="write through"

.. tip:: This udev rule (tested on CentOS 7) will set all SATA/SAS device cache_types to "write
  through":

  .. code-block:: console

    # cat /etc/udev/rules.d/99-ceph-write-through-el7.rules
    ACTION=="add", SUBSYSTEM=="scsi_disk", RUN+="/bin/sh -c 'echo write through > /sys/class/scsi_disk/$kernel/cache_type'"

.. tip:: The ``sdparm`` utility can be used to view/change the volatile write
  cache on several devices at once:

  .. code-block:: console

    # sdparm --get WCE /dev/sd*
        /dev/sda: ATA       TOSHIBA MG07ACA1  0101
    WCE           0  [cha: y]
        /dev/sdb: ATA       TOSHIBA MG07ACA1  0101
    WCE           0  [cha: y]
    # sdparm --clear WCE /dev/sd*
        /dev/sda: ATA       TOSHIBA MG07ACA1  0101
        /dev/sdb: ATA       TOSHIBA MG07ACA1  0101

Additional Considerations
-------------------------

You typically will run multiple OSDs per host, but you should ensure that the
aggregate throughput of your OSD drives doesn't exceed the network bandwidth
required to service a client's need to read or write data. You should also
consider what percentage of the overall data the cluster stores on each host. If
the percentage on a particular host is large and the host fails, it can lead to
problems such as exceeding the ``full ratio``,  which causes Ceph to halt
operations as a safety precaution that prevents data loss.

When you run multiple OSDs per host, you also need to ensure that the kernel
is up to date. See `OS Recommendations`_ for notes on ``glibc`` and
``syncfs(2)`` to ensure that your hardware performs as expected when running
multiple OSDs per host.


Networks
========

Provision at least 10Gbps+ networking in your racks. Replicating 1TB of data
across a 1Gbps network takes 3 hours, and 10TBs takes 30 hours! By contrast,
with a 10Gbps network, the replication times would be 20 minutes and 1 hour
respectively. In a petabyte-scale cluster, failure of an OSD drive is an
expectation, not an exception. System administrators will appreciate PGs
recovering from a ``degraded`` state to an ``active + clean`` state as rapidly
as possible, with price / performance tradeoffs taken into consideration.
Additionally, some deployment tools employ VLANs to make  hardware and network
cabling more manageable. VLANs using 802.1q protocol require VLAN-capable NICs
and Switches. The added hardware expense may be offset by the operational cost
savings for network setup and maintenance. When using VLANs to handle VM
traffic between the cluster and compute stacks (e.g., OpenStack, CloudStack,
etc.), there is additional value in using 10G Ethernet or better; 40Gb or
25/50/100 Gb networking as of 2020 is common for production clusters.

Top-of-rack routers for each network also need to be able to communicate with
spine routers that have even faster throughput, often 40Gbp/s or more.


Your server hardware should have a Baseboard Management Controller (BMC).
Administration and deployment tools may also use BMCs extensively, especially
via IPMI or Redfish, so consider
the cost/benefit tradeoff of an out-of-band network for administration.
Hypervisor SSH access, VM image uploads, OS image installs, management sockets,
etc. can impose significant loads on a network.  Running three networks may seem
like overkill, but each traffic path represents a potential capacity, throughput
and/or performance bottleneck that you should carefully consider before
deploying a large scale data cluster.
 

Failure Domains
===============

A failure domain is any failure that prevents access to one or more OSDs. That
could be a stopped daemon on a host; a disk failure, an OS crash, a
malfunctioning NIC, a failed power supply, a network outage, a power outage,
and so forth. When planning out your hardware needs, you must balance the
temptation to reduce costs by placing too many responsibilities into too few
failure domains, and the added costs of isolating every potential failure
domain.


Minimum Hardware Recommendations
================================

Ceph can run on inexpensive commodity hardware. Small production clusters
and development clusters can run successfully with modest hardware.

+--------------+----------------+-----------------------------------------+
|  Process     | Criteria       | Minimum Recommended                     |
+==============+================+=========================================+
| ``ceph-osd`` | Processor      | - 1 core minimum                        |
|              |                | - 1 core per 200-500 MB/s               |
|              |                | - 1 core per 1000-3000 IOPS             |
|              |                |                                         |
|              |                | * Results are before replication.       |
|              |                | * Results may vary with different       |
|              |                |   CPU models and Ceph features.         |
|              |                |   (erasure coding, compression, etc)    |
|              |                | * ARM processors specifically may       |
|              |                |   require additional cores.             |
|              |                | * Actual performance depends on many    |
|              |                |   factors including drives, net, and    |
|              |                |   client throughput and latency.        |
|              |                |   Benchmarking is highly recommended.   |
|              +----------------+-----------------------------------------+
|              | RAM            | - 4GB+ per daemon (more is better)      |
|              |                | - 2-4GB often functions (may be slow)   |
|              |                | - Less than 2GB not recommended         |
|              +----------------+-----------------------------------------+
|              | Volume Storage |  1x storage drive per daemon            |
|              +----------------+-----------------------------------------+
|              | DB/WAL         |  1x SSD partition per daemon (optional) |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1GbE+ NICs (10GbE+ recommended)     |
+--------------+----------------+-----------------------------------------+
| ``ceph-mon`` | Processor      | - 2 cores minimum                       |
|              +----------------+-----------------------------------------+
|              | RAM            |  2-4GB+ per daemon                      |
|              +----------------+-----------------------------------------+
|              | Disk Space     |  60 GB per daemon                       |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1GbE+ NICs                          |
+--------------+----------------+-----------------------------------------+
| ``ceph-mds`` | Processor      | - 2 cores minimum                       |
|              +----------------+-----------------------------------------+
|              | RAM            |  2GB+ per daemon                        |
|              +----------------+-----------------------------------------+
|              | Disk Space     |  1 MB per daemon                        |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1GbE+ NICs                          |
+--------------+----------------+-----------------------------------------+

.. tip:: If you are running an OSD with a single disk, create a
   partition for your volume storage that is separate from the partition
   containing the OS. Generally, we recommend separate disks for the
   OS and the volume storage.



.. _block and block.db: https://docs.ceph.com/en/latest/rados/configuration/bluestore-config-ref/#block-and-block-db
.. _Ceph blog: https://ceph.com/community/blog/
.. _Ceph Write Throughput 1: http://ceph.com/community/ceph-performance-part-1-disk-controller-write-throughput/
.. _Ceph Write Throughput 2: http://ceph.com/community/ceph-performance-part-2-write-throughput-without-ssd-journals/
.. _Mapping Pools to Different Types of OSDs: ../../rados/operations/crush-map#placing-different-pools-on-different-osds
.. _OS Recommendations: ../os-recommendations
.. _Storage Networking Industry Association's Total Cost of Ownership calculator: https://www.snia.org/forums/cmsi/programs/TCOcalc
.. _Werner Fischer's blog post on partition alignment: https://www.thomas-krenn.com/en/wiki/Partition_Alignment_detailed_explanation
