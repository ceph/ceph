.. _hardware-recommendations:

==========================
 hardware recommendations
==========================

Ceph is designed to run on commodity hardware, which makes building and
maintaining petabyte-scale data clusters flexible and economically feasible.
When planning your cluster's hardware, you will need to balance a number
of considerations, including failure domains, cost, and performance.
Hardware planning should include distributing Ceph daemons and
other processes that use Ceph across many hosts. Generally, we recommend
running Ceph daemons of a specific type on a host configured for that type
of daemon. We recommend using separate hosts for processes that utilize your
data cluster (e.g., OpenStack, OpenNebula, CloudStack, Kubernetes, etc).

The requirements of one Ceph cluster are not the same as the requirements of
another, but below are some general guidelines.

.. tip:: check out the `ceph blog`_ too.

CPU
===

CephFS Metadata Servers (MDS) are CPU-intensive. They are
are single-threaded and perform best with CPUs with a high clock rate (GHz). MDS
servers do not need a large number of CPU cores unless they are also hosting other
services, such as SSD OSDs for the CephFS metadata pool.
OSD nodes need enough processing power to run the RADOS service, to calculate data
placement with CRUSH, to replicate data, and to maintain their own copies of the
cluster map.

With earlier releases of Ceph, we would make hardware recommendations based on
the number of cores per OSD, but this cores-per-osd metric is no longer as
useful a metric as the number of cycles per IOP and the number of IOPS per OSD.
For example, with NVMe OSD drives, Ceph can easily utilize five or six cores on real
clusters and up to about fourteen cores on single OSDs in isolation. So cores
per OSD are no longer as pressing a concern as they were. When selecting
hardware, select for IOPS per core.

.. tip:: When we speak of CPU _cores_, we mean _threads_ when hyperthreading
	 is enabled.  Hyperthreading is usually beneficial for Ceph servers.

Monitor nodes and Manager nodes do not have heavy CPU demands and require only
modest processors. if your hosts will run CPU-intensive processes in
addition to Ceph daemons, make sure that you have enough processing power to
run both the CPU-intensive processes and the Ceph daemons. (OpenStack Nova is
one example of a CPU-intensive process.) We recommend that you run
non-Ceph CPU-intensive processes on separate hosts (that is, on hosts that are
not your Monitor and Manager nodes) in order to avoid resource contention.
If your cluster deployes the Ceph Object Gateway, RGW daemons may co-reside
with your Mon and Manager services if the nodes have sufficient resources.

RAM
===

Generally, more RAM is better.  Monitor / Manager nodes for a modest cluster
might do fine with 64GB; for a larger cluster with hundreds of OSDs 128GB
is advised.

.. tip:: when we speak of RAM and storage requirements, we often describe
	 the needs of a single daemon of a given type.  A given server as
	 a whole will thus need at least the sum of the needs of the
	 daemons that it hosts as well as resources for logs and other operating
	 system components.  Keep in mind that a server's need for RAM
	 and storage will be greater at startup and when components
	 fail or are added and the cluster rebalances.  In other words,
	 allow headroom past what you might see used during a calm period
	 on a small initial cluster footprint.

There is an :confval:`osd_memory_target` setting for BlueStore OSDs that
defaults to 4GB.  Factor in a prudent margin for the operating system and
administrative tasks (like monitoring and metrics) as well as increased
consumption during recovery:  provisioning ~8GB *per BlueStore OSD* is thus
advised.

Monitors and managers (ceph-mon and ceph-mgr)
---------------------------------------------

Monitor and manager daemon memory usage scales with the size of the
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

CephFS metadata daemon memory utilization depends on the configured size of
its cache. We recommend 1 GB as a minimum for most systems.  See
:confval:`mds_cache_memory_limit`.


Memory
======

Bluestore uses its own memory to cache data rather than relying on the
operating system's page cache. In Bluestore you can adjust the amount of memory
that the OSD attempts to consume by changing the :confval:`osd_memory_target`
configuration option.

- Setting the :confval:`osd_memory_target` below 2GB is not
  recommended. Ceph may fail to keep the memory consumption under 2GB and
  extremely slow performance is likely.

- Setting the memory target between 2GB and 4GB typically works but may result
  in degraded performance: metadata may need to be read from disk during IO
  unless the active data set is relatively small.

- 4GB is the current default value for :confval:`osd_memory_target` This default
  was chosen for typical use cases, and is intended to balance RAM cost and
  OSD performance.

- Setting the :confval:`osd_memory_target` higher than 4GB can improve
  performance when there many (small) objects or when large (256GB/OSD
  or more) data sets are processed.  This is especially true with fast
  NVMe OSDs.

.. important:: OSD memory management is "best effort". Although the OSD may
   unmap memory to allow the kernel to reclaim it, there is no guarantee that
   the kernel will actually reclaim freed memory within a specific time
   frame. This applies especially in older versions of Ceph, where transparent
   huge pages can prevent the kernel from reclaiming memory that was freed from
   fragmented huge pages. Modern versions of Ceph disable transparent huge
   pages at the application level to avoid this, but that does not
   guarantee that the kernel will immediately reclaim unmapped memory. The OSD
   may still at times exceed its memory target. We recommend budgeting
   at least 20% extra memory on your system to prevent OSDs from going OOM
   (**O**\ut **O**\f **M**\emory) during temporary spikes or due to delay in
   the kernel reclaiming freed pages. That 20% value might be more or less than
   needed, depending on the exact configuration of the system.

.. tip:: Configuring the operating system with swap to provide additional
	 virtual memory for daemons is not advised for modern systems.  Doing
	 may result in lower performance, and your Ceph cluster may well be
	 happier with a daemon that crashes vs one that slows to a crawl.

When using the legacy FileStore back end, the OS page cache was used for caching
data, so tuning was not normally needed. When using the legacy FileStore backend,
the OSD memory consumption was related to the number of PGs per daemon in the
system.


Data Storage
============

Plan your data storage configuration carefully. There are significant cost and
performance tradeoffs to consider when planning for data storage. Simultaneous
OS operations and simultaneous requests from multiple daemons for read and
write operations against a single drive can impact performance.

OSDs require substantial storage drive space for RADOS data. We recommend a
minimum drive size of 1 terabyte. OSD drives much smaller than one terabyte
use a significant fraction of their capacity for metadata, and drives smaller
than 100 gigabytes will not be effective at all.

It is *strongly* suggested that (enterprise-class) SSDs are provisioned for, at a
minimum, Ceph Monitor and Ceph Manager hosts, as well as CephFS Metadata Server
metadata pools and Ceph Object Gateway (RGW) index pools, even if HDDs are to
be provisioned for bulk OSD data.

To get the best performance out of Ceph, provision the following on separate
drives:

* The operating systems
* OSD data
* BlueStore WAL+DB

For more
information on how to effectively use a mix of fast drives and slow drives in
your Ceph cluster, see the `block and block.db`_ section of the Bluestore
Configuration Reference.

Hard Disk Drives
----------------

Consider carefully the cost-per-gigabyte advantage
of larger disks. We recommend dividing the price of the disk drive by the
number of gigabytes to arrive at a cost per gigabyte, because larger drives may
have a significant impact on the cost-per-gigabyte. For example, a 1 terabyte
hard disk priced at $75.00 has a cost of $0.07 per gigabyte (i.e., $75 / 1024 =
0.0732). By contrast, a 3 terabyte disk priced at $150.00 has a cost of $0.05
per gigabyte (i.e., $150 / 3072 = 0.0488). In the foregoing example, using the
1 terabyte disks would generally increase the cost per gigabyte by
40%--rendering your cluster substantially less cost efficient.

.. tip:: Hosting multiple OSDs on a single SAS / SATA HDD
   is **NOT** a good idea.

.. tip:: Hosting an OSD with monitor, manager, or MDS data on a single
   drive is also **NOT** a good idea.

.. tip:: With spinning disks, the SATA and SAS interface increasingly
   becomes a bottleneck at larger capacities. See also the `Storage Networking
   Industry Association's Total Cost of Ownership calculator`_.


Storage drives are subject to limitations on seek time, access time, read and
write times, as well as total throughput. These physical limitations affect
overall system performance--especially during recovery. We recommend using a
dedicated (ideally mirrored) drive for the operating system and software, and
one drive for each Ceph OSD Daemon you run on the host.
Many "slow OSD" issues (when they are not attributable to hardware failure)
arise from running an operating system and multiple OSDs on the same drive.
Also be aware that today's 22TB HDD uses the same SATA interface as a
3TB HDD from ten years ago: more than seven times the data to squeeze
through the same interface.  For this reason, when using HDDs for
OSDs, drives larger than 8TB may be best suited for storage of large
files / objects that are not at all performance-sensitive.


Solid State Drives
------------------

Ceph performance is much improved when using solid-state drives (SSDs). This
reduces random access time and reduces latency while increasing throughput.

SSDs cost more per gigabyte than do HDDs but SSDs often offer
access times that are, at a minimum, 100 times faster than HDDs.
SSDs avoid hotspot issues and bottleneck issues within busy clusters, and
they may offer better economics when TCO is evaluated holistically. Notably,
the amortized drive cost for a given number of IOPS is much lower with SSDs
than with HDDs.  SSDs do not suffer rotational or seek latency and in addition
to improved client performance, they substantially improve the speed and
client impact of cluster changes including rebalancing when OSDs or Monitors
are added, removed, or fail.

SSDs do not have moving mechanical parts, so they are not subject
to many of the limitations of HDDs.  SSDs do have significant
limitations though. When evaluating SSDs, it is important to consider the
performance of sequential and random reads and writes.

.. important:: We recommend exploring the use of SSDs to improve performance.
   However, before making a significant investment in SSDs, we **strongly
   recommend** reviewing the performance metrics of an SSD and testing the
   SSD in a test configuration in order to gauge performance.

Relatively inexpensive SSDs may appeal to your sense of economy. Use caution.
Acceptable IOPS are not the only factor to consider when selecting SSDs for
use with Ceph. Bargain SSDs are often a false economy: they may experience
"cliffing", which means that after an initial burst, sustained performance
once a limited cache is filled declines considerably.  Consider also durability:
a drive rated for 0.3 Drive Writes Per Day (DWPD or equivalent) may be fine for
OSDs dedicated to certain types of sequentially-written read-mostly data, but
are not a good choice for Ceph Monitor duty.  Enterprise-class SSDs are best
for Ceph:  they almost always feature power less protection (PLP) and do
not suffer the dramatic cliffing that client (desktop) models may experience.

When using a single (or mirrored pair) SSD for both operating system boot
and Ceph Monitor / Manager purposes, a minimum capacity of 256GB is advised
and at least 480GB is recommended. A drive model rated at 1+ DWPD (or the
equivalent in TBW (TeraBytes Written) is suggested.  However, for a given write
workload, a larger drive than technically required will provide more endurance
because it effectively has greater overprovsioning. We stress that
enterprise-class drives are best for production use, as they feature power
loss protection and increased durability compared to client (desktop) SKUs
that are intended for much lighter and intermittent duty cycles.

SSDs have historically been cost prohibitive for object storage, but
QLC SSDs are closing the gap, offering greater density with lower power
consumption and less power spent on cooling. Also, HDD OSDs may see a
significant write latency improvement by offloading WAL+DB onto an SSD.
Many Ceph OSD deployments do not require an SSD with greater endurance than
1 DWPD (aka "read-optimized").  "Mixed-use" SSDs in the 3 DWPD class are
often overkill for this purpose and cost signficantly more.

To get a better sense of the factors that determine the total cost of storage,
you might use the `Storage Networking Industry Association's Total Cost of
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

One way that Ceph accelerates CephFS file system performance is by separating
the storage of CephFS metadata from the storage of the CephFS file contents.
Ceph provides a default ``metadata`` pool for CephFS metadata. You will never
have to manually create a pool for CephFS metadata, but you can create a CRUSH map
hierarchy for your CephFS metadata pool that includes only SSD storage media.
See :ref:`CRUSH Device Class<crush-map-device-class>` for details.


Controllers
-----------

Disk controllers (HBAs) can have a significant impact on write throughput.
Carefully consider your selection of HBAs to ensure that they do not create a
performance bottleneck. Notably, RAID-mode (IR) HBAs may exhibit higher latency
than simpler "JBOD" (IT) mode HBAs. The RAID SoC, write cache, and battery
backup can substantially increase hardware and maintenance costs. Many RAID
HBAs can be configured with an IT-mode "personality" or "JBOD mode" for
streamlined operation.

You do not need an RoC (RAID-capable) HBA. ZFS or Linux MD software mirroring
serve well for boot volume durability.  When using SAS or SATA data drives,
forgoing HBA RAID capabilities can reduce the gap between HDD and SSD
media cost.  Moreover, when using NVMe SSDs, you do not need *any* HBA.  This
additionally reduces the HDD vs SSD cost gap when the system as a whole is
considered. The initial cost of a fancy RAID HBA plus onboard cache plus
battery backup (BBU or supercapacitor) can easily exceed more than 1000 US
dollars even after discounts - a sum that goes a log way toward SSD cost parity.
An HBA-free system may also cost hundreds of US dollars less every year if one
purchases an annual maintenance contract or extended warranty.

.. tip:: The `Ceph blog`_ is often an excellent source of information on Ceph
   performance issues. See `Ceph Write Throughput 1`_ and `Ceph Write
   Throughput 2`_ for additional details.


Benchmarking
------------

BlueStore opens storage devices with ``O_DIRECT`` and issues ``fsync()``
frequently to ensure that data is safely persisted to media. You can evaluate a
drive's low-level write performance using ``fio``. For example, 4kB random write
performance is measured as follows:

.. code-block:: console

  # fio --name=/dev/sdX --ioengine=libaio --direct=1 --fsync=1 --readwrite=randwrite --blocksize=4k --runtime=300

Write Caches
------------

Enterprise SSDs and HDDs normally include power loss protection features which
ensure data durability when power is lost while operating, and
use multi-level caches to speed up direct or synchronous writes.  These devices
can be toggled between two caching modes -- a volatile cache flushed to
persistent media with fsync, or a non-volatile cache written synchronously.

These two modes are selected by either "enabling" or "disabling" the write
(volatile) cache.  When the volatile cache is enabled, Linux uses a device in
"write back" mode, and when disabled, it uses "write through".

The default configuration (usually: caching is enabled) may not be optimal, and
OSD performance may be dramatically increased in terms of increased IOPS and
decreased commit latency by disabling this write cache.

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

In most cases, disabling this cache  using ``hdparm``, ``sdparm``, or ``smartctl``
results in the cache_type changing automatically to "write through". If this is
not the case, you can try setting it directly as follows. (Users should ensure
that setting cache_type also correctly persists the caching mode of the device
until the next reboot as some drives require this to be repeated at every boot):

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

Ceph operators typically provision  multiple OSDs per host, but you should
ensure that the aggregate throughput of your OSD drives doesn't exceed the
network bandwidth required to service a client's read and write operations.
You should also each host's percentage of the cluster's overall capacity. If
the percentage located on a particular host is large and the host fails, it
can lead to problems such as recovery causing OSDs to exceed the ``full ratio``,
which in turn causes Ceph to halt operations to prevent data loss.

When you run multiple OSDs per host, you also need to ensure that the kernel
is up to date. See `OS Recommendations`_ for notes on ``glibc`` and
``syncfs(2)`` to ensure that your hardware performs as expected when running
multiple OSDs per host.


Networks
========

Provision at least 10 Gb/s networking in your datacenter, both among Ceph
hosts and between clients and your Ceph cluster.  Network link active/active
bonding across separate network switches is strongly recommended both for
increased throughput and for tolerance of network failures and maintenance.
Take care that your bonding hash policy distributes traffic across links.

Speed
-----

It takes three hours to replicate 1 TB of data across a 1 Gb/s network and it
takes thirty hours to replicate 10 TB across a 1 Gb/s network. But it takes only
twenty minutes to replicate 1 TB across a 10 Gb/s network, and it takes
only one hour to replicate 10 TB across a 10 Gb/s network.

Note that a 40 Gb/s network link is effectively four 10 Gb/s channels in
parallel, and that a 100Gb/s network link is effectively four 25 Gb/s channels
in parallel.  Thus, and perhaps somewhat counterintuitively, an individual
packet on a 25 Gb/s network has slightly lower latency compared to a 40 Gb/s
network.


Cost
----

The larger the Ceph cluster, the more common OSD failures will be.
The faster that a placement group (PG) can recover from a degraded state to
an ``active + clean`` state, the better. Notably, fast recovery minimizes
the likelihood of multiple, overlapping failures that can cause data to become
temporarily unavailable or even lost. Of course, when provisioning your
network, you will have to balance price against performance.

Some deployment tools employ VLANs to make hardware and network cabling more
manageable. VLANs that use the 802.1q protocol require VLAN-capable NICs and
switches. The added expense of this hardware may be offset by the operational
cost savings on network setup and maintenance. When using VLANs to handle VM
traffic between the cluster and compute stacks (e.g., OpenStack, CloudStack,
etc.), there is additional value in using 10 Gb/s Ethernet or better; 40 Gb/s or
increasingly 25/50/100 Gb/s networking as of 2022 is common for production clusters.

Top-of-rack (TOR) switches also need fast and redundant uplinks to
core / spine network switches or routers, often at least 40 Gb/s.


Baseboard Management Controller (BMC)
-------------------------------------

Your server chassis should have a Baseboard Management Controller (BMC).
Well-known examples are iDRAC (Dell), CIMC (Cisco UCS), and iLO (HPE).
Administration and deployment tools may also use BMCs extensively, especially
via IPMI or Redfish, so consider the cost/benefit tradeoff of an out-of-band
network for security and administration.  Hypervisor SSH access, VM image uploads,
OS image installs, management sockets, etc. can impose significant loads on a network.
Running multiple networks may seem like overkill, but each traffic path represents
a potential capacity, throughput and/or performance bottleneck that you should
carefully consider before deploying a large scale data cluster.

Additionally BMCs as of 2023 rarely sport network connections faster than 1 Gb/s,
so dedicated and inexpensive 1 Gb/s switches for BMC administrative traffic
may reduce costs by wasting fewer expenive ports on faster host switches.


Failure Domains
===============

A failure domain can be thought of as any component loss that prevents access to
one or more OSDs or other Ceph daemons. These could be a stopped daemon on a host;
a storage drive failure, an OS crash, a malfunctioning NIC, a failed power supply,
a network outage, a power outage, and so forth. When planning your hardware
deployment, you must balance the risk of reducing costs by placing too many
responsibilities into too few failure domains against the added costs of
isolating every potential failure domain.


Minimum Hardware Recommendations
================================

Ceph can run on inexpensive commodity hardware. Small production clusters
and development clusters can run successfully with modest hardware.  As
we noted above: when we speak of CPU _cores_, we mean _threads_ when
hyperthreading (HT) is enabled.  Each modern physical x64 CPU core typically
provides two logical CPU threads; other CPU architectures may vary.

Take care that there are many factors that influence resource choices.  The
minimum resources that suffice for one purpose will not necessarily suffice for
another.  A sandbox cluster with one OSD built on a laptop with VirtualBox or on
a trio of Raspberry PIs will get by with fewer resources than a production
deployment with a thousand OSDs serving five thousand of RBD clients.  The
classic Fisher Price PXL 2000 captures video, as does an IMAX or RED camera.
One would not expect the former to do the job of the latter.  We especially
cannot stress enough the criticality of using enterprise-quality storage
media for production workloads.

Additional insights into resource planning for production clusters are
found above and elsewhere within this documentation.

+--------------+----------------+-----------------------------------------+
|  Process     | Criteria       | Bare Minimum and Recommended            |
+==============+================+=========================================+
| ``ceph-osd`` | Processor      | - 1 core minimum, 2 recommended         |
|              |                | - 1 core per 200-500 MB/s throughput    |
|              |                | - 1 core per 1000-3000 IOPS             |
|              |                |                                         |
|              |                | * Results are before replication.       |
|              |                | * Results may vary across CPU and drive |
|              |                |   models and Ceph configuration:        |
|              |                |   (erasure coding, compression, etc)    |
|              |                | * ARM processors specifically may       |
|              |                |   require more cores for performance.   |
|              |                | * SSD OSDs, especially NVMe, will       |
|              |                |   benefit from additional cores per OSD.|
|              |                | * Actual performance depends on many    |
|              |                |   factors including drives, net, and    |
|              |                |   client throughput and latency.        |
|              |                |   Benchmarking is highly recommended.   |
|              +----------------+-----------------------------------------+
|              | RAM            | - 4GB+ per daemon (more is better)      |
|              |                | - 2-4GB may function but may be slow    |
|              |                | - Less than 2GB is not recommended      |
|              +----------------+-----------------------------------------+
|              | Storage Drives |  1x storage drive per OSD               |
|              +----------------+-----------------------------------------+
|              | DB/WAL         |  1x SSD partion per HDD OSD             |
|              | (optional)     |  4-5x HDD OSDs per DB/WAL SATA SSD      |
|              |                |  <= 10 HDD OSDss per DB/WAL NVMe SSD    |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1Gb/s (bonded 10+ Gb/s recommended) |
+--------------+----------------+-----------------------------------------+
| ``ceph-mon`` | Processor      | - 2 cores minimum                       |
|              +----------------+-----------------------------------------+
|              | RAM            |  5GB+ per daemon (large / production    |
|              |                |  clusters need more)                    |
|              +----------------+-----------------------------------------+
|              | Storage        |  100 GB per daemon, SSD is recommended  |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1Gb/s (10+ Gb/s recommended)        |
+--------------+----------------+-----------------------------------------+
| ``ceph-mds`` | Processor      | - 2 cores minimum                       |
|              +----------------+-----------------------------------------+
|              | RAM            |  2GB+ per daemon (more for production)  |
|              +----------------+-----------------------------------------+
|              | Disk Space     |  1 GB per daemon                        |
|              +----------------+-----------------------------------------+
|              | Network        |  1x 1Gb/s (10+ Gb/s recommended)        |
+--------------+----------------+-----------------------------------------+

.. tip:: If you are running an OSD node with a single storage drive, create a
   partition for your OSD that is separate from the partition
   containing the OS. We recommend separate drives for the
   OS and for OSD storage.



.. _block and block.db: https://docs.ceph.com/en/latest/rados/configuration/bluestore-config-ref/#block-and-block-db
.. _Ceph blog: https://ceph.com/community/blog/
.. _Ceph Write Throughput 1: http://ceph.com/community/ceph-performance-part-1-disk-controller-write-throughput/
.. _Ceph Write Throughput 2: http://ceph.com/community/ceph-performance-part-2-write-throughput-without-ssd-journals/
.. _Mapping Pools to Different Types of OSDs: ../../rados/operations/crush-map#placing-different-pools-on-different-osds
.. _OS Recommendations: ../os-recommendations
.. _Storage Networking Industry Association's Total Cost of Ownership calculator: https://www.snia.org/forums/cmsi/programs/TCOcalc
.. _Werner Fischer's blog post on partition alignment: https://www.thomas-krenn.com/en/wiki/Partition_Alignment_detailed_explanation
