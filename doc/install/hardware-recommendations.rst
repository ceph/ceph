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

`Inktank`_ provides excellent premium support for hardware planning.

.. _Inktank: http://www.inktank.com


CPU
===

Ceph metadata servers dynamically redistribute their load, which is CPU
intensive. So your metadata servers should have significant processing power
(e.g., quad core or better CPUs). Ceph OSDs run the RADOS service, calculate
data placement with CRUSH, replicate data, and maintain their own copy of the
cluster map. Therefore, OSDs should have a reasonable amount of processing power
(e.g., dual-core processors). Monitors simply maintain a master copy of the
cluster map, so they are not CPU intensive. You must also consider whether the
host machine will run CPU-intensive processes in addition to Ceph daemons. For
example, if your hosts will run computing VMs (e.g., OpenStack Nova), you will
need to ensure that these other processes leave sufficient processing power for
Ceph daemons. We recommend running additional CPU-intensive processes on
separate hosts.


RAM
===

Metadata servers and monitors must be capable of serving their data quickly, so
they should have plenty of RAM (e.g., 1GB of RAM per daemon instance). OSDs do
not require as much RAM (e.g., 500MB of RAM per daemon instance). Generally,
more RAM is better.


Data Storage
============

Plan your data storage configuration carefully, because there are significant
opportunities for performance improvement by incurring the added cost of using
solid state drives (SSDs), and there are significant cost-per-gigabyte
considerations with hard disk drives. Metadata servers and monitors don't use a
lot of storage space. A metadata server requires approximately 1MB of storage
space per daemon instance. A monitor requires approximately 10GB of storage
space per daemon instance. One opportunity for performance improvement is to use
solid-state drives to reduce random access time and read latency while
accelerating throughput. Solid state drives cost more than 10x as much per
gigabyte when compared to a hard disk, but they often exhibit access times that
are at least 100x faster than a hard disk drive. Since the storage requirements
for metadata servers and monitors are so low, solid state drives may provide an
economical opportunity to improve performance. 

.. important:: We recommend exploring the use of SSDs to improve performance. 
   However, before making a significant investment in SSDs, we **strongly
   recommend** both reviewing the performance metrics of an SSD and testing the
   SSD in a test configuration to gauge performance. SSD write latency may 
   **NOT** improve performance compared to a high performance hard disk. 
   Inexpensive SSDs may introduce write latency even as they accelerate 
   access time, because sometimes hard drives will write faster than SSDs!

OSDs should have plenty of disk space. We recommend a minimum disk size of 1
terabyte. We recommend dividing the price of the hard disk drive by the number
of gigabytes to arrive at a cost per gigabyte, because larger drives may have a
significant impact on the cost-per-gigabyte. For example, a 1 terabyte hard disk
priced at $75.00 has a cost  of $0.07 per gigabyte (i.e., $75 / 1024 = 0.0732).
By contrast, a 3 terabyte hard  disk priced at $150.00 has a cost of $0.05 per
gigabyte (i.e., $150 / 3072 = 0.0488). In the foregoing example, using the 1
terabyte disks would generally increase the cost per gigabyte by 40%--rendering
your cluster substantially less cost efficient. For OSD hosts, we recommend
using an OS disk for the operating system and software, and one disk for each
OSD daemon you run on the host. While solid state drives are cost prohibitive
for object storage, OSDs may see a performance improvement by storing an OSD's
journal on a solid state drive and the OSD's object data on a hard disk drive.
You may run multiple OSDs per host, but you should ensure that the sum of the
total throughput of your OSD hard disks doesn't exceed the network bandwidth
required to service a client's need to read or write data. You should also
consider what percentage of the cluster's data storage is on each host. If the
percentage is large and the host fails, it can lead to problems such as
exceeding the ``full ratio``,  which causes Ceph to halt operations as a safety
precaution that prevents data loss.



Networks
========

We recommend that each host have at least two 1Gbps network interface
controllers (NICs). Since most commodity hard disk drives have a throughput of
approximately 100MB/second, your NICs should be able to handle the traffic for
the OSD disks on your host. We recommend a minimum of two NICs to account for a
public (front-side) network and a cluster (back-side) network. A cluster network
(preferably not connected to the internet) handles the additional load for data
replication and helps stop denial of service attacks that prevent the cluster
from achieving ``active + clean`` states for placement groups as OSDs replicate
data across the cluster. Consider starting with a 10Gbps network in your racks.
Replicating 1TB of data across a 1Gbps network takes 3 hours, and 3TBs (a
typical drive configuration) takes 9 hours. By contrast, with a 10Gbps network,
the  replication times would be 20 minutes and 1 hour respectively. In a
petabyte-scale cluster, failure of an OSD disk should be an expectation, not an
exception. System administrators will appreciate PGs recovering from a
``degraded`` state to an ``active + clean`` state as rapidly as possible, with
price / performance tradeoffs taken into consideration. Additionally, some
deployment tools  (e.g., Dell's Crowbar) deploy with five different networks,
but employ VLANs to make hardware and network cabling more manageable. VLANs
using 802.1q protocol require VLAN-capable NICs and Switches. The added hardware
expense may be offset by the operational cost savings for network setup and
maintenance. When using VLANs to handle VM traffic between between the cluster
and compute stacks (e.g., OpenStack, CloudStack, etc.), it is also worth
considering using 10G Ethernet. Top-of-rack routers for each network also need
to be able to communicate with spine routers that have even faster
throughput--e.g.,  40Gbps to 100Gbps.

Your server hardware should have a Baseboard Management Controller (BMC).
Administration and deployment tools may also use BMCs extensively, so consider
the cost/benefit tradeoff of an out-of-band network for administration.
Hypervisor SSH access, VM image uploads, OS image installs, management sockets,
etc. can impose significant loads on a network.  Running three networks may seem
like overkill, but each traffic path represents a potential capacity, throughput
and/or performance bottleneck that you should carefully consider before
deploying a large scale data cluster.
 

Failure Domains
===============

A failure domain is any failure that prevents access to one or more OSDs. That
could be a stopped daemon on a host; a hard disk failure,  an OS crash, a
malfunctioning NIC, a failed power supply, a network outage, a power outage, and
so forth. When planning out your hardware needs, you must balance the
temptation to reduce costs by placing too many responsibilities into too few
failure domains, and the added costs of isolating every potential failure
domain.


Minimum Hardware Recommendations
================================

Ceph can run on inexpensive commodity hardware. Small production clusters
and development clusters can run successfully with modest hardware.

+--------------+----------------+------------------------------------+
|  Process     | Criteria       | Minimum Recommended                |
+==============+================+====================================+
| ``ceph-osd`` | Processor      |  1x 64-bit AMD-64/i386 dual-core   |
|              +----------------+------------------------------------+
|              | RAM            |  500 MB per daemon                 |
|              +----------------+------------------------------------+
|              | Volume Storage |  1x Disk per daemon                |
|              +----------------+------------------------------------+
|              | Network        |  2x 1GB Ethernet NICs              |
+--------------+----------------+------------------------------------+
| ``ceph-mon`` | Processor      |  1x 64-bit AMD-64/i386             |
|              +----------------+------------------------------------+
|              | RAM            |  1 GB per daemon                   |
|              +----------------+------------------------------------+
|              | Disk Space     |  10 GB per daemon                  |
|              +----------------+------------------------------------+
|              | Network        |  2x 1GB Ethernet NICs              |
+--------------+----------------+------------------------------------+
| ``ceph-mds`` | Processor      |  1x 64-bit AMD-64/i386 quad-core   |
|              +----------------+------------------------------------+
|              | RAM            |  1 GB minimum per daemon           |
|              +----------------+------------------------------------+
|              | Disk Space     |  1 MB per daemon                   |
|              +----------------+------------------------------------+
|              | Network        |  2x 1GB Ethernet NICs              |
+--------------+----------------+------------------------------------+

.. tip:: If you are running an OSD with a single disk, create a
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
| Dell PE R510   | Processor      |  2x 64-bit quad-core Xeon CPUs     |
|                +----------------+------------------------------------+
|                | RAM            |  16 GB                             |
|                +----------------+------------------------------------+
|                | Volume Storage |  8x 2TB drives. 1 OS, 7 Storage    |
|                +----------------+------------------------------------+
|                | Client Network |  2x 1GB Ethernet NICs              |
|                +----------------+------------------------------------+
|                | OSD Network    |  2x 1GB Ethernet NICs              |
|                +----------------+------------------------------------+
|                | Mgmt. Network  |  2x 1GB Ethernet NICs              |
+----------------+----------------+------------------------------------+
| Dell PE R515   | Processor      |  1x hex-core Opteron CPU           |
|                +----------------+------------------------------------+
|                | RAM            |  16 GB                             |
|                +----------------+------------------------------------+
|                | Volume Storage |  12x 3TB drives. Storage           |
|                +----------------+------------------------------------+
|                | OS Storage     |  1x 500GB drive. Operating System. |
|                +----------------+------------------------------------+
|                | Client Network |  2x 1GB Ethernet NICs              |
|                +----------------+------------------------------------+
|                | OSD Network    |  2x 1GB Ethernet NICs              |
|                +----------------+------------------------------------+
|                | Mgmt. Network  |  2x 1GB Ethernet NICs              |
+----------------+----------------+------------------------------------+
