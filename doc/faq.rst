============================
 Frequently Asked Questions
============================

These questions have been frequently asked on the ceph-users and ceph-devel 
mailing lists, the IRC channel, and on the `Ceph.com`_ blog.

.. _Ceph.com: http://ceph.com


Is Ceph Production-Quality?
===========================

Ceph's object store is production ready. Large-scale storage systems (i.e.,
petabytes of data) use Ceph's RESTful object store, which provides APIs
compatible with Amazon's S3 and OpenStack's Swift.  Many deployments also use
the Ceph block device, including deployments of  OpenStack and CloudStack.
`Inktank`_ provides commercial support for the Ceph object store, RESTful
interfaces, block devices and CephFS with running a single metadata server.

The CephFS POSIX-compliant filesystem is functionally complete and has been
evaluated by a large community of users. There are production systems using
CephFS with a single metadata server. The Ceph community is actively testing
clusters with multiple metadata servers for quality assurance. Once CephFS
passes QA muster when running with multiple metadata servers, `Inktank`_ will
provide commercial support for CephFS with multiple metadata servers, too.

.. _Inktank: http://inktank.com


What Kind of Hardware Does Ceph Require?
========================================

Ceph runs on commodity hardware. A typical configuration involves a
rack mountable server with a baseboard management controller, multiple
processors, multiple drives, and multiple NICs. There are no requirements for
proprietary hardware. For details, see `Ceph Hardware Recommendations`_.


What Kind of OS Does Ceph Require?
==================================

Ceph runs on Linux. Most Ceph users run a Debian/Ubuntu distribution, which you
can install from `APT packages`_. Ceph builds `RPM packages`_ for Federa/RHEL
too. You can also download Ceph source `tarballs`_ and build Ceph for your
distribution. See `Installation`_ for details.


How Many OSDs Can I Run per Host?
=================================

Theoretically, a host can run as many OSDs as the hardware can support.  Many
vendors market storage hosts that have large numbers of drives (e.g., 36 drives)
capable of supporting many OSDs. We don't recommend a huge number of OSDs per
host though.  Ceph was designed to distribute the load across what we call
"failure domains." See `CRUSH Maps`_ for details.

At the petabyte scale, hardware failure is an expectation, not a freak
occurrence. Failure domains include datacenters, rooms, rows, racks, and network
switches. In a single host, power supplies, motherboards, NICs, and drives are
all potential points of failure.

If you place a large percentage of your OSDs on a single host and that host
fails, a large percentage of your OSDs will fail too.  Having too large a
percentage of a cluster's OSDs on a single host can cause disruptive data
migration and long recovery times during host failures. We encourage
diversifying the risk across failure domains, and that includes making
reasonable tradeoffs regarding the number of OSDs per host.


Can I Use the Same Drive for Multiple OSDs?
===========================================

Yes. **Please don't do this!** Except for initial evaluations of Ceph, we do not
recommend running multiple OSDs on the same drive. In fact,  we recommend
**exactly** the opposite. Only run one OSD per drive. For better performance,
run journals on a separate drive from the OSD drive, and consider using SSDs for
journals. Run operating systems on a separate drive from any drive storing data
for Ceph.

Storage drives are a performance bottleneck. Total throughput is an important
consideration. Sequential reads and writes are important considerations too.
When you run multiple OSDs per drive, you split up the total throughput between
competing OSDs, which can slow performance  considerably. 


Why Do You Recommend One Drive Per OSD?
=======================================

Ceph OSD performance is one of the most common requests for assistance, and
running an OS, a journal and an OSD on the same disk is a frequently the
impediment to high performance. Total throughput and simultaneous reads and
writes are a major bottleneck. If you journal data, run an OS, or run multiple
OSDs on the same drive, you will very likely see performance degrade
significantly--especially under high loads.

Running multiple OSDs on a single drive is fine for evaluation purposes. We
even encourage that in our `5-minute quick start`_. However, just because it 
works does NOT mean that it will provide acceptable performance in an 
operational cluster.


What Underlying Filesystem Do You Recommend?
============================================

Currently, we recommend using XFS as the underlying filesystem for OSD drives.
We think ``btrfs`` will become the optimal filesystem. However, we still
encounter enough issues that we do not recommend it for production systems yet.
See `Filesystem Recommendations`_ for details.


How Does Ceph Ensure Data Integrity Across Replicas?
====================================================

Ceph periodically scrubs placement groups to ensure that they contain the same
information. Low-level or deep scrubbing reads the object data in each replica
of the placement group to ensure that the data is identical across replicas.


How Many NICs Per Host?
=======================

You can use one :abbr:`NIC (Network Interface Card)` per machine. We recommend a
minimum of two NICs: one for a public (front-side) network and one for a cluster
(back-side) network. When you write an object from the client to the primary
OSD, that single write only accounts for the bandwidth consumed during one leg
of the transaction. If you store multiple copies (usually 2-3 copies in a
typical cluster), the primary OSD makes a write request to your secondary and
tertiary OSDs. So your back-end network traffic can dwarf your front-end network
traffic on writes very easily.


What Kind of Network Throughput Do I Need?
==========================================

Network throughput requirements depend on your load. We recommend starting with
a minimum of 1GB Ethernet. 10GB Ethernet is more expensive, but often comes with
some additional advantages,  including virtual LANs (VLANs). VLANs can
dramatically reduce the cabling requirements when you run front-side, back-side
and other special purpose networks.

The number of object copies (replicas) you create is an important factor,
because replication becomes a larger network load than the initial write itself
when making multiple copies (e.g., triplicate). Network traffic between Ceph and
a cloud-based system such as OpenStack or CloudStack may also become a factor.
Some deployments even run a separate NIC for management APIs. 

Finally load spikes are a factor too. Certain times of the day, week or month
you may see load spikes. You must plan your network capacity to meet those load
spikes in order for Ceph to perform well. This means that excess capacity may
remain idle or unused during low load times.


Can Ceph Support Multiple Data Centers?
=======================================

Yes, but with safeguards to ensure data safety. When a client writes data to
Ceph the primary OSD will not acknowledge the write to the client until the
secondary OSDs have written the replicas synchronously. See `How Ceph Scales`_
for details.

The Ceph community is working to ensure that OSD/monitor heartbeats and peering
processes operate effectively with the additional latency that may occur when
deploying hardware in different geographic locations. See `Monitor/OSD
Interaction`_ for details.

If your data centers have dedicated bandwidth and low latency, you can
distribute your cluster across data centers easily. If you use a WAN over the
Internet, you may need to configure Ceph to ensure effective peering, heartbeat
acknowledgement and writes to ensure the cluster performs well with additional
WAN latency.

Dedicated connections are expensive, so people tend to avoid them. The Ceph
community is exploring asynchronous writes to make distributing a cluster across
data centers without significant changes to the default settings (e.g.,
timeouts). 


How Does Ceph Authenticate Users?
=================================

Ceph provides an authentication framework called ``cephx`` that operates in a
manner similar to  Kerberos. The principal difference is that Ceph's
authentication system is distributed too, so that it doesn't constitute a single
point of failure. For details, see `Ceph Authentication & Authorization`_.


Does Ceph Authentication Provide Multi-tenancy?
===============================================

Ceph provides authentication at the `pool`_ level, which may be sufficient 
for multi-tenancy in limited cases. Ceph plans on developing authentication
namespaces within pools in future releases, so that Ceph is well-suited for
multi-tenancy within pools.


Can Ceph use other Multi-tenancy Modules?
=========================================

The Bobtail release of Ceph integrates RADOS Gateway with OpenStack's Keystone.
See `Keystone Integration`_ for details.

.. _Keystone Integration: ../radosgw/config#integrating-with-openstack-keystone


Does Ceph Enforce Quotas?
=========================

Currently, Ceph doesn't provide enforced storage quotas. The Ceph community has
discussed enforcing user quotas within CephFS.


Does Ceph Track Per User Usage?
===============================

The CephFS filesystem provides user-based usage tracking on a subtree basis.
RADOS Gateway also provides detailed per-user usage tracking. RBD and the
underlying object store do not track per user statistics. The underlying object
store provides storage capacity utilization statistics.


Does Ceph Provide Billing?
==========================

Ceph does not provide billing functionality at this time. Improvements to
pool-based namespaces and pool-based usage tracking may make it feasible to use
Ceph usage statistics with usage tracking and billing systems in the future.


Can Ceph Export a Filesystem via NFS or Samba/CIFS?
===================================================

Ceph doesn't export CephFS via NFS or Samba. However, you can use a gateway to
serve a CephFS filesystem to NFS or Samba clients. 


Can I Access Ceph via a Hypervisor?
===================================

Currently, the `QEMU`_ hypervisor can interact with the Ceph `block device`_.
The :abbr:`KVM (Kernel Virtual Machine)` `module`_ and the `librbd` library
allow you to use QEMU with Ceph. Most Ceph deployments use the `librbd` library. 
Cloud solutions like `OpenStack`_ and `CloudStack`_ interact `libvirt`_ and QEMU
to as a means of integrating with Ceph.

Ceph integrates cloud solutions via ``libvirt`` and QEMU, but the Ceph community
is also talking about supporting the Xen hypervisor. Ceph and Citrix engineers
have built a prototype, but they have not released a stable means of integrating
Xen with Ceph for general use yet. Similarly, there is interest in support for
VMWare, but there is no deep-level integration between VMWare and Ceph as yet.


Can Block, CephFS, and Gateway Clients Share Data?
==================================================

For the most part, no. You cannot write data to Ceph using RBD and access the
same data via CephFS, for example. You cannot write data with RADOS gateway and
read it with RBD. However, you can write data with the RADOS Gateway
S3-compatible API and read the same data using the RADOS Gateway
Swift-comptatible API.

RBD, CephFS and the RADOS Gateway each have their own namespace. The way they
store data differs significantly enough that it isn't possible to use the
clients interchangeably. However, you can use all three types of clients, and
clients you develop yourself via ``librados`` simultaneously on the same
cluster.


Which Ceph Clients Support Striping? 
====================================

Ceph clients--RBD, CephFS and RADOS Gateway--providing striping capability. For
details on  striping, see `Striping`_.


What Programming Languages can Interact with the Object Store?
==============================================================

Ceph's ``librados`` is written in the C programming language. There are
interfaces for other languages, including: 

- C++
- Java
- PHP
- Python
- Ruby


Can I Develop a Client With Another Language?
=============================================

Ceph does not have many native bindings for ``librados`` at this time. If you'd
like to fork Ceph and build a wrapper to the C or C++ versions of ``librados``,
please check out the `Ceph repository`_. You can also use other languages that
can use the ``librados`` native bindings (e.g., you can access the C/C++ bindings
from within Perl).


Do Ceph Clients Run on Windows? 
===============================

No. There are no immediate plans to support Windows clients at this time. However, 
you may be able to emulate a Linux environment on a Windows host. For example, 
Cygwin may make it feasible to use ``librados`` in an emulated environment.


How can I add a question to this list?
======================================

If you'd like to add a question to this list (hopefully with an
accompanying answer!), you can find it in the doc/ directory of our
main git repository:

	`https://github.com/ceph/ceph/blob/master/doc/faq.rst`_


We use Sphinx to manage our documentation, and this page is generated
from reStructuredText source.  See the section on Building Ceph
Documentation for the build procedure.



.. _Ceph Hardware Recommendations: ../install/hardware-recommendations
.. _APT packages: ../install/debian
.. _RPM packages: ../install/rpm
.. _tarballs: ../install/get-tarballs
.. _Installation: ../install
.. _CRUSH Maps: ../rados/operations/crush-map
.. _5-minute quick start: ../start/quick-start
.. _How Ceph Scales: ../architecture#how-ceph-scales
.. _Monitor/OSD Interaction: ../rados/configuration/mon-osd-interaction
.. _Ceph Authentication & Authorization: ../rados/operations/auth-intro
.. _Ceph repository: https://github.com/ceph/ceph
.. _QEMU: ../rbd/qemu-rbd
.. _block device: ../rbd
.. _module: ../rbd/rbd-ko
.. _libvirt: ../rbd/libvirt
.. _OpenStack: ../rbd/rbd-openstack
.. _CloudStack: ../rbd/rbd-cloudstack
.. _pool: ../rados/operations/pools
.. _Striping: ../architecture##how-ceph-clients-stripe-data
.. _https://github.com/ceph/ceph/blob/master/doc/faq.rst: https://github.com/ceph/ceph/blob/master/doc/faq.rst
.. _Filesystem Recommendations: ../rados/configuration/filesystem-recommendations
