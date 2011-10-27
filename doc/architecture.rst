======================
 Ceph Product Overview
======================

======================
About this Document
======================
This document describes the features and benefits of using the Ceph Unified Distributed Storage System, and why it is superior to other  systems.  
The audience for this document consists of sales and marketing personnel, new customers, and all persons who need to get a basic overview of the features and functionality of the system.
======================
Introduction to Ceph
======================
Ceph is a unified, distributed  file system that operates on a large number of hosts connected by a network.  Ceph has been designed to accommodate multiple  petabytes of storage with ease.  Since file sizes and network systems are always increasing, Ceph is perfectly positioned to accommodate these new technologies with its unique, self-healing and self-replicating architecture.   Customers that need to move large amounts of metadata, such as media and entertainment companies, can greatly benefit from this product. Ceph is also dynamic;  no need to cache data like those old-fashioned client-servers!
Benefits of Using Ceph
Ceph’s flexible and scalable architecture translates into cost savings for users.  Its powerful load balancing technology ensures the highest performance in terms of both speed and 
reliability.  Nodes can be added “on the fly” with no impact to the system. In the case of node failure, the load is re-distributed with no degradation to the system. 
 Failure detection is rapid and immediately remedied by efficiently re-adding nodes that were temporarily cut off from the network.
Manageability
Ceph is easy to manage, requiring little or no system administrative intervention.  Its powerful placement algorithm and intelligent nodes manage data seamlessly across any node
configuration.  It also features multiple access methods to its object storage, block storage, and file systems.  Figure 1 displays this configuration.
<img> CephConfig.jpg.
======================RADOS======================
The Reliable Autonomic Distributed Object Store (RADOS) provides a scalable object storage management platform.  RADOS allows the Object Storage Devices (OSD) to operate autonomously 
when recovering from failures or migrating data to expand clusters.   RADOS employs existing node device intelligence to maximized scalability.
The RADOS Block Device (RBD) provides a block device interface to a Linux machine, while striping the data across multiple RADOS objects for improved performance.  
RDB is supported for Linux kernels 2.6.37 and higher.  Each RDB device contains a directory with files and information

======================
RADOS GATEWAY
======================
The RADOS Gateway, ``radosgw``, is an S3-compatible RESTful HTTP service for object
storage, using RADOS storage.
======================
RBD
======================
The RADOS Block Device (RBD) provides a block device interface to a Linux machine.  To the user, RDB is transparent, which means that the entire Ceph system looks like a single, 
limitless hard drive that is always up and has no size limitations.  .



Hypervisor Support
======================
RBD supports the QEMU processor emulator and the Kernel-based Virtual Machine (KVM) virtualization infrastructure for the Linux kernel.  Normally, these hypervisors would not be used 
together in a single configuration.
KVM RBD
The Linux Kernel-based Virtual Machine (KVM) RBD provides the functionality for striping data across multiple distributed RADOS objects for improved performance.  
KVM-RDB is supported for Linux kernels 2.6.37 and higher.  Each RDB device contains a directory with files and information.  
KVM employs the XEN hypervisor to manage its virtual machines.  
QEMU RBD
QEMU-RBD facilitates striping a VM block device over objects stored in the Ceph distributed object store. This provides shared block storage to facilitate VM migration between hosts. 
QEMU has its own hypervisor which interfaces with the librdb user-space library to store its virtual machines

===============
Monitors
===============

Once you have determined your configuration needs, make sure you have access to the following documents:
•	Ceph Installation and Configuration Guide
•	Ceph System Administration Guide
•	Ceph Troubleshooting Manual
.

Glossary
KVM - Kernel-based Virtual Machine virtualization infrastructure for the Linux kernel.  KVM requires the QEMU hypervisor for virtualization.
QEMU - A virtualizer used to execute guest code directly on the host CPU. QEMU supports virtualization when executing under the XEN hypervisor or the KVM kernel module in Linux. 
RBD - RADOS Block Device.  See the Ceph Product Overview for more on RADOS.
REST - Representational State Transfer architecture for distributed hypermedia systems. 
XEN - A virtual-machine monitor that uses a thin software layer known as the XEN hypervisor to allow each physical server to run one or more virtual servers.
===============
RBD Snapshots
===============
RBD provides the ability to create snapshots of any image for backup purposes.  These images can then be exported to any file on the cluster.  The following drawing shows how this feature 
can be used to create clones of an image called the Golden Master Snapshot.  
<img> snapshots.jpg
These clones can then be ready to be used as backups any time an image goes down or needs to be duplicated for a new configuration.



.. _Paxos: http://en.wikipedia.org/wiki/Paxos_algorithm

.. todo:: explain monmap


.. index:: RADOS, OSD, ceph-osd, object
.. _rados:

RADOS
=====

``ceph-osd`` is the storage daemon that provides the RADOS service. It
uses ``ceph-mon`` for cluster membership, services object read/write/etc
request from clients, and peers with other ``ceph-osd``\s for data
replication.

The data model is fairly simple on this level. There are multiple
named pools, and within each pool there are named objects, in a flat
namespace (no directories). Each object has both data and metadata.

The data for an object is a single, potentially big, series of
bytes. Additionally, the series may be sparse, it may have holes that
contain binary zeros, and take up no actual storage.

The metadata is an unordered set of key-value pairs. It's semantics
are completely up to the client; for example, the Ceph filesystem uses
metadata to store file owner etc.

.. todo:: Verify that metadata is unordered.

Underneath, ``ceph-osd`` stores the data on a local filesystem. We
recommend using Btrfs_, but any POSIX filesystem that has extended
attributes should work (see :ref:`xattr`).

.. _Btrfs: http://en.wikipedia.org/wiki/Btrfs

.. todo:: write about access control

.. todo:: explain osdmap

.. todo:: explain plugins ("classes")


.. index:: Ceph filesystem, Ceph Distributed File System, MDS, ceph-mds
.. _cephfs:

Ceph filesystem
===============

The Ceph filesystem service is provided by a daemon called
``ceph-mds``. It uses RADOS to store all the filesystem metadata
(directories, file ownership, access modes, etc), and directs clients
to access RADOS directly for the file contents.

The Ceph filesystem aims for POSIX compatibility, except for a few
chosen differences. See :doc:`/appendix/differences-from-posix`.

``ceph-mds`` can run as a single process, or it can be distributed out to
multiple physical machines, either for high availability or for
scalability.

For high availability, the extra ``ceph-mds`` instances can be `standby`,
ready to take over the duties of any failed ``ceph-mds`` that was
`active`. This is easy because all the data, including the journal, is
stored on RADOS. The transition is triggered automatically by
``ceph-mon``.

For scalability, multiple ``ceph-mds`` instances can be `active`, and they
will split the directory tree into subtrees (and shards of a single
busy directory), effectively balancing the load amongst all `active`
servers.

Combinations of `standby` and `active` etc are possible, for example
running 3 `active` ``ceph-mds`` instances for scaling, and one `standby`.

To control the number of `active` ``ceph-mds``\es, see
:doc:`/ops/manage/grow/mds`.

.. topic:: Status as of 2011-09:

   Multiple `active` ``ceph-mds`` operation is stable under normal
   circumstances, but some failure scenarios may still cause
   operational issues.

.. todo:: document `standby-replay`

.. todo:: mds.0 vs mds.alpha etc details


.. index:: RADOS Gateway, radosgw
.. _radosgw:

``radosgw``
===========

``radosgw`` is a FastCGI service that provides a RESTful_ HTTP API to
store objects and metadata. It layers on top of RADOS with its own
data formats, and maintains it's own user database, authentication,
access control, and so on.

.. _RESTful: http://en.wikipedia.org/wiki/RESTful


.. index:: RBD, Rados Block Device
.. _rbd:

Rados Block Device (RBD)
========================

In virtual machine scenarios, RBD is typically used via the ``rbd``
network storage driver in Qemu/KVM, where the host machine uses
``librbd`` to provide a block device service to the guest.

Alternatively, as no direct ``librbd`` support is available in Xen,
the Linux kernel can act as the RBD client and provide a real block
device on the host machine, that can then be accessed by the
virtualization. This is done with the command-line tool ``rbd`` (see
:doc:`/ops/rbd`).

The latter is also useful in non-virtualized scenarios.

Internally, RBD stripes the device image over multiple RADOS objects,
each typically located on a separate ``ceph-osd``, allowing it to perform
better than a single server could.


Client
======

.. todo:: cephfs, ceph-fuse, librados, libcephfs, librbd


.. todo:: Summarize how much Ceph trusts the client, for what parts (security vs reliability).


TODO
====

.. todo:: Example scenarios Ceph projects are/not suitable for
