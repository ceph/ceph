=====================
Ceph Product Overview
=====================

About this Document
===================

This document describes the features and benefits of using the Ceph
Unified Distributed Storage System, and why it is superior to other
systems.

The audience for this document consists of sales and marketing
personnel, new customers, and all persons who need to get a basic
overview of the features and functionality of the system.

Introduction to Ceph
====================

Ceph is a unified, distributed file system that operates on a large
number of hosts connected by a network.  Ceph has been designed to
accommodate multiple petabytes of storage with ease.  Since file sizes
and network systems are always increasing, Ceph is perfectly
positioned to accommodate these new technologies with its unique,
self-healing and self-replicating architecture.  Customers that need
to move large amounts of metadata, such as media and entertainment
companies, can greatly benefit from this product. Ceph is also
dynamic; no need to cache data like those old-fashioned
client-servers!

Benefits of Using Ceph
======================

Ceph's flexible and scalable architecture translates into cost savings
for users.  Its powerful load balancing technology ensures the highest
performance in terms of both speed and reliability.  Nodes can be
added "on the fly" with no impact to the system. In the case of node
failure, the load is re-distributed with no degradation to the system.

Failure detection is rapid and immediately remedied by efficiently
re-adding nodes that were temporarily cut off from the network.

Manageability
=============

Ceph is easy to manage, requiring little or no system administrative
intervention.  Its powerful placement algorithm and intelligent nodes
manage data seamlessly across any node configuration.  It also
features multiple access methods to its object storage, block storage,
and file systems.  Figure 1 displays this configuration.

.. image:: /images/CEPHConfig.jpg

RADOS
=====

The Reliable Autonomic Distributed Object Store (RADOS) provides a
scalable object storage management platform.  RADOS allows the Object
Storage Devices (OSD) to operate autonomously when recovering from
failures or migrating data to expand clusters.  RADOS employs existing
node device intelligence to maximized scalability.

The RADOS Block Device (RBD) provides a block device interface to a
Linux machine, while striping the data across multiple RADOS objects
for improved performance.  RDB is supported for Linux kernels 2.6.37
and higher.  Each RDB device contains a directory with files and
information

RADOS GATEWAY
=============

``radosgw`` is an S3-compatible RESTful HTTP service for object
storage, using RADOS storage.

The RADOS Block Device (RBD) provides a block device interface to a
Linux machine.  To the user, RDB is transparent, which means that the
entire Ceph system looks like a single, limitless hard drive that is
always up and has no size limitations.  .


Hypervisor Support
==================

RBD supports the QEMU processor emulator and the Kernel-based Virtual
Machine (KVM) virtualization infrastructure for the Linux kernel.
Normally, these hypervisors would not be used together in a single
configuration.

KVM RBD
-------

The Linux Kernel-based Virtual Machine (KVM) RBD provides the
functionality for striping data across multiple distributed RADOS
objects for improved performance.

KVM-RDB is supported for Linux kernels 2.6.37 and higher.  Each RDB
device contains a directory with files and information.

KVM employs the XEN hypervisor to manage its virtual machines.

QEMU RBD
--------

QEMU-RBD facilitates striping a VM block device over objects stored in
the Ceph distributed object store. This provides shared block storage
to facilitate VM migration between hosts.

QEMU has its own hypervisor which interfaces with the librdb
user-space library to store its virtual machines

Monitors
========

Once you have determined your configuration needs, make sure you have
access to the following documents:

- Ceph Installation and Configuration Guide
- Ceph System Administration Guide
- Ceph Troubleshooting Manual
