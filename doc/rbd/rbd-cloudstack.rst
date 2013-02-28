===========================
 RBD and Apache CloudStack
===========================

You can use RBD to run instances on in Apache CloudStack.

This can be done by adding a RBD pool as Primary Storage.

There are a couple of prerequisites:

* You need to use CloudStack 4.0 or higher
* Qemu on the Hypervisor has to be compiled with RBD enabled
* The libvirt version on the Hypervisor has to be at least 0.10 with RBD enabled

Make sure you meet these requirements before installing the CloudStack Agent on the Hypervisor(s).

.. important:: To use RBD with CloudStack, you must have a running Ceph cluster.

Limitations
===========

Running instances from RBD has a couple of limitations:

* An additional NFS Primary Storage pool is required for running System VM's
* Snapshotting RBD volumes is not possible (at this moment)
* Only one monitor can be configured

Add Hypervisor
==============

Please follow the official CloudStack documentation how to do this.

There is no special way of adding a Hypervisor when using RBD, nor is any configuration needed on the hypervisor.

Add RBD Primary Storage
=======================

Once the hypervisor has been added, log on to the CloudStack UI.

* Infrastructure 
* Primary Storage
* "Add Primary Storage"
* Select "Protocol" RBD
* Fill in your cluster information (cephx is supported)
* Optionally add the tag 'rbd'

Now you should be able to deploy instances on RBD.

RBD Disk Offering
=================

Create a special "Disk Offering" which needs to match the tag 'rbd' so you can make sure the StoragePoolAllocator
chooses the RBD pool when searching for a suiteable storage pool.

Since there is also a NFS storage pool it's possible that instances get deployed on NFS instead of RBD.
