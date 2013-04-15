=============================
 Block Devices and CloudStack
=============================

You may use Ceph block device images with CloudStack 4.0 and higher through
``libvirt``, which configures the QEMU interface to ``librbd``. Ceph stripes
block device images as objects across the cluster, which means that large Ceph
block device images have better performance than a standalone server!

To use Ceph block devices with CloudStack 4.0 and higher, you must install QEMU,
``libvirt``, and CloudStack first. We recommend using a separate physical host
for your CloudStack installation. CloudStack recommends a minimum of 4GB of RAM
and a dual-core processor, but more CPU and RAM will perform better. The
following diagram depicts the CloudStack/Ceph technology stack.


.. ditaa::  +---------------------------------------------------+
            |                   CloudStack                      |
            +---------------------------------------------------+
            |                     libvirt                       |
            +------------------------+--------------------------+
                                     |
                                     | configures
                                     v
            +---------------------------------------------------+
            |                       QEMU                        |
            +---------------------------------------------------+
            |                      librbd                       |
            +---------------------------------------------------+
            |                     librados                      |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. important:: To use Ceph block devices with CloudStack, you must have a 
   running Ceph cluster.

CloudStack integrates with Ceph's block devices to provide CloudStack with a
back end for CloudStack's Primary Storage. The instructions below detail the
setup for CloudStack Primary Storage.

.. note:: We recommend installing with Ubuntu 12.04 or later so that 
   you can use package installation instead of having to compile 
   QEMU from source.
   
Installing and configuring QEMU for use with CloudStack doesn't require any
special handling. Ensure that you have a running Ceph  cluster. Install QEMU and
configure it for use with Ceph; then, install ``libvirt`` version 0.9.13 or
higher (you may need to compile from source) and ensure it is running with Ceph.

#. `Install and Configure QEMU`_.
#. `Install and Configure libvirt`_ version 0.9.13 or higher.
#. Also see `KVM Hypervisor Host Installation`_.


.. note:: Raring Ringtail (13.04) will have ``libvirt`` verison 0.9.13 or higher
   by default.

Create a Pool
=============

By default, Ceph block devices use the ``rbd`` pool. Create a pool for
CloudStack NFS Primary Storage. Ensure your Ceph cluster is running, then create
the pool. ::

   ceph osd pool create cloudstack
   
See `Create a Pool`_ for details on specifying the number of placement groups
for your pools, and `Placement Groups`_ for details on the number of placement
groups you should set for your pools.


Add Primary Storage
===================

To add primary storage, refer to `Add Primary Storage (4.0.0)`_ or 
`Add Primary Storage (4.0.1)`_. To add a Ceph block device, the steps
include: 

#. Log in to the CloudStack UI.
#. Click **Infrastructure** on the left side navigation bar. 
#. Select the Zone you want to use for Primary Storage.
#. Click the **Compute** tab.
#. Select **View All** on the `Primary Storage` node in the diagram.
#. Click **Add Primary Storage**.
#. Follow the CloudStack instructions.

   - For **Protocol**, select ``RBD``.
   - Add cluster information (cephx is supported).
   - Add ``rbd`` as a tag.


Create a Disk Offering
======================

To create a new disk offering, refer to `Create a New Disk Offering (4.0.0)`_ or
 `Create a New Disk Offering (4.0.1)`_. Create a disk offering so that it
matches the ``rbd`` tag. The ``StoragePoolAllocator`` will choose the  ``rbd``
pool when searching for a suitable storage pool. If the disk offering doesn't
match the ``rbd`` tag, the ``StoragePoolAllocator`` may select the pool you
created (e.g., ``cloudstack``).


Limitations
===========

- CloudStack will only bind to one monitor.
- CloudStack does not support cloning snapshots.
- You may need to compile ``libvirt`` to use version 0.9.13 with Ubuntu.



.. _Create a Pool: ../../rados/operations/pools#createpool
.. _Placement Groups: ../../rados/operations/placement-groups
.. _Install and Configure QEMU: ../qemu-rbd
.. _Install and Configure libvirt: ../libvirt
.. _KVM Hypervisor Host Installation: http://cloudstack.apache.org/docs/en-US/Apache_CloudStack/4.0.0-incubating/html/Installation_Guide/hypervisor-kvm-install-flow.html
.. _Add Primary Storage (4.0.0): http://cloudstack.apache.org/docs/en-US/Apache_CloudStack/4.0.0-incubating/html/Admin_Guide/primary-storage-add.html
.. _Add Primary Storage (4.0.1): http://cloudstack.apache.org/docs/en-US/Apache_CloudStack/4.0.1-incubating/html/Admin_Guide/primary-storage-add.html
.. _Create a New Disk Offering (4.0.0): http://cloudstack.apache.org/docs/en-US/Apache_CloudStack/4.0.0-incubating/html/Admin_Guide/compute-disk-service-offerings.html#creating-disk-offerings
.. _Create a New Disk Offering (4.0.1): http://cloudstack.apache.org/docs/en-US/Apache_CloudStack/4.0.1-incubating/html/Admin_Guide/compute-disk-service-offerings.html#creating-disk-offerings