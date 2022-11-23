===================
 Ceph Block Device
===================

.. index:: Ceph Block Device; introduction

**rbd** is a utility that can manipulate rados block device (RBD) images. It is
used by the Linux rbd driver and the rbd storage driver for QEMU/KVM. RBD
images are simple block devices that are striped over objects and stored in a
RADOS object store. 

RBD is a service that provides thin-provisioned, resizable block devices whose
features include snapshotting and cloning. 

Block-based storage interfaces are a mature and common way to store data on
media including HDDs, SSDs, CDs, floppy disks, and even tape. The ubiquity of
block device interfaces is a perfect fit for interacting with mass data storage
including Ceph.

RBD stores data striped over multiple OSDs. Ceph block devices leverage
:abbr:`RADOS (Reliable Autonomic Distributed Object Store)` capabilities
including snapshotting and replication, and they provide strong consistency.
Ceph block storage clients communicate with Ceph clusters by means of a Linux
kernel module or the ``librbd`` library.

.. ditaa::

            +------------------------+ +------------------------+
            |     Kernel Module      | |        librbd          |
            +------------------------+-+------------------------+
            |                   RADOS Protocol                  |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. note:: Kernel modules can use Linux page caching. For ``librbd``-based
   applications, Ceph supports `RBD Caching`_.

Ceph's block devices deliver high performance with vast scalability to
`kernel modules`_, or to :abbr:`KVMs (kernel virtual machines)` such as `QEMU`_, and
cloud-based computing systems like `OpenStack`_ and `CloudStack`_ that rely on
libvirt and QEMU to integrate with Ceph block devices. You can use the same cluster
to operate the :ref:`Ceph RADOS Gateway <object-gateway>`, the
:ref:`Ceph File System <ceph-file-system>`, and Ceph block devices simultaneously.

.. important:: To use Ceph Block Devices, you must have access to a running
   Ceph cluster.

.. toctree::
        :maxdepth: 1

	Basic Commands <rados-rbd-cmds>

.. toctree::
        :maxdepth: 2

        Operations <rbd-operations>

.. toctree::
	:maxdepth: 2

        Integrations <rbd-integrations>

.. toctree::
	:maxdepth: 2

	Manpages <man/index>

.. toctree::
	:maxdepth: 2

	APIs <api/index>

.. _RBD Caching: ./rbd-config-ref/
.. _kernel modules: ./rbd-ko/
.. _QEMU: ./qemu-rbd/
.. _OpenStack: ./rbd-openstack
.. _CloudStack: ./rbd-cloudstack

