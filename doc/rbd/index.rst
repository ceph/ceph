===================
 Ceph Block Device
===================

.. index:: Ceph Block Device; introduction

A block is a sequence of bytes (for example, a 512-byte block of data).
Block-based storage interfaces are the most common way to store data with
rotating media such as hard disks, CDs, floppy disks, and even traditional
9-track tape. The ubiquity of block device interfaces makes a virtual block
device an ideal candidate to interact with a mass data storage system like Ceph.

Ceph block devices are thin-provisioned, resizable and store data striped over
multiple OSDs in a Ceph cluster.  Ceph block devices leverage
:abbr:`RADOS (Reliable Autonomic Distributed Object Store)` capabilities
such as snapshotting, replication and consistency. Ceph's
:abbr:`RADOS (Reliable Autonomic Distributed Object Store)` Block Devices (RBD)
interact with OSDs using kernel modules or the ``librbd`` library.

.. ditaa::  +------------------------+ +------------------------+
            |     Kernel Module      | |        librbd          |
            +------------------------+-+------------------------+
            |                   RADOS Protocol                  |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. note:: Kernel modules can use Linux page caching. For ``librbd``-based
   applications, Ceph supports `RBD Caching`_.

Ceph's block devices deliver high performance with infinite scalability to
`kernel modules`_, or to :abbr:`KVMs (kernel virtual machines)` such as `QEMU`_, and
cloud-based computing systems like `OpenStack`_ and `CloudStack`_ that rely on
libvirt and QEMU to integrate with Ceph block devices. You can use the same cluster
to operate the `Ceph RADOS Gateway`_, the `Ceph FS filesystem`_, and Ceph block
devices simultaneously.

.. important:: To use Ceph Block Devices, you must have access to a running
   Ceph cluster.

.. toctree::
	:maxdepth: 1

	Commands <rados-rbd-cmds>
	Kernel Modules <rbd-ko>
	Snapshots<rbd-snapshot>
	Mirroring <rbd-mirroring>
	LIO iSCSI Gateway <iscsi-overview>
	QEMU <qemu-rbd>
	libvirt <libvirt>
	Cache Settings <rbd-config-ref/>
	OpenStack <rbd-openstack>
	CloudStack <rbd-cloudstack>
	RBD Replay <rbd-replay>

.. toctree::
	:maxdepth: 2

	Manpages <man/index>

.. toctree::
	:maxdepth: 2

	APIs <api/index>

.. _RBD Caching: ../rbd-config-ref/
.. _kernel modules: ../rbd-ko/
.. _QEMU: ../qemu-rbd/
.. _OpenStack: ../rbd-openstack
.. _CloudStack: ../rbd-cloudstack
.. _Ceph RADOS Gateway: ../../radosgw/
.. _Ceph FS filesystem: ../../cephfs/
