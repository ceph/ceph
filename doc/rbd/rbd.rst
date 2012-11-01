===============
 Block Devices
===============

A block is a sequence of bytes (for example, a 512-byte block of data). 
Block-based storage interfaces are the most common way to store data with 
rotating media such as hard disks, CDs, floppy disks, and even traditional 
9-track tape. The ubiquity  of block device interfaces makes a virtual block 
device an ideal candidate to interact with a mass data storage system like Ceph.

Ceph's RADOS Block Devices (RBD) interact with OSDs using the 
``librados`` and ``librbd`` libraries. RBDs are thin-provisioned, resizable 
and store data striped over multiple OSDs in a Ceph cluster. RBDs inherit
``librados`` capabilities such as snapshotting and cloning. Ceph's RBDs deliver 
high performance with infinite scalability to kernel objects, kernel virtual 
machines and cloud-based computing systems like OpenStack and CloudStack.

The ``librbd`` library converts data blocks into objects for storage in
RADOS OSD clusters--the same storage system for ``librados`` object stores and 
the Ceph FS filesystem. You can use the same cluster to operate object stores, 
the Ceph FS filesystem, and RADOS block devices simultaneously.

.. important:: To use RBD, you must have a running Ceph cluster.

.. toctree::
	:maxdepth: 1

	Commands <rados-rbd-cmds>
	Kernel Objects <rbd-ko>
	Snapshots<rbd-snapshot>
	QEMU <qemu-rbd>
	libvirt <libvirt>
	OpenStack <rbd-openstack>
	CloudStack <rbd-cloudstack>
	
	
	
