=================
 Add/Remove OSDs
=================

Adding and removing Ceph OSD Daemons to your cluster may involve a few more
steps when compared to adding and removing other Ceph daemons. Ceph OSD Daemons
write data to the disk and to journals. So you need to provide a disk for the
OSD and a path to the journal partition (i.e., this is the most common
configuration, but you may configure your system to  your own needs).

In Ceph v0.60 and later releases, Ceph supports ``dm-crypt`` on disk encryption.
You may specify the ``--dmcrypt`` argument when preparing an OSD to tell
``ceph-deploy`` that you want to use encryption. You may also specify the
``--dmcrypt-key-dir`` argument to specify the location of ``dm-crypt``
encryption keys.

You should test various drive configurations to gauge their throughput before
before building out a large cluster. See `Data Storage`_ for additional details.


List Disks
==========

To list the disks on a node, execute the following command:: 

	ceph-deploy disk list {node-name [node-name]...}


Zap Disks
=========

To zap a disk (delete its partition table) in preparation for use with Ceph,
execute the following::

	ceph-deploy disk zap {osd-server-name}:{disk-name}
	ceph-deploy disk zap osdserver1:sdb

.. important:: This will delete all data.


Prepare OSDs
============

Once you create a cluster, install Ceph packages, and gather keys, you
may prepare the OSDs and deploy them to the OSD node(s). If you need to 
identify a disk or zap it prior to preparing it for use as an OSD, 
see `List Disks`_ and `Zap Disks`_. ::

	ceph-deploy osd prepare {node-name}:{data-disk}[:{journal-disk}]
	ceph-deploy osd prepare osdserver1:sdb:/dev/ssd
	ceph-deploy osd prepare osdserver1:sdc:/dev/ssd

The ``prepare`` command only prepares the OSD. On most operating
systems, the ``activate`` phase will automatically run when the
partitions are created on the disk (using Ceph ``udev`` rules). If not
use the ``activate`` command. See `Activate OSDs`_ for
details.

The foregoing example assumes a disk dedicated to one Ceph OSD Daemon, and 
a path to an SSD journal partition. We recommend storing the journal on 
a separate drive to maximize throughput. You may dedicate a single drive
for the journal too (which may be expensive) or place the journal on the 
same disk as the OSD (not recommended as it impairs performance). In the
foregoing example we store the journal on a partitioned solid state drive.

You can use the settings --fs-type or --bluestore to choose which file system
you want to install in the OSD drive. (More information by running
'ceph-deploy osd prepare --help').

.. note:: When running multiple Ceph OSD daemons on a single node, and 
   sharing a partioned journal with each OSD daemon, you should consider
   the entire node the minimum failure domain for CRUSH purposes, because
   if the SSD drive fails, all of the Ceph OSD daemons that journal to it
   will fail too.


Activate OSDs
=============

Once you prepare an OSD you may activate it with the following command.  ::

	ceph-deploy osd activate {node-name}:{data-disk-partition}[:{journal-disk-partition}]
	ceph-deploy osd activate osdserver1:/dev/sdb1:/dev/ssd1
	ceph-deploy osd activate osdserver1:/dev/sdc1:/dev/ssd2

The ``activate`` command will cause your OSD to come ``up`` and be placed
``in`` the cluster. The ``activate`` command uses the path to the partition
created when running the ``prepare`` command.


Create OSDs
===========

You may prepare OSDs, deploy them to the OSD node(s) and activate them in one
step with the ``create`` command. The ``create`` command is a convenience method
for executing the ``prepare`` and ``activate`` command sequentially.  ::

	ceph-deploy osd create {node-name}:{disk}[:{path/to/journal}]
	ceph-deploy osd create osdserver1:sdb:/dev/ssd1

.. List OSDs
.. =========

.. To list the OSDs deployed on a node(s), execute the following command:: 

..	ceph-deploy osd list {node-name}


Destroy OSDs
============

.. note:: Coming soon. See `Remove OSDs`_ for manual procedures.

.. To destroy an OSD, execute the following command:: 

..	ceph-deploy osd destroy {node-name}:{path-to-disk}[:{path/to/journal}]

.. Destroying an OSD will take it ``down`` and ``out`` of the cluster.

.. _Data Storage: ../../../start/hardware-recommendations#data-storage
.. _Remove OSDs: ../../operations/add-or-rm-osds#removing-osds-manual
