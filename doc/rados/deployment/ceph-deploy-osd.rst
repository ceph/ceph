=================
 Add/Remove OSDs
=================

Adding and removing OSDs may involve a few more steps when compared to adding
and removing other Ceph daemons. OSDs write data to the disk and to journals. So
you need to provide paths for the OSD and journal.

By default, ``ceph-deploy`` will create an OSD with the XFS filesystem. You may
override this by providing a ``--fs-type FS_TYPE`` argument, where ``FS_TYPE`` 
is an alternate filesystem such as ``ext4`` or ``btrfs``.

In Ceph v0.60 and later releases, Ceph supports ``dm-crypt`` on disk encryption.
You may specify the ``--dm-crypt`` argument when preparing an OSD to tell
``ceph-deploy`` that you want to use encryption. You may also specify the
``--dmcrypt-key-dir`` argument to specify the location of ``dm-crypt``
encryption keys.


List Disks
==========

To list the disks on a host, execute the following command:: 

	ceph-deploy disk list {host-name [host-name]...}

Or: 

	ceph-disk-prepare list {host-name [host-name]...}


Zap Disks
=========

To zap a disk (delete its partition table) in preparation for use with Ceph,
execute the following::

	ceph-deploy disk zap {osd-server-name}:/path/to/disk

.. important:: This will delete all data in the partition.


Prepare OSDs
============

Once you create a cluster, install Ceph packages, and gather keys, you
may prepare the OSDs and deploy them to the OSD host(s). If you need to 
identify a disk or zap it prior to preparing it for use as an OSD, 
see `List Disks`_ and `Zap Disks`_. ::

	ceph-deploy osd prepare {host-name}:{path/to/disk}[:{path/to/journal}]
	ceph-deploy osd prepare osdserver1:/dev/sdb1:/dev/ssd1

The ``prepare`` command only prepares the OSD. It does not activate it. To
activate a prepared OSD, use the ``activate`` command. See `Activate OSDs`_ 
for details.


Activate OSDs
=============

Once you prepare an OSD you may activate it with the following command.  ::

	ceph-deploy osd activate {host-name}:{path/to/disk}[:{path/to/journal}]
	ceph-deploy osd activate osdserver1:/dev/sdb1:/dev/ssd1

The ``activate`` command will cause your OSD to come ``up`` and be placed
``in`` the cluster.


Create OSDs
===========

You may prepare OSDs, deploy them to the OSD host(s) and activate them in one
step with the ``create`` command. The ``create`` command is a convenience method
for executing the ``prepare`` and ``activate`` command sequentially.  ::

	ceph-deploy osd create {host-name}:{path-to-disk}[:{path/to/journal}]
	ceph-deploy osd create osdserver1:/dev/sdb1:/dev/ssd1

List OSDs
=========

To list the OSDs deployed on a host(s), execute the following command:: 

	ceph-deploy osd list {host-name}


Destroy OSDs
============

To destroy an OSD, execute the following command:: 

	ceph-deploy osd destroy {host-name}:{path-to-disk}[:{path/to/journal}]

Destroying an OSD will take it ``down`` and ``out`` of the cluster.

