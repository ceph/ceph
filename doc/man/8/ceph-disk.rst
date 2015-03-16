===================================================================
 ceph-disk -- Ceph disk preparation and activation utility for OSD
===================================================================

.. program:: ceph-disk

Synopsis
========

| **ceph-disk** **prepare** [--cluster *clustername*] [--cluster-uuid *uuid*]
	[--fs-type *xfs|ext4|btrfs*] [*data-path*] [*journal-path*]

| **ceph-disk** **activate** [*data-path*] [--activate-key *path*]

| **ceph-disk** **activate-all**

| **ceph-disk** **list**

Description
===========

**ceph-disk** is a utility that can prepare and activate a disk, partition or
directory as a ceph OSD. It is run directly or triggered by **ceph-deploy**
or udev.

It actually automates the multiple steps involved in manual creation and start
of an OSD into 2 steps of preparing and activating the OSD by using the
subcommands **prepare** and **activate**.

Subcommands
============

**prepare**: Prepare a directory, disk or drive for a ceph OSD. It creates a GPT
partition, marks the partition with ceph type uuid, creates a file system, marks
the file system as ready for ceph consumption, uses entire partition and adds a
new partition to the journal disk. It is run directly or triggered by
**ceph-deploy**.

Usage: ceph-disk prepare --cluster [cluster-name] --cluster-uuid [uuid] --fs-type
[ext4|xfs|btrfs] [data-path] [journal-path]

Other options like --osd-uuid, --journal-uuid, --zap-disk, --data-dir, --data-dev,
--journal-file, --journal-dev, --dmcrypt and --dmcrypt-key-dir can also be used
with the subcommand.

**activate**: Activate the ceph OSD. It mounts the volume in a temporary
location, allocates an OSD id (if needed), remounts in the correct location
/var/lib/ceph/osd/$cluster-$id and starts ceph-osd. It is triggered by udev
when it sees the OSD GPT partition type or on ceph service start with
'ceph disk activate-all'. It is also run directly or triggered by **ceph-deploy**.

Usage: ceph-disk activate [PATH]

Here, [PATH] is path to block device or directory.

An additional option [--activate-key PATH] has to be used with this subcommand
when a copy of /var/lib/ceph/bootstrap-osd/{cluster}.keyring isn't present in the
OSD node.

Usage: ceph-disk activate [PATH] [--activate-key PATH]

Another option --mark-init can also be used with this subcommand.

**activate-journal**: Activate an OSD via it's journal device. udev triggers
'ceph-disk activate-journal <dev>' based on the partition type.

Usage: ceph-disk activate-journal [DEV]

Here, [DEV] is the path to journal block device.

Others options can also be used with this subcommand like --activate-key and
--mark-init.

Usage: ceph-disk activate-journal [--activate-key PATH] [--mark-init INITSYSTEM]
[DEV]

**activate-all**: Activate all tagged OSD partitions. activate-all relies on
/dev/disk/by-parttype-uuid/$typeuuid.$uuid to find all partitions. Special udev
rules are installed to create these links. It is triggered on ceph service start
or run directly.

Usage: ceph-disk activate-all

Others options can also be used with this subcommand like --activate-key and
--mark-init.

Usage: ceph-disk activate-all [--activate-key PATH] [--mark-init INITSYSTEM]

**list**: List disk partitions and ceph OSDs. It is run directly or triggered
by **ceph-deploy**.

Usage: ceph-disk list

**suppress-activate**: Suppress activate on a device (prefix).
Mark devices that you want to suppress activate with a file like
/var/lib/ceph/tmp/suppress-activate.sdb where the last bit is
the sanitized device name (/dev/X without the /dev/ prefix). A
function is_suppressed() checks for and  matches a prefix (/dev/).
It means suppressing sdb will stop activate on sdb1, sdb2, etc.

Usage: ceph-disk suppress-activate [PATH]

Here, [PATH] is path to block device or directory.

**unsuppress-activate**: Stop suppressing activate on a device (prefix).

Usage: ceph-disk unsuppress-activate [PATH]

Here, [PATH] is path to block device or directory.

**zap**: Zap/erase/destroy a device's partition table and contents.
It actually uses 'sgdisk' and it's option '--zap-all' to destroy both
GPT and MBR data structures so that the disk becomes suitable for
repartitioning. 'sgdisk' then uses '--mbrtogpt' to convert the MBR or
BSD disklabel disk to a GPT disk. The **prepare** subcommand can now be
executed which will create a new GPT partition. It is also run directly
or triggered by **ceph-deploy**.

Usage: ceph-disk zap [DEV]

Here, [DEV] is path to block device.

Options
=======

.. option:: --prepend-to-path PATH

   Prepend PATH to $PATH for backward compatibility (default /usr/bin).

.. option:: --statedir PATH

   Directory in which ceph configuration is preserved (default /usr/lib/ceph).

.. option:: --sysconfdir PATH

   Directory in which ceph configuration files are found (default /etc/ceph).

.. option:: --cluster

   Provide name of the ceph cluster in which the OSD is being prepared.

.. option:: --cluster-uuid

   Provide uuid of the ceph cluster in which the OSD is being prepared.

.. option:: --fs-type

   Provide the filesytem type for the OSD. e.g. 'xfs/ext4/btrfs'.

.. option:: --osd-uuid

	Unique OSD uuid to assign to the disk.

.. option:: --journal-uuid

	Unique uuid to assign to the journal.

.. option:: --zap-disk

	Destroy the partition table and content of a disk.

.. option:: --data-dir

	Verify that [data-path] is of a directory.

.. option:: --data-dev

	Verify that [data-path] is of a block device.

.. option:: --journal-file

	Verify that journal is a file.

.. option:: --journal-dev

	Verify that journal is a block device.

.. option:: --dmcrypt

	Encrypt [data-path] and/or journal devices with dm-crypt.

.. option:: --dmcrypt-key-dir

	Directory where dm-crypt keys are stored.

.. option:: --activate-key

   Use when a copy of /var/lib/ceph/bootstrap-osd/{cluster}.keyring isn't 
   present in the OSD node. Suffix the option by the path to the keyring.

.. option:: --mark-init

   Provide init system to manage the OSD directory.

Availability
============

**ceph-disk** is a part of the Ceph distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.
