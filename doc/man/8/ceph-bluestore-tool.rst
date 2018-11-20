:orphan:

======================================================
 ceph-bluestore-tool -- bluestore administrative tool
======================================================

.. program:: ceph-bluestore-tool

Synopsis
========

| **ceph-bluestore-tool** *command*
  [ --dev *device* ... ]
  [ --path *osd path* ]
  [ --out-dir *dir* ]
  [ --log-file | -l *filename* ]
  [ --deep ]
| **ceph-bluestore-tool** fsck|repair --path *osd path* [ --deep ]
| **ceph-bluestore-tool** show-label --dev *device* ...
| **ceph-bluestore-tool** prime-osd-dir --dev *device* --path *osd path*
| **ceph-bluestore-tool** bluefs-export --path *osd path* --out-dir *dir*
| **ceph-bluestore-tool** bluefs-bdev-new-wal --path *osd path* --dev-target *new-device*
| **ceph-bluestore-tool** bluefs-bdev-new-db --path *osd path* --dev-target *new-device*
| **ceph-bluestore-tool** bluefs-bdev-migrate --path *osd path* --dev-target *new-device* --devs-source *device1* [--devs-source *device2*]


Description
===========

**ceph-bluestore-tool** is a utility to perform low-level administrative
operations on a BlueStore instance.

Commands
========

:command:`help`

   show help

:command:`fsck` [ --deep ]

   run consistency check on BlueStore metadata.  If *--deep* is specified, also read all object data and verify checksums.

:command:`repair`

   Run a consistency check *and* repair any errors we can.

:command:`bluefs-export`

   Export the contents of BlueFS (i.e., rocksdb files) to an output directory.

:command:`bluefs-bdev-sizes` --path *osd path*

   Print the device sizes, as understood by BlueFS, to stdout.

:command:`bluefs-bdev-expand` --path *osd path*

   Instruct BlueFS to check the size of its block devices and, if they have expanded, make use of the additional space.

:command:`bluefs-bdev-new-wal` --path *osd path* --dev-target *new-device*

   Adds WAL device to BlueFS, fails if WAL device already exists.

:command:`bluefs-bdev-new-db` --path *osd path* --dev-target *new-device*

   Adds DB device to BlueFS, fails if DB device already exists.
   
:command:`bluefs-bdev-migrate` --dev-target *new-device* --devs-source *device1* [--devs-source *device2*]

   Moves BlueFS data from source device(s) to the target one, source devices
   (except the main one) are removed on success. Target device can be both
   already attached or new device. In the latter case it's added to OSD
   replacing one of the source devices. Following replacement rules apply
   (in the order of precedence, stop on the first match):

      - if source list has DB volume - target device replaces it.
      - if source list has WAL volume - target device replace it.
      - if source list has slow volume only - operation isn't permitted, requires explicit allocation via new-db/new-wal command.

:command:`show-label` --dev *device* [...]

   Show device label(s).	   

Options
=======

.. option:: --dev *device*

   Add *device* to the list of devices to consider

.. option:: --devs-source *device*

   Add *device* to the list of devices to consider as sources for migrate operation

.. option:: --dev-target *device*

   Specify target *device* migrate operation or device to add for adding new DB/WAL.

.. option:: --path *osd path*

   Specify an osd path.  In most cases, the device list is inferred from the symlinks present in *osd path*.  This is usually simpler than explicitly specifying the device(s) with --dev.

.. option:: --out-dir *dir*

   Output directory for bluefs-export

.. option:: -l, --log-file *log file*

   file to log to

.. option:: --log-level *num*

   debug log level.  Default is 30 (extremely verbose), 20 is very
   verbose, 10 is verbose, and 1 is not very verbose.

.. option:: --deep

   deep scrub/repair (read and validate object data, not just metadata)

Device labels
=============

Every BlueStore block device has a single block label at the beginning of the
device.  You can dump the contents of the label with::

  ceph-bluestore-tool show-label --dev *device*

The main device will have a lot of metadata, including information
that used to be stored in small files in the OSD data directory.  The
auxiliary devices (db and wal) will only have the minimum required
fields (OSD UUID, size, device type, birth time).

OSD directory priming
=====================

You can generate the content for an OSD data directory that can start up a
BlueStore OSD with the *prime-osd-dir* command::

  ceph-bluestore-tool prime-osd-dir --dev *main device* --path /var/lib/ceph/osd/ceph-*id*


Availability
============

**ceph-bluestore-tool** is part of Ceph, a massively scalable,
open-source, distributed storage system. Please refer to the Ceph
documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph-osd <ceph-osd>`\(8)
