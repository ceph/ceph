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
| **ceph-bluestore-tool** free-dump|free-score --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]


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

:command:`show-label` --dev *device* [...]

   Show device label(s).	   

:command:`free-dump` --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]

   Dump all free regions in allocator.

:command:`free-score` --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]

   Give a [0-1] number that represents quality of fragmentation in allocator.
   0 represents case when all free space is in one chunk. 1 represents worst possible fragmentation.

Options
=======

.. option:: --dev *device*

   Add *device* to the list of devices to consider

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

.. option:: --allocator *name*

   Useful for *free-dump* and *free-score* actions. Selects allocator(s).

Device labels
=============

Every BlueStore block device has a single block label at the beginning of the
device.  You can dump the contents of the label with::

  ceph-bluestore-tool show-label --dev *device*

The main device will have a lot of metadata, including information
that used to be stored in small files in the OSD data directory.  The
auxilliary devices (db and wal) will only have the minimum required
fields (OSD UUID, size, device type, birth time).

OSD directory priming
=====================

You can generate the content for an OSD data directory that can start up a
BlueStore OSD with the *prime-osd-dir* command::

  ceph-bluestore-tool prime-osd-dir --dev *main device* --path /var/lib/ceph/osd/ceph-*id*

BlueFS log rescue
=====================

Some versions of BlueStore were susceptible to BlueFS log growing extremaly large -
beyond the point of making booting OSD impossible. This state is indicated by
booting that takes very long and fails in _replay function.

This can be fixed by::
  ceph-bluestore-tool fsck --path *osd path* --bluefs_replay_recovery=true

It is advised to first check if rescue process would be successfull::
  ceph-bluestore-tool fsck --path *osd path* \
  --bluefs_replay_recovery=true --bluefs_replay_recovery_disable_compact=true

If above fsck is successfull fix procedure can be applied.

Availability
============

**ceph-bluestore-tool** is part of Ceph, a massively scalable,
open-source, distributed storage system. Please refer to the Ceph
documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph-osd <ceph-osd>`\(8)
