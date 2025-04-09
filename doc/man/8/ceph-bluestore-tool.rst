:orphan:

======================================================
 ceph-bluestore-tool -- bluestore administrative tool
======================================================

.. program:: ceph-bluestore-tool

Synopsis
========

| **ceph-bluestore-tool** *command*
  [ --dev *device* ... ]
  [ -i *osd_id* ]
  [ --path *osd path* ]
  [ --out-dir *dir* ]
  [ --log-file | -l *filename* ]
  [ --deep ]
| **ceph-bluestore-tool** fsck|repair --path *osd path* [ --deep ]
| **ceph-bluestore-tool** qfsck       --path *osd path*
| **ceph-bluestore-tool** allocmap    --path *osd path*
| **ceph-bluestore-tool** restore_cfb --path *osd path*
| **ceph-bluestore-tool** show-label --dev *device* ...
| **ceph-bluestore-tool** show-label-at --dev *device* --offset *lba*...
| **ceph-bluestore-tool** prime-osd-dir --dev *device* --path *osd path*
| **ceph-bluestore-tool** bluefs-export --path *osd path* --out-dir *dir*
| **ceph-bluestore-tool** bluefs-bdev-new-wal --path *osd path* --dev-target *new-device*
| **ceph-bluestore-tool** bluefs-bdev-new-db --path *osd path* --dev-target *new-device*
| **ceph-bluestore-tool** bluefs-bdev-migrate --path *osd path* --dev-target *new-device* --devs-source *device1* [--devs-source *device2*]
| **ceph-bluestore-tool** free-dump|free-score --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]
| **ceph-bluestore-tool** bluefs-stats --path *osd path*
| **ceph-bluestore-tool** bluefs-files --path *osd path*
| **ceph-bluestore-tool** reshard --path *osd path* --sharding *new sharding* [ --sharding-ctrl *control string* ]
| **ceph-bluestore-tool** show-sharding --path *osd path*
| **ceph-bluestore-tool** trim --path *osd path*
| **ceph-bluestore-tool** zap-device --dev *dev path*
| **ceph-bluestore-tool** revert-wal-to-plain --path *osd path*


Description
===========

**ceph-bluestore-tool** is a utility to perform low-level administrative
operations on a BlueStore instance.

Commands
========

:command:`help`

   show help

:command:`fsck` [ --deep ] *(on|off) or (yes|no) or (1|0) or (true|false)*

   run consistency check on BlueStore metadata.  If *--deep* is specified, also read all object data and verify checksums.

:command:`repair`

   Run a consistency check *and* repair any errors we can.

:command:`qfsck`

   run consistency check on BlueStore metadata comparing allocator data (from RocksDB CFB when exists and if not uses allocation-file) with ONodes state.

:command:`allocmap`

   performs the same check done by qfsck and then stores a new allocation-file (command is disabled by default and requires a special build)

:command:`restore_cfb`

   Reverses changes done by the new NCB code (either through ceph restart or when running allocmap command) and restores RocksDB B Column-Family (allocator-map).


:command:`bluefs-export`

   Export the contents of BlueFS (i.e., RocksDB files) to an output directory.

:command:`bluefs-bdev-sizes` --path *osd path*

   Print the device sizes, as understood by BlueFS, to stdout.

:command:`bluefs-bdev-expand` --path *osd path*

   Instruct BlueFS to check the size of its block devices and, if they have
   expanded, make use of the additional space. Please note that only the new
   files created by BlueFS will be allocated on the preferred block device if
   it has enough free space, and the existing files that have spilled over to
   the slow device will be gradually removed when RocksDB performs compaction.
   In other words, if there is any data spilled over to the slow device, it
   will be moved to the fast device over time.

:command:`bluefs-bdev-new-wal` --path *osd path* --dev-target *new-device*

   Adds WAL device to BlueFS, fails if WAL device already exists.

:command:`bluefs-bdev-new-db` --path *osd path* --dev-target *new-device*

   Adds DB device to BlueFS, fails if DB device already exists.
   
:command:`bluefs-bdev-migrate` --dev-target *new-device* --devs-source *device1* [--devs-source *device2*]

   Moves BlueFS data from source device(s) to the target device. Source devices
   (except the main one) are removed on success. Expands the target storage
   (updates the size label), making "bluefs-bdev-expand" unnecessary. The
   target device can be either a new device or a device that is already
   attached. If the device is a new device, it is added to the OSD replacing
   one of the source devices. The following replacement rules apply (in the
   order of precedence, stop on the first match):

      - if the source list has DB volume - the target device replaces it.
      - if the source list has WAL volume - the target device replaces it.
      - if the source list has slow volume only - the operation isn't permitted and requires explicit allocation via a new-DB/new-WAL command.

:command:`show-label` --dev *device* [...]

   Show device label(s).
   The label may be printed while an OSD is running.

:command:`show-label-at` --dev *device* --offset *lba*[...]

   Show device label at specific disk location. Dedicated DB/WAL volumes have a single label at offset 0.
   Main device could have valid labels at multiple locations: 0/1GiB/10GiB/100GiB/1000GiB.
   The labels at some locations might not exist though. 
   The label may be printed while an OSD is running.

:command:`free-dump` --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]

   Dump all free regions in allocator.

:command:`free-score` --path *osd path* [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]

   Give a [0-1] number that represents quality of fragmentation in allocator.
   0 represents case when all free space is in one chunk. 1 represents worst possible fragmentation.

:command:`bluefs-stats` --path *osd path*

   Shows summary of BlueFS occupied space with split on devices: block/db/wal and roles: wal/log/db.

:command:`bluefs-files` --path *osd path*

   Lists all BlueFS managed files, printing name, size and space used on devices.

:command:`reshard` --path *osd path* --sharding *new sharding* [ --resharding-ctrl *control string* ]

   Changes sharding of BlueStore's RocksDB. Sharding is build on top of RocksDB column families.
   This option allows to test performance of *new sharding* without need to redeploy OSD.
   Resharding is usually a long process, which involves walking through entire RocksDB key space
   and moving some of them to different column families.
   Option --resharding-ctrl provides performance control over resharding process.
   Interrupted resharding will prevent OSD from running.
   Interrupted resharding does not corrupt data. It is always possible to continue previous resharding,
   or select any other sharding scheme, including reverting to original one.

:command:`show-sharding` --path *osd path*

   Show sharding that is currently applied to BlueStore's RocksDB.

:command: `trim` --path *osd path*

   An SSD that has been used heavily may experience performance degradation.
   This operation uses TRIM / discard to free unused blocks from BlueStore and BlueFS block devices,
   and allows the drive to perform more efficient internal housekeeping.
   If BlueStore runs with discard enabled, this option may not be useful.

:command: `zap-device` --dev *dev path*

   Zeros all device label locations. This effectively makes device appear empty.

:command: `revert-wal-to-plain` --path *osd path*

   Changes WAL files from envelope mode to the legacy plain mode.
   Useful for downgrades, or if you might want to disable this new feature (bluefs_wal_envelope_mode).

Options
=======

.. option:: --dev *device*

   Add *device* to the list of devices to consider

.. option:: -i *osd_id*

   Operate as OSD *osd_id*. Connect to monitor for OSD specific options.
   If monitor is unavailable, add --no-mon-config to read from ceph.conf instead.

.. option:: --devs-source *device*

   Add *device* to the list of devices to consider as sources for migrate operation

.. option:: --dev-target *device*

   Specify target *device* migrate operation or device to add for adding new DB/WAL.

.. option:: --path *osd path*

   Specify an osd path.  In most cases, the device list is inferred from the symlinks present in *osd path*.  This is usually simpler than explicitly specifying the device(s) with --dev. Not necessary if -i *osd_id* is provided.

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

.. option:: --resharding-ctrl *control string*

   Provides control over resharding process. Specifies how often refresh RocksDB iterator,
   and how large should commit batch be before committing to RocksDB. Option format is:
   <iterator_refresh_bytes>/<iterator_refresh_keys>/<batch_commit_bytes>/<batch_commit_keys>
   Default: 10000000/10000/1000000/1000

Additional ceph.conf options
============================

Any configuration option that is accepted by OSD can be also passed to **ceph-bluestore-tool**.
Useful to provide necessary configuration options when access to monitor/ceph.conf is impossible and -i option cannot be used.

Device labels
=============

Every BlueStore block device has a block label at the beginning of the device.
Main device might optionaly have additional labels at different locations
for the sake of OSD robustness.
You can dump the contents of the label with::

  ceph-bluestore-tool show-label --dev *device*

The main device will have a lot of metadata, including information
that used to be stored in small files in the OSD data directory.  The
auxiliary devices (db and wal) will only have the minimum required
fields (OSD UUID, size, device type, birth time).
The main device contains additional label copies at offsets: 1GiB, 10GiB, 100GiB and 1000GiB.
Corrupted labels are fixed as part of repair::

  ceph-bluestore-tool repair --dev *device*

OSD directory priming
=====================

You can generate the content for an OSD data directory that can start up a
BlueStore OSD with the *prime-osd-dir* command::

  ceph-bluestore-tool prime-osd-dir --dev *main device* --path /var/lib/ceph/osd/ceph-*id*

BlueFS log rescue
=====================

Some versions of BlueStore were susceptible to BlueFS log growing extremely large -
beyond the point of making booting OSD impossible. This state is indicated by
booting that takes very long and fails in _replay function.

This can be fixed by::
  ceph-bluestore-tool fsck --path *osd path* --bluefs_replay_recovery=true

It is advised to first check if rescue process would be successful::
  ceph-bluestore-tool fsck --path *osd path* \
  --bluefs_replay_recovery=true --bluefs_replay_recovery_disable_compact=true

If above fsck is successful fix procedure can be applied.

Availability
============

**ceph-bluestore-tool** is part of Ceph, a massively scalable,
open-source, distributed storage system. Please refer to the Ceph
documentation at https://docs.ceph.com for more information.


See also
========

:doc:`ceph-osd <ceph-osd>`\(8)
