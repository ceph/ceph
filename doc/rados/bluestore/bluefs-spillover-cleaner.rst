=========================
BlueFS Spillover Cleaner
=========================

Overview
========

BlueFS may place files on the slow device when the DB device
runs out of free space. This condition is known as spillover.

The BlueFS Spillover Cleaner is a background component that periodically
scans for spillover files and attempts to migrate them back to
the DB device when sufficient space becomes available.

Spillover is most commonly observed when the DB device becomes full,
for example large RocksDB compactions after OSD crashes, upgrades that change
RocksDB behavior.

The cleaner is disabled by default.

Operation
=========

The cleaner operates in two phases:

Active Phase:

- Scan for files located on the slow device.
- Migrate those files back to the DB device.
- While migration is in progress, the cleaner throttles itself according to :confval:`bluefs_spillover_cleaner_work_ratio` to reduce interference with foreground IO.

Idle Phase:

- Entered when no spillover files are found
- When no spillover files remain, the cleaner enters a longer sleep period controlled by :confval:`bluefs_spillover_idle_time` before performing the next scan.

Configuration
=============

Enable cleaner
--------------

.. confval:: bluefs_spillover_cleaner

Idle Time
-------------

.. confval:: bluefs_spillover_idle_time

Work ratio
----------

.. confval:: bluefs_spillover_cleaner_work_ratio

Higher values make migration more aggressive, while lower values reduce
resource consumption.

Migration behavior
==================

Files are migrated incrementally rather than moving an entire file at
once. This helps limit memory consumption and reduce latency spikes.

Migration is attempted only when sufficient free space exists on the
DB device.

Admin Commands
==============

Display spillover cleaner status:

.. prompt:: bash #

   ceph tell osd.N bluefs spillover cleaner stats

Example output:

.. code-block::

   {
    "Files Migrated": [
        ...
        "db/000084.sst size=0xf8cd6 migrated=0x100000 from dev=2->1 ts=2026-07-22T18:49:42.555723+0000",
        "db/000086.sst size=0xfdd34 migrated=0x100000 from dev=2->1 ts=2026-07-22T18:49:42.595349+0000",
        "db.wal/000078.log size=0x19de2e2 migrated=0x2400000 from dev=2->1 ts=2026-07-22T18:49:42.694700+0000",
        "db.wal/000079.log size=0x19d32f7 migrated=0x2400000 from dev=2->1 ts=2026-07-22T18:49:43.424059+0000",
        "db.wal/000080.log size=0xd1a637 migrated=0x1200000 from dev=2->1 ts=2026-07-22T18:49:44.204353+0000",
        "db.wal/000081.log size=0xcbeaf5 migrated=0x1200000 from dev=2->1 ts=2026-07-22T18:49:44.618457+0000",
        "db.wal/000082.log size=0xd6737 migrated=0x1200000 from dev=2->1 ts=2026-07-22T18:49:44.892949+0000",
        "db.wal/000083.log size=0x696fe8 migrated=0x1200000 from dev=2->1 ts=2026-07-22T18:49:45.279432+0000"
    ],
    "pending_files": [
        "db.wal/000091.log",
        "db.wal/000092.log",
        "db.wal/000093.log",
        "db.wal/000094.log",
        "db.wal/000095.log",
        ...
    ],
    "active_files": [
        "db.wal/000085.log num_writers=1",
        "db.wal/000087.log num_writers=1",
        "db.wal/000088.log num_writers=1",
        "db.wal/000089.log num_writers=1",
        "db.wal/000090.log num_writers=1"
    ],
    "last_scan_time": "2026-07-22T18:51:15.895644+0000"
   }

Interpreting Cleaner Stats
===========================

The spillover cleaner operates in repeated scan cycles consisting of an Active Phase and an Idle Phase.
The output of ``ceph tell osd.N bluefs spillover cleaner stats`` shows the
current cycle state from the recent Active Phase and the migration history of
the running spillover cleaner instance.

``Files Migrated``
  Contains files that have been successfully migrated from the slow device
  to the DB device. This is the successful migration history accumulated by
  the running spillover cleaner instance.

``pending_files``
  At the beginning of each cycle, the spillover cleaner scans for files currently
  located on the slow device and adds them to ``pending_files``. These files
  are processed one by one during the cycle:

  - Successfully migrated files are removed from ``pending_files`` and added
    to ``Files Migrated``.
  - Files with active writers are removed from ``pending_files`` and reported
    in ``active_files`` for that cycle.

  During the next cleaner cycle, a new scan is performed and a new set of
  ``pending_files`` is created.

``active_files``
  Contains files that have active writers when the cleaner attempted to
  migrate them. Migration of these files are skipped during the current cycle.

``last_scan_time``
  Shows the time of the most recent cleaner scan.

Testing
=======

For testing purposes, allocations can be forced onto the slow device.

.. confval:: bluefs_debug_force_slow

Example:

.. prompt:: bash #

   ceph config set osd bluefs_debug_force_slow true

This option is intended only for development and testing.