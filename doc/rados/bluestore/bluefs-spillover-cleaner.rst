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
        "db.wal/000052.log size=0x24cd21 migrated=0x1200000 from dev=2->1 ts=2026-06-02T12:44:25.488250+0000",
        "db.wal/000053.log size=0x3696ad migrated=0x1200000 from dev=2->1 ts=2026-06-02T12:44:25.833014+0000",
        "db.wal/000054.log size=0x1571f61 migrated=0x2400000 from dev=2->1 ts=2026-06-02T12:44:26.134245+0000",
        "db.wal/000055.log size=0x1b2b508 migrated=0x2400000 from dev=2->1 ts=2026-06-02T12:44:26.550633+0000"
    ],
    "pending_files": [
        "db.wal/000056.log",
        "db.wal/000057.log",
        "db.wal/000058.log",
        "db.wal/000059.log",
        "db.wal/000060.log",
        ...
    ]
   }

Testing
=======

For testing purposes, allocations can be forced onto the slow device.

.. confval:: bluefs_debug_force_slow

Example:

.. prompt:: bash #

   ceph config set osd bluefs_debug_force_slow true

This option is intended only for development and testing.