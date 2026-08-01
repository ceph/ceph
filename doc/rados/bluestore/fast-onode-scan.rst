============================
 Multithread Onode Scanning
============================

.. index:: bluestore; rocksdb; allocator

Beginning with the Pacific release, BlueStore has the option to configure a 5-10% latency reduction
at a cost of significantly longer ``OSD`` recovery after a crash.

.. confval:: bluestore_allocation_from_file

During crash recovery BlueStore must reconstruct allocator state by scanning all stored RADOS objects.
For OSDs storing millions of RADOS objects this process may take several minutes to several hours to complete.
This page describes multithreaded onode recovery that significantly reduces recovery time.

Allocator
=========

In BlueStore, the configured ``Allocator`` is responsible for tracking unused allocation units on the underlying device.
As many as three allocators may be in use concurrently: one for the main block device, one for the DB, and one for the WAL.
Only the main device stores RADOS objects, and thus is the only allocator that is relevant here.
When a BlueStore OSD is running, allocator state is fully loaded in memory. This ensures that the
allocator can quickly respond when there is a need to allocate space for RADOS objects,
or when RADOS objects no longer exist and their underlying space should be reclaimed.

RocksDB-persisted allocator
---------------------------

The original design envisioned that allocator state updates are stored in RocksDB's ``b`` column.
When RADOS objects are written or release allocation units, the relevant state information
is updated with an object-modifying RocksDB atomic transaction. The information transfer is unidirectional:
BlueStore updates allocator state in RocksDB, but never retrieves it.
The exception is at OSD boot when BlueStore retrieves allocator state from the RocksDB ``b`` column.

File-persisted allocator
------------------------

Tests showed that keeping RocksDB busy by updating ``b`` column with allocation data
has observable cost in terms of write latency and CPU burden when compacting.
To avoid this performance impact the new default mode is to update allocator state only in-memory.
Only on shutdown is state saved to a BlueFS file named ``ALLOCATOR_NCB_DIR/ALLOCATOR_NCB_FILE``.

Crash recovery
==============

If BlueStore was not shut down gracefully there will be no updated file from which to read allocator state.
Since RADOS objects are then the only only source of truth regarding underlying device usage,
the recovery procedure must iterate over all RADOS objects in order to determine used vs
available device blocks.

Multi-threaded recovery
=======================

Iterating over all onodes in the RocksDB can take several minutes on
a relatively small SSD; for large and slow devices it may take several *hours*.
To mitigate this significant impact on OSD boot time, a multi-threaded recovery procedure
has been added for the Vampire release and may be backported to Umbrella and Tentacle.
This is a significantly different procedure than the original.
One config option selects and controls this new recovery strategy.

.. confval:: bluestore_allocation_recovery_threads


Performance
-----------

Below are example elapsed recovery timings for an
OSD on an NVMe SSD hosting 7.5M RBD objects, with 4.6TiB used:

+---------------------+------------+-----------------+
| Recovery Mechanism  | Threads    | Time in seconds |
+=====================+============+=================+
| original            | 1          | 76.0            |
+---------------------+------------+-----------------+
| multi-threaded      | 1          | 55.8            |
+---------------------+------------+-----------------+
| multi-threaded      | 4          | 18.1            |
+---------------------+------------+-----------------+
| multi-threaded      | 8          | 10.8            |
+---------------------+------------+-----------------+
| multi-threaded      | 12         | 8.0             |
+---------------------+------------+-----------------+
| multi-threaded      | 16         | 6.8             |
+---------------------+------------+-----------------+
| multit-hreaded      | 32         | 5.3             |
+---------------------+------------+-----------------+

Testing
-------

Concern for long recovery time has led some Ceph operators to configure
legacy synchronous persistance of allocator state in RocksDB.
There is now a procedure for predicting the legacy recovery time for a given OSD,
allowing an informed decision:

.. prompt:: bash #

  ceph-bluestore-tool --path <osd-data-path> recovery-compare

.. code-block::

  recovery-compare bluestore compare new and legacy onode recovery
  Legacy recovery start
  Legacy recovery result=0 took 76.033992 seconds
  Legacy recovery stats=
  ==========================================================
  onode_count             =    7500217
  shard_count             =    9754950
  shared_blob_count       =          0
  compressed_blob_count   =          0
  spanning_blob_count     =     264519
  skipped_illegal_extent  =  100815206
  extent_count            =  188685236
  insert_count            =          0
  store store_statfs(0x0/0x0/0x0, data 0x0/0x0, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 1 store_statfs(0x0/0x0/0x0, data 0x90220/0x94000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 2 store_statfs(0x0/0x0/0x0, data 0x4348a70e000/0x4c7ee684000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 18446744073709551615 store_statfs(0x0/0x0/0x0, data 0x26a3f/0x150000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  ==========================================================

  bluestore_allocation_recovery_threads = 0
  No multithread recovery to compare.
  recovery-compare success

.. prompt:: bash #

  ceph-bluestore-tool --path <osd-data-path> --bluestore_allocation_recovery_threads=12 recovery-compare

.. code-block::

  recovery-compare bluestore compare new and legacy onode recovery
  New recovery start
  New recovery result=0 took 8.033309 seconds
  New recovery stats=
  ==========================================================
  onode_count             =    7500217
  shard_count             =    9754950
  shared_blob_count       =          0
  compressed_blob_count   =          0
  spanning_blob_count     =     264519
  skipped_illegal_extent  =          0
  extent_count            =  188685236
  insert_count            =          0
  store store_statfs(0x0/0x0/0x0, data 0x0/0x0, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 1 store_statfs(0x0/0x0/0x0, data 0x90220/0x94000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 2 store_statfs(0x0/0x0/0x0, data 0x4348a70e000/0x4c7ee684000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 18446744073709551615 store_statfs(0x0/0x0/0x0, data 0x26a3f/0x150000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  ==========================================================

  Legacy recovery start
  Legacy recovery result=0 took 62.210723 seconds
  Legacy recovery stats=
  ==========================================================
  onode_count             =    7500217
  shard_count             =    9754950
  shared_blob_count       =          0
  compressed_blob_count   =          0
  spanning_blob_count     =     264519
  skipped_illegal_extent  =  100815206
  extent_count            =  188685236
  insert_count            =          0
  store store_statfs(0x0/0x0/0x0, data 0x0/0x0, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 1 store_statfs(0x0/0x0/0x0, data 0x90220/0x94000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 2 store_statfs(0x0/0x0/0x0, data 0x4348a70e000/0x4c7ee684000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  pool 18446744073709551615 store_statfs(0x0/0x0/0x0, data 0x26a3f/0x150000, compress 0x0/0x0/0x0, omap 0x0, meta 0x0)
  ==========================================================

  FSstats the same.
  Allocators the same.
  recovery-compare success

One can see in the above example the improvement in legacy recovery times: 76.0s vs 62.2s.
The second run shows the effect of RocksDB having the data already cached by the new recovery strategy.

