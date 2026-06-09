============================
 Multithread Onode Scanning
============================

.. index:: bluestore; rocksdb; allocator

Since Pacific release BlueStore has the option to select 5-10% latency reduction
at a cost of significantly longer ``OSD`` recovery after crash.

.. confval:: bluestore_allocation_from_file

During crash recovery BlueStore must reconstruct allocator state by scanning all stored objects.
On systems with millions of objects this process can take several minutes.
This page presents new multithreaded onode recovery that significantly reduces recovery time.

Allocator
=========

In BlueStore ``Allocator`` is responsible for tracking unused allocation units on the disk.
Up to 3 allocators can be in use concurrently, one for Main, one for DB, and one for WAL.
Only Main device can contain RADOS object, and only Main's allocator is relevant here.
When BlueStore is running, allocator state is fully loaded to memory. That way
allocator can quickly respond when there is a need to allocate disk space for object,
or when object no longer exists on disk.

RocksDB persisted allocator
---------------------------

Original design envisioned that allocator state updates are stored in RocksDB's ``b`` column.
When RADOS object change allocates or releases allocation unit the relevant state update information
is kept together with object-modifying RocksDB atomic transaction. The information transfer is unidirectional,
BlueStore is updating allocator state in RocksDB, but never retrieving it.
The exception is OSD bootup when BlueStore retrieves allocator state from RocksDB ``b`` column.

File persisted allocator
------------------------

It was tested that keeping RocksDB busy updating ``b`` column with allocation data
has observable cost on write latency and cpu burden on compaction.
To get that extra performance back the new default mode is to update in-memory allocator state only.
Instead, on shutdown state is saved to BlueFS file named ``ALLOCATOR_NCB_DIR/ALLOCATOR_NCB_FILE``.

Crash recovery
--------------

If BlueStore was not shutdown orderly there is no file to read allocator state from.
Since it is the objects that define what is in use, the recovery procedure iterates over all objects and extracts used allocations.

Multi-thread recovery
=====================

Iterating over all onodes in the RocksDB can take several minutes.
To help with that a multi-threaded recovery procedure is created.
It is significantly different procedure than the original one.
There is just one configurable that selects and controls recovery.

.. confval:: bluestore_allocation_recovery_threads


Performance
-----------

Example recovery timings.
OSD with NVME SSD, hosting 7.5M RBD objects, 4.6TiB used.

+------------------------+------------+------------+
| recovery mechanism     | threads    | time(s)    |
+========================+============+============+
| original               | 1          | 76.0       |
+------------------------+------------+------------+
| multithread            | 1          | 55.8       |
+------------------------+------------+------------+
| multithread            | 4          | 18.1       |
+------------------------+------------+------------+
| multithread            | 8          | 10.8       |
+------------------------+------------+------------+
| multithread            | 12         | 8.0        |
+------------------------+------------+------------+
| multithread            | 16         | 6.8        |
+------------------------+------------+------------+
| multithread            | 32         | 5.3        |
+------------------------+------------+------------+

Testing
-------

If long recovery time has been a deciding factor for staying with allocations in RocksDB,
there is a procedure for measuring directly recovery time.

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

One can see above discrepancy between legacy recovery times: 76.0s vs 62.2s.
This is an effect of RocksDB having the data already cached by new recovery when legacy recovery is the second one.

