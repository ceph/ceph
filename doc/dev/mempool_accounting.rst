What is a mempool?
------------------
A memory pool (mempool) is a method for tracking memory consumption. Memory pools represent the memory consumption
of C++ classes and containers, and they are used to assess memory leaks and other insights around memory usage with
low overhead. Each memory pool tracks the number of bytes and items it contains. Mempools are statically declared,
and they serve the purpose of identifying memory-related checks in BlueStore.

Some common mempools that we can track
--------------------------------------

- ``bloom_filter``: tracks objects already in the cache pool in order to determine which objects in the pool are
  being accessed
- ``bluestore_alloc``: accounts for actual allocations done by an allocator
- ``bluestore_inline_bl``: measures encoded length of an inline buffer
- ``bluestore_fsck``: file system consistency check for BlueStore metadata; helps in tracking and debugging during OSD repairs
- ``bluestore_txc``: accounts for committed transactions counter
- ``bluestore_writing_deferred``: measures small writes which are deferred, i.e. first written into RocksDB WAL and later
  flushed to the disk
- ``bluestore_writing``: accounts for in-flight write buffers
- ``bluefs``:  file-system-like interface; provides just enough functionality to allow RocksDB to store its “files” and share
  the same raw device(s) with BlueStore
- ``buffer_anon``: stores arbitrary buffer data
- ``buffer_meta``: all the metadata associated with buffer anon buffers
- ``bluestore_cache_data``: mempool for writing and writing deferred
- ``bluestore_cache_onode``: object node (onode) metadata in the BlueStore cache
- ``bluestore_cache_meta``: key under PREFIX_OBJ where we are stored
- ``bluestore_cache_other``: right now accounts for:

  - ``map_t``: used to track raw extents on disk for SharedBlob and for the in-memory Blob with the blob namespace
  - ``coll_map``: collections_map
  - ``csum_data``: checksum data
- ``bluestore_cache_buffer``: accounts for buffer cache shards
- ``bluestore_extent``: a logical (as well as physical) extent, pointing to some portion of a blob
- ``bluestore_blob``: in-memory blob metadata associated cached buffers
- ``bluestore_shared_blob``: in-memory shared blob state; stores a reference to the set of collections it belongs to
  (includes cached buffers)
- ``bluefs_file_reader``: accounts for bluefs file reader buffer
- ``bluefs_file_writer``: accounts for bluefs file writer buffer

Check mempools usage
~~~~~~~~~~~~~~~~~~~~

Command to see BlueStore memory allocation in these mempools::

     $ ceph daemon osd.NNN dump_mempools


.. note:: see more:
    https://github.com/ceph/ceph/blob/main/src/include/mempool.h
