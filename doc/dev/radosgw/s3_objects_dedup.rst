======================
Full RGW Object Dedup:
======================
Add a radosgw-admin command to collect and report deduplication stats

.. note:: This utility doesn’t perform dedup and doesn’t make any
          change to the existing system and will only collect
          statistics and report them.

----

***************
Admin commands:
***************
- ``radosgw-admin dedup stats``:
   Collects & displays last dedup statistics
- ``radosgw-admin dedup pause``:
   Pauses active dedup session (dedup resources are not released)
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session
- ``radosgw-admin dedup abort``:
   Aborts active dedup session and release all resources used by it
- ``radosgw-admin dedup estimate``
    Starts a new dedup estimate session (aborting first existing session if exists)

----

****************
Skipped Objects:
****************
Dedup Estimates skips the following objects:

- Objects smaller than 4MB (unless they are multipart)
- Objects with non-standard storage-class

The Dedup process itself (which will be released later) will also skip
**compressed** and **user-encrypted** objects, but the estimate
process will accept them (since we don't have access to that
information during the estimate process)

----

********************
Estimate Processing:
********************
The Dedup Estimate process collects all the needed information directly from
the bucket-indices reading one full bucket-index object with 1000's of
entries at a time.

The Bucket-Indices objects are sharded between the participating
members so every bucket-index object is read exactly one time.
The sharding allow processing to scale almost linearly spliting the
load evenly between the participating members.

The Dedup Estimate process does not access the objects themselves
(data/metadata) which means its processing time won't be affected by
the underlined media storing the objects (SSD/HDD) since the bucket-indices are
virtually always stored on a fast medium (SSD with heavy memory
caching)

----

*************
Memory Usage:
*************
The design strives for memory efficiency:

- shard the workload using orthogonal datas shard limiting the amount of memory used in parallel
- use a compact open-hashing system with 32B per entry
- efficient memory allocation:

 - Memory is allocated in a single chunk based on the number of
   existing RGW objects (the number is taken from RGW bucket stats)

- Each RGW allocates a single memory chunk, which is reused for
  subsequent work shards, which are processed serially. The RGW daemon
  only needs enough memory to handle a single shard: shards are of
  roughly equal size.

 - Memory is recycled between steps - first used to buffer collected
   data and then used for the hash table
 - When dedup completes the memory is freed

 +---------------+-----------------+------------+-------------------------------------+
 | RGW Obj Count | MD5 Shard Count |  Memory    |           Calculation               |
 +===============+=================+============+=====================================+
 | | ____1M      | | __4           | | ___8MB   | | ___8MB/32 = _0.25M * __4 = ____1M |
 | | ____4M      | | __8           | | __16MB   | | __16MB/32 = _0.50M * __8 = ____4M |
 | | ___16M      | | _16           | | __32MB   | | __32MB/32 = _1.00M * _16 = ___16M |
 | | ___64M      | | _32           | | __64MB   | | __64MB/32 = _2.00M * _32 = ___64M |
 | | __256M      | | _64           | | _128MB   | | _128MB/32 = _4.00M * _64 = __256M |
 | | _1024M( 1G) | | 128           | | _256MB   | | _256MB/32 = _8.00M * 128 = _1024M |
 | | _4096M( 4G) | | 256           | | _512MB   | | _512MB/32 = 16M.00 * 256 = _4096M |
 | | 16384M(16G) | | 512           | | 1024MB   | | 1024MB/32 = 32M.00 * 512 = 16384M |
 +---------------+-----------------+------------+-------------------------------------+
