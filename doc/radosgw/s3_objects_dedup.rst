=====================
Full RGW Object Dedup
=====================
Adds a ``radosgw-admin`` command to collect and report deduplication stats.

.. note:: This utility doesn't perform dedup and doesn't make any
          change to the existing system and will only collect
          statistics and report them.

**************
Admin commands
**************
- ``radosgw-admin dedup stats``:
   Collects & displays last dedup statistics.
- ``radosgw-admin dedup pause``:
   Pauses an active dedup session (dedup resources are not released).
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session.
- ``radosgw-admin dedup abort``:
   Aborts an active dedup session and release all resources used by it.
- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session (aborting first existing session if exists).

***************
Skipped Objects
***************
Dedup Estimate process skips the following objects:

- Objects smaller than 4 MB (unless they are multipart).
- Objects with different placement rules.
- Objects with different pools.
- Objects with different storage classes.

The dedup process itself (which will be released in a later Ceph release) will also skip
**compressed** and **user-encrypted** objects, but the estimate
process will accept them (since we don't have access to that
information during the estimate process).

*******************
Estimate Processing
*******************
The Dedup Estimate process collects all the needed information directly from
the bucket indices reading one full bucket index object with thousands of
entries at a time.

The bucket indices objects are sharded between the participating
members so every bucket index object is read exactly one time.
The sharding allow processing to scale almost linearly splitting the
load evenly between the participating members.

The Dedup Estimate process does not access the objects themselves
(data/metadata) which means its processing time won't be affected by
the underlying media storing the objects (SSD/HDD) since the bucket indices are
virtually always stored on a fast medium (SSD with heavy memory
caching).

************
Memory Usage
************
 +---------------+----------+
 | RGW Obj Count |  Memory  |
 +===============+==========+
 | 1M            | 8 MB     |
 +---------------+----------+
 | 4M            | 16 MB    |
 +---------------+----------+
 | 16M           | 32 MB    |
 +---------------+----------+
 | 64M           | 64 MB    |
 +---------------+----------+
 | 256M          | 128 MB   |
 +---------------+----------+
 | 1024M (1G)    | 256 MB   |
 +---------------+----------+
 | 4096M (4G)    | 512 MB   |
 +---------------+----------+
 | 16384M (16G)  | 1024 MB  |
 +---------------+----------+
