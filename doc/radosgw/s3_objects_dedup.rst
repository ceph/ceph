=====================
Full RGW Object Dedup
=====================
Full RGW object deduplication adds ``radosgw-admin`` commands to remove
duplicated RGW tail objects and to collect and report dedup statistics.


Admin Commands
==============
- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session (aborting first any existing session).
   No changes are made to the existing system. Only statistics will be
   collected and reported.
- ``radosgw-admin dedup exec --yes-i-really-mean-it``:
   Starts a new dedup session (aborting first any existing session).
   Performs a full dedup, finding duplicated tail objects and removing them.

   This command can lead to **data loss** and should not be used on production
   data!!
- ``radosgw-admin dedup pause``:
   Pauses an active dedup session (dedup resources are not released).
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session.
- ``radosgw-admin dedup abort``:
   Aborts an active dedup session, releasing all resources used by it.
- ``radosgw-admin dedup stats``:
   Collects and displays last dedup statistics.
- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session (aborting first any existing session).
- ``radosgw-admin dedup throttle --max-bucket-index-ops=<count>``:
   Specify maximum bucket index read requests per second allowed for a single
   RGW server during dedup, ``0`` means unlimited.
- ``radosgw-admin dedup throttle --stat``:
   Displays dedup throttle setting.


Skipped Objects
===============
Dedup estimate process skips the following objects:

- Objects smaller than 4 MB (unless they are multipart).
- Objects with different placement rules.
- Objects with different pools.
- Objects with different storage classes.

The full dedup process skips all of the above and additionally skips
**compressed** and **user-encrypted** objects.


Estimate Processing
===================
The dedup estimate process collects all the needed information directly from
the bucket indices reading one full bucket index object with thousands of
entries at a time.

The bucket index objects are sharded between the participating members so each
bucket index object is read exactly one time. The sharding allow processing to
scale almost linearly, splitting the load evenly between the participating
members.

The dedup estimate process does not access the objects themselves
(data/metadata) which means its processing time won't be affected by the
underlying media (SSD/HDD) storing the objects. The bucket indices are
virtually always accessed from a fast medium: placement on SSD
:ref:`is recommended <hardware-recommendations>` and they are cached heavily
in memory.

The administrator can throttle the estimate process by setting a limit on the
number of bucket index reads per second per an RGW server (each read brings
1000 object entries) using:

.. prompt:: bash #

   radosgw-admin dedup throttle --max-bucket-index-ops=<count>

A typical RGW server performs about 100 bucket index reads per second (i.e.
100,000 object entries). For example setting ``count`` to 50 would then
typically slow down the estimate process by half.


Full Dedup Processing
=====================
The full dedup process begins by constructing a dedup table from the bucket
indices, similar to the estimate process above.

This table is then scanned linearly to purge objects without duplicates,
leaving only dedup candidates.

Next, we iterate through these dedup candidate objects, reading their complete
information from the object metadata (a per-object RADOS operation). During
this step, we filter out **compressed** and **user-encrypted** objects.

Following this, we calculate a cryptograhically strong hash of the candidate
object data. This involves a full-object read which is a resource-intensive
operation. The hash ensures that the dedup candidates are indeed perfect
matches. If they are, we proceed with the deduplication:

- Increment the reference count on the source tail objects one by one.
- Copy the manifest from the source to the target.
- Remove all tail objects on the target.


Memory Usage
============
 +------------------+----------+
 | RGW Object Count |  Memory  |
 +==================+==========+
 | 1M               | 8 MB     |
 +------------------+----------+
 | 4M               | 16 MB    |
 +------------------+----------+
 | 16M              | 32 MB    |
 +------------------+----------+
 | 64M              | 64 MB    |
 +------------------+----------+
 | 256M             | 128 MB   |
 +------------------+----------+
 | 1024M (1G)       | 256 MB   |
 +------------------+----------+
 | 4096M (4G)       | 512 MB   |
 +------------------+----------+
 | 16384M (16G)     | 1024 MB  |
 +------------------+----------+
