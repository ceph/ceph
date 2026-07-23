.. _radosgw-s3-dedup:

=====================
Full RGW Object Dedup
=====================

Full RGW object deduplication adds ``radosgw-admin`` commands to deduplicate
RGW tail RADOS objects and to collect and report statistics.

These operations are also available through the `Admin Ops API <../radosgw/adminops/#dedup>`_
under ``/{admin}/dedup``.


Admin Commands
==============

- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session, first ending any existing session.
   No changes are made to the existing system; statistics will be
   collected and reported.
- ``radosgw-admin dedup exec --yes-i-really-mean-it``:
   Starts a new dedup session, first cancelling any existing session.
   Performs a full pass, finding and deduplicating identical RADOS tail objects.

   This is an experimental feature under active development.
   As of this documentation's release, This command can lead to **data loss**
   and should not be used on production data as of the release containing
   this documentation. Note the URL of this page: if you are running a
   newer release, consult that release's updated documentation. Running
   this command on a Ceph release in which it is not yet production-ready
   may irreversibly lose precious data.
- ``radosgw-admin dedup pause``:
   Pauses an active dedup session (dedup resources are not released).
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session.
- ``radosgw-admin dedup abort``:
   Aborts an active dedup session, releasing all resources used by it.
- ``radosgw-admin dedup stats``:
   Collects and displays dedup statistics.
- ``radosgw-admin dedup throttle --max-bucket-index-ops=<count>``:
   Specifies maximum allowed bucket index read requests per second per
   RGW daemon during dedup, ``0`` means unlimited.
- ``radosgw-admin dedup throttle --stat``:
   Displays dedup throttle setting.

The ``dedup estimate`` and ``dedup exec`` commands also accept filter options:

- ``--allow-bucket-list <file>``:
   Path to a file listing bucket names to include (allowlist mode).
   Only buckets listed in the file will be processed.
   Mutually exclusive with ``--deny-bucket-list``.

- ``--deny-bucket-list <file>``:
   Path to a file listing bucket names to exclude (denylist mode).
   All buckets except those listed in the file will be processed.
   Mutually exclusive with ``--allow-bucket-list``.

- ``--allow-storage-class-list <file>``:
   Path to a file listing storage class names to include (allowlist mode).
   Mutually exclusive with ``--deny-storage-class-list``.

- ``--deny-storage-class-list <file>``:
   Path to a file listing storage class names to exclude (denylist mode).
   Mutually exclusive with ``--allow-storage-class-list``.

**File format:** One name per line. Lines starting with or containing ``#``
are treated as comments. Whitespace is ignored. The file must contain at least
one valid name; an empty or all-comment file is rejected.


Configuration
=============

The dedup background thread must be enabled on at least one RGW daemon in each
zone for dedup operations to function. Having the thread enabled on multiple
RGW processes within the same zone spreads the dedup work between them.

.. confval:: rgw_enable_dedup_threads

This setting is evaluated at RGW startup. Changing it requires a daemon
restart.

When running RGW as an NFS-Ganesha gateway (librgw), the dedup thread is
disabled by default. To enable it in NFS mode, also set:

.. confval:: rgw_nfs_run_dedup_threads


Skipped Objects
===============

The dedup estimate process skips the following RGW objects:

- Objects smaller than :confval:`rgw_dedup_min_obj_size_for_dedup` (unless they
  are multipart).
- Objects with different placement rules.
- Objects in different RADOS pools.
- Objects with different RGW storage classes.

The full dedup process skips all of the above and additionally skips
**compressed** and **user-encrypted** objects.

The minimum RGW object size to be deduplicated is controlled by the following
configuration option:

.. confval:: rgw_dedup_min_obj_size_for_dedup


Estimate Processing
===================

The dedup estimate process collects all needed information directly from
the bucket indexes, reading one full bucket index object a thousand entries at
a time.

Bucket index objects are sharded between the participating members so each
is read exactly one time. The sharding allows processing to
scale almost linearly, splitting the load evenly among participating
daemons.

The dedup estimate process does not access the object payload
data, which means that processing time won't be significantly affected by the
underlying media (SSD/HDD) storing the objects. Best practice places bucket index pools
on fast storage: SSDs
:ref:`are recommended <hardware-recommendations>` and they are cached heavily
in memory.

Administrators can throttle the estimate process by setting a limit on the
number of bucket index reads per second per RGW daemon. Each operation
reads 1000 object entries:

.. prompt:: bash #

   radosgw-admin dedup throttle --max-bucket-index-ops=<count>

A typical RGW server performs about 100 bucket index reads per second and thus
100,000 object entries. For example, setting ``count`` to 50 would then
typically slow down the estimate process by half.


Full Dedup Processing
=====================

The full dedup process begins by constructing a dedup table from the bucket
indexes in a fashion similar to the estimate process described above.

This table is then scanned linearly to exclude RADOS objects without duplicates,
leaving only dedup candidates.

Next, it iterates through these dedup candidate objects, reading their complete
information from the object metadata, a per-object RADOS operation. During
this step, **compressed** and **user-encrypted** objects are removed from
consideration.

Following this, we calculate a cryptographically strong hash of candidate
object data. This involves a full-object read, which is a resource-intensive
operation. The hash ensures that dedup candidates are indeed perfect
matches. If they are, we proceed with deduplication:

- Increment the reference count on the source tail objects one by one.
- Copy the manifest from the source to the target.
- Remove all tail objects on the target.

Split Head Mode
===============

The dedup code can split a head object into two objects:

- one with attributes and no data, and
- a new tail object with only data.

The new tail object will be deduplicated, unlike head objects, which cannot
be deduplicated.

:confval:`rgw_dedup_split_obj_head` (default: true). Setting
this option to ``false`` disables split-head entirely.

.. confval:: rgw_dedup_split_obj_head


Memory Usage
============

 +------------------+----------+
 | RGW Object Count |  Memory  |
 +==================+==========+
 |      1M          |    8 MB  |
 +------------------+----------+
 |      4M          |   16 MB  |
 +------------------+----------+
 |     16M          |   32 MB  |
 +------------------+----------+
 |     64M          |   64 MB  |
 +------------------+----------+
 |    256M          |  128 MB  |
 +------------------+----------+
 |   1024M   (1G)   |  256 MB  |
 +------------------+----------+
 |   4096M   (4G)   |  512 MB  |
 +------------------+----------+
 |  16384M  (16G)   | 1024 MB  |
 +------------------+----------+
 |  65536M  (64G)   | 2048 MB  |
 +------------------+----------+
 | 262144M (256G)   | 4096 MB  |
 +------------------+----------+

 .. note::
     Pools with more than ~213 billion user objects (256B with headroom) exceed the
     dedup system's maximum capacity and will be rejected at startup.
