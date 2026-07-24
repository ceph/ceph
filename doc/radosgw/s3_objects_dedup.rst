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

A single memory buffer is allocated per dedup cycle. It is used first for
ingress output streams (``B = allocation / 2 MiB`` concurrent buffers) and
then repurposed as the per-shard hash table (``Slots = allocation / 32``).

When the object count fits in ``B`` shards (single-pass), only one ingress
pass is needed. When more shards are required, a two-pass fan-out model is
used with ``B²`` shards. If the object count requires more memory than the
configured minimum, the allocation is doubled until ``B²`` shards suffice
or the 2048 MB cap is reached.

The minimum allocation is controlled by:

.. confval:: rgw_dedup_min_mem_allocation_mb

 +------+-----------+------+-----------+--------+------------+
 | MB   | Slots     | B    | Single-   | B²     | Two-pass   |
 |      |           |      | pass max  |        | max obj    |
 +======+===========+======+===========+========+============+
 | 8    | 256K      | 4    | 1M        | 16     | 4M         |
 +------+-----------+------+-----------+--------+------------+
 | 16   | 512K      | 8    | 4M        | 64     | 32M        |
 +------+-----------+------+-----------+--------+------------+
 | 32   | 1M        | 16   | 16M       | 256    | 256M       |
 +------+-----------+------+-----------+--------+------------+
 | 64   | 2M        | 32   | 64M       | 1K     | 2G         |
 +------+-----------+------+-----------+--------+------------+
 | 128  | 4M        | 64   | 256M      | 4K     | 16G        |
 +------+-----------+------+-----------+--------+------------+
 | 256  | 8M        | 128  | 1G        | 16K    | 128G       |
 +------+-----------+------+-----------+--------+------------+
 | 512  | 16M       | 256  | 4G        | 64K    | 1T         |
 +------+-----------+------+-----------+--------+------------+
 | 1024 | 32M       | 512  | 16G       | 256K   | 8T         |
 +------+-----------+------+-----------+--------+------------+
 | 2048 | 64M       | 1024 | 64G       | 1M     | 64T        |
 +------+-----------+------+-----------+--------+------------+

All units use strict power-of-2 math (K = 1024, M = 1 Mi, G = 1 Gi,
T = 1 Ti). Max shards (B²) is capped at 1,048,575 (1M − 1).
