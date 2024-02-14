:orphan:

==================================================================================
 rgw-restore-bucket-index -- try to restore a bucket's objects to its bucket index
==================================================================================

.. program:: rgw-restore-bucket-index

Synopsis
========

| **rgw-restore-bucket-index**

Description
===========

:program:`rgw-restore-bucket-index` is an *EXPERIMENTAL* RADOS gateway
user administration utility. It scans the data pool for objects that
belong to a given bucket and tries to add those objects back to the
bucket index. It's intended as a **last resort** after a
**catastrophic** loss of a bucket index. Please thorougly review the
*Warnings* listed below.

The utility works with regular (i.e., un-versioned) buckets, versioned
buckets, and buckets were versioning has been suspended.

Warnings
========

This utility is currently considered *EXPERIMENTAL*.

The results are unpredictable if the bucket is in
active use while this utility is running.

The results are unpredictable if only some bucket's objects are
missing from the bucket index. In such a case, consider using the
"object reindex" subcommand of `radosgw-admin` to restore object's to
the bucket index one-by-one.

For objects in versioned buckets, if the latest version is a delete
marker, it will be restored. If a delete marker has been written over
with a new version, then that delete marker will not be restored. This
should have minimal impact on results in that the it recovers the
latest version and previous versions are all accessible.

Command-Line Arguments
======================

.. option:: -b <bucket>

   Specify the bucket to be reindexed.

.. option:: -p <pool>

   Optional, specify the data pool containing head objects for the
   bucket. If omitted the utility will try to determine the data pool
   on its own.

.. option:: -r <realm-name>

   Optional, specify the realm if the restoration is not being applied
   to the default realm.

.. option:: -g <zonegroup-name>

   Optional, specify the zonegroup if the restoration is not being applied
   to the default zonegroup.

.. option:: -z <zone-name>

   Optional, specify the zone if the restoration is not being applied
   to the default zone.

.. option:: -l <rados-ls-output-file>

   Optional, specify a file containing the output of a rados listing
   of the data pool. Since listing the data pool can be an expensive
   and time-consuming operation, if trying to recover the indices for
   multiple buckets, it could be more efficient to re-use the same
   listing.

.. option:: -y

   Optional, proceed without further prompting. Without this option
   the utility will display some information and prompt the user as to
   whether to proceed. When provided, the utility will simply
   proceed. Please use caution when using this option.

Examples
========

Attempt to restore the index for a bucket named *summer-2023-photos*::

        $ rgw-restore-bucket-index -b summer-2023-photos

Availability
============

:program:`rgw-restore-bucket-index` is part of Ceph, a massively
scalable, open-source, distributed storage system.  Please refer to
the Ceph documentation at https://docs.ceph.com for more information.

See also
========

:doc:`radosgw-admin <radosgw-admin>`\(8)
