:orphan:

=============================================================================
 rgw-gap-list -- List bucket index entries with damaged RADOS backing objects
=============================================================================

.. program:: rgw-gap-list

Synopsis
========

| **rgw-gap-list**

Description
===========

:program:`rgw-gap-list` is an *EXPERIMENTAL* RADOS gateway user
administration utility. It produces a listing of bucket index entries
that have missing backing RADOS objects. It places the results and
intermediate files on the local filesystem rather than on the Ceph
cluster itself, and therefore will not itself consume additional
cluster storage.

In theory these gaps should not exist. However because Ceph evolves
rapidly, bugs do crop up, and they may result in bucket index entries
that have missing RADOS objects, such as when a delete operation does
not fully complete.

Behind the scenes it runs `rados ls` and `radosgw-admin bucket
radoslist ...` and produces a list of those entries that appear in the
latter but not the former. Those entries are presumed to be the
gaps.

Note: Depending on the size of the pool(s) involved, this tool may be
quite slow to produce its results.

Warnings
========

This utility is considered *EXPERIMENTAL*.

Options
=======

.. option:: -p pool

   The RGW bucket data pool name. If option omitted the pool name will
   be prompted during execution. Multiple pools can be supplied as a
   space-separated double quoted list.

.. option:: -t temp_directory

   The tool can produce large intermediate files. By default ``/tmp``
   is used, but if the filesystem housing ``/tmp`` doesn't have
   sufficient free space, a different directory (on a filesystem with
   sufficient free space) can be specified.

.. option:: -m

   Use two (multiple) threads to speed up the run.


Examples
========

Launch the tool::

        $ rgw-gap-list -p default.rgw.buckets.data -t /home/super_admin/temp_files

Availability
============

:program:`rgw-gap-list` is part of Ceph, a massively scalable, open-source,
distributed storage system.  Please refer to the Ceph documentation at
https://docs.ceph.com for more information.

See also
========

:doc:`radosgw-admin <radosgw-admin>`\(8)
:doc:`rgw-orphan-list <rgw-orphan-list>`\(8)
