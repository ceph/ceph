==================================
Orphan List and Associated Tooling
==================================

.. version added:: Luminous

.. contents::

Orphans are RADOS objects that are left behind after their associated
RGW objects are removed. Normally these RADOS objects are removed
automatically, either immediately or through a process known as
"garbage collection". Over the history of RGW, however, there may have
been bugs that prevented these RADOS objects from being deleted, and
these RADOS objects may be consuming space on the Ceph cluster without
being of any use. From the perspective of RGW, we call such RADOS
objects "orphans".

Orphans Find -- DEPRECATED
--------------------------

The `radosgw-admin` tool has/had three subcommands to help manage
orphans, however these subcommands are (or will soon be)
deprecated. These subcommands are:

::
   # radosgw-admin orphans find ...
   # radosgw-admin orphans finish ...
   # radosgw-admin orphans list-jobs ...

There are two key problems with these subcommands, however. First,
these subcommands have not been actively maintained and therefore have
not tracked RGW as it has evolved in terms of features and updates. As
a result the confidence that these subcommands can accurately identify
true orphans is presently low.

Second, these subcommands store intermediate results on the cluster
itself. This can be problematic when cluster administrators are
confronting insufficient storage space and want to remove orphans as a
means of addressing the issue. The intermediate results could strain
the existing cluster storage capacity even further.

For these reasons "orphans find" has been deprecated.

Orphan List
-----------

Because "orphans find" has been deprecated, RGW now includes an
additional tool -- 'rgw-orphan-list'. When run it will list the
available pools and prompt the user to enter the name of the data
pool. At that point the tool will, perhaps after an extended period of
time, produce a local file containing the RADOS objects from the
designated pool that appear to be orphans. The administrator is free
to examine this file and the decide on a course of action, perhaps
removing those RADOS objects from the designated pool.

All intermediate results are stored on the local file system rather
than the Ceph cluster. So running the 'rgw-orphan-list' tool should
have no appreciable impact on the amount of cluster storage consumed.

WARNING: Experimental Status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 'rgw-orphan-list' tool is new and therefore currently considered
experimental. The list of orphans produced should be "sanity checked"
before being used for a large delete operation.

WARNING: Specifying a Data Pool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a pool other than an RGW data pool is specified, the results of the
tool will be erroneous. All RADOS objects found on such a pool will
falsely be designated as orphans.

WARNING: Unindexed Buckets
~~~~~~~~~~~~~~~~~~~~~~~~~~

RGW allows for unindexed buckets, that is buckets that do not maintain
an index of their contents. This is not a typical configuration, but
it is supported. Because the 'rgw-orphan-list' tool uses the bucket
indices to determine what RADOS objects should exist, objects in the
unindexed buckets will falsely be listed as orphans.


RADOS List
----------

One of the sub-steps in computing a list of orphans is to map each RGW
object into its corresponding set of RADOS objects. This is done using
a subcommand of 'radosgw-admin'.

::
   # radosgw-admin bucket radoslist [--bucket={bucket-name}]

The subcommand will produce a list of RADOS objects that support all
of the RGW objects. If a bucket is specified then the subcommand will
only produce a list of RADOS objects that correspond back the RGW
objects in the specified bucket.

Note: Shared Bucket Markers
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some administrators will be aware of the coding schemes used to name
the RADOS objects that correspond to RGW objects, which include a
"marker" unique to a given bucket.

RADOS objects that correspond with the contents of one RGW bucket,
however, may contain a marker that specifies a different bucket. This
behavior is a consequence of the "shallow copy" optimization used by
RGW. When larger objects are copied from bucket to bucket, only the
"head" objects are actually copied, and the tail objects are
shared. Those shared objects will contain the marker of the original
bucket.

.. _Data Layout in RADOS : ../layout
.. _Pool Placement and Storage Classes : ../placement
