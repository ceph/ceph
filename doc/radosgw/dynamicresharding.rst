.. _rgw_dynamic_bucket_index_resharding:

===================================
RGW Dynamic Bucket Index Resharding
===================================

.. versionadded:: Luminous

A large bucket index can lead to performance problems. In order
to address this problem we introduced bucket index sharding.
Until Luminous, changing the number of bucket shards (resharding)
needed to be done offline, from Luminous we support
online bucket resharding.

Each bucket index shard can handle its entries efficiently up until
reaching a certain threshold of entries. If this threshold is exceeded the system
could encounter performance issues.
The dynamic resharding feature detects this situation and increases
automatically the number of shards used by the bucket index,
resulting in the reduction of the number of entries in each bucket index shard.
This process is transparent to the user.

The detection process runs:
1. When new objects are added to the bucket
2. In a background process that periodically scans all the buckets
This is needed in order to deal with existing buckets in the system that are not being updated.
A bucket that requires resharding is added to the ``reshard_log`` queue and will be
scheduled to be resharded later.
The reshard threads run in the background and execute the scheduled resharding, one at a time.

Multisite
=========
Dynamic resharding is not supported in multisite environment.


Configuration
=============

Enable/Disable Dynamic bucket index resharding:

-``rgw_dynamic_resharding``:  true/false, default: true.

Parameters to control the resharding process in Ceph configuration fie:

-``rgw_reshard_num_logs``: number of shards for the resharding log, default: 16

-``rgw_reshard_bucket_lock_duration``: duration of lock on bucket obj during resharding, default:  120 seconds.

-``rgw_max_objs_per_shard``: maximum number of objects per bucket index shard, default: 100000 objects.

-``rgw_reshard_thread_interval``: maximum time between rounds of reshard thread processing,  default: 600 seconds


Admin commands
==============

Add a bucket to the resharding queue
------------------------------------

::

   # radosgw-admin reshard add --bucket <bucket_name> --num-shards <new number of shards>

List resharding queue
---------------------

::

   # radosgw-admin reshard list

Process/Schedule a bucket resharding
------------------------------------

::

   # radosgw-admin reshard process

Bucket resharding status
------------------------

::

   # radosgw-admin reshard status --bucket <bucket_name>

Cancel pending bucket resharding
--------------------------------

Ongoing bucket resharding operations cannot be cancelled. ::

   # radosgw-admin reshard cancel --bucket <bucket_name>

Manual bucket resharding
------------------------

::

   # radosgw-admin bucket reshard --bucket <bucket_name> --num-shards <new number of shards>
