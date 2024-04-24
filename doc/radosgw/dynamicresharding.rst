.. _rgw_dynamic_bucket_index_resharding:

===================================
RGW Dynamic Bucket Index Resharding
===================================

.. versionadded:: Luminous

A large bucket index can lead to performance problems, which can
be addressed by sharding bucket indexes.
Until Luminous, changing the number of bucket shards (resharding)
needed to be done offline, with RGW services disabled.
Since the Luminous release Ceph has supported online bucket resharding.

Each bucket index shard can handle its entries efficiently up until
reaching a certain threshold. If this threshold is
exceeded the system can suffer from performance issues. The dynamic
resharding feature detects this situation and automatically increases
the number of shards used by a bucket's index, resulting in a
reduction of the number of entries in each shard. This
process is transparent to the user. Writes to the target bucket
are blocked (but reads are not) briefly during resharding process.

By default dynamic bucket index resharding can only increase the
number of bucket index shards to 1999, although this upper-bound is a
configuration parameter (see Configuration below). When
possible, the process chooses a prime number of shards in order to
spread the number of entries across the bucket index
shards more evenly.

Detection of resharding opportunities runs as a background process
that periodically
scans all buckets. A bucket that requires resharding is added to
a queue. A thread runs in the background and processes the queueued
resharding tasks, one at a time and in order.

Multisite
=========

With Ceph releases Prior to Reef, the Ceph Object Gateway (RGW) does not support
dynamic resharding in a
multisite environment. For information on dynamic resharding, see
:ref:`Resharding <feature_resharding>` in the RGW multisite documentation.

Configuration
=============

.. confval:: rgw_dynamic_resharding
.. confval:: rgw_max_objs_per_shard
.. confval:: rgw_max_dynamic_shards
.. confval:: rgw_reshard_bucket_lock_duration
.. confval:: rgw_reshard_thread_interval
.. confval:: rgw_reshard_num_logs

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

Process tasks on the resharding queue
-------------------------------------

::

   # radosgw-admin reshard process

Bucket resharding status
------------------------

::

   # radosgw-admin reshard status --bucket <bucket_name>

The output is a JSON array of 3 objects (reshard_status, new_bucket_instance_id, num_shards) per shard.

For example, the output at each dynamic resharding stage is shown below:

``1. Before resharding occurred:``
::

  [
    {
        "reshard_status": "not-resharding",
        "new_bucket_instance_id": "",
        "num_shards": -1
    }
  ]

``2. During resharding:``
::

  [
    {
        "reshard_status": "in-progress",
        "new_bucket_instance_id": "1179f470-2ebf-4630-8ec3-c9922da887fd.8652.1",
        "num_shards": 2
    },
    {
        "reshard_status": "in-progress",
        "new_bucket_instance_id": "1179f470-2ebf-4630-8ec3-c9922da887fd.8652.1",
        "num_shards": 2
    }
  ]

``3. After resharding completed:``
::

  [
    {
        "reshard_status": "not-resharding",
        "new_bucket_instance_id": "",
        "num_shards": -1
    },
    {
        "reshard_status": "not-resharding",
        "new_bucket_instance_id": "",
        "num_shards": -1
    }
  ]


Cancel pending bucket resharding
--------------------------------

Note: Bucket resharding operations cannot be cancelled while executing. ::

   # radosgw-admin reshard cancel --bucket <bucket_name>

Manual immediate bucket resharding
----------------------------------

::

   # radosgw-admin bucket reshard --bucket <bucket_name> --num-shards <new number of shards>

When choosing a number of shards, the administrator must anticipate each
bucket's peak number of objects. Ideally one should aim for no
more than 100000 entries per shard at any given time.

Additionally, bucket index shards that are prime numbers are more effective
in evenly distributing bucket index entries.
For example, 7001 bucket index shards is better than 7000
since the former is prime. A variety of web sites have lists of prime
numbers; search for "list of prime numbers" with your favorite
search engine to locate some web sites.

Troubleshooting
===============

Clusters prior to Luminous 12.2.11 and Mimic 13.2.5 left behind stale bucket
instance entries, which were not automatically cleaned up. This issue also affected
LifeCycle policies, which were no longer applied to resharded buckets. Both of
these issues could be worked around by running ``radosgw-admin`` commands.

Stale instance management
-------------------------

List the stale instances in a cluster that are ready to be cleaned up.

::

   # radosgw-admin reshard stale-instances list

Clean up the stale instances in a cluster. Note: cleanup of these
instances should only be done on a single-site cluster.

::

   # radosgw-admin reshard stale-instances rm


Lifecycle fixes
---------------

For clusters with resharded instances, it is highly likely that the old
lifecycle processes would have flagged and deleted lifecycle processing as the
bucket instance changed during a reshard. While this is fixed for buckets
deployed on newer Ceph releases (from Mimic 13.2.6 and Luminous 12.2.12),
older buckets that had lifecycle policies and that have undergone
resharding must be fixed manually.

The command to do so is:

::

   # radosgw-admin lc reshard fix --bucket {bucketname}


If the ``--bucket`` argument is not provided, this
command will try to fix lifecycle policies for all the buckets in the cluster.

Object Expirer fixes
--------------------

Objects subject to Swift object expiration on older clusters may have
been dropped from the log pool and never deleted after the bucket was
resharded. This would happen if their expiration time was before the
cluster was upgraded, but if their expiration was after the upgrade
the objects would be correctly handled. To manage these expire-stale
objects, ``radosgw-admin`` provides two subcommands.

Listing:

::

   # radosgw-admin objects expire-stale list --bucket {bucketname}

Displays a list of object names and expiration times in JSON format.

Deleting:

::

   # radosgw-admin objects expire-stale rm --bucket {bucketname}


Initiates deletion of such objects, displaying a list of object names, expiration times, and deletion status in JSON format.
