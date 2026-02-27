.. _rgw_dynamic_bucket_index_resharding:

===================================
RGW Dynamic Bucket Index Resharding
===================================

.. versionadded:: Luminous

A bucket index object with too many entries can lead to performance
problems. This can be addressed by resharding bucket indexes.  Until
Luminous, changing the number of bucket shards (resharding) could only
be done offline, with RGW services disabled.  Since the Luminous
release Ceph has supported online bucket resharding.

Each bucket index shard can handle its entries efficiently up until
reaching a certain threshold number. If this threshold is exceeded the
system can suffer from performance issues. The dynamic resharding
feature detects this situation and automatically increases the number
of shards used by a bucket's index, resulting in a reduction of the
number of entries in each shard. This process is transparent to the
user. Writes to the target bucket can be blocked briefly during
resharding process, but reads are not.

By default dynamic bucket index resharding can only increase the
number of bucket index shards to 1999, although this upper-bound is a
configuration parameter (see `Configuration`_ below). When
possible, the process chooses a prime number of shards in order to
spread the number of entries across the bucket index
shards more evenly.

Detection of resharding opportunities runs as a background process
that periodically scans all buckets. A bucket that requires resharding
is added to a queue. A thread runs in the background and processes the
queueued resharding tasks one at a time.

Starting with Tentacle, dynamic resharding has the ability to reduce
the number of shards. Once the condition allowing reduction is noted,
there is a time delay before it will actually be executed, in case the
number of objects increases in the near future. The goal of the delay
is to avoid thrashing where resharding keeps getting re-invoked on
buckets that fluctuate in numbers of objects.

Multisite
=========

With Ceph releases prior to Reef, the Ceph Object Gateway (RGW) does not support
dynamic resharding in a
multisite deployment. For information on dynamic resharding, see
:ref:`Resharding <feature_resharding>` in the RGW multisite documentation.

Configuration
=============

.. confval:: rgw_dynamic_resharding
.. confval:: rgw_max_objs_per_shard
.. confval:: rgw_max_dynamic_shards
.. confval:: rgw_dynamic_resharding_may_reduce
.. confval:: rgw_dynamic_resharding_reduction_wait
.. confval:: rgw_reshard_bucket_lock_duration
.. confval:: rgw_reshard_thread_interval
.. confval:: rgw_reshard_num_logs
.. confval:: rgw_reshard_progress_judge_interval
.. confval:: rgw_reshard_progress_judge_ratio

Admin Commands
==============

Add a Bucket to the Resharding Queue
------------------------------------

.. prompt:: bash #

   radosgw-admin reshard add --bucket <bucket_name> --num-shards <new number of shards>

List Resharding Queue
---------------------

.. prompt:: bash #

   radosgw-admin reshard list

Process Tasks on the Resharding Queue
-------------------------------------

.. prompt:: bash #

   radosgw-admin reshard process

Bucket Resharding Status
------------------------

.. prompt:: bash #

   radosgw-admin reshard status --bucket <bucket_name>

The output is a JSON array of 3 properties (``reshard_status``, ``new_bucket_instance_id``, ``num_shards``) per shard.

For example, the output at each dynamic resharding stage is shown below:

#. Before resharding occurred:

   ::

     [
         {
             "reshard_status": "not-resharding",
             "new_bucket_instance_id": "",
             "num_shards": -1
         }
     ]

#. During resharding:

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

#. After resharding completed:

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


Cancel Pending Bucket Resharding
--------------------------------

.. note::

  Bucket resharding tasks cannot be canceled once they transition to
  the ``in-progress`` state from the initial ``not-resharding`` state.

.. prompt:: bash #

   radosgw-admin reshard cancel --bucket <bucket_name>

Manual Immediate Bucket Resharding
----------------------------------

.. prompt:: bash #

   radosgw-admin bucket reshard --bucket <bucket_name> --num-shards <new number of shards>

When choosing a number of shards, the administrator must anticipate each
bucket's peak number of objects. Ideally one should aim for no
more than 100000 entries per shard at any given time.

Additionally, bucket index shards that are prime numbers are more effective
in evenly distributing bucket index entries.
For example, 7001 bucket index shards is better than 7000
since the former is prime. A variety of web sites have lists of prime
numbers; search for "list of prime numbers" with your favorite
search engine to locate some web sites.

Setting a Bucket's Minimum Number of Shards
-------------------------------------------

.. prompt:: bash #

   radosgw-admin bucket set-min-shards --bucket <bucket_name> --num-shards <min number of shards>

Since dynamic resharding can now reduce the number of shards,
administrators may want to prevent the number of shards from becoming
too low, for example if the expect the number of objects to increase
in the future. This command allows administrators to set a per-bucket
minimum. This does not, however, prevent administrators from manually
resharding to a lower number of shards.

Troubleshooting
===============

Clusters prior to Luminous 12.2.11 and Mimic 13.2.5 left behind stale bucket
instance entries, which were not automatically cleaned up. This issue also affected
lifecycle policies, which were no longer applied to resharded buckets. Both of
these issues can be remediated by running ``radosgw-admin`` commands.

Stale Instance Management
-------------------------

List the stale instances in a cluster that may be cleaned up:

.. prompt:: bash #

   radosgw-admin reshard stale-instances list

Clean up the stale instances in a cluster:

.. prompt:: bash #

   radosgw-admin reshard stale-instances delete

.. note:: Cleanup of stale instances should not be done in a multisite deployment.


Lifecycle Fixes
---------------

For clusters with resharded instances, it is highly likely that the old
lifecycle processes would have flagged and deleted lifecycle processing as the
bucket instance changed during a reshard. While this is fixed for buckets
deployed on newer Ceph releases (from Mimic 13.2.6 and Luminous 12.2.12),
older buckets that had lifecycle policies and that have undergone
resharding must be fixed manually.

The command to do so is:

.. prompt:: bash #

   radosgw-admin lc reshard fix --bucket {bucketname}


If the ``--bucket`` argument is not provided, this
command will try to fix lifecycle policies for all the buckets in the cluster.

Object Expirer Fixes
--------------------

Objects subject to Swift object expiration on older clusters may have
been dropped from the log pool and never deleted after the bucket was
resharded. This would happen if their expiration time was before the
cluster was upgraded, but if their expiration was after the upgrade
the objects would be correctly handled. To manage these expire-stale
objects, ``radosgw-admin`` provides two subcommands.

Listing:

.. prompt:: bash #

   radosgw-admin objects expire-stale list --bucket {bucketname}

Displays a list of object names and expiration times in JSON format.

Deleting:

.. prompt:: bash #

   radosgw-admin objects expire-stale rm --bucket {bucketname}

Initiates deletion of such objects, displaying a list of object names, expiration times, and deletion status in JSON format.
