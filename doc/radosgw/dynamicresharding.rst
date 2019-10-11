===================================
RGW Dynamic Bucket Index Resharding
===================================

.. versionadded:: Luminous

A large bucket index can lead to performance problems. In order
to address this problem we introduced bucket index sharding.
Until Luminous, changing the number of bucket shards (resharding)
needed to be done offline. Starting with Luminous we support
online bucket resharding.

Each bucket index shard can handle its entries efficiently up until
reaching a certain threshold number of entries. If this threshold is exceeded the system
can encounter performance issues.
The dynamic resharding feature detects this situation and automatically
increases the number of shards used by the bucket index,
resulting in the reduction of the number of entries in each bucket index shard.
This process is transparent to the user.

The detection process runs:

1. when new objects are added to the bucket and
2. in a background process that periodically scans all the buckets.

The background process is needed in order to deal with existing buckets in the system that are not being updated.
A bucket that requires resharding is added to the resharding queue and will be
scheduled to be resharded later.
The reshard threads run in the background and execute the scheduled resharding tasks, one at a time.

Multisite
=========

Dynamic resharding is not supported in a multisite environment.


Configuration
=============

Enable/Disable dynamic bucket index resharding:

- ``rgw_dynamic_resharding``:  true/false, default: true

Configuration options that control the resharding process:

- ``rgw_reshard_num_logs``: number of shards for the resharding queue, default: 16

- ``rgw_reshard_bucket_lock_duration``: duration, in seconds, of lock on bucket obj during resharding, default: 120 seconds

- ``rgw_max_objs_per_shard``: maximum number of objects per bucket index shard before resharding is triggered, default: 100000 objects

- ``rgw_reshard_thread_interval``: maximum time, in seconds, between rounds of resharding queue processing, default: 600 seconds


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

The output is a json array of 3 objects (reshard_status, new_bucket_instance_id, num_shards) per shard.

For example, the output at different Dynamic Resharding stages is shown below:

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

``3, After resharding completed:``
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

Note: Ongoing bucket resharding operations cannot be cancelled. ::

   # radosgw-admin reshard cancel --bucket <bucket_name>

Manual immediate bucket resharding
----------------------------------

::

   # radosgw-admin bucket reshard --bucket <bucket_name> --num-shards <new number of shards>


Troubleshooting
===============

Clusters prior to Luminous 12.2.11 and Mimic 13.2.5 left behind stale bucket
instance entries, which were not automatically cleaned up. The issue also affected
LifeCycle policies, which were not applied to resharded buckets anymore. Both of
these issues can be worked around using a couple of radosgw-admin commands.

Stale instance management
-------------------------

List the stale instances in a cluster that are ready to be cleaned up.

::

   # radosgw-admin reshard stale-instances list

Clean up the stale instances in a cluster. Note: cleanup of these
instances should only be done on a single site cluster.

::

   # radosgw-admin reshard stale-instances rm


Lifecycle fixes
---------------

For clusters that had resharded instances, it is highly likely that the old
lifecycle processes would have flagged and deleted lifecycle processing as the
bucket instance changed during a reshard. While this is fixed for newer clusters
(from Mimic 13.2.6 and Luminous 12.2.12), older buckets that had lifecycle policies and
that have undergone resharding will have to be manually fixed.

The command to do so is:

::

   # radosgw-admin lc reshard fix --bucket {bucketname}


As a convenience wrapper, if the ``--bucket`` argument is dropped then this
command will try and fix lifecycle policies for all the buckets in the cluster.

Object Expirer fixes
--------------------

Objects subject to Swift object expiration on older clusters may have
been dropped from the log pool and never deleted after the bucket was
resharded. This would happen if their expiration time was before the
cluster was upgraded, but if their expiration was after the upgrade
the objects would be correctly handled. To manage these expire-stale
objects, radosgw-admin provides two subcommands.

Listing:

::

   # radosgw-admin objects expire-stale list --bucket {bucketname}

Displays a list of object names and expiration times in JSON format.

Deleting:

::

   # radosgw-admin objects expire-stale rm --bucket {bucketname}


Initiates deletion of such objects, displaying a list of object names, expiration times, and deletion status in JSON format.
