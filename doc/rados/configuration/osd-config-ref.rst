======================
 OSD Config Reference
======================

.. index:: OSD; configuration

You can configure Ceph OSD Daemons in the Ceph configuration file (or in recent
releases, the central config store), but Ceph OSD
Daemons can use the default values and a very minimal configuration. A minimal
Ceph OSD Daemon configuration sets ``host`` and
uses default values for nearly everything else.

Ceph OSD Daemons are numerically identified in incremental fashion, beginning
with ``0`` using the following convention. ::

	osd.0
	osd.1
	osd.2

In a configuration file, you may specify settings for all Ceph OSD Daemons in
the cluster by adding configuration settings to the ``[osd]`` section of your
configuration file. To add settings directly to a specific Ceph OSD Daemon
(e.g., ``host``), enter  it in an OSD-specific section of your configuration
file. For example:

.. code-block:: ini

	[osd]
		osd_journal_size = 5120

	[osd.0]
		host = osd-host-a

	[osd.1]
		host = osd-host-b


.. index:: OSD; config settings

General Settings
================

The following settings provide a Ceph OSD Daemon's ID, and determine paths to
data and journals. Ceph deployment scripts typically generate the UUID
automatically.

.. warning:: **DO NOT** change the default paths for data or journals, as it
             makes it more problematic to troubleshoot Ceph later.

When using Filestore, the journal size should be at least twice the product of the expected drive
speed multiplied by ``filestore_max_sync_interval``. However, the most common
practice is to partition the journal drive (often an SSD), and mount it such
that Ceph uses the entire partition for the journal.

.. confval:: osd_uuid
.. confval:: osd_data
.. confval:: osd_max_write_size
.. confval:: osd_max_object_size
.. confval:: osd_client_message_size_cap
.. confval:: osd_class_dir
   :default: $libdir/rados-classes

.. index:: OSD; file system

File System Settings
====================
Ceph builds and mounts file systems which are used for Ceph OSDs.

``osd_mkfs_options {fs-type}``

:Description: Options used when creating a new Ceph Filestore OSD of type {fs-type}.

:Type: String
:Default for xfs: ``-f -i 2048``
:Default for other file systems: {empty string}

For example::
  ``osd_mkfs_options_xfs = -f -d agcount=24``

``osd_mount_options {fs-type}``

:Description: Options used when mounting a Ceph Filestore OSD of type {fs-type}.

:Type: String
:Default for xfs: ``rw,noatime,inode64``
:Default for other file systems: ``rw, noatime``

For example::
  ``osd_mount_options_xfs = rw, noatime, inode64, logbufs=8``


.. index:: OSD; journal settings

Journal Settings
================

This section applies only to the older Filestore OSD back end.  Since Luminous
BlueStore has been default and preferred.

By default, Ceph expects that you will provision a Ceph OSD Daemon's journal at
the following path, which is usually a symlink to a device or partition::

	/var/lib/ceph/osd/$cluster-$id/journal

When using a single device type (for example, spinning drives), the journals
should be *colocated*: the logical volume (or partition) should be in the same
device as the ``data`` logical volume.

When using a mix of fast (SSDs, NVMe) devices with slower ones (like spinning
drives) it makes sense to place the journal on the faster device, while
``data`` occupies the slower device fully.

The default ``osd_journal_size`` value is 5120 (5 gigabytes), but it can be
larger, in which case it will need to be set in the ``ceph.conf`` file.
A value of 10 gigabytes is common in practice::

	osd_journal_size = 10240


.. confval:: osd_journal
.. confval:: osd_journal_size

See `Journal Config Reference`_ for additional details.


Monitor OSD Interaction
=======================

Ceph OSD Daemons check each other's heartbeats and report to monitors
periodically. Ceph can use default values in many cases. However, if your
network has latency issues, you may need to adopt longer intervals. See
`Configuring Monitor/OSD Interaction`_ for a detailed discussion of heartbeats.


Data Placement
==============

See `Pool & PG Config Reference`_ for details.


.. index:: OSD; scrubbing

.. _rados_config_scrubbing:

Scrubbing
=========

One way that Ceph ensures data integrity is by "scrubbing" placement groups.
Ceph scrubbing is analogous to ``fsck`` on the object storage layer. Ceph
generates a catalog of all objects in each placement group and compares each
primary object to its replicas, ensuring that no objects are missing or
mismatched. Light scrubbing checks the object size and attributes, and is
usually done daily. Deep scrubbing reads the data and uses checksums to ensure
data integrity, and is usually done weekly. The freqeuncies of both light
scrubbing and deep scrubbing are determined by the cluster's configuration,
which is fully under your control and subject to the settings explained below
in this section.

Although scrubbing is important for maintaining data integrity, it can reduce
the performance of the Ceph cluster. You can adjust the following settings to
increase or decrease the frequency and depth of scrubbing operations.


.. confval:: osd_max_scrubs
.. confval:: osd_scrub_begin_hour
.. confval:: osd_scrub_end_hour
.. confval:: osd_scrub_begin_week_day
.. confval:: osd_scrub_end_week_day
.. confval:: osd_scrub_during_recovery
.. confval:: osd_scrub_load_threshold
.. confval:: osd_scrub_min_interval
.. confval:: osd_scrub_max_interval
.. confval:: osd_scrub_chunk_min
.. confval:: osd_scrub_chunk_max
.. confval:: osd_scrub_sleep
.. confval:: osd_deep_scrub_interval
.. confval:: osd_scrub_interval_randomize_ratio
.. confval:: osd_deep_scrub_stride
.. confval:: osd_scrub_auto_repair
.. confval:: osd_scrub_auto_repair_num_errors

.. index:: OSD; operations settings

Operations
==========

.. confval:: osd_op_num_shards
.. confval:: osd_op_num_shards_hdd
.. confval:: osd_op_num_shards_ssd
.. confval:: osd_op_queue
.. confval:: osd_op_queue_cut_off
.. confval:: osd_client_op_priority
.. confval:: osd_recovery_op_priority
.. confval:: osd_scrub_priority
.. confval:: osd_requested_scrub_priority
.. confval:: osd_snap_trim_priority
.. confval:: osd_snap_trim_sleep
.. confval:: osd_snap_trim_sleep_hdd
.. confval:: osd_snap_trim_sleep_ssd
.. confval:: osd_snap_trim_sleep_hybrid
.. confval:: osd_op_thread_timeout
.. confval:: osd_op_complaint_time
.. confval:: osd_op_history_size
.. confval:: osd_op_history_duration
.. confval:: osd_op_log_threshold
.. confval:: osd_op_thread_suicide_timeout
.. note:: See https://old.ceph.com/planet/dealing-with-some-osd-timeouts/ for
   more on ``osd_op_thread_suicide_timeout``. Be aware that this is a link to a
   reworking of a blog post from 2017, and that its conclusion will direct you
   back to this page "for more information".

.. _dmclock-qos:

QoS Based on mClock
-------------------

Ceph's use of mClock is now more refined and can be used by following the
steps as described in `mClock Config Reference`_.

Core Concepts
`````````````

Ceph's QoS support is implemented using a queueing scheduler
based on `the dmClock algorithm`_. This algorithm allocates the I/O
resources of the Ceph cluster in proportion to weights, and enforces
the constraints of minimum reservation and maximum limitation, so that
the services can compete for the resources fairly. Currently the
*mclock_scheduler* operation queue divides Ceph services involving I/O
resources into following buckets:

- client op: the iops issued by client
- osd subop: the iops issued by primary OSD
- snap trim: the snap trimming related requests
- pg recovery: the recovery related requests
- pg scrub: the scrub related requests

And the resources are partitioned using following three sets of tags. In other
words, the share of each type of service is controlled by three tags:

#. reservation: the minimum IOPS allocated for the service.
#. limitation: the maximum IOPS allocated for the service.
#. weight: the proportional share of capacity if extra capacity or system
   oversubscribed.

In Ceph, operations are graded with "cost". And the resources allocated
for serving various services are consumed by these "costs". So, for
example, the more reservation a services has, the more resource it is
guaranteed to possess, as long as it requires. Assuming there are 2
services: recovery and client ops:

- recovery: (r:1, l:5, w:1)
- client ops: (r:2, l:0, w:9)

The settings above ensure that the recovery won't get more than 5
requests per second serviced, even if it requires so (see CURRENT
IMPLEMENTATION NOTE below), and no other services are competing with
it. But if the clients start to issue large amount of I/O requests,
neither will they exhaust all the I/O resources. 1 request per second
is always allocated for recovery jobs as long as there are any such
requests. So the recovery jobs won't be starved even in a cluster with
high load. And in the meantime, the client ops can enjoy a larger
portion of the I/O resource, because its weight is "9", while its
competitor "1". In the case of client ops, it is not clamped by the
limit setting, so it can make use of all the resources if there is no
recovery ongoing.

CURRENT IMPLEMENTATION NOTE: the current implementation enforces the limit
values. Therefore, if a service crosses the enforced limit, the op remains
in the operation queue until the limit is restored.

Subtleties of mClock
````````````````````

The reservation and limit values have a unit of requests per
second. The weight, however, does not technically have a unit and the
weights are relative to one another. So if one class of requests has a
weight of 1 and another a weight of 9, then the latter class of
requests should get 9 executed at a 9 to 1 ratio as the first class.
However that will only happen once the reservations are met and those
values include the operations executed under the reservation phase.

Even though the weights do not have units, one must be careful in
choosing their values due how the algorithm assigns weight tags to
requests. If the weight is *W*, then for a given class of requests,
the next one that comes in will have a weight tag of *1/W* plus the
previous weight tag or the current time, whichever is larger. That
means if *W* is sufficiently large and therefore *1/W* is sufficiently
small, the calculated tag may never be assigned as it will get a value
of the current time. The ultimate lesson is that values for weight
should not be too large. They should be under the number of requests
one expects to be serviced each second.

Caveats
```````

There are some factors that can reduce the impact of the mClock op
queues within Ceph. First, requests to an OSD are sharded by their
placement group identifier. Each shard has its own mClock queue and
these queues neither interact nor share information among them. The
number of shards can be controlled with the configuration options
:confval:`osd_op_num_shards`, :confval:`osd_op_num_shards_hdd`, and
:confval:`osd_op_num_shards_ssd`. A lower number of shards will increase the
impact of the mClock queues, but may have other deleterious effects.

Second, requests are transferred from the operation queue to the
operation sequencer, in which they go through the phases of
execution. The operation queue is where mClock resides and mClock
determines the next op to transfer to the operation sequencer. The
number of operations allowed in the operation sequencer is a complex
issue. In general we want to keep enough operations in the sequencer
so it's always getting work done on some operations while it's waiting
for disk and network access to complete on other operations. On the
other hand, once an operation is transferred to the operation
sequencer, mClock no longer has control over it. Therefore to maximize
the impact of mClock, we want to keep as few operations in the
operation sequencer as possible. So we have an inherent tension.

The configuration options that influence the number of operations in
the operation sequencer are :confval:`bluestore_throttle_bytes`,
:confval:`bluestore_throttle_deferred_bytes`,
:confval:`bluestore_throttle_cost_per_io`,
:confval:`bluestore_throttle_cost_per_io_hdd`, and
:confval:`bluestore_throttle_cost_per_io_ssd`.

A third factor that affects the impact of the mClock algorithm is that
we're using a distributed system, where requests are made to multiple
OSDs and each OSD has (can have) multiple shards. Yet we're currently
using the mClock algorithm, which is not distributed (note: dmClock is
the distributed version of mClock).

Various organizations and individuals are currently experimenting with
mClock as it exists in this code base along with their modifications
to the code base. We hope you'll share you're experiences with your
mClock and dmClock experiments on the ``ceph-devel`` mailing list.

.. confval:: osd_async_recovery_min_cost
.. confval:: osd_push_per_object_cost
.. confval:: osd_mclock_scheduler_client_res
.. confval:: osd_mclock_scheduler_client_wgt
.. confval:: osd_mclock_scheduler_client_lim
.. confval:: osd_mclock_scheduler_background_recovery_res
.. confval:: osd_mclock_scheduler_background_recovery_wgt
.. confval:: osd_mclock_scheduler_background_recovery_lim
.. confval:: osd_mclock_scheduler_background_best_effort_res
.. confval:: osd_mclock_scheduler_background_best_effort_wgt
.. confval:: osd_mclock_scheduler_background_best_effort_lim

.. _the dmClock algorithm: https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Gulati.pdf

.. index:: OSD; backfilling

Backfilling
===========

When you add or remove Ceph OSD Daemons to a cluster, CRUSH will
rebalance the cluster by moving placement groups to or from Ceph OSDs
to restore balanced utilization. The process of migrating placement groups and
the objects they contain can reduce the cluster's operational performance
considerably. To maintain operational performance, Ceph performs this migration
with 'backfilling', which allows Ceph to set backfill operations to a lower
priority than requests to read or write data.


.. confval:: osd_max_backfills
.. confval:: osd_backfill_scan_min
.. confval:: osd_backfill_scan_max
.. confval:: osd_backfill_retry_interval

.. index:: OSD; osdmap

OSD Map
=======

OSD maps reflect the OSD daemons operating in the cluster. Over time, the
number of map epochs increases. Ceph provides some settings to ensure that
Ceph performs well as the OSD map grows larger.

.. confval:: osd_map_dedup
.. confval:: osd_map_cache_size
.. confval:: osd_map_message_max

.. index:: OSD; recovery

Recovery
========

When the cluster starts or when a Ceph OSD Daemon crashes and restarts, the OSD
begins peering with other Ceph OSD Daemons before writes can occur.  See
`Monitoring OSDs and PGs`_ for details.

If a Ceph OSD Daemon crashes and comes back online, usually it will be out of
sync with other Ceph OSD Daemons containing more recent versions of objects in
the placement groups. When this happens, the Ceph OSD Daemon goes into recovery
mode and seeks to get the latest copy of the data and bring its map back up to
date. Depending upon how long the Ceph OSD Daemon was down, the OSD's objects
and placement groups may be significantly out of date. Also, if a failure domain
went down (e.g., a rack), more than one Ceph OSD Daemon may come back online at
the same time. This can make the recovery process time consuming and resource
intensive.

To maintain operational performance, Ceph performs recovery with limitations on
the number recovery requests, threads and object chunk sizes which allows Ceph
perform well in a degraded state.

.. confval:: osd_recovery_delay_start
.. confval:: osd_recovery_max_active
.. confval:: osd_recovery_max_active_hdd
.. confval:: osd_recovery_max_active_ssd
.. confval:: osd_recovery_max_chunk
.. confval:: osd_recovery_max_single_start
.. confval:: osd_recover_clone_overlap
.. confval:: osd_recovery_sleep
.. confval:: osd_recovery_sleep_hdd
.. confval:: osd_recovery_sleep_ssd
.. confval:: osd_recovery_sleep_hybrid
.. confval:: osd_recovery_priority

Tiering
=======

.. confval:: osd_agent_max_ops
.. confval:: osd_agent_max_low_ops

See `cache target dirty high ratio`_ for when the tiering agent flushes dirty
objects within the high speed mode.

Miscellaneous
=============

.. confval:: osd_default_notify_timeout
.. confval:: osd_check_for_log_corruption
.. confval:: osd_delete_sleep
.. confval:: osd_delete_sleep_hdd
.. confval:: osd_delete_sleep_ssd
.. confval:: osd_delete_sleep_hybrid
.. confval:: osd_command_max_records
.. confval:: osd_fast_fail_on_connection_refused

.. _pool: ../../operations/pools
.. _Configuring Monitor/OSD Interaction: ../mon-osd-interaction
.. _Monitoring OSDs and PGs: ../../operations/monitoring-osd-pg#peering
.. _Pool & PG Config Reference: ../pool-pg-config-ref
.. _Journal Config Reference: ../journal-ref
.. _cache target dirty high ratio: ../../operations/pools#cache-target-dirty-high-ratio
.. _mClock Config Reference: ../mclock-config-ref
