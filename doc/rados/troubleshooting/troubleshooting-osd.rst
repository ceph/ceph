======================
 Troubleshooting OSDs
======================

Before troubleshooting the cluster's OSDs, check the monitors
and the network. 

First, determine whether the monitors have a quorum. Run the ``ceph health``
command or the ``ceph -s`` command and if Ceph shows ``HEALTH_OK`` then there
is a monitor quorum. 

If the monitors don't have a quorum or if there are errors with the monitor
status, address the monitor issues before proceeding by consulting the material
in `Troubleshooting Monitors <../troubleshooting-mon>`_.

Next, check your networks to make sure that they are running properly. Networks
can have a significant impact on OSD operation and performance. Look for
dropped packets on the host side and CRC errors on the switch side.


Obtaining Data About OSDs
=========================

When troubleshooting OSDs, it is useful to collect different kinds of
information about the OSDs. Some information comes from the practice of
`monitoring OSDs`_ (for example, by running the ``ceph osd tree`` command).
Additional information concerns the topology of your cluster, and is discussed
in the following sections.


Ceph Logs
---------

Ceph log files are stored under ``/var/log/ceph``. Unless the path has been
changed (or you are in a containerized environment that stores logs in a
different location), the log files can be listed by running the following
command:

.. prompt:: bash

   ls /var/log/ceph

If there is not enough log detail, change the logging level. To ensure that
Ceph performs adequately under high logging volume, see `Logging and
Debugging`_.



Admin Socket
------------

Use the admin socket tool to retrieve runtime information. First, list the
sockets of Ceph's daemons by running the following command:

.. prompt:: bash

   ls /var/run/ceph

Next, run a command of the following form (replacing ``{daemon-name}`` with the
name of a specific daemon: for example, ``osd.0``):

.. prompt:: bash

   ceph daemon {daemon-name} help

Alternatively, run the command with a ``{socket-file}`` specified (a "socket
file" is a specific file in ``/var/run/ceph``):

.. prompt:: bash

   ceph daemon {socket-file} help

The admin socket makes many tasks possible, including:

- Listing Ceph configuration at runtime
- Dumping historic operations
- Dumping the operation priority queue state
- Dumping operations in flight
- Dumping perfcounters

Display Free Space
------------------

Filesystem issues may arise. To display your filesystems' free space, run the
following command:

.. prompt:: bash

   df -h

To see this command's supported syntax and options, run ``df --help``.

I/O Statistics
--------------

The `iostat`_ tool can be used to identify I/O-related issues. Run the
following command:

.. prompt:: bash

   iostat -x


Diagnostic Messages
-------------------

To retrieve diagnostic messages from the kernel, run the ``dmesg`` command and
specify the output with ``less``, ``more``, ``grep``, or ``tail``. For
example: 

.. prompt:: bash

    dmesg | grep scsi

Stopping without Rebalancing
============================

It might be occasionally necessary to perform maintenance on a subset of your
cluster or to resolve a problem that affects a failure domain (for example, a
rack).  However, when you stop OSDs for maintenance, you might want to prevent
CRUSH from automatically rebalancing the cluster. To avert this rebalancing
behavior, set the cluster to ``noout`` by running the following command:

.. prompt:: bash

   ceph osd set noout

.. warning:: This is more a thought exercise offered for the purpose of giving
   the reader a sense of failure domains and CRUSH behavior than a suggestion
   that anyone in the post-Luminous world run ``ceph osd set noout``. When the
   OSDs return to an ``up`` state, rebalancing will resume and the change
   introduced by the ``ceph osd set noout`` command will be reverted.

In Luminous and later releases, however, it is a safer approach to flag only
affected OSDs.  To add or remove a ``noout`` flag to a specific OSD, run a
command like the following:

.. prompt:: bash

   ceph osd add-noout osd.0
   ceph osd rm-noout  osd.0

It is also possible to flag an entire CRUSH bucket. For example, if you plan to
take down ``prod-ceph-data1701`` in order to add RAM, you might run the
following command:

.. prompt:: bash

   ceph osd set-group noout prod-ceph-data1701

After the flag is set, stop the OSDs and any other colocated
Ceph services within the failure domain that requires maintenance work::

   systemctl stop ceph\*.service ceph\*.target

.. note:: When an OSD is stopped, any placement groups within the OSD are
   marked as ``degraded``.

After the maintenance is complete, it will be necessary to restart the OSDs
and any other daemons that have stopped. However, if the host was rebooted as
part of the maintenance, they do not need to be restarted and will come back up
automatically. To restart OSDs or other daemons, use a command of the following
form:

.. prompt:: bash

   sudo systemctl start ceph.target

Finally, unset the ``noout`` flag as needed by running commands like the
following:

.. prompt:: bash

   ceph osd unset noout
   ceph osd unset-group noout prod-ceph-data1701

Many contemporary Linux distributions employ ``systemd`` for service
management.  However, for certain operating systems (especially older ones) it
might be necessary to issue equivalent ``service`` or ``start``/``stop``
commands.


.. _osd-not-running:

OSD Not Running
===============

Under normal conditions, restarting a ``ceph-osd`` daemon will allow it to
rejoin the cluster and recover.


An OSD Won't Start
------------------

If the cluster has started but an OSD isn't starting, check the following:

- **Configuration File:** If you were not able to get OSDs running from a new
  installation, check your configuration file to ensure it conforms to the
  standard (for example, make sure that it says ``host`` and not ``hostname``,
  etc.).

- **Check Paths:** Ensure that the paths specified in the configuration
  correspond to the paths for data and metadata that actually exist (for
  example, the paths to the journals, the WAL, and the DB). Separate the OSD
  data from the metadata in order to see whether there are errors in the
  configuration file and in the actual mounts. If so, these errors might
  explain why OSDs are not starting. To store the metadata on a separate block
  device, partition or LVM the drive and assign one partition per OSD.

- **Check Max Threadcount:** If the cluster has a node with an especially high
  number of OSDs, it might be hitting the default maximum number of threads
  (usually 32,000).  This is especially likely to happen during recovery.
  Increasing the maximum number of threads to the maximum possible number of
  threads allowed (4194303) might help with the problem. To increase the number
  of threads to the maximum, run the following command:

  .. prompt:: bash

     sysctl -w kernel.pid_max=4194303

  If this increase resolves the issue, you must make the increase permanent by
  including a ``kernel.pid_max`` setting either in a file under
  ``/etc/sysctl.d`` or within the master ``/etc/sysctl.conf`` file. For
  example::

     kernel.pid_max = 4194303

- **Check ``nf_conntrack``:** This connection-tracking and connection-limiting
  system causes problems for many production Ceph clusters. The problems often
  emerge slowly and subtly. As cluster topology and client workload grow,
  mysterious and intermittent connection failures and performance glitches
  occur more and more, especially at certain times of the day. To begin taking
  the measure of your problem, check the ``syslog`` history for "table full"
  events. One way to address this kind of problem is as follows: First, use the
  ``sysctl`` utility to assign ``nf_conntrack_max`` a much higher value. Next,
  raise the value of ``nf_conntrack_buckets`` so that ``nf_conntrack_buckets``
  × 8 = ``nf_conntrack_max``; this action might require running commands
  outside of ``sysctl`` (for example, ``"echo 131072 >
  /sys/module/nf_conntrack/parameters/hashsize``). Another way to address the
  problem is to blacklist the associated kernel modules in order to disable
  processing altogether. This approach is powerful, but fragile. The modules
  and the order in which the modules must be listed can vary among kernel
  versions. Even when blacklisted, ``iptables`` and ``docker`` might sometimes
  activate connection tracking anyway, so we advise a "set and forget" strategy
  for the tunables. On modern systems, this approach will not consume
  appreciable resources.

- **Kernel Version:** Identify the kernel version and distribution that are in
  use. By default, Ceph uses third-party tools that might be buggy or come into
  conflict with certain distributions or kernel versions (for example, Google's
  ``gperftools`` and ``TCMalloc``). Check the `OS recommendations`_ and the
  release notes for each Ceph version in order to make sure that you have
  addressed any issues related to your kernel.

- **Segment Fault:** If there is a segment fault, increase log levels and
  restart the problematic daemon(s). If segment faults recur, search the Ceph
  bug tracker `https://tracker.ceph/com/projects/ceph
  <https://tracker.ceph.com/projects/ceph/>`_ and the ``dev`` and
  ``ceph-users`` mailing list archives `https://ceph.io/resources
  <https://ceph.io/resources>`_ to see if others have experienced and reported
  these issues. If this truly is a new and unique failure, post to the ``dev``
  email list and provide the following information: the specific Ceph release
  being run, ``ceph.conf`` (with secrets XXX'd out), your monitor status
  output, and excerpts from your log file(s).


An OSD Failed
-------------

When a ``ceph-osd`` process dies, surviving ``ceph-osd`` daemons will report
to the mons that it appears down, which will in turn surface the new status
via the ``ceph health`` command::

	ceph health
	HEALTH_WARN 1/3 in osds are down

Specifically, you will get a warning whenever there are OSDs marked ``in``
and ``down``.  You can identify which  are ``down`` with::

	ceph health detail
	HEALTH_WARN 1/3 in osds are down
	osd.0 is down since epoch 23, last address 192.168.106.220:6800/11080

or ::

	ceph osd tree down

If there is a drive
failure or other fault preventing ``ceph-osd`` from functioning or
restarting, an error message should be present in its log file under
``/var/log/ceph``.

If the daemon stopped because of a heartbeat failure or ``suicide timeout``,
the underlying drive or filesystem may be unresponsive. Check ``dmesg``
and `syslog`  output for drive or other kernel errors.  You may need to
specify something like ``dmesg -T`` to get timestamps, otherwise it's
easy to mistake old errors for new.

If the problem is a software error (failed assertion or other
unexpected error), search the archives and tracker as above, and
report it to the `ceph-devel`_ email list if there's no clear fix or
existing bug.

.. _no-free-drive-space:

No Free Drive Space
-------------------

Ceph prevents you from writing to a full OSD so that you don't lose data.
In an operational cluster, you should receive a warning when your cluster's OSDs
and pools approach the full ratio. The ``mon_osd_full_ratio`` defaults to
``0.95``, or 95% of capacity before it stops clients from writing data.
The ``mon_osd_backfillfull_ratio`` defaults to ``0.90``, or 90 % of
capacity above which backfills will not start. The
OSD nearfull ratio defaults to ``0.85``, or 85% of capacity
when it generates a health warning.

Note that individual OSDs within a cluster will vary in how much data Ceph
allocates to them.  This utilization can be displayed for each OSD with ::

	ceph osd df

Overall cluster / pool fullness can be checked with ::

	ceph df 

Pay close attention to the **most full** OSDs, not the percentage of raw space
used as reported by ``ceph df``.  It only takes one outlier OSD filling up to
fail writes to its pool.  The space available to each pool as reported by
``ceph df`` considers the ratio settings relative to the *most full* OSD that
is part of a given pool.  The distribution can be flattened by progressively
moving data from overfull or to underfull OSDs using the ``reweight-by-utilization``
command.  With Ceph releases beginning with later revisions of Luminous one can also
exploit the ``ceph-mgr`` ``balancer`` module to perform this task automatically
and rather effectively.

The ratios can be adjusted:

::

    ceph osd set-nearfull-ratio <float[0.0-1.0]>
    ceph osd set-full-ratio <float[0.0-1.0]>
    ceph osd set-backfillfull-ratio <float[0.0-1.0]>

Full cluster issues can arise when an OSD fails either as a test or organically
within small and/or very full or unbalanced cluster. When an OSD or node
holds an outsize percentage of the cluster's data, the ``nearfull`` and ``full``
ratios may be exceeded as a result of component failures or even natural growth.
If you are testing how Ceph reacts to OSD failures on a small
cluster, you should leave ample free disk space and consider temporarily
lowering the OSD ``full ratio``, OSD ``backfillfull ratio`` and
OSD ``nearfull ratio``

Full ``ceph-osds`` will be reported by ``ceph health``::

	ceph health
	HEALTH_WARN 1 nearfull osd(s)

Or::

	ceph health detail
	HEALTH_ERR 1 full osd(s); 1 backfillfull osd(s); 1 nearfull osd(s)
	osd.3 is full at 97%
	osd.4 is backfill full at 91%
	osd.2 is near full at 87%

The best way to deal with a full cluster is to add capacity via new OSDs, enabling
the cluster to redistribute data to newly available storage.

If you cannot start a legacy Filestore OSD because it is full, you may reclaim
some space deleting a few placement group directories in the full OSD.

.. important:: If you choose to delete a placement group directory on a full OSD,
   **DO NOT** delete the same placement group directory on another full OSD, or
   **YOU WILL LOSE DATA**. You **MUST** maintain at least one copy of your data on
   at least one OSD.  This is a rare and extreme intervention, and is not to be
   undertaken lightly.

See `Monitor Config Reference`_ for additional details.

OSDs are Slow/Unresponsive
==========================

A common issue involves slow or unresponsive OSDs. Ensure that you
have eliminated other troubleshooting possibilities before delving into OSD
performance issues. For example, ensure that your network(s) is working properly
and your OSDs are running. Check to see if OSDs are throttling recovery traffic.

.. tip:: Newer versions of Ceph provide better recovery handling by preventing
   recovering OSDs from using up system resources so that ``up`` and ``in``
   OSDs are not available or are otherwise slow.

Networking Issues
-----------------

Ceph is a distributed storage system, so it relies upon networks for OSD peering
and replication, recovery from faults, and periodic heartbeats. Networking
issues can cause OSD latency and flapping OSDs. See `Flapping OSDs`_ for
details.

Ensure that Ceph processes and Ceph-dependent processes are connected and/or
listening. ::

	netstat -a | grep ceph
	netstat -l | grep ceph
	sudo netstat -p | grep ceph

Check network statistics. ::

	netstat -s

Drive Configuration
-------------------

A SAS or SATA storage drive should only house one OSD; NVMe drives readily
handle two or more. Read and write throughput can bottleneck if other processes
share the drive, including journals / metadata, operating systems, Ceph monitors,
`syslog` logs, other OSDs, and non-Ceph processes.

Ceph acknowledges writes *after* journaling, so fast SSDs are an
attractive option to accelerate the response time--particularly when
using the ``XFS`` or ``ext4`` file systems for legacy Filestore OSDs.
By contrast, the ``Btrfs``
file system can write and journal simultaneously.  (Note, however, that
we recommend against using ``Btrfs`` for production deployments.)

.. note:: Partitioning a drive does not change its total throughput or
   sequential read/write limits. Running a journal in a separate partition
   may help, but you should prefer a separate physical drive.

Bad Sectors / Fragmented Disk
-----------------------------

Check your drives for bad blocks, fragmentation, and other errors that can cause
performance to drop substantially.  Invaluable tools include ``dmesg``, ``syslog``
logs, and ``smartctl`` (from the ``smartmontools`` package).

Co-resident Monitors/OSDs
-------------------------

Monitors are relatively lightweight processes, but they issue lots of
``fsync()`` calls,
which can interfere with other workloads, particularly if monitors run on the
same drive as an OSD. Additionally, if you run monitors on the same host as
OSDs, you may incur performance issues related to:

- Running an older kernel (pre-3.0)
- Running a kernel with no ``syncfs(2)`` syscall.

In these cases, multiple OSDs running on the same host can drag each other down
by doing lots of commits. That often leads to the bursty writes.

Co-resident Processes
---------------------

Spinning up co-resident processes (convergence) such as a cloud-based solution, virtual
machines and other applications that write data to Ceph while operating on the
same hardware as OSDs can introduce significant OSD latency. Generally, we
recommend optimizing hosts for use with Ceph and using other hosts for other
processes. The practice of separating Ceph operations from other applications
may help improve performance and may streamline troubleshooting and maintenance.

Logging Levels
--------------

If you turned logging levels up to track an issue and then forgot to turn
logging levels back down, the OSD may be putting a lot of logs onto the disk. If
you intend to keep logging levels high, you may consider mounting a drive to the
default path for logging (i.e., ``/var/log/ceph/$cluster-$name.log``).

Recovery Throttling
-------------------

Depending upon your configuration, Ceph may reduce recovery rates to maintain
performance or it may increase recovery rates to the point that recovery
impacts OSD performance. Check to see if the OSD is recovering.

Kernel Version
--------------

Check the kernel version you are running. Older kernels may not receive
new backports that Ceph depends upon for better performance.

Kernel Issues with SyncFS
-------------------------

Try running one OSD per host to see if performance improves. Old kernels
might not have a recent enough version of ``glibc`` to support ``syncfs(2)``.

Filesystem Issues
-----------------

Currently, we recommend deploying clusters with the BlueStore back end.
When running a pre-Luminous release or if you have a specific reason to deploy
OSDs with the previous Filestore backend, we recommend ``XFS``.

We recommend against using ``Btrfs`` or ``ext4``.  The ``Btrfs`` filesystem has
many attractive features, but bugs may lead to
performance issues and spurious ENOSPC errors.  We do not recommend
``ext4`` for Filestore OSDs because ``xattr`` limitations break support for long
object names, which are needed for RGW.

For more information, see `Filesystem Recommendations`_.

.. _Filesystem Recommendations: ../configuration/filesystem-recommendations

Insufficient RAM
----------------

We recommend a *minimum* of 4GB of RAM per OSD daemon and suggest rounding up
from 6-8GB.  You may notice that during normal operations, ``ceph-osd``
processes only use a fraction of that amount.
Unused RAM makes it tempting to use the excess RAM for co-resident
applications or to skimp on each node's memory capacity.  However,
when OSDs experience recovery their memory utilization spikes. If
there is insufficient RAM available, OSD performance will slow considerably
and the daemons may even crash or be killed by the Linux ``OOM Killer``.

Blocked Requests or Slow Requests
---------------------------------

If a ``ceph-osd`` daemon is slow to respond to a request, messages will be logged
noting ops that are taking too long.  The warning threshold
defaults to 30 seconds and is configurable via the ``osd_op_complaint_time``
setting.  When this happens, the cluster log will receive messages.

Legacy versions of Ceph complain about ``old requests``::

	osd.0 192.168.106.220:6800/18813 312 : [WRN] old request osd_op(client.5099.0:790 fatty_26485_object789 [write 0~4096] 2.5e54f643) v4 received at 2012-03-06 15:42:56.054801 currently waiting for sub ops

New versions of Ceph complain about ``slow requests``::

	{date} {osd.num} [WRN] 1 slow requests, 1 included below; oldest blocked for > 30.005692 secs
	{date} {osd.num}  [WRN] slow request 30.005692 seconds old, received at {date-time}: osd_op(client.4240.0:8 benchmark_data_ceph-1_39426_object7 [write 0~4194304] 0.69848840) v4 currently waiting for subops from [610]

Possible causes include:

- A failing drive (check ``dmesg`` output)
- A bug in the kernel file system (check ``dmesg`` output)
- An overloaded cluster (check system load, iostat, etc.)
- A bug in the ``ceph-osd`` daemon.

Possible solutions:

- Remove VMs from Ceph hosts
- Upgrade kernel
- Upgrade Ceph
- Restart OSDs
- Replace failed or failing components

Debugging Slow Requests
-----------------------

If you run ``ceph daemon osd.<id> dump_historic_ops`` or ``ceph daemon osd.<id> dump_ops_in_flight``,
you will see a set of operations and a list of events each operation went
through. These are briefly described below.

Events from the Messenger layer:

- ``header_read``: When the messenger first started reading the message off the wire.
- ``throttled``: When the messenger tried to acquire memory throttle space to read
  the message into memory.
- ``all_read``: When the messenger finished reading the message off the wire.
- ``dispatched``: When the messenger gave the message to the OSD.
- ``initiated``: This is identical to ``header_read``. The existence of both is a
  historical oddity.

Events from the OSD as it processes ops:

- ``queued_for_pg``: The op has been put into the queue for processing by its PG.
- ``reached_pg``: The PG has started doing the op.
- ``waiting for \*``: The op is waiting for some other work to complete before it
  can proceed (e.g. a new OSDMap; for its object target to scrub; for the PG to
  finish peering; all as specified in the message).
- ``started``: The op has been accepted as something the OSD should do and 
  is now being performed.
- ``waiting for subops from``: The op has been sent to replica OSDs.

Events from ```Filestore```:

- ``commit_queued_for_journal_write``: The op has been given to the FileStore.
- ``write_thread_in_journal_buffer``: The op is in the journal's buffer and waiting
  to be persisted (as the next disk write).
- ``journaled_completion_queued``: The op was journaled to disk and its callback
  queued for invocation.

Events from the OSD after data has been given to underlying storage:

- ``op_commit``: The op has been committed (i.e. written to journal) by the
  primary OSD.
- ``op_applied``: The op has been `write()'en <https://www.freebsd.org/cgi/man.cgi?write(2)>`_ to the backing FS (i.e.   applied in memory but not flushed out to disk) on the primary.
- ``sub_op_applied``: ``op_applied``, but for a replica's "subop".
- ``sub_op_committed``: ``op_commit``, but for a replica's subop (only for EC pools).
- ``sub_op_commit_rec/sub_op_apply_rec from <X>``: The primary marks this when it
  hears about the above, but for a particular replica (i.e. ``<X>``).
- ``commit_sent``: We sent a reply back to the client (or primary OSD, for sub ops).

Many of these events are seemingly redundant, but cross important boundaries in
the internal code (such as passing data across locks into new threads).

Flapping OSDs
=============

When OSDs peer and check heartbeats, they use the cluster (back-end)
network when it's available. See `Monitor/OSD Interaction`_ for details.

We have traditionally recommended separate *public* (front-end) and *private*
(cluster / back-end / replication) networks:

#. Segregation of heartbeat and replication / recovery traffic (private)
   from client and OSD <-> mon traffic (public).  This helps keep one
   from DoS-ing the other, which could in turn result in a cascading failure.

#. Additional throughput for both public and private traffic.

When common networking technologies were 100Mb/s and 1Gb/s, this separation
was often critical.  With today's 10Gb/s, 40Gb/s, and 25/50/100Gb/s
networks, the above capacity concerns are often diminished or even obviated.
For example, if your OSD nodes have two network ports, dedicating one to
the public and the other to the private network means no path redundancy.
This degrades your ability to weather network maintenance and failures without
significant cluster or client impact.  Consider instead using both links
for just a public network:  with bonding (LACP) or equal-cost routing (e.g. FRR)
you reap the benefits of increased throughput headroom, fault tolerance, and
reduced OSD flapping.

When a private network (or even a single host link) fails or degrades while the
public network operates normally, OSDs may not handle this situation well. What
happens is that OSDs use the public network to report each other ``down`` to
the monitors, while marking themselves ``up``. The monitors then send out,
again on the public network, an updated cluster map with affected OSDs marked
`down`. These OSDs reply to the monitors "I'm not dead yet!", and the cycle
repeats.  We call this scenario 'flapping`, and it can be difficult to isolate
and remediate.  With no private network, this irksome dynamic is avoided:
OSDs are generally either ``up`` or ``down`` without flapping.

If something does cause OSDs to 'flap' (repeatedly getting marked ``down`` and
then ``up`` again), you can force the monitors to halt the flapping by
temporarily freezing their states::

	ceph osd set noup      # prevent OSDs from getting marked up
	ceph osd set nodown    # prevent OSDs from getting marked down

These flags are recorded in the osdmap::

	ceph osd dump | grep flags
	flags no-up,no-down

You can clear the flags with::

	ceph osd unset noup
	ceph osd unset nodown

Two other flags are supported, ``noin`` and ``noout``, which prevent
booting OSDs from being marked ``in`` (allocated data) or protect OSDs
from eventually being marked ``out`` (regardless of what the current value for
``mon_osd_down_out_interval`` is).

.. note:: ``noup``, ``noout``, and ``nodown`` are temporary in the
   sense that once the flags are cleared, the action they were blocking
   should occur shortly after.  The ``noin`` flag, on the other hand,
   prevents OSDs from being marked ``in`` on boot, and any daemons that
   started while the flag was set will remain that way.

.. note:: The causes and effects of flapping can be somewhat mitigated through
   careful adjustments to the ``mon_osd_down_out_subtree_limit``,
   ``mon_osd_reporter_subtree_level``, and ``mon_osd_min_down_reporters``.
   Derivation of optimal settings depends on cluster size, topology, and the
   Ceph  release in use. Their interactions are subtle and beyond the scope of
   this document.


.. _iostat: https://en.wikipedia.org/wiki/Iostat
.. _Ceph Logging and Debugging: ../../configuration/ceph-conf#ceph-logging-and-debugging
.. _Logging and Debugging: ../log-and-debug
.. _Debugging and Logging: ../debug
.. _Monitor/OSD Interaction: ../../configuration/mon-osd-interaction
.. _Monitor Config Reference: ../../configuration/mon-config-ref
.. _monitoring your OSDs: ../../operations/monitoring-osd-pg
.. _monitoring OSDs: ../../operations/monitoring-osd-pg/#monitoring-osds
.. _subscribe to the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=subscribe+ceph-devel
.. _unsubscribe from the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=unsubscribe+ceph-devel
.. _subscribe to the ceph-users email list: mailto:ceph-users-join@lists.ceph.com
.. _unsubscribe from the ceph-users email list: mailto:ceph-users-leave@lists.ceph.com
.. _OS recommendations: ../../../start/os-recommendations
.. _ceph-devel: ceph-devel@vger.kernel.org
