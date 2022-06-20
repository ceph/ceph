======================
 Troubleshooting OSDs
======================

Before troubleshooting your OSDs, first check your monitors and network. If
you execute ``ceph health`` or ``ceph -s`` on the command line and Ceph shows
``HEALTH_OK``, it means that the monitors have a quorum.
If you don't have a monitor quorum or if there are errors with the monitor
status, `address the monitor issues first <../troubleshooting-mon>`_.
Check your networks to ensure they
are running properly, because networks may have a significant impact on OSD
operation and performance. Look for dropped packets on the host side
and CRC errors on the switch side.

Obtaining Data About OSDs
=========================

A good first step in troubleshooting your OSDs is to obtain topology information in
addition to the information you collected while `monitoring your OSDs`_
(e.g., ``ceph osd tree``).


Ceph Logs
---------

If you haven't changed the default path, you can find Ceph log files at
``/var/log/ceph``::

	ls /var/log/ceph

If you don't see enough log detail you can change your logging level.  See
`Logging and Debugging`_ for details to ensure that Ceph performs adequately
under high logging volume.


Admin Socket
------------

Use the admin socket tool to retrieve runtime information. For details, list
the sockets for your Ceph daemons::

	ls /var/run/ceph

Then, execute the following, replacing ``{daemon-name}`` with an actual
daemon (e.g., ``osd.0``)::

  ceph daemon osd.0 help

Alternatively, you can specify a ``{socket-file}`` (e.g., something in ``/var/run/ceph``)::

  ceph daemon {socket-file} help

The admin socket, among other things, allows you to:

- List your configuration at runtime
- Dump historic operations
- Dump the operation priority queue state
- Dump operations in flight
- Dump perfcounters

Display Freespace
-----------------

Filesystem issues may arise. To display your file system's free space, execute
``df``. ::

	df -h

Execute ``df --help`` for additional usage.

I/O Statistics
--------------

Use `iostat`_ to identify I/O-related issues. ::

	iostat -x

Diagnostic Messages
-------------------

To retrieve diagnostic messages from the kernel, use ``dmesg`` with ``less``, ``more``, ``grep``
or ``tail``.  For example::

	dmesg | grep scsi

Stopping w/out Rebalancing
==========================

Periodically, you may need to perform maintenance on a subset of your cluster,
or resolve a problem that affects a failure domain (e.g., a rack). If you do not
want CRUSH to automatically rebalance the cluster as you stop OSDs for
maintenance, set the cluster to ``noout`` first::

	ceph osd set noout

On Luminous or newer releases it is safer to set the flag only on affected OSDs.
You can do this individually ::

	ceph osd add-noout osd.0
	ceph osd rm-noout  osd.0

Or an entire CRUSH bucket at a time.  Say you're going to take down
``prod-ceph-data1701`` to add RAM ::

	ceph osd set-group noout prod-ceph-data1701

Once the flag is set you can stop the OSDs and any other colocated Ceph
services within the failure domain that requires maintenance work. ::

	systemctl stop ceph\*.service ceph\*.target

.. note:: Placement groups within the OSDs you stop will become ``degraded``
   while you are addressing issues with within the failure domain.

Once you have completed your maintenance, restart the OSDs and any other
daemons.  If you rebooted the host as part of the maintenance, these should
come back on their own without intervention. ::

	sudo systemctl start ceph.target

Finally, you must unset the cluster-wide``noout`` flag::

	ceph osd unset noout
	ceph osd unset-group noout prod-ceph-data1701

Note that most Linux distributions that Ceph supports today employ ``systemd``
for service management.  For other or older operating systems you may need
to issue equivalent ``service`` or ``start``/``stop`` commands.

.. _osd-not-running:

OSD Not Running
===============

Under normal circumstances, simply restarting the ``ceph-osd`` daemon will
allow it to rejoin the cluster and recover.

An OSD Won't Start
------------------

If you start your cluster and an OSD won't start, check the following:

- **Configuration File:** If you were not able to get OSDs running from
  a new installation, check your configuration file to ensure it conforms
  (e.g., ``host`` not ``hostname``, etc.).

- **Check Paths:** Check the paths in your configuration, and the actual
  paths themselves for data and metadata (journals, WAL, DB). If you separate the OSD data from
  the metadata and there are errors in your configuration file or in the
  actual mounts, you may have trouble starting OSDs. If you want to store the
  metadata on a separate block device, you should partition or LVM your
  drive and assign one partition per OSD.

- **Check Max Threadcount:** If you have a node with a lot of OSDs, you may be
  hitting the default maximum number of threads (e.g., usually 32k), especially
  during recovery. You can increase the number of threads using ``sysctl`` to
  see if increasing the maximum number of threads to the maximum possible
  number of threads allowed (i.e.,  4194303) will help. For example::

	sysctl -w kernel.pid_max=4194303

  If increasing the maximum thread count resolves the issue, you can make it
  permanent by including a ``kernel.pid_max`` setting in a file under ``/etc/sysctl.d`` or
  within the master ``/etc/sysctl.conf`` file. For example::

	kernel.pid_max = 4194303

- **Check ``nf_conntrack``:** This connection tracking and limiting system
  is the bane of many production Ceph clusters, and can be insidious in that
  everything is fine at first. As cluster topology and client workload
  grow, mysterious and intermittent connection failures and performance
  glitches manifest, becoming worse over time and at certain times of day.
  Check ``syslog`` history for table fillage events.  You can mitigate this
  bother by raising ``nf_conntrack_max`` to a much higher value via ``sysctl``.
  Be sure to raise ``nf_conntrack_buckets`` accordingly to
  ``nf_conntrack_max / 4``, which may require action outside of ``sysctl`` e.g.
  ``"echo 131072 > /sys/module/nf_conntrack/parameters/hashsize``
  More interdictive but fussier is to blacklist the associated kernel modules
  to disable processing altogether.  This is fragile in that the modules
  vary among kernel versions, as does the order in which they must be listed.
  Even when blacklisted there are situations in which ``iptables`` or ``docker``
  may activate connection tracking anyway, so a "set and forget" strategy for
  the tunables is advised.  On modern systems this will not consume appreciable
  resources.

- **Kernel Version:** Identify the kernel version and distribution you
  are using. Ceph uses some third party tools by default, which may be
  buggy or may conflict with certain distributions and/or kernel
  versions (e.g., Google ``gperftools`` and ``TCMalloc``). Check the
  `OS recommendations`_ and the release notes for each Ceph version
  to ensure you have addressed any issues related to your kernel.

- **Segment Fault:** If there is a segment fault, increase log levels
  and start the problematic daemon(s) again. If segment faults recur,
  search the Ceph bug tracker `https://tracker.ceph/com/projects/ceph <https://tracker.ceph.com/projects/ceph/>`_
  and the ``dev`` and ``ceph-users`` mailing list archives `https://ceph.io/resources <https://ceph.io/resources>`_.
  If this is truly a new and unique
  failure, post to the ``dev`` email list and provide the specific Ceph
  release being run, ``ceph.conf`` (with secrets XXX'd out),
  your monitor status output and excerpts from your log file(s).

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
.. _subscribe to the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=subscribe+ceph-devel
.. _unsubscribe from the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=unsubscribe+ceph-devel
.. _subscribe to the ceph-users email list: mailto:ceph-users-join@lists.ceph.com
.. _unsubscribe from the ceph-users email list: mailto:ceph-users-leave@lists.ceph.com
.. _OS recommendations: ../../../start/os-recommendations
.. _ceph-devel: ceph-devel@vger.kernel.org
