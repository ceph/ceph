==============================
 Troubleshooting OSDs and PGs
==============================

Before troubleshooting your OSDs, check your monitors and network first. If
you execute ``ceph health`` or ``ceph -s`` on the command line and Ceph returns
a health status, the return of a status means that the monitors have a quorum.
If you don't have a monitor quorum or if there are errors with the monitor
status, address the monitor issues first. Check your networks to ensure they
are running properly, because networks may have a significant impact on OSD
operation and performance.


The Ceph Community
==================

The Ceph community is an excellent source of information and help. For
operational issues with Ceph releases we recommend you `subscribe to the
ceph-users email list`_. When  you no longer want to receive emails, you can
`unsubscribe from the ceph-users email list`_. 

If you have read through this guide and you have contacted ``ceph-users``,
but you haven't resolved your issue, you may contact `Inktank`_ for support.

You may also `subscribe to the ceph-devel email list`_. You should do so if
your issue is: 

- Likely related to a bug
- Related to a development release package
- Related to a development testing package
- Related to your own builds

If you no longer want to receive emails from the ``ceph-devel`` email list, you
may `unsubscribe from the ceph-devel email list`_.

.. tip:: The Ceph community is growing rapidly, and community members can help
   you if you provide them with detailed information about your problem. See 
   `Obtaining Data About OSDs`_ before you post questions to ensure that 
   community members have sufficient data to help you.


Obtaining Data About OSDs
=========================

A good first step in troubleshooting your OSDs is to obtain information in
addition to the information you collected while `monitoring your OSDs`_
(e.g., ``ceph osd tree``).


Ceph Logs
---------

If you haven't changed the default path, you can find Ceph log files at
``/var/log/ceph``:: 

	ls /var/log/ceph

If you don't get enough log detail, you can change your logging level.  See
`Ceph Logging and Debugging`_ and `Logging and Debugging Config Reference`_ in
the Ceph Configuration documentation for details. Also, see `Debugging and
Logging`_ in the Ceph Operations documentation to ensure that Ceph performs
adequately under high logging volume.


Admin Socket
------------

Use the admin socket tool to retrieve runtime information. For details, list
the sockets for your Ceph processes:: 

	ls /var/run/ceph

Then, execute the following, replacing ``{socket-name}`` with an actual 
socket name to show the list of available options:: 

	ceph --admin-daemon /var/run/ceph/{socket-name} help

The admin socket, among other things, allows you to:

- List your configuration at runtime
- Dump historic operations
- Dump the operation priority queue state
- Dump operations in flight
- Dump perfcounters


Display Freespace
-----------------

Filesystem issues may arise. To display your filesystem's free space, execute
``df``. ::

	df -h

Execute ``df --help`` for additional usage. 


I/O Statistics
--------------

Use `iostat`_ to identify I/O-related issues. :: 

	iostat -x


Diagnostic Messages
-------------------

To retrieve diagnostic messages, use ``dmesg`` with ``less``, ``more``, ``grep``
or ``tail``.  For example:: 

	dmesg | grep scsi


Stopping w/out Rebalancing
==========================

Periodically, you may need to perform maintenance on a subset of your cluster,
or resolve a problem that affects a failure domain (e.g., a rack). If you do not
want CRUSH to automatically rebalance the cluster as you stop OSDs for
maintenance, set the cluster to ``noout`` first::

	ceph osd set noout

Once the cluster is set to ``noout``, you can begin stopping the OSDs within the
failure domain that requires maintenance work. ::

	ceph osd stop osd.{num}

.. note:: Placement groups within the OSDs you stop will become ``degraded`` 
   while you are addressing issues with within the failure domain. 

Once you have completed your maintenance, restart the OSDs. ::

	ceph osd start osd.{num}

Finally, you must unset the cluster from ``noout``. :: 

	ceph osd unset noout



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
  paths themselves for data and journals. If you separate the OSD data from 
  the journal data and there are errors in your configuration file or in the 
  actual mounts, you may have trouble starting OSDs. If you want to store the 
  journal on a block device, you should partition your journal disk and assign 
  one partition per OSD.

- **Kernel Version:** Identify the kernel version and distribution you 
  are using. Ceph uses some third party tools by default, which may be 
  buggy or may conflict with certain distributions and/or kernel 
  versions (e.g., Google perftools). Check the `OS recommendations`_ 
  to ensure you have addressed any issues related to your kernel.

- **Segment Fault:** If there is a segment fault, turn your logging up 
  (if it isn't already), and try again. If it segment faults again, 
  contact the ceph-devel email list and provide your Ceph configuration
  file, your monitor output and the contents of your log file(s).

If you cannot resolve the issue and the email list isn't helpful, you may
contact `Inktank`_ for support.


An OSD Failed
-------------

When a ``ceph-osd`` process dies, the monitor will learn about the failure
from surviving ``ceph-osd`` daemons and report it via the ``ceph health``
command::

	ceph health
	HEALTH_WARN 1/3 in osds are down

Specifically, you will get a warning whenever there are ``ceph-osd``
processes that are marked ``in`` and ``down``.  You can identify which
``ceph-osds`` are ``down`` with::

	ceph health detail
	HEALTH_WARN 1/3 in osds are down
	osd.0 is down since epoch 23, last address 192.168.106.220:6800/11080

If there is a disk
failure or other fault preventing ``ceph-osd`` from functioning or
restarting, an error message should be present in its log file in
``/var/log/ceph``.  

If the daemon stopped because of a heartbeat failure, the underlying
kernel file system may be unresponsive. Check ``dmesg`` output for disk
or other kernel errors.

If the problem is a software error (failed assertion or other
unexpected error), it should be reported to the `ceph-devel`_ email list.


No Free Drive Space
-------------------

Ceph prevents you from writing to a full OSD so that you don't lose data. 
In an operational cluster, you should receive a warning when your cluster 
is getting near its full ratio. The ``mon osd full ratio`` defaults to 
``0.95``, or 95% of capacity before it stops clients from writing data. 
The ``mon osd nearfull ratio`` defaults to ``0.85``, or 85% of capacity 
when it generates a health warning.

Full cluster issues usually arise when testing how Ceph handles an OSD 
failure on a small cluster. When one node has a high percentage of the 
cluster's data, the cluster can easily eclipse its nearfull and full ratio
immediately. If you are testing how Ceph reacts to OSD failures on a small
cluster, you should leave ample free disk space and consider temporarily
lowering the ``mon osd full ratio`` and ``mon osd nearfull ratio``.

Full ``ceph-osds`` will be reported by ``ceph health``::

	ceph health
	HEALTH_WARN 1 nearfull osds
	osd.2 is near full at 85%

Or::

	ceph health
	HEALTH_ERR 1 nearfull osds, 1 full osds
	osd.2 is near full at 85%
	osd.3 is full at 97%

The best way to deal with a full cluster is to add new ``ceph-osds``, allowing
the cluster to redistribute data to the newly available storage.

If you cannot start an OSD because it is full, you may delete some data by deleting
some placement group directories in the full OSD.

.. important:: If you choose to delete a placement group directory on a full OSD, 
   **DO NOT** delete the same placement group directory on another full OSD, or
   **YOU MAY LOSE DATA**. You **MUST** maintain at least one copy of your data on
   at least one OSD.


OSDs are Slow/Unresponsive
==========================

A commonly recurring issue involves slow or unresponsive OSDs. Ensure that you
have eliminated other troubleshooting possibilities before delving into OSD
performance issues. For example, ensure that your network(s) is working properly
and your OSDs are running. Check to see if OSDs are throttling recovery traffic.

.. tip:: Newer versions of Ceph provide better recovery handling by preventing
   recovering OSDs from using up system resources so that ``up`` and ``in`` 
   OSDs aren't available or are otherwise slow.


Networking Issues
-----------------

Ceph is a distributed storage system, so it  depends upon networks to peer with
OSDs, replicate objects, recover from faults and check heartbeats. Networking
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

A storage drive should only support one OSD. Sequential read and sequential
write throughput can bottleneck if other processes share the drive, including
journals, operating systems, monitors, other OSDs and non-Ceph processes. 

Ceph acknowledges writes *after* journaling, so fast SSDs are an attractive
option to accelerate the response time--particularly when using the ``ext4`` or
XFS filesystems. By contrast, the ``btrfs`` filesystem can write and journal
simultaneously.

.. note:: Partitioning a drive does not change its total throughput or
   sequential read/write limits. Running a journal in a separate partition
   may help, but you should prefer a separate physical drive.


Bad Sectors / Fragmented Disk
-----------------------------

Check your disks for bad sectors and fragmentation. This can cause total throughput
to drop substantially. 


Co-resident Monitors/OSDs
-------------------------

Monitors are generally light-weight processes, but they do lots of ``fsync()``,
which can interfere with other workloads, particularly if monitors run on the
same drive as your OSDs. Additionally, if you run monitors on the same host as
the OSDs, you may incur performance issues related to:

- Running an older kernel (pre-3.0)
- Running Argonaut with an old ``glibc``
- Running a kernel with no syncfs(2) syscall.

In these cases, multiple OSDs running on the same host can drag each other down
by doing lots of commits. That often leads to the bursty writes.


Co-resident Processes
---------------------

Spinning up co-resident processes such as a cloud-based solution, virtual
machines and other applications that write data to Ceph while operating on the
same hardware as OSDs can introduce significant OSD latency. Generally, we
recommend optimizing a host for use with Ceph and using other hosts for other
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

Currently, we recommend deploying clusters with XFS or ext4. The btrfs 
filesystem has many attractive features, but bugs in the filesystem may
lead to performance issues. 


Insufficient RAM
----------------

We recommend 1GB of RAM per OSD daemon. You may notice that during normal
operations, the OSD only uses a fraction of that amount (e.g., 100-200MB).
Unused RAM makes it tempting to use the excess RAM for co-resident applications,
VMs and so forth. However, when OSDs go into recovery mode, their memory
utilization spikes. If there is no RAM available, the OSD performance will slow
considerably. 


Old Requests or Slow Requests
-----------------------------

If a ``ceph-osd`` daemon is slow to respond to a request, it will generate log messages
complaining about requests that are taking too long.  The warning threshold
defaults to 30 seconds, and is configurable via the ``osd op complaint time``
option.  When this happens, the cluster log will receive messages. 

Legacy versions of Ceph complain about 'old requests`::

	osd.0 192.168.106.220:6800/18813 312 : [WRN] old request osd_op(client.5099.0:790 fatty_26485_object789 [write 0~4096] 2.5e54f643) v4 received at 2012-03-06 15:42:56.054801 currently waiting for sub ops

New versions of Ceph complain about 'slow requests`:: 

	{date} {osd.num} [WRN] 1 slow requests, 1 included below; oldest blocked for > 30.005692 secs
	{date} {osd.num}  [WRN] slow request 30.005692 seconds old, received at {date-time}: osd_op(client.4240.0:8 benchmark_data_ceph-1_39426_object7 [write 0~4194304] 0.69848840) v4 currently waiting for subops from [610]


Possible causes include:

- A bad drive (check ``dmesg`` output)
- A bug in the kernel file system bug (check ``dmesg`` output)
- An overloaded cluster (check system load, iostat, etc.)
- A bug in the ``ceph-osd`` daemon.

Possible solutions

- Remove VMs Cloud Solutions from Ceph Hosts
- Upgrade Kernel
- Upgrade Ceph
- Restart OSDs



Flapping OSDs
=============

We recommend using both a public (front-end) network and a cluster (back-end)
network so that you can better meet the capacity requirements of object replication. Another
advantage is that you can run a cluster network such that it isn't connected to
the internet, thereby preventing some denial of service attacks. When OSDs peer
and check heartbeats, they use the cluster (back-end) network when it's available.
See `Monitor/OSD Interaction`_ for details.

However, if the cluster (back-end) network fails or develops significant latency
while the public (front-end) network operates optimally, OSDs currently do not
handle this situation well. What happens is that OSDs mark each other ``down``
on the monitor, while marking themselves ``up``. We call this scenario 'flapping`.

If something is causing OSDs to 'flap' (repeatedly getting marked ``down`` and then
``up`` again), you can force the monitors to stop the flapping with::

	ceph osd set noup      # prevent osds from getting marked up
	ceph osd set nodown    # prevent osds from getting marked down

These flags are recorded in the osdmap structure::

	ceph osd dump | grep flags
	flags no-up,no-down

You can clear the flags with::

	ceph osd unset noup
	ceph osd unset nodown

Two other flags are supported, ``noin`` and ``noout``, which prevent
booting OSDs from being marked ``in`` (allocated data) or down
ceph-osds from eventually being marked ``out`` (regardless of what the
current value for ``mon osd down out interval`` is).

.. note:: ``noup``, ``noout``, and ``nodown`` are temporary in the
   sense that once the flags are cleared, the action they were blocking
   should occur shortly after.  The ``noin`` flag, on the other hand,
   prevents OSDs from being marked ``in`` on boot, and any daemons that
   started while the flag was set will remain that way.



Troubleshooting PG Errors
=========================


Placement Groups Never Get Clean
--------------------------------

There are a few cases where Ceph placement groups never get clean: 

#. **One OSD:** If you deviate from the quick start and use only one OSD, you
   will likely run into problems. OSDs report other OSDs to the monitor, and 
   also interact with other OSDs when replicating data. If you have only one 
   OSD, a second OSD cannot check its heartbeat. Also, if you remove an OSD 
   and have only one OSD remaining, you may encounter problems. An secondary 
   or tertiary OSD expects another OSD to tell it which placement groups it 
   should have. The lack of another OSD prevents this from occurring. So a 
   placement group can remain stuck “stale” forever.

#. **Pool Size = 1**: If you have only one copy of an object, no other OSD will
   tell the OSD which objects it should have. For each placement group mapped 
   to the remaining OSD (see ``ceph pg dump``), you can force the OSD to notice 
   the placement groups it needs by running::
   
   	ceph pg force_create_pg <pgid>

As a general rule, you should run your cluster with more than one OSD and a
pool size greater than 1 object replica.


Stuck Placement Groups
----------------------

It is normal for placement groups to enter states like "degraded" or "peering"
following a failure.  Normally these states indicate the normal progression
through the failure recovery process. However, if a placement group stays in one
of these states for a long time this may be an indication of a larger problem.
For this reason, the monitor will warn when placement groups get "stuck" in a
non-optimal state.  Specifically, we check for:

* ``inactive`` - The placement group has not been ``active`` for too long 
  (i.e., it hasn't been able to service read/write requests).
  
* ``unclean`` - The placement group has not been ``clean`` for too long 
  (i.e., it hasn't been able to completely recover from a previous failure).

* ``stale`` - The placement group status has not been updated by a ``ceph-osd``,
  indicating that all nodes storing this placement group may be ``down``.

You can explicitly list stuck placement groups with one of::

	ceph pg dump_stuck stale
	ceph pg dump_stuck inactive
	ceph pg dump_stuck unclean

For stuck ``stale`` placement groups, it is normally a matter of getting the
right ``ceph-osd`` daemons running again.  For stuck ``inactive`` placement
groups, it is usually a peering problem (see :ref:`failures-osd-peering`).  For
stuck ``unclean`` placement groups, there is usually something preventing
recovery from completing, like unfound objects (see
:ref:`failures-osd-unfound`);



.. _failures-osd-peering:

Placement Group Down - Peering Failure
--------------------------------------

In certain cases, the ``ceph-osd`` `Peering` process can run into
problems, preventing a PG from becoming active and usable.  For
example, ``ceph health`` might report::

	ceph health detail
	HEALTH_ERR 7 pgs degraded; 12 pgs down; 12 pgs peering; 1 pgs recovering; 6 pgs stuck unclean; 114/3300 degraded (3.455%); 1/3 in osds are down
	...
	pg 0.5 is down+peering
	pg 1.4 is down+peering
	...
	osd.1 is down since epoch 69, last address 192.168.106.220:6801/8651

We can query the cluster to determine exactly why the PG is marked ``down`` with::

	ceph pg 0.5 query

.. code-block:: javascript

 { "state": "down+peering",
   ...
   "recovery_state": [
        { "name": "Started\/Primary\/Peering\/GetInfo",
          "enter_time": "2012-03-06 14:40:16.169679",
          "requested_info_from": []},
        { "name": "Started\/Primary\/Peering",
          "enter_time": "2012-03-06 14:40:16.169659",
          "probing_osds": [
                0,
                1],
          "blocked": "peering is blocked due to down osds",
          "down_osds_we_would_probe": [
                1],
          "peering_blocked_by": [
                { "osd": 1,
                  "current_lost_at": 0,
                  "comment": "starting or marking this osd lost may let us proceed"}]},
        { "name": "Started",
          "enter_time": "2012-03-06 14:40:16.169513"}
    ]
 }

The ``recovery_state`` section tells us that peering is blocked due to
down ``ceph-osd`` daemons, specifically ``osd.1``.  In this case, we can start that ``ceph-osd``
and things will recover.

Alternatively, if there is a catastrophic failure of ``osd.1`` (e.g., disk
failure), we can tell the cluster that it is ``lost`` and to cope as
best it can. 

.. important:: This is dangerous in that the cluster cannot
   guarantee that the other copies of the data are consistent 
   and up to date.  

To instruct Ceph to continue anyway::

	ceph osd lost 1

Recovery will proceed.


.. _failures-osd-unfound:

Unfound Objects
---------------

Under certain combinations of failures Ceph may complain about
``unfound`` objects::

	ceph health detail
	HEALTH_WARN 1 pgs degraded; 78/3778 unfound (2.065%)
	pg 2.4 is active+degraded, 78 unfound

This means that the storage cluster knows that some objects (or newer
copies of existing objects) exist, but it hasn't found copies of them.
One example of how this might come about for a PG whose data is on ceph-osds
1 and 2:

* 1 goes down
* 2 handles some writes, alone
* 1 comes up
* 1 and 2 repeer, and the objects missing on 1 are queued for recovery.
* Before the new objects are copied, 2 goes down.

Now 1 knows that these object exist, but there is no live ``ceph-osd`` who
has a copy.  In this case, IO to those objects will block, and the
cluster will hope that the failed node comes back soon; this is
assumed to be preferable to returning an IO error to the user.

First, you can identify which objects are unfound with::

	ceph pg 2.4 list_missing [starting offset, in json]

.. code-block:: javascript

 { "offset": { "oid": "",
      "key": "",
      "snapid": 0,
      "hash": 0,
      "max": 0},
  "num_missing": 0,
  "num_unfound": 0,
  "objects": [
     { "oid": "object 1",
       "key": "",
       "hash": 0,
       "max": 0 },
     ...
  ],
  "more": 0}

If there are too many objects to list in a single result, the ``more``
field will be true and you can query for more.  (Eventually the
command line tool will hide this from you, but not yet.)

Second, you can identify which OSDs have been probed or might contain
data::

	ceph pg 2.4 query

.. code-block:: javascript

   "recovery_state": [
        { "name": "Started\/Primary\/Active",
          "enter_time": "2012-03-06 15:15:46.713212",
          "might_have_unfound": [
                { "osd": 1,
                  "status": "osd is down"}]},

In this case, for example, the cluster knows that ``osd.1`` might have
data, but it is ``down``.  The full range of possible states include::

 * already probed
 * querying
 * osd is down
 * not queried (yet)

Sometimes it simply takes some time for the cluster to query possible
locations.  

It is possible that there are other locations where the object can
exist that are not listed.  For example, if a ceph-osd is stopped and
taken out of the cluster, the cluster fully recovers, and due to some
future set of failures ends up with an unfound object, it won't
consider the long-departed ceph-osd as a potential location to
consider.  (This scenario, however, is unlikely.)

If all possible locations have been queried and objects are still
lost, you may have to give up on the lost objects. This, again, is
possible given unusual combinations of failures that allow the cluster
to learn about writes that were performed before the writes themselves
are recovered.  To mark the "unfound" objects as "lost"::

	ceph pg 2.5 mark_unfound_lost revert

This the final argument specifies how the cluster should deal with
lost objects.  Currently the only supported option is "revert", which
will either roll back to a previous version of the object or (if it
was a new object) forget about it entirely.  Use this with caution, as
it may confuse applications that expected the object to exist.


Homeless Placement Groups
-------------------------

It is possible for all OSDs that had copies of a given placement groups to fail.
If that's the case, that subset of the object store is unavailable, and the
monitor will receive no status updates for those placement groups.  To detect
this situation, the monitor marks any placement group whose primary OSD has
failed as ``stale``.  For example::

	ceph health
	HEALTH_WARN 24 pgs stale; 3/300 in osds are down

You can identify which placement groups are ``stale``, and what the last OSDs to
store them were, with::

	ceph health detail
	HEALTH_WARN 24 pgs stale; 3/300 in osds are down
	...
	pg 2.5 is stuck stale+active+remapped, last acting [2,0]
	...
	osd.10 is down since epoch 23, last address 192.168.106.220:6800/11080
	osd.11 is down since epoch 13, last address 192.168.106.220:6803/11539
	osd.12 is down since epoch 24, last address 192.168.106.220:6806/11861

If we want to get placement group 2.5 back online, for example, this tells us that
it was last managed by ``osd.0`` and ``osd.2``.  Restarting those ``ceph-osd``
daemons will allow the cluster to recover that placement group (and, presumably,
many others).



.. _iostat: http://en.wikipedia.org/wiki/Iostat
.. _Ceph Logging and Debugging: ../../configuration/ceph-conf#ceph-logging-and-debugging
.. _Logging and Debugging Config Reference: ../../configuration/log-and-debug-ref
.. _Debugging and Logging: ../debug
.. _Monitor/OSD Interaction: ../../configuration/mon-osd-interaction
.. _monitoring your OSDs: ../monitoring-osd-pg
.. _subscribe to the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=subscribe+ceph-devel
.. _unsubscribe from the ceph-devel email list: mailto:majordomo@vger.kernel.org?body=unsubscribe+ceph-devel
.. _subscribe to the ceph-users email list: mailto:majordomo@vger.kernel.org?body=subscribe+ceph-users
.. _unsubscribe from the ceph-users email list: mailto:majordomo@vger.kernel.org?body=unsubscribe+ceph-users
.. _Inktank: http://inktank.com
.. _OS recommendations: ../../../install/os-recommendations
.. _ceph-devel: ceph-devel@vger.kernel.org