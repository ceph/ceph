=================
 Troubleshooting
=================

Slow/stuck operations
=====================

Sometimes CephFS operations hang. The first step in troubleshooting them is to
locate the problem causing the operations to hang. Problems present in three
places:

#. in the client
#. in the MDS
#. in the network that connects the client to the MDS

First, use the procedure in :ref:`slow_requests` to determine if the client has
stuck operations or the MDS has stuck operations.

Dump the MDS cache. The contents of the MDS cache will be used to diagnose the
nature of the problem. Run the following command to dump the MDS cache:

.. prompt:: bash #

   ceph daemon mds.<name> dump cache /tmp/dump.txt

.. note:: MDS services that are not controlled by systemd dump the file 
   ``dump.txt`` to the machine that runs the MDS. MDS services that are
   controlled by systemd dump the file ``dump.txt`` to a tmpfs in the MDS
   container. Use `nsenter(1)` to locate ``dump.txt`` or specify another
   system-wide path.

If high logging levels have been set on the MDS, ``dump.txt`` can be expected
to hold the information needed to diagnose and solve the issue causing the
CephFS operations to hang.

.. _cephfs_dr_stuck_during_recovery:

Stuck during recovery
=====================

Stuck in up:replay
------------------

If your MDS is stuck in the ``up:replay`` state, then the journal is probably
very long. The presence of ``MDS_HEALTH_TRIM`` cluster warnings can indicate
that the MDS has not yet caught up while trimming its journal. Very large
journals can take hours to process. There is no working around this, but there
are things you can do to speed up the process:

Temporarily disable MDS debug logs by reducing MDS debugging to ``0``. Even
with the default settings, the MDS logs a few messages to memory for dumping in
case a fatal error is encountered. You can turn off all logging by running the
following commands:

.. prompt:: bash #

   ceph config set mds debug_mds 0
   ceph config set mds debug_ms 0
   ceph config set mds debug_monc 0

Remember that when you set ``debug_mds``, ``debug_ms``, and ``debug_monc`` to
``0``, if the MDS fails then there will be no debugging information that can be
used to determine why fatal errors occurred. If you can calculate when
``up:replay`` will complete, restore these configurations just prior to
entering the next state:

.. code:: bash

   ceph config rm mds debug_mds
   ceph config rm mds debug_ms
   ceph config rm mds debug_monc

After replay has been expedited, calculate when the MDS will complete the
replay. Examine the journal replay status:

.. code:: bash

   $ ceph tell mds.<fs_name>:0 status | jq .replay_status
   {
     "journal_read_pos": 4195244,
     "journal_write_pos": 4195244,
     "journal_expire_pos": 4194304,
     "num_events": 2,
     "num_segments": 2
   }

Replay completes when the ``journal_read_pos`` reaches the
``journal_write_pos``. The write position does not change during replay. Track
the progression of the read position to compute the expected time to complete.

.. In Tentacle and later releases, the following text appears. It should not be
   backported to any branch prior to Tentacle, because the
   ``MDS_ESTIMATED_REPLAY_TIME`` warning was never a backport candidate for the
   Squid and the Reef release branches. See
   https://github.com/ceph/ceph/pull/65058#discussion_r2281247895. Here is the
   text that should not be backported:

   BEGIN QUOTED TEXT
   The MDS emits an `MDS_ESTIMATED_REPLAY_TIME` warning when the act of
   replaying the journal takes more than 30 seconds. The warning message
   includes an estimated time to the completion of journal replay::

  mds.a(mds.0): replay: 50.0446% complete - elapsed time: 582s, estimated time remaining: 581s

   END QUOTED TEXT

Avoiding recovery roadblocks
----------------------------

Do the following when restoring your file system: 

* **Deny all reconnection to clients.** Blocklist all existing CephFS sessions,
  causing all mounts to hang or become unavailable:

  .. prompt:: bash #

     ceph config set mds mds_deny_all_reconnect true

  Remember to undo this after the MDS becomes active.

  .. note:: This does not prevent new sessions from connecting. Use the
     ``refuse_client_session`` file-system setting to prevent new sessions from
     connecting to the CephFS.

* **Extend the MDS heartbeat grace period.** Doing this causes the system to
  avoid replacing an MDS that becomes "stuck" during an operation. Sometimes
  recovery of an MDS may involve operations that take longer than expected
  (from the programmer's perspective). This is more likely when recovery has 
  already taken longer than normal to complete (which, if you're reading this
  document, is likely the situation you find yourself in). Avoid unnecessary
  replacement loops by running the following command and extending the
  heartbeat grace period:

   .. prompt:: bash #

      ceph config set mds mds_heartbeat_grace 3600

  .. note:: This causes the MDS to continue to send beacons to the monitors
     even when its internal "heartbeat" mechanism has not been reset (it has
     not beaten) in one hour. In the past, this was achieved with the
     ``mds_beacon_grace`` monitor setting.

* **Disable open-file-table prefetch.** Under normal circumstances, the MDS
  prefetches directory contents during recovery as a way of heating up its
  cache. During a long recovery, the cache is probably already hot **and
  large**. If the cache is already hot and large, this prefetching is
  unnecessary and can be undesirable. Disable open-file-table prefetching by
  running the following command:

  .. prompt:: bash #

     ceph config set mds mds_oft_prefetch_dirfrags false

* **Turn off clients.** Clients that reconnect to the newly ``up:active`` MDS
  can create new load on the file system just as it is becoming operational.
  This is often undesirable. Maintenance is often necessary before allowing
  clients to connect to the file system and before resuming a regular workload.
  For example, expediting the trimming of journals may be advisable if the
  recovery took a long time due to the amount of time replay spent in reading a
  very large journal.

  Client sessions can be refused manually, or by using the
  ``refuse_client_session`` tunable as in the following command: 

  .. prompt:: bash #

     ceph fs set <fs_name> refuse_client_session true

  This command has the effect of preventing clients from establishing new
  sessions with the MDS.

* **Do not tweak max_mds.** Modifying the file-system setting variable
  ``max_mds`` may seem like a good idea during troubleshooting and recovery,
  but it probably isn't. Modifying ``max_mds`` might have the effect of further
  destabilizing the cluster. If ``max_mds`` must be changed in such
  circumstances, run the command to change ``max_mds`` with the confirmation
  flag (``--yes-i-really-mean-it``).

.. _pause-purge-threads:

* **Turn off async purge threads.** The volumes plugin spawns threads that
  asynchronously purge trashed or deleted subvolumes. During troubleshooting or
  recovery, these purge threads can be disabled by running the following
  command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_purging true

  To resume purging, run the following command:
  
  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_purging false

.. _pause-clone-threads:

* **Turn off async cloner threads.** The volumes plugin spawns threads that
  asynchronously clone subvolume snapshots. During troubleshooting or recovery,
  these cloner threads can be disabled by running the following command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_cloning true

  To resume cloning, run the following command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_cloning false


Expediting MDS journal trim
===========================

If your MDS journal grew too large (maybe your MDS was stuck in up:replay for a
long time!), you will want to have the MDS trim its journal more frequently.
You will know the journal is too large because of ``MDS_HEALTH_TRIM`` warnings.

The main tunable available to do this is to modify the MDS tick interval. The
"tick" interval drives several upkeep activities in the MDS. It is strongly
recommended no significant file system load be present when modifying this tick
interval. This setting only affects an MDS in ``up:active``. The MDS does not
trim its journal during recovery.

.. code:: bash

   ceph config set mds mds_tick_interval 2


RADOS Health
============

If part of the CephFS metadata or data pools is unavailable and CephFS is not
responding, it is probably because RADOS itself is unhealthy. Resolve those
problems first (:doc:`../../rados/troubleshooting/index`).

The MDS
=======

If an operation is hung inside the MDS, it will eventually show up in ``ceph health``,
identifying "slow requests are blocked". It may also identify clients as
"failing to respond" or misbehaving in other ways. If the MDS identifies
specific clients as misbehaving, you should investigate why they are doing so.

Generally it will be the result of

#. Overloading the system (if you have extra RAM, increase the
   "mds cache memory limit" config from its default 1GiB; having a larger active
   file set than your MDS cache is the #1 cause of this!).

#. Running an older (misbehaving) client.

#. Underlying RADOS issues.

Otherwise, you have probably discovered a new bug and should report it to
the developers!

.. _slow_requests:

Slow requests (MDS)
-------------------
You can list current operations via the admin socket by running::

  ceph daemon mds.<name> dump_ops_in_flight

from the MDS host. Identify the stuck commands and examine why they are stuck.
Usually the last "event" will have been an attempt to gather locks, or sending
the operation off to the MDS log. If it is waiting on the OSDs, fix them. If
operations are stuck on a specific inode, you probably have a client holding
caps which prevent others from using it, either because the client is trying
to flush out dirty data or because you have encountered a bug in CephFS'
distributed file lock code (the file "capabilities" ["caps"] system).

If it's a result of a bug in the capabilities code, restarting the MDS
is likely to resolve the problem.

If there are no slow requests reported on the MDS, and it is not reporting
that clients are misbehaving, either the client has a problem or its
requests are not reaching the MDS.

.. _ceph_fuse_debugging:

ceph-fuse debugging
===================

ceph-fuse also supports ``dump_ops_in_flight``. See if it has any and where they are
stuck.

Debug output
------------

To get more debugging information from ceph-fuse, try running in the foreground
with logging to the console (``-d``) and enabling client debug
(``--debug-client=20``), enabling prints for each message sent
(``--debug-ms=1``).

If you suspect a potential monitor issue, enable monitor debugging as well
(``--debug-monc=20``).

.. _kernel_mount_debugging:

Kernel mount debugging
======================

If there is an issue with the kernel client, the most important thing is
figuring out whether the problem is with the kernel client or the MDS. Generally,
this is easy to work out. If the kernel client broke directly, there will be
output in ``dmesg``. Collect it and any inappropriate kernel state.

Slow requests
-------------

Unfortunately, the kernel client does not provide an admin socket. However,
the the kernel on the client has `debugfs
<https://docs.kernel.org/filesystems/debugfs.html>`_ enabled, interfaces
similar to the admin socket are available. 

Find a folder in ``/sys/kernel/debug/ceph/`` with a name like 
``28f7427e-5558-4ffd-ae1a-51ec3042759a.client25386880``.
That folder contains files that can be used to diagnose the causes of slow requests. Use ``cat`` to see their contents.  

These files are described below. The files most useful for diagnosis of slow
requests are the ``mdsc`` (current requests to the MDS) and the ``osdc``
(current operations in-flight to OSDs) files.

* ``bdi``: BDI info about the Ceph system (blocks dirtied, written, etc)
* ``caps``: counts of file "caps" structures in-memory and used
* ``client_options``: dumps the options provided to the CephFS mount
* ``dentry_lru``: Dumps the CephFS dentries currently in-memory
* ``mdsc``: Dumps current requests to the MDS
* ``mdsmap``: Dumps the current MDSMap epoch and MDSes
* ``mds_sessions``: Dumps the current sessions to MDSes
* ``monc``: Dumps the current maps from the monitor, and any "subscriptions" held
* ``monmap``: Dumps the current monitor map epoch and monitors
* ``osdc``: Dumps the current ops in-flight to OSDs (ie, file data IO)
* ``osdmap``: Dumps the current OSDMap epoch, pools, and OSDs

If the data pool is in a ``NEARFULL`` condition, then the kernel CephFS client
will switch to doing writes synchronously. Synchronous writes are quite slow.

Disconnected+Remounted FS
=========================
Because CephFS has a "consistent cache", if your network connection is
disrupted for a long enough time, the client will be forcibly
disconnected from the system. At this point, the kernel client is in
a bind: it cannot safely write back dirty data, and many applications
do not handle IO errors correctly on close().
At the moment, the kernel client will remount the FS, but outstanding file system
IO may or may not be satisfied. In these cases, you may need to reboot your
client system.

You can identify you are in this situation if dmesg/kern.log report something like::

   Jul 20 08:14:38 teuthology kernel: [3677601.123718] ceph: mds0 closed our session
   Jul 20 08:14:38 teuthology kernel: [3677601.128019] ceph: mds0 reconnect start
   Jul 20 08:14:39 teuthology kernel: [3677602.093378] ceph: mds0 reconnect denied
   Jul 20 08:14:39 teuthology kernel: [3677602.098525] ceph:  dropping dirty+flushing Fw state for ffff8802dc150518 1099935956631
   Jul 20 08:14:39 teuthology kernel: [3677602.107145] ceph:  dropping dirty+flushing Fw state for ffff8801008e8518 1099935946707
   Jul 20 08:14:39 teuthology kernel: [3677602.196747] libceph: mds0 172.21.5.114:6812 socket closed (con state OPEN)
   Jul 20 08:14:40 teuthology kernel: [3677603.126214] libceph: mds0 172.21.5.114:6812 connection reset
   Jul 20 08:14:40 teuthology kernel: [3677603.132176] libceph: reset on mds0

This is an area of ongoing work to improve the behavior. Kernels will soon
be reliably issuing error codes to in-progress IO, although your application(s)
may not deal with them well. In the longer-term, we hope to allow reconnect
and reclaim of data in cases where it won't violate POSIX semantics (generally,
data which hasn't been accessed or modified by other clients).

Mounting
========

Mount 5 Error
-------------

A mount 5 error typically occurs if a MDS server is laggy or if it crashed.
Ensure at least one MDS is up and running, and the cluster is ``active +
healthy``. 

Mount 12 Error
--------------

A mount 12 error with ``cannot allocate memory`` usually occurs if you  have a
version mismatch between the :term:`Ceph Client` version and the :term:`Ceph
Storage Cluster` version. Check the versions using::

	ceph -v
	
If the Ceph Client is behind the Ceph cluster, try to upgrade it::

	sudo apt-get update && sudo apt-get install ceph-common 

You may need to uninstall, autoclean and autoremove ``ceph-common`` 
and then reinstall it so that you have the latest version.

Dynamic Debugging
=================

You can enable dynamic debug against the CephFS module.

Please see: https://github.com/ceph/ceph/blob/master/src/script/kcon_all.sh

In-memory Log Dump
==================

In-memory logs can be dumped by setting ``mds_extraordinary_events_dump_interval``
during a lower level debugging (log level < 10). ``mds_extraordinary_events_dump_interval``
is the interval in seconds for dumping the recent in-memory logs when there is an Extra-Ordinary event.

The Extra-Ordinary events are classified as:

* Client Eviction
* Missed Beacon ACK from the monitors
* Missed Internal Heartbeats

In-memory Log Dump is disabled by default to prevent log file bloat in a production environment.
The below commands consecutively enables it::

  $ ceph config set mds debug_mds <log_level>/<gather_level>
  $ ceph config set mds mds_extraordinary_events_dump_interval <seconds>

The ``log_level`` should be < 10 and ``gather_level`` should be >= 10 to enable in-memory log dump.
When it is enabled, the MDS checks for the extra-ordinary events every
``mds_extraordinary_events_dump_interval`` seconds and if any of them occurs, MDS dumps the
in-memory logs containing the relevant event details in ceph-mds log.

.. note:: For higher log levels (log_level >= 10) there is no reason to dump the In-memory Logs and a
          lower gather level (gather_level < 10) is insufficient to gather In-memory Logs. Thus a
          log level >=10 or a gather level < 10 in debug_mds would prevent enabling the In-memory Log Dump.
          In such cases, when there is a failure it's required to reset the value of
          mds_extraordinary_events_dump_interval to 0 before enabling using the above commands.

The In-memory Log Dump can be disabled using::

  $ ceph config set mds mds_extraordinary_events_dump_interval 0

Filesystems Become Inaccessible After an Upgrade
================================================

.. note::
   You can avoid ``operation not permitted`` errors by running this procedure
   before an upgrade. As of May 2023, it seems that ``operation not permitted``
   errors of the kind discussed here occur after upgrades after Nautilus
   (inclusive).

IF

you have CephFS file systems that have data and metadata pools that were
created by a ``ceph fs new`` command (meaning that they were not created
with the defaults)

OR

you have an existing CephFS file system and are upgrading to a new post-Nautilus
major version of Ceph

THEN

in order for the documented ``ceph fs authorize...`` commands to function as
documented (and to avoid 'operation not permitted' errors when doing file I/O
or similar security-related problems for all users except the ``client.admin``
user), you must first run:

.. prompt:: bash $

   ceph osd pool application set <your metadata pool name> cephfs metadata <your ceph fs filesystem name>

and

.. prompt:: bash $

   ceph osd pool application set <your data pool name> cephfs data <your ceph fs filesystem name>

Otherwise, when the OSDs receive a request to read or write data (not the
directory info, but file data) they will not know which Ceph file system name
to look up. This is true also of pool names, because the 'defaults' themselves
changed in the major releases, from::

   data pool=fsname
   metadata pool=fsname_metadata

to::

   data pool=fsname.data and
   metadata pool=fsname.meta

Any setup that used ``client.admin`` for all mounts did not run into this
problem, because the admin key gave blanket permissions.

A temporary fix involves changing mount requests to the 'client.admin' user and
its associated key. A less drastic but half-fix is to change the osd cap for
your user to just ``caps osd = "allow rw"``  and delete ``tag cephfs
data=....``

Disabling the Volumes Plugin
============================
In certain scenarios, the Volumes plugin may need to be disabled to prevent
compromise for rest of the Ceph cluster. For details see:
:ref:`disabling-volumes-plugin`

Reporting Issues
================

If you have identified a specific issue, please report it with as much
information as possible. Especially important information:

* Ceph versions installed on client and server
* Whether you are using the kernel or fuse client
* If you are using the kernel client, what kernel version?
* How many clients are in play, doing what kind of workload?
* If a system is 'stuck', is that affecting all clients or just one?
* Any ceph health messages
* Any backtraces in the ceph logs from crashes

If you are satisfied that you have found a bug, please file it on `the bug
tracker`. For more general queries, please write to the `ceph-users mailing
list`.

.. _the bug tracker: http://tracker.ceph.com
.. _ceph-users mailing list:  http://lists.ceph.com/listinfo.cgi/ceph-users-ceph.com/
