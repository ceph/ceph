=================
 Troubleshooting
=================

Slow/stuck operations
=====================

If you are experiencing apparent hung operations, the first task is to identify
where the problem is occurring: in the client, the MDS, or the network connecting
them. Start by looking to see if either side has stuck operations
(:ref:`slow_requests`, below), and narrow it down from there.

We can get hints about what's going on by dumping the MDS cache ::

  ceph daemon mds.<name> dump cache /tmp/dump.txt

.. note:: The file `dump.txt` is on the machine executing the MDS and for systemd
	  controlled MDS services, this is in a tmpfs in the MDS container.
	  Use `nsenter(1)` to locate `dump.txt` or specify another system-wide path.

If high logging levels are set on the MDS, that will almost certainly hold the
information we need to diagnose and solve the issue.

Stuck during recovery
=====================

Stuck in up:replay
------------------

If your MDS is stuck in ``up:replay`` then it is likely that the journal is
very long. Did you see ``MDS_HEALTH_TRIM`` cluster warnings saying the MDS is
behind on trimming its journal? If the journal has grown very large, it can
take hours to read the journal. There is no working around this but there
are things you can do to speed things along:

Reduce MDS debugging to 0. Even at the default settings, the MDS logs some
messages to memory for dumping if a fatal error is encountered. You can avoid
this:

.. code:: bash

   ceph config set mds debug_mds 0
   ceph config set mds debug_ms 0
   ceph config set mds debug_monc 0

Note if the MDS fails then there will be virtually no information to determine
why. If you can calculate when ``up:replay`` will complete, you should restore
these configs just prior to entering the next state:

.. code:: bash

   ceph config rm mds debug_mds
   ceph config rm mds debug_ms
   ceph config rm mds debug_monc

Once you've got replay moving along faster, you can calculate when the MDS will
complete. This is done by examining the journal replay status:

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
``journal_write_pos``. The write position will not change during replay. Track
the progression of the read position to compute the expected time to complete.


Avoiding recovery roadblocks
----------------------------

When trying to urgently restore your file system during an outage, here are some
things to do:

* **Deny all reconnect to clients.** This effectively blocklists all existing
  CephFS sessions so all mounts will hang or become unavailable.

.. code:: bash

   ceph config set mds mds_deny_all_reconnect true

  Remember to undo this after the MDS becomes active.

.. note:: This does not prevent new sessions from connecting. For that, see the ``refuse_client_session`` file system setting.

* **Extend the MDS heartbeat grace period**. This avoids replacing an MDS that appears
  "stuck" doing some operation. Sometimes recovery of an MDS may involve an
  operation that may take longer than expected (from the programmer's
  perspective). This is more likely when recovery is already taking a longer than
  normal amount of time to complete (indicated by your reading this document).
  Avoid unnecessary replacement loops by extending the heartbeat graceperiod:

.. code:: bash

   ceph config set mds mds_heartbeat_grace 3600

.. note:: This has the effect of having the MDS continue to send beacons to the monitors
          even when its internal "heartbeat" mechanism has not been reset (beat) in one
          hour. The previous mechanism for achieving this was via the
          `mds_beacon_grace` monitor setting.

* **Disable open file table prefetch.** Normally, the MDS will prefetch
  directory contents during recovery to heat up its cache. During long
  recovery, the cache is probably already hot **and large**. So this behavior
  can be undesirable. Disable using:

.. code:: bash

   ceph config set mds mds_oft_prefetch_dirfrags false

* **Turn off clients.** Clients reconnecting to the newly ``up:active`` MDS may
  cause new load on the file system when it's just getting back on its feet.
  There will likely be some general maintenance to do before workloads should be
  resumed. For example, expediting journal trim may be advisable if the recovery
  took a long time because replay was reading a overly large journal.

  You can do this manually or use the new file system tunable:

.. code:: bash

   ceph fs set <fs_name> refuse_client_session true

  That prevents any clients from establishing new sessions with the MDS.



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

Unfortunately the kernel client does not support the admin socket, but it has
similar (if limited) interfaces if your kernel has debugfs enabled. There
will be a folder in ``sys/kernel/debug/ceph/``, and that folder (whose name will
look something like ``28f7427e-5558-4ffd-ae1a-51ec3042759a.client25386880``)
will contain a variety of files that output interesting output when you ``cat``
them. These files are described below; the most interesting when debugging
slow requests are probably the ``mdsc`` and ``osdc`` files.

* bdi: BDI info about the Ceph system (blocks dirtied, written, etc)
* caps: counts of file "caps" structures in-memory and used
* client_options: dumps the options provided to the CephFS mount
* dentry_lru: Dumps the CephFS dentries currently in-memory
* mdsc: Dumps current requests to the MDS
* mdsmap: Dumps the current MDSMap epoch and MDSes
* mds_sessions: Dumps the current sessions to MDSes
* monc: Dumps the current maps from the monitor, and any "subscriptions" held
* monmap: Dumps the current monitor map epoch and monitors
* osdc: Dumps the current ops in-flight to OSDs (ie, file data IO)
* osdmap: Dumps the current OSDMap epoch, pools, and OSDs

If the data pool is in a NEARFULL condition, then the kernel cephfs client
will switch to doing writes synchronously, which is quite slow.

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
