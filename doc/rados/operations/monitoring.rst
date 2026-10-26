======================
 Monitoring a Cluster
======================

After you have a running cluster, you can use the ``ceph`` tool to monitor your
cluster. Monitoring a cluster typically involves checking OSD status, monitor
status, placement group status, and metadata server status.

Using the Command Line
======================

Interactive Mode
----------------

To run the ``ceph`` tool in interactive mode, type ``ceph`` at the command line
with no arguments. For example:

.. prompt:: bash #

    ceph

.. prompt:: ceph>
    :prompts: ceph>

    health
    status
    quorum_status
    mon stat

Non-default Paths
-----------------

If you specified non-default locations for your configuration or keyring when
you install the cluster, you may specify their locations to the ``ceph`` tool
by running the following command:

.. prompt:: bash #

   ceph -c /path/to/conf -k /path/to/keyring health

Checking a Cluster's Status
===========================

After you start your cluster, and before you start reading and/or writing data,
you should check your cluster's status.

To check a cluster's status, run the following command:

.. prompt:: bash #

   ceph status

Alternatively, you can run the following command:

.. prompt:: bash #

   ceph -s

In interactive mode, this operation is performed by typing ``status`` and
pressing **Enter**:

.. prompt:: ceph>
    :prompts: ceph>

    status

Ceph will print the cluster status. For example, a small cluster with three
Monitors, two Managers, three OSDs, and one CephFS file system (one active MDS
and two standby MDS daemons) might print the following:

::

  cluster:
    id:     477e46f1-ae41-4e43-9c8f-72c918ab0a20
    health: HEALTH_OK

  services:
    mon: 3 daemons, quorum a,b,c (age 2h)
    mgr: x(active, since 2h), standbys: y
    mds: 1/1 daemons up, 2 standby
    osd: 3 osds: 3 up (since 2h), 3 in (since 2h)

  data:
    volumes: 1/1 healthy
    pools:   3 pools, 49 pgs
    objects: 21 objects, 2.2 KiB
    usage:   546 GiB used, 384 GiB / 931 GiB avail
    pgs:     49 active+clean

In Tentacle and later releases, the ``mon`` line also names the leader:
for example, ``mon: 3 daemons, quorum a,b,c (age 2h) [leader: a]``.


How Ceph Calculates Data Usage
------------------------------

The ``usage`` value reflects the *actual* amount of raw storage used. The ``xxx
GiB / xxx GiB`` value means the amount available (the lesser number) of the
overall storage capacity of the cluster. The ``objects`` line shows how much
data clients have stored, counted once, before replication or erasure coding.
It includes data that only snapshots still reference. The ``usage`` value is
larger because Ceph stores several object replicas or shards of that data.


Watching a Cluster
==================

Each daemon in the Ceph cluster maintains a log of events, and the Ceph cluster
itself maintains a *cluster log* that records high-level events about the
entire Ceph cluster. These events are logged on the Monitor hosts, by default
to ``/var/log/ceph/ceph.log`` or, in a cluster that cephadm manages, to
journald (see :ref:`cephadm-logs`), and they can be monitored via the command
line.

To follow the cluster log, run the following command:

.. prompt:: bash #

   ceph -w

Ceph will print the status of the system, followed by each log message as it is
added. For example:

::

  cluster:
    id:     477e46f1-ae41-4e43-9c8f-72c918ab0a20
    health: HEALTH_OK

  services:
    mon: 3 daemons, quorum a,b,c (age 2h)
    mgr: x(active, since 2h), standbys: y
    mds: 1/1 daemons up, 2 standby
    osd: 3 osds: 3 up (since 2h), 3 in (since 2h)

  data:
    volumes: 1/1 healthy
    pools:   3 pools, 49 pgs
    objects: 21 objects, 2.2 KiB
    usage:   546 GiB used, 384 GiB / 931 GiB avail
    pgs:     49 active+clean


  2026-10-22T08:15:11.329298+0000 mon.a [INF] osd.0 [v2:172.21.9.34:6802/20527,v1:172.21.9.34:6803/20527] boot
  2026-10-22T08:15:14.258143+0000 mon.a [INF] Activating manager daemon x
  2026-10-22T08:15:15.446025+0000 mon.a [INF] Manager daemon x is now available

Instead of printing log lines as they are added, you might want to print only
the most recent lines. Run ``ceph log last [n]`` to see the most recent ``n``
lines from the cluster log.

Monitoring Health Checks
========================

Ceph continuously runs various :ref:`health-checks`. When
a health check fails, this failure is reflected in the output of ``ceph status`` and
``ceph health``. The cluster log receives messages that
indicate when a check has failed and when the cluster has recovered.

For example, when an OSD goes down, the ``health`` section of the status
output is updated as follows:

::

    health: HEALTH_WARN
            1 osds down
            Degraded data redundancy: 21/63 objects degraded (33.333%), 16 pgs degraded

At the same time, cluster log messages are emitted to record the failure of the
health checks. In the cluster log file and in the output of ``ceph log last``,
they look like this:

::

    2026-10-22T10:08:58.265945+0000 mon.a (mon.0) 91 : cluster [WRN] Health check failed: 1 osds down (OSD_DOWN)
    2026-10-22T10:09:01.302624+0000 mon.a (mon.0) 94 : cluster [WRN] Health check failed: Degraded data redundancy: 21/63 objects degraded (33.333%), 16 pgs degraded (PG_DEGRADED)

When the OSD comes back online, the cluster log records the cluster's return
to a healthy state:

::

    2026-10-22T10:11:11.526841+0000 mon.a (mon.0) 109 : cluster [WRN] Health check update: Degraded data redundancy: 2 pgs degraded, 2 pgs undersized (PG_DEGRADED)
    2026-10-22T10:11:13.535493+0000 mon.a (mon.0) 110 : cluster [INF] Health check cleared: PG_DEGRADED (was: Degraded data redundancy: 2 pgs degraded, 2 pgs undersized)
    2026-10-22T10:11:13.535577+0000 mon.a (mon.0) 111 : cluster [INF] Cluster is now healthy

Network Performance Checks
--------------------------

Ceph OSDs send heartbeat ping messages to each other in order to monitor daemon
availability and network performance. If a single delayed response is detected,
this might indicate nothing more than a busy OSD. But if multiple delays
between distinct pairs of OSDs are detected, this might indicate a failed
network switch, a NIC failure, or a layer 1 failure.

By default, a heartbeat time that exceeds 1 second (1000 milliseconds) raises a
health check (a ``HEALTH_WARN``). For example:

::

    HEALTH_WARN Slow OSD heartbeats on back (longest 1118.001ms)

In the output of the ``ceph health detail`` command, you can see which OSDs are
experiencing delays and how long the delays are. For the back network and the
front network, ``ceph health detail`` lists at most ten pairs of OSDs each,
slowest first, followed by a ``Truncated long network list`` line if there
are more. Here is an example of the output you can
expect from the ``ceph health detail`` command::

    [WRN] OSD_SLOW_PING_TIME_BACK: Slow OSD heartbeats on back (longest 1118.001ms)
        Slow OSD heartbeats on back from osd.0 [dc1,rack1] to osd.1 [dc1,rack1] 1118.001 msec possibly improving
        Slow OSD heartbeats on back from osd.0 [dc1,rack1] to osd.2 [dc1,rack2] 1030.123 msec
        Slow OSD heartbeats on back from osd.2 [dc1,rack2] to osd.1 [dc1,rack1] 1015.321 msec
        Slow OSD heartbeats on back from osd.1 [dc1,rack1] to osd.0 [dc1,rack1] 1010.456 msec

To see more detail and to collect a complete dump of network performance
information, use the ``dump_osd_network`` command. This command is usually sent
to a Ceph Manager Daemon, but it can be used to collect information about a
specific OSD's interactions by sending it to that OSD. The default threshold
for a slow heartbeat is 1 second (1000 milliseconds), but this can be
overridden by providing a number of milliseconds as an argument.

To show all network performance data with a specified threshold of 0, send the
following command to the mgr. In a cluster that cephadm manages, run it inside
the Manager's container (see :ref:`rados-monitoring-using-admin-socket`):

.. prompt:: bash #

   ceph daemon /var/run/ceph/ceph-mgr.x.asok dump_osd_network 0

::

    {
        "threshold": 0,
        "entries": [
            {
                "last update": "Wed Sep  4 17:04:49 2019",
                "stale": false,
                "from osd": 2,
                "to osd": 0,
                "interface": "front",
                "average": {
                    "1min": 1.023,
                    "5min": 0.860,
                    "15min": 0.883
                },
                "min": {
                    "1min": 0.818,
                    "5min": 0.607,
                    "15min": 0.607
                },
                "max": {
                    "1min": 1.164,
                    "5min": 1.173,
                    "15min": 1.544
                },
                "last": 0.924
            },
            {
                "last update": "Wed Sep  4 17:04:49 2019",
                "stale": false,
                "from osd": 2,
                "to osd": 0,
                "interface": "back",
                "average": {
                    "1min": 0.968,
                    "5min": 0.897,
                    "15min": 0.830
                },
                "min": {
                    "1min": 0.860,
                    "5min": 0.563,
                    "15min": 0.502
                },
                "max": {
                    "1min": 1.171,
                    "5min": 1.216,
                    "15min": 1.456
                },
                "last": 0.845
            },
            {
                "last update": "Wed Sep  4 17:04:48 2019",
                "stale": false,
                "from osd": 0,
                "to osd": 1,
                "interface": "front",
                "average": {
                    "1min": 0.965,
                    "5min": 0.811,
                    "15min": 0.850
                },
                "min": {
                    "1min": 0.650,
                    "5min": 0.488,
                    "15min": 0.466
                },
                "max": {
                    "1min": 1.252,
                    "5min": 1.252,
                    "15min": 1.362
                },
            "last": 0.791
        },
        ...


.. _rados-monitoring-muting-health-checks:


Muting Health Checks
--------------------

Health checks can be muted so that they have no effect on the overall
reported status of the cluster. For example, if the cluster has raised a
single health check and then you mute that health check, then the cluster will report a status of ``HEALTH_OK``.
To mute a specific health check, use the health check code that corresponds to that health check (see :ref:`health-checks`), and
run the following command:

.. prompt:: bash #

   ceph health mute <code>

For example, to mute an ``OSD_DOWN`` health check, run the following command:

.. prompt:: bash #

   ceph health mute OSD_DOWN

Mutes are reported as part of the short and long form of the ``ceph health`` command's output.
For example, in the above scenario, the cluster would report:

.. prompt:: bash #

   ceph health

::

   HEALTH_OK (muted: OSD_DOWN)

.. prompt:: bash #

   ceph health detail

::

   HEALTH_OK (muted: OSD_DOWN)
   (MUTED) [WRN] OSD_DOWN: 1 osds down
       osd.1 (root=default,host=node1) is down

A mute can be removed by running the following command:

.. prompt:: bash #

   ceph health unmute <code>

For example:

.. prompt:: bash #

   ceph health unmute OSD_DOWN

A "health mute" can have a TTL (**T**\ime **T**\o **L**\ive)
associated with it: this means that the mute will automatically expire
after a specified period of time. The TTL is specified as an optional
duration argument, as seen in the following examples:

.. prompt:: bash #

   ceph health mute OSD_DOWN 4h    # mute for 4 hours
   ceph health mute MON_DOWN 15m   # mute for 15 minutes

Normally, if a muted health check is resolved (for example, if the OSD that raised the ``OSD_DOWN`` health check
in the example above has come back up), the mute goes away. If the health check comes
back later, it will be reported in the usual way.

It is possible to make a health mute "sticky": this means that the mute will remain even if the
health check clears. For example, to make a health mute "sticky", you might run the following command:

.. prompt:: bash #

   ceph health mute OSD_DOWN 1h --sticky   # ignore any/all down OSDs for next hour

Most health mutes disappear if the unhealthy condition that triggered the health check gets worse.
For example, suppose that there is one OSD down and the health check is muted. In that case, if
one or more additional OSDs go down, then the health mute disappears. This behavior occurs in any health check with a threshold value.


.. _rados-monitoring-pool-usage:

Checking a Cluster's Usage Stats
================================

To check a cluster's data usage and data distribution among pools, use the
``df`` command. This option is similar to Linux's ``df`` command. Run the
following command:

.. prompt:: bash #

   ceph df

The output of ``ceph df`` resembles the following::

   --- RAW STORAGE ---
   CLASS     SIZE    AVAIL     USED  RAW USED  %RAW USED
   hdd    5.4 PiB  1.2 PiB  4.3 PiB   4.3 PiB      78.58
   ssd     22 TiB   19 TiB  2.7 TiB   2.7 TiB      12.36
   TOTAL  5.5 PiB  1.2 PiB  4.3 PiB   4.3 PiB      78.32

   --- POOLS ---
   POOL                         ID   PGS   STORED  OBJECTS     USED  %USED  MAX AVAIL
   .mgr                         11     1  558 MiB      141  1.6 GiB      0    5.8 TiB
   cephfs_meta                  13  1024  166 GiB   14.59M  499 GiB   2.74    5.8 TiB
   cephfs_data                  14  1024      0 B    1.17G      0 B      0    5.8 TiB
   cephfsECvol                  19  2048  2.8 PiB    1.81G  3.5 PiB  83.79    561 TiB
   .nfs                         20    32  9.7 KiB       61  118 KiB      0    5.8 TiB
   testbench                    71    32   12 GiB    3.14k   37 GiB      0    234 TiB
   default.rgw.buckets.data     76  2048  482 TiB  132.09M  643 TiB  47.85    526 TiB
   .rgw.root                    97     1  1.4 KiB        4   48 KiB      0    5.8 TiB
   default.rgw.log              98   256  3.6 KiB      209  408 KiB      0    5.8 TiB
   default.rgw.control          99     1      0 B        8      0 B      0    5.8 TiB
   default.rgw.meta            100   128  3.8 KiB       20  194 KiB      0    5.8 TiB
   default.rgw.buckets.index   101   256  4.2 MiB       33   13 MiB      0    5.8 TiB
   default.rgw.buckets.non-ec  102   128  5.6 MiB       13   17 MiB      0    5.8 TiB
   kubedata                    104   256   63 GiB   17.65k  188 GiB   0.03    234 TiB
   kubemeta                    105   256  241 MiB      166  724 MiB      0    5.8 TiB

- **CLASS:** Statistics for each CRUSH device class present, for example, ``ssd`` and ``hdd``.
- **SIZE:** The amount of storage capacity managed by the cluster.
- **AVAIL:** The amount of free space available in the cluster.
- **USED:** The raw capacity in use (SIZE minus AVAIL), the same as RAW USED.
  It includes the space that BlueStore's database occupies; when the database
  is on a separate device, the whole size of that device counts as used.
- **RAW USED:** The amount of raw storage consumed by user data, internal
  overhead, and reserved capacity.
- **%RAW USED:** The percentage of raw storage used. Watch this number in
  conjunction with ``backfillfull_ratio`` and ``nearfull_ratio`` to be forewarned when
  your cluster approaches the fullness thresholds. See :ref:`storage-capacity`.

Additional information may be displayed by invoking as below:

.. prompt:: bash #

   ceph df detail

The output now resembles the below example::

   --- RAW STORAGE ---
   CLASS     SIZE    AVAIL     USED  RAW USED  %RAW USED
   hdd    5.4 PiB  1.2 PiB  4.3 PiB   4.3 PiB      78.58
   ssd     22 TiB   19 TiB  2.7 TiB   2.7 TiB      12.36
   TOTAL  5.5 PiB  1.2 PiB  4.3 PiB   4.3 PiB      78.32

   --- POOLS ---
   POOL                         ID   PGS   STORED   (DATA)   (OMAP)  OBJECTS     USED   (DATA)   (OMAP)  %USED  MAX AVAIL  QUOTA OBJECTS  QUOTA BYTES  DIRTY  USED COMPR  UNDER COMPR
   .mgr                         11     1  558 MiB  558 MiB      0 B      141  1.6 GiB  1.6 GiB      0 B      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   cephfs_meta                  13  1024  166 GiB  206 MiB  166 GiB   14.59M  499 GiB  618 MiB  498 GiB   2.74    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   cephfs_data                  14  1024      0 B      0 B      0 B    1.17G      0 B      0 B      0 B      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   cephfsECvol                  19  2048  2.8 PiB  2.8 PiB   17 KiB    1.81G  3.5 PiB  3.5 PiB   21 KiB  83.79    561 TiB            N/A          N/A    N/A         0 B          0 B
   .nfs                         20    32  9.7 KiB  2.2 KiB  7.5 KiB       61  118 KiB   96 KiB   22 KiB      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   testbench                    71    32   12 GiB   12 GiB  2.3 KiB    3.14k   37 GiB   37 GiB  6.9 KiB      0    234 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.buckets.data     76  2048  482 TiB  482 TiB      0 B  132.09M  643 TiB  643 TiB      0 B  47.85    526 TiB            N/A          N/A    N/A     312 MiB      623 MiB
   .rgw.root                    97     1  1.4 KiB  1.4 KiB      0 B        4   48 KiB   48 KiB      0 B      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.log              98   256  3.6 KiB  3.6 KiB      0 B      209  408 KiB  408 KiB      0 B      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.control          99     1      0 B      0 B      0 B        8      0 B      0 B      0 B      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.meta            100   128  3.8 KiB  3.2 KiB    671 B       20  194 KiB  192 KiB  2.0 KiB      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.buckets.index   101   256  4.2 MiB      0 B  4.2 MiB       33   13 MiB      0 B   13 MiB      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   default.rgw.buckets.non-ec  102   128  5.6 MiB      0 B  5.6 MiB       13   17 MiB      0 B   17 MiB      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B
   kubedata                    104   256   63 GiB   63 GiB      0 B   17.65k  188 GiB  188 GiB      0 B   0.03    234 TiB            N/A       20 TiB    N/A         0 B          0 B
   kubemeta                    105   256  241 MiB  241 MiB  278 KiB      166  723 MiB  722 MiB  833 KiB      0    5.8 TiB            N/A          N/A    N/A         0 B          0 B



.. rubric:: POOLS

The POOLS section of the output lists each pool. STORED, OBJECTS, and MAX AVAIL
are notional: they count data once, before replication or erasure coding. USED
and USED COMPR are not: they count raw capacity on all OSDs, including the
object replicas or shards. For example, if you store 1 MiB of data in a pool
that keeps three replicas, STORED shows 1 MiB and USED shows about 3 MiB.

- **ID:** The numeric ID of the pool.
- **PGS:** The number of placement groups in the pool.
- **STORED:** The amount of data that clients have stored in the pool,
  counted once regardless of replication or erasure coding.

  - **(DATA):** Usage for RBD (RADOS Block Device), CephFS file data, and RGW
    (RADOS Gateway) object data.
  - **(OMAP):** Key-value pairs. Used primarily by CephFS and RGW (RADOS
    Gateway) for metadata storage.

- **OBJECTS:** The number of objects stored in the pool, counted once
  regardless of replication or erasure coding. Snapshot clones count as
  objects.
- **USED:** The space allocated for a pool over all OSDs. This includes space
  for replication, space for allocation granularity, and space for the overhead
  associated with erasure-coding. Compression savings and object-content gaps
  are also taken into account. USED includes the pool's omap data, which
  BlueStore keeps in its RocksDB database (the (OMAP) column of
  ``ceph df detail``), but not BlueStore's other internal metadata.

  - **(DATA):** Object usage for RBD (RADOS Block Device), CephFS file data,
    and RGW (RADOS Gateway) object data.
  - **(OMAP):** Object key-value pairs. Used primarily by CephFS and RGW (RADOS
    Gateway) for metadata storage.

- **%USED:** The notional percentage of storage used per pool.
- **MAX AVAIL:** An estimate of the notional amount of data that can be written
  to this pool.
- **QUOTA OBJECTS:** The pool's object quota (the ``max_objects`` value set
  with ``ceph osd pool set-quota``), or ``N/A`` if none is set.
- **QUOTA BYTES:** The pool's byte quota (the ``max_bytes`` value set with
  ``ceph osd pool set-quota``), or ``N/A`` if none is set.
- **DIRTY:** The number of objects in the cache pool that have been written to
  the cache pool but have not yet been flushed to the base pool. This field is
  available only when cache tiering is in use.
- **USED COMPR:** The amount of space allocated for compressed data. This
  includes compressed data in addition to all of the space required for
  replication, allocation granularity, and erasure-coding overhead.
- **UNDER COMPR:** The amount of data that has passed through compression
  (summed over all replicas) and that is worth storing in a compressed form.


.. note:: The per-pool USED values do not add up to the USED value in the RAW
   STORAGE section, because the RAW STORAGE section also counts capacity that
   belongs to no pool, such as BlueStore's internal metadata and DB devices.

.. note:: The MAX AVAIL value is a complicated function of the replication or
   the kind of erasure coding used, the CRUSH rule that maps storage to
   devices, the utilization of those devices, and the cluster's full ratio
   (the ``full_ratio`` value in the output of ``ceph osd dump``; see
   :ref:`storage-capacity`).


Checking OSD Status
===================

To check if OSDs are ``up`` and ``in``, run the
following command:

.. prompt:: bash #

  ceph osd stat

Alternatively, you can run the following command:

.. prompt:: bash #

  ceph osd dump

To view OSDs according to their position in the CRUSH map, run the following
command:

.. prompt:: bash #

   ceph osd tree

The output of ``ceph osd tree`` shows each host, its OSDs, whether each OSD is
``up``, and the weight of each OSD. For example:

::

   ID  CLASS  WEIGHT   TYPE NAME              STATUS  REWEIGHT  PRI-AFF
   -1         3.00000  root default
   -3         3.00000      rack mainrack
   -2         3.00000          host osd-host
    0    ssd  1.00000              osd.0          up   1.00000  1.00000
    1    ssd  1.00000              osd.1          up   1.00000  1.00000
    2    ssd  1.00000              osd.2          up   1.00000  1.00000

See :ref:`rados_operations_monitoring_osd_pg`.

Checking Monitor Status
=======================

If your cluster has multiple monitors, then you need to perform certain
"monitor status" checks. After starting the cluster and before reading or
writing data, you should check quorum status. A quorum must be present when
multiple monitors are running to ensure proper functioning of your Ceph
cluster. Check monitor status regularly in order to ensure that all of the
monitors are running.

.. _display-mon-map:

To display the monitor map, run the following command:

.. prompt:: bash #

   ceph mon stat

Alternatively, you can run the following command:

.. prompt:: bash #

   ceph mon dump

To check the quorum status for the monitor cluster, run the following command:

.. prompt:: bash #

   ceph quorum_status

Ceph returns the quorum status as JSON. ``quorum_names`` lists the Monitors in
the quorum, ``quorum_leader_name`` names the leader, ``quorum_age`` gives the
number of seconds since the quorum formed, and ``monmap`` holds the current
monitor map. For example, a Ceph cluster that consists of three Monitors might
return the following (abridged):

.. code-block:: none

    {
        "election_epoch": 10,
        "quorum": [0, 1, 2],
        "quorum_names": ["a", "b", "c"],
        "quorum_leader_name": "a",
        "quorum_age": 3600,
        ...
        "monmap": {
            "epoch": 1,
            ...
            "mons": [
                {
                    "rank": 0,
                    "name": "a",
                    "public_addrs": {
                        "addrvec": [
                            {"type": "v2", "addr": "10.0.0.1:3300", "nonce": 0},
                            {"type": "v1", "addr": "10.0.0.1:6789", "nonce": 0}
                        ]
                    },
                    ...
                },
                ...
            ]
        }
    }

Checking MDS Status
===================

Metadata servers provide metadata services for CephFS. Each MDS daemon is in a
state such as ``up:active``, ``up:standby``, ``up:standby-replay`` or
``up:replay`` (see :doc:`/cephfs/mds-states`). To check the state of your
metadata servers, run the following command:

.. prompt:: bash #

   ceph mds stat

For example, ``cephfs:1 {0=a=up:active} 2 up:standby`` means that the file
system ``cephfs`` has one rank, served by the daemon ``a``, and that two
daemons are on standby.

To display details of the metadata servers, run the following command:

.. prompt:: bash #

   ceph fs dump


Checking Placement Group States
===============================

Placement groups (PGs) map objects to OSDs. PGs are monitored in order to
ensure that they are ``active`` and ``clean``.
See :ref:`rados_operations_monitoring_osd_pg`.

.. _rados-monitoring-using-admin-socket:

Using the Admin Socket
======================

The Ceph admin socket allows you to query a daemon via a socket interface. To
use it, log in to the host that is running the daemon. In a cluster that was
installed from packages, the sockets are in ``/var/run/ceph``. In a cluster
that cephadm manages, they are in ``/var/run/ceph/<fsid>`` on the host, and
inside the daemon's container they appear under ``/var/run/ceph``. To run
``ceph daemon`` in the container, first enter it with a command of the
following form (see :ref:`cephadm-admin-socket`):

.. prompt:: bash #

   cephadm enter --name {daemon-name}

Run a command of one of the following forms:

.. prompt:: bash #

   ceph daemon {daemon-name} {command}
   ceph daemon {path-to-socket-file} {command}

For example, the following commands are equivalent to each other:

.. prompt:: bash #

   ceph daemon osd.0 version
   ceph daemon /var/run/ceph/ceph-osd.0.asok version

There are two methods of running admin socket commands: (1)
using ``ceph daemon`` as described above, which bypasses
the monitor and assumes a direct login to the daemon's host,
and (2) using the ``ceph tell {daemon-type}.{id}`` command, which sends the
command over the network to the daemon (the ``ceph`` tool needs the Monitors to
find it) and does not require access to the daemon's host.

Use the ``raise`` command to send a signal to a daemon, as if by running ``kill -X {daemon.pid}``.
When run via ``ceph tell`` it allows signaling a daemon without access to its host:

.. prompt:: bash #

   ceph daemon {daemon-name} raise HUP
   ceph tell {daemon-type}.{id} raise -9

To view the available admin-socket commands, run the following command:

.. prompt:: bash #

   ceph daemon {daemon-name} help

Admin-socket commands enable you to view and set your configuration at runtime.
For more on viewing your configuration,
see :ref:`configuring_ceph_runtime_view`.

Messenger Status
=================

Ceph daemons and librados clients support an admin socket command
``messenger dump`` that surfaces a snapshot of runtime information
about connections, sockets, bound addresses, and kernel TCP stats (via
tcp(7) TCP_INFO).

.. note:: The queried messenger needs to lock the connection data
  structures for the time it takes to create the snapshot. This lock's
  duration is in the order of tens of milliseconds. This might
  interfere with normal operation. Use the ``dumpcontents`` argument
  to limit data structures dumped.

Examples
---------

When a command is issued without specifying a messenger to dump, the
list of available messengers is returned:

.. prompt:: bash #

   ceph tell osd.0 messenger dump

.. code-block:: json

 {
    "messengers": [
        "client",
        "cluster",
        "hb_back_client",
        "hb_back_server",
        "hb_front_client",
        "hb_front_server",
        "ms_objecter",
        "temp_mon_client"
    ]
  }

The ``client`` messenger serves the public network and the ``cluster``
messenger serves the cluster network (see
:doc:`/rados/configuration/network-config-ref`). Messengers with the ``hb_``
prefix are part of the heartbeat system.

List all current connections on the client messenger:

.. code-block:: bash

        ceph tell osd.0 messenger dump client \
            | jq -r '.messenger.connections[].async_connection |
                [.conn_id, .socket_fd, .worker_id,
                    if .status.connected then "connected" else "disconnected" end,
                    .state,
                    "\(.peer.type).\(.peer.entity_name.id).\(.peer.id)",
                    .protocol.v2.con_mode, .protocol.v2.crypto.rx, .protocol.v2.compression.rx] |
                @tsv'

::

  249     102     0       connected       STATE_CONNECTION_ESTABLISHED    client.admin.6407       crc     PLAIN   UNCOMPRESSED
  242     99      1       connected       STATE_CONNECTION_ESTABLISHED    client.rgw.8000.4473    crc     PLAIN   UNCOMPRESSED
  248     89      1       connected       STATE_CONNECTION_ESTABLISHED    mgr..-1 secure  AES-128-GCM     UNCOMPRESSED
  32      101     2       connected       STATE_CONNECTION_ESTABLISHED    client.rgw.8000.4483    crc     PLAIN   UNCOMPRESSED
  3       86      2       connected       STATE_CONNECTION_ESTABLISHED    mon..-1 secure  AES-128-GCM     UNCOMPRESSED
  244     102     0       connected       STATE_CONNECTION_ESTABLISHED    client.admin.6383       crc     PLAIN   UNCOMPRESSED


Print active connections and their TCP round trip time and retransmission counters:

.. code-block:: bash

        ceph tell osd.0 messenger dump client --tcp-info \
            | jq -r '.messenger.connections[].async_connection |
                select(.status.connected) |
                select(.peer.type != "client") |
                [.conn_id, .socket_fd, .worker_id,
                    "\(.peer.type).\(.peer.global_id)",
                    .tcp_info.tcpi_rtt_us, .tcp_info.tcpi_rttvar_us, .tcp_info.tcpi_total_retrans] |
                @tsv'

::

    248     89      1       mgr.0   863     1677    0
    3       86      2       mon.0   230     278     0

.. _data_availability_score:

Tracking Data Availability Score of a Cluster
=============================================

In Tentacle and later releases, the Monitors can track the data
availability of each pool. The feature is a technology preview and is off by
default: until you turn it on (see below),
``ceph osd pool availability-status`` returns an error. To check the data
availability score of each pool, run the following command:

.. prompt:: bash #

   ceph osd pool availability-status

Example output:

::

   POOL         UPTIME  DOWNTIME  NUMFAILURES  MTBF  MTTR  SCORE     AVAILABLE
   rbd             2m     21s           1        2m   21s  0.888889     1
   .mgr             86s     0s          0        0s   0s        1       1
   cephfs.a.meta    77s     0s          0        0s   0s        1       1
   cephfs.a.data    76s     0s          0        0s   0s        1       1

The time values above are rounded for readability. To see the exact second
values, use the option ``--format`` with ``json`` or ``json-pretty`` value.

A pool is considered ``unavailable`` while at least one of its PGs has been
inactive or stale for longer than :confval:`mon_pg_stuck_threshold` (60 seconds
by default), or while at least one of its PGs has unfound objects. Otherwise
the pool is considered ``available``. Depending on the
current and previous state of the pool we update ``uptime`` and
``downtime`` values:

================ =============== =============== =================
 Previous State   Current State   Uptime Update   Downtime Update
================ =============== =============== =================
 Available        Available       +diff time      no update
 Available        Unavailable     +diff time      no update
 Unavailable      Available       +diff time      no update
 Unavailable      Unavailable     no update       +diff time
================ =============== =============== =================

From the updated ``uptime`` and ``downtime`` values, we calculate the Mean
Time Between Failures (MTBF, the uptime divided by the number of failures) and
the Mean Time To Recover (MTTR, the downtime divided by the number of
failures) for each pool. The availability score is MTBF divided by the sum of
MTBF and MTTR, which is the share of the tracked time during which the pool
was available. A pool that has not failed has a score of 1.

The score is updated every one second. Transient changes to pools that
occur and are reverted between successive updates will not be captured.
It is possible to configure this interval with a command of the following
form:

.. prompt:: bash #

   ceph config set mon pool_availability_update_interval 2

This will set the update interval to two seconds. The score is recalculated
only when the Monitors commit an update, and they normally commit at most once
per ``paxos_propose_interval`` (1 second by default), so a smaller value
usually has no further effect.


This feature is off by default. To turn the feature on, the 
``enable_availability_tracking`` config option can be set to ``true``.

.. prompt:: bash #

   ceph config set mon enable_availability_tracking true

While the feature is turned off, the last calculated score will be preserved. The
score will again start updating once the feature is turned on again.

It's also possible to clear the data availability score for a specific
pool if needed with a command of the following form:

.. prompt:: bash #

   ceph osd pool clear-availability-status <pool-name>

.. note:: Clearing a score is not allowed if the feature itself is disabled.
