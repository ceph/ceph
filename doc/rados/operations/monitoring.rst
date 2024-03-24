======================
 Monitoring a Cluster
======================

After you have a running cluster, you can use the ``ceph`` tool to monitor your
cluster. Monitoring a cluster typically involves checking OSD status, monitor
status, placement group status, and metadata server status.

Using the command line
======================

Interactive mode
----------------

To run the ``ceph`` tool in interactive mode, type ``ceph`` at the command line
with no arguments. For example:

.. prompt:: bash $

    ceph

.. prompt:: ceph>
    :prompts: ceph>

    health
    status
    quorum_status
    mon stat

Non-default paths
-----------------

If you specified non-default locations for your configuration or keyring when
you install the cluster, you may specify their locations to the ``ceph`` tool
by running the following command:

.. prompt:: bash $

   ceph -c /path/to/conf -k /path/to/keyring health

Checking a Cluster's Status
===========================

After you start your cluster, and before you start reading and/or writing data,
you should check your cluster's status.

To check a cluster's status, run the following command:

.. prompt:: bash $

   ceph status

Alternatively, you can run the following command:

.. prompt:: bash $

   ceph -s

In interactive mode, this operation is performed by typing ``status`` and
pressing **Enter**:

.. prompt:: ceph>
    :prompts: ceph>

    status

Ceph will print the cluster status. For example, a tiny Ceph "demonstration
cluster" that is running one instance of each service (monitor, manager, and
OSD) might print the following:

::

  cluster:
    id:     477e46f1-ae41-4e43-9c8f-72c918ab0a20
    health: HEALTH_OK
   
  services:
    mon: 3 daemons, quorum a,b,c
    mgr: x(active)
    mds: cephfs_a-1/1/1 up  {0=a=up:active}, 2 up:standby
    osd: 3 osds: 3 up, 3 in
  
  data:
    pools:   2 pools, 16 pgs
    objects: 21 objects, 2.19K
    usage:   546 GB used, 384 GB / 931 GB avail
    pgs:     16 active+clean


How Ceph Calculates Data Usage
------------------------------

The ``usage`` value reflects the *actual* amount of raw storage used. The ``xxx
GB / xxx GB`` value means the amount available (the lesser number) of the
overall storage capacity of the cluster. The notional number reflects the size
of the stored data before it is replicated, cloned or snapshotted.  Therefore,
the amount of data actually stored typically exceeds the notional amount
stored, because Ceph creates replicas of the data and may also use storage
capacity for cloning and snapshotting.


Watching a Cluster
==================

Each daemon in the Ceph cluster maintains a log of events, and the Ceph cluster
itself maintains a *cluster log* that records high-level events about the
entire Ceph cluster.  These events are logged to disk on monitor servers (in
the default location ``/var/log/ceph/ceph.log``), and they can be monitored via
the command line.

To follow the cluster log, run the following command:

.. prompt:: bash $

   ceph -w

Ceph will print the status of the system, followed by each log message as it is
added. For example:

:: 

  cluster:
    id:     477e46f1-ae41-4e43-9c8f-72c918ab0a20
    health: HEALTH_OK
  
  services:
    mon: 3 daemons, quorum a,b,c
    mgr: x(active)
    mds: cephfs_a-1/1/1 up  {0=a=up:active}, 2 up:standby
    osd: 3 osds: 3 up, 3 in
  
  data:
    pools:   2 pools, 16 pgs
    objects: 21 objects, 2.19K
    usage:   546 GB used, 384 GB / 931 GB avail
    pgs:     16 active+clean
  
  
  2017-07-24 08:15:11.329298 mon.a mon.0 172.21.9.34:6789/0 23 : cluster [INF] osd.0 172.21.9.34:6806/20527 boot
  2017-07-24 08:15:14.258143 mon.a mon.0 172.21.9.34:6789/0 39 : cluster [INF] Activating manager daemon x
  2017-07-24 08:15:15.446025 mon.a mon.0 172.21.9.34:6789/0 47 : cluster [INF] Manager daemon x is now available

Instead of printing log lines as they are added, you might want to print only
the most recent lines. Run ``ceph log last [n]`` to see the most recent ``n``
lines from the cluster log.

Monitoring Health Checks
========================

Ceph continuously runs various *health checks*. When
a health check fails, this failure is reflected in the output of ``ceph status`` and
``ceph health``. The cluster log receives messages that
indicate when a check has failed and when the cluster has recovered.

For example, when an OSD goes down, the ``health`` section of the status
output is updated as follows:

::

    health: HEALTH_WARN
            1 osds down
            Degraded data redundancy: 21/63 objects degraded (33.333%), 16 pgs unclean, 16 pgs degraded

At the same time, cluster log messages are emitted to record the failure of the 
health checks:

::

    2017-07-25 10:08:58.265945 mon.a mon.0 172.21.9.34:6789/0 91 : cluster [WRN] Health check failed: 1 osds down (OSD_DOWN)
    2017-07-25 10:09:01.302624 mon.a mon.0 172.21.9.34:6789/0 94 : cluster [WRN] Health check failed: Degraded data redundancy: 21/63 objects degraded (33.333%), 16 pgs unclean, 16 pgs degraded (PG_DEGRADED)

When the OSD comes back online, the cluster log records the cluster's return
to a healthy state:

::

    2017-07-25 10:11:11.526841 mon.a mon.0 172.21.9.34:6789/0 109 : cluster [WRN] Health check update: Degraded data redundancy: 2 pgs unclean, 2 pgs degraded, 2 pgs undersized (PG_DEGRADED)
    2017-07-25 10:11:13.535493 mon.a mon.0 172.21.9.34:6789/0 110 : cluster [INF] Health check cleared: PG_DEGRADED (was: Degraded data redundancy: 2 pgs unclean, 2 pgs degraded, 2 pgs undersized)
    2017-07-25 10:11:13.535577 mon.a mon.0 172.21.9.34:6789/0 111 : cluster [INF] Cluster is now healthy

Network Performance Checks
--------------------------

Ceph OSDs send heartbeat ping messages to each other in order to monitor daemon
availability and network performance. If a single delayed response is detected,
this might indicate nothing more than a busy OSD. But if multiple delays
between distinct pairs of OSDs are detected, this might indicate a failed
network switch, a NIC failure, or a layer 1 failure.

By default, a heartbeat time that exceeds 1 second (1000 milliseconds) raises a
health check (a ``HEALTH_WARN``. For example:

::

    HEALTH_WARN Slow OSD heartbeats on back (longest 1118.001ms)

In the output of the ``ceph health detail`` command, you can see which OSDs are
experiencing delays and how long the delays are. The output of ``ceph health
detail`` is limited to ten lines. Here is an example of the output you can
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
following command to the mgr:

.. prompt:: bash $

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



Muting Health Checks
--------------------

Health checks can be muted so that they have no effect on the overall
reported status of the cluster. For example, if the cluster has raised a
single health check and then you mute that health check, then the cluster will report a status of ``HEALTH_OK``.
To mute a specific health check, use the health check code that corresponds to that health check (see :ref:`health-checks`), and 
run the following command:

.. prompt:: bash $

   ceph health mute <code>

For example, to mute an ``OSD_DOWN`` health check, run the following command:

.. prompt:: bash $

   ceph health mute OSD_DOWN

Mutes are reported as part of the short and long form of the ``ceph health`` command's output.
For example, in the above scenario, the cluster would report:

.. prompt:: bash $

   ceph health

::

   HEALTH_OK (muted: OSD_DOWN)

.. prompt:: bash $

   ceph health detail

::

   HEALTH_OK (muted: OSD_DOWN)
   (MUTED) OSD_DOWN 1 osds down
       osd.1 is down

A mute can be removed by running the following command:

.. prompt:: bash $

   ceph health unmute <code>

For example:

.. prompt:: bash $

   ceph health unmute OSD_DOWN

A "health mute" can have a TTL (**T**\ime **T**\o **L**\ive)
associated with it: this means that the mute will automatically expire
after a specified period of time. The TTL is specified as an optional
duration argument, as seen in the following examples:

.. prompt:: bash $

   ceph health mute OSD_DOWN 4h    # mute for 4 hours
   ceph health mute MON_DOWN 15m   # mute for 15 minutes

Normally, if a muted health check is resolved (for example, if the OSD that raised the ``OSD_DOWN`` health check 
in the example above has come back up), the mute goes away. If the health check comes
back later, it will be reported in the usual way.

It is possible to make a health mute "sticky": this means that the mute will remain even if the
health check clears. For example, to make a health mute "sticky", you might run the following command:

.. prompt:: bash $

   ceph health mute OSD_DOWN 1h --sticky   # ignore any/all down OSDs for next hour

Most health mutes disappear if the unhealthy condition that triggered the health check gets worse.
For example, suppose that there is one OSD down and the health check is muted. In that case, if
one or more additional OSDs go down, then the health mute disappears. This behavior occurs in any health check with a threshold value.


Checking a Cluster's Usage Stats
================================

To check a cluster's data usage and data distribution among pools, use the
``df`` command. This option is similar to Linux's ``df`` command. Run the
following command:

.. prompt:: bash $

   ceph df

The output of ``ceph df`` resembles the following::

   CLASS     SIZE    AVAIL     USED  RAW USED  %RAW USED
   ssd    202 GiB  200 GiB  2.0 GiB   2.0 GiB       1.00
   TOTAL  202 GiB  200 GiB  2.0 GiB   2.0 GiB       1.00

   --- POOLS ---
   POOL                   ID  PGS   STORED   (DATA)   (OMAP)   OBJECTS     USED  (DATA)   (OMAP)   %USED  MAX AVAIL  QUOTA OBJECTS  QUOTA BYTES  DIRTY  USED COMPR  UNDER COMPR
   device_health_metrics   1    1  242 KiB   15 KiB  227 KiB         4  251 KiB  24 KiB  227 KiB       0    297 GiB            N/A          N/A      4         0 B          0 B
   cephfs.a.meta           2   32  6.8 KiB  6.8 KiB      0 B        22   96 KiB  96 KiB      0 B       0    297 GiB            N/A          N/A     22         0 B          0 B
   cephfs.a.data           3   32      0 B      0 B      0 B         0      0 B     0 B      0 B       0     99 GiB            N/A          N/A      0         0 B          0 B
   test                    4   32   22 MiB   22 MiB   50 KiB       248   19 MiB  19 MiB   50 KiB       0    297 GiB            N/A          N/A    248         0 B          0 B
   
- **CLASS:** For example, "ssd" or "hdd".
- **SIZE:** The amount of storage capacity managed by the cluster.
- **AVAIL:** The amount of free space available in the cluster.
- **USED:** The amount of raw storage consumed by user data (excluding
  BlueStore's database).
- **RAW USED:** The amount of raw storage consumed by user data, internal
  overhead, and reserved capacity.
- **%RAW USED:** The percentage of raw storage used. Watch this number in
  conjunction with ``full ratio`` and ``near full ratio`` to be forewarned when
  your cluster approaches the fullness thresholds. See `Storage Capacity`_.


**POOLS:**

The POOLS section of the output provides a list of pools and the *notional*
usage of each pool. This section of the output **DOES NOT** reflect replicas,
clones, or snapshots. For example, if you store an object with 1MB of data,
then the notional usage will be 1MB, but the actual usage might be 2MB or more
depending on the number of replicas, clones, and snapshots.

- **ID:** The number of the specific node within the pool.
- **STORED:** The actual amount of data that the user has stored in a pool.
  This is similar to the USED column in earlier versions of Ceph, but the
  calculations (for BlueStore!) are more precise (in that gaps are properly
  handled).

  - **(DATA):** Usage for RBD (RADOS Block Device), CephFS file data, and RGW
    (RADOS Gateway) object data.
  - **(OMAP):** Key-value pairs. Used primarily by CephFS and RGW (RADOS
    Gateway) for metadata storage.

- **OBJECTS:** The notional number of objects stored per pool (that is, the
  number of objects other than replicas, clones, or snapshots). 
- **USED:** The space allocated for a pool over all OSDs. This includes space
  for replication, space for allocation granularity, and space for the overhead
  associated with erasure-coding. Compression savings and object-content gaps
  are also taken into account. However, BlueStore's database is not included in
  the amount reported under USED.

  - **(DATA):** Object usage for RBD (RADOS Block Device), CephFS file data,
    and RGW (RADOS Gateway) object data.
  - **(OMAP):** Object key-value pairs. Used primarily by CephFS and RGW (RADOS
    Gateway) for metadata storage.

- **%USED:** The notional percentage of storage used per pool.
- **MAX AVAIL:** An estimate of the notional amount of data that can be written
  to this pool.
- **QUOTA OBJECTS:** The number of quota objects.
- **QUOTA BYTES:** The number of bytes in the quota objects.
- **DIRTY:** The number of objects in the cache pool that have been written to
  the cache pool but have not yet been flushed to the base pool. This field is
  available only when cache tiering is in use.
- **USED COMPR:** The amount of space allocated for compressed data. This
  includes compressed data in addition to all of the space required for
  replication, allocation granularity, and erasure- coding overhead.
- **UNDER COMPR:** The amount of data that has passed through compression
  (summed over all replicas) and that is worth storing in a compressed form.


.. note:: The numbers in the POOLS section are notional. They do not include
   the number of replicas, clones, or snapshots. As a result, the sum of the
   USED and %USED amounts in the POOLS section of the output will not be equal
   to the sum of the USED and %USED amounts in the RAW section of the output.

.. note:: The MAX AVAIL value is a complicated function of the replication or
   the kind of erasure coding used, the CRUSH rule that maps storage to
   devices, the utilization of those devices, and the configured
   ``mon_osd_full_ratio`` setting.


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

To print out a CRUSH tree that displays a host, its OSDs, whether the OSDs are
``up``, and the weight of the OSDs, run the following command:

.. code-block:: bash

   #ID CLASS WEIGHT  TYPE NAME             STATUS REWEIGHT PRI-AFF
    -1       3.00000 pool default
    -3       3.00000 rack mainrack
    -2       3.00000 host osd-host
     0   ssd 1.00000         osd.0             up  1.00000 1.00000
     1   ssd 1.00000         osd.1             up  1.00000 1.00000
     2   ssd 1.00000         osd.2             up  1.00000 1.00000

See `Monitoring OSDs and Placement Groups`_.

Checking Monitor Status
=======================

If your cluster has multiple monitors, then you need to perform certain
"monitor status" checks.  After starting the cluster and before reading or
writing data, you should check quorum status. A quorum must be present when
multiple monitors are running to ensure proper functioning of your Ceph
cluster. Check monitor status regularly in order to ensure that all of the
monitors are running.

.. _display-mon-map:

To display the monitor map, run the following command:

.. prompt:: bash $

   ceph mon stat

Alternatively, you can run the following command:

.. prompt:: bash $

   ceph mon dump

To check the quorum status for the monitor cluster, run the following command:

.. prompt:: bash $

   ceph quorum_status

Ceph returns the quorum status. For example, a Ceph cluster that consists of
three monitors might return the following:

.. code-block:: javascript

    { "election_epoch": 10,
      "quorum": [
            0,
            1,
            2],
      "quorum_names": [
        "a",
        "b",
        "c"],
      "quorum_leader_name": "a",
      "monmap": { "epoch": 1,
          "fsid": "444b489c-4f16-4b75-83f0-cb8097468898",
          "modified": "2011-12-12 13:28:27.505520",
          "created": "2011-12-12 13:28:27.505520",
          "features": {"persistent": [
                "kraken",
                "luminous",
                "mimic"],
        "optional": []
          },
          "mons": [
                { "rank": 0,
                  "name": "a",
                  "addr": "127.0.0.1:6789/0",
              "public_addr": "127.0.0.1:6789/0"},
                { "rank": 1,
                  "name": "b",
                  "addr": "127.0.0.1:6790/0",
              "public_addr": "127.0.0.1:6790/0"},
                { "rank": 2,
                  "name": "c",
                  "addr": "127.0.0.1:6791/0",
              "public_addr": "127.0.0.1:6791/0"}
               ]
      }
    }

Checking MDS Status
===================

Metadata servers provide metadata services for CephFS. Metadata servers have
two sets of states: ``up | down`` and ``active | inactive``. To check if your
metadata servers are ``up`` and ``active``, run the following command:

.. prompt:: bash $

   ceph mds stat

To display details of the metadata servers, run the following command:

.. prompt:: bash $

   ceph fs dump


Checking Placement Group States
===============================

Placement groups (PGs) map objects to OSDs. PGs are monitored in order to
ensure that they are ``active`` and ``clean``.  See `Monitoring OSDs and
Placement Groups`_.

.. _Monitoring OSDs and Placement Groups: ../monitoring-osd-pg

.. _rados-monitoring-using-admin-socket:

Using the Admin Socket
======================

The Ceph admin socket allows you to query a daemon via a socket interface.  By
default, Ceph sockets reside under ``/var/run/ceph``. To access a daemon via
the admin socket, log in to the host that is running the daemon and run one of
the two following commands:

.. prompt:: bash $

   ceph daemon {daemon-name}
   ceph daemon {path-to-socket-file}

For example, the following commands are equivalent to each other:

.. prompt:: bash $

   ceph daemon osd.0 foo
   ceph daemon /var/run/ceph/ceph-osd.0.asok foo

To view the available admin-socket commands, run the following command:

.. prompt:: bash $

   ceph daemon {daemon-name} help

Admin-socket commands enable you to view and set your configuration at runtime.
For more on viewing your configuration, see `Viewing a Configuration at
Runtime`_. There are two methods of setting configuration value at runtime: (1)
using the admin socket, which bypasses the monitor and requires a direct login
to the host in question, and (2) using the ``ceph tell {daemon-type}.{id}
config set`` command, which relies on the monitor and does not require a direct
login.

.. _Viewing a Configuration at Runtime: ../../configuration/ceph-conf#viewing-a-configuration-at-runtime
.. _Storage Capacity: ../../configuration/mon-config-ref#storage-capacity
