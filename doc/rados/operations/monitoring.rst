======================
 Monitoring a Cluster
======================

Once you have a running cluster, you may use the ``ceph`` tool to monitor your
cluster. Monitoring a cluster typically involves checking OSD status, monitor 
status, placement group status and metadata server status.

Interactive Mode
================

To run the ``ceph`` tool in interactive mode, type ``ceph`` at the command line
with no arguments.  For example:: 

	ceph
	ceph> health
	ceph> status
	ceph> quorum_status
	ceph> mon_status
	

Checking Cluster Health
=======================

After you start your cluster, and before you start reading and/or
writing data, check your cluster's health first. You can check on the 
health of your Ceph cluster with the following::

	ceph health

If you specified non-default locations for your configuration or keyring,
you may specify their locations::

   ceph -c /path/to/conf -k /path/to/keyring health

Upon starting the Ceph cluster, you will likely encounter a health
warning such as ``HEALTH_WARN XXX num placement groups stale``. Wait a few moments and check
it again. When your cluster is ready, ``ceph health`` should return a message
such as ``HEALTH_OK``. At that point, it is okay to begin using the cluster.

Watching a Cluster
==================

To watch the cluster's ongoing events, open a new terminal. Then, enter:: 

	ceph -w

Ceph will print each event.  For example, a tiny Ceph cluster consisting of 
one monitor, and two OSDs may print the following:: 

    cluster b370a29d-9287-4ca3-ab57-3d824f65e339
     health HEALTH_OK
     monmap e1: 1 mons at {ceph1=10.0.0.8:3300/0}, election epoch 2, quorum 0 ceph1
     osdmap e63: 2 osds: 2 up, 2 in
      pgmap v41338: 952 pgs, 20 pools, 17130 MB data, 2199 objects
            115 GB used, 167 GB / 297 GB avail
                 952 active+clean

    2014-06-02 15:45:21.655871 osd.0 [INF] 17.71 deep-scrub ok
    2014-06-02 15:45:47.880608 osd.1 [INF] 1.0 scrub ok
    2014-06-02 15:45:48.865375 osd.1 [INF] 1.3 scrub ok
    2014-06-02 15:45:50.866479 osd.1 [INF] 1.4 scrub ok
    2014-06-02 15:45:01.345821 mon.0 [INF] pgmap v41339: 952 pgs: 952 active+clean; 17130 MB data, 115 GB used, 167 GB / 297 GB avail
    2014-06-02 15:45:05.718640 mon.0 [INF] pgmap v41340: 952 pgs: 1 active+clean+scrubbing+deep, 951 active+clean; 17130 MB data, 115 GB used, 167 GB / 297 GB avail
    2014-06-02 15:45:53.997726 osd.1 [INF] 1.5 scrub ok
    2014-06-02 15:45:06.734270 mon.0 [INF] pgmap v41341: 952 pgs: 1 active+clean+scrubbing+deep, 951 active+clean; 17130 MB data, 115 GB used, 167 GB / 297 GB avail
    2014-06-02 15:45:15.722456 mon.0 [INF] pgmap v41342: 952 pgs: 952 active+clean; 17130 MB data, 115 GB used, 167 GB / 297 GB avail
    2014-06-02 15:46:06.836430 osd.0 [INF] 17.75 deep-scrub ok
    2014-06-02 15:45:55.720929 mon.0 [INF] pgmap v41343: 952 pgs: 1 active+clean+scrubbing+deep, 951 active+clean; 17130 MB data, 115 GB used, 167 GB / 297 GB avail


The output provides:

- Cluster ID
- Cluster health status
- The monitor map epoch and the status of the monitor quorum
- The OSD map epoch and the status of OSDs 
- The placement group map version
- The number of placement groups and pools
- The *notional* amount of data stored and the number of objects stored; and,
- The total amount of data stored.

.. topic:: How Ceph Calculates Data Usage

   The ``used`` value reflects the *actual* amount of raw storage used. The 
   ``xxx GB / xxx GB`` value means the amount available (the lesser number)
   of the overall storage capacity of the cluster. The notional number reflects 
   the size of the stored data before it is replicated, cloned or snapshotted.
   Therefore, the amount of data actually stored typically exceeds the notional
   amount stored, because Ceph creates replicas of the data and may also use 
   storage capacity for cloning and snapshotting.


Checking a Cluster's Usage Stats
================================

To check a cluster's data usage and data distribution among pools, you can
use the ``df`` option. It is similar to Linux ``df``. Execute 
the following::

	ceph df

The **GLOBAL** section of the output provides an overview of the amount of 
storage your cluster uses for your data.

- **SIZE:** The overall storage capacity of the cluster.
- **AVAIL:** The amount of free space available in the cluster.
- **RAW USED:** The amount of raw storage used.
- **% RAW USED:** The percentage of raw storage used. Use this number in 
  conjunction with the ``full ratio`` and ``near full ratio`` to ensure that 
  you are not reaching your cluster's capacity. See `Storage Capacity`_ for 
  additional details.

The **POOLS** section of the output provides a list of pools and the notional 
usage of each pool. The output from this section **DOES NOT** reflect replicas,
clones or snapshots. For example, if you store an object with 1MB of data, the 
notional usage will be 1MB, but the actual usage may be 2MB or more depending 
on the number of replicas, clones and snapshots.

- **NAME:** The name of the pool.
- **ID:** The pool ID.
- **USED:** The notional amount of data stored in kilobytes, unless the number 
  appends **M** for megabytes or **G** for gigabytes.
- **%USED:** The notional percentage of storage used per pool.
- **Objects:** The notional number of objects stored per pool.

.. note:: The numbers in the **POOLS** section are notional. They are not 
   inclusive of the number of replicas, shapshots or clones. As a result, 
   the sum of the **USED** and **%USED** amounts will not add up to the 
   **RAW USED** and **%RAW USED** amounts in the **GLOBAL** section of the 
   output.


Checking a Cluster's Status
===========================

To check a cluster's status, execute the following:: 

	ceph status
	
Or:: 

	ceph -s

In interactive mode, type ``status`` and press **Enter**. ::

	ceph> status

Ceph will print the cluster status. For example, a tiny Ceph  cluster consisting
of one monitor, and two OSDs may print the following::

    cluster b370a29d-9287-4ca3-ab57-3d824f65e339
     health HEALTH_OK
     monmap e1: 1 mons at {ceph1=10.0.0.8:3300/0}, election epoch 2, quorum 0 ceph1
     osdmap e63: 2 osds: 2 up, 2 in
      pgmap v41332: 952 pgs, 20 pools, 17130 MB data, 2199 objects
            115 GB used, 167 GB / 297 GB avail
                   1 active+clean+scrubbing+deep
                 951 active+clean


Checking OSD Status
===================

You can check OSDs to ensure they are ``up`` and ``in`` by executing:: 

	ceph osd stat
	
Or:: 

	ceph osd dump
	
You can also check view OSDs according to their position in the CRUSH map. :: 

	ceph osd tree

Ceph will print out a CRUSH tree with a host, its OSDs, whether they are up
and their weight. ::  

	# id	weight	type name	up/down	reweight
	-1	3	pool default
	-3	3		rack mainrack
	-2	3			host osd-host
	0	1				osd.0	up	1	
	1	1				osd.1	up	1	
	2	1				osd.2	up	1

For a detailed discussion, refer to `Monitoring OSDs and Placement Groups`_.

Checking Monitor Status
=======================

If your cluster has multiple monitors (likely), you should check the monitor
quorum status after you start the cluster before reading and/or writing data. A
quorum must be present when multiple monitors are running. You should also check
monitor status periodically to ensure that they are running.

To see display the monitor map, execute the following::

	ceph mon stat
	
Or:: 

	ceph mon dump
	
To check the quorum status for the monitor cluster, execute the following:: 
	
	ceph quorum_status

Ceph will return the quorum status. For example, a Ceph  cluster consisting of
three monitors may return the following:

.. code-block:: javascript

	{ "election_epoch": 10,
	  "quorum": [
	        0,
	        1,
	        2],
	  "monmap": { "epoch": 1,
	      "fsid": "444b489c-4f16-4b75-83f0-cb8097468898",
	      "modified": "2011-12-12 13:28:27.505520",
	      "created": "2011-12-12 13:28:27.505520",
	      "mons": [
	            { "rank": 0,
	              "name": "a",
	              "addr": "127.0.0.1:3300\/0"},
	            { "rank": 1,
	              "name": "b",
	              "addr": "127.0.0.1:6790\/0"},
	            { "rank": 2,
	              "name": "c",
	              "addr": "127.0.0.1:6791\/0"}
	           ]
	    }
	}

Checking MDS Status
===================

Metadata servers provide metadata services for  Ceph FS. Metadata servers have
two sets of states: ``up | down`` and ``active | inactive``. To ensure your
metadata servers are ``up`` and ``active``,  execute the following:: 

	ceph mds stat
	
To display details of the metadata cluster, execute the following:: 

	ceph mds dump


Checking Placement Group States
===============================

Placement groups map objects to OSDs. When you monitor your
placement groups,  you will want them to be ``active`` and ``clean``. 
For a detailed discussion, refer to `Monitoring OSDs and Placement Groups`_.

.. _Monitoring OSDs and Placement Groups: ../monitoring-osd-pg


Using the Admin Socket
======================

The Ceph admin socket allows you to query a daemon via a socket interface. 
By default, Ceph sockets reside under ``/var/run/ceph``. To access a daemon
via the admin socket, login to the host running the daemon and use the 
following command:: 

	ceph daemon {daemon-name}
	ceph daemon {path-to-socket-file}

For example, the following are equivalent::

    ceph daemon osd.0 foo
    ceph daemon /var/run/ceph/ceph-osd.0.asok foo

To view the available admin socket commands, execute the following command:: 

	ceph daemon {daemon-name} help

The admin socket command enables you to show and set your configuration at
runtime. See `Viewing a Configuration at Runtime`_ for details.

Additionally, you can set configuration values at runtime directly (i.e., the
admin socket bypasses the monitor, unlike ``ceph tell {daemon-type}.{id}
injectargs``, which relies on the monitor but doesn't require you to login
directly to the host in question ).

.. _Viewing a Configuration at Runtime: ../../configuration/ceph-conf#ceph-runtime-config
.. _Storage Capacity: ../../configuration/mon-config-ref#storage-capacity
