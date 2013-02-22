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

Ceph will print each version of the placement group map and their status.  For
example, a tiny Ceph cluster consisting of one monitor, one metadata server and
two OSDs may print the following:: 

   health HEALTH_OK
   monmap e1: 1 mons at {a=192.168.0.1:6789/0}, election epoch 0, quorum 0 a
   osdmap e13: 2 osds: 2 up, 2 in
    placement groupmap v9713: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail
   mdsmap e4: 1/1/1 up {0=a=up:active}

	2012-08-01 11:33:53.831268 mon.0 [INF] placement groupmap v9712: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail
	2012-08-01 11:35:31.904650 mon.0 [INF] placement groupmap v9713: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail
	2012-08-01 11:35:53.903189 mon.0 [INF] placement groupmap v9714: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail
	2012-08-01 11:37:31.865809 mon.0 [INF] placement groupmap v9715: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail


Checking a Cluster's Status
===========================

To check a cluster's status, execute the following:: 

	ceph status
	
Or:: 

	ceph -s

In interactive mode, type ``status`` and press **Enter**. ::

	ceph> status

Ceph will print the cluster status. For example, a tiny Ceph  cluster consisting
of one monitor, one metadata server and  two OSDs may print the following::

   health HEALTH_OK
   monmap e1: 1 mons at {a=192.168.0.1:6789/0}, election epoch 0, quorum 0 a
   osdmap e13: 2 osds: 2 up, 2 in
    placement groupmap v9754: 384 placement groups: 384 active+clean; 8730 bytes data, 22948 MB used, 264 GB / 302 GB avail
   mdsmap e4: 1/1/1 up {0=a=up:active}


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
	              "addr": "127.0.0.1:6789\/0"},
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
By default, Ceph sockets reside under ``/var/run/ceph``. To access a socket, 
use the following command:: 

	ceph --admin-daemon /var/run/ceph/{socket-name}

To view the available admin socket commands, execute the following command:: 

	ceph --admin-daemon /var/run/ceph/{socket-name} help

The admin socket command enables you to show and set your configuration at
runtime. See `Viewing a Configuration at Runtime`_ for details.

.. _Viewing a Configuration at Runtime: ../../configuration/ceph-conf#ceph-runtime-config

