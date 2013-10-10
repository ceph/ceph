=============================
 Storage Cluster Quick Start
=============================

If you haven't completed your `Preflight Checklist`_, do that first. This
**Quick Start** sets up a :term:`Ceph Storage Cluster` using ``ceph-deploy``
on your admin node. Create a three Ceph Node cluster so you can 
explore Ceph functionality. 

.. ditaa:: 
           /------------------\         /----------------\
           |    Admin Node    |         |   ceph–node1   |
           |                  +-------->+ cCCC           |
           |    ceph–deploy   |         | mon.ceph–node1 |
           \---------+--------/         \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node2   |
                     +----------------->+ cCCC           |
                     |                  |     osd.0      |
                     |                  \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node3   |
                     +----------------->| cCCC           |
                                        |     osd.1      |
                                        \----------------/

For best results, create a directory on your admin node node for maintaining the
configuration that ``ceph-deploy`` generates for your cluster. ::

	mkdir my-cluster
	cd my-cluster

.. tip:: The ``ceph-deploy`` utility will output files to the 
   current directory. Ensure you are in this directory when executing
   ``ceph-deploy``.

As a first exercise, create a Ceph Storage Cluster with one Ceph Monitor and two
Ceph OSD Daemons. Once the cluster reaches a ``active + clean`` state, expand it 
by adding a third Ceph OSD Daemon, a Metadata Server and two more Ceph Monitors.

.. important:: Do not call ``ceph-deploy`` with ``sudo`` or run it as ``root`` 
   if you are logged in as a different user, because it will not issue ``sudo`` 
   commands needed on the remote host.

Create a Cluster
================

If at any point you run into trouble and you want to start over, execute
the following:: 

	ceph-deploy purgedata {ceph-node} [{ceph-node}]
	ceph-deploy forgetkeys


On your admin node, perform the following steps using ``ceph-deploy``.

#. Create the cluster. :: 

	ceph-deploy new {ceph-node}
	ceph-deploy new ceph-node1

   Check the output of ``ceph-deploy`` with ``ls`` and ``cat`` in the current
   directory. You should see a Ceph configuration file, a keyring, and a log 
   file for the new cluster.  See `ceph-deploy new -h`_ for additional details.

#. Install Ceph. :: 

	ceph-deploy install {ceph-node}[{ceph-node} ...]
	ceph-deploy install ceph-node1 ceph-node2 ceph-node3


#. Add a Ceph Monitor. :: 

	ceph-deploy mon create {ceph-node}
	ceph-deploy mon create ceph-node1
	
#. Gather keys. :: 

	ceph-deploy gatherkeys {ceph-node}
	ceph-deploy gatherkeys ceph-node1

   Once you have gathered keys, your local directory should have the following 
   keyrings:

   - ``{cluster-name}.client.admin.keyring``
   - ``{cluster-name}.bootstrap-osd.keyring``
   - ``{cluster-name}.bootstrap-mds.keyring`` 
   

#. Add two OSDs. For fast setup, this quick start uses a directory rather
   than an entire disk per Ceph OSD Daemon. See `ceph-deploy osd`_ for 
   details on using separate disks/partitions for OSDs and journals. 
   Login to the Ceph Nodes and create a directory for 
   the Ceph OSD Daemon. ::
   
	ssh ceph-node2
	sudo mkdir /tmp/osd0
	exit
	
	ssh ceph-node3
	sudo mkdir /tmp/osd1
	exit 	

   Then, from your admin node, use ``ceph-deploy`` to prepare the OSDs. ::

	ceph-deploy osd prepare {ceph-node}:/path/to/directory
	ceph-deploy osd prepare ceph-node2:/tmp/osd0 ceph-node3:/tmp/osd1

   Finally, activate the OSDs. :: 

	ceph-deploy osd activate {ceph-node}:/path/to/directory
	ceph-deploy osd activate ceph-node2:/tmp/osd0 ceph-node3:/tmp/osd1


#. Use ``ceph-deploy`` to copy the configuration file and admin key to
   your admin node and your Ceph Nodes so that you can use the ``ceph`` 
   CLI without having to specify the monitor address and 
   ``ceph.client.admin.keyring`` each time you execute a command. :: 
   
	ceph-deploy admin {ceph-node}
	ceph-deploy admin admin-node ceph-node1 ceph-node2 ceph-node3

   **Note:** Since you are using ``ceph-deploy`` to talk to the
   local host, your host must be reachable by its hostname 
   (e.g., you can modify ``/etc/hosts`` if necessary). Ensure that
   you have the correct permissions for the ``ceph.client.admin.keyring``. 

#. Check your cluster's health. ::

	ceph health

   Your cluster should return an ``active + clean`` state when it 
   has finished peering.


Operating Your Cluster
======================

Deploying a Ceph cluster with ``ceph-deploy`` automatically starts the cluster. 
To operate the cluster daemons with Debian/Ubuntu distributions, see 
`Running Ceph with Upstart`_.  To operate the cluster daemons with CentOS,
Red Hat, Fedora, and SLES distributions, see `Running Ceph with sysvinit`_.

To learn more about peering and cluster health, see `Monitoring a Cluster`_.
To learn more about Ceph OSD Daemon and placement group health, see 
`Monitoring OSDs and PGs`_.
 
Once you deploy a Ceph cluster, you can try out some of the administration
functionality, the ``rados`` object store command line, and then proceed to
Quick Start guides for Ceph Block Device, Ceph Filesystem, and the Ceph Object
Gateway.


Expanding Your Cluster
======================

Once you have a basic cluster up and running, the next step is to expand
cluster. Add a Ceph OSD Daemon and a Ceph Metadata Server to ``ceph-node1``.
Then add a Ceph Monitor to ``ceph-node2`` and  ``ceph-node3`` to establish a
quorum of Ceph Monitors.

.. ditaa:: 
           /------------------\         /----------------\
           |    ceph–deploy   |         |   ceph–node1   |
           |    Admin Node    |         | cCCC           |
           |                  +-------->+ mon.ceph–node1 |
           |                  |         |     osd.2      |
           |                  |         | mds.ceph–node1 |
           \---------+--------/         \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node2   |
                     |                  | cCCC           |
                     +----------------->+                |
                     |                  |     osd.0      |
                     |                  | mon.ceph–node2 |
                     |                  \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node3   |
                     |                  | cCCC           |
                     +----------------->+                |
                                        |     osd.1      |
                                        | mon.ceph–node3 |
                                        \----------------/

Adding an OSD
-------------

Since you are running a 3-node cluster for demonstration purposes, add the OSD
to the monitor node. ::

	ssh ceph-node1
	sudo mkdir /tmp/osd2
	exit

Then, from your ``ceph-deploy`` node, prepare the OSD. ::

	ceph-deploy osd prepare {ceph-node}:/path/to/directory
	ceph-deploy osd prepare ceph-node1:/tmp/osd2

Finally, activate the OSDs. ::

	ceph-deploy osd activate {ceph-node}:/path/to/directory
	ceph-deploy osd activate ceph-node1:/tmp/osd2


Once you have added your new OSD, Ceph will begin rebalancing the cluster by
migrating placement groups to your new OSD. You can observe this process with
the ``ceph`` CLI. ::

	ceph -w

You should see the placement group states change from ``active+clean`` to active
with some degraded objects, and finally ``active+clean`` when migration
completes. (Control-c to exit.)


Add a Metadata Server
---------------------

To use CephFS, you need at least one metadata server. Execute the following to
create a metadata server::

	ceph-deploy mds create {ceph-node}
	ceph-deploy mds create ceph-node1


.. note:: Currently Ceph runs in production with one metadata server only. You 
   may use more, but there is currently no commercial support for a cluster 
   with multiple metadata servers.


Adding Monitors
---------------

A Ceph Storage Cluster requires at least one Ceph Monitor to run. For high
availability, Ceph Storage Clusters typically run multiple Ceph
Monitors so that the failure of a single Ceph Monitor will not bring down the
Ceph Storage Cluster. Ceph uses the Paxos algorithm, which requires a majority
of monitors (i.e., 1, 2:3, 3:4, 3:5, 4:6, etc.) to form a quorum.

Add two Ceph Monitors to your cluster. ::

	ceph-deploy mon create {ceph-node}
	ceph-deploy mon create ceph-node2 ceph-node3

Once you have added your new Ceph Monitors, Ceph will begin synchronizing
the monitors and form a quorum. You can check the quorum status by executing
the following:: 

	ceph quorum_status



Storing/Retrieving Object Data
==============================

To store object data in the Ceph Storage Cluster, a Ceph client must: 

#. Set an object name
#. Specify a `pool`_

The Ceph Client retrieves the latest cluster map and the CRUSH algorithm
calculates how to map the object to a `placement group`_, and then calculates
how to assign the placement group to a Ceph OSD Daemon dynamically. To find the
object location, all you need is the object name and the pool name. For
example:: 

	ceph osd map {poolname} {object-name}

.. topic:: Exercise: Locate an Object

	As an exercise, lets create an object. Specify an object name, a path to
	a test file containing some object data and a pool name using the 
	``rados put`` command on the command line. For example::
   
		rados put {object-name} {file-path} --pool=data   	
		rados put test-object-1 testfile.txt --pool=data
   
	To verify that the Ceph Storage Cluster stored the object, execute 
	the following::
   
		rados -p data ls
   
	Now, identify the object location::	

		ceph osd map {pool-name} {object-name}
		ceph osd map data test-object-1
   
	Ceph should output the object's location. For example:: 
   
		osdmap e537 pool 'data' (0) object 'test-object-1' -> pg 0.d1743484 (0.4) -> up [1,0] acting [1,0]
   
	To remove the test object, simply delete it using the ``rados rm`` 
	command.	For example:: 
   
		rados rm test-object-1 --pool=data
   
As the cluster evolves, the object location may change dynamically. One benefit
of Ceph's dynamic rebalancing is that Ceph relieves you from having to perform
the migration manually.


.. _Preflight Checklist: ../quick-start-preflight
.. _Ceph Deploy: ../../rados/deployment
.. _ceph-deploy install -h: ../../rados/deployment/ceph-deploy-install
.. _ceph-deploy new -h: ../../rados/deployment/ceph-deploy-new
.. _ceph-deploy osd: ../../rados/deployment/ceph-deploy-osd
.. _Running Ceph with Upstart: ../../rados/operations/operating#running-ceph-with-upstart
.. _Running Ceph with sysvinit: ../../rados/operations/operating#running-ceph-with-sysvinit
.. _CRUSH Map: ../../rados/operations/crush-map
.. _pool: ../../rados/operations/pools
.. _placement group: ../../rados/operations/placement-groups
.. _Monitoring a Cluster: ../../rados/operations/monitoring
.. _Monitoring OSDs and PGs: ../../rados/operations/monitoring-osd-pg