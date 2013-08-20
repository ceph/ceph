=============================
 Storage Cluster Quick Start
=============================

If you haven't completed your `Preflight Checklist`_, do that first. This
**Quick Start** sets up a two-node demo cluster so you can explore some of the
:term:`Ceph Storage Cluster` functionality. This **Quick Start**  will help you
install a minimal Ceph Storage Cluster on a server node from your admin node
using ``ceph-deploy``.

.. ditaa:: 
           /----------------\         /----------------\
           |   Admin Node   |<------->|   Server Node  |
           | cCCC           |         | cCCC           |
           +----------------+         +----------------+
           |  Ceph Commands |         |   ceph - mon   |
           \----------------/         +----------------+
                                      |   ceph - osd   |
                                      +----------------+
                                      |   ceph - mds   |
                                      \----------------/


For best results, create a directory on your admin node for maintaining the
configuration of your cluster. ::

	mkdir my-cluster
	cd my-cluster

.. tip:: The ``ceph-deploy`` utility will output files to the 
   current directory. Ensure you are in this directory when executing
   ``ceph-deploy``.


Create a Cluster
================

To create your Ceph Storage Cluster, declare its initial monitors, generate a
filesystem ID (``fsid``) and generate monitor keys by entering the following
command on a commandline prompt:: 

	ceph-deploy new {mon-server-name}
	ceph-deploy new mon-ceph-node

Check the output of ``ceph-deploy`` with ``ls`` and ``cat`` in the current
directory. You should see a Ceph configuration file, a keyring, and a log file
for the new cluster.  See `ceph-deploy new -h`_ for additional details.

.. topic:: Single Node Quick Start

	Assuming only one node for your Ceph Storage Cluster, you	will need to 
	modify the default ``osd crush chooseleaf type`` setting (it defaults to 
	``1`` for ``node``) to ``0`` for ``device`` so that it will peer with OSDs 
	on the local node. Add the following line to your Ceph configuration file:: 
	
		osd crush chooseleaf type = 0 

.. tip:: If you deploy without executing foregoing step on a single node 
   cluster, your Ceph Storage Cluster will not achieve an ``active + clean``
   state. To remedy this situation, you must modify your `CRUSH Map`_.

Install Ceph
============

To install Ceph on your server node, open a command line on your admin
node and type the following::

	ceph-deploy install {server-node-name}[,{server-node-name}]
	ceph-deploy install mon-ceph-node

Without additional arguments, ``ceph-deploy`` will install the most recent
stable Ceph package to the server node. See `ceph-deploy install -h`_ for
additional details.

.. tip:: When ``ceph-deploy`` completes installation successfully, 
   it should echo ``OK``.


Add a Monitor
=============

To run a Ceph cluster, you need at least one Ceph Monitor. When using
``ceph-deploy``, the tool enforces a single Ceph Monitor per node. Execute the
following to create a Ceph Monitor::

	ceph-deploy mon create {mon-server-name}
	ceph-deploy mon create mon-ceph-node

.. tip:: In production environments, we recommend running Ceph Monitors on 
   nodes that do not run OSDs.

When you have added a monitor successfully, directories under ``/var/lib/ceph``
on your server node should have subdirectories ``bootstrap-mds`` and
``bootstrap-osd`` that contain keyrings. If these directories do not contain
keyrings, execute ``ceph-deploy mon create`` again on the admin node.


Gather Keys
===========

To deploy additional daemons and provision them with monitor authentication keys
from your admin node, you must first gather keys from a monitor node. Execute
the following to gather keys:: 

	ceph-deploy gatherkeys {mon-server-name}
	ceph-deploy gatherkeys mon-ceph-node


Once you have gathered keys, your local directory should have the following keyrings:

- ``{cluster-name}.client.admin.keyring``
- ``{cluster-name}.bootstrap-osd.keyring``
- ``{cluster-name}.bootstrap-mds.keyring``

If you don't have these keyrings, you may not have created a monitor successfully, 
or you may have a problem with your network connection. Ensure that you complete
this step such that you have the foregoing keyrings before proceeding further.

.. tip:: You may repeat this procedure. If it fails, check to see if the 
   ``/var/lib/ceph/boostrap-{osd}|{mds}`` directories on the server node 
   have keyrings. If they do not have keyrings, try adding the monitor again;
   then, return to this step.


Add Ceph OSD Daemons
====================

For a cluster's object placement groups to reach an ``active + clean`` state,
you must have at least two instances of a :term:`Ceph OSD Daemon` running and 
at least two copies of an object (``osd pool default size`` is ``2`` 
by default).

Adding Ceph OSD Daemons is slightly more involved than other ``ceph-deploy`` 
commands, because a Ceph OSD Daemon involves both a data store and a journal. 
The ``ceph-deploy`` tool has the ability to invoke ``ceph-disk-prepare`` to 
prepare the disk and activate the Ceph OSD Daemon for you.

Multiple OSDs on the OS Disk (Demo Only)
----------------------------------------

For demonstration purposes, you may wish to add multiple OSDs to the OS disk
(not recommended for production systems). To use Ceph OSDs daemons on the OS
disk, you must use ``prepare`` and ``activate`` as separate steps. First, 
define a directory for the Ceph OSD daemon(s). ::
   
	mkdir /tmp/osd0
	mkdir /tmp/osd1
   
Then, use ``prepare`` to prepare the directory(ies) for use with a
Ceph OSD Daemon. :: 
   
	ceph-deploy osd prepare {osd-node-name}:/tmp/osd0
	ceph-deploy osd prepare {osd-node-name}:/tmp/osd1

Finally, use ``activate`` to activate the Ceph OSD Daemons. :: 

	ceph-deploy osd activate {osd-node-name}:/tmp/osd0
	ceph-deploy osd activate {osd-node-name}:/tmp/osd1		

.. tip:: You need two OSDs to reach an ``active + clean`` state. You can 
   add one OSD at a time, but OSDs need to communicate with each other
   for Ceph to run properly. Always use more than one OSD per cluster.


List Disks
----------

To list the available disk drives on a prospective :term:`Ceph Node`, execute 
the following::

	ceph-deploy disk list {osd-node-name}
	ceph-deploy disk list ceph-node


Zap a Disk
----------

To zap a disk (delete its partition table) in preparation for use with Ceph,
execute the following::

	ceph-deploy disk zap {osd-node-name}:{disk}
	ceph-deploy disk zap ceph-node:sdb ceph-node:sdb2

.. important:: This will delete all data on the disk.


Add OSDs on Standalone Disks
----------------------------

You can add OSDs using ``prepare`` and ``activate`` in two discrete
steps. To prepare a disk for use with a Ceph OSD Daemon, execute the 
following:: 

	ceph-deploy osd prepare {osd-node-name}:{osd-disk-name}[:/path/to/journal]
	ceph-deploy osd prepare ceph-node:sdb

To activate the Ceph OSD Daemon, execute the following:: 

	ceph-deploy osd activate {osd-node-name}:{osd-partition-name}
	ceph-deploy osd activate ceph-node:sdb1

To prepare an OSD disk and activate it in one step, execute the following:: 

	ceph-deploy osd create {osd-node-name}:{osd-disk-name}[:/path/to/journal] [{osd-node-name}:{osd-disk-name}[:/path/to/journal]]
	ceph-deploy osd create ceph-node:sdb:/dev/ssd1 ceph-node:sdc:/dev/ssd2


.. note:: The journal example assumes you will use a partition on a separate 
   solid state drive (SSD). If you omit a journal drive or partition, 
   ``ceph-deploy`` will use create a separate partition for the journal
   on the same drive. If you have already formatted your disks and created
   partitions, you may also use partition syntax for your OSD disk.

You must add a minimum of two Ceph OSD Daemons for the placement groups in 
a cluster to achieve an ``active + clean`` state. 


Add a MDS
=========

To use CephFS, you need at least one metadata node. Execute the following to
create a metadata node::

	ceph-deploy mds create {node-name}
	ceph-deploy mds create ceph-node


.. note:: Currently Ceph runs in production with one metadata node only. You 
   may use more, but there is currently no commercial support for a cluster 
   with multiple metadata nodes.


Summary
=======

Deploying a Ceph cluster with ``ceph-deploy`` automatically starts the cluster.
To operate the cluster daemons, see `Running Ceph with Upstart`_.

Once you deploy a Ceph cluster, you can try out some of the administration
functionality, the object store command line, and then proceed to Quick Start
guides for RBD, CephFS, and the Ceph Gateway.

.. topic:: Other ceph-deploy Commands

	To view other ``ceph-deploy`` commands, execute: 
	
	``ceph-deploy -h``
	

See `Ceph Deploy`_ for additional details.


.. _Preflight Checklist: ../quick-start-preflight
.. _Ceph Deploy: ../../rados/deployment
.. _ceph-deploy install -h: ../../rados/deployment/ceph-deploy-install
.. _ceph-deploy new -h: ../../rados/deployment/ceph-deploy-new
.. _Running Ceph with Upstart: ../../rados/operations/operating#running-ceph-with-upstart
.. _CRUSH Map: ../../rados/operations/crush-map