==========================
 Object Store Quick Start
==========================

If you haven't completed your `Preflight Checklist`_, do that first. This
**Quick Start** sets up a two-node demo cluster so you can explore some of the
object store functionality. This **Quick Start**  will help you install a
minimal Ceph cluster on a server host from your admin host using
``ceph-deploy``.

.. ditaa:: 
           /----------------\         /----------------\
           |   Admin Host   |<------->|   Server Host  |
           | cCCC           |         | cCCC           |
           +----------------+         +----------------+
           |  Ceph Commands |         |   ceph - mon   |
           \----------------/         +----------------+
                                      |   ceph - osd   |
                                      +----------------+
                                      |   ceph - mds   |
                                      \----------------/


For best results, create a directory on your client machine
for maintaining the configuration of your cluster. ::

	mkdir my-cluster
	cd my-cluster

.. tip:: The ``ceph-deploy`` utility will output files to the 
   current directory.


Install Ceph
============

To install Ceph on your server, open a command line on your client
machine and type the following::

	ceph-deploy install {server-name}[,{server-name}]
	ceph-deploy install --stable cuttlefish ceph-server

Without additional arguments, ``ceph-deploy`` will install the most recent
stable Ceph package to the host machine. See `ceph-deploy install -h`_ for
additional details.


Create a Cluster
================

To create your cluster, declare its inital monitors, generate a filesystem ID
(``fsid``) and generate monitor keys by entering the following command on a
commandline prompt:: 

	ceph-deploy new {server-name}
	ceph-deploy new ceph-server

Check the output with ``ls`` and ``cat`` in the current directory. You should
see a Ceph configuration file, a keyring, and a log file for the new cluster. 
See `ceph-deploy new -h`_ for additional details.

.. topic:: Single Host Quick Start

	Assuming only one host for your cluster, you	will need to modify the default 
	``osd crush chooseleaf type`` setting (it	defaults to ``1`` for ``host``) to 
	``0`` so that it will peer with OSDs on the local host. Add the following
	line to your Ceph configuration file:: 
	
		osd crush chooseleaf type = 0 


Add a Monitor
=============

To run a Ceph cluster, you need at least one monitor. When using ``ceph-deploy``,
the tool enforces a single monitor per host. Execute the following to create
a monitor::

	ceph-deploy mon create {server-name}
	ceph-deploy mon create ceph-server

.. tip:: In production environments, we recommend running monitors on hosts
   that do not run OSDs.


Gather Keys
===========

To deploy additional daemons and provision them with monitor authentication keys
from your admin host, you must first gather keys from a monitor host. Execute
the following to gather keys:: 

	ceph-deploy gatherkeys {mon-server-name}
	ceph-deploy gatherkeys ceph-server


Add OSDs
========

For a cluster's object placement groups to reach an ``active + clean`` state,
you must have at least two OSDs and at least two copies of an object (``osd pool
default size`` is ``2`` by default).

Adding OSDs is slightly more involved than other ``ceph-deploy`` commands,
because an OSD involves both a data store and a journal. The ``ceph-deploy``
tool has the ability to invoke ``ceph-disk-prepare`` to prepare the disk and
activate the OSD for you.


List Disks
----------

To list the available disk drives on a prospective OSD host, execute the
following::

	ceph-deploy disk list {osd-server-name}
	ceph-deploy disk list ceph-server


Zap a Disk
----------

To zap a disk (delete its partition table) in preparation for use with Ceph,
execute the following::

	ceph-deploy disk zap {osd-server-name}:/path/to/disk

.. important:: This will delete all data in the partition.


Add OSDs
--------

To prepare an OSD disk and activate it, execute the following:: 

	ceph-deploy osd create {osd-server-name}:/path/to/disk[:/path/to/journal]
	ceph-deploy osd create {osd-server-name}:/dev/sdb1
	ceph-deploy osd create {osd-server-name}:/dev/sdb2

You must add a minimum of two OSDs for the placement groups in a cluster to achieve
an ``active + clean`` state.  


Add a MDS
=========

To use CephFS, you need at least one metadata server. Execute the following to
create a metadata server::

	ceph-deploy mds create {server-name}
	ceph-deploy mds create ceph-server


.. note:: Currently Ceph runs in production with one metadata server only. You 
   may use more, but there is currently no commercial support for a cluster 
   with multiple metadata servers.


Summary
=======

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