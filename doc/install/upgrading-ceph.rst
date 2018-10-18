================
 Upgrading Ceph
================

Each release of Ceph may have additional steps. Refer to the `release notes
document of your release`_ to identify release-specific procedures for your
cluster before using the upgrade procedures.


Summary
=======

You can upgrade daemons in your Ceph cluster while the cluster is online and in
service! Certain types of daemons depend upon others. For example, Ceph Metadata
Servers and Ceph Object Gateways depend upon Ceph Monitors and Ceph OSD Daemons.
We recommend upgrading in this order:

#. `Ceph Deploy`_
#. Ceph Monitors
#. Ceph OSD Daemons
#. Ceph Metadata Servers
#. Ceph Object Gateways

As a general rule, we recommend upgrading all the daemons of a specific type
(e.g., all ``ceph-mon`` daemons, all ``ceph-osd`` daemons, etc.) to ensure that
they are all on the same release. We also recommend that you upgrade all the
daemons in your cluster before you try to exercise new functionality in a
release.

The `Upgrade Procedures`_ are relatively simple, but do look at the `release
notes document of your release`_ before upgrading. The basic process involves
three steps: 

#. Use ``ceph-deploy`` on your admin node to upgrade the packages for 
   multiple hosts (using the ``ceph-deploy install`` command), or login to each 
   host and upgrade the Ceph package `using your distro's package manager`_.
   For example, when `Upgrading Monitors`_, the ``ceph-deploy`` syntax might
   look like this::
   
	ceph-deploy install --release {release-name} ceph-node1[ ceph-node2]
	ceph-deploy install --release firefly mon1 mon2 mon3

   **Note:** The ``ceph-deploy install`` command will upgrade the packages 
   in the specified node(s) from the old release to the release you specify. 
   There is no ``ceph-deploy upgrade`` command.

#. Login in to each Ceph node and restart each Ceph daemon.
   See `Operating a Cluster`_ for details.

#. Ensure your cluster is healthy. See `Monitoring a Cluster`_ for details.

.. important:: Once you upgrade a daemon, you cannot downgrade it.


Ceph Deploy
===========

Before upgrading Ceph daemons, upgrade the ``ceph-deploy`` tool. ::

	sudo pip install -U ceph-deploy

Or::

	sudo apt-get install ceph-deploy
	
Or::

	sudo yum install ceph-deploy python-pushy


Upgrade Procedures
==================

The following sections describe the upgrade process. 

.. important:: Each release of Ceph may have some additional steps. Refer to
   the `release notes document of your release`_ for details **BEFORE** you
   begin upgrading daemons.


Upgrading Monitors
------------------

To upgrade monitors, perform the following steps:

#. Upgrade the Ceph package for each daemon instance. 

   You may use ``ceph-deploy`` to address all monitor nodes at once. 
   For example::

	ceph-deploy install --release {release-name} ceph-node1[ ceph-node2]
	ceph-deploy install --release hammer mon1 mon2 mon3

   You may also use the package manager for your Linux distribution on 
   each individual node. To upgrade packages manually on each Debian/Ubuntu 
   host, perform the following steps::

	ssh {mon-host}
	sudo apt-get update && sudo apt-get install ceph

   On CentOS/Red Hat hosts, perform the following steps::

	ssh {mon-host}
	sudo yum update && sudo yum install ceph
	
 
#. Restart each monitor. For Ubuntu distributions, use:: 

	sudo restart ceph-mon id={hostname}

   For CentOS/Red Hat/Debian distributions, use::

	sudo /etc/init.d/ceph restart {mon-id}

   For CentOS/Red Hat distributions deployed with ``ceph-deploy``, 
   the monitor ID is usually ``mon.{hostname}``.
   
#. Ensure each monitor has rejoined the quorum::

	ceph mon stat

Ensure that you have completed the upgrade cycle for all of your Ceph Monitors.


Upgrading an OSD
----------------

To upgrade a Ceph OSD Daemon, perform the following steps:

#. Upgrade the Ceph OSD Daemon package. 

   You may use ``ceph-deploy`` to address all Ceph OSD Daemon nodes at 
   once. For example::

	ceph-deploy install --release {release-name} ceph-node1[ ceph-node2]
	ceph-deploy install --release hammer osd1 osd2 osd3

   You may also use the package manager on each node to upgrade packages 
   `using your distro's package manager`_. For Debian/Ubuntu hosts, perform the
   following steps on each host::

	ssh {osd-host}
	sudo apt-get update && sudo apt-get install ceph

   For CentOS/Red Hat hosts, perform the following steps::

	ssh {osd-host}
	sudo yum update && sudo yum install ceph


#. Restart the OSD, where ``N`` is the OSD number. For Ubuntu, use:: 

	sudo restart ceph-osd id=N

   For multiple OSDs on a host, you may restart all of them with Upstart. ::

	sudo restart ceph-osd-all
	
   For CentOS/Red Hat/Debian distributions, use::

	sudo /etc/init.d/ceph restart N	


#. Ensure each upgraded Ceph OSD Daemon has rejoined the cluster::

	ceph osd stat

Ensure that you have completed the upgrade cycle for all of your 
Ceph OSD Daemons.


Upgrading a Metadata Server
---------------------------

To upgrade a Ceph Metadata Server, perform the following steps:

#. Upgrade the Ceph Metadata Server package. You may use ``ceph-deploy`` to 
   address all Ceph Metadata Server nodes at once, or use the package manager 
   on each node. For example::

	ceph-deploy install --release {release-name} ceph-node1
	ceph-deploy install --release hammer mds1

   To upgrade packages manually, perform the following steps on each
   Debian/Ubuntu host::

	ssh {mon-host}
	sudo apt-get update && sudo apt-get install ceph-mds

   Or the following steps on CentOS/Red Hat hosts::

	ssh {mon-host}
	sudo yum update && sudo yum install ceph-mds

 
#. Restart the metadata server. For Ubuntu, use:: 

	sudo restart ceph-mds id={hostname}
	
   For CentOS/Red Hat/Debian distributions, use::

	sudo /etc/init.d/ceph restart mds.{hostname}

   For clusters deployed with ``ceph-deploy``, the name is usually either
   the name you specified on creation or the hostname.

#. Ensure the metadata server is up and running::

	ceph mds stat


Upgrading a Client
------------------

Once you have upgraded the packages and restarted daemons on your Ceph
cluster, we recommend upgrading ``ceph-common`` and client libraries
(``librbd1`` and ``librados2``) on your client nodes too.

#. Upgrade the package:: 

	ssh {client-host}
	apt-get update && sudo apt-get install ceph-common librados2 librbd1 python-rados python-rbd

#. Ensure that you have the latest version::

	ceph --version

If you do not have the latest version, you may need to uninstall, auto remove
dependencies and reinstall.


.. _using your distro's package manager: ../install-storage-cluster/
.. _Operating a Cluster: ../../rados/operations/operating
.. _Monitoring a Cluster: ../../rados/operations/monitoring
.. _release notes document of your release: ../../releases
