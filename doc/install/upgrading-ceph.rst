================
 Upgrading Ceph
================

You can upgrade daemons in your Ceph cluster one-by-one while the cluster is
online and in service! The upgrade process is relatively simple: 

#. Login to a host and upgrade the Ceph package.
#. Restart the daemon.
#. Ensure your cluster is healthy.

.. important:: Once you upgrade a daemon, you cannot downgrade it.

Certain types of daemons depend upon others. For example, metadata servers and
RADOS gateways depend upon Ceph monitors and OSDs. We recommend upgrading
daemons in this order:

#. Monitors (or OSDs)
#. OSDs (or Monitors)
#. Metadata Servers
#. RADOS Gateway

As a general rule, we recommend upgrading all the daemons of a specific type
(e.g., all ``ceph-osd`` daemons, all ``ceph-mon`` daemons, etc.) to ensure that
they are all on the same release. We also recommend that you upgrade all the
daemons in your cluster before you try to excercise new functionality in a
release. 

The following sections describe the upgrade process. 

.. important:: Each release of Ceph may have some additional steps. Refer to
   release-specific sections for details BEFORE you begin upgrading daemons.

Upgrading an OSD
================

To upgrade an OSD peform the following steps:

#. Upgrade the OSD package:: 

	ssh {osd-host}
	sudo apt-get update && sudo apt-get install ceph

#. Restart the OSD, where ``N`` is the OSD number:: 

	service ceph restart osd.N

#. Ensure the upgraded OSD has rejoined the cluster::

	ceph osd stat

Once you have successfully upgraded an OSD, you may upgrade another OSD until
you have completed the upgrade cycle for all of your OSDs.


Upgrading a Monitor
===================

To upgrade a monitor, perform the following steps:

#. Upgrade the ceph package::

	ssh {mon-host}
	sudo apt-get update && sudo apt-get install ceph
 
#. Restart the monitor::

	service ceph restart mon.{name}

#. Ensure the monitor has rejoined the quorum. ::

	ceph mon stat

Once you have successfully upgraded a monitor, you may upgrade another monitor
until you have completed the upgrade cycle for all of your monitors.


Upgrading a Metadata Server
===========================

To upgrade an MDS, perform the following steps:

#. Upgrade the ceph package::

	ssh {mds-host}
	sudo apt-get update && sudo apt-get install ceph-mds
 
#. Restart the metadata server::

	service ceph restart mds.{name}

#. Ensure the metadata server is up and running::

	ceph mds stat

Once you have successfully upgraded a metadata, you may upgrade another metadata
server until you have completed the upgrade cycle for all of your metadata
servers.

Upgrading a Client
==================

Once you have upgraded the packages and restarted daemons on your Ceph
cluster, we recommend upgrading ``ceph-common`` and client libraries
(``librbd1`` and ``librados2``) on your client nodes too.

#. Upgrade the package:: 

	ssh {client-host}
	apt-get update && sudo apt-get install ceph-common librados2 librbd1 python-ceph

#. Ensure that you have the latest version::

	ceph --version


Upgrading from Argonaut to Bobtail
==================================

When upgrading from Argonaut to Bobtail, you need to be aware of three things:

#. Authentication now defaults to **ON**, but used to default to off.
#. Monitors use a new internal on-wire protocol
#. RBD ``format2`` images require updgrading all OSDs before using it.

See the following sections for details. 


Authentication
--------------

The Ceph Bobtail release enables authentication by default. Bobtail also has
finer-grained authentication configuration settings. In previous versions of
Ceph (i.e., actually v 0.55 and earlier), you could simply specify:: 

	auth supported = [cephx | none]

This option still works, but is deprecated.  New releases support
``cluster``, ``service`` and ``client`` authentication settings as
follows::

	auth cluster required = [cephx | none]  # default cephx
	auth service required = [cephx | none] # default cephx
	auth client required = [cephx | none] # default cephx,none

.. important:: If your cluster does not currently have an ``auth
   supported`` line that enables authentication, you must explicitly
   turn it off in Bobtail using the settings below.::

	auth cluster required = none
	auth service required = none

   This will disable authentication on the cluster, but still leave
   clients with the default configuration where they can talk to a
   cluster that does enable it, but do not require it.

.. important:: If your cluster already has an ``auth supported`` option defined in
   the configuration file, no changes are necessary.

See `Ceph Authentication - Backward Compatibility`_ for details.

.. _Ceph Authentication: ../../rados/operations/authentication/
.. _Ceph Authentication - Backward Compatibility: ../../rados/operations/authentication/#backward-compatibility

Monitor On-wire Protocol
------------------------

We recommend upgrading all monitors to Bobtail. A mixture of Bobtail and
Argonaut monitors will not be able to use the new on-wire protocol, as  the
protocol requires all monitors to be Bobtail or greater. Upgrading  only a
majority of the nodes (e.g., two out of three) may expose the cluster to a
situation where a single additional failure may compromise availability (because
the non-upgraded daemon cannot participate in the new protocol).  We recommend
not waiting for an extended period of time between ``ceph-mon`` upgrades.


RBD Images
----------

The Bobtail release supports ``format 2`` images! However, you should not create
or use ``format 2`` RBD images until after all ``ceph-osd`` daemons have been
upgraded.  Note that ``format 1`` is still the default. You can use the new
``ceph osd ls`` and ``ceph tell osd.N version`` commands to doublecheck your
cluster. ``ceph osd ls`` will give a list of all OSD IDs that are part of the
cluster, and you can use that to write a simple shell loop to display all the
OSD version strings: ::

      for i in $(ceph osd ls); do
          ceph tell osd.${i} version
      done
