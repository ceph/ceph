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
	apt-get update && sudo apt-get upgrade

#. Restart the OSD, where ``N`` is the OSD number:: 

	service ceph restart osd.N

#. Ensure the upgraded OSD has rejoined the cluster::

	ceph osd stat

Once you have successfully upgraded an OSD, you may upgrade another OSD until
you have completed the upgrade cycle for all of your OSDs.


Upgrading a Monitor
===================

To upgrade a monitor, perform the following steps:

#. Upgrade the monitor package::

	ssh {mon-host}
	apt-get update && sudo apt-get upgrade
 
#. Restart the monitor, where ``A`` is the monitor letter:: 

	service ceph restart mon.A

#. Ensure the monitor has rejoined the quorum. ::

	ceph mon stat

Once you have successfully upgraded a monitor, you may upgrade another monitor
until you have completed the upgrade cycle for all of your monitors.


Upgrading a Metadata Server
===========================

To upgrade a monitor, perform the following steps:

#. Upgrade the monitor package::

	ssh {mds-host}
	apt-get update && sudo apt-get upgrade
 
#. Restart the metadata server, where ``A`` is the metadata server letter:: 

	service ceph restart mds.A

#. Ensure the metadata server is up and running::

	ceph mds stat

Once you have successfully upgraded a metadata, you may upgrade another metadata
server until you have completed the upgrade cycle for all of your metadata
servers.

Upgrading a Client
==================

Once you have upgraded the packages and restarted daemons on your Ceph cluster,
we recommend upgrading ``ceph-common`` on your client nodes too.

#. Upgrade the package:: 

	ssh {client-host}
	apt-get update && sudo apt-get upgrade

#. Ensure that you have the latest version::

	ceph --version



Upgrading from Argonaut to Bobtail
==================================

When upgrading from Argonaut to Bobtail, you need to be aware of three things:

#. Authentication is **ON** by default.
#. Monitors use a new internal on-wire protocol
#. RBD ``format2`` images require updgrading all OSDs before using it.

See the following sections for details. 


Authentication
--------------

The Ceph Bobtail release enables authentication by default. Bobtail also has
finer-grained authentication configuration settings. In previous versions of
Ceph (i.e., actually v 0.55 and earlier), you could simply specify:: 

	auth supported = [cephx | none]

Bobtail supports ``client``, ``service`` and ``cluster`` authentication settings
as follows:: 

	auth client required = [cephx | none]
	auth service required = [cephx | none]
	auth cluster required = [cephx | none]

See `Ceph Authentication`_ for details. When upgrading from Argonaut to Bobtail,
you should change the authentication  settings in your Ceph configuration file
to reflect these three new ``auth`` settings. 

.. important:: If you do not use authentication, you must explicitly turn it
   off in Bobtail using the three new ``auth`` settings.
   
.. important:: The ``auth client`` setting must be explicitly set in the 
   Ceph configuration file on Ceph clients.

Once you have upgraded all of your daemons, we recommend addding the following
to the ``[global]`` section of your Ceph configuration file. ::

	[global]	
		cephx require signatures = true

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
