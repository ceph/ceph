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
daemons in your cluster before you try to exercise new functionality in a
release.

Each release of Ceph may have some additional steps. Refer to the following
sections to identify release-specific procedures for your cluster before 
using the upgrade procedures.


Argonaut to Bobtail
===================

When upgrading from Argonaut to Bobtail, you need to be aware of several things:

#. Authentication now defaults to **ON**, but used to default to **OFF**.
#. Monitors use a new internal on-wire protocol.
#. RBD ``format2`` images require upgrading all OSDs before using it.

Ensure that you update package repository paths. For example:: 

	sudo rm /etc/apt/sources.sources.list.d/ceph.list
	echo deb http://ceph.com/debian-bobtail/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

See the following sections for additional details.

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


Monitor On-wire Protocol
------------------------

We recommend upgrading all monitors to Bobtail. A mixture of Bobtail and
Argonaut monitors will not be able to use the new on-wire protocol, as the
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


Argonaut to Cuttlefish
======================

To upgrade your cluster from Argonaut to Cuttlefish, please read this section,
and the sections on upgrading from Argonaut to Bobtail and upgrading from
Bobtail to Cuttlefish carefully. When upgrading from Argonaut to Cuttlefish,
**YOU MUST UPGRADE YOUR MONITORS FROM ARGONAUT TO BOBTAIL FIRST!!!**. All other
Ceph daemons can upgrade from Argonaut to Cuttlefish without the intermediate
upgrade to Bobtail.

.. important:: Ensure that the repository specified points to Bobtail, not
   Cuttlefish.
   
For example:: 

	sudo rm /etc/apt/sources.sources.list.d/ceph.list
	echo deb http://ceph.com/debian-bobtail/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

We recommend upgrading all monitors to Bobtail before proceeding with the
upgrade of the monitors to Cuttlefish. A mixture of Bobtail and Argonaut
monitors will not be able to use the new on-wire protocol, as the protocol
requires all monitors to be Bobtail or greater. Upgrading only a majority of the
nodes (e.g., two out of three) may expose the cluster to a situation where a
single additional failure may compromise availability (because the non-upgraded
daemon cannot participate in the new protocol).  We recommend not waiting for an
extended period of time between ``ceph-mon`` upgrades. See `Upgrading a
Monitor`_ for details.

.. note:: See the `Authentication`_ section and the 
   `Ceph Authentication - Backward Compatibility`_ for additional information
   on authentication backward compatibility settings for Bobtail.

Once you complete the upgrade of your monitors from Argonaut to Bobtail, you
must upgrade the monitors from Bobtail to Cuttlefish. Ensure that you have
a quorum before beginning this upgrade procedure. Before upgrading, remember
to replace the reference to the Bobtail repository with a reference to the
Cuttlefish repository. For example:: 

	sudo rm /etc/apt/sources.sources.list.d/ceph.list
	echo deb http://ceph.com/debian-cuttlefish/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

See `Upgrading a Monitor`_ for details.

The architecture of the monitors changed significantly from Argonaut to
Cuttlefish. See `Monitor Config Reference`_ and `Joao's blog post`_ for details.
Once you complete the monitor upgrade, you can upgrade the OSD daemons and the
MDS daemons using the generic procedures. See `Upgrading an OSD`_ and `Upgrading
a Metadata Server`_ for details.


Bobtail to Cuttlefish
=====================

Upgrading your cluster from Bobtail to Cuttlefish has a few important
considerations. First, the monitor uses a new architecture, so you should
upgrade the full set of monitors to use Cuttlefish. Second, if you run multiple
metadata servers in a cluster, ensure the metadata servers have unique names.
See the following sections for details.

Replace any ``apt`` reference to older repositories with a reference to the
Cuttlefish repository. For example:: 

	sudo rm /etc/apt/sources.sources.list.d/ceph.list
	echo deb http://ceph.com/debian-cuttlefish/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list


Monitor
-------

The architecture of the monitors changed significantly from Bobtail to
Cuttlefish. See `Monitor Config Reference`_ and `Joao's blog post`_ for 
details. This means that v0.59 and pre-v0.59 monitors do not talk to each other
(Cuttlefish is v.0.61). When you upgrade each monitor, it will convert its 
local data store to the new format. Once you upgrade a majority of monitors, 
the monitors form a quorum using the new protocol and the old monitors will be
blocked until they get upgraded. For this reason, we recommend upgrading the
monitors in immediate succession. 

.. important:: Do not run a mixed-version cluster for an extended period.


MDS Unique Names
----------------

The monitor now enforces that MDS names be unique. If you have multiple metadata
server daemons that start with with the same ID (e.g., mds.a) the second
metadata server will implicitly mark the first metadata server as ``failed``.
Multi-MDS configurations with identical names must be adjusted accordingly to
give daemons unique names. If you run your cluster with one  metadata server,
you can disregard this notice for now.


Upgrade Procedures
==================

The following sections describe the upgrade process. 

.. important:: Each release of Ceph may have some additional steps. Refer to
   release-specific sections for details **BEFORE** you begin upgrading daemons.


Upgrading a Monitor
-------------------

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


Upgrading an OSD
----------------

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


Upgrading a Metadata Server
---------------------------

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
------------------

Once you have upgraded the packages and restarted daemons on your Ceph
cluster, we recommend upgrading ``ceph-common`` and client libraries
(``librbd1`` and ``librados2``) on your client nodes too.

#. Upgrade the package:: 

	ssh {client-host}
	apt-get update && sudo apt-get install ceph-common librados2 librbd1 python-ceph

#. Ensure that you have the latest version::

	ceph --version


.. _Monitor Config Reference: ../../rados/configuration/mon-config-ref
.. _Joao's blog post: http://ceph.com/dev-notes/cephs-new-monitor-changes 
.. _Ceph Authentication: ../../rados/operations/authentication/
.. _Ceph Authentication - Backward Compatibility: ../../rados/operations/authentication/#backward-compatibility
