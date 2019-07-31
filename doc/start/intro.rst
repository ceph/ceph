===============
 Intro to Ceph
===============

Whether you want to provide :term:`Ceph Object Storage` and/or
:term:`Ceph Block Device` services to :term:`Cloud Platforms`, deploy
a :term:`Ceph Filesystem` or use Ceph for another purpose, all
:term:`Ceph Storage Cluster` deployments begin with setting up each
:term:`Ceph Node`, your network, and the Ceph Storage Cluster. A Ceph
Storage Cluster requires at least one Ceph Monitor, Ceph Manager, and
Ceph OSD (Object Storage Daemon). The Ceph Metadata Server is also
required when running Ceph Filesystem clients.

.. ditaa::  +---------------+ +------------+ +------------+ +---------------+
            |      OSDs     | | Monitors   | |  Managers  | |      MDSs     |
            +---------------+ +------------+ +------------+ +---------------+

- **Monitors**: A :term:`Ceph Monitor` (``ceph-mon``) maintains maps
  of the cluster state, including the monitor map, manager map, the
  OSD map, and the CRUSH map.  These maps are critical cluster state
  required for Ceph daemons to coordinate with each other.  Monitors
  are also responsible for managing authentication between daemons and
  clients.  At least three monitors are normally required for
  redundancy and high availability.

- **Managers**: A :term:`Ceph Manager` daemon (``ceph-mgr``) is
  responsible for keeping track of runtime metrics and the current
  state of the Ceph cluster, including storage utilization, current
  performance metrics, and system load.  The Ceph Manager daemons also
  host python-based modules to manage and expose Ceph cluster
  information, including a web-based :ref:`mgr-dashboard` and
  `REST API`_.  At least two managers are normally required for high
  availability.

- **Ceph OSDs**: A :term:`Ceph OSD` (object storage daemon,
  ``ceph-osd``) stores data, handles data replication, recovery,
  rebalancing, and provides some monitoring information to Ceph
  Monitors and Managers by checking other Ceph OSD Daemons for a
  heartbeat. At least 3 Ceph OSDs are normally required for redundancy
  and high availability.

- **MDSs**: A :term:`Ceph Metadata Server` (MDS, ``ceph-mds``) stores
  metadata on behalf of the :term:`Ceph Filesystem` (i.e., Ceph Block
  Devices and Ceph Object Storage do not use MDS). Ceph Metadata
  Servers allow POSIX file system users to execute basic commands (like
  ``ls``, ``find``, etc.) without placing an enormous burden on the
  Ceph Storage Cluster.

Ceph stores data as objects within logical storage pools. Using the
:term:`CRUSH` algorithm, Ceph calculates which placement group should
contain the object, and further calculates which Ceph OSD Daemon
should store the placement group.  The CRUSH algorithm enables the
Ceph Storage Cluster to scale, rebalance, and recover dynamically.

.. _REST API: ../../mgr/restful

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>Recommendations</h3>
	
To begin using Ceph in production, you should review our hardware
recommendations and operating system recommendations. 

.. toctree::
   :maxdepth: 2

   Hardware Recommendations <hardware-recommendations>
   OS Recommendations <os-recommendations>


.. raw:: html 

	</td><td><h3>Get Involved</h3>

   You can avail yourself of help or contribute documentation, source 
   code or bugs by getting involved in the Ceph community.

.. toctree::
   :maxdepth: 2

   get-involved
   documenting-ceph

.. raw:: html

	</td></tr></tbody></table>
