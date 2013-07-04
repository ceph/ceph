=================
 Getting Started
=================

Whether you want to provide :term:`Ceph Object Storage` and/or :term:`Ceph Block
Device` services to :term:`Cloud Platforms`, deploy a :term:`Ceph Filesystem` or
use Ceph for another purpose, all :term:`Ceph Storage Cluster` deployments begin
with setting up each :term:`Ceph Node`, your network and the Ceph Storage
Cluster. A Ceph Storage Cluster has three essential daemons:

.. ditaa::  +---------------+ +---------------+ +---------------+
            |      OSDs     | |    Monitor    | |      MDS      |
            +---------------+ +---------------+ +---------------+

- **OSDs**: A :term:`Ceph OSD Daemon` (OSD) stores data, handles data 
  replication, recovery, backfilling, rebalancing, and provides some monitoring
  information to Ceph Monitors by checking other Ceph OSD Daemons for a 
  heartbeat. A Ceph Storage Cluster requires at least two Ceph OSD Daemons to 
  achieve an ``active + clean`` state.
  
- **Monitors**: A :term:`Ceph Monitor` maintains maps of the cluster state, 
  including the monitor map, the OSD map, the Placement Group (PG) map, and the
  CRUSH map. Ceph maintains a history (called an "epoch") of each state change 
  in the Ceph Monitors, Ceph OSD Daemons, and PGs.

- **MDSs**: A :term:`Ceph Metadata Server` (MDS) stores metadata on behalf of 
  the :term:`Ceph Filesystem` (i.e., Ceph Block Devices and Ceph Object Storage
  do not use MDS). Ceph Metadata Servers make it feasible for POSIX file system 
  users to execute basic commands like ``ls``, ``find``, etc. without placing 
  an enormous burden on the Ceph Storage Cluster.


.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Preflight</h3>

A :term:`Ceph Client` and a :term:`Ceph Node` may require some basic
configuration  work prior to deploying a Ceph Storage Cluster. You can also
avail yourself of help  from the Ceph community by getting involved.

.. toctree::

   Get Involved <get-involved>
   Preflight <quick-start-preflight>

.. raw:: html 

	</td><td><h3>Step 2: Storage Cluster</h3>
	
Once you've completed your preflight checklist,  you should be able to begin
deploying a Ceph Storage Cluster.

.. toctree::

	Storage Cluster Quick Start <quick-ceph-deploy>


.. raw:: html 

	</td><td><h3>Step 3: Ceph Client(s)</h3>
	
Most Ceph users don't store objects directly in the Ceph Storage Cluster. They typically use at least one of
Ceph Block Devices, the Ceph Filesystem, and Ceph Object Storage.

.. toctree::
	
   Block Device Quick Start <quick-rbd>
   Filesystem Quick Start <quick-cephfs>
   Object Storage Quick Start <quick-rgw>


.. raw:: html

	</td></tr></tbody></table>

For releases prior to Cuttlefish, see the `5-minute Quick Start`_ for deploying
with `mkcephfs`_. To transition  a cluster deployed with ``mkcephfs`` for use
with ``ceph-deploy``, see `Transitioning to ceph-deploy`_.

.. _5-minute Quick Start: quick-start 
.. _mkcephfs: ../rados/deployment/mkcephfs
.. _Transitioning to ceph-deploy: ../rados/deployment/ceph-deploy-transition

.. toctree::
   :hidden:
	
   quick-start


