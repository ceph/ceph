===============
 Intro to Ceph
===============

Whether you want to provide :term:`Ceph Object Storage` and/or :term:`Ceph Block
Device` services to :term:`Cloud Platforms`, deploy a :term:`Ceph Filesystem` or
use Ceph for another purpose, all :term:`Ceph Storage Cluster` deployments begin
with setting up each :term:`Ceph Node`, your network and the Ceph Storage
Cluster. A Ceph Storage Cluster requires at least one Ceph Monitor and at least
two Ceph OSD Daemons. The Ceph Metadata Server is essential when running Ceph
Filesystem clients.

.. ditaa::  +---------------+ +---------------+ +---------------+
            |      OSDs     | |    Monitor    | |      MDS      |
            +---------------+ +---------------+ +---------------+

- **Ceph OSDs**: A :term:`Ceph OSD Daemon` (Ceph OSD) stores data, handles data
  replication, recovery, backfilling, rebalancing, and provides some monitoring
  information to Ceph Monitors by checking other Ceph OSD Daemons for a 
  heartbeat. A Ceph Storage Cluster requires at least two Ceph OSD Daemons to 
  achieve an ``active + clean`` state when the cluster makes two copies of your
  data (Ceph makes 3 copies by default, but you can adjust it).
  
- **Monitors**: A :term:`Ceph Monitor` maintains maps of the cluster state, 
  including the monitor map, the OSD map, the Placement Group (PG) map, and the
  CRUSH map. Ceph maintains a history (called an "epoch") of each state change 
  in the Ceph Monitors, Ceph OSD Daemons, and PGs.

- **MDSs**: A :term:`Ceph Metadata Server` (MDS) stores metadata on behalf of 
  the :term:`Ceph Filesystem` (i.e., Ceph Block Devices and Ceph Object Storage
  do not use MDS). Ceph Metadata Servers make it feasible for POSIX file system 
  users to execute basic commands like ``ls``, ``find``, etc. without placing 
  an enormous burden on the Ceph Storage Cluster.

Ceph stores a client's data as objects within storage pools. Using the CRUSH 
algorithm, Ceph calculates which placement group should contain the object, 
and further calculates which Ceph OSD Daemon should store the placement group.
The CRUSH algorithm enables the Ceph Storage Cluster to scale, rebalance, and
recover dynamically.


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
