=================
 Getting Started
=================

Whether you want to provide RESTful object services and/or block devices to a
cloud solution, deploy a CephFS filesystem or use Ceph for another purpose, all
Ceph clusters begin with setting up your host computers, network and the Ceph
Object Store. A Ceph object store cluster has three essential daemons:

.. ditaa::  +---------------+ +---------------+ +---------------+
            |      OSDs     | |    Monitor    | |      MDS      |
            +---------------+ +---------------+ +---------------+

- **OSDs**: Object Storage Daemons (OSDs) store data, handle data replication, 
  recovery, backfilling, rebalancing, and provide some monitoring information
  to Ceph monitors by checking other OSDs for a heartbeat.
  
- **Monitors**: Ceph monitors maintain maps of the cluster state, including 
  the monitor map, the OSD map, the Placement Group (PG) map, and the CRUSH 
  map. Ceph maintains a history (called an "epoch") of each state change in 
  the monitors, OSDs, or PGs.

- **MDSs**: Metadata Servers (MDSs) store metadata on behalf of the CephFS
  filesystem (i.e., Ceph block devices and Ceph gateways do not use MDS).
  Ceph MDS servers make it feasible for POSIX file system users to execute
  basic commands like ``ls``, ``find``, etc. without placing an enormous
  burden on the object store.


.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="25%"><col width="25%"><col width="25%"><col width="25%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Preflight</h3>

.. toctree::

   Get Involved <get-involved>
   Preflight <quick-start-preflight>

.. raw:: html 

	</td><td><h3>Step 2: Object Store</h3>

.. toctree::

	Object Store Quick Start <quick-ceph-deploy>


.. raw:: html 

	</td><td><h3>Step 3: Ceph Client(s)</h3>

.. toctree::
	
   Block Device Quick Start <quick-rbd>
   CephFS Quick Start <quick-cephfs>
   Gateway Quick Start <quick-rgw>


.. raw:: html 

	</td><td><h3>Step 4: Expand Your Cluster</h3>
	
	<placeholder>

.. raw:: html

	</td></tr></tbody></table>



.. toctree::


