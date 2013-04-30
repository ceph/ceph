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
  to Ceph monitors by checking other OSDs for a heartbeat. A cluster requires
  at least two OSDs to achieve an ``active + clean`` state.
  
- **Monitors**: Ceph monitors maintain maps of the cluster state, including 
  the monitor map, the OSD map, the Placement Group (PG) map, and the CRUSH 
  map. Ceph maintains a history (called an "epoch") of each state change in 
  the monitors, OSDs, and PGs.

- **MDSs**: Metadata Servers (MDSs) store metadata on behalf of the CephFS
  filesystem (i.e., Ceph block devices and Ceph gateways do not use MDS).
  Ceph MDS servers make it feasible for POSIX file system users to execute
  basic commands like ``ls``, ``find``, etc. without placing an enormous
  burden on the object store.


.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Preflight</h3>

Client and server machines may require some basic configuration work prior to
deploying a Ceph cluster. You can also avail yourself of help from the Ceph 
community by getting involved.

.. toctree::

   Get Involved <get-involved>
   Preflight <quick-start-preflight>

.. raw:: html 

	</td><td><h3>Step 2: Object Store</h3>
	
Once you've completed your preflight checklist,  you should be able to begin
deploying a Ceph cluster.

.. toctree::

	Object Store Quick Start <quick-ceph-deploy>


.. raw:: html 

	</td><td><h3>Step 3: Ceph Client(s)</h3>
	
Most Ceph users don't store objects directly. They typically use at least one of
Ceph block devices, the CephFS filesystem, and the RESTful gateway.

.. toctree::
	
   Block Device Quick Start <quick-rbd>
   CephFS Quick Start <quick-cephfs>
   Gateway Quick Start <quick-rgw>


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


