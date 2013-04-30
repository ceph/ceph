====================
 RADOS Object Store
====================

Ceph's :abbr:`RADOS (Reliable Autonomic Distributed Object Store)` Object Store
is the foundation for all Ceph clusters. When you use object store clients such
as the CephFS filesystem, the RESTful Gateway or Ceph block devices, Ceph reads
data from and writes data to the object store. Ceph's RADOS Object Stores
consist of two types of daemons: Object Storage Daemons (OSDs) store data as
objects on storage nodes; and Monitors maintain a master copy of the cluster
map. A Ceph cluster may contain thousands of storage nodes. A minimal system 
will have at least two OSDs for data replication. 

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Config and Deploy</h3>

Once you have installed Ceph packages, you must configure. There are a  a few
required settings, but most configuration settings have default  values.
Following the initial configuration, you must deploy Ceph. Deployment consists
of creating and initializing data directories,  keys, etc. 

.. toctree::
	:maxdepth: 2

	Configuration <configuration/index>
	Deployment <deployment/index>

.. raw:: html 

	</td><td><h3>Operations</h3>

Once you have a deployed Ceph cluster, you may begin operating your cluster.

.. toctree::
	:maxdepth: 2
	
	
	Operations <operations/index>

.. toctree::
	:maxdepth: 1

	Man Pages <man/index>


.. toctree:: 
	:hidden:
	
	troubleshooting/index

.. raw:: html 

	</td><td><h3>APIs</h3>

Most Ceph deployments use Ceph `block devices`_, the `gateway`_ and/or the
`CephFS filesystem`_. You  may also develop applications that talk directly to
the Ceph object store.

.. toctree::
	:maxdepth: 2

	APIs <api/index>
	
.. raw:: html

	</td></tr></tbody></table>

.. _block devices: ../rbd/rbd
.. _CephFS filesystem: ../cephfs/
.. _gateway: ../radosgw/
