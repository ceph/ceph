======================
 Ceph Storage Cluster
======================

The :term:`Ceph Storage Cluster` is the foundation for all Ceph deployments.
Based upon :abbr:`RADOS (Reliable Autonomic Distributed Object Store)`, Ceph
Storage Clusters consist of two types of daemons: a :term:`Ceph OSD Daemon`
(OSD) stores data as objects on a storage node; and a :term:`Ceph Monitor`
maintains a master copy of the cluster map. A Ceph Storage Cluster may contain
thousands of storage nodes. A minimal system will have at least one 
Ceph Monitor and two Ceph OSD Daemons for data replication. 

The Ceph Filesystem, Ceph Object Storage and Ceph Block Devices read data from
and write data to the Ceph Storage Cluster.

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Config and Deploy</h3>

Ceph Storage Clusters have a few required settings, but most configuration
settings have default values. A typical deployment uses a deployment tool 
to define a cluster and bootstrap a monitor. See `Deployment`_ for details 
on ``ceph-deploy.``

.. toctree::
	:maxdepth: 2

	Configuration <configuration/index>
	Deployment <deployment/index>

.. raw:: html 

	</td><td><h3>Operations</h3>

Once you have a deployed a Ceph Storage Cluster, you may begin operating 
your cluster.

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

Most Ceph deployments use `Ceph Block Devices`_, `Ceph Object Storage`_ and/or the
`Ceph Filesystem`_. You  may also develop applications that talk directly to
the Ceph Storage Cluster.

.. toctree::
	:maxdepth: 2

	APIs <api/index>
	
.. raw:: html

	</td></tr></tbody></table>

.. _Ceph Block Devices: ../rbd/rbd
.. _Ceph Filesystem: ../cephfs/
.. _Ceph Object Storage: ../radosgw/
.. _Deployment: ../rados/deployment/