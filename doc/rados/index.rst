.. _rados-index:

======================
 Ceph Storage Cluster
======================

The :term:`Ceph Storage Cluster` is the foundation for all Ceph deployments.
Based upon :abbr:`RADOS (Reliable Autonomic Distributed Object Store)`, Ceph
Storage Clusters consist of several types of daemons: 

- a :term:`Ceph OSD Daemon` (OSD) stores data as objects on a storage node
- a :term:`Ceph Monitor` (MON) maintains a master copy of the cluster map
- a :term:`Ceph Manager` daemon
       
A Ceph Storage Cluster may comprise thousands of storage nodes. A minimal
system runs at least one Ceph Monitor, one Ceph Manager, and as many Ceph OSD
Daemons as the number of object replicas or shards in its pools (three for the
default pool size). See :ref:`ceph-cluster-components`.

The Ceph File System, Ceph Object Storage and Ceph Block Devices read data from
and write data to the Ceph Storage Cluster.

.. container:: columns-3

   .. container:: column

      .. raw:: html

          <h3>Config and Deploy</h3>

      Ceph Storage Clusters have a few required settings, but most configuration
      settings have default values. A typical deployment uses a deployment tool
      to define a cluster and bootstrap a monitor. See :ref:`cephadm` for details.

      .. toctree::
         :maxdepth: 2

         Configuration <configuration/index>

   .. container:: column

      .. raw:: html

          <h3>Operations</h3>

      Once you have deployed a Ceph Storage Cluster, you may begin operating
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

   .. container:: column

      .. raw:: html

          <h3>APIs</h3>

      Most Ceph deployments use `Ceph Block Devices`_, `Ceph Object Storage`_ and/or the
      `Ceph File System`_. You may also develop applications that talk directly to
      the Ceph Storage Cluster.

      .. toctree::
         :maxdepth: 2

         APIs <api/index>

.. _Ceph Block Devices: ../rbd/
.. _Ceph File System: ../cephfs/
.. _Ceph Object Storage: ../radosgw/
.. _Deployment: ../cephadm/
