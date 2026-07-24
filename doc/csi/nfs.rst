.. _csi-nfs:

===================
 NFS with Ceph-CSI
===================

The NFS driver (``nfs.csi.ceph.com``) provisions CephFS subvolumes and
exports them through an NFS-Ganesha cluster that is managed by the Ceph
:ref:`NFS manager module<mgr-nfs>`. Exports are created and deleted
dynamically as volumes are provisioned and destroyed. Pods mount the
volumes with the ordinary NFS client, so the worker nodes need no Ceph
client code at all. This makes the NFS driver a good fit for platforms
where installing Ceph-specific components on every node is not
practical.


Prepare a File System and an NFS Cluster
========================================

The NFS driver builds on the CephFS driver: provision a CephFS volume
and a cephx user as described in :ref:`csi-cephfs`. Then create an NFS
cluster to serve the exports:

.. prompt:: bash $

   ceph nfs cluster create mynfs

Confirm that the cluster is up and note the address that clients will
mount from:

.. prompt:: bash $

   ceph nfs cluster info mynfs

See :ref:`mgr-nfs` for placement options, high availability with an
ingress service, and the rest of the NFS cluster management commands.


Define a StorageClass
=====================

A minimal StorageClass for the NFS driver looks like this:

.. code-block:: yaml

   apiVersion: storage.k8s.io/v1
   kind: StorageClass
   metadata:
     name: ceph-nfs
   provisioner: nfs.csi.ceph.com
   parameters:
     clusterID: <cluster id>
     fsName: cephfs
     nfsCluster: mynfs
     server: <NFS cluster address>
   reclaimPolicy: Delete
   allowVolumeExpansion: true

Complete examples, including the secret references that the provisioner
requires, are maintained in the `Ceph-CSI NFS examples`_. See
:ref:`csi-deployment` for how the ``clusterID`` value is determined.

.. note::

   The worker nodes need NFS client support in the kernel. No Ceph
   packages are required on the nodes.

.. _Ceph-CSI NFS examples: https://github.com/ceph/ceph-csi/tree/devel/examples/nfs
