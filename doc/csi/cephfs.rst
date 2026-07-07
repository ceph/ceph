.. _csi-cephfs:

======================
 CephFS with Ceph-CSI
======================

The CephFS driver (``cephfs.csi.ceph.com``) provisions shared POSIX file
systems backed by :ref:`CephFS<ceph-file-system>`. Volumes support
``ReadWriteMany`` access, so many pods on many nodes can mount the same
volume simultaneously. Each provisioned volume is a CephFS subvolume
(see :ref:`fs-volumes-and-subvolumes`).


Prepare a File System
=====================

Create a CephFS volume, or reuse an existing one:

.. prompt:: bash $

   ceph fs volume create cephfs

The driver stores provisioned volumes as subvolumes in a subvolume
group. Create the group that your StorageClass will use (the Ceph-CSI
default group name is ``csi``):

.. prompt:: bash $

   ceph fs subvolumegroup create cephfs csi


Create a cephx User
===================

Create a user for the driver, restricted to the file system and the
subvolume group created above:

.. prompt:: bash $

   ceph auth get-or-create client.csi-cephfs \
       mgr 'allow rw' \
       mon 'allow r fsname=cephfs' \
       mds 'allow r fsname=cephfs path=/volumes, allow rws fsname=cephfs path=/volumes/csi' \
       osd 'allow rwx tag cephfs metadata=cephfs, allow rw tag cephfs data=cephfs'

Record the generated key. It is stored in a Kubernetes ``Secret`` that
the StorageClass references. See :doc:`/cephfs/client-auth` for
background on CephFS authentication and the up-to-date capability
requirements in the `Ceph-CSI capabilities documentation`_.


Define a StorageClass
=====================

A minimal StorageClass for the CephFS driver looks like this:

.. code-block:: yaml

   apiVersion: storage.k8s.io/v1
   kind: StorageClass
   metadata:
     name: ceph-cephfs
   provisioner: cephfs.csi.ceph.com
   parameters:
     clusterID: <cluster id>
     fsName: cephfs
   reclaimPolicy: Delete
   allowVolumeExpansion: true

Complete examples, including the secret references that the provisioner
and node plugin require, are maintained in the `Ceph-CSI CephFS
examples`_. See :ref:`csi-deployment` for how the ``clusterID`` value
is determined.


Features
========

The CephFS driver supports volume expansion, snapshots and clones
through the standard Kubernetes APIs, and both kernel and FUSE mounters
on the nodes. See the `Ceph-CSI documentation`_ for the full feature
matrix and configuration details.

.. _Ceph-CSI capabilities documentation: https://github.com/ceph/ceph-csi/blob/devel/docs/capabilities.md
.. _Ceph-CSI CephFS examples: https://github.com/ceph/ceph-csi/tree/devel/examples/cephfs
.. _Ceph-CSI documentation: https://ceph.github.io/ceph-csi/
