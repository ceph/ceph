.. _csi-rbd:

===================
 RBD with Ceph-CSI
===================

The RBD driver (``rbd.csi.ceph.com``) provisions
:ref:`Ceph Block Device<ceph_block_device>` images and maps them on
the nodes that run the pods
which consume them. Volumes can be mounted with a file system in
``ReadWriteOnce`` mode, or attached as raw block devices, including
``ReadWriteMany`` raw block for clustered applications that coordinate
their own access.


Prepare a Pool
==============

Create a pool for container volumes and initialize it for use by RBD:

.. prompt:: bash $

   ceph osd pool create kubernetes
   rbd pool init kubernetes

See :ref:`rados_pools` for guidance on pool creation, including
placement group counts.


Create a cephx User
===================

Create a user for the driver, restricted to the pool created above. The
same user is used for provisioning and for mapping images on the nodes:

.. prompt:: bash $

   ceph auth get-or-create client.csi-rbd \
       mon 'profile rbd' \
       osd 'profile rbd pool=kubernetes' \
       mgr 'profile rbd pool=kubernetes'

Record the generated key. It is stored in a Kubernetes ``Secret`` that
the StorageClass references. See :ref:`user-management` for background
on cephx capabilities and the up-to-date capability requirements in the
`Ceph-CSI capabilities documentation`_.


Define a StorageClass
=====================

A minimal StorageClass for the RBD driver looks like this:

.. code-block:: yaml

   apiVersion: storage.k8s.io/v1
   kind: StorageClass
   metadata:
     name: ceph-rbd
   provisioner: rbd.csi.ceph.com
   parameters:
     clusterID: <cluster id>
     pool: kubernetes
     imageFeatures: layering
   reclaimPolicy: Delete
   allowVolumeExpansion: true

Complete examples, including the secret references that the provisioner
and node plugin require, are maintained in the `Ceph-CSI RBD examples`_.
See :ref:`csi-deployment` for how the ``clusterID`` value is
determined.

.. important::

   The driver maps images with the RBD kernel client by default, which
   may not support all :ref:`CRUSH tunables<crush-map-tunables>` or RBD
   image features. ``imageFeatures: layering`` is a safe baseline;
   consult the canonical documentation before enabling more features.


Features
========

The RBD driver supports snapshots and clones through the standard
Kubernetes APIs, volume expansion, topology-aware provisioning, and
RBD mirroring for disaster recovery through `CSI-Addons`_. Feature
maturity varies; see the feature matrix in the `Ceph-CSI
documentation`_ for the current status and configuration details.

.. _Ceph-CSI capabilities documentation: https://github.com/ceph/ceph-csi/blob/devel/docs/capabilities.md
.. _Ceph-CSI RBD examples: https://github.com/ceph/ceph-csi/tree/devel/examples/rbd
.. _CSI-Addons: https://github.com/csi-addons/kubernetes-csi-addons
.. _Ceph-CSI documentation: https://ceph.github.io/ceph-csi/
