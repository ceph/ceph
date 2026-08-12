==============================
 Block Devices and Kubernetes
==============================

Ceph Block Device images are provisioned for Kubernetes by `Ceph-CSI`_,
which dynamically creates RBD images to back Kubernetes `volumes`_ and
maps them as block devices on the worker nodes that run `pods`_
referencing the volumes.

The documentation for using Ceph with Kubernetes and other container
platforms has moved to its own chapter:

- :ref:`ceph-csi` introduces Ceph-CSI and its storage backends.
- :ref:`csi-deployment` covers deployment, including the recommended
  Ceph-CSI-Operator.
- :ref:`csi-rbd` covers the Ceph-side preparation for RBD volumes:
  pools, cephx users, and StorageClass essentials.

CephFS, NFS, and NVMe-oF backends are covered in the same chapter.

.. note::

   Earlier versions of this page walked through deploying Ceph-CSI by
   hand-editing raw manifests. That deployment method is superseded by
   the Ceph-CSI-Operator and Helm charts described in
   :ref:`csi-deployment`; the manifests remain available in the
   `Ceph-CSI repository`_.

.. _Ceph-CSI: https://github.com/ceph/ceph-csi/
.. _Ceph-CSI repository: https://github.com/ceph/ceph-csi/tree/devel/deploy
.. _volumes: https://kubernetes.io/docs/concepts/storage/volumes/
.. _pods: https://kubernetes.io/docs/concepts/workloads/pods/
