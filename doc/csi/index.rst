.. _ceph-csi:

=========================================
 Ceph Container Storage Interface (CSI)
=========================================

`Ceph-CSI <https://github.com/ceph/ceph-csi>`__ implements the
`Container Storage Interface`_ specification for Ceph storage.
Container orchestrators such as Kubernetes use it to dynamically
provision Ceph volumes and to attach those volumes to workloads.
Kubernetes clusters that dynamically provision Ceph volumes rely on
Ceph-CSI, whether Ceph runs inside the cluster under `Rook`_ or as an
external cluster. Other CSI-capable platforms, including HashiCorp
Nomad, can use Ceph-CSI as well.

.. ditaa::

            +---------------------------------------------------+
            |         Kubernetes and other CSI platforms        |
            +---------------------------------------------------+
            |                     Ceph CSI                      |
            +------------------------+--------------------------+
                                     |
                                     | provisions and maps
                                     v
            +---------------------------------------------------+
            |                    Ceph Cluster                   |
            +---------------------------------------------------+


Storage Backends
================

Ceph-CSI ships several drivers in a single binary. Each driver exposes a
different Ceph interface to containerized workloads:

.. list-table::
   :header-rows: 1
   :widths: 15 35 50

   * - Driver
     - Access model
     - Typical use
   * - RBD
     - Block volumes. ``ReadWriteOnce`` file systems, ``ReadWriteMany``
       raw block.
     - Databases, virtual machines, and most single-writer workloads.
       See :ref:`csi-rbd`.
   * - CephFS
     - Shared POSIX file system. ``ReadWriteMany``.
     - Workloads that share data across many pods. See :ref:`csi-cephfs`.
   * - NFS
     - CephFS subvolumes exported over NFS.
     - Clients that cannot run Ceph client code. See :ref:`csi-nfs`.
   * - NVMe-oF
     - Block volumes over standard NVMe/TCP initiators.
     - Under active development. See :ref:`csi-nvmeof`.


Where the Documentation Lives
=============================

Ceph-CSI is developed and released independently of Ceph itself, and one
Ceph-CSI release supports multiple Ceph releases. For that reason the
canonical driver documentation is maintained by the Ceph-CSI project and
published at `ceph.github.io/ceph-csi`_.

The pages in this chapter cover the Ceph side of a Ceph-CSI deployment:
preparing pools, file systems, and cephx users, and choosing a deployment
method. For driver internals, complete example manifests, version
compatibility, and upgrade procedures, follow the links to the canonical
documentation in each page.

.. toctree::
   :maxdepth: 1

   Deployment <deployment>
   RBD <rbd>
   CephFS <cephfs>
   NFS <nfs>
   NVMe-oF <nvmeof>

.. _Container Storage Interface: https://github.com/container-storage-interface/spec
.. _Rook: https://rook.io
.. _ceph.github.io/ceph-csi: https://ceph.github.io/ceph-csi/
