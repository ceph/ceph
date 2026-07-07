.. _csi-nvmeof:

=======================
 NVMe-oF with Ceph-CSI
=======================

.. note::

   NVMe-oF support in Ceph-CSI is under active development and is not
   yet generally available. Check the `Ceph-CSI project`_ for the
   current status before planning a deployment on it.

The NVMe-oF driver exposes RBD images to containerized workloads
through the :ref:`Ceph NVMe-oF gateway<ceph-nvmeof>` rather than
through the RBD client on each node. Worker nodes attach volumes with
the standard NVMe/TCP initiator that ships with the Linux kernel, so,
as with the NFS driver, no Ceph client code is needed on the nodes.
This suits environments that standardize on NVMe/TCP for storage
access or that cannot run RBD kernel clients.


Prepare the Gateway
===================

The driver requires a running NVMe-oF gateway group in the Ceph
cluster. See :ref:`ceph-nvmeof` for the gateway architecture, hardware
sizing, and deployment instructions, and :doc:`/rbd/nvmeof-requirements`
for gateway requirements.

The gateway exposes RBD images from a pool, so the Ceph-side pool
preparation matches the RBD driver: see :ref:`csi-rbd` for creating and
initializing a pool.

.. _Ceph-CSI project: https://github.com/ceph/ceph-csi
