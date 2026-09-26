.. _quick-rbd:

========================================
 Creating and Mounting a Block Device
========================================

.. meta::
   :description: Create a Ceph block device image, map it on a client, and mount a file system on it.
   :ceph-page-type: procedure

This procedure creates a :term:`Ceph Block Device` (RBD) image, maps it on a
Linux client, and mounts a file system on it. Use it to try block storage on a
new cluster. For day-to-day image management, see :ref:`ceph_block_device`.

Prerequisites
=============

- A running Ceph cluster that reports ``HEALTH_OK``.
- A Linux client host that has the ``ceph-common`` package installed, and a
  copy of ``/etc/ceph/ceph.conf`` and of a :term:`keyring<Keyring>` that is
  allowed to use the cluster. On a cephadm bootstrap host both files are in
  ``/etc/ceph``; the keyring is ``ceph.client.admin.keyring``.
- A client host that is not also a host of the Ceph cluster, unless the client
  is a virtual machine. Mapping a block device with the kernel client on a
  host that runs Ceph daemons can cause a deadlock.

Procedure
=========

#. On a cluster host, inside ``cephadm shell``, create a :term:`pool<Pools>`
   for block device images. This example uses the pool name ``rbd1701``:

   .. prompt:: bash #

      ceph osd pool create rbd1701

   The command prints ``pool 'rbd1701' created``.

#. Initialize the pool for use by RBD:

   .. prompt:: bash #

      rbd pool init rbd1701

   The command prints nothing on success.

#. On the client host, create a 1 GB image:

   .. prompt:: bash #

      rbd create rbd1701/image1701 --size 1G

   The command prints nothing. ``rbd ls rbd1701`` lists ``image1701``.

#. Map the image to a block device:

   .. prompt:: bash #

      rbd map rbd1701/image1701

   The command prints the device name, for example ``/dev/rbd0``. The next
   steps use the link ``/dev/rbd/rbd1701/image1701`` that the ``ceph-common``
   udev rule creates. If that link does not exist, use the ``/dev/rbdN`` name
   instead.

#. Create a file system on the device:

   .. prompt:: bash #

      mkfs.ext4 /dev/rbd/rbd1701/image1701

   The output ends with ``Writing superblocks and filesystem accounting
   information: done``.

#. Create a mount point:

   .. prompt:: bash #

      mkdir /mnt/ceph-block-device

#. Mount the file system:

   .. prompt:: bash #

      mount /dev/rbd/rbd1701/image1701 /mnt/ceph-block-device

   The command prints nothing on success.

Verification
============

- List the mapped images:

  .. prompt:: bash #

     rbd device list

  The output shows ``image1701`` in pool ``rbd1701`` and the device that it is
  mapped to.

- Confirm that the file system is mounted:

  .. prompt:: bash #

     df -h /mnt/ceph-block-device

  The output shows the ``/dev/rbd`` device mounted on
  ``/mnt/ceph-block-device`` with about 1 GB of space.

Troubleshooting
===============

- If ``rbd map`` reports ``RBD image feature set mismatch``, the kernel on the
  client does not support every feature that is enabled on the image. Run the
  ``rbd feature disable`` command that the error message suggests, then map
  the image again.
- If ``rbd map`` reports ``rbd: map failed: (2) No such file or directory`` or
  ``modprobe: FATAL: Module rbd not found``, the client kernel has no ``rbd``
  module. Run ``modprobe rbd``, then map the image again.
- If ``mkfs.ext4`` or ``mount`` reports ``No such file or directory`` for
  ``/dev/rbd/rbd1701/image1701``, the udev link was not created. Use the
  ``/dev/rbdN`` device name that ``rbd map`` printed.
- If ``rbd`` commands hang or report an authentication error, check that
  ``/etc/ceph/ceph.conf`` and the keyring on the client are copies of the
  files on the cluster. Then check that the client can reach the Monitors
  over the network.

Next Steps
==========

- Map the image automatically at boot. See the :ref:`rbdmap manpage <rbdmap>`.
- Create a Ceph user for block device clients instead of using the ``admin``
  user. See :doc:`/rbd/rados-rbd-cmds`.

Additional Resources
====================

- :ref:`ceph_block_device`
- :ref:`Pools <rados_pools>`
- :doc:`/rbd/rbd-ko`
