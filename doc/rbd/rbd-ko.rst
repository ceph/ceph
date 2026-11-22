==========================
 Kernel Module Operations
==========================

.. index:: Ceph Block Device; kernel module

.. important:: To use kernel module operations, you must have a running Ceph cluster.

Get a List of Images
====================

To mount a block device image, first return a list of the images. ::

	rbd list

Map a Block Device
==================

Use ``rbd`` to map an image name to a kernel module. You must specify the 
image name, the pool name, and the user name. ``rbd`` will load RBD kernel
module on your behalf if it's not already loaded. ::

  sudo rbd device map {pool-name}/{image-name} --id {user-name}

For example:: 

  sudo rbd device map rbd/myimage --id admin
 
If you use :ref:`CephX <user-management>` authentication, you must also specify a secret.  It may come
from a keyring or a file containing the secret. ::

  sudo rbd device map rbd/myimage --id admin --keyring /path/to/keyring
  sudo rbd device map rbd/myimage --id admin --keyfile /path/to/file


Show Mapped Block Devices
=========================

To show block device images mapped to kernel modules with the ``rbd``,
specify ``device list`` arguments. ::

	rbd device list


Unmapping a Block Device
========================

To unmap a block device image with the ``rbd`` command, specify the
``device unmap`` arguments and the device name (i.e., by convention the
same as the block device image name). :: 

	sudo rbd device unmap /dev/rbd/{poolname}/{imagename}

For example::

	sudo rbd device unmap /dev/rbd/rbd/foo

Troubleshooting a Failed Map
============================

``rbd device map`` fails when the client kernel lacks a feature that the image
or the cluster requires. The two cases fail differently.

Image Features
--------------

If the kernel does not support a feature that is enabled on the image, the map
fails at once. The error names the feature and the command that disables it::

  rbd: sysfs write failed
  RBD image feature set mismatch. You can disable features unsupported by the kernel with "rbd feature disable mypool/myimg journaling".
  In some cases useful info is found in syslog - try "dmesg | tail".
  rbd: map failed: (6) No such device or address

Disabling a feature turns it off for every client of the image, so check first
that nothing depends on it. For example, disabling ``journaling`` deletes the
image's journal, and in a pool whose mirror mode is ``pool`` it also stops
mirroring the image.

Then disable the feature and map the image again:

.. prompt:: bash $

   rbd feature disable mypool/myimg journaling

Cluster Features
----------------

If the kernel lacks a feature that the cluster requires of its clients, for
example support for the cluster's CRUSH tunables, the Monitors refuse the
connection. The client keeps retrying, so the map appears to hang for about 60
seconds before it fails::

  rbd: sysfs write failed
  In some cases useful info is found in syslog - try "dmesg | tail".
  rbd: map failed: (110) Connection timed out

The kernel log names the missing feature bits:

.. prompt:: bash $

   sudo dmesg | tail

::

  libceph: mon0 (1)192.168.1.1:40101 feature set mismatch, my 2f018fb86aa42ada < server's 2f018fb8faa42ada, missing 90000000
  libceph: mon0 (1)192.168.1.1:40101 missing required protocol features

To map the image, upgrade the client kernel to one that has the missing
feature. In this example, ``missing 90000000`` is bits 31 and 28, the part of
the ``SERVER_REEF`` feature mask that the kernel lacks (see
``src/include/ceph_features.h``). The cluster requires ``SERVER_REEF`` of its
clients while the OSD map holds ``pg-upmap-primary`` mappings, so removing the
mappings also lets the kernel connect. See :ref:`read_balancer`.

If the missing feature comes from the CRUSH tunables, you can instead set a
tunables profile that the kernel supports, but this moves data. See
:ref:`crush-map-tunables`.
