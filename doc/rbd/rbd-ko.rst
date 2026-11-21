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
image name. The pool name defaults to ``rbd`` and the user to ``client.admin``.
``rbd`` will load RBD kernel module on your behalf if it's not already
loaded. ::

  sudo rbd device map {pool-name}/{image-name} --id {user-name}

For example:: 

  sudo rbd device map rbd/myimage --id admin
 
If you use :ref:`CephX <user-management>` authentication and the key is not in a
keyring at a default path, you must also specify a secret.  It may come
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

