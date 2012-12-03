==========================
 Kernel Module Operations
==========================

.. important:: To use kernel module operations, you must have a running Ceph cluster.

Load the Ceph RBD Module
========================

To map a block device image to a kernel module, first load the Ceph RBD module:: 

	modprobe rbd

Get a List of Images
====================

To mount a block device image, first return a list of the images. ::

	rbd list

Map a Block Device
==================

Use ``rbd`` to map an image name to a kernel module. You must specify the 
image name, the pool name, and the user name. ::

  sudo rbd map {image-name} --pool {pool-name} --id {user-name}

For example:: 

  sudo rbd map foo --pool rbd myimage --id admin
 
If you use `cephx`_ authentication, you must also specify a secret.  It may come
from a keyring or a file containing the secret. ::

  sudo rbd map foo --pool rbd myimage --id admin --keyring /path/to/keyring
  sudo rbd map foo --pool rbd myimage --id admin --keyfile /path/to/file


Show Mapped Block Devices
=========================

To show block device images mapped to kernel modules with the ``rbd`` command,
specify the ``showmapped`` option. ::

	sudo rbd showmapped


Unmapping a Block Device
========================	

To unmap a block device image with the ``rbd`` command, specify the ``rm``
option  and the device name (i.e., by convention the same as the block device
image name). :: 

	sudo rbd unmap /dev/rbd/{poolname}/{imagename}
	
For example::

	sudo rbd unmap /dev/rbd/rbd/foo


.. _cephx: ../../rados/operations/authentication/