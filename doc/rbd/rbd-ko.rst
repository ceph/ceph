==============================
 RBD Kernel Object Operations
==============================

.. important:: To use kernel object operations, you must have a running Ceph cluster.

Load the Ceph RBD Module
========================

To map an RBD image to a kernel object, first load the Ceph RBD module:: 

	modprobe rbd

Get a List of RBD Images
========================

To mount an RBD image, first return a list of the images. ::

	rbd list

Map a Block Device
==================

Use ``rbd`` to map an image name to a kernel object. You must specify the 
image name, the pool name, and the client name. If you use ``cephx`` 
authentication, you must also specify a secret. ::

  sudo rbd map {image-name} --pool {pool-name} --name {client-name}

For example:: 

  sudo rbd map foo --pool rbd myimage --name client.admin
 
If you use ``cephx`` authentication, you must also specify a secret.  It may come from a keyring, a file containing the secret, or be specified explicitly on the command line. ::

  sudo rbd map foo --pool rbd myimage --name client.foo --keyring /path/to/keyring
  sudo rbd map foo --pool rbd myimage --name client.foo --keyfile /path/to/file


Show Mapped Block Devices
=========================

To show RBD images mapped to kernel block devices with the ``rbd`` command, 
specify the ``showmapped`` option. ::

	sudo rbd showmapped


Unmapping a Block Device
========================	

To unmap an RBD image with the ``rbd`` command, specify the ``rm`` option 
and the device name (i.e., by convention the same as the RBD image name). :: 

	sudo rbd unmap /dev/rbd/{poolname}/{imagename}
	
For example::

	sudo rbd unmap /dev/rbd/rbd/foo
