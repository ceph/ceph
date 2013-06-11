==========================
 Block Device Quick Start
==========================

To use this guide, you must have executed the procedures in the `Object Store
Quick Start`_ guide first. Ensure your :term:`Ceph Storage Cluster` is in an
``active + clean`` state before working with the :term:`Ceph Block Device`.
Execute this quick start on the admin node.

.. note:: The Ceph Block Device is also known as :term:`RBD` or :term:`RADOS`
   Block Device.

#. Install ``ceph-common``. ::

	sudo apt-get install ceph-common

#. Create a block device image. :: 

	rbd create foo --size 4096	[-m {mon-IP}] [-k /path/to/ceph.client.admin.keyring]

#. Load the ``rbd`` client module. ::

	sudo modprobe rbd

#. Map the image to a block device. :: 

	sudo rbd map foo --pool rbd --name client.admin [-m {mon-IP}] [-k /path/to/ceph.client.admin.keyring]
	
#. Use the block device. In the following example, create a file system. :: 

	sudo mkfs.ext4 -m0 /dev/rbd/rbd/foo
	
	This may take a few moments.
	
#. Mount the file system. ::

	sudo mkdir /mnt/ceph-block-device
	sudo mount /dev/rbd/rbd/foo /mnt/ceph-block-device
	cd /mnt/ceph-block-device

.. note:: Mount the block device on the client machine, 
   not the server machine. See `FAQ`_ for details.

See `block devices`_ for additional details.

.. _Object Store Quick Start: ../quick-ceph-deploy
.. _block devices: ../../rbd/rbd
.. _FAQ: http://wiki.ceph.com/03FAQs/01General_FAQ#How_Can_I_Give_Ceph_a_Try.3F
