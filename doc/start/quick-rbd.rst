==========================
 Block Device Quick Start
==========================

To use this guide, you must have executed the procedures in the `5-minute
Quick Start`_ guide first. Execute this quick start on the client machine.

#. Create a block device image. :: 

	rbd create foo --size 4096	

#. Load the ``rbd`` client module. ::

	sudo modprobe rbd

#. Map the image to a block device. :: 

	sudo rbd map foo --pool rbd --name client.admin
	
#. Use the block device. In the following example, create a file system. :: 

	sudo mkfs.ext4 -m0 /dev/rbd/rbd/foo
	
#. Mount the file system. ::

	sudo mkdir /mnt/myrbd
	sudo mount /dev/rbd/rbd/foo /mnt/myrbd

.. note:: Mount the block device on the client machine, 
   not the server machine. See `FAQ`_ for details.

See `block devices`_ for additional details.

.. _5-minute Quick Start: ../quick-start
.. _block devices: ../../rbd/rbd
.. _FAQ: ../../faq#try-ceph
