=================
 RBD Quick Start
=================

To use RADOS block devices, you must have a running Ceph cluster. You may 
execute this quick start on a separate host if you have the Ceph packages and 
the ``/etc/ceph/ceph.conf`` file installed with the appropriate IP address
and host name settings modified in the ``/etc/ceph/ceph.conf`` file.

Create a RADOS Block Device image. :: 

	rbd create foo --size 4096	

Load the ``rbd`` client module. ::

	sudo modprobe rbd

Map the image to a block device. :: 

	sudo rbd map foo --pool rbd --name client.admin
	
Use the block device. In the following example, create a file system. :: 

	sudo mkfs.ext4 -m0 /dev/rbd/rbd/foo
	
Mount the file system. ::

	sudo mkdir /mnt/myrbd
	sudo mount /dev/rbd/rbd/foo /mnt/myrbd
