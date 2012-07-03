=====================
 Ceph FS Quick Start
=====================

To mount the Ceph FS filesystem, you must have a running Ceph cluster. You may 
execute this quick start on a separate host if you have the Ceph packages and 
the ``/etc/ceph/ceph.conf`` file installed with the appropriate IP address
and host name settings modified in the ``/etc/ceph/ceph.conf`` file.

Kernel Driver
-------------

Mount Ceph FS as a kernel driver. :: 

	sudo mkdir /mnt/mycephfs
	sudo mount -t ceph {ip-address-of-monitor}:6789:/ /mnt/mycephfs
	
Filesystem in User Space (FUSE)
-------------------------------

Mount Ceph FS as with FUSE. Replace {username} with your username. ::

	sudo mkdir /home/{username}/cephfs
	sudo ceph-fuse -m {ip-address-of-monitor}:6789 /home/{username}/cephfs
