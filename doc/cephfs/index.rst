=========
 Ceph FS
=========

The Ceph FS file system is a POSIX-compliant file system that uses a RADOS
cluster to store its data. Ceph FS uses the same RADOS object storage device 
system as RADOS block devices and RADOS object stores such as the RADOS gateway
with its S3 and Swift APIs, or native bindings. Using Ceph FS requires at least 
one metadata server in your ``ceph.conf`` configuration file. 

.. toctree:: 

	Mount Ceph FS<kernel>
	Mount Ceph FS as FUSE <fuse>
	Mount Ceph FS in ``fstab`` <fstab>