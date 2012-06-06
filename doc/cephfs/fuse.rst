=========================
 Mount Ceph FS as a FUSE
=========================
To mount the Ceph file system as a File System in User Space (FUSE), you may 
use the ``ceph-fuse`` command. For example:: 

	sudo mkdir /home/usernname/cephfs
	sudo ceph-fuse -m 192.168.0.1:6789 /home/username/cephfs

If ``cephx`` authentication is on, ``ceph-fuse`` will retrieve the name and 
secret from the key ring automatically.

See `ceph-fuse`_ for details.

.. _ceph-fuse: ../../man/8/ceph-fuse/