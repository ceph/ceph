======================================
 Mount Ceph FS with the Kernel Driver
======================================

To mount the Ceph file system you may use the ``mount`` command if you know the
monitor host IP address(es), or use the ``mount.ceph`` utility to resolve the 
monitor host name(s) into IP address(es) for you. For example:: 

	sudo mkdir /mnt/mycephfs
	sudo mount -t ceph 192.168.0.1:3300:/ /mnt/mycephfs

To mount the Ceph file system with ``cephx`` authentication enabled, you must
specify a user name and a secret. ::

	sudo mount -t ceph 192.168.0.1:3300:/ /mnt/mycephfs -o name=admin,secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

The foregoing usage leaves the secret in the Bash history. A more secure
approach reads the secret from a file. For example::

	sudo mount -t ceph 192.168.0.1:3300:/ /mnt/mycephfs -o name=admin,secretfile=/etc/ceph/admin.secret
	
See `Authentication`_ for details on cephx.

To unmount the Ceph file system, you may use the ``umount`` command. For example:: 

	sudo umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

See `mount.ceph`_ for details.

.. _mount.ceph: ../../man/8/mount.ceph/
.. _Authentication: ../../rados/operations/authentication/
