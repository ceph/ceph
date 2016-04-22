==========================================
 Mount Ceph FS in your File Systems Table
==========================================

If you mount Ceph FS in your file systems table, the Ceph file system will mount 
automatically on startup. 

Kernel Driver
=============

To mount Ceph FS in your file systems table as a kernel driver, add the
following to ``/etc/fstab``::

	{ipaddress}:{port}:/ {mount}/{mountpoint} {filesystem-name}	[name=username,secret=secretkey|secretfile=/path/to/secretfile],[{mount.options}]

For example:: 

	10.10.10.10:3300:/     /mnt/ceph    ceph    name=admin,secretfile=/etc/ceph/secret.key,noatime    0       2
	
.. important:: The ``name`` and ``secret`` or ``secretfile`` options are 
   mandatory when you have Ceph authentication running. 
 
See `Authentication`_ for details. 
   
   
FUSE
====

To mount Ceph FS in your file systems table as a filesystem in user space, add the
following to ``/etc/fstab``::

	#DEVICE                                  PATH         TYPE      OPTIONS
	id={user-ID}[,conf={path/to/conf.conf}] /mount/path  fuse.ceph defaults 0 0

For example::

	id=admin  /mnt/ceph  fuse.ceph defaults 0 0 
	id=myuser,conf=/etc/ceph/cluster.conf  /mnt/ceph2  fuse.ceph defaults 0 0 

The ``DEVICE`` field is a comma-delimited list of options to pass to the command line.
Ensure you use the ID (e.g., ``admin``, not ``client.admin``). You can pass any valid 
``ceph-fuse`` option to the command line this way.

See `Authentication`_ for details. 


.. _Authentication: ../../rados/operations/authentication/
