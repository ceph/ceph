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

	10.10.10.10:6789:/     /mnt/ceph    ceph    name=admin,secretfile=/etc/ceph/secret.key,noatime,_netdev    0       2
	
.. important:: The ``name`` and ``secret`` or ``secretfile`` options are 
   mandatory when you have Ceph authentication running. 
 
See `User Management`_ for details.
   
   
FUSE
====

To mount Ceph FS in your file systems table as a filesystem in user space, add the
following to ``/etc/fstab``::

       #DEVICE PATH       TYPE      OPTIONS
       none    /mnt/ceph  fuse.ceph ceph.id={user-ID}[,ceph.conf={path/to/conf.conf}],_netdev,defaults  0 0

For example::

       none    /mnt/ceph  fuse.ceph ceph.id=myuser,_netdev,defaults  0 0
       none    /mnt/ceph  fuse.ceph ceph.id=myuser,ceph.conf=/etc/ceph/foo.conf,_netdev,defaults  0 0

Ensure you use the ID (e.g., ``admin``, not ``client.admin``). You can pass any valid 
``ceph-fuse`` option to the command line this way.

See `User Management`_ for details.


.. _User Management: ../../rados/operations/user-management/
