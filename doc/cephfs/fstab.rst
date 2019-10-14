========================================
 Mount CephFS in your File Systems Table
========================================

If you mount CephFS in your file systems table, the Ceph file system will
mount automatically on startup.

Kernel Driver
=============

To mount CephFS in your file systems table as a kernel driver, add the
following to ``/etc/fstab``::

	[{ipaddress}:{port}]:/ {mount}/{mountpoint} ceph [name=username,secret=secretkey|secretfile=/path/to/secretfile],[{mount.options}]

For example::

	:/     /mnt/mycephfs    ceph    name=admin,noatime,_netdev    0       2
	
The default for the ``name=`` parameter is ``guest``. If the ``secret`` or
``secretfile`` options are not specified then the mount helper will attempt to
find a secret for the given ``name`` in one of the configured keyrings.

FUSE
====

To mount CephFS in your file systems table as a file system in user space, add
the following to ``/etc/fstab``::

       #DEVICE PATH       TYPE      OPTIONS
       none    /mnt/mycephfs  fuse.ceph ceph.id={user-ID}[,ceph.conf={path/to/conf.conf}],_netdev,defaults  0 0

For example::

       none    /mnt/mycephfs  fuse.ceph ceph.id=myuser,_netdev,defaults  0 0
       none    /mnt/mycephfs  fuse.ceph ceph.id=myuser,ceph.conf=/etc/ceph/foo.conf,_netdev,defaults  0 0

Ensure you use the ID (e.g., ``admin``, not ``client.admin``). You can pass
any valid ``ceph-fuse`` option to the command line this way.

``ceph-fuse@.service`` and ``ceph-fuse.target`` systemd units are available.
As usual, these unit files declare the default dependencies and recommended
execution context for ``ceph-fuse``. For example, after making the fstab entry
shown above, ``ceph-fuse`` run following commands::

    systemctl start ceph-fuse@-mnt-mycephfs.service
    systemctl enable ceph-fuse@-mnt-mycephfs.service

See `User Management`_ for details.


.. _User Management: ../../rados/operations/user-management/
