========================
 Mount CephFS using FUSE
========================

`ceph-fuse`_ is an alternate way of mounting CephFS, although it mounts it
in userspace. Therefore, performance of FUSE can be relatively lower but FUSE
clients can be more manageable, especially while upgrading CephFS.

Prerequisites
=============

Go through the prerequisites required by both, kernel as well as FUSE mounts,
in `Mount CephFS: Prerequisites`_ page.

.. note:: Mounting CephFS using FUSE requires superuser privileges to trim dentries
   by issuing a remount of itself.

Synopsis
========
In general, the command to mount CephFS via FUSE looks like this::

    ceph-fuse {mountpoint} {options}

Mounting CephFS
===============
To FUSE-mount the Ceph file system, use the ``ceph-fuse`` command::

    mkdir /mnt/mycephfs
    ceph-fuse --id foo /mnt/mycephfs

Option ``--id`` passes the name of the CephX user whose keyring we intend to
use for mounting CephFS. In the above command, it's ``foo``. You can also use
``-n`` instead, although ``--id`` is evidently easier::

    ceph-fuse -n client.foo /mnt/mycephfs

In case the keyring is not present in standard locations, you may pass it
too::

    ceph-fuse --id foo -k /path/to/keyring /mnt/mycephfs

You may pass the MON's socket too, although this is not mandatory::

    ceph-fuse --id foo -m 192.168.0.1:6789 /mnt/mycephfs

You can also mount a specific directory within CephFS instead of mounting
root of CephFS on your local FS::

    ceph-fuse --id foo -r /path/to/dir /mnt/mycephfs

If you have more than one FS on your Ceph cluster, use the option
``--client_fs`` to mount the non-default FS::

    ceph-fuse --id foo --client_fs mycephfs2 /mnt/mycephfs2

You may also add a ``client_fs`` setting to your ``ceph.conf``. Alternatively, the option
``--client_mds_namespace`` is supported for backward compatibility.

Unmounting CephFS
=================

Use ``umount`` to unmount CephFS like any other FS::

    umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

Persistent Mounts
=================

To mount CephFS as a file system in user space, add the following to ``/etc/fstab``::

       #DEVICE PATH       TYPE      OPTIONS
       none    /mnt/mycephfs  fuse.ceph ceph.id={user-ID}[,ceph.conf={path/to/conf.conf}],_netdev,defaults  0 0

For example::

       none    /mnt/mycephfs  fuse.ceph ceph.id=myuser,_netdev,defaults  0 0
       none    /mnt/mycephfs  fuse.ceph ceph.id=myuser,ceph.conf=/etc/ceph/foo.conf,_netdev,defaults  0 0

Ensure you use the ID (e.g., ``myuser``, not ``client.myuser``). You can pass
any valid ``ceph-fuse`` option to the command line this way.

To mount a subdirectory of the CephFS, add the following to ``/etc/fstab``::

       none    /mnt/mycephfs  fuse.ceph ceph.id=myuser,ceph.client_mountpoint=/path/to/dir,_netdev,defaults  0 0

``ceph-fuse@.service`` and ``ceph-fuse.target`` systemd units are available.
As usual, these unit files declare the default dependencies and recommended
execution context for ``ceph-fuse``. After making the fstab entry shown above,
run following commands::

    systemctl start ceph-fuse@/mnt/mycephfs.service
    systemctl enable ceph-fuse.target
    systemctl enable ceph-fuse@-mnt-mycephfs.service

See :ref:`User Management <user-management>` for details on CephX user management and `ceph-fuse`_
manual for more options it can take. For troubleshooting, see
:ref:`ceph_fuse_debugging`.

.. _ceph-fuse: ../../man/8/ceph-fuse/#options
.. _Mount CephFS\: Prerequisites: ../mount-prerequisites
