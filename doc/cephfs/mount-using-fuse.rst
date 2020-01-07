========================
 Mount CephFS using FUSE
========================

Prerequisite
------------
Before mounting CephFS, ensure that the client host (where CephFS has to be
mounted and used) has a copy of the Ceph configuration file (i.e.
``ceph.conf``) and a keyring of the CephX user that has CAPS for the Ceph MDS.
Both of these files must be present on the host where the Ceph MON resides.

#. Generate a minimal conf for the client host. The conf file should be
   placed at ``/etc/ceph``::

    # on client host
    mkdir /etc/ceph
    ssh {user}@{mon-host} "sudo ceph config generate-minimal-conf" | sudo tee /etc/ceph/ceph.conf

   Alternatively, you may copy the conf file. But the method which generates
   the minimal config is usually sufficient. For more information, see
   :ref:`bootstrap-options`.

#. Ensure that the conf has appropriate permissions::

    chmod 644 /etc/ceph/ceph.conf

#. Create the CephX user and get its secret key::

    ssh {user}@{mon-host} "sudo ceph fs authorize cephfs client.foo / rw" | sudo tee /etc/ceph/ceph.client.foo.keyring

   In above command, replace ``cephfs`` with the name of your CephFS, ``foo``
   by the name you want for your CephX user and ``/`` by the path within your
   CephFS for which you want to allow access to the client host and ``rw``
   stands for both read and write permissions. Alternatively, you may copy the
   Ceph keyring from the MON host to client host at ``/etc/ceph`` but creating
   a keyring specific to the client host is better. While creating a CephX
   keyring/client, using same client name across multiple machines is perfectly
   fine.

.. note:: If you get 2 prompts for password while running above any of 2 above
   command, run ``sudo ls`` (or any other trivial command with sudo)
   immediately before these commands.

#. Ensure that the keyring has appropriate permissions::

    chmod 600 /etc/ceph/ceph.client.foo.keyring

Synopsis
--------
In general, the command to mount CephFS via FUSE looks like this::

    ceph-fuse {mountpoint} {options}

Mounting CephFS
---------------
To FUSE-mount the Ceph file system, use the ``ceph-fuse`` command::

    mkdir /mnt/mycephfs
    ceph-fuse --id foo /mnt/mycephfs

Option ``-id`` passes the name of the CephX user whose keyring we intend to
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
``--client_mds_namespace`` to mount the non-default FS::

    ceph-fuse --id foo --client_mds_namespace mycephfs2 /mnt/mycephfs2

You may also add a ``client_mds_namespace`` setting to your ``ceph.conf``

Unmounting CephFS
-----------------

Use ``umount`` to unmount CephFS like any other FS::

    umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

Persistent Mounts
-----------------

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
