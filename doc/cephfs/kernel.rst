=================================
 Mount CephFS using Kernel Driver
=================================

Prerequisite
------------
Before mounting CephFS, copy the Ceph configuration file and keyring for the
CephX user that has CAPS to mount MDS to the client host (where CephFS will be
mounted and used) from the host where Ceph Monitor resides. Please note that
it's possible to mount CephFS without conf and keyring, but in that case, you
would have to pass the MON's socket and CephX user's secret key manually to
every mount command you run.

#. Generate a minimal conf file for the client host and place it at a
   standard location::

    # on client host
    mkdir /etc/ceph
    ssh {user}@{mon-host} "sudo ceph config generate-minimal-conf" | sudo tee /etc/ceph/ceph.conf

   Alternatively, you may copy the conf file. But the above method creates a
   conf with minimum details which is better.

#. Ensure that the conf file has appropriate permissions::

    chmod 644 /etc/ceph/ceph.conf

#. Create a CephX user and get its secret key::

    ssh {user}@{mon-host} "sudo ceph fs authorize cephfs client.foo / rw" | sudo tee /etc/ceph/ceph.client.foo.keyring

   In above command, replace ``cephfs`` with the name of your CephFS, ``foo``
   by the name you want for CephX user and ``/`` by the path within your
   CephFS for which you want to allow access to the client and ``rw`` stands
   for, both, read and write permissions. Alternatively, you may copy the Ceph
   keyring from the MON host to client host at ``/etc/ceph`` but creating a
   keyring specific to the client host is better.

.. note:: If you get 2 prompts for password while running above any of 2 above
   command, run ``sudo ls`` (or any other trivial command with sudo)
   immediately before these commands.

#. Ensure that the keyring has appropriate permissions::

    chmod 600 /etc/ceph/ceph.client.foo.keyring

#. ``mount.ceph`` helper is installed with Ceph packages. If for some reason
   installing these packages is not feasible and/or ``mount.ceph`` is not
   present on the system, you can still mount CephFS, but you'll need to
   explicitly pass the monitor addreses and CephX user keyring. To verify that
   it is installed, do::

    stat /sbin/mount.ceph

Synopsis
--------
In general, the command to mount CephFS via kernel driver looks like this::

    mount -t ceph {device-string}:{path-to-mounted} {mount-point} -o {key-value-args} {other-args}

Mounting CephFS
---------------
On Ceph clusters, CephX is enabled by default. Use ``mount`` command to
mount CephFS with the kernel driver::

    mkdir /mnt/mycephfs
    mount -t ceph :/ /mnt/mycephfs -o name=foo

The key-value argument right after option ``-o`` is CephX credential;
``name`` is the username of the CephX user we are using to mount CephFS. The
default value for ``name`` is ``guest``.

The kernel driver also requires MON's socket and the secret key for the CephX
user. In case of the above command, ``mount.ceph`` helper figures out these
details automatically by finding and reading Ceph conf file and keyring. In
case you don't have these files on the host where you're running mount
command, you can pass these details yourself too::

    mount -t ceph 192.168.0.1:6789,192.168.0.2:6789:/ /mnt/mycephfs -o name=foo,secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

Passing a single MON socket in above command works too. A potential problem
with the command above is that the secret key is left in your shell's command
history. To prevent that you can copy the secret key inside a file and pass
the file by using the option ``secretfile`` instead of ``secret``::

    mount -t ceph :/ /mnt/mycephfs -o name=foo,secretfile=/etc/ceph/foo.secret

Ensure the permissions on the secret key file are appropriate (preferably,
``600``).

In case CephX is disabled, you can omit ``-o`` and the list of key-value
arguments that follow it::

    mount -t ceph :/ /mnt/mycephfs

To mount a subtree of the CephFS root, append the path to the device string::

    mount -t ceph :/subvolume/dir1/dir2 /mnt/mycephfs -o name=fs

If you have more than one file system on your Ceph cluster, you can mount the
non-default FS on your local FS as follows::

    mount -t ceph :/ /mnt/mycephfs2 -o name=fs,mds_namespace=mycephfs2

Unmounting CephFS
-----------------
To unmount the Ceph file system, use the ``umount`` command as usual::

    umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

Persistent Mounts
------------------

To mount CephFS in your file systems table as a kernel driver, add the
following to ``/etc/fstab``::

    [{ipaddress}:{port}]:/ {mount}/{mountpoint} ceph [name=username,secret=secretkey|secretfile=/path/to/secretfile],[{mount.options}]

For example::

    :/     /mnt/ceph    ceph    name=admin,noatime,_netdev    0       2

The default for the ``name=`` parameter is ``guest``. If the ``secret`` or
``secretfile`` options are not specified then the mount helper will attempt to
find a secret for the given ``name`` in one of the configured keyrings.

See `User Management`_ for details on CephX user management and mount.ceph_
manual for more options it can take. For troubleshooting, see
:ref:`kernel_mount_debugging`.

.. _fstab: ../fstab/#kernel-driver
.. _User Management: ../../rados/operations/user-management/
.. _mount.ceph: ../../man/8/mount.ceph/
