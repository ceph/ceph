=================================
 Mount CephFS using Kernel Driver
=================================

The CephFS kernel driver is part of the Linux kernel. It allows mounting
CephFS as a regular file system with native kernel performance. It is the
client of choice for most use-cases.

Prerequisites
=============

Complete General Prerequisites
------------------------------
Go through the prerequisites required by both, kernel as well as FUSE mounts,
in `Mount CephFS: Prerequisites`_ page.

Is mount helper is present?
---------------------------
``mount.ceph`` helper is installed by Ceph packages. The helper passes the
monitor address(es) and CephX user keyrings automatically saving the Ceph
admin the effort to pass these details explicitly while mountng CephFS. In
case the helper is not present on the client machine, CephFS can still be
mounted using kernel but by passing these details explicitly to the ``mount``
command. To check whether it is present on your system, do::

    stat /sbin/mount.ceph

Which Kernel Version?
---------------------

Because the kernel client is distributed as part of the linux kernel (not
as part of packaged ceph releases), you will need to consider which kernel
version to use on your client nodes. Older kernels are known to include buggy
ceph clients, and may not support features that more recent Ceph clusters
support.

Remember that the "latest" kernel in a stable linux distribution is likely
to be years behind the latest upstream linux kernel where Ceph development
takes place (including bug fixes).

As a rough guide, as of Ceph 10.x (Jewel), you should be using a least a 4.x
kernel. If you absolutely have to use an older kernel, you should use the
fuse client instead of the kernel client.

This advice does not apply if you are using a linux distribution that
includes CephFS support, as in this case the distributor will be responsible
for backporting fixes to their stable kernel: check with your vendor.

Synopsis
========
In general, the command to mount CephFS via kernel driver looks like this::

    mount -t ceph {device-string}:{path-to-mounted} {mount-point} -o {key-value-args} {other-args}

Mounting CephFS
===============
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
non-default FS as follows::

    mount -t ceph :/ /mnt/mycephfs2 -o name=fs,fs=mycephfs2

Unmounting CephFS
=================
To unmount the Ceph file system, use the ``umount`` command as usual::

    umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

Persistent Mounts
==================

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
.. _Mount CephFS\: Prerequisites: ../mount-prerequisites
.. _mount.ceph: ../../man/8/mount.ceph/
.. _User Management: ../../rados/operations/user-management/
