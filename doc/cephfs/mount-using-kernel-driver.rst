.. _cephfs-mount-using-kernel-driver:

=================================
 Mount CephFS using Kernel Driver
=================================

The CephFS kernel driver is part of the Linux kernel. It makes possible the
mounting of CephFS as a regular file system with native kernel performance. It
is the client of choice for most use-cases.

.. note:: The CephFS mount device string now uses a new syntax ("v2"). The
   mount helper is backward compatible with the old syntax. The kernel is
   backward-compatible with the old syntax. This means that the old syntax can
   still be used for mounting with newer mount helpers and with the kernel.

Prerequisites
=============

Complete General Prerequisites
------------------------------
Go through the prerequisites required by both kernel and FUSE mounts,
as described on the `Mount CephFS: Prerequisites`_ page.

Is mount helper present?
------------------------
The ``mount.ceph`` helper is installed by Ceph packages. The helper passes the
monitor address(es) and CephX user keyrings, saving the Ceph admin the effort
of passing these details explicitly while mounting CephFS. If the helper is not
present on the client machine, CephFS can still be mounted using the kernel
driver but only by passing these details explicitly to the ``mount`` command.
To check whether ``mount.ceph`` is present on your system, run the following
command:

.. prompt:: bash #

   stat /sbin/mount.ceph

Which Kernel Version?
---------------------

Because the kernel client is distributed as part of the Linux kernel (and not
as part of the packaged Ceph releases), you will need to consider which kernel
version to use on your client nodes. Older kernels are known to include buggy
Ceph clients and may not support features that more recent Ceph clusters
support.

Remember that the "latest" kernel in a stable Linux distribution is likely
to be years behind the latest upstream Linux kernel where Ceph development
takes place (including bug fixes).

As a rough guide, as of Ceph 10.x (Jewel), you should be using a least a 4.x
kernel. If you absolutely have to use an older kernel, you should use the
fuse client instead of the kernel client.

This advice does not apply if you are using a Linux distribution that includes
CephFS support. In that case, the distributor is responsible for backporting
fixes to their stable kernel. Check with your vendor.

Synopsis
========
This is the general form of the command for mounting CephFS via the kernel driver:

.. prompt:: bash #

   mount -t ceph {device-string}={path-to-mounted} {mount-point} -o {key-value-args} {other-args}

Mounting CephFS
===============
CephX authentication is enabled by default in Ceph clusters. Use the ``mount``
command to use the kernel driver to mount CephFS:

.. prompt:: bash #

   mkdir /mnt/mycephfs
   mount -t ceph <name>@<fsid>.<fs_name>=/ /mnt/mycephfs

#. ``name`` is the username of the CephX user we are using to mount CephFS.
#. ``fsid`` is the FSID of the Ceph cluster, which can be found using the
   ``ceph fsid`` command. ``fs_name`` is the file system to mount. The kernel
   driver requires a ceph Monitor's address and the secret key of the CephX
   user. For example:

   .. prompt:: bash #

      mount -t ceph cephuser@b3acfc0d-575f-41d3-9c91-0e7ed3dbb3fa.cephfs=/ -o mon_addr=192.168.0.1:6789,secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

When using the mount helper, monitor hosts and FSID are optional. The
``mount.ceph`` helper discovers these details by finding and reading the ceph
conf file. For example:

.. prompt:: bash #

   mount -t ceph cephuser@.cephfs=/ -o secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

.. note:: Note that the dot (``.`` in the string ``cephuser@.cephfs``) must  be
   a part of the device string.

A weakness of this method is that it will leave the secret key in your shell's
command history. To avoid this, copy the secret key inside a file and pass the
file by using the option ``secretfile`` instead of ``secret``. For example:

.. prompt:: bash #

   mount -t ceph cephuser@.cephfs=/ /mnt/mycephfs -o secretfile=/etc/ceph/cephuser.secret

Ensure that the permissions on the secret key file are appropriate (preferably,
``600``).

Multiple monitor hosts can be passed by separating addresses with a ``/``:

.. prompt:: bash #

   mount -t ceph cephuser@.cephfs=/ /mnt/mycephfs -o mon_addr=192.168.0.1:6789/192.168.0.2:6789,secretfile=/etc/ceph/cephuser.secret

If CephX is disabled, omit any credential-related options. For example:

.. prompt:: bash #

   mount -t ceph cephuser@.cephfs=/ /mnt/mycephfs

.. note:: The Ceph user name must be passed as part of the device string.

To mount a subtree of the CephFS root, append the path to the device string::

  mount -t ceph cephuser@.cephfs=/subvolume/dir1/dir2 /mnt/mycephfs -o secretfile=/etc/ceph/cephuser.secret

Backward Compatibility
======================
The old syntax is supported for backward compatibility.

To mount CephFS with the kernel driver, run the following commands:

.. prompt:: bash #

   mkdir /mnt/mycephfs
   mount -t ceph :/ /mnt/mycephfs -o name=admin

The key-value argument right after the option ``-o`` is the CephX credential.
``name`` is the username of the CephX user that is mounting CephFS.

To mount a non-default FS (in this example, ``cephfs2``), run commands of the following form. These commands are to be used in cases in which the cluster
has multiple file systems:

.. prompt:: bash #

   mount -t ceph :/ /mnt/mycephfs -o name=admin,fs=cephfs2

or

.. prompt:: bash #

   mount -t ceph :/ /mnt/mycephfs -o name=admin,mds_namespace=cephfs2

.. note:: The option ``mds_namespace`` is deprecated. Use ``fs=`` instead when
   using the old syntax for mounting.

Unmounting CephFS
=================
To unmount the Ceph file system, use the ``umount`` command, as in this
example:

.. prompt:: bash #

   umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

Persistent Mounts
==================

To mount CephFS in your file systems table as a kernel driver, add the
following to ``/etc/fstab``::

  {name}@.{fs_name}=/ {mount}/{mountpoint} ceph [mon_addr={ipaddress},secret=secretkey|secretfile=/path/to/secretfile],[{mount.options}]  {fs_freq}  {fs_passno}

For example::

  cephuser@.cephfs=/     /mnt/ceph    ceph    mon_addr=192.168.0.1:6789,noatime,_netdev    0       0

If the ``secret`` or ``secretfile`` options are not specified, the mount
helper will attempt to find a secret for the given ``name`` in one of the
configured keyrings.

See `User Management`_ for details on CephX user management and the mount.ceph_
manual for a list of the options it recognizes. For troubleshooting, see
:ref:`kernel_mount_debugging`.

.. _fstab: ../fstab/#kernel-driver
.. _Mount CephFS\: Prerequisites: ../mount-prerequisites
.. _mount.ceph: ../../man/8/mount.ceph/
.. _User Management: ../../rados/operations/user-management/
