=======================
Mount CephFS on Windows
=======================

``ceph-dokan`` can be used for mounting CephFS filesystems on Windows.
It leverages Dokany, a Windows driver that allows implementing filesystems in
userspace, pretty much like FUSE.

Prerequisites
=============

Dokany
------

``ceph-dokan`` requires Dokany to be installed. You may fetch the installer as
well as the source code from the Dokany Github repository:
https://github.com/dokan-dev/dokany/releases

The minimum supported Dokany version is 1.3.1. At the time of the writing,
Dokany 2.0 is in Beta stage and is unsupported.

Supported platforms
-------------------

Windows Server 2019 and Windows Server 2016 are supported. Previous Windows
Server versions, including Windows client versions such as Windows 10, might
work but haven't been tested.

Configuration
=============

``ceph-dokan`` requires minimal configuration. Please check the
`Windows configuration sample`_ to get started.

You'll also need a keyring file. The `General Prerequisites`_ page provides a
simple example, showing how a new CephX user can be created and how its secret
key can be retrieved.

For more details on CephX user management, see the `Client Authentication`_
and :ref:`User Management <user-management>`.

Usage
=====

In order to mount a ceph filesystem, the following command can be used::

    ceph-dokan.exe -c c:\ceph.conf -l x

This will mount the default ceph filesystem using the drive letter ``x``.
If ``ceph.conf`` is placed at the default location, which is
``%ProgramData%\ceph\ceph.conf``, then this argument becomes optional.

The ``-l`` argument also allows using an empty folder as a mountpoint
instead of a drive letter.

The uid and gid used for mounting the filesystem default to 0 and may be
changed using the following ``ceph.conf`` options::

    [client]
    # client_permissions = true
    client_mount_uid = 1000
    client_mount_gid = 1000

Please use ``ceph-dokan --help`` for a full list of arguments.

The mount can be removed by either issuing ctrl-c or using the unmap command,
like so::

    ceph-dokan.exe unmap -l x

Note that when unmapping Ceph filesystems, the exact same mountpoint argument
must be used as when the mapping was created.

Credentials
-----------

The ``--id`` option passes the name of the CephX user whose keyring we intend to
use for mounting CephFS. The following commands are equivalent::

    ceph-dokan --id foo -l x
    ceph-dokan --name client.foo -l x

Limitations
-----------

Be aware that Windows ACLs are ignored. Posix ACLs are supported but cannot be
modified using the current CLI. In the future, we may add some command actions
to change file ownership or permissions.

Another thing to note is that cephfs doesn't support mandatory file locks, which
Windows is heavily rely upon. At the moment, we're letting Dokan handle file
locks, which are only enforced locally.

Unlike ``rbd-wnbd``, ``ceph-dokan`` doesn't currently provide a ``service``
command. In order for the cephfs mount to survive host reboots, consider using
``NSSM``.

.. _Windows configuration sample: ../windows-basic-config
.. _General Prerequisites: ../mount-prerequisites
.. _Client Authentication: ../client-auth
