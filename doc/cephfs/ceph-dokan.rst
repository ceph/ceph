.. _ceph-dokan:
=======================
Mount CephFS on Windows
=======================

``ceph-dokan`` can be used for mounting CephFS filesystems on Windows.
It leverages Dokany, a Windows driver that allows implementing filesystems in
userspace, pretty much like FUSE.

Please see the `installation guide`_ to get started.

.. note::

   Please see the `OS recommendations`_ regarding client package support.

Usage
=====

Mounting filesystems
--------------------

In order to mount a ceph filesystem, the following command can be used::

    ceph-dokan.exe -c c:\ceph.conf -l x

This will mount the default ceph filesystem using the drive letter ``x``.
If ``ceph.conf`` is placed at the default location, which is
``%ProgramData%\ceph\ceph.conf``, then this argument becomes optional.

The ``-l`` argument also allows using an empty folder as a mount point
instead of a drive letter.

The uid and gid used for mounting the filesystem default to 0 and may be
changed using the following ``ceph.conf`` options::

    [client]
    # client_permissions = true
    client_mount_uid = 1000
    client_mount_gid = 1000

If you have more than one FS on your Ceph cluster, use the option
``--client_fs`` to mount the non-default FS::

    mkdir -Force C:\mnt\mycephfs2
    ceph-dokan.exe --mountpoint C:\mnt\mycephfs2 --client_fs mycephfs2

CephFS subdirectories can be mounted using the ``--root-path`` parameter::

    ceph-dokan -l y --root-path /a

If the ``-o --removable`` flags are set, the mounts will show up in the
``Get-Volume`` results::

    PS C:\> Get-Volume -FriendlyName "Ceph*" | `
            Select-Object -Property @("DriveLetter", "Filesystem", "FilesystemLabel")

    DriveLetter Filesystem FilesystemLabel
    ----------- ---------- ---------------
              Z Ceph       Ceph
              W Ceph       Ceph - new_fs

Please use ``ceph-dokan --help`` for a full list of arguments.

Credentials
-----------

The ``--id`` option passes the name of the CephX user whose keyring we intend to
use for mounting CephFS. The following commands are equivalent::

    ceph-dokan --id foo -l x
    ceph-dokan --name client.foo -l x

Unmounting filesystems
----------------------

The mount can be removed by either issuing ctrl-c or using the unmap command,
like so::

    ceph-dokan.exe unmap -l x

Note that when unmapping Ceph filesystems, the exact same mount point argument
must be used as when the mapping was created.

Limitations
-----------

Be aware that Windows ACLs are ignored. Posix ACLs are supported but cannot be
modified using the current CLI. In the future, we may add some command actions
to change file ownership or permissions.

Another thing to note is that CephFS doesn't support mandatory file locks, which
Windows relies heavily upon. At present Ceph lets Dokan handle file
locks, which are only enforced locally.

Unlike ``rbd-wnbd``, ``ceph-dokan`` doesn't currently provide a ``service``
command. In order for the cephfs mount to survive host reboots, consider using
``NSSM``.

Troubleshooting
===============

Please consult the `Windows troubleshooting`_ page.

.. _Windows troubleshooting: ../../install/windows-troubleshooting
.. _installation guide: ../../install/windows-install
.. _OS recommendations: ../../start/os-recommendations
