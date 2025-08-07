.. _ceph-dokan:
=======================
Mount CephFS on Windows
=======================

``ceph-dokan`` is used to mount CephFS file systems on Windows. It leverages
Dokany, a Windows driver that allows implementing file systems in userspace in
a manner similar to FUSE.

See the `installation guide`_ to get started.

.. note::

   See the `OS recommendations`_ for information about client package support.

Usage
=====

Mounting file systems
---------------------

Run the following command to mount a Ceph file system::

   ceph-dokan.exe -c c:\ceph.conf -l x

This command mounts the default Ceph file system using the drive letter ``x``.
If ``ceph.conf`` is present in the default location
(``%ProgramData%\ceph\ceph.conf``, then this argument is optional.

The ``-l`` argument allows the use of an empty folder as the mount point
instead of a drive letter.

The uid and gid used for mounting the file system default to ``0`` and can be
changed using the following ``ceph.conf`` options::

    [client]
    # client_permissions = true
    client_mount_uid = 1000
    client_mount_gid = 1000

If you have more than one file system on your Ceph cluster, use the option
``--client_fs`` to mount the non-default file system::

    mkdir -Force C:\mnt\mycephfs2
    ceph-dokan.exe --mountpoint C:\mnt\mycephfs2 --client_fs mycephfs2

Mount CephFS subdirectories by using the ``--root-path`` parameter::

    ceph-dokan -l y --root-path /a

If the ``-o --removable`` flags are set, the mounts will show up in the
``Get-Volume`` results::

    PS C:\> Get-Volume -FriendlyName "Ceph*" | `
            Select-Object -Property @("DriveLetter", "Filesystem", "FilesystemLabel")

    DriveLetter Filesystem FilesystemLabel
    ----------- ---------- ---------------
              Z Ceph       Ceph
              W Ceph       Ceph - new_fs

Run ``ceph-dokan --help`` for a full list of arguments.

Credentials
-----------

The ``--id`` option passes the name of the CephX user whose keyring is used
when mounting a CephFS file system. The following commands are equivalent::

    ceph-dokan --id foo -l x
    ceph-dokan --name client.foo -l x

Unmounting file systems
-----------------=-----

The mount can be removed by either issuing ctrl-c or using the unmap command,
like so::

    ceph-dokan.exe unmap -l x

.. note:: When unmapping CephFS file systems, you must specify the mount point
   argument that was used at the time of the creation of the mapping. 

Limitations
-----------

Windows ACLs are ignored. Posix ACLs are supported but cannot be modified using
the current CLI. In the future, we may add command actions that change file
ownership or permissions.

CephFS doesn't support mandatory file locks, which Windows relies heavily upon.
Ceph relies upon Dokan to handle file locks, which are enforced only locally.

Unlike ``rbd-wnbd``, ``ceph-dokan`` doesn't provide a ``service`` command. To
ensure that a CephFS mount survives reboots of its host, use ``NSSM``.

Troubleshooting
===============

See the `Windows troubleshooting`_ page.

.. _Windows troubleshooting: ../../install/windows-troubleshooting
.. _installation guide: ../../install/windows-install
.. _OS recommendations: ../../start/os-recommendations
