================================
CephFS Client Capabilities
================================

Ceph authentication capabilities are used to restrict CephFS clients to
the lowest level of authority necessary.

.. note:: Path restriction and layout-modification restriction were introduced
   in the Jewel release of Ceph.

.. note:: Using Erasure Coded (EC) pools with CephFS is supported only with
   :term:`BlueStore`. Erasure-coded pools cannot be used as metadata pools.
   Overwrites must be enabled on erasure-coded data pools.


Path restriction
================

By default, clients are not restricted in the paths that they are allowed to
mount. When clients mount a subdirectory (for example ``/home/user``), the MDS
does not by default verify that subsequent operations are "locked" within that
directory.

To restrict clients so that they mount and work only within a certain
directory, use path-based MDS authentication capabilities.

This restriction impacts *only* the filesystem hierarchy, or, in other words,
the metadata tree that is managed by the MDS. Clients will still be able to
access the underlying file data in RADOS directly. To segregate clients fully,
isolate untrusted clients in their own RADOS namespace. You can place a
client's filesystem subtree in a particular namespace using `file layouts`_ and
then restrict their RADOS access to that namespace using `OSD capabilities`_

.. _file layouts: ./file-layouts
.. _OSD capabilities: ../rados/operations/user-management/#authorization-capabilities

Syntax
------

To grant ``rw`` access to the specified directory only, mention the specified
directory while creating key for a client. Use a command of the following form:

.. prompt:: bash #

   ceph fs authorize <fs_name> client.<client_id> <path-in-cephfs> rw

For example, to restrict a client named ``foo`` so that it can write only in
the ``bar`` directory of file system ``cephfs_a``, run the following command:

.. prompt:: bash #

   ceph fs authorize cephfs_a client.foo / r /bar rw

This results in::

 client.foo
   key: *key*
   caps: [mds] allow r, allow rw path=/bar
   caps: [mon] allow r
   caps: [osd] allow rw tag cephfs data=cephfs_a

To completely restrict the client to the ``bar`` directory, omit the
root directory :

.. prompt:: bash #

   ceph fs authorize cephfs_a client.foo /bar rw

If a client's read access is restricted to a path, the client will be able to
mount the file system only by specifying a readable path in the mount command
(see below).

Supplying ``all`` or ``*`` as the file system name grants access to every file
system. It is usually necessary to quote ``*`` to protect it from the
shell.

See `User Management - Add a User to a Keyring`_ for more on user management.

To restrict a client to only the specified sub-directory, mention the specified
directory while mounting. Use a command of the following form: 

.. prompt:: bash #

   ceph-fuse -n client.<client_id> <mount-path> -r *directory_to_be_mounted*

For example, to restrict client ``foo`` to ``mnt/bar`` directory, use the
following command:

.. prompt:: bash #

   ceph-fuse -n client.foo mnt -r /bar

Reporting free space 
--------------------

When a client has mounted a sub-directory, the used space (``df``) is
calculated from the quota on that sub-directory rather than from the overall
amount of space used on the CephFS file system.

To make the client report the overall usage of the file system and not only the
quota usage on the mounted sub-directory, set the following config option on
the client::

    client quota df = false

If quotas are not enabled or if no quota is set on the mounted sub-directory,
then the overall usage of the file system will be reported irrespective of the
value of this setting.

Layout and Quota restriction (the 'p' flag)
===========================================

To set layouts or quotas, clients require the ``p`` flag in addition to ``rw``.
Using the ``p`` flag with ``rw`` restricts all the attributes that are set by
special extended attributes by using a ``ceph.`` prefix, and restricts
other means of setting these fields (such as ``openc`` operations with layouts).

For example, in the following snippet ``client.0`` can modify layouts and
quotas on the file system ``cephfs_a``, but ``client.1`` cannot::

    client.0
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rwp
        caps: [mon] allow r
        caps: [osd] allow rw tag cephfs data=cephfs_a

    client.1
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rw
        caps: [mon] allow r
        caps: [osd] allow rw tag cephfs data=cephfs_a


Snapshot restriction (the 's' flag)
===========================================

To create or delete snapshots, clients require the ``s`` flag in addition to
``rw``. Note that when capability string also contains the ``p`` flag, the
``s`` flag must appear after it (all flags except ``rw`` must be specified in
alphabetical order).

For example, in the following snippet ``client.0`` can create or delete snapshots
in the ``bar`` directory of file system ``cephfs_a``::

    client.0
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rw, allow rws path=/bar
        caps: [mon] allow r
        caps: [osd] allow rw tag cephfs data=cephfs_a


.. _User Management - Add a User to a Keyring: ../../rados/operations/user-management/#add-a-user-to-a-keyring

Network restriction
===================

::

 client.foo
   key: *key*
   caps: [mds] allow r network 10.0.0.0/8, allow rw path=/bar network 10.0.0.0/8
   caps: [mon] allow r network 10.0.0.0/8
   caps: [osd] allow rw tag cephfs data=cephfs_a network 10.0.0.0/8

The optional ``{network/prefix}`` is a standard network-name-and-prefix length
in CIDR notation (for example, ``10.3.0.0/16``). If ``{network/prefix}}`` is
present, the use of this capability is restricted to clients connecting from
this network.

.. _fs-authorize-multifs:

File system Information Restriction
===================================

The monitor cluster can present a limited view of the available file systems.
In this case, the monitor cluster informs clients only about file systems
specified by the administrator. Other file systems are not reported and
commands affecting them fail as though the file systems do not exist.

Consider following example. The Ceph cluster has 2 file systems:

.. prompt:: bash #

   ceph fs ls

::

    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]
    name: cephfs2, metadata pool: cephfs2_metadata, data pools: [cephfs2_data ]

We authorize client ``someuser`` for only one file system:

.. prompt:: bash #

   ceph fs authorize cephfs client.someuser / rw

::

    [client.someuser]
        key = AQAmthpf89M+JhAAiHDYQkMiCq3x+J0n9e8REQ==

.. prompt:: bash #

   cat ceph.client.someuser.keyring

::

    [client.someuser]
        key = AQAmthpf89M+JhAAiHDYQkMiCq3x+J0n9e8REQ==
        caps mds = "allow rw fsname=cephfs"
        caps mon = "allow r fsname=cephfs"
        caps osd = "allow rw tag cephfs data=cephfs"

The client can see only the file system that it is authorized to see: 

.. prompt:: bash #

   ceph fs ls -n client.someuser -k ceph.client.someuser.keyring

::

   name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]

Standby MDS daemons are always displayed. Information about restricted MDS
daemons and file systems may become available by other means, such as by
running ``ceph health detail``.

MDS communication restriction
=============================

By default, user applications may communicate with any MDS, regardless of
whether they are allowed to modify data on an associated file system (see `Path
restriction` above). Client communication can be restricted to MDS daemons
associated with particular file system(s) by adding MDS caps for that
particular file system. Consider the following example where the Ceph cluster
has two file systems:

.. prompt:: bash #

   ceph fs ls

::

    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]
    name: cephfs2, metadata pool: cephfs2_metadata, data pools: [cephfs2_data ]

Client ``someuser`` is authorized for only one file system:

.. prompt:: bash #

   ceph fs authorize cephfs client.someuser / rw

::

    [client.someuser]
        key = AQBPSARfg8hCJRAAEegIxjlm7VkHuiuntm6wsA==

.. prompt:: bash #

   ceph auth get client.someuser > ceph.client.someuser.keyring

::

    exported keyring for client.someuser

.. prompt:: bash #

   cat ceph.client.someuser.keyring

::

    [client.someuser]
        key = AQBPSARfg8hCJRAAEegIxjlm7VkHuiuntm6wsA==
        caps mds = "allow rw fsname=cephfs"
        caps mon = "allow r"
        caps osd = "allow rw tag cephfs data=cephfs"

Mounting ``cephfs1`` on the already-created mountpoint  ``/mnt/cephfs1``  with
``someuser`` works:

.. prompt:: bash #

   sudo ceph-fuse /mnt/cephfs1 -n client.someuser -k ceph.client.someuser.keyring --client-fs=cephfs

.. note:: If ``/mnt/cephfs`` does not exist prior to running the above command,
   create it by running ``mkdir /mnt/cephfs1``.

::

    ceph-fuse[96634]: starting ceph client
    ceph-fuse[96634]: starting fuse

.. prompt:: bash #

   mount | grep ceph-fuse

::

    ceph-fuse on /mnt/cephfs1 type fuse.ceph-fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other)

Mounting ``cephfs2`` with ``someuser`` does not work:

.. prompt:: bash #

   sudo ceph-fuse /mnt/cephfs2 -n client.someuser -k ceph.client.someuser.keyring --client-fs=cephfs2

::

   ceph-fuse[96599]: starting ceph client
   ceph-fuse[96599]: ceph mount failed with (1) Operation not permitted

Root squash
===========

The ``root squash`` feature is implemented as a safety measure to prevent
scenarios such as an accidental forced removal of a path (for example, ``sudo
rm -rf /path``). Enable ``root_squash`` mode in MDS caps to disallow clients
with ``uid=0`` or ``gid=0`` to perform write access operations (for example
``rm``, ``rmdir``, ``rmsnap``, ``mkdir``, and ``mksnap``). This mode permits
the read operations on a root client, unlike the behavior of other file
systems.

Here is an example of enabling ``root_squash`` in a filesystem, except within
the ``/volumes`` directory tree in the filesystem:

.. prompt:: bash #

   ceph fs authorize a client.test_a / rw root_squash /volumes rw
   ceph auth get client.test_a

::

    [client.test_a]
	key = AQBZcDpfEbEUKxAADk14VflBXt71rL9D966mYA==
	caps mds = "allow rw fsname=a root_squash, allow rw fsname=a path=/volumes"
	caps mon = "allow r fsname=a"
	caps osd = "allow rw tag cephfs data=a"

Updating Capabilities using ``fs authorize``
============================================

Beginning with the Reef release of Ceph, ``fs authorize`` can be used to add
new caps to an existing client (for another CephFS or another path in the same
file system).

The following example demonstrates the behavior that results from running the command ``ceph fs authorize a client.x / rw`` twice.

#. Create a new client:

   .. prompt:: bash #

      ceph fs authorize a client.x / rw

   ::

      [client.x]
          key = AQAOtSVk9WWtIhAAJ3gSpsjwfIQ0gQ6vfSx/0w==

#. Get the client capabilities: 

   .. prompt:: bash #

      ceph auth get client.x

   ::

      [client.x]
            key = AQAOtSVk9WWtIhAAJ3gSpsjwfIQ0gQ6vfSx/0w==
            caps mds = "allow rw fsname=a"
            caps mon = "allow r fsname=a"
            caps osd = "allow rw tag cephfs data=a"

#. Previously, running ``fs authorize a client.x / rw`` a second time printed
   an error message. In the Reef release and in later releases, this command
   prints a message reporting that the capabilities did not get updated:

   .. prompt:: bash #

      ./bin/ceph fs authorize a client.x / rw

   ::

       no update for caps of client.x

Adding New Caps Using ``fs authorize``
--------------------------------------

Add capabilities for another path in same CephFS:

.. prompt:: bash #

   ceph fs authorize a client.x /dir1 rw

::

    updated caps for client.x

.. prompt:: bash #

   ceph auth get client.x

::

   [client.x]
           key = AQAOtSVk9WWtIhAAJ3gSpsjwfIQ0gQ6vfSx/0w==
           caps mds = "allow r fsname=a, allow rw fsname=a path=some/dir"
           caps mon = "allow r fsname=a"
           caps osd = "allow rw tag cephfs data=a"

Add capabilities for another CephFS on the Ceph cluster:

.. prompt:: bash #

   ceph fs authorize b client.x / rw

::

    updated caps for client.x

.. prompt:: bash #

   ceph auth get client.x

::

   [client.x]
           key = AQD6tiVk0uJdARAABMaQuLRotxTi3Qdj47FkBA==
           caps mds = "allow rw fsname=a, allow rw fsname=b"
           caps mon = "allow r fsname=a, allow r fsname=b"
           caps osd = "allow rw tag cephfs data=a, allow rw tag cephfs data=b"

Changing rw permissions in caps
-------------------------------

Capabilities can be modified by running ``fs authorize`` only in the case when
read/write permissions must be changed. This is because the command ``fs
authorize`` becomes ambiguous. For example, a user runs ``fs authorize cephfs1
client.x /dir1 rw`` to create a client and then runs ``fs authorize cephfs1
client.x /dir2 rw`` (notice that ``/dir1`` has been changed to ``/dir2``).
Running the second command could be interpreted to change ``/dir1`` to
``/dir2`` with current capabilities or could be interpreted to authorize the
client with a new capability for the path ``/dir2``. As shown previously, the
second interpretation is chosen and it is therefore impossible to update a part
of the capabilities granted except ``rw`` permissions. The following shows how
read/write permissions for ``client.x`` can be changed:

.. prompt:: bash #

   ceph fs authorize a client.x / r
    [client.x]
        key = AQBBKjBkIFhBDBAA6q5PmDDWaZtYjd+jafeVUQ==

.. prompt:: bash #

   ceph auth get client.x

::

    [client.x]
            key = AQBBKjBkIFhBDBAA6q5PmDDWaZtYjd+jafeVUQ==
            caps mds = "allow r fsname=a"
            caps mon = "allow r fsname=a"
            caps osd = "allow r tag cephfs data=a"

``fs authorize`` never deducts any part of caps
-----------------------------------------------
Capabilities that have been issued to a client can not be removed by running
``fs authorize`` again. For example, if a client capability has ``root_squash``
applied on a certain CephFS, running ``fs authorize`` again for the same CephFS
but without ``root_squash`` will not lead to any update and the client caps will
remain unchanged:

.. prompt:: bash #

   ceph fs authorize a client.x / rw root_squash
   
::

    [client.x]
            key = AQD61CVkcA1QCRAAd0XYqPbHvcc+lpUAuc6Vcw==

.. prompt:: bash #

   ceph auth get client.x

::

    [client.x]
            key = AQD61CVkcA1QCRAAd0XYqPbHvcc+lpUAuc6Vcw==
            caps mds = "allow rw fsname=a root_squash"
            caps mon = "allow r fsname=a"
            caps osd = "allow rw tag cephfs data=a"

.. prompt:: bash #

   ceph fs authorize a client.x / rw

::

    [client.x]
            key = AQD61CVkcA1QCRAAd0XYqPbHvcc+lpUAuc6Vcw==
    no update was performed for caps of client.x. caps of client.x remains unchanged.

If a client already has a capability for file-system name ``a`` and path
``dir1``, running ``fs authorize`` again for FS name ``a`` but path ``dir2``,
instead of modifying the capabilities client already holds, a new cap for
``dir2`` will be granted:

.. prompt:: bash #

   ceph fs authorize a client.x /dir1 rw
   ceph auth get client.x

::

    [client.x]
            key = AQC1tyVknMt+JxAAp0pVnbZGbSr/nJrmkMNKqA==
            caps mds = "allow rw fsname=a path=/dir1"
            caps mon = "allow r fsname=a"
            caps osd = "allow rw tag cephfs data=a"

.. prompt:: bash #
   
   ceph fs authorize a client.x /dir2 rw

::

    updated caps for client.x

.. prompt:: bash #

   ceph auth get client.x

::

    [client.x]
            key = AQC1tyVknMt+JxAAp0pVnbZGbSr/nJrmkMNKqA==
            caps mds = "allow rw fsname=a path=dir1, allow rw fsname=a path=dir2"
            caps mon = "allow r fsname=a"
            caps osd = "allow rw tag cephfs data=a"
