================================
CephFS Client Capabilities
================================

Use Ceph authentication capabilities to restrict your file system clients
to the lowest possible level of authority needed.

.. note:: Path restriction and layout modification restriction are new features
    in the Jewel release of Ceph.

.. note:: Using Erasure Coded(EC) pools with CephFS is supported only with the
   BlueStore Backend. They cannot be used as metadata pools and overwrites must
   be enabled on the data pools.


Path restriction
================

By default, clients are not restricted in what paths they are allowed to
mount. Further, when clients mount a subdirectory, e.g., ``/home/user``, the
MDS does not by default verify that subsequent operations are ‘locked’ within
that directory.

To restrict clients to only mount and work within a certain directory, use
path-based MDS authentication capabilities.

Note that this restriction *only* impacts the filesystem hierarchy -- the metadata
tree managed by the MDS. Clients will still be able to access the underlying
file data in RADOS directly. To segregate clients fully, you must also isolate
untrusted clients in their own RADOS namespace. You can place a client's
filesystem subtree in a particular namespace using `file layouts`_ and then
restrict their RADOS access to that namespace using `OSD capabilities`_

.. _file layouts: ./file-layouts
.. _OSD capabilities: ../rados/operations/user-management/#authorization-capabilities

Syntax
------

To grant rw access to the specified directory only, we mention the specified
directory while creating key for a client using the following syntax::

 ceph fs authorize <fs_name> client.<client_id> <path-in-cephfs> rw

For example, to restrict client ``foo`` to writing only in the ``bar``
directory of file system ``cephfs_a``, use ::

 ceph fs authorize cephfs_a client.foo / r /bar rw

 results in:

 client.foo
   key: *key*
   caps: [mds] allow r, allow rw path=/bar
   caps: [mon] allow r
   caps: [osd] allow rw tag cephfs data=cephfs_a

To completely restrict the client to the ``bar`` directory, omit the
root directory ::

 ceph fs authorize cephfs_a client.foo /bar rw

Note that if a client's read access is restricted to a path, they will only
be able to mount the file system when specifying a readable path in the
mount command (see below).

Supplying ``all`` or ``*`` as the file system name will grant access to every
file system. Note that it is usually necessary to quote ``*`` to protect it
from the shell.

See `User Management - Add a User to a Keyring`_. for additional details on
user management

To restrict a client to the specified sub-directory only, we mention the
specified directory while mounting using the following syntax::

 ceph-fuse -n client.<client_id> <mount-path> -r *directory_to_be_mounted*

For example, to restrict client ``foo`` to ``mnt/bar`` directory, we will
use::

 ceph-fuse -n client.foo mnt -r /bar

Free space reporting
--------------------

By default, when a client is mounting a sub-directory, the used space (``df``)
will be calculated from the quota on that sub-directory, rather than reporting
the overall amount of space used on the cluster.

If you would like the client to report the overall usage of the file system,
and not just the quota usage on the sub-directory mounted, then set the
following config option on the client::


    client quota df = false

If quotas are not enabled, or no quota is set on the sub-directory mounted,
then the overall usage of the file system will be reported irrespective of
the value of this setting.

Layout and Quota restriction (the 'p' flag)
===========================================

To set layouts or quotas, clients require the 'p' flag in addition to 'rw'.
This restricts all the attributes that are set by special extended attributes
with a "ceph." prefix, as well as restricting other means of setting
these fields (such as openc operations with layouts).

For example, in the following snippet client.0 can modify layouts and quotas
on the file system cephfs_a, but client.1 cannot::

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

To create or delete snapshots, clients require the 's' flag in addition to
'rw'. Note that when capability string also contains the 'p' flag, the 's'
flag must appear after it (all flags except 'rw' must be specified in
alphabetical order).

For example, in the following snippet client.0 can create or delete snapshots
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

The optional ``{network/prefix}`` is a standard network name and
prefix length in CIDR notation (e.g., ``10.3.0.0/16``).  If present,
the use of this capability is restricted to clients connecting from
this network.

.. _fs-authorize-multifs:

File system Information Restriction
===================================

If desired, the monitor cluster can present a limited view of the file systems
available. In this case, the monitor cluster will only inform clients about
file systems specified by the administrator. Other file systems will not be
reported and commands affecting them will fail as if the file systems do
not exist.

Consider following example. The Ceph cluster has 2 FSs::

    $ ceph fs ls
    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]
    name: cephfs2, metadata pool: cephfs2_metadata, data pools: [cephfs2_data ]

But we authorize client ``someuser`` for only one FS::

    $ ceph fs authorize cephfs client.someuser / rw
    [client.someuser]
        key = AQAmthpf89M+JhAAiHDYQkMiCq3x+J0n9e8REQ==
    $ cat ceph.client.someuser.keyring
    [client.someuser]
        key = AQAmthpf89M+JhAAiHDYQkMiCq3x+J0n9e8REQ==
        caps mds = "allow rw fsname=cephfs"
        caps mon = "allow r fsname=cephfs"
        caps osd = "allow rw tag cephfs data=cephfs"

And the client can only see the FS that it has authorization for::

    $ ceph fs ls -n client.someuser -k ceph.client.someuser.keyring
    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]

Standby MDS daemons will always be displayed. Note that the information about
restricted MDS daemons and file systems may become available by other means,
such as ``ceph health detail``.

MDS communication restriction
=============================

By default, user applications may communicate with any MDS, whether or not
they are allowed to modify data on an associated file system (see
`Path restriction` above). Client's communication can be restricted to MDS
daemons associated with particular file system(s) by adding MDS caps for that
particular file system. Consider the following example where the Ceph cluster
has 2 FSs::

    $ ceph fs ls
    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]
    name: cephfs2, metadata pool: cephfs2_metadata, data pools: [cephfs2_data ]

Client ``someuser`` is authorized only for one FS::

    $ ceph fs authorize cephfs client.someuser / rw
    [client.someuser]
        key = AQBPSARfg8hCJRAAEegIxjlm7VkHuiuntm6wsA==
    $ ceph auth get client.someuser > ceph.client.someuser.keyring
    exported keyring for client.someuser
    $ cat ceph.client.someuser.keyring
    [client.someuser]
        key = AQBPSARfg8hCJRAAEegIxjlm7VkHuiuntm6wsA==
        caps mds = "allow rw fsname=cephfs"
        caps mon = "allow r"
        caps osd = "allow rw tag cephfs data=cephfs"

Mounting ``cephfs1`` with ``someuser`` works::

    $ sudo ceph-fuse /mnt/cephfs1 -n client.someuser -k ceph.client.someuser.keyring --client-fs=cephfs
    ceph-fuse[96634]: starting ceph client
    ceph-fuse[96634]: starting fuse
    $ mount | grep ceph-fuse
    ceph-fuse on /mnt/cephfs1 type fuse.ceph-fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other)

But mounting ``cephfs2`` does not::

    $ sudo ceph-fuse /mnt/cephfs2 -n client.someuser -k ceph.client.someuser.keyring --client-fs=cephfs2
    ceph-fuse[96599]: starting ceph client
    ceph-fuse[96599]: ceph mount failed with (1) Operation not permitted

Root squash
===========

The ``root squash`` feature is implemented as a safety measure to prevent
scenarios such as accidental ``sudo rm -rf /path``. You can enable
``root_squash`` mode in MDS caps to disallow clients with uid=0 or gid=0 to
perform write access operations -- e.g., rm, rmdir, rmsnap, mkdir, mksnap.
However, the mode allows the read operations of a root client unlike in
other file systems.

Following is an example of enabling root_squash in a filesystem except within
'/volumes' directory tree in the filesystem::

    $ ceph fs authorize a client.test_a / rw root_squash /volumes rw
    $ ceph auth get client.test_a
    [client.test_a]
	key = AQBZcDpfEbEUKxAADk14VflBXt71rL9D966mYA==
	caps mds = "allow rw fsname=a root_squash, allow rw fsname=a path=/volumes"
	caps mon = "allow r fsname=a"
	caps osd = "allow rw tag cephfs data=a"
