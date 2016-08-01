================================
CephFS Client Capabilities
================================

Use Ceph authentication capabilities to restrict your filesystem clients
to the lowest possible level of authority needed.

.. note::

    Path restriction and layout modification restriction are new features
    in the Jewel release of Ceph.

Path restriction
================

By default, clients are not restricted in what paths they are allowed to mount.
Further, when clients mount a subdirectory, e.g., /home/user, the MDS does not
by default verify that subsequent operations
are ‘locked’ within that directory.

To restrict clients to only mount and work within a certain directory, use
path-based MDS authentication capabilities.

Syntax
------

To grant rw access to the specified directory only, we mention the specified
directory while creating key for a client following the undermentioned syntax. ::

./ceph auth get-or-create client.*client_name* mon 'allow r' mds 'allow r, allow rw path=/*specified_directory*' osd 'allow rw pool=data'

for example, to restrict client ``foo`` to ``bar`` directory, we will use. ::

./ceph auth get-or-create client.foo mon 'allow r' mds 'allow r, allow rw path=/bar' osd 'allow rw pool=data'


To restrict a client to the specfied sub-directory only, we mention the specified
directory while mounting following the undermentioned syntax. ::

./ceph-fuse -n client.*client_name* *mount_path* -r *directory_to_be_mounted*

for example, to restrict client ``foo`` to ``mnt/bar`` directory, we will use. ::

./ceph-fuse -n client.foo mnt -r /bar

Free space reporting
--------------------

By default, when a client is mounting a sub-directory, the used space (``df``)
will be calculated from the quota on that sub-directory, rather than reporting
the overall amount of space used on the cluster.

If you would like the client to report the overall usage of the filesystem,
and not just the quota usage on the sub-directory mounted, then set the
following config option on the client:

::

    client quota df = false

If quotas are not enabled, or no quota is set on the sub-directory mounted,
then the overall usage of the filesystem will be reported irrespective of
the value of this setting.

OSD restriction
===============

To prevent clients from writing or reading data to pools other than
those in use for CephFS, set an OSD authentication capability that
restricts access to the CephFS data pool(s):

::

    client.0
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rw
        caps: [mon] allow r
        caps: [osd] allow rw pool=data1, allow rw pool=data2

You may also restrict clients from writing data by using 'r' instead of
'rw' in OSD capabilities.  This does not affect the ability of the client
to update filesystem metadata for these files, but it will prevent them
from persistently writing data in a way that would be visible to other clients.

Layout modification restriction
===============================

To prevent clients from modifying the data pool used for files or
directories, use the 'p' modifier in MDS authentication capabilities.

For example, in the following snippet client.0 can modify the pool used
for files, but client.1 cannot.

::

    client.0
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rwp
        caps: [mon] allow r
        caps: [osd] allow rw pool=data

    client.1
        key: AQAz7EVWygILFRAAdIcuJ12opU/JKyfFmxhuaw==
        caps: [mds] allow rw
        caps: [mon] allow r
        caps: [osd] allow rw pool=data

