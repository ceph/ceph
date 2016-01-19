========================
Create a Ceph filesystem
========================

.. tip::

    The ``ceph fs new`` command was introduced in Ceph 0.84.  Prior to this release,
    no manual steps are required to create a filesystem, and pools named ``data`` and
    ``metadata`` exist by default.

    The Ceph command line now includes commands for creating and removing filesystems,
    but at present only one filesystem may exist at a time.

A Ceph filesystem requires at least two RADOS pools, one for data and one for metadata.
When configuring these pools, you might consider:

- Using a higher replication level for the metadata pool, as any data
  loss in this pool can render the whole filesystem inaccessible.
- Using lower-latency storage such as SSDs for the metadata pool, as this
  will directly affect the observed latency of filesystem operations
  on clients.

Refer to :doc:`/rados/operations/pools` to learn more about managing pools.  For
example, to create two pools with default settings for use with a filesystem, you
might run the following commands:

.. code:: bash

    $ ceph osd pool create cephfs_data <pg_num>
    $ ceph osd pool create cephfs_metadata <pg_num>

Once the pools are created, you may enable the filesystem using the ``fs new`` command:

.. code:: bash

    $ ceph fs new <fs_name> <metadata> <data>

For example:

.. code:: bash

    $ ceph fs new cephfs cephfs_metadata cephfs_data
    $ ceph fs ls
    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]

Once a filesystem has been created, your MDS(s) will be able to enter
an *active* state.  For example, in a single MDS system:

.. code:: bash

    $ ceph mds stat
    e5: 1/1/1 up {0=a=up:active}

Once the filesystem is created and the MDS is active, you are ready to mount
the filesystem:

	- `Mount CephFS`_
	- `Mount CephFS as FUSE`_

.. _Mount CephFS: ../../cephfs/kernel
.. _Mount CephFS as FUSE: ../../cephfs/fuse
