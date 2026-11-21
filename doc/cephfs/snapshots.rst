================
CephFS Snapshots
================

CephFS snapshots create an immutable view of the file system at the point in
time they are taken. CephFS snapshots are managed in a special hidden
subdirectory named ``.snap``. Snapshots are created using ``mkdir`` inside the
``.snap`` directory.

Snapshots can be exposed with different names by changing the following client
configurations:

- ``snapdirname`` which is a mount option for kernel clients
- ``client_snapdir`` which is a mount option for ceph-fuse.

Snapshot Creation
==================

The CephFS snapshot feature is enabled by default on new file systems. To
enable the CephFS snapshot feature on existing file systems, use the command
below.

.. code-block:: bash
    
    $ ceph fs set <fs_name> allow_new_snaps true

When snapshots are enabled, all directories in CephFS will have a special
``.snap`` directory. (You may configure a different name with the client's
``snapdir`` setting if you wish.) To create a CephFS snapshot, create a
subdirectory under ``.snap`` with a name of your choice.  For example, to
create a snapshot on directory ``/file1/``, run the command ``mkdir
/file1/.snap/snapshot-name``:

.. code-block:: bash

    $ touch file1
    $ cd .snap
    $ mkdir my_snapshot

Using Snapshots to Recover Data
===============================

Snapshots can also be used to recover deleted files:

- ``create a file1 and create snapshot snap1``

.. code-block:: bash

    $ touch /mnt/cephfs/file1
    $ cd .snap
    $ mkdir snap1

- ``create a file2 and create snapshot snap2``

.. code-block:: bash

    $ touch /mnt/cephfs/file2
    $ cd .snap
    $ mkdir snap2

- ``delete file1 and create a new snapshot snap3``

.. code-block:: bash

    $ rm /mnt/cephfs/file1
    $ cd .snap
    $ mkdir snap3

- ``recover file1 using snapshot snap2 using cp command``

.. code-block:: bash

    $ cd .snap
    $ cd snap2
    $ cp file1 /mnt/cephfs/

Snapshot Deletion
==================

Snapshots are deleted by running ``rmdir`` on the ``.snap`` directory that they
are rooted in. (Attempts to delete a directory that roots the snapshots will
fail. You must delete the snapshots first.)

.. code-block:: bash

    $ cd .snap
    $ rmdir my_snapshot

.. _cephfs-snapshots-removed-data-pool:

Snapshots of Removed Data Pools
===============================

When a snapshot is deleted, the MDS asks the Monitors to remove the snapshot
from each data pool of the file system, and the OSDs then trim the clone
objects of the snapshot from those pools. If a data pool is removed with
``ceph fs rm_data_pool`` while snapshots still reference objects in it, the MDS
records the removed pool and removes snapshots from it in the same way when
they are deleted later. This is automatic and requires no action.

.. important:: Use the procedure below only for data pools that were removed
   from the file system before the MDS was upgraded to a release that records
   removed data pools. The MDS has no record of such pools, so snapshots
   deleted after the removal leave their clone objects in the pool. For pools
   removed after the upgrade, the MDS removes the snapshots and no manual step
   is needed.

If a pool removed before the upgrade is no longer needed, delete it with
``ceph osd pool rm``. This also deletes the clone objects.

To keep the pool and remove the clone objects, use ``cephfs-table-tool``:

#. Find the snapshot IDs that the clone objects hold. ``rados listsnaps``
   prints the snapshot IDs of the clones of one object, and ``rados ls`` lists
   the objects in the pool:

   .. prompt:: bash #

      rados -p <pool> ls
      rados -p <pool> listsnaps <object>

   Alternatively, use the range ``1-<last_snap>``. The command
   ``ceph tell mds.<fs_name>:0 dump snaps --server`` reports ``last_snap``.
   The tool skips the snapshot IDs that the file system still uses.

#. Flush the MDS journal. The tool reads the snap table from the metadata pool,
   and the flush writes recent snapshot deletions to that copy of the table:

   .. prompt:: bash #

      ceph tell mds.<fs_name>:0 flush journal

#. List the snapshot IDs that the tool would remove:

   .. prompt:: bash #

      cephfs-table-tool <fs_name>:0 purge_snaps <pool> <snapids>

   ``<snapids>`` is a comma-separated list of snapshot IDs and ranges, for
   example ``202,203,300-310``. The output lists the snapshot IDs to remove
   under ``purge``. It lists the snapshot IDs that the snap table still
   references under ``skipped_referenced``, and the snapshot IDs greater than
   the ``last_snap`` of the snap table under ``skipped_newer``. The tool does
   not remove the skipped snapshot IDs.

#. Remove the snapshot IDs:

   .. prompt:: bash #

      cephfs-table-tool <fs_name>:0 purge_snaps <pool> <snapids> --yes-i-really-mean-it

   The Monitors mark the snapshot IDs as removed in the pool, and the OSDs
   then trim the clone objects in the background.

The tool refuses a pool that is a data pool or the metadata pool of any file
system. It also refuses a pool whose ``cephfs`` application tag does not name
``<fs_name>``. ``ceph fs rm_data_pool`` does not remove this tag. The tool also
refuses a pool that has an application other than ``cephfs`` enabled, because
the snapshot IDs of that application can be equal to the snapshot IDs of the
file system.

.. note:: The Monitors do not reply if the client does not have the
   ``osd pool rmsnap`` capability, and the tool then waits indefinitely. To make
   the tool fail with ``ETIMEDOUT`` after a number of seconds, pass
   ``--rados_mon_op_timeout <seconds>``.
