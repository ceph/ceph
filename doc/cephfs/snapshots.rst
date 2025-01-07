================
CephFS Snapshots
================

CephFS snapshots create an immutable view of the file system at the point
in time they are taken. CephFS support snapshots which is managed in a 
special hidden subdirectory named ``.snap`` .Snapshots are created using
``mkdir`` inside this directory.

Snapshots can be exposed with a different name by changing the following client configurations.

- ``snapdirname`` which is a mount option for kernel clients
- ``client_snapdir`` which is a mount option for ceph-fuse.

Snapshot Creation
==================

CephFS snapshot feature is enabled by default on new file systems. To enable 
it on existing file systems, use the command below.

.. code-block:: bash
    
    $ ceph fs set <fs_name> allow_new_snaps true

When snapshots are enabled, all directories in CephFS will have a special ``.snap``
directory. (You may configure a different name with the client snapdir setting if 
you wish.)
To create a CephFS snapshot, create a subdirectory under ``.snap`` with a name of 
your choice. 
For example, to create a snapshot on directory ``/file1/``, invoke ``mkdir /file1/.snap/snapshot-name``

.. code-block:: bash

    $ touch file1
    $ cd .snap
    $ mkdir my_snapshot

Using snapshot to recover data
===============================

Snapshots can also be used to recover some deleted files.

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

Snapshots are deleted by invoking ``rmdir`` on the ``.snap`` directory they are
rooted in. (Attempts to delete a directory which roots the snapshots will fail; 
you must delete the snapshots first.)

.. code-block:: bash

    $ cd .snap
    $ rmdir my_snapshot
