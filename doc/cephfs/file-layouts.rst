.. _file-layouts:

File layouts
============

The layout of a file controls how its contents are mapped to Ceph RADOS objects.  You can
read and write a file's layout using *virtual extended attributes* or xattrs.

Clients must use the ``p`` flag when writing a file's layout. See :ref:`Layout
and Quota restriction (the 'p' flag) <cephfs-layout-and-quota-restriction>`.

The name of the layout xattrs depends on whether a file is a regular file or a directory.  Regular
files' layout xattrs are called ``ceph.file.layout``, whereas directories' layout xattrs are called
``ceph.dir.layout``.  Where subsequent examples refer to ``ceph.file.layout``, substitute ``dir`` as appropriate
when dealing with directories.

.. tip::

    Your Linux distribution may not ship with commands for manipulating xattrs by default,
    the required package is usually called ``attr``.

Layout fields
-------------

pool
    This is a string and contains either an ID or a name. Strings may contain
    only characters in the set ``[a-zA-Z0-9\_-.]``. This determines the RADOS
    pool that stores a file's data objects.

pool_id
    This is a string of digits. This is the pool ID that was assigned by Ceph
    at the time of the creation of the RADOS pool.

pool_name
    This is a string. This is the name of the RADOS pool as defined by the user
    when the pool was created. 

pool_namespace
    This is a string containing only characters in the set ``[a-zA-Z0-9\_-.]``.
    This determines which RADOS namespace within the data pool that the objects
    will be written to.
    Empty by default (i.e. default namespace).

stripe_unit
    This is an integer. The size (in bytes) of a block of data used in the
    distribution of a file. All stripe units for a file have equal size. The
    last stripe unit is typically only partly full of data: it holds file data
    through EOF as well as padding that fills the balance of the fixed stripe
    unit size. 

stripe_count
    Integer. The number of consecutive stripe units that constitute a RAID 0
    “stripe” of file data.

object_size
    Integer. The size of the object in bytes. File data is chunked into RADOS
    objects of this size.

.. tip::

    RADOS enforces a configurable limit on object sizes: if you increase CephFS
    object sizes beyond that limit then writes may not succeed.  The OSD
    setting is ``osd_max_object_size``, which is 128MB by default.
    Very large RADOS objects may prevent smooth operation of the cluster,
    so increasing the object size limit past the default is not recommended.

Reading layouts with ``getfattr``
---------------------------------

Read the layout information as a single string:

.. code-block:: bash

    $ touch file
    $ getfattr -n ceph.file.layout file
    # file: file
    ceph.file.layout="stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=cephfs_data"

Read individual layout fields:

.. code-block:: bash

    $ getfattr -n ceph.file.layout.pool_name file
    # file: file
    ceph.file.layout.pool_name="cephfs_data"
    $ getfattr -n ceph.file.layout.pool_id file
    # file: file
    ceph.file.layout.pool_id="5"
    $ getfattr -n ceph.file.layout.pool file
    # file: file
    ceph.file.layout.pool="cephfs_data"
    $ getfattr -n ceph.file.layout.stripe_unit file
    # file: file
    ceph.file.layout.stripe_unit="4194304"
    $ getfattr -n ceph.file.layout.stripe_count file
    # file: file
    ceph.file.layout.stripe_count="1"
    $ getfattr -n ceph.file.layout.object_size file
    # file: file
    ceph.file.layout.object_size="4194304"    

.. note::

    When reading layouts, the pool will usually be indicated by name.  However, in 
    rare cases when pools have only just been created, the ID may be output instead.

Directories do not have an explicit layout until it is customized.  Attempts to read
the layout will fail if it has never been modified: this indicates that layout of the
next ancestor directory with an explicit layout will be used.

.. code-block:: bash

    $ mkdir dir
    $ getfattr -n ceph.dir.layout dir
    dir: ceph.dir.layout: No such attribute
    $ setfattr -n ceph.dir.layout.stripe_count -v 2 dir
    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

Getting the layout in JSON format. If there's no specific layout set for the
particular inode, the system traverses the directory path backwards and finds
the closest ancestor directory with a layout and returns it in JSON format.
A file layout also can be retrieved in JSON format using ``ceph.file.layout.json`` vxattr.

A virtual field named ``inheritance`` is added to the JSON output to show the status of layout.
The ``inheritance`` field can have the following values:

``@default`` implies the system default layout
``@set`` implies that a specific layout has been set for that particular inode
``@inherited`` implies that the returned layout has been inherited from an ancestor

.. code-block:: bash

   $ getfattr -n ceph.dir.layout.json --only-values /mnt/mycephs/accounts
   {"stripe_unit": 4194304, "stripe_count": 1, "object_size": 4194304, "pool_name": "cephfs.a.data", "pool_id": 3, "pool_namespace": "", "inheritance": "@default"}


Writing layouts with ``setfattr``
---------------------------------

Layout fields are modified using ``setfattr``:

.. code-block:: bash

    $ ceph osd lspools
    0 rbd
    1 cephfs_data
    2 cephfs_metadata

    $ setfattr -n ceph.file.layout.stripe_unit -v 1048576 file2
    $ setfattr -n ceph.file.layout.stripe_count -v 8 file2
    $ setfattr -n ceph.file.layout.object_size -v 10485760 file2
    $ setfattr -n ceph.file.layout.pool -v 1 file2  # Setting pool by ID
    $ setfattr -n ceph.file.layout.pool -v cephfs_data file2  # Setting pool by name
    $ setfattr -n ceph.file.layout.pool_id -v 1 file2  # Setting pool by ID
    $ setfattr -n ceph.file.layout.pool_name -v cephfs_data file2  # Setting pool by name

.. note::

    When the layout fields of a file are modified using ``setfattr``, this file must be empty, otherwise an error will occur.

.. code-block:: bash

    # touch an empty file
    $ touch file1
    # modify layout field successfully
    $ setfattr -n ceph.file.layout.stripe_count -v 3 file1

    # write something to file1
    $ echo "hello world" > file1
    $ setfattr -n ceph.file.layout.stripe_count -v 4 file1
    setfattr: file1: Directory not empty

File and Directory layouts can also be set using the JSON format.
The ``inheritance`` field is ignored when setting the layout.
Also, if both ``pool_name`` and ``pool_id`` fields are specified, then the
``pool_name`` is given preference for better disambiguation.

.. code-block:: bash

   $ setfattr -n ceph.file.layout.json -v '{"stripe_unit": 4194304, "stripe_count": 1, "object_size": 4194304, "pool_name": "cephfs.a.data", "pool_id": 3, "pool_namespace": "", "inheritance": "@default"}' file1
    
Clearing layouts
----------------

If you wish to remove an explicit layout from a directory, to revert to
inheriting the layout of its ancestor, you can do so:

.. code-block:: bash

    setfattr -x ceph.dir.layout mydir

Similarly, if you have set the ``pool_namespace`` attribute and wish
to modify the layout to use the default namespace instead:

.. code-block:: bash

    # Create a dir and set a namespace on it
    mkdir mydir
    setfattr -n ceph.dir.layout.pool_namespace -v foons mydir
    getfattr -n ceph.dir.layout mydir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=cephfs_data_a pool_namespace=foons"

    # Clear the namespace from the directory's layout
    setfattr -x ceph.dir.layout.pool_namespace mydir
    getfattr -n ceph.dir.layout mydir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=cephfs_data_a"


Inheritance of layouts
----------------------

Files inherit the layout of their parent directory at creation time.  However, subsequent
changes to the parent directory's layout do not affect children.

.. code-block:: bash

    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # Demonstrate file1 inheriting its parent's layout
    $ touch dir/file1
    $ getfattr -n ceph.file.layout dir/file1
    # file: dir/file1
    ceph.file.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # Now update the layout of the directory before creating a second file
    $ setfattr -n ceph.dir.layout.stripe_count -v 4 dir
    $ touch dir/file2

    # Demonstrate that file1's layout is unchanged
    $ getfattr -n ceph.file.layout dir/file1
    # file: dir/file1
    ceph.file.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # ...while file2 has the parent directory's new layout
    $ getfattr -n ceph.file.layout dir/file2
    # file: dir/file2
    ceph.file.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"


Files created as descendants of the directory also inherit the layout if the intermediate
directories do not have layouts set:

.. code-block:: bash

    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"
    $ mkdir dir/childdir
    $ getfattr -n ceph.dir.layout dir/childdir
    dir/childdir: ceph.dir.layout: No such attribute
    $ touch dir/childdir/grandchild
    $ getfattr -n ceph.file.layout dir/childdir/grandchild
    # file: dir/childdir/grandchild
    ceph.file.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"


.. _adding-data-pool-to-file-system:
    
Adding a data pool to the File System 
-------------------------------------

Before you can use a pool with CephFS you have to add it to the Metadata Servers.

.. code-block:: bash

    $ ceph fs add_data_pool cephfs cephfs_data_ssd
    $ ceph fs ls  # Pool should now show up
    .... data pools: [cephfs_data cephfs_data_ssd ]

Make sure that your CephX key allows the client to access this new pool.

You can then update the layout on a directory in CephFS to use the pool you added:

.. code-block:: bash

    $ mkdir /mnt/cephfs/myssddir
    $ setfattr -n ceph.dir.layout.pool -v cephfs_data_ssd /mnt/cephfs/myssddir

All new files created within that directory will now inherit its layout and place their data in your newly added pool. 

You may notice that object counts in your primary data pool (the one passed to ``fs new``) continue to increase, even if files are being created in the pool you added.  This is normal: the file data is stored in the pool specified by the layout, but a small amount of metadata is kept in the primary data pool for all files.


.. _cephfs-moving-a-file-to-another-pool:

Moving a file to another data pool
----------------------------------

The layout of a file that already holds data cannot be changed.  To move such a
file to another data pool, set the ``ceph.file.migrate.pool`` xattr on it:

.. code-block:: bash

    $ setfattr -n ceph.file.migrate.pool -v cephfs_data_ssd /mnt/cephfs/bigfile
    $ getfattr -n ceph.file.layout.pool /mnt/cephfs/bigfile
    # file: bigfile
    ceph.file.layout.pool="cephfs_data_ssd"

The pool must already be a data pool of the file system (see
:ref:`adding-data-pool-to-file-system`), and the client needs the ``p`` flag.
Only the pool changes: the inode number, the striping and the rest of the
layout stay as they are.  Naming the pool the file is already in does nothing,
and is not an error.

This works on one regular file.  To move every file below a directory, see
:ref:`cephfs-moving-a-tree-to-another-pool`.

.. note::

   This feature is new.  It has been tested with up to three active MDS ranks,
   but not at a large scale, and ``cephfs-data-scan`` has not been tested on
   pools that hold migrated files.

What happens to the file
~~~~~~~~~~~~~~~~~~~~~~~~

The MDS does the work, not the client.  ``setfattr`` returns once the data has
been copied and the new layout is safe in the MDS journal.  During that time:

* The MDS asks every client to flush any data it has buffered for the file, so
  nothing is lost and nothing arrives in the old pool afterwards.
* Other clients' reads and writes of the file wait until the copy has finished.
  Clients that have the file open carry on afterwards.
* A file of at least ``mds_file_migrate_precopy_min`` (64 MiB by default) that
  nobody has open for writing is copied first while clients keep using it.  The
  MDS then takes the file's locks only to check that it did not change, so
  readers wait for a moment rather than for the whole copy.  If the file did
  change, or the copy failed, it is copied again with the locks held.  Smaller
  files, and files open for writing, are always copied with the locks held, so
  their readers and writers wait for as long as the copy takes.

If the copy fails, ``setfattr`` returns the error and the file stays where it
was.  If the MDS fails during the copy, the file also stays where it was, but
what was already copied is left behind in the new pool.  This costs space and
does no other harm.  Running the migration again overwrites it.

After ``setfattr`` has returned, the MDS clears the old pool in the background.
If an MDS fails at this point, the next one picks up the work by itself, whether
or not the client is still there.  An object that is being recovered is cleared
once recovery reaches it.  If clients keep reading a snapshot object through the
old pool, the MDS retries every 5 seconds for 5 minutes.  It then gives up with a
warning in the cluster log (``migration of <inode> left objects in pool <pool
id>``).  The file is fine at that point: only the space is not freed.  A new migration of a file waits until an earlier one has finished
clearing its old pool.

The call is refused in these cases:

* ``EINVAL``: the path is not a regular file, the pool is not a data pool of the
  file system, the file is encrypted with fscrypt, or it has inline data.
* ``EBUSY``: a truncate of the file has not finished yet.  Try again.

Snapshots
~~~~~~~~~

When the MDS can find every snapshot version of the file, they all move with
it.  The file's contents at each snapshot do not change, the old pool's copy
is freed even though the snapshots are still there, and neither the file's
ctime nor the snapshots are touched.

When there is any doubt, only the current version of the file moves.  This is
always safe, but the snapshots keep their data in the old pool, and they do not
free it until they are deleted.  In the worst case, a file that has not changed
since a snapshot, the data is then stored twice: once in the new pool, and once
in the old pool for the snapshot.  Only the current version moves when:

* The file has hard links.
* The file was renamed after a snapshot of it was taken, because the older
  versions are then under another name or in another directory.
* A client has not finished writing back a snapshot of the file.
* An earlier migration left the file's snapshots in a different pool from the
  file.
* A snapshot that can see the directory was taken in the hour before the file
  was created.  Even though the file is new, the MDS cannot rule out that this
  snapshot holds an older version of it, so a file like this also leaves behind
  the snapshots taken after it was created.

To find out which case applied, read the snapshot's layout: if
``ceph.file.layout.pool`` of ``.snap/<snapshot>/<file>`` still names the old
pool, that snapshot stayed behind.

Snapshots take more space after a move.  In the old pool, a snapshot stores only
what has changed since, and shares the rest with the newer versions of the same
object.  The copies in the new pool do not share anything, so each snapshot that
differs from the current file is a full copy of each object that differs.  Before
moving a file with many snapshots, make sure the new pool can hold that.

The old pool stays
~~~~~~~~~~~~~~~~~~

A migration leaves something in the old pool, for as long as the file exists:

* When snapshots moved with the file, every object of the file stays as a small
  redirect to its new location.  A client that had the file open before the
  move, or that still has the old layout of a snapshot, is sent to the new pool
  by the OSD, so it can carry on without knowing that anything has changed.
* When only the current version moved, the first object stays, as it always
  does when a layout changes, along with any data held for snapshots.

So the old pool cannot be taken out of service just because its files have
moved.  ``ceph fs rm_data_pool`` refuses to remove a pool that still holds
objects, unless you add ``--yes-i-really-mean-it``.  If you force it and then
delete the pool, a client that still reads through a redirect, and a snapshot
that stayed behind, read zeros in place of the file's data, with no error.
Deleting a file removes what it left in the old pool, once its snapshots are
gone too.

.. _cephfs-moving-a-tree-to-another-pool:

Moving a directory tree to another data pool
--------------------------------------------

To move every regular file below a directory, set ``ceph.dir.migrate.pool`` on
the directory:

.. code-block:: bash

    $ setfattr -n ceph.dir.migrate.pool -v cephfs_data_ssd /mnt/cephfs/project

The MDS then migrates the files in the background, each one exactly as
``ceph.file.migrate.pool`` would, snapshots included.  ``setfattr`` returns once
the job has been recorded, not when it is done.  At the same time the directory's
own layout is set to the new pool, so files created in it from then on are
written there directly.  The pool, client and permission rules are those of a
single file.

How the walk works:

* The job is kept in the metadata pool, so it survives the failure of an MDS
  and carries on when the rank is back.  At most the directories that were in
  progress are walked again, which is safe: a file that is already in the
  target pool is skipped.
* With several active MDS ranks, each rank moves the files it is responsible
  for.  The job belongs to the rank that holds the directory, and that rank
  reports on all of it.  While a rank that holds part of the tree is down, that
  part waits for it.
* Hard links are followed, even out of the tree, and each file moves once.
  Snapshot directories are not walked: moving a file moves its snapshots, or
  says why it could not.
* A file that is moved or created below a directory after the walk has passed
  it is not found.  Setting ``ceph.dir.migrate.pool`` again once the job is done
  picks it up; the files already moved are skipped.
* Nothing new starts while the target pool is full, and little while the MDS
  cache or journal is too big.

Setting ``ceph.dir.migrate.pool`` again on the same directory with the same pool
while the job runs is not an error.  It fails with ``EBUSY`` if a job is running
on a directory above or below it, or on the same directory with another pool,
since nested jobs could pull files back and forth.

Watching and steering a job
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The rank that holds the directory shows the job and takes commands for it
(rank 0 when there is one active MDS):

.. code-block:: bash

    $ ceph tell mds.<fs_name>:<rank> migrate status
    $ ceph tell mds.<fs_name>:<rank> migrate pause [<id>]
    $ ceph tell mds.<fs_name>:<rank> migrate resume [<id>]
    $ ceph tell mds.<fs_name>:<rank> migrate abort [<id>]

``migrate status`` lists each job with its state (``running``, ``paused``,
``done`` or ``aborted``), its counters of directories and files, the errors it
met, the part of each other rank, and what is holding it back, if anything.
Without an ``<id>``, the other commands apply to every job of the rank.  Pause,
resume and abort reach the other ranks at once, or when they are back if they
are down.  Files in progress finish; after an abort, files already moved stay in
the new pool.

``mds_migrate_max_files`` (default 4) bounds the files that a rank's jobs move
at a time, and each file uses up to 16 operations on the OSDs at a time.  Set it
to 0 to stop the jobs from starting anything new without pausing them.
