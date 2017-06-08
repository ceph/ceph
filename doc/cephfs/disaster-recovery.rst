
Disaster recovery
=================

.. danger::

    The notes in this section are aimed at experts, making a best effort
    to recovery what they can from damaged filesystems.  These steps
    have the potential to make things worse as well as better.  If you
    are unsure, do not proceed.


Journal export
--------------

Before attempting dangerous operations, make a copy of the journal like so:

::

    cephfs-journal-tool journal export backup.bin

Note that this command may not always work if the journal is badly corrupted,
in which case a RADOS-level copy should be made (http://tracker.ceph.com/issues/9902).


Dentry recovery from journal
----------------------------

If a journal is damaged or for any reason an MDS is incapable of replaying it,
attempt to recover what file metadata we can like so:

::

    cephfs-journal-tool event recover_dentries summary

This command by default acts on MDS rank 0, pass --rank=<n> to operate on other ranks.

This command will write any inodes/dentries recoverable from the journal
into the backing store, if these inodes/dentries are higher-versioned
than the previous contents of the backing store.  If any regions of the journal
are missing/damaged, they will be skipped.

Note that in addition to writing out dentries and inodes, this command will update
the InoTables of each 'in' MDS rank, to indicate that any written inodes' numbers
are now in use.  In simple cases, this will result in an entirely valid backing
store state.

.. warning::

    The resulting state of the backing store is not guaranteed to be self-consistent,
    and an online MDS scrub will be required afterwards.  The journal contents
    will not be modified by this command, you should truncate the journal
    separately after recovering what you can.

Journal truncation
------------------

If the journal is corrupt or MDSs cannot replay it for any reason, you can
truncate it like so:

::

    cephfs-journal-tool journal reset

.. warning::

    Resetting the journal *will* lose metadata unless you have extracted
    it by other means such as ``recover_dentries``.  It is likely to leave
    some orphaned objects in the data pool.  It may result in re-allocation
    of already-written inodes, such that permissions rules could be violated.

MDS table wipes
---------------

After the journal has been reset, it may no longer be consistent with respect
to the contents of the MDS tables (InoTable, SessionMap, SnapServer).

To reset the SessionMap (erase all sessions), use:

::

    cephfs-table-tool all reset session

This command acts on the tables of all 'in' MDS ranks.  Replace 'all' with an MDS
rank to operate on that rank only.

The session table is the table most likely to need resetting, but if you know you
also need to reset the other tables then replace 'session' with 'snap' or 'inode'.

MDS map reset
-------------

Once the in-RADOS state of the filesystem (i.e. contents of the metadata pool)
is somewhat recovered, it may be necessary to update the MDS map to reflect
the contents of the metadata pool.  Use the following command to reset the MDS
map to a single MDS:

::

    ceph fs reset <fs name> --yes-i-really-mean-it

Once this is run, any in-RADOS state for MDS ranks other than 0 will be ignored:
as a result it is possible for this to result in data loss.

One might wonder what the difference is between 'fs reset' and 'fs remove; fs new'.  The
key distinction is that doing a remove/new will leave rank 0 in 'creating' state, such
that it would overwrite any existing root inode on disk and orphan any existing files.  In
contrast, the 'reset' command will leave rank 0 in 'active' state such that the next MDS
daemon to claim the rank will go ahead and use the existing in-RADOS metadata.

Recovery from missing metadata objects
--------------------------------------

Depending on what objects are missing or corrupt, you may need to
run various commands to regenerate default versions of the
objects.

::

    # Session table
    cephfs-table-tool 0 reset session
    # SnapServer
    cephfs-table-tool 0 reset snap
    # InoTable
    cephfs-table-tool 0 reset inode
    # Journal
    cephfs-journal-tool --rank=0 journal reset
    # Root inodes ("/" and MDS directory)
    cephfs-data-scan init

Finally, you can regenerate metadata objects for missing files
and directories based on the contents of a data pool.  This is
a two-phase process.  First, scanning *all* objects to calculate
size and mtime metadata for inodes.  Second, scanning the first
object from every file to collect this metadata and inject
it into the metadata pool.

::

    cephfs-data-scan scan_extents <data pool>
    cephfs-data-scan scan_inodes <data pool>

This command may take a very long time if there are many
files or very large files in the data pool.  To accelerate
the process, run multiple instances of the tool.  Decide on
a number of workers, and pass each worker a number within
the range 0-(N_workers - 1), like so:

::

    # Worker 0
    cephfs-data-scan scan_extents <data pool> 0 1
    # Worker 1
    cephfs-data-scan scan_extents <data pool> 1 1

    # Worker 0
    cephfs-data-scan scan_inodes <data pool> 0 1
    # Worker 1
    cephfs-data-scan scan_inodes <data pool> 1 1

It is important to ensure that all workers have completed the
scan_extents phase before any workers enter the scan_inodes phase.
