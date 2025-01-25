
.. _disaster-recovery-experts:

Advanced: Metadata repair tools
===============================

.. warning::

    If you do not have expert knowledge of CephFS internals, you will
    need to seek assistance before using any of these tools.

    The tools mentioned here can easily cause damage as well as fixing it.

    It is essential to understand exactly what has gone wrong with your
    file system before attempting to repair it.

    If you do not have access to professional support for your cluster,
    consult the ceph-users mailing list or the #ceph IRC/Slack channel.


Journal export
--------------

Before attempting any dangerous operation, make a copy of the journal by
running the following command:

.. prompt:: bash #

   cephfs-journal-tool journal export backup.bin

Dentry recovery from journal
----------------------------

If a journal is damaged or for any reason an MDS is incapable of replaying it,
attempt to recover file metadata by running the following command:

.. prompt:: bash #

   cephfs-journal-tool event recover_dentries summary

By default, this command acts on MDS rank ``0``. Pass the option ``--rank=<n>``
to the ``cephfs-journal-tool`` command to operate on other ranks.

This command writes all inodes and dentries recoverable from the journal into
the backing store, but only if these inodes and dentries are higher-versioned
than the existing contents of the backing store. Any regions of the journal
that are missing or damaged will be skipped.

In addition to writing out dentries and inodes, this command updates the
InoTables of each ``in`` MDS rank, to indicate that any written inodes' numbers
are now in use. In simple cases, this will result in an entirely valid backing
store state.

.. warning::

    The resulting state of the backing store is not guaranteed to be
    self-consistent, and an online MDS scrub will be required afterwards. The
    journal contents will not be modified by this command. Truncate the journal
    separately after recovering what you can.

Journal truncation
------------------

Use a command of the following form to truncate any journal that is corrupt or
that an MDS cannot replay:

.. prompt:: bash #

   cephfs-journal-tool [--rank=<fs_name>:{mds-rank|all}] journal reset --yes-i-really-really-mean-it

Specify the filesystem and the MDS rank using the ``--rank`` option when the
file system has or had multiple active MDS daemons.

.. warning::

    Resetting the journal *will* cause metadata to be lost unless you have
    extracted it by other means such as ``recover_dentries``. Resetting the
    journal is likely to leave orphaned objects in the data pool.  Resetting
    the journal may result in the re-allocation of already-written inodes,
    which means that permissions rules could be violated.

MDS table wipes
---------------

After the journal has been reset, it may no longer be consistent with respect
to the contents of the MDS tables (InoTable, SessionMap, SnapServer).

Use the following command to reset the SessionMap (this will erase all
sessions):

.. prompt:: bash #

    cephfs-table-tool all reset session

This command acts on the tables of all MDS ranks that are ``in``. To operate
only on a specified rank, replace ``all`` in the above command with an MDS
rank.

Of all tables, the session table is the table most likely to require a reset.
If you know that you need also to reset the other tables, then replace
``session`` with ``snap`` or ``inode``.

MDS map reset
-------------

When the in-RADOS state of the file system (that is, the contents of the
metadata pool) has been somewhat recovered, it may be necessary to update the
MDS map to reflect the new state of the metadata pool. Use the following
command to reset the MDS map to a single MDS:

.. prompt:: bash #

   ceph fs reset <fs name> --yes-i-really-mean-it

After this command has been run, any in-RADOS state for MDS ranks other than
``0`` will be ignored. This means that running this command can result in data
loss.

There is a difference between the effects of the ``fs reset`` command and the
``fs remove`` command. The ``fs reset`` command leaves rank ``0`` in the
``active`` state so that the next MDS daemon to claim the rank uses the
existing in-RADOS metadata. The ``fs remove`` command leaves rank ``0`` in the
``creating`` state, which means that existing root inodes on disk will be
overwritten. Running the ``fs remove`` command will orphan any existing files.

Recovery from missing metadata objects
--------------------------------------

Depending on which objects are missing or corrupt, you may need to run
additional commands to regenerate default versions of the objects.

::

    # Session table
    cephfs-table-tool 0 reset session
    # SnapServer
    cephfs-table-tool 0 reset snap
    # InoTable
    cephfs-table-tool 0 reset inode
    # Journal
    cephfs-journal-tool --rank=<fs_name>:0 journal reset --yes-i-really-really-mean-it
    # Root inodes ("/" and MDS directory)
    cephfs-data-scan init

Finally, you can regenerate metadata objects for missing files
and directories based on the contents of a data pool. This is
a three-phase process: 

#. Scanning *all* objects to calculate size and mtime metadata for inodes.  
#. Scanning the first object from every file to collect this metadata and
   inject it into the metadata pool. 
#. Checking inode linkages and fixing found errors.

::

    cephfs-data-scan scan_extents [<data pool> [<extra data pool> ...]]
    cephfs-data-scan scan_inodes [<data pool>]
    cephfs-data-scan scan_links

``scan_extents`` and ``scan_inodes`` commands may take a *very long* time if
the data pool contains many files or very large files.

To accelerate the process of running ``scan_extents`` or ``scan_inodes``, run
multiple instances of the tool:

Decide on a number of workers, and pass each worker a number within
the range ``0-(worker_m - 1)`` (that is, 'zero to "worker_m" minus 1').

The example below shows how to run four workers simultaneously:

::

    # Worker 0
    cephfs-data-scan scan_extents --worker_n 0 --worker_m 4
    # Worker 1
    cephfs-data-scan scan_extents --worker_n 1 --worker_m 4
    # Worker 2
    cephfs-data-scan scan_extents --worker_n 2 --worker_m 4
    # Worker 3
    cephfs-data-scan scan_extents --worker_n 3 --worker_m 4

    # Worker 0
    cephfs-data-scan scan_inodes --worker_n 0 --worker_m 4
    # Worker 1
    cephfs-data-scan scan_inodes --worker_n 1 --worker_m 4
    # Worker 2
    cephfs-data-scan scan_inodes --worker_n 2 --worker_m 4
    # Worker 3
    cephfs-data-scan scan_inodes --worker_n 3 --worker_m 4

It is **important** to ensure that all workers have completed the
``scan_extents`` phase before any worker enters the ``scan_inodes phase``.

After completing the metadata recovery process, you may want to run a cleanup
operation to delete ancillary data generated during recovery. Use a command of the following form to run a cleanup operation:

.. prompt:: bash #

   cephfs-data-scan cleanup [<data pool>]

.. note::

   The data pool parameters for ``scan_extents``, ``scan_inodes`` and
   ``cleanup`` commands are optional, and usually the tool will be able to
   detect the pools automatically. Still, you may override this. The
   ``scan_extents`` command requires that all data pools be specified, but the
   ``scan_inodes`` and ``cleanup`` commands require only that you specify the
   main data pool.


Using an alternate metadata pool for recovery
---------------------------------------------

.. warning::

   This procedure has not been extensively tested. It should be undertaken only
   with great care.

If an existing CephFS file system is damaged and inoperative, then it is
possible to create a fresh metadata pool and to attempt the reconstruction the
of the damaged and inoperative file system's metadata into the new pool, while
leaving the old metadata in place. This could be used to make a safer attempt
at recovery since the existing metadata pool would not be modified.

.. caution::

   During this process, multiple metadata pools will contain data referring to
   the same data pool. Extreme caution must be exercised to avoid changing the
   contents of the data pool while this is the case. After recovery is
   complete, archive or delete the damaged metadata pool.

#. Take down the existing file system in order to prevent any further
   modification of the data pool. Unmount all clients. When all clients have
   been unmounted, use the following command to mark the file system failed:

   .. prompt:: bash #

      ceph fs fail <fs_name>

   .. note::

      ``<fs_name>`` here and below refers to the original, damaged file system.

#. Create a recovery file system. This recovery file system will be used to
   recover the data in the damaged pool. First, the filesystem will have a data
   pool deployed for it. Then you will attacha new metadata pool to the new
   data pool. Then you will set the new metadata pool to be backed by the old
   data pool. 

   .. prompt:: bash #

      ceph osd pool create cephfs_recovery_meta
      ceph fs new cephfs_recovery cephfs_recovery_meta <data_pool> --recover --allow-dangerous-metadata-overlay

   .. note::

      You may rename the recovery metadata pool and file system at a future time.
      The ``--recover`` flag prevents any MDS daemon from joining the new file
      system.

#. Create the intial metadata for the file system:

   .. prompt:: bash #

      cephfs-table-tool cephfs_recovery:0 reset session

   .. prompt:: bash #

      cephfs-table-tool cephfs_recovery:0 reset snap

   .. prompt:: bash #
   
      cephfs-table-tool cephfs_recovery:0 reset inode

   .. prompt:: bash #

      cephfs-journal-tool --rank cephfs_recovery:0 journal reset --force --yes-i-really-really-mean-it

#. Use the following commands to rebuild the metadata pool from the data pool:

   .. prompt:: bash #

      cephfs-data-scan init --force-init --filesystem cephfs_recovery --alternate-pool cephfs_recovery_meta

   .. prompt:: bash #
   
      cephfs-data-scan scan_extents --alternate-pool cephfs_recovery_meta --filesystem <fs_name>

   .. prompt:: bash #
   
      cephfs-data-scan scan_inodes --alternate-pool cephfs_recovery_meta --filesystem <fs_name> --force-corrupt

   .. prompt:: bash #

      cephfs-data-scan scan_links --filesystem cephfs_recovery

   .. note::

      Each of the scan procedures above scans through the entire data pool.
      This may take a long time. See the previous section on how to distribute
      this task among workers.

   If the damaged file system contains dirty journal data, it may be recovered
   next with a command of the following form:

   .. prompt:: bash #

      cephfs-journal-tool --rank=<fs_name>:0 event recover_dentries list --alternate-pool cephfs_recovery_meta

#. After recovery, some recovered directories will have incorrect statistics.
   Ensure that the parameters ``mds_verify_scatter`` and
   ``mds_debug_scatterstat`` are set to false (the default) to prevent the MDS
   from checking the statistics:

   .. prompt:: bash #

      ceph config rm mds mds_verify_scatter

   .. prompt:: bash #

      ceph config rm mds mds_debug_scatterstat

   .. note::

      Verify that the config has not been set globally or with a local
      ``ceph.conf`` file.

#. Allow an MDS daemon to join the recovery file system:

   .. prompt:: bash #

      ceph fs set cephfs_recovery joinable true

#. Run a forward :doc:`scrub </cephfs/scrub>` to repair recursive statistics.
   Ensure that you have an MDS daemon running and issue the following command:

   .. prompt:: bash #

      ceph tell mds.cephfs_recovery:0 scrub start / recursive,repair,force

   .. note::

      The `Symbolic link recovery <https://tracker.ceph.com/issues/46166>`_ is
      supported starting in the Quincy release.

      Symbolic links were recovered as empty regular files before.

   It is recommended that you migrate any data from the recovery file system as
   soon as possible. Do not restore the old file system while the recovery file
   system is operational.

   .. note::

       If the data pool is also corrupt, some files may not be restored because
       the backtrace information associated with them is lost. If any data
       objects are missing (due to issues like lost placement groups on the
       data pool), the recovered files will contain holes in place of the
       missing data.

.. _Symbolic link recovery: https://tracker.ceph.com/issues/46166
