
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

.. note:: The Ceph file system must be offline before metadata repair tools can
   be used on it. The tools will complain if they are invoked when the file
   system is online. If any of the recovery steps do not complete successfully,
   DO NOT proceeed to run any more recovery steps. If any recovery step fails,
   seek help from experts via mailing lists and IRC channels and Slack
   channels.

Journal export
--------------

Before attempting any dangerous operation, make a copy of the journal by
running the following command:

.. prompt:: bash #

   cephfs-journal-tool journal export backup.bin

The backed up journal will come in handy in case the recovery procedure does
not go as expected. The MDS journal can be restored to its original state by
importing from the backed up journal:

.. prompt:: bash #

   cephfs-journal-tool journal import backup.bin

Dentry recovery from journal
----------------------------

If a journal is damaged or for any reason an MDS is incapable of replaying it,
attempt to recover file metadata by running the following command:

.. prompt:: bash #

   cephfs-journal-tool event recover_dentries summary

By default, this command acts on MDS rank ``0``. Pass the option ``--rank=<n>``
to the ``cephfs-journal-tool`` command to operate on other ranks or pass
``--rank=all`` to operate on all MDS ranks.

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

.. warning:: Resetting the journal *will* cause metadata to be lost unless the
   journal data has been extracted by other means such as ``recover_dentries``.
   Resetting the journal is likely to leave orphaned objects in the data pool
   and could result in the re-allocation of already-written inodes resulting in
   faulty behaviour of the file system (bugs, etc..).

MDS table wipes
---------------

It is not the case that every MDS table must reset during the recovery
procedure. A reset of an MDS table is required if the corresponding RADOS
object is either missing (say, due to some PGs getting lost) or if the tables
are inconsistent with the metadata stored in the RADOS back-end. To check for
missing table objects, run commands of the following forms:

Session table:

.. prompt:: bash #

   rados -p <metadata-pool> stat mds0_sessionmap

Inode table:

.. prompt:: bash #

   rados -p <metadata-pool> stat mds0_inotable

Snap table:

.. prompt:: bash #

   rados -p <metadata-pool> stat mds_snaptable

.. note:: The ``sessionmap`` and the ``inotable`` objects are per MDS rank (the
   object names have rank number - mds0_inotable, mds1_inotable, etc..).

Even if a table object exists, it can be inconsistent with the metadata stored
in the RADOS back-end. However, it is hard to detect inconsistency without
bringing the file system online if the metadata is in fact inconsistent or
corrupted. In cases like this, the MDS marks itself as `down:damaged`. To reset
individual tables, run commands of the following forms:

Session table:

.. prompt:: bash #

   cephfs-table-tool 0 reset session

SnapServer:

.. prompt:: bash #

   cephfs-table-tool 0 reset snap

InoTable:

.. prompt:: bash #

   cephfs-table-tool 0 reset inode

The above commands act on the tables of a particular MDS rank. To operate on
all MDS ranks that are in the ``in`` state, replace the MDS rank in the above
commands with ``all``, as shown in the following commands:

Session table:

.. prompt:: bash #

   cephfs-table-tool all reset session

SnapServer:

.. prompt:: bash #

   cephfs-table-tool all reset snap

InoTable:

.. prompt:: bash #

   cephfs-table-tool all reset inode

.. note:: Remount or restart all CephFS clients after the session tables have
   been reset. 

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

If the root inode or MDS directory (``~mdsdir``) is missing or corrupt, run the following command: 

Root inodes ("/" and MDS directory):

.. prompt:: bash #

   cephfs-data-scan init

This is generally safe to run, because this command skips generating the root
inode and the mdsdir inode if they exist. But if these inodes are corrupt, then
they must be regenerated. Corruption can be identified by trying to bring the
file system back online (at which point the MDS would transition into
``down:damaged`` with an appropriate log message [in the MDS log] pointing at
potential issues when loading the root inode or the mdsdir inode).  Another
method is to use the ``ceph-dencoder`` tool to decode the inodes.  This step is
a bit more involved.

Finally, you can regenerate metadata objects for missing files and directories
based on the contents of a data pool. This is a three-phase process: 

#. Scan *all* objects to calculate size and mtime metadata for inodes:

   .. prompt:: bash #

      cephfs-data-scan scan_extents [<data pool> [<extra data pool> ...]]
#. Scan the first object from every file to collect this metadata and
   inject it into the metadata pool:

   .. prompt:: bash #

      cephfs-data-scan scan_inodes [<data pool>]
#. Check inode linkages and fixing found errors:

   .. prompt:: bash #

      cephfs-data-scan scan_links

The ``scan_extents`` and ``scan_inodes`` commands may take a *very long* time
if the data pool contains many files or very large files.

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
``scan_extents`` phase before any worker enters the ``scan_inodes`` phase.

After completing the metadata recovery process, you may want to run a cleanup
operation to delete ancillary data generated during recovery. Use a command of
the following form to run a cleanup operation:

.. prompt:: bash #

   cephfs-data-scan cleanup [<data pool>]

The cleanup phase can be run with multiple instances to speed up execution::

    # Worker 0
    cephfs-data-scan cleanup --worker_n 0 --worker_m 4
    # Worker 1
    cephfs-data-scan cleanup --worker_n 1 --worker_m 4
    # Worker 2
    cephfs-data-scan cleanup --worker_n 2 --worker_m 4
    # Worker 3
    cephfs-data-scan cleanup --worker_n 3 --worker_m 4

.. note::

   The data pool parameters for ``scan_extents``, ``scan_inodes`` and
   ``cleanup`` commands are optional, and usually the tool will be able to
   detect the pools automatically. Still, you may override this. The
   ``scan_extents`` command requires that all data pools be specified, but the
   ``scan_inodes`` and ``cleanup`` commands require only that you specify the
   main data pool.

Known Limitations And Pitfalls
------------------------------

The disaster recovery process can be time consuming and daunting. The recovery
steps must be executed in the exact order as detailed above with the utmost
care, to ensure that failure in any step be understood clearly. You must be
able to decide with absolute certainty whether it is safe to proceed. If you
are confident that you can meet these challenges, study this list of
limitations and pitfalls before attempting disaster recovery:

#. The data-scan commands provide no way of estimating the time to completion
   of their operation. A feature that will provide such an estimate is under
   development. See https://tracker.ceph.com/issues/63191 for details.
#. It is important to perform a file system scrub after recovery before CephFS
   clients start using the file system.
#. In general, we do not recommend that you change any MDS-related settings
   (for example, ``max_mds``) while things are broken.
#. Disaster recovery is currently a manual process. There is a plan to automate
   the recovery via the Disaster Recovery Super Tool. See
   https://tracker.ceph.com/issues/71804 for details.
#. A well-known trick (used by some community users) is to use the disaster
   recovery procedure (especially the ``recover_dentries`` step) when the MDS
   is somewhat stuck in the ``up_replay`` state due to a long journal. Before
   jumping onto invoking ``recover_dentries`` when the MDS takes a bit long to
   replay the journal, consider trying to speed up journal replay by following
   the procedure detailed in :ref:`cephfs_dr_stuck_during_recovery`.

Using an alternate metadata pool for recovery
---------------------------------------------

.. warning:: This procedure has not been extensively tested. We recommend 
   recovering the file system using the recovery procedure detailed above
   unless there is a good reason not to do so. This procedure should be
   undertaken with great care.

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
