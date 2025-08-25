.. _cephfs-disaster-recovery:

Disaster recovery
=================

Metadata damage and repair
--------------------------

If a file system has inconsistent or missing metadata, it is considered
*damaged*.  You may find out about damage from a health message, or in some
unfortunate cases from an assertion in a running MDS daemon.

Metadata damage can result either from data loss in the underlying RADOS
layer (e.g. multiple disk failures that lose all copies of a PG), or from
software bugs.

CephFS includes some tools that may be able to recover a damaged file system,
but to use them safely requires a solid understanding of CephFS internals.
The documentation for these potentially dangerous operations is on a
separate page: :ref:`disaster-recovery-experts`.

Data pool damage (files affected by lost data PGs)
--------------------------------------------------

If a PG is lost in a *data* pool, then the file system continues to operate
normally, but some parts of some files will simply be missing (reads will
return zeros).

Losing a data pool PG may affect many files. Files are split into many RADOS
objects, so identifying which files have been affected by the loss of
particular PGs requires a scan of all RADOS objects storing data for the file.
This type of scan may be useful for identifying which files must be restored
from a backup.

.. danger::

    This command does not repair any metadata, so when restoring files in
    this case you must *remove* the damaged file and replace it in order
    to have a fresh inode. Do not overwrite damaged files in place.

If you know that objects have been lost from PGs, use the ``pg_files``
subcommand to scan for the files that may have been damaged as a result:

::

    cephfs-data-scan pg_files <path> <pg id> [<pg id>...]

For example, if you have lost data from PGs 1.4 and 4.5 and you want to know
which files under ``/home/bob`` have been damaged:

::

    cephfs-data-scan pg_files /home/bob 1.4 4.5

The output is a list of paths to potentially damaged files. One file is listed
per line.

.. note:: 

   This command acts as a normal CephFS client to find all the files in the
   file system and read their layouts. This means that the MDS must be up and
   running in order for this command to be usable.

Using first-damage.py
---------------------

#. Unmount all clients.

#. Flush the journal if possible:

   .. prompt:: bash #
      
      ceph tell mds.<fs_name>:0 flush journal

#. Fail the file system:

   .. prompt:: bash #

      ceph fs fail <fs_name>

#. Recover dentries from the journal. If the MDS flushed the journal
   successfully, this will be a no-op:

   .. prompt:: bash #

      cephfs-journal-tool --rank=<fs_name>:0 event recover_dentries summary

#. Reset the journal:
   
   .. prompt:: bash #

      cephfs-journal-tool --rank=<fs_name>:0 journal reset --yes-i-really-mean-it

#. Run ``first-damage.py`` to list damaged dentries:

   .. prompt:: bash #

      python3 first-damage.py --memo run.1 <pool>

#. Optionally, remove the damaged dentries:

   .. prompt:: bash #

      python3 first-damage.py --memo run.2 --remove <pool>

   .. note:: use ``--memo`` to specify a different file to save objects that
      have already been traversed. This makes it possible to separate data made
      during different, independent runs.

      This command has the effect of removing a dentry from the snapshot or
      head (in the current hierarchy). The inode's linkage will be lost. The
      inode may however be recoverable in ``lost+found`` during a future
      data-scan recovery.
