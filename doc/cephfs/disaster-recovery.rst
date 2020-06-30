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

If a PG is lost in a *data* pool, then the file system will continue
to operate normally, but some parts of some files will simply
be missing (reads will return zeros).

Losing a data PG may affect many files.  Files are split into many objects,
so identifying which files are affected by loss of particular PGs requires
a full scan over all object IDs that may exist within the size of a file. 
This type of scan may be useful for identifying which files require
restoring from a backup.

.. danger::

    This command does not repair any metadata, so when restoring files in
    this case you must *remove* the damaged file, and replace it in order
    to have a fresh inode.  Do not overwrite damaged files in place.

If you know that objects have been lost from PGs, use the ``pg_files``
subcommand to scan for files that may have been damaged as a result:

::

    cephfs-data-scan pg_files <path> <pg id> [<pg id>...]

For example, if you have lost data from PGs 1.4 and 4.5, and you would like
to know which files under /home/bob might have been damaged:

::

    cephfs-data-scan pg_files /home/bob 1.4 4.5

The output will be a list of paths to potentially damaged files, one
per line.

Note that this command acts as a normal CephFS client to find all the
files in the file system and read their layouts, so the MDS must be
up and running.

