.. _cephfs-damaged-rank:

=============
Damaged Ranks
=============

A file system rank is marked *damaged* when an MDS daemon encounters
unrecoverable errors while reading the rank's metadata from the
metadata pool: for example, a journal that cannot be replayed or
on-disk structures that are invalid or missing. The daemon reports the
condition to the Monitors and respawns as a standby. The Monitors then
record the rank in the file system map's ``damaged`` set and blocklist
the reporting daemon.

While a rank is damaged, no MDS daemon will be assigned to it, not
even an available standby. The rank stays offline and the file system
is degraded: client access to the metadata served by that rank fails
until an operator intervenes.

Rank damage versus metadata damage
==================================

Two related but distinct conditions are both reported as damage:

- **A damaged rank**: the rank itself is offline, held in the
  ``damaged`` set of the file system map. The cluster reports the
  ``MDS_DAMAGE`` health error ("mds daemon damaged"), and the rank
  appears as ``damaged`` in ``ceph fs status`` and in the ``damaged``
  set shown by ``ceph fs dump``.
- **Metadata damage found by a running rank**: an active MDS that
  fails to load an individual dentry, directory fragment, or backtrace
  isolates the damaged subtree, records it in its damage table, and
  raises the "Metadata damage detected" health message. The rest of
  the file system remains available, but client access to the damaged
  subtree returns I/O errors. See :ref:`cephfs-health-messages`.

Inspecting damage
=================

To see which ranks are damaged:

.. prompt:: bash #

   ceph health detail
   ceph fs status
   ceph fs dump

To list the metadata damage recorded by a running MDS rank:

.. prompt:: bash #

   ceph tell mds.<id> damage ls

Each entry has an ID and one of the following types:

- ``dir_frag``: a directory fragment object could not be loaded, so
  the directory's contents are unavailable.
- ``dentry``: a single directory entry could not be loaded.
- ``backtrace``: an object's backtrace, used to resolve hard links
  and lookups by inode, is missing or corrupt.
- ``uninline``: a data uninlining operation failed for a file.

Repairing damage
================

.. warning:: Marking a rank repaired does not fix anything by itself.
   It only clears the damaged flag so that a daemon may try to take
   the rank again. If the underlying metadata problem has not been
   resolved, the rank will be marked damaged again as soon as an MDS
   attempts to load it.

The general workflow is:

#. Diagnose and repair the underlying metadata problem. For damage
   recorded in the damage table, a scrub with repair may resolve it;
   see :ref:`mds-scrub`:

   .. prompt:: bash #

      ceph tell mds.<fs_name>:0 scrub start / recursive,repair

   For journal or MDS table corruption that caused the rank itself to
   be marked damaged, follow :ref:`cephfs-disaster-recovery` and, for
   the low-level tooling, :ref:`disaster-recovery-experts`. Consider
   seeking expert advice before using those tools; some of the steps
   are destructive.

#. Once the cause has been addressed, clear the damaged flag on the
   rank:

   .. prompt:: bash #

      ceph mds repaired <fs_name>:<rank>

   A standby daemon will then take the rank and attempt to bring it
   back online.

#. Individual damage table entries can be removed after the underlying
   metadata has been verified or repaired:

   .. prompt:: bash #

      ceph tell mds.<id> damage rm <damage_id>
