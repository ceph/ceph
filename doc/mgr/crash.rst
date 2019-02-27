Crash Module
============
The crash module collects information about daemon crashdumps and stores
it in the Ceph cluster for later analysis.

Daemon crashdumps are dumped in /var/lib/ceph/crash by default; this can
be configured with the option 'crash dir'.  Crash directories are named by
time and date and a randomly-generated UUID, and contain a metadata file
'meta' and a recent log file, with a "crash_id" that is the same.
This module allows the metadata about those dumps to be persisted in
the monitors' storage.

Enabling
--------

The *crash* module is enabled with::

  ceph mgr module enable crash

Commands
--------
::

  ceph crash post -i <metafile>

Save a crash dump.  The metadata file is a JSON blob stored in the crash
dir as ``meta``.  As usual, the ceph command can be invoked with ``-i -``,
and will read from stdin.

::

  ceph rm <crashid>

Remove a specific crash dump.

::

  ceph crash ls

List the timestamp/uuid crashids for all saved crash info.

::

  ceph crash stat

Show a summary of saved crash info grouped by age.

::

  ceph crash info <crashid>

Show all details of a saved crash.

::

   ceph crash prune <keep>

Remove saved crashes older than 'keep' days.  <keep> must be an integer.


