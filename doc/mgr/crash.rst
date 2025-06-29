Crash Module
============

The crash module collects information about daemon crashdumps and stores
it in the Ceph cluster for later analysis.

Authentication Setup
--------------------

The *crash* module is one of the modules which are always on and cannot be
disabled. To enable crash report archiving, you must first generate a keyring
with appropriate privileges.

Generate the *crash* upload key by running the following command:

.. prompt:: bash #

   ceph auth get-or-create client.crash mon 'profile crash' mgr 'profile crash'

Store this key in ``/etc/ceph/ceph.client.crash.keyring`` on each node in your
cluster.


Automated collection
--------------------

Daemon crashdumps are dumped in ``/var/lib/ceph/crash`` by default; this can
be configured with the option ``crash dir``.  Crash directories are named by
time and date and a randomly-generated UUID, and contain a metadata file
'meta' and a recent log file, with a "crash_id" that is the same.

These crashes can be automatically submitted and persisted in the monitors'
storage by using ``ceph-crash.service``.
It watches the crashdump directory and uploads them with ``ceph crash post``.

``ceph-crash`` tries some authentication names: ``client.crash.$hostname``,
``client.crash`` and ``client.admin``.
In order to successfully upload with ``ceph crash post``, these need
the suitable permissions: ``mon profile crash`` and ``mgr profile crash``
and a keyring needs to be in ``/etc/ceph``.


Commands
--------

.. prompt:: bash #

   ceph crash post -i <metafile>

Save a crash dump.  The metadata file is a JSON blob stored in the crash
dir as ``meta``.  As usual, the ceph command can be invoked with ``-i -``,
and will read from stdin.

.. prompt:: bash #

   ceph crash rm <crashid>

Remove a specific crash dump.

.. prompt:: bash #

   ceph crash ls

List the timestamp/uuid crashids for all new and archived crash info.


.. prompt:: bash #

   ceph crash ls-new

List the timestamp/uuid crashids for all newcrash info.


.. prompt:: bash #

   ceph crash stat

Show a summary of saved crash info grouped by age.

.. prompt:: bash #

   ceph crash info <crashid>

Show all details of a saved crash.


.. prompt:: bash #

   ceph crash prune <keep>

Remove saved crashes older than 'keep' days.  <keep> must be an integer.

.. prompt:: bash #

   ceph crash archive <crashid>

Archive a crash report so that it is no longer considered for the ``RECENT_CRASH`` health check and does not appear in the ``crash ls-new`` output (it will still appear in the ``crash ls`` output).

.. prompt:: bash #

   ceph crash archive-all

Archive all new crash reports.


Options
-------

* ``mgr/crash/warn_recent_interval`` [default: 2 weeks] controls what constitutes "recent" for the purposes of raising the ``RECENT_CRASH`` health warning.
* ``mgr/crash/retain_interval`` [default: 1 year] controls how long crash reports are retained by the cluster before they are automatically purged.
