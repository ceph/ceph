Crash Module
============

The crash module collects information about daemon crashdumps and stores
it in the Ceph cluster for later analysis.

Authentication Setup
--------------------

The *crash* module is always on, and ``ceph mgr module disable crash`` is
refused. In Squid 19.2.3 and later releases, an always-on module can be turned
off with ``ceph mgr module force disable crash --yes-i-really-mean-it``.
Without ``--yes-i-really-mean-it``, the command is refused with a warning that
it disables operations and removes commands that other Ceph utilities expect,
and that it is meant only for troubleshooting.
``ceph mgr module enable crash`` turns the module back on.

On a cluster deployed with cephadm, nothing needs to be set up: by default,
cephadm deploys a ``crash`` daemon on every host and creates its key,
``client.crash.<host>`` (``<host>`` is the name shown by
``ceph orch host ls``), with ``mon 'profile crash'`` and
``mgr 'profile crash'`` capabilities. On other clusters, ``ceph-crash``
needs a key to post crash reports.

Generate the *crash* upload key by running the following command:

.. prompt:: bash #

   ceph auth get-or-create client.crash mon 'profile crash' mgr 'profile crash'

Store this key in ``/etc/ceph/ceph.client.crash.keyring`` on each node in your
cluster.


Automated collection
--------------------

Daemons write crash dumps to the directory set by the ``crash_dir`` option,
``/var/lib/ceph/crash`` by default. On hosts managed by cephadm, the
directory is ``/var/lib/ceph/<cluster-fsid>/crash`` on the host, and cephadm
mounts it at ``/var/lib/ceph/crash`` inside the daemon containers. Crash
directories are named by time and date and a randomly-generated UUID, and
contain a metadata file 'meta' and a recent log file, with a "crash_id" that
is the same.

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

.. warning::

   A removed crash report cannot be recovered from the cluster.

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


.. warning::

   Pruned crash reports cannot be recovered from the cluster.

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

.. mgr_module:: crash

.. confval:: warn_recent_interval

   The default, 1209600 seconds, is two weeks.

.. confval:: retain_interval

   The default, 31536000 seconds, is one year.
