=======
Pacific
=======

Pacific is the 16th stable release of Ceph.  It is named after the
giant pacific octopus (Enteroctopus dofleini).


v16.2.0 Pacific
===============

This is the first stable release of Ceph Pacific.

Major Changes from Octopus
--------------------------

General
~~~~~~~

* Cephadm can automatically upgrade an Octopus cluster to Pacific with a single
  command to start the process.
* Cephadm has improved significantly over the past year, with improved
  support for RGW (standalone and multisite), and new support for NFS
  and iSCSI.  Most of these changes have already been backported to
  recent Octopus point releases, but with the Pacific release we will
  switch to backporting bug fixes only.
* :ref:`Packages <packages>` are built for the following distributions:

  - CentOS 8
  - Ubuntu 20.04 (Focal)
  - Ubuntu 18.04 (Bionic)
  - Debian Buster
  - :ref:`Container image <containers>` (based on CentOS 8)

  With the exception of Debian Buster, packages and containers are
  built for both x86_64 and aarch64 (arm64) architectures.

  Note that cephadm clusters may work on many other distributions,
  provided Python 3 and a recent version of Docker or Podman is
  available to manage containers.  For more information, see
  :ref:`cephadm-host-requirements`.


Dashboard
~~~~~~~~~

The :ref:`mgr-dashboard` brings improvements in the following management areas:

* Orchestrator/Cephadm:

  - Host management: maintenance mode, labels.
  - Services: display placement specification.
  - OSD: disk replacement, display status of ongoing deletion, and improved
    health/SMART diagnostics reporting.

* Official :ref:`mgr ceph api`:

  - OpenAPI v3 compliant.
  - Stability commitment starting from Pacific release.
  - Versioned via HTTP ``Accept`` header (starting with v1.0).
  - Thoroughly tested (>90% coverage and per Pull Request validation).
  - Fully documented.

* RGW:

  - Multi-site synchronization monitoring.
  - Management of multiple RGW daemons and their resources (buckets and users).
  - Bucket and user quota usage visualization.
  - Improved configuration of S3 tenanted users.

* Security (multiple enhancements and fixes resulting from a pen testing conducted by IBM):

  - Account lock-out after a configurable number of failed log-in attempts.
  - Improved cookie policies to mitigate XSS/CSRF attacks.
  - Reviewed and improved security in HTTP headers.
  - Sensitive information reviewed and removed from logs and error messages.
  - TLS 1.0 and 1.1 support disabled.
  - Debug mode when enabled triggers HEALTH_WARN.

* Pools:

  - Improved visualization of replication and erasure coding modes.
  - CLAY erasure code plugin supported.

* Alerts and notifications:

  - Alert triggered on MTU mismatches in the cluster network.
  - Favicon changes according cluster status.

* Other:

  - Landing page: improved charts and visualization.
  - Telemetry configuration wizard.
  - OSDs: management of individual OSD flags.
  - RBD: per-RBD image Grafana dashboards.
  - CephFS: Dirs and Caps displayed.
  - NFS: v4 support only (v3 backward compatibility planned).
  - Front-end: Angular 10 update.


RADOS
~~~~~

* Pacific introduces :ref:`bluestore-rocksdb-sharding`, which reduces disk space requirements.

* Ceph now provides QoS between client I/O and background operations via the
  mclock scheduler.

* The balancer is now on by default in upmap mode to improve distribution of
  PGs across OSDs.

* The output of ``ceph -s`` has been improved to show recovery progress in
  one progress bar. More detailed progress bars are visible via the
  ``ceph progress`` command.


RBD block storage
~~~~~~~~~~~~~~~~~

* Image live-migration feature has been extended to support external data
  sources.  Images can now be instantly imported from local files, remote
  files served over HTTP(S) or remote S3 buckets in ``raw`` (``rbd export v1``)
  or basic ``qcow`` and ``qcow2`` formats.  Support for ``rbd export v2``
  format, advanced QCOW features and ``rbd export-diff`` snapshot differentials
  is expected in future releases.

* Initial support for client-side encryption has been added.  This is based
  on LUKS and in future releases will allow using per-image encryption keys
  while maintaining snapshot and clone functionality -- so that parent image
  and potentially multiple clone images can be encrypted with different keys.

* A new persistent write-back cache is available.  The cache operates in
  a log-structured manner, providing full point-in-time consistency for the
  backing image.  It should be particularly suitable for PMEM devices.

* A Windows client is now available in the form of ``librbd.dll`` and
  ``rbd-wnbd`` (Windows Network Block Device) daemon.  It allows mapping,
  unmapping and manipulating images similar to ``rbd-nbd``.

* librbd API now offers quiesce/unquiesce hooks, allowing for coordinated
  snapshot creation.


RGW object storage
~~~~~~~~~~~~~~~~~~

* Initial support for S3 Select. See :ref:`s3-select-feature-table` for supported queries.

* Bucket notification topics can be configured as ``persistent``, where events
  are recorded in rados for reliable delivery.

* Bucket notifications can be delivered to SSL-enabled AMQP endpoints.

* Lua scripts can be run during requests and access their metadata.

* SSE-KMS now supports KMIP as a key management service.

* Multisite data logs can now be deployed on ``cls_fifo`` to avoid large omap
  cluster warnings and make their trimming cheaper. See ``rgw_data_log_backing``.


CephFS distributed file system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* The CephFS MDS modifies on-RADOS metadata such that the new format is no
  longer backwards compatible. It is not possible to downgrade a file system from
  Pacific (or later) to an older release.

* Multiple file systems in a single Ceph cluster is now stable. New Ceph
  clusters enable support for multiple file systems by default. Existing clusters
  must still set the "enable_multiple" flag on the FS. See also
  :ref:`cephfs-multifs`.

* A new ``mds_autoscaler`` ``ceph-mgr`` plugin is available for automatically
  deploying MDS daemons in response to changes to the ``max_mds`` configuration.
  Expect further enhancements in the future to simplify and automate MDS scaling.

* ``cephfs-top`` is a new utility for looking at performance metrics from CephFS
  clients. It is development preview quality and will have bugs. For more
  information, see :ref:`cephfs-top`.

* A new ``snap_schedule`` ``ceph-mgr`` plugin provides a command toolset for
  scheduling snapshots on a CephFS file system. For more information, see
  :ref:`snap-schedule`.

* First class NFS gateway support in Ceph is here! It's now possible to create
  scale-out ("active-active") NFS gateway clusters that export CephFS using
  a few commands. The gateways are deployed via cephadm (or Rook, in the future).
  For more information, see :ref:`cephfs-nfs`.

* Multiple active MDS file system scrub is now stable. It is no longer necessary
  to set ``max_mds`` to 1 and wait for non-zero ranks to stop. Scrub commands
  can only be sent to rank 0: ``ceph tell mds.<fs_name>:0 scrub start /path ...``.
  For more information, see :ref:`mds-scrub`.

* Ephemeral pinning -- policy based subtree pinning -- is considered stable.
  ``mds_export_ephemeral_random`` and ``mds_export_ephemeral_distributed`` now
  default to true. For more information, see :ref:`cephfs-ephemeral-pinning`.

* A new ``cephfs-mirror`` daemon is available to mirror CephFS file systems to
  a remote Ceph cluster. For more information, see :ref:`cephfs-mirroring`.

* A Windows client is now available for connecting to CephFS. This is offered
  through a new ``ceph-dokan`` utility which operates via the Dokan userspace
  API, similar to FUSE. For more information, see :ref:`ceph-dokan`.


Upgrading from Octopus or Nautilus
----------------------------------

Before starting, make sure your cluster is stable and healthy (no down or
recovering OSDs).  (This is optional, but recommended.)

Upgrading cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~

If your cluster is deployed with cephadm (first introduced in Octopus), then
the upgrade process is entirely automated.  To initiate the upgrade,

  .. prompt:: bash #

    ceph orch upgrade start --ceph-version 16.2.0

The same process is used to upgrade to future minor releases.

Upgrade progress can be monitored with ``ceph -s`` (which provides a simple
progress bar) or more verbosely with

  .. prompt:: bash #

    ceph -W cephadm

The upgrade can be paused or resumed with

  .. prompt:: bash #

    ceph orch upgrade pause   # to pause
    ceph orch upgrade resume  # to resume

or canceled with

  .. prompt:: bash #

    ceph orch upgrade stop

Note that canceling the upgrade simply stops the process; there is no ability to
downgrade back to Octopus.


Upgrading non-cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
   If you cluster is running Octopus (15.2.x), you might choose
   to first convert it to use cephadm so that the upgrade to Pacific
   is automated (see above).  For more information, see
   :ref:`cephadm-adoption`.

#. Set the ``noout`` flag for the duration of the upgrade. (Optional,
   but recommended.)::

     # ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example, on each monitor host,::

     # systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``octopus`` string in the mon
   map.  The command::

     # ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 16 (pacific)

   If it doesn't, that implies that one or more monitors hasn't been
   upgraded and restarted and/or the quorum does not include all monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and
   restarting all manager daemons.  For example, on each manager host,::

     # systemctl restart ceph-mgr.target

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph
   -s``::

     # ceph -s

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the
   ceph-osd daemons on all OSD hosts::

     # systemctl restart ceph-osd.target

   Note that if you are upgrading from Nautilus, the first time each
   OSD starts, it will do a format conversion to improve the
   accounting for "omap" data.  This may take a few minutes to as much
   as a few hours (for an HDD with lots of omap data).  You can
   disable this automatic conversion with::

     # ceph config set osd bluestore_fsck_quick_fix_on_mount false

   You can monitor the progress of the OSD upgrades with the
   ``ceph versions`` or ``ceph osd versions`` commands::

     # ceph osd versions
     {
        "ceph version 14.2.5 (...) nautilus (stable)": 12,
        "ceph version 16.2.0 (...) pacific (stable)": 22,
     }

#. Upgrade all CephFS MDS daemons. For each CephFS file system,

   #. Disable standby_replay:

   # ceph fs set <fs_name> allow_standby_replay false

   #. Reduce the number of ranks to 1.  (Make note of the original
      number of MDS daemons first if you plan to restore it later.)::

	# ceph status
	# ceph fs set <fs_name> max_mds 1

   #. Wait for the cluster to deactivate any non-zero ranks by
      periodically checking the status::

	# ceph status

   #. Take all standby MDS daemons offline on the appropriate hosts with::

	# systemctl stop ceph-mds@<daemon_name>

   #. Confirm that only one MDS is online and is rank 0 for your FS::

	# ceph status

   #. Upgrade the last remaining MDS daemon by installing the new
      packages and restarting the daemon::

        # systemctl restart ceph-mds.target

   #. Restart all standby MDS daemons that were taken offline::

	# systemctl start ceph-mds.target

   #. Restore the original value of ``max_mds`` for the volume::

	# ceph fs set <fs_name> max_mds <original_max_mds>

#. Upgrade all radosgw daemons by upgrading packages and restarting
   daemons on all hosts::

     # systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Pacific OSDs and enabling
   all new Pacific-only functionality::

     # ceph osd require-osd-release pacific

#. If you set ``noout`` at the beginning, be sure to clear it with::

     # ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment
   and orchestration framework to simplify cluster management and
   future upgrades.  For more information on converting an existing
   cluster to cephadm, see :ref:`cephadm-adoption`.


Post-upgrade
~~~~~~~~~~~~

#. Verify the cluster is healthy with ``ceph health``.

   If your CRUSH tunables are older than Hammer, Ceph will now issue a
   health warning.  If you see a health alert to that effect, you can
   revert this change with::

     ceph config set mon mon_crush_min_required_version firefly

   If Ceph does not complain, however, then we recommend you also
   switch any existing CRUSH buckets to straw2, which was added back
   in the Hammer release.  If you have any 'straw' buckets, this will
   result in a modest amount of data movement, but generally nothing
   too severe.::

     ceph osd getcrushmap -o backup-crushmap
     ceph osd crush set-all-straw-buckets-to-straw2

   If there are problems, you can easily revert with::

     ceph osd setcrushmap -i backup-crushmap

   Moving to 'straw2' buckets will unlock a few recent features, like
   the `crush-compat` :ref:`balancer <balancer>` mode added back in Luminous.

#. If you did not already do so when upgrading from Mimic, we
   recommened you enable the new :ref:`v2 network protocol <msgr2>`,
   issue the following command::

     ceph mon enable-msgr2

   This will instruct all monitors that bind to the old default port
   6789 for the legacy v1 protocol to also bind to the new 3300 v2
   protocol port.  To see if all monitors have been updated,::

     ceph mon dump

   and verify that each monitor has both a ``v2:`` and ``v1:`` address
   listed.

#. Consider enabling the :ref:`telemetry module <telemetry>` to send
   anonymized usage statistics and crash information to the Ceph
   upstream developers.  To see what would be reported (without actually
   sending any information to anyone),::

     ceph mgr module enable telemetry
     ceph telemetry show

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with::

     ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at
   `https://telemetry-public.ceph.com/ <https://telemetry-public.ceph.com/>`_.
 
   For more information about the telemetry module, see :ref:`the
   documentation <telemetry>`.


Upgrade from pre-Nautilus releases (like Mimic or Luminous)
-----------------------------------------------------------

You must first upgrade to Nautilus (14.2.z) or Octopus (15.2.z) before
upgrading to Pacific.


Notable Changes
---------------

* A new library is available, libcephsqlite. It provides a SQLite Virtual File
  System (VFS) on top of RADOS. The database and journals are striped over
  RADOS across multiple objects for virtually unlimited scaling and throughput
  only limited by the SQLite client. Applications using SQLite may change to
  the Ceph VFS with minimal changes, usually just by specifying the alternate
  VFS. We expect the library to be most impactful and useful for applications
  that were storing state in RADOS omap, especially without striping which
  limits scalability.

* New ``bluestore_rocksdb_options_annex`` config parameter. Complements
  ``bluestore_rocksdb_options`` and allows setting rocksdb options without
  repeating the existing defaults.

* $pid expansion in config paths like ``admin_socket`` will now properly expand
  to the daemon pid for commands like ``ceph-mds`` or ``ceph-osd``. Previously
  only ``ceph-fuse``/``rbd-nbd`` expanded ``$pid`` with the actual daemon pid.

* The allowable options for some ``radosgw-admin`` commands have been changed.

  * ``mdlog-list``, ``datalog-list``, ``sync-error-list`` no longer accepts
    start and end dates, but does accept a single optional start marker.
  * ``mdlog-trim``, ``datalog-trim``, ``sync-error-trim`` only accept a
    single marker giving the end of the trimmed range.
  * Similarly the date ranges and marker ranges have been removed on
    the RESTful DATALog and MDLog list and trim operations.

* ceph-volume: The ``lvm batch`` subcommand received a major rewrite. This
  closed a number of bugs and improves usability in terms of size specification
  and calculation, as well as idempotency behaviour and disk replacement 
  process.
  Please refer to https://docs.ceph.com/en/latest/ceph-volume/lvm/batch/ for
  more detailed information.

* Configuration variables for permitted scrub times have changed.  The legal
  values for ``osd_scrub_begin_hour`` and ``osd_scrub_end_hour`` are 0 - 23.
  The use of 24 is now illegal.  Specifying ``0`` for both values causes every
  hour to be allowed.  The legal values for ``osd_scrub_begin_week_day`` and
  ``osd_scrub_end_week_day`` are 0 - 6.  The use of 7 is now illegal.
  Specifying ``0`` for both values causes every day of the week to be allowed.

* volume/nfs: Recently "ganesha-" prefix from cluster id and nfs-ganesha common
  config object was removed, to ensure consistent namespace across different
  orchestrator backends. Please delete any existing nfs-ganesha clusters prior
  to upgrading and redeploy new clusters after upgrading to Pacific.

* A new health check, DAEMON_OLD_VERSION, will warn if different versions of Ceph are running
  on daemons. It will generate a health error if multiple versions are detected.
  This condition must exist for over mon_warn_older_version_delay (set to 1 week by default) in order for the
  health condition to be triggered.  This allows most upgrades to proceed
  without falsely seeing the warning.  If upgrade is paused for an extended
  time period, health mute can be used like this
  "ceph health mute DAEMON_OLD_VERSION --sticky".  In this case after
  upgrade has finished use "ceph health unmute DAEMON_OLD_VERSION".

* MGR: progress module can now be turned on/off, using the commands:
  ``ceph progress on`` and ``ceph progress off``.

* An AWS-compliant API: "GetTopicAttributes" was added to replace the existing "GetTopic" API. The new API
  should be used to fetch information about topics used for bucket notifications.

* librbd: The shared, read-only parent cache's config option ``immutable_object_cache_watermark`` now has been updated
  to property reflect the upper cache utilization before space is reclaimed. The default ``immutable_object_cache_watermark``
  now is ``0.9``. If the capacity reaches 90% the daemon will delete cold cache.

* OSD: the option ``osd_fast_shutdown_notify_mon`` has been introduced to allow
  the OSD to notify the monitor it is shutting down even if ``osd_fast_shutdown``
  is enabled. This helps with the monitor logs on larger clusters, that may get
  many 'osd.X reported immediately failed by osd.Y' messages, and confuse tools.

* The mclock scheduler has been refined. A set of built-in profiles are now available that
  provide QoS between the internal and external clients of Ceph. To enable the mclock
  scheduler, set the config option "osd_op_queue" to "mclock_scheduler". The
  "high_client_ops" profile is enabled by default, and allocates more OSD bandwidth to
  external client operations than to internal client operations (such as background recovery
  and scrubs). Other built-in profiles include "high_recovery_ops" and "balanced". These
  built-in profiles optimize the QoS provided to clients of mclock scheduler.

* The balancer is now on by default in upmap mode. Since upmap mode requires
  ``require_min_compat_client`` luminous, new clusters will only support luminous
  and newer clients by default. Existing clusters can enable upmap support by running
  ``ceph osd set-require-min-compat-client luminous``. It is still possible to turn
  the balancer off using the ``ceph balancer off`` command. In earlier versions,
  the balancer was included in the ``always_on_modules`` list, but needed to be
  turned on explicitly using the ``ceph balancer on`` command.

* Version 2 of the cephx authentication protocol (``CEPHX_V2`` feature bit) is
  now required by default.  It was introduced in 2018, adding replay attack
  protection for authorizers and making msgr v1 message signatures stronger
  (CVE-2018-1128 and CVE-2018-1129).  Support is present in Jewel 10.2.11,
  Luminous 12.2.6, Mimic 13.2.1, Nautilus 14.2.0 and later; upstream kernels
  4.9.150, 4.14.86, 4.19 and later; various distribution kernels, in particular
  CentOS 7.6 and later.  To enable older clients, set ``cephx_require_version``
  and ``cephx_service_require_version`` config options to 1.

* `blacklist` has been replaced with `blocklist` throughout.  The following commands have changed:

  - ``ceph osd blacklist ...`` are now ``ceph osd blocklist ...``
  - ``ceph <tell|daemon> osd.<NNN> dump_blacklist`` is now ``ceph <tell|daemon> osd.<NNN> dump_blocklist``

* The following config options have changed:

  - ``mon osd blacklist default expire`` is now ``mon osd blocklist default expire``
  - ``mon mds blacklist interval`` is now ``mon mds blocklist interval``
  - ``mon mgr blacklist interval`` is now ''mon mgr blocklist interval``
  - ``rbd blacklist on break lock`` is now ``rbd blocklist on break lock``
  - ``rbd blacklist expire seconds`` is now ``rbd blocklist expire seconds``
  - ``mds session blacklist on timeout`` is now ``mds session blocklist on timeout``
  - ``mds session blacklist on evict`` is now ``mds session blocklist on evict``

* The following librados API calls have changed:

  - ``rados_blacklist_add`` is now ``rados_blocklist_add``; the former will issue a deprecation warning and be removed in a future release.
  - ``rados.blacklist_add`` is now ``rados.blocklist_add`` in the C++ API.

* The JSON output for the following commands now shows ``blocklist`` instead of ``blacklist``:

  - ``ceph osd dump``
  - ``ceph <tell|daemon> osd.<N> dump_blocklist``

* Monitors now have config option ``mon_allow_pool_size_one``, which is disabled
  by default. However, if enabled, user now have to pass the
  ``--yes-i-really-mean-it`` flag to ``osd pool set size 1``, if they are really
  sure of configuring pool size 1.

* ``ceph pg #.# list_unfound`` output has been enhanced to provide
  might_have_unfound information which indicates which OSDs may
  contain the unfound objects.

* OSD: A new configuration option ``osd_compact_on_start`` has been added which triggers
  an OSD compaction on start. Setting this option to ``true`` and restarting an OSD
  will result in an offline compaction of the OSD prior to booting.

* OSD: the option named ``bdev_nvme_retry_count`` has been removed. Because
  in SPDK v20.07, there is no easy access to bdev_nvme options, and this
  option is hardly used, so it was removed.

* Alpine build related script, documentation and test have been removed since
  the most updated APKBUILD script of Ceph is already included by Alpine Linux's
  aports repository.

