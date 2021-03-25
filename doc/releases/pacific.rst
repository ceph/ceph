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

RADOS
~~~~~

RBD block storage
~~~~~~~~~~~~~~~~~

* 

RGW object storage
~~~~~~~~~~~~~~~~~~


CephFS distributed file system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* cephfs-mirror...


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

#. Upgrade all CephFS MDS daemons.  For each CephFS file system,

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


#. If you did you already do so when upgrading from Mimic, we
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

#. Consider transitioning your cluster to use the cephadm deployment
   and orchestration framework to simplify cluster management and
   future upgrades.  For more information on converting an existing
   cluster to cephadm, see :ref:`cephadm-adoption`.


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

* The cephfs addes two new CDentry tags, 'I' --> 'i' and 'L' --> 'l', and
  on-RADOS metadata is no longer backwards compatible after upgraded to Pacific
  or a later release.

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

* Multiple file systems in a single Ceph cluster is now stable. New Ceph clusters
  enable support for multiple file systems by default. Existing clusters
  must still set the "enable_multiple" flag on the fs. Please see the CephFS
  documentation for more information.

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


