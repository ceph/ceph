======
Quincy
======

Quincy is the 17th stable release of Ceph.  It is named after Squidward
Quincy Tentacles from Spongebob Squarepants.

v17.2.0 Quincy
==============

This is the first stable release of Ceph Quincy.

Major Changes from Pacific
--------------------------

General
~~~~~~~

* Filestore has been deprecated in Quincy. BlueStore is Ceph's default object
  store.

* The `ceph-mgr-modules-core` debian package no longer recommends
  `ceph-mgr-rook`. `ceph-mgr-rook` depends on `python3-numpy`, which
  cannot be imported in different Python sub-interpreters multiple times
  when the version of `python3-numpy` is older than 1.19. Because
  `apt-get` installs the `Recommends` packages by default, `ceph-mgr-rook`
  was always installed along with the `ceph-mgr` debian package as an
  indirect dependency. If your workflow depends on this behavior, you
  might want to install `ceph-mgr-rook` separately.

* The ``device_health_metrics`` pool has been renamed ``.mgr``. It is now
  used as a common store for all ``ceph-mgr`` modules. After upgrading to
  Quincy, the ``device_health_metrics`` pool will be renamed to ``.mgr``
  on existing clusters.

* The ``ceph pg dump`` command now prints three additional columns:
  `LAST_SCRUB_DURATION` shows the duration (in seconds) of the last completed
  scrub;
  `SCRUB_SCHEDULING` conveys whether a PG is scheduled to be scrubbed at a
  specified time, whether it is queued for scrubbing, or whether it is being
  scrubbed;
  `OBJECTS_SCRUBBED` shows the number of objects scrubbed in a PG after a
  scrub begins.

* A health warning is now reported if the ``require-osd-release`` flag
  is not set to the appropriate release after a cluster upgrade.

* LevelDB support has been removed. ``WITH_LEVELDB`` is no longer a supported
  build option. Users *should* migrate their monitors and OSDs to RocksDB
  before upgrading to Quincy.

* Cephadm: ``osd_memory_target_autotune`` is enabled by default, which sets
  ``mgr/cephadm/autotune_memory_target_ratio`` to ``0.7`` of total RAM. This
  is unsuitable for hyperconverged infrastructures. For hyperconverged Ceph,
  please refer to the documentation or set
  ``mgr/cephadm/autotune_memory_target_ratio`` to ``0.2``.

* telemetry: Improved the opt-in flow so that users can keep sharing the same
  data, even when new data collections are available. A new 'perf' channel that
  collects various performance metrics is now avaiable to opt into with:
  `ceph telemetry on`
  `ceph telemetry enable channel perf`
  See a sample report with `ceph telemetry preview`.
  Note that generating a telemetry report with 'perf' channel data might
  take a few moments in big clusters.
  For more details, see:
  https://docs.ceph.com/en/quincy/mgr/telemetry/

* MGR: The progress module disables the pg recovery event by default since the
  event is expensive and has interrupted other services when there are OSDs
  being marked in/out from the the cluster. However, the user can still enable
  this event anytime. For more detail, see:

  https://docs.ceph.com/en/quincy/mgr/progress/

* https://tracker.ceph.com/issues/55383 is a known issue -
  ``mon_cluster_log_to_journald`` needs to be set to false, when
  ``mon_cluster_log_to_file`` is set to true to continue to log cluster
  log messages to file, after log rotation.

Cephadm
-------

* SNMP Support
* Colocation of Daemons (mgr, mds, rgw)
* osd memory autotuning
* Integration with new NFS mgr module
* Ability to zap osds as they are removed
* cephadm agent for increased performance/scalability

Dashboard
~~~~~~~~~
* Day 1: the new "Cluster Expansion Wizard" will guide users through post-install steps:
  adding new hosts, storage devices or services.
* NFS: the Dashboard now allows users to fully manage all NFS exports from a single place.
* New mgr module (feedback): users can quickly report Ceph tracker issues
  or suggestions directly from the Dashboard or the CLI.
* New "Message of the Day": cluster admins can publish a custom message in a banner.
* Cephadm integration improvements:
   * Host management: maintenance, specs and labelling,
   * Service management: edit and display logs,
   * Daemon management (start, stop, restart, reload),
   * New services supported: ingress (HAProxy) and SNMP-gateway.
* Monitoring and alerting:
   * 43 new alerts have been added (totalling 68) improving observability of events affecting:
     cluster health, monitors, storage devices, PGs and CephFS.
   * Alerts can now be sent externally as SNMP traps via the new SNMP gateway service
     (the MIB is provided).
   * Improved integrated full/nearfull event notifications.
   * Grafana Dashboards now use grafonnet format (though they're still available
     in JSON format).
   * Stack update: images for monitoring containers have been updated.
     Grafana 8.3.5, Prometheus 2.33.4, Alertmanager 0.23.0 and Node Exporter 1.3.1.
     This reduced exposure to several Grafana vulnerabilities (CVE-2021-43798,
     CVE-2021-39226, CVE-2021-43798,  CVE-2020-29510, CVE-2020-29511).

RADOS
~~~~~

* OSD: Ceph now uses `mclock_scheduler` for BlueStore OSDs as its default
  `osd_op_queue` to provide QoS. The 'mclock_scheduler' is not supported
  for Filestore OSDs. Therefore, the default 'osd_op_queue' is set to `wpq`
  for Filestore OSDs and is enforced even if the user attempts to change it.
  For more details on configuring mclock see,

  https://docs.ceph.com/en/quincy/rados/configuration/mclock-config-ref/

  An outstanding issue exists during runtime where the mclock config options
  related to reservation, weight and limit cannot be modified after switching
  to the `custom` mclock profile using the `ceph config set ...` command.
  This is tracked by: https://tracker.ceph.com/issues/55153. Until the issue
  is fixed, users are advised to avoid using the 'custom' profile or use the
  workaround mentioned in the tracker.

* MGR: The pg_autoscaler can now be turned `on` and `off` globally
  with the `noautoscale` flag. By default, it is set to `on`, but this flag
  can come in handy to prevent rebalancing triggered by autoscaling during
  cluster upgrade and maintenance. Pools can now be created with the `--bulk`
  flag, which allows the autoscaler to allocate more PGs to such pools. This
  can be useful to get better out of the box performance for data-heavy pools.

  For more details about autoscaling, see:
  https://docs.ceph.com/en/quincy/rados/operations/placement-groups/

* OSD: Support for on-wire compression for osd-osd communication, `off` by
  default.

  For more details about compression modes, see:
  https://docs.ceph.com/en/quincy/rados/configuration/msgr2/#compression-modes

* OSD: Concise reporting of slow operations in the cluster log. The old
  and more verbose logging behavior can be regained by setting
  `osd_aggregated_slow_ops_logging` to false.

* the "kvs" Ceph object class is not packaged anymore. The "kvs" Ceph
  object class offers a distributed flat b-tree key-value store that
  is implemented on top of the librados objects omap. Because there
  are no existing internal users of this object class, it is not
  packaged anymore.

RBD block storage
~~~~~~~~~~~~~~~~~

* rbd-nbd: `rbd device attach` and `rbd device detach` commands added,
  these allow for safe reattach after `rbd-nbd` daemon is restarted since
  Linux kernel 5.14.

* rbd-nbd: `notrim` map option added to support thick-provisioned images,
  similar to krbd.

* Large stabilization effort for client-side persistent caching on SSD
  devices, also available in 16.2.8. For details on usage, see:

  https://docs.ceph.com/en/quincy/rbd/rbd-persistent-write-log-cache/

* Several bug fixes in diff calculation when using fast-diff image
  feature + whole object (inexact) mode. In some rare cases these
  long-standing issues could cause an incorrect `rbd export`. Also
  fixed in 15.2.16 and 16.2.8.

* Fix for a potential performance degradation when running Windows VMs
  on krbd. For details, see `rxbounce` map option description:

  https://docs.ceph.com/en/quincy/man/8/rbd/#kernel-rbd-krbd-options

RGW object storage
~~~~~~~~~~~~~~~~~~

* RGW now supports rate limiting by user and/or by bucket. With this
  feature it is possible to limit user and/or bucket, the total operations
  and/or bytes per minute can be delivered. This feature allows the
  admin to limit only READ operations and/or WRITE operations. The
  rate-limiting configuration could be applied on all users and all buckets
  by using global configuration.

* `radosgw-admin realm delete` has been renamed to `radosgw-admin realm
  rm`. This is consistent with the help message.

* S3 bucket notification events now contain an `eTag` key instead of
  `etag`, and eventName values no longer carry the `s3:` prefix, fixing
  deviations from the message format that is observed on AWS.

* It is possible to specify ssl options and ciphers for beast frontend
  now. The default ssl options setting is
  "no_sslv2:no_sslv3:no_tlsv1:no_tlsv1_1". If you want to return to the old
  behavior, add 'ssl_options=' (empty) to the ``rgw frontends`` configuration.

* The behavior for Multipart Upload was modified so that only
  CompleteMultipartUpload notification is sent at the end of the multipart
  upload. The POST notification at the beginning of the upload and the PUT
  notifications that were sent on each part are no longer sent.


CephFS distributed file system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* fs: A file system can be created with a specific ID ("fscid"). This is
  useful in certain recovery scenarios (for example, when a monitor
  database has been lost and rebuilt, and the restored file system is
  expected to have the same ID as before).

* fs: A file system can be renamed using the `fs rename` command. Any cephx
  credentials authorized for the old file system name will need to be
  reauthorized to the new file system name. Since the operations of the clients
  using these re-authorized IDs may be disrupted, this command requires the
  "--yes-i-really-mean-it" flag. Also, mirroring is expected to be disabled
  on the file system.

* MDS upgrades no longer require all standby MDS daemons to be stoped before
  upgrading a file systems's sole active MDS.

* CephFS: Failure to replay the journal by a standby-replay daemon now
  causes the rank to be marked "damaged".

Upgrading from Octopus or Pacific
----------------------------------

Quincy does not support LevelDB. Please migrate your OSDs and monitors
to RocksDB before upgrading to Quincy.

Before starting, make sure your cluster is stable and healthy (no down or
recovering OSDs).  (This is optional, but recommended.) You can disable
the autoscaler for all pools during the upgrade using the noautoscale flag.

.. note::

  You can monitor the progress of your upgrade at each stage with the
  ``ceph versions`` command, which will tell you what ceph version(s) are
  running for each type of daemon.

Upgrading cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~

If your cluster is deployed with cephadm (first introduced in Octopus), then
the upgrade process is entirely automated.  To initiate the upgrade,

  .. prompt:: bash #

    ceph orch upgrade start --ceph-version 17.2.0

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
downgrade back to Octopus or Pacific.


Upgrading non-cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
   If you cluster is running Octopus (15.2.x) or later, you might choose
   to first convert it to use cephadm so that the upgrade to Quincy
   is automated (see above).  For more information, see
   :ref:`cephadm-adoption`.

#. Set the ``noout`` flag for the duration of the upgrade. (Optional,
   but recommended.)::

     # ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example, on each monitor host,::

     # systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``quincy`` string in the mon
   map.  The command::

     # ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 17 (quincy)

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

#. Upgrade all CephFS MDS daemons. For each CephFS file system,

   #. Disable standby_replay::

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

#. Complete the upgrade by disallowing pre-Quincy OSDs and enabling
   all new Quincy-only functionality::

     # ceph osd require-osd-release quincy

#. If you set ``noout`` at the beginning, be sure to clear it with::

     # ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment
   and orchestration framework to simplify cluster management and
   future upgrades.  For more information on converting an existing
   cluster to cephadm, see :ref:`cephadm-adoption`.

Post-upgrade
~~~~~~~~~~~~

#. Verify the cluster is healthy with ``ceph health``. If your cluster is
   running Filestore, a deprecation warning is expected. This warning can
   be temporarily muted using the following command::

     ceph health mute OSD_FILESTORE

#. If you are upgrading from Mimic, or did not already do so when you
   upgraded to Nautilus, we recommend you enable the new :ref:`v2
   network protocol <msgr2>`, issue the following command::

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

     ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with::

     ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at
   `https://telemetry-public.ceph.com/ <https://telemetry-public.ceph.com/>`_.

   For more information about the telemetry module, see :ref:`the
   documentation <telemetry>`.


Upgrading from pre-Octopus releases (like Nautilus)
---------------------------------------------------


You *must* first upgrade to Octopus (15.2.z) or Pacific (16.2.z) before
upgrading to Quincy.
