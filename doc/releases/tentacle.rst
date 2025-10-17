========
Tentacle
========

Tentacle is the 20th stable release of Ceph.

v20.2.0 Tentacle
================


Highlights
----------

*See the sections below for more details on these items.*

RADOS

* FastEC: Long-anticipated performance and space amplification
  optimizations are added for erasure-coded pools.
* BlueStore: Improved compression and a new, faster WAL (write-ahead-log).
* Data Availability Score: Users can now track a data availability score
  for each pool in their cluster.
* OMAP: All components have been switched to the faster OMAP iteration
  interface, which improves RGW bucket listing and scrub operations.

Dashboard

* Support has been added for NVMe/TCP gateway groups and multiple
  namespaces, multi-cluster management, OAuth 2.0 integration, and enhanced
  RGW/SMB features including multi-site automation, tiering, policies,
  lifecycles, notifications, and granular replication.

RBD

* New live migration features: RBD images can now be instantly imported
  from another Ceph cluster (native format) or from a wide variety of
  external sources/formats.
* There is now support for RBD namespace remapping while mirroring between
  Ceph clusters.
* Several commands related to group and snap info were added or improved,
  and ``rbd device map`` now defaults to ``msgr2``.

MGR

* Users now have the ability to force-disable always-on modules.
* The ``restful`` and ``zabbix`` modules (deprecated since 2020) have been
  officially removed.

RGW

* Added support for S3 ``GetObjectAttributes``.
* For compatibility with AWS S3, ``LastModified`` timestamps are now truncated
  to the second. Note that during upgrade, users may observe these timestamps
  moving backwards as a result.
* Bucket resharding now does most of its processing before it starts to block
  write operations. This should significantly reduce the client-visible impact
  of resharding on large buckets.

CephFS

* Directories may now be configured with case-insensitive or normalized
  directory entry names.
* Modifying the FS setting variable ``max_mds`` when a cluster is unhealthy
  now requires users to pass the confirmation flag (``--yes-i-really-mean-it``).
* ``EOPNOTSUPP`` (Operation not supported) is now returned by the CephFS FUSE
  client for ``fallocate`` for the default case (i.e. ``mode == 0``).

Ceph
----

* Integrated SMB support: Ceph clusters now offer an SMB Manager module
  that works like the existing NFS subsystem. The new SMB support
  allows the Ceph cluster to automatically create Samba-backed SMB file
  shares connected to CephFS. The ``smb`` module can configure both basic
  Active Directory domain or standalone user authentication. The Ceph
  cluster can host one or more virtual SMB clusters which can be truly
  clustered using Samba's CTDB technology. The ``smb`` module requires a
  cephadm-enabled Ceph cluster and deploys container images provided by
  the ``samba-container`` project. The Ceph dashboard can be used to configure
  SMB clusters and shares. A new ``cephfs-proxy`` daemon is automatically
  deployed to improve scalability and memory usage when connecting
  Samba to CephFS.

CephFS
------

* Directories may now be configured with case-insensitive or
  normalized directory entry names. This is an inheritable configuration,
  making it apply to an entire directory tree.

  For more information, see the :ref:`charmap`.

* It is now possible to pause the threads that asynchronously purge
  deleted subvolumes by using the config option
  ``mgr/volumes/pause_purging``.

* It is now possible to pause the threads that asynchronously clone
  subvolume snapshots by using the config option
  ``mgr/volumes/pause_cloning``.

* Modifying the setting ``max_mds`` when a cluster is
  unhealthy now requires users to pass the confirmation flag
  (``--yes-i-really-mean-it``). This has been added as a precaution to inform
  users that modifying ``max_mds`` may not help with troubleshooting or recovery
  efforts. Instead, it might further destabilize the cluster.

* ``EOPNOTSUPP`` (Operation not supported) is now returned by the CephFS
  FUSE client for ``fallocate`` in the default case (i.e., ``mode == 0``) since
  CephFS does not support disk space reservation. The only flags supported are
  ``FALLOC_FL_KEEP_SIZE`` and ``FALLOC_FL_PUNCH_HOLE``.

* The ``ceph fs subvolume snapshot getpath`` command now allows users
  to get the path of a snapshot of a subvolume. If the snapshot is not present,
  ``ENOENT`` is returned.

* The ``ceph fs volume create`` command now allows users to pass
  metadata and data pool names to be used for creating the volume. If either
  is not passed, or if either is a non-empty pool, the command will abort.

* The format of the pool namespace name for CephFS volumes has been changed
  from ``fsvolumens__<subvol-name>`` to
  ``fsvolumens__<subvol-grp-name>_<subvol-name>`` to avoid namespace collisions
  when two subvolumes located in different subvolume groups have the same name.
  Even with namespace collisions, there were no security issues, since the MDS
  auth cap is restricted to the subvolume path. Now, with this change, the
  namespaces are completely isolated.

* If the subvolume name passed to the command ``ceph fs subvolume info``
  is a clone, the output will now also contain a "source" field that tells the
  user the name of the source snapshot along with the name of the volume,
  subvolume group, and subvolume in which the source snapshot is located.
  For clones created with Tentacle or an earlier release, the value of this
  field will be ``N/A``. Regular subvolumes do not have a source subvolume and
  therefore the output for them will not contain a "source" field regardless of
  the release.

Dashboard
---------

* There is now added support for NVMe/TCP gateway groups and multiple
  namespaces, multi-cluster management, OAuth 2.0 integration, and enhanced
  RGW/SMB features including multi-site automation, tiering, policies,
  lifecycles, notifications, and granular replication.

MGR
---

* The Ceph Manager's always-on modulues/plugins can now be force-disabled.
  This can be necessary in cases where we wish to prevent the manager from being
  flooded by module commands when Ceph services are down or degraded.

* ``mgr/restful``, ``mgr/zabbix``: both modules, already deprecated since 2020, have been
  finally removed. They have not been actively maintained in the last years,
  and started suffering from vulnerabilities in their dependency chain (e.g.:
  CVE-2023-46136). An alternative for the ``restful`` module is the ``dashboard`` module,
  which provides a richer and better maintained RESTful API. Regarding the ``zabbix`` module,
  there are alternative monitoring solutions, like ``prometheus``, which is the most
  widely adopted among the Ceph user community.

RADOS
-----

* Long-anticipated performance and space amplification optimizations (FastEC)
  are added for erasure-coded pools, including partial reads and partial writes.

* A new implementation of the Erasure Coding I/O code provides substantial
  performance improvements and some capacity improvements. The new code is
  designed to optimize performance when using Erasure Coding with block storage
  (RBD) and file storage (CephFS) but will have benefits for object storage
  (RGW), in particular when using smaller sized objects. A new flag
  ``allow_ec_optimizations`` must be set on each pool to switch to using the
  new code. Existing pools can be upgraded once the OSD and Monitor daemons
  have been updated. There is no need to update the clients.

* The default plugin for erasure coded pools has been changed from Jerasure to
  ISA-L. Clusters created on Tentacle or later releases will use ISA-L as the
  default plugin when creating a new pool. Clusters that upgrade to the T release
  will continue to use their existing default values. The default values can be
  overridden by creating a new erasure code profile and selecting it when
  creating a new pool. ISA-L is recommended for new pools because the Jerasure
  library is no longer maintained.

* BlueStore now has better compression and a new, faster WAL (write-ahead-log).

* All components have been switched to the faster OMAP iteration interface, which
  improves RGW bucket listing and scrub operations.

* It is now possible to bypass ``ceph_assert()`` in extreme cases to help with
  disaster recovery.

* Testing improvements for dencoding verification were added.

* A new command, ``ceph osd pool availability-status``, has been added that
  allows users to view the availability score for each pool in a cluster. A pool
  is considered unavailable if any PG in the pool is not ``active`` or if
  there are unfound objects. Otherwise the pool is considered available. The
  score is updated every one second by default. This interval can be changed
  using the new config option ``pool_availability_update_interval``. The feature
  is off by default. A new config option ``enable_availability_tracking`` can be
  used to turn on the feature if required. Another command is added to clear the
  availability status for a specific pool:

  ::

    ceph osd pool clear-availability-status <pool-name>

  This feature is in tech preview.

  Related links:

  - Feature ticket: https://tracker.ceph.com/issues/67777
  - Documentation: :ref:`data_availability_score`

* Leader monitor and stretch mode status are now included in the ``ceph status``
  output.

  Related tracker: https://tracker.ceph.com/issues/70406

* The ``ceph df`` command reports incorrect ``MAX AVAIL`` for stretch mode pools
  when CRUSH rules use multiple take steps for datacenters. ``PGMap::get_rule_avail``
  incorrectly calculates available space from only one datacenter. As a workaround,
  define CRUSH rules with ``take default`` and ``choose firstn 0 type datacenter``.
  See https://tracker.ceph.com/issues/56650#note-6 for details.

  Upgrading a cluster configured with a CRUSH rule with multiple take steps can
  lead to data shuffling, as the new CRUSH changes may necessitate data
  redistribution. In contrast, a stretch rule with a single-take configuration
  will not cause any data movement during the upgrade process.

* Added convenience function ``librados::AioCompletion::cancel()`` with the same
  behavior as ``librados::IoCtx::aio_cancel()``.

* A new command, ``ceph osd rm-pg-upmap-primary-all``, has been added that allows
  users to clear all ``pg-upmap-primary`` mappings in the osdmap when desired.

  Related trackers:

  - https://tracker.ceph.com/issues/67179
  - https://tracker.ceph.com/issues/66867

* The configuration parameter ``osd_repair_during_recovery`` has been removed.
  That configuration flag used to control whether an operator-initiated "repair
  scrub" would be allowed to start on an OSD that is performing a recovery. In
  this Ceph version, operator-initiated scrubs and repair scrubs are never blocked
  by a repair being performed.

* Fixed issue of recovery/backfill hang due to improper handling of items in the
  dmclock's background clean-up thread.

  Related tracker: https://tracker.ceph.com/issues/61594

* The OSD's IOPS capacity used by the mClock scheduler is now also checked to
  determine if it's below a configured threshold value defined by:

  - ``osd_mclock_iops_capacity_low_threshold_hdd`` – set to 50 IOPS
  - ``osd_mclock_iops_capacity_low_threshold_ssd`` – set to 1000 IOPS

  The check is intended to handle cases where the measured IOPS is unrealistically
  low. If such a case is detected, the IOPS capacity is either set to the last
  valid value or the configured default to avoid affecting cluster performance
  (slow or stalled ops).

* Documentation has been updated with steps to override OSD IOPS capacity
  configuration.

  Related links:

  - Tracker ticket: https://tracker.ceph.com/issues/70774
  - Documentation: :ref:`override_max_iops_capacity`

* pybind/rados: Fixes ``WriteOp.zero()`` in the original reversed order of arguments
  ``offset`` and ``length``. When pybind calls ``WriteOp.zero()``, the argument passed
  does not match ``rados_write_op_zero``, and offset and length are swapped, which
  results in an unexpected response.

RBD
---

* All Python APIs that produce timestamps now return "aware" ``datetime``
  objects instead of "naive" ones (i.e., those including time zone information
  instead of those not including it). All timestamps remain in UTC, but
  including ``timezone.utc`` makes it explicit and avoids the potential of the
  returned timestamp getting misinterpreted. In Python 3, many ``datetime``
  methods treat "naive" ``datetime`` objects as local times.

* ``rbd group info`` and ``rbd group snap info`` commands are introduced to
  show information about a group and a group snapshot respectively.

* ``rbd group snap ls`` output now includes the group snapshot IDs. The header
  of the column showing the state of a group snapshot in the unformatted CLI
  output is changed from ``STATUS`` to ``STATE``. The state of a group snapshot
  that was shown as ``ok`` is now shown as ``complete``, which is more
  descriptive.

* Moving an image that is a member of a group to trash is no longer
  allowed. The ``rbd trash mv`` command now behaves the same way as ``rbd rm``
  in this scenario.

* Fetching the mirroring mode of an image is invalid if the image is
  disabled for mirroring. The public APIs -- C++ ``mirror_image_get_mode()``,
  C ``rbd_mirror_image_get_mode()``, and Python ``Image.mirror_image_get_mode()``
  -- will return ``EINVAL`` when mirroring is disabled.

* Promoting an image is invalid if the image is not enabled for mirroring.
  The public APIs -- C++ ``mirror_image_promote()``,
  C ``rbd_mirror_image_promote()``, and Python ``Image.mirror_image_promote()``
  -- will return EINVAL instead of ENOENT when mirroring is not enabled.

* Requesting a resync on an image is invalid if the image is not enabled
  for mirroring. The public APIs -- C++ ``mirror_image_resync()``,
  C ``rbd_mirror_image_resync()``, and Python ``Image.mirror_image_resync()``
  -- will return EINVAL instead of ENOENT when mirroring is not enabled.

RGW
---

* Multiple fixes: Lua scripts will no longer run uselessly against health checks,
  properly quoted ``ETag`` values returned by S3 ``CopyPart``, ``PostObject``, and
  ``CompleteMultipartUpload`` responses.

* IAM policy evaluation now supports conditions ``ArnEquals`` and ``ArnLike``,
  along with their ``Not`` and ``IfExists`` variants.

* Added BEAST frontend option ``so_reuseport`` which facilitates running multiple
  RGW instances on the same host by sharing a single TCP port.

* Replication policies now validate permissions using
  ``s3:ReplicateObject``, ``s3:ReplicateDelete``, and ``s3:ReplicateTags`` for
  destination buckets. For source buckets, both
  ``s3:GetObjectVersionForReplication`` and ``s3:GetObject(Version)`` are
  supported. Actions like ``s3:GetObjectAcl``, ``s3:GetObjectLegalHold``, and
  ``s3:GetObjectRetention`` are also considered when fetching the source object.
  Replication of tags is controlled by the
  ``s3:GetObject(Version)Tagging`` permission.

* Adding missing quotes to the ``ETag`` values returned by S3 ``CopyPart``,
  ``PostObject``, and ``CompleteMultipartUpload`` responses.

* ``PutObjectLockConfiguration`` can now be used to enable S3 Object Lock on an
  existing versioning-enabled bucket that was not created with Object Lock enabled.

* The ``x-amz-confirm-remove-self-bucket-access`` header is now supported by
  ``PutBucketPolicy``. Additionally, the root user will always have access to
  modify the bucket policy, even if the current policy explicitly denies access.

* Added support for the ``RestrictPublicBuckets`` property of the S3
  ``PublicAccessBlock`` configuration.

* The HeadBucket API now reports the ``X-RGW-Bytes-Used`` and ``X-RGW-Object-Count``
  headers only when the ``read-stats`` querystring is explicitly included in the
  API request.

Telemetry
---------

* The ``basic`` channel in telemetry now captures the ``ec_optimizations``
  flag, which will allow us to gauge feature adoption for the new
  FastEC improvements.
  To opt into telemetry, run ``ceph telemetry on``.

Crimson / Seastore
------------------

* Check out the latest news on Crimson here: https://ceph.io/en/news/crimson/

Upgrading from Reef or Squid
----------------------------

Before starting, ensure that your cluster is stable and healthy with no
``down``, ``recovering``, ``incomplete``, ``undersized`` or ``backfilling`` PGs.
You can temporarily disable the PG autoscaler for all pools during the upgrade
by running ``ceph osd pool set noautoscale`` before beginning, and if the
autoscaler is desired after completion, running ``ceph osd pool unset
noautoscale`` after upgrade success is confirmed.

.. note::

   You can monitor the progress of your upgrade at each stage with the ``ceph versions`` command, which will tell you what Ceph version(s) are running for each type of daemon.

Upgrading Cephadm Clusters
--------------------------

If your cluster is deployed with cephadm (first introduced in Octopus), then the upgrade process is entirely automated. To initiate the upgrade,

.. prompt:: bash #

    ceph orch upgrade start --image quay.io/ceph/ceph:v20.2.0

The same process is used to upgrade to future minor releases.

Upgrade progress can be monitored with

.. prompt:: bash #

    ceph orch upgrade status

Upgrade progress can also be monitored with ``ceph -s`` (which provides a simple progress bar) or more verbosely with

.. prompt:: bash #

    ceph -W cephadm

The upgrade can be paused or resumed with

.. prompt:: bash #

    ceph orch upgrade pause  # to pause
    ceph orch upgrade resume # to resume

or canceled with

.. prompt:: bash #

    ceph orch upgrade stop

Note that canceling the upgrade simply stops the process. There is no ability to downgrade back to Reef or Squid.

Upgrading Non-cephadm Clusters
------------------------------

.. note::

   1. If your cluster is running Reef (18.2.x) or later, you might choose
      to first convert it to use cephadm so that the upgrade to Tentacle is automated (see above).
      For more information, see https://docs.ceph.com/en/tentacle/cephadm/adoption/.

   2. If your cluster is running Reef (18.2.x) or later, systemd unit file
      names have changed to include the cluster fsid. To find the correct
      systemd unit file name for your cluster, run following command:

      ::

        systemctl -l | grep <daemon type>

      Example:

      .. prompt:: bash $

        systemctl -l | grep mon | grep active

      ::

        ceph-6ce0347c-314a-11ee-9b52-000af7995d6c@mon.f28-h21-000-r630.service                                           loaded active running   Ceph mon.f28-h21-000-r630 for 6ce0347c-314a-11ee-9b52-000af7995d6c

#. Set the ``noout`` flag for the duration of the upgrade. (Optional, but recommended.)

   .. prompt:: bash #

      ceph osd set noout

#. Upgrade Monitors by installing the new packages and restarting the Monitor daemons. For example, on each Monitor host

   .. prompt:: bash #

      systemctl restart ceph-mon.target

   Once all Monitors are up, verify that the Monitor upgrade is complete by looking for the ``tentacle`` string in the mon map. The command

   .. prompt:: bash #

      ceph mon dump | grep min_mon_release

   should report:

   .. prompt:: bash #

      min_mon_release 20 (tentacle)

   If it does not, that implies that one or more Monitors haven't been upgraded and restarted and/or the quorum does not include all Monitors.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and restarting all Manager daemons. For example, on each Manager host,

   .. prompt:: bash #

      systemctl restart ceph-mgr.target

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph -s``:

   .. prompt:: bash #

      ceph -s

   ::

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the ``ceph-osd`` daemons on all OSD hosts

   .. prompt:: bash #

      systemctl restart ceph-osd.target

#. Upgrade all CephFS MDS daemons. For each CephFS file system,

   #. Disable standby_replay:

      .. prompt:: bash #

         ceph fs set <fs_name> allow_standby_replay false

   #. Reduce the number of ranks to 1. (Make note of the original number of MDS daemons first if you plan to restore it later.)

      .. prompt:: bash #

         ceph status # ceph fs set <fs_name> max_mds 1

   #. Wait for the cluster to deactivate any non-zero ranks by periodically checking the status

      .. prompt:: bash #

         ceph status

   #. Take all standby MDS daemons offline on the appropriate hosts with

      .. prompt:: bash #

         systemctl stop ceph-mds@<daemon_name>

   #. Confirm that only one MDS is online and is rank 0 for your FS

      .. prompt:: bash #

         ceph status

   #. Upgrade the last remaining MDS daemon by installing the new packages and restarting the daemon

      .. prompt:: bash #

         systemctl restart ceph-mds.target

   #. Restart all standby MDS daemons that were taken offline

      .. prompt:: bash #

         systemctl start ceph-mds.target

   #. Restore the original value of ``max_mds`` for the volume

      .. prompt:: bash #

         ceph fs set <fs_name> max_mds <original_max_mds>

#. Upgrade all ``radosgw`` daemons by upgrading packages and restarting daemons on all hosts

   .. prompt:: bash #

      systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Tentacle OSDs and enabling all new Tentacle-only functionality

   .. prompt:: bash #

      ceph osd require-osd-release tentacle

#. If you set ``noout`` at the beginning, be sure to clear it with

   .. prompt:: bash #

      ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment and orchestration framework to simplify
   cluster management and future upgrades. For more information on converting an existing cluster to cephadm,
   see :ref:`cephadm-adoption`.

Post-upgrade
------------

#. Verify the cluster is healthy with ``ceph health``.

#. Consider enabling the :ref:`telemetry` to send anonymized usage statistics
   and crash information to Ceph upstream developers. To see what would
   be reported without actually sending any information to anyone,

   .. prompt:: bash #

      ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to automatically report high-level cluster metadata with

   .. prompt:: bash #

      ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at https://telemetry-public.ceph.com/.

Upgrading from Pre-Reef Releases (like Quincy)
----------------------------------------------

You **must** first upgrade to Reef (18.2.z) or Squid (19.2.z) before upgrading to Tentacle.
