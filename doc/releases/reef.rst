====
Reef
====

Reef is the 18th stable release of Ceph. It is named after the reef squid
(Sepioteuthis).

v18.2.0 Reef
============

This is the first stable release of Ceph Reef.

.. important::

   We are unable to build Ceph on Debian stable (bookworm) for the 18.2.0
   release because of Debian bug
   https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1030129. We will build as
   soon as this bug is resolved in Debian stable.

   *last updated 2023 Aug 04*

Major Changes from Quincy
--------------------------

Highlights
~~~~~~~~~~

See the relevant sections below for more details on these changes.

* **RADOS:** RocksDB has been upgraded to version 7.9.2.
* **RADOS:** There have been significant improvements to RocksDB iteration overhead and performance.
* **RADOS:** The ``perf dump`` and ``perf schema`` commands have been deprecated in
  favor of the new ``counter dump`` and ``counter schema`` commands.
* **RADOS:** Cache tiering is now deprecated.
* **RADOS:** A new feature, the "read balancer", is now available, which allows users to balance primary PGs per pool on their clusters.
* **RGW:** Bucket resharding is now supported for multi-site configurations.
* **RGW:** There have been significant improvements to the stability and consistency of multi-site replication.
* **RGW:** Compression is now supported for objects uploaded with Server-Side Encryption.
* **Dashboard:** There is a new Dashboard page with improved layout. Active alerts and some important charts are now displayed inside cards.
* **RBD:** Support for layered client-side encryption has been added.
* **Telemetry**: Users can now opt in to participate in a leaderboard in the telemetry public dashboards.

CephFS
~~~~~~

* CephFS: The ``mds_max_retries_on_remount_failure`` option has been renamed to
  ``client_max_retries_on_remount_failure`` and moved from ``mds.yaml.in`` to
  ``mds-client.yaml.in``. This change was made because the option has always
  been used only by the MDS client.
* CephFS: It is now possible to delete the recovered files in the
  ``lost+found`` directory after a CephFS post has been recovered in accordance
  with disaster recovery procedures.
* The ``AT_NO_ATTR_SYNC`` macro has been deprecated in favor of the standard
  ``AT_STATX_DONT_SYNC`` macro. The ``AT_NO_ATTR_SYNC`` macro will be removed
  in the future.

Dashboard
~~~~~~~~~

* There is a new Dashboard page with improved layout. Active alerts
  and some important charts are now displayed inside cards.

* Cephx Auth Management: There is a new section dedicated to listing and
  managing Ceph cluster users.
  
* RGW Server Side Encryption: The SSE-S3 and KMS encryption of rgw buckets can
  now be configured at the time of bucket creation.

* RBD Snapshot mirroring: Snapshot mirroring can now be configured through UI.
  Snapshots can now be scheduled.
  
* 1-Click OSD Creation Wizard: OSD creation has been broken into 3 options:

  #. Cost/Capacity Optimized: Use all HDDs

  #. Throughput Optimized: Combine HDDs and SSDs

  #. IOPS Optimized: Use all NVMes

  The current OSD-creation form has been moved to the Advanced section.

* Centralized Logging: There is now a view that collects all the logs from
  the Ceph cluster.

* Accessibility WCAG-AA: Dashboard is WCAG 2.1 level A compliant and therefore
  improved for blind and visually impaired Ceph users.

* Monitoring & Alerting

      * Ceph-exporter: Now the performance metrics for Ceph daemons are
        exported by ceph-exporter, which deploys on each daemon rather than
        using prometheus exporter. This will reduce performance bottlenecks.

      * Monitoring stacks updated:

            * Prometheus 2.43.0

            * Node-exporter 1.5.0

            * Grafana 9.4.7

            * Alertmanager 0.25.0

MGR
~~~

* mgr/snap_schedule: The snap-schedule manager module now retains one snapshot
  less than the number mentioned against the config option
  ``mds_max_snaps_per_dir``. This means that a new snapshot can be created and
  retained during the next schedule run.
* The ``ceph mgr dump`` command now outputs ``last_failure_osd_epoch`` and
  ``active_clients`` fields at the top level. Previously, these fields were
  output under the ``always_on_modules`` field.
  
RADOS
~~~~~

* RocksDB has been upgraded to version 7.9.2, which incorporates several
  performance improvements and features. This is the first release that can
  tune RocksDB settings per column family, which allows for more granular
  tunings to be applied to different kinds of data stored in RocksDB. New
  default settings have been used to optimize performance for most workloads, with a
  slight penalty in some use cases. This slight penalty is outweighed by large
  improvements in compactions and write amplification in use cases such as RGW
  (up to a measured 13.59% improvement in 4K random write IOPs).
* Trimming of PGLog dups is now controlled by the size rather than the version.
  This change fixes the PGLog inflation issue that was happening when the
  online (in OSD) trimming got jammed after a PG split operation. Also, a new
  offline mechanism has been added: ``ceph-objectstore-tool`` has a new
  operation called ``trim-pg-log-dups`` that targets situations in which an OSD
  is unable to boot because of the inflated dups. In such situations, the "You
  can be hit by THE DUPS BUG" warning is visible in OSD logs. Relevant tracker:
  https://tracker.ceph.com/issues/53729
* The RADOS Python bindings are now able to process (opt-in) omap keys as bytes
  objects. This allows interacting with RADOS omap keys that are not
  decodable as UTF-8 strings.
* mClock Scheduler: The mClock scheduler (the default scheduler in Quincy) has
  undergone significant usability and design improvements to address the slow
  backfill issue. The following is a list of some important changes:

  * The ``balanced`` profile is set as the default mClock profile because it
    represents a compromise between prioritizing client I/O and prioritizing
    recovery I/O. Users can then choose either the ``high_client_ops`` profile
    to prioritize client I/O or the ``high_recovery_ops`` profile to prioritize
    recovery I/O.
  * QoS parameters including ``reservation`` and ``limit`` are now specified in 
    terms of a fraction (range: 0.0 to 1.0) of the OSD's IOPS capacity.
  * The cost parameters (``osd_mclock_cost_per_io_usec_*`` and
    ``osd_mclock_cost_per_byte_usec_*``) have been removed. The cost of an 
    operation is now a function of the random IOPS and maximum sequential
    bandwidth capability of the OSD's underlying device.
  * Degraded object recovery is given higher priority than misplaced
    object recovery because degraded objects present a data safety issue that
    is not present with objects that are merely misplaced. As a result,
    backfilling operations with the ``balanced`` and ``high_client_ops`` mClock
    profiles might progress more slowly than in the past, when backfilling
    operations used the 'WeightedPriorityQueue' (WPQ) scheduler.
  * The QoS allocations in all the mClock profiles are optimized in 
    accordance with the above fixes and enhancements.
  * For more details, see:
    https://docs.ceph.com/en/reef/rados/configuration/mclock-config-ref/
* A new feature, the "read balancer", is now available, which allows
  users to balance primary PGs per pool on their clusters. The read balancer is
  currently available as an offline option via the ``osdmaptool``. By providing
  a copy of their osdmap and a pool they want balanced to the ``osdmaptool``, users
  can generate a preview of optimal primary PG mappings that they can then choose to
  apply to their cluster. For more details, see
  https://docs.ceph.com/en/latest/dev/balancer-design/#read-balancing
* The ``active_clients`` array displayed by the ``ceph mgr dump`` command now
  has a ``name`` field that shows the name of the manager module that
  registered a RADOS client. Previously, the ``active_clients`` array showed
  the address of a module's RADOS client, but not the name of the module.
* The ``perf dump`` and ``perf schema`` commands have been deprecated in 
  favor of the new ``counter dump`` and ``counter schema`` commands. These new
  commands add support for labeled perf counters and also emit existing
  unlabeled perf counters. Some unlabeled perf counters became labeled in this
  release, and more will be labeled in future releases; such converted perf
  counters are no longer emitted by the ``perf dump`` and ``perf schema``
  commands.
* Cache tiering is now deprecated.
* The SPDK backend for BlueStore can now connect to an NVMeoF target. This
  is not an officially supported feature.

RBD
~~~

* The semantics of compare-and-write C++ API (`Image::compare_and_write` and
  `Image::aio_compare_and_write` methods) now match those of C API. Both
  compare and write steps operate only on len bytes even if the buffers
  associated with them are larger. The previous behavior of comparing up to the
  size of the compare buffer was prone to subtle breakage upon straddling a
  stripe unit boundary.
* The ``compare-and-write`` operation is no longer limited to 512-byte
  sectors. Assuming proper alignment, it now allows operating on stripe units
  (4MB by default).
* There is a new ``rbd_aio_compare_and_writev`` API method that supports
  scatter/gather on compare buffers as well as on write buffers. This
  complements the existing ``rbd_aio_readv`` and ``rbd_aio_writev`` methods.
* The ``rbd device unmap`` command now has a ``--namespace`` option.
  Support for namespaces was added to RBD in Nautilus 14.2.0, and since then it
  has been possible to map and unmap images in namespaces using the
  ``image-spec`` syntax. However, the corresponding option available in most
  other commands was missing.
* All rbd-mirror daemon perf counters have become labeled and are now
  emitted only by the new ``counter dump`` and ``counter schema`` commands. As
  part of the conversion, many were also renamed in order to better
  disambiguate journal-based and snapshot-based mirroring.
* The list-watchers C++ API (`Image::list_watchers`) now clears the passed
  `std::list` before appending to it. This aligns with the semantics of the C
  API (``rbd_watchers_list``).
* Trailing newline in passphrase files (for example: the
  ``<passphrase-file>`` argument of the ``rbd encryption format`` command and
  the ``--encryption-passphrase-file`` option of other commands) is no longer
  stripped.
* Support for layered client-side encryption has been added. It is now
  possible to encrypt cloned images with a distinct encryption format and
  passphrase, differing from that of the parent image and from that of every
  other cloned image. The efficient copy-on-write semantics intrinsic to
  unformatted (regular) cloned images have been retained.

RGW
~~~

* Bucket resharding is now supported for multi-site configurations. This
  feature is enabled by default for new deployments. Existing deployments must
  enable the ``resharding`` feature manually after all zones have upgraded.
  See https://docs.ceph.com/en/reef/radosgw/multisite/#zone-features for
  details.
* The RGW policy parser now rejects unknown principals by default. If you are
  mirroring policies between RGW and AWS, you might want to set
  ``rgw_policy_reject_invalid_principals`` to ``false``. This change affects
  only newly set policies, not policies that are already in place.
* RGW's default backend for ``rgw_enable_ops_log`` has changed from ``RADOS``
  to ``file``. The default value of ``rgw_ops_log_rados`` is now ``false``, and
  ``rgw_ops_log_file_path`` now defaults to
  ``/var/log/ceph/ops-log-$cluster-$name.log``.
* RGW's pubsub interface now returns boolean fields using ``bool``. Before this
  change, ``/topics/<topic-name>`` returned ``stored_secret`` and
  ``persistent`` using a string of ``"true"`` or ``"false"`` that contains
  enclosing quotation marks. After this change, these fields are returned
  without enclosing quotation marks so that the fields can be decoded as
  boolean values in JSON. The same is true of the ``is_truncated`` field
  returned by ``/subscriptions/<sub-name>``.
* RGW's response of ``Action=GetTopicAttributes&TopicArn=<topic-arn>`` REST 
  API now returns ``HasStoredSecret`` and ``Persistent`` as boolean in the JSON
  string that is encoded in ``Attributes/EndPoint``.
* All boolean fields that were previously rendered as strings by the
  ``rgw-admin`` command when the JSON format was used are now rendered as
  boolean. If your scripts and tools rely on this behavior, update them
  accordingly. The following is a list of the field names impacted by this
  change:

      * ``absolute``
      * ``add``
      * ``admin``
      * ``appendable``
      * ``bucket_key_enabled``
      * ``delete_marker``
      * ``exists``
      * ``has_bucket_info``
      * ``high_precision_time``
      * ``index``
      * ``is_master``
      * ``is_prefix``
      * ``is_truncated``
      * ``linked``
      * ``log_meta``
      * ``log_op``
      * ``pending_removal``
      * ``read_only``
      * ``retain_head_object``
      * ``rule_exist``
      * ``start_with_full_sync``
      * ``sync_from_all``
      * ``syncstopped``
      * ``system``
      * ``truncated``
      * ``user_stats_sync``
* The Beast front end's HTTP access log line now uses a new
  ``debug_rgw_access`` configurable. It has the same defaults as
  ``debug_rgw``, but it can be controlled independently.
* The pubsub functionality for storing bucket notifications inside Ceph
  has been removed. As a result, the pubsub zone should not be used anymore.
  The following have also been removed: the REST operations, ``radosgw-admin``
  commands for manipulating subscriptions, fetching the notifications, and
  acking the notifications. 

  If the endpoint to which the notifications are sent is down or disconnected,
  we recommend that you use persistent notifications to guarantee their
  delivery. If the system that consumes the notifications has to pull them
  (instead of the notifications being pushed to the system), use an external
  message bus (for example, RabbitMQ or Kafka) for that purpose. 
* The serialized format of notification and topics has changed. This means
  that new and updated topics will be unreadable by old RGWs. We recommend
  completing the RGW upgrades before creating or modifying any notification
  topics.
* Compression is now supported for objects uploaded with Server-Side
  Encryption. When both compression and encryption are enabled, compression is
  applied before encryption. Earlier releases of multisite do not replicate
  such objects correctly, so all zones must upgrade to Reef before enabling the
  `compress-encrypted` zonegroup feature: see
  https://docs.ceph.com/en/reef/radosgw/multisite/#zone-features and note the
  security considerations.
  
Telemetry
~~~~~~~~~

* Users who have opted in to telemetry can also opt in to
  participate in a leaderboard in the telemetry public dashboards
  (https://telemetry-public.ceph.com/). In addition, users are now able to
  provide a description of their cluster that will appear publicly in the
  leaderboard. For more details, see:
  https://docs.ceph.com/en/reef/mgr/telemetry/#leaderboard. To see a sample
  report, run ``ceph telemetry preview``. To opt in to telemetry, run ``ceph
  telemetry on``. To opt in to the leaderboard, run ``ceph config set mgr
  mgr/telemetry/leaderboard true``. To add a leaderboard description, run
  ``ceph config set mgr mgr/telemetry/leaderboard_description ‘Cluster
  description’`` (entering your own cluster description).

Upgrading from Pacific or Quincy
--------------------------------

Before starting, make sure your cluster is stable and healthy (no down or recovering OSDs). (This is optional, but recommended.) You can disable the autoscaler for all pools during the upgrade using the noautoscale flag.


.. note::

   You can monitor the progress of your upgrade at each stage with the ``ceph versions`` command, which will tell you what ceph version(s) are running for each type of daemon.

Upgrading cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~

If your cluster is deployed with cephadm (first introduced in Octopus), then the upgrade process is entirely automated. To initiate the upgrade,

  .. prompt:: bash #

    ceph orch upgrade start --image quay.io/ceph/ceph:v18.2.0

The same process is used to upgrade to future minor releases.

Upgrade progress can be monitored with

  .. prompt:: bash #
    
    ceph orch upgrade status

Upgrade progress can also be monitored with `ceph -s` (which provides a simple progress bar) or more verbosely with

  .. prompt:: bash #

    ceph -W cephadm

The upgrade can be paused or resumed with

  .. prompt:: bash #

    ceph orch upgrade pause  # to pause
    ceph orch upgrade resume # to resume

or canceled with

.. prompt:: bash #

    ceph orch upgrade stop

Note that canceling the upgrade simply stops the process; there is no ability to downgrade back to Pacific or Quincy.

Upgrading non-cephadm clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   1. If your cluster is running Pacific (16.2.x) or later, you might choose to first convert it to use cephadm so that the upgrade to Reef is automated (see above).
      For more information, see https://docs.ceph.com/en/reef/cephadm/adoption/.

   2. If your cluster is running Pacific (16.2.x) or later, systemd unit file names have changed to include the cluster fsid. To find the correct systemd unit file name for your cluster, run following command:

      ```
      systemctl -l | grep <daemon type>
      ```

      Example:

      ```
      $ systemctl -l | grep mon | grep active
      ceph-6ce0347c-314a-11ee-9b52-000af7995d6c@mon.f28-h21-000-r630.service                                           loaded active running   Ceph mon.f28-h21-000-r630 for 6ce0347c-314a-11ee-9b52-000af7995d6c
      ```

#. Set the `noout` flag for the duration of the upgrade. (Optional, but recommended.)

   .. prompt:: bash #

      ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the monitor daemons. For example, on each monitor host

   .. prompt:: bash #

      systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is complete by looking for the `reef` string in the mon map. The command

   .. prompt:: bash #

      ceph mon dump | grep min_mon_release

   should report:

   .. prompt:: bash #

      min_mon_release 18 (reef)

   If it does not, that implies that one or more monitors hasn't been upgraded and restarted and/or the quorum does not include all monitors.

#. Upgrade `ceph-mgr` daemons by installing the new packages and restarting all manager daemons. For example, on each manager host,

   .. prompt:: bash #

      systemctl restart ceph-mgr.target

   Verify the `ceph-mgr` daemons are running by checking `ceph -s`:

   .. prompt:: bash #

      ceph -s

   ::

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the ceph-osd daemons on all OSD hosts

   .. prompt:: bash #
   
      systemctl restart ceph-osd.target

#. Upgrade all CephFS MDS daemons. For each CephFS file system,

   #. Disable standby_replay:

         .. prompt:: bash #
         
            ceph fs set <fs_name> allow_standby_replay false

   #. If upgrading from Pacific <=16.2.5:

         .. prompt:: bash #
   
            ceph config set mon mon_mds_skip_sanity true

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

   #. Restore the original value of `max_mds` for the volume

      .. prompt:: bash #

         ceph fs set <fs_name> max_mds <original_max_mds>

   #. If upgrading from Pacific <=16.2.5 (followup to step 5.2):

      .. prompt:: bash #

         ceph config set mon mon_mds_skip_sanity false

#. Upgrade all radosgw daemons by upgrading packages and restarting daemons on all hosts

   .. prompt:: bash #

      systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Reef OSDs and enabling all new Reef-only functionality
   
   .. prompt:: bash #
   
      ceph osd require-osd-release reef

#. If you set `noout` at the beginning, be sure to clear it with

   .. prompt:: bash #

      ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment and orchestration framework to simplify cluster management and future upgrades. For more information on converting an existing cluster to cephadm, see https://docs.ceph.com/en/reef/cephadm/adoption/.

Post-upgrade
~~~~~~~~~~~~

#. Verify the cluster is healthy with `ceph health`. If your cluster is running Filestore, and you are upgrading directly from Pacific to Reef, a deprecation warning is expected. This warning can be temporarily muted using the following command

   .. prompt:: bash #

      ceph health mute OSD_FILESTORE

#. Consider enabling the `telemetry module <https://docs.ceph.com/en/reef/mgr/telemetry/>`_ to send anonymized usage statistics and crash information to the Ceph upstream developers. To see what would be reported (without actually sending any information to anyone),

   .. prompt:: bash #

      ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to automatically report the high-level cluster metadata with
   
   .. prompt:: bash #   
   
      ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at https://telemetry-public.ceph.com/.

Upgrading from pre-Pacific releases (like Octopus)
__________________________________________________

You **must** first upgrade to Pacific (16.2.z) or Quincy (17.2.z) before upgrading to Reef.
