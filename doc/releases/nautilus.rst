v14.1.0 Nautilus (release candidate 1)
======================================

.. note: We expect to make a msgr2 protocol revision after this first
   release candidate.  If you upgrade to v14.1.0 *and* enable msgr2,
   you will need to restart all daemons after upgrading to v14.1.1 or
   any other later nautilus release.

.. note: These are draft notes for the first Nautilus release.

Major Changes from Mimic
------------------------

- *Dashboard*:

  The :ref:`mgr-dashboard` has gained a lot of new functionality:

  * Support for multiple users / roles
  * SSO (SAMLv2) for user authentication
  * Auditing support
  * New landing page, showing more metrics and health info
  * I18N support
  * REST API documentation with Swagger API

  New Ceph management features include:

  * OSD management (mark as down/out, change OSD settings, recovery profiles)
  * Cluster config settings editor
  * Ceph Pool management (create/modify/delete)
  * ECP management
  * RBD mirroring configuration
  * Embedded Grafana Dashboards (derived from Ceph Metrics)
  * CRUSH map viewer
  * NFS Ganesha management
  * iSCSI target management (via :ref:`ceph-iscsi`)
  * RBD QoS configuration
  * Ceph Manager (ceph-mgr) module management
  * Prometheus alert Management

  Also, the Ceph Dashboard is now split into its own package named
  ``ceph-mgr-dashboard``. So, you might want to install it separately,
  if your package management software fails to do so when it installs
  ``ceph-mgr``.

- *RADOS*:

  * The number of placement groups (PGs) per pool can now be decreased
    at any time, and the cluster can automatically tune the PG count
    based on cluster utilization or administrator hints.
  * The new :ref:`v2 wire protocol <msgr2>` brings support for encryption on the wire.
  * Physical storage devices consumed by OSD and Monitor daemons are
    now tracked by the cluster along with health metrics (i.e.,
    SMART), and the cluster can apply a pre-trained prediction model
    or a cloud-based prediction service to warn about expected
    HDD or SSD failures.
  * The NUMA node for OSD daemons can easily be monitored via the
    ``ceph osd numa-status`` command, and configured via the
    ``osd_numa_node`` config option.
  * When BlueStore OSDs are used, space utilization is now broken down
    by object data, omap data, and internal metadata, by pool, and by
    pre- and post- compression sizes.
  * OSDs more effectively prioritize the most important PGs and
    objects when performing recovery and backfill.
  * Progress for long-running background processes--like recovery
    after a device failure--is now reported as part of ``ceph
    status``.
  * An experimental `Coupled-Layer "Clay" erasure code
    <https://www.usenix.org/conference/fast18/presentation/vajha>`_
    plugin has been added that reduces network bandwidth and IO needed
    for most recovery operations.

- *RGW*:

  * S3 lifecycle transition for tiering between storage classes.
  * A new web frontend (Beast) has replaced civetweb as the default,
    improving overall performance.
  * A new publish/subscribe infrastructure allows RGW to feed events
    to serverless frameworks like knative or data pipelies like Kafka.
  * A range of authentication features, including STS federation using
    OAuth2 and OpenID::connect and an OPA (Open Policy Agent)
    authentication delegation prototype.
  * The new archive zone federation feature enables full preservation
    of all objects (including history) in a separate zone.

- *CephFS*:

  * MDS stability has been greatly improved for large caches and
    long-running clients with a lot of RAM. Cache trimming and client
    capability recall is now throttled to prevent overloading the MDS.
  * CephFS may now be exported via NFS-Ganesha clusters in environments managed
    by Rook. Ceph manages the clusters and ensures high-availability and
    scalability. An `introductory demo
    <https://ceph.com/community/deploying-a-cephnfs-server-cluster-with-rook/>`_
    is available. More automation of this feature is expected to be forthcoming
    in future minor releases of Nautilus.
  * The MDS ``mds_standby_for_*``, ``mon_force_standby_active``, and
    ``mds_standby_replay`` configuration options have been obsoleted. Instead,
    the operator :ref:`may now set <mds-standby-replay>` the new
    ``allow_standby_replay`` flag on the CephFS file system. This setting
    causes standbys to become standby-replay for any available rank in the file
    system.
  * MDS now supports dropping its cache which concurrently asks clients
    to trim their caches. This is done using MDS admin socket ``cache drop``
    command.
  * It is now possible to check the progress of an on-going scrub in the MDS.
    Additionally, a scrub may be paused or aborted. See :ref:`the scrub
    documentation <mds-scrub>` for more information.
  * A new interface for creating volumes is provided via the ``ceph volume``
    command-line-interface.
  * A new cephfs-shell tool is available for manipulating a CephFS file
    system without mounting.
  * CephFS-related output from ``ceph status`` has been reformatted for brevity,
    clarity, and usefulness.
  * Lazy IO has been revamped. It can be turned on by the client using the new
    CEPH_O_LAZY flag to the ``ceph_open`` C/C++ API or via the config option
    ``client_force_lazyio``.
  * CephFS file system can now be brought down rapidly via the ``ceph fs fail``
    command. See :ref:`the administration page <cephfs-administration>` for
    more information.

- *RBD*:

  * Images can be live-migrated with minimal downtime to assist with moving
    images between pools or to new layouts.
  * New ``rbd perf image iotop`` and ``rbd perf image iostat`` commands provide
    an iotop- and iostat-like IO monitor for all RBD images.
  * The *ceph-mgr* Prometheus exporter now optionally includes an IO monitor
    for all RBD images.
  * Support for separate image namespaces within a pool for tenant isolation.

- *Misc*:

  * Ceph has a new set of :ref:`orchestrator modules
    <orchestrator-cli-module>` to directly interact with external
    orchestrators like ceph-ansible, DeepSea, Rook, or simply ssh via
    a consistent CLI (and, eventually, Dashboard) interface.


Upgrading from Mimic or Luminous
--------------------------------

Notes
~~~~~

* During the upgrade from Luminous to nautilus, it will not be
  possible to create a new OSD using a Luminous ceph-osd daemon after
  the monitors have been upgraded to Nautilus.  We recommend you avoid adding
  or replacing any OSDs while the upgrade is in process.

* We recommend you avoid creating any RADOS pools while the upgrade is
  in process.

* You can monitor the progress of your upgrade at each stage with the
  ``ceph versions`` command, which will tell you what ceph version(s) are
  running for each type of daemon.

Instructions
~~~~~~~~~~~~

#. If your cluster was originally installed with a version prior to
   Luminous, ensure that it has completed at least one full scrub of
   all PGs while running Luminous.  Failure to do so will cause your
   monitor daemons to refuse to join the quorum on start, leaving them
   non-functional.

   If you are unsure whether or not your Luminous cluster has
   completed a full scrub of all PGs, you can check your cluster's
   state by running::

     # ceph osd dump | grep ^flags

   In order to be able to proceed to Nautilus, your OSD map must include
   the ``recovery_deletes`` and ``purged_snapdirs`` flags.

   If your OSD map does not contain both these flags, you can simply
   wait for approximately 24-48 hours, which in a standard cluster
   configuration should be ample time for all your placement groups to
   be scrubbed at least once, and then repeat the above process to
   recheck.

   However, if you have just completed an upgrade to Luminous and want
   to proceed to Mimic in short order, you can force a scrub on all
   placement groups with a one-line shell command, like::

     # ceph pg dump pgs_brief | cut -d " " -f 1 | xargs -n1 ceph pg scrub

   You should take into consideration that this forced scrub may
   possibly have a negative impact on your Ceph clients' performance.

#. Make sure your cluster is stable and healthy (no down or
   recovering OSDs).  (Optional, but recommended.)

#. Set the ``noout`` flag for the duration of the upgrade. (Optional,
   but recommended.)::

     # ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  For example,::

     # systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is
   complete by looking for the ``nautilus`` string in the mon
   map.  For example::

     # ceph mon dump | grep min_mon_release

   should report::

     min_mon_release 14 (nautilus)

   If it doesn't, that implies that one or more monitors hasn't been
   upgraded and restarted and the quorum is not complete.

#. Upgrade ``ceph-mgr`` daemons by installing the new packages and
   restarting all manager daemons.  For example,::

     # systemctl restart ceph-mgr.target

   Please note, if you are using Ceph Dashboard, you will probably need to
   install ``ceph-mgr-dashboard`` separately after upgrading ``ceph-mgr``
   package. The install script of ``ceph-mgr-dashboard`` will restart the
   manager daemons automatically for you. So in this case, you can just skip
   the step to restart the daemons.

   Verify the ``ceph-mgr`` daemons are running by checking ``ceph
   -s``::

     # ceph -s

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...
  
#. Upgrade all OSDs by installing the new packages and restarting the
   ceph-osd daemons on all hosts::

     # systemctl restart ceph-osd.target

   You can monitor the progress of the OSD upgrades with the
   ``ceph versions`` or ``ceph osd versions`` command::

     # ceph osd versions
     {
        "ceph version 13.2.5 (...) mimic (stable)": 12,
        "ceph version 14.2.0 (...) nautilus (stable)": 22,
     }

#. If there are any OSDs in the cluster deployed with ceph-disk (e.g.,
   almost any OSDs that were created before the Mimic release), you
   need to tell ceph-volume to adopt responsibility for starting the
   daemons.  On each host containing OSDs, ensure the OSDs are
   currently running, and then::

     # ceph-volume simple scan
     # ceph-volume simple activate --all

   We recommend that each OSD host be rebooted following this step to
   verify that the OSDs start up automatically.

   Note that ceph-volume doesn't have the same hot-plug capability
   that ceph-disk did, where a newly attached disk is automatically
   detected via udev events.  If the OSD isn't currently running when the
   above ``scan`` command is run, or a ceph-disk-based OSD is moved to
   a new host, or the host OSD is reinstalled, or the
   ``/etc/ceph/osd`` directory is lost, you will need to scan the main
   data partition for each ceph-disk OSD explicitly.  For example,::

     # ceph-volume simple scan /dev/sdb1

   The output will include the appopriate ``ceph-volume simple
   activate`` command to enable the OSD.

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

     # systemctl restart radosgw.target

#. Complete the upgrade by disallowing pre-Nautilus OSDs and enabling
   all new Nautilus-only functionality::

     # ceph osd require-osd-release nautilus

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

#. To enable the new :ref:`v2 network protocol <msgr2>`, issue the
   following command::

     ceph mon enable-msgr2

   This will instruct all monitors that bind to the old default port
   6789 for the legacy v1 protocol to also bind to the new 3300 v2
   protocol port.  To see if all monitors have been updated,::

     ceph mon dump

   and verify that each monitor has both a ``v2:`` and ``v1:`` address
   listed.

#. For each host that has been upgrade, you should update your
   ``ceph.conf`` file so that it references both the v2 and v1
   addresses.  Things will still work if only the v1 IP and port are
   listed, but each CLI instantiation or daemon will need to reconnect
   after learning the monitors real IPs, slowing things down a bit and
   preventing a full transition to the v2 protocol.

   This is also a good time to fully transition any config options in
   ceph.conf into the cluster's configuration database.  On each host,
   you can use the following command to import any option into the
   monitors with::

     ceph config assimilate-conf -i /etc/ceph/ceph.conf

   To create a minimal but sufficient ceph.conf for each host,::

     ceph config generate-minimal-conf > /etc/ceph/ceph.conf.new
     mv /etc/ceph/ceph.conf.new /etc/ceph/ceph.conf

   Be sure to use this new config--and, specifically, the new syntax
   for the ``mon_host`` option that lists both ``v2:`` and ``v1:``
   addresses in brackets--on hosts that have been upgraded to
   Nautilus, since pre-nautilus versions of Ceph to not understand the
   syntax.

#. Consider enabling the :ref:`telemetry module <telemetry>` to send
   anonymized usage statistics and crash information to the Ceph
   upstream developers.  To see what would be reported (without actually
   sending any information to anyone),::

     ceph mgr module enable telemetry
     ceph telemetry show

   If you are comfortable with the data that is reported, you can opt-in to
   automatically report the high-level cluster metadata with::

     ceph telemetry on

Upgrading from pre-Luminous releases (like Jewel)
-------------------------------------------------

You *must* first upgrade to Luminous (12.2.z) before attempting an
upgrade to Nautilus.  In addition, your cluster must have completed at
least one scrub of all PGs while running Luminous, setting the
``recovery_deletes`` and ``purged_snapdirs`` flags in the OSD map.


Upgrade compatibility notes
---------------------------

These changes occurred between the Mimic and Nautilus releases.

* ``ceph pg stat`` output has been modified in json
  format to match ``ceph df`` output:

  - "raw_bytes" field renamed to "total_bytes"
  - "raw_bytes_avail" field renamed to "total_bytes_avail"
  - "raw_bytes_avail" field renamed to "total_bytes_avail"
  - "raw_bytes_used" field renamed to "total_bytes_raw_used"
  - "total_bytes_used" field added to represent the space (accumulated over
     all OSDs) allocated purely for data objects kept at block(slow) device
  
* ``ceph df [detail]`` output (GLOBAL section) has been modified in plain
  format:

  - new 'USED' column shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - 'RAW USED' is now a sum of 'USED' space and space allocated/reserved at
     block device for Ceph purposes, e.g. BlueFS part for BlueStore.

* ``ceph df [detail]`` output (GLOBAL section) has been modified in json
  format:
  
  - 'total_used_bytes' column now shows the space (accumulated over all OSDs)
    allocated purely for data objects kept at block(slow) device
  - new 'total_used_raw_bytes' column shows a sum of 'USED' space and space
    allocated/reserved at block device for Ceph purposes, e.g. BlueFS part for
    BlueStore.

* ``ceph df [detail]`` output (POOLS section) has been modified in plain
  format:
  
  - 'BYTES USED' column renamed to 'STORED'. Represents amount of data
    stored by the user.
  - 'USED' column now represent amount of space allocated purely for data
    by all OSD nodes in KB.
  - 'QUOTA BYTES', 'QUOTA OBJECTS' aren't showed anymore in non-detailed mode.
  - new column 'USED COMPR' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'UNDER COMPR' - amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.
  - Some columns reordering

* ``ceph df [detail]`` output (POOLS section) has been modified in json
  format:
  
  - 'bytes used' column renamed to 'stored'. Represents amount of data
    stored by the user.
  - 'raw bytes used' column renamed to "stored_raw". Totals of user data
     over all OSD excluding degraded.
  - new 'bytes_used' column now represent amount of space allocated by 
    all OSD nodes.
  - 'kb_used' column - the same as 'bytes_used' but in KB.
  - new column 'compress_bytes_used' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'compress_under_bytes' amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``rados df [detail]`` output (POOLS section) has been modified in plain
  format:
  
  - 'USED' column now shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - new column 'USED COMPR' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'UNDER COMPR' - amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``rados df [detail]`` output (POOLS section) has been modified in json
  format:
  
  - 'size_bytes' and 'size_kb' columns now show the space (accumulated
    over all OSDs) allocated purely for data objects kept at block
    device.
  - new column 'compress_bytes_used' - amount of space allocated for compressed
    data. i.e., compressed data plus all the allocation, replication and erasure
    coding overhead.
  - new column 'compress_under_bytes' amount of data passed through compression
    (summed over all replicas) and beneficial enough to be stored in a
    compressed form.

* ``ceph pg dump`` output (totals section) has been modified in json
  format:
  
  - new 'USED' column shows the space (accumulated over all OSDs) allocated
    purely for data objects kept at block(slow) device.
  - 'USED_RAW' is now a sum of 'USED' space and space allocated/reserved at
    block device for Ceph purposes, e.g. BlueFS part for BlueStore.

* The ``ceph osd rm`` command has been deprecated.  Users should use
  ``ceph osd destroy`` or ``ceph osd purge`` (but after first confirming it is
  safe to do so via the ``ceph osd safe-to-destroy`` command).

* The MDS now supports dropping its cache for the purposes of benchmarking.::

    ceph tell mds.* cache drop <timeout>

  Note that the MDS cache is cooperatively managed by the clients. It is
  necessary for clients to give up capabilities in order for the MDS to fully
  drop its cache. This is accomplished by asking all clients to trim as many
  caps as possible. The timeout argument to the ``cache drop`` command controls
  how long the MDS waits for clients to complete trimming caps. This is optional
  and is 0 by default (no timeout). Keep in mind that clients may still retain
  caps to open files which will prevent the metadata for those files from being
  dropped by both the client and the MDS. (This is an equivalent scenario to
  dropping the Linux page/buffer/inode/dentry caches with some processes pinning
  some inodes/dentries/pages in cache.)

* The ``mon_health_preluminous_compat`` and
  ``mon_health_preluminous_compat_warning`` config options are
  removed, as the related functionality is more than two versions old.
  Any legacy monitoring system expecting Jewel-style health output
  will need to be updated to work with Nautilus.

* Nautilus is not supported on any distros still running upstart so upstart
  specific files and references have been removed.

* The ``ceph pg <pgid> list_missing`` command has been renamed to
  ``ceph pg <pgid> list_unfound`` to better match its behaviour.

* The *rbd-mirror* daemon can now retrieve remote peer cluster configuration
  secrets from the monitor. To use this feature, the rbd-mirror daemon
  CephX user for the local cluster must use the ``profile rbd-mirror`` mon cap.
  The secrets can be set using the ``rbd mirror pool peer add`` and
  ``rbd mirror pool peer set`` actions.

* The 'rbd-mirror' daemon will now run in active/active mode by default, where
  mirrored images are evenly distributed between all active 'rbd-mirror'
  daemons. To revert to active/passive mode, override the
  'rbd_mirror_image_policy_type' config key to 'none'.

* The ``ceph mds deactivate`` is fully obsolete and references to it in the docs
  have been removed or clarified.

* The libcephfs bindings added the ``ceph_select_filesystem`` function
  for use with multiple filesystems.

* The cephfs python bindings now include ``mount_root`` and ``filesystem_name``
  options in the mount() function.

* erasure-code: add experimental *Coupled LAYer (CLAY)* erasure codes
  support. It features less network traffic and disk I/O when performing
  recovery.

* The ``cache drop`` OSD command has been added to drop an OSD's caches:

    - ``ceph tell osd.x cache drop``

* The ``cache status`` OSD command has been added to get the cache stats of an
  OSD:

    - ``ceph tell osd.x cache status``

* The libcephfs added several functions that allow restarted client to destroy
  or reclaim state held by a previous incarnation. These functions are for NFS
  servers.

* The ``ceph`` command line tool now accepts keyword arguments in
  the format ``--arg=value`` or ``--arg value``.

* ``librados::IoCtx::nobjects_begin()`` and
  ``librados::NObjectIterator`` now communicate errors by throwing a
  ``std::system_error`` exception instead of ``std::runtime_error``.

* The callback function passed to ``LibRGWFS.readdir()`` now accepts a ``flags``
  parameter. it will be the last parameter passed to  ``readdir()`` method.

* The ``cephfs-data-scan scan_links`` now automatically repair inotables and
  snaptable.

* Configuration values ``mon_warn_not_scrubbed`` and
  ``mon_warn_not_deep_scrubbed`` have been renamed.  They are now
  ``mon_warn_pg_not_scrubbed_ratio`` and ``mon_warn_pg_not_deep_scrubbed_ratio``
  respectively.  This is to clarify that these warnings are related to
  pg scrubbing and are a ratio of the related interval.  These options
  are now enabled by default.

* The MDS cache trimming is now throttled. Dropping the MDS cache
  via the ``ceph tell mds.<foo> cache drop`` command or large reductions in the
  cache size will no longer cause service unavailability.

* The CephFS MDS behavior with recalling caps has been significantly improved
  to not attempt recalling too many caps at once, leading to instability.
  MDS with a large cache (64GB+) should be more stable.

* MDS now provides a config option ``mds_max_caps_per_client`` (default: 1M) to
  limit the number of caps a client session may hold. Long running client
  sessions with a large number of caps have been a source of instability in the
  MDS when all of these caps need to be processed during certain session
  events. It is recommended to not unnecessarily increase this value.

* The MDS config ``mds_recall_state_timeout`` has been removed. Late
  client recall warnings are now generated based on the number of caps
  the MDS has recalled which have not been released. The new configs
  ``mds_recall_warning_threshold`` (default: 32K) and
  ``mds_recall_warning_decay_rate`` (default: 60s) sets the threshold
  for this warning.

* The Telegraf module for the Manager allows for sending statistics to
  an Telegraf Agent over TCP, UDP or a UNIX Socket. Telegraf can then
  send the statistics to databases like InfluxDB, ElasticSearch, Graphite
  and many more.

* The graylog fields naming the originator of a log event have
  changed: the string-form name is now included (e.g., ``"name":
  "mgr.foo"``), and the rank-form name is now in a nested section
  (e.g., ``"rank": {"type": "mgr", "num": 43243}``).

* If the cluster log is directed at syslog, the entries are now
  prefixed by both the string-form name and the rank-form name (e.g.,
  ``mgr.x mgr.12345 ...`` instead of just ``mgr.12345 ...``).

* The JSON output of the ``ceph osd find`` command has replaced the ``ip``
  field with an ``addrs`` section to reflect that OSDs may bind to
  multiple addresses.

* CephFS clients without the 's' flag in their authentication capability
  string will no longer be able to create/delete snapshots. To allow
  ``client.foo`` to create/delete snapshots in the ``bar`` directory of
  filesystem ``cephfs_a``, use command:

    - ``ceph auth caps client.foo mon 'allow r' osd 'allow rw tag cephfs data=cephfs_a' mds 'allow rw, allow rws path=/bar'``

* The ``osd_heartbeat_addr`` option has been removed as it served no
  (good) purpose: the OSD should always check heartbeats on both the
  public and cluster networks.

* The ``rados`` tool's ``mkpool`` and ``rmpool`` commands have been
  removed because they are redundant; please use the ``ceph osd pool
  create`` and ``ceph osd pool rm`` commands instead.

* The ``auid`` property for cephx users and RADOS pools has been
  removed.  This was an undocumented and partially implemented
  capability that allowed cephx users to map capabilities to RADOS
  pools that they "owned".  Because there are no users we have removed
  this support.  If any cephx capabilities exist in the cluster that
  restrict based on auid then they will no longer parse, and the
  cluster will report a health warning like::

    AUTH_BAD_CAPS 1 auth entities have invalid capabilities
        client.bad osd capability parse failed, stopped at 'allow rwx auid 123' of 'allow rwx auid 123'

  The capability can be adjusted with the ``ceph auth caps``
  command. For example,::

    ceph auth caps client.bad osd 'allow rwx pool foo'

* The ``ceph-kvstore-tool`` ``repair`` command has been renamed
  ``destructive-repair`` since we have discovered it can corrupt an
  otherwise healthy rocksdb database.  It should be used only as a last-ditch
  attempt to recover data from an otherwise corrupted store.


* The default memory utilization for the mons has been increased
  somewhat.  Rocksdb now uses 512 MB of RAM by default, which should
  be sufficient for small to medium-sized clusters; large clusters
  should tune this up.  Also, the ``mon_osd_cache_size`` has been
  increase from 10 OSDMaps to 500, which will translate to an
  additional 500 MB to 1 GB of RAM for large clusters, and much less
  for small clusters.

* The ``mgr/balancer/max_misplaced`` option has been replaced by a new
  global ``target_max_misplaced_ratio`` option that throttles both
  balancer activity and automated adjustments to ``pgp_num`` (normally as a
  result of ``pg_num`` changes).  If you have customized the balancer module
  option, you will need to adjust your config to set the new global option
  or revert to the default of .05 (5%).

* By default, Ceph no longer issues a health warning when there are
  misplaced objects (objects that are fully replicated but not stored
  on the intended OSDs).  You can reenable the old warning by setting
  ``mon_warn_on_misplaced`` to ``true``.

* The ``ceph-create-keys`` tool is now obsolete.  The monitors
  automatically create these keys on their own.  For now the script
  prints a warning message and exits, but it will be removed in the
  next release.  Note that ``ceph-create-keys`` would also write the
  admin and bootstrap keys to /etc/ceph and /var/lib/ceph, but this
  script no longer does that.  Any deployment tools that relied on
  this behavior should instead make use of the ``ceph auth export
  <entity-name>`` command for whichever key(s) they need.

* The ``mon_osd_pool_ec_fast_read`` option has been renamed
  ``osd_pool_default_ec_fast_read`` to be more consistent with other
  ``osd_pool_default_*`` options that affect default values for newly
  created RADOS pools.

* The ``mon addr`` configuration option is now deprecated.  It can
  still be used to specify an address for each monitor in the
  ``ceph.conf`` file, but it only affects cluster creation and
  bootstrapping, and it does not support listing multiple addresses
  (e.g., both a v2 and v1 protocol address).  We strongly recommend
  the option be removed and instead a single ``mon host`` option be
  specified in the ``[global]`` section to allow daemons and clients
  to discover the monitors.

* New command ``ceph fs fail`` has been added to quickly bring down a file
  system. This is a single command that unsets the joinable flag on the file
  system and brings down all of its ranks.

* The ``cache drop`` admin socket command has been removed. The ``ceph
  tell mds.X cache drop`` remains.


Detailed Changelog
------------------
