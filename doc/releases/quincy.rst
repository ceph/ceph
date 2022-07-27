======
Quincy
======

Quincy is the 17th stable release of Ceph.  It is named after Squidward
Quincy Tentacles from Spongebob Squarepants.

v17.2.2 Quincy
==============

This is a hotfix release that resolves two security flaws.

Notable Changes
---------------
* Users who were running OpenStack Manila to export native CephFS, who
  upgraded their Ceph cluster from Nautilus (or earlier) to a later
  major version, were vulnerable to an attack by malicious users. The
  vulnerability allowed users to obtain access to arbitrary portions of
  the CephFS filesystem hierarchy, instead of being properly restricted
  to their own subvolumes. The vulnerability is due to a bug in the
  "volumes" plugin in Ceph Manager. This plugin is responsible for
  managing Ceph File System subvolumes which are used by OpenStack
  Manila services as a way to provide shares to Manila users.
  
  With this hotfix, the vulnerability is fixed. Administrators who are
  concerned they may have been impacted should audit the CephX keys in
  their cluster for proper path restrictions.
  
  Again, this vulnerability only impacts OpenStack Manila clusters which
  provided native CephFS access to their users.

* A regression made it possible to dereference a null pointer for
  for s3website requests that don't refer to a bucket resulting in an RGW
  segfault.

Changelog
---------
* mgr/volumes: Fix subvolume discover during upgrade (:ref:`CVE-2022-0670`, Kotresh HR)
* mgr/volumes: V2 Fix for test_subvolume_retain_snapshot_invalid_recreate (:ref:`CVE-2022-0670`, Kotresh HR)
* qa: validate subvolume discover on upgrade (Kotresh HR)
* rgw: s3website check for bucket before retargeting (Seena Fallah)

v17.2.1 Quincy
==============

This is the first bugfix release of Ceph Quincy.

Notable Changes
---------------
* The "BlueStore zero block detection" feature (first introduced to Quincy in
  https://github.com/ceph/ceph/pull/43337) has been turned off by default with a
  new global option called `bluestore_zero_block_detection`. This feature,
  intended for large-scale synthetic testing, does not interact well with some RBD
  and CephFS features. Any side effects experienced in previous Quincy versions
  would no longer occur, provided that the config option remains set to false.
  Relevant tracker: https://tracker.ceph.com/issues/55521

* telemetry: Added new Rook metrics to the 'basic' channel to report Rook's
  version, Kubernetes version, node metrics, etc.
  See a sample report with `ceph telemetry preview`.
  Opt-in with `ceph telemetry on`.

  For more details, see:

  https://docs.ceph.com/en/latest/mgr/telemetry/

* Add offline dup op trimming ability in the ceph-objectstore-tool.
  Relevant tracker: https://tracker.ceph.com/issues/53729

* Fixes a bug with cluster logs not being populated after log rotation.
  Relevant tracker: https://tracker.ceph.com/issues/55383

Changelog
---------
* .github/CODEOWNERS: tag core devs on core PRs (`pr#46519 <https://github.com/ceph/ceph/pull/46519>`_, Neha Ojha)
* .github: continue on error and reorder milestone step (`pr#46447 <https://github.com/ceph/ceph/pull/46447>`_, Ernesto Puerta)
* [quincy] mgr/alerts: Add Message-Id and Date header to sent emails (`pr#46311 <https://github.com/ceph/ceph/pull/46311>`_, Lorenz Bausch)
* ceph-fuse: ignore fuse mount failure if path is already mounted (`pr#45939 <https://github.com/ceph/ceph/pull/45939>`_, Nikhilkumar Shelke)
* ceph.in: clarify the usage of `--format` in the ceph command (`pr#46246 <https://github.com/ceph/ceph/pull/46246>`_, Laura Flores)
* ceph.spec.in: disable annobin plugin if compile with gcc-toolset (`pr#46377 <https://github.com/ceph/ceph/pull/46377>`_, Kefu Chai)
* ceph.spec.in: remove build directory at end of %install (`pr#45697 <https://github.com/ceph/ceph/pull/45697>`_, Tim Serong)
* ceph.spec.in: Use libthrift-devel on SUSE distros (`pr#45700 <https://github.com/ceph/ceph/pull/45700>`_, Tim Serong)
* ceph.spec: make ninja-build package install always (`pr#45875 <https://github.com/ceph/ceph/pull/45875>`_, Deepika Upadhyay)
* Cephadm Batch Backport April (`pr#46055 <https://github.com/ceph/ceph/pull/46055>`_, Adam King, Lukas Mayer, Ken Dreyer, Redouane Kachach, Aashish Sharma, Avan Thakkar, Moritz Röhrich, Teoman ONAY, Melissa Li, Christoph Glaubitz, Guillaume Abrioux, wangyunqing, Joseph Sawaya, Matan Breizman, Pere Diaz Bou, Michael Fritch, Patrick C. F. Ernzer)
* Cephadm Batch Backport May (`pr#46360 <https://github.com/ceph/ceph/pull/46360>`_, John Mulligan, Adam King, Prashant D, Redouane Kachach, Aashish Sharma, Ramana Raja, Ville Ojamo)
* cephadm: infer the default container image during pull (`pr#45568 <https://github.com/ceph/ceph/pull/45568>`_, Michael Fritch)
* cephadm: preserve `authorized_keys` file during upgrade (`pr#45359 <https://github.com/ceph/ceph/pull/45359>`_, Michael Fritch)
* cephadm: prometheus: The generatorURL in alerts is only using hostname (`pr#46353 <https://github.com/ceph/ceph/pull/46353>`_, Volker Theile)
* cephfs-shell: fix put and get cmd (`pr#46300 <https://github.com/ceph/ceph/pull/46300>`_, Dhairya Parmar, dparmar18)
* cephfs-top: Multiple filesystem support (`pr#46147 <https://github.com/ceph/ceph/pull/46147>`_, Neeraj Pratap Singh)
* client: add option to disable collecting and sending metrics (`pr#46476 <https://github.com/ceph/ceph/pull/46476>`_, Xiubo Li)
* cls/rgw: rgw_dir_suggest_changes detects race with completion (`pr#45901 <https://github.com/ceph/ceph/pull/45901>`_, Casey Bodley)
* cmake/modules: always use the python3 specified in command line (`pr#45966 <https://github.com/ceph/ceph/pull/45966>`_, Kefu Chai)
* cmake/rgw: add missing dependency on Arrow::Arrow (`pr#46144 <https://github.com/ceph/ceph/pull/46144>`_, Casey Bodley)
* cmake: resurrect mutex debugging in all Debug builds (`pr#45913 <https://github.com/ceph/ceph/pull/45913>`_, Ilya Dryomov)
* cmake: WITH_SYSTEM_UTF8PROC defaults to OFF (`pr#45766 <https://github.com/ceph/ceph/pull/45766>`_, Casey Bodley)
* CODEOWNERS: add RBD team (`pr#46542 <https://github.com/ceph/ceph/pull/46542>`_, Ilya Dryomov)
* debian: include the new object_format.py file (`pr#46409 <https://github.com/ceph/ceph/pull/46409>`_, John Mulligan)
* doc/cephfs/add-remove-mds: added cephadm note, refined "Adding an MDS" (`pr#45879 <https://github.com/ceph/ceph/pull/45879>`_, Dhairya Parmar)
* doc/dev: update basic-workflow.rst (`pr#46287 <https://github.com/ceph/ceph/pull/46287>`_, Zac Dover)
* doc/mgr/dashboard: Fix typo and double slash missing from URL (`pr#46075 <https://github.com/ceph/ceph/pull/46075>`_, Ville Ojamo)
* doc/start: add testing support information (`pr#45988 <https://github.com/ceph/ceph/pull/45988>`_, Zac Dover)
* doc/start: s/3/three/ in intro.rst (`pr#46325 <https://github.com/ceph/ceph/pull/46325>`_, Zac Dover)
* doc/start: update "memory" in hardware-recs.rst (`pr#46449 <https://github.com/ceph/ceph/pull/46449>`_, Zac Dover)
* Implement CIDR blocklisting (`pr#46469 <https://github.com/ceph/ceph/pull/46469>`_, Jos Collin, Greg Farnum)
* librbd/cache/pwl: fix bit field endianness issue (`pr#46094 <https://github.com/ceph/ceph/pull/46094>`_, Yin Congmin)
* mds: add a perf counter to record slow replies (`pr#46156 <https://github.com/ceph/ceph/pull/46156>`_, haoyixing)
* mds: include encoded stray inode when sending dentry unlink message to replicas (`issue#54046 <http://tracker.ceph.com/issues/54046>`_, `pr#46184 <https://github.com/ceph/ceph/pull/46184>`_, Venky Shankar)
* mds: reset heartbeat when fetching or committing entries (`pr#46181 <https://github.com/ceph/ceph/pull/46181>`_, Xiubo Li)
* mds: trigger to flush the mdlog in handle_find_ino() (`pr#46497 <https://github.com/ceph/ceph/pull/46497>`_, Xiubo Li)
* mgr/cephadm: Adding python natsort module (`pr#46065 <https://github.com/ceph/ceph/pull/46065>`_, Redouane Kachach)
* mgr/cephadm: try to get FQDN for configuration files (`pr#45665 <https://github.com/ceph/ceph/pull/45665>`_, Tatjana Dehler)
* mgr/dashboard:  don't log 3xx as errors (`pr#46453 <https://github.com/ceph/ceph/pull/46453>`_, Ernesto Puerta)
* mgr/dashboard: Compare values of MTU alert by device (`pr#45814 <https://github.com/ceph/ceph/pull/45814>`_, Aashish Sharma, Patrick Seidensal)
* mgr/dashboard: Creating and editing Prometheus AlertManager silences is buggy (`pr#46278 <https://github.com/ceph/ceph/pull/46278>`_, Volker Theile)
* mgr/dashboard: customizable log-in page text/banner (`pr#46342 <https://github.com/ceph/ceph/pull/46342>`_, Sarthak0702)
* mgr/dashboard: datatable in Cluster Host page hides wrong column on selection (`pr#45862 <https://github.com/ceph/ceph/pull/45862>`_, Sarthak0702)
* mgr/dashboard: extend daemon actions to host details (`pr#45722 <https://github.com/ceph/ceph/pull/45722>`_, Aashish Sharma, Nizamudeen A)
* mgr/dashboard: fix columns in host table  with NaN Undefined (`pr#46446 <https://github.com/ceph/ceph/pull/46446>`_, Avan Thakkar)
* mgr/dashboard: fix ssl cert validation for ingress service creation (`pr#46203 <https://github.com/ceph/ceph/pull/46203>`_, Avan Thakkar)
* mgr/dashboard: fix wrong pg status processing (`pr#46229 <https://github.com/ceph/ceph/pull/46229>`_, Ernesto Puerta)
* mgr/dashboard: form field validation icons overlap with other icons (`pr#46380 <https://github.com/ceph/ceph/pull/46380>`_, Sarthak0702)
* mgr/dashboard: highlight the search text in cluster logs (`pr#45679 <https://github.com/ceph/ceph/pull/45679>`_, Sarthak0702)
* mgr/dashboard: Imrove error message of '/api/grafana/validation' API endpoint (`pr#45957 <https://github.com/ceph/ceph/pull/45957>`_, Volker Theile)
* mgr/dashboard: introduce memory and cpu usage for daemons (`pr#46220 <https://github.com/ceph/ceph/pull/46220>`_, Aashish Sharma, Avan Thakkar)
* mgr/dashboard: Language dropdown box is partly hidden on login page (`pr#45619 <https://github.com/ceph/ceph/pull/45619>`_, Volker Theile)
* mgr/dashboard: RGW users and buckets tables are empty if the selected gateway is down (`pr#45867 <https://github.com/ceph/ceph/pull/45867>`_, Volker Theile)
* mgr/dashboard: Table columns hiding fix (`issue#51119 <http://tracker.ceph.com/issues/51119>`_, `pr#45724 <https://github.com/ceph/ceph/pull/45724>`_, Daniel Persson)
* mgr/dashboard: unselect rows in datatables (`pr#46323 <https://github.com/ceph/ceph/pull/46323>`_, Sarthak0702)
* mgr/dashboard: WDC multipath bug fixes (`pr#46455 <https://github.com/ceph/ceph/pull/46455>`_, Nizamudeen A)
* mgr/stats: be resilient to offline MDS rank-0 (`pr#45291 <https://github.com/ceph/ceph/pull/45291>`_, Jos Collin)
* mgr/telemetry: add Rook data (`pr#46486 <https://github.com/ceph/ceph/pull/46486>`_, Yaarit Hatuka)
* mgr/volumes: Fix idempotent subvolume rm (`pr#46140 <https://github.com/ceph/ceph/pull/46140>`_, Kotresh HR)
* mgr/volumes: set, get, list and remove metadata of snapshot (`pr#46508 <https://github.com/ceph/ceph/pull/46508>`_, Nikhilkumar Shelke)
* mgr/volumes: set, get, list and remove metadata of subvolume (`pr#45994 <https://github.com/ceph/ceph/pull/45994>`_, Nikhilkumar Shelke)
* mgr/volumes: Show clone failure reason in clone status command (`pr#45927 <https://github.com/ceph/ceph/pull/45927>`_, Kotresh HR)
* mon/LogMonitor: reopen log files on SIGHUP (`pr#46374 <https://github.com/ceph/ceph/pull/46374>`_, 胡玮文)
* mon/OSDMonitor: properly set last_force_op_resend in stretch mode (`pr#45871 <https://github.com/ceph/ceph/pull/45871>`_, Ilya Dryomov)
* mount/conf: Fix IPv6 parsing (`pr#46113 <https://github.com/ceph/ceph/pull/46113>`_, Matan Breizman)
* os/bluestore: set upper and lower bounds on rocksdb omap iterators (`pr#46175 <https://github.com/ceph/ceph/pull/46175>`_, Adam Kupczyk, Cory Snyder)
* os/bluestore: turn `bluestore zero block detection` off by default (`pr#46468 <https://github.com/ceph/ceph/pull/46468>`_, Laura Flores)
* osd/PGLog.cc: Trim duplicates by number of entries (`pr#46251 <https://github.com/ceph/ceph/pull/46251>`_, Nitzan Mordechai)
* osd/scrub: ignoring unsolicited DigestUpdate events (`pr#45595 <https://github.com/ceph/ceph/pull/45595>`_, Ronen Friedman)
* osd/scrub: restart snap trimming after a failed scrub (`pr#46418 <https://github.com/ceph/ceph/pull/46418>`_, Ronen Friedman)
* osd: return appropriate error if the object is not manifest (`pr#46061 <https://github.com/ceph/ceph/pull/46061>`_, Myoungwon Oh)
* qa/suites/rados/thrash-erasure-code-big/thrashers: add `osd max backfills` setting to mapgap and pggrow (`pr#46384 <https://github.com/ceph/ceph/pull/46384>`_, Laura Flores)
* qa/tasks/cephadm_cases: increase timeouts in test_cli.py (`pr#45625 <https://github.com/ceph/ceph/pull/45625>`_, Adam King)
* qa: add filesystem/file sync stuck test support (`pr#46496 <https://github.com/ceph/ceph/pull/46496>`_, Xiubo Li)
* qa: fix teuthology master branch ref (`pr#46503 <https://github.com/ceph/ceph/pull/46503>`_, Ernesto Puerta)
* qa: remove .teuthology_branch file (`pr#46491 <https://github.com/ceph/ceph/pull/46491>`_, Jeff Layton)
* Quincy: client: stop forwarding the request when exceeding 256 times (`pr#46178 <https://github.com/ceph/ceph/pull/46178>`_, Xiubo Li)
* Quincy: Wip doc backport quincy release notes to quincy branch 2022 05 24 (`pr#46381 <https://github.com/ceph/ceph/pull/46381>`_, Neha Ojha, David Galloway, Josh Durgin, Ilya Dryomov, Ernesto Puerta, Sridhar Seshasayee, Zac Dover, Yaarit Hatuka)
* rbd persistent cache UX improvements (status report, metrics, flush command) (`pr#45896 <https://github.com/ceph/ceph/pull/45896>`_, Ilya Dryomov, Yin Congmin)
* rgw: OpsLogFile::stop() signals under mutex (`pr#46038 <https://github.com/ceph/ceph/pull/46038>`_, Casey Bodley)
* rgw: remove rgw_rados_pool_pg_num_min and its use on pool creation use the cluster defaults for pg_num_min (`pr#46234 <https://github.com/ceph/ceph/pull/46234>`_, Casey Bodley)
* rgw: RGWCoroutine::set_sleeping() checks for null stack (`pr#46041 <https://github.com/ceph/ceph/pull/46041>`_, Or Friedmann, Casey Bodley)
* rgw_reshard: drop olh entries with empty name (`pr#45846 <https://github.com/ceph/ceph/pull/45846>`_, Dan van der Ster)
* rocksdb: build with rocksdb-7.y.z (`pr#46492 <https://github.com/ceph/ceph/pull/46492>`_, Kaleb S. KEITHLEY)
* rpm: use system libpmem on Centos 9 Stream (`pr#46212 <https://github.com/ceph/ceph/pull/46212>`_, Ilya Dryomov)
* run-make-check.sh: enable RBD persistent caches (`pr#45992 <https://github.com/ceph/ceph/pull/45992>`_, Ilya Dryomov)
* test/rbd_mirror: grab timer lock before calling add_event_after() (`pr#45905 <https://github.com/ceph/ceph/pull/45905>`_, Ilya Dryomov)
* test: fix TierFlushDuringFlush to wait until dedup_tier is set on base pool (`issue#53855 <http://tracker.ceph.com/issues/53855>`_, `pr#45624 <https://github.com/ceph/ceph/pull/45624>`_, Sungmin Lee)
* test: No direct use of nose (`pr#46254 <https://github.com/ceph/ceph/pull/46254>`_, Steve Kowalik)
* Wip doc pr 46109 backport to quincy (`pr#46116 <https://github.com/ceph/ceph/pull/46116>`_, Ville Ojamo)

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
  collects various performance metrics is now available for operators to opt
  into with:
  `ceph telemetry on`
  `ceph telemetry enable channel perf`
  See a sample report with `ceph telemetry preview`.
  Note that generating a telemetry report with 'perf' channel data might
  take a few moments in big clusters.
  For more details, see:
  https://docs.ceph.com/en/quincy/mgr/telemetry/

* MGR: The progress module disables the pg recovery event by default since the
  event is expensive and has interrupted other services when there are OSDs
  being marked in/out from the cluster. However, the user can still enable
  this event anytime. For more detail, see:

  https://docs.ceph.com/en/quincy/mgr/progress/

* https://tracker.ceph.com/issues/55383 is a known issue -
  to continue to log cluster log messages to file,
  run `ceph config set mon mon_cluster_log_to_file true` after every log rotation.

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
