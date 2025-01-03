=======
Pacific
=======

v16.2.15 Pacific
================

This is the fifteenth, and expected to be last, backport release in the Pacific series.

Notable Changes
---------------

* `ceph config dump --format <json|xml>` output will display the localized
  option names instead of their normalized version. For example,
  "mgr/prometheus/x/server_port" will be displayed instead of
  "mgr/prometheus/server_port". This matches the output of the non pretty-print
  formatted version of the command.

* CephFS: MDS evicts clients who are not advancing their request tids, which causes
  a large buildup of session metadata, resulting in the MDS going read-only due to
  the RADOS operation exceeding the size threshold. The `mds_session_metadata_threshold`
  config controls the maximum size that an (encoded) session metadata can grow.

* RADOS: The `get_pool_is_selfmanaged_snaps_mode` C++ API has been deprecated
  due to its susceptibility to false negative results.  Its safer replacement is
  `pool_is_in_selfmanaged_snaps_mode`.

* RBD: When diffing against the beginning of time (`fromsnapname == NULL`) in
  fast-diff mode (`whole_object == true` with `fast-diff` image feature enabled
  and valid), diff-iterate is now guaranteed to execute locally if exclusive
  lock is available.  This brings a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.

Changelog
---------

* [CVE-2023-43040] rgw: Fix bucket validation against POST policies (`pr#53758 <https://github.com/ceph/ceph/pull/53758>`_, Joshua Baergen)
* admin/doc-requirements: bump Sphinx to 5.0.2 (`pr#55258 <https://github.com/ceph/ceph/pull/55258>`_, Nizamudeen A)
* blk/kernel: Add O_EXCL for block devices (`pr#53567 <https://github.com/ceph/ceph/pull/53567>`_, Adam Kupczyk)
* Bluestore: fix bluestore collection_list latency perf counter (`pr#52949 <https://github.com/ceph/ceph/pull/52949>`_, Wangwenjuan)
* bluestore: Fix problem with volume selector (`pr#53587 <https://github.com/ceph/ceph/pull/53587>`_, Adam Kupczyk)
* ceph-volume,python-common: Data allocate fraction (`pr#53581 <https://github.com/ceph/ceph/pull/53581>`_, Jonas Pfefferle)
* ceph-volume: add --osd-id option to raw prepare (`pr#52928 <https://github.com/ceph/ceph/pull/52928>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in _check_generic_reject_reasons (`pr#54707 <https://github.com/ceph/ceph/pull/54707>`_, Kim Minjong, Guillaume Abrioux, Michael English)
* ceph-volume: fix raw list for lvm devices (`pr#52981 <https://github.com/ceph/ceph/pull/52981>`_, Guillaume Abrioux)
* ceph-volume: fix zap_partitions() in devices.lvm.zap (`pr#55658 <https://github.com/ceph/ceph/pull/55658>`_, Guillaume Abrioux)
* ceph-volume: fix zap_partitions() in devices.lvm.zap (`pr#55481 <https://github.com/ceph/ceph/pull/55481>`_, Guillaume Abrioux)
* ceph-volume: fixes fallback to stat in is_device and is_partition (`pr#54709 <https://github.com/ceph/ceph/pull/54709>`_, Guillaume Abrioux, Teoman ONAY)
* ceph: allow xlock state to be LOCK_PREXLOCK when putting it (`pr#53662 <https://github.com/ceph/ceph/pull/53662>`_, Xiubo Li)
* cephadm: add tcmu-runner to logrotate config (`pr#53975 <https://github.com/ceph/ceph/pull/53975>`_, Adam King)
* cephadm: Adding support to configure public_network cfg section (`pr#52411 <https://github.com/ceph/ceph/pull/52411>`_, Redouane Kachach)
* cephadm: allow ports to be opened in firewall during adoption, reconfig, redeploy (`pr#52083 <https://github.com/ceph/ceph/pull/52083>`_, Adam King)
* cephadm: make custom_configs work for tcmu-runner container (`pr#53469 <https://github.com/ceph/ceph/pull/53469>`_, Adam King)
* cephadm: run tcmu-runner through script to do restart on failure (`pr#53977 <https://github.com/ceph/ceph/pull/53977>`_, Adam King, Raimund Sacherer)
* cephfs-journal-tool: disambiguate usage of all keyword (in tool help) (`pr#53645 <https://github.com/ceph/ceph/pull/53645>`_, Manish M Yathnalli)
* cephfs-mirror: do not run concurrent C_RestartMirroring context (`issue#62072 <http://tracker.ceph.com/issues/62072>`_, `pr#53640 <https://github.com/ceph/ceph/pull/53640>`_, Venky Shankar)
* cephfs-top: include the missing fields in --dump output (`pr#53453 <https://github.com/ceph/ceph/pull/53453>`_, Jos Collin)
* cephfs: upgrade cephfs-shell's path wherever necessary (`pr#54144 <https://github.com/ceph/ceph/pull/54144>`_, Rishabh Dave)
* cephfs_mirror: correctly set top level dir permissions (`pr#53270 <https://github.com/ceph/ceph/pull/53270>`_, Milind Changire)
* client: always refresh mds feature bits on session open (`issue#63188 <http://tracker.ceph.com/issues/63188>`_, `pr#54245 <https://github.com/ceph/ceph/pull/54245>`_, Venky Shankar)
* client: fix sync fs to force flush mdlog for all sessions (`pr#53981 <https://github.com/ceph/ceph/pull/53981>`_, Xiubo Li)
* client: issue a cap release immediately if no cap exists (`pr#52852 <https://github.com/ceph/ceph/pull/52852>`_, Xiubo Li)
* client: queue a delay cap flushing if there are ditry caps/snapcaps (`pr#54472 <https://github.com/ceph/ceph/pull/54472>`_, Xiubo Li)
* cmake/modules/BuildRocksDB.cmake: inherit parent's CMAKE_CXX_FLAGS (`pr#55500 <https://github.com/ceph/ceph/pull/55500>`_, Kefu Chai)
* common/weighted_shuffle: don't feed std::discrete_distribution with all-zero weights (`pr#55155 <https://github.com/ceph/ceph/pull/55155>`_, Radosław Zarzyński)
* common:  intrusive_lru destructor add (`pr#54558 <https://github.com/ceph/ceph/pull/54558>`_, Ali Maredia)
* doc/cephfs: note regarding start time time zone (`pr#53576 <https://github.com/ceph/ceph/pull/53576>`_, Milind Changire)
* doc/cephfs: write cephfs commands fully in docs (`pr#53403 <https://github.com/ceph/ceph/pull/53403>`_, Rishabh Dave)
* doc/rados/configuration/bluestore-config-ref: Fix lowcase typo (`pr#54696 <https://github.com/ceph/ceph/pull/54696>`_, Adam Kupczyk)
* doc/rados: update config for autoscaler (`pr#55440 <https://github.com/ceph/ceph/pull/55440>`_, Zac Dover)
* doc: clarify use of `rados rm` command (`pr#51260 <https://github.com/ceph/ceph/pull/51260>`_, J. Eric Ivancich)
* doc: discuss the standard multi-tenant CephFS security model (`pr#53560 <https://github.com/ceph/ceph/pull/53560>`_, Greg Farnum)
* Fixing example of BlueStore resharding (`pr#54474 <https://github.com/ceph/ceph/pull/54474>`_, Adam Kupczyk)
* isa-l: incorporate fix for aarch64 text relocation (`pr#51314 <https://github.com/ceph/ceph/pull/51314>`_, luo rixin)
* libcephsqlite: fill 0s in unread portion of buffer (`pr#53103 <https://github.com/ceph/ceph/pull/53103>`_, Patrick Donnelly)
* librados: make querying pools for selfmanaged snaps reliable (`pr#55024 <https://github.com/ceph/ceph/pull/55024>`_, Ilya Dryomov)
* librbd: Append one journal event per image request (`pr#54820 <https://github.com/ceph/ceph/pull/54820>`_, Joshua Baergen)
* librbd: don't report HOLE_UPDATED when diffing against a hole (`pr#54949 <https://github.com/ceph/ceph/pull/54949>`_, Ilya Dryomov)
* librbd: fix regressions in ObjectListSnapsRequest (`pr#54860 <https://github.com/ceph/ceph/pull/54860>`_, Ilya Dryomov)
* librbd: improve rbd_diff_iterate2() performance in fast-diff mode (`pr#55256 <https://github.com/ceph/ceph/pull/55256>`_, Ilya Dryomov)
* librbd: kick ExclusiveLock state machine on client being blocklisted when waiting for lock (`pr#53295 <https://github.com/ceph/ceph/pull/53295>`_, Ramana Raja)
* librbd: make CreatePrimaryRequest remove any unlinked mirror snapshots (`pr#53274 <https://github.com/ceph/ceph/pull/53274>`_, Ilya Dryomov)
* log: fix the formatting when dumping thread IDs (`pr#53465 <https://github.com/ceph/ceph/pull/53465>`_, Radoslaw Zarzynski)
* log: Make log_max_recent have an effect again (`pr#48311 <https://github.com/ceph/ceph/pull/48311>`_, Joshua Baergen)
* make-dist: don't use --continue option for wget (`pr#55090 <https://github.com/ceph/ceph/pull/55090>`_, Casey Bodley)
* make-dist: download liburing from kernel.io instead of github (`pr#53197 <https://github.com/ceph/ceph/pull/53197>`_, Laura Flores)
* MClientRequest: properly handle ceph_mds_request_head_legacy for ext_num_retry, ext_num_fwd, owner_uid, owner_gid (`pr#54410 <https://github.com/ceph/ceph/pull/54410>`_, Alexander Mikhalitsyn)
* mds,qa: some balancer debug messages (<=5) not printed when debug_mds is >=5 (`pr#53552 <https://github.com/ceph/ceph/pull/53552>`_, Patrick Donnelly)
* mds/Server: mark a cap acquisition throttle event in the request (`pr#53169 <https://github.com/ceph/ceph/pull/53169>`_, Leonid Usov)
* mds: acquire inode snaplock in open (`pr#53185 <https://github.com/ceph/ceph/pull/53185>`_, Patrick Donnelly)
* mds: add event for batching getattr/lookup (`pr#53556 <https://github.com/ceph/ceph/pull/53556>`_, Patrick Donnelly)
* mds: adjust pre_segments_size for MDLog when trimming segments for st… (`issue#59833 <http://tracker.ceph.com/issues/59833>`_, `pr#54033 <https://github.com/ceph/ceph/pull/54033>`_, Venky Shankar)
* mds: blocklist clients with "bloated" session metadata (`issue#61947 <http://tracker.ceph.com/issues/61947>`_, `issue#62873 <http://tracker.ceph.com/issues/62873>`_, `pr#53634 <https://github.com/ceph/ceph/pull/53634>`_, Venky Shankar)
* mds: drop locks and retry when lock set changes (`pr#53243 <https://github.com/ceph/ceph/pull/53243>`_, Patrick Donnelly)
* mds: ensure next replay is queued on req drop (`pr#54314 <https://github.com/ceph/ceph/pull/54314>`_, Patrick Donnelly)
* mds: fix deadlock between unlinking and linkmerge (`pr#53495 <https://github.com/ceph/ceph/pull/53495>`_, Xiubo Li)
* mds: fix issuing redundant reintegrate/migrate_stray requests (`pr#54517 <https://github.com/ceph/ceph/pull/54517>`_, Xiubo Li)
* mds: log message when exiting due to asok command (`pr#53550 <https://github.com/ceph/ceph/pull/53550>`_, Patrick Donnelly)
* mds: replacing bootstrap session only if handle client session message (`pr#53362 <https://github.com/ceph/ceph/pull/53362>`_, Mer Xuanyi)
* mds: report clients laggy due laggy OSDs only after checking any OSD is laggy (`pr#54120 <https://github.com/ceph/ceph/pull/54120>`_, Dhairya Parmar)
* mds: set the loner to true for LOCK_EXCL_XSYN (`pr#54912 <https://github.com/ceph/ceph/pull/54912>`_, Xiubo Li)
* mds: use variable g_ceph_context directly in MDSAuthCaps (`pr#52821 <https://github.com/ceph/ceph/pull/52821>`_, Rishabh Dave)
* mgr/BaseMgrModule: Optimize CPython Call in Finish Function (`pr#55109 <https://github.com/ceph/ceph/pull/55109>`_, Nitzan Mordechai)
* mgr/cephadm: Add "networks" parameter to orch apply rgw (`pr#53974 <https://github.com/ceph/ceph/pull/53974>`_, Teoman ONAY)
* mgr/cephadm: ceph orch add fails when ipv6 address is surrounded by square brackets (`pr#53978 <https://github.com/ceph/ceph/pull/53978>`_, Teoman ONAY)
* mgr/dashboard: add 'omit_usage' query param to dashboard api 'get rbd' endpoint (`pr#54192 <https://github.com/ceph/ceph/pull/54192>`_, Cory Snyder)
* mgr/dashboard: allow tls 1.2 with a config option (`pr#53781 <https://github.com/ceph/ceph/pull/53781>`_, Nizamudeen A)
* mgr/dashboard: Consider null values as zero in grafana panels (`pr#54542 <https://github.com/ceph/ceph/pull/54542>`_, Aashish Sharma)
* mgr/dashboard: fix CephPGImbalance alert (`pr#49478 <https://github.com/ceph/ceph/pull/49478>`_, Aashish Sharma)
* mgr/dashboard: Fix CephPoolGrowthWarning alert (`pr#49477 <https://github.com/ceph/ceph/pull/49477>`_, Aashish Sharma)
* mgr/dashboard: fix constraints.txt (`pr#54652 <https://github.com/ceph/ceph/pull/54652>`_, Ernesto Puerta)
* mgr/dashboard: fix rgw page issues when hostname not resolvable (`pr#53215 <https://github.com/ceph/ceph/pull/53215>`_, Nizamudeen A)
* mgr/dashboard: set CORS header for unauthorized access (`pr#53202 <https://github.com/ceph/ceph/pull/53202>`_, Nizamudeen A)
* mgr/prometheus: avoid duplicates and deleted entries for rbd_stats_pools (`pr#48524 <https://github.com/ceph/ceph/pull/48524>`_, Avan Thakkar)
* mgr/prometheus: change pg_repaired_objects name to pool_repaired_objects (`pr#48439 <https://github.com/ceph/ceph/pull/48439>`_, Pere Diaz Bou)
* mgr/prometheus: fix pool_objects_repaired and daemon_health_metrics format (`pr#51692 <https://github.com/ceph/ceph/pull/51692>`_, banuchka)
* mgr/rbd_support: fix recursive locking on CreateSnapshotRequests lock (`pr#54293 <https://github.com/ceph/ceph/pull/54293>`_, Ramana Raja)
* mgr/snap-schedule: use the right way to check the result returned by… (`pr#53355 <https://github.com/ceph/ceph/pull/53355>`_, Mer Xuanyi)
* mgr/snap_schedule: allow retention spec 'n' to be user defined (`pr#52750 <https://github.com/ceph/ceph/pull/52750>`_, Milind Changire, Jakob Haufe)
* mgr/volumes: Fix pending_subvolume_deletions in volume info (`pr#53574 <https://github.com/ceph/ceph/pull/53574>`_, Kotresh HR)
* mgr: Add one finisher thread per module (`pr#51045 <https://github.com/ceph/ceph/pull/51045>`_, Kotresh HR, Patrick Donnelly)
* mgr: add throttle policy for DaemonServer (`pr#54013 <https://github.com/ceph/ceph/pull/54013>`_, ericqzhao)
* mgr: don't dump global config holding gil (`pr#50194 <https://github.com/ceph/ceph/pull/50194>`_, Mykola Golub)
* mgr: fix a race condition in DaemonServer::handle_report() (`pr#52993 <https://github.com/ceph/ceph/pull/52993>`_, Radoslaw Zarzynski)
* mgr: register OSDs in ms_handle_accept (`pr#53189 <https://github.com/ceph/ceph/pull/53189>`_, Patrick Donnelly)
* mgr: remove out&down osd from mgr daemons (`pr#54553 <https://github.com/ceph/ceph/pull/54553>`_, shimin)
* mon/ConfigMonitor: Show localized name in "config dump --format json" output (`pr#53984 <https://github.com/ceph/ceph/pull/53984>`_, Sridhar Seshasayee)
* mon/MonClient: resurrect original client_mount_timeout handling (`pr#52533 <https://github.com/ceph/ceph/pull/52533>`_, Ilya Dryomov)
* mon/Monitor.cc: exit function if !osdmon()->is_writeable() && mon/OSDMonitor: Added extra check before mon.go_recovery_stretch_mode() (`pr#51414 <https://github.com/ceph/ceph/pull/51414>`_, Kamoltat)
* mon/Monitor: during shutdown don't accept new authentication and crea… (`pr#55113 <https://github.com/ceph/ceph/pull/55113>`_, Nitzan Mordechai)
* mon: add exception handling to ceph health mute (`pr#55118 <https://github.com/ceph/ceph/pull/55118>`_, Daniel Radjenovic)
* mon: add proxy to cache tier options (`pr#50552 <https://github.com/ceph/ceph/pull/50552>`_, tan changzhi)
* mon: fix health store size growing infinitely (`pr#55472 <https://github.com/ceph/ceph/pull/55472>`_, Wei Wang)
* mon: fix iterator mishandling in PGMap::apply_incremental (`pr#52555 <https://github.com/ceph/ceph/pull/52555>`_, Oliver Schmidt)
* mon: fix mds metadata lost in one case (`pr#54318 <https://github.com/ceph/ceph/pull/54318>`_, shimin)
* msg/async: initialize worker in RDMAStack::create_worker() and drop Stack::num_workers (`pr#55443 <https://github.com/ceph/ceph/pull/55443>`_, Kefu Chai)
* msg/AsyncMessenger: re-evaluate the stop condition when woken up in 'wait()' (`pr#53716 <https://github.com/ceph/ceph/pull/53716>`_, Leonid Usov)
* nofail option in fstab not supported (`pr#52987 <https://github.com/ceph/ceph/pull/52987>`_, Leonid Usov)
* os/bluestore: don't require bluestore_db_block_size when attaching new (`pr#52948 <https://github.com/ceph/ceph/pull/52948>`_, Igor Fedotov)
* os/bluestore: get rid off resulting lba alignment in allocators (`pr#54434 <https://github.com/ceph/ceph/pull/54434>`_, Igor Fedotov)
* osd,bluestore: gracefully handle a failure during meta collection load (`pr#53135 <https://github.com/ceph/ceph/pull/53135>`_, Igor Fedotov)
* osd/OpRequest: Add detail description for delayed op in osd log file (`pr#53693 <https://github.com/ceph/ceph/pull/53693>`_, Yite Gu)
* osd/OSD: introduce reset_purged_snaps_last (`pr#53970 <https://github.com/ceph/ceph/pull/53970>`_, Matan Breizman)
* osd/OSDMap: Check for uneven weights & != 2 buckets post stretch mode (`pr#52459 <https://github.com/ceph/ceph/pull/52459>`_, Kamoltat)
* osd/scrub: Fix scrub starts messages spamming the cluster log (`pr#53430 <https://github.com/ceph/ceph/pull/53430>`_, Prashant D)
* osd: don't require RWEXCL lock for stat+write ops (`pr#54593 <https://github.com/ceph/ceph/pull/54593>`_, Alice Zhao)
* osd: ensure async recovery does not drop a pg below min_size (`pr#54548 <https://github.com/ceph/ceph/pull/54548>`_, Samuel Just)
* osd: fix shard-threads cannot wakeup bug (`pr#51262 <https://github.com/ceph/ceph/pull/51262>`_, Jianwei Zhang)
* osd: fix use-after-move in build_incremental_map_msg() (`pr#54268 <https://github.com/ceph/ceph/pull/54268>`_, Ronen Friedman)
* osd: log the number of extents for sparse read (`pr#54604 <https://github.com/ceph/ceph/pull/54604>`_, Xiubo Li)
* pacifc: Revert "mgr/dashboard: unselect rows in datatables" (`pr#55415 <https://github.com/ceph/ceph/pull/55415>`_, Nizamudeen A)
* pybind/mgr/autoscaler: Donot show NEW PG_NUM value if autoscaler is not on (`pr#53464 <https://github.com/ceph/ceph/pull/53464>`_, Prashant D)
* pybind/mgr/mgr_util: fix to_pretty_timedelta() (`pr#51243 <https://github.com/ceph/ceph/pull/51243>`_, Sage Weil)
* pybind/mgr/volumes: log mutex locks to help debug deadlocks (`pr#53916 <https://github.com/ceph/ceph/pull/53916>`_, Kotresh HR)
* pybind/mgr: ceph osd status crash with ZeroDivisionError (`pr#46696 <https://github.com/ceph/ceph/pull/46696>`_, Nitzan Mordechai, Kefu Chai)
* pybind/rados: don't close watch in dealloc if already closed (`pr#51259 <https://github.com/ceph/ceph/pull/51259>`_, Tim Serong)
* pybind/rados: fix missed changes for PEP484 style type annotations (`pr#54361 <https://github.com/ceph/ceph/pull/54361>`_, Igor Fedotov)
* pybind/rbd: don't produce info on errors in aio_mirror_image_get_info() (`pr#54053 <https://github.com/ceph/ceph/pull/54053>`_, Ilya Dryomov)
* python-common/drive_group: handle fields outside of 'spec' even when 'spec' is provided (`pr#52413 <https://github.com/ceph/ceph/pull/52413>`_, Adam King)
* python-common/drive_selection: lower log level of limit policy message (`pr#52412 <https://github.com/ceph/ceph/pull/52412>`_, Adam King)
* qa/distros: backport update from rhel 8.4 -> 8.6 (`pr#54901 <https://github.com/ceph/ceph/pull/54901>`_, Casey Bodley, David Galloway)
* qa/suites/krbd: stress test for recovering from watch errors (`pr#53784 <https://github.com/ceph/ceph/pull/53784>`_, Ilya Dryomov)
* qa/suites/orch: whitelist warnings that are expected in test environments (`pr#55523 <https://github.com/ceph/ceph/pull/55523>`_, Laura Flores)
* qa/suites/rbd: add test to check rbd_support module recovery (`pr#54294 <https://github.com/ceph/ceph/pull/54294>`_, Ramana Raja)
* qa/suites/upgrade/pacific-p2p: run librbd python API tests from pacific tip (`pr#55418 <https://github.com/ceph/ceph/pull/55418>`_, Yuri Weinstein)
* qa/suites/upgrade/pacific-p2p: skip TestClsRbd.mirror_snapshot test (`pr#53204 <https://github.com/ceph/ceph/pull/53204>`_, Ilya Dryomov)
* qa/suites: added more whitelisting + fix typo (`pr#55717 <https://github.com/ceph/ceph/pull/55717>`_, Kamoltat)
* qa/tasks/cephadm: enable mon_cluster_log_to_file (`pr#55429 <https://github.com/ceph/ceph/pull/55429>`_, Dan van der Ster)
* qa/upgrade: disable a failing ceph_test_cls_cmpomap test case (`pr#55519 <https://github.com/ceph/ceph/pull/55519>`_, Casey Bodley)
* qa/upgrade: use ragweed branch for starting ceph release (`pr#55382 <https://github.com/ceph/ceph/pull/55382>`_, Casey Bodley)
* qa/workunits/rbd/cli_generic.sh: narrow race window when checking that rbd_support module command fails after blocklisting the module's client (`pr#54771 <https://github.com/ceph/ceph/pull/54771>`_, Ramana Raja)
* qa: assign file system affinity for replaced MDS (`issue#61764 <http://tracker.ceph.com/issues/61764>`_, `pr#54039 <https://github.com/ceph/ceph/pull/54039>`_, Venky Shankar)
* qa: ignore expected cluster warning from damage tests (`pr#53486 <https://github.com/ceph/ceph/pull/53486>`_, Patrick Donnelly)
* qa: lengthen shutdown timeout for thrashed MDS (`pr#53555 <https://github.com/ceph/ceph/pull/53555>`_, Patrick Donnelly)
* qa: pass arg as list to fix test case failure (`pr#52763 <https://github.com/ceph/ceph/pull/52763>`_, Dhairya Parmar)
* qa: remove duplicate import (`pr#53447 <https://github.com/ceph/ceph/pull/53447>`_, Patrick Donnelly)
* qa: run kernel_untar_build with newer tarball (`pr#54713 <https://github.com/ceph/ceph/pull/54713>`_, Milind Changire)
* qa: wait for file to have correct size (`pr#52744 <https://github.com/ceph/ceph/pull/52744>`_, Patrick Donnelly)
* rados: build minimally when "WITH_MGR" is off (`pr#51250 <https://github.com/ceph/ceph/pull/51250>`_, J. Eric Ivancich)
* rados: increase osd_max_write_op_reply_len default to 64 bytes (`pr#53470 <https://github.com/ceph/ceph/pull/53470>`_, Matt Benjamin)
* RadosGW API: incorrect bucket quota in response to HEAD /{bucket}/?usage (`pr#53439 <https://github.com/ceph/ceph/pull/53439>`_, shreyanshjain7174)
* radosgw-admin: allow 'bi purge' to delete index if entrypoint doesn't exist (`pr#54010 <https://github.com/ceph/ceph/pull/54010>`_, Casey Bodley)
* radosgw-admin: don't crash on --placement-id without --storage-class (`pr#53474 <https://github.com/ceph/ceph/pull/53474>`_, Casey Bodley)
* radosgw-admin: fix segfault on pipe modify without source/dest zone specified (`pr#51256 <https://github.com/ceph/ceph/pull/51256>`_, caisan)
* rbd-nbd: fix stuck with disable request (`pr#54256 <https://github.com/ceph/ceph/pull/54256>`_, Prasanna Kumar Kalever)
* rgw - Fix NoSuchTagSet error (`pr#50533 <https://github.com/ceph/ceph/pull/50533>`_, Daniel Gryniewicz)
* rgw/auth: ignoring signatures for HTTP OPTIONS calls (`pr#55550 <https://github.com/ceph/ceph/pull/55550>`_, Tobias Urdin)
* rgw/beast: add max_header_size option with 16k default, up from 4k (`pr#52113 <https://github.com/ceph/ceph/pull/52113>`_, Casey Bodley)
* rgw/keystone: EC2Engine uses reject() for ERR_SIGNATURE_NO_MATCH (`pr#53764 <https://github.com/ceph/ceph/pull/53764>`_, Casey Bodley)
* rgw/notification: remove non x-amz-meta-\* attributes from bucket notifications (`pr#53376 <https://github.com/ceph/ceph/pull/53376>`_, Juan Zhu)
* rgw/putobj: RadosWriter uses part head object for multipart parts (`pr#55586 <https://github.com/ceph/ceph/pull/55586>`_, Casey Bodley)
* rgw/s3: ListObjectsV2 returns correct object owners (`pr#54160 <https://github.com/ceph/ceph/pull/54160>`_, Casey Bodley)
* rgw/sts: AssumeRole no longer writes to user metadata (`pr#52051 <https://github.com/ceph/ceph/pull/52051>`_, Casey Bodley)
* rgw/sts: code for returning an error when an IAM policy (`pr#44462 <https://github.com/ceph/ceph/pull/44462>`_, Pritha Srivastava)
* rgw/sts: code to fetch certs using .well-known/openid-configuration URL (`pr#44464 <https://github.com/ceph/ceph/pull/44464>`_, Pritha Srivastava)
* rgw/sts: createbucket op should take session_policies into account (`pr#44476 <https://github.com/ceph/ceph/pull/44476>`_, Pritha Srivastava)
* rgw/sts: fix read_obj_policy permission evaluation (`pr#44471 <https://github.com/ceph/ceph/pull/44471>`_, Pritha Srivastava)
* rgw/sts: fixes getsessiontoken authenticated with LDAP (`pr#44463 <https://github.com/ceph/ceph/pull/44463>`_, Pritha Srivastava)
* rgw/swift: check position of first slash in slo manifest files (`pr#51600 <https://github.com/ceph/ceph/pull/51600>`_, Marcio Roberto Starke)
* rgw/sync-policy: Correct "sync status" & "sync group" commands (`pr#53410 <https://github.com/ceph/ceph/pull/53410>`_, Soumya Koduri)
* rgw: 'bucket check' deletes index of multipart meta when its pending_map is nonempty (`pr#54016 <https://github.com/ceph/ceph/pull/54016>`_, Huber-ming)
* rgw: add radosgw-admin bucket check olh/unlinked commands (`pr#53808 <https://github.com/ceph/ceph/pull/53808>`_, Cory Snyder)
* rgw: Avoid segfault when OPA authz is enabled (`pr#46106 <https://github.com/ceph/ceph/pull/46106>`_, Benoît Knecht)
* rgw: beast frontend checks for local_endpoint() errors (`pr#54167 <https://github.com/ceph/ceph/pull/54167>`_, Casey Bodley)
* rgw: Drain async_processor request queue during shutdown (`pr#53472 <https://github.com/ceph/ceph/pull/53472>`_, Soumya Koduri)
* rgw: fix 2 null versionID after convert_plain_entry_to_versioned (`pr#53400 <https://github.com/ceph/ceph/pull/53400>`_, rui ma, zhuo li)
* rgw: Fix Browser POST content-length-range min value (`pr#52936 <https://github.com/ceph/ceph/pull/52936>`_, Robin H. Johnson)
* rgw: fix FP error when calculating enteries per bi shard (`pr#53593 <https://github.com/ceph/ceph/pull/53593>`_, J. Eric Ivancich)
* rgw: fix rgw cache invalidation after unregister_watch() error (`pr#54014 <https://github.com/ceph/ceph/pull/54014>`_, lichaochao)
* rgw: fix SignatureDoesNotMatch when extra headers start with 'x-amz' (`pr#53772 <https://github.com/ceph/ceph/pull/53772>`_, rui ma)
* rgw: Fix truncated ListBuckets response (`pr#49526 <https://github.com/ceph/ceph/pull/49526>`_, Joshua Baergen)
* rgw: fix unwatch crash at radosgw startup (`pr#53759 <https://github.com/ceph/ceph/pull/53759>`_, lichaochao)
* rgw: fix UploadPartCopy error code when src object not exist and src bucket not exist (`pr#53356 <https://github.com/ceph/ceph/pull/53356>`_, yuliyang)
* rgw: handle http options CORS with v4 auth (`pr#53416 <https://github.com/ceph/ceph/pull/53416>`_, Tobias Urdin)
* rgw: improve buffer list utilization in the chunkupload scenario (`pr#53775 <https://github.com/ceph/ceph/pull/53775>`_, liubingrun)
* rgw: multisite data log flag not used (`pr#52055 <https://github.com/ceph/ceph/pull/52055>`_, J. Eric Ivancich)
* rgw: pick http_date in case of http_x_amz_date absence (`pr#53443 <https://github.com/ceph/ceph/pull/53443>`_, Seena Fallah, Mohamed Awnallah)
* rgw: prevent spurious/lost notifications in the index completion thread (`pr#49093 <https://github.com/ceph/ceph/pull/49093>`_, Casey Bodley, Yuval Lifshitz)
* rgw: retry metadata cache notifications with INVALIDATE_OBJ (`pr#52797 <https://github.com/ceph/ceph/pull/52797>`_, Casey Bodley)
* rgw: s3 object lock avoids overflow in retention date (`pr#52605 <https://github.com/ceph/ceph/pull/52605>`_, Casey Bodley)
* rgw: s3website doesn't prefetch for web_dir() check (`pr#53769 <https://github.com/ceph/ceph/pull/53769>`_, Casey Bodley)
* rgw: set keys from from master zone on admin api user create (`pr#51602 <https://github.com/ceph/ceph/pull/51602>`_, Ali Maredia)
* rgw: Solving the issue of not populating etag in Multipart upload result (`pr#51445 <https://github.com/ceph/ceph/pull/51445>`_, Ali Masarwa)
* rgw: swift : check for valid key in POST forms (`pr#52729 <https://github.com/ceph/ceph/pull/52729>`_, Abhishek Lekshmanan)
* rgw: Update "CEPH_RGW_DIR_SUGGEST_LOG_OP" for remove entries (`pr#50540 <https://github.com/ceph/ceph/pull/50540>`_, Soumya Koduri)
* rgw: use unique_ptr for flat_map emplace in BucketTrimWatche (`pr#52996 <https://github.com/ceph/ceph/pull/52996>`_, Vedansh Bhartia)
* rgwlc: prevent lc for one bucket from exceeding time budget (`pr#53562 <https://github.com/ceph/ceph/pull/53562>`_, Matt Benjamin)
* test/lazy-omap-stats: Various enhancements (`pr#50518 <https://github.com/ceph/ceph/pull/50518>`_, Brad Hubbard)
* test/librbd: avoid config-related crashes in DiscardWithPruneWriteOverlap (`pr#54859 <https://github.com/ceph/ceph/pull/54859>`_, Ilya Dryomov)
* test/store_test: adjust physical extents to inject error against (`pr#54782 <https://github.com/ceph/ceph/pull/54782>`_, Igor Fedotov)
* tools/ceph_objectstore_tool: action_on_all_objects_in_pg to skip pgmeta (`pr#54691 <https://github.com/ceph/ceph/pull/54691>`_, Matan Breizman)
* tools/ceph_objectstore_tool: Support get/set/superblock (`pr#55013 <https://github.com/ceph/ceph/pull/55013>`_, Matan Breizman)
* tools/osdmaptool: fix possible segfaults when there are down osds (`pr#52203 <https://github.com/ceph/ceph/pull/52203>`_, Mykola Golub)
* Tools/rados: Improve Error Messaging for Object Name Resolution (`pr#55111 <https://github.com/ceph/ceph/pull/55111>`_, Nitzan Mordechai)
* vstart_runner: maintain log level when --debug is passed (`pr#52977 <https://github.com/ceph/ceph/pull/52977>`_, Rishabh Dave)
* vstart_runner: use FileNotFoundError when os.stat() fails (`pr#52978 <https://github.com/ceph/ceph/pull/52978>`_, Rishabh Dave)
* win32_deps_build.sh: change Boost URL (`pr#55086 <https://github.com/ceph/ceph/pull/55086>`_, Lucian Petrut)

v16.2.14 Pacific
================

This is the fourteenth backport release in the Pacific series.

Notable Changes
---------------

* CephFS: After recovering a Ceph File System post following the disaster
  recovery procedure, the recovered files under `lost+found` directory can now
  be deleted.

* `ceph mgr dump` command now displays the name of the mgr module that
  registered a RADOS client in the `name` field added to elements of the
  `active_clients` array. Previously, only the address of a module's RADOS
  client was shown in the `active_clients` array.

Changelog
---------

* backport PR #39607 (`pr#51344 <https://github.com/ceph/ceph/pull/51344>`_, Rishabh Dave)
* blk/kernel: Fix error code mapping in KernelDevice::read (`pr#49263 <https://github.com/ceph/ceph/pull/49263>`_, Joshua Baergen)
* blk/KernelDevice: Modify the rotational and discard check log message (`pr#50322 <https://github.com/ceph/ceph/pull/50322>`_, Vikhyat Umrao)
* build: Remove ceph-libboost\* packages in install-deps (`pr#52790 <https://github.com/ceph/ceph/pull/52790>`_, Nizamudeen A, Adam Emerson)
* ceph-volume: fix a bug in `get_lvm_fast_allocs()` (batch) (`pr#52063 <https://github.com/ceph/ceph/pull/52063>`_, Guillaume Abrioux)
* ceph-volume: fix batch refactor issue (`pr#51207 <https://github.com/ceph/ceph/pull/51207>`_, Guillaume Abrioux)
* ceph-volume: fix drive-group issue that expects the batch_args to be a string (`pr#51209 <https://github.com/ceph/ceph/pull/51209>`_, Mohan Sharma)
* ceph-volume: quick fix in zap.py (`pr#51196 <https://github.com/ceph/ceph/pull/51196>`_, Guillaume Abrioux)
* ceph-volume: set lvm membership for mpath type devices (`pr#52080 <https://github.com/ceph/ceph/pull/52080>`_, Guillaume Abrioux)
* ceph_test_rados_api_watch_notify: extend Watch3Timeout test (`pr#51261 <https://github.com/ceph/ceph/pull/51261>`_, Sage Weil)
* ceph_volume: support encrypted volumes for lvm new-db/new-wal/migrate commands (`pr#52873 <https://github.com/ceph/ceph/pull/52873>`_, Igor Fedotov)
* cephadm: eliminate duplication of sections (`pr#51433 <https://github.com/ceph/ceph/pull/51433>`_, Rongqi Sun)
* cephadm: mount host /etc/hosts for daemon containers in podman deployments (`pr#51174 <https://github.com/ceph/ceph/pull/51174>`_, Adam King)
* cephadm: reschedule haproxy from an offline host (`pr#51214 <https://github.com/ceph/ceph/pull/51214>`_, Michael Fritch)
* cephadm: using ip instead of short hostname for prometheus urls (`pr#51212 <https://github.com/ceph/ceph/pull/51212>`_, Redouane Kachach)
* cephfs-top: check the minimum compatible python version (`pr#51353 <https://github.com/ceph/ceph/pull/51353>`_, Jos Collin)
* cephfs-top: dump values to stdout and -d [--delay] option fix (`pr#50715 <https://github.com/ceph/ceph/pull/50715>`_, Jos Collin, Neeraj Pratap Singh, wangxinyu, Rishabh Dave)
* cephfs-top: navigate to home screen when no fs (`pr#50737 <https://github.com/ceph/ceph/pull/50737>`_, Jos Collin)
* cephfs-top: Some fixes in `choose_field()` for sorting (`pr#50596 <https://github.com/ceph/ceph/pull/50596>`_, Neeraj Pratap Singh)
* client: clear the suid/sgid in fallocate path (`pr#50988 <https://github.com/ceph/ceph/pull/50988>`_, Lucian Petrut, Xiubo Li, Sven Anderson)
* client: do not dump mds twice in Inode::dump() (`pr#51247 <https://github.com/ceph/ceph/pull/51247>`_, Xue Yantao)
* client: do not send metrics until the MDS rank is ready (`pr#52500 <https://github.com/ceph/ceph/pull/52500>`_, Xiubo Li)
* client: force sending cap revoke ack always (`pr#52506 <https://github.com/ceph/ceph/pull/52506>`_, Xiubo Li)
* client: only wait for write MDS OPs when unmounting (`pr#52304 <https://github.com/ceph/ceph/pull/52304>`_, Xiubo Li)
* client: trigger to flush the buffer when making snapshot (`pr#52499 <https://github.com/ceph/ceph/pull/52499>`_, Xiubo Li)
* client: use deep-copy when setting permission during make_request (`pr#51487 <https://github.com/ceph/ceph/pull/51487>`_, Mer Xuanyi)
* client: wait rename to finish (`pr#52505 <https://github.com/ceph/ceph/pull/52505>`_, Xiubo Li)
* cls/queue: use larger read chunks in queue_list_entries (`pr#49903 <https://github.com/ceph/ceph/pull/49903>`_, Igor Fedotov)
* common/crc32c_aarch64: fix crc32c unittest failed on aarch64 (`pr#51315 <https://github.com/ceph/ceph/pull/51315>`_, luo rixin)
* common/TrackedOp: fix osd reboot optracker coredump (`pr#51249 <https://github.com/ceph/ceph/pull/51249>`_, yaohui.zhou)
* common: notify all when max backlog reached in OutputDataSocket (`pr#47232 <https://github.com/ceph/ceph/pull/47232>`_, Shu Yu)
* common: Use double instead of long double to improve performance (`pr#51316 <https://github.com/ceph/ceph/pull/51316>`_, Chunsong Feng, luo rixin)
* Consider setting "bulk" autoscale pool flag when automatically creating a data pool for CephFS (`pr#52900 <https://github.com/ceph/ceph/pull/52900>`_, Leonid Usov)
* debian: install cephfs-mirror systemd unit files and man page (`pr#52075 <https://github.com/ceph/ceph/pull/52075>`_, Jos Collin)
* do not evict clients if OSDs are laggy (`pr#52270 <https://github.com/ceph/ceph/pull/52270>`_, Laura Flores, Dhairya Parmar)
* doc/cephadm: Revert "doc/cephadm: update about disabling logging to journald for quincy" (`pr#51882 <https://github.com/ceph/ceph/pull/51882>`_, Adam King)
* doc/cephfs: edit fs-volumes.rst (1 of x) (`pr#51467 <https://github.com/ceph/ceph/pull/51467>`_, Zac Dover)
* doc/cephfs: explain cephfs data and metadata set (`pr#51237 <https://github.com/ceph/ceph/pull/51237>`_, Zac Dover)
* doc/cephfs: fix prompts in fs-volumes.rst (`pr#51436 <https://github.com/ceph/ceph/pull/51436>`_, Zac Dover)
* doc/cephfs: line-edit "Mirroring Module" (`pr#51544 <https://github.com/ceph/ceph/pull/51544>`_, Zac Dover)
* doc/cephfs: rectify prompts in fs-volumes.rst (`pr#51460 <https://github.com/ceph/ceph/pull/51460>`_, Zac Dover)
* doc/cephfs: repairing inaccessible FSes (`pr#51373 <https://github.com/ceph/ceph/pull/51373>`_, Zac Dover)
* doc/dev/encoding.txt: update per std::optional (`pr#51399 <https://github.com/ceph/ceph/pull/51399>`_, Radoslaw Zarzynski)
* doc/glossary: update bluestore entry (`pr#51695 <https://github.com/ceph/ceph/pull/51695>`_, Zac Dover)
* doc/mgr: edit "leaderboard" in telemetry.rst (`pr#51722 <https://github.com/ceph/ceph/pull/51722>`_, Zac Dover)
* doc/mgr: update prompts in prometheus.rst (`pr#51311 <https://github.com/ceph/ceph/pull/51311>`_, Zac Dover)
* doc/rados/operations: Acting Set question (`pr#51741 <https://github.com/ceph/ceph/pull/51741>`_, Zac Dover)
* doc/rados/operations: Fix erasure-code-jerasure.rst fix (`pr#51744 <https://github.com/ceph/ceph/pull/51744>`_, Anthony D'Atri)
* doc/rados/ops: edit user-management.rst (3 of x) (`pr#51241 <https://github.com/ceph/ceph/pull/51241>`_, Zac Dover)
* doc/rados: edit balancer.rst (`pr#51826 <https://github.com/ceph/ceph/pull/51826>`_, Zac Dover)
* doc/rados: edit bluestore-config-ref.rst (1 of x) (`pr#51791 <https://github.com/ceph/ceph/pull/51791>`_, Zac Dover)
* doc/rados: edit bluestore-config-ref.rst (2 of x) (`pr#51795 <https://github.com/ceph/ceph/pull/51795>`_, Zac Dover)
* doc/rados: edit data-placement.rst (`pr#51597 <https://github.com/ceph/ceph/pull/51597>`_, Zac Dover)
* doc/rados: edit devices.rst (`pr#51479 <https://github.com/ceph/ceph/pull/51479>`_, Zac Dover)
* doc/rados: edit filestore-config-ref.rst (`pr#51753 <https://github.com/ceph/ceph/pull/51753>`_, Zac Dover)
* doc/rados: edit stretch-mode procedure (`pr#51291 <https://github.com/ceph/ceph/pull/51291>`_, Zac Dover)
* doc/rados: edit stretch-mode.rst (`pr#51339 <https://github.com/ceph/ceph/pull/51339>`_, Zac Dover)
* doc/rados: edit stretch-mode.rst (`pr#51304 <https://github.com/ceph/ceph/pull/51304>`_, Zac Dover)
* doc/rados: edit user-management (2 of x) (`pr#51157 <https://github.com/ceph/ceph/pull/51157>`_, Zac Dover)
* doc/rados: fix link in common.rst (`pr#51757 <https://github.com/ceph/ceph/pull/51757>`_, Zac Dover)
* doc/rados: line-edit devices.rst (`pr#51578 <https://github.com/ceph/ceph/pull/51578>`_, Zac Dover)
* doc/rados: m-config-ref: edit "background" (`pr#51274 <https://github.com/ceph/ceph/pull/51274>`_, Zac Dover)
* doc/rados: stretch-mode.rst (other commands) (`pr#51391 <https://github.com/ceph/ceph/pull/51391>`_, Zac Dover)
* doc/rados: stretch-mode: stretch cluster issues (`pr#51379 <https://github.com/ceph/ceph/pull/51379>`_, Zac Dover)
* doc/radosgw: explain multisite dynamic sharding (`pr#51587 <https://github.com/ceph/ceph/pull/51587>`_, Zac Dover)
* doc/radosgw: rabbitmq - push-endpoint edit (`pr#51307 <https://github.com/ceph/ceph/pull/51307>`_, Zac Dover)
* doc/start/os-recommendations: drop 4.14 kernel and reword guidance (`pr#51491 <https://github.com/ceph/ceph/pull/51491>`_, Ilya Dryomov)
* doc/start: edit first 150 lines of documenting-ceph (`pr#51183 <https://github.com/ceph/ceph/pull/51183>`_, Zac Dover)
* doc/start: fix "Planet Ceph" link (`pr#51421 <https://github.com/ceph/ceph/pull/51421>`_, Zac Dover)
* doc/start: KRBD feature flag support note (`pr#51504 <https://github.com/ceph/ceph/pull/51504>`_, Zac Dover)
* doc/start: rewrite intro paragraph (`pr#51222 <https://github.com/ceph/ceph/pull/51222>`_, Zac Dover)
* doc: add link to "documenting ceph" to index.rst (`pr#51471 <https://github.com/ceph/ceph/pull/51471>`_, Zac Dover)
* doc: Add missing `ceph` command in documentation section `REPLACING A… (`pr#51621 <https://github.com/ceph/ceph/pull/51621>`_, Alexander Proschek)
* doc: deprecate the cache tiering (`pr#51654 <https://github.com/ceph/ceph/pull/51654>`_, Radosław Zarzyński)
* doc: document the relevance of mds_namespace mount option (`pr#49688 <https://github.com/ceph/ceph/pull/49688>`_, Jos Collin)
* doc: explain cephfs mirroring `peer_add` step in detail (`pr#51522 <https://github.com/ceph/ceph/pull/51522>`_, Venky Shankar)
* doc: Update jerasure.org references (`pr#51727 <https://github.com/ceph/ceph/pull/51727>`_, Anthony D'Atri)
* doc: update multisite doc (`pr#51402 <https://github.com/ceph/ceph/pull/51402>`_, parth-gr)
* doc: Use `ceph osd crush tree` command to display weight set weights (`pr#51351 <https://github.com/ceph/ceph/pull/51351>`_, James Lakin)
* kv/RocksDBStore: Add CompactOnDeletion support (`pr#50894 <https://github.com/ceph/ceph/pull/50894>`_, Radoslaw Zarzynski, Mark Nelson)
* kv/RocksDBStore: cumulative backport for rm_range_keys and around (`pr#50637 <https://github.com/ceph/ceph/pull/50637>`_, Igor Fedotov)
* kv/RocksDBStore: don't use real wholespace iterator for prefixed access (`pr#50496 <https://github.com/ceph/ceph/pull/50496>`_, Igor Fedotov)
* librados: aio operate functions can set times (`pr#52117 <https://github.com/ceph/ceph/pull/52117>`_, Casey Bodley)
* librbd/managed_lock/GetLockerRequest: Fix no valid lockers case (`pr#52287 <https://github.com/ceph/ceph/pull/52287>`_, Ilya Dryomov, Matan Breizman)
* librbd: avoid decrementing iterator before first element (`pr#51856 <https://github.com/ceph/ceph/pull/51856>`_, Lucian Petrut)
* librbd: avoid object map corruption in snapshots taken under I/O (`pr#52285 <https://github.com/ceph/ceph/pull/52285>`_, Ilya Dryomov)
* librbd: don't wait for a watch in send_acquire_lock() if client is blocklisted (`pr#50926 <https://github.com/ceph/ceph/pull/50926>`_, Ilya Dryomov, Christopher Hoffman)
* librbd: localize snap_remove op for mirror snapshots (`pr#51431 <https://github.com/ceph/ceph/pull/51431>`_, Christopher Hoffman)
* librbd: remove previous incomplete primary snapshot after successfully creating a new one (`pr#51429 <https://github.com/ceph/ceph/pull/51429>`_, Ilya Dryomov, Prasanna Kumar Kalever)
* log: writes to stderr (pipe) may not be atomic (`pr#50778 <https://github.com/ceph/ceph/pull/50778>`_, Lucian Petrut, Patrick Donnelly)
* MDS imported_inodes metric is not updated (`pr#51699 <https://github.com/ceph/ceph/pull/51699>`_, Yongseok Oh)
* mds: adjust cap acquisition throttles (`pr#52974 <https://github.com/ceph/ceph/pull/52974>`_, Patrick Donnelly)
* mds: allow unlink from lost+found directory (`issue#59569 <http://tracker.ceph.com/issues/59569>`_, `pr#51687 <https://github.com/ceph/ceph/pull/51687>`_, Venky Shankar)
* mds: display sane hex value (0x0) for empty feature bit (`pr#52125 <https://github.com/ceph/ceph/pull/52125>`_, Jos Collin)
* mds: do not send split_realms for CEPH_SNAP_OP_UPDATE msg (`pr#52848 <https://github.com/ceph/ceph/pull/52848>`_, Xiubo Li)
* mds: do not take the ino which has been used (`pr#51508 <https://github.com/ceph/ceph/pull/51508>`_, Xiubo Li)
* mds: fix cpu_profiler asok crash (`pr#52979 <https://github.com/ceph/ceph/pull/52979>`_, liu shi)
* mds: fix stray evaluation using scrub and introduce new option (`pr#50814 <https://github.com/ceph/ceph/pull/50814>`_, Dhairya Parmar)
* mds: Fix the linkmerge assert check (`pr#52726 <https://github.com/ceph/ceph/pull/52726>`_, Kotresh HR)
* mds: force replay sessionmap version (`pr#50725 <https://github.com/ceph/ceph/pull/50725>`_, Xiubo Li)
* mds: make num_fwd and num_retry to __u32 (`pr#50733 <https://github.com/ceph/ceph/pull/50733>`_, Xiubo Li)
* mds: MDLog::_recovery_thread: handle the errors gracefully (`pr#52513 <https://github.com/ceph/ceph/pull/52513>`_, Jos Collin)
* mds: rdlock_path_xlock_dentry supports returning auth target inode (`pr#51609 <https://github.com/ceph/ceph/pull/51609>`_, Zhansong Gao)
* mds: record and dump last tid for trimming completed requests (or flushes) (`issue#57985 <http://tracker.ceph.com/issues/57985>`_, `pr#50811 <https://github.com/ceph/ceph/pull/50811>`_, Venky Shankar)
* mds: skip forwarding request if the session were removed (`pr#52844 <https://github.com/ceph/ceph/pull/52844>`_, Xiubo Li)
* mds: update mdlog perf counters during replay (`pr#52682 <https://github.com/ceph/ceph/pull/52682>`_, Patrick Donnelly)
* mds: wait for unlink operation to finish (`pr#50986 <https://github.com/ceph/ceph/pull/50986>`_, Xiubo Li)
* mds: wait reintegrate to finish when unlinking (`pr#51686 <https://github.com/ceph/ceph/pull/51686>`_, Xiubo Li)
* mgr/cephadm: Adding --storage.tsdb.retention.size prometheus option (`pr#51647 <https://github.com/ceph/ceph/pull/51647>`_, Redouane Kachach)
* mgr/cephadm: don't try to write client/os tuning profiles to known offline hosts (`pr#51346 <https://github.com/ceph/ceph/pull/51346>`_, Adam King)
* mgr/cephadm: support for miscellaneous config files for daemons (`pr#51517 <https://github.com/ceph/ceph/pull/51517>`_, Adam King)
* mgr/dashboard: allow PUT in CORS (`pr#52704 <https://github.com/ceph/ceph/pull/52704>`_, Nizamudeen A)
* mgr/dashboard: API docs UI does not work with Angular dev server (`pr#51245 <https://github.com/ceph/ceph/pull/51245>`_, Volker Theile)
* mgr/dashboard: expose more grafana configs in service form (`pr#51113 <https://github.com/ceph/ceph/pull/51113>`_, Nizamudeen A)
* mgr/dashboard: Fix broken Fedora image URL (`pr#52477 <https://github.com/ceph/ceph/pull/52477>`_, Zack Cerza)
* mgr/dashboard: Fix rbd snapshot creation (`pr#51075 <https://github.com/ceph/ceph/pull/51075>`_, Aashish Sharma)
* mgr/dashboard: fix the rbd mirroring configure check (`pr#51324 <https://github.com/ceph/ceph/pull/51324>`_, Nizamudeen A)
* mgr/dashboard: move cephadm e2e cleanup to jenkins job config (`pr#52389 <https://github.com/ceph/ceph/pull/52389>`_, Nizamudeen A)
* mgr/dashboard: rbd-mirror force promotion (`pr#51056 <https://github.com/ceph/ceph/pull/51056>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: skip Create OSDs step in Cluster expansion (`pr#51150 <https://github.com/ceph/ceph/pull/51150>`_, Nizamudeen A)
* mgr/dashboard: SSO error: AttributeError: 'str' object has no attribute 'decode' (`pr#51950 <https://github.com/ceph/ceph/pull/51950>`_, Volker Theile)
* mgr/nfs: disallow non-existent paths when creating export (`pr#50809 <https://github.com/ceph/ceph/pull/50809>`_, Dhairya Parmar)
* mgr/orchestrator: fix device size in `orch device ls` output (`pr#51211 <https://github.com/ceph/ceph/pull/51211>`_, Adam King)
* mgr/rbd_support: fixes related to recover from rados client blocklisting (`pr#51464 <https://github.com/ceph/ceph/pull/51464>`_, Ramana Raja)
* mgr/snap_schedule: add debug log for paths failing snapshot creation (`pr#51246 <https://github.com/ceph/ceph/pull/51246>`_, Milind Changire)
* mgr/snap_schedule: catch all exceptions for cli (`pr#52753 <https://github.com/ceph/ceph/pull/52753>`_, Milind Changire)
* mgr/volumes: avoid returning -ESHUTDOWN back to cli (`issue#58651 <http://tracker.ceph.com/issues/58651>`_, `pr#51039 <https://github.com/ceph/ceph/pull/51039>`_, Venky Shankar)
* mgr: store names of modules that register RADOS clients in the MgrMap (`pr#52883 <https://github.com/ceph/ceph/pull/52883>`_, Ramana Raja)
* MgrMonitor: batch commit OSDMap and MgrMap mutations (`pr#50980 <https://github.com/ceph/ceph/pull/50980>`_, Patrick Donnelly, Kefu Chai, Radosław Zarzyński)
* mon/ConfigMonitor: update crush_location from osd entity (`pr#52468 <https://github.com/ceph/ceph/pull/52468>`_, Didier Gazen)
* mon/MDSMonitor: batch last_metadata update with pending (`pr#52230 <https://github.com/ceph/ceph/pull/52230>`_, Patrick Donnelly)
* mon/MDSMonitor: check fscid in pending exists in current (`pr#52233 <https://github.com/ceph/ceph/pull/52233>`_, Patrick Donnelly)
* mon/MDSMonitor: do not propose on error in prepare_update (`pr#52240 <https://github.com/ceph/ceph/pull/52240>`_, Patrick Donnelly)
* mon/MDSMonitor: ignore extraneous up:boot messages (`pr#52244 <https://github.com/ceph/ceph/pull/52244>`_, Patrick Donnelly)
* mon/MonClient: before complete auth with error, reopen session (`pr#52133 <https://github.com/ceph/ceph/pull/52133>`_, Nitzan Mordechai)
* mon: avoid exception when setting require-osd-release more than 2 versions up (`pr#51382 <https://github.com/ceph/ceph/pull/51382>`_, Igor Fedotov)
* mon: block osd pool mksnap for fs pools (`pr#52397 <https://github.com/ceph/ceph/pull/52397>`_, Milind Changire)
* Monitor: forward report command to leader (`pr#51258 <https://github.com/ceph/ceph/pull/51258>`_, Dan van der Ster)
* orchestrator: add `--no-destroy` arg to `ceph orch osd rm` (`pr#51213 <https://github.com/ceph/ceph/pull/51213>`_, Guillaume Abrioux)
* os/bluestore: allocator's cumulative backport (`pr#50321 <https://github.com/ceph/ceph/pull/50321>`_, Igor Fedotov, Adam Kupczyk, Ronen Friedman)
* os/bluestore: allow 'fit_to_fast' selector for single-volume osd (`pr#51418 <https://github.com/ceph/ceph/pull/51418>`_, Igor Fedotov)
* os/bluestore: cumulative bluefs backport (`pr#52212 <https://github.com/ceph/ceph/pull/52212>`_, Igor Fedotov, Adam Kupczyk)
* os/bluestore: don't need separate variable to mark hits when lookup oid (`pr#52943 <https://github.com/ceph/ceph/pull/52943>`_, locallocal)
* os/bluestore: fix spillover alert (`pr#50932 <https://github.com/ceph/ceph/pull/50932>`_, Igor Fedotov)
* os/bluestore: proper override rocksdb::WritableFile::Allocate (`pr#51773 <https://github.com/ceph/ceph/pull/51773>`_, Igor Fedotov)
* os/bluestore: report min_alloc_size through "ceph osd metadata" (`pr#50506 <https://github.com/ceph/ceph/pull/50506>`_, Igor Fedotov)
* osd/OSDCap: allow rbd.metadata_list method under rbd-read-only profile (`pr#51876 <https://github.com/ceph/ceph/pull/51876>`_, Ilya Dryomov)
* OSD: Fix check_past_interval_bounds() (`pr#51510 <https://github.com/ceph/ceph/pull/51510>`_, Matan Breizman, Samuel Just)
* pybind/argparse: blocklist ip validation (`pr#51812 <https://github.com/ceph/ceph/pull/51812>`_, Nitzan Mordechai)
* pybind/mgr/pg_autoscaler: Reorderd if statement for the func: _maybe_adjust (`pr#50694 <https://github.com/ceph/ceph/pull/50694>`_, Kamoltat)
* pybind: drop GIL during library callouts (`pr#52323 <https://github.com/ceph/ceph/pull/52323>`_, Ilya Dryomov, Patrick Donnelly)
* python-common: drive_selection: fix KeyError when osdspec_affinity is not set (`pr#53157 <https://github.com/ceph/ceph/pull/53157>`_, Guillaume Abrioux)
* qa/rgw: add POOL_APP_NOT_ENABLED to log-ignorelist (`pr#52048 <https://github.com/ceph/ceph/pull/52048>`_, Casey Bodley)
* qa/suites/rados: remove rook coverage from the rados suite (`pr#52017 <https://github.com/ceph/ceph/pull/52017>`_, Laura Flores)
* qa/suites/rbd: install qemu-utils in addition to qemu-block-extra on Ubuntu (`pr#51059 <https://github.com/ceph/ceph/pull/51059>`_, Ilya Dryomov)
* qa/suites/upgrade/octopus-x: skip TestClsRbd.mirror_snapshot test (`pr#53002 <https://github.com/ceph/ceph/pull/53002>`_, Ilya Dryomov)
* qa: check each fs for health (`pr#51232 <https://github.com/ceph/ceph/pull/51232>`_, Patrick Donnelly)
* qa: data-scan/journal-tool do not output debugging in upstream testing (`pr#50773 <https://github.com/ceph/ceph/pull/50773>`_, Patrick Donnelly)
* qa: fix cephfs-mirror unwinding and 'fs volume create/rm' order (`pr#52654 <https://github.com/ceph/ceph/pull/52654>`_, Jos Collin)
* qa: mirror tests should cleanup fs during unwind (`pr#50765 <https://github.com/ceph/ceph/pull/50765>`_, Patrick Donnelly)
* qa: run scrub post file system recovery (`issue#59527 <http://tracker.ceph.com/issues/59527>`_, `pr#51610 <https://github.com/ceph/ceph/pull/51610>`_, Venky Shankar)
* qa: test_simple failure (`pr#50756 <https://github.com/ceph/ceph/pull/50756>`_, Patrick Donnelly)
* qa: use parallel gzip for compressing logs (`pr#52953 <https://github.com/ceph/ceph/pull/52953>`_, Patrick Donnelly)
* qa: wait for MDSMonitor tick to replace daemons (`pr#52237 <https://github.com/ceph/ceph/pull/52237>`_, Patrick Donnelly)
* radosgw-admin: try reshard even if bucket is resharding (`pr#51836 <https://github.com/ceph/ceph/pull/51836>`_, Casey Bodley)
* rbd-mirror: fix image replayer shut down description on force promote (`pr#52878 <https://github.com/ceph/ceph/pull/52878>`_, Prasanna Kumar Kalever)
* rbd-mirror: fix race preventing local image deletion (`pr#52625 <https://github.com/ceph/ceph/pull/52625>`_, N Balachandran)
* rgw/rados: check_quota() uses real bucket owner (`pr#51330 <https://github.com/ceph/ceph/pull/51330>`_, Mykola Golub, Casey Bodley)
* rgw/s3: dump Message field in Error response even if empty (`pr#51200 <https://github.com/ceph/ceph/pull/51200>`_, Casey Bodley)
* rgw: avoid string_view to temporary in RGWBulkUploadOp (`pr#52159 <https://github.com/ceph/ceph/pull/52159>`_, Casey Bodley)
* rgw: fix consistency bug with OLH objects (`pr#52552 <https://github.com/ceph/ceph/pull/52552>`_, Cory Snyder)
* rgw: LDAP fix resource leak with wrong credentials (`pr#50560 <https://github.com/ceph/ceph/pull/50560>`_, Johannes Liebl, Johannes)
* rgw: under fips & openssl 3.x allow md5 usage in select rgw ops (`pr#51266 <https://github.com/ceph/ceph/pull/51266>`_, Mark Kogan)
* src/valgrind.supp: Adding know leaks unrelated to ceph (`pr#49521 <https://github.com/ceph/ceph/pull/49521>`_, Nitzan Mordechai)
* src/valgrind.supp: Adding know leaks unrelated to ceph (`pr#51341 <https://github.com/ceph/ceph/pull/51341>`_, Nitzan Mordechai)
* test: correct osd pool default size (`pr#51803 <https://github.com/ceph/ceph/pull/51803>`_, Nitzan Mordechai)
* test: monitor thrasher wait until quorum (`pr#51799 <https://github.com/ceph/ceph/pull/51799>`_, Nitzan Mordechai)
* tests: remove pubsub tests from multisite (`pr#48928 <https://github.com/ceph/ceph/pull/48928>`_, Yuval Lifshitz)
* tools/ceph-dencoder: Fix incorrect type define for trash_watcher (`pr#51778 <https://github.com/ceph/ceph/pull/51778>`_, Chen Yuanrun)
* tools/ceph-kvstore-tool: fix segfaults when repair the rocksdb (`pr#51254 <https://github.com/ceph/ceph/pull/51254>`_, huangjun)
* tools/cephfs-data-scan: support for multi-datapool (`pr#50523 <https://github.com/ceph/ceph/pull/50523>`_, Mykola Golub)
* vstart: check mgr status after starting mgr (`pr#51604 <https://github.com/ceph/ceph/pull/51604>`_, Rongqi Sun)
* Wip nitzan fixing few rados/test.sh (`pr#49943 <https://github.com/ceph/ceph/pull/49943>`_, Nitzan Mordechai)
* qa: add subvolume option flavors (`pr#51509 <https://github.com/ceph/ceph/pull/51509>`_, Milind Changire, Venky Shankar)

v16.2.13 Pacific
================

This is the thirteenth backport release in the Pacific series.

Notable Changes
---------------

* CephFS: Rename the `mds_max_retries_on_remount_failure` option to
  `client_max_retries_on_remount_failure` and move it from mds.yaml.in to
  mds-client.yaml.in because this option was only used by MDS client from its
  birth.

* `ceph mgr dump` command now outputs `last_failure_osd_epoch` and
  `active_clients` fields at the top level.  Previously, these fields were
  output under `always_on_modules` field.

Changelog
---------

* backport PR #39607 (`pr#51344 <https://github.com/ceph/ceph/pull/51344>`_, Rishabh Dave)
* ceph-crash: drop privleges to run as "ceph" user, rather than root (CVE-2022-3650) (`pr#48804 <https://github.com/ceph/ceph/pull/48804>`_, Tim Serong, Guillaume Abrioux)
* ceph-mixing: fix ceph_hosts variable (`pr#48933 <https://github.com/ceph/ceph/pull/48933>`_, Tatjana Dehler)
* ceph-volume/tests: add allowlist_externals to tox.ini (`pr#49789 <https://github.com/ceph/ceph/pull/49789>`_, Guillaume Abrioux)
* ceph-volume: do not raise RuntimeError in util.lsblk (`pr#50145 <https://github.com/ceph/ceph/pull/50145>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in get_all_devices_vgs() (`pr#49454 <https://github.com/ceph/ceph/pull/49454>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in lsblk_all() (`pr#49869 <https://github.com/ceph/ceph/pull/49869>`_, Guillaume Abrioux)
* ceph-volume: fix issue with fast device allocs when there are multiple PVs per VG (`pr#50878 <https://github.com/ceph/ceph/pull/50878>`_, Cory Snyder)
* ceph-volume: fix regression in activate (`pr#49972 <https://github.com/ceph/ceph/pull/49972>`_, Guillaume Abrioux)
* ceph-volume: legacy_encrypted() shouldn't call lsblk() when device is 'tmpfs' (`pr#50162 <https://github.com/ceph/ceph/pull/50162>`_, Guillaume Abrioux)
* ceph-volume: update the OS before deploying Ceph (pacific) (`pr#50996 <https://github.com/ceph/ceph/pull/50996>`_, Guillaume Abrioux)
* ceph.spec.in: Replace %usrmerged macro with regular version check (`pr#49830 <https://github.com/ceph/ceph/pull/49830>`_, Tim Serong)
* ceph_fuse: retry the test_dentry_handling if fails (`pr#49944 <https://github.com/ceph/ceph/pull/49944>`_, Xiubo Li)
* cephadm: Adding poststop actions and setting TimeoutStartSec to 200s (`pr#50514 <https://github.com/ceph/ceph/pull/50514>`_, Redouane Kachach)
* cephadm: don't overwrite cluster logrotate file (`pr#49927 <https://github.com/ceph/ceph/pull/49927>`_, Adam King)
* cephadm: set pids-limit unlimited for all ceph daemons (`pr#50512 <https://github.com/ceph/ceph/pull/50512>`_, Adam King, Teoman ONAY)
* cephfs-top: addition of sort feature and limit option (`pr#49303 <https://github.com/ceph/ceph/pull/49303>`_, Neeraj Pratap Singh, Jos Collin)
* cephfs-top: drop curses.A_ITALIC (`pr#50029 <https://github.com/ceph/ceph/pull/50029>`_, Jos Collin)
* cephfs-top: Handle `METRIC_TYPE_NONE` fields for sorting (`pr#50597 <https://github.com/ceph/ceph/pull/50597>`_, Neeraj Pratap Singh)
* cls/rgw: remove index entry after cancelling last racing delete op (`pr#50243 <https://github.com/ceph/ceph/pull/50243>`_, Casey Bodley)
* doc/ceph-volume: fix cephadm references (`pr#50116 <https://github.com/ceph/ceph/pull/50116>`_, Piotr Parczewski)
* doc/ceph-volume: refine encryption.rst (`pr#49793 <https://github.com/ceph/ceph/pull/49793>`_, Zac Dover)
* doc/ceph-volume: update LUKS docs (`pr#49758 <https://github.com/ceph/ceph/pull/49758>`_, Zac Dover)
* doc/cephadm/host-management: add service spec link (`pr#50255 <https://github.com/ceph/ceph/pull/50255>`_, thomas)
* doc/cephadm/troubleshooting: remove word repeat (`pr#50223 <https://github.com/ceph/ceph/pull/50223>`_, thomas)
* doc/cephadm: grammar / syntax in install.rst (`pr#49949 <https://github.com/ceph/ceph/pull/49949>`_, Piotr Parczewski)
* doc/cephadm: Redd up compatibility.rst (`pr#50368 <https://github.com/ceph/ceph/pull/50368>`_, Anthony D'Atri)
* doc/cephadm: update cephadm compatability and stability page (`pr#50337 <https://github.com/ceph/ceph/pull/50337>`_, Adam King)
* doc/cephfs: add note about CephFS extended attributes and getfattr (`pr#50069 <https://github.com/ceph/ceph/pull/50069>`_, Zac Dover)
* doc/cephfs: describe conf opt "client quota df" in quota doc (`pr#50253 <https://github.com/ceph/ceph/pull/50253>`_, Rishabh Dave)
* doc/cephfs: Improve fs-volumes.rst (`pr#50832 <https://github.com/ceph/ceph/pull/50832>`_, Anthony D'Atri)
* doc/dev: add full stop to sentence in basic-wo (`pr#50401 <https://github.com/ceph/ceph/pull/50401>`_, Zac Dover)
* doc/dev: add git branch management commands (`pr#49739 <https://github.com/ceph/ceph/pull/49739>`_, Zac Dover)
* doc/dev: add Slack to Dev Guide essentials (`pr#49875 <https://github.com/ceph/ceph/pull/49875>`_, Zac Dover)
* doc/dev: backport 49908 to P (Upgrade Testing Docs) (`pr#49911 <https://github.com/ceph/ceph/pull/49911>`_, Zac Dover)
* doc/dev: format command in cephfs-mirroring (`pr#51109 <https://github.com/ceph/ceph/pull/51109>`_, Zac Dover)
* doc/dev: use underscores in config vars (`pr#49893 <https://github.com/ceph/ceph/pull/49893>`_, Ville Ojamo)
* doc/glossary: add "application" to the glossary (`pr#50259 <https://github.com/ceph/ceph/pull/50259>`_, Zac Dover)
* doc/glossary: add "Bucket" (`pr#50225 <https://github.com/ceph/ceph/pull/50225>`_, Zac Dover)
* doc/glossary: add "client" to glossary (`pr#50263 <https://github.com/ceph/ceph/pull/50263>`_, Zac Dover)
* doc/glossary: add "Hybrid Storage" (`pr#51098 <https://github.com/ceph/ceph/pull/51098>`_, Zac Dover)
* doc/glossary: add "Period" to glossary (`pr#50156 <https://github.com/ceph/ceph/pull/50156>`_, Zac Dover)
* doc/glossary: add "Placement Groups" definition (`pr#51186 <https://github.com/ceph/ceph/pull/51186>`_, Zac Dover)
* doc/glossary: add "realm" to glossary (`pr#50135 <https://github.com/ceph/ceph/pull/50135>`_, Zac Dover)
* doc/glossary: add "Scrubbing" (`pr#50703 <https://github.com/ceph/ceph/pull/50703>`_, Zac Dover)
* doc/glossary: add "User" (`pr#50673 <https://github.com/ceph/ceph/pull/50673>`_, Zac Dover)
* doc/glossary: Add "zone" to glossary.rst (`pr#50272 <https://github.com/ceph/ceph/pull/50272>`_, Zac Dover)
* doc/glossary: add AWS/OpenStack bucket info (`pr#50248 <https://github.com/ceph/ceph/pull/50248>`_, Zac Dover)
* doc/glossary: improve "CephX" entry (`pr#51065 <https://github.com/ceph/ceph/pull/51065>`_, Zac Dover)
* doc/glossary: link to CephX Config ref (`pr#50709 <https://github.com/ceph/ceph/pull/50709>`_, Zac Dover)
* doc/index: remove "uniquely" from landing page (`pr#50478 <https://github.com/ceph/ceph/pull/50478>`_, Zac Dover)
* doc/install: link to "cephadm installing ceph" (`pr#49782 <https://github.com/ceph/ceph/pull/49782>`_, Zac Dover)
* doc/install: refine index.rst (`pr#50436 <https://github.com/ceph/ceph/pull/50436>`_, Zac Dover)
* doc/install: update index.rst (`pr#50433 <https://github.com/ceph/ceph/pull/50433>`_, Zac Dover)
* doc/mgr/prometheus: fix confval reference (`pr#51094 <https://github.com/ceph/ceph/pull/51094>`_, Piotr Parczewski)
* doc/msgr2: update dual stack status (`pr#50801 <https://github.com/ceph/ceph/pull/50801>`_, Dan van der Ster)
* doc/operations: fix prompt in bluestore-migration (`pr#50663 <https://github.com/ceph/ceph/pull/50663>`_, Zac Dover)
* doc/rados/config: edit auth-config-ref (`pr#50951 <https://github.com/ceph/ceph/pull/50951>`_, Zac Dover)
* doc/rados/operations: edit monitoring.rst (`pr#51037 <https://github.com/ceph/ceph/pull/51037>`_, Zac Dover)
* doc/rados/operations: Fix double prompt (`pr#49899 <https://github.com/ceph/ceph/pull/49899>`_, Ville Ojamo)
* doc/rados/operations: Fix indentation (`pr#49896 <https://github.com/ceph/ceph/pull/49896>`_, Ville Ojamo)
* doc/rados/operations: Fix typo in erasure-code.rst (`pr#50753 <https://github.com/ceph/ceph/pull/50753>`_, Sainithin Artham)
* doc/rados/operations: Improve wording, capitalization, formatting (`pr#50454 <https://github.com/ceph/ceph/pull/50454>`_, Anthony D'Atri)
* doc/rados/ops: add ceph-medic documentation (`pr#50854 <https://github.com/ceph/ceph/pull/50854>`_, Zac Dover)
* doc/rados/ops: add hyphen to mon-osd-pg.rst (`pr#50961 <https://github.com/ceph/ceph/pull/50961>`_, Zac Dover)
* doc/rados/ops: edit health checks.rst (5 of x) (`pr#50968 <https://github.com/ceph/ceph/pull/50968>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (1 of x) (`pr#50798 <https://github.com/ceph/ceph/pull/50798>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (2 of x) (`pr#50913 <https://github.com/ceph/ceph/pull/50913>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (3 of x) (`pr#50954 <https://github.com/ceph/ceph/pull/50954>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (4 of x) (`pr#50957 <https://github.com/ceph/ceph/pull/50957>`_, Zac Dover)
* doc/rados/ops: edit health-checks.rst (6 of x) (`pr#50971 <https://github.com/ceph/ceph/pull/50971>`_, Zac Dover)
* doc/rados/ops: edit monitoring-osd-pg.rst (1 of x) (`pr#50866 <https://github.com/ceph/ceph/pull/50866>`_, Zac Dover)
* doc/rados/ops: edit monitoring-osd-pg.rst (2 of x) (`pr#50947 <https://github.com/ceph/ceph/pull/50947>`_, Zac Dover)
* doc/rados/ops: line-edit operating.rst (`pr#50935 <https://github.com/ceph/ceph/pull/50935>`_, Zac Dover)
* doc/rados/ops: remove ceph-medic from monitoring (`pr#51089 <https://github.com/ceph/ceph/pull/51089>`_, Zac Dover)
* doc/rados: add link to ops/health-checks.rst (`pr#50763 <https://github.com/ceph/ceph/pull/50763>`_, Zac Dover)
* doc/rados: clean up ops/bluestore-migration.rst (`pr#50679 <https://github.com/ceph/ceph/pull/50679>`_, Zac Dover)
* doc/rados: edit operations/bs-migration (1 of x) (`pr#50588 <https://github.com/ceph/ceph/pull/50588>`_, Zac Dover)
* doc/rados: edit operations/bs-migration (2 of x) (`pr#50591 <https://github.com/ceph/ceph/pull/50591>`_, Zac Dover)
* doc/rados: edit ops/monitoring.rst (1 of 3) (`pr#50824 <https://github.com/ceph/ceph/pull/50824>`_, Zac Dover)
* doc/rados: edit ops/monitoring.rst (2 of 3) (`pr#50850 <https://github.com/ceph/ceph/pull/50850>`_, Zac Dover)
* doc/rados: edit user-management.rst (1 of x) (`pr#50642 <https://github.com/ceph/ceph/pull/50642>`_, Zac Dover)
* doc/rados: line edit mon-lookup-dns top matter (`pr#50583 <https://github.com/ceph/ceph/pull/50583>`_, Zac Dover)
* doc/rados: line-edit common.rst (`pr#50944 <https://github.com/ceph/ceph/pull/50944>`_, Zac Dover)
* doc/rados: line-edit erasure-code.rst (`pr#50620 <https://github.com/ceph/ceph/pull/50620>`_, Zac Dover)
* doc/rados: line-edit pg-repair.rst (`pr#50804 <https://github.com/ceph/ceph/pull/50804>`_, Zac Dover)
* doc/rados: line-edit upmap.rst (`pr#50567 <https://github.com/ceph/ceph/pull/50567>`_, Zac Dover)
* doc/rados: refine ceph-conf.rst (`pr#49833 <https://github.com/ceph/ceph/pull/49833>`_, Zac Dover)
* doc/rados: refine pool-pg-config-ref.rst (`pr#49822 <https://github.com/ceph/ceph/pull/49822>`_, Zac Dover)
* doc/rados: update OSD_BACKFILLFULL description (`pr#50219 <https://github.com/ceph/ceph/pull/50219>`_, Ponnuvel Palaniyappan)
* doc/radosgw: format admonitions (`pr#50357 <https://github.com/ceph/ceph/pull/50357>`_, Zac Dover)
* doc/radosgw: format part of s3select (`pr#51118 <https://github.com/ceph/ceph/pull/51118>`_, Cole Mitchell)
* doc/radosgw: format part of s3select (`pr#51106 <https://github.com/ceph/ceph/pull/51106>`_, Cole Mitchell)
* doc/radosgw: multisite - edit "functional changes" (`pr#50278 <https://github.com/ceph/ceph/pull/50278>`_, Zac Dover)
* doc/radosgw: refine "Maintenance" in multisite.rst (`pr#50026 <https://github.com/ceph/ceph/pull/50026>`_, Zac Dover)
* doc/radosgw: s/execute/run/ in multisite.rst (`pr#50174 <https://github.com/ceph/ceph/pull/50174>`_, Zac Dover)
* doc/radosgw: s/zone group/zonegroup/g et alia (`pr#50298 <https://github.com/ceph/ceph/pull/50298>`_, Zac Dover)
* doc/rbd/rbd-exclusive-locks: warn about automatic lock transitions (`pr#49805 <https://github.com/ceph/ceph/pull/49805>`_, Ilya Dryomov)
* doc/rbd: format iscsi-initiator-linux.rbd better (`pr#49750 <https://github.com/ceph/ceph/pull/49750>`_, Zac Dover)
* doc/rgw - fix grammar in table in s3.rst (`pr#50389 <https://github.com/ceph/ceph/pull/50389>`_, Zac Dover)
* doc/rgw: "Migrating Single Site to Multi-Site" (`pr#50094 <https://github.com/ceph/ceph/pull/50094>`_, Zac Dover)
* doc/rgw: caption a diagram (`pr#50294 <https://github.com/ceph/ceph/pull/50294>`_, Zac Dover)
* doc/rgw: clarify multisite.rst top matter (`pr#50205 <https://github.com/ceph/ceph/pull/50205>`_, Zac Dover)
* doc/rgw: clean zone-sync.svg (`pr#50363 <https://github.com/ceph/ceph/pull/50363>`_, Zac Dover)
* doc/rgw: fix caption (`pr#50396 <https://github.com/ceph/ceph/pull/50396>`_, Zac Dover)
* doc/rgw: improve diagram caption (`pr#50332 <https://github.com/ceph/ceph/pull/50332>`_, Zac Dover)
* doc/rgw: multisite ref. top matter cleanup (`pr#50190 <https://github.com/ceph/ceph/pull/50190>`_, Zac Dover)
* doc/rgw: refine "Configuring Secondary Zones" (`pr#50075 <https://github.com/ceph/ceph/pull/50075>`_, Zac Dover)
* doc/rgw: refine "Failover and Disaster Recovery" (`pr#50079 <https://github.com/ceph/ceph/pull/50079>`_, Zac Dover)
* doc/rgw: refine "Multi-site Config Ref" (1 of x) (`pr#50118 <https://github.com/ceph/ceph/pull/50118>`_, Zac Dover)
* doc/rgw: refine "Realms" section (`pr#50140 <https://github.com/ceph/ceph/pull/50140>`_, Zac Dover)
* doc/rgw: refine "Setting a Zonegroup" (`pr#51073 <https://github.com/ceph/ceph/pull/51073>`_, Zac Dover)
* doc/rgw: refine "Zones" in multisite.rst (`pr#49983 <https://github.com/ceph/ceph/pull/49983>`_, Zac Dover)
* doc/rgw: refine 1-50 of multisite.rst (`pr#49996 <https://github.com/ceph/ceph/pull/49996>`_, Zac Dover)
* doc/rgw: refine keycloak.rst (`pr#50379 <https://github.com/ceph/ceph/pull/50379>`_, Zac Dover)
* doc/rgw: refine multisite to "config 2ndary zones" (`pr#50032 <https://github.com/ceph/ceph/pull/50032>`_, Zac Dover)
* doc/rgw: refine ~50-~140 of multisite.rst (`pr#50009 <https://github.com/ceph/ceph/pull/50009>`_, Zac Dover)
* doc/rgw: remove "tertiary", link to procedure (`pr#50288 <https://github.com/ceph/ceph/pull/50288>`_, Zac Dover)
* doc/rgw: s/[Zz]one [Gg]roup/zonegroup/g (`pr#50137 <https://github.com/ceph/ceph/pull/50137>`_, Zac Dover)
* doc/rgw: session-tags.rst - fix link to keycloak (`pr#50188 <https://github.com/ceph/ceph/pull/50188>`_, Zac Dover)
* doc/start: add RST escape character rules for bold (`pr#49752 <https://github.com/ceph/ceph/pull/49752>`_, Zac Dover)
* doc/start: documenting-ceph - add squash procedure (`pr#50741 <https://github.com/ceph/ceph/pull/50741>`_, Zac Dover)
* doc/start: edit first 150 lines of documenting-ceph (`pr#51183 <https://github.com/ceph/ceph/pull/51183>`_, Zac Dover)
* doc/start: format procedure in documenting-ceph (`pr#50789 <https://github.com/ceph/ceph/pull/50789>`_, Zac Dover)
* doc/start: update "notify us" section (`pr#50771 <https://github.com/ceph/ceph/pull/50771>`_, Zac Dover)
* doc: add the damage types that scrub can repair (`pr#49933 <https://github.com/ceph/ceph/pull/49933>`_, Neeraj Pratap Singh)
* doc: document debugging for libcephsqlite (`pr#50034 <https://github.com/ceph/ceph/pull/50034>`_, Patrick Donnelly)
* doc: preen cephadm/troubleshooting.rst and radosgw/placement.rst (`pr#50229 <https://github.com/ceph/ceph/pull/50229>`_, Anthony D'Atri)
* drive_group: fix limit filter in drive_selection.selector (`pr#50371 <https://github.com/ceph/ceph/pull/50371>`_, Guillaume Abrioux)
* kv/RocksDBStore: Add CompactOnDeletion support (`pr#50894 <https://github.com/ceph/ceph/pull/50894>`_, Radoslaw Zarzynski, Mark Nelson)
* libcephsqlite: CheckReservedLock the result will always be zero (`pr#50036 <https://github.com/ceph/ceph/pull/50036>`_, Shuai Wang)
* librbd/crypto: fix bad return checks from libcryptsetup (`pr#49413 <https://github.com/ceph/ceph/pull/49413>`_, Or Ozeri)
* librbd: avoid EUCLEAN error after "rbd rm" is interrupted (`pr#50129 <https://github.com/ceph/ceph/pull/50129>`_, weixinwei)
* librbd: call apply_changes() after setting librados_thread_count (`pr#50289 <https://github.com/ceph/ceph/pull/50289>`_, Ilya Dryomov)
* librbd: Fix local rbd mirror journals growing forever (`pr#50158 <https://github.com/ceph/ceph/pull/50158>`_, Ilya Dryomov, Josef Johansson)
* librbd: fix wrong attribute for rbd_quiesce_complete API (`pr#50872 <https://github.com/ceph/ceph/pull/50872>`_, Dongsheng Yang)
* librbd: report better errors when failing to enable mirroring on an image (`pr#50836 <https://github.com/ceph/ceph/pull/50836>`_, Prasanna Kumar Kalever)
* mds/PurgeQueue: don't consider filer_max_purge_ops when _calculate_ops (`pr#49656 <https://github.com/ceph/ceph/pull/49656>`_, haoyixing)
* mds/Server: do not allow -ve reclaim flags to cause client eviction (`pr#49956 <https://github.com/ceph/ceph/pull/49956>`_, Dhairya Parmar)
* mds: account for snapshot items when deciding to split or merge a directory (`issue#55215 <http://tracker.ceph.com/issues/55215>`_, `pr#49867 <https://github.com/ceph/ceph/pull/49867>`_, Venky Shankar)
* mds: avoid ~mdsdir's scrubbing and reporting damage health status (`pr#49440 <https://github.com/ceph/ceph/pull/49440>`_, Neeraj Pratap Singh)
* mds: catch damage to CDentry's first member before persisting (`issue#58482 <http://tracker.ceph.com/issues/58482>`_, `pr#50781 <https://github.com/ceph/ceph/pull/50781>`_, Patrick Donnelly)
* mds: do not acquire xlock in xlockdone state (`pr#49538 <https://github.com/ceph/ceph/pull/49538>`_, Igor Fedotov)
* mds: fix and skip submitting invalid osd request (`pr#49941 <https://github.com/ceph/ceph/pull/49941>`_, Xiubo Li)
* mds: fix scan_stray_dir not reset next.frag on each run of stray inode (`pr#49669 <https://github.com/ceph/ceph/pull/49669>`_, ethanwu)
* mds: md_log_replay thread blocks waiting to be woken up (`pr#49671 <https://github.com/ceph/ceph/pull/49671>`_, zhikuodu)
* mds: switch submit_mutex to fair mutex for MDLog (`pr#49632 <https://github.com/ceph/ceph/pull/49632>`_, Xiubo Li)
* mgr/cephadm: add ingress support for ssl rgw service (`pr#49917 <https://github.com/ceph/ceph/pull/49917>`_, Frank Ederveen)
* mgr/cephadm: be aware of host's shortname and FQDN (`pr#50516 <https://github.com/ceph/ceph/pull/50516>`_, Adam King)
* mgr/cephadm: call iscsi post_remove from serve loop (`pr#49928 <https://github.com/ceph/ceph/pull/49928>`_, Adam King)
* mgr/cephadm: don't add mgr into iscsi trusted_ip_list if it's already there (`pr#50515 <https://github.com/ceph/ceph/pull/50515>`_, Mykola Golub)
* mgr/cephadm: don't say migration in progress if migration current > migration last (`pr#49919 <https://github.com/ceph/ceph/pull/49919>`_, Adam King)
* mgr/cephadm: fix backends service in haproxy config with multiple nfs of same rank (`pr#50511 <https://github.com/ceph/ceph/pull/50511>`_, Adam King)
* mgr/cephadm: fix check for if devices have changed (`pr#49916 <https://github.com/ceph/ceph/pull/49916>`_, Adam King)
* mgr/cephadm: fix handling of mgr upgrades with 3 or more mgrs (`pr#49921 <https://github.com/ceph/ceph/pull/49921>`_, Adam King)
* mgr/cephadm: Fix how we check if a host belongs to public network (`pr#50007 <https://github.com/ceph/ceph/pull/50007>`_, Redouane Kachach)
* mgr/cephadm: fix removing offline hosts with ingress daemons (`pr#49926 <https://github.com/ceph/ceph/pull/49926>`_, Adam King)
* mgr/cephadm: increase ingress timeout values (`pr#49923 <https://github.com/ceph/ceph/pull/49923>`_, Frank Ederveen)
* mgr/cephadm: iscsi username and password defaults to admin (`pr#49310 <https://github.com/ceph/ceph/pull/49310>`_, Nizamudeen A)
* mgr/cephadm: some master -> main cleanup (`pr#49285 <https://github.com/ceph/ceph/pull/49285>`_, Adam King)
* mgr/cephadm: specify ports for iscsi (`pr#49918 <https://github.com/ceph/ceph/pull/49918>`_, Adam King)
* mgr/cephadm: support for extra entrypoint args (`pr#49925 <https://github.com/ceph/ceph/pull/49925>`_, Adam King)
* mgr/cephadm: try to avoid pull when getting container image info (`pr#50513 <https://github.com/ceph/ceph/pull/50513>`_, Mykola Golub, Adam King)
* mgr/cephadm: write client files after applying services (`pr#49920 <https://github.com/ceph/ceph/pull/49920>`_, Adam King)
* mgr/dashboard: add tooltip mirroring pools table (`pr#49503 <https://github.com/ceph/ceph/pull/49503>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: added pattern validaton for form input (`pr#47330 <https://github.com/ceph/ceph/pull/47330>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: custom image for kcli bootstrap script (`pr#50917 <https://github.com/ceph/ceph/pull/50917>`_, Nizamudeen A)
* mgr/dashboard: fix "can't read .ssh/known_hosts: No such file or directory (`pr#50123 <https://github.com/ceph/ceph/pull/50123>`_, Nizamudeen A)
* mgr/dashboard: fix cephadm e2e expression changed error (`pr#51081 <https://github.com/ceph/ceph/pull/51081>`_, Nizamudeen A)
* mgr/dashboard: fix create osd default selected as recommended not working (`pr#51038 <https://github.com/ceph/ceph/pull/51038>`_, Nizamudeen A)
* mgr/dashboard: fix displaying mirror image progress (`pr#50870 <https://github.com/ceph/ceph/pull/50870>`_, Pere Diaz Bou)
* mgr/dashboard: fix eviction of all FS clients (`pr#51009 <https://github.com/ceph/ceph/pull/51009>`_, Pere Diaz Bou)
* mgr/dashboard: fix weird data in osd details (`pr#50121 <https://github.com/ceph/ceph/pull/50121>`_, Pedro Gonzalez Gomez, Nizamudeen A)
* mgr/dashboard: force TLS 1.3 (`pr#50527 <https://github.com/ceph/ceph/pull/50527>`_, Ernesto Puerta)
* mgr/dashboard: Hide maintenance option on expand cluster (`pr#47725 <https://github.com/ceph/ceph/pull/47725>`_, Nizamudeen A)
* mgr/dashboard: ignore the rules 400 error in dashboard kcli e2e (`pr#50914 <https://github.com/ceph/ceph/pull/50914>`_, Nizamudeen A)
* mgr/dashboard: osd form preselect db/wal device filters (`pr#50122 <https://github.com/ceph/ceph/pull/50122>`_, Nizamudeen A)
* mgr/dashboard: Replace vonage-status-panel with native grafana stat panel (`pr#50044 <https://github.com/ceph/ceph/pull/50044>`_, Aashish Sharma)
* mgr/nfs: add sectype option (`pr#49929 <https://github.com/ceph/ceph/pull/49929>`_, John Mulligan)
* mgr/orchestrator: fix upgrade status help message (`pr#49922 <https://github.com/ceph/ceph/pull/49922>`_, Adam King)
* mgr/prometheus: export zero valued pg state metrics (`pr#49786 <https://github.com/ceph/ceph/pull/49786>`_, Avan Thakkar)
* mgr/prometheus: expose daemon health metrics (`pr#49520 <https://github.com/ceph/ceph/pull/49520>`_, Pere Diaz Bou)
* mgr/prometheus: fix module crash when trying to collect OSDs metrics (`pr#49931 <https://github.com/ceph/ceph/pull/49931>`_, Redouane Kachach)
* mgr/rbd_support: remove localized schedule option during module startup (`pr#49650 <https://github.com/ceph/ceph/pull/49650>`_, Ramana Raja)
* mgr/snap_schedule: replace .snap with the client configured snap dir name (`pr#47726 <https://github.com/ceph/ceph/pull/47726>`_, Neeraj Pratap Singh)
* mon/MgrMap: dump last_failure_osd_epoch and active_clients at top level (`pr#50305 <https://github.com/ceph/ceph/pull/50305>`_, Ilya Dryomov)
* mon/MonCommands: Support dump_historic_slow_ops (`pr#49233 <https://github.com/ceph/ceph/pull/49233>`_, Matan Breizman)
* mon: bail from handle_command() if _generate_command_map() fails (`pr#48846 <https://github.com/ceph/ceph/pull/48846>`_, Nikhil Kshirsagar)
* mon: disable snap id allocation for fsmap pools (`pr#50050 <https://github.com/ceph/ceph/pull/50050>`_, Milind Changire)
* mon: Fix condition to check for ceph version mismatch (`pr#49988 <https://github.com/ceph/ceph/pull/49988>`_, Prashant D)
* os/bluestore: fix onode ref counting (`pr#50072 <https://github.com/ceph/ceph/pull/50072>`_, Igor Fedotov)
* os/memstore: Fix memory leak (`pr#50092 <https://github.com/ceph/ceph/pull/50092>`_, Adam Kupczyk)
* pybind/mgr: check for empty metadata mgr_module:get_metadata() (`issue#57072 <http://tracker.ceph.com/issues/57072>`_, `pr#49966 <https://github.com/ceph/ceph/pull/49966>`_, Venky Shankar)
* qa/rgw: use symlinks to specify distro (`pr#50940 <https://github.com/ceph/ceph/pull/50940>`_, Casey Bodley)
* qa/suites/rbd: fix sporadic "rx-only direction" test failures (`pr#50112 <https://github.com/ceph/ceph/pull/50112>`_, Ilya Dryomov)
* qa/suites/rgw: fix and update tempest and barbican tests (`pr#50000 <https://github.com/ceph/ceph/pull/50000>`_, Tobias Urdin)
* qa/tasks/cephadm.py: fix pulling cephadm from git.ceph.com (`pr#49915 <https://github.com/ceph/ceph/pull/49915>`_, Adam King)
* qa/tests: added pacific client upgrade => reef (`pr#50352 <https://github.com/ceph/ceph/pull/50352>`_, Yuri Weinstein)
* qa: check each fs for health (`pr#51232 <https://github.com/ceph/ceph/pull/51232>`_, Patrick Donnelly)
* qa: ignore expected scrub error (`pr#50775 <https://github.com/ceph/ceph/pull/50775>`_, Patrick Donnelly)
* qa: ignore MDS_TRIM warnings when osd thrashing (`pr#50757 <https://github.com/ceph/ceph/pull/50757>`_, Patrick Donnelly)
* qa: lengthen health warning wait (`pr#50760 <https://github.com/ceph/ceph/pull/50760>`_, Patrick Donnelly)
* qa: load file system info if not created (`pr#50923 <https://github.com/ceph/ceph/pull/50923>`_, Patrick Donnelly)
* qa: test the "ms_mode" options in kclient workflows (`pr#50712 <https://github.com/ceph/ceph/pull/50712>`_, Jeff Layton)
* qa: test_recovery_pool uses wrong recovery procedure (`pr#50860 <https://github.com/ceph/ceph/pull/50860>`_, Patrick Donnelly)
* qa: wait for scrub to finish (`pr#49458 <https://github.com/ceph/ceph/pull/49458>`_, Milind Changire)
* rbd-mirror: add information about the last snapshot sync to image status (`pr#50265 <https://github.com/ceph/ceph/pull/50265>`_, Divyansh Kamboj)
* rbd-mirror: fix syncing_percent calculation logic in get_replay_status() (`pr#50181 <https://github.com/ceph/ceph/pull/50181>`_, N Balachandran)
* rgw/beast: fix interaction between keepalive and 100-continue (`pr#49841 <https://github.com/ceph/ceph/pull/49841>`_, Casey Bodley, Yixin Jin)
* rgw/coroutine: check for null stack on wakeup (`pr#49097 <https://github.com/ceph/ceph/pull/49097>`_, Casey Bodley)
* rgw/s3: dump Message field in Error response even if empty (`pr#51200 <https://github.com/ceph/ceph/pull/51200>`_, Casey Bodley)
* rgw: "reshard cancel" errors with "invalid argument" (`pr#49091 <https://github.com/ceph/ceph/pull/49091>`_, J. Eric Ivancich)
* rgw: adding BUCKET_REWRITE and OBJECT_REWRITE OPS to (`pr#49095 <https://github.com/ceph/ceph/pull/49095>`_, Pritha Srivastava)
* rgw: an empty tagset is allowed by S3 (`pr#49809 <https://github.com/ceph/ceph/pull/49809>`_, Volker Theile, Liu Lan)
* rgw: Backport of issue 57562 to Pacific (`pr#49682 <https://github.com/ceph/ceph/pull/49682>`_, Adam C. Emerson)
* rgw: bucket list operation slow down in special scenario (`pr#49086 <https://github.com/ceph/ceph/pull/49086>`_, zealot)
* rgw: concurrency for multi object deletes (`pr#49327 <https://github.com/ceph/ceph/pull/49327>`_, Casey Bodley, Cory Snyder)
* rgw: fix the problem of duplicate idx when bi list (`pr#49829 <https://github.com/ceph/ceph/pull/49829>`_, wangtengfei)
* rgw: optimizations for handling ECANCELED errors from within get_obj_state (`pr#50886 <https://github.com/ceph/ceph/pull/50886>`_, Cory Snyder)
* rgw: rgw_parse_url_bucket() rejects empty bucket names after 'tenant:' (`pr#50624 <https://github.com/ceph/ceph/pull/50624>`_, Casey Bodley)
* rgw: RGWPutLC does not require Content-MD5 (`pr#49089 <https://github.com/ceph/ceph/pull/49089>`_, Casey Bodley)
* tools/cephfs: include lost+found in scan_links (`pr#50784 <https://github.com/ceph/ceph/pull/50784>`_, Patrick Donnelly)
* Wip nitzan pglog ec getattr error (`pr#49937 <https://github.com/ceph/ceph/pull/49937>`_, Nitzan Mordechai)

v16.2.12 Pacific
================

This is a hotfix release that resolves several performance flaws in ceph-volume, particularly during osd activation (https://tracker.ceph.com/issues/57627)

Notable Changes
---------------

Changelog
---------

* ceph-volume: add test case to reproduce bug in get_physical_fast_allocs (`pr#50878 <https://github.com/ceph/ceph/pull/50878>`_, Cory Snyder)
* ceph-volume: do not raise RuntimeError in util.lsblk (`pr#50145 <https://github.com/ceph/ceph/pull/50145>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in get_all_devices_vgs() (`pr#49454 <https://github.com/ceph/ceph/pull/49454>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in lsblk_all() (`pr#49869 <https://github.com/ceph/ceph/pull/49869>`_, Guillaume Abrioux)
* ceph-volume: fix issue with fast device allocs when there are multiple PVs per VG (`pr#50279 <https://github.com/ceph/ceph/pull/50279>`_, Cory Snyder)
* ceph-volume: fix regression in activate (`pr#49972 <https://github.com/ceph/ceph/pull/49972>`_, Guillaume Abrioux)
* ceph-volume: legacy_encrypted() shouldn't call lsblk() when device is 'tmpfs' (`pr#50162 <https://github.com/ceph/ceph/pull/50162>`_, Guillaume Abrioux)
* ceph-volume: update the OS before deploying Ceph (pacific) (`pr#50996 <https://github.com/ceph/ceph/pull/50996>`_, Guillaume Abrioux)


v16.2.11 Pacific
================

This is the eleventh backport release in the Pacific series.

Notable Changes
---------------

* Cephfs: The 'AT_NO_ATTR_SYNC' macro is deprecated, please use the standard
  'AT_STATX_DONT_SYNC' macro. The 'AT_NO_ATTR_SYNC' macro will be removed in
  the future.

* Trimming of PGLog dups is now controlled by the size instead of the version.
  This fixes the PGLog inflation issue that was happening when the on-line
  (in OSD) trimming got jammed after a PG split operation. Also, a new off-line
  mechanism has been added: `ceph-objectstore-tool` got `trim-pg-log-dups` op
  that targets situations where OSD is unable to boot due to those inflated dups.
  If that is the case, in OSD logs the "You can be hit by THE DUPS BUG" warning
  will be visible.
  Relevant tracker: https://tracker.ceph.com/issues/53729

* RBD: `rbd device unmap` command gained `--namespace` option.  Support for
  namespaces was added to RBD in Nautilus 14.2.0 and it has been possible to
  map and unmap images in namespaces using the `image-spec` syntax since then
  but the corresponding option available in most other commands was missing.

Changelog
---------

* .github/CODEOWNERS: tag core devs on core PRs (`pr#46520 <https://github.com/ceph/ceph/pull/46520>`_, Neha Ojha)
* .github: continue on error and reorder milestone step (`pr#46448 <https://github.com/ceph/ceph/pull/46448>`_, Ernesto Puerta)
* .readthedocs.yml: Always build latest doc/releases pages (`pr#47443 <https://github.com/ceph/ceph/pull/47443>`_, David Galloway)
* mgr/alerts: Add Message-Id and Date header to sent emails (`pr#46312 <https://github.com/ceph/ceph/pull/46312>`_, Lorenz Bausch)
* Add mapping for ernno:13 and adding path in error msg in opendir()/cephfs.pyx (`pr#46646 <https://github.com/ceph/ceph/pull/46646>`_, Sarthak0702)
* backport of cephadm: fix osd adoption with custom cluster name (`pr#46552 <https://github.com/ceph/ceph/pull/46552>`_, Adam King)
* bluestore: Improve deferred write decision (`pr#49170 <https://github.com/ceph/ceph/pull/49170>`_, Adam Kupczyk, Igor Fedotov)
* Catch exception if thrown by __generate_command_map() (`pr#45893 <https://github.com/ceph/ceph/pull/45893>`_, Nikhil Kshirsagar)
* ceph-fuse: add dedicated snap stag map for each directory (`pr#46949 <https://github.com/ceph/ceph/pull/46949>`_, Xiubo Li)
* ceph-mixin: backport of recent cleanups (`pr#46549 <https://github.com/ceph/ceph/pull/46549>`_, Arthur Outhenin-Chalandre)
* ceph mixin: backports (`pr#47868 <https://github.com/ceph/ceph/pull/47868>`_, Aswin Toni, Kefu Chai, Anthony D'Atri)
* ceph-volume/tests: fix lvm centos8-filestore-create job (`pr#48123 <https://github.com/ceph/ceph/pull/48123>`_, Guillaume Abrioux)
* ceph-volume: add a retry in util.disk.remove_partition (`pr#47990 <https://github.com/ceph/ceph/pull/47990>`_, Guillaume Abrioux)
* ceph-volume: allow listing devices by OSD ID (`pr#47018 <https://github.com/ceph/ceph/pull/47018>`_, Rishabh Dave)
* ceph-volume: avoid unnecessary subprocess calls (`pr#46969 <https://github.com/ceph/ceph/pull/46969>`_, Guillaume Abrioux)
* ceph-volume: decrease number of `pvs` calls in `lvm list` (`pr#46967 <https://github.com/ceph/ceph/pull/46967>`_, Guillaume Abrioux)
* ceph-volume: do not log sensitive details (`pr#46729 <https://github.com/ceph/ceph/pull/46729>`_, Guillaume Abrioux)
* ceph-volume: fix activate (`pr#46511 <https://github.com/ceph/ceph/pull/46511>`_, Guillaume Abrioux, Sage Weil)
* ceph-volume: fix inventory with device arg (`pr#48126 <https://github.com/ceph/ceph/pull/48126>`_, Guillaume Abrioux)
* ceph-volume: make is_valid() optional (`pr#46731 <https://github.com/ceph/ceph/pull/46731>`_, Guillaume Abrioux)
* ceph-volume: only warn when config file isn't found (`pr#46069 <https://github.com/ceph/ceph/pull/46069>`_, Guillaume Abrioux)
* ceph-volume: Pacific backports (`pr#47413 <https://github.com/ceph/ceph/pull/47413>`_, Guillaume Abrioux, Zack Cerza, Arthur Outhenin-Chalandre)
* ceph-volume: system.get_mounts() refactor (`pr#47535 <https://github.com/ceph/ceph/pull/47535>`_, Guillaume Abrioux)
* ceph-volume: zap osds in rollback_osd() (`pr#44769 <https://github.com/ceph/ceph/pull/44769>`_, Guillaume Abrioux)
* ceph.spec.in: disable annobin plugin if compile with gcc-toolset (`pr#46368 <https://github.com/ceph/ceph/pull/46368>`_, Kefu Chai)
* ceph.spec.in: remove build directory at end of %install (`pr#45698 <https://github.com/ceph/ceph/pull/45698>`_, Tim Serong)
* ceph_test_librados_service: wait longer for servicemap to update (`pr#46677 <https://github.com/ceph/ceph/pull/46677>`_, Sage Weil)
* cephadm batch backport May (`pr#46327 <https://github.com/ceph/ceph/pull/46327>`_, Adam King, Redouane Kachach, Moritz Röhrich)
* cephadm/ceph-volume: fix rm-cluster --zap (`pr#47627 <https://github.com/ceph/ceph/pull/47627>`_, Guillaume Abrioux)
* cephadm: add "su root root" to cephadm.log logrotate config (`pr#47319 <https://github.com/ceph/ceph/pull/47319>`_, Adam King)
* cephadm: add 'is_paused' field in orch status output (`pr#46570 <https://github.com/ceph/ceph/pull/46570>`_, Guillaume Abrioux)
* cephadm: add `ip_nonlocal_bind` to haproxy deployment (`pr#48212 <https://github.com/ceph/ceph/pull/48212>`_, Michael Fritch)
* Cephadm: Allow multiple virtual IP addresses for keepalived and haproxy (`pr#47611 <https://github.com/ceph/ceph/pull/47611>`_, Luis Domingues)
* cephadm: consider stdout to get container version (`pr#48210 <https://github.com/ceph/ceph/pull/48210>`_, Tatjana Dehler)
* cephadm: Fix disk size calculation (`pr#48098 <https://github.com/ceph/ceph/pull/48098>`_, Paul Cuzner)
* cephadm: Fix repo_gpgkey should return 2 vars (`pr#47376 <https://github.com/ceph/ceph/pull/47376>`_, Laurent Barbe)
* cephadm: improve network handling during bootstrap (`pr#46309 <https://github.com/ceph/ceph/pull/46309>`_, Redouane Kachach)
* cephadm: pin flake8 to 5.0.4 (`pr#49058 <https://github.com/ceph/ceph/pull/49058>`_, Kefu Chai)
* cephadm: preserve cephadm user during RPM upgrade (`pr#46553 <https://github.com/ceph/ceph/pull/46553>`_, Scott Shambarger)
* cephadm: prometheus: The generatorURL in alerts is only using hostname (`pr#46352 <https://github.com/ceph/ceph/pull/46352>`_, Volker Theile)
* cephadm: return nonzero exit code when applying spec fails in bootstrap (`pr#48102 <https://github.com/ceph/ceph/pull/48102>`_, Adam King)
* cephadm: run tests as root (`pr#48470 <https://github.com/ceph/ceph/pull/48470>`_, Kefu Chai)
* cephadm: support for Oracle Linux 8 (`pr#47661 <https://github.com/ceph/ceph/pull/47661>`_, Adam King)
* cephadm: support quotes around public/cluster network in config passed to bootstrap (`pr#47664 <https://github.com/ceph/ceph/pull/47664>`_, Adam King)
* cephfs-data-scan: make scan_links more verbose (`pr#48443 <https://github.com/ceph/ceph/pull/48443>`_, Mykola Golub)
* cephfs-shell: fix put and get cmd (`pr#46297 <https://github.com/ceph/ceph/pull/46297>`_, Dhairya Parmar, dparmar18)
* cephfs-shell: move source to separate subdirectory (`pr#47401 <https://github.com/ceph/ceph/pull/47401>`_, Tim Serong)
* cephfs-top: adding filesystem menu option (`pr#47998 <https://github.com/ceph/ceph/pull/47998>`_, Neeraj Pratap Singh)
* cephfs-top: display average read/write/metadata latency (`issue#48619 <http://tracker.ceph.com/issues/48619>`_, `pr#47978 <https://github.com/ceph/ceph/pull/47978>`_, Venky Shankar)
* cephfs-top: fix the rsp/wsp display (`pr#47647 <https://github.com/ceph/ceph/pull/47647>`_, Jos Collin)
* cephfs-top: make cephfs-top display scrollable (`pr#48734 <https://github.com/ceph/ceph/pull/48734>`_, Jos Collin)
* cephfs-top: Multiple filesystem support (`pr#46146 <https://github.com/ceph/ceph/pull/46146>`_, Neeraj Pratap Singh)
* client: always return ESTALE directly in handle_reply (`pr#46557 <https://github.com/ceph/ceph/pull/46557>`_, Xiubo Li)
* client: stop forwarding the request when exceeding 256 times (`pr#46179 <https://github.com/ceph/ceph/pull/46179>`_, Xiubo Li)
* client: switch AT_NO_ATTR_SYNC to AT_STATX_DONT_SYNC (`pr#46679 <https://github.com/ceph/ceph/pull/46679>`_, Xiubo Li)
* client/fuse: Fix directory DACs overriding for root (`pr#46596 <https://github.com/ceph/ceph/pull/46596>`_, Kotresh HR)
* client: abort the client if we couldn't invalidate dentry caches (`pr#48109 <https://github.com/ceph/ceph/pull/48109>`_, Xiubo Li)
* client: add option to disable collecting and sending metrics (`pr#46798 <https://github.com/ceph/ceph/pull/46798>`_, Xiubo Li)
* client: allow overwrites to file with size greater than the max_file_size (`pr#47972 <https://github.com/ceph/ceph/pull/47972>`_, Tamar Shacked)
* client: buffer the truncate if we have the Fx caps (`pr#45792 <https://github.com/ceph/ceph/pull/45792>`_, Xiubo Li)
* client: choose auth MDS for getxattr with the Xs caps (`pr#46799 <https://github.com/ceph/ceph/pull/46799>`_, Xiubo Li)
* client: do not uninline data for read (`pr#48133 <https://github.com/ceph/ceph/pull/48133>`_, Xiubo Li)
* client: fix incorrectly showing the .snap size for stat (`pr#48413 <https://github.com/ceph/ceph/pull/48413>`_, Xiubo Li)
* client: Inode::hold_caps_until is time from monotonic clock now (`pr#46626 <https://github.com/ceph/ceph/pull/46626>`_, Laura Flores, Neeraj Pratap Singh)
* client: stop the remount_finisher thread in the Client::unmount() (`pr#48108 <https://github.com/ceph/ceph/pull/48108>`_, Xiubo Li)
* client: use parent directory POSIX ACLs for snapshot dir (`issue#57084 <http://tracker.ceph.com/issues/57084>`_, `pr#48553 <https://github.com/ceph/ceph/pull/48553>`_, Venky Shankar)
* cls/rbd: update last_read in group::snap_list (`pr#49195 <https://github.com/ceph/ceph/pull/49195>`_, Ilya Dryomov, Prasanna Kumar Kalever)
* cls/rgw: rgw_dir_suggest_changes detects race with completion (`pr#45900 <https://github.com/ceph/ceph/pull/45900>`_, Casey Bodley)
* cmake: check for python(\d)\.(\d+) when building boost (`pr#46365 <https://github.com/ceph/ceph/pull/46365>`_, Kefu Chai)
* cmake: remove spaces in macro used for compiling cython code (`pr#47484 <https://github.com/ceph/ceph/pull/47484>`_, Kefu Chai)
* CODEOWNERS: add RBD team (`pr#46541 <https://github.com/ceph/ceph/pull/46541>`_, Ilya Dryomov)
* common: use boost::shared_mutex on Windows (`pr#47492 <https://github.com/ceph/ceph/pull/47492>`_, Lucian Petrut)
* crash: pthread_mutex_lock() (`pr#47684 <https://github.com/ceph/ceph/pull/47684>`_, Patrick Donnelly)
* doc/cephadm: add prompts to host-management.rst (`pr#48590 <https://github.com/ceph/ceph/pull/48590>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (`pr#49272 <https://github.com/ceph/ceph/pull/49272>`_, Zac Dover)
* doc: Wip pr 46109 backport to pacific (`pr#46117 <https://github.com/ceph/ceph/pull/46117>`_, Ville Ojamo)
* doc: Wip min hardware typo pacific backport 2022 05 19 (`pr#46347 <https://github.com/ceph/ceph/pull/46347>`_, Zac Dover)
* doc/_static: add scroll-margin-top to custom.css (`pr#49645 <https://github.com/ceph/ceph/pull/49645>`_, Zac Dover)
* doc/architecture: correct PDF link (`pr#48796 <https://github.com/ceph/ceph/pull/48796>`_, Zac Dover)
* doc/ceph-volume: add A. D'Atri's suggestions (`pr#48646 <https://github.com/ceph/ceph/pull/48646>`_, Zac Dover)
* doc/ceph-volume: improve prepare.rst (`pr#48669 <https://github.com/ceph/ceph/pull/48669>`_, Zac Dover)
* doc/ceph-volume: refine "bluestore" section (`pr#48635 <https://github.com/ceph/ceph/pull/48635>`_, Zac Dover)
* doc/ceph-volume: refine "filestore" section (`pr#48637 <https://github.com/ceph/ceph/pull/48637>`_, Zac Dover)
* doc/ceph-volume: refine "prepare" top matter (`pr#48652 <https://github.com/ceph/ceph/pull/48652>`_, Zac Dover)
* doc/ceph-volume: refine Filestore docs (`pr#48671 <https://github.com/ceph/ceph/pull/48671>`_, Zac Dover)
* doc/cephadm/services: fix example for specifying rgw placement (`pr#47948 <https://github.com/ceph/ceph/pull/47948>`_, Redouane Kachach)
* doc/cephadm/services: the config section of service specs (`pr#47321 <https://github.com/ceph/ceph/pull/47321>`_, Redouane Kachach)
* doc/cephadm: add airgapped install procedure (`pr#49146 <https://github.com/ceph/ceph/pull/49146>`_, Zac Dover)
* doc/cephadm: add note about OSDs being recreated to OSD removal section (`pr#47103 <https://github.com/ceph/ceph/pull/47103>`_, Adam King)
* doc/cephadm: Add post-upgrade section (`pr#46977 <https://github.com/ceph/ceph/pull/46977>`_, Redouane Kachach)
* doc/cephadm: alphabetize external tools list (`pr#48726 <https://github.com/ceph/ceph/pull/48726>`_, Zac Dover)
* doc/cephadm: arrange "listing hosts" section (`pr#48724 <https://github.com/ceph/ceph/pull/48724>`_, Zac Dover)
* doc/cephadm: clean colons in host-management.rst (`pr#48604 <https://github.com/ceph/ceph/pull/48604>`_, Zac Dover)
* doc/cephadm: correct version staggered upgrade got in pacific (`pr#48056 <https://github.com/ceph/ceph/pull/48056>`_, Adam King)
* doc/cephadm: document recommended syntax for mounting files with ECA (`pr#48069 <https://github.com/ceph/ceph/pull/48069>`_, Adam King)
* doc/cephadm: enhancing daemon operations documentation (`pr#46976 <https://github.com/ceph/ceph/pull/46976>`_, Redouane Kachach)
* doc/cephadm: fix example for specifying networks for rgw (`pr#47807 <https://github.com/ceph/ceph/pull/47807>`_, Adam King)
* doc/cephadm: fix grammar in compatibility.rst (`pr#48715 <https://github.com/ceph/ceph/pull/48715>`_, Zac Dover)
* doc/cephadm: format airgap install procedure (`pr#49149 <https://github.com/ceph/ceph/pull/49149>`_, Zac Dover)
* doc/cephadm: improve airgapping procedure grammar (`pr#49158 <https://github.com/ceph/ceph/pull/49158>`_, Zac Dover)
* doc/cephadm: improve front matter (`pr#48607 <https://github.com/ceph/ceph/pull/48607>`_, Zac Dover)
* doc/cephadm: improve grammar in "listing hosts" (`pr#49165 <https://github.com/ceph/ceph/pull/49165>`_, Zac Dover)
* doc/cephadm: improve lone sentence (`pr#48738 <https://github.com/ceph/ceph/pull/48738>`_, Zac Dover)
* doc/cephadm: refine "Removing Hosts" (`pr#49707 <https://github.com/ceph/ceph/pull/49707>`_, Zac Dover)
* doc/cephadm: s/osd/OSD/ where appropriate (`pr#49718 <https://github.com/ceph/ceph/pull/49718>`_, Zac Dover)
* doc/cephadm: s/ssh/SSH/ in doc/cephadm (complete) (`pr#48612 <https://github.com/ceph/ceph/pull/48612>`_, Zac Dover)
* doc/cephadm: s/ssh/SSH/ in troubleshooting.rst (`pr#48602 <https://github.com/ceph/ceph/pull/48602>`_, Zac Dover)
* doc/cephadm: update install.rst (`pr#48595 <https://github.com/ceph/ceph/pull/48595>`_, Zac Dover)
* doc/cephfs - s/yet to here/yet to hear/ posix.rst (`pr#49449 <https://github.com/ceph/ceph/pull/49449>`_, Zac Dover)
* doc/cephfs/add-remove-mds: added cephadm note, refined "Adding an MDS" (`pr#45878 <https://github.com/ceph/ceph/pull/45878>`_, Dhairya Parmar)
* doc/cephfs: fix "e.g." in posix.rst (`pr#49451 <https://github.com/ceph/ceph/pull/49451>`_, Zac Dover)
* doc/cephfs: s/all of there are/all of these are/ (`pr#49447 <https://github.com/ceph/ceph/pull/49447>`_, Zac Dover)
* doc/conf.py: run ditaa with java (`pr#48906 <https://github.com/ceph/ceph/pull/48906>`_, Kefu Chai)
* doc/css: add "span" padding to custom.css (`pr#49694 <https://github.com/ceph/ceph/pull/49694>`_, Zac Dover)
* doc/css: add scroll-margin-top to dt elements (`pr#49640 <https://github.com/ceph/ceph/pull/49640>`_, Zac Dover)
* doc/css: Add scroll-margin-top to h2 html element (`pr#49662 <https://github.com/ceph/ceph/pull/49662>`_, Zac Dover)
* doc/css: add top-bar padding for h3 html element (`pr#49702 <https://github.com/ceph/ceph/pull/49702>`_, Zac Dover)
* doc/dev/cephadm: fix host maintenance enter/exit syntax (`pr#49647 <https://github.com/ceph/ceph/pull/49647>`_, Ranjini Mandyam Narasiodeyar)
* doc/dev/developer_guide/tests-unit-tests: Add unit test caveat (`pr#49013 <https://github.com/ceph/ceph/pull/49013>`_, Matan Breizman)
* doc/dev: add context note to dev guide config (`pr#46817 <https://github.com/ceph/ceph/pull/46817>`_, Zac Dover)
* doc/dev: add Dependabot section to essentials.rst (`pr#47043 <https://github.com/ceph/ceph/pull/47043>`_, Zac Dover)
* doc/dev: add explanation of how to use deduplication (`pr#48568 <https://github.com/ceph/ceph/pull/48568>`_, Myoungwon Oh)
* doc/dev: add IRC registration instructions (`pr#46939 <https://github.com/ceph/ceph/pull/46939>`_, Zac Dover)
* doc/dev: add submodule-update link to dev guide (`pr#48480 <https://github.com/ceph/ceph/pull/48480>`_, Zac Dover)
* doc/dev: alphabetize EC glossary (`pr#48686 <https://github.com/ceph/ceph/pull/48686>`_, Zac Dover)
* doc/dev: edit delayed-delete.rst (`pr#47050 <https://github.com/ceph/ceph/pull/47050>`_, Zac Dover)
* doc/dev: Elaborate on boost .deb creation (`pr#47416 <https://github.com/ceph/ceph/pull/47416>`_, David Galloway)
* doc/dev: fix graphviz diagram (`pr#48923 <https://github.com/ceph/ceph/pull/48923>`_, Zac Dover)
* doc/dev: improve Basic Workflow wording (`pr#49078 <https://github.com/ceph/ceph/pull/49078>`_, Zac Dover)
* doc/dev: improve EC glossary (`pr#48676 <https://github.com/ceph/ceph/pull/48676>`_, Zac Dover)
* doc/dev: improve lone sentence (`pr#48741 <https://github.com/ceph/ceph/pull/48741>`_, Zac Dover)
* doc/dev: improve presentation of note (git remote) (`pr#48236 <https://github.com/ceph/ceph/pull/48236>`_, Zac Dover)
* doc/dev: link to Dot User's Manual (`pr#48926 <https://github.com/ceph/ceph/pull/48926>`_, Zac Dover)
* doc/dev: refine erasure_coding.rst (`pr#48701 <https://github.com/ceph/ceph/pull/48701>`_, Zac Dover)
* doc/dev: remove deduplication.rst from pacific (`pr#48571 <https://github.com/ceph/ceph/pull/48571>`_, Zac Dover)
* doc/dev: s/github/GitHub/ in essentials.rst (`pr#47049 <https://github.com/ceph/ceph/pull/47049>`_, Zac Dover)
* doc/dev: s/master/main/ essentials.rst dev guide (`pr#46662 <https://github.com/ceph/ceph/pull/46662>`_, Zac Dover)
* doc/dev: s/master/main/ in basic workflow (`pr#46704 <https://github.com/ceph/ceph/pull/46704>`_, Zac Dover)
* doc/dev: s/master/main/ in title (`pr#46722 <https://github.com/ceph/ceph/pull/46722>`_, Zac Dover)
* doc/dev: s/the the/the/ in basic-workflow.rst (`pr#46934 <https://github.com/ceph/ceph/pull/46934>`_, Zac Dover)
* doc/dev: update basic-workflow.rst (`pr#46288 <https://github.com/ceph/ceph/pull/46288>`_, Zac Dover)
* doc/dev_guide: s/master/main in merging.rst (`pr#46710 <https://github.com/ceph/ceph/pull/46710>`_, Zac Dover)
* doc/glosary.rst: add "Ceph Block Device" term (`pr#48745 <https://github.com/ceph/ceph/pull/48745>`_, Zac Dover)
* doc/glossary - add "secrets" (`pr#49398 <https://github.com/ceph/ceph/pull/49398>`_, Zac Dover)
* doc/glossary.rst: add "Ceph Dashboard" term (`pr#48749 <https://github.com/ceph/ceph/pull/48749>`_, Zac Dover)
* doc/glossary.rst: alphabetize glossary terms (`pr#48339 <https://github.com/ceph/ceph/pull/48339>`_, Zac Dover)
* doc/glossary.rst: define "Ceph Manager" (`pr#48765 <https://github.com/ceph/ceph/pull/48765>`_, Zac Dover)
* doc/glossary.rst: remove duplicates (`pr#48358 <https://github.com/ceph/ceph/pull/48358>`_, Zac Dover)
* doc/glossary.rst: remove old front matter (`pr#48755 <https://github.com/ceph/ceph/pull/48755>`_, Zac Dover)
* doc/glossary: add "BlueStore" (`pr#48778 <https://github.com/ceph/ceph/pull/48778>`_, Zac Dover)
* doc/glossary: add "ceph monitor" entry (`pr#48448 <https://github.com/ceph/ceph/pull/48448>`_, Zac Dover)
* doc/glossary: add "Ceph Object Store" (`pr#49031 <https://github.com/ceph/ceph/pull/49031>`_, Zac Dover)
* doc/glossary: add "Dashboard Module" (`pr#49138 <https://github.com/ceph/ceph/pull/49138>`_, Zac Dover)
* doc/glossary: add "FQDN" entry (`pr#49425 <https://github.com/ceph/ceph/pull/49425>`_, Zac Dover)
* doc/glossary: add "mds" term (`pr#48872 <https://github.com/ceph/ceph/pull/48872>`_, Zac Dover)
* doc/glossary: add "RADOS Cluster" (`pr#49135 <https://github.com/ceph/ceph/pull/49135>`_, Zac Dover)
* doc/glossary: add "RADOS" definition (`pr#48951 <https://github.com/ceph/ceph/pull/48951>`_, Zac Dover)
* doc/glossary: Add "SDS" (`pr#48977 <https://github.com/ceph/ceph/pull/48977>`_, Zac Dover)
* doc/glossary: add DAS (`pr#49255 <https://github.com/ceph/ceph/pull/49255>`_, Zac Dover)
* doc/glossary: add matter to "RBD" (`pr#49266 <https://github.com/ceph/ceph/pull/49266>`_, Zac Dover)
* doc/glossary: add oxford comma to "Cluster Map" (`pr#48993 <https://github.com/ceph/ceph/pull/48993>`_, Zac Dover)
* doc/glossary: beef up "Ceph Block Storage" (`pr#48965 <https://github.com/ceph/ceph/pull/48965>`_, Zac Dover)
* doc/glossary: capitalize "DAS" correctly (`pr#49604 <https://github.com/ceph/ceph/pull/49604>`_, Zac Dover)
* doc/glossary: clean OSD id-related entries (`pr#49590 <https://github.com/ceph/ceph/pull/49590>`_, Zac Dover)
* doc/glossary: Clean up "Ceph Object Storage" (`pr#49668 <https://github.com/ceph/ceph/pull/49668>`_, Zac Dover)
* doc/glossary: collate "releases" entries (`pr#49601 <https://github.com/ceph/ceph/pull/49601>`_, Zac Dover)
* doc/glossary: Define "Ceph Node" (`pr#48995 <https://github.com/ceph/ceph/pull/48995>`_, Zac Dover)
* doc/glossary: define "Ceph Object Gateway" (`pr#48902 <https://github.com/ceph/ceph/pull/48902>`_, Zac Dover)
* doc/glossary: define "Ceph OSD" (`pr#48771 <https://github.com/ceph/ceph/pull/48771>`_, Zac Dover)
* doc/glossary: define "Ceph Storage Cluster" (`pr#49003 <https://github.com/ceph/ceph/pull/49003>`_, Zac Dover)
* doc/glossary: define "OSD" (`pr#48760 <https://github.com/ceph/ceph/pull/48760>`_, Zac Dover)
* doc/glossary: define "RGW" (`pr#48961 <https://github.com/ceph/ceph/pull/48961>`_, Zac Dover)
* doc/glossary: disambiguate "OSD" (`pr#48791 <https://github.com/ceph/ceph/pull/48791>`_, Zac Dover)
* doc/glossary: disambiguate clauses (`pr#49575 <https://github.com/ceph/ceph/pull/49575>`_, Zac Dover)
* doc/glossary: fix "Ceph Client" (`pr#49033 <https://github.com/ceph/ceph/pull/49033>`_, Zac Dover)
* doc/glossary: improve "Ceph Manager Dashboard" (`pr#48825 <https://github.com/ceph/ceph/pull/48825>`_, Zac Dover)
* doc/glossary: improve "Ceph Manager" term (`pr#48812 <https://github.com/ceph/ceph/pull/48812>`_, Zac Dover)
* doc/glossary: improve "Ceph Point Release" entry (`pr#48891 <https://github.com/ceph/ceph/pull/48891>`_, Zac Dover)
* doc/glossary: improve "ceph" term (`pr#48821 <https://github.com/ceph/ceph/pull/48821>`_, Zac Dover)
* doc/glossary: improve wording (`pr#48752 <https://github.com/ceph/ceph/pull/48752>`_, Zac Dover)
* doc/glossary: link to "Ceph Manager" (`pr#49064 <https://github.com/ceph/ceph/pull/49064>`_, Zac Dover)
* doc/glossary: link to OSD material (`pr#48785 <https://github.com/ceph/ceph/pull/48785>`_, Zac Dover)
* doc/glossary: redirect entries to "Ceph OSD" (`pr#48834 <https://github.com/ceph/ceph/pull/48834>`_, Zac Dover)
* doc/glossary: remove "Ceph System" (`pr#49073 <https://github.com/ceph/ceph/pull/49073>`_, Zac Dover)
* doc/glossary: remove "Ceph Test Framework" (`pr#48842 <https://github.com/ceph/ceph/pull/48842>`_, Zac Dover)
* doc/glossary: rewrite "Ceph File System" (`pr#48918 <https://github.com/ceph/ceph/pull/48918>`_, Zac Dover)
* doc/glossary: s/an/each/ where it's needed (`pr#49596 <https://github.com/ceph/ceph/pull/49596>`_, Zac Dover)
* doc/glossary: s/Ceph System/Ceph Cluster/ (`pr#49081 <https://github.com/ceph/ceph/pull/49081>`_, Zac Dover)
* doc/glossary: s/comprising/consisting of/ (`pr#49019 <https://github.com/ceph/ceph/pull/49019>`_, Zac Dover)
* doc/glossary: update "Cluster Map" (`pr#48798 <https://github.com/ceph/ceph/pull/48798>`_, Zac Dover)
* doc/glossary: update "pool/pools" (`pr#48858 <https://github.com/ceph/ceph/pull/48858>`_, Zac Dover)
* doc/index.rst: add link to Dev Guide basic workfl (`pr#46903 <https://github.com/ceph/ceph/pull/46903>`_, Zac Dover)
* doc/install: clone-source.rst s/master/main (`pr#48381 <https://github.com/ceph/ceph/pull/48381>`_, Zac Dover)
* doc/install: improve updating submodules procedure (`pr#48465 <https://github.com/ceph/ceph/pull/48465>`_, Zac Dover)
* doc/install: update "Official Releases" sources (`pr#49039 <https://github.com/ceph/ceph/pull/49039>`_, Zac Dover)
* doc/install: update clone-source.rst (`pr#49378 <https://github.com/ceph/ceph/pull/49378>`_, Zac Dover)
* doc/man/ceph-rbdnamer: remove obsolete udev rule (`pr#49696 <https://github.com/ceph/ceph/pull/49696>`_, Ilya Dryomov)
* doc/man/rbd: Mention changed `bluestore_min_alloc_size` (`pr#47578 <https://github.com/ceph/ceph/pull/47578>`_, Niklas Hambüchen)
* doc/man: define --num-rep, --min-rep and --max-rep (`pr#49660 <https://github.com/ceph/ceph/pull/49660>`_, Zac Dover)
* doc/mgr: add prompt directives to dashboard.rst (`pr#47823 <https://github.com/ceph/ceph/pull/47823>`_, Zac Dover)
* doc/mgr: edit orchestrator.rst (`pr#47781 <https://github.com/ceph/ceph/pull/47781>`_, Zac Dover)
* doc/mgr: name data source in "Man Install & Config" (`pr#48371 <https://github.com/ceph/ceph/pull/48371>`_, Zac Dover)
* doc/mgr: update prompts in dboard.rst includes (`pr#47870 <https://github.com/ceph/ceph/pull/47870>`_, Zac Dover)
* doc/monitoring: add min vers of apps in mon stack (`pr#48062 <https://github.com/ceph/ceph/pull/48062>`_, Zac Dover, Himadri Maheshwari)
* doc/osd: Fixes the introduction for writeback mode of cache tier (`pr#48883 <https://github.com/ceph/ceph/pull/48883>`_, Mingyuan Liang)
* doc/rados/operations: add prompts to operating.rst (`pr#47587 <https://github.com/ceph/ceph/pull/47587>`_, Zac Dover)
* doc/rados: add prompts to monitoring-osd-pg.rst (`pr#49240 <https://github.com/ceph/ceph/pull/49240>`_, Zac Dover)
* doc/rados: add prompts to add-or-remove-osds (`pr#49071 <https://github.com/ceph/ceph/pull/49071>`_, Zac Dover)
* doc/rados: add prompts to add-or-rm-prompts.rst (`pr#48986 <https://github.com/ceph/ceph/pull/48986>`_, Zac Dover)
* doc/rados: add prompts to add-or-rm-prompts.rst (`pr#48980 <https://github.com/ceph/ceph/pull/48980>`_, Zac Dover)
* doc/rados: add prompts to auth-config-ref.rst (`pr#49516 <https://github.com/ceph/ceph/pull/49516>`_, Zac Dover)
* doc/rados: add prompts to balancer.rst (`pr#49112 <https://github.com/ceph/ceph/pull/49112>`_, Zac Dover)
* doc/rados: add prompts to bluestore-config-ref.rst (`pr#49536 <https://github.com/ceph/ceph/pull/49536>`_, Zac Dover)
* doc/rados: add prompts to bluestore-migration.rst (`pr#49123 <https://github.com/ceph/ceph/pull/49123>`_, Zac Dover)
* doc/rados: add prompts to cache-tiering.rst (`pr#49125 <https://github.com/ceph/ceph/pull/49125>`_, Zac Dover)
* doc/rados: add prompts to ceph-conf.rst (`pr#49493 <https://github.com/ceph/ceph/pull/49493>`_, Zac Dover)
* doc/rados: add prompts to change-mon-elections.rst (`pr#49130 <https://github.com/ceph/ceph/pull/49130>`_, Zac Dover)
* doc/rados: add prompts to control.rst (`pr#49128 <https://github.com/ceph/ceph/pull/49128>`_, Zac Dover)
* doc/rados: add prompts to crush-map.rst (`pr#49184 <https://github.com/ceph/ceph/pull/49184>`_, Zac Dover)
* doc/rados: add prompts to devices.rst (`pr#49188 <https://github.com/ceph/ceph/pull/49188>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-clay.rst (`pr#49206 <https://github.com/ceph/ceph/pull/49206>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-isa (`pr#49208 <https://github.com/ceph/ceph/pull/49208>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-jerasure.rst (`pr#49210 <https://github.com/ceph/ceph/pull/49210>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-lrc.rst (`pr#49219 <https://github.com/ceph/ceph/pull/49219>`_, Zac Dover)
* doc/rados: add prompts to erasure-code-shec.rst (`pr#49221 <https://github.com/ceph/ceph/pull/49221>`_, Zac Dover)
* doc/rados: add prompts to health-checks (1 of 5) (`pr#49223 <https://github.com/ceph/ceph/pull/49223>`_, Zac Dover)
* doc/rados: add prompts to health-checks (2 of 5) (`pr#49225 <https://github.com/ceph/ceph/pull/49225>`_, Zac Dover)
* doc/rados: add prompts to health-checks (3 of 5) (`pr#49227 <https://github.com/ceph/ceph/pull/49227>`_, Zac Dover)
* doc/rados: add prompts to health-checks (4 of 5) (`pr#49229 <https://github.com/ceph/ceph/pull/49229>`_, Zac Dover)
* doc/rados: add prompts to health-checks (5 of 5) (`pr#49231 <https://github.com/ceph/ceph/pull/49231>`_, Zac Dover)
* doc/rados: add prompts to librados-intro.rst (`pr#49552 <https://github.com/ceph/ceph/pull/49552>`_, Zac Dover)
* doc/rados: add prompts to monitoring.rst (`pr#49245 <https://github.com/ceph/ceph/pull/49245>`_, Zac Dover)
* doc/rados: add prompts to msgr2.rst (`pr#49512 <https://github.com/ceph/ceph/pull/49512>`_, Zac Dover)
* doc/rados: add prompts to pg-repair.rst (`pr#49247 <https://github.com/ceph/ceph/pull/49247>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (`pr#49274 <https://github.com/ceph/ceph/pull/49274>`_, Zac Dover)
* doc/rados: add prompts to placement-groups.rst (3) (`pr#49276 <https://github.com/ceph/ceph/pull/49276>`_, Zac Dover)
* doc/rados: add prompts to pools.rst (`pr#48060 <https://github.com/ceph/ceph/pull/48060>`_, Zac Dover)
* doc/rados: add prompts to stretch-mode.rst (`pr#49370 <https://github.com/ceph/ceph/pull/49370>`_, Zac Dover)
* doc/rados: add prompts to upmap.rst (`pr#49372 <https://github.com/ceph/ceph/pull/49372>`_, Zac Dover)
* doc/rados: add prompts to user-management.rst (`pr#49385 <https://github.com/ceph/ceph/pull/49385>`_, Zac Dover)
* doc/rados: clarify default EC pool from simplest (`pr#49469 <https://github.com/ceph/ceph/pull/49469>`_, Zac Dover)
* doc/rados: cleanup "erasure code profiles" (`pr#49051 <https://github.com/ceph/ceph/pull/49051>`_, Zac Dover)
* doc/rados: correct typo in python.rst (`pr#49560 <https://github.com/ceph/ceph/pull/49560>`_, Zac Dover)
* doc/rados: fix grammar in configuration/index.rst (`pr#48885 <https://github.com/ceph/ceph/pull/48885>`_, Zac Dover)
* doc/rados: fix prompts in erasure-code.rst (`pr#48335 <https://github.com/ceph/ceph/pull/48335>`_, Zac Dover)
* doc/rados: improve pools.rst (`pr#48868 <https://github.com/ceph/ceph/pull/48868>`_, Zac Dover)
* doc/rados: link to cephadm replacing osd section (`pr#49681 <https://github.com/ceph/ceph/pull/49681>`_, Zac Dover)
* doc/rados: move colon (`pr#49705 <https://github.com/ceph/ceph/pull/49705>`_, Zac Dover)
* doc/rados: refine English in crush-map-edits.rst (`pr#48366 <https://github.com/ceph/ceph/pull/48366>`_, Zac Dover)
* doc/rados: remove prompt from php.ini line (`pr#49562 <https://github.com/ceph/ceph/pull/49562>`_, Zac Dover)
* doc/rados: reword part of cache-tiering.rst (`pr#48888 <https://github.com/ceph/ceph/pull/48888>`_, Zac Dover)
* doc/rados: rewrite EC intro (`pr#48324 <https://github.com/ceph/ceph/pull/48324>`_, Zac Dover)
* doc/rados: s/backend/back end/ (`pr#48782 <https://github.com/ceph/ceph/pull/48782>`_, Zac Dover)
* doc/rados: update "Pools" material (`pr#48856 <https://github.com/ceph/ceph/pull/48856>`_, Zac Dover)
* doc/rados: update bluestore-config-ref.rst (`pr#46485 <https://github.com/ceph/ceph/pull/46485>`_, Zac Dover)
* doc/rados: update prompts in crush-map-edits.rst (`pr#48364 <https://github.com/ceph/ceph/pull/48364>`_, Zac Dover)
* doc/rados: update prompts in network-config-ref (`pr#48158 <https://github.com/ceph/ceph/pull/48158>`_, Zac Dover)
* doc/radosgw: add prompts to multisite.rst (`pr#48660 <https://github.com/ceph/ceph/pull/48660>`_, Zac Dover)
* doc/radosgw: add push_endpoint for rabbitmq (`pr#48488 <https://github.com/ceph/ceph/pull/48488>`_, Zac Dover)
* doc/radosgw: improve "Ceph Object Gateway" text (`pr#48864 <https://github.com/ceph/ceph/pull/48864>`_, Zac Dover)
* doc/radosgw: improve grammar - notifications.rst (`pr#48495 <https://github.com/ceph/ceph/pull/48495>`_, Zac Dover)
* doc/radosgw: refine "bucket notifications" (`pr#48562 <https://github.com/ceph/ceph/pull/48562>`_, Zac Dover)
* doc/radosgw: refine "notification reliability" (`pr#48530 <https://github.com/ceph/ceph/pull/48530>`_, Zac Dover)
* doc/radosgw: refine "notifications" and "events" (`pr#48580 <https://github.com/ceph/ceph/pull/48580>`_, Zac Dover)
* doc/radosgw: refine notifications.rst - top part (`pr#48503 <https://github.com/ceph/ceph/pull/48503>`_, Zac Dover)
* doc/radosgw: update notifications.rst - grammar (`pr#48500 <https://github.com/ceph/ceph/pull/48500>`_, Zac Dover)
* doc/radosgw: Uppercase s3 (`pr#47360 <https://github.com/ceph/ceph/pull/47360>`_, Anthony D'Atri)
* doc/radosw: improve radosgw text (`pr#48967 <https://github.com/ceph/ceph/pull/48967>`_, Zac Dover)
* doc/radowsgw: add prompts to notifications.rst (`pr#48536 <https://github.com/ceph/ceph/pull/48536>`_, Zac Dover)
* doc/rbd: improve grammar in "immutable object..." (`pr#48970 <https://github.com/ceph/ceph/pull/48970>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device Pool" (`pr#49308 <https://github.com/ceph/ceph/pull/49308>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device User" (`pr#49319 <https://github.com/ceph/ceph/pull/49319>`_, Zac Dover)
* doc/rbd: refine "Create a Block Device User" (`pr#49301 <https://github.com/ceph/ceph/pull/49301>`_, Zac Dover)
* doc/rbd: refine "Creating a Block Device Image" (`pr#49347 <https://github.com/ceph/ceph/pull/49347>`_, Zac Dover)
* doc/rbd: refine "Listing Block Device Images" (`pr#49349 <https://github.com/ceph/ceph/pull/49349>`_, Zac Dover)
* doc/rbd: refine "Removing a Block Device Image" (`pr#49357 <https://github.com/ceph/ceph/pull/49357>`_, Zac Dover)
* doc/rbd: refine "Resizing a Block Device Image" (`pr#49353 <https://github.com/ceph/ceph/pull/49353>`_, Zac Dover)
* doc/rbd: refine "Restoring a Block Device Image" (`pr#49355 <https://github.com/ceph/ceph/pull/49355>`_, Zac Dover)
* doc/rbd: refine "Retrieving Image Information" (`pr#49351 <https://github.com/ceph/ceph/pull/49351>`_, Zac Dover)
* doc/rbd: refine rbd-exclusive-locks.rst (`pr#49598 <https://github.com/ceph/ceph/pull/49598>`_, Zac Dover)
* doc/rbd: refine rbd-snapshot.rst (`pr#49485 <https://github.com/ceph/ceph/pull/49485>`_, Zac Dover)
* doc/rbd: remove typo and ill-formed command (`pr#49366 <https://github.com/ceph/ceph/pull/49366>`_, Zac Dover)
* doc/rbd: s/wuold/would/ in rados-rbd-cmds.rst (`pr#49592 <https://github.com/ceph/ceph/pull/49592>`_, Zac Dover)
* doc/rbd: update iSCSI gateway info (`pr#49069 <https://github.com/ceph/ceph/pull/49069>`_, Zac Dover)
* doc/releases: improve grammar in pacific.rst (`pr#48426 <https://github.com/ceph/ceph/pull/48426>`_, Zac Dover)
* doc/releases: update pacific release notes (`pr#48404 <https://github.com/ceph/ceph/pull/48404>`_, Zac Dover)
* doc/security: improve grammar in CVE-2022-0670.rst (`pr#48431 <https://github.com/ceph/ceph/pull/48431>`_, Zac Dover)
* doc/start: add Anthony D'Atri's suggestions (`pr#49616 <https://github.com/ceph/ceph/pull/49616>`_, Zac Dover)
* doc/start: add link-related metadocumentation (`pr#49607 <https://github.com/ceph/ceph/pull/49607>`_, Zac Dover)
* doc/start: alphabetize hardware-recs links (`pr#46340 <https://github.com/ceph/ceph/pull/46340>`_, Zac Dover)
* doc/start: improve documenting-ceph.rst (`pr#49566 <https://github.com/ceph/ceph/pull/49566>`_, Zac Dover)
* doc/start: make OSD and MDS structures parallel (`pr#46656 <https://github.com/ceph/ceph/pull/46656>`_, Zac Dover)
* doc/start: Polish network section of hardware-recommendations.rst (`pr#46663 <https://github.com/ceph/ceph/pull/46663>`_, Anthony D'Atri)
* doc/start: refine "Quirks of RST" (`pr#49611 <https://github.com/ceph/ceph/pull/49611>`_, Zac Dover)
* doc/start: rewrite CRUSH para (`pr#46657 <https://github.com/ceph/ceph/pull/46657>`_, Zac Dover)
* doc/start: rewrite hardware-recs networks section (`pr#46653 <https://github.com/ceph/ceph/pull/46653>`_, Zac Dover)
* doc/start: s/3/three/ in intro.rst (`pr#46326 <https://github.com/ceph/ceph/pull/46326>`_, Zac Dover)
* doc/start: update documenting-ceph branch names (`pr#47956 <https://github.com/ceph/ceph/pull/47956>`_, Zac Dover)
* doc/start: update documenting-ceph.rst (`pr#49571 <https://github.com/ceph/ceph/pull/49571>`_, Zac Dover)
* doc/start: update hardware recs (`pr#47122 <https://github.com/ceph/ceph/pull/47122>`_, Zac Dover)
* doc/various: update link to CRUSH pdf (`pr#48403 <https://github.com/ceph/ceph/pull/48403>`_, Zac Dover)
* doc: add disk benchmarking and cache recommendations (`pr#46348 <https://github.com/ceph/ceph/pull/46348>`_, Dan van der Ster)
* doc: backport pacific release notes into pacific branch (`pr#46484 <https://github.com/ceph/ceph/pull/46484>`_, Zac Dover, David Galloway)
* doc: Change 'ReST' to 'REST' in doc/radosgw/layout.rst (`pr#48654 <https://github.com/ceph/ceph/pull/48654>`_, wangyingbin)
* doc: fix a couple grammatical things (`pr#49622 <https://github.com/ceph/ceph/pull/49622>`_, Brad Fitzpatrick)
* doc: fix a typo (`pr#49684 <https://github.com/ceph/ceph/pull/49684>`_, Brad Fitzpatrick)
* doc: Install graphviz (`pr#48905 <https://github.com/ceph/ceph/pull/48905>`_, David Galloway)
* doc: point to main branch for release info (`pr#48958 <https://github.com/ceph/ceph/pull/48958>`_, Patrick Donnelly)
* doc: Update release process doc to accurately reflect current process (`pr#47838 <https://github.com/ceph/ceph/pull/47838>`_, David Galloway)
* docs/start: fixes typo and empty headline in hardware recommendation … (`pr#48392 <https://github.com/ceph/ceph/pull/48392>`_, Sebastian Schmid)
* docs: correct add system user to the master zone command (`pr#48656 <https://github.com/ceph/ceph/pull/48656>`_, Salar Nosrati-Ershad)
* docs: fix doc link pointing to master in dashboard.rst (`pr#47791 <https://github.com/ceph/ceph/pull/47791>`_, Nizamudeen A)
* Fix data corruption in bluefs truncate() (`pr#45171 <https://github.com/ceph/ceph/pull/45171>`_, Adam Kupczyk)
* fsmap: switch to using iterator based loop (`pr#48269 <https://github.com/ceph/ceph/pull/48269>`_, Aliaksei Makarau)
* Implement CIDR blocklisting (`pr#46470 <https://github.com/ceph/ceph/pull/46470>`_, Jos Collin, Greg Farnum)
* include/buffer: include <memory> (`pr#47295 <https://github.com/ceph/ceph/pull/47295>`_, Kefu Chai, Duncan Bellamy)
* include: fix IS_ERR on Windows (`pr#47923 <https://github.com/ceph/ceph/pull/47923>`_, Lucian Petrut)
* libcephfs: define AT_NO_ATTR_SYNC back for backward compatibility (`pr#47862 <https://github.com/ceph/ceph/pull/47862>`_, Xiubo Li)
* libcephsqlite: ceph-mgr crashes when compiled with gcc12 (`pr#47271 <https://github.com/ceph/ceph/pull/47271>`_, Ganesh Maharaj Mahalingam)
* librados/watch_notify: reconnect after socket injection (`pr#46499 <https://github.com/ceph/ceph/pull/46499>`_, Nitzan Mordechai)
* librados: rados_ioctx_destroy check for initialized ioctx (`pr#47451 <https://github.com/ceph/ceph/pull/47451>`_, Nitzan Mordechai)
* librbd/cache/pwl: fix clean vs bytes_dirty cache state inconsistency (`pr#49054 <https://github.com/ceph/ceph/pull/49054>`_, Yin Congmin)
* librbd/cache/pwl: fix endianness issue (`pr#46815 <https://github.com/ceph/ceph/pull/46815>`_, Yin Congmin)
* librbd/cache/pwl: narrow the scope of m_lock in write_image_cache_state() (`pr#47939 <https://github.com/ceph/ceph/pull/47939>`_, Ilya Dryomov, Yin Congmin)
* librbd: bail from schedule_request_lock() if already lock owner (`pr#47161 <https://github.com/ceph/ceph/pull/47161>`_, Christopher Hoffman)
* librbd: retry ENOENT in V2_REFRESH_PARENT as well (`pr#47995 <https://github.com/ceph/ceph/pull/47995>`_, Ilya Dryomov)
* librbd: tweak misleading "image is still primary" error message (`pr#47247 <https://github.com/ceph/ceph/pull/47247>`_, Ilya Dryomov)
* librbd: unlink newest mirror snapshot when at capacity, bump capacity (`pr#46593 <https://github.com/ceph/ceph/pull/46593>`_, Ilya Dryomov)
* librbd: update progress for non-existent objects on deep-copy (`pr#46909 <https://github.com/ceph/ceph/pull/46909>`_, Ilya Dryomov)
* librbd: use actual monitor addresses when creating a peer bootstrap token (`pr#47911 <https://github.com/ceph/ceph/pull/47911>`_, Ilya Dryomov)
* make-dist: patch boost source to support python 3.10  … (`pr#47027 <https://github.com/ceph/ceph/pull/47027>`_, Tim Serong, Kefu Chai)
* mds: increment directory inode's change attr by one (`pr#48521 <https://github.com/ceph/ceph/pull/48521>`_, Ramana Raja)
* mds: clear MDCache::rejoin\_\*_q queues before recovering file inodes (`pr#46682 <https://github.com/ceph/ceph/pull/46682>`_, Xiubo Li)
* mds: flush mdlog if locked and still has wanted caps not satisfied (`pr#46423 <https://github.com/ceph/ceph/pull/46423>`_, Xiubo Li)
* mds: reset heartbeat when fetching or committing entries (`pr#46180 <https://github.com/ceph/ceph/pull/46180>`_, Xiubo Li)
* mds: trigger to flush the mdlog in handle_find_ino() (`pr#46424 <https://github.com/ceph/ceph/pull/46424>`_, Xiubo Li)
* mds/client: fail the request if the peer MDS doesn't support getvxattr op (`pr#47891 <https://github.com/ceph/ceph/pull/47891>`_, Xiubo Li, Zack Cerza)
* mds/Server: Do not abort MDS on unknown messages (`pr#48253 <https://github.com/ceph/ceph/pull/48253>`_, Dhairya Parmar, Dhairy Parmar)
* mds: add a perf counter to record slow replies (`pr#46138 <https://github.com/ceph/ceph/pull/46138>`_, haoyixing)
* mds: damage table only stores one dentry per dirfrag (`pr#48262 <https://github.com/ceph/ceph/pull/48262>`_, Patrick Donnelly)
* mds: do not assert early on when issuing client leases (`issue#54701 <http://tracker.ceph.com/issues/54701>`_, `pr#46567 <https://github.com/ceph/ceph/pull/46567>`_, Venky Shankar)
* mds: Don't blocklist clients in any replay state (`pr#47111 <https://github.com/ceph/ceph/pull/47111>`_, Kotresh HR)
* mds: fix crash when exporting unlinked dir (`pr#47180 <https://github.com/ceph/ceph/pull/47180>`_, 胡玮文)
* mds: include encoded stray inode when sending dentry unlink message to replicas (`issue#54046 <http://tracker.ceph.com/issues/54046>`_, `pr#46183 <https://github.com/ceph/ceph/pull/46183>`_, Venky Shankar)
* mds: notify the xattr_version to replica MDSes (`pr#47056 <https://github.com/ceph/ceph/pull/47056>`_, Xiubo Li)
* mds: skip fetching the dirfrags if not a directory (`pr#47433 <https://github.com/ceph/ceph/pull/47433>`_, Xiubo Li)
* mds: standby-replay daemon always removed in MDSMonitor::prepare_beacon (`pr#47282 <https://github.com/ceph/ceph/pull/47282>`_, Patrick Donnelly)
* mds: switch to use projected inode instead (`pr#47059 <https://github.com/ceph/ceph/pull/47059>`_, Xiubo Li)
* mds: wait unlink to finish to avoid conflict when creating same entries (`pr#48453 <https://github.com/ceph/ceph/pull/48453>`_, Xiubo Li)
* mgr, mgr/prometheus: Fix regression with prometheus metrics (`pr#47693 <https://github.com/ceph/ceph/pull/47693>`_, Prashant D)
* mgr, mgr/prometheus: Fix regression with prometheus metrics (`pr#46429 <https://github.com/ceph/ceph/pull/46429>`_, Prashant D)
* mgr, mon: Keep upto date metadata with mgr for MONs (`pr#47692 <https://github.com/ceph/ceph/pull/47692>`_, Laura Flores, Prashant D)
* mgr, mon: Keep upto date metadata with mgr for MONs (`pr#46427 <https://github.com/ceph/ceph/pull/46427>`_, Prashant D)
* mgr/ActivePyModules.cc: fix cases where GIL is held while attempting to lock mutex (`pr#46302 <https://github.com/ceph/ceph/pull/46302>`_, Cory Snyder)
* mgr/cephadm: Add disk rescan feature to the orchestrator (`pr#47372 <https://github.com/ceph/ceph/pull/47372>`_, Adam King, Paul Cuzner)
* mgr/cephadm: adding logic to close ports when removing a daemon (`pr#46780 <https://github.com/ceph/ceph/pull/46780>`_, Redouane Kachach)
* mgr/cephadm: Adding logic to store grafana cert/key per node (`pr#48103 <https://github.com/ceph/ceph/pull/48103>`_, Redouane Kachach)
* mgr/cephadm: allow setting prometheus retention time (`pr#48100 <https://github.com/ceph/ceph/pull/48100>`_, Adam King)
* mgr/cephadm: capture exception when not able to list upgrade tags (`pr#46776 <https://github.com/ceph/ceph/pull/46776>`_, Redouane Kachach)
* mgr/cephadm: check if a service exists before trying to restart it (`pr#46779 <https://github.com/ceph/ceph/pull/46779>`_, Redouane Kachach)
* mgr/cephadm: clear error message when resuming upgrade (`pr#47375 <https://github.com/ceph/ceph/pull/47375>`_, Adam King)
* mgr/cephadm: don't redeploy osds seen in raw list if cephadm knows them (`pr#46545 <https://github.com/ceph/ceph/pull/46545>`_, Adam King)
* mgr/cephadm: fixing scheduler consistent hashing (`pr#46975 <https://github.com/ceph/ceph/pull/46975>`_, Redouane Kachach)
* mgr/cephadm: Raw OSD Support (`pr#45964 <https://github.com/ceph/ceph/pull/45964>`_, Guillaume Abrioux, Adam King, Sage Weil)
* mgr/cephadm: reconfig iscsi daemons if trusted_ip_list changes (`pr#48096 <https://github.com/ceph/ceph/pull/48096>`_, Adam King)
* mgr/cephadm: recreate osd config when redeploy/reconfiguring (`pr#47663 <https://github.com/ceph/ceph/pull/47663>`_, Adam King)
* mgr/cephadm: set dashboard grafana-api-password when user provides one (`pr#47662 <https://github.com/ceph/ceph/pull/47662>`_, Adam King)
* mgr/cephadm: staggered upgrade (`pr#46359 <https://github.com/ceph/ceph/pull/46359>`_, Adam King)
* mgr/cephadm: try to get FQDN for active instance (`pr#46775 <https://github.com/ceph/ceph/pull/46775>`_, Tatjana Dehler)
* mgr/cephadm: use host shortname for osd memory autotuning (`pr#46556 <https://github.com/ceph/ceph/pull/46556>`_, Adam King)
* mgr/dashboard:  don't log 3xx as errors (`pr#46461 <https://github.com/ceph/ceph/pull/46461>`_, Ernesto Puerta)
* mgr/dashboard:  WDC multipath bug fixes (`pr#46456 <https://github.com/ceph/ceph/pull/46456>`_, Nizamudeen A)
* mgr/dashboard: Add details to the modal which displays the `safe-to-d… (`pr#48176 <https://github.com/ceph/ceph/pull/48176>`_, Francesco Torchia)
* mgr/dashboard: add option to resolve ip addr (`pr#48220 <https://github.com/ceph/ceph/pull/48220>`_, Tatjana Dehler)
* mgr/dashboard: add required validation for frontend and monitor port (`pr#47357 <https://github.com/ceph/ceph/pull/47357>`_, Avan Thakkar)
* mgr/dashboard: Add text to empty life expectancy column (`pr#48276 <https://github.com/ceph/ceph/pull/48276>`_, Francesco Torchia)
* mgr/dashboard: allow cross origin when the url is set (`pr#49151 <https://github.com/ceph/ceph/pull/49151>`_, Nizamudeen A)
* mgr/dashboard: allow Origin url for CORS if present in config (`pr#49429 <https://github.com/ceph/ceph/pull/49429>`_, Avan Thakkar)
* mgr/dashboard: batch rbd-mirror backports (`pr#46531 <https://github.com/ceph/ceph/pull/46531>`_, Pere Diaz Bou, Pedro Gonzalez Gomez, Nizamudeen A, Melissa Li, Sarthak0702, Avan Thakkar, Aashish Sharma)
* mgr/dashboard: BDD approach for the dashboard cephadm e2e (`pr#46529 <https://github.com/ceph/ceph/pull/46529>`_, Nizamudeen A)
* mgr/dashboard: bug fixes for rbd mirroring edit and promotion/demotion (`pr#48806 <https://github.com/ceph/ceph/pull/48806>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: bump moment from 2.29.1 to 2.29.3 in /src/pybind/mgr/dashboard/frontend (`pr#46717 <https://github.com/ceph/ceph/pull/46717>`_, dependabot[bot])
* mgr/dashboard: bump up teuthology (`pr#47497 <https://github.com/ceph/ceph/pull/47497>`_, Kefu Chai)
* mgr/dashboard: Creating and editing Prometheus AlertManager silences is buggy (`pr#46277 <https://github.com/ceph/ceph/pull/46277>`_, Volker Theile)
* mgr/dashboard: customizable log-in page text/banner (`pr#46343 <https://github.com/ceph/ceph/pull/46343>`_, Sarthak0702)
* mgr/dashboard: dashboard help command showing wrong syntax for login-banner (`pr#46810 <https://github.com/ceph/ceph/pull/46810>`_, Sarthak0702)
* mgr/dashboard: display helpfull message when the iframe-embedded Grafana dashboard failed to load (`pr#47008 <https://github.com/ceph/ceph/pull/47008>`_, Ngwa Sedrick Meh)
* mgr/dashboard: do not recommend throughput for ssd's only cluster (`pr#47155 <https://github.com/ceph/ceph/pull/47155>`_, Nizamudeen A)
* mgr/dashboard: don't log tracebacks on 404s (`pr#47093 <https://github.com/ceph/ceph/pull/47093>`_, Ernesto Puerta)
* mgr/dashboard: enable addition of custom Prometheus alerts (`pr#48099 <https://github.com/ceph/ceph/pull/48099>`_, Patrick Seidensal)
* mgr/dashboard: ensure limit 0 returns 0 images (`pr#47888 <https://github.com/ceph/ceph/pull/47888>`_, Pere Diaz Bou)
* mgr/dashboard: Feature 54330 osd creation workflow (`pr#46690 <https://github.com/ceph/ceph/pull/46690>`_, Pere Diaz Bou, Nizamudeen A, Sarthak0702)
* mgr/dashboard: fix _rbd_image_refs caching (`pr#47636 <https://github.com/ceph/ceph/pull/47636>`_, Pere Diaz Bou)
* mgr/dashboard: fix Expected to find element: `cd-modal .badge but never found it (`pr#48142 <https://github.com/ceph/ceph/pull/48142>`_, Nizamudeen A)
* mgr/dashboard: fix nfs exports form issues with squash field (`pr#47960 <https://github.com/ceph/ceph/pull/47960>`_, Nizamudeen A)
* mgr/dashboard: fix openapi-check (`pr#48045 <https://github.com/ceph/ceph/pull/48045>`_, Pere Diaz Bou)
* mgr/dashboard: fix rgw connect when using ssl (`issue#56970 <http://tracker.ceph.com/issues/56970>`_, `pr#48189 <https://github.com/ceph/ceph/pull/48189>`_, Henry Hirsch)
* mgr/dashboard: fix snapshot creation with duplicate name (`pr#48048 <https://github.com/ceph/ceph/pull/48048>`_, Aashish Sharma)
* mgr/dashboard: fix ssl cert validation for ingress service creation (`pr#46204 <https://github.com/ceph/ceph/pull/46204>`_, Avan Thakkar)
* mgr/dashboard: fix unmanaged service creation (`pr#48026 <https://github.com/ceph/ceph/pull/48026>`_, Nizamudeen A)
* mgr/dashboard: fix wrong pg status processing (`pr#46228 <https://github.com/ceph/ceph/pull/46228>`_, Ernesto Puerta)
* mgr/dashboard: form field validation icons overlap with other icons (`pr#46379 <https://github.com/ceph/ceph/pull/46379>`_, Sarthak0702)
* mgr/dashboard: grafana frontend e2e testing and update cypress (`pr#47721 <https://github.com/ceph/ceph/pull/47721>`_, Nizamudeen A)
* mgr/dashboard: handle the cephfs permission issue in nfs exports (`pr#48316 <https://github.com/ceph/ceph/pull/48316>`_, Nizamudeen A)
* mgr/dashboard: host list tables doesn't show all services deployed (`pr#47454 <https://github.com/ceph/ceph/pull/47454>`_, Avan Thakkar)
* mgr/dashboard: ingress backend service should list all supported services (`pr#47084 <https://github.com/ceph/ceph/pull/47084>`_, Avan Thakkar)
* mgr/dashboard: introduce memory and cpu usage for daemons (`pr#46459 <https://github.com/ceph/ceph/pull/46459>`_, Aashish Sharma, Avan Thakkar)
* mgr/dashboard: iops optimized option enabled (`pr#46737 <https://github.com/ceph/ceph/pull/46737>`_, Pere Diaz Bou)
* mgr/dashboard: iterate through copy of items (`pr#46870 <https://github.com/ceph/ceph/pull/46870>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: prevent alert redirect (`pr#47145 <https://github.com/ceph/ceph/pull/47145>`_, Tatjana Dehler)
* mgr/dashboard: Pull latest languages from Transifex (`pr#46695 <https://github.com/ceph/ceph/pull/46695>`_, Volker Theile)
* mgr/dashboard: rbd image pagination (`pr#47105 <https://github.com/ceph/ceph/pull/47105>`_, Pere Diaz Bou, Nizamudeen A)
* mgr/dashboard: rbd striping setting pre-population and pop-over (`pr#47410 <https://github.com/ceph/ceph/pull/47410>`_, Vrushal Chaudhari)
* mgr/dashboard: remove token logging (`pr#47431 <https://github.com/ceph/ceph/pull/47431>`_, Pere Diaz Bou)
* mgr/dashboard: Show error on creating service with duplicate service id (`pr#47404 <https://github.com/ceph/ceph/pull/47404>`_, Aashish Sharma)
* mgr/dashboard: stop polling when page is not visible (`pr#46675 <https://github.com/ceph/ceph/pull/46675>`_, Sarthak0702)
* mgr/dashboard: unselect rows in datatables (`pr#46322 <https://github.com/ceph/ceph/pull/46322>`_, Sarthak0702)
* mgr/DaemonServer.cc: fix typo in output gap >= max_pg_num_change (`pr#47211 <https://github.com/ceph/ceph/pull/47211>`_, Kamoltat)
* mgr/prometheus: expose num objects repaired in pool (`pr#48205 <https://github.com/ceph/ceph/pull/48205>`_, Pere Diaz Bou)
* mgr/prometheus: use vendored "packaging" instead (`pr#49695 <https://github.com/ceph/ceph/pull/49695>`_, Matan Breizman)
* mgr/rbd_support: avoid wedging the task queue if pool is removed (`pr#49056 <https://github.com/ceph/ceph/pull/49056>`_, Ilya Dryomov)
* mgr/snap_schedule: add time zone suffix to snapshot dir name (`pr#45968 <https://github.com/ceph/ceph/pull/45968>`_, Milind Changire, Venky Shankar)
* mgr/snap_schedule: persist all updates to RADOS (`pr#46797 <https://github.com/ceph/ceph/pull/46797>`_, Milind Changire)
* mgr/snap_schedule: remove subvol interface (`pr#48221 <https://github.com/ceph/ceph/pull/48221>`_, Milind Changire)
* mgr/stats: be resilient to offline MDS rank-0 (`pr#45293 <https://github.com/ceph/ceph/pull/45293>`_, Jos Collin)
* mgr/stats: change in structure of perf_stats o/p (`pr#47851 <https://github.com/ceph/ceph/pull/47851>`_, Neeraj Pratap Singh)
* mgr/stats: missing clients in perf stats command output (`pr#47866 <https://github.com/ceph/ceph/pull/47866>`_, Neeraj Pratap Singh)
* mgr/telemetry: reset health warning after re-opting-in (`pr#47307 <https://github.com/ceph/ceph/pull/47307>`_, Yaarit Hatuka)
* mgr/volumes: A few dependent mgr volumes PRs (`pr#47112 <https://github.com/ceph/ceph/pull/47112>`_, Rishabh Dave, Kotresh HR, John Mulligan, Nikhilkumar Shelke)
* mgr/volumes: Add human-readable flag to volume info command (`pr#48468 <https://github.com/ceph/ceph/pull/48468>`_, Neeraj Pratap Singh)
* mgr/volumes: add interface to check the presence of subvolumegroups/subvolumes (`pr#47460 <https://github.com/ceph/ceph/pull/47460>`_, Neeraj Pratap Singh)
* mgr/volumes: Add volume info command (`pr#47769 <https://github.com/ceph/ceph/pull/47769>`_, Neeraj Pratap Singh)
* mgr/volumes: filter internal directories in 'subvolumegroup ls' command (`pr#47512 <https://github.com/ceph/ceph/pull/47512>`_, Nikhilkumar Shelke)
* mgr/volumes: Fix idempotent subvolume rm (`pr#46139 <https://github.com/ceph/ceph/pull/46139>`_, Kotresh HR)
* mgr/volumes: Fix subvolume creation in FIPS enabled system (`pr#47369 <https://github.com/ceph/ceph/pull/47369>`_, Kotresh HR)
* mgr/volumes: remove incorrect 'size' from output of 'snapshot info' (`pr#46803 <https://github.com/ceph/ceph/pull/46803>`_, Nikhilkumar Shelke)
* mgr/volumes: set, get, list and remove metadata of snapshot (`pr#46515 <https://github.com/ceph/ceph/pull/46515>`_, Nikhilkumar Shelke)
* mgr/volumes: set, get, list and remove metadata of subvolume (`pr#45961 <https://github.com/ceph/ceph/pull/45961>`_, Nikhilkumar Shelke)
* mgr/volumes: Show clone failure reason in clone status command (`pr#45928 <https://github.com/ceph/ceph/pull/45928>`_, Kotresh HR)
* mgr/volumes: subvolume ls command crashes if groupname as '_nogroup' (`pr#46806 <https://github.com/ceph/ceph/pull/46806>`_, Nikhilkumar Shelke)
* mgr/volumes: subvolumegroup quotas (`pr#46668 <https://github.com/ceph/ceph/pull/46668>`_, Kotresh HR)
* mgr: relax "pending_service_map.epoch > service_map.epoch" assert (`pr#46688 <https://github.com/ceph/ceph/pull/46688>`_, Mykola Golub)
* mirror snapshot schedule and trash purge schedule fixes (`pr#46778 <https://github.com/ceph/ceph/pull/46778>`_, Ilya Dryomov)
* mon/ConfigMonitor: fix config get key with whitespaces (`pr#47380 <https://github.com/ceph/ceph/pull/47380>`_, Nitzan Mordechai)
* mon/Elector.cc: Compress peer >= rank_size sanity check into send_peer_ping (`pr#49444 <https://github.com/ceph/ceph/pull/49444>`_, Kamoltat)
* mon/Elector: Added sanity check when pinging a peer monitor (`pr#48320 <https://github.com/ceph/ceph/pull/48320>`_, Kamoltat)
* mon/Elector: Change how we handle removed_ranks and notify_rank_removed() (`pr#49312 <https://github.com/ceph/ceph/pull/49312>`_, Kamoltat)
* mon/Elector: notify_rank_removed erase rank from both live_pinging and dead_pinging sets for highest ranked MON (`pr#47087 <https://github.com/ceph/ceph/pull/47087>`_, Kamoltat)
* mon/MDSMonitor: fix standby-replay mds being removed from MDSMap unexpectedly (`pr#48270 <https://github.com/ceph/ceph/pull/48270>`_, 胡玮文)
* mon/OSDMonitor: Added extra check before mon.go_recovery_stretch_mode() (`pr#48803 <https://github.com/ceph/ceph/pull/48803>`_, Kamoltat)
* mon/OSDMonitor: Ensure kvmon() is writeable before handling "osd new" cmd (`pr#46691 <https://github.com/ceph/ceph/pull/46691>`_, Sridhar Seshasayee)
* mon/OSDMonitor: properly set last_force_op_resend in stretch mode (`pr#45870 <https://github.com/ceph/ceph/pull/45870>`_, Ilya Dryomov)
* mon: allow a MON_DOWN grace period after cluster mkfs (`pr#48558 <https://github.com/ceph/ceph/pull/48558>`_, Sage Weil)
* monitoring/ceph-mixin: add RGW host to label info (`pr#48035 <https://github.com/ceph/ceph/pull/48035>`_, Tatjana Dehler)
* monitoring/ceph-mixin: OSD overview typo fix (`pr#47386 <https://github.com/ceph/ceph/pull/47386>`_, Tatjana Dehler)
* mount/conf: Fix IPv6 parsing (`pr#46112 <https://github.com/ceph/ceph/pull/46112>`_, Matan Breizman)
* msg: fix deadlock when handling existing but closed v2 connection (`pr#48254 <https://github.com/ceph/ceph/pull/48254>`_, Radosław Zarzyński)
* msg: Fix Windows IPv6 support (`pr#47303 <https://github.com/ceph/ceph/pull/47303>`_, Lucian Petrut)
* msg: Log at higher level when Throttle::get_or_fail() fails (`pr#47764 <https://github.com/ceph/ceph/pull/47764>`_, Brad Hubbard)
* msg: reset ProtocolV2's frame assembler in appropriate thread (`pr#48255 <https://github.com/ceph/ceph/pull/48255>`_, Radoslaw Zarzynski)
* os/bluestore:  proper locking for Allocators' dump methods (`pr#48167 <https://github.com/ceph/ceph/pull/48167>`_, Igor Fedotov)
* os/bluestore: add bluefs-import command (`pr#47875 <https://github.com/ceph/ceph/pull/47875>`_, Adam Kupczyk, zhang daolong)
* os/bluestore: Always update the cursor position in AVL near-fit search (`pr#46642 <https://github.com/ceph/ceph/pull/46642>`_, Mark Nelson)
* os/bluestore: Better readability of perf output (`pr#47259 <https://github.com/ceph/ceph/pull/47259>`_, Adam Kupczyk)
* os/bluestore: BlueFS: harmonize log read and writes modes (`pr#49431 <https://github.com/ceph/ceph/pull/49431>`_, Adam Kupczyk)
* os/bluestore: do not signal deleted dirty file to bluefs log (`pr#48168 <https://github.com/ceph/ceph/pull/48168>`_, Igor Fedotov)
* os/bluestore: fix AU accounting in bluestore_cache_other mempool (`pr#47337 <https://github.com/ceph/ceph/pull/47337>`_, Igor Fedotov)
* os/bluestore: Fix collision between BlueFS and BlueStore deferred writes (`pr#47296 <https://github.com/ceph/ceph/pull/47296>`_, Adam Kupczyk)
* os/bluestore: fix improper bluefs log size tracking in volume selector (`pr#45408 <https://github.com/ceph/ceph/pull/45408>`_, Igor Fedotov)
* os/bluestore: get rid of fake onode nref increment for pinned entry (`pr#47556 <https://github.com/ceph/ceph/pull/47556>`_, Igor Fedotov)
* os/bluestore: incremental update mode for bluefs log (`pr#48915 <https://github.com/ceph/ceph/pull/48915>`_, Adam Kupczyk)
* os/bluestore: update perf counter priorities (`pr#47095 <https://github.com/ceph/ceph/pull/47095>`_, Laura Flores)
* os/bluestore: use direct write in BlueStore::_write_bdev_label (`pr#48278 <https://github.com/ceph/ceph/pull/48278>`_, luo rixin)
* osd, mds: fix the "heap" admin cmd printing always to error stream (`pr#48106 <https://github.com/ceph/ceph/pull/48106>`_, Radoslaw Zarzynski)
* osd, tools, kv: non-aggressive, on-line trimming of accumulated dups (`pr#47701 <https://github.com/ceph/ceph/pull/47701>`_, Radoslaw Zarzynski, Nitzan Mordechai)
* osd/PGLog.cc: Trim duplicates by number of entries (`pr#46252 <https://github.com/ceph/ceph/pull/46252>`_, Nitzan Mordechai)
* osd/scrub: mark PG as being scrubbed, from scrub initiation to Inacti… (`pr#46767 <https://github.com/ceph/ceph/pull/46767>`_, Ronen Friedman)
* osd/scrub: Reintroduce scrub starts message (`pr#48070 <https://github.com/ceph/ceph/pull/48070>`_, Prashant D)
* osd/scrub: use the actual active set when requesting replicas (`pr#48544 <https://github.com/ceph/ceph/pull/48544>`_, Ronen Friedman)
* osd/SnapMapper: fix legacy key conversion in snapmapper class (`pr#47134 <https://github.com/ceph/ceph/pull/47134>`_, Manuel Lausch, Matan Breizman)
* osd: add created_at meta (`pr#49144 <https://github.com/ceph/ceph/pull/49144>`_, Alex Marangone)
* osd: fix wrong input when calling recover_object() (`pr#46120 <https://github.com/ceph/ceph/pull/46120>`_, Myoungwon Oh)
* osd: log the number of 'dups' entries in a PG Log (`pr#46608 <https://github.com/ceph/ceph/pull/46608>`_, Radoslaw Zarzynski)
* osd: remove invalid put on message (`pr#47525 <https://github.com/ceph/ceph/pull/47525>`_, Nitzan Mordechai)
* osd: set per_pool_stats true when OSD has no PG (`pr#48250 <https://github.com/ceph/ceph/pull/48250>`_, jindengke, lmgdlmgd)
* osd/scrub: late-arriving reservation grants are not an error (`pr#46873 <https://github.com/ceph/ceph/pull/46873>`_, Ronen Friedman)
* osd/scrubber/pg_scrubber.cc: fix bug where scrub machine gets stuck (`pr#46845 <https://github.com/ceph/ceph/pull/46845>`_, Cory Snyder)
* PendingReleaseNotes: document online and offline trimming of PG Log's… (`pr#48020 <https://github.com/ceph/ceph/pull/48020>`_, Radoslaw Zarzynski)
* pybind/cephfs: fix grammar (`pr#48982 <https://github.com/ceph/ceph/pull/48982>`_, Zac Dover)
* pybind: fix typo in cephfs.pyx (`pr#48953 <https://github.com/ceph/ceph/pull/48953>`_, Zac Dover)
* pybind/mgr/cephadm/serve: don't remove ceph.conf which leads to qa failure (`pr#46974 <https://github.com/ceph/ceph/pull/46974>`_, Dhairya Parmar)
* pybind/mgr/dashboard: move pytest into requirements.txt (`pr#48081 <https://github.com/ceph/ceph/pull/48081>`_, Kefu Chai)
* pybind/mgr/pg_autoscaler: change overlapping roots to warning (`pr#47522 <https://github.com/ceph/ceph/pull/47522>`_, Kamoltat)
* pybind/mgr: fix flake8 (`pr#47393 <https://github.com/ceph/ceph/pull/47393>`_, Avan Thakkar)
* pybind/mgr: fixup after upgrading tox versions (`pr#49363 <https://github.com/ceph/ceph/pull/49363>`_, Adam King, Kefu Chai)
* pybind/mgr: tox and test fixes (`pr#49542 <https://github.com/ceph/ceph/pull/49542>`_, Kefu Chai)
* pybind/rados: notify callback reconnect (`pr#48112 <https://github.com/ceph/ceph/pull/48112>`_, Nitzan Mordechai)
* pybind: add wrapper for rados_write_op_omap_cmp (`pr#48376 <https://github.com/ceph/ceph/pull/48376>`_, Sandy Kaur)
* python-common: Add 'KB' to supported suffixes in SizeMatcher (`pr#48243 <https://github.com/ceph/ceph/pull/48243>`_, Tim Serong)
* python-common: allow crush device class to be set from osd service spec (`pr#46555 <https://github.com/ceph/ceph/pull/46555>`_, Cory Snyder)
* qa/cephadm: remove fsid dir before bootstrap in test_cephadm.sh (`pr#48101 <https://github.com/ceph/ceph/pull/48101>`_, Adam King)
* qa/cephfs: fallback to older way of get_op_read_count (`pr#46901 <https://github.com/ceph/ceph/pull/46901>`_, Dhairya Parmar)
* qa/import-legacy: install python3 package for nautilus ceph (`pr#47528 <https://github.com/ceph/ceph/pull/47528>`_, Xiubo Li)
* qa/suites/rados/thrash-erasure-code-big/thrashers: add `osd max backfills` setting to mapgap and pggrow (`pr#46391 <https://github.com/ceph/ceph/pull/46391>`_, Laura Flores)
* qa/suites/rbd/pwl-cache: ensure recovery is actually tested (`pr#47128 <https://github.com/ceph/ceph/pull/47128>`_, Ilya Dryomov, Yin Congmin)
* qa/suites/rbd: disable workunit timeout for dynamic_features_no_cache (`pr#47158 <https://github.com/ceph/ceph/pull/47158>`_, Ilya Dryomov)
* qa/suites/rbd: place cache file on tmpfs for xfstests (`pr#46597 <https://github.com/ceph/ceph/pull/46597>`_, Ilya Dryomov)
* qa/tasks/ceph_manager.py: increase test_pool_min_size timeout (`pr#47446 <https://github.com/ceph/ceph/pull/47446>`_, Kamoltat)
* qa/tasks/kubeadm: set up tigera resources via kubectl create (`pr#48097 <https://github.com/ceph/ceph/pull/48097>`_, John Mulligan)
* qa/tasks/rbd_fio: bump default to fio 3.32 (`pr#48385 <https://github.com/ceph/ceph/pull/48385>`_, Ilya Dryomov)
* qa/workunits/cephadm: update test_repos master -> main (`pr#47320 <https://github.com/ceph/ceph/pull/47320>`_, Adam King)
* qa/workunits/rados: specify redirect in curl command (`pr#49139 <https://github.com/ceph/ceph/pull/49139>`_, Laura Flores)
* qa: Fix test_subvolume_group_ls_filter_internal_directories (`pr#48328 <https://github.com/ceph/ceph/pull/48328>`_, Kotresh HR)
* qa: Fix test_subvolume_snapshot_info_if_orphan_clone (`pr#48647 <https://github.com/ceph/ceph/pull/48647>`_, Kotresh HR)
* qa: Fix test_subvolume_snapshot_info_if_orphan_clone (`pr#48417 <https://github.com/ceph/ceph/pull/48417>`_, Kotresh HR)
* qa: fix teuthology master branch ref (`pr#46504 <https://github.com/ceph/ceph/pull/46504>`_, Ernesto Puerta)
* qa: ignore disk quota exceeded failure in test (`pr#48165 <https://github.com/ceph/ceph/pull/48165>`_, Nikhilkumar Shelke)
* qa: remove .teuthology_branch file (`pr#46490 <https://github.com/ceph/ceph/pull/46490>`_, Jeff Layton)
* qa: run e2e test on centos only (`pr#49337 <https://github.com/ceph/ceph/pull/49337>`_, Kefu Chai)
* qa: switch back to git protocol for qemu-xfstests (`pr#49543 <https://github.com/ceph/ceph/pull/49543>`_, Ilya Dryomov)
* qa: switch to https protocol for repos' server (`pr#49470 <https://github.com/ceph/ceph/pull/49470>`_, Xiubo Li)
* qa: wait rank 0 to become up:active state before mounting fuse client (`pr#46802 <https://github.com/ceph/ceph/pull/46802>`_, Xiubo Li)
* qa: add filesystem/file sync stuck test support (`pr#46425 <https://github.com/ceph/ceph/pull/46425>`_, Xiubo Li)
* radosgw-admin: 'reshard list' doesn't log ENOENT errors (`pr#45451 <https://github.com/ceph/ceph/pull/45451>`_, Casey Bodley)
* rbd-fuse: librados will filter out -r option from command-line (`pr#46953 <https://github.com/ceph/ceph/pull/46953>`_, wanwencong)
* rbd-mirror: don't prune non-primary snapshot when restarting delta sync (`pr#46590 <https://github.com/ceph/ceph/pull/46590>`_, Ilya Dryomov)
* rbd-mirror: generally skip replay/resync if remote image is not primary (`pr#46813 <https://github.com/ceph/ceph/pull/46813>`_, Ilya Dryomov)
* rbd-mirror: remove bogus completed_non_primary_snapshots_exist check (`pr#47118 <https://github.com/ceph/ceph/pull/47118>`_, Ilya Dryomov)
* rbd-mirror: resume pending shutdown on error in snapshot replayer (`pr#47913 <https://github.com/ceph/ceph/pull/47913>`_, Ilya Dryomov)
* rbd: device map/unmap --namespace handling fixes (`pr#48459 <https://github.com/ceph/ceph/pull/48459>`_, Ilya Dryomov, Stefan Chivu)
* rbd: don't default empty pool name unless namespace is specified (`pr#47143 <https://github.com/ceph/ceph/pull/47143>`_, Ilya Dryomov)
* rbd: find_action() should sort actions first (`pr#47583 <https://github.com/ceph/ceph/pull/47583>`_, Ilya Dryomov)
* rgw: Swift retarget needs bucket set on object (`pr#47230 <https://github.com/ceph/ceph/pull/47230>`_, Daniel Gryniewicz)
* rgw/backport/pacific: Fix crashes with Sync policy APIs (`pr#47994 <https://github.com/ceph/ceph/pull/47994>`_, Soumya Koduri)
* rgw/notifications: Change in multipart upload notification behavior (`pr#47175 <https://github.com/ceph/ceph/pull/47175>`_, Kalpesh Pandya)
* rgw/rgw_string.h: add missing includes for alpine and boost 1.75 (`pr#47304 <https://github.com/ceph/ceph/pull/47304>`_, Duncan Bellamy)
* rgw/sts: adding code for aws:RequestTags as part (`pr#47746 <https://github.com/ceph/ceph/pull/47746>`_, Kalpesh Pandya, Pritha Srivastava)
* rgw: address bug where object puts could write to decommissioned shard (`pr#48663 <https://github.com/ceph/ceph/pull/48663>`_, J. Eric Ivancich)
* rgw: better tenant id from the uri on anonymous access (`pr#47341 <https://github.com/ceph/ceph/pull/47341>`_, Rafał Wądołowski, Marcus Watts)
* rgw: check bucket shard init status in RGWRadosBILogTrimCR (`pr#44907 <https://github.com/ceph/ceph/pull/44907>`_, Mykola Golub)
* rgw: check object storage_class when check_disk_state (`pr#46579 <https://github.com/ceph/ceph/pull/46579>`_, Huber-ming)
* rgw: data sync uses yield_spawn_window() (`pr#45713 <https://github.com/ceph/ceph/pull/45713>`_, Casey Bodley)
* rgw: do not permit locked object version removal (`pr#47041 <https://github.com/ceph/ceph/pull/47041>`_, Igor Fedotov)
* rgw: fix bool/int logic error when calling get_obj_head_ioctx (`pr#48230 <https://github.com/ceph/ceph/pull/48230>`_, J. Eric Ivancich)
* rgw: fix bug where variable referenced after data moved out (`pr#48229 <https://github.com/ceph/ceph/pull/48229>`_, J. Eric Ivancich)
* rgw: fix data corruption due to network jitter (`pr#48274 <https://github.com/ceph/ceph/pull/48274>`_, Shasha Lu)
* rgw: Fix data race in ChangeStatus (`pr#47196 <https://github.com/ceph/ceph/pull/47196>`_, Adam C. Emerson)
* rgw: fix ListBucketMultiparts response with common prefixes (`pr#44558 <https://github.com/ceph/ceph/pull/44558>`_, Casey Bodley)
* rgw: fix segfault in OpsLogRados::log when realm is reloaded (`pr#45410 <https://github.com/ceph/ceph/pull/45410>`_, Cory Snyder)
* rgw: fix self-comparison for RGWCopyObj optimization (`pr#43802 <https://github.com/ceph/ceph/pull/43802>`_, Casey Bodley)
* rgw: Guard against malformed bucket URLs (`pr#47194 <https://github.com/ceph/ceph/pull/47194>`_, Adam C. Emerson)
* rgw: initialize rgw_log_entry::identity_type (`pr#49142 <https://github.com/ceph/ceph/pull/49142>`_, Casey Bodley)
* rgw: log access key id in ops logs (`pr#46622 <https://github.com/ceph/ceph/pull/46622>`_, Cory Snyder)
* rgw: log deletion status of individual objects in multi object delete request (`pr#48348 <https://github.com/ceph/ceph/pull/48348>`_, Cory Snyder)
* rgw: maintain object instance within RGWRadosObject::get_obj_state method (`pr#47266 <https://github.com/ceph/ceph/pull/47266>`_, Casey Bodley, Cory Snyder)
* rgw: OpsLogFile::stop() signals under mutex (`pr#46039 <https://github.com/ceph/ceph/pull/46039>`_, Casey Bodley)
* rgw: remove rgw_rados_pool_pg_num_min and its use on pool creation use the cluster defaults for pg_num_min (`pr#46235 <https://github.com/ceph/ceph/pull/46235>`_, Casey Bodley)
* rgw: reopen ops log file on sighup (`pr#46619 <https://github.com/ceph/ceph/pull/46619>`_, Cory Snyder)
* rgw: return OK on consecutive complete-multipart reqs (`pr#45486 <https://github.com/ceph/ceph/pull/45486>`_, Mark Kogan)
* rgw: RGWCoroutine::set_sleeping() checks for null stack (`pr#46040 <https://github.com/ceph/ceph/pull/46040>`_, Or Friedmann, Casey Bodley)
* rgw: splitting gc chains into smaller parts to prevent (`pr#48240 <https://github.com/ceph/ceph/pull/48240>`_, Pritha Srivastava)
* rgw: x-amz-date change breaks certain cases of aws sig v4 (`pr#48313 <https://github.com/ceph/ceph/pull/48313>`_, Marcus Watts)
* rgw: on FIPS enabled, fix segfault performing s3 multipart PUT (`pr#46715 <https://github.com/ceph/ceph/pull/46715>`_, Mark Kogan)
* rgw_reshard: drop olh entries with empty name (`pr#45847 <https://github.com/ceph/ceph/pull/45847>`_, Dan van der Ster, Casey Bodley)
* rgw_rest_user_policy: Fix GetUserPolicy & ListUserPolicies responses (`pr#47234 <https://github.com/ceph/ceph/pull/47234>`_, Sumedh A. Kulkarni)
* rgwlc:  don't incorrectly expire delete markers when !next_key_name (`pr#47231 <https://github.com/ceph/ceph/pull/47231>`_, Matt Benjamin)
* rgwlc: fix segfault resharding during lc (`pr#46744 <https://github.com/ceph/ceph/pull/46744>`_, Mark Kogan)
* rpm: use system libpmem on Centos 9 Stream (`pr#46211 <https://github.com/ceph/ceph/pull/46211>`_, Ilya Dryomov)
* run-make-check.sh: enable RBD persistent caches (`pr#45991 <https://github.com/ceph/ceph/pull/45991>`_, Ilya Dryomov)
* SimpleRADOSStriper: Avoid moving bufferlists by using deque in read() (`pr#48187 <https://github.com/ceph/ceph/pull/48187>`_, Matan Breizman)
* test/bufferlist: ensure rebuild_aligned_size_and_memory() always rebuilds (`pr#46215 <https://github.com/ceph/ceph/pull/46215>`_, Radoslaw Zarzynski)
* test/cli-integration/rbd: iSCSI REST API responses aren't pretty-printed anymore (`pr#47920 <https://github.com/ceph/ceph/pull/47920>`_, Ilya Dryomov)
* test/{librbd, rgw}: increase delay between and number of bind attempts (`pr#48024 <https://github.com/ceph/ceph/pull/48024>`_, Ilya Dryomov, Kefu Chai)
* test: bump DecayCounter.steady acceptable error (`pr#48031 <https://github.com/ceph/ceph/pull/48031>`_, Patrick Donnelly)
* test: fix TierFlushDuringFlush to wait until dedup_tier is set on bas… (`issue#53855 <http://tracker.ceph.com/issues/53855>`_, `pr#46748 <https://github.com/ceph/ceph/pull/46748>`_, Myoungwon Oh, Sungmin Lee)
* test: No direct use of nose (`pr#46255 <https://github.com/ceph/ceph/pull/46255>`_, Steve Kowalik, Kefu Chai)
* tooling: Change mrun to use bash (`pr#46077 <https://github.com/ceph/ceph/pull/46077>`_, Adam C. Emerson)
* tools: ceph-objectstore-tool is able to trim solely pg log dups' entries (`pr#46631 <https://github.com/ceph/ceph/pull/46631>`_, Radosław Zarzyński, Radoslaw Zarzynski)
* Updates to fix `make check` failures (`pr#47803 <https://github.com/ceph/ceph/pull/47803>`_, Tim Serong, Kefu Chai, Willem Jan Withagen, Nathan Cutler, Boris Ranto, Laura Flores, Pete Zaitcev)
* v16.2.10 (`pr#47220 <https://github.com/ceph/ceph/pull/47220>`_, Kotresh HR, Seena Fallah)
* v16.2.9 (`pr#46336 <https://github.com/ceph/ceph/pull/46336>`_, Cory Snyder)
* win32_deps_build.sh: master -> main for wnbd (`pr#46762 <https://github.com/ceph/ceph/pull/46762>`_, Ilya Dryomov)

v16.2.10 Pacific
================

This is a hotfix release that resolves two security flaws.

Notable Changes
---------------

* Users who were running OpenStack Manila to export native CephFS and who
  upgraded their Ceph cluster from Nautilus (or earlier) to a later
  major version were vulnerable to an attack by malicious users. The
  vulnerability allowed users to obtain access to arbitrary portions of
  the CephFS filesystem hierarchy instead of being properly restricted
  to their own subvolumes. The vulnerability is due to a bug in the
  "volumes" plugin in Ceph Manager. This plugin is responsible for
  managing Ceph File System subvolumes, which are used by OpenStack
  Manila services as a way to provide shares to Manila users.

  With this hotfix, the vulnerability is fixed. Administrators who are
  concerned they may have been impacted should audit the CephX keys in
  their cluster for proper path restrictions.

  Again, this vulnerability impacts only OpenStack Manila clusters that
  provided native CephFS access to their users.

* A regression made it possible to dereference a null pointer for
  s3website requests that don't refer to a bucket resulting in an RGW
  segfault.

Changelog
---------
* mgr/volumes: Fix subvolume discover during upgrade (:ref:`CVE-2022-0670`, Kotresh HR)
* mgr/volumes: V2 Fix for test_subvolume_retain_snapshot_invalid_recreate (:ref:`CVE-2022-0670`, Kotresh HR)
* qa: validate subvolume discover on upgrade (Kotresh HR)
* rgw: s3website check for bucket before retargeting (Seena Fallah)

v16.2.9 Pacific
===============

This is a hotfix release in the Pacific series to address a bug in 16.2.8 that could cause MGRs to deadlock. See https://tracker.ceph.com/issues/55687.

Changelog
---------

* mgr/ActivePyModules.cc: fix cases where GIL is held while attempting to lock mutex (`pr#46302 <https://github.com/ceph/ceph/pull/46302>`_, Cory Snyder)

v16.2.8 Pacific
===============

This is the eighth backport release in the Pacific series.

Notable Changes
---------------

* MON/MGR: Pools can now be created with `--bulk` flag. Any pools created with `bulk`
  will use a profile of the `pg_autoscaler` that provides more performance from the start.
  However, any pools created without the `--bulk` flag will remain using it's old behavior
  by default. For more details, see:

  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* MGR: The pg_autoscaler can now be turned `on` and `off` globally
  with the `noautoscale` flag. By default this flag is unset and
  the default pg_autoscale mode remains the same.
  For more details, see:

  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* A health warning will now be reported if the ``require-osd-release`` flag is not
  set to the appropriate release after a cluster upgrade.

* CephFS: Upgrading Ceph Metadata Servers when using multiple active MDSs requires
  ensuring no pending stray entries which are directories are present for active
  ranks except rank 0. See :ref:`upgrading_from_octopus_or_nautilus`.

Changelog
---------

* [Revert] bluestore: set upper and lower bounds on rocksdb omap iterators (`pr#46092 <https://github.com/ceph/ceph/pull/46092>`_, Neha Ojha)
* admin/doc-requirements: bump sphinx to 4.4.0 (`pr#45876 <https://github.com/ceph/ceph/pull/45876>`_, Kefu Chai)
* auth,mon: don't log "unable to find a keyring" error when key is given (`pr#43313 <https://github.com/ceph/ceph/pull/43313>`_, Ilya Dryomov)
* backport nbd cookie support (`pr#45582 <https://github.com/ceph/ceph/pull/45582>`_, Prasanna Kumar Kalever)
* backport of monitoring related PRs (`pr#45980 <https://github.com/ceph/ceph/pull/45980>`_, Pere Diaz Bou, Travis Nielsen, Aashish Sharma, Nizamudeen A, Arthur Outhenin-Chalandre)
* bluestore: set upper and lower bounds on rocksdb omap iterators (`pr#45963 <https://github.com/ceph/ceph/pull/45963>`_, Cory Snyder)
* build: Add some debugging messages (`pr#45753 <https://github.com/ceph/ceph/pull/45753>`_, David Galloway)
* build: install-deps failing in docker build (`pr#45849 <https://github.com/ceph/ceph/pull/45849>`_, Nizamudeen A, Ernesto Puerta)
* ceph-fuse: perform cleanup if test_dentry_handling failed (`pr#45351 <https://github.com/ceph/ceph/pull/45351>`_, Nikhilkumar Shelke)
* ceph-volume: abort when passed devices have partitions (`pr#45146 <https://github.com/ceph/ceph/pull/45146>`_, Guillaume Abrioux)
* ceph-volume: don't use MultiLogger in find_executable_on_host() (`pr#44701 <https://github.com/ceph/ceph/pull/44701>`_, Guillaume Abrioux)
* ceph-volume: fix error 'KeyError' with inventory (`pr#44884 <https://github.com/ceph/ceph/pull/44884>`_, Guillaume Abrioux)
* ceph-volume: fix regression introcuded via #43536 (`pr#44644 <https://github.com/ceph/ceph/pull/44644>`_, Guillaume Abrioux)
* ceph-volume: fix tags dict output in `lvm list` (`pr#44767 <https://github.com/ceph/ceph/pull/44767>`_, Guillaume Abrioux)
* ceph-volume: honour osd_dmcrypt_key_size option (`pr#44973 <https://github.com/ceph/ceph/pull/44973>`_, Guillaume Abrioux)
* ceph-volume: human_readable_size() refactor (`pr#44209 <https://github.com/ceph/ceph/pull/44209>`_, Guillaume Abrioux)
* ceph-volume: improve mpath devices support (`pr#44789 <https://github.com/ceph/ceph/pull/44789>`_, Guillaume Abrioux)
* ceph-volume: make it possible to skip needs_root() (`pr#44319 <https://github.com/ceph/ceph/pull/44319>`_, Guillaume Abrioux)
* ceph-volume: show RBD devices as not available (`pr#44708 <https://github.com/ceph/ceph/pull/44708>`_, Michael Fritch)
* ceph/admin: s/master/main (`pr#45596 <https://github.com/ceph/ceph/pull/45596>`_, Zac Dover)
* Cephadm Pacific Batch Backport April (`pr#45919 <https://github.com/ceph/ceph/pull/45919>`_, Adam King, Teoman ONAY, Redouane Kachach, Lukas Mayer, Melissa Li)
* Cephadm Pacific Batch Backport March (`pr#45716 <https://github.com/ceph/ceph/pull/45716>`_, Adam King, Redouane Kachach, Matan Breizman, wangyunqing)
* cephadm/ceph-volume: do not use lvm binary in containers (`pr#43954 <https://github.com/ceph/ceph/pull/43954>`_, Guillaume Abrioux, Sage Weil)
* cephadm: _parse_ipv6_route: Fix parsing ifs w/o route (`pr#44877 <https://github.com/ceph/ceph/pull/44877>`_, Sebastian Wagner)
* cephadm: add shared_ceph_folder opt to ceph-volume subcommand (`pr#44880 <https://github.com/ceph/ceph/pull/44880>`_, Guillaume Abrioux)
* cephadm: check if cephadm is root after cli is parsed (`pr#44634 <https://github.com/ceph/ceph/pull/44634>`_, John Mulligan)
* cephadm: chown the prometheus data dir during redeploy (`pr#45046 <https://github.com/ceph/ceph/pull/45046>`_, Michael Fritch)
* cephadm: deal with ambiguity within normalize_image_digest (`pr#44632 <https://github.com/ceph/ceph/pull/44632>`_, Sebastian Wagner)
* cephadm: fix broken telemetry documentation link (`pr#45803 <https://github.com/ceph/ceph/pull/45803>`_, Laura Flores)
* cephadm: infer the default container image during pull (`pr#45569 <https://github.com/ceph/ceph/pull/45569>`_, Michael Fritch)
* cephadm: make extract_uid_gid errors more readable (`pr#44528 <https://github.com/ceph/ceph/pull/44528>`_, Sebastian Wagner)
* cephadm: November batch 2 (`pr#44446 <https://github.com/ceph/ceph/pull/44446>`_, Sage Weil, Adam King, Sebastian Wagner, Melissa Li, Michael Fritch, Guillaume Abrioux)
* cephadm: pass `CEPH_VOLUME_SKIP_RESTORECON=yes` (backport) (`pr#44248 <https://github.com/ceph/ceph/pull/44248>`_, Guillaume Abrioux)
* cephadm: preserve `authorized_keys` file during upgrade (`pr#45355 <https://github.com/ceph/ceph/pull/45355>`_, Michael Fritch)
* cephadm: Remove containers pids-limit (`pr#45580 <https://github.com/ceph/ceph/pull/45580>`_, Ilya Dryomov, Teoman ONAY)
* cephadm: revert pids limit (`pr#45936 <https://github.com/ceph/ceph/pull/45936>`_, Adam King)
* cephadm: validate that the constructed YumDnf baseurl is usable (`pr#44882 <https://github.com/ceph/ceph/pull/44882>`_, John Mulligan)
* cls/journal: skip disconnected clients when calculating min_commit_position (`pr#44690 <https://github.com/ceph/ceph/pull/44690>`_, Mykola Golub)
* cls/rbd: GroupSnapshotNamespace comparator violates ordering rules (`pr#45075 <https://github.com/ceph/ceph/pull/45075>`_, Ilya Dryomov)
* cmake/modules: always use the python3 specified in command line (`pr#45967 <https://github.com/ceph/ceph/pull/45967>`_, Kefu Chai)
* cmake: pass RTE_DEVEL_BUILD=n when building dpdk (`pr#45262 <https://github.com/ceph/ceph/pull/45262>`_, Kefu Chai)
* common/PriorityCache: low perf counters priorities for submodules (`pr#44175 <https://github.com/ceph/ceph/pull/44175>`_, Igor Fedotov)
* common: avoid pthread_mutex_unlock twice (`pr#45464 <https://github.com/ceph/ceph/pull/45464>`_, Dai Zhiwei)
* common: fix FTBFS due to dout & need_dynamic on GCC-12 (`pr#45373 <https://github.com/ceph/ceph/pull/45373>`_, Radoslaw Zarzynski)
* common: fix missing name in PriorityCache perf counters (`pr#45588 <https://github.com/ceph/ceph/pull/45588>`_, Laura Flores)
* common: replace BitVector::NoInitAllocator with wrapper struct (`pr#45179 <https://github.com/ceph/ceph/pull/45179>`_, Casey Bodley)
* crush: Fix segfault in update_from_hook (`pr#44897 <https://github.com/ceph/ceph/pull/44897>`_, Adam Kupczyk)
* doc/cephadm: Add CentOS Stream install instructions (`pr#44996 <https://github.com/ceph/ceph/pull/44996>`_, Patrick C. F. Ernzer)
* doc/cephadm: Co-location of daemons (`pr#44879 <https://github.com/ceph/ceph/pull/44879>`_, Sebastian Wagner)
* doc/cephadm: Doc backport (`pr#44525 <https://github.com/ceph/ceph/pull/44525>`_, Foad Lind, Sebastian Wagner)
* doc/cephadm: improve the development doc a bit (`pr#44636 <https://github.com/ceph/ceph/pull/44636>`_, Radoslaw Zarzynski)
* doc/cephadm: remove duplicate deployment scenario section (`pr#44660 <https://github.com/ceph/ceph/pull/44660>`_, Melissa Li)
* doc/dev: s/repostory/repository/ (really) (`pr#45789 <https://github.com/ceph/ceph/pull/45789>`_, Zac Dover)
* doc/start: add testing support information (`pr#45989 <https://github.com/ceph/ceph/pull/45989>`_, Zac Dover)
* doc/start: include A. D'Atri's hardware-recs recs (`pr#45298 <https://github.com/ceph/ceph/pull/45298>`_, Zac Dover)
* doc/start: remove journal info from hardware recs (`pr#45123 <https://github.com/ceph/ceph/pull/45123>`_, Zac Dover)
* doc/start: remove osd stub from hardware recs (`pr#45316 <https://github.com/ceph/ceph/pull/45316>`_, Zac Dover)
* doc: prerequisites fix for cephFS mount (`pr#44272 <https://github.com/ceph/ceph/pull/44272>`_, Nikhilkumar Shelke)
* doc: Use older mistune (`pr#44226 <https://github.com/ceph/ceph/pull/44226>`_, David Galloway)
* Enable autotune for osd_memory_target on bootstrap (`pr#44633 <https://github.com/ceph/ceph/pull/44633>`_, Melissa Li)
* krbd: return error when no initial monitor address found (`pr#45003 <https://github.com/ceph/ceph/pull/45003>`_, Burt Holzman)
* librados: check latest osdmap on ENOENT in pool_reverse_lookup() (`pr#45586 <https://github.com/ceph/ceph/pull/45586>`_, Ilya Dryomov)
* librbd/cache/pwl: misc backports (`pr#44199 <https://github.com/ceph/ceph/pull/44199>`_, Jianpeng Ma, Jason Dillaman)
* librbd: diff-iterate reports incorrect offsets in fast-diff mode (`pr#44547 <https://github.com/ceph/ceph/pull/44547>`_, Ilya Dryomov)
* librbd: fix use-after-free on ictx in list_descendants() (`pr#44999 <https://github.com/ceph/ceph/pull/44999>`_, Ilya Dryomov, Wang ShuaiChao)
* librbd: fix various memory leaks (`pr#44998 <https://github.com/ceph/ceph/pull/44998>`_, Or Ozeri)
* librbd: make diff-iterate in fast-diff mode sort and merge reported extents (`pr#45638 <https://github.com/ceph/ceph/pull/45638>`_, Ilya Dryomov)
* librbd: readv/writev fix iovecs length computation overflow (`pr#45561 <https://github.com/ceph/ceph/pull/45561>`_, Jonas Pfefferle)
* librbd: restore diff-iterate include_parent functionality in fast-diff mode (`pr#44594 <https://github.com/ceph/ceph/pull/44594>`_, Ilya Dryomov)
* librgw: make rgw file handle versioned (`pr#45495 <https://github.com/ceph/ceph/pull/45495>`_, Xuehan Xu)
* librgw: treat empty root path as "/" on mount (`pr#43968 <https://github.com/ceph/ceph/pull/43968>`_, Matt Benjamin)
* mds,client: add new getvxattr op (`pr#45487 <https://github.com/ceph/ceph/pull/45487>`_, Milind Changire)
* mds: add mds_dir_max_entries config option (`pr#44512 <https://github.com/ceph/ceph/pull/44512>`_, Yongseok Oh)
* mds: directly return just after responding the link request (`pr#44620 <https://github.com/ceph/ceph/pull/44620>`_, Xiubo Li)
* mds: dump tree '/' when the path is empty (`pr#44622 <https://github.com/ceph/ceph/pull/44622>`_, Xiubo Li)
* mds: ensure that we send the btime in cap messages (`pr#45163 <https://github.com/ceph/ceph/pull/45163>`_, Jeff Layton)
* mds: fails to reintegrate strays if destdn's directory is full (ENOSPC) (`pr#44513 <https://github.com/ceph/ceph/pull/44513>`_, Patrick Donnelly)
* mds: fix seg fault in expire_recursive (`pr#45099 <https://github.com/ceph/ceph/pull/45099>`_, 胡玮文)
* mds: ignore unknown client op when tracking op latency (`pr#44975 <https://github.com/ceph/ceph/pull/44975>`_, Venky Shankar)
* mds: kill session when mds do ms_handle_remote_reset (`issue#53911 <http://tracker.ceph.com/issues/53911>`_, `pr#45100 <https://github.com/ceph/ceph/pull/45100>`_, YunfeiGuan)
* mds: mds_oft_prefetch_dirfrags default to false (`pr#45016 <https://github.com/ceph/ceph/pull/45016>`_, Dan van der Ster)
* mds: opening connection to up:replay/up:creating daemon causes message drop (`pr#44296 <https://github.com/ceph/ceph/pull/44296>`_, Patrick Donnelly)
* mds: PurgeQueue.cc fix for 32bit compilation (`pr#44168 <https://github.com/ceph/ceph/pull/44168>`_, Duncan Bellamy)
* mds: recursive scrub does not trigger stray reintegration (`pr#44514 <https://github.com/ceph/ceph/pull/44514>`_, Patrick Donnelly)
* mds: remove the duplicated or incorrect respond (`pr#44623 <https://github.com/ceph/ceph/pull/44623>`_, Xiubo Li)
* mds: reset heartbeat in each MDSContext complete() (`pr#44551 <https://github.com/ceph/ceph/pull/44551>`_, Xiubo Li)
* mgr/autoscaler: Introduce noautoscale flag (`pr#44540 <https://github.com/ceph/ceph/pull/44540>`_, Kamoltat)
* mgr/cephadm/iscsi: use `mon_command` in `post_remove` instead of `check_mon_command` (`pr#44830 <https://github.com/ceph/ceph/pull/44830>`_, Melissa Li)
* mgr/cephadm: Add client.admin keyring when upgrading from older version (`pr#44625 <https://github.com/ceph/ceph/pull/44625>`_, Sebastian Wagner)
* mgr/cephadm: add keep-alive requests to ssh connections (`pr#45632 <https://github.com/ceph/ceph/pull/45632>`_, Adam King)
* mgr/cephadm: Add snmp-gateway service support (`pr#44529 <https://github.com/ceph/ceph/pull/44529>`_, Sebastian Wagner, Paul Cuzner)
* mgr/cephadm: allow miscellaneous container args at service level (`pr#44829 <https://github.com/ceph/ceph/pull/44829>`_, Adam King)
* mgr/cephadm: auto-enable mirroring module when deploying service (`pr#44661 <https://github.com/ceph/ceph/pull/44661>`_, John Mulligan)
* mgr/cephadm: avoid repeated calls to get_module_option (`pr#44535 <https://github.com/ceph/ceph/pull/44535>`_, Sage Weil)
* mgr/cephadm: block draining last _admin host (`pr#45229 <https://github.com/ceph/ceph/pull/45229>`_, Adam King)
* mgr/cephadm: block removing last instance of _admin label (`pr#45231 <https://github.com/ceph/ceph/pull/45231>`_, Adam King)
* mgr/cephadm: Delete ceph.target if last cluster (`pr#45228 <https://github.com/ceph/ceph/pull/45228>`_, Redouane Kachach)
* mgr/cephadm: extend extra_container_args to other service types (`pr#45234 <https://github.com/ceph/ceph/pull/45234>`_, Adam King)
* mgr/cephadm: fix 'cephadm osd activate' on existing osd devices (`pr#44627 <https://github.com/ceph/ceph/pull/44627>`_, Sage Weil)
* mgr/cephadm: fix 'mgr/cephadm: spec.virtual_ip  param should be used by the ingress daemon (`pr#44628 <https://github.com/ceph/ceph/pull/44628>`_, Guillaume Abrioux, Francesco Pantano, Sebastian Wagner)
* mgr/cephadm: Fix count for OSDs with OSD specs (`pr#44629 <https://github.com/ceph/ceph/pull/44629>`_, Sebastian Wagner)
* mgr/cephadm: fix minor grammar nit in Dry-Runs message (`pr#44637 <https://github.com/ceph/ceph/pull/44637>`_, James McClune)
* mgr/cephadm: fix tcmu-runner cephadm_stray_daemon (`pr#44630 <https://github.com/ceph/ceph/pull/44630>`_, Melissa Li)
* mgr/cephadm: Fix test_facts (`pr#44530 <https://github.com/ceph/ceph/pull/44530>`_, Sebastian Wagner)
* mgr/cephadm: less log noise when config checks fail (`pr#44526 <https://github.com/ceph/ceph/pull/44526>`_, Sage Weil)
* mgr/cephadm: nfs migration: avoid port conflicts (`pr#44631 <https://github.com/ceph/ceph/pull/44631>`_, Sebastian Wagner)
* mgr/cephadm: Show an error when invalid format (`pr#45226 <https://github.com/ceph/ceph/pull/45226>`_, Redouane Kachach)
* mgr/cephadm: store contianer registry credentials in config-key (`pr#44658 <https://github.com/ceph/ceph/pull/44658>`_, Daniel Pivonka)
* mgr/cephadm: try to get FQDN for configuration files (`pr#45620 <https://github.com/ceph/ceph/pull/45620>`_, Tatjana Dehler)
* mgr/cephadm: update monitoring stack versions (`pr#45940 <https://github.com/ceph/ceph/pull/45940>`_, Aashish Sharma, Ernesto Puerta)
* mgr/cephadm: validating service_id for MDS (`pr#45227 <https://github.com/ceph/ceph/pull/45227>`_, Redouane Kachach)
* mgr/dashboard: "Please expand your cluster first" shouldn't be shown if cluster is already meaningfully running (`pr#45044 <https://github.com/ceph/ceph/pull/45044>`_, Volker Theile)
* mgr/dashboard: add test coverage for API docs (SwaggerUI) (`pr#44533 <https://github.com/ceph/ceph/pull/44533>`_, Alfonso Martínez)
* mgr/dashboard: avoid tooltip if disk_usage=null and fast-diff enabled (`pr#44149 <https://github.com/ceph/ceph/pull/44149>`_, Avan Thakkar)
* mgr/dashboard: cephadm e2e job improvements (`pr#44938 <https://github.com/ceph/ceph/pull/44938>`_, Nizamudeen A, Alfonso Martínez)
* mgr/dashboard: cephadm e2e job: improvements (`pr#44382 <https://github.com/ceph/ceph/pull/44382>`_, Alfonso Martínez)
* mgr/dashboard: change privacy protocol field from required to optional (`pr#45052 <https://github.com/ceph/ceph/pull/45052>`_, Avan Thakkar)
* mgr/dashboard: Cluster Expansion - Review Section: fixes and improvements (`pr#44389 <https://github.com/ceph/ceph/pull/44389>`_, Aashish Sharma)
* mgr/dashboard: Compare values of MTU alert by device (`pr#45813 <https://github.com/ceph/ceph/pull/45813>`_, Aashish Sharma, Patrick Seidensal)
* mgr/dashboard: dashboard does not show degraded objects if they are less than 0.5% under "Dashboard->Capacity->Objects block (`pr#44091 <https://github.com/ceph/ceph/pull/44091>`_, Aashish Sharma)
* mgr/dashboard: dashboard turns telemetry off when configuring report (`pr#45111 <https://github.com/ceph/ceph/pull/45111>`_, Sarthak0702, Aaryan Porwal)
* mgr/dashboard: datatable in Cluster Host page hides wrong column on selection (`pr#45861 <https://github.com/ceph/ceph/pull/45861>`_, Sarthak0702)
* mgr/dashboard: Directories Menu Can't Use on Ceph File System Dashboard (`pr#45028 <https://github.com/ceph/ceph/pull/45028>`_, Sarthak0702)
* mgr/dashboard: extend daemon actions to host details (`pr#45721 <https://github.com/ceph/ceph/pull/45721>`_, Nizamudeen A)
* mgr/dashboard: fix api test issue with pip (`pr#45880 <https://github.com/ceph/ceph/pull/45880>`_, Ernesto Puerta)
* mgr/dashboard: fix frontend deps' vulnerabilities (`pr#44297 <https://github.com/ceph/ceph/pull/44297>`_, Alfonso Martínez)
* mgr/dashboard: fix Grafana OSD/host panels (`pr#44775 <https://github.com/ceph/ceph/pull/44775>`_, Patrick Seidensal)
* mgr/dashboard: fix orchestrator/02-hosts-inventory.e2e failure (`pr#44467 <https://github.com/ceph/ceph/pull/44467>`_, Nizamudeen A)
* mgr/dashboard: fix timeout error in dashboard cephadm e2e job (`pr#44468 <https://github.com/ceph/ceph/pull/44468>`_, Nizamudeen A)
* mgr/dashboard: fix white screen on Safari (`pr#45301 <https://github.com/ceph/ceph/pull/45301>`_, 胡玮文)
* mgr/dashboard: fix: get SMART data from single-daemon device (`pr#44597 <https://github.com/ceph/ceph/pull/44597>`_, Alfonso Martínez)
* mgr/dashboard: highlight the search text in cluster logs (`pr#45678 <https://github.com/ceph/ceph/pull/45678>`_, Sarthak0702)
* mgr/dashboard: Implement drain host functionality in dashboard (`pr#44376 <https://github.com/ceph/ceph/pull/44376>`_, Nizamudeen A)
* mgr/dashboard: Improve notifications for osd nearfull, full (`pr#44876 <https://github.com/ceph/ceph/pull/44876>`_, Aashish Sharma)
* mgr/dashboard: Imrove error message of '/api/grafana/validation' API endpoint (`pr#45956 <https://github.com/ceph/ceph/pull/45956>`_, Volker Theile)
* mgr/dashboard: introduce HAProxy metrics for RGW (`pr#44273 <https://github.com/ceph/ceph/pull/44273>`_, Avan Thakkar)
* mgr/dashboard: introduce separate front-end component for API docs (`pr#44400 <https://github.com/ceph/ceph/pull/44400>`_, Aashish Sharma)
* mgr/dashboard: Language dropdown box is partly hidden on login page (`pr#45618 <https://github.com/ceph/ceph/pull/45618>`_, Volker Theile)
* mgr/dashboard: monitoring:Implement BlueStore onode hit/miss counters into the dashboard (`pr#44650 <https://github.com/ceph/ceph/pull/44650>`_, Aashish Sharma)
* mgr/dashboard: NFS non-existent files cleanup (`pr#44046 <https://github.com/ceph/ceph/pull/44046>`_, Alfonso Martínez)
* mgr/dashboard: NFS pages shows 'Page not found' (`pr#45723 <https://github.com/ceph/ceph/pull/45723>`_, Volker Theile)
* mgr/dashboard: Notification banners at the top of the UI have fixed height (`pr#44756 <https://github.com/ceph/ceph/pull/44756>`_, Nizamudeen A, Waad AlKhoury)
* mgr/dashboard: perform daemon actions (`pr#45203 <https://github.com/ceph/ceph/pull/45203>`_, Pere Diaz Bou)
* mgr/dashboard: Pull latest translations from Transifex (`pr#45418 <https://github.com/ceph/ceph/pull/45418>`_, Volker Theile)
* mgr/dashboard: Refactoring dashboard cephadm checks (`pr#44652 <https://github.com/ceph/ceph/pull/44652>`_, Nizamudeen A)
* mgr/dashboard: RGW users and buckets tables are empty if the selected gateway is down (`pr#45868 <https://github.com/ceph/ceph/pull/45868>`_, Volker Theile)
* mgr/dashboard: run-backend-api-tests.sh: Older setuptools (`pr#44377 <https://github.com/ceph/ceph/pull/44377>`_, David Galloway)
* mgr/dashboard: set appropriate baseline branch for applitools (`pr#44935 <https://github.com/ceph/ceph/pull/44935>`_, Nizamudeen A)
* mgr/dashboard: support snmp-gateway service creation from UI (`pr#44977 <https://github.com/ceph/ceph/pull/44977>`_, Avan Thakkar)
* mgr/dashboard: Table columns hiding fix (`issue#51119 <http://tracker.ceph.com/issues/51119>`_, `pr#45725 <https://github.com/ceph/ceph/pull/45725>`_, Daniel Persson)
* mgr/dashboard: Update Angular version to 12 (`pr#44534 <https://github.com/ceph/ceph/pull/44534>`_, Ernesto Puerta, Nizamudeen A)
* mgr/dashboard: upgrade Cypress to the latest stable version (`pr#44086 <https://github.com/ceph/ceph/pull/44086>`_, Sage Weil, Alfonso Martínez)
* mgr/dashboard: use -f for npm ci to skip fsevents error (`pr#44105 <https://github.com/ceph/ceph/pull/44105>`_, Duncan Bellamy)
* mgr/devicehealth: fix missing timezone from time delta calculation (`pr#44325 <https://github.com/ceph/ceph/pull/44325>`_, Yaarit Hatuka)
* mgr/devicehealth: skip null pages when extracting wear level (`pr#45151 <https://github.com/ceph/ceph/pull/45151>`_, Yaarit Hatuka)
* mgr/nfs: allow dynamic update of cephfs nfs export (`pr#45543 <https://github.com/ceph/ceph/pull/45543>`_, Ramana Raja)
* mgr/nfs: support managing exports without orchestration enabled (`pr#45508 <https://github.com/ceph/ceph/pull/45508>`_, John Mulligan)
* mgr/orchestrator: add filtering and count option for orch host ls (`pr#44531 <https://github.com/ceph/ceph/pull/44531>`_, Adam King)
* mgr/prometheus: Added `avail_raw` field for Pools DF Prometheus mgr module (`pr#45236 <https://github.com/ceph/ceph/pull/45236>`_, Konstantin Shalygin)
* mgr/prometheus: define module options for standby (`pr#44205 <https://github.com/ceph/ceph/pull/44205>`_, Sage Weil)
* mgr/prometheus: expose ceph healthchecks as metrics (`pr#44480 <https://github.com/ceph/ceph/pull/44480>`_, Paul Cuzner, Sebastian Wagner)
* mgr/prometheus: Fix metric types from gauge to counter (`pr#43187 <https://github.com/ceph/ceph/pull/43187>`_, Patrick Seidensal)
* mgr/prometheus: Fix the per method stats exported (`pr#44146 <https://github.com/ceph/ceph/pull/44146>`_, Paul Cuzner)
* mgr/prometheus: Make prometheus standby behaviour configurable (`pr#43897 <https://github.com/ceph/ceph/pull/43897>`_, Roland Sommer)
* mgr/rbd_support: cast pool_id from int to str when collecting LevelSpec (`pr#45532 <https://github.com/ceph/ceph/pull/45532>`_, Ilya Dryomov)
* mgr/rbd_support: fix schedule remove (`pr#45005 <https://github.com/ceph/ceph/pull/45005>`_, Sunny Kumar)
* mgr/snap_schedule: backports (`pr#45906 <https://github.com/ceph/ceph/pull/45906>`_, Venky Shankar, Milind Changire)
* mgr/stats: exception handling for ceph fs perf stats command (`pr#44516 <https://github.com/ceph/ceph/pull/44516>`_, Nikhilkumar Shelke)
* mgr/telemetry: fix waiting for mgr to warm up (`pr#45773 <https://github.com/ceph/ceph/pull/45773>`_, Yaarit Hatuka)
* mgr/volumes: A few mgr volumes pacific backports (`pr#45205 <https://github.com/ceph/ceph/pull/45205>`_, Kotresh HR)
* mgr/volumes: Subvolume removal and clone failure fixes (`pr#42932 <https://github.com/ceph/ceph/pull/42932>`_, Kotresh HR)
* mgr/volumes: the 'mode' should honor idempotent subvolume creation (`pr#45474 <https://github.com/ceph/ceph/pull/45474>`_, Nikhilkumar Shelke)
* mgr: Fix ceph_daemon label in ceph_rgw\_\* metrics (`pr#44885 <https://github.com/ceph/ceph/pull/44885>`_, Benoît Knecht)
* mgr: fix locking for MetadataUpdate::finish (`pr#44212 <https://github.com/ceph/ceph/pull/44212>`_, Sage Weil)
* mgr: TTL Cache in mgr module (`pr#44750 <https://github.com/ceph/ceph/pull/44750>`_, Waad AlKhoury, Pere Diaz Bou)
* mgr: various fixes for mgr scalability (`pr#44869 <https://github.com/ceph/ceph/pull/44869>`_, Neha Ojha, Sage Weil)
* mon/MDSMonitor: sanity assert when inline data turned on in MDSMap from v16.2.4 -> v16.2.[567] (`pr#44910 <https://github.com/ceph/ceph/pull/44910>`_, Patrick Donnelly)
* mon/MgrStatMonitor: do not spam subscribers (mgr) with service_map (`pr#44721 <https://github.com/ceph/ceph/pull/44721>`_, Sage Weil)
* mon/MonCommands.h: fix target_size_ratio range (`pr#45397 <https://github.com/ceph/ceph/pull/45397>`_, Kamoltat)
* mon/OSDMonitor: avoid null dereference if stats are not available (`pr#44698 <https://github.com/ceph/ceph/pull/44698>`_, Josh Durgin)
* mon: Abort device health when device not found (`pr#44959 <https://github.com/ceph/ceph/pull/44959>`_, Benoît Knecht)
* mon: do not quickly mark mds laggy when MON_DOWN (`pr#43698 <https://github.com/ceph/ceph/pull/43698>`_, Sage Weil, Patrick Donnelly)
* mon: Omit MANY_OBJECTS_PER_PG warning when autoscaler is on (`pr#45152 <https://github.com/ceph/ceph/pull/45152>`_, Christopher Hoffman)
* mon: osd pool create <pool-name> with --bulk flag (`pr#44847 <https://github.com/ceph/ceph/pull/44847>`_, Kamoltat)
* mon: prevent new sessions during shutdown (`pr#44543 <https://github.com/ceph/ceph/pull/44543>`_, Sage Weil)
* monitoring/grafana: Grafana query tester (`pr#44316 <https://github.com/ceph/ceph/pull/44316>`_, Ernesto Puerta, Pere Diaz Bou)
* monitoring: mention PyYAML only once in requirements (`pr#44944 <https://github.com/ceph/ceph/pull/44944>`_, Rishabh Dave)
* os/bluestore/AvlAllocator: introduce bluestore_avl_alloc_ff_max\_\* options (`pr#43745 <https://github.com/ceph/ceph/pull/43745>`_, Kefu Chai, Mauricio Faria de Oliveira, Adam Kupczyk)
* os/bluestore: avoid premature onode release (`pr#44723 <https://github.com/ceph/ceph/pull/44723>`_, Igor Fedotov)
* os/bluestore: make shared blob fsck much less RAM-greedy (`pr#44613 <https://github.com/ceph/ceph/pull/44613>`_, Igor Fedotov)
* os/bluestore: use proper prefix when removing undecodable Share Blob (`pr#43882 <https://github.com/ceph/ceph/pull/43882>`_, Igor Fedotov)
* osd/OSD: Log aggregated slow ops detail to cluster logs (`pr#44771 <https://github.com/ceph/ceph/pull/44771>`_, Prashant D)
* osd/OSDMap.cc: clean up pg_temp for nonexistent pgs (`pr#44096 <https://github.com/ceph/ceph/pull/44096>`_, Cory Snyder)
* osd/OSDMap: Add health warning if 'require-osd-release' != current release (`pr#44259 <https://github.com/ceph/ceph/pull/44259>`_, Sridhar Seshasayee, Patrick Donnelly, Neha Ojha)
* osd/OSDMapMapping: fix spurious threadpool timeout errors (`pr#44545 <https://github.com/ceph/ceph/pull/44545>`_, Sage Weil)
* osd/PeeringState: separate history's pruub from pg's (`pr#44584 <https://github.com/ceph/ceph/pull/44584>`_, Sage Weil)
* osd/PrimaryLogPG.cc: CEPH_OSD_OP_OMAPRMKEYRANGE should mark omap dirty (`pr#45591 <https://github.com/ceph/ceph/pull/45591>`_, Neha Ojha)
* osd/scrub: destruct the scrubber shortly before the PG is destructed (`pr#45731 <https://github.com/ceph/ceph/pull/45731>`_, Ronen Friedman)
* osd/scrub: only telling the scrubber of awaited-for 'updates' events (`pr#45365 <https://github.com/ceph/ceph/pull/45365>`_, Ronen Friedman)
* osd/scrub: remove reliance of Scrubber objects' logging on the PG (`pr#45729 <https://github.com/ceph/ceph/pull/45729>`_, Ronen Friedman)
* osd/scrub: restart snap trimming only after scrubbing is done (`pr#45785 <https://github.com/ceph/ceph/pull/45785>`_, Ronen Friedman)
* osd/scrub: stop sending bogus digest-update events (`issue#54423 <http://tracker.ceph.com/issues/54423>`_, `pr#45194 <https://github.com/ceph/ceph/pull/45194>`_, Ronen Friedman)
* osd/scrub: tag replica scrub messages to identify stale events (`pr#45374 <https://github.com/ceph/ceph/pull/45374>`_, Ronen Friedman)
* osd: add pg_num_max value & pg_num_max reordering (`pr#45173 <https://github.com/ceph/ceph/pull/45173>`_, Kamoltat, Sage Weil)
* osd: fix 'ceph osd stop <osd.nnn>' doesn't take effect (`pr#43955 <https://github.com/ceph/ceph/pull/43955>`_, tan changzhi)
* osd: fix the truncation of an int by int division (`pr#45376 <https://github.com/ceph/ceph/pull/45376>`_, Ronen Friedman)
* osd: PeeringState: fix selection order in calc_replicated_acting_stretch (`pr#44664 <https://github.com/ceph/ceph/pull/44664>`_, Greg Farnum)
* osd: recover unreadable snapshot before reading ref. count info (`pr#44181 <https://github.com/ceph/ceph/pull/44181>`_, Myoungwon Oh)
* osd: require osd_pg_max_concurrent_snap_trims > 0 (`pr#45323 <https://github.com/ceph/ceph/pull/45323>`_, Dan van der Ster)
* osd: set r only if succeed in FillInVerifyExtent (`pr#44173 <https://github.com/ceph/ceph/pull/44173>`_, yanqiang-ux)
* osdc: add set_error in BufferHead, when split set_error to right (`pr#44725 <https://github.com/ceph/ceph/pull/44725>`_, jiawd)
* pacfic: doc/rados/operations/placement-groups: fix --bulk docs (`pr#45328 <https://github.com/ceph/ceph/pull/45328>`_, Kamoltat)
* Pacific fast shutdown backports (`pr#45654 <https://github.com/ceph/ceph/pull/45654>`_, Sridhar Seshasayee, Nitzan Mordechai, Satoru Takeuchi)
* pybind/mgr/balancer: define Plan.{dump,show}() (`pr#43964 <https://github.com/ceph/ceph/pull/43964>`_, Kefu Chai)
* pybind/mgr/progress: enforced try and except on accessing event dictionary (`pr#44672 <https://github.com/ceph/ceph/pull/44672>`_, Kamoltat)
* python-common: add int value validation for count and count_per_host (`pr#44527 <https://github.com/ceph/ceph/pull/44527>`_, John Mulligan)
* python-common: improve OSD spec error messages (`pr#44626 <https://github.com/ceph/ceph/pull/44626>`_, Sebastian Wagner)
* qa/distros/podman: remove centos_8.2 and centos_8.3 (`pr#44903 <https://github.com/ceph/ceph/pull/44903>`_, Neha Ojha)
* qa/rgw: add failing tempest test to blocklist (`pr#45436 <https://github.com/ceph/ceph/pull/45436>`_, Casey Bodley)
* qa/rgw: barbican and pykmip tasks upgrade pip before installing pytz (`pr#45444 <https://github.com/ceph/ceph/pull/45444>`_, Casey Bodley)
* qa/rgw: bump tempest version to resolve dependency issue (`pr#43966 <https://github.com/ceph/ceph/pull/43966>`_, Casey Bodley)
* qa/rgw: Fix vault token file access (`issue#51539 <http://tracker.ceph.com/issues/51539>`_, `pr#43951 <https://github.com/ceph/ceph/pull/43951>`_, Marcus Watts)
* qa/rgw: update apache-maven mirror for rgw/hadoop-s3a (`pr#45445 <https://github.com/ceph/ceph/pull/45445>`_, Casey Bodley)
* qa/rgw: use symlinks for rgw/sts suite, target supported-random-distro$ (`pr#45245 <https://github.com/ceph/ceph/pull/45245>`_, Casey Bodley)
* qa/run-tox-mgr-dashboard: Do not write to /tmp/test_sanitize_password… (`pr#44727 <https://github.com/ceph/ceph/pull/44727>`_, Kevin Zhao)
* qa/run_xfstests_qemu.sh: stop reporting success without actually running any tests (`pr#44596 <https://github.com/ceph/ceph/pull/44596>`_, Ilya Dryomov)
* qa/suites/fs: add prefetch_dirfrags false to thrasher suite (`pr#44504 <https://github.com/ceph/ceph/pull/44504>`_, Arthur Outhenin-Chalandre)
* qa/suites/orch/cephadm: Also run the rbd/iscsi suite (`pr#44635 <https://github.com/ceph/ceph/pull/44635>`_, Sebastian Wagner)
* qa/tasks/qemu: make sure block-rbd.so is installed (`pr#45072 <https://github.com/ceph/ceph/pull/45072>`_, Ilya Dryomov)
* qa/tasks: improve backfill_toofull test (`pr#44387 <https://github.com/ceph/ceph/pull/44387>`_, Mykola Golub)
* qa/tests: added upgrade-clients/client-upgrade-pacific-quincy test (`pr#45326 <https://github.com/ceph/ceph/pull/45326>`_, Yuri Weinstein)
* qa/tests: replaced 16.2.6 with 16.2.7 version (`pr#44369 <https://github.com/ceph/ceph/pull/44369>`_, Yuri Weinstein)
* qa: adjust for MDSs to get deployed before verifying their availability (`issue#53857 <http://tracker.ceph.com/issues/53857>`_, `pr#44639 <https://github.com/ceph/ceph/pull/44639>`_, Venky Shankar)
* qa: Default to CentOS 8 Stream (`pr#44889 <https://github.com/ceph/ceph/pull/44889>`_, David Galloway)
* qa: do not use any time related suffix for \*_op_timeouts (`pr#44621 <https://github.com/ceph/ceph/pull/44621>`_, Xiubo Li)
* qa: fsync dir for asynchronous creat on stray tests (`pr#45565 <https://github.com/ceph/ceph/pull/45565>`_, Patrick Donnelly, Ramana Raja)
* qa: ignore expected metadata cluster log error (`pr#45564 <https://github.com/ceph/ceph/pull/45564>`_, Patrick Donnelly)
* qa: increase the timeout value to wait a litte longer (`pr#43979 <https://github.com/ceph/ceph/pull/43979>`_, Xiubo Li)
* qa: move certificates for kmip task into /etc/ceph (`pr#45413 <https://github.com/ceph/ceph/pull/45413>`_, Ali Maredia)
* qa: remove centos8 from supported distros (`pr#44865 <https://github.com/ceph/ceph/pull/44865>`_, Casey Bodley, Sage Weil)
* qa: skip sanity check during upgrade (`pr#44840 <https://github.com/ceph/ceph/pull/44840>`_, Milind Changire)
* qa: split distro for rados/cephadm/smoke tests (`pr#44681 <https://github.com/ceph/ceph/pull/44681>`_, Guillaume Abrioux)
* qa: wait for purge queue operations to finish (`issue#52487 <http://tracker.ceph.com/issues/52487>`_, `pr#44642 <https://github.com/ceph/ceph/pull/44642>`_, Venky Shankar)
* radosgw-admin: 'sync status' is not behind if there are no mdlog entries (`pr#45442 <https://github.com/ceph/ceph/pull/45442>`_, Casey Bodley)
* rbd persistent cache UX improvements (status report, metrics, flush command) (`pr#45895 <https://github.com/ceph/ceph/pull/45895>`_, Ilya Dryomov, Yin Congmin)
* rbd-mirror: fix races in snapshot-based mirroring deletion propagation (`pr#44754 <https://github.com/ceph/ceph/pull/44754>`_, Ilya Dryomov)
* rbd-mirror: make mirror properly detect pool replayer needs restart (`pr#45170 <https://github.com/ceph/ceph/pull/45170>`_, Mykola Golub)
* rbd-mirror: make RemoveImmediateUpdate test synchronous (`pr#44094 <https://github.com/ceph/ceph/pull/44094>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: synchronize with in-flight stop in ImageReplayer::stop() (`pr#45184 <https://github.com/ceph/ceph/pull/45184>`_, Ilya Dryomov)
* rbd: add missing switch arguments for recognition by get_command_spec() (`pr#44742 <https://github.com/ceph/ceph/pull/44742>`_, Ilya Dryomov)
* rbd: mark optional positional arguments as such in help output (`pr#45008 <https://github.com/ceph/ceph/pull/45008>`_, Ilya Dryomov)
* rbd: recognize rxbounce map option (`pr#45002 <https://github.com/ceph/ceph/pull/45002>`_, Ilya Dryomov)
* Revert "mds: kill session when mds do ms_handle_remote_reset" (`pr#45557 <https://github.com/ceph/ceph/pull/45557>`_, Venky Shankar)
* revert bootstrap network handling changes (`pr#46085 <https://github.com/ceph/ceph/pull/46085>`_, Adam King)
* revival and backport of fix for RocksDB optimized iterators (`pr#46096 <https://github.com/ceph/ceph/pull/46096>`_, Adam Kupczyk, Cory Snyder)
* RGW - Zipper - Make default args match in get_obj_state (`pr#45438 <https://github.com/ceph/ceph/pull/45438>`_, Daniel Gryniewicz)
* RGW - Zipper - Make sure PostObj has bucket set (`pr#45060 <https://github.com/ceph/ceph/pull/45060>`_, Daniel Gryniewicz)
* rgw/admin: fix radosgw-admin datalog list max-entries issue (`pr#45500 <https://github.com/ceph/ceph/pull/45500>`_, Yuval Lifshitz)
* rgw/amqp: add default case to silence compiler warning (`pr#45478 <https://github.com/ceph/ceph/pull/45478>`_, Casey Bodley)
* rgw/amqp: remove the explicit "disconnect()" interface (`pr#45427 <https://github.com/ceph/ceph/pull/45427>`_, Yuval Lifshitz)
* rgw/beast: optimizations for request timeout (`pr#43946 <https://github.com/ceph/ceph/pull/43946>`_, Mark Kogan, Casey Bodley)
* rgw/notification: send correct size in COPY events (`pr#45426 <https://github.com/ceph/ceph/pull/45426>`_, Yuval Lifshitz)
* rgw/sts: adding role name and role session to ops log (`pr#43956 <https://github.com/ceph/ceph/pull/43956>`_, Pritha Srivastava)
* rgw: add object null point judging when listing pubsub  topics (`pr#45476 <https://github.com/ceph/ceph/pull/45476>`_, zhipeng li)
* rgw: add OPT_BUCKET_SYNC_RUN to gc_ops_list, so that (`pr#45421 <https://github.com/ceph/ceph/pull/45421>`_, Pritha Srivastava)
* rgw: add the condition of lock mode conversion to PutObjRentention (`pr#45440 <https://github.com/ceph/ceph/pull/45440>`_, wangzhong)
* rgw: bucket chown bad memory usage (`pr#45491 <https://github.com/ceph/ceph/pull/45491>`_, Mohammad Fatemipour)
* rgw: change order of xml elements in ListRoles response (`pr#45448 <https://github.com/ceph/ceph/pull/45448>`_, Casey Bodley)
* rgw: clean-up logging of function entering to make thorough and consistent (`pr#45450 <https://github.com/ceph/ceph/pull/45450>`_, J. Eric Ivancich)
* rgw: cls_bucket_list_unordered() might return one redundent entry every time is_truncated is true (`pr#45457 <https://github.com/ceph/ceph/pull/45457>`_, Peng Zhang)
* rgw: default ms_mon_client_mode = secure (`pr#45439 <https://github.com/ceph/ceph/pull/45439>`_, Sage Weil)
* rgw: document rgw_lc_debug_interval configuration option (`pr#45453 <https://github.com/ceph/ceph/pull/45453>`_, J. Eric Ivancich)
* rgw: document S3 bucket replication support (`pr#45484 <https://github.com/ceph/ceph/pull/45484>`_, Matt Benjamin)
* rgw: Dump Object Lock Retain Date as ISO 8601 (`pr#44697 <https://github.com/ceph/ceph/pull/44697>`_, Danny Abukalam)
* rgw: fix `bi put` not using right bucket index shard (`pr#44166 <https://github.com/ceph/ceph/pull/44166>`_, J. Eric Ivancich)
* rgw: fix lock scope in ObjectCache::get() (`pr#44747 <https://github.com/ceph/ceph/pull/44747>`_, Casey Bodley)
* rgw: fix md5 not match for RGWBulkUploadOp upload when enable rgw com… (`pr#45432 <https://github.com/ceph/ceph/pull/45432>`_, yuliyang_yewu)
* rgw: fix rgw.none statistics (`pr#45463 <https://github.com/ceph/ceph/pull/45463>`_, J. Eric Ivancich)
* rgw: fix segfault in UserAsyncRefreshHandler::init_fetch (`pr#45411 <https://github.com/ceph/ceph/pull/45411>`_, Cory Snyder)
* rgw: forward request in multisite for RGWDeleteBucketPolicy and RGWDeleteBucketPublicAccessBlock (`pr#45434 <https://github.com/ceph/ceph/pull/45434>`_, yuliyang_yewu)
* rgw: have "bucket check --fix" fix pool ids correctly (`pr#45455 <https://github.com/ceph/ceph/pull/45455>`_, J. Eric Ivancich)
* rgw: in bucket reshard list, clarify new num shards is tentative (`pr#45509 <https://github.com/ceph/ceph/pull/45509>`_, J. Eric Ivancich)
* rgw: init bucket index only if putting bucket instance info succeeds (`pr#45480 <https://github.com/ceph/ceph/pull/45480>`_, Huber-ming)
* rgw: RadosBucket::get_bucket_info() updates RGWBucketEnt (`pr#45483 <https://github.com/ceph/ceph/pull/45483>`_, Casey Bodley)
* rgw: remove bucket API returns NoSuchKey than NoSuchBucket (`pr#45489 <https://github.com/ceph/ceph/pull/45489>`_, Satoru Takeuchi)
* rgw: resolve empty ordered bucket listing results w/ CLS filtering \*and\* bucket index list produces incorrect result when non-ascii entries (`pr#45087 <https://github.com/ceph/ceph/pull/45087>`_, J. Eric Ivancich)
* rgw: RGWPostObj::execute() may lost data (`pr#45502 <https://github.com/ceph/ceph/pull/45502>`_, Lei Zhang)
* rgw: under fips, set flag to allow md5 in select rgw ops (`pr#44778 <https://github.com/ceph/ceph/pull/44778>`_, Mark Kogan)
* rgw: url_decode before parsing copysource in copyobject (`issue#43259 <http://tracker.ceph.com/issues/43259>`_, `pr#45430 <https://github.com/ceph/ceph/pull/45430>`_, Paul Reece)
* rgw: user stats showing 0 value for "size_utilized" and "size_kb_utilized" fields (`pr#44171 <https://github.com/ceph/ceph/pull/44171>`_, J. Eric Ivancich)
* rgw: write meta of a MP part to a correct pool (`issue#49128 <http://tracker.ceph.com/issues/49128>`_, `pr#45428 <https://github.com/ceph/ceph/pull/45428>`_, Jeegn Chen)
* rgw:When KMS encryption is used and the key does not exist, we should… (`pr#45461 <https://github.com/ceph/ceph/pull/45461>`_, wangyingbin)
* rgwlc:  remove lc entry on bucket delete (`pr#44729 <https://github.com/ceph/ceph/pull/44729>`_, Matt Benjamin)
* rgwlc:  warn on missing RGW_ATTR_LC (`pr#45497 <https://github.com/ceph/ceph/pull/45497>`_, Matt Benjamin)
* src/ceph-crash.in: various enhancements and fixes (`pr#45381 <https://github.com/ceph/ceph/pull/45381>`_, Sébastien Han)
* src/rgw: Fix for malformed url (`pr#45459 <https://github.com/ceph/ceph/pull/45459>`_, Kalpesh Pandya)
* test/librbd/test_notify.py: effect post object map rebuild assert (`pr#45311 <https://github.com/ceph/ceph/pull/45311>`_, Ilya Dryomov)
* test/librbd: add test to verify diff_iterate size (`pr#45555 <https://github.com/ceph/ceph/pull/45555>`_, Christopher Hoffman)
* test/librbd: harden RemoveFullTry tests (`pr#43649 <https://github.com/ceph/ceph/pull/43649>`_, Ilya Dryomov)
* test/rgw: disable cls_rgw_gc test cases with defer_gc() (`pr#45477 <https://github.com/ceph/ceph/pull/45477>`_, Casey Bodley)
* test: fix wrong alarm (HitSetWrite) (`pr#45319 <https://github.com/ceph/ceph/pull/45319>`_, Myoungwon Oh)
* test: increase retry duration when calculating manifest ref. count (`pr#44202 <https://github.com/ceph/ceph/pull/44202>`_, Myoungwon Oh)
* tools/rbd: expand where option rbd_default_map_options can be set (`pr#45181 <https://github.com/ceph/ceph/pull/45181>`_, Christopher Hoffman, Ilya Dryomov)
* Wip doc pr 46109 backport to pacific (`pr#46117 <https://github.com/ceph/ceph/pull/46117>`_, Ville Ojamo)


v16.2.7 Pacific
===============

This is the seventh backport release in the Pacific series.

Notable Changes
---------------

* Critical bug in OMAP format upgrade is fixed. This could cause data corruption
  (improperly formatted OMAP keys) after pre-Pacific cluster upgrade if
  bluestore-quick-fix-on-mount parameter is set to true or ceph-bluestore-tool's
  quick-fix/repair commands are invoked.
  Relevant tracker: https://tracker.ceph.com/issues/53062
  ``bluestore-quick-fix-on-mount`` continues to be set to false, by default.

* CephFS:  If you are not using cephadm, you must disable FSMap sanity checks *before starting the upgrade*::

      ceph config set mon mon_mds_skip_sanity true

  After the upgrade has finished and the cluster is stable, please remove that setting::

      ceph config rm mon mon_mds_skip_sanity

  Clusters managed by and upgraded using cephadm take care of this step automatically.

* MGR: The pg_autoscaler will use the 'scale-up' profile as the default profile.
  16.2.6 changed the default profile to 'scale-down' but we ran into issues
  with the device_health_metrics pool consuming too many PGs, which is not ideal
  for performance. So we will continue to use the 'scale-up' profile by default,
  until we implement a limit on the number of PGs default pools should consume,
  in combination with the 'scale-down' profile.

* Cephadm & Ceph Dashboard: NFS management has been completely reworked to
  ensure that NFS exports are managed consistently across the different Ceph
  components. Prior to this, there were 3 incompatible implementations for
  configuring the NFS exports: Ceph-Ansible/OpenStack Manila, Ceph Dashboard and
  'mgr/nfs' module. With this release the 'mgr/nfs' way becomes the official
  interface, and the remaining components (Cephadm and Ceph Dashboard) adhere to
  it. While this might require manually migrating from the deprecated
  implementations, it will simplify the user experience for those heavily
  relying on NFS exports.

* Dashboard: "Cluster Expansion Wizard". After the 'cephadm bootstrap' step,
  users that log into the Ceph Dashboard will be presented with a welcome
  screen. If they choose to follow the installation wizard, they will be guided
  through a set of steps to help them configure their Ceph cluster: expanding
  the cluster by adding more hosts, detecting and defining their storage
  devices, and finally deploying and configuring the different Ceph services.

* OSD: When using mclock_scheduler for QoS, there is no longer a need to run any
  manual benchmark. The OSD now automatically sets an appropriate value for
  `osd_mclock_max_capacity_iops` by running a simple benchmark during
  initialization.

* MGR: The global recovery event in the progress module has been optimized and
  a `sleep_interval` of 5 seconds has been added between stats collection,
  to reduce the impact of the progress module on the MGR, especially in large
  clusters.

Changelog
---------

* rpm, debian: move smartmontools and nvme-cli to ceph-base (`pr#44164 <https://github.com/ceph/ceph/pull/44164>`_, Yaarit Hatuka)
* qa: miscellaneous perf suite fixes (`pr#44154 <https://github.com/ceph/ceph/pull/44154>`_, Neha Ojha)
* qa/suites/orch/cephadm: mgr-nfs-upgrade: add missing 0-distro dir (`pr#44201 <https://github.com/ceph/ceph/pull/44201>`_, Sebastian Wagner)
* \*: s/virtualenv/python -m venv/ (`pr#43002 <https://github.com/ceph/ceph/pull/43002>`_, Kefu Chai, Ken Dreyer)
* admin/doc-requirements.txt: pin Sphinx at 3.5.4 (`pr#43748 <https://github.com/ceph/ceph/pull/43748>`_, Kefu Chai)
* backport mgr/nfs bits (`pr#43811 <https://github.com/ceph/ceph/pull/43811>`_, Sage Weil, Michael Fritch)
* ceph-volume: `get_first_lv()` refactor (`pr#43960 <https://github.com/ceph/ceph/pull/43960>`_, Guillaume Abrioux)
* ceph-volume: fix a typo causing AttributeError (`pr#43949 <https://github.com/ceph/ceph/pull/43949>`_, Taha Jahangir)
* ceph-volume: fix bug with miscalculation of required db/wal slot size for VGs with multiple PVs (`pr#43948 <https://github.com/ceph/ceph/pull/43948>`_, Guillaume Abrioux, Cory Snyder)
* ceph-volume: fix lvm activate --all --no-systemd (`pr#43267 <https://github.com/ceph/ceph/pull/43267>`_, Dimitri Savineau)
* ceph-volume: util/prepare fix osd_id_available() (`pr#43708 <https://github.com/ceph/ceph/pull/43708>`_, Guillaume Abrioux)
* ceph.spec: selinux scripts respect CEPH_AUTO_RESTART_ON_UPGRADE (`pr#43235 <https://github.com/ceph/ceph/pull/43235>`_, Dan van der Ster)
* cephadm: November batch (`pr#43906 <https://github.com/ceph/ceph/pull/43906>`_, Sebastian Wagner, Sage Weil, Daniel Pivonka, Andrew Sharapov, Paul Cuzner, Adam King, Melissa Li)
* cephadm: October batch (`pr#43728 <https://github.com/ceph/ceph/pull/43728>`_, Patrick Donnelly, Sage Weil, Cory Snyder, Sebastian Wagner, Paul Cuzner, Joao Eduardo Luis, Zac Dover, Dmitry Kvashnin, Daniel Pivonka, Adam King, jianglong01, Guillaume Abrioux, Melissa Li, Roaa Sakr, Kefu Chai, Brad Hubbard, Michael Fritch, Javier Cacheiro)
* cephfs-mirror, test: add thrasher for cephfs mirror daemon, HA test yamls (`issue#50372 <http://tracker.ceph.com/issues/50372>`_, `pr#43924 <https://github.com/ceph/ceph/pull/43924>`_, Venky Shankar)
* cephfs-mirror: shutdown ClusterWatcher on termination (`pr#43198 <https://github.com/ceph/ceph/pull/43198>`_, Willem Jan Withagen, Venky Shankar)
* cmake: link Threads::Threads instead of CMAKE_THREAD_LIBS_INIT (`pr#43167 <https://github.com/ceph/ceph/pull/43167>`_, Ken Dreyer)
* cmake: s/Python_EXECUTABLE/Python3_EXECUTABLE/ (`pr#43264 <https://github.com/ceph/ceph/pull/43264>`_, Michael Fritch)
* crush: cancel upmaps with up set size != pool size (`pr#43415 <https://github.com/ceph/ceph/pull/43415>`_, huangjun)
* doc/radosgw/nfs: add note about NFSv3 deprecation (`pr#43941 <https://github.com/ceph/ceph/pull/43941>`_, Michael Fritch)
* doc: document subvolume (group) pins (`pr#43925 <https://github.com/ceph/ceph/pull/43925>`_, Patrick Donnelly)
* github: add dashboard PRs to Dashboard project (`pr#43610 <https://github.com/ceph/ceph/pull/43610>`_, Ernesto Puerta)
* librbd/cache/pwl: persistant cache backports (`pr#43772 <https://github.com/ceph/ceph/pull/43772>`_, Kefu Chai, Yingxin Cheng, Yin Congmin, Feng Hualong, Jianpeng Ma, Ilya Dryomov, Hualong Feng)
* librbd/cache/pwl: SSD caching backports (`pr#43918 <https://github.com/ceph/ceph/pull/43918>`_, Yin Congmin, Jianpeng Ma)
* librbd/object_map: rbd diff between two snapshots lists entire image content (`pr#43805 <https://github.com/ceph/ceph/pull/43805>`_, Sunny Kumar)
* librbd: fix pool validation lockup (`pr#43113 <https://github.com/ceph/ceph/pull/43113>`_, Ilya Dryomov)
* mds/FSMap: do not assert allow_standby_replay on old FSMaps (`pr#43614 <https://github.com/ceph/ceph/pull/43614>`_, Patrick Donnelly)
* mds: Add new flag to MClientSession (`pr#43251 <https://github.com/ceph/ceph/pull/43251>`_, Kotresh HR)
* mds: do not trim stray dentries during opening the root (`pr#43815 <https://github.com/ceph/ceph/pull/43815>`_, Xiubo Li)
* mds: skip journaling blocklisted clients when in `replay` state (`pr#43841 <https://github.com/ceph/ceph/pull/43841>`_, Venky Shankar)
* mds: switch mds_lock to fair mutex to fix the slow performance issue (`pr#43148 <https://github.com/ceph/ceph/pull/43148>`_, Xiubo Li, Kefu Chai)
* MDSMonitor: assertion during upgrade to v16.2.5+ (`pr#43890 <https://github.com/ceph/ceph/pull/43890>`_, Patrick Donnelly)
* MDSMonitor: handle damaged state from standby-replay (`pr#43200 <https://github.com/ceph/ceph/pull/43200>`_, Patrick Donnelly)
* MDSMonitor: no active MDS after cluster deployment (`pr#43891 <https://github.com/ceph/ceph/pull/43891>`_, Patrick Donnelly)
* mgr/dashboard,prometheus: fix handling of server_addr (`issue#52002 <http://tracker.ceph.com/issues/52002>`_, `pr#43631 <https://github.com/ceph/ceph/pull/43631>`_, Scott Shambarger)
* mgr/dashboard: all pyfakefs must be pinned on same version (`pr#43930 <https://github.com/ceph/ceph/pull/43930>`_, Rishabh Dave)
* mgr/dashboard: BATCH incl.: NFS integration, Cluster Expansion Workflow, and Angular 11 upgrade (`pr#43682 <https://github.com/ceph/ceph/pull/43682>`_, Alfonso Martínez, Avan Thakkar, Aashish Sharma, Nizamudeen A, Pere Diaz Bou, Varsha Rao, Ramana Raja, Sage Weil, Kefu Chai)
* mgr/dashboard: cephfs MDS Workload to use rate for counter type metric (`pr#43190 <https://github.com/ceph/ceph/pull/43190>`_, Jan Horacek)
* mgr/dashboard: clean-up controllers and API backward versioning compatibility (`pr#43543 <https://github.com/ceph/ceph/pull/43543>`_, Ernesto Puerta, Avan Thakkar)
* mgr/dashboard: Daemon Events listing using bootstrap class (`pr#44057 <https://github.com/ceph/ceph/pull/44057>`_, Nizamudeen A)
* mgr/dashboard: deprecated variable usage in Grafana dashboards (`pr#43188 <https://github.com/ceph/ceph/pull/43188>`_, Patrick Seidensal)
* mgr/dashboard: Device health status is not getting listed under hosts section (`pr#44053 <https://github.com/ceph/ceph/pull/44053>`_, Aashish Sharma)
* mgr/dashboard: Edit a service feature (`pr#43939 <https://github.com/ceph/ceph/pull/43939>`_, Nizamudeen A)
* mgr/dashboard: Fix failing config dashboard e2e check (`pr#43238 <https://github.com/ceph/ceph/pull/43238>`_, Nizamudeen A)
* mgr/dashboard: fix flaky inventory e2e test (`pr#44056 <https://github.com/ceph/ceph/pull/44056>`_, Nizamudeen A)
* mgr/dashboard: fix missing alert rule details (`pr#43812 <https://github.com/ceph/ceph/pull/43812>`_, Ernesto Puerta)
* mgr/dashboard: Fix orchestrator/01-hosts.e2e-spec.ts failure (`pr#43541 <https://github.com/ceph/ceph/pull/43541>`_, Nizamudeen A)
* mgr/dashboard: include mfa_ids in rgw user-details section (`pr#43893 <https://github.com/ceph/ceph/pull/43893>`_, Avan Thakkar)
* mgr/dashboard: Incorrect MTU mismatch warning (`pr#43185 <https://github.com/ceph/ceph/pull/43185>`_, Aashish Sharma)
* mgr/dashboard: monitoring: grafonnet refactoring for radosgw dashboards (`pr#43644 <https://github.com/ceph/ceph/pull/43644>`_, Aashish Sharma)
* mgr/dashboard: Move force maintenance test to the workflow test suite (`pr#43347 <https://github.com/ceph/ceph/pull/43347>`_, Nizamudeen A)
* mgr/dashboard: pin a version for autopep8 and pyfakefs (`pr#43646 <https://github.com/ceph/ceph/pull/43646>`_, Nizamudeen A)
* mgr/dashboard: Predefine labels in create host form (`pr#44077 <https://github.com/ceph/ceph/pull/44077>`_, Nizamudeen A)
* mgr/dashboard: provisioned values is misleading in RBD image table (`pr#44051 <https://github.com/ceph/ceph/pull/44051>`_, Avan Thakkar)
* mgr/dashboard: replace "Ceph-cluster" Client connections with active-standby MGRs (`pr#43523 <https://github.com/ceph/ceph/pull/43523>`_, Avan Thakkar)
* mgr/dashboard: rgw daemon list: add realm column (`pr#44047 <https://github.com/ceph/ceph/pull/44047>`_, Alfonso Martínez)
* mgr/dashboard: Spelling mistake in host-form Network address field (`pr#43973 <https://github.com/ceph/ceph/pull/43973>`_, Avan Thakkar)
* mgr/dashboard: Visual regression tests for ceph dashboard (`pr#42678 <https://github.com/ceph/ceph/pull/42678>`_, Aaryan Porwal)
* mgr/dashboard: visual tests: Add more ignore regions for dashboard component (`pr#43240 <https://github.com/ceph/ceph/pull/43240>`_, Aaryan Porwal)
* mgr/influx: use "N/A" for unknown hostname (`pr#43368 <https://github.com/ceph/ceph/pull/43368>`_, Kefu Chai)
* mgr/mirroring: remove unnecessary fs_name arg from daemon status command (`issue#51989 <http://tracker.ceph.com/issues/51989>`_, `pr#43199 <https://github.com/ceph/ceph/pull/43199>`_, Venky Shankar)
* mgr/nfs: nfs-rgw batch backport (`pr#43075 <https://github.com/ceph/ceph/pull/43075>`_, Sebastian Wagner, Sage Weil, Varsha Rao, Ramana Raja)
* mgr/progress: optimize global recovery && introduce 5 seconds interval (`pr#43353 <https://github.com/ceph/ceph/pull/43353>`_, Kamoltat, Neha Ojha)
* mgr/prometheus: offer ability to disable cache (`pr#43931 <https://github.com/ceph/ceph/pull/43931>`_, Patrick Seidensal)
* mgr/volumes: Fix permission during subvol creation with mode (`pr#43223 <https://github.com/ceph/ceph/pull/43223>`_, Kotresh HR)
* mgr: Add check to prevent mgr from crashing (`pr#43445 <https://github.com/ceph/ceph/pull/43445>`_, Aswin Toni)
* mon,auth: fix proposal (and mon db rebuild) of rotating secrets (`pr#43697 <https://github.com/ceph/ceph/pull/43697>`_, Sage Weil)
* mon/MDSMonitor: avoid crash when decoding old FSMap epochs (`pr#43615 <https://github.com/ceph/ceph/pull/43615>`_, Patrick Donnelly)
* mon: Allow specifying new tiebreaker monitors (`pr#43457 <https://github.com/ceph/ceph/pull/43457>`_, Greg Farnum)
* mon: MonMap: display disallowed_leaders whenever they're set (`pr#43972 <https://github.com/ceph/ceph/pull/43972>`_, Greg Farnum)
* mon: MonMap: do not increase mon_info_t's compatv in stretch mode, really (`pr#43971 <https://github.com/ceph/ceph/pull/43971>`_, Greg Farnum)
* monitoring: ethernet bonding filter in Network Load (`pr#43694 <https://github.com/ceph/ceph/pull/43694>`_, Pere Diaz Bou)
* msg/async/ProtocolV2: Set the recv_stamp at the beginning of receiving a message (`pr#43511 <https://github.com/ceph/ceph/pull/43511>`_, dongdong tao)
* msgr/async: fix unsafe access in unregister_conn() (`pr#43548 <https://github.com/ceph/ceph/pull/43548>`_, Sage Weil, Radoslaw Zarzynski)
* os/bluestore: _do_write_small fix head_pad (`pr#43756 <https://github.com/ceph/ceph/pull/43756>`_, dheart)
* os/bluestore: do not select absent device in volume selector (`pr#43970 <https://github.com/ceph/ceph/pull/43970>`_, Igor Fedotov)
* os/bluestore: fix invalid omap name conversion when upgrading to per-pg (`pr#43793 <https://github.com/ceph/ceph/pull/43793>`_, Igor Fedotov)
* os/bluestore: list obj which equals to pend (`pr#43512 <https://github.com/ceph/ceph/pull/43512>`_, Mykola Golub, Kefu Chai)
* os/bluestore: multiple repair fixes (`pr#43731 <https://github.com/ceph/ceph/pull/43731>`_, Igor Fedotov)
* osd/OSD: mkfs need wait for transcation completely finish (`pr#43417 <https://github.com/ceph/ceph/pull/43417>`_, Chen Fan)
* osd: fix partial recovery become whole object recovery after restart osd (`pr#43513 <https://github.com/ceph/ceph/pull/43513>`_, Jianwei Zhang)
* osd: fix to allow inc manifest leaked (`pr#43306 <https://github.com/ceph/ceph/pull/43306>`_, Myoungwon Oh)
* osd: fix to recover adjacent clone when set_chunk is called (`pr#43099 <https://github.com/ceph/ceph/pull/43099>`_, Myoungwon Oh)
* osd: handle inconsistent hash info during backfill and deep scrub gracefully (`pr#43544 <https://github.com/ceph/ceph/pull/43544>`_, Ronen Friedman, Mykola Golub)
* osd: re-cache peer_bytes on every peering state activate (`pr#43437 <https://github.com/ceph/ceph/pull/43437>`_, Mykola Golub)
* osd: Run osd bench test to override default max osd capacity for mclock (`pr#41731 <https://github.com/ceph/ceph/pull/41731>`_, Sridhar Seshasayee)
* Pacific: BlueStore: Omap upgrade to per-pg fix fix (`pr#43922 <https://github.com/ceph/ceph/pull/43922>`_, Adam Kupczyk)
* Pacific: client: do not defer releasing caps when revoking (`pr#43782 <https://github.com/ceph/ceph/pull/43782>`_, Xiubo Li)
* Pacific: mds: add read/write io size metrics support (`pr#43784 <https://github.com/ceph/ceph/pull/43784>`_, Xiubo Li)
* Pacific: test/libcephfs: put inodes after lookup (`pr#43562 <https://github.com/ceph/ceph/pull/43562>`_, Patrick Donnelly)
* pybind/mgr/cephadm: set allow_standby_replay during CephFS upgrade (`pr#43559 <https://github.com/ceph/ceph/pull/43559>`_, Patrick Donnelly)
* pybind/mgr/CMakeLists.txt: exclude files not used at runtime (`pr#43787 <https://github.com/ceph/ceph/pull/43787>`_, Duncan Bellamy)
* pybind/mgr/pg_autoscale: revert to default profile scale-up (`pr#44032 <https://github.com/ceph/ceph/pull/44032>`_, Kamoltat)
* qa/mgr/dashboard/test_pool: don't check HEALTH_OK (`pr#43440 <https://github.com/ceph/ceph/pull/43440>`_, Ernesto Puerta)
* qa/mgr/dashboard: add extra wait to test (`pr#43351 <https://github.com/ceph/ceph/pull/43351>`_, Ernesto Puerta)
* qa/rgw: pacific branch targets ceph-pacific branch of java_s3tests (`pr#43809 <https://github.com/ceph/ceph/pull/43809>`_, Casey Bodley)
* qa/tasks/kubeadm: force docker cgroup engine to systemd (`pr#43937 <https://github.com/ceph/ceph/pull/43937>`_, Sage Weil)
* qa/tasks/mgr: skip test_diskprediction_local on python>=3.8 (`pr#43421 <https://github.com/ceph/ceph/pull/43421>`_, Kefu Chai)
* qa/tests: advanced version to reflect the latest 16.2.6 release (`pr#43242 <https://github.com/ceph/ceph/pull/43242>`_, Yuri Weinstein)
* qa: disable metrics on kernel client during upgrade (`pr#44034 <https://github.com/ceph/ceph/pull/44034>`_, Patrick Donnelly)
* qa: lengthen grace for fs map showing dead MDS (`pr#43702 <https://github.com/ceph/ceph/pull/43702>`_, Patrick Donnelly)
* qa: reduce frag split confs for dir_split counter test (`pr#43828 <https://github.com/ceph/ceph/pull/43828>`_, Patrick Donnelly)
* rbd-mirror: fix mirror image removal (`pr#43662 <https://github.com/ceph/ceph/pull/43662>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: unbreak one-way snapshot-based mirroring (`pr#43315 <https://github.com/ceph/ceph/pull/43315>`_, Ilya Dryomov)
* rgw/notification: make notifications agnostic of bucket reshard (`pr#42946 <https://github.com/ceph/ceph/pull/42946>`_, Yuval Lifshitz)
* rgw/notifications: cache object size to avoid accessing invalid memory (`pr#42949 <https://github.com/ceph/ceph/pull/42949>`_, Yuval Lifshitz)
* rgw/notifications: send correct size in case of delete marker creation (`pr#42643 <https://github.com/ceph/ceph/pull/42643>`_, Yuval Lifshitz)
* rgw/notifications: support v4 auth for topics and notifications (`pr#42947 <https://github.com/ceph/ceph/pull/42947>`_, Yuval Lifshitz)
* rgw/rgw_rados: make RGW request IDs non-deterministic (`pr#43695 <https://github.com/ceph/ceph/pull/43695>`_, Cory Snyder)
* rgw/sts: fix for copy object operation using sts (`pr#43703 <https://github.com/ceph/ceph/pull/43703>`_, Pritha Srivastava)
* rgw/tracing: unify SO version numbers within librgw2 package (`pr#43619 <https://github.com/ceph/ceph/pull/43619>`_, Nathan Cutler)
* rgw: add abstraction for ops log destination and add file logger (`pr#43740 <https://github.com/ceph/ceph/pull/43740>`_, Casey Bodley, Cory Snyder)
* rgw: Ensure buckets too old to decode a layout have layout logs (`pr#43823 <https://github.com/ceph/ceph/pull/43823>`_, Adam C. Emerson)
* rgw: fix bucket purge incomplete multipart uploads (`pr#43862 <https://github.com/ceph/ceph/pull/43862>`_, J. Eric Ivancich)
* rgw: fix spelling of eTag in S3 message structure (`pr#42945 <https://github.com/ceph/ceph/pull/42945>`_, Tom Schoonjans)
* rgw: fix sts memory leak (`pr#43348 <https://github.com/ceph/ceph/pull/43348>`_, yuliyang_yewu)
* rgw: remove prefix & delim params for bucket removal & mp upload abort (`pr#43975 <https://github.com/ceph/ceph/pull/43975>`_, J. Eric Ivancich)
* rgw: use existing s->bucket in s3 website retarget() (`pr#43777 <https://github.com/ceph/ceph/pull/43777>`_, Casey Bodley)
* snap-schedule: count retained snapshots per retention policy (`pr#43434 <https://github.com/ceph/ceph/pull/43434>`_, Jan Fajerski)
* test: shutdown the mounter after test finishes (`pr#43475 <https://github.com/ceph/ceph/pull/43475>`_, Xiubo Li)

v16.2.6 Pacific
===============

.. DANGER:: DATE: 01 NOV 2021.

   DO NOT UPGRADE TO CEPH PACIFIC FROM AN OLDER VERSION.

   A recently-discovered bug (https://tracker.ceph.com/issues/53062) can cause
   data corruption. This bug occurs during OMAP format conversion for
   clusters that are updated to Pacific. New clusters are not affected by this
   bug.

   The trigger for this bug is BlueStore's repair/quick-fix functionality. This
   bug can be triggered in two known ways:

    (1) manually via the ceph-bluestore-tool, or
    (2) automatically, by OSD if ``bluestore_fsck_quick_fix_on_mount`` is set
        to true.

   The fix for this bug is expected to be available in Ceph v16.2.7.

   DO NOT set ``bluestore_quick_fix_on_mount`` to true. If it is currently
   set to true in your configuration, immediately set it to false.

   DO NOT run ``ceph-bluestore-tool``'s repair/quick-fix commands.


This is the sixth backport release in the Pacific series.


Notable Changes
---------------

* MGR: The pg_autoscaler has a new default 'scale-down' profile which provides more
  performance from the start for new pools (for newly created clusters).
  Existing clusters will retain the old behavior, now called the 'scale-up' profile.
  For more details, see:
  https://docs.ceph.com/en/latest/rados/operations/placement-groups/

* CephFS: the upgrade procedure for CephFS is now simpler. It is no longer
  necessary to stop all MDS before upgrading the sole active MDS. After
  disabling standby-replay, reducing max_mds to 1, and waiting for the file
  systems to become stable (each fs with 1 active and 0 stopping daemons), a
  rolling upgrade of all MDS daemons can be performed.

* Dashboard: now allows users to set up and display a custom message (MOTD, warning,
  etc.) in a sticky banner at the top of the page. For more details, see:
  https://docs.ceph.com/en/pacific/mgr/dashboard/#message-of-the-day-motd

* Several fixes in BlueStore, including a fix for the deferred write regression,
  which led to excessive RocksDB flushes and compactions. Previously, when
  `bluestore_prefer_deferred_size_hdd` was equal to or more than
  `bluestore_max_blob_size_hdd` (both set to 64K), all the data was deferred,
  which led to increased consumption of the column family used to store
  deferred writes in RocksDB. Now, the `bluestore_prefer_deferred_size` parameter
  independently controls deferred writes, and only writes smaller than
  this size use the deferred write path.

* The default value of `osd_client_message_cap` has been set to 256, to provide
  better flow control by limiting maximum number of in-flight client requests.

* PGs no longer show a `active+clean+scrubbing+deep+repair` state when
  `osd_scrub_auto_repair` is set to true, for regular deep-scrubs with no repair
  required.

* `ceph-mgr-modules-core` debian package does not recommend `ceph-mgr-rook`
  anymore. As the latter depends on `python3-numpy` which cannot be imported in
  different Python sub-interpreters multi-times if the version of
  `python3-numpy` is older than 1.19. Since `apt-get` installs the `Recommends`
  packages by default, `ceph-mgr-rook` was always installed along with
  `ceph-mgr` debian package as an indirect dependency. If your workflow depends
  on this behavior, you might want to install `ceph-mgr-rook` separately.

 * This is the first release built for Debian Bullseye.

Changelog
---------

* bind on loopback address if no other addresses are available (`pr#42477 <https://github.com/ceph/ceph/pull/42477>`_, Kefu Chai)
* ceph-monstore-tool: use a large enough paxos/{first,last}_committed (`issue#38219 <http://tracker.ceph.com/issues/38219>`_, `pr#42411 <https://github.com/ceph/ceph/pull/42411>`_, Kefu Chai)
* ceph-volume/tests: retry when destroying osd (`pr#42546 <https://github.com/ceph/ceph/pull/42546>`_, Guillaume Abrioux)
* ceph-volume/tests: update ansible environment variables in tox (`pr#42490 <https://github.com/ceph/ceph/pull/42490>`_, Dimitri Savineau)
* ceph-volume: Consider /dev/root as mounted (`pr#42755 <https://github.com/ceph/ceph/pull/42755>`_, David Caro)
* ceph-volume: fix lvm activate arguments (`pr#43116 <https://github.com/ceph/ceph/pull/43116>`_, Dimitri Savineau)
* ceph-volume: fix lvm migrate without args (`pr#43110 <https://github.com/ceph/ceph/pull/43110>`_, Dimitri Savineau)
* ceph-volume: fix raw list with logical partition (`pr#43087 <https://github.com/ceph/ceph/pull/43087>`_, Guillaume Abrioux, Dimitri Savineau)
* ceph-volume: implement bluefs volume migration (`pr#42219 <https://github.com/ceph/ceph/pull/42219>`_, Kefu Chai, Igor Fedotov)
* ceph-volume: lvm batch: fast_allocations(): avoid ZeroDivisionError (`pr#42493 <https://github.com/ceph/ceph/pull/42493>`_, Jonas Zeiger)
* ceph-volume: pvs --noheadings replace pvs --no-heading (`pr#43076 <https://github.com/ceph/ceph/pull/43076>`_, FengJiankui)
* ceph-volume: remove --all ref from deactivate help (`pr#43098 <https://github.com/ceph/ceph/pull/43098>`_, Dimitri Savineau)
* ceph-volume: support no_systemd with lvm migrate (`pr#43091 <https://github.com/ceph/ceph/pull/43091>`_, Dimitri Savineau)
* ceph-volume: work around phantom atari partitions (`pr#42753 <https://github.com/ceph/ceph/pull/42753>`_, Blaine Gardner)
* ceph.spec.in: drop gdbm from build deps (`pr#43000 <https://github.com/ceph/ceph/pull/43000>`_, Kefu Chai)
* cephadm: August batch 1 (`pr#42736 <https://github.com/ceph/ceph/pull/42736>`_, Sage Weil, Dimitri Savineau, Guillaume Abrioux, Sebastian Wagner, Varsha Rao, Zac Dover, Adam King, Cory Snyder, Michael Fritch, Asbjørn Sannes, "Wang,Fei", Javier Cacheiro, 胡玮文, Daniel Pivonka)
* cephadm: September batch 1 (`issue#52038 <http://tracker.ceph.com/issues/52038>`_, `pr#43029 <https://github.com/ceph/ceph/pull/43029>`_, Sebastian Wagner, Dimitri Savineau, Paul Cuzner, Oleander Reis, Adam King, Yuxiang Zhu, Zac Dover, Alfonso Martínez, Sage Weil, Daniel Pivonka)
* cephadm: use quay, not docker (`pr#42534 <https://github.com/ceph/ceph/pull/42534>`_, Sage Weil)
* cephfs-mirror: record directory path cancel in DirRegistry (`issue#51666 <http://tracker.ceph.com/issues/51666>`_, `pr#42458 <https://github.com/ceph/ceph/pull/42458>`_, Venky Shankar)
* client: flush the mdlog in unsafe requests' relevant and auth MDSes only (`pr#42925 <https://github.com/ceph/ceph/pull/42925>`_, Xiubo Li)
* client: make sure only to update dir dist from auth mds (`pr#42937 <https://github.com/ceph/ceph/pull/42937>`_, Xue Yantao)
* cls/cmpomap: empty values are 0 in U64 comparisons (`pr#42908 <https://github.com/ceph/ceph/pull/42908>`_, Casey Bodley)
* cmake, ceph.spec.in: build with header only fmt on RHEL (`pr#42472 <https://github.com/ceph/ceph/pull/42472>`_, Kefu Chai)
* cmake: build static libs if they are internal ones (`pr#39902 <https://github.com/ceph/ceph/pull/39902>`_, Kefu Chai)
* cmake: exclude "grafonnet-lib" target from "all" (`pr#42898 <https://github.com/ceph/ceph/pull/42898>`_, Kefu Chai)
* cmake: link bundled fmt statically (`pr#42692 <https://github.com/ceph/ceph/pull/42692>`_, Kefu Chai)
* cmake: Replace boost download url (`pr#42693 <https://github.com/ceph/ceph/pull/42693>`_, Rafał Wądołowski)
* common/buffer: fix SIGABRT in  rebuild_aligned_size_and_memory (`pr#42976 <https://github.com/ceph/ceph/pull/42976>`_, Yin Congmin)
* common/Formatter: include used header (`pr#42233 <https://github.com/ceph/ceph/pull/42233>`_, Kefu Chai)
* common/options: Set osd_client_message_cap to 256 (`pr#42615 <https://github.com/ceph/ceph/pull/42615>`_, Mark Nelson)
* compression/snappy: use uint32_t to be compatible with 1.1.9 (`pr#42542 <https://github.com/ceph/ceph/pull/42542>`_, Kefu Chai, Nathan Cutler)
* debian/control: ceph-mgr-modules-core does not Recommend ceph-mgr-roo… (`pr#42300 <https://github.com/ceph/ceph/pull/42300>`_, Kefu Chai)
* debian/control: dh-systemd is part of debhelper now (`pr#43151 <https://github.com/ceph/ceph/pull/43151>`_, David Galloway)
* debian/control: remove cython from Build-Depends (`pr#43131 <https://github.com/ceph/ceph/pull/43131>`_, Kefu Chai)
* doc/ceph-volume: add lvm migrate/new-db/new-wal (`pr#43089 <https://github.com/ceph/ceph/pull/43089>`_, Dimitri Savineau)
* doc/rados/operations: s/max_misplaced/target_max_misplaced_ratio/ (`pr#42250 <https://github.com/ceph/ceph/pull/42250>`_, Paul Reece, Kefu Chai)
* doc/releases/pacific.rst: remove notes about autoscaler (`pr#42265 <https://github.com/ceph/ceph/pull/42265>`_, Neha Ojha)
* Don't persist report data (`pr#42888 <https://github.com/ceph/ceph/pull/42888>`_, Brad Hubbard)
* krbd: escape udev_enumerate_add_match_sysattr values (`pr#42969 <https://github.com/ceph/ceph/pull/42969>`_, Ilya Dryomov)
* kv/RocksDBStore: Add handling of block_cache option for resharding (`pr#42844 <https://github.com/ceph/ceph/pull/42844>`_, Adam Kupczyk)
* kv/RocksDBStore: enrich debug message (`pr#42544 <https://github.com/ceph/ceph/pull/42544>`_, Toshikuni Fukaya, Satoru Takeuchi)
* librgw/notifications: initialize kafka and amqp (`pr#42648 <https://github.com/ceph/ceph/pull/42648>`_, Yuval Lifshitz)
* mds: add debugging when rejecting mksnap with EPERM (`pr#42935 <https://github.com/ceph/ceph/pull/42935>`_, Patrick Donnelly)
* mds: create file system with specific ID (`pr#42900 <https://github.com/ceph/ceph/pull/42900>`_, Ramana Raja)
* mds: MDCache.cc:5319 FAILED ceph_assert(rejoin_ack_gather.count(mds->get_nodeid())) (`pr#42938 <https://github.com/ceph/ceph/pull/42938>`_, chencan)
* mds: META_POP_READDIR, META_POP_FETCH, META_POP_STORE, and cache_hit_rate are not updated (`pr#42939 <https://github.com/ceph/ceph/pull/42939>`_, Yongseok Oh)
* mds: to print the unknow type value (`pr#42088 <https://github.com/ceph/ceph/pull/42088>`_, Xiubo Li, Jos Collin)
* MDSMonitor: monitor crash after upgrade from ceph 15.2.13 to 16.2.4 (`pr#42536 <https://github.com/ceph/ceph/pull/42536>`_, Patrick Donnelly)
* mgr/DaemonServer: skip redundant update of pgp_num_actual (`pr#42223 <https://github.com/ceph/ceph/pull/42223>`_, Dan van der Ster)
* mgr/dashboard/api: set a UTF-8 locale when running pip (`pr#42829 <https://github.com/ceph/ceph/pull/42829>`_, Kefu Chai)
* mgr/dashboard: Add configurable MOTD or wall notification (`pr#42414 <https://github.com/ceph/ceph/pull/42414>`_, Volker Theile)
* mgr/dashboard: cephadm e2e start script: add --expanded option (`pr#42789 <https://github.com/ceph/ceph/pull/42789>`_, Alfonso Martínez)
* mgr/dashboard: cephadm-e2e job script: improvements (`pr#42585 <https://github.com/ceph/ceph/pull/42585>`_, Alfonso Martínez)
* mgr/dashboard: disable create snapshot with subvolumes (`pr#42819 <https://github.com/ceph/ceph/pull/42819>`_, Pere Diaz Bou)
* mgr/dashboard: don't notify for suppressed alerts (`pr#42974 <https://github.com/ceph/ceph/pull/42974>`_, Tatjana Dehler)
* mgr/dashboard: fix Accept-Language header parsing (`pr#42297 <https://github.com/ceph/ceph/pull/42297>`_, 胡玮文)
* mgr/dashboard: fix rename inventory to disks (`pr#42810 <https://github.com/ceph/ceph/pull/42810>`_, Navin Barnwal)
* mgr/dashboard: fix ssl cert validation for rgw service creation (`pr#42628 <https://github.com/ceph/ceph/pull/42628>`_, Avan Thakkar)
* mgr/dashboard: Fix test_error force maintenance dashboard check (`pr#42354 <https://github.com/ceph/ceph/pull/42354>`_, Nizamudeen A)
* mgr/dashboard: monitoring: replace Grafana JSON with Grafonnet based code (`pr#42812 <https://github.com/ceph/ceph/pull/42812>`_, Aashish Sharma)
* mgr/dashboard: Refresh button on the iscsi targets page (`pr#42817 <https://github.com/ceph/ceph/pull/42817>`_, Nizamudeen A)
* mgr/dashboard: remove usage of 'rgw_frontend_ssl_key' (`pr#42316 <https://github.com/ceph/ceph/pull/42316>`_, Avan Thakkar)
* mgr/dashboard: show perf. counters for rgw svc. on Cluster > Hosts (`pr#42629 <https://github.com/ceph/ceph/pull/42629>`_, Alfonso Martínez)
* mgr/dashboard: stats=false not working when listing buckets (`pr#42889 <https://github.com/ceph/ceph/pull/42889>`_, Avan Thakkar)
* mgr/dashboard: tox.ini: delete useless env. 'apidocs' (`pr#42788 <https://github.com/ceph/ceph/pull/42788>`_, Alfonso Martínez)
* mgr/dashboard: update translations for pacific (`pr#42606 <https://github.com/ceph/ceph/pull/42606>`_, Tatjana Dehler)
* mgr/mgr_util: switch using unshared cephfs connections whenever possible (`issue#51256 <http://tracker.ceph.com/issues/51256>`_, `pr#42083 <https://github.com/ceph/ceph/pull/42083>`_, Venky Shankar)
* mgr/pg_autoscaler: Introduce autoscaler scale-down feature (`pr#42428 <https://github.com/ceph/ceph/pull/42428>`_, Kamoltat, Kefu Chai)
* mgr/rook: Add timezone info (`pr#39834 <https://github.com/ceph/ceph/pull/39834>`_, Varsha Rao, Sebastian Wagner)
* mgr/telemetry: pass leaderboard flag even w/o ident (`pr#42228 <https://github.com/ceph/ceph/pull/42228>`_, Sage Weil)
* mgr/volumes: Add config to insert delay at the beginning of the clone (`pr#42086 <https://github.com/ceph/ceph/pull/42086>`_, Kotresh HR)
* mgr/volumes: use dedicated libcephfs handles for subvolume calls and … (`issue#51271 <http://tracker.ceph.com/issues/51271>`_, `pr#42914 <https://github.com/ceph/ceph/pull/42914>`_, Venky Shankar)
* mgr: set debug_mgr=2/5 (so INFO goes to mgr log by default) (`pr#42225 <https://github.com/ceph/ceph/pull/42225>`_, Sage Weil)
* mon/MDSMonitor: do not pointlessly kill standbys that are incompatible with current CompatSet (`pr#42578 <https://github.com/ceph/ceph/pull/42578>`_, Patrick Donnelly, Zhi Zhang)
* mon/OSDMonitor: resize oversized Lec::epoch_by_pg, after PG merging, preventing osdmap trimming (`pr#42224 <https://github.com/ceph/ceph/pull/42224>`_, Dan van der Ster)
* mon/PGMap: remove DIRTY field in ceph df detail when cache tiering is not in use (`pr#42860 <https://github.com/ceph/ceph/pull/42860>`_, Deepika Upadhyay)
* mon: return -EINVAL when handling unknown option in 'ceph osd pool get' (`pr#42229 <https://github.com/ceph/ceph/pull/42229>`_, Zhao Cuicui)
* mon: Sanely set the default CRUSH rule when creating pools in stretch… (`pr#42909 <https://github.com/ceph/ceph/pull/42909>`_, Greg Farnum)
* monitoring/grafana/build/Makefile: revamp for arm64 builds, pushes to docker and quay, jenkins (`pr#42211 <https://github.com/ceph/ceph/pull/42211>`_, Dan Mick)
* monitoring/grafana/cluster: use per-unit max and limit values (`pr#42679 <https://github.com/ceph/ceph/pull/42679>`_, David Caro)
* monitoring: Clean up Grafana dashboards (`pr#42299 <https://github.com/ceph/ceph/pull/42299>`_, Patrick Seidensal)
* monitoring: fix Physical Device Latency unit (`pr#42298 <https://github.com/ceph/ceph/pull/42298>`_, Seena Fallah)
* msg: active_connections regression (`pr#42936 <https://github.com/ceph/ceph/pull/42936>`_, Sage Weil)
* nfs backport June (`pr#42096 <https://github.com/ceph/ceph/pull/42096>`_, Varsha Rao)
* os/bluestore: accept undecodable multi-block bluefs transactions on log (`pr#43023 <https://github.com/ceph/ceph/pull/43023>`_, Igor Fedotov)
* os/bluestore: cap omap naming scheme upgrade transaction (`pr#42956 <https://github.com/ceph/ceph/pull/42956>`_, Igor Fedotov)
* os/bluestore: compact db after bulk omap naming upgrade (`pr#42426 <https://github.com/ceph/ceph/pull/42426>`_, Igor Fedotov)
* os/bluestore: fix bluefs migrate command (`pr#43100 <https://github.com/ceph/ceph/pull/43100>`_, Igor Fedotov)
* os/bluestore: fix erroneous SharedBlob record removal during repair (`pr#42423 <https://github.com/ceph/ceph/pull/42423>`_, Igor Fedotov)
* os/bluestore: fix using incomplete bluefs log when dumping it (`pr#43007 <https://github.com/ceph/ceph/pull/43007>`_, Igor Fedotov)
* os/bluestore: make deferred writes less aggressive for large writes (`pr#42773 <https://github.com/ceph/ceph/pull/42773>`_, Igor Fedotov, Adam Kupczyk)
* os/bluestore: Remove possibility of replay log and file inconsistency (`pr#42424 <https://github.com/ceph/ceph/pull/42424>`_, Adam Kupczyk)
* os/bluestore: respect bluestore_warn_on_spurious_read_errors setting (`pr#42897 <https://github.com/ceph/ceph/pull/42897>`_, Igor Fedotov)
* osd/scrub: separate between PG state flags and internal scrubber operation (`pr#42398 <https://github.com/ceph/ceph/pull/42398>`_, Ronen Friedman)
* osd: log snaptrim message to dout (`pr#42482 <https://github.com/ceph/ceph/pull/42482>`_, Arthur Outhenin-Chalandre)
* osd: move down peers out from peer_purged (`pr#42238 <https://github.com/ceph/ceph/pull/42238>`_, Mykola Golub)
* pybind/mgr/stats: validate cmdtag (`pr#42702 <https://github.com/ceph/ceph/pull/42702>`_, Jos Collin)
* pybind/mgr: Fix IPv6 url generation (`pr#42990 <https://github.com/ceph/ceph/pull/42990>`_, Sebastian Wagner)
* pybind/rbd: fix mirror_image_get_status (`pr#42972 <https://github.com/ceph/ceph/pull/42972>`_, Ilya Dryomov, Will Smith)
* qa/\*/test_envlibrados_for_rocksdb.sh: install libarchive-3.3.3 (`pr#42344 <https://github.com/ceph/ceph/pull/42344>`_, Neha Ojha)
* qa/cephadm: centos_8.x_container_tools_3.0.yaml (`pr#42868 <https://github.com/ceph/ceph/pull/42868>`_, Sebastian Wagner)
* qa/rgw: move ignore-pg-availability.yaml out of suites/rgw (`pr#40694 <https://github.com/ceph/ceph/pull/40694>`_, Casey Bodley)
* qa/standalone: Add missing cleanups after completion of a subset of osd and scrub tests (`pr#42258 <https://github.com/ceph/ceph/pull/42258>`_, Sridhar Seshasayee)
* qa/tests: advanced pacific version to reflect the latest 16.2.5 point (`pr#42264 <https://github.com/ceph/ceph/pull/42264>`_, Yuri Weinstein)
* qa/workunits/mon/test_mon_config_key: use subprocess.run() instead of proc.communicate() (`pr#42221 <https://github.com/ceph/ceph/pull/42221>`_, Kefu Chai)
* qa: FileNotFoundError: [Errno 2] No such file or directory: '/sys/kernel/debug/ceph/3fab6bea-f243-47a4-a956-8c03a62b61b5.client4721/mds_sessions' (`pr#42165 <https://github.com/ceph/ceph/pull/42165>`_, Patrick Donnelly)
* qa: increase the pg_num for cephfs_data/metadata pools (`pr#42923 <https://github.com/ceph/ceph/pull/42923>`_, Xiubo Li)
* qa: test_ls_H_prints_human_readable_file_size failure (`pr#42166 <https://github.com/ceph/ceph/pull/42166>`_, Patrick Donnelly)
* radosgw-admin: skip GC init on read-only admin ops (`pr#42655 <https://github.com/ceph/ceph/pull/42655>`_, Mark Kogan)
* radosgw: include realm\_{id,name} in service map (`pr#42213 <https://github.com/ceph/ceph/pull/42213>`_, Sage Weil)
* rbd-mirror: add perf counters to snapshot replayer (`pr#42987 <https://github.com/ceph/ceph/pull/42987>`_, Arthur Outhenin-Chalandre)
* rbd-mirror: fix potential async op tracker leak in start_image_replayers (`pr#42979 <https://github.com/ceph/ceph/pull/42979>`_, Mykola Golub)
* rbd: fix default pool handling for nbd map/unmap (`pr#42980 <https://github.com/ceph/ceph/pull/42980>`_, Sunny Kumar)
* Remove dependency on lsb_release (`pr#43001 <https://github.com/ceph/ceph/pull/43001>`_, Ken Dreyer)
* RGW - Bucket Remove Op: Pass in user (`pr#42135 <https://github.com/ceph/ceph/pull/42135>`_, Daniel Gryniewicz)
* RGW - Don't move attrs before setting them (`pr#42320 <https://github.com/ceph/ceph/pull/42320>`_, Daniel Gryniewicz)
* rgw : add check empty for sync url (`pr#42653 <https://github.com/ceph/ceph/pull/42653>`_, caolei)
* rgw : add check for tenant provided in RGWCreateRole (`pr#42637 <https://github.com/ceph/ceph/pull/42637>`_, caolei)
* rgw : modfiy error XML for deleterole (`pr#42639 <https://github.com/ceph/ceph/pull/42639>`_, caolei)
* rgw multisite: metadata sync treats all errors as 'transient' for retry (`pr#42656 <https://github.com/ceph/ceph/pull/42656>`_, Casey Bodley)
* RGW Zipper - Make sure bucket list progresses (`pr#42625 <https://github.com/ceph/ceph/pull/42625>`_, Daniel Gryniewicz)
* rgw/amqp/test: fix mock prototype for librabbitmq-0.11.0 (`pr#42649 <https://github.com/ceph/ceph/pull/42649>`_, Yuval Lifshitz)
* rgw/http/notifications: support content type in HTTP POST messages (`pr#42644 <https://github.com/ceph/ceph/pull/42644>`_, Yuval Lifshitz)
* rgw/multisite: return correct error code when op fails (`pr#42646 <https://github.com/ceph/ceph/pull/42646>`_, Yuval Lifshitz)
* rgw/notification: add exception handling for persistent notification thread (`pr#42647 <https://github.com/ceph/ceph/pull/42647>`_, Yuval Lifshitz)
* rgw/notification: fix persistent notification hang when ack-levl=none (`pr#40696 <https://github.com/ceph/ceph/pull/40696>`_, Yuval Lifshitz)
* rgw/notification: fixing the "persistent=false" flag (`pr#40695 <https://github.com/ceph/ceph/pull/40695>`_, Yuval Lifshitz)
* rgw/notifications: delete bucket notification object when empty (`pr#42631 <https://github.com/ceph/ceph/pull/42631>`_, Yuval Lifshitz)
* rgw/notifications: support metadata filter in CompleteMultipartUpload and Copy events (`pr#42321 <https://github.com/ceph/ceph/pull/42321>`_, Yuval Lifshitz)
* rgw/notifications: support metadata filter in CompleteMultipartUploa… (`pr#42566 <https://github.com/ceph/ceph/pull/42566>`_, Yuval Lifshitz)
* rgw/rgw_file: Fix the return value of read() and readlink() (`pr#42654 <https://github.com/ceph/ceph/pull/42654>`_, Dai zhiwei, luo rixin)
* rgw/sts: correcting the evaluation of session policies (`pr#42632 <https://github.com/ceph/ceph/pull/42632>`_, Pritha Srivastava)
* rgw/sts: read_obj_policy() consults iam_user_policies on ENOENT (`pr#42650 <https://github.com/ceph/ceph/pull/42650>`_, Casey Bodley)
* rgw: allow rgw-orphan-list to process multiple data pools (`pr#42635 <https://github.com/ceph/ceph/pull/42635>`_, J. Eric Ivancich)
* rgw: allow to set ssl options and ciphers for beast frontend (`pr#42363 <https://github.com/ceph/ceph/pull/42363>`_, Mykola Golub)
* rgw: avoid infinite loop when deleting a bucket (`issue#49206 <http://tracker.ceph.com/issues/49206>`_, `pr#42230 <https://github.com/ceph/ceph/pull/42230>`_, Jeegn Chen)
* rgw: avoid occuring radosgw daemon crash when access a conditionally … (`pr#42626 <https://github.com/ceph/ceph/pull/42626>`_, xiangrui meng, yupeng chen)
* rgw: Backport of 51674 to Pacific (`pr#42346 <https://github.com/ceph/ceph/pull/42346>`_, Adam C. Emerson)
* rgw: deprecate the civetweb frontend (`pr#41367 <https://github.com/ceph/ceph/pull/41367>`_, Casey Bodley)
* rgw: Don't segfault on datalog trim (`pr#42336 <https://github.com/ceph/ceph/pull/42336>`_, Adam C. Emerson)
* rgw: during reshard lock contention, adjust logging (`pr#42641 <https://github.com/ceph/ceph/pull/42641>`_, J. Eric Ivancich)
* rgw: extending existing ssl support for vault KMS (`pr#42093 <https://github.com/ceph/ceph/pull/42093>`_, Jiffin Tony Thottan)
* rgw: fail as expected when set/delete-bucket-website attempted on a non-exis… (`pr#42642 <https://github.com/ceph/ceph/pull/42642>`_, xiangrui meng)
* rgw: fix bucket object listing when marker matches prefix (`pr#42638 <https://github.com/ceph/ceph/pull/42638>`_, J. Eric Ivancich)
* rgw: fix for mfa resync crash when supplied with only one totp_pin (`pr#42652 <https://github.com/ceph/ceph/pull/42652>`_, Pritha Srivastava)
* rgw: fix segfault related to explicit object manifest handling (`pr#42633 <https://github.com/ceph/ceph/pull/42633>`_, Mark Kogan)
* rgw: Improve error message on email id reuse (`pr#41783 <https://github.com/ceph/ceph/pull/41783>`_, Ponnuvel Palaniyappan)
* rgw: objectlock: improve client error messages (`pr#40693 <https://github.com/ceph/ceph/pull/40693>`_, Matt Benjamin)
* rgw: parse tenant name out of rgwx-bucket-instance (`pr#42231 <https://github.com/ceph/ceph/pull/42231>`_, Casey Bodley)
* rgw: radosgw-admin errors if marker not specified on data/mdlog trim (`pr#42640 <https://github.com/ceph/ceph/pull/42640>`_, Adam C. Emerson)
* rgw: remove quota soft threshold (`pr#42634 <https://github.com/ceph/ceph/pull/42634>`_, Zulai Wang)
* rgw: require bucket name in bucket chown (`pr#42323 <https://github.com/ceph/ceph/pull/42323>`_, Zulai Wang)
* rgw: when deleted obj removed in versioned bucket, extra del-marker added (`pr#42645 <https://github.com/ceph/ceph/pull/42645>`_, J. Eric Ivancich)
* rpm/luarocks: simplify conditional and support Leap 15.3 (`pr#42561 <https://github.com/ceph/ceph/pull/42561>`_, Nathan Cutler)
* rpm: drop use of $FIRST_ARG in ceph-immutable-object-cache (`pr#42480 <https://github.com/ceph/ceph/pull/42480>`_, Nathan Cutler)
* run-make-check.sh: Increase failure output log size (`pr#42850 <https://github.com/ceph/ceph/pull/42850>`_, David Galloway)
* SimpleRADOSStriper: use debug_cephsqlite (`pr#42659 <https://github.com/ceph/ceph/pull/42659>`_, Patrick Donnelly)
* src/pybind/mgr/mirroring/fs/snapshot_mirror.py: do not assume a cephf… (`pr#42226 <https://github.com/ceph/ceph/pull/42226>`_, Sébastien Han)
* test/rgw: fix use of poll() with timers in unittest_rgw_dmclock_scheduler (`pr#42651 <https://github.com/ceph/ceph/pull/42651>`_, Casey Bodley)
* Warning Cleanup and Clang Compile Fix (`pr#40692 <https://github.com/ceph/ceph/pull/40692>`_, Adam C. Emerson)
* workunits/rgw: semicolon terminates perl statements (`pr#43168 <https://github.com/ceph/ceph/pull/43168>`_, Matt Benjamin)

v16.2.5 Pacific
===============

This is the fifth backport release in the Pacific series. We recommend all
users update to this release.

Notable Changes
---------------

* `ceph-mgr-modules-core` debian package does not recommend `ceph-mgr-rook`
  anymore. As the latter depends on `python3-numpy` which cannot be imported in
  different Python sub-interpreters multi-times if the version of
  `python3-numpy` is older than 1.19. Since `apt-get` installs the `Recommends`
  packages by default, `ceph-mgr-rook` was always installed along with
  `ceph-mgr` debian package as an indirect dependency. If your workflow depends
  on this behavior, you might want to install `ceph-mgr-rook` separately.

* mgr/nfs: ``nfs`` module is moved out of volumes plugin. Prior using the
  ``ceph nfs`` commands, ``nfs`` mgr module must be enabled.

* volumes/nfs: The ``cephfs`` cluster type has been removed from the
  ``nfs cluster create`` subcommand. Clusters deployed by cephadm can
  support an NFS export of both ``rgw`` and ``cephfs`` from a single
  NFS cluster instance.

* The ``nfs cluster update`` command has been removed.  You can modify
  the placement of an existing NFS service (and/or its associated
  ingress service) using ``orch ls --export`` and ``orch apply -i
  ...``.

* The ``orch apply nfs`` command no longer requires a pool or
  namespace argument. We strongly encourage users to use the defaults
  so that the ``nfs cluster ls`` and related commands will work
  properly.

* The ``nfs cluster delete`` and ``nfs export delete`` commands are
  deprecated and will be removed in a future release.  Please use
  ``nfs cluster rm`` and ``nfs export rm`` instead.

* A long-standing bug that prevented 32-bit and 64-bit client/server
  interoperability under msgr v2 has been fixed.  In particular, mixing armv7l
  (armhf) and x86_64 or aarch64 servers in the same cluster now works.

Changelog
---------

* .github/labeler: add api-change label (`pr#41818 <https://github.com/ceph/ceph/pull/41818>`_, Ernesto Puerta)
* Improve mon location handling for stretch clusters (`pr#40484 <https://github.com/ceph/ceph/pull/40484>`_, Greg Farnum)
* MDS heartbeat timed out between during executing MDCache::start_files_to_recover() (`pr#42061 <https://github.com/ceph/ceph/pull/42061>`_, Yongseok Oh)
* MDS slow request lookupino #0x100 on rank 1 block forever on dispatched (`pr#40856 <https://github.com/ceph/ceph/pull/40856>`_, Xiubo Li, Patrick Donnelly)
* MDSMonitor: crash when attempting to mount cephfs (`pr#42068 <https://github.com/ceph/ceph/pull/42068>`_, Patrick Donnelly)
* Pacific stretch mon state [Merge after 40484] (`pr#41130 <https://github.com/ceph/ceph/pull/41130>`_, Greg Farnum)
* Pacific: Add DoutPrefixProvider for RGW Log Messages in Pacfic (`pr#40054 <https://github.com/ceph/ceph/pull/40054>`_, Ali Maredia, Kalpesh Pandya, Casey Bodley)
* Pacific: Direct MMonJoin messages to leader, not first rank [Merge after 41130] (`pr#41131 <https://github.com/ceph/ceph/pull/41131>`_, Greg Farnum)
* Revert "pacific: mgr/dashboard: Generate NPM dependencies manifest" (`pr#41549 <https://github.com/ceph/ceph/pull/41549>`_, Nizamudeen A)
* Update boost url, fixing windows build (`pr#41259 <https://github.com/ceph/ceph/pull/41259>`_, Lucian Petrut)
* bluestore: use string_view and strip trailing slash for dir listing (`pr#41755 <https://github.com/ceph/ceph/pull/41755>`_, Jonas Jelten, Kefu Chai)
* build(deps): bump node-notifier from 8.0.0 to 8.0.1 in /src/pybind/mgr/dashboard/frontend (`pr#40813 <https://github.com/ceph/ceph/pull/40813>`_, Ernesto Puerta, dependabot[bot])
* ceph-volume: fix batch report and respect ceph.conf config values (`pr#41714 <https://github.com/ceph/ceph/pull/41714>`_, Andrew Schoen)
* ceph_test_rados_api_service: more retries for servicemkap (`pr#41182 <https://github.com/ceph/ceph/pull/41182>`_, Sage Weil)
* cephadm june final batch (`pr#42117 <https://github.com/ceph/ceph/pull/42117>`_, Kefu Chai, Sage Weil, Zac Dover, Sebastian Wagner, Varsha Rao, Sandro Bonazzola, Juan Miguel Olmo Martínez)
* cephadm: batch backport for May (2) (`pr#41219 <https://github.com/ceph/ceph/pull/41219>`_, Adam King, Sage Weil, Zac Dover, Dennis Körner, jianglong01, Avan Thakkar, Juan Miguel Olmo Martínez)
* cephadm: june batch 1 (`pr#41684 <https://github.com/ceph/ceph/pull/41684>`_, Sage Weil, Paul Cuzner, Juan Miguel Olmo Martínez, VasishtaShastry, Zac Dover, Sebastian Wagner, Adam King, Michael Fritch, Daniel Pivonka, sunilkumarn417)
* cephadm: june batch 2 (`pr#41815 <https://github.com/ceph/ceph/pull/41815>`_, Sebastian Wagner, Daniel Pivonka, Zac Dover, Michael Fritch)
* cephadm: june batch 3 (`pr#41913 <https://github.com/ceph/ceph/pull/41913>`_, Zac Dover, Adam King, Michael Fritch, Patrick Donnelly, Sage Weil, Juan Miguel Olmo Martínez, jianglong01)
* cephadm: may batch 1 (`pr#41151 <https://github.com/ceph/ceph/pull/41151>`_, Juan Miguel Olmo Martínez, Sage Weil, Zac Dover, Daniel Pivonka, Adam King, Stanislav Datskevych, jianglong01, Kefu Chai, Deepika Upadhyay, Joao Eduardo Luis)
* cephadm: may batch 3 (`pr#41463 <https://github.com/ceph/ceph/pull/41463>`_, Sage Weil, Michael Fritch, Adam King, Patrick Seidensal, Juan Miguel Olmo Martínez, Dimitri Savineau, Zac Dover, Sebastian Wagner)
* cephfs-mirror backports (`issue#50523 <http://tracker.ceph.com/issues/50523>`_, `issue#50035 <http://tracker.ceph.com/issues/50035>`_, `issue#50266 <http://tracker.ceph.com/issues/50266>`_, `issue#50442 <http://tracker.ceph.com/issues/50442>`_, `issue#50581 <http://tracker.ceph.com/issues/50581>`_, `issue#50229 <http://tracker.ceph.com/issues/50229>`_, `issue#49939 <http://tracker.ceph.com/issues/49939>`_, `issue#50224 <http://tracker.ceph.com/issues/50224>`_, `issue#50298 <http://tracker.ceph.com/issues/50298>`_, `pr#41475 <https://github.com/ceph/ceph/pull/41475>`_, Venky Shankar, Lucian Petrut)
* cephfs-mirror: backports (`issue#50447 <http://tracker.ceph.com/issues/50447>`_, `issue#50867 <http://tracker.ceph.com/issues/50867>`_, `issue#51204 <http://tracker.ceph.com/issues/51204>`_, `pr#41947 <https://github.com/ceph/ceph/pull/41947>`_, Venky Shankar)
* cephfs-mirror: reopen logs on SIGHUP (`issue#51413 <http://tracker.ceph.com/issues/51413>`_, `issue#51318 <http://tracker.ceph.com/issues/51318>`_, `pr#42097 <https://github.com/ceph/ceph/pull/42097>`_, Venky Shankar)
* cephfs-top: self-adapt the display according the window size (`pr#41053 <https://github.com/ceph/ceph/pull/41053>`_, Xiubo Li)
* client: Fix executeable access check for the root user (`pr#41294 <https://github.com/ceph/ceph/pull/41294>`_, Kotresh HR)
* client: fix the opened inodes counter increasing (`pr#40685 <https://github.com/ceph/ceph/pull/40685>`_, Xiubo Li)
* client: make Inode to inherit from RefCountedObject (`pr#41052 <https://github.com/ceph/ceph/pull/41052>`_, Xiubo Li)
* cls/rgw: look for plain entries in non-ascii plain namespace too (`pr#41774 <https://github.com/ceph/ceph/pull/41774>`_, Mykola Golub)
* common/buffer: adjust align before calling posix_memalign() (`pr#41249 <https://github.com/ceph/ceph/pull/41249>`_, Ilya Dryomov)
* common/mempool: only fail tests if sharding is very bad (`pr#40566 <https://github.com/ceph/ceph/pull/40566>`_, singuliere)
* common/options/global.yaml.in: increase default value of bluestore_cache_trim_max_skip_pinned (`pr#40918 <https://github.com/ceph/ceph/pull/40918>`_, Neha Ojha)
* crush/crush: ensure alignof(crush_work_bucket) is 1 (`pr#41983 <https://github.com/ceph/ceph/pull/41983>`_, Kefu Chai)
* debian,cmake,cephsqlite: hide non-public symbols (`pr#40689 <https://github.com/ceph/ceph/pull/40689>`_, Kefu Chai)
* debian/control: ceph-mgr-modules-core does not Recommend ceph-mgr-rook (`pr#41877 <https://github.com/ceph/ceph/pull/41877>`_, Kefu Chai)
* doc: pacific updates (`pr#42066 <https://github.com/ceph/ceph/pull/42066>`_, Patrick Donnelly)
* librbd/cache/pwl: fix parsing of cache_type in create_image_cache_state() (`pr#41244 <https://github.com/ceph/ceph/pull/41244>`_, Ilya Dryomov)
* librbd/mirror/snapshot: avoid UnlinkPeerRequest with a unlinked peer (`pr#41304 <https://github.com/ceph/ceph/pull/41304>`_, Arthur Outhenin-Chalandre)
* librbd: don't stop at the first unremovable image when purging (`pr#41664 <https://github.com/ceph/ceph/pull/41664>`_, Ilya Dryomov)
* make-dist: refuse to run if script path contains a colon (`pr#41086 <https://github.com/ceph/ceph/pull/41086>`_, Nathan Cutler)
* mds: "FAILED ceph_assert(r == 0 || r == -2)" (`pr#42072 <https://github.com/ceph/ceph/pull/42072>`_, Xiubo Li)
* mds: "cluster [ERR]   Error recovering journal 0x203: (2) No such file or directory" in cluster log" (`pr#42059 <https://github.com/ceph/ceph/pull/42059>`_, Xiubo Li)
* mds: Add full caps to avoid osd full check (`pr#41691 <https://github.com/ceph/ceph/pull/41691>`_, Patrick Donnelly, Kotresh HR)
* mds: CephFS kclient gets stuck when getattr() on a certain file (`pr#42062 <https://github.com/ceph/ceph/pull/42062>`_, "Yan, Zheng", Xiubo Li)
* mds: Error ENOSYS: mds.a started profiler (`pr#42056 <https://github.com/ceph/ceph/pull/42056>`_, Xiubo Li)
* mds: MDSLog::journaler pointer maybe crash with use-after-free (`pr#42060 <https://github.com/ceph/ceph/pull/42060>`_, Xiubo Li)
* mds: avoid journaling overhead for setxattr("ceph.dir.subvolume") for no-op case (`pr#41995 <https://github.com/ceph/ceph/pull/41995>`_, Patrick Donnelly)
* mds: do not assert when receiving a unknow metric type (`pr#41596 <https://github.com/ceph/ceph/pull/41596>`_, Patrick Donnelly, Xiubo Li)
* mds: journal recovery thread is possibly asserting with mds_lock not locked (`pr#42058 <https://github.com/ceph/ceph/pull/42058>`_, Xiubo Li)
* mds: mkdir on ephemerally pinned directory sometimes blocked on journal flush (`pr#42071 <https://github.com/ceph/ceph/pull/42071>`_, Xiubo Li)
* mds: scrub error on inode 0x1 (`pr#41685 <https://github.com/ceph/ceph/pull/41685>`_, Milind Changire)
* mds: standby-replay only trims cache when it reaches the end of the replay log (`pr#40855 <https://github.com/ceph/ceph/pull/40855>`_, Xiubo Li, Patrick Donnelly)
* mgr/DaemonServer.cc: prevent mgr crashes caused by integer underflow that is triggered by large increases to pg_num/pgp_num (`pr#41862 <https://github.com/ceph/ceph/pull/41862>`_, Cory Snyder)
* mgr/Dashboard: Remove erroneous elements in hosts-overview Grafana dashboard (`pr#40982 <https://github.com/ceph/ceph/pull/40982>`_, Malcolm Holmes)
* mgr/dashboard: API Version changes do not apply to pre-defined methods (list, create etc.) (`pr#41675 <https://github.com/ceph/ceph/pull/41675>`_, Aashish Sharma)
* mgr/dashboard: Alertmanager fails to POST alerts (`pr#41987 <https://github.com/ceph/ceph/pull/41987>`_, Avan Thakkar)
* mgr/dashboard: Fix 500 error while exiting out of maintenance (`pr#41915 <https://github.com/ceph/ceph/pull/41915>`_, Nizamudeen A)
* mgr/dashboard: Fix bucket name input allowing space in the value (`pr#42119 <https://github.com/ceph/ceph/pull/42119>`_, Nizamudeen A)
* mgr/dashboard: Fix for query params resetting on change-password (`pr#41440 <https://github.com/ceph/ceph/pull/41440>`_, Nizamudeen A)
* mgr/dashboard: Generate NPM dependencies manifest (`pr#41204 <https://github.com/ceph/ceph/pull/41204>`_, Nizamudeen A)
* mgr/dashboard: Host Maintenance Follow ups (`pr#41056 <https://github.com/ceph/ceph/pull/41056>`_, Nizamudeen A)
* mgr/dashboard: Include Network address and labels on Host Creation form (`pr#42027 <https://github.com/ceph/ceph/pull/42027>`_, Nizamudeen A)
* mgr/dashboard: OSDs placement text is unreadable (`pr#41096 <https://github.com/ceph/ceph/pull/41096>`_, Aashish Sharma)
* mgr/dashboard: RGW buckets async validator performance enhancement and name constraints (`pr#41296 <https://github.com/ceph/ceph/pull/41296>`_, Nizamudeen A)
* mgr/dashboard: User database migration has been cut out (`pr#42140 <https://github.com/ceph/ceph/pull/42140>`_, Volker Theile)
* mgr/dashboard: avoid data processing in crush-map component (`pr#41203 <https://github.com/ceph/ceph/pull/41203>`_, Avan Thakkar)
* mgr/dashboard: bucket details: show lock retention period only in days (`pr#41948 <https://github.com/ceph/ceph/pull/41948>`_, Alfonso Martínez)
* mgr/dashboard: crushmap tree doesn't display crush type other than root (`pr#42007 <https://github.com/ceph/ceph/pull/42007>`_, Kefu Chai, Avan Thakkar)
* mgr/dashboard: disable NFSv3 support in dashboard (`pr#41200 <https://github.com/ceph/ceph/pull/41200>`_, Volker Theile)
* mgr/dashboard: drop container image name and id from services list (`pr#41505 <https://github.com/ceph/ceph/pull/41505>`_, Avan Thakkar)
* mgr/dashboard: fix API docs link (`pr#41507 <https://github.com/ceph/ceph/pull/41507>`_, Avan Thakkar)
* mgr/dashboard: fix ESOCKETTIMEDOUT E2E failure (`pr#41427 <https://github.com/ceph/ceph/pull/41427>`_, Avan Thakkar)
* mgr/dashboard: fix HAProxy (now called ingress) (`pr#41298 <https://github.com/ceph/ceph/pull/41298>`_, Avan Thakkar)
* mgr/dashboard: fix OSD out count (`pr#42153 <https://github.com/ceph/ceph/pull/42153>`_, 胡玮文)
* mgr/dashboard: fix OSDs Host details/overview grafana graphs (`issue#49769 <http://tracker.ceph.com/issues/49769>`_, `pr#41324 <https://github.com/ceph/ceph/pull/41324>`_, Alfonso Martínez, Michael Wodniok)
* mgr/dashboard: fix base-href (`pr#41634 <https://github.com/ceph/ceph/pull/41634>`_, Avan Thakkar)
* mgr/dashboard: fix base-href: revert it to previous approach (`pr#41251 <https://github.com/ceph/ceph/pull/41251>`_, Avan Thakkar)
* mgr/dashboard: fix bucket objects and size calculations (`pr#41646 <https://github.com/ceph/ceph/pull/41646>`_, Avan Thakkar)
* mgr/dashboard: fix bucket versioning when locking is enabled (`pr#41197 <https://github.com/ceph/ceph/pull/41197>`_, Avan Thakkar)
* mgr/dashboard: fix for right sidebar nav icon not clickable (`pr#42008 <https://github.com/ceph/ceph/pull/42008>`_, Aaryan Porwal)
* mgr/dashboard: fix set-ssl-certificate{,-key} commands (`pr#41170 <https://github.com/ceph/ceph/pull/41170>`_, Alfonso Martínez)
* mgr/dashboard: fix typo: Filesystems to File Systems (`pr#42016 <https://github.com/ceph/ceph/pull/42016>`_, Navin Barnwal)
* mgr/dashboard: ingress service creation follow-up (`pr#41428 <https://github.com/ceph/ceph/pull/41428>`_, Avan Thakkar)
* mgr/dashboard: pass Grafana datasource in URL (`pr#41633 <https://github.com/ceph/ceph/pull/41633>`_, Ernesto Puerta)
* mgr/dashboard: provide the service events when showing a service in the UI (`pr#41494 <https://github.com/ceph/ceph/pull/41494>`_, Aashish Sharma)
* mgr/dashboard: run cephadm-backend e2e tests with KCLI (`pr#42156 <https://github.com/ceph/ceph/pull/42156>`_, Alfonso Martínez)
* mgr/dashboard: set required env. variables in run-backend-api-tests.sh (`pr#41069 <https://github.com/ceph/ceph/pull/41069>`_, Alfonso Martínez)
* mgr/dashboard: show RGW tenant user id correctly in 'NFS create export' form (`pr#41528 <https://github.com/ceph/ceph/pull/41528>`_, Alfonso Martínez)
* mgr/dashboard: show partially deleted RBDs (`pr#41891 <https://github.com/ceph/ceph/pull/41891>`_, Tatjana Dehler)
* mgr/dashboard: simplify object locking fields in 'Bucket Creation' form (`pr#41777 <https://github.com/ceph/ceph/pull/41777>`_, Alfonso Martínez)
* mgr/dashboard: update frontend deps due to security vulnerabilities (`pr#41402 <https://github.com/ceph/ceph/pull/41402>`_, Alfonso Martínez)
* mgr/dashboard:include compression stats on pool dashboard (`pr#41577 <https://github.com/ceph/ceph/pull/41577>`_, Ernesto Puerta, Paul Cuzner)
* mgr/nfs: do not depend on cephadm.utils (`pr#41842 <https://github.com/ceph/ceph/pull/41842>`_, Sage Weil)
* mgr/progress: ensure progress stays between [0,1] (`pr#41312 <https://github.com/ceph/ceph/pull/41312>`_, Dan van der Ster)
* mgr/prometheus:Improve the pool metadata (`pr#40804 <https://github.com/ceph/ceph/pull/40804>`_, Paul Cuzner)
* mgr/pybind/snap_schedule: do not fail when no fs snapshots are available (`pr#41044 <https://github.com/ceph/ceph/pull/41044>`_, Sébastien Han)
* mgr/volumes/nfs: drop type param during cluster create (`pr#41005 <https://github.com/ceph/ceph/pull/41005>`_, Michael Fritch)
* mon,doc: deprecate min_compat_client (`pr#41468 <https://github.com/ceph/ceph/pull/41468>`_, Patrick Donnelly)
* mon/MonClient: reset authenticate_err in _reopen_session() (`pr#41019 <https://github.com/ceph/ceph/pull/41019>`_, Ilya Dryomov)
* mon/MonClient: tolerate a rotating key that is slightly out of date (`pr#41450 <https://github.com/ceph/ceph/pull/41450>`_, Ilya Dryomov)
* mon/OSDMonitor: drop stale failure_info after a grace period (`pr#41090 <https://github.com/ceph/ceph/pull/41090>`_, Kefu Chai)
* mon/OSDMonitor: drop stale failure_info even if can_mark_down() (`pr#41982 <https://github.com/ceph/ceph/pull/41982>`_, Kefu Chai)
* mon: load stashed map before mkfs monmap (`pr#41768 <https://github.com/ceph/ceph/pull/41768>`_, Dan van der Ster)
* nfs backport May (`pr#41389 <https://github.com/ceph/ceph/pull/41389>`_, Varsha Rao)
* os/FileStore: fix to handle readdir error correctly (`pr#41236 <https://github.com/ceph/ceph/pull/41236>`_, Misono Tomohiro)
* os/bluestore: fix unexpected ENOSPC in Avl/Hybrid allocators (`pr#41655 <https://github.com/ceph/ceph/pull/41655>`_, Igor Fedotov, Neha Ojha)
* os/bluestore: introduce multithreading sync for bluestore's repairer (`pr#41752 <https://github.com/ceph/ceph/pull/41752>`_, Igor Fedotov)
* os/bluestore: tolerate zero length for allocators' init\_[add/rm]_free() (`pr#41753 <https://github.com/ceph/ceph/pull/41753>`_, Igor Fedotov)
* osd/PG.cc: handle removal of pgmeta object (`pr#41680 <https://github.com/ceph/ceph/pull/41680>`_, Neha Ojha)
* osd/osd_type: use f->dump_unsigned() when appropriate (`pr#42045 <https://github.com/ceph/ceph/pull/42045>`_, Kefu Chai)
* osd/scrub: replace a ceph_assert() with a test (`pr#41944 <https://github.com/ceph/ceph/pull/41944>`_, Ronen Friedman)
* osd: Override recovery, backfill and sleep related config options during OSD and mclock scheduler initialization (`pr#41125 <https://github.com/ceph/ceph/pull/41125>`_, Sridhar Seshasayee, Zac Dover)
* osd: clear data digest when write_trunc (`pr#42019 <https://github.com/ceph/ceph/pull/42019>`_, Zengran Zhang)
* osd: compute OSD's space usage ratio via raw space utilization (`pr#41113 <https://github.com/ceph/ceph/pull/41113>`_, Igor Fedotov)
* osd: don't assert in-flight backfill is always in recovery list (`pr#41320 <https://github.com/ceph/ceph/pull/41320>`_, Mykola Golub)
* osd: fix scrub reschedule bug (`pr#41971 <https://github.com/ceph/ceph/pull/41971>`_, wencong wan)
* pacific: client: abort after MDS blocklist (`issue#50530 <http://tracker.ceph.com/issues/50530>`_, `pr#42070 <https://github.com/ceph/ceph/pull/42070>`_, Venky Shankar)
* pybind/ceph_volume_client: use cephfs mkdirs api (`pr#42159 <https://github.com/ceph/ceph/pull/42159>`_, Patrick Donnelly)
* pybind/mgr/devicehealth: scrape-health-metrics command accidentally renamed to scrape-daemon-health-metrics (`pr#41089 <https://github.com/ceph/ceph/pull/41089>`_, Patrick Donnelly)
* pybind/mgr/progress: Disregard unreported pgs (`pr#41872 <https://github.com/ceph/ceph/pull/41872>`_, Kamoltat)
* pybind/mgr/snap_schedule: Invalid command: Unexpected argument 'fs=cephfs' (`pr#42064 <https://github.com/ceph/ceph/pull/42064>`_, Patrick Donnelly)
* qa/config/rados: add dispatch delay testing params (`pr#41136 <https://github.com/ceph/ceph/pull/41136>`_, Deepika Upadhyay)
* qa/distros/podman: preserve registries.conf (`pr#40729 <https://github.com/ceph/ceph/pull/40729>`_, Sage Weil)
* qa/suites/rados/standalone: remove mon_election symlink (`pr#41212 <https://github.com/ceph/ceph/pull/41212>`_, Neha Ojha)
* qa/suites/rados: add simultaneous scrubs to the thrasher (`pr#42120 <https://github.com/ceph/ceph/pull/42120>`_, Ronen Friedman)
* qa/tasks/qemu: precise repos have been archived (`pr#41643 <https://github.com/ceph/ceph/pull/41643>`_, Ilya Dryomov)
* qa/tests: corrected point versions to reflect latest releases (`pr#41313 <https://github.com/ceph/ceph/pull/41313>`_, Yuri Weinstein)
* qa/tests: initial checkin for pacific-p2p suite (2) (`pr#41208 <https://github.com/ceph/ceph/pull/41208>`_, Yuri Weinstein)
* qa/tests: replaced ubuntu_latest.yaml with ubuntu 20.04 (`pr#41460 <https://github.com/ceph/ceph/pull/41460>`_, Patrick Donnelly, Kefu Chai)
* qa/upgrade: conditionally disable update_features tests (`pr#41629 <https://github.com/ceph/ceph/pull/41629>`_, Deepika)
* qa/workunits/rbd: use bionic version of qemu-iotests for focal (`pr#41195 <https://github.com/ceph/ceph/pull/41195>`_, Ilya Dryomov)
* qa: AttributeError: 'RemoteProcess' object has no attribute 'split' (`pr#41811 <https://github.com/ceph/ceph/pull/41811>`_, Patrick Donnelly)
* qa: add async dirops testing (`pr#41823 <https://github.com/ceph/ceph/pull/41823>`_, Patrick Donnelly)
* qa: check mounts attribute in ctx (`pr#40634 <https://github.com/ceph/ceph/pull/40634>`_, Jos Collin)
* qa: convert some legacy Filesystem.rados calls (`pr#40996 <https://github.com/ceph/ceph/pull/40996>`_, Patrick Donnelly)
* qa: drop the distro~HEAD directory from the fs suite (`pr#41169 <https://github.com/ceph/ceph/pull/41169>`_, Radoslaw Zarzynski)
* qa: fs:bugs does not specify distro (`pr#42063 <https://github.com/ceph/ceph/pull/42063>`_, Patrick Donnelly)
* qa: fs:upgrade uses teuthology default distro (`pr#42067 <https://github.com/ceph/ceph/pull/42067>`_, Patrick Donnelly)
* qa: scrub code does not join scrubopts with comma (`pr#42065 <https://github.com/ceph/ceph/pull/42065>`_, Kefu Chai, Patrick Donnelly)
* qa: test_data_scan.TestDataScan.test_pg_files AssertionError: Items in the second set but not the first (`pr#42069 <https://github.com/ceph/ceph/pull/42069>`_, Xiubo Li)
* qa: test_ephemeral_pin_distribution failure (`pr#41659 <https://github.com/ceph/ceph/pull/41659>`_, Patrick Donnelly)
* qa: update RHEL to 8.4 (`pr#41822 <https://github.com/ceph/ceph/pull/41822>`_, Patrick Donnelly)
* rbd-mirror: fix segfault in snapshot replayer shutdown (`pr#41503 <https://github.com/ceph/ceph/pull/41503>`_, Arthur Outhenin-Chalandre)
* rbd: --source-spec-file should be --source-spec-path (`pr#41122 <https://github.com/ceph/ceph/pull/41122>`_, Ilya Dryomov)
* rbd: don't attempt to interpret image cache state json (`pr#41281 <https://github.com/ceph/ceph/pull/41281>`_, Ilya Dryomov)
* rgw: Simplify log shard probing and err on the side of omap (`pr#41576 <https://github.com/ceph/ceph/pull/41576>`_, Adam C. Emerson)
* rgw: completion of multipart upload leaves delete marker (`pr#41769 <https://github.com/ceph/ceph/pull/41769>`_, J. Eric Ivancich)
* rgw: crash on multipart upload to bucket with policy (`pr#41893 <https://github.com/ceph/ceph/pull/41893>`_, Or Friedmann)
* rgw: radosgw_admin remove bucket not purging past 1,000 objects (`pr#41863 <https://github.com/ceph/ceph/pull/41863>`_, J. Eric Ivancich)
* rgw: radoslist incomplete multipart parts marker (`pr#40819 <https://github.com/ceph/ceph/pull/40819>`_, J. Eric Ivancich)
* rocksdb: pickup fix to detect PMULL instruction (`pr#41079 <https://github.com/ceph/ceph/pull/41079>`_, Kefu Chai)
* session dump includes completed_requests twice, once as an integer and once as a list (`pr#42057 <https://github.com/ceph/ceph/pull/42057>`_, Dan van der Ster)
* systemd: remove `ProtectClock=true` for `ceph-osd@.service` (`pr#41232 <https://github.com/ceph/ceph/pull/41232>`_, Wong Hoi Sing Edison)
* test/librbd: use really invalid domain (`pr#42010 <https://github.com/ceph/ceph/pull/42010>`_, Mykola Golub)
* win32\*.sh: disable libcephsqlite when targeting Windows (`pr#40557 <https://github.com/ceph/ceph/pull/40557>`_, Lucian Petrut)


v16.2.4 Pacific
===============

This is a hotfix release addressing a number of security issues and regressions. We recommend all users update to this release.

Changelog
---------

* mgr/dashboard: fix base-href: revert it to previous approach (`issue#50684 <https://tracker.ceph.com/issues/50684>`_, Avan Thakkar)
* mgr/dashboard: fix cookie injection issue (:ref:`CVE-2021-3509`, Ernesto Puerta)
* mgr/dashboard: fix set-ssl-certificate{,-key} commands (`issue#50519 <https://tracker.ceph.com/issues/50519>`_, Alfonso Martínez)
* rgw: RGWSwiftWebsiteHandler::is_web_dir checks empty subdir_name (:ref:`CVE-2021-3531`, Felix Huettner)
* rgw: sanitize \r in s3 CORSConfiguration's ExposeHeader (:ref:`CVE-2021-3524`, Sergey Bobrov, Casey Bodley)
* systemd: remove ProtectClock=true for ceph-osd@.service (`issue#50347 <https://tracker.ceph.com/issues/50347>`_, Wong Hoi Sing Edison)

v16.2.3 Pacific
===============

This is the third backport release in the Pacific series.  We recommend all users
update to this release.

Notable Changes
---------------

* This release fixes a cephadm upgrade bug that caused some systems to get stuck in a loop
  restarting the first mgr daemon.


v16.2.2 Pacific
===============

This is the second backport release in the Pacific series. We recommend all
users update to this release.

Notable Changes
---------------

* Cephadm now supports an *ingress* service type that provides load
  balancing and HA (via haproxy and keepalived on a virtual IP) for
  RGW service (see :ref:`orchestrator-haproxy-service-spec`).  (The experimental
  *rgw-ha* service has been removed.)

Changelog
---------

* ceph-fuse: src/include/buffer.h: 1187: FAILED ceph_assert(_num <= 1024) (`pr#40628 <https://github.com/ceph/ceph/pull/40628>`_, Yanhu Cao)
* ceph-volume: fix "device" output (`pr#41054 <https://github.com/ceph/ceph/pull/41054>`_, Sébastien Han)
* ceph-volume: fix raw listing when finding OSDs from different clusters (`pr#40985 <https://github.com/ceph/ceph/pull/40985>`_, Sébastien Han)
* ceph.spec.in: Enable tcmalloc on IBM Power and Z (`pr#39488 <https://github.com/ceph/ceph/pull/39488>`_, Nathan Cutler, Yaakov Selkowitz)
* cephadm april batch 3 (`issue#49737 <http://tracker.ceph.com/issues/49737>`_, `pr#40922 <https://github.com/ceph/ceph/pull/40922>`_, Adam King, Sage Weil, Daniel Pivonka, Shreyaa Sharma, Sebastian Wagner, Juan Miguel Olmo Martínez, Zac Dover, Jeff Layton, Guillaume Abrioux, 胡玮文, Melissa Li, Nathan Cutler, Yaakov Selkowitz)
* cephadm: april batch 1 (`pr#40544 <https://github.com/ceph/ceph/pull/40544>`_, Sage Weil, Daniel Pivonka, Joao Eduardo Luis, Adam King)
* cephadm: april batch backport 2 (`pr#40746 <https://github.com/ceph/ceph/pull/40746>`_, Guillaume Abrioux, Sage Weil, Paul Cuzner)
* cephadm: specify addr on bootstrap's host add (`pr#40554 <https://github.com/ceph/ceph/pull/40554>`_, Joao Eduardo Luis)
* cephfs: minor ceph-dokan improvements (`pr#40627 <https://github.com/ceph/ceph/pull/40627>`_, Lucian Petrut)
* client: items pinned in cache preventing unmount (`pr#40629 <https://github.com/ceph/ceph/pull/40629>`_, Xiubo Li)
* client: only check pool permissions for regular files (`pr#40686 <https://github.com/ceph/ceph/pull/40686>`_, Xiubo Li)
* cmake: define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT globally (`pr#40706 <https://github.com/ceph/ceph/pull/40706>`_, Kefu Chai)
* cmake: pass unparsed args to add_ceph_test() (`pr#40523 <https://github.com/ceph/ceph/pull/40523>`_, Kefu Chai)
* cmake: use --smp 1 --memory 256M to crimson tests (`pr#40568 <https://github.com/ceph/ceph/pull/40568>`_, Kefu Chai)
* crush/CrushLocation: do not print logging message in constructor (`pr#40679 <https://github.com/ceph/ceph/pull/40679>`_, Alex Wu)
* doc/cephfs/nfs: add user id, fs name and key to FSAL block (`pr#40687 <https://github.com/ceph/ceph/pull/40687>`_, Varsha Rao)
* include/librados: fix doxygen syntax for docs build (`pr#40805 <https://github.com/ceph/ceph/pull/40805>`_, Josh Durgin)
* mds: "cluster [WRN] Scrub error on inode 0x1000000039d (/client.0/tmp/blogbench-1.0/src/blogtest_in) see mds.a log and `damage ls` output for details" (`pr#40825 <https://github.com/ceph/ceph/pull/40825>`_, Milind Changire)
* mds: skip the buffer in UnknownPayload::decode() (`pr#40682 <https://github.com/ceph/ceph/pull/40682>`_, Xiubo Li)
* mgr/PyModule: put mgr_module_path before Py_GetPath() (`pr#40517 <https://github.com/ceph/ceph/pull/40517>`_, Kefu Chai)
* mgr/dashboard: Device health status is not getting listed under hosts section (`pr#40494 <https://github.com/ceph/ceph/pull/40494>`_, Aashish Sharma)
* mgr/dashboard: Fix for alert notification message being undefined (`pr#40588 <https://github.com/ceph/ceph/pull/40588>`_, Nizamudeen A)
* mgr/dashboard: Fix for broken User management role cloning (`pr#40398 <https://github.com/ceph/ceph/pull/40398>`_, Nizamudeen A)
* mgr/dashboard: Improve descriptions in some parts of the dashboard (`pr#40545 <https://github.com/ceph/ceph/pull/40545>`_, Nizamudeen A)
* mgr/dashboard: Remove username and password from request body (`pr#40981 <https://github.com/ceph/ceph/pull/40981>`_, Nizamudeen A)
* mgr/dashboard: Remove username, password fields from Manager Modules/dashboard,influx (`pr#40489 <https://github.com/ceph/ceph/pull/40489>`_, Aashish Sharma)
* mgr/dashboard: Revoke read-only user's access to Manager modules (`pr#40648 <https://github.com/ceph/ceph/pull/40648>`_, Nizamudeen A)
* mgr/dashboard: Unable to login to ceph dashboard until clearing cookies manually (`pr#40586 <https://github.com/ceph/ceph/pull/40586>`_, Avan Thakkar)
* mgr/dashboard: debug nodeenv hangs (`pr#40815 <https://github.com/ceph/ceph/pull/40815>`_, Ernesto Puerta)
* mgr/dashboard: filesystem pool size should use stored stat (`pr#40980 <https://github.com/ceph/ceph/pull/40980>`_, Avan Thakkar)
* mgr/dashboard: fix broken feature toggles (`pr#40474 <https://github.com/ceph/ceph/pull/40474>`_, Ernesto Puerta)
* mgr/dashboard: fix duplicated rows when creating NFS export (`pr#40990 <https://github.com/ceph/ceph/pull/40990>`_, Alfonso Martínez)
* mgr/dashboard: fix errors when creating NFS export (`pr#40822 <https://github.com/ceph/ceph/pull/40822>`_, Alfonso Martínez)
* mgr/dashboard: improve telemetry opt-in reminder notification message (`pr#40887 <https://github.com/ceph/ceph/pull/40887>`_, Waad Alkhoury)
* mgr/dashboard: test prometheus rules through promtool (`pr#40929 <https://github.com/ceph/ceph/pull/40929>`_, Aashish Sharma, Kefu Chai)
* mon: Modifying trim logic to change paxos_service_trim_max dynamically (`pr#40691 <https://github.com/ceph/ceph/pull/40691>`_, Aishwarya Mathuria)
* monmaptool: Don't call set_port on an invalid address (`pr#40690 <https://github.com/ceph/ceph/pull/40690>`_, Brad Hubbard, Kefu Chai)
* os/FileStore: don't propagate split/merge error to "create"/"remove" (`pr#40989 <https://github.com/ceph/ceph/pull/40989>`_, Mykola Golub)
* os/bluestore/BlueFS: do not _flush_range deleted files (`pr#40677 <https://github.com/ceph/ceph/pull/40677>`_, weixinwei)
* osd/PeeringState: fix acting_set_writeable min_size check (`pr#40759 <https://github.com/ceph/ceph/pull/40759>`_, Samuel Just)
* packaging: require ceph-common for immutable object cache daemon (`pr#40665 <https://github.com/ceph/ceph/pull/40665>`_, Ilya Dryomov)
* pybind/mgr/volumes: deadlock on async job hangs finisher thread (`pr#40630 <https://github.com/ceph/ceph/pull/40630>`_, Kefu Chai, Patrick Donnelly)
* qa/suites/krbd: don't require CEPHX_V2 for unmap subsuite (`pr#40826 <https://github.com/ceph/ceph/pull/40826>`_, Ilya Dryomov)
* qa/suites/rados/cephadm: stop testing on broken focal kubic podman (`pr#40512 <https://github.com/ceph/ceph/pull/40512>`_, Sage Weil)
* qa/tasks/ceph.conf: shorten cephx TTL for testing (`pr#40663 <https://github.com/ceph/ceph/pull/40663>`_, Sage Weil)
* qa/tasks/cephfs: create enough subvolumes (`pr#40688 <https://github.com/ceph/ceph/pull/40688>`_, Ramana Raja)
* qa/tasks/vstart_runner.py: start max required mgrs (`pr#40612 <https://github.com/ceph/ceph/pull/40612>`_, Alfonso Martínez)
* qa/tasks: Add wait_for_clean() check prior to initiating scrubbing (`pr#40461 <https://github.com/ceph/ceph/pull/40461>`_, Sridhar Seshasayee)
* qa: "AttributeError: 'NoneType' object has no attribute 'mon_manager'" (`pr#40645 <https://github.com/ceph/ceph/pull/40645>`_, Rishabh Dave)
* qa: "log [ERR] : error reading sessionmap 'mds2_sessionmap'" (`pr#40852 <https://github.com/ceph/ceph/pull/40852>`_, Patrick Donnelly)
* qa: fix ino_release_cb racy behavior (`pr#40683 <https://github.com/ceph/ceph/pull/40683>`_, Patrick Donnelly)
* qa: fs:cephadm mount does not wait for mds to be created (`pr#40528 <https://github.com/ceph/ceph/pull/40528>`_, Patrick Donnelly)
* qa: test standby_replay in workloads (`pr#40853 <https://github.com/ceph/ceph/pull/40853>`_, Patrick Donnelly)
* rbd-mirror: fix UB while registering perf counters (`pr#40680 <https://github.com/ceph/ceph/pull/40680>`_, Arthur Outhenin-Chalandre)
* rgw: add latency to the request summary of an op (`pr#40448 <https://github.com/ceph/ceph/pull/40448>`_, Ali Maredia)
* rgw: Backport of datalog improvements to Pacific (`pr#40559 <https://github.com/ceph/ceph/pull/40559>`_, Yuval Lifshitz, Adam C. Emerson)
* test: disable mgr/mirroring for `test_mirroring_init_failure_with_recovery` test (`issue#50020 <http://tracker.ceph.com/issues/50020>`_, `pr#40684 <https://github.com/ceph/ceph/pull/40684>`_, Venky Shankar)
* tools/cephfs_mirror/PeerReplayer.cc: add missing include (`pr#40678 <https://github.com/ceph/ceph/pull/40678>`_, Duncan Bellamy)
* vstart.sh: disable "auth_allow_insecure_global_id_reclaim" (`pr#40957 <https://github.com/ceph/ceph/pull/40957>`_, Kefu Chai)


v16.2.1 Pacific
===============

This is the first bugfix release in the Pacific stable series.  It addresses a
security vulnerability in the Ceph authentication framework.

We recommend all Pacific users upgrade.

Security fixes
--------------

* This release includes a security fix that ensures the global_id
  value (a numeric value that should be unique for every authenticated
  client or daemon in the cluster) is reclaimed after a network
  disconnect or ticket renewal in a secure fashion.  Two new health
  alerts may appear during the upgrade indicating that there are
  clients or daemons that are not yet patched with the appropriate
  fix.

  To temporarily mute the health alerts around insecure clients for the duration of the
  upgrade, you may want to::

    ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM 1h
    ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED 1h

  For more information, see :ref:`CVE-2021-20288`.


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
  For more information, see :ref:`mgr-nfs`.

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


.. _upgrading_from_octopus_or_nautilus:

Upgrading from Octopus or Nautilus
----------------------------------

Before starting, make sure your cluster is stable and healthy (no down or
recovering OSDs).  (This is optional, but recommended.)


.. note::
  WARNING: Please do not set `bluestore_fsck_quick_fix_on_mount` to true or
  run `ceph-bluestore-tool` repair or quick-fix commands in Pacific versions
  <= 16.2.6, because this can lead to data corruption, details in
  https://tracker.ceph.com/issues/53062.

.. note::
   When using multiple active Ceph Metadata Servers, ensure that there are
   no pending stray entries which are directories for active ranks except rank 0 as
   starting an upgrade (which sets `max_mds` to 1) could crash the Ceph
   Metadata Server. The following command should return zero (0) stray entries
   for all stray directories::

     # for idx in {0..9}; do ceph tell mds.<rank> dump tree ~mdsdir/stray$idx| jq '.[] | select (.nlink == 0 and .dir_layout.dir_hash > 0) | .stray_prior_path' | wc -l; done

   Ensure that all active ranks except rank 0 are checked for absence of stray
   entries which are directories (using the above command). Details are captured
   in http://tracker.ceph.com/issues/53597.

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

.. note:

   If you have deployed an RGW service on Octopus using the default port (7280), you
   will need to redeploy it because the default port changed (to 80 or 443, depending
   on whether SSL is enabled):

   .. prompt: bash #

     ceph orch apply rgw <realm>.<zone> --port 7280


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

   #. Disable FSMap sanity checks::

        # ceph config set mon mon_mds_skip_sanity true

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

   #. Remove `mon_mds_skip_sanity` setting::

        # ceph config rm mon mon_mds_skip_sanity

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
   recommended you enable the new :ref:`v2 network protocol <msgr2>`,
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

