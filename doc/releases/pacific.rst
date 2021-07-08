=======
Pacific
=======

Pacific is the 16th stable release of Ceph.  It is named after the
giant pacific octopus (Enteroctopus dofleini).

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

