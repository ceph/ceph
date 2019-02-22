v12.2.11 Luminous
=================

This is the eleventh bug fix release of the Luminous v12.2.x long term stable
release series. We recommend that all users upgrade to this release. Please note
the following precautions while upgrading.

Notable Changes
---------------

* This release fixes the pg log hard limit bug that was introduced in
  12.2.9, https://tracker.ceph.com/issues/36686.  A flag called
  `pglog_hardlimit` has been introduced, which is off by default. Enabling
  this flag will limit the length of the pg log.  In order to enable
  that, the flag must be set by running `ceph osd set pglog_hardlimit`
  after completely upgrading to 12.2.11. Once the cluster has this flag
  set, the length of the pg log will be capped by a hard limit. Once set,
  this flag *must not* be unset anymore.
  
* There have been fixes to RGW dynamic and manual resharding, which no longer
  leaves behind stale bucket instances to be removed manually. For finding and
  cleaning up older instances from a reshard a radosgw-admin command `reshard
  stale-instances list` and `reshard stale-instances rm` should do the necessary
  cleanup.

* `cephfs-journal-tool` makes rank argument (--rank) mandatory. Rank is
  of format `filesystem:rank`, where `filesystem` is the CephFS filesystem
  and `rank` is the MDS rank on which the operation is to be executed. To
  operate on all ranks, use `all` or `*` as the rank specifier. Note that,
  operations that dump journal information to file will now dump to per-rank
  suffixed dump files. Importing journal information from dump files is
  disallowed if operation is targetted for all ranks.

* CVE-2018-14662: mon: limit caps allowed to access the config store

* CVE-2018-16846: rgw: enforce bounds on max-keys/max-uploads/max-parts (`issue#35994 <http://tracker.ceph.com/issues/35994>`)

* CVE-2018-16889: rgw: sanitize customer encryption keys from log output in v4 auth (`issue#37847 <http://tracker.ceph.com/issues/37847>`)

Changelog
---------

* build/ops: cmake: link unittest_compression against gtest (`pr#24921 <https://github.com/ceph/ceph/pull/24921>`_, Willem Jan Withagen)
* build/ops: run-make-check.sh ccache tweaks (`issue#24826 <http://tracker.ceph.com/issues/24826>`_, `issue#24817 <http://tracker.ceph.com/issues/24817>`_, `issue#24777 <http://tracker.ceph.com/issues/24777>`_, `pr#23902 <https://github.com/ceph/ceph/pull/23902>`_, Nathan Cutler, Erwan Velu)
* ceph-bluestore-tool: fix set label functionality for specific keys (`pr#25187 <https://github.com/ceph/ceph/pull/25187>`_, Igor Fedotov)
* ceph-create-keys: fix octal notation for Python 3 without losing compatibility with Python 2 (`issue#37643 <http://tracker.ceph.com/issues/37643>`_, `pr#25532 <https://github.com/ceph/ceph/pull/25532>`_, James Page)
* cephfs: ceph-volume-client: allow setting mode of CephFS volumes (`pr#25407 <https://github.com/ceph/ceph/pull/25407>`_, Tom Barron)
* cephfs-journal-tool: make --rank argument mandatory (`pr#24728 <https://github.com/ceph/ceph/pull/24728>`_, Venky Shankar)
* cephfs: mgr/status: fix fs status subcommand did not show standby-replay MDS' perf info (`issue#36575 <http://tracker.ceph.com/issues/36575>`_, `issue#36399 <http://tracker.ceph.com/issues/36399>`_, `pr#25032 <https://github.com/ceph/ceph/pull/25032>`_, Zhi Zhang)
* cephfs: race of updating wanted caps (`issue#37635 <http://tracker.ceph.com/issues/37635>`_, `issue#37464 <http://tracker.ceph.com/issues/37464>`_, `pr#25762 <https://github.com/ceph/ceph/pull/25762>`_, "Yan, Zheng")
* ceph-volume: Adapt code to support Python3 (`pr#26030 <https://github.com/ceph/ceph/pull/26030>`_, Volker Theile)
* ceph-volume add device_id to inventory listing (`pr#25350 <https://github.com/ceph/ceph/pull/25350>`_, Jan Fajerski)
* ceph-volume: enable device discards (`issue#36532 <http://tracker.ceph.com/issues/36532>`_, `pr#25748 <https://github.com/ceph/ceph/pull/25748>`_, Jonas Jelten)
* ceph-volume: fix Batch object in py3 environments (`pr#25552 <https://github.com/ceph/ceph/pull/25552>`_, Jan Fajerski)
* ceph-volume: fix JSON output in `inventory` (`issue#37390 <http://tracker.ceph.com/issues/37390>`_, `pr#25922 <https://github.com/ceph/ceph/pull/25922>`_, Sebastian Wagner)
* ceph-volume: Fix TypeError: join() takes exactly one argument (2 given) (`issue#37595 <http://tracker.ceph.com/issues/37595>`_, `pr#25772 <https://github.com/ceph/ceph/pull/25772>`_, Sebastian Wagner)
* ceph-volume fix TypeError on dmcrypt when using Python3 (`pr#26114 <https://github.com/ceph/ceph/pull/26114>`_, Alfredo Deza)
* ceph-volume: introduce class hierachy for strategies (`pr#25553 <https://github.com/ceph/ceph/pull/25553>`_, Jan Fajerski, Alfredo Deza)
* ceph-volume: mark a device not available if it belongs to ceph-disk (`pr#26117 <https://github.com/ceph/ceph/pull/26117>`_, Andrew Schoen)
* ceph-volume normalize comma to dot for string to int conversions (`issue#37442 <http://tracker.ceph.com/issues/37442>`_, `pr#25776 <https://github.com/ceph/ceph/pull/25776>`_, Alfredo Deza)
* ceph-volume: set permissions right before prime-osd-dir (`issue#37486 <http://tracker.ceph.com/issues/37486>`_, `pr#25778 <https://github.com/ceph/ceph/pull/25778>`_, Andrew Schoen, Alfredo Deza)
* ceph-volume tests/functional declare ceph-ansible roles instead of importing them (`issue#37805 <http://tracker.ceph.com/issues/37805>`_, `pr#25838 <https://github.com/ceph/ceph/pull/25838>`_, Alfredo Deza)
* ceph-volume zap devices associated with an OSD ID and/or OSD FSID (`pr#26014 <https://github.com/ceph/ceph/pull/26014>`_, Alfredo Deza)
* ceph-volume: zap: improve zapping to remove all partitions and all LVs, encrypted or not (`issue#37449 <http://tracker.ceph.com/issues/37449>`_, `pr#25352 <https://github.com/ceph/ceph/pull/25352>`_, Alfredo Deza)
* cli: dump osd-fsid as part of osd find <id> (`issue#37966 <http://tracker.ceph.com/issues/37966>`_, `pr#26036 <https://github.com/ceph/ceph/pull/26036>`_, Noah Watkins)
* client: do not move f->pos untill success write (`issue#37631 <http://tracker.ceph.com/issues/37631>`_, `pr#25684 <https://github.com/ceph/ceph/pull/25684>`_, Junhui Tang)
* client: explicitly show blacklisted state via asok status command (`issue#36456 <http://tracker.ceph.com/issues/36456>`_, `issue#36352 <http://tracker.ceph.com/issues/36352>`_, `pr#24994 <https://github.com/ceph/ceph/pull/24994>`_, Jonathan Brielmaier, Zhi Zhang)
* client: fix fuse client hang because its pipe to mds is not ok4 (`issue#37829 <http://tracker.ceph.com/issues/37829>`_, `issue#36079 <http://tracker.ceph.com/issues/36079>`_, `pr#25904 <https://github.com/ceph/ceph/pull/25904>`_, Guan yunfei)
* client: request next osdmap for blacklisted client (`issue#36668 <http://tracker.ceph.com/issues/36668>`_, `issue#36691 <http://tracker.ceph.com/issues/36691>`_, `pr#24986 <https://github.com/ceph/ceph/pull/24986>`_, Zhi Zhang)
* common: auth/AuthSessionHandler: no handler if no session key (`issue#37427 <http://tracker.ceph.com/issues/37427>`_, `issue#36443 <http://tracker.ceph.com/issues/36443>`_, `pr#25297 <https://github.com/ceph/ceph/pull/25297>`_, Sage Weil)
* common/blkdev, ceph-volume: improve get_device_id (`pr#25752 <https://github.com/ceph/ceph/pull/25752>`_, Sage Weil)
* common: fix memory leaks in WeightedPriorityQueue (`issue#37429 <http://tracker.ceph.com/issues/37429>`_, `issue#36248 <http://tracker.ceph.com/issues/36248>`_, `pr#25296 <https://github.com/ceph/ceph/pull/25296>`_, Radoslaw Zarzynski)
* common: (mon) command sanitization accepts floats when Int type is defined resulting in exception fault in ceph-mon (`issue#26919 <http://tracker.ceph.com/issues/26919>`_, `pr#24374 <https://github.com/ceph/ceph/pull/24374>`_, Sage Weil)
* common: shut up some warnings (`pr#24648 <https://github.com/ceph/ceph/pull/24648>`_, Kefu Chai)
* config: drop config::lock when invoking config observer (`issue#37762 <http://tracker.ceph.com/issues/37762>`_, `pr#25833 <https://github.com/ceph/ceph/pull/25833>`_, Kefu Chai, Venky Shankar)
* core: bluestore: rename does not old ref to replacement onode at old name (`issue#36541 <http://tracker.ceph.com/issues/36541>`_, `issue#36638 <http://tracker.ceph.com/issues/36638>`_, `pr#24989 <https://github.com/ceph/ceph/pull/24989>`_, Jonathan Brielmaier, Sage Weil)
* core: enable the pg deletion process to be throttled (`issue#36321 <http://tracker.ceph.com/issues/36321>`_, `pr#24501 <https://github.com/ceph/ceph/pull/24501>`_, David Zafman)
* core: mgr crash on scrub of unconnected osd (`issue#36110 <http://tracker.ceph.com/issues/36110>`_, `issue#36464 <http://tracker.ceph.com/issues/36464>`_, `pr#25030 <https://github.com/ceph/ceph/pull/25030>`_, Sage Weil)
* core: mon osdmap cash too small during upgrade to mimic (`issue#36506 <http://tracker.ceph.com/issues/36506>`_, `pr#25021 <https://github.com/ceph/ceph/pull/25021>`_, Sage Weil)
* core: Objecter: add ignore cache flag if got redirect reply (`issue#36657 <http://tracker.ceph.com/issues/36657>`_, `pr#25074 <https://github.com/ceph/ceph/pull/25074>`_, Iain Buclaw, Jonathan Brielmaier)
* core: os/bluestore_tool: fix bluefs expand (`pr#25384 <https://github.com/ceph/ceph/pull/25384>`_, Igor Fedotov)
* core: rados rm --force-full is blocked when cluster is in full status (`issue#36436 <http://tracker.ceph.com/issues/36436>`_, `pr#25018 <https://github.com/ceph/ceph/pull/25018>`_, Yang Honggang)
* crushtool: add --reclassify operation to convert legacy crush maps to use device classes (`pr#25307 <https://github.com/ceph/ceph/pull/25307>`_, Sage Weil)
* debian: correct ceph-common relationship with older radosgw package (`pr#24997 <https://github.com/ceph/ceph/pull/24997>`_, Matthew Vernon)
* doc: broken link on troubleshooting-mon page (`pr#25500 <https://github.com/ceph/ceph/pull/25500>`_, James McClune)
* doc: fix broken fstab url in cephfs/fuse (`issue#36286 <http://tracker.ceph.com/issues/36286>`_, `pr#24434 <https://github.com/ceph/ceph/pull/24434>`_, Jos Collin)
* doc: Fix typo error on cephfs/fuse/ (`issue#36180 <http://tracker.ceph.com/issues/36180>`_, `issue#36309 <http://tracker.ceph.com/issues/36309>`_, `pr#24752 <https://github.com/ceph/ceph/pull/24752>`_, Karun Josy)
* doc: Put command template into literal block (`pr#25001 <https://github.com/ceph/ceph/pull/25001>`_, Alexey Stupnikov)
* doc/rados: update bluestore provisioning and autotuning docs (`issue#37341 <http://tracker.ceph.com/issues/37341>`_, `pr#25284 <https://github.com/ceph/ceph/pull/25284>`_, Mark Nelson)
* doc: show edit on github links and version warnings (`pr#25267 <https://github.com/ceph/ceph/pull/25267>`_, Neha Ojha, Noah Watkins)
* doc/user-management: Remove obsolete reset caps command (`issue#37663 <http://tracker.ceph.com/issues/37663>`_, `pr#25609 <https://github.com/ceph/ceph/pull/25609>`_, Brad Hubbard)
* examples: fix link order in librados example Makefile (`issue#37795 <http://tracker.ceph.com/issues/37795>`_, `pr#25829 <https://github.com/ceph/ceph/pull/25829>`_, Mahati Chamarthy)
* extend reconnect period when mds is busy (`issue#37739 <http://tracker.ceph.com/issues/37739>`_, `pr#25784 <https://github.com/ceph/ceph/pull/25784>`_, "Yan, Zheng")
* fsck: cid is improperly matched to oid (`issue#36145 <http://tracker.ceph.com/issues/36145>`_, `issue#32731 <http://tracker.ceph.com/issues/32731>`_, `pr#24705 <https://github.com/ceph/ceph/pull/24705>`_, Sage Weil)
* libcephfs: expose CEPH_SETATTR_MTIME_NOW and CEPH_SETATTR_ATIME_NOW (`issue#36206 <http://tracker.ceph.com/issues/36206>`_, `issue#35961 <http://tracker.ceph.com/issues/35961>`_, `pr#24465 <https://github.com/ceph/ceph/pull/24465>`_, Zhu Shangzhong)
* librbd: fix missing unblock_writes if shrink is not allowed (`issue#37363 <http://tracker.ceph.com/issues/37363>`_, `issue#36778 <http://tracker.ceph.com/issues/36778>`_, `pr#25253 <https://github.com/ceph/ceph/pull/25253>`_, runsisi)
* librbd: reset snaps in rbd_snap_list() (`issue#37535 <http://tracker.ceph.com/issues/37535>`_, `issue#37508 <http://tracker.ceph.com/issues/37508>`_, `pr#25458 <https://github.com/ceph/ceph/pull/25458>`_, Kefu Chai)
* mds: add "drop cache" command (`issue#36695 <http://tracker.ceph.com/issues/36695>`_, `issue#36281 <http://tracker.ceph.com/issues/36281>`_, `pr#24468 <https://github.com/ceph/ceph/pull/24468>`_, Rishabh Dave, Patrick Donnelly, Venky Shankar)
* mds: clean up log messages for standby-replay (`pr#25804 <https://github.com/ceph/ceph/pull/25804>`_, Patrick Donnelly)
* mds: create heartbeat grace config option (`issue#37674 <http://tracker.ceph.com/issues/37674>`_, `issue#37820 <http://tracker.ceph.com/issues/37820>`_, `pr#25889 <https://github.com/ceph/ceph/pull/25889>`_, Patrick Donnelly)
* mds: directories pinned keep being replicated back and forth between exporting mds and importing mds (`issue#37368 <http://tracker.ceph.com/issues/37368>`_, `issue#37606 <http://tracker.ceph.com/issues/37606>`_, `pr#25522 <https://github.com/ceph/ceph/pull/25522>`_, Xuehan Xu)
* mds: disallow dumping huge caches to formatter (`issue#37608 <http://tracker.ceph.com/issues/37608>`_, `pr#25567 <https://github.com/ceph/ceph/pull/25567>`_, Venky Shankar)
* mds: do not call Journaler::_trim twice (`issue#37566 <http://tracker.ceph.com/issues/37566>`_, `issue#37629 <http://tracker.ceph.com/issues/37629>`_, `pr#25562 <https://github.com/ceph/ceph/pull/25562>`_, Tang Junhui)
* mds: fix bug filelock stuck at LOCK_XSYN leading client can't read data (`issue#37700 <http://tracker.ceph.com/issues/37700>`_, `issue#37333 <http://tracker.ceph.com/issues/37333>`_, `pr#25677 <https://github.com/ceph/ceph/pull/25677>`_, Guan yunfei)
* mds: fix incorrect l_pq_executing_ops statistics when meet an invalid item in purge queue (`issue#37627 <http://tracker.ceph.com/issues/37627>`_, `issue#37567 <http://tracker.ceph.com/issues/37567>`_, `pr#25560 <https://github.com/ceph/ceph/pull/25560>`_, Junhui Tang)
* mds: fix infinite loop in OpTracker::check_ops_in_flight (`issue#37977 <http://tracker.ceph.com/issues/37977>`_, `pr#26048 <https://github.com/ceph/ceph/pull/26048>`_, "Yan, Zheng")
* mds: fix infinite loop in OpTracker::check_ops_in_flight (`issue#37977 <http://tracker.ceph.com/issues/37977>`_, `pr#26088 <https://github.com/ceph/ceph/pull/26088>`_, "Yan, Zheng")
* mds: fix mds damaged due to unexpected journal length (`issue#36200 <http://tracker.ceph.com/issues/36200>`_, `pr#24440 <https://github.com/ceph/ceph/pull/24440>`_, Zhi Zhang)
* mds: migrate strays part by part when shutdown mds (`issue#26926 <http://tracker.ceph.com/issues/26926>`_, `issue#32091 <http://tracker.ceph.com/issues/32091>`_, `pr#24324 <https://github.com/ceph/ceph/pull/24324>`_, "Yan, Zheng")
* MDSMonitor: allow beacons from stopping MDS that was laggy (`issue#37737 <http://tracker.ceph.com/issues/37737>`_, `pr#25686 <https://github.com/ceph/ceph/pull/25686>`_, Patrick Donnelly)
* mds: obsolete MDSMap option configs (`issue#37540 <http://tracker.ceph.com/issues/37540>`_, `pr#25431 <https://github.com/ceph/ceph/pull/25431>`_, Patrick Donnelly)
* mds: purge queue recovery hangs during boot if PQ journal is damaged (`issue#37899 <http://tracker.ceph.com/issues/37899>`_, `issue#37543 <http://tracker.ceph.com/issues/37543>`_, `pr#25968 <https://github.com/ceph/ceph/pull/25968>`_, Patrick Donnelly)
* mds: PurgeQueue write error handler does not handle EBLACKLISTED (`issue#37604 <http://tracker.ceph.com/issues/37604>`_, `pr#25524 <https://github.com/ceph/ceph/pull/25524>`_, Patrick Donnelly)
* mds: rctime not set on system inode (root) at startup (`issue#36221 <http://tracker.ceph.com/issues/36221>`_, `issue#36460 <http://tracker.ceph.com/issues/36460>`_, `pr#25043 <https://github.com/ceph/ceph/pull/25043>`_, Patrick Donnelly)
* mds: remove duplicated l_mdc_num_strays perfcounter set (`issue#37633 <http://tracker.ceph.com/issues/37633>`_, `issue#37516 <http://tracker.ceph.com/issues/37516>`_, `pr#25682 <https://github.com/ceph/ceph/pull/25682>`_, Zhi Zhang)
* mds: severe internal fragment when decoding xattr_map from log event (`issue#37399 <http://tracker.ceph.com/issues/37399>`_, `issue#37602 <http://tracker.ceph.com/issues/37602>`_, `pr#25520 <https://github.com/ceph/ceph/pull/25520>`_, "Yan, Zheng")
* mds: "src/mds/MDLog.cc: 281: FAILED ceph_assert(!capped)" during max_mds thrashing (`issue#36350 <http://tracker.ceph.com/issues/36350>`_, `issue#37092 <http://tracker.ceph.com/issues/37092>`_, `pr#25826 <https://github.com/ceph/ceph/pull/25826>`_, "Yan, Zheng")
* mgr/balancer: add cmd to list all plans (`issue#37420 <http://tracker.ceph.com/issues/37420>`_, `pr#25259 <https://github.com/ceph/ceph/pull/25259>`_, Yang Honggang)
* mgr/balancer: add crush_compat_metrics param (`issue#37413 <http://tracker.ceph.com/issues/37413>`_, `pr#25257 <https://github.com/ceph/ceph/pull/25257>`_, Dan van der Ster)
* mgr: balancer: python 3 compat fixes (`issue#37416 <http://tracker.ceph.com/issues/37416>`_, `pr#25258 <https://github.com/ceph/ceph/pull/25258>`_, Noah Watkins)
* mgr: fix crash due to multiple sessions from daemons with same name (`pr#25867 <https://github.com/ceph/ceph/pull/25867>`_, Mykola Golub)
* mgr: hold lock while accessing the request list and submitting request (`pr#25047 <https://github.com/ceph/ceph/pull/25047>`_, Jerry Lee)
* mgr: Module 'influx' has failed (`issue#25201 <http://tracker.ceph.com/issues/25201>`_, `pr#25184 <https://github.com/ceph/ceph/pull/25184>`_, Nathan Cutler, Wido den Hollander)
* mgr: prometheus: added bluestore db and wal devices to ceph_disk_occupation metric.// (`issue#37362 <http://tracker.ceph.com/issues/37362>`_, `pr#25216 <https://github.com/ceph/ceph/pull/25216>`_, Konstantin Shalygin)
* mgr: race between daemon state and service map in 'service status' (`issue#37478 <http://tracker.ceph.com/issues/37478>`_, `issue#36656 <http://tracker.ceph.com/issues/36656>`_, `pr#25369 <https://github.com/ceph/ceph/pull/25369>`_, Mykola Golub)
* mgr: [restful] deep_scrub is not a valid OSD command (`issue#36720 <http://tracker.ceph.com/issues/36720>`_, `issue#36750 <http://tracker.ceph.com/issues/36750>`_, `pr#25041 <https://github.com/ceph/ceph/pull/25041>`_, Boris Ranto)
* mon: mark REMOVE_SNAPS messages as no_reply (`issue#37568 <http://tracker.ceph.com/issues/37568>`_, `issue#37694 <http://tracker.ceph.com/issues/37694>`_, `pr#25779 <https://github.com/ceph/ceph/pull/25779>`_, "Yan, Zheng")
* mon/OSDMonitor: do not populate void pg_temp into nextmap (`issue#37811 <http://tracker.ceph.com/issues/37811>`_, `pr#25845 <https://github.com/ceph/ceph/pull/25845>`_, Aleksei Zakharov)
* mon: shutdown messenger early to avoid accessing deleted logger (`issue#37780 <http://tracker.ceph.com/issues/37780>`_, `issue#37813 <http://tracker.ceph.com/issues/37813>`_, `pr#25847 <https://github.com/ceph/ceph/pull/25847>`_, ningtao)
* os/bluestore: avoid frequent allocator dump on bluefs rebalance failure (`pr#24543 <https://github.com/ceph/ceph/pull/24543>`_, Igor Fedotov)
* os/bluestore/BlueStore.cc: 1025: FAILED assert(buffer_bytes >= b->length) from ObjectStore/StoreTest.ColSplitTest2/2 (`issue#26943 <http://tracker.ceph.com/issues/26943>`_, `issue#24439 <http://tracker.ceph.com/issues/24439>`_, `pr#24992 <https://github.com/ceph/ceph/pull/24992>`_, Jonathan Brielmaier, Sage Weil)
* os/bluestore: handle spurious read errors (`issue#22464 <http://tracker.ceph.com/issues/22464>`_, `pr#24649 <https://github.com/ceph/ceph/pull/24649>`_, Paul Emmerich)
* osd: backport recent upmap fixes (`pr#25418 <https://github.com/ceph/ceph/pull/25418>`_, ningtao, xie xingguo)
* osdc/Objecter: update op_target_t::paused in _calc_target (`issue#37398 <http://tracker.ceph.com/issues/37398>`_, `issue#37553 <http://tracker.ceph.com/issues/37553>`_, `pr#25719 <https://github.com/ceph/ceph/pull/25719>`_, Song Shun, runsisi)
* osdc: reduce ObjectCacher's memory fragments (`issue#36642 <http://tracker.ceph.com/issues/36642>`_, `issue#36192 <http://tracker.ceph.com/issues/36192>`_, `pr#24872 <https://github.com/ceph/ceph/pull/24872>`_, "Yan, Zheng")
* osd: failed assert when osd_memory_target options mismatch (`issue#37697 <http://tracker.ceph.com/issues/37697>`_, `issue#37507 <http://tracker.ceph.com/issues/37507>`_, `pr#25604 <https://github.com/ceph/ceph/pull/25604>`_, xie xingguo)
* osd/mon: pg log hard limit with upgrades fixed (`issue#37903 <http://tracker.ceph.com/issues/37903>`_, `issue#21416 <http://tracker.ceph.com/issues/21416>`_, `pr#25949 <https://github.com/ceph/ceph/pull/25949>`_, Neha Ojha, xie xingguo)
* osd/OSD.cc: log slow requests in OSD logs (`pr#25824 <https://github.com/ceph/ceph/pull/25824>`_, Neha Ojha)
* osd/OSDMap: cancel mapping if target osd is out (`issue#37501 <http://tracker.ceph.com/issues/37501>`_, `pr#25698 <https://github.com/ceph/ceph/pull/25698>`_, ningtao, xie xingguo)
* osd: potential deadlock in PG::_scan_snaps when repairing snap mapper (`issue#36630 <http://tracker.ceph.com/issues/36630>`_, `pr#24833 <https://github.com/ceph/ceph/pull/24833>`_, Mykola Golub)
* osd: Prioritize user specified scrubs (`issue#37343 <http://tracker.ceph.com/issues/37343>`_, `issue#37269 <http://tracker.ceph.com/issues/37269>`_, `pr#25514 <https://github.com/ceph/ceph/pull/25514>`_, kungf, David Zafman)
* osd: race condition opening heartbeat connection (`issue#36602 <http://tracker.ceph.com/issues/36602>`_, `issue#36636 <http://tracker.ceph.com/issues/36636>`_, `pr#25035 <https://github.com/ceph/ceph/pull/25035>`_, Sage Weil)
* osd: RBD client IOPS pool stats are incorrect (2x higher; includes IO hints as an op) (`issue#24909 <http://tracker.ceph.com/issues/24909>`_, `issue#36556 <http://tracker.ceph.com/issues/36556>`_, `pr#25025 <https://github.com/ceph/ceph/pull/25025>`_, Jason Dillaman)
* pybind/mgr/status: fix ceph fs status in py3 environments (`issue#37573 <http://tracker.ceph.com/issues/37573>`_, `issue#37625 <http://tracker.ceph.com/issues/37625>`_, `pr#25695 <https://github.com/ceph/ceph/pull/25695>`_, Jan Fajerski)
* rbd: pybind: added missing RBD_FLAG_FAST_DIFF_INVALID constant (`issue#36407 <http://tracker.ceph.com/issues/36407>`_, `pr#25006 <https://github.com/ceph/ceph/pull/25006>`_, Jason Dillaman)
* rbd: [rbd-mirror] periodic mirror status timer might fail to be scheduled (`issue#36500 <http://tracker.ceph.com/issues/36500>`_, `issue#36554 <http://tracker.ceph.com/issues/36554>`_, `pr#24917 <https://github.com/ceph/ceph/pull/24917>`_, Nathan Cutler, Jason Dillaman)
* rgw: add ssl support to beast frontend (`issue#22832 <http://tracker.ceph.com/issues/22832>`_, `issue#24358 <http://tracker.ceph.com/issues/24358>`_, `issue#23680 <http://tracker.ceph.com/issues/23680>`_, `pr#24621 <https://github.com/ceph/ceph/pull/24621>`_, Casey Bodley)
* rgw: apply quota config to users created via external auth (`issue#24595 <http://tracker.ceph.com/issues/24595>`_, `issue#36222 <http://tracker.ceph.com/issues/36222>`_, `pr#24547 <https://github.com/ceph/ceph/pull/24547>`_, Casey Bodley, Matt Benjamin)
* rgw: beast frontend fails to parse ipv6 endpoints (`issue#36733 <http://tracker.ceph.com/issues/36733>`_, `issue#36662 <http://tracker.ceph.com/issues/36662>`_, `pr#25512 <https://github.com/ceph/ceph/pull/25512>`_, Casey Bodley)
* rgw: bucket resharding fixes (`issue#37446 <http://tracker.ceph.com/issues/37446>`_, `issue#36688 <http://tracker.ceph.com/issues/36688>`_, `pr#25326 <https://github.com/ceph/ceph/pull/25326>`_, Orit Wasserman, Abhishek Lekshmanan, J. Eric Ivancich)
* rgw: catch exceptions from librados::NObjectIterator (`issue#37091 <http://tracker.ceph.com/issues/37091>`_, `issue#37475 <http://tracker.ceph.com/issues/37475>`_, `pr#25289 <https://github.com/ceph/ceph/pull/25289>`_, Casey Bodley)
* rgw: Don't treat colons specially in resource part of ARN (`issue#37482 <http://tracker.ceph.com/issues/37482>`_, `issue#23817 <http://tracker.ceph.com/issues/23817>`_, `pr#25387 <https://github.com/ceph/ceph/pull/25387>`_, Adam C. Emerson)
* rgw: es fixes for working with nfs ganesha (`issue#37349 <http://tracker.ceph.com/issues/37349>`_, `issue#36233 <http://tracker.ceph.com/issues/36233>`_, `issue#22758 <http://tracker.ceph.com/issues/22758>`_, `pr#25444 <https://github.com/ceph/ceph/pull/25444>`_, Abhishek Lekshmanan)
* rgw_file: user info never synced since librgw init (`issue#37549 <http://tracker.ceph.com/issues/37549>`_, `pr#25484 <https://github.com/ceph/ceph/pull/25484>`_, Tao Chen)
* rgw: fixes for zone deletion (`issue#37328 <http://tracker.ceph.com/issues/37328>`_, `issue#37466 <http://tracker.ceph.com/issues/37466>`_, `pr#25320 <https://github.com/ceph/ceph/pull/25320>`_, Abhishek Lekshmanan)
* rgw: fix max-size in radosgw-admin and REST Admin API (`issue#37519 <http://tracker.ceph.com/issues/37519>`_, `pr#25448 <https://github.com/ceph/ceph/pull/25448>`_, Nick Erdmann)
* rgw: fix version bucket stats (`issue#37563 <http://tracker.ceph.com/issues/37563>`_, `issue#21429 <http://tracker.ceph.com/issues/21429>`_, `pr#25644 <https://github.com/ceph/ceph/pull/25644>`_, Shasha Lu)
* rgw: librgw: crashes in multisite configuration (`issue#36302 <http://tracker.ceph.com/issues/36302>`_, `issue#36414 <http://tracker.ceph.com/issues/36414>`_, `pr#24909 <https://github.com/ceph/ceph/pull/24909>`_, Casey Bodley)
* rgw: multisite: sync gets stuck retrying deletes that fail with ERR_PRECONDITION_FAILED (`issue#37551 <http://tracker.ceph.com/issues/37551>`_, `issue#37448 <http://tracker.ceph.com/issues/37448>`_, `pr#25506 <https://github.com/ceph/ceph/pull/25506>`_, Casey Bodley)
* rgw: radosgw-admin: translate reshard status codes (trivial) (`issue#37284 <http://tracker.ceph.com/issues/37284>`_, `issue#36486 <http://tracker.ceph.com/issues/36486>`_, `pr#25195 <https://github.com/ceph/ceph/pull/25195>`_, Matt Benjamin)
* rgw: rgw-admin: reshard add can add a non-existent bucket (`issue#36449 <http://tracker.ceph.com/issues/36449>`_, `issue#36757 <http://tracker.ceph.com/issues/36757>`_, `pr#25088 <https://github.com/ceph/ceph/pull/25088>`_, Jonathan Brielmaier, Abhishek Lekshmanan)
* rgw: SSE encryption does not detect ssl termination in proxy (`issue#36644 <http://tracker.ceph.com/issues/36644>`_, `issue#27221 <http://tracker.ceph.com/issues/27221>`_, `pr#24944 <https://github.com/ceph/ceph/pull/24944>`_, Jonathan Brielmaier, Casey Bodley)
* rpm: Use hardened LDFLAGS (`issue#36316 <http://tracker.ceph.com/issues/36316>`_, `issue#36391 <http://tracker.ceph.com/issues/36391>`_, `pr#25173 <https://github.com/ceph/ceph/pull/25173>`_, Boris Ranto)


v12.2.10 Luminous
=================

This is the tenth bug fix release of the Luminous v12.2.x long term stable
release series. The previous release, v12.2.9, introduced the PG hard-limit
patches which were found to cause an issue in certain upgrade scenarios, and
this release was expedited to revert those patches. If you already successfully
upgraded to v12.2.9, you should **not** upgrade to v12.2.10, but rather **wait**
for a release in which http://tracker.ceph.com/issues/36686 is addressed. All
other users are encouraged to upgrade to this release.

Notable Changes
---------------

OSD

* This release reverts the PG hard-limit patches added in v12.2.9.

Changelog
---------

* ceph-volume: add some choose_disk capabilities (`issue#36446 <http://tracker.ceph.com/issues/36446>`_, `pr#24783 <https://github.com/ceph/ceph/pull/24783>`_, Erwan Velu)
* ceph-volume: remove version reporting from help menu (`issue#36386 <http://tracker.ceph.com/issues/36386>`_, `pr#24754 <https://github.com/ceph/ceph/pull/24754>`_, Alfredo Deza)
* ceph-volume: systemd import main so console_scripts work for executable (`issue#36648 <http://tracker.ceph.com/issues/36648>`_, `pr#24853 <https://github.com/ceph/ceph/pull/24853>`_, Alfredo Deza)
* ceph-volume: tests install ceph-ansible's requirements.txt dependencies (`issue#36672 <http://tracker.ceph.com/issues/36672>`_, `pr#24960 <https://github.com/ceph/ceph/pull/24960>`_, Alfredo Deza)
* ceph-volume: util.encryption don't push stderr to terminal (`issue#36246 <http://tracker.ceph.com/issues/36246>`_, `pr#24827 <https://github.com/ceph/ceph/pull/24827>`_, Alfredo Deza)
* ceph-volume: util.encryption robust blkid+lsblk detection of lockbox (`pr#24981 <https://github.com/ceph/ceph/pull/24981>`_, Alfredo Deza)
* ceph-volume: use console_scripts (`issue#36601 <http://tracker.ceph.com/issues/36601>`_, `pr#24837 <https://github.com/ceph/ceph/pull/24837>`_, Mehdi Abaakouk)
* OSDMapMapping does not handle active.size() > pool size (`issue#26866 <http://tracker.ceph.com/issues/26866>`_, `issue#35935 <http://tracker.ceph.com/issues/35935>`_, `pr#24432 <https://github.com/ceph/ceph/pull/24432>`_, Sage Weil)
* PG: add custom_reaction Backfilled and release reservations (`issue#24333 <http://tracker.ceph.com/issues/24333>`_, `pr#23493 <https://github.com/ceph/ceph/pull/23493>`_, Neha Ojha)
* Revert "PG: add custom_reaction Backfilled and release reservations after backfill (`pr#24902 <https://github.com/ceph/ceph/pull/24902>`_, Neha Ojha)
* Revert pg log limit changes (`issue#36686 <http://tracker.ceph.com/issues/36686>`_, `pr#24903 <https://github.com/ceph/ceph/pull/24903>`_, Neha Ojha)
* backport and other test fixes for osd-scrub-repair.sh (`issue#35845 <http://tracker.ceph.com/issues/35845>`_, `issue#36393 <http://tracker.ceph.com/issues/36393>`_, `pr#24532 <https://github.com/ceph/ceph/pull/24532>`_, Xinying Song, David Zafman)
* ceph-volume tests.systemd update imports for systemd module (`issue#36704 <http://tracker.ceph.com/issues/36704>`_, `pr#24958 <https://github.com/ceph/ceph/pull/24958>`_, Alfredo Deza)
* ceph-volume: adds a --prepare flag to `lvm batch` (`issue#36363 <http://tracker.ceph.com/issues/36363>`_, `pr#24759 <https://github.com/ceph/ceph/pull/24759>`_, Andrew Schoen)
* cls/user: cls_user_remove_bucket writes modified header (`issue#36534 <http://tracker.ceph.com/issues/36534>`_, `issue#36496 <http://tracker.ceph.com/issues/36496>`_, `pr#24855 <https://github.com/ceph/ceph/pull/24855>`_, Casey Bodley)
* core: by pass cache if performing deep scrub (`issue#35067 <http://tracker.ceph.com/issues/35067>`_, `pr#24802 <https://github.com/ceph/ceph/pull/24802>`_, Xiaoguang Wang)
* crush/CrushWrapper: fix crush tree json dumper (`issue#36149 <http://tracker.ceph.com/issues/36149>`_, `pr#24482 <https://github.com/ceph/ceph/pull/24482>`_, Oshyn Song)
* ec: src/common/interval_map.h: 161: FAILED assert(len > 0) (`issue#21931 <http://tracker.ceph.com/issues/21931>`_, `issue#22330 <http://tracker.ceph.com/issues/22330>`_, `pr#24582 <https://github.com/ceph/ceph/pull/24582>`_, Neha Ojha)
* gperftools-libs-2.6.1-1 or newer required for binaries linked against corresponding version at build time (`issue#36552 <http://tracker.ceph.com/issues/36552>`_, `issue#23657 <http://tracker.ceph.com/issues/23657>`_, `issue#36558 <http://tracker.ceph.com/issues/36558>`_, `issue#36508 <http://tracker.ceph.com/issues/36508>`_, `pr#24706 <https://github.com/ceph/ceph/pull/24706>`_, Brad Hubbard)
* osd: add creating to pg_string_state (`issue#36174 <http://tracker.ceph.com/issues/36174>`_, `issue#36297 <http://tracker.ceph.com/issues/36297>`_, `pr#24602 <https://github.com/ceph/ceph/pull/24602>`_, Dan van der Ster)
* osd: cast 'whoami' to unsigned so it can be used as the seed for RNG (`issue#26890 <http://tracker.ceph.com/issues/26890>`_, `pr#24659 <https://github.com/ceph/ceph/pull/24659>`_, Kefu Chai)
* osd: get loadavg per cpu for scrub load threshold check (`pr#24593 <https://github.com/ceph/ceph/pull/24593>`_, kungf)
* osdc/Objecter: possible race condition with connection reset (`issue#36183 <http://tracker.ceph.com/issues/36183>`_, `issue#36295 <http://tracker.ceph.com/issues/36295>`_, `pr#24574 <https://github.com/ceph/ceph/pull/24574>`_, Jason Dillaman)
* qa: add test that builds example librados programs (`issue#36229 <http://tracker.ceph.com/issues/36229>`_, `issue#15100 <http://tracker.ceph.com/issues/15100>`_, `pr#24538 <https://github.com/ceph/ceph/pull/24538>`_, Nathan Cutler)
* qa/ceph-ansible: Specify stable-3.2 branch (`issue#37331 <https://tracker.ceph.com/issues/37331>`_, `pr#25170 <https://github.com/ceph/ceph/pull/25170>`_, Brad Hubbard)
* rgw/beast: drop privileges after binding ports (`issue#36041 <http://tracker.ceph.com/issues/36041>`_, `pr#24454 <https://github.com/ceph/ceph/pull/24454>`_, Paul Emmerich)
* rgw: RGWAsyncGetBucketInstanceInfo does not access coroutine memory (`issue#35812 <http://tracker.ceph.com/issues/35812>`_, `issue#36212 <http://tracker.ceph.com/issues/36212>`_, `pr#24507 <https://github.com/ceph/ceph/pull/24507>`_, Casey Bodley)
* rgw: fix leak of curl handle on shutdown (`issue#35715 <http://tracker.ceph.com/issues/35715>`_, `issue#36214 <http://tracker.ceph.com/issues/36214>`_, `pr#24519 <https://github.com/ceph/ceph/pull/24519>`_, Casey Bodley)
* rgw: list bucket can not show the object uploaded by RGWPostObj when enable bucket versioning (`pr#24570 <https://github.com/ceph/ceph/pull/24570>`_, yuliyang)
* rgw: multisite: enforce spawn_window for data full sync (`issue#26897 <http://tracker.ceph.com/issues/26897>`_, `pr#24857 <https://github.com/ceph/ceph/pull/24857>`_, Casey Bodley)
* rgw: set default objecter_inflight_ops = 24576 (`issue#36570 <http://tracker.ceph.com/issues/36570>`_, `issue#25109 <http://tracker.ceph.com/issues/25109>`_, `pr#24862 <https://github.com/ceph/ceph/pull/24862>`_, Matt Benjamin)
* rgw: user stats account for resharded buckets (`pr#24854 <https://github.com/ceph/ceph/pull/24854>`_, Casey Bodley)
* segv in BlueStore::OldExtent::create (`issue#36526 <http://tracker.ceph.com/issues/36526>`_, `issue#36591 <http://tracker.ceph.com/issues/36591>`_, `pr#24746 <https://github.com/ceph/ceph/pull/24746>`_, Sage Weil)
* test/common: unittest_mclock_priority_queue builds with "make" command (`pr#24808 <https://github.com/ceph/ceph/pull/24808>`_, J. Eric Ivancich)


v12.2.9 Luminous
================

This is the ninth bug fix release of the Luminous v12.2.x long term stable
release series. Although this release contains several bugfixes across all the
components, it also introduced the PG hard-limit patches which could cause
problems during upgrade when not all PGs were active+clean. Therefore, users
should not install this release. Instead, they should skip it and upgrade to
12.2.10 directly.

Notable Changes
---------------

OSD

* 12.2.9 contains the pg hard hard limit patches (https://tracker.ceph.com/issues/23979).
  A partial upgrade during recovery/backfill, can cause the osds on the
  previous version, to fail with assert(trim_to <= info.last_complete). The
  workaround for users is to upgrade and restart all OSDs to a version with the
  pg hard limit, or only upgrade when all PGs are active+clean. This patch will
  be reverted in 12.2.10, until a clean upgrade path is added to the pg log hard
  limit patches.

  See also: http://tracker.ceph.com/issues/36686

* The bluestore_cache_* options are no longer needed. They are replaced
  by osd_memory_target, defaulting to 4GB. BlueStore will expand
  and contract its cache to attempt to stay within this
  limit. Users upgrading should note this is a higher default
  than the previous bluestore_cache_size of 1GB, so OSDs using
  BlueStore will use more memory by default.

  For more details, see `BlueStore docs <http://docs.ceph.com/docs/master/rados/configuration/bluestore-config-ref/#cache-size>_`

Changelog
---------

* build/ops: add e2fsprogs runtime dependency (`pr#24663 <https://github.com/ceph/ceph/pull/24663>`_, Guillaume Abrioux, Alfredo Deza)
* build/ops: deb: fix ceph-mgr .pyc files left behind (`issue#26883 <http://tracker.ceph.com/issues/26883>`_, `pr#23832 <https://github.com/ceph/ceph/pull/23832>`_, Dan Mick)
* build/ops: deb: require fuse for ceph-fuse (`issue#21057 <http://tracker.ceph.com/issues/21057>`_, `pr#23693 <https://github.com/ceph/ceph/pull/23693>`_, Thomas Serlin)
* build/ops: rpm: selinux-policy fixes (`pr#24136 <https://github.com/ceph/ceph/pull/24136>`_, Brad Hubbard)
* build/ops: rpm: use updated gperftools (`issue#35969 <http://tracker.ceph.com/issues/35969>`_, `pr#24259 <https://github.com/ceph/ceph/pull/24259>`_, Kefu Chai)
* ceph-volume:  activate option --auto-detect-objectstore respects --no-systemd (`issue#36249 <http://tracker.ceph.com/issues/36249>`_, `pr#24358 <https://github.com/ceph/ceph/pull/24358>`_, Alfredo Deza)
* ceph-volume:  lsblk can fail to find PARTLABEL, must fallback to blkid (`issue#36098 <http://tracker.ceph.com/issues/36098>`_, `pr#24335 <https://github.com/ceph/ceph/pull/24335>`_, Alfredo Deza)
* ceph-volume: add new ceph-handlers role from ceph-ansible (`issue#36251 <http://tracker.ceph.com/issues/36251>`_, `pr#24338 <https://github.com/ceph/ceph/pull/24338>`_, Alfredo Deza)
* ceph-volume: batch carve out lvs for bluestore (`issue#34535 <http://tracker.ceph.com/issues/34535>`_, `pr#24075 <https://github.com/ceph/ceph/pull/24075>`_, Alfredo Deza)
* ceph-volume: batch tests for mixed-type of devices (`issue#35535 <http://tracker.ceph.com/issues/35535>`_, `issue#27210 <http://tracker.ceph.com/issues/27210>`_, `pr#23967 <https://github.com/ceph/ceph/pull/23967>`_, Alfredo Deza)
* ceph-volume: batch: allow --osds-per-device, default it to 1 (`issue#35913 <http://tracker.ceph.com/issues/35913>`_, `pr#24080 <https://github.com/ceph/ceph/pull/24080>`_, Alfredo Deza)
* ceph-volume: batch: allow journal+block.db sizing on the CLI (`issue#36088 <http://tracker.ceph.com/issues/36088>`_, `pr#24209 <https://github.com/ceph/ceph/pull/24209>`_, Alfredo Deza)
* ceph-volume: custom cluster names fail on filestore trigger (`issue#27210 <http://tracker.ceph.com/issues/27210>`_, `pr#24280 <https://github.com/ceph/ceph/pull/24280>`_, Alfredo Deza)
* ceph-volume: do not send (lvm) stderr/stdout to the terminal, use the logfile (`issue#36492 <http://tracker.ceph.com/issues/36492>`_, `pr#24741 <https://github.com/ceph/ceph/pull/24741>`_, Alfredo Deza)
* ceph-volume: earlier detection for --journal and --filestore flag requirements (`issue#24794 <http://tracker.ceph.com/issues/24794>`_, `pr#24206 <https://github.com/ceph/ceph/pull/24206>`_, Alfredo Deza)
* ceph-volume: fix journal and filestore data size in `lvm batch --report` (`issue#36242 <http://tracker.ceph.com/issues/36242>`_, `pr#24307 <https://github.com/ceph/ceph/pull/24307>`_, Andrew Schoen)
* ceph-volume: fix zap not working with LVs (`issue#35970 <http://tracker.ceph.com/issues/35970>`_, `pr#24082 <https://github.com/ceph/ceph/pull/24082>`_, Alfredo Deza)
* ceph-volume: lvm.prepare update help to indicate partitions are needed, not devices (`issue#24795 <http://tracker.ceph.com/issues/24795>`_, `pr#24451 <https://github.com/ceph/ceph/pull/24451>`_, Jeffrey Zhang, Alfredo Deza)
* ceph-volume: make `lvm batch` idempotent (`pr#24589 <https://github.com/ceph/ceph/pull/24589>`_, Andrew Schoen)
* ceph-volume: remove version reporting from help menu (`issue#36386 <http://tracker.ceph.com/issues/36386>`_, `pr#24754 <https://github.com/ceph/ceph/pull/24754>`_, Alfredo Deza)
* ceph-volume: skip processing devices that don't exist when scanning system disks (`issue#36247 <http://tracker.ceph.com/issues/36247>`_, `pr#24382 <https://github.com/ceph/ceph/pull/24382>`_, Alfredo Deza)
* cephfs: MDSMonitor: consider raising priority of MMDSBeacons from MDS so they are processed before other client messages (`issue#26899 <http://tracker.ceph.com/issues/26899>`_, `pr#23554 <https://github.com/ceph/ceph/pull/23554>`_, Patrick Donnelly)
* cephfs: MDSMonitor: lookup of gid in prepare_beacon that has been removed will cause exception (`issue#35848 <http://tracker.ceph.com/issues/35848>`_, `pr#23990 <https://github.com/ceph/ceph/pull/23990>`_, Patrick Donnelly)
* cephfs: ceph-fuse: add SELinux policy (`issue#36103 <http://tracker.ceph.com/issues/36103>`_, `pr#24313 <https://github.com/ceph/ceph/pull/24313>`_, Patrick Donnelly)
* cephfs: ceph_volume_client: allow atomic update of RADOS objects (`issue#24173 <http://tracker.ceph.com/issues/24173>`_, `pr#24084 <https://github.com/ceph/ceph/pull/24084>`_, Rishabh Dave)
* cephfs: ceph_volume_client: delay required after adding data pool to MDSMap (`issue#25141 <http://tracker.ceph.com/issues/25141>`_, `pr#23726 <https://github.com/ceph/ceph/pull/23726>`_, Patrick Donnelly)
* cephfs: ceph_volume_client: py3 compatible (`issue#17230 <http://tracker.ceph.com/issues/17230>`_, `pr#24083 <https://github.com/ceph/ceph/pull/24083>`_, Rishabh Dave, Patrick Donnelly)
* cephfs: cephfs-data-scan: print the max used ino (`issue#26925 <http://tracker.ceph.com/issues/26925>`_, `pr#23881 <https://github.com/ceph/ceph/pull/23881>`_, "Yan, Zheng")
* cephfs: cephfs-journal-tool: wrong layout info used (`issue#24644 <http://tracker.ceph.com/issues/24644>`_, `pr#24033 <https://github.com/ceph/ceph/pull/24033>`_, Gu Zhongyan)
* cephfs: client: check for unmounted condition before printing debug output (`issue#25213 <http://tracker.ceph.com/issues/25213>`_, `pr#23617 <https://github.com/ceph/ceph/pull/23617>`_, Jeff Layton)
* cephfs: client: drop null child dentries before try pruning inode's alias (`issue#22293 <http://tracker.ceph.com/issues/22293>`_, `pr#24119 <https://github.com/ceph/ceph/pull/24119>`_, "Yan, Zheng")
* cephfs: client: fix choose_target_mds for requests that do name lookup (`issue#26860 <http://tracker.ceph.com/issues/26860>`_, `pr#23793 <https://github.com/ceph/ceph/pull/23793>`_, "Yan, Zheng")
* cephfs: client: retry remount on dcache invalidation failure (`issue#27657 <http://tracker.ceph.com/issues/27657>`_, `pr#24303 <https://github.com/ceph/ceph/pull/24303>`_, Venky Shankar)
* cephfs: client: statfs inode count odd (`issue#24849 <http://tracker.ceph.com/issues/24849>`_, `pr#24376 <https://github.com/ceph/ceph/pull/24376>`_, Rishabh Dave)
* cephfs: client: two ceph-fuse clients, one can not list out files created by another (`issue#27051 <http://tracker.ceph.com/issues/27051>`_, `pr#24282 <https://github.com/ceph/ceph/pull/24282>`_, Peng Xie)
* cephfs: client: update ctime when modifying file content (`issue#35945 <http://tracker.ceph.com/issues/35945>`_, `pr#24323 <https://github.com/ceph/ceph/pull/24323>`_, "Yan, Zheng")
* common: get real hostname from container/pod environment (`pr#23915 <https://github.com/ceph/ceph/pull/23915>`_, Sage Weil)
* core: PGPool::update optimizations (`pr#23969 <https://github.com/ceph/ceph/pull/23969>`_, Zac Medico)
* core: ceph-disk: compatibility fix for python 3 (`issue#35906 <http://tracker.ceph.com/issues/35906>`_, `pr#24347 <https://github.com/ceph/ceph/pull/24347>`_, Tim Serong)
* core: discover_all_missing() not always called during activating (`issue#22837 <http://tracker.ceph.com/issues/22837>`_, `pr#23817 <https://github.com/ceph/ceph/pull/23817>`_, Sage Weil, David Zafman)
* core: kv/KeyValueDB: return const char\* from MergeOperator::name() (`issue#26875 <http://tracker.ceph.com/issues/26875>`_, `pr#23566 <https://github.com/ceph/ceph/pull/23566>`_, Sage Weil)
* core: librados application's symbol could conflict with the libceph-common (`issue#25154 <http://tracker.ceph.com/issues/25154>`_, `pr#23483 <https://github.com/ceph/ceph/pull/23483>`_, Kefu Chai)
* core: mgr/MgrClient: guard send_pgstats() with lock (`issue#23370 <http://tracker.ceph.com/issues/23370>`_, `pr#23791 <https://github.com/ceph/ceph/pull/23791>`_, Kefu Chai)
* core: mgr/balancer: deepcopy best plan - otherwise we get latest (`issue#27000 <http://tracker.ceph.com/issues/27000>`_, `pr#23740 <https://github.com/ceph/ceph/pull/23740>`_, Stefan Priebe)
* core: mgrc: enable disabling stats via mgr_stats_threshold (`issue#25197 <http://tracker.ceph.com/issues/25197>`_, `pr#23461 <https://github.com/ceph/ceph/pull/23461>`_, John Spray)
* core: mon/OSDMonitor: invalidate max_failed_since on cancel_report (`issue#35860 <http://tracker.ceph.com/issues/35860>`_, `pr#24257 <https://github.com/ceph/ceph/pull/24257>`_, xie xingguo)
* core: object errors found in be_select_auth_object() aren't logged the same (`issue#25108 <http://tracker.ceph.com/issues/25108>`_, `pr#23871 <https://github.com/ceph/ceph/pull/23871>`_, David Zafman)
* core: os/bluestore: bluestore_buffer_hit_bytes perf counter doesn't reset (`pr#23773 <https://github.com/ceph/ceph/pull/23773>`_, Igor Fedotov)
* core: os/bluestore: cache autotuning and memory limit (`pr#24065 <https://github.com/ceph/ceph/pull/24065>`_, Mark Nelson)
* core: osd,mon: increase mon_max_pg_per_osd to 250 (`issue#25112 <http://tracker.ceph.com/issues/25112>`_, `pr#23862 <https://github.com/ceph/ceph/pull/23862>`_, Neha Ojha)
* core: osd/PG: avoid choose_acting picking want with > pool size items (`issue#35924 <http://tracker.ceph.com/issues/35924>`_, `pr#24299 <https://github.com/ceph/ceph/pull/24299>`_, Sage Weil)
* core: osdc/Objecter: fix split vs reconnect race (`issue#22544 <http://tracker.ceph.com/issues/22544>`_, `pr#24188 <https://github.com/ceph/ceph/pull/24188>`_, Sage Weil)
* core: rados python bindings use prval from stack (`issue#25175 <http://tracker.ceph.com/issues/25175>`_, `pr#23864 <https://github.com/ceph/ceph/pull/23864>`_, Sage Weil)
* doc: Fix broken urls (`issue#25185 <http://tracker.ceph.com/issues/25185>`_, `pr#23621 <https://github.com/ceph/ceph/pull/23621>`_, Jos Collin)
* doc: remove deprecated 'scrubq' from ceph(8) (`issue#35813 <http://tracker.ceph.com/issues/35813>`_, `pr#24211 <https://github.com/ceph/ceph/pull/24211>`_, Ruben Kerkhof)
* doc: rgw: ldap-auth: fixed option name 'rgw_ldap_searchfilter' (`issue#23081 <http://tracker.ceph.com/issues/23081>`_, `pr#23761 <https://github.com/ceph/ceph/pull/23761>`_, Konstantin Shalygin)
* mds: MDBalancer::try_rebalance() may stop prematurely (`issue#26973 <http://tracker.ceph.com/issues/26973>`_, `pr#23884 <https://github.com/ceph/ceph/pull/23884>`_, "Yan, Zheng")
* mds: allows client to create .. and . dirents (`issue#25113 <http://tracker.ceph.com/issues/25113>`_, `pr#24329 <https://github.com/ceph/ceph/pull/24329>`_, Venky Shankar)
* mds: avoid using g_conf->get_val<...>(...) in hot path (`issue#24820 <http://tracker.ceph.com/issues/24820>`_, `pr#23408 <https://github.com/ceph/ceph/pull/23408>`_, "Yan, Zheng")
* mds: calculate load by checking self CPU usage (`issue#26834 <http://tracker.ceph.com/issues/26834>`_, `pr#23505 <https://github.com/ceph/ceph/pull/23505>`_, "Yan, Zheng")
* mds: configurable timeout for client eviction (`issue#25188 <http://tracker.ceph.com/issues/25188>`_, `pr#24086 <https://github.com/ceph/ceph/pull/24086>`_, Patrick Donnelly, Venky Shankar)
* mds: crash when dumping ops in flight (`issue#26894 <http://tracker.ceph.com/issues/26894>`_, `pr#23677 <https://github.com/ceph/ceph/pull/23677>`_, "Yan, Zheng")
* mds: curate priority of perf counters sent to mgr (`issue#22097 <http://tracker.ceph.com/issues/22097>`_, `issue#24004 <http://tracker.ceph.com/issues/24004>`_, `pr#24089 <https://github.com/ceph/ceph/pull/24089>`_, Guan yunfei, Venky Shankar)
* mds: explain delayed client_request due to subtree migration (`issue#24840 <http://tracker.ceph.com/issues/24840>`_, `pr#23678 <https://github.com/ceph/ceph/pull/23678>`_, Yan, Zheng, "Yan, Zheng")
* mds: health warning for slow metadata IO (`issue#24879 <http://tracker.ceph.com/issues/24879>`_, `pr#24171 <https://github.com/ceph/ceph/pull/24171>`_, "Yan, Zheng")
* mds: internal op missing events time 'throttled', 'all_read', 'dispatched' (`issue#36114 <http://tracker.ceph.com/issues/36114>`_, `pr#24410 <https://github.com/ceph/ceph/pull/24410>`_, Yanhu Cao)
* mds: mds got laggy because of MDSBeacon stuck in mqueue (`issue#23519 <http://tracker.ceph.com/issues/23519>`_, `pr#23556 <https://github.com/ceph/ceph/pull/23556>`_, "Yan, Zheng")
* mds: optimize the way how max export size is enforced (`issue#25131 <http://tracker.ceph.com/issues/25131>`_, `pr#23789 <https://github.com/ceph/ceph/pull/23789>`_, "Yan, Zheng")
* mds: prevent MDSRank::evict_client from blocking finisher thread (`issue#35720 <http://tracker.ceph.com/issues/35720>`_, `pr#23946 <https://github.com/ceph/ceph/pull/23946>`_, "Yan, Zheng")
* mds: print is_laggy message once (`issue#35250 <http://tracker.ceph.com/issues/35250>`_, `pr#24138 <https://github.com/ceph/ceph/pull/24138>`_, Patrick Donnelly)
* mds: rctime may go back (`issue#35916 <http://tracker.ceph.com/issues/35916>`_, `pr#24378 <https://github.com/ceph/ceph/pull/24378>`_, "Yan, Zheng")
* mds: reset heartbeat map at potential time-consuming places (`issue#26858 <http://tracker.ceph.com/issues/26858>`_, `pr#23507 <https://github.com/ceph/ceph/pull/23507>`_, Yan, Zheng, "Yan, Zheng")
* mds: runs out of file descriptors after several respawns (`issue#35850 <http://tracker.ceph.com/issues/35850>`_, `pr#24310 <https://github.com/ceph/ceph/pull/24310>`_, Patrick Donnelly)
* mds: track average session uptime (`issue#25013 <http://tracker.ceph.com/issues/25013>`_, `pr#24421 <https://github.com/ceph/ceph/pull/24421>`_, Patrick Donnelly, Venky Shankar)
* mds: use monotonic clock for beacon message timekeeping (`issue#26959 <http://tracker.ceph.com/issues/26959>`_, `pr#24311 <https://github.com/ceph/ceph/pull/24311>`_, Patrick Donnelly)
* mgr: Sync the prometheus module (`pr#23216 <https://github.com/ceph/ceph/pull/23216>`_, Boris Ranto)
* mon: Automatically set expected_num_objects for new pools with >=100 PGs per OSD (`issue#24687 <http://tracker.ceph.com/issues/24687>`_, `pr#24395 <https://github.com/ceph/ceph/pull/24395>`_, Douglas Fuller)
* msg: "challenging authorizer" messages appear at debug_ms=0 (`issue#35251 <http://tracker.ceph.com/issues/35251>`_, `pr#23943 <https://github.com/ceph/ceph/pull/23943>`_, Patrick Donnelly)
* msg: async: clean up local buffers on dispatch (`issue#35987 <http://tracker.ceph.com/issues/35987>`_, `pr#24387 <https://github.com/ceph/ceph/pull/24387>`_, Greg Farnum)
* msg: ceph_abort() when there are enough accepter errors in msg server (`issue#23649 <http://tracker.ceph.com/issues/23649>`_, `pr#24419 <https://github.com/ceph/ceph/pull/24419>`_, penglaiyxy@gmail.com)
* osd: EC: slow/hung ops in multimds suite test (`issue#23769 <http://tracker.ceph.com/issues/23769>`_, `pr#24393 <https://github.com/ceph/ceph/pull/24393>`_, Sage Weil)
* osd: ECBackend: don't get result code of subchunk-read overwritten (`issue#21769 <http://tracker.ceph.com/issues/21769>`_, `pr#24342 <https://github.com/ceph/ceph/pull/24342>`_, songweibin)
* osd: Limit pg log length during recovery/backfill so that we don't run out of memory (`issue#21416 <http://tracker.ceph.com/issues/21416>`_, `pr#23211 <https://github.com/ceph/ceph/pull/23211>`_, Neha Ojha, xie xingguo)
* osd: OSDMap: fix apply upmap segfault (`issue#22056 <http://tracker.ceph.com/issues/22056>`_, `pr#23579 <https://github.com/ceph/ceph/pull/23579>`_, Brad Hubbard)
* osd: PG: add custom_reaction Backfilled and release reservations after bac… (`issue#23614 <http://tracker.ceph.com/issues/23614>`_, `pr#23493 <https://github.com/ceph/ceph/pull/23493>`_, Neha Ojha)
* osd: PrimaryLogPG: fix potential pg-log overtrimming (`pr#24308 <https://github.com/ceph/ceph/pull/24308>`_, xie xingguo)
* osd: backport 'bench' and stdout changes (`issue#24022 <http://tracker.ceph.com/issues/24022>`_, `pr#23680 <https://github.com/ceph/ceph/pull/23680>`_, Коренберг Маркr, John Spray, Kefu Chai)
* osd: read object attrs failed at EC recovery (`issue#24406 <http://tracker.ceph.com/issues/24406>`_, `pr#24327 <https://github.com/ceph/ceph/pull/24327>`_, xiaofei cui)
* osd: scrub livelock (`issue#26890 <http://tracker.ceph.com/issues/26890>`_, `pr#24396 <https://github.com/ceph/ceph/pull/24396>`_, Sage Weil)
* qa/suites/rados/upgrade/jewel-x-singleton: exclude python3-rados, python3-cephfs (`pr#24479 <https://github.com/ceph/ceph/pull/24479>`_, Neha Ojha)
* rbd: [rbd-mirror] failed assertion when updating mirror status (`issue#36084 <http://tracker.ceph.com/issues/36084>`_, `pr#24320 <https://github.com/ceph/ceph/pull/24320>`_, Jason Dillaman)
* rbd: fix error import when the input is a pipe (`issue#34536 <http://tracker.ceph.com/issues/34536>`_, `pr#24003 <https://github.com/ceph/ceph/pull/24003>`_, songweibin)
* rbd: librbd: blacklisted client might not notice it lost the lock (`issue#34534 <http://tracker.ceph.com/issues/34534>`_, `pr#24405 <https://github.com/ceph/ceph/pull/24405>`_, Song Shun, Mykola Golub, Jason Dillaman)
* rbd: librbd: discard should wait for in-flight cache writeback to complete (`issue#23548 <http://tracker.ceph.com/issues/23548>`_, `pr#23594 <https://github.com/ceph/ceph/pull/23594>`_, Jason Dillaman)
* rbd: librbd: ensure exclusive lock acquired when removing sync point snaps… (`issue#24898 <http://tracker.ceph.com/issues/24898>`_, `pr#24123 <https://github.com/ceph/ceph/pull/24123>`_, Mykola Golub, Jason Dillaman)
* rbd: librbd: fix refuse to release lock when cookie is the same at rewatch (`issue#27986 <http://tracker.ceph.com/issues/27986>`_, `pr#23758 <https://github.com/ceph/ceph/pull/23758>`_, Song Shun)
* rbd: librbd: fixed assert when flattening clone with zero overlap (`issue#35702 <http://tracker.ceph.com/issues/35702>`_, `pr#24285 <https://github.com/ceph/ceph/pull/24285>`_, Jason Dillaman)
* rbd: librbd: image create request should validate data pool for self-managed snapshot support (`issue#24675 <http://tracker.ceph.com/issues/24675>`_, `pr#24390 <https://github.com/ceph/ceph/pull/24390>`_, Mykola Golub)
* rbd: librbd: journaling unable request can not be sent to remote lock owner (`issue#26939 <http://tracker.ceph.com/issues/26939>`_, `pr#24100 <https://github.com/ceph/ceph/pull/24100>`_, Mykola Golub)
* rbd: librbd: object map improperly flagged as invalidated (`issue#24516 <http://tracker.ceph.com/issues/24516>`_, `pr#24415 <https://github.com/ceph/ceph/pull/24415>`_, Jason Dillaman)
* rbd: librbd: potential race on image create request complete (`issue#24910 <http://tracker.ceph.com/issues/24910>`_, `pr#23892 <https://github.com/ceph/ceph/pull/23892>`_, Mykola Golub)
* rgw: 'radosgw-admin sync error trim' only trims partially (`issue#24873 <http://tracker.ceph.com/issues/24873>`_, `pr#24054 <https://github.com/ceph/ceph/pull/24054>`_, Casey Bodley)
* rgw: Fix log level of gc_iterate_entries (`issue#23801 <http://tracker.ceph.com/issues/23801>`_, `pr#23665 <https://github.com/ceph/ceph/pull/23665>`_, iliul)
* rgw: Limit the number of lifecycle rules on one bucket (`issue#24572 <http://tracker.ceph.com/issues/24572>`_, `pr#23522 <https://github.com/ceph/ceph/pull/23522>`_, Zhang Shaowen)
* rgw: The delete markers generated by object expiration should have owner (`issue#24568 <http://tracker.ceph.com/issues/24568>`_, `pr#23545 <https://github.com/ceph/ceph/pull/23545>`_, Zhang Shaowen)
* rgw: abort_bucket_multiparts() ignores individual NoSuchUpload errors (`issue#35986 <http://tracker.ceph.com/issues/35986>`_, `pr#24389 <https://github.com/ceph/ceph/pull/24389>`_, Casey Bodley)
* rgw: change default rgw_thread_pool_size to 512 (`issue#25214 <http://tracker.ceph.com/issues/25214>`_, `issue#24544 <http://tracker.ceph.com/issues/24544>`_, `pr#24034 <https://github.com/ceph/ceph/pull/24034>`_, Douglas Fuller, Casey Bodley)
* rgw: cls/rgw: don't assert in decode_list_index_key() (`issue#24117 <http://tracker.ceph.com/issues/24117>`_, `pr#24391 <https://github.com/ceph/ceph/pull/24391>`_, Yehuda Sadeh)
* rgw: cls/rgw: ready rgw_usage_log_entry for extraction via ceph-dencoder (`issue#34537 <http://tracker.ceph.com/issues/34537>`_, `pr#23974 <https://github.com/ceph/ceph/pull/23974>`_, Vaibhav Bhembre)
* rgw: fix chunked-encoding for chunks >1MiB (`issue#35990 <http://tracker.ceph.com/issues/35990>`_, `pr#24361 <https://github.com/ceph/ceph/pull/24361>`_, Robin H. Johnson)
* rgw: fix deadlock on RGWIndexCompletionManager::stop (`issue#26949 <http://tracker.ceph.com/issues/26949>`_, `pr#24069 <https://github.com/ceph/ceph/pull/24069>`_, Yao Zongyou)
* rgw: incremental data sync uses truncated flag to detect end of listing (`issue#26952 <http://tracker.ceph.com/issues/26952>`_, `pr#24242 <https://github.com/ceph/ceph/pull/24242>`_, Casey Bodley)
* rgw: multisite: data sync error repo processing does not back off on empty (`issue#26938 <http://tracker.ceph.com/issues/26938>`_, `pr#24318 <https://github.com/ceph/ceph/pull/24318>`_, Casey Bodley)
* rgw: multisite: intermittent failures in test_bucket_sync_disable_enable (`issue#26895 <http://tracker.ceph.com/issues/26895>`_, `pr#24316 <https://github.com/ceph/ceph/pull/24316>`_, Casey Bodley)
* rgw: multisite: intermittent test_bucket_index_log_trim failures (`issue#36034 <http://tracker.ceph.com/issues/36034>`_, `pr#24398 <https://github.com/ceph/ceph/pull/24398>`_, Casey Bodley)
* rgw: multisite: object metadata operations are skipped by sync (`issue#24367 <http://tracker.ceph.com/issues/24367>`_, `pr#24056 <https://github.com/ceph/ceph/pull/24056>`_, Casey Bodley)
* rgw: multisite: object name should be urlencoded when we put it into ES (`issue#23216 <http://tracker.ceph.com/issues/23216>`_, `pr#24424 <https://github.com/ceph/ceph/pull/24424>`_, Chang Liu)
* rgw: multisite: out of order updates to sync status markers (`issue#35539 <http://tracker.ceph.com/issues/35539>`_, `pr#24317 <https://github.com/ceph/ceph/pull/24317>`_, Yehuda Sadeh)
* rgw: multisite: segfault on shutdown/realm reload (`issue#35543 <http://tracker.ceph.com/issues/35543>`_, `pr#24231 <https://github.com/ceph/ceph/pull/24231>`_, Casey Bodley)
* rgw: multisite: update index segfault on shutdown/realm reload (`issue#35905 <http://tracker.ceph.com/issues/35905>`_, `pr#24397 <https://github.com/ceph/ceph/pull/24397>`_, Tianshan Qu)
* rgw: raise debug level on redundant data sync error messages (`issue#35830 <http://tracker.ceph.com/issues/35830>`_, `issue#36037 <http://tracker.ceph.com/issues/36037>`_, `pr#24135 <https://github.com/ceph/ceph/pull/24135>`_, Casey Bodley, Matt Benjamin)
* rgw: raise default rgw_curl_low_speed_time to 300 seconds (`issue#27989 <http://tracker.ceph.com/issues/27989>`_, `pr#24046 <https://github.com/ceph/ceph/pull/24046>`_, Casey Bodley)
* rgw: resharding produces invalid values of bucket stats (`issue#36290 <http://tracker.ceph.com/issues/36290>`_, `pr#24527 <https://github.com/ceph/ceph/pull/24527>`_, Abhishek Lekshmanan)
* rgw: return x-amz-version-id: null when delete obj in versioning suspended bucket (`issue#35814 <http://tracker.ceph.com/issues/35814>`_, `pr#24190 <https://github.com/ceph/ceph/pull/24190>`_, yuliyang)
* rgw: rgw_file: deep stat handling (`issue#24915 <http://tracker.ceph.com/issues/24915>`_, `pr#23499 <https://github.com/ceph/ceph/pull/23499>`_, Matt Benjamin)
* tests: Excluded 'python34-cephfs' from the install tasks (`pr#24650 <https://github.com/ceph/ceph/pull/24650>`_, Yuri Weinstein)
* tests: Use pids instead of jobspecs which were wrong (`issue#27056 <http://tracker.ceph.com/issues/27056>`_, `pr#23901 <https://github.com/ceph/ceph/pull/23901>`_, David Zafman)
* tests: cephfs: multifs requires 4 mds but gets only 2 (`issue#24899 <http://tracker.ceph.com/issues/24899>`_, `pr#24328 <https://github.com/ceph/ceph/pull/24328>`_, Patrick Donnelly)
* tests: cls_rgw test is only run in rados suite: add it to rgw suite as well (`issue#24815 <http://tracker.ceph.com/issues/24815>`_, `pr#24070 <https://github.com/ceph/ceph/pull/24070>`_, Casey Bodley, Sage Weil)
* tests: librbd: not valid to have different parents between image snapshots (`issue#36097 <http://tracker.ceph.com/issues/36097>`_, `pr#24245 <https://github.com/ceph/ceph/pull/24245>`_, Jason Dillaman)
* tests: move mds/client config to qa from teuthology ceph.conf.template (`issue#26900 <http://tracker.ceph.com/issues/26900>`_, `issue#24839 <http://tracker.ceph.com/issues/24839>`_, `pr#23877 <https://github.com/ceph/ceph/pull/23877>`_, Patrick Donnelly)
* tests: qa/tasks: s3a fix mirror (`pr#24039 <https://github.com/ceph/ceph/pull/24039>`_, Vasu Kulkarni)
* tests: qa/workunits: replace 'realpath' with 'readlink -f' in fsstress.sh (`issue#27211 <http://tracker.ceph.com/issues/27211>`_, `issue#36409 <http://tracker.ceph.com/issues/36409>`_, `pr#24620 <https://github.com/ceph/ceph/pull/24620>`_, Ilya Dryomov, Jason Dillaman)
* tests: qa: add .qa helper link (`pr#24134 <https://github.com/ceph/ceph/pull/24134>`_, Patrick Donnelly)
* tests: qa: added v12.2.8 to the mix (`issue#35541 <http://tracker.ceph.com/issues/35541>`_, `pr#23913 <https://github.com/ceph/ceph/pull/23913>`_, Yuri Weinstein)
* tests: remove knfs qa suite from future releases (`issue#36075 <http://tracker.ceph.com/issues/36075>`_, `pr#24268 <https://github.com/ceph/ceph/pull/24268>`_, Yuri Weinstein)
* tools: ceph-objectstore-tool: Allow target level as first positional parameter (`issue#35846 <http://tracker.ceph.com/issues/35846>`_, `pr#24115 <https://github.com/ceph/ceph/pull/24115>`_, David Zafman)


v12.2.8 Luminous
================

This is the eighth bug fix release of the Luminous v12.2.x long term stable
release series. This release contains several bugfixes across all the components
and we recommend all users upgrade.

Upgrade Notes from previous luminous releases
---------------------------------------------

When upgrading from v12.2.5 or v12.2.6 please note that upgrade caveats from
12.2.5 will apply to any _newer_ luminous version including 12.2.8. Please read
the notes at luminous-12-2-5-upgrades_ .

For the cluster that installed the broken 12.2.6 release, 12.2.7 fixed the
regression and introduced a workaround option `osd distrust data digest = true`,
but 12.2.7 clusters still generated health warnings like ::

  [ERR] 11.288 shard 207: soid  11:1155c332:::rbd_data.207dce238e1f29.0000000000000527:head data_digest 0xc8997a5b != data_digest 0x2ca15853


12.2.8 improves the deep scrub code to automatically repair these
inconsistencies. Once the entire cluster has been upgraded and then fully deep
scrubbed, and all such inconsistencies are resolved, it will be safe to disable
the `osd distrust data digest = true` workaround option.

Notable Changes
---------------

- *OSD*

  * Scrub repair is enhanced to handle data digest mismatch info on
    replicas as long as all replicas' digests match each other.

- *RGW*

  * Options `rgw curl low speed limit` and `rgw curl low speed time`
    are added to control the lower speed limits and times below which
    the requests are considered too slow to be aborted and can help
    mitigate data sync getting blocked during network issues

  * Option `rgw s3 auth order` configurable added which takes a comma
    separated list of order to try for s3 authentication when external
    engines are involved.

Changelog
---------
* bluestore: set correctly shard for existed Collection (`issue#24761 <http://tracker.ceph.com/issues/24761>`_, `pr#22860 <https://github.com/ceph/ceph/pull/22860>`_, Jianpeng Ma)
* build/ops: Boost system library is no longer required to compile and link example librados program (`issue#25054 <http://tracker.ceph.com/issues/25054>`_, `pr#23202 <https://github.com/ceph/ceph/pull/23202>`_, Nathan Cutler)
* build/ops: Bring back diff -y for non-FreeBSD (`issue#24396 <http://tracker.ceph.com/issues/24396>`_, `issue#21664 <http://tracker.ceph.com/issues/21664>`_, `pr#22848 <https://github.com/ceph/ceph/pull/22848>`_, Sage Weil, David Zafman)
* build/ops: install-deps.sh fails on newest openSUSE Leap (`issue#25064 <http://tracker.ceph.com/issues/25064>`_, `pr#23179 <https://github.com/ceph/ceph/pull/23179>`_, Kyr Shatskyy)
* build/ops: Mimic build fails with -DWITH_RADOSGW=0 (`issue#24437 <http://tracker.ceph.com/issues/24437>`_, `pr#22864 <https://github.com/ceph/ceph/pull/22864>`_, Dan Mick)
* build/ops: order rbdmap.service before remote-fs-pre.target (`issue#24713 <http://tracker.ceph.com/issues/24713>`_, `pr#22844 <https://github.com/ceph/ceph/pull/22844>`_, Ilya Dryomov)
* build/ops: rpm: silence osd block chown (`issue#25152 <http://tracker.ceph.com/issues/25152>`_, `pr#23313 <https://github.com/ceph/ceph/pull/23313>`_, Dan van der Ster)
* cephfs-journal-tool: Fix purging when importing an zero-length journal (`issue#24239 <http://tracker.ceph.com/issues/24239>`_, `pr#22980 <https://github.com/ceph/ceph/pull/22980>`_, yupeng chen, zhongyan gu)
* cephfs: MDSMonitor: uncommitted state exposed to clients/mdss (`issue#23768 <http://tracker.ceph.com/issues/23768>`_, `pr#23013 <https://github.com/ceph/ceph/pull/23013>`_, Patrick Donnelly)
* ceph-fuse mount failed because no mds (`issue#22205 <http://tracker.ceph.com/issues/22205>`_, `pr#22895 <https://github.com/ceph/ceph/pull/22895>`_, liyan)
* ceph-volume add a __release__ string, to help version-conditional calls (`issue#25170 <http://tracker.ceph.com/issues/25170>`_, `pr#23331 <https://github.com/ceph/ceph/pull/23331>`_, Alfredo Deza)
* ceph-volume: adds test for `ceph-volume lvm list /dev/sda` (`issue#24784 <http://tracker.ceph.com/issues/24784>`_, `issue#24957 <http://tracker.ceph.com/issues/24957>`_, `pr#23350 <https://github.com/ceph/ceph/pull/23350>`_, Andrew Schoen)
* ceph-volume: do not use stdin in luminous (`issue#25173 <http://tracker.ceph.com/issues/25173>`_, `issue#23260 <http://tracker.ceph.com/issues/23260>`_, `pr#23367 <https://github.com/ceph/ceph/pull/23367>`_, Alfredo Deza)
* ceph-volume enable the ceph-osd during lvm activation (`issue#24152 <http://tracker.ceph.com/issues/24152>`_, `pr#23394 <https://github.com/ceph/ceph/pull/23394>`_, Dan van der Ster, Alfredo Deza)
* ceph-volume expand on the LVM API to create multiple LVs at different sizes (`issue#24020 <http://tracker.ceph.com/issues/24020>`_, `pr#23395 <https://github.com/ceph/ceph/pull/23395>`_, Alfredo Deza)
* ceph-volume lvm.activate conditional mon-config on prime-osd-dir (`issue#25216 <http://tracker.ceph.com/issues/25216>`_, `pr#23397 <https://github.com/ceph/ceph/pull/23397>`_, Alfredo Deza)
* ceph-volume lvm.batch remove non-existent sys_api property (`issue#34310 <http://tracker.ceph.com/issues/34310>`_, `pr#23811 <https://github.com/ceph/ceph/pull/23811>`_, Alfredo Deza)
* ceph-volume lvm.listing only include devices if they exist (`issue#24952 <http://tracker.ceph.com/issues/24952>`_, `pr#23150 <https://github.com/ceph/ceph/pull/23150>`_, Alfredo Deza)
* ceph-volume: process.call with stdin in Python 3 fix (`issue#24993 <http://tracker.ceph.com/issues/24993>`_, `pr#23238 <https://github.com/ceph/ceph/pull/23238>`_, Alfredo Deza)
* ceph-volume: PVolumes.get() should return one PV when using name or uuid (`issue#24784 <http://tracker.ceph.com/issues/24784>`_, `pr#23329 <https://github.com/ceph/ceph/pull/23329>`_, Andrew Schoen)
* ceph-volume: refuse to zap mapper devices (`issue#24504 <http://tracker.ceph.com/issues/24504>`_, `pr#23374 <https://github.com/ceph/ceph/pull/23374>`_, Andrew Schoen)
* ceph-volume: tests.functional inherit SSH_ARGS from ansible (`issue#34311 <http://tracker.ceph.com/issues/34311>`_, `pr#23813 <https://github.com/ceph/ceph/pull/23813>`_, Alfredo Deza)
* ceph-volume tests/functional run lvm list after OSD provisioning (`issue#24961 <http://tracker.ceph.com/issues/24961>`_, `pr#23147 <https://github.com/ceph/ceph/pull/23147>`_, Alfredo Deza)
* ceph-volume: unmount lvs correctly before zapping (`issue#24796 <http://tracker.ceph.com/issues/24796>`_, `pr#23128 <https://github.com/ceph/ceph/pull/23128>`_, Andrew Schoen)
* ceph-volume: update batch documentation to explain filestore strategies (`issue#34309 <http://tracker.ceph.com/issues/34309>`_, `pr#23825 <https://github.com/ceph/ceph/pull/23825>`_, Alfredo Deza)
* change default filestore_merge_threshold to -10 (`issue#24686 <http://tracker.ceph.com/issues/24686>`_, `pr#22814 <https://github.com/ceph/ceph/pull/22814>`_, Douglas Fuller)
* client: add inst to asok status output (`issue#24724 <http://tracker.ceph.com/issues/24724>`_, `pr#23107 <https://github.com/ceph/ceph/pull/23107>`_, Patrick Donnelly)
* client: fixup parallel calls to ceph_ll_lookup_inode() in NFS FASL (`issue#22683 <http://tracker.ceph.com/issues/22683>`_, `pr#23012 <https://github.com/ceph/ceph/pull/23012>`_, huanwen ren)
* client: increase verbosity level for log messages in helper methods (`issue#21014 <http://tracker.ceph.com/issues/21014>`_, `pr#23014 <https://github.com/ceph/ceph/pull/23014>`_, Rishabh Dave)
* client:  update inode fields according to issued caps (`issue#24269 <http://tracker.ceph.com/issues/24269>`_, `pr#22783 <https://github.com/ceph/ceph/pull/22783>`_, "Yan, Zheng")
* common: Abort in OSDMap::decode() during qa/standalone/erasure-code/test-erasure-eio.sh (`issue#23492 <http://tracker.ceph.com/issues/23492>`_, `pr#23025 <https://github.com/ceph/ceph/pull/23025>`_, Sage Weil)
* common/DecayCounter: set last_decay to current time when decoding decay counter (`issue#24440 <http://tracker.ceph.com/issues/24440>`_, `pr#22779 <https://github.com/ceph/ceph/pull/22779>`_, Zhi Zhang)
* doc: ceph-bluestore-tool manpage not getting rendered correctly (`issue#24800 <http://tracker.ceph.com/issues/24800>`_, `pr#23177 <https://github.com/ceph/ceph/pull/23177>`_, Nathan Cutler)
* filestore: add pgid in filestore pg dir split log message (`issue#24878 <http://tracker.ceph.com/issues/24878>`_, `pr#23454 <https://github.com/ceph/ceph/pull/23454>`_, Vikhyat Umrao)
* let "ceph status" use base 10 when printing numbers not sizes (`issue#22095 <http://tracker.ceph.com/issues/22095>`_, `pr#22680 <https://github.com/ceph/ceph/pull/22680>`_, Jan Fajerski, Kefu Chai)
* librados: fix buffer overflow for aio_exec python binding (`issue#23964 <http://tracker.ceph.com/issues/23964>`_, `pr#22708 <https://github.com/ceph/ceph/pull/22708>`_, Aleksei Gutikov)
* librbd: force 'invalid object map' flag on-disk update (`issue#24434 <http://tracker.ceph.com/issues/24434>`_, `pr#22753 <https://github.com/ceph/ceph/pull/22753>`_, Mykola Golub)
* librbd: utilize the journal disabled policy when removing images (`issue#23512 <http://tracker.ceph.com/issues/23512>`_, `pr#23595 <https://github.com/ceph/ceph/pull/23595>`_, Jason Dillaman)
* mds: don't report slow request for blocked filelock request (`issue#22428 <http://tracker.ceph.com/issues/22428>`_, `pr#22782 <https://github.com/ceph/ceph/pull/22782>`_, "Yan, Zheng")
* mds: dump recent events on respawn (`issue#24853 <http://tracker.ceph.com/issues/24853>`_, `pr#23213 <https://github.com/ceph/ceph/pull/23213>`_, Patrick Donnelly)
* mds: handle discontinuous mdsmap (`issue#24856 <http://tracker.ceph.com/issues/24856>`_, `pr#23169 <https://github.com/ceph/ceph/pull/23169>`_, "Yan, Zheng")
* mds: increase debug level for dropped client cap msg (`issue#24855 <http://tracker.ceph.com/issues/24855>`_, `pr#23214 <https://github.com/ceph/ceph/pull/23214>`_, Patrick Donnelly)
* mds: low wrlock efficiency due to dirfrags traversal (`issue#24467 <http://tracker.ceph.com/issues/24467>`_, `pr#22885 <https://github.com/ceph/ceph/pull/22885>`_, Xuehan Xu)
* mds: print mdsmap processed at low debug level (`issue#24852 <http://tracker.ceph.com/issues/24852>`_, `pr#23212 <https://github.com/ceph/ceph/pull/23212>`_, Patrick Donnelly)
* mds: scrub doesn't always return JSON results (`issue#23958 <http://tracker.ceph.com/issues/23958>`_, `pr#23222 <https://github.com/ceph/ceph/pull/23222>`_, Venky Shankar)
* mds: unset deleted vars in shutdown_pass (`issue#23766 <http://tracker.ceph.com/issues/23766>`_, `pr#23015 <https://github.com/ceph/ceph/pull/23015>`_, Patrick Donnelly)
* mgr: add units to performance counters (`issue#22747 <http://tracker.ceph.com/issues/22747>`_, `pr#23266 <https://github.com/ceph/ceph/pull/23266>`_, Ernesto Puerta, Rubab Syed)
* mgr: ceph osd safe-to-destroy crashes the mgr (`issue#23249 <http://tracker.ceph.com/issues/23249>`_, `pr#22806 <https://github.com/ceph/ceph/pull/22806>`_, Sage Weil)
* mgr/MgrClient: Protect daemon_health_metrics (`issue#23352 <http://tracker.ceph.com/issues/23352>`_, `pr#23459 <https://github.com/ceph/ceph/pull/23459>`_, Kjetil Joergensen, Brad Hubbard)
* mon: Add option to view IP addresses of clients in output of 'ceph features' (`issue#21315 <http://tracker.ceph.com/issues/21315>`_, `pr#22773 <https://github.com/ceph/ceph/pull/22773>`_, Paul Emmerich)
* mon/HealthMonitor: do not send MMonHealthChecks to pre-luminous mon (`issue#24481 <http://tracker.ceph.com/issues/24481>`_, `pr#22655 <https://github.com/ceph/ceph/pull/22655>`_, Sage Weil)
* os/bluestore: fix flush_commit locking (`issue#21480 <http://tracker.ceph.com/issues/21480>`_, `pr#22904 <https://github.com/ceph/ceph/pull/22904>`_, Sage Weil)
* os/bluestore: fix incomplete faulty range marking when doing compression (`issue#21480 <http://tracker.ceph.com/issues/21480>`_, `pr#22909 <https://github.com/ceph/ceph/pull/22909>`_, Igor Fedotov)
* os/bluestore: fix races on SharedBlob::coll in ~SharedBlob (`issue#24859 <http://tracker.ceph.com/issues/24859>`_, `pr#23064 <https://github.com/ceph/ceph/pull/23064>`_, Radoslaw Zarzynski)
* osdc: Fix the wrong BufferHead offset (`issue#24484 <http://tracker.ceph.com/issues/24484>`_, `pr#22865 <https://github.com/ceph/ceph/pull/22865>`_, dongdong tao)
* osd: do_sparse_read(): Verify checksum earlier so we will try to repair and missed backport (`issue#24875 <http://tracker.ceph.com/issues/24875>`_, `pr#23379 <https://github.com/ceph/ceph/pull/23379>`_, xie xingguo, David Zafman)
* osd: eternal stuck PG in 'unfound_recovery' (`issue#24373 <http://tracker.ceph.com/issues/24373>`_, `pr#22546 <https://github.com/ceph/ceph/pull/22546>`_, Sage Weil)
* osd: may get empty info at recovery (`issue#24588 <http://tracker.ceph.com/issues/24588>`_, `pr#22862 <https://github.com/ceph/ceph/pull/22862>`_, Sage Weil)
* osd/OSDMap: CRUSH_TUNABLES5 added in jewel, not kraken (`issue#25057 <http://tracker.ceph.com/issues/25057>`_, `pr#23227 <https://github.com/ceph/ceph/pull/23227>`_, Sage Weil)
* osd/Session: fix invalid iterator dereference in Sessoin::have_backoff() (`issue#24486 <http://tracker.ceph.com/issues/24486>`_, `pr#22729 <https://github.com/ceph/ceph/pull/22729>`_, Sage Weil)
* pjd: cd: too many arguments (`issue#24307 <http://tracker.ceph.com/issues/24307>`_, `pr#22883 <https://github.com/ceph/ceph/pull/22883>`_, Neha Ojha)
* PurgeQueue sometimes ignores Journaler errors (`issue#24533 <http://tracker.ceph.com/issues/24533>`_, `pr#22811 <https://github.com/ceph/ceph/pull/22811>`_, John Spray)
* pybind: pybind/mgr/mgr_module: make rados handle available to all modules (`issue#24788 <http://tracker.ceph.com/issues/24788>`_, `issue#25102 <http://tracker.ceph.com/issues/25102>`_, `pr#23235 <https://github.com/ceph/ceph/pull/23235>`_, Ernesto Puerta, Sage Weil)
* pybind: Python bindings use iteritems method which is not Python 3 compatible (`issue#24779 <http://tracker.ceph.com/issues/24779>`_, `pr#22918 <https://github.com/ceph/ceph/pull/22918>`_, Nathan Cutler, Kefu Chai)
* pybind: rados.pyx: make all exceptions accept keyword arguments (`issue#24033 <http://tracker.ceph.com/issues/24033>`_, `pr#22979 <https://github.com/ceph/ceph/pull/22979>`_, Rishabh Dave)
* rbd: fix issues in IEC unit handling (`issue#26927 <http://tracker.ceph.com/issues/26927>`_, `issue#26928 <http://tracker.ceph.com/issues/26928>`_, `pr#23776 <https://github.com/ceph/ceph/pull/23776>`_, Jason Dillaman)
* repeated eviction of idle client until some IO happens (`issue#24052 <http://tracker.ceph.com/issues/24052>`_, `pr#22780 <https://github.com/ceph/ceph/pull/22780>`_, "Yan, Zheng")
* rgw: add curl_low_speed_limit and curl_low_speed_time config to avoid the thread hangs in data sync (`issue#25019 <http://tracker.ceph.com/issues/25019>`_, `pr#23144 <https://github.com/ceph/ceph/pull/23144>`_, Mark Kogan, Zhang Shaowen)
* rgw: add unit test for cls bi list command (`issue#24483 <http://tracker.ceph.com/issues/24483>`_, `pr#22846 <https://github.com/ceph/ceph/pull/22846>`_, Orit Wasserman, Xinying Song)
* rgw: do not ignore EEXIST in RGWPutObj::execute (`issue#22790 <http://tracker.ceph.com/issues/22790>`_, `pr#23207 <https://github.com/ceph/ceph/pull/23207>`_, Matt Benjamin)
* rgw: fail to recover index from crash luminous backport (`issue#24640 <http://tracker.ceph.com/issues/24640>`_, `issue#24280 <http://tracker.ceph.com/issues/24280>`_, `pr#23130 <https://github.com/ceph/ceph/pull/23130>`_, Tianshan Qu)
* rgw: fix gc may cause a large number of read traffic (`issue#24767 <http://tracker.ceph.com/issues/24767>`_, `pr#22984 <https://github.com/ceph/ceph/pull/22984>`_, Xin Liao)
* rgw: fix the bug of radowgw-admin zonegroup set requires realm (`issue#21583 <http://tracker.ceph.com/issues/21583>`_, `pr#22767 <https://github.com/ceph/ceph/pull/22767>`_, lvshanchun)
* rgw: have a configurable authentication order (`issue#23089 <http://tracker.ceph.com/issues/23089>`_, `pr#23501 <https://github.com/ceph/ceph/pull/23501>`_, Abhishek Lekshmanan)
* rgw: index complete miss zones_trace set (`issue#24590 <http://tracker.ceph.com/issues/24590>`_, `pr#22820 <https://github.com/ceph/ceph/pull/22820>`_, Tianshan Qu)
* rgw: Invalid Access-Control-Request-Request may bypass validate_cors_rule_method (`issue#24223 <http://tracker.ceph.com/issues/24223>`_, `pr#22934 <https://github.com/ceph/ceph/pull/22934>`_, Jeegn Chen)
* rgw: meta and data notify thread miss stop cr manager (`issue#24589 <http://tracker.ceph.com/issues/24589>`_, `pr#22822 <https://github.com/ceph/ceph/pull/22822>`_, Tianshan Qu)
* rgw-multisite: endless loop in RGWBucketShardIncrementalSyncCR (`issue#24603 <http://tracker.ceph.com/issues/24603>`_, `pr#22817 <https://github.com/ceph/ceph/pull/22817>`_, cfanz)
* rgw performance regression for luminous 12.2.4 (`issue#23379 <http://tracker.ceph.com/issues/23379>`_, `pr#22930 <https://github.com/ceph/ceph/pull/22930>`_, Mark Kogan)
* rgw: radogw-admin reshard status command should print text for reshar… (`issue#23257 <http://tracker.ceph.com/issues/23257>`_, `pr#23019 <https://github.com/ceph/ceph/pull/23019>`_, Orit Wasserman)
* rgw: "radosgw-admin objects expire" always returns ok even if the pro… (`issue#24592 <http://tracker.ceph.com/issues/24592>`_, `pr#23000 <https://github.com/ceph/ceph/pull/23000>`_, Zhang Shaowen)
* rgw: require --yes-i-really-mean-it to run radosgw-admin orphans find (`issue#24146 <http://tracker.ceph.com/issues/24146>`_, `pr#22985 <https://github.com/ceph/ceph/pull/22985>`_, Matt Benjamin)
* rgw: REST admin metadata API paging failure bucket & bucket.instance: InvalidArgument (`issue#23099 <http://tracker.ceph.com/issues/23099>`_, `pr#22932 <https://github.com/ceph/ceph/pull/22932>`_, Matt Benjamin)
* rgw: set cr state if aio_read err return in RGWCloneMetaLogCoroutine (`issue#24566 <http://tracker.ceph.com/issues/24566>`_, `pr#22942 <https://github.com/ceph/ceph/pull/22942>`_, Tianshan Qu)
* spdk: fix ceph-osd crash when activate SPDK (`issue#24371 <http://tracker.ceph.com/issues/24371>`_, `pr#22686 <https://github.com/ceph/ceph/pull/22686>`_, tone-zhang)
* tools/ceph-objectstore-tool: split filestore directories offline to target hash level (`issue#21366 <http://tracker.ceph.com/issues/21366>`_, `pr#23418 <https://github.com/ceph/ceph/pull/23418>`_, Zhi Zhang)


v12.2.7 Luminous
================

This is the seventh bugfix release of Luminous v12.2.x long term
stable release series. This release contains several fixes for
regressions in the v12.2.6 and v12.2.5 releases.  We recommend that
all users upgrade.

:note: The v12.2.6 release has serious known regressions.  If you installed this release, please see the upgrade procedure below.

:note: The v12.2.5 release has a potential data corruption issue with erasure coded pools.  If you ran v12.2.5 with erasure coding, please see below.

.. _luminous-12-2-5-upgrades:

Upgrading from v12.2.6
----------------------

v12.2.6 included an incomplete backport of an optimization for
BlueStore OSDs that avoids maintaining both the per-object checksum
and the internal BlueStore checksum.  Due to the accidental omission
of a critical follow-on patch, v12.2.6 corrupts (fails to update) the
stored per-object checksum value for some objects.  This can result in
an EIO error when trying to read those objects.

#. If your cluster uses FileStore only, no special action is required.
   This problem only affects clusters with BlueStore.

#. If your cluster has only BlueStore OSDs (no FileStore), then you
   should enable the following OSD option::

     osd skip data digest = true

   This will avoid setting and start ignoring the full-object digests
   whenever the primary for a PG is BlueStore.

#. If you have a mix of BlueStore and FileStore OSDs, then you should
   enable the following OSD option::

     osd distrust data digest = true

   This will avoid setting and start ignoring the full-object digests
   in all cases.  This weakens the data integrity checks for
   FileStore (although those checks were always only opportunistic).

If your cluster includes BlueStore OSDs and was affected, deep scrubs
will generate errors about mismatched CRCs for affected objects.
Currently the repair operation does not know how to correct them
(since all replicas do not match the expected checksum it does not
know how to proceed).  These warnings are harmless in the sense that
IO is not affected and the replicas are all still in sync.  The number
of affected objects is likely to drop (possibly to zero) on their own
over time as those objects are modified.  We expect to include a scrub
improvement in v12.2.8 to clean up any remaining objects.

Additionally, see the notes below, which apply to both v12.2.5 and v12.2.6.

Upgrading from v12.2.5 or v12.2.6
---------------------------------

If you used v12.2.5 or v12.2.6 in combination with erasure coded
pools, there is a small risk of corruption under certain workloads.
Specifically, when:

* An erasure coded pool is in use
* The pool is busy with successful writes
* The pool is also busy with updates that result in an error result to
  the librados user.  RGW garbage collection is the most common
  example of this (it sends delete operations on objects that don't
  always exist.)
* Some OSDs are reasonably busy.  One known example of such load is
  FileStore splitting, although in principle any load on the cluster
  could also trigger the behavior.
* One or more OSDs restarts.

This combination can trigger an OSD crash and possibly leave PGs in a state
where they fail to peer.

Notably, upgrading a cluster involves OSD restarts and as such may
increase the risk of encountering this bug.  For this reason, for
clusters with erasure coded pools, we recommend the following upgrade
procedure to minimize risk:

#. Install the v12.2.7 packages.
#. Temporarily quiesce IO to cluster::

     ceph osd pause

#. Restart all OSDs and wait for all PGs to become active.
#. Resume IO::

     ceph osd unpause

This will cause an availability outage for the duration of the OSD
restarts.  If this in unacceptable, an *more risky* alternative is to
disable RGW garbage collection (the primary known cause of these rados
operations) for the duration of the upgrade::

#. Set ``rgw_enable_gc_threads = false`` in ceph.conf
#. Restart all radosgw daemons
#. Upgrade and restart all OSDs
#. Remove ``rgw_enable_gc_threads = false`` from ceph.conf
#. Restart all radosgw daemons

Upgrading from other versions
-----------------------------

If your cluster did not run v12.2.5 or v12.2.6 then none of the above
issues apply to you and you should upgrade normally.

Notable Changes
---------------

* mon/AuthMonitor: improve error message (`issue#21765 <http://tracker.ceph.com/issues/21765>`_, `pr#22963 <https://github.com/ceph/ceph/pull/22963>`_, Douglas Fuller)
* osd/PG: do not blindly roll forward to log.head (`issue#24597 <http://tracker.ceph.com/issues/24597>`_, `pr#22976 <https://github.com/ceph/ceph/pull/22976>`_, Sage Weil)
* osd/PrimaryLogPG: rebuild attrs from clients (`issue#24768 <http://tracker.ceph.com/issues/24768>`_, `pr#22962 <https://github.com/ceph/ceph/pull/22962>`_, Sage Weil)
* osd: work around data digest problems in 12.2.6 (version 2) (`issue#24922 <http://tracker.ceph.com/issues/24922>`_, `pr#23055 <https://github.com/ceph/ceph/pull/23055>`_, Sage Weil)
* rgw: objects in cache never refresh after rgw_cache_expiry_interval (`issue#24346 <http://tracker.ceph.com/issues/24346>`_, `pr#22369 <https://github.com/ceph/ceph/pull/22369>`_, Casey Bodley, Matt Benjamin)


v12.2.6 Luminous
================

:note: This is a broken release with serious known regressions.  Do not install it.

This is the sixth bugfix release of Luminous v12.2.x long term stable release
series. This release contains a range of bug fixes across all components of
Ceph and a few security fixes.

Notable Changes
---------------

- *Auth*:

  * In 12.2.4 and earlier releases, keyring caps were not checked for validity,
    so the caps string could be anything. As of 12.2.6, caps strings are
    validated and providing a keyring with an invalid caps string to, e.g.,
    "ceph auth add" will result in an error.
  * CVE 2018-1128: auth: cephx authorizer subject to replay attack (`issue#24836 <http://tracker.ceph.com/issues/24836>`_, Sage Weil)
  * CVE 2018-1129: auth: cephx signature check is weak (`issue#24837 <http://tracker.ceph.com/issues/24837>`_, Sage Weil)
  * CVE 2018-10861: mon: auth checks not correct for pool ops (`issue#24838 <http://tracker.ceph.com/issues/24838>`_, Jason Dillaman)


* The config-key interface can store arbitrary binary blobs but JSON
  can only express printable strings.  If binary blobs are present,
  the 'ceph config-key dump' command will show them as something like
  ``<<< binary blob of length N >>>``.


Other Notable Changes
---------------------
* build/ops: build-integration-branch script (`issue#24003 <http://tracker.ceph.com/issues/24003>`_, `pr#21919 <https://github.com/ceph/ceph/pull/21919>`_, Nathan Cutler, Kefu Chai, Sage Weil)
* cephfs-journal-tool: wait prezero ops before destroying journal (`issue#20549 <http://tracker.ceph.com/issues/20549>`_, `pr#21874 <https://github.com/ceph/ceph/pull/21874>`_, "Yan, Zheng")
* cephfs: MDSMonitor: cleanup and protect fsmap access (`issue#23762 <http://tracker.ceph.com/issues/23762>`_, `pr#21732 <https://github.com/ceph/ceph/pull/21732>`_, Patrick Donnelly)
* cephfs: MDSMonitor: crash after assigning standby-replay daemon in multifs setup (`issue#23762 <http://tracker.ceph.com/issues/23762>`_, `issue#23658 <http://tracker.ceph.com/issues/23658>`_, `pr#22603 <https://github.com/ceph/ceph/pull/22603>`_, "Yan, Zheng")
* cephfs: MDSMonitor: fix mds health printed in bad format (`issue#23582 <http://tracker.ceph.com/issues/23582>`_, `pr#21447 <https://github.com/ceph/ceph/pull/21447>`_, Patrick Donnelly)
* cephfs: MDSMonitor: initialize new Filesystem epoch from pending (`issue#23764 <http://tracker.ceph.com/issues/23764>`_, `pr#21512 <https://github.com/ceph/ceph/pull/21512>`_, Patrick Donnelly)
* ceph-fuse: missing dentries in readdir result (`issue#23894 <http://tracker.ceph.com/issues/23894>`_, `pr#22119 <https://github.com/ceph/ceph/pull/22119>`_, "Yan, Zheng")
* ceph-fuse: return proper exit code (`issue#23665 <http://tracker.ceph.com/issues/23665>`_, `pr#21495 <https://github.com/ceph/ceph/pull/21495>`_, Patrick Donnelly)
* ceph-fuse: trim ceph-fuse -V output (`issue#23248 <http://tracker.ceph.com/issues/23248>`_, `pr#21600 <https://github.com/ceph/ceph/pull/21600>`_, Jos Collin)
* ceph_test_rados_api_aio: fix race with full pool and osdmap (`issue#23917 <http://tracker.ceph.com/issues/23917>`_, `issue#23876 <http://tracker.ceph.com/issues/23876>`_, `pr#21778 <https://github.com/ceph/ceph/pull/21778>`_, Sage Weil)
* ceph-volume: error on commands that need ceph.conf to operate (`issue#23941 <http://tracker.ceph.com/issues/23941>`_, `pr#22746 <https://github.com/ceph/ceph/pull/22746>`_, Andrew Schoen)
* ceph-volume: failed ceph-osd --mkfs command doesn't halt the OSD creation process (`issue#23874 <http://tracker.ceph.com/issues/23874>`_, `pr#21746 <https://github.com/ceph/ceph/pull/21746>`_, Alfredo Deza)
* client: add ceph_ll_sync_inode (`issue#23291 <http://tracker.ceph.com/issues/23291>`_, `pr#21109 <https://github.com/ceph/ceph/pull/21109>`_, Jeff Layton)
* client: add client option descriptions (`issue#22933 <http://tracker.ceph.com/issues/22933>`_, `pr#21589 <https://github.com/ceph/ceph/pull/21589>`_, Patrick Donnelly)
* client: anchor dentries for trimming to make cap traversal safe (`issue#24137 <http://tracker.ceph.com/issues/24137>`_, `pr#22201 <https://github.com/ceph/ceph/pull/22201>`_, Patrick Donnelly)
* client: avoid freeing inode when it contains TX buffer head (`issue#23837 <http://tracker.ceph.com/issues/23837>`_, `pr#22168 <https://github.com/ceph/ceph/pull/22168>`_, Guan yunfei, "Yan, Zheng", Jason Dillaman)
* client: dirty caps may never get the chance to flush (`issue#22546 <http://tracker.ceph.com/issues/22546>`_, `pr#21278 <https://github.com/ceph/ceph/pull/21278>`_, dongdong tao)
* client: fix issue of revoking non-auth caps (`issue#24172 <http://tracker.ceph.com/issues/24172>`_, `pr#22221 <https://github.com/ceph/ceph/pull/22221>`_, "Yan, Zheng")
* client: fix request send_to_auth was never really used (`issue#23541 <http://tracker.ceph.com/issues/23541>`_, `pr#21354 <https://github.com/ceph/ceph/pull/21354>`_, Zhi Zhang)
* client: Fix the gid_count check (`issue#23652 <http://tracker.ceph.com/issues/23652>`_, `pr#21596 <https://github.com/ceph/ceph/pull/21596>`_, Jos Collin)
* client: flush the mdlog in _fsync before waiting on unstable reqs (`issue#23714 <http://tracker.ceph.com/issues/23714>`_, `pr#21542 <https://github.com/ceph/ceph/pull/21542>`_, Jeff Layton)
* client: hangs on umount if it had an MDS session evicted (`issue#10915 <http://tracker.ceph.com/issues/10915>`_, `pr#22018 <https://github.com/ceph/ceph/pull/22018>`_, Rishabh Dave)
* client: void sending mds request while holding cap reference (`issue#24369 <http://tracker.ceph.com/issues/24369>`_, `pr#22354 <https://github.com/ceph/ceph/pull/22354>`_, "Yan, Zheng")
* cmake: fix the cepfs java binding build on Bionic (`issue#23458 <http://tracker.ceph.com/issues/23458>`_, `issue#24012 <http://tracker.ceph.com/issues/24012>`_, `pr#21872 <https://github.com/ceph/ceph/pull/21872>`_, Kefu Chai, Shengjing Zhu)
* cmake/modules/BuildRocksDB.cmake: enable compressions for rocksdb (`issue#24025 <http://tracker.ceph.com/issues/24025>`_, `pr#22215 <https://github.com/ceph/ceph/pull/22215>`_, Kefu Chai)
* common: ARMv8 feature detection broken, leading to illegal instruction crashes (`issue#23464 <http://tracker.ceph.com/issues/23464>`_, `pr#22567 <https://github.com/ceph/ceph/pull/22567>`_, Adam Kupczyk)
* common: fix BoundedKeyCounter const_pointer_iterator (`issue#22139 <http://tracker.ceph.com/issues/22139>`_, `pr#21083 <https://github.com/ceph/ceph/pull/21083>`_, Casey Bodley)
* common: fix typo in rados bench write JSON output (`issue#24199 <http://tracker.ceph.com/issues/24199>`_, `pr#22391 <https://github.com/ceph/ceph/pull/22391>`_, Sandor Zeestraten)
* common: partially revert 95fc248 to make get_process_name work (`issue#24123 <http://tracker.ceph.com/issues/24123>`_, `pr#22290 <https://github.com/ceph/ceph/pull/22290>`_, Mykola Golub)
* core: Deleting a pool with active notify linger ops can result in seg fault (`issue#23966 <http://tracker.ceph.com/issues/23966>`_, `pr#22143 <https://github.com/ceph/ceph/pull/22143>`_, Kefu Chai, Jason Dillaman)
* core: mon/MgrMonitor: change 'unresponsive' message to info level (`issue#24222 <http://tracker.ceph.com/issues/24222>`_, `pr#22331 <https://github.com/ceph/ceph/pull/22331>`_, Sage Weil)
* core: Wip scrub omap (`issue#24366 <http://tracker.ceph.com/issues/24366>`_, `pr#22375 <https://github.com/ceph/ceph/pull/22375>`_, xie xingguo, David Zafman)
* crush: fix device_class_clone for unpopulated/empty weight-sets (`issue#23386 <http://tracker.ceph.com/issues/23386>`_, `pr#22381 <https://github.com/ceph/ceph/pull/22381>`_, Sage Weil)
* crush, osd: handle multiple parents properly when applying pg upmaps (`issue#23921 <http://tracker.ceph.com/issues/23921>`_, `pr#22115 <https://github.com/ceph/ceph/pull/22115>`_, xiexingguo)
* doc: Fix -d description in ceph-fuse (`issue#23214 <http://tracker.ceph.com/issues/23214>`_, `pr#21616 <https://github.com/ceph/ceph/pull/21616>`_, Jos Collin)
* doc:Update ceph-fuse doc (`issue#23084 <http://tracker.ceph.com/issues/23084>`_, `pr#21603 <https://github.com/ceph/ceph/pull/21603>`_, Jos Collin)
* fuse: wire up fuse_ll_access (`issue#23509 <http://tracker.ceph.com/issues/23509>`_, `pr#21475 <https://github.com/ceph/ceph/pull/21475>`_, Jeff Layton)
* kceph: umount on evicted client blocks forever (`issue#24053 <http://tracker.ceph.com/issues/24053>`_, `issue#24054 <http://tracker.ceph.com/issues/24054>`_, `pr#22208 <https://github.com/ceph/ceph/pull/22208>`_, Yan, Zheng, "Yan, Zheng")
* librbd: commit IO as safe when complete if writeback cache is disabled (`issue#23516 <http://tracker.ceph.com/issues/23516>`_, `pr#22370 <https://github.com/ceph/ceph/pull/22370>`_, Jason Dillaman)
* librbd: prevent watcher from unregistering with in-flight actions (`issue#23955 <http://tracker.ceph.com/issues/23955>`_, `pr#21938 <https://github.com/ceph/ceph/pull/21938>`_, Jason Dillaman)
* lvm: when osd creation fails log the exception (`issue#24456 <http://tracker.ceph.com/issues/24456>`_, `pr#22641 <https://github.com/ceph/ceph/pull/22641>`_, Andrew Schoen)
* mds: avoid calling rejoin_gather_finish() two times successively (`issue#24047 <http://tracker.ceph.com/issues/24047>`_, `pr#22171 <https://github.com/ceph/ceph/pull/22171>`_, "Yan, Zheng")
* mds: broadcast quota to relevant clients when quota is explicitly set (`issue#24133 <http://tracker.ceph.com/issues/24133>`_, `pr#22271 <https://github.com/ceph/ceph/pull/22271>`_, Zhi Zhang)
* mds: crash when failover (`issue#23518 <http://tracker.ceph.com/issues/23518>`_, `pr#21900 <https://github.com/ceph/ceph/pull/21900>`_, "Yan, Zheng")
* mds: don't discover inode/dirfrag when mds is in 'starting' state (`issue#23812 <http://tracker.ceph.com/issues/23812>`_, `pr#21990 <https://github.com/ceph/ceph/pull/21990>`_, "Yan, Zheng")
* mds: fix occasional dir rstat inconsistency between multi-MDSes (`issue#23538 <http://tracker.ceph.com/issues/23538>`_, `pr#21617 <https://github.com/ceph/ceph/pull/21617>`_, "Yan, Zheng", Zhi Zhang)
* mds: fix some memory leak (`issue#24289 <http://tracker.ceph.com/issues/24289>`_, `pr#22310 <https://github.com/ceph/ceph/pull/22310>`_, "Yan, Zheng")
* mds: fix unhealth heartbeat during rejoin (`issue#23530 <http://tracker.ceph.com/issues/23530>`_, `pr#21366 <https://github.com/ceph/ceph/pull/21366>`_, dongdong tao)
* mds: handle imported session race (`issue#24072 <http://tracker.ceph.com/issues/24072>`_, `issue#24087 <http://tracker.ceph.com/issues/24087>`_, `pr#21989 <https://github.com/ceph/ceph/pull/21989>`_, Patrick Donnelly)
* mds: include nfiles/nsubdirs of directory inode in MClientCaps (`issue#23855 <http://tracker.ceph.com/issues/23855>`_, `pr#22118 <https://github.com/ceph/ceph/pull/22118>`_, "Yan, Zheng")
* mds: kick rdlock if waiting for dirfragtreelock (`issue#23919 <http://tracker.ceph.com/issues/23919>`_, `pr#21901 <https://github.com/ceph/ceph/pull/21901>`_, Patrick Donnelly)
* mds: make rstat.rctime follow inodes' ctime (`issue#23380 <http://tracker.ceph.com/issues/23380>`_, `pr#21448 <https://github.com/ceph/ceph/pull/21448>`_, "Yan, Zheng")
* mds: mark damaged if sessions' preallocated inos don't match inotable (`issue#23452 <http://tracker.ceph.com/issues/23452>`_, `pr#21372 <https://github.com/ceph/ceph/pull/21372>`_, "Yan, Zheng")
* mds: mark new root inode dirty (`issue#23960 <http://tracker.ceph.com/issues/23960>`_, `pr#21922 <https://github.com/ceph/ceph/pull/21922>`_, Patrick Donnelly)
* mds: mds shutdown fixes and optimization (`issue#23602 <http://tracker.ceph.com/issues/23602>`_, `pr#21346 <https://github.com/ceph/ceph/pull/21346>`_, "Yan, Zheng")
* mds: misc load balancer fixes (`issue#21745 <http://tracker.ceph.com/issues/21745>`_, `pr#21412 <https://github.com/ceph/ceph/pull/21412>`_, "Yan, Zheng", Jianyu Li)
* mds: properly check auth subtree count in MDCache::shutdown_pass() (`issue#23813 <http://tracker.ceph.com/issues/23813>`_, `pr#21844 <https://github.com/ceph/ceph/pull/21844>`_, "Yan, Zheng")
* mds: properly dirty sessions opened by journal replay (`issue#23625 <http://tracker.ceph.com/issues/23625>`_, `pr#21441 <https://github.com/ceph/ceph/pull/21441>`_, "Yan, Zheng")
* mds: properly trim log segments after scrub repairs something (`issue#23880 <http://tracker.ceph.com/issues/23880>`_, `pr#21840 <https://github.com/ceph/ceph/pull/21840>`_, "Yan, Zheng")
* mds: set could_consume to false when no purge queue item actually exe… (`issue#24073 <http://tracker.ceph.com/issues/24073>`_, `pr#22176 <https://github.com/ceph/ceph/pull/22176>`_, Xuehan Xu)
* mds: trim log during shutdown to clean metadata (`issue#23923 <http://tracker.ceph.com/issues/23923>`_, `pr#21899 <https://github.com/ceph/ceph/pull/21899>`_, Patrick Donnelly)
* mds: underwater dentry check in CDir::_omap_fetched is racy (`issue#23032 <http://tracker.ceph.com/issues/23032>`_, `pr#21187 <https://github.com/ceph/ceph/pull/21187>`_, Yan, Zheng)
* mg_read() call has wrong arguments (`issue#23596 <http://tracker.ceph.com/issues/23596>`_, `pr#21382 <https://github.com/ceph/ceph/pull/21382>`_, Nathan Cutler)
* mgr/influx: Only split string on first occurence of dot (.) (`issue#23996 <http://tracker.ceph.com/issues/23996>`_, `pr#21965 <https://github.com/ceph/ceph/pull/21965>`_, Wido den Hollander)
* mgr: Module 'balancer' has failed: could not find bucket -14 (`issue#24167 <http://tracker.ceph.com/issues/24167>`_, `pr#22308 <https://github.com/ceph/ceph/pull/22308>`_, Sage Weil)
* mon: add  'ceph osd pool get erasure allow_ec_overwrites' command (`issue#23487 <http://tracker.ceph.com/issues/23487>`_, `pr#21378 <https://github.com/ceph/ceph/pull/21378>`_, Mykola Golub)
* mon: enable level_compaction_dynamic_level_bytes for rocksdb (`issue#24361 <http://tracker.ceph.com/issues/24361>`_, `pr#22360 <https://github.com/ceph/ceph/pull/22360>`_, Kefu Chai)
* mon: handle bad snapshot removal reqs gracefully (`issue#18746 <http://tracker.ceph.com/issues/18746>`_, `pr#21717 <https://github.com/ceph/ceph/pull/21717>`_, Paul Emmerich)
* mon: High MON cpu usage when cluster is changing (`issue#23713 <http://tracker.ceph.com/issues/23713>`_, `pr#21968 <https://github.com/ceph/ceph/pull/21968>`_, Sage Weil, Xiaoxi CHEN)
* mon/MDSMonitor: do not send redundant MDS health messages to cluster log (`issue#24308 <http://tracker.ceph.com/issues/24308>`_, `pr#22558 <https://github.com/ceph/ceph/pull/22558>`_, Sage Weil)
* msg/async/AsyncConnection: Fix FPE in process_connection (`issue#23618 <http://tracker.ceph.com/issues/23618>`_, `pr#21376 <https://github.com/ceph/ceph/pull/21376>`_, Brad Hubbard)
* os/bluestore: alter the allow_eio policy regarding kernel's error list (`issue#23333 <http://tracker.ceph.com/issues/23333>`_, `pr#21405 <https://github.com/ceph/ceph/pull/21405>`_, Radoslaw Zarzynski)
* os/bluestore/bluefs_types: make block_mask 64-bit (`issue#23840 <http://tracker.ceph.com/issues/23840>`_, `pr#21740 <https://github.com/ceph/ceph/pull/21740>`_, Sage Weil)
* os/bluestore: fix exceeding the max IO queue depth in KernelDevice (`issue#23246 <http://tracker.ceph.com/issues/23246>`_, `pr#21407 <https://github.com/ceph/ceph/pull/21407>`_, Radoslaw Zarzynski)
* os/bluestore: fix SharedBlobSet refcounting race (`issue#24319 <http://tracker.ceph.com/issues/24319>`_, `pr#22650 <https://github.com/ceph/ceph/pull/22650>`_, Sage Weil)
* os/bluestore: simplify and fix SharedBlob::put() (`issue#24211 <http://tracker.ceph.com/issues/24211>`_, `pr#22351 <https://github.com/ceph/ceph/pull/22351>`_, Sage Weil)
* osdc/Objecter: fix recursive locking in _finish_command (`issue#23940 <http://tracker.ceph.com/issues/23940>`_, `pr#21939 <https://github.com/ceph/ceph/pull/21939>`_, Sage Weil)
* osdc/Objecter: prevent double-invocation of linger op callback (`issue#23872 <http://tracker.ceph.com/issues/23872>`_, `pr#21752 <https://github.com/ceph/ceph/pull/21752>`_, Jason Dillaman)
* osd: do not crash on empty snapset (`issue#23851 <http://tracker.ceph.com/issues/23851>`_, `pr#21638 <https://github.com/ceph/ceph/pull/21638>`_, Mykola Golub, Igor Fedotov)
* osd: Don't evict even when preemption has restarted with smaller chunk (`issue#22881 <http://tracker.ceph.com/issues/22881>`_, `issue#23909 <http://tracker.ceph.com/issues/23909>`_, `issue#23646 <http://tracker.ceph.com/issues/23646>`_, `pr#22044 <https://github.com/ceph/ceph/pull/22044>`_, Sage Weil, fang yuxiang, Jianpeng Ma, kungf, xie xingguo, David Zafman)
* osd/ECBackend: only check required shards when finishing recovery reads (`issue#23195 <http://tracker.ceph.com/issues/23195>`_, `pr#21911 <https://github.com/ceph/ceph/pull/21911>`_, Josh Durgin, Kefu Chai)
* osd: increase default hard pg limit (`issue#24243 <http://tracker.ceph.com/issues/24243>`_, `pr#22592 <https://github.com/ceph/ceph/pull/22592>`_, Josh Durgin)
* osd/OSDMap: check against cluster topology changing before applying pg upmaps (`issue#23878 <http://tracker.ceph.com/issues/23878>`_, `pr#21818 <https://github.com/ceph/ceph/pull/21818>`_, xiexingguo)
* osd/PG: fix DeferRecovery vs AllReplicasRecovered race (`issue#23860 <http://tracker.ceph.com/issues/23860>`_, `pr#21964 <https://github.com/ceph/ceph/pull/21964>`_, Sage Weil)
* osd/PG: fix uninit read in Incomplete::react(AdvMap&) (`issue#23980 <http://tracker.ceph.com/issues/23980>`_, `pr#21993 <https://github.com/ceph/ceph/pull/21993>`_, Sage Weil)
* osd/PrimaryLogPG: avoid infinite loop when flush collides with write … (`issue#23664 <http://tracker.ceph.com/issues/23664>`_, `pr#21764 <https://github.com/ceph/ceph/pull/21764>`_, Sage Weil)
* osd: publish osdmap to OSDService before starting wq threads (`issue#21977 <http://tracker.ceph.com/issues/21977>`_, `pr#21737 <https://github.com/ceph/ceph/pull/21737>`_, Sage Weil)
* osd: Warn about objects with too many omap entries (`issue#23784 <http://tracker.ceph.com/issues/23784>`_, `pr#21518 <https://github.com/ceph/ceph/pull/21518>`_, Brad Hubbard)
* qa: disable -Werror when compiling env_librados_test (`issue#23786 <http://tracker.ceph.com/issues/23786>`_, `pr#21655 <https://github.com/ceph/ceph/pull/21655>`_, Kefu Chai)
* qa: fix blacklisted check for test_lifecycle (`issue#23975 <http://tracker.ceph.com/issues/23975>`_, `pr#21921 <https://github.com/ceph/ceph/pull/21921>`_, Patrick Donnelly)
* qa: remove racy/buggy test_purge_queue_op_rate (`issue#23829 <http://tracker.ceph.com/issues/23829>`_, `pr#21841 <https://github.com/ceph/ceph/pull/21841>`_, Patrick Donnelly)
* qa/suites/rbd/basic/msgr-failures: remove many.yaml (`issue#23789 <http://tracker.ceph.com/issues/23789>`_, `pr#22128 <https://github.com/ceph/ceph/pull/22128>`_, Sage Weil)
* qa: wait longer for osd to flush pg stats (`issue#24321 <http://tracker.ceph.com/issues/24321>`_, `pr#22296 <https://github.com/ceph/ceph/pull/22296>`_, Kefu Chai)
* qa/workunits/mon/test_mon_config_key.py fails on master (`issue#23622 <http://tracker.ceph.com/issues/23622>`_, `pr#21368 <https://github.com/ceph/ceph/pull/21368>`_, Sage Weil)
* qa/workunits/rbd: adapt import_export test to handle multiple units (`issue#24733 <http://tracker.ceph.com/issues/24733>`_, `pr#22911 <https://github.com/ceph/ceph/pull/22911>`_, Jason Dillaman)
* qa/workunits/rbd: potential race in mirror disconnect test (`issue#23938 <http://tracker.ceph.com/issues/23938>`_, `pr#21869 <https://github.com/ceph/ceph/pull/21869>`_, Mykola Golub)
* radosgw-admin sync status improvements (`issue#20473 <http://tracker.ceph.com/issues/20473>`_, `pr#21908 <https://github.com/ceph/ceph/pull/21908>`_, lvshanchun, Casey Bodley)
* rbd: improve 'import-diff' corrupt input error messages (`issue#18844 <http://tracker.ceph.com/issues/18844>`_, `issue#23038 <http://tracker.ceph.com/issues/23038>`_, `pr#21316 <https://github.com/ceph/ceph/pull/21316>`_, PCzhangPC, songweibin, Jason Dillaman)
* rbd-mirror: ensure remote demotion is replayed locally (`issue#24009 <http://tracker.ceph.com/issues/24009>`_, `pr#22142 <https://github.com/ceph/ceph/pull/22142>`_, Jason Dillaman)
* rbd-nbd can deadlock in logging thread (`issue#23143 <http://tracker.ceph.com/issues/23143>`_, `pr#21705 <https://github.com/ceph/ceph/pull/21705>`_, Sage Weil)
* rbd: python bindings fixes and improvements (`issue#23609 <http://tracker.ceph.com/issues/23609>`_, `pr#21725 <https://github.com/ceph/ceph/pull/21725>`_, Ricardo Dias)
* rbd: [rbd-mirror] asok hook for image replayer not re-registered after bootstrap (`issue#23888 <http://tracker.ceph.com/issues/23888>`_, `pr#21726 <https://github.com/ceph/ceph/pull/21726>`_, Jason Dillaman)
* rbd: [rbd-mirror] local tag predecessor mirror uuid is incorrectly replaced with remote (`issue#23876 <http://tracker.ceph.com/issues/23876>`_, `pr#21741 <https://github.com/ceph/ceph/pull/21741>`_, Jason Dillaman)
* rbd: [rbd-mirror] potential deadlock when running asok 'flush' command (`issue#24141 <http://tracker.ceph.com/issues/24141>`_, `pr#22180 <https://github.com/ceph/ceph/pull/22180>`_, Mykola Golub)
* rbd: [rbd-mirror] potential races during PoolReplayer shut-down (`issue#24008 <http://tracker.ceph.com/issues/24008>`_, `pr#22172 <https://github.com/ceph/ceph/pull/22172>`_, Jason Dillaman)
* rgw: add buffering filter to compression for fetch_remote_obj (`issue#23547 <http://tracker.ceph.com/issues/23547>`_, `pr#21758 <https://github.com/ceph/ceph/pull/21758>`_, Casey Bodley)
* rgw: add configurable AWS-compat invalid range get behavior (`issue#24317 <http://tracker.ceph.com/issues/24317>`_, `pr#22302 <https://github.com/ceph/ceph/pull/22302>`_, Matt Benjamin)
* rgw: admin rest api shouldn't return error when getting user's stats if (`issue#23821 <http://tracker.ceph.com/issues/23821>`_, `pr#21661 <https://github.com/ceph/ceph/pull/21661>`_, Zhang Shaowen)
* rgw: Allow swift acls to be deleted (`issue#22897 <http://tracker.ceph.com/issues/22897>`_, `pr#22465 <https://github.com/ceph/ceph/pull/22465>`_, Marcus Watts)
* rgw: aws4 auth supports PutBucketRequestPayment (`issue#23803 <http://tracker.ceph.com/issues/23803>`_, `pr#21660 <https://github.com/ceph/ceph/pull/21660>`_, Casey Bodley)
* rgw: beast frontend can listen on multiple endpoints (`issue#22779 <http://tracker.ceph.com/issues/22779>`_, `pr#21568 <https://github.com/ceph/ceph/pull/21568>`_, Casey Bodley)
* rgw: Bucket lifecycles stick around after buckets are deleted (`issue#19632 <http://tracker.ceph.com/issues/19632>`_, `pr#22551 <https://github.com/ceph/ceph/pull/22551>`_, Wei Qiaomiao)
* rgw: Do not modify email if argument is not set (`issue#24142 <http://tracker.ceph.com/issues/24142>`_, `pr#22352 <https://github.com/ceph/ceph/pull/22352>`_, Volker Theile)
* rgw: do not reflect period if not current (`issue#22844 <http://tracker.ceph.com/issues/22844>`_, `pr#21735 <https://github.com/ceph/ceph/pull/21735>`_, Tianshan Qu)
* rgw: es module: set compression type correctly (`issue#22758 <http://tracker.ceph.com/issues/22758>`_, `pr#21736 <https://github.com/ceph/ceph/pull/21736>`_, Abhishek Lekshmanan)
* rgw_file: conditionally unlink handles when direct deleted (`issue#23299 <http://tracker.ceph.com/issues/23299>`_, `pr#21438 <https://github.com/ceph/ceph/pull/21438>`_, Matt Benjamin)
* rgw: fix bi_list to reset is_truncated flag if it skips entires (`issue#22721 <http://tracker.ceph.com/issues/22721>`_, `pr#21669 <https://github.com/ceph/ceph/pull/21669>`_, Orit Wasserman)
* rgw: fix 'copy part' without 'x-amz-copy-source-range' when compressi… (`issue#23196 <http://tracker.ceph.com/issues/23196>`_, `pr#22438 <https://github.com/ceph/ceph/pull/22438>`_, fang yuxiang)
* rgw: fix error handling for GET with ?torrent (`issue#23506 <http://tracker.ceph.com/issues/23506>`_, `pr#21674 <https://github.com/ceph/ceph/pull/21674>`_, Casey Bodley)
* rgw: fix use of libcurl with empty header values (`issue#23663 <http://tracker.ceph.com/issues/23663>`_, `pr#21738 <https://github.com/ceph/ceph/pull/21738>`_, Casey Bodley)
* rgw:lc: RGWPutLC return ERR_MALFORMED_XML  when missing <Rule> tag in… (`issue#21377 <http://tracker.ceph.com/issues/21377>`_, `pr#19884 <https://github.com/ceph/ceph/pull/19884>`_, Shasha Lu)
* rgw: making implicit_tenants backwards compatible (`issue#24348 <http://tracker.ceph.com/issues/24348>`_, `pr#22363 <https://github.com/ceph/ceph/pull/22363>`_, Marcus Watts)
* rgw: Misnamed S3 operation (`issue#24061 <http://tracker.ceph.com/issues/24061>`_, `pr#21917 <https://github.com/ceph/ceph/pull/21917>`_, xiangxiang)
* rgw: move all pool creation into rgw_init_ioctx (`issue#23480 <http://tracker.ceph.com/issues/23480>`_, `pr#21675 <https://github.com/ceph/ceph/pull/21675>`_, Casey Bodley)
* rgw: radosgw-admin should not use metadata cache for readonly commands (`issue#23468 <http://tracker.ceph.com/issues/23468>`_, `pr#21437 <https://github.com/ceph/ceph/pull/21437>`_, Orit Wasserman)
* rgw: raise log level on coroutine shutdown errors (`issue#23974 <http://tracker.ceph.com/issues/23974>`_, `pr#21792 <https://github.com/ceph/ceph/pull/21792>`_, Casey Bodley)
* rgw: return EINVAL if max_keys can not convert correctly (`issue#23586 <http://tracker.ceph.com/issues/23586>`_, `pr#21435 <https://github.com/ceph/ceph/pull/21435>`_, yuliyang)
* rgw: rgw_statfs should report the correct stats (`issue#22202 <http://tracker.ceph.com/issues/22202>`_, `pr#21724 <https://github.com/ceph/ceph/pull/21724>`_, Supriti Singh)
* rgw: trim all spaces inside a metadata value (`issue#23301 <http://tracker.ceph.com/issues/23301>`_, `pr#22177 <https://github.com/ceph/ceph/pull/22177>`_, Orit Wasserman)
* slow mon ops from osd_failure (`issue#24322 <http://tracker.ceph.com/issues/24322>`_, `pr#22568 <https://github.com/ceph/ceph/pull/22568>`_, Sage Weil)
* table of contents doesn't render for luminous/jewel docs (`issue#23780 <http://tracker.ceph.com/issues/23780>`_, `pr#21502 <https://github.com/ceph/ceph/pull/21502>`_, Alfredo Deza)
* test/librados: increase pgp_num along with pg_num (`issue#23763 <http://tracker.ceph.com/issues/23763>`_, `pr#21556 <https://github.com/ceph/ceph/pull/21556>`_, Kefu Chai)
* test/rgw: fix for bucket checkpoints (`issue#24212 <http://tracker.ceph.com/issues/24212>`_, `pr#22541 <https://github.com/ceph/ceph/pull/22541>`_, Casey Bodley)
* tests: filestore journal replay does not guard omap operations (`issue#22920 <http://tracker.ceph.com/issues/22920>`_, `pr#21547 <https://github.com/ceph/ceph/pull/21547>`_, Sage Weil)
* tools: ceph-disk: write log to /var/log/ceph not to /var/run/ceph (`issue#24041 <http://tracker.ceph.com/issues/24041>`_, `pr#21870 <https://github.com/ceph/ceph/pull/21870>`_, Kefu Chai)
* tools: ceph-fuse: getgroups failure causes exception (`issue#23446 <http://tracker.ceph.com/issues/23446>`_, `pr#21687 <https://github.com/ceph/ceph/pull/21687>`_, Jeff Layton)


v12.2.5 Luminous
================

This is the fifth bugfix release of Luminous v12.2.x long term stable
release series. This release contains a range of bug fixes across all
components of Ceph. We recommend all the users of 12.2.x series to
update.

Notable Changes
---------------

* MGR

  The ceph-rest-api command-line tool included in the ceph-mon
  package has been obsoleted by the MGR "restful" module. The
  ceph-rest-api tool is hereby declared deprecated and will be dropped
  in Mimic.

  The MGR "restful" module provides similar functionality via a "pass through"
  method. See http://docs.ceph.com/docs/luminous/mgr/restful for details.

* CephFS

  Upgrading an MDS cluster to 12.2.3+ will result in all active MDS
  exiting due to feature incompatibilities once an upgraded MDS comes
  online (even as standby). Operators may ignore the error messages
  and continue upgrading/restarting or follow this upgrade sequence:

  Reduce the number of ranks to 1 (`ceph fs set <fs_name> max_mds 1`),
  wait for all other MDS to deactivate, leaving the one active MDS,
  upgrade the single active MDS, then upgrade/start standbys. Finally,
  restore the previous max_mds.

  See also: https://tracker.ceph.com/issues/23172


Other Notable Changes
---------------------

* add --add-bucket and --move options to crushtool (`issue#23472 <http://tracker.ceph.com/issues/23472>`_, `issue#23471 <http://tracker.ceph.com/issues/23471>`_, `pr#21079 <https://github.com/ceph/ceph/pull/21079>`_, Kefu Chai)
* BlueStore.cc: _balance_bluefs_freespace: assert(0 == "allocate failed, wtf") (`issue#23063 <http://tracker.ceph.com/issues/23063>`_, `pr#21394 <https://github.com/ceph/ceph/pull/21394>`_, Igor Fedotov, xie xingguo, Sage Weil, Zac Medico)
* bluestore: correctly check all block devices to decide if journal is\_… (`issue#23173 <http://tracker.ceph.com/issues/23173>`_, `issue#23141 <http://tracker.ceph.com/issues/23141>`_, `pr#20651 <https://github.com/ceph/ceph/pull/20651>`_, Greg Farnum)
* bluestore: statfs available can go negative (`issue#23074 <http://tracker.ceph.com/issues/23074>`_, `pr#20554 <https://github.com/ceph/ceph/pull/20554>`_, Igor Fedotov, Sage Weil)
* build Debian installation packages failure (`issue#22856 <http://tracker.ceph.com/issues/22856>`_, `issue#22828 <http://tracker.ceph.com/issues/22828>`_, `pr#20250 <https://github.com/ceph/ceph/pull/20250>`_, Tone Zhang)
* build/ops: deb: move python-jinja2 dependency to mgr (`issue#22457 <http://tracker.ceph.com/issues/22457>`_, `pr#20748 <https://github.com/ceph/ceph/pull/20748>`_, Nathan Cutler)
* build/ops: deb: move python-jinja2 dependency to mgr (`issue#22457 <http://tracker.ceph.com/issues/22457>`_, `pr#21233 <https://github.com/ceph/ceph/pull/21233>`_, Nathan Cutler)
* build/ops: run-make-check.sh: fix SUSE support (`issue#22875 <http://tracker.ceph.com/issues/22875>`_, `issue#23178 <http://tracker.ceph.com/issues/23178>`_, `pr#20737 <https://github.com/ceph/ceph/pull/20737>`_, Nathan Cutler)
* cephfs-journal-tool: Fix Dumper destroyed before shutdown (`issue#22862 <http://tracker.ceph.com/issues/22862>`_, `issue#22734 <http://tracker.ceph.com/issues/22734>`_, `pr#20251 <https://github.com/ceph/ceph/pull/20251>`_, dongdong tao)
* ceph.in: print all matched commands if arg missing (`issue#22344 <http://tracker.ceph.com/issues/22344>`_, `issue#23186 <http://tracker.ceph.com/issues/23186>`_, `pr#20664 <https://github.com/ceph/ceph/pull/20664>`_, Luo Kexue, Kefu Chai)
* ceph-objectstore-tool command to trim the pg log (`issue#23242 <http://tracker.ceph.com/issues/23242>`_, `pr#20803 <https://github.com/ceph/ceph/pull/20803>`_, Josh Durgin, David Zafman)
* ceph osd force-create-pg cause all ceph-mon to crash and unable to come up again (`issue#22942 <http://tracker.ceph.com/issues/22942>`_, `pr#20399 <https://github.com/ceph/ceph/pull/20399>`_, Sage Weil)
* ceph-volume: adds raw device support to 'lvm list' (`issue#23140 <http://tracker.ceph.com/issues/23140>`_, `pr#20647 <https://github.com/ceph/ceph/pull/20647>`_, Andrew Schoen)
* ceph-volume: allow parallel creates (`issue#23757 <http://tracker.ceph.com/issues/23757>`_, `pr#21509 <https://github.com/ceph/ceph/pull/21509>`_, Theofilos Mouratidis)
* ceph-volume: allow skipping systemd interactions on activate/create (`issue#23678 <http://tracker.ceph.com/issues/23678>`_, `pr#21538 <https://github.com/ceph/ceph/pull/21538>`_, Alfredo Deza)
* ceph-volume: automatic VDO detection (`issue#23581 <http://tracker.ceph.com/issues/23581>`_, `pr#21505 <https://github.com/ceph/ceph/pull/21505>`_, Alfredo Deza)
* ceph-volume be resilient to $PATH issues (`pr#20716 <https://github.com/ceph/ceph/pull/20716>`_, Alfredo Deza)
* ceph-volume: fix action plugins path in tox (`pr#20923 <https://github.com/ceph/ceph/pull/20923>`_, Guillaume Abrioux)
* ceph-volume Implement an 'activate all' to help with dense servers or migrating OSDs (`pr#21533 <https://github.com/ceph/ceph/pull/21533>`_, Alfredo Deza)
* ceph-volume improve robustness when reloading vms in tests (`pr#21072 <https://github.com/ceph/ceph/pull/21072>`_, Alfredo Deza)
* ceph-volume lvm.activate error if no bluestore OSDs are found (`issue#23644 <http://tracker.ceph.com/issues/23644>`_, `pr#21335 <https://github.com/ceph/ceph/pull/21335>`_, Alfredo Deza)
* ceph-volume: Nits noticed while studying code (`pr#21565 <https://github.com/ceph/ceph/pull/21565>`_, Dan Mick)
* ceph-volume tests alleviate libvirt timeouts when reloading (`issue#23163 <http://tracker.ceph.com/issues/23163>`_, `pr#20754 <https://github.com/ceph/ceph/pull/20754>`_, Alfredo Deza)
* ceph-volume update man page for prepare/activate flags (`pr#21574 <https://github.com/ceph/ceph/pull/21574>`_, Alfredo Deza)
* ceph-volume: Using --readonly for {vg|pv|lv}s commands (`pr#21519 <https://github.com/ceph/ceph/pull/21519>`_, Erwan Velu)
* client: allow client to use caps that are revoked but not yet returned (`issue#23028 <http://tracker.ceph.com/issues/23028>`_, `issue#23314 <http://tracker.ceph.com/issues/23314>`_, `pr#20904 <https://github.com/ceph/ceph/pull/20904>`_, Jeff Layton)
* : Client:Fix readdir bug (`issue#22936 <http://tracker.ceph.com/issues/22936>`_, `pr#20356 <https://github.com/ceph/ceph/pull/20356>`_, dongdong tao)
* client: release revoking Fc after invalidate cache (`issue#22652 <http://tracker.ceph.com/issues/22652>`_, `pr#20342 <https://github.com/ceph/ceph/pull/20342>`_, "Yan, Zheng")
* Client: setattr should drop "Fs" rather than "As" for mtime and size (`issue#22935 <http://tracker.ceph.com/issues/22935>`_, `pr#20354 <https://github.com/ceph/ceph/pull/20354>`_, dongdong tao)
* client: use either dentry_invalidate_cb or remount_cb to invalidate k… (`issue#23355 <http://tracker.ceph.com/issues/23355>`_, `pr#20960 <https://github.com/ceph/ceph/pull/20960>`_, Zhi Zhang)
* cls/rbd: group_image_list incorrectly flagged as RW (`issue#23407 <http://tracker.ceph.com/issues/23407>`_, `issue#23388 <http://tracker.ceph.com/issues/23388>`_, `pr#20967 <https://github.com/ceph/ceph/pull/20967>`_, Jason Dillaman)
* cls/rgw: fix bi_log_iterate_entries return wrong truncated (`issue#22737 <http://tracker.ceph.com/issues/22737>`_, `issue#23225 <http://tracker.ceph.com/issues/23225>`_, `pr#21054 <https://github.com/ceph/ceph/pull/21054>`_, Tianshan Qu)
* cmake: rbd resource agent needs to be executable (`issue#22980 <http://tracker.ceph.com/issues/22980>`_, `pr#20617 <https://github.com/ceph/ceph/pull/20617>`_, Tim Bishop)
* common/dns_resolv.cc: Query for AAAA-record if ms_bind_ipv6 is True (`issue#23078 <http://tracker.ceph.com/issues/23078>`_, `issue#23174 <http://tracker.ceph.com/issues/23174>`_, `pr#20710 <https://github.com/ceph/ceph/pull/20710>`_, Wido den Hollander)
* common/ipaddr: Do not select link-local IPv6 addresses (`issue#21813 <http://tracker.ceph.com/issues/21813>`_, `pr#21111 <https://github.com/ceph/ceph/pull/21111>`_, Willem Jan Withagen)
* common: omit short option for id in help for clients (`issue#23156 <http://tracker.ceph.com/issues/23156>`_, `issue#23041 <http://tracker.ceph.com/issues/23041>`_, `pr#20654 <https://github.com/ceph/ceph/pull/20654>`_, Patrick Donnelly)
* common: should not check for VERSION_ID (`issue#23477 <http://tracker.ceph.com/issues/23477>`_, `issue#23478 <http://tracker.ceph.com/issues/23478>`_, `pr#21090 <https://github.com/ceph/ceph/pull/21090>`_, Kefu Chai, Shengjing Zhu)
* config: Change bluestore_cache_kv_max to type INT64 (`pr#20334 <https://github.com/ceph/ceph/pull/20334>`_, Zhi Zhang)
* Couldn't init storage provider (RADOS) (`issue#23349 <http://tracker.ceph.com/issues/23349>`_, `issue#22351 <http://tracker.ceph.com/issues/22351>`_, `pr#20896 <https://github.com/ceph/ceph/pull/20896>`_, Brad Hubbard)
* doc: Add missing pg states from doc (`issue#23113 <http://tracker.ceph.com/issues/23113>`_, `pr#20584 <https://github.com/ceph/ceph/pull/20584>`_, David Zafman)
* doc: outline upgrade procedure for mds cluster (`issue#23634 <http://tracker.ceph.com/issues/23634>`_, `issue#23568 <http://tracker.ceph.com/issues/23568>`_, `pr#21352 <https://github.com/ceph/ceph/pull/21352>`_, Patrick Donnelly)
* doc/rgw: add page for http frontend configuration (`issue#13523 <http://tracker.ceph.com/issues/13523>`_, `issue#22884 <http://tracker.ceph.com/issues/22884>`_, `pr#20242 <https://github.com/ceph/ceph/pull/20242>`_, Casey Bodley)
* doc: rgw: mention the civetweb support for binding to multiple ports (`issue#20942 <http://tracker.ceph.com/issues/20942>`_, `issue#23317 <http://tracker.ceph.com/issues/23317>`_, `pr#20906 <https://github.com/ceph/ceph/pull/20906>`_, Abhishek Lekshmanan)
* docs fix ceph-volume missing sub-commands (`pr#20691 <https://github.com/ceph/ceph/pull/20691>`_, Katie Holly, Yao Zongyou, David Galloway, Sage Weil, Alfredo Deza)
* doc: update man page to explain ceph-volume support bluestore (`issue#23142 <http://tracker.ceph.com/issues/23142>`_, `issue#22663 <http://tracker.ceph.com/issues/22663>`_, `pr#20679 <https://github.com/ceph/ceph/pull/20679>`_, lijing)
* Double free in rados_getxattrs_next (`issue#22940 <http://tracker.ceph.com/issues/22940>`_, `issue#22042 <http://tracker.ceph.com/issues/22042>`_, `pr#20358 <https://github.com/ceph/ceph/pull/20358>`_, Gu Zhongyan)
* fixes for openssl & libcurl (`issue#23239 <http://tracker.ceph.com/issues/23239>`_, `issue#23245 <http://tracker.ceph.com/issues/23245>`_, `issue#22951 <http://tracker.ceph.com/issues/22951>`_, `issue#23221 <http://tracker.ceph.com/issues/23221>`_, `issue#23203 <http://tracker.ceph.com/issues/23203>`_, `pr#20722 <https://github.com/ceph/ceph/pull/20722>`_, Marcus Watts, Abhishek Lekshmanan, Jesse Williamson)
* invalid JSON returned when querying pool parameters (`issue#23312 <http://tracker.ceph.com/issues/23312>`_, `issue#23200 <http://tracker.ceph.com/issues/23200>`_, `pr#20890 <https://github.com/ceph/ceph/pull/20890>`_, Chang Liu)
* is_qemu_running in qemu_rebuild_object_map.sh and qemu_dynamic_features.sh may return false positive (`issue#23524 <http://tracker.ceph.com/issues/23524>`_, `pr#21192 <https://github.com/ceph/ceph/pull/21192>`_, Mykola Golub)
* [journal] allocating a new tag after acquiring the lock should use on-disk committed position (`issue#23011 <http://tracker.ceph.com/issues/23011>`_, `issue#22945 <http://tracker.ceph.com/issues/22945>`_, `pr#20454 <https://github.com/ceph/ceph/pull/20454>`_, Jason Dillaman)
* journal: Message too long error when appending journal (`issue#23545 <http://tracker.ceph.com/issues/23545>`_, `issue#23526 <http://tracker.ceph.com/issues/23526>`_, `pr#21216 <https://github.com/ceph/ceph/pull/21216>`_, Mykola Golub)
* legal: remove doc license ambiguity (`issue#23410 <http://tracker.ceph.com/issues/23410>`_, `issue#23336 <http://tracker.ceph.com/issues/23336>`_, `pr#20988 <https://github.com/ceph/ceph/pull/20988>`_, Nathan Cutler)
* librados: make OPERATION_FULL_FORCE the default for rados_remove() (`issue#23114 <http://tracker.ceph.com/issues/23114>`_, `issue#22413 <http://tracker.ceph.com/issues/22413>`_, `pr#20585 <https://github.com/ceph/ceph/pull/20585>`_, Kefu Chai)
* librados/snap_set_diff: don't assert on empty snapset (`issue#23423 <http://tracker.ceph.com/issues/23423>`_, `pr#20991 <https://github.com/ceph/ceph/pull/20991>`_, Mykola Golub)
* librbd: potential crash if object map check encounters error (`issue#22857 <http://tracker.ceph.com/issues/22857>`_, `issue#22819 <http://tracker.ceph.com/issues/22819>`_, `pr#20253 <https://github.com/ceph/ceph/pull/20253>`_, Jason Dillaman)
* log: Fix AddressSanitizer: new-delete-type-mismatch (`issue#23324 <http://tracker.ceph.com/issues/23324>`_, `issue#23412 <http://tracker.ceph.com/issues/23412>`_, `pr#20998 <https://github.com/ceph/ceph/pull/20998>`_, Brad Hubbard)
* mds: add uptime to MDS status (`issue#23150 <http://tracker.ceph.com/issues/23150>`_, `pr#20626 <https://github.com/ceph/ceph/pull/20626>`_, Patrick Donnelly)
* mds: FAILED assert (p != active_requests.end()) in MDRequestRef MDCache::request_get(metareqid_t) (`issue#23154 <http://tracker.ceph.com/issues/23154>`_, `issue#23059 <http://tracker.ceph.com/issues/23059>`_, `pr#21176 <https://github.com/ceph/ceph/pull/21176>`_, "Yan, Zheng")
* mds: fix session reference leak (`issue#22821 <http://tracker.ceph.com/issues/22821>`_, `issue#22969 <http://tracker.ceph.com/issues/22969>`_, `pr#20432 <https://github.com/ceph/ceph/pull/20432>`_, "Yan, Zheng")
* mds: optimize getattr file size (`issue#23013 <http://tracker.ceph.com/issues/23013>`_, `issue#22925 <http://tracker.ceph.com/issues/22925>`_, `pr#20455 <https://github.com/ceph/ceph/pull/20455>`_, "Yan, Zheng")
* mgr: Backport recent prometheus exporter changes (`pr#20642 <https://github.com/ceph/ceph/pull/20642>`_, Jan Fajerski, Boris Ranto)
* mgr: Backport recent prometheus rgw changes (`pr#21492 <https://github.com/ceph/ceph/pull/21492>`_, Jan Fajerski, John Spray, Boris Ranto, Rubab-Syed)
* mgr/balancer: pool-specific optimization support and bug fixes (`pr#20359 <https://github.com/ceph/ceph/pull/20359>`_, xie xingguo)
* mgr: die on bind() failure (`issue#23175 <http://tracker.ceph.com/issues/23175>`_, `pr#20712 <https://github.com/ceph/ceph/pull/20712>`_, John Spray)
* mgr: fix MSG_MGR_MAP handling (`issue#23409 <http://tracker.ceph.com/issues/23409>`_, `pr#20973 <https://github.com/ceph/ceph/pull/20973>`_, Gu Zhongyan)
* mgr: prometheus: fix PG state names (`pr#21365 <https://github.com/ceph/ceph/pull/21365>`_, John Spray)
* mgr: prometheus: set metadata metrics value to '1' (#22717) (`pr#20254 <https://github.com/ceph/ceph/pull/20254>`_, Konstantin Shalygin)
* mgr: quieten logging on missing OSD stats (`issue#23224 <http://tracker.ceph.com/issues/23224>`_, `pr#21053 <https://github.com/ceph/ceph/pull/21053>`_, John Spray)
* mgr/zabbix: Backports to Luminous (`pr#20781 <https://github.com/ceph/ceph/pull/20781>`_, Wido den Hollander)
* mon: allow removal of tier of ec overwritable pool (`issue#22971 <http://tracker.ceph.com/issues/22971>`_, `issue#22754 <http://tracker.ceph.com/issues/22754>`_, `pr#20433 <https://github.com/ceph/ceph/pull/20433>`_, Patrick Donnelly)
* mon: ops get stuck in "resend forwarded message to leader" (`issue#22114 <http://tracker.ceph.com/issues/22114>`_, `issue#23077 <http://tracker.ceph.com/issues/23077>`_, `pr#21016 <https://github.com/ceph/ceph/pull/21016>`_, Kefu Chai, Greg Farnum)
* mon, osd: fix potential collided \*Up Set\* after PG remapping (`issue#23118 <http://tracker.ceph.com/issues/23118>`_, `pr#20829 <https://github.com/ceph/ceph/pull/20829>`_, xie xingguo)
* mon/OSDMonitor.cc: fix expected_num_objects interpret error (`issue#22530 <http://tracker.ceph.com/issues/22530>`_, `issue#23315 <http://tracker.ceph.com/issues/23315>`_, `pr#20907 <https://github.com/ceph/ceph/pull/20907>`_, Yang Honggang)
* mon: update PaxosService::cached_first_committed in PaxosService::may… (`issue#23626 <http://tracker.ceph.com/issues/23626>`_, `issue#11332 <http://tracker.ceph.com/issues/11332>`_, `pr#21328 <https://github.com/ceph/ceph/pull/21328>`_, Xuehan Xu, yupeng chen)
* msg/async: size of EventCenter::file_events should be greater than fd (`issue#23253 <http://tracker.ceph.com/issues/23253>`_, `issue#23306 <http://tracker.ceph.com/issues/23306>`_, `pr#20867 <https://github.com/ceph/ceph/pull/20867>`_, Yupeng Chen)
* Objecter: add ignore overlay flag if got redirect reply (`pr#20766 <https://github.com/ceph/ceph/pull/20766>`_, Ting Yi Lin)
* os/bluestore: avoid overhead of std::function in blob_t (`pr#20674 <https://github.com/ceph/ceph/pull/20674>`_, Radoslaw Zarzynski)
* os/bluestore: avoid unneeded BlobRefing in _do_read() (`pr#20675 <https://github.com/ceph/ceph/pull/20675>`_, Radoslaw Zarzynski)
* os/bluestore: backport fixes around _reap_collection (`pr#20964 <https://github.com/ceph/ceph/pull/20964>`_, Jianpeng Ma)
* os/bluestore: change the type of aio_t:res to long (`issue#23527 <http://tracker.ceph.com/issues/23527>`_, `issue#23544 <http://tracker.ceph.com/issues/23544>`_, `pr#21231 <https://github.com/ceph/ceph/pull/21231>`_, kungf)
* os/bluestore: _dump_onode() don't prolongate Onode anymore (`pr#20676 <https://github.com/ceph/ceph/pull/20676>`_, Radoslaw Zarzynski)
* os/bluestore: recalc_allocated() when decoding bluefs_fnode_t (`issue#23256 <http://tracker.ceph.com/issues/23256>`_, `issue#23212 <http://tracker.ceph.com/issues/23212>`_, `pr#20771 <https://github.com/ceph/ceph/pull/20771>`_, Jianpeng Ma, Igor Fedotov, Kefu Chai)
* os/bluestore: trim cache every 50ms (instead of 200ms) (`issue#23226 <http://tracker.ceph.com/issues/23226>`_, `pr#21059 <https://github.com/ceph/ceph/pull/21059>`_, Sage Weil)
* osd: add numpg_removing metric (`pr#20785 <https://github.com/ceph/ceph/pull/20785>`_, Sage Weil)
* osdc/Journaler: make sure flush() writes enough data (`issue#22967 <http://tracker.ceph.com/issues/22967>`_, `issue#22824 <http://tracker.ceph.com/issues/22824>`_, `pr#20431 <https://github.com/ceph/ceph/pull/20431>`_, "Yan, Zheng")
* osd: do not release_reserved_pushes when requeuing (`pr#21229 <https://github.com/ceph/ceph/pull/21229>`_, Sage Weil)
* osd: Fix assert when checking missing version (`issue#21218 <http://tracker.ceph.com/issues/21218>`_, `issue#23024 <http://tracker.ceph.com/issues/23024>`_, `pr#20495 <https://github.com/ceph/ceph/pull/20495>`_, David Zafman)
* osd: objecter sends out of sync with pg epochs for proxied ops (`issue#22123 <http://tracker.ceph.com/issues/22123>`_, `issue#23075 <http://tracker.ceph.com/issues/23075>`_, `pr#20609 <https://github.com/ceph/ceph/pull/20609>`_, Sage Weil)
* osd/OSDMap: skip out/crush-out osds (`pr#20840 <https://github.com/ceph/ceph/pull/20840>`_, xie xingguo)
* osd/osd_types: fix pg_pool_t encoding for hammer (`pr#21283 <https://github.com/ceph/ceph/pull/21283>`_, Sage Weil)
* osd: remove cost from mclock op queues; cost not handled well in dmcl… (`pr#21426 <https://github.com/ceph/ceph/pull/21426>`_, J. Eric Ivancich)
* osd: Remove partially created pg known as DNE (`issue#23160 <http://tracker.ceph.com/issues/23160>`_, `issue#21833 <http://tracker.ceph.com/issues/21833>`_, `pr#20668 <https://github.com/ceph/ceph/pull/20668>`_, David Zafman)
* osd: resend osd_pgtemp if it's not acked (`issue#23610 <http://tracker.ceph.com/issues/23610>`_, `issue#23630 <http://tracker.ceph.com/issues/23630>`_, `pr#21330 <https://github.com/ceph/ceph/pull/21330>`_, Kefu Chai)
* osd: treat successful and erroroneous writes the same for log trimming (`issue#23323 <http://tracker.ceph.com/issues/23323>`_, `issue#22050 <http://tracker.ceph.com/issues/22050>`_, `pr#20851 <https://github.com/ceph/ceph/pull/20851>`_, Josh Durgin)
* os/filestore: fix do_copy_range replay bug (`issue#23351 <http://tracker.ceph.com/issues/23351>`_, `issue#23298 <http://tracker.ceph.com/issues/23298>`_, `pr#20957 <https://github.com/ceph/ceph/pull/20957>`_, Sage Weil)
* parent blocks are still seen after a whole-object discard (`issue#23304 <http://tracker.ceph.com/issues/23304>`_, `issue#23285 <http://tracker.ceph.com/issues/23285>`_, `pr#20860 <https://github.com/ceph/ceph/pull/20860>`_, Ilya Dryomov, Jason Dillaman)
* PendingReleaseNotes: add note about upgrading MDS (`issue#23414 <http://tracker.ceph.com/issues/23414>`_, `pr#21001 <https://github.com/ceph/ceph/pull/21001>`_, Patrick Donnelly)
* : qa: adjust cephfs full test for kclient (`issue#22966 <http://tracker.ceph.com/issues/22966>`_, `issue#22886 <http://tracker.ceph.com/issues/22886>`_, `pr#20417 <https://github.com/ceph/ceph/pull/20417>`_, "Yan, Zheng")
* qa: ignore io pause warnings in mds-full test (`issue#23062 <http://tracker.ceph.com/issues/23062>`_, `issue#22990 <http://tracker.ceph.com/issues/22990>`_, `pr#20525 <https://github.com/ceph/ceph/pull/20525>`_, Patrick Donnelly)
* qa: ignore MON_DOWN while thrashing mons (`issue#23061 <http://tracker.ceph.com/issues/23061>`_, `pr#20523 <https://github.com/ceph/ceph/pull/20523>`_, Patrick Donnelly)
* qa/rgw: remove some civetweb overrides for beast testing (`issue#23002 <http://tracker.ceph.com/issues/23002>`_, `issue#23176 <http://tracker.ceph.com/issues/23176>`_, `pr#20736 <https://github.com/ceph/ceph/pull/20736>`_, Casey Bodley)
* qa: src/test/libcephfs/test.cc:376: Expected: (len) > (0), actual: -34 vs 0 (`issue#22383 <http://tracker.ceph.com/issues/22383>`_, `issue#22221 <http://tracker.ceph.com/issues/22221>`_, `pr#21173 <https://github.com/ceph/ceph/pull/21173>`_, Patrick Donnelly)
* qa: synchronize kcephfs suites with fs/multimds (`issue#22891 <http://tracker.ceph.com/issues/22891>`_, `issue#22627 <http://tracker.ceph.com/issues/22627>`_, `pr#20302 <https://github.com/ceph/ceph/pull/20302>`_, Patrick Donnelly)
* qa/tests - added tag: v12.2.2 to be used by client.1 (`pr#21452 <https://github.com/ceph/ceph/pull/21452>`_, Yuri Weinstein)
* qa/tests - Change machine type from 'vps' to 'ovh' as 'vps' does not … (`pr#21031 <https://github.com/ceph/ceph/pull/21031>`_, Yuri Weinstein)
* qa/workunits/rados/test-upgrade-to-mimic.sh: fix tee output (`pr#21506 <https://github.com/ceph/ceph/pull/21506>`_, Sage Weil)
* qa/workunits/rbd: switch devstack tempest to 17.2.0 tag (`issue#23177 <http://tracker.ceph.com/issues/23177>`_, `issue#22961 <http://tracker.ceph.com/issues/22961>`_, `pr#20715 <https://github.com/ceph/ceph/pull/20715>`_, Jason Dillaman)
* radosgw-admin data sync run crashes (`issue#23180 <http://tracker.ceph.com/issues/23180>`_, `pr#20762 <https://github.com/ceph/ceph/pull/20762>`_, lvshanchun)
* rbd-mirror: fix potential infinite loop when formatting status message (`issue#22964 <http://tracker.ceph.com/issues/22964>`_, `issue#22932 <http://tracker.ceph.com/issues/22932>`_, `pr#20416 <https://github.com/ceph/ceph/pull/20416>`_, Mykola Golub)
* rbd-nbd: fix ebusy when do map (`issue#23542 <http://tracker.ceph.com/issues/23542>`_, `issue#23528 <http://tracker.ceph.com/issues/23528>`_, `pr#21230 <https://github.com/ceph/ceph/pull/21230>`_, Li Wang)
* rgw: add radosgw-admin sync error trim to trim sync error log (`issue#23302 <http://tracker.ceph.com/issues/23302>`_, `pr#20859 <https://github.com/ceph/ceph/pull/20859>`_, fang yuxiang)
* rgw: add xml output header in RGWCopyObj_ObjStore_S3 response msg (`issue#22416 <http://tracker.ceph.com/issues/22416>`_, `issue#22635 <http://tracker.ceph.com/issues/22635>`_, `pr#19883 <https://github.com/ceph/ceph/pull/19883>`_, Enming Zhang)
* rgw: Admin API Support for bucket quota change (`issue#23357 <http://tracker.ceph.com/issues/23357>`_, `issue#21811 <http://tracker.ceph.com/issues/21811>`_, `pr#20885 <https://github.com/ceph/ceph/pull/20885>`_, Jeegn Chen)
* rgw: allow beast frontend to listen on specific IP address (`issue#22858 <http://tracker.ceph.com/issues/22858>`_, `issue#22778 <http://tracker.ceph.com/issues/22778>`_, `pr#20252 <https://github.com/ceph/ceph/pull/20252>`_, Yuan Zhou)
* rgw: can't download object with range when compression enabled (`issue#23146 <http://tracker.ceph.com/issues/23146>`_, `issue#23179 <http://tracker.ceph.com/issues/23179>`_, `issue#22852 <http://tracker.ceph.com/issues/22852>`_, `pr#20741 <https://github.com/ceph/ceph/pull/20741>`_, fang yuxiang)
* rgw: data sync of versioned objects, note updating bi marker (`issue#23025 <http://tracker.ceph.com/issues/23025>`_, `pr#21214 <https://github.com/ceph/ceph/pull/21214>`_, Yehuda Sadeh)
* RGW doesn't check time skew in auth v4 http header request (`issue#23252 <http://tracker.ceph.com/issues/23252>`_, `issue#22766 <http://tracker.ceph.com/issues/22766>`_, `issue#22439 <http://tracker.ceph.com/issues/22439>`_, `issue#22418 <http://tracker.ceph.com/issues/22418>`_, `pr#20072 <https://github.com/ceph/ceph/pull/20072>`_, Bingyin Zhang, Casey Bodley)
* rgw_file: avoid evaluating nullptr for readdir offset (`issue#22889 <http://tracker.ceph.com/issues/22889>`_, `pr#20345 <https://github.com/ceph/ceph/pull/20345>`_, Matt Benjamin)
* rgw: fix crash with rgw_run_sync_thread false (`issue#23318 <http://tracker.ceph.com/issues/23318>`_, `issue#20448 <http://tracker.ceph.com/issues/20448>`_, `pr#20932 <https://github.com/ceph/ceph/pull/20932>`_, Orit Wasserman)
* rgw: fix memory fragmentation problem reading data from client (`issue#23347 <http://tracker.ceph.com/issues/23347>`_, `pr#20953 <https://github.com/ceph/ceph/pull/20953>`_, Marcus Watts)
* rgw: fix mutlisite read-write issues (`issue#23690 <http://tracker.ceph.com/issues/23690>`_, `issue#22804 <http://tracker.ceph.com/issues/22804>`_, `pr#21390 <https://github.com/ceph/ceph/pull/21390>`_, Niu Pengju)
* rgw: fix the max-uploads parameter not work (`issue#23020 <http://tracker.ceph.com/issues/23020>`_, `issue#22825 <http://tracker.ceph.com/issues/22825>`_, `pr#20476 <https://github.com/ceph/ceph/pull/20476>`_, Xin Liao)
* rgw_log, rgw_file: account for new required envvars (`issue#23192 <http://tracker.ceph.com/issues/23192>`_, `issue#21942 <http://tracker.ceph.com/issues/21942>`_, `pr#20672 <https://github.com/ceph/ceph/pull/20672>`_, Matt Benjamin)
* rgw: log the right http status code in civetweb frontend's access log (`issue#22812 <http://tracker.ceph.com/issues/22812>`_, `issue#22538 <http://tracker.ceph.com/issues/22538>`_, `pr#20157 <https://github.com/ceph/ceph/pull/20157>`_, Yao Zongyou)
* rgw: parse old rgw_obj with namespace correctly (`issue#23102 <http://tracker.ceph.com/issues/23102>`_, `issue#22982 <http://tracker.ceph.com/issues/22982>`_, `pr#20586 <https://github.com/ceph/ceph/pull/20586>`_, Yehuda Sadeh)
* rgw recalculate stats option added (`issue#23691 <http://tracker.ceph.com/issues/23691>`_, `issue#23720 <http://tracker.ceph.com/issues/23720>`_, `issue#23335 <http://tracker.ceph.com/issues/23335>`_, `issue#23322 <http://tracker.ceph.com/issues/23322>`_, `pr#21393 <https://github.com/ceph/ceph/pull/21393>`_, Abhishek Lekshmanan)
* rgw: reject encrypted object COPY before supported (`issue#23232 <http://tracker.ceph.com/issues/23232>`_, `issue#23346 <http://tracker.ceph.com/issues/23346>`_, `pr#20937 <https://github.com/ceph/ceph/pull/20937>`_, Jeegn Chen)
* rgw: rgw: reshard cancel command should clear bucket resharding flag (`issue#21619 <http://tracker.ceph.com/issues/21619>`_, `pr#21389 <https://github.com/ceph/ceph/pull/21389>`_, Orit Wasserman)
* rgw: s3website error handler uses original object name (`issue#23201 <http://tracker.ceph.com/issues/23201>`_, `issue#23310 <http://tracker.ceph.com/issues/23310>`_, `pr#20889 <https://github.com/ceph/ceph/pull/20889>`_, Casey Bodley)
* rgw: upldate the max-buckets when the quota is uploaded (`issue#23022 <http://tracker.ceph.com/issues/23022>`_, `pr#20477 <https://github.com/ceph/ceph/pull/20477>`_, zhaokun)
* rgw: usage log fixes (`issue#23686 <http://tracker.ceph.com/issues/23686>`_, `issue#23758 <http://tracker.ceph.com/issues/23758>`_, `pr#21388 <https://github.com/ceph/ceph/pull/21388>`_, Yehuda Sadeh, Greg Farnum, Robin H. Johnson)
* rocksdb: incorporate the fix in RocksDB for no fast CRC32 path (`issue#22534 <http://tracker.ceph.com/issues/22534>`_, `pr#20825 <https://github.com/ceph/ceph/pull/20825>`_, Radoslaw Zarzynski)
* scrub errors not cleared on replicas can cause inconsistent pg state when replica takes over primary (`issue#23267 <http://tracker.ceph.com/issues/23267>`_, `pr#21103 <https://github.com/ceph/ceph/pull/21103>`_, David Zafman)
* snapmapper inconsistency, crash on luminous (`issue#23500 <http://tracker.ceph.com/issues/23500>`_, `pr#21118 <https://github.com/ceph/ceph/pull/21118>`_, Sage Weil)
* Special scrub handling of hinfo_key errors (`issue#23654 <http://tracker.ceph.com/issues/23654>`_, `issue#23428 <http://tracker.ceph.com/issues/23428>`_, `issue#23364 <http://tracker.ceph.com/issues/23364>`_, `pr#21397 <https://github.com/ceph/ceph/pull/21397>`_, David Zafman)
* src: s/--use-wheel// (`pr#21177 <https://github.com/ceph/ceph/pull/21177>`_, Kefu Chai)
* systemd: Wait 10 seconds before restarting ceph-mgr (`issue#23083 <http://tracker.ceph.com/issues/23083>`_, `issue#23101 <http://tracker.ceph.com/issues/23101>`_, `pr#20604 <https://github.com/ceph/ceph/pull/20604>`_, Wido den Hollander)
* test_admin_socket.sh may fail on wait_for_clean (`issue#23507 <http://tracker.ceph.com/issues/23507>`_, `pr#21124 <https://github.com/ceph/ceph/pull/21124>`_, Mykola Golub)
* test/ceph-disk: specify the python used for creating venv (`issue#23281 <http://tracker.ceph.com/issues/23281>`_, `pr#20817 <https://github.com/ceph/ceph/pull/20817>`_, Kefu Chai)
* TestLibRBD.RenameViaLockOwner may still fail with -ENOENT (`issue#23152 <http://tracker.ceph.com/issues/23152>`_, `issue#23068 <http://tracker.ceph.com/issues/23068>`_, `pr#20628 <https://github.com/ceph/ceph/pull/20628>`_, Mykola Golub)
* test/librbd: utilize unique pool for cache tier testing (`issue#23064 <http://tracker.ceph.com/issues/23064>`_, `issue#11502 <http://tracker.ceph.com/issues/11502>`_, `pr#20550 <https://github.com/ceph/ceph/pull/20550>`_, Jason Dillaman)
* test/pybind/test_rbd: allow v1 images for testing (`pr#21471 <https://github.com/ceph/ceph/pull/21471>`_, Sage Weil)
* test: Replace bc command with printf command (`pr#21015 <https://github.com/ceph/ceph/pull/21015>`_, David Zafman)
* tests: drop upgrade/jewel-x/point-to-point-x in luminous and master (`issue#23159 <http://tracker.ceph.com/issues/23159>`_, `issue#22888 <http://tracker.ceph.com/issues/22888>`_, `pr#20641 <https://github.com/ceph/ceph/pull/20641>`_, Nathan Cutler)
* tests: ENGINE Error in 'start' listener <bound  in rados (`issue#23606 <http://tracker.ceph.com/issues/23606>`_, `pr#21307 <https://github.com/ceph/ceph/pull/21307>`_, John Spray)
* tests: rgw: swift tests target ceph-luminous branch (`pr#21048 <https://github.com/ceph/ceph/pull/21048>`_, Nathan Cutler)
* tests: unittest_pglog timeout (`issue#23522 <http://tracker.ceph.com/issues/23522>`_, `issue#23504 <http://tracker.ceph.com/issues/23504>`_, `pr#21134 <https://github.com/ceph/ceph/pull/21134>`_, Nathan Cutler)
* Update mgr/restful documentation (`issue#23230 <http://tracker.ceph.com/issues/23230>`_, `pr#20725 <https://github.com/ceph/ceph/pull/20725>`_, Boris Ranto)

v12.2.4 Luminous
================

This is the fourth bugfix release of Luminous v12.2.x long term stable release
series. This was primarily intended to fix a few build, ceph-volume/ceph-disk
and RGW issues. We recommend all the users of 12.2.x series to update.

Notable Changes
---------------

* ceph-volume: adds support to zap encrypted devices (`issue#22878 <http://tracker.ceph.com/issues/22878>`_, `pr#20545 <https://github.com/ceph/ceph/pull/20545>`_, Andrew Schoen)
* ceph-volume: log the current running command for easier debugging (`issue#23004 <http://tracker.ceph.com/issues/23004>`_, `pr#20597 <https://github.com/ceph/ceph/pull/20597>`_, Andrew Schoen)
* ceph-volume: warn on mix of filestore and bluestore flags (`issue#23003 <http://tracker.ceph.com/issues/23003>`_, `pr#20568 <https://github.com/ceph/ceph/pull/20568>`_, Alfredo Deza)
* cmake: check bootstrap.sh instead before downloading boost (`issue#23071 <http://tracker.ceph.com/issues/23071>`_, `pr#20515 <https://github.com/ceph/ceph/pull/20515>`_, Kefu Chai)
* core: Backport of cache manipulation: issues #22603 and #22604 (`issue#22604 <http://tracker.ceph.com/issues/22604>`_, `issue#22603 <http://tracker.ceph.com/issues/22603>`_, `pr#20353 <https://github.com/ceph/ceph/pull/20353>`_, Adam C. Emerson)
* core: last-stat-seq returns 0 because osd stats are cleared (`issue#23093 <http://tracker.ceph.com/issues/23093>`_, `pr#20548 <https://github.com/ceph/ceph/pull/20548>`_, Sage Weil, David Zafman)
* core: Snapset inconsistency is detected with its own error (`issue#22996 <http://tracker.ceph.com/issues/22996>`_, `pr#20501 <https://github.com/ceph/ceph/pull/20501>`_, David Zafman)
* rgw:  make init env methods return an error (`issue#23039 <http://tracker.ceph.com/issues/23039>`_, `pr#20564 <https://github.com/ceph/ceph/pull/20564>`_, Abhishek Lekshmanan)
* rgw: parse old rgw_obj with namespace correctly (`issue#22982 <http://tracker.ceph.com/issues/22982>`_, `pr#20566 <https://github.com/ceph/ceph/pull/20566>`_, Yehuda Sadeh)
* rgw: return valid Location element, CompleteMultipartUpload (`issue#22655 <http://tracker.ceph.com/issues/22655>`_, `pr#20266 <https://github.com/ceph/ceph/pull/20266>`_, Matt Benjamin)
* rgw: URL-decode S3 and Swift object-copy URLs (`issue#22121 <http://tracker.ceph.com/issues/22121>`_, `issue#22729 <http://tracker.ceph.com/issues/22729>`_, `pr#20236 <https://github.com/ceph/ceph/pull/20236>`_, Malcolm Lee, Matt Benjamin)
* rgw: use explicit index pool placement (`issue#22928 <http://tracker.ceph.com/issues/22928>`_, `pr#20565 <https://github.com/ceph/ceph/pull/20565>`_, Yehuda Sadeh)
* tools: ceph-disk: v12.2.2 unable to create bluestore osd using ceph-disk (`issue#22354 <http://tracker.ceph.com/issues/22354>`_, `pr#20563 <https://github.com/ceph/ceph/pull/20563>`_, Kefu Chai)
* tools: ceph-objectstore-tool: "$OBJ get-omaphdr" and "$OBJ list-omap" scan all pgs instead of using specific pg (`issue#21327 <http://tracker.ceph.com/issues/21327>`_, `pr#20283 <https://github.com/ceph/ceph/pull/20283>`_, David Zafman)


v12.2.3 Luminous
================

This is the third bugfix release of Luminous v12.2.x long term stable release
series. It contains a range of bug fixes and a few features across Bluestore,
CephFS, RBD & RGW. We recommend all the users of 12.2.x series update.

Notable Changes
---------------

- *CephFS*:

  * The CephFS client now checks for older kernels' inability to reliably clear
    dentries from the kernel dentry cache. The new option
    client_die_on_failed_dentry_invalidate (default: true) may be turned off to
    allow the client to proceed (dangerous!).


Other Notable Changes
---------------------
* bluestore: do not crash on over-large objects (`issue#22161 <http://tracker.ceph.com/issues/22161>`_, `pr#19630 <https://github.com/ceph/ceph/pull/19630>`_, Sage Weil)
* bluestore: OSD crash on boot with assert caused by Bluefs on flush write (`issue#21932 <http://tracker.ceph.com/issues/21932>`_, `pr#19047 <https://github.com/ceph/ceph/pull/19047>`_, Jianpeng Ma)
* build/ops: ceph-base symbols not stripped in debs (`issue#22640 <http://tracker.ceph.com/issues/22640>`_, `pr#19969 <https://github.com/ceph/ceph/pull/19969>`_, Sage Weil)
* build/ops: ceph-conf: dump parsed config in plain text or as json (`issue#21862 <http://tracker.ceph.com/issues/21862>`_, `pr#18842 <https://github.com/ceph/ceph/pull/18842>`_, Piotr Dałek)
* build/ops: ceph-mgr dashboard has dependency on python-jinja2 (`issue#22457 <http://tracker.ceph.com/issues/22457>`_, `pr#19865 <https://github.com/ceph/ceph/pull/19865>`_, John Spray)
* build/ops: ceph-volume fails when centos7 image doesn't have lvm2 installed (`issue#22443 <http://tracker.ceph.com/issues/22443>`_, `issue#22217 <http://tracker.ceph.com/issues/22217>`_, `pr#20215 <https://github.com/ceph/ceph/pull/20215>`_, Nathan Cutler, Theofilos Mouratidis)
* build/ops: Default kernel.pid_max is easily exceeded during recovery on high OSD-count system (`issue#21929 <http://tracker.ceph.com/issues/21929>`_, `pr#19133 <https://github.com/ceph/ceph/pull/19133>`_, David Disseldorp, Kefu Chai)
* build/ops: install-deps.sh: revert gcc to the one shipped by distro (`issue#22220 <http://tracker.ceph.com/issues/22220>`_, `pr#19680 <https://github.com/ceph/ceph/pull/19680>`_, Kefu Chai)
* build/ops: luminous build fails with --without-radosgw (`issue#22321 <http://tracker.ceph.com/issues/22321>`_, `pr#19483 <https://github.com/ceph/ceph/pull/19483>`_, Jason Dillaman)
* build/ops: move ceph-\*-tool binaries out of ceph-test subpackage (`issue#22319 <http://tracker.ceph.com/issues/22319>`_, `issue#21762 <http://tracker.ceph.com/issues/21762>`_, `pr#19355 <https://github.com/ceph/ceph/pull/19355>`_, liuchang0812, Nathan Cutler, Kefu Chai, Sage Weil)
* build.ops: rpm: adjust ceph-{osdomap,kvstore,monstore}-tool feature move (`issue#22558 <http://tracker.ceph.com/issues/22558>`_, `pr#19839 <https://github.com/ceph/ceph/pull/19839>`_, Kefu Chai)
* ceph: cluster [ERR] Unhandled exception from module 'balancer' while running on mgr.x: 'NoneType' object has no attribute 'iteritems'" in cluster log (`issue#22090 <http://tracker.ceph.com/issues/22090>`_, `pr#19023 <https://github.com/ceph/ceph/pull/19023>`_, Sage Weil)
* cephfs: cephfs-journal-tool: add "set pool_id" option (`issue#22631 <http://tracker.ceph.com/issues/22631>`_, `pr#20085 <https://github.com/ceph/ceph/pull/20085>`_, dongdong tao)
* cephfs: cephfs-journal-tool: tool would miss to report some invalid range (`issue#22459 <http://tracker.ceph.com/issues/22459>`_, `pr#19626 <https://github.com/ceph/ceph/pull/19626>`_, dongdong tao)
* cephfs: cephfs: potential adjust failure in lru_expire (`issue#22458 <http://tracker.ceph.com/issues/22458>`_, `pr#19627 <https://github.com/ceph/ceph/pull/19627>`_, dongdong tao)
* cephfs: "ceph tell mds" commands result in "File exists" errors on client admin socket (`issue#21406 <http://tracker.ceph.com/issues/21406>`_, `issue#21967 <http://tracker.ceph.com/issues/21967>`_, `pr#18831 <https://github.com/ceph/ceph/pull/18831>`_, Patrick Donnelly)
* cephfs: client: anchor Inode while trimming caps (`issue#22157 <http://tracker.ceph.com/issues/22157>`_, `pr#19105 <https://github.com/ceph/ceph/pull/19105>`_, Patrick Donnelly)
* cephfs: client: avoid recursive lock in ll_get_vino (`issue#22629 <http://tracker.ceph.com/issues/22629>`_, `pr#20086 <https://github.com/ceph/ceph/pull/20086>`_, dongdong tao)
* cephfs: client: dual client segfault with racing ceph_shutdown (`issue#21512 <http://tracker.ceph.com/issues/21512>`_, `issue#20988 <http://tracker.ceph.com/issues/20988>`_, `pr#20082 <https://github.com/ceph/ceph/pull/20082>`_, Jeff Layton)
* cephfs: client: implement delegation support in userland cephfs (`issue#18490 <http://tracker.ceph.com/issues/18490>`_, `pr#19480 <https://github.com/ceph/ceph/pull/19480>`_, Jeff Layton)
* cephfs: client: quit on failed remount during dentry invalidate test #19218 (`issue#22269 <http://tracker.ceph.com/issues/22269>`_, `pr#19370 <https://github.com/ceph/ceph/pull/19370>`_, Patrick Donnelly)
* cephfs: List of filesystems does not get refreshed after a filesystem deletion (`issue#21599 <http://tracker.ceph.com/issues/21599>`_, `pr#18730 <https://github.com/ceph/ceph/pull/18730>`_, John Spray)
* cephfs: MDS : Avoid the assert failure when the inode for the cap_export from other… (`issue#22610 <http://tracker.ceph.com/issues/22610>`_, `pr#20300 <https://github.com/ceph/ceph/pull/20300>`_, Jianyu Li)
* cephfs: MDSMonitor: monitor gives constant "is now active in filesystem cephfs as rank" cluster log info messages (`issue#21959 <http://tracker.ceph.com/issues/21959>`_, `pr#19055 <https://github.com/ceph/ceph/pull/19055>`_, Patrick Donnelly)
* cephfs: racy is_mounted() checks in libcephfs (`issue#21025 <http://tracker.ceph.com/issues/21025>`_, `pr#17875 <https://github.com/ceph/ceph/pull/17875>`_, Jeff Layton)
* cephfs: src/mds/MDCache.cc: 7421: FAILED assert(CInode::count() == inode_map.size() + snap_inode_map.size()) (`issue#21928 <http://tracker.ceph.com/issues/21928>`_, `pr#18912 <https://github.com/ceph/ceph/pull/18912>`_, "Yan, Zheng")
* cephfs: vstart_runner: fixes for recent cephfs changes (`issue#22526 <http://tracker.ceph.com/issues/22526>`_, `pr#19829 <https://github.com/ceph/ceph/pull/19829>`_, Patrick Donnelly)
* ceph-volume: adds a --destroy flag to ceph-volume lvm zap (`issue#22653 <http://tracker.ceph.com/issues/22653>`_, `pr#20240 <https://github.com/ceph/ceph/pull/20240>`_, Andrew Schoen)
* ceph-volume: adds success messages for lvm prepare/activate/create (`issue#22307 <http://tracker.ceph.com/issues/22307>`_, `pr#20238 <https://github.com/ceph/ceph/pull/20238>`_, Andrew Schoen)
* ceph-volume: dmcrypt support for lvm (`issue#22619 <http://tracker.ceph.com/issues/22619>`_, `pr#20241 <https://github.com/ceph/ceph/pull/20241>`_, Alfredo Deza)
* ceph-volume dmcrypt support for simple (`issue#22620 <http://tracker.ceph.com/issues/22620>`_, `pr#20350 <https://github.com/ceph/ceph/pull/20350>`_, Andrew Schoen, Alfredo Deza)
* ceph-volume: do not use --key during mkfs (`issue#22283 <http://tracker.ceph.com/issues/22283>`_, `pr#20244 <https://github.com/ceph/ceph/pull/20244>`_, Kefu Chai, Sage Weil)
* ceph-volume: fix usage of the --osd-id flag (`issue#22642 <http://tracker.ceph.com/issues/22642>`_, `issue#22836 <http://tracker.ceph.com/issues/22836>`_, `pr#20323 <https://github.com/ceph/ceph/pull/20323>`_, Andrew Schoen)
* ceph-volume Format correctly when vg/lv cannot be used (`issue#22299 <http://tracker.ceph.com/issues/22299>`_, `pr#19527 <https://github.com/ceph/ceph/pull/19527>`_, Alfredo Deza)
* ceph-volume handle inline comments in the ceph.conf file (`issue#22297 <http://tracker.ceph.com/issues/22297>`_, `pr#19532 <https://github.com/ceph/ceph/pull/19532>`_, Alfredo Deza)
* ceph-volume: handle leading whitespace/tabs in ceph.conf (`issue#22280 <http://tracker.ceph.com/issues/22280>`_, `pr#19526 <https://github.com/ceph/ceph/pull/19526>`_, Alfredo Deza)
* ceph-volume: lvm zap will unmount osd paths used by zapped devices (`issue#22876 <http://tracker.ceph.com/issues/22876>`_, `pr#20438 <https://github.com/ceph/ceph/pull/20438>`_, Andrew Schoen)
* ceph-volume: removed the explicit use of sudo (`issue#22282 <http://tracker.ceph.com/issues/22282>`_, `pr#19525 <https://github.com/ceph/ceph/pull/19525>`_, Andrew Schoen)
* ceph-volume rollback on failed OSD prepare/create (`issue#22281 <http://tracker.ceph.com/issues/22281>`_, `pr#20237 <https://github.com/ceph/ceph/pull/20237>`_, Alfredo Deza)
* ceph-volume should be able to handle multiple LVM (VG/LV) tags (`issue#22305 <http://tracker.ceph.com/issues/22305>`_, `pr#19528 <https://github.com/ceph/ceph/pull/19528>`_, Alfredo Deza)
* ceph-volume use realpath when checking mounts (`issue#22988 <http://tracker.ceph.com/issues/22988>`_, `pr#20429 <https://github.com/ceph/ceph/pull/20429>`_, Alfredo Deza)
* ceph-volume: warn on missing ceph.conf file (`issue#22326 <http://tracker.ceph.com/issues/22326>`_, `pr#19530 <https://github.com/ceph/ceph/pull/19530>`_, Alfredo Deza)
* common: compute SimpleLRU's size with contents.size() instead of lru.… (`issue#22613 <http://tracker.ceph.com/issues/22613>`_, `pr#19977 <https://github.com/ceph/ceph/pull/19977>`_, Xuehan Xu)
* config: lower default omap entries recovered at once (`issue#21897 <http://tracker.ceph.com/issues/21897>`_, `pr#19928 <https://github.com/ceph/ceph/pull/19928>`_, Josh Durgin)
* core: backoff causes out of order op (`issue#21407 <http://tracker.ceph.com/issues/21407>`_, `pr#18747 <https://github.com/ceph/ceph/pull/18747>`_, Sage Weil)
* core: common/throttle: start using 64-bit values (`issue#22539 <http://tracker.ceph.com/issues/22539>`_, `pr#19995 <https://github.com/ceph/ceph/pull/19995>`_, Igor Fedotov)
* core: fix broken use of streamstream::rdbuf() (`issue#22715 <http://tracker.ceph.com/issues/22715>`_, `pr#20042 <https://github.com/ceph/ceph/pull/20042>`_, Sage Weil)
* core: possible deadlock in various maintenance operations (`issue#22120 <http://tracker.ceph.com/issues/22120>`_, `pr#19123 <https://github.com/ceph/ceph/pull/19123>`_, Jason Dillaman)
* core: _read_bdev_label unable to decode label at offset (`issue#22285 <http://tracker.ceph.com/issues/22285>`_, `pr#20326 <https://github.com/ceph/ceph/pull/20326>`_, Sage Weil)
* core: rocksdb: fixes early metadata spill over to slow device in (`issue#22264 <http://tracker.ceph.com/issues/22264>`_, `pr#19257 <https://github.com/ceph/ceph/pull/19257>`_, Igor Fedotov)
* core: Various odd clog messages for mons (`issue#22082 <http://tracker.ceph.com/issues/22082>`_, `pr#19031 <https://github.com/ceph/ceph/pull/19031>`_, John Spray)
* crush: balancer crush-compat sends "foo" command (`issue#22361 <http://tracker.ceph.com/issues/22361>`_, `pr#19555 <https://github.com/ceph/ceph/pull/19555>`_, John Spray)
* doc: crush_ruleset is invalid command in luminous (`issue#20559 <http://tracker.ceph.com/issues/20559>`_, `pr#19446 <https://github.com/ceph/ceph/pull/19446>`_, Nathan Cutler)
* doc: doc/rbd: tweaks for the LIO iSCSI gateway (`issue#21763 <http://tracker.ceph.com/issues/21763>`_, `pr#20213 <https://github.com/ceph/ceph/pull/20213>`_, Ashish Singh, Mike Christie, Jason Dillaman)
* doc: man page for mount.fuse.ceph (`issue#21539 <http://tracker.ceph.com/issues/21539>`_, `issue#22595 <http://tracker.ceph.com/issues/22595>`_, `pr#19449 <https://github.com/ceph/ceph/pull/19449>`_, Jos Collin)
* doc: misc fixes for CephFS best practices (`issue#22630 <http://tracker.ceph.com/issues/22630>`_, `pr#19858 <https://github.com/ceph/ceph/pull/19858>`_, Jos Collin)
* doc: remove region from "INSTALL CEPH OBJECT GATEWAY" (`issue#21610 <http://tracker.ceph.com/issues/21610>`_, `pr#18865 <https://github.com/ceph/ceph/pull/18865>`_, Orit Wasserman)
* doc: update Blacklisting and OSD epoch barrier (`issue#22542 <http://tracker.ceph.com/issues/22542>`_, `pr#19741 <https://github.com/ceph/ceph/pull/19741>`_, Jos Collin)
* librbd: cannot clone all image-metas if we have more than 64 key/value pairs (`issue#21814 <http://tracker.ceph.com/issues/21814>`_, `pr#19503 <https://github.com/ceph/ceph/pull/19503>`_, PCzhangPC)
* librbd: cannot copy all image-metas if we have more than 64 key/value pairs (`issue#21815 <http://tracker.ceph.com/issues/21815>`_, `pr#19504 <https://github.com/ceph/ceph/pull/19504>`_, PCzhangPC)
* librbd: compare and write against a clone can result in failure (`issue#20789 <http://tracker.ceph.com/issues/20789>`_, `pr#20211 <https://github.com/ceph/ceph/pull/20211>`_, Mykola Golub, Jason Dillaman)
* librbd: default to sparse-reads for any IO operation over 64K (`issue#21849 <http://tracker.ceph.com/issues/21849>`_, `pr#20208 <https://github.com/ceph/ceph/pull/20208>`_, Jason Dillaman)
* librbd: fix snap create/rm may taking long time (`issue#22716 <http://tracker.ceph.com/issues/22716>`_, `pr#20153 <https://github.com/ceph/ceph/pull/20153>`_, Song Shun)
* librbd: force removal of a snapshot cannot ignore dependent children (`issue#22791 <http://tracker.ceph.com/issues/22791>`_, `pr#20135 <https://github.com/ceph/ceph/pull/20135>`_, Jason Dillaman)
* librbd: Image-meta should be dynamically refreshed (`issue#21529 <http://tracker.ceph.com/issues/21529>`_, `pr#19447 <https://github.com/ceph/ceph/pull/19447>`_, Dongsheng Yang, Jason Dillaman)
* librbd: journal should ignore -EILSEQ errors from compare-and-write (`issue#21628 <http://tracker.ceph.com/issues/21628>`_, `pr#20206 <https://github.com/ceph/ceph/pull/20206>`_, Jason Dillaman)
* librbd: refresh image after applying new/removing old metadata (`issue#21711 <http://tracker.ceph.com/issues/21711>`_, `pr#19485 <https://github.com/ceph/ceph/pull/19485>`_, Jason Dillaman)
* librbd: set deleted parent pointer to null (`issue#22158 <http://tracker.ceph.com/issues/22158>`_, `pr#20210 <https://github.com/ceph/ceph/pull/20210>`_, Jason Dillaman)
* luminous: ceph-fuse: ::rmdir() uses a deleted memory structure of dentry leads … (`issue#22536 <http://tracker.ceph.com/issues/22536>`_, `pr#19968 <https://github.com/ceph/ceph/pull/19968>`_, YunfeiGuan)
* mds: check for CEPH_OSDMAP_FULL is now wrong; cluster full flag is obsolete (`issue#22483 <http://tracker.ceph.com/issues/22483>`_, `pr#19830 <https://github.com/ceph/ceph/pull/19830>`_, Patrick Donnelly)
* mds: don't check gid when none specified in auth caps (`issue#22009 <http://tracker.ceph.com/issues/22009>`_, `pr#18835 <https://github.com/ceph/ceph/pull/18835>`_, Douglas Fuller)
* mds: don't delay processing completed requests in replay queue (`issue#22163 <http://tracker.ceph.com/issues/22163>`_, `pr#19157 <https://github.com/ceph/ceph/pull/19157>`_, "Yan, Zheng")
* mds: don't report repaired backtraces in damagetable, write back after repair, clean up scrub log (`issue#18743 <http://tracker.ceph.com/issues/18743>`_, `issue#22058 <http://tracker.ceph.com/issues/22058>`_, `pr#20341 <https://github.com/ceph/ceph/pull/20341>`_, "Yan, Zheng", John Spray)
* mds: fix CDir::log_mark_dirty() (`issue#21584 <http://tracker.ceph.com/issues/21584>`_, `pr#18008 <https://github.com/ceph/ceph/pull/18008>`_, "Yan, Zheng")
* mds: fix dump last_sent (`issue#22562 <http://tracker.ceph.com/issues/22562>`_, `pr#19959 <https://github.com/ceph/ceph/pull/19959>`_, dongdong tao)
* mds: fix MDS_FEATURE_INCOMPAT_FILE_LAYOUT_V2 definition (`issue#21985 <http://tracker.ceph.com/issues/21985>`_, `pr#18782 <https://github.com/ceph/ceph/pull/18782>`_, "Yan, Zheng")
* mds: fix return value of MDCache::dump_cache (`issue#22798 <http://tracker.ceph.com/issues/22798>`_, `pr#20121 <https://github.com/ceph/ceph/pull/20121>`_, "Yan, Zheng")
* mds: fix scrub crash (`issue#22730 <http://tracker.ceph.com/issues/22730>`_, `pr#20249 <https://github.com/ceph/ceph/pull/20249>`_, dongdong tao)
* mds: fix StrayManager::truncate() (`issue#21091 <http://tracker.ceph.com/issues/21091>`_, `pr#18019 <https://github.com/ceph/ceph/pull/18019>`_, "Yan, Zheng")
* mds: handle client reconnect gather race (`issue#22263 <http://tracker.ceph.com/issues/22263>`_, `pr#19326 <https://github.com/ceph/ceph/pull/19326>`_, "Yan, Zheng")
* mds: handle client session messages when mds is stopping (`issue#22460 <http://tracker.ceph.com/issues/22460>`_, `pr#19585 <https://github.com/ceph/ceph/pull/19585>`_, "Yan, Zheng")
* mds: handle 'inode gets queued for recovery multiple times' (`issue#22647 <http://tracker.ceph.com/issues/22647>`_, `pr#19982 <https://github.com/ceph/ceph/pull/19982>`_, "Yan, Zheng")
* mds: ignore export pin for unlinked directory (`issue#22219 <http://tracker.ceph.com/issues/22219>`_, `pr#19360 <https://github.com/ceph/ceph/pull/19360>`_, "Yan, Zheng")
* mds: limit size of subtree migration (`issue#21892 <http://tracker.ceph.com/issues/21892>`_, `pr#20339 <https://github.com/ceph/ceph/pull/20339>`_, "Yan, Zheng")
* mds: no assertion on inode being purging in find_ino_peers() (`issue#21722 <http://tracker.ceph.com/issues/21722>`_, `pr#18869 <https://github.com/ceph/ceph/pull/18869>`_, Zhi Zhang)
* mds: preserve order of requests during recovery of multimds cluster (`issue#21843 <http://tracker.ceph.com/issues/21843>`_, `pr#18871 <https://github.com/ceph/ceph/pull/18871>`_, "Yan, Zheng")
* mds: prevent filelock from being stuck at XSYN state (`issue#22008 <http://tracker.ceph.com/issues/22008>`_, `pr#20340 <https://github.com/ceph/ceph/pull/20340>`_, "Yan, Zheng")
* mds: properly eval locks after importing inode (`issue#22357 <http://tracker.ceph.com/issues/22357>`_, `pr#19646 <https://github.com/ceph/ceph/pull/19646>`_, "Yan, Zheng")
* mds: reduce debugging level for balancer messages (`issue#21853 <http://tracker.ceph.com/issues/21853>`_, `pr#19827 <https://github.com/ceph/ceph/pull/19827>`_, Patrick Donnelly)
* mds: respect mds_client_writeable_range_max_inc_objs config (`issue#22492 <http://tracker.ceph.com/issues/22492>`_, `pr#19776 <https://github.com/ceph/ceph/pull/19776>`_, "Yan, Zheng")
* mds: set higher priority for some perf counters (`issue#22776 <http://tracker.ceph.com/issues/22776>`_, `pr#20299 <https://github.com/ceph/ceph/pull/20299>`_, Shangzhong Zhu)
* mds: set PRIO_USEFUL on num_sessions counter (`issue#21927 <http://tracker.ceph.com/issues/21927>`_, `pr#18722 <https://github.com/ceph/ceph/pull/18722>`_, John Spray)
* mds: tell session ls returns vanila EINVAL when MDS is not active (`issue#21991 <http://tracker.ceph.com/issues/21991>`_, `pr#19505 <https://github.com/ceph/ceph/pull/19505>`_, Jos Collin)
* mds: track dirty dentries in separate list (`issue#19578 <http://tracker.ceph.com/issues/19578>`_, `pr#19775 <https://github.com/ceph/ceph/pull/19775>`_, "Yan, Zheng")
* mds: trim 'N' log segments according to how many log segments are there (`issue#21975 <http://tracker.ceph.com/issues/21975>`_, `pr#18783 <https://github.com/ceph/ceph/pull/18783>`_, "Yan, Zheng")
* mgr: ceph-mgr spuriously reloading OSD metadata on map changes (`issue#21159 <http://tracker.ceph.com/issues/21159>`_, `pr#18732 <https://github.com/ceph/ceph/pull/18732>`_, Yanhu Cao)
* mgr: disconnect unregistered service daemon when report received (`issue#22286 <http://tracker.ceph.com/issues/22286>`_, `pr#20089 <https://github.com/ceph/ceph/pull/20089>`_, Jason Dillaman)
* mgr: KeyError: ('name',) in balancer rm (`issue#22470 <http://tracker.ceph.com/issues/22470>`_, `pr#19624 <https://github.com/ceph/ceph/pull/19624>`_, Dan van der Ster)
* mgr: Manager daemon x is unresponsive. No standby daemons available (`issue#21147 <http://tracker.ceph.com/issues/21147>`_, `pr#19501 <https://github.com/ceph/ceph/pull/19501>`_, Sage Weil)
* mgr: mgr/balancer/upmap_max_iterations must be cast to integer (`issue#22429 <http://tracker.ceph.com/issues/22429>`_, `pr#19553 <https://github.com/ceph/ceph/pull/19553>`_, Dan van der Ster)
* mgr: mgr/dashboard: added iSCSI IOPS/throughput metrics (`issue#21391 <http://tracker.ceph.com/issues/21391>`_, `pr#20209 <https://github.com/ceph/ceph/pull/20209>`_, Jason Dillaman)
* mgr: mgr/dashboard: Fix PG status coloring (`issue#22615 <http://tracker.ceph.com/issues/22615>`_, `pr#19844 <https://github.com/ceph/ceph/pull/19844>`_, Wido den Hollander)
* mgr: mgr/prometheus: add missing 'deep' state to PG_STATES in ceph-mgr pro… (`issue#22116 <http://tracker.ceph.com/issues/22116>`_, `pr#19929 <https://github.com/ceph/ceph/pull/19929>`_, Jan Fajerski, Peter Woodman)
* mgr: mgr tests don't indicate failure if exception thrown from serve() (`issue#21999 <http://tracker.ceph.com/issues/21999>`_, `pr#18832 <https://github.com/ceph/ceph/pull/18832>`_, John Spray)
* mgr: mgr[zabbix] float division by zero (osd['kb'] = 0) (`issue#21904 <http://tracker.ceph.com/issues/21904>`_, `pr#19048 <https://github.com/ceph/ceph/pull/19048>`_, Ilja Slepnev)
* mgr: prometheus: added osd commit/apply latency metrics (#22718) (`issue#22718 <http://tracker.ceph.com/issues/22718>`_, `pr#20084 <https://github.com/ceph/ceph/pull/20084>`_, Konstantin Shalygin)
* mgr: pybind/mgr/dashboard: fix duplicated slash in html href (`issue#22851 <http://tracker.ceph.com/issues/22851>`_, `pr#20325 <https://github.com/ceph/ceph/pull/20325>`_, Shengjing Zhu)
* mgr: pybind/mgr/dashboard: fix reverse proxy support (`issue#22557 <http://tracker.ceph.com/issues/22557>`_, `pr#20182 <https://github.com/ceph/ceph/pull/20182>`_, Nick Erdmann, Kefu Chai)
* mgr: pybind/mgr/prometheus: fix metric type undef -> untyped (`issue#22313 <http://tracker.ceph.com/issues/22313>`_, `pr#19834 <https://github.com/ceph/ceph/pull/19834>`_, Ilya Margolin)
* mgr: restarting active ceph-mgr cause glitches in bps and iops metrics (`issue#21773 <http://tracker.ceph.com/issues/21773>`_, `pr#18735 <https://github.com/ceph/ceph/pull/18735>`_, Aleksei Gutikov, Kefu Chai)
* mgr: Services reported with blank hostname (`issue#20887 <http://tracker.ceph.com/issues/20887>`_, `issue#21687 <http://tracker.ceph.com/issues/21687>`_, `pr#17869 <https://github.com/ceph/ceph/pull/17869>`_, liuchang0812, Chang Liu)
* mon: do not use per_pool_sum_delta to show recovery summary (`issue#22727 <http://tracker.ceph.com/issues/22727>`_, `pr#20150 <https://github.com/ceph/ceph/pull/20150>`_, Chang Liu)
* mon: fix mgr using auth_client_required policy (`issue#22096 <http://tracker.ceph.com/issues/22096>`_, `pr#20156 <https://github.com/ceph/ceph/pull/20156>`_, John Spray)
* mon: MDSMonitor: reject misconfigured mds_blacklist_interval (`issue#21821 <http://tracker.ceph.com/issues/21821>`_, `pr#19871 <https://github.com/ceph/ceph/pull/19871>`_, John Spray)
* mon/MgrMonitor: limit mgrmap history (`issue#22257 <http://tracker.ceph.com/issues/22257>`_, `pr#19187 <https://github.com/ceph/ceph/pull/19187>`_, Sage Weil)
* mon: reenable timer to send digest when paxos is temporarily inactive (`issue#22142 <http://tracker.ceph.com/issues/22142>`_, `pr#19481 <https://github.com/ceph/ceph/pull/19481>`_, Jan Fajerski)
* msg: msg/async/AsyncConnection.cc: 1835: FAILED assert(state == STATE_CLOSED) (`issue#21883 <http://tracker.ceph.com/issues/21883>`_, `pr#18746 <https://github.com/ceph/ceph/pull/18746>`_, Haomai Wang)
* msg: msg/async: unregister connection failed when racing happened (`issue#22437 <http://tracker.ceph.com/issues/22437>`_, `pr#19552 <https://github.com/ceph/ceph/pull/19552>`_, Haomai Wang)
* osdc: "FAILED assert(bh->last_write_tid > tid)" in powercycle-wip-yuri-master-1.19.18-distro-basic-smithi (`issue#22741 <http://tracker.ceph.com/issues/22741>`_, `pr#20256 <https://github.com/ceph/ceph/pull/20256>`_, "Yan, Zheng")
* osdc/Journaler: add 'stopping' check to various finish callbacks (`issue#22360 <http://tracker.ceph.com/issues/22360>`_, `pr#19610 <https://github.com/ceph/ceph/pull/19610>`_, "Yan, Zheng")
* osdc/Objecter: objecter op_send_bytes perf counter always 0 (`issue#21982 <http://tracker.ceph.com/issues/21982>`_, `pr#19046 <https://github.com/ceph/ceph/pull/19046>`_, Jianpeng Ma)
* osd: do not check out-of-date osdmap for DESTROYED flag on start (`issue#22673 <http://tracker.ceph.com/issues/22673>`_, `pr#20068 <https://github.com/ceph/ceph/pull/20068>`_, Sage Weil)
* osd,mgr: report pending creating pgs to mgr (`issue#22440 <http://tracker.ceph.com/issues/22440>`_, `pr#20204 <https://github.com/ceph/ceph/pull/20204>`_, Kefu Chai)
* osd: miscounting degraded objects and PG stuck in recovery_unfound (`issue#22145 <http://tracker.ceph.com/issues/22145>`_, `pr#20055 <https://github.com/ceph/ceph/pull/20055>`_, Sage Weil, David Zafman)
* osd: Objecter::C_ObjectOperation_sparse_read throws/catches exceptions on -ENOENT (`issue#21844 <http://tracker.ceph.com/issues/21844>`_, `pr#18744 <https://github.com/ceph/ceph/pull/18744>`_, Jason Dillaman)
* osd: Objecter::_send_op unnecessarily constructs costly hobject_t (`issue#21845 <http://tracker.ceph.com/issues/21845>`_, `pr#18745 <https://github.com/ceph/ceph/pull/18745>`_, Jason Dillaman)
* osd: On pg repair the primary is not favored as was intended (`issue#21907 <http://tracker.ceph.com/issues/21907>`_, `pr#19083 <https://github.com/ceph/ceph/pull/19083>`_, David Zafman)
* osd: OSD crushes with FAILED assert(used_blocks.size() > count) during the first start after upgrade 12.2.1 -> 12.2.2 (`issue#22535 <http://tracker.ceph.com/issues/22535>`_, `pr#19888 <https://github.com/ceph/ceph/pull/19888>`_, Igor Fedotov)
* osd: OSDMap cache assert on shutdown (`issue#21737 <http://tracker.ceph.com/issues/21737>`_, `pr#18749 <https://github.com/ceph/ceph/pull/18749>`_, Greg Farnum)
* osd: OSDService::recovery_need_sleep read+updated without locking (`issue#21566 <http://tracker.ceph.com/issues/21566>`_, `pr#18753 <https://github.com/ceph/ceph/pull/18753>`_, Neha Ojha)
* osd: "osd status" command exception if OSD not in pgmap stats (`issue#21707 <http://tracker.ceph.com/issues/21707>`_, `pr#19084 <https://github.com/ceph/ceph/pull/19084>`_, Yanhu Cao)
* osd, pg, mgr: make snap trim queue problems visible (`issue#22448 <http://tracker.ceph.com/issues/22448>`_, `pr#20098 <https://github.com/ceph/ceph/pull/20098>`_, Piotr Dałek)
* osd: Pool Compression type option doesn't apply to new OSDs (`issue#22419 <http://tracker.ceph.com/issues/22419>`_, `pr#20106 <https://github.com/ceph/ceph/pull/20106>`_, Kefu Chai)
* osd: replica read can trigger cache promotion (`issue#20919 <http://tracker.ceph.com/issues/20919>`_, `pr#19499 <https://github.com/ceph/ceph/pull/19499>`_, Sage Weil)
* osd/ReplicatedPG.cc: recover_replicas: object added to missing set for backfill, but is not in recovering, error! (`issue#21382 <http://tracker.ceph.com/issues/21382>`_, `issue#14513 <http://tracker.ceph.com/issues/14513>`_, `issue#18162 <http://tracker.ceph.com/issues/18162>`_, `pr#20081 <https://github.com/ceph/ceph/pull/20081>`_, David Zafman)
* osd: subscribe osdmaps if any pending pgs (`issue#22113 <http://tracker.ceph.com/issues/22113>`_, `pr#19059 <https://github.com/ceph/ceph/pull/19059>`_, Kefu Chai)
* osd: "sudo cp /var/lib/ceph/osd/ceph-0/fsid ..." fails (`issue#20736 <http://tracker.ceph.com/issues/20736>`_, `pr#19631 <https://github.com/ceph/ceph/pull/19631>`_, Patrick Donnelly)
* os: fix 0-length zero semantics, test (`issue#21712 <http://tracker.ceph.com/issues/21712>`_, `pr#20049 <https://github.com/ceph/ceph/pull/20049>`_, Sage Weil)
* qa/tests: Applied PR 20053 to stress-split tests (`issue#22665 <http://tracker.ceph.com/issues/22665>`_, `pr#20451 <https://github.com/ceph/ceph/pull/20451>`_, Yuri Weinstein)
* rbd: abort in listing mapped nbd devices when running in a container (`issue#22012 <http://tracker.ceph.com/issues/22012>`_, `issue#22011 <http://tracker.ceph.com/issues/22011>`_, `pr#19051 <https://github.com/ceph/ceph/pull/19051>`_, Li Wang)
* rbd: [api] compare-and-write methods not properly advertised (`issue#22036 <http://tracker.ceph.com/issues/22036>`_, `pr#18834 <https://github.com/ceph/ceph/pull/18834>`_, Jason Dillaman)
* rbd: class rbd.Image discard----OSError: [errno 2147483648] error discarding region (`issue#21966 <http://tracker.ceph.com/issues/21966>`_, `pr#19058 <https://github.com/ceph/ceph/pull/19058>`_, Jason Dillaman)
* rbd: cluster resource agent ocf:ceph:rbd - wrong permissions (`issue#22362 <http://tracker.ceph.com/issues/22362>`_, `pr#19554 <https://github.com/ceph/ceph/pull/19554>`_, Nathan Cutler)
* rbd: disk usage on empty pool no longer returns an error message (`issue#22200 <http://tracker.ceph.com/issues/22200>`_, `pr#19107 <https://github.com/ceph/ceph/pull/19107>`_, Jason Dillaman)
* rbd: fix crash during map (`issue#21808 <http://tracker.ceph.com/issues/21808>`_, `pr#18698 <https://github.com/ceph/ceph/pull/18698>`_, Peter Keresztes Schmidt)
* rbd: [journal] tags are not being expired if no other clients are registered (`issue#21960 <http://tracker.ceph.com/issues/21960>`_, `pr#18840 <https://github.com/ceph/ceph/pull/18840>`_, Jason Dillaman)
* rbd: librbd: filter out potential race with image rename (`issue#18435 <http://tracker.ceph.com/issues/18435>`_, `pr#19853 <https://github.com/ceph/ceph/pull/19853>`_, Jason Dillaman)
* rbd-mirror: Allow a different data-pool to be used on the secondary cluster (`issue#21088 <http://tracker.ceph.com/issues/21088>`_, `pr#19305 <https://github.com/ceph/ceph/pull/19305>`_, Adam Wolfe Gordon)
* rbd-mirror: primary image should register in remote, non-primary image's journal (`issue#21961 <http://tracker.ceph.com/issues/21961>`_, `issue#21561 <http://tracker.ceph.com/issues/21561>`_, `pr#20207 <https://github.com/ceph/ceph/pull/20207>`_, Jason Dillaman)
* rbd-mirror: sync image metadata when transfering remote image (`issue#21535 <http://tracker.ceph.com/issues/21535>`_, `pr#19484 <https://github.com/ceph/ceph/pull/19484>`_, Jason Dillaman)
* rbd: Python RBD metadata_get does not work (`issue#22306 <http://tracker.ceph.com/issues/22306>`_, `pr#19479 <https://github.com/ceph/ceph/pull/19479>`_, Mykola Golub)
* rbd: rbd ls -l crashes with SIGABRT (`issue#21558 <http://tracker.ceph.com/issues/21558>`_, `pr#19800 <https://github.com/ceph/ceph/pull/19800>`_, Jason Dillaman)
* rbd: [rbd-mirror] new pools might not be detected (`issue#22461 <http://tracker.ceph.com/issues/22461>`_, `pr#19625 <https://github.com/ceph/ceph/pull/19625>`_, Jason Dillaman)
* rbd: [rbd-nbd] Fedora does not register resize events (`issue#22131 <http://tracker.ceph.com/issues/22131>`_, `pr#19066 <https://github.com/ceph/ceph/pull/19066>`_, Jason Dillaman)
* rbd: [test] UpdateFeatures RPC message should be included in test_notify.py (`issue#21936 <http://tracker.ceph.com/issues/21936>`_, `pr#18838 <https://github.com/ceph/ceph/pull/18838>`_, Jason Dillaman)
* Revert " luminous: msg/async: unregister connection failed when racing happened" (`issue#22231 <http://tracker.ceph.com/issues/22231>`_, `pr#20247 <https://github.com/ceph/ceph/pull/20247>`_, Sage Weil)
* rgw: 501 is returned When init multipart is using V4 signature and chunk encoding (`issue#22129 <http://tracker.ceph.com/issues/22129>`_, `pr#19506 <https://github.com/ceph/ceph/pull/19506>`_, Jeegn Chen)
* rgw: add cors header rule check in cors option request (`issue#22002 <http://tracker.ceph.com/issues/22002>`_, `pr#19053 <https://github.com/ceph/ceph/pull/19053>`_, yuliyang)
* rgw: backport beast frontend and boost 1.66 update (`issue#22101 <http://tracker.ceph.com/issues/22101>`_, `issue#20935 <http://tracker.ceph.com/issues/20935>`_, `issue#21831 <http://tracker.ceph.com/issues/21831>`_, `issue#20048 <http://tracker.ceph.com/issues/20048>`_, `issue#22600 <http://tracker.ceph.com/issues/22600>`_, `issue#20971 <http://tracker.ceph.com/issues/20971>`_, `pr#19848 <https://github.com/ceph/ceph/pull/19848>`_, Casey Bodley, Jiaying Ren)
* rgw: bucket index object not deleted after radosgw-admin bucket rm --purge-objects --bypass-gc (`issue#22122 <http://tracker.ceph.com/issues/22122>`_, `issue#19959 <http://tracker.ceph.com/issues/19959>`_, `pr#19085 <https://github.com/ceph/ceph/pull/19085>`_, Aleksei Gutikov)
* rgw: bucket policy evaluation logical error (`issue#21901 <http://tracker.ceph.com/issues/21901>`_, `issue#21896 <http://tracker.ceph.com/issues/21896>`_, `pr#19810 <https://github.com/ceph/ceph/pull/19810>`_, Adam C. Emerson)
* rgw: bucket resharding should not update bucket ACL or user stats (`issue#22742 <http://tracker.ceph.com/issues/22742>`_, `issue#22124 <http://tracker.ceph.com/issues/22124>`_, `pr#20327 <https://github.com/ceph/ceph/pull/20327>`_, Orit Wasserman)
* rgw: check going_down() when lifecycle processing (`issue#22099 <http://tracker.ceph.com/issues/22099>`_, `pr#19088 <https://github.com/ceph/ceph/pull/19088>`_, Yao Zongyou)
* rgw: Dynamic bucket indexing, resharding and tenants seems to be broken (`issue#22046 <http://tracker.ceph.com/issues/22046>`_, `pr#19050 <https://github.com/ceph/ceph/pull/19050>`_, Orit Wasserman)
* rgw: file deadlock on lru evicting (`issue#22736 <http://tracker.ceph.com/issues/22736>`_, `pr#20075 <https://github.com/ceph/ceph/pull/20075>`_, Matt Benjamin)
* rgw: fix chained cache invalidation to prevent cache size growth (`issue#22410 <http://tracker.ceph.com/issues/22410>`_, `pr#19785 <https://github.com/ceph/ceph/pull/19785>`_, Mark Kogan)
* rgw: fix for empty query string in beast frontend (`issue#22797 <http://tracker.ceph.com/issues/22797>`_, `pr#20338 <https://github.com/ceph/ceph/pull/20338>`_, Casey Bodley)
* rgw: fix GET website response error code (`issue#22272 <http://tracker.ceph.com/issues/22272>`_, `pr#19489 <https://github.com/ceph/ceph/pull/19489>`_, Dmitry Plyakin)
* rgw: fix rewrite a versioning object create a new object bug (`issue#21984 <http://tracker.ceph.com/issues/21984>`_, `issue#22529 <http://tracker.ceph.com/issues/22529>`_, `pr#19787 <https://github.com/ceph/ceph/pull/19787>`_, Enming Zhang, Matt Benjamin)
* rgw: Fix swift object expiry not deleting objects (`issue#22084 <http://tracker.ceph.com/issues/22084>`_, `pr#18972 <https://github.com/ceph/ceph/pull/18972>`_, Pavan Rallabhandi)
* rgw: Fix swift object expiry not deleting objects (`issue#22084 <http://tracker.ceph.com/issues/22084>`_, `pr#19090 <https://github.com/ceph/ceph/pull/19090>`_, Pavan Rallabhandi)
* rgw: librgw: fix shutdown error with resources uncleaned (`issue#22296 <http://tracker.ceph.com/issues/22296>`_, `pr#20073 <https://github.com/ceph/ceph/pull/20073>`_, Tao Chen)
* rgw: log keystone errors at a higher level (`issue#22151 <http://tracker.ceph.com/issues/22151>`_, `pr#19077 <https://github.com/ceph/ceph/pull/19077>`_, Abhishek Lekshmanan)
* rgw: make HTTP dechunking compatible with Amazon S3 (`issue#21015 <http://tracker.ceph.com/issues/21015>`_, `pr#19500 <https://github.com/ceph/ceph/pull/19500>`_, Radoslaw Zarzynski)
* rgw: modify s3 type subuser access permission fail (`issue#21983 <http://tracker.ceph.com/issues/21983>`_, `pr#18766 <https://github.com/ceph/ceph/pull/18766>`_, yuliyang)
* rgw: multisite: destination zone does not compress synced objects (`issue#21895 <http://tracker.ceph.com/issues/21895>`_, `pr#18867 <https://github.com/ceph/ceph/pull/18867>`_, Casey Bodley)
* rgw: multisite: 'radosgw-admin sync error list' contains temporary EBUSY errors (`issue#22473 <http://tracker.ceph.com/issues/22473>`_, `pr#19799 <https://github.com/ceph/ceph/pull/19799>`_, Casey Bodley)
* rgw: null instance mtime incorrect when enable versioning (`issue#21743 <http://tracker.ceph.com/issues/21743>`_, `pr#18870 <https://github.com/ceph/ceph/pull/18870>`_, Shasha Lu)
* rgw: Policy parser may or may not dereference uninitialized boost::optional sometimes (`issue#21962 <http://tracker.ceph.com/issues/21962>`_, `pr#18868 <https://github.com/ceph/ceph/pull/18868>`_, Adam C. Emerson)
* rgw: Possible deadlock in 'list_children' when refresh is required (`issue#21670 <http://tracker.ceph.com/issues/21670>`_, `pr#18564 <https://github.com/ceph/ceph/pull/18564>`_, Jason Dillaman)
* rgw: put bucket policy panics RGW process (`issue#22541 <http://tracker.ceph.com/issues/22541>`_, `pr#19847 <https://github.com/ceph/ceph/pull/19847>`_, Bingyin Zhang)
* rgw: radosgw-admin reshard command argument error (`issue#21723 <http://tracker.ceph.com/issues/21723>`_, `pr#19502 <https://github.com/ceph/ceph/pull/19502>`_, Yao Zongyou)
* rgw: radosgw-admin zonegroup get and zone get should return defaults when there is no realm (`issue#21615 <http://tracker.ceph.com/issues/21615>`_, `pr#19086 <https://github.com/ceph/ceph/pull/19086>`_, lvshanchun)
* rgw: Random 500 errors in Swift PutObject (needs cache fixes) (`issue#22517 <http://tracker.ceph.com/issues/22517>`_, `issue#21560 <http://tracker.ceph.com/issues/21560>`_, `pr#19788 <https://github.com/ceph/ceph/pull/19788>`_, Adam C. Emerson)
* rgw: refuses upload when Content-Type missing from POST policy (`issue#20201 <http://tracker.ceph.com/issues/20201>`_, `pr#19867 <https://github.com/ceph/ceph/pull/19867>`_, Matt Benjamin)
* rgw: revert PR #8765 (`issue#22364 <http://tracker.ceph.com/issues/22364>`_, `pr#19434 <https://github.com/ceph/ceph/pull/19434>`_, fang.yuxiang)
* rgw: RGWCrashError: RGW will crash if a putting lc config request does not include an ID tag in the request xml (`issue#21980 <http://tracker.ceph.com/issues/21980>`_, `issue#22006 <http://tracker.ceph.com/issues/22006>`_, `pr#18765 <https://github.com/ceph/ceph/pull/18765>`_, Enming Zhang)
* rgw: rgw multisite: automated trimming for bucket index logs (`issue#18229 <http://tracker.ceph.com/issues/18229>`_, `pr#20062 <https://github.com/ceph/ceph/pull/20062>`_, Casey Bodley)
* rgw: RGW: S3 POST policy should not require Content-Type (`issue#20201 <http://tracker.ceph.com/issues/20201>`_, `pr#19784 <https://github.com/ceph/ceph/pull/19784>`_, Matt Benjamin)
* rgw: rgw segfaults after running radosgw-admin data sync init (`issue#22083 <http://tracker.ceph.com/issues/22083>`_, `pr#19071 <https://github.com/ceph/ceph/pull/19071>`_, Casey Bodley, Abhishek Lekshmanan)
* rgw: rgw usage trim only trims a few entries (`issue#22234 <http://tracker.ceph.com/issues/22234>`_, `pr#19636 <https://github.com/ceph/ceph/pull/19636>`_, Abhishek Lekshmanan)
* rgw: S3 API Policy Conditions IpAddress and NotIpAddress do not work (`issue#20931 <http://tracker.ceph.com/issues/20931>`_, `issue#20991 <http://tracker.ceph.com/issues/20991>`_, `pr#19819 <https://github.com/ceph/ceph/pull/19819>`_, John Gibson, yuliyang, Casey Bodley, Abhishek Lekshmanan, Jiaying Ren)
* rgw: Segmentation fault when starting radosgw after reverting .rgw.root (`issue#21996 <http://tracker.ceph.com/issues/21996>`_, `pr#18764 <https://github.com/ceph/ceph/pull/18764>`_, Orit Wasserman, Casey Bodley)
* rgw: set sync_from_all as true when no value is seen (`issue#22062 <http://tracker.ceph.com/issues/22062>`_, `pr#19038 <https://github.com/ceph/ceph/pull/19038>`_, Abhishek Lekshmanan)
* rgw: unlink deleted bucket from bucket's owner (`issue#22248 <http://tracker.ceph.com/issues/22248>`_, `pr#20357 <https://github.com/ceph/ceph/pull/20357>`_, Casey Bodley)
* rgw: user stats increased after bucket reshard (`issue#22124 <http://tracker.ceph.com/issues/22124>`_, `pr#19538 <https://github.com/ceph/ceph/pull/19538>`_, Orit Wasserman)
* rgw: When a system object is created exclusively, do not distribute the (`issue#22792 <http://tracker.ceph.com/issues/22792>`_, `pr#20107 <https://github.com/ceph/ceph/pull/20107>`_, J. Eric Ivancich, Robin H. Johnson)
* tests: ceph_test_cls_log failures related to cls_cxx_subop_version() (`issue#21964 <http://tracker.ceph.com/issues/21964>`_, `pr#18715 <https://github.com/ceph/ceph/pull/18715>`_, Casey Bodley)
* tests: ceph_test_objectstore fails ObjectStore/StoreTest.Synthetic/1 (filestore) buffer content mismatch (`issue#21712 <http://tracker.ceph.com/issues/21712>`_, `issue#21818 <http://tracker.ceph.com/issues/21818>`_, `pr#18742 <https://github.com/ceph/ceph/pull/18742>`_, Sage Weil)
* tests: configure zabbix properly before selftest (`issue#22514 <http://tracker.ceph.com/issues/22514>`_, `pr#19831 <https://github.com/ceph/ceph/pull/19831>`_, John Spray)
* tests: do not configure ec data pool with memstore (`issue#22436 <http://tracker.ceph.com/issues/22436>`_, `pr#19628 <https://github.com/ceph/ceph/pull/19628>`_, Patrick Donnelly)
* tests: force backfill test can conflict with pool removal (`issue#22614 <http://tracker.ceph.com/issues/22614>`_, `pr#19966 <https://github.com/ceph/ceph/pull/19966>`_, Sage Weil)
* tests: full flag not set on osdmap for tasks.cephfs.test_full (`issue#22475 <http://tracker.ceph.com/issues/22475>`_, `pr#19962 <https://github.com/ceph/ceph/pull/19962>`_, Patrick Donnelly)
* tests: increase osd count for ec testing (`issue#22646 <http://tracker.ceph.com/issues/22646>`_, `pr#19976 <https://github.com/ceph/ceph/pull/19976>`_, Patrick Donnelly)
* tests - Initial checkin for luminous point-to-point upgrade (`issue#22048 <http://tracker.ceph.com/issues/22048>`_, `pr#18771 <https://github.com/ceph/ceph/pull/18771>`_, Yuri Weinstein)
* tests: qa/workunits/rbd: simplify split-brain test to avoid potential race (`issue#22485 <http://tracker.ceph.com/issues/22485>`_, `pr#20205 <https://github.com/ceph/ceph/pull/20205>`_, Jason Dillaman)
* tests: qa/workunits/rbd: switch devstack to pike release (`issue#22786 <http://tracker.ceph.com/issues/22786>`_, `pr#20136 <https://github.com/ceph/ceph/pull/20136>`_, Jason Dillaman)
* tests: rbd_mirror_helpers.sh request_resync_image function saves image id to wrong variable (`issue#21663 <http://tracker.ceph.com/issues/21663>`_, `pr#19802 <https://github.com/ceph/ceph/pull/19802>`_, Jason Dillaman)
* tools/ceph_monstore_tool: include mgrmap in initial paxos epoch (`issue#22266 <http://tracker.ceph.com/issues/22266>`_, `pr#20116 <https://github.com/ceph/ceph/pull/20116>`_, Kefu Chai)
* tools: ceph-monstore-tool --readable mode doesn't understand FSMap, MgrMap (`issue#21577 <http://tracker.ceph.com/issues/21577>`_, `pr#18754 <https://github.com/ceph/ceph/pull/18754>`_, John Spray)
* tools: ceph-objectstore-tool: Add option dump-import to examine an export (`issue#22086 <http://tracker.ceph.com/issues/22086>`_, `pr#19487 <https://github.com/ceph/ceph/pull/19487>`_, David Zafman)
* tools: ceph_objectstore_tool: no flush before collection_empty() calls; ObjectStore/StoreTest.SimpleAttrTest/2 fails (`issue#22409 <http://tracker.ceph.com/issues/22409>`_, `pr#19967 <https://github.com/ceph/ceph/pull/19967>`_, Igor Fedotov)
* tools: ceph-objectstore-tool set-size should clear data-digest (`issue#22112 <http://tracker.ceph.com/issues/22112>`_, `pr#20069 <https://github.com/ceph/ceph/pull/20069>`_, David Zafman)
* tools/crushtool: skip device id if no name exists (`issue#22117 <http://tracker.ceph.com/issues/22117>`_, `pr#19039 <https://github.com/ceph/ceph/pull/19039>`_, Jan Fajerski)


v12.2.2 Luminous
================

This is the second bugfix release of Luminous v12.2.x long term stable release
series. It contains a range of bug fixes and a few features across Bluestore,
CephFS, RBD & RGW. We recommend all the users of 12.2.x series update.

For more detailed information, see :download:`the complete changelog <../changelog/v12.2.2.txt>`.


Notable Changes
---------------

* Standby ceph-mgr daemons now redirect requests to the active messenger, easing
  configuration for tools & users accessing the web dashboard, restful API, or
  other ceph-mgr module services.
* The prometheus module has several significant updates and improvements.
* The new balancer module enables automatic optimization of CRUSH weights to
  balance data across the cluster.
* The ceph-volume tool has been updated to include support for BlueStore as well
  as FileStore. The only major missing ceph-volume feature is dm-crypt support.
* RGW's dynamic bucket index resharding is disabled in multisite environments,
  as it can cause inconsistencies in replication of bucket indexes to remote
  sites.

Other Notable Changes
---------------------

* build/ops: bump sphinx to 1.6 (`issue#21717 <http://tracker.ceph.com/issues/21717>`_, `pr#18167 <https://github.com/ceph/ceph/pull/18167>`_, Kefu Chai, Alfredo Deza)
* build/ops: macros expanding in spec file comment (`issue#22250 <http://tracker.ceph.com/issues/22250>`_, `pr#19173 <https://github.com/ceph/ceph/pull/19173>`_, Ken Dreyer)
* build/ops: python-numpy-devel build dependency for SUSE (`issue#21176 <http://tracker.ceph.com/issues/21176>`_, `pr#17692 <https://github.com/ceph/ceph/pull/17692>`_, Nathan Cutler)
* build/ops: selinux: Allow getattr on lnk sysfs files (`issue#21492 <http://tracker.ceph.com/issues/21492>`_, `pr#18650 <https://github.com/ceph/ceph/pull/18650>`_, Boris Ranto)
* build/ops: Ubuntu amd64 client can not discover the ubuntu arm64 ceph cluster (`issue#19705 <http://tracker.ceph.com/issues/19705>`_, `pr#18293 <https://github.com/ceph/ceph/pull/18293>`_, Kefu Chai)
* core: buffer: fix ABI breakage by removing list _mempool member (`issue#21573 <http://tracker.ceph.com/issues/21573>`_, `pr#18491 <https://github.com/ceph/ceph/pull/18491>`_, Sage Weil)
* core: Daemons(OSD, Mon...) exit abnormally at injectargs command (`issue#21365 <http://tracker.ceph.com/issues/21365>`_, `pr#17864 <https://github.com/ceph/ceph/pull/17864>`_, Yan Jun)
* core: Disable messenger logging (debug ms = 0/0) for clients unless overridden (`issue#21860 <http://tracker.ceph.com/issues/21860>`_, `pr#18529 <https://github.com/ceph/ceph/pull/18529>`_, Jason Dillaman)
* core: Improve OSD startup time by only scanning for omap corruption once (`issue#21328 <http://tracker.ceph.com/issues/21328>`_, `pr#17889 <https://github.com/ceph/ceph/pull/17889>`_, Luo Kexue, David Zafman)
* core: upmap does not respect osd reweights (`issue#21538 <http://tracker.ceph.com/issues/21538>`_, `pr#18699 <https://github.com/ceph/ceph/pull/18699>`_, Theofilos Mouratidis)
* dashboard: barfs on nulls where it expects numbers (`issue#21570 <http://tracker.ceph.com/issues/21570>`_, `pr#18728 <https://github.com/ceph/ceph/pull/18728>`_, John Spray)
* dashboard: OSD list has servers and osds in arbitrary order (`issue#21572 <http://tracker.ceph.com/issues/21572>`_, `pr#18736 <https://github.com/ceph/ceph/pull/18736>`_, John Spray)
* dashboard: the dashboard uses absolute links for filesystems and clients (`issue#20568 <http://tracker.ceph.com/issues/20568>`_, `pr#18737 <https://github.com/ceph/ceph/pull/18737>`_, Nick Erdmann)
* filestore: set default readahead and compaction threads for rocksdb (`issue#21505 <http://tracker.ceph.com/issues/21505>`_, `pr#18234 <https://github.com/ceph/ceph/pull/18234>`_, Josh Durgin, Mark Nelson)
* librbd: object map batch update might cause OSD suicide timeout (`issue#21797 <http://tracker.ceph.com/issues/21797>`_, `pr#18416 <https://github.com/ceph/ceph/pull/18416>`_, Jason Dillaman)
* librbd: snapshots should be created/removed against data pool (`issue#21567 <http://tracker.ceph.com/issues/21567>`_, `pr#18336 <https://github.com/ceph/ceph/pull/18336>`_, Jason Dillaman)
* mds: make sure snap inode's last matches its parent dentry's last (`issue#21337 <http://tracker.ceph.com/issues/21337>`_, `pr#17994 <https://github.com/ceph/ceph/pull/17994>`_, "Yan, Zheng")
* mds: sanitize mdsmap of removed pools (`issue#21945 <http://tracker.ceph.com/issues/21945>`_, `issue#21568 <http://tracker.ceph.com/issues/21568>`_, `pr#18628 <https://github.com/ceph/ceph/pull/18628>`_, Patrick Donnelly)
* mgr: bulk backport of ceph-mgr improvements (`issue#21594 <http://tracker.ceph.com/issues/21594>`_, `issue#17460 <http://tracker.ceph.com/issues/17460>`_, `issue#21197 <http://tracker.ceph.com/issues/21197>`_, `issue#21158 <http://tracker.ceph.com/issues/21158>`_, `issue#21593 <http://tracker.ceph.com/issues/21593>`_, `pr#18675 <https://github.com/ceph/ceph/pull/18675>`_, Benjeman Meekhof, Sage Weil, Jan Fajerski, John Spray, Kefu Chai, My Do, Spandan Kumar Sahu)
* mgr: ceph-mgr gets process called "exe" after respawn (`issue#21404 <http://tracker.ceph.com/issues/21404>`_, `pr#18738 <https://github.com/ceph/ceph/pull/18738>`_, John Spray)
* mgr: fix crashable DaemonStateIndex::get calls (`issue#17737 <http://tracker.ceph.com/issues/17737>`_, `pr#18412 <https://github.com/ceph/ceph/pull/18412>`_, John Spray)
* mgr: key mismatch for mgr after upgrade from jewel to luminous(dev) (`issue#20950 <http://tracker.ceph.com/issues/20950>`_, `pr#18727 <https://github.com/ceph/ceph/pull/18727>`_, John Spray)
* mgr: mgr status module uses base 10 units (`issue#21189 <http://tracker.ceph.com/issues/21189>`_, `issue#21752 <http://tracker.ceph.com/issues/21752>`_, `pr#18257 <https://github.com/ceph/ceph/pull/18257>`_, John Spray, Yanhu Cao)
* mgr: mgr[zabbix] float division by zero (`issue#21518 <http://tracker.ceph.com/issues/21518>`_, `pr#18734 <https://github.com/ceph/ceph/pull/18734>`_, John Spray)
* mgr: Prometheus crash when update (`issue#21253 <http://tracker.ceph.com/issues/21253>`_, `pr#17867 <https://github.com/ceph/ceph/pull/17867>`_, John Spray)
* mgr: prometheus module generates invalid output when counter names contain non-alphanum characters (`issue#20899 <http://tracker.ceph.com/issues/20899>`_, `pr#17868 <https://github.com/ceph/ceph/pull/17868>`_, John Spray, Jeremy H Austin)
* mgr: Quieten scary RuntimeError from restful module on startup (`issue#21292 <http://tracker.ceph.com/issues/21292>`_, `pr#17866 <https://github.com/ceph/ceph/pull/17866>`_, John Spray)
* mgr: Spurious ceph-mgr failovers during mon elections (`issue#20629 <http://tracker.ceph.com/issues/20629>`_, `pr#18726 <https://github.com/ceph/ceph/pull/18726>`_, John Spray)
* mon: Client client.admin marked osd.2 out, after it was down for 1504627577 seconds (`issue#21249 <http://tracker.ceph.com/issues/21249>`_, `pr#17862 <https://github.com/ceph/ceph/pull/17862>`_, John Spray)
* mon: DNS SRV default service name not used anymore (`issue#21204 <http://tracker.ceph.com/issues/21204>`_, `pr#17863 <https://github.com/ceph/ceph/pull/17863>`_, Kefu Chai)
* mon/MgrMonitor: handle cmd descs to/from disk in the absence of active mgr (`issue#21300 <http://tracker.ceph.com/issues/21300>`_, `pr#18038 <https://github.com/ceph/ceph/pull/18038>`_, Joao Eduardo Luis)
* mon/mgr: sync "mgr_command_descs","osd_metadata" and "mgr_metadata" prefixes to new mons (`issue#21527 <http://tracker.ceph.com/issues/21527>`_, `pr#18620 <https://github.com/ceph/ceph/pull/18620>`_, huanwen ren)
* mon: osd feature checks with 0 up osds (`issue#21471 <http://tracker.ceph.com/issues/21471>`_, `issue#20751 <http://tracker.ceph.com/issues/20751>`_, `pr#18364 <https://github.com/ceph/ceph/pull/18364>`_, Brad Hubbard, Sage Weil)
* mon,osd: fix "pg ls {forced_backfill, backfilling}" (`issue#21609 <http://tracker.ceph.com/issues/21609>`_, `pr#18236 <https://github.com/ceph/ceph/pull/18236>`_, Kefu Chai)
* mon/OSDMonitor: add option to fix up ruleset-\* to crush-\* for ec profiles (`issue#22128 <http://tracker.ceph.com/issues/22128>`_, `pr#18945 <https://github.com/ceph/ceph/pull/18945>`_, Sage Weil)
* mon, osd: per pool space-full flag support (`issue#21409 <http://tracker.ceph.com/issues/21409>`_, `pr#17730 <https://github.com/ceph/ceph/pull/17730>`_, xie xingguo)
* mon/PGMap: Fix %USED calculation (`issue#22247 <http://tracker.ceph.com/issues/22247>`_, `pr#19230 <https://github.com/ceph/ceph/pull/19230>`_, Xiaoxi Chen)
* mon: update get_store_prefixes implementations (`issue#21534 <http://tracker.ceph.com/issues/21534>`_, `pr#18621 <https://github.com/ceph/ceph/pull/18621>`_, John Spray, huanwen ren)
* msgr: messages/MOSDMap: do compat reencode of crush map, too (`issue#21882 <http://tracker.ceph.com/issues/21882>`_, `pr#18456 <https://github.com/ceph/ceph/pull/18456>`_, Sage Weil)
* msgr: src/messages/MOSDMap: reencode OSDMap for older clients (`issue#21660 <http://tracker.ceph.com/issues/21660>`_, `pr#18140 <https://github.com/ceph/ceph/pull/18140>`_, Sage Weil)
* os/bluestore/BlueFS: fix race with log flush during async log compaction (`issue#21878 <http://tracker.ceph.com/issues/21878>`_, `pr#18503 <https://github.com/ceph/ceph/pull/18503>`_, Sage Weil)
* os/bluestore: fix another aio stall/deadlock (`issue#21470 <http://tracker.ceph.com/issues/21470>`_, `pr#18127 <https://github.com/ceph/ceph/pull/18127>`_, Sage Weil)
* os/bluestore: fix SharedBlob unregistration (`issue#22039 <http://tracker.ceph.com/issues/22039>`_, `pr#18983 <https://github.com/ceph/ceph/pull/18983>`_, Sage Weil)
* os/bluestore: handle compressed extents in blob unsharing checks (`issue#21766 <http://tracker.ceph.com/issues/21766>`_, `pr#18501 <https://github.com/ceph/ceph/pull/18501>`_, Sage Weil)
* os/bluestore: replace 21089 repair with something online (instead of fsck) (`issue#21089 <http://tracker.ceph.com/issues/21089>`_, `pr#17734 <https://github.com/ceph/ceph/pull/17734>`_, Sage Weil)
* os/bluestore: set bitmap freelist resolution to min_alloc_size (`issue#21408 <http://tracker.ceph.com/issues/21408>`_, `pr#18050 <https://github.com/ceph/ceph/pull/18050>`_, Sage Weil)
* os/blueStore::umount will crash when the BlueStore is opened by start_kv_only() (`issue#21624 <http://tracker.ceph.com/issues/21624>`_, `pr#18750 <https://github.com/ceph/ceph/pull/18750>`_, Chang Liu)
* osd: additional protection for out-of-bounds EC reads (`issue#21629 <http://tracker.ceph.com/issues/21629>`_, `pr#18413 <https://github.com/ceph/ceph/pull/18413>`_, Jason Dillaman)
* osd: allow recovery preemption (`issue#21613 <http://tracker.ceph.com/issues/21613>`_, `pr#18025 <https://github.com/ceph/ceph/pull/18025>`_, Sage Weil)
* osd: build_past_intervals_parallel: Ignore new partially created PGs (`issue#21833 <http://tracker.ceph.com/issues/21833>`_, `pr#18673 <https://github.com/ceph/ceph/pull/18673>`_, David Zafman)
* osd: dump bluestore debug on shutdown if debug option is set (`issue#21259 <http://tracker.ceph.com/issues/21259>`_, `pr#18103 <https://github.com/ceph/ceph/pull/18103>`_, Sage Weil)
* osd: make stat_bytes and stat_bytes_used counters PRIO_USEFUL (`issue#21981 <http://tracker.ceph.com/issues/21981>`_, `pr#18723 <https://github.com/ceph/ceph/pull/18723>`_, Yao Zongyou)
* osd: make the PG's SORTBITWISE assert a more generous shutdown (`issue#20416 <http://tracker.ceph.com/issues/20416>`_, `pr#18132 <https://github.com/ceph/ceph/pull/18132>`_, Greg Farnum)
* osd: OSD metadata 'backend_filestore_dev_node' is unknown even for simple deployment (`issue#20944 <http://tracker.ceph.com/issues/20944>`_, `pr#17865 <https://github.com/ceph/ceph/pull/17865>`_, Sage Weil)
* rbd: [cli] mirror getter commands will fail if mirroring has never been enabled (`issue#21319 <http://tracker.ceph.com/issues/21319>`_, `pr#17861 <https://github.com/ceph/ceph/pull/17861>`_, Jason Dillaman)
* rbd: cls/journal: fixed possible infinite loop in expire_tags (`issue#21956 <http://tracker.ceph.com/issues/21956>`_, `pr#18626 <https://github.com/ceph/ceph/pull/18626>`_, Jason Dillaman)
* rbd: cls/journal: possible infinite loop within tag_list class method (`issue#21771 <http://tracker.ceph.com/issues/21771>`_, `pr#18417 <https://github.com/ceph/ceph/pull/18417>`_, Jason Dillaman)
* rbd: [rbd-mirror] asok hook names not updated when image is renamed (`issue#20860 <http://tracker.ceph.com/issues/20860>`_, `pr#17860 <https://github.com/ceph/ceph/pull/17860>`_, Mykola Golub)
* rbd: [rbd-mirror] forced promotion can result in incorrect status (`issue#21559 <http://tracker.ceph.com/issues/21559>`_, `pr#18337 <https://github.com/ceph/ceph/pull/18337>`_, Jason Dillaman)
* rbd: [rbd-mirror] peer cluster connections should filter out command line optionals (`issue#21894 <http://tracker.ceph.com/issues/21894>`_, `pr#18566 <https://github.com/ceph/ceph/pull/18566>`_, Jason Dillaman)
* rgw: add support for Swift's per storage policy statistics (`issue#17932 <http://tracker.ceph.com/issues/17932>`_, `issue#21506 <http://tracker.ceph.com/issues/21506>`_, `pr#17835 <https://github.com/ceph/ceph/pull/17835>`_, Radoslaw Zarzynski, Casey Bodley)
* rgw: add support for Swift's reversed account listings (`issue#21148 <http://tracker.ceph.com/issues/21148>`_, `pr#17834 <https://github.com/ceph/ceph/pull/17834>`_, Radoslaw Zarzynski)
* rgw: avoid logging keystone revocation failures when no keystone is configured (`issue#21400 <http://tracker.ceph.com/issues/21400>`_, `pr#18441 <https://github.com/ceph/ceph/pull/18441>`_, Abhishek Lekshmanan)
* rgw: disable dynamic resharding in multisite enviorment (`issue#21725 <http://tracker.ceph.com/issues/21725>`_, `pr#18432 <https://github.com/ceph/ceph/pull/18432>`_, Orit Wasserman)
* rgw: encryption: PutObj response does not include sse-kms headers (`issue#21576 <http://tracker.ceph.com/issues/21576>`_, `pr#18442 <https://github.com/ceph/ceph/pull/18442>`_, Casey Bodley)
* rgw: encryption: reject requests that don't provide all expected headers (`issue#21581 <http://tracker.ceph.com/issues/21581>`_, `pr#18429 <https://github.com/ceph/ceph/pull/18429>`_, Enming Zhang)
* rgw: expose --sync-stats via admin api (`issue#21301 <http://tracker.ceph.com/issues/21301>`_, `pr#18439 <https://github.com/ceph/ceph/pull/18439>`_, Nathan Johnson)
* rgw: failed CompleteMultipartUpload request does not release lock (`issue#21596 <http://tracker.ceph.com/issues/21596>`_, `pr#18430 <https://github.com/ceph/ceph/pull/18430>`_, Matt Benjamin)
* rgw_file:  set s->obj_size from bytes_written (`issue#21940 <http://tracker.ceph.com/issues/21940>`_, `pr#18599 <https://github.com/ceph/ceph/pull/18599>`_, Matt Benjamin)
* rgw: fix a bug about inconsistent unit of comparison (`issue#21590 <http://tracker.ceph.com/issues/21590>`_, `pr#18438 <https://github.com/ceph/ceph/pull/18438>`_, gaosibei)
* rgw: fix bilog entries on multipart complete (`issue#21772 <http://tracker.ceph.com/issues/21772>`_, `pr#18334 <https://github.com/ceph/ceph/pull/18334>`_, Casey Bodley)
* rgw: fix error handling in ListBucketIndexesCR (`issue#21735 <http://tracker.ceph.com/issues/21735>`_, `pr#18591 <https://github.com/ceph/ceph/pull/18591>`_, Casey Bodley)
* rgw: fix refcnt issues (`issue#21819 <http://tracker.ceph.com/issues/21819>`_, `pr#18539 <https://github.com/ceph/ceph/pull/18539>`_, baixueyu)
* rgw: lc process only schdule the first item of lc objects (`issue#21022 <http://tracker.ceph.com/issues/21022>`_, `pr#17859 <https://github.com/ceph/ceph/pull/17859>`_, Shasha Lu)
* rgw: list bucket which enable versioning get wrong result when user marker (`issue#21500 <http://tracker.ceph.com/issues/21500>`_, `pr#18569 <https://github.com/ceph/ceph/pull/18569>`_, yuliyang)
* rgw: list_objects() honors end_marker regardless of namespace (`issue#18977 <http://tracker.ceph.com/issues/18977>`_, `pr#17832 <https://github.com/ceph/ceph/pull/17832>`_, Radoslaw Zarzynski)
* rgw: Multipart upload may double the quota (`issue#21586 <http://tracker.ceph.com/issues/21586>`_, `pr#18435 <https://github.com/ceph/ceph/pull/18435>`_, Sibei Gao)
* rgw: multisite: Get bucket location which is located in another zonegroup, will return 301 Moved Permanently (`issue#21125 <http://tracker.ceph.com/issues/21125>`_, `pr#17857 <https://github.com/ceph/ceph/pull/17857>`_, Shasha Lu)
* rgw: multisite: race between sync of bucket and bucket instance metadata (`issue#21990 <http://tracker.ceph.com/issues/21990>`_, `pr#18767 <https://github.com/ceph/ceph/pull/18767>`_, Casey Bodley)
* rgw: policy checks missing from Get/SetRequestPayment operations (`issue#21389 <http://tracker.ceph.com/issues/21389>`_, `pr#18440 <https://github.com/ceph/ceph/pull/18440>`_, Adam C. Emerson)
* rgw: radosgw-admin usage show loops indefinitly (`issue#21196 <http://tracker.ceph.com/issues/21196>`_, `pr#18437 <https://github.com/ceph/ceph/pull/18437>`_, Mark Kogan)
* rgw: rgw_file: explicit NFSv3 open() emulation (`issue#21854 <http://tracker.ceph.com/issues/21854>`_, `pr#18446 <https://github.com/ceph/ceph/pull/18446>`_, Matt Benjamin)
* rgw: rgw_file: fix write error when the write offset overlaps (`issue#21455 <http://tracker.ceph.com/issues/21455>`_, `pr#18004 <https://github.com/ceph/ceph/pull/18004>`_, Yao Zongyou)
* rgw: rgw file write error (`issue#21455 <http://tracker.ceph.com/issues/21455>`_, `pr#18433 <https://github.com/ceph/ceph/pull/18433>`_, Yao Zongyou)
* rgw: s3:GetBucketCORS/s3:PutBucketCORS policy fails with 403 (`issue#21578 <http://tracker.ceph.com/issues/21578>`_, `pr#18444 <https://github.com/ceph/ceph/pull/18444>`_, Adam C. Emerson)
* rgw:  s3:GetBucketLocation bucket policy fails with 403 (`issue#21582 <http://tracker.ceph.com/issues/21582>`_, `pr#18443 <https://github.com/ceph/ceph/pull/18443>`_, Adam C. Emerson)
* rgw: s3:GetBucketWebsite/PutBucketWebsite fails with 403 (`issue#21597 <http://tracker.ceph.com/issues/21597>`_, `pr#18445 <https://github.com/ceph/ceph/pull/18445>`_, Adam C. Emerson)
* rgw: setxattrs call leads to different mtimes for bucket index and object (`issue#21200 <http://tracker.ceph.com/issues/21200>`_, `pr#17856 <https://github.com/ceph/ceph/pull/17856>`_, Abhishek Lekshmanan)
* rgw: stop/join TokenCache revoke thread only if started (`issue#21666 <http://tracker.ceph.com/issues/21666>`_, `pr#18138 <https://github.com/ceph/ceph/pull/18138>`_, Karol Mroz)
* rgw: string_view instance points to expired memory in PrefixableSignatureHelper (`issue#21085 <http://tracker.ceph.com/issues/21085>`_, `pr#17858 <https://github.com/ceph/ceph/pull/17858>`_, Radoslaw Zarzynski)
* rgw: user creation can overwrite existing user even if different uid is given (`issue#21685 <http://tracker.ceph.com/issues/21685>`_, `pr#18436 <https://github.com/ceph/ceph/pull/18436>`_, Casey Bodley)
* rgw: We cant't get torrents if  objects are encrypted using SSE-C (`issue#21720 <http://tracker.ceph.com/issues/21720>`_, `pr#18431 <https://github.com/ceph/ceph/pull/18431>`_, Zhang Shaowen)
* rgw: wrong error message is returned when putting container with a name that is too long (`issue#17938 <http://tracker.ceph.com/issues/17938>`_, `issue#21169 <http://tracker.ceph.com/issues/21169>`_, `issue#17935 <http://tracker.ceph.com/issues/17935>`_, `issue#17934 <http://tracker.ceph.com/issues/17934>`_, `issue#17936 <http://tracker.ceph.com/issues/17936>`_, `pr#17811 <https://github.com/ceph/ceph/pull/17811>`_, Radoslaw Zarzynski)
* rgw: zone compression type is not validated (`issue#21775 <http://tracker.ceph.com/issues/21775>`_, `pr#18434 <https://github.com/ceph/ceph/pull/18434>`_, Casey Bodley)
* tools: ceph-disk create deprecation warnings (`issue#22154 <http://tracker.ceph.com/issues/22154>`_, `pr#18989 <https://github.com/ceph/ceph/pull/18989>`_, Alfredo Deza)
* tools: ceph-disk: fix '--runtime' omission for ceph-osd service (`issue#21498 <http://tracker.ceph.com/issues/21498>`_, `pr#17914 <https://github.com/ceph/ceph/pull/17914>`_, Carl Xiong)
* tools: ceph-disk flake8 test fails on very old, and very new, versions of flake8 (`issue#22207 <http://tracker.ceph.com/issues/22207>`_, `pr#19152 <https://github.com/ceph/ceph/pull/19152>`_, Nathan Cutler)
* tools: ceph-disk: retry on OSError (`issue#21728 <http://tracker.ceph.com/issues/21728>`_, `pr#18189 <https://github.com/ceph/ceph/pull/18189>`_, Kefu Chai)
* tools: ceph-disk: unlocks dmcrypted partitions when activating them (`issue#20488 <http://tracker.ceph.com/issues/20488>`_, `pr#18625 <https://github.com/ceph/ceph/pull/18625>`_, Kefu Chai, Felix Winterhalter)
* tools: ceph-kvstore-tool does not call bluestore's umount when exit (`issue#21625 <http://tracker.ceph.com/issues/21625>`_, `pr#18751 <https://github.com/ceph/ceph/pull/18751>`_, Chang Liu)
* tools: ceph_monstore_tool: rebuild initial mgrmap also (`issue#22266 <http://tracker.ceph.com/issues/22266>`_, `pr#19240 <https://github.com/ceph/ceph/pull/19240>`_, Kefu Chai)
* tools: ceph-objectstore-tool and ceph-bluestore-tool: backports from master (`issue#21272 <http://tracker.ceph.com/issues/21272>`_, `pr#17896 <https://github.com/ceph/ceph/pull/17896>`_, Sage Weil, David Zafman)
* tools: ceph_volume_client: add get, put, and delete object interfaces (`issue#21601 <http://tracker.ceph.com/issues/21601>`_, `pr#18037 <https://github.com/ceph/ceph/pull/18037>`_, Ramana Raja)
* tools: cli/crushtools/build.t sometimes fails in jenkins' make check run (`issue#21758 <http://tracker.ceph.com/issues/21758>`_, `pr#18398 <https://github.com/ceph/ceph/pull/18398>`_, Kefu Chai, Sage Weil)



v12.2.1 Luminous
================

This is the first bugfix release of Luminous v12.2.x long term stable
release series. It contains a range of bug fixes and a few features
across CephFS, RBD & RGW. We recommend all the users of 12.2.x series
update.

For more detailed information, see :download:`the complete changelog <../changelog/v12.2.1.txt>`.


Notable Changes
---------------

* Dynamic resharding is now enabled by default for RGW, RGW will now
  automatically reshard there bucket index once the index grows beyond
  `rgw_max_objs_per_shard`

* Limiting MDS cache via a memory limit is now supported using the new
  mds_cache_memory_limit config option (1GB by default).  A cache reservation
  can also be specified using mds_cache_reservation as a percentage of the
  limit (5% by default). Limits by inode count are still supported using
  mds_cache_size. Setting mds_cache_size to 0 (the default) disables the
  inode limit.

* The maximum number of PGs per OSD before the monitor issues a
  warning has been reduced from 300 to 200 PGs.  200 is still twice
  the generally recommended target of 100 PGs per OSD.  This limit can
  be adjusted via the ``mon_max_pg_per_osd`` option on the
  monitors.  The older ``mon_pg_warn_max_per_osd`` option has been removed.

* Creating pools or adjusting pg_num will now fail if the change would
  make the number of PGs per OSD exceed the configured
  ``mon_max_pg_per_osd`` limit.  The option can be adjusted if it
  is really necessary to create a pool with more PGs.

* There was a bug in the PG mapping behavior of the new *upmap*
  feature. If you made use of this feature (e.g., via the `ceph osd
  pg-upmap-items` command), we recommend that all mappings be removed (via
  the `ceph osd rm-pg-upmap-items` command) before upgrading to this
  point release.

* A stall in BlueStore IO submission that was affecting many users has
  been resolved.

Other Notable Changes
---------------------

* bluestore: asyn cdeferred_try_submit deadlock (`issue#21207 <http://tracker.ceph.com/issues/21207>`_, `pr#17494 <https://github.com/ceph/ceph/pull/17494>`_, Sage Weil)
* bluestore: fix deferred write deadlock, aio short return handling (`issue#21171 <http://tracker.ceph.com/issues/21171>`_, `pr#17601 <https://github.com/ceph/ceph/pull/17601>`_, Sage Weil)
* bluestore: osd crash when change option bluestore_csum_type from none to CRC32 (`issue#21175 <http://tracker.ceph.com/issues/21175>`_, `pr#17497 <https://github.com/ceph/ceph/pull/17497>`_, xie xingguo)
* bluestore: os/bluestore/BlueFS.cc: 1255: FAILED assert(!log_file->fnode.extents.empty()) (`issue#21250 <http://tracker.ceph.com/issues/21250>`_, `pr#17562 <https://github.com/ceph/ceph/pull/17562>`_, Sage Weil)
* build/ops: ceph-fuse RPM should require fusermount (`issue#21057 <http://tracker.ceph.com/issues/21057>`_, `pr#17470 <https://github.com/ceph/ceph/pull/17470>`_, Ken Dreyer)
* build/ops: RHEL 7.3 Selinux denials at OSD start (`issue#19200 <http://tracker.ceph.com/issues/19200>`_, `pr#17468 <https://github.com/ceph/ceph/pull/17468>`_, Boris Ranto)
* build/ops: rocksdb,cmake:  build portable binaries (`issue#20529 <http://tracker.ceph.com/issues/20529>`_, `pr#17745 <https://github.com/ceph/ceph/pull/17745>`_, Kefu Chai)
* cephfs: client/mds has wrong check to clear S_ISGID on chown (`issue#21004 <http://tracker.ceph.com/issues/21004>`_, `pr#17471 <https://github.com/ceph/ceph/pull/17471>`_, Patrick Donnelly)
* cephfs: get_quota_root sends lookupname op for every buffered write (`issue#20945 <http://tracker.ceph.com/issues/20945>`_, `pr#17473 <https://github.com/ceph/ceph/pull/17473>`_, Dan van der Ster)
* cephfs: MDCache::try_subtree_merge() may print N^2 lines of debug message (`issue#21221 <http://tracker.ceph.com/issues/21221>`_, `pr#17712 <https://github.com/ceph/ceph/pull/17712>`_, Patrick Donnelly)
* cephfs: MDS rank add/remove log messages say wrong number of ranks (`issue#21421 <http://tracker.ceph.com/issues/21421>`_, `pr#17887 <https://github.com/ceph/ceph/pull/17887>`_, John Spray)
* cephfs: MDS: standby-replay mds should avoid initiating subtree export (`issue#21378 <http://tracker.ceph.com/issues/21378>`_, `issue#21222 <http://tracker.ceph.com/issues/21222>`_, `pr#17714 <https://github.com/ceph/ceph/pull/17714>`_, "Yan, Zheng", Jianyu Li)
* cephfs: the standbys are not updated via ceph tell mds.\* command (`issue#21230 <http://tracker.ceph.com/issues/21230>`_, `pr#17565 <https://github.com/ceph/ceph/pull/17565>`_, Kefu Chai)
* common: adding line break at end of some cli results (`issue#21019 <http://tracker.ceph.com/issues/21019>`_, `pr#17467 <https://github.com/ceph/ceph/pull/17467>`_, songweibin)
* core: [cls] metadata_list API function does not honor `max_return` parameter (`issue#21247 <http://tracker.ceph.com/issues/21247>`_, `pr#17558 <https://github.com/ceph/ceph/pull/17558>`_, Jason Dillaman)
* core: incorrect erasure-code space in command ceph df (`issue#21243 <http://tracker.ceph.com/issues/21243>`_, `pr#17724 <https://github.com/ceph/ceph/pull/17724>`_, liuchang0812)
* core: interval_set: optimize intersect_of insert operations (`issue#21229 <http://tracker.ceph.com/issues/21229>`_, `pr#17487 <https://github.com/ceph/ceph/pull/17487>`_, Zac Medico)
* core: osd crush rule rename not idempotent (`issue#21162 <http://tracker.ceph.com/issues/21162>`_, `pr#17481 <https://github.com/ceph/ceph/pull/17481>`_, xie xingguo)
* core: osd/PGLog: write only changed dup entries (`issue#21026 <http://tracker.ceph.com/issues/21026>`_, `pr#17378 <https://github.com/ceph/ceph/pull/17378>`_, Josh Durgin)
* doc: doc/rbd: iSCSI Gateway Documentation (`issue#20437 <http://tracker.ceph.com/issues/20437>`_, `pr#17381 <https://github.com/ceph/ceph/pull/17381>`_, Aron Gunn, Jason Dillaman)
* mds: fix 'dirfrag end' check in Server::handle_client_readdir (`issue#21070 <http://tracker.ceph.com/issues/21070>`_, `pr#17686 <https://github.com/ceph/ceph/pull/17686>`_, "Yan, Zheng")
* mds: support limiting cache by memory (`issue#20594 <http://tracker.ceph.com/issues/20594>`_, `pr#17711 <https://github.com/ceph/ceph/pull/17711>`_, "Yan, Zheng", Patrick Donnelly)
* mgr: 500 error when attempting to view filesystem data (`issue#20692 <http://tracker.ceph.com/issues/20692>`_, `pr#17477 <https://github.com/ceph/ceph/pull/17477>`_, John Spray)
* mgr: ceph mgr versions shows active mgr as Unknown (`issue#21260 <http://tracker.ceph.com/issues/21260>`_, `pr#17635 <https://github.com/ceph/ceph/pull/17635>`_, John Spray)
* mgr: Crash in MonCommandCompletion (`issue#21157 <http://tracker.ceph.com/issues/21157>`_, `pr#17483 <https://github.com/ceph/ceph/pull/17483>`_, John Spray)
* mon: mon/OSDMonitor: deleting pool while pgs are being created leads to assert(p != pools.end) in update_creating_pgs() (`issue#21309 <http://tracker.ceph.com/issues/21309>`_, `pr#17634 <https://github.com/ceph/ceph/pull/17634>`_, Joao Eduardo Luis)
* mon: OSDMonitor: osd pool application get support (`issue#20976 <http://tracker.ceph.com/issues/20976>`_, `pr#17472 <https://github.com/ceph/ceph/pull/17472>`_, xie xingguo)
* mon: rate limit on health check update logging (`issue#20888 <http://tracker.ceph.com/issues/20888>`_, `pr#17500 <https://github.com/ceph/ceph/pull/17500>`_, John Spray)
* osd: build_initial_pg_history doesn't update up/acting/etc (`issue#21203 <http://tracker.ceph.com/issues/21203>`_, `pr#17496 <https://github.com/ceph/ceph/pull/17496>`_, w11979, Sage Weil)
* osd: osd/PG: discard msgs from down peers (`issue#19605 <http://tracker.ceph.com/issues/19605>`_, `pr#17501 <https://github.com/ceph/ceph/pull/17501>`_, Kefu Chai)
* osd/PrimaryLogPG: request osdmap update in the right block (`issue#21428 <http://tracker.ceph.com/issues/21428>`_, `pr#17829 <https://github.com/ceph/ceph/pull/17829>`_, Josh Durgin)
* osd: PrimaryLogPG: sparse read won't trigger repair correctly (`issue#21123 <http://tracker.ceph.com/issues/21123>`_, `pr#17475 <https://github.com/ceph/ceph/pull/17475>`_, xie xingguo)
* osd: request new map from PG when needed (`issue#21428 <http://tracker.ceph.com/issues/21428>`_, `pr#17796 <https://github.com/ceph/ceph/pull/17796>`_, Josh Durgin)
* osd: Revert "osd/OSDMap: allow bidirectional swap of pg-upmap-items" (`issue#21410 <http://tracker.ceph.com/issues/21410>`_, `pr#17812 <https://github.com/ceph/ceph/pull/17812>`_, Sage Weil)
* osd: subscribe to new osdmap while waiting_for_healthy (`issue#21121 <http://tracker.ceph.com/issues/21121>`_, `pr#17498 <https://github.com/ceph/ceph/pull/17498>`_, Sage Weil)
* osd: update info only if new_interval (`issue#21203 <http://tracker.ceph.com/issues/21203>`_, `pr#17622 <https://github.com/ceph/ceph/pull/17622>`_, Kefu Chai)
* pybind: dashboard usage graph getting bigger and bigger (`issue#20746 <http://tracker.ceph.com/issues/20746>`_, `pr#17486 <https://github.com/ceph/ceph/pull/17486>`_, Yixing Yan)
* rbd: image-meta list does not return all entries (`issue#21179 <http://tracker.ceph.com/issues/21179>`_, `pr#17561 <https://github.com/ceph/ceph/pull/17561>`_, Jason Dillaman)
* rbd: some generic options can not be passed by rbd-nbd (`issue#20426 <http://tracker.ceph.com/issues/20426>`_, `pr#17557 <https://github.com/ceph/ceph/pull/17557>`_, Pan Liu)
* rbd: switch to new config option getter methods (`issue#20737 <http://tracker.ceph.com/issues/20737>`_, `pr#17464 <https://github.com/ceph/ceph/pull/17464>`_, Jason Dillaman)
* rbd: TestMirroringWatcher.ModeUpdated: periodic failure due to injected message failures (`issue#21029 <http://tracker.ceph.com/issues/21029>`_, `pr#17465 <https://github.com/ceph/ceph/pull/17465>`_, Jason Dillaman)
* rgw: bucket index sporadically reshards to 65521 shards (`issue#20934 <http://tracker.ceph.com/issues/20934>`_, `pr#17476 <https://github.com/ceph/ceph/pull/17476>`_, Aleksei Gutikov)
* rgw: bytes_send and bytes_recv in the msg of usage show returning is 0 in master branch (`issue#19870 <http://tracker.ceph.com/issues/19870>`_, `pr#17444 <https://github.com/ceph/ceph/pull/17444>`_, Marcus Watts)
* rgw: data encryption sometimes fails to follow AWS settings (`issue#21349 <http://tracker.ceph.com/issues/21349>`_, `pr#17642 <https://github.com/ceph/ceph/pull/17642>`_, hechuang)
* rgw: memory leak in MetadataHandlers (`issue#21214 <http://tracker.ceph.com/issues/21214>`_, `pr#17570 <https://github.com/ceph/ceph/pull/17570>`_, Luo Kexue, Jos Collin)
* rgw: multisite: objects encrypted with SSE-KMS are stored unencrypted in target zone (`issue#20668 <http://tracker.ceph.com/issues/20668>`_, `issue#20671 <http://tracker.ceph.com/issues/20671>`_, `pr#17446 <https://github.com/ceph/ceph/pull/17446>`_, Casey Bodley)
* rgw: need to stream metadata full sync init (`issue#18079 <http://tracker.ceph.com/issues/18079>`_, `pr#17448 <https://github.com/ceph/ceph/pull/17448>`_, Yehuda Sadeh)
* rgw: object copied from remote src acl permission become full-control issue (`issue#20658 <http://tracker.ceph.com/issues/20658>`_, `pr#17478 <https://github.com/ceph/ceph/pull/17478>`_, Enming Zhang)
* rgw: put lifecycle configuration fails if Prefix is not set (`issue#19587 <http://tracker.ceph.com/issues/19587>`_, `issue#20872 <http://tracker.ceph.com/issues/20872>`_, `pr#17479 <https://github.com/ceph/ceph/pull/17479>`_, Shasha Lu, Abhishek Lekshmanan)
* rgw: rgw_file:  incorrect lane lock behavior in evict_block() (`issue#21141 <http://tracker.ceph.com/issues/21141>`_, `pr#17485 <https://github.com/ceph/ceph/pull/17485>`_, Matt Benjamin)
* rgw: send data-log list infinitely (`issue#20951 <http://tracker.ceph.com/issues/20951>`_, `pr#17445 <https://github.com/ceph/ceph/pull/17445>`_, fang.yuxiang)
* rgw: shadow objects are sometimes not removed (`issue#20234 <http://tracker.ceph.com/issues/20234>`_, `pr#17555 <https://github.com/ceph/ceph/pull/17555>`_, Yehuda Sadeh)
* rgw: usage of --inconsistent-index should require user confirmation and print a warning (`issue#20777 <http://tracker.ceph.com/issues/20777>`_, `pr#17488 <https://github.com/ceph/ceph/pull/17488>`_, Orit Wasserman)
* tools: [cli] rename of non-existent image results in seg fault (`issue#21248 <http://tracker.ceph.com/issues/21248>`_, `pr#17556 <https://github.com/ceph/ceph/pull/17556>`_, Jason Dillaman)


v12.2.0 Luminous
================

This is the first release of Luminous v12.2.x long term stable release
series.  There have been major changes since Kraken (v11.2.z) and
Jewel (v10.2.z), and the upgrade process is non-trivial. Please read
these release notes carefully.

Major Changes from Kraken
-------------------------

- *General*:

  * Ceph now has a simple, `built-in web-based dashboard
    <../mgr/dashboard>`_ for monitoring cluster status.

- *RADOS*:

  * *BlueStore*:

    - The new *BlueStore* backend for *ceph-osd* is now stable and the
      new default for newly created OSDs.  BlueStore manages data
      stored by each OSD by directly managing the physical HDDs or
      SSDs without the use of an intervening file system like XFS.
      This provides greater performance and features. See
      :doc:`/rados/configuration/storage-devices` and
      :doc:`/rados/configuration/bluestore-config-ref`.
    - BlueStore supports `full data and metadata checksums
      <../rados/configuration/bluestore-config-ref/#checksums>`_ of all
      data stored by Ceph.
    - BlueStore supports `inline compression
      <../rados/configuration/bluestore-config-ref/#inline-compression>`_ using
      zlib, snappy, or LZ4. (Ceph also supports zstd for `RGW compression
      <../man/8/radosgw-admin/#options>`_ but zstd is not recommended for
      BlueStore for performance reasons.)

  * *Erasure coded* pools now have `full support for overwrites
    <../rados/operations/erasure-code/#erasure-coding-with-overwrites>`_,
    allowing them to be used with RBD and CephFS.

  * *ceph-mgr*:

    - There is a new daemon, *ceph-mgr*, which is a required part of
      any Ceph deployment.  Although IO can continue when *ceph-mgr*
      is down, metrics will not refresh and some metrics-related calls
      (e.g., ``ceph df``) may block.  We recommend deploying several
      instances of *ceph-mgr* for reliability.  See the notes on
      `Upgrading`_ below.
    - The *ceph-mgr* daemon includes a `REST-based management API
      <../mgr/restful>`_. The API is still experimental and somewhat
      limited but will form the basis for API-based management of Ceph
      going forward.
    - *ceph-mgr* also includes a `Prometheus exporter
      <../mgr/prometheus>`_ plugin, which can provide Ceph
      perfcounters to Prometheus.
    - ceph-mgr now has a `Zabbix <../mgr/zabbix>`_ plugin. Using
      zabbix_sender it sends trapper events to a Zabbix server
      containing high-level information of the Ceph cluster. This
      makes it easy to monitor a Ceph cluster's status and send out
      notifications in case of a malfunction.

  * The overall *scalability* of the cluster has improved. We have
    successfully tested clusters with up to 10,000 OSDs.
  * Each OSD can now have a `device class
    <../rados/operations/crush-map/#device-classes>`_ associated with
    it (e.g., `hdd` or `ssd`), allowing CRUSH rules to trivially map
    data to a subset of devices in the system.  Manually writing CRUSH
    rules or manual editing of the CRUSH is normally not required.
  * There is a new `upmap <../rados/operations/upmap>`_ exception
    mechanism that allows individual PGs to be moved around to achieve
    a *perfect distribution* (this requires luminous clients).
  * Each OSD now adjusts its default configuration based on whether the
    backing device is an HDD or SSD.  Manual tuning generally not required.
  * The prototype `mClock QoS queueing algorithm
    <../rados/configuration/osd-config-ref/#qos-based-on-mclock>`_ is now
    available.
  * There is now a *backoff* mechanism that prevents OSDs from being
    overloaded by requests to objects or PGs that are not currently able to
    process IO.
  * There is a simplified `OSD replacement process
    <../rados/operations/add-or-rm-osds/#replacing-an-osd>`_ that is more
    robust.
  * You can query the supported features and (apparent) releases of
    all connected daemons and clients with `ceph features
    <../man/8/ceph#features>`_.
  * You can configure the oldest Ceph client version you wish to allow to
    connect to the cluster via ``ceph osd set-require-min-compat-client`` and
    Ceph will prevent you from enabling features that will break compatibility
    with those clients.
  * Several `sleep` settings, include ``osd_recovery_sleep``,
    ``osd_snap_trim_sleep``, and ``osd_scrub_sleep`` have been
    reimplemented to work efficiently.  (These are used in some cases
    to work around issues throttling background work.)
  * Pools are now expected to be associated with the application using them.
    Upon completing the upgrade to Luminous, the cluster will attempt to associate
    existing pools to known applications (i.e. CephFS, RBD, and RGW). In-use pools
    that are not associated to an application will generate a health warning. Any
    unassociated pools can be manually associated using the new
    ``ceph osd pool application enable`` command. For more details see
    `associate pool to application <../rados/operations/pools/#associate-pool-to-application>`_
    in the documentation.

- *RGW*:

  * RGW *metadata search* backed by ElasticSearch now supports end
    user requests service via RGW itself, and also supports custom
    metadata fields. A query language a set of RESTful APIs were
    created for users to be able to search objects by their
    metadata. New APIs that allow control of custom metadata fields
    were also added.
  * RGW now supports *dynamic bucket index sharding*. This has to be enabled via
    the `rgw dyamic resharding` configurable. As the number of objects in a
    bucket grows, RGW will automatically reshard the bucket index in response.
    No user intervention or bucket size capacity planning is required.
  * RGW introduces *server side encryption* of uploaded objects with
    three options for the management of encryption keys: automatic
    encryption (only recommended for test setups), customer provided
    keys similar to Amazon SSE-C specification, and through the use of
    an external key management service (Openstack Barbican) similar
    to Amazon SSE-KMS specification. :doc:`/radosgw/encryption`
  * RGW now has preliminary AWS-like bucket policy API support.  For
    now, policy is a means to express a range of new authorization
    concepts.  In the future it will be the foundation for additional
    auth capabilities such as STS and group policy. :doc:`/radosgw/bucketpolicy`
  * RGW has consolidated the several metadata index pools via the use of rados
    namespaces. :doc:`/radosgw/pools`
  * S3 Object Tagging API has been added; while APIs are
    supported for GET/PUT/DELETE object tags and in PUT object
    API, there is no support for tags on Policies & Lifecycle yet
  * RGW multisite now supports for enabling or disabling sync at a
    bucket level.

- *RBD*:

  * RBD now has full, stable support for *erasure coded pools* via the new
    ``--data-pool`` option to ``rbd create``.
  * RBD mirroring's rbd-mirror daemon is now highly available. We
    recommend deploying several instances of rbd-mirror for
    reliability.
  * RBD mirroring's rbd-mirror daemon should utilize unique Ceph user
    IDs per instance to support the new mirroring dashboard.
  * The default 'rbd' pool is no longer created automatically during
    cluster creation. Additionally, the name of the default pool used
    by the rbd CLI when no pool is specified can be overridden via a
    new ``rbd default pool = <pool name>`` configuration option.
  * Initial support for deferred image deletion via new ``rbd
    trash`` CLI commands. Images, even ones actively in-use by
    clones, can be moved to the trash and deleted at a later time.
  * New pool-level ``rbd mirror pool promote`` and ``rbd mirror pool
    demote`` commands to batch promote/demote all mirrored images
    within a pool.
  * Mirroring now optionally supports a configurable replication delay
    via the ``rbd mirroring replay delay = <seconds>`` configuration
    option.
  * Improved discard handling when the object map feature is enabled.
  * rbd CLI ``import`` and ``copy`` commands now detect sparse and
    preserve sparse regions.
  * Images and Snapshots will now include a creation timestamp.
  * Specifying user authorization capabilities for RBD clients has been
    simplified. The general syntax for using RBD capability profiles is
    "mon 'profile rbd' osd 'profile rbd[-read-only][ pool={pool-name}[, ...]]'".
    For more details see :doc:`/rados/operations/user-management`
    in the documentation.

- *CephFS*:

  * *Multiple active MDS daemons* is now considered stable.  The number
    of active MDS servers may be adjusted up or down on an active CephFS file
    system.
  * CephFS *directory fragmentation* is now stable and enabled by
    default on new filesystems.  To enable it on existing filesystems
    use "ceph fs set <fs_name> allow_dirfrags".  Large or very busy
    directories are sharded and (potentially) distributed across
    multiple MDS daemons automatically.
  * Directory subtrees can be explicitly pinned to specific MDS daemons in
    cases where the automatic load balancing is not desired or effective.
  * Client keys can now be created using the new ``ceph fs authorize`` command
    to create keys with access to the given CephFS file system and all of its
    data pools.
  * When running 'df' on a CephFS filesystem comprising exactly one data pool,
    the result now reflects the file storage space used and available in that
    data pool (fuse client only).

- *Miscellaneous*:

  * Release packages are now being built for *Debian Stretch*.  Note
    that QA is limited to CentOS and Ubuntu (xenial and trusty).  The
    distributions we build for now include:

    - CentOS 7 (x86_64 and aarch64)
    - Debian 8 Jessie (x86_64)
    - Debian 9 Stretch (x86_64)
    - Ubuntu 16.04 Xenial (x86_64 and aarch64)
    - Ubuntu 14.04 Trusty (x86_64)

  * A first release of Ceph for FreeBSD is available which contains a full set
    of features, other than Bluestore. It will run everything needed to build a
    storage cluster. For clients, all access methods are available, albeit
    CephFS is only accessible through a Fuse implementation. RBD images can be
    mounted on FreeBSD systems through rbd-ggate
    Ceph versions are released through the regular FreeBSD ports and packages
    system. The most current version is available as: net/ceph-devel. Once
    Luminous goes into official release, this version will be available as
    net/ceph. Future development releases will be available via net/ceph-devel
    More details about this port are in: `README.FreeBSD <https://github.com/ceph/ceph/blob/master/README.FreeBSD>`_

  * *CLI changes*:

    - The ``ceph -s`` or ``ceph status`` command has a fresh look.
    - ``ceph mgr metadata`` will dump metadata associated with each mgr
      daemon.
    - ``ceph versions`` or ``ceph {osd,mds,mon,mgr} versions``
      summarize versions of running daemons.
    - ``ceph {osd,mds,mon,mgr} count-metadata <property>`` similarly
      tabulates any other daemon metadata visible via the ``ceph
      {osd,mds,mon,mgr} metadata`` commands.
    - ``ceph features`` summarizes features and releases of connected
      clients and daemons.
    - ``ceph osd require-osd-release <release>`` replaces the old
      ``require_RELEASE_osds`` flags.
    - ``ceph osd pg-upmap``, ``ceph osd rm-pg-upmap``, ``ceph osd
      pg-upmap-items``, ``ceph osd rm-pg-upmap-items`` can explicitly
      manage `upmap` items (see :doc:`/rados/operations/upmap`).
    - ``ceph osd getcrushmap`` returns a crush map version number on
      stderr, and ``ceph osd setcrushmap [version]`` will only inject
      an updated crush map if the version matches.  This allows crush
      maps to be updated offline and then reinjected into the cluster
      without fear of clobbering racing changes (e.g., by newly added
      osds or changes by other administrators).
    - ``ceph osd create`` has been replaced by ``ceph osd new``.  This
      should be hidden from most users by user-facing tools like
      `ceph-disk`.
    - ``ceph osd destroy`` will mark an OSD destroyed and remove its
      cephx and lockbox keys.  However, the OSD id and CRUSH map entry
      will remain in place, allowing the id to be reused by a
      replacement device with minimal data rebalancing.
    - ``ceph osd purge`` will remove all traces of an OSD from the
      cluster, including its cephx encryption keys, dm-crypt lockbox
      keys, OSD id, and crush map entry.
    - ``ceph osd ls-tree <name>`` will output a list of OSD ids under
      the given CRUSH name (like a host or rack name).  This is useful
      for applying changes to entire subtrees.  For example, ``ceph
      osd down `ceph osd ls-tree rack1```.
    - ``ceph osd {add,rm}-{noout,noin,nodown,noup}`` allow the
      `noout`, `noin`, `nodown`, and `noup` flags to be applied to
      specific OSDs.
    - ``ceph osd safe-to-destroy <osd(s)>`` will report whether it is safe to
      remove or destroy OSD(s) without reducing data durability or redundancy.
    - ``ceph osd ok-to-stop <osd(s)>`` will report whether it is okay to stop
      OSD(s) without immediately compromising availability (i.e., all PGs
      should remain active but may be degraded).
    - ``ceph log last [n]`` will output the last *n* lines of the cluster
      log.
    - ``ceph mgr dump`` will dump the MgrMap, including the currently active
      ceph-mgr daemon and any standbys.
    - ``ceph mgr module ls`` will list active ceph-mgr modules.
    - ``ceph mgr module {enable,disable} <name>`` will enable or
      disable the named mgr module.  The module must be present in the
      configured `mgr_module_path` on the host(s) where `ceph-mgr` is
      running.
    - ``ceph osd crush ls <node>`` will list items (OSDs or other CRUSH nodes)
      directly beneath a given CRUSH node.
    - ``ceph osd crush swap-bucket <src> <dest>`` will swap the
      contents of two CRUSH buckets in the hierarchy while preserving
      the buckets' ids.  This allows an entire subtree of devices to
      be replaced (e.g., to replace an entire host of FileStore OSDs
      with newly-imaged BlueStore OSDs) without disrupting the
      distribution of data across neighboring devices.
    - ``ceph osd set-require-min-compat-client <release>`` configures
      the oldest client release the cluster is required to support.
      Other changes, like CRUSH tunables, will fail with an error if
      they would violate this setting.  Changing this setting also
      fails if clients older than the specified release are currently
      connected to the cluster.
    - ``ceph config-key dump`` dumps config-key entries and their
      contents.  (The existing ``ceph config-key list`` only dumps the key
      names, not the values.)
    - ``ceph config-key list`` is deprecated in favor of ``ceph config-key ls``.
    - ``ceph config-key put`` is deprecated in favor of ``ceph config-key set``.
    - ``ceph auth list`` is deprecated in favor of ``ceph auth ls``.
    - ``ceph osd crush rule list`` is deprecated in favor of ``ceph osd crush rule ls``.
    - ``ceph osd set-{full,nearfull,backfillfull}-ratio`` sets the
      cluster-wide ratio for various full thresholds (when the cluster
      refuses IO, when the cluster warns about being close to full,
      when an OSD will defer rebalancing a PG to itself,
      respectively).
    - ``ceph osd reweightn`` will specify the `reweight` values for
      multiple OSDs in a single command.  This is equivalent to a series of
      ``ceph osd reweight`` commands.
    - ``ceph osd crush {set,rm}-device-class`` manage the new
      CRUSH *device class* feature. Note that manually creating or deleting
      a device class name is generally not necessary as it will be smart
      enough to be self-managed. ``ceph osd crush class ls`` and
      ``ceph osd crush class ls-osd`` will output all existing device classes
      and a list of OSD ids under the given device class respectively.
    - ``ceph osd crush rule create-replicated`` replaces the old
      ``ceph osd crush rule create-simple`` command to create a CRUSH
      rule for a replicated pool.  Notably it takes a `class` argument
      for the *device class* the rule should target (e.g., `ssd` or
      `hdd`).
    - ``ceph mon feature ls`` will list monitor features recorded in the
      MonMap.  ``ceph mon feature set`` will set an optional feature (none of
      these exist yet).
    - ``ceph tell <daemon> help`` will now return a usage summary.
    - ``ceph fs authorize`` creates a new client key with caps automatically
      set to access the given CephFS file system.
    - The ``ceph health`` structured output (JSON or XML) no longer contains
      'timechecks' section describing the time sync status.  This
      information is now available via the 'ceph time-sync-status'
      command.
    - Certain extra fields in the ``ceph health`` structured output that
      used to appear if the mons were low on disk space (which duplicated
      the information in the normal health warning messages) are now gone.
    - The ``ceph -w`` output no longer contains audit log entries by default.
      Add a ``--watch-channel=audit`` or ``--watch-channel=*`` to see them.
    - New "ceph -w" behavior - the "ceph -w" output no longer contains
      I/O rates, available space, pg info, etc. because these are no
      longer logged to the central log (which is what ``ceph -w``
      shows). The same information can be obtained by running ``ceph pg
      stat``; alternatively, I/O rates per pool can be determined using
      ``ceph osd pool stats``. Although these commands do not
      self-update like ``ceph -w`` did, they do have the ability to
      return formatted output by providing a ``--format=<format>``
      option.
    - Added new commands ``pg force-recovery`` and
      ``pg-force-backfill``. Use them to boost recovery or backfill
      priority of specified pgs, so they're recovered/backfilled
      before any other. Note that these commands don't interrupt
      ongoing recovery/backfill, but merely queue specified pgs before
      others so they're recovered/backfilled as soon as possible. New
      commands ``pg cancel-force-recovery`` and ``pg
      cancel-force-backfill`` restore default recovery/backfill
      priority of previously forced pgs.


Major Changes from Jewel
------------------------

- *RADOS*:

  * We now default to the AsyncMessenger (``ms type = async``) instead
    of the legacy SimpleMessenger.  The most noticeable difference is
    that we now use a fixed sized thread pool for network connections
    (instead of two threads per socket with SimpleMessenger).
  * Some OSD failures are now detected almost immediately, whereas
    previously the heartbeat timeout (which defaults to 20 seconds)
    had to expire.  This prevents IO from blocking for an extended
    period for failures where the host remains up but the ceph-osd
    process is no longer running.
  * The size of encoded OSDMaps has been reduced.
  * The OSDs now quiesce scrubbing when recovery or rebalancing is in progress.

- *RGW*:

  * RGW now supports the S3 multipart object copy-part API.
  * It is possible now to reshard an existing bucket offline. Offline
    bucket resharding currently requires that all IO (especially
    writes) to the specific bucket is quiesced.  (For automatic online
    resharding, see the new feature in Luminous above.)
  * RGW now supports data compression for objects.
  * Civetweb version has been upgraded to 1.8
  * The Swift static website API is now supported (S3 support has been added
    previously).
  * S3 bucket lifecycle API has been added. Note that currently it only supports
    object expiration.
  * Support for custom search filters has been added to the LDAP auth
    implementation.
  * Support for NFS version 3 has been added to the RGW NFS gateway.
  * A Python binding has been created for librgw.

- *RBD*:

  * The rbd-mirror daemon now supports replicating dynamic image
    feature updates and image metadata key/value pairs from the
    primary image to the non-primary image.
  * The number of image snapshots can be optionally restricted to a
    configurable maximum.
  * The rbd Python API now supports asynchronous IO operations.

- *CephFS*:

  * libcephfs function definitions have been changed to enable proper
    uid/gid control.  The library version has been increased to reflect the
    interface change.
  * Standby replay MDS daemons now consume less memory on workloads
    doing deletions.
  * Scrub now repairs backtrace, and populates `damage ls` with
    discovered errors.
  * A new `pg_files` subcommand to `cephfs-data-scan` can identify
    files affected by a damaged or lost RADOS PG.
  * The false-positive "failing to respond to cache pressure" warnings have
    been fixed.


Upgrade from Jewel or Kraken
----------------------------
.. _Upgrading:

#. Ensure that the ``sortbitwise`` flag is enabled::

     # ceph osd set sortbitwise

#. Make sure your cluster is stable and healthy (no down or
   recovering OSDs).  (Optional, but recommended.)

#. Do not create any new erasure-code pools while upgrading the monitors.

#. You can monitor the progress of your upgrade at each stage with the
   ``ceph versions`` command, which will tell you what ceph version is
   running for each type of daemon.

#. Set the ``noout`` flag for the duration of the upgrade. (Optional
   but recommended.)::

     # ceph osd set noout

#. Verify that all RBD client users have sufficient caps to blacklist
   other client users. RBD client users with only ``"allow r"``
   monitor caps should be updated as follows::

     # ceph auth caps client.<ID> mon 'allow r, allow command "osd blacklist"' osd '<existing OSD caps for user>'

#. Upgrade monitors by installing the new packages and restarting the
   monitor daemons.  Note that, unlike prior releases, the ceph-mon
   daemons *must* be upgraded first::

     # systemctl restart ceph-mon.target

   Verify the monitor upgrade is complete once all monitors are up by
   looking for the ``luminous`` feature string in the mon map.  For
   example::

     # ceph mon feature ls

   should include `luminous` under persistent features::

     on current monmap (epoch NNN)
        persistent: [kraken,luminous]
        required: [kraken,luminous]

#. Add or restart ``ceph-mgr`` daemons.  If you are upgrading from
   kraken, upgrade packages and restart ceph-mgr daemons with::

     # systemctl restart ceph-mgr.target

   If you are upgrading from kraken, you may already have ceph-mgr
   daemons deployed.  If not, or if you are upgrading from jewel, you
   can deploy new daemons with tools like ceph-deploy or ceph-ansible.
   For example::

     # ceph-deploy mgr create HOST

   Verify the ceph-mgr daemons are running by checking ``ceph -s``::

     # ceph -s

     ...
       services:
        mon: 3 daemons, quorum foo,bar,baz
        mgr: foo(active), standbys: bar, baz
     ...

#. Upgrade all OSDs by installing the new packages and restarting the
   ceph-osd daemons on all hosts::

     # systemctl restart ceph-osd.target

   You can monitor the progress of the OSD upgrades with the new
   ``ceph versions`` or ``ceph osd versions`` command::

     # ceph osd versions
     {
        "ceph version 12.2.0 (...) luminous (stable)": 12,
        "ceph version 10.2.6 (...)": 3,
     }

#. Upgrade all CephFS daemons by upgrading packages and restarting
   daemons on all hosts::

     # systemctl restart ceph-mds.target

#. Upgrade all radosgw daemons by upgrading packages and restarting
   daemons on all hosts::

     # systemctl restart radosgw.target

#. Complete the upgrade by disallowing pre-luminous OSDs and enabling
   all new Luminous-only functionality::

     # ceph osd require-osd-release luminous

   If you set ``noout`` at the beginning, be sure to clear it with::

     # ceph osd unset noout

#. Verify the cluster is healthy with ``ceph health``.


Upgrading from pre-Jewel releases (like Hammer)
-----------------------------------------------

You *must* first upgrade to Jewel (10.2.z) before attempting an
upgrade to Luminous.

Upgrade compatibility notes, Jewel to Kraken
--------------------------------------------

These changes occurred between the Jewel and Kraken releases and will affect
upgrades from Jewel to Luminous.

* The ``osd crush location`` config option is no longer supported.  Please
  update your ceph.conf to use the ``crush location`` option instead.  Be sure
  to update your config file to avoid any movement of OSDs from your customized
  location back to the default one.

* The OSDs now avoid starting new scrubs while recovery is in progress.  To
  revert to the old behavior (and do not let recovery activity affect the
  scrub scheduling) you can set the following option::

    osd scrub during recovery = true

* The list of monitor hosts/addresses for building the monmap can now be
  obtained from DNS SRV records. The service name used in when querying the DNS
  is defined in the "mon_dns_srv_name" config option, which defaults to
  "ceph-mon".

* The 'osd class load list' config option is a list of object class names that
  the OSD is permitted to load (or '*' for all classes). By default it
  contains all existing in-tree classes for backwards compatibility.

* The 'osd class default list' config option is a list of object class
  names (or '*' for all classes) that clients may invoke having only
  the '*', 'x', 'class-read', or 'class-write' capabilities. By
  default it contains all existing in-tree classes for backwards
  compatibility. Invoking classes not listed in 'osd class default
  list' requires a capability naming the class (e.g. 'allow class
  foo').

* The 'rgw rest getusage op compat' config option allows you to dump
  (or not dump) the description of user stats in the S3 GetUsage
  API. This option defaults to false.  If the value is true, the
  response data for GetUsage looks like::

    "stats": {
                "TotalBytes": 516,
                "TotalBytesRounded": 1024,
                "TotalEntries": 1
             }

  If the value is false, the response for GetUsage looks as it did before::

    {
         516,
         1024,
         1
    }

* The 'osd out ...' and 'osd in ...' commands now preserve the OSD
  weight.  That is, after marking an OSD out and then in, the weight
  will be the same as before (instead of being reset to 1.0).
  Previously the mons would only preserve the weight if the mon
  automatically marked an OSD out and then in, but not when an admin
  did so explicitly.

* The 'ceph osd perf' command will display 'commit_latency(ms)' and
  'apply_latency(ms)'. Previously, the names of these two columns were
  'fs_commit_latency(ms)' and 'fs_apply_latency(ms)'. We removed the
  prefix 'fs\_' because the values are not filestore-specific.

* Monitors will no longer allow pools to be removed by default.  The
  setting mon_allow_pool_delete has to be set to true (defaults to
  false) before they allow pools to be removed.  This is a additional
  safeguard against pools being removed by accident.

* If you have manually specified the monitor user rocksdb via the
  ``mon keyvaluedb = rocksdb`` option, you will need to manually add a
  file to the mon data directory to preserve this option::

     echo rocksdb > /var/lib/ceph/mon/ceph-`hostname`/kv_backend

  New monitors will now use rocksdb by default, but if that file is
  not present, existing monitors will use leveldb.  The ``mon
  keyvaluedb`` option now only affects the backend chosen when a
  monitor is created.

* The 'osd crush initial weight' option allows you to specify a CRUSH
  weight for a newly added OSD.  Previously a value of 0 (the default)
  meant that we should use the size of the OSD's store to weight the
  new OSD.  Now, a value of 0 means it should have a weight of 0, and
  a negative value (the new default) means we should automatically
  weight the OSD based on its size.  If your configuration file
  explicitly specifies a value of 0 for this option you will need to
  change it to a negative value (e.g., -1) to preserve the current
  behavior.

* The static libraries are no longer included by the debian
  development packages (lib*-dev) as it is not required per debian
  packaging policy.  The shared (.so) versions are packaged as before.

* The libtool pseudo-libraries (.la files) are no longer included by
  the debian development packages (lib*-dev) as they are not required
  per https://wiki.debian.org/ReleaseGoals/LAFileRemoval and
  https://www.debian.org/doc/manuals/maint-guide/advanced.en.html.

* The jerasure and shec plugins can now detect SIMD instruction at
  runtime and no longer need to be explicitly configured for different
  processors.  The following plugins are now deprecated:
  jerasure_generic, jerasure_sse3, jerasure_sse4, jerasure_neon,
  shec_generic, shec_sse3, shec_sse4, and shec_neon. If you use any of
  these plugins directly you will see a warning in the mon log file.
  Please switch to using just 'jerasure' or 'shec'.

* The librados omap get_keys and get_vals operations include a start key and a
  limit on the number of keys to return.  The OSD now imposes a configurable
  limit on the number of keys and number of total bytes it will respond with,
  which means that a librados user might get fewer keys than they asked for.
  This is necessary to prevent careless users from requesting an unreasonable
  amount of data from the cluster in a single operation.  The new limits are
  configured with ``osd_max_omap_entries_per_request``, defaulting to 131,072, and
  ``osd_max_omap_bytes_per_request``, defaulting to 4MB.

* Calculation of recovery priorities has been updated.
  This could lead to unintuitive recovery prioritization
  during cluster upgrade. In case of such recovery, OSDs
  in old version would operate on different priority ranges
  than new ones. Once upgraded, cluster will operate on
  consistent values.


Upgrade compatibility notes, Kraken to Luminous
-----------------------------------------------

* The configuration option ``osd pool erasure code stripe width`` has
  been replaced by ``osd pool erasure code stripe unit``, and given
  the ability to be overridden by the erasure code profile setting
  ``stripe_unit``. For more details see
  :ref:`erasure-code-profiles`.

* rbd and cephfs can use erasure coding with bluestore. This may be
  enabled by setting ``allow_ec_overwrites`` to ``true`` for a pool. Since
  this relies on bluestore's checksumming to do deep scrubbing,
  enabling this on a pool stored on filestore is not allowed.

* The ``rados df`` JSON output now prints numeric values as numbers instead of
  strings.

* The ``mon_osd_max_op_age`` option has been renamed to
  ``mon_osd_warn_op_age`` (default: 32 seconds), to indicate we
  generate a warning at this age.  There is also a new
  ``mon_osd_err_op_age_ratio`` that is a expressed as a multiple of
  ``mon_osd_warn_op_age`` (default: 128, for roughly 60 minutes) to
  control when an error is generated.

* The default maximum size for a single RADOS object has been reduced from
  100GB to 128MB.  The 100GB limit was completely impractical in practice
  while the 128MB limit is a bit high but not unreasonable.  If you have an
  application written directly to librados that is using objects larger than
  128MB you may need to adjust ``osd_max_object_size``.

* The semantics of the ``rados ls`` and librados object listing
  operations have always been a bit confusing in that "whiteout"
  objects (which logically don't exist and will return ENOENT if you
  try to access them) are included in the results.  Previously
  whiteouts only occurred in cache tier pools.  In luminous, logically
  deleted but snapshotted objects now result in a whiteout object, and
  as a result they will appear in ``rados ls`` results, even though
  trying to read such an object will result in ENOENT.  The ``rados
  listsnaps`` operation can be used in such a case to enumerate which
  snapshots are present.
  This may seem a bit strange, but is less strange than having a
  deleted-but-snapshotted object not appear at all and be completely
  hidden from librados's ability to enumerate objects.  Future
  versions of Ceph will likely include an alternative object
  enumeration interface that makes it more natural and efficient to
  enumerate all objects along with their snapshot and clone metadata.

* The deprecated ``crush_ruleset`` property has finally been removed;
  please use  ``crush_rule`` instead for the ``osd pool get ...`` and ``osd
  pool set ...`` commands.

* The ``osd pool default crush replicated ruleset`` option has been
  removed and replaced by the ``osd pool default crush rule`` option.
  By default it is -1, which means the mon will pick the first type
  replicated rule in the CRUSH map for replicated pools.  Erasure
  coded pools have rules that are automatically created for them if
  they are not specified at pool creation time.

* We no longer test the FileStore ceph-osd backend in combination with
  btrfs.  We recommend against using btrfs.  If you are using
  btrfs-based OSDs and want to upgrade to luminous you will need to
  add the following to your ceph.conf::

    enable experimental unrecoverable data corrupting features = btrfs

  The code is mature and unlikely to change, but we are only
  continuing to test the Jewel stable branch against btrfs.  We
  recommend moving these OSDs to FileStore with XFS or BlueStore.
* The ``ruleset-*`` properties for the erasure code profiles have been
  renamed to ``crush-*`` to move away from the obsolete 'ruleset'
  term and to be more clear about their purpose.  There is also a new
  optional ``crush-device-class`` property to specify a CRUSH device
  class to use for the erasure coded pool.  Existing erasure code
  profiles will be converted automatically when upgrade completes
  (when the ``ceph osd require-osd-release luminous`` command is run)
  but any provisioning tools that create erasure coded pools may need
  to be updated.
* The structure of the XML output for ``osd crush tree`` has changed
  slightly to better match the ``osd tree`` output.  The top level
  structure is now ``nodes`` instead of ``crush_map_roots``.
* When assigning a network to the public network and not to
  the cluster network the network specification of the public
  network will be used for the cluster network as well.
  In older versions this would lead to cluster services
  being bound to 0.0.0.0:<port>, thus making the
  cluster service even more publicly available than the
  public services. When only specifying a cluster network it
  will still result in the public services binding to 0.0.0.0.

* In previous versions, if a client sent an op to the wrong OSD, the OSD
  would reply with ENXIO.  The rationale here is that the client or OSD is
  clearly buggy and we want to surface the error as clearly as possible.
  We now only send the ENXIO reply if the osd_enxio_on_misdirected_op option
  is enabled (it's off by default).  This means that a VM using librbd that
  previously would have gotten an EIO and gone read-only will now see a
  blocked/hung IO instead.

* The "journaler allow split entries" config setting has been removed.

* The 'mon_warn_osd_usage_min_max_delta' config option has been
  removed and the associated health warning has been disabled because
  it does not address clusters undergoing recovery or CRUSH rules that do
  not target all devices in the cluster.

* Added new configuration "public bind addr" to support dynamic
  environments like Kubernetes. When set the Ceph MON daemon could
  bind locally to an IP address and advertise a different IP address
  ``public addr`` on the network.

* The crush ``choose_args`` encoding has been changed to make it
  architecture-independent. If you deployed Luminous dev releases or
  12.1.0 rc release and made use of the CRUSH choose_args feature, you
  need to remove all choose_args mappings from your CRUSH map before
  starting the upgrade.


- *librados*:

  * Some variants of the omap_get_keys and omap_get_vals librados
    functions have been deprecated in favor of omap_get_vals2 and
    omap_get_keys2.  The new methods include an output argument
    indicating whether there are additional keys left to fetch.
    Previously this had to be inferred from the requested key count vs
    the number of keys returned, but this breaks with new OSD-side
    limits on the number of keys or bytes that can be returned by a
    single omap request.  These limits were introduced by kraken but
    are effectively disabled by default (by setting a very large limit
    of 1 GB) because users of the newly deprecated interface cannot
    tell whether they should fetch more keys or not.  In the case of
    the standalone calls in the C++ interface
    (IoCtx::get_omap_{keys,vals}), librados has been updated to loop on
    the client side to provide a correct result via multiple calls to
    the OSD.  In the case of the methods used for building
    multi-operation transactions, however, client-side looping is not
    practical, and the methods have been deprecated.  Note that use of
    either the IoCtx methods on older librados versions or the
    deprecated methods on any version of librados will lead to
    incomplete results if/when the new OSD limits are enabled.

  * The original librados rados_objects_list_open (C) and objects_begin
    (C++) object listing API, deprecated in Hammer, has finally been
    removed.  Users of this interface must update their software to use
    either the rados_nobjects_list_open (C) and nobjects_begin (C++) API or
    the new rados_object_list_begin (C) and object_list_begin (C++) API
    before updating the client-side librados library to Luminous.
    Object enumeration (via any API) with the latest librados version
    and pre-Hammer OSDs is no longer supported.  Note that no in-tree
    Ceph services rely on object enumeration via the deprecated APIs, so
    only external librados users might be affected.
    The newest (and recommended) rados_object_list_begin (C) and
    object_list_begin (C++) API is only usable on clusters with the
    SORTBITWISE flag enabled (Jewel and later).  (Note that this flag is
    required to be set before upgrading beyond Jewel.)

- *CephFS*:

  * When configuring ceph-fuse mounts in /etc/fstab, a new syntax is
    available that uses "ceph.<arg>=<val>" in the options column, instead
    of putting configuration in the device column.  The old style syntax
    still works.  See the documentation page "Mount CephFS in your
    file systems table" for details.
  * CephFS clients without the 'p' flag in their authentication capability
    string will no longer be able to set quotas or any layout fields.  This
    flag previously only restricted modification of the pool and namespace
    fields in layouts.
  * CephFS will generate a health warning if you have fewer standby daemons
    than it thinks you wanted.  By default this will be 1 if you ever had
    a standby, and 0 if you did not.  You can customize this using
    ``ceph fs set <fs> standby_count_wanted <number>``.  Setting it
    to zero will effectively disable the health check.
  * The "ceph mds tell ..." command has been removed.  It is superseded
    by "ceph tell mds.<id> ..."
  * The ``apply`` mode of cephfs-journal-tool has been removed


Other Notable Changes
---------------------
* async: Fixed compilation error when enable -DWITH_DPDK (`pr#12660 <https://github.com/ceph/ceph/pull/12660>`_, Pan Liu)
* async: fixed coredump when enable dpdk (`pr#12854 <https://github.com/ceph/ceph/pull/12854>`_, Pan Liu)
* async: fixed the error "Cause: Cannot create lock on '/var/run/.rte_c… (`pr#12860 <https://github.com/ceph/ceph/pull/12860>`_, Pan Liu)
* bluestore: avoid unnecessary copy with coll_t (`pr#12576 <https://github.com/ceph/ceph/pull/12576>`_, Yunchuan Wen)
* bluestore: BitAllocator: delete useless codes (`pr#13619 <https://github.com/ceph/ceph/pull/13619>`_, Jie Wang)
* bluestore: bluestore/BlueFS: pass string as const ref (`pr#16600 <https://github.com/ceph/ceph/pull/16600>`_, dingdangzhang)
* bluestore: bluestore, NVMEDEVICE: Specify the max io completion in conf (`pr#13799 <https://github.com/ceph/ceph/pull/13799>`_, optimistyzy)
* bluestore: bluestore/NVMEDEVICE: update SPDK to version 17.03 (`pr#14585 <https://github.com/ceph/ceph/pull/14585>`_, optimistyzy)
* bluestore: bluestore, NVMeDevice: use task' own lock for (random) read (`pr#14094 <https://github.com/ceph/ceph/pull/14094>`_, optimistyzy)
* bluestore,build/ops,performance: os/bluestore: enable SSE-assisted CRC32 calculations in RocksDB (`pr#13741 <https://github.com/ceph/ceph/pull/13741>`_, Radoslaw Zarzynski)
* bluestore: ceph-disk: add --filestore argument, default to --bluestore (`pr#15437 <https://github.com/ceph/ceph/pull/15437>`_, Loic Dachary, Sage Weil)
* bluestore: common/config: set rocksdb_cache_size to OPT_U64 (`pr#13995 <https://github.com/ceph/ceph/pull/13995>`_, liuhongtong)
* bluestore: common/options: make "blue{fs,store}_allocator" LEVEL_DEV (`issue#20660 <http://tracker.ceph.com/issues/20660>`_, `pr#16645 <https://github.com/ceph/ceph/pull/16645>`_, Kefu Chai)
* bluestore,common,performance: common/Finisher: Using queue(list<context\*>) instead queue(context\*) (`pr#8942 <https://github.com/ceph/ceph/pull/8942>`_, Jianpeng Ma)
* bluestore,common,performance: isa-l: update isa-l to v2.18 (`pr#15895 <https://github.com/ceph/ceph/pull/15895>`_, Ganesh Mahalingam, Tushar Gohad)
* bluestore,core: os/bluestore: fix statfs to not include DB partition in free space (`issue#18599 <http://tracker.ceph.com/issues/18599>`_, `pr#13140 <https://github.com/ceph/ceph/pull/13140>`_, Sage Weil)
* bluestore,core: os/bluestore: fix warning (`pr#15435 <https://github.com/ceph/ceph/pull/15435>`_, Sage Weil)
* bluestore,core: os/bluestore: improve mempool usage (`pr#15402 <https://github.com/ceph/ceph/pull/15402>`_, Sage Weil)
* bluestore,core: os/bluestore: write "mkfs_done" into disk only if we pass fsck() tests (`pr#15238 <https://github.com/ceph/ceph/pull/15238>`_, xie xingguo)
* bluestore,core: osd/OSDMap: should update input param if osd dne (`pr#14863 <https://github.com/ceph/ceph/pull/14863>`_, Kefu Chai)
* bluestore,core: os: remove experimental status for BlueStore (`pr#15177 <https://github.com/ceph/ceph/pull/15177>`_, Sage Weil)
* bluestore: fixed compilation error when enable spdk (`pr#12672 <https://github.com/ceph/ceph/pull/12672>`_, Pan Liu)
* bluestore: include/intarith: templatize ctz/clz/cbits helpers (`pr#14862 <https://github.com/ceph/ceph/pull/14862>`_, Kefu Chai)
* bluestore: luminous: os/bluestore: compensate for bad freelistmanager size/blocks metadata (`issue#21089 <http://tracker.ceph.com/issues/21089>`_, `pr#17273 <https://github.com/ceph/ceph/pull/17273>`_, Sage Weil)
* bluestore: NVMEDevice: add the spdk core mask check (`pr#14068 <https://github.com/ceph/ceph/pull/14068>`_, optimistyzy)
* bluestore: NVMEDevice: cleanup the logic in data_buf_next_sge (`pr#13056 <https://github.com/ceph/ceph/pull/13056>`_, optimistyzy)
* bluestore: NVMeDevice: fix the core id for rte_remote_launch (`pr#13896 <https://github.com/ceph/ceph/pull/13896>`_, optimistyzy)
* bluestore: NVMEDevice: fix bug in data_buf_next_sge (`pr#12812 <https://github.com/ceph/ceph/pull/12812>`_, optimistyzy)
* bluestore: NVMEDevice: minor error for get slave core (`pr#14012 <https://github.com/ceph/ceph/pull/14012>`_, Ziye Yang)
* bluestore: NVMEDevice: optimize sector_size usage (`pr#12780 <https://github.com/ceph/ceph/pull/12780>`_, optimistyzy)
* bluestore: NVMEDevice: remove unnessary dpdk header file (`pr#14650 <https://github.com/ceph/ceph/pull/14650>`_, optimistyzy)
* bluestore: NVMEDevice: fix the I/O logic for read (`pr#13971 <https://github.com/ceph/ceph/pull/13971>`_, optimistyzy)
* bluestore: os/bluestore: add a debug option to bypass block device writes for bl… (`pr#12464 <https://github.com/ceph/ceph/pull/12464>`_, Igor Fedotov)
* bluestore: os/bluestore: Add bluestore pextent vector to mempool (`pr#12946 <https://github.com/ceph/ceph/pull/12946>`_, Igor Fedotvo, Igor Fedotov)
* bluestore: os/bluestore: add flush_store_cache cmd (`pr#13428 <https://github.com/ceph/ceph/pull/13428>`_, xie xingguo)
* bluestore: os/bluestore: add more perf_counters to BlueStore (`pr#13274 <https://github.com/ceph/ceph/pull/13274>`_, Igor Fedotov)
* bluestore: os/bluestore: add new garbage collector (`pr#12144 <https://github.com/ceph/ceph/pull/12144>`_, Igor Fedotov)
* bluestore: os/bluestore: add perf variable for throttle info in bluestore (`pr#12583 <https://github.com/ceph/ceph/pull/12583>`_, Pan Liu)
* bluestore: os/bluestore: add "_" prefix for internal methods (`pr#13409 <https://github.com/ceph/ceph/pull/13409>`_, xie xingguo)
* bluestore: os/bluestore: align reclaim size to bluefs_alloc_size (`pr#14744 <https://github.com/ceph/ceph/pull/14744>`_, Haomai Wang)
* bluestore: os/bluestore/Allocator: drop unused return value in release function (`pr#13913 <https://github.com/ceph/ceph/pull/13913>`_, wangzhengyong)
* bluestore: os/bluestore: allow multiple DeferredBatches in flight at once (`issue#20295 <http://tracker.ceph.com/issues/20295>`_, `pr#16769 <https://github.com/ceph/ceph/pull/16769>`_, Nathan Cutler, Sage Weil)
* bluestore: os/bluestore: allow multiple SPDK BlueStore OSD instances (`issue#16966 <http://tracker.ceph.com/issues/16966>`_, `pr#12604 <https://github.com/ceph/ceph/pull/12604>`_, Orlando Moreno)
* bluestore: os/bluestore: assert blob map returns success (`pr#14473 <https://github.com/ceph/ceph/pull/14473>`_, shiqi)
* bluestore: os/bluestore: avoid nullptr in bluestore_extent_ref_map_t::bound_encode (`pr#14073 <https://github.com/ceph/ceph/pull/14073>`_, Sage Weil)
* bluestore: os/bluestore: avoid unnecessary memory copy, use variable reference in BlockDevice::Open (`pr#12942 <https://github.com/ceph/ceph/pull/12942>`_, liuchang0812)
* bluestore: os/bluestore: better debug output on unsharing blobs (`issue#20227 <http://tracker.ceph.com/issues/20227>`_, `pr#15746 <https://github.com/ceph/ceph/pull/15746>`_, Sage Weil)
* bluestore: os/bluestore/BitAllocator: fix bug of checking required blocks (`pr#13470 <https://github.com/ceph/ceph/pull/13470>`_, wangzhengyong)
* bluestore: os/bluestore/BitMapAllocator: rm unused variable (`pr#13599 <https://github.com/ceph/ceph/pull/13599>`_, Jie Wang)
* bluestore: os/bluestore/BitmapFreelistManager: readability improvements (`pr#12719 <https://github.com/ceph/ceph/pull/12719>`_, xie xingguo)
* bluestore: os/bluestore/BlockDevice: support pmem device as bluestore backend (`pr#15102 <https://github.com/ceph/ceph/pull/15102>`_, Jianpeng Ma)
* bluestore: os/bluestore/BlueFS: clean up log_writer aios from compaction (`issue#20454 <http://tracker.ceph.com/issues/20454>`_, `pr#16017 <https://github.com/ceph/ceph/pull/16017>`_, Sage Weil)
* bluestore: os/bluestore/BlueFS: clear current log entrys before dump all fnode (`pr#15973 <https://github.com/ceph/ceph/pull/15973>`_, Jianpeng Ma)
* bluestore: os/bluestore/BlueFS: fix reclaim_blocks (`issue#18368 <http://tracker.ceph.com/issues/18368>`_, `pr#12725 <https://github.com/ceph/ceph/pull/12725>`_, Sage Weil)
* bluestore: os/bluestore/BlueFS: Rebuild memcopy for bufferlist::page_aligned_app… (`pr#15728 <https://github.com/ceph/ceph/pull/15728>`_, Jianpeng Ma, Sage Weil)
* bluestore: os/bluestore/BlueFS: .slow should be compared with dirname (`pr#15595 <https://github.com/ceph/ceph/pull/15595>`_, zhanglei)
* bluestore: os/bluestore/BlueStore: Avoid double counting state_kv_queued_lat (`pr#16374 <https://github.com/ceph/ceph/pull/16374>`_, Jianpeng Ma)
* bluestore: os/bluestore/BlueStore.cc:remove unuse code in _open_bdev() (`pr#13553 <https://github.com/ceph/ceph/pull/13553>`_, yonghengdexin735)
* bluestore: os/bluestore/BlueStore.cc: remove unused variable (`pr#12703 <https://github.com/ceph/ceph/pull/12703>`_, Li Wang)
* bluestore: os/bluestore/BlueStore: no device no symlink (`pr#15721 <https://github.com/ceph/ceph/pull/15721>`_, Jianpeng Ma)
* bluestore: os/bluestore/BlueStore: remove unused code (`pr#16522 <https://github.com/ceph/ceph/pull/16522>`_, Jianpeng Ma)
* bluestore: os/bluestore: cleanup BitAllocator (`pr#12661 <https://github.com/ceph/ceph/pull/12661>`_, xie xingguo)
* bluestore: os/bluestore: cleanup bluestore_types (`pr#15680 <https://github.com/ceph/ceph/pull/15680>`_, xie xingguo)
* bluestore: os/bluestore: clean up flush logic (`pr#14162 <https://github.com/ceph/ceph/pull/14162>`_, Jianpeng Ma)
* bluestore: os/bluestore: cleanup, got rid of table reference of 1<<x (`pr#13718 <https://github.com/ceph/ceph/pull/13718>`_, Adam Kupczyk)
* bluestore: os/bluestore: clean up Invalid return value judgment (`pr#14219 <https://github.com/ceph/ceph/pull/14219>`_, shiqi)
* bluestore: os/bluestore: cleanup min_alloc_size; some formatting nits (`pr#15826 <https://github.com/ceph/ceph/pull/15826>`_, xie xingguo)
* bluestore: os/bluestore: clear result in BlueRocksEnv::getChildren (`issue#20857 <http://tracker.ceph.com/issues/20857>`_, `pr#16683 <https://github.com/ceph/ceph/pull/16683>`_, liuchang0812)
* bluestore: os/bluestore: clear up redundant size assignment in KerenelDevice (`pr#16121 <https://github.com/ceph/ceph/pull/16121>`_, Shasha Lu)
* bluestore: os/bluestore: conditionally load crr option (`pr#12877 <https://github.com/ceph/ceph/pull/12877>`_, xie xingguo)
* bluestore: os/bluestore: configure rocksdb cache via bluestore_cache_kv_ratio (`pr#15580 <https://github.com/ceph/ceph/pull/15580>`_, Sage Weil)
* bluestore: os/bluestore: default journal media to store media if bluefs is disabled (`pr#16844 <https://github.com/ceph/ceph/pull/16844>`_, xie xingguo)
* bluestore: os/bluestore: _do_remove: dirty shard individually as each blob is unshared (`issue#20849 <http://tracker.ceph.com/issues/20849>`_, `pr#16822 <https://github.com/ceph/ceph/pull/16822>`_, Sage Weil)
* bluestore: os/blueStore: Failure retry for opening file (`pr#16237 <https://github.com/ceph/ceph/pull/16237>`_, Yankun Li)
* bluestore: os/bluestore: fix a bug in small write handling on sharded extents (`pr#13728 <https://github.com/ceph/ceph/pull/13728>`_, Igor Fedotov)
* bluestore: os/bluestore: fix Allocator::allocate() int truncation (`issue#18595 <http://tracker.ceph.com/issues/18595>`_, `pr#13010 <https://github.com/ceph/ceph/pull/13010>`_, Sage Weil)
* bluestore: os/bluestore: fix a typo about bleustore (`pr#15357 <https://github.com/ceph/ceph/pull/15357>`_, Dongsheng Yang)
* bluestore: os/bluestore: fix BitMapAllocator assert on out-of-bound hint value (`pr#15289 <https://github.com/ceph/ceph/pull/15289>`_, Igor Fedotov)
* bluestore: os/bluestore: fix buffers pinned by indefinitely deferred writes (`pr#15398 <https://github.com/ceph/ceph/pull/15398>`_, Sage Weil)
* bluestore: os/bluestore: fix bug for calc extent_avg in reshard function (`pr#13931 <https://github.com/ceph/ceph/pull/13931>`_, wangzhengyong)
* bluestore: os/bluestore: fix bug in aio_read() (`pr#13511 <https://github.com/ceph/ceph/pull/13511>`_, tangwenjun)
* bluestore: os/bluestore: fix bug in _open_alloc() (`pr#13577 <https://github.com/ceph/ceph/pull/13577>`_, yonghengdexin735)
* bluestore: os/bluestore: fix bug in _open_super_meta() (`pr#13559 <https://github.com/ceph/ceph/pull/13559>`_, Taeksang Kim)
* bluestore: os/bluestore: fix bugs in bluefs and bdev flush (`issue#19250 <http://tracker.ceph.com/issues/19250>`_, `issue#19251 <http://tracker.ceph.com/issues/19251>`_, `pr#13911 <https://github.com/ceph/ceph/pull/13911>`_, Sage Weil)
* bluestore: os/bluestore: fix coredump in register_ctrlr() (`pr#13556 <https://github.com/ceph/ceph/pull/13556>`_, tangwenjun)
* bluestore: os/bluestore: fix deferred_aio deadlock (`pr#16051 <https://github.com/ceph/ceph/pull/16051>`_, Sage Weil)
* bluestore: os/bluestore: fix deferred write race (`issue#19880 <http://tracker.ceph.com/issues/19880>`_, `pr#15004 <https://github.com/ceph/ceph/pull/15004>`_, Sage Weil)
* bluestore: os/bluestore: fix deferred writes vs collection split race (`issue#19379 <http://tracker.ceph.com/issues/19379>`_, `pr#14157 <https://github.com/ceph/ceph/pull/14157>`_, Sage Weil)
* bluestore: os/bluestore: fix dirty_range on _do_clone_range (`issue#20810 <http://tracker.ceph.com/issues/20810>`_, `pr#16738 <https://github.com/ceph/ceph/pull/16738>`_, Sage Weil)
* bluestore: os/bluestore: fix false assert in IOContext::aio_wake (`pr#15268 <https://github.com/ceph/ceph/pull/15268>`_, Igor Fedotov)
* bluestore: os/bluestore: fix false asserts in Cache::trim_all() (`pr#15470 <https://github.com/ceph/ceph/pull/15470>`_, xie xingguo)
* bluestore: os/bluestore: fix fsck deferred_replay (`pr#15295 <https://github.com/ceph/ceph/pull/15295>`_, Sage Weil)
* bluestore: os/bluestore: fix min_alloc_size at mkfs time (`pr#13192 <https://github.com/ceph/ceph/pull/13192>`_, Sage Weil)
* bluestore: os/bluestore: fix narrow osr->flush() race (`pr#14489 <https://github.com/ceph/ceph/pull/14489>`_, Sage Weil)
* bluestore: os/bluestore: fix NVMEDevice::open failure if serial number ends with a … (`pr#12956 <https://github.com/ceph/ceph/pull/12956>`_, Hongtong Liu)
* bluestore: os/bluestore: fix OnodeSizeTracking testing (`issue#20498 <http://tracker.ceph.com/issues/20498>`_, `pr#12684 <https://github.com/ceph/ceph/pull/12684>`_, xie xingguo)
* bluestore: os/bluestore: fix perf counters (`pr#13965 <https://github.com/ceph/ceph/pull/13965>`_, Sage Weil)
* bluestore: os/bluestore: fix possible out of order shard(offset == 0); add sanity check (`pr#15658 <https://github.com/ceph/ceph/pull/15658>`_, xie xingguo)
* bluestore: os/bluestore: fix potential access violation (`pr#15657 <https://github.com/ceph/ceph/pull/15657>`_, xie xingguo)
* bluestore: os/bluestore: fix potential assert in cache _trim method (`pr#13234 <https://github.com/ceph/ceph/pull/13234>`_, Igor Fedotov)
* bluestore: os/bluestore: fix reclaim_blocks and clean up Allocator interface (`issue#18573 <http://tracker.ceph.com/issues/18573>`_, `pr#12963 <https://github.com/ceph/ceph/pull/12963>`_, Sage Weil)
* bluestore: os/bluestore: fix typo(s/trasnaction/transaction/) (`pr#14890 <https://github.com/ceph/ceph/pull/14890>`_, xie xingguo)
* bluestore: os/bluestore: fix unsharing blob dirty_range args (`issue#20227 <http://tracker.ceph.com/issues/20227>`_, `pr#15766 <https://github.com/ceph/ceph/pull/15766>`_, Sage Weil)
* bluestore: os/bluestore: fix use after free race with aio_wait (`pr#14956 <https://github.com/ceph/ceph/pull/14956>`_, Sage Weil)
* bluestore: os/bluestore: fix wal-queue bytes-counter to keep pace with others (`pr#13382 <https://github.com/ceph/ceph/pull/13382>`_, xie xingguo)
* bluestore: os/bluestore: fsck: verify blob.unused field (`pr#14316 <https://github.com/ceph/ceph/pull/14316>`_, Sage Weil)
* bluestore: os/bluestore: handle rounding error in cache ratios (`pr#15672 <https://github.com/ceph/ceph/pull/15672>`_, Sage Weil)
* bluestore: os/bluestore: implement collect_metadata (`pr#14115 <https://github.com/ceph/ceph/pull/14115>`_, Sage Weil)
* bluestore: os/bluestore: include logical object offset in crc error (`pr#13074 <https://github.com/ceph/ceph/pull/13074>`_, Sage Weil)
* bluestore: os/bluestore: initialize finishers properly (`pr#15666 <https://github.com/ceph/ceph/pull/15666>`_, xie xingguo)
* bluestore: os/bluestore/KernelDevice: fix comments (`pr#15264 <https://github.com/ceph/ceph/pull/15264>`_, xie xingguo)
* bluestore: os/bluestore/KernelDevice: fix debug message (`pr#13135 <https://github.com/ceph/ceph/pull/13135>`_, Sage Weil)
* bluestore: os/bluestore/KernelDevice: helpful warning when aio limit exhausted (`pr#15116 <https://github.com/ceph/ceph/pull/15116>`_, Sage Weil)
* bluestore: os/bluestore/KernelDevice: kill zeros (`pr#12856 <https://github.com/ceph/ceph/pull/12856>`_, xie xingguo)
* bluestore: os/bluestore: kill BufferSpace.empty() (`pr#12871 <https://github.com/ceph/ceph/pull/12871>`_, xie xingguo)
* bluestore: os/bluestore: kill orphan declaration of do_write_check_depth() (`pr#12853 <https://github.com/ceph/ceph/pull/12853>`_, xie xingguo)
* bluestore: os/bluestore: leverage the type knowledge in BitMapAreaLeaf (`pr#13736 <https://github.com/ceph/ceph/pull/13736>`_, Radoslaw Zarzynski)
* bluestore: os/bluestore: Make BitmapFreelistManager kv itereator short lived (`pr#16243 <https://github.com/ceph/ceph/pull/16243>`_, Mark Nelson)
* bluestore: os/bluestore: make live changes for BlueStore throttle config work like initial config (`pr#14225 <https://github.com/ceph/ceph/pull/14225>`_, J. Eric Ivancich)
* bluestore: os/bluestore: miscellaneous fixes to BitAllocator (`pr#12696 <https://github.com/ceph/ceph/pull/12696>`_, xie xingguo)
* bluestore: os/bluestore: misc fix and cleanups (`pr#16315 <https://github.com/ceph/ceph/pull/16315>`_, Jianpeng Ma)
* bluestore: os/bluestore: misc fixes (`pr#14333 <https://github.com/ceph/ceph/pull/14333>`_, Sage Weil)
* bluestore: os/bluestore: move aio.h/cc from fs dir to bluestore dir (`pr#16409 <https://github.com/ceph/ceph/pull/16409>`_, Pan Liu)
* bluestore: os/bluestore: move object exist in assign nid (`pr#16117 <https://github.com/ceph/ceph/pull/16117>`_, Jianpeng Ma)
* bluestore: os/bluestore: move sharedblob to new collection in same shard (`issue#20358 <http://tracker.ceph.com/issues/20358>`_, `pr#15783 <https://github.com/ceph/ceph/pull/15783>`_, Sage Weil)
* bluestore: os/bluestore: narrow cache lock range; make sure min_alloc_size p2 aligned (`pr#15911 <https://github.com/ceph/ceph/pull/15911>`_, xie xingguo)
* bluestore: os/bluestore: "noid" is not always necessary in clone op (`pr#13769 <https://github.com/ceph/ceph/pull/13769>`_, wangzhengyong)
* bluestore: os/bluestore: nullptr in OmapIteratorImpl::valid (`pr#12900 <https://github.com/ceph/ceph/pull/12900>`_, Xinze Chi)
* bluestore: os/bluestore/NVMEDevice: Add multiple thread support for SPDK I/O thread (`pr#14420 <https://github.com/ceph/ceph/pull/14420>`_, Ziye Yang)
* bluestore: os/bluestore/NVMEDevice.cc: fix the random read issue (`pr#13055 <https://github.com/ceph/ceph/pull/13055>`_, optimistyzy)
* bluestore: os/bluestore/NVMEDevice: fix the compilation issue for collect_metadata (`pr#14455 <https://github.com/ceph/ceph/pull/14455>`_, optimistyzy)
* bluestore: os/bluestore/NVMEdevice: fix the unrelease segs issue (`pr#12862 <https://github.com/ceph/ceph/pull/12862>`_, optimistyzy)
* bluestore: os/bluestore: only submit deferred if there is any (`pr#16269 <https://github.com/ceph/ceph/pull/16269>`_, Sage Weil)
* bluestore: os/bluestore: preallocate object[extent_shard] key to avoid reallocate (`pr#12644 <https://github.com/ceph/ceph/pull/12644>`_, xie xingguo)
* bluestore: os/bluestore: pre-calculate number of ghost buffers to evict (`pr#15029 <https://github.com/ceph/ceph/pull/15029>`_, xie xingguo)
* bluestore: os/bluestore: put strings in mempool (`pr#12651 <https://github.com/ceph/ceph/pull/12651>`_, Allen Samuels, Sage Weil)
* bluestore: os/bluestore: Record l_bluestore_state_kv_queued_lat for sync_submit (`pr#14448 <https://github.com/ceph/ceph/pull/14448>`_, Jianpeng Ma)
* bluestore: os/bluestore: reduce some overhead for _do_clone_range() and _do_remove() (`pr#15944 <https://github.com/ceph/ceph/pull/15944>`_, xie xingguo)
* bluestore: os/bluestore: refactor BlueStore::_do_write;  kill dead ExtentMap::find_lextent() method (`pr#15750 <https://github.com/ceph/ceph/pull/15750>`_, xie xingguo)
* bluestore: os/bluestore: refactor ExtentMap::update to avoid preceeding db updat… (`pr#12394 <https://github.com/ceph/ceph/pull/12394>`_, Igor Fedotov)
* bluestore: os/bluestore: remove a never read value (`pr#12618 <https://github.com/ceph/ceph/pull/12618>`_, liuchang0812)
* bluestore: os/bluestore: Remove ExtentFreeListManager (`pr#14772 <https://github.com/ceph/ceph/pull/14772>`_, Jianpeng Ma)
* bluestore: os/bluestore: remove intermediate key var to avoid string copy (`pr#12643 <https://github.com/ceph/ceph/pull/12643>`_, xie xingguo)
* bluestore: os/bluestore: remove no use parameter in bluestore_blob_t::map_bl (`pr#13013 <https://github.com/ceph/ceph/pull/13013>`_, wangzhengyong)
* bluestore: os/bluestore: remove unneeded indirection in BitMapZone (`pr#13743 <https://github.com/ceph/ceph/pull/13743>`_, Radoslaw Zarzynski)
* bluestore: os/bluestore: remove unused condition variable (`pr#14973 <https://github.com/ceph/ceph/pull/14973>`_, Igor Fedotov)
* bluestore: os/bluestore: remove unused local variable "pos" (`pr#13715 <https://github.com/ceph/ceph/pull/13715>`_, wangzhengyong)
* bluestore: os/bluestore: remove unused variables (`pr#15718 <https://github.com/ceph/ceph/pull/15718>`_, zhanglei)
* bluestore: os/bluestore: rename/fix throttle options (`pr#14717 <https://github.com/ceph/ceph/pull/14717>`_, Sage Weil)
* bluestore: os/bluestore: roundoff bluefs allocs to bluefs_alloc_size (`pr#14876 <https://github.com/ceph/ceph/pull/14876>`_, Ramesh Chander)
* bluestore: os/bluestore: shrink buffer_map key into uint32_t (`pr#12850 <https://github.com/ceph/ceph/pull/12850>`_, xie xingguo)
* bluestore: os/bluestore: slightly refactor Blob::try_reuse_blob (`pr#15836 <https://github.com/ceph/ceph/pull/15836>`_, xie xingguo)
* bluestore: os/bluestore: some cleanup (`pr#13390 <https://github.com/ceph/ceph/pull/13390>`_, liuchang0812)
* bluestore: os/bluestore: space between func and contents (`pr#16804 <https://github.com/ceph/ceph/pull/16804>`_, xie xingguo)
* bluestore: os/bluestore: stop calculating bound if we must reshard; narrow shard combination condition (`pr#15631 <https://github.com/ceph/ceph/pull/15631>`_, xie xingguo)
* bluestore: os/bluestore/StupidAllocator: rounded down len to an align boundary (`issue#20660 <http://tracker.ceph.com/issues/20660>`_, `pr#16593 <https://github.com/ceph/ceph/pull/16593>`_, Zhu Shangzhong)
* bluestore: os/bluestore: target_bytes should scale with meta/data ratios (`pr#15708 <https://github.com/ceph/ceph/pull/15708>`_, Mark Nelson)
* bluestore: os/bluestore: _txc_release_alloc when do wal cleaning (`pr#12692 <https://github.com/ceph/ceph/pull/12692>`_, Xinze Chi)
* bluestore: os/bluestore: use bufferlist functions whenever possible (`pr#16158 <https://github.com/ceph/ceph/pull/16158>`_, Jianpeng Ma)
* bluestore: os/bluestore: use correct bound encode size for unused (`pr#14731 <https://github.com/ceph/ceph/pull/14731>`_, Haomai Wang)
* bluestore: os/bluestore: use reference to avoid string copy (`pr#16364 <https://github.com/ceph/ceph/pull/16364>`_, Pan Liu)
* bluestore: os: extend ObjectStore interface to dump store's performance counters (`pr#13203 <https://github.com/ceph/ceph/pull/13203>`_, Igor Fedotov)
* bluestore,performance: common/config_opts.h: compaction readahead for bluestore/rocksdb (`pr#14932 <https://github.com/ceph/ceph/pull/14932>`_, Mark Nelson)
* bluestore,performance: kv/RocksDBStore: implement rm_range_keys operator interface and test (`pr#13855 <https://github.com/ceph/ceph/pull/13855>`_, Haomai Wang)
* bluestore,performance: os/aio: remove the redundant memset(struct iocb) (`pr#13662 <https://github.com/ceph/ceph/pull/13662>`_, Jianpeng Ma)
* bluestore,performance: os/bluestore: add bluestore_prefer_wal_size option (`pr#13217 <https://github.com/ceph/ceph/pull/13217>`_, Sage Weil)
* bluestore,performance: os/bluestore: avoid overloading extents during reshard; atomic deferred_batch_ops (`pr#15502 <https://github.com/ceph/ceph/pull/15502>`_, xie xingguo)
* bluestore,performance: os/bluestore: avoid the VTABLE-related burden in BitMapAllocator's hotspot (`pr#14348 <https://github.com/ceph/ceph/pull/14348>`_, Radoslaw Zarzynski)
* bluestore,performance: os/bluestore: batch throttle (`pr#15284 <https://github.com/ceph/ceph/pull/15284>`_, Jianpeng Ma)
* bluestore,performance: os/bluestore/BlueFS: add bluefs_sync_write option (`pr#14510 <https://github.com/ceph/ceph/pull/14510>`_, Sage Weil)
* bluestore,performance: os/bluestore/BlueFS: optimize get_allocated (`pr#14121 <https://github.com/ceph/ceph/pull/14121>`_, Jianpeng Ma)
* bluestore,performance: os/bluestore/BlueFS: tune flushing of writes (`pr#13032 <https://github.com/ceph/ceph/pull/13032>`_, Sage Weil)
* bluestore,performance: os/bluestore/bluestore_types: drop std::bitset for blob unused (`pr#12569 <https://github.com/ceph/ceph/pull/12569>`_, Sage Weil)
* bluestore,performance: os/bluestore: cap rocksdb cache size (`pr#15786 <https://github.com/ceph/ceph/pull/15786>`_, Mark Nelson)
* bluestore,performance: os/bluestore: default 16KB min_alloc_size on ssd (`pr#14076 <https://github.com/ceph/ceph/pull/14076>`_, Sage Weil)
* bluestore,performance: os/bluestore: default cache size of 3gb (`pr#15976 <https://github.com/ceph/ceph/pull/15976>`_, Sage Weil)
* bluestore,performance: os/bluestore: differ default cache size for hdd/ssd backends (`pr#16157 <https://github.com/ceph/ceph/pull/16157>`_, xie xingguo)
* bluestore,performance: os/bluestore: do not balance bluefs on every kv_sync_thread iteration (`pr#14557 <https://github.com/ceph/ceph/pull/14557>`_, Sage Weil)
* bluestore,performance: os/bluestore: do not cache shard keys (`pr#12634 <https://github.com/ceph/ceph/pull/12634>`_, Sage Weil)
* bluestore,performance: os/bluestore: eliminate some excessive stuff (`pr#14675 <https://github.com/ceph/ceph/pull/14675>`_, Igor Fedotov)
* bluestore,performance: os/bluestore: fix deferred writes; improve flush (`pr#13888 <https://github.com/ceph/ceph/pull/13888>`_, Sage Weil)
* bluestore,performance: os/bluestore: generate same onode extent-shard keys in a more efficient way (`pr#12681 <https://github.com/ceph/ceph/pull/12681>`_, xie xingguo)
* bluestore,performance: os/bluestore: get rid off excessive lock at BitMapAllocator (`pr#14749 <https://github.com/ceph/ceph/pull/14749>`_, Igor Fedotov)
* bluestore,performance: os/blueStore: In osd_tp_thread, call _txc_finalize_kv (`pr#14709 <https://github.com/ceph/ceph/pull/14709>`_, Jianpeng Ma)
* bluestore,performance: os/bluestore: keep statfs replica in RAM to avoid expensive KV retrieval (`pr#15309 <https://github.com/ceph/ceph/pull/15309>`_, Igor Fedotov)
* bluestore,performance: os/bluestore/KernelDevice: batch aio submit (`pr#16032 <https://github.com/ceph/ceph/pull/16032>`_, Haodong Tang)
* bluestore,performance: os/bluestore/KernelDevice: fix sync write vs flush (`pr#15034 <https://github.com/ceph/ceph/pull/15034>`_, Sage Weil)
* bluestore,performance: os/bluestore: kvdb histogram (`pr#12620 <https://github.com/ceph/ceph/pull/12620>`_, Varada Kari)
* bluestore,performance: os/bluestore: make bluestore_max_blob_size parameter hdd/ssd case dependant (`pr#14434 <https://github.com/ceph/ceph/pull/14434>`_, Igor Fedotov)
* bluestore,performance: os/bluestore: memory and dereference clean-up in the BitAllocator (`pr#13811 <https://github.com/ceph/ceph/pull/13811>`_, Radoslaw Zarzynski)
* bluestore,performance: os/bluestore: move cache_trim into MempoolThread (`pr#15380 <https://github.com/ceph/ceph/pull/15380>`_, xie xingguo)
* bluestore,performance: os/bluestore: optimize blob usage when doing appends/overwrites (`pr#13337 <https://github.com/ceph/ceph/pull/13337>`_, Igor Fedotov)
* bluestore,performance: os/bluestore: optimized (encode|decode)_escaped (`pr#15759 <https://github.com/ceph/ceph/pull/15759>`_, Piotr Dałek)
* bluestore,performance: os/bluestore: partial reshard support (`pr#13162 <https://github.com/ceph/ceph/pull/13162>`_, Sage Weil)
* bluestore,performance: os/bluestore: prevent lock for almost "flush" calls (`pr#12524 <https://github.com/ceph/ceph/pull/12524>`_, Haomai Wang)
* bluestore,performance: os/bluestore: put bluefs in the middle of the shared device (`pr#14873 <https://github.com/ceph/ceph/pull/14873>`_, Sage Weil)
* bluestore,performance: os/bluestore: refactor small write handling to reuse blob more effect… (`pr#14399 <https://github.com/ceph/ceph/pull/14399>`_, Igor Fedotov)
* bluestore,performance: os/bluestore: remove CephContext\* from BmapEntry (`pr#13651 <https://github.com/ceph/ceph/pull/13651>`_, Radoslaw Zarzynski)
* bluestore,performance: os/bluestore: replace Blob ref_map with reference counting (`pr#12904 <https://github.com/ceph/ceph/pull/12904>`_, Igor Fedotov)
* bluestore,performance: os/bluestore: rewrite deferred write handling (`issue#16644 <http://tracker.ceph.com/issues/16644>`_, `pr#14491 <https://github.com/ceph/ceph/pull/14491>`_, Sage Weil)
* bluestore,performance: os/bluestore: separate kv_sync_thread into two parts (`pr#14035 <https://github.com/ceph/ceph/pull/14035>`_, Jianpeng Ma, Igor Fedotov, Sage Weil)
* bluestore,performance: os/bluestore: set cache meta ratio to .9 (`pr#12635 <https://github.com/ceph/ceph/pull/12635>`_, Sage Weil)
* bluestore,performance: os/bluestore: the exhausted check in BitMapZone can be lock-less (`pr#13653 <https://github.com/ceph/ceph/pull/13653>`_, Radoslaw Zarzynski)
* bluestore,performance: os/bluestore: try to unshare blobs for EC overwrite workload (`pr#14239 <https://github.com/ceph/ceph/pull/14239>`_, Sage Weil)
* bluestore,performance: os/bluestore: tune deferred_batch_ops separately for hdd and ssd (`pr#14435 <https://github.com/ceph/ceph/pull/14435>`_, Sage Weil)
* bluestore,performance: os/bluestore: unify throttling model (`issue#19542 <http://tracker.ceph.com/issues/19542>`_, `pr#14306 <https://github.com/ceph/ceph/pull/14306>`_, Sage Weil)
* bluestore,performance: os/bluestore: use aio for reads (`issue#19030 <http://tracker.ceph.com/issues/19030>`_, `pr#13066 <https://github.com/ceph/ceph/pull/13066>`_, Sage Weil)
* bluestore,performance: os/bluestore: use Best-Effort policy when evicting onode from cache (`pr#12876 <https://github.com/ceph/ceph/pull/12876>`_, xie xingguo)
* bluestore,performance: os/bluestore: use denc for varint encoding (`pr#14911 <https://github.com/ceph/ceph/pull/14911>`_, Piotr Dałek)
* bluestore,performance: os/bluestore: various onode changes to reduce its in-memory footprint (`pr#12700 <https://github.com/ceph/ceph/pull/12700>`_, Igor Fedotov)
* bluestore,performance: os/fs/aio: use small_vector for aio_t; clean up header location (`pr#14853 <https://github.com/ceph/ceph/pull/14853>`_, Sage Weil)
* bluestore: rocksdb: add option: writable_file_max_buffer_size = 0 (`pr#12562 <https://github.com/ceph/ceph/pull/12562>`_, Jianpeng Ma)
* bluestore,tests: ceph-dencoder: enable bluestore types (`pr#13595 <https://github.com/ceph/ceph/pull/13595>`_, Willem Jan Withagen, Kefu Chai)
* bluestore,tests: ceph_test_objectstore: match clone_range src and dst offset (`pr#13211 <https://github.com/ceph/ceph/pull/13211>`_, Sage Weil)
* bluestore,tests: qa/objectstore/bluestore\*: fsck on mount (`pr#15785 <https://github.com/ceph/ceph/pull/15785>`_, Sage Weil)
* bluestore,tests: test/ceph-test-objectstore: Don't always include BlueStore code (`pr#13516 <https://github.com/ceph/ceph/pull/13516>`_, Willem Jan Withagen)
* bluestore,tests: test/objectstore/store_test_fixture.cc: Exclude bluestore code if required (`pr#14085 <https://github.com/ceph/ceph/pull/14085>`_, Willem Jan Withagen)
* bluestore,tests: test/store_test: add deferred test case setup to support explicit min… (`issue#18857 <http://tracker.ceph.com/issues/18857>`_, `pr#13415 <https://github.com/ceph/ceph/pull/13415>`_, Igor Fedotov)
* bluestore,tests: test/store_test: fix bluestore test cases disablement (`pr#14228 <https://github.com/ceph/ceph/pull/14228>`_, Igor Fedotov)
* bluestore,tests: test/unittest_bluefs: check whether add_block_device success (`pr#14013 <https://github.com/ceph/ceph/pull/14013>`_, shiqi)
* bluestore,tests: test/unittest_bluefs: When fsync ret is less than 0, fsync can not be… (`pr#15365 <https://github.com/ceph/ceph/pull/15365>`_, shiqi)
* bluestore,tests: unittest_alloc: add test_alloc_big (`issue#16662 <http://tracker.ceph.com/issues/16662>`_, `pr#14844 <https://github.com/ceph/ceph/pull/14844>`_, Sage Weil)
* bluestore,tools: ceph-bluestore-tool: rename from bluefs-tool; improve usage (`pr#14258 <https://github.com/ceph/ceph/pull/14258>`_, Sage Weil)
* bluestore,tools: ceph-kvstore-tool: allow 'bluestore-kv' as kvdb type; add escaping, compaction (`pr#14718 <https://github.com/ceph/ceph/pull/14718>`_, Sage Weil)
* bluestore: wrap blob id when it reaches maximum value of int16_t (`issue#19555 <http://tracker.ceph.com/issues/19555>`_, `pr#15654 <https://github.com/ceph/ceph/pull/15654>`_, Xiaoyan Li)
* build/ops: 12.0.3 (`pr#15600 <https://github.com/ceph/ceph/pull/15600>`_, Jenkins Build Slave User)
* build/ops: add 12.0.1 release tag in master (`pr#14690 <https://github.com/ceph/ceph/pull/14690>`_, Jenkins Build Slave User)
* build/ops: add psmisc dependency to ceph-base (deb and rpm) (`issue#19129 <http://tracker.ceph.com/issues/19129>`_, `pr#13744 <https://github.com/ceph/ceph/pull/13744>`_, Nathan Cutler)
* build/ops: add sanity checks to run-make-check.sh (`pr#12683 <https://github.com/ceph/ceph/pull/12683>`_, Nathan Cutler)
* build/ops: alpine: add alpine linux dev support (`pr#9853 <https://github.com/ceph/ceph/pull/9853>`_, John Coyle)
* build/ops: arch: fix build on PowerPC with FreeBSD (`pr#14378 <https://github.com/ceph/ceph/pull/14378>`_, Andrew Solomon)
* build/ops: arch: fix cmake's ARM CRC intrinsics test to handle duplicitous gcc 4.8.5 (`issue#19386 <http://tracker.ceph.com/issues/19386>`_, `pr#14132 <https://github.com/ceph/ceph/pull/14132>`_, Dan Mick)
* build/ops: arch: use __get_cpuid instead of do_cpuid (`issue#7869 <http://tracker.ceph.com/issues/7869>`_, `pr#14857 <https://github.com/ceph/ceph/pull/14857>`_, Jos Collin)
* build/ops: auth: Let's not use the deprecated cephx option (`pr#12721 <https://github.com/ceph/ceph/pull/12721>`_, Dave Chen)
* build/ops: build: Add Virtuozzo Linux support (`pr#14301 <https://github.com/ceph/ceph/pull/14301>`_, Andrey Parfenov)
* build/ops: build: build erasure-code isa lib without versions (`pr#16205 <https://github.com/ceph/ceph/pull/16205>`_, James Page)
* build/ops: build/cmake:  provide asan, tsan, ubsan builds (`pr#12615 <https://github.com/ceph/ceph/pull/12615>`_, Matt Benjamin)
* build/ops: build: execute dh_systemd_{enable,start} after dh_install (`issue#19585 <http://tracker.ceph.com/issues/19585>`_, `pr#16218 <https://github.com/ceph/ceph/pull/16218>`_, James Page)
* build/ops: build: move bash_completion.d/ceph to ceph-common (`pr#15148 <https://github.com/ceph/ceph/pull/15148>`_, Leo Zhang)
* build/ops: build: remove ceph-disk-udev entirely (`pr#15259 <https://github.com/ceph/ceph/pull/15259>`_, Leo Zhang)
* build/ops: build: remove ceph-qa-suite directory (`pr#13880 <https://github.com/ceph/ceph/pull/13880>`_, Casey Bodley)
* build/ops: build: revert -Wvla from #15342 (`pr#15469 <https://github.com/ceph/ceph/pull/15469>`_, Willem Jan Withagen)
* build/ops: builds with dpdk v16.07 (`pr#12707 <https://github.com/ceph/ceph/pull/12707>`_, Kefu Chai)
* build/ops: build: Use .S suffix for ppc64le assembly files (`issue#20106 <http://tracker.ceph.com/issues/20106>`_, `pr#15373 <https://github.com/ceph/ceph/pull/15373>`_, Andrew Solomon)
* build/ops: ceph-disk: ability to use a different cluster name with dmcrypt (`issue#17821 <http://tracker.ceph.com/issues/17821>`_, `pr#11786 <https://github.com/ceph/ceph/pull/11786>`_, Sébastien Han, Erwan Velu)
* build/ops: ceph-disk: don't activate suppressed journal devices (`issue#19489 <http://tracker.ceph.com/issues/19489>`_, `pr#16123 <https://github.com/ceph/ceph/pull/16123>`_, David Disseldorp)
* build/ops: ceph.in: allow developer mode from outside build tree (`issue#20472 <http://tracker.ceph.com/issues/20472>`_, `pr#16055 <https://github.com/ceph/ceph/pull/16055>`_, Dan Mick)
* build/ops: ceph_release: we are in the 'rc' phase (12.1.z) (`pr#15957 <https://github.com/ceph/ceph/pull/15957>`_, Sage Weil)
* build/ops: ceph.spec.in, debian/control: Add bc to build dependencies (`issue#18876 <http://tracker.ceph.com/issues/18876>`_, `pr#13338 <https://github.com/ceph/ceph/pull/13338>`_, Kyr Shatskyy)
* build/ops: Clean up make check for persistent test nodes (like arm64) (`pr#16773 <https://github.com/ceph/ceph/pull/16773>`_, Dan Mick)
* build/ops: cmake,crc32c: conditionalize crc32c on different archs (`pr#14289 <https://github.com/ceph/ceph/pull/14289>`_, Kefu Chai)
* build/ops: CMakeLists.txt: boost_python.so requires libpython.\*.so on FreeBSD (`pr#12763 <https://github.com/ceph/ceph/pull/12763>`_, Willem Jan Withagen)
* build/ops: CMakeLists.txt: don't do crypto/isa-l if not Intel (`pr#14721 <https://github.com/ceph/ceph/pull/14721>`_, Dan Mick)
* build/ops: CMakeLists.txt: suppress unneeded warning about jemalloc (`pr#13377 <https://github.com/ceph/ceph/pull/13377>`_, Willem Jan Withagen)
* build/ops,common: build: Adds C++ warning flag for C Variable-Length Arrays (`pr#15342 <https://github.com/ceph/ceph/pull/15342>`_, Jesse Williamson)
* build/ops,common: common/blkdev.cc: propagate get_device_by_fd to different OSes (`pr#15547 <https://github.com/ceph/ceph/pull/15547>`_, Willem Jan Withagen)
* build/ops: common/module.c: do not use strerror_r the GNU way (`pr#12363 <https://github.com/ceph/ceph/pull/12363>`_, Willem Jan Withagen)
* build/ops: compressor/zstd: add zstd to embedded ceph (`pr#13159 <https://github.com/ceph/ceph/pull/13159>`_, Bassam Tabbara)
* build/ops: conditionalize rgw Beast frontend so it isn't built on s390x architecture (`issue#20048 <http://tracker.ceph.com/issues/20048>`_, `pr#15225 <https://github.com/ceph/ceph/pull/15225>`_, Willem Jan Withagen, Nathan Cutler, Kefu Chai, Tim Serong, Casey Bodley)
* build/ops,core: build: let FreeBSD build ceph-fuse (`pr#14282 <https://github.com/ceph/ceph/pull/14282>`_, Willem Jan Withagen)
* build/ops,core: ceph-disk: use correct user in check_journal_req (`issue#18538 <http://tracker.ceph.com/issues/18538>`_, `pr#12947 <https://github.com/ceph/ceph/pull/12947>`_, Samuel Matzek)
* build/ops,core: common/freebsd_errno.cc: fix missing (`pr#15741 <https://github.com/ceph/ceph/pull/15741>`_, Willem Jan Withagen)
* build/ops,core: erasure-code: update ec_isa version + add missing AVX512 ISA-L sources (`pr#15636 <https://github.com/ceph/ceph/pull/15636>`_, Ganesh Mahalingam, Tushar Gohad)
* build/ops,core: os: allow offline conversion of filestore -> bluestore (or anything else) (`pr#14210 <https://github.com/ceph/ceph/pull/14210>`_, Sage Weil)
* build/ops,core: osd/OSD: auto class on osd start up (`pr#16014 <https://github.com/ceph/ceph/pull/16014>`_, xie xingguo)
* build/ops,core: osd/Pool: Disallow enabling 'hashpspool' option to a pool without  '--yes-i-really-mean-it' (`issue#18468 <http://tracker.ceph.com/issues/18468>`_, `pr#13406 <https://github.com/ceph/ceph/pull/13406>`_, Vikhyat Umrao)
* build/ops,core,tests: osd/dmclock/testing: reorganize testing, building now optional (`issue#19987 <http://tracker.ceph.com/issues/19987>`_, `pr#15375 <https://github.com/ceph/ceph/pull/15375>`_, J. Eric Ivancich)
* build/ops: debian: Add missing tp files in deb packaging (`pr#13526 <https://github.com/ceph/ceph/pull/13526>`_, Ganesh Mahalingam)
* build/ops: debian: ceph-mgr: fix package description (`pr#15513 <https://github.com/ceph/ceph/pull/15513>`_, Fabian Grünbichler)
* build/ops: debian/control: add ceph-base-dbg (`pr#13796 <https://github.com/ceph/ceph/pull/13796>`_, Sage Weil)
* build/ops: debian: drop boost build dependencies (`pr#13524 <https://github.com/ceph/ceph/pull/13524>`_, Kefu Chai)
* build/ops: debian: package ceph.logroate properly (`issue#19390 <http://tracker.ceph.com/issues/19390>`_, `pr#14600 <https://github.com/ceph/ceph/pull/14600>`_, Kefu Chai)
* build/ops: debian: package crypto plugin only on amd64 (`pr#14820 <https://github.com/ceph/ceph/pull/14820>`_, Kefu Chai)
* build/ops: debian/rpm: move radosgw-admin to ceph-common (`issue#19577 <http://tracker.ceph.com/issues/19577>`_, `pr#14940 <https://github.com/ceph/ceph/pull/14940>`_, Ali Maredia)
* build/ops: debian/rules, ceph.spec.in: invoke cmake with -DBOOST_J (`pr#14114 <https://github.com/ceph/ceph/pull/14114>`_, Dan Mick)
* build/ops: debian: sync logrotate packaging with downstream (`issue#19938 <http://tracker.ceph.com/issues/19938>`_, `pr#15567 <https://github.com/ceph/ceph/pull/15567>`_, Fabian Grünbichler)
* build/ops: debian: workaround the bug in dpkg-maintscript-helper (`issue#20453 <http://tracker.ceph.com/issues/20453>`_, `pr#16072 <https://github.com/ceph/ceph/pull/16072>`_, Kefu Chai)
* build/ops: debian: wrap-and-sort all files (`pr#16110 <https://github.com/ceph/ceph/pull/16110>`_, James Page)
* build/ops: dmclock: error: ‘function’ in namespace ‘std’ does not name a template type (`pr#14909 <https://github.com/ceph/ceph/pull/14909>`_, Jos Collin)
* build/ops: dmclock: include missing <functional> header (`pr#14923 <https://github.com/ceph/ceph/pull/14923>`_, Jos Collin)
* build/ops: dmclock: initial commit of dmclock QoS library (`pr#14330 <https://github.com/ceph/ceph/pull/14330>`_, J. Eric Ivancich)
* build/ops: do_cmake.sh: enable ccache if installed (`pr#15274 <https://github.com/ceph/ceph/pull/15274>`_, Sage Weil)
* build/ops: do_cmake.sh: fix syntax for /bin/sh (doesn't have +=) (`pr#16433 <https://github.com/ceph/ceph/pull/16433>`_, Dan Mick)
* build/ops: do_freebsd.sh: Remove ENODATA requirement (`pr#13626 <https://github.com/ceph/ceph/pull/13626>`_, Willem Jan Withagen)
* build/ops: drop libfcgi build dependency (`pr#15285 <https://github.com/ceph/ceph/pull/15285>`_, Nathan Cutler)
* build/ops: gitignore: Ignore rejects by patch (`pr#14405 <https://github.com/ceph/ceph/pull/14405>`_, Willem Jan Withagen)
* build/ops: include/assert: test c++ before using static_cast<> (`pr#16424 <https://github.com/ceph/ceph/pull/16424>`_, Kefu Chai)
* build/ops: init-ceph: add ceph libraries path to environment (`pr#14693 <https://github.com/ceph/ceph/pull/14693>`_, Mohamad Gebai)
* build/ops: init-ceph: fix ceph user args (`pr#13467 <https://github.com/ceph/ceph/pull/13467>`_, Sage Weil)
* build/ops: init-ceph: Make init-ceph work under FreeBSD for init-system (`pr#13209 <https://github.com/ceph/ceph/pull/13209>`_, Willem Jan Withagen)
* build/ops: install-deps.sh: add missing dependencies for FreeBSD (`pr#16545 <https://github.com/ceph/ceph/pull/16545>`_, Alan Somers)
* build/ops: install-deps.sh: workaround setuptools' dependency on six (`pr#15406 <https://github.com/ceph/ceph/pull/15406>`_, Kefu Chai)
* build/ops: mailmap: Update OVH contributors (`pr#13063 <https://github.com/ceph/ceph/pull/13063>`_, Bartłomiej Święcki)
* build/ops: make package groups comply with openSUSE guidelines (`issue#19184 <http://tracker.ceph.com/issues/19184>`_, `pr#13781 <https://github.com/ceph/ceph/pull/13781>`_, Nathan Cutler)
* build/ops: make-srpm: Pass first parameter to make-dist for building SRPM (`pr#13480 <https://github.com/ceph/ceph/pull/13480>`_, Wido den Hollander)
* build/ops: merge v12.0.2 release tag (`pr#15091 <https://github.com/ceph/ceph/pull/15091>`_, Jenkins Build Slave User)
* build/ops,mgr: debian/ceph-base.dirs: create bootstrap-mgr dirs (`pr#14838 <https://github.com/ceph/ceph/pull/14838>`_, Sage Weil)
* build/ops: miscellaneous cleanups and fixes (run-make-check.sh, ceph.spec.in) (`issue#20091 <http://tracker.ceph.com/issues/20091>`_, `issue#20127 <http://tracker.ceph.com/issues/20127>`_, `pr#15399 <https://github.com/ceph/ceph/pull/15399>`_, Nathan Cutler)
* build/ops,mon: mon/ConfigKeyService: add 'config-key dump' to show keys and vals (`pr#14858 <https://github.com/ceph/ceph/pull/14858>`_, Dan Mick)
* build/ops,mon: systemd: Restart Mon after 10s in case of failure (`issue#18635 <http://tracker.ceph.com/issues/18635>`_, `pr#13057 <https://github.com/ceph/ceph/pull/13057>`_, Wido den Hollander)
* build/ops: msg/async/rdma: compile with rdma as default (`pr#13901 <https://github.com/ceph/ceph/pull/13901>`_, DanielBar-On)
* build/ops: os/bluestore: fix build errors when spdk is on (`pr#16118 <https://github.com/ceph/ceph/pull/16118>`_, Ilsoo Byun)
* build/ops: packaging: install libceph-common.so\* not libceph-common.so.\* (`issue#18692 <http://tracker.ceph.com/issues/18692>`_, `pr#13148 <https://github.com/ceph/ceph/pull/13148>`_, Kefu Chai)
* build/ops,performance: crc32c: Add crc32c function optimized for ppc architecture (`pr#13909 <https://github.com/ceph/ceph/pull/13909>`_, Andrew Solomon)
* build/ops,performance,rbd: byteorder: use gcc intrinsics for byteswap (`pr#15012 <https://github.com/ceph/ceph/pull/15012>`_, Kefu Chai)
* build/ops,rbd,rgw: CMakeLists: trim rbd/rgw forced dependencies (`pr#16574 <https://github.com/ceph/ceph/pull/16574>`_, Patrick Donnelly)
* build/ops,rbd,tests: test/librbd: decouple ceph_test_librbd_api from libceph-common (`issue#20175 <http://tracker.ceph.com/issues/20175>`_, `pr#15611 <https://github.com/ceph/ceph/pull/15611>`_, Kefu Chai)
* build/ops,rbd,tests: test/librbd: re-enable internal tests in ceph_test_librbd (`pr#16255 <https://github.com/ceph/ceph/pull/16255>`_, Mykola Golub)
* build/ops,rbd,tests: test: Need to exclude the fsx executable also on FreeBSD (`pr#13686 <https://github.com/ceph/ceph/pull/13686>`_, Willem Jan Withagen)
* build/ops: Revert "msg/async: increase worker reference with local listen table enabled backend" (`issue#20603 <http://tracker.ceph.com/issues/20603>`_, `pr#16323 <https://github.com/ceph/ceph/pull/16323>`_, Haomai Wang)
* build/ops: Revert "msg/async/rdma: Debug prints for ibv (`pr#14245 <https://github.com/ceph/ceph/pull/14245>`_, Kefu Chai)
* build/ops,rgw: rgw_file: radosgw-admin can be built under FreeBSD (`pr#12191 <https://github.com/ceph/ceph/pull/12191>`_, Willem Jan Withagen)
* build/ops,rgw,tests,tools: vstart: allow to start multiple radosgw when RGW=x (`pr#15632 <https://github.com/ceph/ceph/pull/15632>`_, Adam Kupczyk)
* build/ops,rgw,tools: vstart: add --rgw_compression to set rgw compression plugin (`pr#15929 <https://github.com/ceph/ceph/pull/15929>`_, Casey Bodley)
* build/ops: rocksdb: build with ppc64 (`pr#12908 <https://github.com/ceph/ceph/pull/12908>`_, Kefu Chai)
* build/ops: rocksdb: sync with upstream (`pr#14456 <https://github.com/ceph/ceph/pull/14456>`_, Kefu Chai)
* build/ops: rocksdb: sync with upstream (`pr#14818 <https://github.com/ceph/ceph/pull/14818>`_, Nathan Cutler, Kefu Chai)
* build/ops: rpm: apply epoch only if %epoch macro is defined (`pr#15286 <https://github.com/ceph/ceph/pull/15286>`_, Nathan Cutler)
* build/ops: rpm: build ceph-resource-agents by default (`issue#17613 <http://tracker.ceph.com/issues/17613>`_, `pr#13515 <https://github.com/ceph/ceph/pull/13515>`_, Nathan Cutler)
* build/ops: rpm: bump epoch ahead of RHEL base (`issue#20508 <http://tracker.ceph.com/issues/20508>`_, `pr#16126 <https://github.com/ceph/ceph/pull/16126>`_, Ken Dreyer)
* build/ops: rpm,deb: fix ceph-volume (`issue#20915 <http://tracker.ceph.com/issues/20915>`_, `pr#16832 <https://github.com/ceph/ceph/pull/16832>`_, Sage Weil)
* build/ops: rpm: disable dwz to speed up valgrind (`issue#19099 <http://tracker.ceph.com/issues/19099>`_, `pr#13748 <https://github.com/ceph/ceph/pull/13748>`_, Kefu Chai)
* build/ops: rpm: drop boost build dependencies (`pr#13519 <https://github.com/ceph/ceph/pull/13519>`_, Nathan Cutler)
* build/ops: rpm: Drop legacy libxio support (`pr#16449 <https://github.com/ceph/ceph/pull/16449>`_, Nathan Cutler)
* build/ops: rpm: fix python-Sphinx package name for SUSE (`pr#15015 <https://github.com/ceph/ceph/pull/15015>`_, Nathan Cutler, Jan Matejek)
* build/ops: rpm: fix typo WTIH_BABELTRACE (`pr#16366 <https://github.com/ceph/ceph/pull/16366>`_, Nathan Cutler)
* build/ops: rpm: Fix undefined FIRST_ARG (`issue#20077 <http://tracker.ceph.com/issues/20077>`_, `pr#16208 <https://github.com/ceph/ceph/pull/16208>`_, Boris Ranto)
* build/ops: rpm: gperftools-devel >= 2.4 (`issue#13522 <http://tracker.ceph.com/issues/13522>`_, `pr#14870 <https://github.com/ceph/ceph/pull/14870>`_, Nathan Cutler)
* build/ops: rpm: make librbd1 %post scriptlet depend on coreutils (`issue#20052 <http://tracker.ceph.com/issues/20052>`_, `pr#15231 <https://github.com/ceph/ceph/pull/15231>`_, Giacomo Comes, Nathan Cutler)
* build/ops: rpm: move _epoch_prefix below Epoch definition (`pr#15417 <https://github.com/ceph/ceph/pull/15417>`_, Nathan Cutler)
* build/ops: rpm: move RDMA and python-prettytables build dependencies to distro-conditional section (`pr#15200 <https://github.com/ceph/ceph/pull/15200>`_, Nathan Cutler)
* build/ops: rpm: obsolete libcephfs1 (`pr#16074 <https://github.com/ceph/ceph/pull/16074>`_, Nathan Cutler)
* build/ops: rpm: package COPYING, move sample ceph.conf to ceph-common (`pr#15596 <https://github.com/ceph/ceph/pull/15596>`_, Nathan Cutler)
* build/ops: rpm: package crypto on x86_64 only (`pr#14779 <https://github.com/ceph/ceph/pull/14779>`_, Nathan Cutler)
* build/ops: rpm: put mgr python build dependencies in make_check bcond (`issue#20425 <http://tracker.ceph.com/issues/20425>`_, `pr#15940 <https://github.com/ceph/ceph/pull/15940>`_, Nathan Cutler, Tim Serong)
* build/ops: rpm: sane packaging of %{_docdir}/ceph directory (`pr#15900 <https://github.com/ceph/ceph/pull/15900>`_, Nathan Cutler)
* build/ops: script: adding contributor credits script (`pr#13251 <https://github.com/ceph/ceph/pull/13251>`_, Patrick McGarry)
* build/ops: script: drop the -x arg for credits script (`pr#14296 <https://github.com/ceph/ceph/pull/14296>`_, Abhishek Lekshmanan)
* build/ops: script/sepia_bt.sh: download packages from shaman not gitbuilder (`pr#12799 <https://github.com/ceph/ceph/pull/12799>`_, Kefu Chai)
* build/ops: script/sepia_bt.sh: get sha1,release from t.log if it's not in core (`pr#13620 <https://github.com/ceph/ceph/pull/13620>`_, Kefu Chai)
* build/ops: script/sepia_bt.sh: support xenial (`pr#13292 <https://github.com/ceph/ceph/pull/13292>`_, Kefu Chai)
* build/ops: selinux: Allow ceph daemons to read net stats (`issue#19254 <http://tracker.ceph.com/issues/19254>`_, `pr#13945 <https://github.com/ceph/ceph/pull/13945>`_, Boris Ranto)
* build/ops: selinux: Allow read on var_run_t (`issue#16674 <http://tracker.ceph.com/issues/16674>`_, `pr#15523 <https://github.com/ceph/ceph/pull/15523>`_, Boris Ranto)
* build/ops: selinux: Do parallel relabel on package install (`issue#20077 <http://tracker.ceph.com/issues/20077>`_, `pr#14871 <https://github.com/ceph/ceph/pull/14871>`_, Boris Ranto)
* build/ops: selinux: Install ceph-base before ceph-selinux (`issue#20184 <http://tracker.ceph.com/issues/20184>`_, `pr#15490 <https://github.com/ceph/ceph/pull/15490>`_, Boris Ranto)
* build/ops: Set subman cron attributes in spec file (`issue#20074 <http://tracker.ceph.com/issues/20074>`_, `pr#15270 <https://github.com/ceph/ceph/pull/15270>`_, Thomas Serlin)
* build/ops: spdk: upgrade spdk to v16.12 (`pr#12734 <https://github.com/ceph/ceph/pull/12734>`_, Pan Liu)
* build/ops: src/CMakeLists.txt: disable -Werror on rocksdb (`pr#12560 <https://github.com/ceph/ceph/pull/12560>`_, Willem Jan Withagen)
* build/ops: src/CMakeLists.txt: Move parse_secret_objs setting within definition block (`pr#12785 <https://github.com/ceph/ceph/pull/12785>`_, Willem Jan Withagen)
* build/ops: src/init-ceph.in:  allow one((re)?start|stop) as commands (`pr#14560 <https://github.com/ceph/ceph/pull/14560>`_, Willem Jan Withagen)
* build/ops: sync luminous tag back to master (`pr#16758 <https://github.com/ceph/ceph/pull/16758>`_, Jenkins Build Slave User)
* build/ops: systemd: Add explicit Before=ceph.target (`pr#15835 <https://github.com/ceph/ceph/pull/15835>`_, Tim Serong)
* build/ops: systemd/ceph-disk: make it possible to customize timeout (`issue#18740 <http://tracker.ceph.com/issues/18740>`_, `pr#13197 <https://github.com/ceph/ceph/pull/13197>`_, Alexey Sheplyakov)
* build/ops: systemd/ceph-mgr: remove automagic mgr creation hack (`issue#19994 <http://tracker.ceph.com/issues/19994>`_, `pr#16023 <https://github.com/ceph/ceph/pull/16023>`_, Sage Weil)
* build/ops: systemd: remove ceph-create-keys from presets (`pr#14226 <https://github.com/ceph/ceph/pull/14226>`_, Sébastien Han)
* build/ops: systemd: Start OSDs after MONs (`issue#18516 <http://tracker.ceph.com/issues/18516>`_, `pr#13097 <https://github.com/ceph/ceph/pull/13097>`_, Boris Ranto)
* build/ops: test/fio_ceph_objectstore: fix fio plugin build failure caused by rec… (`pr#12655 <https://github.com/ceph/ceph/pull/12655>`_, Igor Fedotov)
* build/ops,tests: qa: make run-standalone work on FreeBSD (`pr#16595 <https://github.com/ceph/ceph/pull/16595>`_, Willem Jan Withagen)
* build/ops,tests: test/osd/CMakeLists.txt: osd-dup.sh require BlueStore/AIO (`pr#14387 <https://github.com/ceph/ceph/pull/14387>`_, Willem Jan Withagen)
* build/ops,tests: test/osd/osd-dup.sh: warn on low open file limit (`pr#14637 <https://github.com/ceph/ceph/pull/14637>`_, Piotr Dałek)
* build/ops,tests,tools: vstart.sh: Work around mgr restfull not available (`pr#15877 <https://github.com/ceph/ceph/pull/15877>`_, Willem Jan Withagen)
* build/ops: The Clangtastic Mr. Clocks (`pr#15186 <https://github.com/ceph/ceph/pull/15186>`_, Adam C. Emerson)
* build/ops: tool: add some ceph relate processes to ps-ceph.pl (`pr#12406 <https://github.com/ceph/ceph/pull/12406>`_, songbaisen)
* build/ops: tools/scripts:"FreeBSD getopt is not compatible, use the one from packages" (`pr#13260 <https://github.com/ceph/ceph/pull/13260>`_, Willem Jan Withagen)
* build/ops: tracing: Fix error in including all files in osd_tp (`pr#12501 <https://github.com/ceph/ceph/pull/12501>`_, Ganesh Mahalingam)
* build/ops: upstart: start radosgw-all according to runlevel (`issue#18313 <http://tracker.ceph.com/issues/18313>`_, `pr#12586 <https://github.com/ceph/ceph/pull/12586>`_, Ken Dreyer)
* build/ops: vstart: clean up usage a bit (`pr#13138 <https://github.com/ceph/ceph/pull/13138>`_, Sage Weil)
* build/ops: vstart: do not start mgr if not start_all (`pr#13974 <https://github.com/ceph/ceph/pull/13974>`_, Kefu Chai)
* build/ops: yasm-wrapper: filter -pthread (`pr#15249 <https://github.com/ceph/ceph/pull/15249>`_, Alessandro Barbieri)
* build/ops: yasm-wrapper: strip -E (stops ccache trashing source files) (`pr#14633 <https://github.com/ceph/ceph/pull/14633>`_, Tim Serong)
* cephfs: #11950: Persistent purge queue (`issue#11950 <http://tracker.ceph.com/issues/11950>`_, `pr#12786 <https://github.com/ceph/ceph/pull/12786>`_, John Spray)
* cephfs: #17980: MDS client blacklisting and blacklist on eviction (`issue#17980 <http://tracker.ceph.com/issues/17980>`_, `issue#9754 <http://tracker.ceph.com/issues/9754>`_, `pr#14610 <https://github.com/ceph/ceph/pull/14610>`_, John Spray)
* cephfs: #18600: Clear out tasks that don't make sense from multimds suite (`issue#18600 <http://tracker.ceph.com/issues/18600>`_, `pr#13089 <https://github.com/ceph/ceph/pull/13089>`_, John Spray)
* cephfs: ceph_fuse: fix daemonization when pid file is non-empty (`pr#13532 <https://github.com/ceph/ceph/pull/13532>`_, "Yan, Zheng")
* cephfs: ceph_fuse: pid_file default to empty (`issue#18309 <http://tracker.ceph.com/issues/18309>`_, `pr#12628 <https://github.com/ceph/ceph/pull/12628>`_, Nathan Cutler)
* cephfs: ceph-fuse: use user space permission check by default (`issue#19820 <http://tracker.ceph.com/issues/19820>`_, `pr#14907 <https://github.com/ceph/ceph/pull/14907>`_, "Yan, Zheng")
* cephfs: ceph: simplify CInode::maybe_export_pin() (`pr#15106 <https://github.com/ceph/ceph/pull/15106>`_, "Yan, Zheng")
* cephfs: client: avoid returning negative space available (`issue#20178 <http://tracker.ceph.com/issues/20178>`_, `pr#15481 <https://github.com/ceph/ceph/pull/15481>`_, John Spray)
* cephfs: client: call the lru_remove() twice,when trim cache (`pr#15662 <https://github.com/ceph/ceph/pull/15662>`_, huanwen ren)
* cephfs: client: check for luminous MDS before sending FLUSH_MDLOG (`pr#15805 <https://github.com/ceph/ceph/pull/15805>`_, John Spray)
* cephfs: client/Client.cc: after reset session from MDS - reconnect (`issue#18757 <http://tracker.ceph.com/issues/18757>`_, `pr#13522 <https://github.com/ceph/ceph/pull/13522>`_, Henrik Korkuc)
* cephfs: client/Client.cc: prevent segfaulting (`issue#9935 <http://tracker.ceph.com/issues/9935>`_, `pr#12550 <https://github.com/ceph/ceph/pull/12550>`_, Michal Jarzabek)
* cephfs: client: client_quota no longer optional (`pr#14978 <https://github.com/ceph/ceph/pull/14978>`_, Dan van der Ster)
* cephfs: client: don't request lookup parent if ino is root (`pr#12478 <https://github.com/ceph/ceph/pull/12478>`_, huanwen ren)
* cephfs: client: drop cap snaps when auth mds session gets closed (`issue#19022 <http://tracker.ceph.com/issues/19022>`_, `pr#13579 <https://github.com/ceph/ceph/pull/13579>`_, "Yan, Zheng")
* cephfs: client: fix clang warn of "argument is an uninitialized value" (`pr#12580 <https://github.com/ceph/ceph/pull/12580>`_, liuchang0812)
* cephfs: 	client: fix Client::handle_cap_flushsnap_ack() crash (`issue#18460 <http://tracker.ceph.com/issues/18460>`_, `pr#12859 <https://github.com/ceph/ceph/pull/12859>`_, Yan, Zheng)
* cephfs: client: fix Dentry::dump (`pr#15779 <https://github.com/ceph/ceph/pull/15779>`_, huanwen ren)
* cephfs: client: fix display ino in the ldout (`pr#15314 <https://github.com/ceph/ceph/pull/15314>`_, huanwen ren)
* cephfs: client: fix potential buffer overflow (`pr#12515 <https://github.com/ceph/ceph/pull/12515>`_, Yunchuan Wen)
* cephfs: client: fix the cross-quota rename boundary check conditions (`pr#12489 <https://github.com/ceph/ceph/pull/12489>`_, Greg Farnum)
* cephfs: client: fix UserPerm::gid_in_group() (`issue#19903 <http://tracker.ceph.com/issues/19903>`_, `pr#15039 <https://github.com/ceph/ceph/pull/15039>`_, "Yan, Zheng")
* cephfs: client: getattr before returning quota/layout xattrs (`issue#17939 <http://tracker.ceph.com/issues/17939>`_, `pr#14018 <https://github.com/ceph/ceph/pull/14018>`_, John Spray)
* cephfs: client/inode: fix the dump type of Inode::dump() (`pr#15198 <https://github.com/ceph/ceph/pull/15198>`_, huanwen ren)
* cephfs: client: populate metadata during mount (`issue#18361 <http://tracker.ceph.com/issues/18361>`_, `pr#12915 <https://github.com/ceph/ceph/pull/12915>`_, John Spray)
* cephfs: client: priority to verify the correctness of the "flag" (`pr#12897 <https://github.com/ceph/ceph/pull/12897>`_, huanwen ren)
* cephfs: client: refine fsync/close writeback error handling (`pr#14589 <https://github.com/ceph/ceph/pull/14589>`_, John Spray)
* cephfs: client: remove dead log code (`pr#13093 <https://github.com/ceph/ceph/pull/13093>`_, Patrick Donnelly)
* cephfs: client: remove request from session->requests when handling forward (`issue#18675 <http://tracker.ceph.com/issues/18675>`_, `pr#13124 <https://github.com/ceph/ceph/pull/13124>`_, "Yan, Zheng")
* cephfs: client: simplify remove_cap interface (`pr#12161 <https://github.com/ceph/ceph/pull/12161>`_, John Spray)
* cephfs: client: specify inode in get_caps log message (`pr#13966 <https://github.com/ceph/ceph/pull/13966>`_, John Spray)
* cephfs: client: wait for lastest osdmap when handling set file/dir layout (`issue#18914 <http://tracker.ceph.com/issues/18914>`_, `pr#13580 <https://github.com/ceph/ceph/pull/13580>`_, "Yan, Zheng")
* cephfs,common: common/MemoryModel: Bump int to long and drop mallinfo (`pr#13453 <https://github.com/ceph/ceph/pull/13453>`_, Xiaoxi Chen)
* cephfs,common,core: librados,osdc: kill ack vs commit distinction (`pr#12607 <https://github.com/ceph/ceph/pull/12607>`_, Sage Weil)
* cephfs,common: include/fs_types: fix unsigned integer overflow (`pr#12440 <https://github.com/ceph/ceph/pull/12440>`_, runsisi)
* cephfs,common,rbd: blkin: librbd trace hooks (`pr#15053 <https://github.com/ceph/ceph/pull/15053>`_, Victor Araujo, Jason Dillaman)
* cephfs,common,rbd: osdc: cache should ignore error bhs during trim (`issue#18436 <http://tracker.ceph.com/issues/18436>`_, `pr#12966 <https://github.com/ceph/ceph/pull/12966>`_, Jason Dillaman)
* cephfs,core: Add test for is_hacky_ecoverwrites in cephfs pool checks (`pr#13466 <https://github.com/ceph/ceph/pull/13466>`_, John Spray)
* cephfs,core: cleanup: use std::make_shared to replace new (`pr#12276 <https://github.com/ceph/ceph/pull/12276>`_, Yunchuan Wen)
* cephfs,core,mon: mon/MDSMonitor: fix segv when multiple MDSs raise same alert (`pr#16302 <https://github.com/ceph/ceph/pull/16302>`_, Sage Weil)
* cephfs: fix mount point break off problem after mds switch occured (`issue#19437 <http://tracker.ceph.com/issues/19437>`_, `pr#14267 <https://github.com/ceph/ceph/pull/14267>`_, Guan yunfei)
* cephfs: fix write_buf's _len overflow problem (`issue#19033 <http://tracker.ceph.com/issues/19033>`_, `pr#13587 <https://github.com/ceph/ceph/pull/13587>`_, Yang Honggang)
* cephfs: fs/ceph-fuse: normalize file open flags on the wire (`pr#14822 <https://github.com/ceph/ceph/pull/14822>`_, Jan Fajerski)
* cephfs: libcephfs.cc: fix memory leak (`pr#12557 <https://github.com/ceph/ceph/pull/12557>`_, Michal Jarzabek)
* cephfs: libcephfs: cleanups (`pr#12830 <https://github.com/ceph/ceph/pull/12830>`_, huanwen ren)
* cephfs: libcephfs: fix cct refcount constructing from rados (`pr#12831 <https://github.com/ceph/ceph/pull/12831>`_, John Spray)
* cephfs: mds/MDBalancer: remove useless check_targets and hit_targets logic from MDS balancer (`issue#20131 <http://tracker.ceph.com/issues/20131>`_, `pr#15407 <https://github.com/ceph/ceph/pull/15407>`_, Zhi Zhang)
* cephfs: mds/MDLog.cc Fix perf counter type for jlat (`pr#13449 <https://github.com/ceph/ceph/pull/13449>`_, Xiaoxi Chen)
* cephfs: mds/Server.cc: Don't evict a slow client if (`issue#17855 <http://tracker.ceph.com/issues/17855>`_, `pr#12935 <https://github.com/ceph/ceph/pull/12935>`_, Michal Jarzabek)
* cephfs: mds/StrayManager: avoid reusing deleted inode in StrayManager::_purge_stray_logged (`issue#18877 <http://tracker.ceph.com/issues/18877>`_, `pr#13347 <https://github.com/ceph/ceph/pull/13347>`_, Zhi Zhang)
* cephfs,mgr: pybind/mgr/fsstatus: use mds_mem.dn as dentry counter (`pr#15255 <https://github.com/ceph/ceph/pull/15255>`_, Zhi Zhang)
* cephfs: Mitigation for #16842, validate sessions after load (`issue#16842 <http://tracker.ceph.com/issues/16842>`_, `pr#14164 <https://github.com/ceph/ceph/pull/14164>`_, John Spray)
* cephfs: mon/FSCommand: fix indentation (`pr#15423 <https://github.com/ceph/ceph/pull/15423>`_, Sage Weil)
* cephfs: mon/MDSMonitor.cc:refuse fs new on pools with obj (`issue#11124 <http://tracker.ceph.com/issues/11124>`_, `pr#12825 <https://github.com/ceph/ceph/pull/12825>`_, Michal Jarzabek)
* cephfs: mon/MDSMonitor: respect mds_standby_for_rank config (`pr#15129 <https://github.com/ceph/ceph/pull/15129>`_, "Yan, Zheng")
* cephfs: mount: do not print "unknown" option to kclient (`issue#18159 <http://tracker.ceph.com/issues/18159>`_, `pr#12465 <https://github.com/ceph/ceph/pull/12465>`_, John Spray)
* cephfs: osdc/Filer: truncate large file party by party (`issue#19755 <http://tracker.ceph.com/issues/19755>`_, `pr#14769 <https://github.com/ceph/ceph/pull/14769>`_, "Yan, Zheng")
* cephfs: osdc/Journaler: avoid executing on_safe contexts prematurely (`issue#20055 <http://tracker.ceph.com/issues/20055>`_, `pr#15240 <https://github.com/ceph/ceph/pull/15240>`_, "Yan, Zheng")
* cephfs: osdc/Journaler: fix memory leak in Journaler::_issue_read() (`issue#20338 <http://tracker.ceph.com/issues/20338>`_, `pr#15776 <https://github.com/ceph/ceph/pull/15776>`_, "Yan, Zheng")
* cephfs: osdc/Objecter: fix inflight_ops update (`pr#15768 <https://github.com/ceph/ceph/pull/15768>`_, "Yan, Zheng")
* cephfs: osdc: remove journaler_allow_split_entries option (`issue#19691 <http://tracker.ceph.com/issues/19691>`_, `pr#14636 <https://github.com/ceph/ceph/pull/14636>`_, John Spray)
* cephfs,performance: client: make seeky readdir more efficiency (`issue#19306 <http://tracker.ceph.com/issues/19306>`_, `pr#14317 <https://github.com/ceph/ceph/pull/14317>`_, "Yan, Zheng")
* cephfs,performance: mds/server: skip unwanted dn in handle_client_readdir (`pr#12870 <https://github.com/ceph/ceph/pull/12870>`_, Xiaoxi Chen)
* cephfs: Permit recovering metadata into a new RADOS pool (`issue#15069 <http://tracker.ceph.com/issues/15069>`_, `issue#15068 <http://tracker.ceph.com/issues/15068>`_, `pr#10636 <https://github.com/ceph/ceph/pull/10636>`_, Douglas Fuller)
* cephfs: qa/cephfs: disable mds_bal_frag for TestStrays.test_purge_queue_op_rate (`issue#19892 <http://tracker.ceph.com/issues/19892>`_, `pr#15105 <https://github.com/ceph/ceph/pull/15105>`_, "Yan, Zheng")
* cephfs: qa/cephfs: Fix for test_data_scan (`issue#19893 <http://tracker.ceph.com/issues/19893>`_, `pr#15094 <https://github.com/ceph/ceph/pull/15094>`_, Douglas Fuller)
* cephfs: qa: fix race in Mount.open_background (`issue#18661 <http://tracker.ceph.com/issues/18661>`_, `pr#13137 <https://github.com/ceph/ceph/pull/13137>`_, John Spray)
* cephfs: qa/suites/fs: reserve more space for mds in full tests (`issue#19891 <http://tracker.ceph.com/issues/19891>`_, `pr#15026 <https://github.com/ceph/ceph/pull/15026>`_, "Yan, Zheng")
* cephfs: qa/tasks/cephfs: use getattr to guarantee inode is in client cache (`issue#19912 <http://tracker.ceph.com/issues/19912>`_, `pr#15062 <https://github.com/ceph/ceph/pull/15062>`_, "Yan, Zheng")
* cephfs: qa: unpin knfs from ubuntu (`issue#16397 <http://tracker.ceph.com/issues/16397>`_, `pr#13088 <https://github.com/ceph/ceph/pull/13088>`_, John Spray)
* cephfs: qa: update log whitelists for kcephfs suite (`pr#14922 <https://github.com/ceph/ceph/pull/14922>`_, "Yan, Zheng")
* cephfs: qa: update remaining ceph.com to download.ceph.com (`issue#18574 <http://tracker.ceph.com/issues/18574>`_, `pr#12964 <https://github.com/ceph/ceph/pull/12964>`_, John Spray)
* cephfs: qa: whitelist new fullness messages in fs tests (`issue#19253 <http://tracker.ceph.com/issues/19253>`_, `pr#13915 <https://github.com/ceph/ceph/pull/13915>`_, John Spray)
* cephfs: Remove "experimental" warnings from multimds (`pr#15154 <https://github.com/ceph/ceph/pull/15154>`_, John Spray, "Yan, Zheng")
* cephfs: Rewrite mount.fuse.ceph (to python) and move ceph-fuse options to fs_mntops (`pr#11448 <https://github.com/ceph/ceph/pull/11448>`_, Edgaras Lukosevicius)
* cephfs: tasks/cephfs: fix kernel force umount (`issue#18396 <http://tracker.ceph.com/issues/18396>`_, `pr#12833 <https://github.com/ceph/ceph/pull/12833>`_, Yan, Zheng)
* cephfs: test/libcephfs: avoid buffer overflow when testing ceph_getdents() (`issue#18941 <http://tracker.ceph.com/issues/18941>`_, `pr#13429 <https://github.com/ceph/ceph/pull/13429>`_, "Yan, Zheng")
* cephfs,tests: Add multimds:thrash sub-suite and fix bugs in thrasher for multimds (`issue#18690 <http://tracker.ceph.com/issues/18690>`_, `issue#10792 <http://tracker.ceph.com/issues/10792>`_, `pr#13262 <https://github.com/ceph/ceph/pull/13262>`_, Patrick Donnelly)
* cephfs,tests: ceph-object-corpus: mark MMDSSlaveRequest incompat change (`pr#15730 <https://github.com/ceph/ceph/pull/15730>`_, Sage Weil)
* cephfs,tests: Improve vstart_runner to (optionally) create its own cluster (`pr#12800 <https://github.com/ceph/ceph/pull/12800>`_, John Spray)
* cephfs,tests: qa: fix float parse error in test_fragment (`pr#15122 <https://github.com/ceph/ceph/pull/15122>`_, Patrick Donnelly)
* cephfs,tests: qa: fix test_standby_for_invalid_fscid with vstart_runner (`pr#14272 <https://github.com/ceph/ceph/pull/14272>`_, John Spray)
* cephfs,tests: qa: handle SSHException in logrotate (`pr#13359 <https://github.com/ceph/ceph/pull/13359>`_, John Spray)
* cephfs,tests: qa, mds: add checks for fragmentation, and enable it by default (`issue#16523 <http://tracker.ceph.com/issues/16523>`_, `pr#13862 <https://github.com/ceph/ceph/pull/13862>`_, john Spray, John Spray)
* cephfs,tests: qa: misc cephfs test improvements (`issue#20131 <http://tracker.ceph.com/issues/20131>`_, `pr#15411 <https://github.com/ceph/ceph/pull/15411>`_, John Spray)
* cephfs,tests: qa: re-enable ENOSPC tests for kclient (`issue#19550 <http://tracker.ceph.com/issues/19550>`_, `pr#14396 <https://github.com/ceph/ceph/pull/14396>`_, John Spray)
* cephfs,tests: qa: silence spurious insufficient standby health warnings (`pr#15035 <https://github.com/ceph/ceph/pull/15035>`_, Patrick Donnelly)
* cephfs,tests: qa: silence upgrade test failure (`issue#19934 <http://tracker.ceph.com/issues/19934>`_, `pr#15126 <https://github.com/ceph/ceph/pull/15126>`_, Patrick Donnelly)
* cephfs,tests: qa: simplify TestJournalRepair (`pr#15096 <https://github.com/ceph/ceph/pull/15096>`_, John Spray)
* cephfs,tests: qa/tasks: force umount during kclient teardown (`issue#18663 <http://tracker.ceph.com/issues/18663>`_, `pr#13099 <https://github.com/ceph/ceph/pull/13099>`_, John Spray)
* cephfs,tests: qa: Tidy up fs/ suite (`pr#14575 <https://github.com/ceph/ceph/pull/14575>`_, John Spray)
* cephfs,tests: qa/vstart_runner: amend ps invocation (`pr#14254 <https://github.com/ceph/ceph/pull/14254>`_, Ilya Dryomov)
* cephfs,tests: qa: whitelist another fullness log message (`issue#19253 <http://tracker.ceph.com/issues/19253>`_, `pr#14221 <https://github.com/ceph/ceph/pull/14221>`_, John Spray)
* cephfs,tests: tasks/cephfs: tear down on mount() failure (`pr#13282 <https://github.com/ceph/ceph/pull/13282>`_, John Spray)
* cephfs: tools/cephfs: remove `apply` mode of cephfs-journal-tool (`pr#15715 <https://github.com/ceph/ceph/pull/15715>`_, John Spray)
* cephfs: tools/cephfs: set dir_layout when injecting inodes (`issue#19406 <http://tracker.ceph.com/issues/19406>`_, `pr#14234 <https://github.com/ceph/ceph/pull/14234>`_, John Spray)
* ceph-volume: use unique logical volumes (`pr#17208 <https://github.com/ceph/ceph/pull/17208>`_, Alfredo Deza)
* cleanup: .gitignore: exclude rpm files (`pr#15745 <https://github.com/ceph/ceph/pull/15745>`_, Leo Zhang)
* cleanup: Move code from .h into .cc (`pr#12737 <https://github.com/ceph/ceph/pull/12737>`_, Amir Vadai)
* cleanup: resolve compiler warnings (`pr#13236 <https://github.com/ceph/ceph/pull/13236>`_, Adam C. Emerson)
* cleanup: src: put-to operator function - const input cleanup (`issue#3977 <http://tracker.ceph.com/issues/3977>`_, `pr#15364 <https://github.com/ceph/ceph/pull/15364>`_, Jos Collin)
* cmake: add "container" to required boost components (`pr#14850 <https://github.com/ceph/ceph/pull/14850>`_, Kefu Chai)
* cmake: Add -finstrument-functions flag to OSD code (`pr#15055 <https://github.com/ceph/ceph/pull/15055>`_, Mohamad Gebai)
* cmake: add RGW and MDS to libcephd (`pr#12345 <https://github.com/ceph/ceph/pull/12345>`_, Bassam Tabbara)
* cmake: Add simple recursive ctags target for Ceph source only (`pr#14334 <https://github.com/ceph/ceph/pull/14334>`_, Kefu Chai, Dan Mick)
* cmake: align cmake names of library packages (`issue#19853 <http://tracker.ceph.com/issues/19853>`_, `pr#14951 <https://github.com/ceph/ceph/pull/14951>`_, Nathan Cutler)
* cmake: Allow tests to build without NSS (`pr#13315 <https://github.com/ceph/ceph/pull/13315>`_, Daniel Gryniewicz)
* cmake: build boost as an external project (`pr#15376 <https://github.com/ceph/ceph/pull/15376>`_, Kefu Chai)
* cmake: build tracepoint libraries for vstart target (`pr#14354 <https://github.com/ceph/ceph/pull/14354>`_, Mohamad Gebai)
* cmake: check the existence of gperf before using it (`pr#15164 <https://github.com/ceph/ceph/pull/15164>`_, Kefu Chai)
* cmake: cleanup the use of udev and blkid in target_link_lib() (`pr#12811 <https://github.com/ceph/ceph/pull/12811>`_, Willem Jan Withagen)
* cmake: disable -fvar-tracking-assignments for config.cc (`pr#16695 <https://github.com/ceph/ceph/pull/16695>`_, Kefu Chai)
* cmake: disable mallinfo for jemalloc (`pr#12469 <https://github.com/ceph/ceph/pull/12469>`_, Bassam Tabbara)
* cmake: do not add dependencies to INTERFACE library on cmake < 3.3 (`pr#15813 <https://github.com/ceph/ceph/pull/15813>`_, Kefu Chai)
* cmake: do not compile crush twice (`pr#14725 <https://github.com/ceph/ceph/pull/14725>`_, Kefu Chai)
* cmake: do not link libcommon against some libs (`pr#15340 <https://github.com/ceph/ceph/pull/15340>`_, Willem Jan Withagen)
* cmake: do not try to add submodule to exclude list if .git is not around (`pr#14495 <https://github.com/ceph/ceph/pull/14495>`_, Kefu Chai)
* cmake: enable cross-compilation of boost (`issue#18938 <http://tracker.ceph.com/issues/18938>`_, `pr#14881 <https://github.com/ceph/ceph/pull/14881>`_, Kefu Chai)
* cmake: exclude \*.css while generating ctags (`pr#15663 <https://github.com/ceph/ceph/pull/15663>`_, Leo Zhang)
* cmake: explictly call find_package(PythonInterp) first to fix build err (`pr#12385 <https://github.com/ceph/ceph/pull/12385>`_, Yixun Lan)
* cmake: fix boost components for WITH_SYSTEM_BOOST (`pr#15160 <https://github.com/ceph/ceph/pull/15160>`_, Bassam Tabbara)
* cmake: Fix broken async/rdma compilation since move to libceph-common (`pr#13122 <https://github.com/ceph/ceph/pull/13122>`_, Oren Duer)
* cmake: fix broken RDMA compilation after merge PR #12878 (`pr#13186 <https://github.com/ceph/ceph/pull/13186>`_, Oren Duer)
* cmake: fix hard coded boost python lib (`pr#12480 <https://github.com/ceph/ceph/pull/12480>`_, John Coyle)
* cmake: fix rpath on shared libraries and binaries targets (`pr#12927 <https://github.com/ceph/ceph/pull/12927>`_, Ricardo Dias)
* cmake: fix the build with -DWITH_ZFS=ON (`pr#15907 <https://github.com/ceph/ceph/pull/15907>`_, Kefu Chai)
* cmake: fix the linked lib reference of unittest_rgw_crypto (`pr#14869 <https://github.com/ceph/ceph/pull/14869>`_, Willem Jan Withagen)
* cmake: improved build speed by 5x when using ccache (`pr#15147 <https://github.com/ceph/ceph/pull/15147>`_, Bassam Tabbara)
* cmake: kill duplicated cmake commands (`pr#14948 <https://github.com/ceph/ceph/pull/14948>`_, liuchang0812)
* cmake: link against fcgi only if enabled (`pr#15425 <https://github.com/ceph/ceph/pull/15425>`_, Yao Zongyou)
* cmake: link ceph-{mgr,mon,mds,osd} against libcommon statically (`pr#12878 <https://github.com/ceph/ceph/pull/12878>`_, Kefu Chai)
* cmake: link consumers of libclient with libcommon (`issue#18838 <http://tracker.ceph.com/issues/18838>`_, `pr#13394 <https://github.com/ceph/ceph/pull/13394>`_, Kefu Chai)
* cmake: misc fixes for build on i386 (`pr#15516 <https://github.com/ceph/ceph/pull/15516>`_, James Page)
* cmake: pass -d0 to b2 if not CMAKE_VERBOSE_MAKEFILE (`pr#14651 <https://github.com/ceph/ceph/pull/14651>`_, Kefu Chai)
* cmake: remove Findpciaccess.cmake (`pr#12776 <https://github.com/ceph/ceph/pull/12776>`_, optimistyzy)
* cmake: Rewrite HAVE_BABELTRACE  option to WITH (`pr#15305 <https://github.com/ceph/ceph/pull/15305>`_, Willem Jan Withagen)
* cmake: rgw: do not link against boost in a wholesale (`pr#15347 <https://github.com/ceph/ceph/pull/15347>`_, Nathan Cutler, Kefu Chai)
* cmake: search for Keyutils in default paths (`pr#12769 <https://github.com/ceph/ceph/pull/12769>`_, Pascal Bach)
* cmake: search for nspr include files for both suffixes: nspr4 and nspr (`issue#18535 <http://tracker.ceph.com/issues/18535>`_, `pr#12939 <https://github.com/ceph/ceph/pull/12939>`_, John Lin)
* cmake: should not compile crc32c_ppc.c on intel arch (`pr#14423 <https://github.com/ceph/ceph/pull/14423>`_, Kefu Chai)
* cmake: simplify find_package jemalloc (`pr#12468 <https://github.com/ceph/ceph/pull/12468>`_, Bassam Tabbara)
* cmake: support for external rocksdb (`pr#12467 <https://github.com/ceph/ceph/pull/12467>`_, Bassam Tabbara)
* cmake: support optional argument for overriding default ctag excludes (`pr#14379 <https://github.com/ceph/ceph/pull/14379>`_, Kefu Chai)
* cmake: turn libcommon into a shared library (`pr#12840 <https://github.com/ceph/ceph/pull/12840>`_, Kefu Chai)
* cmake: use CMAKE_INSTALL_INCLUDEDIR (`pr#16483 <https://github.com/ceph/ceph/pull/16483>`_, David Disseldorp)
* cmake: workaound ccache issue with .S assembly files (`pr#15142 <https://github.com/ceph/ceph/pull/15142>`_, Bassam Tabbara)
* common: add ceph::size() (`pr#15181 <https://github.com/ceph/ceph/pull/15181>`_, Kefu Chai)
* common: add override in common and misc (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13443 <https://github.com/ceph/ceph/pull/13443>`_, liuchang0812)
* common: add override in header file (`pr#13774 <https://github.com/ceph/ceph/pull/13774>`_, liuchang0812)
* common: add override in msg subsystem (`pr#13771 <https://github.com/ceph/ceph/pull/13771>`_, liuchang0812)
* common: auth: Enhancement for the supported auth methods (`pr#12937 <https://github.com/ceph/ceph/pull/12937>`_, Dave Chen)
* common: auth/RotatingKeyRing: use std::move() to set secrets (`pr#15866 <https://github.com/ceph/ceph/pull/15866>`_, Kefu Chai)
* common: avoid statically allocating configuration options (`issue#20869 <http://tracker.ceph.com/issues/20869>`_, `pr#16735 <https://github.com/ceph/ceph/pull/16735>`_, Jason Dillaman)
* common: Better handling for missing/inaccessible ceph.conf files (`issue#19658 <http://tracker.ceph.com/issues/19658>`_, `pr#14757 <https://github.com/ceph/ceph/pull/14757>`_, Dan Mick)
* common: bufferlist: cleanup semantical wrong for bufferlist::append (`pr#12247 <https://github.com/ceph/ceph/pull/12247>`_, Yankun Li)
* common: buffer: silence unused var warning on FreeBSD (`pr#16452 <https://github.com/ceph/ceph/pull/16452>`_, Willem Jan Withagen)
* common: ceph_osd: remove client message cap limit (`pr#14944 <https://github.com/ceph/ceph/pull/14944>`_, Haomai Wang)
* common: ceph: wait for maps before doing 'ceph tell ... help' (`issue#20113 <http://tracker.ceph.com/issues/20113>`_, `pr#16756 <https://github.com/ceph/ceph/pull/16756>`_, Sage Weil)
* common: cls/log/cls_log.cc: reduce logging noise (`issue#19835 <http://tracker.ceph.com/issues/19835>`_, `pr#14879 <https://github.com/ceph/ceph/pull/14879>`_, Willem Jan Withagen)
* common: cls: optimize header file dependency (`pr#15165 <https://github.com/ceph/ceph/pull/15165>`_, Brad Hubbard, Xiaowei Chen)
* common: cmdparse: more constness (`pr#15023 <https://github.com/ceph/ceph/pull/15023>`_, Kefu Chai)
* common: common/admin_socket: add config for admin socket permission bits (`pr#11684 <https://github.com/ceph/ceph/pull/11684>`_, runsisi)
* common: common/admin-socket: fix potential buffer overflow (`pr#12518 <https://github.com/ceph/ceph/pull/12518>`_, Yunchuan Wen)
* common: common/auth: add override in headers (`pr#13692 <https://github.com/ceph/ceph/pull/13692>`_, liuchang0812)
* common: common/BackTrace: add operator<< (`pr#9028 <https://github.com/ceph/ceph/pull/9028>`_, Kefu Chai)
* common: common/BackTrace: demangle on FreeBSD also (`pr#12992 <https://github.com/ceph/ceph/pull/12992>`_, Kefu Chai)
* common: common/buffer: close pipe fd if set nonblocking fails (`pr#12828 <https://github.com/ceph/ceph/pull/12828>`_, donglinpeng)
* common: common/buffer: off-by-one error in max iov length blocking (`issue#20907 <http://tracker.ceph.com/issues/20907>`_, `pr#16803 <https://github.com/ceph/ceph/pull/16803>`_, Dan Mick)
* common: common/ceph_context.cc: Use CEPH_DEV to reduce logfile noise (`pr#10384 <https://github.com/ceph/ceph/pull/10384>`_, Willem Jan Withagen)
* common: common/ceph_context: 'config diff get' option added (`pr#10736 <https://github.com/ceph/ceph/pull/10736>`_, Daniel Oliveira)
* common: common/ceph_context: fewer warnings about experimental features (`pr#14170 <https://github.com/ceph/ceph/pull/14170>`_, Sage Weil)
* common: common/ceph_context: fix leak of registered commands on exit (`pr#15302 <https://github.com/ceph/ceph/pull/15302>`_, xie xingguo)
* common: common/ceph_context: Show clear message if all features are enabled (`pr#12676 <https://github.com/ceph/ceph/pull/12676>`_, Dave Chen)
* common: common/cmdparse.cc: remove unused variable 'argnum' in dump_cmd_to_json() (`pr#16862 <https://github.com/ceph/ceph/pull/16862>`_, Luo Kexue)
* common: common/common_init: disable default dout logging for UTILITY_NODOUT too (`issue#20771 <http://tracker.ceph.com/issues/20771>`_, `pr#16578 <https://github.com/ceph/ceph/pull/16578>`_, Sage Weil)
* common: common/config: Add /usr/local/etc/ceph to default paths (`pr#14797 <https://github.com/ceph/ceph/pull/14797>`_, Willem Jan Withagen)
* common: common/config: eliminate config_t::set_val unsafe option (`issue#19106 <http://tracker.ceph.com/issues/19106>`_, `pr#13687 <https://github.com/ceph/ceph/pull/13687>`_, liuchang0812)
* common: common/config: fix return type of string::find and use string::npos (`pr#9924 <https://github.com/ceph/ceph/pull/9924>`_, Yan Jun)
* common: common,config: OPT_FLOAT and OPT_DOUBLE output format in config show (`issue#20104 <http://tracker.ceph.com/issues/20104>`_, `pr#15647 <https://github.com/ceph/ceph/pull/15647>`_, Yanhu Cao)
* common: common/config_opt: remove unused config (`pr#15874 <https://github.com/ceph/ceph/pull/15874>`_, alex.wu)
* common: common/config_opts: drop unused opt (`pr#15876 <https://github.com/ceph/ceph/pull/15876>`_, Yanhu Cao)
* common: common/config_opts.h: FreeBSD timing changed due to no SO_REUSEADDR (`pr#12594 <https://github.com/ceph/ceph/pull/12594>`_, Willem Jan Withagen)
* common: common/config_opts.h: Remove deprecated osd_compact_leveldb_on_mount option (`issue#19318 <http://tracker.ceph.com/issues/19318>`_, `pr#14059 <https://github.com/ceph/ceph/pull/14059>`_, Vikhyat Umrao)
* common: common/config_opts.h: remove obsolete configuration option (`pr#12659 <https://github.com/ceph/ceph/pull/12659>`_, Li Wang)
* common: common/config_opts: Set the HDD throttle cost to 1.5M (`pr#14808 <https://github.com/ceph/ceph/pull/14808>`_, Mark Nelson)
* common: common/EventTrace: fix compiler warning (`pr#13659 <https://github.com/ceph/ceph/pull/13659>`_, Jianpeng Ma)
* common: common/Finisher: fix uninitialized variable warning (`pr#14958 <https://github.com/ceph/ceph/pull/14958>`_, Piotr Dałek)
* common: common/freebsd_errno.cc: fixed again a stupid typo (`pr#15742 <https://github.com/ceph/ceph/pull/15742>`_, Willem Jan Withagen)
* common: common/interval_set: return int64_t for size() (`pr#12898 <https://github.com/ceph/ceph/pull/12898>`_, Xinze Chi)
* common: common/iso_8601.cc: Make return expression Clang compatible (`pr#15336 <https://github.com/ceph/ceph/pull/15336>`_, Willem Jan Withagen)
* common: common/LogEntry: include EntityName in log entries (`pr#15395 <https://github.com/ceph/ceph/pull/15395>`_, Sage Weil)
* common: common/Mutex.cc: fixed the error in comment (`pr#16214 <https://github.com/ceph/ceph/pull/16214>`_, Pan Liu)
* common: common/options: refactors to set the properties in a more structured way (`pr#16482 <https://github.com/ceph/ceph/pull/16482>`_, Kefu Chai)
* common: common,osdc: remove atomic_t completely (`pr#15562 <https://github.com/ceph/ceph/pull/15562>`_, Kefu Chai)
* common: common/perf_counters: add average time for PERFCOUNTER_TIME (`pr#15478 <https://github.com/ceph/ceph/pull/15478>`_, xie xingguo)
* common: common/perf_counters: fix race condition with atomic variables (`pr#14227 <https://github.com/ceph/ceph/pull/14227>`_, J. Eric Ivancich)
* common: common/perf_counters: make schema more friendly and update docs (`pr#14933 <https://github.com/ceph/ceph/pull/14933>`_, Sage Weil)
* common: common/perf_counters.: Remove unnecessary judgment (`pr#10407 <https://github.com/ceph/ceph/pull/10407>`_, zhang.zezhu)
* common: common/simple_spin: use __ppc_yield() on all powerpc archs (`pr#14310 <https://github.com/ceph/ceph/pull/14310>`_, Kefu Chai)
* common: common,test: migrate atomic_t to std::atomic (`pr#14866 <https://github.com/ceph/ceph/pull/14866>`_, Jesse Williamson)
* common: common/Timer: do not add event if already shutdown (`issue#20432 <http://tracker.ceph.com/issues/20432>`_, `pr#16201 <https://github.com/ceph/ceph/pull/16201>`_, Kefu Chai)
* common:   common/WorkQueue: use threadpoolname + threadaddr for heartbeat_han… (`pr#16563 <https://github.com/ceph/ceph/pull/16563>`_, huangjun)
* common: common/xmlformatter: turn on underscored and add unittest (`pr#12916 <https://github.com/ceph/ceph/pull/12916>`_, liuchang0812)
* common: compressor/zlib: remove g_ceph_context/g_conf from compressor plugin (`pr#16245 <https://github.com/ceph/ceph/pull/16245>`_, Casey Bodley)
* common: compressor/zstd: add zstd compression plugin (`pr#13075 <https://github.com/ceph/ceph/pull/13075>`_, Kefu Chai, Sage Weil)
* common: config: Improve warning for unobserved value (`issue#18424 <http://tracker.ceph.com/issues/18424>`_, `pr#12855 <https://github.com/ceph/ceph/pull/12855>`_, Brad Hubbard)
* common: config_opt: use bool instead of int for the default value of filestore_debug_omap_check (`pr#15651 <https://github.com/ceph/ceph/pull/15651>`_, Leo Zhang)
* common,core: ceph_test_rados_api_misc: fix LibRadosMiscConnectFailure.ConnectFailure retry (`issue#19901 <http://tracker.ceph.com/issues/19901>`_, `pr#15522 <https://github.com/ceph/ceph/pull/15522>`_, Sage Weil)
* common: core/common: Fix ENODATA for FreeBSD with compat.h (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15685 <https://github.com/ceph/ceph/pull/15685>`_, Willem Jan Withagen)
* common,core: common, osd, tools: Add histograms to performance counters (`pr#12829 <https://github.com/ceph/ceph/pull/12829>`_, Bartłomiej Święcki)
* common,core: common/pick_address.cc: Copy public_netw to cluster_netw if cluster empty (`pr#12929 <https://github.com/ceph/ceph/pull/12929>`_, Willem Jan Withagen)
* common,core: common/TracepointProvider: add assert if dlopen error (`pr#13430 <https://github.com/ceph/ceph/pull/13430>`_, Jianpeng Ma)
* common,core: common/TrackedOp: make TrackedOp::reset_desc() safe (`issue#19110 <http://tracker.ceph.com/issues/19110>`_, `pr#13702 <https://github.com/ceph/ceph/pull/13702>`_, Sage Weil)
* common,core: mempool: put bloom_filter in mempool (`pr#13009 <https://github.com/ceph/ceph/pull/13009>`_, Sage Weil)
* common,core: osd,mds,mgr: do not dereference null rotating_keys (`issue#20667 <http://tracker.ceph.com/issues/20667>`_, `pr#16455 <https://github.com/ceph/ceph/pull/16455>`_, Sage Weil)
* common,core: osd,osdc: pg and osd-based backoff (`pr#12342 <https://github.com/ceph/ceph/pull/12342>`_, Sage Weil)
* common,core: osd/OSDMap: make osd_state 32 bits wide (`pr#15390 <https://github.com/ceph/ceph/pull/15390>`_, Sage Weil)
* common,core: osd/OSDMap: replace require_osds flags with a single require_osd_release field (`pr#15068 <https://github.com/ceph/ceph/pull/15068>`_, Sage Weil)
* common,core: osd/OSDMap: replace string-based min_compat_client with a CEPH_RELEASE uint8_t (`pr#15351 <https://github.com/ceph/ceph/pull/15351>`_, Sage Weil)
* common,core: osd/osd_types: add flag name (IGNORE_REDIRECT) (`pr#15795 <https://github.com/ceph/ceph/pull/15795>`_, Myoungwon Oh)
* common,core: rados:  we need to get the latest osdmap when pool does not exists (`pr#13289 <https://github.com/ceph/ceph/pull/13289>`_, song baisen)
* common,core,tests: Wip cppcheck errors (`pr#14446 <https://github.com/ceph/ceph/pull/14446>`_, Brad Hubbard)
* common: crc32c: include acconfig.h to fix ceph_crc32c_aarch64() (`pr#15515 <https://github.com/ceph/ceph/pull/15515>`_, Kefu Chai)
* common: crush/CrushWrapper: fix has_incompat_choose_args (`pr#15218 <https://github.com/ceph/ceph/pull/15218>`_, Sage Weil)
* common: crush/CrushWrapper: fix has_incompat_choose_args() (`pr#15244 <https://github.com/ceph/ceph/pull/15244>`_, Sage Weil)
* common: crypto: cleanup NSPR in main thread (`pr#14801 <https://github.com/ceph/ceph/pull/14801>`_, Kefu Chai)
* common: delete unused conf "filestore_debug_disable_sharded_check" (`pr#13051 <https://github.com/ceph/ceph/pull/13051>`_, Chuanhong Wang)
* common: denc: add encode/decode for basic_sstring (`pr#15135 <https://github.com/ceph/ceph/pull/15135>`_, Kefu Chai, Casey Bodley)
* common: do not print error when asok is closed (`pr#14022 <https://github.com/ceph/ceph/pull/14022>`_, Patrick Donnelly)
* common: fix building against libcryptopp (`pr#14949 <https://github.com/ceph/ceph/pull/14949>`_, Shengjing Zhu)
* common: Fix clang compilation (`pr#13335 <https://github.com/ceph/ceph/pull/13335>`_, Bartłomiej Święcki)
* common: Fix heap buffer overflow in do_request (`issue#19393 <http://tracker.ceph.com/issues/19393>`_, `pr#14173 <https://github.com/ceph/ceph/pull/14173>`_, Brad Hubbard)
* common: fix lockdep vs recursive mutexes (`pr#9940 <https://github.com/ceph/ceph/pull/9940>`_, Adam Kupczyk)
* common: fix log warnings (`pr#16056 <https://github.com/ceph/ceph/pull/16056>`_, xie xingguo)
* common: fix Option set_long_description (`pr#16668 <https://github.com/ceph/ceph/pull/16668>`_, Yan Jun)
* common: fix segfault in public IPv6 addr picking (`issue#19371 <http://tracker.ceph.com/issues/19371>`_, `pr#14124 <https://github.com/ceph/ceph/pull/14124>`_, Fabian Grünbichler)
* common: fix that $host always expands to localhost instead of actual hostname (`issue#11081 <http://tracker.ceph.com/issues/11081>`_, `pr#12998 <https://github.com/ceph/ceph/pull/12998>`_, liuchang0812)
* common: fix typo in option of rados_mon_op_timeout's comment (`pr#15681 <https://github.com/ceph/ceph/pull/15681>`_, Leo Zhang)
* common: Fix unused variable references warnings (`pr#14790 <https://github.com/ceph/ceph/pull/14790>`_, Willem Jan Withagen)
* common: follow up to new options infrastructure (`pr#16527 <https://github.com/ceph/ceph/pull/16527>`_, John Spray)
    common: Forward-declare container I/O overloads
* common: get_process_name: use getprogname on bsd systems (`pr#15338 <https://github.com/ceph/ceph/pull/15338>`_, Mykola Golub)
* common: get rid of "warning: ignoring return value of ‘strerror_r’" (`pr#12775 <https://github.com/ceph/ceph/pull/12775>`_, xie xingguo)
* common: global: we need to handle the init_on_startup return value when global_init (`pr#13018 <https://github.com/ceph/ceph/pull/13018>`_, song baisen)
* common: Implements simple_spin_t in terms of std::atomic_flag (`pr#14370 <https://github.com/ceph/ceph/pull/14370>`_, Jesse Williamson)
* common: Improved CRC calculation for zero buffers (`pr#11966 <https://github.com/ceph/ceph/pull/11966>`_, Adam Kupczyk)
* common: include/ceph_features.h uses uint64_t, which is in sys/types.h (`pr#13339 <https://github.com/ceph/ceph/pull/13339>`_, Willem Jan Withagen)
* common: include/denc: improvements (`pr#12626 <https://github.com/ceph/ceph/pull/12626>`_, Adam C. Emerson)
* common: include/denc, kv: silence gcc warnings (`pr#13458 <https://github.com/ceph/ceph/pull/13458>`_, Kefu Chai)
* common: include/denc: remove nullptr runtime magic boundedness check (`pr#13889 <https://github.com/ceph/ceph/pull/13889>`_, Sage Weil)
* common: include/lru.h: add const to member functions (`pr#15408 <https://github.com/ceph/ceph/pull/15408>`_, yonghengdexin735)
* common: include/mempool: fix typo in comments (`pr#12772 <https://github.com/ceph/ceph/pull/12772>`_, huangjun)
* common: include/rados: Fix typo in rados_ioctx_cct() doc (`pr#15220 <https://github.com/ceph/ceph/pull/15220>`_, Jos Collin)
* common: include: Redo some includes for FreeBSD (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15337 <https://github.com/ceph/ceph/pull/15337>`_, Willem Jan Withagen)
* common: initialize array in struct BackTrace (`pr#15864 <https://github.com/ceph/ceph/pull/15864>`_, Jos Collin)
* common: initialize _hash in LogEntryKey() (`pr#15615 <https://github.com/ceph/ceph/pull/15615>`_, Jos Collin)
* common: int_types.h: remove hacks to workaround old systems (`pr#15069 <https://github.com/ceph/ceph/pull/15069>`_, Kefu Chai)
* common: kv: resolve a crash issue in ~LevelDBStore() (`pr#16553 <https://github.com/ceph/ceph/pull/16553>`_, wumingqiao)
* common: librados,libradosstriper,test: migrate atomic_t to std::atomic (baragon) (`pr#14658 <https://github.com/ceph/ceph/pull/14658>`_, Jesse Williamson)
* common: librados, osd: clang fixes (`pr#13768 <https://github.com/ceph/ceph/pull/13768>`_, Kefu Chai)
* common: libradosstriper: Add example code (`pr#15350 <https://github.com/ceph/ceph/pull/15350>`_, Logan Blyth)
* common: libradosstriper: fix format injection vulnerability (`issue#20240 <http://tracker.ceph.com/issues/20240>`_, `pr#15674 <https://github.com/ceph/ceph/pull/15674>`_, Stan K)
* common: libradosstriper: fix MultiAioCompletion leaks on failure (`pr#15471 <https://github.com/ceph/ceph/pull/15471>`_, Kefu Chai)
* common: make attempts of auth rotating configurable (`pr#12563 <https://github.com/ceph/ceph/pull/12563>`_, xie xingguo)
* common: Make spinlock delay more conventional (`pr#14248 <https://github.com/ceph/ceph/pull/14248>`_, Brad Hubbard)
* common: mempool: improve dump; fix buffer accounting bugs (`pr#15403 <https://github.com/ceph/ceph/pull/15403>`_, Sage Weil)
* common: messages: fix return type name of MOSDMap (`pr#14382 <https://github.com/ceph/ceph/pull/14382>`_, Leo Zhang)
* common: mgr/PyFormatter: implement dump_format_va (`pr#15634 <https://github.com/ceph/ceph/pull/15634>`_, Sage Weil)
* common: misc cleanups in common, global, os, osd submodules (`pr#16321 <https://github.com/ceph/ceph/pull/16321>`_, Yan Jun)
* common: misc fixes detected by crypto shutdown assert (`pr#12925 <https://github.com/ceph/ceph/pull/12925>`_, Sage Weil)
* common,mon: crush,mon: add weight-set introspection and manipulation commands (`pr#16326 <https://github.com/ceph/ceph/pull/16326>`_, Sage Weil)
* common,mon: messenger,client,compressor: migrate atomic_t to std::atomic (`pr#14657 <https://github.com/ceph/ceph/pull/14657>`_, Jesse Williamson)
* common: mon/MonClient: scale backoff interval down when we have a healthy mon session (`issue#20371 <http://tracker.ceph.com/issues/20371>`_, `pr#16576 <https://github.com/ceph/ceph/pull/16576>`_, Kefu Chai, Sage Weil)
* common,mon: mon,crush: add 'osd crush swap-bucket' command (`pr#15072 <https://github.com/ceph/ceph/pull/15072>`_, Sage Weil)
* common: msg/async: add assert of ms_async_op_threads > 0 (`pr#15629 <https://github.com/ceph/ceph/pull/15629>`_, linbing)
* common: msg/async: assert if compiled code doesn't support the configured ms (`pr#12559 <https://github.com/ceph/ceph/pull/12559>`_, Avner BenHanoch)
* common: msg/async: fix crash that writing char to nonblock-fd gets EAGAIN in EventCenter::wakeup (`pr#13822 <https://github.com/ceph/ceph/pull/13822>`_, liuchang0812)
* common: msg/async: make recv_stamp more precise (`pr#15810 <https://github.com/ceph/ceph/pull/15810>`_, Pan Liu)
* common: msg/async/rdma: Add fork safe on RDMA (`pr#13740 <https://github.com/ceph/ceph/pull/13740>`_, Sarit Zubakov)
* common: msg/async/rdma: clean line endings (`pr#12688 <https://github.com/ceph/ceph/pull/12688>`_, Adir Lev)
* common: msg/async/rdma: Remove compilation warning (`pr#13142 <https://github.com/ceph/ceph/pull/13142>`_, Sarit Zubakov)
* common: msg/async/rdma: rename chunk_size to buffer_size (`pr#13666 <https://github.com/ceph/ceph/pull/13666>`_, Adir Lev)
* common: msg/async/rdma: Support for RoCE v2 and SL (`pr#12556 <https://github.com/ceph/ceph/pull/12556>`_, Oren Duer)
* common: msg/async/rdma: Update fix broken compilation (`pr#13940 <https://github.com/ceph/ceph/pull/13940>`_, Sarit Zubakov)
* common: msg/async: return right away in NetHandler::set_priority() if not supported (`pr#14795 <https://github.com/ceph/ceph/pull/14795>`_, Kefu Chai)
* common: msg/simple: call clear_pipe in wait() shutdown path (`issue#15784 <http://tracker.ceph.com/issues/15784>`_, `pr#12633 <https://github.com/ceph/ceph/pull/12633>`_, Sage Weil)
* common: msg/SimpleMessenger: error out misplace in set_socket_options (`pr#13961 <https://github.com/ceph/ceph/pull/13961>`_, wangzhengyong)
* common: msg/simple/Pipe: support IPv6 QoS (`issue#18887 <http://tracker.ceph.com/issues/18887>`_, `pr#13370 <https://github.com/ceph/ceph/pull/13370>`_, Robin H. Johnson)
* common: .organizationmap: Updated authors (`pr#14360 <https://github.com/ceph/ceph/pull/14360>`_, Jos Collin)
* common: osdc: fix osdc_osd_seesion perf counter (`pr#13478 <https://github.com/ceph/ceph/pull/13478>`_, Xiaoxi Chen)
* common: osdc/Objecter: fix bugs in explicit naming of op spg_t (`pr#13534 <https://github.com/ceph/ceph/pull/13534>`_, Sage Weil)
* common: osdc/Objecter: fix pool dne corner case (`issue#19552 <http://tracker.ceph.com/issues/19552>`_, `pr#14901 <https://github.com/ceph/ceph/pull/14901>`_, Sage Weil)
* common: osdc/Objecter: handle command target that goes down (`issue#19452 <http://tracker.ceph.com/issues/19452>`_, `pr#14302 <https://github.com/ceph/ceph/pull/14302>`_, Sage Weil)
* common: osdc/Objecter: release message if it's not handled (`issue#19741 <http://tracker.ceph.com/issues/19741>`_, `pr#15890 <https://github.com/ceph/ceph/pull/15890>`_, Kefu Chai)
* common: osdc/Objecter: resend RWORDERED ops on full (`issue#19133 <http://tracker.ceph.com/issues/19133>`_, `pr#13759 <https://github.com/ceph/ceph/pull/13759>`_, Sage Weil)
* common: osd/OSDMap: fix feature commit comment (`pr#15056 <https://github.com/ceph/ceph/pull/15056>`_, Sage Weil)
* common: osd/OSDMap: get_previous_up_osd_before() may run into endless loop (`pr#12976 <https://github.com/ceph/ceph/pull/12976>`_, Mingxin Liu)
* common: osd/OSDMap: print require_osd_release (`pr#15974 <https://github.com/ceph/ceph/pull/15974>`_, Sage Weil)
* common: osd/osd_types: clean up OSDOp printers (`pr#12980 <https://github.com/ceph/ceph/pull/12980>`_, Sage Weil)
* common: Passing null pointer option_name to operator << in md_config_t::parse_option() (`pr#15881 <https://github.com/ceph/ceph/pull/15881>`_, Jos Collin)
* common,performance: buffer: allow buffers to be accounted in arbitrary mempools (`pr#15352 <https://github.com/ceph/ceph/pull/15352>`_, Sage Weil)
* common,performance: common/Finisher: batch handle perfcounter && only send signal when waiter existed (`pr#14363 <https://github.com/ceph/ceph/pull/14363>`_, Jianpeng Ma)
* common,performance: crc32c: Add ppc64le fast zero optimized assembly (`pr#15100 <https://github.com/ceph/ceph/pull/15100>`_, Andrew Solomon)
* common,performance: inline_memory: optimized mem_is_zero for non-x64 (`pr#15307 <https://github.com/ceph/ceph/pull/15307>`_, Piotr Dałek)
* common,performance: kv/rocksdb: supports SliceParts interface (`pr#15058 <https://github.com/ceph/ceph/pull/15058>`_, Haomai Wang)
* common,performance: osd/OSDMap: make pg_temp more efficient (`pr#15291 <https://github.com/ceph/ceph/pull/15291>`_, Sage Weil)
* common: possible lockdep false alarm for ThreadPool lock (`issue#18819 <http://tracker.ceph.com/issues/18819>`_, `pr#13258 <https://github.com/ceph/ceph/pull/13258>`_, Mykola Golub)
* common: prevent unset_dumpable from generating warnings (`pr#16462 <https://github.com/ceph/ceph/pull/16462>`_, Willem Jan Withagen)
* common: rados: allow "rados purge" to delete objects when osd is full (`pr#13814 <https://github.com/ceph/ceph/pull/13814>`_, Pan Liu)
* common: rados: more info added to pool deletion error (`issue#19400 <http://tracker.ceph.com/issues/19400>`_, `pr#14235 <https://github.com/ceph/ceph/pull/14235>`_, Vedant Nanda)
* common,rbd: osdc/Objecter: unify disparate EAGAIN handling paths into one (`pr#16627 <https://github.com/ceph/ceph/pull/16627>`_, Sage Weil)
* common,rbd,rgw: common/escape: do not escape / in json (`pr#14130 <https://github.com/ceph/ceph/pull/14130>`_, Sage Weil)
* common,rbd,rgw: common/rgw/rbd: remove some unused variables (`pr#16690 <https://github.com/ceph/ceph/pull/16690>`_, Luo Kexue)
* common,rdma: msg/async/rdma: automatically set RDMAV_HUGEPAGES_SAFE according to conf (`pr#15755 <https://github.com/ceph/ceph/pull/15755>`_, DanielBar-On)
* common,rdma: msg/async/rdma: check ulimit (`pr#13655 <https://github.com/ceph/ceph/pull/13655>`_, Sarit Zubakov, Adir Lev)
* common,rdma: msg/async/rdma: Introduce Device.{cc,h} (`pr#14001 <https://github.com/ceph/ceph/pull/14001>`_, Amir Vadai)
* common,rdma: msg/async/rdma: Introduce RDMAConnMgr + Debug prints (`pr#14201 <https://github.com/ceph/ceph/pull/14201>`_, Amir Vadai)
* common,rdma: msg/async/rdma: Move resource handling to Device (`pr#14088 <https://github.com/ceph/ceph/pull/14088>`_, Sarit Zubakov, Amir Vadai)
* common,rdma: msg/async/rdma: RDMA-CM Initialize device on first connect (`pr#14179 <https://github.com/ceph/ceph/pull/14179>`_, Amir Vadai)
* common,rdma: msg/async/rdma: reduce number of rdma rx/tx buffers (`pr#13190 <https://github.com/ceph/ceph/pull/13190>`_, Adir Lev)
* common,rdma: msg/async/rdma: use lists properly (`pr#15908 <https://github.com/ceph/ceph/pull/15908>`_, Adir lev, Adir Lev)
* common: remove config opt conversion utility (`pr#16480 <https://github.com/ceph/ceph/pull/16480>`_, John Spray)
* common: remove \n on clog messages (`pr#13794 <https://github.com/ceph/ceph/pull/13794>`_, Sage Weil)
* common: Remove redundant includes - 2 (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15169 <https://github.com/ceph/ceph/pull/15169>`_, Jos Collin)
* common: Remove redundant includes - 3 (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15204 <https://github.com/ceph/ceph/pull/15204>`_, Jos Collin)
* common: Remove redundant includes - 4 (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15251 <https://github.com/ceph/ceph/pull/15251>`_, Jos Collin)
* common: Remove redundant includes - 5 (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15267 <https://github.com/ceph/ceph/pull/15267>`_, Jos Collin)
* common: Remove redundant includes - 6 (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15299 <https://github.com/ceph/ceph/pull/15299>`_, Jos Collin)
* common: Remove redundant includes (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15003 <https://github.com/ceph/ceph/pull/15003>`_, Brad Hubbard)
* common: Remove redundant includes (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15019 <https://github.com/ceph/ceph/pull/15019>`_, Brad Hubbard)
* common: Remove redundant includes (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15042 <https://github.com/ceph/ceph/pull/15042>`_, Brad Hubbard)
* common: Remove redundant includes (`issue#19883 <http://tracker.ceph.com/issues/19883>`_, `pr#15086 <https://github.com/ceph/ceph/pull/15086>`_, Jos Collin)
* common: remove useless parameter (`pr#14096 <https://github.com/ceph/ceph/pull/14096>`_, baiyanchun)
* common: Revamp config option definitions (`issue#20627 <http://tracker.ceph.com/issues/20627>`_, `pr#16211 <https://github.com/ceph/ceph/pull/16211>`_, John Spray, Kefu Chai, Sage Weil)
* common,rgw: cls/refcount: store and use list of retired tags (`issue#20107 <http://tracker.ceph.com/issues/20107>`_, `pr#15673 <https://github.com/ceph/ceph/pull/15673>`_, Yehuda Sadeh)
* common: src/common/ceph_string: stringify new osd states (`pr#15751 <https://github.com/ceph/ceph/pull/15751>`_, xie xingguo)
* common: src/common: change last_work_queue to next_work_queue (`pr#14738 <https://github.com/ceph/ceph/pull/14738>`_, Pan Liu)
* common: support s390 and unknown architectures in spin-wait loop (`issue#19492 <http://tracker.ceph.com/issues/19492>`_, `pr#14337 <https://github.com/ceph/ceph/pull/14337>`_, Nathan Cutler)
* common,tests: ceph_test_rados_api_c_read_operations: do not assert per-op rval is correct (`issue#19518 <http://tracker.ceph.com/issues/19518>`_, `pr#16196 <https://github.com/ceph/ceph/pull/16196>`_, Sage Weil)
* common,tests: ceph_test_rados_api_list: more fix LibRadosListNP.ListObjectsError (`issue#19963 <http://tracker.ceph.com/issues/19963>`_, `pr#15138 <https://github.com/ceph/ceph/pull/15138>`_, Sage Weil)
* common,tests: test: Make screencandy optional for FreeBSD (`pr#15444 <https://github.com/ceph/ceph/pull/15444>`_, Willem Jan Withagen)
* common: the latency dumped by "ceph osd perf" is not real (`issue#20749 <http://tracker.ceph.com/issues/20749>`_, `pr#16512 <https://github.com/ceph/ceph/pull/16512>`_, Pan Liu)
* common,tools: osdmaptool: show all the pg map to osds info (`pr#9419 <https://github.com/ceph/ceph/pull/9419>`_, song baisen)
* common: tracing: Fix handle leak in TracepointProvider (`pr#12652 <https://github.com/ceph/ceph/pull/12652>`_, Brad Hubbard)
* common: tracing: fix segv (`issue#18576 <http://tracker.ceph.com/issues/18576>`_, `pr#14304 <https://github.com/ceph/ceph/pull/14304>`_, Anjaneya Chagam)
* common: Update the error string when res_nsearch() or res_search() fails (`pr#15878 <https://github.com/ceph/ceph/pull/15878>`_, huanwen ren)
* common: use ref to avoid unnecessary memory copy (`issue#19107 <http://tracker.ceph.com/issues/19107>`_, `pr#13689 <https://github.com/ceph/ceph/pull/13689>`_, liuchang0812)
* common: use std::move() for better performance (`pr#16620 <https://github.com/ceph/ceph/pull/16620>`_, Xinying Song)
* common: xio: migrate atomic_t to std::atomic<> (`pr#15230 <https://github.com/ceph/ceph/pull/15230>`_, Jesse Williamson)
* compressor: conditionalize on HAVE_LZ4 (`pr#17174 <https://github.com/ceph/ceph/pull/17174>`_, Kefu Chai)
* compressor: fix Mutex::Locker used is not correct (`pr#13935 <https://github.com/ceph/ceph/pull/13935>`_, hechuang)
* compressor: zlib: fix plugin for non-Intel arches (`pr#14947 <https://github.com/ceph/ceph/pull/14947>`_, Dan Mick)
* core: auth: 'ceph auth import -i' overwrites caps, if caps are not specified (`issue#18932 <http://tracker.ceph.com/issues/18932>`_, `pr#13468 <https://github.com/ceph/ceph/pull/13468>`_, Vikhyat Umrao)
* core: auth: Remove unused function in AuthSessionHandler (`pr#16666 <https://github.com/ceph/ceph/pull/16666>`_, Luo Kexue)
* core: ceph: allow '-' with -i and -o for stdin/stdout (`pr#16359 <https://github.com/ceph/ceph/pull/16359>`_, Sage Weil)
* core: ceph-create-keys: Add connection timeouts (`pr#11995 <https://github.com/ceph/ceph/pull/11995>`_, Owen Synge)
* core: ceph-dencoder: Silence coverity CID 1412579 (`pr#15744 <https://github.com/ceph/ceph/pull/15744>`_, Brad Hubbard)
* core: ceph-detect-init: Add docker detection (`pr#13218 <https://github.com/ceph/ceph/pull/13218>`_, Guillaume Abrioux)
* core: ceph-disk: Adding retry loop in get_partition_dev() (`pr#14275 <https://github.com/ceph/ceph/pull/14275>`_, Erwan Velu)
* core: ceph-disk/ceph_disk/main.py: fix calling of the bsdrc init scripts (`pr#14476 <https://github.com/ceph/ceph/pull/14476>`_, Willem Jan Withagen)
* core: ceph-disk/ceph_disk/main.py: Replace ST_ISBLK() test by is_diskdevice() (`pr#15587 <https://github.com/ceph/ceph/pull/15587>`_, Willem Jan Withagen)
* core: ceph-disk: ceph-disk on FreeBSD should not use mpath-code (`pr#14837 <https://github.com/ceph/ceph/pull/14837>`_, Willem Jan Withagen)
* core: ceph-disk: dmcrypt activate must use the same cluster as prepare (`issue#17821 <http://tracker.ceph.com/issues/17821>`_, `pr#13573 <https://github.com/ceph/ceph/pull/13573>`_, Loic Dachary)
* core: ceph-disk: dmcrypt cluster must default to ceph (`issue#20893 <http://tracker.ceph.com/issues/20893>`_, `pr#16776 <https://github.com/ceph/ceph/pull/16776>`_, Loic Dachary)
* core: ceph-disk: do not setup_statedir on trigger (`issue#19941 <http://tracker.ceph.com/issues/19941>`_, `pr#15410 <https://github.com/ceph/ceph/pull/15410>`_, Loic Dachary)
* core: ceph-disk: enable directory backed OSD at boot time (`issue#19628 <http://tracker.ceph.com/issues/19628>`_, `pr#14546 <https://github.com/ceph/ceph/pull/14546>`_, Loic Dachary)
* core: ceph-disk: Fix getting wrong group name when --setgroup in bluestore (`issue#18955 <http://tracker.ceph.com/issues/18955>`_, `pr#13457 <https://github.com/ceph/ceph/pull/13457>`_, craigchi)
* core: ceph-disk: FreeBSD changes to get it working and passing tests (`pr#12086 <https://github.com/ceph/ceph/pull/12086>`_, Willem Jan Withagen)
* core: ceph-disk: implement prepare --no-locking (`pr#14728 <https://github.com/ceph/ceph/pull/14728>`_, Dan van der Ster, Loic Dachary)
* core: ceph_disk/main.py: Allow FreeBSD zap a OSD disk (`pr#15642 <https://github.com/ceph/ceph/pull/15642>`_, Willem Jan Withagen)
* core: ceph-disk,osd: add support for crush device classes (`issue#19513 <http://tracker.ceph.com/issues/19513>`_, `pr#14436 <https://github.com/ceph/ceph/pull/14436>`_, Loic Dachary)
* core: ceph-disk: Populate mount options when running "list" (`issue#17331 <http://tracker.ceph.com/issues/17331>`_, `pr#14293 <https://github.com/ceph/ceph/pull/14293>`_, Brad Hubbard)
* core: ceph-disk: Reporting /sys directory in get_partition_dev() (`pr#14080 <https://github.com/ceph/ceph/pull/14080>`_, Erwan Velu)
* core: ceph-disk: Revert "Revert "change get_dmcrypt_key test to support different cluster name"" (`pr#13600 <https://github.com/ceph/ceph/pull/13600>`_, Loic Dachary)
* core: ceph-disk: separate ceph-osd --check-needs-\* logs (`issue#19888 <http://tracker.ceph.com/issues/19888>`_, `pr#15016 <https://github.com/ceph/ceph/pull/15016>`_, Loic Dachary)
* core: ceph-disk: set the default systemd unit timeout to 3h (`issue#20229 <http://tracker.ceph.com/issues/20229>`_, `pr#15585 <https://github.com/ceph/ceph/pull/15585>`_, Loic Dachary)
* core: ceph-disk: support osd new (`pr#15432 <https://github.com/ceph/ceph/pull/15432>`_, Loic Dachary, Sage Weil)
* core: ceph-disk: Write 10M to all partitions before zapping (`issue#18962 <http://tracker.ceph.com/issues/18962>`_, `pr#13766 <https://github.com/ceph/ceph/pull/13766>`_, Wido den Hollander)
* core: ceph: do not throw TypeError on connection failure (`pr#13268 <https://github.com/ceph/ceph/pull/13268>`_, Kefu Chai)
* core: ceph.in: Fix couple of minor issues on the messages (`pr#12797 <https://github.com/ceph/ceph/pull/12797>`_, Dave Chen)
* core: ceph-objectstore-tool: do not populate snapmapper with missing clones (`issue#19943 <http://tracker.ceph.com/issues/19943>`_, `pr#15787 <https://github.com/ceph/ceph/pull/15787>`_, Sage Weil)
* core: ceph-osd: fix auto detect which objectstore is currently running (`issue#20865 <http://tracker.ceph.com/issues/20865>`_, `pr#16717 <https://github.com/ceph/ceph/pull/16717>`_, Yanhu Cao)
* core: ceph-osd: --flush-journal: sporadic segfaults on exit (`issue#18820 <http://tracker.ceph.com/issues/18820>`_, `pr#13311 <https://github.com/ceph/ceph/pull/13311>`_, Alexey Sheplyakov)
* core: client/SyntheticClient.cc: Fix warning in random_walk (`issue#19445 <http://tracker.ceph.com/issues/19445>`_, `pr#14308 <https://github.com/ceph/ceph/pull/14308>`_, Brad Hubbard)
* core: cls/timeindex: clean up cls_timeindex_client.h|cc (`pr#13987 <https://github.com/ceph/ceph/pull/13987>`_, Shinobu Kinjo)
* core: common/options: remove mon_warn_osd_usage_min_max_delta from options.cc too (`pr#16488 <https://github.com/ceph/ceph/pull/16488>`_, Sage Weil)
* core: common/TrackedOp: allow dumping historic ops sorted by duration (`pr#14050 <https://github.com/ceph/ceph/pull/14050>`_, Piotr Dałek)
* core: compressor: add LZ4 support (`pr#15434 <https://github.com/ceph/ceph/pull/15434>`_, Haomai Wang)
* core: compressor: optimize header file dependency (`pr#15187 <https://github.com/ceph/ceph/pull/15187>`_, Brad Hubbard, Xiaowei Chen)
* core: Context: C_ContextsBase: delete enclosed contexts in dtor (`issue#20432 <http://tracker.ceph.com/issues/20432>`_, `pr#16159 <https://github.com/ceph/ceph/pull/16159>`_, Kefu Chai)
* core: crush/CrushWrapper: chooseargs encoding fix (`pr#15984 <https://github.com/ceph/ceph/pull/15984>`_, Ilya Dryomov)
* core: crush/CrushWrapper: make get_immediate_parent[_id] ignore per-class shadow hierarchy (`issue#20546 <http://tracker.ceph.com/issues/20546>`_, `pr#16221 <https://github.com/ceph/ceph/pull/16221>`_, Sage Weil)
* core: crush, mon: make jewel the lower bound for client/crush compat for new clusters (`pr#15370 <https://github.com/ceph/ceph/pull/15370>`_, Sage Weil)
* core: erasure-code: optimize header file dependency (`pr#15172 <https://github.com/ceph/ceph/pull/15172>`_, Brad Hubbard, Xiaowei Chen)
* core: erasure-code: Remove duplicate of isa-l files (`pr#15372 <https://github.com/ceph/ceph/pull/15372>`_, Ganesh Mahalingam)
* core: erasure-code: sync jerasure/gf-complete submodules (`pr#14424 <https://github.com/ceph/ceph/pull/14424>`_, Loic Dachary)
* core: filestore: migrate atomic_t to std::atomic<> (`pr#15228 <https://github.com/ceph/ceph/pull/15228>`_, Jesse Williamson)
* core: Give requested scrub work a higher priority (`issue#15789 <http://tracker.ceph.com/issues/15789>`_, `pr#14488 <https://github.com/ceph/ceph/pull/14488>`_, David Zafman)
* core: global: start removing g_ceph_context (`pr#12149 <https://github.com/ceph/ceph/pull/12149>`_, Adam C. Emerson)
* core: HashIndex.cc: add compat.h for ENODATA (`pr#16697 <https://github.com/ceph/ceph/pull/16697>`_, Willem Jan Withagen)
* core: include/denc: add {encode,decode}_nohead for denc_traits<basic_string> (`issue#18938 <http://tracker.ceph.com/issues/18938>`_, `pr#14099 <https://github.com/ceph/ceph/pull/14099>`_, Kefu Chai)
* core: include/mempool.h: fix Clangs complaint about types (`pr#13523 <https://github.com/ceph/ceph/pull/13523>`_, Willem Jan Withagen)
* core: include/types.h, introduce host_to_ceph_errno (`pr#15496 <https://github.com/ceph/ceph/pull/15496>`_, Willem Jan Withagen)
* core: Install Pecan for FreeBSD (`pr#15610 <https://github.com/ceph/ceph/pull/15610>`_, Willem Jan Withagen)
* core: introduce (and fix) code to pass errno to other OSes (`pr#15495 <https://github.com/ceph/ceph/pull/15495>`_, Willem Jan Withagen)
* core: introduce DirectMessenger (`pr#14755 <https://github.com/ceph/ceph/pull/14755>`_, Casey Bodley, Matt Benjamin)
* core: kv/RocksDBStore: abort if rocksdb EIO, don't return incorrect result (`pr#15862 <https://github.com/ceph/ceph/pull/15862>`_, Haomai Wang)
* core: kv/RocksDBStore: use vector instead of VLA for holding slices (`pr#16615 <https://github.com/ceph/ceph/pull/16615>`_, Kefu Chai)
* core: libradosstriper: Initialize member variable m_writeRc in WriteCompletionData (`pr#16780 <https://github.com/ceph/ceph/pull/16780>`_, amitkuma)
* core: luminous: Improve size scrub error handling and ignore system attrs in xattr checking (`issue#21051 <http://tracker.ceph.com/issues/21051>`_, `issue#18836 <http://tracker.ceph.com/issues/18836>`_, `issue#20243 <http://tracker.ceph.com/issues/20243>`_, `pr#17196 <https://github.com/ceph/ceph/pull/17196>`_, David Zafman)
* core: luminous: Include front/back interface names in OSD metadata (`issue#21048 <http://tracker.ceph.com/issues/21048>`_, `issue#20956 <http://tracker.ceph.com/issues/20956>`_, `pr#17193 <https://github.com/ceph/ceph/pull/17193>`_, John Spray)
* core: luminous: mon: bug in functon reweight_by_utilization (`issue#21079 <http://tracker.ceph.com/issues/21079>`_, `issue#20970 <http://tracker.ceph.com/issues/20970>`_, `pr#17198 <https://github.com/ceph/ceph/pull/17198>`_, xie xingguo)
* core: luminous: mon: "ceph osd crush rule rename" support (`pr#17260 <https://github.com/ceph/ceph/pull/17260>`_, xie xingguo)
* core: luminous: multisite: FAILED assert(prev_iter != pos_to_prev.end()) in RGWMetaSyncShardCR::collect_children() (`issue#21097 <http://tracker.ceph.com/issues/21097>`_, `issue#20906 <http://tracker.ceph.com/issues/20906>`_, `pr#17234 <https://github.com/ceph/ceph/pull/17234>`_, Casey Bodley)
* core: luminous: osd: osd_scrub_during_recovery only considers primary, not replicas (`issue#18206 <http://tracker.ceph.com/issues/18206>`_, `issue#21077 <http://tracker.ceph.com/issues/21077>`_, `pr#17195 <https://github.com/ceph/ceph/pull/17195>`_, David Zafman)
* core: luminous: src/common/LogClient.cc: 310: FAILED assert(num_unsent <= log_queue.size()) (`issue#20965 <http://tracker.ceph.com/issues/20965>`_, `issue#18209 <http://tracker.ceph.com/issues/18209>`_, `pr#17197 <https://github.com/ceph/ceph/pull/17197>`_, Sage Weil)
* core: make the conversion from wire error to host OS work (`pr#15780 <https://github.com/ceph/ceph/pull/15780>`_, Willem Jan Withagen)
* core: Merge pull request #16755 from ivancich/wip-pull-new-dmclock (`pr#16922 <https://github.com/ceph/ceph/pull/16922>`_, Gregory Farnum)
* core: messages: default-initialize MOSDPGRecoveryDelete[Reply] members (`pr#16584 <https://github.com/ceph/ceph/pull/16584>`_, Greg Farnum)
* core: messages: Initialize members in MMDSTableRequest (`pr#16810 <https://github.com/ceph/ceph/pull/16810>`_, amitkuma)
* core: messages: Initialize member variables (`pr#16819 <https://github.com/ceph/ceph/pull/16819>`_, amitkuma)
* core: messages: Initialize member variables (`pr#16839 <https://github.com/ceph/ceph/pull/16839>`_, amitkuma)
* core: messages: Initializing member variable in MMDSCacheRejoin (`pr#16791 <https://github.com/ceph/ceph/pull/16791>`_, amitkuma)
* core: messages/MOSDOp: fix pg_t decoding for version <7 decoding (`issue#19005 <http://tracker.ceph.com/issues/19005>`_, `pr#13537 <https://github.com/ceph/ceph/pull/13537>`_, Sage Weil)
* core: messages/MOSDPGTrim: add the missed HEAD_VERSION AND COMPAT_VERSION (`issue#18266 <http://tracker.ceph.com/issues/18266>`_, `pr#12517 <https://github.com/ceph/ceph/pull/12517>`_, huangjun)
* core: messages/MOSDPing.h: drop unused fields (`pr#15843 <https://github.com/ceph/ceph/pull/15843>`_, Piotr Dałek)
* core: messages/MOSDPing: initialize MOSDPing padding (`issue#20323 <http://tracker.ceph.com/issues/20323>`_, `pr#15714 <https://github.com/ceph/ceph/pull/15714>`_, Sage Weil)
* core: messages/MOSDSubOp: Make encode_payload can be reentrant (`pr#12654 <https://github.com/ceph/ceph/pull/12654>`_, Haomai Wang)
* core: messages: remove compat cruft (`pr#14475 <https://github.com/ceph/ceph/pull/14475>`_, Sage Weil)
* core: mgr/MgrClient: do not attempt to access a global variable for config (`pr#16544 <https://github.com/ceph/ceph/pull/16544>`_, Jason Dillaman)
* core: mgr/MgrClient: use unique_ptr for MgrClient::session (`issue#19097 <http://tracker.ceph.com/issues/19097>`_, `pr#13685 <https://github.com/ceph/ceph/pull/13685>`_, Kefu Chai)
* core,mgr: mgr/DaemonServer: stop spamming log with pg stats (`pr#15487 <https://github.com/ceph/ceph/pull/15487>`_, Sage Weil)
* core,mgr: mgr,librados: service map (`pr#15858 <https://github.com/ceph/ceph/pull/15858>`_, Yehuda Sadeh, John Spray, Sage Weil)
* core,mgr,mon: mgr,mon: enable/disable mgr modules via 'ceph mgr module ...' commands (`pr#15958 <https://github.com/ceph/ceph/pull/15958>`_, Sage Weil)
* core,mgr,mon: mon,mgr: tag some commands for ceph-mgr (`pr#13617 <https://github.com/ceph/ceph/pull/13617>`_, Sage Weil)
* core,mgr,mon: mon/PGMap: fix osd_epoch update when removing osd_stat (`issue#20208 <http://tracker.ceph.com/issues/20208>`_, `pr#15573 <https://github.com/ceph/ceph/pull/15573>`_, Sage Weil)
* core,mgr: mon/PGMap: slightly better debugging around pgmap updates (`pr#15820 <https://github.com/ceph/ceph/pull/15820>`_, Sage Weil)
* core,mgr,tests: qa: flush out monc's dropped msgs on msgr failure injection (`issue#20371 <http://tracker.ceph.com/issues/20371>`_, `pr#16484 <https://github.com/ceph/ceph/pull/16484>`_, Joao Eduardo Luis)
* core,mgr,tests: qa/suites/rados/rest: test restful mgr module (`pr#15604 <https://github.com/ceph/ceph/pull/15604>`_, Sage Weil)
* core: misc: SCA fixes (`pr#14426 <https://github.com/ceph/ceph/pull/14426>`_, Danny Al-Gaaf)
* core,mon: crush, mon: simplify device class manipulation commands (`pr#16388 <https://github.com/ceph/ceph/pull/16388>`_, xie xingguo)
* core: mon,mgr: fix "ceph osd df", add some tools to find untested commands (`issue#20256 <http://tracker.ceph.com/issues/20256>`_, `pr#15675 <https://github.com/ceph/ceph/pull/15675>`_, Greg Farnum)
* core: mon/MonClient: discard stray messages from non-acitve conns (`issue#19015 <http://tracker.ceph.com/issues/19015>`_, `pr#13656 <https://github.com/ceph/ceph/pull/13656>`_, Kefu Chai)
* core: mon/MonClient: don't return zero global_id (`issue#19134 <http://tracker.ceph.com/issues/19134>`_, `pr#13853 <https://github.com/ceph/ceph/pull/13853>`_, "Yan, Zheng", Kefu Chai)
* core: mon/MonClient: hunt monitors in parallel (`issue#16091 <http://tracker.ceph.com/issues/16091>`_, `pr#11128 <https://github.com/ceph/ceph/pull/11128>`_, Steven Dieffenbach, Kefu Chai)
* core: mon/MonClient: persist global_id across re-connecting (`issue#18968 <http://tracker.ceph.com/issues/18968>`_, `pr#13550 <https://github.com/ceph/ceph/pull/13550>`_, Kefu Chai)
* core: mon/MonClient: respect the priority in SRV RR (`issue#5249 <http://tracker.ceph.com/issues/5249>`_, `pr#15964 <https://github.com/ceph/ceph/pull/15964>`_, Kefu Chai)
* core,mon: mon/LogMonitor: 'log last' command (`pr#15497 <https://github.com/ceph/ceph/pull/15497>`_, Sage Weil)
* core: mon/MonmapMonitor: use `__func__` instead of hard code function name (`pr#16037 <https://github.com/ceph/ceph/pull/16037>`_, Yanhu Cao)
* core,mon: mon/MgrStatMonitor: avoid dup health warnings during luminous upgrade (`issue#20435 <http://tracker.ceph.com/issues/20435>`_, `pr#15986 <https://github.com/ceph/ceph/pull/15986>`_, Sage Weil)
* core,mon: mon/MgrStatMonitor: keep mgrstat version ahead of pgmon (`issue#20219 <http://tracker.ceph.com/issues/20219>`_, `pr#15584 <https://github.com/ceph/ceph/pull/15584>`_, Sage Weil)
* core,mon: mon,osd: add crush_version to OSDMap, and allow crush map updates to gate on crush_version (`pr#15533 <https://github.com/ceph/ceph/pull/15533>`_, Sage Weil)
* core,mon: mon,osd: decouple creating pgs from pgmap (`pr#13999 <https://github.com/ceph/ceph/pull/13999>`_, Kefu Chai)
* core,mon: mon, osd: misc fixes (`pr#16078 <https://github.com/ceph/ceph/pull/16078>`_, xie xingguo)
* core,mon: mon/OSDMonitor: cancel mapping job from update_from_paxos (`issue#20067 <http://tracker.ceph.com/issues/20067>`_, `pr#15320 <https://github.com/ceph/ceph/pull/15320>`_, Sage Weil)
* core,mon: mon/OSDMonitor: make 'osd crush move ...' work on osds (`issue#18587 <http://tracker.ceph.com/issues/18587>`_, `pr#12981 <https://github.com/ceph/ceph/pull/12981>`_, Sage Weil)
* core,mon: mon/OSDMonitor: make snaps on tier pool should not be allowed (`pr#9348 <https://github.com/ceph/ceph/pull/9348>`_, Mingxin Liu)
* core,mon: mon/OSDMonitor: use up set instead of acting set in reweight_by_utilization (`pr#13802 <https://github.com/ceph/ceph/pull/13802>`_, Mingxin Liu)
* core,mon: mon,osd: new mechanism for managing full and nearfull OSDs for luminous (`pr#13615 <https://github.com/ceph/ceph/pull/13615>`_, Sage Weil)
* core,mon: mon/PGMap: call blocked requests ERR not WARN (`pr#15501 <https://github.com/ceph/ceph/pull/15501>`_, Sage Weil)
* core: mon,osd: add require_min_compat_client setting to enforce and clarify client compatibility (`pr#14959 <https://github.com/ceph/ceph/pull/14959>`_, Sage Weil)
* core: mon,osd: luminous feature bits, require flags, upgrade gates (`pr#13278 <https://github.com/ceph/ceph/pull/13278>`_, Sage Weil)
* core: mon, osd: misc fixes and cleanups (`pr#16160 <https://github.com/ceph/ceph/pull/16160>`_, xie xingguo)
* core: mon, osd: misc fixes (`pr#16283 <https://github.com/ceph/ceph/pull/16283>`_, xie xingguo)
* core: mon/OSDMonitor: _apply_remap -> _apply_upmap; less code redundancy (`pr#15846 <https://github.com/ceph/ceph/pull/15846>`_, xie xingguo)
* core: mon/OSDMonitor: batch noup/noin osds support (`pr#15725 <https://github.com/ceph/ceph/pull/15725>`_, xie xingguo)
* core: mon/OSDMonitor: batch OSDs nodown/noout support (`pr#15381 <https://github.com/ceph/ceph/pull/15381>`_, xie xingguo)
* core: mon/OSDMonitor: change info in 'osd failed' messages (`pr#15321 <https://github.com/ceph/ceph/pull/15321>`_, Sage Weil)
* core: mon/OSDMonitor: do not allow crush device classes until luminous (`pr#16188 <https://github.com/ceph/ceph/pull/16188>`_, Sage Weil)
* core: mon/OSDMonitor: fixup sortbitwise flag warning (`pr#12682 <https://github.com/ceph/ceph/pull/12682>`_, huanwen ren)
* core: mon/OSDMonitor: make mapping job behave if mon_osd_prime_pg_temp = false (`issue#19020 <http://tracker.ceph.com/issues/19020>`_, `pr#13574 <https://github.com/ceph/ceph/pull/13574>`_, Sage Weil)
* core: mon/OSDMonitor: osd crush set-device-class (`issue#19307 <http://tracker.ceph.com/issues/19307>`_, `pr#14039 <https://github.com/ceph/ceph/pull/14039>`_, Loic Dachary)
* core: mon/OSDMonitor: set last_force_op_resend on overlay pool too (`issue#18366 <http://tracker.ceph.com/issues/18366>`_, `pr#12712 <https://github.com/ceph/ceph/pull/12712>`_, Sage Weil)
* core: mon/OSDMonitor: should propose osdmap update when cluster addr changed (`pr#11065 <https://github.com/ceph/ceph/pull/11065>`_, Mingxin Liu)
* core: mon/OSDMonitor: skip prime_pg_temp if mapping is prior to osdmap (`pr#14826 <https://github.com/ceph/ceph/pull/14826>`_, Kefu Chai)
* core: mon,osd/OSDMap: a couple pg-upmap fixes (`pr#15319 <https://github.com/ceph/ceph/pull/15319>`_, Sage Weil)
* core: mon/PGMap: factor mon_osd_full_ratio into MAX AVAIL calc (`issue#18522 <http://tracker.ceph.com/issues/18522>`_, `pr#12923 <https://github.com/ceph/ceph/pull/12923>`_, Sage Weil)
* core: mon/PGMonitor: fix wrongly report "pg stuck in inactive" (`pr#14391 <https://github.com/ceph/ceph/pull/14391>`_, Mingxin Liu)
* core,mon,rbd: mon,osd: new rbd-based cephx cap profiles (`pr#15991 <https://github.com/ceph/ceph/pull/15991>`_, Jason Dillaman)
* core: msg/async/AsyncConnection: keepalive objecter ping connection to avoid timeout (`pr#14009 <https://github.com/ceph/ceph/pull/14009>`_, Haomai Wang)
* core: msg/async/AsyncConnection: socket's fd can be zero, avoid false assert (`pr#13080 <https://github.com/ceph/ceph/pull/13080>`_, Haomai Wang)
* core: msg/async: avoid requeue racing with handle_write (`issue#20093 <http://tracker.ceph.com/issues/20093>`_, `pr#15324 <https://github.com/ceph/ceph/pull/15324>`_, Haomai Wang)
* core: msg/async/dpdk: fix compile errors (`pr#12698 <https://github.com/ceph/ceph/pull/12698>`_, Haomai Wang)
* core: msg/async: fix deleted_conn is out of sync with conns (`issue#20230 <http://tracker.ceph.com/issues/20230>`_, `pr#15645 <https://github.com/ceph/ceph/pull/15645>`_, Haomai Wang)
* core: msg/async: fix the bug of inaccurate calculation of l_msgr_send_bytes (`pr#16526 <https://github.com/ceph/ceph/pull/16526>`_, Jin Cai)
* core: msg/async/rdma: add log to show correct destruct queuepair (`pr#13412 <https://github.com/ceph/ceph/pull/13412>`_, Haomai Wang)
* core: msg/async/rdma: add perf counters to RDMA backend (`pr#13484 <https://github.com/ceph/ceph/pull/13484>`_, Haomai Wang)
* core: msg/async/rdma: destroy QueuePair if needed (`pr#13810 <https://github.com/ceph/ceph/pull/13810>`_, Haomai Wang)
* core: msg/async/rdma: don't need to delete event when tcp connection isn't … (`pr#13528 <https://github.com/ceph/ceph/pull/13528>`_, Haomai Wang)
* core: msg/async/rdma: fix ceph_clock_now calls (`pr#12711 <https://github.com/ceph/ceph/pull/12711>`_, Haomai Wang)
* core: msg/async/rdma: fix potential racing connection usage (`pr#13738 <https://github.com/ceph/ceph/pull/13738>`_, Haomai Wang)
* core: msg/async/rdma: make Infiniband can be forkable (`pr#13525 <https://github.com/ceph/ceph/pull/13525>`_, Haomai Wang)
* core: msg/async/rdm: fix leak when existing failure in ip network (`pr#13435 <https://github.com/ceph/ceph/pull/13435>`_, Haomai Wang)
* core: msg/async: set thread name for msgr worker (`pr#13699 <https://github.com/ceph/ceph/pull/13699>`_, Haomai Wang)
* core: msg/async/Stack.cc: use of pthread_setname_np() needs compat.h (`pr#13825 <https://github.com/ceph/ceph/pull/13825>`_, Willem Jan Withagen)
* core: msg/async: support IPv6 QoS (`issue#18887 <http://tracker.ceph.com/issues/18887>`_, `issue#18928 <http://tracker.ceph.com/issues/18928>`_, `pr#13418 <https://github.com/ceph/ceph/pull/13418>`_, Robin H. Johnson)
* core: msg/simple: fix missing unlock when already bind (`pr#13267 <https://github.com/ceph/ceph/pull/13267>`_, Haomai Wang)
* core: msg/simple/Pipe:the returned value for do_recv unequal to zero (`pr#10272 <https://github.com/ceph/ceph/pull/10272>`_, zhang.zezhu)
* core: objclass: modify omap_get_{keys,vals} api (`pr#16667 <https://github.com/ceph/ceph/pull/16667>`_, Yehuda Sadeh, Casey Bodley)
* core: objclass-sdk: use namespace ceph for bufferlist (`pr#15581 <https://github.com/ceph/ceph/pull/15581>`_, Neha Ojha)
* core: os/bluestore: do not use nullptr to calc the size of bluestore_pextent_t (`pr#14030 <https://github.com/ceph/ceph/pull/14030>`_, Kefu Chai)
* core: os/bluestore rm unused variable in aio_read() (`pr#13530 <https://github.com/ceph/ceph/pull/13530>`_, tangwenjun)
* core: os/bluestore: silence gcc warning (`pr#14028 <https://github.com/ceph/ceph/pull/14028>`_, Kefu Chai)
* core: osdc: clean up osd_command/start_mon_command interfaces (`pr#13727 <https://github.com/ceph/ceph/pull/13727>`_, John Spray)
* core: osdc/Objecter: fix possible OSDSession leak on wrong connection (`pr#13365 <https://github.com/ceph/ceph/pull/13365>`_, xie xingguo)
* core: osdc/Objecter: resend pg commands on interval change (`issue#18358 <http://tracker.ceph.com/issues/18358>`_, `pr#12869 <https://github.com/ceph/ceph/pull/12869>`_, Samuel Just)
* core: osdc/Objecter: respect epoch barrier in _op_submit() (`issue#19396 <http://tracker.ceph.com/issues/19396>`_, `pr#14190 <https://github.com/ceph/ceph/pull/14190>`_, Ilya Dryomov)
* core: osd/: don't leak context for Blessed\*Context or RecoveryQueueAsync (`issue#18809 <http://tracker.ceph.com/issues/18809>`_, `pr#13342 <https://github.com/ceph/ceph/pull/13342>`_, Samuel Just)
* core: OSD: drop parameter t from merge_log() (`pr#13923 <https://github.com/ceph/ceph/pull/13923>`_, xie xingguo)
* core: osd/ECBackend: cleanup for unnecessary copy with pg_stat_t (`pr#12564 <https://github.com/ceph/ceph/pull/12564>`_, Yunchuan Wen)
* core: osd/ECBackend: drop duplicated pending_commit field from << operator (`pr#13665 <https://github.com/ceph/ceph/pull/13665>`_, xie xingguo)
* core: osd/ECBackend: only need check missing_loc when doing recovery (`pr#12526 <https://github.com/ceph/ceph/pull/12526>`_, huangjun)
* core:   osd/ECBackend: remove unused variable "ReadCB" (`pr#12543 <https://github.com/ceph/ceph/pull/12543>`_, huangjun)
* core: osd/ECTransaction: cleanup the redundant check which works in overwrite IO context (`pr#15765 <https://github.com/ceph/ceph/pull/15765>`_, tang.jin)
* core: osd/ECTransaction: only read partial stripes when below \*original\* object size (`issue#19882 <http://tracker.ceph.com/issues/19882>`_, `pr#15712 <https://github.com/ceph/ceph/pull/15712>`_, Sage Weil)
* core: osd/filestore: Revert "os/filestore: move ondisk in front (`issue#20524 <http://tracker.ceph.com/issues/20524>`_, `pr#16156 <https://github.com/ceph/ceph/pull/16156>`_, Kefu Chai)
* core: osd,librados: add manifest, redirect (`pr#15325 <https://github.com/ceph/ceph/pull/15325>`_, Sage Weil)
* core: osd,librados: cmpext support (`pr#14715 <https://github.com/ceph/ceph/pull/14715>`_, Zhengyong Wang, David Disseldorp, Mike Christie)
* core: osd,librados: remove clone_range and associated multi-object cruft (`pr#13008 <https://github.com/ceph/ceph/pull/13008>`_, Samuel Just)
* core: osd, messages/MOSDPing: bunch of fixes related to ping inflation (`pr#15727 <https://github.com/ceph/ceph/pull/15727>`_, Piotr Dałek)
* core: osd/mon/mds: fix `config set` tell command (`issue#20803 <http://tracker.ceph.com/issues/20803>`_, `pr#16700 <https://github.com/ceph/ceph/pull/16700>`_, John Spray)
* core: osd,mon: misc full fixes and cleanups (`pr#13968 <https://github.com/ceph/ceph/pull/13968>`_, David Zafman)
* core: osd/OpRequest: dump both name and addr for the client op (`pr#12691 <https://github.com/ceph/ceph/pull/12691>`_, runsisi)
* core: osd/OSD: bump up current version; conditionally encoding manifest into oi (`pr#15687 <https://github.com/ceph/ceph/pull/15687>`_, xie xingguo)
* core: osd/osd_internal_types: wake snaptrimmer on put_read lock, too (`issue#19131 <http://tracker.ceph.com/issues/19131>`_, `pr#13755 <https://github.com/ceph/ceph/pull/13755>`_, Sage Weil)
* core: osd/OSDMap: bump encoding version for require_min_compat_client (`pr#15046 <https://github.com/ceph/ceph/pull/15046>`_, "Yan, Zheng")
* core: osd/OSDMap: Change \*pg_to_\* to return void (`pr#15684 <https://github.com/ceph/ceph/pull/15684>`_, Brad Hubbard)
* core: osd/OSDMap: don't set weight to IN when OSD is destroyed (`issue#19119 <http://tracker.ceph.com/issues/19119>`_, `pr#13730 <https://github.com/ceph/ceph/pull/13730>`_, Ilya Dryomov)
* core: osd/OSDMap: hide require_osd and sortbitwise flags (`pr#14440 <https://github.com/ceph/ceph/pull/14440>`_, Sage Weil)
* core: osd/OSDMap: improve upmap calculation (`issue#19818 <http://tracker.ceph.com/issues/19818>`_, `pr#14902 <https://github.com/ceph/ceph/pull/14902>`_, Sage Weil)
* core: osd/OSDMap: Uncomment code to enable private default constructors (`pr#12597 <https://github.com/ceph/ceph/pull/12597>`_, Brad Hubbard)
* core: osd/OSD: tolerate any 'set-device-class' error on OSD startup (`pr#16812 <https://github.com/ceph/ceph/pull/16812>`_, xie xingguo)
* core: osd/osd_type: Fix logging output (`pr#12778 <https://github.com/ceph/ceph/pull/12778>`_, Brad Hubbard)
* core: osd/osd_types: Move comment to more relevant position (`pr#12779 <https://github.com/ceph/ceph/pull/12779>`_, Brad Hubbard)
* core: osd/osd_types: print notify-ack op properly (`pr#12585 <https://github.com/ceph/ceph/pull/12585>`_, Sage Weil)
* core: osd/PG: add new have_unfound() function in MissingLoc (`pr#12668 <https://github.com/ceph/ceph/pull/12668>`_, huangjun)
* core: osd/PG: Add two new mClock implementations of the PG sharded operator queue (`pr#14997 <https://github.com/ceph/ceph/pull/14997>`_, J. Eric Ivancich)
* core: osd/PG.cc: Optimistic estimation on PG.last_active (`pr#14799 <https://github.com/ceph/ceph/pull/14799>`_, Xiaoxi Chen)
* core: osd/PG.cc: unify the call of checking whether lock is held (`pr#15013 <https://github.com/ceph/ceph/pull/15013>`_, Jin Cai)
* core: osd/PG: check the connection first in fulfill_log (`pr#12579 <https://github.com/ceph/ceph/pull/12579>`_, huangjun)
* core: osd/PG: conditionally retry on receiving pg-notify when Primary is Incomplete (`pr#13942 <https://github.com/ceph/ceph/pull/13942>`_, xie xingguo)
* core: osd/PG: drop pre-firefly compat_mode for choose_acting (`pr#15057 <https://github.com/ceph/ceph/pull/15057>`_, Sage Weil)
* core: osd/PG: fix lost unfound + delete when there are no missing objects (`issue#20904 <http://tracker.ceph.com/issues/20904>`_, `pr#16809 <https://github.com/ceph/ceph/pull/16809>`_, Josh Durgin)
* core: osd/PG: fix possible overflow on unfound objects (`pr#12669 <https://github.com/ceph/ceph/pull/12669>`_, huangjun)
* core: osd/PG: fix warning so we discard_event() on a no-op state change (`pr#16655 <https://github.com/ceph/ceph/pull/16655>`_, Sage Weil)
* core: osd/PG: ignore CancelRecovery in NotRecovering (`issue#20804 <http://tracker.ceph.com/issues/20804>`_, `pr#16638 <https://github.com/ceph/ceph/pull/16638>`_, Sage Weil)
* core: osd/PGLog: avoid infinite loop if missing version is corrupted (`pr#16798 <https://github.com/ceph/ceph/pull/16798>`_, Josh Durgin)
* core: osd/PGLog: fix inaccurate missing assert (`issue#20753 <http://tracker.ceph.com/issues/20753>`_, `pr#16539 <https://github.com/ceph/ceph/pull/16539>`_, Josh Durgin)
* core: osd/PGLog: fix index for parent and child log on split (`issue#18975 <http://tracker.ceph.com/issues/18975>`_, `pr#13493 <https://github.com/ceph/ceph/pull/13493>`_, Sage Weil)
* core: osd/pglog: remove loop through empty collection (`pr#15121 <https://github.com/ceph/ceph/pull/15121>`_, J. Eric Ivancich)
* core: osd/PGLog: skip ERROR entires in _merge_object_divergent_entries (`issue#20843 <http://tracker.ceph.com/issues/20843>`_, `pr#16675 <https://github.com/ceph/ceph/pull/16675>`_, Jeegn Chen)
* core: osd/PG: make non-empty PastIntervals non-fatal (`issue#20167 <http://tracker.ceph.com/issues/20167>`_, `pr#15639 <https://github.com/ceph/ceph/pull/15639>`_, Sage Weil)
* core: osd/PG: only correct filestore collection bits on load (`issue#19541 <http://tracker.ceph.com/issues/19541>`_, `pr#14397 <https://github.com/ceph/ceph/pull/14397>`_, Sage Weil)
* core: osd/PG: publish PG stats when backfill-related states change (`issue#18369 <http://tracker.ceph.com/issues/18369>`_, `pr#12727 <https://github.com/ceph/ceph/pull/12727>`_, Sage Weil)
* core: osd/PG: reset the missing set when restarting backfill (`issue#19191 <http://tracker.ceph.com/issues/19191>`_, `pr#14053 <https://github.com/ceph/ceph/pull/14053>`_, Josh Durgin)
* core: osd/PG: restrict want_acting to up+acting on recovery completion (`issue#18929 <http://tracker.ceph.com/issues/18929>`_, `pr#13420 <https://github.com/ceph/ceph/pull/13420>`_, Sage Weil)
* core: osd/PG: set clean when last_epoch_clean is updated (`issue#19023 <http://tracker.ceph.com/issues/19023>`_, `pr#15555 <https://github.com/ceph/ceph/pull/15555>`_, Samuel Just)
* core: osd/PG: simplify the logic of backfill_targets checking (`pr#12519 <https://github.com/ceph/ceph/pull/12519>`_, huangjun)
* core: osd/PG: some minor cleanups (`pr#14133 <https://github.com/ceph/ceph/pull/14133>`_, runsisi)
* core: osd/PrimaryLogPG: clear oi from trim_object() (`issue#19947 <http://tracker.ceph.com/issues/19947>`_, `pr#15519 <https://github.com/ceph/ceph/pull/15519>`_, Sage Weil)
* core: osd/PrimaryLogPG: do not call on_shutdown() if (pg.deleting) (`issue#19902 <http://tracker.ceph.com/issues/19902>`_, `pr#15040 <https://github.com/ceph/ceph/pull/15040>`_, Kefu Chai)
* core: osd/PrimaryLogPG: do not expect FULL_TRY ops to get resent (`issue#19430 <http://tracker.ceph.com/issues/19430>`_, `pr#14255 <https://github.com/ceph/ceph/pull/14255>`_, Sage Weil)
* core: osd/PrimaryLogPG::failed_push: update missing as well (`issue#18165 <http://tracker.ceph.com/issues/18165>`_, `pr#12888 <https://github.com/ceph/ceph/pull/12888>`_, Samuel Just)
* core: osd/PrimaryLogPG: fix oi reset during trim_object (`issue#19947 <http://tracker.ceph.com/issues/19947>`_, `pr#15696 <https://github.com/ceph/ceph/pull/15696>`_, Sage Weil)
* core:   osd/PrimaryLogPG: fix recovering hang when have unfound objects (`pr#16558 <https://github.com/ceph/ceph/pull/16558>`_, huangjun)
* core: osd/PrimaryLogPG: optimal pick_newest_available (`pr#12695 <https://github.com/ceph/ceph/pull/12695>`_, huangjun)
* core: osd/PrimaryLogPG: record prior_version for DELETE events (`issue#20274 <http://tracker.ceph.com/issues/20274>`_, `pr#15649 <https://github.com/ceph/ceph/pull/15649>`_, Sage Weil)
* core: osd/PrimaryLogPG: remove duplicated code (`pr#13894 <https://github.com/ceph/ceph/pull/13894>`_, Jianpeng Ma)
* core: osd/PrimaryLogPG: set return value if sparse read failed (`pr#14093 <https://github.com/ceph/ceph/pull/14093>`_, huangjun)
* core: osd/PrimaryLogPG: skip deleted missing objects in pg[n]ls (`issue#20739 <http://tracker.ceph.com/issues/20739>`_, `pr#16490 <https://github.com/ceph/ceph/pull/16490>`_, Josh Durgin)
* core: osd/PrimaryLogPG solve cache tier osd high memory consumption (`issue#20464 <http://tracker.ceph.com/issues/20464>`_, `pr#16011 <https://github.com/ceph/ceph/pull/16011>`_, Peng Xie)
* core: osd/PrimaryLogPG::try_lock_for_read: give up if missing (`issue#18583 <http://tracker.ceph.com/issues/18583>`_, `pr#13087 <https://github.com/ceph/ceph/pull/13087>`_, Samuel Just)
* core: osd/PrimaryLogPG: unify the access to primary pg (`pr#12527 <https://github.com/ceph/ceph/pull/12527>`_, huangjun)
* core: osd/PrimayLogPG: update modified range to include the whole object size for write_full op (`pr#15021 <https://github.com/ceph/ceph/pull/15021>`_, runsisi)
* core: osd/ReplicatedBackend: clear pull source once we are done with it (`issue#19076 <http://tracker.ceph.com/issues/19076>`_, `pr#13879 <https://github.com/ceph/ceph/pull/13879>`_, Samuel Just)
* core: osd/ReplicatedBackend: remove MOSDSubOp cruft from repop_applied (`pr#14358 <https://github.com/ceph/ceph/pull/14358>`_, Jianpeng Ma)
* core: osd/ReplicatedBackend: reset thread heartbeat after every omap entry … (`issue#20375 <http://tracker.ceph.com/issues/20375>`_, `pr#15823 <https://github.com/ceph/ceph/pull/15823>`_, Josh Durgin)
* core: osd/ReplicatedBackend: take read locks for clone sources during recovery (`issue#17831 <http://tracker.ceph.com/issues/17831>`_, `pr#12844 <https://github.com/ceph/ceph/pull/12844>`_, Samuel Just)
* core: os/filestore: call committed_thru when no journal entries are replayed (`pr#15781 <https://github.com/ceph/ceph/pull/15781>`_, Kuan-Kai Chiu)
* core: os/filestore: debug which omap keys are set (`issue#19067 <http://tracker.ceph.com/issues/19067>`_, `pr#13671 <https://github.com/ceph/ceph/pull/13671>`_, Sage Weil)
* core: os/filestore: do not free event if not added (`pr#16235 <https://github.com/ceph/ceph/pull/16235>`_, Kefu Chai)
* core: os/filestore/FileJournal: bufferlist rebuild (`pr#13980 <https://github.com/ceph/ceph/pull/13980>`_, Jianpeng Ma)
* core: os/filestore/FileJournal: FileJournal::open() close journal file before return error (`issue#20504 <http://tracker.ceph.com/issues/20504>`_, `pr#16120 <https://github.com/ceph/ceph/pull/16120>`_, Yang Honggang)
* core: os/filestore/FileStore.cc: remove a redundant judgement when get max latency (`pr#15961 <https://github.com/ceph/ceph/pull/15961>`_, Jianpeng Ma)
* core: os/filestore/FileStore.cc: remove unneeded loop (`pr#12177 <https://github.com/ceph/ceph/pull/12177>`_, Li Wang)
* core: os/filestore: fix clang static check warn "use-after-free“ (`pr#12581 <https://github.com/ceph/ceph/pull/12581>`_, liuchang0812)
* core: os/filestore: fix infinit loops in fiemap() (`pr#14367 <https://github.com/ceph/ceph/pull/14367>`_, Ning Yao)
* core: os/filestore: handle error returned from write_fd() (`pr#10146 <https://github.com/ceph/ceph/pull/10146>`_, yonghengdexin735)
* core: os/filestore/HashIndex: be loud about splits (`issue#18235 <http://tracker.ceph.com/issues/18235>`_, `pr#12421 <https://github.com/ceph/ceph/pull/12421>`_, Dan van der Ster)
* core: os/filestore/JournalingObjectStore cleanup (`pr#12528 <https://github.com/ceph/ceph/pull/12528>`_, Li Wang)
* core: os/filestore: require experimental flag for btrfs (`pr#16086 <https://github.com/ceph/ceph/pull/16086>`_, Sage Weil)
* core: os/filestore: version will be uninitialized varible if store_version doesn't exist (`pr#12582 <https://github.com/ceph/ceph/pull/12582>`_, liuchang0812)
* core: os/fs/FS.cc: remove the redundant code (`pr#14362 <https://github.com/ceph/ceph/pull/14362>`_, Jianpeng Ma)
* core: os/FuseStore: include <functional> header in src/os/FuseStore.h for gcc 7.x (`pr#13454 <https://github.com/ceph/ceph/pull/13454>`_, Jos Collin)
* core,performance: common/config_opts: improve rdma buffer size to 128k (`pr#13510 <https://github.com/ceph/ceph/pull/13510>`_, Haomai Wang)
* core,performance: common/TrackedOp: various cleanups and optimizations (`pr#12537 <https://github.com/ceph/ceph/pull/12537>`_, Sage Weil)
* core,performance: kv/RocksDBStore: Table options for indexing and filtering (`pr#16450 <https://github.com/ceph/ceph/pull/16450>`_, Mark Nelson)
* core,performance: mon,osd: explicitly remap some pgs (`pr#13984 <https://github.com/ceph/ceph/pull/13984>`_, Sage Weil)
* core,performance: msg/async: avoid lossy connection sending ack message (`pr#13700 <https://github.com/ceph/ceph/pull/13700>`_, Haomai Wang)
* core,performance: msg/async/rdma: cleanup (`pr#13509 <https://github.com/ceph/ceph/pull/13509>`_, Haomai Wang)
* core,performance: msg/async/rdma: refactor tx handle flow to get rid of locks (`pr#13680 <https://github.com/ceph/ceph/pull/13680>`_, Haomai Wang)
* core,performance: msg/async: reduce write_lock contention (`pr#15092 <https://github.com/ceph/ceph/pull/15092>`_, Haomai Wang)
* core,performance: osd/ECBackend: Send write message to peers first, then do local write (`pr#12522 <https://github.com/ceph/ceph/pull/12522>`_, huangjun)
* core,performance: osd/OSD.h: requeue the scrub job with higher priority to shorten the blocking time of related requests (`pr#15552 <https://github.com/ceph/ceph/pull/15552>`_, Jin Cai)
* core,performance: osd, os: reduce fiemap burden (`pr#14640 <https://github.com/ceph/ceph/pull/14640>`_, Piotr Dałek)
* core,performance: osd/pg: bound the portion of the log we request in GetLog::GetLog() (`pr#12233 <https://github.com/ceph/ceph/pull/12233>`_, Jie Wang)
* core,performance: osd/PG: make prioritized recovery possible (`pr#13723 <https://github.com/ceph/ceph/pull/13723>`_, Piotr Dałek)
* core,performance: os/filestore: avoid unnecessary copy in filestore::_do_transaction (`pr#12578 <https://github.com/ceph/ceph/pull/12578>`_, Yunchuan Wen)
* core,performance: os/filestore/HashIndex: randomize split threshold by a configurable amount (`issue#15835 <http://tracker.ceph.com/issues/15835>`_, `pr#15689 <https://github.com/ceph/ceph/pull/15689>`_, Josh Durgin)
* core,performance: os/filestore: queue ondisk completion before apply work (`pr#13918 <https://github.com/ceph/ceph/pull/13918>`_, Pan Liu)
* core,performance: os/filestore: use new sleep strategy when io_submit gets EAGAIN (`pr#14860 <https://github.com/ceph/ceph/pull/14860>`_, Pan Liu)
* core,performance: os/kstore: Added rocksdb bloom filter settings (`pr#13053 <https://github.com/ceph/ceph/pull/13053>`_, Ted-Chang)
* core,performance: src/OSD: add more useful perf counters for performance tuning (`pr#15915 <https://github.com/ceph/ceph/pull/15915>`_, Pan Liu)
* core: PGLog: store extra duplicate ops beyond the normal log entries (`pr#16172 <https://github.com/ceph/ceph/pull/16172>`_, Josh Durgin, J. Eric Ivancich)
* core: Prefix /proc/ with FreeBSD emulation (`pr#14290 <https://github.com/ceph/ceph/pull/14290>`_, Willem Jan Withagen)
* core: PrimaryLogPG: don't update digests for objects with mismatched names (`issue#18409 <http://tracker.ceph.com/issues/18409>`_, `pr#12788 <https://github.com/ceph/ceph/pull/12788>`_, Samuel Just)
* core: print more information when run ceph-osd cmd with 'check options' (`pr#16678 <https://github.com/ceph/ceph/pull/16678>`_, mychoxin)
* core: qa: do not restrict valgrind runs to centos (`issue#18126 <http://tracker.ceph.com/issues/18126>`_, `pr#15389 <https://github.com/ceph/ceph/pull/15389>`_, Greg Farnum)
* core,rbd: mon,osd: do not create rbd pool by default (`pr#15894 <https://github.com/ceph/ceph/pull/15894>`_, Greg Farnum, Sage Weil, David Zafman)
* core: ReplicatedBackend: don't queue Context outside of ObjectStore with obc (`issue#18927 <http://tracker.ceph.com/issues/18927>`_, `pr#13569 <https://github.com/ceph/ceph/pull/13569>`_, Samuel Just)
* core: Revert "PrimaryLogPG::failed_push: update missing as well" (`issue#18624 <http://tracker.ceph.com/issues/18624>`_, `pr#13090 <https://github.com/ceph/ceph/pull/13090>`_, David Zafman)
* core,rgw: misc: SCA and Coverity Fixes (`pr#13208 <https://github.com/ceph/ceph/pull/13208>`_, Danny Al-Gaaf)
* core,rgw: qa: Removed all 'default_idle_timeout' due to chnage in rwg task (`pr#15420 <https://github.com/ceph/ceph/pull/15420>`_, Yuri Weinstein)
* core,rgw,tests: qa/rgw_snaps: move default_idle_timeout config under the client (`issue#20128 <http://tracker.ceph.com/issues/20128>`_, `pr#15400 <https://github.com/ceph/ceph/pull/15400>`_, Yehuda Sadeh)
* core,rgw,tests: qa/suits/rados/basic/tasks/rgw_snaps: wait for pools to be created (`pr#16509 <https://github.com/ceph/ceph/pull/16509>`_, Sage Weil)
* core: rocksdb: sync with upstream (`issue#18464 <http://tracker.ceph.com/issues/18464>`_, `pr#13306 <https://github.com/ceph/ceph/pull/13306>`_, Kefu Chai)
* core: src/ceph.in: Use env(CEPH_DEV) to suppress noise from ceph (`pr#14746 <https://github.com/ceph/ceph/pull/14746>`_, Willem Jan Withagen)
* core: src/vstart.sh: kill dead upmap option (`pr#15848 <https://github.com/ceph/ceph/pull/15848>`_, xie xingguo)
* core:" Stringify needs access to << before reference" src/include/stringify.h (`pr#16334 <https://github.com/ceph/ceph/pull/16334>`_, Willem Jan Withagen)
* core: test, osd: fix some coverity issues (`pr#13293 <https://github.com/ceph/ceph/pull/13293>`_, liuchang0812)
* core: test/pybind/test_rados.py: tolerate TimedOut in test_ping_monitor (`issue#18529 <http://tracker.ceph.com/issues/18529>`_, `pr#12934 <https://github.com/ceph/ceph/pull/12934>`_, Samuel Just)
* core,tests: ceph-disk: sensible default for block.db (`pr#15576 <https://github.com/ceph/ceph/pull/15576>`_, Loic Dachary)
* core,tests: ceph-disk/tests: Certain partition types do not work on FreeBSD (`pr#13560 <https://github.com/ceph/ceph/pull/13560>`_, Willem Jan Withagen)
* core,tests: ceph-disk/tests/test_main.py: FreeBSD does not do multipath (`pr#13847 <https://github.com/ceph/ceph/pull/13847>`_, Willem Jan Withagen)
* core,tests: ceph_test_librados_api_misc: fix stupid LibRadosMiscConnectFailure.ConnectFailure test (`issue#15368 <http://tracker.ceph.com/issues/15368>`_, `pr#14261 <https://github.com/ceph/ceph/pull/14261>`_, Sage Weil)
* core,tests: ceph_test_rados_api_misc: avoid livelock from PoolCreationRace (`pr#13565 <https://github.com/ceph/ceph/pull/13565>`_, Sage Weil)
* core,tests: ceph_test_rados_api_misc: Fix trivial memory leak (`pr#12680 <https://github.com/ceph/ceph/pull/12680>`_, Brad Hubbard)
* core,tests: ceph_test_rados_api: wait for snap trim on ENOENT during cleanup (`issue#19948 <http://tracker.ceph.com/issues/19948>`_, `pr#15638 <https://github.com/ceph/ceph/pull/15638>`_, Sage Weil)
* core,tests: ceph_test_rados_api_watch_notify: flush after unwatch (`issue#20105 <http://tracker.ceph.com/issues/20105>`_, `pr#16402 <https://github.com/ceph/ceph/pull/16402>`_, Sage Weil)
* core,tests: ceph_test_rados_api_watch_notify: make LibRadosWatchNotify.Watch3Timeout tolerate thrashing (`issue#19433 <http://tracker.ceph.com/issues/19433>`_, `pr#14899 <https://github.com/ceph/ceph/pull/14899>`_, Sage Weil)
* core,tests: ceph_test_rados: max_stride_size must be more than min_stride_size (`issue#20775 <http://tracker.ceph.com/issues/20775>`_, `pr#16590 <https://github.com/ceph/ceph/pull/16590>`_, Lianne Wang)
* core,tests: c_write_operations.cc: Fix trivial memory leak (`pr#12663 <https://github.com/ceph/ceph/pull/12663>`_, Brad Hubbard)
* core,tests: do all valgrind runs on centos (`issue#20360 <http://tracker.ceph.com/issues/20360>`_, `issue#18126 <http://tracker.ceph.com/issues/18126>`_, `pr#16046 <https://github.com/ceph/ceph/pull/16046>`_, Sage Weil)
* core,tests: os: allow 'osd objectstore = random' to pick either filestore or bluestore (`pr#13754 <https://github.com/ceph/ceph/pull/13754>`_, Sage Weil)
* core,tests: qa: avoid map-gap tests for k=2 m=1 (`issue#20844 <http://tracker.ceph.com/issues/20844>`_, `pr#16789 <https://github.com/ceph/ceph/pull/16789>`_, Sage Weil)
* core,tests: qa: move ceph-helpers-based make check tests to qa/standalone; run via teuthology (`pr#16513 <https://github.com/ceph/ceph/pull/16513>`_, Sage Weil)
* core,tests: qa/objectstore/filestore-btrfs: test btrfs on trusty only (`issue#20169 <http://tracker.ceph.com/issues/20169>`_, `pr#15814 <https://github.com/ceph/ceph/pull/15814>`_, Sage Weil)
* core,tests: qa/objectstore: test bluestore with aggressive compression (`pr#14623 <https://github.com/ceph/ceph/pull/14623>`_, Sage Weil)
* core,tests: qa/rados/upgrade/jewel-x-singleton: run luminous.yaml at the end (`pr#13378 <https://github.com/ceph/ceph/pull/13378>`_, Sage Weil)
* core,tests: qa: stop testing btrfs (`issue#20169 <http://tracker.ceph.com/issues/20169>`_, `pr#16044 <https://github.com/ceph/ceph/pull/16044>`_, Sage Weil)
* core,tests: qa/suites/powercycle/osd/tasks/radosbench: consume less space (`issue#20302 <http://tracker.ceph.com/issues/20302>`_, `pr#15821 <https://github.com/ceph/ceph/pull/15821>`_, Sage Weil)
* core,tests: qa/suites/rados: at-end: ignore PG_{AVAILABILITY,DEGRADED} (`issue#20693 <http://tracker.ceph.com/issues/20693>`_, `pr#16575 <https://github.com/ceph/ceph/pull/16575>`_, Sage Weil)
* core,tests: qa/suites/rados/\*/at-end: wait for healthy before scrubbing (`pr#15245 <https://github.com/ceph/ceph/pull/15245>`_, Sage Weil)
* core,tests: qa/suites/rados/basic: set low omap limit for rgw workload (`pr#13071 <https://github.com/ceph/ceph/pull/13071>`_, Sage Weil)
* core,tests: qa/suites/rados/basic/tasks/rados_python: POOL_APP_NOT_ENABLED (`pr#16827 <https://github.com/ceph/ceph/pull/16827>`_, Sage Weil)
* core,tests: qa/suites/rados/mgr/tasks/failover: whitelist (`pr#16795 <https://github.com/ceph/ceph/pull/16795>`_, Sage Weil)
* core,tests: qa/suites/rados/singleton/all/reg11184: whitelist health warnings (`pr#16306 <https://github.com/ceph/ceph/pull/16306>`_, Sage Weil)
* core,tests: qa/suites/rados/singleton-nomsg/health-warnings: behave on ext4 (`issue#20043 <http://tracker.ceph.com/issues/20043>`_, `pr#15207 <https://github.com/ceph/ceph/pull/15207>`_, Sage Weil)
* core,tests: qa/suites/rados: temporarily remove scrub_test from basic/ until post-luminous (`issue#19935 <http://tracker.ceph.com/issues/19935>`_, `pr#15202 <https://github.com/ceph/ceph/pull/15202>`_, Sage Weil)
* core,tests: qa/suites/rados/thrash/workload/\*: enable rados.py cache tiering ops (`issue#11793 <http://tracker.ceph.com/issues/11793>`_, `pr#16244 <https://github.com/ceph/ceph/pull/16244>`_, Sage Weil)
* core,tests: qa/suites/upgrade/kraken-x: enable experimental for bluestore (`pr#15359 <https://github.com/ceph/ceph/pull/15359>`_, Sage Weil)
* core,tests: qa/tasks/ceph: enable rbd on rbd pool (`pr#16794 <https://github.com/ceph/ceph/pull/16794>`_, Sage Weil)
* core,tests: qa/tasks/ceph_manager: get osds all in after thrashing (`pr#15784 <https://github.com/ceph/ceph/pull/15784>`_, Sage Weil)
* core,tests: qa/tasks/ceph_manager: wait for osd to start after objectstore-tool sequence (`issue#20705 <http://tracker.ceph.com/issues/20705>`_, `pr#16454 <https://github.com/ceph/ceph/pull/16454>`_, Sage Weil)
* core,tests: qa/tasks/ceph_manager: wait longer for pg stats to flush (`pr#16322 <https://github.com/ceph/ceph/pull/16322>`_, Sage Weil)
* core,tests: qa/tasks/ceph: osd_scrub_pgs: reissue scrub requests in loop (`issue#20326 <http://tracker.ceph.com/issues/20326>`_, `pr#15747 <https://github.com/ceph/ceph/pull/15747>`_, Sage Weil)
* core,tests: qa/tasks/ceph.py: no osd id to 'osd create' command (`issue#20548 <http://tracker.ceph.com/issues/20548>`_, `pr#16233 <https://github.com/ceph/ceph/pull/16233>`_, Sage Weil)
* core,tests: qa/tasks/ceph.py: tolerate active+clean+something (`pr#15717 <https://github.com/ceph/ceph/pull/15717>`_, Sage Weil)
* core,tests: qa/tasks/ceph: simplify ceph deployment slightly (`pr#15853 <https://github.com/ceph/ceph/pull/15853>`_, Sage Weil)
* core,tests: qa/tasks/ceph: wait for mgr to activate and pg stats to flush in health() (`issue#20744 <http://tracker.ceph.com/issues/20744>`_, `pr#16514 <https://github.com/ceph/ceph/pull/16514>`_, Sage Weil)
* core,tests: qa/tasks/dump_stuck: fix dump_stuck test bug (`pr#16559 <https://github.com/ceph/ceph/pull/16559>`_, huangjun)
* core,tests: qa/tasks/dump_stuck: fix for active+clean+remapped (`issue#20431 <http://tracker.ceph.com/issues/20431>`_, `pr#15955 <https://github.com/ceph/ceph/pull/15955>`_, Sage Weil)
* core,tests: qa/tasks/radosbench: longer timeout (`pr#16213 <https://github.com/ceph/ceph/pull/16213>`_, Sage Weil)
* core,tests: qa/workunits/cephtool/test.sh: add sudo for daemon compact (`pr#16500 <https://github.com/ceph/ceph/pull/16500>`_, Sage Weil)
* core,tests: qa/workunits/cephtool/test.sh: fix osd full health detail grep (`issue#20187 <http://tracker.ceph.com/issues/20187>`_, `pr#15494 <https://github.com/ceph/ceph/pull/15494>`_, Sage Weil)
* core,tests: qa/workunits/rados/test_health_warning: misc fixes (`issue#19990 <http://tracker.ceph.com/issues/19990>`_, `pr#15201 <https://github.com/ceph/ceph/pull/15201>`_, Sage Weil)
* core,tests: qa/workunits/rest: use unique pool names for cephfs test (`pr#13188 <https://github.com/ceph/ceph/pull/13188>`_, Sage Weil)
* core,tests: Revert "qa: do not restrict valgrind runs to centos" (`issue#20360 <http://tracker.ceph.com/issues/20360>`_, `pr#15791 <https://github.com/ceph/ceph/pull/15791>`_, Sage Weil)
* core,tests: test: add separate ceph-helpers-based smoke test (`pr#16572 <https://github.com/ceph/ceph/pull/16572>`_, Sage Weil)
* core,tests: test/librados/cmd.cc: Fix trivial memory leaks (`pr#12671 <https://github.com/ceph/ceph/pull/12671>`_, Brad Hubbard)
* core,tests: test/librados/c_read_operations.cc: Fix trivial memory leak (`pr#12656 <https://github.com/ceph/ceph/pull/12656>`_, Brad Hubbard)
* core,tests: test/librados/c_read_operations.cc: Fix valgrind errors (`issue#18354 <http://tracker.ceph.com/issues/18354>`_, `pr#12657 <https://github.com/ceph/ceph/pull/12657>`_, Brad Hubbard)
* core,tests: test/librados: Silence Coverity memory leak warnings (`pr#12442 <https://github.com/ceph/ceph/pull/12442>`_, Brad Hubbard, Samuel Just)
* core,tests: test/librados/snapshots.cc: Fix memory leak (`pr#12690 <https://github.com/ceph/ceph/pull/12690>`_, Brad Hubbard)
* core,tests: test/librados/tier.cc: Fix valgrind errors (`issue#18360 <http://tracker.ceph.com/issues/18360>`_, `pr#12705 <https://github.com/ceph/ceph/pull/12705>`_, Brad Hubbard)
* core,tests: test/osd/TestRados.cc: run set-redirect test after finishing setup (`issue#20114 <http://tracker.ceph.com/issues/20114>`_, `pr#15385 <https://github.com/ceph/ceph/pull/15385>`_, Myoungwon Oh)
* core,tests: test_rados_watch_notify: Fix trivial memory leaks (`pr#12713 <https://github.com/ceph/ceph/pull/12713>`_, Brad Hubbard)
* core,tests,tools: Fixes: http://tracker.ceph.com/issues/18533 (`pr#13423 <https://github.com/ceph/ceph/pull/13423>`_, Samuel Just, David Zafman)
* core,tests: upgrade/jewel-x: a few fixes (`pr#16830 <https://github.com/ceph/ceph/pull/16830>`_, Sage Weil)
* core: throttle: Minimal destructor fix for Luminous (`pr#16661 <https://github.com/ceph/ceph/pull/16661>`_, Adam C. Emerson)
* core,tools: ceph: perfcounter priorities and daemonperf updates to use them (`pr#14793 <https://github.com/ceph/ceph/pull/14793>`_, Sage Weil, Dan Mick)
* core,tools: kv: move 'bluestore-kv' hackery out of KeyValueDB into ceph-kvstore-tool (`issue#19778 <http://tracker.ceph.com/issues/19778>`_, `pr#14895 <https://github.com/ceph/ceph/pull/14895>`_, Sage Weil)
* core,tools: osdmaptool: require --upmap-save before modifying input osdmap (`pr#15247 <https://github.com/ceph/ceph/pull/15247>`_, Sage Weil)
* core: vstart.sh: start mgr after mon, before osds (`pr#16613 <https://github.com/ceph/ceph/pull/16613>`_, Sage Weil)
* core: Wip 20985 divergent handling luminous (`issue#20985 <http://tracker.ceph.com/issues/20985>`_, `pr#17001 <https://github.com/ceph/ceph/pull/17001>`_, Greg Farnum)
* create the ceph-volume and ceph-volume-systemd man pages (`pr#17158 <https://github.com/ceph/ceph/pull/17158>`_, Alfredo Deza)
* crush: a couple of weight-set fixes (`pr#16623 <https://github.com/ceph/ceph/pull/16623>`_, xie xingguo)
* crush: add devices class that rules can use as a filter (`issue#18943 <http://tracker.ceph.com/issues/18943>`_, `pr#13444 <https://github.com/ceph/ceph/pull/13444>`_, Loic Dachary)
* crush: add --dump to crushtool (`pr#13726 <https://github.com/ceph/ceph/pull/13726>`_, Loic Dachary)
* crush: add missing tunable in tests (`pr#15412 <https://github.com/ceph/ceph/pull/15412>`_, Loic Dachary)
* crush: allow uniform buckets with no items (`pr#13521 <https://github.com/ceph/ceph/pull/13521>`_, Loic Dachary)
* crush: API documentation (`pr#13205 <https://github.com/ceph/ceph/pull/13205>`_, Loic Dachary)
* crush: bucket: crush_add_uniform_bucket_item should check for uniformity (`pr#14208 <https://github.com/ceph/ceph/pull/14208>`_, Sahid Orentino Ferdjaoui)
* crush: builder: clean the arguments of crush_reweight\* methods (`pr#14110 <https://github.com/ceph/ceph/pull/14110>`_, Sahid Orentino Ferdjaoui)
* crush: builder: creating crush map with optimal configurations (`pr#14209 <https://github.com/ceph/ceph/pull/14209>`_, Sahid Orentino Ferdjaoui)
* crush: builder: legacy has chooseleaf_stable = 0 (`pr#14695 <https://github.com/ceph/ceph/pull/14695>`_, Loic Dachary)
* crush: crush_init_workspace starts with struct crush_work (`pr#14696 <https://github.com/ceph/ceph/pull/14696>`_, Loic Dachary)
* crush: detect and (usually) fix ruleset != rule id (`pr#13683 <https://github.com/ceph/ceph/pull/13683>`_, Sage Weil)
* crush: document tunables and rule step set (`pr#13722 <https://github.com/ceph/ceph/pull/13722>`_, Loic Dachary)
* crush: do is_out test only if we do not collide (`pr#13326 <https://github.com/ceph/ceph/pull/13326>`_, xie xingguo)
* crush: encode can override weights with weight set (`issue#19836 <http://tracker.ceph.com/issues/19836>`_, `pr#15002 <https://github.com/ceph/ceph/pull/15002>`_, Loic Dachary)
* crush: enforce buckets-before-rules rule (`pr#16453 <https://github.com/ceph/ceph/pull/16453>`_, Sage Weil)
* crush: fix CrushCompiler won't compile maps with empty shadow tree (`pr#17228 <https://github.com/ceph/ceph/pull/17228>`_, xie xingguo)
* crush: fix dprintk compilation (`pr#13424 <https://github.com/ceph/ceph/pull/13424>`_, Loic Dachary)
* crush: force rebuilding shadow hierarchy after swapping buckets (`pr#17229 <https://github.com/ceph/ceph/pull/17229>`_, xie xingguo)
* crush: misc changes/fixes for device classes (`issue#20845 <http://tracker.ceph.com/issues/20845>`_, `pr#16805 <https://github.com/ceph/ceph/pull/16805>`_, Kefu Chai, xie xingguo, Sage Weil)
* crush: more class fixes (`pr#16837 <https://github.com/ceph/ceph/pull/16837>`_, xie xingguo)
* crush: only encode class info if SERVER_LUMINOUS (`issue#19361 <http://tracker.ceph.com/issues/19361>`_, `pr#14131 <https://github.com/ceph/ceph/pull/14131>`_, Sage Weil)
* crush: optimize header file dependency (`pr#9307 <https://github.com/ceph/ceph/pull/9307>`_, Xiaowei Chen)
* crush: silence warning from -Woverflow (`pr#16329 <https://github.com/ceph/ceph/pull/16329>`_, Jos Collin)
* crush: s/ruleset/id/ in decompiled output; prevent compilation when ruleset != id (`pr#16400 <https://github.com/ceph/ceph/pull/16400>`_, Sage Weil)
* crush: update choose_args when items are added/removed (`pr#15311 <https://github.com/ceph/ceph/pull/15311>`_, Loic Dachary)
* crush: update documentation for negative choose step (`pr#14970 <https://github.com/ceph/ceph/pull/14970>`_, Loic Dachary)
* crush: various weight-set fixes (`pr#17214 <https://github.com/ceph/ceph/pull/17214>`_, xie xingguo)
* crush: verify weights is influenced by the number of replicas (`issue#15653 <http://tracker.ceph.com/issues/15653>`_, `pr#13083 <https://github.com/ceph/ceph/pull/13083>`_, Adam C. Emerson, Loic Dachary)
* crush: weight_set and id remapping (`issue#15653 <http://tracker.ceph.com/issues/15653>`_, `pr#14486 <https://github.com/ceph/ceph/pull/14486>`_, Loic Dachary)
* crush:  when osd_location_hook does not exist, we should exit error (`pr#12961 <https://github.com/ceph/ceph/pull/12961>`_, song baisen)
* doc: 12.1.0/release notes 2 (`pr#15627 <https://github.com/ceph/ceph/pull/15627>`_, Abhishek Lekshmanan)
* doc: 12.1.1 & 12.1.2 release notes (`pr#16377 <https://github.com/ceph/ceph/pull/16377>`_, Abhishek Lekshmanan)
* doc: add 0.94.10 and hammer EOL to releases.rst (`pr#13069 <https://github.com/ceph/ceph/pull/13069>`_, Nathan Cutler)
* doc: add 12.0.1 release notes (`pr#14106 <https://github.com/ceph/ceph/pull/14106>`_, Abhishek Lekshmanan)
* doc: Add amitkumar50 affiliation to .organizationmap (`pr#16475 <https://github.com/ceph/ceph/pull/16475>`_, Amit Kumar)
* doc add ceph-volume and ceph-volume-systemd man pages to CMakeLists file (`pr#17170 <https://github.com/ceph/ceph/pull/17170>`_, Alfredo Deza)
* doc: add changelog for v0.94.10 (`pr#13572 <https://github.com/ceph/ceph/pull/13572>`_, Abhishek Lekshmanan)
* doc: add changelog for v10.2.6 Jewel release (`pr#13839 <https://github.com/ceph/ceph/pull/13839>`_, Abhishek Lekshmanan)
* doc: add changelog for v10.2.7 (`pr#14441 <https://github.com/ceph/ceph/pull/14441>`_, Abhishek Lekshmanan)
* doc: add descriptions for mon/mgr options (`pr#15032 <https://github.com/ceph/ceph/pull/15032>`_, Kefu Chai)
* doc: add doc requirements on PR submitters (`pr#16394 <https://github.com/ceph/ceph/pull/16394>`_, John Spray)
* doc: added mgr caps to manual deployment documentation (`pr#16660 <https://github.com/ceph/ceph/pull/16660>`_, Nick Erdmann)
* doc: add FreeBSD manual install (`pr#14941 <https://github.com/ceph/ceph/pull/14941>`_, Willem Jan Withagen)
* doc: add instructions for replacing an OSD (`pr#16314 <https://github.com/ceph/ceph/pull/16314>`_, Kefu Chai)
* doc: add new cn ceph mirror to doc and mirroring (`pr#15089 <https://github.com/ceph/ceph/pull/15089>`_, Shengjing Zhu)
* doc: add optional argument for build-doc (`pr#14058 <https://github.com/ceph/ceph/pull/14058>`_, Kefu Chai)
* doc: add rados xattr commands to manpage (`pr#15362 <https://github.com/ceph/ceph/pull/15362>`_, Andreas Gerstmayr)
* doc: add rbd new trash cli and cleanups  in release-notes.rst (`issue#20702 <http://tracker.ceph.com/issues/20702>`_, `pr#16498 <https://github.com/ceph/ceph/pull/16498>`_, songweibin)
* doc: add README to dmclock subdir to inform developers it's a git subtree (`pr#15386 <https://github.com/ceph/ceph/pull/15386>`_, J. Eric Ivancich)
* doc: add RGW ldap auth documentation (`pr#14339 <https://github.com/ceph/ceph/pull/14339>`_, Harald Klein)
* doc: add some undocumented options to rbd-nbd (`pr#14134 <https://github.com/ceph/ceph/pull/14134>`_, wangzhengyong)
* doc: add verbiage to rbdmap manpage (`issue#18262 <http://tracker.ceph.com/issues/18262>`_, `pr#12509 <https://github.com/ceph/ceph/pull/12509>`_, Nathan Cutler)
* doc: Add Zabbix ceph-mgr plugin to PendingReleaseNotes (`pr#16412 <https://github.com/ceph/ceph/pull/16412>`_, Wido den Hollander)
* doc: AUTHORS: update CephFS PTL (`pr#16399 <https://github.com/ceph/ceph/pull/16399>`_, Patrick Donnelly)
* doc: AUTHORS: update tech leads (`pr#14350 <https://github.com/ceph/ceph/pull/14350>`_, Patrick Donnelly)
* doc: AUTHORS: update with release manager, backport team (`pr#15391 <https://github.com/ceph/ceph/pull/15391>`_, Sage Weil)
* doc: build/install-deps.sh: Add sphinx package for building docs on FreeBSD (`pr#13223 <https://github.com/ceph/ceph/pull/13223>`_, Willem Jan Withagen)
* doc: ceph-disk: use '-' for feeding ceph cli with stdin (`pr#16362 <https://github.com/ceph/ceph/pull/16362>`_, Kefu Chai)
* doc: change osd_op_thread_timeout default value to 15 (`pr#14199 <https://github.com/ceph/ceph/pull/14199>`_, Andreas Gerstmayr)
* doc: Change the default values of some OSD options (`issue#20199 <http://tracker.ceph.com/issues/20199>`_, `pr#15566 <https://github.com/ceph/ceph/pull/15566>`_, Bara Ancincova)
* doc: clarify "ceph quorum" syntax (`issue#17802 <http://tracker.ceph.com/issues/17802>`_, `pr#11787 <https://github.com/ceph/ceph/pull/11787>`_, Nathan Cutler)
* doc: clarify SubmittingPatches.rst (`pr#12988 <https://github.com/ceph/ceph/pull/12988>`_, Nathan Cutler)
* doc: clarify that "ms bind ipv6" disables IPv4 (`pr#13317 <https://github.com/ceph/ceph/pull/13317>`_, Ken Dreyer)
* doc: clarify the path restriction mds cap example (`pr#12993 <https://github.com/ceph/ceph/pull/12993>`_, John Spray)
* doc: common/options.cc: document bluestore config options (`pr#16489 <https://github.com/ceph/ceph/pull/16489>`_, Sage Weil)
* doc: correct and improve add user capability section (`pr#14055 <https://github.com/ceph/ceph/pull/14055>`_, Chu, Hua-Rong)
* doc: correct arguments for ceph tell osd.N bench (`pr#14462 <https://github.com/ceph/ceph/pull/14462>`_, Patrick Dinnen)
* doc: Correcting the remove bucket example and adding bucket link/unlink examples (`pr#12460 <https://github.com/ceph/ceph/pull/12460>`_, Uday Mullangi)
* doc: correct S3 lifecycle support explain (`issue#18459 <http://tracker.ceph.com/issues/18459>`_, `pr#12827 <https://github.com/ceph/ceph/pull/12827>`_, liuchang0812)
* doc: correct the quota section (`issue#19397 <http://tracker.ceph.com/issues/19397>`_, `pr#14122 <https://github.com/ceph/ceph/pull/14122>`_, Chu, Hua-Rong)
* doc: crush: API documentation fixes (`pr#13589 <https://github.com/ceph/ceph/pull/13589>`_, Loic Dachary)
* doc: crush typo in algorithm description (`pr#13661 <https://github.com/ceph/ceph/pull/13661>`_, Loic Dachary)
* doc: deletes duplicated word and clarifies an example (`pr#13746 <https://github.com/ceph/ceph/pull/13746>`_, Tahia Khan)
* doc: describe CephFS max_file_size (`pr#15287 <https://github.com/ceph/ceph/pull/15287>`_, Ken Dreyer)
* doc: describe mark_events logging available via the OSD's OpTracker (`pr#15095 <https://github.com/ceph/ceph/pull/15095>`_, Greg Farnum)
* doc: Describe mClock's use within Ceph in great detail (`pr#16707 <https://github.com/ceph/ceph/pull/16707>`_, J. Eric Ivancich)
* doc: dev add a note about ccache (`pr#14478 <https://github.com/ceph/ceph/pull/14478>`_, Abhishek Lekshmanan)
* doc: dev: add notes on PR make check validation test (`pr#16079 <https://github.com/ceph/ceph/pull/16079>`_, Nathan Cutler)
* doc: dev guide: how to run s3-tests locally against vstart (`pr#14508 <https://github.com/ceph/ceph/pull/14508>`_, Nathan Cutler, Abhishek Lekshmanan)
* doc: dev improve the s3tests doc to reflect current scripts (`pr#15180 <https://github.com/ceph/ceph/pull/15180>`_, Abhishek Lekshmanan)
* doc: doc/cephfs: mention RADOS object size limit (`pr#15550 <https://github.com/ceph/ceph/pull/15550>`_, John Spray)
* doc: doc/dev: update log_based_pg.rst, fix some display problem (`pr#12730 <https://github.com/ceph/ceph/pull/12730>`_, liuchang0812)
* doc: Doc:Fixes Python Swift client commands (`issue#17746 <http://tracker.ceph.com/issues/17746>`_, `pr#12887 <https://github.com/ceph/ceph/pull/12887>`_, Ronak Jain)
* doc: doc/install/manual-deployment: update osd creation steps (`pr#16573 <https://github.com/ceph/ceph/pull/16573>`_, Sage Weil)
* doc: doc/mgr/dashboard: update dashboard docs to reflect new defaults (`pr#16241 <https://github.com/ceph/ceph/pull/16241>`_, Sage Weil)
* doc: doc/mon: fix ceph-authtool command in rebuild mon's sample (`pr#16503 <https://github.com/ceph/ceph/pull/16503>`_, huanwen ren)
* doc: doc/qa: cover `config help` command (`pr#16727 <https://github.com/ceph/ceph/pull/16727>`_, John Spray)
* doc: doc/rados.8: add offset option for put command (`pr#16155 <https://github.com/ceph/ceph/pull/16155>`_, Jianpeng Ma)
* doc: doc/rados: add page for health checks and update monitoring.rst (`pr#16566 <https://github.com/ceph/ceph/pull/16566>`_, John Spray)
* doc: doc/rados/configuration: document bluestore (`pr#16765 <https://github.com/ceph/ceph/pull/16765>`_, Sage Weil)
* doc: doc/radosgw/s3/cpp.rst: update usage of libs3 APIs to make the examples work (`pr#10851 <https://github.com/ceph/ceph/pull/10851>`_, Weibing Zhang)
* doc: doc/rados/operations/health-checks: osd section (`pr#16611 <https://github.com/ceph/ceph/pull/16611>`_, Sage Weil)
* doc: doc/release-notes: add Images creation timestamp note (`pr#15963 <https://github.com/ceph/ceph/pull/15963>`_, clove)
* doc: doc/release-notes: avoid 'production-ready' in describing kraken (`pr#13675 <https://github.com/ceph/ceph/pull/13675>`_, Sage Weil)
* doc: doc/release-notes: final kraken notes (`pr#12968 <https://github.com/ceph/ceph/pull/12968>`_, Sage Weil)
* doc: doc/release-notes: fix bluestore links (`pr#16787 <https://github.com/ceph/ceph/pull/16787>`_, Sage Weil)
* doc: doc/release-notes: fix links, formatting; add crush device class docs (`pr#16741 <https://github.com/ceph/ceph/pull/16741>`_, Sage Weil)
* doc: doc/release-notes: fix upmap and osd replacement links; add fixme (`pr#16730 <https://github.com/ceph/ceph/pull/16730>`_, Sage Weil)
* doc: doc/release-notes: Luminous release notes typo fixes  "ceph config-key ls"->"ceph config-key list" (`pr#16330 <https://github.com/ceph/ceph/pull/16330>`_, scienceluo)
* doc: doc/release-notes: Luminous release notes typo fixes (`pr#16338 <https://github.com/ceph/ceph/pull/16338>`_, Luo Kexue)
* doc: doc/release-notes: sort release note changes into the right section (`pr#16764 <https://github.com/ceph/ceph/pull/16764>`_, Sage Weil)
* doc: doc/release-notes: update device class cli (`pr#16851 <https://github.com/ceph/ceph/pull/16851>`_, xie xingguo)
* doc: doc/release-notes: update luminous notes (`pr#15851 <https://github.com/ceph/ceph/pull/15851>`_, Sage Weil)
* doc: doc/release-notes: update which jewel version does sortbitwise warning (`pr#15209 <https://github.com/ceph/ceph/pull/15209>`_, Sage Weil)
* doc: doc/releases: Update releases from Feb 2017 to July 2017 (`pr#16303 <https://github.com/ceph/ceph/pull/16303>`_, Bryan Stillwell)
* doc: doc/rgw: instructions for changing multisite master zone (`pr#14089 <https://github.com/ceph/ceph/pull/14089>`_, Casey Bodley)
* doc: doc/rgw: remove fastcgi page and sample configs (`pr#15133 <https://github.com/ceph/ceph/pull/15133>`_, Casey Bodley)
* doc: doc/rgw: remove Federated Configuration, clean up multisite (`issue#19504 <http://tracker.ceph.com/issues/19504>`_, `issue#18082 <http://tracker.ceph.com/issues/18082>`_, `pr#15132 <https://github.com/ceph/ceph/pull/15132>`_, Casey Bodley)
* doc: docs: Clarify the relationship of min_size to EC pool recovery (`pr#14419 <https://github.com/ceph/ceph/pull/14419>`_, Brad Hubbard)
* doc: docs: Fix problems with example code (`pr#14007 <https://github.com/ceph/ceph/pull/14007>`_, Brad Hubbard)
* doc: docs: mgr dashboard (`pr#15920 <https://github.com/ceph/ceph/pull/15920>`_, Wido den Hollander)
* doc: [docs/quick-start]: update quick start to add a note for mgr create command for luminous+ builds (`pr#16350 <https://github.com/ceph/ceph/pull/16350>`_, Vasu Kulkarni)
* doc: Documentation Fixes for http://tracker.ceph.com/issues/19879 (`issue#20057 <http://tracker.ceph.com/issues/20057>`_, `issue#19879 <http://tracker.ceph.com/issues/19879>`_, `pr#15606 <https://github.com/ceph/ceph/pull/15606>`_, Sameer Tiwari)
* doc: Documentation updates for July 2017 releases (`pr#16401 <https://github.com/ceph/ceph/pull/16401>`_, Bryan Stillwell)
* doc: document bluestore compression settings (`pr#16747 <https://github.com/ceph/ceph/pull/16747>`_, Kefu Chai)
* doc: document mClock related options (`pr#16552 <https://github.com/ceph/ceph/pull/16552>`_, Kefu Chai)
* doc: document `osd-agent-{max,low}-ops` options (`pr#13648 <https://github.com/ceph/ceph/pull/13648>`_, Kefu Chai)
* doc: document perf historgrams (`pr#15150 <https://github.com/ceph/ceph/pull/15150>`_, Piotr Dałek)
* doc: document "rados cleanup" in rados manpage (`issue#20894 <http://tracker.ceph.com/issues/20894>`_, `pr#16777 <https://github.com/ceph/ceph/pull/16777>`_, Nathan Cutler)
* doc: document repair/scrub features (`issue#15786 <http://tracker.ceph.com/issues/15786>`_, `pr#9032 <https://github.com/ceph/ceph/pull/9032>`_, Kefu Chai, David Zafman)
* doc: Document RGW quota cache options (`issue#18747 <http://tracker.ceph.com/issues/18747>`_, `pr#13395 <https://github.com/ceph/ceph/pull/13395>`_, Daniel Gryniewicz)
* doc: Document that osd_heartbeat_grace applies to MON and OSD (`pr#13098 <https://github.com/ceph/ceph/pull/13098>`_, Wido den Hollander)
* doc: document the setup of restful and dashboard plugins (`issue#20239 <http://tracker.ceph.com/issues/20239>`_, `pr#15707 <https://github.com/ceph/ceph/pull/15707>`_, Kefu Chai)
* doc: explain about logging levels (`pr#12920 <https://github.com/ceph/ceph/pull/12920>`_, liuchang0812)
* doc: fio: update README.md so only the fio ceph engine is built (`pr#15081 <https://github.com/ceph/ceph/pull/15081>`_, Kefu Chai)
* doc: fix a typo (`pr#13930 <https://github.com/ceph/ceph/pull/13930>`_, Drunkard Zhang)
* doc: fix broken link in erasure-code.rst (`issue#19972 <http://tracker.ceph.com/issues/19972>`_, `pr#15143 <https://github.com/ceph/ceph/pull/15143>`_, MinSheng Lin)
* doc: fix document about rados mon (`pr#12662 <https://github.com/ceph/ceph/pull/12662>`_, liuchang0812)
* doc: Fixed a typo in yum repo filename script (`pr#16431 <https://github.com/ceph/ceph/pull/16431>`_, Jeff Green)
* doc: fixes a broken hyperlink to RADOS paper in architecture (`pr#13682 <https://github.com/ceph/ceph/pull/13682>`_, Tahia Khan)
* doc: Fixes a typo (`pr#13985 <https://github.com/ceph/ceph/pull/13985>`_, Edwin F. Boza)
* doc: Fixes parameter name in rbd configuration on openstack havana/icehouse (`issue#17978 <http://tracker.ceph.com/issues/17978>`_, `pr#13403 <https://github.com/ceph/ceph/pull/13403>`_, Michael Eischer)
* doc: Fixes radosgw-admin ex: in swift auth section (`issue#16687 <http://tracker.ceph.com/issues/16687>`_, `pr#12646 <https://github.com/ceph/ceph/pull/12646>`_, SirishaGuduru)
* doc:  fixes to silence sphinx-build (`pr#13997 <https://github.com/ceph/ceph/pull/13997>`_, Kefu Chai)
* doc: fix factual inaccuracy in doc/architecture.rst (`pr#15235 <https://github.com/ceph/ceph/pull/15235>`_, Nathan Cutler, Sage Weil)
* doc: fixing an error in 12.0.3 release notes (`pr#15195 <https://github.com/ceph/ceph/pull/15195>`_, Abhishek Lekshmanan)
* doc: fix link for ceph-mgr cephx authorization (`pr#16246 <https://github.com/ceph/ceph/pull/16246>`_, Greg Farnum)
* doc: fix link that pointed to a nonexistent file (`pr#14740 <https://github.com/ceph/ceph/pull/14740>`_, Peter Maloney)
* doc: fix syntax on code snippets in cephfs/multimds (`pr#15499 <https://github.com/ceph/ceph/pull/15499>`_, John Spray)
* doc: fix the librados c api can not compile problem (`pr#9396 <https://github.com/ceph/ceph/pull/9396>`_, song baisen)
* doc: fix the links to http://ceph.com/docs (`issue#19090 <http://tracker.ceph.com/issues/19090>`_, `pr#13976 <https://github.com/ceph/ceph/pull/13976>`_, Kefu Chai)
* doc: Fix typo and grammar in RGW config reference (`pr#13356 <https://github.com/ceph/ceph/pull/13356>`_, Ruben Kerkhof)
* doc: fix typo in config.rst (`pr#16721 <https://github.com/ceph/ceph/pull/16721>`_, Jos Collin)
* doc: fix typos in config.rst (`pr#16681 <https://github.com/ceph/ceph/pull/16681>`_, Song Shun)
* doc: fix typos in radosgw-admin usage (`pr#13936 <https://github.com/ceph/ceph/pull/13936>`_, Enming Zhang)
* doc: freshen mgr docs (`pr#15690 <https://github.com/ceph/ceph/pull/15690>`_, John Spray)
* doc: hammer 0.94.10 release notes (`pr#13152 <https://github.com/ceph/ceph/pull/13152>`_, Nathan Cutler)
* doc: Have install put manpages in the FreeBSD correct location (`pr#13301 <https://github.com/ceph/ceph/pull/13301>`_, Willem Jan Withagen)
* doc: how to specify filesystem for cephfs clients (`pr#14087 <https://github.com/ceph/ceph/pull/14087>`_, John Spray)
* doc: improve firewalld instructions (`pr#13360 <https://github.com/ceph/ceph/pull/13360>`_, Ken Dreyer)
* doc: Indicate how to add multiple admin capbabilies (`pr#13956 <https://github.com/ceph/ceph/pull/13956>`_, Chu, Hua-Rong)
* doc: instructions and guidance for multimds (`issue#19135 <http://tracker.ceph.com/issues/19135>`_, `pr#13830 <https://github.com/ceph/ceph/pull/13830>`_, John Spray)
* doc: instructions for provisioning OpenStack VMs ad hoc (`pr#13368 <https://github.com/ceph/ceph/pull/13368>`_, Nathan Cutler)
* doc: Jewel 10.2.6 release notes (`pr#13835 <https://github.com/ceph/ceph/pull/13835>`_, Abhishek Lekshmanan)
* doc: Jewel v10.2.8 release notes (`pr#16274 <https://github.com/ceph/ceph/pull/16274>`_, Nathan Cutler)
* doc: Jewel v10.2.9 release notes (`pr#16318 <https://github.com/ceph/ceph/pull/16318>`_, Nathan Cutler)
* doc: kernel client os-recommendations update (`pr#13369 <https://github.com/ceph/ceph/pull/13369>`_, John Spray, Ilya Dryomov)
* doc: kill some broken links (`pr#15203 <https://github.com/ceph/ceph/pull/15203>`_, liuchang0812)
* doc: kill sphinx warnings (`pr#16198 <https://github.com/ceph/ceph/pull/16198>`_, Kefu Chai)
* doc: luminous: doc: update rbd-mirroring documentation (`issue#20701 <http://tracker.ceph.com/issues/20701>`_, `pr#16912 <https://github.com/ceph/ceph/pull/16912>`_, Jason Dillaman)
* doc: Luminous release notes typo fixes (`pr#15899 <https://github.com/ceph/ceph/pull/15899>`_, Abhishek Lekshmanan)
* doc: mailmap: add affiliation for Zhu Shangzhong (`pr#16537 <https://github.com/ceph/ceph/pull/16537>`_, Zhu Shangzhong)
* doc: mailmap: add Alibaba into organization map (`pr#14900 <https://github.com/ceph/ceph/pull/14900>`_, James Liu)
* doc: mailmap: add Myoungwon Oh's mailmap and affiliation (`pr#15934 <https://github.com/ceph/ceph/pull/15934>`_, Myoungwon Oh)
* doc: mailmap for v12.0.2 (`pr#14753 <https://github.com/ceph/ceph/pull/14753>`_, Abhishek Lekshmanan)
* doc: mailmap: Michal Koutny affiliation (`pr#13036 <https://github.com/ceph/ceph/pull/13036>`_, Nathan Cutler)
* doc: mailmap, organizationmap: add affiliation for Tushar Gohad (`pr#16081 <https://github.com/ceph/ceph/pull/16081>`_, Tushar Gohad)
* doc:  .mailmap, .organizationmap: Update Fan Yang information and affiliation (`pr#16067 <https://github.com/ceph/ceph/pull/16067>`_, Fan Yang)
* doc: .mailmap, .organizationmap: Update Song Weibin information and affiliation (`pr#16311 <https://github.com/ceph/ceph/pull/16311>`_, songweibin)
* doc: .mailmap, .organizationmap: Update ztczll affiliation (`pr#16038 <https://github.com/ceph/ceph/pull/16038>`_, zhanglei)
* doc: mailmap updates for v11.1.0 (`pr#12335 <https://github.com/ceph/ceph/pull/12335>`_, Abhishek Lekshmanan)
* doc: mailmap updates (`pr#13309 <https://github.com/ceph/ceph/pull/13309>`_, Loic Dachary)
* doc: mailmap: V12.0.1 credits (`pr#14479 <https://github.com/ceph/ceph/pull/14479>`_, M Ranga Swami Reddy)
* doc: mailmap: Willem Jan Withagen affiliation (`pr#13034 <https://github.com/ceph/ceph/pull/13034>`_, Willem Jan Withagen)
* doc: mailmap: ztczll affiliation (`pr#15079 <https://github.com/ceph/ceph/pull/15079>`_, zhanglei)
* doc: man/8/ceph-disk: fix formatting (`pr#13969 <https://github.com/ceph/ceph/pull/13969>`_, Kefu Chai)
* doc: mention certain conf vars should be in global (`pr#15119 <https://github.com/ceph/ceph/pull/15119>`_, Ali Maredia)
* doc: mention ENXIO change in the 10.2.6 release notes (`pr#13878 <https://github.com/ceph/ceph/pull/13878>`_, Nathan Cutler)
* doc: mention --show-mappings in crushtool manpage (`issue#19649 <http://tracker.ceph.com/issues/19649>`_, `pr#14599 <https://github.com/ceph/ceph/pull/14599>`_, Nathan Cutler, Loic Dachary)
* doc: mention teuthology-worker security group (`pr#14748 <https://github.com/ceph/ceph/pull/14748>`_, Nathan Cutler)
* doc: Merge pull request from stiwari/wip-19879 (`issue#19879 <http://tracker.ceph.com/issues/19879>`_, `pr#15609 <https://github.com/ceph/ceph/pull/15609>`_, Sameer Tiwari)
* doc: mgr/restful: bind to :: and update docs (`pr#16267 <https://github.com/ceph/ceph/pull/16267>`_, Sage Weil)
* doc: minor changes in fuse client config reference (`pr#13065 <https://github.com/ceph/ceph/pull/13065>`_, Barbora Ančincová)
* doc: minor change to a cloud testing paragraph (`pr#13277 <https://github.com/ceph/ceph/pull/13277>`_, Jan Fajerski)
* doc: minor fixes in radosgw/ (`pr#15103 <https://github.com/ceph/ceph/pull/15103>`_, Drunkard Zhang)
* doc: min_size advice is not helpful (`pr#12936 <https://github.com/ceph/ceph/pull/12936>`_, Brad Hubbard)
* doc: misc minor fixes (`pr#13713 <https://github.com/ceph/ceph/pull/13713>`_, Drunkard Zhang)
* doc: Modify Configuring Cinder section (`issue#18840 <http://tracker.ceph.com/issues/18840>`_, `pr#13400 <https://github.com/ceph/ceph/pull/13400>`_, Shinobu Kinjo)
* doc: op queue and mclock related options (`pr#16662 <https://github.com/ceph/ceph/pull/16662>`_, J. Eric Ivancich)
* doc: organizationmap: add Xianxia Xiao to Kylin Cloud team (`pr#12718 <https://github.com/ceph/ceph/pull/12718>`_, Yunchuan Wen)
* doc: PendingReleaseNotes: "ceph -w" behavior has changed drastically (`pr#16425 <https://github.com/ceph/ceph/pull/16425>`_, Joao Eduardo Luis, Nathan Cutler)
* doc: PendingReleaseNotes: notes on whiteouts vs pgnls (`pr#15575 <https://github.com/ceph/ceph/pull/15575>`_, Sage Weil)
* doc: PendingReleaseNotes: note the fuse fstab format change (`pr#13259 <https://github.com/ceph/ceph/pull/13259>`_, John Spray)
* doc: PendingReleaseNotes: recent cephfs changes (`pr#14196 <https://github.com/ceph/ceph/pull/14196>`_, John Spray)
* doc: PendingReleaseNotes: warning about 'osd rm ...' and #19119 (`issue#19119 <http://tracker.ceph.com/issues/19119>`_, `pr#13731 <https://github.com/ceph/ceph/pull/13731>`_, Sage Weil)
* doc: peoplemap: add pdonnell alias (`pr#14352 <https://github.com/ceph/ceph/pull/14352>`_, Patrick Donnelly)
* doc: radosgw-admin: new 'global quota' commands update period config (`issue#19409 <http://tracker.ceph.com/issues/19409>`_, `pr#14252 <https://github.com/ceph/ceph/pull/14252>`_, Casey Bodley)
* doc: README.FreeBSD: update current status (`pr#12096 <https://github.com/ceph/ceph/pull/12096>`_, Willem Jan Withagen)
* doc: README.FreeBSD: Update the status (`pr#14406 <https://github.com/ceph/ceph/pull/14406>`_, Willem Jan Withagen)
* doc: README.md: fix build instructions inconsistent (`pr#14555 <https://github.com/ceph/ceph/pull/14555>`_, Yao Zongyou)
* doc: README.md: use github heading syntax to mark the headings (`pr#14591 <https://github.com/ceph/ceph/pull/14591>`_, Kefu Chai)
* doc: release-notes clarify about rgw encryption (`pr#14800 <https://github.com/ceph/ceph/pull/14800>`_, Abhishek Lekshmanan)
* doc: release notes for v10.2.7 Jewel (`pr#14295 <https://github.com/ceph/ceph/pull/14295>`_, Abhishek Lekshmanan)
* doc: release notes for v11.1.1 (`pr#12642 <https://github.com/ceph/ceph/pull/12642>`_, Abhishek Lekshmanan)
* doc: release notes for v12.0.3 (dev) (`pr#15090 <https://github.com/ceph/ceph/pull/15090>`_, Abhishek Lekshmanan)
* doc: releases update the luminous, hammer, jewel release dates (`pr#13584 <https://github.com/ceph/ceph/pull/13584>`_, Abhishek Lekshmanan)
* doc: remove deprecated subcommand in man/8/ceph.rst (`pr#14928 <https://github.com/ceph/ceph/pull/14928>`_, Drunkard Zhang)
* doc: remove docs on non-existant command (`pr#16616 <https://github.com/ceph/ceph/pull/16616>`_, Luo Kexue, Kefu Chai)
* doc: remove duplicated references (`pr#13396 <https://github.com/ceph/ceph/pull/13396>`_, Kefu Chai)
* doc: remove mentions about mon_osd_min_down_reports (`issue#19016 <http://tracker.ceph.com/issues/19016>`_, `pr#13558 <https://github.com/ceph/ceph/pull/13558>`_, Barbora Ančincová)
* doc: remove some non-existent and fix the default value according to … (`pr#15664 <https://github.com/ceph/ceph/pull/15664>`_, Leo Zhang)
* doc: Remove "splitting" state (`pr#12636 <https://github.com/ceph/ceph/pull/12636>`_, Brad Hubbard)
* doc: reword mds deactivate docs; add optional fs_name argument (`issue#20607 <http://tracker.ceph.com/issues/20607>`_, `pr#16471 <https://github.com/ceph/ceph/pull/16471>`_, Jan Fajerski)
* doc: Re-word the warnings about using git subtrees (`pr#14999 <https://github.com/ceph/ceph/pull/14999>`_, J. Eric Ivancich)
* doc: rgw clarify limitations when creating tenant names (`pr#16418 <https://github.com/ceph/ceph/pull/16418>`_, Abhishek Lekshmanan)
* doc: rgw: Clean up create subuser parameters (`pr#14335 <https://github.com/ceph/ceph/pull/14335>`_, hrchu)
* doc: rgw: correct get usage parameter default value (`pr#14372 <https://github.com/ceph/ceph/pull/14372>`_, hrchu)
* doc: rgw: Get user usage needs to specify user (`pr#14804 <https://github.com/ceph/ceph/pull/14804>`_, hrchu)
* doc: rgw: make a note abt system users vs normal users (`issue#18889 <http://tracker.ceph.com/issues/18889>`_, `pr#13461 <https://github.com/ceph/ceph/pull/13461>`_, Abhishek Lekshmanan)
* doc: rgw: note rgw_enable_usage_log option in adminops guide (`pr#14803 <https://github.com/ceph/ceph/pull/14803>`_, hrchu)
* doc: rgw: remove mention of megabytes for quotas (`pr#14413 <https://github.com/ceph/ceph/pull/14413>`_, Hans van den Bogert)
* doc: rgw: Rewrite Java swift examples (`pr#14268 <https://github.com/ceph/ceph/pull/14268>`_, Chu, Hua-Rong)
* doc: rgw: Rewrite the key management (`pr#14384 <https://github.com/ceph/ceph/pull/14384>`_, hrchu)
* doc: rgw server-side encryption and barbican (`pr#13483 <https://github.com/ceph/ceph/pull/13483>`_, Adam Kupczyk, Casey Bodley)
* doc: script: build-doc/serve-doc fixes (`pr#14438 <https://github.com/ceph/ceph/pull/14438>`_, Abhishek Lekshmanan)
* doc: script: ceph-release-notes: use https instead of http (`pr#14103 <https://github.com/ceph/ceph/pull/14103>`_, Kefu Chai)
* docs: doc/cephfs/troubleshooting: fix broken bullet list (`pr#12894 <https://github.com/ceph/ceph/pull/12894>`_, Dan Mick)
* docs: doc/dev: add some info about FreeBSD (`pr#14503 <https://github.com/ceph/ceph/pull/14503>`_, Willem Jan Withagen)
* docs: doc/release-notes: fix ceph-deploy command (`pr#15987 <https://github.com/ceph/ceph/pull/15987>`_, Sage Weil)
* docs: doc/release-note: update release-note (`pr#15748 <https://github.com/ceph/ceph/pull/15748>`_, liuchang0812)
* docs: document "osd recovery max single start" setting (`issue#17396 <http://tracker.ceph.com/issues/17396>`_, `pr#15275 <https://github.com/ceph/ceph/pull/15275>`_, Ken Dreyer)
* docs: mailmap: fix Zhao Chao affiliation (`pr#13413 <https://github.com/ceph/ceph/pull/13413>`_, Zhao Chao)
* docs: mailmap: Leo Zhang infomation and affiliation (`pr#15145 <https://github.com/ceph/ceph/pull/15145>`_, Leo Zhang)
* docs: mailmap: Liu Yang affiliation (`pr#13427 <https://github.com/ceph/ceph/pull/13427>`_, LiuYang)
* docs: mailmap: shiqi affiliation (`pr#14361 <https://github.com/ceph/ceph/pull/14361>`_, shiqi)
* docs: mailmap: update organization info (`pr#14747 <https://github.com/ceph/ceph/pull/14747>`_, liuchang0812)
* docs: mailmap: Weibing Zhang mailmap affiliation (`pr#15076 <https://github.com/ceph/ceph/pull/15076>`_, Weibing Zhang)
* docs: PendingReleaseNotes: mention forced recovery (`pr#16775 <https://github.com/ceph/ceph/pull/16775>`_, Piotr Dałek)
* docs: Remove contractions from the documentation (`pr#16629 <https://github.com/ceph/ceph/pull/16629>`_, John Wilkins)
* doc: style fix for doc/cephfs/client-config-ref.rst (`pr#14840 <https://github.com/ceph/ceph/pull/14840>`_, Drunkard Zhang)
* doc: tools/cephfs: fix cephfs-journal-tool --help (`pr#15614 <https://github.com/ceph/ceph/pull/15614>`_, John Spray)
* doc: two minor fixes (`pr#14494 <https://github.com/ceph/ceph/pull/14494>`_, Drunkard Zhang)
* doc: typo fixes on hyperlink/words (`pr#15144 <https://github.com/ceph/ceph/pull/15144>`_, Drunkard Zhang)
* doc: typo fix in s3_compliance (`pr#12598 <https://github.com/ceph/ceph/pull/12598>`_, LiuYang)
* doc: typo in hit_set_search_last_n (`pr#14108 <https://github.com/ceph/ceph/pull/14108>`_, Sven Seeberg)
* doc: Update adminops.rst (`pr#13893 <https://github.com/ceph/ceph/pull/13893>`_, Chu, Hua-Rong)
* doc: update ceph(8) man page with new sub-commands (`pr#16437 <https://github.com/ceph/ceph/pull/16437>`_, Kefu Chai)
* doc: Update CephFS disaster recovery documentation (`pr#12370 <https://github.com/ceph/ceph/pull/12370>`_, Wido den Hollander)
* doc: Update disk thread section to reflect that scrubbing is no longe… (`pr#12621 <https://github.com/ceph/ceph/pull/12621>`_, Nick Fisk)
* doc: update intro, quick start docs (`pr#16224 <https://github.com/ceph/ceph/pull/16224>`_, Sage Weil)
* doc: Update keystone.rst (`pr#12717 <https://github.com/ceph/ceph/pull/12717>`_, Chu, Hua-Rong)
* doc: update links to point to ceph/qa instead of ceph-qa-suite (`pr#13397 <https://github.com/ceph/ceph/pull/13397>`_, Jan Fajerski, Nathan Cutler)
* doc: Update .organizationmap (`pr#16507 <https://github.com/ceph/ceph/pull/16507>`_, luokexue)
* doc: update packages mentioned by build-doc and related doc (`pr#14649 <https://github.com/ceph/ceph/pull/14649>`_, Yu Shengzuo)
* doc: Update sample.ceph.conf (`pr#13751 <https://github.com/ceph/ceph/pull/13751>`_, Saumay Agrawal)
* doc: update sample explaning "%" operator in test suites (`pr#15511 <https://github.com/ceph/ceph/pull/15511>`_, Kefu Chai)
* doc: Update some RGW documentation (`pr#15175 <https://github.com/ceph/ceph/pull/15175>`_, Jens Rosenboom)
* doc: update the pool names created by vstart.sh by default (`pr#16652 <https://github.com/ceph/ceph/pull/16652>`_, Zhu Shangzhong)
* doc: update the rados namespace docs (`pr#15838 <https://github.com/ceph/ceph/pull/15838>`_, Abhishek Lekshmanan)
* doc: update the support status of swift static website (`pr#13824 <https://github.com/ceph/ceph/pull/13824>`_, Jing Wenjun)
* doc: update the usage of 'ceph-deploy purge' (`pr#15080 <https://github.com/ceph/ceph/pull/15080>`_, Yu Shengzuo)
* doc: update to new ceph fs commands (`pr#13346 <https://github.com/ceph/ceph/pull/13346>`_, Patrick Donnelly)
* doc: upmap docs; various missing links for release notes (`pr#16637 <https://github.com/ceph/ceph/pull/16637>`_, Sage Weil)
* doc: use do_cmake.sh instead of `cmake ..` (`pr#15110 <https://github.com/ceph/ceph/pull/15110>`_, Kefu Chai)
* doc: v12.0.0 release notes (`pr#13281 <https://github.com/ceph/ceph/pull/13281>`_, Abhishek Lekshmanan)
* doc: v12.0.2 (dev) release notes (`pr#14625 <https://github.com/ceph/ceph/pull/14625>`_, Abhishek Lekshmanan)
* doc: v12.1.0 release notes notable changes addition again (`pr#15857 <https://github.com/ceph/ceph/pull/15857>`_, Abhishek Lekshmanan)
* doc: various fixes (`pr#16723 <https://github.com/ceph/ceph/pull/16723>`_, Kefu Chai)
* doc: vstart: add --help documentation for rgw_num (`pr#13817 <https://github.com/ceph/ceph/pull/13817>`_, Ali Maredia)
* doc: wip-doc-multisite ports downstream multisite document upstream (`pr#14259 <https://github.com/ceph/ceph/pull/14259>`_, John Wilkins)
* doc: Wip osd discussion docs (`pr#13344 <https://github.com/ceph/ceph/pull/13344>`_, Greg Farnum)
* filestore: os/filestore: Exclude BTRFS on FreeBSD (`pr#16171 <https://github.com/ceph/ceph/pull/16171>`_, Willem Jan Withagen)
* filestore: os/filestore/FileJournal: Fix typo in the comment (`pr#14493 <https://github.com/ceph/ceph/pull/14493>`_, Zhou Zhengping)
* filestore: os/filestore: use existing variable for same func (`pr#13742 <https://github.com/ceph/ceph/pull/13742>`_, Pan Liu)
* filestore: os/filestore: when print log, use `__func__` instead of hard code function name (`pr#15261 <https://github.com/ceph/ceph/pull/15261>`_, mychoxin)
* filestore: os/filestore: zfs add get_name() (`pr#15650 <https://github.com/ceph/ceph/pull/15650>`_, Yanhu Cao)
    Fix full testing in cephtool/test.sh when used by rados suite
    @Jing-Scott  updated, addressing @rzarzynski's change request
* librados: add log channel to rados_monitor_log2 callback (`pr#15926 <https://github.com/ceph/ceph/pull/15926>`_, Sage Weil)
* librados: add missing implementations for C service daemon API methods (`pr#16543 <https://github.com/ceph/ceph/pull/16543>`_, Jason Dillaman)
* librados: add override for librados (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13442 <https://github.com/ceph/ceph/pull/13442>`_, liuchang0812)
* librados: add override in headers (`pr#13775 <https://github.com/ceph/ceph/pull/13775>`_, liuchang0812)
* librados: asynchronous selfmanaged_snap_create/selfmanaged_snap_remove APIs (`issue#16180 <http://tracker.ceph.com/issues/16180>`_, `pr#12050 <https://github.com/ceph/ceph/pull/12050>`_, Jason Dillaman)
* librados: do not expose non-public symbols (`pr#13265 <https://github.com/ceph/ceph/pull/13265>`_, Kefu Chai)
* librados: fix compile errors from simplified aio completions (`pr#12849 <https://github.com/ceph/ceph/pull/12849>`_, xie xingguo)
* librados: fix rados_pool_list when buf is null (`pr#14859 <https://github.com/ceph/ceph/pull/14859>`_, Sage Weil)
* librados: redirect balanced reads to acting primary when targeting object isn't recovered (`issue#17968 <http://tracker.ceph.com/issues/17968>`_, `pr#15489 <https://github.com/ceph/ceph/pull/15489>`_, Xuehan Xu)
* librados: remove legacy object listing API, clean up newer api (`pr#13149 <https://github.com/ceph/ceph/pull/13149>`_, Sage Weil)
* librados: replace the var name from onack to complete (`pr#13857 <https://github.com/ceph/ceph/pull/13857>`_, Pan Liu)
* librados: set the flag CEPH_OSD_FLAG_FULL_TRY of Op in the right place (`pr#14193 <https://github.com/ceph/ceph/pull/14193>`_, Pan Liu)
* librados: use cursor for nobjects listing (`pr#13323 <https://github.com/ceph/ceph/pull/13323>`_, Yehuda Sadeh, Sage Weil)
* librbd: add compare and write API (`pr#14868 <https://github.com/ceph/ceph/pull/14868>`_, Zhengyong Wang, Jason Dillaman)
* librbd: add create timestamp metadata for image (`pr#15757 <https://github.com/ceph/ceph/pull/15757>`_, runsisi)
* librbd: added rbd_flatten_with_progress to API (`issue#15824 <http://tracker.ceph.com/issues/15824>`_, `pr#12905 <https://github.com/ceph/ceph/pull/12905>`_, Ricardo Dias)
* librbd: add LIBRBD_SUPPORTS_WRITESAME support (`pr#16583 <https://github.com/ceph/ceph/pull/16583>`_, Xiubo Li)
* librbd: add override keyword in header files (`issue#19012 <http://tracker.ceph.com/issues/19012>`_, `pr#13536 <https://github.com/ceph/ceph/pull/13536>`_, liuchang0812)
* librbd: add SnapshotNamespace to ImageCtx (`pr#12970 <https://github.com/ceph/ceph/pull/12970>`_, Victor Denisov)
* librbd: add writesame API (`pr#12645 <https://github.com/ceph/ceph/pull/12645>`_, Mingxin Liu, Gui Hecheng)
* librbd: allow to open an image without opening the parent image (`issue#18325 <http://tracker.ceph.com/issues/18325>`_, `pr#12885 <https://github.com/ceph/ceph/pull/12885>`_, Ricardo Dias)
* librbd: asynchronous clone state machine (`pr#12041 <https://github.com/ceph/ceph/pull/12041>`_, Dongsheng Yang)
* librbd: asynchronous image removal state machine (`pr#12102 <https://github.com/ceph/ceph/pull/12102>`_, Dongsheng Yang, Venky Shankar)
* librbd: avoid possible recursive lock when racing acquire lock (`issue#17447 <http://tracker.ceph.com/issues/17447>`_, `pr#12991 <https://github.com/ceph/ceph/pull/12991>`_, Jason Dillaman)
* librbd: changed the return type of ImageRequestWQ::discard() (`issue#18511 <http://tracker.ceph.com/issues/18511>`_, `pr#14032 <https://github.com/ceph/ceph/pull/14032>`_, Jos Collin)
* librbd: cleanup logging code under librbd/io (`pr#14975 <https://github.com/ceph/ceph/pull/14975>`_, runsisi)
* librbd: corrected resize RPC message backwards compatibility (`issue#19636 <http://tracker.ceph.com/issues/19636>`_, `pr#14615 <https://github.com/ceph/ceph/pull/14615>`_, Jason Dillaman)
* librbd: create fewer empty objects during copyup (`issue#15028 <http://tracker.ceph.com/issues/15028>`_, `pr#12326 <https://github.com/ceph/ceph/pull/12326>`_, Douglas Fuller, Venky Shankar)
* librbd: deferred image deletion (`issue#18481 <http://tracker.ceph.com/issues/18481>`_, `pr#13105 <https://github.com/ceph/ceph/pull/13105>`_, Ricardo Dias)
* librbd: delay mirror registration when creating clones (`issue#17993 <http://tracker.ceph.com/issues/17993>`_, `pr#12839 <https://github.com/ceph/ceph/pull/12839>`_, Jason Dillaman)
* librbd: discard related IO should skip op if object non-existent (`issue#19962 <http://tracker.ceph.com/issues/19962>`_, `pr#15239 <https://github.com/ceph/ceph/pull/15239>`_, Mykola Golub)
* librbd: do not instantiate templates while building tests (`issue#18938 <http://tracker.ceph.com/issues/18938>`_, `pr#14891 <https://github.com/ceph/ceph/pull/14891>`_, Kefu Chai)
* librbd: do not raise an error if trash list returns -ENOENT (`pr#15085 <https://github.com/ceph/ceph/pull/15085>`_, runsisi)
* librbd: don't continue to remove an image w/ incompatible features (`issue#18315 <http://tracker.ceph.com/issues/18315>`_, `pr#12638 <https://github.com/ceph/ceph/pull/12638>`_, Dongsheng Yang)
* librbd: eliminate compiler warnings (`pr#13729 <https://github.com/ceph/ceph/pull/13729>`_, Jason Dillaman)
* librbd: fail IO request when exclusive lock cannot be obtained (`pr#15860 <https://github.com/ceph/ceph/pull/15860>`_, Jason Dillaman)
* librbd: filter expected error codes from is_exclusive_lock_owner (`issue#20182 <http://tracker.ceph.com/issues/20182>`_, `pr#15483 <https://github.com/ceph/ceph/pull/15483>`_, Jason Dillaman)
* librbd: fix clang compilation error (`issue#19260 <http://tracker.ceph.com/issues/19260>`_, `pr#13926 <https://github.com/ceph/ceph/pull/13926>`_, Mykola Golub)
* librbd: fixed initializer list ordering (`pr#13042 <https://github.com/ceph/ceph/pull/13042>`_, Jason Dillaman)
* librbd: fix issues with image removal state machine (`pr#15734 <https://github.com/ceph/ceph/pull/15734>`_, Jason Dillaman)
* librbd: fix rbd_metadata_list and rbd_metadata_get (`issue#19588 <http://tracker.ceph.com/issues/19588>`_, `pr#14471 <https://github.com/ceph/ceph/pull/14471>`_, Mykola Golub)
* librbd: fix segfault on EOPNOTSUPP returned while fetching snapshot timestamp (`issue#18839 <http://tracker.ceph.com/issues/18839>`_, `pr#13287 <https://github.com/ceph/ceph/pull/13287>`_, Gui Hecheng)
* librbd: fix valgrind errors and ensure tests detect future leaks (`pr#15415 <https://github.com/ceph/ceph/pull/15415>`_, Jason Dillaman)
* librbd: fix valid coverity warnings (`pr#14023 <https://github.com/ceph/ceph/pull/14023>`_, Jason Dillaman)
* librbd: image create validates that pool supports overwrites (`issue#19081 <http://tracker.ceph.com/issues/19081>`_, `pr#13986 <https://github.com/ceph/ceph/pull/13986>`_, Jason Dillaman)
* librbd: image-extent cache needs to clip out-of-bounds read buffers (`pr#13679 <https://github.com/ceph/ceph/pull/13679>`_, Jason Dillaman)
* librbd: Include WorkQueue.h since we use it (`issue#18862 <http://tracker.ceph.com/issues/18862>`_, `pr#13322 <https://github.com/ceph/ceph/pull/13322>`_, Boris Ranto)
* librbd: initialize diff parent overlap to zero (`pr#13077 <https://github.com/ceph/ceph/pull/13077>`_, Gu Zhongyan)
* librbd: introduce new constants for tracking max block name prefix (`issue#18653 <http://tracker.ceph.com/issues/18653>`_, `pr#13141 <https://github.com/ceph/ceph/pull/13141>`_, Jason Dillaman)
* librbd: is_exclusive_lock_owner API should ping OSD (`issue#19287 <http://tracker.ceph.com/issues/19287>`_, `pr#14003 <https://github.com/ceph/ceph/pull/14003>`_, Jason Dillaman)
* librbd: managed lock refactoring (`pr#12922 <https://github.com/ceph/ceph/pull/12922>`_, Mykola Golub)
* librbd: metadata_set API operation should not change global config setting (`issue#18465 <http://tracker.ceph.com/issues/18465>`_, `pr#12843 <https://github.com/ceph/ceph/pull/12843>`_, Mykola Golub)
* librbd: minor fixes for image trash move (`pr#14834 <https://github.com/ceph/ceph/pull/14834>`_, runsisi)
* librbd: new API method to force break a peer's exclusive lock (`issue#18429 <http://tracker.ceph.com/issues/18429>`_, `issue#16988 <http://tracker.ceph.com/issues/16988>`_, `issue#18327 <http://tracker.ceph.com/issues/18327>`_, `pr#12639 <https://github.com/ceph/ceph/pull/12639>`_, Jason Dillaman)
* librbd: Notifier::notify API improvement (`pr#14072 <https://github.com/ceph/ceph/pull/14072>`_, Mykola Golub)
* librbd: optimize copy-up to add hints only once to object op (`issue#19875 <http://tracker.ceph.com/issues/19875>`_, `pr#15037 <https://github.com/ceph/ceph/pull/15037>`_, Mykola Golub)
* librbd: pass an uint64_t to clip_io() as the third param (`issue#18938 <http://tracker.ceph.com/issues/18938>`_, `pr#14159 <https://github.com/ceph/ceph/pull/14159>`_, Kefu Chai)
* librbd: permit removal of image being bootstrapped by rbd-mirror (`issue#16555 <http://tracker.ceph.com/issues/16555>`_, `pr#12549 <https://github.com/ceph/ceph/pull/12549>`_, Mykola Golub)
* librbd: possible deadlock with flush if refresh in-progress (`issue#18419 <http://tracker.ceph.com/issues/18419>`_, `pr#12838 <https://github.com/ceph/ceph/pull/12838>`_, Jason Dillaman)
* librbd: potential read IO hang when image is flattened (`issue#19832 <http://tracker.ceph.com/issues/19832>`_, `pr#15234 <https://github.com/ceph/ceph/pull/15234>`_, Jason Dillaman)
* librbd: potential use of uninitialised value in ImageWatcher (`pr#14091 <https://github.com/ceph/ceph/pull/14091>`_, Mykola Golub)
* librbd: prevent self-blacklisting during break lock (`issue#18666 <http://tracker.ceph.com/issues/18666>`_, `pr#13110 <https://github.com/ceph/ceph/pull/13110>`_, Jason Dillaman)
* librbd: race initializing exclusive lock and configuring IO path (`pr#13086 <https://github.com/ceph/ceph/pull/13086>`_, Jason Dillaman)
* librbd: random unit test failures due to shut down race (`issue#19389 <http://tracker.ceph.com/issues/19389>`_, `pr#14166 <https://github.com/ceph/ceph/pull/14166>`_, Jason Dillaman)
* librbd: rbd ack cleanup (`pr#13791 <https://github.com/ceph/ceph/pull/13791>`_, runsisi)
* librbd: reacquire lock should update lock owner client id (`issue#19929 <http://tracker.ceph.com/issues/19929>`_, `pr#15093 <https://github.com/ceph/ceph/pull/15093>`_, Jason Dillaman)
* librbd: reduce potential of erroneous blacklisting on image close (`issue#19970 <http://tracker.ceph.com/issues/19970>`_, `pr#15162 <https://github.com/ceph/ceph/pull/15162>`_, Jason Dillaman)
* librbd: refactor exclusive lock support into generic managed lock (`issue#17016 <http://tracker.ceph.com/issues/17016>`_, `pr#12846 <https://github.com/ceph/ceph/pull/12846>`_, Ricardo Dias, Jason Dillaman)
* librbd: relax "is parent mirrored" check when enabling mirroring for pool (`issue#19130 <http://tracker.ceph.com/issues/19130>`_, `pr#13752 <https://github.com/ceph/ceph/pull/13752>`_, Mykola Golub)
* librbd: remove redundant check for image id emptiness (`pr#14830 <https://github.com/ceph/ceph/pull/14830>`_, runsisi)
* librbd: remove unnecessary dependencies of ManagedLock (`pr#12982 <https://github.com/ceph/ceph/pull/12982>`_, Jason Dillaman)
* librbd: remove unused rbd_image_options_t ostream operator (`pr#15443 <https://github.com/ceph/ceph/pull/15443>`_, Mykola Golub)
* librbd: resolve static analyser warnings (`pr#12863 <https://github.com/ceph/ceph/pull/12863>`_, Jason Dillaman)
* librbd: scatter/gather support for the C API (`issue#13025 <http://tracker.ceph.com/issues/13025>`_, `pr#13447 <https://github.com/ceph/ceph/pull/13447>`_, Jason Dillaman)
* librbd: silence -Wunused-variable warning (`pr#14953 <https://github.com/ceph/ceph/pull/14953>`_, Kefu Chai)
* librbd: simplify image open/close semantics (`pr#13701 <https://github.com/ceph/ceph/pull/13701>`_, Jason Dillaman)
* librbd: support for shared locking in ManagedLock (`pr#12886 <https://github.com/ceph/ceph/pull/12886>`_, Ricardo Dias)
* librbd: support to list snapshot timestamp (`issue#808 <http://tracker.ceph.com/issues/808>`_, `pr#12817 <https://github.com/ceph/ceph/pull/12817>`_, Pan Liu)
* librbd: Uninitialized variable used handle_refresh() (`pr#16724 <https://github.com/ceph/ceph/pull/16724>`_, amitkuma)
* librbd: use 'override' keyword instead of 'virtual' (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13437 <https://github.com/ceph/ceph/pull/13437>`_, liuchang0812)
* librbd: warning message for mirroring pool option (`issue#18125 <http://tracker.ceph.com/issues/18125>`_, `pr#12319 <https://github.com/ceph/ceph/pull/12319>`_, Gaurav Kumar Garg)
* log: use one write system call per message (`pr#11955 <https://github.com/ceph/ceph/pull/11955>`_, Patrick Donnelly)
* mds: add authority check for delay dirfrag split (`issue#18487 <http://tracker.ceph.com/issues/18487>`_, `pr#12994 <https://github.com/ceph/ceph/pull/12994>`_, "Yan, Zheng")
* mds: add override in headers (`pr#13691 <https://github.com/ceph/ceph/pull/13691>`_, liuchang0812)
* mds: add override in mds subsystem (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13438 <https://github.com/ceph/ceph/pull/13438>`_, liuchang0812)
* mds: add perf counters for file system operations (`pr#14938 <https://github.com/ceph/ceph/pull/14938>`_, Michael Sevilla)
* mds: automate MDS object count tracking (`pr#13591 <https://github.com/ceph/ceph/pull/13591>`_, Patrick Donnelly)
* mds: bump client_reply debug to match client_req (`pr#14036 <https://github.com/ceph/ceph/pull/14036>`_, Patrick Donnelly)
* mds: change_attr++ and set ctime for set_vxattr (`issue#19583 <http://tracker.ceph.com/issues/19583>`_, `pr#14726 <https://github.com/ceph/ceph/pull/14726>`_, Patrick Donnelly)
* mds: change the type of data_pools (`pr#15278 <https://github.com/ceph/ceph/pull/15278>`_, Vicente Cheng)
* mds: check export pin during replay (`issue#20039 <http://tracker.ceph.com/issues/20039>`_, `pr#15205 <https://github.com/ceph/ceph/pull/15205>`_, Patrick Donnelly)
* mds: check for errors decoding backtraces (`issue#18311 <http://tracker.ceph.com/issues/18311>`_, `pr#12588 <https://github.com/ceph/ceph/pull/12588>`_, John Spray)
* mds:  Client syncfs is slow (waits for next MDS tick) (`issue#20129 <http://tracker.ceph.com/issues/20129>`_, `pr#15544 <https://github.com/ceph/ceph/pull/15544>`_, dongdong tao)
* mds: don't assert on read errors in RecoveryQueue (`issue#19282 <http://tracker.ceph.com/issues/19282>`_, `pr#14017 <https://github.com/ceph/ceph/pull/14017>`_, John Spray)
* mds: don't modify inode that is not projected (`issue#16768 <http://tracker.ceph.com/issues/16768>`_, `pr#13052 <https://github.com/ceph/ceph/pull/13052>`_, "Yan, Zheng")
* mds: drop partial entry and adjust write_pos when opening PurgeQueue (`issue#19450 <http://tracker.ceph.com/issues/19450>`_, `pr#14447 <https://github.com/ceph/ceph/pull/14447>`_, "Yan, Zheng")
* mds: explicitly output error msg for dump cache asok command (`pr#15592 <https://github.com/ceph/ceph/pull/15592>`_, Zhi Zhang)
* mds: extend 'p' auth cap to cover all vxattr stuff (`issue#19075 <http://tracker.ceph.com/issues/19075>`_, `pr#13628 <https://github.com/ceph/ceph/pull/13628>`_, John Spray)
* mds: finish clientreplay requests before requesting active state (`issue#18461 <http://tracker.ceph.com/issues/18461>`_, `pr#12852 <https://github.com/ceph/ceph/pull/12852>`_, Yan, Zheng)
* mds: fix bad iterator dereference reported by coverity (`issue#18830 <http://tracker.ceph.com/issues/18830>`_, `pr#13272 <https://github.com/ceph/ceph/pull/13272>`_, John Spray)
* mds: fix CDir::merge() for mds_debug_auth_pins (`issue#19946 <http://tracker.ceph.com/issues/19946>`_, `pr#15130 <https://github.com/ceph/ceph/pull/15130>`_, "Yan, Zheng")
* mds: fix client ID truncation (`pr#15258 <https://github.com/ceph/ceph/pull/15258>`_, Henry Chang)
* mds: fix handling very fast delete ops (`issue#19245 <http://tracker.ceph.com/issues/19245>`_, `pr#13899 <https://github.com/ceph/ceph/pull/13899>`_, John Spray)
* mds: fix hangs involving re-entrant calls to journaler (`issue#20165 <http://tracker.ceph.com/issues/20165>`_, `pr#15430 <https://github.com/ceph/ceph/pull/15430>`_, John Spray)
* mds: fix incorrect assertion in Server::_dir_is_nonempty() (`issue#18578 <http://tracker.ceph.com/issues/18578>`_, `pr#12973 <https://github.com/ceph/ceph/pull/12973>`_, Yan, Zheng)
* mds: fix IO error handling in SessionMap (`pr#13464 <https://github.com/ceph/ceph/pull/13464>`_, John Spray)
* mds: fix mantle script to not fail for last rank (`issue#19589 <http://tracker.ceph.com/issues/19589>`_, `pr#14704 <https://github.com/ceph/ceph/pull/14704>`_, Patrick Donnelly)
* mds: fix mgrc shutdown (`issue#19566 <http://tracker.ceph.com/issues/19566>`_, `pr#14505 <https://github.com/ceph/ceph/pull/14505>`_, John Spray)
* mds: fix null pointer dereference in Locker::handle_client_caps (`issue#18306 <http://tracker.ceph.com/issues/18306>`_, `pr#12808 <https://github.com/ceph/ceph/pull/12808>`_, Yan, Zheng)
* mds: fix stray creation/removal notification (`issue#19630 <http://tracker.ceph.com/issues/19630>`_, `pr#14554 <https://github.com/ceph/ceph/pull/14554>`_, "Yan, Zheng")
* mds: fix use-after-free in Locker::file_update_finish() (`issue#19828 <http://tracker.ceph.com/issues/19828>`_, `pr#14991 <https://github.com/ceph/ceph/pull/14991>`_, "Yan, Zheng")
* mds: ignore ENOENT on writing backtrace (`issue#19401 <http://tracker.ceph.com/issues/19401>`_, `pr#14207 <https://github.com/ceph/ceph/pull/14207>`_, John Spray)
* mds: ignore fs full check for CEPH_MDS_OP_SETFILELOCK (`issue#18953 <http://tracker.ceph.com/issues/18953>`_, `pr#13455 <https://github.com/ceph/ceph/pull/13455>`_, "Yan, Zheng")
* mds: improvements for stray reintegration (`pr#15548 <https://github.com/ceph/ceph/pull/15548>`_, "Yan, Zheng")
* mds: include advisory `path` field in damage (`issue#18509 <http://tracker.ceph.com/issues/18509>`_, `pr#14104 <https://github.com/ceph/ceph/pull/14104>`_, John Spray)
* mds: issue new caps when sending reply to client (`issue#19635 <http://tracker.ceph.com/issues/19635>`_, `pr#14743 <https://github.com/ceph/ceph/pull/14743>`_, "Yan, Zheng")
* mds: limit client writable range increment (`issue#19955 <http://tracker.ceph.com/issues/19955>`_, `pr#15131 <https://github.com/ceph/ceph/pull/15131>`_, "Yan, Zheng")
* mds: make C_MDSInternalNoop::complete() delete 'this' (`issue#19501 <http://tracker.ceph.com/issues/19501>`_, `pr#14347 <https://github.com/ceph/ceph/pull/14347>`_, "Yan, Zheng")
* mds: mds perf item 'l_mdl_expos' always behind journaler (`pr#15621 <https://github.com/ceph/ceph/pull/15621>`_, redickwang)
* mds: miscellaneous fixes (`issue#18646 <http://tracker.ceph.com/issues/18646>`_, `pr#12974 <https://github.com/ceph/ceph/pull/12974>`_, Yan, Zheng, "Yan, Zheng")
* mds: miscellaneous multimds fixes (`issue#19022 <http://tracker.ceph.com/issues/19022>`_, `pr#13698 <https://github.com/ceph/ceph/pull/13698>`_, "Yan, Zheng")
* mds: miscellaneous multimds fixes part2 (`pr#15125 <https://github.com/ceph/ceph/pull/15125>`_, "Yan, Zheng")
* mds: miscellaneous multimds fixes (`pr#14550 <https://github.com/ceph/ceph/pull/14550>`_, "Yan, Zheng")
* mds: misc multimds fixes (`issue#18717 <http://tracker.ceph.com/issues/18717>`_, `issue#18754 <http://tracker.ceph.com/issues/18754>`_, `pr#13227 <https://github.com/ceph/ceph/pull/13227>`_, "Yan, Zheng")
* mds: misc multimds fixes part2 (`pr#12794 <https://github.com/ceph/ceph/pull/12794>`_, Yan, Zheng)
* mds: misc multimds fixes (`pr#12274 <https://github.com/ceph/ceph/pull/12274>`_, Yan, Zheng)
* mds,mon: Clean issues detected by cppcheck (`pr#13199 <https://github.com/ceph/ceph/pull/13199>`_, Ilya Shipitsin)
* mds: multimds flock fixes (`pr#15440 <https://github.com/ceph/ceph/pull/15440>`_, "Yan, Zheng")
* mds: Pass empty string to clear mantle balancer (`issue#20076 <http://tracker.ceph.com/issues/20076>`_, `pr#15282 <https://github.com/ceph/ceph/pull/15282>`_, Zhi Zhang)
* mds: pretty json from `tell` commands (`pr#14105 <https://github.com/ceph/ceph/pull/14105>`_, John Spray)
* mds: print rank as int (`issue#19201 <http://tracker.ceph.com/issues/19201>`_, `pr#13816 <https://github.com/ceph/ceph/pull/13816>`_, Patrick Donnelly)
* mds: propagate error encountered during opening inode by number (`issue#18179 <http://tracker.ceph.com/issues/18179>`_, `pr#12749 <https://github.com/ceph/ceph/pull/12749>`_, Yan, Zheng)
* mds: properly create aux subtrees for pinned directory (`issue#20083 <http://tracker.ceph.com/issues/20083>`_, `pr#15300 <https://github.com/ceph/ceph/pull/15300>`_, "Yan, Zheng")
* mds: relocate PTRWAITER put near get (`pr#14921 <https://github.com/ceph/ceph/pull/14921>`_, Patrick Donnelly)
* mds: remove boost::pool usage and use tcmalloc directly (`issue#18425 <http://tracker.ceph.com/issues/18425>`_, `pr#12792 <https://github.com/ceph/ceph/pull/12792>`_, Zhi Zhang)
* mds: remove legacy "mds tell" command (`issue#19288 <http://tracker.ceph.com/issues/19288>`_, `pr#14015 <https://github.com/ceph/ceph/pull/14015>`_, John Spray)
* mds: remove "mds log" config option (`issue#18816 <http://tracker.ceph.com/issues/18816>`_, `pr#14652 <https://github.com/ceph/ceph/pull/14652>`_, John Spray)
* mds: remove some redundant object counters (`pr#13704 <https://github.com/ceph/ceph/pull/13704>`_, Patrick Donnelly)
* mds: replace C_VoidFn in MDSDaemon with lambdas (`pr#13465 <https://github.com/ceph/ceph/pull/13465>`_, John Spray)
* mds: Return error message instead of asserting (`pr#14469 <https://github.com/ceph/ceph/pull/14469>`_, Brad Hubbard)
* mds: save  projected path into inode_t::stray_prior_path (`issue#20340 <http://tracker.ceph.com/issues/20340>`_, `pr#15800 <https://github.com/ceph/ceph/pull/15800>`_, "Yan, Zheng")
* mds: set ceph-mds name uncond for external tools (`issue#19291 <http://tracker.ceph.com/issues/19291>`_, `pr#14021 <https://github.com/ceph/ceph/pull/14021>`_, Patrick Donnelly)
* mds: shut down finisher before objecter (`issue#19204 <http://tracker.ceph.com/issues/19204>`_, `pr#13859 <https://github.com/ceph/ceph/pull/13859>`_, John Spray)
* mds: skip fragment space check for replayed request (`issue#18660 <http://tracker.ceph.com/issues/18660>`_, `pr#13095 <https://github.com/ceph/ceph/pull/13095>`_, "Yan, Zheng")
* mds: support export pinning on directories (`issue#17834 <http://tracker.ceph.com/issues/17834>`_, `pr#14598 <https://github.com/ceph/ceph/pull/14598>`_, "Yan, Zheng", Patrick Donnelly)
* mds: try to avoid false positive heartbeat timeouts (`issue#19118 <http://tracker.ceph.com/issues/19118>`_, `pr#13807 <https://github.com/ceph/ceph/pull/13807>`_, John Spray)
* mds: use debug_mds for most subsys (`issue#19734 <http://tracker.ceph.com/issues/19734>`_, `pr#15052 <https://github.com/ceph/ceph/pull/15052>`_, Patrick Donnelly)
* mds: use same inode count in health check as in trim (`issue#19395 <http://tracker.ceph.com/issues/19395>`_, `pr#14197 <https://github.com/ceph/ceph/pull/14197>`_, John Spray)
* mds: warn if insufficient standbys exist (`issue#17604 <http://tracker.ceph.com/issues/17604>`_, `pr#12074 <https://github.com/ceph/ceph/pull/12074>`_, Patrick Donnelly)
* mgr: add a get_version to the python interface (`pr#13669 <https://github.com/ceph/ceph/pull/13669>`_, John Spray)
* mgr: add machinery for python modules to send MCommands to daemons (`pr#14920 <https://github.com/ceph/ceph/pull/14920>`_, John Spray)
* mgr: add mgr allow \* to client.admin (`pr#14864 <https://github.com/ceph/ceph/pull/14864>`_, huanwen ren)
* mgr: add override in headers (`pr#13772 <https://github.com/ceph/ceph/pull/13772>`_, liuchang0812)
* mgr: add override in mgr subsystem (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13436 <https://github.com/ceph/ceph/pull/13436>`_, liuchang0812)
* mgr: add per-DaemonState lock (`pr#16432 <https://github.com/ceph/ceph/pull/16432>`_, Sage Weil)
* mgr: always free allocated MgrPyModule (`issue#19590 <http://tracker.ceph.com/issues/19590>`_, `pr#14507 <https://github.com/ceph/ceph/pull/14507>`_, Kefu Chai)
* mgr: ceph-create-keys: update client.admin if it already exists (`issue#19940 <http://tracker.ceph.com/issues/19940>`_, `pr#15112 <https://github.com/ceph/ceph/pull/15112>`_, John Spray)
* mgr: ceph: introduce "tell x help" subcommand (`issue#19885 <http://tracker.ceph.com/issues/19885>`_, `pr#15111 <https://github.com/ceph/ceph/pull/15111>`_, liuchang0812)
* mgr: ceph-mgr: Implement new pecan-based rest api (`pr#14457 <https://github.com/ceph/ceph/pull/14457>`_, Boris Ranto)
* mgr: ceph-mgr: rotate logs on sighup (`issue#19568 <http://tracker.ceph.com/issues/19568>`_, `pr#14437 <https://github.com/ceph/ceph/pull/14437>`_, Dan van der Ster)
* mgr: clean up daemon start process (`issue#20383 <http://tracker.ceph.com/issues/20383>`_, `pr#16020 <https://github.com/ceph/ceph/pull/16020>`_, John Spray)
* mgr: clean up fsstatus module (`pr#15925 <https://github.com/ceph/ceph/pull/15925>`_, John Spray)
* mgr: cleanup, stop clients sending in perf counters (`pr#15578 <https://github.com/ceph/ceph/pull/15578>`_, John Spray)
* mgr: cluster log message on plugin load error (`pr#15927 <https://github.com/ceph/ceph/pull/15927>`_, John Spray)
* mgr: dashboard code cleanup (`pr#15577 <https://github.com/ceph/ceph/pull/15577>`_, John Spray)
* mgr: dashboard GUI module (`pr#14946 <https://github.com/ceph/ceph/pull/14946>`_, John Spray, Dan Mick)
* mgr: dashboard improvements (`pr#16043 <https://github.com/ceph/ceph/pull/16043>`_, John Spray)
* mgr: do shutdown using finisher so we can do it in the right order (`issue#19743 <http://tracker.ceph.com/issues/19743>`_, `pr#14835 <https://github.com/ceph/ceph/pull/14835>`_, Kefu Chai)
* mgr: do the shutdown in the right order (`issue#19813 <http://tracker.ceph.com/issues/19813>`_, `pr#14952 <https://github.com/ceph/ceph/pull/14952>`_, Kefu Chai)
* mgr: drop repeated log info. and unnecessary write permission (`pr#15896 <https://github.com/ceph/ceph/pull/15896>`_, Yan Jun)
* mgr: enable ceph_send_command() to send pg command (`pr#15865 <https://github.com/ceph/ceph/pull/15865>`_, Kefu Chai)
* mgr: fix bugs in init, beacons (`issue#19516 <http://tracker.ceph.com/issues/19516>`_, `issue#19502 <http://tracker.ceph.com/issues/19502>`_, `pr#14374 <https://github.com/ceph/ceph/pull/14374>`_, Sage Weil)
* mgr: fix crash on missing 'ceph_version' in daemon metadata (fixes #18764) (`issue#18764 <http://tracker.ceph.com/issues/18764>`_, `pr#14129 <https://github.com/ceph/ceph/pull/14129>`_, Tim Serong)
* mgr: fix crash on set_config from python module with insufficient caps (`issue#19629 <http://tracker.ceph.com/issues/19629>`_, `pr#14706 <https://github.com/ceph/ceph/pull/14706>`_, Tim Serong)
* mgr: fix lock cycle (`pr#16508 <https://github.com/ceph/ceph/pull/16508>`_, Sage Weil)
* mgr: fix metadata handling from old MDS daemons (`pr#14161 <https://github.com/ceph/ceph/pull/14161>`_, John Spray)
* mgr: fix MgrStandby eating messages (`pr#15716 <https://github.com/ceph/ceph/pull/15716>`_, John Spray)
* mgr: fix python module teardown & add tests (`issue#19407 <http://tracker.ceph.com/issues/19407>`_, `issue#19412 <http://tracker.ceph.com/issues/19412>`_, `issue#19258 <http://tracker.ceph.com/issues/19258>`_, `pr#14232 <https://github.com/ceph/ceph/pull/14232>`_, John Spray)
* mgr: fix session leak (`issue#19591 <http://tracker.ceph.com/issues/19591>`_, `pr#14720 <https://github.com/ceph/ceph/pull/14720>`_, Sage Weil)
* mgr: fix several init/re-init bugs (`issue#19491 <http://tracker.ceph.com/issues/19491>`_, `pr#14328 <https://github.com/ceph/ceph/pull/14328>`_, Sage Weil)
* mgr: handle "module.set_config(.., None)" correctly (`pr#16749 <https://github.com/ceph/ceph/pull/16749>`_, Kefu Chai)
* mgr: increase debug level for ticks 0 -> 10 (`pr#16301 <https://github.com/ceph/ceph/pull/16301>`_, Dan Mick)
* mgr: load modules in separate python sub-interpreters (`pr#14971 <https://github.com/ceph/ceph/pull/14971>`_, Tim Serong)
* mgr: luminous: mgr: add missing call to pick_addresses (`issue#20955 <http://tracker.ceph.com/issues/20955>`_, `issue#21049 <http://tracker.ceph.com/issues/21049>`_, `pr#17173 <https://github.com/ceph/ceph/pull/17173>`_, John Spray)
* mgr: Make stats period configurable (`issue#17449 <http://tracker.ceph.com/issues/17449>`_, `pr#12732 <https://github.com/ceph/ceph/pull/12732>`_, liuchang0812)
* mgr: Mark session connections down on shutdown (`issue#19900 <http://tracker.ceph.com/issues/19900>`_, `pr#15192 <https://github.com/ceph/ceph/pull/15192>`_, Brad Hubbard)
* mgr: mgr/ClusterState: do not mangle PGMap outside of Incremental (`issue#20208 <http://tracker.ceph.com/issues/20208>`_, `pr#16262 <https://github.com/ceph/ceph/pull/16262>`_, Sage Weil)
* mgr: mgr/DaemonServer.cc: log daemon type string as well as id (`pr#15560 <https://github.com/ceph/ceph/pull/15560>`_, Dan Mick)
* mgr: mgr/dashboard: add OSD list view (`pr#16373 <https://github.com/ceph/ceph/pull/16373>`_, John Spray)
* mgr: mgr/dashboard: fix type error in get_rate function (`issue#20276 <http://tracker.ceph.com/issues/20276>`_, `pr#15668 <https://github.com/ceph/ceph/pull/15668>`_, liuchang0812)
* mgr: mgr/dashboard: load log lines on startup, split out audit log (`pr#15709 <https://github.com/ceph/ceph/pull/15709>`_, John Spray)
* mgr: mgr/MgrClient: fix reconnect event leak (`issue#19580 <http://tracker.ceph.com/issues/19580>`_, `pr#14431 <https://github.com/ceph/ceph/pull/14431>`_, Sage Weil)
* mgr: mgr/MgrStandby: prevent use-after-free on just-shut-down Mgr (`issue#19595 <http://tracker.ceph.com/issues/19595>`_, `pr#15297 <https://github.com/ceph/ceph/pull/15297>`_, Sage Weil)
* mgr: mgr/MgrStandby: respawn when deactivated (`issue#19595 <http://tracker.ceph.com/issues/19595>`_, `issue#19549 <http://tracker.ceph.com/issues/19549>`_, `pr#15557 <https://github.com/ceph/ceph/pull/15557>`_, Sage Weil)
* mgr: mgr_module interface to report health alerts (`pr#16487 <https://github.com/ceph/ceph/pull/16487>`_, Sage Weil)
* mgr: mgr,osd:  ceph-mgr --help, unify usage text of other daemons (`pr#15176 <https://github.com/ceph/ceph/pull/15176>`_, Tim Serong)
* mgr: mgr/PyState: shut up about get_config on nonexistent keys (`pr#16641 <https://github.com/ceph/ceph/pull/16641>`_, Sage Weil)
* mgr: mgr/status: row has incorrect number of values (`issue#20750 <http://tracker.ceph.com/issues/20750>`_, `pr#16529 <https://github.com/ceph/ceph/pull/16529>`_, liuchang0812)
* mgr: Misc. bug fixes (`issue#18994 <http://tracker.ceph.com/issues/18994>`_, `pr#14883 <https://github.com/ceph/ceph/pull/14883>`_, John Spray)
* mgr: mkdir bootstrap-mgr (`pr#14824 <https://github.com/ceph/ceph/pull/14824>`_, huanwen ren)
* mgr: mon/mgr: add detail error infomation (`pr#16048 <https://github.com/ceph/ceph/pull/16048>`_, Yan Jun)
* mgr,mon: mgr,mon: debug init and mgrdigest subscriptions (`issue#20633 <http://tracker.ceph.com/issues/20633>`_, `pr#16351 <https://github.com/ceph/ceph/pull/16351>`_, Sage Weil)
* mgr: mon/MgrMonitor: fix standby addition to mgrmap (`issue#20647 <http://tracker.ceph.com/issues/20647>`_, `pr#16397 <https://github.com/ceph/ceph/pull/16397>`_, Sage Weil)
* mgr,mon: mon/AuthMonitor: generate bootstrap-mgr key on upgrade (`issue#20666 <http://tracker.ceph.com/issues/20666>`_, `pr#16395 <https://github.com/ceph/ceph/pull/16395>`_, Joao Eduardo Luis)
* mgr,mon: mon,mgr: extricate PGmap from monitor (`issue#20067 <http://tracker.ceph.com/issues/20067>`_, `issue#20174 <http://tracker.ceph.com/issues/20174>`_, `issue#20050 <http://tracker.ceph.com/issues/20050>`_, `pr#15073 <https://github.com/ceph/ceph/pull/15073>`_, Kefu Chai, Sage Weil, Greg Farnum)
* mgr,mon: mon/MgrMonitor: add 'mgr dump [epoch]' command (`pr#15158 <https://github.com/ceph/ceph/pull/15158>`_, Sage Weil)
* mgr,mon: mon/MgrMonitor: only propose if we updated (`pr#14645 <https://github.com/ceph/ceph/pull/14645>`_, Sage Weil)
* mgr,mon: mon/MgrMonitor: reset mgrdigest timer with new subscription (`issue#20633 <http://tracker.ceph.com/issues/20633>`_, `pr#16582 <https://github.com/ceph/ceph/pull/16582>`_, Sage Weil)
* mgr,mon: mon,mgr: move reweight-by-\* to mgr (`pr#14404 <https://github.com/ceph/ceph/pull/14404>`_, Kefu Chai)
* mgr,mon: mon,mgr: print pgmap reports to debug (not cluster) log (`pr#15740 <https://github.com/ceph/ceph/pull/15740>`_, Sage Weil)
* mgr,mon: mon,mgr: trim osdmap without the help of pgmap (`pr#14504 <https://github.com/ceph/ceph/pull/14504>`_, Kefu Chai)
* mgr: move 'osd perf' and 'osd blocked-by' to mgr (`pr#14303 <https://github.com/ceph/ceph/pull/14303>`_, Sage Weil)
* mgr: move "osd pool stats" to mgr (`pr#14365 <https://github.com/ceph/ceph/pull/14365>`_, Kefu Chai)
* mgr: optimization some judgment and adjust the debug remove value in register_new_pgs (`pr#14046 <https://github.com/ceph/ceph/pull/14046>`_, song baisen)
* mgr: optimize DaemonStateIndex::cull() a little bit (`pr#14967 <https://github.com/ceph/ceph/pull/14967>`_, Kefu Chai)
* mgr: pass through cluster log to plugins (`pr#13690 <https://github.com/ceph/ceph/pull/13690>`_, John Spray)
* mgr: perf schema fns/change notification and Prometheus plugin (`pr#16406 <https://github.com/ceph/ceph/pull/16406>`_, Dan Mick)
* mgr: print a more helpful error message for when users lack mgr ceph caps (`issue#20296 <http://tracker.ceph.com/issues/20296>`_, `pr#15697 <https://github.com/ceph/ceph/pull/15697>`_, Greg Farnum)
* mgr,pybind: luminous: mgr/dashboard: fix duplicate images listed on iSCSI status page (`issue#21017 <http://tracker.ceph.com/issues/21017>`_, `pr#17282 <https://github.com/ceph/ceph/pull/17282>`_, Jason Dillaman)
* mgr: pybind/mgr/dashboard: bind to :: by default (`pr#16223 <https://github.com/ceph/ceph/pull/16223>`_, Sage Weil)
* mgr: pybind/mgr/dashboard: monkeypatch os.exit to stop cherrypy from taking down mgr (`issue#20216 <http://tracker.ceph.com/issues/20216>`_, `pr#15588 <https://github.com/ceph/ceph/pull/15588>`_, Sage Weil)
* mgr: pybind/mgr: Delete `rest` module (`pr#15429 <https://github.com/ceph/ceph/pull/15429>`_, John Spray)
* mgr: pybind/mgr/rest: completely terminate cherrypy in shutdown (`pr#14995 <https://github.com/ceph/ceph/pull/14995>`_, Tim Serong)
* mgr: pybind/mgr/rest: don't set timezone to Chicago (`pr#14184 <https://github.com/ceph/ceph/pull/14184>`_, Tim Serong)
* mgr: pybind/mgr/restful: improve cert handling; work with vstart (`pr#15405 <https://github.com/ceph/ceph/pull/15405>`_, Sage Weil)
* mgr: pybind/mgr/zabbix: fix health in non-compat mode (`issue#20767 <http://tracker.ceph.com/issues/20767>`_, `pr#16580 <https://github.com/ceph/ceph/pull/16580>`_, Sage Weil)
* mgr,pybind,rbd: mgr/dashboard: show rbd image features (`pr#16468 <https://github.com/ceph/ceph/pull/16468>`_, Yanhu Cao)
* mgr: raise python exception on failure in send_command() (`pr#15704 <https://github.com/ceph/ceph/pull/15704>`_, Kefu Chai)
* mgr,rbd: mgr/dashboard: RBD iSCSI daemon status page (`pr#16547 <https://github.com/ceph/ceph/pull/16547>`_, Jason Dillaman)
* mgr,rbd: mgr/dashboard: rbd mirroring status page (`pr#16360 <https://github.com/ceph/ceph/pull/16360>`_, Jason Dillaman)
* mgr,rbd: pybind/mgr/dashboard: initial block integration (`pr#15521 <https://github.com/ceph/ceph/pull/15521>`_, Jason Dillaman)
* mgr: redirect python stdout,stderr to ceph log (`pr#14189 <https://github.com/ceph/ceph/pull/14189>`_, Kefu Chai, Tim Serong, Dan Mick)
* mgr: release allocated PyString (`pr#14716 <https://github.com/ceph/ceph/pull/14716>`_, Kefu Chai)
* mgr: remove default cert; disable both restful and dashboard by default (`pr#15601 <https://github.com/ceph/ceph/pull/15601>`_, Boris Ranto, Sage Weil)
* mgr: remove non-existent MDS daemons from FSMap (`issue#17453 <http://tracker.ceph.com/issues/17453>`_, `pr#14937 <https://github.com/ceph/ceph/pull/14937>`_, Spandan Kumar Sahu)
* mgr: remove unused function declarations (`pr#14366 <https://github.com/ceph/ceph/pull/14366>`_, Wei Jin)
* mgr: rm nonused main function (`pr#14313 <https://github.com/ceph/ceph/pull/14313>`_, Wei Jin)
* mgr: shutdown py_modules in Mgr::shutdown() (`issue#19258 <http://tracker.ceph.com/issues/19258>`_, `pr#14078 <https://github.com/ceph/ceph/pull/14078>`_, Kefu Chai)
* mgr,tests: qa/suites: move mgr tests into rados suite (`pr#14687 <https://github.com/ceph/ceph/pull/14687>`_, John Spray)
* mgr,tests: qa/upgrade/jewel-x/point-to-point: add a mgr during final upgrade (`pr#15637 <https://github.com/ceph/ceph/pull/15637>`_, Sage Weil)
* mgr: use unique_ptr for MgrStandby::active_mgr (`pr#13667 <https://github.com/ceph/ceph/pull/13667>`_, John Spray)
* mgr: various cleanups (`pr#14802 <https://github.com/ceph/ceph/pull/14802>`_, Kefu Chai)
* mgr: vstart.sh: fix mgr vs restful command startup race (`pr#16564 <https://github.com/ceph/ceph/pull/16564>`_, Sage Weil)
* mgr: Zabbix monitoring module (`pr#16019 <https://github.com/ceph/ceph/pull/16019>`_, Wido den Hollander)
* misc: fix code typos in header files (`pr#12716 <https://github.com/ceph/ceph/pull/12716>`_, Xianxia Xiao)
* misc: kill clang warnings (`pr#14549 <https://github.com/ceph/ceph/pull/14549>`_, Kefu Chai)
* misc: Warning Elimination (`pr#14439 <https://github.com/ceph/ceph/pull/14439>`_, Adam C. Emerson)
* mon: add crush type down health warnings (`pr#14914 <https://github.com/ceph/ceph/pull/14914>`_, Neha Ojha)
* mon: added bootstrap-rbd auth profile (`pr#16633 <https://github.com/ceph/ceph/pull/16633>`_, Jason Dillaman)
* mon: add force-create-pg back (`issue#20605 <http://tracker.ceph.com/issues/20605>`_, `pr#16353 <https://github.com/ceph/ceph/pull/16353>`_, Kefu Chai)
* mon: add mgr metdata commands, and overall 'versions' command for all daemon versions (`pr#16460 <https://github.com/ceph/ceph/pull/16460>`_, Sage Weil)
* mon: add mon_debug_no_require_luminous (`pr#14490 <https://github.com/ceph/ceph/pull/14490>`_, Sage Weil)
* mon: Add override for FsNewHandler::handle() (`pr#15331 <https://github.com/ceph/ceph/pull/15331>`_, yonghengdexin735)
* mon: add override in headers (`pr#13693 <https://github.com/ceph/ceph/pull/13693>`_, liuchang0812)
* mon: add override in mon subsystem (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13440 <https://github.com/ceph/ceph/pull/13440>`_, liuchang0812)
* mon: add support public_bind_addr option (`pr#16189 <https://github.com/ceph/ceph/pull/16189>`_, Bassam Tabbara)
* mon: add warn info for osds were removed from osdmap but still kept in crushmap (`pr#12273 <https://github.com/ceph/ceph/pull/12273>`_, song baisen)
* mon: a few health fixes (`pr#16415 <https://github.com/ceph/ceph/pull/16415>`_, xie xingguo)
* mon: a few more upmap (and other) fixes (`pr#16239 <https://github.com/ceph/ceph/pull/16239>`_, xie xingguo)
* mon: avoid segfault in wait_auth_rotating (`issue#19566 <http://tracker.ceph.com/issues/19566>`_, `pr#14430 <https://github.com/ceph/ceph/pull/14430>`_, John Spray)
* mon: avoid start election twice when quorum enter (`pr#10150 <https://github.com/ceph/ceph/pull/10150>`_, song baisen)
* mon: check is_shutdown() in timer callbacks (`issue#19825 <http://tracker.ceph.com/issues/19825>`_, `pr#14919 <https://github.com/ceph/ceph/pull/14919>`_, Kefu Chai)
* mon: clean up in ceph_mon.cc (`pr#14102 <https://github.com/ceph/ceph/pull/14102>`_, huanwen ren)
* mon: clean up some osdmon/pgmon interactions (`pr#12403 <https://github.com/ceph/ceph/pull/12403>`_, Sage Weil)
* mon: cleanups (`pr#15272 <https://github.com/ceph/ceph/pull/15272>`_, Kefu Chai)
* mon: collect mon metdata as part of the election (`issue#20434 <http://tracker.ceph.com/issues/20434>`_, `pr#16148 <https://github.com/ceph/ceph/pull/16148>`_, Sage Weil)
* mon: common/config_opts.h: kill mon_pg_create_interval (`pr#13800 <https://github.com/ceph/ceph/pull/13800>`_, xie xingguo)
* mon: 'config-key put' -> 'config-key set' (`pr#16569 <https://github.com/ceph/ceph/pull/16569>`_, Sage Weil)
* mon:  crush straw_calc_version value is 0 or 1 not 0 to 2 (`pr#13554 <https://github.com/ceph/ceph/pull/13554>`_, song baisen)
* mon: debug session feature tracking (`issue#20475 <http://tracker.ceph.com/issues/20475>`_, `pr#16128 <https://github.com/ceph/ceph/pull/16128>`_, Sage Weil)
* mon: delete unused config opts of mon_sync_fs_threshold (`pr#15676 <https://github.com/ceph/ceph/pull/15676>`_, linbing)
* mon: delete useless function definition (`pr#15188 <https://github.com/ceph/ceph/pull/15188>`_, shiqi)
* mon: detect existing fs and duplicate name earlier (`issue#18964 <http://tracker.ceph.com/issues/18964>`_, `pr#13471 <https://github.com/ceph/ceph/pull/13471>`_, Patrick Donnelly)
* mon: DIVIDE_BY_ZERO in PGMapDigest::dump_pool_stats_full() (`pr#15622 <https://github.com/ceph/ceph/pull/15622>`_, Jos Collin)
* mon: Division by zero in PGMapDigest::dump_pool_stats_full() (`pr#15901 <https://github.com/ceph/ceph/pull/15901>`_, Jos Collin)
* mon: do crushtool test with fork and timeout, but w/o exec of crushtool (`issue#19964 <http://tracker.ceph.com/issues/19964>`_, `pr#16025 <https://github.com/ceph/ceph/pull/16025>`_, Sage Weil)
* mon: do not dereference empty mgr_commands (`pr#16501 <https://github.com/ceph/ceph/pull/16501>`_, Sage Weil)
* mon: do not prime_pg_temp creating pgs; clean up pg create conditions (`issue#19826 <http://tracker.ceph.com/issues/19826>`_, `pr#14913 <https://github.com/ceph/ceph/pull/14913>`_, Sage Weil)
* mon: don't call propose_pending in prepare_update() (`issue#19738 <http://tracker.ceph.com/issues/19738>`_, `pr#14711 <https://github.com/ceph/ceph/pull/14711>`_, John Spray)
* mon: don't kill MDSs unless some beacons are getting through (`issue#19706 <http://tracker.ceph.com/issues/19706>`_, `pr#15308 <https://github.com/ceph/ceph/pull/15308>`_, John Spray)
* mon: don't prefix mgr summary with epoch number (`pr#15512 <https://github.com/ceph/ceph/pull/15512>`_, John Spray)
* mon: don't set last_osd_report when the pg stats msg is ignored (`pr#12975 <https://github.com/ceph/ceph/pull/12975>`_, Zhiqiang Wang)
* mon: drop useless assignment statements (`pr#13958 <https://github.com/ceph/ceph/pull/13958>`_, wangzhengyong)
* mon: emit cluster log messages on MDS health changes (`issue#19551 <http://tracker.ceph.com/issues/19551>`_, `pr#14398 <https://github.com/ceph/ceph/pull/14398>`_, John Spray)
* mon: enable luminous monmap feature on full quorum (`pr#13379 <https://github.com/ceph/ceph/pull/13379>`_, Joao Eduardo Luis)
* mon: extensible output format for health checks (`pr#16701 <https://github.com/ceph/ceph/pull/16701>`_, John Spray)
* mon: Filter `log last` output by severity and channel (`pr#15924 <https://github.com/ceph/ceph/pull/15924>`_, John Spray)
* mon: fix accesing pending_fsmap from peon (`issue#20040 <http://tracker.ceph.com/issues/20040>`_, `pr#15213 <https://github.com/ceph/ceph/pull/15213>`_, John Spray)
* mon: fix a few bugs with the osd health reporting (`pr#15179 <https://github.com/ceph/ceph/pull/15179>`_, Sage Weil)
* mon: fix a few nits (`pr#12670 <https://github.com/ceph/ceph/pull/12670>`_, Sage Weil)
* mon: Fix deep_age copy paste error (`pr#16434 <https://github.com/ceph/ceph/pull/16434>`_, Brad Hubbard)
* mon: Fixed typo in function comment blocks and in other comments (`pr#15304 <https://github.com/ceph/ceph/pull/15304>`_, linbing)
* mon: Fixed typo in @post of _active() (`pr#15191 <https://github.com/ceph/ceph/pull/15191>`_, Linbing)
* mon: fix force_pg_create pg stuck in creating bug (`issue#18298 <http://tracker.ceph.com/issues/18298>`_, `pr#12539 <https://github.com/ceph/ceph/pull/12539>`_, Sage Weil)
* mon: fix hang on deprecated/removed 'pg set_full_ratio' commands (`issue#20600 <http://tracker.ceph.com/issues/20600>`_, `pr#16300 <https://github.com/ceph/ceph/pull/16300>`_, Sage Weil)
* mon: fix hiding mdsmonitor informative strings (`issue#16709 <http://tracker.ceph.com/issues/16709>`_, `pr#13904 <https://github.com/ceph/ceph/pull/13904>`_, John Spray)
* mon: fix kvstore type in mon compact command (`pr#15954 <https://github.com/ceph/ceph/pull/15954>`_, liuchang0812)
* mon: fix legacy health checks in 'ceph status' during upgrade; fix jewel-x upgrade combo (`pr#17176 <https://github.com/ceph/ceph/pull/17176>`_, Sage Weil)
* mon: fix mon_keyvaluedb application (`pr#15059 <https://github.com/ceph/ceph/pull/15059>`_, Sage Weil)
* mon: Fix output text and doc (`pr#16367 <https://github.com/ceph/ceph/pull/16367>`_, Yan Jun)
* mon: fix prime_pg_temp overrun (`issue#19874 <http://tracker.ceph.com/issues/19874>`_, `pr#14979 <https://github.com/ceph/ceph/pull/14979>`_, Sage Weil)
* mon: Fix status output warning for mon_warn_osd_usage_min_max_delta (`issue#20544 <http://tracker.ceph.com/issues/20544>`_, `pr#16220 <https://github.com/ceph/ceph/pull/16220>`_, David Zafman)
* mon: fix synchronise pgmap with others (`pr#14418 <https://github.com/ceph/ceph/pull/14418>`_, song baisen, z09440)
* mon: fix wrongly delete routed pgstats op (`issue#18458 <http://tracker.ceph.com/issues/18458>`_, `pr#12784 <https://github.com/ceph/ceph/pull/12784>`_, Mingxin Liu)
* mon: fix wrong mon-num counting logic of 'ceph features' command (`pr#17172 <https://github.com/ceph/ceph/pull/17172>`_, xie xingguo)
* mon: handle cases where store->get() may return error (`issue#19601 <http://tracker.ceph.com/issues/19601>`_, `pr#14678 <https://github.com/ceph/ceph/pull/14678>`_, Jos Collin)
* mon: include device class in tree view; hide shadow hierarchy (`pr#16016 <https://github.com/ceph/ceph/pull/16016>`_, Sage Weil)
* mon: Incorrect expression in PGMap::get_health() (`pr#15648 <https://github.com/ceph/ceph/pull/15648>`_, Jos Collin)
* mon: in output of "ceph osd df tree", display "-", not "0",  for pg amount of a bucket (`pr#13015 <https://github.com/ceph/ceph/pull/13015>`_, Chuanhong Hong)
* mon: it's no need to get pg action_primary osd twice in pg scrub (`pr#15313 <https://github.com/ceph/ceph/pull/15313>`_, linbing)
* mon: '\* list' -> '\* ls' (`pr#16423 <https://github.com/ceph/ceph/pull/16423>`_, Sage Weil)
* mon: load mgr commands at runtime (`pr#16028 <https://github.com/ceph/ceph/pull/16028>`_, John Spray, Sage Weil)
* mon: logclient: use the seq id of the 1st log entry when resetting session (`issue#19427 <http://tracker.ceph.com/issues/19427>`_, `pr#14927 <https://github.com/ceph/ceph/pull/14927>`_, Kefu Chai)
* mon: Log errors at startup (`issue#14088 <http://tracker.ceph.com/issues/14088>`_, `pr#15723 <https://github.com/ceph/ceph/pull/15723>`_, Brad Hubbard)
* mon: luminous: mon/MonCommands: fix copy-and-paste error (`pr#17274 <https://github.com/ceph/ceph/pull/17274>`_, xie xingguo)
* mon: maintain the "cluster" PerfCounters when using ceph-mgr (`issue#20562 <http://tracker.ceph.com/issues/20562>`_, `pr#16249 <https://github.com/ceph/ceph/pull/16249>`_, Greg Farnum)
* mon: mark `osd create` as deprecated (`pr#15641 <https://github.com/ceph/ceph/pull/15641>`_, Joao Eduardo Luis)
* mon: mon,crush: create crush rules using device classes for replicated and ec pools via cli (`pr#16027 <https://github.com/ceph/ceph/pull/16027>`_, Sage Weil)
* mon: mon/HealthMonitor: avoid sending unnecessary MMonHealthChecks to leader (`pr#16478 <https://github.com/ceph/ceph/pull/16478>`_, xie xingguo)
* mon: mon/HealthMonitor: trigger a proposal if stat updated (`pr#16477 <https://github.com/ceph/ceph/pull/16477>`_, Kefu Chai)
* mon: mon/LogMonitor: don't read list's end() for log last (`pr#16376 <https://github.com/ceph/ceph/pull/16376>`_, Joao Eduardo Luis)
* mon: mon/MDSMonitor: close object section of formatter (`pr#16516 <https://github.com/ceph/ceph/pull/16516>`_, Chang Liu)
* mon: mon/MDSMonitor: remove create_new_fs from header (`pr#14019 <https://github.com/ceph/ceph/pull/14019>`_, Henrik Korkuc)
* mon: mon/MgrMonitor: only induce mgr epoch shortly after mkfs (`pr#16356 <https://github.com/ceph/ceph/pull/16356>`_, Sage Weil)
* mon: mon/MgrMonitor: send digests only if is_active() (`pr#15109 <https://github.com/ceph/ceph/pull/15109>`_, Kefu Chai)
* mon: mon/MgrStatMonitor: do not crash on luminous dev version upgrades (`pr#16287 <https://github.com/ceph/ceph/pull/16287>`_, Sage Weil)
* mon: mon/MonClient: cancel pending commands on shutdown (`issue#20051 <http://tracker.ceph.com/issues/20051>`_, `pr#15227 <https://github.com/ceph/ceph/pull/15227>`_, Kefu Chai, Sage Weil)
* mon: mon/MonClient: make get_mon_log_message() atomic (`issue#19427 <http://tracker.ceph.com/issues/19427>`_, `pr#14422 <https://github.com/ceph/ceph/pull/14422>`_, Kefu Chai)
* mon: mon/MonClient: random all ranks then pick first_n (`pr#13479 <https://github.com/ceph/ceph/pull/13479>`_, Mingxin Liu)
* mon: mon/Monitor.h: add const to member function (`pr#10412 <https://github.com/ceph/ceph/pull/10412>`_, Michal Jarzabek)
* mon: mon/Monitor: recreate mon session if features changed (`issue#20433 <http://tracker.ceph.com/issues/20433>`_, `pr#16230 <https://github.com/ceph/ceph/pull/16230>`_, Joao Eduardo Luis)
* mon: {mon,osd,mds} {versions,count-metadata} (`pr#15436 <https://github.com/ceph/ceph/pull/15436>`_, Sage Weil)
* mon: mon/OSDMonitor: a couple of upmap and other fixes (`pr#15917 <https://github.com/ceph/ceph/pull/15917>`_, xie xingguo)
* mon: mon/OSDMonitor: check get()'s return value instead of bl's length (`pr#14805 <https://github.com/ceph/ceph/pull/14805>`_, Kefu Chai)
* mon: mon/OSDMonitor: check last_osd_report only when the whole cluster is lu… (`pr#14294 <https://github.com/ceph/ceph/pull/14294>`_, Kefu Chai)
* mon: mon/OSDMonitor: Clean up: delete extra S signature for plural (`pr#14174 <https://github.com/ceph/ceph/pull/14174>`_, Shinobu Kinjo)
* mon: mon/OSDMonitor: cleanup pending_created_pgs after done with it (`pr#14898 <https://github.com/ceph/ceph/pull/14898>`_, Kefu Chai)
* mon: mon/OSDMonitor: do not alter the "created" epoch of a pg (`issue#19787 <http://tracker.ceph.com/issues/19787>`_, `pr#14849 <https://github.com/ceph/ceph/pull/14849>`_, Kefu Chai)
* mon: mon/OSDMonitor: ensure UP is not set for newly-created OSDs (`issue#20751 <http://tracker.ceph.com/issues/20751>`_, `pr#16534 <https://github.com/ceph/ceph/pull/16534>`_, Sage Weil)
* mon: mon/OSDMonitor: fix dividing by zero in OSDUtilizationDumper (`pr#13531 <https://github.com/ceph/ceph/pull/13531>`_, Mingxin Liu)
* mon: mon/OSDMonitor: fix output func name in can_mark_out (`pr#14758 <https://github.com/ceph/ceph/pull/14758>`_, xie xingguo)
* mon: mon/OSDMonitor: fix process osd failure (`pr#12938 <https://github.com/ceph/ceph/pull/12938>`_, Mingxin Liu)
* mon: mon/OSDMonitor: guard 'osd crush set-device-class' (`pr#16217 <https://github.com/ceph/ceph/pull/16217>`_, Sage Weil)
* mon: mon/OSDMonitor: increase last_epoch_clean's lower bound if possible (`pr#14855 <https://github.com/ceph/ceph/pull/14855>`_, Kefu Chai)
* mon: mon/OSDMonitor: issue pool application related warning (`pr#16520 <https://github.com/ceph/ceph/pull/16520>`_, xie xingguo)
* mon: mon/OSDMonitor: "osd crush class rename" support (`pr#15875 <https://github.com/ceph/ceph/pull/15875>`_, xie xingguo)
* mon: mon/OSDMonitor: remove trivial PGMap dependency for 'osd primary-temp' command (`pr#13616 <https://github.com/ceph/ceph/pull/13616>`_, Sage Weil)
* mon: mon/OSDMonitor: remove zeroed new_state updates (`issue#20751 <http://tracker.ceph.com/issues/20751>`_, `pr#16518 <https://github.com/ceph/ceph/pull/16518>`_, Sage Weil)
* mon: mon/OSDMonitor: sanity check osd before performing 'osd purge' (`pr#16838 <https://github.com/ceph/ceph/pull/16838>`_, xie xingguo)
* mon: mon/OSDMonitor: some cleanup for reweight-by-pg (`pr#13462 <https://github.com/ceph/ceph/pull/13462>`_, Haodong Tang)
* mon: mon/OSDMonitor: spinlock -> std::mutex (`pr#14269 <https://github.com/ceph/ceph/pull/14269>`_, Sage Weil)
* mon: mon/OSDMonitor: tolerate upgrade from post-kraken dev cluster (`pr#14442 <https://github.com/ceph/ceph/pull/14442>`_, Sage Weil)
* mon: mon/OSDMonitor: transit creating_pgs from pgmap when upgrading (`issue#19584 <http://tracker.ceph.com/issues/19584>`_, `pr#14551 <https://github.com/ceph/ceph/pull/14551>`_, Kefu Chai)
* mon: mon/OSDMonitor: two pool opts related fix (`pr#15968 <https://github.com/ceph/ceph/pull/15968>`_, xie xingguo)
* mon: mon/OSDMonitor: update creating epoch if target osd changed (`issue#19515 <http://tracker.ceph.com/issues/19515>`_, `pr#14386 <https://github.com/ceph/ceph/pull/14386>`_, Kefu Chai)
* mon: mon/OSDMonitor: update creating_pgs using pending_creatings (`issue#19814 <http://tracker.ceph.com/issues/19814>`_, `pr#14897 <https://github.com/ceph/ceph/pull/14897>`_, Kefu Chai)
* mon: mon/OSDMonitor: update pg_creatings even the new acting set is empty (`issue#19744 <http://tracker.ceph.com/issues/19744>`_, `pr#14730 <https://github.com/ceph/ceph/pull/14730>`_, Kefu Chai)
* mon: mon/PaxosService: use `__func__` instead of hard code function name (`pr#15863 <https://github.com/ceph/ceph/pull/15863>`_, Yanhu Cao)
* mon: mon/PGMap: add up_primary pg number field for pg-dump cmd (`pr#13451 <https://github.com/ceph/ceph/pull/13451>`_, xie xingguo)
* mon: mon/PGMap.cc: fix "osd_epochs" section of dump_basic (`pr#14996 <https://github.com/ceph/ceph/pull/14996>`_, xie xingguo)
* mon: mon/PGMap: make si units more readable in PGMap summary (`pr#14185 <https://github.com/ceph/ceph/pull/14185>`_, liuhong)
* mon: mon/PGMap: remove skewed utilizatoin warning (`issue#20730 <http://tracker.ceph.com/issues/20730>`_, `pr#16461 <https://github.com/ceph/ceph/pull/16461>`_, Sage Weil)
* mon: mon/PGMap: show %used in formatted output (`issue#20123 <http://tracker.ceph.com/issues/20123>`_, `pr#15387 <https://github.com/ceph/ceph/pull/15387>`_, Joao Eduardo Luis)
* mon: mon/PGMonitor: clean up min/max span warning (`pr#14611 <https://github.com/ceph/ceph/pull/14611>`_, Sage Weil)
* mon: mon/PGMonitor: fix description for ceph pg ls (`pr#12807 <https://github.com/ceph/ceph/pull/12807>`_, runsisi)
* mon: mon/PGMonitor: rm nonused function (`pr#14033 <https://github.com/ceph/ceph/pull/14033>`_, Wei Jin)
* mon: move 'pg map' to OSDMonitor (`pr#14559 <https://github.com/ceph/ceph/pull/14559>`_, Sage Weil)
* mon: no delay for single message MSG_ALIVE and MSG_PGTEMP (`pr#12107 <https://github.com/ceph/ceph/pull/12107>`_, yaoning)
* mon: optracker's initiated_at timestamp should not be NULL (`pr#12826 <https://github.com/ceph/ceph/pull/12826>`_, Mingxin Liu)
* mon: osd crush set crushmap need sanity check (`issue#19302 <http://tracker.ceph.com/issues/19302>`_, `pr#14029 <https://github.com/ceph/ceph/pull/14029>`_, Loic Dachary)
* mon: OSDMonitor add check only concern our self cluster command (`pr#10309 <https://github.com/ceph/ceph/pull/10309>`_, song baisen)
* mon/OSDMonitor: add plain output for "crush class ls-osd" command (`pr#17230 <https://github.com/ceph/ceph/pull/17230>`_, xie xingguo)
* mon/OSDMonitor: check creating_pgs.last_scan_epoch instead when sending creates (`issue#20785 <http://tracker.ceph.com/issues/20785>`_, `pr#17257 <https://github.com/ceph/ceph/pull/17257>`_, Kefu Chai)
* mon: OSDMonitor: check mon_max_pool_pg_num when set pool pg_num (`pr#16511 <https://github.com/ceph/ceph/pull/16511>`_, chenhg)
* mon/OSDMonitor: do not send_pg_creates with stale info (`issue#20785 <http://tracker.ceph.com/issues/20785>`_, `pr#17191 <https://github.com/ceph/ceph/pull/17191>`_, Kefu Chai)
* mon/OSDMonitor: fix improper input/testing range of crush somke testing (`pr#17232 <https://github.com/ceph/ceph/pull/17232>`_, xie xingguo)
* mon: osd/PGMonitor: always update pgmap with latest osdmap (`issue#19398 <http://tracker.ceph.com/issues/19398>`_, `pr#14777 <https://github.com/ceph/ceph/pull/14777>`_, Kefu Chai)
* mon/pgmap: add objects prefix for unfound type (`issue#21127 <http://tracker.ceph.com/issues/21127>`_, `pr#17264 <https://github.com/ceph/ceph/pull/17264>`_, huanwen ren, Sage Weil)
* mon/PGMap: fix "0 stuck requests are blocked > 4096 sec" warn (`pr#17215 <https://github.com/ceph/ceph/pull/17215>`_, xie xingguo)
* mon: PGMonitor add check only concern our self cluster command (`pr#9976 <https://github.com/ceph/ceph/pull/9976>`_, song baisen)
* mon: post-jewel cleanups (`pr#13150 <https://github.com/ceph/ceph/pull/13150>`_, Kefu Chai)
* mon: prime pg_temp and a few health warning fixes (`pr#16530 <https://github.com/ceph/ceph/pull/16530>`_, xie xingguo)
* mon: refactor MDSMonitor command handling (`pr#13581 <https://github.com/ceph/ceph/pull/13581>`_, John Spray)
* mon: Removed unnecessary function declaration in MDSMonitor.h (`pr#15374 <https://github.com/ceph/ceph/pull/15374>`_, yonghengdexin735)
* mon: remove the redudant jugement in paxosservice is_writeable function (`pr#10240 <https://github.com/ceph/ceph/pull/10240>`_, song baisen)
* mon: remove unnecessary function declaration (`pr#13762 <https://github.com/ceph/ceph/pull/13762>`_, liuchang0812)
* mon: replace osds with `osd destroy` and `osd new` (`pr#14074 <https://github.com/ceph/ceph/pull/14074>`_, Joao Eduardo Luis, Sage Weil)
* mon: restructure prime_pg_temp around a full pg mapping calculated on multiple CPUs (`pr#13207 <https://github.com/ceph/ceph/pull/13207>`_, Sage Weil)
* mon: revamp health check/warning system (`pr#15643 <https://github.com/ceph/ceph/pull/15643>`_, John Spray, Sage Weil)
* mon: revise "ceph status" output (`pr#15396 <https://github.com/ceph/ceph/pull/15396>`_, John Spray)
* mon: show class in 'osd crush tree' output; sort output (`pr#16740 <https://github.com/ceph/ceph/pull/16740>`_, Sage Weil)
* mon: show destroyed status in tree view; do not auto-out destroyed osds (`pr#16446 <https://github.com/ceph/ceph/pull/16446>`_, xie xingguo)
* mon: show inactive % in ceph status (`pr#14810 <https://github.com/ceph/ceph/pull/14810>`_, Sage Weil)
* mon: show io status quickly if no update in a long period (`pr#14176 <https://github.com/ceph/ceph/pull/14176>`_, Mingxin Liu)
* mon: show the leader info on mon stat command (`pr#14178 <https://github.com/ceph/ceph/pull/14178>`_, song baisen)
* mon: skip crush smoke test when running under valgrind (`issue#20602 <http://tracker.ceph.com/issues/20602>`_, `pr#16346 <https://github.com/ceph/ceph/pull/16346>`_, Sage Weil)
* mon: smooth io/recovery stats over longer period (`pr#13249 <https://github.com/ceph/ceph/pull/13249>`_, Sage Weil)
* mon: stop issuing not-[deep]-scrubbed warnings if disabled (`pr#16465 <https://github.com/ceph/ceph/pull/16465>`_, xie xingguo)
* mon: support pool application metadata key/values (`pr#15763 <https://github.com/ceph/ceph/pull/15763>`_, Jason Dillaman)
* mon,tests: qa/suites: add test exercising workunits/mon/auth_caps.sh (`pr#15754 <https://github.com/ceph/ceph/pull/15754>`_, Kefu Chai)
* mon,tests: test: Initialize pointer msg in MonClientHelper (`pr#16784 <https://github.com/ceph/ceph/pull/16784>`_, amitkuma)
* mon: Tidy up removal of debug mon features (`pr#14467 <https://github.com/ceph/ceph/pull/14467>`_, Brad Hubbard)
* mon: track features from connect clients, and use it to gate set-require-min-compat-client (`pr#15371 <https://github.com/ceph/ceph/pull/15371>`_, Sage Weil)
* mon: trim the creating_pgs after updating it with pgmap (`issue#20067 <http://tracker.ceph.com/issues/20067>`_, `pr#15318 <https://github.com/ceph/ceph/pull/15318>`_, Kefu Chai)
* mon: update mgrmap when active goes offline (`issue#19407 <http://tracker.ceph.com/issues/19407>`_, `pr#14220 <https://github.com/ceph/ceph/pull/14220>`_, John Spray)
* mon: Update OSDMon.cc comments (`pr#13750 <https://github.com/ceph/ceph/pull/13750>`_, Saumay Agrawal)
* mon: warn about using osd new instead of osd create (`pr#17302 <https://github.com/ceph/ceph/pull/17302>`_, Neha Ojha)
* msg: allow different ms type for cluster network and public network (`pr#12023 <https://github.com/ceph/ceph/pull/12023>`_, Haomai Wang)
* msg: always set header.version in encode_payload() (`issue#19939 <http://tracker.ceph.com/issues/19939>`_, `pr#16421 <https://github.com/ceph/ceph/pull/16421>`_, Kefu Chai)
* msg: client bind (`pr#12901 <https://github.com/ceph/ceph/pull/12901>`_, Zengran Zhang, Haomai Wang)
* msg: do not enable client-side binding by default (`issue#20049 <http://tracker.ceph.com/issues/20049>`_, `pr#15392 <https://github.com/ceph/ceph/pull/15392>`_, Jason Dillaman)
* msg: don't set msgr addr when disabing client bind (`pr#15243 <https://github.com/ceph/ceph/pull/15243>`_, Haomai Wang)
* msg: end parameter in entity_addr_t::parse is optional (`pr#13650 <https://github.com/ceph/ceph/pull/13650>`_, Mykola Golub)
* msg: Fix calls to Messenger::create with new parameter (`pr#13329 <https://github.com/ceph/ceph/pull/13329>`_, Sarit Zubakov)
* msg: Increase loglevels on some messages (`pr#14707 <https://github.com/ceph/ceph/pull/14707>`_, Willem Jan Withagen)
* msg: Initialize member variables in Infiniband (`pr#16781 <https://github.com/ceph/ceph/pull/16781>`_, amitkuma)
* msg: Initializing uninitialized members MMonGetVersion (`pr#16811 <https://github.com/ceph/ceph/pull/16811>`_, amitkuma)
* msg: Initializing uninitialized members MMonGetVersionReply (`pr#16813 <https://github.com/ceph/ceph/pull/16813>`_, amitkuma)
* msg: Initializing uninitialized members MMonPaxos (`pr#16814 <https://github.com/ceph/ceph/pull/16814>`_, amitkuma)
* msg: Initializing uninitialized members MMonProbe (`pr#16815 <https://github.com/ceph/ceph/pull/16815>`_, amitkuma)
* msg: Initializing uninitialized members module messages (`pr#16817 <https://github.com/ceph/ceph/pull/16817>`_, amitkuma)
* msg: Initializing uninitialized members MOSDAlive (`pr#16816 <https://github.com/ceph/ceph/pull/16816>`_, amitkuma)
* msg: make listen backlog an option, increase from 128 to 512 (`issue#20330 <http://tracker.ceph.com/issues/20330>`_, `pr#15743 <https://github.com/ceph/ceph/pull/15743>`_, Haomai Wang)
* msg: messages: coverity fixes (`pr#13473 <https://github.com/ceph/ceph/pull/13473>`_, Kefu Chai)
* msg: msg/async: avoid atomic variable overhead (`pr#12809 <https://github.com/ceph/ceph/pull/12809>`_, Wei Jin)
* msg: msg/async: cleanup code (`pr#13304 <https://github.com/ceph/ceph/pull/13304>`_, Jianpeng Ma)
* msg: msg/async: cleanups (`pr#12832 <https://github.com/ceph/ceph/pull/12832>`_, Wei Jin)
* msg: msg/async: fix file description leak in NetHandler (`pr#13271 <https://github.com/ceph/ceph/pull/13271>`_, liuchang0812)
* msg: msg/async: increase worker reference with local listen table enabled backend (`issue#20390 <http://tracker.ceph.com/issues/20390>`_, `pr#15897 <https://github.com/ceph/ceph/pull/15897>`_, Haomai Wang)
* msg: msg/async: Lower down the AsyncMessenger's standby warning from debug (`pr#15242 <https://github.com/ceph/ceph/pull/15242>`_, Pan Liu)
* msg: msg/AsyncMessenger: remove unused method (`pr#10125 <https://github.com/ceph/ceph/pull/10125>`_, Michal Jarzabek)
* msg: msg/async/net_handler: errno should be stored before calling next function (`pr#14985 <https://github.com/ceph/ceph/pull/14985>`_, Zhou Zhengping)
* msg: msg/async/rdma: check if exp verbs avail (`pr#13391 <https://github.com/ceph/ceph/pull/13391>`_, Oren Duer, Adir Lev)
* msg: msg/async/rdma: check if fin message completed (`pr#15624 <https://github.com/ceph/ceph/pull/15624>`_, Alexander Mikheev, Adir Lev)
* msg: msg/async/rdma: Data path fixes (`pr#15903 <https://github.com/ceph/ceph/pull/15903>`_, Adir lev)
* msg: msg/async/rdma: Debug prints for ibv* (`pr#14249 <https://github.com/ceph/ceph/pull/14249>`_, Amir Vadai)
* msg: msg/async/rdma: Device::last_poll_dev must be positive (`pr#14250 <https://github.com/ceph/ceph/pull/14250>`_, Amir Vadai)
* msg: msg/async/rdma: Fix broken compilation (`pr#13603 <https://github.com/ceph/ceph/pull/13603>`_, Sarit Zubakov)
* msg: msg/async/rdma: Fix memory leak of OSD (`pr#13101 <https://github.com/ceph/ceph/pull/13101>`_, Sarit Zubakov)
* msg: msg/async/rdma: fix outstanding queuepair when destruct RDMAStack (`pr#13905 <https://github.com/ceph/ceph/pull/13905>`_, Haomai Wang)
* msg: msg/async/rdma: fix RoCE v2 deafult value (`pr#12648 <https://github.com/ceph/ceph/pull/12648>`_, Adir Lev, Oren Duer)
* msg: msg/async/rdma: Fix small memory leaks detected by valgrind (`pr#14288 <https://github.com/ceph/ceph/pull/14288>`_, Amir Vadai)
* msg: msg/async/rdma: handle buffers after close msg (`pr#15749 <https://github.com/ceph/ceph/pull/15749>`_, DanielBar-On, Alexander Mikheev, Adir Lev)
* msg: msg/async/rdma: move active_queue_pairs perf counter dec to polling (`pr#13716 <https://github.com/ceph/ceph/pull/13716>`_, DanielBar-On)
* msg: msg/async/rdma: Print error only on ENOMEM (`pr#13538 <https://github.com/ceph/ceph/pull/13538>`_, Sarit Zubakov)
* msg: msg/async/rdma: RDMA-CM, Pass specific ConnMgr info in constructor (`pr#14409 <https://github.com/ceph/ceph/pull/14409>`_, Amir Vadai)
* msg: msg/async/rdma: register buffer as continuous (`pr#15967 <https://github.com/ceph/ceph/pull/15967>`_, Adir Lev)
* msg: msg/async/rdma: remove assert from ibv_dealloc_pd in ProtectionDomain (`pr#15832 <https://github.com/ceph/ceph/pull/15832>`_, DanielBar-On)
* msg: msg/async/rdma: update destructor message (`pr#13539 <https://github.com/ceph/ceph/pull/13539>`_, Sarit Zubakov)
* msg: msg/async/rdma: zero wqe inline (`pr#13392 <https://github.com/ceph/ceph/pull/13392>`_, Adir Lev)
* msg: msg/async: remove false alert "assert" (`pr#15288 <https://github.com/ceph/ceph/pull/15288>`_, Haomai Wang)
* msg: msg/async: remove useless close function (`pr#13286 <https://github.com/ceph/ceph/pull/13286>`_, liuchang0812)
* msg: msg/async: rm nonused thread variable in posixworker (`pr#12777 <https://github.com/ceph/ceph/pull/12777>`_, Wei Jin)
* msg: msg/async: use auto iterator having more simple code and good performance (`pr#16524 <https://github.com/ceph/ceph/pull/16524>`_, dingdangzhang)
* msg: msg/Messenger.cc: add std::move (`pr#9760 <https://github.com/ceph/ceph/pull/9760>`_, Michal Jarzabek)
* msg: msg/MOSDOpReply: fix missing trace decode (`pr#15999 <https://github.com/ceph/ceph/pull/15999>`_, Yan Jun)
* msg: msg/RDMA: Fix broken compilation due to new argument in net.connect() (`pr#13096 <https://github.com/ceph/ceph/pull/13096>`_, Amir Vadai)
* msg: msg/simple: Remove dead code in pipe.cc (`issue#12684 <http://tracker.ceph.com/issues/12684>`_, `pr#12601 <https://github.com/ceph/ceph/pull/12601>`_, Rishabh Kumar)
* msg: msg/simple: use my addr when setting sock priority (`issue#19801 <http://tracker.ceph.com/issues/19801>`_, `pr#14878 <https://github.com/ceph/ceph/pull/14878>`_, Kefu Chai)
* msg: no need to pass supported features to Messenger::Policy ctor (`pr#13785 <https://github.com/ceph/ceph/pull/13785>`_, Sage Weil)
* msg: QueueStrategy::wait() joins all threads (`issue#20534 <http://tracker.ceph.com/issues/20534>`_, `pr#16194 <https://github.com/ceph/ceph/pull/16194>`_, Casey Bodley)
* msg: Remove unused variable perf_counter in RDMAStack (`pr#16783 <https://github.com/ceph/ceph/pull/16783>`_, amitkuma)
* msg: Revert the change from assert(0)-> ceph_abort() where is not applicable (`pr#12930 <https://github.com/ceph/ceph/pull/12930>`_, Dave Chen)
* msg: src/msg/async/AsyncConnect.cc: Use of sizeof() on a Pointer Type (`pr#14773 <https://github.com/ceph/ceph/pull/14773>`_, Svyatoslav)
* msg: src/msg/async: Update fix broken compilation for Posix (`pr#14336 <https://github.com/ceph/ceph/pull/14336>`_, Sarit Zubakov)
* msg: src/msg/simple/Pipe.cc: Fix the inclusion of '}' (`pr#14843 <https://github.com/ceph/ceph/pull/14843>`_, Willem Jan Withagen)
* os/bluestore: print leaked extents to debug output (`pr#17303 <https://github.com/ceph/ceph/pull/17303>`_, Sage Weil)
* osd: add asock command to dump the scrub queue (`issue#17861 <http://tracker.ceph.com/issues/17861>`_, `pr#12728 <https://github.com/ceph/ceph/pull/12728>`_, liuchang0812)
* osd: add default_device_class to metadata (`pr#16634 <https://github.com/ceph/ceph/pull/16634>`_, Neha Ojha)
* osd: add dump filter for tracked ops (`pr#16561 <https://github.com/ceph/ceph/pull/16561>`_, Yan Jun)
* osd: add "heap \*" admin command (`issue#15475 <http://tracker.ceph.com/issues/15475>`_, `pr#13073 <https://github.com/ceph/ceph/pull/13073>`_, Jesse Williamson)
* osd: adding PerfCounters for backoff throttle (`pr#13017 <https://github.com/ceph/ceph/pull/13017>`_, Chuanhong Wang)
* osd:  add is_split check before _start_split (`pr#13307 <https://github.com/ceph/ceph/pull/13307>`_, song baisen)
* osd: add override in headers files (`pr#13962 <https://github.com/ceph/ceph/pull/13962>`_, liuchang0812)
* osd: add override in osd subsystem (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13439 <https://github.com/ceph/ceph/pull/13439>`_, liuchang0812)
* osd: Add recovery sleep configuration option for HDDs and SSDs (`pr#16328 <https://github.com/ceph/ceph/pull/16328>`_, Neha Ojha)
* osd: add snap trim reservation and re-implement osd_snap_trim_sleep (`pr#13594 <https://github.com/ceph/ceph/pull/13594>`_, Samuel Just)
* osd: adjust osd_min_pg_log_entries (`issue#21026 <http://tracker.ceph.com/issues/21026>`_, `pr#17202 <https://github.com/ceph/ceph/pull/17202>`_, J. Eric Ivancich)
* osd: allow client throttler to be adjusted on-fly, without restart (`issue#18791 <http://tracker.ceph.com/issues/18791>`_, `pr#13213 <https://github.com/ceph/ceph/pull/13213>`_, Piotr Dałek)
* osd: bail from _committed_osd_maps inside osd_lock (`issue#20273 <http://tracker.ceph.com/issues/20273>`_, `pr#15710 <https://github.com/ceph/ceph/pull/15710>`_, Sage Weil)
* osd: Calculate degraded and misplaced more accurately (`issue#18619 <http://tracker.ceph.com/issues/18619>`_, `pr#13031 <https://github.com/ceph/ceph/pull/13031>`_, David Zafman)
* osd: change a few messages at level 0 and 1; change default level to 1/5 (`pr#13407 <https://github.com/ceph/ceph/pull/13407>`_, Sage Weil)
* osd: Check for and automatically repair object info soid during scrub (`issue#20471 <http://tracker.ceph.com/issues/20471>`_, `pr#16052 <https://github.com/ceph/ceph/pull/16052>`_, David Zafman)
* osd: check fsid is normal before osd mkfs (`pr#13898 <https://github.com/ceph/ceph/pull/13898>`_, song baisen)
* osd: check queue_transaction return value (`pr#15873 <https://github.com/ceph/ceph/pull/15873>`_, zhanglei)
* osd: Check snapset for validity when selecting authoritative shard (`issue#20186 <http://tracker.ceph.com/issues/20186>`_, `issue#18409 <http://tracker.ceph.com/issues/18409>`_, `pr#15559 <https://github.com/ceph/ceph/pull/15559>`_, David Zafman)
* osd: Check whether journal is rotational or not (`pr#16614 <https://github.com/ceph/ceph/pull/16614>`_, Neha Ojha)
* osd: clarify REQUIRE_LUMINOUS error message (`pr#13363 <https://github.com/ceph/ceph/pull/13363>`_, Josh Durgin)
* osd: clean nonused work queue (`pr#14990 <https://github.com/ceph/ceph/pull/14990>`_, Wei Jin)
* osd: Cleanup-Updated OSDMap.cc with C++11 style range-for loops (`pr#14381 <https://github.com/ceph/ceph/pull/14381>`_, Jos Collin)
* osd: cleanup: use string & to avoid unnecessary copy (`pr#12336 <https://github.com/ceph/ceph/pull/12336>`_, Yunchuan Wen)
* osd: clear_queued_recovery() in on_shutdown() (`issue#20432 <http://tracker.ceph.com/issues/20432>`_, `pr#16093 <https://github.com/ceph/ceph/pull/16093>`_, Kefu Chai)
* osd: cmpext operator should ignore -ENOENT on read (`pr#16622 <https://github.com/ceph/ceph/pull/16622>`_, Jason Dillaman)
* osd: combine conditional statements (`pr#16391 <https://github.com/ceph/ceph/pull/16391>`_, Yan Jun)
* osd: combine unstable stats with info.stats when publish stats to osd (`pr#14060 <https://github.com/ceph/ceph/pull/14060>`_, Mingxin Liu)
* osd: compact osd feature (`issue#19592 <http://tracker.ceph.com/issues/19592>`_, `pr#16045 <https://github.com/ceph/ceph/pull/16045>`_, liuchang0812)
* osd: condition object_info_t encoding on required (not up) features (`issue#18644 <http://tracker.ceph.com/issues/18644>`_, `pr#13114 <https://github.com/ceph/ceph/pull/13114>`_, Sage Weil)
* osd: constify OpRequest::get_req(); fix a few cases of operator<< vs mutated message races (`pr#13545 <https://github.com/ceph/ceph/pull/13545>`_, Sage Weil)
* osd: correct comment of perfcounter cached_crc in code (`pr#13256 <https://github.com/ceph/ceph/pull/13256>`_, lvshuhua)
* osd: correct epoch setting of osd boot msg (`pr#12623 <https://github.com/ceph/ceph/pull/12623>`_, Mingxin Liu)
* osd: correct the func name in execute_ctx() log messages (`pr#13582 <https://github.com/ceph/ceph/pull/13582>`_, Gu Zhongyan)
* osd: Corrupt objects stop snaptrim and mark pg snaptrim_error (`issue#13837 <http://tracker.ceph.com/issues/13837>`_, `pr#15635 <https://github.com/ceph/ceph/pull/15635>`_, David Zafman)
* osd: debug con in ms_handle_connect (`pr#13540 <https://github.com/ceph/ceph/pull/13540>`_, Sage Weil)
* osd: do not send ENXIO on misdirected op by default (`issue#18751 <http://tracker.ceph.com/issues/18751>`_, `pr#13206 <https://github.com/ceph/ceph/pull/13206>`_, Sage Weil)
* osd: do not send pg_created unless luminous (`issue#20785 <http://tracker.ceph.com/issues/20785>`_, `pr#16677 <https://github.com/ceph/ceph/pull/16677>`_, Kefu Chai)
* osd: do not try to boot until we've seen the first osdmap (`pr#15732 <https://github.com/ceph/ceph/pull/15732>`_, Sage Weil)
* osd: do not try to set device class before luminous (`issue#20850 <http://tracker.ceph.com/issues/20850>`_, `pr#16706 <https://github.com/ceph/ceph/pull/16706>`_, Josh Durgin)
* osd: don't leak pgrefs or reservations in SnapTrimmer (`issue#19931 <http://tracker.ceph.com/issues/19931>`_, `pr#15214 <https://github.com/ceph/ceph/pull/15214>`_, Greg Farnum)
* osd: don't share osdmap with objecter when preboot (`issue#15025 <http://tracker.ceph.com/issues/15025>`_, `pr#13946 <https://github.com/ceph/ceph/pull/13946>`_, Mingxin Liu)
* osd: don't use ORDERSNAP for flush; always request/send ondisk ack (`issue#18961 <http://tracker.ceph.com/issues/18961>`_, `pr#13570 <https://github.com/ceph/ceph/pull/13570>`_, Samuel Just)
* osd: drop support for listing objects at a given snap (`pr#13398 <https://github.com/ceph/ceph/pull/13398>`_, Sage Weil)
* osd: dump the field name of object watchers  and cleanups (`pr#15946 <https://github.com/ceph/ceph/pull/15946>`_, Yan Jun)
* osd: EC read handling: don't grab an objectstore error to use as the read error (`pr#16663 <https://github.com/ceph/ceph/pull/16663>`_, David Zafman)
* osd: eliminate snapdir objects and move clone snaps vector into SnapSet (`pr#13610 <https://github.com/ceph/ceph/pull/13610>`_, Sage Weil)
* osd: Execute crush_location_hook when configured in ceph.conf (`pr#15951 <https://github.com/ceph/ceph/pull/15951>`_, Wido den Hollander)
* osd: _exit() intead of exit() for failure injection (`issue#18372 <http://tracker.ceph.com/issues/18372>`_, `pr#12726 <https://github.com/ceph/ceph/pull/12726>`_, Sage Weil)
* osd: extend OMAP_GETKEYS and GETVALS to include a 'more' output field (`pr#12950 <https://github.com/ceph/ceph/pull/12950>`_, Sage Weil)
* osd: fall back to failsafe threshold if osdmap doesn't set [near]full (`pr#14004 <https://github.com/ceph/ceph/pull/14004>`_, Sage Weil)
* osd: faster dispatch (`pr#13343 <https://github.com/ceph/ceph/pull/13343>`_, Sage Weil)
* osd: fix a couple bugs with persisting the missing set when it contains deletes (`issue#20704 <http://tracker.ceph.com/issues/20704>`_, `pr#16459 <https://github.com/ceph/ceph/pull/16459>`_, Josh Durgin)
* osd: fix argument-dependent lookup of swap() (`pr#15124 <https://github.com/ceph/ceph/pull/15124>`_, Casey Bodley)
* osd: fix a signed/unsigned warning in PG (`pr#13922 <https://github.com/ceph/ceph/pull/13922>`_, Greg Farnum)
* osd: fix comments about pg refs and lock (`pr#14279 <https://github.com/ceph/ceph/pull/14279>`_, tang.jin)
* osd: fix coverity warning for uninitialized members (`pr#12724 <https://github.com/ceph/ceph/pull/12724>`_, Li Wang)
* osd: fix func name in log produced by handle_pg_peering_evt() (`pr#13801 <https://github.com/ceph/ceph/pull/13801>`_, xie xingguo)
* osd: fix occasional MOSDMap leak (`issue#18293 <http://tracker.ceph.com/issues/18293>`_, `pr#14558 <https://github.com/ceph/ceph/pull/14558>`_, Sage Weil)
* osd: fix OpRequest and tracked op dump information (`pr#16504 <https://github.com/ceph/ceph/pull/16504>`_, Yan Jun)
* osd: fix past_intervals base case by adding epoch_pool_created to pg_history_t (`issue#19877 <http://tracker.ceph.com/issues/19877>`_, `pr#14989 <https://github.com/ceph/ceph/pull/14989>`_, Sage Weil)
* osd: fix pg ref leaks when osd shutdown (`issue#20684 <http://tracker.ceph.com/issues/20684>`_, `pr#16408 <https://github.com/ceph/ceph/pull/16408>`_, Yang Honggang)
* osd: fix some osd beacon bugs (`pr#14274 <https://github.com/ceph/ceph/pull/14274>`_, Sage Weil)
* osd: fix stat sum update of recovery pushing (`pr#13328 <https://github.com/ceph/ceph/pull/13328>`_, Zhiqiang Wang)
* osd: fix the setting of soid in sub_op_push (`pr#13353 <https://github.com/ceph/ceph/pull/13353>`_, Zhiqiang Wang)
* osd: fix typo in comment (`pr#13061 <https://github.com/ceph/ceph/pull/13061>`_, Gu Zhongyan)
* osd: Fix useless MAX(0, unsigned) to prevent out of wack misplaced (`issue#18718 <http://tracker.ceph.com/issues/18718>`_, `pr#13164 <https://github.com/ceph/ceph/pull/13164>`_, David Zafman)
* osd: have clients resend ops on pg split (`pr#13235 <https://github.com/ceph/ceph/pull/13235>`_, Sage Weil)
* osd: hdd vs ssd defaults for osd op thread pool (`pr#15422 <https://github.com/ceph/ceph/pull/15422>`_, Sage Weil)
* osd: heartbeat with packets large enough to require working jumbo frames (`issue#20087 <http://tracker.ceph.com/issues/20087>`_, `pr#15535 <https://github.com/ceph/ceph/pull/15535>`_, Greg Farnum)
* osd: Implement asynchronous recovery sleep (`pr#15212 <https://github.com/ceph/ceph/pull/15212>`_, Neha Ojha)
* osd: Implement asynchronous scrub sleep (`issue#19497 <http://tracker.ceph.com/issues/19497>`_, `pr#14886 <https://github.com/ceph/ceph/pull/14886>`_, Brad Hubbard)
* osd: Implement peering state timing (`pr#14627 <https://github.com/ceph/ceph/pull/14627>`_, Brad Hubbard)
* osd: improve error message when FileStore op fails due to EPERM (`issue#18037 <http://tracker.ceph.com/issues/18037>`_, `pr#12181 <https://github.com/ceph/ceph/pull/12181>`_, Nathan Cutler)
* osd: initialize waiting_for_pg_osdmap on startup (`issue#20748 <http://tracker.ceph.com/issues/20748>`_, `pr#16535 <https://github.com/ceph/ceph/pull/16535>`_, Sage Weil)
* osd: kill all remaining MOSDSubOp users (`pr#13401 <https://github.com/ceph/ceph/pull/13401>`_, Sage Weil)
* osd: kill sortbitwise (`pr#13321 <https://github.com/ceph/ceph/pull/13321>`_, Sage Weil)
* osd: Log audit (`pr#16281 <https://github.com/ceph/ceph/pull/16281>`_, Brad Hubbard)
* osd: make ec overwrites ready to use (`pr#14496 <https://github.com/ceph/ceph/pull/14496>`_, Josh Durgin)
* osd: moved OpFinisher logic from OSDOp to OpContext (`issue#20783 <http://tracker.ceph.com/issues/20783>`_, `pr#16617 <https://github.com/ceph/ceph/pull/16617>`_, Jason Dillaman)
* osd: Move scrub sleep timer to osdservice (`issue#19986 <http://tracker.ceph.com/issues/19986>`_, `pr#15217 <https://github.com/ceph/ceph/pull/15217>`_, Brad Hubbard)
* osd: never send rados ack (only commit) (`pr#12451 <https://github.com/ceph/ceph/pull/12451>`_, Sage Weil)
* osd: new op for calculating an extent checksum (`pr#14256 <https://github.com/ceph/ceph/pull/14256>`_, Jason Dillaman)
* osd: objclass sdk (`pr#14723 <https://github.com/ceph/ceph/pull/14723>`_, Neha Ojha)
* osd: Object level shard errors are tracked and used if no auth available (`issue#20089 <http://tracker.ceph.com/issues/20089>`_, `pr#15397 <https://github.com/ceph/ceph/pull/15397>`_, David Zafman)
* osd: On EIO from read recover the primary replica from another copy (`issue#18165 <http://tracker.ceph.com/issues/18165>`_, `pr#14760 <https://github.com/ceph/ceph/pull/14760>`_, David Zafman)
* osd: osdc/ObjectCacher: use state instead of get_state() (`pr#12544 <https://github.com/ceph/ceph/pull/12544>`_, huangjun)
* osd: osdc/Objecter: more constness (`pr#14819 <https://github.com/ceph/ceph/pull/14819>`_, Kefu Chai)
* osd: osdc: silence warning from `-Wsign-compare` (`pr#14729 <https://github.com/ceph/ceph/pull/14729>`_, Jos Collin)
* osd: osd does not using MPing Messages,do not include unused include (`pr#15833 <https://github.com/ceph/ceph/pull/15833>`_, linbing)
* osd: osd/OSDMap.cc: check if osd is out in subtree_type_is_down (`issue#19989 <http://tracker.ceph.com/issues/19989>`_, `pr#15250 <https://github.com/ceph/ceph/pull/15250>`_, Neha Ojha)
* osd: osd/OSDMap: require OSD features only of OSDs (`issue#18831 <http://tracker.ceph.com/issues/18831>`_, `pr#13275 <https://github.com/ceph/ceph/pull/13275>`_, Ilya Dryomov)
* osd: osd/PrimaryLogPG: nullptr not NULL (`pr#13973 <https://github.com/ceph/ceph/pull/13973>`_, Shinobu Kinjo)
* osd: 'osd tree in|out|up|down' to filter tree results (`pr#15294 <https://github.com/ceph/ceph/pull/15294>`_, Sage Weil)
* osd: os/kstore: some error handling (`pr#13960 <https://github.com/ceph/ceph/pull/13960>`_, wangzhengyong)
* osd/PGBackend: delete reply if fails to complete delete request (`issue#20913 <http://tracker.ceph.com/issues/20913>`_, `pr#17233 <https://github.com/ceph/ceph/pull/17233>`_, Kefu Chai)
* osd: pg: be more careful with locking around forced pg recovery (`issue#20808 <http://tracker.ceph.com/issues/20808>`_, `pr#16712 <https://github.com/ceph/ceph/pull/16712>`_, Greg Farnum)
* osd: pglog trimming fixes (`pr#12882 <https://github.com/ceph/ceph/pull/12882>`_, Zhiqiang Wang)
* osd: pglog: with config, don't assert in the presence of stale diverg… (`issue#17916 <http://tracker.ceph.com/issues/17916>`_, `pr#14648 <https://github.com/ceph/ceph/pull/14648>`_, Greg Farnum)
* osd: pg-remap -> pg-upmap (`pr#14556 <https://github.com/ceph/ceph/pull/14556>`_, Sage Weil)
* osd: populate last_epoch_split during build_initial_pg_history (`issue#20754 <http://tracker.ceph.com/issues/20754>`_, `pr#16519 <https://github.com/ceph/ceph/pull/16519>`_, Sage Weil)
* osd: Preserve OSDOp information for historic ops (`pr#15265 <https://github.com/ceph/ceph/pull/15265>`_, Guo-Fu Tseng)
* osd: PrimaryLogPG, PGBackend: complete callback even if interval changes (`issue#20747 <http://tracker.ceph.com/issues/20747>`_, `pr#16536 <https://github.com/ceph/ceph/pull/16536>`_, Josh Durgin)
* osd: print pg_info_t::purged_snaps as array, not string (`issue#18584 <http://tracker.ceph.com/issues/18584>`_, `pr#14217 <https://github.com/ceph/ceph/pull/14217>`_, liuchang0812)
* osd: process deletes during recovery instead of peering (`issue#19971 <http://tracker.ceph.com/issues/19971>`_, `pr#15952 <https://github.com/ceph/ceph/pull/15952>`_, Josh Durgin)
* osd: put osdmap in mempool (`pr#14780 <https://github.com/ceph/ceph/pull/14780>`_, Sage Weil)
* osd: reduce buffer pinning from EC entries (`pr#15120 <https://github.com/ceph/ceph/pull/15120>`_, Sage Weil)
* osd: reduce map cache size (`pr#15292 <https://github.com/ceph/ceph/pull/15292>`_, Sage Weil)
* osd: reduce rados_max_object_size from 100 GB -> 128 MB (`pr#15520 <https://github.com/ceph/ceph/pull/15520>`_, Sage Weil)
* osd: remove copy-get-classic (`pr#13547 <https://github.com/ceph/ceph/pull/13547>`_, Sage Weil)
* osd: remove sortbitwise thrashing (`pr#13296 <https://github.com/ceph/ceph/pull/13296>`_, Sage Weil)
* osd: renamed the new vector name in OSDMap::build_simple_crush_map_from_conf (`pr#14583 <https://github.com/ceph/ceph/pull/14583>`_, Jos Collin)
* osd: rename osd -> osd_pglog; include pglog-related bufferlists (`pr#15531 <https://github.com/ceph/ceph/pull/15531>`_, Sage Weil)
* osd: rephrase "wrongly marked me down" clog message (`pr#16365 <https://github.com/ceph/ceph/pull/16365>`_, John Spray)
* osd: replace object_info_t::operator=() with decode() (`pr#13938 <https://github.com/ceph/ceph/pull/13938>`_, tang.jin)
* osd: ReplicatedBackend::prep_push() remove redundant variable assignments (`pr#14817 <https://github.com/ceph/ceph/pull/14817>`_, Jin Cai)
* osd: restart boot process if waiting for luminous mons (`issue#20631 <http://tracker.ceph.com/issues/20631>`_, `pr#16341 <https://github.com/ceph/ceph/pull/16341>`_, Sage Weil)
* osd: Return correct osd_objectstore in OSD metadata (`issue#18638 <http://tracker.ceph.com/issues/18638>`_, `pr#13072 <https://github.com/ceph/ceph/pull/13072>`_, Wido den Hollander)
* osd: Return early on shutdown (`issue#19900 <http://tracker.ceph.com/issues/19900>`_, `pr#15345 <https://github.com/ceph/ceph/pull/15345>`_, Brad Hubbard)
* osd: Reverse order of op_has_sufficient_caps and do_pg_op (`issue#19790 <http://tracker.ceph.com/issues/19790>`_, `pr#15354 <https://github.com/ceph/ceph/pull/15354>`_, Brad Hubbard)
* osd: sched_scrub() lock pg only if all scrubbing conditions are fulfilled (`pr#14968 <https://github.com/ceph/ceph/pull/14968>`_, Jin Cai)
* osd: scrub_to specifies clone ver, but transaction include head write… (`issue#20041 <http://tracker.ceph.com/issues/20041>`_, `pr#16404 <https://github.com/ceph/ceph/pull/16404>`_, David Zafman)
* osd: silence warning from -Wint-in-bool-context (`pr#16744 <https://github.com/ceph/ceph/pull/16744>`_, Jos Collin)
* osd: simplify past_intervals representation (`pr#14444 <https://github.com/ceph/ceph/pull/14444>`_, Samuel Just, Sage Weil)
* osd: small clear up and optimize on _recover_now and should_share_map function (`pr#13476 <https://github.com/ceph/ceph/pull/13476>`_, song baisen)
* osd: stop mgrc earlier in shutdown() (`issue#19638 <http://tracker.ceph.com/issues/19638>`_, `pr#14904 <https://github.com/ceph/ceph/pull/14904>`_, Kefu Chai)
* osd: stop MgrClient callbacks on shutdown (`issue#19638 <http://tracker.ceph.com/issues/19638>`_, `pr#14896 <https://github.com/ceph/ceph/pull/14896>`_, Sage Weil)
* osd: strip pglog op name (`pr#14764 <https://github.com/ceph/ceph/pull/14764>`_, liuchang0812)
* osd: support cmpext operation on EC-backed pools (`pr#15693 <https://github.com/ceph/ceph/pull/15693>`_, Zhengyong Wang, Jason Dillaman)
* osd: support dumping long ops (`pr#13019 <https://github.com/ceph/ceph/pull/13019>`_, Zhiqiang Wang)
* osd: switch filestore to default to rocksdb (`pr#14814 <https://github.com/ceph/ceph/pull/14814>`_, Neha Ojha)
* osd: tag fast dispatch messages with min_epoch (`pr#13681 <https://github.com/ceph/ceph/pull/13681>`_, Sage Weil)
* osd: take PGRef for recovery sleep wakeup event (`issue#20226 <http://tracker.ceph.com/issues/20226>`_, `pr#15582 <https://github.com/ceph/ceph/pull/15582>`_, Sage Weil)
* osd: the condition of last epoch <= superblock.newest_map epoch has been check twice (`pr#15590 <https://github.com/ceph/ceph/pull/15590>`_, linbing)
* osd: the osd should not share map with others when it is in stopping state (`pr#13668 <https://github.com/ceph/ceph/pull/13668>`_, song baisen)
* osd: unlock sdata_op_ordering_lock with sdata_lock hold to avoid miss… (`pr#15891 <https://github.com/ceph/ceph/pull/15891>`_, Ming Lin)
* osd: use append(bufferlist &) to avoid unnecessary copy (`pr#12272 <https://github.com/ceph/ceph/pull/12272>`_, Yunchuan Wen)
* osd: use separate waitlist for scrub (`pr#13136 <https://github.com/ceph/ceph/pull/13136>`_, Sage Weil)
* osd: various changes for preventing internal ENOSPC condition (`issue#16878 <http://tracker.ceph.com/issues/16878>`_, `pr#13425 <https://github.com/ceph/ceph/pull/13425>`_, David Zafman)
* osd: we know the definite epoch of marking down (`pr#13121 <https://github.com/ceph/ceph/pull/13121>`_, Mingxin Liu)
* osd: when osd in not in failure_pending, we don't need to get osd inst from osdmap (`pr#15558 <https://github.com/ceph/ceph/pull/15558>`_, linbing)
* osd: When scrub finds an attr error mark shard inconsistent (`issue#20089 <http://tracker.ceph.com/issues/20089>`_, `pr#15368 <https://github.com/ceph/ceph/pull/15368>`_, David Zafman)
* osd: zipkin tracing (`pr#14305 <https://github.com/ceph/ceph/pull/14305>`_, Sage Weil, Marios-Evaggelos Kogias, Victor Araujo, Casey Bodley, Andrew Shewmaker, Chendi.Xue)
* performance: buffer, osd: add missing crc cache miss perf counter (`pr#14957 <https://github.com/ceph/ceph/pull/14957>`_, Piotr Dałek)
* performance: common/config_opts.h: Lower HDD throttle cost (`pr#15485 <https://github.com/ceph/ceph/pull/15485>`_, Mark Nelson)
* performance: crc32c: optimize aarch64 crc32c implementation (`pr#12977 <https://github.com/ceph/ceph/pull/12977>`_, wei xiao)
* performance: denc: add need_contiguous to denc_traits (`pr#15224 <https://github.com/ceph/ceph/pull/15224>`_, Kefu Chai)
* performance: osd, messenger, librados: lttng oid tracing (`pr#12492 <https://github.com/ceph/ceph/pull/12492>`_, Anjaneya Chagam)
* performance: osd/PG.cc: loop invariant code motion (`pr#12720 <https://github.com/ceph/ceph/pull/12720>`_, Li Wang)
* performance: osd/ReplicatedBackend: do not set omap header if it is empty (`pr#12612 <https://github.com/ceph/ceph/pull/12612>`_, fang yuxiang)
* performance,rgw: rgw_file: permit dirent offset computation (`pr#16275 <https://github.com/ceph/ceph/pull/16275>`_, Matt Benjamin)
* pybind: better error msg (`pr#14497 <https://github.com/ceph/ceph/pull/14497>`_, Kefu Chai)
* pybind: cephfs should be built without librados / python-rados (`pr#13431 <https://github.com/ceph/ceph/pull/13431>`_, Kefu Chai)
* pybind: ceph.in: Check return value when connecting (`pr#16130 <https://github.com/ceph/ceph/pull/16130>`_, Douglas Fuller)
* pybind: ceph-rest-api: Various REST API fixes (`pr#15910 <https://github.com/ceph/ceph/pull/15910>`_, Wido den Hollander)
* pybind: conditional compile the linux specific constant (`pr#12198 <https://github.com/ceph/ceph/pull/12198>`_, Kefu Chai)
* pybind: fix docstring for librbd Python binding (`pr#13977 <https://github.com/ceph/ceph/pull/13977>`_, runsisi)
* pybind: fix open flags calculation (`issue#19890 <http://tracker.ceph.com/issues/19890>`_, `pr#15018 <https://github.com/ceph/ceph/pull/15018>`_, "Yan, Zheng")
* pybind: pybind/ceph_argparse: fix empty string check (`issue#20135 <http://tracker.ceph.com/issues/20135>`_, `pr#15500 <https://github.com/ceph/ceph/pull/15500>`_, Sage Weil)
* pybind: pybind/ceph_daemon.py: fix Termsize.update (`pr#15253 <https://github.com/ceph/ceph/pull/15253>`_, Kefu Chai)
* pybind: pybind/ceph_daemon: use small chunk for recv (`pr#13804 <https://github.com/ceph/ceph/pull/13804>`_, Xiaoxi Chen)
* pybind: pybind/mgr/dashboard: fix get kernel_version error (`pr#16094 <https://github.com/ceph/ceph/pull/16094>`_, Peng Zhang)
* pybind: pybind/mgr/restful: fix typo (`pr#16560 <https://github.com/ceph/ceph/pull/16560>`_, Nick Erdmann)
* pybind: pybind/mgr/restful: use list to pass hooks to create a `Pecan` instance (`issue#20258 <http://tracker.ceph.com/issues/20258>`_, `pr#15646 <https://github.com/ceph/ceph/pull/15646>`_, Kefu Chai)
* pybind: pybind/rados: avoid call free() on invalid pointer (`pr#15159 <https://github.com/ceph/ceph/pull/15159>`_, Mingxin Liu)
* pybind: pybind/rados: use new APIs instead of deprecated ones (`pr#16684 <https://github.com/ceph/ceph/pull/16684>`_, Kefu Chai)
* pybind,rbd: pybind/rbd: OSError should be picklable (`issue#20223 <http://tracker.ceph.com/issues/20223>`_, `pr#15574 <https://github.com/ceph/ceph/pull/15574>`_, Jason Dillaman)
* pybind: restore original API for backwards compatibility (`issue#20421 <http://tracker.ceph.com/issues/20421>`_, `pr#15932 <https://github.com/ceph/ceph/pull/15932>`_, Jason Dillaman)
* pybind: support mon target in pybind (`pr#15409 <https://github.com/ceph/ceph/pull/15409>`_, liuchang0812)
* qa: fix POOL_APP_NOT_ENABLED warning in krbd:unmap suite (`pr#17192 <https://github.com/ceph/ceph/pull/17192>`_, Ilya Dryomov)
* rbd: add default note info to size (create and resize) (`pr#15561 <https://github.com/ceph/ceph/pull/15561>`_, Zheng Yin)
* rbd: add error prompt when input command 'snap set limit' is incomplete (`pr#12945 <https://github.com/ceph/ceph/pull/12945>`_, Tang Jin)
* rbd: additional validation for 'bench' optional parameters (`pr#12697 <https://github.com/ceph/ceph/pull/12697>`_, Yunchuan Wen)
* rbd: bench-write should return error if io-size >= 4G (`issue#18422 <http://tracker.ceph.com/issues/18422>`_, `pr#12864 <https://github.com/ceph/ceph/pull/12864>`_, Gaurav Kumar Garg)
* rbd: cleanup: fix the typo in namespace comment (`pr#12858 <https://github.com/ceph/ceph/pull/12858>`_, Dongsheng Yang)
* rbd: cleanup: rbd: fix a typo in comment (`pr#14049 <https://github.com/ceph/ceph/pull/14049>`_, Dongsheng Yang)
* rbd: cls_rbd: default initialize snapshot namespace for legacy clients (`issue#19413 <http://tracker.ceph.com/issues/19413>`_, `pr#14903 <https://github.com/ceph/ceph/pull/14903>`_, Jason Dillaman)
* rbd: cls/rbd: silence warning from -Wunused-variable (`pr#16670 <https://github.com/ceph/ceph/pull/16670>`_, Yan Jun)
* rbd: cls/rbd: trash_list should be iterable (`issue#20643 <http://tracker.ceph.com/issues/20643>`_, `pr#16372 <https://github.com/ceph/ceph/pull/16372>`_, Jason Dillaman)
* rbd: common/bit_vector: utilize deep-copy during data decode (`issue#19863 <http://tracker.ceph.com/issues/19863>`_, `pr#15017 <https://github.com/ceph/ceph/pull/15017>`_, Jason Dillaman)
* rbd: correct coverity warnings (`pr#12954 <https://github.com/ceph/ceph/pull/12954>`_, Jason Dillaman)
* rbd: correct issues with image importing (`pr#14401 <https://github.com/ceph/ceph/pull/14401>`_, Jason Dillaman)
* rbd: demote/promote all mirrored images in a pool (`issue#18748 <http://tracker.ceph.com/issues/18748>`_, `pr#13758 <https://github.com/ceph/ceph/pull/13758>`_, Jason Dillaman)
* rbd: destination pool should be source pool if it is not specified (`issue#18326 <http://tracker.ceph.com/issues/18326>`_, `pr#13189 <https://github.com/ceph/ceph/pull/13189>`_, Gaurav Kumar Garg)
* rbd: do not attempt to load key if auth is disabled (`issue#19035 <http://tracker.ceph.com/issues/19035>`_, `pr#16024 <https://github.com/ceph/ceph/pull/16024>`_, Jason Dillaman)
* rbd: Drop unused member variable reopen in C_OpenComplete (`pr#16729 <https://github.com/ceph/ceph/pull/16729>`_, amitkuma)
* rbd: enable rbd on FreeBSD (without KRBD) (`pr#12798 <https://github.com/ceph/ceph/pull/12798>`_, Willem Jan Withagen)
* rbd: error out if import image format failed (`pr#13957 <https://github.com/ceph/ceph/pull/13957>`_, wangzhengyong)
* rbd: fixed coverity 'Argument cannot be negative' warning (`pr#16686 <https://github.com/ceph/ceph/pull/16686>`_, amitkuma)
* rbd: fix typo in Kernel.cc (`issue#19273 <http://tracker.ceph.com/issues/19273>`_, `pr#13983 <https://github.com/ceph/ceph/pull/13983>`_, Gaurav Kumar Garg)
* rbd: 'image-meta remove' for missing key does not return error (`issue#16990 <http://tracker.ceph.com/issues/16990>`_, `pr#16393 <https://github.com/ceph/ceph/pull/16393>`_, PCzhangPC)
* rbd: import-diff should discard any zeroed extents (`pr#14445 <https://github.com/ceph/ceph/pull/14445>`_, Jason Dillaman)
* rbd: import needs to sanity check auto-generated image name (`issue#19128 <http://tracker.ceph.com/issues/19128>`_, `pr#14754 <https://github.com/ceph/ceph/pull/14754>`_, Mykola Golub)
* rbd: import real thin-provision image (`issue#15648 <http://tracker.ceph.com/issues/15648>`_, `pr#12883 <https://github.com/ceph/ceph/pull/12883>`_, yaoning, Ning Yao)
* rbd: info command should indicate if parent is in trash (`pr#14875 <https://github.com/ceph/ceph/pull/14875>`_, Jason Dillaman)
* rbd: introduce v2 format for rbd export/import (`issue#13186 <http://tracker.ceph.com/issues/13186>`_, `pr#10487 <https://github.com/ceph/ceph/pull/10487>`_, Dongsheng Yang)
* rbd: journal: don't hold future lock during assignment (`issue#18618 <http://tracker.ceph.com/issues/18618>`_, `pr#13033 <https://github.com/ceph/ceph/pull/13033>`_, Jason Dillaman)
* rbd: journal: stop processing removal after error (`issue#18738 <http://tracker.ceph.com/issues/18738>`_, `pr#13193 <https://github.com/ceph/ceph/pull/13193>`_, Jason Dillaman)
* rbd: luminous: librbd: default localize parent reads to false (`issue#20941 <http://tracker.ceph.com/issues/20941>`_, `pr#16899 <https://github.com/ceph/ceph/pull/16899>`_, Jason Dillaman)
* rbd: luminous: librbd: remove consistency group rbd cli and API support (`pr#16875 <https://github.com/ceph/ceph/pull/16875>`_, Jason Dillaman)
* rbd: luminous: rbd-ggate: tool to map images on FreeBSD via GEOM Gate (`pr#16895 <https://github.com/ceph/ceph/pull/16895>`_, Mykola Golub)
* rbd: luminous: rbd-mirror: align use of uint64_t in service_daemon::AttributeType (`pr#16948 <https://github.com/ceph/ceph/pull/16948>`_, James Page)
* rbd: luminous: rbd-mirror: simplify notifications for image assignment (`issue#15764 <http://tracker.ceph.com/issues/15764>`_, `pr#16878 <https://github.com/ceph/ceph/pull/16878>`_, Jason Dillaman)
* rbd: luminous: rbd: parallelize rbd ls -l (`pr#16921 <https://github.com/ceph/ceph/pull/16921>`_, Piotr Dałek)
* rbd: make it more understandable when adding peer returns error (`pr#16313 <https://github.com/ceph/ceph/pull/16313>`_, songweibin)
* rbd-mirror: add support for active/passive daemon instances (`issue#17018 <http://tracker.ceph.com/issues/17018>`_, `issue#17019 <http://tracker.ceph.com/issues/17019>`_, `issue#17020 <http://tracker.ceph.com/issues/17020>`_, `pr#12948 <https://github.com/ceph/ceph/pull/12948>`_, Mykola Golub)
* rbd-mirror: assertion failure during pool replayer shut down (`issue#20644 <http://tracker.ceph.com/issues/20644>`_, `pr#16704 <https://github.com/ceph/ceph/pull/16704>`_, Jason Dillaman)
* rbd-mirror: avoid processing new events after stop requested (`issue#18441 <http://tracker.ceph.com/issues/18441>`_, `pr#12837 <https://github.com/ceph/ceph/pull/12837>`_, Jason Dillaman)
* rbd-mirror: check remote image mirroring state when bootstrapping (`issue#18447 <http://tracker.ceph.com/issues/18447>`_, `pr#12820 <https://github.com/ceph/ceph/pull/12820>`_, Mykola Golub)
* rbd-mirror: coordinate image syncs with leader (`issue#18789 <http://tracker.ceph.com/issues/18789>`_, `pr#14745 <https://github.com/ceph/ceph/pull/14745>`_, Mykola Golub)
* rbd-mirror: delayed replication support (`issue#15371 <http://tracker.ceph.com/issues/15371>`_, `pr#11879 <https://github.com/ceph/ceph/pull/11879>`_, Mykola Golub)
* rbd-mirror: deleting a snapshot during sync can result in read errors (`issue#18990 <http://tracker.ceph.com/issues/18990>`_, `pr#13568 <https://github.com/ceph/ceph/pull/13568>`_, Jason Dillaman)
* rbd-mirror: ensure missing images are re-synced when detected (`issue#19811 <http://tracker.ceph.com/issues/19811>`_, `pr#14945 <https://github.com/ceph/ceph/pull/14945>`_, Jason Dillaman)
* rbd-mirror: failover and failback of unmodified image results in split-brain (`issue#19858 <http://tracker.ceph.com/issues/19858>`_, `pr#14963 <https://github.com/ceph/ceph/pull/14963>`_, Jason Dillaman)
* rbd-mirror: guard the deletion of non-primary images (`pr#16398 <https://github.com/ceph/ceph/pull/16398>`_, Jason Dillaman)
* rbd-mirror: ignore permission errors on rbd_mirroring object (`issue#20571 <http://tracker.ceph.com/issues/20571>`_, `pr#16264 <https://github.com/ceph/ceph/pull/16264>`_, Jason Dillaman)
* rbd-mirror: image deletions should be handled by assigned instance (`pr#14832 <https://github.com/ceph/ceph/pull/14832>`_, Jason Dillaman)
* rbd-mirror: initialize timer context pointer to null (`pr#16603 <https://github.com/ceph/ceph/pull/16603>`_, Jason Dillaman)
* rbd-mirror: InstanceWatcher watch/notify stub for leader/follower RPC (`issue#18783 <http://tracker.ceph.com/issues/18783>`_, `pr#13312 <https://github.com/ceph/ceph/pull/13312>`_, Mykola Golub)
* rbd-mirror: lock loss during sync should wait for in-flight copies (`pr#15532 <https://github.com/ceph/ceph/pull/15532>`_, Jason Dillaman)
* rbd-mirror: permit release of local image exclusive lock after force promotion (`issue#18963 <http://tracker.ceph.com/issues/18963>`_, `pr#15140 <https://github.com/ceph/ceph/pull/15140>`_, Jason Dillaman)
* rbd-mirror: pool watcher should track mirror uuid (`pr#14240 <https://github.com/ceph/ceph/pull/14240>`_, Jason Dillaman)
* rbd-mirror: remove tracking of image names from pool watcher (`pr#14712 <https://github.com/ceph/ceph/pull/14712>`_, Jason Dillaman)
* rbd-mirror: replace remote pool polling with add/remove notifications (`issue#15029 <http://tracker.ceph.com/issues/15029>`_, `pr#12364 <https://github.com/ceph/ceph/pull/12364>`_, Jason Dillaman)
* rbd-mirror: resolve admin socket path names collision (`issue#19907 <http://tracker.ceph.com/issues/19907>`_, `pr#15048 <https://github.com/ceph/ceph/pull/15048>`_, Mykola Golub)
* rbd-mirror: separate ImageReplayer handling from Replayer (`issue#18785 <http://tracker.ceph.com/issues/18785>`_, `pr#13803 <https://github.com/ceph/ceph/pull/13803>`_, Mykola Golub)
* rbd-mirror: Set the data pool correctly when creating images (`issue#20567 <http://tracker.ceph.com/issues/20567>`_, `pr#17023 <https://github.com/ceph/ceph/pull/17023>`_, Adam Wolfe Gordon)
* rbd-mirror: track images via global image id (`pr#13416 <https://github.com/ceph/ceph/pull/13416>`_, Jason Dillaman)
* rbd: modified some commands' description into imperative sentence (`pr#16694 <https://github.com/ceph/ceph/pull/16694>`_, songweibin)
* rbd-nbd: check /sys/block/nbdX/size to ensure kernel mapped correctly (`issue#18335 <http://tracker.ceph.com/issues/18335>`_, `pr#13229 <https://github.com/ceph/ceph/pull/13229>`_, Mykola Golub)
* rbd-nbd: clean up the doc and help information (`pr#14146 <https://github.com/ceph/ceph/pull/14146>`_, Pan Liu)
* rbd-nbd: create admin socket only for map command (`issue#17951 <http://tracker.ceph.com/issues/17951>`_, `pr#12433 <https://github.com/ceph/ceph/pull/12433>`_, Pan Liu)
* rbd-nbd: display pool/image/snap information in list output (`pr#15317 <https://github.com/ceph/ceph/pull/15317>`_, Pan Liu)
* rbd-nbd: don't ignore --read-only option in BLKROSET ioctl (`pr#13944 <https://github.com/ceph/ceph/pull/13944>`_, Pan Liu)
* rbd-nbd: ensure unmap returns error code (`pr#15593 <https://github.com/ceph/ceph/pull/15593>`_, guojiannan, chenfangxian)
* rbd-nbd: fix a typo "moudle" (`pr#13652 <https://github.com/ceph/ceph/pull/13652>`_, Pan Liu)
* rbd-nbd: fix typo in comment (`pr#14034 <https://github.com/ceph/ceph/pull/14034>`_, Pan Liu)
* rbd-nbd: no need to check image format any more (`pr#13389 <https://github.com/ceph/ceph/pull/13389>`_, Mykola Golub)
* rbd-nbd: relax size check for newer kernel versions (`issue#19871 <http://tracker.ceph.com/issues/19871>`_, `pr#14976 <https://github.com/ceph/ceph/pull/14976>`_, Mykola Golub)
* rbd-nbd: remove debug messages from do_unmap (`pr#14253 <https://github.com/ceph/ceph/pull/14253>`_, Pan Liu)
* rbd-nbd: s/cpp_error/cpp_strerror/ to fix FTBFS (`pr#14223 <https://github.com/ceph/ceph/pull/14223>`_, Kefu Chai)
* rbd-nbd: support signal handle for SIGHUP, SIGINT and SIGTERM (`issue#19349 <http://tracker.ceph.com/issues/19349>`_, `pr#14079 <https://github.com/ceph/ceph/pull/14079>`_, Pan Liu)
* rbd-nbd: update size only when NBD_SET_SIZE successful (`pr#14005 <https://github.com/ceph/ceph/pull/14005>`_, Pan Liu)
* rbd-nbd: warn when kernel parameters are ignored (`issue#19108 <http://tracker.ceph.com/issues/19108>`_, `pr#13694 <https://github.com/ceph/ceph/pull/13694>`_, Pan Liu)
* rbd: prevent adding multiple mirror peers to a single pool (`issue#19256 <http://tracker.ceph.com/issues/19256>`_, `pr#13919 <https://github.com/ceph/ceph/pull/13919>`_, Jason Dillaman)
* rbd: properly decode features when using image name optional (`issue#20185 <http://tracker.ceph.com/issues/20185>`_, `pr#15492 <https://github.com/ceph/ceph/pull/15492>`_, Jason Dillaman)
* rbd:  pybind/rbd: add image metadata methods (`issue#19451 <http://tracker.ceph.com/issues/19451>`_, `pr#14463 <https://github.com/ceph/ceph/pull/14463>`_, Mykola Golub)
* rbd: pybind/rbd: fix crash if more than 1024 images in trash bin (`pr#15134 <https://github.com/ceph/ceph/pull/15134>`_, runsisi)
* rbd: rbd/bench: add notes of default values, it's easy to use (`pr#14762 <https://github.com/ceph/ceph/pull/14762>`_, Zheng Yin)
* rbd: rbd/bench: fix write gaps when doing sequential writes with io-threads > 1 (`pr#15206 <https://github.com/ceph/ceph/pull/15206>`_, Igor Fedotov)
* rbd: rbd, librbd: migrate atomic_t to std::atomic (`pr#14656 <https://github.com/ceph/ceph/pull/14656>`_, Jesse Williamson)
* rbd: rbd-mirror A/A: leader should track up/down rbd-mirror instances (`issue#18784 <http://tracker.ceph.com/issues/18784>`_, `pr#13571 <https://github.com/ceph/ceph/pull/13571>`_, Mykola Golub)
* rbd: rbd-mirror A/A: proxy InstanceReplayer APIs via InstanceWatcher RPC (`issue#18787 <http://tracker.ceph.com/issues/18787>`_, `pr#13978 <https://github.com/ceph/ceph/pull/13978>`_, Mykola Golub)
* rbd: recognize exclusive option (`pr#14785 <https://github.com/ceph/ceph/pull/14785>`_, Ilya Dryomov)
* rbd: removed hardcoded default pool (`pr#15518 <https://github.com/ceph/ceph/pull/15518>`_, Jason Dillaman)
* rbd: remove direct linking to static boost libraries (`pr#12962 <https://github.com/ceph/ceph/pull/12962>`_, Jason Dillaman)
* rbd: removed spurious error message from mirror pool commands (`pr#14935 <https://github.com/ceph/ceph/pull/14935>`_, Jason Dillaman)
* rbd: remove unused condition within group action handler (`pr#12723 <https://github.com/ceph/ceph/pull/12723>`_, Gaurav Kumar Garg)
* rbd,rgw,tools: tools/rbd, rgw: Removed unreachable returns (`pr#16308 <https://github.com/ceph/ceph/pull/16308>`_, Jos Collin)
* rbd: spell out image features unsupported by the kernel (`issue#19095 <http://tracker.ceph.com/issues/19095>`_, `pr#13812 <https://github.com/ceph/ceph/pull/13812>`_, Ilya Dryomov)
* rbd: stop indefinite thread waiting in krbd udev handling (`issue#17195 <http://tracker.ceph.com/issues/17195>`_, `pr#14051 <https://github.com/ceph/ceph/pull/14051>`_, Spandan Kumar Sahu)
* rbd: test: fix rbd unit test cases w/ striping feature (`issue#18888 <http://tracker.ceph.com/issues/18888>`_, `pr#13196 <https://github.com/ceph/ceph/pull/13196>`_, Venky Shankar)
* rbd,tests: luminous: qa/workunits/rbd: use command line option to specify watcher asok (`issue#20954 <http://tracker.ceph.com/issues/20954>`_, `pr#16946 <https://github.com/ceph/ceph/pull/16946>`_, Mykola Golub)
* rbd,tests: luminous: test/librbd: fix race condition with OSD map refresh (`issue#20918 <http://tracker.ceph.com/issues/20918>`_, `pr#16903 <https://github.com/ceph/ceph/pull/16903>`_, Jason Dillaman)
* rbd,tests: qa: add workunit to test krbd data-pool support (`pr#13482 <https://github.com/ceph/ceph/pull/13482>`_, Ilya Dryomov)
* rbd,tests: qa: integrate OpenStack 'gate-tempest-dsvm-full-devstack-plugin-ceph' (`issue#18594 <http://tracker.ceph.com/issues/18594>`_, `pr#13158 <https://github.com/ceph/ceph/pull/13158>`_, Jason Dillaman)
* rbd,tests: qa: krbd_data_pool.sh: account for rbd_info metadata object (`pr#14631 <https://github.com/ceph/ceph/pull/14631>`_, Ilya Dryomov)
* rbd,tests: qa: krbd discard/zeroout tests (`pr#15388 <https://github.com/ceph/ceph/pull/15388>`_, Ilya Dryomov)
* rbd,tests: qa: krbd write-after-checksum tests (`pr#14836 <https://github.com/ceph/ceph/pull/14836>`_, Ilya Dryomov)
* rbd,tests: qa/suites/krbd: unmap subsuite needs straw buckets (`pr#15290 <https://github.com/ceph/ceph/pull/15290>`_, Ilya Dryomov)
* rbd,tests: qa/suites/rbd: restrict python memcheck validation to CentOS (`pr#15923 <https://github.com/ceph/ceph/pull/15923>`_, Jason Dillaman)
* rbd,tests: qa/tasks/qemu: update default image url after ceph.com redesign (`issue#18542 <http://tracker.ceph.com/issues/18542>`_, `pr#12953 <https://github.com/ceph/ceph/pull/12953>`_, Jason Dillaman)
* rbd,tests: qa/tasks/rbd_fio: bump default fio version to 2.21 (`pr#16656 <https://github.com/ceph/ceph/pull/16656>`_, Ilya Dryomov)
* rbd,tests: qa/tasks: rbd-mirror daemon not properly run in foreground mode (`issue#20630 <http://tracker.ceph.com/issues/20630>`_, `pr#16340 <https://github.com/ceph/ceph/pull/16340>`_, Jason Dillaman)
* rbd,tests: qa: thrash tests for backoff and upmap (`pr#16428 <https://github.com/ceph/ceph/pull/16428>`_, Ilya Dryomov)
* rbd,tests: qa: update krbd_data_pool.sh to match the new rados ls behavior (`pr#15594 <https://github.com/ceph/ceph/pull/15594>`_, Ilya Dryomov)
* rbd,tests: qa/workunits: adjust path to ceph-helpers.sh (`pr#16599 <https://github.com/ceph/ceph/pull/16599>`_, Sage Weil)
* rbd,tests: qa/workunits: corrected issues with RBD cli test (`pr#14460 <https://github.com/ceph/ceph/pull/14460>`_, Jason Dillaman)
* rbd,tests: qa/workunits/rbd: diff.sh failed removing nonexistent file (`pr#14482 <https://github.com/ceph/ceph/pull/14482>`_, Mykola Golub)
* rbd,tests: qa/workunits/rbd: increased trash deferment period (`pr#14846 <https://github.com/ceph/ceph/pull/14846>`_, Jason Dillaman)
* rbd,tests: qa/workunits/rbd: resolve potential rbd-mirror race conditions (`issue#18935 <http://tracker.ceph.com/issues/18935>`_, `pr#13421 <https://github.com/ceph/ceph/pull/13421>`_, Jason Dillaman)
* rbd,tests: qa/workunits/rbd: test data pool is mirrored correctly (`pr#17077 <https://github.com/ceph/ceph/pull/17077>`_, Mykola Golub)
* rbd,tests: qa/workunits/rbd: tweak rbd-mirror config to spead up testing (`pr#13228 <https://github.com/ceph/ceph/pull/13228>`_, Mykola Golub)
* rbd,tests: qa/workunits: switch to OpenStack Ocata release for RBD testing (`pr#14465 <https://github.com/ceph/ceph/pull/14465>`_, Jason Dillaman)
* rbd,tests: test: correct language mode in file headers (`pr#12924 <https://github.com/ceph/ceph/pull/12924>`_, Jason Dillaman)
* rbd,tests: test: fix compile warning in ceph_test_cls_rbd (`pr#15919 <https://github.com/ceph/ceph/pull/15919>`_, Jason Dillaman)
* rbd,tests: test: fix failing rbd devstack teuthology test (`pr#15956 <https://github.com/ceph/ceph/pull/15956>`_, Jason Dillaman)
* rbd,tests: test/librados_test_stub: fixed cls_cxx_map_get_keys/vals return value (`issue#19597 <http://tracker.ceph.com/issues/19597>`_, `pr#14484 <https://github.com/ceph/ceph/pull/14484>`_, Jason Dillaman)
* rbd,tests: test/librbd: add break_lock test (`pr#12842 <https://github.com/ceph/ceph/pull/12842>`_, Mykola Golub)
* rbd,tests: test/librbd/CMakeLists.txt: ceph_test_librbd_fsx requires linux includes/libs (`pr#13630 <https://github.com/ceph/ceph/pull/13630>`_, Willem Jan Withagen)
* rbd,tests: test/librbd/fsx: Add break in case OP_WRITESAME and OP_COMPARE_AND_WRITE (`pr#16742 <https://github.com/ceph/ceph/pull/16742>`_, Luo Kexue)
* rbd,tests: test/librbd: move tests using non-public api to internal (`pr#13806 <https://github.com/ceph/ceph/pull/13806>`_, Venky Shankar)
* rbd,tests: test/librbd/test_librbd.cc: set \*features even if RBD_FEATURES is unset (`issue#19865 <http://tracker.ceph.com/issues/19865>`_, `pr#14965 <https://github.com/ceph/ceph/pull/14965>`_, Dan Mick)
* rbd,tests: test/librbd/test_notify.py: don't disable feature in slave (`issue#19716 <http://tracker.ceph.com/issues/19716>`_, `pr#14751 <https://github.com/ceph/ceph/pull/14751>`_, Mykola Golub)
* rbd,tests: test/librbd: unit tests cleanup (`pr#15113 <https://github.com/ceph/ceph/pull/15113>`_, Mykola Golub)
* rbd,tests: test: rbd master/slave notify test should test active features (`issue#19692 <http://tracker.ceph.com/issues/19692>`_, `pr#14638 <https://github.com/ceph/ceph/pull/14638>`_, Jason Dillaman)
* rbd,tests: test/rbd_mirror: race in TestMockInstanceWatcher on destroy (`pr#14453 <https://github.com/ceph/ceph/pull/14453>`_, Mykola Golub)
* rbd,tests: test/rbd_mirror: race in TestMockLeaderWatcher.AcquireError (`issue#19405 <http://tracker.ceph.com/issues/19405>`_, `pr#14741 <https://github.com/ceph/ceph/pull/14741>`_, Mykola Golub)
* rbd,tests: test: remove hard-coded image name from RBD metadata test (`issue#19798 <http://tracker.ceph.com/issues/19798>`_, `pr#14848 <https://github.com/ceph/ceph/pull/14848>`_, Jason Dillaman)
* rbd,tests: test: support blacklisting within librados_test_stub (`pr#13737 <https://github.com/ceph/ceph/pull/13737>`_, Jason Dillaman)
* rbd,tests: test/unittest_librbd: remove unused variables (`pr#15720 <https://github.com/ceph/ceph/pull/15720>`_, shiqi)
* rbd,tests: test: use librados API to retrieve config params (`issue#18617 <http://tracker.ceph.com/issues/18617>`_, `pr#13076 <https://github.com/ceph/ceph/pull/13076>`_, Jason Dillaman)
* rbd,tools: rbdmap: consider /etc/ceph/rbdmap when unmapping images (`issue#18884 <http://tracker.ceph.com/issues/18884>`_, `pr#13361 <https://github.com/ceph/ceph/pull/13361>`_, David Disseldorp)
* rbd,tools: tools/rbd_mirror: initialize non-static class member m_do_resync in ImageReplayer (`pr#15889 <https://github.com/ceph/ceph/pull/15889>`_, Jos Collin)
* rbd,tools: tools/rbd_nbd: add --version show support (`pr#16254 <https://github.com/ceph/ceph/pull/16254>`_, Jin Cai)
* rbd: use concurrent writes for imports (`issue#19034 <http://tracker.ceph.com/issues/19034>`_, `pr#13782 <https://github.com/ceph/ceph/pull/13782>`_, Venky Shankar)
* rbd: use min<uint64_t>() explicitly (`issue#18938 <http://tracker.ceph.com/issues/18938>`_, `pr#14202 <https://github.com/ceph/ceph/pull/14202>`_, Kefu Chai)
* rbd: validate pool and snap name optionals (`issue#14535 <http://tracker.ceph.com/issues/14535>`_, `pr#13836 <https://github.com/ceph/ceph/pull/13836>`_, Gaurav Kumar Garg)
* rbd: warning, ‘devno’ may be used uninitialized in this function (`pr#14271 <https://github.com/ceph/ceph/pull/14271>`_, Jos Collin)
* rbd: When Ceph cluster becomes full, should allow user to remove rbd … (`pr#12627 <https://github.com/ceph/ceph/pull/12627>`_, Pan Liu)
* rdma: msg/async: Postpone bind if network stack is not ready (`pr#14414 <https://github.com/ceph/ceph/pull/14414>`_, Amir Vadai, Haomai Wang)
* rdma: msg/async/rdma: Add DSCP support (`pr#15484 <https://github.com/ceph/ceph/pull/15484>`_, Sarit Zubakov)
* rdma: msg/async/rdma: add inqueue rx chunks perf counter (`pr#14782 <https://github.com/ceph/ceph/pull/14782>`_, Haomai Wang)
* rdma: msg/async/rdma: fix log line spacing (`pr#13263 <https://github.com/ceph/ceph/pull/13263>`_, Adir Lev)
* rdma: msg/async/rdma: Make poll_blocking() poll for async events in additio… (`pr#14320 <https://github.com/ceph/ceph/pull/14320>`_, Amir Vadai)
* rdma: msg/async/rdma: Make port number an attribute of the Connection not o… (`pr#14297 <https://github.com/ceph/ceph/pull/14297>`_, Amir Vadai)
* rdma: msg/async/rdma: RDMA-CM, get_device() by ibv_context (`pr#14410 <https://github.com/ceph/ceph/pull/14410>`_, Amir Vadai)
* rdma: msg/async: Revert RDMA-CM (`pr#15262 <https://github.com/ceph/ceph/pull/15262>`_, Amir Vadai)
    Replace using sleep with new wait_for_health() bash function
* rgw: abort early when s->length empty during putobj (`pr#15682 <https://github.com/ceph/ceph/pull/15682>`_, Jiaying Ren)
* rgw: AbortMultipart request returns NoSuchUpload error if the meta obj doesn't exist (`pr#12793 <https://github.com/ceph/ceph/pull/12793>`_, Zhang Shaowen)
* rgw: acl grants num limit (`pr#16291 <https://github.com/ceph/ceph/pull/16291>`_, Enming Zhang)
* rgw: add a new error code for non-existed subuser (`pr#16095 <https://github.com/ceph/ceph/pull/16095>`_, Zhao Chao)
* rgw: add a new error code for non-existed user (`issue#20468 <http://tracker.ceph.com/issues/20468>`_, `pr#16033 <https://github.com/ceph/ceph/pull/16033>`_, Zhao Chao)
* rgw: add apis to support ragweed (`pr#13645 <https://github.com/ceph/ceph/pull/13645>`_, Yehuda Sadeh)
* rgw: add a separate configuration for data notify interval (`pr#16551 <https://github.com/ceph/ceph/pull/16551>`_, fang yuxiang)
* rgw: add bucket size limit check to radosgw-admin (`issue#17925 <http://tracker.ceph.com/issues/17925>`_, `pr#11796 <https://github.com/ceph/ceph/pull/11796>`_, Matt Benjamin)
* rgw: Added a globbing method for AWS Policies (`pr#12445 <https://github.com/ceph/ceph/pull/12445>`_, Pritha Srivastava)
* rgw: Added code for REST APIs for AWS Roles (`pr#12104 <https://github.com/ceph/ceph/pull/12104>`_, Pritha Srivastava)
* rgw: Added code to correctly account for bytes sent/ received during a 'PUT' operation (`pr#14042 <https://github.com/ceph/ceph/pull/14042>`_, Pritha Srivastava)
* rgw: Adding code to create tenanted user for s3 bucket policy tests (`pr#15028 <https://github.com/ceph/ceph/pull/15028>`_, Pritha Srivastava)
* rgw: add lifecycle validation according to S3 (`issue#18394 <http://tracker.ceph.com/issues/18394>`_, `pr#12750 <https://github.com/ceph/ceph/pull/12750>`_, Zhang Shaowen)
* rgw: add missing RGWPeriod::reflect() based on new atomic update_latest_epoch() (`issue#19816 <http://tracker.ceph.com/issues/19816>`_, `issue#19817 <http://tracker.ceph.com/issues/19817>`_, `pr#14915 <https://github.com/ceph/ceph/pull/14915>`_, Casey Bodley)
* rgw: add --num-zonegroups option for multi test (`pr#14216 <https://github.com/ceph/ceph/pull/14216>`_, lvshuhua)
* rgw: add override in header files mostly (`pr#13586 <https://github.com/ceph/ceph/pull/13586>`_, liuchang0812)
* rgw: add override in rgw subsystem (`issue#18922 <http://tracker.ceph.com/issues/18922>`_, `pr#13441 <https://github.com/ceph/ceph/pull/13441>`_, liuchang0812)
* rgw: add pool namespace to cache's key so that system obj can have unique key (`issue#19372 <http://tracker.ceph.com/issues/19372>`_, `pr#14125 <https://github.com/ceph/ceph/pull/14125>`_, Zhang Shaowen)
* rgw: add radosclient finisher to perf counter (`issue#19011 <http://tracker.ceph.com/issues/19011>`_, `pr#13535 <https://github.com/ceph/ceph/pull/13535>`_, lvshuhua)
* rgw: add "rgw_verify_ssl" config (`pr#15301 <https://github.com/ceph/ceph/pull/15301>`_, Shasha Lu)
* rgw: add 'state==SyncState::IncrementalSync' condition when add item … (`pr#14552 <https://github.com/ceph/ceph/pull/14552>`_, Shasha Lu)
* rgw: add support container and object levels of swift bulkupload (`pr#14775 <https://github.com/ceph/ceph/pull/14775>`_, Jing Wenjun)
* rgw: add support for delete marker expiration in s3 lifecycle (`issue#19730 <http://tracker.ceph.com/issues/19730>`_, `pr#14703 <https://github.com/ceph/ceph/pull/14703>`_, Zhang Shaowen)
* rgw: add support for FormPost of Swift API (`issue#17273 <http://tracker.ceph.com/issues/17273>`_, `pr#11179 <https://github.com/ceph/ceph/pull/11179>`_, Radoslaw Zarzynski, Orit Wasserman)
* rgw: add support for multipart upload expiration (`issue#19088 <http://tracker.ceph.com/issues/19088>`_, `pr#13622 <https://github.com/ceph/ceph/pull/13622>`_, Zhang Shaowen)
* rgw: add support for noncurrentversion expiration in s3 lifecycle (`issue#18916 <http://tracker.ceph.com/issues/18916>`_, `pr#13385 <https://github.com/ceph/ceph/pull/13385>`_, Zhang Shaowen)
* rgw: add support for Swift's TempURLs with prefix-based scope (`issue#20398 <http://tracker.ceph.com/issues/20398>`_, `pr#16370 <https://github.com/ceph/ceph/pull/16370>`_, Radoslaw Zarzynski)
* rgw: add support for the BulkUpload of Swift API (`pr#12243 <https://github.com/ceph/ceph/pull/12243>`_, Radoslaw Zarzynski)
* rgw: add the remove-x-delete feature to cancel swift object expiration (`issue#19074 <http://tracker.ceph.com/issues/19074>`_, `pr#13621 <https://github.com/ceph/ceph/pull/13621>`_, Jing Wenjun)
* rgw: add the Vim's modeline into rgw_orphan.cc (`pr#15431 <https://github.com/ceph/ceph/pull/15431>`_, Radoslaw Zarzynski)
* rgw: add variadic string join for s3 signature generation (`pr#15678 <https://github.com/ceph/ceph/pull/15678>`_, Casey Bodley)
* rgw: Add --zonegroup-new-name  in usage (`pr#12084 <https://github.com/ceph/ceph/pull/12084>`_, Hans van den Bogert)
* rgw: allow larger payload for period commit (`issue#19505 <http://tracker.ceph.com/issues/19505>`_, `pr#14355 <https://github.com/ceph/ceph/pull/14355>`_, Casey Bodley)
* rgw: allow system users to read SLO parts (`issue#19027 <http://tracker.ceph.com/issues/19027>`_, `pr#13561 <https://github.com/ceph/ceph/pull/13561>`_, Casey Bodley)
* rgw: auto reshard old buckets (`pr#15665 <https://github.com/ceph/ceph/pull/15665>`_, Orit Wasserman)
* rgw: avoid listing user buckets for rgw_delete_user (`pr#13991 <https://github.com/ceph/ceph/pull/13991>`_, liuchang0812)
* rgw: avoid using null pointer in rgw_file.cc (`pr#14474 <https://github.com/ceph/ceph/pull/14474>`_, lihongjie)
* rgw: be aware abount tenants on cls_user_bucket -> rgw_bucket conversion (`issue#18364 <http://tracker.ceph.com/issues/18364>`_, `issue#16355 <http://tracker.ceph.com/issues/16355>`_, `pr#13220 <https://github.com/ceph/ceph/pull/13220>`_, Radoslaw Zarzynski)
* rgw: bucket index check in radosgw-admin removes valid index (`issue#18470 <http://tracker.ceph.com/issues/18470>`_, `pr#12851 <https://github.com/ceph/ceph/pull/12851>`_, Zhang Shaowen)
* rgw: bucket stats display bucket index type (`pr#14466 <https://github.com/ceph/ceph/pull/14466>`_, fang yuxiang)
* rgw: change default chunk size to 4MB (`issue#18621 <http://tracker.ceph.com/issues/18621>`_, `issue#18622 <http://tracker.ceph.com/issues/18622>`_, `issue#18623 <http://tracker.ceph.com/issues/18623>`_, `pr#13035 <https://github.com/ceph/ceph/pull/13035>`_, Yehuda Sadeh)
* rgw: change loglevel to 20 for 'System already converted' message (`issue#18919 <http://tracker.ceph.com/issues/18919>`_, `pr#13399 <https://github.com/ceph/ceph/pull/13399>`_, Vikhyat Umrao)
* rgw: change loglevel to 5 in user's quota sync (`issue#18921 <http://tracker.ceph.com/issues/18921>`_, `pr#13408 <https://github.com/ceph/ceph/pull/13408>`_, Zhang Shaowen)
* rgw: Changes for s3test config file, to add user under a tenant (`pr#15753 <https://github.com/ceph/ceph/pull/15753>`_, Pritha Srivastava)
* rgw: check placement existence when create bucket (`pr#16385 <https://github.com/ceph/ceph/pull/16385>`_, Jiaying Ren)
* rgw: check placement target existence during bucket creation (`pr#16384 <https://github.com/ceph/ceph/pull/16384>`_, Jiaying Ren)
* rgw: civetweb don't go past the array index while calling mg_start (`issue#19749 <http://tracker.ceph.com/issues/19749>`_, `pr#14750 <https://github.com/ceph/ceph/pull/14750>`_, Abhishek Lekshmanan, Jesse Williamson)
* rgw: clean redundant code (`pr#13302 <https://github.com/ceph/ceph/pull/13302>`_, Yankun Li)
* rgw: clean unuse code in cls_statelog_check_state (`pr#10260 <https://github.com/ceph/ceph/pull/10260>`_, weiqiaomiao)
* rgw: clean-up error mapping in Swift's authentication strategy (`pr#15756 <https://github.com/ceph/ceph/pull/15756>`_, Radoslaw Zarzynski)
* rgw: cleanup: fix variable name in RGWRados::create_pool() declaration (`pr#14547 <https://github.com/ceph/ceph/pull/14547>`_, Nathan Cutler)
* rgw: cleanup lc continuation (`pr#14906 <https://github.com/ceph/ceph/pull/14906>`_, Jiaying Ren)
* rgw: cleanup lifecycle managament (`pr#13820 <https://github.com/ceph/ceph/pull/13820>`_, Jiaying Ren)
* rgw: cleanup rgw-admin duplicated judge during OLH GET/READLOG (`pr#15700 <https://github.com/ceph/ceph/pull/15700>`_, Jiaying Ren)
* rgw: clean up the redundant assignment in last_entry_in_listing (`pr#13387 <https://github.com/ceph/ceph/pull/13387>`_, Jing Wenjun)
* rgw: clean up the unneeded rgw::io::ChunkingFilter::has_content_length (`pr#13504 <https://github.com/ceph/ceph/pull/13504>`_, Radoslaw Zarzynski)
* rgw: cleanup unused codes in rgw_admin.cc (`pr#15771 <https://github.com/ceph/ceph/pull/15771>`_, fang yuxiang)
* rgw: cleanup unused var in rgw/rgw_rest_s3.cc (`pr#13434 <https://github.com/ceph/ceph/pull/13434>`_, Jiaying Ren)
* rgw: clear master_zonegroup when reseting RGWPeriodMap (`issue#17239 <http://tracker.ceph.com/issues/17239>`_, `pr#12658 <https://github.com/ceph/ceph/pull/12658>`_, Orit Wasserman)
* rgw: clear old zone short ids on period update (`issue#15618 <http://tracker.ceph.com/issues/15618>`_, `pr#13949 <https://github.com/ceph/ceph/pull/13949>`_, Casey Bodley)
* rgw: cls: ceph::timespan tag_timeout wrong units (`issue#20380 <http://tracker.ceph.com/issues/20380>`_, `pr#16026 <https://github.com/ceph/ceph/pull/16026>`_, Matt Benjamin)
* rgw: cls/rgw: Clean up the "magic string" usage in the cls layer for RGW (`pr#12536 <https://github.com/ceph/ceph/pull/12536>`_, Ira Cooper)
* rgw: cls/rgw: list_plain_entries() stops before bi_log entries (`issue#19876 <http://tracker.ceph.com/issues/19876>`_, `pr#14981 <https://github.com/ceph/ceph/pull/14981>`_, Casey Bodley)
* rgw: cls/user: cls_user_bucket backward compatibility (`issue#19367 <http://tracker.ceph.com/issues/19367>`_, `pr#14128 <https://github.com/ceph/ceph/pull/14128>`_, Yehuda Sadeh)
* rgw: cls_user don't clobber existing bucket stats when creating bucket (`issue#16357 <http://tracker.ceph.com/issues/16357>`_, `pr#10121 <https://github.com/ceph/ceph/pull/10121>`_, Abhishek Lekshmanan)
* rgw: complete versioning enablement after sending it to meta master (`issue#18003 <http://tracker.ceph.com/issues/18003>`_, `pr#12444 <https://github.com/ceph/ceph/pull/12444>`_, Orit Wasserman)
* rgw: Compress crash bug refactor (`issue#20098 <http://tracker.ceph.com/issues/20098>`_, `pr#15569 <https://github.com/ceph/ceph/pull/15569>`_, Adam Kupczyk)
* rgw: continuation of the auth rework -- AWSv4 (`issue#18800 <http://tracker.ceph.com/issues/18800>`_, `pr#14885 <https://github.com/ceph/ceph/pull/14885>`_, Radoslaw Zarzynski, Javier M. Mellid)
* rgw: continuation of the auth rework (`pr#12893 <https://github.com/ceph/ceph/pull/12893>`_, Radoslaw Zarzynski, Matt Benjamin)
* rgw: Correcting the condition in ceph_assert while parsing an AWS Principal (`pr#15997 <https://github.com/ceph/ceph/pull/15997>`_, Pritha Srivastava)
* rgw: correct the debug info when unlink instance failed (`pr#13761 <https://github.com/ceph/ceph/pull/13761>`_, Zhang Shaowen)
* rgw: Correct the return codes for the health check feature (`issue#19025 <http://tracker.ceph.com/issues/19025>`_, `pr#13557 <https://github.com/ceph/ceph/pull/13557>`_, Pavan Rallabhandi)
* rgw: custom user data header (`issue#19644 <http://tracker.ceph.com/issues/19644>`_, `pr#14592 <https://github.com/ceph/ceph/pull/14592>`_, Pavan Rallabhandi)
* rgw: datalog trim and mdlog trim handles the result returned by osd incorrectly (`issue#20190 <http://tracker.ceph.com/issues/20190>`_, `pr#15507 <https://github.com/ceph/ceph/pull/15507>`_, Zhang Shaowen)
* rgw: data sync includes instance in rgw_obj_index_key (`pr#13948 <https://github.com/ceph/ceph/pull/13948>`_, Casey Bodley)
* rgw: deduplicate variants of rgw_make_bucket_entry_name() (`pr#14299 <https://github.com/ceph/ceph/pull/14299>`_, Radoslaw Zarzynski)
* rgw: delete non-empty buckets in slave zonegroup works not well (`issue#19313 <http://tracker.ceph.com/issues/19313>`_, `pr#14043 <https://github.com/ceph/ceph/pull/14043>`_, Zhang Shaowen)
* rgw: delete object in error path (`issue#20620 <http://tracker.ceph.com/issues/20620>`_, `pr#16324 <https://github.com/ceph/ceph/pull/16324>`_, Yehuda Sadeh)
* rgw: disable dynamic reshading for 1st L point release (`pr#16969 <https://github.com/ceph/ceph/pull/16969>`_, Matt Benjamin)
* rgw: display more info when using  radosgw-admin bucket stats (`pr#15256 <https://github.com/ceph/ceph/pull/15256>`_, fang.yuxiang)
* rgw: Do not decrement stats cache when the cache values are zero (`issue#20661 <http://tracker.ceph.com/issues/20661>`_, `pr#16389 <https://github.com/ceph/ceph/pull/16389>`_, Pavan Rallabhandi)
* rgw: Do not fetch bucket stats by default upon bucket listing (`issue#20377 <http://tracker.ceph.com/issues/20377>`_, `pr#15834 <https://github.com/ceph/ceph/pull/15834>`_, Pavan Rallabhandi)
* rgw: do not log debug output at level 0 (`pr#15633 <https://github.com/ceph/ceph/pull/15633>`_, Jens Rosenboom)
* rgw: don't do unneccesary write if buffer with zero length (`pr#14925 <https://github.com/ceph/ceph/pull/14925>`_, fang yuxiang)
* rgw: don't init rgw_obj from rgw_obj_key when it's incorrect to do so (`issue#19096 <http://tracker.ceph.com/issues/19096>`_, `pr#13676 <https://github.com/ceph/ceph/pull/13676>`_, Yehuda Sadeh)
* rgw: don't log the env_map twice (`pr#13481 <https://github.com/ceph/ceph/pull/13481>`_, Abhishek Lekshmanan)
* rgw: don't read all user input for a few param requests (`pr#13815 <https://github.com/ceph/ceph/pull/13815>`_, Abhishek Lekshmanan)
* rgw: don't return skew time error in pre-signed url (`issue#18828 <http://tracker.ceph.com/issues/18828>`_, `pr#13354 <https://github.com/ceph/ceph/pull/13354>`_, liuchang0812)
* rgw: dont spawn error_repo until lease is acquired (`issue#19446 <http://tracker.ceph.com/issues/19446>`_, `pr#14714 <https://github.com/ceph/ceph/pull/14714>`_, Casey Bodley)
* rgw: don't specify a length when converting bl -> string (`issue#20037 <http://tracker.ceph.com/issues/20037>`_, `pr#15599 <https://github.com/ceph/ceph/pull/15599>`_, Abhishek Lekshmanan)
* rgw: don't use strlen in constexprs to not brake Clang builds (`pr#15688 <https://github.com/ceph/ceph/pull/15688>`_, Radoslaw Zarzynski)
* rgw: drop asio/{yield,coroutine}.hpp replacements (`pr#15413 <https://github.com/ceph/ceph/pull/15413>`_, Kefu Chai)
* rgw: Drop dump_usage_bucket_info() to silence warning from -Wunused-function (`pr#16497 <https://github.com/ceph/ceph/pull/16497>`_, Wei Qiaomiao)
* rgw: drop unused find_replacement() and some function docs (`pr#16386 <https://github.com/ceph/ceph/pull/16386>`_, Jiaying Ren)
* rgw: drop unused function RGWRemoteDataLog::get_shard_info() (`pr#16236 <https://github.com/ceph/ceph/pull/16236>`_, Shasha Lu)
* rgw: drop unused param "bucket" from select_bucket_placement (`pr#14390 <https://github.com/ceph/ceph/pull/14390>`_, Shasha Lu)
* rgw: drop unused port var (`pr#14412 <https://github.com/ceph/ceph/pull/14412>`_, Jiaying Ren)
* rgw: drop unused rgw_pool parameter, local variables and member variable (`pr#16154 <https://github.com/ceph/ceph/pull/16154>`_, Jiaying Ren)
* rgw: drop unused var header_ended (`pr#15686 <https://github.com/ceph/ceph/pull/15686>`_, Jiaying Ren)
* rgw: drop using std ns in header files and other cleanups (`pr#15137 <https://github.com/ceph/ceph/pull/15137>`_, Abhishek Lekshmanan)
* rgw: dynamic resharding (`pr#15493 <https://github.com/ceph/ceph/pull/15493>`_, Yehuda Sadeh, Orit Wasserman)
* rgw: enable to update acl of bucket created in slave zonegroup (`issue#16888 <http://tracker.ceph.com/issues/16888>`_, `pr#14082 <https://github.com/ceph/ceph/pull/14082>`_, Guo Zhandong)
* rgw: error_code in error log is not right when data sync fails (`issue#18437 <http://tracker.ceph.com/issues/18437>`_, `pr#12810 <https://github.com/ceph/ceph/pull/12810>`_, Zhang Shaowen)
* rgw: error more verbosely in RGWRados::create_pool (`pr#14642 <https://github.com/ceph/ceph/pull/14642>`_, Matt Benjamin)
* rgw: external auth engines of S3 honor rgw_keystone_implicit_tenants (`issue#17779 <http://tracker.ceph.com/issues/17779>`_, `pr#15572 <https://github.com/ceph/ceph/pull/15572>`_, Radoslaw Zarzynski)
* rgw: Fix a bug that multipart upload may exceed the quota (`issue#19602 <http://tracker.ceph.com/issues/19602>`_, `pr#12010 <https://github.com/ceph/ceph/pull/12010>`_, Zhang Shaowen)
* rgw: fix asctime when logging in rgw_lc (`pr#16422 <https://github.com/ceph/ceph/pull/16422>`_, Abhishek Lekshmanan)
* rgw: fix break inside of yield in RGWFetchAllMetaCR (`issue#17655 <http://tracker.ceph.com/issues/17655>`_, `pr#11586 <https://github.com/ceph/ceph/pull/11586>`_, Casey Bodley)
* rgw: fix broken /crossdomain.xml, /info and /healthcheck of Swift API (`issue#19520 <http://tracker.ceph.com/issues/19520>`_, `pr#14373 <https://github.com/ceph/ceph/pull/14373>`_, Radoslaw Zarzynski)
* rgw: fix build of conflict after auth rework (`pr#14203 <https://github.com/ceph/ceph/pull/14203>`_, Casey Bodley)
* rgw: fix configurable write obj window size (`pr#13934 <https://github.com/ceph/ceph/pull/13934>`_, hechuang)
* rgw: fix constexpr for string_size in clang (`pr#15738 <https://github.com/ceph/ceph/pull/15738>`_, Adam C. Emerson, Casey Bodley)
* rgw: fix disabling Swift's object versioning through empty X-Versions-Location (`issue#18852 <http://tracker.ceph.com/issues/18852>`_, `pr#13303 <https://github.com/ceph/ceph/pull/13303>`_, Jing Wenjun)
* rgw: Fix duplicate tag removal during GC (`issue#20107 <http://tracker.ceph.com/issues/20107>`_, `pr#15912 <https://github.com/ceph/ceph/pull/15912>`_, Jens Rosenboom)
* rgw: fix error code of inexistence of versions location in swift api (`issue#18880 <http://tracker.ceph.com/issues/18880>`_, `pr#13350 <https://github.com/ceph/ceph/pull/13350>`_, Jing Wenjun)
* rgw: fix error handling in get_params() of RGWPostObj_ObjStore_S3 (`pr#15670 <https://github.com/ceph/ceph/pull/15670>`_, Radoslaw Zarzynski)
* rgw: fix error handling in the link() method of RGWBucket (`issue#20279 <http://tracker.ceph.com/issues/20279>`_, `pr#15669 <https://github.com/ceph/ceph/pull/15669>`_, Radoslaw Zarzynski)
* rgw: fix error message in removing bucket with --bypass-gc flag (`issue#20688 <http://tracker.ceph.com/issues/20688>`_, `pr#16419 <https://github.com/ceph/ceph/pull/16419>`_, Abhishek Varshney)
* rgw: fix err when copy object in bucket with specified placement rule (`issue#20378 <http://tracker.ceph.com/issues/20378>`_, `pr#15837 <https://github.com/ceph/ceph/pull/15837>`_, fang yuxiang)
* rgw: fixes for AWSBrowserUploadAbstractor auth (`issue#20372 <http://tracker.ceph.com/issues/20372>`_, `pr#15882 <https://github.com/ceph/ceph/pull/15882>`_, Radoslaw Zarzynski, Casey Bodley)
* rgw:Fixes typo in rgw_admin.cc (`issue#19026 <http://tracker.ceph.com/issues/19026>`_, `pr#13576 <https://github.com/ceph/ceph/pull/13576>`_, Ronak Jain)
* rgw: fix for broken yields in RGWMetaSyncShardCR (`issue#18076 <http://tracker.ceph.com/issues/18076>`_, `pr#12223 <https://github.com/ceph/ceph/pull/12223>`_, Casey Bodley)
* rgw: fix for EINVAL errors on forwarded bucket put_acl requests (`pr#14376 <https://github.com/ceph/ceph/pull/14376>`_, Casey Bodley)
* rgw: fix for null version_id in fetch_remote_obj() (`pr#14375 <https://github.com/ceph/ceph/pull/14375>`_, Casey Bodley)
* rgw: Fix for Policy Parse exception in case of multiple statements (`pr#16689 <https://github.com/ceph/ceph/pull/16689>`_, Pritha Srivastava)
* rgw: fix forward request for bulkupload to be applied in multisite (`issue#19645 <http://tracker.ceph.com/issues/19645>`_, `pr#14601 <https://github.com/ceph/ceph/pull/14601>`_, Jing Wenjun)
* rgw: fix 'gc list --include-all' command infinite loop the first items (`issue#19978 <http://tracker.ceph.com/issues/19978>`_, `pr#12774 <https://github.com/ceph/ceph/pull/12774>`_, Shasha Lu, fang yuxiang)
* rgw: fix get bucket policy s3 compatible issue (`pr#15280 <https://github.com/ceph/ceph/pull/15280>`_, Enming Zhang)
* rgw: fix handling of --remote in radosgw-admin period commands (`issue#19554 <http://tracker.ceph.com/issues/19554>`_, `pr#14407 <https://github.com/ceph/ceph/pull/14407>`_, Casey Bodley)
* rgw: fix handling RGWUserInfo::system in RGWHandler_REST_SWIFT (`issue#18476 <http://tracker.ceph.com/issues/18476>`_, `pr#12865 <https://github.com/ceph/ceph/pull/12865>`_, Radoslaw Zarzynski)
* rgw: fix infinite loop in rest api for log list (`issue#20386 <http://tracker.ceph.com/issues/20386>`_, `pr#15983 <https://github.com/ceph/ceph/pull/15983>`_, xierui, Casey Bodley)
* rgw: fix init_bucket_for_sync retcode (`pr#13684 <https://github.com/ceph/ceph/pull/13684>`_, Shasha Lu)
* rgw: fix lc list failure when shards not be all created (`issue#19898 <http://tracker.ceph.com/issues/19898>`_, `pr#15025 <https://github.com/ceph/ceph/pull/15025>`_, Jiaying Ren)
* rgw: fix leaks with incomplete multiparts (`issue#17164 <http://tracker.ceph.com/issues/17164>`_, `pr#15630 <https://github.com/ceph/ceph/pull/15630>`_, Abhishek Varshney)
* rgw: fix marker encoding problem (`issue#20463 <http://tracker.ceph.com/issues/20463>`_, `pr#15998 <https://github.com/ceph/ceph/pull/15998>`_, Marcus Watts)
* rgw: fix memory leak in copy_obj_to_remote_dest (`pr#9974 <https://github.com/ceph/ceph/pull/9974>`_, weiqiaomiao)
* rgw: fix memory leak in delete_obj_aio (`pr#13998 <https://github.com/ceph/ceph/pull/13998>`_, wangzhengyong)
* rgw: fix memory leak in RGWGetObjLayout (`pr#14014 <https://github.com/ceph/ceph/pull/14014>`_, liuchang0812)
* rgw: fix memory leaks during Swift Static Website's error handling (`issue#20757 <http://tracker.ceph.com/issues/20757>`_, `pr#16531 <https://github.com/ceph/ceph/pull/16531>`_, Radoslaw Zarzynski)
* rgw: fix not initialized vars which cause rgw crash with ec data pool (`issue#20542 <http://tracker.ceph.com/issues/20542>`_, `pr#16177 <https://github.com/ceph/ceph/pull/16177>`_, Aleksei Gutikov)
* rgw: fix off-by-one in RGWDataChangesLog::get_info (`issue#18488 <http://tracker.ceph.com/issues/18488>`_, `pr#12884 <https://github.com/ceph/ceph/pull/12884>`_, Casey Bodley)
* rgw: fix parse/eval of policy conditions with IfExists (`issue#20708 <http://tracker.ceph.com/issues/20708>`_, `pr#16463 <https://github.com/ceph/ceph/pull/16463>`_, Casey Bodley)
* rgw: fix period update crash (`issue#18631 <http://tracker.ceph.com/issues/18631>`_, `pr#13054 <https://github.com/ceph/ceph/pull/13054>`_, Orit Wasserman)
* rgw: fix potential null pointer dereference in rgw_admin (`pr#15667 <https://github.com/ceph/ceph/pull/15667>`_, Radoslaw Zarzynski)
* rgw: fix radosgw-admin data sync run crash (`issue#20423 <http://tracker.ceph.com/issues/20423>`_, `pr#15938 <https://github.com/ceph/ceph/pull/15938>`_, Shasha Lu)
* rgw: fix radosgw-admin retcode (`pr#15257 <https://github.com/ceph/ceph/pull/15257>`_, Shasha Lu)
* rgw: fix RadosGW hang during multi-chunk upload of AWSv4 (`issue#19754 <http://tracker.ceph.com/issues/19754>`_, `pr#14770 <https://github.com/ceph/ceph/pull/14770>`_, Radoslaw Zarzynski)
* rgw: fix radosgw will crash when service is restarted during lifecycl… (`issue#20756 <http://tracker.ceph.com/issues/20756>`_, `pr#16495 <https://github.com/ceph/ceph/pull/16495>`_, Wei Qiaomiao)
* rgw: fix response header of Swift API (`issue#19443 <http://tracker.ceph.com/issues/19443>`_, `pr#14280 <https://github.com/ceph/ceph/pull/14280>`_, tone-zhang)
* rgw: fix rest client's order of args in get_v2_signature (`pr#15731 <https://github.com/ceph/ceph/pull/15731>`_, Casey Bodley)
* rgw: fix rgw bucket policy IfExists position (`issue#20248 <http://tracker.ceph.com/issues/20248>`_, `pr#15607 <https://github.com/ceph/ceph/pull/15607>`_, yuliyang)
* rgw: fix rgw hang when do RGWRealmReloader::reload after go SIGHUP (`issue#20686 <http://tracker.ceph.com/issues/20686>`_, `pr#16417 <https://github.com/ceph/ceph/pull/16417>`_, fang.yuxiang)
* rgw: fix RGWPutBucketPolicy error when set BucketPolicy again without delete pre set Policy (`issue#20252 <http://tracker.ceph.com/issues/20252>`_, `pr#15617 <https://github.com/ceph/ceph/pull/15617>`_, yuliyang)
* rgw: fix s3 object uploads with chunked transfers and v4 signatures (`issue#20447 <http://tracker.ceph.com/issues/20447>`_, `pr#15965 <https://github.com/ceph/ceph/pull/15965>`_, Marcus Watts)
* rgw: fix segfault in RevokeThread during its shutdown procedure (`issue#19831 <http://tracker.ceph.com/issues/19831>`_, `pr#15033 <https://github.com/ceph/ceph/pull/15033>`_, Radoslaw Zarzynski)
* rgw: fix slave zonegroup cannot enable the bucket versioning (`issue#18003 <http://tracker.ceph.com/issues/18003>`_, `pr#12175 <https://github.com/ceph/ceph/pull/12175>`_, lvshuhua)
* rgw: fix SLO/DLO range requests (`pr#15060 <https://github.com/ceph/ceph/pull/15060>`_, Shasha Lu)
    rgw: fix swift cannot disable object versioning
* rgw: fix swift default auth error after auth strategy refactoring (`pr#15711 <https://github.com/ceph/ceph/pull/15711>`_, Casey Bodley)
* rgw: fix test_multi.py default config file path (`pr#15306 <https://github.com/ceph/ceph/pull/15306>`_, Jiaying Ren)
* rgw: fix the bug that part's index can't be removed after completing multipart upload (`issue#19604 <http://tracker.ceph.com/issues/19604>`_, `pr#14500 <https://github.com/ceph/ceph/pull/14500>`_, Zhang Shaowen)
* rgw: fix the signature mismatch of FormPost in swift API (`issue#20220 <http://tracker.ceph.com/issues/20220>`_, `pr#15564 <https://github.com/ceph/ceph/pull/15564>`_, Jing Wenjun)
* rgw: fix the UTF8 check on bucket entry name in rgw_log_op() (`issue#20779 <http://tracker.ceph.com/issues/20779>`_, `pr#16604 <https://github.com/ceph/ceph/pull/16604>`_, Radoslaw Zarzynski)
* rgw: fix transition from full to incremental meta sync (`pr#13920 <https://github.com/ceph/ceph/pull/13920>`_, Casey Bodley)
* rgw: fix typo in comment (`pr#13578 <https://github.com/ceph/ceph/pull/13578>`_, liuchang0812)
* rgw: fix uninitialized fields (`pr#14120 <https://github.com/ceph/ceph/pull/14120>`_, wangzhengyong)
* rgw: fix upgrate from hammer when zone doesn't have zoneparams (`issue#19231 <http://tracker.ceph.com/issues/19231>`_, `pr#13900 <https://github.com/ceph/ceph/pull/13900>`_, Orit Wasserman)
* rgw: Fix up to 1000 entries at a time in check_bad_index_multipart (`issue#20772 <http://tracker.ceph.com/issues/20772>`_, `pr#16692 <https://github.com/ceph/ceph/pull/16692>`_, Orit Wasserman)
* rgw: fix use of marker in List::list_objects() (`issue#18331 <http://tracker.ceph.com/issues/18331>`_, `pr#13147 <https://github.com/ceph/ceph/pull/13147>`_, Yehuda Sadeh)
* rgw: fix versioned bucket data sync fail when upload is busy (`issue#18208 <http://tracker.ceph.com/issues/18208>`_, `pr#12357 <https://github.com/ceph/ceph/pull/12357>`_, lvshuhua)
* rgw: fix wrong error code for expired Swift TempURL's links (`issue#20384 <http://tracker.ceph.com/issues/20384>`_, `pr#15850 <https://github.com/ceph/ceph/pull/15850>`_, Radoslaw Zarzynski)
* rgw: fix X-Object-Meta-Static-Large-Object in SLO download (`issue#19951 <http://tracker.ceph.com/issues/19951>`_, `pr#15045 <https://github.com/ceph/ceph/pull/15045>`_, Shasha Lu)
* rgw: fix zone did't update realm_id when added to zonegroup (`issue#17995 <http://tracker.ceph.com/issues/17995>`_, `pr#12139 <https://github.com/ceph/ceph/pull/12139>`_, Tianshan Qu)
* rgw: forward RGWPutBucketPolicy to meta master (`issue#20297 <http://tracker.ceph.com/issues/20297>`_, `pr#15736 <https://github.com/ceph/ceph/pull/15736>`_, Casey Bodley)
* rgw: get torrent request's parameter is not the same as amazon s3 (`issue#19136 <http://tracker.ceph.com/issues/19136>`_, `pr#13760 <https://github.com/ceph/ceph/pull/13760>`_, Zhang Shaowen)
* rgw: get wrong content when download object with specific range with compression (`issue#20100 <http://tracker.ceph.com/issues/20100>`_, `pr#15323 <https://github.com/ceph/ceph/pull/15323>`_, fang yuxiang)
* rgw: handle error return value in build_linked_oids_index (`pr#13955 <https://github.com/ceph/ceph/pull/13955>`_, wangzhengyong)
* rgw: http_client clarify the debug msg function call (`pr#13688 <https://github.com/ceph/ceph/pull/13688>`_, Abhishek Lekshmanan)
* rgw: if user.email is empty, dont try to delete (`issue#18980 <http://tracker.ceph.com/issues/18980>`_, `pr#13783 <https://github.com/ceph/ceph/pull/13783>`_, Casey Bodley)
* rgw: implement get/put object tags for S3 (`pr#13753 <https://github.com/ceph/ceph/pull/13753>`_, Abhishek Lekshmanan)
* rgw: improve handling of illformed Swift's container ACLs (`issue#18796 <http://tracker.ceph.com/issues/18796>`_, `pr#13248 <https://github.com/ceph/ceph/pull/13248>`_, Radoslaw Zarzynski)
* rgw: /info claims we do support Swift's accounts ACLs (`issue#20394 <http://tracker.ceph.com/issues/20394>`_, `pr#15887 <https://github.com/ceph/ceph/pull/15887>`_, Radoslaw Zarzynski)
* rgw: initialize non-static class members in ESQueryCompiler (`pr#15884 <https://github.com/ceph/ceph/pull/15884>`_, Jos Collin)
* rgw: initialize Non-static class member val in ESQueryNodeLeafVal_Int (`pr#15888 <https://github.com/ceph/ceph/pull/15888>`_, Jos Collin)
* rgw: initialize Non-static class member worker in RGWReshard (`pr#15886 <https://github.com/ceph/ceph/pull/15886>`_, Jos Collin)
* rgw: Initialize of member variable admin_specified in RGWUserAdminOpState (`pr#16847 <https://github.com/ceph/ceph/pull/16847>`_, amitkuma)
* rgw: Initialize pointer fields (`pr#16021 <https://github.com/ceph/ceph/pull/16021>`_, Jos Collin)
* rgw: LCWorker's worktime is not the same as config rgw_lifecycle_work_time (`issue#18087 <http://tracker.ceph.com/issues/18087>`_, `pr#11963 <https://github.com/ceph/ceph/pull/11963>`_, Zhang Shaowen)
* rgw: ldap: simple_bind() should set ldap version option on tldap (`pr#12616 <https://github.com/ceph/ceph/pull/12616>`_, Weibing Zhang)
* rgw: lease_stack: use reset method instead of assignment (`pr#16185 <https://github.com/ceph/ceph/pull/16185>`_, Nathan Cutler)
* rgw: Let the object stat command be shown in the usage (`issue#19013 <http://tracker.ceph.com/issues/19013>`_, `pr#13291 <https://github.com/ceph/ceph/pull/13291>`_, Pavan Rallabhandi)
* rgw: librgw shut (`issue#18585 <http://tracker.ceph.com/issues/18585>`_, `pr#12972 <https://github.com/ceph/ceph/pull/12972>`_, Matt Benjamin)
* rgw: lifecycle thread shouldn't process the bucket which has been deleted (`issue#20285 <http://tracker.ceph.com/issues/20285>`_, `pr#15677 <https://github.com/ceph/ceph/pull/15677>`_, Zhang Shaowen)
* rgw: lock is not released when set sync marker is failed (`issue#18077 <http://tracker.ceph.com/issues/18077>`_, `pr#12197 <https://github.com/ceph/ceph/pull/12197>`_, Zhang Shaowen)
* rgw: log_meta only for more than one zone (`issue#20357 <http://tracker.ceph.com/issues/20357>`_, `pr#15777 <https://github.com/ceph/ceph/pull/15777>`_, Orit Wasserman, Leo Zhang)
* rgw: lower some log's level in gc process (`pr#15426 <https://github.com/ceph/ceph/pull/15426>`_, Zhang Shaowen)
* rgw: luminous: rgw: Fix rgw not responding occasionally when receiving SIGHUP signal (`issue#20962 <http://tracker.ceph.com/issues/20962>`_, `pr#17113 <https://github.com/ceph/ceph/pull/17113>`_, Yao Zongyou)
* rgw: luminous: RGW: Get Bucket ACL does not honor the s3:GetBucketACL action (`issue#21013 <http://tracker.ceph.com/issues/21013>`_, `issue#21056 <http://tracker.ceph.com/issues/21056>`_, `pr#17117 <https://github.com/ceph/ceph/pull/17117>`_, Abhishek Lekshmanan)
* rgw: luminous: rgw: GetObject Tagging needs to exit earlier if the object has no attributes (`issue#21054 <http://tracker.ceph.com/issues/21054>`_, `issue#21010 <http://tracker.ceph.com/issues/21010>`_, `pr#17118 <https://github.com/ceph/ceph/pull/17118>`_, Abhishek Lekshmanan)
* rgw: luminous: rgw_lc: support for AWSv4 authentication (`pr#16914 <https://github.com/ceph/ceph/pull/16914>`_, Abhishek Lekshmanan)
* rgw: luminous: rgw: S3 v4 auth fails when query string contains (`issue#21000 <http://tracker.ceph.com/issues/21000>`_, `issue#21003 <http://tracker.ceph.com/issues/21003>`_, `issue#20501 <http://tracker.ceph.com/issues/20501>`_, `issue#21043 <http://tracker.ceph.com/issues/21043>`_, `pr#17114 <https://github.com/ceph/ceph/pull/17114>`_, Zhang Shaowen, Marcus Watts)
* rgw: luminous: rgw: Use namespace for lc_pool and roles_pool (`issue#20177 <http://tracker.ceph.com/issues/20177>`_, `issue#20967 <http://tracker.ceph.com/issues/20967>`_, `pr#16943 <https://github.com/ceph/ceph/pull/16943>`_, Orit Wasserman)
* rgw: make RGWEnv return a const ref. to its map (`pr#15269 <https://github.com/ceph/ceph/pull/15269>`_, Abhishek Lekshmanan)
* rgw: make sending Content-Length in 204 and 304 responses controllable (`issue#16602 <http://tracker.ceph.com/issues/16602>`_, `pr#10156 <https://github.com/ceph/ceph/pull/10156>`_, Radoslaw Zarzynski)
* rgw: make sync thread name clear (`issue#18860 <http://tracker.ceph.com/issues/18860>`_, `pr#13324 <https://github.com/ceph/ceph/pull/13324>`_, lvshuhua)
* rgw: match wildcards in StringLike policy conditions (`issue#20308 <http://tracker.ceph.com/issues/20308>`_, `pr#16491 <https://github.com/ceph/ceph/pull/16491>`_, Casey Bodley)
* rgw: metadata search part 2 (`pr#14351 <https://github.com/ceph/ceph/pull/14351>`_, Yehuda Sadeh)
* rgw: meta sync thread crash at RGWMetaSyncShardCR (`issue#20251 <http://tracker.ceph.com/issues/20251>`_, `pr#15660 <https://github.com/ceph/ceph/pull/15660>`_, fang.yuxiang)
* rgw: migrate atomic_t to std::atomic<> (ebirah) (`pr#14839 <https://github.com/ceph/ceph/pull/14839>`_, Jesse Williamson)
* rgw: migrate atomic_t to std::atomic<> (`pr#15001 <https://github.com/ceph/ceph/pull/15001>`_, Jesse Williamson)
* rgw: modify email to empty by admin RESTful api doesn't work (`pr#16309 <https://github.com/ceph/ceph/pull/16309>`_, fang.yuxiang)
* rgw: move the S3 anonymous auth handling to a dedicated engine (`pr#16485 <https://github.com/ceph/ceph/pull/16485>`_, Radoslaw Zarzynski)
* rgw: multipart copy-part remove '/' for s3 java sdk request header (`issue#20075 <http://tracker.ceph.com/issues/20075>`_, `pr#15283 <https://github.com/ceph/ceph/pull/15283>`_, root)
* rgw: multisite enabled over multiple clusters (`pr#12535 <https://github.com/ceph/ceph/pull/12535>`_, Ali Maredia)
* rgw: multisite: fixes for zonegroup redirect (`issue#19488 <http://tracker.ceph.com/issues/19488>`_, `pr#14319 <https://github.com/ceph/ceph/pull/14319>`_, Casey Bodley)
* rgw:multisite: fix RGWRadosRemoveOmapKeysCR and change cn to intrusive_ptr (`issue#20539 <http://tracker.ceph.com/issues/20539>`_, `pr#16197 <https://github.com/ceph/ceph/pull/16197>`_, Shasha Lu)
* rgw: never let http_redirect_code of RGWRedirectInfo to stay uninitialized (`issue#20774 <http://tracker.ceph.com/issues/20774>`_, `pr#16601 <https://github.com/ceph/ceph/pull/16601>`_, Radoslaw Zarzynski)
* rgw: omit X-Account-Access-Control if there is no grant to serialize (`issue#20395 <http://tracker.ceph.com/issues/20395>`_, `pr#15883 <https://github.com/ceph/ceph/pull/15883>`_, Radoslaw Zarzynski)
* rgw: only log metadata on metadata master zone (`issue#20244 <http://tracker.ceph.com/issues/20244>`_, `pr#15613 <https://github.com/ceph/ceph/pull/15613>`_, Casey Bodley)
* rgw: optimize data sync. Add zones_trace in log to avoid needless sync (`issue#19219 <http://tracker.ceph.com/issues/19219>`_, `pr#13851 <https://github.com/ceph/ceph/pull/13851>`_, Zhang Shaowen)
* rgw: optimize generating torrent file. Object data won't stay in memory  now (`pr#15153 <https://github.com/ceph/ceph/pull/15153>`_, Zhang Shaowen)
* rgw: orphan: fix error messages (`pr#12782 <https://github.com/ceph/ceph/pull/12782>`_, Weibing Zhang)
* rgw: pass authentication domain to civetweb (`issue#17657 <http://tracker.ceph.com/issues/17657>`_, `pr#12861 <https://github.com/ceph/ceph/pull/12861>`_, Abhishek Lekshmanan)
* rgw: polymorphic error codes (`pr#10690 <https://github.com/ceph/ceph/pull/10690>`_, Pritha Srivastava, Marcus Watts)
* rgw: print is_admin as int instead of __u8 (`pr#12264 <https://github.com/ceph/ceph/pull/12264>`_, Casey Bodley)
* rgw: put object's acl can't work well on the latest object (`issue#18649 <http://tracker.ceph.com/issues/18649>`_, `pr#13078 <https://github.com/ceph/ceph/pull/13078>`_, Zhang Shaowen)
* rgw: radosgw-admin: use zone id when creating a zone (`issue#19498 <http://tracker.ceph.com/issues/19498>`_, `pr#14340 <https://github.com/ceph/ceph/pull/14340>`_, Orit Wasserman)
* rgw: radosgw-admin: warn that 'realm rename' does not update other clusters (`issue#19746 <http://tracker.ceph.com/issues/19746>`_, `pr#14722 <https://github.com/ceph/ceph/pull/14722>`_, Casey Bodley)
* rgw: radosgw, crypto: simplified code in handle_data functions (`pr#15598 <https://github.com/ceph/ceph/pull/15598>`_, Adam Kupczyk)
* rgw: radosgw: fix compilation with cryptopp (`pr#15960 <https://github.com/ceph/ceph/pull/15960>`_, Adam Kupczyk)
* rgw: raise debug level of meta sync logging (`pr#15524 <https://github.com/ceph/ceph/pull/15524>`_, Casey Bodley)
* rgw: raise debug level of RGWPostObj_ObjStore_S3::get_policy (`pr#16203 <https://github.com/ceph/ceph/pull/16203>`_, Shasha Lu)
* rgw: reject request if decoded URI contains \0 in the middle (`issue#20418 <http://tracker.ceph.com/issues/20418>`_, `pr#15953 <https://github.com/ceph/ceph/pull/15953>`_, Radoslaw Zarzynski)
* rgw: remove a redundant judgement in rgw_rados.cc:delete_obj (`pr#11124 <https://github.com/ceph/ceph/pull/11124>`_, Zhang Shaowen)
* rgw: Removed Unwanted headers (`pr#14183 <https://github.com/ceph/ceph/pull/14183>`_, Jos Collin)
* rgw: remove duplicate flush formatter (`pr#12437 <https://github.com/ceph/ceph/pull/12437>`_, Guo Zhandong)
* rgw: remove extra RGWMPObj in rgw_multi.h (`pr#14619 <https://github.com/ceph/ceph/pull/14619>`_, Casey Bodley)
* rgw: remove fastcgi from default rgw frontends (`pr#15098 <https://github.com/ceph/ceph/pull/15098>`_, Casey Bodley)
* rgw: remove invalid read size4 (`issue#18071 <http://tracker.ceph.com/issues/18071>`_, `pr#12767 <https://github.com/ceph/ceph/pull/12767>`_, Matt Benjamin)
    rgw: Remove pessimizing move
* rgw: remove redundant codes in rgw_cache.h (`pr#13902 <https://github.com/ceph/ceph/pull/13902>`_, lihongjie)
* rgw: Remove spurious XML header for GetBucketPolicy (`issue#20247 <http://tracker.ceph.com/issues/20247>`_, `pr#15586 <https://github.com/ceph/ceph/pull/15586>`_, Adam C. Emerson)
* rgw: remove the useless output when listing zonegroups (`pr#16331 <https://github.com/ceph/ceph/pull/16331>`_, Zhang Shaowen)
* rgw: remove unused func in rgw_file.h (`pr#15698 <https://github.com/ceph/ceph/pull/15698>`_, lihongjie)
* rgw: remove useless --tier_type in radosgw-admin (`pr#13856 <https://github.com/ceph/ceph/pull/13856>`_, Zhang Shaowen)
* rgw: rename s3_code to err_code for swift (`pr#12300 <https://github.com/ceph/ceph/pull/12300>`_, Guo Zhandong)
* rgw: Replace get_zonegroup().is_master_zonegroup() with is_meta_master() in RGWBulkDelete::Deleter::delete_single() (`pr#16062 <https://github.com/ceph/ceph/pull/16062>`_, Fan Yang)
* rgw: req xml params size limitation error msg (`pr#16310 <https://github.com/ceph/ceph/pull/16310>`_, Enming Zhang)
* rgw: respect Swift's negative, HTTP referer-based ACL grants (`issue#18841 <http://tracker.ceph.com/issues/18841>`_, `pr#14344 <https://github.com/ceph/ceph/pull/14344>`_, Radoslaw Zarzynski)
* rgw: restore admin socket path in mrgw.sh (`pr#16540 <https://github.com/ceph/ceph/pull/16540>`_, Casey Bodley)
* rgw: return the version id in get object and object metadata request (`issue#19370 <http://tracker.ceph.com/issues/19370>`_, `pr#14117 <https://github.com/ceph/ceph/pull/14117>`_, Zhang Shaowen)
* rgw: rgw-admin: fix bucket limit check argparse, div(0) (`pr#15316 <https://github.com/ceph/ceph/pull/15316>`_, Matt Benjamin)
* rgw: rgw-admin: remove deprecated regionmap commands (`issue#18725 <http://tracker.ceph.com/issues/18725>`_, `pr#13963 <https://github.com/ceph/ceph/pull/13963>`_, Casey Bodley)
* rgw: rgw_common: use string::npos for the results of str.find (`pr#14341 <https://github.com/ceph/ceph/pull/14341>`_, Abhishek Lekshmanan)
* rgw: rgw_crypt: log error messages during failures (`pr#16726 <https://github.com/ceph/ceph/pull/16726>`_, Abhishek Lekshmanan)
* rgw: rgw_file: add compression interop to RGW NFS (`issue#20462 <http://tracker.ceph.com/issues/20462>`_, `pr#15989 <https://github.com/ceph/ceph/pull/15989>`_, Matt Benjamin)
* rgw: rgw_file: add lock protection for readdir against gc (`issue#20121 <http://tracker.ceph.com/issues/20121>`_, `pr#15329 <https://github.com/ceph/ceph/pull/15329>`_, Gui Hecheng)
* rgw: rgw_file:  add service map registration (`pr#16251 <https://github.com/ceph/ceph/pull/16251>`_, Matt Benjamin)
* rgw: rgw_file: add timed namespace invalidation (`issue#18651 <http://tracker.ceph.com/issues/18651>`_, `pr#13038 <https://github.com/ceph/ceph/pull/13038>`_, Matt Benjamin)
* rgw: rgw_file: avoid a recursive lane lock in LRU drain (`issue#20374 <http://tracker.ceph.com/issues/20374>`_, `pr#15819 <https://github.com/ceph/ceph/pull/15819>`_, Matt Benjamin)
* rgw: rgw_file: avoid stranding invalid-name bucket handles in fhcache (`issue#19036 <http://tracker.ceph.com/issues/19036>`_, `pr#13590 <https://github.com/ceph/ceph/pull/13590>`_, Matt Benjamin)
* rgw: rgw_file cleanup names (`pr#15568 <https://github.com/ceph/ceph/pull/15568>`_, Gui Hecheng)
* rgw: rgw_file: cleanup virtual keyword on derived functions (`pr#14908 <https://github.com/ceph/ceph/pull/14908>`_, Gui Hecheng)
* rgw: rgw_file: ensure valid_s3_object_name for directories, too (`issue#19066 <http://tracker.ceph.com/issues/19066>`_, `pr#13614 <https://github.com/ceph/ceph/pull/13614>`_, Matt Benjamin)
* rgw: rgw_file: fix assert upon setattr on bucket (`issue#20287 <http://tracker.ceph.com/issues/20287>`_, `pr#15679 <https://github.com/ceph/ceph/pull/15679>`_, Gui Hecheng)
* rgw: rgw_file: fix double unref on rgw_fh for rename (`pr#13988 <https://github.com/ceph/ceph/pull/13988>`_, Gui Hecheng)
* rgw: rgw_file: fix flags set on unsuccessful unlink (`pr#15222 <https://github.com/ceph/ceph/pull/15222>`_, Gui Hecheng)
* rgw: rgw_file:  fix fs_inst progression (`issue#19214 <http://tracker.ceph.com/issues/19214>`_, `pr#13832 <https://github.com/ceph/ceph/pull/13832>`_, Matt Benjamin)
* rgw: rgw_file: fix missing unlock in unlink (`issue#19435 <http://tracker.ceph.com/issues/19435>`_, `pr#14262 <https://github.com/ceph/ceph/pull/14262>`_, Gui Hecheng)
* rgw: rgw_file: fix misuse of make_key_name before make_fhk (`pr#15108 <https://github.com/ceph/ceph/pull/15108>`_, Gui Hecheng)
* rgw: rgw_file: fix non-negative return code for open operation (`pr#14045 <https://github.com/ceph/ceph/pull/14045>`_, Gui Hecheng)
* rgw: rgw_file: fix non-posix errcode EINVAL to ENAMETOOLONG (`pr#13764 <https://github.com/ceph/ceph/pull/13764>`_, Gui Hecheng)
* rgw: rgw_file: fix readdir after dirent-change (`issue#19634 <http://tracker.ceph.com/issues/19634>`_, `pr#14561 <https://github.com/ceph/ceph/pull/14561>`_, Matt Benjamin)
* rgw: rgw_file: fix reversed return value of getattr (`pr#13895 <https://github.com/ceph/ceph/pull/13895>`_, Gui Hecheng)
* rgw: rgw_file:  fix RGWLibFS::setattr for directory objects (`issue#18808 <http://tracker.ceph.com/issues/18808>`_, `pr#13252 <https://github.com/ceph/ceph/pull/13252>`_, Matt Benjamin)
* rgw: rgw_file: fix up potential race condition (`pr#14553 <https://github.com/ceph/ceph/pull/14553>`_, Gui Hecheng)
* rgw: rgw_file: implement reliable has-children check (unlink dir) (`issue#19270 <http://tracker.ceph.com/issues/19270>`_, `pr#13953 <https://github.com/ceph/ceph/pull/13953>`_, Matt Benjamin)
* rgw: rgw_file:  interned RGWFileHandle objects need parent refs (`issue#18650 <http://tracker.ceph.com/issues/18650>`_, `pr#13084 <https://github.com/ceph/ceph/pull/13084>`_, Matt Benjamin)
* rgw: rgw_file: posix style atime,ctime,mtime (`pr#13765 <https://github.com/ceph/ceph/pull/13765>`_, Gui Hecheng)
* rgw: rgw_file:  pre-compute unix attrs in write_finish() (`issue#19653 <http://tracker.ceph.com/issues/19653>`_, `pr#14609 <https://github.com/ceph/ceph/pull/14609>`_, Matt Benjamin)
* rgw: rgw_file: prevent conflict of mkdir between restarts (`issue#20275 <http://tracker.ceph.com/issues/20275>`_, `pr#15655 <https://github.com/ceph/ceph/pull/15655>`_, Gui Hecheng)
* rgw: rgw_file: properly & or flags (`issue#20663 <http://tracker.ceph.com/issues/20663>`_, `pr#16448 <https://github.com/ceph/ceph/pull/16448>`_, Matt Benjamin)
* rgw: rgw_file: release rgw_fh lock and ref on ENOTEMPTY (`issue#20061 <http://tracker.ceph.com/issues/20061>`_, `pr#15246 <https://github.com/ceph/ceph/pull/15246>`_, Matt Benjamin)
* rgw: rgw_file: removed extra rele() on fs in rgw_umount() (`pr#15152 <https://github.com/ceph/ceph/pull/15152>`_, Gui Hecheng)
* rgw: rgw_file: remove hidden uxattr objects from buckets on delete (`issue#20045 <http://tracker.ceph.com/issues/20045>`_, `pr#15210 <https://github.com/ceph/ceph/pull/15210>`_, Matt Benjamin)
* rgw: rgw_file:  remove post-unlink lookup check (`issue#20047 <http://tracker.ceph.com/issues/20047>`_, `pr#15216 <https://github.com/ceph/ceph/pull/15216>`_, Matt Benjamin)
* rgw: rgw_file: replace raw fs->fh_lru.unref with predefined fs->unref (`pr#15541 <https://github.com/ceph/ceph/pull/15541>`_, Gui Hecheng)
* rgw: rgw_file: RGWFileHandle dtor must also cond-unlink from FHCache (`issue#19112 <http://tracker.ceph.com/issues/19112>`_, `pr#13712 <https://github.com/ceph/ceph/pull/13712>`_, Matt Benjamin)
* rgw: rgw_file skip policy read for virtual components (`pr#16034 <https://github.com/ceph/ceph/pull/16034>`_, Gui Hecheng)
* rgw: rgw_file:  split last argv on ws, if provided (`pr#12965 <https://github.com/ceph/ceph/pull/12965>`_, Matt Benjamin)
* rgw: rgw_file: store bucket uxattrs on the bucket (`issue#20082 <http://tracker.ceph.com/issues/20082>`_, `pr#15293 <https://github.com/ceph/ceph/pull/15293>`_, Matt Benjamin)
* rgw: rgw_file: support readdir cb type hints (plus fixes) (`issue#19623 <http://tracker.ceph.com/issues/19623>`_, `issue#19625 <http://tracker.ceph.com/issues/19625>`_, `issue#19624 <http://tracker.ceph.com/issues/19624>`_, `pr#14458 <https://github.com/ceph/ceph/pull/14458>`_, Matt Benjamin)
* rgw: rgw_file:  use fh_hook::is_linked() to check residence (`issue#19111 <http://tracker.ceph.com/issues/19111>`_, `pr#13703 <https://github.com/ceph/ceph/pull/13703>`_, Matt Benjamin)
* rgw: rgw_file: v3: fix write-timer action (`issue#19932 <http://tracker.ceph.com/issues/19932>`_, `pr#15097 <https://github.com/ceph/ceph/pull/15097>`_, Matt Benjamin)
* rgw: rgw : fix race in RGWCompleteMultipart (`issue#20861 <http://tracker.ceph.com/issues/20861>`_, `pr#16732 <https://github.com/ceph/ceph/pull/16732>`_, Abhishek Varshney)
* rgw: rgw：fix s3 aws v2 signature priority between header['X-Amz-Date'] and header['Date'] (`issue#20176 <http://tracker.ceph.com/issues/20176>`_, `pr#15467 <https://github.com/ceph/ceph/pull/15467>`_, yuliyang)
* rgw:  rgw: fix the subdir without slash of s3 website url (`issue#20307 <http://tracker.ceph.com/issues/20307>`_, `pr#15703 <https://github.com/ceph/ceph/pull/15703>`_, liuhong)
* rgw: rgw_lc: drop a bunch of unused headers (`pr#14342 <https://github.com/ceph/ceph/pull/14342>`_, Abhishek Lekshmanan)
* rgw: rgw_ldap: log the ldap err in case of bind failure (`pr#14781 <https://github.com/ceph/ceph/pull/14781>`_, Abhishek Lekshmanan)
* rgw: rgw/lifecycle: do not send lifecycle rules when GetLifeCycle failed (`issue#19363 <http://tracker.ceph.com/issues/19363>`_, `pr#14160 <https://github.com/ceph/ceph/pull/14160>`_, liuchang0812)
* rgw: RGWMetaSyncShardControlCR retries with backoff on all error codes (`issue#19019 <http://tracker.ceph.com/issues/19019>`_, `pr#13546 <https://github.com/ceph/ceph/pull/13546>`_, Casey Bodley)
* rgw: RGWMetaSyncShardCR drops stack refs on destruction (`issue#18412 <http://tracker.ceph.com/issues/18412>`_, `issue#18300 <http://tracker.ceph.com/issues/18300>`_, `pr#12605 <https://github.com/ceph/ceph/pull/12605>`_, Casey Bodley)
* rgw: rgw multisite: automated mdlog trimming (`pr#13111 <https://github.com/ceph/ceph/pull/13111>`_, Casey Bodley)
* rgw: rgw multisite: feature of bucket sync enable/disable (`pr#15801 <https://github.com/ceph/ceph/pull/15801>`_, Zhang Shaowen, Casey Bodley, Zengran Zhang)
* rgw: rgw multisite: fixes for meta sync across periods (`issue#18639 <http://tracker.ceph.com/issues/18639>`_, `pr#13070 <https://github.com/ceph/ceph/pull/13070>`_, Casey Bodley)
* rgw: rgw multisite: fix ref counting of completions (`issue#18414 <http://tracker.ceph.com/issues/18414>`_, `issue#18407 <http://tracker.ceph.com/issues/18407>`_, `pr#12841 <https://github.com/ceph/ceph/pull/12841>`_, Casey Bodley)
* rgw: rgw-multisite: fix the problem of rgw website configure 'RedirectAllRequestsTo' failed to sync to slave zone (`pr#15036 <https://github.com/ceph/ceph/pull/15036>`_, yuliyang)
* rgw: rgw-multisite: fix the problem of rgw website configure request not redirect to metadata master (`pr#15082 <https://github.com/ceph/ceph/pull/15082>`_, yuliyang)
* rgw: rgw multisite: remove the redundant post in OPT_ZONEGROUP_MODIFY (`pr#14359 <https://github.com/ceph/ceph/pull/14359>`_, Jing Wenjun)
* rgw: rgw/multisite: validate bucket location during bucket creation (`pr#15333 <https://github.com/ceph/ceph/pull/15333>`_, Jiaying Ren)
* rgw: RGW NFS: add nfs.rst to doc/radosgw (`pr#15789 <https://github.com/ceph/ceph/pull/15789>`_, Matt Benjamin)
* rgw: rgw_op: remove unused variable iter (`pr#14276 <https://github.com/ceph/ceph/pull/14276>`_, Weibing Zhang)
* rgw: RGWPeriodPusher spawns http thread before cr thread (`issue#19834 <http://tracker.ceph.com/issues/19834>`_, `pr#14936 <https://github.com/ceph/ceph/pull/14936>`_, Casey Bodley)
* rgw: rgw_rados: create sync module instances only if run_sync_thread is set (`issue#19830 <http://tracker.ceph.com/issues/19830>`_, `pr#14994 <https://github.com/ceph/ceph/pull/14994>`_, Abhishek Lekshmanan)
* rgw: rgw_rados drop deprecated global var (`pr#14411 <https://github.com/ceph/ceph/pull/14411>`_, Jiaying Ren)
* rgw: rgw_rados: initialize cur_shard (`pr#15735 <https://github.com/ceph/ceph/pull/15735>`_, Abhishek Lekshmanan)
* rgw: rgw realm set fixes (`issue#18333 <http://tracker.ceph.com/issues/18333>`_, `pr#12731 <https://github.com/ceph/ceph/pull/12731>`_, Orit Wasserman)
* rgw: rgw/rgw_frontend.h: Return negative value for empty uid in RGWLoadGenFrontend::init() (`pr#16204 <https://github.com/ceph/ceph/pull/16204>`_, jimifm)
* rgw: rgw/rgw_main.cc: fix parenteses and function result (`pr#12295 <https://github.com/ceph/ceph/pull/12295>`_, Willem Jan Withagen)
* rgw: rgw/rgw_op:Prevents memory leaks when calling func swift_versioning_copy() fails (`pr#15328 <https://github.com/ceph/ceph/pull/15328>`_, jimifm)
* rgw: rgw/rgw_rados: Remove duplicate calls in RGWRados::finalize() (`pr#15281 <https://github.com/ceph/ceph/pull/15281>`_, jimifm)
* rgw: rgw/rgw_string.h: FreeBSD would like errno.h included (`pr#15737 <https://github.com/ceph/ceph/pull/15737>`_, Willem Jan Withagen)
* rgw: rgw/rgw_swift_auth.cc: using string::back() instead as the C++11 recommend (`pr#14827 <https://github.com/ceph/ceph/pull/14827>`_, liuyuhong)
* rgw: rgw structures rework (`issue#17996 <http://tracker.ceph.com/issues/17996>`_, `issue#19249 <http://tracker.ceph.com/issues/19249>`_, `pr#11485 <https://github.com/ceph/ceph/pull/11485>`_, Yehuda Sadeh)
* rgw: rgw,test: fix rgw placement rule pool config option (`pr#16084 <https://github.com/ceph/ceph/pull/16084>`_, Jiaying Ren)
* rgw: S3 lifecycle now supports expiration date (`pr#15807 <https://github.com/ceph/ceph/pull/15807>`_, Zhang Shaowen)
* rgw: s3 server-side encryption (SSE-C, SSE-KMS) (`pr#11049 <https://github.com/ceph/ceph/pull/11049>`_, Adam Kupczyk, Casey Bodley, Radoslaw Zarzynski)
* rgw: segment fault when shard id out of range (`issue#19732 <http://tracker.ceph.com/issues/19732>`_, `pr#14389 <https://github.com/ceph/ceph/pull/14389>`_, redickwang)
* rgw: set dumpable flag after setuid post ff0e521 (`issue#19089 <http://tracker.ceph.com/issues/19089>`_, `pr#13657 <https://github.com/ceph/ceph/pull/13657>`_, Brad Hubbard)
* rgw: set FCGI_INCLUDE_DIR for cephd_rgw_base (`issue#18918 <http://tracker.ceph.com/issues/18918>`_, `pr#13393 <https://github.com/ceph/ceph/pull/13393>`_, David Disseldorp)
* rgw: set object accounted size correctly (`issue#20071 <http://tracker.ceph.com/issues/20071>`_, `pr#14950 <https://github.com/ceph/ceph/pull/14950>`_, fang yuxiang)
* rgw: set placement rule properly (`pr#15221 <https://github.com/ceph/ceph/pull/15221>`_, fang.yuxiang)
* rgw: should delete in_stream_req  if conn->get_obj(...) return not zero value (`pr#9950 <https://github.com/ceph/ceph/pull/9950>`_, weiqiaomiao)
* rgw: should not restrict location_constraint same when user not provide (`pr#16770 <https://github.com/ceph/ceph/pull/16770>`_, Tianshan Qu)
* rgw: should unlock when reshard_log->update() reture non-zero in RGWB… (`pr#16502 <https://github.com/ceph/ceph/pull/16502>`_, Wei Qiaomiao)
* rgw: silence compile warning from -Wmaybe-uninitialized (`pr#15996 <https://github.com/ceph/ceph/pull/15996>`_, Jiaying Ren)
* rgw: silence warning from -Wmaybe-uninitialized (`pr#15949 <https://github.com/ceph/ceph/pull/15949>`_, Jos Collin)
* rgw: stat requests skip compression, manifest handling, etc (`pr#14109 <https://github.com/ceph/ceph/pull/14109>`_, Casey Bodley)
* rgw: Support certain archaic and antiquated distributions(`pr#15498 <https://github.com/ceph/ceph/pull/15498>`_, Adam C. Emerson)
* rgw: swift: ability to update swift read and write acls separately (`issue#19289 <http://tracker.ceph.com/issues/19289>`_, `pr#14499 <https://github.com/ceph/ceph/pull/14499>`_, Marcus Watts)
* rgw: swift: disable revocation thread if sleep == 0 (`issue#19499 <http://tracker.ceph.com/issues/19499>`_, `issue#9493 <http://tracker.ceph.com/issues/9493>`_, `pr#14501 <https://github.com/ceph/ceph/pull/14501>`_, Marcus Watts)
* rgw: swift: fix anonymous user's error code of getting object (`issue#18806 <http://tracker.ceph.com/issues/18806>`_, `pr#13242 <https://github.com/ceph/ceph/pull/13242>`_, Jing Wenjun)
* rgw: swift: the http referer acl in swift API should be shown (`issue#18665 <http://tracker.ceph.com/issues/18665>`_, `pr#13003 <https://github.com/ceph/ceph/pull/13003>`_, Jing Wenjun)
* rgw: swift: The http referer should be parsed to compare in swift API (`issue#18685 <http://tracker.ceph.com/issues/18685>`_, `pr#13005 <https://github.com/ceph/ceph/pull/13005>`_, Jing Wenjun)
* rgw: switch from "timegm()" to "internal_timegm()" for better portability (`issue#12863 <http://tracker.ceph.com/issues/12863>`_, `pr#14327 <https://github.com/ceph/ceph/pull/14327>`_, Rishabh Kumar)
* rgw: switch to std::array in RGWBulkUploadOp due to C++11 and FreeBSD (`pr#14314 <https://github.com/ceph/ceph/pull/14314>`_, Radoslaw Zarzynski)
* rgw: sync status compares the current master period (`issue#18064 <http://tracker.ceph.com/issues/18064>`_, `pr#12907 <https://github.com/ceph/ceph/pull/12907>`_, Abhishek Lekshmanan)
* rgw: test,rgw: fix rgw placement rule pool config option (`pr#16380 <https://github.com/ceph/ceph/pull/16380>`_, Jiaying Ren)
* rgw,tests: luminous: qa/rgw: use 'ceph osd pool application enable' on created pools (`pr#17259 <https://github.com/ceph/ceph/pull/17259>`_, Casey Bodley)
* rgw,tests: qa/rgw: add cluster name to path when s3tests scans rgw log (`pr#14845 <https://github.com/ceph/ceph/pull/14845>`_, Casey Bodley)
* rgw,tests: qa/rgw: add configuration for server-side encryption tests (`pr#13597 <https://github.com/ceph/ceph/pull/13597>`_, Casey Bodley)
* rgw,tests: qa/rgw: add encryption config for s3tests under thrash (`pr#15694 <https://github.com/ceph/ceph/pull/15694>`_, Casey Bodley)
* rgw,tests: qa/rgw: add multisite suite to configure and run multisite tests (`pr#14688 <https://github.com/ceph/ceph/pull/14688>`_, Casey Bodley)
* rgw,tests: qa/rgw: disable lifecycle tests because of expiration failures (`pr#16760 <https://github.com/ceph/ceph/pull/16760>`_, Casey Bodley)
* rgw,tests: qa/rgw: don't scan radosgw logs for encryption keys on jewel upgrade test (`pr#14697 <https://github.com/ceph/ceph/pull/14697>`_, Casey Bodley)
* rgw,tests: qa/rgw: fix assertions in radosgw_admin task (`pr#14842 <https://github.com/ceph/ceph/pull/14842>`_, Casey Bodley)
* rgw,tests: qa/rgw: remove apache/fastcgi and radosgw-agent tests (`pr#15184 <https://github.com/ceph/ceph/pull/15184>`_, Casey Bodley)
* rgw,tests: qa/suites/rgw/thrash: add osd thrashing tests (`pr#13445 <https://github.com/ceph/ceph/pull/13445>`_, Sage Weil)
* rgw,tests: qa/tasks: S3A hadoop task to test s3a with Ceph (`pr#14624 <https://github.com/ceph/ceph/pull/14624>`_, Vasu Kulkarni)
* rgw,tests: test/rgw: add bucket acl and versioning tests to test_multi.py (`pr#12449 <https://github.com/ceph/ceph/pull/12449>`_, Casey Bodley)
* rgw,tests: test/rgw: add test for versioned object sync (`pr#12474 <https://github.com/ceph/ceph/pull/12474>`_, Casey Bodley)
* rgw,tests: test/rgw: fixes for test_multi_period_incremental_sync() (`pr#13067 <https://github.com/ceph/ceph/pull/13067>`_, Casey Bodley)
* rgw,tests: test/rgw: fix for empty lists as default arguments (`pr#14816 <https://github.com/ceph/ceph/pull/14816>`_, Casey Bodley)
* rgw,tests: test/rgw: refactor test_multi.py for use in qa suite (`pr#14433 <https://github.com/ceph/ceph/pull/14433>`_, Casey Bodley)
* rgw,tests: test/rgw: test_bucket_delete_notempty in test_multi.py (`pr#14090 <https://github.com/ceph/ceph/pull/14090>`_, Casey Bodley)
* rgw,tests: vstart: add rgw configuration needed to pass all s3tests (`pr#15782 <https://github.com/ceph/ceph/pull/15782>`_, Casey Bodley)
* rgw,tests: vstart: remove rgw_enable_static_website (`pr#15856 <https://github.com/ceph/ceph/pull/15856>`_, Casey Bodley)
* rgw: the swift container acl should support field .ref (`issue#18484 <http://tracker.ceph.com/issues/18484>`_, `pr#12874 <https://github.com/ceph/ceph/pull/12874>`_, Jing Wenjun)
* rgw: Turn off fcgi as a frontend (`issue#16784 <http://tracker.ceph.com/issues/16784>`_, `pr#15070 <https://github.com/ceph/ceph/pull/15070>`_, Thomas Serlin)
* rgw: Uninitialized member in LCRule (`pr#15827 <https://github.com/ceph/ceph/pull/15827>`_, Jos Collin)
* rgw: update Beast for streaming reads in asio frontend (`pr#14273 <https://github.com/ceph/ceph/pull/14273>`_, Casey Bodley)
* rgw: update bucket cors in secondary zonegroup should forward to master (`issue#16888 <http://tracker.ceph.com/issues/16888>`_, `pr#15260 <https://github.com/ceph/ceph/pull/15260>`_, Shasha Lu)
* rgw: update function doc in rgw_rados.h  and rgw_rados.cc (`pr#15803 <https://github.com/ceph/ceph/pull/15803>`_, Jiaying Ren)
* rgw: update is_truncated in function rgw_read_user_buckets (`issue#19365 <http://tracker.ceph.com/issues/19365>`_, `pr#14343 <https://github.com/ceph/ceph/pull/14343>`_, liuchang0812)
* rgw: usage (`issue#16191 <http://tracker.ceph.com/issues/16191>`_, `pr#14287 <https://github.com/ceph/ceph/pull/14287>`_, Ji Chen, Orit Wasserman)
* rgw: use 64-bit offsets for compression (`issue#20231 <http://tracker.ceph.com/issues/20231>`_, `pr#15656 <https://github.com/ceph/ceph/pull/15656>`_, Adam Kupczyk, fang yuxiang)
* rgw: use a namespace for rgw reshard pool for upgrades as well (`issue#20289 <http://tracker.ceph.com/issues/20289>`_, `pr#16368 <https://github.com/ceph/ceph/pull/16368>`_, Karol Mroz, Abhishek Lekshmanan)
* rgw: Use comparison instead of assignment (`pr#16653 <https://github.com/ceph/ceph/pull/16653>`_, amitkuma)
* rgw: Use decoded URI when verifying TempURL (`issue#18590 <http://tracker.ceph.com/issues/18590>`_, `pr#13007 <https://github.com/ceph/ceph/pull/13007>`_, Michal Koutný)
* rgw: use get_data_extra_pool() when get extra pool (`issue#20064 <http://tracker.ceph.com/issues/20064>`_, `pr#15219 <https://github.com/ceph/ceph/pull/15219>`_, fang yuxiang)
* rgw: use pre-defined calls to replace raw flag operation (`pr#15107 <https://github.com/ceph/ceph/pull/15107>`_, Gui Hecheng)
* rgw: use rgw_zone_root_pool for region_map like is done in hammer (`issue#19195 <http://tracker.ceph.com/issues/19195>`_, `pr#13928 <https://github.com/ceph/ceph/pull/13928>`_, Orit Wasserman)
* rgw: use separate http_manager for read_sync_status (`issue#19236 <http://tracker.ceph.com/issues/19236>`_, `pr#13660 <https://github.com/ceph/ceph/pull/13660>`_, Shasha Lu)
* rgw: use uncompressed size for range_to_ofs() in slo/dlo (`pr#15931 <https://github.com/ceph/ceph/pull/15931>`_, Casey Bodley)
* rgw: using RGW_OBJ_NS_MULTIPART in check_bad_index_multipart (`pr#15774 <https://github.com/ceph/ceph/pull/15774>`_, Shasha Lu)
* rgw: using the same bucket num_shards as master zg when create bucket in secondary zg (`issue#19745 <http://tracker.ceph.com/issues/19745>`_, `pr#14388 <https://github.com/ceph/ceph/pull/14388>`_, Shasha Lu)
* rgw: validate tenant names during user create (`pr#16442 <https://github.com/ceph/ceph/pull/16442>`_, Abhishek Lekshmanan)
* rgw: verify md5 in post obj (`issue#19739 <http://tracker.ceph.com/issues/19739>`_, `pr#14961 <https://github.com/ceph/ceph/pull/14961>`_, Yehuda Sadeh)
* rgw: version id doesn't work in fetch_remote_obj (`pr#14010 <https://github.com/ceph/ceph/pull/14010>`_, Zhang Shaowen)
* rgw: VersionIdMarker and NextVersionIdMarker should be returned when listing object versions (`issue#19886 <http://tracker.ceph.com/issues/19886>`_, `pr#15014 <https://github.com/ceph/ceph/pull/15014>`_, Zhang Shaowen)
* rgw: warning, output may be truncated before the last format character (`pr#14194 <https://github.com/ceph/ceph/pull/14194>`_, Jos Collin)
* rgw: when create_bucket use the same num_shards with info.num_shards (`issue#19745 <http://tracker.ceph.com/issues/19745>`_, `pr#15010 <https://github.com/ceph/ceph/pull/15010>`_, Shasha Lu)
* rgw: wip dir orphan (`issue#18992 <http://tracker.ceph.com/issues/18992>`_, `issue#18989 <http://tracker.ceph.com/issues/18989>`_, `issue#19018 <http://tracker.ceph.com/issues/19018>`_, `issue#18991 <http://tracker.ceph.com/issues/18991>`_, `pr#13529 <https://github.com/ceph/ceph/pull/13529>`_, Matt Benjamin)
* rgw: Wip librgw refcnt (`pr#13405 <https://github.com/ceph/ceph/pull/13405>`_, Matt Benjamin)
* rgw: wip parentref (`issue#19060 <http://tracker.ceph.com/issues/19060>`_, `issue#19059 <http://tracker.ceph.com/issues/19059>`_, `pr#13607 <https://github.com/ceph/ceph/pull/13607>`_, Matt Benjamin)
* rgw: Wip rgw fix prefix list (`issue#19432 <http://tracker.ceph.com/issues/19432>`_, `pr#15916 <https://github.com/ceph/ceph/pull/15916>`_, Giovani Rinaldi, Orit Wasserman)
* rgw: Wip rgw openssl 7 (`issue#11239 <http://tracker.ceph.com/issues/11239>`_, `issue#16535 <http://tracker.ceph.com/issues/16535>`_, `pr#11776 <https://github.com/ceph/ceph/pull/11776>`_, Yehuda Sadeh, Marcus Watts)
* rgw: wip: rgw: rest_admin/user avoid double checking input args (`pr#13460 <https://github.com/ceph/ceph/pull/13460>`_, Abhishek Lekshmanan)
* tests: Add integration tests for admin socket output (`pr#15223 <https://github.com/ceph/ceph/pull/15223>`_, Brad Hubbard)
* tests: add MGR=1 so 'pg dump' won't be blocked (`pr#14266 <https://github.com/ceph/ceph/pull/14266>`_, Kefu Chai)
* tests: Add openstack requirements to smoke suite (`pr#12913 <https://github.com/ceph/ceph/pull/12913>`_, Zack Cerza)
* tests: add setup/teardown for asok dir (`pr#16523 <https://github.com/ceph/ceph/pull/16523>`_, Kefu Chai)
* tests: buildpackages: remove because it does not belong (`issue#18846 <http://tracker.ceph.com/issues/18846>`_, `pr#13297 <https://github.com/ceph/ceph/pull/13297>`_, Loic Dachary)
* tests: ceph-disk: add setting for external py-modules for tox-testing (`pr#15433 <https://github.com/ceph/ceph/pull/15433>`_, Willem Jan Withagen)
* tests: ceph-disk: use communicate() instead of wait() for output (`pr#16347 <https://github.com/ceph/ceph/pull/16347>`_, Kefu Chai)
* tests: ceph-helpers.sh reduce get_timeout_delays() verbosity (`pr#13257 <https://github.com/ceph/ceph/pull/13257>`_, Kefu Chai)
* tests: ceph_objectstore_tool.py: kill all daemons (`pr#14428 <https://github.com/ceph/ceph/pull/14428>`_, Kefu Chai)
* tests: ceph_test_objectstore: tolerate fsck EOPNOTSUPP too (`pr#13325 <https://github.com/ceph/ceph/pull/13325>`_, Sage Weil)
* tests: ceph_test_rados_api_tier: tolerate ENOENT from 'pg scrub' (`pr#14807 <https://github.com/ceph/ceph/pull/14807>`_, Sage Weil)
* tests: ceph_test_rados_api_watch_notify: move global variables into test class (`issue#18395 <http://tracker.ceph.com/issues/18395>`_, `pr#12751 <https://github.com/ceph/ceph/pull/12751>`_, Kefu Chai)
* tests: ceph_test_rados_api_watch_notify: test timeout using rados_wat… (`issue#19312 <http://tracker.ceph.com/issues/19312>`_, `pr#14061 <https://github.com/ceph/ceph/pull/14061>`_, Kefu Chai)
* tests: cephtool/test.sh error on full tests (`issue#19698 <http://tracker.ceph.com/issues/19698>`_, `pr#14647 <https://github.com/ceph/ceph/pull/14647>`_, Willem Jan Withagen, David Zafman)
* tests: cephtool/test.sh: Only delete a test pool when no longer needed (`pr#16443 <https://github.com/ceph/ceph/pull/16443>`_, Willem Jan Withagen)
* tests: cls_lock: move lock_info_t definition to cls_lock_types.h (`pr#16091 <https://github.com/ceph/ceph/pull/16091>`_, runsisi)
* tests: config_opts: drop unused opts (`pr#15031 <https://github.com/ceph/ceph/pull/15031>`_, Kefu Chai)
* tests: Decreased amount of jobs on master, kraken, luminous runs (`pr#17074 <https://github.com/ceph/ceph/pull/17074>`_, Yuri Weinstein)
* tests: Don't dump core when using EXPECT_DEATH (`pr#14821 <https://github.com/ceph/ceph/pull/14821>`_, Kefu Chai, Brad Hubbard)
* tests: drop buildpackages.py (`issue#18846 <http://tracker.ceph.com/issues/18846>`_, `pr#13319 <https://github.com/ceph/ceph/pull/13319>`_, Nathan Cutler)
* tests: drop obsolete Perl scripts (`pr#13951 <https://github.com/ceph/ceph/pull/13951>`_, Nathan Cutler)
* tests: drop rbd_cli_tests.pl and RbdLib.pm (`issue#14825 <http://tracker.ceph.com/issues/14825>`_, `pr#12821 <https://github.com/ceph/ceph/pull/12821>`_, Nathan Cutler)
* tests: drop unused rbd_functional_tests.pl script (`issue#14825 <http://tracker.ceph.com/issues/14825>`_, `pr#12818 <https://github.com/ceph/ceph/pull/12818>`_, Nathan Cutler)
* tests: fio_ceph_objectstore: fixes improper write request data lifetime (`pr#14338 <https://github.com/ceph/ceph/pull/14338>`_, Adam Kupczyk)
* tests: fix broken links in upgrade/hammer-jewel-x/stress-split (`issue#19793 <http://tracker.ceph.com/issues/19793>`_, `pr#14831 <https://github.com/ceph/ceph/pull/14831>`_, Nathan Cutler)
* tests: fix NULL references to be acceptable by Clang (`pr#12880 <https://github.com/ceph/ceph/pull/12880>`_, Willem Jan Withagen)
* tests: fix rados/upgrade/jewel-x-singleton and make workunit task handle repo URLs not ending in ".git" (`issue#20554 <http://tracker.ceph.com/issues/20554>`_, `issue#20368 <http://tracker.ceph.com/issues/20368>`_, `pr#16228 <https://github.com/ceph/ceph/pull/16228>`_, Nathan Cutler, Sage Weil)
* tests: fix regression in qa/tasks/ceph_master.py (`issue#16263 <http://tracker.ceph.com/issues/16263>`_, `pr#13279 <https://github.com/ceph/ceph/pull/13279>`_, Nathan Cutler, Kefu Chai)
* tests: fix template specialization of PromoteRequest class (`pr#12815 <https://github.com/ceph/ceph/pull/12815>`_, Ricardo Dias)
* tests: ignore bogus ceph-objectstore-tool error in ceph_manager (`issue#16263 <http://tracker.ceph.com/issues/16263>`_, `pr#13194 <https://github.com/ceph/ceph/pull/13194>`_, Nathan Cutler)
* tests: include/denc: support ENCODE_DUMP (`pr#14962 <https://github.com/ceph/ceph/pull/14962>`_, Sage Weil)
* tests: libradosstriper: do not assign garbage to returned value (`pr#15009 <https://github.com/ceph/ceph/pull/15009>`_, Kefu Chai)
* tests: luminous: tests: qa/standalone: misc fixes (`issue#20465 <http://tracker.ceph.com/issues/20465>`_, `issue#20921 <http://tracker.ceph.com/issues/20921>`_, `issue#20979 <http://tracker.ceph.com/issues/20979>`_, `pr#16985 <https://github.com/ceph/ceph/pull/16985>`_, David Zafman)
* tests: mgr,os,test: kill clang analyzer warnings (`pr#16227 <https://github.com/ceph/ceph/pull/16227>`_, Kefu Chai)
* tests: move swift.py task from teuthology to ceph, phase one (master) (`issue#20392 <http://tracker.ceph.com/issues/20392>`_, `pr#15859 <https://github.com/ceph/ceph/pull/15859>`_, Nathan Cutler, Sage Weil, Warren Usui, Greg Farnum, Ali Maredia, Tommi Virtanen, Zack Cerza, Sam Lang, Yehuda Sadeh, Joe Buck, Josh Durgin)
* tests: nosetests: use /usr/bin/env to find nosetests (`pr#12091 <https://github.com/ceph/ceph/pull/12091>`_, Willem Jan Withagen)
* tests: os: Argument cannot be negative (`pr#16688 <https://github.com/ceph/ceph/pull/16688>`_, amitkuma)
* tests: os/bluestore,test/ceph_test_objectstore: silence gcc warnings (`pr#13924 <https://github.com/ceph/ceph/pull/13924>`_, Kefu Chai)
* tests: osd-scrub-repair.sh disable scrub backoff in test (`pr#13334 <https://github.com/ceph/ceph/pull/13334>`_, Kefu Chai)
* tests: qa: Added luminous to the mix in schedule_subset.sh (`pr#16430 <https://github.com/ceph/ceph/pull/16430>`_, Yuri Weinstein)
* tests: qa/added overrides (`pr#14917 <https://github.com/ceph/ceph/pull/14917>`_, Yuri Weinstein)
* tests: qa: Add reboot case for systemd test (`issue#19717 <http://tracker.ceph.com/issues/19717>`_, `pr#14229 <https://github.com/ceph/ceph/pull/14229>`_, Vasu Kulkarni)
* tests: qa: add supported distros for ceph-ansible (`pr#13711 <https://github.com/ceph/ceph/pull/13711>`_, Tamil Muthamizhan)
* tests: qa: add task for dnsmasq configuration (`pr#15071 <https://github.com/ceph/ceph/pull/15071>`_, Casey Bodley)
* tests: [qa/ceph-deploy]:  run create mgr nodes as well (`pr#16216 <https://github.com/ceph/ceph/pull/16216>`_, Vasu Kulkarni)
* tests: qa: Cleaned up distros to use `latest` versions (`pr#12804 <https://github.com/ceph/ceph/pull/12804>`_, Yuri Weinstein)
* tests: qa/distros: make centos_latest 7.3 (`pr#12944 <https://github.com/ceph/ceph/pull/12944>`_, Sage Weil)
* tests: qa,doc: document and fix tests for pool application warnings (`pr#16568 <https://github.com/ceph/ceph/pull/16568>`_, Sage Weil)
* tests: qa: do not mention ceph branch explicitly (`pr#13225 <https://github.com/ceph/ceph/pull/13225>`_, Tamil Muthamizhan)
* tests: qa: do not restrict valgrind runs to centos (`issue#18126 <http://tracker.ceph.com/issues/18126>`_, `pr#15893 <https://github.com/ceph/ceph/pull/15893>`_, Greg Farnum)
* tests: qa/erasure-code: override min_size to 2 (`issue#19770 <http://tracker.ceph.com/issues/19770>`_, `pr#14872 <https://github.com/ceph/ceph/pull/14872>`_, Kefu Chai)
* tests: qa: fixed distros links (`pr#12770 <https://github.com/ceph/ceph/pull/12770>`_, Yuri Weinstein)
* tests: qa/run-standalone.sh: fix the find option to be compatible with GNU find (`pr#16646 <https://github.com/ceph/ceph/pull/16646>`_, Kefu Chai)
* tests: qa: specify client for fs workunit (`pr#12914 <https://github.com/ceph/ceph/pull/12914>`_, Tamil Muthamizhan)
* tests: qa: split test_tiering into smaller pieces (`pr#15146 <https://github.com/ceph/ceph/pull/15146>`_, Kefu Chai)
* tests: qa/suite: Added a smoke suite for ceph-ansible (`pr#12610 <https://github.com/ceph/ceph/pull/12610>`_, Tamil Muthamizhan)
* tests: qa/suite: replace reference to fs/xfs.yaml (`pr#14756 <https://github.com/ceph/ceph/pull/14756>`_, Yehuda Sadeh)
* tests: qa/suites/ceph-ansible: removing fs workunit (`pr#12928 <https://github.com/ceph/ceph/pull/12928>`_, Tamil Muthamizhan)
* tests: qa/suites/{ceph-ansible,rest}: OpenStack volumes (`pr#13672 <https://github.com/ceph/ceph/pull/13672>`_, Zack Cerza)
* tests: qa/suites/ceph-deploy: Drop OpenStack volume count (`pr#13706 <https://github.com/ceph/ceph/pull/13706>`_, Zack Cerza)
* tests: qa/suites: drop 'fs' facet, and add 'objectstore' facet where missing (`pr#14198 <https://github.com/ceph/ceph/pull/14198>`_, Sage Weil)
* tests: qa/suites: escape the parenthesis of the whitelist text (`pr#16722 <https://github.com/ceph/ceph/pull/16722>`_, Kefu Chai)
* tests: qa/suites: fix upgrade tests vs cluster full thrashing (`pr#13852 <https://github.com/ceph/ceph/pull/13852>`_, Sage Weil)
* tests: qa/suites/fs: Add openstack volume configuration (`pr#13640 <https://github.com/ceph/ceph/pull/13640>`_, Zack Cerza)
* tests: qa/suites/jewel-x/point-to-point: don't scane for keys on second s3tests either (`pr#14788 <https://github.com/ceph/ceph/pull/14788>`_, Sage Weil)
* tests: qa/suites/kcephfs: Openstack volume configuration (`pr#13634 <https://github.com/ceph/ceph/pull/13634>`_, Zack Cerza)
* tests: qa/suites/{knfs,hadoop,samba}: OpenStack volume configuration (`pr#13637 <https://github.com/ceph/ceph/pull/13637>`_, Zack Cerza)
* tests: qa/suites/krbd: Add openstack volume configuration (`pr#13631 <https://github.com/ceph/ceph/pull/13631>`_, Zack Cerza)
* tests: qa/suites/powercycle/osd/whitelist_health: whitelist more (`pr#17306 <https://github.com/ceph/ceph/pull/17306>`_, Sage Weil)
* tests: qa/suites/powercycle: whitelist health for thrashing (`pr#16759 <https://github.com/ceph/ceph/pull/16759>`_, Sage Weil)
* tests: qa/suites/rados: a bit more whitelisting (`pr#16820 <https://github.com/ceph/ceph/pull/16820>`_, Sage Weil)
* tests: qa/suites/rados: fix ec thrashing (`pr#15087 <https://github.com/ceph/ceph/pull/15087>`_, Sage Weil)
* tests: qa/suites/rados/objectstore: enable experimental features for testing bluestore (`pr#13456 <https://github.com/ceph/ceph/pull/13456>`_, Kefu Chai, Dan Mick)
* tests: qa/suites/rados/singleton/all/erasure-code-nonregression: fix typo (`pr#16579 <https://github.com/ceph/ceph/pull/16579>`_, Sage Weil)
* tests: qa/suites/rados/singleton/all/mon-auth-caps: more osds so we can go clean (`pr#16225 <https://github.com/ceph/ceph/pull/16225>`_, Sage Weil)
* tests: qa/suites/rados/singleton-bluestore: concat settings (`pr#14884 <https://github.com/ceph/ceph/pull/14884>`_, Kefu Chai)
* tests: qa/suites/rados/singleton-nomsgr/all/multi-backfill-reject: sleep longer (`pr#16739 <https://github.com/ceph/ceph/pull/16739>`_, Sage Weil)
* tests: qa/suites/rados/singleton-nomsgr: fix syntax (`pr#15276 <https://github.com/ceph/ceph/pull/15276>`_, Sage Weil)
* tests: qa/suites/rados/thrash: make sure osds have map before legacy scrub (`pr#15117 <https://github.com/ceph/ceph/pull/15117>`_, Sage Weil)
* tests: qa/suites/rados/upgrade: restart mds (`pr#15517 <https://github.com/ceph/ceph/pull/15517>`_, Sage Weil)
* tests: qa/suites: Reduce fs combination tests for smoke, use bluestore (`pr#14854 <https://github.com/ceph/ceph/pull/14854>`_, Vasu Kulkarni)
* tests: qa/suites: Revert "qa/suites: add mon-reweight-min-pgs-per-osd = 4" (`pr#14584 <https://github.com/ceph/ceph/pull/14584>`_, Kefu Chai)
* tests: qa/suites/rgw: Add openstack volume configuration (`pr#13611 <https://github.com/ceph/ceph/pull/13611>`_, Zack Cerza)
* tests: qa/suites/upgarde/jewel-x/parallel: more whitelisting (`pr#16849 <https://github.com/ceph/ceph/pull/16849>`_, Sage Weil)
* tests: qa/suites/upgrade: add tiering test to hammer-jewel-x (`issue#19185 <http://tracker.ceph.com/issues/19185>`_, `pr#13805 <https://github.com/ceph/ceph/pull/13805>`_, Kefu Chai)
* tests: qa/suites/upgrade/hammer-jewel-x: add luminous.yaml (`issue#20342 <http://tracker.ceph.com/issues/20342>`_, `pr#15764 <https://github.com/ceph/ceph/pull/15764>`_, Kefu Chai)
* tests: qa/suites/upgrade/jewel-x: add mgr.x role (`pr#14689 <https://github.com/ceph/ceph/pull/14689>`_, Sage Weil)
* tests: qa/suites/upgrade/jewel-x: misc fixes for new health checks (`pr#16429 <https://github.com/ceph/ceph/pull/16429>`_, Sage Weil)
* tests: qa/suites/upgrade/kraken-x: do not thrash cluster full during upgrade (`issue#19232 <http://tracker.ceph.com/issues/19232>`_, `pr#13892 <https://github.com/ceph/ceph/pull/13892>`_, Dan Mick)
* tests: qa/suites/upgrade/kraken-x: misc fixes (`pr#14887 <https://github.com/ceph/ceph/pull/14887>`_, Sage Weil)
* tests: qa/suites/upgrade/kraken-x (`pr#13517 <https://github.com/ceph/ceph/pull/13517>`_, Sage Weil, Yuri Weinstein)
* tests: qa/suites/upgrade: set "sortbitwise" for jewel clusters (`pr#15661 <https://github.com/ceph/ceph/pull/15661>`_, Kefu Chai)
* tests: qa/suite/upgrade/jewel-x: various fixes (`pr#13734 <https://github.com/ceph/ceph/pull/13734>`_, Sage Weil)
* tests: qa/tasks: assert on pg status with a timeout (`issue#19594 <http://tracker.ceph.com/issues/19594>`_, `pr#14608 <https://github.com/ceph/ceph/pull/14608>`_, Kefu Chai)
* tests: qa/tasks/ceph: debug osd setup (`pr#16841 <https://github.com/ceph/ceph/pull/16841>`_, Sage Weil)
* tests: qa/tasks/ceph-deploy: create-keys explicitly (`pr#12867 <https://github.com/ceph/ceph/pull/12867>`_, Vasu Kulkarni)
* tests: qa/tasks/ceph-deploy: Fix bluestore options for ceph-deploy (`pr#16571 <https://github.com/ceph/ceph/pull/16571>`_, Vasu Kulkarni)
* tests: qa/tasks/ceph-deploy: use the new create option during instantiation (`pr#12892 <https://github.com/ceph/ceph/pull/12892>`_, Vasu Kulkarni)
* tests: qa/tasks/ceph: don't hard-code cluster name when copying fsid (`pr#16212 <https://github.com/ceph/ceph/pull/16212>`_, Jason Dillaman)
* tests: qa/tasks/ceph_manager: always fix pgp_num when done with thrashosd task (`issue#19771 <http://tracker.ceph.com/issues/19771>`_, `pr#14931 <https://github.com/ceph/ceph/pull/14931>`_, Kefu Chai)
* tests: qa/tasks/ceph_manager: 'ceph $service tell ...' is obsolete (`pr#15252 <https://github.com/ceph/ceph/pull/15252>`_, Sage Weil)
* tests: qa/tasks/ceph.py: debug which pgs aren't scrubbing (`pr#13649 <https://github.com/ceph/ceph/pull/13649>`_, Sage Weil)
* tests: qa/tasks/ceph: raise exceptions if scrubbing fails or cannot proceed (`pr#15310 <https://github.com/ceph/ceph/pull/15310>`_, Sage Weil)
* tests: qa/tasks/ceph: should be "Waiting for all PGs", not "all osds" (`pr#16122 <https://github.com/ceph/ceph/pull/16122>`_, Kefu Chai)
* tests: qa/tasks/ceph: wait longer for scrub (`pr#16824 <https://github.com/ceph/ceph/pull/16824>`_, Sage Weil)
* tests: qa/tasks: few fixes to get ceph-deploy 1node to working state (`pr#14400 <https://github.com/ceph/ceph/pull/14400>`_, Vasu Kulkarni)
* tests: qa/tasks/radosbench: increase timeout (`pr#15885 <https://github.com/ceph/ceph/pull/15885>`_, Sage Weil)
* tests: qa/tasks/rebuild_mondb: grant "mgr:allow \*" to client.admin (`issue#19439 <http://tracker.ceph.com/issues/19439>`_, `pr#14284 <https://github.com/ceph/ceph/pull/14284>`_, Kefu Chai)
* tests: qa/tasks/reg11184: use literal 'foo' instead pool_name (`pr#16451 <https://github.com/ceph/ceph/pull/16451>`_, Kefu Chai)
* tests: qa/tasks/repair_test: unset flags we set (`pr#15296 <https://github.com/ceph/ceph/pull/15296>`_, Sage Weil)
* tests: qa/tasks/rgw.py: start Apache before RadosGW (`pr#13846 <https://github.com/ceph/ceph/pull/13846>`_, Radoslaw Zarzynski)
* tests: qa/tasks/thrashosds-health.yaml: ignore MON_DOWN (`issue#20910 <http://tracker.ceph.com/issues/20910>`_, `pr#17003 <https://github.com/ceph/ceph/pull/17003>`_, Sage Weil)
* tests: qa/tasks: use sudo to check ceph health for systemd test (`pr#14464 <https://github.com/ceph/ceph/pull/14464>`_, Vasu Kulkarni)
* tests: qa/tasks/workunit.py: use "overrides" as the default settings of workunit (`issue#19429 <http://tracker.ceph.com/issues/19429>`_, `pr#14281 <https://github.com/ceph/ceph/pull/14281>`_, Kefu Chai)
* tests: qa/tasks/workunit: use ceph.git as an alternative of ceph-ci.git for cloning workunit (`pr#13663 <https://github.com/ceph/ceph/pull/13663>`_, Kefu Chai)
* tests: qa/tasks/workunit: use the suite repo for cloning workunit (`pr#13452 <https://github.com/ceph/ceph/pull/13452>`_, Kefu Chai)
* tests: qa/tasks/workunit: use the suite repo for cloning workunit (`pr#13625 <https://github.com/ceph/ceph/pull/13625>`_, Kefu Chai)
* tests: qa/test_rados_tool.sh: POSIX dd only accepts 'k' as multiplier (`pr#12699 <https://github.com/ceph/ceph/pull/12699>`_, Willem Jan Withagen)
* tests: qa: timeout when waiting for mgr to be available in healthy() (`pr#16797 <https://github.com/ceph/ceph/pull/16797>`_, Josh Durgin)
* tests: qa: Using centos 7.2 for `latest` version (`pr#12806 <https://github.com/ceph/ceph/pull/12806>`_, Yuri Weinstein)
* tests: qa/workunits/ceph-helpers: display rejected string (`issue#20344 <http://tracker.ceph.com/issues/20344>`_, `pr#14468 <https://github.com/ceph/ceph/pull/14468>`_, Kefu Chai)
* tests: qa/workunits/ceph-helpers: enable experimental features for osd (`pr#16319 <https://github.com/ceph/ceph/pull/16319>`_, Kefu Chai)
* tests: qa/workunits/ceph-helpers.sh: use syntax understood by jq 1.3 (`pr#15530 <https://github.com/ceph/ceph/pull/15530>`_, Kefu Chai)
* tests: qa/workunits/ceph-helpers: test wait_for_health_ok differently (`pr#16317 <https://github.com/ceph/ceph/pull/16317>`_, Kefu Chai)
* tests: qa/workunits/ceph-helpers: wait_for_clean() races with pg creation (`pr#12866 <https://github.com/ceph/ceph/pull/12866>`_, David Zafman)
* tests: qa/workunits/cephtool/test.sh: Be more liberal in testing health-output (`pr#14614 <https://github.com/ceph/ceph/pull/14614>`_, Willem Jan Withagen)
* tests: qa/workunits/cephtool/test.sh: "ceph osd stat" output changed, update accordingly (`pr#16444 <https://github.com/ceph/ceph/pull/16444>`_, Willem Jan Withagen, Kefu Chai)
* tests: qa/workunits/cephtool/test.sh: disable 'fs status' until bug is fixed (`issue#20761 <http://tracker.ceph.com/issues/20761>`_, `pr#16541 <https://github.com/ceph/ceph/pull/16541>`_, Sage Weil)
* tests: qa/workunits/cephtool/test.sh: fix test to watch audit channel (`pr#16470 <https://github.com/ceph/ceph/pull/16470>`_, Sage Weil)
* tests: qa/workunits/cephtool/test.sh: only include last line for epoch (`issue#20477 <http://tracker.ceph.com/issues/20477>`_, `pr#15770 <https://github.com/ceph/ceph/pull/15770>`_, Kefu Chai)
* tests: qa/workunits/rados/test.sh: print test name when it fails (`pr#13264 <https://github.com/ceph/ceph/pull/13264>`_, Kefu Chai)
* tests: rados: move cephtool.yaml to new singleton/bluestore subsuite (`issue#19797 <http://tracker.ceph.com/issues/19797>`_, `pr#14847 <https://github.com/ceph/ceph/pull/14847>`_, Nathan Cutler)
* tests: rbd/test_lock_fence.sh: fix rbdrw.py relative path (`issue#18388 <http://tracker.ceph.com/issues/18388>`_, `pr#12747 <https://github.com/ceph/ceph/pull/12747>`_, Nathan Cutler)
* tests: re-enable cephfs python tests on kclient (`issue#17193 <http://tracker.ceph.com/issues/17193>`_, `issue#18161 <http://tracker.ceph.com/issues/18161>`_, `pr#13200 <https://github.com/ceph/ceph/pull/13200>`_, Nathan Cutler)
* tests: remove temporary file (`pr#12919 <https://github.com/ceph/ceph/pull/12919>`_, Kefu Chai)
* tests: Revert "dummy: reduce run time, run user.yaml playbook" (`issue#18259 <http://tracker.ceph.com/issues/18259>`_, `pr#12506 <https://github.com/ceph/ceph/pull/12506>`_, Nathan Cutler)
* tests: Revert "qa/tasks/workunit: use the suite repo for cloning workunit" (`pr#13495 <https://github.com/ceph/ceph/pull/13495>`_, Sage Weil)
* tests: rgw.py: put client roles in a separate list (`issue#20417 <http://tracker.ceph.com/issues/20417>`_, `pr#15913 <https://github.com/ceph/ceph/pull/15913>`_, Nathan Cutler)
* tests: rgw/singleton: drop duplicate filestore-xfs.yaml (`pr#15959 <https://github.com/ceph/ceph/pull/15959>`_, Nathan Cutler)
* tests: set -x in suites/iozone.sh workunit (`issue#19740 <http://tracker.ceph.com/issues/19740>`_, `pr#14713 <https://github.com/ceph/ceph/pull/14713>`_, Nathan Cutler)
* tests: src/test/test_denc.cc: Fix errors in buffer overflow (`pr#12653 <https://github.com/ceph/ceph/pull/12653>`_, Willem Jan Withagen)
* tests: subst repo and branch in git.ceph.com URL in qa/tasks/cram.py and qa/tasks/qemu.py (`issue#18440 <http://tracker.ceph.com/issues/18440>`_, `pr#12816 <https://github.com/ceph/ceph/pull/12816>`_, Nathan Cutler)
* tests: tasks/workunit.py: when cloning, use --depth=1 (`pr#14214 <https://github.com/ceph/ceph/pull/14214>`_, Dan Mick)
* tests: test: add explicit braces to avoid ambiguous ‘else’ and to silence warnings (`pr#14472 <https://github.com/ceph/ceph/pull/14472>`_, Jos Collin)
* tests: test: add override in test submodule (`pr#13773 <https://github.com/ceph/ceph/pull/13773>`_, liuchang0812)
* tests: test: ceph osd stat out has changed, fix tests for that (`pr#16403 <https://github.com/ceph/ceph/pull/16403>`_, Willem Jan Withagen)
* tests: test:Check make_writeable() return value (`pr#15266 <https://github.com/ceph/ceph/pull/15266>`_, zhanglei)
* tests: test: clean up unused variable (`pr#12873 <https://github.com/ceph/ceph/pull/12873>`_, liuchang0812)
* tests: test/CMakeLists: disable test_pidfile.sh (`issue#20975 <http://tracker.ceph.com/issues/20975>`_, `pr#17241 <https://github.com/ceph/ceph/pull/17241>`_, Sage Weil)
* tests: test/compressor: disable isal tests if not available (`pr#14929 <https://github.com/ceph/ceph/pull/14929>`_, Kefu Chai)
* tests: test: c_read_operations.cc: silence warning from -Wsign-compare (`pr#14888 <https://github.com/ceph/ceph/pull/14888>`_, Jos Collin)
* tests: test: create asok files in a temp directory under $TMPDIR (`issue#16895 <http://tracker.ceph.com/issues/16895>`_, `pr#16445 <https://github.com/ceph/ceph/pull/16445>`_, Kefu Chai)
* tests: test/crush: silence warnings from -Walloc-size-larger-than= and -Wstringop-overflow (`pr#15173 <https://github.com/ceph/ceph/pull/15173>`_, Jos Collin)
* tests: test: c_write_operations.cc: silence warning from -Wsign-compare (`pr#14889 <https://github.com/ceph/ceph/pull/14889>`_, Jos Collin)
* tests: test: Division by zero in Legacy::encode_n() (`pr#15902 <https://github.com/ceph/ceph/pull/15902>`_, Jos Collin)
* tests: test/encoding: fix readable.sh bugs; fix ceph-object-corpus (`pr#13678 <https://github.com/ceph/ceph/pull/13678>`_, Sage Weil)
* tests: test/fio_ceph_objectstore: fix fio plugin build failure by engine_data (`pr#15044 <https://github.com/ceph/ceph/pull/15044>`_, lisali)
* tests: test/fio: Fix assert in set_cache_shards in bluestore fio (`pr#15659 <https://github.com/ceph/ceph/pull/15659>`_, Xiaoyan Li)
* tests: test/fio: fix lack of setting for Sequencer::shard_hint (`pr#15571 <https://github.com/ceph/ceph/pull/15571>`_, Igor Fedotov)
* tests: test/fio: print all perfcounters rather than objectstore itself (`pr#16339 <https://github.com/ceph/ceph/pull/16339>`_, Jianpeng Ma)
* tests: test/fio: remove experimental option for bluestore & rocksdb (`pr#16263 <https://github.com/ceph/ceph/pull/16263>`_, Pan Liu)
* tests: test: Fixes for test_pidfile (`issue#20770 <http://tracker.ceph.com/issues/20770>`_, `pr#16587 <https://github.com/ceph/ceph/pull/16587>`_, David Zafman)
* tests: test: fixing assert that creates warning: comparison between signed and unsigned integer expressions (`pr#14794 <https://github.com/ceph/ceph/pull/14794>`_, Jos Collin)
* tests: test: Fix mismatched sign comparison in histogram test (`pr#13362 <https://github.com/ceph/ceph/pull/13362>`_, Adam C. Emerson)
* tests: test: Fix reg11184 test to remove extraneous pg (`pr#16265 <https://github.com/ceph/ceph/pull/16265>`_, David Zafman)
* tests: test: fix test_pidfile (`pr#13646 <https://github.com/ceph/ceph/pull/13646>`_, yaoning)
* tests: test/fsx:  Remove the dead code associated with aio backend (`pr#14905 <https://github.com/ceph/ceph/pull/14905>`_, Zhou Zhengping)
* tests: test: Initialize pointer variables in TestMemIoCtxImpl (`pr#16785 <https://github.com/ceph/ceph/pull/16785>`_, amitkuma)
* tests: test/librados: Initialize member variables in aio.cc (`pr#16845 <https://github.com/ceph/ceph/pull/16845>`_, amitkuma)
* tests: test: librados_test_stub: tmap_update: return -ENOENT when removing nonexisent key (`pr#12667 <https://github.com/ceph/ceph/pull/12667>`_, Mykola Golub)
* tests: test: migrate atomic_t to std::atomic (`pr#14655 <https://github.com/ceph/ceph/pull/14655>`_, Jesse Williamson)
* tests: test,mon,msg: kill clang analyzer warnings (`pr#16320 <https://github.com/ceph/ceph/pull/16320>`_, Kefu Chai)
* tests: test/mon: silence warnings from -Wreorder (`pr#15692 <https://github.com/ceph/ceph/pull/15692>`_, Jos Collin)
* tests: test/msgr: fixed the hang issue for perf_msg_client (`pr#16358 <https://github.com/ceph/ceph/pull/16358>`_, Pan Liu)
* tests: test/msgr: silence warnings from -Wsign-compare (`pr#15356 <https://github.com/ceph/ceph/pull/15356>`_, Jos Collin)
* tests: test/msgr: silence warnings from -Wsign-compare (`pr#15570 <https://github.com/ceph/ceph/pull/15570>`_, Jos Collin)
* tests: test: objectstore: chain_xattr: fix wrong memset usage to fill buf (`pr#14277 <https://github.com/ceph/ceph/pull/14277>`_, Weibing Zhang)
* tests: test/objectstore: Check apply_transaction() return values (`pr#15171 <https://github.com/ceph/ceph/pull/15171>`_, zhanglei)
* tests: test/objectstore/: Check put_ref return value (`pr#15007 <https://github.com/ceph/ceph/pull/15007>`_, zhanglei)
* tests: test/old: Removed commented code (`pr#15366 <https://github.com/ceph/ceph/pull/15366>`_, Jos Collin)
* tests: test/osdc: fix comparison error and silence warning from -Wunused-value (`pr#15353 <https://github.com/ceph/ceph/pull/15353>`_, Willem Jan Withagen)
* tests: test/osd: kill compile warning (`pr#16669 <https://github.com/ceph/ceph/pull/16669>`_, Yan Jun)
* tests: test/osd/osd-dup.sh: lower wb fd throttle limits (`pr#14984 <https://github.com/ceph/ceph/pull/14984>`_, Dan Mick)
* tests: test/osd/osd-dup.sh: use wait_for_clean (`pr#15722 <https://github.com/ceph/ceph/pull/15722>`_, Dan Mick)
* tests: test/osd/osd-scrub-repair.sh: disable ec_overwrite tests on FreeBSD (`pr#15445 <https://github.com/ceph/ceph/pull/15445>`_, Willem Jan Withagen)
* tests: test/osd/osd-scrub-repair.sh: Fix diff options on FreeBSD (`pr#15914 <https://github.com/ceph/ceph/pull/15914>`_, Willem Jan Withagen)
* tests: test/osd: Removed Commented Code - 2 (`issue#20207 <http://tracker.ceph.com/issues/20207>`_, `pr#15540 <https://github.com/ceph/ceph/pull/15540>`_, Jos Collin)
* tests: test: osd/TestOSDMap.cc: fix Clang complain about promotion (`pr#15525 <https://github.com/ceph/ceph/pull/15525>`_, Willem Jan Withagen)
* tests: test/rados: fix wrong parameter order of RETURN1_IF_NOT_VAL (`pr#16589 <https://github.com/ceph/ceph/pull/16589>`_, Yan Jun)
* tests: test: reg11184 might not always find pg 2.0 prior to import (`pr#16610 <https://github.com/ceph/ceph/pull/16610>`_, David Zafman)
* tests: test: Rename FileJournal object to distinguish (`pr#15279 <https://github.com/ceph/ceph/pull/15279>`_, Jos Collin)
* tests: test: replace hard-code binary names with varibles (`pr#12675 <https://github.com/ceph/ceph/pull/12675>`_, liuchang0812)
* tests: test: sed on FreeBSD requires "-i extension", so use gsed (`pr#13903 <https://github.com/ceph/ceph/pull/13903>`_, Willem Jan Withagen)
* tests: test: s/osd_objectstore_type/osd_objectstore (`pr#16469 <https://github.com/ceph/ceph/pull/16469>`_, xie xingguo)
* tests: test: test_denc.cc: silence warning from -Wsign-compare (`pr#15355 <https://github.com/ceph/ceph/pull/15355>`_, Jos Collin)
* tests: test: Test fix for SnapSet change (`pr#15161 <https://github.com/ceph/ceph/pull/15161>`_, David Zafman)
* tests: test: test_pidfile running 2nd mon has unreliable log output (`pr#16635 <https://github.com/ceph/ceph/pull/16635>`_, David Zafman)
* tests: test: Thrasher: do not update pools_to_fix_pgp_num if nothing happens (`pr#13518 <https://github.com/ceph/ceph/pull/13518>`_, Kefu Chai)
* tests: test: Thrasher: update pgp_num of all expanded pools if not yet (`pr#13367 <https://github.com/ceph/ceph/pull/13367>`_, Kefu Chai)
* tests: test/unittest_bluefs: check whether mounted success (`pr#14988 <https://github.com/ceph/ceph/pull/14988>`_, shiqi)
* tests: test/unittest_bluefs: remove unused variable (`pr#14006 <https://github.com/ceph/ceph/pull/14006>`_, shiqi)
* tests: test: unittest_hostname compile error on freebsd (`pr#13739 <https://github.com/ceph/ceph/pull/13739>`_, liuchang0812)
* tests: test: update test_rados_tool.sh, use POOL and OBJ var (`pr#12706 <https://github.com/ceph/ceph/pull/12706>`_, liuchang0812)
* tests: test: use 7130 for crush-classes.sh (`pr#14783 <https://github.com/ceph/ceph/pull/14783>`_, Loic Dachary)
* tests: test/vstart_wrapper.sh: display_log on test failure (`pr#15620 <https://github.com/ceph/ceph/pull/15620>`_, Kefu Chai)
* tests: test: warning: comparison between signed and unsigned integer expressions (`pr#14705 <https://github.com/ceph/ceph/pull/14705>`_, Jos Collin)
* tests: Thrasher: eliminate a race between kill_osd and __init__ (`issue#18799 <http://tracker.ceph.com/issues/18799>`_, `pr#13237 <https://github.com/ceph/ceph/pull/13237>`_, Nathan Cutler)
* tests: Thrasher: handle "OSD has the store locked" gracefully (`issue#19556 <http://tracker.ceph.com/issues/19556>`_, `pr#14415 <https://github.com/ceph/ceph/pull/14415>`_, Nathan Cutler)
* tests,tools: script/find_dups_in_pg_log: scrip to find dup requests due to short pg logs (`pr#13417 <https://github.com/ceph/ceph/pull/13417>`_, Sage Weil)
* tests,tools: test, ceph-osdomap-tool: kill clang warnings (`pr#15905 <https://github.com/ceph/ceph/pull/15905>`_, Kefu Chai)
* tests,tools: test: kill warnings (`pr#14892 <https://github.com/ceph/ceph/pull/14892>`_, Kefu Chai)
* tests: update SUSE yaml facets in qa/distros/all (`issue#18856 <http://tracker.ceph.com/issues/18856>`_, `pr#13313 <https://github.com/ceph/ceph/pull/13313>`_, Nathan Cutler)
* tests: workunit: request branch when cloning (`pr#14260 <https://github.com/ceph/ceph/pull/14260>`_, Kefu Chai, Dan Mick)
* tools: add override in tool submodule (`pr#13776 <https://github.com/ceph/ceph/pull/13776>`_, liuchang0812)
* tools: brag: count the number of mds in fsmap not in mdsmap (`issue#19192 <http://tracker.ceph.com/issues/19192>`_, `pr#13798 <https://github.com/ceph/ceph/pull/13798>`_, Peng Zhang)
* tools: ceph_common.sh: fix syntax error (`issue#17826 <http://tracker.ceph.com/issues/17826>`_, `pr#13419 <https://github.com/ceph/ceph/pull/13419>`_, Dan Mick)
* tools: ceph-conf: fix typo in usage: 'mon add' should be 'mon addr' (`pr#15935 <https://github.com/ceph/ceph/pull/15935>`_, Peng Zhang)
* tools: ceph-create-keys: add an argument to override default 10-minute timeout (`pr#16049 <https://github.com/ceph/ceph/pull/16049>`_, Douglas Fuller)
* tools: ceph-detect-init: adding Arch Linux support (`pr#12787 <https://github.com/ceph/ceph/pull/12787>`_, Jamin W. Collins)
* tools: ceph-detect-init: Adds Oracle Linux Server and Oracle VM Server detect (`pr#13917 <https://github.com/ceph/ceph/pull/13917>`_, Nikita Gerasimov)
* tools: ceph-detect-init: detect init system by poking the system (`issue#19884 <http://tracker.ceph.com/issues/19884>`_, `pr#15043 <https://github.com/ceph/ceph/pull/15043>`_, Kefu Chai)
* tools: ceph-disk: Add fix subcommand (`pr#13310 <https://github.com/ceph/ceph/pull/13310>`_, Boris Ranto)
* tools: ceph-disk: change the lockbox partition number to 5 (`issue#20556 <http://tracker.ceph.com/issues/20556>`_, `pr#16247 <https://github.com/ceph/ceph/pull/16247>`_, Shangzhong Zhu)
* tools: ceph-disk: command invocation needs all fields separate (`pr#15733 <https://github.com/ceph/ceph/pull/15733>`_, Willem Jan Withagen)
* tools: ceph-disk: convert none str to str before printing it (`issue#18371 <http://tracker.ceph.com/issues/18371>`_, `pr#12760 <https://github.com/ceph/ceph/pull/12760>`_, Kefu Chai)
* tools: ceph-disk: do not remove mount point if deactive --once (`pr#16474 <https://github.com/ceph/ceph/pull/16474>`_, Song Shun)
* tools: ceph-disk: Fix for missing 'not' in \*_is_diskdevice checks (`issue#20706 <http://tracker.ceph.com/issues/20706>`_, `pr#16481 <https://github.com/ceph/ceph/pull/16481>`_, Nikita Gerasimov)
* tools: ceph_disk/main.py: FreeBSD root has wheel for group (`pr#16609 <https://github.com/ceph/ceph/pull/16609>`_, Willem Jan Withagen)
* tools: ceph-disk: s/ceph_osd_mkfs/command_check_call/ (`issue#20685 <http://tracker.ceph.com/issues/20685>`_, `pr#16427 <https://github.com/ceph/ceph/pull/16427>`_, Zhu Shangzhong)
* tools: ceph.in: add help for locally-handled commands (`pr#13288 <https://github.com/ceph/ceph/pull/13288>`_, Dan Mick)
* tools: ceph.in: adjust usage width according to user's tty (`pr#15190 <https://github.com/ceph/ceph/pull/15190>`_, Kefu Chai)
* tools: ceph.in: assert(state==connected) before help_for_target() (`pr#15156 <https://github.com/ceph/ceph/pull/15156>`_, Kefu Chai)
* tools: ceph.in: drop the compatiiblity to handle non json commands (`pr#15508 <https://github.com/ceph/ceph/pull/15508>`_, Kefu Chai)
* tools: ceph.in: filter out audit from ceph -w (`pr#16345 <https://github.com/ceph/ceph/pull/16345>`_, John Spray)
* tools: ceph.in, mgr: misc cleanups (`pr#16229 <https://github.com/ceph/ceph/pull/16229>`_, liuchang0812)
* tools: ceph.in: print return code when json_command failed (`pr#15378 <https://github.com/ceph/ceph/pull/15378>`_, liuchang0812)
* tools: ceph-objectstore-tool: Handle object names that are also valid json (`pr#12848 <https://github.com/ceph/ceph/pull/12848>`_, David Zafman)
* tools: ceph-release-notes: escape asterisks not for inline emphasis (`pr#16199 <https://github.com/ceph/ceph/pull/16199>`_, Kefu Chai)
* tools: ceph-release-notes: escape _ for unintended links (`issue#17499 <http://tracker.ceph.com/issues/17499>`_, `pr#16528 <https://github.com/ceph/ceph/pull/16528>`_, Kefu Chai)
* tools: ceph-release-notes: handle an edge case (`pr#16277 <https://github.com/ceph/ceph/pull/16277>`_, Nathan Cutler)
* tools: ceph-release-notes: ignore low-numbered PRs (`issue#18695 <http://tracker.ceph.com/issues/18695>`_, `pr#13151 <https://github.com/ceph/ceph/pull/13151>`_, Nathan Cutler)
* tools: ceph-release-notes: port it to py3 (`pr#16261 <https://github.com/ceph/ceph/pull/16261>`_, Kefu Chai)
* tools: ceph-release-notes: prefixes and pep8 compliance (`pr#14156 <https://github.com/ceph/ceph/pull/14156>`_, Nathan Cutler)
* tools: ceph-release-notes: refactor and fix regressions (`pr#16411 <https://github.com/ceph/ceph/pull/16411>`_, Nathan Cutler)
* tools: ceph-release-notes: strip trailing punctuation (`pr#14385 <https://github.com/ceph/ceph/pull/14385>`_, Nathan Cutler)
* tools: ceph-rest-api: be more tolerant on network failure (`issue#20115 <http://tracker.ceph.com/issues/20115>`_, `pr#15706 <https://github.com/ceph/ceph/pull/15706>`_, Kefu Chai)
* tools: ceph-volume: adds functional CI testing #16919 (`pr#16970 <https://github.com/ceph/ceph/pull/16970>`_, Andrew Schoen, Alfredo Deza)
* tools: ceph-volume docs (`pr#17124 <https://github.com/ceph/ceph/pull/17124>`_, Alfredo Deza)
* tools: ceph-volume: initial take on ceph-volume CLI tool (`pr#16632 <https://github.com/ceph/ceph/pull/16632>`_, Dan Mick, Alfredo Deza)
* tools: ceph-volume: Use a delimited CLI output parser instead of JSON (`pr#17123 <https://github.com/ceph/ceph/pull/17123>`_, Alfredo Deza)
* tools: change compare_exchange_weak to compare_exchange_strong (`pr#15030 <https://github.com/ceph/ceph/pull/15030>`_, Jesse Williamson)
* tools: Cleanup dead code in ceph-objectstore-tool (`pr#15812 <https://github.com/ceph/ceph/pull/15812>`_, David Zafman)
* tools: fio_ceph_objectstore: Print db_statistics when rocksdb_perf is enabled (`pr#15796 <https://github.com/ceph/ceph/pull/15796>`_, Xiaoyan Li)
* tools: init-ceph: print trailing n in "status" output (`pr#13351 <https://github.com/ceph/ceph/pull/13351>`_, Kefu Chai)
* tools: init-ceph: should have a space before "]" (`pr#14796 <https://github.com/ceph/ceph/pull/14796>`_, Kefu Chai)
* tools: os/bluestore/bluestore_tool: add sanity check to get rid of occasionally crash (`pr#16013 <https://github.com/ceph/ceph/pull/16013>`_, xie xingguo)
* tools: rados: check for negative return value of rados_create_with_context() as its comment put (`pr#10893 <https://github.com/ceph/ceph/pull/10893>`_, zhang.zezhu)
* tools: rados: fix typo in 'df' column name (`pr#15603 <https://github.com/ceph/ceph/pull/15603>`_, Ilya Dryomov)
* tools: rados: out json 'df' values as numbers, not strings (`issue#15546 <http://tracker.ceph.com/issues/15546>`_, `pr#14644 <https://github.com/ceph/ceph/pull/14644>`_, Sage Weil)
* tools: script: add docker core dump debugger (`pr#16375 <https://github.com/ceph/ceph/pull/16375>`_, Patrick Donnelly)
* tools: script: ceph-release-notes check orig. issue only for backports (`pr#12979 <https://github.com/ceph/ceph/pull/12979>`_, Abhishek Lekshmanan)
* tools: script/sepia_bt.sh: no need to pass version and sha1 anymore (`pr#13380 <https://github.com/ceph/ceph/pull/13380>`_, Kefu Chai)
* tools: src/ceph-disk/ceph_disk/main.py: Make 'ceph-disk list' work on FreeBSD (`pr#14483 <https://github.com/ceph/ceph/pull/14483>`_, Willem Jan Withagen)
* tools: stop.sh: boilerplate error (don't stop mon when stopping mgr) (`pr#14461 <https://github.com/ceph/ceph/pull/14461>`_, Dan Mick)
* tools: support hammer in rbd_recover_tool (`pr#12413 <https://github.com/ceph/ceph/pull/12413>`_, Bartłomiej Święcki)
* tools: tools/ceph_kvstore_tool: add "bluestore-kv" to usage (`pr#15326 <https://github.com/ceph/ceph/pull/15326>`_, xie xingguo)
* tools: tools/crushtool: replicated-rule API support (`pr#15011 <https://github.com/ceph/ceph/pull/15011>`_, xie xingguo)
* tools: tools/rados: add a parameter "--offset" to rados put command (`pr#12674 <https://github.com/ceph/ceph/pull/12674>`_, liuchang0812)
* tools: tools/rados: Check return value of connect (`issue#19319 <http://tracker.ceph.com/issues/19319>`_, `pr#14057 <https://github.com/ceph/ceph/pull/14057>`_, Brad Hubbard)
* tools: tools/rados: fixed typo in help information (`pr#15618 <https://github.com/ceph/ceph/pull/15618>`_, Pan Liu)
* tools: tools/rados: remove useless function declaration (`pr#12566 <https://github.com/ceph/ceph/pull/12566>`_, liuchang0812)
* tools: tools/rados: some cleanups (`pr#16147 <https://github.com/ceph/ceph/pull/16147>`_, Yan Jun)
* tools: vstart: "debug_ms=1" for mgr by default (`pr#15127 <https://github.com/ceph/ceph/pull/15127>`_, Kefu Chai)
* tools: vstart: don't configure rgw_dns_name (`pr#13411 <https://github.com/ceph/ceph/pull/13411>`_, Yehuda Sadeh)
* tools: vstart: don't create cluster by default (`pr#13891 <https://github.com/ceph/ceph/pull/13891>`_, Yehuda Sadeh)
* tools: vstart: print "start osd.$id" instead of "start osd$id" (`pr#15427 <https://github.com/ceph/ceph/pull/15427>`_, Kefu Chai)
* tools: vstart.sh: bind restful, dashboard to ::, not 127.0.0.1 (`pr#16349 <https://github.com/ceph/ceph/pull/16349>`_, Sage Weil)
* tools: vstart.sh: do not add host for mgr.\* section if not $overwrite_conf (`pr#13767 <https://github.com/ceph/ceph/pull/13767>`_, Kefu Chai)
* tools: warning, ‘%.16x’ directive output truncated writing 16 bytes into a region of size 9 (`pr#14292 <https://github.com/ceph/ceph/pull/14292>`_, Jos Collin)
* tracing: don't include oid when tracing at dequeue_op() (`pr#13410 <https://github.com/ceph/ceph/pull/13410>`_, Yehuda Sadeh)
