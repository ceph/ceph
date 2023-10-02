======
Hammer
======

Hammer is the 8th stable release of Ceph.  It is named after the
hammer octopus (Octopus australis).

v0.94.10 Hammer
===============

This Hammer point release fixes several bugs and adds two new features.

We recommend that all hammer v0.94.x users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.10.txt>`.

New Features
------------

ceph-objectstore-tool and ceph-monstore-tool now enable user to
rebuild the monitor database from OSDs. (This feature is especially useful when
all monitors fail to boot due to leveldb corruption.)

In RADOS Gateway, it is now possible to reshard an existing bucket's index
using an off-line tool.

Usage:

$ radosgw-admin bucket reshard --bucket=<bucket_name> --num_shards=<num_shards>

This will create a new linked bucket instance that points to the newly created
index objects. The old bucket instance still exists and currently it's up to
the user to manually remove the old bucket index objects. (Note that bucket
resharding currently requires that all IO (especially writes) to the specific
bucket is quiesced.)

Other Notable Changes
---------------------

* build/ops: ceph-create-keys loops forever (`issue#17753 <http://tracker.ceph.com/issues/17753>`_, `pr#12805 <http://github.com/ceph/ceph/pull/12805>`_, Alfredo Deza)
* build/ops: improve ceph.in error message (`issue#11101 <http://tracker.ceph.com/issues/11101>`_, `pr#10905 <http://github.com/ceph/ceph/pull/10905>`_, Kefu Chai)
* build/ops: make stop.sh more portable (`issue#16918 <http://tracker.ceph.com/issues/16918>`_, `pr#10569 <http://github.com/ceph/ceph/pull/10569>`_, Mykola Golub)
* build/ops: remove SYSTEMD_RUN from initscript (`issue#16440 <http://tracker.ceph.com/issues/16440>`_, `issue#7627 <http://tracker.ceph.com/issues/7627>`_, `pr#9873 <http://github.com/ceph/ceph/pull/9873>`_, Vladislav Odintsov)
* cephx: Fix multiple segfaults due to attempts to encrypt or decrypt (`issue#16266 <http://tracker.ceph.com/issues/16266>`_, `pr#11930 <http://github.com/ceph/ceph/pull/11930>`_, Brad Hubbard)
* common: SIGABRT in TrackedOp::dump() via dump_ops_in_flight() (`issue#8885 <http://tracker.ceph.com/issues/8885>`_, `pr#12121 <http://github.com/ceph/ceph/pull/12121>`_, Jianpeng Ma, Zhiqiang Wang, David Zafman)
* common: os/ObjectStore: fix _update_op for split dest_cid (`issue#15345 <http://tracker.ceph.com/issues/15345>`_, `pr#12071 <http://github.com/ceph/ceph/pull/12071>`_, Sage Weil)
* crush: reset bucket->h.items[i] when removing tree item (`issue#16525 <http://tracker.ceph.com/issues/16525>`_, `pr#10724 <http://github.com/ceph/ceph/pull/10724>`_, Kefu Chai)
* doc: add "Upgrading to Hammer" section (`issue#17386 <http://tracker.ceph.com/issues/17386>`_, `pr#11372 <http://github.com/ceph/ceph/pull/11372>`_, Kefu Chai)
* doc: add orphan options to radosgw-admin --help and man page (`issue#17281 <http://tracker.ceph.com/issues/17281>`_, `issue#17280 <http://tracker.ceph.com/issues/17280>`_, `pr#11140 <http://github.com/ceph/ceph/pull/11140>`_, Abhishek Lekshmanan, Casey Bodley, Ken Dreyer, Thomas Serlin)
* doc: clarify that RGW bucket object versioning is supported (`issue#16574 <http://tracker.ceph.com/issues/16574>`_, `pr#10437 <http://github.com/ceph/ceph/pull/10437>`_, Yuan Zhou, shawn chen)
* librados: bad flags can crash the osd (`issue#16012 <http://tracker.ceph.com/issues/16012>`_, `pr#11936 <http://github.com/ceph/ceph/pull/11936>`_, Jianpeng Ma, Sage Weil)
* librbd: ceph 10.2.2 rbd status on image format 2 returns "(2) No such file or directory" (`issue#16887 <http://tracker.ceph.com/issues/16887>`_, `pr#10987 <http://github.com/ceph/ceph/pull/10987>`_, Jason Dillaman)
* librbd: diffs to clone's first snapshot should include parent diffs (`issue#18068 <http://tracker.ceph.com/issues/18068>`_, `pr#12446 <http://github.com/ceph/ceph/pull/12446>`_, Jason Dillaman)
* librbd: image.stat() call in librbdpy fails sometimes (`issue#17310 <http://tracker.ceph.com/issues/17310>`_, `pr#11949 <http://github.com/ceph/ceph/pull/11949>`_, Jason Dillaman)
* librbd: request exclusive lock if current owner cannot execute op (`issue#16171 <http://tracker.ceph.com/issues/16171>`_, `pr#12018 <http://github.com/ceph/ceph/pull/12018>`_, Mykola Golub)
* mds: fix cephfs-java ftruncate unit test failure (`issue#11258 <http://tracker.ceph.com/issues/11258>`_, `pr#11939 <http://github.com/ceph/ceph/pull/11939>`_, Yan, Zheng)
* mon: %USED of ceph df is wrong (`issue#16933 <http://tracker.ceph.com/issues/16933>`_, `pr#11934 <http://github.com/ceph/ceph/pull/11934>`_, Kefu Chai)
* mon: MonmapMonitor should return success when MON will be removed (`issue#17725 <http://tracker.ceph.com/issues/17725>`_, `pr#12006 <http://github.com/ceph/ceph/pull/12006>`_, Joao Eduardo Luis)
* mon: OSDMonitor: Missing nearfull flag set (`issue#17390 <http://tracker.ceph.com/issues/17390>`_, `pr#11273 <http://github.com/ceph/ceph/pull/11273>`_, Igor Podoski)
* mon: OSDs marked OUT wrongly after monitor failover (`issue#17719 <http://tracker.ceph.com/issues/17719>`_, `pr#11946 <http://github.com/ceph/ceph/pull/11946>`_, Dong Wu)
* mon: fix memory leak in prepare_beacon (`issue#17285 <http://tracker.ceph.com/issues/17285>`_, `pr#10238 <http://github.com/ceph/ceph/pull/10238>`_, Igor Podoski)
* mon: osd flag health message is misleading (`issue#18175 <http://tracker.ceph.com/issues/18175>`_, `pr#12687 <http://github.com/ceph/ceph/pull/12687>`_, Sage Weil)
* mon: prepare_pgtemp needs to only update up_thru if newer than the existing one (`issue#16185 <http://tracker.ceph.com/issues/16185>`_, `pr#11937 <http://github.com/ceph/ceph/pull/11937>`_, Samuel Just)
* mon: return size_t from MonitorDBStore::Transaction::size() (`issue#14217 <http://tracker.ceph.com/issues/14217>`_, `pr#10904 <http://github.com/ceph/ceph/pull/10904>`_, Kefu Chai)
* mon: send updated monmap to its subscribers (`issue#17558 <http://tracker.ceph.com/issues/17558>`_, `pr#11457 <http://github.com/ceph/ceph/pull/11457>`_, Kefu Chai)
* msgr: OpTracker needs to release the message throttle in _unregistered (`issue#14248 <http://tracker.ceph.com/issues/14248>`_, `pr#11938 <http://github.com/ceph/ceph/pull/11938>`_, Samuel Just)
* msgr: simple/Pipe: error decoding addr (`issue#18072 <http://tracker.ceph.com/issues/18072>`_, `pr#12266 <http://github.com/ceph/ceph/pull/12266>`_, Sage Weil)
* osd: PG::_update_calc_stats wrong for CRUSH_ITEM_NONE up set items (`issue#16998 <http://tracker.ceph.com/issues/16998>`_, `pr#11933 <http://github.com/ceph/ceph/pull/11933>`_, Samuel Just)
* osd: PG::choose_acting valgrind error or ./common/hobject.h: 182: FAILED assert(!max || (\*this == hobject_t(hobject_t::get_max()))) (`issue#13967 <http://tracker.ceph.com/issues/13967>`_, `pr#11932 <http://github.com/ceph/ceph/pull/11932>`_, Tao Chang)
* osd: ReplicatedBackend::build_push_op: add a second config to limit omap entries/chunk independently of object data (`issue#16128 <http://tracker.ceph.com/issues/16128>`_, `pr#12417 <http://github.com/ceph/ceph/pull/12417>`_, Wanlong Gao)
* osd: crash on EIO during deep-scrubbing (`issue#16034 <http://tracker.ceph.com/issues/16034>`_, `pr#11935 <http://github.com/ceph/ceph/pull/11935>`_, Nathan Cutler)
* osd: filestore: FALLOC_FL_PUNCH_HOLE must be used with FALLOC_FL_KEEP_SIZE (`issue#18446 <http://tracker.ceph.com/issues/18446>`_, `pr#13041 <http://github.com/ceph/ceph/pull/13041>`_, xinxin shu)
* osd: fix cached_removed_snaps bug in PGPool::update after map gap (`issue#18628 <http://tracker.ceph.com/issues/18628>`_, `issue#15943 <http://tracker.ceph.com/issues/15943>`_, `pr#12906 <http://github.com/ceph/ceph/pull/12906>`_, Samuel Just)
* osd: fix collection_list shadow return value (`issue#17713 <http://tracker.ceph.com/issues/17713>`_, `pr#11927 <http://github.com/ceph/ceph/pull/11927>`_, Haomai Wang)
* osd: fix fiemap issue in xfs when #extents > 1364 (`issue#17610 <http://tracker.ceph.com/issues/17610>`_, `pr#11615 <http://github.com/ceph/ceph/pull/11615>`_, Kefu Chai, Ning Yao)
* osd: update PGPool to detect map gaps and reset cached_removed_snaps (`issue#15943 <http://tracker.ceph.com/issues/15943>`_, `pr#11676 <http://github.com/ceph/ceph/pull/11676>`_, Samuel Just)
* rbd: export diff should open image as read-only (`issue#17671 <http://tracker.ceph.com/issues/17671>`_, `pr#11948 <http://github.com/ceph/ceph/pull/11948>`_, liyankun)
* rbd: fix parameter check (`issue#18237 <http://tracker.ceph.com/issues/18237>`_, `pr#12312 <http://github.com/ceph/ceph/pull/12312>`_, Yankun Li)
* rbd: fix possible rbd data corruption (`issue#16002 <http://tracker.ceph.com/issues/16002>`_, `pr#11618 <http://github.com/ceph/ceph/pull/11618>`_, Yan, Zheng, Greg Farnum)
* rgw: Anonymous user is able to read bucket with authenticated read ACL (`issue#13207 <http://tracker.ceph.com/issues/13207>`_, `pr#11045 <http://github.com/ceph/ceph/pull/11045>`_, rahul.1aggarwal@gmail.com)
* rgw: COPY broke multipart files uploaded under dumpling (`issue#16435 <http://tracker.ceph.com/issues/16435>`_, `pr#11950 <http://github.com/ceph/ceph/pull/11950>`_, Yehuda Sadeh)
* rgw: TempURL in radosgw behaves now like its Swift's counterpart.  (`issue#18316 <http://tracker.ceph.com/issues/18316>`_, `pr#12619 <http://github.com/ceph/ceph/pull/12619>`_, Radoslaw Zarzynski)
* rgw: default quota fixes (`issue#16410 <http://tracker.ceph.com/issues/16410>`_, `pr#10839 <http://github.com/ceph/ceph/pull/10839>`_, Pavan Rallabhandi, Daniel Gryniewicz)
* rgw: do not abort when accept a CORS request with short origin (`issue#18187 <http://tracker.ceph.com/issues/18187>`_, `pr#12398 <http://github.com/ceph/ceph/pull/12398>`_, LiuYang)
* rgw: do not omap_getvals with (u64)-1 max (`issue#17985 <http://tracker.ceph.com/issues/17985>`_, `pr#12418 <http://github.com/ceph/ceph/pull/12418>`_, Yehuda Sadeh, Sage Weil)
* rgw: fix crash when client posts object with null condition (`issue#17635 <http://tracker.ceph.com/issues/17635>`_, `pr#11809 <http://github.com/ceph/ceph/pull/11809>`_, Yehuda Sadeh)
* rgw: fix inconsistent uid/email handling in radosgw-admin (`issue#13598 <http://tracker.ceph.com/issues/13598>`_, `pr#11952 <http://github.com/ceph/ceph/pull/11952>`_, Matt Benjamin)
* rgw: implement offline resharding command (`issue#17745 <http://tracker.ceph.com/issues/17745>`_, `pr#12227 <http://github.com/ceph/ceph/pull/12227>`_, Yehuda Sadeh, Orit Wasserman, weiqiaomiao)
* rgw: swift: ranged request on a DLO provides wrong values in Content-Range HTTP header (`issue#13452 <http://tracker.ceph.com/issues/13452>`_, `pr#11951 <http://github.com/ceph/ceph/pull/11951>`_, Radoslaw Zarzynski)
* rgw: the value of total_time is wrong in the result of 'radosgw-admin log show' opt (`issue#17598 <http://tracker.ceph.com/issues/17598>`_, `pr#11899 <http://github.com/ceph/ceph/pull/11899>`_, weiqiaomiao)
* tests: Cannot clone ceph/s3-tests.git (missing branch) (`issue#18384 <http://tracker.ceph.com/issues/18384>`_, `pr#12744 <http://github.com/ceph/ceph/pull/12744>`_, Orit Wasserman)
* tests: Cannot reserve CentOS 7.2 smithi machines (`issue#18401 <http://tracker.ceph.com/issues/18401>`_, `pr#12762 <http://github.com/ceph/ceph/pull/12762>`_, Nathan Cutler)
* tests: OSDs commit suicide in rbd suite when testing on btrfs (`issue#18397 <http://tracker.ceph.com/issues/18397>`_, `pr#12758 <http://github.com/ceph/ceph/pull/12758>`_, Nathan Cutler)
* tests: Workunits needlessly wget from git.ceph.com (`issue#18336 <http://tracker.ceph.com/issues/18336>`_, `issue#18271 <http://tracker.ceph.com/issues/18271>`_, `issue#18388 <http://tracker.ceph.com/issues/18388>`_, `pr#12685 <http://github.com/ceph/ceph/pull/12685>`_, Sage Weil, Nathan Cutler)
* tests: cephfs test failures (ceph.com/qa is broken, should be download.ceph.com/qa) (`issue#18574 <http://tracker.ceph.com/issues/18574>`_, `pr#13022 <http://github.com/ceph/ceph/pull/13022>`_, John Spray)
* tests: merge ceph-qa-suite (`pr#12455 <http://github.com/ceph/ceph/pull/12455>`_, Sage Weil)
* tests: objecter_requests workunit fails on wip branches (`issue#18393 <http://tracker.ceph.com/issues/18393>`_, `pr#12759 <http://github.com/ceph/ceph/pull/12759>`_, Sage Weil)
* tests: populate mnt_point in qa/tasks/ceph.py (`issue#18383 <http://tracker.ceph.com/issues/18383>`_, `pr#12743 <http://github.com/ceph/ceph/pull/12743>`_, Nathan Cutler)
* tests: qemu/tests/qemu-iotests/077 fails in dumpling, hammer, and jewel (`issue#10773 <http://tracker.ceph.com/issues/10773>`_, `pr#12423 <http://github.com/ceph/ceph/pull/12423>`_, Jason Dillaman)
* tests: run fs/thrash on xfs instead of btrfs (`issue#17151 <http://tracker.ceph.com/issues/17151>`_, `pr#13039 <http://github.com/ceph/ceph/pull/13039>`_, Nathan Cutler)
* tests: update Ubuntu image url after ceph.com refactor (`issue#18542 <http://tracker.ceph.com/issues/18542>`_, `pr#12957 <http://github.com/ceph/ceph/pull/12957>`_, Jason Dillaman)
* tests: update rbd/singleton/all/formatted-output.yaml to support ceph-ci * (`issue#18440 <http://tracker.ceph.com/issues/18440>`_, `pr#12824 * <http://github.com/ceph/ceph/pull/12824>`_, Venky Shankar, Nathan Cutler)
* tools: add a tool to rebuild mon store from OSD (`issue#17179 <http://tracker.ceph.com/issues/17179>`_, `issue#17400 <http://tracker.ceph.com/issues/17400>`_, `pr#11125 <http://github.com/ceph/ceph/pull/11125>`_, Kefu Chai, xie xingguo)
* tools: ceph-objectstore-tool crashes if --journal-path <a-directory> (`issue#17307 <http://tracker.ceph.com/issues/17307>`_, `pr#11929 <http://github.com/ceph/ceph/pull/11929>`_, Kefu Chai)
* tools: ceph-objectstore-tool: add a way to split filestore directories offline (`issue#17220 <http://tracker.ceph.com/issues/17220>`_, `pr#11253 <http://github.com/ceph/ceph/pull/11253>`_, Josh Durgin)
* tools: crushtool --compile generates output despite missing item (`issue#17306 <http://tracker.ceph.com/issues/17306>`_, `pr#11931 <http://github.com/ceph/ceph/pull/11931>`_, Kefu Chai)


v0.94.9 Hammer
==============

This Hammer point release fixes a build issue present in 0.94.8 that prevented us
from generating packages for Ubuntu Precise and CentOS 6.x.

We recommend all users of v0.94.7 or older upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.9.txt>`.

Notable Changes
---------------

* build/ops: revert: boost uuid makes valgrind complain (`pr#10913 <http://github.com/ceph/ceph/pull/10913>`_, Sage Weil)


v0.94.8 Hammer
==============

This Hammer point release fixes several bugs.

We recommend that all hammer v0.94.x users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.8.txt>`.

Notable Changes
---------------

* build/ops: rocksdb do not link against tcmalloc if it's disabled (`issue#14799 <http://tracker.ceph.com/issues/14799>`_, `pr#10750 <http://github.com/ceph/ceph/pull/10750>`_, Sage Weil, Kefu Chai)
* build/ops: Add -D_LARGEFILE64_SOURCE to Linux build. (`issue#16611 <http://tracker.ceph.com/issues/16611>`_, `pr#10182 <http://github.com/ceph/ceph/pull/10182>`_, Ira Cooper)
* build/ops: boost uuid makes valgrind complain (`issue#12736 <http://tracker.ceph.com/issues/12736>`_, `pr#9741 <http://github.com/ceph/ceph/pull/9741>`_, Sage Weil, Rohan Mars)
* build/ops: ceph-disk s/by-parttype-uuid/by-parttypeuuid/ (`issue#15867 <http://tracker.ceph.com/issues/15867>`_, `pr#9107 <http://github.com/ceph/ceph/pull/9107>`_, Nathan Cutler)
* common: add units to rados bench output and clean up formatting (`issue#12248 <http://tracker.ceph.com/issues/12248>`_, `pr#8960 <http://github.com/ceph/ceph/pull/8960>`_, Dmitry Yatsushkevich, Brad Hubbard, Gu Zhongyan)
* common: config set with negative value results in "error setting 'filestore_merge_threshold' to '-40': (22) Invalid argument" (`issue#13829 <http://tracker.ceph.com/issues/13829>`_, `pr#10291 <http://github.com/ceph/ceph/pull/10291>`_, Brad Hubbard, Kefu Chai)
* common: linking to -lrbd causes process startup times to balloon (`issue#15225 <http://tracker.ceph.com/issues/15225>`_, `pr#8538 <http://github.com/ceph/ceph/pull/8538>`_, Richard W.M. Jones)
* doc: fix by-parttypeuuid in ceph-disk(8) nroff (`issue#15867 <http://tracker.ceph.com/issues/15867>`_, `pr#10699 <http://github.com/ceph/ceph/pull/10699>`_, Ken Dreyer)
* fs: double decreased the count to trim caps which will cause failing to respond to cache pressure (`issue#14319 <http://tracker.ceph.com/issues/14319>`_, `pr#8804 <http://github.com/ceph/ceph/pull/8804>`_, Zhi Zhang)
* log: do not repeat errors to stderr (`issue#14616 <http://tracker.ceph.com/issues/14616>`_, `pr#10227 <http://github.com/ceph/ceph/pull/10227>`_, Sage Weil)
* mds: failing file operations on kernel based cephfs mount point leaves unaccessible file behind on hammer 0.94.7 (`issue#16013 <http://tracker.ceph.com/issues/16013>`_, `pr#10198 <http://github.com/ceph/ceph/pull/10198>`_, Yan, Zheng)
* mds: fix stray purging in 'stripe_count > 1' case (`issue#15050 <http://tracker.ceph.com/issues/15050>`_, `pr#8042 <http://github.com/ceph/ceph/pull/8042>`_, Yan, Zheng)
* mds: wrongly treat symlink inode as normal file/dir when symlink inode is stale on kcephfs (`issue#15702 <http://tracker.ceph.com/issues/15702>`_, `pr#9404 <http://github.com/ceph/ceph/pull/9404>`_, Zhi Zhang)
* mon: LibRadosMiscConnectFailure.ConnectFailure (not so intermittent) failure in upgrade/hammer-x  (`issue#13992 <http://tracker.ceph.com/issues/13992>`_, `pr#8806 <http://github.com/ceph/ceph/pull/8806>`_, Sage Weil)
* mon: Monitor: validate prefix on handle_command() (`issue#16297 <http://tracker.ceph.com/issues/16297>`_, `pr#10038 <http://github.com/ceph/ceph/pull/10038>`_, You Ji)
* mon: drop pg temps from not the current primary in OSDMonitor (`issue#16127 <http://tracker.ceph.com/issues/16127>`_, `pr#9893 <http://github.com/ceph/ceph/pull/9893>`_, Samuel Just)
* mon: fix calculation of %USED (`issue#15641 <http://tracker.ceph.com/issues/15641>`_, `pr#9125 <http://github.com/ceph/ceph/pull/9125>`_, Ruifeng Yang, David Zafman)
* mon: improve reweight_by_utilization() logic (`issue#15686 <http://tracker.ceph.com/issues/15686>`_, `pr#9416 <http://github.com/ceph/ceph/pull/9416>`_, xie xingguo)
* mon: pool quota alarm is not in effect  (`issue#15478 <http://tracker.ceph.com/issues/15478>`_, `pr#8593 <http://github.com/ceph/ceph/pull/8593>`_, Danny Al-Gaaf)
* mon: wrong ceph get mdsmap assertion (`issue#14681 <http://tracker.ceph.com/issues/14681>`_, `pr#7542 <http://github.com/ceph/ceph/pull/7542>`_, Vicente Cheng)
* msgr: ceph-osd valgrind invalid reads/writes (`issue#15870 <http://tracker.ceph.com/issues/15870>`_, `pr#9238 <http://github.com/ceph/ceph/pull/9238>`_, Samuel Just)
* objecter: LibRadosWatchNotifyPPTests/LibRadosWatchNotifyPP.WatchNotify2Timeout/1 segv (`issue#15760 <http://tracker.ceph.com/issues/15760>`_, `pr#9400 <http://github.com/ceph/ceph/pull/9400>`_, Sage Weil)
* osd: OSD reporting ENOTEMPTY and crashing (`issue#14766 <http://tracker.ceph.com/issues/14766>`_, `pr#9277 <http://github.com/ceph/ceph/pull/9277>`_, Samuel Just)
* osd: When generating past intervals due to an import end at pg epoch and fix build_past_intervals_parallel (`issue#12387 <http://tracker.ceph.com/issues/12387>`_, `issue#14438 <http://tracker.ceph.com/issues/14438>`_, `pr#8464 <http://github.com/ceph/ceph/pull/8464>`_, David Zafman)
* osd: acting_primary not updated on split (`issue#15523 <http://tracker.ceph.com/issues/15523>`_, `pr#9001 <http://github.com/ceph/ceph/pull/9001>`_, Sage Weil)
* osd: assert(!actingbackfill.empty()): old watch timeout tries to queue repop on replica (`issue#15391 <http://tracker.ceph.com/issues/15391>`_, `pr#8665 <http://github.com/ceph/ceph/pull/8665>`_, Sage Weil)
* osd: assert(rollback_info_trimmed_to == head) in PGLog (`issue#13965 <http://tracker.ceph.com/issues/13965>`_, `pr#8849 <http://github.com/ceph/ceph/pull/8849>`_, Samuel Just)
* osd: delete one of the repeated op->mark_started in ReplicatedBackend::sub_op_modify_impl (`issue#16572 <http://tracker.ceph.com/issues/16572>`_, `pr#9977 <http://github.com/ceph/ceph/pull/9977>`_, shun-s)
* osd: fix omap digest compare when scrub (`issue#16000 <http://tracker.ceph.com/issues/16000>`_, `pr#9271 <http://github.com/ceph/ceph/pull/9271>`_, Xinze Chi)
* osd: is_split crash in handle_pg_create (`issue#15426 <http://tracker.ceph.com/issues/15426>`_, `pr#8805 <http://github.com/ceph/ceph/pull/8805>`_, Kefu Chai)
* osd: objects unfound after repair (fixed by repeering the pg) (`issue#15006 <http://tracker.ceph.com/issues/15006>`_, `pr#7961 <http://github.com/ceph/ceph/pull/7961>`_, Jianpeng Ma, Loic Dachary, Kefu Chai)
* osd: rados cppool omap to ec pool crashes osd (`issue#14695 <http://tracker.ceph.com/issues/14695>`_, `pr#8845 <http://github.com/ceph/ceph/pull/8845>`_, Jianpeng Ma)
* osd: remove all stale osdmaps in handle_osd_map() (`issue#13990 <http://tracker.ceph.com/issues/13990>`_, `pr#9090 <http://github.com/ceph/ceph/pull/9090>`_, Kefu Chai)
* osd: send write and read sub ops on behalf of client ops at normal priority in ECBackend (`issue#14313 <http://tracker.ceph.com/issues/14313>`_, `pr#8573 <http://github.com/ceph/ceph/pull/8573>`_, Samuel Just)
* rbd: snap rollback: restore the link to parent (`issue#14512 <http://tracker.ceph.com/issues/14512>`_, `pr#8535 <http://github.com/ceph/ceph/pull/8535>`_, Alexey Sheplyakov)
* rgw: S3: set EncodingType in ListBucketResult (`issue#15896 <http://tracker.ceph.com/issues/15896>`_, `pr#8987 <http://github.com/ceph/ceph/pull/8987>`_, Victor Makarov, Robin H. Johnson)
* rgw: backport rgwx-copy-if-newer for radosgw-agent (`issue#16262 <http://tracker.ceph.com/issues/16262>`_, `pr#9671 <http://github.com/ceph/ceph/pull/9671>`_, Yehuda Sadeh)
* rgw: bucket listing following object delete is partial (`issue#14826 <http://tracker.ceph.com/issues/14826>`_, `pr#10555 <http://github.com/ceph/ceph/pull/10555>`_, Orit Wasserman)
* rgw: convert plain object to versioned (with null version) when removing (`issue#15243 <http://tracker.ceph.com/issues/15243>`_, `pr#8755 <http://github.com/ceph/ceph/pull/8755>`_, Yehuda Sadeh)
* rgw: fix multi-delete query param parsing. (`issue#16618 <http://tracker.ceph.com/issues/16618>`_, `pr#10189 <http://github.com/ceph/ceph/pull/10189>`_, Robin H. Johnson)
* rgw: have a flavor of bucket deletion to bypass GC and to trigger (`issue#15557 <http://tracker.ceph.com/issues/15557>`_, `pr#10509 <http://github.com/ceph/ceph/pull/10509>`_, Pavan Rallabhandi)
* rgw: keep track of written_objs correctly (`issue#15886 <http://tracker.ceph.com/issues/15886>`_, `pr#9240 <http://github.com/ceph/ceph/pull/9240>`_, Yehuda Sadeh)
* rgw: multipart ListPartsResult has missing quotes on ETag (`issue#15334 <http://tracker.ceph.com/issues/15334>`_, `pr#8475 <http://github.com/ceph/ceph/pull/8475>`_, xie xingguo, Robin H. Johnson)
* rgw: no Last-Modified, Content-Size and X-Object-Manifest headers if no segments in DLO manifest (`issue#15812 <http://tracker.ceph.com/issues/15812>`_, `pr#9402 <http://github.com/ceph/ceph/pull/9402>`_, Radoslaw Zarzynski)
* rgw: radosgw server abort when user passed bad parameters to set quota (`issue#14190 <http://tracker.ceph.com/issues/14190>`_, `issue#14191 <http://tracker.ceph.com/issues/14191>`_, `pr#8313 <http://github.com/ceph/ceph/pull/8313>`_, Dunrong Huang)
* rgw: radosgw-admin region-map set is not reporting the bucket quota correctly (`issue#16815 <http://tracker.ceph.com/issues/16815>`_, `pr#10554 <http://github.com/ceph/ceph/pull/10554>`_, Yehuda Sadeh, Orit Wasserman)
* rgw: refrain from sending Content-Type/Content-Length for 304 responses (`issue#16327 <http://tracker.ceph.com/issues/16327>`_, `issue#13582 <http://tracker.ceph.com/issues/13582>`_, `issue#15119 <http://tracker.ceph.com/issues/15119>`_, `issue#14005 <http://tracker.ceph.com/issues/14005>`_, `pr#8379 <http://github.com/ceph/ceph/pull/8379>`_, Yehuda Sadeh, Nathan Cutler, Wido den Hollander)
* rgw: remove bucket index objects when deleting the bucket (`issue#16412 <http://tracker.ceph.com/issues/16412>`_, `pr#10530 <http://github.com/ceph/ceph/pull/10530>`_, Orit Wasserman)
* rgw: set Access-Control-Allow-Origin to an asterisk if allowed in a rule (`issue#15348 <http://tracker.ceph.com/issues/15348>`_, `pr#8528 <http://github.com/ceph/ceph/pull/8528>`_, Wido den Hollander)
* rgw: subset of uploaded objects via radosgw are unretrievable when using EC pool (`issue#15745 <http://tracker.ceph.com/issues/15745>`_, `pr#9407 <http://github.com/ceph/ceph/pull/9407>`_, Yehuda Sadeh)
* rgw: subuser rm fails with status 125 (`issue#14375 <http://tracker.ceph.com/issues/14375>`_, `pr#9961 <http://github.com/ceph/ceph/pull/9961>`_, Orit Wasserman)
* rgw: the swift key remains after removing a subuser (`issue#12890 <http://tracker.ceph.com/issues/12890>`_, `issue#14375 <http://tracker.ceph.com/issues/14375>`_, `pr#10718 <http://github.com/ceph/ceph/pull/10718>`_, Orit Wasserman, Sangdi Xu)
* rgw: user quota may not adjust on bucket removal (`issue#14507 <http://tracker.ceph.com/issues/14507>`_, `pr#8113 <http://github.com/ceph/ceph/pull/8113>`_, Edward Yang)
* tests: be more generous with test timeout (`issue#15403 <http://tracker.ceph.com/issues/15403>`_, `pr#8470 <http://github.com/ceph/ceph/pull/8470>`_, Loic Dachary)
* tests: qa/workunits/rbd: respect RBD_CREATE_ARGS environment variable (`issue#16289 <http://tracker.ceph.com/issues/16289>`_, `pr#9722 <http://github.com/ceph/ceph/pull/9722>`_, Mykola Golub)

v0.94.7 Hammer
==============

This Hammer point release fixes several minor bugs.  It also includes
a backport of an improved 'ceph osd reweight-by-utilization' command
for handling OSDs with higher-than-average utilizations.

We recommend that all hammer v0.94.x users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.7.txt>`.

Notable Changes
---------------

* auth: keyring permisions for mon deamon (`issue#14950 <http://tracker.ceph.com/issues/14950>`_, `pr#8049 <http://github.com/ceph/ceph/pull/8049>`_, Owen Synge)
* auth: PK11_DestroyContext() is called twice if PK11_DigestFinal() fails (`issue#14958 <http://tracker.ceph.com/issues/14958>`_, `pr#7922 <http://github.com/ceph/ceph/pull/7922>`_, Brad Hubbard, Dunrong Huang)
* auth: use libnss more safely (`issue#14620 <http://tracker.ceph.com/issues/14620>`_, `pr#7488 <http://github.com/ceph/ceph/pull/7488>`_, Sage Weil)
* ceph-disk: use blkid instead of sgdisk -i (`issue#14080 <http://tracker.ceph.com/issues/14080>`_, `issue#14094 <http://tracker.ceph.com/issues/14094>`_, `pr#7475 <http://github.com/ceph/ceph/pull/7475>`_, Ilya Dryomov, Loic Dachary)
* ceph-fuse: fix ceph-fuse writing to stale log file after log rotation (`issue#12350 <http://tracker.ceph.com/issues/12350>`_, `pr#7110 <http://github.com/ceph/ceph/pull/7110>`_, Zhi Zhang)
* ceph init script unconditionally sources /lib/lsb/init-functions (`issue#14402 <http://tracker.ceph.com/issues/14402>`_, `pr#7797 <http://github.com/ceph/ceph/pull/7797>`_, Yan, Zheng)
* ceph.in: Notify user that 'tell' can't be used in interactive mode (`issue#14773 <http://tracker.ceph.com/issues/14773>`_, `pr#7656 <http://github.com/ceph/ceph/pull/7656>`_, David Zafman)
* ceph-objectstore-tool, osd: Fix import handling (`issue#10794 <http://tracker.ceph.com/issues/10794>`_, `issue#13382 <http://tracker.ceph.com/issues/13382>`_, `pr#7917 <http://github.com/ceph/ceph/pull/7917>`_, Sage Weil, David Zafman)
* client: added permission check based on getgrouplist (`issue#13268 <http://tracker.ceph.com/issues/13268>`_, `pr#6604 <http://github.com/ceph/ceph/pull/6604>`_, Yan, Zheng, Danny Al-Gaaf)
* client: inoderef (`issue#13729 <http://tracker.ceph.com/issues/13729>`_, `pr#6551 <http://github.com/ceph/ceph/pull/6551>`_, Yan, Zheng)
* common: clock skew report is incorrect by ceph health detail command (`issue#14175 <http://tracker.ceph.com/issues/14175>`_, `pr#8051 <http://github.com/ceph/ceph/pull/8051>`_, Joao Eduardo Luis)
* global/pidfile: do not start two daemons with a single pid-file (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7671 <http://github.com/ceph/ceph/pull/7671>`_, Loic Dachary, shun song)
* librados: segfault in Objecter::handle_watch_notify (`issue#13805 <http://tracker.ceph.com/issues/13805>`_, `pr#7992 <http://github.com/ceph/ceph/pull/7992>`_, Sage Weil)
* librbd: flattening an rbd image with active IO can lead to hang (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `issue#14483 <http://tracker.ceph.com/issues/14483>`_, `pr#7485 <http://github.com/ceph/ceph/pull/7485>`_, Jason Dillaman)
* librbd: possible QEMU deadlock after creating image snapshots (`issue#14988 <http://tracker.ceph.com/issues/14988>`_, `pr#8011 <http://github.com/ceph/ceph/pull/8011>`_, Jason Dillaman)
* mon: Bucket owner isn't changed after unlink/link (`issue#11076 <http://tracker.ceph.com/issues/11076>`_, `pr#8583 <http://github.com/ceph/ceph/pull/8583>`_, Zengran Zhang)
* monclient: avoid key renew storm on clock skew (`issue#12065 <http://tracker.ceph.com/issues/12065>`_, `pr#8398 <http://github.com/ceph/ceph/pull/8398>`_, Alexey Sheplyakov)
* mon: implement reweight-by-utilization feature (`issue#15054 <http://tracker.ceph.com/issues/15054>`_, `pr#8026 <http://github.com/ceph/ceph/pull/8026>`_, Kefu Chai, Dan van der Ster, Sage Weil)
* mon/LogMonitor: use the configured facility if log to syslog (`issue#13748 <http://tracker.ceph.com/issues/13748>`_, `pr#7648 <http://github.com/ceph/ceph/pull/7648>`_, Kefu Chai)
* mon: mon sync does not copy config-key (`issue#14577 <http://tracker.ceph.com/issues/14577>`_, `pr#7576 <http://github.com/ceph/ceph/pull/7576>`_, Xiaowei Chen)
* mon/OSDMonitor: avoid underflow in reweight-by-utilization if max_change=1 (`issue#15655 <http://tracker.ceph.com/issues/15655>`_, `pr#8979 <http://github.com/ceph/ceph/pull/8979>`_, Samuel Just)
* osd: consume_maps clearing of waiting_for_pg needs to check the spg_t shard for acting set membership (`issue#14278 <http://tracker.ceph.com/issues/14278>`_, `pr#7577 <http://github.com/ceph/ceph/pull/7577>`_, Samuel Just)
* osd: log inconsistent shard sizes (`issue#14009 <http://tracker.ceph.com/issues/14009>`_, `pr#6946 <http://github.com/ceph/ceph/pull/6946>`_, Loic Dachary)
* osd: OSD coredumps with leveldb compact on mount = true (`issue#14748 <http://tracker.ceph.com/issues/14748>`_, `pr#7645 <http://github.com/ceph/ceph/pull/7645>`_, Xiaoxi Chen)
* osd/OSDMap: reset osd_primary_affinity shared_ptr when deepish_copy_from (`issue#14686 <http://tracker.ceph.com/issues/14686>`_, `pr#7590 <http://github.com/ceph/ceph/pull/7590>`_, Xinze Chi)
* osd: Protect against excessively large object map sizes (`issue#15121 <http://tracker.ceph.com/issues/15121>`_, `pr#8401 <http://github.com/ceph/ceph/pull/8401>`_, Jason Dillaman)
* osd/ReplicatedPG: do not proxy read *and* process op locally (`issue#15171 <http://tracker.ceph.com/issues/15171>`_, `pr#8187 <http://github.com/ceph/ceph/pull/8187>`_, Sage Weil)
* osd: scrub bogus results when missing a clone (`issue#14875 <http://tracker.ceph.com/issues/14875>`_, `issue#14874 <http://tracker.ceph.com/issues/14874>`_, `issue#14877 <http://tracker.ceph.com/issues/14877>`_, `issue#10098 <http://tracker.ceph.com/issues/10098>`_, `issue#14878 <http://tracker.ceph.com/issues/14878>`_, `issue#14881 <http://tracker.ceph.com/issues/14881>`_, `issue#14882 <http://tracker.ceph.com/issues/14882>`_, `issue#14883 <http://tracker.ceph.com/issues/14883>`_, `issue#14879 <http://tracker.ceph.com/issues/14879>`_, `issue#10290 <http://tracker.ceph.com/issues/10290>`_, `issue#12740 <http://tracker.ceph.com/issues/12740>`_, `issue#12738 <http://tracker.ceph.com/issues/12738>`_, `issue#14880 <http://tracker.ceph.com/issues/14880>`_, `issue#11135 <http://tracker.ceph.com/issues/11135>`_, `issue#14876 <http://tracker.ceph.com/issues/14876>`_, `issue#10809 <http://tracker.ceph.com/issues/10809>`_, `issue#12193 <http://tracker.ceph.com/issues/12193>`_, `issue#11237 <http://tracker.ceph.com/issues/11237>`_, `pr#7702 <http://github.com/ceph/ceph/pull/7702>`_, Xinze Chi, Sage Weil, John Spray, Kefu Chai, Mykola Golub, David Zafman)
* osd: Unable to bring up OSD's after dealing with FULL cluster (OSD assert with /include/interval_set.h: 386: FAILED assert(_size >= 0)) (`issue#14428 <http://tracker.ceph.com/issues/14428>`_, `pr#7415 <http://github.com/ceph/ceph/pull/7415>`_, Alexey Sheplyakov)
* osd: use GMT time for the object name of hitsets (`issue#13192 <http://tracker.ceph.com/issues/13192>`_, `issue#9732 <http://tracker.ceph.com/issues/9732>`_, `issue#12968 <http://tracker.ceph.com/issues/12968>`_, `pr#7883 <http://github.com/ceph/ceph/pull/7883>`_, Kefu Chai, David Zafman)
* qa/workunits/post-file.sh: sudo (`issue#14586 <http://tracker.ceph.com/issues/14586>`_, `pr#7456 <http://github.com/ceph/ceph/pull/7456>`_, Sage Weil)
* qa/workunits: remove 'mds setmap' from workunits (`pr#8123 <http://github.com/ceph/ceph/pull/8123>`_, Sage Weil)
* rgw: default quota params (`issue#12997 <http://tracker.ceph.com/issues/12997>`_, `pr#7188 <http://github.com/ceph/ceph/pull/7188>`_, Daniel Gryniewicz)
* rgw: make rgw_fronends more forgiving of whitespace (`issue#12038 <http://tracker.ceph.com/issues/12038>`_, `pr#7414 <http://github.com/ceph/ceph/pull/7414>`_, Matt Benjamin)
* rgw: radosgw-admin bucket check --fix not work (`issue#14215 <http://tracker.ceph.com/issues/14215>`_, `pr#7185 <http://github.com/ceph/ceph/pull/7185>`_, Weijun Duan)
* rpm package building fails if the build machine has lttng and babeltrace development packages installed locally (`issue#14844 <http://tracker.ceph.com/issues/14844>`_, `pr#8440 <http://github.com/ceph/ceph/pull/8440>`_, Kefu Chai)
* rpm: redhat-lsb-core dependency was dropped, but is still needed (`issue#14906 <http://tracker.ceph.com/issues/14906>`_, `pr#7876 <http://github.com/ceph/ceph/pull/7876>`_, Nathan Cutler)
* test_bit_vector.cc uses magic numbers against #defines that vary (`issue#14747 <http://tracker.ceph.com/issues/14747>`_, `pr#7672 <http://github.com/ceph/ceph/pull/7672>`_, Jason Dillaman)
* test/librados/tier.cc doesn't completely clean up EC pools (`issue#13878 <http://tracker.ceph.com/issues/13878>`_, `pr#8052 <http://github.com/ceph/ceph/pull/8052>`_, Loic Dachary, Dan Mick)
* tests: bufferlist: do not expect !is_page_aligned() after unaligned rebuild (`issue#15305 <http://tracker.ceph.com/issues/15305>`_, `pr#8272 <http://github.com/ceph/ceph/pull/8272>`_, Kefu Chai)
* tools: fix race condition in seq/rand bench (part 1) (`issue#14968 <http://tracker.ceph.com/issues/14968>`_, `issue#14873 <http://tracker.ceph.com/issues/14873>`_, `pr#7896 <http://github.com/ceph/ceph/pull/7896>`_, Alexey Sheplyakov, Piotr Dałek)
* tools: fix race condition in seq/rand bench (part 2) (`issue#14873 <http://tracker.ceph.com/issues/14873>`_, `pr#7817 <http://github.com/ceph/ceph/pull/7817>`_, Alexey Sheplyakov)
* tools/rados: add bench smoke tests (`issue#14971 <http://tracker.ceph.com/issues/14971>`_, `pr#7903 <http://github.com/ceph/ceph/pull/7903>`_, Piotr Dałek)
* tools, test: Add ceph-objectstore-tool to operate on the meta collection (`issue#14977 <http://tracker.ceph.com/issues/14977>`_, `pr#7911 <http://github.com/ceph/ceph/pull/7911>`_, David Zafman)
* unittest_crypto: benchmark 100,000 CryptoKey::encrypt() calls (`issue#14863 <http://tracker.ceph.com/issues/14863>`_, `pr#7801 <http://github.com/ceph/ceph/pull/7801>`_, Sage Weil)


v0.94.6 Hammer
======================

This Hammer point release fixes a range of bugs, most notably a fix
for unbounded growth of the monitor's leveldb store, and a workaround
in the OSD to keep most xattrs small enough to be stored inline in XFS
inodes.

We recommend that all hammer v0.94.x users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.6.txt>`.

Notable Changes
---------------
* build/ops: Ceph daemon failed to start, because the service name was already used. (`issue#13474 <http://tracker.ceph.com/issues/13474>`_, `pr#6832 <http://github.com/ceph/ceph/pull/6832>`_, Chuanhong Wang)
* build/ops: LTTng-UST tracing should be dynamically enabled (`issue#13274 <http://tracker.ceph.com/issues/13274>`_, `pr#6415 <http://github.com/ceph/ceph/pull/6415>`_, Jason Dillaman)
* build/ops: ceph upstart script rbdmap.conf incorrectly processes parameters (`issue#13214 <http://tracker.ceph.com/issues/13214>`_, `pr#6159 <http://github.com/ceph/ceph/pull/6159>`_, Sage Weil)
* build/ops: ceph.spec.in License line does not reflect COPYING (`issue#12935 <http://tracker.ceph.com/issues/12935>`_, `pr#6680 <http://github.com/ceph/ceph/pull/6680>`_, Nathan Cutler)
* build/ops: ceph.spec.in libcephfs_jni1 has no %post and %postun  (`issue#12927 <http://tracker.ceph.com/issues/12927>`_, `pr#5789 <http://github.com/ceph/ceph/pull/5789>`_, Owen Synge)
* build/ops: configure.ac: no use to add "+" before ac_ext=c (`issue#14330 <http://tracker.ceph.com/issues/14330>`_, `pr#6973 <http://github.com/ceph/ceph/pull/6973>`_, Kefu Chai, Robin H. Johnson)
* build/ops: deb: strip tracepoint libraries from Wheezy/Precise builds (`issue#14801 <http://tracker.ceph.com/issues/14801>`_, `pr#7316 <http://github.com/ceph/ceph/pull/7316>`_, Jason Dillaman)
* build/ops: init script reload doesn't work on EL7 (`issue#13709 <http://tracker.ceph.com/issues/13709>`_, `pr#7187 <http://github.com/ceph/ceph/pull/7187>`_, Hervé Rousseau)
* build/ops: init-rbdmap uses distro-specific functions (`issue#12415 <http://tracker.ceph.com/issues/12415>`_, `pr#6528 <http://github.com/ceph/ceph/pull/6528>`_, Boris Ranto)
* build/ops: logrotate reload error on Ubuntu 14.04 (`issue#11330 <http://tracker.ceph.com/issues/11330>`_, `pr#5787 <http://github.com/ceph/ceph/pull/5787>`_, Sage Weil)
* build/ops: miscellaneous spec file fixes (`issue#12931 <http://tracker.ceph.com/issues/12931>`_, `issue#12994 <http://tracker.ceph.com/issues/12994>`_, `issue#12924 <http://tracker.ceph.com/issues/12924>`_, `issue#12360 <http://tracker.ceph.com/issues/12360>`_, `pr#5790 <http://github.com/ceph/ceph/pull/5790>`_, Boris Ranto, Nathan Cutler, Owen Synge, Travis Rhoden, Ken Dreyer)
* build/ops: pass tcmalloc env through to ceph-os (`issue#14802 <http://tracker.ceph.com/issues/14802>`_, `pr#7365 <http://github.com/ceph/ceph/pull/7365>`_, Sage Weil)
* build/ops: rbd-replay-* moved from ceph-test-dbg to ceph-common-dbg as well (`issue#13785 <http://tracker.ceph.com/issues/13785>`_, `pr#6580 <http://github.com/ceph/ceph/pull/6580>`_, Loic Dachary)
* build/ops: unknown argument --quiet in udevadm settle (`issue#13560 <http://tracker.ceph.com/issues/13560>`_, `pr#6530 <http://github.com/ceph/ceph/pull/6530>`_, Jason Dillaman)
* common: Objecter: pool op callback may hang forever. (`issue#13642 <http://tracker.ceph.com/issues/13642>`_, `pr#6588 <http://github.com/ceph/ceph/pull/6588>`_, xie xingguo)
* common: Objecter: potential null pointer access when do pool_snap_list. (`issue#13639 <http://tracker.ceph.com/issues/13639>`_, `pr#6839 <http://github.com/ceph/ceph/pull/6839>`_, xie xingguo)
* common: ThreadPool add/remove work queue methods not thread safe (`issue#12662 <http://tracker.ceph.com/issues/12662>`_, `pr#5889 <http://github.com/ceph/ceph/pull/5889>`_, Jason Dillaman)
* common: auth/cephx: large amounts of log are produced by osd (`issue#13610 <http://tracker.ceph.com/issues/13610>`_, `pr#6835 <http://github.com/ceph/ceph/pull/6835>`_, Qiankun Zheng)
* common: client nonce collision due to unshared pid namespaces (`issue#13032 <http://tracker.ceph.com/issues/13032>`_, `pr#6151 <http://github.com/ceph/ceph/pull/6151>`_, Josh Durgin)
* common: common/Thread:pthread_attr_destroy(thread_attr) when done with it (`issue#12570 <http://tracker.ceph.com/issues/12570>`_, `pr#6157 <http://github.com/ceph/ceph/pull/6157>`_, Piotr Dałek)
* common: log: Log.cc: Assign LOG_DEBUG priority to syslog calls (`issue#13993 <http://tracker.ceph.com/issues/13993>`_, `pr#6994 <http://github.com/ceph/ceph/pull/6994>`_, Brad Hubbard)
* common: objecter: cancellation bugs (`issue#13071 <http://tracker.ceph.com/issues/13071>`_, `pr#6155 <http://github.com/ceph/ceph/pull/6155>`_, Jianpeng Ma)
* common: pure virtual method called (`issue#13636 <http://tracker.ceph.com/issues/13636>`_, `pr#6587 <http://github.com/ceph/ceph/pull/6587>`_, Jason Dillaman)
* common: small probability sigabrt when setting rados_osd_op_timeout (`issue#13208 <http://tracker.ceph.com/issues/13208>`_, `pr#6143 <http://github.com/ceph/ceph/pull/6143>`_, Ruifeng Yang)
* common: wrong conditional for boolean function KeyServer::get_auth() (`issue#9756 <http://tracker.ceph.com/issues/9756>`_, `issue#13424 <http://tracker.ceph.com/issues/13424>`_, `pr#6213 <http://github.com/ceph/ceph/pull/6213>`_, Nathan Cutler)
* crush: crash if we see CRUSH_ITEM_NONE in early rule step (`issue#13477 <http://tracker.ceph.com/issues/13477>`_, `pr#6430 <http://github.com/ceph/ceph/pull/6430>`_, Sage Weil)
* doc: man: document listwatchers cmd in "rados" manpage (`issue#14556 <http://tracker.ceph.com/issues/14556>`_, `pr#7434 <http://github.com/ceph/ceph/pull/7434>`_, Kefu Chai)
* doc: regenerate man pages, add orphans commands to radosgw-admin(8) (`issue#14637 <http://tracker.ceph.com/issues/14637>`_, `pr#7524 <http://github.com/ceph/ceph/pull/7524>`_, Ken Dreyer)
* fs: CephFS restriction on removing cache tiers is overly strict (`issue#11504 <http://tracker.ceph.com/issues/11504>`_, `pr#6402 <http://github.com/ceph/ceph/pull/6402>`_, John Spray)
* fs: fsstress.sh fails (`issue#12710 <http://tracker.ceph.com/issues/12710>`_, `pr#7454 <http://github.com/ceph/ceph/pull/7454>`_, Yan, Zheng)
* librados: LibRadosWatchNotify.WatchNotify2Timeout (`issue#13114 <http://tracker.ceph.com/issues/13114>`_, `pr#6336 <http://github.com/ceph/ceph/pull/6336>`_, Sage Weil)
* librbd: ImageWatcher shouldn't block the notification thread (`issue#14373 <http://tracker.ceph.com/issues/14373>`_, `pr#7407 <http://github.com/ceph/ceph/pull/7407>`_, Jason Dillaman)
* librbd: diff_iterate needs to handle holes in parent images (`issue#12885 <http://tracker.ceph.com/issues/12885>`_, `pr#6097 <http://github.com/ceph/ceph/pull/6097>`_, Jason Dillaman)
* librbd: fix merge-diff for >2GB diff-files (`issue#14030 <http://tracker.ceph.com/issues/14030>`_, `pr#6980 <http://github.com/ceph/ceph/pull/6980>`_, Jason Dillaman)
* librbd: invalidate object map on error even w/o holding lock (`issue#13372 <http://tracker.ceph.com/issues/13372>`_, `pr#6289 <http://github.com/ceph/ceph/pull/6289>`_, Jason Dillaman)
* librbd: reads larger than cache size hang (`issue#13164 <http://tracker.ceph.com/issues/13164>`_, `pr#6354 <http://github.com/ceph/ceph/pull/6354>`_, Lu Shi)
* mds: ceph mds add_data_pool check for EC pool is wrong (`issue#12426 <http://tracker.ceph.com/issues/12426>`_, `pr#5766 <http://github.com/ceph/ceph/pull/5766>`_, John Spray)
* mon: MonitorDBStore: get_next_key() only if prefix matches (`issue#11786 <http://tracker.ceph.com/issues/11786>`_, `pr#5361 <http://github.com/ceph/ceph/pull/5361>`_, Joao Eduardo Luis)
* mon: OSDMonitor: do not assume a session exists in send_incremental() (`issue#14236 <http://tracker.ceph.com/issues/14236>`_, `pr#7150 <http://github.com/ceph/ceph/pull/7150>`_, Joao Eduardo Luis)
* mon: check for store writeablility before participating in election (`issue#13089 <http://tracker.ceph.com/issues/13089>`_, `pr#6144 <http://github.com/ceph/ceph/pull/6144>`_, Sage Weil)
* mon: compact full epochs also (`issue#14537 <http://tracker.ceph.com/issues/14537>`_, `pr#7446 <http://github.com/ceph/ceph/pull/7446>`_, Kefu Chai)
* mon: include min_last_epoch_clean as part of PGMap::print_summary and PGMap::dump (`issue#13198 <http://tracker.ceph.com/issues/13198>`_, `pr#6152 <http://github.com/ceph/ceph/pull/6152>`_, Guang Yang)
* mon: map_cache can become inaccurate if osd does not receive the osdmaps (`issue#10930 <http://tracker.ceph.com/issues/10930>`_, `pr#5773 <http://github.com/ceph/ceph/pull/5773>`_, Kefu Chai)
* mon: should not set isvalid = true when cephx_verify_authorizer return false (`issue#13525 <http://tracker.ceph.com/issues/13525>`_, `pr#6391 <http://github.com/ceph/ceph/pull/6391>`_, Ruifeng Yang)
* osd: Ceph Pools' MAX AVAIL is 0 if some OSDs' weight is 0 (`issue#13840 <http://tracker.ceph.com/issues/13840>`_, `pr#6834 <http://github.com/ceph/ceph/pull/6834>`_, Chengyuan Li)
* osd: FileStore calls syncfs(2) even it is not supported (`issue#12512 <http://tracker.ceph.com/issues/12512>`_, `pr#5530 <http://github.com/ceph/ceph/pull/5530>`_, Kefu Chai)
* osd: FileStore: potential memory leak if getattrs fails. (`issue#13597 <http://tracker.ceph.com/issues/13597>`_, `pr#6420 <http://github.com/ceph/ceph/pull/6420>`_, xie xingguo)
* osd: IO error on kvm/rbd with an erasure coded pool tier (`issue#12012 <http://tracker.ceph.com/issues/12012>`_, `pr#5897 <http://github.com/ceph/ceph/pull/5897>`_, Kefu Chai)
* osd: OSD::build_past_intervals_parallel() shall reset primary and up_primary when begin a new past_interval. (`issue#13471 <http://tracker.ceph.com/issues/13471>`_, `pr#6398 <http://github.com/ceph/ceph/pull/6398>`_, xiexingguo)
* osd: ReplicatedBackend: populate recovery_info.size for clone (bug symptom is size mismatch on replicated backend on a clone in scrub) (`issue#12828 <http://tracker.ceph.com/issues/12828>`_, `pr#6153 <http://github.com/ceph/ceph/pull/6153>`_, Samuel Just)
* osd: ReplicatedPG: wrong result code checking logic during sparse_read (`issue#14151 <http://tracker.ceph.com/issues/14151>`_, `pr#7179 <http://github.com/ceph/ceph/pull/7179>`_, xie xingguo)
* osd: ReplicatedPG::hit_set_trim osd/ReplicatedPG.cc: 11006: FAILED assert(obc) (`issue#13192 <http://tracker.ceph.com/issues/13192>`_, `issue#9732 <http://tracker.ceph.com/issues/9732>`_, `issue#12968 <http://tracker.ceph.com/issues/12968>`_, `pr#5825 <http://github.com/ceph/ceph/pull/5825>`_, Kefu Chai, Zhiqiang Wang, Samuel Just, David Zafman)
* osd: avoid multi set osd_op.outdata in tier pool (`issue#12540 <http://tracker.ceph.com/issues/12540>`_, `pr#6060 <http://github.com/ceph/ceph/pull/6060>`_, Xinze Chi)
* osd: bug with cache/tiering and snapshot reads (`issue#12748 <http://tracker.ceph.com/issues/12748>`_, `pr#6589 <http://github.com/ceph/ceph/pull/6589>`_, Kefu Chai)
* osd: ceph osd pool stats broken in hammer (`issue#13843 <http://tracker.ceph.com/issues/13843>`_, `pr#7180 <http://github.com/ceph/ceph/pull/7180>`_, BJ Lougee)
* osd: ceph-disk prepare fails if device is a symlink (`issue#13438 <http://tracker.ceph.com/issues/13438>`_, `pr#7176 <http://github.com/ceph/ceph/pull/7176>`_, Joe Julian)
* osd: check for full before changing the cached obc (hammer) (`issue#13098 <http://tracker.ceph.com/issues/13098>`_, `pr#6918 <http://github.com/ceph/ceph/pull/6918>`_, Alexey Sheplyakov)
* osd: config_opts: increase suicide timeout to 300 to match recovery (`issue#14376 <http://tracker.ceph.com/issues/14376>`_, `pr#7236 <http://github.com/ceph/ceph/pull/7236>`_, Samuel Just)
* osd: disable filestore_xfs_extsize by default (`issue#14397 <http://tracker.ceph.com/issues/14397>`_, `pr#7411 <http://github.com/ceph/ceph/pull/7411>`_, Ken Dreyer)
* osd: do not cache unused memory in attrs (`issue#12565 <http://tracker.ceph.com/issues/12565>`_, `pr#6499 <http://github.com/ceph/ceph/pull/6499>`_, Xinze Chi, Ning Yao)
* osd: dumpling incrementals do not work properly on hammer and newer (`issue#13234 <http://tracker.ceph.com/issues/13234>`_, `pr#6132 <http://github.com/ceph/ceph/pull/6132>`_, Samuel Just)
* osd: filestore: fix peek_queue for OpSequencer (`issue#13209 <http://tracker.ceph.com/issues/13209>`_, `pr#6145 <http://github.com/ceph/ceph/pull/6145>`_, Xinze Chi)
* osd: hit set clear repops fired in same epoch as map change -- segfault since they fall into the new interval even though the repops are cleared (`issue#12809 <http://tracker.ceph.com/issues/12809>`_, `pr#5890 <http://github.com/ceph/ceph/pull/5890>`_, Samuel Just)
* osd: object_info_t::decode() has wrong version (`issue#13462 <http://tracker.ceph.com/issues/13462>`_, `pr#6335 <http://github.com/ceph/ceph/pull/6335>`_, David Zafman)
* osd: osd/OSD.cc: 2469: FAILED assert(pg_stat_queue.empty()) on shutdown (`issue#14212 <http://tracker.ceph.com/issues/14212>`_, `pr#7178 <http://github.com/ceph/ceph/pull/7178>`_, Sage Weil)
* osd: osd/PG.cc: 288: FAILED assert(info.last_epoch_started >= info.history.last_epoch_started) (`issue#14015 <http://tracker.ceph.com/issues/14015>`_, `pr#7177 <http://github.com/ceph/ceph/pull/7177>`_, David Zafman)
* osd: osd/PG.cc: 3837: FAILED assert(0 == "Running incompatible OSD") (`issue#11661 <http://tracker.ceph.com/issues/11661>`_, `pr#7206 <http://github.com/ceph/ceph/pull/7206>`_, David Zafman)
* osd: osd/ReplicatedPG: Recency fix (`issue#14320 <http://tracker.ceph.com/issues/14320>`_, `pr#7207 <http://github.com/ceph/ceph/pull/7207>`_, Sage Weil, Robert LeBlanc)
* osd: pg stuck in replay (`issue#13116 <http://tracker.ceph.com/issues/13116>`_, `pr#6401 <http://github.com/ceph/ceph/pull/6401>`_, Sage Weil)
* osd: race condition detected during send_failures (`issue#13821 <http://tracker.ceph.com/issues/13821>`_, `pr#6755 <http://github.com/ceph/ceph/pull/6755>`_, Sage Weil)
* osd: randomize scrub times (`issue#10973 <http://tracker.ceph.com/issues/10973>`_, `pr#6199 <http://github.com/ceph/ceph/pull/6199>`_, Kefu Chai)
* osd: requeue_scrub when kick_object_context_blocked (`issue#12515 <http://tracker.ceph.com/issues/12515>`_, `pr#5891 <http://github.com/ceph/ceph/pull/5891>`_, Xinze Chi)
* osd: revert: use GMT time for hitsets (`issue#13812 <http://tracker.ceph.com/issues/13812>`_, `pr#6644 <http://github.com/ceph/ceph/pull/6644>`_, Loic Dachary)
* osd: segfault in agent_work (`issue#13199 <http://tracker.ceph.com/issues/13199>`_, `pr#6146 <http://github.com/ceph/ceph/pull/6146>`_, Samuel Just)
* osd: should recalc the min_last_epoch_clean when decode PGMap (`issue#13112 <http://tracker.ceph.com/issues/13112>`_, `pr#6154 <http://github.com/ceph/ceph/pull/6154>`_, Kefu Chai)
* osd: smaller object_info_t xattrs (`issue#14803 <http://tracker.ceph.com/issues/14803>`_, `pr#6544 <http://github.com/ceph/ceph/pull/6544>`_, Sage Weil)
* osd: we do not ignore notify from down osds (`issue#12990 <http://tracker.ceph.com/issues/12990>`_, `pr#6158 <http://github.com/ceph/ceph/pull/6158>`_, Samuel Just)
* rbd: QEMU hangs after creating snapshot and stopping VM (`issue#13726 <http://tracker.ceph.com/issues/13726>`_, `pr#6586 <http://github.com/ceph/ceph/pull/6586>`_, Jason Dillaman)
* rbd: TaskFinisher::cancel should remove event from SafeTimer (`issue#14476 <http://tracker.ceph.com/issues/14476>`_, `pr#7417 <http://github.com/ceph/ceph/pull/7417>`_, Douglas Fuller)
* rbd: avoid re-writing old-format image header on resize (`issue#13674 <http://tracker.ceph.com/issues/13674>`_, `pr#6585 <http://github.com/ceph/ceph/pull/6585>`_, Jason Dillaman)
* rbd: fix bench-write (`issue#14225 <http://tracker.ceph.com/issues/14225>`_, `pr#7183 <http://github.com/ceph/ceph/pull/7183>`_, Sage Weil)
* rbd: rbd-replay does not check for EOF and goes to endless loop (`issue#14452 <http://tracker.ceph.com/issues/14452>`_, `pr#7416 <http://github.com/ceph/ceph/pull/7416>`_, Mykola Golub)
* rbd: rbd-replay-prep and rbd-replay improvements (`issue#13221 <http://tracker.ceph.com/issues/13221>`_, `issue#13220 <http://tracker.ceph.com/issues/13220>`_, `issue#13378 <http://tracker.ceph.com/issues/13378>`_, `pr#6286 <http://github.com/ceph/ceph/pull/6286>`_, Jason Dillaman)
* rbd: verify self-managed snapshot functionality on image create (`issue#13633 <http://tracker.ceph.com/issues/13633>`_, `pr#7182 <http://github.com/ceph/ceph/pull/7182>`_, Jason Dillaman)
* rgw: Make RGW_MAX_PUT_SIZE configurable (`issue#6999 <http://tracker.ceph.com/issues/6999>`_, `pr#7441 <http://github.com/ceph/ceph/pull/7441>`_, Vladislav Odintsov, Yuan Zhou)
* rgw: Setting ACL on Object removes ETag (`issue#12955 <http://tracker.ceph.com/issues/12955>`_, `pr#6620 <http://github.com/ceph/ceph/pull/6620>`_, Brian Felton)
* rgw: backport content-type casing (`issue#12939 <http://tracker.ceph.com/issues/12939>`_, `pr#5910 <http://github.com/ceph/ceph/pull/5910>`_, Robin H. Johnson)
* rgw: bucket listing hangs on versioned buckets (`issue#12913 <http://tracker.ceph.com/issues/12913>`_, `pr#6352 <http://github.com/ceph/ceph/pull/6352>`_, Yehuda Sadeh)
* rgw: fix wrong etag calculation during POST on S3 bucket. (`issue#11241 <http://tracker.ceph.com/issues/11241>`_, `pr#7442 <http://github.com/ceph/ceph/pull/7442>`_, Vladislav Odintsov, Radoslaw Zarzynski)
* rgw: get bucket location returns region name, not region api name (`issue#13458 <http://tracker.ceph.com/issues/13458>`_, `pr#6349 <http://github.com/ceph/ceph/pull/6349>`_, Yehuda Sadeh)
* rgw: missing handling of encoding-type=url when listing keys in bucket (`issue#12735 <http://tracker.ceph.com/issues/12735>`_, `pr#6527 <http://github.com/ceph/ceph/pull/6527>`_, Jeff Weber)
* rgw: orphan tool should be careful about removing head objects (`issue#12958 <http://tracker.ceph.com/issues/12958>`_, `pr#6351 <http://github.com/ceph/ceph/pull/6351>`_, Yehuda Sadeh)
* rgw: orphans finish segfaults (`issue#13824 <http://tracker.ceph.com/issues/13824>`_, `pr#7186 <http://github.com/ceph/ceph/pull/7186>`_, Igor Fedotov)
* rgw: rgw-admin: document orphans commands in usage (`issue#14516 <http://tracker.ceph.com/issues/14516>`_, `pr#7526 <http://github.com/ceph/ceph/pull/7526>`_, Yehuda Sadeh)
* rgw: swift API returns more than real object count and bytes used when retrieving account metadata (`issue#13140 <http://tracker.ceph.com/issues/13140>`_, `pr#6512 <http://github.com/ceph/ceph/pull/6512>`_, Sangdi Xu)
* rgw: swift use Civetweb ssl can not get right url (`issue#13628 <http://tracker.ceph.com/issues/13628>`_, `pr#6491 <http://github.com/ceph/ceph/pull/6491>`_, Weijun Duan)
* rgw: value of Swift API's X-Object-Manifest header is not url_decoded during segment look up (`issue#12728 <http://tracker.ceph.com/issues/12728>`_, `pr#6353 <http://github.com/ceph/ceph/pull/6353>`_, Radoslaw Zarzynski)
* tests: fixed broken Makefiles after integration of ttng into rados (`issue#13210 <http://tracker.ceph.com/issues/13210>`_, `pr#6322 <http://github.com/ceph/ceph/pull/6322>`_, Sebastien Ponce)
* tests: fsx failed to compile (`issue#14384 <http://tracker.ceph.com/issues/14384>`_, `pr#7501 <http://github.com/ceph/ceph/pull/7501>`_, Greg Farnum)
* tests: notification slave needs to wait for master (`issue#13810 <http://tracker.ceph.com/issues/13810>`_, `pr#7226 <http://github.com/ceph/ceph/pull/7226>`_, Jason Dillaman)
* tests: qa: remove legacy OS support from rbd/qemu-iotests (`issue#13483 <http://tracker.ceph.com/issues/13483>`_, `issue#14385 <http://tracker.ceph.com/issues/14385>`_, `pr#7252 <http://github.com/ceph/ceph/pull/7252>`_, Vasu Kulkarni, Jason Dillaman)
* tests: testprofile must be removed before it is re-created (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6450 <http://github.com/ceph/ceph/pull/6450>`_, Loic Dachary)
* tools: ceph-monstore-tool must do out_store.close() (`issue#10093 <http://tracker.ceph.com/issues/10093>`_, `pr#7347 <http://github.com/ceph/ceph/pull/7347>`_, huangjun)
* tools: heavy memory shuffling in rados bench (`issue#12946 <http://tracker.ceph.com/issues/12946>`_, `pr#5810 <http://github.com/ceph/ceph/pull/5810>`_, Piotr Dałek)
* tools: race condition in rados bench (`issue#12947 <http://tracker.ceph.com/issues/12947>`_, `pr#6791 <http://github.com/ceph/ceph/pull/6791>`_, Piotr Dałek)
* tools: tool for artificially inflate the leveldb of the mon store for testing purposes  (`issue#10093 <http://tracker.ceph.com/issues/10093>`_, `issue#11815 <http://tracker.ceph.com/issues/11815>`_, `issue#14217 <http://tracker.ceph.com/issues/14217>`_, `pr#7412 <http://github.com/ceph/ceph/pull/7412>`_, Cilang Zhao, Bo Cai, Kefu Chai, huangjun, Joao Eduardo Luis)

v0.94.5 Hammer
==============

This Hammer point release fixes a critical regression in librbd that can cause
QEMU/KVM to crash when caching is enabled on images that have been cloned.

All v0.94.4 Hammer users are strongly encouraged to upgrade.

Notable Changes
---------------
* librbd: potential assertion failure during cache read (`issue#13559 <http://tracker.ceph.com/issues/13559>`_, `pr#6348 <http://github.com/ceph/ceph/pull/6348>`_, Jason Dillaman)
* osd: osd/ReplicatedPG: remove stray debug line (`issue#13455 <http://tracker.ceph.com/issues/13455>`_, `pr#6362 <http://github.com/ceph/ceph/pull/6362>`_, Sage Weil)
* tests: qemu workunit refers to apt-mirror.front.sepia.ceph.com (`issue#13420 <http://tracker.ceph.com/issues/13420>`_, `pr#6330 <http://github.com/ceph/ceph/pull/6330>`_, Yuan Zhou)

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.5.txt>`.

v0.94.4 Hammer
==============

This Hammer point release fixes several important bugs in Hammer, as well as
fixing interoperability issues that are required before an upgrade to
Infernalis. That is, all users of earlier version of Hammer or any
version of Firefly will first need to upgrade to hammer v0.94.4 or
later before upgrading to Infernalis (or future releases).

All v0.94.x Hammer users are strongly encouraged to upgrade.

Notable Changes
---------------
* build/ops: ceph.spec.in: 50-rbd.rules conditional is wrong (`issue#12166 <http://tracker.ceph.com/issues/12166>`_, `pr#5207 <http://github.com/ceph/ceph/pull/5207>`_, Nathan Cutler)
* build/ops: ceph.spec.in: ceph-common needs python-argparse on older distros, but doesn't require it (`issue#12034 <http://tracker.ceph.com/issues/12034>`_, `pr#5216 <http://github.com/ceph/ceph/pull/5216>`_, Nathan Cutler)
* build/ops: ceph.spec.in: radosgw requires apache for SUSE only -- makes no sense (`issue#12358 <http://tracker.ceph.com/issues/12358>`_, `pr#5411 <http://github.com/ceph/ceph/pull/5411>`_, Nathan Cutler)
* build/ops: ceph.spec.in: rpm: cephfs_java not fully conditionalized (`issue#11991 <http://tracker.ceph.com/issues/11991>`_, `pr#5202 <http://github.com/ceph/ceph/pull/5202>`_, Nathan Cutler)
* build/ops: ceph.spec.in: rpm: not possible to turn off Java (`issue#11992 <http://tracker.ceph.com/issues/11992>`_, `pr#5203 <http://github.com/ceph/ceph/pull/5203>`_, Owen Synge)
* build/ops: ceph.spec.in: running fdupes unnecessarily (`issue#12301 <http://tracker.ceph.com/issues/12301>`_, `pr#5223 <http://github.com/ceph/ceph/pull/5223>`_, Nathan Cutler)
* build/ops: ceph.spec.in: snappy-devel for all supported distros (`issue#12361 <http://tracker.ceph.com/issues/12361>`_, `pr#5264 <http://github.com/ceph/ceph/pull/5264>`_, Nathan Cutler)
* build/ops: ceph.spec.in: SUSE/openSUSE builds need libbz2-devel (`issue#11629 <http://tracker.ceph.com/issues/11629>`_, `pr#5204 <http://github.com/ceph/ceph/pull/5204>`_, Nathan Cutler)
* build/ops: ceph.spec.in: useless %py_requires breaks SLE11-SP3 build (`issue#12351 <http://tracker.ceph.com/issues/12351>`_, `pr#5412 <http://github.com/ceph/ceph/pull/5412>`_, Nathan Cutler)
* build/ops: error in ext_mime_map_init() when /etc/mime.types is missing (`issue#11864 <http://tracker.ceph.com/issues/11864>`_, `pr#5385 <http://github.com/ceph/ceph/pull/5385>`_, Ken Dreyer)
* build/ops: upstart: limit respawn to 3 in 30 mins (instead of 5 in 30s) (`issue#11798 <http://tracker.ceph.com/issues/11798>`_, `pr#5930 <http://github.com/ceph/ceph/pull/5930>`_, Sage Weil)
* build/ops: With root as default user, unable to have multiple RGW instances running (`issue#10927 <http://tracker.ceph.com/issues/10927>`_, `pr#6161 <http://github.com/ceph/ceph/pull/6161>`_, Sage Weil)
* build/ops: With root as default user, unable to have multiple RGW instances running (`issue#11140 <http://tracker.ceph.com/issues/11140>`_, `pr#6161 <http://github.com/ceph/ceph/pull/6161>`_, Sage Weil)
* build/ops: With root as default user, unable to have multiple RGW instances running (`issue#11686 <http://tracker.ceph.com/issues/11686>`_, `pr#6161 <http://github.com/ceph/ceph/pull/6161>`_, Sage Weil)
* build/ops: With root as default user, unable to have multiple RGW instances running (`issue#12407 <http://tracker.ceph.com/issues/12407>`_, `pr#6161 <http://github.com/ceph/ceph/pull/6161>`_, Sage Weil)
* cli: ceph: cli throws exception on unrecognized errno (`issue#11354 <http://tracker.ceph.com/issues/11354>`_, `pr#5368 <http://github.com/ceph/ceph/pull/5368>`_, Kefu Chai)
* cli: ceph tell: broken error message / misleading hinting (`issue#11101 <http://tracker.ceph.com/issues/11101>`_, `pr#5371 <http://github.com/ceph/ceph/pull/5371>`_, Kefu Chai)
* common: arm: all programs that link to librados2 hang forever on startup (`issue#12505 <http://tracker.ceph.com/issues/12505>`_, `pr#5366 <http://github.com/ceph/ceph/pull/5366>`_, Boris Ranto)
* common: buffer: critical bufferlist::zero bug (`issue#12252 <http://tracker.ceph.com/issues/12252>`_, `pr#5365 <http://github.com/ceph/ceph/pull/5365>`_, Haomai Wang)
* common: ceph-object-corpus: add 0.94.2-207-g88e7ee7 hammer objects (`issue#13070 <http://tracker.ceph.com/issues/13070>`_, `pr#5551 <http://github.com/ceph/ceph/pull/5551>`_, Sage Weil)
* common: do not insert emtpy ptr when rebuild emtpy bufferlist (`issue#12775 <http://tracker.ceph.com/issues/12775>`_, `pr#5764 <http://github.com/ceph/ceph/pull/5764>`_, Xinze Chi)
* common: [  FAILED  ] TestLibRBD.BlockingAIO (`issue#12479 <http://tracker.ceph.com/issues/12479>`_, `pr#5768 <http://github.com/ceph/ceph/pull/5768>`_, Jason Dillaman)
* common: LibCephFS.GetPoolId failure (`issue#12598 <http://tracker.ceph.com/issues/12598>`_, `pr#5887 <http://github.com/ceph/ceph/pull/5887>`_, Yan, Zheng)
* common: Memory leak in Mutex.cc, pthread_mutexattr_init without pthread_mutexattr_destroy (`issue#11762 <http://tracker.ceph.com/issues/11762>`_, `pr#5378 <http://github.com/ceph/ceph/pull/5378>`_, Ketor Meng)
* common: object_map_update fails with -EINVAL return code (`issue#12611 <http://tracker.ceph.com/issues/12611>`_, `pr#5559 <http://github.com/ceph/ceph/pull/5559>`_, Jason Dillaman)
* common: Pipe: Drop connect_seq increase line (`issue#13093 <http://tracker.ceph.com/issues/13093>`_, `pr#5908 <http://github.com/ceph/ceph/pull/5908>`_, Haomai Wang)
* common: recursive lock of md_config_t (0) (`issue#12614 <http://tracker.ceph.com/issues/12614>`_, `pr#5759 <http://github.com/ceph/ceph/pull/5759>`_, Josh Durgin)
* crush: ceph osd crush reweight-subtree does not reweight parent node (`issue#11855 <http://tracker.ceph.com/issues/11855>`_, `pr#5374 <http://github.com/ceph/ceph/pull/5374>`_, Sage Weil)
* doc: update docs to point to download.ceph.com (`issue#13162 <http://tracker.ceph.com/issues/13162>`_, `pr#6156 <http://github.com/ceph/ceph/pull/6156>`_, Alfredo Deza)
* fs: ceph-fuse 0.94.2-1trusty segfaults / aborts (`issue#12297 <http://tracker.ceph.com/issues/12297>`_, `pr#5381 <http://github.com/ceph/ceph/pull/5381>`_, Greg Farnum)
* fs: segfault launching ceph-fuse with bad --name (`issue#12417 <http://tracker.ceph.com/issues/12417>`_, `pr#5382 <http://github.com/ceph/ceph/pull/5382>`_, John Spray)
* librados: Change radosgw pools default crush ruleset (`issue#11640 <http://tracker.ceph.com/issues/11640>`_, `pr#5754 <http://github.com/ceph/ceph/pull/5754>`_, Yuan Zhou)
* librbd: correct issues discovered via lockdep / helgrind (`issue#12345 <http://tracker.ceph.com/issues/12345>`_, `pr#5296 <http://github.com/ceph/ceph/pull/5296>`_, Jason Dillaman)
* librbd: Crash during TestInternal.MultipleResize (`issue#12664 <http://tracker.ceph.com/issues/12664>`_, `pr#5769 <http://github.com/ceph/ceph/pull/5769>`_, Jason Dillaman)
* librbd: deadlock during cooperative exclusive lock transition (`issue#11537 <http://tracker.ceph.com/issues/11537>`_, `pr#5319 <http://github.com/ceph/ceph/pull/5319>`_, Jason Dillaman)
* librbd: Possible crash while concurrently writing and shrinking an image (`issue#11743 <http://tracker.ceph.com/issues/11743>`_, `pr#5318 <http://github.com/ceph/ceph/pull/5318>`_, Jason Dillaman)
* mon: add a cache layer over MonitorDBStore (`issue#12638 <http://tracker.ceph.com/issues/12638>`_, `pr#5697 <http://github.com/ceph/ceph/pull/5697>`_, Kefu Chai)
* mon: fix crush testing for new pools (`issue#13400 <http://tracker.ceph.com/issues/13400>`_, `pr#6192 <http://github.com/ceph/ceph/pull/6192>`_, Sage Weil)
* mon: get pools health'info have error (`issue#12402 <http://tracker.ceph.com/issues/12402>`_, `pr#5369 <http://github.com/ceph/ceph/pull/5369>`_, renhwztetecs)
* mon: implicit erasure code crush ruleset is not validated (`issue#11814 <http://tracker.ceph.com/issues/11814>`_, `pr#5276 <http://github.com/ceph/ceph/pull/5276>`_, Loic Dachary)
* mon: PaxosService: call post_refresh() instead of post_paxos_update() (`issue#11470 <http://tracker.ceph.com/issues/11470>`_, `pr#5359 <http://github.com/ceph/ceph/pull/5359>`_, Joao Eduardo Luis)
* mon: pgmonitor: wrong at/near target max“ reporting (`issue#12401 <http://tracker.ceph.com/issues/12401>`_, `pr#5370 <http://github.com/ceph/ceph/pull/5370>`_, huangjun)
* mon: register_new_pgs() should check ruleno instead of its index (`issue#12210 <http://tracker.ceph.com/issues/12210>`_, `pr#5377 <http://github.com/ceph/ceph/pull/5377>`_, Xinze Chi)
* mon: Show osd as NONE in ceph osd map <pool> <object>  output (`issue#11820 <http://tracker.ceph.com/issues/11820>`_, `pr#5376 <http://github.com/ceph/ceph/pull/5376>`_, Shylesh Kumar)
* mon: the output is wrong when runing ceph osd reweight (`issue#12251 <http://tracker.ceph.com/issues/12251>`_, `pr#5372 <http://github.com/ceph/ceph/pull/5372>`_, Joao Eduardo Luis)
* osd: allow peek_map_epoch to return an error (`issue#13060 <http://tracker.ceph.com/issues/13060>`_, `pr#5892 <http://github.com/ceph/ceph/pull/5892>`_, Sage Weil)
* osd: cache agent is idle although one object is left in the cache (`issue#12673 <http://tracker.ceph.com/issues/12673>`_, `pr#5765 <http://github.com/ceph/ceph/pull/5765>`_, Loic Dachary)
* osd: copy-from doesn't preserve truncate_{seq,size} (`issue#12551 <http://tracker.ceph.com/issues/12551>`_, `pr#5885 <http://github.com/ceph/ceph/pull/5885>`_, Samuel Just)
* osd: crash creating/deleting pools (`issue#12429 <http://tracker.ceph.com/issues/12429>`_, `pr#5527 <http://github.com/ceph/ceph/pull/5527>`_, John Spray)
* osd: fix repair when recorded digest is wrong (`issue#12577 <http://tracker.ceph.com/issues/12577>`_, `pr#5468 <http://github.com/ceph/ceph/pull/5468>`_, Sage Weil)
* osd: include/ceph_features: define HAMMER_0_94_4 feature (`issue#13026 <http://tracker.ceph.com/issues/13026>`_, `pr#5687 <http://github.com/ceph/ceph/pull/5687>`_, Sage Weil)
* osd: is_new_interval() fixes (`issue#10399 <http://tracker.ceph.com/issues/10399>`_, `pr#5691 <http://github.com/ceph/ceph/pull/5691>`_, Jason Dillaman)
* osd: is_new_interval() fixes (`issue#11771 <http://tracker.ceph.com/issues/11771>`_, `pr#5691 <http://github.com/ceph/ceph/pull/5691>`_, Jason Dillaman)
* osd: long standing slow requests: connection->session->waiting_for_map->connection ref cycle (`issue#12338 <http://tracker.ceph.com/issues/12338>`_, `pr#5761 <http://github.com/ceph/ceph/pull/5761>`_, Samuel Just)
* osd: Mutex Assert from PipeConnection::try_get_pipe (`issue#12437 <http://tracker.ceph.com/issues/12437>`_, `pr#5758 <http://github.com/ceph/ceph/pull/5758>`_, David Zafman)
* osd: pg_interval_t::check_new_interval - for ec pool, should not rely on min_size to determine if the PG was active at the interval (`issue#12162 <http://tracker.ceph.com/issues/12162>`_, `pr#5373 <http://github.com/ceph/ceph/pull/5373>`_, Guang G Yang)
* osd: PGLog.cc: 732: FAILED assert(log.log.size() == log_keys_debug.size()) (`issue#12652 <http://tracker.ceph.com/issues/12652>`_, `pr#5763 <http://github.com/ceph/ceph/pull/5763>`_, Sage Weil)
* osd: PGLog::proc_replica_log: correctly handle case where entries between olog.head and log.tail were split out (`issue#11358 <http://tracker.ceph.com/issues/11358>`_, `pr#5380 <http://github.com/ceph/ceph/pull/5380>`_, Samuel Just)
* osd: read on chunk-aligned xattr not handled (`issue#12309 <http://tracker.ceph.com/issues/12309>`_, `pr#5367 <http://github.com/ceph/ceph/pull/5367>`_, Sage Weil)
* osd: suicide timeout during peering - search for missing objects (`issue#12523 <http://tracker.ceph.com/issues/12523>`_, `pr#5762 <http://github.com/ceph/ceph/pull/5762>`_, Guang G Yang)
* osd: WBThrottle::clear_object: signal on cond when we reduce throttle values (`issue#12223 <http://tracker.ceph.com/issues/12223>`_, `pr#5757 <http://github.com/ceph/ceph/pull/5757>`_, Samuel Just)
* rbd: crash during shutdown after writeback blocked by IO errors (`issue#12597 <http://tracker.ceph.com/issues/12597>`_, `pr#5767 <http://github.com/ceph/ceph/pull/5767>`_, Jianpeng Ma)
* rgw: add delimiter to prefix only when path is specified (`issue#12960 <http://tracker.ceph.com/issues/12960>`_, `pr#5860 <http://github.com/ceph/ceph/pull/5860>`_, Sylvain Baubeau)
* rgw: create a tool for orphaned objects cleanup (`issue#9604 <http://tracker.ceph.com/issues/9604>`_, `pr#5717 <http://github.com/ceph/ceph/pull/5717>`_, Yehuda Sadeh)
* rgw: don't preserve acls when copying object (`issue#11563 <http://tracker.ceph.com/issues/11563>`_, `pr#6039 <http://github.com/ceph/ceph/pull/6039>`_, Yehuda Sadeh)
* rgw: don't preserve acls when copying object (`issue#12370 <http://tracker.ceph.com/issues/12370>`_, `pr#6039 <http://github.com/ceph/ceph/pull/6039>`_, Yehuda Sadeh)
* rgw: don't preserve acls when copying object (`issue#13015 <http://tracker.ceph.com/issues/13015>`_, `pr#6039 <http://github.com/ceph/ceph/pull/6039>`_, Yehuda Sadeh)
* rgw: Ensure that swift keys don't include backslashes (`issue#7647 <http://tracker.ceph.com/issues/7647>`_, `pr#5716 <http://github.com/ceph/ceph/pull/5716>`_, Yehuda Sadeh)
* rgw: GWWatcher::handle_error -> common/Mutex.cc: 95: FAILED assert(r == 0) (`issue#12208 <http://tracker.ceph.com/issues/12208>`_, `pr#6164 <http://github.com/ceph/ceph/pull/6164>`_, Yehuda Sadeh)
* rgw: HTTP return code is not being logged by CivetWeb  (`issue#12432 <http://tracker.ceph.com/issues/12432>`_, `pr#5498 <http://github.com/ceph/ceph/pull/5498>`_, Yehuda Sadeh)
* rgw: init_rados failed leads to repeated delete (`issue#12978 <http://tracker.ceph.com/issues/12978>`_, `pr#6165 <http://github.com/ceph/ceph/pull/6165>`_, Xiaowei Chen)
* rgw: init some manifest fields when handling explicit objs (`issue#11455 <http://tracker.ceph.com/issues/11455>`_, `pr#5732 <http://github.com/ceph/ceph/pull/5732>`_, Yehuda Sadeh)
* rgw: Keystone Fernet tokens break auth (`issue#12761 <http://tracker.ceph.com/issues/12761>`_, `pr#6162 <http://github.com/ceph/ceph/pull/6162>`_, Abhishek Lekshmanan)
* rgw: region data still exist in region-map after region-map update (`issue#12964 <http://tracker.ceph.com/issues/12964>`_, `pr#6163 <http://github.com/ceph/ceph/pull/6163>`_, dwj192)
* rgw: remove trailing :port from host for purposes of subdomain matching (`issue#12353 <http://tracker.ceph.com/issues/12353>`_, `pr#6042 <http://github.com/ceph/ceph/pull/6042>`_, Yehuda Sadeh)
* rgw: rest-bench common/WorkQueue.cc: 54: FAILED assert(_threads.empty()) (`issue#3896 <http://tracker.ceph.com/issues/3896>`_, `pr#5383 <http://github.com/ceph/ceph/pull/5383>`_, huangjun)
* rgw: returns requested bucket name raw in Bucket response header (`issue#12537 <http://tracker.ceph.com/issues/12537>`_, `pr#5715 <http://github.com/ceph/ceph/pull/5715>`_, Yehuda Sadeh)
* rgw: segmentation fault when rgw_gc_max_objs > HASH_PRIME (`issue#12630 <http://tracker.ceph.com/issues/12630>`_, `pr#5719 <http://github.com/ceph/ceph/pull/5719>`_, Ruifeng Yang)
* rgw: segments are read during HEAD on Swift DLO (`issue#12780 <http://tracker.ceph.com/issues/12780>`_, `pr#6160 <http://github.com/ceph/ceph/pull/6160>`_, Yehuda Sadeh)
* rgw: setting max number of buckets for user via ceph.conf option  (`issue#12714 <http://tracker.ceph.com/issues/12714>`_, `pr#6166 <http://github.com/ceph/ceph/pull/6166>`_, Vikhyat Umrao)
* rgw: Swift API: X-Trans-Id header is wrongly formatted (`issue#12108 <http://tracker.ceph.com/issues/12108>`_, `pr#5721 <http://github.com/ceph/ceph/pull/5721>`_, Radoslaw Zarzynski)
* rgw: testGetContentType and testHead failed (`issue#11091 <http://tracker.ceph.com/issues/11091>`_, `pr#5718 <http://github.com/ceph/ceph/pull/5718>`_, Radoslaw Zarzynski)
* rgw: testGetContentType and testHead failed (`issue#11438 <http://tracker.ceph.com/issues/11438>`_, `pr#5718 <http://github.com/ceph/ceph/pull/5718>`_, Radoslaw Zarzynski)
* rgw: testGetContentType and testHead failed (`issue#12157 <http://tracker.ceph.com/issues/12157>`_, `pr#5718 <http://github.com/ceph/ceph/pull/5718>`_, Radoslaw Zarzynski)
* rgw: testGetContentType and testHead failed (`issue#12158 <http://tracker.ceph.com/issues/12158>`_, `pr#5718 <http://github.com/ceph/ceph/pull/5718>`_, Radoslaw Zarzynski)
* rgw: testGetContentType and testHead failed (`issue#12363 <http://tracker.ceph.com/issues/12363>`_, `pr#5718 <http://github.com/ceph/ceph/pull/5718>`_, Radoslaw Zarzynski)
* rgw: the arguments 'domain' should not be assigned when return false (`issue#12629 <http://tracker.ceph.com/issues/12629>`_, `pr#5720 <http://github.com/ceph/ceph/pull/5720>`_, Ruifeng Yang)
* tests: qa/workunits/cephtool/test.sh: don't assume crash_replay_interval=45 (`issue#13406 <http://tracker.ceph.com/issues/13406>`_, `pr#6172 <http://github.com/ceph/ceph/pull/6172>`_, Sage Weil)
* tests: TEST_crush_rule_create_erasure consistently fails on i386 builder (`issue#12419 <http://tracker.ceph.com/issues/12419>`_, `pr#6201 <http://github.com/ceph/ceph/pull/6201>`_, Loic Dachary)
* tools: ceph-disk zap should ensure block device (`issue#11272 <http://tracker.ceph.com/issues/11272>`_, `pr#5755 <http://github.com/ceph/ceph/pull/5755>`_, Loic Dachary)

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.4.txt>`.


v0.94.3 Hammer
==============

This Hammer point release fixes a critical (though rare) data
corruption bug that could be triggered when logs are rotated via
SIGHUP.  It also fixes a range of other important bugs in the OSD,
monitor, RGW, RGW, and CephFS.

All v0.94.x Hammer users are strongly encouraged to upgrade.

Upgrading
---------

* The ``pg ls-by-{pool,primary,osd}`` commands and ``pg ls`` now take
  the argument ``recovering`` instead of ``recovery`` in order to
  include the recovering pgs in the listed pgs.

Notable Changes
---------------
* librbd: aio calls may block (`issue#11770 <http://tracker.ceph.com/issues/11770>`_, `pr#4875 <http://github.com/ceph/ceph/pull/4875>`_, Jason Dillaman)
* osd: make the all osd/filestore thread pool suicide timeouts separately configurable (`issue#11701 <http://tracker.ceph.com/issues/11701>`_, `pr#5159 <http://github.com/ceph/ceph/pull/5159>`_, Samuel Just)
* mon: ceph fails to compile with boost 1.58 (`issue#11982 <http://tracker.ceph.com/issues/11982>`_, `pr#5122 <http://github.com/ceph/ceph/pull/5122>`_, Kefu Chai)
* tests: TEST_crush_reject_empty must not run a mon (`issue#12285,11975 <http://tracker.ceph.com/issues/12285,11975>`_, `pr#5208 <http://github.com/ceph/ceph/pull/5208>`_, Kefu Chai)
* osd: FAILED assert(!old_value.deleted()) in upgrade:giant-x-hammer-distro-basic-multi run (`issue#11983 <http://tracker.ceph.com/issues/11983>`_, `pr#5121 <http://github.com/ceph/ceph/pull/5121>`_, Samuel Just)
* build/ops: linking ceph to tcmalloc causes segfault on SUSE SLE11-SP3 (`issue#12368 <http://tracker.ceph.com/issues/12368>`_, `pr#5265 <http://github.com/ceph/ceph/pull/5265>`_, Thorsten Behrens)
* common: utf8 and old gcc breakage on RHEL6.5 (`issue#7387 <http://tracker.ceph.com/issues/7387>`_, `pr#4687 <http://github.com/ceph/ceph/pull/4687>`_, Kefu Chai)
* crush: take crashes due to invalid arg (`issue#11740 <http://tracker.ceph.com/issues/11740>`_, `pr#4891 <http://github.com/ceph/ceph/pull/4891>`_, Sage Weil)
* rgw: need conversion tool to handle fixes following #11974 (`issue#12502 <http://tracker.ceph.com/issues/12502>`_, `pr#5384 <http://github.com/ceph/ceph/pull/5384>`_, Yehuda Sadeh)
* rgw: Swift API: support for 202 Accepted response code on container creation (`issue#12299 <http://tracker.ceph.com/issues/12299>`_, `pr#5214 <http://github.com/ceph/ceph/pull/5214>`_, Radoslaw Zarzynski)
* common: Log::reopen_log_file: take m_flush_mutex (`issue#12520 <http://tracker.ceph.com/issues/12520>`_, `pr#5405 <http://github.com/ceph/ceph/pull/5405>`_, Samuel Just)
* rgw: Properly respond to the Connection header with Civetweb (`issue#12398 <http://tracker.ceph.com/issues/12398>`_, `pr#5284 <http://github.com/ceph/ceph/pull/5284>`_, Wido den Hollander)
* rgw: multipart list part response returns incorrect field (`issue#12399 <http://tracker.ceph.com/issues/12399>`_, `pr#5285 <http://github.com/ceph/ceph/pull/5285>`_, Henry Chang)
* build/ops: ceph.spec.in: 95-ceph-osd.rules, mount.ceph, and mount.fuse.ceph not installed properly on SUSE (`issue#12397 <http://tracker.ceph.com/issues/12397>`_, `pr#5283 <http://github.com/ceph/ceph/pull/5283>`_, Nathan Cutler)
* rgw: radosgw-admin dumps user info twice (`issue#12400 <http://tracker.ceph.com/issues/12400>`_, `pr#5286 <http://github.com/ceph/ceph/pull/5286>`_, guce)
* doc: fix doc build (`issue#12180 <http://tracker.ceph.com/issues/12180>`_, `pr#5095 <http://github.com/ceph/ceph/pull/5095>`_, Kefu Chai)
* tests: backport 11493 fixes, and test, preventing ec cache pools (`issue#12314 <http://tracker.ceph.com/issues/12314>`_, `pr#4961 <http://github.com/ceph/ceph/pull/4961>`_, Samuel Just)
* rgw: does not send Date HTTP header when civetweb frontend is used (`issue#11872 <http://tracker.ceph.com/issues/11872>`_, `pr#5228 <http://github.com/ceph/ceph/pull/5228>`_, Radoslaw Zarzynski)
* mon: pg ls is broken (`issue#11910 <http://tracker.ceph.com/issues/11910>`_, `pr#5160 <http://github.com/ceph/ceph/pull/5160>`_, Kefu Chai)
* librbd: A client opening an image mid-resize can result in the object map being invalidated (`issue#12237 <http://tracker.ceph.com/issues/12237>`_, `pr#5279 <http://github.com/ceph/ceph/pull/5279>`_, Jason Dillaman)
* doc: missing man pages for ceph-create-keys, ceph-disk-* (`issue#11862 <http://tracker.ceph.com/issues/11862>`_, `pr#4846 <http://github.com/ceph/ceph/pull/4846>`_, Nathan Cutler)
* tools: ceph-post-file fails on rhel7 (`issue#11876 <http://tracker.ceph.com/issues/11876>`_, `pr#5038 <http://github.com/ceph/ceph/pull/5038>`_, Sage Weil)
* build/ops: rcceph script is buggy (`issue#12090 <http://tracker.ceph.com/issues/12090>`_, `pr#5028 <http://github.com/ceph/ceph/pull/5028>`_, Owen Synge)
* rgw: Bucket header is enclosed by quotes (`issue#11874 <http://tracker.ceph.com/issues/11874>`_, `pr#4862 <http://github.com/ceph/ceph/pull/4862>`_, Wido den Hollander)
* build/ops: packaging: add SuSEfirewall2 service files (`issue#12092 <http://tracker.ceph.com/issues/12092>`_, `pr#5030 <http://github.com/ceph/ceph/pull/5030>`_, Tim Serong)
* rgw: Keystone PKI token expiration is not enforced (`issue#11722 <http://tracker.ceph.com/issues/11722>`_, `pr#4884 <http://github.com/ceph/ceph/pull/4884>`_, Anton Aksola)
* build/ops: debian/control: ceph-common (>> 0.94.2) must be >= 0.94.2-2 (`issue#12529,11998 <http://tracker.ceph.com/issues/12529,11998>`_, `pr#5417 <http://github.com/ceph/ceph/pull/5417>`_, Loic Dachary)
* mon: Clock skew causes missing summary and confuses Calamari (`issue#11879 <http://tracker.ceph.com/issues/11879>`_, `pr#4868 <http://github.com/ceph/ceph/pull/4868>`_, Thorsten Behrens)
* rgw: rados objects wronly deleted (`issue#12099 <http://tracker.ceph.com/issues/12099>`_, `pr#5117 <http://github.com/ceph/ceph/pull/5117>`_, wuxingyi)
* tests: kernel_untar_build fails on EL7 (`issue#12098 <http://tracker.ceph.com/issues/12098>`_, `pr#5119 <http://github.com/ceph/ceph/pull/5119>`_, Greg Farnum)
* fs: Fh ref count will leak if readahead does not need to do read from osd (`issue#12319 <http://tracker.ceph.com/issues/12319>`_, `pr#5427 <http://github.com/ceph/ceph/pull/5427>`_, Zhi Zhang)
* mon: OSDMonitor: allow addition of cache pool with non-empty snaps with co… (`issue#12595 <http://tracker.ceph.com/issues/12595>`_, `pr#5252 <http://github.com/ceph/ceph/pull/5252>`_, Samuel Just)
* mon: MDSMonitor: handle MDSBeacon messages properly (`issue#11979 <http://tracker.ceph.com/issues/11979>`_, `pr#5123 <http://github.com/ceph/ceph/pull/5123>`_, Kefu Chai)
* tools: ceph-disk: get_partition_type fails on /dev/cciss... (`issue#11760 <http://tracker.ceph.com/issues/11760>`_, `pr#4892 <http://github.com/ceph/ceph/pull/4892>`_, islepnev)
* build/ops: max files open limit for OSD daemon is too low (`issue#12087 <http://tracker.ceph.com/issues/12087>`_, `pr#5026 <http://github.com/ceph/ceph/pull/5026>`_, Owen Synge)
* mon: add an "osd crush tree" command (`issue#11833 <http://tracker.ceph.com/issues/11833>`_, `pr#5248 <http://github.com/ceph/ceph/pull/5248>`_, Kefu Chai)
* mon: mon crashes when "ceph osd tree 85 --format json" (`issue#11975 <http://tracker.ceph.com/issues/11975>`_, `pr#4936 <http://github.com/ceph/ceph/pull/4936>`_, Kefu Chai)
* build/ops: ceph / ceph-dbg steal ceph-objecstore-tool from ceph-test / ceph-test-dbg (`issue#11806 <http://tracker.ceph.com/issues/11806>`_, `pr#5069 <http://github.com/ceph/ceph/pull/5069>`_, Loic Dachary)
* rgw: DragonDisk fails to create directories via S3: MissingContentLength (`issue#12042 <http://tracker.ceph.com/issues/12042>`_, `pr#5118 <http://github.com/ceph/ceph/pull/5118>`_, Yehuda Sadeh)
* build/ops: /usr/bin/ceph from ceph-common is broken without installing ceph (`issue#11998 <http://tracker.ceph.com/issues/11998>`_, `pr#5206 <http://github.com/ceph/ceph/pull/5206>`_, Ken Dreyer)
* build/ops: systemd: Increase max files open limit for OSD daemon (`issue#11964 <http://tracker.ceph.com/issues/11964>`_, `pr#5040 <http://github.com/ceph/ceph/pull/5040>`_, Owen Synge)
* build/ops: rgw/logrotate.conf calls service with wrong init script name (`issue#12044 <http://tracker.ceph.com/issues/12044>`_, `pr#5055 <http://github.com/ceph/ceph/pull/5055>`_, wuxingyi)
* common: OPT_INT option interprets 3221225472 as -1073741824, and crashes in Throttle::Throttle() (`issue#11738 <http://tracker.ceph.com/issues/11738>`_, `pr#4889 <http://github.com/ceph/ceph/pull/4889>`_, Kefu Chai)
* doc: doc/release-notes: v0.94.2 (`issue#11492 <http://tracker.ceph.com/issues/11492>`_, `pr#4934 <http://github.com/ceph/ceph/pull/4934>`_, Sage Weil)
* common: admin_socket: close socket descriptor in destructor (`issue#11706 <http://tracker.ceph.com/issues/11706>`_, `pr#4657 <http://github.com/ceph/ceph/pull/4657>`_, Jon Bernard)
* rgw: Object copy bug (`issue#11755 <http://tracker.ceph.com/issues/11755>`_, `pr#4885 <http://github.com/ceph/ceph/pull/4885>`_, Javier M. Mellid)
* rgw: empty json response when getting user quota (`issue#12245 <http://tracker.ceph.com/issues/12245>`_, `pr#5237 <http://github.com/ceph/ceph/pull/5237>`_, wuxingyi)
* fs: cephfs Dumper tries to load whole journal into memory at once (`issue#11999 <http://tracker.ceph.com/issues/11999>`_, `pr#5120 <http://github.com/ceph/ceph/pull/5120>`_, John Spray)
* rgw: Fix tool for #11442 does not correctly fix objects created via multipart uploads (`issue#12242 <http://tracker.ceph.com/issues/12242>`_, `pr#5229 <http://github.com/ceph/ceph/pull/5229>`_, Yehuda Sadeh)
* rgw: Civetweb RGW appears to report full size of object as downloaded when only partially downloaded (`issue#12243 <http://tracker.ceph.com/issues/12243>`_, `pr#5231 <http://github.com/ceph/ceph/pull/5231>`_, Yehuda Sadeh)
* osd: stuck incomplete (`issue#12362 <http://tracker.ceph.com/issues/12362>`_, `pr#5269 <http://github.com/ceph/ceph/pull/5269>`_, Samuel Just)
* osd: start_flush: filter out removed snaps before determining snapc's (`issue#11911 <http://tracker.ceph.com/issues/11911>`_, `pr#4899 <http://github.com/ceph/ceph/pull/4899>`_, Samuel Just)
* librbd: internal.cc: 1967: FAILED assert(watchers.size() == 1) (`issue#12239 <http://tracker.ceph.com/issues/12239>`_, `pr#5243 <http://github.com/ceph/ceph/pull/5243>`_, Jason Dillaman)
* librbd: new QA client upgrade tests (`issue#12109 <http://tracker.ceph.com/issues/12109>`_, `pr#5046 <http://github.com/ceph/ceph/pull/5046>`_, Jason Dillaman)
* librbd: [  FAILED  ] TestLibRBD.ExclusiveLockTransition (`issue#12238 <http://tracker.ceph.com/issues/12238>`_, `pr#5241 <http://github.com/ceph/ceph/pull/5241>`_, Jason Dillaman)
* rgw: Swift API: XML document generated in response for GET on account does not contain account name (`issue#12323 <http://tracker.ceph.com/issues/12323>`_, `pr#5227 <http://github.com/ceph/ceph/pull/5227>`_, Radoslaw Zarzynski)
* rgw: keystone does not support chunked input (`issue#12322 <http://tracker.ceph.com/issues/12322>`_, `pr#5226 <http://github.com/ceph/ceph/pull/5226>`_, Hervé Rousseau)
* mds: MDS is crashed (mds/CDir.cc: 1391: FAILED assert(!is_complete())) (`issue#11737 <http://tracker.ceph.com/issues/11737>`_, `pr#4886 <http://github.com/ceph/ceph/pull/4886>`_, Yan, Zheng)
* cli: ceph: cli interactive mode does not understand quotes (`issue#11736 <http://tracker.ceph.com/issues/11736>`_, `pr#4776 <http://github.com/ceph/ceph/pull/4776>`_, Kefu Chai)
* librbd: add valgrind memory checks for unit tests (`issue#12384 <http://tracker.ceph.com/issues/12384>`_, `pr#5280 <http://github.com/ceph/ceph/pull/5280>`_, Zhiqiang Wang)
* build/ops: admin/build-doc: script fails silently under certain circumstances (`issue#11902 <http://tracker.ceph.com/issues/11902>`_, `pr#4877 <http://github.com/ceph/ceph/pull/4877>`_, John Spray)
* osd: Fixes for rados ops with snaps (`issue#11908 <http://tracker.ceph.com/issues/11908>`_, `pr#4902 <http://github.com/ceph/ceph/pull/4902>`_, Samuel Just)
* build/ops: ceph.spec.in: ceph-common subpackage def needs tweaking for SUSE/openSUSE (`issue#12308 <http://tracker.ceph.com/issues/12308>`_, `pr#4883 <http://github.com/ceph/ceph/pull/4883>`_, Nathan Cutler)
* fs: client: reference counting 'struct Fh' (`issue#12088 <http://tracker.ceph.com/issues/12088>`_, `pr#5222 <http://github.com/ceph/ceph/pull/5222>`_, Yan, Zheng)
* build/ops: ceph.spec: update OpenSUSE BuildRequires  (`issue#11611 <http://tracker.ceph.com/issues/11611>`_, `pr#4667 <http://github.com/ceph/ceph/pull/4667>`_, Loic Dachary)

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.3.txt>`.



v0.94.2 Hammer
==============

This Hammer point release fixes a few critical bugs in RGW that can
prevent objects starting with underscore from behaving properly and
that prevent garbage collection of deleted objects when using the
Civetweb standalone mode.

All v0.94.x Hammer users are strongly encouraged to upgrade, and to
make note of the repair procedure below if RGW is in use.

Upgrading from previous Hammer release
--------------------------------------

Bug #11442 introduced a change that made rgw objects that start with underscore
incompatible with previous versions. The fix to that bug reverts to the
previous behavior. In order to be able to access objects that start with an
underscore and were created in prior Hammer releases, following the upgrade it
is required to run (for each affected bucket)::

    $ radosgw-admin bucket check --check-head-obj-locator \
                                 --bucket=<bucket> [--fix]

Notable changes
---------------

* build: compilation error: No high-precision counter available  (armhf, powerpc..) (#11432, James Page)
* ceph-dencoder links to libtcmalloc, and shouldn't (#10691, Boris Ranto)
* ceph-disk: disk zap sgdisk invocation (#11143, Owen Synge)
* ceph-disk: use a new disk as journal disk,ceph-disk prepare fail (#10983, Loic Dachary)
* ceph-objectstore-tool should be in the ceph server package (#11376, Ken Dreyer)
* librados: can get stuck in redirect loop if osdmap epoch == last_force_op_resend (#11026, Jianpeng Ma)
* librbd: A retransmit of proxied flatten request can result in -EINVAL (Jason Dillaman)
* librbd: ImageWatcher should cancel in-flight ops on watch error (#11363, Jason Dillaman)
* librbd: Objectcacher setting max object counts too low (#7385, Jason Dillaman)
* librbd: Periodic failure of TestLibRBD.DiffIterateStress (#11369, Jason Dillaman)
* librbd: Queued AIO reference counters not properly updated (#11478, Jason Dillaman)
* librbd: deadlock in image refresh (#5488, Jason Dillaman)
* librbd: notification race condition on snap_create (#11342, Jason Dillaman)
* mds: Hammer uclient checking (#11510, John Spray)
* mds: remove caps from revoking list when caps are voluntarily released (#11482, Yan, Zheng)
* messenger: double clear of pipe in reaper (#11381, Haomai Wang)
* mon: Total size of OSDs is a maginitude less than it is supposed to be. (#11534, Zhe Zhang)
* osd: don't check order in finish_proxy_read (#11211, Zhiqiang Wang)
* osd: handle old semi-deleted pgs after upgrade (#11429, Samuel Just)
* osd: object creation by write cannot use an offset on an erasure coded pool (#11507, Jianpeng Ma)
* rgw: Improve rgw HEAD request by avoiding read the body of the first chunk (#11001, Guang Yang)
* rgw: civetweb is hitting a limit (number of threads 1024) (#10243, Yehuda Sadeh)
* rgw: civetweb should use unique request id (#10295, Orit Wasserman)
* rgw: critical fixes for hammer (#11447, #11442, Yehuda Sadeh)
* rgw: fix swift COPY headers (#10662, #10663, #11087, #10645, Radoslaw Zarzynski)
* rgw: improve performance for large object  (multiple chunks) GET (#11322, Guang Yang)
* rgw: init-radosgw: run RGW as root (#11453, Ken Dreyer)
* rgw: keystone token cache does not work correctly (#11125, Yehuda Sadeh)
* rgw: make quota/gc thread configurable for starting (#11047, Guang Yang)
* rgw: make swift responses of RGW return last-modified, content-length, x-trans-id headers.(#10650, Radoslaw Zarzynski)
* rgw: merge manifests correctly when there's prefix override (#11622, Yehuda Sadeh)
* rgw: quota not respected in POST object (#11323, Sergey Arkhipov)
* rgw: restore buffer of multipart upload after EEXIST (#11604, Yehuda Sadeh)
* rgw: shouldn't need to disable rgw_socket_path if frontend is configured (#11160, Yehuda Sadeh)
* rgw: swift: Response header of GET request for container does not contain X-Container-Object-Count, X-Container-Bytes-Used and x-trans-id headers (#10666, Dmytro Iurchenko)
* rgw: swift: Response header of POST request for object does not contain content-length and x-trans-id headers (#10661, Radoslaw Zarzynski)
* rgw: swift: response for GET/HEAD on container does not contain the X-Timestamp header (#10938, Radoslaw Zarzynski)
* rgw: swift: response for PUT on /container does not contain the mandatory Content-Length header when FCGI is used (#11036, #10971, Radoslaw Zarzynski)
* rgw: swift: wrong handling of empty metadata on Swift container (#11088, Radoslaw Zarzynski)
* tests: TestFlatIndex.cc races with TestLFNIndex.cc (#11217, Xinze Chi)
* tests: ceph-helpers kill_daemons fails when kill fails (#11398, Loic Dachary)

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.2.txt>`.


v0.94.1 Hammer
==============

This bug fix release fixes a few critical issues with CRUSH.  The most
important addresses a bug in feature bit enforcement that may prevent
pre-hammer clients from communicating with the cluster during an
upgrade.  This only manifests in some cases (for example, when the
'rack' type is in use in the CRUSH map, and possibly other cases), but for
safety we strongly recommend that all users use 0.94.1 instead of 0.94 when
upgrading.

There is also a fix in the new straw2 buckets when OSD weights are 0.

We recommend that all v0.94 users upgrade.

Notable changes
---------------

* crush: fix divide-by-0 in straw2 (#11357 Sage Weil)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* osd: fix negative degraded objects during backfilling (#7737 Guang Yang)

For more detailed information, see :download:`the complete changelog <../changelog/v0.94.1.txt>`.


v0.94 Hammer
============

This major release is expected to form the basis of the next long-term
stable series.  It is intended to supersede v0.80.x Firefly.

Highlights since Giant include:

* *RADOS Performance*: a range of improvements have been made in the
  OSD and client-side librados code that improve the throughput on
  flash backends and improve parallelism and scaling on fast machines.
* *Simplified RGW deployment*: the ceph-deploy tool now has a new
  'ceph-deploy rgw create HOST' command that quickly deploys a
  instance of the S3/Swift gateway using the embedded Civetweb server.
  This is vastly simpler than the previous Apache-based deployment.
  There are a few rough edges (e.g., around SSL support) but we
  encourage users to try `the new method`_.
* *RGW object versioning*: RGW now supports the S3 object versioning
  API, which preserves old version of objects instead of overwriting
  them.
* *RGW bucket sharding*: RGW can now shard the bucket index for large
  buckets across, improving performance for very large buckets.
* *RBD object maps*: RBD now has an object map function that tracks
  which parts of the image are allocating, improving performance for
  clones and for commands like export and delete.
* *RBD mandatory locking*: RBD has a new mandatory locking framework
  (still disabled by default) that adds additional safeguards to
  prevent multiple clients from using the same image at the same time.
* *RBD copy-on-read*: RBD now supports copy-on-read for image clones,
  improving performance for some workloads.
* *CephFS snapshot improvements*: Many many bugs have been fixed with
  CephFS snapshots.  Although they are still disabled by default,
  stability has improved significantly.
* *CephFS Recovery tools*: We have built some journal recovery and
  diagnostic tools. Stability and performance of single-MDS systems is
  vastly improved in Giant, and more improvements have been made now
  in Hammer.  Although we still recommend caution when storing
  important data in CephFS, we do encourage testing for non-critical
  workloads so that we can better gauge the feature, usability,
  performance, and stability gaps.
* *CRUSH improvements*: We have added a new straw2 bucket algorithm
  that reduces the amount of data migration required when changes are
  made to the cluster.
* *Shingled erasure codes (SHEC)*: The OSDs now have experimental
  support for shingled erasure codes, which allow a small amount of
  additional storage to be traded for improved recovery performance.
* *RADOS cache tiering*: A series of changes have been made in the
  cache tiering code that improve performance and reduce latency.
* *RDMA support*: There is now experimental support the RDMA via the
  Accelio (libxio) library.
* *New administrator commands*: The 'ceph osd df' command shows
  pertinent details on OSD disk utilizations.  The 'ceph pg ls ...'
  command makes it much simpler to query PG states while diagnosing
  cluster issues.

.. _the new method: ../start/quick-ceph-deploy/#add-an-rgw-instance

Other highlights since Firefly include:

* *CephFS*: we have fixed a raft of bugs in CephFS and built some
  basic journal recovery and diagnostic tools.  Stability and
  performance of single-MDS systems is vastly improved in Giant.
  Although we do not yet recommend CephFS for production deployments,
  we do encourage testing for non-critical workloads so that we can
  better gauge the feature, usability, performance, and stability
  gaps.
* *Local Recovery Codes*: the OSDs now support an erasure-coding scheme
  that stores some additional data blocks to reduce the IO required to
  recover from single OSD failures.
* *Degraded vs misplaced*: the Ceph health reports from 'ceph -s' and
  related commands now make a distinction between data that is
  degraded (there are fewer than the desired number of copies) and
  data that is misplaced (stored in the wrong location in the
  cluster).  The distinction is important because the latter does not
  compromise data safety.
* *Tiering improvements*: we have made several improvements to the
  cache tiering implementation that improve performance.  Most
  notably, objects are not promoted into the cache tier by a single
  read; they must be found to be sufficiently hot before that happens.
* *Monitor performance*: the monitors now perform writes to the local
  data store asynchronously, improving overall responsiveness.
* *Recovery tools*: the ceph-objectstore-tool is greatly expanded to
  allow manipulation of an individual OSDs data store for debugging
  and repair purposes.  This is most heavily used by our QA
  infrastructure to exercise recovery code.

I would like to take this opportunity to call out the amazing growth
in contributors to Ceph beyond the core development team from Inktank.
Hammer features major new features and improvements from Intel, Fujitsu,
UnitedStack, Yahoo, UbuntuKylin, CohortFS, Mellanox, CERN, Deutsche
Telekom, Mirantis, and SanDisk.

Dedication
----------

This release is dedicated in memoriam to Sandon Van Ness, aka
Houkouonchi, who unexpectedly passed away a few weeks ago.  Sandon was
responsible for maintaining the large and complex Sepia lab that
houses the Ceph project's build and test infrastructure.  His efforts
have made an important impact on our ability to reliably test Ceph
with a relatively small group of people.  He was a valued member of
the team and we will miss him.  H is also for Houkouonchi.

Upgrading
---------

* If your existing cluster is running a version older than v0.80.x
  Firefly, please first upgrade to the latest Firefly release before
  moving on to Giant.  We have not tested upgrades directly from
  Emperor, Dumpling, or older releases.

  We *have* tested:

   * Firefly to Hammer
   * Giant to Hammer
   * Dumpling to Firefly to Hammer

* Please upgrade daemons in the following order:

   #. Monitors
   #. OSDs
   #. MDSs and/or radosgw

  Note that the relative ordering of OSDs and monitors should not matter, but
  we primarily tested upgrading monitors first.

* The ceph-osd daemons will perform a disk-format upgrade improve the
  PG metadata layout and to repair a minor bug in the on-disk format.
  It may take a minute or two for this to complete, depending on how
  many objects are stored on the node; do not be alarmed if they do
  not marked "up" by the cluster immediately after starting.

* If upgrading from v0.93, set
   osd enable degraded writes = false

  on all osds prior to upgrading.  The degraded writes feature has
  been reverted due to 11155.

* The LTTNG tracing in librbd and librados is disabled in the release packages
  until we find a way to avoid violating distro security policies when linking
  libust.

Upgrading from v0.87.x Giant
----------------------------

* librbd and librados include lttng tracepoints on distros with
  liblttng 2.4 or later (only Ubuntu Trusty for the ceph.com
  packages). When running a daemon that uses these libraries, i.e. an
  application that calls fork(2) or clone(2) without exec(3), you must
  set LD_PRELOAD=liblttng-ust-fork.so.0 to prevent a crash in the
  lttng atexit handler when the process exits. The only ceph tool that
  requires this is rbd-fuse.

* If rgw_socket_path is defined and rgw_frontends defines a
  socket_port and socket_host, we now allow the rgw_frontends settings
  to take precedence.  This change should only affect users who have
  made non-standard changes to their radosgw configuration.

* If you are upgrading specifically from v0.92, you must stop all OSD
  daemons and flush their journals (``ceph-osd -i NNN
  --flush-journal``) before upgrading.  There was a transaction
  encoding bug in v0.92 that broke compatibility.  Upgrading from v0.93,
  v0.91, or anything earlier is safe.

* The experimental 'keyvaluestore-dev' OSD backend has been renamed
  'keyvaluestore' (for simplicity) and marked as experimental.  To
  enable this untested feature and acknowledge that you understand
  that it is untested and may destroy data, you need to add the
  following to your ceph.conf::

    enable experimental unrecoverable data corrupting features = keyvaluestore

* The following librados C API function calls take a 'flags' argument whose value
  is now correctly interpreted:

     rados_write_op_operate()
     rados_aio_write_op_operate()
     rados_read_op_operate()
     rados_aio_read_op_operate()

  The flags were not correctly being translated from the librados constants to the
  internal values.  Now they are.  Any code that is passing flags to these methods
  should be audited to ensure that they are using the correct LIBRADOS_OP_FLAG_*
  constants.

* The 'rados' CLI 'copy' and 'cppool' commands now use the copy-from operation,
  which means the latest CLI cannot run these commands against pre-firefly OSDs.

* The librados watch/notify API now includes a watch_flush() operation to flush
  the async queue of notify operations.  This should be called by any watch/notify
  user prior to rados_shutdown().

* The 'category' field for objects has been removed.  This was originally added
  to track PG stat summations over different categories of objects for use by
  radosgw.  It is no longer has any known users and is prone to abuse because it
  can lead to a pg_stat_t structure that is unbounded.  The librados API calls
  that accept this field now ignore it, and the OSD no longer tracks the
  per-category summations.

* The output for 'rados df' has changed.  The 'category' level has been
  eliminated, so there is now a single stat object per pool.  The structure of
  the JSON output is different, and the plaintext output has one less column.

* The 'rados create <objectname> [category]' optional category argument is no
  longer supported or recognized.

* rados.py's Rados class no longer has a __del__ method; it was causing
  problems on interpreter shutdown and use of threads.  If your code has
  Rados objects with limited lifetimes and you're concerned about locked
  resources, call Rados.shutdown() explicitly.

* There is a new version of the librados watch/notify API with vastly
  improved semantics.  Any applications using this interface are
  encouraged to migrate to the new API.  The old API calls are marked
  as deprecated and will eventually be removed.

* The librados rados_unwatch() call used to be safe to call on an
  invalid handle.  The new version has undefined behavior when passed
  a bogus value (for example, when rados_watch() returns an error and
  handle is not defined).

* The structure of the formatted 'pg stat' command is changed for the
  portion that counts states by name to avoid using the '+' character
  (which appears in state names) as part of the XML token (it is not
  legal).

* Previously, the formatted output of 'ceph pg stat -f ...' was a full
  pg dump that included all metadata about all PGs in the system.  It
  is now a concise summary of high-level PG stats, just like the
  unformatted 'ceph pg stat' command.

* All JSON dumps of floating point values were incorrecting surrounding the
  value with quotes.  These quotes have been removed.  Any consumer of structured
  JSON output that was consuming the floating point values was previously having
  to interpret the quoted string and will most likely need to be fixed to take
  the unquoted number.

* New ability to list all objects from all namespaces that can fail or
  return incomplete results when not all OSDs have been upgraded.
  Features rados --all ls, rados cppool, rados export, rados
  cache-flush-evict-all and rados cache-try-flush-evict-all can also
  fail or return incomplete results.

* Due to a change in the Linux kernel version 3.18 and the limits of the FUSE
  interface, ceph-fuse needs be mounted as root on at least some systems. See
  issues #9997, #10277, and #10542 for details.

Upgrading from v0.80x Firefly (additional notes)
------------------------------------------------

* The client-side caching for librbd is now enabled by default (rbd
  cache = true).  A safety option (rbd cache writethrough until flush
  = true) is also enabled so that writeback caching is not used until
  the library observes a 'flush' command, indicating that the librbd
  users is passing that operation through from the guest VM.  This
  avoids potential data loss when used with older versions of qemu
  that do not support flush.

    leveldb_write_buffer_size = 8*1024*1024  = 33554432   // 8MB
    leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
    leveldb_block_size        = 64*1024       = 65536     // 64KB
    leveldb_compression       = false
    leveldb_log               = ""

  OSDs will still maintain the following osd-specific defaults:

    leveldb_log               = ""

* The 'rados getxattr ...' command used to add a gratuitous newline to the attr
  value; it now does not.

* The ``*_kb perf`` counters on the monitor have been removed.  These are
  replaced with a new set of ``*_bytes`` counters (e.g., ``cluster_osd_kb`` is
  replaced by ``cluster_osd_bytes``).

* The ``rd_kb`` and ``wr_kb`` fields in the JSON dumps for pool stats (accessed
  via the ``ceph df detail -f json-pretty`` and related commands) have been
  replaced with corresponding ``*_bytes`` fields.  Similarly, the
  ``total_space``, ``total_used``, and ``total_avail`` fields are replaced with
  ``total_bytes``, ``total_used_bytes``,  and ``total_avail_bytes`` fields.

* The ``rados df --format=json`` output ``read_bytes`` and ``write_bytes``
  fields were incorrectly reporting ops; this is now fixed.

* The ``rados df --format=json`` output previously included ``read_kb`` and
  ``write_kb`` fields; these have been removed.  Please use ``read_bytes`` and
  ``write_bytes`` instead (and divide by 1024 if appropriate).

* The experimental keyvaluestore-dev OSD backend had an on-disk format
  change that prevents existing OSD data from being upgraded.  This
  affects developers and testers only.

* mon-specific and osd-specific leveldb options have been removed.
  From this point onward users should use the `leveldb_*` generic
  options and add the options in the appropriate sections of their
  configuration files.  Monitors will still maintain the following
  monitor-specific defaults:

    leveldb_write_buffer_size = 8*1024*1024  = 33554432   // 8MB
    leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
    leveldb_block_size        = 64*1024       = 65536     // 64KB
    leveldb_compression       = false
    leveldb_log               = ""

  OSDs will still maintain the following osd-specific defaults:

    leveldb_log               = ""

* CephFS support for the legacy anchor table has finally been removed.
  Users with file systems created before firefly should ensure that inodes
  with multiple hard links are modified *prior* to the upgrade to ensure that
  the backtraces are written properly.  For example::

    sudo find /mnt/cephfs -type f -links +1 -exec touch \{\} \;

* We disallow nonsensical 'tier cache-mode' transitions.  From this point
  onward, 'writeback' can only transition to 'forward' and 'forward'
  can transition to 1) 'writeback' if there are dirty objects, or 2) any if
  there are no dirty objects.


Notable changes since v0.93
---------------------------

* build: a few cmake fixes (Matt Benjamin)
* build: fix build on RHEL/CentOS 5.9 (Rohan Mars)
* build: reorganize Makefile to allow modular builds (Boris Ranto)
* ceph-fuse: be more forgiving on remount (#10982 Greg Farnum)
* ceph: improve CLI parsing (#11093 David Zafman)
* common: fix cluster logging to default channel (#11177 Sage Weil)
* crush: fix parsing of straw2 buckets (#11015 Sage Weil)
* doc: update man pages (David Zafman)
* librados: fix leak in C_TwoContexts (Xiong Yiliang)
* librados: fix leak in watch/notify path (Sage Weil)
* librbd: fix and improve AIO cache invalidation (#10958 Jason Dillaman)
* librbd: fix memory leak (Jason Dillaman)
* librbd: fix ordering/queueing of resize operations (Jason Dillaman)
* librbd: validate image is r/w on resize/flatten (Jason Dillaman)
* librbd: various internal locking fixes (Jason Dillaman)
* lttng: tracing is disabled until we streamline dependencies (Josh Durgin)
* mon: add bootstrap-rgw profile (Sage Weil)
* mon: do not pollute mon dir with CSV files from CRUSH check (Loic Dachary)
* mon: fix clock drift time check interval (#10546 Joao Eduardo Luis)
* mon: fix units in store stats (Joao Eduardo Luis)
* mon: improve error handling on erasure code profile set (#10488, #11144 Loic Dachary)
* mon: set {read,write}_tier on 'osd tier add-cache ...' (Jianpeng Ma)
* ms: xio: fix misc bugs (Matt Benjamin, Vu Pham)
* osd: DBObjectMap: fix locking to prevent rare crash (#9891 Samuel Just)
* osd: fix and document last_epoch_started semantics (Samuel Just)
* osd: fix divergent entry handling on PG split (Samuel Just)
* osd: fix leak on shutdown (Kefu Chai)
* osd: fix recording of digest on scrub (Samuel Just)
* osd: fix whiteout handling (Sage Weil)
* rbd: allow v2 striping parameters for clones and imports (Jason Dillaman)
* rbd: fix formatted output of image features (Jason Dillaman)
* rbd: updat eman page (Ilya Dryomov)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for civetweb (#10965 Yehuda Sadeh)
* rgw: fix sysvinit script when rgw_socket_path is not defined (#11159 Yehuda Sadeh, Dan Mick)
* rgw: pass civetweb configurables through (#10907 Yehuda Sadeh)
* rgw: use new watch/notify API (Yehuda Sadeh, Sage Weil)
* osd: reverted degraded writes feature due to 11155

Notable changes since v0.87.x Giant
-----------------------------------

* add experimental features option (Sage Weil)
* arch: fix NEON feaeture detection (#10185 Loic Dachary)
* asyncmsgr: misc fixes (Haomai Wang)
* buffer: add 'shareable' construct (Matt Benjamin)
* buffer: add list::get_contiguous (Sage Weil)
* buffer: avoid rebuild if buffer already contiguous (Jianpeng Ma)
* build: CMake support (Ali Maredia, Casey Bodley, Adam Emerson, Marcus Watts, Matt Benjamin)
* build: a few cmake fixes (Matt Benjamin)
* build: aarch64 build fixes (Noah Watkins, Haomai Wang)
* build: adjust build deps for yasm, virtualenv (Jianpeng Ma)
* build: fix 'make check' races (#10384 Loic Dachary)
* build: fix build on RHEL/CentOS 5.9 (Rohan Mars)
* build: fix pkg names when libkeyutils is missing (Pankag Garg, Ken Dreyer)
* build: improve build dependency tooling (Loic Dachary)
* build: reorganize Makefile to allow modular builds (Boris Ranto)
* build: support for jemalloc (Shishir Gowda)
* ceph-disk: Scientific Linux support (Dan van der Ster)
* ceph-disk: allow journal partition re-use (#10146 Loic Dachary, Dav van der Ster)
* ceph-disk: call partx/partprobe consistency (#9721 Loic Dachary)
* ceph-disk: do not re-use partition if encryption is required (Loic Dachary)
* ceph-disk: fix dmcrypt key permissions (Loic Dachary)
* ceph-disk: fix umount race condition (#10096 Blaine Gardner)
* ceph-disk: improved systemd support (Owen Synge)
* ceph-disk: init=none option (Loic Dachary)
* ceph-disk: misc fixes (Christos Stavrakakis)
* ceph-disk: respect --statedir for keyring (Loic Dachary)
* ceph-disk: set guid if reusing journal partition (Dan van der Ster)
* ceph-disk: support LUKS for encrypted partitions (Andrew Bartlett, Loic Dachary)
* ceph-fuse, libcephfs: POSIX file lock support (Yan, Zheng)
* ceph-fuse, libcephfs: allow xattr caps in inject_release_failure (#9800 John Spray)
* ceph-fuse, libcephfs: fix I_COMPLETE_ORDERED checks (#9894 Yan, Zheng)
* ceph-fuse, libcephfs: fix cap flush overflow (Greg Farnum, Yan, Zheng)
* ceph-fuse, libcephfs: fix root inode xattrs (Yan, Zheng)
* ceph-fuse, libcephfs: preserve dir ordering (#9178 Yan, Zheng)
* ceph-fuse, libcephfs: trim inodes before reconnecting to MDS (Yan, Zheng)
* ceph-fuse,libcephfs: add support for O_NOFOLLOW and O_PATH (Greg Farnum)
* ceph-fuse,libcephfs: resend requests before completing cap reconnect (#10912 Yan, Zheng)
* ceph-fuse: be more forgiving on remount (#10982 Greg Farnum)
* ceph-fuse: fix dentry invalidation on 3.18+ kernels (#9997 Yan, Zheng)
* ceph-fuse: fix kernel cache trimming (#10277 Yan, Zheng)
* ceph-fuse: select kernel cache invalidation mechanism based on kernel version (Greg Farnum)
* ceph-monstore-tool: fix shutdown (#10093 Loic Dachary)
* ceph-monstore-tool: fix/improve CLI (Joao Eduardo Luis)
* ceph-objectstore-tool: fix import (#10090 David Zafman)
* ceph-objectstore-tool: improved import (David Zafman)
* ceph-objectstore-tool: many improvements and tests (David Zafman)
* ceph-objectstore-tool: many many improvements (David Zafman)
* ceph-objectstore-tool: misc improvements, fixes (#9870 #9871 David Zafman)
* ceph.spec: package rbd-replay-prep (Ken Dreyer)
* ceph: add 'ceph osd df [tree]' command (#10452 Mykola Golub)
* ceph: do not parse injectargs twice (Loic Dachary)
* ceph: fix 'ceph tell ...' command validation (#10439 Joao Eduardo Luis)
* ceph: improve 'ceph osd tree' output (Mykola Golub)
* ceph: improve CLI parsing (#11093 David Zafman)
* ceph: make 'ceph -s' output more readable (Sage Weil)
* ceph: make 'ceph -s' show PG state counts in sorted order (Sage Weil)
* ceph: make 'ceph tell mon.* version' work (Mykola Golub)
* ceph: new 'ceph tell mds.$name_or_rank_or_gid' (John Spray)
* ceph: show primary-affinity in 'ceph osd tree' (Mykola Golub)
* ceph: test robustness (Joao Eduardo Luis)
* ceph_objectstore_tool: behave with sharded flag (#9661 David Zafman)
* cephfs-journal-tool: add recover_dentries function (#9883 John Spray)
* cephfs-journal-tool: fix journal import (#10025 John Spray)
* cephfs-journal-tool: skip up to expire_pos (#9977 John Spray)
* cleanup rados.h definitions with macros (Ilya Dryomov)
* common: add 'perf reset ...' admin command (Jianpeng Ma)
* common: add TableFormatter (Andreas Peters)
* common: add newline to flushed json output (Sage Weil)
* common: check syncfs() return code (Jianpeng Ma)
* common: do not unlock rwlock on destruction (Federico Simoncelli)
* common: filtering for 'perf dump' (John Spray)
* common: fix Formatter factory breakage (#10547 Loic Dachary)
* common: fix block device discard check (#10296 Sage Weil)
* common: make json-pretty output prettier (Sage Weil)
* common: remove broken CEPH_LOCKDEP optoin (Kefu Chai)
* common: shared_cache unit tests (Cheng Cheng)
* common: support new gperftools header locations (Key Dreyer)
* config: add $cctid meta variable (Adam Crume)
* crush: fix buffer overrun for poorly formed rules (#9492 Johnu George)
* crush: fix detach_bucket (#10095 Sage Weil)
* crush: fix parsing of straw2 buckets (#11015 Sage Weil)
* crush: fix several bugs in adjust_item_weight (Rongze Zhu)
* crush: fix tree bucket behavior (Rongze Zhu)
* crush: improve constness (Loic Dachary)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* crush: straw bucket weight calculation fixes (#9998 Sage Weil)
* crush: update tries stats for indep rules (#10349 Loic Dachary)
* crush: use larger choose_tries value for erasure code rulesets (#10353 Loic Dachary)
* crushtool: add --location <id> command (Sage Weil, Loic Dachary)
* debian,rpm: move RBD udev rules to ceph-common (#10864 Ken Dreyer)
* debian: split python-ceph into python-{rbd,rados,cephfs} (Boris Ranto)
* default to libnss instead of crypto++ (Federico Gimenez)
* doc: CephFS disaster recovery guidance (John Spray)
* doc: CephFS for early adopters (John Spray)
* doc: add build-doc guidlines for Fedora and CentOS/RHEL (Nilamdyuti Goswami)
* doc: add dumpling to firefly upgrade section (#7679 John Wilkins)
* doc: ceph osd reweight vs crush weight (Laurent Guerby)
* doc: do not suggest dangerous XFS nobarrier option (Dan van der Ster)
* doc: document erasure coded pool operations (#9970 Loic Dachary)
* doc: document the LRC per-layer plugin configuration (Yuan Zhou)
* doc: enable rbd cache on openstack deployments (Sebastien Han)
* doc: erasure code doc updates (Loic Dachary)
* doc: file system osd config settings (Kevin Dalley)
* doc: fix OpenStack Glance docs (#10478 Sebastien Han)
* doc: improved installation nots on CentOS/RHEL installs (John Wilkins)
* doc: key/value store config reference (John Wilkins)
* doc: misc cleanups (Adam Spiers, Sebastien Han, Nilamdyuti Goswami, Ken Dreyer, John Wilkins)
* doc: misc improvements (Nilamdyuti Goswami, John Wilkins, Chris Holcombe)
* doc: misc updates (#9793 #9922 #10204 #10203 Travis Rhoden, Hazem, Ayari, Florian Coste, Andy Allan, Frank Yu, Baptiste Veuillez-Mainard, Yuan Zhou, Armando Segnini, Robert Jansen, Tyler Brekke, Viktor Suprun)
* doc: misc updates (Alfredo Deza, VRan Liu)
* doc: misc updates (Nilamdyuti Goswami, John Wilkins)
* doc: new man pages (Nilamdyuti Goswami)
* doc: preflight doc fixes (John Wilkins)
* doc: replace cloudfiles with swiftclient Python Swift example (Tim Freund)
* doc: update PG count guide (Gerben Meijer, Laurent Guerby, Loic Dachary)
* doc: update man pages (David Zafman)
* doc: update openstack docs for Juno (Sebastien Han)
* doc: update release descriptions (Ken Dreyer)
* doc: update sepia hardware inventory (Sandon Van Ness)
* erasure-code: add mSHEC erasure code support (Takeshi Miyamae)
* erasure-code: improved docs (#10340 Loic Dachary)
* erasure-code: set max_size to 20 (#10363 Loic Dachary)
* fix cluster logging from non-mon daemons (Sage Weil)
* init-ceph: check for systemd-run before using it (Boris Ranto)
* install-deps.sh: do not require sudo when root (Loic Dachary)
* keyvaluestore: misc fixes (Haomai Wang)
* keyvaluestore: performance improvements (Haomai Wang)
* libcephfs,ceph-fuse: add 'status' asok (John Spray)
* libcephfs,ceph-fuse: fix getting zero-length xattr (#10552 Yan, Zheng)
* libcephfs: fix dirfrag trimming (#10387 Yan, Zheng)
* libcephfs: fix mount timeout (#10041 Yan, Zheng)
* libcephfs: fix test (#10415 Yan, Zheng)
* libcephfs: fix use-afer-free on umount (#10412 Yan, Zheng)
* libcephfs: include ceph and git version in client metadata (Sage Weil)
* librados, osd: new watch/notify implementation (Sage Weil)
* librados: add blacklist_add convenience method (Jason Dillaman)
* librados: add rados_pool_get_base_tier() call (Adam Crume)
* librados: add watch_flush() operation (Sage Weil, Haomai Wang)
* librados: avoid memcpy on getxattr, read (Jianpeng Ma)
* librados: cap buffer length (Loic Dachary)
* librados: create ioctx by pool id (Jason Dillaman)
* librados: do notify completion in fast-dispatch (Sage Weil)
* librados: drop 'category' feature (Sage Weil)
* librados: expose rados_{read|write}_op_assert_version in C API (Kim Vandry)
* librados: fix infinite loop with skipped map epochs (#9986 Ding Dinghua)
* librados: fix iterator operator= bugs (#10082 David Zafman, Yehuda Sadeh)
* librados: fix leak in C_TwoContexts (Xiong Yiliang)
* librados: fix leak in watch/notify path (Sage Weil)
* librados: fix null deref when pool DNE (#9944 Sage Weil)
* librados: fix objecter races (#9617 Josh Durgin)
* librados: fix pool deletion handling (#10372 Sage Weil)
* librados: fix pool name caching (#10458 Radoslaw Zarzynski)
* librados: fix resource leak, misc bugs (#10425 Radoslaw Zarzynski)
* librados: fix some watch/notify locking (Jason Dillaman, Josh Durgin)
* librados: fix timer race from recent refactor (Sage Weil)
* librados: new fadvise API (Ma Jianpeng)
* librados: only export public API symbols (Jason Dillaman)
* librados: remove shadowed variable (Kefu Chain)
* librados: translate op flags from C APIs (Matthew Richards)
* libradosstriper: fix remove() (Dongmao Zhang)
* libradosstriper: fix shutdown hang (Dongmao Zhang)
* libradosstriper: fix stat strtoll (Dongmao Zhang)
* libradosstriper: fix trunc method (#10129 Sebastien Ponce)
* libradosstriper: fix write_full when ENOENT (#10758 Sebastien Ponce)
* libradosstriper: misc fixes (Sebastien Ponce)
* librbd: CRC protection for RBD image map (Jason Dillaman)
* librbd: add missing python docstrings (Jason Dillaman)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: add readahead (Adam Crume)
* librbd: add support for an "object map" indicating which objects exist (Jason Dillaman)
* librbd: adjust internal locking (Josh Durgin, Jason Dillaman)
* librbd: better handling of watch errors (Jason Dillaman)
* librbd: complete pending ops before closing image (#10299 Josh Durgin)
* librbd: coordinate maint operations through lock owner (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng, Jason Dillaman)
* librbd: differentiate between R/O vs R/W features (Jason Dillaman)
* librbd: don't close a closed parent in failure path (#10030 Jason Dillaman)
* librbd: enforce write ordering with a snapshot (Jason Dillaman)
* librbd: exclusive image locking (Jason Dillaman)
* librbd: fadvise API (Ma Jianpeng)
* librbd: fadvise-style hints; add misc hints for certain operations (Jianpeng Ma)
* librbd: fix and improve AIO cache invalidation (#10958 Jason Dillaman)
* librbd: fix cache tiers in list_children and snap_unprotect (Adam Crume)
* librbd: fix coverity false-positives (Jason Dillaman)
* librbd: fix diff test (#10002 Josh Durgin)
* librbd: fix list_children from invalid pool ioctxs (#10123 Jason Dillaman)
* librbd: fix locking for readahead (#10045 Jason Dillaman)
* librbd: fix memory leak (Jason Dillaman)
* librbd: fix ordering/queueing of resize operations (Jason Dillaman)
* librbd: fix performance regression in ObjectCacher (#9513 Adam Crume)
* librbd: fix snap create races (Jason Dillaman)
* librbd: fix write vs import race (#10590 Jason Dillaman)
* librbd: flush AIO operations asynchronously (#10714 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* librbd: lttng tracepoints (Adam Crume)
* librbd: make async versions of long-running maint operations (Jason Dillaman)
* librbd: misc fixes (Xinxin Shu, Jason Dillaman)
* librbd: mock tests (Jason Dillaman)
* librbd: only export public API symbols (Jason Dillaman)
* librbd: optionally blacklist clients before breaking locks (#10761 Jason Dillaman)
* librbd: prevent copyup during shrink (Jason Dillaman)
* librbd: refactor unit tests to use fixtures (Jason Dillaman)
* librbd: validate image is r/w on resize/flatten (Jason Dillaman)
* librbd: various internal locking fixes (Jason Dillaman)
* many coverity fixes (Danny Al-Gaaf)
* many many coverity cleanups (Danny Al-Gaaf)
* mds: 'flush journal' admin command (John Spray)
* mds: ENOSPC and OSDMap epoch barriers (#7317 John Spray)
* mds: a whole bunch of initial scrub infrastructure (Greg Farnum)
* mds: add cephfs-table-tool (John Spray)
* mds: asok command for fetching subtree map (John Spray)
* mds: avoid sending traceless replies in most cases (Yan, Zheng)
* mds: constify MDSCacheObjects (John Spray)
* mds: dirfrag buf fix (Yan, Zheng)
* mds: disallow most commands on inactive MDS's (Greg Farnum)
* mds: drop dentries, leases on deleted directories (#10164 Yan, Zheng)
* mds: export dir asok command (John Spray)
* mds: fix MDLog IO callback deadlock (John Spray)
* mds: fix compat_version for MClientSession (#9945 John Spray)
* mds: fix deadlock during journal probe vs purge (#10229 Yan, Zheng)
* mds: fix race trimming log segments (Yan, Zheng)
* mds: fix reply snapbl (Yan, Zheng)
* mds: fix sessionmap lifecycle bugs (Yan, Zheng)
* mds: fix stray/purge perfcounters (#10388 John Spray)
* mds: handle heartbeat_reset during shutdown (#10382 John Spray)
* mds: handle zero-size xattr (#10335 Yan, Zheng)
* mds: initialize root inode xattr version (Yan, Zheng)
* mds: introduce auth caps (John Spray)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mds: misc bugs (Greg Farnum, John Spray, Yan, Zheng, Henry Change)
* mds: refactor, improve Session storage (John Spray)
* mds: store backtrace for stray dir (Yan, Zheng)
* mds: subtree quota support (Yunchuan Wen)
* mds: verify backtrace when fetching dirfrag (#9557 Yan, Zheng)
* memstore: free space tracking (John Spray)
* misc cleanup (Danny Al-Gaaf, David Anderson)
* misc coverity fixes (Danny Al-Gaaf)
* misc coverity fixes (Danny Al-Gaaf)
* misc: various valgrind fixes and cleanups (Danny Al-Gaaf)
* mon: 'osd crush reweight-all' command (Sage Weil)
* mon: add 'ceph osd rename-bucket ...' command (Loic Dachary)
* mon: add bootstrap-rgw profile (Sage Weil)
* mon: add max pgs per osd warning (Sage Weil)
* mon: add noforward flag for some mon commands (Mykola Golub)
* mon: allow adding tiers to fs pools (#10135 John Spray)
* mon: allow full flag to be manually cleared (#9323 Sage Weil)
* mon: clean up auth list output (Loic Dachary)
* mon: delay failure injection (Joao Eduardo Luis)
* mon: disallow empty pool names (#10555 Wido den Hollander)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: do not pollute mon dir with CSV files from CRUSH check (Loic Dachary)
* mon: drop old ceph_mon_store_converter (Sage Weil)
* mon: fix 'ceph pg dump_stuck degraded' (Xinxin Shu)
* mon: fix 'mds fail' for standby MDSs (John Spray)
* mon: fix 'osd crush link' id resolution (John Spray)
* mon: fix 'profile osd' use of config-key function on mon (#10844 Joao Eduardo Luis)
* mon: fix *_ratio* units and types (Sage Weil)
* mon: fix JSON dumps to dump floats as flots and not strings (Sage Weil)
* mon: fix MDS health status from peons (#10151 John Spray)
* mon: fix caching for min_last_epoch_clean (#9987 Sage Weil)
* mon: fix clock drift time check interval (#10546 Joao Eduardo Luis)
* mon: fix compatset initalization during mkfs (Joao Eduardo Luis)
* mon: fix error output for add_data_pool (#9852 Joao Eduardo Luis)
* mon: fix feature tracking during elections (Joao Eduardo Luis)
* mon: fix formatter 'pg stat' command output (Sage Weil)
* mon: fix mds gid/rank/state parsing (John Spray)
* mon: fix misc error paths (Joao Eduardo Luis)
* mon: fix paxos off-by-one corner case (#9301 Sage Weil)
* mon: fix paxos timeouts (#10220 Joao Eduardo Luis)
* mon: fix stashed monmap encoding (#5203 Xie Rui)
* mon: fix units in store stats (Joao Eduardo Luis)
* mon: get canonical OSDMap from leader (#10422 Sage Weil)
* mon: ignore failure reports from before up_from (#10762 Dan van der Ster, Sage Weil)
* mon: implement 'fs reset' command (John Spray)
* mon: improve error handling on erasure code profile set (#10488, #11144 Loic Dachary)
* mon: improved corrupt CRUSH map detection (Joao Eduardo Luis)
* mon: include entity name in audit log for forwarded requests (#9913 Joao Eduardo Luis)
* mon: include pg_temp count in osdmap summary (Sage Weil)
* mon: log health summary to cluster log (#9440 Joao Eduardo Luis)
* mon: make 'mds fail' idempotent (John Spray)
* mon: make pg dump {sum,pgs,pgs_brief} work for format=plain (#5963 #6759 Mykola Golub)
* mon: new 'ceph pool ls [detail]' command (Sage Weil)
* mon: new pool safety flags nodelete, nopgchange, nosizechange (#9792 Mykola Golub)
* mon: new, friendly 'ceph pg ls ...' command (Xinxin Shu)
* mon: paxos: allow reads while proposing (#9321 #9322 Joao Eduardo Luis)
* mon: prevent MDS transition from STOPPING (#10791 Greg Farnum)
* mon: propose all pending work in one transaction (Sage Weil)
* mon: remove pg_temps for nonexistent pools (Joao Eduardo Luis)
* mon: require mon_allow_pool_delete option to remove pools (Sage Weil)
* mon: respect down flag when promoting standbys (John Spray)
* mon: set globalid prealloc to larger value (Sage Weil)
* mon: set {read,write}_tier on 'osd tier add-cache ...' (Jianpeng Ma)
* mon: skip zeroed osd stats in get_rule_avail (#10257 Joao Eduardo Luis)
* mon: validate min_size range (Jianpeng Ma)
* mon: wait for writeable before cross-proposing (#9794 Joao Eduardo Luis)
* mount.ceph: fix suprious error message (#10351 Yan, Zheng)
* ms: xio: fix misc bugs (Matt Benjamin, Vu Pham)
* msgr: async: bind threads to CPU cores, improved poll (Haomai Wang)
* msgr: async: many fixes, unit tests (Haomai Wang)
* msgr: async: several fixes (Haomai Wang)
* msgr: asyncmessenger: add kqueue support (#9926 Haomai Wang)
* msgr: avoid useless new/delete (Haomai Wang)
* msgr: fix RESETSESSION bug (#10080 Greg Farnum)
* msgr: fix crc configuration (Mykola Golub)
* msgr: fix delay injection bug (#9910 Sage Weil, Greg Farnum)
* msgr: misc unit tests (Haomai Wang)
* msgr: new AsymcMessenger alternative implementation (Haomai Wang)
* msgr: prefetch data when doing recv (Yehuda Sadeh)
* msgr: simple: fix rare deadlock (Greg Farnum)
* msgr: simple: retry binding to port on failure (#10029 Wido den Hollander)
* msgr: xio: XioMessenger RDMA support (Casey Bodley, Vu Pham, Matt Benjamin)
* objectstore: deprecate collection attrs (Sage Weil)
* osd, librados: fadvise-style librados hints (Jianpeng Ma)
* osd, librados: fix xattr_cmp_u64 (Dongmao Zhang)
* osd, librados: revamp PG listing API to handle namespaces (#9031 #9262 #9438 David Zafman)
* osd, mds: 'ops' as shorthand for 'dump_ops_in_flight' on asok (Sage Weil)
* osd, mon: add checksums to all OSDMaps (Sage Weil)
* osd, mon: send intiial pg create time from mon to osd (#9887 David Zafman)
* osd,mon: add 'norebalance' flag (Kefu Chai)
* osd,mon: specify OSD features explicitly in MOSDBoot (#10911 Sage Weil)
* osd: DBObjectMap: fix locking to prevent rare crash (#9891 Samuel Just)
* osd: EIO on whole-object reads when checksum is wrong (Sage Weil)
* osd: add erasure code corpus (Loic Dachary)
* osd: add fadvise flags to ObjectStore API (Jianpeng Ma)
* osd: add get_latest_osdmap asok command (#9483 #9484 Mykola Golub)
* osd: add misc tests (Loic Dachary, Danny Al-Gaaf)
* osd: add option to prioritize heartbeat network traffic (Jian Wen)
* osd: add support for the SHEC erasure-code algorithm (Takeshi Miyamae, Loic Dachary)
* osd: allow deletion of objects with watcher (#2339 Sage Weil)
* osd: allow recovery while below min_size (Samuel Just)
* osd: allow recovery with fewer than min_size OSDs (Samuel Just)
* osd: allow sparse read for Push/Pull (Haomai Wang)
* osd: allow whiteout deletion in cache pool (Sage Weil)
* osd: allow writes to degraded objects (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: avoid publishing unchanged PG stats (Sage Weil)
* osd: batch pg log trim (Xinze Chi)
* osd: cache pool: ignore min flush age when cache is full (Xinze Chi)
* osd: cache recent ObjectContexts (Dong Yuan)
* osd: cache reverse_nibbles hash value (Dong Yuan)
* osd: clean up internal ObjectStore interface (Sage Weil)
* osd: cleanup boost optionals (William Kennington)
* osd: clear cache on interval change (Samuel Just)
* osd: do no proxy reads unless target OSDs are new (#10788 Sage Weil)
* osd: do not abort deep scrub on missing hinfo (#10018 Loic Dachary)
* osd: do not update digest on inconsistent object (#10524 Samuel Just)
* osd: don't record digests for snapdirs (#10536 Samuel Just)
* osd: drop upgrade support for pre-dumpling (Sage Weil)
* osd: enable and use posix_fadvise (Sage Weil)
* osd: erasure coding: allow bench.sh to test ISA backend (Yuan Zhou)
* osd: erasure-code: encoding regression tests, corpus (#9420 Loic Dachary)
* osd: erasure-code: enforce chunk size alignment (#10211 Loic Dachary)
* osd: erasure-code: jerasure support for NEON (Loic Dachary)
* osd: erasure-code: relax cauchy w restrictions (#10325 David Zhang, Loic Dachary)
* osd: erasure-code: update gf-complete to latest upstream (Loic Dachary)
* osd: expose non-journal backends via ceph-osd CLI (Hoamai Wang)
* osd: filejournal: don't cache journal when not using direct IO (Jianpeng Ma)
* osd: fix JSON output for stray OSDs (Loic Dachary)
* osd: fix OSDCap parser on old (el6) boost::spirit (#10757 Kefu Chai)
* osd: fix OSDCap parsing on el6 (#10757 Kefu Chai)
* osd: fix ObjectStore::Transaction encoding version (#10734 Samuel Just)
* osd: fix WBTHrottle perf counters (Haomai Wang)
* osd: fix and document last_epoch_started semantics (Samuel Just)
* osd: fix auth object selection during repair (#10524 Samuel Just)
* osd: fix backfill bug (#10150 Samuel Just)
* osd: fix bug in pending digest updates (#10840 Samuel Just)
* osd: fix cancel_proxy_read_ops (Sage Weil)
* osd: fix cleanup of interrupted pg deletion (#10617 Sage Weil)
* osd: fix divergent entry handling on PG split (Samuel Just)
* osd: fix ghobject_t formatted output to include shard (#10063 Loic Dachary)
* osd: fix ioprio option (Mykola Golub)
* osd: fix ioprio options (Loic Dachary)
* osd: fix journal shutdown race (Sage Weil)
* osd: fix journal wrapping bug (#10883 David Zafman)
* osd: fix leak in SnapTrimWQ (#10421 Kefu Chai)
* osd: fix leak on shutdown (Kefu Chai)
* osd: fix memstore free space calculation (Xiaoxi Chen)
* osd: fix mixed-version peering issues (Samuel Just)
* osd: fix object age eviction (Zhiqiang Wang)
* osd: fix object atime calculation (Xinze Chi)
* osd: fix object digest update bug (#10840 Samuel Just)
* osd: fix occasional peering stalls (#10431 Sage Weil)
* osd: fix ordering issue with new transaction encoding (#10534 Dong Yuan)
* osd: fix osd peer check on scrub messages (#9555 Sage Weil)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: fix past_interval generation (#10427 #10430 David Zafman)
* osd: fix pgls filter ops (#9439 David Zafman)
* osd: fix recording of digest on scrub (Samuel Just)
* osd: fix scrub delay bug (#10693 Samuel Just)
* osd: fix scrub vs try-flush bug (#8011 Samuel Just)
* osd: fix short read handling on push (#8121 David Zafman)
* osd: fix stderr with -f or -d (Dan Mick)
* osd: fix transaction accounting (Jianpeng Ma)
* osd: fix watch reconnect race (#10441 Sage Weil)
* osd: fix watch timeout cache state update (#10784 David Zafman)
* osd: fix whiteout handling (Sage Weil)
* osd: flush snapshots from cache tier immediately (Sage Weil)
* osd: force promotion of watch/notify ops (Zhiqiang Wang)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* osd: improve idempotency detection across cache promotion/demotion (#8935 Sage Weil, Samuel Just)
* osd: include activating peers in blocked_by (#10477 Sage Weil)
* osd: jerasure and gf-complete updates from upstream (#10216 Loic Dachary)
* osd: journal: check fsync/fdatasync result (Jianpeng Ma)
* osd: journal: fix alignment checks, avoid useless memmove (Jianpeng Ma)
* osd: journal: fix hang on shutdown (#10474 David Zafman)
* osd: journal: fix header.committed_up_to (Xinze Chi)
* osd: journal: fix journal zeroing when direct IO is enabled (Xie Rui)
* osd: journal: initialize throttle (Ning Yao)
* osd: journal: misc bug fixes (#6003 David Zafman, Samuel Just)
* osd: journal: update committed_thru after replay (#6756 Samuel Just)
* osd: keyvaluestore: cleanup dead code (Ning Yao)
* osd: keyvaluestore: fix getattr semantics (Haomai Wang)
* osd: keyvaluestore: fix key ordering (#10119 Haomai Wang)
* osd: keyvaluestore_dev: optimization (Chendi Xue)
* osd: limit in-flight read requests (Jason Dillaman)
* osd: log when scrub or repair starts (Loic Dachary)
* osd: make misdirected op checks robust for EC pools (#9835 Sage Weil)
* osd: memstore: fix size limit (Xiaoxi Chen)
* osd: misc FIEMAP fixes (Ma Jianpeng)
* osd: misc cleanup (Xinze Chi, Yongyue Sun)
* osd: misc optimizations (Xinxin Shu, Zhiqiang Wang, Xinze Chi)
* osd: misc scrub fixes (#10017 Loic Dachary)
* osd: new 'activating' state between peering and active (Sage Weil)
* osd: new optimized encoding for ObjectStore::Transaction (Dong Yuan)
* osd: optimize Finisher (Xinze Chi)
* osd: optimize WBThrottle map with unordered_map (Ning Yao)
* osd: optimize filter_snapc (Ning Yao)
* osd: preserve reqids for idempotency checks for promote/demote (Sage Weil, Zhiqiang Wang, Samuel Just)
* osd: proxy read support (Zhiqiang Wang)
* osd: proxy reads during cache promote (Zhiqiang Wang)
* osd: remove dead locking code (Xinxin Shu)
* osd: remove legacy classic scrub code (Sage Weil)
* osd: remove unused fields in MOSDSubOp (Xiaoxi Chen)
* osd: removed some dead code (Xinze Chi)
* osd: replace MOSDSubOp messages with simpler, optimized MOSDRepOp (Xiaoxi Chen)
* osd: restrict scrub to certain times of day (Xinze Chi)
* osd: rocksdb: fix shutdown (Hoamai Wang)
* osd: store PG metadata in per-collection objects for better concurrency (Sage Weil)
* osd: store whole-object checksums on scrub, write_full (Sage Weil)
* osd: support for discard for journal trim (Jianpeng Ma)
* osd: use FIEMAP_FLAGS_SYNC instead of fsync (Jianpeng Ma)
* osd: verify kernel is new enough before using XFS extsize ioctl, enable by default (#9956 Sage Weil)
* pybind: fix memory leak in librados bindings (Billy Olsen)
* pyrados: add object lock support (#6114 Mehdi Abaakouk)
* pyrados: fix misnamed wait_* routings (#10104 Dan Mick)
* pyrados: misc cleanups (Kefu Chai)
* qa: add large auth ticket tests (Ilya Dryomov)
* qa: fix mds tests (#10539 John Spray)
* qa: fix osd create dup tests (#10083 Loic Dachary)
* qa: ignore duplicates in rados ls (Josh Durgin)
* qa: improve hadoop tests (Noah Watkins)
* qa: many 'make check' improvements (Loic Dachary)
* qa: misc tests (Loic Dachary, Yan, Zheng)
* qa: parallelize make check (Loic Dachary)
* qa: reorg fs quota tests (Greg Farnum)
* qa: tolerate nearly-full disk for make check (Loic Dachary)
* rados: fix put of /dev/null (Loic Dachary)
* rados: fix usage (Jianpeng Ma)
* rados: parse command-line arguments more strictly (#8983 Adam Crume)
* rados: use copy-from operation for copy, cppool (Sage Weil)
* radosgw-admin: add replicalog update command (Yehuda Sadeh)
* rbd-fuse: clean up on shutdown (Josh Durgin)
* rbd-fuse: fix memory leak (Adam Crume)
* rbd-replay-many (Adam Crume)
* rbd-replay: --anonymize flag to rbd-replay-prep (Adam Crume)
* rbd: add 'merge-diff' function (MingXin Liu, Yunchuan Wen, Li Wang)
* rbd: allow v2 striping parameters for clones and imports (Jason Dillaman)
* rbd: fix 'rbd diff' for non-existent objects (Adam Crume)
* rbd: fix buffer handling on image import (#10590 Jason Dillaman)
* rbd: fix error when striping with format 1 (Sebastien Han)
* rbd: fix export for image sizes over 2GB (Vicente Cheng)
* rbd: fix formatted output of image features (Jason Dillaman)
* rbd: leave exclusive lockin goff by default (Jason Dillaman)
* rbd: updat eman page (Ilya Dryomov)
* rbd: update init-rbdmap to fix dup mount point (Karel Striegel)
* rbd: use IO hints for import, export, and bench operations (#10462 Jason Dillaman)
* rbd: use rolling average for rbd bench-write throughput (Jason Dillaman)
* rbd_recover_tool: RBD image recovery tool (Min Chen)
* rgw: S3-style object versioning support (Yehuda Sadeh)
* rgw: add location header when object is in another region (VRan Liu)
* rgw: change multipart upload id magic (#10271 Yehuda Sadeh)
* rgw: check keystone auth for S3 POST requests (#10062 Abhishek Lekshmanan)
* rgw: check timestamp on s3 keystone auth (#10062 Abhishek Lekshmanan)
* rgw: conditional PUT on ETag (#8562 Ray Lv)
* rgw: create subuser if needed when creating user (#10103 Yehuda Sadeh)
* rgw: decode http query params correction (#10271 Yehuda Sadeh)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for civetweb (#10965 Yehuda Sadeh)
* rgw: extend replica log API (purge-all) (Yehuda Sadeh)
* rgw: fail S3 POST if keystone not configured (#10688 Valery Tschopp, Yehuda Sadeh)
* rgw: fix If-Modified-Since (VRan Liu)
* rgw: fix XML header on get ACL request (#10106 Yehuda Sadeh)
* rgw: fix bucket removal with data purge (Yehuda Sadeh)
* rgw: fix content length check (#10701 Axel Dunkel, Yehuda Sadeh)
* rgw: fix content-length update (#9576 Yehuda Sadeh)
* rgw: fix disabling of max_size quota (#9907 Dong Lei)
* rgw: fix error codes (#10334 #10329 Yehuda Sadeh)
* rgw: fix incorrect len when len is 0 (#9877 Yehuda Sadeh)
* rgw: fix object copy content type (#9478 Yehuda Sadeh)
* rgw: fix partial GET in swift (#10553 Yehuda Sadeh)
* rgw: fix replica log indexing (#8251 Yehuda Sadeh)
* rgw: fix shutdown (#10472 Yehuda Sadeh)
* rgw: fix swift metadata header name (Dmytro Iurchenko)
* rgw: fix sysvinit script when rgw_socket_path is not defined (#11159 Yehuda Sadeh, Dan Mick)
* rgw: fix user stags in get-user-info API (#9359 Ray Lv)
* rgw: include XML ns on get ACL request (#10106 Yehuda Sadeh)
* rgw: index swift keys appropriately (#10471 Yehuda Sadeh)
* rgw: make sysvinit script set ulimit -n properly (Sage Weil)
* rgw: misc fixes (#10307 Yehuda Sadeh)
* rgw: only track cleanup for objects we write (#10311 Yehuda Sadeh)
* rgw: pass civetweb configurables through (#10907 Yehuda Sadeh)
* rgw: prevent illegal bucket policy that doesn't match placement rule (Yehuda Sadeh)
* rgw: remove multipart entries from bucket index on abort (#10719 Yehuda Sadeh)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: respond with 204 to POST on containers (#10667 Yuan Zhou)
* rgw: return timestamp on GET/HEAD (#8911 Yehuda Sadeh)
* rgw: reuse fcgx connection struct (#10194 Yehuda Sadeh)
* rgw: run radosgw as apache with systemd (#10125 Loic Dachary)
* rgw: send explicit HTTP status string (Yehuda Sadeh)
* rgw: set ETag on object copy (#9479 Yehuda Sadeh)
* rgw: set length for keystone token validation request (#7796 Yehuda Sadeh, Mark Kirkwood)
* rgw: support X-Storage-Policy header for Swift storage policy compat (Yehuda Sadeh)
* rgw: support multiple host names (#7467 Yehuda Sadeh)
* rgw: swift: dump container's custom metadata (#10665 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: support Accept header for response format (#10746 Dmytro Iurchenko)
* rgw: swift: support for X-Remove-Container-Meta-{key} (#10475 Dmytro Iurchenko)
* rgw: tweak error codes (#10329 #10334 Yehuda Sadeh)
* rgw: update bucket index on attr changes, for multi-site sync (#5595 Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Yehuda Sadeh)
* rgw: use gc for multipart abort (#10445 Aaron Bassett, Yehuda Sadeh)
* rgw: use new watch/notify API (Yehuda Sadeh, Sage Weil)
* rpm: misc fixes (Key Dreyer)
* rpm: move rgw logrotate to radosgw subpackage (Ken Dreyer)
* systemd: better systemd unit files (Owen Synge)
* sysvinit: fix race in 'stop' (#10389 Loic Dachary)
* test: fix bufferlist tests (Jianpeng Ma)
* tests: ability to run unit tests under docker (Loic Dachary)
* tests: centos-6 dockerfile (#10755 Loic Dachary)
* tests: improve docker-based tests (Loic Dachary)
* tests: unit tests for shared_cache (Dong Yuan)
* udev: fix rules for CentOS7/RHEL7 (Loic Dachary)
* use clock_gettime instead of gettimeofday (Jianpeng Ma)
* vstart.sh: set up environment for s3-tests (Luis Pabon)
* vstart.sh: work with cmake (Yehuda Sadeh)






v0.93
=====

This is the first release candidate for Hammer, and includes all of
the features that will be present in the final release.  We welcome
and encourage any and all testing in non-production clusters to identify
any problems with functionality, stability, or performance before the
final Hammer release.

We suggest some caution in one area: librbd.  There is a lot of new
functionality around object maps and locking that is disabled by
default but may still affect stability for existing images.  We are
continuing to shake out those bugs so that the final Hammer release
(probably v0.94) will be rock solid.

Major features since Giant include:

* cephfs: journal scavenger repair tool (John Spray)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* doc: improved guidance for CephFS early adopters (John Spray)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng)
* librados: fadvise-style IO hints (Jianpeng Ma)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mon: new 'ceph osd df' command (Mykola Golub)
* mon: new 'ceph pg ls ...' command (Xinxin Shu)
* osd: improved performance for high-performance backends
* osd: improved recovery behavior (Samuel Just)
* osd: improved cache tier behavior with reads (Zhiqiang Wang)
* rgw: S3-compatible bucket versioning support (Yehuda Sadeh)
* rgw: large bucket index sharding (Guang Yang, Yehuda Sadeh)
* RDMA "xio" messenger support (Matt Benjamin, Vu Pham)

Upgrading
---------

* If you are upgrading from v0.92, you must stop all OSD daemons and flush their
  journals (``ceph-osd -i NNN --flush-journal``) before upgrading.  There was
  a transaction encoding bug in v0.92 that broke compatibility.  Upgrading from
  v0.91 or anything earlier is safe.

* No special restrictions when upgrading from firefly or giant.

Notable Changes
---------------

* build: CMake support (Ali Maredia, Casey Bodley, Adam Emerson, Marcus Watts, Matt Benjamin)
* ceph-disk: do not re-use partition if encryption is required (Loic Dachary)
* ceph-disk: support LUKS for encrypted partitions (Andrew Bartlett, Loic Dachary)
* ceph-fuse,libcephfs: add support for O_NOFOLLOW and O_PATH (Greg Farnum)
* ceph-fuse,libcephfs: resend requests before completing cap reconnect (#10912 Yan, Zheng)
* ceph-fuse: select kernel cache invalidation mechanism based on kernel version (Greg Farnum)
* ceph-objectstore-tool: improved import (David Zafman)
* ceph-objectstore-tool: misc improvements, fixes (#9870 #9871 David Zafman)
* ceph: add 'ceph osd df [tree]' command (#10452 Mykola Golub)
* ceph: fix 'ceph tell ...' command validation (#10439 Joao Eduardo Luis)
* ceph: improve 'ceph osd tree' output (Mykola Golub)
* cephfs-journal-tool: add recover_dentries function (#9883 John Spray)
* common: add newline to flushed json output (Sage Weil)
* common: filtering for 'perf dump' (John Spray)
* common: fix Formatter factory breakage (#10547 Loic Dachary)
* common: make json-pretty output prettier (Sage Weil)
* crush: new and improved straw2 bucket type (Sage Weil, Christina Anderson, Xiaoxi Chen)
* crush: update tries stats for indep rules (#10349 Loic Dachary)
* crush: use larger choose_tries value for erasure code rulesets (#10353 Loic Dachary)
* debian,rpm: move RBD udev rules to ceph-common (#10864 Ken Dreyer)
* debian: split python-ceph into python-{rbd,rados,cephfs} (Boris Ranto)
* doc: CephFS disaster recovery guidance (John Spray)
* doc: CephFS for early adopters (John Spray)
* doc: fix OpenStack Glance docs (#10478 Sebastien Han)
* doc: misc updates (#9793 #9922 #10204 #10203 Travis Rhoden, Hazem, Ayari, Florian Coste, Andy Allan, Frank Yu, Baptiste Veuillez-Mainard, Yuan Zhou, Armando Segnini, Robert Jansen, Tyler Brekke, Viktor Suprun)
* doc: replace cloudfiles with swiftclient Python Swift example (Tim Freund)
* erasure-code: add mSHEC erasure code support (Takeshi Miyamae)
* erasure-code: improved docs (#10340 Loic Dachary)
* erasure-code: set max_size to 20 (#10363 Loic Dachary)
* libcephfs,ceph-fuse: fix getting zero-length xattr (#10552 Yan, Zheng)
* librados: add blacklist_add convenience method (Jason Dillaman)
* librados: expose rados_{read|write}_op_assert_version in C API (Kim Vandry)
* librados: fix pool name caching (#10458 Radoslaw Zarzynski)
* librados: fix resource leak, misc bugs (#10425 Radoslaw Zarzynski)
* librados: fix some watch/notify locking (Jason Dillaman, Josh Durgin)
* libradosstriper: fix write_full when ENOENT (#10758 Sebastien Ponce)
* librbd: CRC protection for RBD image map (Jason Dillaman)
* librbd: add per-image object map for improved performance (Jason Dillaman)
* librbd: add support for an "object map" indicating which objects exist (Jason Dillaman)
* librbd: adjust internal locking (Josh Durgin, Jason Dillaman)
* librbd: better handling of watch errors (Jason Dillaman)
* librbd: coordinate maint operations through lock owner (Jason Dillaman)
* librbd: copy-on-read (Min Chen, Li Wang, Yunchuan Wen, Cheng Cheng, Jason Dillaman)
* librbd: enforce write ordering with a snapshot (Jason Dillaman)
* librbd: fadvise-style hints; add misc hints for certain operations (Jianpeng Ma)
* librbd: fix coverity false-positives (Jason Dillaman)
* librbd: fix snap create races (Jason Dillaman)
* librbd: flush AIO operations asynchronously (#10714 Jason Dillaman)
* librbd: make async versions of long-running maint operations (Jason Dillaman)
* librbd: mock tests (Jason Dillaman)
* librbd: optionally blacklist clients before breaking locks (#10761 Jason Dillaman)
* librbd: prevent copyup during shrink (Jason Dillaman)
* mds: add cephfs-table-tool (John Spray)
* mds: avoid sending traceless replies in most cases (Yan, Zheng)
* mds: export dir asok command (John Spray)
* mds: fix stray/purge perfcounters (#10388 John Spray)
* mds: handle heartbeat_reset during shutdown (#10382 John Spray)
* mds: many many snapshot-related fixes (Yan, Zheng)
* mds: refactor, improve Session storage (John Spray)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add noforward flag for some mon commands (Mykola Golub)
* mon: disallow empty pool names (#10555 Wido den Hollander)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: drop old ceph_mon_store_converter (Sage Weil)
* mon: fix 'ceph pg dump_stuck degraded' (Xinxin Shu)
* mon: fix 'profile osd' use of config-key function on mon (#10844 Joao Eduardo Luis)
* mon: fix compatset initalization during mkfs (Joao Eduardo Luis)
* mon: fix feature tracking during elections (Joao Eduardo Luis)
* mon: fix mds gid/rank/state parsing (John Spray)
* mon: ignore failure reports from before up_from (#10762 Dan van der Ster, Sage Weil)
* mon: improved corrupt CRUSH map detection (Joao Eduardo Luis)
* mon: include pg_temp count in osdmap summary (Sage Weil)
* mon: log health summary to cluster log (#9440 Joao Eduardo Luis)
* mon: make 'mds fail' idempotent (John Spray)
* mon: make pg dump {sum,pgs,pgs_brief} work for format=plain (#5963 #6759 Mykola Golub)
* mon: new pool safety flags nodelete, nopgchange, nosizechange (#9792 Mykola Golub)
* mon: new, friendly 'ceph pg ls ...' command (Xinxin Shu)
* mon: prevent MDS transition from STOPPING (#10791 Greg Farnum)
* mon: propose all pending work in one transaction (Sage Weil)
* mon: remove pg_temps for nonexistent pools (Joao Eduardo Luis)
* mon: require mon_allow_pool_delete option to remove pools (Sage Weil)
* mon: set globalid prealloc to larger value (Sage Weil)
* mon: skip zeroed osd stats in get_rule_avail (#10257 Joao Eduardo Luis)
* mon: validate min_size range (Jianpeng Ma)
* msgr: async: bind threads to CPU cores, improved poll (Haomai Wang)
* msgr: fix crc configuration (Mykola Golub)
* msgr: misc unit tests (Haomai Wang)
* msgr: xio: XioMessenger RDMA support (Casey Bodley, Vu Pham, Matt Benjamin)
* osd, librados: fadvise-style librados hints (Jianpeng Ma)
* osd, librados: fix xattr_cmp_u64 (Dongmao Zhang)
* osd,mon: add 'norebalance' flag (Kefu Chai)
* osd,mon: specify OSD features explicitly in MOSDBoot (#10911 Sage Weil)
* osd: add option to prioritize heartbeat network traffic (Jian Wen)
* osd: add support for the SHEC erasure-code algorithm (Takeshi Miyamae, Loic Dachary)
* osd: allow recovery while below min_size (Samuel Just)
* osd: allow recovery with fewer than min_size OSDs (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: allow writes to degraded objects (Samuel Just)
* osd: avoid publishing unchanged PG stats (Sage Weil)
* osd: cache recent ObjectContexts (Dong Yuan)
* osd: clear cache on interval change (Samuel Just)
* osd: do no proxy reads unless target OSDs are new (#10788 Sage Weil)
* osd: do not update digest on inconsistent object (#10524 Samuel Just)
* osd: don't record digests for snapdirs (#10536 Samuel Just)
* osd: fix OSDCap parser on old (el6) boost::spirit (#10757 Kefu Chai)
* osd: fix OSDCap parsing on el6 (#10757 Kefu Chai)
* osd: fix ObjectStore::Transaction encoding version (#10734 Samuel Just)
* osd: fix auth object selection during repair (#10524 Samuel Just)
* osd: fix bug in pending digest updates (#10840 Samuel Just)
* osd: fix cancel_proxy_read_ops (Sage Weil)
* osd: fix cleanup of interrupted pg deletion (#10617 Sage Weil)
* osd: fix journal wrapping bug (#10883 David Zafman)
* osd: fix leak in SnapTrimWQ (#10421 Kefu Chai)
* osd: fix memstore free space calculation (Xiaoxi Chen)
* osd: fix mixed-version peering issues (Samuel Just)
* osd: fix object digest update bug (#10840 Samuel Just)
* osd: fix ordering issue with new transaction encoding (#10534 Dong Yuan)
* osd: fix past_interval generation (#10427 #10430 David Zafman)
* osd: fix short read handling on push (#8121 David Zafman)
* osd: fix watch timeout cache state update (#10784 David Zafman)
* osd: force promotion of watch/notify ops (Zhiqiang Wang)
* osd: improve idempotency detection across cache promotion/demotion (#8935 Sage Weil, Samuel Just)
* osd: include activating peers in blocked_by (#10477 Sage Weil)
* osd: jerasure and gf-complete updates from upstream (#10216 Loic Dachary)
* osd: journal: check fsync/fdatasync result (Jianpeng Ma)
* osd: journal: fix hang on shutdown (#10474 David Zafman)
* osd: journal: fix header.committed_up_to (Xinze Chi)
* osd: journal: initialize throttle (Ning Yao)
* osd: journal: misc bug fixes (#6003 David Zafman, Samuel Just)
* osd: misc cleanup (Xinze Chi, Yongyue Sun)
* osd: new 'activating' state between peering and active (Sage Weil)
* osd: preserve reqids for idempotency checks for promote/demote (Sage Weil, Zhiqiang Wang, Samuel Just)
* osd: remove dead locking code (Xinxin Shu)
* osd: restrict scrub to certain times of day (Xinze Chi)
* osd: rocksdb: fix shutdown (Hoamai Wang)
* pybind: fix memory leak in librados bindings (Billy Olsen)
* qa: fix mds tests (#10539 John Spray)
* qa: ignore duplicates in rados ls (Josh Durgin)
* qa: improve hadoop tests (Noah Watkins)
* qa: reorg fs quota tests (Greg Farnum)
* rados: fix usage (Jianpeng Ma)
* radosgw-admin: add replicalog update command (Yehuda Sadeh)
* rbd-fuse: clean up on shutdown (Josh Durgin)
* rbd: add 'merge-diff' function (MingXin Liu, Yunchuan Wen, Li Wang)
* rbd: fix buffer handling on image import (#10590 Jason Dillaman)
* rbd: leave exclusive lockin goff by default (Jason Dillaman)
* rbd: update init-rbdmap to fix dup mount point (Karel Striegel)
* rbd: use IO hints for import, export, and bench operations (#10462 Jason Dillaman)
* rbd_recover_tool: RBD image recovery tool (Min Chen)
* rgw: S3-style object versioning support (Yehuda Sadeh)
* rgw: check keystone auth for S3 POST requests (#10062 Abhishek Lekshmanan)
* rgw: extend replica log API (purge-all) (Yehuda Sadeh)
* rgw: fail S3 POST if keystone not configured (#10688 Valery Tschopp, Yehuda Sadeh)
* rgw: fix XML header on get ACL request (#10106 Yehuda Sadeh)
* rgw: fix bucket removal with data purge (Yehuda Sadeh)
* rgw: fix replica log indexing (#8251 Yehuda Sadeh)
* rgw: fix swift metadata header name (Dmytro Iurchenko)
* rgw: remove multipart entries from bucket index on abort (#10719 Yehuda Sadeh)
* rgw: respond with 204 to POST on containers (#10667 Yuan Zhou)
* rgw: reuse fcgx connection struct (#10194 Yehuda Sadeh)
* rgw: support multiple host names (#7467 Yehuda Sadeh)
* rgw: swift: dump container's custom metadata (#10665 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: support Accept header for response format (#10746 Dmytro Iurchenko)
* rgw: swift: support for X-Remove-Container-Meta-{key} (#10475 Dmytro Iurchenko)
* rpm: move rgw logrotate to radosgw subpackage (Ken Dreyer)
* tests: centos-6 dockerfile (#10755 Loic Dachary)
* tests: unit tests for shared_cache (Dong Yuan)
* vstart.sh: work with cmake (Yehuda Sadeh)



v0.92
=====

This is the second-to-last chunk of new stuff before Hammer.  Big items
include additional checksums on OSD objects, proxied reads in the
cache tier, image locking in RBD, optimized OSD Transaction and
replication messages, and a big pile of RGW and MDS bug fixes.

Upgrading
---------

* The experimental 'keyvaluestore-dev' OSD backend has been renamed
  'keyvaluestore' (for simplicity) and marked as experimental.  To
  enable this untested feature and acknowledge that you understand
  that it is untested and may destroy data, you need to add the
  following to your ceph.conf::

    enable experimental unrecoverable data corrupting features = keyvaluestore

* The following librados C API function calls take a 'flags' argument whose value
  is now correctly interpreted:

     rados_write_op_operate()
     rados_aio_write_op_operate()
     rados_read_op_operate()
     rados_aio_read_op_operate()

  The flags were not correctly being translated from the librados constants to the
  internal values.  Now they are.  Any code that is passing flags to these methods
  should be audited to ensure that they are using the correct LIBRADOS_OP_FLAG_*
  constants.

* The 'rados' CLI 'copy' and 'cppool' commands now use the copy-from operation,
  which means the latest CLI cannot run these commands against pre-firefly OSDs.

* The librados watch/notify API now includes a watch_flush() operation to flush
  the async queue of notify operations.  This should be called by any watch/notify
  user prior to rados_shutdown().

Notable Changes
---------------

* add experimental features option (Sage Weil)
* build: fix 'make check' races (#10384 Loic Dachary)
* build: fix pkg names when libkeyutils is missing (Pankag Garg, Ken Dreyer)
* ceph: make 'ceph -s' show PG state counts in sorted order (Sage Weil)
* ceph: make 'ceph tell mon.* version' work (Mykola Golub)
* ceph-monstore-tool: fix/improve CLI (Joao Eduardo Luis)
* ceph: show primary-affinity in 'ceph osd tree' (Mykola Golub)
* common: add TableFormatter (Andreas Peters)
* common: check syncfs() return code (Jianpeng Ma)
* doc: do not suggest dangerous XFS nobarrier option (Dan van der Ster)
* doc: misc updates (Nilamdyuti Goswami, John Wilkins)
* install-deps.sh: do not require sudo when root (Loic Dachary)
* libcephfs: fix dirfrag trimming (#10387 Yan, Zheng)
* libcephfs: fix mount timeout (#10041 Yan, Zheng)
* libcephfs: fix test (#10415 Yan, Zheng)
* libcephfs: fix use-afer-free on umount (#10412 Yan, Zheng)
* libcephfs: include ceph and git version in client metadata (Sage Weil)
* librados: add watch_flush() operation (Sage Weil, Haomai Wang)
* librados: avoid memcpy on getxattr, read (Jianpeng Ma)
* librados: create ioctx by pool id (Jason Dillaman)
* librados: do notify completion in fast-dispatch (Sage Weil)
* librados: remove shadowed variable (Kefu Chain)
* librados: translate op flags from C APIs (Matthew Richards)
* librbd: differentiate between R/O vs R/W features (Jason Dillaman)
* librbd: exclusive image locking (Jason Dillaman)
* librbd: fix write vs import race (#10590 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* mds: asok command for fetching subtree map (John Spray)
* mds: constify MDSCacheObjects (John Spray)
* misc: various valgrind fixes and cleanups (Danny Al-Gaaf)
* mon: fix 'mds fail' for standby MDSs (John Spray)
* mon: fix stashed monmap encoding (#5203 Xie Rui)
* mon: implement 'fs reset' command (John Spray)
* mon: respect down flag when promoting standbys (John Spray)
* mount.ceph: fix suprious error message (#10351 Yan, Zheng)
* msgr: async: many fixes, unit tests (Haomai Wang)
* msgr: simple: retry binding to port on failure (#10029 Wido den Hollander)
* osd: add fadvise flags to ObjectStore API (Jianpeng Ma)
* osd: add get_latest_osdmap asok command (#9483 #9484 Mykola Golub)
* osd: EIO on whole-object reads when checksum is wrong (Sage Weil)
* osd: filejournal: don't cache journal when not using direct IO (Jianpeng Ma)
* osd: fix ioprio option (Mykola Golub)
* osd: fix scrub delay bug (#10693 Samuel Just)
* osd: fix watch reconnect race (#10441 Sage Weil)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* osd: journal: fix journal zeroing when direct IO is enabled (Xie Rui)
* osd: keyvaluestore: cleanup dead code (Ning Yao)
* osd, mds: 'ops' as shorthand for 'dump_ops_in_flight' on asok (Sage Weil)
* osd: memstore: fix size limit (Xiaoxi Chen)
* osd: misc scrub fixes (#10017 Loic Dachary)
* osd: new optimized encoding for ObjectStore::Transaction (Dong Yuan)
* osd: optimize filter_snapc (Ning Yao)
* osd: optimize WBThrottle map with unordered_map (Ning Yao)
* osd: proxy reads during cache promote (Zhiqiang Wang)
* osd: proxy read support (Zhiqiang Wang)
* osd: remove legacy classic scrub code (Sage Weil)
* osd: remove unused fields in MOSDSubOp (Xiaoxi Chen)
* osd: replace MOSDSubOp messages with simpler, optimized MOSDRepOp (Xiaoxi Chen)
* osd: store whole-object checksums on scrub, write_full (Sage Weil)
* osd: verify kernel is new enough before using XFS extsize ioctl, enable by default (#9956 Sage Weil)
* rados: use copy-from operation for copy, cppool (Sage Weil)
* rgw: change multipart upload id magic (#10271 Yehuda Sadeh)
* rgw: decode http query params correction (#10271 Yehuda Sadeh)
* rgw: fix content length check (#10701 Axel Dunkel, Yehuda Sadeh)
* rgw: fix partial GET in swift (#10553 Yehuda Sadeh)
* rgw: fix shutdown (#10472 Yehuda Sadeh)
* rgw: include XML ns on get ACL request (#10106 Yehuda Sadeh)
* rgw: misc fixes (#10307 Yehuda Sadeh)
* rgw: only track cleanup for objects we write (#10311 Yehuda Sadeh)
* rgw: tweak error codes (#10329 #10334 Yehuda Sadeh)
* rgw: use gc for multipart abort (#10445 Aaron Bassett, Yehuda Sadeh)
* sysvinit: fix race in 'stop' (#10389 Loic Dachary)
* test: fix bufferlist tests (Jianpeng Ma)
* tests: improve docker-based tests (Loic Dachary)


v0.91
=====

We are quickly approaching the Hammer feature freeze but have a few
more dev releases to go before we get there.  The headline items are
subtree-based quota support in CephFS (ceph-fuse/libcephfs client
support only for now), a rewrite of the watch/notify librados API used
by RBD and RGW, OSDMap checksums to ensure that maps are always
consistent inside the cluster, new API calls in librados and librbd
for IO hinting modeled after posix_fadvise, and improved storage of
per-PG state.

We expect two more releases before the Hammer feature freeze (v0.93).

Upgrading
---------

* The 'category' field for objects has been removed.  This was originally added
  to track PG stat summations over different categories of objects for use by
  radosgw.  It is no longer has any known users and is prone to abuse because it
  can lead to a pg_stat_t structure that is unbounded.  The librados API calls
  that accept this field now ignore it, and the OSD no longer tracks the
  per-category summations.

* The output for 'rados df' has changed.  The 'category' level has been
  eliminated, so there is now a single stat object per pool.  The structure of
  the JSON output is different, and the plaintext output has one less column.

* The 'rados create <objectname> [category]' optional category argument is no
  longer supported or recognized.

* rados.py's Rados class no longer has a __del__ method; it was causing
  problems on interpreter shutdown and use of threads.  If your code has
  Rados objects with limited lifetimes and you're concerned about locked
  resources, call Rados.shutdown() explicitly.

* There is a new version of the librados watch/notify API with vastly
  improved semantics.  Any applications using this interface are
  encouraged to migrate to the new API.  The old API calls are marked
  as deprecated and will eventually be removed.

* The librados rados_unwatch() call used to be safe to call on an
  invalid handle.  The new version has undefined behavior when passed
  a bogus value (for example, when rados_watch() returns an error and
  handle is not defined).

* The structure of the formatted 'pg stat' command is changed for the
  portion that counts states by name to avoid using the '+' character
  (which appears in state names) as part of the XML token (it is not
  legal).

Notable Changes
---------------

* asyncmsgr: misc fixes (Haomai Wang)
* buffer: add 'shareable' construct (Matt Benjamin)
* build: aarch64 build fixes (Noah Watkins, Haomai Wang)
* build: support for jemalloc (Shishir Gowda)
* ceph-disk: allow journal partition re-use (#10146 Loic Dachary, Dav van der Ster)
* ceph-disk: misc fixes (Christos Stavrakakis)
* ceph-fuse: fix kernel cache trimming (#10277 Yan, Zheng)
* ceph-objectstore-tool: many many improvements (David Zafman)
* common: support new gperftools header locations (Key Dreyer)
* crush: straw bucket weight calculation fixes (#9998 Sage Weil)
* doc: misc improvements (Nilamdyuti Goswami, John Wilkins, Chris Holcombe)
* libcephfs,ceph-fuse: add 'status' asok (John Spray)
* librados, osd: new watch/notify implementation (Sage Weil)
* librados: drop 'category' feature (Sage Weil)
* librados: fix pool deletion handling (#10372 Sage Weil)
* librados: new fadvise API (Ma Jianpeng)
* libradosstriper: fix remove() (Dongmao Zhang)
* librbd: complete pending ops before closing image (#10299 Josh Durgin)
* librbd: fadvise API (Ma Jianpeng)
* mds: ENOSPC and OSDMap epoch barriers (#7317 John Spray)
* mds: dirfrag buf fix (Yan, Zheng)
* mds: disallow most commands on inactive MDS's (Greg Farnum)
* mds: drop dentries, leases on deleted directories (#10164 Yan, Zheng)
* mds: handle zero-size xattr (#10335 Yan, Zheng)
* mds: subtree quota support (Yunchuan Wen)
* memstore: free space tracking (John Spray)
* misc cleanup (Danny Al-Gaaf, David Anderson)
* mon: 'osd crush reweight-all' command (Sage Weil)
* mon: allow full flag to be manually cleared (#9323 Sage Weil)
* mon: delay failure injection (Joao Eduardo Luis)
* mon: fix paxos timeouts (#10220 Joao Eduardo Luis)
* mon: get canonical OSDMap from leader (#10422 Sage Weil)
* msgr: fix RESETSESSION bug (#10080 Greg Farnum)
* objectstore: deprecate collection attrs (Sage Weil)
* osd, mon: add checksums to all OSDMaps (Sage Weil)
* osd: allow deletion of objects with watcher (#2339 Sage Weil)
* osd: allow sparse read for Push/Pull (Haomai Wang)
* osd: cache reverse_nibbles hash value (Dong Yuan)
* osd: drop upgrade support for pre-dumpling (Sage Weil)
* osd: enable and use posix_fadvise (Sage Weil)
* osd: erasure-code: enforce chunk size alignment (#10211 Loic Dachary)
* osd: erasure-code: jerasure support for NEON (Loic Dachary)
* osd: erasure-code: relax cauchy w restrictions (#10325 David Zhang, Loic Dachary)
* osd: erasure-code: update gf-complete to latest upstream (Loic Dachary)
* osd: fix WBTHrottle perf counters (Haomai Wang)
* osd: fix backfill bug (#10150 Samuel Just)
* osd: fix occasional peering stalls (#10431 Sage Weil)
* osd: fix scrub vs try-flush bug (#8011 Samuel Just)
* osd: fix stderr with -f or -d (Dan Mick)
* osd: misc FIEMAP fixes (Ma Jianpeng)
* osd: optimize Finisher (Xinze Chi)
* osd: store PG metadata in per-collection objects for better concurrency (Sage Weil)
* pyrados: add object lock support (#6114 Mehdi Abaakouk)
* pyrados: fix misnamed wait_* routings (#10104 Dan Mick)
* pyrados: misc cleanups (Kefu Chai)
* qa: add large auth ticket tests (Ilya Dryomov)
* qa: many 'make check' improvements (Loic Dachary)
* qa: misc tests (Loic Dachary, Yan, Zheng)
* rgw: conditional PUT on ETag (#8562 Ray Lv)
* rgw: fix error codes (#10334 #10329 Yehuda Sadeh)
* rgw: index swift keys appropriately (#10471 Yehuda Sadeh)
* rgw: prevent illegal bucket policy that doesn't match placement rule (Yehuda Sadeh)
* rgw: run radosgw as apache with systemd (#10125 Loic Dachary)
* rgw: support X-Storage-Policy header for Swift storage policy compat (Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Yehuda Sadeh)
* rpm: misc fixes (Key Dreyer)


v0.90
=====

This is the last development release before Christmas.  There are some
API cleanups for librados and librbd, and lots of bug fixes across the
board for the OSD, MDS, RGW, and CRUSH.  The OSD also gets support for
discard (potentially helpful on SSDs, although it is off by default), and there
are several improvements to ceph-disk.

The next two development releases will be getting a slew of new
functionality for hammer.  Stay tuned!

Upgrading
---------

* Previously, the formatted output of 'ceph pg stat -f ...' was a full
  pg dump that included all metadata about all PGs in the system.  It
  is now a concise summary of high-level PG stats, just like the
  unformatted 'ceph pg stat' command.

* All JSON dumps of floating point values were incorrecting surrounding the
  value with quotes.  These quotes have been removed.  Any consumer of structured
  JSON output that was consuming the floating point values was previously having
  to interpret the quoted string and will most likely need to be fixed to take
  the unquoted number.

Notable Changes
---------------

* arch: fix NEON feaeture detection (#10185 Loic Dachary)
* build: adjust build deps for yasm, virtualenv (Jianpeng Ma)
* build: improve build dependency tooling (Loic Dachary)
* ceph-disk: call partx/partprobe consistency (#9721 Loic Dachary)
* ceph-disk: fix dmcrypt key permissions (Loic Dachary)
* ceph-disk: fix umount race condition (#10096 Blaine Gardner)
* ceph-disk: init=none option (Loic Dachary)
* ceph-monstore-tool: fix shutdown (#10093 Loic Dachary)
* ceph-objectstore-tool: fix import (#10090 David Zafman)
* ceph-objectstore-tool: many improvements and tests (David Zafman)
* ceph.spec: package rbd-replay-prep (Ken Dreyer)
* common: add 'perf reset ...' admin command (Jianpeng Ma)
* common: do not unlock rwlock on destruction (Federico Simoncelli)
* common: fix block device discard check (#10296 Sage Weil)
* common: remove broken CEPH_LOCKDEP optoin (Kefu Chai)
* crush: fix tree bucket behavior (Rongze Zhu)
* doc: add build-doc guidlines for Fedora and CentOS/RHEL (Nilamdyuti Goswami)
* doc: enable rbd cache on openstack deployments (Sebastien Han)
* doc: improved installation nots on CentOS/RHEL installs (John Wilkins)
* doc: misc cleanups (Adam Spiers, Sebastien Han, Nilamdyuti Goswami, Ken Dreyer, John Wilkins)
* doc: new man pages (Nilamdyuti Goswami)
* doc: update release descriptions (Ken Dreyer)
* doc: update sepia hardware inventory (Sandon Van Ness)
* librados: only export public API symbols (Jason Dillaman)
* libradosstriper: fix stat strtoll (Dongmao Zhang)
* libradosstriper: fix trunc method (#10129 Sebastien Ponce)
* librbd: fix list_children from invalid pool ioctxs (#10123 Jason Dillaman)
* librbd: only export public API symbols (Jason Dillaman)
* many coverity fixes (Danny Al-Gaaf)
* mds: 'flush journal' admin command (John Spray)
* mds: fix MDLog IO callback deadlock (John Spray)
* mds: fix deadlock during journal probe vs purge (#10229 Yan, Zheng)
* mds: fix race trimming log segments (Yan, Zheng)
* mds: store backtrace for stray dir (Yan, Zheng)
* mds: verify backtrace when fetching dirfrag (#9557 Yan, Zheng)
* mon: add max pgs per osd warning (Sage Weil)
* mon: fix *_ratio* units and types (Sage Weil)
* mon: fix JSON dumps to dump floats as flots and not strings (Sage Weil)
* mon: fix formatter 'pg stat' command output (Sage Weil)
* msgr: async: several fixes (Haomai Wang)
* msgr: simple: fix rare deadlock (Greg Farnum)
* osd: batch pg log trim (Xinze Chi)
* osd: clean up internal ObjectStore interface (Sage Weil)
* osd: do not abort deep scrub on missing hinfo (#10018 Loic Dachary)
* osd: fix ghobject_t formatted output to include shard (#10063 Loic Dachary)
* osd: fix osd peer check on scrub messages (#9555 Sage Weil)
* osd: fix pgls filter ops (#9439 David Zafman)
* osd: flush snapshots from cache tier immediately (Sage Weil)
* osd: keyvaluestore: fix getattr semantics (Haomai Wang)
* osd: keyvaluestore: fix key ordering (#10119 Haomai Wang)
* osd: limit in-flight read requests (Jason Dillaman)
* osd: log when scrub or repair starts (Loic Dachary)
* osd: support for discard for journal trim (Jianpeng Ma)
* qa: fix osd create dup tests (#10083 Loic Dachary)
* rgw: add location header when object is in another region (VRan Liu)
* rgw: check timestamp on s3 keystone auth (#10062 Abhishek Lekshmanan)
* rgw: make sysvinit script set ulimit -n properly (Sage Weil)
* systemd: better systemd unit files (Owen Synge)
* tests: ability to run unit tests under docker (Loic Dachary)


v0.89
=====

This is the second development release since Giant.  The big items
include the first batch of scrub patches from Greg for CephFS, a rework
in the librados object listing API to properly handle namespaces, and
a pile of bug fixes for RGW.  There are also several smaller issues
fixed up in the performance area with buffer alignment and memory
copies, osd cache tiering agent, and various CephFS fixes.

Upgrading
---------

* New ability to list all objects from all namespaces can fail or
  return incomplete results when not all OSDs have been upgraded.
  Features rados --all ls, rados cppool, rados export, rados
  cache-flush-evict-all and rados cache-try-flush-evict-all can also
  fail or return incomplete results.

Notable Changes
---------------

* buffer: add list::get_contiguous (Sage Weil)
* buffer: avoid rebuild if buffer already contiguous (Jianpeng Ma)
* ceph-disk: improved systemd support (Owen Synge)
* ceph-disk: set guid if reusing journal partition (Dan van der Ster)
* ceph-fuse, libcephfs: allow xattr caps in inject_release_failure (#9800 John Spray)
* ceph-fuse, libcephfs: fix I_COMPLETE_ORDERED checks (#9894 Yan, Zheng)
* ceph-fuse: fix dentry invalidation on 3.18+ kernels (#9997 Yan, Zheng)
* crush: fix detach_bucket (#10095 Sage Weil)
* crush: fix several bugs in adjust_item_weight (Rongze Zhu)
* doc: add dumpling to firefly upgrade section (#7679 John Wilkins)
* doc: document erasure coded pool operations (#9970 Loic Dachary)
* doc: file system osd config settings (Kevin Dalley)
* doc: key/value store config reference (John Wilkins)
* doc: update openstack docs for Juno (Sebastien Han)
* fix cluster logging from non-mon daemons (Sage Weil)
* init-ceph: check for systemd-run before using it (Boris Ranto)
* librados: fix infinite loop with skipped map epochs (#9986 Ding Dinghua)
* librados: fix iterator operator= bugs (#10082 David Zafman, Yehuda Sadeh)
* librados: fix null deref when pool DNE (#9944 Sage Weil)
* librados: fix timer race from recent refactor (Sage Weil)
* libradosstriper: fix shutdown hang (Dongmao Zhang)
* librbd: don't close a closed parent in failure path (#10030 Jason Dillaman)
* librbd: fix diff test (#10002 Josh Durgin)
* librbd: fix locking for readahead (#10045 Jason Dillaman)
* librbd: refactor unit tests to use fixtures (Jason Dillaman)
* many many coverity cleanups (Danny Al-Gaaf)
* mds: a whole bunch of initial scrub infrastructure (Greg Farnum)
* mds: fix compat_version for MClientSession (#9945 John Spray)
* mds: fix reply snapbl (Yan, Zheng)
* mon: allow adding tiers to fs pools (#10135 John Spray)
* mon: fix MDS health status from peons (#10151 John Spray)
* mon: fix caching for min_last_epoch_clean (#9987 Sage Weil)
* mon: fix error output for add_data_pool (#9852 Joao Eduardo Luis)
* mon: include entity name in audit log for forwarded requests (#9913 Joao Eduardo Luis)
* mon: paxos: allow reads while proposing (#9321 #9322 Joao Eduardo Luis)
* msgr: asyncmessenger: add kqueue support (#9926 Haomai Wang)
* osd, librados: revamp PG listing API to handle namespaces (#9031 #9262 #9438 David Zafman)
* osd, mon: send intiial pg create time from mon to osd (#9887 David Zafman)
* osd: allow whiteout deletion in cache pool (Sage Weil)
* osd: cache pool: ignore min flush age when cache is full (Xinze Chi)
* osd: erasure coding: allow bench.sh to test ISA backend (Yuan Zhou)
* osd: erasure-code: encoding regression tests, corpus (#9420 Loic Dachary)
* osd: fix journal shutdown race (Sage Weil)
* osd: fix object age eviction (Zhiqiang Wang)
* osd: fix object atime calculation (Xinze Chi)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: journal: fix alignment checks, avoid useless memmove (Jianpeng Ma)
* osd: journal: update committed_thru after replay (#6756 Samuel Just)
* osd: keyvaluestore_dev: optimization (Chendi Xue)
* osd: make misdirected op checks robust for EC pools (#9835 Sage Weil)
* osd: removed some dead code (Xinze Chi)
* qa: parallelize make check (Loic Dachary)
* qa: tolerate nearly-full disk for make check (Loic Dachary)
* rgw: create subuser if needed when creating user (#10103 Yehuda Sadeh)
* rgw: fix If-Modified-Since (VRan Liu)
* rgw: fix content-length update (#9576 Yehuda Sadeh)
* rgw: fix disabling of max_size quota (#9907 Dong Lei)
* rgw: fix incorrect len when len is 0 (#9877 Yehuda Sadeh)
* rgw: fix object copy content type (#9478 Yehuda Sadeh)
* rgw: fix user stags in get-user-info API (#9359 Ray Lv)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: return timestamp on GET/HEAD (#8911 Yehuda Sadeh)
* rgw: set ETag on object copy (#9479 Yehuda Sadeh)
* rgw: update bucket index on attr changes, for multi-site sync (#5595 Yehuda Sadeh)


v0.88
=====

This is the first development release after Giant.  The two main
features merged this round are the new AsyncMessenger (an alternative
implementation of the network layer) from Haomai Wang at UnitedStack,
and support for POSIX file locks in ceph-fuse and libcephfs from Yan,
Zheng.  There is also a big pile of smaller items that re merged while
we were stabilizing Giant, including a range of smaller performance
and bug fixes and some new tracepoints for LTTNG.

Notable Changes
---------------

* ceph-disk: Scientific Linux support (Dan van der Ster)
* ceph-disk: respect --statedir for keyring (Loic Dachary)
* ceph-fuse, libcephfs: POSIX file lock support (Yan, Zheng)
* ceph-fuse, libcephfs: fix cap flush overflow (Greg Farnum, Yan, Zheng)
* ceph-fuse, libcephfs: fix root inode xattrs (Yan, Zheng)
* ceph-fuse, libcephfs: preserve dir ordering (#9178 Yan, Zheng)
* ceph-fuse, libcephfs: trim inodes before reconnecting to MDS (Yan, Zheng)
* ceph: do not parse injectargs twice (Loic Dachary)
* ceph: make 'ceph -s' output more readable (Sage Weil)
* ceph: new 'ceph tell mds.$name_or_rank_or_gid' (John Spray)
* ceph: test robustness (Joao Eduardo Luis)
* ceph_objectstore_tool: behave with sharded flag (#9661 David Zafman)
* cephfs-journal-tool: fix journal import (#10025 John Spray)
* cephfs-journal-tool: skip up to expire_pos (#9977 John Spray)
* cleanup rados.h definitions with macros (Ilya Dryomov)
* common: shared_cache unit tests (Cheng Cheng)
* config: add $cctid meta variable (Adam Crume)
* crush: fix buffer overrun for poorly formed rules (#9492 Johnu George)
* crush: improve constness (Loic Dachary)
* crushtool: add --location <id> command (Sage Weil, Loic Dachary)
* default to libnss instead of crypto++ (Federico Gimenez)
* doc: ceph osd reweight vs crush weight (Laurent Guerby)
* doc: document the LRC per-layer plugin configuration (Yuan Zhou)
* doc: erasure code doc updates (Loic Dachary)
* doc: misc updates (Alfredo Deza, VRan Liu)
* doc: preflight doc fixes (John Wilkins)
* doc: update PG count guide (Gerben Meijer, Laurent Guerby, Loic Dachary)
* keyvaluestore: misc fixes (Haomai Wang)
* keyvaluestore: performance improvements (Haomai Wang)
* librados: add rados_pool_get_base_tier() call (Adam Crume)
* librados: cap buffer length (Loic Dachary)
* librados: fix objecter races (#9617 Josh Durgin)
* libradosstriper: misc fixes (Sebastien Ponce)
* librbd: add missing python docstrings (Jason Dillaman)
* librbd: add readahead (Adam Crume)
* librbd: fix cache tiers in list_children and snap_unprotect (Adam Crume)
* librbd: fix performance regression in ObjectCacher (#9513 Adam Crume)
* librbd: lttng tracepoints (Adam Crume)
* librbd: misc fixes (Xinxin Shu, Jason Dillaman)
* mds: fix sessionmap lifecycle bugs (Yan, Zheng)
* mds: initialize root inode xattr version (Yan, Zheng)
* mds: introduce auth caps (John Spray)
* mds: misc bugs (Greg Farnum, John Spray, Yan, Zheng, Henry Change)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add 'ceph osd rename-bucket ...' command (Loic Dachary)
* mon: clean up auth list output (Loic Dachary)
* mon: fix 'osd crush link' id resolution (John Spray)
* mon: fix misc error paths (Joao Eduardo Luis)
* mon: fix paxos off-by-one corner case (#9301 Sage Weil)
* mon: new 'ceph pool ls [detail]' command (Sage Weil)
* mon: wait for writeable before cross-proposing (#9794 Joao Eduardo Luis)
* msgr: avoid useless new/delete (Haomai Wang)
* msgr: fix delay injection bug (#9910 Sage Weil, Greg Farnum)
* msgr: new AsymcMessenger alternative implementation (Haomai Wang)
* msgr: prefetch data when doing recv (Yehuda Sadeh)
* osd: add erasure code corpus (Loic Dachary)
* osd: add misc tests (Loic Dachary, Danny Al-Gaaf)
* osd: cleanup boost optionals (William Kennington)
* osd: expose non-journal backends via ceph-osd CLI (Hoamai Wang)
* osd: fix JSON output for stray OSDs (Loic Dachary)
* osd: fix ioprio options (Loic Dachary)
* osd: fix transaction accounting (Jianpeng Ma)
* osd: misc optimizations (Xinxin Shu, Zhiqiang Wang, Xinze Chi)
* osd: use FIEMAP_FLAGS_SYNC instead of fsync (Jianpeng Ma)
* rados: fix put of /dev/null (Loic Dachary)
* rados: parse command-line arguments more strictly (#8983 Adam Crume)
* rbd-fuse: fix memory leak (Adam Crume)
* rbd-replay-many (Adam Crume)
* rbd-replay: --anonymize flag to rbd-replay-prep (Adam Crume)
* rbd: fix 'rbd diff' for non-existent objects (Adam Crume)
* rbd: fix error when striping with format 1 (Sebastien Han)
* rbd: fix export for image sizes over 2GB (Vicente Cheng)
* rbd: use rolling average for rbd bench-write throughput (Jason Dillaman)
* rgw: send explicit HTTP status string (Yehuda Sadeh)
* rgw: set length for keystone token validation request (#7796 Yehuda Sadeh, Mark Kirkwood)
* udev: fix rules for CentOS7/RHEL7 (Loic Dachary)
* use clock_gettime instead of gettimeofday (Jianpeng Ma)
* vstart.sh: set up environment for s3-tests (Luis Pabon)
