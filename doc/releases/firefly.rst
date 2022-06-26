=======
Firefly
=======

Firefly is the 6th stable release of Ceph. It is named after the
firefly squid (Watasenia scintillans).

v0.80.11 Firefly
================

This is a bugfix release for Firefly.  This Firefly 0.80.x is nearing
its planned end of life in January 2016 it may also be the last.

We recommend that all Firefly users upgrade.

For more detailed information, see :download:`the complete changelog
<../changelog/v0.80.11.txt>`.

Notable Changes
---------------

* build/ops: /etc/init.d/radosgw restart does not work correctly (`issue#11140 <http://tracker.ceph.com/issues/11140>`_, `pr#5831 <http://github.com/ceph/ceph/pull/5831>`_, Dmitry Yatsushkevich)
* build/ops: Fix -Wno-format and -Werror=format-security options clash  (`issue#13417 <http://tracker.ceph.com/issues/13417>`_, `pr#6207 <http://github.com/ceph/ceph/pull/6207>`_, Boris Ranto)
* build/ops: ceph-common needs python-argparse on older distros, but doesn't require it (`issue#12034 <http://tracker.ceph.com/issues/12034>`_, `pr#5217 <http://github.com/ceph/ceph/pull/5217>`_, Nathan Cutler)
* build/ops: ceph.spec.in running fdupes unnecessarily (`issue#12301 <http://tracker.ceph.com/issues/12301>`_, `pr#5224 <http://github.com/ceph/ceph/pull/5224>`_, Nathan Cutler)
* build/ops: ceph.spec.in: 50-rbd.rules conditional is wrong (`issue#12166 <http://tracker.ceph.com/issues/12166>`_, `pr#5225 <http://github.com/ceph/ceph/pull/5225>`_, Nathan Cutler)
* build/ops: ceph.spec.in: useless %py_requires breaks SLE11-SP3 build (`issue#12351 <http://tracker.ceph.com/issues/12351>`_, `pr#5394 <http://github.com/ceph/ceph/pull/5394>`_, Nathan Cutler)
* build/ops: fedora21 has junit, not junit4  (`issue#10728 <http://tracker.ceph.com/issues/10728>`_, `pr#6203 <http://github.com/ceph/ceph/pull/6203>`_, Ken Dreyer, Loic Dachary)
* build/ops: upstart: configuration is too generous on restarts (`issue#11798 <http://tracker.ceph.com/issues/11798>`_, `pr#5992 <http://github.com/ceph/ceph/pull/5992>`_, Sage Weil)
* common: Client admin socket leaks file descriptors (`issue#11535 <http://tracker.ceph.com/issues/11535>`_, `pr#4633 <http://github.com/ceph/ceph/pull/4633>`_, Jon Bernard)
* common: FileStore calls syncfs(2) even it is not supported (`issue#12512 <http://tracker.ceph.com/issues/12512>`_, `pr#5529 <http://github.com/ceph/ceph/pull/5529>`_, Danny Al-Gaaf, Kefu Chai, Jianpeng Ma)
* common: HeartBeat: include types (`issue#13088 <http://tracker.ceph.com/issues/13088>`_, `pr#6038 <http://github.com/ceph/ceph/pull/6038>`_, Sage Weil)
* common: Malformed JSON command output when non-ASCII strings are present  (`issue#7387 <http://tracker.ceph.com/issues/7387>`_, `pr#4635 <http://github.com/ceph/ceph/pull/4635>`_, Kefu Chai, Tim Serong)
* common: Memory leak in Mutex.cc, pthread_mutexattr_init without pthread_mutexattr_destroy (`issue#11762 <http://tracker.ceph.com/issues/11762>`_, `pr#5403 <http://github.com/ceph/ceph/pull/5403>`_, Ketor Meng)
* common: Thread:pthread_attr_destroy(thread_attr) when done with it (`issue#12570 <http://tracker.ceph.com/issues/12570>`_, `pr#6325 <http://github.com/ceph/ceph/pull/6325>`_, Piotr Dałek, Zheng Qiankun)
* common: ThreadPool add/remove work queue methods not thread safe (`issue#12662 <http://tracker.ceph.com/issues/12662>`_, `pr#5991 <http://github.com/ceph/ceph/pull/5991>`_, Jason Dillaman)
* common: buffer: critical bufferlist::zero bug (`issue#12252 <http://tracker.ceph.com/issues/12252>`_, `pr#5388 <http://github.com/ceph/ceph/pull/5388>`_, Haomai Wang)
* common: log: take mutex while opening fd (`issue#12465 <http://tracker.ceph.com/issues/12465>`_, `pr#5406 <http://github.com/ceph/ceph/pull/5406>`_, Samuel Just)
* common: recursive lock of md_config_t (0) (`issue#12614 <http://tracker.ceph.com/issues/12614>`_, `pr#5814 <http://github.com/ceph/ceph/pull/5814>`_, Josh Durgin)
* crush: take crashes due to invalid arg (`issue#11602 <http://tracker.ceph.com/issues/11602>`_, `pr#4769 <http://github.com/ceph/ceph/pull/4769>`_, Sage Weil)
* doc: backport v0.80.10 release notes to firefly (`issue#11090 <http://tracker.ceph.com/issues/11090>`_, `pr#5307 <http://github.com/ceph/ceph/pull/5307>`_, Loic Dachary, Sage Weil)
* doc: update docs to point to download.ceph.com (`issue#13162 <http://tracker.ceph.com/issues/13162>`_, `pr#5993 <http://github.com/ceph/ceph/pull/5993>`_, Alfredo Deza)
* fs: MDSMonitor: handle MDSBeacon messages properly (`issue#11590 <http://tracker.ceph.com/issues/11590>`_, `pr#5199 <http://github.com/ceph/ceph/pull/5199>`_, Kefu Chai)
* fs: client nonce collision due to unshared pid namespaces (`issue#13032 <http://tracker.ceph.com/issues/13032>`_, `pr#6087 <http://github.com/ceph/ceph/pull/6087>`_, Josh Durgin, Sage Weil)
* librbd: Objectcacher setting max object counts too low (`issue#7385 <http://tracker.ceph.com/issues/7385>`_, `pr#4639 <http://github.com/ceph/ceph/pull/4639>`_, Jason Dillaman)
* librbd: aio calls may block (`issue#11056 <http://tracker.ceph.com/issues/11056>`_, `pr#4854 <http://github.com/ceph/ceph/pull/4854>`_, Haomai Wang, Sage Weil, Jason Dillaman)
* librbd: internal.cc: 1967: FAILED assert(watchers.size() == 1) (`issue#12176 <http://tracker.ceph.com/issues/12176>`_, `pr#5171 <http://github.com/ceph/ceph/pull/5171>`_, Jason Dillaman)
* mon: Clock skew causes missing summary and confuses Calamari (`issue#11877 <http://tracker.ceph.com/issues/11877>`_, `pr#4867 <http://github.com/ceph/ceph/pull/4867>`_, Thorsten Behrens)
* mon: EC pools are not allowed as cache pools, disallow in the mon (`issue#11650 <http://tracker.ceph.com/issues/11650>`_, `pr#5389 <http://github.com/ceph/ceph/pull/5389>`_, Samuel Just)
* mon: Make it more difficult to delete pools in firefly (`issue#11800 <http://tracker.ceph.com/issues/11800>`_, `pr#4788 <http://github.com/ceph/ceph/pull/4788>`_, Sage Weil)
* mon: MonitorDBStore: get_next_key() only if prefix matches (`issue#11786 <http://tracker.ceph.com/issues/11786>`_, `pr#5360 <http://github.com/ceph/ceph/pull/5360>`_, Joao Eduardo Luis)
* mon: PaxosService: call post_refresh() instead of post_paxos_update() (`issue#11470 <http://tracker.ceph.com/issues/11470>`_, `pr#5358 <http://github.com/ceph/ceph/pull/5358>`_, Joao Eduardo Luis)
* mon: add a cache layer over MonitorDBStore (`issue#12638 <http://tracker.ceph.com/issues/12638>`_, `pr#5698 <http://github.com/ceph/ceph/pull/5698>`_, Kefu Chai)
* mon: adding exsting pool as tier with --force-nonempty clobbers removed_snaps (`issue#11493 <http://tracker.ceph.com/issues/11493>`_, `pr#5236 <http://github.com/ceph/ceph/pull/5236>`_, Sage Weil, Samuel Just)
* mon: ceph fails to compile with boost 1.58 (`issue#11576 <http://tracker.ceph.com/issues/11576>`_, `pr#5129 <http://github.com/ceph/ceph/pull/5129>`_, Kefu Chai)
* mon: does not check for IO errors on every transaction (`issue#13089 <http://tracker.ceph.com/issues/13089>`_, `pr#6091 <http://github.com/ceph/ceph/pull/6091>`_, Sage Weil)
* mon: get pools health'info have error (`issue#12402 <http://tracker.ceph.com/issues/12402>`_, `pr#5410 <http://github.com/ceph/ceph/pull/5410>`_, renhwztetecs)
* mon: increase globalid default for firefly (`issue#13255 <http://tracker.ceph.com/issues/13255>`_, `pr#6010 <http://github.com/ceph/ceph/pull/6010>`_, Sage Weil)
* mon: pgmonitor: wrong at/near target max“ reporting (`issue#12401 <http://tracker.ceph.com/issues/12401>`_, `pr#5409 <http://github.com/ceph/ceph/pull/5409>`_, huangjun)
* mon: register_new_pgs() should check ruleno instead of its index (`issue#12210 <http://tracker.ceph.com/issues/12210>`_, `pr#5404 <http://github.com/ceph/ceph/pull/5404>`_, Xinze Chi)
* mon: scrub error (osdmap encoding mismatch?) upgrading from 0.80 to ~0.80.2 (`issue#8815 <http://tracker.ceph.com/issues/8815>`_, `issue#8674 <http://tracker.ceph.com/issues/8674>`_, `issue#9064 <http://tracker.ceph.com/issues/9064>`_, `pr#5200 <http://github.com/ceph/ceph/pull/5200>`_, Sage Weil, Zhiqiang Wang, Samuel Just)
* mon: the output is wrong when runing ceph osd reweight (`issue#12251 <http://tracker.ceph.com/issues/12251>`_, `pr#5408 <http://github.com/ceph/ceph/pull/5408>`_, Joao Eduardo Luis)
* objecter: can get stuck in redirect loop if osdmap epoch == last_force_op_resend (`issue#11026 <http://tracker.ceph.com/issues/11026>`_, `pr#4597 <http://github.com/ceph/ceph/pull/4597>`_, Jianpeng Ma, Sage Weil)
* objecter: pg listing can deadlock when throttling is in use (`issue#9008 <http://tracker.ceph.com/issues/9008>`_, `pr#5043 <http://github.com/ceph/ceph/pull/5043>`_, Guang Yang)
* objecter: resend linger ops on split (`issue#9806 <http://tracker.ceph.com/issues/9806>`_, `pr#5062 <http://github.com/ceph/ceph/pull/5062>`_, Josh Durgin, Samuel Just)
* osd: Cleanup boost optionals for boost 1.56 (`issue#9983 <http://tracker.ceph.com/issues/9983>`_, `pr#5039 <http://github.com/ceph/ceph/pull/5039>`_, William A. Kennington III)
* osd: LibRadosTwoPools[EC]PP.PromoteSnap failure (`issue#10052 <http://tracker.ceph.com/issues/10052>`_, `pr#5050 <http://github.com/ceph/ceph/pull/5050>`_, Sage Weil)
* osd: Mutex Assert from PipeConnection::try_get_pipe (`issue#12437 <http://tracker.ceph.com/issues/12437>`_, `pr#5815 <http://github.com/ceph/ceph/pull/5815>`_, David Zafman)
* osd: PG stuck with remapped (`issue#9614 <http://tracker.ceph.com/issues/9614>`_, `pr#5044 <http://github.com/ceph/ceph/pull/5044>`_, Guang Yang)
* osd: PG::handle_advance_map: on_pool_change after handling the map change (`issue#12809 <http://tracker.ceph.com/issues/12809>`_, `pr#5988 <http://github.com/ceph/ceph/pull/5988>`_, Samuel Just)
* osd: PGLog: split divergent priors as well (`issue#11069 <http://tracker.ceph.com/issues/11069>`_, `pr#4631 <http://github.com/ceph/ceph/pull/4631>`_, Samuel Just)
* osd: PGLog::proc_replica_log: correctly handle case where entries between olog.head and log.tail were split out (`issue#11358 <http://tracker.ceph.com/issues/11358>`_, `pr#5287 <http://github.com/ceph/ceph/pull/5287>`_, Samuel Just)
* osd: WBThrottle::clear_object: signal on cond when we reduce throttle values (`issue#12223 <http://tracker.ceph.com/issues/12223>`_, `pr#5822 <http://github.com/ceph/ceph/pull/5822>`_, Samuel Just)
* osd: cache full mode still skips young objects (`issue#10006 <http://tracker.ceph.com/issues/10006>`_, `pr#5051 <http://github.com/ceph/ceph/pull/5051>`_, Xinze Chi, Zhiqiang Wang)
* osd: crash creating/deleting pools (`issue#12429 <http://tracker.ceph.com/issues/12429>`_, `pr#5526 <http://github.com/ceph/ceph/pull/5526>`_, John Spray)
* osd: explicitly specify OSD features in MOSDBoot (`issue#10911 <http://tracker.ceph.com/issues/10911>`_, `pr#4960 <http://github.com/ceph/ceph/pull/4960>`_, Sage Weil)
* osd: is_new_interval() fixes (`issue#11771 <http://tracker.ceph.com/issues/11771>`_, `issue#10399 <http://tracker.ceph.com/issues/10399>`_, `pr#5726 <http://github.com/ceph/ceph/pull/5726>`_, Samuel Just, Jason Dillaman)
* osd: make the all osd/filestore thread pool suicide timeouts separately configurable (`issue#11439 <http://tracker.ceph.com/issues/11439>`_, `pr#5823 <http://github.com/ceph/ceph/pull/5823>`_, Samuel Just)
* osd: object creation by write cannot use an offset on an erasure coded pool (`issue#11507 <http://tracker.ceph.com/issues/11507>`_, `pr#4632 <http://github.com/ceph/ceph/pull/4632>`_, Jianpeng Ma, Loic Dachary)
* osd: os/FileJournal: Fix journal write fail, align for direct io (`issue#12943 <http://tracker.ceph.com/issues/12943>`_, `pr#5619 <http://github.com/ceph/ceph/pull/5619>`_, Xie Rui)
* osd: osd/PGLog.cc: 732: FAILED assert(log.log.size() == log_keys_debug.size()) (`issue#12652 <http://tracker.ceph.com/issues/12652>`_, `pr#5820 <http://github.com/ceph/ceph/pull/5820>`_, Sage Weil)
* osd: read on chunk-aligned xattr not handled (`issue#12309 <http://tracker.ceph.com/issues/12309>`_, `pr#5235 <http://github.com/ceph/ceph/pull/5235>`_, Sage Weil)
* rgw: Change variable length array of std::strings (not legal in C++) to std::vector<std::string> (`issue#12467 <http://tracker.ceph.com/issues/12467>`_, `pr#4583 <http://github.com/ceph/ceph/pull/4583>`_, Daniel J. Hofmann)
* rgw: Civetweb RGW appears to report full size of object as downloaded when only partially downloaded (`issue#11851 <http://tracker.ceph.com/issues/11851>`_, `pr#5234 <http://github.com/ceph/ceph/pull/5234>`_, Yehuda Sadeh)
* rgw: Keystone PKI token expiration is not enforced (`issue#11367 <http://tracker.ceph.com/issues/11367>`_, `pr#4765 <http://github.com/ceph/ceph/pull/4765>`_, Anton Aksola)
* rgw: Object copy bug (`issue#11639 <http://tracker.ceph.com/issues/11639>`_, `pr#4762 <http://github.com/ceph/ceph/pull/4762>`_, Javier M. Mellid)
* rgw: RGW returns requested bucket name raw in "Bucket" response header (`issue#11860 <http://tracker.ceph.com/issues/11860>`_, `issue#12537 <http://tracker.ceph.com/issues/12537>`_, `pr#5730 <http://github.com/ceph/ceph/pull/5730>`_, Yehuda Sadeh, Wido den Hollander)
* rgw: Swift API: response for PUT on /container does not contain the mandatory Content-Length header when FCGI is used (`issue#11036 <http://tracker.ceph.com/issues/11036>`_, `pr#5170 <http://github.com/ceph/ceph/pull/5170>`_, Radoslaw Zarzynski)
* rgw: content length parsing calls strtol() instead of strtoll() (`issue#10701 <http://tracker.ceph.com/issues/10701>`_, `pr#5997 <http://github.com/ceph/ceph/pull/5997>`_, Yehuda Sadeh)
* rgw: delete bucket does not remove .bucket.meta file (`issue#11149 <http://tracker.ceph.com/issues/11149>`_, `pr#4641 <http://github.com/ceph/ceph/pull/4641>`_, Orit Wasserman)
* rgw: doesn't return 'x-timestamp' in header which is used by 'View Details' of OpenStack (`issue#8911 <http://tracker.ceph.com/issues/8911>`_, `pr#4584 <http://github.com/ceph/ceph/pull/4584>`_, Yehuda Sadeh)
* rgw: init some manifest fields when handling explicit objs (`issue#11455 <http://tracker.ceph.com/issues/11455>`_, `pr#5729 <http://github.com/ceph/ceph/pull/5729>`_, Yehuda Sadeh)
* rgw: logfile does not get chowned properly (`issue#12073 <http://tracker.ceph.com/issues/12073>`_, `pr#5233 <http://github.com/ceph/ceph/pull/5233>`_, Thorsten Behrens)
* rgw: logrotate.conf calls service with wrong init script name (`issue#12043 <http://tracker.ceph.com/issues/12043>`_, `pr#5390 <http://github.com/ceph/ceph/pull/5390>`_, wuxingyi)
* rgw: quota not respected in POST object (`issue#11323 <http://tracker.ceph.com/issues/11323>`_, `pr#4642 <http://github.com/ceph/ceph/pull/4642>`_, Sergey Arkhipov)
* rgw: swift smoke test fails on TestAccountUTF8 (`issue#11091 <http://tracker.ceph.com/issues/11091>`_, `issue#11438 <http://tracker.ceph.com/issues/11438>`_, `issue#12939 <http://tracker.ceph.com/issues/12939>`_, `issue#12157 <http://tracker.ceph.com/issues/12157>`_, `issue#12158 <http://tracker.ceph.com/issues/12158>`_, `issue#12363 <http://tracker.ceph.com/issues/12363>`_, `pr#5532 <http://github.com/ceph/ceph/pull/5532>`_, Radoslaw Zarzynski, Orit Wasserman, Robin H. Johnson)
* rgw: use correct objv_tracker for bucket instance (`issue#11416 <http://tracker.ceph.com/issues/11416>`_, `pr#4535 <http://github.com/ceph/ceph/pull/4535>`_, Yehuda Sadeh)
* tests: ceph-fuse crash in test_client_recovery (`issue#12673 <http://tracker.ceph.com/issues/12673>`_, `pr#5813 <http://github.com/ceph/ceph/pull/5813>`_, Loic Dachary)
* tests: kernel_untar_build fails on EL7 (`issue#11758 <http://tracker.ceph.com/issues/11758>`_, `pr#6000 <http://github.com/ceph/ceph/pull/6000>`_, Greg Farnum)
* tests: qemu workunit refers to apt-mirror.front.sepia.ceph.com (`issue#13420 <http://tracker.ceph.com/issues/13420>`_, `pr#6328 <http://github.com/ceph/ceph/pull/6328>`_, Yuan Zhou, Sage Weil)
* tools:  src/ceph-disk : disk zap sgdisk invocation (`issue#11143 <http://tracker.ceph.com/issues/11143>`_, `pr#4636 <http://github.com/ceph/ceph/pull/4636>`_, Thorsten Behrens, Owen Synge)
* tools: ceph-disk: sometimes the journal symlink is not created (`issue#10146 <http://tracker.ceph.com/issues/10146>`_, `pr#5541 <http://github.com/ceph/ceph/pull/5541>`_, Dan van der Ster)
* tools: ceph-disk: support NVMe device partitions (`issue#11612 <http://tracker.ceph.com/issues/11612>`_, `pr#4771 <http://github.com/ceph/ceph/pull/4771>`_, Ilja Slepnev)
* tools: ceph-post-file fails on rhel7 (`issue#11836 <http://tracker.ceph.com/issues/11836>`_, `pr#5037 <http://github.com/ceph/ceph/pull/5037>`_, Joseph McDonald, Sage Weil)
* tools: ceph_argparse_flag has no regular 3rd parameter (`issue#11543 <http://tracker.ceph.com/issues/11543>`_, `pr#4582 <http://github.com/ceph/ceph/pull/4582>`_, Thorsten Behrens)
* tools: use a new disk as journal disk,ceph-disk prepare fail (`issue#10983 <http://tracker.ceph.com/issues/10983>`_, `pr#4630 <http://github.com/ceph/ceph/pull/4630>`_, Loic Dachary)


v0.80.10 Firefly
================

This is a bugfix release for Firefly.

We recommend that all Firefly users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.10.txt>`.

Notable Changes
---------------

* build/ops: ceph.spec.in: package mkcephfs on EL6 (`issue#11955 <http://tracker.ceph.com/issues/11955>`_, `pr#4924 <http://github.com/ceph/ceph/pull/4924>`_, Ken Dreyer)
* build/ops: debian: ceph-test and rest-bench debug packages should require their respective binary packages (`issue#11673 <http://tracker.ceph.com/issues/11673>`_, `pr#4766 <http://github.com/ceph/ceph/pull/4766>`_, Ken Dreyer)
* build/ops: run RGW as root (`issue#11453 <http://tracker.ceph.com/issues/11453>`_, `pr#4638 <http://github.com/ceph/ceph/pull/4638>`_, Ken Dreyer)
* common: messages/MWatchNotify: include an error code in the message (`issue#9193 <http://tracker.ceph.com/issues/9193>`_, `pr#3944 <http://github.com/ceph/ceph/pull/3944>`_, Sage Weil)
* common: Rados.shutdown() dies with Illegal instruction (core dumped) (`issue#10153 <http://tracker.ceph.com/issues/10153>`_, `pr#3963 <http://github.com/ceph/ceph/pull/3963>`_, Federico Simoncelli)
* common: SimpleMessenger: allow RESETSESSION whenever we forget an endpoint (`issue#10080 <http://tracker.ceph.com/issues/10080>`_, `pr#3915 <http://github.com/ceph/ceph/pull/3915>`_, Greg Farnum)
* common: WorkQueue: make wait timeout on empty queue configurable (`issue#10817 <http://tracker.ceph.com/issues/10817>`_, `pr#3941 <http://github.com/ceph/ceph/pull/3941>`_, Samuel Just)
* crush: set_choose_tries = 100 for erasure code rulesets (`issue#10353 <http://tracker.ceph.com/issues/10353>`_, `pr#3824 <http://github.com/ceph/ceph/pull/3824>`_, Loic Dachary)
* doc: backport ceph-disk man page to Firefly (`issue#10724 <http://tracker.ceph.com/issues/10724>`_, `pr#3936 <http://github.com/ceph/ceph/pull/3936>`_, Nilamdyuti Goswami)
* doc: Fix ceph command manpage to match ceph -h (`issue#10676 <http://tracker.ceph.com/issues/10676>`_, `pr#3996 <http://github.com/ceph/ceph/pull/3996>`_, David Zafman)
* fs: mount.ceph: avoid spurious error message (`issue#10351 <http://tracker.ceph.com/issues/10351>`_, `pr#3927 <http://github.com/ceph/ceph/pull/3927>`_, Yan, Zheng)
* librados: Fix memory leak in python rados bindings (`issue#10723 <http://tracker.ceph.com/issues/10723>`_, `pr#3935 <http://github.com/ceph/ceph/pull/3935>`_, Josh Durgin)
* librados: fix resources leakage in RadosClient::connect() (`issue#10425 <http://tracker.ceph.com/issues/10425>`_, `pr#3828 <http://github.com/ceph/ceph/pull/3828>`_, Radoslaw Zarzynski)
* librados: Translate operation flags from C APIs (`issue#10497 <http://tracker.ceph.com/issues/10497>`_, `pr#3930 <http://github.com/ceph/ceph/pull/3930>`_, Matt Richards)
* librbd: acquire cache_lock before refreshing parent (`issue#5488 <http://tracker.ceph.com/issues/5488>`_, `pr#4206 <http://github.com/ceph/ceph/pull/4206>`_, Jason Dillaman)
* librbd: snap_remove should ignore -ENOENT errors (`issue#11113 <http://tracker.ceph.com/issues/11113>`_, `pr#4245 <http://github.com/ceph/ceph/pull/4245>`_, Jason Dillaman)
* mds: fix assertion caused by system clock backwards (`issue#11053 <http://tracker.ceph.com/issues/11053>`_, `pr#3970 <http://github.com/ceph/ceph/pull/3970>`_, Yan, Zheng)
* mon: ignore osd failures from before up_from (`issue#10762 <http://tracker.ceph.com/issues/10762>`_, `pr#3937 <http://github.com/ceph/ceph/pull/3937>`_, Sage Weil)
* mon: MonCap: take EntityName instead when expanding profiles (`issue#10844 <http://tracker.ceph.com/issues/10844>`_, `pr#3942 <http://github.com/ceph/ceph/pull/3942>`_, Joao Eduardo Luis)
* mon: Monitor: fix timecheck rounds period (`issue#10546 <http://tracker.ceph.com/issues/10546>`_, `pr#3932 <http://github.com/ceph/ceph/pull/3932>`_, Joao Eduardo Luis)
* mon: OSDMonitor: do not trust small values in osd epoch cache (`issue#10787 <http://tracker.ceph.com/issues/10787>`_, `pr#3823 <http://github.com/ceph/ceph/pull/3823>`_, Sage Weil)
* mon: OSDMonitor: fallback to json-pretty in case of invalid formatter (`issue#9538 <http://tracker.ceph.com/issues/9538>`_, `pr#4475 <http://github.com/ceph/ceph/pull/4475>`_, Loic Dachary)
* mon: PGMonitor: several stats output error fixes (`issue#10257 <http://tracker.ceph.com/issues/10257>`_, `pr#3826 <http://github.com/ceph/ceph/pull/3826>`_, Joao Eduardo Luis)
* objecter: fix map skipping (`issue#9986 <http://tracker.ceph.com/issues/9986>`_, `pr#3952 <http://github.com/ceph/ceph/pull/3952>`_, Ding Dinghua)
* osd: cache tiering: fix the atime logic of the eviction (`issue#9915 <http://tracker.ceph.com/issues/9915>`_, `pr#3949 <http://github.com/ceph/ceph/pull/3949>`_, Zhiqiang Wang)
* osd: cancel_pull: requeue waiters (`issue#11244 <http://tracker.ceph.com/issues/11244>`_, `pr#4415 <http://github.com/ceph/ceph/pull/4415>`_, Samuel Just)
* osd: check that source OSD is valid for MOSDRepScrub (`issue#9555 <http://tracker.ceph.com/issues/9555>`_, `pr#3947 <http://github.com/ceph/ceph/pull/3947>`_, Sage Weil)
* osd: DBObjectMap: lock header_lock on sync() (`issue#9891 <http://tracker.ceph.com/issues/9891>`_, `pr#3948 <http://github.com/ceph/ceph/pull/3948>`_, Samuel Just)
* osd: do not ignore deleted pgs on startup (`issue#10617 <http://tracker.ceph.com/issues/10617>`_, `pr#3933 <http://github.com/ceph/ceph/pull/3933>`_, Sage Weil)
* osd: ENOENT on clone (`issue#11199 <http://tracker.ceph.com/issues/11199>`_, `pr#4385 <http://github.com/ceph/ceph/pull/4385>`_, Samuel Just)
* osd: erasure-code-profile set races with erasure-code-profile rm (`issue#11144 <http://tracker.ceph.com/issues/11144>`_, `pr#4383 <http://github.com/ceph/ceph/pull/4383>`_, Loic Dachary)
* osd: FAILED assert(soid < scrubber.start || soid >= scrubber.end) (`issue#11156 <http://tracker.ceph.com/issues/11156>`_, `pr#4185 <http://github.com/ceph/ceph/pull/4185>`_, Samuel Just)
* osd: FileJournal: fix journalq population in do_read_entry() (`issue#6003 <http://tracker.ceph.com/issues/6003>`_, `pr#3960 <http://github.com/ceph/ceph/pull/3960>`_, Samuel Just)
* osd: fix negative degraded objects during backfilling (`issue#7737 <http://tracker.ceph.com/issues/7737>`_, `pr#4021 <http://github.com/ceph/ceph/pull/4021>`_, Guang Yang)
* osd: get the currently atime of the object in cache pool for eviction (`issue#9985 <http://tracker.ceph.com/issues/9985>`_, `pr#3950 <http://github.com/ceph/ceph/pull/3950>`_, Sage Weil)
* osd: load_pgs: we need to handle the case where an upgrade from earlier versions which ignored non-existent pgs resurrects a pg with a prehistoric osdmap (`issue#11429 <http://tracker.ceph.com/issues/11429>`_, `pr#4556 <http://github.com/ceph/ceph/pull/4556>`_, Samuel Just)
* osd: ObjectStore: Don't use largest_data_off to calc data_align. (`issue#10014 <http://tracker.ceph.com/issues/10014>`_, `pr#3954 <http://github.com/ceph/ceph/pull/3954>`_, Jianpeng Ma)
* osd: osd_types: op_queue_age_hist and fs_perf_stat should be in osd_stat_t::o... (`issue#10259 <http://tracker.ceph.com/issues/10259>`_, `pr#3827 <http://github.com/ceph/ceph/pull/3827>`_, Samuel Just)
* osd: PG::actingset should be used when checking the number of acting OSDs for... (`issue#11454 <http://tracker.ceph.com/issues/11454>`_, `pr#4453 <http://github.com/ceph/ceph/pull/4453>`_, Guang Yang)
* osd: PG::all_unfound_are_queried_or_lost for non-existent osds (`issue#10976 <http://tracker.ceph.com/issues/10976>`_, `pr#4416 <http://github.com/ceph/ceph/pull/4416>`_, Mykola Golub)
* osd: PG: always clear_primary_state (`issue#10059 <http://tracker.ceph.com/issues/10059>`_, `pr#3955 <http://github.com/ceph/ceph/pull/3955>`_, Samuel Just)
* osd: PGLog.h: 279: FAILED assert(log.log.size() == log_keys_debug.size()) (`issue#10718 <http://tracker.ceph.com/issues/10718>`_, `pr#4382 <http://github.com/ceph/ceph/pull/4382>`_, Samuel Just)
* osd: PGLog: include rollback_info_trimmed_to in (read|write)_log (`issue#10157 <http://tracker.ceph.com/issues/10157>`_, `pr#3964 <http://github.com/ceph/ceph/pull/3964>`_, Samuel Just)
* osd: pg stuck stale after create with activation delay (`issue#11197 <http://tracker.ceph.com/issues/11197>`_, `pr#4384 <http://github.com/ceph/ceph/pull/4384>`_, Samuel Just)
* osd: ReplicatedPG: fail a non-blocking flush if the object is being scrubbed (`issue#8011 <http://tracker.ceph.com/issues/8011>`_, `pr#3943 <http://github.com/ceph/ceph/pull/3943>`_, Samuel Just)
* osd: ReplicatedPG::on_change: clean up callbacks_for_degraded_object (`issue#8753 <http://tracker.ceph.com/issues/8753>`_, `pr#3940 <http://github.com/ceph/ceph/pull/3940>`_, Samuel Just)
* osd: ReplicatedPG::scan_range: an object can disappear between the list and t... (`issue#10150 <http://tracker.ceph.com/issues/10150>`_, `pr#3962 <http://github.com/ceph/ceph/pull/3962>`_, Samuel Just)
* osd: requeue blocked op before flush it was blocked on (`issue#10512 <http://tracker.ceph.com/issues/10512>`_, `pr#3931 <http://github.com/ceph/ceph/pull/3931>`_, Sage Weil)
* rgw: check for timestamp for s3 keystone auth (`issue#10062 <http://tracker.ceph.com/issues/10062>`_, `pr#3958 <http://github.com/ceph/ceph/pull/3958>`_, Abhishek Lekshmanan)
* rgw: civetweb should use unique request id (`issue#11720 <http://tracker.ceph.com/issues/11720>`_, `pr#4780 <http://github.com/ceph/ceph/pull/4780>`_, Orit Wasserman)
* rgw: don't allow negative / invalid content length (`issue#11890 <http://tracker.ceph.com/issues/11890>`_, `pr#4829 <http://github.com/ceph/ceph/pull/4829>`_, Yehuda Sadeh)
* rgw: fail s3 POST auth if keystone not configured (`issue#10698 <http://tracker.ceph.com/issues/10698>`_, `pr#3966 <http://github.com/ceph/ceph/pull/3966>`_, Yehuda Sadeh)
* rgw: flush xml header on get acl request (`issue#10106 <http://tracker.ceph.com/issues/10106>`_, `pr#3961 <http://github.com/ceph/ceph/pull/3961>`_, Yehuda Sadeh)
* rgw: generate new tag for object when setting object attrs (`issue#11256 <http://tracker.ceph.com/issues/11256>`_, `pr#4571 <http://github.com/ceph/ceph/pull/4571>`_, Yehuda Sadeh)
* rgw: generate the "Date" HTTP header for civetweb. (`issue#11871,11891 <http://tracker.ceph.com/issues/11871,11891>`_, `pr#4851 <http://github.com/ceph/ceph/pull/4851>`_, Radoslaw Zarzynski)
* rgw: keystone token cache does not work correctly (`issue#11125 <http://tracker.ceph.com/issues/11125>`_, `pr#4414 <http://github.com/ceph/ceph/pull/4414>`_, Yehuda Sadeh)
* rgw: merge manifests correctly when there's prefix override (`issue#11622 <http://tracker.ceph.com/issues/11622>`_, `pr#4697 <http://github.com/ceph/ceph/pull/4697>`_, Yehuda Sadeh)
* rgw: send appropriate op to cancel bucket index pending operation (`issue#10770 <http://tracker.ceph.com/issues/10770>`_, `pr#3938 <http://github.com/ceph/ceph/pull/3938>`_, Yehuda Sadeh)
* rgw: shouldn't need to disable rgw_socket_path if frontend is configured (`issue#11160 <http://tracker.ceph.com/issues/11160>`_, `pr#4275 <http://github.com/ceph/ceph/pull/4275>`_, Yehuda Sadeh)
* rgw: Swift API. Dump container's custom metadata. (`issue#10665 <http://tracker.ceph.com/issues/10665>`_, `pr#3934 <http://github.com/ceph/ceph/pull/3934>`_, Dmytro Iurchenko)
* rgw: Swift API. Support for X-Remove-Container-Meta-{key} header. (`issue#10475 <http://tracker.ceph.com/issues/10475>`_, `pr#3929 <http://github.com/ceph/ceph/pull/3929>`_, Dmytro Iurchenko)
* rgw: use correct objv_tracker for bucket instance (`issue#11416 <http://tracker.ceph.com/issues/11416>`_, `pr#4379 <http://github.com/ceph/ceph/pull/4379>`_, Yehuda Sadeh)
* tests: force checkout of submodules (`issue#11157 <http://tracker.ceph.com/issues/11157>`_, `pr#4079 <http://github.com/ceph/ceph/pull/4079>`_, Loic Dachary)
* tools: Backport ceph-objectstore-tool changes to firefly (`issue#12327 <http://tracker.ceph.com/issues/12327>`_, `pr#3866 <http://github.com/ceph/ceph/pull/3866>`_, David Zafman)
* tools: ceph-objectstore-tool: Output only unsupported features when incomatible (`issue#11176 <http://tracker.ceph.com/issues/11176>`_, `pr#4126 <http://github.com/ceph/ceph/pull/4126>`_, David Zafman)
* tools: ceph-objectstore-tool: Use exit status 11 for incompatible import attemp... (`issue#11139 <http://tracker.ceph.com/issues/11139>`_, `pr#4129 <http://github.com/ceph/ceph/pull/4129>`_, David Zafman)
* tools: Fix do_autogen.sh so that -L is allowed (`issue#11303 <http://tracker.ceph.com/issues/11303>`_, `pr#4247 <http://github.com/ceph/ceph/pull/4247>`_, Alfredo Deza)

v0.80.9 Firefly
===============

This is a bugfix release for firefly.  It fixes a performance
regression in librbd, an important CRUSH misbehavior (see below), and
several RGW bugs.  We have also backported support for flock/fcntl
locks to ceph-fuse and libcephfs.

We recommend that all Firefly users upgrade.

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.9.txt>`.

Adjusting CRUSH maps
--------------------

* This point release fixes several issues with CRUSH that trigger
  excessive data migration when adjusting OSD weights.  These are most
  obvious when a very small weight change (e.g., a change from 0 to
  .01) triggers a large amount of movement, but the same set of bugs
  can also lead to excessive (though less noticeable) movement in
  other cases.

  However, because the bug may already have affected your cluster,
  fixing it may trigger movement *back* to the more correct location.
  For this reason, you must manually opt-in to the fixed behavior.

  In order to set the new tunable to correct the behavior::

     ceph osd crush set-tunable straw_calc_version 1

  Note that this change will have no immediate effect.  However, from
  this point forward, any 'straw' bucket in your CRUSH map that is
  adjusted will get non-buggy internal weights, and that transition
  may trigger some rebalancing.

  You can estimate how much rebalancing will eventually be necessary
  on your cluster with::

     ceph osd getcrushmap -o /tmp/cm
     crushtool -i /tmp/cm --num-rep 3 --test --show-mappings > /tmp/a 2>&1
     crushtool -i /tmp/cm --set-straw-calc-version 1 -o /tmp/cm2
     crushtool -i /tmp/cm2 --reweight -o /tmp/cm2
     crushtool -i /tmp/cm2 --num-rep 3 --test --show-mappings > /tmp/b 2>&1
     wc -l /tmp/a                          # num total mappings
     diff -u /tmp/a /tmp/b | grep -c ^+    # num changed mappings

   Divide the number of changed lines by the total number of lines in
   /tmp/a.  We've found that most clusters are under 10%.

   You can force all of this rebalancing to happen at once with::

     ceph osd crush reweight-all

   Otherwise, it will happen at some unknown point in the future when
   CRUSH weights are next adjusted.

Notable Changes
---------------

* ceph-fuse: flock, fcntl lock support (Yan, Zheng, Greg Farnum)
* crush: fix straw bucket weight calculation, add straw_calc_version tunable (#10095 Sage Weil)
* crush: fix tree bucket (Rongzu Zhu)
* crush: fix underflow of tree weights (Loic Dachary, Sage Weil)
* crushtool: add --reweight (Sage Weil)
* librbd: complete pending operations before losing image (#10299 Jason Dillaman)
* librbd: fix read caching performance regression (#9854 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* mon: fix dump of chooseleaf_vary_r tunable (Sage Weil)
* osd: fix PG ref leak in snaptrimmer on peering (#10421 Kefu Chai)
* osd: handle no-op write with snapshot (#10262 Sage Weil)
* radosgw-admin: create subuser when creating user (#10103 Yehuda Sadeh)
* rgw: change multipart uplaod id magic (#10271 Georgio Dimitrakakis, Yehuda Sadeh)
* rgw: don't overwrite bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: enable IPv6 for embedded civetweb (#10965 Yehuda Sadeh)
* rgw: fix partial swift GET (#10553 Yehuda Sadeh)
* rgw: fix quota disable (#9907 Dong Lei)
* rgw: index swift keys appropriately (#10471 Hemant Burman, Yehuda Sadeh)
* rgw: make setattrs update bucket index (#5595 Yehuda Sadeh)
* rgw: pass civetweb configurables (#10907 Yehuda Sadeh)
* rgw: remove swift user manifest (DLO) hash calculation (#9973 Yehuda Sadeh)
* rgw: return correct len for 0-len objects (#9877 Yehuda Sadeh)
* rgw: S3 object copy content-type fix (#9478 Yehuda Sadeh)
* rgw: send ETag on S3 object copy (#9479 Yehuda Sadeh)
* rgw: send HTTP status reason explicitly in fastcgi (Yehuda Sadeh)
* rgw: set ulimit -n from sysvinit (el6) init script (#9587 Sage Weil)
* rgw: update swift subuser permission masks when authenticating (#9918 Yehuda Sadeh)
* rgw: URL decode query params correctly (#10271 Georgio Dimitrakakis, Yehuda Sadeh)
* rgw: use attrs when reading object attrs (#10307 Yehuda Sadeh)
* rgw: use \r\n for http headers (#9254 Benedikt Fraunhofer, Yehuda Sadeh)


v0.80.8 Firefly
===============

This is a long-awaited bugfix release for firefly.  It has several
important (but relatively rare) OSD peering fixes, performance issues
when snapshots are trimmed, several RGW fixes, a paxos corner case
fix, and some packaging updates.

We recommend that all users for v0.80.x firefly upgrade when it is
convenient to do so.

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.8.txt>`.

Notable Changes
---------------

* build: remove stack-execute bit from assembled code sections (#10114 Dan Mick)
* ceph-disk: fix dmcrypt key permissions (#9785 Loic Dachary)
* ceph-disk: fix keyring location (#9653 Loic Dachary)
* ceph-disk: make partition checks more robust (#9721 #9665 Loic Dachary)
* ceph: cleanly shut down librados context on shutdown (#8797 Dan Mick)
* common: add $cctid config metavariable (#6228 Adam Crume)
* crush: align rule and ruleset ids (#9675 Xiaoxi Chen)
* crush: fix negative weight bug during create_or_move_item (#9998 Pawel Sadowski)
* crush: fix potential buffer overflow in erasure rules (#9492 Johnu George)
* debian: fix python-ceph -> ceph file movement (Sage Weil)
* libcephfs,ceph-fuse: fix flush tid wraparound bug (#9869 Greg Farnum, Yan, Zheng)
* libcephfs: close fd befure umount (#10415 Yan, Zheng)
* librados: fix crash from C API when read timeout is enabled (#9582 Sage Weil)
* librados: handle reply race with pool deletion (#10372 Sage Weil)
* librbd: cap memory utilization for read requests (Jason Dillaman)
* librbd: do not close a closed parent image on failure (#10030 Jason Dillaman)
* librbd: fix diff tests (#10002 Josh Durgin)
* librbd: protect list_children from invalid pools (#10123 Jason Dillaman)
* make check improvemens (Loic Dachary)
* mds: fix ctime updates (#9514 Greg Farnum)
* mds: fix journal import tool (#10025 John Spray)
* mds: fix rare NULL deref in cap flush handler (Greg Farnum)
* mds: handle unknown lock messages (Yan, Zheng)
* mds: store backtrace for straydir (Yan, Zheng)
* mon: abort startup if disk is full (#9502 Joao Eduardo Luis)
* mon: add paxos instrumentation (Sage Weil)
* mon: fix double-free in rare OSD startup path (Sage Weil)
* mon: fix osdmap trimming (#9987 Sage Weil)
* mon: fix paxos corner cases (#9301 #9053 Sage Weil)
* osd: cancel callback on blacklisted watchers (#8315 Samuel Just)
* osd: cleanly abort set-alloc-hint operations during upgrade (#9419 David Zafman)
* osd: clear rollback PG metadata on PG deletion (#9293 Samuel Just)
* osd: do not abort deep scrub if hinfo is missing (#10018 Loic Dachary)
* osd: erasure-code regression tests (Loic Dachary)
* osd: fix distro metadata reporting for SUSE (#8654 Danny Al-Gaaf)
* osd: fix full OSD checks during backfill (#9574 Samuel Just)
* osd: fix ioprio parsing (#9677 Loic Dachary)
* osd: fix journal direct-io shutdown (#9073 Mark Kirkwood, Ma Jianpeng, Somnath Roy)
* osd: fix journal dump (Ma Jianpeng)
* osd: fix occasional stall during peering or activation (Sage Weil)
* osd: fix past_interval display bug (#9752 Loic Dachary)
* osd: fix rare crash triggered by admin socket dump_ops_in_filght (#9916 Dong Lei)
* osd: fix snap trimming performance issues (#9487 #9113 Samuel Just, Sage Weil, Dan van der Ster, Florian Haas)
* osd: fix snapdir handling on cache eviction (#8629 Sage Weil)
* osd: handle map gaps in map advance code (Sage Weil)
* osd: handle undefined CRUSH results in interval check (#9718 Samuel Just)
* osd: include shard in JSON dump of ghobject (#10063 Loic Dachary)
* osd: make backfill reservation denial handling more robust (#9626 Samuel Just)
* osd: make misdirected op checks handle EC + primary affinity (#9835 Samuel Just, Sage Weil)
* osd: mount XFS with inode64 by default (Sage Weil)
* osd: other misc bugs (#9821 #9875 Samuel Just)
* rgw: add .log to default log path (#9353 Alexandre Marangone)
* rgw: clean up fcgi request context (#10194 Yehuda Sadeh)
* rgw: convet header underscores to dashes (#9206 Yehuda Sadeh)
* rgw: copy object data if copy target is in different pool (#9039 Yehuda Sadeh)
* rgw: don't try to authenticate CORS peflight request (#8718 Robert Hubbard, Yehuda Sadeh)
* rgw: fix civetweb URL decoding (#8621 Yehuda Sadeh)
* rgw: fix hash calculation during PUT (Yehuda Sadeh)
* rgw: fix misc bugs (#9089 #9201 Yehuda Sadeh)
* rgw: fix object tail test (#9226 Sylvain Munaut, Yehuda Sadeh)
* rgw: make sysvinit script run rgw under systemd context as needed (#10125 Loic Dachary)
* rgw: separate civetweb log from rgw log (Yehuda Sadeh)
* rgw: set length for keystone token validations (#7796 Mark Kirkwood, Yehuda Sadeh)
* rgw: subuser creation fixes (#8587 Yehuda Sadeh)
* rpm: misc packaging improvements (Sandon Van Ness, Dan Mick, Erik Logthenberg, Boris Ranto)
* rpm: use standard udev rules for CentOS7/RHEL7 (#9747 Loic Dachary)


v0.80.7 Firefly
===============

This release fixes a few critical issues with v0.80.6, particularly
with clusters running mixed versions.

We recommend that all v0.80.x Firefly users upgrade to this release.

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.7.txt>`.

Notable Changes
---------------

* osd: fix invalid memory reference in log trimming (#9731 Samuel Just)
* osd: fix use-after-free in cache tiering code (#7588 Sage Weil)
* osd: remove bad backfill assertion for mixed-version clusters (#9696 Samuel Just)



v0.80.6 Firefly
===============

This is a major bugfix release for firefly, fixing a range of issues
in the OSD and monitor, particularly with cache tiering.  There are
also important fixes in librados, with the watch/notify mechanism used
by librbd, and in radosgw.

A few pieces of new functionality of been backported, including improved
'ceph df' output (view amount of writeable space per pool), support for
non-default cluster names when using sysvinit or systemd, and improved
(and fixed) support for dmcrypt.

We recommend that all v0.80.x Firefly users upgrade to this release.

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.6.txt>`.

Notable Changes
---------------

* build: fix atomic64_t on i386 (#8969 Sage Weil)
* build: fix build on alpha (Michael Cree, Dmitry Smirnov)
* build: fix build on hppa (Dmitry Smirnov)
* build: fix yasm detection on x32 arch (Sage Weil)
* ceph-disk: fix 'list' function with dmcrypt (Sage Weil)
* ceph-disk: fix dmcrypt support (Alfredo Deza)
* ceph: allow non-default cluster to be specified (#8944)
* common: fix dup log messages to mon (#9080 Sage Weil)
* global: write pid file when -f is used (systemd, upstart) (Alexandre Oliva)
* librados: fix crash when read timeout is enabled (#9362 Matthias Kiefer, Sage Weil)
* librados: fix lock leaks in error paths (#9022 Pavan Rallabhandi)
* librados: fix watch resend on PG acting set change (#9220 Samuel Just)
* librados: python: fix aio_read handling with \0 (Mohammad Salehe)
* librbd: add interface to invalidate cached data (Josh Durgin)
* librbd: fix crash when using clone of flattened image (#8845 Josh Durgin)
* librbd: fix error path cleanup on open (#8912 Josh Durgin)
* librbd: fix null pointer check (Danny Al-Gaaf)
* librbd: limit dirty object count (Haomai Wang)
* mds: fix rstats for root and mdsdir (Yan, Zheng)
* mon: add 'get' command for new cache tier pool properties (Joao Eduardo Luis)
* mon: add 'osd pool get-quota' (#8523 Joao Eduardo Luis)
* mon: add cluster fingerprint (Sage Weil)
* mon: disallow nonsensical cache-mode transitions (#8155 Joao Eduardo Luis)
* mon: fix cache tier rounding error on i386 (Sage Weil)
* mon: fix occasional memory leak (#9176 Sage Weil)
* mon: fix reported latency for 'osd perf' (#9269 Samuel Just)
* mon: include 'max avail' in 'ceph df' output (Sage Weil, Xioaxi Chen)
* mon: persistently mark pools where scrub may find incomplete clones (#8882 Sage Weil)
* mon: preload erasure plugins (Loic Dachary)
* mon: prevent cache-specific settings on non-tier pools (#8696 Joao Eduardo Luis)
* mon: reduce log spam (Aanchal Agrawal, Sage Weil)
* mon: warn when cache pools have no hit_sets enabled (Sage Weil)
* msgr: fix trivial memory leak (Sage Weil)
* osd: automatically scrub PGs with invalid stats (#8147 Sage Weil)
* osd: avoid sharing PG metadata that is not durable (Samuel Just)
* osd: cap hit_set size (#9339 Samuel Just)
* osd: create default erasure profile if needed (#8601 Loic Dachary)
* osd: dump tid as JSON int (not string)  where appropriate (Joao Eduardo Luis)
* osd: encode blacklist in deterministic order (#9211 Sage Weil)
* osd: fix behavior when cache tier has no hit_sets enabled (#8982 Sage Weil)
* osd: fix cache tier flushing of snapshots (#9054 Samuel Just)
* osd: fix cache tier op ordering when going from full to non-full (#8931 Sage Weil)
* osd: fix crash on dup recovery reservation (#8863 Sage Weil)
* osd: fix division by zero when pg_num adjusted with no OSDs (#9052 Sage Weil)
* osd: fix hint crash in experimental keyvaluestore_dev backend (Hoamai Wang)
* osd: fix leak in copyfrom cancellation (#8894 Samuel Just)
* osd: fix locking for copyfrom finish (#8889 Sage Weil)
* osd: fix long filename handling in backend (#8701 Sage Weil)
* osd: fix min_size check with backfill (#9497 Samuel Just)
* osd: fix mount/remount sync race (#9144 Sage Weil)
* osd: fix object listing + erasure code bug (Guang Yang)
* osd: fix race on reconnect to failed OSD (#8944 Greg Farnum)
* osd: fix recovery reservation deadlock (Samuel Just)
* osd: fix tiering agent arithmetic for negative values (#9082 Karan Singh)
* osd: improve shutdown order (#9218 Sage Weil)
* osd: improve subop discard logic (#9259 Samuel Just)
* osd: introduce optional sleep, io priority for scrub and snap trim (Sage Weil)
* osd: make scrub check for and remove stale erasure-coded objects (Samuel Just)
* osd: misc fixes (#9481 #9482 #9179 Sameul Just)
* osd: mix keyvaluestore_dev improvements (Haomai Wang)
* osd: only require CRUSH features for rules that are used (#8963 Sage Weil)
* osd: preload erasure plugins on startup (Loic Dachary)
* osd: prevent PGs from falling behind when consuming OSDMaps (#7576 Sage Weil)
* osd: prevent old clients from using tiered pools (#8714 Sage Weil)
* osd: set min_size on erasure pools to data chunk count (Sage Weil)
* osd: trim old erasure-coded objects more aggressively (Samuel Just)
* rados: enforce erasure code alignment (Lluis Pamies-Juarez)
* rgw: align object stripes with erasure pool alignment (#8442 Yehuda Sadeh)
* rgw: don't send error body on HEAD for civetweb (#8539 Yehuda Sadeh)
* rgw: fix crash in CORS preflight request (Yehuda Sadeh)
* rgw: fix decoding of + in URL (#8702 Brian Rak)
* rgw: fix object removal on object create (#8972 Patrycja Szabowska, Yehuda Sadeh)
* systemd: use systemd-run when starting radosgw (JuanJose Galvez)
* sysvinit: support non-default cluster name (Alfredo Deza)


v0.80.5 Firefly
===============

This release fixes a few important bugs in the radosgw and fixes
several packaging and environment issues, including OSD log rotation,
systemd environments, and daemon restarts on upgrade.

We recommend that all v0.80.x Firefly users upgrade, particularly if they
are using upstart, systemd, or radosgw.

Notable Changes
---------------

* ceph-dencoder: do not needlessly link to librgw, librados, etc. (Sage Weil)
* do not needlessly link binaries to leveldb (Sage Weil)
* mon: fix mon crash when no auth keys are present (#8851, Joao Eduardo Luis)
* osd: fix cleanup (and avoid occasional crash) during shutdown (#7981, Sage Weil)
* osd: fix log rotation under upstart (Sage Weil)
* rgw: fix multipart upload when object has irregular size (#8846, Yehuda Sadeh, Sylvain Munaut)
* rgw: improve bucket listing S3 compatibility (#8858, Yehuda Sadeh)
* rgw: improve delimited bucket listing (Yehuda Sadeh)
* rpm: do not restart daemons on upgrade (#8849, Alfredo Deza)

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.5.txt>`.

v0.80.4 Firefly
===============

This Firefly point release fixes an potential data corruption problem
when ceph-osd daemons run on top of XFS and service Firefly librbd
clients.  A recently added allocation hint that RBD utilizes triggers
an XFS bug on some kernels (Linux 3.2, and likely others) that leads
to data corruption and deep-scrub errors (and inconsistent PGs).  This
release avoids the situation by disabling the allocation hint until we
can validate which kernels are affected and/or are known to be safe to
use the hint on.

We recommend that all v0.80.x Firefly users urgently upgrade,
especially if they are using RBD.

Notable Changes
---------------

* osd: disable XFS extsize hint by default (#8830, Samuel Just)
* rgw: fix extra data pool default name (Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.4.txt>`.


v0.80.3 Firefly
===============

This is the third Firefly point release.  It includes a single fix
for a radosgw regression that was discovered in v0.80.2 right after it
was released.

We recommend that all v0.80.x Firefly users upgrade.

Notable Changes
---------------

* radosgw: fix regression in manifest decoding (#8804, Sage Weil)

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.3.txt>`.


v0.80.2 Firefly
===============

This is the second Firefly point release.  It contains a range of
important fixes, including several bugs in the OSD cache tiering, some
compatibility checks that affect upgrade situations, several radosgw
bugs, and an irritating and unnecessary feature bit check that
prevents older clients from communicating with a cluster with any
erasure coded pools.

One someone large change in this point release is that the ceph RPM
package is separated into a ceph and ceph-common package, similar to
Debian.  The ceph-common package contains just the client libraries
without any of the server-side daemons.

We recommend that all v0.80.x Firefly users skip this release and use
v0.80.3.

Notable Changes
---------------

* ceph-disk: better debug logging (Alfredo Deza)
* ceph-disk: fix preparation of OSDs with dmcrypt (#6700, Stephen F Taylor)
* ceph-disk: partprobe on prepare to fix dm-crypt (#6966, Eric Eastman)
* do not require ERASURE_CODE feature from clients (#8556, Sage Weil)
* libcephfs-java: build with older JNI headers (Greg Farnum)
* libcephfs-java: fix build with gcj-jdk (Dmitry Smirnov)
* librados: fix osd op tid for redirected ops (#7588, Samuel Just)
* librados: fix rados_pool_list buffer bounds checks (#8447, Sage Weil)
* librados: resend ops when pool overlay changes (#8305, Sage Weil)
* librbd, ceph-fuse: reduce CPU overhead for clean object check in cache (Haomai Wang)
* mon: allow deletion of cephfs pools (John Spray)
* mon: fix default pool ruleset choice (#8373, John Spray)
* mon: fix health summary for mon low disk warning (Sage Weil)
* mon: fix 'osd pool set <pool> cache_target_full_ratio' (Geoffrey Hartz)
* mon: fix quorum feature check (Greg Farnum)
* mon: fix request forwarding in mixed firefly+dumpling clusters 9#8727, Joao Eduardo Luis)
* mon: fix rule vs ruleset check in 'osd pool set ... crush_ruleset' command (John Spray)
* mon: make osd 'down' count accurate (Sage Weil)
* mon: set 'next commit' in primary-affinity reply (Ilya Dryomov)
* mon: verify CRUSH features are supported by all mons (#8738, Greg Farnum)
* msgr: fix sequence negotiation during connection reset (Guang Yang)
* osd: block scrub on blocked objects (#8011, Samuel Just)
* osd: call XFS hint ioctl less often (#8241, Ilya Dryomov)
* osd: copy xattr spill out marker on clone (Haomai Wang)
* osd: fix flush of snapped objects (#8334, Samuel Just)
* osd: fix hashindex restart of merge operation (#8332, Samuel Just)
* osd: fix osdmap subscription bug causing startup hang (Greg Farnum)
* osd: fix potential null deref (#8328, Sage Weil)
* osd: fix shutdown race (#8319, Sage Weil)
* osd: handle 'none' in CRUSH results properly during peering (#8507, Samuel Just)
* osd: set no spill out marker on new objects (Greg Farnum)
* osd: skip op ordering debug checks on tiered pools (#8380, Sage Weil)
* rados: enforce 'put' alignment (Lluis Pamies-Juarez)
* rest-api: fix for 'rx' commands (Ailing Zhang)
* rgw: calc user manifest etag and fix check (#8169, #8436, Yehuda Sadeh)
* rgw: fetch attrs on multipart completion (#8452, Yehuda Sadeh, Sylvain Munaut)
* rgw: fix buffer overflow for long instance ids (#8608, Yehuda Sadeh)
* rgw: fix entity permission check on metadata put (#8428, Yehuda Sadeh)
* rgw: fix multipart retry race (#8269, Yehuda Sadeh)
* rpm: split ceph into ceph and ceph-common RPMs (Sandon Van Ness, Dan Mick)
* sysvinit: continue startin daemons after failure doing mount (#8554, Sage Weil)

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.2.txt>`.

v0.80.1 Firefly
===============

This first Firefly point release fixes a few bugs, the most visible
being a problem that prevents scrub from completing in some cases.

Notable Changes
---------------

* osd: revert incomplete scrub fix (Samuel Just)
* rgw: fix stripe calculation for manifest objects (Yehuda Sadeh)
* rgw: improve handling, memory usage for abort reads (Yehuda Sadeh)
* rgw: send Swift user manifest HTTP header (Yehuda Sadeh)
* libcephfs, ceph-fuse: expose MDS session state via admin socket (Yan, Zheng)
* osd: add simple throttle for snap trimming (Sage Weil)
* monclient: fix possible hang from ill-timed monitor connection failure (Sage Weil)
* osd: fix trimming of past HitSets (Sage Weil)
* osd: fix whiteouts for non-writeback cache modes (Sage Weil)
* osd: prevent divide by zero in tiering agent (David Zafman)
* osd: prevent busy loop when tiering agent can do no work (David Zafman)

For more detailed information, see :download:`the complete changelog <../changelog/v0.80.1.txt>`.


v0.80 Firefly
=============

This release will form the basis for our long-term supported release
Firefly, v0.80.x.  The big new features are support for erasure coding
and cache tiering, although a broad range of other features, fixes,
and improvements have been made across the code base.  Highlights include:

* *Erasure coding*: support for a broad range of erasure codes for lower
  storage overhead and better data durability.
* *Cache tiering*: support for creating 'cache pools' that store hot,
  recently accessed objects with automatic demotion of colder data to
  a base tier.  Typically the cache pool is backed by faster storage
  devices like SSDs.
* *Primary affinity*: Ceph now has the ability to skew selection of
  OSDs as the "primary" copy, which allows the read workload to be
  cheaply skewed away from parts of the cluster without migrating any
  data.
* *Key/value OSD backend* (experimental): An alternative storage backend
  for Ceph OSD processes that puts all data in a key/value database like
  leveldb.  This provides better performance for workloads dominated by
  key/value operations (like radosgw bucket indices).
* *Standalone radosgw* (experimental): The radosgw process can now run
  in a standalone mode without an apache (or similar) web server or
  fastcgi.  This simplifies deployment and can improve performance.

We expect to maintain a series of stable releases based on v0.80
Firefly for as much as a year.  In the meantime, development of Ceph
continues with the next release, Giant, which will feature work on the
CephFS distributed file system, more alternative storage backends
(like RocksDB and f2fs), RDMA support, support for pyramid erasure
codes, and additional functionality in the block device (RBD) like
copy-on-read and multisite mirroring.


Upgrade Sequencing
------------------

* If your existing cluster is running a version older than v0.67
  Dumpling, please first upgrade to the latest Dumpling release before
  upgrading to v0.80 Firefly.  Please refer to the :ref:`dumpling-upgrade`
  documentation.

* We recommend adding the following to the [mon] section of your
  ceph.conf prior to upgrade::

    mon warn on legacy crush tunables = false

  This will prevent health warnings due to the use of legacy CRUSH
  placement.  Although it is possible to rebalance existing data
  across your cluster (see the upgrade notes below), we do not
  normally recommend it for production environments as a large amount
  of data will move and there is a significant performance impact from
  the rebalancing.

* Upgrade daemons in the following order:

    #. Monitors
    #. OSDs
    #. MDSs and/or radosgw

  If the ceph-mds daemon is restarted first, it will wait until all
  OSDs have been upgraded before finishing its startup sequence.  If
  the ceph-mon daemons are not restarted prior to the ceph-osd
  daemons, they will not correctly register their new capabilities
  with the cluster and new features may not be usable until they are
  restarted a second time.

* Upgrade radosgw daemons together.  There is a subtle change in behavior
  for multipart uploads that prevents a multipart request that was initiated
  with a new radosgw from being completed by an old radosgw.


Upgrading from v0.79
--------------------

* OSDMap's json-formatted dump changed for keys 'full' and 'nearfull'.
  What was previously being outputted as 'true' or 'false' strings are
  now being outputted 'true' and 'false' booleans according to json syntax.

* HEALTH_WARN on 'mon osd down out interval == 0'. Having this option set
  to zero on the leader acts much like having the 'noout' flag set.  This
  warning will only be reported if the monitor getting the 'health' or
  'status' request has this option set to zero.

* Monitor 'auth' commands now require the mon 'x' capability.  This matches
  dumpling v0.67.x and earlier, but differs from emperor v0.72.x.

* A librados WATCH operation on a non-existent object now returns ENOENT;
  previously it did not.

* Librados interface change:  As there are no partial writes, the rados_write()
  and rados_append() operations now return 0 on success like rados_write_full()
  always has.  This includes the C++ interface equivalents and AIO return
  values for the aio variants.

* The radosgw init script (sysvinit) how requires that the 'host = ...' line in
  ceph.conf, if present, match the short hostname (the output of 'hostname -s'),
  not the fully qualified hostname or the (occasionally non-short) output of
  'hostname'.  Failure to adjust this when upgrading from emperor or dumpling
  may prevent the radosgw daemon from starting.

Upgrading from v0.72 Emperor
----------------------------

* See notes above.

* The 'ceph -s' or 'ceph status' command's 'num_in_osds' field in the
  JSON and XML output has been changed from a string to an int.

* The recently added 'ceph mds set allow_new_snaps' command's syntax
  has changed slightly; it is now 'ceph mds set allow_new_snaps true'.
  The 'unset' command has been removed; instead, set the value to
  'false'.

* The syntax for allowing snapshots is now 'mds set allow_new_snaps
  <true|false>' instead of 'mds <set,unset> allow_new_snaps'.

* 'rbd ls' on a pool which never held rbd images now exits with code
  0. It outputs nothing in plain format, or an empty list in
  non-plain format. This is consistent with the behavior for a pool
  which used to hold images, but contains none. Scripts relying on
  this behavior should be updated.

* The MDS requires a new OSD operation TMAP2OMAP, added in this release.  When
  upgrading, be sure to upgrade and restart the ceph-osd daemons before the
  ceph-mds daemon.  The MDS will refuse to start if any up OSDs do not support
  the new feature.

* The 'ceph mds set_max_mds N' command is now deprecated in favor of
  'ceph mds set max_mds N'.

* The 'osd pool create ...' syntax has changed for erasure pools.

* The default CRUSH rules and layouts are now using the 'bobtail'
  tunables and defaults.  Upgraded clusters using the old values will
  now present with a health WARN state.  This can be disabled by
  adding 'mon warn on legacy crush tunables = false' to ceph.conf and
  restarting the monitors.  Alternatively, you can switch to the new
  tunables with 'ceph osd crush tunables firefly,' but keep in mind
  that this will involve moving a *significant* portion of the data
  already stored in the cluster and in a large cluster may take
  several days to complete.  We do not recommend adjusting tunables on a
  production cluster.

* We now default to the 'bobtail' CRUSH tunable values that are first supported
  by Ceph clients in bobtail (v0.56) and Linux kernel version v3.9.  If you
  plan to access a newly created Ceph cluster with an older kernel client, you
  should use 'ceph osd crush tunables legacy' to switch back to the legacy
  behavior.  Note that making that change will likely result in some data
  movement in the system, so adjust the setting before populating the new
  cluster with data.

* We now set the HASHPSPOOL flag on newly created pools (and new
  clusters) by default.  Support for this flag first appeared in
  v0.64; v0.67 Dumpling is the first major release that supports it.
  It is first supported by the Linux kernel version v3.9.  If you plan
  to access a newly created Ceph cluster with an older kernel or
  clients (e.g, librados, librbd) from a pre-dumpling Ceph release,
  you should add 'osd pool default flag hashpspool = false' to the
  '[global]' section of your 'ceph.conf' prior to creating your
  monitors (e.g., after 'ceph-deploy new' but before 'ceph-deploy mon
  create ...').

* The configuration option 'osd pool default crush rule' is deprecated
  and replaced with 'osd pool default crush replicated ruleset'. 'osd
  pool default crush rule' takes precedence for backward compatibility
  and a deprecation warning is displayed when it is used.

* As part of fix for #6796, 'ceph osd pool set <pool> <var> <arg>' now
  receives <arg> as an integer instead of a string.  This affects how
  'hashpspool' flag is set/unset: instead of 'true' or 'false', it now
  must be '0' or '1'.

* The behavior of the CRUSH 'indep' choose mode has been changed.  No
  ceph cluster should have been using this behavior unless someone has
  manually extracted a crush map, modified a CRUSH rule to replace
  'firstn' with 'indep', recompiled, and reinjected the new map into
  the cluster.  If the 'indep' mode is currently in use on a cluster,
  the rule should be modified to use 'firstn' instead, and the
  administrator should wait until any data movement completes before
  upgrading.

* The 'osd dump' command now dumps pool snaps as an array instead of an
  object.


Upgrading from v0.67 Dumpling
-----------------------------

* See notes above.

* ceph-fuse and radosgw now use the same default values for the admin
  socket and log file paths that the other daemons (ceph-osd,
  ceph-mon, etc.) do.  If you run these daemons as non-root, you may
  need to adjust your ceph.conf to disable these options or to adjust
  the permissions on /var/run/ceph and /var/log/ceph.

* The MDS now disallows snapshots by default as they are not
  considered stable.  The command 'ceph mds set allow_snaps' will
  enable them.

* For clusters that were created before v0.44 (pre-argonaut, Spring
  2012) and store radosgw data, the auto-upgrade from TMAP to OMAP
  objects has been disabled.  Before upgrading, make sure that any
  buckets created on pre-argonaut releases have been modified (e.g.,
  by PUTing and then DELETEing an object from each bucket).  Any
  cluster created with argonaut (v0.48) or a later release or not
  using radosgw never relied on the automatic conversion and is not
  affected by this change.

* Any direct users of the 'tmap' portion of the librados API should be
  aware that the automatic tmap -> omap conversion functionality has
  been removed.

* Most output that used K or KB (e.g., for kilobyte) now uses a
  lower-case k to match the official SI convention.  Any scripts that
  parse output and check for an upper-case K will need to be modified.

* librados::Rados::pool_create_async() and librados::Rados::pool_delete_async()
  don't drop a reference to the completion object on error, caller needs to take
  care of that. This has never really worked correctly and we were leaking an
  object

* 'ceph osd crush set <id> <weight> <loc..>' no longer adds the osd to the
  specified location, as that's a job for 'ceph osd crush add'.  It will
  however continue to work just the same as long as the osd already exists
  in the crush map.

* The OSD now enforces that class write methods cannot both mutate an
  object and return data.  The rbd.assign_bid method, the lone
  offender, has been removed.  This breaks compatibility with
  pre-bobtail librbd clients by preventing them from creating new
  images.

* librados now returns on commit instead of ack for synchronous calls.
  This is a bit safer in the case where both OSDs and the client crash, and
  is probably how it should have been acting from the beginning. Users are
  unlikely to notice but it could result in lower performance in some
  circumstances. Those who care should switch to using the async interfaces,
  which let you specify safety semantics precisely.

* The C++ librados AioComplete::get_version() method was incorrectly
  returning an int (usually 32-bits).  To avoid breaking library
  compatibility, a get_version64() method is added that returns the
  full-width value.  The old method is deprecated and will be removed
  in a future release.  Users of the C++ librados API that make use of
  the get_version() method should modify their code to avoid getting a
  value that is truncated from 64 to to 32 bits.


Notable changes since v0.79
---------------------------

* ceph-fuse, libcephfs: fix several caching bugs (Yan, Zheng)
* ceph-fuse: trim inodes in response to mds memory pressure (Yan, Zheng)
* librados: fix inconsistencies in API error values (David Zafman)
* librados: fix watch operations with cache pools (Sage Weil)
* librados: new snap rollback operation (David Zafman)
* mds: fix respawn (John Spray)
* mds: misc bugs (Yan, Zheng)
* mds: misc multi-mds fixes (Yan, Zheng)
* mds: use shared_ptr for requests (Greg Farnum)
* mon: fix peer feature checks (Sage Weil)
* mon: require 'x' mon caps for auth operations (Joao Luis)
* mon: shutdown when removed from mon cluster (Joao Luis)
* msgr: fix locking bug in authentication (Josh Durgin)
* osd: fix bug in journal replay/restart (Sage Weil)
* osd: many many many bug fixes with cache tiering (Samuel Just)
* osd: track omap and hit_set objects in pg stats (Samuel Just)
* osd: warn if agent cannot enable due to invalid (post-split) stats (Sage Weil)
* rados bench: track metadata for multiple runs separately (Guang Yang)
* rgw: fixed subuser modify (Yehuda Sadeh)
* rpm: fix redhat-lsb dependency (Sage Weil, Alfredo Deza)


Notable changes since v0.72 Emperor
-----------------------------------

* buffer: some zero-copy groundwork (Josh Durgin)
* build: misc improvements (Ken Dreyer)
* ceph-conf: stop creating bogus log files (Josh Durgin, Sage Weil)
* ceph-crush-location: new hook for setting CRUSH location of osd daemons on start)
* ceph-disk: avoid fd0 (Loic Dachary)
* ceph-disk: generalize path names, add tests (Loic Dachary)
* ceph-disk: misc improvements for puppet (Loic Dachary)
* ceph-disk: several bug fixes (Loic Dachary)
* ceph-fuse: fix race for sync reads (Sage Weil)
* ceph-fuse, libcephfs: fix several caching bugs (Yan, Zheng)
* ceph-fuse: trim inodes in response to mds memory pressure (Yan, Zheng)
* ceph-kvstore-tool: expanded command set and capabilities (Joao Eduardo Luis)
* ceph.spec: fix build dependency (Loic Dachary)
* common: bloom filter improvements (Sage Weil)
* common: check preexisting admin socket for active daemon before removing (Loic Dachary)
* common: fix aligned buffer allocation (Loic Dachary)
* common: fix authentication on big-endian architectures (Dan Mick)
* common: fix config variable substitution (Loic Dachary)
* common: portability changes to support libc++ (Noah Watkins)
* common: switch to unordered_map from hash_map (Noah Watkins)
* config: recursive metavariable expansion (Loic Dachary)
* crush: default to bobtail tunables (Sage Weil)
* crush: fix off-by-one error in recent refactor (Sage Weil)
* crush: many additional tests (Loic Dachary)
* crush: misc fixes, cleanups (Loic Dachary)
* crush: new rule steps to adjust retry attempts (Sage Weil)
* crush, osd: s/rep/replicated/ for less confusion (Loic Dachary)
* crush: refactor descend_once behavior; support set_choose*_tries for replicated rules (Sage Weil)
* crush: usability and test improvements (Loic Dachary)
* debian: change directory ownership between ceph and ceph-common (Sage Weil)
* debian: integrate misc fixes from downstream packaging (James Page)
* doc: big update to install docs (John Wilkins)
* doc: many many install doc improvements (John Wilkins)
* doc: many many updates (John Wilkins)
* doc: misc fixes (David Moreau Simard, Kun Huang)
* erasure-code: improve buffer alignment (Loic Dachary)
* erasure-code: rewrite region-xor using vector operations (Andreas Peters)
* init: fix startup ordering/timeout problem with OSDs (Dmitry Smirnov)
* libcephfs: fix resource leak (Zheng Yan)
* librados: add C API coverage for atomic write operations (Christian Marie)
* librados: fix inconsistencies in API error values (David Zafman)
* librados: fix throttle leak (and eventual deadlock) (Josh Durgin)
* librados: fix watch operations with cache pools (Sage Weil)
* librados: new snap rollback operation (David Zafman)
* librados, osd: new TMAP2OMAP operation (Yan, Zheng)
* librados: read directly into user buffer (Rutger ter Borg)
* librbd: fix use-after-free aio completion bug #5426 (Josh Durgin)
* librbd: localize/distribute parent reads (Sage Weil)
* librbd: skip zeroes/holes when copying sparse images (Josh Durgin)
* mailmap: affiliation updates (Loic Dachary)
* mailmap updates (Loic Dachary)
* many portability improvements (Noah Watkins)
* many unit test improvements (Loic Dachary)
* mds: always store backtrace in default pool (Yan, Zheng)
* mds: cope with MDS failure during creation (John Spray)
* mds: fix cap migration behavior (Yan, Zheng)
* mds: fix client session flushing (Yan, Zheng)
* mds: fix crash from client sleep/resume (Zheng Yan)
* mds: fix many many multi-mds bugs (Yan, Zheng)
* mds: fix readdir end check (Zheng Yan)
* mds: fix Resetter locking (Alexandre Oliva)
* mds: fix respawn (John Spray)
* mds: inline data support (Li Wang, Yunchuan Wen)
* mds: misc bugs (Yan, Zheng)
* mds: misc fixes for directory fragments (Zheng Yan)
* mds: misc fixes for larger directories (Zheng Yan)
* mds: misc fixes for multiple MDSs (Zheng Yan)
* mds: misc multi-mds fixes (Yan, Zheng)
* mds: remove .ceph directory (John Spray)
* mds: store directories in omap instead of tmap (Yan, Zheng)
* mds: update old-format backtraces opportunistically (Zheng Yan)
* mds: use shared_ptr for requests (Greg Farnum)
* misc cleanups from coverity (Xing Lin)
* misc coverity fixes, cleanups (Danny Al-Gaaf)
* misc coverity fixes (Xing Lin, Li Wang, Danny Al-Gaaf)
* misc portability fixes (Noah Watkins, Alan Somers)
* misc portability fixes (Noah Watkins, Christophe Courtaut, Alan Somers, huanjun)
* misc portability work (Noah Watkins)
* mon: add erasure profiles and improve erasure pool creation (Loic Dachary)
* mon: add 'mon getmap EPOCH' (Joao Eduardo Luis)
* mon: allow adjustment of cephfs max file size via 'ceph mds set max_file_size' (Sage Weil)
* mon: allow debug quorum_{enter,exit} commands via admin socket
* mon: 'ceph osd pg-temp ...' and primary-temp commands (Ilya Dryomov)
* mon: change mds allow_new_snaps syntax to be more consistent (Sage Weil)
* mon: clean up initial crush rule creation (Loic Dachary)
* mon: collect misc metadata about osd (os, kernel, etc.), new 'osd metadata' command (Sage Weil)
* mon: do not create erasure rules by default (Sage Weil)
* mon: do not generate spurious MDSMaps in certain cases (Sage Weil)
* mon: do not use keyring if auth = none (Loic Dachary)
* mon: fix peer feature checks (Sage Weil)
* mon: fix pg_temp leaks (Joao Eduardo Luis)
* mon: fix pool count in 'ceph -s' output (Sage Weil)
* mon: handle more whitespace (newline, tab) in mon capabilities (Sage Weil)
* mon: improve (replicate or erasure) pool creation UX (Loic Dachary)
* mon: infrastructure to handle mixed-version mon cluster and cli/rest API (Greg Farnum)
* mon: MForward tests (Loic Dachary)
* mon: mkfs now idempotent (Loic Dachary)
* mon: only seed new osdmaps to current OSDs (Sage Weil)
* mon, osd: create erasure style crush rules (Loic Dachary, Sage Weil)
* mon: 'osd crush show-tunables' (Sage Weil)
* mon: 'osd dump' dumps pool snaps as array, not object (Dan Mick)
* mon, osd: new 'erasure' pool type (still not fully supported)
* mon: persist quorum features to disk (Greg Farnum)
* mon: prevent extreme changes in pool pg_num (Greg Farnum)
* mon: require 'x' mon caps for auth operations (Joao Luis)
* mon: shutdown when removed from mon cluster (Joao Luis)
* mon: take 'osd pool set ...' value as an int, not string (Joao Eduardo Luis)
* mon: track osd features in OSDMap (Joao Luis, David Zafman)
* mon: trim MDSMaps (Joao Eduardo Luis)
* mon: warn if crush has non-optimal tunables (Sage Weil)
* mount.ceph: add -n for autofs support (Steve Stock)
* msgr: fix locking bug in authentication (Josh Durgin)
* msgr: fix messenger restart race (Xihui He)
* msgr: improve connection error detection between clients and monitors (Greg Farnum, Sage Weil)
* osd: add/fix CPU feature detection for jerasure (Loic Dachary)
* osd: add HitSet tracking for read ops (Sage Weil, Greg Farnum)
* osd: avoid touching leveldb for some xattrs (Haomai Wang, Sage Weil)
* osd: backfill to multiple targets (David Zafman)
* osd: backfill to osds not in acting set (David Zafman)
* osd: cache pool support for snapshots (Sage Weil)
* osd: client IO path changes for EC (Samuel Just)
* osd: default to 3x replication
* osd: do not include backfill targets in acting set (David Zafman)
* osd: enable new hashpspool layout by default (Sage Weil)
* osd: erasure plugin benchmarking tool (Loic Dachary)
* osd: fix and cleanup misc backfill issues (David Zafman)
* osd: fix bug in journal replay/restart (Sage Weil)
* osd: fix copy-get omap bug (Sage Weil)
* osd: fix linux kernel version detection (Ilya Dryomov)
* osd: fix memstore segv (Haomai Wang)
* osd: fix object_info_t encoding bug from emperor (Sam Just)
* osd: fix omap_clear operation to not zap xattrs (Sam Just, Yan, Zheng)
* osd: fix several bugs with tier infrastructure
* osd: fix throttle thread (Haomai Wang)
* osd: fix XFS detection (Greg Farnum, Sushma Gurram)
* osd: generalize scrubbing infrastructure to allow EC (David Zafman)
* osd: handle more whitespace (newline, tab) in osd capabilities (Sage Weil)
* osd: ignore num_objects_dirty on scrub for old pools (Sage Weil)
* osd: improved scrub checks on clones (Sage Weil, Sam Just)
* osd: improve locking in fd lookup cache (Samuel Just, Greg Farnum)
* osd: include more info in pg query result (Sage Weil)
* osd, librados: fix full cluster handling (Josh Durgin)
* osd: many erasure fixes (Sam Just)
* osd: many many many bug fixes with cache tiering (Samuel Just)
* osd: move to jerasure2 library (Loic Dachary)
* osd: new 'chassis' type in default crush hierarchy (Sage Weil)
* osd: new keyvaluestore-dev backend based on leveldb (Haomai Wang)
* osd: new OSDMap encoding (Greg Farnum)
* osd: new tests for erasure pools (David Zafman)
* osd: preliminary cache pool support (no snaps) (Greg Farnum, Sage Weil)
* osd: reduce scrub lock contention (Guang Yang)
* osd: requery unfound on stray notify (#6909) (Samuel Just)
* osd: some PGBackend infrastructure (Samuel Just)
* osd: support for new 'memstore' (memory-backed) backend (Sage Weil)
* osd: track erasure compatibility (David Zafman)
* osd: track omap and hit_set objects in pg stats (Samuel Just)
* osd: warn if agent cannot enable due to invalid (post-split) stats (Sage Weil)
* rados: add 'crush location', smart replica selection/balancing (Sage Weil)
* rados bench: track metadata for multiple runs separately (Guang Yang)
* rados: some performance optimizations (Yehuda Sadeh)
* rados tool: fix listomapvals (Josh Durgin)
* rbd: add 'rbdmap' init script for mapping rbd images on book (Adam Twardowski)
* rbd: add rbdmap support for upstart (Laurent Barbe)
* rbd: expose kernel rbd client options via 'rbd map' (Ilya Dryomov)
* rbd: fix bench-write command (Hoamai Wang)
* rbd: make 'rbd list' return empty list and success on empty pool (Josh Durgin)
* rbd: prevent deletion of images with watchers (Ilya Dryomov)
* rbd: support for 4096 mapped devices, up from ~250 (Ilya Dryomov)
* rest-api: do not fail when no OSDs yet exist (Dan Mick)
* rgw: add 'status' command to sysvinit script (David Moreau Simard)
* rgw: allow multiple frontends (Yehuda Sadeh)
* rgw: allow use of an erasure data pool (Yehuda Sadeh)
* rgw: convert bucket info to new format on demand (Yehuda Sadeh)
* rgw: fixed subuser modify (Yehuda Sadeh)
* rgw: fix error setting empty owner on ACLs (Yehuda Sadeh)
* rgw: fix fastcgi deadlock (do not return data from librados callback) (Yehuda Sadeh)
* rgw: fix many-part multipart uploads (Yehuda Sadeh)
* rgw: fix misc CORS bugs (Robin H. Johnson)
* rgw: fix object placement read op (Yehuda Sadeh)
* rgw: fix reading bucket policy (#6940)
* rgw: fix read_user_buckets 'max' behavior (Yehuda Sadeh)
* rgw: fix several CORS bugs (Robin H. Johnson)
* rgw: fix use-after-free when releasing completion handle (Yehuda Sadeh)
* rgw: improve swift temp URL support (Yehuda Sadeh)
* rgw: make multi-object delete idempotent (Yehuda Sadeh)
* rgw: optionally defer to bucket ACLs instead of object ACLs (Liam Monahan)
* rgw: prototype mongoose frontend (Yehuda Sadeh)
* rgw: several doc fixes (Alexandre Marangone)
* rgw: support for password (instead of admin token) for keystone authentication (Christophe Courtaut)
* rgw: switch from mongoose to civetweb (Yehuda Sadeh)
* rgw: user quotas (Yehuda Sadeh)
* rpm: fix redhat-lsb dependency (Sage Weil, Alfredo Deza)
* specfile: fix RPM build on RHEL6 (Ken Dreyer, Derek Yarnell)
* specfile: ship libdir/ceph (Key Dreyer)
* sysvinit, upstart: prevent both init systems from starting the same daemons (Josh Durgin)


Notable changes since v0.67 Dumpling
------------------------------------

* build cleanly under clang (Christophe Courtaut)
* build: Makefile refactor (Roald J. van Loon)
* build: fix [/usr]/sbin locations (Alan Somers)
* ceph-disk: fix journal preallocation
* ceph-fuse, radosgw: enable admin socket and logging by default
* ceph-fuse: fix problem with readahead vs truncate race (Yan, Zheng)
* ceph-fuse: trim deleted inodes from cache (Yan, Zheng)
* ceph-fuse: use newer fuse api (Jianpeng Ma)
* ceph-kvstore-tool: new tool for working with leveldb (copy, crc) (Joao Luis)
* ceph-post-file: new command to easily share logs or other files with ceph devs
* ceph: improve parsing of CEPH_ARGS (Benoit Knecht)
* ceph: make -h behave when monitors are down
* ceph: parse CEPH_ARGS env variable
* common: bloom_filter improvements, cleanups
* common: cache crc32c values where possible
* common: correct SI is kB not KB (Dan Mick)
* common: fix looping on BSD (Alan Somers)
* common: migrate SharedPtrRegistry to use boost::shared_ptr<> (Loic Dachary)
* common: misc portability fixes (Noah Watkins)
* crc32c: fix optimized crc32c code (it now detects arch support properly)
* crc32c: improved intel-optimized crc32c support (~8x faster on my laptop!)
* crush: fix name caching
* doc: erasure coding design notes (Loic Dachary)
* hadoop: removed old version of shim to avoid confusing users (Noah Watkins)
* librados, mon: ability to query/ping out-of-quorum monitor status (Joao Luis)
* librados: fix async aio completion wakeup
* librados: fix installed header #includes (Dan Mick)
* librados: get_version64() method for C++ API
* librados: hello_world example (Greg Farnum)
* librados: sync calls now return on commit (instead of ack) (Greg Farnum)
* librbd python bindings: fix parent image name limit (Josh Durgin)
* librbd, ceph-fuse: avoid some sources of ceph-fuse, rbd cache stalls
* mds: avoid leaking objects when deleting truncated files (Yan, Zheng)
* mds: fix F_GETLK (Yan, Zheng)
* mds: fix LOOKUPSNAP bug
* mds: fix heap profiler commands (Joao Luis)
* mds: fix locking deadlock (David Disseldorp)
* mds: fix many bugs with stray (unlinked) inodes (Yan, Zheng)
* mds: fix many directory fragmentation bugs (Yan, Zheng)
* mds: fix mds rejoin with legacy parent backpointer xattrs (Alexandre Oliva)
* mds: fix rare restart/failure race during fs creation
* mds: fix standby-replay when we fall behind (Yan, Zheng)
* mds: fix stray directory purging (Yan, Zheng)
* mds: notify clients about deleted files (so they can release from their cache) (Yan, Zheng)
* mds: several bug fixes with clustered mds (Yan, Zheng)
* mon, osd: improve osdmap trimming logic (Samuel Just)
* mon, osd: initial CLI for configuring tiering
* mon: a few 'ceph mon add' races fixed (command is now idempotent) (Joao Luis)
* mon: allow (un)setting HASHPSPOOL flag on existing pools (Joao Luis)
* mon: allow cap strings with . to be unquoted
* mon: allow logging level of cluster log (/var/log/ceph/ceph.log) to be adjusted
* mon: avoid rewriting full osdmaps on restart (Joao Luis)
* mon: continue to discover peer addr info during election phase
* mon: disallow CephFS snapshots until 'ceph mds set allow_new_snaps' (Greg Farnum)
* mon: do not expose uncommitted state from 'osd crush {add,set} ...' (Joao Luis)
* mon: fix 'ceph osd crush reweight ...' (Joao Luis)
* mon: fix 'osd crush move ...' command for buckets (Joao Luis)
* mon: fix byte counts (off by factor of 4) (Dan Mick, Joao Luis)
* mon: fix paxos corner case
* mon: kv properties for pools to support EC (Loic Dachary)
* mon: make 'osd pool rename' idempotent (Joao Luis)
* mon: modify 'auth add' semantics to make a bit more sense (Joao Luis)
* mon: new 'osd perf' command to dump recent performance information (Samuel Just)
* mon: new and improved 'ceph -s' or 'ceph status' command (more info, easier to read)
* mon: some auth check cleanups (Joao Luis)
* mon: track per-pool stats (Joao Luis)
* mon: warn about pools with bad pg_num
* mon: warn when mon data stores grow very large (Joao Luis)
* monc: fix small memory leak
* new wireshark patches pulled into the tree (Kevin Jones)
* objecter, librados: redirect requests based on cache tier config
* objecter: fix possible hang when cluster is unpaused (Josh Durgin)
* osd, librados: add new COPY_FROM rados operation
* osd, librados: add new COPY_GET rados operations (used by COPY_FROM)
* osd: 'osd recover clone overlap limit' option to limit cloning during recovery (Samuel Just)
* osd: COPY_GET on-wire encoding improvements (Greg Farnum)
* osd: add 'osd heartbeat min healthy ratio' configurable (was hard-coded at 33%)
* osd: add option to disable pg log debug code (which burns CPU)
* osd: allow cap strings with . to be unquoted
* osd: automatically detect proper xattr limits (David Zafman)
* osd: avoid extra copy in erasure coding reference implementation (Loic Dachary)
* osd: basic cache pool redirects (Greg Farnum)
* osd: basic whiteout, dirty flag support (not yet used)
* osd: bloom_filter encodability, fixes, cleanups (Loic Dachary, Sage Weil)
* osd: clean up and generalize copy-from code (Greg Farnum)
* osd: cls_hello OSD class example
* osd: erasure coding doc updates (Loic Dachary)
* osd: erasure coding plugin infrastructure, tests (Loic Dachary)
* osd: experiemental support for ZFS (zfsonlinux.org) (Yan, Zheng)
* osd: fix RWORDER flags
* osd: fix exponential backoff of slow request warnings (Loic Dachary)
* osd: fix handling of racing read vs write (Samuel Just)
* osd: fix version value returned by various operations (Greg Farnum)
* osd: generalized temp object infrastructure
* osd: ghobject_t infrastructure for EC (David Zafman)
* osd: improvements for compatset support and storage (David Zafman)
* osd: infrastructure to copy objects from other OSDs
* osd: instrument peering states (David Zafman)
* osd: misc copy-from improvements
* osd: opportunistic crc checking on stored data (off by default)
* osd: properly enforce RD/WR flags for rados classes
* osd: reduce blocking on backing fs (Samuel Just)
* osd: refactor recovery using PGBackend (Samuel Just)
* osd: remove old magical tmap->omap conversion
* osd: remove old pg log on upgrade (Samuel Just)
* osd: revert xattr size limit (fixes large rgw uploads)
* osd: use fdatasync(2) instead of fsync(2) to improve performance (Sam Just)
* pybind: fix blacklisting nonce (Loic Dachary)
* radosgw-agent: multi-region replication/DR
* rgw: complete in-progress requests before shutting down
* rgw: default log level is now more reasonable (Yehuda Sadeh)
* rgw: fix S3 auth with response-* query string params (Sylvain Munaut, Yehuda Sadeh)
* rgw: fix a few minor memory leaks (Yehuda Sadeh)
* rgw: fix acl group check (Yehuda Sadeh)
* rgw: fix inefficient use of std::list::size() (Yehuda Sadeh)
* rgw: fix major CPU utilization bug with internal caching (Yehuda Sadeh, Mark Nelson)
* rgw: fix ordering of write operations (preventing data loss on crash) (Yehuda Sadeh)
* rgw: fix ordering of writes for mulitpart upload (Yehuda Sadeh)
* rgw: fix various CORS bugs (Yehuda Sadeh)
* rgw: fix/improve swift COPY support (Yehuda Sadeh)
* rgw: improve help output (Christophe Courtaut)
* rgw: misc fixes to support DR (Josh Durgin, Yehuda Sadeh)
* rgw: per-bucket quota (Yehuda Sadeh)
* rgw: validate S3 tokens against keystone (Roald J. van Loon)
* rgw: wildcard support for keystone roles (Christophe Courtaut)
* rpm: fix junit dependencies (Alan Grosskurth)
* sysvinit radosgw: fix status return code (Danny Al-Gaaf)
* sysvinit rbdmap: fix error 'service rbdmap stop' (Laurent Barbe)
* sysvinit: add condrestart command (Dan van der Ster)
* sysvinit: fix shutdown order (mons last) (Alfredo Deza)


v0.79
=====

This release is intended to serve as a release candidate for firefly,
which will hopefully be v0.80.  No changes are being made to the code
base at this point except those that fix bugs.  Please test this
release if you intend to make use of the new erasure-coded pools or
cache tiers in firefly.

This release fixes a range of bugs found in v0.78 and streamlines the
user experience when creating erasure-coded pools.  There is also a
raft of fixes for the MDS (multi-mds, directory fragmentation, and
large directories).  The main notable new piece of functionality is a
small change to allow radosgw to use an erasure-coded pool for object
data.


Upgrading
---------
* Erasure pools created with v0.78 will no longer function with v0.79.  You
  will need to delete the old pool and create a new one.

* A bug was fixed in the authentication handshake with big-endian
  architectures that prevent authentication between big- and
  little-endian machines in the same cluster.  If you have a cluster
  that consists entirely of big-endian machines, you will need to
  upgrade all daemons and clients and restart.

* The 'ceph.file.layout' and 'ceph.dir.layout' extended attributes are
  no longer included in the listxattr(2) results to prevent problems with
  'cp -a' and similar tools.

* Monitor 'auth' read-only commands now expect the user to have 'rx' caps.
  This is the same behavior that was present in dumpling, but in emperor
  and more recent development releases the 'r' cap was sufficient.  The
  affected commands are::

    ceph auth export
    ceph auth get
    ceph auth get-key
    ceph auth print-key
    ceph auth list

Notable Changes
---------------
* ceph-conf: stop creating bogus log files (Josh Durgin, Sage Weil)
* common: fix authentication on big-endian architectures (Dan Mick)
* debian: change directory ownership between ceph and ceph-common (Sage Weil)
* init: fix startup ordering/timeout problem with OSDs (Dmitry Smirnov)
* librbd: skip zeroes/holes when copying sparse images (Josh Durgin)
* mds: cope with MDS failure during creation (John Spray)
* mds: fix crash from client sleep/resume (Zheng Yan)
* mds: misc fixes for directory fragments (Zheng Yan)
* mds: misc fixes for larger directories (Zheng Yan)
* mds: misc fixes for multiple MDSs (Zheng Yan)
* mds: remove .ceph directory (John Spray)
* misc coverity fixes, cleanups (Danny Al-Gaaf)
* mon: add erasure profiles and improve erasure pool creation (Loic Dachary)
* mon: 'ceph osd pg-temp ...' and primary-temp commands (Ilya Dryomov)
* mon: fix pool count in 'ceph -s' output (Sage Weil)
* msgr: improve connection error detection between clients and monitors (Greg Farnum, Sage Weil)
* osd: add/fix CPU feature detection for jerasure (Loic Dachary)
* osd: improved scrub checks on clones (Sage Weil, Sam Just)
* osd: many erasure fixes (Sam Just)
* osd: move to jerasure2 library (Loic Dachary)
* osd: new tests for erasure pools (David Zafman)
* osd: reduce scrub lock contention (Guang Yang)
* rgw: allow use of an erasure data pool (Yehuda Sadeh)


v0.78
=====

This development release includes two key features: erasure coding and
cache tiering.  A huge amount of code was merged for this release and
several additional weeks were spent stabilizing the code base, and it
is now in a state where it is ready to be tested by a broader user
base.

This is *not* the firefly release.  Firefly will be delayed for at
least another sprint so that we can get some operational experience
with the new code and do some additional testing before committing to
long term support.

.. note:: Please note that while it is possible to create and test
          erasure coded pools in this release, the pools will not be
          usable when you upgrade to v0.79 as the OSDMap encoding will
          subtlely change.  Please do not populate your test pools
          with important data that can't be reloaded.

Upgrading
---------

* Upgrade daemons in the following order:

    #. Monitors
    #. OSDs
    #. MDSs and/or radosgw

  If the ceph-mds daemon is restarted first, it will wait until all
  OSDs have been upgraded before finishing its startup sequence.  If
  the ceph-mon daemons are not restarted prior to the ceph-osd
  daemons, they will not correctly register their new capabilities
  with the cluster and new features may not be usable until they are
  restarted a second time.

* Upgrade radosgw daemons together.  There is a subtle change in behavior
  for multipart uploads that prevents a multipart request that was initiated
  with a new radosgw from being completed by an old radosgw.

* CephFS recently added support for a new 'backtrace' attribute on
  file data objects that is used for lookup by inode number (i.e., NFS
  reexport and hard links), and will later be used by fsck repair.
  This replaces the existing anchor table mechanism that is used for
  hard link resolution.  In order to completely phase that out, any
  inode that has an outdated backtrace attribute will get updated when
  the inode itself is modified.  This will result in some extra workload
  after a legacy CephFS file system is upgraded.

* The per-op return code in librados' ObjectWriteOperation interface
  is now filled in.

* The librados cmpxattr operation now handles xattrs containing null bytes as
  data rather than null-terminated strings.

* Compound operations in librados that create and then delete the same object
  are now explicitly disallowed (they fail with -EINVAL).

* The default leveldb cache size for the ceph-osd daemon has been
  increased from 4 MB to 128 MB.  This will increase the memory
  footprint of that process but tends to increase performance of omap
  (key/value) objects (used for CephFS and the radosgw).  If memory in your
  deployment is tight, you can preserve the old behavior by adding::

    leveldb write buffer size = 0
    leveldb cache size = 0

  to your ceph.conf to get back the (leveldb) defaults.

Notable Changes
---------------
* ceph-brag: new client and server tools (Sebastien Han, Babu Shanmugam)
* ceph-disk: use partx on RHEL or CentOS instead of partprobe (Alfredo Deza)
* ceph: fix combination of 'tell' and interactive mode (Joao Eduardo Luis)
* ceph-fuse: fix bugs with inline data and multiple MDSs (Zheng Yan)
* client: fix getcwd() to use new LOOKUPPARENT operation (Zheng Yan)
* common: fall back to json-pretty for admin socket (Loic Dachary)
* common: fix 'config dump' debug prefix (Danny Al-Gaaf)
* common: misc coverity fixes (Danny Al-Gaaf)
* common: throtller, shared_cache performance improvements, TrackedOp (Greg Farnum, Samuel Just)
* crush: fix JSON schema for dump (John Spray)
* crush: misc cleanups, tests (Loic Dachary)
* crush: new vary_r tunable (Sage Weil)
* crush: prevent invalid buckets of type 0 (Sage Weil)
* keyvaluestore: add perfcounters, misc bug fixes (Haomai Wang)
* keyvaluestore: portability improvements (Noah Watkins)
* libcephfs: API changes to better support NFS reexport via Ganesha (Matt Benjamin, Adam Emerson, Andrey Kuznetsov, Casey Bodley, David Zafman)
* librados: API documentation improvements (John Wilkins, Josh Durgin)
* librados: fix object enumeration bugs; allow iterator assignment (Josh Durgin)
* librados: streamline tests (Josh Durgin)
* librados: support for atomic read and omap operations for C API (Josh Durgin)
* librados: support for osd and mon command timeouts (Josh Durgin)
* librbd: pass allocation hints to OSD (Ilya Dryomov)
* logrotate: fix bug that prevented rotation for some daemons (Loic Dachary)
* mds: avoid duplicated discovers during recovery (Zheng Yan)
* mds: fix file lock owner checks (Zheng Yan)
* mds: fix LOOKUPPARENT, new LOOKUPNAME ops for reliable NFS reexport (Zheng Yan)
* mds: fix xattr handling on setxattr (Zheng Yan)
* mds: fix xattrs in getattr replies (Sage Weil)
* mds: force backtrace updates for old inodes on update (Zheng Yan)
* mds: several multi-mds and dirfrag bug fixes (Zheng Yan)
* mon: encode erasure stripe width in pool metadata (Loic Dachary)
* mon: erasure code crush rule creation (Loic Dachary)
* mon: erasure code plugin support (Loic Dachary)
* mon: fix bugs in initial post-mkfs quorum creation (Sage Weil)
* mon: fix error output to terminal during startup (Joao Eduardo Luis)
* mon: fix legacy CRUSH tunables warning (Sage Weil)
* mon: fix osd_epochs lower bound tracking for map trimming (Sage Weil)
* mon: fix OSDMap encoding features (Sage Weil, Aaron Ten Clay)
* mon: fix 'pg dump' JSON output (John Spray)
* mon: include dirty stats in 'ceph df detail' (Sage Weil)
* mon: list quorum member names in quorum order (Sage Weil)
* mon: prevent addition of non-empty cache tier (Sage Weil)
* mon: prevent deletion of CephFS pools (John Spray)
* mon: warn when cache tier approaches 'full' (Sage Weil)
* osd: allocation hint, with XFS support (Ilya Dryomov)
* osd: erasure coded pool support (Samuel Just)
* osd: fix bug causing slow/stalled recovery (#7706) (Samuel Just)
* osd: fix bugs in log merging (Samuel Just)
* osd: fix/clarify end-of-object handling on read (Loic Dachary)
* osd: fix impolite mon session backoff, reconnect behavior (Greg Farnum)
* osd: fix SnapContext cache id bug (Samuel Just)
* osd: increase default leveldb cache size and write buffer (Sage Weil, Dmitry Smirnov)
* osd: limit size of 'osd bench ...' arguments (Joao Eduardo Luis)
* osdmaptool: new --test-map-pgs mode (Sage Weil, Ilya Dryomov)
* osd, mon: add primary-affinity to adjust selection of primaries (Sage Weil)
* osd: new 'status' admin socket command (Sage Weil)
* osd: simple tiering agent (Sage Weil)
* osd: store checksums for erasure coded object stripes (Samuel Just)
* osd: tests for objectstore backends (Haomai Wang)
* osd: various refactoring and bug fixes (Samuel Just, David Zafman)
* rados: add 'set-alloc-hint' command (Ilya Dryomov)
* rbd-fuse: fix enumerate_images overflow, memory leak (Ilya Dryomov)
* rbdmap: fix upstart script (Stephan Renatus)
* rgw: avoid logging system events to usage log (Yehuda Sadeh)
* rgw: fix Swift range reponse (Yehuda Sadeh)
* rgw: improve scalability for manifest objects (Yehuda Sadeh)
* rgw: misc fixes for multipart objects, policies (Yehuda Sadeh)
* rgw: support non-standard MultipartUpload command (Yehuda Sadeh)



v0.77
=====

This is the final development release before the Firefly feature
freeze.  The main items in this release include some additional
refactoring work in the OSD IO path (include some locking
improvements), per-user quotas for the radosgw, a switch to civetweb
from mongoose for the prototype radosgw standalone mode, and a
prototype leveldb-based backend for the OSD.  The C librados API also
got support for atomic write operations (read side transactions will
appear in v0.78).

Upgrading
---------

* The 'ceph -s' or 'ceph status' command's 'num_in_osds' field in the
  JSON and XML output has been changed from a string to an int.

* The recently added 'ceph mds set allow_new_snaps' command's syntax
  has changed slightly; it is now 'ceph mds set allow_new_snaps true'.
  The 'unset' command has been removed; instead, set the value to
  'false'.

* The syntax for allowing snapshots is now 'mds set allow_new_snaps
  <true|false>' instead of 'mds <set,unset> allow_new_snaps'.

Notable Changes
---------------

* osd: client IO path changes for EC (Samuel Just)
* common: portability changes to support libc++ (Noah Watkins)
* common: switch to unordered_map from hash_map (Noah Watkins)
* rgw: switch from mongoose to civetweb (Yehuda Sadeh)
* osd: improve locking in fd lookup cache (Samuel Just, Greg Farnum)
* doc: many many updates (John Wilkins)
* rgw: user quotas (Yehuda Sadeh)
* mon: persist quorum features to disk (Greg Farnum)
* mon: MForward tests (Loic Dachary)
* mds: inline data support (Li Wang, Yunchuan Wen)
* rgw: fix many-part multipart uploads (Yehuda Sadeh)
* osd: new keyvaluestore-dev backend based on leveldb (Haomai Wang)
* rbd: prevent deletion of images with watchers (Ilya Dryomov)
* osd: avoid touching leveldb for some xattrs (Haomai Wang, Sage Weil)
* mailmap: affiliation updates (Loic Dachary)
* osd: new OSDMap encoding (Greg Farnum)
* osd: generalize scrubbing infrastructure to allow EC (David Zafman)
* rgw: several doc fixes (Alexandre Marangone)
* librados: add C API coverage for atomic write operations (Christian Marie)
* rgw: improve swift temp URL support (Yehuda Sadeh)
* rest-api: do not fail when no OSDs yet exist (Dan Mick)
* common: check preexisting admin socket for active daemon before removing (Loic Dachary)
* osd: handle more whitespace (newline, tab) in osd capabilities (Sage Weil)
* mon: handle more whitespace (newline, tab) in mon capabilities (Sage Weil)
* rgw: make multi-object delete idempotent (Yehuda Sadeh)
* crush: fix off-by-one error in recent refactor (Sage Weil)
* rgw: fix read_user_buckets 'max' behavior (Yehuda Sadeh)
* mon: change mds allow_new_snaps syntax to be more consistent (Sage Weil)


v0.76
=====

This release includes another batch of updates for firefly
functionality.  Most notably, the cache pool infrastructure now
support snapshots, the OSD backfill functionality has been generalized
to include multiple targets (necessary for the coming erasure pools),
and there were performance improvements to the erasure code plugin on
capable processors.  The MDS now properly utilizes (and seamlessly
migrates to) the OSD key/value interface (aka omap) for storing directory
objects.  There continue to be many other fixes and improvements for
usability and code portability across the tree.

Upgrading
---------

* 'rbd ls' on a pool which never held rbd images now exits with code
  0. It outputs nothing in plain format, or an empty list in
  non-plain format. This is consistent with the behavior for a pool
  which used to hold images, but contains none. Scripts relying on
  this behavior should be updated.

* The MDS requires a new OSD operation TMAP2OMAP, added in this release.  When
  upgrading, be sure to upgrade and restart the ceph-osd daemons before the
  ceph-mds daemon.  The MDS will refuse to start if any up OSDs do not support
  the new feature.

* The 'ceph mds set_max_mds N' command is now deprecated in favor of
  'ceph mds set max_mds N'.

Notable Changes
---------------

* build: misc improvements (Ken Dreyer)
* ceph-disk: generalize path names, add tests (Loic Dachary)
* ceph-disk: misc improvements for puppet (Loic Dachary)
* ceph-disk: several bug fixes (Loic Dachary)
* ceph-fuse: fix race for sync reads (Sage Weil)
* config: recursive metavariable expansion (Loic Dachary)
* crush: usability and test improvements (Loic Dachary)
* doc: misc fixes (David Moreau Simard, Kun Huang)
* erasure-code: improve buffer alignment (Loic Dachary)
* erasure-code: rewrite region-xor using vector operations (Andreas Peters)
* librados, osd: new TMAP2OMAP operation (Yan, Zheng)
* mailmap updates (Loic Dachary)
* many portability improvements (Noah Watkins)
* many unit test improvements (Loic Dachary)
* mds: always store backtrace in default pool (Yan, Zheng)
* mds: store directories in omap instead of tmap (Yan, Zheng)
* mon: allow adjustment of cephfs max file size via 'ceph mds set max_file_size' (Sage Weil)
* mon: do not create erasure rules by default (Sage Weil)
* mon: do not generate spurious MDSMaps in certain cases (Sage Weil)
* mon: do not use keyring if auth = none (Loic Dachary)
* mon: fix pg_temp leaks (Joao Eduardo Luis)
* osd: backfill to multiple targets (David Zafman)
* osd: cache pool support for snapshots (Sage Weil)
* osd: fix and cleanup misc backfill issues (David Zafman)
* osd: fix omap_clear operation to not zap xattrs (Sam Just, Yan, Zheng)
* osd: ignore num_objects_dirty on scrub for old pools (Sage Weil)
* osd: include more info in pg query result (Sage Weil)
* osd: track erasure compatibility (David Zafman)
* rbd: make 'rbd list' return empty list and success on empty pool (Josh Durgin)
* rgw: fix object placement read op (Yehuda Sadeh)
* rgw: fix several CORS bugs (Robin H. Johnson)
* specfile: fix RPM build on RHEL6 (Ken Dreyer, Derek Yarnell)
* specfile: ship libdir/ceph (Key Dreyer)


v0.75
=====

This is a big release, with lots of infrastructure going in for
firefly.  The big items include a prototype standalone frontend for
radosgw (which does not require apache or fastcgi), tracking for read
activity on the osds (to inform tiering decisions), preliminary cache
pool support (no snapshots yet), and lots of bug fixes and other work
across the tree to get ready for the next batch of erasure coding
patches.

For comparison, here are the diff stats for the last few versions::

 v0.75 291 files changed, 82713 insertions(+), 33495 deletions(-)
 v0.74 192 files changed, 17980 insertions(+), 1062 deletions(-)
 v0.73 148 files changed, 4464 insertions(+), 2129 deletions(-)

Upgrading
---------

- The 'osd pool create ...' syntax has changed for erasure pools.

- The default CRUSH rules and layouts are now using the latest and
  greatest tunables and defaults.  Clusters using the old values will
  now present with a health WARN state.  This can be disabled by
  adding 'mon warn on legacy crush tunables = false' to ceph.conf.


Notable Changes
---------------

* common: bloom filter improvements (Sage Weil)
* common: fix config variable substitution (Loic Dachary)
* crush, osd: s/rep/replicated/ for less confusion (Loic Dachary)
* crush: refactor descend_once behavior; support set_choose*_tries for replicated rules (Sage Weil)
* librados: fix throttle leak (and eventual deadlock) (Josh Durgin)
* librados: read directly into user buffer (Rutger ter Borg)
* librbd: fix use-after-free aio completion bug #5426 (Josh Durgin)
* librbd: localize/distribute parent reads (Sage Weil)
* mds: fix Resetter locking (Alexandre Oliva)
* mds: fix cap migration behavior (Yan, Zheng)
* mds: fix client session flushing (Yan, Zheng)
* mds: fix many many multi-mds bugs (Yan, Zheng)
* misc portability work (Noah Watkins)
* mon, osd: create erasure style crush rules (Loic Dachary, Sage Weil)
* mon: 'osd crush show-tunables' (Sage Weil)
* mon: clean up initial crush rule creation (Loic Dachary)
* mon: improve (replicate or erasure) pool creation UX (Loic Dachary)
* mon: infrastructure to handle mixed-version mon cluster and cli/rest API (Greg Farnum)
* mon: mkfs now idempotent (Loic Dachary)
* mon: only seed new osdmaps to current OSDs (Sage Weil)
* mon: track osd features in OSDMap (Joao Luis, David Zafman)
* mon: warn if crush has non-optimal tunables (Sage Weil)
* mount.ceph: add -n for autofs support (Steve Stock)
* msgr: fix messenger restart race (Xihui He)
* osd, librados: fix full cluster handling (Josh Durgin)
* osd: add HitSet tracking for read ops (Sage Weil, Greg Farnum)
* osd: backfill to osds not in acting set (David Zafman)
* osd: enable new hashpspool layout by default (Sage Weil)
* osd: erasure plugin benchmarking tool (Loic Dachary)
* osd: fix XFS detection (Greg Farnum, Sushma Gurram)
* osd: fix copy-get omap bug (Sage Weil)
* osd: fix linux kernel version detection (Ilya Dryomov)
* osd: fix memstore segv (Haomai Wang)
* osd: fix several bugs with tier infrastructure
* osd: fix throttle thread (Haomai Wang)
* osd: preliminary cache pool support (no snaps) (Greg Farnum, Sage Weil)
* rados tool: fix listomapvals (Josh Durgin)
* rados: add 'crush location', smart replica selection/balancing (Sage Weil)
* rados: some performance optimizations (Yehuda Sadeh)
* rbd: add rbdmap support for upstart (Laurent Barbe)
* rbd: expose kernel rbd client options via 'rbd map' (Ilya Dryomov)
* rbd: fix bench-write command (Hoamai Wang)
* rbd: support for 4096 mapped devices, up from ~250 (Ilya Dryomov)
* rgw: allow multiple frontends (Yehuda Sadeh)
* rgw: convert bucket info to new format on demand (Yehuda Sadeh)
* rgw: fix misc CORS bugs (Robin H. Johnson)
* rgw: prototype mongoose frontend (Yehuda Sadeh)



v0.74
=====

This release includes a few substantial pieces for Firefly, including
a long-overdue switch to 3x replication by default and a switch to the
"new" CRUSH tunables by default (supported since bobtail).  There is
also a fix for a long-standing radosgw bug (stalled GET) that has
already been backported to emperor and dumpling.

Upgrading
---------

* We now default to the 'bobtail' CRUSH tunable values that are first supported
  by Ceph clients in bobtail (v0.56) and Linux kernel version v3.9.  If you
  plan to access a newly created Ceph cluster with an older kernel client, you
  should use 'ceph osd crush tunables legacy' to switch back to the legacy
  behavior.  Note that making that change will likely result in some data
  movement in the system, so adjust the setting before populating the new
  cluster with data.

* We now set the HASHPSPOOL flag on newly created pools (and new
  clusters) by default.  Support for this flag first appeared in
  v0.64; v0.67 Dumpling is the first major release that supports it.
  It is first supported by the Linux kernel version v3.9.  If you plan
  to access a newly created Ceph cluster with an older kernel or
  clients (e.g, librados, librbd) from a pre-dumpling Ceph release,
  you should add 'osd pool default flag hashpspool = false' to the
  '[global]' section of your 'ceph.conf' prior to creating your
  monitors (e.g., after 'ceph-deploy new' but before 'ceph-deploy mon
  create ...').

* The configuration option 'osd pool default crush rule' is deprecated
  and replaced with 'osd pool default crush replicated ruleset'. 'osd
  pool default crush rule' takes precedence for backward compatibility
  and a deprecation warning is displayed when it is used.

Notable Changes
---------------

* buffer: some zero-copy groundwork (Josh Durgin)
* ceph-disk: avoid fd0 (Loic Dachary)
* crush: default to bobtail tunables (Sage Weil)
* crush: many additional tests (Loic Dachary)
* crush: misc fixes, cleanups (Loic Dachary)
* crush: new rule steps to adjust retry attempts (Sage Weil)
* debian: integrate misc fixes from downstream packaging (James Page)
* doc: big update to install docs (John Wilkins)
* libcephfs: fix resource leak (Zheng Yan)
* misc coverity fixes (Xing Lin, Li Wang, Danny Al-Gaaf)
* misc portability fixes (Noah Watkins, Alan Somers)
* mon, osd: new 'erasure' pool type (still not fully supported)
* mon: add 'mon getmap EPOCH' (Joao Eduardo Luis)
* mon: collect misc metadata about osd (os, kernel, etc.), new 'osd metadata' command (Sage Weil)
* osd: default to 3x replication
* osd: do not include backfill targets in acting set (David Zafman)
* osd: new 'chassis' type in default crush hierarchy (Sage Weil)
* osd: requery unfound on stray notify (#6909) (Samuel Just)
* osd: some PGBackend infrastructure (Samuel Just)
* osd: support for new 'memstore' (memory-backed) backend (Sage Weil)
* rgw: fix fastcgi deadlock (do not return data from librados callback) (Yehuda Sadeh)
* rgw: fix reading bucket policy (#6940)
* rgw: fix use-after-free when releasing completion handle (Yehuda Sadeh)


v0.73
=====

This release, the first development release after emperor, includes
many bug fixes and a few additional pieces of functionality.  The
first batch of larger changes will be landing in the next version,
v0.74.

Upgrading
---------

- As part of fix for #6796, 'ceph osd pool set <pool> <var> <arg>' now
  receives <arg> as an integer instead of a string.  This affects how
  'hashpspool' flag is set/unset: instead of 'true' or 'false', it now
  must be '0' or '1'.

- The behavior of the CRUSH 'indep' choose mode has been changed.  No
  ceph cluster should have been using this behavior unless someone has
  manually extracted a crush map, modified a CRUSH rule to replace
  'firstn' with 'indep', recompiled, and reinjected the new map into
  the cluster.  If the 'indep' mode is currently in use on a cluster,
  the rule should be modified to use 'firstn' instead, and the
  administrator should wait until any data movement completes before
  upgrading.

- The 'osd dump' command now dumps pool snaps as an array instead of an
  object.

- The radosgw init script (sysvinit) how requires that the 'host = ...' line in
  ceph.conf, if present, match the short hostname (the output of 'hostname -s'),
  not the fully qualified hostname or the (occasionally non-short) output of
  'hostname'.  Failure to adjust this when upgrading from emperor or dumpling
  may prevent the radosgw daemon from starting.


Notable Changes
---------------

* ceph-crush-location: new hook for setting CRUSH location of osd daemons on start
* ceph-kvstore-tool: expanded command set and capabilities (Joao Eduardo Luis)
* ceph.spec: fix build dependency (Loic Dachary)
* common: fix aligned buffer allocation (Loic Dachary)
* doc: many many install doc improvements (John Wilkins)
* mds: fix readdir end check (Zheng Yan)
* mds: update old-format backtraces opportunistically (Zheng Yan)
* misc cleanups from coverity (Xing Lin)
* misc portability fixes (Noah Watkins, Christophe Courtaut, Alan Somers, huanjun)
* mon: 'osd dump' dumps pool snaps as array, not object (Dan Mick)
* mon: allow debug quorum_{enter,exit} commands via admin socket
* mon: prevent extreme changes in pool pg_num (Greg Farnum)
* mon: take 'osd pool set ...' value as an int, not string (Joao Eduardo Luis)
* mon: trim MDSMaps (Joao Eduardo Luis)
* osd: fix object_info_t encoding bug from emperor (Sam Just)
* rbd: add 'rbdmap' init script for mapping rbd images on book (Adam Twardowski)
* rgw: add 'status' command to sysvinit script (David Moreau Simard)
* rgw: fix error setting empty owner on ACLs (Yehuda Sadeh)
* rgw: optionally defer to bucket ACLs instead of object ACLs (Liam Monahan)
* rgw: support for password (instead of admin token) for keystone authentication (Christophe Courtaut)
* sysvinit, upstart: prevent both init systems from starting the same daemons (Josh Durgin)
