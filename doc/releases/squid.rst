=====
Squid
=====

Squid is the 19th stable release of Ceph.

v19.2.3 Squid
=============
This is the third backport release in the Squid series.
We recommend that all users update to this release.

Notable Changes
---------------

* RGW: PutObjectLockConfiguration can now be used to enable S3 Object Lock on an
  existing versioning-enabled bucket that was not created with Object Lock enabled.
* RADOS: A new command, `ceph osd rm-pg-upmap-primary-all`, has been added that allows
  users to clear all pg-upmap-primary mappings in the osdmap when desired.
  Related trackers:

   - https://tracker.ceph.com/issues/67179
   - https://tracker.ceph.com/issues/66867
* RBD: Moving an image that is a member of a group to trash is no longer
  allowed.  `rbd trash mv` command now behaves the same way as `rbd rm` in this
  scenario.
* MGR: MGR's always-on modulues/plugins can now be force-disabled. This can be
  necessary in cases where MGR(s) needs to be prevented from being flooded by
  the module commands when coresponding Ceph service is down/degraded.
* RGW: An authentication bypass vulnerability in STS
  [CVE-2023-43040] has been fixed.
* RGW: S3 policy now enforces ARN-based conditionals.
* RGW: Copying an object to itself no longer causes data
  loss. Potential corruption on ETIMEDOUT (not enabled by default),
  was also fixed.

Changelog
---------
* [CVE-2024-48916] rgw/sts: fix to disallow unsupported JWT algorithms (`pr#62137 <https://github.com/ceph/ceph/pull/62137>`_, Pritha Srivastava, Adam Emerson)
* [squid] RGW backports (`pr#63030 <https://github.com/ceph/ceph/pull/63030>`_, Soumya Koduri)
* ceph-volume: fix Zap.ensure_associated_raw() (`pr#61260 <https://github.com/ceph/ceph/pull/61260>`_, Guillaume Abrioux)
* doc/rados: edit ops/user-management.rst (`pr#63828 <https://github.com/ceph/ceph/pull/63828>`_, Zac Dover)
* doc: Fixes a typo in controllers section of hardware recommendations (`pr#61178 <https://github.com/ceph/ceph/pull/61178>`_, Kevin Niederwanger)
* exporter: repair Squid ceph_exporter.cc (`pr#61448 <https://github.com/ceph/ceph/pull/61448>`_, Zac Dover)
* Links to Jenkins jobs in PR comment commands / Remove deprecated commands (`pr#62035 <https://github.com/ceph/ceph/pull/62035>`_, David Galloway)
* mds: do not process client metrics message with fast dispatch (`issue#68865 <http://tracker.ceph.com/issues/68865>`_, `pr#62058 <https://github.com/ceph/ceph/pull/62058>`_, Venky Shankar)
* qa/tests: added squid-p2p suite (`pr#61809 <https://github.com/ceph/ceph/pull/61809>`_, Yuri Weinstein)
* qa/tests: changed ubuntu 20.04 to 22.04 (`pr#64309 <https://github.com/ceph/ceph/pull/64309>`_, yuriw)
* squid : os/bluestore : Assigning a named variable to ceph::time_guard to prevent immediate destruction (`pr#61971 <https://github.com/ceph/ceph/pull/61971>`_, Jaya Prakash)
* squid: .github: Fix RTD build retrigger (`pr#63211 <https://github.com/ceph/ceph/pull/63211>`_, David Galloway)
* squid: <common> fix formatter buffer out-of-bounds (`pr#61104 <https://github.com/ceph/ceph/pull/61104>`_, liubingrun)
* squid: <rgw> Ensure the ETag format is consistent with AWS S3 API (`pr#62607 <https://github.com/ceph/ceph/pull/62607>`_, Casey Bodley, liubingrun)
* squid: AsyncMessenger.cc : improve error messages (`pr#61401 <https://github.com/ceph/ceph/pull/61401>`_, Anthony D'Atri)
* squid: Backport the hybrid_btree2 allocator and prereqs (`pr#62540 <https://github.com/ceph/ceph/pull/62540>`_, Igor Fedotov, Jrchyang Yu)
* squid: Backport two commits to the Squid branch (`pr#61629 <https://github.com/ceph/ceph/pull/61629>`_, Kamoltat Sirivadhna)
* squid: blk/kernel: bring "bdev_async_discard" config parameter back (`pr#62254 <https://github.com/ceph/ceph/pull/62254>`_, Igor Fedotov, Yite Gu)
* squid: blk/kerneldevice: fix invalid iterator usage after erase in discard_q… (`pr#62576 <https://github.com/ceph/ceph/pull/62576>`_, Yite Gu)
* squid: blk/KernelDevice: Introduce a cap on the number of pending discards (`pr#62221 <https://github.com/ceph/ceph/pull/62221>`_, Joshua Baergen)
* squid: blk/kerneldevice: notify_all only required when discard_drain wait for condition (`pr#62151 <https://github.com/ceph/ceph/pull/62151>`_, Yite Gu)
* squid: build-with-container fixes exec bit, dnf cache dir option (`pr#61912 <https://github.com/ceph/ceph/pull/61912>`_, John Mulligan)
* squid: build-with-container: fixes and enhancements (`pr#62161 <https://github.com/ceph/ceph/pull/62161>`_, John Mulligan)
* squid: build-with-container: two small fixes (`pr#62340 <https://github.com/ceph/ceph/pull/62340>`_, John Mulligan)
* squid: build: Fix opentelemetry-cpp build failure on Noble (`pr#64012 <https://github.com/ceph/ceph/pull/64012>`_, Adam C. Emerson)
* squid: ceph-volume: allow zapping partitions on multipath devices (`pr#62177 <https://github.com/ceph/ceph/pull/62177>`_, Guillaume Abrioux)
* squid: ceph-volume: fix loop devices support (`pr#61420 <https://github.com/ceph/ceph/pull/61420>`_, Guillaume Abrioux)
* squid: ceph-volume: Fix splitting with too many parts (`pr#63012 <https://github.com/ceph/ceph/pull/63012>`_, Janne Heß)
* squid: ceph-volume: support splitting db even on collocated scenario (`pr#61975 <https://github.com/ceph/ceph/pull/61975>`_, Guillaume Abrioux)
* squid: ceph-volume: support zapping by osd-id for RAW OSDs (`pr#60487 <https://github.com/ceph/ceph/pull/60487>`_, Guillaume Abrioux)
* squid: ceph.spec.in: add man/rgw-gap-list (`pr#63998 <https://github.com/ceph/ceph/pull/63998>`_, Matan Breizman)
* squid: cephadm: check "ceph_device_lvm" field instead of "ceph_device" during zap (`pr#62905 <https://github.com/ceph/ceph/pull/62905>`_, Adam King)
* squid: cephfs-shell: fixing cephfs-shell test failures (`pr#60443 <https://github.com/ceph/ceph/pull/60443>`_, Neeraj Pratap Singh)
* squid: cephfs-top, qa: Remove unnecessary global statements in tests (`pr#62605 <https://github.com/ceph/ceph/pull/62605>`_, Kefu Chai)
* squid: cephfs-top: fix exceptions on small/large sized windows (`pr#59899 <https://github.com/ceph/ceph/pull/59899>`_, Jos Collin)
* squid: client,mds: case-insensitive directory trees (`pr#62095 <https://github.com/ceph/ceph/pull/62095>`_, Patrick Donnelly, Casey Bodley, Lucian Petrut, John Mulligan)
* squid: client: contiguous read fails for non-contiguous write (in async I/O api) (`pr#60218 <https://github.com/ceph/ceph/pull/60218>`_, Dhairya Parmar)
* squid: client: disallow unprivileged users to escalate root privileges (`pr#63458 <https://github.com/ceph/ceph/pull/63458>`_, Xiubo Li, Venky Shankar)
* squid: client: Fix opening and reading of symlinks (`pr#60372 <https://github.com/ceph/ceph/pull/60372>`_, Anoop C S)
* squid: client: fixed a bug that read operation hung (`pr#60694 <https://github.com/ceph/ceph/pull/60694>`_, Tod Chen)
* squid: client: flush the caps release in filesystem sync (`pr#59395 <https://github.com/ceph/ceph/pull/59395>`_, Xiubo Li)
* squid: client: Prevent race condition when printing Inode in ll_sync_inode (`pr#59621 <https://github.com/ceph/ceph/pull/59621>`_, Chengen Du)
* squid: client: return EOPNOTSUPP for fallocate with mode 0 (`pr#60656 <https://github.com/ceph/ceph/pull/60656>`_, Milind Changire)
* squid: cls/rbd: write image mirror status if state is CREATING (`pr#63234 <https://github.com/ceph/ceph/pull/63234>`_, N Balachandran)
* squid: cls/rgw: non-versioned listings skip past version suffix (`pr#62590 <https://github.com/ceph/ceph/pull/62590>`_, Casey Bodley)
* squid: common,ceph: add output file switch to dump json to (`pr#57675 <https://github.com/ceph/ceph/pull/57675>`_, Patrick Donnelly)
* squid: common/options: fix the description of osd_max_scrubs (`pr#62377 <https://github.com/ceph/ceph/pull/62377>`_, Satoru Takeuchi)
* squid: common/pick_address: Add IPv6 support to is_addr_in_subnet (`pr#61323 <https://github.com/ceph/ceph/pull/61323>`_, Nitzan Mordechai)
* squid: common/StackStringStream: update pointer to newly allocated memory in overflow() (`pr#57361 <https://github.com/ceph/ceph/pull/57361>`_, Rongqi Sun)
* squid: common: CephContext::_refresh_perf_values() checks for null _mempool_perf (`pr#62852 <https://github.com/ceph/ceph/pull/62852>`_, Casey Bodley)
* squid: common: fix md_config_cacher_t (`pr#61398 <https://github.com/ceph/ceph/pull/61398>`_, Ronen Friedman)
* squid: common: Leverage a better CRC32C implementation (`pr#59389 <https://github.com/ceph/ceph/pull/59389>`_, Tyler Stachecki)
* squid: common: use close_range on Linux (`pr#61639 <https://github.com/ceph/ceph/pull/61639>`_, edef)
* squid: container/build.sh: don't require repo creds on NO_PUSH (`pr#61585 <https://github.com/ceph/ceph/pull/61585>`_, Dan Mick)
* squid: container/build.sh: fix up org vs. repo naming (`pr#61584 <https://github.com/ceph/ceph/pull/61584>`_, Dan Mick)
* squid: container/build.sh: remove local container images (`pr#62066 <https://github.com/ceph/ceph/pull/62066>`_, Dan Mick)
* squid: container/Containerfile: replace CEPH_VERSION label for backward compact (`pr#61583 <https://github.com/ceph/ceph/pull/61583>`_, Dan Mick)
* squid: container: add label ceph=True back (`pr#61611 <https://github.com/ceph/ceph/pull/61611>`_, John Mulligan)
* squid: container: small container image improvements (`pr#62346 <https://github.com/ceph/ceph/pull/62346>`_, John Mulligan)
* squid: containerized build tools [V2] (`pr#61681 <https://github.com/ceph/ceph/pull/61681>`_, John Mulligan)
* squid: crush: use std::vector instead of variable length arrays (`pr#61956 <https://github.com/ceph/ceph/pull/61956>`_, Kefu Chai)
* squid: debian: add ceph-exporter package (`pr#62270 <https://github.com/ceph/ceph/pull/62270>`_, Shinya Hayashi)
* squid: dencoder tests fix type backwards incompatible checks (`pr#62198 <https://github.com/ceph/ceph/pull/62198>`_, Nitzan Mordechai)
* squid: doc/architecture: remove sentence (`pr#61614 <https://github.com/ceph/ceph/pull/61614>`_, Zac Dover)
* squid: doc/cephadm/services: Add mention of --zap for OSD removal (`pr#62443 <https://github.com/ceph/ceph/pull/62443>`_, Anthony D'Atri)
* squid: doc/cephadm/services: Correct indentation in osd.rst (`pr#62427 <https://github.com/ceph/ceph/pull/62427>`_, Anthony D'Atri)
* squid: doc/cephadm/services: Fix formatting in osd.rst (`pr#62810 <https://github.com/ceph/ceph/pull/62810>`_, Anthony D'Atri)
* squid: doc/cephadm/services: improve rgw.rst and snmp-gateway.rst (`pr#62694 <https://github.com/ceph/ceph/pull/62694>`_, Anthony D'Atri)
* squid: doc/cephadm/services: Re-improve osd.rst (`pr#61952 <https://github.com/ceph/ceph/pull/61952>`_, Anthony D'Atri)
* squid: doc/cephadm: Add admonition re restarting an OSD service (`pr#62796 <https://github.com/ceph/ceph/pull/62796>`_, Anthony D'Atri)
* squid: doc/cephadm: Add PG autoscaler advice to upgrade.rst (`pr#62379 <https://github.com/ceph/ceph/pull/62379>`_, Anthony D'Atri)
* squid: doc/cephadm: clarify "Monitoring OSD State" (`pr#61664 <https://github.com/ceph/ceph/pull/61664>`_, Zac Dover)
* squid: doc/cephadm: Correct formatting in upgrade.rst (`pr#63147 <https://github.com/ceph/ceph/pull/63147>`_, Anthony D'Atri)
* squid: doc/cephadm: correct markup in rgw.rst (`pr#63073 <https://github.com/ceph/ceph/pull/63073>`_, Zac Dover)
* squid: doc/cephadm: correct note (`pr#61528 <https://github.com/ceph/ceph/pull/61528>`_, Zac Dover)
* squid: doc/cephadm: improve "Activate Existing OSDs" (`pr#61747 <https://github.com/ceph/ceph/pull/61747>`_, Zac Dover)
* squid: doc/cephadm: improve "Activate Existing OSDs" (`pr#61725 <https://github.com/ceph/ceph/pull/61725>`_, Zac Dover)
* squid: doc/cephadm: improve "Maintenance Mode" (`pr#63495 <https://github.com/ceph/ceph/pull/63495>`_, Zac Dover)
* squid: doc/cephadm: s/confg/config/ (`pr#62644 <https://github.com/ceph/ceph/pull/62644>`_, Zac Dover)
* squid: doc/cephadm: simplify confusing math proposition (`pr#61574 <https://github.com/ceph/ceph/pull/61574>`_, Zac Dover)
* squid: doc/cephfs: correct ill-formatted command (`pr#63501 <https://github.com/ceph/ceph/pull/63501>`_, Zac Dover)
* squid: doc/cephfs: correct reference structure in fs-volumes.rst (`pr#63544 <https://github.com/ceph/ceph/pull/63544>`_, Zac Dover)
* squid: doc/cephfs: Cosmetic changes and small fixes in cephfs-mirroring.rst (`pr#63467 <https://github.com/ceph/ceph/pull/63467>`_, Ville Ojamo)
* squid: doc/cephfs: disaster-recovery-experts cleanup (`pr#61446 <https://github.com/ceph/ceph/pull/61446>`_, Zac Dover)
* squid: doc/cephfs: document first-damage.py (`pr#63977 <https://github.com/ceph/ceph/pull/63977>`_, Zac Dover)
* squid: doc/cephfs: document purge queue and its perf counters (`pr#61193 <https://github.com/ceph/ceph/pull/61193>`_, Dhairya Parmar)
* squid: doc/cephfs: edit 2nd 3rd of mount-using-kernel-driver (`pr#61058 <https://github.com/ceph/ceph/pull/61058>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (`pr#61423 <https://github.com/ceph/ceph/pull/61423>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (2 of x) (`pr#61443 <https://github.com/ceph/ceph/pull/61443>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (3 of x) (`pr#61453 <https://github.com/ceph/ceph/pull/61453>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (4 of x) (`pr#61479 <https://github.com/ceph/ceph/pull/61479>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (5 of x) (`pr#61499 <https://github.com/ceph/ceph/pull/61499>`_, Zac Dover)
* squid: doc/cephfs: edit disaster-recovery-experts (6 of x) (`pr#61521 <https://github.com/ceph/ceph/pull/61521>`_, Zac Dover)
* squid: doc/cephfs: edit first 3rd of mount-using-kernel-driver (`pr#61056 <https://github.com/ceph/ceph/pull/61056>`_, Zac Dover)
* squid: doc/cephfs: edit grammar in snapshots.rst (`pr#61459 <https://github.com/ceph/ceph/pull/61459>`_, Zac Dover)
* squid: doc/cephfs: link section for pausing async threads in section for (`pr#62874 <https://github.com/ceph/ceph/pull/62874>`_, Rishabh Dave)
* squid: doc/cephfs: Update deprecation notice in experimental-features.rst (`pr#63948 <https://github.com/ceph/ceph/pull/63948>`_, Ville Ojamo)
* squid: doc/dev/cephfs-mirroring: edit file 1 of x (`pr#63298 <https://github.com/ceph/ceph/pull/63298>`_, Zac Dover)
* squid: doc/dev/cephfs-mirroring: edit file 2 of x (`pr#63273 <https://github.com/ceph/ceph/pull/63273>`_, Zac Dover)
* squid: doc/dev/cephfs-mirroring: edit file 3 of x (`pr#63547 <https://github.com/ceph/ceph/pull/63547>`_, Zac Dover)
* squid: doc/dev/cephfs-mirroring: edit file 4 of x (`pr#63660 <https://github.com/ceph/ceph/pull/63660>`_, Zac Dover)
* squid: doc/dev/developer_guide/essentials: update mailing lists (`pr#62375 <https://github.com/ceph/ceph/pull/62375>`_, Laimis Juzeliunas)
* squid: doc/dev/release-process.rst: release builds cannot build containers (`pr#61817 <https://github.com/ceph/ceph/pull/61817>`_, Dan Mick, Zac Dover)
* squid: doc/dev: Debuggging with gdb (`pr#63993 <https://github.com/ceph/ceph/pull/63993>`_, Matan Breizman)
* squid: doc/dev: update link to backporter manual (`pr#63990 <https://github.com/ceph/ceph/pull/63990>`_, Zac Dover)
* squid: doc/glossary: s/OMAP/omap/ (`pr#63737 <https://github.com/ceph/ceph/pull/63737>`_, Zac Dover)
* squid: doc/man: supplant "wsync" with "nowsync" as the default (`pr#60199 <https://github.com/ceph/ceph/pull/60199>`_, Zac Dover)
* squid: doc/mgr/ceph_api: edit index.rst (`pr#63197 <https://github.com/ceph/ceph/pull/63197>`_, Zac Dover)
* squid: doc/mgr/dashboard_plugins: edit feature_toggles.inc.rst (`pr#63704 <https://github.com/ceph/ceph/pull/63704>`_, Zac Dover)
* squid: doc/mgr: Add root CA cert instructions to rgw.rst (`pr#61884 <https://github.com/ceph/ceph/pull/61884>`_, Anuradha Gadge, Zac Dover)
* squid: doc/mgr: edit administrator.rst (`pr#63207 <https://github.com/ceph/ceph/pull/63207>`_, Zac Dover)
* squid: doc/mgr: edit alerts.rst (`pr#63200 <https://github.com/ceph/ceph/pull/63200>`_, Zac Dover)
* squid: doc/mgr: edit cli_api (`pr#63743 <https://github.com/ceph/ceph/pull/63743>`_, Zac Dover)
* squid: doc/mgr: edit cli_api.rst (`pr#63689 <https://github.com/ceph/ceph/pull/63689>`_, Zac Dover)
* squid: doc/mgr: edit crash.rst (`pr#63538 <https://github.com/ceph/ceph/pull/63538>`_, Zac Dover)
* squid: doc/mgr: edit dashboard.rst (`pr#63315 <https://github.com/ceph/ceph/pull/63315>`_, Zac Dover)
* squid: doc/mgr: edit debug.inc.rst (`pr#63393 <https://github.com/ceph/ceph/pull/63393>`_, Zac Dover)
* squid: doc/mgr: edit diskpredictor.rst (`pr#63423 <https://github.com/ceph/ceph/pull/63423>`_, Zac Dover)
* squid: doc/mgr: edit feature_toggles.inc.rst (`pr#63396 <https://github.com/ceph/ceph/pull/63396>`_, Zac Dover)
* squid: doc/mgr: edit hello.rst (`pr#63507 <https://github.com/ceph/ceph/pull/63507>`_, Zac Dover)
* squid: doc/mgr: edit influx.rst (`pr#63454 <https://github.com/ceph/ceph/pull/63454>`_, Zac Dover)
* squid: doc/mgr: edit insights.rst (`pr#63510 <https://github.com/ceph/ceph/pull/63510>`_, Zac Dover)
* squid: doc/mgr: edit iostat.rst (`pr#63680 <https://github.com/ceph/ceph/pull/63680>`_, Zac Dover)
* squid: doc/mgr: edit iostat.rst (`pr#63513 <https://github.com/ceph/ceph/pull/63513>`_, Zac Dover)
* squid: doc/mgr: edit localpool.rst (`pr#63669 <https://github.com/ceph/ceph/pull/63669>`_, Zac Dover)
* squid: doc/mgr: edit localpool.rst (`pr#63550 <https://github.com/ceph/ceph/pull/63550>`_, Zac Dover)
* squid: doc/mgr: edit mds_autoscaler.rst (`pr#63492 <https://github.com/ceph/ceph/pull/63492>`_, Zac Dover)
* squid: doc/mgr: edit modules.rst (`pr#63666 <https://github.com/ceph/ceph/pull/63666>`_, Zac Dover)
* squid: doc/mgr: edit modules.rst (`pr#63577 <https://github.com/ceph/ceph/pull/63577>`_, Zac Dover)
* squid: doc/mgr: edit motd.inc.rst (`pr#63402 <https://github.com/ceph/ceph/pull/63402>`_, Zac Dover)
* squid: doc/mgr: edit nfs.rst (`pr#63663 <https://github.com/ceph/ceph/pull/63663>`_, Zac Dover)
* squid: doc/mgr: edit nfs.rst (`pr#63580 <https://github.com/ceph/ceph/pull/63580>`_, Zac Dover)
* squid: doc/mgr: edit orchestrator.rst (`pr#63583 <https://github.com/ceph/ceph/pull/63583>`_, Zac Dover)
* squid: doc/mgr: edit progress.rst (`pr#63657 <https://github.com/ceph/ceph/pull/63657>`_, Zac Dover)
* squid: doc/mgr: edit progress.rst (`pr#63586 <https://github.com/ceph/ceph/pull/63586>`_, Zac Dover)
* squid: doc/mgr: edit prometheus.rst (`pr#63589 <https://github.com/ceph/ceph/pull/63589>`_, Zac Dover)
* squid: doc/mgr: edit rgw.rst (`pr#63592 <https://github.com/ceph/ceph/pull/63592>`_, Zac Dover)
* squid: doc/mgr: edit telegraf.rst (`pr#63611 <https://github.com/ceph/ceph/pull/63611>`_, Zac Dover)
* squid: doc/mgr: edit telemetry (1 of x) (`pr#63768 <https://github.com/ceph/ceph/pull/63768>`_, Zac Dover)
* squid: doc/mgr: edit telemetry (2 of x) (`pr#63771 <https://github.com/ceph/ceph/pull/63771>`_, Zac Dover)
* squid: doc/mgr: edit telemetry (3 of x) (`pr#63774 <https://github.com/ceph/ceph/pull/63774>`_, Zac Dover)
* squid: doc/mgr: edit telemetry (4 of x) (`pr#63777 <https://github.com/ceph/ceph/pull/63777>`_, Zac Dover)
* squid: doc/mgr: edit telemetry.rst (`pr#63905 <https://github.com/ceph/ceph/pull/63905>`_, Zac Dover)
* squid: doc/mgr: edit telemetry.rst (`pr#63864 <https://github.com/ceph/ceph/pull/63864>`_, Zac Dover)
* squid: doc/mgr: edit telemetry.rst (`pr#63692 <https://github.com/ceph/ceph/pull/63692>`_, Zac Dover)
* squid: doc/mgr: edit telemetry.rst (lines 300-400) (`pr#63867 <https://github.com/ceph/ceph/pull/63867>`_, Zac Dover)
* squid: doc/mgr: Improve prometheus.rst (`pr#62930 <https://github.com/ceph/ceph/pull/62930>`_, Anthony D'Atri)
* squid: doc/mgr: Small improvements in rgw.rst (`pr#63625 <https://github.com/ceph/ceph/pull/63625>`_, Ville Ojamo)
* squid: doc/monitoring: correct list formatting (`pr#63541 <https://github.com/ceph/ceph/pull/63541>`_, Zac Dover)
* squid: doc/monitoring: Improve index.rst (`pr#62265 <https://github.com/ceph/ceph/pull/62265>`_, Anthony D'Atri)
* squid: doc/rados/configuration/bluestore-config-ref: Fix lowercase typos (`pr#62290 <https://github.com/ceph/ceph/pull/62290>`_, Dan van der Ster)
* squid: doc/rados/configuration: Correct admonition in ceph-conf.rst (`pr#62620 <https://github.com/ceph/ceph/pull/62620>`_, Anthony D'Atri)
* squid: doc/rados/configuration: Improve ceph-conf.rst (`pr#63942 <https://github.com/ceph/ceph/pull/63942>`_, Anthony D'Atri)
* squid: doc/rados/operations/stretch-mode.rst: Added Limitations to stretch pool configurations (`pr#61006 <https://github.com/ceph/ceph/pull/61006>`_, Kamoltat Sirivadhna)
* squid: doc/rados/operations: Actually mention `upmap_max_deviation` setting … (`pr#64118 <https://github.com/ceph/ceph/pull/64118>`_, Niklas Hambüchen)
* squid: doc/rados/operations: Add settings advice to balancer.rst (`pr#63535 <https://github.com/ceph/ceph/pull/63535>`_, Anthony D'Atri)
* squid: doc/rados/operations: Additional improvements to placement-groups.rst (`pr#63649 <https://github.com/ceph/ceph/pull/63649>`_, Anthony D'Atri)
* squid: doc/rados/operations: Address suggestions for stretch-mode.rst (`pr#63849 <https://github.com/ceph/ceph/pull/63849>`_, Anthony D'Atri, Zac Dover)
* squid: doc/rados/operations: Clarify stretch mode vs device class (`pr#62077 <https://github.com/ceph/ceph/pull/62077>`_, Anthony D'Atri)
* squid: doc/rados/operations: Fix unordered list in health-checks.rst (`pr#63958 <https://github.com/ceph/ceph/pull/63958>`_, Ville Ojamo)
* squid: doc/rados/operations: improve crush-map-edits.rst (`pr#62317 <https://github.com/ceph/ceph/pull/62317>`_, Anthony D'Atri)
* squid: doc/rados/operations: Improve erasure-code.rst (`pr#62573 <https://github.com/ceph/ceph/pull/62573>`_, Anthony D'Atri)
* squid: doc/rados/operations: Improve placement-groups.rst (`pr#63646 <https://github.com/ceph/ceph/pull/63646>`_, Anthony D'Atri)
* squid: doc/rados/operations: Improve pools.rst (`pr#61728 <https://github.com/ceph/ceph/pull/61728>`_, Anthony D'Atri)
* squid: doc/rados/operations: Improve stretch-mode.rst (`pr#63815 <https://github.com/ceph/ceph/pull/63815>`_, Anthony D'Atri)
* squid: doc/rados/ops: edit cache-tiering.rst (`pr#63830 <https://github.com/ceph/ceph/pull/63830>`_, Zac Dover)
* squid: doc/rados/troubleshooting: Improve troubleshooting-pg.rst (`pr#62320 <https://github.com/ceph/ceph/pull/62320>`_, Anthony D'Atri)
* squid: doc/rados: edit balancer.rst (`pr#63683 <https://github.com/ceph/ceph/pull/63683>`_, Zac Dover)
* squid: doc/rados: enhance "pools.rst" (`pr#63861 <https://github.com/ceph/ceph/pull/63861>`_, Zac Dover)
* squid: doc/rados: improve markup in cache-tiering.rst (`pr#63504 <https://github.com/ceph/ceph/pull/63504>`_, Zac Dover)
* squid: doc/rados: improve pg_num/pgp_num info (`pr#62056 <https://github.com/ceph/ceph/pull/62056>`_, Zac Dover)
* squid: doc/rados: pool and namespace are independent osdcap restrictions (`pr#61523 <https://github.com/ceph/ceph/pull/61523>`_, Ilya Dryomov)
* squid: doc/rados: s/enpty/empty/ in pgcalc doc (`pr#63498 <https://github.com/ceph/ceph/pull/63498>`_, Zac Dover)
* squid: doc/rados: Update mClock doc on steps to override OSD IOPS capacity config (`pr#63071 <https://github.com/ceph/ceph/pull/63071>`_, Sridhar Seshasayee)
* squid: doc/radosgw /notifications: fix topic details (`pr#62404 <https://github.com/ceph/ceph/pull/62404>`_, Laimis Juzeliunas)
* squid: doc/radosgw/admin.rst: explain bucket and uid flags for bucket quota (`pr#64021 <https://github.com/ceph/ceph/pull/64021>`_, Hyun Jin Kim)
* squid: doc/radosgw/cloud-transition: fix details (`pr#62834 <https://github.com/ceph/ceph/pull/62834>`_, Laimis Juzeliunas)
* squid: doc/radosgw/config-ref: fix lc worker thread tuning (`pr#61437 <https://github.com/ceph/ceph/pull/61437>`_, Laimis Juzeliunas)
* squid: doc/radosgw/s3: correct eTag op match tables (`pr#61308 <https://github.com/ceph/ceph/pull/61308>`_, Anthony D'Atri)
* squid: doc/radosgw: add "persistent_topic_size" (`pr#64139 <https://github.com/ceph/ceph/pull/64139>`_, Zac Dover)
* squid: doc/radosgw: Cosmetic and formatting improvements in vault.rst (`pr#63229 <https://github.com/ceph/ceph/pull/63229>`_, Ville Ojamo)
* squid: doc/radosgw: Cosmetic improvements in cloud-transition.rst (`pr#63448 <https://github.com/ceph/ceph/pull/63448>`_, Ville Ojamo)
* squid: doc/radosgw: Cosmetic improvements in dynamicresharding.rst (`pr#64058 <https://github.com/ceph/ceph/pull/64058>`_, Ville Ojamo)
* squid: doc/radosgw: edit cloud-transition (1 of x) (`pr#64024 <https://github.com/ceph/ceph/pull/64024>`_, Zac Dover)
* squid: doc/radosgw: edit sentence in metrics.rst (`pr#63700 <https://github.com/ceph/ceph/pull/63700>`_, Zac Dover)
* squid: doc/radosgw: Fix RST syntax rendeded as text in oidc.rst (`pr#62989 <https://github.com/ceph/ceph/pull/62989>`_, Ville Ojamo)
* squid: doc/radosgw: improve "pubsub_push_pending" info (`pr#64113 <https://github.com/ceph/ceph/pull/64113>`_, Zac Dover)
* squid: doc/radosgw: Improve and more consistent formatting (`pr#62909 <https://github.com/ceph/ceph/pull/62909>`_, Ville Ojamo)
* squid: doc/radosgw: Improve cloud-restore and cloud-transition (`pr#62666 <https://github.com/ceph/ceph/pull/62666>`_, Anthony D'Atri)
* squid: doc/radosgw: Improve formatting in layout.rst (`pr#62999 <https://github.com/ceph/ceph/pull/62999>`_, Anthony D'Atri)
* squid: doc/radosgw: Improve layout.rst (`pr#62449 <https://github.com/ceph/ceph/pull/62449>`_, Anthony D'Atri)
* squid: doc/radosgw: Promptify CLI commands and fix formatting in layout.rst (`pr#63915 <https://github.com/ceph/ceph/pull/63915>`_, Ville Ojamo)
* squid: doc/radosgw: Promptify CLI, cosmetic fixes (`pr#62856 <https://github.com/ceph/ceph/pull/62856>`_, Ville Ojamo)
* squid: doc/radosgw: remove "pubsub_event_lost" (`pr#64126 <https://github.com/ceph/ceph/pull/64126>`_, Zac Dover)
* squid: doc/radosgw: remove "pubsub_event_triggered" (`pr#64155 <https://github.com/ceph/ceph/pull/64155>`_, Zac Dover)
* squid: doc/radosgw: s/zonegroup/pools/ (`pr#61556 <https://github.com/ceph/ceph/pull/61556>`_, Zac Dover)
* squid: doc/radosgw: update aws specification link (`pr#64095 <https://github.com/ceph/ceph/pull/64095>`_, Zac Dover)
* squid: doc/radosgw: Use ref for hyperlinking to multisite (`pr#63311 <https://github.com/ceph/ceph/pull/63311>`_, Ville Ojamo)
* squid: doc/rbd: add mirroring troubleshooting info (`pr#63846 <https://github.com/ceph/ceph/pull/63846>`_, Zac Dover)
* squid: doc/rbd: use https links in live import examples (`pr#61605 <https://github.com/ceph/ceph/pull/61605>`_, Ilya Dryomov)
* squid: doc/releases: add actual_eol for quincy (`pr#61359 <https://github.com/ceph/ceph/pull/61359>`_, Zac Dover)
* squid: doc/releases: Add ordering comment to releases.yml (`pr#62192 <https://github.com/ceph/ceph/pull/62192>`_, Anthony D'Atri)
* squid: doc/releases: correct squid release order (`pr#61988 <https://github.com/ceph/ceph/pull/61988>`_, Zac Dover)
* squid: doc/rgw: add man documentation for the rgw-gap-list tool (`pr#63996 <https://github.com/ceph/ceph/pull/63996>`_, J. Eric Ivancich)
* squid: doc/rgw: add man documentation for the rgw-gap-list tool (`pr#63728 <https://github.com/ceph/ceph/pull/63728>`_, Matan Breizman, J. Eric Ivancich)
* squid: doc/rgw: clarify path-style vs virtual-hosted-style access (`pr#61986 <https://github.com/ceph/ceph/pull/61986>`_, Casey Bodley)
* squid: doc/rgw: document Admin and System Users (`pr#62881 <https://github.com/ceph/ceph/pull/62881>`_, Casey Bodley)
* squid: doc/rgw: document UserName requirements for account migration (`pr#61333 <https://github.com/ceph/ceph/pull/61333>`_, Casey Bodley)
* squid: doc/rgw: use 'confval' directive to render sts config options (`pr#63441 <https://github.com/ceph/ceph/pull/63441>`_, Casey Bodley)
* squid: doc/src: edit osd.yaml.in (osd_deep_scrub_interval_cv) (`pr#63955 <https://github.com/ceph/ceph/pull/63955>`_, Zac Dover)
* squid: doc/start: edit documenting-ceph.rst (`pr#63652 <https://github.com/ceph/ceph/pull/63652>`_, Zac Dover)
* squid: doc/start: edit documenting-ceph.rst (`pr#63707 <https://github.com/ceph/ceph/pull/63707>`_, Zac Dover)
* squid: doc/start: Mention RGW in Intro to Ceph (`pr#61926 <https://github.com/ceph/ceph/pull/61926>`_, Anthony D'Atri)
* squid: doc: add snapshots in docs under Cephfs concepts (`pr#61246 <https://github.com/ceph/ceph/pull/61246>`_, Neeraj Pratap Singh)
* squid: doc: Clarify that there are no tertiary OSDs (`pr#61730 <https://github.com/ceph/ceph/pull/61730>`_, Anthony D'Atri)
* squid: doc: fix formatting in cephfs_mirror dev doc (`pr#63250 <https://github.com/ceph/ceph/pull/63250>`_, Jos Collin)
* squid: doc: fix incorrect radosgw-admin subcommand (`pr#62004 <https://github.com/ceph/ceph/pull/62004>`_, Toshikuni Fukaya)
* squid: doc: Fix missing blank line Sphinx warnings (`pr#63337 <https://github.com/ceph/ceph/pull/63337>`_, Ville Ojamo)
* squid: doc: fixup #58689 - document SSE-C iam condition key (`pr#62297 <https://github.com/ceph/ceph/pull/62297>`_, dawg)
* squid: doc: improve tests-integration-testing-teuthology-workflow.rst (`pr#61342 <https://github.com/ceph/ceph/pull/61342>`_, Vallari Agrawal)
* squid: doc: mgr/dashboard: add OAuth2 SSO documentation (`pr#64033 <https://github.com/ceph/ceph/pull/64033>`_, Pedro Gonzalez Gomez, Zac Dover)
* squid: doc: src: modernize sample.ceph.conf (`pr#61641 <https://github.com/ceph/ceph/pull/61641>`_, Anthony D'Atri)
* squid: doc: update cephfs-journal-tool docs (`pr#63108 <https://github.com/ceph/ceph/pull/63108>`_, Jos Collin)
* squid: doc: Upgrade and unpin some python versions (`pr#61931 <https://github.com/ceph/ceph/pull/61931>`_, David Galloway)
* squid: fix: the RGW crash caused by special characters (`pr#64049 <https://github.com/ceph/ceph/pull/64049>`_, mertsunacoglu, Emin)
* squid: integrate blockdiff with cephfs-mirror daemon (`issue#70287 <http://tracker.ceph.com/issues/70287>`_, `issue#69791 <http://tracker.ceph.com/issues/69791>`_, `issue#70225 <http://tracker.ceph.com/issues/70225>`_, `issue#70584 <http://tracker.ceph.com/issues/70584>`_, `pr#63241 <https://github.com/ceph/ceph/pull/63241>`_, Venky Shankar, Jos Collin)
* squid: librbd/cache/pwl: fix memory leak in SyncPoint persist context cleanup (`pr#64097 <https://github.com/ceph/ceph/pull/64097>`_, Kefu Chai)
* squid: librbd/migration/QCOWFormat: don't complete read_clusters() inline (`pr#64196 <https://github.com/ceph/ceph/pull/64196>`_, Ilya Dryomov)
* squid: librbd/migration: add external clusters support + NBD stream (`pr#63406 <https://github.com/ceph/ceph/pull/63406>`_, Ilya Dryomov, Effi Ofer, Or Ozeri)
* squid: librbd: add rbd_diff_iterate3() API to take source snapshot by ID (`pr#62130 <https://github.com/ceph/ceph/pull/62130>`_, Ilya Dryomov, Vinay Bhaskar Varada)
* squid: librbd: clear ctx before initiating close in Image::{aio\_,}close() (`pr#61527 <https://github.com/ceph/ceph/pull/61527>`_, Ilya Dryomov)
* squid: librbd: disallow "rbd trash mv" if image is in a group (`pr#62968 <https://github.com/ceph/ceph/pull/62968>`_, Ilya Dryomov)
* squid: librbd: fix a crash in get_rollback_snap_id (`pr#62044 <https://github.com/ceph/ceph/pull/62044>`_, Ilya Dryomov, N Balachandran)
* squid: librbd: fix a deadlock on image_lock caused by Mirror::image_disable() (`pr#62128 <https://github.com/ceph/ceph/pull/62128>`_, Ilya Dryomov)
* squid: librbd: fix mirror image status summary in a namespace (`pr#61832 <https://github.com/ceph/ceph/pull/61832>`_, Ilya Dryomov)
* squid: librbd: respect rbd_default_snapshot_quiesce_mode in group_snap_create() (`pr#62963 <https://github.com/ceph/ceph/pull/62963>`_, Ilya Dryomov)
* squid: librbd: stop filtering async request error codes (`pr#61645 <https://github.com/ceph/ceph/pull/61645>`_, Ilya Dryomov)
* squid: log: concatenate thread names and print once per thread (`pr#61287 <https://github.com/ceph/ceph/pull/61287>`_, Patrick Donnelly)
* squid: LogMonitor: set no_reply for forward MLog commands (`pr#62213 <https://github.com/ceph/ceph/pull/62213>`_, Nitzan Mordechai)
* squid: mds/Beacon: wake up the thread in shutdown() (`pr#60837 <https://github.com/ceph/ceph/pull/60837>`_, Max Kellermann)
* squid: mds: add an asok command to dump export states (`pr#60836 <https://github.com/ceph/ceph/pull/60836>`_, Zhansong Gao)
* squid: mds: batch backtrace updates by pool-id when expiring a log segment (`issue#63259 <http://tracker.ceph.com/issues/63259>`_, `pr#60688 <https://github.com/ceph/ceph/pull/60688>`_, Venky Shankar)
* squid: mds: cephx path restriction incorrectly rejects snapshots of deleted directory (`pr#59518 <https://github.com/ceph/ceph/pull/59518>`_, Patrick Donnelly)
* squid: mds: drop client metrics during recovery (`pr#59866 <https://github.com/ceph/ceph/pull/59866>`_, Patrick Donnelly)
* squid: mds: enforce usage of host error in cephfs, use errorcode32_t in MClientReply message (`pr#61994 <https://github.com/ceph/ceph/pull/61994>`_, Igor Golikov)
* squid: mds: getattr just waits the xlock to be released by the previous client (`pr#60691 <https://github.com/ceph/ceph/pull/60691>`_, Xiubo Li)
* squid: mds: Implement remove for ceph vxattrs (`pr#60752 <https://github.com/ceph/ceph/pull/60752>`_, Christopher Hoffman)
* squid: mds: invalid id for client eviction is to be treated as success (`issue#68132 <http://tracker.ceph.com/issues/68132>`_, `pr#60059 <https://github.com/ceph/ceph/pull/60059>`_, Venky Shankar)
* squid: mds: move fscrypt inode_t metadata to mds_co mempool (`pr#59616 <https://github.com/ceph/ceph/pull/59616>`_, Patrick Donnelly)
* squid: mds: prevent duplicate wrlock acquisition for a single request (`pr#61840 <https://github.com/ceph/ceph/pull/61840>`_, Xiubo Li, Sunnatillo)
* squid: mds: trim mdlog when segments exceed threshold and trim was idle (`pr#60838 <https://github.com/ceph/ceph/pull/60838>`_, Venky Shankar)
* squid: mgr/cephadm: fixing logic in cert-store save_cert method (`pr#63853 <https://github.com/ceph/ceph/pull/63853>`_, Redouane Kachach)
* squid: mgr/dashboard: (refactor)fix image size in nvmeof namespace create/update api (`pr#61969 <https://github.com/ceph/ceph/pull/61969>`_, Afreen Misbah)
* squid: mgr/dashboard: accept dot(.) in user_id & restrict tenant validation in user form (`pr#63477 <https://github.com/ceph/ceph/pull/63477>`_, Naman Munet)
* squid: mgr/dashboard: Add --force flag for listeners (`pr#64132 <https://github.com/ceph/ceph/pull/64132>`_, Afreen Misbah)
* squid: mgr/dashboard: add a custom warning message when enabling feature (`pr#61039 <https://github.com/ceph/ceph/pull/61039>`_, Nizamudeen A)
* squid: mgr/dashboard: Add additional NVME API endpoints (`pr#61998 <https://github.com/ceph/ceph/pull/61998>`_, Tomer Haskalovitch)
* squid: mgr/dashboard: Add ceph_daemon filter to rgw overview grafana panel queries (`pr#62267 <https://github.com/ceph/ceph/pull/62267>`_, Aashish Sharma)
* squid: mgr/dashboard: add prometheus read permission to cluster_mgr role (`pr#62650 <https://github.com/ceph/ceph/pull/62650>`_, Nizamudeen A)
* squid: mgr/dashboard: add xlmtodict import and fix lifecycle get request (`pr#62393 <https://github.com/ceph/ceph/pull/62393>`_, Pedro Gonzalez Gomez)
* Squid: mgr/dashboard: Administration > Configuration > Some of the config options are not updatable at runtime (`pr#61181 <https://github.com/ceph/ceph/pull/61181>`_, Naman Munet)
* squid: mgr/dashboard: Changing SimpleGraphPanel to TimeSeries Panel in cephfs.libsonnet to fix inconsistency in Line Graphs (`pr#62381 <https://github.com/ceph/ceph/pull/62381>`_, Piyush Agarwal)
* squid: mgr/dashboard: Changing SimpleGraphPanel to TimeSeries Panel in host.libsonnet to fix inconsistency in Line Graphs (`pr#62382 <https://github.com/ceph/ceph/pull/62382>`_, Piyush Agarwal)
* squid: mgr/dashboard: Changing SimpleGraphPanel to TimeSeries Panel in osd.libsonnet to fix inconsistency in Line Graphs (`pr#62383 <https://github.com/ceph/ceph/pull/62383>`_, Piyush Agarwal)
* squid: mgr/dashboard: Changing SimpleGraphPanel to TimeSeries Panel in pool.libsonnet to fix inconsistency in Line Graphs (`pr#62384 <https://github.com/ceph/ceph/pull/62384>`_, Piyush Agarwal)
* squid: mgr/dashboard: critical confirmation modal changes (`pr#61961 <https://github.com/ceph/ceph/pull/61961>`_, Naman Munet)
* squid: mgr/dashboard: deprecate transifex-i18ntool and support transifex cli (`pr#63287 <https://github.com/ceph/ceph/pull/63287>`_, John Mulligan, Afreen Misbah)
* squid: mgr/dashboard: disable deleting bucket with objects (`pr#61972 <https://github.com/ceph/ceph/pull/61972>`_, Naman Munet)
* squid: mgr/dashboard: enable ha by default on subsystem POST API (`pr#62623 <https://github.com/ceph/ceph/pull/62623>`_, Nizamudeen A)
* squid: mgr/dashboard: Fix empty ceph version in GET api/hosts (`pr#62731 <https://github.com/ceph/ceph/pull/62731>`_, Afreen Misbah)
* squid: mgr/dashboard: fix image filter's query on rbd-details grafana panel (`pr#62531 <https://github.com/ceph/ceph/pull/62531>`_, Aashish Sharma)
* squid: mgr/dashboard: Fix Latency chart data units in rgw overview page (`pr#61238 <https://github.com/ceph/ceph/pull/61238>`_, Aashish Sharma)
* squid: mgr/dashboard: fix make check tests (`pr#63187 <https://github.com/ceph/ceph/pull/63187>`_, John Mulligan, Afreen Misbah)
* Squid: mgr/dashboard: fix multisite e2e failures (`pr#61189 <https://github.com/ceph/ceph/pull/61189>`_, Naman Munet)
* squid: mgr/dashboard: fix total objects/Avg object size in RGW Overview Page (`pr#61457 <https://github.com/ceph/ceph/pull/61457>`_, Aashish Sharma)
* squid: mgr/dashboard: Fix variable capitalization in embedded rbd-details panel (`pr#62208 <https://github.com/ceph/ceph/pull/62208>`_, Juan Ferrer Toribio)
* squid: mgr/dashboard: namespace update route robustness (`pr#61999 <https://github.com/ceph/ceph/pull/61999>`_, Tomer Haskalovitch)
* squid: mgr/dashboard: pin lxml to fix run-dashboard-tox-make-check failure (`pr#62257 <https://github.com/ceph/ceph/pull/62257>`_, Nizamudeen A)
* squid: mgr/dashboard: Update and correct zonegroup delete notification (`pr#61235 <https://github.com/ceph/ceph/pull/61235>`_, Aashish Sharma)
* squid: mgr/dashboard: upgrading nvmeof doesn't update configuration (`pr#62628 <https://github.com/ceph/ceph/pull/62628>`_, Nizamudeen A)
* squid: mgr/dashboard: When configuring the RGW Multisite endpoints from the UI allow FQDN(Not only IP) (`pr#62353 <https://github.com/ceph/ceph/pull/62353>`_, Aashish Sharma)
* squid: mgr/nfs: Don't crash ceph-mgr if NFS clusters are unavailable (`pr#58285 <https://github.com/ceph/ceph/pull/58285>`_, Anoop C S, Ponnuvel Palaniyappan)
* squid: mgr/rbd_support: always parse interval and start_time in Schedules::remove() (`pr#62965 <https://github.com/ceph/ceph/pull/62965>`_, Ilya Dryomov)
* squid: mgr/vol : shortening the name of helper method (`pr#60396 <https://github.com/ceph/ceph/pull/60396>`_, Neeraj Pratap Singh)
* squid: mgr: add status command (`pr#62504 <https://github.com/ceph/ceph/pull/62504>`_, Patrick Donnelly)
* squid: mgr: allow disabling always-on modules (`pr#60562 <https://github.com/ceph/ceph/pull/60562>`_, Rishabh Dave)
* squid: mgr: fix subuser creation via dashboard (`pr#62086 <https://github.com/ceph/ceph/pull/62086>`_, Hannes Baum)
* squid: mgr: process map before notifying clients (`pr#57064 <https://github.com/ceph/ceph/pull/57064>`_, Patrick Donnelly)
* squid: mon [stretch mode]: support disable_stretch_mode (`pr#60629 <https://github.com/ceph/ceph/pull/60629>`_, Kamoltat Sirivadhna)
* squid: mon, osd: add command to remove invalid pg-upmap-primary entries (`pr#62421 <https://github.com/ceph/ceph/pull/62421>`_, Laura Flores)
* squid: mon/AuthMonitor: provide command to rotate the key for a user credential (`pr#58235 <https://github.com/ceph/ceph/pull/58235>`_, Patrick Donnelly)
* squid: mon/LogMonitor: Use generic cluster log level config (`pr#61069 <https://github.com/ceph/ceph/pull/61069>`_, Prashant D)
* squid: mon/OSDMonitor: relax cap enforcement for unmanaged snapshots (`pr#61603 <https://github.com/ceph/ceph/pull/61603>`_, Ilya Dryomov)
* squid: mon/scrub: log error details of store access failures (`pr#61346 <https://github.com/ceph/ceph/pull/61346>`_, Yite Gu)
* squid: mon/test_mon_osdmap_prune: Use first_pinned instead of first_committed (`pr#63341 <https://github.com/ceph/ceph/pull/63341>`_, Aishwarya Mathuria)
* squid: mon: fix `fs set down` to adjust max_mds only when cluster is not down (`pr#59704 <https://github.com/ceph/ceph/pull/59704>`_, chungfengz)
* squid: monitoring: Fix OSDs panel in host-details grafana dashboard (`pr#62625 <https://github.com/ceph/ceph/pull/62625>`_, Aashish Sharnma)
* squid: node-proxy: address `ceph orch hardware status` cmd (`pr#63787 <https://github.com/ceph/ceph/pull/63787>`_, Guillaume Abrioux)
* squid: os, osd: bring the lightweight OMAP iteration (`pr#61363 <https://github.com/ceph/ceph/pull/61363>`_, Radoslaw Zarzynski)
* squid: os/bluestore/ceph-bluestore-tool: Modify show-label for many devs (`pr#60543 <https://github.com/ceph/ceph/pull/60543>`_, Adam Kupczyk)
* squid: os/bluestore: Add health warning for bluestore fragmentation (`pr#61910 <https://github.com/ceph/ceph/pull/61910>`_, Adam Kupczyk)
* squid: os/bluestore: allow use BtreeAllocator (`pr#59497 <https://github.com/ceph/ceph/pull/59497>`_, tan changzhi)
* squid: os/bluestore: Create additional bdev labels when expanding block device (`pr#61671 <https://github.com/ceph/ceph/pull/61671>`_, Adam Kupczyk)
* squid: os/bluestore: do cache locally compressor engines ever used (`pr#62143 <https://github.com/ceph/ceph/pull/62143>`_, Igor Fedotov, Adam Kupczyk)
* squid: os/bluestore: fix bdev expansion and more (`pr#62202 <https://github.com/ceph/ceph/pull/62202>`_, Igor Fedotov)
* squid: os/bluestore: Fix BlueRocksEnv attempts to use POSIX (`pr#61111 <https://github.com/ceph/ceph/pull/61111>`_, Adam Kupczyk)
* squid: os/bluestore: Fix ExtentDecoderPartial::_consume_new_blob (`pr#62053 <https://github.com/ceph/ceph/pull/62053>`_, Adam Kupczyk)
* squid: os/bluestore: Fix race in BlueFS truncate / remove (`pr#62839 <https://github.com/ceph/ceph/pull/62839>`_, Adam Kupczyk)
* squid: os/bluestore: fix the problem that _estimate_log_size_N calculates the log size incorrectly (`pr#61891 <https://github.com/ceph/ceph/pull/61891>`_, Wang Linke)
* squid: os/bluestore: Make truncate() drop unused allocations (`pr#60240 <https://github.com/ceph/ceph/pull/60240>`_, Adam Kupczyk, Igor Fedotov)
* squid: os/bluestore: use block size (4K) as minimal allocation unit for dedicated DB/WAL volumes (`pr#62514 <https://github.com/ceph/ceph/pull/62514>`_, Igor Fedotov)
* squid: os: remove unused btrfs_ioctl.h and tests (`pr#60613 <https://github.com/ceph/ceph/pull/60613>`_, Casey Bodley)
* squid: osd/scheduler/OpSchedulerItem: Fix calculation of recovery latency counters (`pr#62802 <https://github.com/ceph/ceph/pull/62802>`_, Sridhar Seshasayee)
* squid: osd/scrub: additional configuration parameters to trigger scrub reschedule (`pr#62956 <https://github.com/ceph/ceph/pull/62956>`_, Ronen Friedman)
* squid: osd/scrub: always round up reported scrub duration (`pr#62995 <https://github.com/ceph/ceph/pull/62995>`_, Ronen Friedman)
* squid: osd/scrub: clarify that osd_scrub_auto_repair_num_errors counts objects (`pr#64073 <https://github.com/ceph/ceph/pull/64073>`_, Ronen Friedman)
* squid: osd/scrub: discard repair_oinfo_oid() (`pr#61935 <https://github.com/ceph/ceph/pull/61935>`_, Ronen Friedman)
* squid: osd/scrub: register for 'osd_max_scrubs' config changes (`pr#61185 <https://github.com/ceph/ceph/pull/61185>`_, Ronen Friedman)
* squid: osd: fix for segmentation fault on OSD fast shutdown (`pr#57613 <https://github.com/ceph/ceph/pull/57613>`_, Md Mahamudur Rahaman Sajib)
* squid: osd: fix osd mclock queue item leak (`pr#62363 <https://github.com/ceph/ceph/pull/62363>`_, Samuel Just)
* squid: osd: full-object read CRC mismatch due to 'truncate' modifying oi.size w/o clearing 'data_digest' (`pr#57586 <https://github.com/ceph/ceph/pull/57586>`_, Samuel Just, Nitzan Mordechai, Matan Breizman, jiawd)
* squid: osd: optimize extent comparison in PrimaryLogPG (`pr#61337 <https://github.com/ceph/ceph/pull/61337>`_, Dongdong Tao)
* squid: OSD: Split osd_recovery_sleep into settings applied to degraded or clean PGs (`pr#62400 <https://github.com/ceph/ceph/pull/62400>`_, Md Mahamudur Rahaman Sajib)
* squid: osd_types: Restore new_object marking for delete missing entries (`pr#63154 <https://github.com/ceph/ceph/pull/63154>`_, Nitzan Mordechai)
* squid: OSDMonitor: exclude destroyed OSDs from "ceph node ls" output (`pr#62327 <https://github.com/ceph/ceph/pull/62327>`_, Nitzan Mordechai)
* squid: PendingReleaseNotes; doc/rados/operations: document "rm-pg-upmap-primary-{all}" commands (`pr#62467 <https://github.com/ceph/ceph/pull/62467>`_, Laura Flores)
* squid: PGMap: remove pool max_avail scale factor (`pr#62437 <https://github.com/ceph/ceph/pull/62437>`_, Michael J. Kidd)
* squid: pybind/ceph_argparse: Fix error message for ceph tell command (`pr#59005 <https://github.com/ceph/ceph/pull/59005>`_, Neeraj Pratap Singh)
* squid: pybind/mgr/mgr_module: turn off all automatic transactions (`pr#61854 <https://github.com/ceph/ceph/pull/61854>`_, Patrick Donnelly)
* squid: pybind/mgr: disable sqlite3/python autocommit (`pr#57189 <https://github.com/ceph/ceph/pull/57189>`_, Patrick Donnelly)
* squid: python-common: fix mypy errors in earmarking.py (`pr#63911 <https://github.com/ceph/ceph/pull/63911>`_, John Mulligan, Avan Thakkar)
* squid: qa/mgr/dashboard: fix test race condition (`pr#59698 <https://github.com/ceph/ceph/pull/59698>`_, Nizamudeen A, Ernesto Puerta)
* squid: qa/multisite: add extra checkpoints in datalog_autotrim testcase (`pr#62306 <https://github.com/ceph/ceph/pull/62306>`_, Shilpa Jagannath, Adam C. Emerson)
* squid: qa/rados/dashboard: Add PG_DEGRADED to ignorelist (`pr#61281 <https://github.com/ceph/ceph/pull/61281>`_, Aishwarya Mathuria)
* squid: qa/rgw: bump keystone/barbican from 2023.1 to 2024.1 (`pr#61023 <https://github.com/ceph/ceph/pull/61023>`_, Casey Bodley)
* squid: qa/rgw: bump maven version in hadoop task to resolve 404 Not Found (`pr#63928 <https://github.com/ceph/ceph/pull/63928>`_, Casey Bodley)
* squid: qa/rgw: configure 'iam root' accounts outside of rgw/verify (`pr#62033 <https://github.com/ceph/ceph/pull/62033>`_, Casey Bodley)
* squid: qa/rgw: fix perl tests missing Amazon::S3 module (`pr#64227 <https://github.com/ceph/ceph/pull/64227>`_, Mark Kogan, Adam C. Emerson)
* squid: qa/rgw: fix user cleanup in s3tests task (`pr#62365 <https://github.com/ceph/ceph/pull/62365>`_, Casey Bodley)
* squid: qa/rgw: run verify tests with garbage collection disabled (`pr#62954 <https://github.com/ceph/ceph/pull/62954>`_, Casey Bodley)
* squid: qa/standalone/mon/mon_cluster_log.sh: retry check for log line (`pr#61475 <https://github.com/ceph/ceph/pull/61475>`_, Shraddha Agrawal, Naveen Naidu)
* squid: qa/standalone/scrub: fix osd-scrub-test.sh (`pr#62974 <https://github.com/ceph/ceph/pull/62974>`_, Ronen Friedman)
* squid: qa/standalone/scrub: fix TEST_periodic_scrub_replicated (`pr#61118 <https://github.com/ceph/ceph/pull/61118>`_, Ronen Friedman)
* squid: qa/suites/orch/cephadm: add PG_DEGRADED to ignorelist (`pr#63054 <https://github.com/ceph/ceph/pull/63054>`_, Shraddha Agrawal)
* squid: qa/suites/rados/verify/validater/valgrind: increase op thread timeout (`pr#60912 <https://github.com/ceph/ceph/pull/60912>`_, Matan Breizman, Laura Flores)
* squid: qa/suites/upgrade/reef-x: sync log-ignorelist with quincy-x (`pr#61335 <https://github.com/ceph/ceph/pull/61335>`_, Ilya Dryomov, Pere Diaz Bou)
* squid: qa/suites/upgrade/{quincy|reef}-x skip TestClsRbd.mirror_snapshot test (`pr#60375 <https://github.com/ceph/ceph/pull/60375>`_, Mohit Agrawal)
* squid: qa/suites/upgrade: ignore PG_AVAILABILITY and MON_DOWN for quincy-x and reef-x upgrade suites (`pr#59245 <https://github.com/ceph/ceph/pull/59245>`_, Laura Flores)
* squid: qa/suites: wait longer before stopping OSDs with valgrind (`pr#63718 <https://github.com/ceph/ceph/pull/63718>`_, Nitzan Mordechai)
* squid: qa/tasks/fwd_scrub: remove unnecessary traceback (`pr#60651 <https://github.com/ceph/ceph/pull/60651>`_, Neeraj Pratap Singh)
* squid: qa/tasks: improve ignorelist for thrashing OSDs (`pr#61864 <https://github.com/ceph/ceph/pull/61864>`_, Laura Flores)
* squid: qa/tasks: Include stderr on tasks badness check (`pr#61435 <https://github.com/ceph/ceph/pull/61435>`_, Christopher Hoffman, Ilya Dryomov)
* squid: qa/tasks: watchdog should terminate thrasher (`pr#59191 <https://github.com/ceph/ceph/pull/59191>`_, Nitzan Mordechai)
* squid: qa/workunits/mon: ensure election strategy is "connectivity" for stretch mode (`pr#61496 <https://github.com/ceph/ceph/pull/61496>`_, Laura Flores)
* squid: qa/workunits/rbd: wait for resize to be applied in rbd-nbd (`pr#62219 <https://github.com/ceph/ceph/pull/62219>`_, Ilya Dryomov)
* squid: qa: Add ignorelist entries for reef-x tests (`pr#60618 <https://github.com/ceph/ceph/pull/60618>`_, Brad Hubbard)
* squid: qa: barbican: restrict python packages with upper-constraints (`pr#59327 <https://github.com/ceph/ceph/pull/59327>`_, Tobias Urdin)
* squid: qa: failfast mount for better performance and unblock `fs volume ls` (`pr#59919 <https://github.com/ceph/ceph/pull/59919>`_, Milind Changire)
* squid: qa: fix test failure test_cephfs_mirror_cancel_mirroring_and_readd (`pr#60181 <https://github.com/ceph/ceph/pull/60181>`_, Jos Collin)
* squid: qa: increase the http.maxRequestBuffer to 100MB and enable the git debug logs (`pr#60835 <https://github.com/ceph/ceph/pull/60835>`_, Xiubo Li)
* squid: qa: restrict 'perf dump' on active mds only (`pr#60975 <https://github.com/ceph/ceph/pull/60975>`_, Jos Collin)
* squid: qa: wait for file creation before changing mode (`issue#67408 <http://tracker.ceph.com/issues/67408>`_, `pr#59685 <https://github.com/ceph/ceph/pull/59685>`_, Venky Shankar)
* squid: rados/test_crash.sh: add PG_DEGRADED to ignorelist (`pr#62395 <https://github.com/ceph/ceph/pull/62395>`_, Shraddha Agrawal)
* squid: radosgw-admin: 'user create' rejects uids matching the account id format (`pr#60980 <https://github.com/ceph/ceph/pull/60980>`_, Casey Bodley)
* squid: radosgw-admin: allow 'sync group pipe modify' with existing user (`pr#60979 <https://github.com/ceph/ceph/pull/60979>`_, Casey Bodley)
* squid: radosgw-admin: bucket link/unlink support accounts (`pr#60982 <https://github.com/ceph/ceph/pull/60982>`_, Casey Bodley)
* squid: radosgw-admin: lower default thread pool size (`pr#62155 <https://github.com/ceph/ceph/pull/62155>`_, Casey Bodley)
* squid: rbd-mirror: fix possible recursive lock of ImageReplayer::m_lock (`pr#62042 <https://github.com/ceph/ceph/pull/62042>`_, N Balachandran)
* squid: rbd-mirror: release lock before calling m_async_op_tracker.finish_op() (`pr#64092 <https://github.com/ceph/ceph/pull/64092>`_, VinayBhaskar-V)
* squid: rbd: display mirror state creating (`pr#62940 <https://github.com/ceph/ceph/pull/62940>`_, N Balachandran)
* squid: rbd: open images in read-only mode for "rbd mirror pool status --verbose" (`pr#61170 <https://github.com/ceph/ceph/pull/61170>`_, Ilya Dryomov)
* squid: Revert "rgw/auth: Fix the return code returned by AuthStrategy," (`pr#61162 <https://github.com/ceph/ceph/pull/61162>`_, Casey Bodley, Pritha Srivastava)
* squid: rgw-admin: report correct error code for non-existent bucket on deletion (`pr#63405 <https://github.com/ceph/ceph/pull/63405>`_, Seena Fallah)
* squid: rgw/abortmp: Race condition on AbortMultipartUpload (`pr#61134 <https://github.com/ceph/ceph/pull/61134>`_, Casey Bodley, Artem Vasilev)
* squid: rgw/async/notifications: use common async waiter in pubsub push (`pr#62337 <https://github.com/ceph/ceph/pull/62337>`_, Yuval Lifshitz, Casey Bodley)
* squid: rgw/d3n: fix valgrind invalid read during exit (`pr#63438 <https://github.com/ceph/ceph/pull/63438>`_, Mark Kogan)
* squid: rgw/iam: add policy evaluation for Arn-based Conditions (`pr#62435 <https://github.com/ceph/ceph/pull/62435>`_, Casey Bodley)
* squid: rgw/iam: correcting the caps for OIDC Provider for a user (`pr#62892 <https://github.com/ceph/ceph/pull/62892>`_, Pritha Srivastava)
* squid: rgw/lc: delete expired delete-marker when processing Expiration lc action with Days specified (`pr#60783 <https://github.com/ceph/ceph/pull/60783>`_, Juan Zhu)
* squid: rgw/lc: make lc worker thread name shorter (`pr#61484 <https://github.com/ceph/ceph/pull/61484>`_, lightmelodies)
* squid: rgw/log: Fix crash during shutdown with ops-log enable (`pr#62134 <https://github.com/ceph/ceph/pull/62134>`_, kchheda3)
* squid: rgw/multisite: fix forwarded requests for tenanted buckets (`pr#62310 <https://github.com/ceph/ceph/pull/62310>`_, Shilpa Jagannath, Adam C. Emerson)
* squid: rgw/multisite: Fix use-after-move in retry logic in logbacking (`pr#61330 <https://github.com/ceph/ceph/pull/61330>`_, Adam Emerson)
* squid: rgw/multisite: handle errors properly in RGWDataFullSyncSingleEntryCR() (`pr#62307 <https://github.com/ceph/ceph/pull/62307>`_, Shilpa Jagannath, Adam C. Emerson)
* squid: rgw/notification: add rgw notification specific debug log subsystem (`pr#60784 <https://github.com/ceph/ceph/pull/60784>`_, Yuval Lifshitz, kchheda3)
* squid: rgw/notification: For kafka include user-id & password as part of the key along with endpoint for connection pooling (`pr#62495 <https://github.com/ceph/ceph/pull/62495>`_, kchheda3)
* squid: rgw/notification: Forward Topic & Notification creation request to master when notification_v2 enabled (`pr#61242 <https://github.com/ceph/ceph/pull/61242>`_, kchheda3)
* squid: rgw/posix: std::ignore return value of write() (`pr#61147 <https://github.com/ceph/ceph/pull/61147>`_, Casey Bodley)
* squid: rgw/rados: enable object deletion at rados pool quota (`pr#62093 <https://github.com/ceph/ceph/pull/62093>`_, Casey Bodley, Samuel Just)
* squid: rgw/rgw_rest: determine the domain uri prefix by rgw_transport_is_secure (`pr#63363 <https://github.com/ceph/ceph/pull/63363>`_, Xuehan Xu)
* squid: rgw/s3-notifications: use user-name/password topic attributes for SASL authentication (`pr#60952 <https://github.com/ceph/ceph/pull/60952>`_, Igor Gomon)
* squid: rgw/s3: remove local variable 'uri' that shadows member variable (`pr#62526 <https://github.com/ceph/ceph/pull/62526>`_, Casey Bodley)
* squid: rgw/s3select (`pr#62959 <https://github.com/ceph/ceph/pull/62959>`_, Gal Salomon, Seena Fallah, J. Eric Ivancich, galsalomon66)
* squid: rgw/sts: correcting authentication in case s3 ops are directed to a primary from secondary after  assumerole (`pr#63065 <https://github.com/ceph/ceph/pull/63065>`_, Pritha Srivastava)
* squid: rgw/sts: Implementation of validating JWT using modulus and exponent (`pr#63052 <https://github.com/ceph/ceph/pull/63052>`_, Pritha Srivastava)
* squid: rgw/sts: replacing load_stats with list_buckets (`pr#62386 <https://github.com/ceph/ceph/pull/62386>`_, Pritha Srivastava)
* squid: rgw: /admin/account APIs accept negative values (`pr#62131 <https://github.com/ceph/ceph/pull/62131>`_, Casey Bodley)
* squid: rgw: /admin/user api dumps account-related user info (`pr#61430 <https://github.com/ceph/ceph/pull/61430>`_, Casey Bodley)
* squid: rgw: add force option to radosgw-admin object rm (`pr#62748 <https://github.com/ceph/ceph/pull/62748>`_, J. Eric Ivancich)
* squid: rgw: add missing last_modified field to swift API (`pr#61546 <https://github.com/ceph/ceph/pull/61546>`_, Andrei Ivashchenko)
* squid: rgw: allow management of `accounts` user caps (`pr#61782 <https://github.com/ceph/ceph/pull/61782>`_, Richard Poole)
* squid: rgw: allow send bucket notification to multiple brokers of kafka cluster (`pr#61764 <https://github.com/ceph/ceph/pull/61764>`_, Hoai-Thu Vuong)
* squid: rgw: append query string to redirect URL if present (`pr#61159 <https://github.com/ceph/ceph/pull/61159>`_, Seena Fallah)
* squid: rgw: bucket creation fixes for multi-zonegroup scenario (`pr#62420 <https://github.com/ceph/ceph/pull/62420>`_, Seena Fallah, Shilpa Jagannath)
* squid: rgw: Changed discard buffer size (`pr#63710 <https://github.com/ceph/ceph/pull/63710>`_, Artem Vasilev)
* squid: rgw: cleanup orphaned bucket entry in <user>.buckets OMAP (`pr#62741 <https://github.com/ceph/ceph/pull/62741>`_, Jane Zhu)
* squid: rgw: Delete stale entries in bucket indexes while deleting obj (`pr#61062 <https://github.com/ceph/ceph/pull/61062>`_, Shasha Lu)
* squid: rgw: Deleting an object with null version fix (`pr#62309 <https://github.com/ceph/ceph/pull/62309>`_, Shilpa Jagannath, Adam C. Emerson, Kalpesh Pandya)
* squid: rgw: Don't crash on exceptions from pool listing (`pr#61306 <https://github.com/ceph/ceph/pull/61306>`_, Adam Emerson)
* squid: rgw: don't use merge_and_store_attrs() when recreating a bucket (`pr#64387 <https://github.com/ceph/ceph/pull/64387>`_, Casey Bodley)
* squid: rgw: exclude logging of request payer for 403 requests (`pr#62305 <https://github.com/ceph/ceph/pull/62305>`_, Seena Fallah, Adam C. Emerson)
* squid: rgw: Fix a bug in LCOpAction_Transition::check() (`pr#61532 <https://github.com/ceph/ceph/pull/61532>`_, Soumya Koduri)
* squid: rgw: fix bucket link operation (`pr#61051 <https://github.com/ceph/ceph/pull/61051>`_, Yehuda Sadeh)
* squid: rgw: fix bug with rgw-gap-list (`pr#62722 <https://github.com/ceph/ceph/pull/62722>`_, J. Eric Ivancich, Michael J. Kidd)
* squid: rgw: fix data corruption when rados op return ETIMEDOUT (`pr#61092 <https://github.com/ceph/ceph/pull/61092>`_, Shasha Lu)
* squid: rgw: fix to correctly store updated attrs in backend store after erasing an attr/attrs for delete ops on a bucket (`pr#61995 <https://github.com/ceph/ceph/pull/61995>`_, Soumya Koduri, Pritha Srivastava, Wei Wang)
* squid: rgw: fixing tempest ObjectTestACLs and ObjectACLsNegativeTest cases (`pr#62586 <https://github.com/ceph/ceph/pull/62586>`_, Alexey Odinokov)
* squid: rgw: implement x-amz-replication-status for PENDING & COMPLETED (`pr#60785 <https://github.com/ceph/ceph/pull/60785>`_, Alex Wojno, Casey Bodley)
* squid: rgw: keep the tails when copying object to itself (`pr#62711 <https://github.com/ceph/ceph/pull/62711>`_, Jane Zhu)
* squid: rgw: prefetch data from versioned object instance head (`pr#63193 <https://github.com/ceph/ceph/pull/63193>`_, Jane Zhu)
* squid: rgw: prevent crash in `radosgw-admin bucket object shard ...` (`pr#62884 <https://github.com/ceph/ceph/pull/62884>`_, J. Eric Ivancich)
* squid: rgw: PutObjectLockConfiguration can enable object lock on existing buckets (`pr#62064 <https://github.com/ceph/ceph/pull/62064>`_, Casey Bodley)
* squid: rgw: radoslist improvements primarily to better support gap list tool (`pr#62417 <https://github.com/ceph/ceph/pull/62417>`_, J. Eric Ivancich)
* squid: rgw: relax RGWPutACLs_ObjStore::get_params read data log (`pr#61161 <https://github.com/ceph/ceph/pull/61161>`_, Seena Fallah)
* squid: rgw: revert PR #41897 to allow multiple delete markers to be created (`pr#62740 <https://github.com/ceph/ceph/pull/62740>`_, Jane Zhu, Juan Zhu)
* squid: rgw: rgw_init_ioctx() adds set_pool_full_try() (`pr#62559 <https://github.com/ceph/ceph/pull/62559>`_, Casey Bodley)
* squid: rgw: S3 Delete Bucket Policy should return 204 on success (`pr#61431 <https://github.com/ceph/ceph/pull/61431>`_, Simon Jürgensmeyer)
* squid: rgw: skip empty check on non-owned buckets by zonegroup (`pr#62994 <https://github.com/ceph/ceph/pull/62994>`_, Seena Fallah)
* squid: rgw: sync fairness watcher reconnects on any error (`pr#62356 <https://github.com/ceph/ceph/pull/62356>`_, Oguzhan Ozmen, Casey Bodley)
* squid: rgw: Try to handle unwatch errors sensibly (`pr#62402 <https://github.com/ceph/ceph/pull/62402>`_, Adam C. Emerson)
* squid: rgw: use object ARN for InitMultipart permissions (`pr#62154 <https://github.com/ceph/ceph/pull/62154>`_, Casey Bodley)
* squid: rgw:lua: Skip the healthchecks and system requests from going to backend storage (`pr#62034 <https://github.com/ceph/ceph/pull/62034>`_, kchheda3)
* squid: RGW|Bucket Notification: fix for v2 topics rgw-admin list operation (`pr#62534 <https://github.com/ceph/ceph/pull/62534>`_, Casey Bodley, Ali Masarwa)
* squid: script/lib-build: Use clang 14 (`pr#61869 <https://github.com/ceph/ceph/pull/61869>`_, Matan Breizman)
* squid: src/common : proper handling of units in `strict_iec_cast` (`pr#60581 <https://github.com/ceph/ceph/pull/60581>`_, Neeraj Pratap Singh)
* squid: src/common: add guidance for deep-scrubbing ratio warning (`pr#62502 <https://github.com/ceph/ceph/pull/62502>`_, Zac Dover)
* squid: src/exporter: improve usage message (`pr#61331 <https://github.com/ceph/ceph/pull/61331>`_, Anthony D'Atri)
* squid: src/exporter: revert incorrect lines (`pr#61419 <https://github.com/ceph/ceph/pull/61419>`_, Zac Dover)
* squid: src/mon/OSDMonitor.cc: [Stretch Mode] WRN non-existent CRUSH location assigned to MON (`pr#62039 <https://github.com/ceph/ceph/pull/62039>`_, Kamoltat Sirivadhna)
* squid: suites/rados: cache tier deprecated, no need to keep the tests for it (`pr#62211 <https://github.com/ceph/ceph/pull/62211>`_, Nitzan Mordechai)
* squid: test/cls_2pc_queue: fix multi-threaded access to non-atomic variables (`pr#62311 <https://github.com/ceph/ceph/pull/62311>`_, Yuval Lifshitz, Adam C. Emerson)
* squid: test/libcephfs: copy DT_NEEDED entries from input libraries (`pr#63720 <https://github.com/ceph/ceph/pull/63720>`_, batrick)
* squid: test/librbd/test_notify.py: force line-buffered output (`pr#62752 <https://github.com/ceph/ceph/pull/62752>`_, Ilya Dryomov)
* squid: test/neorados: timeout test won't reconnect at timeout (`pr#61110 <https://github.com/ceph/ceph/pull/61110>`_, Nitzan Mordechai)
* squid: test/pybind: Clean whitespace. (Doc and test fixes) (`pr#61377 <https://github.com/ceph/ceph/pull/61377>`_, Paulo E. Castro)
* squid: test/rbd_mirror: clear Namespace::s_instance at the end of a test (`pr#61960 <https://github.com/ceph/ceph/pull/61960>`_, Ilya Dryomov)
* squid: test/rbd_mirror: flush watch/notify callbacks in TestImageReplayer (`pr#61958 <https://github.com/ceph/ceph/pull/61958>`_, Ilya Dryomov)
* squid: test/rgw/multisite: add meta checkpoint after bucket creation (`pr#60978 <https://github.com/ceph/ceph/pull/60978>`_, Casey Bodley)
* squid: test/scrub: only instruct clean PGs to scrub (`pr#61088 <https://github.com/ceph/ceph/pull/61088>`_, Ronen Friedman)
* squid: test: ceph daemon command with asok path (`pr#61482 <https://github.com/ceph/ceph/pull/61482>`_, Nitzan Mordechai)
* squid: test: Create ParallelPGMapper object before start threadpool (`pr#58919 <https://github.com/ceph/ceph/pull/58919>`_, Mohit Agrawal)
* squid: test: test_rados_tools compare output without trimming newline (`pr#59625 <https://github.com/ceph/ceph/pull/59625>`_, Nitzan Mordechai)
* squid: TEST_backfill_grow fails after finding "num_bytes mismatch" in osd log (`pr#60902 <https://github.com/ceph/ceph/pull/60902>`_, Mohit Agrawal)
* squid: tool/ceph-bluestore-tool: fix wrong keyword for 'free-fragmentation' command (`pr#62125 <https://github.com/ceph/ceph/pull/62125>`_, Igor Fedotov)
* squid: tools/ceph-objectstore-tool: tricks to tolerate disk errors for "pg export" command (`pr#62123 <https://github.com/ceph/ceph/pull/62123>`_, Igor Fedotov)
* squid: tools/cephfs-mirror: eliminate redundant ceph_close() call (`pr#61100 <https://github.com/ceph/ceph/pull/61100>`_, Igor Fedotov)
* squid: tools/objectstore: check for wrong coll open_collection (`pr#60861 <https://github.com/ceph/ceph/pull/60861>`_, Pere Diaz Bou)
* squid: tools/objectstore: check for wrong coll open_collection #58353 (`pr#58732 <https://github.com/ceph/ceph/pull/58732>`_, Pere Diaz Bou)
* squid: tools/rados: Fix extra NL in getxattr (`pr#60687 <https://github.com/ceph/ceph/pull/60687>`_, Adam Kupczyk)
* squid: win32_deps_build.sh: pin zlib tag (`pr#61631 <https://github.com/ceph/ceph/pull/61631>`_, Lucian Petrut)
* squid: workunit/dencoder: dencoder test forward incompat fix (`pr#61011 <https://github.com/ceph/ceph/pull/61011>`_, NitzanMordhai)

v19.2.2 Squid
=============
This is the second hotfix release in the Squid series.
We recommend that all users update to this release.

.. warning: Upgrade to Squid v19.2.2. Do not upgrade to Squid v19.2.1.

Notable Changes
---------------

* This hotfix release resolves an RGW data loss bug when CopyObject is used to copy an object onto itself.
  S3 clients typically do this when they want to change the metadata of an existing object.
  Due to a regression caused by an earlier fix for https://tracker.ceph.com/issues/66286,
  any tail objects associated with such objects are erroneously marked for garbage collection.
  RGW deployments on Squid are encouraged to upgrade as soon as possible to minimize the damage.
  The experimental rgw-gap-list tool can help to identify damaged objects.

Changelog
---------

* squid: rgw: keep the tails when copying object to itself (`pr#62711 <https://github.com/ceph/ceph/pull/62711>`_, cbodley)

v19.2.1 Squid
=============
This is the first backport release in the Squid series.

.. warning: Do not upgrade to Squid v19.2.1. Upgrade instead to Squid v19.2.2.

Notable Changes
---------------

* CephFS: The command `fs subvolume create` now allows tagging subvolumes by supplying the option
  `--earmark` with a unique identifier needed for NFS or SMB services. The earmark
  string for a subvolume is empty by default. To remove an already present earmark,
  an empty string can be assigned to it. Additionally, the commands
  `ceph fs subvolume earmark set`, `ceph fs subvolume earmark get`, and
  `ceph fs subvolume earmark rm` have been added to set, get and remove earmark from a given subvolume.

* CephFS: Expanded removexattr support for CephFS virtual extended attributes.
  Previously one had to use setxattr to restore the default in order to "remove".
  You may now properly use removexattr to remove. You can also now remove layout
  on the root inode, which then will restore the layout to the default.

* RADOS: A performance bottleneck in the balancer mgr module has been fixed.

  Related Tracker: https://tracker.ceph.com/issues/68657

* RADOS: Based on tests performed at scale on an HDD-based Ceph cluster, it was found
  that scheduling with mClock was not optimal with multiple OSD shards. For
  example, in the test cluster with multiple OSD node failures, the client
  throughput was found to be inconsistent across test runs coupled with multiple
  reported slow requests. However, the same test with a single OSD shard and
  with multiple worker threads yielded significantly better results in terms of
  consistency of client and recovery throughput across multiple test runs.
  Therefore, as an interim measure until the issue with multiple OSD shards
  (or multiple mClock queues per OSD) is investigated and fixed, the following
  change to the default HDD OSD shard configuration is made:

    * `osd_op_num_shards_hdd = 1` (was 5)
    * `osd_op_num_threads_per_shard_hdd = 5` (was 1)

  For more details, see https://tracker.ceph.com/issues/66289.

* mgr/REST: The REST manager module will trim requests based on the 'max_requests' option.
  Without this feature, and in the absence of manual deletion of old requests,
  the accumulation of requests in the array can lead to Out Of Memory (OOM) issues,
  resulting in the Manager crashing.

Changelog
---------

* doc/rgw/notification: add missing admin commands (`pr#60609 <https://github.com/ceph/ceph/pull/60609>`_, Yuval Lifshitz)
* squid: [RGW] Fix the handling of HEAD requests that do not comply with RFC standards (`pr#59123 <https://github.com/ceph/ceph/pull/59123>`_, liubingrun)
* squid: a series of optimizations for kerneldevice discard (`pr#59065 <https://github.com/ceph/ceph/pull/59065>`_, Adam Kupczyk, Joshua Baergen, Gabriel BenHanokh, Matt Vandermeulen)
* squid: Add Containerfile and build.sh to build it (`pr#60229 <https://github.com/ceph/ceph/pull/60229>`_, Dan Mick)
* squid: AsyncMessenger: Don't decrease l_msgr_active_connections if it is negative (`pr#60447 <https://github.com/ceph/ceph/pull/60447>`_, Mohit Agrawal)
* squid: blk/aio: fix long batch (64+K entries) submission (`pr#58676 <https://github.com/ceph/ceph/pull/58676>`_, Yingxin Cheng, Igor Fedotov, Adam Kupczyk, Robin Geuze)
* squid: blk/KernelDevice: using join() to wait thread end is more safe (`pr#60616 <https://github.com/ceph/ceph/pull/60616>`_, Yite Gu)
* squid: bluestore/bluestore_types: avoid heap-buffer-overflow in another way to keep code uniformity (`pr#58816 <https://github.com/ceph/ceph/pull/58816>`_, Rongqi Sun)
* squid: ceph-bluestore-tool: Fixes for multilple bdev label (`pr#59967 <https://github.com/ceph/ceph/pull/59967>`_, Adam Kupczyk, Igor Fedotov)
* squid: ceph-volume: add call to `ceph-bluestore-tool zap-device` (`pr#59968 <https://github.com/ceph/ceph/pull/59968>`_, Guillaume Abrioux)
* squid: ceph-volume: add new class UdevData (`pr#60091 <https://github.com/ceph/ceph/pull/60091>`_, Guillaume Abrioux)
* squid: ceph-volume: add TPM2 token enrollment support for encrypted OSDs (`pr#59196 <https://github.com/ceph/ceph/pull/59196>`_, Guillaume Abrioux)
* squid: ceph-volume: do not convert LVs's symlink to real path (`pr#58954 <https://github.com/ceph/ceph/pull/58954>`_, Guillaume Abrioux)
* squid: ceph-volume: do source devices zapping if they're detached (`pr#58964 <https://github.com/ceph/ceph/pull/58964>`_, Guillaume Abrioux, Igor Fedotov)
* squid: ceph-volume: drop unnecessary call to `get_single_lv()` (`pr#60353 <https://github.com/ceph/ceph/pull/60353>`_, Guillaume Abrioux)
* squid: ceph-volume: fix dmcrypt activation regression (`pr#60734 <https://github.com/ceph/ceph/pull/60734>`_, Guillaume Abrioux)
* squid: ceph-volume: fix generic activation with raw osds (`pr#59598 <https://github.com/ceph/ceph/pull/59598>`_, Guillaume Abrioux)
* squid: ceph-volume: fix OSD lvm/tpm2 activation (`pr#59953 <https://github.com/ceph/ceph/pull/59953>`_, Guillaume Abrioux)
* squid: ceph-volume: pass self.osd_id to create_id() call (`pr#59622 <https://github.com/ceph/ceph/pull/59622>`_, Guillaume Abrioux)
* squid: ceph-volume: switch over to new disk sorting behavior (`pr#59623 <https://github.com/ceph/ceph/pull/59623>`_, Guillaume Abrioux)
* squid: ceph.spec.in: we need jsonnet for all distroes for make check (`pr#60075 <https://github.com/ceph/ceph/pull/60075>`_, Kyr Shatskyy)
* squid: cephadm/services/ingress: fixed keepalived config bug (`pr#58381 <https://github.com/ceph/ceph/pull/58381>`_, Bernard Landon)
* Squid: cephadm: bootstrap should not have "This is a development version of cephadm" message (`pr#60880 <https://github.com/ceph/ceph/pull/60880>`_, Shweta Bhosale)
* squid: cephadm: emit warning if daemon's image is not to be used (`pr#59929 <https://github.com/ceph/ceph/pull/59929>`_, Matthew Vernon)
* squid: cephadm: fix apparmor profiles with spaces in the names (`pr#58542 <https://github.com/ceph/ceph/pull/58542>`_, John Mulligan)
* squid: cephadm: pull container images from quay.io (`pr#60354 <https://github.com/ceph/ceph/pull/60354>`_, Guillaume Abrioux)
* squid: cephadm: Support Docker Live Restore (`pr#59933 <https://github.com/ceph/ceph/pull/59933>`_, Michal Nasiadka)
* squid: cephadm: update default image and latest stable release (`pr#59827 <https://github.com/ceph/ceph/pull/59827>`_, Adam King)
* squid: cephfs,mon: fix bugs related to updating MDS caps (`pr#59672 <https://github.com/ceph/ceph/pull/59672>`_, Rishabh Dave)
* squid: cephfs-shell: excute cmd 'rmdir_helper' reported error (`pr#58810 <https://github.com/ceph/ceph/pull/58810>`_, teng jie)
* squid: cephfs: Fixed a bug in the readdir_cache_cb function that may have us… (`pr#58804 <https://github.com/ceph/ceph/pull/58804>`_, Tod Chen)
* squid: cephfs_mirror: provide metrics for last successful snapshot sync (`pr#59070 <https://github.com/ceph/ceph/pull/59070>`_, Jos Collin)
* squid: cephfs_mirror: update peer status for invalid metadata in remote snapshot (`pr#59406 <https://github.com/ceph/ceph/pull/59406>`_, Jos Collin)
* squid: cephfs_mirror: use snapdiff api for incremental syncing (`pr#58984 <https://github.com/ceph/ceph/pull/58984>`_, Jos Collin)
* squid: client: calls to _ll_fh_exists() should hold client_lock (`pr#59487 <https://github.com/ceph/ceph/pull/59487>`_, Venky Shankar)
* squid: client: check mds down status before getting mds_gid_t from mdsmap (`pr#58587 <https://github.com/ceph/ceph/pull/58587>`_, Yite Gu, Dhairya Parmar)
* squid: cls/user: reset stats only returns marker when truncated (`pr#60164 <https://github.com/ceph/ceph/pull/60164>`_, Casey Bodley)
* squid: cmake: use ExternalProjects to build isa-l and isa-l_crypto libraries (`pr#60107 <https://github.com/ceph/ceph/pull/60107>`_, Casey Bodley)
* squid: common,osd: Use last valid OSD IOPS value if measured IOPS is unrealistic (`pr#60660 <https://github.com/ceph/ceph/pull/60660>`_, Sridhar Seshasayee)
* squid: common/dout: fix FTBFS on GCC 14 (`pr#59055 <https://github.com/ceph/ceph/pull/59055>`_, Radoslaw Zarzynski)
* squid: common/options: Change HDD OSD shard configuration defaults for mClock (`pr#59973 <https://github.com/ceph/ceph/pull/59973>`_, Sridhar Seshasayee)
* squid: corpus: update submodule with mark cls_rgw_reshard_entry forward_inco… (`pr#58923 <https://github.com/ceph/ceph/pull/58923>`_, NitzanMordhai)
* squid: crimson/os/seastore/cached_extent: add the "refresh" ability to lba mappings (`pr#58957 <https://github.com/ceph/ceph/pull/58957>`_, Xuehan Xu)
* squid: crimson/os/seastore/lba_manager: do batch mapping allocs when remapping multiple mappings (`pr#58820 <https://github.com/ceph/ceph/pull/58820>`_, Xuehan Xu)
* squid: crimson/os/seastore/onode: add hobject_t into Onode (`pr#58830 <https://github.com/ceph/ceph/pull/58830>`_, Xuehan Xu)
* squid: crimson/os/seastore/transaction_manager: consider inconsistency between backrefs and lbas acceptable when cleaning segments (`pr#58837 <https://github.com/ceph/ceph/pull/58837>`_, Xuehan Xu)
* squid: crimson/os/seastore: add checksum offload to RBM (`pr#59298 <https://github.com/ceph/ceph/pull/59298>`_, Myoungwon Oh)
* squid: crimson/os/seastore: add writer level stats to RBM (`pr#58828 <https://github.com/ceph/ceph/pull/58828>`_, Myoungwon Oh)
* squid: crimson/os/seastore: track transactions/conflicts/outstanding periodically (`pr#58835 <https://github.com/ceph/ceph/pull/58835>`_, Yingxin Cheng)
* squid: crimson/osd/pg_recovery: push the iteration forward after finding unfound objects when starting primary recoveries (`pr#58958 <https://github.com/ceph/ceph/pull/58958>`_, Xuehan Xu)
* squid: crimson: access coll_map under alien tp with a lock (`pr#58841 <https://github.com/ceph/ceph/pull/58841>`_, Samuel Just)
* squid: crimson: audit and correct epoch captured by IOInterruptCondition (`pr#58839 <https://github.com/ceph/ceph/pull/58839>`_, Samuel Just)
* squid: crimson: simplify obc loading by locking excl for load and demoting to needed lock (`pr#58905 <https://github.com/ceph/ceph/pull/58905>`_, Matan Breizman, Samuel Just)
* squid: debian pkg: record python3-packaging dependency for ceph-volume (`pr#59202 <https://github.com/ceph/ceph/pull/59202>`_, Kefu Chai, Thomas Lamprecht)
* squid: doc,mailmap: update my email / association to ibm (`pr#60338 <https://github.com/ceph/ceph/pull/60338>`_, Patrick Donnelly)
* squid: doc/ceph-volume: add spillover fix procedure (`pr#59540 <https://github.com/ceph/ceph/pull/59540>`_, Zac Dover)
* squid: doc/cephadm: add malformed-JSON removal instructions (`pr#59663 <https://github.com/ceph/ceph/pull/59663>`_, Zac Dover)
* squid: doc/cephadm: Clarify "Deploying a new Cluster" (`pr#60809 <https://github.com/ceph/ceph/pull/60809>`_, Zac Dover)
* squid: doc/cephadm: clean "Adv. OSD Service Specs" (`pr#60679 <https://github.com/ceph/ceph/pull/60679>`_, Zac Dover)
* squid: doc/cephadm: correct "ceph orch apply" command (`pr#60432 <https://github.com/ceph/ceph/pull/60432>`_, Zac Dover)
* squid: doc/cephadm: how to get exact size_spec from device (`pr#59430 <https://github.com/ceph/ceph/pull/59430>`_, Zac Dover)
* squid: doc/cephadm: link to "host pattern" matching sect (`pr#60644 <https://github.com/ceph/ceph/pull/60644>`_, Zac Dover)
* squid: doc/cephadm: Update operations.rst (`pr#60637 <https://github.com/ceph/ceph/pull/60637>`_, rhkelson)
* squid: doc/cephfs: add cache pressure information (`pr#59148 <https://github.com/ceph/ceph/pull/59148>`_, Zac Dover)
* squid: doc/cephfs: add doc for disabling mgr/volumes plugin (`pr#60496 <https://github.com/ceph/ceph/pull/60496>`_, Rishabh Dave)
* squid: doc/cephfs: edit "Disabling Volumes Plugin" (`pr#60467 <https://github.com/ceph/ceph/pull/60467>`_, Zac Dover)
* squid: doc/cephfs: edit "Layout Fields" text (`pr#59021 <https://github.com/ceph/ceph/pull/59021>`_, Zac Dover)
* squid: doc/cephfs: edit 3rd 3rd of mount-using-kernel-driver (`pr#61080 <https://github.com/ceph/ceph/pull/61080>`_, Zac Dover)
* squid: doc/cephfs: improve "layout fields" text (`pr#59250 <https://github.com/ceph/ceph/pull/59250>`_, Zac Dover)
* squid: doc/cephfs: improve cache-configuration.rst (`pr#59214 <https://github.com/ceph/ceph/pull/59214>`_, Zac Dover)
* squid: doc/cephfs: rearrange subvolume group information (`pr#60435 <https://github.com/ceph/ceph/pull/60435>`_, Indira Sawant)
* squid: doc/cephfs: s/mountpoint/mount point/ (`pr#59294 <https://github.com/ceph/ceph/pull/59294>`_, Zac Dover)
* squid: doc/cephfs: s/mountpoint/mount point/ (`pr#59289 <https://github.com/ceph/ceph/pull/59289>`_, Zac Dover)
* squid: doc/cephfs: use 'p' flag to set layouts or quotas (`pr#60482 <https://github.com/ceph/ceph/pull/60482>`_, TruongSinh Tran-Nguyen)
* squid: doc/dev/peering: Change acting set num (`pr#59062 <https://github.com/ceph/ceph/pull/59062>`_, qn2060)
* squid: doc/dev/release-checklist: check telemetry validation (`pr#59813 <https://github.com/ceph/ceph/pull/59813>`_, Yaarit Hatuka)
* squid: doc/dev/release-checklists.rst: enable rtd for squid (`pr#59812 <https://github.com/ceph/ceph/pull/59812>`_, Neha Ojha)
* squid: doc/dev/release-process.rst: New container build/release process (`pr#60971 <https://github.com/ceph/ceph/pull/60971>`_, Dan Mick)
* squid: doc/dev: add "activate latest release" RTD step (`pr#59654 <https://github.com/ceph/ceph/pull/59654>`_, Zac Dover)
* squid: doc/dev: instruct devs to backport (`pr#61063 <https://github.com/ceph/ceph/pull/61063>`_, Zac Dover)
* squid: doc/dev: remove "Stable Releases and Backports" (`pr#60272 <https://github.com/ceph/ceph/pull/60272>`_, Zac Dover)
* squid: doc/glossary.rst: add "Dashboard Plugin" (`pr#60896 <https://github.com/ceph/ceph/pull/60896>`_, Zac Dover)
* squid: doc/glossary: add "ceph-ansible" (`pr#59007 <https://github.com/ceph/ceph/pull/59007>`_, Zac Dover)
* squid: doc/glossary: add "flapping OSD" (`pr#60864 <https://github.com/ceph/ceph/pull/60864>`_, Zac Dover)
* squid: doc/glossary: add "object storage" (`pr#59424 <https://github.com/ceph/ceph/pull/59424>`_, Zac Dover)
* squid: doc/glossary: add "PLP" to glossary (`pr#60503 <https://github.com/ceph/ceph/pull/60503>`_, Zac Dover)
* squid: doc/governance: add exec council responsibilites (`pr#60139 <https://github.com/ceph/ceph/pull/60139>`_, Zac Dover)
* squid: doc/governance: add Zac Dover's updated email (`pr#60134 <https://github.com/ceph/ceph/pull/60134>`_, Zac Dover)
* squid: doc/install: Keep the name field of the created user consistent with … (`pr#59756 <https://github.com/ceph/ceph/pull/59756>`_, hejindong)
* squid: doc/man: edit ceph-bluestore-tool.rst (`pr#59682 <https://github.com/ceph/ceph/pull/59682>`_, Zac Dover)
* squid: doc/mds: improve wording (`pr#59585 <https://github.com/ceph/ceph/pull/59585>`_, Piotr Parczewski)
* squid: doc/mgr/dashboard: fix TLS typo (`pr#59031 <https://github.com/ceph/ceph/pull/59031>`_, Mindy Preston)
* squid: doc/rados/operations: Improve health-checks.rst (`pr#59582 <https://github.com/ceph/ceph/pull/59582>`_, Anthony D'Atri)
* squid: doc/rados/troubleshooting: Improve log-and-debug.rst (`pr#60824 <https://github.com/ceph/ceph/pull/60824>`_, Anthony D'Atri)
* squid: doc/rados: add "pgs not deep scrubbed in time" info (`pr#59733 <https://github.com/ceph/ceph/pull/59733>`_, Zac Dover)
* squid: doc/rados: add blaum_roth coding guidance (`pr#60537 <https://github.com/ceph/ceph/pull/60537>`_, Zac Dover)
* squid: doc/rados: add confval directives to health-checks (`pr#59871 <https://github.com/ceph/ceph/pull/59871>`_, Zac Dover)
* squid: doc/rados: add link to messenger v2 info in mon-lookup-dns.rst (`pr#59794 <https://github.com/ceph/ceph/pull/59794>`_, Zac Dover)
* squid: doc/rados: add osd_deep_scrub_interval setting operation (`pr#59802 <https://github.com/ceph/ceph/pull/59802>`_, Zac Dover)
* squid: doc/rados: correct "full ratio" note (`pr#60737 <https://github.com/ceph/ceph/pull/60737>`_, Zac Dover)
* squid: doc/rados: document unfound object cache-tiering scenario (`pr#59380 <https://github.com/ceph/ceph/pull/59380>`_, Zac Dover)
* squid: doc/rados: edit "Placement Groups Never Get Clean" (`pr#60046 <https://github.com/ceph/ceph/pull/60046>`_, Zac Dover)
* squid: doc/rados: fix sentences in health-checks (2 of x) (`pr#60931 <https://github.com/ceph/ceph/pull/60931>`_, Zac Dover)
* squid: doc/rados: fix sentences in health-checks (3 of x) (`pr#60949 <https://github.com/ceph/ceph/pull/60949>`_, Zac Dover)
* squid: doc/rados: make sentences agree in health-checks.rst (`pr#60920 <https://github.com/ceph/ceph/pull/60920>`_, Zac Dover)
* squid: doc/rados: standardize markup of "clean" (`pr#60500 <https://github.com/ceph/ceph/pull/60500>`_, Zac Dover)
* squid: doc/radosgw/multisite: fix Configuring Secondary Zones -> Updating the Period (`pr#60332 <https://github.com/ceph/ceph/pull/60332>`_, Casey Bodley)
* squid: doc/radosgw/qat-accel: Update and Add QATlib information (`pr#58874 <https://github.com/ceph/ceph/pull/58874>`_, Feng, Hualong)
* squid: doc/radosgw: Improve archive-sync-module.rst (`pr#60852 <https://github.com/ceph/ceph/pull/60852>`_, Anthony D'Atri)
* squid: doc/radosgw: Improve archive-sync-module.rst more (`pr#60867 <https://github.com/ceph/ceph/pull/60867>`_, Anthony D'Atri)
* squid: doc/radosgw: Improve config-ref.rst (`pr#59578 <https://github.com/ceph/ceph/pull/59578>`_, Anthony D'Atri)
* squid: doc/radosgw: improve qat-accel.rst (`pr#59179 <https://github.com/ceph/ceph/pull/59179>`_, Anthony D'Atri)
* squid: doc/radosgw: s/Poliicy/Policy/ (`pr#60707 <https://github.com/ceph/ceph/pull/60707>`_, Zac Dover)
* squid: doc/radosgw: update rgw_dns_name doc (`pr#60885 <https://github.com/ceph/ceph/pull/60885>`_, Zac Dover)
* squid: doc/rbd: add namespace information for mirror commands (`pr#60269 <https://github.com/ceph/ceph/pull/60269>`_, N Balachandran)
* squid: doc/README.md - add ordered list (`pr#59798 <https://github.com/ceph/ceph/pull/59798>`_, Zac Dover)
* squid: doc/README.md: create selectable commands (`pr#59834 <https://github.com/ceph/ceph/pull/59834>`_, Zac Dover)
* squid: doc/README.md: edit "Build Prerequisites" (`pr#59637 <https://github.com/ceph/ceph/pull/59637>`_, Zac Dover)
* squid: doc/README.md: improve formatting (`pr#59785 <https://github.com/ceph/ceph/pull/59785>`_, Zac Dover)
* squid: doc/README.md: improve formatting (`pr#59700 <https://github.com/ceph/ceph/pull/59700>`_, Zac Dover)
* squid: doc/rgw/account: Handling notification topics when migrating an existing user into an account (`pr#59491 <https://github.com/ceph/ceph/pull/59491>`_, Oguzhan Ozmen)
* squid: doc/rgw/d3n: pass cache dir volume to extra_container_args (`pr#59767 <https://github.com/ceph/ceph/pull/59767>`_, Mark Kogan)
* squid: doc/rgw/notification: clarified the notification_v2 behavior upon upg… (`pr#60662 <https://github.com/ceph/ceph/pull/60662>`_, Yuval Lifshitz)
* squid: doc/rgw/notification: persistent notification queue full behavior (`pr#59233 <https://github.com/ceph/ceph/pull/59233>`_, Yuval Lifshitz)
* squid: doc/start: add supported Squid distros (`pr#60557 <https://github.com/ceph/ceph/pull/60557>`_, Zac Dover)
* squid: doc/start: add vstart install guide (`pr#60461 <https://github.com/ceph/ceph/pull/60461>`_, Zac Dover)
* squid: doc/start: fix "are are" typo (`pr#60708 <https://github.com/ceph/ceph/pull/60708>`_, Zac Dover)
* squid: doc/start: separate package chart from container chart (`pr#60698 <https://github.com/ceph/ceph/pull/60698>`_, Zac Dover)
* squid: doc/start: update os-recommendations.rst (`pr#60766 <https://github.com/ceph/ceph/pull/60766>`_, Zac Dover)
* squid: doc: Correct link to Prometheus docs (`pr#59559 <https://github.com/ceph/ceph/pull/59559>`_, Matthew Vernon)
* squid: doc: Document the Windows CI job (`pr#60033 <https://github.com/ceph/ceph/pull/60033>`_, Lucian Petrut)
* squid: doc: Document which options are disabled by mClock (`pr#60671 <https://github.com/ceph/ceph/pull/60671>`_, Niklas Hambüchen)
* squid: doc: documenting the feature that scrub clear the entries from damage… (`pr#59078 <https://github.com/ceph/ceph/pull/59078>`_, Neeraj Pratap Singh)
* squid: doc: explain the consequence of enabling mirroring through monitor co… (`pr#60525 <https://github.com/ceph/ceph/pull/60525>`_, Jos Collin)
* squid: doc: fix email (`pr#60233 <https://github.com/ceph/ceph/pull/60233>`_, Ernesto Puerta)
* squid: doc: fix typo (`pr#59991 <https://github.com/ceph/ceph/pull/59991>`_, N Balachandran)
* squid: doc: Harmonize 'mountpoint' (`pr#59291 <https://github.com/ceph/ceph/pull/59291>`_, Anthony D'Atri)
* squid: doc: s/Whereas,/Although/ (`pr#60593 <https://github.com/ceph/ceph/pull/60593>`_, Zac Dover)
* squid: doc: SubmittingPatches-backports - remove backports team (`pr#60297 <https://github.com/ceph/ceph/pull/60297>`_, Zac Dover)
* squid: doc: Update "Getting Started" to link to start not install (`pr#59907 <https://github.com/ceph/ceph/pull/59907>`_, Matthew Vernon)
* squid: doc: update Key Idea in cephfs-mirroring.rst (`pr#60343 <https://github.com/ceph/ceph/pull/60343>`_, Jos Collin)
* squid: doc: update nfs doc for Kerberos setup of ganesha in Ceph (`pr#59939 <https://github.com/ceph/ceph/pull/59939>`_, Avan Thakkar)
* squid: doc: update tests-integration-testing-teuthology-workflow.rst (`pr#59548 <https://github.com/ceph/ceph/pull/59548>`_, Vallari Agrawal)
* squid: doc:update e-mail addresses governance (`pr#60084 <https://github.com/ceph/ceph/pull/60084>`_, Tobias Fischer)
* squid: docs/rados/operations/stretch-mode: warn device class is not supported (`pr#59099 <https://github.com/ceph/ceph/pull/59099>`_, Kamoltat Sirivadhna)
* squid: global: Call getnam_r with a 64KiB buffer on the heap (`pr#60127 <https://github.com/ceph/ceph/pull/60127>`_, Adam Emerson)
* squid: librados: use CEPH_OSD_FLAG_FULL_FORCE for IoCtxImpl::remove (`pr#59284 <https://github.com/ceph/ceph/pull/59284>`_, Chen Yuanrun)
* squid: librbd/crypto/LoadRequest: clone format for migration source image (`pr#60171 <https://github.com/ceph/ceph/pull/60171>`_, Ilya Dryomov)
* squid: librbd/crypto: fix issue when live-migrating from encrypted export (`pr#59145 <https://github.com/ceph/ceph/pull/59145>`_, Ilya Dryomov)
* squid: librbd/migration/HttpClient: avoid reusing ssl_stream after shut down (`pr#61095 <https://github.com/ceph/ceph/pull/61095>`_, Ilya Dryomov)
* squid: librbd/migration: prune snapshot extents in RawFormat::list_snaps() (`pr#59661 <https://github.com/ceph/ceph/pull/59661>`_, Ilya Dryomov)
* squid: librbd: avoid data corruption on flatten when object map is inconsistent (`pr#61168 <https://github.com/ceph/ceph/pull/61168>`_, Ilya Dryomov)
* squid: log: save/fetch thread name infra (`pr#60279 <https://github.com/ceph/ceph/pull/60279>`_, Milind Changire)
* squid: Make mon addrs consistent with mon info (`pr#60751 <https://github.com/ceph/ceph/pull/60751>`_, shenjiatong)
* squid: mds/QuiesceDbManager: get requested state of members before iterating… (`pr#58912 <https://github.com/ceph/ceph/pull/58912>`_, junxiang Mu)
* squid: mds: CInode::item_caps used in two different lists (`pr#56887 <https://github.com/ceph/ceph/pull/56887>`_, Dhairya Parmar)
* squid: mds: encode quiesce payload on demand (`pr#59517 <https://github.com/ceph/ceph/pull/59517>`_, Patrick Donnelly)
* squid: mds: find a new head for the batch ops when the head is dead (`pr#57494 <https://github.com/ceph/ceph/pull/57494>`_, Xiubo Li)
* squid: mds: fix session/client evict command (`pr#58727 <https://github.com/ceph/ceph/pull/58727>`_, Neeraj Pratap Singh)
* squid: mds: only authpin on wrlock when not a locallock (`pr#59097 <https://github.com/ceph/ceph/pull/59097>`_, Patrick Donnelly)
* squid: mgr/balancer: optimize 'balancer status detail' (`pr#60718 <https://github.com/ceph/ceph/pull/60718>`_, Laura Flores)
* squid: mgr/cephadm/services/ingress Fix HAProxy to listen on IPv4 and IPv6 (`pr#58515 <https://github.com/ceph/ceph/pull/58515>`_, Bernard Landon)
* squid: mgr/cephadm: add "original_weight" parameter to OSD class (`pr#59410 <https://github.com/ceph/ceph/pull/59410>`_, Adam King)
* squid: mgr/cephadm: add --no-exception-when-missing flag to cert-store cert/key get (`pr#59935 <https://github.com/ceph/ceph/pull/59935>`_, Adam King)
* squid: mgr/cephadm: add command to expose systemd units of all daemons (`pr#59931 <https://github.com/ceph/ceph/pull/59931>`_, Adam King)
* squid: mgr/cephadm: bump monitoring stacks version (`pr#58711 <https://github.com/ceph/ceph/pull/58711>`_, Nizamudeen A)
* squid: mgr/cephadm: make ssh keepalive settings configurable (`pr#59710 <https://github.com/ceph/ceph/pull/59710>`_, Adam King)
* squid: mgr/cephadm: redeploy when some dependency daemon is add/removed (`pr#58383 <https://github.com/ceph/ceph/pull/58383>`_, Redouane Kachach)
* squid: mgr/cephadm: Update multi-site configs before deploying  daemons on rgw service create (`pr#60321 <https://github.com/ceph/ceph/pull/60321>`_, Aashish Sharma)
* squid: mgr/cephadm: use host address while updating rgw zone endpoints (`pr#59948 <https://github.com/ceph/ceph/pull/59948>`_, Aashish Sharma)
* squid: mgr/client: validate connection before sending (`pr#58887 <https://github.com/ceph/ceph/pull/58887>`_, NitzanMordhai)
* squid: mgr/dashboard: add cephfs rename REST API (`pr#60620 <https://github.com/ceph/ceph/pull/60620>`_, Yite Gu)
* squid: mgr/dashboard: Add group field in nvmeof service form (`pr#59446 <https://github.com/ceph/ceph/pull/59446>`_, Afreen Misbah)
* squid: mgr/dashboard: add gw_groups support to nvmeof api (`pr#59751 <https://github.com/ceph/ceph/pull/59751>`_, Nizamudeen A)
* squid: mgr/dashboard: add gw_groups to all nvmeof endpoints (`pr#60310 <https://github.com/ceph/ceph/pull/60310>`_, Nizamudeen A)
* squid: mgr/dashboard: add restful api for creating crush rule with type of 'erasure' (`pr#59139 <https://github.com/ceph/ceph/pull/59139>`_, sunlan)
* squid: mgr/dashboard: Changes for Sign out text to Login out (`pr#58988 <https://github.com/ceph/ceph/pull/58988>`_, Prachi Goel)
* Squid: mgr/dashboard: Cloning subvolume not listing _nogroup if no subvolume (`pr#59951 <https://github.com/ceph/ceph/pull/59951>`_, Dnyaneshwari talwekar)
* squid: mgr/dashboard: custom image for kcli bootstrap script (`pr#59879 <https://github.com/ceph/ceph/pull/59879>`_, Pedro Gonzalez Gomez)
* squid: mgr/dashboard: Dashboard not showing Object/Overview correctly (`pr#59038 <https://github.com/ceph/ceph/pull/59038>`_, Aashish Sharma)
* squid: mgr/dashboard: Fix adding listener and null issue for groups (`pr#60078 <https://github.com/ceph/ceph/pull/60078>`_, Afreen Misbah)
* squid: mgr/dashboard: fix bucket get for s3 account owned bucket (`pr#60466 <https://github.com/ceph/ceph/pull/60466>`_, Nizamudeen A)
* squid: mgr/dashboard: fix ceph-users api doc (`pr#59140 <https://github.com/ceph/ceph/pull/59140>`_, Nizamudeen A)
* squid: mgr/dashboard: fix doc links in rgw-multisite (`pr#60154 <https://github.com/ceph/ceph/pull/60154>`_, Pedro Gonzalez Gomez)
* squid: mgr/dashboard: fix gateways section error:”404 - Not Found RGW Daemon not found: None” (`pr#60231 <https://github.com/ceph/ceph/pull/60231>`_, Aashish Sharma)
* squid: mgr/dashboard: fix group name bugs in the nvmeof API (`pr#60348 <https://github.com/ceph/ceph/pull/60348>`_, Nizamudeen A)
* squid: mgr/dashboard: fix handling NaN values in dashboard charts (`pr#59961 <https://github.com/ceph/ceph/pull/59961>`_, Aashish Sharma)
* squid: mgr/dashboard: fix lifecycle issues (`pr#60378 <https://github.com/ceph/ceph/pull/60378>`_, Pedro Gonzalez Gomez)
* squid: mgr/dashboard: Fix listener deletion (`pr#60292 <https://github.com/ceph/ceph/pull/60292>`_, Afreen Misbah)
* squid: mgr/dashboard: fix setting compression type while editing rgw zone (`pr#59970 <https://github.com/ceph/ceph/pull/59970>`_, Aashish Sharma)
* Squid: mgr/dashboard: Forbid snapshot name "." and any containing "/" (`pr#59995 <https://github.com/ceph/ceph/pull/59995>`_, Dnyaneshwari Talwekar)
* squid: mgr/dashboard: handle infinite values for pools (`pr#61096 <https://github.com/ceph/ceph/pull/61096>`_, Afreen)
* squid: mgr/dashboard: ignore exceptions raised when no cert/key found (`pr#60311 <https://github.com/ceph/ceph/pull/60311>`_, Nizamudeen A)
* squid: mgr/dashboard: Increase maximum namespace count to 1024 (`pr#59717 <https://github.com/ceph/ceph/pull/59717>`_, Afreen Misbah)
* squid: mgr/dashboard: introduce server side pagination for osds (`pr#60294 <https://github.com/ceph/ceph/pull/60294>`_, Nizamudeen A)
* squid: mgr/dashboard: mgr/dashboard: Select no device by default in EC profile (`pr#59811 <https://github.com/ceph/ceph/pull/59811>`_, Afreen Misbah)
* Squid: mgr/dashboard: multisite sync policy improvements (`pr#59965 <https://github.com/ceph/ceph/pull/59965>`_, Naman Munet)
* Squid: mgr/dashboard: NFS Export form fixes (`pr#59900 <https://github.com/ceph/ceph/pull/59900>`_, Dnyaneshwari Talwekar)
* squid: mgr/dashboard: Nvme mTLS support and service name changes (`pr#59819 <https://github.com/ceph/ceph/pull/59819>`_, Afreen Misbah)
* squid: mgr/dashboard: provide option to enable pool based mirroring mode while creating a pool (`pr#58638 <https://github.com/ceph/ceph/pull/58638>`_, Aashish Sharma)
* squid: mgr/dashboard: remove cherrypy_backports.py (`pr#60632 <https://github.com/ceph/ceph/pull/60632>`_, Nizamudeen A)
* Squid: mgr/dashboard: remove orch required decorator from host UI router (list) (`pr#59851 <https://github.com/ceph/ceph/pull/59851>`_, Naman Munet)
* squid: mgr/dashboard: Rephrase dedicated pool helper in rbd create form (`pr#59721 <https://github.com/ceph/ceph/pull/59721>`_, Aashish Sharma)
* Squid: mgr/dashboard: RGW multisite sync remove zones fix (`pr#59825 <https://github.com/ceph/ceph/pull/59825>`_, Naman Munet)
* squid: mgr/dashboard: rm nvmeof conf based on its daemon name (`pr#60604 <https://github.com/ceph/ceph/pull/60604>`_, Nizamudeen A)
* Squid: mgr/dashboard: service form hosts selection only show up to 10 entries (`pr#59760 <https://github.com/ceph/ceph/pull/59760>`_, Naman Munet)
* squid: mgr/dashboard: show non default realm sync status in rgw overview page (`pr#60232 <https://github.com/ceph/ceph/pull/60232>`_, Aashish Sharma)
* squid: mgr/dashboard: Show which daemons failed in CEPHADM_FAILED_DAEMON healthcheck (`pr#59597 <https://github.com/ceph/ceph/pull/59597>`_, Aashish Sharma)
* Squid: mgr/dashboard: sync policy's in Object >> Multi-site >> Sync-policy, does not show the zonegroup to which policy belongs to (`pr#60346 <https://github.com/ceph/ceph/pull/60346>`_, Naman Munet)
* Squid: mgr/dashboard: The subvolumes are missing from the dropdown menu on the "Create NFS export" page (`pr#60356 <https://github.com/ceph/ceph/pull/60356>`_, Dnyaneshwari Talwekar)
* Squid: mgr/dashboard: unable to edit pipe config for bucket level policy of bucket (`pr#60293 <https://github.com/ceph/ceph/pull/60293>`_, Naman Munet)
* squid: mgr/dashboard: Update nvmeof microcopies (`pr#59718 <https://github.com/ceph/ceph/pull/59718>`_, Afreen Misbah)
* squid: mgr/dashboard: update period after migrating to multi-site (`pr#59964 <https://github.com/ceph/ceph/pull/59964>`_, Aashish Sharma)
* squid: mgr/dashboard: update translations for squid (`pr#60367 <https://github.com/ceph/ceph/pull/60367>`_, Nizamudeen A)
* squid: mgr/dashboard: use grafana server instead of grafana-server in grafana 10.4.0 (`pr#59722 <https://github.com/ceph/ceph/pull/59722>`_, Aashish Sharma)
* Squid: mgr/dashboard: Wrong(half) uid is observed in dashboard when user created via cli contains $ in its name (`pr#59693 <https://github.com/ceph/ceph/pull/59693>`_, Dnyaneshwari Talwekar)
* squid: mgr/dashboard: Zone details showing incorrect data for data pool values and compression info for Storage Classes (`pr#59596 <https://github.com/ceph/ceph/pull/59596>`_, Aashish Sharma)
* Squid: mgr/dashboard: zonegroup level policy created at master zone did not sync to non-master zone (`pr#59892 <https://github.com/ceph/ceph/pull/59892>`_, Naman Munet)
* squid: mgr/nfs: generate user_id & access_key for apply_export(CephFS) (`pr#59896 <https://github.com/ceph/ceph/pull/59896>`_, Avan Thakkar, avanthakkar, John Mulligan)
* squid: mgr/orchestrator: fix encrypted flag handling in orch daemon add osd (`pr#59473 <https://github.com/ceph/ceph/pull/59473>`_, Yonatan Zaken)
* squid: mgr/rest: Trim  requests array and limit size (`pr#59372 <https://github.com/ceph/ceph/pull/59372>`_, Nitzan Mordechai)
* squid: mgr/rgw: Adding a retry config while calling zone_create() (`pr#59138 <https://github.com/ceph/ceph/pull/59138>`_, Kritik Sachdeva)
* squid: mgr/rgwam: use realm/zonegroup/zone method arguments for period update (`pr#59945 <https://github.com/ceph/ceph/pull/59945>`_, Aashish Sharma)
* squid: mgr/volumes: add earmarking for subvol (`pr#59894 <https://github.com/ceph/ceph/pull/59894>`_, Avan Thakkar)
* squid: Modify container/ software to support release containers and the promotion of prerelease containers (`pr#60962 <https://github.com/ceph/ceph/pull/60962>`_, Dan Mick)
* squid: mon/ElectionLogic: tie-breaker mon ignore proposal from marked down mon (`pr#58669 <https://github.com/ceph/ceph/pull/58669>`_, Kamoltat)
* squid: mon/MonClient: handle ms_handle_fast_authentication return (`pr#59306 <https://github.com/ceph/ceph/pull/59306>`_, Patrick Donnelly)
* squid: mon/OSDMonitor: Add force-remove-snap mon command (`pr#59402 <https://github.com/ceph/ceph/pull/59402>`_, Matan Breizman)
* squid: mon/OSDMonitor: fix get_min_last_epoch_clean() (`pr#55865 <https://github.com/ceph/ceph/pull/55865>`_, Matan Breizman)
* squid: mon: Remove any pg_upmap_primary mapping during remove a pool (`pr#58914 <https://github.com/ceph/ceph/pull/58914>`_, Mohit Agrawal)
* squid: msg: insert PriorityDispatchers in sorted position (`pr#58991 <https://github.com/ceph/ceph/pull/58991>`_, Casey Bodley)
* squid: node-proxy: fix a regression when processing the RedFish API (`pr#59997 <https://github.com/ceph/ceph/pull/59997>`_, Guillaume Abrioux)
* squid: node-proxy: make the daemon discover endpoints (`pr#58482 <https://github.com/ceph/ceph/pull/58482>`_, Guillaume Abrioux)
* squid: objclass: deprecate cls_cxx_gather (`pr#57819 <https://github.com/ceph/ceph/pull/57819>`_, Nitzan Mordechai)
* squid: orch: disk replacement enhancement (`pr#60486 <https://github.com/ceph/ceph/pull/60486>`_, Guillaume Abrioux)
* squid: orch: refactor boolean handling in drive group spec (`pr#59863 <https://github.com/ceph/ceph/pull/59863>`_, Guillaume Abrioux)
* squid: os/bluestore: enable async manual compactions (`pr#58740 <https://github.com/ceph/ceph/pull/58740>`_, Igor Fedotov)
* squid: os/bluestore: Fix BlueFS allocating bdev label reserved location (`pr#59969 <https://github.com/ceph/ceph/pull/59969>`_, Adam Kupczyk)
* squid: os/bluestore: Fix ceph-bluestore-tool allocmap command (`pr#60335 <https://github.com/ceph/ceph/pull/60335>`_, Adam Kupczyk)
* squid: os/bluestore: Fix repair of multilabel when collides with BlueFS (`pr#60336 <https://github.com/ceph/ceph/pull/60336>`_, Adam Kupczyk)
* squid: os/bluestore: Improve documentation introduced by #57722 (`pr#60893 <https://github.com/ceph/ceph/pull/60893>`_, Anthony D'Atri)
* squid: os/bluestore: Multiple bdev labels on main block device (`pr#59106 <https://github.com/ceph/ceph/pull/59106>`_, Adam Kupczyk)
* squid: os/bluestore: Mute warnings (`pr#59217 <https://github.com/ceph/ceph/pull/59217>`_, Adam Kupczyk)
* squid: os/bluestore: Warning added for slow operations and stalled read (`pr#59464 <https://github.com/ceph/ceph/pull/59464>`_, Md Mahamudur Rahaman Sajib)
* squid: osd/scheduler: add mclock queue length perfcounter (`pr#59035 <https://github.com/ceph/ceph/pull/59035>`_, zhangjianwei2)
* squid: osd/scrub: decrease default deep scrub chunk size (`pr#59791 <https://github.com/ceph/ceph/pull/59791>`_, Ronen Friedman)
* squid: osd/scrub: exempt only operator scrubs from max_scrubs limit (`pr#59020 <https://github.com/ceph/ceph/pull/59020>`_, Ronen Friedman)
* squid: osd/scrub: reduce osd_requested_scrub_priority default value (`pr#59885 <https://github.com/ceph/ceph/pull/59885>`_, Ronen Friedman)
* squid: osd: fix require_min_compat_client handling for msr rules (`pr#59492 <https://github.com/ceph/ceph/pull/59492>`_, Samuel Just, Radoslaw Zarzynski)
* squid: PeeringState.cc: Only populate want_acting when num_osds < bucket_max (`pr#59083 <https://github.com/ceph/ceph/pull/59083>`_, Kamoltat)
* squid: qa/cephadm: extend iscsi teuth test (`pr#59934 <https://github.com/ceph/ceph/pull/59934>`_, Adam King)
* squid: qa/cephfs: fix TestRenameCommand and unmount the clinet before failin… (`pr#59398 <https://github.com/ceph/ceph/pull/59398>`_, Xiubo Li)
* squid: qa/cephfs: ignore variant of MDS_UP_LESS_THAN_MAX (`pr#58788 <https://github.com/ceph/ceph/pull/58788>`_, Patrick Donnelly)
* squid: qa/distros: reinstall nvme-cli on centos 9 nodes (`pr#59471 <https://github.com/ceph/ceph/pull/59471>`_, Adam King)
* squid: qa/rgw/multisite: specify realm/zonegroup/zone args for 'account create' (`pr#59603 <https://github.com/ceph/ceph/pull/59603>`_, Casey Bodley)
* squid: qa/rgw: bump keystone/barbican from 2023.1 to 2024.1 (`pr#61023 <https://github.com/ceph/ceph/pull/61023>`_, Casey Bodley)
* squid: qa/rgw: fix s3 java tests by forcing gradle to run on Java 8 (`pr#61053 <https://github.com/ceph/ceph/pull/61053>`_, J. Eric Ivancich)
* squid: qa/rgw: force Hadoop to run under Java 1.8 (`pr#61120 <https://github.com/ceph/ceph/pull/61120>`_, J. Eric Ivancich)
* squid: qa/rgw: pull Apache artifacts from mirror instead of archive.apache.org (`pr#61101 <https://github.com/ceph/ceph/pull/61101>`_, J. Eric Ivancich)
* squid: qa/standalone/scrub: fix the searched-for text for snaps decode errors (`pr#58967 <https://github.com/ceph/ceph/pull/58967>`_, Ronen Friedman)
* squid: qa/standalone/scrub: increase status updates frequency (`pr#59974 <https://github.com/ceph/ceph/pull/59974>`_, Ronen Friedman)
* squid: qa/standalone/scrub: remove TEST_recovery_scrub_2 (`pr#60287 <https://github.com/ceph/ceph/pull/60287>`_, Ronen Friedman)
* squid: qa/suites/crimson-rados/perf: add ssh keys (`pr#61109 <https://github.com/ceph/ceph/pull/61109>`_, Nitzan Mordechai)
* squid: qa/suites/rados/thrash-old-clients: Add noscrub, nodeep-scrub to ignorelist (`pr#58629 <https://github.com/ceph/ceph/pull/58629>`_, Kamoltat)
* squid: qa/suites/rados/thrash-old-clients: test with N-2 releases on centos 9 (`pr#58607 <https://github.com/ceph/ceph/pull/58607>`_, Laura Flores)
* squid: qa/suites/rados/verify/validater: increase heartbeat grace timeout (`pr#58785 <https://github.com/ceph/ceph/pull/58785>`_, Sridhar Seshasayee)
* squid: qa/suites/rados: Cancel injectfull to allow cleanup (`pr#59156 <https://github.com/ceph/ceph/pull/59156>`_, Brad Hubbard)
* squid: qa/suites/rbd/iscsi: enable all supported container hosts (`pr#60089 <https://github.com/ceph/ceph/pull/60089>`_, Ilya Dryomov)
* squid: qa/suites: drop --show-reachable=yes from fs:valgrind tests (`pr#59068 <https://github.com/ceph/ceph/pull/59068>`_, Jos Collin)
* squid: qa/task: update alertmanager endpoints version (`pr#59930 <https://github.com/ceph/ceph/pull/59930>`_, Nizamudeen A)
* squid: qa/tasks/mgr/test_progress.py: deal with pre-exisiting pool (`pr#58263 <https://github.com/ceph/ceph/pull/58263>`_, Kamoltat)
* squid: qa/tasks/nvme_loop: update task to work with new nvme list format (`pr#61026 <https://github.com/ceph/ceph/pull/61026>`_, Adam King)
* squid: qa/upgrade: fix checks to make sure upgrade is still in progress (`pr#59472 <https://github.com/ceph/ceph/pull/59472>`_, Adam King)
* squid: qa: adjust expected io_opt in krbd_discard_granularity.t (`pr#59232 <https://github.com/ceph/ceph/pull/59232>`_, Ilya Dryomov)
* squid: qa: ignore container checkpoint/restore related selinux denials for c… (`issue#66640 <http://tracker.ceph.com/issues/66640>`_, `issue#67117 <http://tracker.ceph.com/issues/67117>`_, `pr#58808 <https://github.com/ceph/ceph/pull/58808>`_, Venky Shankar)
* squid: qa: load all dirfrags before testing altname recovery (`pr#59521 <https://github.com/ceph/ceph/pull/59521>`_, Patrick Donnelly)
* squid: qa: remove all bluestore signatures on devices (`pr#60021 <https://github.com/ceph/ceph/pull/60021>`_, Guillaume Abrioux)
* squid: qa: suppress __trans_list_add valgrind warning (`pr#58790 <https://github.com/ceph/ceph/pull/58790>`_, Patrick Donnelly)
* squid: RADOS: Generalize stretch mode pg temp handling to be usable without stretch mode (`pr#59084 <https://github.com/ceph/ceph/pull/59084>`_, Kamoltat)
* squid: rbd-mirror: use correct ioctx for namespace (`pr#59771 <https://github.com/ceph/ceph/pull/59771>`_, N Balachandran)
* squid: rbd: "rbd bench" always writes the same byte (`pr#59502 <https://github.com/ceph/ceph/pull/59502>`_, Ilya Dryomov)
* squid: rbd: amend "rbd {group,} rename" and "rbd mirror pool" command descriptions (`pr#59602 <https://github.com/ceph/ceph/pull/59602>`_, Ilya Dryomov)
* squid: rbd: handle --{group,image}-namespace in "rbd group image {add,rm}" (`pr#61172 <https://github.com/ceph/ceph/pull/61172>`_, Ilya Dryomov)
* squid: rgw/beast: optimize for accept when meeting error in listenning (`pr#60244 <https://github.com/ceph/ceph/pull/60244>`_, Mingyuan Liang, Casey Bodley)
* squid: rgw/http: finish_request() after logging errors (`pr#59439 <https://github.com/ceph/ceph/pull/59439>`_, Casey Bodley)
* squid: rgw/kafka: refactor topic creation to avoid rd_kafka_topic_name() (`pr#59754 <https://github.com/ceph/ceph/pull/59754>`_, Yuval Lifshitz)
* squid: rgw/lc: Fix lifecycle not working while bucket versioning is suspended (`pr#61138 <https://github.com/ceph/ceph/pull/61138>`_, Trang Tran)
* squid: rgw/multipart: use cls_version to avoid racing between part upload and multipart complete (`pr#59678 <https://github.com/ceph/ceph/pull/59678>`_, Jane Zhu)
* squid: rgw/multisite: metadata polling event based on unmodified mdlog_marker (`pr#60792 <https://github.com/ceph/ceph/pull/60792>`_, Shilpa Jagannath)
* squid: rgw/notifications: fixing radosgw-admin notification json (`pr#59302 <https://github.com/ceph/ceph/pull/59302>`_, Yuval Lifshitz)
* squid: rgw/notifications: free completion pointer using unique_ptr (`pr#59671 <https://github.com/ceph/ceph/pull/59671>`_, Yuval Lifshitz)
* squid: rgw/notify: visit() returns copy of owner string (`pr#59226 <https://github.com/ceph/ceph/pull/59226>`_, Casey Bodley)
* squid: rgw/rados: don't rely on IoCtx::get_last_version() for async ops (`pr#60065 <https://github.com/ceph/ceph/pull/60065>`_, Casey Bodley)
* squid: rgw: add s3select usage to log usage (`pr#59120 <https://github.com/ceph/ceph/pull/59120>`_, Seena Fallah)
* squid: rgw: decrement qlen/qactive perf counters on error (`pr#59670 <https://github.com/ceph/ceph/pull/59670>`_, Mark Kogan)
* squid: rgw: decrypt multipart get part when encrypted (`pr#60130 <https://github.com/ceph/ceph/pull/60130>`_, sungjoon-koh)
* squid: rgw: ignore zoneless default realm when not configured (`pr#59445 <https://github.com/ceph/ceph/pull/59445>`_, Casey Bodley)
* squid: rgw: load copy source bucket attrs in putobj (`pr#59413 <https://github.com/ceph/ceph/pull/59413>`_, Seena Fallah)
* squid: rgw: optimize bucket listing to skip past regions of namespaced entries (`pr#61070 <https://github.com/ceph/ceph/pull/61070>`_, J. Eric Ivancich)
* squid: rgw: revert account-related changes to get_iam_policy_from_attr() (`pr#59221 <https://github.com/ceph/ceph/pull/59221>`_, Casey Bodley)
* squid: rgw: RGWAccessKey::decode_json() preserves default value of 'active' (`pr#60823 <https://github.com/ceph/ceph/pull/60823>`_, Casey Bodley)
* squid: rgw: switch back to boost::asio for spawn() and yield_context (`pr#60133 <https://github.com/ceph/ceph/pull/60133>`_, Casey Bodley)
* squid: rgwlc: fix typo in getlc (ObjectSizeGreaterThan) (`pr#59223 <https://github.com/ceph/ceph/pull/59223>`_, Matt Benjamin)
* squid: RGW|BN: fix lifecycle test issue (`pr#59010 <https://github.com/ceph/ceph/pull/59010>`_, Ali Masarwa)
* squid: RGW|Bucket notification: fix for v2 topics rgw-admin list operation (`pr#60774 <https://github.com/ceph/ceph/pull/60774>`_, Oshrey Avraham, Ali Masarwa)
* squid: seastar: update submodule (`pr#58955 <https://github.com/ceph/ceph/pull/58955>`_, Matan Breizman)
* squid: src/ceph_release, doc: mark squid stable (`pr#59537 <https://github.com/ceph/ceph/pull/59537>`_, Neha Ojha)
* squid: src/crimson/osd/scrub: fix the null pointer error (`pr#58885 <https://github.com/ceph/ceph/pull/58885>`_, junxiang Mu)
* squid: src/mon/ConnectionTracker.cc: Fix dump function (`pr#60003 <https://github.com/ceph/ceph/pull/60003>`_, Kamoltat)
* squid: suites/upgrade/quincy-x: update the ignore list (`pr#59624 <https://github.com/ceph/ceph/pull/59624>`_, Nitzan Mordechai)
* squid: suites: adding ignore list for stray daemon (`pr#58267 <https://github.com/ceph/ceph/pull/58267>`_, Nitzan Mordechai)
* squid: suites: test should ignore osd_down warnings (`pr#59147 <https://github.com/ceph/ceph/pull/59147>`_, Nitzan Mordechai)
* squid: test/neorados: remove depreciated RemoteReads cls test (`pr#58144 <https://github.com/ceph/ceph/pull/58144>`_, Laura Flores)
* squid: test/rgw/notification: fixing backport issues in the tests (`pr#60545 <https://github.com/ceph/ceph/pull/60545>`_, Yuval Lifshitz)
* squid: test/rgw/notification: use real ip address instead of localhost (`pr#59303 <https://github.com/ceph/ceph/pull/59303>`_, Yuval Lifshitz)
* squid: test/rgw/notifications: don't check for full queue if topics expired (`pr#59917 <https://github.com/ceph/ceph/pull/59917>`_, Yuval Lifshitz)
* squid: test/rgw/notifications: fix test regression (`pr#61119 <https://github.com/ceph/ceph/pull/61119>`_, Yuval Lifshitz)
* squid: Test: osd-recovery-space.sh extends the wait time for "recovery toofull" (`pr#59041 <https://github.com/ceph/ceph/pull/59041>`_, Nitzan Mordechai)
* upgrade/cephfs/mds_upgrade_sequence: ignore osds down (`pr#59865 <https://github.com/ceph/ceph/pull/59865>`_, Kamoltat Sirivadhna)
* squid: rgw: Don't crash on exceptions from pool listing (`pr#61306 <https://github.com/ceph/ceph/pull/61306>`_, Adam Emerson)
* squid: container/Containerfile: replace CEPH_VERSION label for backward compact (`pr#61583 <https://github.com/ceph/ceph/pull/61583>`_, Dan Mick)
* squid: container/build.sh: fix up org vs. repo naming (`pr#61584 <https://github.com/ceph/ceph/pull/61584>`_, Dan Mick)
* squid: container/build.sh: don't require repo creds on NO_PUSH (`pr#61585 <https://github.com/ceph/ceph/pull/61585>`_, Dan Mick)

v19.2.0 Squid
=============

.. ATTENTION::
   iSCSI users are advised that the upstream developers of Ceph encountered a
   bug during an upgrade from Ceph 19.1.1 to Ceph 19.2.0. Read `Tracker Issue
   68215 <https://tracker.ceph.com/issues/68215>`_ before attempting an upgrade
   to 19.2.0.

   Some users have encountered a Ceph Manager balancer module issue when
   upgrading to Ceph 19.2.0. If you encounter this issue, disable the balancer
   by running the command ``ceph balancer off`` and the cluster will operate as
   expected. A fix has been implemented in 19.2.1, please read `Tracker Issue
   68657 <https://tracker.ceph.com/issues/68657>`_ before attempting an
   upgrade.

Highlights
----------

RADOS

* BlueStore has been optimized for better performance in snapshot-intensive workloads.
* BlueStore RocksDB LZ4 compression is now enabled by default to improve average performance
  and "fast device" space usage.
* Other improvements include more flexible EC configurations, an OpTracker to help debug mgr
  module issues, and better scrub scheduling.

Dashboard

* Improved navigation layout
* Support for managing CephFS snapshots and clones, as well as snapshot schedule management
* Manage authorization capabilities for CephFS resources
* Helpers on mounting a CephFS volume

RBD

* diff-iterate can now execute locally, bringing a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.
* Support for cloning from non-user type snapshots is added.
* rbd-wnbd driver has gained the ability to multiplex image mappings.

RGW

* The User Accounts feature unlocks several new AWS-compatible IAM APIs for the self-service
  management of users, keys, groups, roles, policy and more.

Crimson/Seastore

* Crimson's first tech preview release! Supporting RBD workloads on Replicated pools. For more
  information please visit: https://ceph.io/en/news/crimson

Ceph
----

* ceph: a new `--daemon-output-file` switch is available for `ceph tell`
  commands to dump output to a file local to the daemon. For commands which
  produce large amounts of output, this avoids a potential spike in memory
  usage on the daemon, allows for faster streaming writes to a file local to
  the daemon, and reduces time holding any locks required to execute the
  command. For analysis, it is necessary to manually retrieve the file from the host
  running the daemon. Currently, only ``--format=json|json-pretty``
  are supported.
* ``cls_cxx_gather`` is marked as deprecated.
* Tracing: The blkin tracing feature (see
  https://docs.ceph.com/en/reef/dev/blkin/) is now deprecated in favor of
  Opentracing
  (https://docs.ceph.com/en/reef/dev/developer_guide/jaegertracing/) and will
  be removed in a later release.
* PG dump: The default output of ``ceph pg dump --format json`` has changed.
  The default JSON format produces a rather massive output in large clusters
  and isn't scalable, so we have removed the 'network_ping_times' section from
  the output. Details in the tracker: https://tracker.ceph.com/issues/57460

CephFS
------

* CephFS: it is now possible to pause write I/O and metadata mutations on a
  tree in the file system using a new suite of subvolume quiesce commands.
  This is implemented to support crash-consistent snapshots for distributed
  applications. Please see the relevant section in the documentation on CephFS
  subvolumes for more information.
* CephFS: MDS evicts clients which are not advancing their request tids which
  causes a large buildup of session metadata resulting in the MDS going
  read-only due to the RADOS operation exceeding the size threshold.
  `mds_session_metadata_threshold` config controls the maximum size that a
  (encoded) session metadata can grow.
* CephFS: A new "mds last-seen" command is available for querying the last time
  an MDS was in the FSMap, subject to a pruning threshold.
* CephFS: For clusters with multiple CephFS file systems, all the snap-schedule
  commands now expect the '--fs' argument.
* CephFS: The period specifier ``m`` now implies minutes and the period
  specifier ``M`` now implies months. This has been made consistent with the
  rest of the system.
* CephFS: Running the command "ceph fs authorize" for an existing entity now
  upgrades the entity's capabilities instead of printing an error. It can now
  also change read/write permissions in a capability that the entity already
  holds. If the capability passed by user is same as one of the capabilities
  that the entity already holds, idempotency is maintained.
* CephFS: Two FS names can now be swapped, optionally along with their IDs,
  using "ceph fs swap" command. The function of this API is to facilitate
  file system swaps for disaster recovery. In particular, it avoids situations
  where a named file system is temporarily missing which would prompt a higher
  level storage operator (like Rook) to recreate the missing file system.
  See https://docs.ceph.com/en/latest/cephfs/administration/#file-systems
  docs for more information.
* CephFS: Before running the command "ceph fs rename", the filesystem to be
  renamed must be offline and the config "refuse_client_session" must be set
  for it. The config "refuse_client_session" can be removed/unset and
  filesystem can be online after the rename operation is complete.
* CephFS: Disallow delegating preallocated inode ranges to clients. Config
  `mds_client_delegate_inos_pct` defaults to 0 which disables async dirops
  in the kclient.
* CephFS: MDS log trimming is now driven by a separate thread which tries to
  trim the log every second (`mds_log_trim_upkeep_interval` config). Also, a
  couple of configs govern how much time the MDS spends in trimming its logs.
  These configs are `mds_log_trim_threshold` and `mds_log_trim_decay_rate`.
* CephFS: Full support for subvolumes and subvolume groups is now available
* CephFS: The `subvolume snapshot clone` command now depends on the config
  option `snapshot_clone_no_wait` which is used to reject the clone operation
  when all the cloner threads are busy. This config option is enabled by
  default which means that if no cloner threads are free, the clone request
  errors out with EAGAIN.  The value of the config option can be fetched by
  using: `ceph config get mgr mgr/volumes/snapshot_clone_no_wait` and it can be
  disabled by using: `ceph config set mgr mgr/volumes/snapshot_clone_no_wait
  false`
  for snap_schedule Manager module.
* CephFS: Commands ``ceph mds fail`` and ``ceph fs fail`` now require a
  confirmation flag when some MDSs exhibit health warning MDS_TRIM or
  MDS_CACHE_OVERSIZED. This is to prevent accidental MDS failover causing
  further delays in recovery.
* CephFS: fixes to the implementation of the ``root_squash`` mechanism enabled
  via cephx ``mds`` caps on a client credential require a new client feature
  bit, ``client_mds_auth_caps``. Clients using credentials with ``root_squash``
  without this feature will trigger the MDS to raise a HEALTH_ERR on the
  cluster, MDS_CLIENTS_BROKEN_ROOTSQUASH. See the documentation on this warning
  and the new feature bit for more information.
* CephFS: Expanded removexattr support for cephfs virtual extended attributes.
  Previously one had to use setxattr to restore the default in order to
  "remove".  You may now properly use removexattr to remove. You can also now
  remove layout on root inode, which then will restore layout to default
  layout.
* CephFS: cephfs-journal-tool is guarded against running on an online file
  system.  The 'cephfs-journal-tool --rank <fs_name>:<mds_rank> journal reset'
  and 'cephfs-journal-tool --rank <fs_name>:<mds_rank> journal reset --force'
  commands require '--yes-i-really-really-mean-it'.
* CephFS: "ceph fs clone status" command will now print statistics about clone
  progress in terms of how much data has been cloned (in both percentage as
  well as bytes) and how many files have been cloned.
* CephFS: "ceph status" command will now print a progress bar when cloning is
  ongoing. If clone jobs are more than the cloner threads, it will print one
  more progress bar that shows total amount of progress made by both ongoing
  as well as pending clones. Both progress are accompanied by messages that
  show number of clone jobs in the respective categories and the amount of
  progress made by each of them.
* cephfs-shell: The cephfs-shell utility is now packaged for RHEL 9 / CentOS 9
  as required python dependencies are now available in EPEL9.
* The CephFS automatic metadata load (sometimes called "default") balancer is
  now disabled by default. The new file system flag `balance_automate`
  can be used to toggle it on or off. It can be enabled or disabled via
  `ceph fs set <fs_name> balance_automate <bool>`.

CephX
-----

* cephx: key rotation is now possible using `ceph auth rotate`. Previously,
  this was only possible by deleting and then recreating the key.

Dashboard
---------

* Dashboard: Rearranged Navigation Layout: The navigation layout has been reorganized for improved usability and easier access to key features.
* Dashboard: CephFS Improvments
  * Support for managing CephFS snapshots and clones, as well as snapshot schedule management
  * Manage authorization capabilities for CephFS resources
  * Helpers on mounting a CephFS volume
* Dashboard: RGW Improvements
  * Support for managing bucket policies
  * Add/Remove bucket tags
  * ACL Management
  * Several UI/UX Improvements to the bucket form

MGR
---

* MGR/REST: The REST manager module will trim requests based on the
  'max_requests' option.  Without this feature, and in the absence of manual
  deletion of old requests, the accumulation of requests in the array can lead
  to Out Of Memory (OOM) issues, resulting in the Manager crashing.
* MGR: An OpTracker to help debug mgr module issues is now available.

Monitoring
----------

* Monitoring: Grafana dashboards are now loaded into the container at runtime
  rather than building a grafana image with the grafana dashboards. Official
  Ceph grafana images can be found in quay.io/ceph/grafana
* Monitoring: RGW S3 Analytics: A new Grafana dashboard is now available,
  enabling you to visualize per bucket and user analytics data, including total
  GETs, PUTs, Deletes, Copies, and list metrics.
* The ``mon_cluster_log_file_level`` and ``mon_cluster_log_to_syslog_level``
  options have been removed. Henceforth, users should use the new generic
  option ``mon_cluster_log_level`` to control the cluster log level verbosity
  for the cluster log file as well as for all external entities.

RADOS
-----

* RADOS: ``A POOL_APP_NOT_ENABLED`` health warning will now be reported if the
  application is not enabled for the pool irrespective of whether the pool is
  in use or not. Always tag a pool with an application using ``ceph osd pool
  application enable`` command to avoid reporting of POOL_APP_NOT_ENABLED
  health warning for that pool. The user might temporarily mute this warning
  using ``ceph health mute POOL_APP_NOT_ENABLED``.
* RADOS: `get_pool_is_selfmanaged_snaps_mode` C++ API has been deprecated due
  to being prone to false negative results.  Its safer replacement is
  `pool_is_in_selfmanaged_snaps_mode`.
* RADOS: For bug 62338 (https://tracker.ceph.com/issues/62338), we did not
  choose to condition the fix on a server flag in order to simplify
  backporting.  As a result, in rare cases it may be possible for a PG to flip
  between two acting sets while an upgrade to a version with the fix is in
  progress.  If you observe this behavior, you should be able to work around it
  by completing the upgrade or by disabling async recovery by setting
  osd_async_recovery_min_cost to a very large value on all OSDs until the
  upgrade is complete: ``ceph config set osd osd_async_recovery_min_cost
  1099511627776``
* RADOS: A detailed version of the `balancer status` CLI command in the
  balancer module is now available. Users may run `ceph balancer status detail`
  to see more details about which PGs were updated in the balancer's last
  optimization.  See https://docs.ceph.com/en/latest/rados/operations/balancer/
  for more information.
* RADOS: Read balancing may now be managed automatically via the balancer
  manager module. Users may choose between two new modes: ``upmap-read``, which
  offers upmap and read optimization simultaneously, or ``read``, which may be
  used to only optimize reads. For more detailed information see
  https://docs.ceph.com/en/latest/rados/operations/read-balancer/#online-optimization.
* RADOS: BlueStore has been optimized for better performance in snapshot-intensive workloads.
* RADOS: BlueStore RocksDB LZ4 compression is now enabled by default to improve average
  performance and "fast device" space usage.
* RADOS: A new CRUSH rule type, MSR (Multi-Step Retry), allows for more flexible EC
  configurations.
* RADOS: Scrub scheduling behavior has been improved.

Crimson/Seastore
----------------

* Crimson's first tech preview release!
  Supporting RBD workloads on Replicated pools.
  For more information please visit: https://ceph.io/en/news/crimson

RBD
---

* RBD: When diffing against the beginning of time (`fromsnapname == NULL`) in
  fast-diff mode (`whole_object == true` with ``fast-diff`` image feature enabled
  and valid), diff-iterate is now guaranteed to execute locally if exclusive
  lock is available.  This brings a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.
* RBD: The ``try-netlink`` mapping option for rbd-nbd has become the default
  and is now deprecated. If the NBD netlink interface is not supported by the
  kernel, then the mapping is retried using the legacy ioctl interface.
* RBD: The option ``--image-id`` has been added to `rbd children` CLI command,
  so it can be run for images in the trash.
* RBD: `Image::access_timestamp` and `Image::modify_timestamp` Python APIs now
  return timestamps in UTC.
* RBD: Support for cloning from non-user type snapshots is added.  This is
  intended primarily as a building block for cloning new groups from group
  snapshots created with `rbd group snap create` command, but has also been
  exposed via the new `--snap-id` option for `rbd clone` command.
* RBD: The output of `rbd snap ls --all` command now includes the original
  type for trashed snapshots.
* RBD: `RBD_IMAGE_OPTION_CLONE_FORMAT` option has been exposed in Python
  bindings via `clone_format` optional parameter to `clone`, `deep_copy` and
  `migration_prepare` methods.
* RBD: `RBD_IMAGE_OPTION_FLATTEN` option has been exposed in Python bindings
  via `flatten` optional parameter to `deep_copy` and `migration_prepare`
  methods.
* RBD: `rbd-wnbd` driver has gained the ability to multiplex image mappings.
  Previously, each image mapping spawned its own `rbd-wnbd` daemon, which lead
  to an excessive amount of TCP sessions and other resources being consumed,
  eventually exceeding Windows limits.  With this change, a single `rbd-wnbd`
  daemon is spawned per host and most OS resources are shared between image
  mappings.  Additionally, `ceph-rbd` service starts much faster.

RGW
---

* RGW: GetObject and HeadObject requests now return a x-rgw-replicated-at
  header for replicated objects. This timestamp can be compared against the
  Last-Modified header to determine how long the object took to replicate.
* RGW: S3 multipart uploads using Server-Side Encryption now replicate
  correctly in multi-site. Previously, the replicas of such objects were
  corrupted on decryption.  A new tool, ``radosgw-admin bucket resync encrypted
  multipart``, can be used to identify these original multipart uploads. The
  ``LastModified`` timestamp of any identified object is incremented by 1ns to
  cause peer zones to replicate it again.  For multi-site deployments that make
  any use of Server-Side Encryption, we recommended running this command
  against every bucket in every zone after all zones have upgraded.
* RGW: Introducing a new data layout for the Topic metadata associated with S3
  Bucket Notifications, where each Topic is stored as a separate RADOS object
  and the bucket notification configuration is stored in a bucket attribute.
  This new representation supports multisite replication via metadata sync and
  can scale to many topics. This is on by default for new deployments, but is
  not enabled by default on upgrade. Once all radosgws have upgraded (on all
  zones in a multisite configuration), the ``notification_v2`` zone feature can
  be enabled to migrate to the new format. See
  https://docs.ceph.com/en/squid/radosgw/zone-features for details. The "v1"
  format is now considered deprecated and may be removed after 2 major releases.
* RGW: New tools have been added to radosgw-admin for identifying and
  correcting issues with versioned bucket indexes. Historical bugs with the
  versioned bucket index transaction workflow made it possible for the index
  to accumulate extraneous "book-keeping" olh entries and plain placeholder
  entries. In some specific scenarios where clients made concurrent requests
  referencing the same object key, it was likely that a lot of extra index
  entries would accumulate. When a significant number of these entries are
  present in a single bucket index shard, they can cause high bucket listing
  latencies and lifecycle processing failures. To check whether a versioned
  bucket has unnecessary olh entries, users can now run ``radosgw-admin
  bucket check olh``. If the ``--fix`` flag is used, the extra entries will
  be safely removed. A distinct issue from the one described thus far, it is
  also possible that some versioned buckets are maintaining extra unlinked
  objects that are not listable from the S3/ Swift APIs. These extra objects
  are typically a result of PUT requests that exited abnormally, in the middle
  of a bucket index transaction - so the client would not have received a
  successful response. Bugs in prior releases made these unlinked objects easy
  to reproduce with any PUT request that was made on a bucket that was actively
  resharding. Besides the extra space that these hidden, unlinked objects
  consume, there can be another side effect in certain scenarios, caused by
  the nature of the failure mode that produced them, where a client of a bucket
  that was a victim of this bug may find the object associated with the key to
  be in an inconsistent state. To check whether a versioned bucket has unlinked
  entries, users can now run ``radosgw-admin bucket check unlinked``. If the
  ``--fix`` flag is used, the unlinked objects will be safely removed. Finally,
  a third issue made it possible for versioned bucket index stats to be
  accounted inaccurately. The tooling for recalculating versioned bucket stats
  also had a bug, and was not previously capable of fixing these inaccuracies.
  This release resolves those issues and users can now expect that the existing
  ``radosgw-admin bucket check`` command will produce correct results. We
  recommend that users with versioned buckets, especially those that existed
  on prior releases, use these new tools to check whether their buckets are
  affected and to clean them up accordingly.
* RGW: The User Accounts feature unlocks several new AWS-compatible IAM APIs
  for the self-service management of users, keys, groups, roles, policy and
  more. Existing users can be adopted into new accounts. This process is
  optional but irreversible. See https://docs.ceph.com/en/squid/radosgw/account
  and https://docs.ceph.com/en/squid/radosgw/iam for details.
* RGW: On startup, radosgw and radosgw-admin now validate the ``rgw_realm``
  config option. Previously, they would ignore invalid or missing realms and go
  on to load a zone/zonegroup in a different realm. If startup fails with a
  "failed to load realm" error, fix or remove the ``rgw_realm`` option.
* RGW: The radosgw-admin commands ``realm create`` and ``realm pull`` no longer
  set the default realm without ``--default``.
* RGW: Fixed an S3 Object Lock bug with PutObjectRetention requests that
  specify a RetainUntilDate after the year 2106. This date was truncated to 32
  bits when stored, so a much earlier date was used for object lock
  enforcement.  This does not effect PutBucketObjectLockConfiguration where a
  duration is given in Days.  The RetainUntilDate encoding is fixed for new
  PutObjectRetention requests, but cannot repair the dates of existing object
  locks. Such objects can be identified with a HeadObject request based on the
  x-amz-object-lock-retain-until-date response header.
* S3 ``Get/HeadObject`` now supports the query parameter ``partNumber`` to read
  a specific part of a completed multipart upload.
* RGW: The SNS CreateTopic API now enforces the same topic naming requirements
  as AWS: Topic names must be made up of only uppercase and lowercase ASCII
  letters, numbers, underscores, and hyphens, and must be between 1 and 256
  characters long.
* RGW: Notification topics are now owned by the user that created them.  By
  default, only the owner can read/write their topics. Topic policy documents
  are now supported to grant these permissions to other users. Preexisting
  topics are treated as if they have no owner, and any user can read/write them
  using the SNS API.  If such a topic is recreated with CreateTopic, the
  issuing user becomes the new owner.  For backward compatibility, all users
  still have permission to publish bucket notifications to topics owned by
  other users. A new configuration parameter,
  ``rgw_topic_require_publish_policy``, can be enabled to deny ``sns:Publish``
  permissions unless explicitly granted by topic policy.
* RGW: Fix issue with persistent notifications where the changes to topic param
  that were modified while persistent notifications were in the queue will be
  reflected in notifications.  So if the user sets up topic with incorrect config
  (password/ssl) causing failure while delivering the notifications to broker,
  can now modify the incorrect topic attribute and on retry attempt to delivery
  the notifications, new configs will be used.
* RGW: in bucket notifications, the ``principalId`` inside ``ownerIdentity``
  now contains the complete user ID, prefixed with the tenant ID.

Telemetry
---------

* The ``basic`` channel in telemetry now captures pool flags that allows us to
  better understand feature adoption, such as Crimson.
  To opt in to telemetry, run ``ceph telemetry on``.

Upgrading from Quincy or Reef
--------------------------------

Before starting, make sure your cluster is stable and healthy (no down or recovering OSDs).
(This is optional, but recommended.) You can disable the autoscaler for all pools during the
upgrade using the noautoscale flag.

.. note::

   You can monitor the progress of your upgrade at each stage with the ``ceph versions`` command, which will tell you what ceph version(s) are running for each type of daemon.

Upgrading cephadm clusters
--------------------------

If your cluster is deployed with cephadm (first introduced in Octopus), then the upgrade process is entirely automated. To initiate the upgrade,

  .. prompt:: bash #

    ceph orch upgrade start --image quay.io/ceph/ceph:v19.2.0

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

Note that canceling the upgrade simply stops the process; there is no ability to downgrade back to Quincy or Reef.

Upgrading non-cephadm clusters
------------------------------

.. note::

   1. If your cluster is running Quincy (17.2.x) or later, you might choose to first convert it to use cephadm so that the upgrade to Squid is automated (see above).
      For more information, see https://docs.ceph.com/en/squid/cephadm/adoption/.

   2. If your cluster is running Quincy (17.2.x) or later, systemd unit file names have changed to include the cluster fsid. To find the correct systemd unit file name for your cluster, run following command:

      ::

        systemctl -l | grep <daemon type>

      Example:

      .. prompt:: bash $

        systemctl -l | grep mon | grep active

      ::

        ceph-6ce0347c-314a-11ee-9b52-000af7995d6c@mon.f28-h21-000-r630.service                                           loaded active running   Ceph mon.f28-h21-000-r630 for 6ce0347c-314a-11ee-9b52-000af7995d6c

#. Set the `noout` flag for the duration of the upgrade. (Optional, but recommended.)

   .. prompt:: bash #

      ceph osd set noout

#. Upgrade monitors by installing the new packages and restarting the monitor daemons. For example, on each monitor host

   .. prompt:: bash #

      systemctl restart ceph-mon.target

   Once all monitors are up, verify that the monitor upgrade is complete by looking for the `squid` string in the mon map. The command

   .. prompt:: bash #

      ceph mon dump | grep min_mon_release

   should report:

   .. prompt:: bash #

      min_mon_release 19 (squid)

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

#. Upgrade all radosgw daemons by upgrading packages and restarting daemons on all hosts

   .. prompt:: bash #

      systemctl restart ceph-radosgw.target

#. Complete the upgrade by disallowing pre-Squid OSDs and enabling all new Squid-only functionality

   .. prompt:: bash #

      ceph osd require-osd-release squid

#. If you set `noout` at the beginning, be sure to clear it with

   .. prompt:: bash #

      ceph osd unset noout

#. Consider transitioning your cluster to use the cephadm deployment and orchestration framework to simplify
   cluster management and future upgrades. For more information on converting an existing cluster to cephadm,
   see https://docs.ceph.com/en/squid/cephadm/adoption/.

Post-upgrade
------------

#. Verify the cluster is healthy with `ceph health`. If your cluster is running Filestore, and you are upgrading directly from Quincy to Squid, a deprecation warning is expected. This warning can be temporarily muted using the following command

   .. prompt:: bash #

      ceph health mute OSD_FILESTORE

#. Consider enabling the `telemetry module <https://docs.ceph.com/en/squid/mgr/telemetry/>`_ to send anonymized usage statistics and crash information to the Ceph upstream developers. To see what would be reported (without actually sending any information to anyone),

   .. prompt:: bash #

      ceph telemetry preview-all

   If you are comfortable with the data that is reported, you can opt-in to automatically report the high-level cluster metadata with

   .. prompt:: bash #

      ceph telemetry on

   The public dashboard that aggregates Ceph telemetry can be found at https://telemetry-public.ceph.com/.

Upgrading from pre-Quincy releases (like Pacific)
-------------------------------------------------

You **must** first upgrade to Quincy (17.2.z) or Reef (18.2.z) before upgrading to Squid.
