===============
 Release Notes
===============

v10.1.1 (draft)
===============

Notable Changes since v10.1.0
-----------------------------

* Adding documentation on how to use new dynamic throttle scheme (`pr#8069 <http://github.com/ceph/ceph/pull/8069>`_, Somnath Roy)
* Be more careful about directory fragmentation and scrubbing (`issue#15167 <http://tracker.ceph.com/issues/15167>`_, `pr#8180 <http://github.com/ceph/ceph/pull/8180>`_, Yan, Zheng)
* CMake: For CMake version <= 2.8.11, use LINK_PRIVATE (`pr#8422 <http://github.com/ceph/ceph/pull/8422>`_, Haomai Wang)
* Makefile-env.am: set a default for CEPH_BUILD_VIRTUALENV (part 2) (`pr#8320 <http://github.com/ceph/ceph/pull/8320>`_, Loic Dachary)
* Minor fixes around data scan in some scenarios (`pr#8115 <http://github.com/ceph/ceph/pull/8115>`_, Yan, Zheng)
* PG: pg down state blocked by osd.x, lost osd.x cannot solve peering stuck (`issue#13531 <http://tracker.ceph.com/issues/13531>`_, `pr#6317 <http://github.com/ceph/ceph/pull/6317>`_, Xiaowei Chen)
* Revewed-by: Samuel Just <sjust@redhat.com> (`pr#8156 <http://github.com/ceph/ceph/pull/8156>`_, Sage Weil)
* Striper: reduce assemble_result log level (`pr#8426 <http://github.com/ceph/ceph/pull/8426>`_, Jason Dillaman)
* Test exit values on test.sh, fix tier.cc (`issue#15165 <http://tracker.ceph.com/issues/15165>`_, `pr#8266 <http://github.com/ceph/ceph/pull/8266>`_, Samuel Just)
* Tested-by: Sage Weil <sage@redhat.com> (`pr#8360 <http://github.com/ceph/ceph/pull/8360>`_, Josh Durgin)
* Wip 14438 (`pr#8215 <http://github.com/ceph/ceph/pull/8215>`_, David Zafman)
* Wip rgw sync fixes 4 (`pr#8190 <http://github.com/ceph/ceph/pull/8190>`_, Yehuda Sadeh)
* [rgw] Check return code in RGWFileHandle::write (`pr#7875 <http://github.com/ceph/ceph/pull/7875>`_, Brad Hubbard)
* build: fix compiling warnings (`pr#8366 <http://github.com/ceph/ceph/pull/8366>`_, Dongsheng Yang)
* ceph-detect-init/run-tox.sh: FreeBSD: No init detect (`pr#8373 <http://github.com/ceph/ceph/pull/8373>`_, Willem Jan Withagen)
* ceph.in: fix python libpath for automake as well (`pr#8362 <http://github.com/ceph/ceph/pull/8362>`_, Josh Durgin)
* ceph: bash auto complete for CLI based on mon command descriptions (`pr#7693 <http://github.com/ceph/ceph/pull/7693>`_, Adam Kupczyk)
* cls_journal: fix -EEXIST checking (`pr#8413 <http://github.com/ceph/ceph/pull/8413>`_, runsisi)
* cls_rbd: fix -EEXIST checking in cls::rbd::image_set (`pr#8371 <http://github.com/ceph/ceph/pull/8371>`_, runsisi)
* cls_rbd: mirror_image_list should return global image id (`pr#8297 <http://github.com/ceph/ceph/pull/8297>`_, Jason Dillaman)
* cls_rbd: pass WILLNEED fadvise flags during object map update (`issue#15332 <http://tracker.ceph.com/issues/15332>`_, `pr#8380 <http://github.com/ceph/ceph/pull/8380>`_, Jason Dillaman)
* cls_rbd: read_peers: update last_read on next cls_cxx_map_get_vals (`pr#8374 <http://github.com/ceph/ceph/pull/8374>`_, Mykola Golub)
* cmake: Build cython modules and change paths to bin/, lib/ (`pr#8351 <http://github.com/ceph/ceph/pull/8351>`_, John Spray, Ali Maredia)
* cmake: add FindOpenSSL.cmake (`pr#8106 <http://github.com/ceph/ceph/pull/8106>`_, Marcus Watts, Matt Benjamin)
* cmake: add StandardPolicy.cc to librbd (`pr#8368 <http://github.com/ceph/ceph/pull/8368>`_, Kefu Chai)
* cmake: add missing librbd/MirrorWatcher.cc and librd/ObjectWatcher.cc (`pr#8399 <http://github.com/ceph/ceph/pull/8399>`_, Orit Wasserman)
* cmake: fix mrun to handle cmake build structure (`pr#8237 <http://github.com/ceph/ceph/pull/8237>`_, Orit Wasserman)
* cmake: fix the build of test_rados_api_list (`pr#8438 <http://github.com/ceph/ceph/pull/8438>`_, Kefu Chai)
* common: fix race during optracker switches between enabled/disabled mode (`pr#8330 <http://github.com/ceph/ceph/pull/8330>`_, xie xingguo)
* config_opts: disable filestore throttle soft backoff by default (`pr#8265 <http://github.com/ceph/ceph/pull/8265>`_, Samuel Just)
* configure: Add -D_LARGEFILE64_SOURCE to Linux build. (`pr#8402 <http://github.com/ceph/ceph/pull/8402>`_, Ira Cooper)
* crush: fix error log (`pr#8430 <http://github.com/ceph/ceph/pull/8430>`_, Wei Jin)
* crushtool: Don't crash when called on a file that isn't a crushmap (`issue#8286 <http://tracker.ceph.com/issues/8286>`_, `pr#8038 <http://github.com/ceph/ceph/pull/8038>`_, Brad Hubbard)
* debian/rules: put init-ceph in /etc/init.d/ceph, not ceph-base (`issue#15329 <http://tracker.ceph.com/issues/15329>`_, `pr#8406 <http://github.com/ceph/ceph/pull/8406>`_, Dan Mick)
* doc/dev: add "Deploy a cluster for manual testing" section (`issue#15218 <http://tracker.ceph.com/issues/15218>`_, `pr#8228 <http://github.com/ceph/ceph/pull/8228>`_, Nathan Cutler)
* doc/rados/operations/crush: fix the formatting (`pr#8306 <http://github.com/ceph/ceph/pull/8306>`_, Kefu Chai)
* doc/release-notes: fix indents (`pr#8345 <http://github.com/ceph/ceph/pull/8345>`_, Kefu Chai)
* doc: Fixes headline different font size and type (`pr#8328 <http://github.com/ceph/ceph/pull/8328>`_, scienceluo)
* doc: Remove Ceph Monitors do lots of fsync() (`issue#15288 <http://tracker.ceph.com/issues/15288>`_, `pr#8327 <http://github.com/ceph/ceph/pull/8327>`_, Vikhyat Umrao)
* doc: Updated CloudStack RBD documentation (`pr#8308 <http://github.com/ceph/ceph/pull/8308>`_, Wido den Hollander)
* doc: amend Fixes instructions in SubmittingPatches (`pr#8312 <http://github.com/ceph/ceph/pull/8312>`_, Nathan Cutler)
* doc: draft notes for jewel (`pr#8211 <http://github.com/ceph/ceph/pull/8211>`_, Loic Dachary, Sage Weil)
* doc: fix typo, duplicated content etc. for Jewel release notes (`pr#8342 <http://github.com/ceph/ceph/pull/8342>`_, xie xingguo)
* doc: fix wrong type of hyphen (`pr#8252 <http://github.com/ceph/ceph/pull/8252>`_, xie xingguo)
* doc: rgw_region_root_pool option should be in [global] (`issue#15244 <http://tracker.ceph.com/issues/15244>`_, `pr#8271 <http://github.com/ceph/ceph/pull/8271>`_, Vikhyat Umrao)
* doc: very basic doc on mstart (`pr#8207 <http://github.com/ceph/ceph/pull/8207>`_, Abhishek Lekshmanan)
* global/global_init: expand metavariables in setuser_match_path (`issue#15365 <http://tracker.ceph.com/issues/15365>`_, `pr#8433 <http://github.com/ceph/ceph/pull/8433>`_, Sage Weil)
* global/signal_handler: print thread name in signal handler (`pr#8177 <http://github.com/ceph/ceph/pull/8177>`_, Jianpeng Ma)
* libcephfs: fix python tests and fix getcwd on missing dir (`pr#7901 <http://github.com/ceph/ceph/pull/7901>`_, John Spray)
* librbd: avoid throwing error if mirroring is unsupported (`pr#8417 <http://github.com/ceph/ceph/pull/8417>`_, Jason Dillaman)
* librbd: disable image mirroring when image is removed (`issue#15265 <http://tracker.ceph.com/issues/15265>`_, `pr#8375 <http://github.com/ceph/ceph/pull/8375>`_, Ricardo Dias)
* librbd: send notifications for mirroring status updates (`pr#8355 <http://github.com/ceph/ceph/pull/8355>`_, Jason Dillaman)
* mailmap updates (`pr#8256 <http://github.com/ceph/ceph/pull/8256>`_, Loic Dachary)
* makefile: fix rbdmap manpage (`pr#8310 <http://github.com/ceph/ceph/pull/8310>`_, Kefu Chai)
* mds: allow client to request caps when opening file (`issue#14360 <http://tracker.ceph.com/issues/14360>`_, `pr#7952 <http://github.com/ceph/ceph/pull/7952>`_, Yan, Zheng)
* messages/MOSDOp: clear reqid inc for v6 encoding (`issue#15230 <http://tracker.ceph.com/issues/15230>`_, `pr#8299 <http://github.com/ceph/ceph/pull/8299>`_, Sage Weil)
* mon/MonClient: fix shutdown race (`issue#13992 <http://tracker.ceph.com/issues/13992>`_, `pr#8335 <http://github.com/ceph/ceph/pull/8335>`_, Sage Weil)
* mon: do not send useless pg_create messages for split pgs (`pr#8247 <http://github.com/ceph/ceph/pull/8247>`_, Sage Weil)
* mon: mark_down_pgs in lockstep with pg_map's osdmap epoch (`pr#8208 <http://github.com/ceph/ceph/pull/8208>`_, Sage Weil)
* mon: remove remove_legacy_versions() (`pr#8324 <http://github.com/ceph/ceph/pull/8324>`_, Kefu Chai)
* mon: remove unnecessary comment for update_from_paxos (`pr#8400 <http://github.com/ceph/ceph/pull/8400>`_, Qinghua Jin)
* mon: remove unused variable (`issue#15292 <http://tracker.ceph.com/issues/15292>`_, `pr#8337 <http://github.com/ceph/ceph/pull/8337>`_, Javier M. Mellid)
* mon: show the pool quota info on ceph df detail command (`issue#14216 <http://tracker.ceph.com/issues/14216>`_, `pr#7094 <http://github.com/ceph/ceph/pull/7094>`_, song baisen)
* monclient: avoid key renew storm on clock skew (`issue#12065 <http://tracker.ceph.com/issues/12065>`_, `pr#8258 <http://github.com/ceph/ceph/pull/8258>`_, Alexey Sheplyakov)
* mrun: update path to cmake binaries (`pr#8447 <http://github.com/ceph/ceph/pull/8447>`_, Casey Bodley)
* msg/async: avoid log spam on throttle (`issue#15031 <http://tracker.ceph.com/issues/15031>`_, `pr#8263 <http://github.com/ceph/ceph/pull/8263>`_, Kefu Chai)
* msg/async: remove experiment feature (`pr#7820 <http://github.com/ceph/ceph/pull/7820>`_, Haomai Wang)
* os/ObjectStore: add noexcept to ensure move ctor is used (`pr#8421 <http://github.com/ceph/ceph/pull/8421>`_, Kefu Chai)
* os/ObjectStore: fix _update_op for split dest_cid (`pr#8364 <http://github.com/ceph/ceph/pull/8364>`_, Sage Weil)
* os/ObjectStore: try_move_rename in transaction append and add coverage to store_test (`issue#15205 <http://tracker.ceph.com/issues/15205>`_, `pr#8359 <http://github.com/ceph/ceph/pull/8359>`_, Samuel Just)
* os/bluestore: a few fixes (`pr#8193 <http://github.com/ceph/ceph/pull/8193>`_, Sage Weil)
* os/bluestore: ceph-bluefs-tool fixes (`issue#15261 <http://tracker.ceph.com/issues/15261>`_, `pr#8292 <http://github.com/ceph/ceph/pull/8292>`_, Venky Shankar)
* osd/ClassHandler: only dlclose() the classes not missing (`pr#8354 <http://github.com/ceph/ceph/pull/8354>`_, Kefu Chai)
* osd/OSD.cc: finish full_map_request every MOSDMap message. (`issue#15130 <http://tracker.ceph.com/issues/15130>`_, `pr#8147 <http://github.com/ceph/ceph/pull/8147>`_, Xiaoxi Chen)
* osd: add 'proxy' cache mode (`issue#12814 <http://tracker.ceph.com/issues/12814>`_, `pr#8210 <http://github.com/ceph/ceph/pull/8210>`_, Sage Weil)
* osd: add the support of per pool scrub priority (`pr#7062 <http://github.com/ceph/ceph/pull/7062>`_, Zhiqiang Wang)
* osd: bail out of _committed_osd_maps if we are shutting down (`pr#8267 <http://github.com/ceph/ceph/pull/8267>`_, Samuel Just)
* osd: duplicated clear for peer_missing (`pr#8315 <http://github.com/ceph/ceph/pull/8315>`_, Ning Yao)
* osd: fix bugs for omap ops (`pr#8230 <http://github.com/ceph/ceph/pull/8230>`_, Jianpeng Ma)
* osd: fix dirtying info without correctly setting drity_info field (`pr#8275 <http://github.com/ceph/ceph/pull/8275>`_, xie xingguo)
* osd: fix dump_ops_in_flight races (`issue#8885 <http://tracker.ceph.com/issues/8885>`_, `pr#8044 <http://github.com/ceph/ceph/pull/8044>`_, David Zafman)
* osd: fix epoch check in handle_pg_create (`pr#8382 <http://github.com/ceph/ceph/pull/8382>`_, Samuel Just)
* osd: fix failure report handling during ms_handle_connect() (`pr#8348 <http://github.com/ceph/ceph/pull/8348>`_, xie xingguo)
* osd: fix log info (`pr#8273 <http://github.com/ceph/ceph/pull/8273>`_, Wei Jin)
* osd: fix reference count, rare race condition etc. (`pr#8254 <http://github.com/ceph/ceph/pull/8254>`_, xie xingguo)
* osd: fix tick relevant issues (`pr#8369 <http://github.com/ceph/ceph/pull/8369>`_, xie xingguo)
* osd: more fixes for incorrectly dirtying info; resend reply for duplicated scrub-reserve req (`pr#8291 <http://github.com/ceph/ceph/pull/8291>`_, xie xingguo)
* osdc/Objecter: dout log after assign tid (`pr#8202 <http://github.com/ceph/ceph/pull/8202>`_, Xinze Chi)
* osdc/Objecter: use full pgid hash in PGNLS ops (`pr#8378 <http://github.com/ceph/ceph/pull/8378>`_, Sage Weil)
* osdmap: rm nonused variable (`pr#8423 <http://github.com/ceph/ceph/pull/8423>`_, Wei Jin)
* pybind/Makefile.am: Prevent race creating CYTHON_BUILD_DIR (`issue#15276 <http://tracker.ceph.com/issues/15276>`_, `pr#8356 <http://github.com/ceph/ceph/pull/8356>`_, Dan Mick)
* pybind/rados: python3 fix (`pr#8331 <http://github.com/ceph/ceph/pull/8331>`_, Mehdi Abaakouk)
* pybind: add flock to libcephfs python bindings (`pr#7902 <http://github.com/ceph/ceph/pull/7902>`_, John Spray)
* qa: update rest test cephfs calls (`issue#15309 <http://tracker.ceph.com/issues/15309>`_, `pr#8372 <http://github.com/ceph/ceph/pull/8372>`_, John Spray)
* qa: update rest test cephfs calls (part 2) (`issue#15309 <http://tracker.ceph.com/issues/15309>`_, `pr#8393 <http://github.com/ceph/ceph/pull/8393>`_, John Spray)
* radosgw-admin: 'period commit' supplies user-readable error messages (`pr#8264 <http://github.com/ceph/ceph/pull/8264>`_, Casey Bodley)
* radosgw-admin: fix for 'realm pull' (`pr#8404 <http://github.com/ceph/ceph/pull/8404>`_, Casey Bodley)
* rbd-mirror: asok commands to get status and flush on Mirror and Replayer level (`pr#8235 <http://github.com/ceph/ceph/pull/8235>`_, Mykola Golub)
* rbd-mirror: enabling/disabling pool mirroring should update the mirroring directory (`issue#15217 <http://tracker.ceph.com/issues/15217>`_, `pr#8261 <http://github.com/ceph/ceph/pull/8261>`_, Ricardo Dias)
* rbd-mirror: fix missing increment for iterators (`pr#8352 <http://github.com/ceph/ceph/pull/8352>`_, runsisi)
* rbd-mirror: initial failover / failback support (`pr#8287 <http://github.com/ceph/ceph/pull/8287>`_, Jason Dillaman)
* rbd-mirror: prevent enabling/disabling an image's mirroring when not in image mode (`issue#15267 <http://tracker.ceph.com/issues/15267>`_, `pr#8332 <http://github.com/ceph/ceph/pull/8332>`_, Ricardo Dias)
* rbd-mirror: switch fsid over to mirror uuid (`issue#15238 <http://tracker.ceph.com/issues/15238>`_, `pr#8280 <http://github.com/ceph/ceph/pull/8280>`_, Ricardo Dias)
* rbd: allow librados to prune the command-line for config overrides (`issue#15250 <http://tracker.ceph.com/issues/15250>`_, `pr#8282 <http://github.com/ceph/ceph/pull/8282>`_, Jason Dillaman)
* rbdmap: add manpage (`issue#15212 <http://tracker.ceph.com/issues/15212>`_, `pr#8224 <http://github.com/ceph/ceph/pull/8224>`_, Nathan Cutler)
* releases: what is merged where and when ? (`pr#8358 <http://github.com/ceph/ceph/pull/8358>`_, Loic Dachary)
* rgw/rgw_admin:fix bug about list and stats command (`pr#8200 <http://github.com/ceph/ceph/pull/8200>`_, Qiankun Zheng)
* rgw: Do not send a Content-Type on a '304 Not Modified' response (`issue#15119 <http://tracker.ceph.com/issues/15119>`_, `pr#8253 <http://github.com/ceph/ceph/pull/8253>`_, Wido den Hollander)
* rgw: Multipart ListPartsResult ETag quotes (`issue#15334 <http://tracker.ceph.com/issues/15334>`_, `pr#8387 <http://github.com/ceph/ceph/pull/8387>`_, Robin H. Johnson)
* rgw: S3: set EncodingType in ListBucketResult (`pr#7712 <http://github.com/ceph/ceph/pull/7712>`_, Victor Makarov)
* rgw: accept data only at the first time in response to a request (`pr#8084 <http://github.com/ceph/ceph/pull/8084>`_, sunspot)
* rgw: add a few more help options in admin interface (`pr#8410 <http://github.com/ceph/ceph/pull/8410>`_, Abhishek Lekshmanan)
* rgw: add zone delete to rgw-admin help (`pr#8184 <http://github.com/ceph/ceph/pull/8184>`_, Abhishek Lekshmanan)
* rgw: convert plain object to versioned (with null version) when removing (`issue#15243 <http://tracker.ceph.com/issues/15243>`_, `pr#8268 <http://github.com/ceph/ceph/pull/8268>`_, Yehuda Sadeh)
* rgw: fix compiling error (`pr#8394 <http://github.com/ceph/ceph/pull/8394>`_, xie xingguo)
* rgw: fix lockdep false positive (`pr#8284 <http://github.com/ceph/ceph/pull/8284>`_, Yehuda Sadeh)
* rgw:Use count fn in RGWUserBuckets for quota check (`pr#8294 <http://github.com/ceph/ceph/pull/8294>`_, Abhishek Lekshmanan)
* rgw_admin: remove unused parent_period arg (`pr#8411 <http://github.com/ceph/ceph/pull/8411>`_, Abhishek Lekshmanan)
* rgw_file: set owner uid, gid, and Unix mode on new objects (`pr#8321 <http://github.com/ceph/ceph/pull/8321>`_, Matt Benjamin)
* rpm: prefer UID/GID 167 when creating ceph user/group (`issue#15246 <http://tracker.ceph.com/issues/15246>`_, `pr#8277 <http://github.com/ceph/ceph/pull/8277>`_, Nathan Cutler)
* script: subscription-manager support (`issue#14972 <http://tracker.ceph.com/issues/14972>`_, `pr#7907 <http://github.com/ceph/ceph/pull/7907>`_, Loic Dachary)
* script: subscription-manager support (part 2) (`issue#14972 <http://tracker.ceph.com/issues/14972>`_, `pr#8293 <http://github.com/ceph/ceph/pull/8293>`_, Loic Dachary)
* script: subscription-manager support (part 3) (`pr#8334 <http://github.com/ceph/ceph/pull/8334>`_, Loic Dachary)
* set 128MB tcmalloc cache size by bytes (`pr#8427 <http://github.com/ceph/ceph/pull/8427>`_, Star Guo)
* systemd: set up environment in rbdmap unit file (`issue#14984 <http://tracker.ceph.com/issues/14984>`_, `pr#8222 <http://github.com/ceph/ceph/pull/8222>`_, Nathan Cutler)
* test/system/*: use dynamically generated pool name (`issue#15240 <http://tracker.ceph.com/issues/15240>`_, `pr#8318 <http://github.com/ceph/ceph/pull/8318>`_, Kefu Chai)
* test/system/rados_list_parallel: print oid if rados_write fails (`issue#15240 <http://tracker.ceph.com/issues/15240>`_, `pr#8309 <http://github.com/ceph/ceph/pull/8309>`_, Kefu Chai)
* test/test-erasure-code.sh: disable pg_temp priming (`issue#15211 <http://tracker.ceph.com/issues/15211>`_, `pr#8260 <http://github.com/ceph/ceph/pull/8260>`_, Sage Weil)
* test/test_pool_create.sh: fix port (`pr#8361 <http://github.com/ceph/ceph/pull/8361>`_, Sage Weil)
* test: Fix test to run with btrfs which has snap_### dirs (`issue#15347 <http://tracker.ceph.com/issues/15347>`_, `pr#8420 <http://github.com/ceph/ceph/pull/8420>`_, David Zafman)
* test: TestMirroringWatcher test cases were not closing images (`pr#8435 <http://github.com/ceph/ceph/pull/8435>`_, Jason Dillaman)
* test: rbd-mirror: script improvements for manual testing (`pr#8325 <http://github.com/ceph/ceph/pull/8325>`_, Mykola Golub)
* tests: Fixing broken test/cephtool-test-mon.sh test (`pr#8429 <http://github.com/ceph/ceph/pull/8429>`_, Erwan Velu)
* tests: Improving 'make check' execution time (`pr#8131 <http://github.com/ceph/ceph/pull/8131>`_, Erwan Velu)
* unittest_erasure_code_plugin: fix deadlock (Alpine) (`pr#8314 <http://github.com/ceph/ceph/pull/8314>`_, John Coyle)
* vstart: fix up cmake paths when VSTART_DEST is given (`pr#8363 <http://github.com/ceph/ceph/pull/8363>`_, Casey Bodley)
* vstart: make -k with optional mon_num. (`pr#8251 <http://github.com/ceph/ceph/pull/8251>`_, Jianpeng Ma)
* xio: add prefix to xio msgr logs (`pr#8148 <http://github.com/ceph/ceph/pull/8148>`_, Roi Dayan)
* xio: fix compilation against latest accelio (`pr#8022 <http://github.com/ceph/ceph/pull/8022>`_, Roi Dayan)
* xio: xio_init needs to be called before any other xio function (`pr#8227 <http://github.com/ceph/ceph/pull/8227>`_, Roi Dayan)
* ceph.spec.in: disable lttng and babeltrace explicitly (`issue#14844 <http://tracker.ceph.com/issues/14844>`_, `pr#7857 <http://github.com/ceph/ceph/pull/7857>`_, Kefu Chai)
    ceph.spec.in: enable lttng and babeltrace explicitly
    thanks to ktdreyer, we have lttng and babel in EPEL-7 now.

v10.1.0 Jewel (release candidate)
=================================

This major release of Ceph will be the foundation for the next
long-term stable release.  There have been many major changes since
the Infernalis (9.2.x) and Hammer (0.94.x) releases, and the upgrade
process is non-trivial. Please read these release notes carefully.

There are a few known issues with this release candidate; see below.

Known Issues with v10.1.0
-------------------------

* While running a mixed version cluster of jewel and infernalis or
  hammer monitors, any MDSMap updates will cause the pre-jewel
  monitors to crash.  Workaround is to simply upgrde all monitors.
  There is a fix but it is still being tested.

* Some of the rbd-mirror functionality for switching between active
  and replica images is not yet merged.


Major Changes from Infernalis
-----------------------------

- *CephFS*:

  * This is the first release in which CephFS is declared stable and
    production ready!  Several features are disabled by default, including
    snapshots and multiple active MDS servers.
  * The repair and disaster recovery tools are now feature complete.
  * A new cephfs-volume-manager module is included that provides a
    high-level interface for creating "shares" for OpenStack Manila
    and similar projects.
  * There is now experimental support for multiple CephFS file systems
    within a single cluster.
  
- *RGW*:

  * The multisite feature has been almost completely rearchitected and
    rewritten to support any number of clusters/sites, bidirectional
    fail-over, and active/active configurations.
  * You can now access radosgw buckets via NFS (experimental).
  * The AWS4 authentication protocol is now supported.
  * There is now support for S3 request payer buckets.
  * The new multitenancy infrastructure improves compatibility with
    Swift, which provides separate container namespace for each
    user/tenant.
  * The OpenStack Keystone v3 API is now supported.  There are a range
    of other small Swift API features and compatibility improvements
    as well, including bulk delete and SLO (static large objects).

- *RBD*:

  * There is new support for mirroring (asynchronous replication) of
    RBD images across clusters.  This is implemented as a per-RBD
    image journal that can be streamed across a WAN to another site,
    and a new rbd-mirror daemon that performs the cross-cluster
    replication.
  * The exclusive-lock, object-map, fast-diff, and journaling features
    can be enabled or disabled dynamically. The deep-flatten can be
    disabled dynamically but not re-enabled.
  * The RBD CLI has been rewritten to provide command-specific help
    and full bash completion support.
  * RBD snapshots can now be renamed.

- *RADOS*:

  * BlueStore, a new OSD backend, is included as an experimental
    feature.  The plan is for it to become the default backend in the
    K or L release.
  * The OSD now persists scrub results and provides a librados to
    query results in detail, including the nature of inconsistencies
    found, the ability to fetch alternate versions of the same
    specific object (if any), and fine-grained control over repair.

Major Changes from Hammer
-------------------------

- *General*:

  * Ceph daemons are now managed via systemd (with the exception of
    Ubuntu Trusty, which still uses upstart).
  * Ceph daemons run as 'ceph' user instead root.
  * On Red Hat distros, there is also an SELinux policy.

- *RADOS*:

  * The RADOS cache tier can now proxy write operations to the base
    tier, allowing writes to be handled without forcing migration of
    an object into the cache.
  * The SHEC erasure coding support is no longer flagged as
    experimental. SHEC trades some additional storage space for faster
    repair.
  * There is now a unified queue (and thus prioritization) of client
    IO, recovery, scrubbing, and snapshot trimming.
  * There have been many improvements to low-level repair tooling
    (ceph-objectstore-tool).
  * The internal ObjectStore API has been significantly cleaned up in order
    to faciliate new storage backends like NewStore.

- *RGW*:

  * The Swift API now supports object expiration.
  * There are many Swift API compatibility improvements.

- *RBD*:

  * The ``rbd du`` command shows actual usage (quickly, when
    object-map is enabled).
  * The object-map feature has seen many stability improvements.
  * Object-map and exclusive-lock features can be enabled or disabled
    dynamically.
  * You can now store user metadata and set persistent librbd options
    associated with individual images.
  * The new deep-flatten features allow flattening of a clone and all
    of its snapshots.  (Previously snapshots could not be flattened.)
  * The export-diff command is now faster (it uses aio).  There is also
    a new fast-diff feature.
  * The --size argument can be specified with a suffix for units
    (e.g., ``--size 64G``).
  * There is a new ``rbd status`` command that, for now, shows who has
    the image open/mapped.

- *CephFS*:

  * You can now rename snapshots.
  * There have been ongoing improvements around administration, diagnostics,
    and the check and repair tools.
  * The caching and revocation of client cache state due to unused
    inodes has been dramatically improved.
  * The ceph-fuse client behaves better on 32-bit hosts.

Distro compatibility
--------------------

Starting with Infernalis, we have dropped support for many older
distributions so that we can move to a newer compiler toolchain (e.g.,
C++11).  Although it is still possible to build Ceph on older
distributions by installing backported development tools, we are not
building and publishing release packages for ceph.com.

We now build packages for:

* CentOS 7.x.  We have dropped support for CentOS 6 (and other RHEL 6
  derivatives, like Scientific Linux 6).
* Debian Jessie 8.x.  Debian Wheezy 7.x's g++ has incomplete support
  for C++11 (and no systemd).
* Ubuntu Trusty 14.04 and Ubuntu Xenial.  Ubuntu Precise 12.04 is no
  longer supported.
* Fedora 22 or later.

Upgrading from Firefly
----------------------

Upgrading directly from Firefly v0.80.z is not recommended.  It is
possible to do a direct upgrade, but not without downtime.  We
recommend that clusters are first upgraded to Hammer v0.94.6 or a
later v0.94.z release; only then is it possible to upgrade to
Jewel 10.2.z for an online upgrade (see below).

To do an offline upgrade directly from Firefly, all Firefly OSDs must
be stopped and marked down before any Jewel OSDs will be allowed
to start up.  This fencing is enforced by the Jewel monitor, so
use an upgrade procedure like:

  #. Upgrade Ceph on monitor hosts
  #. Restart all ceph-mon daemons
  #. Upgrade Ceph on all OSD hosts
  #. Stop all ceph-osd daemons
  #. Mark all OSDs down with something like::
       ceph osd down `seq 0 1000`
  #. Start all ceph-osd daemons
  #. Upgrade and restart remaining daemons (ceph-mds, radosgw)

Upgrading from Hammer
---------------------

* All cluster nodes must first upgrade to Hammer v0.94.4 or a later
  v0.94.z release; only then is it possible to upgrade to Jewel
  10.2.z.

* For all distributions that support systemd (CentOS 7, Fedora, Debian
  Jessie 8.x, OpenSUSE), ceph daemons are now managed using native systemd
  files instead of the legacy sysvinit scripts.  For example,::

    systemctl start ceph.target       # start all daemons
    systemctl status ceph-osd@12      # check status of osd.12

  The main notable distro that is *not* yet using systemd is Ubuntu trusty
  14.04.  (The next Ubuntu LTS, 16.04, will use systemd instead of upstart.)

* Ceph daemons now run as user and group ``ceph`` by default.  The
  ceph user has a static UID assigned by Fedora and Debian (also used
  by derivative distributions like RHEL/CentOS and Ubuntu).  On SUSE
  the ceph user will currently get a dynamically assigned UID when the
  user is created.

  If your systems already have a ceph user, upgrading the package will cause
  problems.  We suggest you first remove or rename the existing 'ceph' user
  and 'ceph' group before upgrading.

  When upgrading, administrators have two options:

   #. Add the following line to ``ceph.conf`` on all hosts::

        setuser match path = /var/lib/ceph/$type/$cluster-$id

      This will make the Ceph daemons run as root (i.e., not drop
      privileges and switch to user ceph) if the daemon's data
      directory is still owned by root.  Newly deployed daemons will
      be created with data owned by user ceph and will run with
      reduced privileges, but upgraded daemons will continue to run as
      root.

   #. Fix the data ownership during the upgrade.  This is the
      preferred option, but it is more work and can be very time
      consuming.  The process for each host is to:

      #. Upgrade the ceph package.  This creates the ceph user and group.  For
	 example::

	   ceph-deploy install --stable jewel HOST

      #. Stop the daemon(s).::

	   service ceph stop           # fedora, centos, rhel, debian
	   stop ceph-all               # ubuntu

      #. Fix the ownership::

	   chown -R ceph:ceph /var/lib/ceph

      #. Restart the daemon(s).::

	   start ceph-all                # ubuntu
	   systemctl start ceph.target   # debian, centos, fedora, rhel

      Alternatively, the same process can be done with a single daemon
      type, for example by stopping only monitors and chowning only
      ``/var/lib/ceph/mon``.

* The on-disk format for the experimental KeyValueStore OSD backend has
  changed.  You will need to remove any OSDs using that backend before you
  upgrade any test clusters that use it.

* When a pool quota is reached, librados operations now block indefinitely,
  the same way they do when the cluster fills up.  (Previously they would return
  -ENOSPC).  By default, a full cluster or pool will now block.  If your
  librados application can handle ENOSPC or EDQUOT errors gracefully, you can
  get error returns instead by using the new librados OPERATION_FULL_TRY flag.

* The return code for librbd's rbd_aio_read and Image::aio_read API methods no
  longer returns the number of bytes read upon success.  Instead, it returns 0
  upon success and a negative value upon failure.

* 'ceph scrub', 'ceph compact' and 'ceph sync force' are now DEPRECATED.  Users
  should instead use 'ceph mon scrub', 'ceph mon compact' and
  'ceph mon sync force'.

* 'ceph mon_metadata' should now be used as 'ceph mon metadata'. There is no
  need to deprecate this command (same major release since it was first
  introduced).

* The `--dump-json` option of "osdmaptool" is replaced by `--dump json`.

* The commands of "pg ls-by-{pool,primary,osd}" and "pg ls" now take "recovering"
  instead of "recovery", to include the recovering pgs in the listed pgs.

Upgrading from Infernalis
-------------------------

* There are no major compatibility changes since Infernalis.  Simply
  upgrading the daemons on each host and restarting all daemons is
  sufficient.

* The rbd CLI no longer accepts the deprecated '--image-features' option
  during create, import, and clone operations.  The '--image-feature'
  option should be used instead.

* The rbd legacy image format (version 1) is deprecated with the Jewel release.
  Attempting to create a new version 1 RBD image will result in a warning.
  Future releases of Ceph will remove support for version 1 RBD images.

* The 'send_pg_creates' and 'map_pg_creates' mon CLI commands are
  obsolete and no longer supported.

* A new configure option 'mon_election_timeout' is added to specifically
  limit max waiting time of monitor election process, which was previously
  restricted by 'mon_lease'.

* CephFS filesystems created using versions older than Firefly (0.80) must
  use the new "cephfs-data-scan tmap_upgrade" command after upgrading to
  Jewel.  See 'Upgrading' in the CephFS documentation for more information.

* The 'ceph mds setmap' command has been removed.

* The default RBD image features for new images have been updated to
  enable the following: exclusive lock, object map, fast-diff, and
  deep-flatten. These features are not currently supported by the RBD
  kernel driver nor older RBD clients. These features can be disabled on
  a per-image basis via the RBD CLI or the default features can be
  updated to the pre-Jewel setting by adding the following to the client
  section of the Ceph configuration file::

    rbd default features = 1

* The rbd legacy image format (version 1) is deprecated with the Jewel
  release.

* After upgrading, users should set the 'sortbitwise' flag to enable the new
  internal object sort order::

    ceph osd set sortbitwise

  This flag is important for the new object enumeration API and for
  new backends like BlueStore.

Notable Changes since v10.0.4
-----------------------------

* ceph.spec.in: do not install Ceph RA on systemd platforms (`issue#14828 <http://tracker.ceph.com/issues/14828>`_, `pr#7894 <http://github.com/ceph/ceph/pull/7894>`_, Nathan Cutler)
* mdsa: A few more snapshot fixes, mostly around snapshotted inode/dentry tracking (`pr#7798 <http://github.com/ceph/ceph/pull/7798>`_, Yan, Zheng)
* AUTHORS: update email (`pr#7854 <http://github.com/ceph/ceph/pull/7854>`_, Yehuda Sadeh)
* ceph-disk: Add --setuser and --setgroup options for ceph-disk (`pr#7351 <http://github.com/ceph/ceph/pull/7351>`_, Mike Shuey)
* build: Adding build requires (`pr#7742 <http://github.com/ceph/ceph/pull/7742>`_, Erwan Velu)
* msg/async: AsyncMessenger: fix several bugs (`pr#7831 <http://github.com/ceph/ceph/pull/7831>`_, Haomai Wang)
* msg/async: AsyncMessenger: fix valgrind leak (`pr#7725 <http://github.com/ceph/ceph/pull/7725>`_, Haomai Wang)
* doc: Clarify usage on starting single osd/mds/mon. (`pr#7641 <http://github.com/ceph/ceph/pull/7641>`_, Patrick Donnelly)
* common: Deprecate or free up a bunch of feature bits (`pr#8214 <http://github.com/ceph/ceph/pull/8214>`_, Samuel Just)
* msg/async: Event: fix clock skew problem (`pr#7949 <http://github.com/ceph/ceph/pull/7949>`_, Wei Jin)
* osd: FileStore: Added O_DSYNC write scheme (`pr#7752 <http://github.com/ceph/ceph/pull/7752>`_, Somnath Roy)
* osd: FileStore: fix initialization order for m_disable_wbthrottle (`pr#8067 <http://github.com/ceph/ceph/pull/8067>`_, Samuel Just)
* build: Fixing BTRFS issue at 'make check' (`pr#7805 <http://github.com/ceph/ceph/pull/7805>`_, Erwan Velu)
* build: FreeBSD related fixes (`pr#7170 <http://github.com/ceph/ceph/pull/7170>`_, Mykola Golub)
* ceph-disk: Improving 'make check' for ceph-disk (`pr#7762 <http://github.com/ceph/ceph/pull/7762>`_, Erwan Velu)
* rgw: increase verbosity level on RGWObjManifest line (`pr#7285 <http://github.com/ceph/ceph/pull/7285>`_, magicrobotmonkey)
* build: workaround an automake bug for "make check" (`issue#14723 <http://tracker.ceph.com/issues/14723>`_, `pr#7626 <http://github.com/ceph/ceph/pull/7626>`_, Kefu Chai)
* ceph-fuse,libcephfs: Fix client handling of "lost" open directories on shutdown (`issue#14996 <http://tracker.ceph.com/issues/14996>`_, `pr#7994 <http://github.com/ceph/ceph/pull/7994>`_, Yan, Zheng)
* mds: Multi-filesystem support (`issue#14952 <http://tracker.ceph.com/issues/14952>`_, `pr#6953 <http://github.com/ceph/ceph/pull/6953>`_, John Spray, Sage Weil)
* os/bluestore/NVMEDevice: refactor probe/attach codes and support zero command (`pr#7647 <http://github.com/ceph/ceph/pull/7647>`_, Haomai Wang)
* librados: detect laggy ops with objecter_timeout, not osd_timeout (`pr#7629 <http://github.com/ceph/ceph/pull/7629>`_, Greg Farnum)
* ceph.spec.in: fix openldap and openssl build dependencies for SUSE (`issue#15138 <http://tracker.ceph.com/issues/15138>`_, `pr#8120 <http://github.com/ceph/ceph/pull/8120>`_, Nathan Cutler)
* osd: repop and lost-unfound overhaul (`pr#7765 <http://github.com/ceph/ceph/pull/7765>`_, Samuel Just)
* librbd: Revert "librbd: use task finisher per CephContext" (`issue#14780 <http://tracker.ceph.com/issues/14780>`_, `pr#7667 <http://github.com/ceph/ceph/pull/7667>`_, Josh Durgin)
* rgw: Fix subuser harder with tenants (`pr#7618 <http://github.com/ceph/ceph/pull/7618>`_, Pete Zaitcev)
* rgw: ldap fixes (`pr#8168 <http://github.com/ceph/ceph/pull/8168>`_, Matt Benjamin)
* rgw: check the return value when call fe->run() (`issue#14585 <http://tracker.ceph.com/issues/14585>`_, `pr#7457 <http://github.com/ceph/ceph/pull/7457>`_, wei qiaomiao)
* rgw: Revert "rgw ldap" (`pr#8075 <http://github.com/ceph/ceph/pull/8075>`_, Yehuda Sadeh)
* librados: Striper: Fix incorrect push_front -> append_zero change (`pr#7578 <http://github.com/ceph/ceph/pull/7578>`_, Haomai Wang)
* rgw: build-related fixes (`pr#8076 <http://github.com/ceph/ceph/pull/8076>`_, Yehuda Sadeh, Matt Benjamin)
* mirrors: Updated scripts and documentation for mirrors (`pr#7847 <http://github.com/ceph/ceph/pull/7847>`_, Wido den Hollander)
* misc: use make_shared while creating shared_ptr (`pr#7769 <http://github.com/ceph/ceph/pull/7769>`_, Somnath Roy)
* cmake (`pr#7849 <http://github.com/ceph/ceph/pull/7849>`_, Ali Maredia)
* mds: filelock deadlock (`pr#7713 <http://github.com/ceph/ceph/pull/7713>`_, Yan, Zheng)
* mds: fix fsmap decode (`pr#8063 <http://github.com/ceph/ceph/pull/8063>`_, Greg Farnum)
* rgw: fix mdlog (`pr#8183 <http://github.com/ceph/ceph/pull/8183>`_, Orit Wasserman)
* rgw: highres time stamps (`pr#8108 <http://github.com/ceph/ceph/pull/8108>`_, Yehuda Sadeh, Adam C. Emerson, Matt Benjamin)
* rgw: swift versioning disabled (`pr#8066 <http://github.com/ceph/ceph/pull/8066>`_, Yehuda Sadeh, Radoslaw Zarzynski)
* rgw: sync fixes 3 (`pr#8170 <http://github.com/ceph/ceph/pull/8170>`_, Yehuda Sadeh)
* msg/xio: fixes (`pr#7603 <http://github.com/ceph/ceph/pull/7603>`_, Roi Dayan)
* ceph-fuse,libcephfs: fix free fds being exhausted eventually because freed fds are never put back (`issue#14798 <http://tracker.ceph.com/issues/14798>`_, `pr#7685 <http://github.com/ceph/ceph/pull/7685>`_, Zhi Zhang)
* rgw: RGWLib::env is not used so remove it (`pr#7874 <http://github.com/ceph/ceph/pull/7874>`_, Brad Hubbard)
* build: a few armhf (32-bit build) fixes (`pr#7999 <http://github.com/ceph/ceph/pull/7999>`_, Eric Lee, Sage Weil)
* osd: add scrub persist/query API (`issue#13505 <http://tracker.ceph.com/issues/13505>`_, `pr#6898 <http://github.com/ceph/ceph/pull/6898>`_, Kefu Chai, Samuel Just)
* rgw: adds the radosgw-admin sync status command that gives a human readable status of the sync process at a specific zone (`pr#8030 <http://github.com/ceph/ceph/pull/8030>`_, Yehuda Sadeh)
* scripts: adjust mstart and mstop script to run with cmake build (`pr#6920 <http://github.com/ceph/ceph/pull/6920>`_, Orit Wasserman)
* buffer: add symmetry operator==() and operator!=() (`pr#7974 <http://github.com/ceph/ceph/pull/7974>`_, Kefu Chai)
* buffer: hide iterator_impl symbols (`issue#14788 <http://tracker.ceph.com/issues/14788>`_, `pr#7688 <http://github.com/ceph/ceph/pull/7688>`_, Kefu Chai)
* buffer: increment history alloc as well in raw_combined (`issue#14955 <http://tracker.ceph.com/issues/14955>`_, `pr#7910 <http://github.com/ceph/ceph/pull/7910>`_, Samuel Just)
* buffer: raw_combined allocations buffer and ref count together (`pr#7612 <http://github.com/ceph/ceph/pull/7612>`_, Sage Weil)
* ceph-detect-init: add debian/jessie test (`pr#8074 <http://github.com/ceph/ceph/pull/8074>`_, Kefu Chai)
* ceph-detect-init: add missing test case (`pr#8105 <http://github.com/ceph/ceph/pull/8105>`_, Nathan Cutler)
* ceph-detect-init: fix py3 test (`pr#7243 <http://github.com/ceph/ceph/pull/7243>`_, Kefu Chai)
* ceph-detect-init: return correct value on recent SUSE distros (`issue#14770 <http://tracker.ceph.com/issues/14770>`_, `pr#7909 <http://github.com/ceph/ceph/pull/7909>`_, Nathan Cutler)
* ceph-disk: deactivate / destroy PATH arg are optional (`pr#7756 <http://github.com/ceph/ceph/pull/7756>`_, Loic Dachary)
* ceph-disk: fix prepare --help (`pr#7758 <http://github.com/ceph/ceph/pull/7758>`_, Loic Dachary)
* ceph-disk: flake8 fixes (`pr#7646 <http://github.com/ceph/ceph/pull/7646>`_, Loic Dachary)
* ceph-disk: key management support (`issue#14669 <http://tracker.ceph.com/issues/14669>`_, `pr#7552 <http://github.com/ceph/ceph/pull/7552>`_, Loic Dachary)
* ceph-disk: make some arguments as required if necessary (`pr#7687 <http://github.com/ceph/ceph/pull/7687>`_, Dongsheng Yang)
* ceph-disk: s/dmcrpyt/dmcrypt/ (`issue#14838 <http://tracker.ceph.com/issues/14838>`_, `pr#7744 <http://github.com/ceph/ceph/pull/7744>`_, Loic Dachary, Frode Sandholtbraaten)
* ceph-fuse: Fix potential filehandle ref leak at umount (`issue#14800 <http://tracker.ceph.com/issues/14800>`_, `pr#7686 <http://github.com/ceph/ceph/pull/7686>`_, Zhi Zhang)
* ceph.in: Minor python3 specific changes (`pr#7947 <http://github.com/ceph/ceph/pull/7947>`_, Sarthak Munshi)
* ceph_daemon.py: Resolved ImportError to work with python3 (`pr#7937 <http://github.com/ceph/ceph/pull/7937>`_, Sarthak Munshi)
* ceph_detect_init/__init__.py: remove shebang (`pr#7731 <http://github.com/ceph/ceph/pull/7731>`_, Nathan Cutler)
* ceph_test_msgr: reduce test size to fix memory size (`pr#8127 <http://github.com/ceph/ceph/pull/8127>`_, Haomai Wang)
* ceph_test_rados_misc: shorten mount timeout (`pr#8209 <http://github.com/ceph/ceph/pull/8209>`_, Sage Weil)
* cleanup (`pr#8058 <http://github.com/ceph/ceph/pull/8058>`_, Yehuda Sadeh, Orit Wasserman)
* client: flush kernel pagecache before creating snapshot (`issue#10436 <http://tracker.ceph.com/issues/10436>`_, `pr#7495 <http://github.com/ceph/ceph/pull/7495>`_, Yan, Zheng)
* client: removed unused Mutex from MetaRequest (`pr#7655 <http://github.com/ceph/ceph/pull/7655>`_, Greg Farnum)
* cls/rgw: fix FTBFS (`pr#8142 <http://github.com/ceph/ceph/pull/8142>`_, Kefu Chai)
* cls/rgw: fix use of timespan (`issue#15181 <http://tracker.ceph.com/issues/15181>`_, `pr#8212 <http://github.com/ceph/ceph/pull/8212>`_, Yehuda Sadeh)
* cls_hello: Fix grammatical error in description comment (`pr#7951 <http://github.com/ceph/ceph/pull/7951>`_, Brad Hubbard)
* cls_rbd: fix the test for ceph-dencoder (`pr#7793 <http://github.com/ceph/ceph/pull/7793>`_, Kefu Chai)
* cls_rbd: mirroring directory (`issue#14419 <http://tracker.ceph.com/issues/14419>`_, `pr#7620 <http://github.com/ceph/ceph/pull/7620>`_, Josh Durgin)
* cls_rbd: protect against excessively large object maps (`issue#15121 <http://tracker.ceph.com/issues/15121>`_, `pr#8099 <http://github.com/ceph/ceph/pull/8099>`_, Jason Dillaman)
* cmake: Remove duplicate find_package libcurl line. (`pr#7972 <http://github.com/ceph/ceph/pull/7972>`_, Brad Hubbard)
* cmake: add ErasureCode.cc to jerasure plugins (`pr#7808 <http://github.com/ceph/ceph/pull/7808>`_, Casey Bodley)
* cmake: add common/fs_types.cc to libcommon (`pr#7898 <http://github.com/ceph/ceph/pull/7898>`_, Orit Wasserman)
* cmake: add missing librbd image_watcher sources (`issue#14823 <http://tracker.ceph.com/issues/14823>`_, `pr#7717 <http://github.com/ceph/ceph/pull/7717>`_, Casey Bodley)
* cmake: avoid false-positive LDAP header detect (`pr#8100 <http://github.com/ceph/ceph/pull/8100>`_, Matt Benjamin)
* cmake: fix paths to various EC source files (`pr#7748 <http://github.com/ceph/ceph/pull/7748>`_, Ali Maredia, Matt Benjamin)
* cmake: fix the build of tests (`pr#7523 <http://github.com/ceph/ceph/pull/7523>`_, Kefu Chai)
* common/TrackedOp: fix inaccurate counting for slow requests (`issue#14804 <http://tracker.ceph.com/issues/14804>`_, `pr#7690 <http://github.com/ceph/ceph/pull/7690>`_, xie xingguo)
* common/bit_vector: use hard-coded value for block size (`issue#14747 <http://tracker.ceph.com/issues/14747>`_, `pr#7610 <http://github.com/ceph/ceph/pull/7610>`_, Jason Dillaman)
* common/obj_bencher.cc: bump the precision of bandwidth field (`pr#8021 <http://github.com/ceph/ceph/pull/8021>`_, Piotr Dałek)
* common/obj_bencher.cc: faster object name generation (`pr#7863 <http://github.com/ceph/ceph/pull/7863>`_, Piotr Dałek)
* common/obj_bencher.cc: make verify error fatal (`issue#14971 <http://tracker.ceph.com/issues/14971>`_, `pr#7897 <http://github.com/ceph/ceph/pull/7897>`_, Piotr Dałek)
* common/page.cc: _page_mask has too many bits (`pr#7588 <http://github.com/ceph/ceph/pull/7588>`_, Dan Mick)
* common/strtol.cc: fix the coverity warnings (`pr#7967 <http://github.com/ceph/ceph/pull/7967>`_, Kefu Chai)
* common: Do not use non-portable constants in mutex_debug (`pr#7766 <http://github.com/ceph/ceph/pull/7766>`_, Adam C. Emerson)
* common: SubProcess: Avoid buffer corruption when calling err() (`issue#15011 <http://tracker.ceph.com/issues/15011>`_, `pr#8054 <http://github.com/ceph/ceph/pull/8054>`_, Erwan Velu)
* common: default cluster name to config file prefix (`pr#7364 <http://github.com/ceph/ceph/pull/7364>`_, Javen Wu)
* common: set thread name from correct thread (`pr#7845 <http://github.com/ceph/ceph/pull/7845>`_, Igor Podoski)
* common: various fixes from SCA runs (`pr#7680 <http://github.com/ceph/ceph/pull/7680>`_, Danny Al-Gaaf)
* config: fix osd_crush_initial_weight (`pr#7975 <http://github.com/ceph/ceph/pull/7975>`_, You Ji)
* config: increase default async op threads (`pr#7802 <http://github.com/ceph/ceph/pull/7802>`_, Piotr Dałek)
* configure.ac: boost_iostreams is required, not optional (`pr#7816 <http://github.com/ceph/ceph/pull/7816>`_, Hector Martin)
* configure.ac: update help strings for cython (`pr#7856 <http://github.com/ceph/ceph/pull/7856>`_, Josh Durgin)
* crush/CrushTester: workaround a bug in boost::icl (`pr#7560 <http://github.com/ceph/ceph/pull/7560>`_, Kefu Chai)
* crush: fix cli tests for new crush tunables (`pr#8107 <http://github.com/ceph/ceph/pull/8107>`_, Sage Weil)
* crush: update tunable docs.  change default profile to jewel (`pr#7964 <http://github.com/ceph/ceph/pull/7964>`_, Sage Weil)
* debian/changelog: Remove stray 'v' in version (`pr#7936 <http://github.com/ceph/ceph/pull/7936>`_, Dan Mick)
* debian/changelog: Remove stray 'v' in version (`pr#7938 <http://github.com/ceph/ceph/pull/7938>`_, Dan Mick)
* debian: include cpio in build-requiers (`pr#7533 <http://github.com/ceph/ceph/pull/7533>`_, Rémi BUISSON)
* debian: package librgw_file* tests (`pr#7930 <http://github.com/ceph/ceph/pull/7930>`_, Ken Dreyer)
* doc/architecture.rst: remove redundant word "across" (`pr#8179 <http://github.com/ceph/ceph/pull/8179>`_, Zhao Junwang)
* doc/dev: add section on interrupting a running suite (`pr#8116 <http://github.com/ceph/ceph/pull/8116>`_, Nathan Cutler)
* doc/dev: continue writing Testing in the cloud chapter (`pr#7960 <http://github.com/ceph/ceph/pull/7960>`_, Nathan Cutler)
* doc/dev: integrate testing into the narrative (`pr#7946 <http://github.com/ceph/ceph/pull/7946>`_, Nathan Cutler)
* doc/dev: various refinements (`pr#7954 <http://github.com/ceph/ceph/pull/7954>`_, Nathan Cutler)
* doc/rados/api/librados-intro.rst: fix typo (`pr#7879 <http://github.com/ceph/ceph/pull/7879>`_, xie xingguo)
* doc: add ceph-detect-init(8) source to dist tarball (`pr#7933 <http://github.com/ceph/ceph/pull/7933>`_, Ken Dreyer)
* doc: add cinder backend section to rbd-openstack.rst (`pr#7923 <http://github.com/ceph/ceph/pull/7923>`_, RustShen)
* doc: detailed description of bugfixing workflow (`pr#7941 <http://github.com/ceph/ceph/pull/7941>`_, Nathan Cutler)
* doc: fix 0.94.4 and 0.94.5 ordering (`pr#7763 <http://github.com/ceph/ceph/pull/7763>`_, Loic Dachary)
* doc: fix typo, indention etc. (`pr#7829 <http://github.com/ceph/ceph/pull/7829>`_, xie xingguo)
* doc: initial draft of RBD mirroring admin documentation (`issue#15041 <http://tracker.ceph.com/issues/15041>`_, `pr#8169 <http://github.com/ceph/ceph/pull/8169>`_, Jason Dillaman)
* doc: osd-config Add Configuration Options for op queue. (`pr#7837 <http://github.com/ceph/ceph/pull/7837>`_, Robert LeBlanc)
* doc: rgw explain keystone's verify ssl switch (`pr#7862 <http://github.com/ceph/ceph/pull/7862>`_, Abhishek Lekshmanan)
* doc: small fixes (`pr#7813 <http://github.com/ceph/ceph/pull/7813>`_, xiexingguo)
* doc: standardize @param (not @parma, @parmam, @params) (`pr#7714 <http://github.com/ceph/ceph/pull/7714>`_, Nathan Cutler)
* fix FTBFS introduced by d0af316 (`pr#7792 <http://github.com/ceph/ceph/pull/7792>`_, Kefu Chai)
* ghobject_t: use # instead of ! as a separator (`pr#8055 <http://github.com/ceph/ceph/pull/8055>`_, Sage Weil)
* include/encoding: do not try to be clever with list encoding (`pr#7913 <http://github.com/ceph/ceph/pull/7913>`_, Sage Weil)
* init-ceph.in: allow case-insensitive true in `osd crush update on start' (`pr#7943 <http://github.com/ceph/ceph/pull/7943>`_, Eric Cook)
* init-ceph.in: skip ceph-disk if it is not present (`issue#10587 <http://tracker.ceph.com/issues/10587>`_, `pr#7286 <http://github.com/ceph/ceph/pull/7286>`_, Ken Dreyer)
* journal: async methods to (un)register and update client (`pr#7832 <http://github.com/ceph/ceph/pull/7832>`_, Mykola Golub)
* journal: improve commit position tracking (`pr#7776 <http://github.com/ceph/ceph/pull/7776>`_, Jason Dillaman)
* journal: prevent race injecting new records into overflowed object (`issue#15202 <http://tracker.ceph.com/issues/15202>`_, `pr#8220 <http://github.com/ceph/ceph/pull/8220>`_, Jason Dillaman)
* librados: cancel aio notification linger op upon completion (`pr#8102 <http://github.com/ceph/ceph/pull/8102>`_, Jason Dillaman)
* librados: check connection state in rados_monitor_log (`issue#14499 <http://tracker.ceph.com/issues/14499>`_, `pr#7350 <http://github.com/ceph/ceph/pull/7350>`_, David Disseldorp)
* librados: do not clear handle for aio_watch() (`pr#7771 <http://github.com/ceph/ceph/pull/7771>`_, xie xingguo)
* librados: fix test failure with new aio watch/unwatch API  (`pr#7824 <http://github.com/ceph/ceph/pull/7824>`_, Jason Dillaman)
* librados: implement async watch/unwatch (`pr#7649 <http://github.com/ceph/ceph/pull/7649>`_, Haomai Wang)
* librados: mix lock cycle (un)registering asok commands (`pr#7581 <http://github.com/ceph/ceph/pull/7581>`_, John Spray)
* librados: race condition on aio_notify completion handling (`pr#7864 <http://github.com/ceph/ceph/pull/7864>`_, Jason Dillaman)
* librados: stat2 with higher time precision (`pr#7915 <http://github.com/ceph/ceph/pull/7915>`_, Yehuda Sadeh, Matt Benjamin)
* librbd: allocate new journal tag after acquiring exclusive lock (`pr#7884 <http://github.com/ceph/ceph/pull/7884>`_, Jason Dillaman)
* librbd: block read requests until journal replayed (`pr#7627 <http://github.com/ceph/ceph/pull/7627>`_, Jason Dillaman)
* librbd: correct issues discovered via valgrind memcheck (`pr#8132 <http://github.com/ceph/ceph/pull/8132>`_, Jason Dillaman)
* librbd: differentiate journal replay flush vs shut down (`pr#7698 <http://github.com/ceph/ceph/pull/7698>`_, Jason Dillaman)
* librbd: enable/disable image mirroring automatically for pool mode (`issue#15143 <http://tracker.ceph.com/issues/15143>`_, `pr#8204 <http://github.com/ceph/ceph/pull/8204>`_, Ricardo Dias)
* librbd: fix state machine race conditions during shut down (`pr#7761 <http://github.com/ceph/ceph/pull/7761>`_, Jason Dillaman)
* librbd: handle unregistering the image watcher when disconnected (`pr#8094 <http://github.com/ceph/ceph/pull/8094>`_, Jason Dillaman)
* librbd: integrate journal replay with fsx testing (`pr#7583 <http://github.com/ceph/ceph/pull/7583>`_, Jason Dillaman)
* librbd: journal replay needs to support re-executing maintenance ops (`issue#14822 <http://tracker.ceph.com/issues/14822>`_, `pr#7785 <http://github.com/ceph/ceph/pull/7785>`_, Jason Dillaman)
* librbd: reduce mem copies to user-buffer during read (`pr#7548 <http://github.com/ceph/ceph/pull/7548>`_, Jianpeng Ma)
* librbd: refresh image if required before replaying journal ops (`issue#14908 <http://tracker.ceph.com/issues/14908>`_, `pr#7978 <http://github.com/ceph/ceph/pull/7978>`_, Jason Dillaman)
* librbd: remove last synchronous librados calls from open/close state machine (`pr#7839 <http://github.com/ceph/ceph/pull/7839>`_, Jason Dillaman)
* librbd: replaying a journal op post-refresh requires locking (`pr#8028 <http://github.com/ceph/ceph/pull/8028>`_, Jason Dillaman)
* librbd: retrieve image name when opening by id (`pr#7736 <http://github.com/ceph/ceph/pull/7736>`_, Mykola Golub)
* librbd: several race conditions discovered under single CPU environment (`pr#7653 <http://github.com/ceph/ceph/pull/7653>`_, Jason Dillaman)
* librbd: truncate does not need to mark the object as existing in the object map (`issue#14789 <http://tracker.ceph.com/issues/14789>`_, `pr#7772 <http://github.com/ceph/ceph/pull/7772>`_, xinxin shu)
* librbd: update of mirror pool mode and mirror peer handling (`pr#7718 <http://github.com/ceph/ceph/pull/7718>`_, Jason Dillaman)
* librbd: use async librados notifications (`pr#7668 <http://github.com/ceph/ceph/pull/7668>`_, Jason Dillaman)
* log: do not repeat errors to stderr (`issue#14616 <http://tracker.ceph.com/issues/14616>`_, `pr#7983 <http://github.com/ceph/ceph/pull/7983>`_, Sage Weil)
* log: fix stack overflow when flushing large log lines (`issue#14707 <http://tracker.ceph.com/issues/14707>`_, `pr#7599 <http://github.com/ceph/ceph/pull/7599>`_, Igor Fedotov)
* log: segv in a portable way (`issue#14856 <http://tracker.ceph.com/issues/14856>`_, `pr#7790 <http://github.com/ceph/ceph/pull/7790>`_, Kefu Chai)
* log: use delete[] (`pr#7904 <http://github.com/ceph/ceph/pull/7904>`_, Sage Weil)
* mailmap for 10.0.4 (`pr#7932 <http://github.com/ceph/ceph/pull/7932>`_, Abhishek Lekshmanan)
* mailmap updates (`pr#7528 <http://github.com/ceph/ceph/pull/7528>`_, Yann Dupont)
* man/8/ceph-disk: fix formatting issue (`pr#8012 <http://github.com/ceph/ceph/pull/8012>`_, Sage Weil)
* man/8/ceph-disk: fix formatting issue (`pr#8003 <http://github.com/ceph/ceph/pull/8003>`_, Sage Weil)
* mds, client: add namespace to file_layout_t (previously ceph_file_layout) (`pr#7098 <http://github.com/ceph/ceph/pull/7098>`_, Yan, Zheng, Sage Weil)
* mds: don't double-shutdown the timer when suiciding (`issue#14697 <http://tracker.ceph.com/issues/14697>`_, `pr#7616 <http://github.com/ceph/ceph/pull/7616>`_, Greg Farnum)
* mds: fix FSMap upgrade with daemons in the map (`pr#8073 <http://github.com/ceph/ceph/pull/8073>`_, John Spray, Greg Farnum)
* mds: fix inode_t::compare() (`issue#15038 <http://tracker.ceph.com/issues/15038>`_, `pr#8014 <http://github.com/ceph/ceph/pull/8014>`_, Yan, Zheng)
* mds: fix stray purging in 'stripe_count > 1' case (`issue#15050 <http://tracker.ceph.com/issues/15050>`_, `pr#8040 <http://github.com/ceph/ceph/pull/8040>`_, Yan, Zheng)
* mds: function parameter 'df' should be passed by reference (`pr#7490 <http://github.com/ceph/ceph/pull/7490>`_, Na Xie)
* mirrors: Change contact e-mail address for se.ceph.com (`pr#8007 <http://github.com/ceph/ceph/pull/8007>`_, Wido den Hollander)
* mon/PGMonitor: reliably mark PGs state (`pr#8089 <http://github.com/ceph/ceph/pull/8089>`_, Sage Weil)
* mon/monitor: some clean up (`pr#7520 <http://github.com/ceph/ceph/pull/7520>`_, huanwen ren)
* mon/pgmonitor: use appropriate forced conversions in get_rule_avail (`pr#7705 <http://github.com/ceph/ceph/pull/7705>`_, huanwen ren)
* mon: cleanup set-quota error msg (`pr#7371 <http://github.com/ceph/ceph/pull/7371>`_, Abhishek Lekshmanan)
* mon: consider pool size when creating pool (`issue#14509 <http://tracker.ceph.com/issues/14509>`_, `pr#7359 <http://github.com/ceph/ceph/pull/7359>`_, songbaisen)
* mon: enable 'mon osd prime pg temp' by default (`pr#7838 <http://github.com/ceph/ceph/pull/7838>`_, Robert LeBlanc)
* mon: fix calculation of %USED (`pr#7881 <http://github.com/ceph/ceph/pull/7881>`_, Adam Kupczyk)
* mon: fix keyring permissions (`issue#14950 <http://tracker.ceph.com/issues/14950>`_, `pr#7880 <http://github.com/ceph/ceph/pull/7880>`_, Owen Synge)
* mon: initialize last_* timestamps on new pgs to creation time (`issue#14952 <http://tracker.ceph.com/issues/14952>`_, `pr#7980 <http://github.com/ceph/ceph/pull/7980>`_, Sage Weil)
* mon: make clock skew checks sane (`issue#14175 <http://tracker.ceph.com/issues/14175>`_, `pr#7141 <http://github.com/ceph/ceph/pull/7141>`_, Joao Eduardo Luis)
* mon: osd [test-]reweight-by-{pg,utilization} command updates (`pr#7890 <http://github.com/ceph/ceph/pull/7890>`_, Dan van der Ster, Sage Weil)
* mon: remove 'mds setmap' (`issue#15136 <http://tracker.ceph.com/issues/15136>`_, `pr#8121 <http://github.com/ceph/ceph/pull/8121>`_, Sage Weil)
* mon: standardize Ceph removal commands (`pr#7939 <http://github.com/ceph/ceph/pull/7939>`_, Dongsheng Yang)
* mon: unconfuse object count skew message (`pr#7882 <http://github.com/ceph/ceph/pull/7882>`_, Piotr Dałek)
* mon: unregister command on shutdown (`pr#7504 <http://github.com/ceph/ceph/pull/7504>`_, huanwen ren)
* mount.fuse.ceph: better parsing of arguments passed to mount.fuse.ceph by mount command (`issue#14735 <http://tracker.ceph.com/issues/14735>`_, `pr#7607 <http://github.com/ceph/ceph/pull/7607>`_, Florent Bautista)
* msg/async: _try_send trim already sent for outcoming_bl more efficient (`pr#7970 <http://github.com/ceph/ceph/pull/7970>`_, Yan Jun)
* msg/async: don't calculate msg header crc when not needed (`pr#7815 <http://github.com/ceph/ceph/pull/7815>`_, Piotr Dałek)
* msg/async: smarter MSG_MORE (`pr#7625 <http://github.com/ceph/ceph/pull/7625>`_, Piotr Dałek)
* msg: add thread safety for "random" Messenger + fix wrong usage of random functions (`pr#7650 <http://github.com/ceph/ceph/pull/7650>`_, Avner BenHanoch)
* msg: async: fix perf counter description and simplify _send_keepalive_or_ack (`pr#8046 <http://github.com/ceph/ceph/pull/8046>`_, xie xingguo)
* msg: async: small cleanups (`pr#7871 <http://github.com/ceph/ceph/pull/7871>`_, xie xingguo)
* msg: async: start over after failing to bind a port in specified range (`issue#14928 <http://tracker.ceph.com/issues/14928>`_, `issue#13002 <http://tracker.ceph.com/issues/13002>`_, `pr#7852 <http://github.com/ceph/ceph/pull/7852>`_, xie xingguo)
* msg: remove duplicated code - local_delivery will now call 'enqueue' (`pr#7948 <http://github.com/ceph/ceph/pull/7948>`_, Avner BenHanoch)
* msg: significantly reduce minimal memory usage of connections (`pr#7567 <http://github.com/ceph/ceph/pull/7567>`_, Piotr Dałek)
* mstart: start rgw on different ports as well (`pr#8167 <http://github.com/ceph/ceph/pull/8167>`_, Abhishek Lekshmanan)
* nfs for rgw (Matt Benjamin, Orit Wasserman) (`pr#7634 <http://github.com/ceph/ceph/pull/7634>`_, Yehuda Sadeh, Matt Benjamin)
* os/ObjectStore: implement more efficient get_encoded_bytes()  (`pr#7775 <http://github.com/ceph/ceph/pull/7775>`_, Piotr Dałek)
* os/bluestore/BlueFS: Before reap ioct, it should wait io complete (`pr#8178 <http://github.com/ceph/ceph/pull/8178>`_, Jianpeng Ma)
* os/bluestore/BlueStore: Don't leak trim overlay data before write. (`pr#7895 <http://github.com/ceph/ceph/pull/7895>`_, Jianpeng Ma)
* os/bluestore/KernelDevice: force block size (`pr#8006 <http://github.com/ceph/ceph/pull/8006>`_, Sage Weil)
* os/bluestore/NVMEDevice: make IO thread using dpdk launch (`pr#8160 <http://github.com/ceph/ceph/pull/8160>`_, Haomai Wang)
* os/bluestore: clone overlay data (`pr#7860 <http://github.com/ceph/ceph/pull/7860>`_, Jianpeng Ma)
* os/bluestore: fix a typo in SPDK path parsing (`pr#7601 <http://github.com/ceph/ceph/pull/7601>`_, Jianjian Huo)
* os/bluestore: make bluestore_sync_transaction = true can work. (`pr#7674 <http://github.com/ceph/ceph/pull/7674>`_, Jianpeng Ma)
* os/bluestore: small fixes in bluestore StupidAllocator (`pr#8101 <http://github.com/ceph/ceph/pull/8101>`_, Jianjian Huo)
* os/filestore/FileJournal: set block size via config option (`pr#7628 <http://github.com/ceph/ceph/pull/7628>`_, Sage Weil)
* os/filestore: fix punch hole usage in _zero (`pr#8050 <http://github.com/ceph/ceph/pull/8050>`_, Sage Weil)
* os/filestore: fix result handling logic of destroy_collection (`pr#7721 <http://github.com/ceph/ceph/pull/7721>`_, xie xingguo)
* os/filestore: require offset == length == 0 for full object read; add test (`pr#7957 <http://github.com/ceph/ceph/pull/7957>`_, Jianpeng Ma)
* osd/OSDMap: fix typo in summarize_mapping_stats (`pr#8088 <http://github.com/ceph/ceph/pull/8088>`_, Sage Weil)
* osd/PGLog: fix warning (`pr#8057 <http://github.com/ceph/ceph/pull/8057>`_, Sage Weil)
* osd/ReplicatedPG: be more careful about calling publish_stats_to_osd() (`issue#14962 <http://tracker.ceph.com/issues/14962>`_, `pr#8039 <http://github.com/ceph/ceph/pull/8039>`_, Greg Farnum)
* osd/ReplicatedPG: clear watches on change after applying repops (`issue#15151 <http://tracker.ceph.com/issues/15151>`_, `pr#8163 <http://github.com/ceph/ceph/pull/8163>`_, Sage Weil)
* osd/ScrubStore: remove unused function (`pr#8045 <http://github.com/ceph/ceph/pull/8045>`_, Kefu Chai)
* osd: BlueStore/NVMEDevice: fix compiling and fd leak (`pr#7496 <http://github.com/ceph/ceph/pull/7496>`_, xie xingguo)
* osd: FileStore: use pwritev instead of lseek+writev (`pr#7349 <http://github.com/ceph/ceph/pull/7349>`_, Haomai Wang, Tao Chang)
* osd: OSDMap: reset osd_primary_affinity shared_ptr when deepish_copy_from (`issue#14686 <http://tracker.ceph.com/issues/14686>`_, `pr#7553 <http://github.com/ceph/ceph/pull/7553>`_, Xinze Chi)
* osd: Replace snprintf with faster implementation in eversion_t::get_key_name (`pr#7121 <http://github.com/ceph/ceph/pull/7121>`_, Evgeniy Firsov)
* osd: WeightedPriorityQueue: move to intrusive containers (`pr#7654 <http://github.com/ceph/ceph/pull/7654>`_, Robert LeBlanc)
* osd: a fix for HeartbeatDispatcher and cleanups (`pr#7550 <http://github.com/ceph/ceph/pull/7550>`_, Kefu Chai)
* osd: add missing newline to usage message (`pr#7613 <http://github.com/ceph/ceph/pull/7613>`_, Willem Jan Withagen)
* osd: avoid FORCE updating digest been overwritten by MAYBE when comparing scrub map (`pr#7051 <http://github.com/ceph/ceph/pull/7051>`_, Zhiqiang Wang)
* osd: bluefs: fix alignment for odd page sizes (`pr#7900 <http://github.com/ceph/ceph/pull/7900>`_, Dan Mick)
* osd: bluestore updates, scrub fixes (`pr#8035 <http://github.com/ceph/ceph/pull/8035>`_, Sage Weil)
* osd: bluestore/blockdevice: use std::mutex et al (`pr#7568 <http://github.com/ceph/ceph/pull/7568>`_, Sage Weil)
* osd: bluestore: NVMEDevice: fix error handling (`pr#7799 <http://github.com/ceph/ceph/pull/7799>`_, xie xingguo)
* osd: bluestore: Revert NVMEDevice task cstor and refresh interface changes (`pr#7729 <http://github.com/ceph/ceph/pull/7729>`_, Haomai Wang)
* osd: bluestore: add 'override' to virtual functions (`pr#7886 <http://github.com/ceph/ceph/pull/7886>`_, Michal Jarzabek)
* osd: bluestore: allow _dump_onode dynamic accept log level (`pr#7995 <http://github.com/ceph/ceph/pull/7995>`_, Jianpeng Ma)
* osd: bluestore: fix check for write falling within the same extent (`issue#14954 <http://tracker.ceph.com/issues/14954>`_, `pr#7892 <http://github.com/ceph/ceph/pull/7892>`_, Jianpeng Ma)
* osd: bluestore: for overwrite a extent, allocate new extent on min_alloc_size write (`pr#7996 <http://github.com/ceph/ceph/pull/7996>`_, Jianpeng Ma)
* osd: bluestore: improve fs-type verification and tidy up (`pr#7651 <http://github.com/ceph/ceph/pull/7651>`_, xie xingguo)
* osd: bluestore: misc fixes (`pr#7658 <http://github.com/ceph/ceph/pull/7658>`_, Jianpeng Ma)
* osd: bluestore: remove unneeded includes (`pr#7870 <http://github.com/ceph/ceph/pull/7870>`_, Michal Jarzabek)
* osd: clean up CMPXATTR checks (`pr#5961 <http://github.com/ceph/ceph/pull/5961>`_, Jianpeng Ma)
* osd: consider high/low mode when putting agent to sleep (`issue#14752 <http://tracker.ceph.com/issues/14752>`_, `pr#7631 <http://github.com/ceph/ceph/pull/7631>`_, Sage Weil)
* osd: ensure new osdmaps commit before publishing them to pgs (`issue#15073 <http://tracker.ceph.com/issues/15073>`_, `pr#8096 <http://github.com/ceph/ceph/pull/8096>`_, Sage Weil)
* osd: filejournal: report journal entry count (`pr#7643 <http://github.com/ceph/ceph/pull/7643>`_, tianqing)
* osd: filestore: FALLOC_FL_PUNCH_HOLE must be used with FALLOC_FL_KEEP_SIZE (`pr#7768 <http://github.com/ceph/ceph/pull/7768>`_, xinxin shu)
* osd: filestore: fast abort if statfs encounters ENOENT (`pr#7703 <http://github.com/ceph/ceph/pull/7703>`_, xie xingguo)
* osd: filestore: fix race condition with split vs collection_move_rename and long object names (`issue#14766 <http://tracker.ceph.com/issues/14766>`_, `pr#8136 <http://github.com/ceph/ceph/pull/8136>`_, Samuel Just)
* osd: filestore: fix result code overwritten for clone (`issue#14817 <http://tracker.ceph.com/issues/14817>`_, `issue#14827 <http://tracker.ceph.com/issues/14827>`_, `pr#7711 <http://github.com/ceph/ceph/pull/7711>`_, xie xingguo)
* osd: filestore: fix wrong scope of result code for error cases during mkfs (`issue#14814 <http://tracker.ceph.com/issues/14814>`_, `pr#7704 <http://github.com/ceph/ceph/pull/7704>`_, xie xingguo)
* osd: filestore: fix wrong scope of result code for error cases during mount (`issue#14815 <http://tracker.ceph.com/issues/14815>`_, `pr#7707 <http://github.com/ceph/ceph/pull/7707>`_, xie xingguo)
* osd: filestore: restructure journal and op queue throttling (`pr#7767 <http://github.com/ceph/ceph/pull/7767>`_, Samuel Just)
* osd: fix forced prmootion for CALL ops (`issue#14745 <http://tracker.ceph.com/issues/14745>`_, `pr#7617 <http://github.com/ceph/ceph/pull/7617>`_, Sage Weil)
* osd: fix fusestore hanging during stop/quit (`issue#14786 <http://tracker.ceph.com/issues/14786>`_, `pr#7677 <http://github.com/ceph/ceph/pull/7677>`_, xie xingguo)
* osd: fix inaccurate counter and skip over queueing an empty transaction (`pr#7754 <http://github.com/ceph/ceph/pull/7754>`_, xie xingguo)
* osd: fix lack of object unblock when flush fails (`issue#14511 <http://tracker.ceph.com/issues/14511>`_, `pr#7584 <http://github.com/ceph/ceph/pull/7584>`_, Igor Fedotov)
* osd: fix overload of '==' operator for pg_stat_t (`issue#14921 <http://tracker.ceph.com/issues/14921>`_, `pr#7842 <http://github.com/ceph/ceph/pull/7842>`_, xie xingguo)
* osd: fix race condition for heartbeat_need_update (`issue#14387 <http://tracker.ceph.com/issues/14387>`_, `pr#7739 <http://github.com/ceph/ceph/pull/7739>`_, xie xingguo)
* osd: fix return value from maybe_handle_cache_detail() (`pr#7593 <http://github.com/ceph/ceph/pull/7593>`_, Igor Fedotov)
* osd: fix unnecessary object promotion when deleting from cache pool (`issue#13894 <http://tracker.ceph.com/issues/13894>`_, `pr#7537 <http://github.com/ceph/ceph/pull/7537>`_, Igor Fedotov)
* osd: fix wrong return type of find_osd_on_ip() (`issue#14872 <http://tracker.ceph.com/issues/14872>`_, `pr#7812 <http://github.com/ceph/ceph/pull/7812>`_, xie xingguo)
* osd: ghobject_t: use ! instead of @ as a separator (`pr#7595 <http://github.com/ceph/ceph/pull/7595>`_, Sage Weil)
* osd: handle dup pg_create that races with pg deletion (`pr#8033 <http://github.com/ceph/ceph/pull/8033>`_, Sage Weil)
* osd: initialize last_recalibrate field at construction (`pr#8071 <http://github.com/ceph/ceph/pull/8071>`_, xie xingguo)
* osd: kstore: fix a race condition in _txc_finish() (`pr#7804 <http://github.com/ceph/ceph/pull/7804>`_, Jianjian Huo)
* osd: kstore: latency breakdown (`pr#7850 <http://github.com/ceph/ceph/pull/7850>`_, James Liu)
* osd: kstore: sync up kstore with recent bluestore updates (`pr#7681 <http://github.com/ceph/ceph/pull/7681>`_, Jianjian Huo)
* osd: memstore: fix alignment of Page for test_pageset (`pr#7587 <http://github.com/ceph/ceph/pull/7587>`_, Casey Bodley)
* osd: min_write_recency_for_promote & min_read_recency_for_promote are tiering only (`pr#8081 <http://github.com/ceph/ceph/pull/8081>`_, huanwen ren)
* osd: probabilistic cache tier promotion throttling (`pr#7465 <http://github.com/ceph/ceph/pull/7465>`_, Sage Weil)
* osd: remove up_thru_pending field, which is never used (`pr#7991 <http://github.com/ceph/ceph/pull/7991>`_, xie xingguo)
* osd: replicatedpg: break out loop if we encounter fatal error during do_pg_op() (`issue#14922 <http://tracker.ceph.com/issues/14922>`_, `pr#7844 <http://github.com/ceph/ceph/pull/7844>`_, xie xingguo)
* osd: resolve boot vs NOUP set + clear race (`pr#7483 <http://github.com/ceph/ceph/pull/7483>`_, Sage Weil)
* packaging: make infernalis -> jewel upgrade work (`issue#15047 <http://tracker.ceph.com/issues/15047>`_, `pr#8034 <http://github.com/ceph/ceph/pull/8034>`_, Nathan Cutler)
* packaging: move cephfs repair tools to ceph-common (`issue#15145 <http://tracker.ceph.com/issues/15145>`_, `pr#8133 <http://github.com/ceph/ceph/pull/8133>`_, Boris Ranto, Ken Dreyer)
* pybind/rados: fix object lifetime issues and other bugs in aio (`pr#7778 <http://github.com/ceph/ceph/pull/7778>`_, Hector Martin)
* pybind/rados: use __dealloc__ since __del__ is ignored by cython (`pr#7692 <http://github.com/ceph/ceph/pull/7692>`_, Mehdi Abaakouk)
* pybind: Ensure correct python flags are passed (`pr#7663 <http://github.com/ceph/ceph/pull/7663>`_, James Page)
* pybind: flag an RBD image as closed regardless of result code (`pr#8005 <http://github.com/ceph/ceph/pull/8005>`_, Jason Dillaman)
* pybind: move cephfs to Cython (`pr#7745 <http://github.com/ceph/ceph/pull/7745>`_, John Spray, Mehdi Abaakouk)
* pybind: remove next() on iterators (`pr#7706 <http://github.com/ceph/ceph/pull/7706>`_, Mehdi Abaakouk)
* pybind: replace __del__ with __dealloc__ for rbd (`pr#7708 <http://github.com/ceph/ceph/pull/7708>`_, Josh Durgin)
* pybind: use correct subdir for rados install-exec rule (`pr#7684 <http://github.com/ceph/ceph/pull/7684>`_, Josh Durgin)
* python binding of librados with cython (`pr#7621 <http://github.com/ceph/ceph/pull/7621>`_, Mehdi Abaakouk)
* python: use pip instead of python setup.py (`pr#7605 <http://github.com/ceph/ceph/pull/7605>`_, Loic Dachary)
* qa/workunits/cephtool/test.sh: wait longer in ceph_watch_start() (`issue#14910 <http://tracker.ceph.com/issues/14910>`_, `pr#7861 <http://github.com/ceph/ceph/pull/7861>`_, Kefu Chai)
* qa/workunits/rados/test.sh: capture stderr too (`pr#8004 <http://github.com/ceph/ceph/pull/8004>`_, Sage Weil)
* qa/workunits/rados/test.sh: test tmap_migrate (`pr#8114 <http://github.com/ceph/ceph/pull/8114>`_, Sage Weil)
* qa/workunits/rbd: do not use object map during read flag testing (`pr#8104 <http://github.com/ceph/ceph/pull/8104>`_, Jason Dillaman)
* qa/workunits/rbd: new online maintenance op tests (`pr#8216 <http://github.com/ceph/ceph/pull/8216>`_, Jason Dillaman)
* qa/workunits/rbd: use POSIX function definition (`issue#15104 <http://tracker.ceph.com/issues/15104>`_, `pr#8068 <http://github.com/ceph/ceph/pull/8068>`_, Nathan Cutler)
* qa/workunits/rest/test.py: add confirmation to 'mds setmap' (`issue#14606 <http://tracker.ceph.com/issues/14606>`_, `pr#7982 <http://github.com/ceph/ceph/pull/7982>`_, Sage Weil)
* qa/workunits/rest/test.py: don't use newfs (`pr#8191 <http://github.com/ceph/ceph/pull/8191>`_, Sage Weil)
* qa: add workunit to run ceph_test_rbd_mirror (`pr#8221 <http://github.com/ceph/ceph/pull/8221>`_, Josh Durgin)
* rados: add ceph:: namespace to bufferlist type (`pr#8059 <http://github.com/ceph/ceph/pull/8059>`_, Noah Watkins)
* rados: fix bug for write bench (`pr#7851 <http://github.com/ceph/ceph/pull/7851>`_, James Liu)
* rbd-mirror: ImageReplayer async start/stop (`pr#7944 <http://github.com/ceph/ceph/pull/7944>`_, Mykola Golub)
* rbd-mirror: ImageReplayer improvements (`pr#7759 <http://github.com/ceph/ceph/pull/7759>`_, Mykola Golub)
* rbd-mirror: fix image replay test failures (`pr#8158 <http://github.com/ceph/ceph/pull/8158>`_, Jason Dillaman)
* rbd-mirror: fix long termination due to 30sec wait in main loop (`pr#8185 <http://github.com/ceph/ceph/pull/8185>`_, Mykola Golub)
* rbd-mirror: implement ImageReplayer (`pr#7614 <http://github.com/ceph/ceph/pull/7614>`_, Mykola Golub)
* rbd-mirror: integrate with image sync state machine (`pr#8079 <http://github.com/ceph/ceph/pull/8079>`_, Jason Dillaman)
* rbd-mirror: minor fix-ups for initial skeleton implementation (`pr#7958 <http://github.com/ceph/ceph/pull/7958>`_, Mykola Golub)
* rbd-mirror: remote to local cluster image sync (`pr#7979 <http://github.com/ceph/ceph/pull/7979>`_, Jason Dillaman)
* rbd-mirror: use pool/image names in asok commands (`pr#8159 <http://github.com/ceph/ceph/pull/8159>`_, Mykola Golub)
* rbd-mirror: use the mirroring directory to detect candidate images (`issue#15142 <http://tracker.ceph.com/issues/15142>`_, `pr#8162 <http://github.com/ceph/ceph/pull/8162>`_, Ricardo Dias)
* rbd/run_cli_tests.sh: Reflect test failures (`issue#14825 <http://tracker.ceph.com/issues/14825>`_, `pr#7781 <http://github.com/ceph/ceph/pull/7781>`_, Zack Cerza)
* rbd: add support for mirror image promotion/demotion/resync (`pr#8138 <http://github.com/ceph/ceph/pull/8138>`_, Jason Dillaman)
* rbd: clone operation should default to image format 2 (`pr#8119 <http://github.com/ceph/ceph/pull/8119>`_, Jason Dillaman)
* rbd: deprecate image format 1 (`pr#7841 <http://github.com/ceph/ceph/pull/7841>`_, Jason Dillaman)
* rbd: support for enabling/disabling mirroring on specific images (`issue#13296 <http://tracker.ceph.com/issues/13296>`_, `pr#8056 <http://github.com/ceph/ceph/pull/8056>`_, Ricardo Dias)
* release-notes: draft v0.94.6 release notes (`issue#13356 <http://tracker.ceph.com/issues/13356>`_, `pr#7689 <http://github.com/ceph/ceph/pull/7689>`_, Abhishek Varshney, Loic Dachary)
* release-notes: draft v10.0.3 release notes (`pr#7592 <http://github.com/ceph/ceph/pull/7592>`_, Loic Dachary)
* release-notes: draft v10.0.4 release notes (`pr#7966 <http://github.com/ceph/ceph/pull/7966>`_, Loic Dachary)
* release-notes: draft v9.2.1 release notes (`issue#13750 <http://tracker.ceph.com/issues/13750>`_, `pr#7694 <http://github.com/ceph/ceph/pull/7694>`_, Abhishek Varshney)
* rgw: ldap (Matt Benjamin) (`pr#7985 <http://github.com/ceph/ceph/pull/7985>`_, Matt Benjamin)
* rgw: multisite fixes (`pr#8013 <http://github.com/ceph/ceph/pull/8013>`_, Yehuda Sadeh)
* rgw: support for aws authentication v4 (Javier M. Mellid) (`issue#10333 <http://tracker.ceph.com/issues/10333>`_, `pr#7720 <http://github.com/ceph/ceph/pull/7720>`_, Yehuda Sadeh, Javier M. Mellid)
* rgw sync fixes (`pr#8095 <http://github.com/ceph/ceph/pull/8095>`_, Yehuda Sadeh)
* rgw/rgw_common.h: fix the RGWBucketInfo decoding (`pr#8165 <http://github.com/ceph/ceph/pull/8165>`_, Kefu Chai)
* rgw/rgw_common.h: fix the RGWBucketInfo decoding (`pr#8154 <http://github.com/ceph/ceph/pull/8154>`_, Kefu Chai)
* rgw/rgw_orphan: check the return value of save_state (`pr#7544 <http://github.com/ceph/ceph/pull/7544>`_, Boris Ranto)
* rgw: Allow an implicit tenant in case of Keystone (`pr#8139 <http://github.com/ceph/ceph/pull/8139>`_, Pete Zaitcev)
* rgw: Drop unused usage_exit from rgw_admin.cc (`pr#7632 <http://github.com/ceph/ceph/pull/7632>`_, Pete Zaitcev)
* rgw: RGWZoneParams::create should not handle -EEXIST error (`pr#7927 <http://github.com/ceph/ceph/pull/7927>`_, Orit Wasserman)
* rgw: add bucket request payment feature usage statistics integration (`issue#13834 <http://tracker.ceph.com/issues/13834>`_, `pr#6656 <http://github.com/ceph/ceph/pull/6656>`_, Javier M. Mellid)
* rgw: add support for caching of Keystone admin token. (`pr#7630 <http://github.com/ceph/ceph/pull/7630>`_, Radoslaw Zarzynski)
* rgw: add support for metadata upload during PUT on Swift container. (`pr#8002 <http://github.com/ceph/ceph/pull/8002>`_, Radoslaw Zarzynski)
* rgw: add support for system requests over Swift API (`pr#7666 <http://github.com/ceph/ceph/pull/7666>`_, Radoslaw Zarzynski)
* rgw: adjust the request_uri to support absoluteURI of http request (`issue#12917 <http://tracker.ceph.com/issues/12917>`_, `pr#7675 <http://github.com/ceph/ceph/pull/7675>`_, Wenjun Huang)
* rgw: admin api for retrieving usage info (Ji Chen) (`pr#8031 <http://github.com/ceph/ceph/pull/8031>`_, Yehuda Sadeh, Ji Chen)
* rgw: allow authentication keystone with self signed certs  (`issue#14853 <http://tracker.ceph.com/issues/14853>`_, `issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7777 <http://github.com/ceph/ceph/pull/7777>`_, Abhishek Lekshmanan)
* rgw: approximate AmazonS3 HostId error field. (`pr#7444 <http://github.com/ceph/ceph/pull/7444>`_, Robin H. Johnson)
* rgw: calculate payload hash in RGWPutObj_ObjStore only when necessary. (`pr#7869 <http://github.com/ceph/ceph/pull/7869>`_, Radoslaw Zarzynski)
* rgw: cleanups to comments and messages (`pr#7633 <http://github.com/ceph/ceph/pull/7633>`_, Pete Zaitcev)
* rgw: don't use s->bucket for metadata api path entry (`issue#14549 <http://tracker.ceph.com/issues/14549>`_, `pr#7408 <http://github.com/ceph/ceph/pull/7408>`_, Yehuda Sadeh)
* rgw: drop permissions of rgw/civetweb after startup (`issue#13600 <http://tracker.ceph.com/issues/13600>`_, `pr#8019 <http://github.com/ceph/ceph/pull/8019>`_, Karol Mroz)
* rgw: fcgi should include acconfig (`pr#7760 <http://github.com/ceph/ceph/pull/7760>`_, Abhishek Lekshmanan)
* rgw: fix wrong handling of limit=0 during listing of Swift account. (`issue#14903 <http://tracker.ceph.com/issues/14903>`_, `pr#7821 <http://github.com/ceph/ceph/pull/7821>`_, Radoslaw Zarzynski)
* rgw: fixes for per-period metadata logs (`pr#7827 <http://github.com/ceph/ceph/pull/7827>`_, Casey Bodley)
* rgw: improve error handling in S3/Keystone integration (`pr#7597 <http://github.com/ceph/ceph/pull/7597>`_, Radoslaw Zarzynski)
* rgw: link civetweb with openssl (Sage, Marcus Watts) (`pr#7825 <http://github.com/ceph/ceph/pull/7825>`_, Marcus Watts, Sage Weil)
* rgw: link payer info to usage logging (`pr#7918 <http://github.com/ceph/ceph/pull/7918>`_, Yehuda Sadeh, Javier M. Mellid)
* rgw: move signal.h dependency from rgw_front.h (`pr#7678 <http://github.com/ceph/ceph/pull/7678>`_, Matt Benjamin)
* rgw: multiple Swift API compliance improvements for TempURL (Radoslaw Zarzynsk) (`issue#14806 <http://tracker.ceph.com/issues/14806>`_, `issue#11163 <http://tracker.ceph.com/issues/11163>`_, `pr#7891 <http://github.com/ceph/ceph/pull/7891>`_, Radoslaw Zarzynski)
* rgw: multiple improvements regarding etag calculation for SLO/DLO of Swift API. (`pr#7764 <http://github.com/ceph/ceph/pull/7764>`_, Radoslaw Zarzynski)
* rgw: remove duplicated code in RGWRados::get_bucket_info() (`pr#7413 <http://github.com/ceph/ceph/pull/7413>`_, liyankun)
* rgw: remove unused vector (`pr#7990 <http://github.com/ceph/ceph/pull/7990>`_, Na Xie)
* rgw: reset return code in when iterating over the bucket the objects (`issue#14826 <http://tracker.ceph.com/issues/14826>`_, `pr#7803 <http://github.com/ceph/ceph/pull/7803>`_, Orit Wasserman)
* rgw: store system object meta in cache when creating it (`issue#14678 <http://tracker.ceph.com/issues/14678>`_, `pr#7615 <http://github.com/ceph/ceph/pull/7615>`_, Yehuda Sadeh)
* rgw: support json format for admin policy API (Dunrong Huang) (`issue#14090 <http://tracker.ceph.com/issues/14090>`_, `pr#8036 <http://github.com/ceph/ceph/pull/8036>`_, Yehuda Sadeh, Dunrong Huang)
* rgw: try to parse Keystone token in order appropriate to configuration. (`pr#7822 <http://github.com/ceph/ceph/pull/7822>`_, Radoslaw Zarzynski)
* rgw: use pimpl pattern for RGWPeriodHistory (`pr#7809 <http://github.com/ceph/ceph/pull/7809>`_, Casey Bodley)
* rgw: user quota may not adjust on bucket removal (`issue#14507 <http://tracker.ceph.com/issues/14507>`_, `pr#7586 <http://github.com/ceph/ceph/pull/7586>`_, root)
* rgw:bucket link now set the bucket.instance acl (bug fix) (`issue#11076 <http://tracker.ceph.com/issues/11076>`_, `pr#8037 <http://github.com/ceph/ceph/pull/8037>`_, Zengran Zhang)
* rpm,deb: remove conditional BuildRequires for btrfs-progs (`issue#15042 <http://tracker.ceph.com/issues/15042>`_, `pr#8016 <http://github.com/ceph/ceph/pull/8016>`_, Erwan Velu)
* rpm: remove sub-package dependencies on "ceph" (`issue#15146 <http://tracker.ceph.com/issues/15146>`_, `pr#8137 <http://github.com/ceph/ceph/pull/8137>`_, Ken Dreyer)
* script: add missing stop_rgw variable to stop.sh script (`pr#7959 <http://github.com/ceph/ceph/pull/7959>`_, Karol Mroz)
* selinux: Update policy to grant additional access (`issue#14870 <http://tracker.ceph.com/issues/14870>`_, `pr#7971 <http://github.com/ceph/ceph/pull/7971>`_, Boris Ranto)
* selinux: allow log files to be located in /var/log/radosgw (`pr#7604 <http://github.com/ceph/ceph/pull/7604>`_, Boris Ranto)
* sstring.hh: return type from str_len(...) need not be const (`pr#7679 <http://github.com/ceph/ceph/pull/7679>`_, Matt Benjamin)
* submodules: revert an accidental change (`pr#7929 <http://github.com/ceph/ceph/pull/7929>`_, Yehuda Sadeh)
* systemd: correctly escape block device paths (`issue#14706 <http://tracker.ceph.com/issues/14706>`_, `pr#7579 <http://github.com/ceph/ceph/pull/7579>`_, James Page)
* test/TestPGLog: fix the FTBFS (`issue#14930 <http://tracker.ceph.com/issues/14930>`_, `pr#7855 <http://github.com/ceph/ceph/pull/7855>`_, Kefu Chai)
* test/bufferlist: Avoid false-positive tests (`pr#7955 <http://github.com/ceph/ceph/pull/7955>`_, Erwan Velu)
* test/cli-integration/rbd: disable progress output (`issue#14931 <http://tracker.ceph.com/issues/14931>`_, `pr#7858 <http://github.com/ceph/ceph/pull/7858>`_, Josh Durgin)
* test/osd: Relax the timing intervals in osd-markdown.sh (`pr#7899 <http://github.com/ceph/ceph/pull/7899>`_, Dan Mick)
* test/pybind/test_ceph_argparse: fix reweight-by-utilization tests (`pr#8027 <http://github.com/ceph/ceph/pull/8027>`_, Kefu Chai, Sage Weil)
* test/radosgw-admin: update the expected usage outputs (`pr#7723 <http://github.com/ceph/ceph/pull/7723>`_, Kefu Chai)
* test/rgw: add multisite test for meta sync across periods (`pr#7887 <http://github.com/ceph/ceph/pull/7887>`_, Casey Bodley)
* test/time: no need to abs(uint64_t) for comparing (`pr#7726 <http://github.com/ceph/ceph/pull/7726>`_, Kefu Chai)
* test: add missing shut_down mock method (`pr#8125 <http://github.com/ceph/ceph/pull/8125>`_, Jason Dillaman)
* test: correct librbd errors discovered with unoptimized cmake build (`pr#7914 <http://github.com/ceph/ceph/pull/7914>`_, Jason Dillaman)
* test: create pools for rbd tests with different prefix (`pr#7738 <http://github.com/ceph/ceph/pull/7738>`_, Mykola Golub)
* test: enable test for bug #2339 which has been resolved. (`pr#7743 <http://github.com/ceph/ceph/pull/7743>`_, You Ji)
* test: fix issues discovered via the rbd permissions test case (`pr#8129 <http://github.com/ceph/ceph/pull/8129>`_, Jason Dillaman)
* test: fixup and improvements for rbd-mirror test (`pr#8090 <http://github.com/ceph/ceph/pull/8090>`_, Mykola Golub)
* test: handle exception thrown from close during rbd lock test (`pr#8124 <http://github.com/ceph/ceph/pull/8124>`_, Jason Dillaman)
* test: more debug logging for TestWatchNotify (`pr#7737 <http://github.com/ceph/ceph/pull/7737>`_, Mykola Golub)
* test: new librbd flatten test case (`pr#7609 <http://github.com/ceph/ceph/pull/7609>`_, Jason Dillaman)
* test: rbd-mirror: add "switch to the next tag" test (`pr#8149 <http://github.com/ceph/ceph/pull/8149>`_, Mykola Golub)
* test: rbd-mirror: compare positions using all fields (`pr#8172 <http://github.com/ceph/ceph/pull/8172>`_, Mykola Golub)
* test: reproducer for writeback CoW deadlock (`pr#8009 <http://github.com/ceph/ceph/pull/8009>`_, Jason Dillaman)
* test: update rbd integration cram tests for new default features (`pr#8001 <http://github.com/ceph/ceph/pull/8001>`_, Jason Dillaman)
* test_pool_create.sh: put test files in the test dir so they are cleaned up (`pr#8219 <http://github.com/ceph/ceph/pull/8219>`_, Josh Durgin)
* tests: ceph-disk.sh: should use "readlink -f" instead (`pr#7594 <http://github.com/ceph/ceph/pull/7594>`_, Kefu Chai)
* tests: ceph-disk.sh: use "readlink -f" instead for fullpath (`pr#7606 <http://github.com/ceph/ceph/pull/7606>`_, Kefu Chai)
* tests: fix a few build warnings (`pr#7608 <http://github.com/ceph/ceph/pull/7608>`_, Sage Weil)
* tests: sync ceph-erasure-code-corpus for mktemp -d (`pr#7596 <http://github.com/ceph/ceph/pull/7596>`_, Loic Dachary)
* tests: test_pidfile.sh lingering processes (`issue#14834 <http://tracker.ceph.com/issues/14834>`_, `pr#7734 <http://github.com/ceph/ceph/pull/7734>`_, Loic Dachary)
* tools/cephfs: add tmap_upgrade (`pr#7003 <http://github.com/ceph/ceph/pull/7003>`_, John Spray)
* tools/cephfs: fix tmap_upgrade (`issue#15135 <http://tracker.ceph.com/issues/15135>`_, `pr#8128 <http://github.com/ceph/ceph/pull/8128>`_, John Spray)
* tools/rados: reduce "rados put" memory usage by op_size (`pr#7928 <http://github.com/ceph/ceph/pull/7928>`_, Piotr Dałek)
* unittest_compression_zlib: do not assume buffer will be null terminated (`pr#8064 <http://github.com/ceph/ceph/pull/8064>`_, Sage Weil)
* unittest_osdmap: default crush tunables now firefly (`pr#8098 <http://github.com/ceph/ceph/pull/8098>`_, Sage Weil)
* vstart.sh: avoid race condition starting rgw via vstart.sh (`issue#14829 <http://tracker.ceph.com/issues/14829>`_, `pr#7727 <http://github.com/ceph/ceph/pull/7727>`_, Javier M. Mellid)
* vstart.sh: silence a harmless msg where btrfs is not found (`pr#7640 <http://github.com/ceph/ceph/pull/7640>`_, Patrick Donnelly)
* xio: fix incorrect ip being assigned in case of multiple RDMA ports (`pr#7747 <http://github.com/ceph/ceph/pull/7747>`_, Subramanyam Varanasi)
* xio: remove duplicate assignment of peer addr (`pr#8025 <http://github.com/ceph/ceph/pull/8025>`_, Roi Dayan)
* xio: remove redundant magic methods (`pr#7773 <http://github.com/ceph/ceph/pull/7773>`_, Roi Dayan)
* xio: remove unused variable (`pr#8023 <http://github.com/ceph/ceph/pull/8023>`_, Roi Dayan)
* xxhash: use clone of xxhash.git; add .gitignore (`pr#7986 <http://github.com/ceph/ceph/pull/7986>`_, Sage Weil)
* rbd: update default image features (`pr#7846 <http://github.com/ceph/ceph/pull/7846>`_, Jason Dillaman)
* rbd-mirror: make remote context respect env and argv config params (`pr#8182 <http://github.com/ceph/ceph/pull/8182>`_, Mykola Golub)
* journal: re-use common threads between journalers (`pr#7906 <http://github.com/ceph/ceph/pull/7906>`_, Jason Dillaman)
* client: add option to control how directory size is calculated (`pr#7323 <http://github.com/ceph/ceph/pull/7323>`_, Yan, Zheng)
* rgw: keystone v3 (`pr#7719 <http://github.com/ceph/ceph/pull/7719>`_, Mark Barnes, Radoslaw Zarzynski)
* rgw: new multisite merge (`issue#14549 <http://tracker.ceph.com/issues/14549>`_, `pr#7709 <http://github.com/ceph/ceph/pull/7709>`_, Yehuda Sadeh, Orit Wasserman, Casey Bodley, Daniel Gryniewicz)
* rgw: adjust error code when bucket does not exist in copy operation (`issue#14975 <http://tracker.ceph.com/issues/14975>`_, `pr#7916 <http://github.com/ceph/ceph/pull/7916>`_, Yehuda Sadeh)
* rgw: indexless (`pr#7786 <http://github.com/ceph/ceph/pull/7786>`_, Yehuda Sadeh)


Notable Changes since Infernalis
--------------------------------

changes up to 10.0.4...

* build: build internal plugins and classes as modules (`pr#6462 <http://github.com/ceph/ceph/pull/6462>`_, James Page)
* build: fix Jenkins make check errors due to deep-scrub randomization (`pr#6671 <http://github.com/ceph/ceph/pull/6671>`_, David Zafman)
* build/ops: enable CR in CentOS 7 (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6844 <http://github.com/ceph/ceph/pull/6844>`_, Loic Dachary)
* build/ops: rbd-replay moved from ceph-test-dbg to ceph-common-dbg (`issue#13785 <http://tracker.ceph.com/issues/13785>`_, `pr#6578 <http://github.com/ceph/ceph/pull/6578>`_, Loic Dachary)
* ceph-disk: Add destroy and deactivate option (`issue#7454 <http://tracker.ceph.com/issues/7454>`_, `pr#5867 <http://github.com/ceph/ceph/pull/5867>`_, Vicente Cheng)
* ceph-disk: compare parted output with the dereferenced path (`issue#13438 <http://tracker.ceph.com/issues/13438>`_, `pr#6219 <http://github.com/ceph/ceph/pull/6219>`_, Joe Julian)
* ceph-objectstore-tool: fix --dry-run for many ceph-objectstore-tool operations (`pr#6545 <http://github.com/ceph/ceph/pull/6545>`_, David Zafman)
* ceph.spec.in: limit _smp_mflags when lowmem_builder is set in SUSE's OBS (`issue#13858 <http://tracker.ceph.com/issues/13858>`_, `pr#6691 <http://github.com/ceph/ceph/pull/6691>`_, Nathan Cutler)
* ceph_test_msgr: Use send_message instead of keepalive to wakeup connection (`pr#6605 <http://github.com/ceph/ceph/pull/6605>`_, Haomai Wang)
* client: avoid creating orphan object in Client::check_pool_perm() (`issue#13782 <http://tracker.ceph.com/issues/13782>`_, `pr#6603 <http://github.com/ceph/ceph/pull/6603>`_, Yan, Zheng)
* client: use null snapc to check pool permission (`issue#13714 <http://tracker.ceph.com/issues/13714>`_, `pr#6497 <http://github.com/ceph/ceph/pull/6497>`_, Yan, Zheng)
* cmake: add nss as a suffix for pk11pub.h (`pr#6556 <http://github.com/ceph/ceph/pull/6556>`_, Samuel Just)
* cmake: fix files list (`pr#6539 <http://github.com/ceph/ceph/pull/6539>`_, Yehuda Sadeh)
* cmake: librbd and libjournal build fixes (`pr#6557 <http://github.com/ceph/ceph/pull/6557>`_, Ilya Dryomov)
* coc: fix typo in the apt-get command (`pr#6659 <http://github.com/ceph/ceph/pull/6659>`_, Chris Holcombe)
* common: allow enable/disable of optracker at runtime (`pr#5168 <http://github.com/ceph/ceph/pull/5168>`_, Jianpeng Ma)
* common: fix reset max in Throttle using perf reset command (`issue#13517 <http://tracker.ceph.com/issues/13517>`_, `pr#6300 <http://github.com/ceph/ceph/pull/6300>`_, Xinze Chi)
* doc: add v0.80.11 to the release timeline (`pr#6658 <http://github.com/ceph/ceph/pull/6658>`_, Loic Dachary)
* doc: release-notes: draft v0.80.11 release notes (`pr#6374 <http://github.com/ceph/ceph/pull/6374>`_, Loic Dachary)
* doc: release-notes: draft v10.0.0 release notes (`pr#6666 <http://github.com/ceph/ceph/pull/6666>`_, Loic Dachary)
* doc: SubmittingPatches: there is no next; only jewel (`pr#6811 <http://github.com/ceph/ceph/pull/6811>`_, Nathan Cutler)
* doc: Update ceph-disk manual page with new feature deactivate/destroy. (`pr#6637 <http://github.com/ceph/ceph/pull/6637>`_, Vicente Cheng)
* doc: update infernalis release notes (`pr#6575 <http://github.com/ceph/ceph/pull/6575>`_, vasukulkarni)
* fix: use right init_flags to finish CephContext (`pr#6549 <http://github.com/ceph/ceph/pull/6549>`_, Yunchuan Wen)
* init-ceph: use getopt to make option processing more flexible (`issue#3015 <http://tracker.ceph.com/issues/3015>`_, `pr#6089 <http://github.com/ceph/ceph/pull/6089>`_, Nathan Cutler)
* journal: incremental improvements and fixes (`pr#6552 <http://github.com/ceph/ceph/pull/6552>`_, Mykola Golub)
* krbd: remove deprecated --quiet param from udevadm (`issue#13560 <http://tracker.ceph.com/issues/13560>`_, `pr#6394 <http://github.com/ceph/ceph/pull/6394>`_, Jason Dillaman)
* kv: fix bug in kv key optimization (`pr#6511 <http://github.com/ceph/ceph/pull/6511>`_, Sage Weil)
* kv/KineticStore: fix broken split_key (`pr#6574 <http://github.com/ceph/ceph/pull/6574>`_, Haomai Wang)
* kv: optimize and clean up internal key/value interface (`pr#6312 <http://github.com/ceph/ceph/pull/6312>`_, Piotr Dałek, Sage Weil)
* librados: do cleanup (`pr#6488 <http://github.com/ceph/ceph/pull/6488>`_, xie xingguo)
* librados: fix pool alignment API overflow issue (`issue#13715 <http://tracker.ceph.com/issues/13715>`_, `pr#6489 <http://github.com/ceph/ceph/pull/6489>`_, xie xingguo)
* librados: fix potential null pointer access when do pool_snap_list (`issue#13639 <http://tracker.ceph.com/issues/13639>`_, `pr#6422 <http://github.com/ceph/ceph/pull/6422>`_, xie xingguo)
* librados: fix PromoteOn2ndRead test for EC (`pr#6373 <http://github.com/ceph/ceph/pull/6373>`_, Sage Weil)
* librados: fix rare race where pool op callback may hang forever (`issue#13642 <http://tracker.ceph.com/issues/13642>`_, `pr#6426 <http://github.com/ceph/ceph/pull/6426>`_, xie xingguo)
* librados: Solaris port (`pr#6416 <http://github.com/ceph/ceph/pull/6416>`_, Rohan Mars)
* librbd: flush and invalidate cache via admin socket (`issue#2468 <http://tracker.ceph.com/issues/2468>`_, `pr#6453 <http://github.com/ceph/ceph/pull/6453>`_, Mykola Golub)
* librbd: integrate journaling support for IO operations (`pr#6541 <http://github.com/ceph/ceph/pull/6541>`_, Jason Dillaman)
* librbd: perf counters might not be initialized on error (`issue#13740 <http://tracker.ceph.com/issues/13740>`_, `pr#6523 <http://github.com/ceph/ceph/pull/6523>`_, Jason Dillaman)
* librbd: perf section name: use hyphen to separate components (`issue#13719 <http://tracker.ceph.com/issues/13719>`_, `pr#6516 <http://github.com/ceph/ceph/pull/6516>`_, Mykola Golub)
* librbd: resize should only update image size within header (`issue#13674 <http://tracker.ceph.com/issues/13674>`_, `pr#6447 <http://github.com/ceph/ceph/pull/6447>`_, Jason Dillaman)
* librbd: start perf counters after id is initialized (`issue#13720 <http://tracker.ceph.com/issues/13720>`_, `pr#6494 <http://github.com/ceph/ceph/pull/6494>`_, Mykola Golub)
* mailmap: revise organization (`pr#6519 <http://github.com/ceph/ceph/pull/6519>`_, Li Wang)
* mailmap: Ubuntu Kylin name changed to Kylin Cloud (`pr#6532 <http://github.com/ceph/ceph/pull/6532>`_, Loic Dachary)
* mailmap: update .organizationmap (`pr#6565 <http://github.com/ceph/ceph/pull/6565>`_, chenji-kael)
* mailmap: updates for infernalis. (`pr#6495 <http://github.com/ceph/ceph/pull/6495>`_, Yann Dupont)
* mailmap: updates (`pr#6594 <http://github.com/ceph/ceph/pull/6594>`_, chenji-kael)
* mds: fix scrub_path (`pr#6684 <http://github.com/ceph/ceph/pull/6684>`_, John Spray)
* mds: properly set STATE_STRAY/STATE_ORPHAN for stray dentry/inode (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#6553 <http://github.com/ceph/ceph/pull/6553>`_, Yan, Zheng)
* mds: ScrubStack and "tag path" command (`pr#5662 <http://github.com/ceph/ceph/pull/5662>`_, Yan, Zheng, John Spray, Greg Farnum)
* mon: block 'ceph osd pg-temp ...' if pg_temp update is already pending (`pr#6704 <http://github.com/ceph/ceph/pull/6704>`_, Sage Weil)
* mon: don't require OSD W for MRemoveSnaps (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#6601 <http://github.com/ceph/ceph/pull/6601>`_, John Spray)
* mon: initialize recorded election epoch properly even when standalone (`issue#13627 <http://tracker.ceph.com/issues/13627>`_, `pr#6407 <http://github.com/ceph/ceph/pull/6407>`_, huanwen ren)
* mon: revert MonitorDBStore's WholeStoreIteratorImpl::get (`issue#13742 <http://tracker.ceph.com/issues/13742>`_, `pr#6522 <http://github.com/ceph/ceph/pull/6522>`_, Piotr Dałek)
* msg/async: let receiver ack message ASAP (`pr#6478 <http://github.com/ceph/ceph/pull/6478>`_, Haomai Wang)
* msg/async: support of non-block connect in async messenger (`issue#12802 <http://tracker.ceph.com/issues/12802>`_, `pr#5848 <http://github.com/ceph/ceph/pull/5848>`_, Jianhui Yuan)
* msg/async: will crash if enabling async msg because of an assertion (`pr#6640 <http://github.com/ceph/ceph/pull/6640>`_, Zhi Zhang)
* osd: avoid calculating crush mapping for most ops (`pr#6371 <http://github.com/ceph/ceph/pull/6371>`_, Sage Weil)
* osd: avoid double-check for replaying and can_checkpoint() in FileStore::_check_replay_guard (`pr#6471 <http://github.com/ceph/ceph/pull/6471>`_, Ning Yao)
* osd: call on_new_interval on newly split child PG (`issue#13962 <http://tracker.ceph.com/issues/13962>`_, `pr#6778 <http://github.com/ceph/ceph/pull/6778>`_, Sage Weil)
* osd: change mutex to spinlock to optimize thread context switch. (`pr#6492 <http://github.com/ceph/ceph/pull/6492>`_, Xiaowei Chen)
* osd: check do_shutdown before do_restart (`pr#6547 <http://github.com/ceph/ceph/pull/6547>`_, Xiaoxi Chen)
* osd: clarify the scrub result report (`pr#6534 <http://github.com/ceph/ceph/pull/6534>`_, Li Wang)
* osd: don't do random deep scrubs for user initiated scrubs (`pr#6673 <http://github.com/ceph/ceph/pull/6673>`_, David Zafman)
* osd: FileStore: support multiple ondisk finish and apply finishers (`pr#6486 <http://github.com/ceph/ceph/pull/6486>`_, Xinze Chi, Haomai Wang)
* osd: fix broken balance / localized read handling (`issue#13491 <http://tracker.ceph.com/issues/13491>`_, `pr#6364 <http://github.com/ceph/ceph/pull/6364>`_, Jason Dillaman)
* osd: fix bug in last_* PG state timestamps (`pr#6517 <http://github.com/ceph/ceph/pull/6517>`_, Li Wang)
* osd: fix ClassHandler::ClassData::get_filter() (`pr#6747 <http://github.com/ceph/ceph/pull/6747>`_, Yan, Zheng)
* osd: fixes for several cases where op result code was not checked or set (`issue#13566 <http://tracker.ceph.com/issues/13566>`_, `pr#6347 <http://github.com/ceph/ceph/pull/6347>`_, xie xingguo)
* osd: fix reactivate (check OSDSuperblock in mkfs() when we already have the superblock) (`issue#13586 <http://tracker.ceph.com/issues/13586>`_, `pr#6385 <http://github.com/ceph/ceph/pull/6385>`_, Vicente Cheng)
* osd: fix wrong use of right parenthesis in localized read logic (`pr#6566 <http://github.com/ceph/ceph/pull/6566>`_, Jie Wang)
* osd: improve temperature calculation for cache tier agent (`pr#4737 <http://github.com/ceph/ceph/pull/4737>`_, MingXin Liu)
* osd: merge local_t and op_t txn to single one (`pr#6439 <http://github.com/ceph/ceph/pull/6439>`_, Xinze Chi)
* osd: newstore: misc updates (including kv and os/fs stuff) (`pr#6609 <http://github.com/ceph/ceph/pull/6609>`_, Sage Weil)
* osd: note down the number of missing clones (`pr#6654 <http://github.com/ceph/ceph/pull/6654>`_, Kefu Chai)
* osd: optimize clone write path if object-map is enabled (`pr#6403 <http://github.com/ceph/ceph/pull/6403>`_, xinxin shu)
* osd: optimize scrub subset_last_update calculation (`pr#6518 <http://github.com/ceph/ceph/pull/6518>`_, Li Wang)
* osd: partial revert of "ReplicatedPG: result code not correctly set in some cases." (`issue#13796 <http://tracker.ceph.com/issues/13796>`_, `pr#6622 <http://github.com/ceph/ceph/pull/6622>`_, Sage Weil)
* osd: randomize deep scrubbing (`pr#6550 <http://github.com/ceph/ceph/pull/6550>`_, Dan van der Ster, Herve Rousseau)
* osd: scrub: do not assign value if read error (`pr#6568 <http://github.com/ceph/ceph/pull/6568>`_, Li Wang)
* osd: write file journal optimization (`pr#6484 <http://github.com/ceph/ceph/pull/6484>`_, Xinze Chi)
* rbd: accept --user, refuse -i command-line optionals (`pr#6590 <http://github.com/ceph/ceph/pull/6590>`_, Ilya Dryomov)
* rbd: add missing command aliases to refactored CLI (`issue#13806 <http://tracker.ceph.com/issues/13806>`_, `pr#6606 <http://github.com/ceph/ceph/pull/6606>`_, Jason Dillaman)
* rbd: dynamically generated bash completion (`issue#13494 <http://tracker.ceph.com/issues/13494>`_, `pr#6316 <http://github.com/ceph/ceph/pull/6316>`_, Jason Dillaman)
* rbd: fixes for refactored CLI and related tests (`pr#6738 <http://github.com/ceph/ceph/pull/6738>`_, Ilya Dryomov)
* rbd: make config changes actually apply (`pr#6520 <http://github.com/ceph/ceph/pull/6520>`_, Mykola Golub)
* rbd: refactor cli command handling (`pr#5987 <http://github.com/ceph/ceph/pull/5987>`_, Jason Dillaman)
* rbd: stripe unit/count set incorrectly from config (`pr#6593 <http://github.com/ceph/ceph/pull/6593>`_, Mykola Golub)
* rbd: support negative boolean command-line optionals (`issue#13784 <http://tracker.ceph.com/issues/13784>`_, `pr#6607 <http://github.com/ceph/ceph/pull/6607>`_, Jason Dillaman)
* rbd: unbreak rbd map + cephx_sign_messages option (`pr#6583 <http://github.com/ceph/ceph/pull/6583>`_, Ilya Dryomov)
* rgw: bucket request payment support (`issue#13427 <http://tracker.ceph.com/issues/13427>`_, `pr#6214 <http://github.com/ceph/ceph/pull/6214>`_, Javier M. Mellid)
* rgw: extend rgw_extended_http_attrs to affect Swift accounts and containers as well (`pr#5969 <http://github.com/ceph/ceph/pull/5969>`_, Radoslaw Zarzynski)
* rgw: fix openssl linkage (`pr#6513 <http://github.com/ceph/ceph/pull/6513>`_, Yehuda Sadeh)
* rgw: fix partial read issue in rgw_admin and rgw_tools (`pr#6761 <http://github.com/ceph/ceph/pull/6761>`_, Jiaying Ren)
* rgw: fix reload on non Debian systems. (`pr#6482 <http://github.com/ceph/ceph/pull/6482>`_, Hervé Rousseau)
* rgw: fix response of delete expired objects (`issue#13469 <http://tracker.ceph.com/issues/13469>`_, `pr#6228 <http://github.com/ceph/ceph/pull/6228>`_, Yuan Zhou)
* rgw: fix swift API returning incorrect account metadata (`issue#13140 <http://tracker.ceph.com/issues/13140>`_, `pr#6047 <http://github.com/ceph/ceph/pull/6047>`_, Sangdi Xu)
* rgw: link against system openssl (instead of dlopen at runtime) (`pr#6419 <http://github.com/ceph/ceph/pull/6419>`_, Sage Weil)
* rgw: prevent anonymous user from reading bucket with authenticated read ACL (`issue#13207 <http://tracker.ceph.com/issues/13207>`_, `pr#6057 <http://github.com/ceph/ceph/pull/6057>`_, root)
* rgw: use smart pointer for C_Reinitwatch (`pr#6767 <http://github.com/ceph/ceph/pull/6767>`_, Orit Wasserman)
* systemd: fix typos (`pr#6679 <http://github.com/ceph/ceph/pull/6679>`_, Tobias Suckow)
* tests: centos7 needs the Continuous Release (CR) Repository enabled for (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6842 <http://github.com/ceph/ceph/pull/6842>`_, Brad Hubbard)
* tests: concatenate test_rados_test_tool from src and qa (`issue#13691 <http://tracker.ceph.com/issues/13691>`_, `pr#6464 <http://github.com/ceph/ceph/pull/6464>`_, Loic Dachary)
* tests: fix test_rados_tools.sh rados lookup (`issue#13691 <http://tracker.ceph.com/issues/13691>`_, `pr#6502 <http://github.com/ceph/ceph/pull/6502>`_, Loic Dachary)
* tests: fix typo in TestClsRbd.snapshots test case (`issue#13727 <http://tracker.ceph.com/issues/13727>`_, `pr#6504 <http://github.com/ceph/ceph/pull/6504>`_, Jason Dillaman)
* tests: ignore test-suite.log (`pr#6584 <http://github.com/ceph/ceph/pull/6584>`_, Loic Dachary)
* tests: restore run-cli-tests (`pr#6571 <http://github.com/ceph/ceph/pull/6571>`_, Loic Dachary, Sage Weil, Jason Dillaman)
* tools/cephfs: fix overflow writing header to fixed size buffer (#13816) (`pr#6617 <http://github.com/ceph/ceph/pull/6617>`_, John Spray)
* build: cmake tweaks (`pr#6254 <http://github.com/ceph/ceph/pull/6254>`_, John Spray)
* build: more CMake package check fixes (`pr#6108 <http://github.com/ceph/ceph/pull/6108>`_, Daniel Gryniewicz)
* auth: fail if rotating key is missing (do not spam log) (`pr#6473 <http://github.com/ceph/ceph/pull/6473>`_, Qiankun Zheng)
* auth: fix crash when bad keyring is passed (`pr#6698 <http://github.com/ceph/ceph/pull/6698>`_, Dunrong Huang)
* auth: make keyring without mon entity type return -EACCES (`pr#5734 <http://github.com/ceph/ceph/pull/5734>`_, Xiaowei Chen)
* buffer: make usable outside of ceph source again (`pr#6863 <http://github.com/ceph/ceph/pull/6863>`_, Josh Durgin)
* build: cmake check fixes (`pr#6787 <http://github.com/ceph/ceph/pull/6787>`_, Orit Wasserman)
* build: fix bz2-dev dependency (`pr#6948 <http://github.com/ceph/ceph/pull/6948>`_, Samuel Just)
* build: Gentoo: _FORTIFY_SOURCE fix. (`issue#13920 <http://tracker.ceph.com/issues/13920>`_, `pr#6739 <http://github.com/ceph/ceph/pull/6739>`_, Robin H. Johnson)
* build/ops: systemd ceph-disk unit must not assume /bin/flock (`issue#13975 <http://tracker.ceph.com/issues/13975>`_, `pr#6803 <http://github.com/ceph/ceph/pull/6803>`_, Loic Dachary)
* ceph-detect-init: Ubuntu >= 15.04 uses systemd (`pr#6873 <http://github.com/ceph/ceph/pull/6873>`_, James Page)
* cephfs-data-scan: scan_frags (`pr#5941 <http://github.com/ceph/ceph/pull/5941>`_, John Spray)
* cephfs-data-scan: scrub tag filtering (#12133 and #12145) (`issue#12133 <http://tracker.ceph.com/issues/12133>`_, `issue#12145 <http://tracker.ceph.com/issues/12145>`_, `pr#5685 <http://github.com/ceph/ceph/pull/5685>`_, John Spray)
* ceph-fuse: add process to ceph-fuse --help (`pr#6821 <http://github.com/ceph/ceph/pull/6821>`_, Wei Feng)
* ceph-kvstore-tool: handle bad out file on command line (`pr#6093 <http://github.com/ceph/ceph/pull/6093>`_, Kefu Chai)
* ceph-mds:add --help/-h (`pr#6850 <http://github.com/ceph/ceph/pull/6850>`_, Cilang Zhao)
* ceph_objectstore_bench: fix race condition, bugs (`issue#13516 <http://tracker.ceph.com/issues/13516>`_, `pr#6681 <http://github.com/ceph/ceph/pull/6681>`_, Igor Fedotov)
* ceph.spec.in: add BuildRequires: systemd (`issue#13860 <http://tracker.ceph.com/issues/13860>`_, `pr#6692 <http://github.com/ceph/ceph/pull/6692>`_, Nathan Cutler)
* client: a better check for MDS availability (`pr#6253 <http://github.com/ceph/ceph/pull/6253>`_, John Spray)
* client: close mds sessions in shutdown() (`pr#6269 <http://github.com/ceph/ceph/pull/6269>`_, John Spray)
* client: don't invalidate page cache when inode is no longer used (`pr#6380 <http://github.com/ceph/ceph/pull/6380>`_, Yan, Zheng)
* client: modify a word in log (`pr#6906 <http://github.com/ceph/ceph/pull/6906>`_, YongQiang He)
* cls/cls_rbd.cc: fix misused metadata_name_from_key (`issue#13922 <http://tracker.ceph.com/issues/13922>`_, `pr#6661 <http://github.com/ceph/ceph/pull/6661>`_, Xiaoxi Chen)
* cmake: Add common/PluginRegistry.cc to CMakeLists.txt (`pr#6805 <http://github.com/ceph/ceph/pull/6805>`_, Pete Zaitcev)
* cmake: add rgw_basic_types.cc to librgw.a (`pr#6786 <http://github.com/ceph/ceph/pull/6786>`_, Orit Wasserman)
* cmake: add TracepointProvider.cc to libcommon (`pr#6823 <http://github.com/ceph/ceph/pull/6823>`_, Orit Wasserman)
* cmake: define STRERROR_R_CHAR_P for GNU-specific strerror_r (`pr#6751 <http://github.com/ceph/ceph/pull/6751>`_, Ilya Dryomov)
* cmake: update for recent librbd changes (`pr#6715 <http://github.com/ceph/ceph/pull/6715>`_, John Spray)
* cmake: update for recent rbd changes (`pr#6818 <http://github.com/ceph/ceph/pull/6818>`_, Mykola Golub)
* common: add generic plugin infrastructure (`pr#6696 <http://github.com/ceph/ceph/pull/6696>`_, Sage Weil)
* common: add latency perf counter for finisher (`pr#6175 <http://github.com/ceph/ceph/pull/6175>`_, Xinze Chi)
* common: buffer: add cached_crc and cached_crc_adjust counts to perf dump (`pr#6535 <http://github.com/ceph/ceph/pull/6535>`_, Ning Yao)
* common: buffer: remove unneeded list destructor (`pr#6456 <http://github.com/ceph/ceph/pull/6456>`_, Michal Jarzabek)
* common/ceph_context.cc:fix order of initialisers (`pr#6838 <http://github.com/ceph/ceph/pull/6838>`_, Michal Jarzabek)
* common: don't reverse hobject_t hash bits when zero (`pr#6653 <http://github.com/ceph/ceph/pull/6653>`_, Piotr Dałek)
* common: log: Assign LOG_DEBUG priority to syslog calls (`issue#13993 <http://tracker.ceph.com/issues/13993>`_, `pr#6815 <http://github.com/ceph/ceph/pull/6815>`_, Brad Hubbard)
* common: log: predict log message buffer allocation size (`pr#6641 <http://github.com/ceph/ceph/pull/6641>`_, Adam Kupczyk)
* common: optimize debug logging code (`pr#6441 <http://github.com/ceph/ceph/pull/6441>`_, Adam Kupczyk)
* common: perf counter for bufferlist history total alloc (`pr#6198 <http://github.com/ceph/ceph/pull/6198>`_, Xinze Chi)
* common: reduce CPU usage by making stringstream in stringify function thread local (`pr#6543 <http://github.com/ceph/ceph/pull/6543>`_, Evgeniy Firsov)
* common: re-enable backtrace support (`pr#6771 <http://github.com/ceph/ceph/pull/6771>`_, Jason Dillaman)
* common: SubProcess: fix multiple definition bug (`pr#6790 <http://github.com/ceph/ceph/pull/6790>`_, Yunchuan Wen)
* common: use namespace instead of subclasses for buffer (`pr#6686 <http://github.com/ceph/ceph/pull/6686>`_, Michal Jarzabek)
* configure.ac: macro fix (`pr#6769 <http://github.com/ceph/ceph/pull/6769>`_, Igor Podoski)
* doc: admin/build-doc: add lxml dependencies on debian (`pr#6610 <http://github.com/ceph/ceph/pull/6610>`_, Ken Dreyer)
* doc/cephfs/posix: update (`pr#6922 <http://github.com/ceph/ceph/pull/6922>`_, Sage Weil)
* doc: CodingStyle: fix broken URLs (`pr#6733 <http://github.com/ceph/ceph/pull/6733>`_, Kefu Chai)
* doc: correct typo 'restared' to 'restarted' (`pr#6734 <http://github.com/ceph/ceph/pull/6734>`_, Yilong Zhao)
* doc/dev/index: refactor/reorg (`pr#6792 <http://github.com/ceph/ceph/pull/6792>`_, Nathan Cutler)
* doc/dev/index.rst: begin writing Contributing to Ceph (`pr#6727 <http://github.com/ceph/ceph/pull/6727>`_, Nathan Cutler)
* doc/dev/index.rst: fix headings (`pr#6780 <http://github.com/ceph/ceph/pull/6780>`_, Nathan Cutler)
* doc: dev: introduction to tests (`pr#6910 <http://github.com/ceph/ceph/pull/6910>`_, Loic Dachary)
* doc: file must be empty when writing layout fields of file use "setfattr" (`pr#6848 <http://github.com/ceph/ceph/pull/6848>`_, Cilang Zhao)
* doc: Fixed incorrect name of a "List Multipart Upload Parts" Response Entity (`issue#14003 <http://tracker.ceph.com/issues/14003>`_, `pr#6829 <http://github.com/ceph/ceph/pull/6829>`_, Lenz Grimmer)
* doc: Fixes a spelling error (`pr#6705 <http://github.com/ceph/ceph/pull/6705>`_, Jeremy Qian)
* doc: fix typo in cephfs/quota (`pr#6745 <http://github.com/ceph/ceph/pull/6745>`_, Drunkard Zhang)
* doc: fix typo in developer guide (`pr#6943 <http://github.com/ceph/ceph/pull/6943>`_, Nathan Cutler)
* doc: INSTALL redirect to online documentation (`pr#6749 <http://github.com/ceph/ceph/pull/6749>`_, Loic Dachary)
* doc: little improvements for troubleshooting scrub issues (`pr#6827 <http://github.com/ceph/ceph/pull/6827>`_, Mykola Golub)
* doc: Modified a note section in rbd-snapshot doc. (`pr#6908 <http://github.com/ceph/ceph/pull/6908>`_, Nilamdyuti Goswami)
* doc: note that cephfs auth stuff is new in jewel (`pr#6858 <http://github.com/ceph/ceph/pull/6858>`_, John Spray)
* doc: osd: s/schedued/scheduled/ (`pr#6872 <http://github.com/ceph/ceph/pull/6872>`_, Loic Dachary)
* doc: remove unnecessary period in headline (`pr#6775 <http://github.com/ceph/ceph/pull/6775>`_, Marc Koderer)
* doc: rst style fix for pools document (`pr#6816 <http://github.com/ceph/ceph/pull/6816>`_, Drunkard Zhang)
* doc: Update list of admin/build-doc dependencies (`issue#14070 <http://tracker.ceph.com/issues/14070>`_, `pr#6934 <http://github.com/ceph/ceph/pull/6934>`_, Nathan Cutler)
* init-ceph: do umount when the path exists. (`pr#6866 <http://github.com/ceph/ceph/pull/6866>`_, Xiaoxi Chen)
* journal: disconnect watch after watch error (`issue#14168 <http://tracker.ceph.com/issues/14168>`_, `pr#7113 <http://github.com/ceph/ceph/pull/7113>`_, Jason Dillaman)
* journal: fire replay complete event after reading last object (`issue#13924 <http://tracker.ceph.com/issues/13924>`_, `pr#6762 <http://github.com/ceph/ceph/pull/6762>`_, Jason Dillaman)
* journal: support replaying beyond skipped splay objects (`pr#6687 <http://github.com/ceph/ceph/pull/6687>`_, Jason Dillaman)
* librados: aix gcc librados port (`pr#6675 <http://github.com/ceph/ceph/pull/6675>`_, Rohan Mars)
* librados: avoid malloc(0) (which can return NULL on some platforms) (`issue#13944 <http://tracker.ceph.com/issues/13944>`_, `pr#6779 <http://github.com/ceph/ceph/pull/6779>`_, Dan Mick)
* librados: clean up Objecter.h (`pr#6731 <http://github.com/ceph/ceph/pull/6731>`_, Jie Wang)
* librados: include/rados/librados.h: fix typo (`pr#6741 <http://github.com/ceph/ceph/pull/6741>`_, Nathan Cutler)
* librbd: automatically flush IO after blocking write operations (`issue#13913 <http://tracker.ceph.com/issues/13913>`_, `pr#6742 <http://github.com/ceph/ceph/pull/6742>`_, Jason Dillaman)
* librbd: better handling of exclusive lock transition period (`pr#7204 <http://github.com/ceph/ceph/pull/7204>`_, Jason Dillaman)
* librbd: check for presence of journal before attempting to remove  (`issue#13912 <http://tracker.ceph.com/issues/13912>`_, `pr#6737 <http://github.com/ceph/ceph/pull/6737>`_, Jason Dillaman)
* librbd: clear error when older OSD doesn't support image flags (`issue#14122 <http://tracker.ceph.com/issues/14122>`_, `pr#7035 <http://github.com/ceph/ceph/pull/7035>`_, Jason Dillaman)
* librbd: correct include guard in RenameRequest.h (`pr#7143 <http://github.com/ceph/ceph/pull/7143>`_, Jason Dillaman)
* librbd: correct issues discovered during teuthology testing (`issue#14108 <http://tracker.ceph.com/issues/14108>`_, `issue#14107 <http://tracker.ceph.com/issues/14107>`_, `pr#6974 <http://github.com/ceph/ceph/pull/6974>`_, Jason Dillaman)
* librbd: correct issues discovered when cache is disabled (`issue#14123 <http://tracker.ceph.com/issues/14123>`_, `pr#6979 <http://github.com/ceph/ceph/pull/6979>`_, Jason Dillaman)
* librbd: correct race conditions discovered during unit testing (`issue#14060 <http://tracker.ceph.com/issues/14060>`_, `pr#6923 <http://github.com/ceph/ceph/pull/6923>`_, Jason Dillaman)
* librbd: disable copy-on-read when not exclusive lock owner (`issue#14167 <http://tracker.ceph.com/issues/14167>`_, `pr#7129 <http://github.com/ceph/ceph/pull/7129>`_, Jason Dillaman)
* librbd: do not ignore self-managed snapshot release result (`issue#14170 <http://tracker.ceph.com/issues/14170>`_, `pr#7043 <http://github.com/ceph/ceph/pull/7043>`_, Jason Dillaman)
* librbd: ensure copy-on-read requests are complete prior to closing parent image  (`pr#6740 <http://github.com/ceph/ceph/pull/6740>`_, Jason Dillaman)
* librbd: ensure librados callbacks are flushed prior to destroying (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `pr#7040 <http://github.com/ceph/ceph/pull/7040>`_, Jason Dillaman)
* librbd: fix journal iohint (`pr#6917 <http://github.com/ceph/ceph/pull/6917>`_, Jianpeng Ma)
* librbd: fix known test case race condition failures (`issue#13969 <http://tracker.ceph.com/issues/13969>`_, `pr#6800 <http://github.com/ceph/ceph/pull/6800>`_, Jason Dillaman)
* librbd: fix merge-diff for >2GB diff-files (`issue#14030 <http://tracker.ceph.com/issues/14030>`_, `pr#6889 <http://github.com/ceph/ceph/pull/6889>`_, Yunchuan Wen)
* librbd: fix test case race condition for journaling ops (`pr#6877 <http://github.com/ceph/ceph/pull/6877>`_, Jason Dillaman)
* librbd: fix tracepoint parameter in diff_iterate (`pr#6892 <http://github.com/ceph/ceph/pull/6892>`_, Yunchuan Wen)
* librbd: image refresh code paths converted to async state machines (`pr#6859 <http://github.com/ceph/ceph/pull/6859>`_, Jason Dillaman)
* librbd: include missing header for bool type (`pr#6798 <http://github.com/ceph/ceph/pull/6798>`_, Mykola Golub)
* librbd: initial collection of state machine unit tests (`pr#6703 <http://github.com/ceph/ceph/pull/6703>`_, Jason Dillaman)
* librbd: integrate journaling for maintenance operations (`pr#6625 <http://github.com/ceph/ceph/pull/6625>`_, Jason Dillaman)
* librbd: journaling-related lock dependency cleanup (`pr#6777 <http://github.com/ceph/ceph/pull/6777>`_, Jason Dillaman)
* librbd: not necessary to hold owner_lock while releasing snap id (`issue#13914 <http://tracker.ceph.com/issues/13914>`_, `pr#6736 <http://github.com/ceph/ceph/pull/6736>`_, Jason Dillaman)
* librbd: only send signal when AIO completions queue empty (`pr#6729 <http://github.com/ceph/ceph/pull/6729>`_, Jianpeng Ma)
* librbd: optionally validate new RBD pools for snapshot support (`issue#13633 <http://tracker.ceph.com/issues/13633>`_, `pr#6925 <http://github.com/ceph/ceph/pull/6925>`_, Jason Dillaman)
* librbd: partial revert of commit 9b0e359 (`issue#13969 <http://tracker.ceph.com/issues/13969>`_, `pr#6789 <http://github.com/ceph/ceph/pull/6789>`_, Jason Dillaman)
* librbd: properly handle replay of snap remove RPC message (`issue#14164 <http://tracker.ceph.com/issues/14164>`_, `pr#7042 <http://github.com/ceph/ceph/pull/7042>`_, Jason Dillaman)
* librbd: reduce verbosity of common error condition logging (`issue#14234 <http://tracker.ceph.com/issues/14234>`_, `pr#7114 <http://github.com/ceph/ceph/pull/7114>`_, Jason Dillaman)
* librbd: simplify IO method signatures for 32bit environments (`pr#6700 <http://github.com/ceph/ceph/pull/6700>`_, Jason Dillaman)
* librbd: support eventfd for AIO completion notifications (`pr#5465 <http://github.com/ceph/ceph/pull/5465>`_, Haomai Wang)
* mailmap: add UMCloud affiliation (`pr#6820 <http://github.com/ceph/ceph/pull/6820>`_, Jiaying Ren)
* mailmap: Jewel updates (`pr#6750 <http://github.com/ceph/ceph/pull/6750>`_, Abhishek Lekshmanan)
* makefiles: remove bz2-dev from dependencies (`issue#13981 <http://tracker.ceph.com/issues/13981>`_, `pr#6939 <http://github.com/ceph/ceph/pull/6939>`_, Piotr Dałek)
* mds: add 'p' flag in auth caps to control setting pool in layout (`pr#6567 <http://github.com/ceph/ceph/pull/6567>`_, John Spray)
* mds: fix client capabilities during reconnect (client.XXXX isn't responding to mclientcaps(revoke)) (`issue#11482 <http://tracker.ceph.com/issues/11482>`_, `pr#6432 <http://github.com/ceph/ceph/pull/6432>`_, Yan, Zheng)
* mds: fix setvxattr (broken in a536d114) (`issue#14029 <http://tracker.ceph.com/issues/14029>`_, `pr#6941 <http://github.com/ceph/ceph/pull/6941>`_, John Spray)
* mds: repair the command option "--hot-standby" (`pr#6454 <http://github.com/ceph/ceph/pull/6454>`_, Wei Feng)
* mds: tear down connections from `tell` commands (`issue#14048 <http://tracker.ceph.com/issues/14048>`_, `pr#6933 <http://github.com/ceph/ceph/pull/6933>`_, John Spray)
* mon: fix ceph df pool available calculation for 0-weighted OSDs (`pr#6660 <http://github.com/ceph/ceph/pull/6660>`_, Chengyuan Li)
* mon: fix routed_request_tids leak (`pr#6102 <http://github.com/ceph/ceph/pull/6102>`_, Ning Yao)
* mon: support min_down_reporter by subtree level (default by host) (`pr#6709 <http://github.com/ceph/ceph/pull/6709>`_, Xiaoxi Chen)
* mount.ceph: memory leaks (`pr#6905 <http://github.com/ceph/ceph/pull/6905>`_, Qiankun Zheng)
* osd: add osd op queue latency perfcounter (`pr#5793 <http://github.com/ceph/ceph/pull/5793>`_, Haomai Wang)
* osd: Allow repair of history.last_epoch_started using config (`pr#6793 <http://github.com/ceph/ceph/pull/6793>`_, David Zafman)
* osd: avoid duplicate op->mark_started in ReplicatedBackend (`pr#6689 <http://github.com/ceph/ceph/pull/6689>`_, Jacek J. Łakis)
* osd: cancel failure reports if we fail to rebind network (`pr#6278 <http://github.com/ceph/ceph/pull/6278>`_, Xinze Chi)
* osd: correctly handle small osd_scrub_interval_randomize_ratio (`pr#7147 <http://github.com/ceph/ceph/pull/7147>`_, Samuel Just)
* osd: defer decoding of MOSDRepOp/MOSDRepOpReply (`pr#6503 <http://github.com/ceph/ceph/pull/6503>`_, Xinze Chi)
* osd: don't update epoch and rollback_info objects attrs if there is no need (`pr#6555 <http://github.com/ceph/ceph/pull/6555>`_, Ning Yao)
* osd: dump number of missing objects for each peer with pg query (`pr#6058 <http://github.com/ceph/ceph/pull/6058>`_, Guang Yang)
* osd: enable perfcounters on sharded work queue mutexes (`pr#6455 <http://github.com/ceph/ceph/pull/6455>`_, Jacek J. Łakis)
* osd: FileJournal: reduce locking scope in write_aio_bl (`issue#12789 <http://tracker.ceph.com/issues/12789>`_, `pr#5670 <http://github.com/ceph/ceph/pull/5670>`_, Zhi Zhang)
* osd: FileStore: remove __SWORD_TYPE dependency (`pr#6263 <http://github.com/ceph/ceph/pull/6263>`_, John Coyle)
* osd: fix FileStore::_destroy_collection error return code (`pr#6612 <http://github.com/ceph/ceph/pull/6612>`_, Ruifeng Yang)
* osd: fix incorrect throttle in WBThrottle (`pr#6713 <http://github.com/ceph/ceph/pull/6713>`_, Zhang Huan)
* osd: fix MOSDRepScrub reference counter in replica_scrub (`pr#6730 <http://github.com/ceph/ceph/pull/6730>`_, Jie Wang)
* osd: fix rollback_info_trimmed_to before index() (`issue#13965 <http://tracker.ceph.com/issues/13965>`_, `pr#6801 <http://github.com/ceph/ceph/pull/6801>`_, Samuel Just)
* osd: fix trivial scrub bug (`pr#6533 <http://github.com/ceph/ceph/pull/6533>`_, Li Wang)
* osd: KeyValueStore: don't queue NULL context (`pr#6783 <http://github.com/ceph/ceph/pull/6783>`_, Haomai Wang)
* osd: make backend and block device code a bit more generic (`pr#6759 <http://github.com/ceph/ceph/pull/6759>`_, Sage Weil)
* osd: move newest decode version of MOSDOp and MOSDOpReply to the front (`pr#6642 <http://github.com/ceph/ceph/pull/6642>`_, Jacek J. Łakis)
* osd: pg_pool_t: add dictionary for pool options (`issue#13077 <http://tracker.ceph.com/issues/13077>`_, `pr#6081 <http://github.com/ceph/ceph/pull/6081>`_, Mykola Golub)
* osd: reduce memory consumption of some structs (`pr#6475 <http://github.com/ceph/ceph/pull/6475>`_, Piotr Dałek)
* osd: release the message throttle when OpRequest unregistered (`issue#14248 <http://tracker.ceph.com/issues/14248>`_, `pr#7148 <http://github.com/ceph/ceph/pull/7148>`_, Samuel Just)
* osd: remove __SWORD_TYPE dependency (`pr#6262 <http://github.com/ceph/ceph/pull/6262>`_, John Coyle)
* osd: slightly reduce actual size of pg_log_entry_t (`pr#6690 <http://github.com/ceph/ceph/pull/6690>`_, Piotr Dałek)
* osd: support pool level recovery_priority and recovery_op_priority (`pr#5953 <http://github.com/ceph/ceph/pull/5953>`_, Guang Yang)
* osd: use pg id (without shard) when referring the PG (`pr#6236 <http://github.com/ceph/ceph/pull/6236>`_, Guang Yang)
* packaging: add build dependency on python devel package (`pr#7205 <http://github.com/ceph/ceph/pull/7205>`_, Josh Durgin)
* pybind/cephfs: add symlink and its unit test (`pr#6323 <http://github.com/ceph/ceph/pull/6323>`_, Shang Ding)
* pybind: decode empty string in conf_parse_argv() correctly (`pr#6711 <http://github.com/ceph/ceph/pull/6711>`_, Josh Durgin)
* pybind: Implementation of rados_ioctx_snapshot_rollback (`pr#6878 <http://github.com/ceph/ceph/pull/6878>`_, Florent Manens)
* pybind: port the rbd bindings to Cython (`issue#13115 <http://tracker.ceph.com/issues/13115>`_, `pr#6768 <http://github.com/ceph/ceph/pull/6768>`_, Hector Martin)
* pybind: support ioctx:exec (`pr#6795 <http://github.com/ceph/ceph/pull/6795>`_, Noah Watkins)
* qa: erasure-code benchmark plugin selection (`pr#6685 <http://github.com/ceph/ceph/pull/6685>`_, Loic Dachary)
* qa/krbd: Expunge generic/247 (`pr#6831 <http://github.com/ceph/ceph/pull/6831>`_, Douglas Fuller)
* qa/workunits/cephtool/test.sh: false positive fail on /tmp/obj1. (`pr#6837 <http://github.com/ceph/ceph/pull/6837>`_, Robin H. Johnson)
* qa/workunits/cephtool/test.sh: no ./ (`pr#6748 <http://github.com/ceph/ceph/pull/6748>`_, Sage Weil)
* qa/workunits/rbd: rbd-nbd test should use sudo for map/unmap ops (`issue#14221 <http://tracker.ceph.com/issues/14221>`_, `pr#7101 <http://github.com/ceph/ceph/pull/7101>`_, Jason Dillaman)
* rados: bench: fix off-by-one to avoid writing past object_size (`pr#6677 <http://github.com/ceph/ceph/pull/6677>`_, Tao Chang)
* rbd: add --object-size option, deprecate --order (`issue#12112 <http://tracker.ceph.com/issues/12112>`_, `pr#6830 <http://github.com/ceph/ceph/pull/6830>`_, Vikhyat Umrao)
* rbd: add RBD pool mirroring configuration API + CLI (`pr#6129 <http://github.com/ceph/ceph/pull/6129>`_, Jason Dillaman)
* rbd: fix build with "--without-rbd" (`issue#14058 <http://tracker.ceph.com/issues/14058>`_, `pr#6899 <http://github.com/ceph/ceph/pull/6899>`_, Piotr Dałek)
* rbd: journal: configuration via conf, cli, api and some fixes (`pr#6665 <http://github.com/ceph/ceph/pull/6665>`_, Mykola Golub)
* rbd: merge_diff test should use new --object-size parameter instead of --order (`issue#14106 <http://tracker.ceph.com/issues/14106>`_, `pr#6972 <http://github.com/ceph/ceph/pull/6972>`_, Na Xie, Jason Dillaman)
* rbd-nbd: network block device (NBD) support for RBD  (`pr#6657 <http://github.com/ceph/ceph/pull/6657>`_, Yunchuan Wen, Li Wang)
* rbd: output formatter may not be closed upon error (`issue#13711 <http://tracker.ceph.com/issues/13711>`_, `pr#6706 <http://github.com/ceph/ceph/pull/6706>`_, xie xingguo)
* rgw: add a missing cap type (`pr#6774 <http://github.com/ceph/ceph/pull/6774>`_, Yehuda Sadeh)
* rgw: add an inspection to the field of type when assigning user caps (`pr#6051 <http://github.com/ceph/ceph/pull/6051>`_, Kongming Wu)
* rgw: add LifeCycle feature (`pr#6331 <http://github.com/ceph/ceph/pull/6331>`_, Ji Chen)
* rgw: add support for Static Large Objects of Swift API (`issue#12886 <http://tracker.ceph.com/issues/12886>`_, `issue#13452 <http://tracker.ceph.com/issues/13452>`_, `pr#6643 <http://github.com/ceph/ceph/pull/6643>`_, Yehuda Sadeh, Radoslaw Zarzynski)
* rgw: fix a glaring syntax error (`pr#6888 <http://github.com/ceph/ceph/pull/6888>`_, Pavan Rallabhandi)
* rgw: fix the build failure (`pr#6927 <http://github.com/ceph/ceph/pull/6927>`_, Kefu Chai)
* rgw: multitenancy support (`pr#6784 <http://github.com/ceph/ceph/pull/6784>`_, Yehuda Sadeh, Pete Zaitcev)
* rgw: Remove unused code in PutMetadataAccount:execute (`pr#6668 <http://github.com/ceph/ceph/pull/6668>`_, Pete Zaitcev)
* rgw: remove unused variable in RGWPutMetadataBucket::execute (`pr#6735 <http://github.com/ceph/ceph/pull/6735>`_, Radoslaw Zarzynski)
* rgw/rgw_resolve: fallback to res_query when res_nquery not implemented (`pr#6292 <http://github.com/ceph/ceph/pull/6292>`_, John Coyle)
* rgw: static large objects (Radoslaw Zarzynski, Yehuda Sadeh)
* rgw: swift bulk delete (Radoslaw Zarzynski)
* systemd: start/stop/restart ceph services by daemon type (`issue#13497 <http://tracker.ceph.com/issues/13497>`_, `pr#6276 <http://github.com/ceph/ceph/pull/6276>`_, Zhi Zhang)
* sysvinit: allow custom cluster names (`pr#6732 <http://github.com/ceph/ceph/pull/6732>`_, Richard Chan)
* test/encoding/readable.sh fix (`pr#6714 <http://github.com/ceph/ceph/pull/6714>`_, Igor Podoski)
* test: fix osd-scrub-snaps.sh (`pr#6697 <http://github.com/ceph/ceph/pull/6697>`_, Xinze Chi)
* test/librados/test.cc: clean up EC pools' crush rules too (`issue#13878 <http://tracker.ceph.com/issues/13878>`_, `pr#6788 <http://github.com/ceph/ceph/pull/6788>`_, Loic Dachary, Dan Mick)
* tests: allow object corpus readable test to skip specific incompat instances (`pr#6932 <http://github.com/ceph/ceph/pull/6932>`_, Igor Podoski)
* tests: ceph-helpers assert success getting backfills (`pr#6699 <http://github.com/ceph/ceph/pull/6699>`_, Loic Dachary)
* tests: ceph_test_keyvaluedb_iterators: fix broken test (`pr#6597 <http://github.com/ceph/ceph/pull/6597>`_, Haomai Wang)
* tests: fix failure for osd-scrub-snap.sh (`issue#13986 <http://tracker.ceph.com/issues/13986>`_, `pr#6890 <http://github.com/ceph/ceph/pull/6890>`_, Loic Dachary, Ning Yao)
* tests: fix race condition testing auto scrub (`issue#13592 <http://tracker.ceph.com/issues/13592>`_, `pr#6724 <http://github.com/ceph/ceph/pull/6724>`_, Xinze Chi, Loic Dachary)
* tests: flush op work queue prior to destroying MockImageCtx (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `pr#7002 <http://github.com/ceph/ceph/pull/7002>`_, Jason Dillaman)
* tests: --osd-scrub-load-threshold=2000 for more consistency (`issue#14027 <http://tracker.ceph.com/issues/14027>`_, `pr#6871 <http://github.com/ceph/ceph/pull/6871>`_, Loic Dachary)
* tests: osd-scrub-snaps.sh to display full osd logs on error (`issue#13986 <http://tracker.ceph.com/issues/13986>`_, `pr#6857 <http://github.com/ceph/ceph/pull/6857>`_, Loic Dachary)
* test: use sequential journal_tid for object cacher test (`issue#13877 <http://tracker.ceph.com/issues/13877>`_, `pr#6710 <http://github.com/ceph/ceph/pull/6710>`_, Josh Durgin)
* tools: add cephfs-table-tool 'take_inos' (`pr#6655 <http://github.com/ceph/ceph/pull/6655>`_, John Spray)
* tools: Fix layout handing in cephfs-data-scan (#13898) (`pr#6719 <http://github.com/ceph/ceph/pull/6719>`_, John Spray)
* tools: support printing part cluster map in readable fashion (`issue#13079 <http://tracker.ceph.com/issues/13079>`_, `pr#5921 <http://github.com/ceph/ceph/pull/5921>`_, Bo Cai)
* vstart.sh: add mstart, mstop, mrun wrappers for running multiple vstart-style test clusters out of src tree (`pr#6901 <http://github.com/ceph/ceph/pull/6901>`_, Yehuda Sadeh)
* bluestore: latest and greatest (`issue#14210 <http://tracker.ceph.com/issues/14210>`_, `issue#13801 <http://tracker.ceph.com/issues/13801>`_, `pr#6896 <http://github.com/ceph/ceph/pull/6896>`_, xie.xingguo, Jianpeng Ma, YiQiang Chen, Sage Weil, Ning Yao)
* buffer: fix internal iterator invalidation on rebuild, get_contiguous (`pr#6962 <http://github.com/ceph/ceph/pull/6962>`_, Sage Weil)
* build: fix a few warnings (`pr#6847 <http://github.com/ceph/ceph/pull/6847>`_, Orit Wasserman)
* build: misc make check fixes (`pr#7153 <http://github.com/ceph/ceph/pull/7153>`_, Sage Weil)
* ceph-detect-init: fix py3 test (`pr#7025 <http://github.com/ceph/ceph/pull/7025>`_, Kefu Chai)
* ceph-disk: add -f flag for btrfs mkfs (`pr#7222 <http://github.com/ceph/ceph/pull/7222>`_, Darrell Enns)
* ceph-disk: ceph-disk list fails on /dev/cciss!c0d0 (`issue#13970 <http://tracker.ceph.com/issues/13970>`_, `issue#14233 <http://tracker.ceph.com/issues/14233>`_, `issue#14230 <http://tracker.ceph.com/issues/14230>`_, `pr#6879 <http://github.com/ceph/ceph/pull/6879>`_, Loic Dachary)
* ceph-disk: fix failures when preparing disks with udev > 214 (`issue#14080 <http://tracker.ceph.com/issues/14080>`_, `issue#14094 <http://tracker.ceph.com/issues/14094>`_, `pr#6926 <http://github.com/ceph/ceph/pull/6926>`_, Loic Dachary, Ilya Dryomov)
* ceph-disk: Fix trivial typo (`pr#7472 <http://github.com/ceph/ceph/pull/7472>`_, Brad Hubbard)
* ceph-disk: warn for prepare partitions with bad GUIDs (`issue#13943 <http://tracker.ceph.com/issues/13943>`_, `pr#6760 <http://github.com/ceph/ceph/pull/6760>`_, David Disseldorp)
* ceph-fuse: fix double decreasing the count to trim caps (`issue#14319 <http://tracker.ceph.com/issues/14319>`_, `pr#7229 <http://github.com/ceph/ceph/pull/7229>`_, Zhi Zhang)
* ceph-fuse: fix double free of args (`pr#7015 <http://github.com/ceph/ceph/pull/7015>`_, Ilya Shipitsin)
* ceph-fuse: fix fsync() (`pr#6388 <http://github.com/ceph/ceph/pull/6388>`_, Yan, Zheng)
* ceph-fuse:print usage information when no parameter specified (`pr#6868 <http://github.com/ceph/ceph/pull/6868>`_, Bo Cai)
* ceph: improve the error message (`issue#11101 <http://tracker.ceph.com/issues/11101>`_, `pr#7106 <http://github.com/ceph/ceph/pull/7106>`_, Kefu Chai)
* ceph.in: avoid a broken pipe error when use ceph command (`issue#14354 <http://tracker.ceph.com/issues/14354>`_, `pr#7212 <http://github.com/ceph/ceph/pull/7212>`_, Bo Cai)
* ceph.spec.in: add copyright notice (`issue#14694 <http://tracker.ceph.com/issues/14694>`_, `pr#7569 <http://github.com/ceph/ceph/pull/7569>`_, Nathan Cutler)
* ceph.spec.in: add license declaration (`pr#7574 <http://github.com/ceph/ceph/pull/7574>`_, Nathan Cutler)
* ceph_test_libcephfs: tolerate duplicated entries in readdir (`issue#14377 <http://tracker.ceph.com/issues/14377>`_, `pr#7246 <http://github.com/ceph/ceph/pull/7246>`_, Yan, Zheng)
* client: check if Fh is readable when processing a read (`issue#11517 <http://tracker.ceph.com/issues/11517>`_, `pr#7209 <http://github.com/ceph/ceph/pull/7209>`_, Yan, Zheng)
* client: properly trim unlinked inode (`issue#13903 <http://tracker.ceph.com/issues/13903>`_, `pr#7297 <http://github.com/ceph/ceph/pull/7297>`_, Yan, Zheng)
* cls_rbd: add guards for error cases (`issue#14316 <http://tracker.ceph.com/issues/14316>`_, `issue#14317 <http://tracker.ceph.com/issues/14317>`_, `pr#7165 <http://github.com/ceph/ceph/pull/7165>`_, xie xingguo)
* cls_rbd: enable object map checksums for object_map_save (`issue#14280 <http://tracker.ceph.com/issues/14280>`_, `pr#7149 <http://github.com/ceph/ceph/pull/7149>`_, Douglas Fuller)
* cmake: Add ENABLE_GIT_VERSION to avoid rebuilding (`pr#7171 <http://github.com/ceph/ceph/pull/7171>`_, Kefu Chai)
* cmake: add missing check for HAVE_EXECINFO_H (`pr#7270 <http://github.com/ceph/ceph/pull/7270>`_, Casey Bodley)
* cmake: cleanups and more features from automake (`pr#7103 <http://github.com/ceph/ceph/pull/7103>`_, Casey Bodley, Ali Maredia)
* cmake: detect bzip2 and lz4 (`pr#7126 <http://github.com/ceph/ceph/pull/7126>`_, Kefu Chai)
* cmake: fix build with bluestore (`pr#7099 <http://github.com/ceph/ceph/pull/7099>`_, John Spray)
* cmake: fix the build on trusty (`pr#7249 <http://github.com/ceph/ceph/pull/7249>`_, Kefu Chai)
* cmake: made rocksdb an imported library (`pr#7131 <http://github.com/ceph/ceph/pull/7131>`_, Ali Maredia)
* cmake: no need to run configure from run-cmake-check.sh (`pr#6959 <http://github.com/ceph/ceph/pull/6959>`_, Orit Wasserman)
* cmake: test_build_libcephfs needs ${ALLOC_LIBS} (`pr#7300 <http://github.com/ceph/ceph/pull/7300>`_, Ali Maredia)
* common/address_help.cc: fix the leak in entity_addr_from_url() (`issue#14132 <http://tracker.ceph.com/issues/14132>`_, `pr#6987 <http://github.com/ceph/ceph/pull/6987>`_, Qiankun Zheng)
* common: add thread names (`pr#5882 <http://github.com/ceph/ceph/pull/5882>`_, Igor Podoski)
* common: assert: abort() rather than throw (`pr#6804 <http://github.com/ceph/ceph/pull/6804>`_, Adam C. Emerson)
* common: buffer/assert minor fixes (`pr#6990 <http://github.com/ceph/ceph/pull/6990>`_, Matt Benjamin)
* common/Formatter: avoid newline if there is no output (`pr#5351 <http://github.com/ceph/ceph/pull/5351>`_, Aran85)
* common: improve shared_cache and simple_cache efficiency with hash table (`pr#6909 <http://github.com/ceph/ceph/pull/6909>`_, Ning Yao)
* common/lockdep: increase max lock names (`pr#6961 <http://github.com/ceph/ceph/pull/6961>`_, Sage Weil)
* common: new timekeeping common code, and Objecter conversion (`pr#5782 <http://github.com/ceph/ceph/pull/5782>`_, Adam C. Emerson)
* common: signal_handler: added support for using reentrant strsignal() implementations vs. sys_siglist[] (`pr#6796 <http://github.com/ceph/ceph/pull/6796>`_, John Coyle)
* config: complains when a setting is not tracked (`issue#11692 <http://tracker.ceph.com/issues/11692>`_, `pr#7085 <http://github.com/ceph/ceph/pull/7085>`_, Kefu Chai)
* configure: detect bz2 and lz4 (`issue#13850 <http://tracker.ceph.com/issues/13850>`_, `issue#13981 <http://tracker.ceph.com/issues/13981>`_, `pr#7030 <http://github.com/ceph/ceph/pull/7030>`_, Kefu Chai)
* correct radosgw-admin command (`pr#7006 <http://github.com/ceph/ceph/pull/7006>`_, YankunLi)
* crush: add chooseleaf_stable tunable (`pr#6572 <http://github.com/ceph/ceph/pull/6572>`_, Sangdi Xu, Sage Weil)
* crush: clean up whitespace removal (`issue#14302 <http://tracker.ceph.com/issues/14302>`_, `pr#7157 <http://github.com/ceph/ceph/pull/7157>`_, songbaisen)
* crush/CrushTester: check for overlapped rules (`pr#7139 <http://github.com/ceph/ceph/pull/7139>`_, Kefu Chai)
* crushtool: improve usage/tip messages (`pr#7142 <http://github.com/ceph/ceph/pull/7142>`_, xie xingguo)
* crushtool: set type 0 name "device" for --build option (`pr#6824 <http://github.com/ceph/ceph/pull/6824>`_, Sangdi Xu)
* doc: adding "--allow-shrink" in decreasing the size of the rbd block to distinguish from the increasing option (`pr#7020 <http://github.com/ceph/ceph/pull/7020>`_, Yehua)
* doc: admin/build-doc: make paths absolute (`pr#7119 <http://github.com/ceph/ceph/pull/7119>`_, Dan Mick)
* doc: dev: document ceph-qa-suite (`pr#6955 <http://github.com/ceph/ceph/pull/6955>`_, Loic Dachary)
* doc: document "readforward" and "readproxy" cache mode (`pr#7023 <http://github.com/ceph/ceph/pull/7023>`_, Kefu Chai)
* doc: fix "mon osd down out subtree limit" option name (`pr#7164 <http://github.com/ceph/ceph/pull/7164>`_, François Lafont)
* doc: fix typo (`pr#7004 <http://github.com/ceph/ceph/pull/7004>`_, tianqing)
* doc: Updated the rados command man page to include the --run-name opt… (`issue#12899 <http://tracker.ceph.com/issues/12899>`_, `pr#5900 <http://github.com/ceph/ceph/pull/5900>`_, ritz303)
* fs: be more careful about the "mds setmap" command to prevent breakage (`issue#14380 <http://tracker.ceph.com/issues/14380>`_, `pr#7262 <http://github.com/ceph/ceph/pull/7262>`_, Yan, Zheng)
* helgrind: additional race conditionslibrbd: journal replay should honor inter-event dependencies (`pr#7274 <http://github.com/ceph/ceph/pull/7274>`_, Jason Dillaman)
* helgrind: fix real (and imaginary) race conditions (`issue#14163 <http://tracker.ceph.com/issues/14163>`_, `pr#7208 <http://github.com/ceph/ceph/pull/7208>`_, Jason Dillaman)
* kv: implement value_as_ptr() and use it in .get() (`pr#7052 <http://github.com/ceph/ceph/pull/7052>`_, Piotr Dałek)
* librados: add c++ style osd/pg command interface (`pr#6893 <http://github.com/ceph/ceph/pull/6893>`_, Yunchuan Wen)
* librados: fix several flaws introduced by the enumeration_objects API (`issue#14299 <http://tracker.ceph.com/issues/14299>`_, `issue#14301 <http://tracker.ceph.com/issues/14301>`_, `issue#14300 <http://tracker.ceph.com/issues/14300>`_, `pr#7156 <http://github.com/ceph/ceph/pull/7156>`_, xie xingguo)
* librados: new style (sharded) object listing (`pr#6405 <http://github.com/ceph/ceph/pull/6405>`_, John Spray, Sage Weil)
* librados: potential null pointer access in list_(n)objects (`issue#13822 <http://tracker.ceph.com/issues/13822>`_, `pr#6639 <http://github.com/ceph/ceph/pull/6639>`_, xie xingguo)
* librbd: exit if parent's snap is gone during clone (`issue#14118 <http://tracker.ceph.com/issues/14118>`_, `pr#6968 <http://github.com/ceph/ceph/pull/6968>`_, xie xingguo)
* librbd: fix potential memory leak (`issue#14332 <http://tracker.ceph.com/issues/14332>`_, `issue#14333 <http://tracker.ceph.com/issues/14333>`_, `pr#7174 <http://github.com/ceph/ceph/pull/7174>`_, xie xingguo)
* librbd: fix snap_exists API return code overflow (`issue#14129 <http://tracker.ceph.com/issues/14129>`_, `pr#6986 <http://github.com/ceph/ceph/pull/6986>`_, xie xingguo)
* librbd: journal replay should honor inter-event dependencies (`pr#7019 <http://github.com/ceph/ceph/pull/7019>`_, Jason Dillaman)
* librbd: return error if we fail to delete object_map head object (`issue#14098 <http://tracker.ceph.com/issues/14098>`_, `pr#6958 <http://github.com/ceph/ceph/pull/6958>`_, xie xingguo)
* librbd: small fixes for error messages and readahead counter (`issue#14127 <http://tracker.ceph.com/issues/14127>`_, `pr#6983 <http://github.com/ceph/ceph/pull/6983>`_, xie xingguo)
* librbd: uninitialized state in snap remove state machine (`pr#6982 <http://github.com/ceph/ceph/pull/6982>`_, Jason Dillaman)
* mailmap: hange organization for Dongmao Zhang (`pr#7173 <http://github.com/ceph/ceph/pull/7173>`_, Dongmao Zhang)
* mailmap: Igor Podoski affiliation (`pr#7219 <http://github.com/ceph/ceph/pull/7219>`_, Igor Podoski)
* mailmap update (`pr#7210 <http://github.com/ceph/ceph/pull/7210>`_, M Ranga Swami Reddy)
* mailmap updates (`pr#6992 <http://github.com/ceph/ceph/pull/6992>`_, Loic Dachary)
* mailmap updates (`pr#7189 <http://github.com/ceph/ceph/pull/7189>`_, Loic Dachary)
* man: document listwatchers cmd in "rados" manpage (`pr#7021 <http://github.com/ceph/ceph/pull/7021>`_, Kefu Chai)
* mds: advance clientreplay when replying (`issue#14357 <http://tracker.ceph.com/issues/14357>`_, `pr#7216 <http://github.com/ceph/ceph/pull/7216>`_, John Spray)
* mds: expose state of recovery to status ASOK command (`issue#14146 <http://tracker.ceph.com/issues/14146>`_, `pr#7068 <http://github.com/ceph/ceph/pull/7068>`_, Yan, Zheng)
* mds: fix client cap/message replay order on restart (`issue#14254 <http://tracker.ceph.com/issues/14254>`_, `issue#13546 <http://tracker.ceph.com/issues/13546>`_, `pr#7199 <http://github.com/ceph/ceph/pull/7199>`_, Yan, Zheng)
* mds: fix standby replay thread creation (`issue#14144 <http://tracker.ceph.com/issues/14144>`_, `pr#7132 <http://github.com/ceph/ceph/pull/7132>`_, John Spray)
* mds: we should wait messenger when MDSDaemon suicide (`pr#6996 <http://github.com/ceph/ceph/pull/6996>`_, Wei Feng)
* mon: add `osd blacklist clear` (`pr#6945 <http://github.com/ceph/ceph/pull/6945>`_, John Spray)
* mon: add RAW USED column to ceph df detail (`pr#7087 <http://github.com/ceph/ceph/pull/7087>`_, Ruifeng Yang)
* mon: degrade a log message to level 2 (`pr#6929 <http://github.com/ceph/ceph/pull/6929>`_, Kongming Wu)
* mon: fix coding-style on PG related Monitor files (`pr#6881 <http://github.com/ceph/ceph/pull/6881>`_, Wido den Hollander)
* mon: fixes related to mondbstore->get() changes (`pr#6564 <http://github.com/ceph/ceph/pull/6564>`_, Piotr Dałek)
* mon: fix reuse of osd ids (clear osd info on osd deletion) (`issue#13988 <http://tracker.ceph.com/issues/13988>`_, `pr#6900 <http://github.com/ceph/ceph/pull/6900>`_, Loic Dachary, Sage Weil)
* mon: fix the can't change subscribe level bug in monitoring log (`pr#7031 <http://github.com/ceph/ceph/pull/7031>`_, Zhiqiang Wang)
* mon/MDSMonitor: add confirmation to "ceph mds rmfailed" (`issue#14379 <http://tracker.ceph.com/issues/14379>`_, `pr#7248 <http://github.com/ceph/ceph/pull/7248>`_, Yan, Zheng)
* mon: modify a dout level in OSDMonitor.cc (`pr#6928 <http://github.com/ceph/ceph/pull/6928>`_, Yongqiang He)
* mon: MonmapMonitor: don't expose uncommitted state to client (`pr#6854 <http://github.com/ceph/ceph/pull/6854>`_, Joao Eduardo Luis)
* mon/OSDMonitor: osdmap laggy set a maximum limit for interval (`pr#7109 <http://github.com/ceph/ceph/pull/7109>`_, Zengran Zhang)
* mon: paxos is_recovering calc error (`pr#7227 <http://github.com/ceph/ceph/pull/7227>`_, Weijun Duan)
* mon/PGMap: show rd/wr iops separately in status reports (`pr#7072 <http://github.com/ceph/ceph/pull/7072>`_, Cilang Zhao)
* mon: PGMonitor: acting primary diff with cur_stat, should not set pg to stale (`pr#7083 <http://github.com/ceph/ceph/pull/7083>`_, Xiaowei Chen)
* msg: add override to virutal methods (`pr#6977 <http://github.com/ceph/ceph/pull/6977>`_, Michal Jarzabek)
* msg/async: cleanup dead connection and misc things (`pr#7158 <http://github.com/ceph/ceph/pull/7158>`_, Haomai Wang)
* msg/async: don't use shared_ptr to manage EventCallback (`pr#7028 <http://github.com/ceph/ceph/pull/7028>`_, Haomai Wang)
* msg: filter out lo addr when bind osd addr (`pr#7012 <http://github.com/ceph/ceph/pull/7012>`_, Ji Chen)
* msg: removed unneeded includes from Dispatcher (`pr#6814 <http://github.com/ceph/ceph/pull/6814>`_, Michal Jarzabek)
* msg: remove unneeded inline (`pr#6989 <http://github.com/ceph/ceph/pull/6989>`_, Michal Jarzabek)
* msgr:  fix large message data content length causing overflow (`pr#6809 <http://github.com/ceph/ceph/pull/6809>`_, Jun Huang, Haomai Wang)
* msg/simple: pipe: memory leak when signature check failed (`pr#7096 <http://github.com/ceph/ceph/pull/7096>`_, Ruifeng Yang)
* msg/simple: remove unneeded friend declarations (`pr#6924 <http://github.com/ceph/ceph/pull/6924>`_, Michal Jarzabek)
* objecter: avoid recursive lock of Objecter::rwlock (`pr#7343 <http://github.com/ceph/ceph/pull/7343>`_, Yan, Zheng)
* os/bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7419 <http://github.com/ceph/ceph/pull/7419>`_, Kefu Chai, Brad Hubbard)
* osd: add cache hint when pushing raw clone during recovery (`pr#7069 <http://github.com/ceph/ceph/pull/7069>`_, Zhiqiang Wang)
* osd: avoid debug std::string initialization in PG::get/put (`pr#7117 <http://github.com/ceph/ceph/pull/7117>`_, Evgeniy Firsov)
* osd: avoid osd_op_thread suicide because osd_scrub_sleep (`pr#7009 <http://github.com/ceph/ceph/pull/7009>`_, Jianpeng Ma)
* osd: bluestore: bluefs: fix several small bugs (`issue#14344 <http://tracker.ceph.com/issues/14344>`_, `issue#14343 <http://tracker.ceph.com/issues/14343>`_, `pr#7200 <http://github.com/ceph/ceph/pull/7200>`_, xie xingguo)
* osd: bluestore: don't include when building without libaio (`issue#14207 <http://tracker.ceph.com/issues/14207>`_, `pr#7169 <http://github.com/ceph/ceph/pull/7169>`_, Mykola Golub)
* osd: bluestore: fix bluestore onode_t attr leak (`pr#7125 <http://github.com/ceph/ceph/pull/7125>`_, Ning Yao)
* osd: bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7168 <http://github.com/ceph/ceph/pull/7168>`_, Kefu Chai)
* osd: bluestore: fix several bugs (`issue#14259 <http://tracker.ceph.com/issues/14259>`_, `issue#14353 <http://tracker.ceph.com/issues/14353>`_, `issue#14260 <http://tracker.ceph.com/issues/14260>`_, `issue#14261 <http://tracker.ceph.com/issues/14261>`_, `pr#7122 <http://github.com/ceph/ceph/pull/7122>`_, xie xingguo)
* osd: bluestore: fix space rebalancing, collection split, buffered reads (`pr#7196 <http://github.com/ceph/ceph/pull/7196>`_, Sage Weil)
* osd: bluestore: more fixes (`pr#7130 <http://github.com/ceph/ceph/pull/7130>`_, Sage Weil)
* osd: cache tier: add config option for eviction check list size (`pr#6997 <http://github.com/ceph/ceph/pull/6997>`_, Yuan Zhou)
* osdc: Fix race condition with tick_event and shutdown (`issue#14256 <http://tracker.ceph.com/issues/14256>`_, `pr#7151 <http://github.com/ceph/ceph/pull/7151>`_, Adam C. Emerson)
* osd: check health state before pre_booting (`issue#14181 <http://tracker.ceph.com/issues/14181>`_, `pr#7053 <http://github.com/ceph/ceph/pull/7053>`_, Xiaoxi Chen)
* osd: clear pg_stat_queue after stopping pgs (`issue#14212 <http://tracker.ceph.com/issues/14212>`_, `pr#7091 <http://github.com/ceph/ceph/pull/7091>`_, Sage Weil)
* osd: delay populating in-memory PG log hashmaps (`pr#6425 <http://github.com/ceph/ceph/pull/6425>`_, Piotr Dałek)
* osd: disable filestore_xfs_extsize by default (`issue#14397 <http://tracker.ceph.com/issues/14397>`_, `pr#7265 <http://github.com/ceph/ceph/pull/7265>`_, Ken Dreyer)
* osd: do not keep ref of old osdmap in pg (`issue#13990 <http://tracker.ceph.com/issues/13990>`_, `pr#7007 <http://github.com/ceph/ceph/pull/7007>`_, Kefu Chai)
* osd: drop deprecated removal pg type (`pr#6970 <http://github.com/ceph/ceph/pull/6970>`_, Igor Podoski)
* osd: FileJournal: fix return code of create method (`issue#14134 <http://tracker.ceph.com/issues/14134>`_, `pr#6988 <http://github.com/ceph/ceph/pull/6988>`_, xie xingguo)
* osd: FileJournal: support batch peak and pop from writeq (`pr#6701 <http://github.com/ceph/ceph/pull/6701>`_, Xinze Chi)
* osd: FileStore: conditional collection of drive metadata (`pr#6956 <http://github.com/ceph/ceph/pull/6956>`_, Somnath Roy)
* osd: FileStore:: optimize lfn_unlink (`pr#6649 <http://github.com/ceph/ceph/pull/6649>`_, Jianpeng Ma)
* osd: fix null pointer access and race condition (`issue#14072 <http://tracker.ceph.com/issues/14072>`_, `pr#6916 <http://github.com/ceph/ceph/pull/6916>`_, xie xingguo)
* osd: fix scrub start hobject (`pr#7467 <http://github.com/ceph/ceph/pull/7467>`_, Sage Weil)
* osd: fix sparse-read result code checking logic (`issue#14151 <http://tracker.ceph.com/issues/14151>`_, `pr#7016 <http://github.com/ceph/ceph/pull/7016>`_, xie xingguo)
* osd: fix temp object removal after upgrade (`issue#13862 <http://tracker.ceph.com/issues/13862>`_, `pr#6976 <http://github.com/ceph/ceph/pull/6976>`_, David Zafman)
* osd: fix wip (l_osd_op_wip) perf counter and remove repop_map (`pr#7077 <http://github.com/ceph/ceph/pull/7077>`_, Xinze Chi)
* osd: fix wrongly placed assert and some cleanups (`pr#6766 <http://github.com/ceph/ceph/pull/6766>`_, xiexingguo, xie xingguo)
* osd: KeyValueStore: fix return code of mkfs (`pr#7036 <http://github.com/ceph/ceph/pull/7036>`_, xie xingguo)
* osd: KeyValueStore: fix wrongly placed assert (`issue#14176 <http://tracker.ceph.com/issues/14176>`_, `issue#14178 <http://tracker.ceph.com/issues/14178>`_, `pr#7047 <http://github.com/ceph/ceph/pull/7047>`_, xie xingguo)
* osd: kstore: several small fixes (`issue#14351 <http://tracker.ceph.com/issues/14351>`_, `issue#14352 <http://tracker.ceph.com/issues/14352>`_, `pr#7213 <http://github.com/ceph/ceph/pull/7213>`_, xie xingguo)
* osd: kstore: small fixes to kstore (`issue#14204 <http://tracker.ceph.com/issues/14204>`_, `pr#7095 <http://github.com/ceph/ceph/pull/7095>`_, xie xingguo)
* osd: make list_missing query missing_loc.needs_recovery_map (`pr#6298 <http://github.com/ceph/ceph/pull/6298>`_, Guang Yang)
* osdmap: remove unused local variables (`pr#6864 <http://github.com/ceph/ceph/pull/6864>`_, luo kexue)
* osd: memstore: fix two bugs (`pr#6963 <http://github.com/ceph/ceph/pull/6963>`_, Casey Bodley, Sage Weil)
* osd: misc FileStore fixes (`issue#14192 <http://tracker.ceph.com/issues/14192>`_, `issue#14188 <http://tracker.ceph.com/issues/14188>`_, `issue#14194 <http://tracker.ceph.com/issues/14194>`_, `issue#14187 <http://tracker.ceph.com/issues/14187>`_, `issue#14186 <http://tracker.ceph.com/issues/14186>`_, `pr#7059 <http://github.com/ceph/ceph/pull/7059>`_, xie xingguo)
* osd: misc optimization for map utilization (`pr#6950 <http://github.com/ceph/ceph/pull/6950>`_, Ning Yao)
* osd,mon: log leveldb and rocksdb to ceph log (`pr#6921 <http://github.com/ceph/ceph/pull/6921>`_, Sage Weil)
* osd: Omap small bugs adapted (`pr#6669 <http://github.com/ceph/ceph/pull/6669>`_, Jianpeng Ma, David Zafman)
* osd: optimize the session_handle_reset function (`issue#14182 <http://tracker.ceph.com/issues/14182>`_, `pr#7054 <http://github.com/ceph/ceph/pull/7054>`_, songbaisen)
* osd: OSDService: Fix typo in osdmap comment (`pr#7275 <http://github.com/ceph/ceph/pull/7275>`_, Brad Hubbard)
* osd: os: skip checking pg_meta object existance in FileStore (`pr#6870 <http://github.com/ceph/ceph/pull/6870>`_, Ning Yao)
* osd: PGLog: clean up read_log (`pr#7092 <http://github.com/ceph/ceph/pull/7092>`_, Jie Wang)
* osd: prevent osd_recovery_sleep from causing recovery-thread suicide (`pr#7065 <http://github.com/ceph/ceph/pull/7065>`_, Jianpeng Ma)
* osd: reduce string use in coll_t::calc_str() (`pr#6505 <http://github.com/ceph/ceph/pull/6505>`_, Igor Podoski)
* osd: release related sources when scrub is interrupted (`pr#6744 <http://github.com/ceph/ceph/pull/6744>`_, Jianpeng Ma)
* osd: remove unused OSDMap::set_weightf() (`issue#14369 <http://tracker.ceph.com/issues/14369>`_, `pr#7231 <http://github.com/ceph/ceph/pull/7231>`_, huanwen ren)
* osd: ReplicatedPG: clean up unused function (`pr#7211 <http://github.com/ceph/ceph/pull/7211>`_, Xiaowei Chen)
* osd/ReplicatedPG: fix promotion recency logic (`issue#14320 <http://tracker.ceph.com/issues/14320>`_, `pr#6702 <http://github.com/ceph/ceph/pull/6702>`_, Sage Weil)
* osd: several small cleanups (`pr#7055 <http://github.com/ceph/ceph/pull/7055>`_, xie xingguo)
* osd: shut down if we flap too many times in a short period (`pr#6708 <http://github.com/ceph/ceph/pull/6708>`_, Xiaoxi Chen)
* osd: skip promote for writefull w/ FADVISE_DONTNEED/NOCACHE (`pr#7010 <http://github.com/ceph/ceph/pull/7010>`_, Jianpeng Ma)
* osd: small fixes to memstore (`issue#14228 <http://tracker.ceph.com/issues/14228>`_, `issue#14229 <http://tracker.ceph.com/issues/14229>`_, `issue#14227 <http://tracker.ceph.com/issues/14227>`_, `pr#7107 <http://github.com/ceph/ceph/pull/7107>`_, xie xingguo)
* osd: try evicting after flushing is done (`pr#5630 <http://github.com/ceph/ceph/pull/5630>`_, Zhiqiang Wang)
* osd: use atomic to generate ceph_tid (`pr#7017 <http://github.com/ceph/ceph/pull/7017>`_, Evgeniy Firsov)
* osd: use optimized is_zero in object_stat_sum_t.is_zero() (`pr#7203 <http://github.com/ceph/ceph/pull/7203>`_, Piotr Dałek)
* osd: utime_t, eversion_t, osd_stat_sum_t encoding optimization (`pr#6902 <http://github.com/ceph/ceph/pull/6902>`_, Xinze Chi)
* pybind: add ceph_volume_client interface for Manila and similar frameworks (`pr#6205 <http://github.com/ceph/ceph/pull/6205>`_, John Spray)
* pybind: fix build failure, remove extraneous semicolon in method (`issue#14371 <http://tracker.ceph.com/issues/14371>`_, `pr#7235 <http://github.com/ceph/ceph/pull/7235>`_, Abhishek Lekshmanan)
* pybind/test_rbd: fix test_create_defaults (`issue#14279 <http://tracker.ceph.com/issues/14279>`_, `pr#7155 <http://github.com/ceph/ceph/pull/7155>`_, Josh Durgin)
* qa: disable rbd/qemu-iotests test case 055 on RHEL/CentOSlibrbd: journal replay should honor inter-event dependencies (`issue#14385 <http://tracker.ceph.com/issues/14385>`_, `pr#7272 <http://github.com/ceph/ceph/pull/7272>`_, Jason Dillaman)
* qa/workunits: merge_diff shouldn't attempt to use striping (`issue#14165 <http://tracker.ceph.com/issues/14165>`_, `pr#7041 <http://github.com/ceph/ceph/pull/7041>`_, Jason Dillaman)
* qa/workunits/snaps: move snap tests into fs sub-directory (`pr#6496 <http://github.com/ceph/ceph/pull/6496>`_, Yan, Zheng)
* rados: implement rm --force option to force remove when full (`pr#6202 <http://github.com/ceph/ceph/pull/6202>`_, Xiaowei Chen)
* rbd: additional validation for striping parameters (`pr#6914 <http://github.com/ceph/ceph/pull/6914>`_, Na Xie)
* rbd: add pool name to disambiguate rbd admin socket commands (`pr#6904 <http://github.com/ceph/ceph/pull/6904>`_, wuxiangwei)
* rbd: correct an output string for merge-diff (`pr#7046 <http://github.com/ceph/ceph/pull/7046>`_, Kongming Wu)
* rbd: fix static initialization ordering issues (`pr#6978 <http://github.com/ceph/ceph/pull/6978>`_, Mykola Golub)
* rbd-fuse: image name can not include snap name (`pr#7044 <http://github.com/ceph/ceph/pull/7044>`_, Yongqiang He)
* rbd-fuse: implement mv operation (`pr#6938 <http://github.com/ceph/ceph/pull/6938>`_, wuxiangwei)
* rbd: must specify both of stripe-unit and stripe-count when specifying stripingv2 feature (`pr#7026 <http://github.com/ceph/ceph/pull/7026>`_, Donghai Xu)
* rbd-nbd: add copyright (`pr#7166 <http://github.com/ceph/ceph/pull/7166>`_, Li Wang)
* rbd-nbd: fix up return code handling (`pr#7215 <http://github.com/ceph/ceph/pull/7215>`_, Mykola Golub)
* rbd-nbd: small improvements in logging and forking (`pr#7127 <http://github.com/ceph/ceph/pull/7127>`_, Mykola Golub)
* rbd: rbd order will be place in 22, when set to 0 in the config_opt (`issue#14139 <http://tracker.ceph.com/issues/14139>`_, `issue#14047 <http://tracker.ceph.com/issues/14047>`_, `pr#6886 <http://github.com/ceph/ceph/pull/6886>`_, huanwen ren)
* rbd: striping parameters should support 64bit integers (`pr#6942 <http://github.com/ceph/ceph/pull/6942>`_, Na Xie)
* rbd: use default order from configuration when not specified (`pr#6965 <http://github.com/ceph/ceph/pull/6965>`_, Yunchuan Wen)
* rgw: add a method to purge all associate keys when removing a subuser (`issue#12890 <http://tracker.ceph.com/issues/12890>`_, `pr#6002 <http://github.com/ceph/ceph/pull/6002>`_, Sangdi Xu)
* rgw: add missing error code for admin op API (`pr#7037 <http://github.com/ceph/ceph/pull/7037>`_, Dunrong Huang)
* rgw: add support for "end_marker" parameter for GET on Swift account. (`issue#10682 <http://tracker.ceph.com/issues/10682>`_, `pr#4216 <http://github.com/ceph/ceph/pull/4216>`_, Radoslaw Zarzynski)
* rgw_admin: orphans finish segfaults (`pr#6652 <http://github.com/ceph/ceph/pull/6652>`_, Igor Fedotov)
* rgw: content length (`issue#13582 <http://tracker.ceph.com/issues/13582>`_, `pr#6975 <http://github.com/ceph/ceph/pull/6975>`_, Yehuda Sadeh)
* rgw: delete default zone (`pr#7005 <http://github.com/ceph/ceph/pull/7005>`_, YankunLi)
* rgw: do not abort radowgw server when using admin op API with bad parameters  (`issue#14190 <http://tracker.ceph.com/issues/14190>`_, `issue#14191 <http://tracker.ceph.com/issues/14191>`_, `pr#7063 <http://github.com/ceph/ceph/pull/7063>`_, Dunrong Huang)
* rgw: Drop a debugging message (`pr#7280 <http://github.com/ceph/ceph/pull/7280>`_, Pete Zaitcev)
* rgw: fix a typo in init-radosgw (`pr#6817 <http://github.com/ceph/ceph/pull/6817>`_, Zhi Zhang)
* rgw: fix compilation warning (`pr#7160 <http://github.com/ceph/ceph/pull/7160>`_, Yehuda Sadeh)
* rgw: fix wrong check for parse() return (`pr#6797 <http://github.com/ceph/ceph/pull/6797>`_, Dunrong Huang)
* rgw: let radosgw-admin bucket stats return a standard josn (`pr#7029 <http://github.com/ceph/ceph/pull/7029>`_, Ruifeng Yang)
* rgw: modify command stucking when operating radosgw-admin metadata list user (`pr#7032 <http://github.com/ceph/ceph/pull/7032>`_, Peiyang Liu)
* rgw: modify documents and help infos' descriptions to the usage of option date when executing command "log show" (`pr#6080 <http://github.com/ceph/ceph/pull/6080>`_, Kongming Wu)
* rgw: Parse --subuser better (`pr#7279 <http://github.com/ceph/ceph/pull/7279>`_, Pete Zaitcev)
* rgw: radosgw-admin bucket check --fix not work (`pr#7093 <http://github.com/ceph/ceph/pull/7093>`_, Weijun Duan)
* rgw: warn on suspicious civetweb frontend parameters (`pr#6944 <http://github.com/ceph/ceph/pull/6944>`_, Matt Benjamin)
* rocksdb: remove rdb sources from dist tarball (`issue#13554 <http://tracker.ceph.com/issues/13554>`_, `pr#7105 <http://github.com/ceph/ceph/pull/7105>`_, Venky Shankar)
* stringify outputted error code and fix unmatched parentheses. (`pr#6998 <http://github.com/ceph/ceph/pull/6998>`_, xie.xingguo, xie xingguo)
* test/librbd/fsx: Use c++11 std::mt19937 generator instead of random_r() (`pr#6332 <http://github.com/ceph/ceph/pull/6332>`_, John Coyle)
* test/mon/osd-erasure-code-profile: pick new mon port (`pr#7161 <http://github.com/ceph/ceph/pull/7161>`_, Sage Weil)
* tests: add const for ec test (`pr#6911 <http://github.com/ceph/ceph/pull/6911>`_, Michal Jarzabek)
* tests: configure with rocksdb by default (`issue#14220 <http://tracker.ceph.com/issues/14220>`_, `pr#7100 <http://github.com/ceph/ceph/pull/7100>`_, Loic Dachary)
* tests: Fix for make check. (`pr#7102 <http://github.com/ceph/ceph/pull/7102>`_, David Zafman)
* tests: notification slave needs to wait for master (`issue#13810 <http://tracker.ceph.com/issues/13810>`_, `pr#7220 <http://github.com/ceph/ceph/pull/7220>`_, Jason Dillaman)
* tests: snap rename and rebuild object map in client update test (`pr#7224 <http://github.com/ceph/ceph/pull/7224>`_, Jason Dillaman)
* tests: unittest_bufferlist: fix hexdump test (`pr#7152 <http://github.com/ceph/ceph/pull/7152>`_, Sage Weil)
* tests: unittest_ipaddr: fix segv (`pr#7154 <http://github.com/ceph/ceph/pull/7154>`_, Sage Weil)
* tools: ceph_monstore_tool: add inflate-pgmap command (`issue#14217 <http://tracker.ceph.com/issues/14217>`_, `pr#7097 <http://github.com/ceph/ceph/pull/7097>`_, Kefu Chai)
* tools: monstore: add 'show-versions' command. (`pr#7073 <http://github.com/ceph/ceph/pull/7073>`_, Cilang Zhao)
* admin/build-doc: depend on zlib1g-dev and graphviz (`pr#7522 <http://github.com/ceph/ceph/pull/7522>`_, Ken Dreyer)
* buffer: use move construct to append/push_back/push_front (`pr#7455 <http://github.com/ceph/ceph/pull/7455>`_, Haomai Wang)
* build: allow jemalloc with rocksdb-static (`pr#7368 <http://github.com/ceph/ceph/pull/7368>`_, Somnath Roy)
* build: fix the autotools and cmake build (the new fusestore needs libfuse) (`pr#7393 <http://github.com/ceph/ceph/pull/7393>`_, Kefu Chai)
* build: fix warnings (`pr#7197 <http://github.com/ceph/ceph/pull/7197>`_, Kefu Chai, xie xingguo)
* build: fix warnings (`pr#7315 <http://github.com/ceph/ceph/pull/7315>`_, Kefu Chai)
* build: kill warnings (`pr#7397 <http://github.com/ceph/ceph/pull/7397>`_, Kefu Chai)
* build: move libexec scripts to standardize across distros (`issue#14687 <http://tracker.ceph.com/issues/14687>`_, `issue#14705 <http://tracker.ceph.com/issues/14705>`_, `issue#14723 <http://tracker.ceph.com/issues/14723>`_, `pr#7636 <http://github.com/ceph/ceph/pull/7636>`_, Nathan Cutler, Kefu Chai)
* build: Refrain from versioning and packaging EC testing plugins (`issue#14756 <http://tracker.ceph.com/issues/14756>`_, `issue#14723 <http://tracker.ceph.com/issues/14723>`_, `pr#7637 <http://github.com/ceph/ceph/pull/7637>`_, Nathan Cutler, Kefu Chai)
* build: spdk submodule; cmake (`pr#7503 <http://github.com/ceph/ceph/pull/7503>`_, Kefu Chai)
* ceph-disk: support bluestore (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7218 <http://github.com/ceph/ceph/pull/7218>`_, Loic Dachary, Sage Weil)
* ceph-disk/test: fix test_prepare.py::TestPrepare tests (`pr#7549 <http://github.com/ceph/ceph/pull/7549>`_, Kefu Chai)
* cleanup: remove misc dead code (`pr#7201 <http://github.com/ceph/ceph/pull/7201>`_, Erwan Velu)
* cls/cls_rbd: pass string by reference (`pr#7232 <http://github.com/ceph/ceph/pull/7232>`_, Jeffrey Lu)
* cmake: Added new unittests to make check (`pr#7572 <http://github.com/ceph/ceph/pull/7572>`_, Ali Maredia)
* cmake: add KernelDevice.cc to libos_srcs (`pr#7507 <http://github.com/ceph/ceph/pull/7507>`_, Kefu Chai)
* cmake: check for libsnappy in default path also (`pr#7366 <http://github.com/ceph/ceph/pull/7366>`_, Kefu Chai)
* cmake: feb5 (`pr#7541 <http://github.com/ceph/ceph/pull/7541>`_, Matt Benjamin)
* cmake: For CMake version <= 2.8.11, use LINK_PRIVATE and LINK_PUBLIC (`pr#7474 <http://github.com/ceph/ceph/pull/7474>`_, Tao Chang)
* cmake: let ceph-client-debug link with tcmalloc (`pr#7314 <http://github.com/ceph/ceph/pull/7314>`_, Kefu Chai)
* cmake: support ccache via a WITH_CCACHE build option (`pr#6875 <http://github.com/ceph/ceph/pull/6875>`_, John Coyle)
* common: add zlib compression plugin (`pr#7437 <http://github.com/ceph/ceph/pull/7437>`_, Alyona Kiseleva, Kiseleva Alyona)
* common: admin socket commands for tcmalloc heap get/set operations (`pr#7512 <http://github.com/ceph/ceph/pull/7512>`_, Samuel Just)
* common: ake ceph_time clocks work under BSD (`pr#7340 <http://github.com/ceph/ceph/pull/7340>`_, Adam C. Emerson)
* common: Allow OPT_INT settings with negative values (`issue#13829 <http://tracker.ceph.com/issues/13829>`_, `pr#7390 <http://github.com/ceph/ceph/pull/7390>`_, Brad Hubbard, Kefu Chai)
* common/buffer: replace RWLock with spinlocks (`pr#7294 <http://github.com/ceph/ceph/pull/7294>`_, Piotr Dałek)
* common: change the type of counter total/unhealthy_workers (`pr#7254 <http://github.com/ceph/ceph/pull/7254>`_, Guang Yang)
* common: snappy decompressor may assert when handling segmented input bufferlist (`issue#14400 <http://tracker.ceph.com/issues/14400>`_, `pr#7268 <http://github.com/ceph/ceph/pull/7268>`_, Igor Fedotov)
* common/str_map: cleanup: replaced get_str_map() function overloading by using default parameters for delimiters (`pr#7266 <http://github.com/ceph/ceph/pull/7266>`_, Sahithi R V)
* common: time: have skewing-now call non-skewing now (`pr#7466 <http://github.com/ceph/ceph/pull/7466>`_, Adam C. Emerson)
* common: unit test for interval_set implementations (`pr#6 <http://github.com/ceph/ceph/pull/6>`_, Igor Fedotov)
* config: add $data_dir/config to config search path (`pr#7377 <http://github.com/ceph/ceph/pull/7377>`_, Sage Weil)
* configure.ac: make "--with-librocksdb-static" default to 'check' (`issue#14463 <http://tracker.ceph.com/issues/14463>`_, `pr#7317 <http://github.com/ceph/ceph/pull/7317>`_, Dan Mick)
* crush: add safety assert (`issue#14496 <http://tracker.ceph.com/issues/14496>`_, `pr#7344 <http://github.com/ceph/ceph/pull/7344>`_, songbaisen)
* crush: reply quickly from get_immediate_parent (`issue#14334 <http://tracker.ceph.com/issues/14334>`_, `pr#7181 <http://github.com/ceph/ceph/pull/7181>`_, song baisen)
* debian: packaging fixes for jewel (`pr#7807 <http://github.com/ceph/ceph/pull/7807>`_, Ken Dreyer, Ali Maredia)
* debian/rpm split servers (`issue#10587 <http://tracker.ceph.com/issues/10587>`_, `pr#7746 <http://github.com/ceph/ceph/pull/7746>`_, Ken Dreyer)
* doc: add orphans commands to radosgw-admin(8) (`issue#14637 <http://tracker.ceph.com/issues/14637>`_, `pr#7518 <http://github.com/ceph/ceph/pull/7518>`_, Ken Dreyer)
* doc: amend the rados.8 (`pr#7251 <http://github.com/ceph/ceph/pull/7251>`_, Kefu Chai)
* doc: Fixes a CRUSH map step take argument (`pr#7327 <http://github.com/ceph/ceph/pull/7327>`_, Ivan Grcic)
* doc: fixing image in section ERASURE CODING (`pr#7298 <http://github.com/ceph/ceph/pull/7298>`_, Rachana Patel)
* doc: fix misleading configuration guide on cache tiering (`pr#7000 <http://github.com/ceph/ceph/pull/7000>`_, Yuan Zhou)
* doc: fix S3 C# example (`pr#7027 <http://github.com/ceph/ceph/pull/7027>`_, Dunrong Huang)
* doc: remove redundant space in ceph-authtool/monmaptool doc (`pr#7244 <http://github.com/ceph/ceph/pull/7244>`_, Jiaying Ren)
* doc: revise SubmittingPatches (`pr#7292 <http://github.com/ceph/ceph/pull/7292>`_, Kefu Chai)
* doc: rgw: port changes from downstream to upstream (`pr#7264 <http://github.com/ceph/ceph/pull/7264>`_, Bara Ancincova)
* doc: script and guidelines for mirroring Ceph (`pr#7384 <http://github.com/ceph/ceph/pull/7384>`_, Wido den Hollander)
* doc: use 'ceph auth get-or-create' for creating RGW keyring (`pr#6930 <http://github.com/ceph/ceph/pull/6930>`_, Wido den Hollander)
* global: do not start two daemons with a single pid-file (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7075 <http://github.com/ceph/ceph/pull/7075>`_, shun song)
* global: do not start two daemons with a single pid-file (part 2) (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7463 <http://github.com/ceph/ceph/pull/7463>`_, Loic Dachary)
* journal: flush commit position on metadata shutdown (`pr#7385 <http://github.com/ceph/ceph/pull/7385>`_, Mykola Golub)
* journal: reset commit_position_task_ctx pointer after task complete (`pr#7480 <http://github.com/ceph/ceph/pull/7480>`_, Mykola Golub)
* libcephfs: update LIBCEPHFS_VERSION to indicate the interface was changed (`pr#7551 <http://github.com/ceph/ceph/pull/7551>`_, Jevon Qiao)
* librados: move to c++11 concurrency types (`pr#5931 <http://github.com/ceph/ceph/pull/5931>`_, Adam C. Emerson)
* librados: remove duplicate definitions for rados pool_stat_t and cluster_stat_t (`pr#7330 <http://github.com/ceph/ceph/pull/7330>`_, Igor Fedotov)
* librados: shutdown finisher in a more graceful way (`pr#7519 <http://github.com/ceph/ceph/pull/7519>`_, xie xingguo)
* librados_test_stub: protect against notify/unwatch race (`pr#7540 <http://github.com/ceph/ceph/pull/7540>`_, Jason Dillaman)
* librbd: API: async open and close (`issue#14264 <http://tracker.ceph.com/issues/14264>`_, `pr#7259 <http://github.com/ceph/ceph/pull/7259>`_, Mykola Golub)
* librbd: Avoid create two threads per image (`pr#7400 <http://github.com/ceph/ceph/pull/7400>`_, Haomai Wang)
* librbd: block maintenance ops until after journal is ready (`issue#14510 <http://tracker.ceph.com/issues/14510>`_, `pr#7382 <http://github.com/ceph/ceph/pull/7382>`_, Jason Dillaman)
* librbd: fix internal handling of dynamic feature updates (`pr#7299 <http://github.com/ceph/ceph/pull/7299>`_, Jason Dillaman)
* librbd: journal framework for tracking exclusive lock transitions (`issue#13298 <http://tracker.ceph.com/issues/13298>`_, `pr#7529 <http://github.com/ceph/ceph/pull/7529>`_, Jason Dillaman)
* librbd: journal shut down flush race condition (`issue#14434 <http://tracker.ceph.com/issues/14434>`_, `pr#7302 <http://github.com/ceph/ceph/pull/7302>`_, Jason Dillaman)
* librbd: remove canceled tasks from timer thread (`issue#14476 <http://tracker.ceph.com/issues/14476>`_, `pr#7329 <http://github.com/ceph/ceph/pull/7329>`_, Douglas Fuller)
* makefile: remove libedit from libclient.la (`pr#7284 <http://github.com/ceph/ceph/pull/7284>`_, Kefu Chai)
* mds, client: fix locking around handle_conf_change (`issue#14365 <http://tracker.ceph.com/issues/14365>`_, `issue#14374 <http://tracker.ceph.com/issues/14374>`_, `pr#7312 <http://github.com/ceph/ceph/pull/7312>`_, John Spray)
* mds: judgment added to avoid the risk of visiting the NULL pointer (`pr#7358 <http://github.com/ceph/ceph/pull/7358>`_, Kongming Wu)
* mon: add an independent option for max election time (`pr#7245 <http://github.com/ceph/ceph/pull/7245>`_, Sangdi Xu)
* mon: compact full epochs also (`issue#14537 <http://tracker.ceph.com/issues/14537>`_, `pr#7396 <http://github.com/ceph/ceph/pull/7396>`_, Kefu Chai)
* mon: consider the pool size when setting pool crush rule (`issue#14495 <http://tracker.ceph.com/issues/14495>`_, `pr#7341 <http://github.com/ceph/ceph/pull/7341>`_, song baisen)
* mon: drop useless rank init assignment (`issue#14508 <http://tracker.ceph.com/issues/14508>`_, `pr#7321 <http://github.com/ceph/ceph/pull/7321>`_, huanwen ren)
* mon: fix locking in preinit error paths (`issue#14473 <http://tracker.ceph.com/issues/14473>`_, `pr#7353 <http://github.com/ceph/ceph/pull/7353>`_, huanwen ren)
* mon: fix monmap creation stamp (`pr#7459 <http://github.com/ceph/ceph/pull/7459>`_, duanweijun)
* mon: fix sync of config-key data (`pr#7363 <http://github.com/ceph/ceph/pull/7363>`_, Xiaowei Chen)
* mon: go into ERR state if multiple PGs are stuck inactive (`issue#13923 <http://tracker.ceph.com/issues/13923>`_, `pr#7253 <http://github.com/ceph/ceph/pull/7253>`_, Wido den Hollander)
* mon/MDSMonitor.cc: properly note beacon when health metrics changes (`issue#14684 <http://tracker.ceph.com/issues/14684>`_, `pr#7757 <http://github.com/ceph/ceph/pull/7757>`_, Yan, Zheng)
* mon/MonClient: avoid null pointer error when configured incorrectly (`issue#14405 <http://tracker.ceph.com/issues/14405>`_, `pr#7276 <http://github.com/ceph/ceph/pull/7276>`_, Bo Cai)
* mon: PG Monitor should report waiting for backfill (`issue#12744 <http://tracker.ceph.com/issues/12744>`_, `pr#7398 <http://github.com/ceph/ceph/pull/7398>`_, Abhishek Lekshmanan)
* mon: reduce CPU and memory manager pressure of pg health check (`pr#7482 <http://github.com/ceph/ceph/pull/7482>`_, Piotr Dałek)
* mon: some cleanup in MonmapMonitor.cc (`pr#7418 <http://github.com/ceph/ceph/pull/7418>`_, huanwen ren)
* mon: warn if pg(s) not scrubbed (`issue#13142 <http://tracker.ceph.com/issues/13142>`_, `pr#6440 <http://github.com/ceph/ceph/pull/6440>`_, Michal Jarzabek)
* msg/async: AsyncConnection: avoid debug log in cleanup_handler (`pr#7547 <http://github.com/ceph/ceph/pull/7547>`_, Haomai Wang)
* msg/async: bunch of fixes (`pr#7379 <http://github.com/ceph/ceph/pull/7379>`_, Piotr Dałek)
* msg/async: fix array boundary (`pr#7451 <http://github.com/ceph/ceph/pull/7451>`_, Wei Jin)
* msg/async: fix potential race condition (`pr#7453 <http://github.com/ceph/ceph/pull/7453>`_, Haomai Wang)
* msg/async: fix send closed local_connection message problem (`pr#7255 <http://github.com/ceph/ceph/pull/7255>`_, Haomai Wang)
* msg/async: reduce extra tcp packet for message ack (`pr#7380 <http://github.com/ceph/ceph/pull/7380>`_, Haomai Wang)
* msg/xio: fix compilation (`pr#7479 <http://github.com/ceph/ceph/pull/7479>`_, Roi Dayan)
* organizationmap: modify org mail info. (`pr#7240 <http://github.com/ceph/ceph/pull/7240>`_, Xiaowei Chen)
* os/bluestore: fix assert (`issue#14436 <http://tracker.ceph.com/issues/14436>`_, `pr#7293 <http://github.com/ceph/ceph/pull/7293>`_, xie xingguo)
* os/bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7342 <http://github.com/ceph/ceph/pull/7342>`_, Kefu Chai)
* os/bluestore: insert new onode to the front position of onode LRU (`pr#7492 <http://github.com/ceph/ceph/pull/7492>`_, Jianjian Huo)
* os/bluestore: use intrusive_ptr for Dir (`pr#7247 <http://github.com/ceph/ceph/pull/7247>`_, Igor Fedotov)
* osd: blockdevice: avoid implicit cast and add guard (`pr#7460 <http://github.com/ceph/ceph/pull/7460>`_, xie xingguo)
* osd: bluestore/BlueFS: initialize super block_size earlier in mkfs (`pr#7535 <http://github.com/ceph/ceph/pull/7535>`_, Sage Weil)
* osd: BlueStore: fix fsck and blockdevice read-relevant issue (`pr#7362 <http://github.com/ceph/ceph/pull/7362>`_, xie xingguo)
* osd: BlueStore: fix null pointer access (`issue#14561 <http://tracker.ceph.com/issues/14561>`_, `pr#7435 <http://github.com/ceph/ceph/pull/7435>`_, xie xingguo)
* osd: bluestore, kstore: fix nid overwritten logic (`issue#14407 <http://tracker.ceph.com/issues/14407>`_, `issue#14433 <http://tracker.ceph.com/issues/14433>`_, `pr#7283 <http://github.com/ceph/ceph/pull/7283>`_, xie xingguo)
* osd: bluestore: use btree_map for allocator (`pr#7269 <http://github.com/ceph/ceph/pull/7269>`_, Igor Fedotov, Sage Weil)
* osd: drop fiemap len=0 logic (`pr#7267 <http://github.com/ceph/ceph/pull/7267>`_, Sage Weil)
* osd: FileStore: add error check for object_map->sync() (`pr#7281 <http://github.com/ceph/ceph/pull/7281>`_, Chendi Xue)
* osd: FileStore: cleanup: remove obsolete option "filestore_xattr_use_omap" (`issue#14356 <http://tracker.ceph.com/issues/14356>`_, `pr#7217 <http://github.com/ceph/ceph/pull/7217>`_, Vikhyat Umrao)
* osd: FileStore: modify the format of colon (`pr#7333 <http://github.com/ceph/ceph/pull/7333>`_, Donghai Xu)
* osd: FileStore: print file name before osd assert if read file failed (`pr#7111 <http://github.com/ceph/ceph/pull/7111>`_, Ji Chen)
* osd: fix invalid list traversal in process_copy_chunk (`pr#7511 <http://github.com/ceph/ceph/pull/7511>`_, Samuel Just)
* osd, mon: fix exit issue (`pr#7420 <http://github.com/ceph/ceph/pull/7420>`_, Jiaying Ren)
* osd: PG::activate(): handle unexpected cached_removed_snaps more gracefully (`issue#14428 <http://tracker.ceph.com/issues/14428>`_, `pr#7309 <http://github.com/ceph/ceph/pull/7309>`_, Alexey Sheplyakov)
* os/fs: fix io_getevents argument (`pr#7355 <http://github.com/ceph/ceph/pull/7355>`_, Jingkai Yuan)
* os/fusestore: add error handling (`pr#7395 <http://github.com/ceph/ceph/pull/7395>`_, xie xingguo)
* os/keyvaluestore: kill KeyValueStore (`pr#7320 <http://github.com/ceph/ceph/pull/7320>`_, Haomai Wang)
* os/kstore: insert new onode to the front position of onode LRU (`pr#7505 <http://github.com/ceph/ceph/pull/7505>`_, xie xingguo)
* os/ObjectStore: add custom move operations for ObjectStore::Transaction (`pr#7303 <http://github.com/ceph/ceph/pull/7303>`_, Casey Bodley)
* rgw: Bug fix for mtime anomalies in RadosGW and other places (`pr#7328 <http://github.com/ceph/ceph/pull/7328>`_, Adam C. Emerson, Casey Bodley)
* rpm: move %post(un) ldconfig calls to ceph-base (`issue#14940 <http://tracker.ceph.com/issues/14940>`_, `pr#7867 <http://github.com/ceph/ceph/pull/7867>`_, Nathan Cutler)
* rpm: move runtime dependencies to ceph-base and fix other packaging issues (`issue#14864 <http://tracker.ceph.com/issues/14864>`_, `pr#7826 <http://github.com/ceph/ceph/pull/7826>`_, Nathan Cutler)
* test: ceph_test_rados: use less CPU (`pr#7513 <http://github.com/ceph/ceph/pull/7513>`_, Samuel Just)
* ceph-disk: get Nonetype when ceph-disk list with --format plain on single device. (`pr#6410 <http://github.com/ceph/ceph/pull/6410>`_, Vicente Cheng)
* ceph: fix tell behavior (`pr#6329 <http://github.com/ceph/ceph/pull/6329>`_, David Zafman)
* ceph-fuse: While starting ceph-fuse, start the log thread first (`issue#13443 <http://tracker.ceph.com/issues/13443>`_, `pr#6224 <http://github.com/ceph/ceph/pull/6224>`_, Wenjun Huang)
* client: don't mark_down on command reply (`pr#6204 <http://github.com/ceph/ceph/pull/6204>`_, John Spray)
* client: drop prefix from ints (`pr#6275 <http://github.com/ceph/ceph/pull/6275>`_, John Coyle)
* client: sys/file.h includes for flock operations (`pr#6282 <http://github.com/ceph/ceph/pull/6282>`_, John Coyle)
* cls_rbd: change object_map_update to return 0 on success, add logging (`pr#6467 <http://github.com/ceph/ceph/pull/6467>`_, Douglas Fuller)
* cmake: Use uname instead of arch. (`pr#6358 <http://github.com/ceph/ceph/pull/6358>`_, John Coyle)
* common: assert: __STRING macro is not defined by musl libc. (`pr#6210 <http://github.com/ceph/ceph/pull/6210>`_, John Coyle)
* common: fix OpTracker age histogram calculation (`pr#5065 <http://github.com/ceph/ceph/pull/5065>`_, Zhiqiang Wang)
* common/MemoryModel: Added explicit feature check for mallinfo(). (`pr#6252 <http://github.com/ceph/ceph/pull/6252>`_, John Coyle)
* common/obj_bencher.cc: fix verification crashing when there's no objects (`pr#5853 <http://github.com/ceph/ceph/pull/5853>`_, Piotr Dałek)
* common: optimize debug logging (`pr#6307 <http://github.com/ceph/ceph/pull/6307>`_, Adam Kupczyk)
* common: Thread: move copy constructor and assignment op (`pr#5133 <http://github.com/ceph/ceph/pull/5133>`_, Michal Jarzabek)
* common: WorkQueue: new PointerWQ base class for ContextWQ (`issue#13636 <http://tracker.ceph.com/issues/13636>`_, `pr#6525 <http://github.com/ceph/ceph/pull/6525>`_, Jason Dillaman)
* compat: use prefixed typeof extension (`pr#6216 <http://github.com/ceph/ceph/pull/6216>`_, John Coyle)
* crush: validate bucket id before indexing buckets array (`issue#13477 <http://tracker.ceph.com/issues/13477>`_, `pr#6246 <http://github.com/ceph/ceph/pull/6246>`_, Sage Weil)
* doc: download GPG key from download.ceph.com (`issue#13603 <http://tracker.ceph.com/issues/13603>`_, `pr#6384 <http://github.com/ceph/ceph/pull/6384>`_, Ken Dreyer)
* doc: fix outdated content in cache tier (`pr#6272 <http://github.com/ceph/ceph/pull/6272>`_, Yuan Zhou)
* doc/release-notes: v9.1.0 (`pr#6281 <http://github.com/ceph/ceph/pull/6281>`_, Loic Dachary)
* doc/releases-notes: fix build error (`pr#6483 <http://github.com/ceph/ceph/pull/6483>`_, Kefu Chai)
* doc: remove toctree items under Create CephFS (`pr#6241 <http://github.com/ceph/ceph/pull/6241>`_, Jevon Qiao)
* doc: rename the "Create a Ceph User" section and add verbage about… (`issue#13502 <http://tracker.ceph.com/issues/13502>`_, `pr#6297 <http://github.com/ceph/ceph/pull/6297>`_, ritz303)
* docs: Fix styling of newly added mirror docs (`pr#6127 <http://github.com/ceph/ceph/pull/6127>`_, Wido den Hollander)
* doc, tests: update all http://ceph.com/ to download.ceph.com (`pr#6435 <http://github.com/ceph/ceph/pull/6435>`_, Alfredo Deza)
* doc: update doc for with new pool settings (`pr#5951 <http://github.com/ceph/ceph/pull/5951>`_, Guang Yang)
* doc: update radosgw-admin example (`pr#6256 <http://github.com/ceph/ceph/pull/6256>`_, YankunLi)
* doc: update the OS recommendations for newer Ceph releases (`pr#6355 <http://github.com/ceph/ceph/pull/6355>`_, ritz303)
* drop envz.h includes (`pr#6285 <http://github.com/ceph/ceph/pull/6285>`_, John Coyle)
* libcephfs: Improve portability by replacing loff_t type usage with off_t (`pr#6301 <http://github.com/ceph/ceph/pull/6301>`_, John Coyle)
* libcephfs: only check file offset on glibc platforms (`pr#6288 <http://github.com/ceph/ceph/pull/6288>`_, John Coyle)
* librados: fix examples/librados/Makefile error. (`pr#6320 <http://github.com/ceph/ceph/pull/6320>`_, You Ji)
* librados: init crush_location from config file. (`issue#13473 <http://tracker.ceph.com/issues/13473>`_, `pr#6243 <http://github.com/ceph/ceph/pull/6243>`_, Wei Luo)
* librados: wrongly passed in argument for stat command (`issue#13703 <http://tracker.ceph.com/issues/13703>`_, `pr#6476 <http://github.com/ceph/ceph/pull/6476>`_, xie xingguo)
* librbd: deadlock while attempting to flush AIO requests (`issue#13726 <http://tracker.ceph.com/issues/13726>`_, `pr#6508 <http://github.com/ceph/ceph/pull/6508>`_, Jason Dillaman)
* librbd: fix enable objectmap feature issue (`issue#13558 <http://tracker.ceph.com/issues/13558>`_, `pr#6339 <http://github.com/ceph/ceph/pull/6339>`_, xinxin shu)
* librbd: remove duplicate read_only test in librbd::async_flatten (`pr#5856 <http://github.com/ceph/ceph/pull/5856>`_, runsisi)
* mailmap: modify member info  (`pr#6468 <http://github.com/ceph/ceph/pull/6468>`_, Xiaowei Chen)
* mailmap: updates (`pr#6258 <http://github.com/ceph/ceph/pull/6258>`_, M Ranga Swami Reddy)
* mailmap: Xie Xingguo affiliation (`pr#6409 <http://github.com/ceph/ceph/pull/6409>`_, Loic Dachary)
* mds: implement snapshot rename (`pr#5645 <http://github.com/ceph/ceph/pull/5645>`_, xinxin shu)
* mds: messages/MOSDOp: cast in assert to eliminate warnings (`issue#13625 <http://tracker.ceph.com/issues/13625>`_, `pr#6414 <http://github.com/ceph/ceph/pull/6414>`_, David Zafman)
* mds: new filtered MDS tell commands for sessions (`pr#6180 <http://github.com/ceph/ceph/pull/6180>`_, John Spray)
* mds/Session: use projected parent for auth path check (`issue#13364 <http://tracker.ceph.com/issues/13364>`_, `pr#6200 <http://github.com/ceph/ceph/pull/6200>`_, Sage Weil)
* mon: should not set isvalid = true when cephx_verify_authorizer return false (`issue#13525 <http://tracker.ceph.com/issues/13525>`_, `pr#6306 <http://github.com/ceph/ceph/pull/6306>`_, Ruifeng Yang)
* osd: Add config option osd_read_ec_check_for_errors for testing (`pr#5865 <http://github.com/ceph/ceph/pull/5865>`_, David Zafman)
* osd: add pin/unpin support to cache tier (11066) (`pr#6326 <http://github.com/ceph/ceph/pull/6326>`_, Zhiqiang Wang)
* osd: auto repair EC pool (`issue#12754 <http://tracker.ceph.com/issues/12754>`_, `pr#6196 <http://github.com/ceph/ceph/pull/6196>`_, Guang Yang)
* osd: drop the interim set from load_pgs() (`pr#6277 <http://github.com/ceph/ceph/pull/6277>`_, Piotr Dałek)
* osd: FileJournal: _fdump wrongly returns if journal is currently unreadable. (`issue#13626 <http://tracker.ceph.com/issues/13626>`_, `pr#6406 <http://github.com/ceph/ceph/pull/6406>`_, xie xingguo)
* osd: FileStore: add a field indicate xattr only one chunk for set xattr. (`pr#6244 <http://github.com/ceph/ceph/pull/6244>`_, Jianpeng Ma)
* osd: FileStore: LFNIndex: remove redundant local variable 'obj'. (`issue#13552 <http://tracker.ceph.com/issues/13552>`_, `pr#6333 <http://github.com/ceph/ceph/pull/6333>`_, xiexingguo)
* osd: FileStore: potential memory leak if _fgetattrs fails (`issue#13597 <http://tracker.ceph.com/issues/13597>`_, `pr#6377 <http://github.com/ceph/ceph/pull/6377>`_, xie xingguo)
* osd: FileStore: remove unused local variable 'handle' (`pr#6381 <http://github.com/ceph/ceph/pull/6381>`_, xie xingguo)
* osd: fix bogus scrub results when missing a clone (`issue#12738 <http://tracker.ceph.com/issues/12738>`_, `issue#12740 <http://tracker.ceph.com/issues/12740>`_, `pr#5783 <http://github.com/ceph/ceph/pull/5783>`_, David Zafman)
* osd: fix debug message in OSD::is_healthy (`pr#6226 <http://github.com/ceph/ceph/pull/6226>`_, Xiaoxi Chen)
* osd: fix MOSDOp encoding (`pr#6174 <http://github.com/ceph/ceph/pull/6174>`_, Sage Weil)
* osd: init started to 0 (`issue#13206 <http://tracker.ceph.com/issues/13206>`_, `pr#6107 <http://github.com/ceph/ceph/pull/6107>`_, Sage Weil)
* osd: KeyValueStore: fix the name's typo of keyvaluestore_default_strip_size (`pr#6375 <http://github.com/ceph/ceph/pull/6375>`_, Zhi Zhang)
* osd: new and delete ObjectStore::Transaction in a function is not necessary (`pr#6299 <http://github.com/ceph/ceph/pull/6299>`_, Ruifeng Yang)
* osd: optimize get_object_context (`pr#6305 <http://github.com/ceph/ceph/pull/6305>`_, Jianpeng Ma)
* osd: optimize MOSDOp/do_op/handle_op (`pr#5211 <http://github.com/ceph/ceph/pull/5211>`_, Jacek J. Lakis)
* osd: os/chain_xattr: On linux use linux/limits.h for XATTR_NAME_MAX. (`pr#6343 <http://github.com/ceph/ceph/pull/6343>`_, John Coyle)
* osd: reorder bool fields in PGLog struct (`pr#6279 <http://github.com/ceph/ceph/pull/6279>`_, Piotr Dałek)
* osd: ReplicatedPG: remove unused local variables (`issue#13575 <http://tracker.ceph.com/issues/13575>`_, `pr#6360 <http://github.com/ceph/ceph/pull/6360>`_, xiexingguo)
* osd: reset primary and up_primary when building a new past_interval. (`issue#13471 <http://tracker.ceph.com/issues/13471>`_, `pr#6240 <http://github.com/ceph/ceph/pull/6240>`_, xiexingguo)
* radosgw-admin: Checking the legality of the parameters (`issue#13018 <http://tracker.ceph.com/issues/13018>`_, `pr#5879 <http://github.com/ceph/ceph/pull/5879>`_, Qiankun Zheng)
* radosgw-admin: Create --secret-key alias for --secret (`issue#5821 <http://tracker.ceph.com/issues/5821>`_, `pr#5335 <http://github.com/ceph/ceph/pull/5335>`_, Yuan Zhou)
* radosgw-admin: metadata list user should return an empty list when user pool is empty (`issue#13596 <http://tracker.ceph.com/issues/13596>`_, `pr#6465 <http://github.com/ceph/ceph/pull/6465>`_, Orit Wasserman)
* rados: new options for write benchmark (`pr#6340 <http://github.com/ceph/ceph/pull/6340>`_, Joaquim Rocha)
* rbd: fix clone isssue (`issue#13553 <http://tracker.ceph.com/issues/13553>`_, `pr#6334 <http://github.com/ceph/ceph/pull/6334>`_, xinxin shu)
* rbd: fix init-rbdmap CMDPARAMS (`issue#13214 <http://tracker.ceph.com/issues/13214>`_, `pr#6109 <http://github.com/ceph/ceph/pull/6109>`_, Sage Weil)
* rbdmap: systemd support (`issue#13374 <http://tracker.ceph.com/issues/13374>`_, `pr#6479 <http://github.com/ceph/ceph/pull/6479>`_, Boris Ranto)
* rbd: rbdmap improvements (`pr#6445 <http://github.com/ceph/ceph/pull/6445>`_, Boris Ranto)
* release-notes: draft v0.94.4 release notes (`pr#5907 <http://github.com/ceph/ceph/pull/5907>`_, Loic Dachary)
* release-notes: draft v0.94.4 release notes (`pr#6195 <http://github.com/ceph/ceph/pull/6195>`_, Loic Dachary)
* release-notes: draft v0.94.4 release notes (`pr#6238 <http://github.com/ceph/ceph/pull/6238>`_, Loic Dachary)
* rgw: add compat header for TEMP_FAILURE_RETRY (`pr#6294 <http://github.com/ceph/ceph/pull/6294>`_, John Coyle)
* rgw: add default quota config (`pr#6400 <http://github.com/ceph/ceph/pull/6400>`_, Daniel Gryniewicz)
* rgw: add support for getting Swift's DLO without manifest handling (`pr#6206 <http://github.com/ceph/ceph/pull/6206>`_, Radoslaw Zarzynski)
* rgw: clarify the error message when trying to create an existed user (`pr#5938 <http://github.com/ceph/ceph/pull/5938>`_, Zeqiang Zhuang)
* rgw: fix objects can not be displayed which object name does not cont… (`issue#12963 <http://tracker.ceph.com/issues/12963>`_, `pr#5738 <http://github.com/ceph/ceph/pull/5738>`_, Weijun Duan)
* rgw: fix typo in RGWHTTPClient::process error message (`pr#6424 <http://github.com/ceph/ceph/pull/6424>`_, Brad Hubbard)
* rgw: fix wrong etag calculation during POST on S3 bucket. (`issue#11241 <http://tracker.ceph.com/issues/11241>`_, `pr#6030 <http://github.com/ceph/ceph/pull/6030>`_, Radoslaw Zarzynski)
* rgw: mdlog trim add usage prompt (`pr#6059 <http://github.com/ceph/ceph/pull/6059>`_, Weijun Duan)
* rgw: modify the conditional statement in parse_metadata_key method. (`pr#5875 <http://github.com/ceph/ceph/pull/5875>`_, Zengran Zhang)
* rgw: refuse to calculate digest when the s3 secret key is empty (`issue#13133 <http://tracker.ceph.com/issues/13133>`_, `pr#6045 <http://github.com/ceph/ceph/pull/6045>`_, Sangdi Xu)
* rgw: remove extra check in RGWGetObj::execute (`issue#12352 <http://tracker.ceph.com/issues/12352>`_, `pr#5262 <http://github.com/ceph/ceph/pull/5262>`_, Javier M. Mellid)
* rgw: support core file limit for radosgw daemon (`pr#6346 <http://github.com/ceph/ceph/pull/6346>`_, Guang Yang)
* rgw: swift use Civetweb ssl can not get right url (`issue#13628 <http://tracker.ceph.com/issues/13628>`_, `pr#6408 <http://github.com/ceph/ceph/pull/6408>`_, Weijun Duan)
* rocksdb: build with PORTABLE=1 (`pr#6311 <http://github.com/ceph/ceph/pull/6311>`_, Sage Weil)
* rocksdb: remove rdb source files from dist tarball (`issue#13554 <http://tracker.ceph.com/issues/13554>`_, `pr#6379 <http://github.com/ceph/ceph/pull/6379>`_, Kefu Chai)
* rocksdb: use native rocksdb makefile (and our autotools) (`pr#6290 <http://github.com/ceph/ceph/pull/6290>`_, Sage Weil)
* rpm: ceph.spec.in: correctly declare systemd dependency for SLE/openSUSE (`pr#6114 <http://github.com/ceph/ceph/pull/6114>`_, Nathan Cutler)
* rpm: ceph.spec.in: fix libs-compat / devel-compat conditional (`issue#12315 <http://tracker.ceph.com/issues/12315>`_, `pr#5219 <http://github.com/ceph/ceph/pull/5219>`_, Ken Dreyer)
* rpm: rhel 5.9 librados compile fix, moved blkid to RBD check/compilation (`issue#13177 <http://tracker.ceph.com/issues/13177>`_, `pr#5954 <http://github.com/ceph/ceph/pull/5954>`_, Rohan Mars)
* scripts: release_notes can track original issue (`pr#6009 <http://github.com/ceph/ceph/pull/6009>`_, Abhishek Lekshmanan)
* test/libcephfs/flock: add sys/file.h include for flock operations (`pr#6310 <http://github.com/ceph/ceph/pull/6310>`_, John Coyle)
* test_rgw_admin: use freopen for output redirection. (`pr#6303 <http://github.com/ceph/ceph/pull/6303>`_, John Coyle)
* tests: allow docker-test.sh to run under root (`issue#13355 <http://tracker.ceph.com/issues/13355>`_, `pr#6173 <http://github.com/ceph/ceph/pull/6173>`_, Loic Dachary)
* tests: ceph-disk workunit uses configobj  (`pr#6342 <http://github.com/ceph/ceph/pull/6342>`_, Loic Dachary)
* tests: destroy testprofile before creating one (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6446 <http://github.com/ceph/ceph/pull/6446>`_, Loic Dachary)
* tests: port uniqueness reminder (`pr#6387 <http://github.com/ceph/ceph/pull/6387>`_, Loic Dachary)
* tests: test/librados/test.cc must create profile (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6452 <http://github.com/ceph/ceph/pull/6452>`_, Loic Dachary)
* tools/cephfs: fix overflow writing header to fixed size buffer (#13816) (`pr#6617 <http://github.com/ceph/ceph/pull/6617>`_, John Spray)
* tools: ceph-monstore-update-crush: add "--test" when testing crushmap (`pr#6418 <http://github.com/ceph/ceph/pull/6418>`_, Kefu Chai)
* tools:remove duplicate references (`pr#5917 <http://github.com/ceph/ceph/pull/5917>`_, Bo Cai)
* vstart: grant full access to Swift testing account (`pr#6239 <http://github.com/ceph/ceph/pull/6239>`_, Yuan Zhou)
* vstart: set cephfs root uid/gid to caller (`pr#6255 <http://github.com/ceph/ceph/pull/6255>`_, John Spray)


Notable Changes since Hammer
----------------------------

* aarch64: add optimized version of crc32c (Yazen Ghannam, Steve Capper)
* auth: cache/reuse crypto lib key objects, optimize msg signature check (Sage Weil)
* auth: reinit NSS after fork() (#11128 Yan, Zheng)
* autotools: fix out of tree build (Krxysztof Kosinski)
* autotools: improve make check output (Loic Dachary)
* buffer: add invalidate_crc() (Piotr Dalek)
* buffer: fix zero bug (#12252 Haomai Wang)
* buffer: some cleanup (Michal Jarzabek)
* build: allow tcmalloc-minimal (Thorsten Behrens)
* build: C++11 now supported
* build: cmake: fix nss linking (Danny Al-Gaaf)
* build: cmake: misc fixes (Orit Wasserman, Casey Bodley)
* build: disable LTTNG by default (#11333 Josh Durgin)
* build: do not build ceph-dencoder with tcmalloc (#10691 Boris Ranto)
* build: fix junit detection on Fedora 22 (Ira Cooper)
* build: fix pg ref disabling (William A. Kennington III)
* build: fix ppc build (James Page)
* build: install-deps: misc fixes (Loic Dachary)
* build: install-deps.sh improvements (Loic Dachary)
* build: install-deps: support OpenSUSE (Loic Dachary)
* build: make_dist_tarball.sh (Sage Weil)
* build: many cmake improvements
* build: misc cmake fixes (Matt Benjamin)
* build: misc fixes (Boris Ranto, Ken Dreyer, Owen Synge)
* build: OSX build fixes (Yan, Zheng)
* build: remove rest-bench
* ceph-authtool: fix return code on error (Gerhard Muntingh)
* ceph-detect-init: added Linux Mint (Michal Jarzabek)
* ceph-detect-init: robust init system detection (Owen Synge)
* ceph-disk: ensure 'zap' only operates on a full disk (#11272 Loic Dachary)
* ceph-disk: fix zap sgdisk invocation (Owen Synge, Thorsten Behrens)
* ceph-disk: follow ceph-osd hints when creating journal (#9580 Sage Weil)
* ceph-disk: handle re-using existing partition (#10987 Loic Dachary)
* ceph-disk: improve parted output parsing (#10983 Loic Dachary)
* ceph-disk: install pip > 6.1 (#11952 Loic Dachary)
* ceph-disk: make suppression work for activate-all and activate-journal (Dan van der Ster)
* ceph-disk: many fixes (Loic Dachary, Alfredo Deza)
* ceph-disk: fixes to respect init system (Loic Dachary, Owen Synge)
* ceph-disk: pass --cluster arg on prepare subcommand (Kefu Chai)
* ceph-disk: support for multipath devices (Loic Dachary)
* ceph-disk: support NVMe device partitions (#11612 Ilja Slepnev)
* ceph: fix 'df' units (Zhe Zhang)
* ceph: fix parsing in interactive cli mode (#11279 Kefu Chai)
* cephfs-data-scan: many additions, improvements (John Spray)
* ceph-fuse: do not require successful remount when unmounting (#10982 Greg Farnum)
* ceph-fuse, libcephfs: don't clear COMPLETE when trimming null (Yan, Zheng)
* ceph-fuse, libcephfs: drop inode when rmdir finishes (#11339 Yan, Zheng)
* ceph-fuse,libcephfs: fix uninline (#11356 Yan, Zheng)
* ceph-fuse, libcephfs: hold exclusive caps on dirs we "own" (#11226 Greg Farnum)
* ceph-fuse: mostly behave on 32-bit hosts (Yan, Zheng)
* ceph: improve error output for 'tell' (#11101 Kefu Chai)
* ceph-monstore-tool: fix store-copy (Huangjun)
* ceph: new 'ceph daemonperf' command (John Spray, Mykola Golub)
* ceph-objectstore-tool: many many improvements (David Zafman)
* ceph-objectstore-tool: refactoring and cleanup (John Spray)
* ceph-post-file: misc fixes (Joey McDonald, Sage Weil)
* ceph_test_rados: test pipelined reads (Zhiqiang Wang)
* client: avoid sending unnecessary FLUSHSNAP messages (Yan, Zheng)
* client: exclude setfilelock when calculating oldest tid (Yan, Zheng)
* client: fix error handling in check_pool_perm (John Spray)
* client: fsync waits only for inode's caps to flush (Yan, Zheng)
* client: invalidate kernel dcache when cache size exceeds limits (Yan, Zheng)
* client: make fsync wait for unsafe dir operations (Yan, Zheng)
* client: pin lookup dentry to avoid inode being freed (Yan, Zheng)
* common: add descriptions to perfcounters (Kiseleva Alyona)
* common: add perf counter descriptions (Alyona Kiseleva)
* common: bufferlist performance tuning (Piotr Dalek, Sage Weil)
* common: detect overflow of int config values (#11484 Kefu Chai)
* common: fix bit_vector extent calc (#12611 Jason Dillaman)
* common: fix json parsing of utf8 (#7387 Tim Serong)
* common: fix leak of pthread_mutexattr (#11762 Ketor Meng)
* common: fix LTTNG vs fork issue (Josh Durgin)
* common: fix throttle max change (Henry Chang)
* common: make mutex more efficient
* common: make work queue addition/removal thread safe (#12662 Jason Dillaman)
* common: optracker improvements (Zhiqiang Wang, Jianpeng Ma)
* common: PriorityQueue tests (Kefu Chai)
* common: some async compression infrastructure (Haomai Wang)
* crush: add --check to validate dangling names, max osd id (Kefu Chai)
* crush: cleanup, sync with kernel (Ilya Dryomov)
* crush: fix crash from invalid 'take' argument (#11602 Shiva Rkreddy, Sage Weil)
* crush: fix divide-by-2 in straw2 (#11357 Yann Dupont, Sage Weil)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* crush: fix subtree base weight on adjust_subtree_weight (#11855 Sage Weil)
* crush: respect default replicated ruleset config on map creation (Ilya Dryomov)
* crushtool: fix order of operations, usage (Sage Weil)
* crypto: fix NSS leak (Jason Dillaman)
* crypto: fix unbalanced init/shutdown (#12598 Zheng Yan)
* deb: fix rest-bench-dbg and ceph-test-dbg dependendies (Ken Dreyer)
* debian: minor package reorg (Ken Dreyer)
* deb, rpm: move ceph-objectstore-tool to ceph (Ken Dreyer)
* doc: docuemnt object corpus generation (#11099 Alexis Normand)
* doc: document region hostnames (Robin H. Johnson)
* doc: fix gender neutrality (Alexandre Maragone)
* doc: fix install doc (#10957 Kefu Chai)
* doc: fix sphinx issues (Kefu Chai)
* doc: man page updates (Kefu Chai)
* doc: mds data structure docs (Yan, Zheng)
* doc: misc updates (Fracois Lafont, Ken Dreyer, Kefu Chai, Owen Synge, Gael Fenet-Garde, Loic Dachary, Yannick Atchy-Dalama, Jiaying Ren, Kevin Caradant, Robert Maxime, Nicolas Yong, Germain Chipaux, Arthur Gorjux, Gabriel Sentucq, Clement Lebrun, Jean-Remi Deveaux, Clair Massot, Robin Tang, Thomas Laumondais, Jordan Dorne, Yuan Zhou, Valentin Thomas, Pierre Chaumont, Benjamin Troquereau, Benjamin Sesia, Vikhyat Umrao, Nilamdyuti Goswami, Vartika Rai, Florian Haas, Loic Dachary, Simon Guinot, Andy Allan, Alistair Israel, Ken Dreyer, Robin Rehu, Lee Revell, Florian Marsylle, Thomas Johnson, Bosse Klykken, Travis Rhoden, Ian Kelling)
* doc: swift tempurls (#10184 Abhishek Lekshmanan)
* doc: switch doxygen integration back to breathe (#6115 Kefu Chai)
* doc: update release schedule docs (Loic Dachary)
* erasure-code: cleanup (Kefu Chai)
* erasure-code: improve tests (Loic Dachary)
* erasure-code: shec: fix recovery bugs (Takanori Nakao, Shotaro Kawaguchi)
* erasure-code: update ISA-L to 2.13 (Yuan Zhou)
* gmock: switch to submodule (Danny Al-Gaaf, Loic Dachary)
* hadoop: add terasort test (Noah Watkins)
* init-radosgw: merge with sysv version; fix enumeration (Sage Weil)
* java: fix libcephfs bindings (Noah Watkins)
* libcephfs: add pread, pwrite (Jevon Qiao)
* libcephfs,ceph-fuse: cache cleanup (Zheng Yan)
* libcephfs,ceph-fuse: fix request resend on cap reconnect (#10912 Yan, Zheng)
* librados: add config observer (Alistair Strachan)
* librados: add FULL_TRY and FULL_FORCE flags for dealing with full clusters or pools (Sage Weil)
* librados: add src_fadvise_flags for copy-from (Jianpeng Ma)
* librados: define C++ flags from C constants (Josh Durgin)
* librados: fadvise flags per op (Jianpeng Ma)
* librados: fix last_force_resent handling (#11026 Jianpeng Ma)
* librados: fix memory leak from C_TwoContexts (Xiong Yiliang)
* librados: fix notify completion race (#13114 Sage Weil)
* librados: fix striper when stripe_count = 1 and stripe_unit != object_size (#11120 Yan, Zheng)
* librados, libcephfs: randomize client nonces (Josh Durgin)
* librados: op perf counters (John Spray)
* librados: pybind: fix binary omap values (Robin H. Johnson)
* librados: pybind: fix write() method return code (Javier Guerra)
* librados: respect default_crush_ruleset on pool_create (#11640 Yuan Zhou)
* libradosstriper: fix leak (Danny Al-Gaaf)
* librbd: add const for single-client-only features (Josh Durgin)
* librbd: add deep-flatten operation (Jason Dillaman)
* librbd: add purge_on_error cache behavior (Jianpeng Ma)
* librbd: allow additional metadata to be stored with the image (Haomai Wang)
* librbd: avoid blocking aio API methods (#11056 Jason Dillaman)
* librbd: better handling for dup flatten requests (#11370 Jason Dillaman)
* librbd: cancel in-flight ops on watch error (#11363 Jason Dillaman)
* librbd: default new images to format 2 (#11348 Jason Dillaman)
* librbd: fadvise for copy, export, import (Jianpeng Ma)
* librbd: fast diff implementation that leverages object map (Jason Dillaman)
* librbd: fix fast diff bugs (#11553 Jason Dillaman)
* librbd: fix image format detection (Zhiqiang Wang)
* librbd: fix lock ordering issue (#11577 Jason Dillaman)
* librbd: fix reads larger than the cache size (Lu Shi)
* librbd: fix snapshot creation when other snap is active (#11475 Jason Dillaman)
* librbd: flatten/copyup fixes (Jason Dillaman)
* librbd: handle NOCACHE fadvise flag (Jinapeng Ma)
* librbd: lockdep, helgrind validation (Jason Dillaman, Josh Durgin)
* librbd: metadata filter fixes (Haomai Wang)
* librbd: misc aio fixes (#5488 Jason Dillaman)
* librbd: misc rbd fixes (#11478 #11113 #11342 #11380 Jason Dillaman, Zhiqiang Wang)
* librbd: new diff_iterate2 API (Jason Dillaman)
* librbd: object map rebuild support (Jason Dillaman)
* librbd: only update image flags while hold exclusive lock (#11791 Jason Dillaman)
* librbd: optionally disable allocation hint (Haomai Wang)
* librbd: prevent race between resize requests (#12664 Jason Dillaman)
* librbd: readahead fixes (Zhiqiang Wang)
* librbd: return result code from close (#12069 Jason Dillaman)
* librbd: store metadata, including config options, in image (Haomai Wang)
* librbd: tolerate old osds when getting image metadata (#11549 Jason Dillaman)
* librbd: use write_full when possible (Zhiqiang Wang)
* log: fix data corruption race resulting from log rotation (#12465 Samuel Just)
* logrotate.d: prefer service over invoke-rc.d (#11330 Win Hierman, Sage Weil)
* mds: add 'damaged' state to MDSMap (John Spray)
* mds: add nicknames for perfcounters (John Spray)
* mds: avoid emitting cap warnigns before evicting session (John Spray)
* mds: avoid getting stuck in XLOCKDONE (#11254 Yan, Zheng)
* mds: disable problematic rstat propagation into snap parents (Yan, Zheng)
* mds: do not add snapped items to bloom filter (Yan, Zheng)
* mds: expose frags via asok (John Spray)
* mds: fix expected holes in journal objects (#13167 Yan, Zheng)
* mds: fix handling for missing mydir dirfrag (#11641 John Spray)
* mds: fix integer truncateion on large client ids (Henry Chang)
* mds: fix mydir replica issue with shutdown (#10743 John Spray)
* mds: fix out-of-order messages (#11258 Yan, Zheng)
* mds: fix rejoin (Yan, Zheng)
* mds: fix setting entire file layout in one setxattr (John Spray)
* mds: fix shutdown (John Spray)
* mds: fix shutdown with strays (#10744 John Spray)
* mds: fix SnapServer crash on deleted pool (John Spray)
* mds: fix snapshot bugs (Yan, Zheng)
* mds: fix stray reintegration (Yan, Zheng)
* mds: fix stray handling (John Spray)
* mds: fix suicide beacon (John Spray)
* mds: flush immediately in do_open_truncate (#11011 John Spray)
* mds: handle misc corruption issues (John Spray)
* mds: improve dump methods (John Spray)
* mds: many fixes (Yan, Zheng, John Spray, Greg Farnum)
* mds: many snapshot and stray fixes (Yan, Zheng)
* mds: misc fixes (Jianpeng Ma, Dan van der Ster, Zhang Zhi)
* mds: misc journal cleanups and fixes (#10368 John Spray)
* mds: misc repair improvements (John Spray)
* mds: misc snap fixes (Zheng Yan)
* mds: misc snapshot fixes (Yan, Zheng)
* mds: new SessionMap storage using omap (#10649 John Spray)
* mds: persist completed_requests reliably (#11048 John Spray)
* mds: reduce memory consumption (Yan, Zheng)
* mds: respawn instead of suicide on blacklist (John Spray)
* mds: separate safe_pos in Journaler (#10368 John Spray)
* mds: snapshot rename support (#3645 Yan, Zheng)
* mds: store layout on header object (#4161 John Spray)
* mds: throttle purge stray operations (#10390 John Spray)
* mds: tolerate clock jumping backwards (#11053 Yan, Zheng)
* mds: warn when clients fail to advance oldest_client_tid (#10657 Yan, Zheng)
* misc cleanups and fixes (Danny Al-Gaaf)
* misc coverity fixes (Danny Al-Gaaf)
* misc performance and cleanup (Nathan Cutler, Xinxin Shu)
* mon: add cache over MonitorDBStore (Kefu Chai)
* mon: add 'mon_metadata <id>' command (Kefu Chai)
* mon: add 'node ls ...' command (Kefu Chai)
* mon: add NOFORWARD, OBSOLETE, DEPRECATE flags for mon commands (Joao Eduardo Luis)
* mon: add PG count to 'ceph osd df' output (Michal Jarzabek)
* mon: 'ceph osd metadata' can dump all osds (Haomai Wang)
* mon: clean up, reorg some mon commands (Joao Eduardo Luis)
* monclient: flush_log (John Spray)
* mon: detect kv backend failures (Sage Weil)
* mon: disallow >2 tiers (#11840 Kefu Chai)
* mon: disallow ec pools as tiers (#11650 Samuel Just)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: fix average utilization calc for 'osd df' (Mykola Golub)
* mon: fix CRUSH map test for new pools (Sage Weil)
* mon: fix log dump crash when debugging (Mykola Golub)
* mon: fix mds beacon replies (#11590 Kefu Chai)
* mon: fix metadata update race (Mykola Golub)
* mon: fix min_last_epoch_clean tracking (Kefu Chai)
* mon: fix 'pg ls' sort order, state names (#11569 Kefu Chai)
* mon: fix refresh (#11470 Joao Eduardo Luis)
* mon: fix variance calc in 'osd df' (Sage Weil)
* mon: improve callout to crushtool (Mykola Golub)
* mon: make blocked op messages more readable (Jianpeng Ma)
* mon: make osd get pool 'all' only return applicable fields (#10891 Michal Jarzabek)
* mon: misc scaling fixes (Sage Weil)
* mon: normalize erasure-code profile for storage and comparison (Loic Dachary)
* mon: only send mon metadata to supporting peers (Sage Weil)
* mon: optionally specify osd id on 'osd create' (Mykola Golub)
* mon: 'osd tree' fixes (Kefu Chai)
* mon: periodic background scrub (Joao Eduardo Luis)
* mon: prevent bucket deletion when referenced by a crush rule (#11602 Sage Weil)
* mon: prevent pgp_num > pg_num (#12025 Xinxin Shu)
* mon: prevent pool with snapshot state from being used as a tier (#11493 Sage Weil)
* mon: prime pg_temp when CRUSH map changes (Sage Weil)
* mon: refine check_remove_tier checks (#11504 John Spray)
* mon: reject large max_mds values (#12222 John Spray)
* mon: remove spurious who arg from 'mds rm ...' (John Spray)
* mon: streamline session handling, fix memory leaks (Sage Weil)
* mon: upgrades must pass through hammer (Sage Weil)
* mon: warn on bogus cache tier config (Jianpeng Ma)
* msgr: add ceph_perf_msgr tool (Hoamai Wang)
* msgr: async: fix seq handling (Haomai Wang)
* msgr: async: many many fixes (Haomai Wang)
* msgr: simple: fix clear_pipe (#11381 Haomai Wang)
* msgr: simple: fix connect_seq assert (Haomai Wang)
* msgr: xio: fastpath improvements (Raju Kurunkad)
* msgr: xio: fix ip and nonce (Raju Kurunkad)
* msgr: xio: improve lane assignment (Vu Pham)
* msgr: xio: sync with accellio v1.4 (Vu Pham)
* msgr: xio: misc fixes (#10735 Matt Benjamin, Kefu Chai, Danny Al-Gaaf, Raju Kurunkad, Vu Pham, Casey Bodley)
* msg: unit tests (Haomai Wang)
* objectcacher: misc bug fixes (Jianpeng Ma)
* osd: add latency perf counters for tier operations (Xinze Chi)
* osd: add misc perfcounters (Xinze Chi)
* osd: add simple sleep injection in recovery (Sage Weil)
* osd: allow SEEK_HOLE/SEEK_DATA for sparse read (Zhiqiang Wang)
* osd: avoid dup omap sets for in pg metadata (Sage Weil)
* osd: avoid multiple hit set insertions (Zhiqiang Wang)
* osd: avoid transaction append in some cases (Sage Weil)
* osd: break PG removal into multiple iterations (#10198 Guang Yang)
* osd: cache proxy-write support (Zhiqiang Wang, Samuel Just)
* osd: check scrub state when handling map (Jianpeng Ma)
* osd: clean up some constness, privateness (Kefu Chai)
* osd: clean up temp object if promotion fails (Jianpeng Ma)
* osd: configure promotion based on write recency (Zhiqiang Wang)
* osd: constrain collections to meta and PGs (normal and temp) (Sage Weil)
* osd: don't send dup MMonGetOSDMap requests (Sage Weil, Kefu Chai)
* osd: EIO injection (David Zhang)
* osd: elminiate txn apend, ECSubWrite copy (Samuel Just)
* osd: erasure-code: drop entries according to LRU (Andreas-Joachim Peters)
* osd: erasure-code: fix SHEC floating point bug (#12936 Loic Dachary)
* osd: erasure-code: update to ISA-L 2.14 (Yuan Zhou)
* osd: filejournal: cleanup (David Zafman)
* osd: filestore: clone using splice (Jianpeng Ma)
* osd: filestore: fix recursive lock (Xinxin Shu)
* osd: fix check_for_full (Henry Chang)
* osd: fix dirty accounting in make_writeable (Zhiqiang Wang)
* osd: fix dup promotion lost op bug (Zhiqiang Wang)
* osd: fix endless repair when object is unrecoverable (Jianpeng Ma, Kefu Chai)
* osd: fix hitset object naming to use GMT (Kefu Chai)
* osd: fix misc memory leaks (Sage Weil)
* osd: fix negative degraded stats during backfill (Guang Yang)
* osd: fix osdmap dump of blacklist items (John Spray)
* osd: fix peek_queue locking in FileStore (Xinze Chi)
* osd: fix pg resurrection (#11429 Samuel Just)
* osd: fix promotion vs full cache tier (Samuel Just)
* osd: fix replay requeue when pg is still activating (#13116 Samuel Just)
* osd: fix scrub stat bugs (Sage Weil, Samuel Just)
* osd: fix snap flushing from cache tier (again) (#11787 Samuel Just)
* osd: fix snap handling on promotion (#11296 Sam Just)
* osd: fix temp-clearing (David Zafman)
* osd: force promotion for ops EC can't handle (Zhiqiang Wang)
* osd: handle log split with overlapping entries (#11358 Samuel Just)
* osd: ignore non-existent osds in unfound calc (#10976 Mykola Golub)
* osd: improve behavior on machines with large memory pages (Steve Capper)
* osd: include a temp namespace within each collection/pgid (Sage Weil)
* osd: increase default max open files (Owen Synge)
* osd: keyvaluestore: misc fixes (Varada Kari)
* osd: low and high speed flush modes (Mingxin Liu)
* osd: make suicide timeouts individually configurable (Samuel Just)
* osd: merge multiple setattr calls into a setattrs call (Xinxin Shu)
* osd: misc fixes (Ning Yao, Kefu Chai, Xinze Chi, Zhiqiang Wang, Jianpeng Ma)
* osd: move scrub in OpWQ (Samuel Just)
* osd: newstore prototype (Sage Weil)
* osd: ObjectStore internal API refactor (Sage Weil)
* osd: peer_features includes self (David Zafman)
* osd: pool size change triggers new interval (#11771 Samuel Just)
* osd: prepopulate needs_recovery_map when only one peer has missing (#9558 Guang Yang)
* osd: randomize scrub times (#10973 Kefu Chai)
* osd: recovery, peering fixes (#11687 Samuel Just)
* osd: refactor scrub and digest recording (Sage Weil)
* osd: refuse first write to EC object at non-zero offset (Jianpeng Ma)
* osd: relax reply order on proxy read (#11211 Zhiqiang Wang)
* osd: require firefly features (David Zafman)
* osd: set initial crush weight with more precision (Sage Weil)
* osd: SHEC no longer experimental
* osd: skip promotion for flush/evict op (Zhiqiang Wang)
* osd: stripe over small xattrs to fit in XFS's 255 byte inline limit (Sage Weil, Ning Yao)
* osd: sync object_map on syncfs (Samuel Just)
* osd: take excl lock of op is rw (Samuel Just)
* osd: throttle evict ops (Yunchuan Wen)
* osd: upgrades must pass through hammer (Sage Weil)
* osd: use a temp object for recovery (Sage Weil)
* osd: use blkid to collection partition information (Joseph Handzik)
* osd: use SEEK_HOLE / SEEK_DATA for sparse copy (Xinxin Shu)
* osd: WBThrottle cleanups (Jianpeng Ma)
* osd: write journal header on clean shutdown (Xinze Chi)
* osdc/Objecter: allow per-pool calls to op_cancel_writes (John Spray)
* os/filestore: enlarge getxattr buffer size (Jianpeng Ma)
* pybind: pep8 cleanups (Danny Al-Gaaf)
* pycephfs: many fixes for bindings (Haomai Wang)
* qa: fix filelock_interrupt.py test (Yan, Zheng)
* qa: improve ceph-disk tests (Loic Dachary)
* qa: improve docker build layers (Loic Dachary)
* qa: run-make-check.sh script (Loic Dachary)
* rados: add --striper option to use libradosstriper (#10759 Sebastien Ponce)
* rados: bench: add --no-verify option to improve performance (Piotr Dalek)
* rados bench: misc fixes (Dmitry Yatsushkevich)
* rados: fix error message on failed pool removal (Wido den Hollander)
* radosgw-admin: add 'bucket check' function to repair bucket index (Yehuda Sadeh)
* radosgw-admin: fix subuser modify output (#12286 Guce)
* rados: handle --snapid arg properly (Abhishek Lekshmanan)
* rados: improve bench buffer handling, performance (Piotr Dalek)
* rados: misc bench fixes (Dmitry Yatsushkevich)
* rados: new pool import implementation (John Spray)
* rados: translate errno to string in CLI (#10877 Kefu Chai)
* rbd: accept map options config option (Ilya Dryomov)
* rbd: add disk usage tool (#7746 Jason Dillaman)
* rbd: allow unmapping by spec (Ilya Dryomov)
* rbd: cli: fix arg parsing with --io-pattern (Dmitry Yatsushkevich)
* rbd: deprecate --new-format option (Jason Dillman)
* rbd: fix error messages (#2862 Rajesh Nambiar)
* rbd: fix link issues (Jason Dillaman)
* rbd: improve CLI arg parsing, usage (Ilya Dryomov)
* rbd: rbd-replay-prep and rbd-replay improvements (Jason Dillaman)
* rbd: recognize queue_depth kernel option (Ilya Dryomov)
* rbd: support G and T units for CLI (Abhishek Lekshmanan)
* rbd: update rbd man page (Ilya Dryomov)
* rbd: update xfstests tests (Douglas Fuller)
* rbd: use image-spec and snap-spec in help (Vikhyat Umrao, Ilya Dryomov)
* rest-bench: misc fixes (Shawn Chen)
* rest-bench: support https (#3968 Yuan Zhou)
* rgw: add max multipart upload parts (#12146 Abshishek Dixit)
* rgw: add missing headers to Swift container details (#10666 Ahmad Faheem, Dmytro Iurchenko)
* rgw: add stats to headers for account GET (#10684 Yuan Zhou)
* rgw: add Trasnaction-Id to response (Abhishek Dixit)
* rgw: add X-Timestamp for Swift containers (#10938 Radoslaw Zarzynski)
* rgw: always check if token is expired (#11367 Anton Aksola, Riku Lehto)
* rgw: conversion tool to repair broken multipart objects (#12079 Yehuda Sadeh)
* rgw: document layout of pools and objects (Pete Zaitcev)
* rgw: do not enclose bucket header in quotes (#11860 Wido den Hollander)
* rgw: do not prefetch data for HEAD requests (Guang Yang)
* rgw: do not preserve ACLs when copying object (#12370 Yehuda Sadeh)
* rgw: do not set content-type if length is 0 (#11091 Orit Wasserman)
* rgw: don't clobber bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: don't use end_marker for namespaced object listing (#11437 Yehuda Sadeh)
* rgw: don't use rgw_socket_path if frontend is configured (#11160 Yehuda Sadeh)
* rgw: enforce Content-Length for POST on Swift cont/obj (#10661 Radoslaw Zarzynski)
* rgw: error out if frontend did not send all data (#11851 Yehuda Sadeh)
* rgw: expose the number of unhealthy workers through admin socket (Guang Yang)
* rgw: fail if parts not specified on multipart upload (#11435 Yehuda Sadeh)
* rgw: fix assignment of copy obj attributes (#11563 Yehuda Sadeh)
* rgw: fix broken stats in container listing (#11285 Radoslaw Zarzynski)
* rgw: fix bug in domain/subdomain splitting (Robin H. Johnson)
* rgw: fix casing of Content-Type header (Robin H. Johnson)
* rgw: fix civetweb max threads (#10243 Yehuda Sadeh)
* rgw: fix Connection: header handling (#12298 Wido den Hollander)
* rgw: fix copy metadata, support X-Copied-From for swift (#10663 Radoslaw Zarzynski)
* rgw: fix data corruptions race condition (#11749 Wuxingyi)
* rgw: fix decoding of X-Object-Manifest from GET on Swift DLO (Radslow Rzarzynski)
* rgw: fix GET on swift account when limit == 0 (#10683 Radoslaw Zarzynski)
* rgw: fix handling empty metadata items on Swift container (#11088 Radoslaw Zarzynski)
* rgw: fix JSON response when getting user quota (#12117 Wuxingyi)
* rgw: fix locator for objects starting with _ (#11442 Yehuda Sadeh)
* rgw: fix log rotation (Wuxingyi)
* rgw: fix mulitipart upload in retry path (#11604 Yehuda Sadeh)
* rgw: fix quota enforcement on POST (#11323 Sergey Arkhipov)
* rgw: fix reset_loc (#11974 Yehuda Sadeh)
* rgw: fix return code on missing upload (#11436 Yehuda Sadeh)
* rgw: fix sysvinit script
* rgw: fix sysvinit script w/ multiple instances (Sage Weil, Pavan Rallabhandi)
* rgw: force content_type for swift bucket stats requests (#12095 Orit Wasserman)
* rgw: force content type header on responses with no body (#11438 Orit Wasserman)
* rgw: generate Date header for civetweb (#10873 Radoslaw Zarzynski)
* rgw: generate new object tag when setting attrs (#11256 Yehuda Sadeh)
* rgw: improve content-length env var handling (#11419 Robin H. Johnson)
* rgw: improved support for swift account metadata (Radoslaw Zarzynski)
* rgw: improve handling of already removed buckets in expirer (Radoslaw Rzarzynski)
* rgw: issue aio for first chunk before flush cached data (#11322 Guang Yang)
* rgw: log to /var/log/ceph instead of /var/log/radosgw
* rgw: make init script wait for radosgw to stop (#11140 Dmitry Yatsushkevich)
* rgw: make max put size configurable (#6999 Yuan Zhou)
* rgw: make quota/gc threads configurable (#11047 Guang Yang)
* rgw: make read user buckets backward compat (#10683 Radoslaw Zarzynski)
* rgw: merge manifests properly with prefix override (#11622 Yehuda Sadeh)
* rgw: only scan for objects not in a namespace (#11984 Yehuda Sadeh)
* rgw: orphan detection tool (Yehuda Sadeh)
* rgw: pass in civetweb configurables (#10907 Yehuda Sadeh)
* rgw: rectify 202 Accepted in PUT response (#11148 Radoslaw Zarzynski)
* rgw: remove meta file after deleting bucket (#11149 Orit Wasserman)
* rgw: remove trailing :port from HTTP_HOST header (Sage Weil)
* rgw: return 412 on bad limit when listing buckets (#11613 Yehuda Sadeh)
* rgw: rework X-Trans-Id header to conform with Swift API (Radoslaw Rzarzynski)
* rgw: s3 encoding-type for get bucket (Jeff Weber)
* rgw: send ETag, Last-Modified for swift (#11087 Radoslaw Zarzynski)
* rgw: set content length on container GET, PUT, DELETE, HEAD (#10971, #11036 Radoslaw Zarzynski)
* rgw: set max buckets per user in ceph.conf (Vikhyat Umrao)
* rgw: shard work over multiple librados instances (Pavan Rallabhandi)
* rgw: support end marker on swift container GET (#10682 Radoslaw Zarzynski)
* rgw: support for Swift expiration API (Radoslaw Rzarzynski, Yehuda Sadeh)
* rgw: swift: allow setting attributes with COPY (#10662 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: do not override sent content type (#12363 Orit Wasserman)
* rgw: swift: enforce Content-Type in response (#12157 Radoslaw Zarzynski)
* rgw: swift: fix account listing (#11501 Radoslaw Zarzynski)
* rgw: swift: fix metadata handling on copy (#10645 Radoslaw Zarzynski)
* rgw: swift: send Last-Modified header (#10650 Radoslaw Zarzynski)
* rgw: swift: set Content-Length for account GET (#12158 Radoslav Zarzynski)
* rgw: swift: set content-length on keystone tokens (#11473 Herv Rousseau)
* rgw: update keystone cache with token info (#11125 Yehuda Sadeh)
* rgw: update to latest civetweb, enable config for IPv6 (#10965 Yehuda Sadeh)
* rgw: use attrs from source bucket on copy (#11639 Javier M. Mellid)
* rgw: use correct oid for gc chains (#11447 Yehuda Sadeh)
* rgw: user rm is idempotent (Orit Wasserman)
* rgw: use unique request id for civetweb (#10295 Orit Wasserman)
* rocksdb: add perf counters for get/put latency (Xinxin Shu)
* rocksdb, leveldb: fix compact_on_mount (Xiaoxi Chen)
* rocksdb: pass options as single string (Xiaoxi Chen)
* rocksdb: update to latest (Xiaoxi Chen)
* rpm: add suse firewall files (Tim Serong)
* rpm: always rebuild and install man pages for rpm (Owen Synge)
* rpm: loosen ceph-test dependencies (Ken Dreyer)
* rpm: many spec file fixes (Owen Synge, Ken Dreyer)
* rpm: misc fixes (Boris Ranto, Owen Synge, Ken Dreyer, Ira Cooper)
* rpm: misc systemd and SUSE fixes (Owen Synge, Nathan Cutler)
* selinux policy (Boris Ranto, Milan Broz)
* systemd: logrotate fixes (Tim Serong, Lars Marowsky-Bree, Nathan Cutler)
* systemd: many fixes (Sage Weil, Owen Synge, Boris Ranto, Dan van der Ster)
* systemd: run daemons as user ceph
* sysvinit compat: misc fixes (Owen Synge)
* test: misc fs test improvements (John Spray, Loic Dachary)
* test: python tests, linter cleanup (Alfredo Deza)
* tests: fixes for rbd xstests (Douglas Fuller)
* tests: fix tiering health checks (Loic Dachary)
* tests for low-level performance (Haomai Wang)
* tests: many ec non-regression improvements (Loic Dachary)
* tests: many many ec test improvements (Loic Dachary)
* upstart: throttle restarts (#11798 Sage Weil, Greg Farnum)


v10.0.5
=======

This is identical to v10.0.4 and was only created because of a git tagging mistake.

v10.0.4
=======

This is the fifth and last development release before Jewel.  The next release
will be a release candidate with the final set of features.  Big items include
RGW static website support, librbd journal framework, fixed mon sync of config-key
data, C++11 updates, and bluestore/kstore.

Notable Changes
---------------

* admin/build-doc: depend on zlib1g-dev and graphviz (`pr#7522 <http://github.com/ceph/ceph/pull/7522>`_, Ken Dreyer)
* buffer: use move construct to append/push_back/push_front (`pr#7455 <http://github.com/ceph/ceph/pull/7455>`_, Haomai Wang)
* build: allow jemalloc with rocksdb-static (`pr#7368 <http://github.com/ceph/ceph/pull/7368>`_, Somnath Roy)
* build: fix the autotools and cmake build (the new fusestore needs libfuse) (`pr#7393 <http://github.com/ceph/ceph/pull/7393>`_, Kefu Chai)
* build: fix warnings (`pr#7197 <http://github.com/ceph/ceph/pull/7197>`_, Kefu Chai, xie xingguo)
* build: fix warnings (`pr#7315 <http://github.com/ceph/ceph/pull/7315>`_, Kefu Chai)
* build: kill warnings (`pr#7397 <http://github.com/ceph/ceph/pull/7397>`_, Kefu Chai)
* build: move libexec scripts to standardize across distros (`issue#14687 <http://tracker.ceph.com/issues/14687>`_, `issue#14705 <http://tracker.ceph.com/issues/14705>`_, `issue#14723 <http://tracker.ceph.com/issues/14723>`_, `pr#7636 <http://github.com/ceph/ceph/pull/7636>`_, Nathan Cutler, Kefu Chai)
* build: Refrain from versioning and packaging EC testing plugins (`issue#14756 <http://tracker.ceph.com/issues/14756>`_, `issue#14723 <http://tracker.ceph.com/issues/14723>`_, `pr#7637 <http://github.com/ceph/ceph/pull/7637>`_, Nathan Cutler, Kefu Chai)
* build: spdk submodule; cmake (`pr#7503 <http://github.com/ceph/ceph/pull/7503>`_, Kefu Chai)
* ceph-disk: support bluestore (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7218 <http://github.com/ceph/ceph/pull/7218>`_, Loic Dachary, Sage Weil)
* ceph-disk/test: fix test_prepare.py::TestPrepare tests (`pr#7549 <http://github.com/ceph/ceph/pull/7549>`_, Kefu Chai)
* cleanup: remove misc dead code (`pr#7201 <http://github.com/ceph/ceph/pull/7201>`_, Erwan Velu)
* cls/cls_rbd: pass string by reference (`pr#7232 <http://github.com/ceph/ceph/pull/7232>`_, Jeffrey Lu)
* cmake: Added new unittests to make check (`pr#7572 <http://github.com/ceph/ceph/pull/7572>`_, Ali Maredia)
* cmake: add KernelDevice.cc to libos_srcs (`pr#7507 <http://github.com/ceph/ceph/pull/7507>`_, Kefu Chai)
* cmake: check for libsnappy in default path also (`pr#7366 <http://github.com/ceph/ceph/pull/7366>`_, Kefu Chai)
* cmake: feb5 (`pr#7541 <http://github.com/ceph/ceph/pull/7541>`_, Matt Benjamin)
* cmake: For CMake version <= 2.8.11, use LINK_PRIVATE and LINK_PUBLIC (`pr#7474 <http://github.com/ceph/ceph/pull/7474>`_, Tao Chang)
* cmake: let ceph-client-debug link with tcmalloc (`pr#7314 <http://github.com/ceph/ceph/pull/7314>`_, Kefu Chai)
* cmake: support ccache via a WITH_CCACHE build option (`pr#6875 <http://github.com/ceph/ceph/pull/6875>`_, John Coyle)
* common: add zlib compression plugin (`pr#7437 <http://github.com/ceph/ceph/pull/7437>`_, Alyona Kiseleva, Kiseleva Alyona)
* common: admin socket commands for tcmalloc heap get/set operations (`pr#7512 <http://github.com/ceph/ceph/pull/7512>`_, Samuel Just)
* common: ake ceph_time clocks work under BSD (`pr#7340 <http://github.com/ceph/ceph/pull/7340>`_, Adam C. Emerson)
* common: Allow OPT_INT settings with negative values (`issue#13829 <http://tracker.ceph.com/issues/13829>`_, `pr#7390 <http://github.com/ceph/ceph/pull/7390>`_, Brad Hubbard, Kefu Chai)
* common/buffer: replace RWLock with spinlocks (`pr#7294 <http://github.com/ceph/ceph/pull/7294>`_, Piotr Dałek)
* common: change the type of counter total/unhealthy_workers (`pr#7254 <http://github.com/ceph/ceph/pull/7254>`_, Guang Yang)
* common: snappy decompressor may assert when handling segmented input bufferlist (`issue#14400 <http://tracker.ceph.com/issues/14400>`_, `pr#7268 <http://github.com/ceph/ceph/pull/7268>`_, Igor Fedotov)
* common/str_map: cleanup: replaced get_str_map() function overloading by using default parameters for delimiters (`pr#7266 <http://github.com/ceph/ceph/pull/7266>`_, Sahithi R V)
* common: time: have skewing-now call non-skewing now (`pr#7466 <http://github.com/ceph/ceph/pull/7466>`_, Adam C. Emerson)
* common: unit test for interval_set implementations (`pr#6 <http://github.com/ceph/ceph/pull/6>`_, Igor Fedotov)
* config: add $data_dir/config to config search path (`pr#7377 <http://github.com/ceph/ceph/pull/7377>`_, Sage Weil)
* configure.ac: make "--with-librocksdb-static" default to 'check' (`issue#14463 <http://tracker.ceph.com/issues/14463>`_, `pr#7317 <http://github.com/ceph/ceph/pull/7317>`_, Dan Mick)
* crush: add safety assert (`issue#14496 <http://tracker.ceph.com/issues/14496>`_, `pr#7344 <http://github.com/ceph/ceph/pull/7344>`_, songbaisen)
* crush: reply quickly from get_immediate_parent (`issue#14334 <http://tracker.ceph.com/issues/14334>`_, `pr#7181 <http://github.com/ceph/ceph/pull/7181>`_, song baisen)
* debian: packaging fixes for jewel (`pr#7807 <http://github.com/ceph/ceph/pull/7807>`_, Ken Dreyer, Ali Maredia)
* debian/rpm split servers (`issue#10587 <http://tracker.ceph.com/issues/10587>`_, `pr#7746 <http://github.com/ceph/ceph/pull/7746>`_, Ken Dreyer)
* doc: add orphans commands to radosgw-admin(8) (`issue#14637 <http://tracker.ceph.com/issues/14637>`_, `pr#7518 <http://github.com/ceph/ceph/pull/7518>`_, Ken Dreyer)
* doc: amend the rados.8 (`pr#7251 <http://github.com/ceph/ceph/pull/7251>`_, Kefu Chai)
* doc: Fixes a CRUSH map step take argument (`pr#7327 <http://github.com/ceph/ceph/pull/7327>`_, Ivan Grcic)
* doc: fixing image in section ERASURE CODING (`pr#7298 <http://github.com/ceph/ceph/pull/7298>`_, Rachana Patel)
* doc: fix misleading configuration guide on cache tiering (`pr#7000 <http://github.com/ceph/ceph/pull/7000>`_, Yuan Zhou)
* doc: fix S3 C# example (`pr#7027 <http://github.com/ceph/ceph/pull/7027>`_, Dunrong Huang)
* doc: remove redundant space in ceph-authtool/monmaptool doc (`pr#7244 <http://github.com/ceph/ceph/pull/7244>`_, Jiaying Ren)
* doc: revise SubmittingPatches (`pr#7292 <http://github.com/ceph/ceph/pull/7292>`_, Kefu Chai)
* doc: rgw: port changes from downstream to upstream (`pr#7264 <http://github.com/ceph/ceph/pull/7264>`_, Bara Ancincova)
* doc: script and guidelines for mirroring Ceph (`pr#7384 <http://github.com/ceph/ceph/pull/7384>`_, Wido den Hollander)
* doc: use 'ceph auth get-or-create' for creating RGW keyring (`pr#6930 <http://github.com/ceph/ceph/pull/6930>`_, Wido den Hollander)
* global: do not start two daemons with a single pid-file (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7075 <http://github.com/ceph/ceph/pull/7075>`_, shun song)
* global: do not start two daemons with a single pid-file (part 2) (`issue#13422 <http://tracker.ceph.com/issues/13422>`_, `pr#7463 <http://github.com/ceph/ceph/pull/7463>`_, Loic Dachary)
* journal: flush commit position on metadata shutdown (`pr#7385 <http://github.com/ceph/ceph/pull/7385>`_, Mykola Golub)
* journal: reset commit_position_task_ctx pointer after task complete (`pr#7480 <http://github.com/ceph/ceph/pull/7480>`_, Mykola Golub)
* libcephfs: update LIBCEPHFS_VERSION to indicate the interface was changed (`pr#7551 <http://github.com/ceph/ceph/pull/7551>`_, Jevon Qiao)
* librados: move to c++11 concurrency types (`pr#5931 <http://github.com/ceph/ceph/pull/5931>`_, Adam C. Emerson)
* librados: remove duplicate definitions for rados pool_stat_t and cluster_stat_t (`pr#7330 <http://github.com/ceph/ceph/pull/7330>`_, Igor Fedotov)
* librados: shutdown finisher in a more graceful way (`pr#7519 <http://github.com/ceph/ceph/pull/7519>`_, xie xingguo)
* librados_test_stub: protect against notify/unwatch race (`pr#7540 <http://github.com/ceph/ceph/pull/7540>`_, Jason Dillaman)
* librbd: API: async open and close (`issue#14264 <http://tracker.ceph.com/issues/14264>`_, `pr#7259 <http://github.com/ceph/ceph/pull/7259>`_, Mykola Golub)
* librbd: Avoid create two threads per image (`pr#7400 <http://github.com/ceph/ceph/pull/7400>`_, Haomai Wang)
* librbd: block maintenance ops until after journal is ready (`issue#14510 <http://tracker.ceph.com/issues/14510>`_, `pr#7382 <http://github.com/ceph/ceph/pull/7382>`_, Jason Dillaman)
* librbd: fix internal handling of dynamic feature updates (`pr#7299 <http://github.com/ceph/ceph/pull/7299>`_, Jason Dillaman)
* librbd: journal framework for tracking exclusive lock transitions (`issue#13298 <http://tracker.ceph.com/issues/13298>`_, `pr#7529 <http://github.com/ceph/ceph/pull/7529>`_, Jason Dillaman)
* librbd: journal shut down flush race condition (`issue#14434 <http://tracker.ceph.com/issues/14434>`_, `pr#7302 <http://github.com/ceph/ceph/pull/7302>`_, Jason Dillaman)
* librbd: remove canceled tasks from timer thread (`issue#14476 <http://tracker.ceph.com/issues/14476>`_, `pr#7329 <http://github.com/ceph/ceph/pull/7329>`_, Douglas Fuller)
* makefile: remove libedit from libclient.la (`pr#7284 <http://github.com/ceph/ceph/pull/7284>`_, Kefu Chai)
* mds, client: fix locking around handle_conf_change (`issue#14365 <http://tracker.ceph.com/issues/14365>`_, `issue#14374 <http://tracker.ceph.com/issues/14374>`_, `pr#7312 <http://github.com/ceph/ceph/pull/7312>`_, John Spray)
* mds: judgment added to avoid the risk of visiting the NULL pointer (`pr#7358 <http://github.com/ceph/ceph/pull/7358>`_, Kongming Wu)
* mon: add an independent option for max election time (`pr#7245 <http://github.com/ceph/ceph/pull/7245>`_, Sangdi Xu)
* mon: compact full epochs also (`issue#14537 <http://tracker.ceph.com/issues/14537>`_, `pr#7396 <http://github.com/ceph/ceph/pull/7396>`_, Kefu Chai)
* mon: consider the pool size when setting pool crush rule (`issue#14495 <http://tracker.ceph.com/issues/14495>`_, `pr#7341 <http://github.com/ceph/ceph/pull/7341>`_, song baisen)
* mon: drop useless rank init assignment (`issue#14508 <http://tracker.ceph.com/issues/14508>`_, `pr#7321 <http://github.com/ceph/ceph/pull/7321>`_, huanwen ren)
* mon: fix locking in preinit error paths (`issue#14473 <http://tracker.ceph.com/issues/14473>`_, `pr#7353 <http://github.com/ceph/ceph/pull/7353>`_, huanwen ren)
* mon: fix monmap creation stamp (`pr#7459 <http://github.com/ceph/ceph/pull/7459>`_, duanweijun)
* mon: fix sync of config-key data (`pr#7363 <http://github.com/ceph/ceph/pull/7363>`_, Xiaowei Chen)
* mon: go into ERR state if multiple PGs are stuck inactive (`issue#13923 <http://tracker.ceph.com/issues/13923>`_, `pr#7253 <http://github.com/ceph/ceph/pull/7253>`_, Wido den Hollander)
* mon/MDSMonitor.cc: properly note beacon when health metrics changes (`issue#14684 <http://tracker.ceph.com/issues/14684>`_, `pr#7757 <http://github.com/ceph/ceph/pull/7757>`_, Yan, Zheng)
* mon/MonClient: avoid null pointer error when configured incorrectly (`issue#14405 <http://tracker.ceph.com/issues/14405>`_, `pr#7276 <http://github.com/ceph/ceph/pull/7276>`_, Bo Cai)
* mon: PG Monitor should report waiting for backfill (`issue#12744 <http://tracker.ceph.com/issues/12744>`_, `pr#7398 <http://github.com/ceph/ceph/pull/7398>`_, Abhishek Lekshmanan)
* mon: reduce CPU and memory manager pressure of pg health check (`pr#7482 <http://github.com/ceph/ceph/pull/7482>`_, Piotr Dałek)
* mon: some cleanup in MonmapMonitor.cc (`pr#7418 <http://github.com/ceph/ceph/pull/7418>`_, huanwen ren)
* mon: warn if pg(s) not scrubbed (`issue#13142 <http://tracker.ceph.com/issues/13142>`_, `pr#6440 <http://github.com/ceph/ceph/pull/6440>`_, Michal Jarzabek)
* msg/async: AsyncConnection: avoid debug log in cleanup_handler (`pr#7547 <http://github.com/ceph/ceph/pull/7547>`_, Haomai Wang)
* msg/async: bunch of fixes (`pr#7379 <http://github.com/ceph/ceph/pull/7379>`_, Piotr Dałek)
* msg/async: fix array boundary (`pr#7451 <http://github.com/ceph/ceph/pull/7451>`_, Wei Jin)
* msg/async: fix potential race condition (`pr#7453 <http://github.com/ceph/ceph/pull/7453>`_, Haomai Wang)
* msg/async: fix send closed local_connection message problem (`pr#7255 <http://github.com/ceph/ceph/pull/7255>`_, Haomai Wang)
* msg/async: reduce extra tcp packet for message ack (`pr#7380 <http://github.com/ceph/ceph/pull/7380>`_, Haomai Wang)
* msg/xio: fix compilation (`pr#7479 <http://github.com/ceph/ceph/pull/7479>`_, Roi Dayan)
* organizationmap: modify org mail info. (`pr#7240 <http://github.com/ceph/ceph/pull/7240>`_, Xiaowei Chen)
* os/bluestore: fix assert (`issue#14436 <http://tracker.ceph.com/issues/14436>`_, `pr#7293 <http://github.com/ceph/ceph/pull/7293>`_, xie xingguo)
* os/bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7342 <http://github.com/ceph/ceph/pull/7342>`_, Kefu Chai)
* os/bluestore: insert new onode to the front position of onode LRU (`pr#7492 <http://github.com/ceph/ceph/pull/7492>`_, Jianjian Huo)
* os/bluestore: use intrusive_ptr for Dir (`pr#7247 <http://github.com/ceph/ceph/pull/7247>`_, Igor Fedotov)
* osd: blockdevice: avoid implicit cast and add guard (`pr#7460 <http://github.com/ceph/ceph/pull/7460>`_, xie xingguo)
* osd: bluestore/BlueFS: initialize super block_size earlier in mkfs (`pr#7535 <http://github.com/ceph/ceph/pull/7535>`_, Sage Weil)
* osd: BlueStore: fix fsck and blockdevice read-relevant issue (`pr#7362 <http://github.com/ceph/ceph/pull/7362>`_, xie xingguo)
* osd: BlueStore: fix null pointer access (`issue#14561 <http://tracker.ceph.com/issues/14561>`_, `pr#7435 <http://github.com/ceph/ceph/pull/7435>`_, xie xingguo)
* osd: bluestore, kstore: fix nid overwritten logic (`issue#14407 <http://tracker.ceph.com/issues/14407>`_, `issue#14433 <http://tracker.ceph.com/issues/14433>`_, `pr#7283 <http://github.com/ceph/ceph/pull/7283>`_, xie xingguo)
* osd: bluestore: use btree_map for allocator (`pr#7269 <http://github.com/ceph/ceph/pull/7269>`_, Igor Fedotov, Sage Weil)
* osd: drop fiemap len=0 logic (`pr#7267 <http://github.com/ceph/ceph/pull/7267>`_, Sage Weil)
* osd: FileStore: add error check for object_map->sync() (`pr#7281 <http://github.com/ceph/ceph/pull/7281>`_, Chendi Xue)
* osd: FileStore: cleanup: remove obsolete option "filestore_xattr_use_omap" (`issue#14356 <http://tracker.ceph.com/issues/14356>`_, `pr#7217 <http://github.com/ceph/ceph/pull/7217>`_, Vikhyat Umrao)
* osd: FileStore: modify the format of colon (`pr#7333 <http://github.com/ceph/ceph/pull/7333>`_, Donghai Xu)
* osd: FileStore: print file name before osd assert if read file failed (`pr#7111 <http://github.com/ceph/ceph/pull/7111>`_, Ji Chen)
* osd: fix invalid list traversal in process_copy_chunk (`pr#7511 <http://github.com/ceph/ceph/pull/7511>`_, Samuel Just)
* osd, mon: fix exit issue (`pr#7420 <http://github.com/ceph/ceph/pull/7420>`_, Jiaying Ren)
* osd: PG::activate(): handle unexpected cached_removed_snaps more gracefully (`issue#14428 <http://tracker.ceph.com/issues/14428>`_, `pr#7309 <http://github.com/ceph/ceph/pull/7309>`_, Alexey Sheplyakov)
* os/fs: fix io_getevents argument (`pr#7355 <http://github.com/ceph/ceph/pull/7355>`_, Jingkai Yuan)
* os/fusestore: add error handling (`pr#7395 <http://github.com/ceph/ceph/pull/7395>`_, xie xingguo)
* os/keyvaluestore: kill KeyValueStore (`pr#7320 <http://github.com/ceph/ceph/pull/7320>`_, Haomai Wang)
* os/kstore: insert new onode to the front position of onode LRU (`pr#7505 <http://github.com/ceph/ceph/pull/7505>`_, xie xingguo)
* os/ObjectStore: add custom move operations for ObjectStore::Transaction (`pr#7303 <http://github.com/ceph/ceph/pull/7303>`_, Casey Bodley)
* rgw: Bug fix for mtime anomalies in RadosGW and other places (`pr#7328 <http://github.com/ceph/ceph/pull/7328>`_, Adam C. Emerson, Casey Bodley)
* rpm: move %post(un) ldconfig calls to ceph-base (`issue#14940 <http://tracker.ceph.com/issues/14940>`_, `pr#7867 <http://github.com/ceph/ceph/pull/7867>`_, Nathan Cutler)
* rpm: move runtime dependencies to ceph-base and fix other packaging issues (`issue#14864 <http://tracker.ceph.com/issues/14864>`_, `pr#7826 <http://github.com/ceph/ceph/pull/7826>`_, Nathan Cutler)
* test: ceph_test_rados: use less CPU (`pr#7513 <http://github.com/ceph/ceph/pull/7513>`_, Samuel Just)

v10.0.3
=======

This is the fourth development release for Jewel.  Several big pieces
have been added this release, including BlueStore (a new backend for
OSD to replace FileStore), many ceph-disk fixes, a new CRUSH tunable
that improves mapping stability, a new librados object enumeration API,
and a whole slew of OSD and RADOS optimizations.

Note that, due to general developer busyness, we aren't building official
release packages for this dev release.  You can fetch autobuilt gitbuilder
packages from the usual location (gitbuilder.ceph.com).

Notable Changes
---------------

* bluestore: latest and greatest (`issue#14210 <http://tracker.ceph.com/issues/14210>`_, `issue#13801 <http://tracker.ceph.com/issues/13801>`_, `pr#6896 <http://github.com/ceph/ceph/pull/6896>`_, xie.xingguo, Jianpeng Ma, YiQiang Chen, Sage Weil, Ning Yao)
* buffer: fix internal iterator invalidation on rebuild, get_contiguous (`pr#6962 <http://github.com/ceph/ceph/pull/6962>`_, Sage Weil)
* build: fix a few warnings (`pr#6847 <http://github.com/ceph/ceph/pull/6847>`_, Orit Wasserman)
* build: misc make check fixes (`pr#7153 <http://github.com/ceph/ceph/pull/7153>`_, Sage Weil)
* ceph-detect-init: fix py3 test (`pr#7025 <http://github.com/ceph/ceph/pull/7025>`_, Kefu Chai)
* ceph-disk: add -f flag for btrfs mkfs (`pr#7222 <http://github.com/ceph/ceph/pull/7222>`_, Darrell Enns)
* ceph-disk: ceph-disk list fails on /dev/cciss!c0d0 (`issue#13970 <http://tracker.ceph.com/issues/13970>`_, `issue#14233 <http://tracker.ceph.com/issues/14233>`_, `issue#14230 <http://tracker.ceph.com/issues/14230>`_, `pr#6879 <http://github.com/ceph/ceph/pull/6879>`_, Loic Dachary)
* ceph-disk: fix failures when preparing disks with udev > 214 (`issue#14080 <http://tracker.ceph.com/issues/14080>`_, `issue#14094 <http://tracker.ceph.com/issues/14094>`_, `pr#6926 <http://github.com/ceph/ceph/pull/6926>`_, Loic Dachary, Ilya Dryomov)
* ceph-disk: Fix trivial typo (`pr#7472 <http://github.com/ceph/ceph/pull/7472>`_, Brad Hubbard)
* ceph-disk: warn for prepare partitions with bad GUIDs (`issue#13943 <http://tracker.ceph.com/issues/13943>`_, `pr#6760 <http://github.com/ceph/ceph/pull/6760>`_, David Disseldorp)
* ceph-fuse: fix double decreasing the count to trim caps (`issue#14319 <http://tracker.ceph.com/issues/14319>`_, `pr#7229 <http://github.com/ceph/ceph/pull/7229>`_, Zhi Zhang)
* ceph-fuse: fix double free of args (`pr#7015 <http://github.com/ceph/ceph/pull/7015>`_, Ilya Shipitsin)
* ceph-fuse: fix fsync() (`pr#6388 <http://github.com/ceph/ceph/pull/6388>`_, Yan, Zheng)
* ceph-fuse:print usage information when no parameter specified (`pr#6868 <http://github.com/ceph/ceph/pull/6868>`_, Bo Cai)
* ceph: improve the error message (`issue#11101 <http://tracker.ceph.com/issues/11101>`_, `pr#7106 <http://github.com/ceph/ceph/pull/7106>`_, Kefu Chai)
* ceph.in: avoid a broken pipe error when use ceph command (`issue#14354 <http://tracker.ceph.com/issues/14354>`_, `pr#7212 <http://github.com/ceph/ceph/pull/7212>`_, Bo Cai)
* ceph.spec.in: add copyright notice (`issue#14694 <http://tracker.ceph.com/issues/14694>`_, `pr#7569 <http://github.com/ceph/ceph/pull/7569>`_, Nathan Cutler)
* ceph.spec.in: add license declaration (`pr#7574 <http://github.com/ceph/ceph/pull/7574>`_, Nathan Cutler)
* ceph_test_libcephfs: tolerate duplicated entries in readdir (`issue#14377 <http://tracker.ceph.com/issues/14377>`_, `pr#7246 <http://github.com/ceph/ceph/pull/7246>`_, Yan, Zheng)
* client: check if Fh is readable when processing a read (`issue#11517 <http://tracker.ceph.com/issues/11517>`_, `pr#7209 <http://github.com/ceph/ceph/pull/7209>`_, Yan, Zheng)
* client: properly trim unlinked inode (`issue#13903 <http://tracker.ceph.com/issues/13903>`_, `pr#7297 <http://github.com/ceph/ceph/pull/7297>`_, Yan, Zheng)
* cls_rbd: add guards for error cases (`issue#14316 <http://tracker.ceph.com/issues/14316>`_, `issue#14317 <http://tracker.ceph.com/issues/14317>`_, `pr#7165 <http://github.com/ceph/ceph/pull/7165>`_, xie xingguo)
* cls_rbd: enable object map checksums for object_map_save (`issue#14280 <http://tracker.ceph.com/issues/14280>`_, `pr#7149 <http://github.com/ceph/ceph/pull/7149>`_, Douglas Fuller)
* cmake: Add ENABLE_GIT_VERSION to avoid rebuilding (`pr#7171 <http://github.com/ceph/ceph/pull/7171>`_, Kefu Chai)
* cmake: add missing check for HAVE_EXECINFO_H (`pr#7270 <http://github.com/ceph/ceph/pull/7270>`_, Casey Bodley)
* cmake: cleanups and more features from automake (`pr#7103 <http://github.com/ceph/ceph/pull/7103>`_, Casey Bodley, Ali Maredia)
* cmake: detect bzip2 and lz4 (`pr#7126 <http://github.com/ceph/ceph/pull/7126>`_, Kefu Chai)
* cmake: fix build with bluestore (`pr#7099 <http://github.com/ceph/ceph/pull/7099>`_, John Spray)
* cmake: fix the build on trusty (`pr#7249 <http://github.com/ceph/ceph/pull/7249>`_, Kefu Chai)
* cmake: made rocksdb an imported library (`pr#7131 <http://github.com/ceph/ceph/pull/7131>`_, Ali Maredia)
* cmake: no need to run configure from run-cmake-check.sh (`pr#6959 <http://github.com/ceph/ceph/pull/6959>`_, Orit Wasserman)
* cmake: test_build_libcephfs needs ${ALLOC_LIBS} (`pr#7300 <http://github.com/ceph/ceph/pull/7300>`_, Ali Maredia)
* common/address_help.cc: fix the leak in entity_addr_from_url() (`issue#14132 <http://tracker.ceph.com/issues/14132>`_, `pr#6987 <http://github.com/ceph/ceph/pull/6987>`_, Qiankun Zheng)
* common: add thread names (`pr#5882 <http://github.com/ceph/ceph/pull/5882>`_, Igor Podoski)
* common: assert: abort() rather than throw (`pr#6804 <http://github.com/ceph/ceph/pull/6804>`_, Adam C. Emerson)
* common: buffer/assert minor fixes (`pr#6990 <http://github.com/ceph/ceph/pull/6990>`_, Matt Benjamin)
* common/Formatter: avoid newline if there is no output (`pr#5351 <http://github.com/ceph/ceph/pull/5351>`_, Aran85)
* common: improve shared_cache and simple_cache efficiency with hash table (`pr#6909 <http://github.com/ceph/ceph/pull/6909>`_, Ning Yao)
* common/lockdep: increase max lock names (`pr#6961 <http://github.com/ceph/ceph/pull/6961>`_, Sage Weil)
* common: new timekeeping common code, and Objecter conversion (`pr#5782 <http://github.com/ceph/ceph/pull/5782>`_, Adam C. Emerson)
* common: signal_handler: added support for using reentrant strsignal() implementations vs. sys_siglist[] (`pr#6796 <http://github.com/ceph/ceph/pull/6796>`_, John Coyle)
* config: complains when a setting is not tracked (`issue#11692 <http://tracker.ceph.com/issues/11692>`_, `pr#7085 <http://github.com/ceph/ceph/pull/7085>`_, Kefu Chai)
* configure: detect bz2 and lz4 (`issue#13850 <http://tracker.ceph.com/issues/13850>`_, `issue#13981 <http://tracker.ceph.com/issues/13981>`_, `pr#7030 <http://github.com/ceph/ceph/pull/7030>`_, Kefu Chai)
* correct radosgw-admin command (`pr#7006 <http://github.com/ceph/ceph/pull/7006>`_, YankunLi)
* crush: add chooseleaf_stable tunable (`pr#6572 <http://github.com/ceph/ceph/pull/6572>`_, Sangdi Xu, Sage Weil)
* crush: clean up whitespace removal (`issue#14302 <http://tracker.ceph.com/issues/14302>`_, `pr#7157 <http://github.com/ceph/ceph/pull/7157>`_, songbaisen)
* crush/CrushTester: check for overlapped rules (`pr#7139 <http://github.com/ceph/ceph/pull/7139>`_, Kefu Chai)
* crushtool: improve usage/tip messages (`pr#7142 <http://github.com/ceph/ceph/pull/7142>`_, xie xingguo)
* crushtool: set type 0 name "device" for --build option (`pr#6824 <http://github.com/ceph/ceph/pull/6824>`_, Sangdi Xu)
* doc: adding "--allow-shrink" in decreasing the size of the rbd block to distinguish from the increasing option (`pr#7020 <http://github.com/ceph/ceph/pull/7020>`_, Yehua)
* doc: admin/build-doc: make paths absolute (`pr#7119 <http://github.com/ceph/ceph/pull/7119>`_, Dan Mick)
* doc: dev: document ceph-qa-suite (`pr#6955 <http://github.com/ceph/ceph/pull/6955>`_, Loic Dachary)
* doc: document "readforward" and "readproxy" cache mode (`pr#7023 <http://github.com/ceph/ceph/pull/7023>`_, Kefu Chai)
* doc: fix "mon osd down out subtree limit" option name (`pr#7164 <http://github.com/ceph/ceph/pull/7164>`_, François Lafont)
* doc: fix typo (`pr#7004 <http://github.com/ceph/ceph/pull/7004>`_, tianqing)
* doc: Updated the rados command man page to include the --run-name opt… (`issue#12899 <http://tracker.ceph.com/issues/12899>`_, `pr#5900 <http://github.com/ceph/ceph/pull/5900>`_, ritz303)
* fs: be more careful about the "mds setmap" command to prevent breakage (`issue#14380 <http://tracker.ceph.com/issues/14380>`_, `pr#7262 <http://github.com/ceph/ceph/pull/7262>`_, Yan, Zheng)
* helgrind: additional race conditionslibrbd: journal replay should honor inter-event dependencies (`pr#7274 <http://github.com/ceph/ceph/pull/7274>`_, Jason Dillaman)
* helgrind: fix real (and imaginary) race conditions (`issue#14163 <http://tracker.ceph.com/issues/14163>`_, `pr#7208 <http://github.com/ceph/ceph/pull/7208>`_, Jason Dillaman)
* kv: implement value_as_ptr() and use it in .get() (`pr#7052 <http://github.com/ceph/ceph/pull/7052>`_, Piotr Dałek)
* librados: add c++ style osd/pg command interface (`pr#6893 <http://github.com/ceph/ceph/pull/6893>`_, Yunchuan Wen)
* librados: fix several flaws introduced by the enumeration_objects API (`issue#14299 <http://tracker.ceph.com/issues/14299>`_, `issue#14301 <http://tracker.ceph.com/issues/14301>`_, `issue#14300 <http://tracker.ceph.com/issues/14300>`_, `pr#7156 <http://github.com/ceph/ceph/pull/7156>`_, xie xingguo)
* librados: new style (sharded) object listing (`pr#6405 <http://github.com/ceph/ceph/pull/6405>`_, John Spray, Sage Weil)
* librados: potential null pointer access in list_(n)objects (`issue#13822 <http://tracker.ceph.com/issues/13822>`_, `pr#6639 <http://github.com/ceph/ceph/pull/6639>`_, xie xingguo)
* librbd: exit if parent's snap is gone during clone (`issue#14118 <http://tracker.ceph.com/issues/14118>`_, `pr#6968 <http://github.com/ceph/ceph/pull/6968>`_, xie xingguo)
* librbd: fix potential memory leak (`issue#14332 <http://tracker.ceph.com/issues/14332>`_, `issue#14333 <http://tracker.ceph.com/issues/14333>`_, `pr#7174 <http://github.com/ceph/ceph/pull/7174>`_, xie xingguo)
* librbd: fix snap_exists API return code overflow (`issue#14129 <http://tracker.ceph.com/issues/14129>`_, `pr#6986 <http://github.com/ceph/ceph/pull/6986>`_, xie xingguo)
* librbd: journal replay should honor inter-event dependencies (`pr#7019 <http://github.com/ceph/ceph/pull/7019>`_, Jason Dillaman)
* librbd: return error if we fail to delete object_map head object (`issue#14098 <http://tracker.ceph.com/issues/14098>`_, `pr#6958 <http://github.com/ceph/ceph/pull/6958>`_, xie xingguo)
* librbd: small fixes for error messages and readahead counter (`issue#14127 <http://tracker.ceph.com/issues/14127>`_, `pr#6983 <http://github.com/ceph/ceph/pull/6983>`_, xie xingguo)
* librbd: uninitialized state in snap remove state machine (`pr#6982 <http://github.com/ceph/ceph/pull/6982>`_, Jason Dillaman)
* mailmap: hange organization for Dongmao Zhang (`pr#7173 <http://github.com/ceph/ceph/pull/7173>`_, Dongmao Zhang)
* mailmap: Igor Podoski affiliation (`pr#7219 <http://github.com/ceph/ceph/pull/7219>`_, Igor Podoski)
* mailmap update (`pr#7210 <http://github.com/ceph/ceph/pull/7210>`_, M Ranga Swami Reddy)
* mailmap updates (`pr#6992 <http://github.com/ceph/ceph/pull/6992>`_, Loic Dachary)
* mailmap updates (`pr#7189 <http://github.com/ceph/ceph/pull/7189>`_, Loic Dachary)
* man: document listwatchers cmd in "rados" manpage (`pr#7021 <http://github.com/ceph/ceph/pull/7021>`_, Kefu Chai)
* mds: advance clientreplay when replying (`issue#14357 <http://tracker.ceph.com/issues/14357>`_, `pr#7216 <http://github.com/ceph/ceph/pull/7216>`_, John Spray)
* mds: expose state of recovery to status ASOK command (`issue#14146 <http://tracker.ceph.com/issues/14146>`_, `pr#7068 <http://github.com/ceph/ceph/pull/7068>`_, Yan, Zheng)
* mds: fix client cap/message replay order on restart (`issue#14254 <http://tracker.ceph.com/issues/14254>`_, `issue#13546 <http://tracker.ceph.com/issues/13546>`_, `pr#7199 <http://github.com/ceph/ceph/pull/7199>`_, Yan, Zheng)
* mds: fix standby replay thread creation (`issue#14144 <http://tracker.ceph.com/issues/14144>`_, `pr#7132 <http://github.com/ceph/ceph/pull/7132>`_, John Spray)
* mds: we should wait messenger when MDSDaemon suicide (`pr#6996 <http://github.com/ceph/ceph/pull/6996>`_, Wei Feng)
* mon: add `osd blacklist clear` (`pr#6945 <http://github.com/ceph/ceph/pull/6945>`_, John Spray)
* mon: add RAW USED column to ceph df detail (`pr#7087 <http://github.com/ceph/ceph/pull/7087>`_, Ruifeng Yang)
* mon: degrade a log message to level 2 (`pr#6929 <http://github.com/ceph/ceph/pull/6929>`_, Kongming Wu)
* mon: fix coding-style on PG related Monitor files (`pr#6881 <http://github.com/ceph/ceph/pull/6881>`_, Wido den Hollander)
* mon: fixes related to mondbstore->get() changes (`pr#6564 <http://github.com/ceph/ceph/pull/6564>`_, Piotr Dałek)
* mon: fix reuse of osd ids (clear osd info on osd deletion) (`issue#13988 <http://tracker.ceph.com/issues/13988>`_, `pr#6900 <http://github.com/ceph/ceph/pull/6900>`_, Loic Dachary, Sage Weil)
* mon: fix the can't change subscribe level bug in monitoring log (`pr#7031 <http://github.com/ceph/ceph/pull/7031>`_, Zhiqiang Wang)
* mon/MDSMonitor: add confirmation to "ceph mds rmfailed" (`issue#14379 <http://tracker.ceph.com/issues/14379>`_, `pr#7248 <http://github.com/ceph/ceph/pull/7248>`_, Yan, Zheng)
* mon: modify a dout level in OSDMonitor.cc (`pr#6928 <http://github.com/ceph/ceph/pull/6928>`_, Yongqiang He)
* mon: MonmapMonitor: don't expose uncommitted state to client (`pr#6854 <http://github.com/ceph/ceph/pull/6854>`_, Joao Eduardo Luis)
* mon/OSDMonitor: osdmap laggy set a maximum limit for interval (`pr#7109 <http://github.com/ceph/ceph/pull/7109>`_, Zengran Zhang)
* mon: paxos is_recovering calc error (`pr#7227 <http://github.com/ceph/ceph/pull/7227>`_, Weijun Duan)
* mon/PGMap: show rd/wr iops separately in status reports (`pr#7072 <http://github.com/ceph/ceph/pull/7072>`_, Cilang Zhao)
* mon: PGMonitor: acting primary diff with cur_stat, should not set pg to stale (`pr#7083 <http://github.com/ceph/ceph/pull/7083>`_, Xiaowei Chen)
* msg: add override to virutal methods (`pr#6977 <http://github.com/ceph/ceph/pull/6977>`_, Michal Jarzabek)
* msg/async: cleanup dead connection and misc things (`pr#7158 <http://github.com/ceph/ceph/pull/7158>`_, Haomai Wang)
* msg/async: don't use shared_ptr to manage EventCallback (`pr#7028 <http://github.com/ceph/ceph/pull/7028>`_, Haomai Wang)
* msg: filter out lo addr when bind osd addr (`pr#7012 <http://github.com/ceph/ceph/pull/7012>`_, Ji Chen)
* msg: removed unneeded includes from Dispatcher (`pr#6814 <http://github.com/ceph/ceph/pull/6814>`_, Michal Jarzabek)
* msg: remove unneeded inline (`pr#6989 <http://github.com/ceph/ceph/pull/6989>`_, Michal Jarzabek)
* msgr:  fix large message data content length causing overflow (`pr#6809 <http://github.com/ceph/ceph/pull/6809>`_, Jun Huang, Haomai Wang)
* msg/simple: pipe: memory leak when signature check failed (`pr#7096 <http://github.com/ceph/ceph/pull/7096>`_, Ruifeng Yang)
* msg/simple: remove unneeded friend declarations (`pr#6924 <http://github.com/ceph/ceph/pull/6924>`_, Michal Jarzabek)
* objecter: avoid recursive lock of Objecter::rwlock (`pr#7343 <http://github.com/ceph/ceph/pull/7343>`_, Yan, Zheng)
* os/bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7419 <http://github.com/ceph/ceph/pull/7419>`_, Kefu Chai, Brad Hubbard)
* osd: add cache hint when pushing raw clone during recovery (`pr#7069 <http://github.com/ceph/ceph/pull/7069>`_, Zhiqiang Wang)
* osd: avoid debug std::string initialization in PG::get/put (`pr#7117 <http://github.com/ceph/ceph/pull/7117>`_, Evgeniy Firsov)
* osd: avoid osd_op_thread suicide because osd_scrub_sleep (`pr#7009 <http://github.com/ceph/ceph/pull/7009>`_, Jianpeng Ma)
* osd: bluestore: bluefs: fix several small bugs (`issue#14344 <http://tracker.ceph.com/issues/14344>`_, `issue#14343 <http://tracker.ceph.com/issues/14343>`_, `pr#7200 <http://github.com/ceph/ceph/pull/7200>`_, xie xingguo)
* osd: bluestore: don't include when building without libaio (`issue#14207 <http://tracker.ceph.com/issues/14207>`_, `pr#7169 <http://github.com/ceph/ceph/pull/7169>`_, Mykola Golub)
* osd: bluestore: fix bluestore onode_t attr leak (`pr#7125 <http://github.com/ceph/ceph/pull/7125>`_, Ning Yao)
* osd: bluestore: fix bluestore_wal_transaction_t encoding test (`pr#7168 <http://github.com/ceph/ceph/pull/7168>`_, Kefu Chai)
* osd: bluestore: fix several bugs (`issue#14259 <http://tracker.ceph.com/issues/14259>`_, `issue#14353 <http://tracker.ceph.com/issues/14353>`_, `issue#14260 <http://tracker.ceph.com/issues/14260>`_, `issue#14261 <http://tracker.ceph.com/issues/14261>`_, `pr#7122 <http://github.com/ceph/ceph/pull/7122>`_, xie xingguo)
* osd: bluestore: fix space rebalancing, collection split, buffered reads (`pr#7196 <http://github.com/ceph/ceph/pull/7196>`_, Sage Weil)
* osd: bluestore: more fixes (`pr#7130 <http://github.com/ceph/ceph/pull/7130>`_, Sage Weil)
* osd: cache tier: add config option for eviction check list size (`pr#6997 <http://github.com/ceph/ceph/pull/6997>`_, Yuan Zhou)
* osdc: Fix race condition with tick_event and shutdown (`issue#14256 <http://tracker.ceph.com/issues/14256>`_, `pr#7151 <http://github.com/ceph/ceph/pull/7151>`_, Adam C. Emerson)
* osd: check health state before pre_booting (`issue#14181 <http://tracker.ceph.com/issues/14181>`_, `pr#7053 <http://github.com/ceph/ceph/pull/7053>`_, Xiaoxi Chen)
* osd: clear pg_stat_queue after stopping pgs (`issue#14212 <http://tracker.ceph.com/issues/14212>`_, `pr#7091 <http://github.com/ceph/ceph/pull/7091>`_, Sage Weil)
* osd: delay populating in-memory PG log hashmaps (`pr#6425 <http://github.com/ceph/ceph/pull/6425>`_, Piotr Dałek)
* osd: disable filestore_xfs_extsize by default (`issue#14397 <http://tracker.ceph.com/issues/14397>`_, `pr#7265 <http://github.com/ceph/ceph/pull/7265>`_, Ken Dreyer)
* osd: do not keep ref of old osdmap in pg (`issue#13990 <http://tracker.ceph.com/issues/13990>`_, `pr#7007 <http://github.com/ceph/ceph/pull/7007>`_, Kefu Chai)
* osd: drop deprecated removal pg type (`pr#6970 <http://github.com/ceph/ceph/pull/6970>`_, Igor Podoski)
* osd: FileJournal: fix return code of create method (`issue#14134 <http://tracker.ceph.com/issues/14134>`_, `pr#6988 <http://github.com/ceph/ceph/pull/6988>`_, xie xingguo)
* osd: FileJournal: support batch peak and pop from writeq (`pr#6701 <http://github.com/ceph/ceph/pull/6701>`_, Xinze Chi)
* osd: FileStore: conditional collection of drive metadata (`pr#6956 <http://github.com/ceph/ceph/pull/6956>`_, Somnath Roy)
* osd: FileStore:: optimize lfn_unlink (`pr#6649 <http://github.com/ceph/ceph/pull/6649>`_, Jianpeng Ma)
* osd: fix null pointer access and race condition (`issue#14072 <http://tracker.ceph.com/issues/14072>`_, `pr#6916 <http://github.com/ceph/ceph/pull/6916>`_, xie xingguo)
* osd: fix scrub start hobject (`pr#7467 <http://github.com/ceph/ceph/pull/7467>`_, Sage Weil)
* osd: fix sparse-read result code checking logic (`issue#14151 <http://tracker.ceph.com/issues/14151>`_, `pr#7016 <http://github.com/ceph/ceph/pull/7016>`_, xie xingguo)
* osd: fix temp object removal after upgrade (`issue#13862 <http://tracker.ceph.com/issues/13862>`_, `pr#6976 <http://github.com/ceph/ceph/pull/6976>`_, David Zafman)
* osd: fix wip (l_osd_op_wip) perf counter and remove repop_map (`pr#7077 <http://github.com/ceph/ceph/pull/7077>`_, Xinze Chi)
* osd: fix wrongly placed assert and some cleanups (`pr#6766 <http://github.com/ceph/ceph/pull/6766>`_, xiexingguo, xie xingguo)
* osd: KeyValueStore: fix return code of mkfs (`pr#7036 <http://github.com/ceph/ceph/pull/7036>`_, xie xingguo)
* osd: KeyValueStore: fix wrongly placed assert (`issue#14176 <http://tracker.ceph.com/issues/14176>`_, `issue#14178 <http://tracker.ceph.com/issues/14178>`_, `pr#7047 <http://github.com/ceph/ceph/pull/7047>`_, xie xingguo)
* osd: kstore: several small fixes (`issue#14351 <http://tracker.ceph.com/issues/14351>`_, `issue#14352 <http://tracker.ceph.com/issues/14352>`_, `pr#7213 <http://github.com/ceph/ceph/pull/7213>`_, xie xingguo)
* osd: kstore: small fixes to kstore (`issue#14204 <http://tracker.ceph.com/issues/14204>`_, `pr#7095 <http://github.com/ceph/ceph/pull/7095>`_, xie xingguo)
* osd: make list_missing query missing_loc.needs_recovery_map (`pr#6298 <http://github.com/ceph/ceph/pull/6298>`_, Guang Yang)
* osdmap: remove unused local variables (`pr#6864 <http://github.com/ceph/ceph/pull/6864>`_, luo kexue)
* osd: memstore: fix two bugs (`pr#6963 <http://github.com/ceph/ceph/pull/6963>`_, Casey Bodley, Sage Weil)
* osd: misc FileStore fixes (`issue#14192 <http://tracker.ceph.com/issues/14192>`_, `issue#14188 <http://tracker.ceph.com/issues/14188>`_, `issue#14194 <http://tracker.ceph.com/issues/14194>`_, `issue#14187 <http://tracker.ceph.com/issues/14187>`_, `issue#14186 <http://tracker.ceph.com/issues/14186>`_, `pr#7059 <http://github.com/ceph/ceph/pull/7059>`_, xie xingguo)
* osd: misc optimization for map utilization (`pr#6950 <http://github.com/ceph/ceph/pull/6950>`_, Ning Yao)
* osd,mon: log leveldb and rocksdb to ceph log (`pr#6921 <http://github.com/ceph/ceph/pull/6921>`_, Sage Weil)
* osd: Omap small bugs adapted (`pr#6669 <http://github.com/ceph/ceph/pull/6669>`_, Jianpeng Ma, David Zafman)
* osd: optimize the session_handle_reset function (`issue#14182 <http://tracker.ceph.com/issues/14182>`_, `pr#7054 <http://github.com/ceph/ceph/pull/7054>`_, songbaisen)
* osd: OSDService: Fix typo in osdmap comment (`pr#7275 <http://github.com/ceph/ceph/pull/7275>`_, Brad Hubbard)
* osd: os: skip checking pg_meta object existance in FileStore (`pr#6870 <http://github.com/ceph/ceph/pull/6870>`_, Ning Yao)
* osd: PGLog: clean up read_log (`pr#7092 <http://github.com/ceph/ceph/pull/7092>`_, Jie Wang)
* osd: prevent osd_recovery_sleep from causing recovery-thread suicide (`pr#7065 <http://github.com/ceph/ceph/pull/7065>`_, Jianpeng Ma)
* osd: reduce string use in coll_t::calc_str() (`pr#6505 <http://github.com/ceph/ceph/pull/6505>`_, Igor Podoski)
* osd: release related sources when scrub is interrupted (`pr#6744 <http://github.com/ceph/ceph/pull/6744>`_, Jianpeng Ma)
* osd: remove unused OSDMap::set_weightf() (`issue#14369 <http://tracker.ceph.com/issues/14369>`_, `pr#7231 <http://github.com/ceph/ceph/pull/7231>`_, huanwen ren)
* osd: ReplicatedPG: clean up unused function (`pr#7211 <http://github.com/ceph/ceph/pull/7211>`_, Xiaowei Chen)
* osd/ReplicatedPG: fix promotion recency logic (`issue#14320 <http://tracker.ceph.com/issues/14320>`_, `pr#6702 <http://github.com/ceph/ceph/pull/6702>`_, Sage Weil)
* osd: several small cleanups (`pr#7055 <http://github.com/ceph/ceph/pull/7055>`_, xie xingguo)
* osd: shut down if we flap too many times in a short period (`pr#6708 <http://github.com/ceph/ceph/pull/6708>`_, Xiaoxi Chen)
* osd: skip promote for writefull w/ FADVISE_DONTNEED/NOCACHE (`pr#7010 <http://github.com/ceph/ceph/pull/7010>`_, Jianpeng Ma)
* osd: small fixes to memstore (`issue#14228 <http://tracker.ceph.com/issues/14228>`_, `issue#14229 <http://tracker.ceph.com/issues/14229>`_, `issue#14227 <http://tracker.ceph.com/issues/14227>`_, `pr#7107 <http://github.com/ceph/ceph/pull/7107>`_, xie xingguo)
* osd: try evicting after flushing is done (`pr#5630 <http://github.com/ceph/ceph/pull/5630>`_, Zhiqiang Wang)
* osd: use atomic to generate ceph_tid (`pr#7017 <http://github.com/ceph/ceph/pull/7017>`_, Evgeniy Firsov)
* osd: use optimized is_zero in object_stat_sum_t.is_zero() (`pr#7203 <http://github.com/ceph/ceph/pull/7203>`_, Piotr Dałek)
* osd: utime_t, eversion_t, osd_stat_sum_t encoding optimization (`pr#6902 <http://github.com/ceph/ceph/pull/6902>`_, Xinze Chi)
* pybind: add ceph_volume_client interface for Manila and similar frameworks (`pr#6205 <http://github.com/ceph/ceph/pull/6205>`_, John Spray)
* pybind: fix build failure, remove extraneous semicolon in method (`issue#14371 <http://tracker.ceph.com/issues/14371>`_, `pr#7235 <http://github.com/ceph/ceph/pull/7235>`_, Abhishek Lekshmanan)
* pybind/test_rbd: fix test_create_defaults (`issue#14279 <http://tracker.ceph.com/issues/14279>`_, `pr#7155 <http://github.com/ceph/ceph/pull/7155>`_, Josh Durgin)
* qa: disable rbd/qemu-iotests test case 055 on RHEL/CentOSlibrbd: journal replay should honor inter-event dependencies (`issue#14385 <http://tracker.ceph.com/issues/14385>`_, `pr#7272 <http://github.com/ceph/ceph/pull/7272>`_, Jason Dillaman)
* qa/workunits: merge_diff shouldn't attempt to use striping (`issue#14165 <http://tracker.ceph.com/issues/14165>`_, `pr#7041 <http://github.com/ceph/ceph/pull/7041>`_, Jason Dillaman)
* qa/workunits/snaps: move snap tests into fs sub-directory (`pr#6496 <http://github.com/ceph/ceph/pull/6496>`_, Yan, Zheng)
* rados: implement rm --force option to force remove when full (`pr#6202 <http://github.com/ceph/ceph/pull/6202>`_, Xiaowei Chen)
* rbd: additional validation for striping parameters (`pr#6914 <http://github.com/ceph/ceph/pull/6914>`_, Na Xie)
* rbd: add pool name to disambiguate rbd admin socket commands (`pr#6904 <http://github.com/ceph/ceph/pull/6904>`_, wuxiangwei)
* rbd: correct an output string for merge-diff (`pr#7046 <http://github.com/ceph/ceph/pull/7046>`_, Kongming Wu)
* rbd: fix static initialization ordering issues (`pr#6978 <http://github.com/ceph/ceph/pull/6978>`_, Mykola Golub)
* rbd-fuse: image name can not include snap name (`pr#7044 <http://github.com/ceph/ceph/pull/7044>`_, Yongqiang He)
* rbd-fuse: implement mv operation (`pr#6938 <http://github.com/ceph/ceph/pull/6938>`_, wuxiangwei)
* rbd: must specify both of stripe-unit and stripe-count when specifying stripingv2 feature (`pr#7026 <http://github.com/ceph/ceph/pull/7026>`_, Donghai Xu)
* rbd-nbd: add copyright (`pr#7166 <http://github.com/ceph/ceph/pull/7166>`_, Li Wang)
* rbd-nbd: fix up return code handling (`pr#7215 <http://github.com/ceph/ceph/pull/7215>`_, Mykola Golub)
* rbd-nbd: small improvements in logging and forking (`pr#7127 <http://github.com/ceph/ceph/pull/7127>`_, Mykola Golub)
* rbd: rbd order will be place in 22, when set to 0 in the config_opt (`issue#14139 <http://tracker.ceph.com/issues/14139>`_, `issue#14047 <http://tracker.ceph.com/issues/14047>`_, `pr#6886 <http://github.com/ceph/ceph/pull/6886>`_, huanwen ren)
* rbd: striping parameters should support 64bit integers (`pr#6942 <http://github.com/ceph/ceph/pull/6942>`_, Na Xie)
* rbd: use default order from configuration when not specified (`pr#6965 <http://github.com/ceph/ceph/pull/6965>`_, Yunchuan Wen)
* rgw: add a method to purge all associate keys when removing a subuser (`issue#12890 <http://tracker.ceph.com/issues/12890>`_, `pr#6002 <http://github.com/ceph/ceph/pull/6002>`_, Sangdi Xu)
* rgw: add missing error code for admin op API (`pr#7037 <http://github.com/ceph/ceph/pull/7037>`_, Dunrong Huang)
* rgw: add support for "end_marker" parameter for GET on Swift account. (`issue#10682 <http://tracker.ceph.com/issues/10682>`_, `pr#4216 <http://github.com/ceph/ceph/pull/4216>`_, Radoslaw Zarzynski)
* rgw_admin: orphans finish segfaults (`pr#6652 <http://github.com/ceph/ceph/pull/6652>`_, Igor Fedotov)
* rgw: content length (`issue#13582 <http://tracker.ceph.com/issues/13582>`_, `pr#6975 <http://github.com/ceph/ceph/pull/6975>`_, Yehuda Sadeh)
* rgw: delete default zone (`pr#7005 <http://github.com/ceph/ceph/pull/7005>`_, YankunLi)
* rgw: do not abort radowgw server when using admin op API with bad parameters  (`issue#14190 <http://tracker.ceph.com/issues/14190>`_, `issue#14191 <http://tracker.ceph.com/issues/14191>`_, `pr#7063 <http://github.com/ceph/ceph/pull/7063>`_, Dunrong Huang)
* rgw: Drop a debugging message (`pr#7280 <http://github.com/ceph/ceph/pull/7280>`_, Pete Zaitcev)
* rgw: fix a typo in init-radosgw (`pr#6817 <http://github.com/ceph/ceph/pull/6817>`_, Zhi Zhang)
* rgw: fix compilation warning (`pr#7160 <http://github.com/ceph/ceph/pull/7160>`_, Yehuda Sadeh)
* rgw: fix wrong check for parse() return (`pr#6797 <http://github.com/ceph/ceph/pull/6797>`_, Dunrong Huang)
* rgw: let radosgw-admin bucket stats return a standard josn (`pr#7029 <http://github.com/ceph/ceph/pull/7029>`_, Ruifeng Yang)
* rgw: modify command stucking when operating radosgw-admin metadata list user (`pr#7032 <http://github.com/ceph/ceph/pull/7032>`_, Peiyang Liu)
* rgw: modify documents and help infos' descriptions to the usage of option date when executing command "log show" (`pr#6080 <http://github.com/ceph/ceph/pull/6080>`_, Kongming Wu)
* rgw: Parse --subuser better (`pr#7279 <http://github.com/ceph/ceph/pull/7279>`_, Pete Zaitcev)
* rgw: radosgw-admin bucket check --fix not work (`pr#7093 <http://github.com/ceph/ceph/pull/7093>`_, Weijun Duan)
* rgw: warn on suspicious civetweb frontend parameters (`pr#6944 <http://github.com/ceph/ceph/pull/6944>`_, Matt Benjamin)
* rocksdb: remove rdb sources from dist tarball (`issue#13554 <http://tracker.ceph.com/issues/13554>`_, `pr#7105 <http://github.com/ceph/ceph/pull/7105>`_, Venky Shankar)
* stringify outputted error code and fix unmatched parentheses. (`pr#6998 <http://github.com/ceph/ceph/pull/6998>`_, xie.xingguo, xie xingguo)
* test/librbd/fsx: Use c++11 std::mt19937 generator instead of random_r() (`pr#6332 <http://github.com/ceph/ceph/pull/6332>`_, John Coyle)
* test/mon/osd-erasure-code-profile: pick new mon port (`pr#7161 <http://github.com/ceph/ceph/pull/7161>`_, Sage Weil)
* tests: add const for ec test (`pr#6911 <http://github.com/ceph/ceph/pull/6911>`_, Michal Jarzabek)
* tests: configure with rocksdb by default (`issue#14220 <http://tracker.ceph.com/issues/14220>`_, `pr#7100 <http://github.com/ceph/ceph/pull/7100>`_, Loic Dachary)
* tests: Fix for make check. (`pr#7102 <http://github.com/ceph/ceph/pull/7102>`_, David Zafman)
* tests: notification slave needs to wait for master (`issue#13810 <http://tracker.ceph.com/issues/13810>`_, `pr#7220 <http://github.com/ceph/ceph/pull/7220>`_, Jason Dillaman)
* tests: snap rename and rebuild object map in client update test (`pr#7224 <http://github.com/ceph/ceph/pull/7224>`_, Jason Dillaman)
* tests: unittest_bufferlist: fix hexdump test (`pr#7152 <http://github.com/ceph/ceph/pull/7152>`_, Sage Weil)
* tests: unittest_ipaddr: fix segv (`pr#7154 <http://github.com/ceph/ceph/pull/7154>`_, Sage Weil)
* tools: ceph_monstore_tool: add inflate-pgmap command (`issue#14217 <http://tracker.ceph.com/issues/14217>`_, `pr#7097 <http://github.com/ceph/ceph/pull/7097>`_, Kefu Chai)
* tools: monstore: add 'show-versions' command. (`pr#7073 <http://github.com/ceph/ceph/pull/7073>`_, Cilang Zhao)

v10.0.2
=======

This development release includes a raft of changes and improvements
for Jewel.  Key additions include CephFS scrub/repair improvements, an
AIX and Solaris port of librados, many librbd journaling additions and
fixes, extended per-pool options, and NBD driver for RBD (rbd-nbd)
that allows librbd to present a kernel-level block device on Linux,
multitenancy support for RGW, RGW bucket lifecycle support, RGW
support for Swift static large objects (SLO), and RGW support for
Swift bulk delete.

There are also lots of smaller optimizations and performance fixes
going in all over the tree, particular in the OSD and common code.

Notable Changes
---------------

* auth: fail if rotating key is missing (do not spam log) (`pr#6473 <http://github.com/ceph/ceph/pull/6473>`_, Qiankun Zheng)
* auth: fix crash when bad keyring is passed (`pr#6698 <http://github.com/ceph/ceph/pull/6698>`_, Dunrong Huang)
* auth: make keyring without mon entity type return -EACCES (`pr#5734 <http://github.com/ceph/ceph/pull/5734>`_, Xiaowei Chen)
* buffer: make usable outside of ceph source again (`pr#6863 <http://github.com/ceph/ceph/pull/6863>`_, Josh Durgin)
* build: cmake check fixes (`pr#6787 <http://github.com/ceph/ceph/pull/6787>`_, Orit Wasserman)
* build: fix bz2-dev dependency (`pr#6948 <http://github.com/ceph/ceph/pull/6948>`_, Samuel Just)
* build: Gentoo: _FORTIFY_SOURCE fix. (`issue#13920 <http://tracker.ceph.com/issues/13920>`_, `pr#6739 <http://github.com/ceph/ceph/pull/6739>`_, Robin H. Johnson)
* build/ops: systemd ceph-disk unit must not assume /bin/flock (`issue#13975 <http://tracker.ceph.com/issues/13975>`_, `pr#6803 <http://github.com/ceph/ceph/pull/6803>`_, Loic Dachary)
* ceph-detect-init: Ubuntu >= 15.04 uses systemd (`pr#6873 <http://github.com/ceph/ceph/pull/6873>`_, James Page)
* cephfs-data-scan: scan_frags (`pr#5941 <http://github.com/ceph/ceph/pull/5941>`_, John Spray)
* cephfs-data-scan: scrub tag filtering (#12133 and #12145) (`issue#12133 <http://tracker.ceph.com/issues/12133>`_, `issue#12145 <http://tracker.ceph.com/issues/12145>`_, `pr#5685 <http://github.com/ceph/ceph/pull/5685>`_, John Spray)
* ceph-fuse: add process to ceph-fuse --help (`pr#6821 <http://github.com/ceph/ceph/pull/6821>`_, Wei Feng)
* ceph-kvstore-tool: handle bad out file on command line (`pr#6093 <http://github.com/ceph/ceph/pull/6093>`_, Kefu Chai)
* ceph-mds:add --help/-h (`pr#6850 <http://github.com/ceph/ceph/pull/6850>`_, Cilang Zhao)
* ceph_objectstore_bench: fix race condition, bugs (`issue#13516 <http://tracker.ceph.com/issues/13516>`_, `pr#6681 <http://github.com/ceph/ceph/pull/6681>`_, Igor Fedotov)
* ceph.spec.in: add BuildRequires: systemd (`issue#13860 <http://tracker.ceph.com/issues/13860>`_, `pr#6692 <http://github.com/ceph/ceph/pull/6692>`_, Nathan Cutler)
* client: a better check for MDS availability (`pr#6253 <http://github.com/ceph/ceph/pull/6253>`_, John Spray)
* client: close mds sessions in shutdown() (`pr#6269 <http://github.com/ceph/ceph/pull/6269>`_, John Spray)
* client: don't invalidate page cache when inode is no longer used (`pr#6380 <http://github.com/ceph/ceph/pull/6380>`_, Yan, Zheng)
* client: modify a word in log (`pr#6906 <http://github.com/ceph/ceph/pull/6906>`_, YongQiang He)
* cls/cls_rbd.cc: fix misused metadata_name_from_key (`issue#13922 <http://tracker.ceph.com/issues/13922>`_, `pr#6661 <http://github.com/ceph/ceph/pull/6661>`_, Xiaoxi Chen)
* cmake: Add common/PluginRegistry.cc to CMakeLists.txt (`pr#6805 <http://github.com/ceph/ceph/pull/6805>`_, Pete Zaitcev)
* cmake: add rgw_basic_types.cc to librgw.a (`pr#6786 <http://github.com/ceph/ceph/pull/6786>`_, Orit Wasserman)
* cmake: add TracepointProvider.cc to libcommon (`pr#6823 <http://github.com/ceph/ceph/pull/6823>`_, Orit Wasserman)
* cmake: define STRERROR_R_CHAR_P for GNU-specific strerror_r (`pr#6751 <http://github.com/ceph/ceph/pull/6751>`_, Ilya Dryomov)
* cmake: update for recent librbd changes (`pr#6715 <http://github.com/ceph/ceph/pull/6715>`_, John Spray)
* cmake: update for recent rbd changes (`pr#6818 <http://github.com/ceph/ceph/pull/6818>`_, Mykola Golub)
* common: add generic plugin infrastructure (`pr#6696 <http://github.com/ceph/ceph/pull/6696>`_, Sage Weil)
* common: add latency perf counter for finisher (`pr#6175 <http://github.com/ceph/ceph/pull/6175>`_, Xinze Chi)
* common: buffer: add cached_crc and cached_crc_adjust counts to perf dump (`pr#6535 <http://github.com/ceph/ceph/pull/6535>`_, Ning Yao)
* common: buffer: remove unneeded list destructor (`pr#6456 <http://github.com/ceph/ceph/pull/6456>`_, Michal Jarzabek)
* common/ceph_context.cc:fix order of initialisers (`pr#6838 <http://github.com/ceph/ceph/pull/6838>`_, Michal Jarzabek)
* common: don't reverse hobject_t hash bits when zero (`pr#6653 <http://github.com/ceph/ceph/pull/6653>`_, Piotr Dałek)
* common: log: Assign LOG_DEBUG priority to syslog calls (`issue#13993 <http://tracker.ceph.com/issues/13993>`_, `pr#6815 <http://github.com/ceph/ceph/pull/6815>`_, Brad Hubbard)
* common: log: predict log message buffer allocation size (`pr#6641 <http://github.com/ceph/ceph/pull/6641>`_, Adam Kupczyk)
* common: optimize debug logging code (`pr#6441 <http://github.com/ceph/ceph/pull/6441>`_, Adam Kupczyk)
* common: perf counter for bufferlist history total alloc (`pr#6198 <http://github.com/ceph/ceph/pull/6198>`_, Xinze Chi)
* common: reduce CPU usage by making stringstream in stringify function thread local (`pr#6543 <http://github.com/ceph/ceph/pull/6543>`_, Evgeniy Firsov)
* common: re-enable backtrace support (`pr#6771 <http://github.com/ceph/ceph/pull/6771>`_, Jason Dillaman)
* common: SubProcess: fix multiple definition bug (`pr#6790 <http://github.com/ceph/ceph/pull/6790>`_, Yunchuan Wen)
* common: use namespace instead of subclasses for buffer (`pr#6686 <http://github.com/ceph/ceph/pull/6686>`_, Michal Jarzabek)
* configure.ac: macro fix (`pr#6769 <http://github.com/ceph/ceph/pull/6769>`_, Igor Podoski)
* doc: admin/build-doc: add lxml dependencies on debian (`pr#6610 <http://github.com/ceph/ceph/pull/6610>`_, Ken Dreyer)
* doc/cephfs/posix: update (`pr#6922 <http://github.com/ceph/ceph/pull/6922>`_, Sage Weil)
* doc: CodingStyle: fix broken URLs (`pr#6733 <http://github.com/ceph/ceph/pull/6733>`_, Kefu Chai)
* doc: correct typo 'restared' to 'restarted' (`pr#6734 <http://github.com/ceph/ceph/pull/6734>`_, Yilong Zhao)
* doc/dev/index: refactor/reorg (`pr#6792 <http://github.com/ceph/ceph/pull/6792>`_, Nathan Cutler)
* doc/dev/index.rst: begin writing Contributing to Ceph (`pr#6727 <http://github.com/ceph/ceph/pull/6727>`_, Nathan Cutler)
* doc/dev/index.rst: fix headings (`pr#6780 <http://github.com/ceph/ceph/pull/6780>`_, Nathan Cutler)
* doc: dev: introduction to tests (`pr#6910 <http://github.com/ceph/ceph/pull/6910>`_, Loic Dachary)
* doc: file must be empty when writing layout fields of file use "setfattr" (`pr#6848 <http://github.com/ceph/ceph/pull/6848>`_, Cilang Zhao)
* doc: Fixed incorrect name of a "List Multipart Upload Parts" Response Entity (`issue#14003 <http://tracker.ceph.com/issues/14003>`_, `pr#6829 <http://github.com/ceph/ceph/pull/6829>`_, Lenz Grimmer)
* doc: Fixes a spelling error (`pr#6705 <http://github.com/ceph/ceph/pull/6705>`_, Jeremy Qian)
* doc: fix typo in cephfs/quota (`pr#6745 <http://github.com/ceph/ceph/pull/6745>`_, Drunkard Zhang)
* doc: fix typo in developer guide (`pr#6943 <http://github.com/ceph/ceph/pull/6943>`_, Nathan Cutler)
* doc: INSTALL redirect to online documentation (`pr#6749 <http://github.com/ceph/ceph/pull/6749>`_, Loic Dachary)
* doc: little improvements for troubleshooting scrub issues (`pr#6827 <http://github.com/ceph/ceph/pull/6827>`_, Mykola Golub)
* doc: Modified a note section in rbd-snapshot doc. (`pr#6908 <http://github.com/ceph/ceph/pull/6908>`_, Nilamdyuti Goswami)
* doc: note that cephfs auth stuff is new in jewel (`pr#6858 <http://github.com/ceph/ceph/pull/6858>`_, John Spray)
* doc: osd: s/schedued/scheduled/ (`pr#6872 <http://github.com/ceph/ceph/pull/6872>`_, Loic Dachary)
* doc: remove unnecessary period in headline (`pr#6775 <http://github.com/ceph/ceph/pull/6775>`_, Marc Koderer)
* doc: rst style fix for pools document (`pr#6816 <http://github.com/ceph/ceph/pull/6816>`_, Drunkard Zhang)
* doc: Update list of admin/build-doc dependencies (`issue#14070 <http://tracker.ceph.com/issues/14070>`_, `pr#6934 <http://github.com/ceph/ceph/pull/6934>`_, Nathan Cutler)
* init-ceph: do umount when the path exists. (`pr#6866 <http://github.com/ceph/ceph/pull/6866>`_, Xiaoxi Chen)
* journal: disconnect watch after watch error (`issue#14168 <http://tracker.ceph.com/issues/14168>`_, `pr#7113 <http://github.com/ceph/ceph/pull/7113>`_, Jason Dillaman)
* journal: fire replay complete event after reading last object (`issue#13924 <http://tracker.ceph.com/issues/13924>`_, `pr#6762 <http://github.com/ceph/ceph/pull/6762>`_, Jason Dillaman)
* journal: support replaying beyond skipped splay objects (`pr#6687 <http://github.com/ceph/ceph/pull/6687>`_, Jason Dillaman)
* librados: aix gcc librados port (`pr#6675 <http://github.com/ceph/ceph/pull/6675>`_, Rohan Mars)
* librados: avoid malloc(0) (which can return NULL on some platforms) (`issue#13944 <http://tracker.ceph.com/issues/13944>`_, `pr#6779 <http://github.com/ceph/ceph/pull/6779>`_, Dan Mick)
* librados: clean up Objecter.h (`pr#6731 <http://github.com/ceph/ceph/pull/6731>`_, Jie Wang)
* librados: include/rados/librados.h: fix typo (`pr#6741 <http://github.com/ceph/ceph/pull/6741>`_, Nathan Cutler)
* librbd: automatically flush IO after blocking write operations (`issue#13913 <http://tracker.ceph.com/issues/13913>`_, `pr#6742 <http://github.com/ceph/ceph/pull/6742>`_, Jason Dillaman)
* librbd: better handling of exclusive lock transition period (`pr#7204 <http://github.com/ceph/ceph/pull/7204>`_, Jason Dillaman)
* librbd: check for presence of journal before attempting to remove  (`issue#13912 <http://tracker.ceph.com/issues/13912>`_, `pr#6737 <http://github.com/ceph/ceph/pull/6737>`_, Jason Dillaman)
* librbd: clear error when older OSD doesn't support image flags (`issue#14122 <http://tracker.ceph.com/issues/14122>`_, `pr#7035 <http://github.com/ceph/ceph/pull/7035>`_, Jason Dillaman)
* librbd: correct include guard in RenameRequest.h (`pr#7143 <http://github.com/ceph/ceph/pull/7143>`_, Jason Dillaman)
* librbd: correct issues discovered during teuthology testing (`issue#14108 <http://tracker.ceph.com/issues/14108>`_, `issue#14107 <http://tracker.ceph.com/issues/14107>`_, `pr#6974 <http://github.com/ceph/ceph/pull/6974>`_, Jason Dillaman)
* librbd: correct issues discovered when cache is disabled (`issue#14123 <http://tracker.ceph.com/issues/14123>`_, `pr#6979 <http://github.com/ceph/ceph/pull/6979>`_, Jason Dillaman)
* librbd: correct race conditions discovered during unit testing (`issue#14060 <http://tracker.ceph.com/issues/14060>`_, `pr#6923 <http://github.com/ceph/ceph/pull/6923>`_, Jason Dillaman)
* librbd: disable copy-on-read when not exclusive lock owner (`issue#14167 <http://tracker.ceph.com/issues/14167>`_, `pr#7129 <http://github.com/ceph/ceph/pull/7129>`_, Jason Dillaman)
* librbd: do not ignore self-managed snapshot release result (`issue#14170 <http://tracker.ceph.com/issues/14170>`_, `pr#7043 <http://github.com/ceph/ceph/pull/7043>`_, Jason Dillaman)
* librbd: ensure copy-on-read requests are complete prior to closing parent image  (`pr#6740 <http://github.com/ceph/ceph/pull/6740>`_, Jason Dillaman)
* librbd: ensure librados callbacks are flushed prior to destroying (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `pr#7040 <http://github.com/ceph/ceph/pull/7040>`_, Jason Dillaman)
* librbd: fix journal iohint (`pr#6917 <http://github.com/ceph/ceph/pull/6917>`_, Jianpeng Ma)
* librbd: fix known test case race condition failures (`issue#13969 <http://tracker.ceph.com/issues/13969>`_, `pr#6800 <http://github.com/ceph/ceph/pull/6800>`_, Jason Dillaman)
* librbd: fix merge-diff for >2GB diff-files (`issue#14030 <http://tracker.ceph.com/issues/14030>`_, `pr#6889 <http://github.com/ceph/ceph/pull/6889>`_, Yunchuan Wen)
* librbd: fix test case race condition for journaling ops (`pr#6877 <http://github.com/ceph/ceph/pull/6877>`_, Jason Dillaman)
* librbd: fix tracepoint parameter in diff_iterate (`pr#6892 <http://github.com/ceph/ceph/pull/6892>`_, Yunchuan Wen)
* librbd: image refresh code paths converted to async state machines (`pr#6859 <http://github.com/ceph/ceph/pull/6859>`_, Jason Dillaman)
* librbd: include missing header for bool type (`pr#6798 <http://github.com/ceph/ceph/pull/6798>`_, Mykola Golub)
* librbd: initial collection of state machine unit tests (`pr#6703 <http://github.com/ceph/ceph/pull/6703>`_, Jason Dillaman)
* librbd: integrate journaling for maintenance operations (`pr#6625 <http://github.com/ceph/ceph/pull/6625>`_, Jason Dillaman)
* librbd: journaling-related lock dependency cleanup (`pr#6777 <http://github.com/ceph/ceph/pull/6777>`_, Jason Dillaman)
* librbd: not necessary to hold owner_lock while releasing snap id (`issue#13914 <http://tracker.ceph.com/issues/13914>`_, `pr#6736 <http://github.com/ceph/ceph/pull/6736>`_, Jason Dillaman)
* librbd: only send signal when AIO completions queue empty (`pr#6729 <http://github.com/ceph/ceph/pull/6729>`_, Jianpeng Ma)
* librbd: optionally validate new RBD pools for snapshot support (`issue#13633 <http://tracker.ceph.com/issues/13633>`_, `pr#6925 <http://github.com/ceph/ceph/pull/6925>`_, Jason Dillaman)
* librbd: partial revert of commit 9b0e359 (`issue#13969 <http://tracker.ceph.com/issues/13969>`_, `pr#6789 <http://github.com/ceph/ceph/pull/6789>`_, Jason Dillaman)
* librbd: properly handle replay of snap remove RPC message (`issue#14164 <http://tracker.ceph.com/issues/14164>`_, `pr#7042 <http://github.com/ceph/ceph/pull/7042>`_, Jason Dillaman)
* librbd: reduce verbosity of common error condition logging (`issue#14234 <http://tracker.ceph.com/issues/14234>`_, `pr#7114 <http://github.com/ceph/ceph/pull/7114>`_, Jason Dillaman)
* librbd: simplify IO method signatures for 32bit environments (`pr#6700 <http://github.com/ceph/ceph/pull/6700>`_, Jason Dillaman)
* librbd: support eventfd for AIO completion notifications (`pr#5465 <http://github.com/ceph/ceph/pull/5465>`_, Haomai Wang)
* mailmap: add UMCloud affiliation (`pr#6820 <http://github.com/ceph/ceph/pull/6820>`_, Jiaying Ren)
* mailmap: Jewel updates (`pr#6750 <http://github.com/ceph/ceph/pull/6750>`_, Abhishek Lekshmanan)
* makefiles: remove bz2-dev from dependencies (`issue#13981 <http://tracker.ceph.com/issues/13981>`_, `pr#6939 <http://github.com/ceph/ceph/pull/6939>`_, Piotr Dałek)
* mds: add 'p' flag in auth caps to control setting pool in layout (`pr#6567 <http://github.com/ceph/ceph/pull/6567>`_, John Spray)
* mds: fix client capabilities during reconnect (client.XXXX isn't responding to mclientcaps(revoke)) (`issue#11482 <http://tracker.ceph.com/issues/11482>`_, `pr#6432 <http://github.com/ceph/ceph/pull/6432>`_, Yan, Zheng)
* mds: fix setvxattr (broken in a536d114) (`issue#14029 <http://tracker.ceph.com/issues/14029>`_, `pr#6941 <http://github.com/ceph/ceph/pull/6941>`_, John Spray)
* mds: repair the command option "--hot-standby" (`pr#6454 <http://github.com/ceph/ceph/pull/6454>`_, Wei Feng)
* mds: tear down connections from `tell` commands (`issue#14048 <http://tracker.ceph.com/issues/14048>`_, `pr#6933 <http://github.com/ceph/ceph/pull/6933>`_, John Spray)
* mon: fix ceph df pool available calculation for 0-weighted OSDs (`pr#6660 <http://github.com/ceph/ceph/pull/6660>`_, Chengyuan Li)
* mon: fix routed_request_tids leak (`pr#6102 <http://github.com/ceph/ceph/pull/6102>`_, Ning Yao)
* mon: support min_down_reporter by subtree level (default by host) (`pr#6709 <http://github.com/ceph/ceph/pull/6709>`_, Xiaoxi Chen)
* mount.ceph: memory leaks (`pr#6905 <http://github.com/ceph/ceph/pull/6905>`_, Qiankun Zheng)
* osd: add osd op queue latency perfcounter (`pr#5793 <http://github.com/ceph/ceph/pull/5793>`_, Haomai Wang)
* osd: Allow repair of history.last_epoch_started using config (`pr#6793 <http://github.com/ceph/ceph/pull/6793>`_, David Zafman)
* osd: avoid duplicate op->mark_started in ReplicatedBackend (`pr#6689 <http://github.com/ceph/ceph/pull/6689>`_, Jacek J. Łakis)
* osd: cancel failure reports if we fail to rebind network (`pr#6278 <http://github.com/ceph/ceph/pull/6278>`_, Xinze Chi)
* osd: correctly handle small osd_scrub_interval_randomize_ratio (`pr#7147 <http://github.com/ceph/ceph/pull/7147>`_, Samuel Just)
* osd: defer decoding of MOSDRepOp/MOSDRepOpReply (`pr#6503 <http://github.com/ceph/ceph/pull/6503>`_, Xinze Chi)
* osd: don't update epoch and rollback_info objects attrs if there is no need (`pr#6555 <http://github.com/ceph/ceph/pull/6555>`_, Ning Yao)
* osd: dump number of missing objects for each peer with pg query (`pr#6058 <http://github.com/ceph/ceph/pull/6058>`_, Guang Yang)
* osd: enable perfcounters on sharded work queue mutexes (`pr#6455 <http://github.com/ceph/ceph/pull/6455>`_, Jacek J. Łakis)
* osd: FileJournal: reduce locking scope in write_aio_bl (`issue#12789 <http://tracker.ceph.com/issues/12789>`_, `pr#5670 <http://github.com/ceph/ceph/pull/5670>`_, Zhi Zhang)
* osd: FileStore: remove __SWORD_TYPE dependency (`pr#6263 <http://github.com/ceph/ceph/pull/6263>`_, John Coyle)
* osd: fix FileStore::_destroy_collection error return code (`pr#6612 <http://github.com/ceph/ceph/pull/6612>`_, Ruifeng Yang)
* osd: fix incorrect throttle in WBThrottle (`pr#6713 <http://github.com/ceph/ceph/pull/6713>`_, Zhang Huan)
* osd: fix MOSDRepScrub reference counter in replica_scrub (`pr#6730 <http://github.com/ceph/ceph/pull/6730>`_, Jie Wang)
* osd: fix rollback_info_trimmed_to before index() (`issue#13965 <http://tracker.ceph.com/issues/13965>`_, `pr#6801 <http://github.com/ceph/ceph/pull/6801>`_, Samuel Just)
* osd: fix trivial scrub bug (`pr#6533 <http://github.com/ceph/ceph/pull/6533>`_, Li Wang)
* osd: KeyValueStore: don't queue NULL context (`pr#6783 <http://github.com/ceph/ceph/pull/6783>`_, Haomai Wang)
* osd: make backend and block device code a bit more generic (`pr#6759 <http://github.com/ceph/ceph/pull/6759>`_, Sage Weil)
* osd: move newest decode version of MOSDOp and MOSDOpReply to the front (`pr#6642 <http://github.com/ceph/ceph/pull/6642>`_, Jacek J. Łakis)
* osd: pg_pool_t: add dictionary for pool options (`issue#13077 <http://tracker.ceph.com/issues/13077>`_, `pr#6081 <http://github.com/ceph/ceph/pull/6081>`_, Mykola Golub)
* osd: reduce memory consumption of some structs (`pr#6475 <http://github.com/ceph/ceph/pull/6475>`_, Piotr Dałek)
* osd: release the message throttle when OpRequest unregistered (`issue#14248 <http://tracker.ceph.com/issues/14248>`_, `pr#7148 <http://github.com/ceph/ceph/pull/7148>`_, Samuel Just)
* osd: remove __SWORD_TYPE dependency (`pr#6262 <http://github.com/ceph/ceph/pull/6262>`_, John Coyle)
* osd: slightly reduce actual size of pg_log_entry_t (`pr#6690 <http://github.com/ceph/ceph/pull/6690>`_, Piotr Dałek)
* osd: support pool level recovery_priority and recovery_op_priority (`pr#5953 <http://github.com/ceph/ceph/pull/5953>`_, Guang Yang)
* osd: use pg id (without shard) when referring the PG (`pr#6236 <http://github.com/ceph/ceph/pull/6236>`_, Guang Yang)
* packaging: add build dependency on python devel package (`pr#7205 <http://github.com/ceph/ceph/pull/7205>`_, Josh Durgin)
* pybind/cephfs: add symlink and its unit test (`pr#6323 <http://github.com/ceph/ceph/pull/6323>`_, Shang Ding)
* pybind: decode empty string in conf_parse_argv() correctly (`pr#6711 <http://github.com/ceph/ceph/pull/6711>`_, Josh Durgin)
* pybind: Implementation of rados_ioctx_snapshot_rollback (`pr#6878 <http://github.com/ceph/ceph/pull/6878>`_, Florent Manens)
* pybind: port the rbd bindings to Cython (`issue#13115 <http://tracker.ceph.com/issues/13115>`_, `pr#6768 <http://github.com/ceph/ceph/pull/6768>`_, Hector Martin)
* pybind: support ioctx:exec (`pr#6795 <http://github.com/ceph/ceph/pull/6795>`_, Noah Watkins)
* qa: erasure-code benchmark plugin selection (`pr#6685 <http://github.com/ceph/ceph/pull/6685>`_, Loic Dachary)
* qa/krbd: Expunge generic/247 (`pr#6831 <http://github.com/ceph/ceph/pull/6831>`_, Douglas Fuller)
* qa/workunits/cephtool/test.sh: false positive fail on /tmp/obj1. (`pr#6837 <http://github.com/ceph/ceph/pull/6837>`_, Robin H. Johnson)
* qa/workunits/cephtool/test.sh: no ./ (`pr#6748 <http://github.com/ceph/ceph/pull/6748>`_, Sage Weil)
* qa/workunits/rbd: rbd-nbd test should use sudo for map/unmap ops (`issue#14221 <http://tracker.ceph.com/issues/14221>`_, `pr#7101 <http://github.com/ceph/ceph/pull/7101>`_, Jason Dillaman)
* rados: bench: fix off-by-one to avoid writing past object_size (`pr#6677 <http://github.com/ceph/ceph/pull/6677>`_, Tao Chang)
* rbd: add --object-size option, deprecate --order (`issue#12112 <http://tracker.ceph.com/issues/12112>`_, `pr#6830 <http://github.com/ceph/ceph/pull/6830>`_, Vikhyat Umrao)
* rbd: add RBD pool mirroring configuration API + CLI (`pr#6129 <http://github.com/ceph/ceph/pull/6129>`_, Jason Dillaman)
* rbd: fix build with "--without-rbd" (`issue#14058 <http://tracker.ceph.com/issues/14058>`_, `pr#6899 <http://github.com/ceph/ceph/pull/6899>`_, Piotr Dałek)
* rbd: journal: configuration via conf, cli, api and some fixes (`pr#6665 <http://github.com/ceph/ceph/pull/6665>`_, Mykola Golub)
* rbd: merge_diff test should use new --object-size parameter instead of --order (`issue#14106 <http://tracker.ceph.com/issues/14106>`_, `pr#6972 <http://github.com/ceph/ceph/pull/6972>`_, Na Xie, Jason Dillaman)
* rbd-nbd: network block device (NBD) support for RBD  (`pr#6657 <http://github.com/ceph/ceph/pull/6657>`_, Yunchuan Wen, Li Wang)
* rbd: output formatter may not be closed upon error (`issue#13711 <http://tracker.ceph.com/issues/13711>`_, `pr#6706 <http://github.com/ceph/ceph/pull/6706>`_, xie xingguo)
* rgw: add a missing cap type (`pr#6774 <http://github.com/ceph/ceph/pull/6774>`_, Yehuda Sadeh)
* rgw: add an inspection to the field of type when assigning user caps (`pr#6051 <http://github.com/ceph/ceph/pull/6051>`_, Kongming Wu)
* rgw: add LifeCycle feature (`pr#6331 <http://github.com/ceph/ceph/pull/6331>`_, Ji Chen)
* rgw: add support for Static Large Objects of Swift API (`issue#12886 <http://tracker.ceph.com/issues/12886>`_, `issue#13452 <http://tracker.ceph.com/issues/13452>`_, `pr#6643 <http://github.com/ceph/ceph/pull/6643>`_, Yehuda Sadeh, Radoslaw Zarzynski)
* rgw: fix a glaring syntax error (`pr#6888 <http://github.com/ceph/ceph/pull/6888>`_, Pavan Rallabhandi)
* rgw: fix the build failure (`pr#6927 <http://github.com/ceph/ceph/pull/6927>`_, Kefu Chai)
* rgw: multitenancy support (`pr#6784 <http://github.com/ceph/ceph/pull/6784>`_, Yehuda Sadeh, Pete Zaitcev)
* rgw: Remove unused code in PutMetadataAccount:execute (`pr#6668 <http://github.com/ceph/ceph/pull/6668>`_, Pete Zaitcev)
* rgw: remove unused variable in RGWPutMetadataBucket::execute (`pr#6735 <http://github.com/ceph/ceph/pull/6735>`_, Radoslaw Zarzynski)
* rgw/rgw_resolve: fallback to res_query when res_nquery not implemented (`pr#6292 <http://github.com/ceph/ceph/pull/6292>`_, John Coyle)
* rgw: static large objects (Radoslaw Zarzynski, Yehuda Sadeh)
* rgw: swift bulk delete (Radoslaw Zarzynski)
* systemd: start/stop/restart ceph services by daemon type (`issue#13497 <http://tracker.ceph.com/issues/13497>`_, `pr#6276 <http://github.com/ceph/ceph/pull/6276>`_, Zhi Zhang)
* sysvinit: allow custom cluster names (`pr#6732 <http://github.com/ceph/ceph/pull/6732>`_, Richard Chan)
* test/encoding/readable.sh fix (`pr#6714 <http://github.com/ceph/ceph/pull/6714>`_, Igor Podoski)
* test: fix osd-scrub-snaps.sh (`pr#6697 <http://github.com/ceph/ceph/pull/6697>`_, Xinze Chi)
* test/librados/test.cc: clean up EC pools' crush rules too (`issue#13878 <http://tracker.ceph.com/issues/13878>`_, `pr#6788 <http://github.com/ceph/ceph/pull/6788>`_, Loic Dachary, Dan Mick)
* tests: allow object corpus readable test to skip specific incompat instances (`pr#6932 <http://github.com/ceph/ceph/pull/6932>`_, Igor Podoski)
* tests: ceph-helpers assert success getting backfills (`pr#6699 <http://github.com/ceph/ceph/pull/6699>`_, Loic Dachary)
* tests: ceph_test_keyvaluedb_iterators: fix broken test (`pr#6597 <http://github.com/ceph/ceph/pull/6597>`_, Haomai Wang)
* tests: fix failure for osd-scrub-snap.sh (`issue#13986 <http://tracker.ceph.com/issues/13986>`_, `pr#6890 <http://github.com/ceph/ceph/pull/6890>`_, Loic Dachary, Ning Yao)
* tests: fix race condition testing auto scrub (`issue#13592 <http://tracker.ceph.com/issues/13592>`_, `pr#6724 <http://github.com/ceph/ceph/pull/6724>`_, Xinze Chi, Loic Dachary)
* tests: flush op work queue prior to destroying MockImageCtx (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `pr#7002 <http://github.com/ceph/ceph/pull/7002>`_, Jason Dillaman)
* tests: --osd-scrub-load-threshold=2000 for more consistency (`issue#14027 <http://tracker.ceph.com/issues/14027>`_, `pr#6871 <http://github.com/ceph/ceph/pull/6871>`_, Loic Dachary)
* tests: osd-scrub-snaps.sh to display full osd logs on error (`issue#13986 <http://tracker.ceph.com/issues/13986>`_, `pr#6857 <http://github.com/ceph/ceph/pull/6857>`_, Loic Dachary)
* test: use sequential journal_tid for object cacher test (`issue#13877 <http://tracker.ceph.com/issues/13877>`_, `pr#6710 <http://github.com/ceph/ceph/pull/6710>`_, Josh Durgin)
* tools: add cephfs-table-tool 'take_inos' (`pr#6655 <http://github.com/ceph/ceph/pull/6655>`_, John Spray)
* tools: Fix layout handing in cephfs-data-scan (#13898) (`pr#6719 <http://github.com/ceph/ceph/pull/6719>`_, John Spray)
* tools: support printing part cluster map in readable fashion (`issue#13079 <http://tracker.ceph.com/issues/13079>`_, `pr#5921 <http://github.com/ceph/ceph/pull/5921>`_, Bo Cai)
* vstart.sh: add mstart, mstop, mrun wrappers for running multiple vstart-style test clusters out of src tree (`pr#6901 <http://github.com/ceph/ceph/pull/6901>`_, Yehuda Sadeh)

v10.0.1
=======

This is the second development release for the Jewel cycle.
Highlights include some KeyValueDB interface optimizations, initial
journaling support in librbd, MDS scrubbing progress, and a bunch of
OSD optimizations and improvements (removal of an unnecessary CRUSH
calculation in the IO path, sharding for FileStore completions,
improved temperature calculation for cache tiering, fixed
randomization of scrub times, and various optimizations).

Notable Changes
---------------

* build: build internal plugins and classes as modules (`pr#6462 <http://github.com/ceph/ceph/pull/6462>`_, James Page)
* build: fix Jenkins make check errors due to deep-scrub randomization (`pr#6671 <http://github.com/ceph/ceph/pull/6671>`_, David Zafman)
* build/ops: enable CR in CentOS 7 (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6844 <http://github.com/ceph/ceph/pull/6844>`_, Loic Dachary)
* build/ops: rbd-replay moved from ceph-test-dbg to ceph-common-dbg (`issue#13785 <http://tracker.ceph.com/issues/13785>`_, `pr#6578 <http://github.com/ceph/ceph/pull/6578>`_, Loic Dachary)
* ceph-disk: Add destroy and deactivate option (`issue#7454 <http://tracker.ceph.com/issues/7454>`_, `pr#5867 <http://github.com/ceph/ceph/pull/5867>`_, Vicente Cheng)
* ceph-disk: compare parted output with the dereferenced path (`issue#13438 <http://tracker.ceph.com/issues/13438>`_, `pr#6219 <http://github.com/ceph/ceph/pull/6219>`_, Joe Julian)
* ceph-objectstore-tool: fix --dry-run for many ceph-objectstore-tool operations (`pr#6545 <http://github.com/ceph/ceph/pull/6545>`_, David Zafman)
* ceph.spec.in: limit _smp_mflags when lowmem_builder is set in SUSE's OBS (`issue#13858 <http://tracker.ceph.com/issues/13858>`_, `pr#6691 <http://github.com/ceph/ceph/pull/6691>`_, Nathan Cutler)
* ceph_test_msgr: Use send_message instead of keepalive to wakeup connection (`pr#6605 <http://github.com/ceph/ceph/pull/6605>`_, Haomai Wang)
* client: avoid creating orphan object in Client::check_pool_perm() (`issue#13782 <http://tracker.ceph.com/issues/13782>`_, `pr#6603 <http://github.com/ceph/ceph/pull/6603>`_, Yan, Zheng)
* client: use null snapc to check pool permission (`issue#13714 <http://tracker.ceph.com/issues/13714>`_, `pr#6497 <http://github.com/ceph/ceph/pull/6497>`_, Yan, Zheng)
* cmake: add nss as a suffix for pk11pub.h (`pr#6556 <http://github.com/ceph/ceph/pull/6556>`_, Samuel Just)
* cmake: fix files list (`pr#6539 <http://github.com/ceph/ceph/pull/6539>`_, Yehuda Sadeh)
* cmake: librbd and libjournal build fixes (`pr#6557 <http://github.com/ceph/ceph/pull/6557>`_, Ilya Dryomov)
* coc: fix typo in the apt-get command (`pr#6659 <http://github.com/ceph/ceph/pull/6659>`_, Chris Holcombe)
* common: allow enable/disable of optracker at runtime (`pr#5168 <http://github.com/ceph/ceph/pull/5168>`_, Jianpeng Ma)
* common: fix reset max in Throttle using perf reset command (`issue#13517 <http://tracker.ceph.com/issues/13517>`_, `pr#6300 <http://github.com/ceph/ceph/pull/6300>`_, Xinze Chi)
* doc: add v0.80.11 to the release timeline (`pr#6658 <http://github.com/ceph/ceph/pull/6658>`_, Loic Dachary)
* doc: release-notes: draft v0.80.11 release notes (`pr#6374 <http://github.com/ceph/ceph/pull/6374>`_, Loic Dachary)
* doc: release-notes: draft v10.0.0 release notes (`pr#6666 <http://github.com/ceph/ceph/pull/6666>`_, Loic Dachary)
* doc: SubmittingPatches: there is no next; only jewel (`pr#6811 <http://github.com/ceph/ceph/pull/6811>`_, Nathan Cutler)
* doc: Update ceph-disk manual page with new feature deactivate/destroy. (`pr#6637 <http://github.com/ceph/ceph/pull/6637>`_, Vicente Cheng)
* doc: update infernalis release notes (`pr#6575 <http://github.com/ceph/ceph/pull/6575>`_, vasukulkarni)
* fix: use right init_flags to finish CephContext (`pr#6549 <http://github.com/ceph/ceph/pull/6549>`_, Yunchuan Wen)
* init-ceph: use getopt to make option processing more flexible (`issue#3015 <http://tracker.ceph.com/issues/3015>`_, `pr#6089 <http://github.com/ceph/ceph/pull/6089>`_, Nathan Cutler)
* journal: incremental improvements and fixes (`pr#6552 <http://github.com/ceph/ceph/pull/6552>`_, Mykola Golub)
* krbd: remove deprecated --quiet param from udevadm (`issue#13560 <http://tracker.ceph.com/issues/13560>`_, `pr#6394 <http://github.com/ceph/ceph/pull/6394>`_, Jason Dillaman)
* kv: fix bug in kv key optimization (`pr#6511 <http://github.com/ceph/ceph/pull/6511>`_, Sage Weil)
* kv/KineticStore: fix broken split_key (`pr#6574 <http://github.com/ceph/ceph/pull/6574>`_, Haomai Wang)
* kv: optimize and clean up internal key/value interface (`pr#6312 <http://github.com/ceph/ceph/pull/6312>`_, Piotr Dałek, Sage Weil)
* librados: do cleanup (`pr#6488 <http://github.com/ceph/ceph/pull/6488>`_, xie xingguo)
* librados: fix pool alignment API overflow issue (`issue#13715 <http://tracker.ceph.com/issues/13715>`_, `pr#6489 <http://github.com/ceph/ceph/pull/6489>`_, xie xingguo)
* librados: fix potential null pointer access when do pool_snap_list (`issue#13639 <http://tracker.ceph.com/issues/13639>`_, `pr#6422 <http://github.com/ceph/ceph/pull/6422>`_, xie xingguo)
* librados: fix PromoteOn2ndRead test for EC (`pr#6373 <http://github.com/ceph/ceph/pull/6373>`_, Sage Weil)
* librados: fix rare race where pool op callback may hang forever (`issue#13642 <http://tracker.ceph.com/issues/13642>`_, `pr#6426 <http://github.com/ceph/ceph/pull/6426>`_, xie xingguo)
* librados: Solaris port (`pr#6416 <http://github.com/ceph/ceph/pull/6416>`_, Rohan Mars)
* librbd: flush and invalidate cache via admin socket (`issue#2468 <http://tracker.ceph.com/issues/2468>`_, `pr#6453 <http://github.com/ceph/ceph/pull/6453>`_, Mykola Golub)
* librbd: integrate journaling support for IO operations (`pr#6541 <http://github.com/ceph/ceph/pull/6541>`_, Jason Dillaman)
* librbd: perf counters might not be initialized on error (`issue#13740 <http://tracker.ceph.com/issues/13740>`_, `pr#6523 <http://github.com/ceph/ceph/pull/6523>`_, Jason Dillaman)
* librbd: perf section name: use hyphen to separate components (`issue#13719 <http://tracker.ceph.com/issues/13719>`_, `pr#6516 <http://github.com/ceph/ceph/pull/6516>`_, Mykola Golub)
* librbd: resize should only update image size within header (`issue#13674 <http://tracker.ceph.com/issues/13674>`_, `pr#6447 <http://github.com/ceph/ceph/pull/6447>`_, Jason Dillaman)
* librbd: start perf counters after id is initialized (`issue#13720 <http://tracker.ceph.com/issues/13720>`_, `pr#6494 <http://github.com/ceph/ceph/pull/6494>`_, Mykola Golub)
* mailmap: revise organization (`pr#6519 <http://github.com/ceph/ceph/pull/6519>`_, Li Wang)
* mailmap: Ubuntu Kylin name changed to Kylin Cloud (`pr#6532 <http://github.com/ceph/ceph/pull/6532>`_, Loic Dachary)
* mailmap: update .organizationmap (`pr#6565 <http://github.com/ceph/ceph/pull/6565>`_, chenji-kael)
* mailmap: updates for infernalis. (`pr#6495 <http://github.com/ceph/ceph/pull/6495>`_, Yann Dupont)
* mailmap: updates (`pr#6594 <http://github.com/ceph/ceph/pull/6594>`_, chenji-kael)
* mds: fix scrub_path (`pr#6684 <http://github.com/ceph/ceph/pull/6684>`_, John Spray)
* mds: properly set STATE_STRAY/STATE_ORPHAN for stray dentry/inode (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#6553 <http://github.com/ceph/ceph/pull/6553>`_, Yan, Zheng)
* mds: ScrubStack and "tag path" command (`pr#5662 <http://github.com/ceph/ceph/pull/5662>`_, Yan, Zheng, John Spray, Greg Farnum)
* mon: block 'ceph osd pg-temp ...' if pg_temp update is already pending (`pr#6704 <http://github.com/ceph/ceph/pull/6704>`_, Sage Weil)
* mon: don't require OSD W for MRemoveSnaps (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#6601 <http://github.com/ceph/ceph/pull/6601>`_, John Spray)
* mon: initialize recorded election epoch properly even when standalone (`issue#13627 <http://tracker.ceph.com/issues/13627>`_, `pr#6407 <http://github.com/ceph/ceph/pull/6407>`_, huanwen ren)
* mon: revert MonitorDBStore's WholeStoreIteratorImpl::get (`issue#13742 <http://tracker.ceph.com/issues/13742>`_, `pr#6522 <http://github.com/ceph/ceph/pull/6522>`_, Piotr Dałek)
* msg/async: let receiver ack message ASAP (`pr#6478 <http://github.com/ceph/ceph/pull/6478>`_, Haomai Wang)
* msg/async: support of non-block connect in async messenger (`issue#12802 <http://tracker.ceph.com/issues/12802>`_, `pr#5848 <http://github.com/ceph/ceph/pull/5848>`_, Jianhui Yuan)
* msg/async: will crash if enabling async msg because of an assertion (`pr#6640 <http://github.com/ceph/ceph/pull/6640>`_, Zhi Zhang)
* osd: avoid calculating crush mapping for most ops (`pr#6371 <http://github.com/ceph/ceph/pull/6371>`_, Sage Weil)
* osd: avoid double-check for replaying and can_checkpoint() in FileStore::_check_replay_guard (`pr#6471 <http://github.com/ceph/ceph/pull/6471>`_, Ning Yao)
* osd: call on_new_interval on newly split child PG (`issue#13962 <http://tracker.ceph.com/issues/13962>`_, `pr#6778 <http://github.com/ceph/ceph/pull/6778>`_, Sage Weil)
* osd: change mutex to spinlock to optimize thread context switch. (`pr#6492 <http://github.com/ceph/ceph/pull/6492>`_, Xiaowei Chen)
* osd: check do_shutdown before do_restart (`pr#6547 <http://github.com/ceph/ceph/pull/6547>`_, Xiaoxi Chen)
* osd: clarify the scrub result report (`pr#6534 <http://github.com/ceph/ceph/pull/6534>`_, Li Wang)
* osd: don't do random deep scrubs for user initiated scrubs (`pr#6673 <http://github.com/ceph/ceph/pull/6673>`_, David Zafman)
* osd: FileStore: support multiple ondisk finish and apply finishers (`pr#6486 <http://github.com/ceph/ceph/pull/6486>`_, Xinze Chi, Haomai Wang)
* osd: fix broken balance / localized read handling (`issue#13491 <http://tracker.ceph.com/issues/13491>`_, `pr#6364 <http://github.com/ceph/ceph/pull/6364>`_, Jason Dillaman)
* osd: fix bug in last_* PG state timestamps (`pr#6517 <http://github.com/ceph/ceph/pull/6517>`_, Li Wang)
* osd: fix ClassHandler::ClassData::get_filter() (`pr#6747 <http://github.com/ceph/ceph/pull/6747>`_, Yan, Zheng)
* osd: fixes for several cases where op result code was not checked or set (`issue#13566 <http://tracker.ceph.com/issues/13566>`_, `pr#6347 <http://github.com/ceph/ceph/pull/6347>`_, xie xingguo)
* osd: fix reactivate (check OSDSuperblock in mkfs() when we already have the superblock) (`issue#13586 <http://tracker.ceph.com/issues/13586>`_, `pr#6385 <http://github.com/ceph/ceph/pull/6385>`_, Vicente Cheng)
* osd: fix wrong use of right parenthesis in localized read logic (`pr#6566 <http://github.com/ceph/ceph/pull/6566>`_, Jie Wang)
* osd: improve temperature calculation for cache tier agent (`pr#4737 <http://github.com/ceph/ceph/pull/4737>`_, MingXin Liu)
* osd: merge local_t and op_t txn to single one (`pr#6439 <http://github.com/ceph/ceph/pull/6439>`_, Xinze Chi)
* osd: newstore: misc updates (including kv and os/fs stuff) (`pr#6609 <http://github.com/ceph/ceph/pull/6609>`_, Sage Weil)
* osd: note down the number of missing clones (`pr#6654 <http://github.com/ceph/ceph/pull/6654>`_, Kefu Chai)
* osd: optimize clone write path if object-map is enabled (`pr#6403 <http://github.com/ceph/ceph/pull/6403>`_, xinxin shu)
* osd: optimize scrub subset_last_update calculation (`pr#6518 <http://github.com/ceph/ceph/pull/6518>`_, Li Wang)
* osd: partial revert of "ReplicatedPG: result code not correctly set in some cases." (`issue#13796 <http://tracker.ceph.com/issues/13796>`_, `pr#6622 <http://github.com/ceph/ceph/pull/6622>`_, Sage Weil)
* osd: randomize deep scrubbing (`pr#6550 <http://github.com/ceph/ceph/pull/6550>`_, Dan van der Ster, Herve Rousseau)
* osd: scrub: do not assign value if read error (`pr#6568 <http://github.com/ceph/ceph/pull/6568>`_, Li Wang)
* osd: write file journal optimization (`pr#6484 <http://github.com/ceph/ceph/pull/6484>`_, Xinze Chi)
* rbd: accept --user, refuse -i command-line optionals (`pr#6590 <http://github.com/ceph/ceph/pull/6590>`_, Ilya Dryomov)
* rbd: add missing command aliases to refactored CLI (`issue#13806 <http://tracker.ceph.com/issues/13806>`_, `pr#6606 <http://github.com/ceph/ceph/pull/6606>`_, Jason Dillaman)
* rbd: dynamically generated bash completion (`issue#13494 <http://tracker.ceph.com/issues/13494>`_, `pr#6316 <http://github.com/ceph/ceph/pull/6316>`_, Jason Dillaman)
* rbd: fixes for refactored CLI and related tests (`pr#6738 <http://github.com/ceph/ceph/pull/6738>`_, Ilya Dryomov)
* rbd: make config changes actually apply (`pr#6520 <http://github.com/ceph/ceph/pull/6520>`_, Mykola Golub)
* rbd: refactor cli command handling (`pr#5987 <http://github.com/ceph/ceph/pull/5987>`_, Jason Dillaman)
* rbd: stripe unit/count set incorrectly from config (`pr#6593 <http://github.com/ceph/ceph/pull/6593>`_, Mykola Golub)
* rbd: support negative boolean command-line optionals (`issue#13784 <http://tracker.ceph.com/issues/13784>`_, `pr#6607 <http://github.com/ceph/ceph/pull/6607>`_, Jason Dillaman)
* rbd: unbreak rbd map + cephx_sign_messages option (`pr#6583 <http://github.com/ceph/ceph/pull/6583>`_, Ilya Dryomov)
* rgw: bucket request payment support (`issue#13427 <http://tracker.ceph.com/issues/13427>`_, `pr#6214 <http://github.com/ceph/ceph/pull/6214>`_, Javier M. Mellid)
* rgw: extend rgw_extended_http_attrs to affect Swift accounts and containers as well (`pr#5969 <http://github.com/ceph/ceph/pull/5969>`_, Radoslaw Zarzynski)
* rgw: fix openssl linkage (`pr#6513 <http://github.com/ceph/ceph/pull/6513>`_, Yehuda Sadeh)
* rgw: fix partial read issue in rgw_admin and rgw_tools (`pr#6761 <http://github.com/ceph/ceph/pull/6761>`_, Jiaying Ren)
* rgw: fix reload on non Debian systems. (`pr#6482 <http://github.com/ceph/ceph/pull/6482>`_, Hervé Rousseau)
* rgw: fix response of delete expired objects (`issue#13469 <http://tracker.ceph.com/issues/13469>`_, `pr#6228 <http://github.com/ceph/ceph/pull/6228>`_, Yuan Zhou)
* rgw: fix swift API returning incorrect account metadata (`issue#13140 <http://tracker.ceph.com/issues/13140>`_, `pr#6047 <http://github.com/ceph/ceph/pull/6047>`_, Sangdi Xu)
* rgw: link against system openssl (instead of dlopen at runtime) (`pr#6419 <http://github.com/ceph/ceph/pull/6419>`_, Sage Weil)
* rgw: prevent anonymous user from reading bucket with authenticated read ACL (`issue#13207 <http://tracker.ceph.com/issues/13207>`_, `pr#6057 <http://github.com/ceph/ceph/pull/6057>`_, root)
* rgw: use smart pointer for C_Reinitwatch (`pr#6767 <http://github.com/ceph/ceph/pull/6767>`_, Orit Wasserman)
* systemd: fix typos (`pr#6679 <http://github.com/ceph/ceph/pull/6679>`_, Tobias Suckow)
* tests: centos7 needs the Continuous Release (CR) Repository enabled for (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6842 <http://github.com/ceph/ceph/pull/6842>`_, Brad Hubbard)
* tests: concatenate test_rados_test_tool from src and qa (`issue#13691 <http://tracker.ceph.com/issues/13691>`_, `pr#6464 <http://github.com/ceph/ceph/pull/6464>`_, Loic Dachary)
* tests: fix test_rados_tools.sh rados lookup (`issue#13691 <http://tracker.ceph.com/issues/13691>`_, `pr#6502 <http://github.com/ceph/ceph/pull/6502>`_, Loic Dachary)
* tests: fix typo in TestClsRbd.snapshots test case (`issue#13727 <http://tracker.ceph.com/issues/13727>`_, `pr#6504 <http://github.com/ceph/ceph/pull/6504>`_, Jason Dillaman)
* tests: ignore test-suite.log (`pr#6584 <http://github.com/ceph/ceph/pull/6584>`_, Loic Dachary)
* tests: restore run-cli-tests (`pr#6571 <http://github.com/ceph/ceph/pull/6571>`_, Loic Dachary, Sage Weil, Jason Dillaman)
* tools/cephfs: fix overflow writing header to fixed size buffer (#13816) (`pr#6617 <http://github.com/ceph/ceph/pull/6617>`_, John Spray)

v10.0.0
=======

This is the first development release for the Jewel cycle.

Notable Changes
---------------

* build: cmake tweaks (`pr#6254 <http://github.com/ceph/ceph/pull/6254>`_, John Spray)
* build: more CMake package check fixes (`pr#6108 <http://github.com/ceph/ceph/pull/6108>`_, Daniel Gryniewicz)
* ceph-disk: get Nonetype when ceph-disk list with --format plain on single device. (`pr#6410 <http://github.com/ceph/ceph/pull/6410>`_, Vicente Cheng)
* ceph: fix tell behavior (`pr#6329 <http://github.com/ceph/ceph/pull/6329>`_, David Zafman)
* ceph-fuse: While starting ceph-fuse, start the log thread first (`issue#13443 <http://tracker.ceph.com/issues/13443>`_, `pr#6224 <http://github.com/ceph/ceph/pull/6224>`_, Wenjun Huang)
* client: don't mark_down on command reply (`pr#6204 <http://github.com/ceph/ceph/pull/6204>`_, John Spray)
* client: drop prefix from ints (`pr#6275 <http://github.com/ceph/ceph/pull/6275>`_, John Coyle)
* client: sys/file.h includes for flock operations (`pr#6282 <http://github.com/ceph/ceph/pull/6282>`_, John Coyle)
* cls_rbd: change object_map_update to return 0 on success, add logging (`pr#6467 <http://github.com/ceph/ceph/pull/6467>`_, Douglas Fuller)
* cmake: Use uname instead of arch. (`pr#6358 <http://github.com/ceph/ceph/pull/6358>`_, John Coyle)
* common: assert: __STRING macro is not defined by musl libc. (`pr#6210 <http://github.com/ceph/ceph/pull/6210>`_, John Coyle)
* common: fix OpTracker age histogram calculation (`pr#5065 <http://github.com/ceph/ceph/pull/5065>`_, Zhiqiang Wang)
* common/MemoryModel: Added explicit feature check for mallinfo(). (`pr#6252 <http://github.com/ceph/ceph/pull/6252>`_, John Coyle)
* common/obj_bencher.cc: fix verification crashing when there's no objects (`pr#5853 <http://github.com/ceph/ceph/pull/5853>`_, Piotr Dałek)
* common: optimize debug logging (`pr#6307 <http://github.com/ceph/ceph/pull/6307>`_, Adam Kupczyk)
* common: Thread: move copy constructor and assignment op (`pr#5133 <http://github.com/ceph/ceph/pull/5133>`_, Michal Jarzabek)
* common: WorkQueue: new PointerWQ base class for ContextWQ (`issue#13636 <http://tracker.ceph.com/issues/13636>`_, `pr#6525 <http://github.com/ceph/ceph/pull/6525>`_, Jason Dillaman)
* compat: use prefixed typeof extension (`pr#6216 <http://github.com/ceph/ceph/pull/6216>`_, John Coyle)
* crush: validate bucket id before indexing buckets array (`issue#13477 <http://tracker.ceph.com/issues/13477>`_, `pr#6246 <http://github.com/ceph/ceph/pull/6246>`_, Sage Weil)
* doc: download GPG key from download.ceph.com (`issue#13603 <http://tracker.ceph.com/issues/13603>`_, `pr#6384 <http://github.com/ceph/ceph/pull/6384>`_, Ken Dreyer)
* doc: fix outdated content in cache tier (`pr#6272 <http://github.com/ceph/ceph/pull/6272>`_, Yuan Zhou)
* doc/release-notes: v9.1.0 (`pr#6281 <http://github.com/ceph/ceph/pull/6281>`_, Loic Dachary)
* doc/releases-notes: fix build error (`pr#6483 <http://github.com/ceph/ceph/pull/6483>`_, Kefu Chai)
* doc: remove toctree items under Create CephFS (`pr#6241 <http://github.com/ceph/ceph/pull/6241>`_, Jevon Qiao)
* doc: rename the "Create a Ceph User" section and add verbage about… (`issue#13502 <http://tracker.ceph.com/issues/13502>`_, `pr#6297 <http://github.com/ceph/ceph/pull/6297>`_, ritz303)
* docs: Fix styling of newly added mirror docs (`pr#6127 <http://github.com/ceph/ceph/pull/6127>`_, Wido den Hollander)
* doc, tests: update all http://ceph.com/ to download.ceph.com (`pr#6435 <http://github.com/ceph/ceph/pull/6435>`_, Alfredo Deza)
* doc: update doc for with new pool settings (`pr#5951 <http://github.com/ceph/ceph/pull/5951>`_, Guang Yang)
* doc: update radosgw-admin example (`pr#6256 <http://github.com/ceph/ceph/pull/6256>`_, YankunLi)
* doc: update the OS recommendations for newer Ceph releases (`pr#6355 <http://github.com/ceph/ceph/pull/6355>`_, ritz303)
* drop envz.h includes (`pr#6285 <http://github.com/ceph/ceph/pull/6285>`_, John Coyle)
* libcephfs: Improve portability by replacing loff_t type usage with off_t (`pr#6301 <http://github.com/ceph/ceph/pull/6301>`_, John Coyle)
* libcephfs: only check file offset on glibc platforms (`pr#6288 <http://github.com/ceph/ceph/pull/6288>`_, John Coyle)
* librados: fix examples/librados/Makefile error. (`pr#6320 <http://github.com/ceph/ceph/pull/6320>`_, You Ji)
* librados: init crush_location from config file. (`issue#13473 <http://tracker.ceph.com/issues/13473>`_, `pr#6243 <http://github.com/ceph/ceph/pull/6243>`_, Wei Luo)
* librados: wrongly passed in argument for stat command (`issue#13703 <http://tracker.ceph.com/issues/13703>`_, `pr#6476 <http://github.com/ceph/ceph/pull/6476>`_, xie xingguo)
* librbd: deadlock while attempting to flush AIO requests (`issue#13726 <http://tracker.ceph.com/issues/13726>`_, `pr#6508 <http://github.com/ceph/ceph/pull/6508>`_, Jason Dillaman)
* librbd: fix enable objectmap feature issue (`issue#13558 <http://tracker.ceph.com/issues/13558>`_, `pr#6339 <http://github.com/ceph/ceph/pull/6339>`_, xinxin shu)
* librbd: remove duplicate read_only test in librbd::async_flatten (`pr#5856 <http://github.com/ceph/ceph/pull/5856>`_, runsisi)
* mailmap: modify member info  (`pr#6468 <http://github.com/ceph/ceph/pull/6468>`_, Xiaowei Chen)
* mailmap: updates (`pr#6258 <http://github.com/ceph/ceph/pull/6258>`_, M Ranga Swami Reddy)
* mailmap: Xie Xingguo affiliation (`pr#6409 <http://github.com/ceph/ceph/pull/6409>`_, Loic Dachary)
* mds: implement snapshot rename (`pr#5645 <http://github.com/ceph/ceph/pull/5645>`_, xinxin shu)
* mds: messages/MOSDOp: cast in assert to eliminate warnings (`issue#13625 <http://tracker.ceph.com/issues/13625>`_, `pr#6414 <http://github.com/ceph/ceph/pull/6414>`_, David Zafman)
* mds: new filtered MDS tell commands for sessions (`pr#6180 <http://github.com/ceph/ceph/pull/6180>`_, John Spray)
* mds/Session: use projected parent for auth path check (`issue#13364 <http://tracker.ceph.com/issues/13364>`_, `pr#6200 <http://github.com/ceph/ceph/pull/6200>`_, Sage Weil)
* mon: should not set isvalid = true when cephx_verify_authorizer return false (`issue#13525 <http://tracker.ceph.com/issues/13525>`_, `pr#6306 <http://github.com/ceph/ceph/pull/6306>`_, Ruifeng Yang)
* osd: Add config option osd_read_ec_check_for_errors for testing (`pr#5865 <http://github.com/ceph/ceph/pull/5865>`_, David Zafman)
* osd: add pin/unpin support to cache tier (11066) (`pr#6326 <http://github.com/ceph/ceph/pull/6326>`_, Zhiqiang Wang)
* osd: auto repair EC pool (`issue#12754 <http://tracker.ceph.com/issues/12754>`_, `pr#6196 <http://github.com/ceph/ceph/pull/6196>`_, Guang Yang)
* osd: drop the interim set from load_pgs() (`pr#6277 <http://github.com/ceph/ceph/pull/6277>`_, Piotr Dałek)
* osd: FileJournal: _fdump wrongly returns if journal is currently unreadable. (`issue#13626 <http://tracker.ceph.com/issues/13626>`_, `pr#6406 <http://github.com/ceph/ceph/pull/6406>`_, xie xingguo)
* osd: FileStore: add a field indicate xattr only one chunk for set xattr. (`pr#6244 <http://github.com/ceph/ceph/pull/6244>`_, Jianpeng Ma)
* osd: FileStore: LFNIndex: remove redundant local variable 'obj'. (`issue#13552 <http://tracker.ceph.com/issues/13552>`_, `pr#6333 <http://github.com/ceph/ceph/pull/6333>`_, xiexingguo)
* osd: FileStore: potential memory leak if _fgetattrs fails (`issue#13597 <http://tracker.ceph.com/issues/13597>`_, `pr#6377 <http://github.com/ceph/ceph/pull/6377>`_, xie xingguo)
* osd: FileStore: remove unused local variable 'handle' (`pr#6381 <http://github.com/ceph/ceph/pull/6381>`_, xie xingguo)
* osd: fix bogus scrub results when missing a clone (`issue#12738 <http://tracker.ceph.com/issues/12738>`_, `issue#12740 <http://tracker.ceph.com/issues/12740>`_, `pr#5783 <http://github.com/ceph/ceph/pull/5783>`_, David Zafman)
* osd: fix debug message in OSD::is_healthy (`pr#6226 <http://github.com/ceph/ceph/pull/6226>`_, Xiaoxi Chen)
* osd: fix MOSDOp encoding (`pr#6174 <http://github.com/ceph/ceph/pull/6174>`_, Sage Weil)
* osd: init started to 0 (`issue#13206 <http://tracker.ceph.com/issues/13206>`_, `pr#6107 <http://github.com/ceph/ceph/pull/6107>`_, Sage Weil)
* osd: KeyValueStore: fix the name's typo of keyvaluestore_default_strip_size (`pr#6375 <http://github.com/ceph/ceph/pull/6375>`_, Zhi Zhang)
* osd: new and delete ObjectStore::Transaction in a function is not necessary (`pr#6299 <http://github.com/ceph/ceph/pull/6299>`_, Ruifeng Yang)
* osd: optimize get_object_context (`pr#6305 <http://github.com/ceph/ceph/pull/6305>`_, Jianpeng Ma)
* osd: optimize MOSDOp/do_op/handle_op (`pr#5211 <http://github.com/ceph/ceph/pull/5211>`_, Jacek J. Lakis)
* osd: os/chain_xattr: On linux use linux/limits.h for XATTR_NAME_MAX. (`pr#6343 <http://github.com/ceph/ceph/pull/6343>`_, John Coyle)
* osd: reorder bool fields in PGLog struct (`pr#6279 <http://github.com/ceph/ceph/pull/6279>`_, Piotr Dałek)
* osd: ReplicatedPG: remove unused local variables (`issue#13575 <http://tracker.ceph.com/issues/13575>`_, `pr#6360 <http://github.com/ceph/ceph/pull/6360>`_, xiexingguo)
* osd: reset primary and up_primary when building a new past_interval. (`issue#13471 <http://tracker.ceph.com/issues/13471>`_, `pr#6240 <http://github.com/ceph/ceph/pull/6240>`_, xiexingguo)
* radosgw-admin: Checking the legality of the parameters (`issue#13018 <http://tracker.ceph.com/issues/13018>`_, `pr#5879 <http://github.com/ceph/ceph/pull/5879>`_, Qiankun Zheng)
* radosgw-admin: Create --secret-key alias for --secret (`issue#5821 <http://tracker.ceph.com/issues/5821>`_, `pr#5335 <http://github.com/ceph/ceph/pull/5335>`_, Yuan Zhou)
* radosgw-admin: metadata list user should return an empty list when user pool is empty (`issue#13596 <http://tracker.ceph.com/issues/13596>`_, `pr#6465 <http://github.com/ceph/ceph/pull/6465>`_, Orit Wasserman)
* rados: new options for write benchmark (`pr#6340 <http://github.com/ceph/ceph/pull/6340>`_, Joaquim Rocha)
* rbd: fix clone isssue (`issue#13553 <http://tracker.ceph.com/issues/13553>`_, `pr#6334 <http://github.com/ceph/ceph/pull/6334>`_, xinxin shu)
* rbd: fix init-rbdmap CMDPARAMS (`issue#13214 <http://tracker.ceph.com/issues/13214>`_, `pr#6109 <http://github.com/ceph/ceph/pull/6109>`_, Sage Weil)
* rbdmap: systemd support (`issue#13374 <http://tracker.ceph.com/issues/13374>`_, `pr#6479 <http://github.com/ceph/ceph/pull/6479>`_, Boris Ranto)
* rbd: rbdmap improvements (`pr#6445 <http://github.com/ceph/ceph/pull/6445>`_, Boris Ranto)
* release-notes: draft v0.94.4 release notes (`pr#5907 <http://github.com/ceph/ceph/pull/5907>`_, Loic Dachary)
* release-notes: draft v0.94.4 release notes (`pr#6195 <http://github.com/ceph/ceph/pull/6195>`_, Loic Dachary)
* release-notes: draft v0.94.4 release notes (`pr#6238 <http://github.com/ceph/ceph/pull/6238>`_, Loic Dachary)
* rgw: add compat header for TEMP_FAILURE_RETRY (`pr#6294 <http://github.com/ceph/ceph/pull/6294>`_, John Coyle)
* rgw: add default quota config (`pr#6400 <http://github.com/ceph/ceph/pull/6400>`_, Daniel Gryniewicz)
* rgw: add support for getting Swift's DLO without manifest handling (`pr#6206 <http://github.com/ceph/ceph/pull/6206>`_, Radoslaw Zarzynski)
* rgw: clarify the error message when trying to create an existed user (`pr#5938 <http://github.com/ceph/ceph/pull/5938>`_, Zeqiang Zhuang)
* rgw: fix objects can not be displayed which object name does not cont… (`issue#12963 <http://tracker.ceph.com/issues/12963>`_, `pr#5738 <http://github.com/ceph/ceph/pull/5738>`_, Weijun Duan)
* rgw: fix typo in RGWHTTPClient::process error message (`pr#6424 <http://github.com/ceph/ceph/pull/6424>`_, Brad Hubbard)
* rgw: fix wrong etag calculation during POST on S3 bucket. (`issue#11241 <http://tracker.ceph.com/issues/11241>`_, `pr#6030 <http://github.com/ceph/ceph/pull/6030>`_, Radoslaw Zarzynski)
* rgw: mdlog trim add usage prompt (`pr#6059 <http://github.com/ceph/ceph/pull/6059>`_, Weijun Duan)
* rgw: modify the conditional statement in parse_metadata_key method. (`pr#5875 <http://github.com/ceph/ceph/pull/5875>`_, Zengran Zhang)
* rgw: refuse to calculate digest when the s3 secret key is empty (`issue#13133 <http://tracker.ceph.com/issues/13133>`_, `pr#6045 <http://github.com/ceph/ceph/pull/6045>`_, Sangdi Xu)
* rgw: remove extra check in RGWGetObj::execute (`issue#12352 <http://tracker.ceph.com/issues/12352>`_, `pr#5262 <http://github.com/ceph/ceph/pull/5262>`_, Javier M. Mellid)
* rgw: support core file limit for radosgw daemon (`pr#6346 <http://github.com/ceph/ceph/pull/6346>`_, Guang Yang)
* rgw: swift use Civetweb ssl can not get right url (`issue#13628 <http://tracker.ceph.com/issues/13628>`_, `pr#6408 <http://github.com/ceph/ceph/pull/6408>`_, Weijun Duan)
* rocksdb: build with PORTABLE=1 (`pr#6311 <http://github.com/ceph/ceph/pull/6311>`_, Sage Weil)
* rocksdb: remove rdb source files from dist tarball (`issue#13554 <http://tracker.ceph.com/issues/13554>`_, `pr#6379 <http://github.com/ceph/ceph/pull/6379>`_, Kefu Chai)
* rocksdb: use native rocksdb makefile (and our autotools) (`pr#6290 <http://github.com/ceph/ceph/pull/6290>`_, Sage Weil)
* rpm: ceph.spec.in: correctly declare systemd dependency for SLE/openSUSE (`pr#6114 <http://github.com/ceph/ceph/pull/6114>`_, Nathan Cutler)
* rpm: ceph.spec.in: fix libs-compat / devel-compat conditional (`issue#12315 <http://tracker.ceph.com/issues/12315>`_, `pr#5219 <http://github.com/ceph/ceph/pull/5219>`_, Ken Dreyer)
* rpm: rhel 5.9 librados compile fix, moved blkid to RBD check/compilation (`issue#13177 <http://tracker.ceph.com/issues/13177>`_, `pr#5954 <http://github.com/ceph/ceph/pull/5954>`_, Rohan Mars)
* scripts: release_notes can track original issue (`pr#6009 <http://github.com/ceph/ceph/pull/6009>`_, Abhishek Lekshmanan)
* test/libcephfs/flock: add sys/file.h include for flock operations (`pr#6310 <http://github.com/ceph/ceph/pull/6310>`_, John Coyle)
* test_rgw_admin: use freopen for output redirection. (`pr#6303 <http://github.com/ceph/ceph/pull/6303>`_, John Coyle)
* tests: allow docker-test.sh to run under root (`issue#13355 <http://tracker.ceph.com/issues/13355>`_, `pr#6173 <http://github.com/ceph/ceph/pull/6173>`_, Loic Dachary)
* tests: ceph-disk workunit uses configobj  (`pr#6342 <http://github.com/ceph/ceph/pull/6342>`_, Loic Dachary)
* tests: destroy testprofile before creating one (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6446 <http://github.com/ceph/ceph/pull/6446>`_, Loic Dachary)
* tests: port uniqueness reminder (`pr#6387 <http://github.com/ceph/ceph/pull/6387>`_, Loic Dachary)
* tests: test/librados/test.cc must create profile (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6452 <http://github.com/ceph/ceph/pull/6452>`_, Loic Dachary)
* tools/cephfs: fix overflow writing header to fixed size buffer (#13816) (`pr#6617 <http://github.com/ceph/ceph/pull/6617>`_, John Spray)
* tools: ceph-monstore-update-crush: add "--test" when testing crushmap (`pr#6418 <http://github.com/ceph/ceph/pull/6418>`_, Kefu Chai)
* tools:remove duplicate references (`pr#5917 <http://github.com/ceph/ceph/pull/5917>`_, Bo Cai)
* vstart: grant full access to Swift testing account (`pr#6239 <http://github.com/ceph/ceph/pull/6239>`_, Yuan Zhou)
* vstart: set cephfs root uid/gid to caller (`pr#6255 <http://github.com/ceph/ceph/pull/6255>`_, John Spray)


v9.2.1 Infernalis
=================

This Infernalis point release fixes several packagins and init script
issues, enables the librbd objectmap feature by default, a few librbd
bugs, and a range of miscellaneous bug fixes across the system.

We recommend that all infernalis v9.2.0 users upgrade.

For more detailed information, see :download:`the complete changelog <changelog/v9.2.1.txt>`.

Upgrading
---------

* Some symbols wrongly exposed by the C++ interface for librados in
  v9.1.0 and v9.2.0 were removed.  If you compiled your own
  application against librados shipped with these releases, it is very
  likely referencing these removed symbols. So you will need to
  recompile it.


Notable Changes
---------------
* build/ops: Ceph daemon failed to start, because the service name was already used. (`issue#13474 <http://tracker.ceph.com/issues/13474>`_, `pr#6833 <http://github.com/ceph/ceph/pull/6833>`_, Chuanhong Wang)
* build/ops: ceph upstart script rbdmap.conf incorrectly processes parameters (`issue#13214 <http://tracker.ceph.com/issues/13214>`_, `pr#6396 <http://github.com/ceph/ceph/pull/6396>`_, Sage Weil)
* build/ops: libunwind package missing on CentOS 7 (`issue#13997 <http://tracker.ceph.com/issues/13997>`_, `pr#6845 <http://github.com/ceph/ceph/pull/6845>`_, Loic Dachary)
* build/ops: rbd-replay-* moved from ceph-test-dbg to ceph-common-dbg as well (`issue#13785 <http://tracker.ceph.com/issues/13785>`_, `pr#6628 <http://github.com/ceph/ceph/pull/6628>`_, Loic Dachary)
* build/ops: systemd/ceph-disk@.service assumes /bin/flock (`issue#13975 <http://tracker.ceph.com/issues/13975>`_, `pr#6852 <http://github.com/ceph/ceph/pull/6852>`_, Loic Dachary)
* build/ops: systemd: no rbdmap systemd unit file (`issue#13374 <http://tracker.ceph.com/issues/13374>`_, `pr#6500 <http://github.com/ceph/ceph/pull/6500>`_, Boris Ranto)
* common: auth/cephx: large amounts of log are produced by osd (`issue#13610 <http://tracker.ceph.com/issues/13610>`_, `pr#6836 <http://github.com/ceph/ceph/pull/6836>`_, Qiankun Zheng)
* common: log: Log.cc: Assign LOG_DEBUG priority to syslog calls (`issue#13993 <http://tracker.ceph.com/issues/13993>`_, `pr#6993 <http://github.com/ceph/ceph/pull/6993>`_, Brad Hubbard)
* crush: crash if we see CRUSH_ITEM_NONE in early rule step (`issue#13477 <http://tracker.ceph.com/issues/13477>`_, `pr#6626 <http://github.com/ceph/ceph/pull/6626>`_, Sage Weil)
* fs: Ceph file system is not freeing space (`issue#13777 <http://tracker.ceph.com/issues/13777>`_, `pr#7431 <http://github.com/ceph/ceph/pull/7431>`_, Yan, Zheng, John Spray)
* fs: Ceph-fuse won't start correctly when the option log_max_new in ceph.conf set to zero (`issue#13443 <http://tracker.ceph.com/issues/13443>`_, `pr#6395 <http://github.com/ceph/ceph/pull/6395>`_, Wenjun Huang)
* fs: Segmentation fault accessing file using fuse mount (`issue#13714 <http://tracker.ceph.com/issues/13714>`_, `pr#6853 <http://github.com/ceph/ceph/pull/6853>`_, Yan, Zheng)
* librbd: Avoid re-writing old-format image header on resize (`issue#13674 <http://tracker.ceph.com/issues/13674>`_, `pr#6630 <http://github.com/ceph/ceph/pull/6630>`_, Jason Dillaman)
* librbd: ImageWatcher shouldn't block the notification thread (`issue#14373 <http://tracker.ceph.com/issues/14373>`_, `pr#7406 <http://github.com/ceph/ceph/pull/7406>`_, Jason Dillaman)
* librbd: QEMU hangs after creating snapshot and stopping VM (`issue#13726 <http://tracker.ceph.com/issues/13726>`_, `pr#6632 <http://github.com/ceph/ceph/pull/6632>`_, Jason Dillaman)
* librbd: Verify self-managed snapshot functionality on image create (`issue#13633 <http://tracker.ceph.com/issues/13633>`_, `pr#7080 <http://github.com/ceph/ceph/pull/7080>`_, Jason Dillaman)
* librbd: [ FAILED ] TestLibRBD.SnapRemoveViaLockOwner (`issue#14164 <http://tracker.ceph.com/issues/14164>`_, `pr#7079 <http://github.com/ceph/ceph/pull/7079>`_, Jason Dillaman)
* librbd: enable feature objectmap (`issue#13558 <http://tracker.ceph.com/issues/13558>`_, `pr#6477 <http://github.com/ceph/ceph/pull/6477>`_, xinxin shu)
* librbd: fix merge-diff for >2GB diff-files (`issue#14030 <http://tracker.ceph.com/issues/14030>`_, `pr#6981 <http://github.com/ceph/ceph/pull/6981>`_, Jason Dillaman)
* librbd: flattening an rbd image with active IO can lead to hang (`issue#14092 <http://tracker.ceph.com/issues/14092>`_, `issue#14483 <http://tracker.ceph.com/issues/14483>`_, `pr#7484 <http://github.com/ceph/ceph/pull/7484>`_, Jason Dillaman)
* mds: fix client capabilities during reconnect (client.XXXX isn't responding to mclientcaps warning) (`issue#11482 <http://tracker.ceph.com/issues/11482>`_, `pr#6752 <http://github.com/ceph/ceph/pull/6752>`_, Yan, Zheng)
* mon: Ceph Pools' MAX AVAIL is 0 if some OSDs' weight is 0 (`issue#13840 <http://tracker.ceph.com/issues/13840>`_, `pr#6907 <http://github.com/ceph/ceph/pull/6907>`_, Chengyuan Li)
* mon: should not set isvalid = true when cephx_verify_authorizer retur... (`issue#13525 <http://tracker.ceph.com/issues/13525>`_, `pr#6392 <http://github.com/ceph/ceph/pull/6392>`_, Ruifeng Yang)
* objecter: pool op callback may hang forever. (`issue#13642 <http://tracker.ceph.com/issues/13642>`_, `pr#6627 <http://github.com/ceph/ceph/pull/6627>`_, xie xingguo)
* objecter: potential null pointer access when do pool_snap_list. (`issue#13639 <http://tracker.ceph.com/issues/13639>`_, `pr#6840 <http://github.com/ceph/ceph/pull/6840>`_, xie xingguo)
* osd: FileStore: potential memory leak if getattrs fails. (`issue#13597 <http://tracker.ceph.com/issues/13597>`_, `pr#6846 <http://github.com/ceph/ceph/pull/6846>`_, xie xingguo)
* osd: OSD::build_past_intervals_parallel() shall reset primary and up_primary when begin a new past_interval. (`issue#13471 <http://tracker.ceph.com/issues/13471>`_, `pr#6397 <http://github.com/ceph/ceph/pull/6397>`_, xiexingguo)
* osd: call on_new_interval on newly split child PG (`issue#13962 <http://tracker.ceph.com/issues/13962>`_, `pr#6849 <http://github.com/ceph/ceph/pull/6849>`_, Sage Weil)
* osd: ceph-disk list fails on /dev/cciss!c0d0 (`issue#13970 <http://tracker.ceph.com/issues/13970>`_, `issue#14230 <http://tracker.ceph.com/issues/14230>`_, `pr#6880 <http://github.com/ceph/ceph/pull/6880>`_, Loic Dachary)
* osd: ceph-disk: use blkid instead of sgdisk -i (`issue#14080 <http://tracker.ceph.com/issues/14080>`_, `pr#7001 <http://github.com/ceph/ceph/pull/7001>`_, Loic Dachary, Ilya Dryomov)
* osd: fix race condition during send_failures (`issue#13821 <http://tracker.ceph.com/issues/13821>`_, `pr#6694 <http://github.com/ceph/ceph/pull/6694>`_, Sage Weil)
* osd: osd/PG.cc: 288: FAILED assert(info.last_epoch_started >= info.history.last_epoch_started) (`issue#14015 <http://tracker.ceph.com/issues/14015>`_, `pr#6851 <http://github.com/ceph/ceph/pull/6851>`_, David Zafman)
* osd: pgs stuck inconsistent after infernalis upgrade (`issue#13862 <http://tracker.ceph.com/issues/13862>`_, `pr#7421 <http://github.com/ceph/ceph/pull/7421>`_, David Zafman)
* rbd: TaskFinisher::cancel should remove event from SafeTimer (`issue#14476 <http://tracker.ceph.com/issues/14476>`_, `pr#7426 <http://github.com/ceph/ceph/pull/7426>`_, Douglas Fuller)
* rbd: cls_rbd: object_map_save should enable checksums (`issue#14280 <http://tracker.ceph.com/issues/14280>`_, `pr#7428 <http://github.com/ceph/ceph/pull/7428>`_, Douglas Fuller)
* rbd: misdirected op in rbd balance-reads test (`issue#13491 <http://tracker.ceph.com/issues/13491>`_, `pr#6629 <http://github.com/ceph/ceph/pull/6629>`_, Jason Dillaman)
* rbd: pure virtual method called (`issue#13636 <http://tracker.ceph.com/issues/13636>`_, `pr#6633 <http://github.com/ceph/ceph/pull/6633>`_, Jason Dillaman)
* rbd: rbd clone issue (`issue#13553 <http://tracker.ceph.com/issues/13553>`_, `pr#6474 <http://github.com/ceph/ceph/pull/6474>`_, xinxin shu)
* rbd: rbd-replay does not check for EOF and goes to endless loop (`issue#14452 <http://tracker.ceph.com/issues/14452>`_, `pr#7427 <http://github.com/ceph/ceph/pull/7427>`_, Mykola Golub)
* rbd: unknown argument --quiet in udevadm settle (`issue#13560 <http://tracker.ceph.com/issues/13560>`_, `pr#6634 <http://github.com/ceph/ceph/pull/6634>`_, Jason Dillaman)
* rgw: init script reload doesn't work on EL7 (`issue#13709 <http://tracker.ceph.com/issues/13709>`_, `pr#6650 <http://github.com/ceph/ceph/pull/6650>`_, Hervé Rousseau)
* rgw: radosgw-admin --help doesn't show the orphans find command (`issue#14516 <http://tracker.ceph.com/issues/14516>`_, `pr#7543 <http://github.com/ceph/ceph/pull/7543>`_, Yehuda Sadeh)
* tests: ceph-disk workunit uses configobj (`issue#14004 <http://tracker.ceph.com/issues/14004>`_, `pr#6828 <http://github.com/ceph/ceph/pull/6828>`_, Loic Dachary)
* tests: fsx failed to compile (`issue#14384 <http://tracker.ceph.com/issues/14384>`_, `pr#7429 <http://github.com/ceph/ceph/pull/7429>`_, Greg Farnum)
* tests: notification slave needs to wait for master (`issue#13810 <http://tracker.ceph.com/issues/13810>`_, `pr#7225 <http://github.com/ceph/ceph/pull/7225>`_, Jason Dillaman)
* tests: rebuild exclusive lock test should acquire exclusive lock (`issue#14121 <http://tracker.ceph.com/issues/14121>`_, `pr#7038 <http://github.com/ceph/ceph/pull/7038>`_, Jason Dillaman)
* tests: testprofile must be removed before it is re-created (`issue#13664 <http://tracker.ceph.com/issues/13664>`_, `pr#6449 <http://github.com/ceph/ceph/pull/6449>`_, Loic Dachary)
* tests: verify it is possible to reuse an OSD id (`issue#13988 <http://tracker.ceph.com/issues/13988>`_, `pr#6882 <http://github.com/ceph/ceph/pull/6882>`_, Loic Dachary)

v9.2.0 Infernalis
=================

This major release will be the foundation for the next stable series.
There have been some major changes since v0.94.x Hammer, and the
upgrade process is non-trivial.  Please read these release notes carefully.

Major Changes from Hammer
-------------------------

- *General*:

  * Ceph daemons are now managed via systemd (with the exception of
    Ubuntu Trusty, which still uses upstart).
  * Ceph daemons run as 'ceph' user instead root.
  * On Red Hat distros, there is also an SELinux policy.

- *RADOS*:

  * The RADOS cache tier can now proxy write operations to the base
    tier, allowing writes to be handled without forcing migration of
    an object into the cache.
  * The SHEC erasure coding support is no longer flagged as
    experimental. SHEC trades some additional storage space for faster
    repair.
  * There is now a unified queue (and thus prioritization) of client
    IO, recovery, scrubbing, and snapshot trimming.
  * There have been many improvements to low-level repair tooling
    (ceph-objectstore-tool).
  * The internal ObjectStore API has been significantly cleaned up in order
    to faciliate new storage backends like NewStore.

- *RGW*:

  * The Swift API now supports object expiration.
  * There are many Swift API compatibility improvements.

- *RBD*:

  * The ``rbd du`` command shows actual usage (quickly, when
    object-map is enabled).
  * The object-map feature has seen many stability improvements.
  * Object-map and exclusive-lock features can be enabled or disabled
    dynamically.
  * You can now store user metadata and set persistent librbd options
    associated with individual images.
  * The new deep-flatten features allows flattening of a clone and all
    of its snapshots.  (Previously snapshots could not be flattened.)
  * The export-diff command command is now faster (it uses aio).  There is also
    a new fast-diff feature.
  * The --size argument can be specified with a suffix for units
    (e.g., ``--size 64G``).
  * There is a new ``rbd status`` command that, for now, shows who has
    the image open/mapped.

- *CephFS*:

  * You can now rename snapshots.
  * There have been ongoing improvements around administration, diagnostics,
    and the check and repair tools.
  * The caching and revocation of client cache state due to unused
    inodes has been dramatically improved.
  * The ceph-fuse client behaves better on 32-bit hosts.

Distro compatibility
--------------------

We have decided to drop support for many older distributions so that we can
move to a newer compiler toolchain (e.g., C++11).  Although it is still possible
to build Ceph on older distributions by installing backported development tools,
we are not building and publishing release packages for ceph.com.

We now build packages for:

* CentOS 7 or later.  We have dropped support for CentOS 6 (and other
  RHEL 6 derivatives, like Scientific Linux 6).
* Debian Jessie 8.x or later.  Debian Wheezy 7.x's g++ has incomplete
  support for C++11 (and no systemd).
* Ubuntu Trusty 14.04 or later.  Ubuntu Precise 12.04 is no longer
  supported.
* Fedora 22 or later.

Upgrading from Firefly
----------------------

Upgrading directly from Firefly v0.80.z is not recommended.  It is
possible to do a direct upgrade, but not without downtime.  We
recommend that clusters are first upgraded to Hammer v0.94.4 or a
later v0.94.z release; only then is it possible to upgrade to
Infernalis 9.2.z for an online upgrade (see below).

To do an offline upgrade directly from Firefly, all Firefly OSDs must
be stopped and marked down before any Infernalis OSDs will be allowed
to start up.  This fencing is enforced by the Infernalis monitor, so
use an upgrade procedure like:

#. Upgrade Ceph on monitor hosts
#. Restart all ceph-mon daemons
#. Upgrade Ceph on all OSD hosts
#. Stop all ceph-osd daemons
#. Mark all OSDs down with something like::

     ceph osd down `seq 0 1000`

#. Start all ceph-osd daemons
#. Upgrade and restart remaining daemons (ceph-mds, radosgw)

Upgrading from Hammer
---------------------

* All cluster nodes must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
  then is it possible to upgrade to Infernalis 9.2.z.

* For all distributions that support systemd (CentOS 7, Fedora, Debian
  Jessie 8.x, OpenSUSE), ceph daemons are now managed using native systemd
  files instead of the legacy sysvinit scripts.  For example,::

    systemctl start ceph.target       # start all daemons
    systemctl status ceph-osd@12      # check status of osd.12

  The main notable distro that is *not* yet using systemd is Ubuntu trusty
  14.04.  (The next Ubuntu LTS, 16.04, will use systemd instead of upstart.)

* Ceph daemons now run as user and group ``ceph`` by default.  The
  ceph user has a static UID assigned by Fedora and Debian (also used
  by derivative distributions like RHEL/CentOS and Ubuntu).  On SUSE
  the ceph user will currently get a dynamically assigned UID when the
  user is created.

  If your systems already have a ceph user, upgrading the package will cause
  problems.  We suggest you first remove or rename the existing 'ceph' user
  and 'ceph' group before upgrading.

  When upgrading, administrators have two options:

   #. Add the following line to ``ceph.conf`` on all hosts::

        setuser match path = /var/lib/ceph/$type/$cluster-$id

      This will make the Ceph daemons run as root (i.e., not drop
      privileges and switch to user ceph) if the daemon's data
      directory is still owned by root.  Newly deployed daemons will
      be created with data owned by user ceph and will run with
      reduced privileges, but upgraded daemons will continue to run as
      root.

   #. Fix the data ownership during the upgrade.  This is the
      preferred option, but it is more work and can be very time
      consuming.  The process for each host is to:

      #. Upgrade the ceph package.  This creates the ceph user and group.  For
	 example::

	   ceph-deploy install --stable infernalis HOST

      #. Stop the daemon(s).::

	   service ceph stop           # fedora, centos, rhel, debian
	   stop ceph-all               # ubuntu

      #. Fix the ownership::

	   chown -R ceph:ceph /var/lib/ceph

      #. Restart the daemon(s).::

	   start ceph-all                # ubuntu
	   systemctl start ceph.target   # debian, centos, fedora, rhel

      Alternatively, the same process can be done with a single daemon
      type, for example by stopping only monitors and chowning only
      ``/var/lib/ceph/mon``.

* The on-disk format for the experimental KeyValueStore OSD backend has
  changed.  You will need to remove any OSDs using that backend before you
  upgrade any test clusters that use it.

* When a pool quota is reached, librados operations now block indefinitely,
  the same way they do when the cluster fills up.  (Previously they would return
  -ENOSPC).  By default, a full cluster or pool will now block.  If your
  librados application can handle ENOSPC or EDQUOT errors gracefully, you can
  get error returns instead by using the new librados OPERATION_FULL_TRY flag.

* The return code for librbd's rbd_aio_read and Image::aio_read API methods no
  longer returns the number of bytes read upon success.  Instead, it returns 0
  upon success and a negative value upon failure.

* 'ceph scrub', 'ceph compact' and 'ceph sync force are now DEPRECATED.  Users
  should instead use 'ceph mon scrub', 'ceph mon compact' and
  'ceph mon sync force'.

* 'ceph mon_metadata' should now be used as 'ceph mon metadata'. There is no
  need to deprecate this command (same major release since it was first
  introduced).

* The `--dump-json` option of "osdmaptool" is replaced by `--dump json`.

* The commands of "pg ls-by-{pool,primary,osd}" and "pg ls" now take "recovering"
  instead of "recovery", to include the recovering pgs in the listed pgs.

Notable Changes since Hammer
----------------------------

* aarch64: add optimized version of crc32c (Yazen Ghannam, Steve Capper)
* auth: cache/reuse crypto lib key objects, optimize msg signature check (Sage Weil)
* auth: reinit NSS after fork() (#11128 Yan, Zheng)
* autotools: fix out of tree build (Krxysztof Kosinski)
* autotools: improve make check output (Loic Dachary)
* buffer: add invalidate_crc() (Piotr Dalek)
* buffer: fix zero bug (#12252 Haomai Wang)
* buffer: some cleanup (Michal Jarzabek)
* build: allow tcmalloc-minimal (Thorsten Behrens)
* build: C++11 now supported
* build: cmake: fix nss linking (Danny Al-Gaaf)
* build: cmake: misc fixes (Orit Wasserman, Casey Bodley)
* build: disable LTTNG by default (#11333 Josh Durgin)
* build: do not build ceph-dencoder with tcmalloc (#10691 Boris Ranto)
* build: fix junit detection on Fedora 22 (Ira Cooper)
* build: fix pg ref disabling (William A. Kennington III)
* build: fix ppc build (James Page)
* build: install-deps: misc fixes (Loic Dachary)
* build: install-deps.sh improvements (Loic Dachary)
* build: install-deps: support OpenSUSE (Loic Dachary)
* build: make_dist_tarball.sh (Sage Weil)
* build: many cmake improvements
* build: misc cmake fixes (Matt Benjamin)
* build: misc fixes (Boris Ranto, Ken Dreyer, Owen Synge)
* build: OSX build fixes (Yan, Zheng)
* build: remove rest-bench
* ceph-authtool: fix return code on error (Gerhard Muntingh)
* ceph-detect-init: added Linux Mint (Michal Jarzabek)
* ceph-detect-init: robust init system detection (Owen Synge)
* ceph-disk: ensure 'zap' only operates on a full disk (#11272 Loic Dachary)
* ceph-disk: fix zap sgdisk invocation (Owen Synge, Thorsten Behrens)
* ceph-disk: follow ceph-osd hints when creating journal (#9580 Sage Weil)
* ceph-disk: handle re-using existing partition (#10987 Loic Dachary)
* ceph-disk: improve parted output parsing (#10983 Loic Dachary)
* ceph-disk: install pip > 6.1 (#11952 Loic Dachary)
* ceph-disk: make suppression work for activate-all and activate-journal (Dan van der Ster)
* ceph-disk: many fixes (Loic Dachary, Alfredo Deza)
* ceph-disk: fixes to respect init system (Loic Dachary, Owen Synge)
* ceph-disk: pass --cluster arg on prepare subcommand (Kefu Chai)
* ceph-disk: support for multipath devices (Loic Dachary)
* ceph-disk: support NVMe device partitions (#11612 Ilja Slepnev)
* ceph: fix 'df' units (Zhe Zhang)
* ceph: fix parsing in interactive cli mode (#11279 Kefu Chai)
* cephfs-data-scan: many additions, improvements (John Spray)
* ceph-fuse: do not require successful remount when unmounting (#10982 Greg Farnum)
* ceph-fuse, libcephfs: don't clear COMPLETE when trimming null (Yan, Zheng)
* ceph-fuse, libcephfs: drop inode when rmdir finishes (#11339 Yan, Zheng)
* ceph-fuse,libcephfs: fix uninline (#11356 Yan, Zheng)
* ceph-fuse, libcephfs: hold exclusive caps on dirs we "own" (#11226 Greg Farnum)
* ceph-fuse: mostly behave on 32-bit hosts (Yan, Zheng)
* ceph: improve error output for 'tell' (#11101 Kefu Chai)
* ceph-monstore-tool: fix store-copy (Huangjun)
* ceph: new 'ceph daemonperf' command (John Spray, Mykola Golub)
* ceph-objectstore-tool: many many improvements (David Zafman)
* ceph-objectstore-tool: refactoring and cleanup (John Spray)
* ceph-post-file: misc fixes (Joey McDonald, Sage Weil)
* ceph_test_rados: test pipelined reads (Zhiqiang Wang)
* client: avoid sending unnecessary FLUSHSNAP messages (Yan, Zheng)
* client: exclude setfilelock when calculating oldest tid (Yan, Zheng)
* client: fix error handling in check_pool_perm (John Spray)
* client: fsync waits only for inode's caps to flush (Yan, Zheng)
* client: invalidate kernel dcache when cache size exceeds limits (Yan, Zheng)
* client: make fsync wait for unsafe dir operations (Yan, Zheng)
* client: pin lookup dentry to avoid inode being freed (Yan, Zheng)
* common: add descriptions to perfcounters (Kiseleva Alyona)
* common: add perf counter descriptions (Alyona Kiseleva)
* common: bufferlist performance tuning (Piotr Dalek, Sage Weil)
* common: detect overflow of int config values (#11484 Kefu Chai)
* common: fix bit_vector extent calc (#12611 Jason Dillaman)
* common: fix json parsing of utf8 (#7387 Tim Serong)
* common: fix leak of pthread_mutexattr (#11762 Ketor Meng)
* common: fix LTTNG vs fork issue (Josh Durgin)
* common: fix throttle max change (Henry Chang)
* common: make mutex more efficient
* common: make work queue addition/removal thread safe (#12662 Jason Dillaman)
* common: optracker improvements (Zhiqiang Wang, Jianpeng Ma)
* common: PriorityQueue tests (Kefu Chai)
* common: some async compression infrastructure (Haomai Wang)
* crush: add --check to validate dangling names, max osd id (Kefu Chai)
* crush: cleanup, sync with kernel (Ilya Dryomov)
* crush: fix crash from invalid 'take' argument (#11602 Shiva Rkreddy, Sage Weil)
* crush: fix divide-by-2 in straw2 (#11357 Yann Dupont, Sage Weil)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* crush: fix subtree base weight on adjust_subtree_weight (#11855 Sage Weil)
* crush: respect default replicated ruleset config on map creation (Ilya Dryomov)
* crushtool: fix order of operations, usage (Sage Weil)
* crypto: fix NSS leak (Jason Dillaman)
* crypto: fix unbalanced init/shutdown (#12598 Zheng Yan)
* deb: fix rest-bench-dbg and ceph-test-dbg dependendies (Ken Dreyer)
* debian: minor package reorg (Ken Dreyer)
* deb, rpm: move ceph-objectstore-tool to ceph (Ken Dreyer)
* doc: docuemnt object corpus generation (#11099 Alexis Normand)
* doc: document region hostnames (Robin H. Johnson)
* doc: fix gender neutrality (Alexandre Maragone)
* doc: fix install doc (#10957 Kefu Chai)
* doc: fix sphinx issues (Kefu Chai)
* doc: man page updates (Kefu Chai)
* doc: mds data structure docs (Yan, Zheng)
* doc: misc updates (Fracois Lafont, Ken Dreyer, Kefu Chai, Owen Synge, Gael Fenet-Garde, Loic Dachary, Yannick Atchy-Dalama, Jiaying Ren, Kevin Caradant, Robert Maxime, Nicolas Yong, Germain Chipaux, Arthur Gorjux, Gabriel Sentucq, Clement Lebrun, Jean-Remi Deveaux, Clair Massot, Robin Tang, Thomas Laumondais, Jordan Dorne, Yuan Zhou, Valentin Thomas, Pierre Chaumont, Benjamin Troquereau, Benjamin Sesia, Vikhyat Umrao, Nilamdyuti Goswami, Vartika Rai, Florian Haas, Loic Dachary, Simon Guinot, Andy Allan, Alistair Israel, Ken Dreyer, Robin Rehu, Lee Revell, Florian Marsylle, Thomas Johnson, Bosse Klykken, Travis Rhoden, Ian Kelling)
* doc: swift tempurls (#10184 Abhishek Lekshmanan)
* doc: switch doxygen integration back to breathe (#6115 Kefu Chai)
* doc: update release schedule docs (Loic Dachary)
* erasure-code: cleanup (Kefu Chai)
* erasure-code: improve tests (Loic Dachary)
* erasure-code: shec: fix recovery bugs (Takanori Nakao, Shotaro Kawaguchi)
* erasure-code: update ISA-L to 2.13 (Yuan Zhou)
* gmock: switch to submodule (Danny Al-Gaaf, Loic Dachary)
* hadoop: add terasort test (Noah Watkins)
* init-radosgw: merge with sysv version; fix enumeration (Sage Weil)
* java: fix libcephfs bindings (Noah Watkins)
* libcephfs: add pread, pwrite (Jevon Qiao)
* libcephfs,ceph-fuse: cache cleanup (Zheng Yan)
* libcephfs,ceph-fuse: fix request resend on cap reconnect (#10912 Yan, Zheng)
* librados: add config observer (Alistair Strachan)
* librados: add FULL_TRY and FULL_FORCE flags for dealing with full clusters or pools (Sage Weil)
* librados: add src_fadvise_flags for copy-from (Jianpeng Ma)
* librados: define C++ flags from C constants (Josh Durgin)
* librados: fadvise flags per op (Jianpeng Ma)
* librados: fix last_force_resent handling (#11026 Jianpeng Ma)
* librados: fix memory leak from C_TwoContexts (Xiong Yiliang)
* librados: fix notify completion race (#13114 Sage Weil)
* librados: fix striper when stripe_count = 1 and stripe_unit != object_size (#11120 Yan, Zheng)
* librados, libcephfs: randomize client nonces (Josh Durgin)
* librados: op perf counters (John Spray)
* librados: pybind: fix binary omap values (Robin H. Johnson)
* librados: pybind: fix write() method return code (Javier Guerra)
* librados: respect default_crush_ruleset on pool_create (#11640 Yuan Zhou)
* libradosstriper: fix leak (Danny Al-Gaaf)
* librbd: add const for single-client-only features (Josh Durgin)
* librbd: add deep-flatten operation (Jason Dillaman)
* librbd: add purge_on_error cache behavior (Jianpeng Ma)
* librbd: allow additional metadata to be stored with the image (Haomai Wang)
* librbd: avoid blocking aio API methods (#11056 Jason Dillaman)
* librbd: better handling for dup flatten requests (#11370 Jason Dillaman)
* librbd: cancel in-flight ops on watch error (#11363 Jason Dillaman)
* librbd: default new images to format 2 (#11348 Jason Dillaman)
* librbd: fadvise for copy, export, import (Jianpeng Ma)
* librbd: fast diff implementation that leverages object map (Jason Dillaman)
* librbd: fix fast diff bugs (#11553 Jason Dillaman)
* librbd: fix image format detection (Zhiqiang Wang)
* librbd: fix lock ordering issue (#11577 Jason Dillaman)
* librbd: fix reads larger than the cache size (Lu Shi)
* librbd: fix snapshot creation when other snap is active (#11475 Jason Dillaman)
* librbd: flatten/copyup fixes (Jason Dillaman)
* librbd: handle NOCACHE fadvise flag (Jinapeng Ma)
* librbd: lockdep, helgrind validation (Jason Dillaman, Josh Durgin)
* librbd: metadata filter fixes (Haomai Wang)
* librbd: misc aio fixes (#5488 Jason Dillaman)
* librbd: misc rbd fixes (#11478 #11113 #11342 #11380 Jason Dillaman, Zhiqiang Wang)
* librbd: new diff_iterate2 API (Jason Dillaman)
* librbd: object map rebuild support (Jason Dillaman)
* librbd: only update image flags while hold exclusive lock (#11791 Jason Dillaman)
* librbd: optionally disable allocation hint (Haomai Wang)
* librbd: prevent race between resize requests (#12664 Jason Dillaman)
* librbd: readahead fixes (Zhiqiang Wang)
* librbd: return result code from close (#12069 Jason Dillaman)
* librbd: store metadata, including config options, in image (Haomai Wang)
* librbd: tolerate old osds when getting image metadata (#11549 Jason Dillaman)
* librbd: use write_full when possible (Zhiqiang Wang)
* log: fix data corruption race resulting from log rotation (#12465 Samuel Just)
* logrotate.d: prefer service over invoke-rc.d (#11330 Win Hierman, Sage Weil)
* mds: add 'damaged' state to MDSMap (John Spray)
* mds: add nicknames for perfcounters (John Spray)
* mds: avoid emitting cap warnigns before evicting session (John Spray)
* mds: avoid getting stuck in XLOCKDONE (#11254 Yan, Zheng)
* mds: disable problematic rstat propagation into snap parents (Yan, Zheng)
* mds: do not add snapped items to bloom filter (Yan, Zheng)
* mds: expose frags via asok (John Spray)
* mds: fix expected holes in journal objects (#13167 Yan, Zheng)
* mds: fix handling for missing mydir dirfrag (#11641 John Spray)
* mds: fix integer truncateion on large client ids (Henry Chang)
* mds: fix mydir replica issue with shutdown (#10743 John Spray)
* mds: fix out-of-order messages (#11258 Yan, Zheng)
* mds: fix rejoin (Yan, Zheng)
* mds: fix setting entire file layout in one setxattr (John Spray)
* mds: fix shutdown (John Spray)
* mds: fix shutdown with strays (#10744 John Spray)
* mds: fix SnapServer crash on deleted pool (John Spray)
* mds: fix snapshot bugs (Yan, Zheng)
* mds: fix stray reintegration (Yan, Zheng)
* mds: fix stray handling (John Spray)
* mds: fix suicide beacon (John Spray)
* mds: flush immediately in do_open_truncate (#11011 John Spray)
* mds: handle misc corruption issues (John Spray)
* mds: improve dump methods (John Spray)
* mds: many fixes (Yan, Zheng, John Spray, Greg Farnum)
* mds: many snapshot and stray fixes (Yan, Zheng)
* mds: misc fixes (Jianpeng Ma, Dan van der Ster, Zhang Zhi)
* mds: misc journal cleanups and fixes (#10368 John Spray)
* mds: misc repair improvements (John Spray)
* mds: misc snap fixes (Zheng Yan)
* mds: misc snapshot fixes (Yan, Zheng)
* mds: new SessionMap storage using omap (#10649 John Spray)
* mds: persist completed_requests reliably (#11048 John Spray)
* mds: reduce memory consumption (Yan, Zheng)
* mds: respawn instead of suicide on blacklist (John Spray)
* mds: separate safe_pos in Journaler (#10368 John Spray)
* mds: snapshot rename support (#3645 Yan, Zheng)
* mds: store layout on header object (#4161 John Spray)
* mds: throttle purge stray operations (#10390 John Spray)
* mds: tolerate clock jumping backwards (#11053 Yan, Zheng)
* mds: warn when clients fail to advance oldest_client_tid (#10657 Yan, Zheng)
* misc cleanups and fixes (Danny Al-Gaaf)
* misc coverity fixes (Danny Al-Gaaf)
* misc performance and cleanup (Nathan Cutler, Xinxin Shu)
* mon: add cache over MonitorDBStore (Kefu Chai)
* mon: add 'mon_metadata <id>' command (Kefu Chai)
* mon: add 'node ls ...' command (Kefu Chai)
* mon: add NOFORWARD, OBSOLETE, DEPRECATE flags for mon commands (Joao Eduardo Luis)
* mon: add PG count to 'ceph osd df' output (Michal Jarzabek)
* mon: 'ceph osd metadata' can dump all osds (Haomai Wang)
* mon: clean up, reorg some mon commands (Joao Eduardo Luis)
* monclient: flush_log (John Spray)
* mon: detect kv backend failures (Sage Weil)
* mon: disallow >2 tiers (#11840 Kefu Chai)
* mon: disallow ec pools as tiers (#11650 Samuel Just)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: fix average utilization calc for 'osd df' (Mykola Golub)
* mon: fix CRUSH map test for new pools (Sage Weil)
* mon: fix log dump crash when debugging (Mykola Golub)
* mon: fix mds beacon replies (#11590 Kefu Chai)
* mon: fix metadata update race (Mykola Golub)
* mon: fix min_last_epoch_clean tracking (Kefu Chai)
* mon: fix 'pg ls' sort order, state names (#11569 Kefu Chai)
* mon: fix refresh (#11470 Joao Eduardo Luis)
* mon: fix variance calc in 'osd df' (Sage Weil)
* mon: improve callout to crushtool (Mykola Golub)
* mon: make blocked op messages more readable (Jianpeng Ma)
* mon: make osd get pool 'all' only return applicable fields (#10891 Michal Jarzabek)
* mon: misc scaling fixes (Sage Weil)
* mon: normalize erasure-code profile for storage and comparison (Loic Dachary)
* mon: only send mon metadata to supporting peers (Sage Weil)
* mon: optionally specify osd id on 'osd create' (Mykola Golub)
* mon: 'osd tree' fixes (Kefu Chai)
* mon: periodic background scrub (Joao Eduardo Luis)
* mon: prevent bucket deletion when referenced by a crush rule (#11602 Sage Weil)
* mon: prevent pgp_num > pg_num (#12025 Xinxin Shu)
* mon: prevent pool with snapshot state from being used as a tier (#11493 Sage Weil)
* mon: prime pg_temp when CRUSH map changes (Sage Weil)
* mon: refine check_remove_tier checks (#11504 John Spray)
* mon: reject large max_mds values (#12222 John Spray)
* mon: remove spurious who arg from 'mds rm ...' (John Spray)
* mon: streamline session handling, fix memory leaks (Sage Weil)
* mon: upgrades must pass through hammer (Sage Weil)
* mon: warn on bogus cache tier config (Jianpeng Ma)
* msgr: add ceph_perf_msgr tool (Hoamai Wang)
* msgr: async: fix seq handling (Haomai Wang)
* msgr: async: many many fixes (Haomai Wang)
* msgr: simple: fix clear_pipe (#11381 Haomai Wang)
* msgr: simple: fix connect_seq assert (Haomai Wang)
* msgr: xio: fastpath improvements (Raju Kurunkad)
* msgr: xio: fix ip and nonce (Raju Kurunkad)
* msgr: xio: improve lane assignment (Vu Pham)
* msgr: xio: sync with accellio v1.4 (Vu Pham)
* msgr: xio: misc fixes (#10735 Matt Benjamin, Kefu Chai, Danny Al-Gaaf, Raju Kurunkad, Vu Pham, Casey Bodley)
* msg: unit tests (Haomai Wang)
* objectcacher: misc bug fixes (Jianpeng Ma)
* osd: add latency perf counters for tier operations (Xinze Chi)
* osd: add misc perfcounters (Xinze Chi)
* osd: add simple sleep injection in recovery (Sage Weil)
* osd: allow SEEK_HOLE/SEEK_DATA for sparse read (Zhiqiang Wang)
* osd: avoid dup omap sets for in pg metadata (Sage Weil)
* osd: avoid multiple hit set insertions (Zhiqiang Wang)
* osd: avoid transaction append in some cases (Sage Weil)
* osd: break PG removal into multiple iterations (#10198 Guang Yang)
* osd: cache proxy-write support (Zhiqiang Wang, Samuel Just)
* osd: check scrub state when handling map (Jianpeng Ma)
* osd: clean up some constness, privateness (Kefu Chai)
* osd: clean up temp object if promotion fails (Jianpeng Ma)
* osd: configure promotion based on write recency (Zhiqiang Wang)
* osd: constrain collections to meta and PGs (normal and temp) (Sage Weil)
* osd: don't send dup MMonGetOSDMap requests (Sage Weil, Kefu Chai)
* osd: EIO injection (David Zhang)
* osd: elminiate txn apend, ECSubWrite copy (Samuel Just)
* osd: erasure-code: drop entries according to LRU (Andreas-Joachim Peters)
* osd: erasure-code: fix SHEC floating point bug (#12936 Loic Dachary)
* osd: erasure-code: update to ISA-L 2.14 (Yuan Zhou)
* osd: filejournal: cleanup (David Zafman)
* osd: filestore: clone using splice (Jianpeng Ma)
* osd: filestore: fix recursive lock (Xinxin Shu)
* osd: fix check_for_full (Henry Chang)
* osd: fix dirty accounting in make_writeable (Zhiqiang Wang)
* osd: fix dup promotion lost op bug (Zhiqiang Wang)
* osd: fix endless repair when object is unrecoverable (Jianpeng Ma, Kefu Chai)
* osd: fix hitset object naming to use GMT (Kefu Chai)
* osd: fix misc memory leaks (Sage Weil)
* osd: fix negative degraded stats during backfill (Guang Yang)
* osd: fix osdmap dump of blacklist items (John Spray)
* osd: fix peek_queue locking in FileStore (Xinze Chi)
* osd: fix pg resurrection (#11429 Samuel Just)
* osd: fix promotion vs full cache tier (Samuel Just)
* osd: fix replay requeue when pg is still activating (#13116 Samuel Just)
* osd: fix scrub stat bugs (Sage Weil, Samuel Just)
* osd: fix snap flushing from cache tier (again) (#11787 Samuel Just)
* osd: fix snap handling on promotion (#11296 Sam Just)
* osd: fix temp-clearing (David Zafman)
* osd: force promotion for ops EC can't handle (Zhiqiang Wang)
* osd: handle log split with overlapping entries (#11358 Samuel Just)
* osd: ignore non-existent osds in unfound calc (#10976 Mykola Golub)
* osd: improve behavior on machines with large memory pages (Steve Capper)
* osd: include a temp namespace within each collection/pgid (Sage Weil)
* osd: increase default max open files (Owen Synge)
* osd: keyvaluestore: misc fixes (Varada Kari)
* osd: low and high speed flush modes (Mingxin Liu)
* osd: make suicide timeouts individually configurable (Samuel Just)
* osd: merge multiple setattr calls into a setattrs call (Xinxin Shu)
* osd: misc fixes (Ning Yao, Kefu Chai, Xinze Chi, Zhiqiang Wang, Jianpeng Ma)
* osd: move scrub in OpWQ (Samuel Just)
* osd: newstore prototype (Sage Weil)
* osd: ObjectStore internal API refactor (Sage Weil)
* osd: peer_features includes self (David Zafman)
* osd: pool size change triggers new interval (#11771 Samuel Just)
* osd: prepopulate needs_recovery_map when only one peer has missing (#9558 Guang Yang)
* osd: randomize scrub times (#10973 Kefu Chai)
* osd: recovery, peering fixes (#11687 Samuel Just)
* osd: refactor scrub and digest recording (Sage Weil)
* osd: refuse first write to EC object at non-zero offset (Jianpeng Ma)
* osd: relax reply order on proxy read (#11211 Zhiqiang Wang)
* osd: require firefly features (David Zafman)
* osd: set initial crush weight with more precision (Sage Weil)
* osd: SHEC no longer experimental
* osd: skip promotion for flush/evict op (Zhiqiang Wang)
* osd: stripe over small xattrs to fit in XFS's 255 byte inline limit (Sage Weil, Ning Yao)
* osd: sync object_map on syncfs (Samuel Just)
* osd: take excl lock of op is rw (Samuel Just)
* osd: throttle evict ops (Yunchuan Wen)
* osd: upgrades must pass through hammer (Sage Weil)
* osd: use a temp object for recovery (Sage Weil)
* osd: use blkid to collection partition information (Joseph Handzik)
* osd: use SEEK_HOLE / SEEK_DATA for sparse copy (Xinxin Shu)
* osd: WBThrottle cleanups (Jianpeng Ma)
* osd: write journal header on clean shutdown (Xinze Chi)
* osdc/Objecter: allow per-pool calls to op_cancel_writes (John Spray)
* os/filestore: enlarge getxattr buffer size (Jianpeng Ma)
* pybind: pep8 cleanups (Danny Al-Gaaf)
* pycephfs: many fixes for bindings (Haomai Wang)
* qa: fix filelock_interrupt.py test (Yan, Zheng)
* qa: improve ceph-disk tests (Loic Dachary)
* qa: improve docker build layers (Loic Dachary)
* qa: run-make-check.sh script (Loic Dachary)
* rados: add --striper option to use libradosstriper (#10759 Sebastien Ponce)
* rados: bench: add --no-verify option to improve performance (Piotr Dalek)
* rados bench: misc fixes (Dmitry Yatsushkevich)
* rados: fix error message on failed pool removal (Wido den Hollander)
* radosgw-admin: add 'bucket check' function to repair bucket index (Yehuda Sadeh)
* radosgw-admin: fix subuser modify output (#12286 Guce)
* rados: handle --snapid arg properly (Abhishek Lekshmanan)
* rados: improve bench buffer handling, performance (Piotr Dalek)
* rados: misc bench fixes (Dmitry Yatsushkevich)
* rados: new pool import implementation (John Spray)
* rados: translate errno to string in CLI (#10877 Kefu Chai)
* rbd: accept map options config option (Ilya Dryomov)
* rbd: add disk usage tool (#7746 Jason Dillaman)
* rbd: allow unmapping by spec (Ilya Dryomov)
* rbd: cli: fix arg parsing with --io-pattern (Dmitry Yatsushkevich)
* rbd: deprecate --new-format option (Jason Dillman)
* rbd: fix error messages (#2862 Rajesh Nambiar)
* rbd: fix link issues (Jason Dillaman)
* rbd: improve CLI arg parsing, usage (Ilya Dryomov)
* rbd: rbd-replay-prep and rbd-replay improvements (Jason Dillaman)
* rbd: recognize queue_depth kernel option (Ilya Dryomov)
* rbd: support G and T units for CLI (Abhishek Lekshmanan)
* rbd: update rbd man page (Ilya Dryomov)
* rbd: update xfstests tests (Douglas Fuller)
* rbd: use image-spec and snap-spec in help (Vikhyat Umrao, Ilya Dryomov)
* rest-bench: misc fixes (Shawn Chen)
* rest-bench: support https (#3968 Yuan Zhou)
* rgw: add max multipart upload parts (#12146 Abshishek Dixit)
* rgw: add missing headers to Swift container details (#10666 Ahmad Faheem, Dmytro Iurchenko)
* rgw: add stats to headers for account GET (#10684 Yuan Zhou)
* rgw: add Trasnaction-Id to response (Abhishek Dixit)
* rgw: add X-Timestamp for Swift containers (#10938 Radoslaw Zarzynski)
* rgw: always check if token is expired (#11367 Anton Aksola, Riku Lehto)
* rgw: conversion tool to repair broken multipart objects (#12079 Yehuda Sadeh)
* rgw: document layout of pools and objects (Pete Zaitcev)
* rgw: do not enclose bucket header in quotes (#11860 Wido den Hollander)
* rgw: do not prefetch data for HEAD requests (Guang Yang)
* rgw: do not preserve ACLs when copying object (#12370 Yehuda Sadeh)
* rgw: do not set content-type if length is 0 (#11091 Orit Wasserman)
* rgw: don't clobber bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: don't use end_marker for namespaced object listing (#11437 Yehuda Sadeh)
* rgw: don't use rgw_socket_path if frontend is configured (#11160 Yehuda Sadeh)
* rgw: enforce Content-Length for POST on Swift cont/obj (#10661 Radoslaw Zarzynski)
* rgw: error out if frontend did not send all data (#11851 Yehuda Sadeh)
* rgw: expose the number of unhealthy workers through admin socket (Guang Yang)
* rgw: fail if parts not specified on multipart upload (#11435 Yehuda Sadeh)
* rgw: fix assignment of copy obj attributes (#11563 Yehuda Sadeh)
* rgw: fix broken stats in container listing (#11285 Radoslaw Zarzynski)
* rgw: fix bug in domain/subdomain splitting (Robin H. Johnson)
* rgw: fix casing of Content-Type header (Robin H. Johnson)
* rgw: fix civetweb max threads (#10243 Yehuda Sadeh)
* rgw: fix Connection: header handling (#12298 Wido den Hollander)
* rgw: fix copy metadata, support X-Copied-From for swift (#10663 Radoslaw Zarzynski)
* rgw: fix data corruptions race condition (#11749 Wuxingyi)
* rgw: fix decoding of X-Object-Manifest from GET on Swift DLO (Radslow Rzarzynski)
* rgw: fix GET on swift account when limit == 0 (#10683 Radoslaw Zarzynski)
* rgw: fix handling empty metadata items on Swift container (#11088 Radoslaw Zarzynski)
* rgw: fix JSON response when getting user quota (#12117 Wuxingyi)
* rgw: fix locator for objects starting with _ (#11442 Yehuda Sadeh)
* rgw: fix log rotation (Wuxingyi)
* rgw: fix mulitipart upload in retry path (#11604 Yehuda Sadeh)
* rgw: fix quota enforcement on POST (#11323 Sergey Arkhipov)
* rgw: fix reset_loc (#11974 Yehuda Sadeh)
* rgw: fix return code on missing upload (#11436 Yehuda Sadeh)
* rgw: fix sysvinit script
* rgw: fix sysvinit script w/ multiple instances (Sage Weil, Pavan Rallabhandi)
* rgw: force content_type for swift bucket stats requests (#12095 Orit Wasserman)
* rgw: force content type header on responses with no body (#11438 Orit Wasserman)
* rgw: generate Date header for civetweb (#10873 Radoslaw Zarzynski)
* rgw: generate new object tag when setting attrs (#11256 Yehuda Sadeh)
* rgw: improve content-length env var handling (#11419 Robin H. Johnson)
* rgw: improved support for swift account metadata (Radoslaw Zarzynski)
* rgw: improve handling of already removed buckets in expirer (Radoslaw Rzarzynski)
* rgw: issue aio for first chunk before flush cached data (#11322 Guang Yang)
* rgw: log to /var/log/ceph instead of /var/log/radosgw
* rgw: make init script wait for radosgw to stop (#11140 Dmitry Yatsushkevich)
* rgw: make max put size configurable (#6999 Yuan Zhou)
* rgw: make quota/gc threads configurable (#11047 Guang Yang)
* rgw: make read user buckets backward compat (#10683 Radoslaw Zarzynski)
* rgw: merge manifests properly with prefix override (#11622 Yehuda Sadeh)
* rgw: only scan for objects not in a namespace (#11984 Yehuda Sadeh)
* rgw: orphan detection tool (Yehuda Sadeh)
* rgw: pass in civetweb configurables (#10907 Yehuda Sadeh)
* rgw: rectify 202 Accepted in PUT response (#11148 Radoslaw Zarzynski)
* rgw: remove meta file after deleting bucket (#11149 Orit Wasserman)
* rgw: remove trailing :port from HTTP_HOST header (Sage Weil)
* rgw: return 412 on bad limit when listing buckets (#11613 Yehuda Sadeh)
* rgw: rework X-Trans-Id header to conform with Swift API (Radoslaw Rzarzynski)
* rgw: s3 encoding-type for get bucket (Jeff Weber)
* rgw: send ETag, Last-Modified for swift (#11087 Radoslaw Zarzynski)
* rgw: set content length on container GET, PUT, DELETE, HEAD (#10971, #11036 Radoslaw Zarzynski)
* rgw: set max buckets per user in ceph.conf (Vikhyat Umrao)
* rgw: shard work over multiple librados instances (Pavan Rallabhandi)
* rgw: support end marker on swift container GET (#10682 Radoslaw Zarzynski)
* rgw: support for Swift expiration API (Radoslaw Rzarzynski, Yehuda Sadeh)
* rgw: swift: allow setting attributes with COPY (#10662 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: do not override sent content type (#12363 Orit Wasserman)
* rgw: swift: enforce Content-Type in response (#12157 Radoslaw Zarzynski)
* rgw: swift: fix account listing (#11501 Radoslaw Zarzynski)
* rgw: swift: fix metadata handling on copy (#10645 Radoslaw Zarzynski)
* rgw: swift: send Last-Modified header (#10650 Radoslaw Zarzynski)
* rgw: swift: set Content-Length for account GET (#12158 Radoslav Zarzynski)
* rgw: swift: set content-length on keystone tokens (#11473 Herv Rousseau)
* rgw: update keystone cache with token info (#11125 Yehuda Sadeh)
* rgw: update to latest civetweb, enable config for IPv6 (#10965 Yehuda Sadeh)
* rgw: use attrs from source bucket on copy (#11639 Javier M. Mellid)
* rgw: use correct oid for gc chains (#11447 Yehuda Sadeh)
* rgw: user rm is idempotent (Orit Wasserman)
* rgw: use unique request id for civetweb (#10295 Orit Wasserman)
* rocksdb: add perf counters for get/put latency (Xinxin Shu)
* rocksdb, leveldb: fix compact_on_mount (Xiaoxi Chen)
* rocksdb: pass options as single string (Xiaoxi Chen)
* rocksdb: update to latest (Xiaoxi Chen)
* rpm: add suse firewall files (Tim Serong)
* rpm: always rebuild and install man pages for rpm (Owen Synge)
* rpm: loosen ceph-test dependencies (Ken Dreyer)
* rpm: many spec file fixes (Owen Synge, Ken Dreyer)
* rpm: misc fixes (Boris Ranto, Owen Synge, Ken Dreyer, Ira Cooper)
* rpm: misc systemd and SUSE fixes (Owen Synge, Nathan Cutler)
* selinux policy (Boris Ranto, Milan Broz)
* systemd: logrotate fixes (Tim Serong, Lars Marowsky-Bree, Nathan Cutler)
* systemd: many fixes (Sage Weil, Owen Synge, Boris Ranto, Dan van der Ster)
* systemd: run daemons as user ceph
* sysvinit compat: misc fixes (Owen Synge)
* test: misc fs test improvements (John Spray, Loic Dachary)
* test: python tests, linter cleanup (Alfredo Deza)
* tests: fixes for rbd xstests (Douglas Fuller)
* tests: fix tiering health checks (Loic Dachary)
* tests for low-level performance (Haomai Wang)
* tests: many ec non-regression improvements (Loic Dachary)
* tests: many many ec test improvements (Loic Dachary)
* upstart: throttle restarts (#11798 Sage Weil, Greg Farnum)


v9.1.0 Infernalis release candidate
===================================

This is the first Infernalis release candidate.  There have been some
major changes since Hammer, and the upgrade process is non-trivial.
Please read carefully.

Getting the release candidate
-----------------------------

The v9.1.0 packages are pushed to the development release repositories::
  
  http://download.ceph.com/rpm-testing
  http://download.ceph.com/debian-testing

For for info, see::
  
  http://docs.ceph.com/docs/master/install/get-packages/

Or install with ceph-deploy via::
  
  ceph-deploy install --testing HOST


Known issues
------------

* librbd and librados ABI compatibility is broken.  Be careful
  installing this RC on client machines (e.g., those running qemu).
  It will be fixed in the final v9.2.0 release.


Major Changes from Hammer
-------------------------

- *General*:

  * Ceph daemons are now managed via systemd (with the exception of
    Ubuntu Trusty, which still uses upstart).
  * Ceph daemons run as 'ceph' user instead of root.
  * On Red Hat distros, there is also an SELinux policy.

- *RADOS*:

  * The RADOS cache tier can now proxy write operations to the base
    tier, allowing writes to be handled without forcing migration of
    an object into the cache.
  * The SHEC erasure coding support is no longer flagged as
    experimental. SHEC trades some additional storage space for faster
    repair.
  * There is now a unified queue (and thus prioritization) of client
    IO, scrubbing, and snapshot trimming.
  * There have been many improvements to low-level repair tooling
    (ceph-objectstore-tool).
  * The internal ObjectStore API has been significantly cleaned up in order
    to faciliate new storage backends like NewStore.

- *RGW*:

  * The Swift API now supports object expiration.
  * There are many Swift API compatibility improvements.

- *RBD*:

  * The ``rbd du`` command shows actual usage (quickly, when
    object-map is enabled).
  * The object-map feature has seen many stability improvements.
  * Object-map and exclusive-lock features can be enabled or disabled
    dynamically.
  * You can now store user metadata and set persistent librbd options
    associated with individual images.
  * The new deep-flatten features allows flattening of a clone and all
    of its snapshots.  (Previously snapshots could not be flattened.)
  * The export-diff command command is now faster (it uses aio).  There is also
    a new fast-diff feature.
  * The --size argument can be specified with a suffix for units
    (e.g., ``--size 64G``).
  * There is a new ``rbd status`` command that, for now, shows who has
    the image open/mapped.

- *CephFS*:

  * You can now rename snapshots.
  * There have been ongoing improvements around administration, diagnostics,
    and the check and repair tools.
  * The caching and revocation of client cache state due to unused
    inodes has been dramatically improved.
  * The ceph-fuse client behaves better on 32-bit hosts.

Distro compatibility
--------------------

We have decided to drop support for many older distributions so that we can
move to a newer compiler toolchain (e.g., C++11).  Although it is still possible
to build Ceph on older distributions by installing backported development tools,
we are not building and publishing release packages for them on ceph.com.

In particular,

* CentOS 7 or later; we have dropped support for CentOS 6 (and other
  RHEL 6 derivatives, like Scientific Linux 6).
* Debian Jessie 8.x or later; Debian Wheezy 7.x's g++ has incomplete
  support for C++11 (and no systemd).
* Ubuntu Trusty 14.04 or later; Ubuntu Precise 12.04 is no longer
  supported.
* Fedora 22 or later.

Upgrading from Firefly
----------------------

Upgrading directly from Firefly v0.80.z is not possible.  All clusters
must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
then is it possible to do online upgrade to Infernalis 9.2.z.

User can upgrade to latest hammer v0.94.z 
from gitbuilder with(also refer the hammer release notes for more details)::

  ceph-deploy install --release hammer HOST


Upgrading from Hammer
---------------------

* All cluster nodes must first upgrade to Hammer v0.94.4 or a later v0.94.z release; only
  then is it possible to do online upgrade to Infernalis 9.2.z.

* For all distributions that support systemd (CentOS 7, Fedora, Debian
  Jessie 8.x, OpenSUSE), ceph daemons are now managed using native systemd
  files instead of the legacy sysvinit scripts.  For example,::

    systemctl start ceph.target       # start all daemons
    systemctl status ceph-osd@12      # check status of osd.12

  The main notable distro that is *not* yet using systemd is Ubuntu trusty
  14.04.  (The next Ubuntu LTS, 16.04, will use systemd instead of upstart.)
    
* Ceph daemons now run as user and group ``ceph`` by default.  The
  ceph user has a static UID assigned by Fedora and Debian (also used
  by derivative distributions like RHEL/CentOS and Ubuntu).  On SUSE
  the ceph user will currently get a dynamically assigned UID when the
  user is created.

  If your systems already have a ceph user, the package upgrade
  process will usually fail with an error.  We suggest you first
  remove or rename the existing 'ceph' user and then upgrade.

  When upgrading, administrators have two options:

   #. Add the following line to ``ceph.conf`` on all hosts::

        setuser match path = /var/lib/ceph/$type/$cluster-$id

      This will make the Ceph daemons run as root (i.e., not drop
      privileges and switch to user ceph) if the daemon's data
      directory is still owned by root.  Newly deployed daemons will
      be created with data owned by user ceph and will run with
      reduced privileges, but upgraded daemons will continue to run as
      root.

   #. Fix the data ownership during the upgrade.  This is the preferred option,
      but is more work.  The process for each host would be to:

      #. Upgrade the ceph package.  This creates the ceph user and group.  For
	 example::

	   ceph-deploy install --stable infernalis HOST

      #. Stop the daemon(s).::

	   service ceph stop           # fedora, centos, rhel, debian
	   stop ceph-all               # ubuntu
	   
      #. Fix the ownership::

	   chown -R ceph:ceph /var/lib/ceph

      #. Restart the daemon(s).::

	   start ceph-all                # ubuntu
	   systemctl start ceph.target   # debian, centos, fedora, rhel

* The on-disk format for the experimental KeyValueStore OSD backend has
  changed.  You will need to remove any OSDs using that backend before you
  upgrade any test clusters that use it.

Upgrade notes
-------------

* When a pool quota is reached, librados operations now block indefinitely,
  the same way they do when the cluster fills up.  (Previously they would return
  -ENOSPC).  By default, a full cluster or pool will now block.  If your
  librados application can handle ENOSPC or EDQUOT errors gracefully, you can
  get error returns instead by using the new librados OPERATION_FULL_TRY flag.

Notable changes
---------------

NOTE: These notes are somewhat abbreviated while we find a less
time-consuming process for generating them.

* build: C++11 now supported
* build: many cmake improvements
* build: OSX build fixes (Yan, Zheng)
* build: remove rest-bench
* ceph-disk: many fixes (Loic Dachary)
* ceph-disk: support for multipath devices (Loic Dachary)
* ceph-fuse: mostly behave on 32-bit hosts (Yan, Zheng)
* ceph-objectstore-tool: many improvements (David Zafman)
* common: bufferlist performance tuning (Piotr Dalek, Sage Weil)
* common: make mutex more efficient
* common: some async compression infrastructure (Haomai Wang)
* librados: add FULL_TRY and FULL_FORCE flags for dealing with full clusters or pools (Sage Weil)
* librados: fix notify completion race (#13114 Sage Weil)
* librados, libcephfs: randomize client nonces (Josh Durgin)
* librados: pybind: fix binary omap values (Robin H. Johnson)
* librbd: fix reads larger than the cache size (Lu Shi)
* librbd: metadata filter fixes (Haomai Wang)
* librbd: use write_full when possible (Zhiqiang Wang)
* mds: avoid emitting cap warnigns before evicting session (John Spray)
* mds: fix expected holes in journal objects (#13167 Yan, Zheng)
* mds: fix SnapServer crash on deleted pool (John Spray)
* mds: many fixes (Yan, Zheng, John Spray, Greg Farnum)
* mon: add cache over MonitorDBStore (Kefu Chai)
* mon: 'ceph osd metadata' can dump all osds (Haomai Wang)
* mon: detect kv backend failures (Sage Weil)
* mon: fix CRUSH map test for new pools (Sage Weil)
* mon: fix min_last_epoch_clean tracking (Kefu Chai)
* mon: misc scaling fixes (Sage Weil)
* mon: streamline session handling, fix memory leaks (Sage Weil)
* mon: upgrades must pass through hammer (Sage Weil)
* msg/async: many fixes (Haomai Wang)
* osd: cache proxy-write support (Zhiqiang Wang, Samuel Just)
* osd: configure promotion based on write recency (Zhiqiang Wang)
* osd: don't send dup MMonGetOSDMap requests (Sage Weil, Kefu Chai)
* osd: erasure-code: fix SHEC floating point bug (#12936 Loic Dachary)
* osd: erasure-code: update to ISA-L 2.14 (Yuan Zhou)
* osd: fix hitset object naming to use GMT (Kefu Chai)
* osd: fix misc memory leaks (Sage Weil)
* osd: fix peek_queue locking in FileStore (Xinze Chi)
* osd: fix promotion vs full cache tier (Samuel Just)
* osd: fix replay requeue when pg is still activating (#13116 Samuel Just)
* osd: fix scrub stat bugs (Sage Weil, Samuel Just)
* osd: force promotion for ops EC can't handle (Zhiqiang Wang)
* osd: improve behavior on machines with large memory pages (Steve Capper)
* osd: merge multiple setattr calls into a setattrs call (Xinxin Shu)
* osd: newstore prototype (Sage Weil)
* osd: ObjectStore internal API refactor (Sage Weil)
* osd: SHEC no longer experimental
* osd: throttle evict ops (Yunchuan Wen)
* osd: upgrades must pass through hammer (Sage Weil)
* osd: use SEEK_HOLE / SEEK_DATA for sparse copy (Xinxin Shu)
* rbd: rbd-replay-prep and rbd-replay improvements (Jason Dillaman)
* rgw: expose the number of unhealthy workers through admin socket (Guang Yang)
* rgw: fix casing of Content-Type header (Robin H. Johnson)
* rgw: fix decoding of X-Object-Manifest from GET on Swift DLO (Radslow Rzarzynski)
* rgw: fix sysvinit script
* rgw: fix sysvinit script w/ multiple instances (Sage Weil, Pavan Rallabhandi)
* rgw: improve handling of already removed buckets in expirer (Radoslaw Rzarzynski)
* rgw: log to /var/log/ceph instead of /var/log/radosgw
* rgw: rework X-Trans-Id header to be conform with Swift API (Radoslaw Rzarzynski)
* rgw: s3 encoding-type for get bucket (Jeff Weber)
* rgw: set max buckets per user in ceph.conf (Vikhyat Umrao)
* rgw: support for Swift expiration API (Radoslaw Rzarzynski, Yehuda Sadeh)
* rgw: user rm is idempotent (Orit Wasserman)
* selinux policy (Boris Ranto, Milan Broz)
* systemd: many fixes (Sage Weil, Owen Synge, Boris Ranto, Dan van der Ster)
* systemd: run daemons as user ceph


v9.0.3
======

This is the second to last batch of development work for the
Infernalis cycle.  The most intrusive change is an internal (non
user-visible) change to the OSD's ObjectStore interface.  Many fixes and
improvements elsewhere across RGW, RBD, and another big pile of CephFS
scrub/repair improvements.

Upgrading
---------

* The return code for librbd's rbd_aio_read and Image::aio_read API methods no
  longer returns the number of bytes read upon success.  Instead, it returns 0
  upon success and a negative value upon failure.

* 'ceph scrub', 'ceph compact' and 'ceph sync force' are now deprecated.  Users
  should instead use 'ceph mon scrub', 'ceph mon compact' and
  'ceph mon sync force'.

* 'ceph mon_metadata' should now be used as 'ceph mon metadata'.

* The `--dump-json` option of "osdmaptool" is replaced by `--dump json`.

* The commands of 'pg ls-by-{pool,primary,osd}' and 'pg ls' now take 'recovering'
  instead of 'recovery' to include the recovering pgs in the listed pgs.


Notable Changes
---------------

  * autotools: fix out of tree build (Krxysztof Kosinski)
  * autotools: improve make check output (Loic Dachary)
  * buffer: add invalidate_crc() (Piotr Dalek)
  * buffer: fix zero bug (#12252 Haomai Wang)
  * build: fix junit detection on Fedora 22 (Ira Cooper)
  * ceph-disk: install pip > 6.1 (#11952 Loic Dachary)
  * cephfs-data-scan: many additions, improvements (John Spray)
  * ceph: improve error output for 'tell' (#11101 Kefu Chai)
  * ceph-objectstore-tool: misc improvements (David Zafman)
  * ceph-objectstore-tool: refactoring and cleanup (John Spray)
  * ceph_test_rados: test pipelined reads (Zhiqiang Wang)
  * common: fix bit_vector extent calc (#12611 Jason Dillaman)
  * common: make work queue addition/removal thread safe (#12662 Jason Dillaman)
  * common: optracker improvements (Zhiqiang Wang, Jianpeng Ma)
  * crush: add --check to validate dangling names, max osd id (Kefu Chai)
  * crush: cleanup, sync with kernel (Ilya Dryomov)
  * crush: fix subtree base weight on adjust_subtree_weight (#11855 Sage Weil)
  * crypo: fix NSS leak (Jason Dillaman)
  * crypto: fix unbalanced init/shutdown (#12598 Zheng Yan)
  * doc: misc updates (Kefu Chai, Owen Synge, Gael Fenet-Garde, Loic Dachary, Yannick Atchy-Dalama, Jiaying Ren, Kevin Caradant, Robert Maxime, Nicolas Yong, Germain Chipaux, Arthur Gorjux, Gabriel Sentucq, Clement Lebrun, Jean-Remi Deveaux, Clair Massot, Robin Tang, Thomas Laumondais, Jordan Dorne, Yuan Zhou, Valentin Thomas, Pierre Chaumont, Benjamin Troquereau, Benjamin Sesia, Vikhyat Umrao)
  * erasure-code: cleanup (Kefu Chai)
  * erasure-code: improve tests (Loic Dachary)
  * erasure-code: shec: fix recovery bugs (Takanori Nakao, Shotaro Kawaguchi)
  * libcephfs: add pread, pwrite (Jevon Qiao)
  * libcephfs,ceph-fuse: cache cleanup (Zheng Yan)
  * librados: add src_fadvise_flags for copy-from (Jianpeng Ma)
  * librados: respect default_crush_ruleset on pool_create (#11640 Yuan Zhou)
  * librbd: fadvise for copy, export, import (Jianpeng Ma)
  * librbd: handle NOCACHE fadvise flag (Jinapeng Ma)
  * librbd: optionally disable allocation hint (Haomai Wang)
  * librbd: prevent race between resize requests (#12664 Jason Dillaman)
  * log: fix data corruption race resulting from log rotation (#12465 Samuel Just)
  * mds: expose frags via asok (John Spray)
  * mds: fix setting entire file layout in one setxattr (John Spray)
  * mds: fix shutdown (John Spray)
  * mds: handle misc corruption issues (John Spray)
  * mds: misc fixes (Jianpeng Ma, Dan van der Ster, Zhang Zhi)
  * mds: misc snap fixes (Zheng Yan)
  * mds: store layout on header object (#4161 John Spray)
  * misc performance and cleanup (Nathan Cutler, Xinxin Shu)
  * mon: add NOFORWARD, OBSOLETE, DEPRECATE flags for mon commands (Joao Eduardo Luis)
  * mon: add PG count to 'ceph osd df' output (Michal Jarzabek)
  * mon: clean up, reorg some mon commands (Joao Eduardo Luis)
  * mon: disallow >2 tiers (#11840 Kefu Chai)
  * mon: fix log dump crash when debugging (Mykola Golub)
  * mon: fix metadata update race (Mykola Golub)
  * mon: fix refresh (#11470 Joao Eduardo Luis)
  * mon: make blocked op messages more readable (Jianpeng Ma)
  * mon: only send mon metadata to supporting peers (Sage Weil)
  * mon: periodic background scrub (Joao Eduardo Luis)
  * mon: prevent pgp_num > pg_num (#12025 Xinxin Shu)
  * mon: reject large max_mds values (#12222 John Spray)
  * msgr: add ceph_perf_msgr tool (Hoamai Wang)
  * msgr: async: fix seq handling (Haomai Wang)
  * msgr: xio: fastpath improvements (Raju Kurunkad)
  * msgr: xio: sync with accellio v1.4 (Vu Pham)
  * osd: clean up temp object if promotion fails (Jianpeng Ma)
  * osd: constrain collections to meta and PGs (normal and temp) (Sage Weil)
  * osd: filestore: clone using splice (Jianpeng Ma)
  * osd: filestore: fix recursive lock (Xinxin Shu)
  * osd: fix dup promotion lost op bug (Zhiqiang Wang)
  * osd: fix temp-clearing (David Zafman)
  * osd: include a temp namespace within each collection/pgid (Sage Weil)
  * osd: low and high speed flush modes (Mingxin Liu)
  * osd: peer_features includes self (David Zafman)
  * osd: recovery, peering fixes (#11687 Samuel Just)
  * osd: require firefly features (David Zafman)
  * osd: set initial crush weight with more precision (Sage Weil)
  * osd: use a temp object for recovery (Sage Weil)
  * osd: use blkid to collection partition information (Joseph Handzik)
  * rados: add --striper option to use libradosstriper (#10759 Sebastien Ponce)
  * radosgw-admin: fix subuser modify output (#12286 Guce)
  * rados: handle --snapid arg properly (Abhishek Lekshmanan)
  * rados: improve bench buffer handling, performance (Piotr Dalek)
  * rados: new pool import implementation (John Spray)
  * rbd: fix link issues (Jason Dillaman)
  * rbd: improve CLI arg parsing, usage (Ilya Dryomov)
  * rbd: recognize queue_depth kernel option (Ilya Dryomov)
  * rbd: support G and T units for CLI (Abhishek Lekshmanan)
  * rbd: use image-spec and snap-spec in help (Vikhyat Umrao, Ilya Dryomov)
  * rest-bench: misc fixes (Shawn Chen)
  * rest-bench: support https (#3968 Yuan Zhou)
  * rgw: add max multipart upload parts (#12146 Abshishek Dixit)
  * rgw: add Trasnaction-Id to response (Abhishek Dixit)
  * rgw: document layout of pools and objects (Pete Zaitcev)
  * rgw: do not preserve ACLs when copying object (#12370 Yehuda Sadeh)
  * rgw: fix Connection: header handling (#12298 Wido den Hollander)
  * rgw: fix data corruptions race condition (#11749 Wuxingyi)
  * rgw: fix JSON response when getting user quota (#12117 Wuxingyi)
  * rgw: force content_type for swift bucket stats requests (#12095 Orit Wasserman)
  * rgw: improved support for swift account metadata (Radoslaw Zarzynski)
  * rgw: make max put size configurable (#6999 Yuan Zhou)
  * rgw: orphan detection tool (Yehuda Sadeh)
  * rgw: swift: do not override sent content type (#12363 Orit Wasserman)
  * rgw: swift: set Content-Length for account GET (#12158 Radoslav Zarzynski)
  * rpm: always rebuild and install man pages for rpm (Owen Synge)
  * rpm: misc fixes (Boris Ranto, Owen Synge, Ken Dreyer, Ira Cooper)
  * systemd: logrotate fixes (Tim Seron, Lars Marowsky-Bree, Nathan Cutler)
  * sysvinit compat: misc fixes (Owen Synge)
  * test: misc fs test improvements (John Spray, Loic Dachary)
  * test: python tests, linter cleanup (Alfredo Deza)


v9.0.2
======

This development release features more of the OSD work queue
unification, randomized osd scrub times, a huge pile of librbd fixes,
more MDS repair and snapshot fixes, and a significant amount of work
on the tests and build infrastructure.

Notable Changes
---------------

* buffer: some cleanup (Michal Jarzabek)
* build: cmake: fix nss linking (Danny Al-Gaaf)
* build: cmake: misc fixes (Orit Wasserman, Casey Bodley)
* build: install-deps: misc fixes (Loic Dachary)
* build: make_dist_tarball.sh (Sage Weil)
* ceph-detect-init: added Linux Mint (Michal Jarzabek)
* ceph-detect-init: robust init system detection (Owen Synge, Loic Dachary)
* ceph-disk: ensure 'zap' only operates on a full disk (#11272 Loic Dachary)
* ceph-disk: misc fixes to respect init system (Loic Dachary, Owen Synge)
* ceph-disk: support NVMe device partitions (#11612 Ilja Slepnev)
* ceph: fix 'df' units (Zhe Zhang)
* ceph: fix parsing in interactive cli mode (#11279 Kefu Chai)
* ceph-objectstore-tool: many many changes (David Zafman)
* ceph-post-file: misc fixes (Joey McDonald, Sage Weil)
* client: avoid sending unnecessary FLUSHSNAP messages (Yan, Zheng)
* client: exclude setfilelock when calculating oldest tid (Yan, Zheng)
* client: fix error handling in check_pool_perm (John Spray)
* client: fsync waits only for inode's caps to flush (Yan, Zheng)
* client: invalidate kernel dcache when cache size exceeds limits (Yan, Zheng)
* client: make fsync wait for unsafe dir operations (Yan, Zheng)
* client: pin lookup dentry to avoid inode being freed (Yan, Zheng)
* common: detect overflow of int config values (#11484 Kefu Chai)
* common: fix json parsing of utf8 (#7387 Tim Serong)
* common: fix leak of pthread_mutexattr (#11762 Ketor Meng)
* crush: respect default replicated ruleset config on map creation (Ilya Dryomov)
* deb, rpm: move ceph-objectstore-tool to ceph (Ken Dreyer)
* doc: man page updates (Kefu Chai)
* doc: misc updates (#11396 Nilamdyuti, Fracois Lafont, Ken Dreyer, Kefu Chai)
* init-radosgw: merge with sysv version; fix enumeration (Sage Weil)
* librados: add config observer (Alistair Strachan)
* librbd: add const for single-client-only features (Josh Durgin)
* librbd: add deep-flatten operation (Jason Dillaman)
* librbd: avoid blocking aio API methods (#11056 Jason Dillaman)
* librbd: fix fast diff bugs (#11553 Jason Dillaman)
* librbd: fix image format detection (Zhiqiang Wang)
* librbd: fix lock ordering issue (#11577 Jason Dillaman)
* librbd: flatten/copyup fixes (Jason Dillaman)
* librbd: lockdep, helgrind validation (Jason Dillaman, Josh Durgin)
* librbd: only update image flags while hold exclusive lock (#11791 Jason Dillaman)
* librbd: return result code from close (#12069 Jason Dillaman)
* librbd: tolerate old osds when getting image metadata (#11549 Jason Dillaman)
* mds: do not add snapped items to bloom filter (Yan, Zheng)
* mds: fix handling for missing mydir dirfrag (#11641 John Spray)
* mds: fix rejoin (Yan, Zheng)
* mds: fix stra reintegration (Yan, Zheng)
* mds: fix suicide beason (John Spray)
* mds: misc repair improvements (John Spray)
* mds: misc snapshot fixes (Yan, Zheng)
* mds: respawn instead of suicide on blacklist (John Spray)
* misc coverity fixes (Danny Al-Gaaf)
* mon: add 'mon_metadata <id>' command (Kefu Chai)
* mon: add 'node ls ...' command (Kefu Chai)
* mon: disallow ec pools as tiers (#11650 Samuel Just)
* mon: fix mds beacon replies (#11590 Kefu Chai)
* mon: fix 'pg ls' sort order, state names (#11569 Kefu Chai)
* mon: normalize erasure-code profile for storage and comparison (Loic Dachary)
* mon: optionally specify osd id on 'osd create' (Mykola Golub)
* mon: 'osd tree' fixes (Kefu Chai)
* mon: prevent pool with snapshot state from being used as a tier (#11493 Sage Weil)
* mon: refine check_remove_tier checks (#11504 John Spray)
* mon: remove spurious who arg from 'mds rm ...' (John Spray)
* msgr: async: misc fixes (Haomai Wang)
* msgr: xio: fix ip and nonce (Raju Kurunkad)
* msgr: xio: improve lane assignment (Vu Pham)
* msgr: xio: misc fixes (Vu Pham, Cosey Bodley)
* osd: avoid transaction append in some cases (Sage Weil)
* osdc/Objecter: allow per-pool calls to op_cancel_writes (John Spray)
* osd: elminiate txn apend, ECSubWrite copy (Samuel Just)
* osd: filejournal: cleanup (David Zafman)
* osd: fix check_for_full (Henry Chang)
* osd: fix dirty accounting in make_writeable (Zhiqiang Wang)
* osd: fix osdmap dump of blacklist items (John Spray)
* osd: fix snap flushing from cache tier (again) (#11787 Samuel Just)
* osd: fix snap handling on promotion (#11296 Sam Just)
* osd: handle log split with overlapping entries (#11358 Samuel Just)
* osd: keyvaluestore: misc fixes (Varada Kari)
* osd: make suicide timeouts individually configurable (Samuel Just)
* osd: move scrub in OpWQ (Samuel Just)
* osd: pool size change triggers new interval (#11771 Samuel Just)
* osd: randomize scrub times (#10973 Kefu Chai)
* osd: refactor scrub and digest recording (Sage Weil)
* osd: refuse first write to EC object at non-zero offset (Jianpeng Ma)
* osd: stripe over small xattrs to fit in XFS's 255 byte inline limit (Sage Weil, Ning Yao)
* osd: sync object_map on syncfs (Samuel Just)
* osd: take excl lock of op is rw (Samuel Just)
* osd: WBThrottle cleanups (Jianpeng Ma)
* pycephfs: many fixes for bindings (Haomai Wang)
* rados: bench: add --no-verify option to improve performance (Piotr Dalek)
* rados: misc bench fixes (Dmitry Yatsushkevich)
* rbd: add disk usage tool (#7746 Jason Dillaman)
* rgw: alwasy check if token is expired (#11367 Anton Aksola, Riku Lehto)
* rgw: conversion tool to repair broken multipart objects (#12079 Yehuda Sadeh)
* rgw: do not enclose bucket header in quotes (#11860 Wido den Hollander)
* rgw: error out if frontend did not send all data (#11851 Yehuda Sadeh)
* rgw: fix assignment of copy obj attributes (#11563 Yehuda Sadeh)
* rgw: fix reset_loc (#11974 Yehuda Sadeh)
* rgw: improve content-length env var handling (#11419 Robin H. Johnson)
* rgw: only scan for objects not in a namespace (#11984 Yehuda Sadeh)
* rgw: remove trailing :port from HTTP_HOST header (Sage Weil)
* rgw: shard work over multiple librados instances (Pavan Rallabhandi)
* rgw: swift: enforce Content-Type in response (#12157 Radoslaw Zarzynski)
* rgw: use attrs from source bucket on copy (#11639 Javier M. Mellid)
* rocksdb: pass options as single string (Xiaoxi Chen)
* rpm: many spec file fixes (Owen Synge, Ken Dreyer)
* tests: fixes for rbd xstests (Douglas Fuller)
* tests: fix tiering health checks (Loic Dachary)
* tests for low-level performance (Haomai Wang)
* tests: many ec non-regression improvements (Loic Dachary)
* tests: many many ec test improvements (Loic Dachary)
* upstart: throttle restarts (#11798 Sage Weil, Greg Farnum)


v9.0.1
======

This development release is delayed a bit due to tooling changes in the build
environment.  As a result the next one (v9.0.2) will have a bit more work than
is usual.

Highlights here include lots of RGW Swift fixes, RBD feature work
surrounding the new object map feature, more CephFS snapshot fixes,
and a few important CRUSH fixes.

Notable Changes
---------------

* auth: cache/reuse crypto lib key objects, optimize msg signature check (Sage Weil)
* build: allow tcmalloc-minimal (Thorsten Behrens)
* build: do not build ceph-dencoder with tcmalloc (#10691 Boris Ranto)
* build: fix pg ref disabling (William A. Kennington III)
* build: install-deps.sh improvements (Loic Dachary)
* build: misc fixes (Boris Ranto, Ken Dreyer, Owen Synge)
* ceph-authtool: fix return code on error (Gerhard Muntingh)
* ceph-disk: fix zap sgdisk invocation (Owen Synge, Thorsten Behrens)
* ceph-disk: pass --cluster arg on prepare subcommand (Kefu Chai)
* ceph-fuse, libcephfs: drop inode when rmdir finishes (#11339 Yan, Zheng)
* ceph-fuse,libcephfs: fix uninline (#11356 Yan, Zheng)
* ceph-monstore-tool: fix store-copy (Huangjun)
* common: add perf counter descriptions (Alyona Kiseleva)
* common: fix throttle max change (Henry Chang)
* crush: fix crash from invalid 'take' argument (#11602 Shiva Rkreddy, Sage Weil)
* crush: fix divide-by-2 in straw2 (#11357 Yann Dupont, Sage Weil)
* deb: fix rest-bench-dbg and ceph-test-dbg dependendies (Ken Dreyer)
* doc: document region hostnames (Robin H. Johnson)
* doc: update release schedule docs (Loic Dachary)
* init-radosgw: run radosgw as root (#11453 Ken Dreyer)
* librados: fadvise flags per op (Jianpeng Ma)
* librbd: allow additional metadata to be stored with the image (Haomai Wang)
* librbd: better handling for dup flatten requests (#11370 Jason Dillaman)
* librbd: cancel in-flight ops on watch error (#11363 Jason Dillaman)
* librbd: default new images to format 2 (#11348 Jason Dillaman)
* librbd: fast diff implementation that leverages object map (Jason Dillaman)
* librbd: fix snapshot creation when other snap is active (#11475 Jason Dillaman)
* librbd: new diff_iterate2 API (Jason Dillaman)
* librbd: object map rebuild support (Jason Dillaman)
* logrotate.d: prefer service over invoke-rc.d (#11330 Win Hierman, Sage Weil)
* mds: avoid getting stuck in XLOCKDONE (#11254 Yan, Zheng)
* mds: fix integer truncateion on large client ids (Henry Chang)
* mds: many snapshot and stray fixes (Yan, Zheng)
* mds: persist completed_requests reliably (#11048 John Spray)
* mds: separate safe_pos in Journaler (#10368 John Spray)
* mds: snapshot rename support (#3645 Yan, Zheng)
* mds: warn when clients fail to advance oldest_client_tid (#10657 Yan, Zheng)
* misc cleanups and fixes (Danny Al-Gaaf)
* mon: fix average utilization calc for 'osd df' (Mykola Golub)
* mon: fix variance calc in 'osd df' (Sage Weil)
* mon: improve callout to crushtool (Mykola Golub)
* mon: prevent bucket deletion when referenced by a crush rule (#11602 Sage Weil)
* mon: prime pg_temp when CRUSH map changes (Sage Weil)
* monclient: flush_log (John Spray)
* msgr: async: many many fixes (Haomai Wang)
* msgr: simple: fix clear_pipe (#11381 Haomai Wang)
* osd: add latency perf counters for tier operations (Xinze Chi)
* osd: avoid multiple hit set insertions (Zhiqiang Wang)
* osd: break PG removal into multiple iterations (#10198 Guang Yang)
* osd: check scrub state when handling map (Jianpeng Ma)
* osd: fix endless repair when object is unrecoverable (Jianpeng Ma, Kefu Chai)
* osd: fix pg resurrection (#11429 Samuel Just)
* osd: ignore non-existent osds in unfound calc (#10976 Mykola Golub)
* osd: increase default max open files (Owen Synge)
* osd: prepopulate needs_recovery_map when only one peer has missing (#9558 Guang Yang)
* osd: relax reply order on proxy read (#11211 Zhiqiang Wang)
* osd: skip promotion for flush/evict op (Zhiqiang Wang)
* osd: write journal header on clean shutdown (Xinze Chi)
* qa: run-make-check.sh script (Loic Dachary)
* rados bench: misc fixes (Dmitry Yatsushkevich)
* rados: fix error message on failed pool removal (Wido den Hollander)
* radosgw-admin: add 'bucket check' function to repair bucket index (Yehuda Sadeh)
* rbd: allow unmapping by spec (Ilya Dryomov)
* rbd: deprecate --new-format option (Jason Dillman)
* rgw: do not set content-type if length is 0 (#11091 Orit Wasserman)
* rgw: don't use end_marker for namespaced object listing (#11437 Yehuda Sadeh)
* rgw: fail if parts not specified on multipart upload (#11435 Yehuda Sadeh)
* rgw: fix GET on swift account when limit == 0 (#10683 Radoslaw Zarzynski)
* rgw: fix broken stats in container listing (#11285 Radoslaw Zarzynski)
* rgw: fix bug in domain/subdomain splitting (Robin H. Johnson)
* rgw: fix civetweb max threads (#10243 Yehuda Sadeh)
* rgw: fix copy metadata, support X-Copied-From for swift (#10663 Radoslaw Zarzynski)
* rgw: fix locator for objects starting with _ (#11442 Yehuda Sadeh)
* rgw: fix mulitipart upload in retry path (#11604 Yehuda Sadeh)
* rgw: fix quota enforcement on POST (#11323 Sergey Arkhipov)
* rgw: fix return code on missing upload (#11436 Yehuda Sadeh)
* rgw: force content type header on responses with no body (#11438 Orit Wasserman)
* rgw: generate new object tag when setting attrs (#11256 Yehuda Sadeh)
* rgw: issue aio for first chunk before flush cached data (#11322 Guang Yang)
* rgw: make read user buckets backward compat (#10683 Radoslaw Zarzynski)
* rgw: merge manifests properly with prefix override (#11622 Yehuda Sadeh)
* rgw: return 412 on bad limit when listing buckets (#11613 Yehuda Sadeh)
* rgw: send ETag, Last-Modified for swift (#11087 Radoslaw Zarzynski)
* rgw: set content length on container GET, PUT, DELETE, HEAD (#10971, #11036 Radoslaw Zarzynski)
* rgw: support end marker on swift container GET (#10682 Radoslaw Zarzynski)
* rgw: swift: fix account listing (#11501 Radoslaw Zarzynski)
* rgw: swift: set content-length on keystone tokens (#11473 Herv Rousseau)
* rgw: use correct oid for gc chains (#11447 Yehuda Sadeh)
* rgw: use unique request id for civetweb (#10295 Orit Wasserman)
* rocksdb, leveldb: fix compact_on_mount (Xiaoxi Chen)
* rocksdb: add perf counters for get/put latency (Xinxin Shu)
* rpm: add suse firewall files (Tim Serong)
* rpm: misc systemd and suse fixes (Owen Synge, Nathan Cutler)



v9.0.0
======

This is the first development release for the Infernalis cycle, and
the first Ceph release to sport a version number from the new
numbering scheme.  The "9" indicates this is the 9th release cycle--I
(for Infernalis) is the 9th letter.  The first "0" indicates this is a
development release ("1" will mean release candidate and "2" will mean
stable release), and the final "0" indicates this is the first such
development release.

A few highlights include:

* a new 'ceph daemonperf' command to watch perfcounter stats in realtime
* reduced MDS memory usage
* many MDS snapshot fixes
* librbd can now store options in the image itself
* many fixes for RGW Swift API support
* OSD performance improvements
* many doc updates and misc bug fixes

Notable Changes
---------------

* aarch64: add optimized version of crc32c (Yazen Ghannam, Steve Capper)
* auth: reinit NSS after fork() (#11128 Yan, Zheng)
* build: disable LTTNG by default (#11333 Josh Durgin)
* build: fix ppc build (James Page)
* build: install-deps: support OpenSUSE (Loic Dachary)
* build: misc cmake fixes (Matt Benjamin)
* ceph-disk: follow ceph-osd hints when creating journal (#9580 Sage Weil)
* ceph-disk: handle re-using existing partition (#10987 Loic Dachary)
* ceph-disk: improve parted output parsing (#10983 Loic Dachary)
* ceph-disk: make suppression work for activate-all and activate-journal (Dan van der Ster)
* ceph-disk: misc fixes (Alfredo Deza)
* ceph-fuse, libcephfs: don't clear COMPLETE when trimming null (Yan, Zheng)
* ceph-fuse, libcephfs: hold exclusive caps on dirs we "own" (#11226 Greg Farnum)
* ceph-fuse: do not require successful remount when unmounting (#10982 Greg Farnum)
* ceph: new 'ceph daemonperf' command (John Spray, Mykola Golub)
* common: PriorityQueue tests (Kefu Chai)
* common: add descriptions to perfcounters (Kiseleva Alyona)
* common: fix LTTNG vs fork issue (Josh Durgin)
* crush: fix has_v4_buckets (#11364 Sage Weil)
* crushtool: fix order of operations, usage (Sage Weil)
* debian: minor package reorg (Ken Dreyer)
* doc: docuemnt object corpus generation (#11099 Alexis Normand)
* doc: fix gender neutrality (Alexandre Maragone)
* doc: fix install doc (#10957 Kefu Chai)
* doc: fix sphinx issues (Kefu Chai)
* doc: mds data structure docs (Yan, Zheng)
* doc: misc updates (Nilamdyuti Goswami, Vartika Rai, Florian Haas, Loic Dachary, Simon Guinot, Andy Allan, Alistair Israel, Ken Dreyer, Robin Rehu, Lee Revell, Florian Marsylle, Thomas Johnson, Bosse Klykken, Travis Rhoden, Ian Kelling)
* doc: swift tempurls (#10184 Abhishek Lekshmanan)
* doc: switch doxygen integration back to breathe (#6115 Kefu Chai)
* erasure-code: update ISA-L to 2.13 (Yuan Zhou)
* gmock: switch to submodule (Danny Al-Gaaf, Loic Dachary)
* hadoop: add terasort test (Noah Watkins)
* java: fix libcephfs bindings (Noah Watkins)
* libcephfs,ceph-fuse: fix request resend on cap reconnect (#10912 Yan, Zheng)
* librados: define C++ flags from C constants (Josh Durgin)
* librados: fix last_force_resent handling (#11026 Jianpeng Ma)
* librados: fix memory leak from C_TwoContexts (Xiong Yiliang)
* librados: fix striper when stripe_count = 1 and stripe_unit != object_size (#11120 Yan, Zheng)
* librados: op perf counters (John Spray)
* librados: pybind: fix write() method return code (Javier Guerra)
* libradosstriper: fix leak (Danny Al-Gaaf)
* librbd: add purge_on_error cache behavior (Jianpeng Ma)
* librbd: misc aio fixes (#5488 Jason Dillaman)
* librbd: misc rbd fixes (#11478 #11113 #11342 #11380 Jason Dillaman, Zhiqiang Wang)
* librbd: readahead fixes (Zhiqiang Wang)
* librbd: store metadata, including config options, in image (Haomai Wang)
* mds: add 'damaged' state to MDSMap (John Spray)
* mds: add nicknames for perfcounters (John Spray)
* mds: disable problematic rstat propagation into snap parents (Yan, Zheng)
* mds: fix mydir replica issue with shutdown (#10743 John Spray)
* mds: fix out-of-order messages (#11258 Yan, Zheng)
* mds: fix shutdown with strays (#10744 John Spray)
* mds: fix snapshot fixes (Yan, Zheng)
* mds: fix stray handling (John Spray)
* mds: flush immediately in do_open_truncate (#11011 John Spray)
* mds: improve dump methods (John Spray)
* mds: misc journal cleanups and fixes (#10368 John Spray)
* mds: new SessionMap storage using omap (#10649 John Spray)
* mds: reduce memory consumption (Yan, Zheng)
* mds: throttle purge stray operations (#10390 John Spray)
* mds: tolerate clock jumping backwards (#11053 Yan, Zheng)
* misc coverity fixes (Danny Al-Gaaf)
* mon: do not deactivate last mds (#10862 John Spray)
* mon: make osd get pool 'all' only return applicable fields (#10891 Michal Jarzabek)
* mon: warn on bogus cache tier config (Jianpeng Ma)
* msg/async: misc bug fixes and updates (Haomai Wang)
* msg/simple: fix connect_seq assert (Haomai Wang)
* msg/xio: misc fixes (#10735 Matt Benjamin, Kefu Chai, Danny Al-Gaaf, Raju Kurunkad, Vu Pham)
* msg: unit tests (Haomai Wang)
* objectcacher: misc bug fixes (Jianpeng Ma)
* os/filestore: enlarge getxattr buffer size (Jianpeng Ma)
* osd: EIO injection (David Zhang)
* osd: add misc perfcounters (Xinze Chi)
* osd: add simple sleep injection in recovery (Sage Weil)
* osd: allow SEEK_HOLE/SEEK_DATA for sparse read (Zhiqiang Wang)
* osd: avoid dup omap sets for in pg metadata (Sage Weil)
* osd: clean up some constness, privateness (Kefu Chai)
* osd: erasure-code: drop entries according to LRU (Andreas-Joachim Peters)
* osd: fix negative degraded stats during backfill (Guang Yang)
* osd: misc fixes (Ning Yao, Kefu Chai, Xinze Chi, Zhiqiang Wang, Jianpeng Ma)
* pybind: pep8 cleanups (Danny Al-Gaaf)
* qa: fix filelock_interrupt.py test (Yan, Zheng)
* qa: improve ceph-disk tests (Loic Dachary)
* qa: improve docker build layers (Loic Dachary)
* rados: translate erno to string in CLI (#10877 Kefu Chai)
* rbd: accept map options config option (Ilya Dryomov)
* rbd: cli: fix arg parsing with --io-pattern (Dmitry Yatsushkevich)
* rbd: fix error messages (#2862 Rajesh Nambiar)
* rbd: update rbd man page (Ilya Dryomov)
* rbd: update xfstests tests (Douglas Fuller)
* rgw: add X-Timestamp for Swift containers (#10938 Radoslaw Zarzynski)
* rgw: add missing headers to Swift container details (#10666 Ahmad Faheem, Dmytro Iurchenko)
* rgw: add stats to headers for account GET (#10684 Yuan Zhou)
* rgw: do not prefecth data for HEAD requests (Guang Yang)
* rgw: don't clobber bucket/object owner when setting ACLs (#10978 Yehuda Sadeh)
* rgw: don't use rgw_socket_path if frontend is configured (#11160 Yehuda Sadeh)
* rgw: enforce Content-Lenth for POST on Swift cont/obj (#10661 Radoslaw Zarzynski)
* rgw: fix handling empty metadata items on Swift container (#11088 Radoslaw Zarzynski)
* rgw: fix log rotation (Wuxingyi)
* rgw: generate Date header for civetweb (#10873 Radoslaw Zarzynski)
* rgw: make init script wait for radosgw to stop (#11140 Dmitry Yatsushkevich)
* rgw: make quota/gc threads configurable (#11047 Guang Yang)
* rgw: pass in civetweb configurables (#10907 Yehuda Sadeh)
* rgw: rectify 202 Accepted in PUT response (#11148 Radoslaw Zarzynski)
* rgw: remove meta file after deleting bucket (#11149 Orit Wasserman)
* rgw: swift: allow setting attributes with COPY (#10662 Ahmad Faheem, Dmytro Iurchenko)
* rgw: swift: fix metadata handling on copy (#10645 Radoslaw Zarzynski)
* rgw: swift: send Last-Modified header (#10650 Radoslaw Zarzynski)
* rgw: update keystone cache with token info (#11125 Yehuda Sadeh)
* rgw: update to latest civetweb, enable config for IPv6 (#10965 Yehuda Sadeh)
* rocksdb: update to latest (Xiaoxi Chen)
* rpm: loosen ceph-test dependencies (Ken Dreyer)

v0.94.6 Hammer
======================

This Hammer point release fixes a range of bugs, most notably a fix
for unbounded growth of the monitor's leveldb store, and a workaround
in the OSD to keep most xattrs small enough to be stored inline in XFS
inodes.

We recommend that all hammer v0.94.x users upgrade.

For more detailed information, see :download:`the complete changelog <changelog/v0.94.6.txt>`.

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
Qemu/KVM to crash when caching is enabled on images that have been cloned.

All v0.94.4 Hammer users are strongly encouraged to upgrade.

Notable Changes
---------------
* librbd: potential assertion failure during cache read (`issue#13559 <http://tracker.ceph.com/issues/13559>`_, `pr#6348 <http://github.com/ceph/ceph/pull/6348>`_, Jason Dillaman)
* osd: osd/ReplicatedPG: remove stray debug line (`issue#13455 <http://tracker.ceph.com/issues/13455>`_, `pr#6362 <http://github.com/ceph/ceph/pull/6362>`_, Sage Weil)
* tests: qemu workunit refers to apt-mirror.front.sepia.ceph.com (`issue#13420 <http://tracker.ceph.com/issues/13420>`_, `pr#6330 <http://github.com/ceph/ceph/pull/6330>`_, Yuan Zhou)

For more detailed information, see :download:`the complete changelog <changelog/v0.94.5.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.94.4.txt>`.


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

For more detailed information, see :download:`the complete changelog <changelog/v0.94.3.txt>`.


  
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

For more detailed information, see :download:`the complete changelog <changelog/v0.94.2.txt>`.


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

For more detailed information, see :download:`the complete changelog <changelog/v0.94.1.txt>`.


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
  workloads so that we can better guage the feature, usability,
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
  better guage the feature, usability, performance, and stability
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

    enable experimental unrecoverable data corrupting featuers = keyvaluestore

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
  that accept this field now ignore it, and the OSD no longers tracks the
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

    enable experimental unrecoverable data corrupting featuers = keyvaluestore

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
  that accept this field now ignore it, and the OSD no longers tracks the
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
include the first batch of scrub patchs from Greg for CephFS, a rework
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


v0.87.2 Giant
=============

This is the second (and possibly final) point release for Giant.

We recommend all v0.87.x Giant users upgrade to this release.

Notable Changes
---------------

* ceph-objectstore-tool: only output unsupported features when incompatible (#11176 David Zafman)
* common: do not implicitly unlock rwlock on destruction (Federico Simoncelli)
* common: make wait timeout on empty queue configurable (#10818 Samuel Just)
* crush: pick ruleset id that matches and rule id (Xiaoxi Chen)
* crush: set_choose_tries = 100 for new erasure code rulesets (#10353 Loic Dachary)
* librados: check initialized atomic safely (#9617 Josh Durgin)
* librados: fix failed tick_event assert (#11183 Zhiqiang Wang)
* librados: fix looping on skipped maps (#9986 Ding Dinghua)
* librados: fix op submit with timeout (#10340 Samuel Just)
* librados: pybind: fix memory leak (#10723 Billy Olsen)
* librados: pybind: keep reference to callbacks (#10775 Josh Durgin)
* librados: translate operation flags from C APIs (Matthew Richards)
* libradosstriper: fix write_full on ENOENT (#10758 Sebastien Ponce)
* libradosstriper: use strtoll instead of strtol (Dongmao Zhang)
* mds: fix assertion caused by system time moving backwards (#11053 Yan, Zheng)
* mon: allow injection of random delays on writes (Joao Eduardo Luis)
* mon: do not trust small osd epoch cache values (#10787 Sage Weil)
* mon: fail non-blocking flush if object is being scrubbed (#8011 Samuel Just)
* mon: fix division by zero in stats dump (Joao Eduardo Luis)
* mon: fix get_rule_avail when no osds (#10257 Joao Eduardo Luis)
* mon: fix timeout rounds period (#10546 Joao Eduardo Luis)
* mon: ignore osd failures before up_from (#10762 Dan van der Ster, Sage Weil)
* mon: paxos: reset accept timeout before writing to store (#10220 Joao Eduardo Luis)
* mon: return if fs exists on 'fs new' (Joao Eduardo Luis)
* mon: use EntityName when expanding profiles (#10844 Joao Eduardo Luis)
* mon: verify cross-service proposal preconditions (#10643 Joao Eduardo Luis)
* mon: wait for osdmon to be writeable when requesting proposal (#9794 Joao Eduardo Luis)
* mount.ceph: avoid spurious error message about /etc/mtab (#10351 Yan, Zheng)
* msg/simple: allow RESETSESSION when we forget an endpoint (#10080 Greg Farnum)
* msg/simple: discard delay queue before incoming queue (#9910 Sage Weil)
* osd: clear_primary_state when leaving Primary (#10059 Samuel Just)
* osd: do not ignore deleted pgs on startup (#10617 Sage Weil)
* osd: fix FileJournal wrap to get header out first (#10883 David Zafman)
* osd: fix PG leak in SnapTrimWQ (#10421 Kefu Chai)
* osd: fix journalq population in do_read_entry (#6003 Samuel Just)
* osd: fix operator== for op_queue_age_hit and fs_perf_stat (#10259 Samuel Just)
* osd: fix rare assert after split (#10430 David Zafman)
* osd: get pgid ancestor from last_map when building past intervals (#10430 David Zafman)
* osd: include rollback_info_trimmed_to in {read,write}_log (#10157 Samuel Just)
* osd: lock header_lock in DBObjectMap::sync (#9891 Samuel Just)
* osd: requeue blocked op before flush it was blocked on (#10512 Sage Weil)
* osd: tolerate missing object between list and attr get on backfill (#10150 Samuel Just)
* osd: use correct atime for eviction decision (Xinze Chi)
* rgw: flush XML header on get ACL request (#10106 Yehuda Sadeh)
* rgw: index swift keys appropriately (#10471 Hemant Bruman, Yehuda Sadeh)
* rgw: send cancel for bucket index pending ops (#10770 Baijiaruo, Yehuda Sadeh)
* rgw: swift: support X_Remove_Container-Meta-{key} (#01475 Dmytro Iurchenko)

For more detailed information, see :download:`the complete changelog <changelog/v0.87.2.txt>`.

v0.87.1 Giant
=============

This is the first (and possibly final) point release for Giant.  Our focus
on stability fixes will be directed towards Hammer and Firefly.

We recommend that all v0.87 Giant users upgrade to this release.

Upgrading
---------

* Due to a change in the Linux kernel version 3.18 and the limits of the FUSE
  interface, ceph-fuse needs be mounted as root on at least some systems. See
  issues #9997, #10277, and #10542 for details.

Notable Changes
---------------

* build: disable stack-execute bit on assembler objects (#10114 Dan Mick)
* build: support boost 1.57.0 (#10688 Ken Dreyer)
* ceph-disk: fix dmcrypt file permissions (#9785 Loic Dachary)
* ceph-disk: run partprobe after zap, behave with partx or partprobe (#9665 #9721 Loic Dachary)
* cephfs-journal-tool: fix import for aged journals (#9977 John Spray)
* cephfs-journal-tool: fix journal import (#10025 John Spray)
* ceph-fuse: use remount to trim kernel dcache (#10277 Yan, Zheng)
* common: add cctid meta variable (#6228 Adam Crume)
* common: fix dump of shard for ghobject_t (#10063 Loic Dachary)
* crush: fix bucket weight underflow (#9998 Pawel Sadowski)
* erasure-code: enforce chunk size alignment (#10211 Loic Dachary)
* erasure-code: regression test suite (#9420 Loic Dachary)
* erasure-code: relax caucy w restrictions (#10325 Loic Dachary)
* libcephfs,ceph-fuse: allow xattr caps on inject_release_failure (#9800 John Spray)
* libcephfs,ceph-fuse: fix cap flush tid comparison (#9869 Greg Farnum)
* libcephfs,ceph-fuse: new flag to indicated sorted dcache (#9178 Yan, Zheng)
* libcephfs,ceph-fuse: prune cache before reconnecting to MDS (Yan, Zheng)
* librados: limit number of in-flight read requests (#9854 Jason Dillaman)
* libradospy: fix thread shutdown (#8797 Dan Mick)
* libradosstriper: fix locking issue in truncate (#10129 Sebastien Ponce)
* librbd: complete pending ops before closing mage (#10299 Jason Dillaman)
* librbd: fix error path on image open failure (#10030 Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 Jason Dillaman)
* librbd: handle errors when creating ioctx while listing children (#10123 Jason Dillaman)
* mds: fix compat version in MClientSession (#9945 John Spray)
* mds: fix journaler write error handling (#10011 John Spray)
* mds: fix locking for file size recovery (#10229 Yan, Zheng)
* mds: handle heartbeat_reset during shutdown (#10382 John Spray)
* mds: store backtrace for straydir (Yan, Zheng)
* mon: allow tiers for FS pools (#10135 John Spray)
* mon: fix caching of last_epoch_clean, osdmap trimming (#9987 Sage Weil)
* mon: fix 'fs ls' on peons (#10288 John Spray)
* mon: fix MDS health status from peons (#10151 John Spray)
* mon: fix paxos off-by-one (#9301 Sage Weil)
* msgr: simple: do not block on takeover while holding global lock (#9921 Greg Farnum)
* osd: deep scrub must not abort if hinfo is missing (#10018 Loic Dachary)
* osd: fix misdirected op detection (#9835 Sage Weil)
* osd: fix past_interval display for acting (#9752 Loic Dachary)
* osd: fix PG peering backoff when behind on osdmaps (#10431 Sage Weil)
* osd: handle no-op write with snapshot case (#10262 Ssage Weil)
* osd: use fast-dispatch (Sage Weil, Greg Farnum)
* rados: fix write to /dev/null (Loic Dachary)
* radosgw-admin: create subuser when needed (#10103 Yehuda Sadeh)
* rbd: avoid invalidating aio_write buffer during image import (#10590 Jason Dillaman)
* rbd: fix export with images > 2GB (Vicente Cheng)
* rgw: change multipart upload id magic (#10271 Georgios Dimitrakakis, Yehuda Sadeh)
* rgw: check keystone auth for S3 POST (#10062 Abhishek Lekshmanan)
* rgw: check timestamp for S3 keystone auth (#10062 Abhishek Lekshmanan)
* rgw: fix partial GET with swift (#10553 Yehuda Sadeh)
* rgw: fix quota disable (#9907 Dong Lei)
* rgw: fix rare corruption of object metadata on put (#9576 Yehuda Sadeh)
* rgw: fix S3 object copy content-type (#9478 Yehuda Sadeh)
* rgw: headers end with \r\n (#9254 Benedikt Fraunhofer, Yehuda Sadeh)
* rgw: remove swift user manifest DLO hash calculation (#9973 Yehuda Sadeh)
* rgw: return correct len when len is 0 (#9877 Yehuda Sadeh)
* rgw: return X-Timestamp field (#8911 Yehuda Sadeh)
* rgw: run radosgw as apache with systemd (#10125)
* rgw: sent ETag on S3 object copy (#9479 Yehuda Sadeh)
* rgw: sent HTTP status reason explicitly in fastcgi (Yehuda Sadeh)
* rgw: set length for keystone token validation (#7796 Mark Kirkwood, Yehuda Sadeh)
* rgw: set ulimit -n on sysvinit before starting daemon (#9587 Sage Weil)
* rgw: update bucket index on set_attrs (#5595 Yehuda Sadeh)
* rgw: update swift subuser permission masks when authenticating (#9918 Yehuda Sadeh)
* rgw: URL decode HTTP query params correction (#10271 Georgios Dimitrakakis, Yehuda Sadeh)
* rgw: use cached attrs while reading object attrs (#10307 Yehuda Sadeh)
* rgw: use strict_strtoll for content length (#10701 Axel Dunkel, Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.87.1.txt>`.



v0.87 Giant
===========

This release will form the basis for the stable release Giant,
v0.87.x.  Highlights for Giant include:

* *RADOS Performance*: a range of improvements have been made in the
  OSD and client-side librados code that improve the throughput on
  flash backends and improve parallelism and scaling on fast machines.
* *CephFS*: we have fixed a raft of bugs in CephFS and built some
  basic journal recovery and diagnostic tools.  Stability and
  performance of single-MDS systems is vastly improved in Giant.
  Although we do not yet recommend CephFS for production deployments,
  we do encourage testing for non-critical workloads so that we can
  better guage the feature, usability, performance, and stability
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
* *Recovery tools*: the ceph_objectstore_tool is greatly expanded to
  allow manipulation of an individual OSDs data store for debugging
  and repair purposes.  This is most heavily used by our QA
  infrastructure to exercise recovery code.

Upgrade Sequencing
------------------

* If your existing cluster is running a version older than v0.80.x
  Firefly, please first upgrade to the latest Firefly release before
  moving on to Giant.  We have not tested upgrades directly from
  Emperor, Dumpling, or older releases.

  We *have* tested:

   * Firefly to Giant
   * Dumpling to Firefly to Giant

* Please upgrade daemons in the following order:

   #. Monitors
   #. OSDs
   #. MDSs and/or radosgw

  Note that the relative ordering of OSDs and monitors should not matter, but
  we primarily tested upgrading monitors first.

Upgrading from v0.80x Firefly
-----------------------------

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

Notable Changes since v0.86
---------------------------

* ceph-disk: use new udev rules for centos7/rhel7 (#9747 Loic Dachary)
* libcephfs-java: fix fstat mode (Noah Watkins)
* librados: fix deadlock when listing PG contents (Guang Yang)
* librados: misc fixes to the new threading model (#9582 #9706 #9845 #9873 Sage Weil)
* mds: fix inotable initialization (Henry C Chang)
* mds: gracefully handle unknown lock type in flock requests (Yan, Zheng)
* mon: add read-only, read-write, and role-definer rols (Joao Eduardo Luis)
* mon: fix mon cap checks (Joao Eduardo Luis)
* mon: misc fixes for new paxos async writes (#9635 Sage Weil)
* mon: set scrub timestamps on PG creation (#9496 Joao Eduardo Luis)
* osd: erasure code: fix buffer alignment (Janne Grunau, Loic Dachary)
* osd: fix alloc hint induced crashes on mixed clusters (#9419 David Zafman)
* osd: fix backfill reservation release on rejection (#9626, Samuel Just)
* osd: fix ioprio option parsing (#9676 #9677 Loic Dachary)
* osd: fix memory leak during snap trimming (#9113 Samuel Just)
* osd: misc peering and recovery fixes (#9614 #9696 #9731 #9718 #9821 #9875 Samuel Just, Guang Yang)

Notable Changes since v0.80.x Firefly
-------------------------------------

* bash completion improvements (Wido den Hollander)
* brag: fixes, improvements (Loic Dachary)
* buffer: improve rebuild_page_aligned (Ma Jianpeng)
* build: fix build on alpha (Michael Cree, Dmitry Smirnov)
* build: fix CentOS 5 (Gerben Meijer)
* build: fix yasm check for x32 (Daniel Schepler, Sage Weil)
* ceph-brag: add tox tests (Alfredo Deza)
* ceph-conf: flush log on exit (Sage Weil)
* ceph.conf: update sample (Sebastien Han)
* ceph-dencoder: refactor build a bit to limit dependencies (Sage Weil, Dan Mick)
* ceph-disk: add Scientific Linux support (Dan van der Ster)
* ceph-disk: do not inadvertantly create directories (Owne Synge)
* ceph-disk: fix dmcrypt support (Sage Weil)
* ceph-disk: fix dmcrypt support (Stephen Taylor)
* ceph-disk: handle corrupt volumes (Stuart Longlang)
* ceph-disk: linter cleanup, logging improvements (Alfredo Deza)
* ceph-disk: partprobe as needed (Eric Eastman)
* ceph-disk: show information about dmcrypt in 'ceph-disk list' output (Sage Weil)
* ceph-disk: use partition type UUIDs and blkid (Sage Weil)
* ceph: fix for non-default cluster names (#8944, Dan Mick)
* ceph-fuse, libcephfs: asok hooks for handling session resets, timeouts (Yan, Zheng)
* ceph-fuse, libcephfs: fix crash in trim_caps (John Spray)
* ceph-fuse, libcephfs: improve cap trimming (John Spray)
* ceph-fuse, libcephfs: improve traceless reply handling (Sage Weil)
* ceph-fuse, libcephfs: virtual xattrs for rstat (Yan, Zheng)
* ceph_objectstore_tool: vastly improved and extended tool for working offline with OSD data stores (David Zafman)
* ceph.spec: many fixes (Erik Logtenberg, Boris Ranto, Dan Mick, Sandon Van Ness)
* ceph.spec: split out ceph-common package, other fixes (Sandon Van Ness)
* ceph_test_librbd_fsx: fix RNG, make deterministic (Ilya Dryomov)
* cephtool: fix help (Yilong Zhao)
* cephtool: refactor and improve CLI tests (Joao Eduardo Luis)
* cephtool: test cleanup (Joao Eduardo Luis)
* clang build fixes (John Spray, Danny Al-Gaaf)
* client: improved MDS session dumps (John Spray)
* common: add config diff admin socket command (Joao Eduardo Luis)
* common: add rwlock assertion checks (Yehuda Sadeh)
* common: fix dup log messages (#9080, Sage Weil)
* common: perfcounters now use atomics and go faster (Sage Weil)
* config: support G, M, K, etc. suffixes (Joao Eduardo Luis)
* coverity cleanups (Danny Al-Gaaf)
* crush: clean up CrushWrapper interface (Xioaxi Chen)
* crush: include new tunables in dump (Sage Weil)
* crush: make ruleset ids unique (Xiaoxi Chen, Loic Dachary)
* crush: only require rule features if the rule is used (#8963, Sage Weil)
* crushtool: send output to stdout, not stderr (Wido den Hollander)
* doc: cache tiering (John Wilkins)
* doc: CRUSH updates (John Wilkins)
* doc: document new upstream wireshark dissector (Kevin Cox)
* doc: improve manual install docs (Francois Lafont)
* doc: keystone integration docs (John Wilkins)
* doc: librados example fixes (Kevin Dalley)
* doc: many doc updates (John Wilkins)
* doc: many install doc updates (John Wilkins)
* doc: misc updates (John Wilkins, Loic Dachary, David Moreau Simard, Wido den Hollander. Volker Voigt, Alfredo Deza, Stephen Jahl, Dan van der Ster)
* doc: osd primary affinity (John Wilkins)
* doc: pool quotas (John Wilkins)
* doc: pre-flight doc improvements (Kevin Dalley)
* doc: switch to an unencumbered font (Ross Turk)
* doc: updated simple configuration guides (John Wilkins)
* doc: update erasure docs (Loic Dachary, Venky Shankar)
* doc: update openstack docs (Josh Durgin)
* filestore: disable use of XFS hint (buggy on old kernels) (Samuel Just)
* filestore: fix xattr spillout (Greg Farnum, Haomai Wang)
* fix hppa arch build (Dmitry Smirnov)
* fix i386 builds (Sage Weil)
* fix struct vs class inconsistencies (Thorsten Behrens)
* global: write pid file even when running in foreground (Alexandre Oliva)
* hadoop: improve tests (Huamin Chen, Greg Farnum, John Spray)
* hadoop: update hadoop tests for Hadoop 2.0 (Haumin Chen)
* init-ceph: continue starting other daemons on crush or mount failure (#8343, Sage Weil)
* journaler: fix locking (Zheng, Yan)
* keyvaluestore: fix hint crash (#8381, Haomai Wang)
* keyvaluestore: header cache (Haomai Wang)
* libcephfs-java: build against older JNI headers (Greg Farnum)
* libcephfs-java: fix gcj-jdk build (Dmitry Smirnov)
* librados: fix crash on read op timeout (#9362 Matthias Kiefer, Sage Weil)
* librados: fix lock leaks in error paths (#9022, Paval Rallabhandi)
* librados: fix pool existence check (#8835, Pavan Rallabhandi)
* librados: fix rados_pool_list bounds checks (Sage Weil)
* librados: fix shutdown race (#9130 Sage Weil)
* librados: fix watch/notify test (#7934 David Zafman)
* librados: fix watch reregistration on acting set change (#9220 Samuel Just)
* librados: give Objecter fine-grained locks (Yehuda Sadeh, Sage Weil, John Spray)
* librados: lttng tracepoitns (Adam Crume)
* librados, osd: return ETIMEDOUT on failed notify (Sage Weil)
* librados: pybind: fix reads when \0 is present (#9547 Mohammad Salehe)
* librados_striper: striping library for librados (Sebastien Ponce)
* librbd, ceph-fuse: reduce cache flush overhead (Haomai Wang)
* librbd: check error code on cache invalidate (Josh Durgin)
* librbd: enable caching by default (Sage Weil)
* librbd: enforce cache size on read requests (Jason Dillaman)
* librbd: fix crash using clone of flattened image (#8845, Josh Durgin)
* librbd: fix error path when opening image (#8912, Josh Durgin)
* librbd: handle blacklisting during shutdown (#9105 John Spray)
* librbd: lttng tracepoints (Adam Crume)
* librbd: new libkrbd library for kernel map/unmap/showmapped (Ilya Dryomov)
* librbd: store and retrieve snapshot metadata based on id (Josh Durgin)
* libs3: update to latest (Danny Al-Gaaf)
* log: fix derr level (Joao Eduardo Luis)
* logrotate: fix osd log rotation on ubuntu (Sage Weil)
* lttng: tracing infrastructure (Noah Watkins, Adam Crume)
* mailmap: many updates (Loic Dachary)
* mailmap: updates (Loic Dachary, Abhishek Lekshmanan, M Ranga Swami Reddy)
* Makefile: fix out of source builds (Stefan Eilemann)
* many many coverity fixes, cleanups (Danny Al-Gaaf)
* mds: adapt to new Objecter locking, give types to all Contexts (John Spray)
* mds: add file system name, enabled flag (John Spray)
* mds: add internal health checks (John Spray)
* mds: add min/max UID for snapshot creation/deletion (#9029, Wido den Hollander)
* mds: avoid tight mon reconnect loop (#9428 Sage Weil)
* mds: boot refactor, cleanup (John Spray)
* mds: cephfs-journal-tool (John Spray)
* mds: fix crash killing sessions (#9173 John Spray)
* mds: fix ctime updates (#9514 Greg Farnum)
* mds: fix journal conversion with standby-replay (John Spray)
* mds: fix replay locking (Yan, Zheng)
* mds: fix standby-replay cache trimming (#8648 Zheng, Yan)
* mds: fix xattr bug triggered by ACLs (Yan, Zheng)
* mds: give perfcounters meaningful names (Sage Weil)
* mds: improve health reporting to monitor (John Spray)
* mds: improve Journaler on-disk format (John Spray)
* mds: improve journal locking (Zheng, Yan)
* mds, libcephfs: use client timestamp for mtime/ctime (Sage Weil)
* mds: make max file recoveries tunable (Sage Weil)
* mds: misc encoding improvements (John Spray)
* mds: misc fixes for multi-mds (Yan, Zheng)
* mds: multi-mds fixes (Yan, Zheng)
* mds: OPTracker integration, dump_ops_in_flight (Greg Farnum)
* mds: prioritize file recovery when appropriate (Sage Weil)
* mds: refactor beacon, improve reliability (John Spray)
* mds: remove legacy anchor table (Yan, Zheng)
* mds: remove legacy discover ino (Yan, Zheng)
* mds: restart on EBLACKLISTED (John Spray)
* mds: separate inode recovery queue (John Spray)
* mds: session ls, evict commands (John Spray)
* mds: submit log events in async thread (Yan, Zheng)
* mds: track RECALL progress, report failure (#9284 John Spray)
* mds: update segment references during journal write (John Spray, Greg Farnum)
* mds: use client-provided timestamp for user-visible file metadata (Yan, Zheng)
* mds: use meaningful names for clients (John Spray)
* mds: validate journal header on load and save (John Spray)
* mds: warn clients which aren't revoking caps (Zheng, Yan, John Spray)
* misc build errors/warnings for Fedora 20 (Boris Ranto)
* misc build fixes for OS X (John Spray)
* misc cleanup (Christophe Courtaut)
* misc integer size cleanups (Kevin Cox)
* misc memory leaks, cleanups, fixes (Danny Al-Gaaf, Sahid Ferdjaoui)
* misc suse fixes (Danny Al-Gaaf)
* misc word size fixes (Kevin Cox)
* mon: add audit log for all admin commands (Joao Eduardo Luis)
* mon: add cluster fingerprint (Sage Weil)
* mon: add get-quota commands (Joao Eduardo Luis)
* mon: add 'osd blocked-by' command to easily see which OSDs are blocking peering progress (Sage Weil)
* mon: add 'osd reweight-by-pg' command (Sage Weil, Guang Yang)
* mon: add perfcounters for paxos operations (Sage Weil)
* mon: avoid creating unnecessary rule on pool create (#9304 Loic Dachary)
* monclient: fix hang (Sage Weil)
* mon: create default EC profile if needed (Loic Dachary)
* mon: do not create file system by default (John Spray)
* mon: do not spam log (Aanchal Agrawal, Sage Weil)
* mon: drop mon- and osd- specific leveldb options (Joao Eduardo Luis)
* mon: ec pool profile fixes (Loic Dachary)
* mon: fix bug when no auth keys are present (#8851, Joao Eduardo Luis)
* mon: fix 'ceph df' output for available space (Xiaoxi Chen)
* mon: fix compat version for MForward (Joao Eduardo Luis)
* mon: fix crash on loopback messages and paxos timeouts (#9062, Sage Weil)
* mon: fix default replication pool ruleset choice (#8373, John Spray)
* mon: fix divide by zero when pg_num is adjusted before OSDs are added (#9101, Sage Weil)
* mon: fix double-free of old MOSDBoot (Sage Weil)
* mon: fix health down messages (Sage Weil)
* mon: fix occasional memory leak after session reset (#9176, Sage Weil)
* mon: fix op write latency perfcounter (#9217 Xinxin Shu)
* mon: fix 'osd perf' reported latency (#9269 Samuel Just)
* mon: fix quorum feature check (#8738, Greg Farnum)
* mon: fix ruleset/ruleid bugs (#9044, Loic Dachary)
* mon: fix set cache_target_full_ratio (#8440, Geoffrey Hartz)
* mon: fix store check on startup (Joao Eduardo Luis)
* mon: include per-pool 'max avail' in df output (Sage Weil)
* mon: make paxos transaction commits asynchronous (Sage Weil)
* mon: make usage dumps in terms of bytes, not kB (Sage Weil)
* mon: 'osd crush reweight-subtree ...' (Sage Weil)
* mon, osd: relax client EC support requirements (Sage Weil)
* mon: preload erasure plugins (#9153 Loic Dachary)
* mon: prevent cache pools from being used directly by CephFS (#9435 John Spray)
* mon: prevent EC pools from being used with cephfs (Joao Eduardo Luis)
* mon: prevent implicit destruction of OSDs with 'osd setmaxosd ...' (#8865, Anand Bhat)
* mon: prevent nonsensical cache-mode transitions (Joao Eduardo Luis)
* mon: restore original weight when auto-marked out OSDs restart (Sage Weil)
* mon: restrict some pool properties to tiered pools (Joao Eduardo Luis)
* mon: some instrumentation (Sage Weil)
* mon: use msg header tid for MMonGetVersionReply (Ilya Dryomov)
* mon: use user-provided ruleset for replicated pool (Xiaoxi Chen)
* mon: verify all quorum members are contiguous at end of Paxos round (#9053, Sage Weil)
* mon: verify available disk space on startup (#9502 Joao Eduardo Luis)
* mon: verify erasure plugin version on load (Loic Dachary)
* msgr: avoid big lock when sending (most) messages (Greg Farnum)
* msgr: fix logged address (Yongyue Sun)
* msgr: misc locking fixes for fast dispatch (#8891, Sage Weil)
* msgr: refactor to cleanly separate SimpleMessenger implemenetation, move toward Connection-based calls (Matt Benjamin, Sage Wei)
* objecter: flag operations that are redirected by caching (Sage Weil)
* objectstore: clean up KeyValueDB interface for key/value backends (Sage Weil)
* osd: account for hit_set_archive bytes (Sage Weil)
* osd: add ability to prehash filestore directories (Guang Yang)
* osd: add 'dump_reservations' admin socket command (Sage Weil)
* osd: add feature bit for erasure plugins (Loic Dachary)
* osd: add header cache for KeyValueStore (Haomai Wang)
* osd: add ISA erasure plugin table cache (Andreas-Joachim Peters)
* osd: add local_mtime for use by cache agent (Zhiqiang Wang)
* osd: add local recovery code (LRC) erasure plugin (Loic Dachary)
* osd: add prototype KineticStore based on Seagate Kinetic (Josh Durgin)
* osd: add READFORWARD caching mode (Luis Pabon)
* osd: add superblock for KeyValueStore backend (Haomai Wang)
* osd: add support for Intel ISA-L erasure code library (Andreas-Joachim Peters)
* osd: allow map cache size to be adjusted at runtime (Sage Weil)
* osd: avoid refcounting overhead by passing a few things by ref (Somnath Roy)
* osd: avoid sharing PG info that is not durable (Samuel Just)
* osd: bound osdmap epoch skew between PGs (Sage Weil)
* osd: cache tier flushing fixes for snapped objects (Samuel Just)
* osd: cap hit_set size (#9339 Samuel Just)
* osd: clean up shard_id_t, shard_t (Loic Dachary)
* osd: clear FDCache on unlink (#8914 Loic Dachary)
* osd: clear slow request latency info on osd up/down (Sage Weil)
* osd: do not evict blocked objects (#9285 Zhiqiang Wang)
* osd: do not skip promote for write-ordered reads (#9064, Samuel Just)
* osd: fix agent early finish looping (David Zafman)
* osd: fix ambigous encoding order for blacklisted clients (#9211, Sage Weil)
* osd: fix bogus assert during OSD shutdown (Sage Weil)
* osd: fix bug with long object names and rename (#8701, Sage Weil)
* osd: fix cache flush corner case for snapshotted objects (#9054, Samuel Just)
* osd: fix cache full -> not full requeueing (#8931, Sage Weil)
* osd: fix clone deletion case (#8334, Sam Just)
* osd: fix clone vs cache_evict bug (#8629 Sage Weil)
* osd: fix connection reconnect race (Greg Farnum)
* osd: fix crash from duplicate backfill reservation (#8863 Sage Weil)
* osd: fix dead peer connection checks (#9295 Greg Farnum, Sage Weil)
* osd: fix discard of old/obsolete subop replies (#9259, Samuel Just)
* osd: fix discard of peer messages from previous intervals (Greg Farnum)
* osd: fix dump of open fds on EMFILE (Sage Weil)
* osd: fix dumps (Joao Eduardo Luis)
* osd: fix erasure-code lib initialization (Loic Dachary)
* osd: fix extent normalization (Adam Crume)
* osd: fix filestore removal corner case (#8332, Sam Just)
* osd: fix flush vs OpContext (Samuel Just)
* osd: fix gating of messages from old OSD instances (Greg Farnum)
* osd: fix hang waiting for osdmap (#8338, Greg Farnum)
* osd: fix interval check corner case during peering (#8104, Sam Just)
* osd: fix ISA erasure alignment (Loic Dachary, Andreas-Joachim Peters)
* osd: fix journal dump (Ma Jianpeng)
* osd: fix journal-less operation (Sage Weil)
* osd: fix keyvaluestore scrub (#8589 Haomai Wang)
* osd: fix keyvaluestore upgrade (Haomai Wang)
* osd: fix loopback msgr issue (Ma Jianpeng)
* osd: fix LSB release parsing (Danny Al-Gaaf)
* osd: fix MarkMeDown and other shutdown races (Sage Weil)
* osd: fix memstore bugs with collection_move_rename, lock ordering (Sage Weil)
* osd: fix min_read_recency_for_promote default on upgrade (Zhiqiang Wang)
* osd: fix mon feature bit requirements bug and resulting log spam (Sage Weil)
* osd: fix mount/remount sync race (#9144 Sage Weil)
* osd: fix PG object listing/ordering bug (Guang Yang)
* osd: fix PG stat errors with tiering (#9082, Sage Weil)
* osd: fix purged_snap initialization on backfill (Sage Weil, Samuel Just, Dan van der Ster, Florian Haas)
* osd: fix race condition on object deletion (#9480 Somnath Roy)
* osd: fix recovery chunk size usage during EC recovery (Ma Jianpeng)
* osd: fix recovery reservation deadlock for EC pools (Samuel Just)
* osd: fix removal of old xattrs when overwriting chained xattrs (Ma Jianpeng)
* osd: fix requesting queueing on PG split (Samuel Just)
* osd: fix scrub vs cache bugs (Samuel Just)
* osd: fix snap object writeback from cache tier (#9054 Samuel Just)
* osd: fix trim of hitsets (Sage Weil)
* osd: force new xattrs into leveldb if fs returns E2BIG (#7779, Sage Weil)
* osd: implement alignment on chunk sizes (Loic Dachary)
* osd: improved backfill priorities (Sage Weil)
* osd: improve journal shutdown (Ma Jianpeng, Mark Kirkwood)
* osd: improve locking for KeyValueStore (Haomai Wang)
* osd: improve locking in OpTracker (Pavan Rallabhandi, Somnath Roy)
* osd: improve prioritization of recovery of degraded over misplaced objects (Sage Weil)
* osd: improve tiering agent arithmetic (Zhiqiang Wang, Sage Weil, Samuel Just)
* osd: include backend information in metadata reported to mon (Sage Weil)
* osd: locking, sharding, caching improvements in FileStore's FDCache (Somnath Roy, Greg Farnum)
* osd: lttng tracepoints for filestore (Noah Watkins)
* osd: make blacklist encoding deterministic (#9211 Sage Weil)
* osd: make tiering behave if hit_sets aren't enabled (Sage Weil)
* osd: many important bug fixes (Samuel Just)
* osd: many many core fixes (Samuel Just)
* osd: many many important fixes (#8231 #8315 #9113 #9179 #9293 #9294 #9326 #9453 #9481 #9482 #9497 #9574 Samuel Just)
* osd: mark pools with incomplete clones (Sage Weil)
* osd: misc erasure code plugin fixes (Loic Dachary)
* osd: misc locking fixes for fast dispatch (Samuel Just, Ma Jianpeng)
* osd, mon: add rocksdb support (Xinxin Shu, Sage Weil)
* osd, mon: config sanity checks on start (Sage Weil, Joao Eduardo Luis)
* osd, mon: distinguish between "misplaced" and "degraded" objects in cluster health and PG state reporting (Sage Weil)
* osd, msgr: fast-dispatch of OSD ops (Greg Farnum, Samuel Just)
* osd, objecter: resend ops on last_force_op_resend barrier; fix cache overlay op ordering (Sage Weil)
* osd: preload erasure plugins (#9153 Loic Dachary)
* osd: prevent old rados clients from using tiered pools (#8714, Sage Weil)
* osd: reduce OpTracker overhead (Somnath Roy)
* osd: refactor some ErasureCode functionality into command parent class (Loic Dachary)
* osd: remove obsolete classic scrub code (David Zafman)
* osd: scrub PGs with invalid stats (Sage Weil)
* osd: set configurable hard limits on object and xattr names (Sage Weil, Haomai Wang)
* osd: set rollback_info_completed on create (#8625, Samuel Just)
* osd: sharded threadpool to improve parallelism (Somnath Roy)
* osd: shard OpTracker to improve performance (Somnath Roy)
* osd: simple io prioritization for scrub (Sage Weil)
* osd: simple scrub throttling (Sage Weil)
* osd: simple snap trimmer throttle (Sage Weil)
* osd: tests for bench command (Loic Dachary)
* osd: trim old EC objects quickly; verify on scrub (Samuel Just)
* osd: use FIEMAP to inform copy_range (Haomai Wang)
* osd: use local time for tiering decisions (Zhiqiang Wang)
* osd: use xfs hint less frequently (Ilya Dryomov)
* osd: verify erasure plugin version on load (Loic Dachary)
* osd: work around GCC 4.8 bug in journal code (Matt Benjamin)
* pybind/rados: fix small timeouts (John Spray)
* qa: xfstests updates (Ilya Dryomov)
* rados: allow setxattr value to be read from stdin (Sage Weil)
* rados bench: fix arg order (Kevin Dalley)
* rados: drop gratuitous \n from getxattr command (Sage Weil)
* rados: fix bench write arithmetic (Jiangheng)
* rados: fix {read,write}_ops values for df output (Sage Weil)
* rbd: add rbdmap pre- and post post- hooks, fix misc bugs (Dmitry Smirnov)
* rbd-fuse: allow exposing single image (Stephen Taylor)
* rbd-fuse: fix unlink (Josh Durgin)
* rbd: improve option default behavior (Josh Durgin)
* rbd: parallelize rbd import, export (Jason Dillaman)
* rbd: rbd-replay utility to replay captured rbd workload traces (Adam Crume)
* rbd: use write-back (not write-through) when caching is enabled (Jason Dillaman)
* removed mkcephfs (deprecated since dumpling)
* rest-api: fix help (Ailing Zhang)
* rgw: add civetweb as default frontent on port 7490 (#9013 Yehuda Sadeh)
* rgw: add --min-rewrite-stripe-size for object restriper (Yehuda Sadeh)
* rgw: add powerdns hook for dynamic DNS for global clusters (Wido den Hollander)
* rgw: add S3 bucket get location operation (Abhishek Lekshmanan)
* rgw: allow : in S3 access key (Roman Haritonov)
* rgw: automatically align writes to EC pool (#8442, Yehuda Sadeh)
* rgw: bucket link uses instance id (Yehuda Sadeh)
* rgw: cache bucket info (Yehuda Sadeh)
* rgw: cache decoded user info (Yehuda Sadeh)
* rgw: check entity permission for put_metadata (#8428, Yehuda Sadeh)
* rgw: copy object data is target bucket is in a different pool (#9039, Yehuda Sadeh)
* rgw: do not try to authenticate CORS preflight requests (#8718, Robert Hubbard, Yehuda Sadeh)
* rgw: fix admin create user op (#8583 Ray Lv)
* rgw: fix civetweb URL decoding (#8621, Yehuda Sadeh)
* rgw: fix crash on swift CORS preflight request (#8586, Yehuda Sadeh)
* rgw: fix log filename suffix (#9353 Alexandre Marangone)
* rgw: fix memory leak following chunk read error (Yehuda Sadeh)
* rgw: fix memory leaks (Andrey Kuznetsov)
* rgw: fix multipart object attr regression (#8452, Yehuda Sadeh)
* rgw: fix multipart upload (#8846, Silvain Munaut, Yehuda Sadeh)
* rgw: fix radosgw-admin 'show log' command (#8553, Yehuda Sadeh)
* rgw: fix removal of objects during object creation (Patrycja Szablowska, Yehuda Sadeh)
* rgw: fix striping for copied objects (#9089, Yehuda Sadeh)
* rgw: fix test for identify whether an object has a tail (#9226, Yehuda Sadeh)
* rgw: fix URL decoding (#8702, Brian Rak)
* rgw: fix URL escaping (Yehuda Sadeh)
* rgw: fix usage (Abhishek Lekshmanan)
* rgw: fix user manifest (Yehuda Sadeh)
* rgw: fix when stripe size is not a multiple of chunk size (#8937, Yehuda Sadeh)
* rgw: handle empty extra pool name (Yehuda Sadeh)
* rgw: improve civetweb logging (Yehuda Sadeh)
* rgw: improve delimited listing of bucket, misc fixes (Yehuda Sadeh)
* rgw: improve -h (Abhishek Lekshmanan)
* rgw: many fixes for civetweb (Yehuda Sadeh)
* rgw: misc civetweb fixes (Yehuda Sadeh)
* rgw: misc civetweb frontend fixes (Yehuda Sadeh)
* rgw: object and bucket rewrite functions to allow restriping old objects (Yehuda Sadeh)
* rgw: powerdns backend for global namespaces (Wido den Hollander)
* rgw: prevent multiobject PUT race (Yehuda Sadeh)
* rgw: send user manifest header (Yehuda Sadeh)
* rgw: subuser creation fixes (#8587 Yehuda Sadeh)
* rgw: use systemd-run from sysvinit script (JuanJose Galvez)
* rpm: do not restart daemons on upgrade (Alfredo Deza)
* rpm: misc packaging fixes for rhel7 (Sandon Van Ness)
* rpm: split ceph-common from ceph (Sandon Van Ness)
* systemd: initial systemd config files (Federico Simoncelli)
* systemd: wrap started daemons in new systemd environment (Sage Weil, Dan Mick)
* sysvinit: add support for non-default cluster names (Alfredo Deza)
* sysvinit: less sensitive to failures (Sage Weil)
* test_librbd_fsx: test krbd as well as librbd (Ilya Dryomov)
* unit test improvements (Loic Dachary)
* upstart: increase max open files limit (Sage Weil)
* vstart.sh: fix/improve rgw support (Luis Pabon, Abhishek Lekshmanan)




v0.86
=====

This is a release candidate for Giant, which will hopefully be out
in another week or two.  We did a feature freeze about a month ago
and since then have been doing only stabilization and bug fixing (and
a handful on low-risk enhancements).  A fair bit of new functionality
went into the final sprint, but it's baked for quite a while now and
we're feeling pretty good about it.

Major items include:

* librados locking refactor to improve scaling and client performance
* local recovery code (LRC) erasure code plugin to trade some
  additional storage overhead for improved recovery performance
* LTTNG tracing framework, with initial tracepoints in librados,
  librbd, and the OSD FileStore backend
* separate monitor audit log for all administrative commands
* asynchronos monitor transaction commits to reduce the impact on
  monitor read requests while processing updates
* low-level tool for working with individual OSD data stores for
  debugging, recovery, and testing
* many MDS improvements (bug fixes, health reporting)

There are still a handful of known bugs in this release, but nothing
severe enough to prevent a release.  By and large we are pretty
pleased with the stability and expect the final Giant release to be
quite reliable.

Please try this out on your non-production clusters for a preview

Notable Changes
---------------

* buffer: improve rebuild_page_aligned (Ma Jianpeng)
* build: fix CentOS 5 (Gerben Meijer)
* build: fix build on alpha (Michael Cree, Dmitry Smirnov)
* build: fix yasm check for x32 (Daniel Schepler, Sage Weil)
* ceph-disk: add Scientific Linux support (Dan van der Ster)
* ceph-fuse, libcephfs: fix crash in trim_caps (John Spray)
* ceph-fuse, libcephfs: improve cap trimming (John Spray)
* ceph-fuse, libcephfs: virtual xattrs for rstat (Yan, Zheng)
* ceph.conf: update sample (Sebastien Han)
* ceph.spec: many fixes (Erik Logtenberg, Boris Ranto, Dan Mick, Sandon Van Ness)
* ceph_objectstore_tool: vastly improved and extended tool for working offline with OSD data stores (David Zafman)
* common: add config diff admin socket command (Joao Eduardo Luis)
* common: add rwlock assertion checks (Yehuda Sadeh)
* crush: clean up CrushWrapper interface (Xioaxi Chen)
* crush: make ruleset ids unique (Xiaoxi Chen, Loic Dachary)
* doc: improve manual install docs (Francois Lafont)
* doc: misc updates (John Wilkins, Loic Dachary, David Moreau Simard, Wido den Hollander. Volker Voigt, Alfredo Deza, Stephen Jahl, Dan van der Ster)
* global: write pid file even when running in foreground (Alexandre Oliva)
* hadoop: improve tests (Huamin Chen, Greg Farnum, John Spray)
* journaler: fix locking (Zheng, Yan)
* librados, osd: return ETIMEDOUT on failed notify (Sage Weil)
* librados: fix crash on read op timeout (#9362 Matthias Kiefer, Sage Weil)
* librados: fix shutdown race (#9130 Sage Weil)
* librados: fix watch reregistration on acting set change (#9220 Samuel Just)
* librados: fix watch/notify test (#7934 David Zafman)
* librados: give Objecter fine-grained locks (Yehuda Sadeh, Sage Weil, John Spray)
* librados: lttng tracepoitns (Adam Crume)
* librados: pybind: fix reads when \0 is present (#9547 Mohammad Salehe)
* librbd: enforce cache size on read requests (Jason Dillaman)
* librbd: handle blacklisting during shutdown (#9105 John Spray)
* librbd: lttng tracepoints (Adam Crume)
* lttng: tracing infrastructure (Noah Watkins, Adam Crume)
* mailmap: updates (Loic Dachary, Abhishek Lekshmanan, M Ranga Swami Reddy)
* many many coverity fixes, cleanups (Danny Al-Gaaf)
* mds: adapt to new Objecter locking, give types to all Contexts (John Spray)
* mds: add internal health checks (John Spray)
* mds: avoid tight mon reconnect loop (#9428 Sage Weil)
* mds: fix crash killing sessions (#9173 John Spray)
* mds: fix ctime updates (#9514 Greg Farnum)
* mds: fix replay locking (Yan, Zheng)
* mds: fix standby-replay cache trimming (#8648 Zheng, Yan)
* mds: give perfcounters meaningful names (Sage Weil)
* mds: improve health reporting to monitor (John Spray)
* mds: improve journal locking (Zheng, Yan)
* mds: make max file recoveries tunable (Sage Weil)
* mds: prioritize file recovery when appropriate (Sage Weil)
* mds: refactor beacon, improve reliability (John Spray)
* mds: restart on EBLACKLISTED (John Spray)
* mds: track RECALL progress, report failure (#9284 John Spray)
* mds: update segment references during journal write (John Spray, Greg Farnum)
* mds: use meaningful names for clients (John Spray)
* mds: warn clients which aren't revoking caps (Zheng, Yan, John Spray)
* mon: add 'osd reweight-by-pg' command (Sage Weil, Guang Yang)
* mon: add audit log for all admin commands (Joao Eduardo Luis)
* mon: add cluster fingerprint (Sage Weil)
* mon: avoid creating unnecessary rule on pool create (#9304 Loic Dachary)
* mon: do not spam log (Aanchal Agrawal, Sage Weil)
* mon: fix 'osd perf' reported latency (#9269 Samuel Just)
* mon: fix double-free of old MOSDBoot (Sage Weil)
* mon: fix op write latency perfcounter (#9217 Xinxin Shu)
* mon: fix store check on startup (Joao Eduardo Luis)
* mon: make paxos transaction commits asynchronous (Sage Weil)
* mon: preload erasure plugins (#9153 Loic Dachary)
* mon: prevent cache pools from being used directly by CephFS (#9435 John Spray)
* mon: use user-provided ruleset for replicated pool (Xiaoxi Chen)
* mon: verify available disk space on startup (#9502 Joao Eduardo Luis)
* mon: verify erasure plugin version on load (Loic Dachary)
* msgr: fix logged address (Yongyue Sun)
* osd: account for hit_set_archive bytes (Sage Weil)
* osd: add ISA erasure plugin table cache (Andreas-Joachim Peters)
* osd: add ability to prehash filestore directories (Guang Yang)
* osd: add feature bit for erasure plugins (Loic Dachary)
* osd: add local recovery code (LRC) erasure plugin (Loic Dachary)
* osd: cap hit_set size (#9339 Samuel Just)
* osd: clear FDCache on unlink (#8914 Loic Dachary)
* osd: do not evict blocked objects (#9285 Zhiqiang Wang)
* osd: fix ISA erasure alignment (Loic Dachary, Andreas-Joachim Peters)
* osd: fix clone vs cache_evict bug (#8629 Sage Weil)
* osd: fix crash from duplicate backfill reservation (#8863 Sage Weil)
* osd: fix dead peer connection checks (#9295 Greg Farnum, Sage Weil)
* osd: fix keyvaluestore scrub (#8589 Haomai Wang)
* osd: fix keyvaluestore upgrade (Haomai Wang)
* osd: fix min_read_recency_for_promote default on upgrade (Zhiqiang Wang)
* osd: fix mount/remount sync race (#9144 Sage Weil)
* osd: fix purged_snap initialization on backfill (Sage Weil, Samuel Just, Dan van der Ster, Florian Haas)
* osd: fix race condition on object deletion (#9480 Somnath Roy)
* osd: fix snap object writeback from cache tier (#9054 Samuel Just)
* osd: improve journal shutdown (Ma Jianpeng, Mark Kirkwood)
* osd: improve locking in OpTracker (Pavan Rallabhandi, Somnath Roy)
* osd: improve tiering agent arithmetic (Zhiqiang Wang, Sage Weil, Samuel Just)
* osd: lttng tracepoints for filestore (Noah Watkins)
* osd: make blacklist encoding deterministic (#9211 Sage Weil)
* osd: many many important fixes (#8231 #8315 #9113 #9179 #9293 #9294 #9326 #9453 #9481 #9482 #9497 #9574 Samuel Just)
* osd: misc erasure code plugin fixes (Loic Dachary)
* osd: preload erasure plugins (#9153 Loic Dachary)
* osd: shard OpTracker to improve performance (Somnath Roy)
* osd: use local time for tiering decisions (Zhiqiang Wang)
* osd: verify erasure plugin version on load (Loic Dachary)
* rados: fix bench write arithmetic (Jiangheng)
* rbd: parallelize rbd import, export (Jason Dillaman)
* rbd: rbd-replay utility to replay captured rbd workload traces (Adam Crume)
* rbd: use write-back (not write-through) when caching is enabled (Jason Dillaman)
* rgw: add S3 bucket get location operation (Abhishek Lekshmanan)
* rgw: add civetweb as default frontent on port 7490 (#9013 Yehuda Sadeh)
* rgw: allow : in S3 access key (Roman Haritonov)
* rgw: fix admin create user op (#8583 Ray Lv)
* rgw: fix log filename suffix (#9353 Alexandre Marangone)
* rgw: fix usage (Abhishek Lekshmanan)
* rgw: many fixes for civetweb (Yehuda Sadeh)
* rgw: subuser creation fixes (#8587 Yehuda Sadeh)
* rgw: use systemd-run from sysvinit script (JuanJose Galvez)
* unit test improvements (Loic Dachary)
* vstart.sh: fix/improve rgw support (Luis Pabon, Abhishek Lekshmanan)

v0.85
=====

This is the second-to-last development release before Giant that
contains new functionality.  The big items to land during this cycle
are the messenger refactoring from Matt Benjmain that lays some
groundwork for RDMA support, a performance improvement series from
SanDisk that improves performance on SSDs, lots of improvements to our
new standalone civetweb-based RGW frontend, and a new 'osd blocked-by'
mon command that allows admins to easily identify which OSDs are
blocking peering progress.  The other big change is that the OSDs and
Monitors now distinguish between "misplaced" and "degraded" objects:
the latter means there are fewer copies than we'd like, while the
former simply means the are not stored in the locations where we want
them to be.

Also of note is a change to librbd that enables client-side caching by
default.  This is coupled with another option that makes the cache
write-through until a "flush" operations is observed: this implies
that the librbd user (usually a VM guest OS) supports barriers and
flush and that it is safe for the cache to switch into writeback mode
without compromising data safety or integrity.  It has long been
recommended practice that these options be enabled (e.g., in OpenStack
environments) but until now it has not been the default.

We have frozen the tree for the looming Giant release, and the next
development release will be a release candidate with a final batch of
new functionality.

Upgrading
---------

* The client-side caching for librbd is now enabled by default (rbd
  cache = true).  A safety option (rbd cache writethrough until flush
  = true) is also enabled so that writeback caching is not used until
  the library observes a 'flush' command, indicating that the librbd
  users is passing that operation through from the guest VM.  This
  avoids potential data loss when used with older versions of qemu
  that do not support flush.

    leveldb_write_buffer_size = 32*1024*1024  = 33554432  // 32MB
    leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
    leveldb_block_size        = 64*1024       = 65536     // 64KB
    leveldb_compression       = false
    leveldb_log               = ""

  OSDs will still maintain the following osd-specific defaults:

    leveldb_log               = ""

* The 'rados getxattr ...' command used to add a gratuitous newline to the attr
  value; it now does not.

Notable Changes
---------------

* ceph-disk: do not inadvertantly create directories (Owne Synge)
* ceph-disk: fix dmcrypt support (Sage Weil)
* ceph-disk: linter cleanup, logging improvements (Alfredo Deza)
* ceph-disk: show information about dmcrypt in 'ceph-disk list' output (Sage Weil)
* ceph-disk: use partition type UUIDs and blkid (Sage Weil)
* ceph: fix for non-default cluster names (#8944, Dan Mick)
* doc: document new upstream wireshark dissector (Kevin Cox)
* doc: many install doc updates (John Wilkins)
* librados: fix lock leaks in error paths (#9022, Paval Rallabhandi)
* librados: fix pool existence check (#8835, Pavan Rallabhandi)
* librbd: enable caching by default (Sage Weil)
* librbd: fix crash using clone of flattened image (#8845, Josh Durgin)
* librbd: store and retrieve snapshot metadata based on id (Josh Durgin)
* mailmap: many updates (Loic Dachary)
* mds: add min/max UID for snapshot creation/deletion (#9029, Wido den Hollander)
* misc build errors/warnings for Fedora 20 (Boris Ranto)
* mon: add 'osd blocked-by' command to easily see which OSDs are blocking peering progress (Sage Weil)
* mon: add perfcounters for paxos operations (Sage Weil)
* mon: create default EC profile if needed (Loic Dachary)
* mon: fix crash on loopback messages and paxos timeouts (#9062, Sage Weil)
* mon: fix divide by zero when pg_num is adjusted before OSDs are added (#9101, Sage Weil)
* mon: fix occasional memory leak after session reset (#9176, Sage Weil)
* mon: fix ruleset/ruleid bugs (#9044, Loic Dachary)
* mon: make usage dumps in terms of bytes, not kB (Sage Weil)
* mon: prevent implicit destruction of OSDs with 'osd setmaxosd ...' (#8865, Anand Bhat)
* mon: verify all quorum members are contiguous at end of Paxos round (#9053, Sage Weil)
* msgr: refactor to cleanly separate SimpleMessenger implemenetation, move toward Connection-based calls (Matt Benjamin, Sage Wei)
* objectstore: clean up KeyValueDB interface for key/value backends (Sage Weil)
* osd: add local_mtime for use by cache agent (Zhiqiang Wang)
* osd: add superblock for KeyValueStore backend (Haomai Wang)
* osd: add support for Intel ISA-L erasure code library (Andreas-Joachim Peters)
* osd: do not skip promote for write-ordered reads (#9064, Samuel Just)
* osd: fix ambigous encoding order for blacklisted clients (#9211, Sage Weil)
* osd: fix cache flush corner case for snapshotted objects (#9054, Samuel Just)
* osd: fix discard of old/obsolete subop replies (#9259, Samuel Just)
* osd: fix discard of peer messages from previous intervals (Greg Farnum)
* osd: fix dump of open fds on EMFILE (Sage Weil)
* osd: fix journal dump (Ma Jianpeng)
* osd: fix mon feature bit requirements bug and resulting log spam (Sage Weil)
* osd: fix recovery chunk size usage during EC recovery (Ma Jianpeng)
* osd: fix recovery reservation deadlock for EC pools (Samuel Just)
* osd: fix removal of old xattrs when overwriting chained xattrs (Ma Jianpeng)
* osd: fix requesting queueing on PG split (Samuel Just)
* osd: force new xattrs into leveldb if fs returns E2BIG (#7779, Sage Weil)
* osd: implement alignment on chunk sizes (Loic Dachary)
* osd: improve prioritization of recovery of degraded over misplaced objects (Sage Weil)
* osd: locking, sharding, caching improvements in FileStore's FDCache (Somnath Roy, Greg Farnum)
* osd: many important bug fixes (Samuel Just)
* osd, mon: add rocksdb support (Xinxin Shu, Sage Weil)
* osd, mon: distinguish between "misplaced" and "degraded" objects in cluster health and PG state reporting (Sage Weil)
* osd: refactor some ErasureCode functionality into command parent class (Loic Dachary)
* osd: set rollback_info_completed on create (#8625, Samuel Just)
* rados: allow setxattr value to be read from stdin (Sage Weil)
* rados: drop gratuitous \n from getxattr command (Sage Weil)
* rgw: add --min-rewrite-stripe-size for object restriper (Yehuda Sadeh)
* rgw: add powerdns hook for dynamic DNS for global clusters (Wido den Hollander)
* rgw: copy object data is target bucket is in a different pool (#9039, Yehuda Sadeh)
* rgw: do not try to authenticate CORS preflight requests (#8718, Robert Hubbard, Yehuda Sadeh)
* rgw: fix civetweb URL decoding (#8621, Yehuda Sadeh)
* rgw: fix removal of objects during object creation (Patrycja Szablowska, Yehuda Sadeh)
* rgw: fix striping for copied objects (#9089, Yehuda Sadeh)
* rgw: fix test for identify whether an object has a tail (#9226, Yehuda Sadeh)
* rgw: fix when stripe size is not a multiple of chunk size (#8937, Yehuda Sadeh)
* rgw: improve civetweb logging (Yehuda Sadeh)
* rgw: misc civetweb frontend fixes (Yehuda Sadeh)
* sysvinit: add support for non-default cluster names (Alfredo Deza)


v0.84
=====

The next Ceph development release is here!  This release contains
several meaty items, including some MDS improvements for journaling,
the ability to remove the CephFS file system (and name it), several
mon cleanups with tiered pools, several OSD performance branches, a
new "read forward" RADOS caching mode, a prototype Kinetic OSD
backend, and various radosgw improvements (especially with the new
standalone civetweb frontend).  And there are a zillion OSD bug
fixes. Things are looking pretty good for the Giant release that is
coming up in the next month.

Upgrading
---------

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

Notable Changes
---------------

* ceph-conf: flush log on exit (Sage Weil)
* ceph-dencoder: refactor build a bit to limit dependencies (Sage Weil, Dan Mick)
* ceph.spec: split out ceph-common package, other fixes (Sandon Van Ness)
* ceph_test_librbd_fsx: fix RNG, make deterministic (Ilya Dryomov)
* cephtool: refactor and improve CLI tests (Joao Eduardo Luis)
* client: improved MDS session dumps (John Spray)
* common: fix dup log messages (#9080, Sage Weil)
* crush: include new tunables in dump (Sage Weil)
* crush: only require rule features if the rule is used (#8963, Sage Weil)
* crushtool: send output to stdout, not stderr (Wido den Hollander)
* fix i386 builds (Sage Weil)
* fix struct vs class inconsistencies (Thorsten Behrens)
* hadoop: update hadoop tests for Hadoop 2.0 (Haumin Chen)
* librbd, ceph-fuse: reduce cache flush overhead (Haomai Wang)
* librbd: fix error path when opening image (#8912, Josh Durgin)
* mds: add file system name, enabled flag (John Spray)
* mds: boot refactor, cleanup (John Spray)
* mds: fix journal conversion with standby-replay (John Spray)
* mds: separate inode recovery queue (John Spray)
* mds: session ls, evict commands (John Spray)
* mds: submit log events in async thread (Yan, Zheng)
* mds: use client-provided timestamp for user-visible file metadata (Yan, Zheng)
* mds: validate journal header on load and save (John Spray)
* misc build fixes for OS X (John Spray)
* misc integer size cleanups (Kevin Cox)
* mon: add get-quota commands (Joao Eduardo Luis)
* mon: do not create file system by default (John Spray)
* mon: fix 'ceph df' output for available space (Xiaoxi Chen)
* mon: fix bug when no auth keys are present (#8851, Joao Eduardo Luis)
* mon: fix compat version for MForward (Joao Eduardo Luis)
* mon: restrict some pool properties to tiered pools (Joao Eduardo Luis)
* msgr: misc locking fixes for fast dispatch (#8891, Sage Weil)
* osd: add 'dump_reservations' admin socket command (Sage Weil)
* osd: add READFORWARD caching mode (Luis Pabon)
* osd: add header cache for KeyValueStore (Haomai Wang)
* osd: add prototype KineticStore based on Seagate Kinetic (Josh Durgin)
* osd: allow map cache size to be adjusted at runtime (Sage Weil)
* osd: avoid refcounting overhead by passing a few things by ref (Somnath Roy)
* osd: avoid sharing PG info that is not durable (Samuel Just)
* osd: clear slow request latency info on osd up/down (Sage Weil)
* osd: fix PG object listing/ordering bug (Guang Yang)
* osd: fix PG stat errors with tiering (#9082, Sage Weil)
* osd: fix bug with long object names and rename (#8701, Sage Weil)
* osd: fix cache full -> not full requeueing (#8931, Sage Weil)
* osd: fix gating of messages from old OSD instances (Greg Farnum)
* osd: fix memstore bugs with collection_move_rename, lock ordering (Sage Weil)
* osd: improve locking for KeyValueStore (Haomai Wang)
* osd: make tiering behave if hit_sets aren't enabled (Sage Weil)
* osd: mark pools with incomplete clones (Sage Weil)
* osd: misc locking fixes for fast dispatch (Samuel Just, Ma Jianpeng)
* osd: prevent old rados clients from using tiered pools (#8714, Sage Weil)
* osd: reduce OpTracker overhead (Somnath Roy)
* osd: set configurable hard limits on object and xattr names (Sage Weil, Haomai Wang)
* osd: trim old EC objects quickly; verify on scrub (Samuel Just)
* osd: work around GCC 4.8 bug in journal code (Matt Benjamin)
* rados bench: fix arg order (Kevin Dalley)
* rados: fix {read,write}_ops values for df output (Sage Weil)
* rbd: add rbdmap pre- and post post- hooks, fix misc bugs (Dmitry Smirnov)
* rbd: improve option default behavior (Josh Durgin)
* rgw: automatically align writes to EC pool (#8442, Yehuda Sadeh)
* rgw: fix crash on swift CORS preflight request (#8586, Yehuda Sadeh)
* rgw: fix memory leaks (Andrey Kuznetsov)
* rgw: fix multipart upload (#8846, Silvain Munaut, Yehuda Sadeh)
* rgw: improve -h (Abhishek Lekshmanan)
* rgw: improve delimited listing of bucket, misc fixes (Yehuda Sadeh)
* rgw: misc civetweb fixes (Yehuda Sadeh)
* rgw: powerdns backend for global namespaces (Wido den Hollander)
* systemd: initial systemd config files (Federico Simoncelli)


v0.83
=====

Another Ceph development release!  This has been a longer cycle, so
there has been quite a bit of bug fixing and stabilization in this
round.  There is also a bunch of packaging fixes for RPM distros
(RHEL/CentOS, Fedora, and SUSE) and for systemd.  We've also added a new
librados-striper library from Sebastien Ponce that provides a generic
striping API for applications to code to.

Upgrading
---------

* The experimental keyvaluestore-dev OSD backend had an on-disk format
  change that prevents existing OSD data from being upgraded.  This
  affects developers and testers only.

* mon-specific and osd-specific leveldb options have been removed.
  From this point onward users should use the `leveldb_*` generic
  options and add the options in the appropriate sections of their
  configuration files.  Monitors will still maintain the following
  monitor-specific defaults:

    leveldb_write_buffer_size = 32*1024*1024  = 33554432  // 32MB
    leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
    leveldb_block_size        = 64*1024       = 65536     // 64KB
    leveldb_compression       = false
    leveldb_log               = ""

  OSDs will still maintain the following osd-specific defaults:

    leveldb_log               = ""

Notable Changes
---------------

* ceph-disk: fix dmcrypt support (Stephen Taylor)
* cephtool: fix help (Yilong Zhao)
* cephtool: test cleanup (Joao Eduardo Luis)
* doc: librados example fixes (Kevin Dalley)
* doc: many doc updates (John Wilkins)
* doc: update erasure docs (Loic Dachary, Venky Shankar)
* filestore: disable use of XFS hint (buggy on old kernels) (Samuel Just)
* filestore: fix xattr spillout (Greg Farnum, Haomai Wang)
* keyvaluestore: header cache (Haomai Wang)
* librados_striper: striping library for librados (Sebastien Ponce)
* libs3: update to latest (Danny Al-Gaaf)
* log: fix derr level (Joao Eduardo Luis)
* logrotate: fix osd log rotation on ubuntu (Sage Weil)
* mds: fix xattr bug triggered by ACLs (Yan, Zheng)
* misc memory leaks, cleanups, fixes (Danny Al-Gaaf, Sahid Ferdjaoui)
* misc suse fixes (Danny Al-Gaaf)
* misc word size fixes (Kevin Cox)
* mon: drop mon- and osd- specific leveldb options (Joao Eduardo Luis)
* mon: ec pool profile fixes (Loic Dachary)
* mon: fix health down messages (Sage Weil)
* mon: fix quorum feature check (#8738, Greg Farnum)
* mon: 'osd crush reweight-subtree ...' (Sage Weil)
* mon, osd: relax client EC support requirements (Sage Weil)
* mon: some instrumentation (Sage Weil)
* objecter: flag operations that are redirected by caching (Sage Weil)
* osd: clean up shard_id_t, shard_t (Loic Dachary)
* osd: fix connection reconnect race (Greg Farnum)
* osd: fix dumps (Joao Eduardo Luis)
* osd: fix erasure-code lib initialization (Loic Dachary)
* osd: fix extent normalization (Adam Crume)
* osd: fix loopback msgr issue (Ma Jianpeng)
* osd: fix LSB release parsing (Danny Al-Gaaf)
* osd: improved backfill priorities (Sage Weil)
* osd: many many core fixes (Samuel Just)
* osd, mon: config sanity checks on start (Sage Weil, Joao Eduardo Luis)
* osd: sharded threadpool to improve parallelism (Somnath Roy)
* osd: simple io prioritization for scrub (Sage Weil)
* osd: simple scrub throttling (Sage Weil)
* osd: tests for bench command (Loic Dachary)
* osd: use xfs hint less frequently (Ilya Dryomov)
* pybind/rados: fix small timeouts (John Spray)
* qa: xfstests updates (Ilya Dryomov)
* rgw: cache bucket info (Yehuda Sadeh)
* rgw: cache decoded user info (Yehuda Sadeh)
* rgw: fix multipart object attr regression (#8452, Yehuda Sadeh)
* rgw: fix radosgw-admin 'show log' command (#8553, Yehuda Sadeh)
* rgw: fix URL decoding (#8702, Brian Rak)
* rgw: handle empty extra pool name (Yehuda Sadeh)
* rpm: do not restart daemons on upgrade (Alfredo Deza)
* rpm: misc packaging fixes for rhel7 (Sandon Van Ness)
* rpm: split ceph-common from ceph (Sandon Van Ness)
* systemd: wrap started daemons in new systemd environment (Sage Weil, Dan Mick)
* sysvinit: less sensitive to failures (Sage Weil)
* upstart: increase max open files limit (Sage Weil)

v0.82
=====

This is the second post-firefly development release.  It includes a range
of bug fixes and some usability improvements.  There are some MDS debugging
and diagnostic tools, an improved 'ceph df', and some OSD backend refactoring
and cleanup.

Notable Changes
---------------

* ceph-brag: add tox tests (Alfredo Deza)
* common: perfcounters now use atomics and go faster (Sage Weil)
* doc: CRUSH updates (John Wilkins)
* doc: osd primary affinity (John Wilkins)
* doc: pool quotas (John Wilkins)
* doc: pre-flight doc improvements (Kevin Dalley)
* doc: switch to an unencumbered font (Ross Turk)
* doc: update openstack docs (Josh Durgin)
* fix hppa arch build (Dmitry Smirnov)
* init-ceph: continue starting other daemons on crush or mount failure (#8343, Sage Weil)
* keyvaluestore: fix hint crash (#8381, Haomai Wang)
* libcephfs-java: build against older JNI headers (Greg Farnum)
* librados: fix rados_pool_list bounds checks (Sage Weil)
* mds: cephfs-journal-tool (John Spray)
* mds: improve Journaler on-disk format (John Spray)
* mds, libcephfs: use client timestamp for mtime/ctime (Sage Weil)
* mds: misc encoding improvements (John Spray)
* mds: misc fixes for multi-mds (Yan, Zheng)
* mds: OPTracker integration, dump_ops_in_flight (Greg Farnum)
* misc cleanup (Christophe Courtaut)
* mon: fix default replication pool ruleset choice (#8373, John Spray)
* mon: fix set cache_target_full_ratio (#8440, Geoffrey Hartz)
* mon: include per-pool 'max avail' in df output (Sage Weil)
* mon: prevent EC pools from being used with cephfs (Joao Eduardo Luis)
* mon: restore original weight when auto-marked out OSDs restart (Sage Weil)
* mon: use msg header tid for MMonGetVersionReply (Ilya Dryomov)
* osd: fix bogus assert during OSD shutdown (Sage Weil)
* osd: fix clone deletion case (#8334, Sam Just)
* osd: fix filestore removal corner case (#8332, Sam Just)
* osd: fix hang waiting for osdmap (#8338, Greg Farnum)
* osd: fix interval check corner case during peering (#8104, Sam Just)
* osd: fix journal-less operation (Sage Weil)
* osd: include backend information in metadata reported to mon (Sage Weil)
* rest-api: fix help (Ailing Zhang)
* rgw: check entity permission for put_metadata (#8428, Yehuda Sadeh)


v0.81
=====

This is the first development release since Firefly.  It includes a
lot of work that we delayed merging while stabilizing things.  Lots of
new functionality, as well as several fixes that are baking a bit before
getting backported.

Upgrading
---------

* CephFS support for the legacy anchor table has finally been removed.
  Users with file systems created before firefly should ensure that inodes
  with multiple hard links are modified *prior* to the upgrade to ensure that
  the backtraces are written properly.  For example::

    sudo find /mnt/cephfs -type f -links +1 -exec touch \{\} \;

* Disallow nonsensical 'tier cache-mode' transitions.  From this point
  onward, 'writeback' can only transition to 'forward' and 'forward'
  can transition to 1) 'writeback' if there are dirty objects, or 2) any if
  there are no dirty objects.

Notable Changes
---------------

* bash completion improvements (Wido den Hollander)
* brag: fixes, improvements (Loic Dachary)
* ceph-disk: handle corrupt volumes (Stuart Longlang)
* ceph-disk: partprobe as needed (Eric Eastman)
* ceph-fuse, libcephfs: asok hooks for handling session resets, timeouts (Yan, Zheng)
* ceph-fuse, libcephfs: improve traceless reply handling (Sage Weil)
* clang build fixes (John Spray, Danny Al-Gaaf)
* config: support G, M, K, etc. suffixes (Joao Eduardo Luis)
* coverity cleanups (Danny Al-Gaaf)
* doc: cache tiering (John Wilkins)
* doc: keystone integration docs (John Wilkins)
* doc: updated simple configuration guides (John Wilkins)
* libcephfs-java: fix gcj-jdk build (Dmitry Smirnov)
* librbd: check error code on cache invalidate (Josh Durgin)
* librbd: new libkrbd library for kernel map/unmap/showmapped (Ilya Dryomov)
* Makefile: fix out of source builds (Stefan Eilemann)
* mds: multi-mds fixes (Yan, Zheng)
* mds: remove legacy anchor table (Yan, Zheng)
* mds: remove legacy discover ino (Yan, Zheng)
* monclient: fix hang (Sage Weil)
* mon: prevent nonsensical cache-mode transitions (Joao Eduardo Luis)
* msgr: avoid big lock when sending (most) messages (Greg Farnum)
* osd: bound osdmap epoch skew between PGs (Sage Weil)
* osd: cache tier flushing fixes for snapped objects (Samuel Just)
* osd: fix agent early finish looping (David Zafman)
* osd: fix flush vs OpContext (Samuel Just)
* osd: fix MarkMeDown and other shutdown races (Sage Weil)
* osd: fix scrub vs cache bugs (Samuel Just)
* osd: fix trim of hitsets (Sage Weil)
* osd, msgr: fast-dispatch of OSD ops (Greg Farnum, Samuel Just)
* osd, objecter: resend ops on last_force_op_resend barrier; fix cache overlay op ordering (Sage Weil)
* osd: remove obsolete classic scrub code (David Zafman)
* osd: scrub PGs with invalid stats (Sage Weil)
* osd: simple snap trimmer throttle (Sage Weil)
* osd: use FIEMAP to inform copy_range (Haomai Wang)
* rbd-fuse: allow exposing single image (Stephen Taylor)
* rbd-fuse: fix unlink (Josh Durgin)
* removed mkcephfs (deprecated since dumpling)
* rgw: bucket link uses instance id (Yehuda Sadeh)
* rgw: fix memory leak following chunk read error (Yehuda Sadeh)
* rgw: fix URL escaping (Yehuda Sadeh)
* rgw: fix user manifest (Yehuda Sadeh)
* rgw: object and bucket rewrite functions to allow restriping old objects (Yehuda Sadeh)
* rgw: prevent multiobject PUT race (Yehuda Sadeh)
* rgw: send user manifest header (Yehuda Sadeh)
* test_librbd_fsx: test krbd as well as librbd (Ilya Dryomov)

v0.80.11 Firefly
================

This is a bugfix release for Firefly.  This Firefly 0.80.x is nearing
its planned end of life in January 2016 it may also be the last.

We recommend that all Firefly users upgrade.

For more detailed information, see :download:`the complete changelog
<changelog/v0.80.11.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.10.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.9.txt>`.

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
imporant (but relatively rare) OSD peering fixes, performance issues
when snapshots are trimmed, several RGW fixes, a paxos corner case
fix, and some packaging updates.

We recommend that all users for v0.80.x firefly upgrade when it is
convenient to do so.

For more detailed information, see :download:`the complete changelog <changelog/v0.80.8.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.7.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.6.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.5.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.4.txt>`.


v0.80.3 Firefly
===============

This is the third Firefly point release.  It includes a single fix
for a radosgw regression that was discovered in v0.80.2 right after it
was released.

We recommand that all v0.80.x Firefly users upgrade.

Notable Changes
---------------

* radosgw: fix regression in manifest decoding (#8804, Sage Weil)

For more detailed information, see :download:`the complete changelog <changelog/v0.80.3.txt>`.


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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.2.txt>`.

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

For more detailed information, see :download:`the complete changelog <changelog/v0.80.1.txt>`.


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
  upgrading to v0.80 Firefly.  Please refer to the `Dumpling upgrade`_
  documentation.

* We recommand adding the following to the [mon] section of your
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
  tunables and defaults.  Upgaded clusters using the old values will
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
  deployment is tight, you can preserve the old behavio by adding::

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

v0.72.3 Emperor (pending release)
=================================

Upgrading
---------

* Monitor 'auth' read-only commands now expect the user to have 'rx' caps.
  This is the same behavior that was present in dumpling, but in emperor
  and more recent development releases the 'r' cap was sufficient.  Note that
  this backported security fix will break mon keys that are using the following
  commands but do not have the 'x' bit in the mon capability::

    ceph auth export
    ceph auth get
    ceph auth get-key
    ceph auth print-key
    ceph auth list


v0.72.2 Emperor
===============

This is the second bugfix release for the v0.72.x Emperor series.  We
have fixed a hang in radosgw, and fixed (again) a problem with monitor
CLI compatiblity with mixed version monitors.  (In the future this
will no longer be a problem.)

Upgrading
---------

* The JSON schema for the 'osd pool set ...' command changed slightly.  Please
  avoid issuing this particular command via the CLI while there is a mix of
  v0.72.1 and v0.72.2 monitor daemons running.

* As part of fix for #6796, 'ceph osd pool set <pool> <var> <arg>' now
  receives <arg> as an integer instead of a string.  This affects how
  'hashpspool' flag is set/unset: instead of 'true' or 'false', it now
  must be '0' or '1'.


Changes
-------

* mon: 'osd pool set ...' syntax change
* osd: added test for missing on-disk HEAD object
* osd: fix osd bench block size argument
* rgw: fix hang on large object GET
* rgw: fix rare use-after-free
* rgw: various DR bug fixes
* rgw: do not return error on empty owner when setting ACL
* sysvinit, upstart: prevent starting daemons using both init systems

For more detailed information, see :download:`the complete changelog <changelog/v0.72.2.txt>`.

v0.72.1 Emperor
===============

Important Note
--------------

When you are upgrading from Dumpling to Emperor, do not run any of the
"ceph osd pool set" commands while your monitors are running separate versions.
Doing so could result in inadvertently changing cluster configuration settings
that exhaust compute resources in your OSDs.

Changes
-------

* osd: fix upgrade bug #6761
* ceph_filestore_tool: introduced tool to repair errors caused by #6761

This release addresses issue #6761.  Upgrading to Emperor can cause
reads to begin returning ENFILE (too many open files).  v0.72.1 fixes
that upgrade issue and adds a tool ceph_filestore_tool to repair osd
stores affected by this bug.

To repair a cluster affected by this bug:

#. Upgrade all osd machines to v0.72.1
#. Install the ceph-test package on each osd machine to get ceph_filestore_tool
#. Stop all osd processes
#. To see all lost objects, run the following on each osd with the osd stopped and
   the osd data directory mounted::

     ceph_filestore_tool --list-lost-objects=true --filestore-path=<path-to-osd-filestore> --journal-path=<path-to-osd-journal>

#. To fix all lost objects, run the following on each osd with the
   osd stopped and the osd data directory mounted::

     ceph_filestore_tool --fix-lost-objects=true --list-lost-objects=true --filestore-path=<path-to-osd-filestore> --journal-path=<path-to-osd-journal>

#. Once lost objects have been repaired on each osd, you can restart
   the cluster.

Note, the ceph_filestore_tool performs a scan of all objects on the
osd and may take some time.


v0.72 Emperor
=============

This is the fifth major release of Ceph, the fourth since adopting a
3-month development cycle.  This release brings several new features,
including multi-datacenter replication for the radosgw, improved
usability, and lands a lot of incremental performance and internal
refactoring work to support upcoming features in Firefly.

Important Note
--------------

When you are upgrading from Dumpling to Emperor, do not run any of the
"ceph osd pool set" commands while your monitors are running separate versions.
Doing so could result in inadvertently changing cluster configuration settings
that exhaust compute resources in your OSDs.

Highlights
----------

* common: improved crc32c performance
* librados: new example client and class code
* mds: many bug fixes and stability improvements
* mon: health warnings when pool pg_num values are not reasonable
* mon: per-pool performance stats
* osd, librados: new object copy primitives
* osd: improved interaction with backend file system to reduce latency
* osd: much internal refactoring to support ongoing erasure coding and tiering support
* rgw: bucket quotas
* rgw: improved CORS support
* rgw: performance improvements
* rgw: validate S3 tokens against Keystone

Coincident with core Ceph, the Emperor release also brings:

* radosgw-agent: support for multi-datacenter replication for disaster recovery
* tgt: improved support for iSCSI via upstream tgt

Packages for both are available on ceph.com.

Upgrade sequencing
------------------

There are no specific upgrade restrictions on the order or sequence of
upgrading from 0.67.x Dumpling. However, you cannot run any of the
"ceph osd pool set" commands while your monitors are running separate versions.
Doing so could result in inadvertently changing cluster configuration settings
and exhausting compute resources in your OSDs.

It is also possible to do a rolling upgrade from 0.61.x Cuttlefish,
but there are ordering restrictions.  (This is the same set of
restrictions for Cuttlefish to Dumpling.)

#. Upgrade ceph-common on all nodes that will use the command line 'ceph' utility.
#. Upgrade all monitors (upgrade ceph package, restart ceph-mon
   daemons).  This can happen one daemon or host at a time.  Note that
   because cuttlefish and dumpling monitors can't talk to each other,
   all monitors should be upgraded in relatively short succession to
   minimize the risk that an a untimely failure will reduce
   availability.
#. Upgrade all osds (upgrade ceph package, restart ceph-osd daemons).
   This can happen one daemon or host at a time.
#. Upgrade radosgw (upgrade radosgw package, restart radosgw daemons).


Upgrading from v0.71
--------------------

* ceph-fuse and radosgw now use the same default values for the admin
  socket and log file paths that the other daemons (ceph-osd,
  ceph-mon, etc.) do.  If you run these daemons as non-root, you may
  need to adjust your ceph.conf to disable these options or to adjust
  the permissions on /var/run/ceph and /var/log/ceph.

Upgrading from v0.67 Dumpling
-----------------------------

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


Notable Changes since v0.71
---------------------------

* build: fix [/usr]/sbin locations (Alan Somers)
* ceph-fuse, radosgw: enable admin socket and logging by default
* ceph: make -h behave when monitors are down
* common: cache crc32c values where possible
* common: fix looping on BSD (Alan Somers)
* librados, mon: ability to query/ping out-of-quorum monitor status (Joao Luis)
* librbd python bindings: fix parent image name limit (Josh Durgin)
* mds: avoid leaking objects when deleting truncated files (Yan, Zheng)
* mds: fix F_GETLK (Yan, Zheng)
* mds: fix many bugs with stray (unlinked) inodes (Yan, Zheng)
* mds: fix many directory fragmentation bugs (Yan, Zheng)
* mon: allow (un)setting HASHPSPOOL flag on existing pools (Joao Luis)
* mon: make 'osd pool rename' idempotent (Joao Luis)
* osd: COPY_GET on-wire encoding improvements (Greg Farnum)
* osd: bloom_filter encodability, fixes, cleanups (Loic Dachary, Sage Weil)
* osd: fix handling of racing read vs write (Samuel Just)
* osd: reduce blocking on backing fs (Samuel Just)
* radosgw-agent: multi-region replication/DR
* rgw: fix/improve swift COPY support (Yehuda Sadeh)
* rgw: misc fixes to support DR (Josh Durgin, Yehuda Sadeh)
* rgw: per-bucket quota (Yehuda Sadeh)
* rpm: fix junit dependencies (Alan Grosskurth)

Notable Changes since v0.67 Dumpling
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



v0.71
=====

This development release includes a significant amount of new code and
refactoring, as well as a lot of preliminary functionality that will be needed
for erasure coding and tiering support.  There are also several significant
patch sets improving this with the MDS.

Upgrading
---------

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

Notable Changes
---------------

* build: Makefile refactor (Roald J. van Loon)
* ceph-disk: fix journal preallocation
* ceph-fuse: trim deleted inodes from cache (Yan, Zheng)
* ceph-fuse: use newer fuse api (Jianpeng Ma)
* ceph-kvstore-tool: new tool for working with leveldb (copy, crc) (Joao Luis)
* common: bloom_filter improvements, cleanups
* common: correct SI is kB not KB (Dan Mick)
* common: misc portability fixes (Noah Watkins)
* hadoop: removed old version of shim to avoid confusing users (Noah Watkins)
* librados: fix installed header #includes (Dan Mick)
* librbd, ceph-fuse: avoid some sources of ceph-fuse, rbd cache stalls
* mds: fix LOOKUPSNAP bug
* mds: fix standby-replay when we fall behind (Yan, Zheng)
* mds: fix stray directory purging (Yan, Zheng)
* mon: disallow CephFS snapshots until 'ceph mds set allow_new_snaps' (Greg Farnum)
* mon, osd: improve osdmap trimming logic (Samuel Just)
* mon: kv properties for pools to support EC (Loic Dachary)
* mon: some auth check cleanups (Joao Luis)
* mon: track per-pool stats (Joao Luis)
* mon: warn about pools with bad pg_num
* osd: automatically detect proper xattr limits (David Zafman)
* osd: avoid extra copy in erasure coding reference implementation (Loic Dachary)
* osd: basic cache pool redirects (Greg Farnum)
* osd: basic whiteout, dirty flag support (not yet used)
* osd: clean up and generalize copy-from code (Greg Farnum)
* osd: erasure coding doc updates (Loic Dachary)
* osd: erasure coding plugin infrastructure, tests (Loic Dachary)
* osd: fix RWORDER flags
* osd: fix exponential backoff of slow request warnings (Loic Dachary)
* osd: generalized temp object infrastructure
* osd: ghobject_t infrastructure for EC (David Zafman)
* osd: improvements for compatset support and storage (David Zafman)
* osd: misc copy-from improvements
* osd: opportunistic crc checking on stored data (off by default)
* osd: refactor recovery using PGBackend (Samuel Just)
* osd: remove old magical tmap->omap conversion
* pybind: fix blacklisting nonce (Loic Dachary)
* rgw: default log level is now more reasonable (Yehuda Sadeh)
* rgw: fix acl group check (Yehuda Sadeh)
* sysvinit: fix shutdown order (mons last) (Alfredo Deza)

v0.70
=====

Upgrading
---------

* librados::Rados::pool_create_async() and librados::Rados::pool_delete_async()
  don't drop a reference to the completion object on error, caller needs to take
  care of that. This has never really worked correctly and we were leaking an
  object

* 'ceph osd crush set <id> <weight> <loc..>' no longer adds the osd to the
  specified location, as that's a job for 'ceph osd crush add'.  It will
  however continue to work just the same as long as the osd already exists
  in the crush map.

Notable Changes
---------------

* mon: a few 'ceph mon add' races fixed (command is now idempotent) (Joao Luis)
* crush: fix name caching
* rgw: fix a few minor memory leaks (Yehuda Sadeh)
* ceph: improve parsing of CEPH_ARGS (Benoit Knecht)
* mon: avoid rewriting full osdmaps on restart (Joao Luis)
* crc32c: fix optimized crc32c code (it now detects arch support properly)
* mon: fix 'ceph osd crush reweight ...' (Joao Luis)
* osd: revert xattr size limit (fixes large rgw uploads)
* mds: fix heap profiler commands (Joao Luis)
* rgw: fix inefficient use of std::list::size() (Yehuda Sadeh)


v0.69
=====

Upgrading
---------

* The sysvinit /etc/init.d/ceph script will, by default, update the
  CRUSH location of an OSD when it starts.  Previously, if the
  monitors were not available, this command would hang indefinitely.
  Now, that step will time out after 10 seconds and the ceph-osd daemon
  will not be started.

* Users of the librados C++ API should replace users of get_version()
  with get_version64() as the old method only returns a 32-bit value
  for a 64-bit field.  The existing 32-bit get_version() method is now
  deprecated.

* The OSDs are now more picky that request payload match their
  declared size.  A write operation across N bytes that includes M
  bytes of data will now be rejected.  No known clients do this, but
  the because the server-side behavior has changed it is possible that
  an application misusing the interface may now get errors.

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


Notable Changes
---------------

* build cleanly under clang (Christophe Courtaut)
* common: migrate SharedPtrRegistry to use boost::shared_ptr<> (Loic Dachary)
* doc: erasure coding design notes (Loic Dachary)
* improved intel-optimized crc32c support (~8x faster on my laptop!)
* librados: get_version64() method for C++ API
* mds: fix locking deadlock (David Disseldorp)
* mon, osd: initial CLI for configuring tiering
* mon: allow cap strings with . to be unquoted
* mon: continue to discover peer addr info during election phase
* mon: fix 'osd crush move ...' command for buckets (Joao Luis)
* mon: warn when mon data stores grow very large (Joao Luis)
* objecter, librados: redirect requests based on cache tier config
* osd, librados: add new COPY_FROM rados operation
* osd, librados: add new COPY_GET rados operations (used by COPY_FROM)
* osd: add 'osd heartbeat min healthy ratio' configurable (was hard-coded at 33%)
* osd: add option to disable pg log debug code (which burns CPU)
* osd: allow cap strings with . to be unquoted
* osd: fix version value returned by various operations (Greg Farnum)
* osd: infrastructure to copy objects from other OSDs
* osd: use fdatasync(2) instead of fsync(2) to improve performance (Sam Just)
* rgw: fix major CPU utilization bug with internal caching (Yehuda Sadeh, Mark Nelson)
* rgw: fix ordering of write operations (preventing data loss on crash) (Yehuda Sadeh)
* rgw: fix ordering of writes for mulitpart upload (Yehuda Sadeh)
* rgw: fix various CORS bugs (Yehuda Sadeh)
* rgw: improve help output (Christophe Courtaut)
* rgw: validate S3 tokens against keystone (Roald J. van Loon)
* rgw: wildcard support for keystone roles (Christophe Courtaut)
* sysvinit radosgw: fix status return code (Danny Al-Gaaf)
* sysvinit rbdmap: fix error 'service rbdmap stop' (Laurent Barbe)

v0.68
=====

Upgrading
---------

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



Notable Changes
---------------

* ceph-fuse: fix problem with readahead vs truncate race (Yan, Zheng)
* ceph-post-file: new command to easily share logs or other files with ceph devs
* ceph: parse CEPH_ARGS env variable
* librados: fix async aio completion wakeup
* librados: hello_world example (Greg Farnum)
* librados: sync calls now return on commit (instead of ack) (Greg Farnum)
* mds: fix mds rejoin with legacy parent backpointer xattrs (Alexandre Oliva)
* mds: fix rare restart/failure race during fs creation
* mds: notify clients about deleted files (so they can release from their cache) (Yan, Zheng)
* mds: several bug fixes with clustered mds (Yan, Zheng)
* mon: allow logging level of cluster log (/var/log/ceph/ceph.log) to be adjusted
* mon: do not expose uncommitted state from 'osd crush {add,set} ...' (Joao Luis)
* mon: fix byte counts (off by factor of 4) (Dan Mick, Joao Luis)
* mon: fix paxos corner case
* mon: modify 'auth add' semantics to make a bit more sense (Joao Luis)
* mon: new 'osd perf' command to dump recent performance information (Samuel Just)
* mon: new and improved 'ceph -s' or 'ceph status' command (more info, easier to read)
* monc: fix small memory leak
* new wireshark patches pulled into the tree (Kevin Jones)
* objecter: fix possible hang when cluster is unpaused (Josh Durgin)
* osd: 'osd recover clone overlap limit' option to limit cloning during recovery (Samuel Just)
* osd: cls_hello OSD class example
* osd: experiemental support for ZFS (zfsonlinux.org) (Yan, Zheng)
* osd: instrument peering states (David Zafman)
* osd: properly enforce RD/WR flags for rados classes
* osd: remove old pg log on upgrade (Samuel Just)
* rgw: complete in-progress requests before shutting down
* rgw: fix S3 auth with response-* query string params (Sylvain Munaut, Yehuda Sadeh)
* sysvinit: add condrestart command (Dan van der Ster)


v0.67.12 "Dumpling" (draft)
===========================

This stable update for Dumpling fixes a few longstanding issues with
backfill in the OSD that can lead to stalled IOs.  There is also a fix
for memory utilization for reads in librbd when caching is enabled,
and then several other small fixes across the rest of the system.

Dumpling users who have encountered IO stalls during backfill and who
do not expect to upgrade to Firefly soon should upgrade.  Everyone
else should upgrade to Firefly already.  This is likely to be the last stable
release for the 0.67.x Dumpling series.


Notable Changes
---------------

* buffer: fix buffer rebuild alignment corner case (#6614 #6003 Loic Dachary, Samuel Just)
* ceph-disk: reprobe partitions after zap (#9665 #9721 Loic Dachary)
* ceph-disk: use partx instead of partprobe when appropriate (Loic Dachary)
* common: add $cctid meta variable (#6228 Adam Crume)
* crush: fix get_full_location_ordered (Sage Weil)
* crush: pick ruleset id that matches rule_id (#9675 Xiaoxi Chen)
* libcephfs: fix tid wrap bug (#9869 Greg Farnum)
* libcephfs: get osd location on -1 should return EINVAL (Sage Weil)
* librados: fix race condition with C API and op timeouts (#9582 Sage Weil)
* librbd: constrain max number of in-flight read requests (#9854 Jason Dillaman)
* librbd: enforce cache size on read requests (Jason Dillaman)
* librbd: fix invalid close in image open failure path (#10030 Jason Dillaman)
* librbd: fix read hang on sparse files (Jason Dillaman)
* librbd: gracefully handle deleted/renamed pools (#10270 #10122 Jason Dillaman)
* librbd: protect list_children from invalid child pool ioctxs (#10123 Jason Dillaman)
* mds: fix ctime updates from clients without dirty caps (#9514 Greg Farnum)
* mds: fix rare NULL dereference in cap update path (Greg Farnum)
* mds: fix assertion caused by system clock backwards (#11053 Yan, Zheng)
* mds: store backtrace on straydir (Yan, Zheng)
* osd: fix journal committed_thru update after replay (#6756 Samuel Just)
* osd: fix memory leak, busy loop on snap trim (#9113 Samuel Just)
* osd: fix misc peering, recovery bugs (#10168 Samuel Just)
* osd: fix purged_snap field on backfill start (#9487 Sage Weil, Samuel Just)
* osd: handle no-op write with snapshot corner case (#10262 Sage Weil, Loic Dachary)
* osd: respect RWORDERED rados flag (Sage Weil)
* osd: several backfill fixes and refactors (Samuel Just, David Zafman)
* rgw: send http status reason explicitly in fastcgi (Yehuda Sadeh)

v0.67.11 "Dumpling"
===================

This stable update for Dumpling fixes several important bugs that
affect a small set of users.

We recommend that all Dumpling users upgrade at their convenience.  If
none of these issues are affecting your deployment there is no
urgency.


Notable Changes
---------------

* common: fix sending dup cluster log items (#9080 Sage Weil)
* doc: several doc updates (Alfredo Deza)
* libcephfs-java: fix build against older JNI headesr (Greg Farnum)
* librados: fix crash in op timeout path (#9362 Matthias Kiefer, Sage Weil)
* librbd: fix crash using clone of flattened image (#8845 Josh Durgin)
* librbd: fix error path cleanup when failing to open image (#8912 Josh Durgin)
* mon: fix crash when adjusting pg_num before any OSDs are added (#9052 Sage Weil)
* mon: reduce log noise from paxos (Aanchal Agrawal, Sage Weil)
* osd: allow scrub and snap trim thread pool IO priority to be adjusted (Sage Weil)
* osd: fix mount/remount sync race (#9144 Sage Weil)


v0.67.10 "Dumpling"
===================

This stable update release for Dumpling includes primarily fixes for
RGW, including several issues with bucket listings and a potential
data corruption problem when multiple multi-part uploads race.  There is also
some throttling capability added in the OSD for scrub that can mitigate the
performance impact on production clusters.

We recommend that all Dumpling users upgrade at their convenience.

Notable Changes
---------------

* ceph-disk: partprobe befoere settle, fixing dm-crypt (#6966, Eric Eastman)
* librbd: add invalidate cache interface (Josh Durgin)
* librbd: close image if remove_child fails (Ilya Dryomov)
* librbd: fix potential null pointer dereference (Danny Al-Gaaf)
* librbd: improve writeback checks, performance (Haomai Wang)
* librbd: skip zeroes when copying image (#6257, Josh Durgin)
* mon: fix rule(set) check on 'ceph pool set ... crush_ruleset ...' (#8599, John Spray)
* mon: shut down if mon is removed from cluster (#6789, Joao Eduardo Luis)
* osd: fix filestore perf reports to mon (Sage Weil)
* osd: force any new or updated xattr into leveldb if E2BIG from XFS (#7779, Sage Weil)
* osd: lock snapdir object during write to fix race with backfill (Samuel Just)
* osd: option sleep during scrub (Sage Weil)
* osd: set io priority on scrub and snap trim threads (Sage Weil)
* osd: 'status' admin socket command (Sage Weil)
* rbd: tolerate missing NULL terminator on block_name_prefix (#7577, Dan Mick)
* rgw: calculate user manifest (#8169, Yehuda Sadeh)
* rgw: fix abort on chunk read error, avoid using extra memory (#8289, Yehuda Sadeh)
* rgw: fix buffer overflow on bucket instance id (#8608, Yehuda Sadeh)
* rgw: fix crash in swift CORS preflight request (#8586, Yehuda Sadeh)
* rgw: fix implicit removal of old objects on object creation (#8972, Patrycja Szablowska, Yehuda Sadeh)
* rgw: fix MaxKeys in bucket listing (Yehuda Sadeh)
* rgw: fix race with multiple updates to a single multipart object (#8269, Yehuda Sadeh)
* rgw: improve bucket listing with delimiter (Yehuda Sadeh)
* rgw: include NextMarker in bucket listing (#8858, Yehuda Sadeh)
* rgw: return error early on non-existent bucket (#7064, Yehuda Sadeh)
* rgw: set truncation flag correctly in bucket listing (Yehuda Sadeh)
* sysvinit: continue starting daemons after pre-mount error (#8554, Sage Weil)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.10.txt>`.


v0.67.9 "Dumpling"
==================

This Dumpling point release fixes several minor bugs. The most
prevalent in the field is one that occasionally prevents OSDs from
starting on recently created clusters.

We recommend that all Dumpling users upgrade at their convenience.

Notable Changes
---------------

* ceph-fuse, libcephfs: client admin socket command to kick and inspect MDS sessions (#8021, Zheng Yan)
* monclient: fix failure detection during mon handshake (#8278, Sage Weil)
* mon: set tid on no-op PGStatsAck messages (#8280, Sage Weil)
* msgr: fix a rare bug with connection negotiation between OSDs (Guang Yang)
* osd: allow snap trim throttling with simple delay (#6278, Sage Weil)
* osd: check for splitting when processing recover/backfill reservations (#6565, Samuel Just)
* osd: fix backfill position tracking (#8162, Samuel Just)
* osd: fix bug in backfill stats (Samuel Just)
* osd: fix bug preventing OSD startup for infant clusters (#8162, Greg Farnum)
* osd: fix rare PG resurrection race causing an incomplete PG (#7740, Samuel Just)
* osd: only complete replicas count toward min_size (#7805, Samuel Just)
* rgw: allow setting ACLs with empty owner (#6892, Yehuda Sadeh)
* rgw: send user manifest header field (#8170, Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.9.txt>`.


v0.67.8 "Dumpling"
==================

This Dumpling point release fixes several non-critical issues since
v0.67.7.  The most notable bug fixes are an auth fix in librbd
(observed as an occasional crash from KVM), an improvement in the
network failure detection with the monitor, and several hard to hit
OSD crashes or hangs.

We recommend that all users upgrade at their convenience.

Upgrading
---------

* The 'rbd ls' function now returns success and returns an empty when a pool
  does not store any rbd images.  Previously it would return an ENOENT error.

* Ceph will now issue a health warning if the 'mon osd down out
  interval' config option is set to zero.  This warning can be
  disabled by adding 'mon warn on osd down out interval zero = false'
  to ceph.conf.

Notable Changes
---------------

* all: improve keepalive detection of failed monitor connections (#7888, Sage Weil)
* ceph-fuse, libcephfs: pin inodes during readahead, fixing rare crash (#7867, Sage Weil)
* librbd: make cache writeback a bit less aggressive (Sage Weil)
* librbd: make symlink for qemu to detect librbd in RPM (#7293, Josh Durgin)
* mon: allow 'hashpspool' pool flag to be set and unset (Loic Dachary)
* mon: commit paxos state only after entire quorum acks, fixing rare race where prior round state is readable (#7736, Sage Weil)
* mon: make elections and timeouts a bit more robust (#7212, Sage Weil)
* mon: prevent extreme pool split operations (Greg Farnum)
* mon: wait for quorum for get_version requests to close rare pool creation race (#7997, Sage Weil)
* mon: warn on 'mon osd down out interval = 0' (#7784, Joao Luis)
* msgr: fix byte-order for auth challenge, fixing auth errors on big-endian clients (#7977, Dan Mick)
* msgr: fix occasional crash in authentication code (usually triggered by librbd) (#6840, Josh Durgin)
* msgr: fix rebind() race (#6992, Xihui He)
* osd: avoid timeouts during slow PG deletion (#6528, Samuel Just)
* osd: fix bug in pool listing during recovery (#6633, Samuel Just)
* osd: fix queue limits, fixing recovery stalls (#7706, Samuel Just)
* osd: fix rare peering crashes (#6722, #6910, Samuel Just)
* osd: fix rare recovery hang (#6681, Samuel Just)
* osd: improve error handling on journal errors (#7738, Sage Weil)
* osd: reduce load on the monitor from OSDMap subscriptions (Greg Farnum)
* osd: rery GetLog on peer osd startup, fixing some rare peering stalls (#6909, Samuel Just)
* osd: reset journal state on remount to fix occasional crash on OSD startup (#8019, Sage Weil)
* osd: share maps with peers more aggressively (Greg Farnum)
* rbd: make it harder to delete an rbd image that is currently in use (#7076, Ilya Drymov)
* rgw: deny writes to secondary zone by non-system users (#6678, Yehuda Sadeh)
* rgw: do'nt log system requests in usage log (#6889, Yehuda Sadeh)
* rgw: fix bucket recreation (#6951, Yehuda Sadeh)
* rgw: fix Swift range response (#7099, Julien Calvet, Yehuda Sadeh)
* rgw: fix URL escaping (#8202, Yehuda Sadeh)
* rgw: fix whitespace trimming in http headers (#7543, Yehuda Sadeh)
* rgw: make multi-object deletion idempotent (#7346, Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.8.txt>`.

v0.67.7 "Dumpling"
==================

This Dumpling point release fixes a few critical issues in v0.67.6.

All v0.67.6 users are urgently encouraged to upgrade.  We also
recommend that all v0.67.5 (or older) users upgrade.

Upgrading
---------

* Once you have upgraded a radosgw instance or OSD to v0.67.7, you should not
  downgrade to a previous version.

Notable Changes
---------------

* ceph-disk: additional unit tests
* librbd: revert caching behavior change in v0.67.6
* osd: fix problem reading xattrs due to incomplete backport in v0.67.6
* radosgw-admin: fix reading object policy

For more detailed information, see :download:`the complete changelog <changelog/v0.67.7.txt>`.


v0.67.6 "Dumpling"
==================

.. note: This release contains a librbd bug that is fixed in v0.67.7.  Please upgrade to v0.67.7 and do not use v0.67.6.

This Dumpling point release contains a number of important fixed for
the OSD, monitor, and radosgw.  Most significantly, a change that
forces large object attributes to spill over into leveldb has been
backported that can prevent objects and the cluster from being damaged
by large attributes (which can be induced via the radosgw).  There is
also a set of fixes that improves data safety and RADOS semantics when
the cluster becomes full and then non-full.

We recommend that all 0.67.x Dumpling users skip this release and upgrade to v0.67.7.

Upgrading
---------

* The OSD has long contained a feature that allows large xattrs to
  spill over into the leveldb backing store in situations where not
  all local file systems are able to store them reliably.  This option
  is now enabled unconditionally in order to avoid rare cases where
  storing large xattrs renders the object unreadable. This is known to
  be triggered by very large multipart objects, but could be caused by
  other workloads as well.  Although there is some small risk that
  performance for certain workloads will degrade, it is more important
  that data be retrievable.  Note that newer versions of Ceph (e.g.,
  firefly) do some additional work to avoid the potential performance
  regression in this case, but that is current considered too complex
  for backport to the Dumpling stable series.

* It is very dangerous to downgrade from v0.67.6 to a prior version of
  Dumpling.  If the old version does not have 'filestore xattr use
  omap = true' it may not be able to read all xattrs for an object and
  can cause undefined behavior.

Notable changes
---------------

* ceph-disk: misc bug fixes, particularly on RHEL (Loic Dachary, Alfredo Deza, various)
* ceph-fuse, libcephfs: fix crash from read over certain sparseness patterns (Sage Weil)
* ceph-fuse, libcephfs: fix integer overflow for sync reads racing with appends (Sage Weil)
* ceph.spec: fix udev rule when building RPM under RHEL (Derek Yarnell)
* common: fix crash from bad format from admin socket (Loic Dachary)
* librados: add optional timeouts (Josh Durgin)
* librados: do not leak budget when resending localized or redirected ops (Josh Durgin)
* librados, osd: fix and improve full cluster handling (Josh Durgin)
* librbd: fix use-after-free when updating perfcounters during image close (Josh Durgin)
* librbd: remove limit on objects in cache (Josh Durgin)
* mon: avoid on-disk full OSDMap corruption from pg_temp removal (Sage Weil)
* mon: avoid stray pg_temp entries from pool deletion race (Joao Eduardo Luis)
* mon: do not generate spurious MDSMaps from laggy daemons (Joao Eduardo Luis)
* mon: fix error code from 'osd rm|down|out|in ...' commands (Loic Dachary)
* mon: include all health items in summary output (John Spray)
* osd: fix occasional race/crash during startup (Sage Weil)
* osd: ignore stray OSDMap messages during init (Sage Weil)
* osd: unconditionally let xattrs overflow into leveldb (David Zafman)
* rados: fix a few error checks for the CLI (Josh Durgin)
* rgw: convert legacy bucket info objects on demand (Yehuda Sadeh)
* rgw: fix bug causing system users to lose privileges (Yehuda Sadeh)
* rgw: fix CORS bugs related to headers and case sensitivity (Robin H. Johnson)
* rgw: fix multipart object listing (Yehuda Sadeh)
* rgw: fix racing object creations (Yehuda Sadeh)
* rgw: fix racing object put and delete (Yehuda Sadeh)
* rgw: fix S3 auth when using response-* query string params (Sylvain Munaut)
* rgw: use correct secret key for POST authentication (Robin H. Johnson)

For more detailed information, see :download:`the complete changelog <changelog/v0.67.6.txt>`.


v0.67.5 "Dumpling"
==================

This release includes a few critical bug fixes for the radosgw, 
including a fix for hanging operations on large objects.  There are also
several bug fixes for radosgw multi-site replications, and a few 
backported features.  Also, notably, the 'osd perf' command (which dumps
recent performance information about active OSDs) has been backported.

We recommend that all 0.67.x Dumpling users upgrade.

Notable changes
---------------

* ceph-fuse: fix crash in caching code
* mds: fix looping in populate_mydir()
* mds: fix standby-replay race
* mon: accept 'osd pool set ...' as string
* mon: backport: 'osd perf' command to dump recent OSD performance stats
* osd: add feature compat check for upcoming object sharding
* osd: fix osd bench block size argument
* rbd.py: increase parent name size limit
* rgw: backport: allow wildcard in supported keystone roles
* rgw: backport: improve swift COPY behavior
* rgw: backport: log and open admin socket by default
* rgw: backport: validate S3 tokens against keystone
* rgw: fix bucket removal
* rgw: fix client error code for chunked PUT failure
* rgw: fix hang on large object GET
* rgw: fix rare use-after-free
* rgw: various DR bug fixes
* sysvinit, upstart: prevent starting daemons using both init systems

For more detailed information, see :download:`the complete changelog <changelog/v0.67.5.txt>`.


v0.67.4 "Dumpling"
==================

This point release fixes an important performance issue with radosgw,
keystone authentication token caching, and CORS.  All users
(especially those of rgw) are encouraged to upgrade.

Notable changes
---------------

* crush: fix invalidation of cached names
* crushtool: do not crash on non-unique bucket ids
* mds: be more careful when decoding LogEvents
* mds: fix heap check debugging commands
* mon: avoid rebuilding old full osdmaps
* mon: fix 'ceph crush move ...'
* mon: fix 'ceph osd crush reweight ...'
* mon: fix writeout of full osdmaps during trim
* mon: limit size of transactions
* mon: prevent both unmanaged and pool snaps
* osd: disable xattr size limit (prevents upload of large rgw objects)
* osd: fix recovery op throttling
* osd: fix throttling of log messages for very slow requests
* rgw: drain pending requests before completing write
* rgw: fix CORS
* rgw: fix inefficient list::size() usage
* rgw: fix keystone token expiration
* rgw: fix minor memory leaks
* rgw: fix null termination of buffer

For more detailed information, see :download:`the complete changelog <changelog/v0.67.4.txt>`.


v0.67.3 "Dumpling"
==================

This point release fixes a few important performance regressions with
the OSD (both with CPU and disk utilization), as well as several other
important but less common problems.  We recommend that all production users
upgrade.

Notable Changes
---------------

* ceph-disk: partprobe after creation journal partition
* ceph-disk: specify fs type when mounting
* ceph-post-file: new utility to help share logs and other files with ceph developers
* libcephfs: fix truncate vs readahead race (crash)
* mds: fix flock/fcntl lock deadlock
* mds: fix rejoin loop when encountering pre-dumpling backpointers
* mon: allow name and addr discovery during election stage
* mon: always refresh after Paxos store_state (fixes recovery corner case)
* mon: fix off-by-4x bug with osd byte counts
* osd: add and disable 'pg log keys debug' by default
* osd: add option to disable throttling
* osd: avoid leveldb iterators for pg log append and trim
* osd: fix readdir_r invocations
* osd: use fdatasync instead of sync
* radosgw: fix sysvinit script return status
* rbd: relicense as LGPL2
* rgw: flush pending data on multipart upload
* rgw: recheck object name during S3 POST
* rgw: reorder init/startup
* rpm: fix debuginfo package build

For more detailed information, see :download:`the complete changelog <changelog/v0.67.3.txt>`.


v0.67.2 "Dumpling"
==================

This is an imporant point release for Dumpling.  Most notably, it
fixes a problem when upgrading directly from v0.56.x Bobtail to
v0.67.x Dumpling (without stopping at v0.61.x Cuttlefish along the
way).  It also fixes a problem with the CLI parsing of the CEPH_ARGS
environment variable, high CPU utilization by the ceph-osd daemons,
and cleans up the radosgw shutdown sequence.

Notable Changes
---------------

* objecter: resend linger requests when cluster goes from full to non-full
* ceph: parse CEPH_ARGS environment variable
* librados: fix small memory leak
* osd: remove old log objects on upgrade (fixes bobtail -> dumpling jump)
* osd: disable PGLog::check() via config option (fixes CPU burn)
* rgw: drain requests on shutdown
* rgw: misc memory leaks on shutdown

For more detailed information, see :download:`the complete changelog <changelog/v0.67.2.txt>`.


v0.67.1 "Dumpling"
==================

This is a minor point release for Dumpling that fixes problems with
OpenStack and librbd hangs when caching is disabled.

Notable changes
---------------

* librados, librbd: fix constructor for python bindings with certain
  usages (in particular, that used by OpenStack)
* librados, librbd: fix aio_flush wakeup when cache is disabled
* librados: fix locking for aio completion refcounting
* fixes 'ceph --admin-daemon ...' command error code on error
* fixes 'ceph daemon ... config set ...' command for boolean config
  options.

For more detailed information, see :download:`the complete changelog <changelog/v0.67.1.txt>`.

v0.67 "Dumpling"
================

This is the fourth major release of Ceph, code-named "Dumpling."  The
headline features for this release include:

* Multi-site support for radosgw.  This includes the ability to set up
  separate "regions" in the same or different Ceph clusters that share
  a single S3/Swift bucket/container namespace.

* RESTful API endpoint for Ceph cluster administration.
  ceph-rest-api, a wrapper around ceph_rest_api.py, can be used to
  start up a test single-threaded HTTP server that provides access to
  cluster information and administration in very similar ways to the
  ceph commandline tool.  ceph_rest_api.py can be used as a WSGI
  application for deployment in a more-capable web server.  See
  ceph-rest-api.8 for more.

* Object namespaces in librados.


.. _Dumpling upgrade:

Upgrade Sequencing
------------------

It is possible to do a rolling upgrade from Cuttlefish to Dumpling.

#. Upgrade ceph-common on all nodes that will use the command line
   'ceph' utility.
#. Upgrade all monitors (upgrade ceph package, restart ceph-mon
   daemons).  This can happen one daemon or host at a time.  Note that
   because cuttlefish and dumpling monitors can't talk to each other,
   all monitors should be upgraded in relatively short succession to
   minimize the risk that an a untimely failure will reduce
   availability.
#. Upgrade all osds (upgrade ceph package, restart ceph-osd daemons).
   This can happen one daemon or host at a time.
#. Upgrade radosgw (upgrade radosgw package, restart radosgw daemons).


Upgrading from v0.66
--------------------

* There is monitor internal protocol change, which means that v0.67
  ceph-mon daemons cannot talk to v0.66 or older daemons.  We
  recommend upgrading all monitors at once (or in relatively quick
  succession) to minimize the possibility of downtime.

* The output of 'ceph status --format=json' or 'ceph -s --format=json'
  has changed to return status information in a more structured and
  usable format.

* The 'ceph pg dump_stuck [threshold]' command used to require a
  --threshold or -t prefix to the threshold argument, but now does
  not.

* Many more ceph commands now output formatted information; select
  with '--format=<format>', where <format> can be 'json', 'json-pretty',
  'xml', or 'xml-pretty'.

* The 'ceph pg <pgid> ...' commands (like 'ceph pg <pgid> query') are
  deprecated in favor of 'ceph tell <pgid> ...'.  This makes the
  distinction between 'ceph pg <command> <pgid>' and 'ceph pg <pgid>
  <command>' less awkward by making it clearer that the 'tell'
  commands are talking to the OSD serving the placement group, not the
  monitor.

* The 'ceph --admin-daemon <path> <command ...>' used to accept the
  command and arguments as either a single string or as separate
  arguments.  It will now only accept the command spread across
  multiple arguments.  This means that any script which does something
  like::

    ceph --admin-daemon /var/run/ceph/ceph-osd.0.asok 'config set debug_ms 1'

  needs to remove the quotes.  Also, note that the above can now be
  shortened to::

    ceph daemon osd.0 config set debug_ms 1

* The radosgw caps were inconsistently documented to be either 'mon =
  allow r' or 'mon = allow rw'.  The 'mon = allow rw' is required for
  radosgw to create its own pools.  All documentation has been updated
  accordingly.

* The radosgw copy object operation may return extra progress info
  during the operation. At this point it will only happen when doing
  cross zone copy operations. The S3 response will now return extra
  <Progress> field under the <CopyResult> container. The Swift
  response will now send the progress as a json array.

* In v0.66 and v0.65 the HASHPSPOOL pool flag was enabled by default
  for new pools, but has been disabled again until Linux kernel client
  support reaches more distributions and users.

* ceph-osd now requires a max file descriptor limit (e.g., ``ulimit -n
  ...``) of at least
  filestore_wbthrottle_(xfs|btrfs)_inodes_hard_limit (5000 by default)
  in order to accomodate the new write back throttle system.  On
  Ubuntu, upstart now sets the fd limit to 32k.  On other platforms,
  the sysvinit script will set it to 32k by default (still
  overrideable via max_open_files).  If this field has been customized
  in ceph.conf it should likely be adjusted upwards.

Upgrading from v0.61 "Cuttlefish"
---------------------------------

In addition to the above notes about upgrading from v0.66:

* There has been a huge revamp of the 'ceph' command-line interface
  implementation.  The ``ceph-common`` client library needs to be
  upgrade before ``ceph-mon`` is restarted in order to avoid problems
  using the CLI (the old ``ceph`` client utility cannot talk to the
  new ``ceph-mon``).

* The CLI is now very careful about sending the 'status' one-liner
  output to stderr and command output to stdout.  Scripts relying on
  output should take care.

* The 'ceph osd tell ...' and 'ceph mon tell ...' commands are no
  longer supported.  Any callers should use::

	ceph tell osd.<id or *> ...
	ceph tell mon.<id or name or *> ...

  The 'ceph mds tell ...' command is still there, but will soon also
  transition to 'ceph tell mds.<id or name or \*> ...'

* The 'ceph osd crush add ...' command used to take one of two forms::

    ceph osd crush add 123 osd.123 <weight> <location ...>
    ceph osd crush add osd.123 <weight> <location ...>

  This is because the id and crush name are redundant.  Now only the
  simple form is supported, where the osd name/id can either be a bare
  id (integer) or name (osd.<id>)::

    ceph osd crush add osd.123 <weight> <location ...>
    ceph osd crush add 123 <weight> <location ...>

* There is now a maximum RADOS object size, configurable via 'osd max
  object size', defaulting to 100 GB.  Note that this has no effect on
  RBD, CephFS, or radosgw, which all stripe over objects. If you are
  using librados and storing objects larger than that, you will need
  to adjust 'osd max object size', and should consider using smaller
  objects instead.

* The 'osd min down {reporters|reports}' config options have been
  renamed to 'mon osd min down {reporters|reports}', and the
  documentation has been updated to reflect that these options apply
  to the monitors (who process failure reports) and not OSDs.  If you
  have adjusted these settings, please update your ``ceph.conf``
  accordingly.


Notable changes since v0.66
---------------------------

* mon: sync improvements (performance and robustness)
* mon: many bug fixes (paxos and services)
* mon: fixed bugs in recovery and io rate reporting (negative/large values)
* mon: collect metadata on osd performance
* mon: generate health warnings from slow or stuck requests
* mon: expanded --format=<json|xml|...> support for monitor commands
* mon: scrub function for verifying data integrity
* mon, osd: fix old osdmap trimming logic
* mon: enable leveldb caching by default
* mon: more efficient storage of PG metadata
* ceph-rest-api: RESTful endpoint for administer cluster (mirrors CLI)
* rgw: multi-region support
* rgw: infrastructure to support georeplication of bucket and user metadata
* rgw: infrastructure to support georeplication of bucket data
* rgw: COPY object support between regions
* rbd: /etc/ceph/rbdmap file for mapping rbd images on startup
* osd: many bug fixes
* osd: limit number of incremental osdmaps sent to peers (could cause osds to be wrongly marked down)
* osd: more efficient small object recovery
* osd, librados: support for object namespaces
* osd: automatically enable xattrs on leveldb as necessary
* mds: fix bug in LOOKUPINO (used by nfs reexport)
* mds: fix O_TRUNC locking
* msgr: fixed race condition in inter-osd network communication
* msgr: fixed various memory leaks related to network sessions
* ceph-disk: fixes for unusual device names, partition detection
* hypertable: fixes for hypertable CephBroker bindings
* use SSE4.2 crc32c instruction if present


Notable changes since v0.61 "Cuttlefish"
----------------------------------------

* add 'config get' admin socket command
* ceph-conf: --show-config-value now reflects daemon defaults
* ceph-disk: add '[un]suppress-active DEV' command
* ceph-disk: avoid mounting over an existing osd in /var/lib/ceph/osd/*
* ceph-disk: fixes for unusual device names, partition detection
* ceph-disk: improved handling of odd device names
* ceph-disk: many fixes for RHEL/CentOS, Fedora, wheezy
* ceph-disk: simpler, more robust locking
* ceph-fuse, libcephfs: fix a few caps revocation bugs
* ceph-fuse, libcephfs: fix read zeroing at EOF
* ceph-fuse, libcephfs: fix request refcounting bug (hang on shutdown)
* ceph-fuse, libcephfs: fix truncatation bug on >4MB files (Yan, Zheng)
* ceph-fuse, libcephfs: fix for cap release/hang
* ceph-fuse: add ioctl support
* ceph-fuse: fixed long-standing O_NOATIME vs O_LAZY bug
* ceph-rest-api: RESTful endpoint for administer cluster (mirrors CLI)
* ceph, librados: fix resending of commands on mon reconnect
* daemons: create /var/run/ceph as needed
* debian wheezy: fix udev rules
* debian, specfile: packaging cleanups
* debian: fix upstart behavior with upgrades
* debian: rgw: stop daemon on uninstall
* debian: stop daemons on uninstall; fix dependencies
* hypertable: fixes for hypertable CephBroker bindings
* librados python binding cleanups
* librados python: fix xattrs > 4KB (Josh Durgin)
* librados: configurable max object size (default 100 GB)
* librados: new calls to administer the cluster
* librbd: ability to read from local replicas
* librbd: locking tests (Josh Durgin)
* librbd: make default options/features for newly created images (e.g., via qemu-img) configurable
* librbd: parallelize delete, rollback, flatten, copy, resize
* many many fixes from static code analysis (Danny Al-Gaaf)
* mds: fix O_TRUNC locking
* mds: fix bug in LOOKUPINO (used by nfs reexport)
* mds: fix rare hang after client restart
* mds: fix several bugs (Yan, Zheng)
* mds: many backpointer improvements (Yan, Zheng)
* mds: many fixes for mds clustering
* mds: misc stability fixes (Yan, Zheng, Greg Farnum)
* mds: new robust open-by-ino support (Yan, Zheng)
* mds: support robust lookup by ino number (good for NFS) (Yan, Zheng)
* mon, ceph: huge revamp of CLI and internal admin API. (Dan Mick)
* mon, osd: fix old osdmap trimming logic
* mon, osd: many memory leaks fixed
* mon: better trim/compaction behavior
* mon: collect metadata on osd performance
* mon: enable leveldb caching by default
* mon: expanded --format=<json|xml|...> support for monitor commands
* mon: fix election timeout
* mon: fix leveldb compression, trimming
* mon: fix start fork behavior
* mon: fix units in 'ceph df' output
* mon: fix validation of mds ids from CLI commands
* mon: fixed bugs in recovery and io rate reporting (negative/large values)
* mon: generate health warnings from slow or stuck requests
* mon: many bug fixes (paxos and services, sync)
* mon: many stability fixes (Joao Luis)
* mon: more efficient storage of PG metadata
* mon: new --extract-monmap to aid disaster recovery
* mon: new capability syntax
* mon: scrub function for verifying data integrity
* mon: simplify PaxosService vs Paxos interaction, fix readable/writeable checks
* mon: sync improvements (performance and robustness)
* mon: tuning, performance improvements
* msgr: fix various memory leaks
* msgr: fixed race condition in inter-osd network communication
* msgr: fixed various memory leaks related to network sessions
* osd, librados: support for object namespaces
* osd, mon: optionally dump leveldb transactions to a log
* osd: automatically enable xattrs on leveldb as necessary
* osd: avoid osd flapping from asymmetric network failure
* osd: break blacklisted client watches (David Zafman)
* osd: close narrow journal race
* osd: do not use fadvise(DONTNEED) on XFS (data corruption on power cycle)
* osd: fix for an op ordering bug
* osd: fix handling for split after upgrade from bobtail
* osd: fix incorrect mark-down of osds
* osd: fix internal heartbeart timeouts when scrubbing very large objects
* osd: fix memory/network inefficiency during deep scrub
* osd: fixed problem with front-side heartbeats and mixed clusters (David Zafman)
* osd: limit number of incremental osdmaps sent to peers (could cause osds to be wrongly marked down)
* osd: many bug fixes
* osd: monitor both front and back interfaces
* osd: more efficient small object recovery
* osd: new writeback throttling (for less bursty write performance) (Sam Just)
* osd: pg log (re)writes are now vastly more efficient (faster peering) (Sam Just)
* osd: ping/heartbeat on public and private interfaces
* osd: prioritize recovery for degraded PGs
* osd: re-use partially deleted PG contents when present (Sam Just)
* osd: recovery and peering performance improvements
* osd: resurrect partially deleted PGs
* osd: verify both front and back network are working before rejoining cluster
* rados: clonedata command for cli
* radosgw-admin: create keys for new users by default
* rbd: /etc/ceph/rbdmap file for mapping rbd images on startup
* rgw: COPY object support between regions
* rgw: fix CORS bugs
* rgw: fix locking issue, user operation mask,
* rgw: fix radosgw-admin buckets list (Yehuda Sadeh)
* rgw: fix usage log scanning for large, untrimmed logs
* rgw: handle deep uri resources
* rgw: infrastructure to support georeplication of bucket and user metadata
* rgw: infrastructure to support georeplication of bucket data
* rgw: multi-region support
* sysvinit: fix enumeration of local daemons
* sysvinit: fix osd crush weight calculation when using -a
* sysvinit: handle symlinks in /var/lib/ceph/osd/*
* use SSE4.2 crc32c instruction if present


v0.66
=====

Upgrading
---------

* There is now a configurable maximum rados object size, defaulting to 100 GB.  If you
  are using librados and storing objects larger than that, you will need to adjust
  'osd max object size', and should consider using smaller objects instead.

Notable changes
---------------

* osd: pg log (re)writes are now vastly more efficient (faster peering) (Sam Just)
* osd: fixed problem with front-side heartbeats and mixed clusters (David Zafman)
* mon: tuning, performance improvements
* mon: simplify PaxosService vs Paxos interaction, fix readable/writeable checks
* rgw: fix radosgw-admin buckets list (Yehuda Sadeh)
* mds: support robust lookup by ino number (good for NFS) (Yan, Zheng)
* mds: fix several bugs (Yan, Zheng)
* ceph-fuse, libcephfs: fix truncatation bug on >4MB files (Yan, Zheng)
* ceph/librados: fix resending of commands on mon reconnect
* librados python: fix xattrs > 4KB (Josh Durgin)
* librados: configurable max object size (default 100 GB)
* msgr: fix various memory leaks
* ceph-fuse: fixed long-standing O_NOATIME vs O_LAZY bug
* ceph-fuse, libcephfs: fix request refcounting bug (hang on shutdown)
* ceph-fuse, libcephfs: fix read zeroing at EOF
* ceph-conf: --show-config-value now reflects daemon defaults
* ceph-disk: simpler, more robust locking
* ceph-disk: avoid mounting over an existing osd in /var/lib/ceph/osd/*
* sysvinit: handle symlinks in /var/lib/ceph/osd/*


v0.65
=====

Upgrading
---------

* Huge revamp of the 'ceph' command-line interface implementation.
  The ``ceph-common`` client library needs to be upgrade before
  ``ceph-mon`` is restarted in order to avoid problems using the CLI
  (the old ``ceph`` client utility cannot talk to the new
  ``ceph-mon``).

* The CLI is now very careful about sending the 'status' one-liner
  output to stderr and command output to stdout.  Scripts relying on
  output should take care.

* The 'ceph osd tell ...' and 'ceph mon tell ...' commands are no
  longer supported.  Any callers should use::

	ceph tell osd.<id or *> ...
	ceph tell mon.<id or name or *> ...

  The 'ceph mds tell ...' command is still there, but will soon also
  transition to 'ceph tell mds.<id or name or \*> ...'

* The 'ceph osd crush add ...' command used to take one of two forms::

    ceph osd crush add 123 osd.123 <weight> <location ...>
    ceph osd crush add osd.123 <weight> <location ...>

  This is because the id and crush name are redundant.  Now only the
  simple form is supported, where the osd name/id can either be a bare
  id (integer) or name (osd.<id>)::

    ceph osd crush add osd.123 <weight> <location ...>
    ceph osd crush add 123 <weight> <location ...>

* There is now a maximum RADOS object size, configurable via 'osd max
  object size', defaulting to 100 GB.  Note that this has no effect on
  RBD, CephFS, or radosgw, which all stripe over objects.


Notable changes
---------------

* mon, ceph: huge revamp of CLI and internal admin API. (Dan Mick)
* mon: new capability syntax
* osd: do not use fadvise(DONTNEED) on XFS (data corruption on power cycle)
* osd: recovery and peering performance improvements
* osd: new writeback throttling (for less bursty write performance) (Sam Just)
* osd: ping/heartbeat on public and private interfaces
* osd: avoid osd flapping from asymmetric network failure
* osd: re-use partially deleted PG contents when present (Sam Just)
* osd: break blacklisted client watches (David Zafman)
* mon: many stability fixes (Joao Luis)
* mon, osd: many memory leaks fixed
* mds: misc stability fixes (Yan, Zheng, Greg Farnum)
* mds: many backpointer improvements (Yan, Zheng)
* mds: new robust open-by-ino support (Yan, Zheng)
* ceph-fuse, libcephfs: fix a few caps revocation bugs
* librados: new calls to administer the cluster
* librbd: locking tests (Josh Durgin)
* ceph-disk: improved handling of odd device names
* ceph-disk: many fixes for RHEL/CentOS, Fedora, wheezy
* many many fixes from static code analysis (Danny Al-Gaaf)
* daemons: create /var/run/ceph as needed


v0.64
=====

Upgrading
---------

* New pools now have the HASHPSPOOL flag set by default to provide
  better distribution over OSDs.  Support for this feature was
  introduced in v0.59 and Linux kernel version v3.9.  If you wish to
  access the cluster from an older kernel, set the 'osd pool default
  flag hashpspool = false' option in your ceph.conf prior to creating
  the cluster or creating new pools.  Note that the presense of any
  pool in the cluster with the flag enabled will make the OSD require
  support from all clients.

Notable changes
---------------

* osd: monitor both front and back interfaces
* osd: verify both front and back network are working before rejoining cluster
* osd: fix memory/network inefficiency during deep scrub
* osd: fix incorrect mark-down of osds
* mon: fix start fork behavior
* mon: fix election timeout
* mon: better trim/compaction behavior
* mon: fix units in 'ceph df' output
* mon, osd: misc memory leaks
* librbd: make default options/features for newly created images (e.g., via qemu-img) configurable
* mds: many fixes for mds clustering
* mds: fix rare hang after client restart
* ceph-fuse: add ioctl support
* ceph-fuse/libcephfs: fix for cap release/hang
* rgw: handle deep uri resources
* rgw: fix CORS bugs
* ceph-disk: add '[un]suppress-active DEV' command
* debian: rgw: stop daemon on uninstall
* debian: fix upstart behavior with upgrades


v0.63
=====

Upgrading
---------

* The 'osd min down {reporters|reports}' config options have been
  renamed to 'mon osd min down {reporters|reports}', and the
  documentation has been updated to reflect that these options apply
  to the monitors (who process failure reports) and not OSDs.  If you
  have adjusted these settings, please update your ``ceph.conf``
  accordingly.

Notable Changes
---------------

* librbd: parallelize delete, rollback, flatten, copy, resize
* librbd: ability to read from local replicas
* osd: resurrect partially deleted PGs
* osd: prioritize recovery for degraded PGs
* osd: fix internal heartbeart timeouts when scrubbing very large objects
* osd: close narrow journal race
* rgw: fix usage log scanning for large, untrimmed logs
* rgw: fix locking issue, user operation mask,
* initscript: fix osd crush weight calculation when using -a
* initscript: fix enumeration of local daemons
* mon: several fixes to paxos, sync
* mon: new --extract-monmap to aid disaster recovery
* mon: fix leveldb compression, trimming
* add 'config get' admin socket command
* rados: clonedata command for cli
* debian: stop daemons on uninstall; fix dependencies
* debian wheezy: fix udev rules
* many many small fixes from coverity scan


v0.62
=====

Notable Changes
---------------

* mon: fix validation of mds ids from CLI commands
* osd: fix for an op ordering bug
* osd, mon: optionally dump leveldb transactions to a log
* osd: fix handling for split after upgrade from bobtail
* debian, specfile: packaging cleanups
* radosgw-admin: create keys for new users by default
* librados python binding cleanups
* misc code cleanups




v0.61.9 "Cuttlefish"
====================

This point release resolves several low to medium-impact bugs across
the code base, and fixes a performance problem (CPU utilization) with
radosgw.  We recommend that all production cuttlefish users upgrade.

Notable Changes
---------------

* ceph, ceph-authtool: fix help (Danny Al-Gaaf)
* ceph-disk: partprobe after creating journal partition
* ceph-disk: specific fs type when mounting (Alfredo Deza)
* ceph-fuse: fix bug when compiled against old versions
* ceph-fuse: fix use-after-free in caching code (Yan, Zheng)
* ceph-fuse: misc caching bugs
* ceph.spec: remove incorrect mod_fcgi dependency (Gary Lowell)
* crush: fix name caching
* librbd: fix bug when unpausing cluster (Josh Durgin)
* mds: fix LAZYIO lock hang
* mds: fix bug in file size recovery (after client crash)
* mon: fix paxos recovery corner case
* osd: fix exponential backoff for slow request warnings (Loic Dachary)
* osd: fix readdir_r usage
* osd: fix startup for long-stopped OSDs
* rgw: avoid std::list::size() to avoid wasting CPU cycles (Yehuda Sadeh)
* rgw: drain pending requests during write (fixes data safety issue) (Yehuda Sadeh)
* rgw: fix authenticated users group ACL check (Yehuda Sadeh)
* rgw: fix bug in POST (Yehuda Sadeh)
* rgw: fix sysvinit script 'status' command, return value (Danny Al-Gaaf)
* rgw: reduce default log level (Yehuda Sadeh)

For more detailed information, see :download:`the complete changelog <changelog/v0.61.9.txt>`.

v0.61.8 "Cuttlefish"
====================

This release includes a number of important issues, including rare
race conditions in the OSD, a few monitor bugs, and fixes for RBD
flush behavior.  We recommend that production users upgrade at their
convenience.

Notable Changes
---------------

* librados: fix async aio completion wakeup
* librados: fix aio completion locking
* librados: fix rare deadlock during shutdown
* osd: fix race when queueing recovery operations
* osd: fix possible race during recovery
* osd: optionally preload rados classes on startup (disabled by default)
* osd: fix journal replay corner condition
* osd: limit size of peering work queue batch (to speed up peering)
* mon: fix paxos recovery corner case
* mon: fix rare hang when monmap updates during an election
* mon: make 'osd pool mksnap ...' avoid exposing uncommitted state
* mon: make 'osd pool rmsnap ...' not racy, avoid exposing uncommitted state
* mon: fix bug during mon cluster expansion
* rgw: fix crash during multi delete operation
* msgr: fix race conditions during osd network reinitialization
* ceph-disk: apply mount options when remounting

For more detailed information, see :download:`the complete changelog <changelog/v0.61.8.txt>`.


v0.61.7 "Cuttlefish"
====================

This release fixes another regression preventing monitors to start after
undergoing certain upgrade sequences, as well as some corner cases with
Paxos and support for unusual device names in ceph-disk/ceph-deploy.

Notable Changes
---------------

* mon: fix regression in latest full osdmap retrieval
* mon: fix a long-standing bug in a paxos corner case
* ceph-disk: improved support for unusual device names (e.g., /dev/cciss/c0d0)

For more detailed information, see :download:`the complete changelog <changelog/v0.61.7.txt>`.


v0.61.6 "Cuttlefish"
====================

This release fixes a regression in v0.61.5 that could prevent monitors
from restarting.  This affects any cluster that was upgraded from a
previous version of Ceph (and not freshly created with v0.61.5).

All users are strongly recommended to upgrade.

Notable Changes
---------------

* mon: record latest full osdmap
* mon: work around previous bug in which latest full osdmap is not recorded
* mon: avoid scrub while updating

For more detailed information, see :download:`the complete changelog <changelog/v0.61.6.txt>`.


v0.61.5 "Cuttlefish"
====================

This release most improves stability of the monitor and fixes a few
bugs with the ceph-disk utility (used by ceph-deploy).  We recommand
that all v0.61.x users upgrade.

Upgrading
---------

* This release fixes a 32-bit vs 64-bit arithmetic bug with the
  feature bits.  An unfortunate consequence of the fix is that 0.61.4
  (or earlier) ceph-mon daemons can't form a quorum with 0.61.5 (or
  later) monitors.  To avoid the possibility of service disruption, we
  recommend you upgrade all monitors at once.

Notable Changes
---------------

* mon: misc sync improvements (faster, more reliable, better tuning)
* mon: enable leveldb cache by default (big performance improvement)
* mon: new scrub feature (primarily for diagnostic, testing purposes)
* mon: fix occasional leveldb assertion on startup
* mon: prevent reads until initial state is committed
* mon: improved logic for trimming old osdmaps
* mon: fix pick_addresses bug when expanding mon cluster
* mon: several small paxos fixes, improvements
* mon: fix bug osdmap trim behavior
* osd: fix several bugs with PG stat reporting
* osd: limit number of maps shared with peers (which could cause domino failures)
* rgw: fix radosgw-admin buckets list (for all buckets)
* mds: fix occasional client failure to reconnect
* mds: fix bad list traversal after unlink
* mds: fix underwater dentry cleanup (occasional crash after mds restart)
* libcephfs, ceph-fuse: fix occasional hangs on umount
* libcephfs, ceph-fuse: fix old bug with O_LAZY vs O_NOATIME confusion
* ceph-disk: more robust journal device detection on RHEL/CentOS
* ceph-disk: better, simpler locking
* ceph-disk: do not inadvertantely mount over existing osd mounts
* ceph-disk: better handling for unusual device names
* sysvinit, upstart: handle symlinks in /var/lib/ceph/*

For more detailed information, see :download:`the complete changelog <changelog/v0.61.5.txt>`.


v0.61.4 "Cuttlefish"
====================

This release resolves a possible data corruption on power-cycle when
using XFS, a few outstanding problems with monitor sync, several
problems with ceph-disk and ceph-deploy operation, and a problem with
OSD memory usage during scrub.

Upgrading
---------

* No issues.

Notable Changes
---------------

* mon: fix daemon exit behavior when error is encountered on startup
* mon: more robust sync behavior
* osd: do not use sync_file_range(2), posix_fadvise(...DONTNEED) (can cause data corruption on power loss on XFS)
* osd: avoid unnecessary log rewrite (improves peering speed)
* osd: fix scrub efficiency bug (problematic on old clusters)
* rgw: fix listing objects that start with underscore
* rgw: fix deep URI resource, CORS bugs
* librados python binding: fix truncate on 32-bit architectures
* ceph-disk: fix udev rules
* rpm: install sysvinit script on package install
* ceph-disk: fix OSD start on machine reboot on Debian wheezy
* ceph-disk: activate OSD when journal device appears second
* ceph-disk: fix various bugs on RHEL/CentOS 6.3
* ceph-disk: add 'zap' command
* ceph-disk: add '[un]suppress-activate' command for preparing spare disks
* upstart: start on runlevel [2345] (instead of after the first network interface starts)
* ceph-fuse, libcephfs: handle mds session reset during session open
* ceph-fuse, libcephfs: fix two capability revocation bugs
* ceph-fuse: fix thread creation on startup
* all daemons: create /var/run/ceph directory on startup if missing

For more detailed information, see :download:`the complete changelog <changelog/v0.61.4.txt>`.


v0.61.3 "Cuttlefish"
====================

This release resolves a number of problems with the monitors and leveldb that users have
been seeing.  Please upgrade.

Upgrading
---------

* There is one known problem with mon upgrades from bobtail.  If the
  ceph-mon conversion on startup is aborted or fails for some reason, we
  do not correctly error out, but instead continue with (in certain cases)
  odd results.  Please be careful if you have to restart the mons during
  the upgrade.  A 0.61.4 release with a fix will be out shortly.

* In the meantime, for current cuttlefish users, v0.61.3 is safe to use.


Notable Changes
---------------

* mon: paxos state trimming fix (resolves runaway disk usage)
* mon: finer-grained compaction on trim
* mon: discard messages from disconnected clients (lowers load)
* mon: leveldb compaction and other stats available via admin socket
* mon: async compaction (lower overhead)
* mon: fix bug incorrectly marking osds down with insufficient failure reports
* osd: fixed small bug in pg request map
* osd: avoid rewriting pg info on every osdmap
* osd: avoid internal heartbeta timeouts when scrubbing very large objects
* osd: fix narrow race with journal replay
* mon: fixed narrow pg split race
* rgw: fix leaked space when copying object
* rgw: fix iteration over large/untrimmed usage logs
* rgw: fix locking issue with ops log socket
* rgw: require matching version of librados
* librbd: make image creation defaults configurable (e.g., create format 2 images via qemu-img)
* fix units in 'ceph df' output
* debian: fix prerm/postinst hooks to start/stop daemons appropriately
* upstart: allow uppercase daemons names (and thus hostnames)
* sysvinit: fix enumeration of local daemons by type
* sysvinit: fix osd weight calcuation when using -a
* fix build on unsigned char platforms (e.g., arm)

For more detailed information, see :download:`the complete changelog <changelog/v0.61.3.txt>`.


v0.61.2 "Cuttlefish"
====================

This release disables a monitor debug log that consumes disk space and
fixes a bug when upgrade some monitors from bobtail to cuttlefish.

Notable Changes
---------------

* mon: fix conversion of stores with duplicated GV values
* mon: disable 'mon debug dump transactions' by default

For more detailed information, see :download:`the complete changelog <changelog/v0.61.2.txt>`.


v0.61.1 "Cuttlefish"
====================

This release fixes a problem when upgrading a bobtail cluster that had
snapshots to cuttlefish.

Notable Changes
---------------

* osd: handle upgrade when legacy snap collections are present; repair from previous failed restart
* ceph-create-keys: fix race with ceph-mon startup (which broke 'ceph-deploy gatherkeys ...')
* ceph-create-keys: gracefully handle bad response from ceph-osd
* sysvinit: do not assume default osd_data when automatically weighting OSD
* osd: avoid crash from ill-behaved classes using getomapvals
* debian: fix squeeze dependency
* mon: debug options to log or dump leveldb transactions

For more detailed information, see :download:`the complete changelog <changelog/v0.61.1.txt>`.

v0.61 "Cuttlefish"
==================

Upgrading from v0.60
--------------------

* The ceph-deploy tool is now the preferred method of provisioning
  new clusters.  For existing clusters created via mkcephfs that
  would like to transition to the new tool, there is a migration
  path, documented at `Transitioning to ceph-deploy`_.


* The sysvinit script (/etc/init.d/ceph) will now verify (and, if
  necessary, update) the OSD's position in the CRUSH map on startup.
  (The upstart script has always worked this way.) By default, this
  ensures that the OSD is under a 'host' with a name that matches the
  hostname (``hostname -s``).  Legacy clusters create with mkcephfs do
  this by default, so this should not cause any problems, but legacy
  clusters with customized CRUSH maps with an alternate structure
  should set ``osd crush update on start = false``.

* radosgw-admin now uses the term zone instead of cluster to describe
  each instance of the radosgw data store (and corresponding
  collection of radosgw daemons).  The usage for the radosgw-admin
  command and the 'rgw zone root pool' config options have changed
  accordingly.

* rbd progress indicators now go to standard error instead of standard
  out.  (You can disable progress with --no-progress.)

* The 'rbd resize ...' command now requires the --allow-shrink option
  when resizing to a smaller size.  Expanding images to a larger size
  is unchanged.

* Please review the changes going back to 0.56.4 if you are upgrading
  all the way from bobtail.

* The old 'ceph stop_cluster' command has been removed.

* The sysvinit script now uses the ceph.conf file on the remote host
  when starting remote daemons via the '-a' option.  Note that if '-a'
  is used in conjunction with '-c path', the path must also be present
  on the remote host (it is not copied to a temporary file, as it was
  previously).


Upgrading from v0.56.4 "Bobtail"
--------------------------------

Please see `Upgrading from Bobtail to Cuttlefish`_ for details.

.. _Upgrading from Bobtail to Cuttlefish: ../install/upgrading-ceph/#upgrading-from-bobtail-to-cuttlefish

* The ceph-deploy tool is now the preferred method of provisioning
  new clusters.  For existing clusters created via mkcephfs that
  would like to transition to the new tool, there is a migration
  path, documented at `Transitioning to ceph-deploy`_.

.. _Transitioning to ceph-deploy: ../rados/deployment/ceph-deploy-transition

* The sysvinit script (/etc/init.d/ceph) will now verify (and, if
  necessary, update) the OSD's position in the CRUSH map on startup.
  (The upstart script has always worked this way.) By default, this
  ensures that the OSD is under a 'host' with a name that matches the
  hostname (``hostname -s``).  Legacy clusters create with mkcephfs do
  this by default, so this should not cause any problems, but legacy
  clusters with customized CRUSH maps with an alternate structure
  should set ``osd crush update on start = false``.

* radosgw-admin now uses the term zone instead of cluster to describe
  each instance of the radosgw data store (and corresponding
  collection of radosgw daemons).  The usage for the radosgw-admin
  command and the 'rgw zone root pool' config optoins have changed
  accordingly.

* rbd progress indicators now go to standard error instead of standard
  out.  (You can disable progress with --no-progress.)

* The 'rbd resize ...' command now requires the --allow-shrink option
  when resizing to a smaller size.  Expanding images to a larger size
  is unchanged.

* Please review the changes going back to 0.56.4 if you are upgrading
  all the way from bobtail.

* The old 'ceph stop_cluster' command has been removed.

* The sysvinit script now uses the ceph.conf file on the remote host
  when starting remote daemons via the '-a' option.  Note that if '-a'
  is used in conjuction with '-c path', the path must also be present
  on the remote host (it is not copied to a temporary file, as it was
  previously).

* The monitor is using a completely new storage strategy and
  intra-cluster protocol.  This means that cuttlefish and bobtail
  monitors do not talk to each other.  When you upgrade each one, it
  will convert its local data store to the new format.  Once you
  upgrade a majority, the quorum will be formed using the new protocol
  and the old monitors will be blocked out until they too get
  upgraded.  For this reason, we recommend not running a mixed-version
  cluster for very long.

* ceph-mon now requires the creation of its data directory prior to
  --mkfs, similarly to what happens on ceph-osd.  This directory is no
  longer automatically created, and custom scripts should be adjusted to
  reflect just that.

* The monitor now enforces that MDS names be unique.  If you have
  multiple daemons start with with the same id (e.g., ``mds.a``) the
  second one will implicitly mark the first as failed.  This makes
  things less confusing and makes a daemon restart faster (we no
  longer wait for the stopped daemon to time out) but existing
  multi-mds configurations may need to be adjusted accordingly to give
  daemons unique names.

* The 'ceph osd pool delete <poolname>' and 'rados rmpool <poolname>'
  now have safety interlocks with loud warnings that make you confirm
  pool removal.  Any scripts curenty rely on these functions zapping
  data without confirmation need to be adjusted accordingly.


Notable Changes from v0.60
--------------------------

* rbd: incremental backups
* rbd: only set STRIPINGV2 feature if striping parameters are incompatible with old versions
* rbd: require --allow-shrink for resizing images down
* librbd: many bug fixes
* rgw: management REST API
* rgw: fix object corruption on COPY to self
* rgw: new sysvinit script for rpm-based systems
* rgw: allow buckets with '_'
* rgw: CORS support
* mon: many fixes
* mon: improved trimming behavior
* mon: fix data conversion/upgrade problem (from bobtail)
* mon: ability to tune leveldb
* mon: config-keys service to store arbitrary data on monitor
* mon: 'osd crush add|link|unlink|add-bucket ...' commands
* mon: trigger leveldb compaction on trim
* osd: per-rados pool quotas (objects, bytes)
* osd: tool to export, import, and delete PGs from an individual OSD data store
* osd: notify mon on clean shutdown to avoid IO stall
* osd: improved detection of corrupted journals
* osd: ability to tune leveldb
* osd: improve client request throttling
* osd, librados: fixes to the LIST_SNAPS operation
* osd: improvements to scrub error repair
* osd: better prevention of wedging OSDs with ENOSPC 
* osd: many small fixes
* mds: fix xattr handling on root inode
* mds: fixed bugs in journal replay
* mds: many fixes
* librados: clean up snapshot constant definitions
* libcephfs: calls to query CRUSH topology (used by Hadoop)
* ceph-fuse, libcephfs: misc fixes to mds session management
* ceph-fuse: disabled cache invalidation (again) due to potential deadlock with kernel
* sysvinit: try to start all daemons despite early failures
* ceph-disk: new 'list' command
* ceph-disk: hotplug fixes for RHEL/CentOS
* ceph-disk: fix creation of OSD data partitions on >2TB disks
* osd: fix udev rules for RHEL/CentOS systems
* fix daemon logging during initial startup

Notable changes from v0.56 "Bobtail"
------------------------------------
* always use installed system leveldb (Gary Lowell)
* auth: ability to require new cephx signatures on messages (still off by default)
* buffer unit testing (Loic Dachary)
* ceph tool: some CLI interface cleanups
* ceph-disk: improve multicluster support, error handling (Sage Weil)
* ceph-disk: support for dm-crypt (Alexandre Marangone)
* ceph-disk: support for sysvinit, directories or partitions (not full disks)
* ceph-disk: fix mkfs args on old distros (Alexandre Marangone)
* ceph-disk: fix creation of OSD data partitions on >2TB disks
* ceph-disk: hotplug fixes for RHEL/CentOS
* ceph-disk: new 'list' command
* ceph-fuse, libcephfs: misc fixes to mds session management
* ceph-fuse: disabled cache invalidation (again) due to potential deadlock with kernel
* ceph-fuse: enable kernel cache invalidation (Sam Lang)
* ceph-fuse: fix statfs(2) reporting
* ceph-fuse: session handling cleanup, bug fixes (Sage Weil)
* crush: ability to create, remove rules via CLI
* crush: update weights for all instances of an item, not just the first (Sage Weil)
* fix daemon logging during initial startup
* fixed log rotation (Gary Lowell)
* init-ceph, mkcephfs: close a few security holes with -a  (Sage Weil)
* libcephfs: calls to query CRUSH topology (used by Hadoop)
* libcephfs: many fixes, cleanups with the Java bindings
* libcephfs: new topo API requests for Hadoop (Noah Watkins)
* librados: clean up snapshot constant definitions
* librados: fix linger bugs (Josh Durgin)
* librbd: fixed flatten deadlock (Josh Durgin)
* librbd: fixed some locking issues with flatten (Josh Durgin)
* librbd: many bug fixes
* librbd: optionally wait for flush before enabling writeback (Josh Durgin)
* many many cleanups (Danny Al-Gaaf)
* mds, ceph-fuse: fix bugs with replayed requests after MDS restart (Sage Weil)
* mds, ceph-fuse: manage layouts via xattrs
* mds: allow xattrs on root
* mds: fast failover between MDSs (enforce unique mds names)
* mds: fix xattr handling on root inode
* mds: fixed bugs in journal replay
* mds: improve session cleanup (Sage Weil)
* mds: many fixes (Yan Zheng)
* mds: misc bug fixes with clustered MDSs and failure recovery
* mds: misc bug fixes with readdir
* mds: new encoding for all data types (to allow forward/backward compatbility) (Greg Farnum)
* mds: store and update backpointers/traces on directory, file objects (Sam Lang)
* mon: 'osd crush add|link|unlink|add-bucket ...' commands
* mon: ability to tune leveldb
* mon: approximate recovery, IO workload stats
* mon: avoid marking entire CRUSH subtrees out (e.g., if an entire rack goes offline)
* mon: config-keys service to store arbitrary data on monitor
* mon: easy adjustment of crush tunables via 'ceph osd crush tunables ...'
* mon: easy creation of crush rules vai 'ceph osd rule ...'
* mon: fix data conversion/upgrade problem (from bobtail)
* mon: improved trimming behavior
* mon: many fixes
* mon: new 'ceph df [detail]' command
* mon: new checks for identifying and reporting clock drift
* mon: rearchitected to utilize single instance of paxos and a key/value store (Joao Luis)
* mon: safety check for pool deletion
* mon: shut down safely if disk approaches full (Joao Luis)
* mon: trigger leveldb compaction on trim
* msgr: fix comparison of IPv6 addresses (fixes monitor bringup via ceph-deploy, chef)
* msgr: fixed race in connection reset
* msgr: optionally tune TCP buffer size to avoid throughput collapse (Jim Schutt)
* much code cleanup and optimization (Danny Al-Gaaf)
* osd, librados: ability to list watchers (David Zafman)
* osd, librados: fixes to the LIST_SNAPS operation
* osd, librados: new listsnaps command (David Zafman)
* osd: a few journaling bug fixes
* osd: ability to tune leveldb
* osd: add 'noscrub', 'nodeepscrub' osdmap flags (David Zafman)
* osd: better prevention of wedging OSDs with ENOSPC
* osd: ceph-filestore-dump tool for debugging
* osd: connection handling bug fixes
* osd: deep-scrub omap keys/values
* osd: default to libaio for the journal (some performance boost)
* osd: fix hang in 'journal aio = true' mode (Sage Weil)
* osd: fix pg log trimming (avoids memory bloat on degraded clusters)
* osd: fix udev rules for RHEL/CentOS systems
* osd: fixed bug in journal checksums (Sam Just)
* osd: improved client request throttling
* osd: improved handling when disk fills up (David Zafman)
* osd: improved journal corruption detection (Sam Just)
* osd: improved detection of corrupted journals
* osd: improvements to scrub error repair
* osd: make tracking of object snapshot metadata more efficient (Sam Just)
* osd: many small fixes
* osd: misc fixes to PG split (Sam Just)
* osd: move pg info, log into leveldb (== better performance) (David Zafman)
* osd: notify mon on clean shutdown to avoid IO stall
* osd: per-rados pool quotas (objects, bytes)
* osd: refactored watch/notify infrastructure (fixes protocol, removes many bugs) (Sam Just)
* osd: support for improved hashing of PGs across OSDs via HASHPSPOOL pool flag and feature
* osd: tool to export, import, and delete PGs from an individual OSD data store
* osd: trim log more aggressively, avoid appearance of leak memory
* osd: validate snap collections on startup
* osd: verify snap collections on startup (Sam Just)
* radosgw: ACL grants in headers (Caleb Miles)
* radosgw: ability to listen to fastcgi via a port (Guilhem Lettron)
* radosgw: fix object copy onto self (Yehuda Sadeh)
* radosgw: misc fixes
* rbd-fuse: new tool, package
* rbd: avoid FIEMAP when importing from file (it can be buggy)
* rbd: incremental backups
* rbd: only set STRIPINGV2 feature if striping parameters are incompatible with old versions
* rbd: require --allow-shrink for resizing images down
* rbd: udevadm settle on map/unmap to avoid various races (Dan Mick)
* rbd: wait for udev to settle in strategic places (avoid spurious errors, failures)
* rgw: CORS support
* rgw: allow buckets with '_'
* rgw: fix Content-Length on 32-bit machines (Jan Harkes)
* rgw: fix log rotation
* rgw: fix object corruption on COPY to self
* rgw: fixed >4MB range requests (Jan Harkes)
* rgw: new sysvinit script for rpm-based systems
* rpm/deb: do not remove /var/lib/ceph on purge (v0.59 was the only release to do so)
* sysvinit: try to start all daemons despite early failures
* upstart: automatically set osd weight based on df (Guilhem Lettron)
* use less memory for logging by default


v0.60
=====

Upgrading
---------

* Please note that the recently added librados 'list_snaps' function
  call is in a state of flux and is changing slightly in v0.61.  You
  are advised not to make use of it in v0.59 or v0.60.

Notable Changes
---------------

* osd: make tracking of object snapshot metadata more efficient (Sam Just)
* osd: misc fixes to PG split (Sam Just)
* osd: improve journal corruption detection (Sam Just)
* osd: improve handling when disk fills up (David Zafman)
* osd: add 'noscrub', 'nodeepscrub' osdmap flags (David Zafman)
* osd: fix hang in 'journal aio = true' mode (Sage Weil)
* ceph-disk-prepare: fix mkfs args on old distros (Alexandre Marangone)
* ceph-disk-activate: improve multicluster support, error handling (Sage Weil)
* librbd: optionally wait for flush before enabling writeback (Josh Durgin)
* crush: update weights for all instances of an item, not just the first (Sage Weil)
* mon: shut down safely if disk approaches full (Joao Luis)
* rgw: fix Content-Length on 32-bit machines (Jan Harkes)
* mds: store and update backpointers/traces on directory, file objects (Sam Lang)
* mds: improve session cleanup (Sage Weil)
* mds, ceph-fuse: fix bugs with replayed requests after MDS restart (Sage Weil)
* ceph-fuse: enable kernel cache invalidation (Sam Lang)
* libcephfs: new topo API requests for Hadoop (Noah Watkins)
* ceph-fuse: session handling cleanup, bug fixes (Sage Weil)
* much code cleanup and optimization (Danny Al-Gaaf)
* use less memory for logging by default
* upstart: automatically set osd weight based on df (Guilhem Lettron)
* init-ceph, mkcephfs: close a few security holes with -a  (Sage Weil)
* rpm/deb: do not remove /var/lib/ceph on purge (v0.59 was the only release to do so)


v0.59
=====

Upgrading
---------

* The monitor is using a completely new storage strategy and
  intra-cluster protocol.  This means that v0.59 and pre-v0.59
  monitors do not talk to each other.  When you upgrade each one, it
  will convert its local data store to the new format.  Once you
  upgrade a majority, the quorum will be formed using the new protocol
  and the old monitors will be blocked out until they too get
  upgraded.  For this reason, we recommend not running a mixed-version
  cluster for very long.

* ceph-mon now requires the creation of its data directory prior to
  --mkfs, similarly to what happens on ceph-osd.  This directory is no
  longer automatically created, and custom scripts should be adjusted to
  reflect just that.


Notable Changes
---------------

 * mon: rearchitected to utilize single instance of paxos and a key/value store (Joao Luis)
 * mon: new 'ceph df [detail]' command
 * osd: support for improved hashing of PGs across OSDs via HASHPSPOOL pool flag and feature
 * osd: refactored watch/notify infrastructure (fixes protocol, removes many bugs) (Sam Just) 
 * osd, librados: ability to list watchers (David Zafman)
 * osd, librados: new listsnaps command (David Zafman)
 * osd: trim log more aggressively, avoid appearance of leak memory
 * osd: misc split fixes
 * osd: a few journaling bug fixes
 * osd: connection handling bug fixes
 * rbd: avoid FIEMAP when importing from file (it can be buggy)
 * librados: fix linger bugs (Josh Durgin)
 * librbd: fixed flatten deadlock (Josh Durgin)
 * rgw: fixed >4MB range requests (Jan Harkes)
 * rgw: fix log rotation
 * mds: allow xattrs on root
 * ceph-fuse: fix statfs(2) reporting
 * msgr: optionally tune TCP buffer size to avoid throughput collapse (Jim Schutt)
 * consume less memory for logging by default
 * always use system leveldb (Gary Lowell)



v0.58
=====

Upgrading
---------

* The monitor now enforces that MDS names be unique.  If you have
  multiple daemons start with with the same id (e.g., ``mds.a``) the
  second one will implicitly mark the first as failed.  This makes
  things less confusing and makes a daemon restart faster (we no
  longer wait for the stopped daemon to time out) but existing
  multi-mds configurations may need to be adjusted accordingly to give
  daemons unique names.

Notable Changes
---------------

 * librbd: fixed some locking issues with flatten (Josh Durgin)
 * rbd: udevadm settle on map/unmap to avoid various races (Dan Mick)
 * osd: move pg info, log into leveldb (== better performance) (David Zafman)
 * osd: fix pg log trimming (avoids memory bloat on degraded clusters)
 * osd: fixed bug in journal checksums (Sam Just)
 * osd: verify snap collections on startup (Sam Just)
 * ceph-disk-prepare/activate: support for dm-crypt (Alexandre Marangone)
 * ceph-disk-prepare/activate: support for sysvinit, directories or partitions (not full disks)
 * msgr: fixed race in connection reset
 * msgr: fix comparison of IPv6 addresses (fixes monitor bringup via ceph-deploy, chef)
 * radosgw: fix object copy onto self (Yehuda Sadeh)
 * radosgw: ACL grants in headers (Caleb Miles)
 * radosgw: ability to listen to fastcgi via a port (Guilhem Lettron)
 * mds: new encoding for all data types (to allow forward/backward compatbility) (Greg Farnum)
 * mds: fast failover between MDSs (enforce unique mds names)
 * crush: ability to create, remove rules via CLI
 * many many cleanups (Danny Al-Gaaf)
 * buffer unit testing (Loic Dachary)
 * fixed log rotation (Gary Lowell)

v0.57
=====

This development release has a lot of additional functionality
accumulated over the last couple months.  Most of the bug fixes (with
the notable exception of the MDS related work) has already been
backported to v0.56.x, and is not mentioned here.

Upgrading
---------

* The 'ceph osd pool delete <poolname>' and 'rados rmpool <poolname>'
  now have safety interlocks with loud warnings that make you confirm
  pool removal.  Any scripts curenty rely on these functions zapping
  data without confirmation need to be adjusted accordingly.

Notable Changes
---------------

* osd: default to libaio for the journal (some performance boost)
* osd: validate snap collections on startup
* osd: ceph-filestore-dump tool for debugging
* osd: deep-scrub omap keys/values
* ceph tool: some CLI interface cleanups
* mon: easy adjustment of crush tunables via 'ceph osd crush tunables ...'
* mon: easy creation of crush rules vai 'ceph osd rule ...'
* mon: approximate recovery, IO workload stats
* mon: avoid marking entire CRUSH subtrees out (e.g., if an entire rack goes offline)
* mon: safety check for pool deletion
* mon: new checks for identifying and reporting clock drift
* radosgw: misc fixes
* rbd: wait for udev to settle in strategic places (avoid spurious errors, failures)
* rbd-fuse: new tool, package
* mds, ceph-fuse: manage layouts via xattrs
* mds: misc bug fixes with clustered MDSs and failure recovery
* mds: misc bug fixes with readdir
* libcephfs: many fixes, cleanups with the Java bindings
* auth: ability to require new cephx signatures on messages (still off by default)



v0.56.7 "bobtail"
=================

This bobtail update fixes a range of radosgw bugs (including an easily
triggered crash from multi-delete), a possible data corruption issue
with power failure on XFS, and several OSD problems, including a
memory "leak" that will affect aged clusters.

Notable changes
---------------

* ceph-fuse: create finisher flags after fork()
* debian: fix prerm/postinst hooks; do not restart daemons on upgrade
* librados: fix async aio completion wakeup (manifests as rbd hang)
* librados: fix hang when osd becomes full and then not full
* librados: fix locking for aio completion refcounting
* librbd python bindings: fix stripe_unit, stripe_count
* librbd: make image creation default configurable
* mon: fix validation of mds ids in mon commands
* osd: avoid excessive disk updates during peering
* osd: avoid excessive memory usage on scrub
* osd: avoid heartbeat failure/suicide when scrubbing
* osd: misc minor bug fixes
* osd: use fdatasync instead of sync_file_range (may avoid xfs power-loss corruption)
* rgw: escape prefix correctly when listing objects
* rgw: fix copy attrs
* rgw: fix crash on multi delete
* rgw: fix locking/crash when using ops log socket
* rgw: fix usage logging
* rgw: handle deep uri resources

For more detailed information, see :download:`the complete changelog <changelog/v0.56.7.txt>`.


v0.56.6 "bobtail"
=================

Notable changes
---------------

* rgw: fix garbage collection
* rpm: fix package dependencies

For more detailed information, see :download:`the complete changelog <changelog/v0.56.6.txt>`.


v0.56.5 "bobtail"
=================

Upgrading
---------

* ceph-disk[-prepare,-activate] behavior has changed in various ways.
  There should not be any compatibility issues, but chef users should
  be aware.

Notable changes
---------------

* mon: fix recording of quorum feature set (important for argonaut -> bobtail -> cuttlefish mon upgrades)
* osd: minor peering bug fixes
* osd: fix a few bugs when pools are renamed
* osd: fix occasionally corrupted pg stats
* osd: fix behavior when broken v0.56[.0] clients connect
* rbd: avoid FIEMAP ioctl on import (it is broken on some kernels)
* librbd: fixes for several request/reply ordering bugs
* librbd: only set STRIPINGV2 feature on new images when needed
* librbd: new async flush method to resolve qemu hangs (requires Qemu update as well)
* librbd: a few fixes to flatten
* ceph-disk: support for dm-crypt
* ceph-disk: many backports to allow bobtail deployments with ceph-deploy, chef
* sysvinit: do not stop starting daemons on first failure
* udev: fixed rules for redhat-based distros
* build fixes for raring

For more detailed information, see :download:`the complete changelog <changelog/v0.56.5.txt>`.

v0.56.4 "bobtail"
=================

Upgrading
---------

* There is a fix in the syntax for the output of 'ceph osd tree --format=json'.

* The MDS disk format has changed from prior releases *and* from v0.57.  In particular,
  upgrades to v0.56.4 are safe, but you cannot move from v0.56.4 to v0.57 if you are using
  the MDS for CephFS; you must upgrade directly to v0.58 (or later) instead.

Notable changes
---------------

* mon: fix bug in bringup with IPv6
* reduce default memory utilization by internal logging (all daemons)
* rgw: fix for bucket removal
* rgw: reopen logs after log rotation
* rgw: fix multipat upload listing
* rgw: don't copy object when copied onto self
* osd: fix caps parsing for pools with - or _
* osd: allow pg log trimming when degraded, scrubbing, recoverying (reducing memory consumption)
* osd: fix potential deadlock when 'journal aio = true'
* osd: various fixes for collection creation/removal, rename, temp collections
* osd: various fixes for PG split
* osd: deep-scrub omap key/value data
* osd: fix rare bug in journal replay
* osd: misc fixes for snapshot tracking
* osd: fix leak in recovery reservations on pool deletion
* osd: fix bug in connection management
* osd: fix for op ordering when rebalancing
* ceph-fuse: report file system size with correct units
* mds: get and set directory layout policies via virtual xattrs
* mds: on-disk format revision (see upgrading note above)
* mkcephfs, init-ceph: close potential security issues with predictable filenames

For more detailed information, see :download:`the complete changelog <changelog/v0.56.4.txt>`.

v0.56.3 "bobtail"
=================

This release has several bug fixes surrounding OSD stability.  Most
significantly, an issue with OSDs being unresponsive shortly after
startup (and occasionally crashing due to an internal heartbeat check)
is resolved.  Please upgrade.

Upgrading
---------

* A bug was fixed in which the OSDMap epoch for PGs without any IO
  requests was not recorded.  If there are pools in the cluster that
  are completely idle (for example, the ``data`` and ``metadata``
  pools normally used by CephFS), and a large number of OSDMap epochs
  have elapsed since the ``ceph-osd`` daemon was last restarted, those
  maps will get reprocessed when the daemon restarts.  This process
  can take a while if there are a lot of maps.  A workaround is to
  'touch' any idle pools with IO prior to restarting the daemons after
  packages are upgraded::

   rados bench 10 write -t 1 -b 4096 -p {POOLNAME}

  This will typically generate enough IO to touch every PG in the pool
  without generating significant cluster load, and also cleans up any
  temporary objects it creates.

Notable changes
---------------

* osd: flush peering work queue prior to start
* osd: persist osdmap epoch for idle PGs
* osd: fix and simplify connection handling for heartbeats
* osd: avoid crash on invalid admin command
* mon: fix rare races with monitor elections and commands
* mon: enforce that OSD reweights be between 0 and 1 (NOTE: not CRUSH weights)
* mon: approximate client, recovery bandwidth logging
* radosgw: fixed some XML formatting to conform to Swift API inconsistency
* radosgw: fix usage accounting bug; add repair tool
* radosgw: make fallback URI configurable (necessary on some web servers)
* librbd: fix handling for interrupted 'unprotect' operations
* mds, ceph-fuse: allow file and directory layouts to be modified via virtual xattrs

For more detailed information, see :download:`the complete changelog <changelog/v0.56.3.txt>`.


v0.56.2 "bobtail"
=================

This release has a wide range of bug fixes, stability improvements, and some performance improvements.  Please upgrade.

Upgrading
---------

* The meaning of the 'osd scrub min interval' and 'osd scrub max
  interval' has changed slightly.  The min interval used to be
  meaningless, while the max interval would only trigger a scrub if
  the load was sufficiently low.  Now, the min interval option works
  the way the old max interval did (it will trigger a scrub after this
  amount of time if the load is low), while the max interval will
  force a scrub regardless of load.  The default options have been
  adjusted accordingly.  If you have customized these in ceph.conf,
  please review their values when upgrading.

* CRUSH maps that are generated by default when calling ``ceph-mon
  --mkfs`` directly now distribute replicas across hosts instead of
  across OSDs.  Any provisioning tools that are being used by Ceph may
  be affected, although probably for the better, as distributing across
  hosts is a much more commonly sought behavior.  If you use
  ``mkcephfs`` to create the cluster, the default CRUSH rule is still
  inferred by the number of hosts and/or racks in the initial ceph.conf.

Notable changes
---------------

* osd: snapshot trimming fixes
* osd: scrub snapshot metadata
* osd: fix osdmap trimming
* osd: misc peering fixes
* osd: stop heartbeating with peers if internal threads are stuck/hung
* osd: PG removal is friendlier to other workloads
* osd: fix recovery start delay (was causing very slow recovery)
* osd: fix scheduling of explicitly requested scrubs
* osd: fix scrub interval config options
* osd: improve recovery vs client io tuning
* osd: improve 'slow request' warning detail for better diagnosis
* osd: default CRUSH map now distributes across hosts, not OSDs
* osd: fix crash on 32-bit hosts triggered by librbd clients
* librbd: fix error handling when talking to older OSDs
* mon: fix a few rare crashes
* ceph command: ability to easily adjust CRUSH tunables
* radosgw: object copy does not copy source ACLs
* rados command: fix omap command usage
* sysvinit script: set ulimit -n properly on remote hosts
* msgr: fix narrow race with message queuing
* fixed compilation on some old distros (e.g., RHEL 5.x)

For more detailed information, see :download:`the complete changelog <changelog/v0.56.2.txt>`.


v0.56.1 "bobtail"
=================

This release has two critical fixes.  Please upgrade.

Upgrading
---------

* There is a protocol compatibility problem between v0.56 and any
  other version that is now fixed.  If your radosgw or RBD clients are
  running v0.56, they will need to be upgraded too.  If they are
  running a version prior to v0.56, they can be left as is.

Notable changes
---------------
* osd: fix commit sequence for XFS, ext4 (or any other non-btrfs) to prevent data loss on power cycle or kernel panic
* osd: fix compatibility for CALL operation
* osd: process old osdmaps prior to joining cluster (fixes slow startup)
* osd: fix a couple of recovery-related crashes
* osd: fix large io requests when journal is in (non-default) aio mode
* log: fix possible deadlock in logging code

For more detailed information, see :download:`the complete changelog <changelog/v0.56.1.txt>`.

v0.56 "bobtail"
===============

Bobtail is the second stable release of Ceph, named in honor of the
`Bobtail Squid`: http://en.wikipedia.org/wiki/Bobtail_squid.

Key features since v0.48 "argonaut"
-----------------------------------

* Object Storage Daemon (OSD): improved threading, small-io performance, and performance during recovery
* Object Storage Daemon (OSD): regular "deep" scrubbing of all stored data to detect latent disk errors
* RADOS Block Device (RBD): support for copy-on-write clones of images.
* RADOS Block Device (RBD): better client-side caching.
* RADOS Block Device (RBD): advisory image locking
* Rados Gateway (RGW): support for efficient usage logging/scraping (for billing purposes)
* Rados Gateway (RGW): expanded S3 and Swift API coverage (e.g., POST, multi-object delete)
* Rados Gateway (RGW): improved striping for large objects
* Rados Gateway (RGW): OpenStack Keystone integration
* RPM packages for Fedora, RHEL/CentOS, OpenSUSE, and SLES
* mkcephfs: support for automatically formatting and mounting XFS and ext4 (in addition to btrfs)

Upgrading
---------

Please refer to the document `Upgrading from Argonaut to Bobtail`_ for details.

.. _Upgrading from Argonaut to Bobtail: ../install/upgrading-ceph/#upgrading-from-argonaut-to-bobtail

* Cephx authentication is now enabled by default (since v0.55).
  Upgrading a cluster without adjusting the Ceph configuration will
  likely prevent the system from starting up on its own.  We recommend
  first modifying the configuration to indicate that authentication is
  disabled, and only then upgrading to the latest version.::

     auth client required = none
     auth service required = none
     auth cluster required = none

* Ceph daemons can be upgraded one-by-one while the cluster is online
  and in service.

* The ``ceph-osd`` daemons must be upgraded and restarted *before* any
  ``radosgw`` daemons are restarted, as they depend on some new
  ceph-osd functionality.  (The ``ceph-mon``, ``ceph-osd``, and
  ``ceph-mds`` daemons can be upgraded and restarted in any order.)

* Once each individual daemon has been upgraded and restarted, it
  cannot be downgraded.

* The cluster of ``ceph-mon`` daemons will migrate to a new internal
  on-wire protocol once all daemons in the quorum have been upgraded.
  Upgrading only a majority of the nodes (e.g., two out of three) may
  expose the cluster to a situation where a single additional failure
  may compromise availability (because the non-upgraded daemon cannot
  participate in the new protocol).  We recommend not waiting for an
  extended period of time between ``ceph-mon`` upgrades.

* The ops log and usage log for radosgw are now off by default.  If
  you need these logs (e.g., for billing purposes), you must enable
  them explicitly.  For logging of all operations to objects in the
  ``.log`` pool (see ``radosgw-admin log ...``)::

    rgw enable ops log = true

  For usage logging of aggregated bandwidth usage (see ``radosgw-admin
  usage ...``)::

    rgw enable usage log = true

* You should not create or use "format 2" RBD images until after all
  ``ceph-osd`` daemons have been upgraded.  Note that "format 1" is
  still the default.  You can use the new ``ceph osd ls`` and
  ``ceph tell osd.N version`` commands to doublecheck your cluster.
  ``ceph osd ls`` will give a list of all OSD IDs that are part of the
  cluster, and you can use that to write a simple shell loop to display
  all the OSD version strings: ::

      for i in $(ceph osd ls); do
          ceph tell osd.${i} version
      done


Compatibility changes
---------------------

* The 'ceph osd create [<uuid>]' command now rejects an argument that
  is not a UUID.  (Previously it would take take an optional integer
  OSD id.)  This correct syntax has been 'ceph osd create [<uuid>]'
  since v0.47, but the older calling convention was being silently
  ignored.

* The CRUSH map root nodes now have type ``root`` instead of type
  ``pool``.  This avoids confusion with RADOS pools, which are not
  directly related.  Any scripts or tools that use the ``ceph osd
  crush ...`` commands may need to be adjusted accordingly.

* The ``ceph osd pool create <poolname> <pgnum>`` command now requires
  the ``pgnum`` argument. Previously this was optional, and would
  default to 8, which was almost never a good number.

* Degraded mode (when there fewer than the desired number of replicas)
  is now more configurable on a per-pool basis, with the min_size
  parameter. By default, with min_size 0, this allows I/O to objects
  with N - floor(N/2) replicas, where N is the total number of
  expected copies. Argonaut behavior was equivalent to having min_size
  = 1, so I/O would always be possible if any completely up to date
  copy remained. min_size = 1 could result in lower overall
  availability in certain cases, such as flapping network partitions.

* The sysvinit start/stop script now defaults to adjusting the max
  open files ulimit to 16384.  On most systems the default is 1024, so
  this is an increase and won't break anything.  If some system has a
  higher initial value, however, this change will lower the limit.
  The value can be adjusted explicitly by adding an entry to the
  ``ceph.conf`` file in the appropriate section.  For example::

     [global]
             max open files = 32768

* 'rbd lock list' and 'rbd showmapped' no longer use tabs as
  separators in their output.

* There is configurable limit on the number of PGs when creating a new
  pool, to prevent a user from accidentally specifying a ridiculous
  number for pg_num.  It can be adjusted via the 'mon max pool pg num'
  option on the monitor, and defaults to 65536 (the current max
  supported by the Linux kernel client).

* The osd capabilities associated with a rados user have changed
  syntax since 0.48 argonaut. The new format is mostly backwards
  compatible, but there are two backwards-incompatible changes:

  * specifying a list of pools in one grant, i.e.
    'allow r pool=foo,bar' is now done in separate grants, i.e.
    'allow r pool=foo, allow r pool=bar'.

  * restricting pool access by pool owner ('allow r uid=foo') is
    removed. This feature was not very useful and unused in practice.

  The new format is documented in the ceph-authtool man page.

* 'rbd cp' and 'rbd rename' use rbd as the default destination pool,
  regardless of what pool the source image is in. Previously they
  would default to the same pool as the source image.

* 'rbd export' no longer prints a message for each object written. It
  just reports percent complete like other long-lasting operations.

* 'ceph osd tree' now uses 4 decimal places for weight so output is
  nicer for humans

* Several monitor operations are now idempotent:

  * ceph osd pool create
  * ceph osd pool delete
  * ceph osd pool mksnap
  * ceph osd rm
  * ceph pg <pgid> revert

Notable changes
---------------

* auth: enable cephx by default
* auth: expanded authentication settings for greater flexibility
* auth: sign messages when using cephx
* build fixes for Fedora 18, CentOS/RHEL 6
* ceph: new 'osd ls' and 'osd tell <osd.N> version' commands
* ceph-debugpack: misc improvements
* ceph-disk-prepare: creates and labels GPT partitions
* ceph-disk-prepare: support for external journals, default mount/mkfs options, etc.
* ceph-fuse/libcephfs: many misc fixes, admin socket debugging
* ceph-fuse: fix handling for .. in root directory
* ceph-fuse: many fixes (including memory leaks, hangs)
* ceph-fuse: mount helper (mount.fuse.ceph) for use with /etc/fstab
* ceph.spec: misc packaging fixes
* common: thread pool sizes can now be adjusted at runtime
* config: $pid is now available as a metavariable
* crush: default root of tree type is now 'root' instead of 'pool' (to avoid confusiong wrt rados pools)
* crush: fixed retry behavior with chooseleaf via tunable
* crush: tunables documented; feature bit now present and enforced
* libcephfs: java wrapper
* librados: several bug fixes (rare races, locking errors)
* librados: some locking fixes
* librados: watch/notify fixes, misc memory leaks
* librbd: a few fixes to 'discard' support
* librbd: fine-grained striping feature
* librbd: fixed memory leaks
* librbd: fully functional and documented image cloning
* librbd: image (advisory) locking
* librbd: improved caching (of object non-existence)
* librbd: 'flatten' command to sever clone parent relationship
* librbd: 'protect'/'unprotect' commands to prevent clone parent from being deleted
* librbd: clip requests past end-of-image.
* librbd: fixes an issue with some windows guests running in qemu (remove floating point usage)
* log: fix in-memory buffering behavior (to only write log messages on crash)
* mds: fix ino release on abort session close, relative getattr path, mds shutdown, other misc items
* mds: misc fixes
* mkcephfs: fix for default keyring, osd data/journal locations
* mkcephfs: support for formatting xfs, ext4 (as well as btrfs)
* init: support for automatically mounting xfs and ext4 osd data directories
* mon, radosgw, ceph-fuse: fixed memory leaks
* mon: improved ENOSPC, fs error checking
* mon: less-destructive ceph-mon --mkfs behavior
* mon: misc fixes
* mon: more informative info about stuck PGs in 'health detail'
* mon: information about recovery and backfill in 'pg <pgid> query'
* mon: new 'osd crush create-or-move ...' command
* mon: new 'osd crush move ...' command lets you rearrange your CRUSH hierarchy
* mon: optionally dump 'osd tree' in json
* mon: configurable cap on maximum osd number (mon max osd)
* mon: many bug fixes (various races causing ceph-mon crashes)
* mon: new on-disk metadata to facilitate future mon changes (post-bobtail)
* mon: election bug fixes
* mon: throttle client messages (limit memory consumption)
* mon: throttle osd flapping based on osd history (limits osdmap ΄thrashing' on overloaded or unhappy clusters)
* mon: 'report' command for dumping detailed cluster status (e.g., for use when reporting bugs)
* mon: osdmap flags like noup, noin now cause a health warning
* msgr: improved failure handling code
* msgr: many bug fixes
* osd, mon: honor new 'nobackfill' and 'norecover' osdmap flags
* osd, mon: use feature bits to lock out clients lacking CRUSH tunables when they are in use
* osd: backfill reservation framework (to avoid flooding new osds with backfill data)
* osd: backfill target reservations (improve performance during recovery)
* osd: better tracking of recent slow operations
* osd: capability grammar improvements, bug fixes
* osd: client vs recovery io prioritization
* osd: crush performance improvements
* osd: default journal size to 5 GB
* osd: experimental support for PG "splitting" (pg_num adjustment for existing pools)
* osd: fix memory leak on certain error paths
* osd: fixed detection of EIO errors from fs on read
* osd: major refactor of PG peering and threading
* osd: many bug fixes
* osd: more/better dump info about in-progress operations
* osd: new caps structure (see compatibility notes)
* osd: new 'deep scrub' will compare object content across replicas (once per week by default)
* osd: new 'lock' rados class for generic object locking
* osd: optional 'min' pg size
* osd: recovery reservations
* osd: scrub efficiency improvement
* osd: several out of order reply bug fixes
* osd: several rare peering cases fixed
* osd: some performance improvements related to request queuing
* osd: use entire device if journal is a block device
* osd: use syncfs(2) when kernel supports it, even if glibc does not
* osd: various fixes for out-of-order op replies
* rados: ability to copy, rename pools
* rados: bench command now cleans up after itself
* rados: 'cppool' command to copy rados pools
* rados: 'rm' now accepts a list of objects to be removed
* radosgw: POST support
* radosgw: REST API for managing usage stats
* radosgw: fix bug in bucket stat updates
* radosgw: fix copy-object vs attributes
* radosgw: fix range header for large objects, ETag quoting, GMT dates, other compatibility fixes
* radosgw: improved garbage collection framework
* radosgw: many small fixes, cleanups
* radosgw: openstack keystone integration
* radosgw: stripe large (non-multipart) objects
* radosgw: support for multi-object deletes
* radosgw: support for swift manifest objects
* radosgw: vanity bucket dns names
* radosgw: various API compatibility fixes
* rbd: import from stdin, export to stdout
* rbd: new 'ls -l' option to view images with metadata
* rbd: use generic id and keyring options for 'rbd map'
* rbd: don't issue usage on errors
* udev: fix symlink creation for rbd images containing partitions
* upstart: job files for all daemon types (not enabled by default)
* wireshark: ceph protocol dissector patch updated


v0.54
=====

Upgrading
---------

* The osd capabilities associated with a rados user have changed
  syntax since 0.48 argonaut. The new format is mostly backwards
  compatible, but there are two backwards-incompatible changes:

  * specifying a list of pools in one grant, i.e.
    'allow r pool=foo,bar' is now done in separate grants, i.e.
    'allow r pool=foo, allow r pool=bar'.

  * restricting pool access by pool owner ('allow r uid=foo') is
    removed. This feature was not very useful and unused in practice.

  The new format is documented in the ceph-authtool man page.

* Bug fixes to the new osd capability format parsing properly validate
  the allowed operations. If an existing rados user gets permissions
  errors after upgrading, its capabilities were probably
  misconfigured. See the ceph-authtool man page for details on osd
  capabilities.

* 'rbd lock list' and 'rbd showmapped' no longer use tabs as
  separators in their output.


v0.48.3 "argonaut"
==================

This release contains a critical fix that can prevent data loss or
corruption after a power loss or kernel panic event.  Please upgrade
immediately.

Upgrading
---------

* If you are using the undocumented ``ceph-disk-prepare`` and
  ``ceph-disk-activate`` tools, they have several new features and
  some additional functionality.  Please review the changes in
  behavior carefully before upgrading.
* The .deb packages now require xfsprogs.

Notable changes
---------------

* filestore: fix op_seq write order (fixes journal replay after power loss)
* osd: fix occasional indefinitely hung "slow" request
* osd: fix encoding for pool_snap_info_t when talking to pre-v0.48 clients
* osd: fix heartbeat check
* osd: reduce log noise about rbd watch
* log: fixes for deadlocks in the internal logging code
* log: make log buffer size adjustable
* init script: fix for 'ceph status' across machines
* radosgw: fix swift error handling
* radosgw: fix swift authentication concurrency bug
* radosgw: don't cache large objects
* radosgw: fix some memory leaks
* radosgw: fix timezone conversion on read
* radosgw: relax date format restrictions
* radosgw: fix multipart overwrite
* radosgw: stop processing requests on client disconnect
* radosgw: avoid adding port to url that already has a port
* radosgw: fix copy to not override ETAG
* common: make parsing of ip address lists more forgiving
* common: fix admin socket compatibility with old protocol (for collectd plugin)
* mon: drop dup commands on paxos reset
* mds: fix loner selection for multiclient workloads
* mds: fix compat bit checks
* ceph-fuse: fix segfault on startup when keyring is missing
* ceph-authtool: fix usage
* ceph-disk-activate: misc backports
* ceph-disk-prepare: misc backports
* debian: depend on xfsprogs (we use xfs by default)
* rpm: build rpms, some related Makefile changes

For more detailed information, see :download:`the complete changelog <changelog/v0.48.3argonaut.txt>`.

v0.48.2 "argonaut"
==================

Upgrading
---------

* The default search path for keyring files now includes /etc/ceph/ceph.$name.keyring.  If such files are present on your cluster, be aware that by default they may now be used.

* There are several changes to the upstart init files.  These have not been previously documented or recommended.  Any existing users should review the changes before upgrading.

* The ceph-disk-prepare and ceph-disk-active scripts have been updated significantly.  These have not been previously documented or recommended.  Any existing users should review the changes before upgrading.

Notable changes
---------------

* mkcephfs: fix keyring generation for mds, osd when default paths are used
* radosgw: fix bug causing occasional corruption of per-bucket stats
* radosgw: workaround to avoid previously corrupted stats from going negative
* radosgw: fix bug in usage stats reporting on busy buckets
* radosgw: fix Content-Range: header for objects bigger than 2 GB.
* rbd: avoid leaving watch acting when command line tool errors out (avoids 30s delay on subsequent operations)
* rbd: friendlier use of --pool/--image options for import (old calling convention still works)
* librbd: fix rare snapshot creation race (could "lose" a snap when creation is concurrent)
* librbd: fix discard handling when spanning holes
* librbd: fix memory leak on discard when caching is enabled
* objecter: misc fixes for op reordering
* objecter: fix for rare startup-time deadlock waiting for osdmap
* ceph: fix usage
* mon: reduce log noise about "check_sub"
* ceph-disk-activate: misc fixes, improvements
* ceph-disk-prepare: partition and format osd disks automatically
* upstart: start everyone on a reboot
* upstart: always update the osd crush location on start if specified in the config
* config: add /etc/ceph/ceph.$name.keyring to default keyring search path
* ceph.spec: don't package crush headers

For more detailed information, see :download:`the complete changelog <changelog/v0.48.2argonaut.txt>`.

v0.48.1 "argonaut"
==================

Upgrading
---------

* The radosgw usage trim function was effectively broken in v0.48.  Earlier it would remove more usage data than what was requested.  This is fixed in v0.48.1, but the fix is incompatible.  The v0.48 radosgw-admin tool cannot be used to initiate the trimming; please use the v0.48.1 version.

* v0.48.1 now explicitly indicates support for the CRUSH_TUNABLES feature.  No other version of Ceph requires this, yet, but future versions will when the tunables are adjusted from their historical defaults.

* There are no other compatibility changes between v0.48.1 and v0.48.

Notable changes
---------------

* mkcephfs: use default 'keyring', 'osd data', 'osd journal' paths when not specified in conf
* msgr: various fixes to socket error handling
* osd: reduce scrub overhead
* osd: misc peering fixes (past_interval sharing, pgs stuck in 'peering' states)
* osd: fail on EIO in read path (do not silently ignore read errors from failing disks)
* osd: avoid internal heartbeat errors by breaking some large transactions into pieces
* osd: fix osdmap catch-up during startup (catch up and then add daemon to osdmap)
* osd: fix spurious 'misdirected op' messages
* osd: report scrub status via 'pg ... query'
* rbd: fix race when watch registrations are resent
* rbd: fix rbd image id assignment scheme (new image data objects have slightly different names)
* rbd: fix perf stats for cache hit rate
* rbd tool: fix off-by-one in key name (crash when empty key specified)
* rbd: more robust udev rules
* rados tool: copy object, pool commands
* radosgw: fix in usage stats trimming
* radosgw: misc API compatibility fixes (date strings, ETag quoting, swift headers, etc.)
* ceph-fuse: fix locking in read/write paths
* mon: fix rare race corrupting on-disk data
* config: fix admin socket 'config set' command
* log: fix in-memory log event gathering
* debian: remove crush headers, include librados-config
* rpm: add ceph-disk-{activate, prepare}

For more detailed information, see :download:`the complete changelog <changelog/v0.48.1argonaut.txt>`.

v0.48 "argonaut"
================

Upgrading
---------

* This release includes a disk format upgrade.  Each ceph-osd daemon, upon startup, will migrate its locally stored data to the new format.  This process can take a while (for large object counts, even hours), especially on non-btrfs file systems.  

* To keep the cluster available while the upgrade is in progress, we recommend you upgrade a storage node or rack at a time, and wait for the cluster to recover each time.  To prevent the cluster from moving data around in response to the OSD daemons being down for minutes or hours, you may want to::

    ceph osd set noout

  This will prevent the cluster from marking down OSDs as "out" and re-replicating the data elsewhere. If you do this, be sure to clear the flag when the upgrade is complete::

    ceph osd unset noout

* There is a encoding format change internal to the monitor cluster. The monitor daemons are careful to switch to the new format only when all members of the quorum support it.  However, that means that a partial quorum with new code may move to the new format, and a recovering monitor running old code will be unable to join (it will crash).  If this occurs, simply upgrading the remaining monitor will resolve the problem.

* The ceph tool's -s and -w commands from previous versions are incompatible with this version. Upgrade your client tools at the same time you upgrade the monitors if you rely on those commands.

* It is not possible to downgrade from v0.48 to a previous version.

Notable changes
---------------

* osd: stability improvements
* osd: capability model simplification
* osd: simpler/safer --mkfs (no longer removes all files; safe to re-run on active osd)
* osd: potentially buggy FIEMAP behavior disabled by default
* rbd: caching improvements
* rbd: improved instrumentation
* rbd: bug fixes
* radosgw: new, scalable usage logging infrastructure
* radosgw: per-user bucket limits
* mon: streamlined process for setting up authentication keys
* mon: stability improvements
* mon: log message throttling
* doc: improved documentation (ceph, rbd, radosgw, chef, etc.)
* config: new default locations for daemon keyrings
* config: arbitrary variable substitutions
* improved 'admin socket' daemon admin interface (ceph --admin-daemon ...)
* chef: support for multiple monitor clusters
* upstart: basic support for monitors, mds, radosgw; osd support still a work in progress.

The new default keyring locations mean that when enabling authentication (``auth supported = cephx``), keyring locations do not need to be specified if the keyring file is located inside the daemon's data directory (``/var/lib/ceph/$type/ceph-$id`` by default).

There is also a lot of librbd code in this release that is laying the groundwork for the upcoming layering functionality, but is not actually used. Likewise, the upstart support is still incomplete and not recommended; we will backport that functionality later if it turns out to be non-disruptive.




