====
Reef
====

Reef is the 18th stable release of Ceph. It is named after the reef squid (Sepioteuthis).

v18.2.6 Reef
============

This is the sixth backport (hotfix) release in the Reef series. We recommend that all users update to this release.

Notable Changes
---------------

* ceph-volume: A bug related to cryptsetup version handling has been fixed.
  Related tracker: https://tracker.ceph.com/issues/66393

* RADOS: A bug related to IPv6 support is now fixed.
  Related tracker: https://tracker.ceph.com/issues/67517

Changelog
---------

* ceph-volume: fix regex usage in `set_dmcrypt_no_workqueue` (`pr#62791 <https://github.com/ceph/ceph/pull/62791>`_, Matt1360)
* common/pick_address: Add IPv6 support to is_addr_in_subnet (`pr#62814 <https://github.com/ceph/ceph/pull/62814>`_, rzarzynski)

v18.2.5 Reef
============

This is the fifth backport release in the Reef series. We recommend that all users update to this release.

Notable Changes
---------------

* RBD: The ``try-netlink`` mapping option for rbd-nbd has become the default
  and is now deprecated. If the NBD netlink interface is not supported by the
  kernel, then the mapping is retried using the legacy ioctl interface.

* RADOS: A new command, `ceph osd rm-pg-upmap-primary-all`, has been added that allows
  users to clear all pg-upmap-primary mappings in the osdmap when desired.

  Related trackers:
   - https://tracker.ceph.com/issues/67179
   - https://tracker.ceph.com/issues/66867

Changelog
---------

* (reintroduce) test/librados: fix LibRadosIoECPP.CrcZeroWrite (`pr#61395 <https://github.com/ceph/ceph/pull/61395>`_, Samuel Just, Nitzan Mordechai)
* .github: sync the list of paths for rbd label, expand tests label to qa/\* (`pr#57727 <https://github.com/ceph/ceph/pull/57727>`_, Ilya Dryomov)
* <common> fix formatter buffer out-of-bounds (`pr#61105 <https://github.com/ceph/ceph/pull/61105>`_, liubingrun)
* [reef] os/bluestore: introduce allocator state histogram (`pr#61318 <https://github.com/ceph/ceph/pull/61318>`_, Igor Fedotov)
* [reef] qa/multisite: stabilize multisite testing (`pr#60402 <https://github.com/ceph/ceph/pull/60402>`_, Shilpa Jagannath, Casey Bodley)
* [reef] qa/rgw: the rgw/verify suite runs java tests last (`pr#60849 <https://github.com/ceph/ceph/pull/60849>`_, Casey Bodley)
* [RGW] Fix the handling of HEAD requests that do not comply with RFC standards (`pr#59122 <https://github.com/ceph/ceph/pull/59122>`_, liubingrun)
* a series of optimizations for kerneldevice discard (`pr#59048 <https://github.com/ceph/ceph/pull/59048>`_, Adam Kupczyk, Joshua Baergen, Gabriel BenHanokh, Matt Vandermeulen)
* Add Containerfile and build.sh to build it (`pr#60228 <https://github.com/ceph/ceph/pull/60228>`_, Dan Mick)
* add RBD Mirror monitoring alerts (`pr#56552 <https://github.com/ceph/ceph/pull/56552>`_, Arun Kumar Mohan)
* AsyncMessenger.cc : improve error messages (`pr#61402 <https://github.com/ceph/ceph/pull/61402>`_, Anthony D'Atri)
* AsyncMessenger: Don't decrease l_msgr_active_connections if it is negative (`pr#60445 <https://github.com/ceph/ceph/pull/60445>`_, Mohit Agrawal)
* blk/aio: fix long batch (64+K entries) submission (`pr#58675 <https://github.com/ceph/ceph/pull/58675>`_, Yingxin Cheng, Igor Fedotov, Adam Kupczyk, Robin Geuze)
* blk/KernelDevice: using join() to wait thread end is more safe (`pr#60615 <https://github.com/ceph/ceph/pull/60615>`_, Yite Gu)
* bluestore/bluestore_types: avoid heap-buffer-overflow in another way to keep code uniformity (`pr#58817 <https://github.com/ceph/ceph/pull/58817>`_, Rongqi Sun)
* BlueStore: Improve fragmentation score metric (`pr#59263 <https://github.com/ceph/ceph/pull/59263>`_, Adam Kupczyk)
* build-with-container fixes exec bit, dnf cache dir option (`pr#61913 <https://github.com/ceph/ceph/pull/61913>`_, John Mulligan)
* build-with-container: fixes and enhancements (`pr#62162 <https://github.com/ceph/ceph/pull/62162>`_, John Mulligan)
* build: Make boost_url a list (`pr#58315 <https://github.com/ceph/ceph/pull/58315>`_, Adam Emerson)
* ceph-mixin: Update mixin to include alerts for the nvmeof gateway(s) (`pr#56948 <https://github.com/ceph/ceph/pull/56948>`_, Adam King, Paul Cuzner)
* ceph-volume: allow zapping partitions on multipath devices (`pr#62178 <https://github.com/ceph/ceph/pull/62178>`_, Guillaume Abrioux)
* ceph-volume: create LVs when using partitions (`pr#58220 <https://github.com/ceph/ceph/pull/58220>`_, Guillaume Abrioux)
* ceph-volume: do source devices zapping if they're detached (`pr#58996 <https://github.com/ceph/ceph/pull/58996>`_, Igor Fedotov)
* ceph-volume: fix set_dmcrypt_no_workqueue() (`pr#58997 <https://github.com/ceph/ceph/pull/58997>`_, Guillaume Abrioux)
* ceph-volume: Fix unbound var in disk.get_devices() (`pr#59262 <https://github.com/ceph/ceph/pull/59262>`_, Zack Cerza)
* ceph-volume: fix unit tests errors (`pr#59956 <https://github.com/ceph/ceph/pull/59956>`_, Guillaume Abrioux)
* ceph-volume: update functional testing (`pr#56857 <https://github.com/ceph/ceph/pull/56857>`_, Guillaume Abrioux)
* ceph-volume: use importlib from stdlib on Python 3.8 and up (`pr#58005 <https://github.com/ceph/ceph/pull/58005>`_, Guillaume Abrioux, Kefu Chai)
* ceph-volume: use os.makedirs for mkdir_p (`pr#57472 <https://github.com/ceph/ceph/pull/57472>`_, Chen Yuanrun)
* ceph.spec.in: remove command-with-macro line (`pr#57357 <https://github.com/ceph/ceph/pull/57357>`_, John Mulligan)
* ceph.spec.in: we need jsonnet for all distroes for make check (`pr#60076 <https://github.com/ceph/ceph/pull/60076>`_, Kyr Shatskyy)
* ceph_mon: Fix MonitorDBStore usage (`pr#54150 <https://github.com/ceph/ceph/pull/54150>`_, Matan Breizman)
* ceph_test_rados_api_misc: adjust LibRadosMiscConnectFailure.ConnectTimeout timeout (`pr#58137 <https://github.com/ceph/ceph/pull/58137>`_, Lucian Petrut)
* cephadm/services/ingress: configure security user in keepalived template (`pr#61151 <https://github.com/ceph/ceph/pull/61151>`_, Bernard Landon)
* cephadm: add idmap.conf to nfs sample file (`pr#59453 <https://github.com/ceph/ceph/pull/59453>`_, Adam King)
* cephadm: added check for `--skip-firewalld` to section on adding explicit Ports to firewalld (`pr#57519 <https://github.com/ceph/ceph/pull/57519>`_, Michaela Lang)
* cephadm: CephExporter doesn't bind to IPv6 in dual stack (`pr#59461 <https://github.com/ceph/ceph/pull/59461>`_, Mouratidis Theofilos)
* cephadm: change loki/promtail default image tags (`pr#57475 <https://github.com/ceph/ceph/pull/57475>`_, Guillaume Abrioux)
* cephadm: disable ms_bind_ipv4 if we will enable ms_bind_ipv6 (`pr#61714 <https://github.com/ceph/ceph/pull/61714>`_, Dan van der Ster, Joshua Blanch)
* cephadm: emit warning if daemon's image is not to be used (`pr#61721 <https://github.com/ceph/ceph/pull/61721>`_, Matthew Vernon)
* cephadm: fix `cephadm shell --name <daemon-name>` for stopped/failed daemon (`pr#56490 <https://github.com/ceph/ceph/pull/56490>`_, Adam King)
* cephadm: fix apparmor profiles with spaces in the names (`pr#61712 <https://github.com/ceph/ceph/pull/61712>`_, John Mulligan)
* cephadm: fix host-maintenance command always exiting with a failure (`pr#59454 <https://github.com/ceph/ceph/pull/59454>`_, John Mulligan)
* cephadm: have agent check for errors before json loading mgr response (`pr#59455 <https://github.com/ceph/ceph/pull/59455>`_, Adam King)
* cephadm: make bootstrap default to "global" section for public_network setting (`pr#61918 <https://github.com/ceph/ceph/pull/61918>`_, Adam King)
* cephadm: pin pyfakefs version for tox tests (`pr#56762 <https://github.com/ceph/ceph/pull/56762>`_, Adam King)
* cephadm: pull container images from quay.io (`pr#60474 <https://github.com/ceph/ceph/pull/60474>`_, Guillaume Abrioux)
* cephadm: rgw: allow specifying the ssl_certificate by filepath (`pr#61922 <https://github.com/ceph/ceph/pull/61922>`_, Alexander Hussein-Kershaw)
* cephadm: Support Docker Live Restore (`pr#61916 <https://github.com/ceph/ceph/pull/61916>`_, Michal Nasiadka)
* cephadm: turn off cgroups_split setting  when bootstrapping with --no-cgroups-split (`pr#61716 <https://github.com/ceph/ceph/pull/61716>`_, Adam King)
* cephadm: use importlib.metadata for querying ceph_iscsi's version (`pr#58323 <https://github.com/ceph/ceph/pull/58323>`_, Zac Dover)
* CephContext: acquire _fork_watchers_lock in notify_post_fork() (`issue#63494 <http://tracker.ceph.com/issues/63494>`_, `pr#59266 <https://github.com/ceph/ceph/pull/59266>`_, Venky Shankar)
* cephfs-journal-tool: Add preventive measures to avoid fs corruption (`pr#57761 <https://github.com/ceph/ceph/pull/57761>`_, Jos Collin)
* cephfs-mirror: use monotonic clock (`pr#56701 <https://github.com/ceph/ceph/pull/56701>`_, Jos Collin)
* cephfs-shell: excute cmd 'rmdir_helper' reported error (`pr#58812 <https://github.com/ceph/ceph/pull/58812>`_, teng jie)
* cephfs-shell: fixing cephfs-shell test failures (`pr#60410 <https://github.com/ceph/ceph/pull/60410>`_, Neeraj Pratap Singh)
* cephfs-shell: prints warning, hangs and aborts when launched (`pr#58088 <https://github.com/ceph/ceph/pull/58088>`_, Rishabh Dave)
* cephfs-top: fix exceptions on small/large sized windows (`pr#59898 <https://github.com/ceph/ceph/pull/59898>`_, Jos Collin)
* cephfs: add command "ceph fs swap" (`pr#54942 <https://github.com/ceph/ceph/pull/54942>`_, Rishabh Dave)
* cephfs: Fixed a bug in the readdir_cache_cb function that may have us… (`pr#58805 <https://github.com/ceph/ceph/pull/58805>`_, Tod Chen)
* cephfs_mirror, qa: fix mirror daemon doesn't restart when blocklisted or failed (`pr#58632 <https://github.com/ceph/ceph/pull/58632>`_, Jos Collin)
* cephfs_mirror, qa: fix test failure test_cephfs_mirror_cancel_mirroring_and_readd (`pr#60182 <https://github.com/ceph/ceph/pull/60182>`_, Jos Collin)
* cephfs_mirror: 'ceph fs snapshot mirror ls' command (`pr#60178 <https://github.com/ceph/ceph/pull/60178>`_, Jos Collin)
* cephfs_mirror: fix crash in update_fs_mirrors() (`pr#57451 <https://github.com/ceph/ceph/pull/57451>`_, Jos Collin)
* cephfs_mirror: increment sync_failures when sync_perms() and sync_snaps() fails (`pr#57437 <https://github.com/ceph/ceph/pull/57437>`_, Jos Collin)
* cephfs_mirror: provide metrics for last successful snapshot sync (`pr#59071 <https://github.com/ceph/ceph/pull/59071>`_, Jos Collin)
* client: check mds down status before getting mds_gid_t from mdsmap (`pr#58492 <https://github.com/ceph/ceph/pull/58492>`_, Yite Gu, Dhairya Parmar)
* client: clear resend_mds only after sending request (`pr#57174 <https://github.com/ceph/ceph/pull/57174>`_, Patrick Donnelly)
* client: disallow unprivileged users to escalate root privileges (`pr#61379 <https://github.com/ceph/ceph/pull/61379>`_, Xiubo Li, Venky Shankar)
* client: do not proceed with I/O if filehandle is invalid (`pr#58397 <https://github.com/ceph/ceph/pull/58397>`_, Venky Shankar, Dhairya Parmar)
* client: Fix leading / issue with mds_check_access (`pr#58982 <https://github.com/ceph/ceph/pull/58982>`_, Kotresh HR, Rishabh Dave)
* client: Fix opening and reading of symlinks (`pr#60373 <https://github.com/ceph/ceph/pull/60373>`_, Anoop C S)
* client: flush the caps release in filesystem sync (`pr#59397 <https://github.com/ceph/ceph/pull/59397>`_, Xiubo Li)
* client: log debug message when requesting unmount (`pr#56955 <https://github.com/ceph/ceph/pull/56955>`_, Patrick Donnelly)
* client: Prevent race condition when printing Inode in ll_sync_inode (`pr#59620 <https://github.com/ceph/ceph/pull/59620>`_, Chengen Du)
* client: set LIBMOUNT_FORCE_MOUNT2=always (`pr#58529 <https://github.com/ceph/ceph/pull/58529>`_, Jakob Haufe)
* cls/cas/cls_cas_internal: Initialize 'hash' value before decoding (`pr#59237 <https://github.com/ceph/ceph/pull/59237>`_, Nitzan Mordechai)
* cls/user: reset stats only returns marker when truncated (`pr#60165 <https://github.com/ceph/ceph/pull/60165>`_, Casey Bodley)
* cmake/arrow: don't treat warnings as errors (`pr#57375 <https://github.com/ceph/ceph/pull/57375>`_, Casey Bodley)
* cmake: use ExternalProjects to build isa-l and isa-l_crypto libraries (`pr#60108 <https://github.com/ceph/ceph/pull/60108>`_, Casey Bodley)
* common,osd: Use last valid OSD IOPS value if measured IOPS is unrealistic (`pr#60659 <https://github.com/ceph/ceph/pull/60659>`_, Sridhar Seshasayee)
* common/admin_socket: add a command to raise a signal (`pr#54357 <https://github.com/ceph/ceph/pull/54357>`_, Leonid Usov)
* common/dout: fix FTBFS on GCC 14 (`pr#59056 <https://github.com/ceph/ceph/pull/59056>`_, Radoslaw Zarzynski)
* common/Formatter: dump inf/nan as null (`pr#60061 <https://github.com/ceph/ceph/pull/60061>`_, Md Mahamudur Rahaman Sajib)
* common/options: Change HDD OSD shard configuration defaults for mClock (`pr#59972 <https://github.com/ceph/ceph/pull/59972>`_, Sridhar Seshasayee)
* common/pick_address: check if address in subnet all public address (`pr#57590 <https://github.com/ceph/ceph/pull/57590>`_, Nitzan Mordechai)
* common/StackStringStream: update pointer to newly allocated memory in overflow() (`pr#57362 <https://github.com/ceph/ceph/pull/57362>`_, Rongqi Sun)
* common/TrackedOp: do not count the ops marked as nowarn (`pr#58744 <https://github.com/ceph/ceph/pull/58744>`_, Xiubo Li)
* common/TrackedOp: rename and raise prio of slow op perfcounter (`pr#59280 <https://github.com/ceph/ceph/pull/59280>`_, Yite Gu)
* common: fix md_config_cacher_t (`pr#61403 <https://github.com/ceph/ceph/pull/61403>`_, Ronen Friedman)
* common: use close_range on Linux (`pr#61625 <https://github.com/ceph/ceph/pull/61625>`_, edef)
* container/build.sh: don't require repo creds on NO_PUSH (`pr#61582 <https://github.com/ceph/ceph/pull/61582>`_, Dan Mick)
* container/build.sh: fix up org vs. repo naming (`pr#61581 <https://github.com/ceph/ceph/pull/61581>`_, Dan Mick)
* container/build.sh: remove local container images (`pr#62065 <https://github.com/ceph/ceph/pull/62065>`_, Dan Mick)
* container/Containerfile: replace CEPH_VERSION label for backward compat (`pr#61580 <https://github.com/ceph/ceph/pull/61580>`_, Dan Mick)
* container: add label ceph=True back (`pr#61612 <https://github.com/ceph/ceph/pull/61612>`_, John Mulligan)
* containerized build tools [V2] (`pr#61683 <https://github.com/ceph/ceph/pull/61683>`_, John Mulligan, Ernesto Puerta)
* debian pkg: record python3-packaging dependency for ceph-volume (`pr#59201 <https://github.com/ceph/ceph/pull/59201>`_, Kefu Chai, Thomas Lamprecht)
* debian: add ceph-exporter package (`pr#56541 <https://github.com/ceph/ceph/pull/56541>`_, Shinya Hayashi)
* debian: add missing bcrypt to ceph-mgr .requires to fix resulting package dependencies (`pr#54662 <https://github.com/ceph/ceph/pull/54662>`_, Thomas Lamprecht)
* debian: recursively adjust permissions of /var/lib/ceph/crash (`pr#58458 <https://github.com/ceph/ceph/pull/58458>`_, Max Carrara)
* doc,mailmap: update my email / association to ibm (`pr#60339 <https://github.com/ceph/ceph/pull/60339>`_, Patrick Donnelly)
* doc/ceph-volume: add spillover fix procedure (`pr#59541 <https://github.com/ceph/ceph/pull/59541>`_, Zac Dover)
* doc/cephadm/services: Re-improve osd.rst (`pr#61953 <https://github.com/ceph/ceph/pull/61953>`_, Anthony D'Atri)
* doc/cephadm/upgrade: ceph-ci containers are hosted by quay.ceph.io (`pr#58681 <https://github.com/ceph/ceph/pull/58681>`_, Casey Bodley)
* doc/cephadm: add default monitor images (`pr#57209 <https://github.com/ceph/ceph/pull/57209>`_, Zac Dover)
* doc/cephadm: add malformed-JSON removal instructions (`pr#59664 <https://github.com/ceph/ceph/pull/59664>`_, Zac Dover)
* doc/cephadm: Clarify "Deploying a new Cluster" (`pr#60810 <https://github.com/ceph/ceph/pull/60810>`_, Zac Dover)
* doc/cephadm: clean "Adv. OSD Service Specs" (`pr#60680 <https://github.com/ceph/ceph/pull/60680>`_, Zac Dover)
* doc/cephadm: correct note (`pr#61529 <https://github.com/ceph/ceph/pull/61529>`_, Zac Dover)
* doc/cephadm: edit "Using Custom Images" (`pr#58941 <https://github.com/ceph/ceph/pull/58941>`_, Zac Dover)
* doc/cephadm: how to get exact size_spec from device (`pr#59431 <https://github.com/ceph/ceph/pull/59431>`_, Zac Dover)
* doc/cephadm: improve "Activate Existing OSDs" (`pr#61748 <https://github.com/ceph/ceph/pull/61748>`_, Zac Dover)
* doc/cephadm: improve "Activate Existing OSDs" (`pr#61726 <https://github.com/ceph/ceph/pull/61726>`_, Zac Dover)
* doc/cephadm: link to "host pattern" matching sect (`pr#60645 <https://github.com/ceph/ceph/pull/60645>`_, Zac Dover)
* doc/cephadm: Reef default images procedure (`pr#57236 <https://github.com/ceph/ceph/pull/57236>`_, Zac Dover)
* doc/cephadm: remove downgrade reference from upgrade docs (`pr#57086 <https://github.com/ceph/ceph/pull/57086>`_, Adam King)
* doc/cephadm: simplify confusing math proposition (`pr#61575 <https://github.com/ceph/ceph/pull/61575>`_, Zac Dover)
* doc/cephadm: Update operations.rst (`pr#60638 <https://github.com/ceph/ceph/pull/60638>`_, rhkelson)
* doc/cephfs: add cache pressure information (`pr#59149 <https://github.com/ceph/ceph/pull/59149>`_, Zac Dover)
* doc/cephfs: add doc for disabling mgr/volumes plugin (`pr#60497 <https://github.com/ceph/ceph/pull/60497>`_, Rishabh Dave)
* doc/cephfs: add metrics to left pane (`pr#57736 <https://github.com/ceph/ceph/pull/57736>`_, Zac Dover)
* doc/cephfs: disambiguate "Reporting Free Space" (`pr#56872 <https://github.com/ceph/ceph/pull/56872>`_, Zac Dover)
* doc/cephfs: disambiguate two sentences (`pr#57704 <https://github.com/ceph/ceph/pull/57704>`_, Zac Dover)
* doc/cephfs: disaster-recovery-experts cleanup (`pr#61447 <https://github.com/ceph/ceph/pull/61447>`_, Zac Dover)
* doc/cephfs: document purge queue and its perf counters (`pr#61194 <https://github.com/ceph/ceph/pull/61194>`_, Dhairya Parmar)
* doc/cephfs: edit "Cloning Snapshots" in fs-volumes.rst (`pr#57666 <https://github.com/ceph/ceph/pull/57666>`_, Zac Dover)
* doc/cephfs: edit "Disabling Volumes Plugin" (`pr#60468 <https://github.com/ceph/ceph/pull/60468>`_, Rishabh Dave)
* doc/cephfs: edit "Dynamic Subtree Partitioning" (`pr#58910 <https://github.com/ceph/ceph/pull/58910>`_, Zac Dover)
* doc/cephfs: edit "is mount helper present" (`pr#58579 <https://github.com/ceph/ceph/pull/58579>`_, Zac Dover)
* doc/cephfs: edit "Layout Fields" text (`pr#59022 <https://github.com/ceph/ceph/pull/59022>`_, Zac Dover)
* doc/cephfs: edit "Pinning Subvolumes..." (`pr#57663 <https://github.com/ceph/ceph/pull/57663>`_, Zac Dover)
* doc/cephfs: edit 2nd 3rd of mount-using-kernel-driver (`pr#61059 <https://github.com/ceph/ceph/pull/61059>`_, Zac Dover)
* doc/cephfs: edit 3rd 3rd of mount-using-kernel-driver (`pr#61081 <https://github.com/ceph/ceph/pull/61081>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (`pr#61424 <https://github.com/ceph/ceph/pull/61424>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (2 of x) (`pr#61444 <https://github.com/ceph/ceph/pull/61444>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (3 of x) (`pr#61454 <https://github.com/ceph/ceph/pull/61454>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (4 of x) (`pr#61480 <https://github.com/ceph/ceph/pull/61480>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (5 of x) (`pr#61500 <https://github.com/ceph/ceph/pull/61500>`_, Zac Dover)
* doc/cephfs: edit disaster-recovery-experts (6 of x) (`pr#61522 <https://github.com/ceph/ceph/pull/61522>`_, Zac Dover)
* doc/cephfs: edit first 3rd of mount-using-kernel-driver (`pr#61042 <https://github.com/ceph/ceph/pull/61042>`_, Zac Dover)
* doc/cephfs: edit front matter in client-auth.rst (`pr#57122 <https://github.com/ceph/ceph/pull/57122>`_, Zac Dover)
* doc/cephfs: edit front matter in mantle.rst (`pr#57792 <https://github.com/ceph/ceph/pull/57792>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (1 of x) (`pr#57418 <https://github.com/ceph/ceph/pull/57418>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (1 of x) followup (`pr#57427 <https://github.com/ceph/ceph/pull/57427>`_, Zac Dover)
* doc/cephfs: edit fs-volumes.rst (2 of x) (`pr#57543 <https://github.com/ceph/ceph/pull/57543>`_, Zac Dover)
* doc/cephfs: edit grammar in snapshots.rst (`pr#61460 <https://github.com/ceph/ceph/pull/61460>`_, Zac Dover)
* doc/cephfs: edit vstart warning text (`pr#57815 <https://github.com/ceph/ceph/pull/57815>`_, Zac Dover)
* doc/cephfs: fix "file layouts" link (`pr#58876 <https://github.com/ceph/ceph/pull/58876>`_, Zac Dover)
* doc/cephfs: fix "OSD capabilities" link (`pr#58893 <https://github.com/ceph/ceph/pull/58893>`_, Zac Dover)
* doc/cephfs: fix typo (`pr#58469 <https://github.com/ceph/ceph/pull/58469>`_, spdfnet)
* doc/cephfs: improve "layout fields" text (`pr#59251 <https://github.com/ceph/ceph/pull/59251>`_, Zac Dover)
* doc/cephfs: improve cache-configuration.rst (`pr#59215 <https://github.com/ceph/ceph/pull/59215>`_, Zac Dover)
* doc/cephfs: improve ceph-fuse command (`pr#56968 <https://github.com/ceph/ceph/pull/56968>`_, Zac Dover)
* doc/cephfs: rearrange subvolume group information (`pr#60436 <https://github.com/ceph/ceph/pull/60436>`_, Indira Sawant)
* doc/cephfs: refine client-auth (1 of 3) (`pr#56780 <https://github.com/ceph/ceph/pull/56780>`_, Zac Dover)
* doc/cephfs: refine client-auth (2 of 3) (`pr#56842 <https://github.com/ceph/ceph/pull/56842>`_, Zac Dover)
* doc/cephfs: refine client-auth (3 of 3) (`pr#56851 <https://github.com/ceph/ceph/pull/56851>`_, Zac Dover)
* doc/cephfs: s/mountpoint/mount point/ (`pr#59295 <https://github.com/ceph/ceph/pull/59295>`_, Zac Dover)
* doc/cephfs: s/mountpoint/mount point/ (`pr#59287 <https://github.com/ceph/ceph/pull/59287>`_, Zac Dover)
* doc/cephfs: s/subvolumegroups/subvolume groups (`pr#57743 <https://github.com/ceph/ceph/pull/57743>`_, Zac Dover)
* doc/cephfs: separate commands into sections (`pr#57669 <https://github.com/ceph/ceph/pull/57669>`_, Zac Dover)
* doc/cephfs: streamline a paragraph (`pr#58775 <https://github.com/ceph/ceph/pull/58775>`_, Zac Dover)
* doc/cephfs: take Anthony's suggestion (`pr#58360 <https://github.com/ceph/ceph/pull/58360>`_, Zac Dover)
* doc/cephfs: update cephfs-shell link (`pr#58371 <https://github.com/ceph/ceph/pull/58371>`_, Zac Dover)
* doc/cephfs: use 'p' flag to set layouts or quotas (`pr#60483 <https://github.com/ceph/ceph/pull/60483>`_, TruongSinh Tran-Nguyen)
* doc/dev/developer_guide/essentials: update mailing lists (`pr#62376 <https://github.com/ceph/ceph/pull/62376>`_, Laimis Juzeliunas)
* doc/dev/peering: Change acting set num (`pr#59063 <https://github.com/ceph/ceph/pull/59063>`_, qn2060)
* doc/dev/release-process.rst: New container build/release process (`pr#60972 <https://github.com/ceph/ceph/pull/60972>`_, Dan Mick)
* doc/dev/release-process.rst: note new 'project' arguments (`pr#57644 <https://github.com/ceph/ceph/pull/57644>`_, Dan Mick)
* doc/dev: add "activate latest release" RTD step (`pr#59655 <https://github.com/ceph/ceph/pull/59655>`_, Zac Dover)
* doc/dev: add formatting to basic workflow (`pr#58738 <https://github.com/ceph/ceph/pull/58738>`_, Zac Dover)
* doc/dev: add note about intro of perf counters (`pr#57758 <https://github.com/ceph/ceph/pull/57758>`_, Zac Dover)
* doc/dev: add target links to perf_counters.rst (`pr#57734 <https://github.com/ceph/ceph/pull/57734>`_, Zac Dover)
* doc/dev: edit "Principles for format change" (`pr#58576 <https://github.com/ceph/ceph/pull/58576>`_, Zac Dover)
* doc/dev: Fix typos in encoding.rst (`pr#58305 <https://github.com/ceph/ceph/pull/58305>`_, N Balachandran)
* doc/dev: improve basic-workflow.rst (`pr#58938 <https://github.com/ceph/ceph/pull/58938>`_, Zac Dover)
* doc/dev: instruct devs to backport (`pr#61064 <https://github.com/ceph/ceph/pull/61064>`_, Zac Dover)
* doc/dev: link to ceph.io leads list (`pr#58106 <https://github.com/ceph/ceph/pull/58106>`_, Zac Dover)
* doc/dev: origin of Labeled Perf Counters (`pr#57914 <https://github.com/ceph/ceph/pull/57914>`_, Zac Dover)
* doc/dev: remove "Stable Releases and Backports" (`pr#60273 <https://github.com/ceph/ceph/pull/60273>`_, Zac Dover)
* doc/dev: repair broken image (`pr#57008 <https://github.com/ceph/ceph/pull/57008>`_, Zac Dover)
* doc/dev: s/to asses/to assess/ (`pr#57423 <https://github.com/ceph/ceph/pull/57423>`_, Zac Dover)
* doc/dev_guide: add needs-upgrade-testing label info (`pr#58730 <https://github.com/ceph/ceph/pull/58730>`_, Zac Dover)
* doc/developer_guide: update doc about installing teuthology (`pr#57750 <https://github.com/ceph/ceph/pull/57750>`_, Rishabh Dave)
* doc/foundation.rst: update Intel point of contact (`pr#61032 <https://github.com/ceph/ceph/pull/61032>`_, Neha Ojha)
* doc/glossary.rst: add "Dashboard Plugin" (`pr#60897 <https://github.com/ceph/ceph/pull/60897>`_, Zac Dover)
* doc/glossary.rst: add "OpenStack Swift" and "Swift" (`pr#57942 <https://github.com/ceph/ceph/pull/57942>`_, Zac Dover)
* doc/glossary: add "ceph-ansible" (`pr#59008 <https://github.com/ceph/ceph/pull/59008>`_, Zac Dover)
* doc/glossary: add "ceph-fuse" entry (`pr#58944 <https://github.com/ceph/ceph/pull/58944>`_, Zac Dover)
* doc/glossary: add "DC" (Data Center) to glossary (`pr#60876 <https://github.com/ceph/ceph/pull/60876>`_, Zac Dover)
* doc/glossary: add "flapping OSD" (`pr#60865 <https://github.com/ceph/ceph/pull/60865>`_, Zac Dover)
* doc/glossary: add "object storage" (`pr#59425 <https://github.com/ceph/ceph/pull/59425>`_, Zac Dover)
* doc/glossary: add "PLP" to glossary (`pr#60504 <https://github.com/ceph/ceph/pull/60504>`_, Zac Dover)
* doc/glossary: add "Prometheus" (`pr#58978 <https://github.com/ceph/ceph/pull/58978>`_, Zac Dover)
* doc/glossary: Add "S3" (`pr#57983 <https://github.com/ceph/ceph/pull/57983>`_, Zac Dover)
* doc/governance: add exec council responsibilites (`pr#60140 <https://github.com/ceph/ceph/pull/60140>`_, Zac Dover)
* doc/governance: add Zac Dover's updated email (`pr#60135 <https://github.com/ceph/ceph/pull/60135>`_, Zac Dover)
* doc/install: fix typos in openEuler-installation doc (`pr#56413 <https://github.com/ceph/ceph/pull/56413>`_, Rongqi Sun)
* doc/install: Keep the name field of the created user consistent with … (`pr#59757 <https://github.com/ceph/ceph/pull/59757>`_, hejindong)
* doc/man/8/radosgw-admin: add get lifecycle command (`pr#57160 <https://github.com/ceph/ceph/pull/57160>`_, rkhudov)
* doc/man: add missing long option switches (`pr#57707 <https://github.com/ceph/ceph/pull/57707>`_, Patrick Donnelly)
* doc/man: edit ceph-bluestore-tool.rst (`pr#59683 <https://github.com/ceph/ceph/pull/59683>`_, Zac Dover)
* doc/man: supplant "wsync" with "nowsync" as the default (`pr#60200 <https://github.com/ceph/ceph/pull/60200>`_, Zac Dover)
* doc/mds: improve wording (`pr#59586 <https://github.com/ceph/ceph/pull/59586>`_, Piotr Parczewski)
* doc/mgr/dashboard: fix TLS typo (`pr#59032 <https://github.com/ceph/ceph/pull/59032>`_, Mindy Preston)
* doc/mgr: Add root CA cert instructions to rgw.rst (`pr#61885 <https://github.com/ceph/ceph/pull/61885>`_, Anuradha Gadge, Zac Dover)
* doc/mgr: edit "Overview" in dashboard.rst (`pr#57336 <https://github.com/ceph/ceph/pull/57336>`_, Zac Dover)
* doc/mgr: edit "Resolve IP address to hostname before redirect" (`pr#57296 <https://github.com/ceph/ceph/pull/57296>`_, Zac Dover)
* doc/mgr: explain error message - dashboard.rst (`pr#57109 <https://github.com/ceph/ceph/pull/57109>`_, Zac Dover)
* doc/mgr: remove Zabbix 1 information (`pr#56798 <https://github.com/ceph/ceph/pull/56798>`_, Zac Dover)
* doc/monitoring: Improve index.rst (`pr#62266 <https://github.com/ceph/ceph/pull/62266>`_, Anthony D'Atri)
* doc/rados/operations: Clarify stretch mode vs device class (`pr#62078 <https://github.com/ceph/ceph/pull/62078>`_, Anthony D'Atri)
* doc/rados/operations: improve crush-map-edits.rst (`pr#62318 <https://github.com/ceph/ceph/pull/62318>`_, Anthony D'Atri)
* doc/rados/operations: Improve health-checks.rst (`pr#59583 <https://github.com/ceph/ceph/pull/59583>`_, Anthony D'Atri)
* doc/rados/operations: Improve pools.rst (`pr#61729 <https://github.com/ceph/ceph/pull/61729>`_, Anthony D'Atri)
* doc/rados/operations: remove vanity cluster name reference from crush… (`pr#58948 <https://github.com/ceph/ceph/pull/58948>`_, Anthony D'Atri)
* doc/rados/operations: rephrase OSDs peering (`pr#57157 <https://github.com/ceph/ceph/pull/57157>`_, Piotr Parczewski)
* doc/rados/troubleshooting: Improve log-and-debug.rst (`pr#60825 <https://github.com/ceph/ceph/pull/60825>`_, Anthony D'Atri)
* doc/rados/troubleshooting: Improve troubleshooting-pg.rst (`pr#62321 <https://github.com/ceph/ceph/pull/62321>`_, Anthony D'Atri)
* doc/rados: add "pgs not deep scrubbed in time" info (`pr#59734 <https://github.com/ceph/ceph/pull/59734>`_, Zac Dover)
* doc/rados: add blaum_roth coding guidance (`pr#60538 <https://github.com/ceph/ceph/pull/60538>`_, Zac Dover)
* doc/rados: add bucket rename command (`pr#57027 <https://github.com/ceph/ceph/pull/57027>`_, Zac Dover)
* doc/rados: add confval directives to health-checks (`pr#59872 <https://github.com/ceph/ceph/pull/59872>`_, Zac Dover)
* doc/rados: add link to messenger v2 info in mon-lookup-dns.rst (`pr#59795 <https://github.com/ceph/ceph/pull/59795>`_, Zac Dover)
* doc/rados: add options to network config ref (`pr#57916 <https://github.com/ceph/ceph/pull/57916>`_, Zac Dover)
* doc/rados: add osd_deep_scrub_interval setting operation (`pr#59803 <https://github.com/ceph/ceph/pull/59803>`_, Zac Dover)
* doc/rados: add pg-states and pg-concepts to tree (`pr#58050 <https://github.com/ceph/ceph/pull/58050>`_, Zac Dover)
* doc/rados: add stop monitor command (`pr#57851 <https://github.com/ceph/ceph/pull/57851>`_, Zac Dover)
* doc/rados: add stretch_rule workaround (`pr#58182 <https://github.com/ceph/ceph/pull/58182>`_, Zac Dover)
* doc/rados: correct "full ratio" note (`pr#60738 <https://github.com/ceph/ceph/pull/60738>`_, Zac Dover)
* doc/rados: credit Prashant for a procedure (`pr#58258 <https://github.com/ceph/ceph/pull/58258>`_, Zac Dover)
* doc/rados: document manually passing search domain (`pr#58432 <https://github.com/ceph/ceph/pull/58432>`_, Zac Dover)
* doc/rados: document unfound object cache-tiering scenario (`pr#59381 <https://github.com/ceph/ceph/pull/59381>`_, Zac Dover)
* doc/rados: edit "Placement Groups Never Get Clean" (`pr#60047 <https://github.com/ceph/ceph/pull/60047>`_, Zac Dover)
* doc/rados: edit troubleshooting-osd.rst (`pr#58272 <https://github.com/ceph/ceph/pull/58272>`_, Zac Dover)
* doc/rados: explain replaceable parts of command (`pr#58060 <https://github.com/ceph/ceph/pull/58060>`_, Zac Dover)
* doc/rados: fix outdated value for ms_bind_port_max (`pr#57048 <https://github.com/ceph/ceph/pull/57048>`_, Pierre Riteau)
* doc/rados: fix sentences in health-checks (2 of x) (`pr#60932 <https://github.com/ceph/ceph/pull/60932>`_, Zac Dover)
* doc/rados: fix sentences in health-checks (3 of x) (`pr#60950 <https://github.com/ceph/ceph/pull/60950>`_, Zac Dover)
* doc/rados: followup to PR#58057 (`pr#58162 <https://github.com/ceph/ceph/pull/58162>`_, Zac Dover)
* doc/rados: improve leader/peon monitor explanation (`pr#57959 <https://github.com/ceph/ceph/pull/57959>`_, Zac Dover)
* doc/rados: improve pg_num/pgp_num info (`pr#62057 <https://github.com/ceph/ceph/pull/62057>`_, Zac Dover)
* doc/rados: make sentences agree in health-checks.rst (`pr#60921 <https://github.com/ceph/ceph/pull/60921>`_, Zac Dover)
* doc/rados: pool and namespace are independent osdcap restrictions (`pr#61524 <https://github.com/ceph/ceph/pull/61524>`_, Ilya Dryomov)
* doc/rados: PR#57022 unfinished business (`pr#57265 <https://github.com/ceph/ceph/pull/57265>`_, Zac Dover)
* doc/rados: remove dual-stack docs (`pr#57073 <https://github.com/ceph/ceph/pull/57073>`_, Zac Dover)
* doc/rados: remove redundant pg repair commands (`pr#57040 <https://github.com/ceph/ceph/pull/57040>`_, Zac Dover)
* doc/rados: s/cepgsqlite/cephsqlite/ (`pr#57247 <https://github.com/ceph/ceph/pull/57247>`_, Zac Dover)
* doc/rados: standardize markup of "clean" (`pr#60501 <https://github.com/ceph/ceph/pull/60501>`_, Zac Dover)
* doc/rados: update how to install c++ header files (`pr#58308 <https://github.com/ceph/ceph/pull/58308>`_, Pere Diaz Bou)
* doc/radosgw/config-ref: fix lc worker thread tuning (`pr#61438 <https://github.com/ceph/ceph/pull/61438>`_, Laimis Juzeliunas)
* doc/radosgw/multisite: fix Configuring Secondary Zones -> Updating the Period (`pr#60333 <https://github.com/ceph/ceph/pull/60333>`_, Casey Bodley)
* doc/radosgw/s3: correct eTag op match tables (`pr#61309 <https://github.com/ceph/ceph/pull/61309>`_, Anthony D'Atri)
* doc/radosgw: disambiguate version-added remarks (`pr#57141 <https://github.com/ceph/ceph/pull/57141>`_, Zac Dover)
* doc/radosgw: Improve archive-sync-module.rst (`pr#60853 <https://github.com/ceph/ceph/pull/60853>`_, Anthony D'Atri)
* doc/radosgw: Improve archive-sync-module.rst more (`pr#60868 <https://github.com/ceph/ceph/pull/60868>`_, Anthony D'Atri)
* doc/radosgw: s/zonegroup/pools/ (`pr#61557 <https://github.com/ceph/ceph/pull/61557>`_, Zac Dover)
* doc/radosgw: update Reef S3 action list (`pr#57365 <https://github.com/ceph/ceph/pull/57365>`_, Zac Dover)
* doc/radosgw: update rgw_dns_name doc (`pr#60886 <https://github.com/ceph/ceph/pull/60886>`_, Zac Dover)
* doc/radosgw: use 'confval' directive for reshard config options (`pr#57024 <https://github.com/ceph/ceph/pull/57024>`_, Casey Bodley)
* doc/rbd/rbd-exclusive-locks: mention incompatibility with advisory locks (`pr#58864 <https://github.com/ceph/ceph/pull/58864>`_, Ilya Dryomov)
* doc/rbd: add namespace information for mirror commands (`pr#60270 <https://github.com/ceph/ceph/pull/60270>`_, N Balachandran)
* doc/rbd: fix typos in NVMe-oF docs (`pr#58188 <https://github.com/ceph/ceph/pull/58188>`_, N Balachandran)
* doc/rbd: use https links in live import examples (`pr#61604 <https://github.com/ceph/ceph/pull/61604>`_, Ilya Dryomov)
* doc/README.md - add ordered list (`pr#59799 <https://github.com/ceph/ceph/pull/59799>`_, Zac Dover)
* doc/README.md: create selectable commands (`pr#59835 <https://github.com/ceph/ceph/pull/59835>`_, Zac Dover)
* doc/README.md: edit "Build Prerequisites" (`pr#59638 <https://github.com/ceph/ceph/pull/59638>`_, Zac Dover)
* doc/README.md: improve formatting (`pr#59786 <https://github.com/ceph/ceph/pull/59786>`_, Zac Dover)
* doc/README.md: improve formatting (`pr#59701 <https://github.com/ceph/ceph/pull/59701>`_, Zac Dover)
* doc/releases: add actual_eol for quincy (`pr#61360 <https://github.com/ceph/ceph/pull/61360>`_, Zac Dover)
* doc/releases: Add ordering comment to releases.yml (`pr#62193 <https://github.com/ceph/ceph/pull/62193>`_, Anthony D'Atri)
* doc/rgw/d3n: pass cache dir volume to extra_container_args (`pr#59768 <https://github.com/ceph/ceph/pull/59768>`_, Mark Kogan)
* doc/rgw/notification: persistent notification queue full behavior (`pr#59234 <https://github.com/ceph/ceph/pull/59234>`_, Yuval Lifshitz)
* doc/rgw/notifications: specify which event types are enabled by default (`pr#54500 <https://github.com/ceph/ceph/pull/54500>`_, Yuval Lifshitz)
* doc/security: remove old GPG information (`pr#56914 <https://github.com/ceph/ceph/pull/56914>`_, Zac Dover)
* doc/security: update CVE list (`pr#57018 <https://github.com/ceph/ceph/pull/57018>`_, Zac Dover)
* doc/src: add inline literals (` `` `) to variables (`pr#57937 <https://github.com/ceph/ceph/pull/57937>`_, Zac Dover)
* doc/src: invadvisable is not a word (`pr#58190 <https://github.com/ceph/ceph/pull/58190>`_, Doug Whitfield)
* doc/start/os-recommendations: remove 16.2.z support for CentOS 7 (`pr#58721 <https://github.com/ceph/ceph/pull/58721>`_, gukaifeng)
* doc/start: Add Beginner's Guide (`pr#57822 <https://github.com/ceph/ceph/pull/57822>`_, Zac Dover)
* doc/start: add links to Beginner's Guide (`pr#58203 <https://github.com/ceph/ceph/pull/58203>`_, Zac Dover)
* doc/start: add tested container host oses (`pr#58713 <https://github.com/ceph/ceph/pull/58713>`_, Zac Dover)
* doc/start: add vstart install guide (`pr#60462 <https://github.com/ceph/ceph/pull/60462>`_, Zac Dover)
* doc/start: Edit Beginner's Guide (`pr#57845 <https://github.com/ceph/ceph/pull/57845>`_, Zac Dover)
* doc/start: fix "are are" typo (`pr#60709 <https://github.com/ceph/ceph/pull/60709>`_, Zac Dover)
* doc/start: fix wording & syntax (`pr#58364 <https://github.com/ceph/ceph/pull/58364>`_, Piotr Parczewski)
* doc/start: Mention RGW in Intro to Ceph (`pr#61927 <https://github.com/ceph/ceph/pull/61927>`_, Anthony D'Atri)
* doc/start: remove "intro.rst" (`pr#57949 <https://github.com/ceph/ceph/pull/57949>`_, Zac Dover)
* doc/start: remove mention of Centos 8 support (`pr#58390 <https://github.com/ceph/ceph/pull/58390>`_, Zac Dover)
* doc/start: s/http/https/ in links (`pr#57871 <https://github.com/ceph/ceph/pull/57871>`_, Zac Dover)
* doc/start: s/intro.rst/index.rst/ (`pr#57903 <https://github.com/ceph/ceph/pull/57903>`_, Zac Dover)
* doc/start: separate package and container support tables (`pr#60789 <https://github.com/ceph/ceph/pull/60789>`_, Zac Dover)
* doc/start: separate package chart from container chart (`pr#60699 <https://github.com/ceph/ceph/pull/60699>`_, Zac Dover)
* doc/start: update mailing list links (`pr#58684 <https://github.com/ceph/ceph/pull/58684>`_, Zac Dover)
* doc: add snapshots in docs under Cephfs concepts (`pr#61247 <https://github.com/ceph/ceph/pull/61247>`_, Neeraj Pratap Singh)
* doc: Amend dev mailing list subscribe instructions (`pr#58697 <https://github.com/ceph/ceph/pull/58697>`_, Paulo E. Castro)
* doc: clarify availability vs integrity (`pr#58131 <https://github.com/ceph/ceph/pull/58131>`_, Gregory O'Neill)
* doc: clarify superuser note for ceph-fuse (`pr#58615 <https://github.com/ceph/ceph/pull/58615>`_, Patrick Donnelly)
* doc: Clarify that there are no tertiary OSDs (`pr#61731 <https://github.com/ceph/ceph/pull/61731>`_, Anthony D'Atri)
* doc: clarify use of location: in host spec (`pr#57647 <https://github.com/ceph/ceph/pull/57647>`_, Matthew Vernon)
* doc: Correct link to "Device management" (`pr#58489 <https://github.com/ceph/ceph/pull/58489>`_, Matthew Vernon)
* doc: Correct link to Prometheus docs (`pr#59560 <https://github.com/ceph/ceph/pull/59560>`_, Matthew Vernon)
* doc: correct typo (`pr#57884 <https://github.com/ceph/ceph/pull/57884>`_, Matthew Vernon)
* doc: document metrics exported by CephFS (`pr#57724 <https://github.com/ceph/ceph/pull/57724>`_, Jos Collin)
* doc: Document the Windows CI job (`pr#60034 <https://github.com/ceph/ceph/pull/60034>`_, Lucian Petrut)
* doc: Document which options are disabled by mClock (`pr#60672 <https://github.com/ceph/ceph/pull/60672>`_, Niklas Hambüchen)
* doc: documenting the feature that scrub clear the entries from damage… (`pr#59079 <https://github.com/ceph/ceph/pull/59079>`_, Neeraj Pratap Singh)
* doc: explain the consequence of enabling mirroring through monitor co… (`pr#60526 <https://github.com/ceph/ceph/pull/60526>`_, Jos Collin)
* doc: fix email (`pr#60234 <https://github.com/ceph/ceph/pull/60234>`_, Ernesto Puerta)
* doc: fix incorrect radosgw-admin subcommand (`pr#62005 <https://github.com/ceph/ceph/pull/62005>`_, Toshikuni Fukaya)
* doc: fix typo (`pr#59992 <https://github.com/ceph/ceph/pull/59992>`_, N Balachandran)
* doc: Fixes a typo in controllers section of hardware recommendations (`pr#61179 <https://github.com/ceph/ceph/pull/61179>`_, Kevin Niederwanger)
* doc: fixup #58689 - document SSE-C iam condition key (`pr#62298 <https://github.com/ceph/ceph/pull/62298>`_, dawg)
* doc: Improve doc/radosgw/placement.rst (`pr#58974 <https://github.com/ceph/ceph/pull/58974>`_, Anthony D'Atri)
* doc: improve tests-integration-testing-teuthology-workflow.rst (`pr#61343 <https://github.com/ceph/ceph/pull/61343>`_, Vallari Agrawal)
* doc: s/Whereas,/Although/ (`pr#60594 <https://github.com/ceph/ceph/pull/60594>`_, Zac Dover)
* doc: SubmittingPatches-backports - remove backports team (`pr#60298 <https://github.com/ceph/ceph/pull/60298>`_, Zac Dover)
* doc: Update "Getting Started" to link to start not install (`pr#59908 <https://github.com/ceph/ceph/pull/59908>`_, Matthew Vernon)
* doc: update Key Idea in cephfs-mirroring.rst (`pr#60344 <https://github.com/ceph/ceph/pull/60344>`_, Jos Collin)
* doc: update nfs doc for Kerberos setup of ganesha in Ceph (`pr#59940 <https://github.com/ceph/ceph/pull/59940>`_, Avan Thakkar)
* doc: update tests-integration-testing-teuthology-workflow.rst (`pr#59549 <https://github.com/ceph/ceph/pull/59549>`_, Vallari Agrawal)
* doc: Upgrade and unpin some python versions (`pr#61932 <https://github.com/ceph/ceph/pull/61932>`_, David Galloway)
* doc:update e-mail addresses governance (`pr#60085 <https://github.com/ceph/ceph/pull/60085>`_, Tobias Fischer)
* docs/rados/operations/stretch-mode: warn device class is not supported (`pr#59100 <https://github.com/ceph/ceph/pull/59100>`_, Kamoltat Sirivadhna)
* docs: removed centos 8 and added squid to the build matrix (`pr#58902 <https://github.com/ceph/ceph/pull/58902>`_, Yuri Weinstein)
* exporter: fix regex for rgw sync metrics (`pr#57658 <https://github.com/ceph/ceph/pull/57658>`_, Avan Thakkar)
* exporter: handle exceptions gracefully (`pr#57371 <https://github.com/ceph/ceph/pull/57371>`_, Divyansh Kamboj)
* fix issue with bucket notification test (`pr#61881 <https://github.com/ceph/ceph/pull/61881>`_, Yuval Lifshitz)
* global: Call getnam_r with a 64KiB buffer on the heap (`pr#60126 <https://github.com/ceph/ceph/pull/60126>`_, Adam Emerson)
* install-deps.sh, do_cmake.sh: almalinux is another el flavour (`pr#58522 <https://github.com/ceph/ceph/pull/58522>`_, Dan van der Ster)
* install-deps: save and restore user's XDG_CACHE_HOME (`pr#56993 <https://github.com/ceph/ceph/pull/56993>`_, luo rixin)
* kv/RocksDBStore: Configure compact-on-deletion for all CFs (`pr#57402 <https://github.com/ceph/ceph/pull/57402>`_, Joshua Baergen)
* librados: use CEPH_OSD_FLAG_FULL_FORCE for IoCtxImpl::remove (`pr#59282 <https://github.com/ceph/ceph/pull/59282>`_, Chen Yuanrun)
* librbd/crypto/LoadRequest: clone format for migration source image (`pr#60170 <https://github.com/ceph/ceph/pull/60170>`_, Ilya Dryomov)
* librbd/crypto: fix issue when live-migrating from encrypted export (`pr#59151 <https://github.com/ceph/ceph/pull/59151>`_, Ilya Dryomov)
* librbd/migration/HttpClient: avoid reusing ssl_stream after shut down (`pr#61094 <https://github.com/ceph/ceph/pull/61094>`_, Ilya Dryomov)
* librbd/migration: prune snapshot extents in RawFormat::list_snaps() (`pr#59660 <https://github.com/ceph/ceph/pull/59660>`_, Ilya Dryomov)
* librbd: add rbd_diff_iterate3() API to take source snapshot by ID (`pr#62129 <https://github.com/ceph/ceph/pull/62129>`_, Ilya Dryomov, Vinay Bhaskar Varada)
* librbd: avoid data corruption on flatten when object map is inconsistent (`pr#61167 <https://github.com/ceph/ceph/pull/61167>`_, Ilya Dryomov)
* librbd: clear ctx before initiating close in Image::{aio\_,}close() (`pr#61526 <https://github.com/ceph/ceph/pull/61526>`_, Ilya Dryomov)
* librbd: create rbd_trash object during pool initialization and namespace creation (`pr#57603 <https://github.com/ceph/ceph/pull/57603>`_, Ramana Raja)
* librbd: diff-iterate shouldn't crash on an empty byte range (`pr#58211 <https://github.com/ceph/ceph/pull/58211>`_, Ilya Dryomov)
* librbd: disallow group snap rollback if memberships don't match (`pr#58207 <https://github.com/ceph/ceph/pull/58207>`_, Ilya Dryomov)
* librbd: don't crash on a zero-length read if buffer is NULL (`pr#57570 <https://github.com/ceph/ceph/pull/57570>`_, Ilya Dryomov)
* librbd: fix a crash in get_rollback_snap_id (`pr#62045 <https://github.com/ceph/ceph/pull/62045>`_, Ilya Dryomov, N Balachandran)
* librbd: fix a deadlock on image_lock caused by Mirror::image_disable() (`pr#62127 <https://github.com/ceph/ceph/pull/62127>`_, Ilya Dryomov)
* librbd: fix mirror image status summary in a namespace (`pr#61831 <https://github.com/ceph/ceph/pull/61831>`_, Ilya Dryomov)
* librbd: make diff-iterate in fast-diff mode aware of encryption (`pr#58345 <https://github.com/ceph/ceph/pull/58345>`_, Ilya Dryomov)
* librbd: make group and group snapshot IDs more random (`pr#57091 <https://github.com/ceph/ceph/pull/57091>`_, Ilya Dryomov)
* librbd: stop filtering async request error codes (`pr#61644 <https://github.com/ceph/ceph/pull/61644>`_, Ilya Dryomov)
* Links to Jenkins jobs in PR comment commands / Remove deprecated commands (`pr#62037 <https://github.com/ceph/ceph/pull/62037>`_, David Galloway)
* log: save/fetch thread name infra (`pr#60728 <https://github.com/ceph/ceph/pull/60728>`_, Milind Changire, Patrick Donnelly)
* Make mon addrs consistent with mon info (`pr#60750 <https://github.com/ceph/ceph/pull/60750>`_, shenjiatong)
* mds/client: return -ENODATA when xattr doesn't exist for removexattr (`pr#58770 <https://github.com/ceph/ceph/pull/58770>`_, Xiubo Li)
* mds/purgequeue: add l_pq_executed_ops counter (`pr#58328 <https://github.com/ceph/ceph/pull/58328>`_, shimin)
* mds: Add fragment to scrub (`pr#56895 <https://github.com/ceph/ceph/pull/56895>`_, Christopher Hoffman)
* mds: batch backtrace updates by pool-id when expiring a log segment (`issue#63259 <http://tracker.ceph.com/issues/63259>`_, `pr#60689 <https://github.com/ceph/ceph/pull/60689>`_, Venky Shankar)
* mds: cephx path restriction incorrectly rejects snapshots of deleted directory (`pr#59519 <https://github.com/ceph/ceph/pull/59519>`_, Patrick Donnelly)
* mds: check relevant caps for fs include root_squash (`pr#57343 <https://github.com/ceph/ceph/pull/57343>`_, Patrick Donnelly)
* mds: CInode::item_caps used in two different lists (`pr#56886 <https://github.com/ceph/ceph/pull/56886>`_, Dhairya Parmar)
* mds: defer trim() until after the last cache_rejoin ack being received (`pr#56747 <https://github.com/ceph/ceph/pull/56747>`_, Xiubo Li)
* mds: do remove the cap when seqs equal or larger than last issue (`pr#58295 <https://github.com/ceph/ceph/pull/58295>`_, Xiubo Li)
* mds: don't add counters in warning for standby-replay MDS (`pr#57834 <https://github.com/ceph/ceph/pull/57834>`_, Rishabh Dave)
* mds: don't stall the asok thread for flush commands (`pr#57560 <https://github.com/ceph/ceph/pull/57560>`_, Leonid Usov)
* mds: fix session/client evict command (`issue#68132 <http://tracker.ceph.com/issues/68132>`_, `pr#58726 <https://github.com/ceph/ceph/pull/58726>`_, Venky Shankar, Neeraj Pratap Singh)
* mds: fix the description for inotable testing only options (`pr#57115 <https://github.com/ceph/ceph/pull/57115>`_, Xiubo Li)
* mds: getattr just waits the xlock to be released by the previous client (`pr#60692 <https://github.com/ceph/ceph/pull/60692>`_, Xiubo Li)
* mds: Implement remove for ceph vxattrs (`pr#58350 <https://github.com/ceph/ceph/pull/58350>`_, Christopher Hoffman)
* mds: inode_t flags may not be protected by the policylock during set_vxattr (`pr#57177 <https://github.com/ceph/ceph/pull/57177>`_, Patrick Donnelly)
* mds: log at a lower level when stopping (`pr#57227 <https://github.com/ceph/ceph/pull/57227>`_, Kotresh HR)
* mds: misc fixes for MDSAuthCaps code (`pr#60207 <https://github.com/ceph/ceph/pull/60207>`_, Xiubo Li)
* mds: prevent scrubbing for standby-replay MDS (`pr#58493 <https://github.com/ceph/ceph/pull/58493>`_, Neeraj Pratap Singh)
* mds: relax divergent backtrace scrub failures for replicated ancestor inodes (`issue#64730 <http://tracker.ceph.com/issues/64730>`_, `pr#58502 <https://github.com/ceph/ceph/pull/58502>`_, Venky Shankar)
* mds: set the correct WRLOCK flag always in wrlock_force() (`pr#58497 <https://github.com/ceph/ceph/pull/58497>`_, Xiubo Li)
* mds: set the proper extra bl for the create request (`pr#58528 <https://github.com/ceph/ceph/pull/58528>`_, Xiubo Li)
* mds: some request errors come from errno.h rather than fs_types.h (`pr#56664 <https://github.com/ceph/ceph/pull/56664>`_, Patrick Donnelly)
* mds: try to choose a new batch head in request_clientup() (`pr#58842 <https://github.com/ceph/ceph/pull/58842>`_, Xiubo Li)
* mds: use regular dispatch for processing beacons (`pr#57683 <https://github.com/ceph/ceph/pull/57683>`_, Patrick Donnelly)
* mds: use regular dispatch for processing metrics (`pr#57681 <https://github.com/ceph/ceph/pull/57681>`_, Patrick Donnelly)
* mgr/BaseMgrModule: Optimize CPython Call in Finish Function (`pr#55110 <https://github.com/ceph/ceph/pull/55110>`_, Nitzan Mordechai)
* mgr/cephadm: add "original_weight" parameter to OSD class (`pr#59411 <https://github.com/ceph/ceph/pull/59411>`_, Adam King)
* mgr/cephadm: add command to expose systemd units of all daemons (`pr#61915 <https://github.com/ceph/ceph/pull/61915>`_, Adam King)
* mgr/cephadm: Allows enabling NFS Ganesha NLM (`pr#56909 <https://github.com/ceph/ceph/pull/56909>`_, Teoman ONAY)
* mgr/cephadm: ceph orch host drain command to return error for invalid hostname (`pr#61919 <https://github.com/ceph/ceph/pull/61919>`_, Shweta Bhosale)
* mgr/cephadm: cleanup iscsi and nvmeof keyrings upon daemon removal (`pr#59459 <https://github.com/ceph/ceph/pull/59459>`_, Adam King)
* mgr/cephadm: create OSD daemon deploy specs through make_daemon_spec (`pr#61923 <https://github.com/ceph/ceph/pull/61923>`_, Adam King)
* mgr/cephadm: fix flake8 test failures (`pr#58076 <https://github.com/ceph/ceph/pull/58076>`_, Nizamudeen A)
* mgr/cephadm: fix typo with vrrp_interfaces in keepalive setup (`pr#61904 <https://github.com/ceph/ceph/pull/61904>`_, Adam King)
* mgr/cephadm: make client-keyring deploying ceph.conf optional (`pr#59451 <https://github.com/ceph/ceph/pull/59451>`_, Adam King)
* mgr/cephadm: make setting --cgroups=split configurable for adopted daemons (`pr#59460 <https://github.com/ceph/ceph/pull/59460>`_, Gilad Sid)
* mgr/cephadm: make SMB and NVMEoF upgrade last in staggered upgrade (`pr#59462 <https://github.com/ceph/ceph/pull/59462>`_, Adam King)
* mgr/cephadm: mgr orchestrator module raise exception if there is trailing tab in yaml file (`pr#61921 <https://github.com/ceph/ceph/pull/61921>`_, Shweta Bhosale)
* mgr/cephadm: set OSD cap for NVMEoF daemon to "profile rbd" (`pr#57234 <https://github.com/ceph/ceph/pull/57234>`_, Adam King)
* mgr/cephadm: Update multi-site configs before deploying  daemons on rgw service create (`pr#60350 <https://github.com/ceph/ceph/pull/60350>`_, Aashish Sharma)
* mgr/cephadm: use double quotes for NFSv4 RecoveryBackend in ganesha conf (`pr#61924 <https://github.com/ceph/ceph/pull/61924>`_, Adam King)
* mgr/cephadm: use host address while updating rgw zone endpoints (`pr#59947 <https://github.com/ceph/ceph/pull/59947>`_, Aashish Sharma)
* mgr/dashboard: add a custom warning message when enabling feature (`pr#61038 <https://github.com/ceph/ceph/pull/61038>`_, Nizamudeen A)
* mgr/dashboard: add absolute path validation for pseudo path of nfs export (`pr#57637 <https://github.com/ceph/ceph/pull/57637>`_, avanthakkar)
* mgr/dashboard: add cephfs rename REST API (`pr#60729 <https://github.com/ceph/ceph/pull/60729>`_, Yite Gu)
* mgr/dashboard: add dueTime to rgw bucket validator (`pr#58247 <https://github.com/ceph/ceph/pull/58247>`_, Nizamudeen A)
* mgr/dashboard: add NFS export button for subvolume/ grp (`pr#58657 <https://github.com/ceph/ceph/pull/58657>`_, Avan Thakkar)
* mgr/dashboard: add prometheus federation config for mullti-cluster monitoring (`pr#57255 <https://github.com/ceph/ceph/pull/57255>`_, Aashish Sharma)
* mgr/dashboard: Administration > Configuration > Some of the config options are not updatable at runtime (`pr#61182 <https://github.com/ceph/ceph/pull/61182>`_, Naman Munet)
* mgr/dashboard: bump follow-redirects from 1.15.3 to 1.15.6 in /src/pybind/mgr/dashboard/frontend (`pr#56877 <https://github.com/ceph/ceph/pull/56877>`_, dependabot[bot])
* mgr/dashboard: Changes for Sign out text to Login out (`pr#58989 <https://github.com/ceph/ceph/pull/58989>`_, Prachi Goel)
* mgr/dashboard: Cloning subvolume not listing _nogroup if no subvolume (`pr#59952 <https://github.com/ceph/ceph/pull/59952>`_, Dnyaneshwari talwekar)
* mgr/dashboard: critical confirmation modal changes (`pr#61980 <https://github.com/ceph/ceph/pull/61980>`_, Naman Munet)
* mgr/dashboard: disable deleting bucket with objects (`pr#61973 <https://github.com/ceph/ceph/pull/61973>`_, Naman Munet)
* mgr/dashboard: exclude cloned-deleted RBD snaps (`pr#57219 <https://github.com/ceph/ceph/pull/57219>`_, Ernesto Puerta)
* mgr/dashboard: fix clone async validators with different groups (`pr#58338 <https://github.com/ceph/ceph/pull/58338>`_, Nizamudeen A)
* mgr/dashboard: fix dashboard not visible on disabled anonymous access (`pr#56965 <https://github.com/ceph/ceph/pull/56965>`_, Nizamudeen A)
* mgr/dashboard: fix doc links in rgw-multisite (`pr#60155 <https://github.com/ceph/ceph/pull/60155>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix duplicate grafana panels when on mgr failover (`pr#56929 <https://github.com/ceph/ceph/pull/56929>`_, Avan Thakkar)
* mgr/dashboard: fix edit bucket failing in other selected gateways (`pr#58245 <https://github.com/ceph/ceph/pull/58245>`_, Nizamudeen A)
* mgr/dashboard: fix handling NaN values in dashboard charts (`pr#59962 <https://github.com/ceph/ceph/pull/59962>`_, Aashish Sharma)
* mgr/dashboard: Fix Latency chart data units in rgw overview page (`pr#61237 <https://github.com/ceph/ceph/pull/61237>`_, Aashish Sharma)
* mgr/dashboard: fix readonly landingpage (`pr#57752 <https://github.com/ceph/ceph/pull/57752>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix setting compression type while editing rgw zone (`pr#59971 <https://github.com/ceph/ceph/pull/59971>`_, Aashish Sharma)
* mgr/dashboard: fix snap schedule delete retention (`pr#56862 <https://github.com/ceph/ceph/pull/56862>`_, Ivo Almeida)
* mgr/dashboard: fix total objects/Avg object size in RGW Overview Page (`pr#61458 <https://github.com/ceph/ceph/pull/61458>`_, Aashish Sharma)
* mgr/dashboard: Fix variable capitalization in embedded rbd-details panel (`pr#62209 <https://github.com/ceph/ceph/pull/62209>`_, Juan Ferrer Toribio)
* mgr/dashboard: Forbid snapshot name "." and any containing "/" (`pr#59994 <https://github.com/ceph/ceph/pull/59994>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: handle infinite values for pools (`pr#61097 <https://github.com/ceph/ceph/pull/61097>`_, Afreen)
* mgr/dashboard: introduce server side pagination for osds (`pr#60295 <https://github.com/ceph/ceph/pull/60295>`_, Nizamudeen A)
* mgr/dashboard: Move features to advanced section and expand by default rbd config section (`pr#56921 <https://github.com/ceph/ceph/pull/56921>`_, Afreen)
* mgr/dashboard: nfs export enhancement for CEPHFS (`pr#58475 <https://github.com/ceph/ceph/pull/58475>`_, Avan Thakkar)
* mgr/dashboard: pin lxml to fix run-dashboard-tox-make-check failure (`pr#62256 <https://github.com/ceph/ceph/pull/62256>`_, Nizamudeen A)
* mgr/dashboard: remove cherrypy_backports.py (`pr#60633 <https://github.com/ceph/ceph/pull/60633>`_, Nizamudeen A)
* mgr/dashboard: remove minutely from retention (`pr#56917 <https://github.com/ceph/ceph/pull/56917>`_, Ivo Almeida)
* mgr/dashboard: remove orch required decorator from host UI router (list) (`pr#59852 <https://github.com/ceph/ceph/pull/59852>`_, Naman Munet)
* mgr/dashboard: service form hosts selection only show up to 10 entries (`pr#59761 <https://github.com/ceph/ceph/pull/59761>`_, Naman Munet)
* mgr/dashboard: snapshot schedule repeat frequency validation (`pr#56880 <https://github.com/ceph/ceph/pull/56880>`_, Ivo Almeida)
* mgr/dashboard: Update and correct zonegroup delete notification (`pr#61236 <https://github.com/ceph/ceph/pull/61236>`_, Aashish Sharma)
* mgr/dashboard: update period after migrating to multi-site (`pr#59963 <https://github.com/ceph/ceph/pull/59963>`_, Aashish Sharma)
* mgr/dashboard: update translations for reef (`pr#60358 <https://github.com/ceph/ceph/pull/60358>`_, Nizamudeen A)
* mgr/dashboard: When configuring the RGW Multisite endpoints from the UI allow FQDN(Not only IP) (`pr#62354 <https://github.com/ceph/ceph/pull/62354>`_, Aashish Sharma)
* mgr/dashboard: Wrong(half) uid is observed in dashboard (`pr#59876 <https://github.com/ceph/ceph/pull/59876>`_, Dnyaneshwari Talwekar)
* mgr/dashboard: Zone details showing incorrect data for data pool values and compression info for Storage Classes (`pr#59877 <https://github.com/ceph/ceph/pull/59877>`_, Aashish Sharma)
* mgr/diskprediction_local: avoid more mypy errors (`pr#62369 <https://github.com/ceph/ceph/pull/62369>`_, John Mulligan)
* mgr/diskprediction_local: avoid mypy error (`pr#61292 <https://github.com/ceph/ceph/pull/61292>`_, John Mulligan)
* mgr/k8sevents: update V1Events to CoreV1Events (`pr#57994 <https://github.com/ceph/ceph/pull/57994>`_, Nizamudeen A)
* mgr/Mgr.cc: clear daemon health metrics instead of removing down/out osd from daemon state (`pr#58513 <https://github.com/ceph/ceph/pull/58513>`_, Cory Snyder)
* mgr/nfs: Don't crash ceph-mgr if NFS clusters are unavailable (`pr#58283 <https://github.com/ceph/ceph/pull/58283>`_, Anoop C S, Ponnuvel Palaniyappan)
* mgr/nfs: scrape nfs monitoring endpoint (`pr#61719 <https://github.com/ceph/ceph/pull/61719>`_, avanthakkar)
* mgr/orchestrator: fix encrypted flag handling in orch daemon add osd (`pr#61720 <https://github.com/ceph/ceph/pull/61720>`_, Yonatan Zaken)
* mgr/pybind/object_format: fix json-pretty being marked invalid (`pr#59458 <https://github.com/ceph/ceph/pull/59458>`_, Adam King)
* mgr/rest: Trim  requests array and limit size (`pr#59371 <https://github.com/ceph/ceph/pull/59371>`_, Nitzan Mordechai)
* mgr/rgw: Adding a retry config while calling zone_create() (`pr#61717 <https://github.com/ceph/ceph/pull/61717>`_, Kritik Sachdeva)
* mgr/rgw: fix error handling in rgw zone create (`pr#61713 <https://github.com/ceph/ceph/pull/61713>`_, Adam King)
* mgr/rgw: fix setting rgw realm token in secondary site rgw spec (`pr#61715 <https://github.com/ceph/ceph/pull/61715>`_, Adam King)
* mgr/snap_schedule: correctly fetch mds_max_snaps_per_dir from mds (`pr#59648 <https://github.com/ceph/ceph/pull/59648>`_, Milind Changire)
* mgr/snap_schedule: restore yearly spec to lowercase y (`pr#57446 <https://github.com/ceph/ceph/pull/57446>`_, Milind Changire)
* mgr/stats: initialize mx_last_updated in FSPerfStats (`pr#57441 <https://github.com/ceph/ceph/pull/57441>`_, Jos Collin)
* mgr/status: Fix 'fs status' json output (`pr#60188 <https://github.com/ceph/ceph/pull/60188>`_, Kotresh HR)
* mgr/vol : shortening the name of helper method (`pr#60369 <https://github.com/ceph/ceph/pull/60369>`_, Neeraj Pratap Singh)
* mgr/vol: handle case where clone index entry goes missing (`pr#58556 <https://github.com/ceph/ceph/pull/58556>`_, Rishabh Dave)
* mgr: fix subuser creation via dashboard (`pr#62087 <https://github.com/ceph/ceph/pull/62087>`_, Hannes Baum)
* mgr: remove out&down osd from mgr daemons (`pr#54533 <https://github.com/ceph/ceph/pull/54533>`_, shimin)
* Modify container/ software to support release containers and the promotion of prerelease containers (`pr#60961 <https://github.com/ceph/ceph/pull/60961>`_, Dan Mick)
* mon, osd, \*: expose upmap-primary in OSDMap::get_features() (`pr#57794 <https://github.com/ceph/ceph/pull/57794>`_, Radoslaw Zarzynski)
* mon, osd: add command to remove invalid pg-upmap-primary entries (`pr#62191 <https://github.com/ceph/ceph/pull/62191>`_, Laura Flores)
* mon, qa: suites override ec profiles with --yes_i_really_mean_it; monitors accept that (`pr#59274 <https://github.com/ceph/ceph/pull/59274>`_, Radoslaw Zarzynski, Radosław Zarzyński)
* mon,cephfs: require confirmation flag to bring down unhealthy MDS (`pr#57837 <https://github.com/ceph/ceph/pull/57837>`_, Rishabh Dave)
* mon/ElectionLogic: tie-breaker mon ignore proposal from marked down mon (`pr#58687 <https://github.com/ceph/ceph/pull/58687>`_, Kamoltat)
* mon/LogMonitor: Use generic cluster log level config (`pr#57495 <https://github.com/ceph/ceph/pull/57495>`_, Prashant D)
* mon/MDSMonitor: fix assert crash in `fs swap` (`pr#57373 <https://github.com/ceph/ceph/pull/57373>`_, Patrick Donnelly)
* mon/MonClient: handle ms_handle_fast_authentication return (`pr#59307 <https://github.com/ceph/ceph/pull/59307>`_, Patrick Donnelly)
* mon/MonmapMonitor: do not propose on error in prepare_update (`pr#56400 <https://github.com/ceph/ceph/pull/56400>`_, Patrick Donnelly)
* mon/OSDMonitor: Add force-remove-snap mon command (`pr#59404 <https://github.com/ceph/ceph/pull/59404>`_, Matan Breizman)
* mon/OSDMonitor: fix rmsnap command (`pr#56431 <https://github.com/ceph/ceph/pull/56431>`_, Matan Breizman)
* mon/OSDMonitor: relax cap enforcement for unmanaged snapshots (`pr#61602 <https://github.com/ceph/ceph/pull/61602>`_, Ilya Dryomov)
* mon/scrub: log error details of store access failures (`pr#61345 <https://github.com/ceph/ceph/pull/61345>`_, Yite Gu)
* mon: add created_at and ceph_version_when_created meta (`pr#56681 <https://github.com/ceph/ceph/pull/56681>`_, Ryotaro Banno)
* mon: do not log MON_DOWN if monitor uptime is less than threshold (`pr#56408 <https://github.com/ceph/ceph/pull/56408>`_, Patrick Donnelly)
* mon: fix `fs set down` to adjust max_mds only when cluster is not down (`pr#59705 <https://github.com/ceph/ceph/pull/59705>`_, chungfengz)
* mon: Remove any pg_upmap_primary mapping during remove a pool (`pr#59270 <https://github.com/ceph/ceph/pull/59270>`_, Mohit Agrawal)
* mon: stuck peering since warning is misleading (`pr#57408 <https://github.com/ceph/ceph/pull/57408>`_, shreyanshjain7174)
* mon: validate also mons and osds on {rm-,}pg-upmap-primary (`pr#59275 <https://github.com/ceph/ceph/pull/59275>`_, Radosław Zarzyński)
* msg/async: Encode message once features are set (`pr#59286 <https://github.com/ceph/ceph/pull/59286>`_, Aishwarya Mathuria)
* msg/AsyncMessenger: re-evaluate the stop condition when woken up in 'wait()' (`pr#53717 <https://github.com/ceph/ceph/pull/53717>`_, Leonid Usov)
* msg: always generate random nonce; don't try to reuse PID (`pr#53269 <https://github.com/ceph/ceph/pull/53269>`_, Radoslaw Zarzynski)
* msg: insert PriorityDispatchers in sorted position (`pr#61507 <https://github.com/ceph/ceph/pull/61507>`_, Casey Bodley)
* node-proxy: make the daemon discover endpoints (`pr#58483 <https://github.com/ceph/ceph/pull/58483>`_, Guillaume Abrioux)
* nofail option in fstab not supported (`pr#52985 <https://github.com/ceph/ceph/pull/52985>`_, Leonid Usov)
* orch: refactor boolean handling in drive group spec (`pr#61914 <https://github.com/ceph/ceph/pull/61914>`_, Guillaume Abrioux)
* os/bluestore: add perfcount for bluestore/bluefs allocator (`pr#59103 <https://github.com/ceph/ceph/pull/59103>`_, Yite Gu)
* os/bluestore: add some slow count for bluestore (`pr#59104 <https://github.com/ceph/ceph/pull/59104>`_, Yite Gu)
* os/bluestore: allow use BtreeAllocator (`pr#59499 <https://github.com/ceph/ceph/pull/59499>`_, tan changzhi)
* os/bluestore: enable async manual compactions (`pr#58741 <https://github.com/ceph/ceph/pull/58741>`_, Igor Fedotov)
* os/bluestore: expand BlueFS log if available space is insufficient (`pr#57241 <https://github.com/ceph/ceph/pull/57241>`_, Pere Diaz Bou)
* os/bluestore: Fix BlueRocksEnv attempts to use POSIX (`pr#61112 <https://github.com/ceph/ceph/pull/61112>`_, Adam Kupczyk)
* os/bluestore: fix btree allocator (`pr#59264 <https://github.com/ceph/ceph/pull/59264>`_, Igor Fedotov)
* os/bluestore: fix crash caused by dividing by 0 (`pr#57197 <https://github.com/ceph/ceph/pull/57197>`_, Jrchyang Yu)
* os/bluestore: fix the problem of l_bluefs_log_compactions double recording (`pr#57194 <https://github.com/ceph/ceph/pull/57194>`_, Wang Linke)
* os/bluestore: fix the problem that _estimate_log_size_N calculates the log size incorrectly (`pr#61892 <https://github.com/ceph/ceph/pull/61892>`_, Wang Linke)
* os/bluestore: Improve documentation introduced by #57722 (`pr#60894 <https://github.com/ceph/ceph/pull/60894>`_, Anthony D'Atri)
* os/bluestore: Make truncate() drop unused allocations (`pr#60237 <https://github.com/ceph/ceph/pull/60237>`_, Adam Kupczyk, Igor Fedotov)
* os/bluestore: set rocksdb iterator bounds for Bluestore::_collection_list() (`pr#57625 <https://github.com/ceph/ceph/pull/57625>`_, Cory Snyder)
* os/bluestore: Warning added for slow operations and stalled read (`pr#59466 <https://github.com/ceph/ceph/pull/59466>`_, Md Mahamudur Rahaman Sajib)
* os/store_test: Retune tests to current code (`pr#56139 <https://github.com/ceph/ceph/pull/56139>`_, Adam Kupczyk)
* os: introduce ObjectStore::refresh_perf_counters() method (`pr#55136 <https://github.com/ceph/ceph/pull/55136>`_, Igor Fedotov)
* os: remove unused btrfs_ioctl.h and tests (`pr#60612 <https://github.com/ceph/ceph/pull/60612>`_, Casey Bodley)
* osd/OSDMonitor: check svc is writeable before changing pending (`pr#57067 <https://github.com/ceph/ceph/pull/57067>`_, Patrick Donnelly)
* osd/PeeringState: introduce osd_skip_check_past_interval_bounds (`pr#60284 <https://github.com/ceph/ceph/pull/60284>`_, Matan Breizman)
* osd/perf_counters: raise prio of before queue op perfcounter (`pr#59105 <https://github.com/ceph/ceph/pull/59105>`_, Yite Gu)
* osd/scheduler: add mclock queue length perfcounter (`pr#59034 <https://github.com/ceph/ceph/pull/59034>`_, zhangjianwei2)
* osd/scrub: Change scrub cost to average object size (`pr#59629 <https://github.com/ceph/ceph/pull/59629>`_, Aishwarya Mathuria)
* osd/scrub: decrease default deep scrub chunk size (`pr#59792 <https://github.com/ceph/ceph/pull/59792>`_, Ronen Friedman)
* osd/scrub: reduce osd_requested_scrub_priority default value (`pr#59886 <https://github.com/ceph/ceph/pull/59886>`_, Ronen Friedman)
* osd/SnapMapper: fix _lookup_purged_snap (`pr#56813 <https://github.com/ceph/ceph/pull/56813>`_, Matan Breizman)
* osd/TrackedOp: Fix TrackedOp event order (`pr#59108 <https://github.com/ceph/ceph/pull/59108>`_, YiteGu)
* osd: Add memstore to unsupported objstores for QoS (`pr#59285 <https://github.com/ceph/ceph/pull/59285>`_, Aishwarya Mathuria)
* osd: adding 'reef' to pending_require_osd_release (`pr#60981 <https://github.com/ceph/ceph/pull/60981>`_, Philipp Hufangl)
* osd: always send returnvec-on-errors for client's retry (`pr#59273 <https://github.com/ceph/ceph/pull/59273>`_, Radoslaw Zarzynski)
* osd: avoid watcher remains after "rados watch" is interrupted (`pr#58846 <https://github.com/ceph/ceph/pull/58846>`_, weixinwei)
* osd: bump versions of decoders for upmap-primary (`pr#58802 <https://github.com/ceph/ceph/pull/58802>`_, Radoslaw Zarzynski)
* osd: CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE flag is passed from ECBackend (`pr#57621 <https://github.com/ceph/ceph/pull/57621>`_, Md Mahamudur Rahaman Sajib)
* osd: Change PG Deletion cost for mClock (`pr#56475 <https://github.com/ceph/ceph/pull/56475>`_, Aishwarya Mathuria)
* osd: do not assert on fast shutdown timeout (`pr#55135 <https://github.com/ceph/ceph/pull/55135>`_, Igor Fedotov)
* osd: ensure async recovery does not drop a pg below min_size (`pr#54550 <https://github.com/ceph/ceph/pull/54550>`_, Samuel Just)
* osd: fix for segmentation fault on OSD fast shutdown (`pr#57615 <https://github.com/ceph/ceph/pull/57615>`_, Md Mahamudur Rahaman Sajib)
* osd: full-object read CRC mismatch due to 'truncate' modifying oi.size w/o clearing 'data_digest' (`pr#57588 <https://github.com/ceph/ceph/pull/57588>`_, Samuel Just, Matan Breizman, Nitzan Mordechai, jiawd)
* osd: make _set_cache_sizes ratio aware of cache_kv_onode_ratio (`pr#55220 <https://github.com/ceph/ceph/pull/55220>`_, Raimund Sacherer)
* osd: optimize extent comparison in PrimaryLogPG (`pr#61336 <https://github.com/ceph/ceph/pull/61336>`_, Dongdong Tao)
* osd: Report health error if OSD public address is not within subnet (`pr#55697 <https://github.com/ceph/ceph/pull/55697>`_, Prashant D)
* pybind/ceph_argparse: Fix error message for ceph tell command (`pr#59197 <https://github.com/ceph/ceph/pull/59197>`_, Neeraj Pratap Singh)
* pybind/mgr/mirroring: Fix KeyError: 'directory_count' in daemon status (`pr#57763 <https://github.com/ceph/ceph/pull/57763>`_, Jos Collin)
* pybind/mgr: disable sqlite3/python autocommit (`pr#57190 <https://github.com/ceph/ceph/pull/57190>`_, Patrick Donnelly)
* pybind/rados: fix missed changes for PEP484 style type annotations (`pr#54358 <https://github.com/ceph/ceph/pull/54358>`_, Igor Fedotov)
* pybind/rbd: expose CLONE_FORMAT and FLATTEN image options (`pr#57309 <https://github.com/ceph/ceph/pull/57309>`_, Ilya Dryomov)
* python-common: fix valid_addr on python 3.11 (`pr#61947 <https://github.com/ceph/ceph/pull/61947>`_, John Mulligan)
* python-common: handle "anonymous_access: false" in to_json of Grafana spec (`pr#59457 <https://github.com/ceph/ceph/pull/59457>`_, Adam King)
* qa/cephadm: use reef image as default for test_cephadm workunit (`pr#56714 <https://github.com/ceph/ceph/pull/56714>`_, Adam King)
* qa/cephadm: wait a bit before checking rgw daemons upgraded w/ `ceph versions` (`pr#61917 <https://github.com/ceph/ceph/pull/61917>`_, Adam King)
* qa/cephfs: a bug fix and few missing backport for caps_helper.py (`pr#58340 <https://github.com/ceph/ceph/pull/58340>`_, Rishabh Dave)
* qa/cephfs: add mgr debugging (`pr#56415 <https://github.com/ceph/ceph/pull/56415>`_, Patrick Donnelly)
* qa/cephfs: add more ignorelist entries (`issue#64746 <http://tracker.ceph.com/issues/64746>`_, `pr#56022 <https://github.com/ceph/ceph/pull/56022>`_, Venky Shankar)
* qa/cephfs: add probabilistic ignorelist for pg_health (`pr#56666 <https://github.com/ceph/ceph/pull/56666>`_, Patrick Donnelly)
* qa/cephfs: CephFSTestCase.create_client() must keyring (`pr#56836 <https://github.com/ceph/ceph/pull/56836>`_, Rishabh Dave)
* qa/cephfs: fix test_single_path_authorize_on_nonalphanumeric_fsname (`pr#58560 <https://github.com/ceph/ceph/pull/58560>`_, Rishabh Dave)
* qa/cephfs: fix TestRenameCommand and unmount the clinet before failin… (`pr#59399 <https://github.com/ceph/ceph/pull/59399>`_, Xiubo Li)
* qa/cephfs: ignore variant of MDS_UP_LESS_THAN_MAX (`pr#58789 <https://github.com/ceph/ceph/pull/58789>`_, Patrick Donnelly)
* qa/cephfs: ignore when specific OSD is reported down during upgrade (`pr#60390 <https://github.com/ceph/ceph/pull/60390>`_, Rishabh Dave)
* qa/cephfs: ignorelist clog of MDS_UP_LESS_THAN_MAX (`pr#56403 <https://github.com/ceph/ceph/pull/56403>`_, Patrick Donnelly)
* qa/cephfs: improvements for "mds fail" and "fs fail" (`pr#58563 <https://github.com/ceph/ceph/pull/58563>`_, Rishabh Dave)
* qa/cephfs: remove dependency on centos8/rhel8 entirely (`pr#59054 <https://github.com/ceph/ceph/pull/59054>`_, Venky Shankar)
* qa/cephfs: switch to ubuntu 22.04 for stock kernel testing (`pr#62492 <https://github.com/ceph/ceph/pull/62492>`_, Venky Shankar)
* qa/cephfs: use different config options to generate MDS_TRIM (`pr#59375 <https://github.com/ceph/ceph/pull/59375>`_, Rishabh Dave)
* qa/distros: reinstall nvme-cli on centos 9 nodes (`pr#59463 <https://github.com/ceph/ceph/pull/59463>`_, Adam King)
* qa/distros: remove centos 8 from supported distros (`pr#57932 <https://github.com/ceph/ceph/pull/57932>`_, Guillaume Abrioux, Casey Bodley, Adam King, Laura Flores)
* qa/fsx: use a specified sha1 to build the xfstest-dev (`pr#57557 <https://github.com/ceph/ceph/pull/57557>`_, Xiubo Li)
* qa/mgr/dashboard: fix test race condition (`pr#59697 <https://github.com/ceph/ceph/pull/59697>`_, Nizamudeen A, Ernesto Puerta)
* qa/multisite: add boto3.client to the library (`pr#60850 <https://github.com/ceph/ceph/pull/60850>`_, Shilpa Jagannath)
* qa/rgw/crypt: disable failing kmip testing (`pr#60701 <https://github.com/ceph/ceph/pull/60701>`_, Casey Bodley)
* qa/rgw/sts: keycloak task installs java manually (`pr#60418 <https://github.com/ceph/ceph/pull/60418>`_, Casey Bodley)
* qa/rgw: avoid 'user rm' of keystone users (`pr#62104 <https://github.com/ceph/ceph/pull/62104>`_, Casey Bodley)
* qa/rgw: barbican uses branch stable/2023.1 (`pr#56819 <https://github.com/ceph/ceph/pull/56819>`_, Casey Bodley)
* qa/rgw: bump keystone/barbican from 2023.1 to 2024.1 (`pr#61022 <https://github.com/ceph/ceph/pull/61022>`_, Casey Bodley)
* qa/rgw: fix s3 java tests by forcing gradle to run on Java 8 (`pr#61054 <https://github.com/ceph/ceph/pull/61054>`_, J. Eric Ivancich)
* qa/rgw: force Hadoop to run under Java 1.8 (`pr#61121 <https://github.com/ceph/ceph/pull/61121>`_, J. Eric Ivancich)
* qa/rgw: pull Apache artifacts from mirror instead of archive.apache.org (`pr#61102 <https://github.com/ceph/ceph/pull/61102>`_, J. Eric Ivancich)
* qa/standalone/mon/mon_cluster_log.sh: retry check for log line (`pr#60780 <https://github.com/ceph/ceph/pull/60780>`_, Shraddha Agrawal, Naveen Naidu)
* qa/standalone/scrub: increase status updates frequency (`pr#59975 <https://github.com/ceph/ceph/pull/59975>`_, Ronen Friedman)
* qa/suites/krbd: drop pre-single-major and move "layering only" coverage (`pr#57464 <https://github.com/ceph/ceph/pull/57464>`_, Ilya Dryomov)
* qa/suites/krbd: stress test for recovering from watch errors for -o exclusive (`pr#58856 <https://github.com/ceph/ceph/pull/58856>`_, Ilya Dryomov)
* qa/suites/rados/singleton: add POOL_APP_NOT_ENABLED to ignorelist (`pr#57487 <https://github.com/ceph/ceph/pull/57487>`_, Laura Flores)
* qa/suites/rados/thrash-old-clients: update supported releases and distro (`pr#57999 <https://github.com/ceph/ceph/pull/57999>`_, Laura Flores)
* qa/suites/rados/thrash/workloads: remove cache tiering workload (`pr#58413 <https://github.com/ceph/ceph/pull/58413>`_, Laura Flores)
* qa/suites/rados/verify/validater/valgrind: increase op thread timeout (`pr#54527 <https://github.com/ceph/ceph/pull/54527>`_, Matan Breizman)
* qa/suites/rados/verify/validater: increase heartbeat grace timeout (`pr#58786 <https://github.com/ceph/ceph/pull/58786>`_, Sridhar Seshasayee)
* qa/suites/rados: Cancel injectfull to allow cleanup (`pr#59157 <https://github.com/ceph/ceph/pull/59157>`_, Brad Hubbard)
* qa/suites/rbd/iscsi: enable all supported container hosts (`pr#60088 <https://github.com/ceph/ceph/pull/60088>`_, Ilya Dryomov)
* qa/suites/rbd: override extra_system_packages directly on install task (`pr#57765 <https://github.com/ceph/ceph/pull/57765>`_, Ilya Dryomov)
* qa/suites/upgrade/reef-p2p/reef-p2p-parallel: increment upgrade to 18.2.2 (`pr#58411 <https://github.com/ceph/ceph/pull/58411>`_, Laura Flores)
* qa/suites: add "mon down" log variations to ignorelist (`pr#61711 <https://github.com/ceph/ceph/pull/61711>`_, Laura Flores)
* qa/suites: drop --show-reachable=yes from fs:valgrind tests (`pr#59069 <https://github.com/ceph/ceph/pull/59069>`_, Jos Collin)
* qa/tasks/ceph_manager.py: Rewrite test_pool_min_size (`pr#59268 <https://github.com/ceph/ceph/pull/59268>`_, Kamoltat)
* qa/tasks/cephadm: enable mon_cluster_log_to_file (`pr#55431 <https://github.com/ceph/ceph/pull/55431>`_, Dan van der Ster)
* qa/tasks/nvme_loop: update task to work with new nvme list format (`pr#61027 <https://github.com/ceph/ceph/pull/61027>`_, Adam King)
* qa/tasks/qemu: Fix OS version comparison (`pr#58170 <https://github.com/ceph/ceph/pull/58170>`_, Zack Cerza)
* qa/tasks: Include stderr on tasks badness check (`pr#61434 <https://github.com/ceph/ceph/pull/61434>`_, Christopher Hoffman, Ilya Dryomov)
* qa/tasks: watchdog should terminate thrasher (`pr#59193 <https://github.com/ceph/ceph/pull/59193>`_, Nitzan Mordechai)
* qa/tests: added client-upgrade-reef-squid tests (`pr#58447 <https://github.com/ceph/ceph/pull/58447>`_, Yuri Weinstein)
* qa/upgrade: fix checks to make sure upgrade is still in progress (`pr#61718 <https://github.com/ceph/ceph/pull/61718>`_, Adam King)
* qa/workunits/rbd: avoid caching effects in luks-encryption.sh (`pr#58853 <https://github.com/ceph/ceph/pull/58853>`_, Ilya Dryomov)
* qa/workunits/rbd: wait for resize to be applied in rbd-nbd (`pr#62218 <https://github.com/ceph/ceph/pull/62218>`_, Ilya Dryomov)
* qa: account for rbd_trash object in krbd_data_pool.sh + related ceph{,adm} task fixes (`pr#58540 <https://github.com/ceph/ceph/pull/58540>`_, Ilya Dryomov)
* qa: add a YAML to ignore MGR_DOWN warning (`pr#57565 <https://github.com/ceph/ceph/pull/57565>`_, Dhairya Parmar)
* qa: Add multifs root_squash testcase (`pr#56690 <https://github.com/ceph/ceph/pull/56690>`_, Rishabh Dave, Kotresh HR)
* qa: add support/qa for cephfs-shell on CentOS 9 / RHEL9 (`pr#57162 <https://github.com/ceph/ceph/pull/57162>`_, Patrick Donnelly)
* qa: adjust expected io_opt in krbd_discard_granularity.t (`pr#59231 <https://github.com/ceph/ceph/pull/59231>`_, Ilya Dryomov)
* qa: barbican: restrict python packages with upper-constraints (`pr#59326 <https://github.com/ceph/ceph/pull/59326>`_, Tobias Urdin)
* qa: cleanup snapshots before subvolume delete (`pr#58332 <https://github.com/ceph/ceph/pull/58332>`_, Milind Changire)
* qa: disable mon_warn_on_pool_no_app in fs suite (`pr#57920 <https://github.com/ceph/ceph/pull/57920>`_, Patrick Donnelly)
* qa: do the set/get attribute on the remote filesystem (`pr#59828 <https://github.com/ceph/ceph/pull/59828>`_, Jos Collin)
* qa: enable debug logs for fs:cephadm:multivolume subsuite (`issue#66029 <http://tracker.ceph.com/issues/66029>`_, `pr#58157 <https://github.com/ceph/ceph/pull/58157>`_, Venky Shankar)
* qa: enhance per-client labelled perf counters test (`pr#58251 <https://github.com/ceph/ceph/pull/58251>`_, Jos Collin, Rishabh Dave)
* qa: failfast mount for better performance and unblock `fs volume ls` (`pr#59920 <https://github.com/ceph/ceph/pull/59920>`_, Milind Changire)
* qa: fix error reporting string in assert_cluster_log (`pr#55391 <https://github.com/ceph/ceph/pull/55391>`_, Dhairya Parmar)
* qa: fix krbd_msgr_segments and krbd_rxbounce failing on 8.stream (`pr#57030 <https://github.com/ceph/ceph/pull/57030>`_, Ilya Dryomov)
* qa: fix log errors for cephadm tests (`pr#58421 <https://github.com/ceph/ceph/pull/58421>`_, Guillaume Abrioux)
* qa: fixing tests in test_cephfs_shell.TestShellOpts (`pr#58111 <https://github.com/ceph/ceph/pull/58111>`_, Neeraj Pratap Singh)
* qa: ignore cluster warnings generated from forward-scrub task (`issue#48562 <http://tracker.ceph.com/issues/48562>`_, `pr#57611 <https://github.com/ceph/ceph/pull/57611>`_, Venky Shankar)
* qa: ignore container checkpoint/restore related selinux denials for centos9 (`issue#64616 <http://tracker.ceph.com/issues/64616>`_, `pr#56019 <https://github.com/ceph/ceph/pull/56019>`_, Venky Shankar)
* qa: ignore container checkpoint/restore related selinux denials for c… (`issue#67118 <http://tracker.ceph.com/issues/67118>`_, `issue#66640 <http://tracker.ceph.com/issues/66640>`_, `pr#58809 <https://github.com/ceph/ceph/pull/58809>`_, Venky Shankar)
* qa: ignore human-friendly POOL_APP_NOT_ENABLED in clog (`pr#56951 <https://github.com/ceph/ceph/pull/56951>`_, Patrick Donnelly)
* qa: ignore PG health warnings in CephFS QA (`pr#58172 <https://github.com/ceph/ceph/pull/58172>`_, Patrick Donnelly)
* qa: ignore variation of PG_DEGRADED health warning (`pr#58231 <https://github.com/ceph/ceph/pull/58231>`_, Patrick Donnelly)
* qa: ignore warnings variations (`pr#59618 <https://github.com/ceph/ceph/pull/59618>`_, Patrick Donnelly)
* qa: increase debugging for snap_schedule (`pr#57172 <https://github.com/ceph/ceph/pull/57172>`_, Patrick Donnelly)
* qa: increase the http postBuffer size and disable sslVerify (`pr#53628 <https://github.com/ceph/ceph/pull/53628>`_, Xiubo Li)
* qa: load all dirfrags before testing altname recovery (`pr#59522 <https://github.com/ceph/ceph/pull/59522>`_, Patrick Donnelly)
* qa: relocate subvol creation overrides and test (`pr#59923 <https://github.com/ceph/ceph/pull/59923>`_, Milind Changire)
* qa: suppress __trans_list_add valgrind warning (`pr#58791 <https://github.com/ceph/ceph/pull/58791>`_, Patrick Donnelly)
* qa: suppress Leak_StillReachable mon leak in centos 9 jobs (`pr#58692 <https://github.com/ceph/ceph/pull/58692>`_, Laura Flores)
* qa: switch to use the merge fragment for fscrypt (`pr#55857 <https://github.com/ceph/ceph/pull/55857>`_, Xiubo Li)
* qa: test test_kill_mdstable for all mount types (`pr#56953 <https://github.com/ceph/ceph/pull/56953>`_, Patrick Donnelly)
* qa: unmount clients before damaging the fs (`pr#57524 <https://github.com/ceph/ceph/pull/57524>`_, Patrick Donnelly)
* qa: use centos9 for fs:upgrade (`pr#58113 <https://github.com/ceph/ceph/pull/58113>`_, Venky Shankar, Dhairya Parmar)
* qa: wait for file creation before changing mode (`issue#67408 <http://tracker.ceph.com/issues/67408>`_, `pr#59686 <https://github.com/ceph/ceph/pull/59686>`_, Venky Shankar)
* rbd-mirror: clean up stale pool replayers and callouts better (`pr#57306 <https://github.com/ceph/ceph/pull/57306>`_, Ilya Dryomov)
* rbd-mirror: fix possible recursive lock of ImageReplayer::m_lock (`pr#62043 <https://github.com/ceph/ceph/pull/62043>`_, N Balachandran)
* rbd-mirror: use correct ioctx for namespace (`pr#59772 <https://github.com/ceph/ceph/pull/59772>`_, N Balachandran)
* rbd-nbd: use netlink interface by default (`pr#62175 <https://github.com/ceph/ceph/pull/62175>`_, Ilya Dryomov, Ramana Raja)
* rbd: "rbd bench" always writes the same byte (`pr#59501 <https://github.com/ceph/ceph/pull/59501>`_, Ilya Dryomov)
* rbd: amend "rbd {group,} rename" and "rbd mirror pool" command descriptions (`pr#59601 <https://github.com/ceph/ceph/pull/59601>`_, Ilya Dryomov)
* rbd: handle --{group,image}-namespace in "rbd group image {add,rm}" (`pr#61171 <https://github.com/ceph/ceph/pull/61171>`_, Ilya Dryomov)
* rbd: open images in read-only mode for "rbd mirror pool status --verbose" (`pr#61169 <https://github.com/ceph/ceph/pull/61169>`_, Ilya Dryomov)
* Revert "reef: rgw/amqp: lock erase and create connection before emplace" (`pr#59016 <https://github.com/ceph/ceph/pull/59016>`_, Rongqi Sun)
* Revert "rgw/auth: Fix the return code returned by AuthStrategy," (`pr#61405 <https://github.com/ceph/ceph/pull/61405>`_, Casey Bodley, Pritha Srivastava)
* rgw/abortmp: Race condition on AbortMultipartUpload (`pr#61133 <https://github.com/ceph/ceph/pull/61133>`_, Casey Bodley, Artem Vasilev)
* rgw/admin/notification: add command to dump notifications (`pr#58070 <https://github.com/ceph/ceph/pull/58070>`_, Yuval Lifshitz)
* rgw/amqp: lock erase and create connection before emplace (`pr#59018 <https://github.com/ceph/ceph/pull/59018>`_, Rongqi Sun)
* rgw/amqp: lock erase and create connection before emplace (`pr#58715 <https://github.com/ceph/ceph/pull/58715>`_, Rongqi Sun)
* rgw/archive: avoid duplicating objects when syncing from multiple zones (`pr#59341 <https://github.com/ceph/ceph/pull/59341>`_, Shilpa Jagannath)
* rgw/auth: ignoring signatures for HTTP OPTIONS calls (`pr#60455 <https://github.com/ceph/ceph/pull/60455>`_, Tobias Urdin)
* rgw/beast: fix crash observed in SSL stream.async_shutdown() (`pr#57425 <https://github.com/ceph/ceph/pull/57425>`_, Mark Kogan)
* rgw/http/client-side: disable curl path normalization (`pr#59258 <https://github.com/ceph/ceph/pull/59258>`_, Oguzhan Ozmen)
* rgw/http: finish_request() after logging errors (`pr#59440 <https://github.com/ceph/ceph/pull/59440>`_, Casey Bodley)
* rgw/iam: fix role deletion replication (`pr#59126 <https://github.com/ceph/ceph/pull/59126>`_, Alex Wojno)
* rgw/kafka: refactor topic creation to avoid rd_kafka_topic_name() (`pr#59764 <https://github.com/ceph/ceph/pull/59764>`_, Yuval Lifshitz)
* rgw/kafka: set message timeout to 5 seconds (`pr#56158 <https://github.com/ceph/ceph/pull/56158>`_, Yuval Lifshitz)
* rgw/lc: make lc worker thread name shorter (`pr#61485 <https://github.com/ceph/ceph/pull/61485>`_, lightmelodies)
* rgw/lua: add lib64 to the package search path (`pr#59343 <https://github.com/ceph/ceph/pull/59343>`_, Yuval Lifshitz)
* rgw/lua: add more info on package install errors (`pr#59127 <https://github.com/ceph/ceph/pull/59127>`_, Yuval Lifshitz)
* rgw/multisite: allow PutACL replication (`pr#58546 <https://github.com/ceph/ceph/pull/58546>`_, Shilpa Jagannath)
* rgw/multisite: avoid writing multipart parts to the bucket index log (`pr#57127 <https://github.com/ceph/ceph/pull/57127>`_, Juan Zhu)
* rgw/multisite: don't retain RGW_ATTR_OBJ_REPLICATION_TRACE attr on copy_object (`pr#58764 <https://github.com/ceph/ceph/pull/58764>`_, Shilpa Jagannath)
* rgw/multisite: Fix use-after-move in retry logic in logbacking (`pr#61329 <https://github.com/ceph/ceph/pull/61329>`_, Adam Emerson)
* rgw/multisite: metadata polling event based on unmodified mdlog_marker (`pr#60793 <https://github.com/ceph/ceph/pull/60793>`_, Shilpa Jagannath)
* rgw/notifications/test: fix rabbitmq and kafka issues in centos9 (`pr#58312 <https://github.com/ceph/ceph/pull/58312>`_, Yuval Lifshitz)
* rgw/notifications: cleanup all coroutines after sending the notification (`pr#59354 <https://github.com/ceph/ceph/pull/59354>`_, Yuval Lifshitz)
* rgw/rados: don't rely on IoCtx::get_last_version() for async ops (`pr#60097 <https://github.com/ceph/ceph/pull/60097>`_, Casey Bodley)
* rgw/rgw_rados: fix server side-copy orphans tail-objects (`pr#61367 <https://github.com/ceph/ceph/pull/61367>`_, Adam Kupczyk, Gabriel BenHanokh, Daniel Gryniewicz)
* rgw/s3select: s3select response handler refactor (`pr#57229 <https://github.com/ceph/ceph/pull/57229>`_, Seena Fallah, Gal Salomon)
* rgw/sts: changing identity to boost::none, when role policy (`pr#59346 <https://github.com/ceph/ceph/pull/59346>`_, Pritha Srivastava)
* rgw/sts: fix to disallow unsupported JWT algorithms (`pr#62046 <https://github.com/ceph/ceph/pull/62046>`_, Pritha Srivastava)
* rgw/swift: preserve dashes/underscores in swift user metadata names (`pr#56615 <https://github.com/ceph/ceph/pull/56615>`_, Juan Zhu, Ali Maredia)
* rgw/test/kafka: let consumer read events from the beginning (`pr#61595 <https://github.com/ceph/ceph/pull/61595>`_, Yuval Lifshitz)
* rgw: add versioning status during `radosgw-admin bucket stats` (`pr#59261 <https://github.com/ceph/ceph/pull/59261>`_, J. Eric Ivancich)
* rgw: append query string to redirect URL if present (`pr#61160 <https://github.com/ceph/ceph/pull/61160>`_, Seena Fallah)
* rgw: compatibility issues on BucketPublicAccessBlock (`pr#59125 <https://github.com/ceph/ceph/pull/59125>`_, Seena Fallah)
* rgw: cumulatively fix 6 AWS SigV4 request failure cases (`pr#58435 <https://github.com/ceph/ceph/pull/58435>`_, Zac Dover, Casey Bodley, Ali Maredia, Matt Benjamin)
* rgw: decrement qlen/qactive perf counters on error (`pr#59669 <https://github.com/ceph/ceph/pull/59669>`_, Mark Kogan)
* rgw: Delete stale entries in bucket indexes while deleting obj (`pr#61061 <https://github.com/ceph/ceph/pull/61061>`_, Shasha Lu)
* rgw: do not assert on thread name setting failures (`pr#58058 <https://github.com/ceph/ceph/pull/58058>`_, Yuval Lifshitz)
* rgw: fix bucket link operation (`pr#61052 <https://github.com/ceph/ceph/pull/61052>`_, Yehuda Sadeh)
* RGW: fix cloud-sync not being able to sync folders (`pr#56554 <https://github.com/ceph/ceph/pull/56554>`_, Gabriel Adrian Samfira)
* rgw: fix CompleteMultipart error handling regression (`pr#57301 <https://github.com/ceph/ceph/pull/57301>`_, Casey Bodley)
* rgw: fix data corruption when rados op return ETIMEDOUT (`pr#61093 <https://github.com/ceph/ceph/pull/61093>`_, Shasha Lu)
* rgw: Fix LC process stuck issue (`pr#61531 <https://github.com/ceph/ceph/pull/61531>`_, Soumya Koduri, Tongliang Deng)
* rgw: fix the Content-Length in response header of static website (`pr#60741 <https://github.com/ceph/ceph/pull/60741>`_, xiangrui meng)
* rgw: fix user.rgw.user-policy attr remove by modify user (`pr#59134 <https://github.com/ceph/ceph/pull/59134>`_, ivan)
* rgw: increase log level on abort_early (`pr#59124 <https://github.com/ceph/ceph/pull/59124>`_, Seena Fallah)
* rgw: invalidate and retry keystone admin token (`pr#59075 <https://github.com/ceph/ceph/pull/59075>`_, Tobias Urdin)
* rgw: keep the tails when copying object to itself (`pr#62656 <https://github.com/ceph/ceph/pull/62656>`_, Jane Zhu)
* rgw: link only radosgw with ALLOC_LIBS (`pr#60733 <https://github.com/ceph/ceph/pull/60733>`_, Matt Benjamin)
* rgw: load copy source bucket attrs in putobj (`pr#59415 <https://github.com/ceph/ceph/pull/59415>`_, Seena Fallah)
* rgw: modify string match_wildcards with fnmatch (`pr#57901 <https://github.com/ceph/ceph/pull/57901>`_, zhipeng li, Adam Emerson)
* rgw: optimize gc chain size calculation (`pr#58168 <https://github.com/ceph/ceph/pull/58168>`_, Wei Wang)
* rgw: S3 Delete Bucket Policy should return 204 on success (`pr#61432 <https://github.com/ceph/ceph/pull/61432>`_, Simon Jürgensmeyer)
* rgw: swift: tempurl fixes for ceph (`pr#59356 <https://github.com/ceph/ceph/pull/59356>`_, Casey Bodley, Marcus Watts)
* rgw: update options yaml file so LDAP uri isn't an invalid example (`pr#56721 <https://github.com/ceph/ceph/pull/56721>`_, J. Eric Ivancich)
* rgw: when there are a large number of multiparts, the unorder list result may miss objects (`pr#60745 <https://github.com/ceph/ceph/pull/60745>`_, J. Eric Ivancich)
* rgwfile: fix lock_guard decl (`pr#59351 <https://github.com/ceph/ceph/pull/59351>`_, Matt Benjamin)
* run-make-check: use get_processors in run-make-check script (`pr#58872 <https://github.com/ceph/ceph/pull/58872>`_, John Mulligan)
* src/ceph-volume/ceph_volume/devices/lvm/listing.py : lvm list filters with vg name (`pr#58998 <https://github.com/ceph/ceph/pull/58998>`_, Pierre Lemay)
* src/exporter: improve usage message (`pr#61332 <https://github.com/ceph/ceph/pull/61332>`_, Anthony D'Atri)
* src/mon/ConnectionTracker.cc: Fix dump function (`pr#60004 <https://github.com/ceph/ceph/pull/60004>`_, Kamoltat)
* src/pybind/mgr/pg_autoscaler/module.py: fix 'pg_autoscale_mode' output (`pr#59444 <https://github.com/ceph/ceph/pull/59444>`_, Kamoltat)
* suites: test should ignore osd_down warnings (`pr#59146 <https://github.com/ceph/ceph/pull/59146>`_, Nitzan Mordechai)
* test/cls_lock: expired lock before unlock and start check (`pr#59271 <https://github.com/ceph/ceph/pull/59271>`_, Nitzan Mordechai)
* test/lazy-omap-stats: Convert to boost::regex (`pr#57456 <https://github.com/ceph/ceph/pull/57456>`_, Brad Hubbard)
* test/librbd/fsx: switch to netlink interface for rbd-nbd (`pr#61259 <https://github.com/ceph/ceph/pull/61259>`_, Ilya Dryomov)
* test/librbd/test_notify.py: conditionally ignore some errors (`pr#62688 <https://github.com/ceph/ceph/pull/62688>`_, Ilya Dryomov)
* test/librbd: clean up unused TEST_COOKIE variable (`pr#58549 <https://github.com/ceph/ceph/pull/58549>`_, Rongqi Sun)
* test/rbd_mirror: clear Namespace::s_instance at the end of a test (`pr#61959 <https://github.com/ceph/ceph/pull/61959>`_, Ilya Dryomov)
* test/rbd_mirror: flush watch/notify callbacks in TestImageReplayer (`pr#61957 <https://github.com/ceph/ceph/pull/61957>`_, Ilya Dryomov)
* test/rgw/multisite: add meta checkpoint after bucket creation (`pr#60977 <https://github.com/ceph/ceph/pull/60977>`_, Casey Bodley)
* test/rgw/notification: use real ip address instead of localhost (`pr#59304 <https://github.com/ceph/ceph/pull/59304>`_, Yuval Lifshitz)
* test/rgw: address potential race condition in reshard testing (`pr#58793 <https://github.com/ceph/ceph/pull/58793>`_, J. Eric Ivancich)
* test/store_test: fix deferred writing test cases (`pr#55778 <https://github.com/ceph/ceph/pull/55778>`_, Igor Fedotov)
* test/store_test: fix DeferredWrite test when prefer_deferred_size=0 (`pr#56199 <https://github.com/ceph/ceph/pull/56199>`_, Igor Fedotov)
* test/store_test: get rid off assert_death (`pr#55774 <https://github.com/ceph/ceph/pull/55774>`_, Igor Fedotov)
* test/store_test: refactor spillover tests (`pr#55200 <https://github.com/ceph/ceph/pull/55200>`_, Igor Fedotov)
* test: ceph daemon command with asok path (`pr#61481 <https://github.com/ceph/ceph/pull/61481>`_, Nitzan Mordechai)
* test: Create ParallelPGMapper object before start threadpool (`pr#58920 <https://github.com/ceph/ceph/pull/58920>`_, Mohit Agrawal)
* Test: osd-recovery-space.sh extends the wait time for "recovery toofull" (`pr#59043 <https://github.com/ceph/ceph/pull/59043>`_, Nitzan Mordechai)
* teuthology/bluestore: Fix running of compressed tests (`pr#57094 <https://github.com/ceph/ceph/pull/57094>`_, Adam Kupczyk)
* tool/ceph-bluestore-tool: fix wrong keyword for 'free-fragmentation' … (`pr#62124 <https://github.com/ceph/ceph/pull/62124>`_, Igor Fedotov)
* tools/ceph_objectstore_tool: Support get/set/superblock (`pr#55015 <https://github.com/ceph/ceph/pull/55015>`_, Matan Breizman)
* tools/cephfs: recover alternate_name of dentries from journal (`pr#58232 <https://github.com/ceph/ceph/pull/58232>`_, Patrick Donnelly)
* tools/objectstore: check for wrong coll open_collection (`pr#58734 <https://github.com/ceph/ceph/pull/58734>`_, Pere Diaz Bou)
* valgrind: update suppression for SyscallParam under call_init (`pr#52611 <https://github.com/ceph/ceph/pull/52611>`_, Casey Bodley)
* win32_deps_build.sh: pin zlib tag (`pr#61630 <https://github.com/ceph/ceph/pull/61630>`_, Lucian Petrut)
* workunit/dencoder: dencoder test forward incompat fix (`pr#61750 <https://github.com/ceph/ceph/pull/61750>`_, NitzanMordhai, Nitzan Mordechai)

v18.2.4 Reef
============

This is the fourth backport release in the Reef series. We recommend that all users update to this release.

An early build of this release was accidentally exposed and packaged as 18.2.3 by the Debian project in April.
That 18.2.3 release should not be used. The official release was re-tagged as v18.2.4 to avoid
further confusion.

v18.2.4 container images, now based on CentOS 9, may be incompatible on older kernels (e.g., Ubuntu 18.04) due
to differences in thread creation methods. Users upgrading to v18.2.4 container images with older OS versions
may encounter crashes during `pthread_create`. For workarounds, refer to the related tracker. However, we recommend
upgrading your OS to avoid this unsupported combination.
Related tracker: https://tracker.ceph.com/issues/66989

Release Date
------------

July 24, 2024

Notable Changes
---------------

* RADOS: This release fixes a bug (https://tracker.ceph.com/issues/61948) where pre-reef clients were allowed
  to connect to the `pg-upmap-primary` (https://docs.ceph.com/en/reef/rados/operations/read-balancer/)
  interface despite users having set `require-min-compat-client=reef`, leading to an assert in the osds
  and mons. You are susceptible to this bug in Reef versions prior to 18.2.4 if 1) you are using an osdmap
  generated via the offline osdmaptool with the `--read` option or 2) you have explicitly generated pg-upmap-primary
  mappings with the CLI command. Please note that the fix is minimal and does not address corner cases such as
  adding a mapping in the middle of an upgrade or in a partially upgraded cluster (related trackers linked
  in https://tracker.ceph.com/issues/61948). As such, we recommend removing any existing pg-upmap-primary
  mappings until remaining issues are addressed in future point releases.
  See https://tracker.ceph.com/issues/61948#note-32 for instructions on how to remove existing
  pg-upmap-primary mappings.
* RBD: When diffing against the beginning of time (`fromsnapname == NULL`) in
  fast-diff mode (`whole_object == true` with `fast-diff` image feature enabled
  and valid), diff-iterate is now guaranteed to execute locally if exclusive
  lock is available.  This brings a dramatic performance improvement for QEMU
  live disk synchronization and backup use cases.
* RADOS: `get_pool_is_selfmanaged_snaps_mode` C++ API has been deprecated
  due to being prone to false negative results.  Its safer replacement is
  `pool_is_in_selfmanaged_snaps_mode`.
* RBD: The option ``--image-id`` has been added to `rbd children` CLI command,
  so it can be run for images in the trash.

Changelog
---------

* (reef) node-proxy: improve http error handling in fetch_oob_details (`pr#55538 <https://github.com/ceph/ceph/pull/55538>`_, Guillaume Abrioux)
* [rgw][lc][rgw_lifecycle_work_time] adjust timing if the configured end time is less than the start time (`pr#54866 <https://github.com/ceph/ceph/pull/54866>`_, Oguzhan Ozmen)
* add checking for rgw frontend init (`pr#54844 <https://github.com/ceph/ceph/pull/54844>`_, zhipeng li)
* admin/doc-requirements: bump Sphinx to 5.0.2 (`pr#55191 <https://github.com/ceph/ceph/pull/55191>`_, Nizamudeen A)
* backport of fixes for 63678 and 63694 (`pr#55104 <https://github.com/ceph/ceph/pull/55104>`_, Redouane Kachach)
* backport rook/mgr recent changes (`pr#55706 <https://github.com/ceph/ceph/pull/55706>`_, Redouane Kachach)
* ceph-menv:fix typo in README (`pr#55163 <https://github.com/ceph/ceph/pull/55163>`_, yu.wang)
* ceph-volume: add missing import (`pr#56259 <https://github.com/ceph/ceph/pull/56259>`_, Guillaume Abrioux)
* ceph-volume: fix a bug in _check_generic_reject_reasons (`pr#54705 <https://github.com/ceph/ceph/pull/54705>`_, Kim Minjong)
* ceph-volume: Fix migration from WAL to data with no DB (`pr#55497 <https://github.com/ceph/ceph/pull/55497>`_, Igor Fedotov)
* ceph-volume: fix mpath device support (`pr#53539 <https://github.com/ceph/ceph/pull/53539>`_, Guillaume Abrioux)
* ceph-volume: fix zap_partitions() in devices.lvm.zap (`pr#55477 <https://github.com/ceph/ceph/pull/55477>`_, Guillaume Abrioux)
* ceph-volume: fixes fallback to stat in is_device and is_partition (`pr#54629 <https://github.com/ceph/ceph/pull/54629>`_, Teoman ONAY)
* ceph-volume: update functional testing (`pr#56857 <https://github.com/ceph/ceph/pull/56857>`_, Guillaume Abrioux)
* ceph-volume: use 'no workqueue' options with dmcrypt (`pr#55335 <https://github.com/ceph/ceph/pull/55335>`_, Guillaume Abrioux)
* ceph-volume: Use safe accessor to get TYPE info (`pr#56323 <https://github.com/ceph/ceph/pull/56323>`_, Dillon Amburgey)
* ceph.spec.in: add support for openEuler OS (`pr#56361 <https://github.com/ceph/ceph/pull/56361>`_, liuqinfei)
* ceph.spec.in: remove command-with-macro line (`pr#57357 <https://github.com/ceph/ceph/pull/57357>`_, John Mulligan)
* cephadm/nvmeof: scrape nvmeof prometheus endpoint (`pr#56108 <https://github.com/ceph/ceph/pull/56108>`_, Avan Thakkar)
* cephadm: Add mount for nvmeof log location (`pr#55819 <https://github.com/ceph/ceph/pull/55819>`_, Roy Sahar)
* cephadm: Add nvmeof to autotuner calculation (`pr#56100 <https://github.com/ceph/ceph/pull/56100>`_, Paul Cuzner)
* cephadm: add timemaster to timesync services list (`pr#56307 <https://github.com/ceph/ceph/pull/56307>`_, Florent Carli)
* cephadm: adjust the ingress ha proxy health check interval (`pr#56286 <https://github.com/ceph/ceph/pull/56286>`_, Jiffin Tony Thottan)
* cephadm: create ceph-exporter sock dir if it's not present (`pr#56102 <https://github.com/ceph/ceph/pull/56102>`_, Adam King)
* cephadm: fix get_version for nvmeof (`pr#56099 <https://github.com/ceph/ceph/pull/56099>`_, Adam King)
* cephadm: improve cephadm pull usage message (`pr#56292 <https://github.com/ceph/ceph/pull/56292>`_, Adam King)
* cephadm: remove restriction for crush device classes (`pr#56106 <https://github.com/ceph/ceph/pull/56106>`_, Seena Fallah)
* cephadm: rm podman-auth.json if removing last cluster (`pr#56105 <https://github.com/ceph/ceph/pull/56105>`_, Adam King)
* cephfs-shell: remove distutils Version classes because they're deprecated (`pr#54119 <https://github.com/ceph/ceph/pull/54119>`_, Venky Shankar, Jos Collin)
* cephfs-top: include the missing fields in --dump output (`pr#54520 <https://github.com/ceph/ceph/pull/54520>`_, Jos Collin)
* client/fuse: handle case of renameat2 with non-zero flags (`pr#55002 <https://github.com/ceph/ceph/pull/55002>`_, Leonid Usov, Shachar Sharon)
* client: append to buffer list to save all result from wildcard command (`pr#53893 <https://github.com/ceph/ceph/pull/53893>`_, Rishabh Dave, Jinmyeong Lee, Jimyeong Lee)
* client: call _getattr() for -ENODATA returned _getvxattr() calls (`pr#54404 <https://github.com/ceph/ceph/pull/54404>`_, Jos Collin)
* client: fix leak of file handles (`pr#56122 <https://github.com/ceph/ceph/pull/56122>`_, Xavi Hernandez)
* client: Fix return in removexattr for xattrs from `system.` namespace (`pr#55803 <https://github.com/ceph/ceph/pull/55803>`_, Anoop C S)
* client: queue a delay cap flushing if there are ditry caps/snapcaps (`pr#54466 <https://github.com/ceph/ceph/pull/54466>`_, Xiubo Li)
* client: readdir_r_cb: get rstat for dir only if using rbytes for size (`pr#53359 <https://github.com/ceph/ceph/pull/53359>`_, Pinghao Wu)
* cmake/modules/BuildRocksDB.cmake: inherit parent's CMAKE_CXX_FLAGS (`pr#55502 <https://github.com/ceph/ceph/pull/55502>`_, Kefu Chai)
* cmake: use or turn off liburing for rocksdb (`pr#54122 <https://github.com/ceph/ceph/pull/54122>`_, Casey Bodley, Patrick Donnelly)
* common/options: Set LZ4 compression for bluestore RocksDB (`pr#55197 <https://github.com/ceph/ceph/pull/55197>`_, Mark Nelson)
* common/weighted_shuffle: don't feed std::discrete_distribution with all-zero weights (`pr#55153 <https://github.com/ceph/ceph/pull/55153>`_, Radosław Zarzyński)
* common: resolve config proxy deadlock using refcounted pointers (`pr#54373 <https://github.com/ceph/ceph/pull/54373>`_, Patrick Donnelly)
* DaemonServer.cc: fix config show command for RGW daemons (`pr#55077 <https://github.com/ceph/ceph/pull/55077>`_, Aishwarya Mathuria)
* debian: add ceph-exporter package (`pr#56541 <https://github.com/ceph/ceph/pull/56541>`_, Shinya Hayashi)
* debian: add missing bcrypt to ceph-mgr .requires to fix resulting package dependencies (`pr#54662 <https://github.com/ceph/ceph/pull/54662>`_, Thomas Lamprecht)
* doc/architecture.rst - fix typo (`pr#55384 <https://github.com/ceph/ceph/pull/55384>`_, Zac Dover)
* doc/architecture.rst: improve rados definition (`pr#55343 <https://github.com/ceph/ceph/pull/55343>`_, Zac Dover)
* doc/architecture: correct typo (`pr#56012 <https://github.com/ceph/ceph/pull/56012>`_, Zac Dover)
* doc/architecture: improve some paragraphs (`pr#55399 <https://github.com/ceph/ceph/pull/55399>`_, Zac Dover)
* doc/architecture: remove pleonasm (`pr#55933 <https://github.com/ceph/ceph/pull/55933>`_, Zac Dover)
* doc/cephadm - edit t11ing (`pr#55482 <https://github.com/ceph/ceph/pull/55482>`_, Zac Dover)
* doc/cephadm/services: Improve monitoring.rst (`pr#56290 <https://github.com/ceph/ceph/pull/56290>`_, Anthony D'Atri)
* doc/cephadm: correct nfs config pool name (`pr#55603 <https://github.com/ceph/ceph/pull/55603>`_, Zac Dover)
* doc/cephadm: improve host-management.rst (`pr#56111 <https://github.com/ceph/ceph/pull/56111>`_, Anthony D'Atri)
* doc/cephadm: Improve multiple files (`pr#56130 <https://github.com/ceph/ceph/pull/56130>`_, Anthony D'Atri)
* doc/cephfs/client-auth.rst: correct ``fs authorize cephfs1 /dir1 client.x rw`` (`pr#55246 <https://github.com/ceph/ceph/pull/55246>`_, 叶海丰)
* doc/cephfs: edit add-remove-mds (`pr#55648 <https://github.com/ceph/ceph/pull/55648>`_, Zac Dover)
* doc/cephfs: fix architecture link to correct relative path (`pr#56340 <https://github.com/ceph/ceph/pull/56340>`_, molpako)
* doc/cephfs: Update disaster-recovery-experts.rst to mention Slack (`pr#55044 <https://github.com/ceph/ceph/pull/55044>`_, Dhairya Parmar)
* doc/crimson: cleanup duplicate seastore description (`pr#55730 <https://github.com/ceph/ceph/pull/55730>`_, Rongqi Sun)
* doc/dev: backport zipapp docs to reef (`pr#56161 <https://github.com/ceph/ceph/pull/56161>`_, Zac Dover)
* doc/dev: edit internals.rst (`pr#55852 <https://github.com/ceph/ceph/pull/55852>`_, Zac Dover)
* doc/dev: edit teuthology workflow (`pr#56002 <https://github.com/ceph/ceph/pull/56002>`_, Zac Dover)
* doc/dev: fix spelling in crimson.rst (`pr#55737 <https://github.com/ceph/ceph/pull/55737>`_, Zac Dover)
* doc/dev: osd_internals/snaps.rst: add clone_overlap doc (`pr#56523 <https://github.com/ceph/ceph/pull/56523>`_, Matan Breizman)
* doc/dev: refine "Concepts" (`pr#56660 <https://github.com/ceph/ceph/pull/56660>`_, Zac Dover)
* doc/dev: refine "Concepts" 2 of 3 (`pr#56725 <https://github.com/ceph/ceph/pull/56725>`_, Zac Dover)
* doc/dev: refine "Concepts" 3 of 3 (`pr#56729 <https://github.com/ceph/ceph/pull/56729>`_, Zac Dover)
* doc/dev: refine "Concepts" 4 of 3 (`pr#56740 <https://github.com/ceph/ceph/pull/56740>`_, Zac Dover)
* doc/dev: update leads list (`pr#56603 <https://github.com/ceph/ceph/pull/56603>`_, Zac Dover)
* doc/dev: update leads list (`pr#56589 <https://github.com/ceph/ceph/pull/56589>`_, Zac Dover)
* doc/glossary.rst: add "Monitor Store" (`pr#54743 <https://github.com/ceph/ceph/pull/54743>`_, Zac Dover)
* doc/glossary: add "Crimson" entry (`pr#56073 <https://github.com/ceph/ceph/pull/56073>`_, Zac Dover)
* doc/glossary: add "librados" entry (`pr#56235 <https://github.com/ceph/ceph/pull/56235>`_, Zac Dover)
* doc/glossary: Add "OMAP" to glossary (`pr#55749 <https://github.com/ceph/ceph/pull/55749>`_, Zac Dover)
* doc/glossary: Add link to CRUSH paper (`pr#55557 <https://github.com/ceph/ceph/pull/55557>`_, Zac Dover)
* doc/glossary: improve "MDS" entry (`pr#55849 <https://github.com/ceph/ceph/pull/55849>`_, Zac Dover)
* doc/glossary: improve OSD definitions (`pr#55613 <https://github.com/ceph/ceph/pull/55613>`_, Zac Dover)
* doc/install: add manual RADOSGW install procedure (`pr#55880 <https://github.com/ceph/ceph/pull/55880>`_, Zac Dover)
* doc/install: update "update submodules" (`pr#54961 <https://github.com/ceph/ceph/pull/54961>`_, Zac Dover)
* doc/man/8/mount.ceph.rst: add more mount options (`pr#55754 <https://github.com/ceph/ceph/pull/55754>`_, Xiubo Li)
* doc/man: edit "manipulating the omap key" (`pr#55635 <https://github.com/ceph/ceph/pull/55635>`_, Zac Dover)
* doc/man: edit ceph-osd description (`pr#54551 <https://github.com/ceph/ceph/pull/54551>`_, Zac Dover)
* doc/mgr: credit John Jasen for Zabbix 2 (`pr#56684 <https://github.com/ceph/ceph/pull/56684>`_, Zac Dover)
* doc/mgr: document lack of MSWin NFS 4.x support (`pr#55032 <https://github.com/ceph/ceph/pull/55032>`_, Zac Dover)
* doc/mgr: update zabbix information (`pr#56631 <https://github.com/ceph/ceph/pull/56631>`_, Zac Dover)
* doc/rados/configuration/bluestore-config-ref: Fix lowcase typo (`pr#54694 <https://github.com/ceph/ceph/pull/54694>`_, Adam Kupczyk)
* doc/rados/configuration/osd-config-ref: fix typo (`pr#55678 <https://github.com/ceph/ceph/pull/55678>`_, Pierre Riteau)
* doc/rados/operations: add EC overhead table to erasure-code.rst (`pr#55244 <https://github.com/ceph/ceph/pull/55244>`_, Anthony D'Atri)
* doc/rados/operations: Fix off-by-one errors in control.rst (`pr#55231 <https://github.com/ceph/ceph/pull/55231>`_, tobydarling)
* doc/rados/operations: Improve crush_location docs (`pr#56594 <https://github.com/ceph/ceph/pull/56594>`_, Niklas Hambüchen)
* doc/rados: add "change public network" procedure (`pr#55799 <https://github.com/ceph/ceph/pull/55799>`_, Zac Dover)
* doc/rados: add link to pg blog post (`pr#55611 <https://github.com/ceph/ceph/pull/55611>`_, Zac Dover)
* doc/rados: add PG definition (`pr#55630 <https://github.com/ceph/ceph/pull/55630>`_, Zac Dover)
* doc/rados: edit "client can't connect..." (`pr#54654 <https://github.com/ceph/ceph/pull/54654>`_, Zac Dover)
* doc/rados: edit "Everything Failed! Now What?" (`pr#54665 <https://github.com/ceph/ceph/pull/54665>`_, Zac Dover)
* doc/rados: edit "monitor store failures" (`pr#54659 <https://github.com/ceph/ceph/pull/54659>`_, Zac Dover)
* doc/rados: edit "recovering broken monmap" (`pr#54601 <https://github.com/ceph/ceph/pull/54601>`_, Zac Dover)
* doc/rados: edit "understanding mon_status" (`pr#54579 <https://github.com/ceph/ceph/pull/54579>`_, Zac Dover)
* doc/rados: edit "Using the Monitor's Admin Socket" (`pr#54576 <https://github.com/ceph/ceph/pull/54576>`_, Zac Dover)
* doc/rados: fix broken links (`pr#55680 <https://github.com/ceph/ceph/pull/55680>`_, Zac Dover)
* doc/rados: format sections in tshooting-mon.rst (`pr#54638 <https://github.com/ceph/ceph/pull/54638>`_, Zac Dover)
* doc/rados: improve "Ceph Subsystems" (`pr#54702 <https://github.com/ceph/ceph/pull/54702>`_, Zac Dover)
* doc/rados: improve formatting of log-and-debug.rst (`pr#54746 <https://github.com/ceph/ceph/pull/54746>`_, Zac Dover)
* doc/rados: link to pg setting commands (`pr#55936 <https://github.com/ceph/ceph/pull/55936>`_, Zac Dover)
* doc/rados: ops/pgs: s/power of 2/power of two (`pr#54700 <https://github.com/ceph/ceph/pull/54700>`_, Zac Dover)
* doc/rados: remove PGcalc from docs (`pr#55901 <https://github.com/ceph/ceph/pull/55901>`_, Zac Dover)
* doc/rados: repair stretch-mode.rst (`pr#54762 <https://github.com/ceph/ceph/pull/54762>`_, Zac Dover)
* doc/rados: restore PGcalc tool (`pr#56057 <https://github.com/ceph/ceph/pull/56057>`_, Zac Dover)
* doc/rados: update "stretch mode" (`pr#54756 <https://github.com/ceph/ceph/pull/54756>`_, Michael Collins)
* doc/rados: update common.rst (`pr#56268 <https://github.com/ceph/ceph/pull/56268>`_, Zac Dover)
* doc/rados: update config for autoscaler (`pr#55438 <https://github.com/ceph/ceph/pull/55438>`_, Zac Dover)
* doc/rados: update PG guidance (`pr#55460 <https://github.com/ceph/ceph/pull/55460>`_, Zac Dover)
* doc/radosgw - edit admin.rst "set user rate limit" (`pr#55150 <https://github.com/ceph/ceph/pull/55150>`_, Zac Dover)
* doc/radosgw/admin.rst: use underscores in config var names (`pr#54933 <https://github.com/ceph/ceph/pull/54933>`_, Ville Ojamo)
* doc/radosgw: add confval directives (`pr#55484 <https://github.com/ceph/ceph/pull/55484>`_, Zac Dover)
* doc/radosgw: add gateway starting command (`pr#54833 <https://github.com/ceph/ceph/pull/54833>`_, Zac Dover)
* doc/radosgw: admin.rst - edit "Create a Subuser" (`pr#55020 <https://github.com/ceph/ceph/pull/55020>`_, Zac Dover)
* doc/radosgw: admin.rst - edit "Create a User" (`pr#55004 <https://github.com/ceph/ceph/pull/55004>`_, Zac Dover)
* doc/radosgw: admin.rst - edit sections (`pr#55017 <https://github.com/ceph/ceph/pull/55017>`_, Zac Dover)
* doc/radosgw: edit "Add/Remove a Key" (`pr#55055 <https://github.com/ceph/ceph/pull/55055>`_, Zac Dover)
* doc/radosgw: edit "Enable/Disable Bucket Rate Limit" (`pr#55260 <https://github.com/ceph/ceph/pull/55260>`_, Zac Dover)
* doc/radosgw: edit "read/write global rate limit" admin.rst (`pr#55271 <https://github.com/ceph/ceph/pull/55271>`_, Zac Dover)
* doc/radosgw: edit "remove a subuser" (`pr#55034 <https://github.com/ceph/ceph/pull/55034>`_, Zac Dover)
* doc/radosgw: edit "Usage" admin.rst (`pr#55321 <https://github.com/ceph/ceph/pull/55321>`_, Zac Dover)
* doc/radosgw: edit admin.rst "Get Bucket Rate Limit" (`pr#55253 <https://github.com/ceph/ceph/pull/55253>`_, Zac Dover)
* doc/radosgw: edit admin.rst "get user rate limit" (`pr#55157 <https://github.com/ceph/ceph/pull/55157>`_, Zac Dover)
* doc/radosgw: edit admin.rst "set bucket rate limit" (`pr#55242 <https://github.com/ceph/ceph/pull/55242>`_, Zac Dover)
* doc/radosgw: edit admin.rst - quota (`pr#55082 <https://github.com/ceph/ceph/pull/55082>`_, Zac Dover)
* doc/radosgw: edit admin.rst 1 of x (`pr#55000 <https://github.com/ceph/ceph/pull/55000>`_, Zac Dover)
* doc/radosgw: edit compression.rst (`pr#54985 <https://github.com/ceph/ceph/pull/54985>`_, Zac Dover)
* doc/radosgw: edit front matter - role.rst (`pr#54854 <https://github.com/ceph/ceph/pull/54854>`_, Zac Dover)
* doc/radosgw: edit multisite.rst (`pr#55671 <https://github.com/ceph/ceph/pull/55671>`_, Zac Dover)
* doc/radosgw: edit sections (`pr#55027 <https://github.com/ceph/ceph/pull/55027>`_, Zac Dover)
* doc/radosgw: fix formatting (`pr#54753 <https://github.com/ceph/ceph/pull/54753>`_, Zac Dover)
* doc/radosgw: Fix JSON typo in Principal Tag example code snippet (`pr#54642 <https://github.com/ceph/ceph/pull/54642>`_, Daniel Parkes)
* doc/radosgw: fix verb disagreement - index.html (`pr#55338 <https://github.com/ceph/ceph/pull/55338>`_, Zac Dover)
* doc/radosgw: format "Create a Role" (`pr#54886 <https://github.com/ceph/ceph/pull/54886>`_, Zac Dover)
* doc/radosgw: format commands in role.rst (`pr#54905 <https://github.com/ceph/ceph/pull/54905>`_, Zac Dover)
* doc/radosgw: format POST statements (`pr#54849 <https://github.com/ceph/ceph/pull/54849>`_, Zac Dover)
* doc/radosgw: list supported plugins-compression.rst (`pr#54995 <https://github.com/ceph/ceph/pull/54995>`_, Zac Dover)
* doc/radosgw: update link in rgw-cache.rst (`pr#54805 <https://github.com/ceph/ceph/pull/54805>`_, Zac Dover)
* doc/radosrgw: edit admin.rst (`pr#55073 <https://github.com/ceph/ceph/pull/55073>`_, Zac Dover)
* doc/rbd: add clone mapping command (`pr#56208 <https://github.com/ceph/ceph/pull/56208>`_, Zac Dover)
* doc/rbd: add map information for clone images to rbd-encryption.rst (`pr#56186 <https://github.com/ceph/ceph/pull/56186>`_, N Balachandran)
* doc/rbd: minor changes to the rbd man page (`pr#56256 <https://github.com/ceph/ceph/pull/56256>`_, N Balachandran)
* doc/rbd: repair ordered list (`pr#55732 <https://github.com/ceph/ceph/pull/55732>`_, Zac Dover)
* doc/releases: edit reef.rst (`pr#55064 <https://github.com/ceph/ceph/pull/55064>`_, Zac Dover)
* doc/releases: specify dashboard improvements (`pr#55049 <https://github.com/ceph/ceph/pull/55049>`_, Laura Flores, Zac Dover)
* doc/rgw: edit admin.rst - rate limit management (`pr#55128 <https://github.com/ceph/ceph/pull/55128>`_, Zac Dover)
* doc/rgw: fix Attributes index in CreateTopic example (`pr#55432 <https://github.com/ceph/ceph/pull/55432>`_, Casey Bodley)
* doc/start: add Slack invite link (`pr#56041 <https://github.com/ceph/ceph/pull/56041>`_, Zac Dover)
* doc/start: explain "OSD" (`pr#54559 <https://github.com/ceph/ceph/pull/54559>`_, Zac Dover)
* doc/start: improve MDS explanation (`pr#56466 <https://github.com/ceph/ceph/pull/56466>`_, Zac Dover)
* doc/start: improve MDS explanation (`pr#56426 <https://github.com/ceph/ceph/pull/56426>`_, Zac Dover)
* doc/start: link to mon map command (`pr#56410 <https://github.com/ceph/ceph/pull/56410>`_, Zac Dover)
* doc/start: update release names (`pr#54572 <https://github.com/ceph/ceph/pull/54572>`_, Zac Dover)
* doc: add description of metric fields for cephfs-top (`pr#55511 <https://github.com/ceph/ceph/pull/55511>`_, Neeraj Pratap Singh)
* doc: Add NVMe-oF gateway documentation (`pr#55724 <https://github.com/ceph/ceph/pull/55724>`_, Orit Wasserman)
* doc: add supported file types in cephfs-mirroring.rst (`pr#54822 <https://github.com/ceph/ceph/pull/54822>`_, Jos Collin)
* doc: adding documentation for secure monitoring stack configuration (`pr#56104 <https://github.com/ceph/ceph/pull/56104>`_, Redouane Kachach)
* doc: cephadm/services/osd: fix typo (`pr#56230 <https://github.com/ceph/ceph/pull/56230>`_, Lorenz Bausch)
* doc: Fixes two typos and grammatical errors. Signed-off-by: Sina Ahma… (`pr#54775 <https://github.com/ceph/ceph/pull/54775>`_, Sina Ahmadi)
* doc: fixing doc/cephfs/fs-volumes (`pr#56648 <https://github.com/ceph/ceph/pull/56648>`_, Neeraj Pratap Singh)
* doc: remove releases docs (`pr#56567 <https://github.com/ceph/ceph/pull/56567>`_, Patrick Donnelly)
* doc: specify correct fs type for mkfs (`pr#55282 <https://github.com/ceph/ceph/pull/55282>`_, Vladislav Glagolev)
* doc: update rgw admin api req params for get user info (`pr#55071 <https://github.com/ceph/ceph/pull/55071>`_, Ali Maredia)
* doc:start.rst fix typo in hw-recs (`pr#55505 <https://github.com/ceph/ceph/pull/55505>`_, Eduardo Roldan)
* docs/rados: remove incorrect ceph command (`pr#56495 <https://github.com/ceph/ceph/pull/56495>`_, Taha Jahangir)
* docs/radosgw: edit admin.rst "enable/disable user rate limit" (`pr#55194 <https://github.com/ceph/ceph/pull/55194>`_, Zac Dover)
* docs/rbd: fix typo in arg name (`pr#56262 <https://github.com/ceph/ceph/pull/56262>`_, N Balachandran)
* docs: Add information about OpenNebula integration (`pr#54938 <https://github.com/ceph/ceph/pull/54938>`_, Daniel Clavijo)
* librados: make querying pools for selfmanaged snaps reliable (`pr#55026 <https://github.com/ceph/ceph/pull/55026>`_, Ilya Dryomov)
* librbd: account for discards that truncate in ObjectListSnapsRequest (`pr#56213 <https://github.com/ceph/ceph/pull/56213>`_, Ilya Dryomov)
* librbd: Append one journal event per image request (`pr#54818 <https://github.com/ceph/ceph/pull/54818>`_, Ilya Dryomov, Joshua Baergen)
* librbd: don't report HOLE_UPDATED when diffing against a hole (`pr#54951 <https://github.com/ceph/ceph/pull/54951>`_, Ilya Dryomov)
* librbd: fix regressions in ObjectListSnapsRequest (`pr#54862 <https://github.com/ceph/ceph/pull/54862>`_, Ilya Dryomov)
* librbd: fix split() for SparseExtent and SparseBufferlistExtent (`pr#55665 <https://github.com/ceph/ceph/pull/55665>`_, Ilya Dryomov)
* librbd: improve rbd_diff_iterate2() performance in fast-diff mode (`pr#55427 <https://github.com/ceph/ceph/pull/55427>`_, Ilya Dryomov)
* librbd: return ENOENT from Snapshot::get_timestamp for nonexistent snap_id (`pr#55474 <https://github.com/ceph/ceph/pull/55474>`_, John Agombar)
* make-dist: don't use --continue option for wget (`pr#55091 <https://github.com/ceph/ceph/pull/55091>`_, Casey Bodley)
* MClientRequest: properly handle ceph_mds_request_head_legacy for ext_num_retry, ext_num_fwd, owner_uid, owner_gid (`pr#54407 <https://github.com/ceph/ceph/pull/54407>`_, Alexander Mikhalitsyn)
* mds,cephfs_mirror: add labelled per-client and replication metrics (`issue#63945 <http://tracker.ceph.com/issues/63945>`_, `pr#55640 <https://github.com/ceph/ceph/pull/55640>`_, Venky Shankar, Jos Collin)
* mds/client: check the cephx mds auth access in client side (`pr#54468 <https://github.com/ceph/ceph/pull/54468>`_, Xiubo Li, Ramana Raja)
* mds/MDBalancer: ignore queued callbacks if MDS is not active (`pr#54493 <https://github.com/ceph/ceph/pull/54493>`_, Leonid Usov)
* mds/MDSRank: Add set_history_slow_op_size_and_threshold for op_tracker (`pr#53357 <https://github.com/ceph/ceph/pull/53357>`_, Yite Gu)
* mds: accept human readable values for quotas (`issue#55940 <http://tracker.ceph.com/issues/55940>`_, `pr#53333 <https://github.com/ceph/ceph/pull/53333>`_, Venky Shankar, Dhairya Parmar, dparmar18)
* mds: add a command to dump directory information (`pr#55987 <https://github.com/ceph/ceph/pull/55987>`_, Jos Collin, Zhansong Gao)
* mds: add balance_automate fs setting (`pr#54952 <https://github.com/ceph/ceph/pull/54952>`_, Patrick Donnelly)
* mds: add debug logs during setxattr ceph.dir.subvolume (`pr#56062 <https://github.com/ceph/ceph/pull/56062>`_, Milind Changire)
* mds: allow all types of mds caps (`pr#52581 <https://github.com/ceph/ceph/pull/52581>`_, Rishabh Dave)
* mds: allow lock state to be LOCK_MIX_SYNC in replica for filelock (`pr#56049 <https://github.com/ceph/ceph/pull/56049>`_, Xiubo Li)
* mds: change priority of mds rss perf counter to useful (`pr#55057 <https://github.com/ceph/ceph/pull/55057>`_, sp98)
* mds: check file layout in mknod (`pr#56031 <https://github.com/ceph/ceph/pull/56031>`_, Xue Yantao)
* mds: check relevant caps for fs include root_squash (`pr#57343 <https://github.com/ceph/ceph/pull/57343>`_, Patrick Donnelly)
* mds: disable `defer_client_eviction_on_laggy_osds' by default (`issue#64685 <http://tracker.ceph.com/issues/64685>`_, `pr#56196 <https://github.com/ceph/ceph/pull/56196>`_, Venky Shankar)
* mds: do not evict clients if OSDs are laggy (`pr#52268 <https://github.com/ceph/ceph/pull/52268>`_, Dhairya Parmar, Laura Flores)
* mds: do not simplify fragset (`pr#54895 <https://github.com/ceph/ceph/pull/54895>`_, Milind Changire)
* mds: ensure next replay is queued on req drop (`pr#54313 <https://github.com/ceph/ceph/pull/54313>`_, Patrick Donnelly)
* mds: ensure snapclient is synced before corruption check (`pr#56398 <https://github.com/ceph/ceph/pull/56398>`_, Patrick Donnelly)
* mds: fix issuing redundant reintegrate/migrate_stray requests (`pr#54467 <https://github.com/ceph/ceph/pull/54467>`_, Xiubo Li)
* mds: just wait the client flushes the snap and dirty buffer (`pr#55743 <https://github.com/ceph/ceph/pull/55743>`_, Xiubo Li)
* mds: optionally forbid to use standby for another fs as last resort (`pr#53340 <https://github.com/ceph/ceph/pull/53340>`_, Venky Shankar, Mykola Golub, Luís Henriques)
* mds: relax certain asserts in mdlog replay thread (`issue#57048 <http://tracker.ceph.com/issues/57048>`_, `pr#56016 <https://github.com/ceph/ceph/pull/56016>`_, Venky Shankar)
* mds: reverse MDSMap encoding of max_xattr_size/bal_rank_mask (`pr#55669 <https://github.com/ceph/ceph/pull/55669>`_, Patrick Donnelly)
* mds: revert standby-replay trimming changes (`pr#54716 <https://github.com/ceph/ceph/pull/54716>`_, Patrick Donnelly)
* mds: scrub repair does not clear earlier damage health status (`pr#54899 <https://github.com/ceph/ceph/pull/54899>`_, Neeraj Pratap Singh)
* mds: set the loner to true for LOCK_EXCL_XSYN (`pr#54911 <https://github.com/ceph/ceph/pull/54911>`_, Xiubo Li)
* mds: skip sr moves when target is an unlinked dir (`pr#56672 <https://github.com/ceph/ceph/pull/56672>`_, Patrick Donnelly, Dan van der Ster)
* mds: use explicitly sized types for network and disk encoding (`pr#55742 <https://github.com/ceph/ceph/pull/55742>`_, Xiubo Li)
* MDSAuthCaps: minor improvements (`pr#54185 <https://github.com/ceph/ceph/pull/54185>`_, Rishabh Dave)
* MDSAuthCaps: print better error message for perm flag in MDS caps (`pr#54945 <https://github.com/ceph/ceph/pull/54945>`_, Rishabh Dave)
* mgr/(object_format && nfs/export): enhance nfs export update failure response (`pr#55395 <https://github.com/ceph/ceph/pull/55395>`_, Dhairya Parmar, John Mulligan)
* mgr/.dashboard: batch backport of cephfs snapshot schedule management (`pr#55581 <https://github.com/ceph/ceph/pull/55581>`_, Ivo Almeida)
* mgr/cephadm is not defining haproxy tcp healthchecks for Ganesha (`pr#56101 <https://github.com/ceph/ceph/pull/56101>`_, avanthakkar)
* mgr/cephadm: allow grafana and prometheus to only bind to specific network (`pr#56302 <https://github.com/ceph/ceph/pull/56302>`_, Adam King)
* mgr/cephadm: Allow idmap overrides in nfs-ganesha configuration (`pr#56029 <https://github.com/ceph/ceph/pull/56029>`_, Teoman ONAY)
* mgr/cephadm: catch CancelledError in asyncio timeout handler (`pr#56103 <https://github.com/ceph/ceph/pull/56103>`_, Adam King)
* mgr/cephadm: discovery service (port 8765) fails on ipv6 only clusters (`pr#56093 <https://github.com/ceph/ceph/pull/56093>`_, Theofilos Mouratidis)
* mgr/cephadm: fix placement with label and host pattern (`pr#56107 <https://github.com/ceph/ceph/pull/56107>`_, Adam King)
* mgr/cephadm: fix reweighting of OSD when OSD removal is stopped (`pr#56094 <https://github.com/ceph/ceph/pull/56094>`_, Adam King)
* mgr/cephadm: fixups for asyncio based timeout (`pr#55555 <https://github.com/ceph/ceph/pull/55555>`_, Adam King)
* mgr/cephadm: make jaeger-collector a dep for jaeger-agent (`pr#56089 <https://github.com/ceph/ceph/pull/56089>`_, Adam King)
* mgr/cephadm: refresh public_network for config checks before checking (`pr#56325 <https://github.com/ceph/ceph/pull/56325>`_, Adam King)
* mgr/cephadm: support for regex based host patterns (`pr#56221 <https://github.com/ceph/ceph/pull/56221>`_, Adam King)
* mgr/cephadm: support for removing host entry from crush map during host removal (`pr#56092 <https://github.com/ceph/ceph/pull/56092>`_, Adam King)
* mgr/cephadm: update timestamp on repeat daemon/service events (`pr#56090 <https://github.com/ceph/ceph/pull/56090>`_, Adam King)
* mgr/dashboard/frontend:Ceph dashboard supports multiple languages (`pr#56359 <https://github.com/ceph/ceph/pull/56359>`_, TomNewChao)
* mgr/dashboard: Add advanced fieldset component (`pr#56692 <https://github.com/ceph/ceph/pull/56692>`_, Afreen)
* cmake/arrow: don't treat warnings as errors (`pr#57375 <https://github.com/ceph/ceph/pull/57375>`_, Casey Bodley)
* mgr/dashboard: add frontend unit tests for rgw multisite sync status card (`pr#55222 <https://github.com/ceph/ceph/pull/55222>`_, Aashish Sharma)
* mgr/dashboard: add snap schedule M, Y frequencies (`pr#56059 <https://github.com/ceph/ceph/pull/56059>`_, Ivo Almeida)
* mgr/dashboard: add support for editing and deleting rgw roles (`pr#55541 <https://github.com/ceph/ceph/pull/55541>`_, Nizamudeen A)
* mgr/dashboard: add system users to rgw user form (`pr#56471 <https://github.com/ceph/ceph/pull/56471>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: add Table Schema to grafonnet (`pr#56736 <https://github.com/ceph/ceph/pull/56736>`_, Aashish Sharma)
* mgr/dashboard: Allow the user to add the access/secret key on zone edit and not on zone creation (`pr#56472 <https://github.com/ceph/ceph/pull/56472>`_, Aashish Sharma)
* mgr/dashboard: ceph authenticate user from fs (`pr#56254 <https://github.com/ceph/ceph/pull/56254>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: change deprecated grafana URL in daemon logs (`pr#55544 <https://github.com/ceph/ceph/pull/55544>`_, Nizamudeen A)
* mgr/dashboard: chartjs and ng2-charts version upgrade (`pr#55224 <https://github.com/ceph/ceph/pull/55224>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Consider null values as zero in grafana panels (`pr#54541 <https://github.com/ceph/ceph/pull/54541>`_, Aashish Sharma)
* mgr/dashboard: create cephfs snapshot clone (`pr#55489 <https://github.com/ceph/ceph/pull/55489>`_, Nizamudeen A)
* mgr/dashboard: Create realm sets to default (`pr#55221 <https://github.com/ceph/ceph/pull/55221>`_, Aashish Sharma)
* mgr/dashboard: Create subvol of same name in different group (`pr#55369 <https://github.com/ceph/ceph/pull/55369>`_, Afreen)
* mgr/dashboard: dashboard area chart unit test (`pr#55517 <https://github.com/ceph/ceph/pull/55517>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: debugging make check failure (`pr#56127 <https://github.com/ceph/ceph/pull/56127>`_, Nizamudeen A)
* mgr/dashboard: disable applitools e2e (`pr#56215 <https://github.com/ceph/ceph/pull/56215>`_, Nizamudeen A)
* mgr/dashboard: fix cephfs name validation (`pr#56501 <https://github.com/ceph/ceph/pull/56501>`_, Nizamudeen A)
* mgr/dashboard: fix clone unique validator for name validation (`pr#56550 <https://github.com/ceph/ceph/pull/56550>`_, Nizamudeen A)
* mgr/dashboard: fix e2e failure related to landing page (`pr#55124 <https://github.com/ceph/ceph/pull/55124>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix empty tags (`pr#56439 <https://github.com/ceph/ceph/pull/56439>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix error while accessing roles tab when policy attached (`pr#55515 <https://github.com/ceph/ceph/pull/55515>`_, Afreen)
* mgr/dashboard: Fix inconsistency in capitalisation of "Multi-site" (`pr#55311 <https://github.com/ceph/ceph/pull/55311>`_, Afreen)
* mgr/dashboard: fix M retention frequency display (`pr#56363 <https://github.com/ceph/ceph/pull/56363>`_, Ivo Almeida)
* mgr/dashboard: fix retention add for subvolume (`pr#56370 <https://github.com/ceph/ceph/pull/56370>`_, Ivo Almeida)
* mgr/dashboard: fix rgw display name validation (`pr#56548 <https://github.com/ceph/ceph/pull/56548>`_, Nizamudeen A)
* mgr/dashboard: fix roles page for roles without policies (`pr#55827 <https://github.com/ceph/ceph/pull/55827>`_, Nizamudeen A)
* mgr/dashboard: fix snap schedule date format (`pr#55815 <https://github.com/ceph/ceph/pull/55815>`_, Ivo Almeida)
* mgr/dashboard: fix snap schedule list toggle cols (`pr#56115 <https://github.com/ceph/ceph/pull/56115>`_, Ivo Almeida)
* mgr/dashboard: fix snap schedule time format (`pr#56154 <https://github.com/ceph/ceph/pull/56154>`_, Ivo Almeida)
* mgr/dashboard: fix subvolume group edit (`pr#55811 <https://github.com/ceph/ceph/pull/55811>`_, Ivo Almeida)
* mgr/dashboard: fix subvolume group edit size (`pr#56385 <https://github.com/ceph/ceph/pull/56385>`_, Ivo Almeida)
* mgr/dashboard: fix the jsonschema issue in install-deps (`pr#55542 <https://github.com/ceph/ceph/pull/55542>`_, Nizamudeen A)
* mgr/dashboard: fix volume creation with multiple hosts (`pr#55786 <https://github.com/ceph/ceph/pull/55786>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fixed cephfs mount command (`pr#55993 <https://github.com/ceph/ceph/pull/55993>`_, Ivo Almeida)
* mgr/dashboard: fixed nfs attach command (`pr#56387 <https://github.com/ceph/ceph/pull/56387>`_, Ivo Almeida)
* mgr/dashboard: Fixes multisite topology page breadcrumb (`pr#55212 <https://github.com/ceph/ceph/pull/55212>`_, Afreen Misbah)
* mgr/dashboard: get object bucket policies for a bucket (`pr#55361 <https://github.com/ceph/ceph/pull/55361>`_, Nizamudeen A)
* mgr/dashboard: get rgw port from ssl_endpoint (`pr#54764 <https://github.com/ceph/ceph/pull/54764>`_, Nizamudeen A)
* mgr/dashboard: Handle errors for /api/osd/settings (`pr#55704 <https://github.com/ceph/ceph/pull/55704>`_, Afreen)
* mgr/dashboard: increase the number of plottable graphs in charts (`pr#55571 <https://github.com/ceph/ceph/pull/55571>`_, Afreen, Aashish Sharma)
* mgr/dashboard: Locking improvements in bucket create form (`pr#56560 <https://github.com/ceph/ceph/pull/56560>`_, Afreen)
* mgr/dashboard: make ceph logo redirect to dashboard (`pr#56557 <https://github.com/ceph/ceph/pull/56557>`_, Afreen)
* mgr/dashboard: Mark placement targets as non-required (`pr#56621 <https://github.com/ceph/ceph/pull/56621>`_, Afreen)
* mgr/dashboard: replace deprecated table panel in grafana with a newer table panel (`pr#56682 <https://github.com/ceph/ceph/pull/56682>`_, Aashish Sharma)
* mgr/dashboard: replace piechart plugin charts with native pie chart panel (`pr#56654 <https://github.com/ceph/ceph/pull/56654>`_, Aashish Sharma)
* mgr/dashboard: rgw bucket features (`pr#55575 <https://github.com/ceph/ceph/pull/55575>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: rm warning/error threshold for cpu usage (`pr#56443 <https://github.com/ceph/ceph/pull/56443>`_, Nizamudeen A)
* mgr/dashboard: s/active_mds/active_nfs in fs attach form (`pr#56546 <https://github.com/ceph/ceph/pull/56546>`_, Nizamudeen A)
* mgr/dashboard: sanitize dashboard user creation (`pr#56452 <https://github.com/ceph/ceph/pull/56452>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: Show the OSDs Out and Down panels as red whenever an OSD is in Out or Down state in Ceph Cluster grafana dashboard (`pr#54538 <https://github.com/ceph/ceph/pull/54538>`_, Aashish Sharma)
* mgr/dashboard: Simplify authentication protocol (`pr#55689 <https://github.com/ceph/ceph/pull/55689>`_, Daniel Persson)
* mgr/dashboard: subvolume snapshot management (`pr#55186 <https://github.com/ceph/ceph/pull/55186>`_, Nizamudeen A)
* mgr/dashboard: update fedora link for dashboard-cephadm-e2e test (`pr#54718 <https://github.com/ceph/ceph/pull/54718>`_, Adam King)
* mgr/dashboard: upgrade from old 'graph' type panels to the new 'timeseries' panel (`pr#56652 <https://github.com/ceph/ceph/pull/56652>`_, Aashish Sharma)
* mgr/dashboard:Update encryption and tags in bucket form (`pr#56707 <https://github.com/ceph/ceph/pull/56707>`_, Afreen)
* mgr/dashboard:Use advanced fieldset for rbd image (`pr#56710 <https://github.com/ceph/ceph/pull/56710>`_, Afreen)
* mgr/nfs: include pseudo in JSON output when nfs export apply -i fails (`pr#55394 <https://github.com/ceph/ceph/pull/55394>`_, Dhairya Parmar)
* mgr/node-proxy: handle 'None' statuses returned by RedFish (`pr#55999 <https://github.com/ceph/ceph/pull/55999>`_, Guillaume Abrioux)
* mgr/pg_autoscaler: add check for norecover flag (`pr#55078 <https://github.com/ceph/ceph/pull/55078>`_, Aishwarya Mathuria)
* mgr/snap_schedule: add support for monthly snapshots (`pr#55208 <https://github.com/ceph/ceph/pull/55208>`_, Milind Changire)
* mgr/snap_schedule: exceptions management and subvol support (`pr#52751 <https://github.com/ceph/ceph/pull/52751>`_, Milind Changire)
* mgr/volumes: fix `subvolume group rm` error message (`pr#54207 <https://github.com/ceph/ceph/pull/54207>`_, neeraj pratap singh, Neeraj Pratap Singh)
* mgr/volumes: support to reject CephFS clones if cloner threads are not available (`pr#55692 <https://github.com/ceph/ceph/pull/55692>`_, Rishabh Dave, Venky Shankar, Neeraj Pratap Singh)
* mgr: pin pytest to version 7.4.4 (`pr#55362 <https://github.com/ceph/ceph/pull/55362>`_, Laura Flores)
* mon, doc: overriding ec profile requires --yes-i-really-mean-it (`pr#56435 <https://github.com/ceph/ceph/pull/56435>`_, Radoslaw Zarzynski)
* mon, osd, \*: expose upmap-primary in OSDMap::get_features() (`pr#57794 <https://github.com/ceph/ceph/pull/57794>`_, rzarzynski)
* mon/ConfigMonitor: Show localized name in "config dump --format json" output (`pr#53888 <https://github.com/ceph/ceph/pull/53888>`_, Sridhar Seshasayee)
* mon/ConnectionTracker.cc: disregard connection scores from mon_rank = -1 (`pr#55167 <https://github.com/ceph/ceph/pull/55167>`_, Kamoltat)
* mon/OSDMonitor: fix get_min_last_epoch_clean() (`pr#55867 <https://github.com/ceph/ceph/pull/55867>`_, Matan Breizman)
* mon: fix health store size growing infinitely (`pr#55548 <https://github.com/ceph/ceph/pull/55548>`_, Wei Wang)
* mon: fix mds metadata lost in one case (`pr#54316 <https://github.com/ceph/ceph/pull/54316>`_, shimin)
* msg: update MOSDOp() to use ceph_tid_t instead of long (`pr#55424 <https://github.com/ceph/ceph/pull/55424>`_, Lucian Petrut)
* node-proxy: fix RedFishClient.logout() method (`pr#56252 <https://github.com/ceph/ceph/pull/56252>`_, Guillaume Abrioux)
* node-proxy: refactor entrypoint (backport) (`pr#55454 <https://github.com/ceph/ceph/pull/55454>`_, Guillaume Abrioux)
* orch: implement hardware monitoring (`pr#55405 <https://github.com/ceph/ceph/pull/55405>`_, Guillaume Abrioux, Adam King, Redouane Kachach)
* orchestrator: Add summary line to orch device ls output (`pr#56098 <https://github.com/ceph/ceph/pull/56098>`_, Paul Cuzner)
* orchestrator: Fix representation of CPU threads in host ls --detail command (`pr#56097 <https://github.com/ceph/ceph/pull/56097>`_, Paul Cuzner)
* os/bluestore: add bluestore fragmentation micros to prometheus (`pr#54258 <https://github.com/ceph/ceph/pull/54258>`_, Yite Gu)
* os/bluestore: fix free space update after bdev-expand in NCB mode (`pr#55777 <https://github.com/ceph/ceph/pull/55777>`_, Igor Fedotov)
* os/bluestore: get rid off resulting lba alignment in allocators (`pr#54772 <https://github.com/ceph/ceph/pull/54772>`_, Igor Fedotov)
* os/kv_test: Fix estimate functions (`pr#56197 <https://github.com/ceph/ceph/pull/56197>`_, Adam Kupczyk)
* osd/OSD: introduce reset_purged_snaps_last (`pr#53972 <https://github.com/ceph/ceph/pull/53972>`_, Matan Breizman)
* osd/scrub: increasing max_osd_scrubs to 3 (`pr#55173 <https://github.com/ceph/ceph/pull/55173>`_, Ronen Friedman)
* osd: Apply randomly selected scheduler type across all OSD shards (`pr#54981 <https://github.com/ceph/ceph/pull/54981>`_, Sridhar Seshasayee)
* osd: don't require RWEXCL lock for stat+write ops (`pr#54595 <https://github.com/ceph/ceph/pull/54595>`_, Alice Zhao)
* osd: fix Incremental decode for new/old_pg_upmap_primary (`pr#55046 <https://github.com/ceph/ceph/pull/55046>`_, Laura Flores)
* osd: improve OSD robustness (`pr#54783 <https://github.com/ceph/ceph/pull/54783>`_, Igor Fedotov)
* osd: log the number of extents for sparse read (`pr#54606 <https://github.com/ceph/ceph/pull/54606>`_, Xiubo Li)
* osd: Tune snap trim item cost to reflect a PGs' average object size for mClock scheduler (`pr#55040 <https://github.com/ceph/ceph/pull/55040>`_, Sridhar Seshasayee)
* pybind/mgr/devicehealth: replace SMART data if exists for same DATETIME (`pr#54879 <https://github.com/ceph/ceph/pull/54879>`_, Patrick Donnelly)
* pybind/mgr/devicehealth: skip legacy objects that cannot be loaded (`pr#56479 <https://github.com/ceph/ceph/pull/56479>`_, Patrick Donnelly)
* pybind/mgr/mirroring: drop mon_host from peer_list (`pr#55237 <https://github.com/ceph/ceph/pull/55237>`_, Jos Collin)
* pybind/rbd: fix compilation with cython3 (`pr#54807 <https://github.com/ceph/ceph/pull/54807>`_, Mykola Golub)
* python-common/drive_selection: fix limit with existing devices (`pr#56096 <https://github.com/ceph/ceph/pull/56096>`_, Adam King)
* python-common: fix osdspec_affinity check (`pr#56095 <https://github.com/ceph/ceph/pull/56095>`_, Guillaume Abrioux)
* qa/cephadm: testing for extra daemon/container features (`pr#55957 <https://github.com/ceph/ceph/pull/55957>`_, Adam King)
* qa/cephfs: improvements for name generators in test_volumes.py (`pr#54729 <https://github.com/ceph/ceph/pull/54729>`_, Rishabh Dave)
* qa/distros: remove centos 8 from supported distros (`pr#57932 <https://github.com/ceph/ceph/pull/57932>`_, Guillaume Abrioux, Casey Bodley, Adam King, Laura Flores)
* qa/suites/fs/nfs: use standard health ignorelist (`pr#56392 <https://github.com/ceph/ceph/pull/56392>`_, Patrick Donnelly)
* qa/suites/fs/workload: enable snap_schedule early (`pr#56424 <https://github.com/ceph/ceph/pull/56424>`_, Patrick Donnelly)
* qa/tasks/cephfs/test_misc: switch duration to timeout (`pr#55746 <https://github.com/ceph/ceph/pull/55746>`_, Xiubo Li)
* qa/tests: added the initial reef-p2p suite (`pr#55714 <https://github.com/ceph/ceph/pull/55714>`_, Yuri Weinstein)
* qa/workunits/rbd/cli_generic.sh: narrow race window when checking that rbd_support module command fails after blocklisting the module's client (`pr#54769 <https://github.com/ceph/ceph/pull/54769>`_, Ramana Raja)
* qa: `fs volume rename` requires `fs fail` and `refuse_client_session` set (`issue#64174 <http://tracker.ceph.com/issues/64174>`_, `pr#56171 <https://github.com/ceph/ceph/pull/56171>`_, Venky Shankar)
* qa: Add benign cluster warning from ec-inconsistent-hinfo test to ignorelist (`pr#56151 <https://github.com/ceph/ceph/pull/56151>`_, Sridhar Seshasayee)
* qa: add centos_latest (9.stream) and ubuntu_20.04 yamls to supported-all-distro (`pr#54677 <https://github.com/ceph/ceph/pull/54677>`_, Venky Shankar)
* qa: add diff-continuous and compare-mirror-image tests to rbd and krbd suites respectively (`pr#55928 <https://github.com/ceph/ceph/pull/55928>`_, Ramana Raja)
* qa: Add tests to validate synced images on rbd-mirror (`pr#55762 <https://github.com/ceph/ceph/pull/55762>`_, Ilya Dryomov, Ramana Raja)
* qa: bump up scrub status command timeout (`pr#55915 <https://github.com/ceph/ceph/pull/55915>`_, Milind Changire)
* qa: change log-whitelist to log-ignorelist (`pr#56396 <https://github.com/ceph/ceph/pull/56396>`_, Patrick Donnelly)
* qa: correct usage of DEBUGFS_META_DIR in dedent (`pr#56167 <https://github.com/ceph/ceph/pull/56167>`_, Venky Shankar)
* qa: do upgrades from quincy and older reef minor releases (`pr#55590 <https://github.com/ceph/ceph/pull/55590>`_, Patrick Donnelly)
* qa: enhance labeled perf counters test for cephfs-mirror (`pr#56211 <https://github.com/ceph/ceph/pull/56211>`_, Jos Collin)
* qa: Fix fs/full suite (`pr#55829 <https://github.com/ceph/ceph/pull/55829>`_, Kotresh HR)
* qa: fix incorrectly using the wait_for_health() helper (`issue#57985 <http://tracker.ceph.com/issues/57985>`_, `pr#54237 <https://github.com/ceph/ceph/pull/54237>`_, Venky Shankar)
* qa: fix rank_asok() to handle errors from asok commands (`pr#55302 <https://github.com/ceph/ceph/pull/55302>`_, Neeraj Pratap Singh)
* qa: ignore container checkpoint/restore related selinux denials for centos9 (`issue#64616 <http://tracker.ceph.com/issues/64616>`_, `pr#56019 <https://github.com/ceph/ceph/pull/56019>`_, Venky Shankar)
* qa: remove error string checks and check w/ return value (`pr#55943 <https://github.com/ceph/ceph/pull/55943>`_, Venky Shankar)
* qa: remove vstart runner from radosgw_admin task (`pr#55097 <https://github.com/ceph/ceph/pull/55097>`_, Ali Maredia)
* qa: run kernel_untar_build with newer tarball (`pr#54711 <https://github.com/ceph/ceph/pull/54711>`_, Milind Changire)
* qa: set mds config with `config set` for a particular test (`issue#57087 <http://tracker.ceph.com/issues/57087>`_, `pr#56169 <https://github.com/ceph/ceph/pull/56169>`_, Venky Shankar)
* qa: use correct imports to resolve fuse_mount and kernel_mount (`pr#54714 <https://github.com/ceph/ceph/pull/54714>`_, Milind Changire)
* qa: use exisitng ignorelist override list for fs:mirror[-ha] (`issue#62482 <http://tracker.ceph.com/issues/62482>`_, `pr#54766 <https://github.com/ceph/ceph/pull/54766>`_, Venky Shankar)
* radosgw-admin: 'zone set' won't overwrite existing default-placement (`pr#55061 <https://github.com/ceph/ceph/pull/55061>`_, Casey Bodley)
* rbd-nbd: fix resize of images mapped using netlink (`pr#55316 <https://github.com/ceph/ceph/pull/55316>`_, Ramana Raja)
* reef backport: rook e2e testing related PRs (`pr#55375 <https://github.com/ceph/ceph/pull/55375>`_, Redouane Kachach)
* RGW - Swift retarget needs bucket set on object (`pr#56004 <https://github.com/ceph/ceph/pull/56004>`_, Daniel Gryniewicz)
* rgw/auth: Fix the return code returned by AuthStrategy (`pr#54794 <https://github.com/ceph/ceph/pull/54794>`_, Pritha Srivastava)
* rgw/beast: Enable SSL session-id reuse speedup mechanism (`pr#56120 <https://github.com/ceph/ceph/pull/56120>`_, Mark Kogan)
* rgw/datalog: RGWDataChangesLog::add_entry() uses null_yield (`pr#55655 <https://github.com/ceph/ceph/pull/55655>`_, Casey Bodley)
* rgw/iam: admin/system users ignore iam policy parsing errors (`pr#54843 <https://github.com/ceph/ceph/pull/54843>`_, Casey Bodley)
* rgw/kafka/amqp: fix race conditionn in async completion handlers (`pr#54736 <https://github.com/ceph/ceph/pull/54736>`_, Yuval Lifshitz)
* rgw/lc: do not add datalog/bilog for some lc actions (`pr#55289 <https://github.com/ceph/ceph/pull/55289>`_, Juan Zhu)
* rgw/lua: fix CopyFrom crash (`pr#54296 <https://github.com/ceph/ceph/pull/54296>`_, Yuval Lifshitz)
* rgw/notification: Kafka persistent notifications not retried and removed even when the broker is down (`pr#56140 <https://github.com/ceph/ceph/pull/56140>`_, kchheda3)
* rgw/putobj: RadosWriter uses part head object for multipart parts (`pr#55621 <https://github.com/ceph/ceph/pull/55621>`_, Casey Bodley)
* rgw/rest: fix url decode of post params for iam/sts/sns (`pr#55356 <https://github.com/ceph/ceph/pull/55356>`_, Casey Bodley)
* rgw/S3select: remove assert from csv-parser, adding updates (`pr#55969 <https://github.com/ceph/ceph/pull/55969>`_, Gal Salomon)
* RGW/STS: when generating keys, take the trailing null character into account (`pr#54127 <https://github.com/ceph/ceph/pull/54127>`_, Oguzhan Ozmen)
* rgw: add headers to guide cache update in 304 response (`pr#55094 <https://github.com/ceph/ceph/pull/55094>`_, Casey Bodley, Ilsoo Byun)
* rgw: Add missing empty checks to the split string in is_string_in_set() (`pr#56347 <https://github.com/ceph/ceph/pull/56347>`_, Matt Benjamin)
* rgw: d3n: fix valgrind reported leak related to libaio worker threads (`pr#54852 <https://github.com/ceph/ceph/pull/54852>`_, Mark Kogan)
* rgw: do not copy olh attributes in versioning suspended bucket (`pr#55606 <https://github.com/ceph/ceph/pull/55606>`_, Juan Zhu)
* rgw: fix cloud-sync multi-tenancy scenario (`pr#54328 <https://github.com/ceph/ceph/pull/54328>`_, Ionut Balutoiu)
* rgw: object lock avoids 32-bit truncation of RetainUntilDate (`pr#54674 <https://github.com/ceph/ceph/pull/54674>`_, Casey Bodley)
* rgw: only buckets with reshardable layouts need to be considered for resharding (`pr#54129 <https://github.com/ceph/ceph/pull/54129>`_, J. Eric Ivancich)
* RGW: pubsub publish commit with etag populated (`pr#56453 <https://github.com/ceph/ceph/pull/56453>`_, Ali Masarwa)
* rgw: RGWSI_SysObj_Cache::remove() invalidates after successful delete (`pr#55716 <https://github.com/ceph/ceph/pull/55716>`_, Casey Bodley)
* rgw: SignatureDoesNotMatch for certain RGW Admin Ops endpoints w/v4 auth (`pr#54791 <https://github.com/ceph/ceph/pull/54791>`_, David.Hall)
* Snapshot schedule show subvolume path (`pr#56419 <https://github.com/ceph/ceph/pull/56419>`_, Ivo Almeida)
* src/common/options: Correct typo in rgw.yaml.in (`pr#55445 <https://github.com/ceph/ceph/pull/55445>`_, Anthony D'Atri)
* src/mount: kernel mount command returning misleading error message (`pr#55300 <https://github.com/ceph/ceph/pull/55300>`_, Neeraj Pratap Singh)
* test/libcephfs: skip flaky timestamp assertion on Windows (`pr#54614 <https://github.com/ceph/ceph/pull/54614>`_, Lucian Petrut)
* test/rgw: increase timeouts in unittest_rgw_dmclock_scheduler (`pr#55790 <https://github.com/ceph/ceph/pull/55790>`_, Casey Bodley)
* test: explicitly link to ceph-common for some libcephfs tests (`issue#57206 <http://tracker.ceph.com/issues/57206>`_, `pr#53635 <https://github.com/ceph/ceph/pull/53635>`_, Venky Shankar)
* tools/ceph_objectstore_tool: action_on_all_objects_in_pg to skip pgmeta (`pr#54693 <https://github.com/ceph/ceph/pull/54693>`_, Matan Breizman)
* Tools/rados: Improve Error Messaging for Object Name Resolution (`pr#55112 <https://github.com/ceph/ceph/pull/55112>`_, Nitzan Mordechai)
* tools/rbd: make 'children' command support --image-id (`pr#55617 <https://github.com/ceph/ceph/pull/55617>`_, Mykola Golub)
* use raw_cluster_cmd instead of run_ceph_cmd (`pr#55836 <https://github.com/ceph/ceph/pull/55836>`_, Venky Shankar)
* win32_deps_build.sh: change Boost URL (`pr#55084 <https://github.com/ceph/ceph/pull/55084>`_, Lucian Petrut)

v18.2.2 Reef
============

This is a hotfix release that resolves several flaws including Prometheus crashes and an encoder fix.

Release Date
------------

March 11, 2024

Notable Changes
---------------

* mgr/Prometheus: refine the orchestrator availability check to prevent against crashes in the prometheus module during startup. Introduce additional checks to handle daemon_ids generated within the Rook environment, thus preventing potential issues during RGW metrics metadata generation.

Changelog
---------

* mgr/prometheus: fix orch check to prevent Prometheus crash (`pr#55491 <https://github.com/ceph/ceph/pull/55491>`_, Redouane Kachach)
* debian/\*.postinst: add adduser as a dependency and specify --home when adduser (`pr#55709 <https://github.com/ceph/ceph/pull/55709>`_, Kefu Chai)
* src/osd/OSDMap.cc: Fix encoder to produce same bytestream (`pr#55712 <https://github.com/ceph/ceph/pull/55712>`_, Kamoltat)

v18.2.1 Reef
============

This is the first backport release in the Reef series, and the first with Debian packages,
for Debian Bookworm. We recommend that all users update to this release.

Release Date
------------

December 18, 2023

Notable Changes
---------------

* RGW: S3 multipart uploads using Server-Side Encryption now replicate correctly in
  a multi-site deployment. Previously, the replicas of such objects were corrupted on
  decryption. A new command, ``radosgw-admin bucket resync encrypted multipart``, can be
  used to identify these original multipart uploads. The ``LastModified`` timestamp of
  any identified object is incremented by 1ns to cause peer zones to replicate it again.
  For multi-site deployments that make any use of Server-Side Encryption, we
  recommended running this command against every bucket in every zone after all
  zones have upgraded.

* CEPHFS: MDS now evicts clients which are not advancing their request tids (transaction IDs),
  which causes a large buildup of session metadata, resulting in the MDS going read-only due to
  the RADOS operation exceeding the size threshold. `mds_session_metadata_threshold`
  config controls the maximum size that an (encoded) session metadata can grow.

* RGW: New tools have been added to ``radosgw-admin`` for identifying and
  correcting issues with versioned bucket indexes. Historical bugs with the
  versioned bucket index transaction workflow made it possible for the index
  to accumulate extraneous "book-keeping" olh (object logical head) entries
  and plain placeholder entries. In some specific scenarios where clients made
  concurrent requests referencing the same object key, it was likely that a lot
  of extra index entries would accumulate. When a significant number of these entries are
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

* mgr/snap-schedule: For clusters with multiple CephFS file systems, all the
  snap-schedule commands now expect the '--fs' argument.

* RADOS: A ``POOL_APP_NOT_ENABLED`` health warning will now be reported if the
  application is not enabled for the pool whether the pool is in use or not.
  Always tag a pool with an application using ``ceph osd pool application
  enable`` command to avoid reporting ``POOL_APP_NOT_ENABLED`` for that pool.
  The user might temporarily mute this warning using ``ceph health mute
  POOL_APP_NOT_ENABLED``.

* Dashboard: An overview page for RGW to show the overall status of RGW components.

* Dashboard: Added management support for RGW Multi-site and CephFS Subvolumes and groups.

* Dashboard: Fixed few bugs and issues around the new dashboard page including the broken layout,
  some metrics giving wrong values and introduced a popover to display details
  when there are HEALTH_WARN or HEALTH_ERR.

* Dashboard: Fixed several issues in Ceph dashboard on Rook-backed clusters,
  and improved the user experience on the Rook environment.

Changelog
---------

* .github: Clarify checklist details (`pr#54130 <https://github.com/ceph/ceph/pull/54130>`_, Anthony D'Atri)
* [CVE-2023-43040] rgw: Fix bucket validation against POST policies (`pr#53756 <https://github.com/ceph/ceph/pull/53756>`_, Joshua Baergen)
* Adding rollback mechanism to handle bootstrap failures (`pr#53864 <https://github.com/ceph/ceph/pull/53864>`_, Adam King, Redouane Kachach)
* backport of rook orchestrator fixes and e2e automated testing (`pr#54224 <https://github.com/ceph/ceph/pull/54224>`_, Redouane Kachach)
* Bluestore: fix bluestore collection_list latency perf counter (`pr#52950 <https://github.com/ceph/ceph/pull/52950>`_, Wangwenjuan)
* build: Remove ceph-libboost\* packages in install-deps (`pr#52769 <https://github.com/ceph/ceph/pull/52769>`_, Adam Emerson)
* ceph-volume/cephadm: support lv devices in inventory (`pr#53286 <https://github.com/ceph/ceph/pull/53286>`_, Guillaume Abrioux)
* ceph-volume: add --osd-id option to raw prepare (`pr#52927 <https://github.com/ceph/ceph/pull/52927>`_, Guillaume Abrioux)
* ceph-volume: fix a regression in `raw list` (`pr#54521 <https://github.com/ceph/ceph/pull/54521>`_, Guillaume Abrioux)
* ceph-volume: fix mpath device support (`pr#53539 <https://github.com/ceph/ceph/pull/53539>`_, Guillaume Abrioux)
* ceph-volume: fix raw list for lvm devices (`pr#52619 <https://github.com/ceph/ceph/pull/52619>`_, Guillaume Abrioux)
* ceph-volume: fix raw list for lvm devices (`pr#52980 <https://github.com/ceph/ceph/pull/52980>`_, Guillaume Abrioux)
* ceph-volume: Revert "ceph-volume: fix raw list for lvm devices" (`pr#54429 <https://github.com/ceph/ceph/pull/54429>`_, Matthew Booth, Guillaume Abrioux)
* ceph: allow xlock state to be LOCK_PREXLOCK when putting it (`pr#53661 <https://github.com/ceph/ceph/pull/53661>`_, Xiubo Li)
* ceph_fs.h: add separate owner\_{u,g}id fields (`pr#53138 <https://github.com/ceph/ceph/pull/53138>`_, Alexander Mikhalitsyn)
* ceph_volume: support encrypted volumes for lvm new-db/new-wal/migrate commands (`pr#52875 <https://github.com/ceph/ceph/pull/52875>`_, Igor Fedotov)
* cephadm batch backport Aug 23 (`pr#53124 <https://github.com/ceph/ceph/pull/53124>`_, Adam King, Luis Domingues, John Mulligan, Redouane Kachach)
* cephadm: add a --dry-run option to cephadm shell (`pr#54220 <https://github.com/ceph/ceph/pull/54220>`_, John Mulligan)
* cephadm: add tcmu-runner to logrotate config (`pr#53122 <https://github.com/ceph/ceph/pull/53122>`_, Adam King)
* cephadm: Adding support to configure public_network cfg section (`pr#53110 <https://github.com/ceph/ceph/pull/53110>`_, Redouane Kachach)
* cephadm: delete /tmp/cephadm-<fsid> when removing the cluster (`pr#53109 <https://github.com/ceph/ceph/pull/53109>`_, Redouane Kachach)
* cephadm: Fix extra_container_args for iSCSI (`pr#53010 <https://github.com/ceph/ceph/pull/53010>`_, Raimund Sacherer)
* cephadm: fix haproxy version with certain containers (`pr#53751 <https://github.com/ceph/ceph/pull/53751>`_, Adam King)
* cephadm: make custom_configs work for tcmu-runner container (`pr#53404 <https://github.com/ceph/ceph/pull/53404>`_, Adam King)
* cephadm: run tcmu-runner through script to do restart on failure (`pr#53866 <https://github.com/ceph/ceph/pull/53866>`_, Adam King)
* cephadm: support for CA signed keys (`pr#53121 <https://github.com/ceph/ceph/pull/53121>`_, Adam King)
* cephfs-journal-tool: disambiguate usage of all keyword (in tool help) (`pr#53646 <https://github.com/ceph/ceph/pull/53646>`_, Manish M Yathnalli)
* cephfs-mirror: do not run concurrent C_RestartMirroring context (`issue#62072 <http://tracker.ceph.com/issues/62072>`_, `pr#53638 <https://github.com/ceph/ceph/pull/53638>`_, Venky Shankar)
* cephfs: implement snapdiff (`pr#53229 <https://github.com/ceph/ceph/pull/53229>`_, Igor Fedotov, Lucian Petrut, Denis Barahtanov)
* cephfs_mirror: correctly set top level dir permissions (`pr#53271 <https://github.com/ceph/ceph/pull/53271>`_, Milind Changire)
* client: always refresh mds feature bits on session open (`issue#63188 <http://tracker.ceph.com/issues/63188>`_, `pr#54146 <https://github.com/ceph/ceph/pull/54146>`_, Venky Shankar)
* client: correct quota check in Client::_rename() (`pr#52578 <https://github.com/ceph/ceph/pull/52578>`_, Rishabh Dave)
* client: do not send metrics until the MDS rank is ready (`pr#52501 <https://github.com/ceph/ceph/pull/52501>`_, Xiubo Li)
* client: force sending cap revoke ack always (`pr#52507 <https://github.com/ceph/ceph/pull/52507>`_, Xiubo Li)
* client: issue a cap release immediately if no cap exists (`pr#52850 <https://github.com/ceph/ceph/pull/52850>`_, Xiubo Li)
* client: move the Inode to new auth mds session when changing auth cap (`pr#53666 <https://github.com/ceph/ceph/pull/53666>`_, Xiubo Li)
* client: trigger to flush the buffer when making snapshot (`pr#52497 <https://github.com/ceph/ceph/pull/52497>`_, Xiubo Li)
* client: wait rename to finish (`pr#52504 <https://github.com/ceph/ceph/pull/52504>`_, Xiubo Li)
* cmake: ensure fmtlib is at least 8.1.1 (`pr#52970 <https://github.com/ceph/ceph/pull/52970>`_, Abhishek Lekshmanan)
* Consider setting "bulk" autoscale pool flag when automatically creating a data pool for CephFS (`pr#52899 <https://github.com/ceph/ceph/pull/52899>`_, Leonid Usov)
* crimson/admin/admin_socket: remove path file if it exists (`pr#53964 <https://github.com/ceph/ceph/pull/53964>`_, Matan Breizman)
* crimson/ertr: assert on invocability of func provided to safe_then() (`pr#53958 <https://github.com/ceph/ceph/pull/53958>`_, Radosław Zarzyński)
* crimson/mgr: Fix config show command (`pr#53954 <https://github.com/ceph/ceph/pull/53954>`_, Aishwarya Mathuria)
* crimson/net: consolidate messenger implementations and enable multi-shard UTs (`pr#54095 <https://github.com/ceph/ceph/pull/54095>`_, Yingxin Cheng)
* crimson/net: set TCP_NODELAY according to ms_tcp_nodelay (`pr#54063 <https://github.com/ceph/ceph/pull/54063>`_, Xuehan Xu)
* crimson/net: support connections in multiple shards (`pr#53949 <https://github.com/ceph/ceph/pull/53949>`_, Yingxin Cheng)
* crimson/os/object_data_handler: splitting right side doesn't mean splitting only one extent (`pr#54061 <https://github.com/ceph/ceph/pull/54061>`_, Xuehan Xu)
* crimson/os/seastore/backref_manager: scan backref entries by journal seq (`pr#53939 <https://github.com/ceph/ceph/pull/53939>`_, Zhang Song)
* crimson/os/seastore/btree: should add left's size when merging levels… (`pr#53946 <https://github.com/ceph/ceph/pull/53946>`_, Xuehan Xu)
* crimson/os/seastore/cache: don't add EXIST_CLEAN extents to lru (`pr#54098 <https://github.com/ceph/ceph/pull/54098>`_, Xuehan Xu)
* crimson/os/seastore/cached_extent: add prepare_commit interface (`pr#53941 <https://github.com/ceph/ceph/pull/53941>`_, Xuehan Xu)
* crimson/os/seastore/cbj: fix a potential overflow bug on segment_seq (`pr#53968 <https://github.com/ceph/ceph/pull/53968>`_, Myoungwon Oh)
* crimson/os/seastore/collection_manager: fill CollectionNode::decoded on clean reads (`pr#53956 <https://github.com/ceph/ceph/pull/53956>`_, Xuehan Xu)
* crimson/os/seastore/journal/cbj: generalize scan_valid_records() (`pr#53961 <https://github.com/ceph/ceph/pull/53961>`_, Myoungwon Oh, Yingxin Cheng)
* crimson/os/seastore/omap_manager: correct editor settings (`pr#53947 <https://github.com/ceph/ceph/pull/53947>`_, Zhang Song)
* crimson/os/seastore/omap_manager: fix the entry leak issue in BtreeOMapManager::omap_list() (`pr#53962 <https://github.com/ceph/ceph/pull/53962>`_, Xuehan Xu)
* crimson/os/seastore/onode_manager: populate value recorders of onodes to be erased (`pr#53966 <https://github.com/ceph/ceph/pull/53966>`_, Xuehan Xu)
* crimson/os/seastore/rbm: make rbm support multiple shards (`pr#53952 <https://github.com/ceph/ceph/pull/53952>`_, Myoungwon Oh)
* crimson/os/seastore/transaction_manager: data loss issues (`pr#53955 <https://github.com/ceph/ceph/pull/53955>`_, Xuehan Xu)
* crimson/os/seastore/transaction_manager: move intermediate_key by "remap_offset" when remapping the "back" half of the original pin (`pr#54140 <https://github.com/ceph/ceph/pull/54140>`_, Xuehan Xu)
* crimson/os/seastore/zbd: zbdsegmentmanager write path fixes (`pr#54062 <https://github.com/ceph/ceph/pull/54062>`_, Aravind Ramesh)
* crimson/os/seastore: add metrics about total invalidated transactions (`pr#53953 <https://github.com/ceph/ceph/pull/53953>`_, Zhang Song)
* crimson/os/seastore: create page aligned bufferptr in copy ctor of CachedExtent (`pr#54097 <https://github.com/ceph/ceph/pull/54097>`_, Zhang Song)
* crimson/os/seastore: enable SMR HDD (`pr#53935 <https://github.com/ceph/ceph/pull/53935>`_, Aravind Ramesh)
* crimson/os/seastore: fix ceph_assert in segment_manager.h (`pr#53938 <https://github.com/ceph/ceph/pull/53938>`_, Aravind Ramesh)
* crimson/os/seastore: fix daggling reference of oid in SeaStore::Shard::stat() (`pr#53960 <https://github.com/ceph/ceph/pull/53960>`_, Xuehan Xu)
* crimson/os/seastore: fix in check_node (`pr#53945 <https://github.com/ceph/ceph/pull/53945>`_, Xinyu Huang)
* crimson/os/seastore: OP_CLONE in seastore (`pr#54092 <https://github.com/ceph/ceph/pull/54092>`_, xuxuehan, Xuehan Xu)
* crimson/os/seastore: realize lazy read in split overwrite with overwrite refactor (`pr#53951 <https://github.com/ceph/ceph/pull/53951>`_, Xinyu Huang)
* crimson/os/seastore: retire_extent_addr clean up (`pr#53959 <https://github.com/ceph/ceph/pull/53959>`_, Xinyu Huang)
* crimson/osd/heartbeat: Improve maybe_share_osdmap behavior (`pr#53940 <https://github.com/ceph/ceph/pull/53940>`_, Samuel Just)
* crimson/osd/lsan_suppressions.cc: Add MallocExtension::Initialize() (`pr#54057 <https://github.com/ceph/ceph/pull/54057>`_, Mark Nelson, Matan Breizman)
* crimson/osd/lsan_suppressions: add MallocExtension::Register (`pr#54139 <https://github.com/ceph/ceph/pull/54139>`_, Matan Breizman)
* crimson/osd/object_context: consider clones found as long as they're in SnapSet::clones (`pr#53965 <https://github.com/ceph/ceph/pull/53965>`_, Xuehan Xu)
* crimson/osd/osd_operations: add pipeline to LogMissingRequest to sync it (`pr#53957 <https://github.com/ceph/ceph/pull/53957>`_, Xuehan Xu)
* crimson/osd/osd_operations: consistent naming to pipeline users (`pr#54060 <https://github.com/ceph/ceph/pull/54060>`_, Matan Breizman)
* crimson/osd/pg: check if backfill_state exists when judging objects' (`pr#53963 <https://github.com/ceph/ceph/pull/53963>`_, Xuehan Xu)
* crimson/osd/watch: Add logs around Watch/Notify (`pr#53950 <https://github.com/ceph/ceph/pull/53950>`_, Matan Breizman)
* crimson/osd: add embedded suppression ruleset for LSan (`pr#53937 <https://github.com/ceph/ceph/pull/53937>`_, Radoslaw Zarzynski)
* crimson/osd: cleanup and drop OSD::ShardDispatcher (`pr#54138 <https://github.com/ceph/ceph/pull/54138>`_, Yingxin Cheng)
* Crimson/osd: Disable concurrent MOSDMap handling (`pr#53944 <https://github.com/ceph/ceph/pull/53944>`_, Matan Breizman)
* crimson/osd: don't ignore start_pg_operation returned future (`pr#53948 <https://github.com/ceph/ceph/pull/53948>`_, Matan Breizman)
* crimson/osd: fix ENOENT on accessing RadosGW user's index of buckets (`pr#53942 <https://github.com/ceph/ceph/pull/53942>`_, Radoslaw Zarzynski)
* crimson/osd: fix Notify life-time mismanagement in Watch::notify_ack (`pr#53943 <https://github.com/ceph/ceph/pull/53943>`_, Radoslaw Zarzynski)
* crimson/osd: fixes and cleanups around multi-core OSD (`pr#54091 <https://github.com/ceph/ceph/pull/54091>`_, Yingxin Cheng)
* Crimson/osd: support multicore osd (`pr#54058 <https://github.com/ceph/ceph/pull/54058>`_, chunmei)
* crimson/tools/perf_crimson_msgr: integrate multi-core msgr with various improvements (`pr#54059 <https://github.com/ceph/ceph/pull/54059>`_, Yingxin Cheng)
* crimson/tools/perf_crimson_msgr: randomize client nonce (`pr#54093 <https://github.com/ceph/ceph/pull/54093>`_, Yingxin Cheng)
* crimson/tools/perf_staged_fltree: fix compile error (`pr#54096 <https://github.com/ceph/ceph/pull/54096>`_, Myoungwon Oh)
* crimson/vstart: default seastore_device_size will be out of space f… (`pr#53969 <https://github.com/ceph/ceph/pull/53969>`_, chunmei)
* crimson: Enable tcmalloc when using seastar (`pr#54105 <https://github.com/ceph/ceph/pull/54105>`_, Mark Nelson, Matan Breizman)
* debian/control: add docker-ce as recommends for cephadm package (`pr#52908 <https://github.com/ceph/ceph/pull/52908>`_, Adam King)
* Debian: update to dh compat 12, fix more serious packaging errors, correct copyright syntax (`pr#53654 <https://github.com/ceph/ceph/pull/53654>`_, Matthew Vernon)
* doc/architecture.rst - edit a sentence (`pr#53372 <https://github.com/ceph/ceph/pull/53372>`_, Zac Dover)
* doc/architecture.rst - edit up to "Cluster Map" (`pr#53366 <https://github.com/ceph/ceph/pull/53366>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (`pr#53619 <https://github.com/ceph/ceph/pull/53619>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (one of several) (`pr#53585 <https://github.com/ceph/ceph/pull/53585>`_, Zac Dover)
* doc/architecture: "Edit HA Auth" (one of several) (`pr#53491 <https://github.com/ceph/ceph/pull/53491>`_, Zac Dover)
* doc/architecture: edit "Calculating PG IDs" (`pr#53748 <https://github.com/ceph/ceph/pull/53748>`_, Zac Dover)
* doc/architecture: edit "Cluster Map" (`pr#53434 <https://github.com/ceph/ceph/pull/53434>`_, Zac Dover)
* doc/architecture: edit "Data Scrubbing" (`pr#53730 <https://github.com/ceph/ceph/pull/53730>`_, Zac Dover)
* doc/architecture: Edit "HA Auth" (`pr#53488 <https://github.com/ceph/ceph/pull/53488>`_, Zac Dover)
* doc/architecture: edit "HA Authentication" (`pr#53632 <https://github.com/ceph/ceph/pull/53632>`_, Zac Dover)
* doc/architecture: edit "High Avail. Monitors" (`pr#53451 <https://github.com/ceph/ceph/pull/53451>`_, Zac Dover)
* doc/architecture: edit "OSD Membership and Status" (`pr#53727 <https://github.com/ceph/ceph/pull/53727>`_, Zac Dover)
* doc/architecture: edit "OSDs service clients directly" (`pr#53686 <https://github.com/ceph/ceph/pull/53686>`_, Zac Dover)
* doc/architecture: edit "Peering and Sets" (`pr#53871 <https://github.com/ceph/ceph/pull/53871>`_, Zac Dover)
* doc/architecture: edit "Replication" (`pr#53738 <https://github.com/ceph/ceph/pull/53738>`_, Zac Dover)
* doc/architecture: edit "SDEH" (`pr#53659 <https://github.com/ceph/ceph/pull/53659>`_, Zac Dover)
* doc/architecture: edit several sections (`pr#53742 <https://github.com/ceph/ceph/pull/53742>`_, Zac Dover)
* doc/architecture: repair RBD sentence (`pr#53877 <https://github.com/ceph/ceph/pull/53877>`_, Zac Dover)
* doc/ceph-volume: explain idempotence (`pr#54233 <https://github.com/ceph/ceph/pull/54233>`_, Zac Dover)
* doc/ceph-volume: improve front matter (`pr#54235 <https://github.com/ceph/ceph/pull/54235>`_, Zac Dover)
* doc/cephadm/services: remove excess rendered indentation in osd.rst (`pr#54323 <https://github.com/ceph/ceph/pull/54323>`_, Ville Ojamo)
* doc/cephadm: add ssh note to install.rst (`pr#53199 <https://github.com/ceph/ceph/pull/53199>`_, Zac Dover)
* doc/cephadm: edit "Adding Hosts" in install.rst (`pr#53224 <https://github.com/ceph/ceph/pull/53224>`_, Zac Dover)
* doc/cephadm: edit sentence in mgr.rst (`pr#53164 <https://github.com/ceph/ceph/pull/53164>`_, Zac Dover)
* doc/cephadm: edit troubleshooting.rst (1 of x) (`pr#54283 <https://github.com/ceph/ceph/pull/54283>`_, Zac Dover)
* doc/cephadm: edit troubleshooting.rst (2 of x) (`pr#54320 <https://github.com/ceph/ceph/pull/54320>`_, Zac Dover)
* doc/cephadm: fix typo in cephadm initial crush location section (`pr#52887 <https://github.com/ceph/ceph/pull/52887>`_, John Mulligan)
* doc/cephadm: fix typo in set ssh key command (`pr#54388 <https://github.com/ceph/ceph/pull/54388>`_, Piotr Parczewski)
* doc/cephadm: update cephadm reef version (`pr#53162 <https://github.com/ceph/ceph/pull/53162>`_, Rongqi Sun)
* doc/cephfs: edit mount-using-fuse.rst (`pr#54353 <https://github.com/ceph/ceph/pull/54353>`_, Jaanus Torp)
* doc/cephfs: write cephfs commands fully in docs (`pr#53402 <https://github.com/ceph/ceph/pull/53402>`_, Rishabh Dave)
* doc/config: edit "ceph-conf.rst" (`pr#54463 <https://github.com/ceph/ceph/pull/54463>`_, Zac Dover)
* doc/configuration: edit "bg" in mon-config-ref.rst (`pr#53347 <https://github.com/ceph/ceph/pull/53347>`_, Zac Dover)
* doc/dev/release-checklist: check telemetry validation (`pr#52805 <https://github.com/ceph/ceph/pull/52805>`_, Yaarit Hatuka)
* doc/dev: Fix typos in files cephfs-mirroring.rst and deduplication.rst (`pr#53519 <https://github.com/ceph/ceph/pull/53519>`_, Daniel Parkes)
* doc/dev: remove cache-pool (`pr#54007 <https://github.com/ceph/ceph/pull/54007>`_, Zac Dover)
* doc/glossary: add "primary affinity" to glossary (`pr#53427 <https://github.com/ceph/ceph/pull/53427>`_, Zac Dover)
* doc/glossary: add "Quorum" to glossary (`pr#54509 <https://github.com/ceph/ceph/pull/54509>`_, Zac Dover)
* doc/glossary: improve "BlueStore" entry (`pr#54265 <https://github.com/ceph/ceph/pull/54265>`_, Zac Dover)
* doc/man/8/ceph-monstore-tool: add documentation (`pr#52872 <https://github.com/ceph/ceph/pull/52872>`_, Matan Breizman)
* doc/man/8: improve radosgw-admin.rst (`pr#53267 <https://github.com/ceph/ceph/pull/53267>`_, Anthony D'Atri)
* doc/man: edit ceph-monstore-tool.rst (`pr#53476 <https://github.com/ceph/ceph/pull/53476>`_, Zac Dover)
* doc/man: radosgw-admin.rst typo (`pr#53315 <https://github.com/ceph/ceph/pull/53315>`_, Zac Dover)
* doc/man: remove docs about support for unix domain sockets (`pr#53312 <https://github.com/ceph/ceph/pull/53312>`_, Zac Dover)
* doc/man: s/kvstore-tool/monstore-tool/ (`pr#53536 <https://github.com/ceph/ceph/pull/53536>`_, Zac Dover)
* doc/rados/configuration: Avoid repeating "support" in msgr2.rst (`pr#52998 <https://github.com/ceph/ceph/pull/52998>`_, Ville Ojamo)
* doc/rados: add bulk flag to pools.rst (`pr#53317 <https://github.com/ceph/ceph/pull/53317>`_, Zac Dover)
* doc/rados: edit "troubleshooting-mon" (`pr#54502 <https://github.com/ceph/ceph/pull/54502>`_, Zac Dover)
* doc/rados: edit memory-profiling.rst (`pr#53932 <https://github.com/ceph/ceph/pull/53932>`_, Zac Dover)
* doc/rados: edit operations/add-or-rm-mons (1 of x) (`pr#52889 <https://github.com/ceph/ceph/pull/52889>`_, Zac Dover)
* doc/rados: edit operations/add-or-rm-mons (2 of x) (`pr#52825 <https://github.com/ceph/ceph/pull/52825>`_, Zac Dover)
* doc/rados: edit ops/control.rst (1 of x) (`pr#53811 <https://github.com/ceph/ceph/pull/53811>`_, zdover23, Zac Dover)
* doc/rados: edit ops/control.rst (2 of x) (`pr#53815 <https://github.com/ceph/ceph/pull/53815>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (1 of x) (`pr#54418 <https://github.com/ceph/ceph/pull/54418>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (2 of x) (`pr#54421 <https://github.com/ceph/ceph/pull/54421>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (3 of x) (`pr#54438 <https://github.com/ceph/ceph/pull/54438>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (4 of x) (`pr#54443 <https://github.com/ceph/ceph/pull/54443>`_, Zac Dover)
* doc/rados: edit t-mon "common issues" (5 of x) (`pr#54455 <https://github.com/ceph/ceph/pull/54455>`_, Zac Dover)
* doc/rados: edit t-mon.rst text (`pr#54349 <https://github.com/ceph/ceph/pull/54349>`_, Zac Dover)
* doc/rados: edit t-shooting-mon.rst (`pr#54427 <https://github.com/ceph/ceph/pull/54427>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (2 of x) (`pr#52839 <https://github.com/ceph/ceph/pull/52839>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (3 of x) (`pr#53879 <https://github.com/ceph/ceph/pull/53879>`_, Zac Dover)
* doc/rados: edit troubleshooting-mon.rst (4 of x) (`pr#53897 <https://github.com/ceph/ceph/pull/53897>`_, Zac Dover)
* doc/rados: edit troubleshooting-osd (1 of x) (`pr#53982 <https://github.com/ceph/ceph/pull/53982>`_, Zac Dover)
* doc/rados: Edit troubleshooting-osd (2 of x) (`pr#54000 <https://github.com/ceph/ceph/pull/54000>`_, Zac Dover)
* doc/rados: Edit troubleshooting-osd (3 of x) (`pr#54026 <https://github.com/ceph/ceph/pull/54026>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg (2 of x) (`pr#54114 <https://github.com/ceph/ceph/pull/54114>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg.rst (`pr#54228 <https://github.com/ceph/ceph/pull/54228>`_, Zac Dover)
* doc/rados: edit troubleshooting-pg.rst (1 of x) (`pr#54073 <https://github.com/ceph/ceph/pull/54073>`_, Zac Dover)
* doc/rados: edit troubleshooting.rst (`pr#53837 <https://github.com/ceph/ceph/pull/53837>`_, Zac Dover)
* doc/rados: edit troubleshooting/community.rst (`pr#53881 <https://github.com/ceph/ceph/pull/53881>`_, Zac Dover)
* doc/rados: format "initial troubleshooting" (`pr#54477 <https://github.com/ceph/ceph/pull/54477>`_, Zac Dover)
* doc/rados: format Q&A list in t-mon.rst (`pr#54345 <https://github.com/ceph/ceph/pull/54345>`_, Zac Dover)
* doc/rados: format Q&A list in tshooting-mon.rst (`pr#54366 <https://github.com/ceph/ceph/pull/54366>`_, Zac Dover)
* doc/rados: improve "scrubbing" explanation (`pr#54270 <https://github.com/ceph/ceph/pull/54270>`_, Zac Dover)
* doc/rados: parallelize t-mon headings (`pr#54461 <https://github.com/ceph/ceph/pull/54461>`_, Zac Dover)
* doc/rados: remove cache-tiering-related keys (`pr#54227 <https://github.com/ceph/ceph/pull/54227>`_, Zac Dover)
* doc/rados: remove FileStore material (in Reef) (`pr#54008 <https://github.com/ceph/ceph/pull/54008>`_, Zac Dover)
* doc/rados: remove HitSet-related key information (`pr#54217 <https://github.com/ceph/ceph/pull/54217>`_, Zac Dover)
* doc/rados: update monitoring-osd-pg.rst (`pr#52958 <https://github.com/ceph/ceph/pull/52958>`_, Zac Dover)
* doc/radosgw: Improve dynamicresharding.rst (`pr#54368 <https://github.com/ceph/ceph/pull/54368>`_, Anthony D'Atri)
* doc/radosgw: Improve language and formatting in config-ref.rst (`pr#52835 <https://github.com/ceph/ceph/pull/52835>`_, Ville Ojamo)
* doc/radosgw: multisite - edit "migrating a single-site" (`pr#53261 <https://github.com/ceph/ceph/pull/53261>`_, Qi Tao)
* doc/radosgw: update rate limit management (`pr#52910 <https://github.com/ceph/ceph/pull/52910>`_, Zac Dover)
* doc/README.md - edit "Building Ceph" (`pr#53057 <https://github.com/ceph/ceph/pull/53057>`_, Zac Dover)
* doc/README.md - improve "Running a test cluster" (`pr#53258 <https://github.com/ceph/ceph/pull/53258>`_, Zac Dover)
* doc/rgw: correct statement about default zone features (`pr#52833 <https://github.com/ceph/ceph/pull/52833>`_, Casey Bodley)
* doc/rgw: pubsub capabilities reference was removed from docs (`pr#54137 <https://github.com/ceph/ceph/pull/54137>`_, Yuval Lifshitz)
* doc/rgw: several response headers are supported (`pr#52803 <https://github.com/ceph/ceph/pull/52803>`_, Casey Bodley)
* doc/start: correct ABC test chart (`pr#53256 <https://github.com/ceph/ceph/pull/53256>`_, Dmitry Kvashnin)
* doc/start: edit os-recommendations.rst (`pr#53179 <https://github.com/ceph/ceph/pull/53179>`_, Zac Dover)
* doc/start: fix typo in hardware-recommendations.rst (`pr#54480 <https://github.com/ceph/ceph/pull/54480>`_, Anthony D'Atri)
* doc/start: Modernize and clarify hardware-recommendations.rst (`pr#54071 <https://github.com/ceph/ceph/pull/54071>`_, Anthony D'Atri)
* doc/start: refactor ABC test chart (`pr#53094 <https://github.com/ceph/ceph/pull/53094>`_, Zac Dover)
* doc/start: update "platforms" table (`pr#53075 <https://github.com/ceph/ceph/pull/53075>`_, Zac Dover)
* doc/start: update linking conventions (`pr#52912 <https://github.com/ceph/ceph/pull/52912>`_, Zac Dover)
* doc/start: update linking conventions (`pr#52841 <https://github.com/ceph/ceph/pull/52841>`_, Zac Dover)
* doc/troubleshooting: edit cpu-profiling.rst (`pr#53059 <https://github.com/ceph/ceph/pull/53059>`_, Zac Dover)
* doc: Add a note on possible deadlock on volume deletion (`pr#52946 <https://github.com/ceph/ceph/pull/52946>`_, Kotresh HR)
* doc: add note for removing (automatic) partitioning policy (`pr#53569 <https://github.com/ceph/ceph/pull/53569>`_, Venky Shankar)
* doc: Add Reef 18.2.0 release notes (`pr#52905 <https://github.com/ceph/ceph/pull/52905>`_, Zac Dover)
* doc: Add warning on manual CRUSH rule removal (`pr#53420 <https://github.com/ceph/ceph/pull/53420>`_, Alvin Owyong)
* doc: clarify upmap balancer documentation (`pr#53004 <https://github.com/ceph/ceph/pull/53004>`_, Laura Flores)
* doc: correct option name (`pr#53128 <https://github.com/ceph/ceph/pull/53128>`_, Patrick Donnelly)
* doc: do not recommend pulling cephadm from git (`pr#52997 <https://github.com/ceph/ceph/pull/52997>`_, John Mulligan)
* doc: Documentation about main Ceph metrics (`pr#54111 <https://github.com/ceph/ceph/pull/54111>`_, Juan Miguel Olmo Martínez)
* doc: edit README.md - contributing code (`pr#53049 <https://github.com/ceph/ceph/pull/53049>`_, Zac Dover)
* doc: expand and consolidate mds placement (`pr#53146 <https://github.com/ceph/ceph/pull/53146>`_, Patrick Donnelly)
* doc: Fix doc for mds cap acquisition throttle (`pr#53024 <https://github.com/ceph/ceph/pull/53024>`_, Kotresh HR)
* doc: improve submodule update command - README.md (`pr#53000 <https://github.com/ceph/ceph/pull/53000>`_, Zac Dover)
* doc: make instructions to get an updated cephadm common (`pr#53260 <https://github.com/ceph/ceph/pull/53260>`_, John Mulligan)
* doc: remove egg fragment from dev/developer_guide/running-tests-locally (`pr#53853 <https://github.com/ceph/ceph/pull/53853>`_, Dhairya Parmar)
* doc: Update dynamicresharding.rst (`pr#54329 <https://github.com/ceph/ceph/pull/54329>`_, Aliaksei Makarau)
* doc: Update mClock QOS documentation to discard osd_mclock_cost_per\_\* (`pr#54079 <https://github.com/ceph/ceph/pull/54079>`_, tanchangzhi)
* doc: update rados.cc (`pr#52967 <https://github.com/ceph/ceph/pull/52967>`_, Zac Dover)
* doc: update test cluster commands in README.md (`pr#53349 <https://github.com/ceph/ceph/pull/53349>`_, Zac Dover)
* exporter: add ceph_daemon labels to labeled counters as well (`pr#53695 <https://github.com/ceph/ceph/pull/53695>`_, avanthakkar)
* exposed the open api and telemetry links in details card (`pr#53142 <https://github.com/ceph/ceph/pull/53142>`_, cloudbehl, dpandit)
* libcephsqlite: fill 0s in unread portion of buffer (`pr#53101 <https://github.com/ceph/ceph/pull/53101>`_, Patrick Donnelly)
* librbd: kick ExclusiveLock state machine on client being blocklisted when waiting for lock (`pr#53293 <https://github.com/ceph/ceph/pull/53293>`_, Ramana Raja)
* librbd: kick ExclusiveLock state machine stalled waiting for lock from reacquire_lock() (`pr#53919 <https://github.com/ceph/ceph/pull/53919>`_, Ramana Raja)
* librbd: make CreatePrimaryRequest remove any unlinked mirror snapshots (`pr#53276 <https://github.com/ceph/ceph/pull/53276>`_, Ilya Dryomov)
* MClientRequest: properly handle ceph_mds_request_head_legacy for ext_num_retry, ext_num_fwd, owner_uid, owner_gid (`pr#54407 <https://github.com/ceph/ceph/pull/54407>`_, Alexander Mikhalitsyn)
* MDS imported_inodes metric is not updated (`pr#51698 <https://github.com/ceph/ceph/pull/51698>`_, Yongseok Oh)
* mds/FSMap: allow upgrades if no up mds (`pr#53851 <https://github.com/ceph/ceph/pull/53851>`_, Patrick Donnelly)
* mds/Server: mark a cap acquisition throttle event in the request (`pr#53168 <https://github.com/ceph/ceph/pull/53168>`_, Leonid Usov)
* mds: acquire inode snaplock in open (`pr#53183 <https://github.com/ceph/ceph/pull/53183>`_, Patrick Donnelly)
* mds: add event for batching getattr/lookup (`pr#53558 <https://github.com/ceph/ceph/pull/53558>`_, Patrick Donnelly)
* mds: adjust pre_segments_size for MDLog when trimming segments for st… (`issue#59833 <http://tracker.ceph.com/issues/59833>`_, `pr#54035 <https://github.com/ceph/ceph/pull/54035>`_, Venky Shankar)
* mds: blocklist clients with "bloated" session metadata (`issue#62873 <http://tracker.ceph.com/issues/62873>`_, `issue#61947 <http://tracker.ceph.com/issues/61947>`_, `pr#53329 <https://github.com/ceph/ceph/pull/53329>`_, Venky Shankar)
* mds: do not send split_realms for CEPH_SNAP_OP_UPDATE msg (`pr#52847 <https://github.com/ceph/ceph/pull/52847>`_, Xiubo Li)
* mds: drop locks and retry when lock set changes (`pr#53241 <https://github.com/ceph/ceph/pull/53241>`_, Patrick Donnelly)
* mds: dump locks when printing mutation ops (`pr#52975 <https://github.com/ceph/ceph/pull/52975>`_, Patrick Donnelly)
* mds: fix deadlock between unlinking and linkmerge (`pr#53497 <https://github.com/ceph/ceph/pull/53497>`_, Xiubo Li)
* mds: fix stray evaluation using scrub and introduce new option (`pr#50813 <https://github.com/ceph/ceph/pull/50813>`_, Dhairya Parmar)
* mds: Fix the linkmerge assert check (`pr#52724 <https://github.com/ceph/ceph/pull/52724>`_, Kotresh HR)
* mds: log message when exiting due to asok command (`pr#53548 <https://github.com/ceph/ceph/pull/53548>`_, Patrick Donnelly)
* mds: MDLog::_recovery_thread: handle the errors gracefully (`pr#52512 <https://github.com/ceph/ceph/pull/52512>`_, Jos Collin)
* mds: session ls command appears twice in command listing (`pr#52515 <https://github.com/ceph/ceph/pull/52515>`_, Neeraj Pratap Singh)
* mds: skip forwarding request if the session were removed (`pr#52846 <https://github.com/ceph/ceph/pull/52846>`_, Xiubo Li)
* mds: update mdlog perf counters during replay (`pr#52681 <https://github.com/ceph/ceph/pull/52681>`_, Patrick Donnelly)
* mds: use variable g_ceph_context directly in MDSAuthCaps (`pr#52819 <https://github.com/ceph/ceph/pull/52819>`_, Rishabh Dave)
* mgr/cephadm: Add "networks" parameter to orch apply rgw (`pr#53120 <https://github.com/ceph/ceph/pull/53120>`_, Teoman ONAY)
* mgr/cephadm: add ability to zap OSDs' devices while draining host (`pr#53869 <https://github.com/ceph/ceph/pull/53869>`_, Adam King)
* mgr/cephadm: add is_host\_<status> functions to HostCache (`pr#53118 <https://github.com/ceph/ceph/pull/53118>`_, Adam King)
* mgr/cephadm: Adding sort-by support for ceph orch ps (`pr#53867 <https://github.com/ceph/ceph/pull/53867>`_, Redouane Kachach)
* mgr/cephadm: allow draining host without removing conf/keyring files (`pr#53123 <https://github.com/ceph/ceph/pull/53123>`_, Adam King)
* mgr/cephadm: also don't write client files/tuned profiles to maintenance hosts (`pr#53111 <https://github.com/ceph/ceph/pull/53111>`_, Adam King)
* mgr/cephadm: ceph orch add fails when ipv6 address is surrounded by square brackets (`pr#53870 <https://github.com/ceph/ceph/pull/53870>`_, Teoman ONAY)
* mgr/cephadm: don't use image tag in orch upgrade ls (`pr#53865 <https://github.com/ceph/ceph/pull/53865>`_, Adam King)
* mgr/cephadm: fix default image base in reef (`pr#53922 <https://github.com/ceph/ceph/pull/53922>`_, Adam King)
* mgr/cephadm: fix REFRESHED column of orch ps being unpopulated (`pr#53741 <https://github.com/ceph/ceph/pull/53741>`_, Adam King)
* mgr/cephadm: fix upgrades with nvmeof (`pr#53924 <https://github.com/ceph/ceph/pull/53924>`_, Adam King)
* mgr/cephadm: removing double quotes from the generated nvmeof config (`pr#53868 <https://github.com/ceph/ceph/pull/53868>`_, Redouane Kachach)
* mgr/cephadm: show meaningful messages when failing to execute cmds (`pr#53106 <https://github.com/ceph/ceph/pull/53106>`_, Redouane Kachach)
* mgr/cephadm: storing prometheus/alertmanager credentials in monstore (`pr#53119 <https://github.com/ceph/ceph/pull/53119>`_, Redouane Kachach)
* mgr/cephadm: validate host label before removing (`pr#53112 <https://github.com/ceph/ceph/pull/53112>`_, Redouane Kachach)
* mgr/dashboard: add e2e tests for cephfs management (`pr#53190 <https://github.com/ceph/ceph/pull/53190>`_, Nizamudeen A)
* mgr/dashboard: Add more decimals in latency graph (`pr#52727 <https://github.com/ceph/ceph/pull/52727>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: add port and zone endpoints to import realm token form in rgw multisite (`pr#54118 <https://github.com/ceph/ceph/pull/54118>`_, Aashish Sharma)
* mgr/dashboard: add validator for size field in the forms (`pr#53378 <https://github.com/ceph/ceph/pull/53378>`_, Nizamudeen A)
* mgr/dashboard: align charts of landing page (`pr#53543 <https://github.com/ceph/ceph/pull/53543>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: allow PUT in CORS (`pr#52705 <https://github.com/ceph/ceph/pull/52705>`_, Nizamudeen A)
* mgr/dashboard: allow tls 1.2 with a config option (`pr#53780 <https://github.com/ceph/ceph/pull/53780>`_, Nizamudeen A)
* mgr/dashboard: Block Ui fails in angular with target es2022 (`pr#54260 <https://github.com/ceph/ceph/pull/54260>`_, Aashish Sharma)
* mgr/dashboard: cephfs volume and subvolume management (`pr#53017 <https://github.com/ceph/ceph/pull/53017>`_, Pedro Gonzalez Gomez, Nizamudeen A, Pere Diaz Bou)
* mgr/dashboard: cephfs volume rm and rename (`pr#53026 <https://github.com/ceph/ceph/pull/53026>`_, avanthakkar)
* mgr/dashboard: cleanup rbd-mirror process in dashboard e2e (`pr#53220 <https://github.com/ceph/ceph/pull/53220>`_, Nizamudeen A)
* mgr/dashboard: cluster upgrade management (batch backport) (`pr#53016 <https://github.com/ceph/ceph/pull/53016>`_, avanthakkar, Nizamudeen A)
* mgr/dashboard: Dashboard RGW multisite configuration (`pr#52922 <https://github.com/ceph/ceph/pull/52922>`_, Aashish Sharma, Pedro Gonzalez Gomez, Avan Thakkar, avanthakkar)
* mgr/dashboard: disable hosts field while editing the filesystem (`pr#54069 <https://github.com/ceph/ceph/pull/54069>`_, Nizamudeen A)
* mgr/dashboard: disable promote on mirroring not enabled (`pr#52536 <https://github.com/ceph/ceph/pull/52536>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: disable protect if layering is not enabled on the image (`pr#53173 <https://github.com/ceph/ceph/pull/53173>`_, avanthakkar)
* mgr/dashboard: display the groups in cephfs subvolume tab (`pr#53394 <https://github.com/ceph/ceph/pull/53394>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: empty grafana panels for performance of daemons (`pr#52774 <https://github.com/ceph/ceph/pull/52774>`_, Avan Thakkar, avanthakkar)
* mgr/dashboard: enable protect option if layering enabled (`pr#53795 <https://github.com/ceph/ceph/pull/53795>`_, avanthakkar)
* mgr/dashboard: fix cephfs create form validator (`pr#53219 <https://github.com/ceph/ceph/pull/53219>`_, Nizamudeen A)
* mgr/dashboard: fix cephfs form validator (`pr#53778 <https://github.com/ceph/ceph/pull/53778>`_, Nizamudeen A)
* mgr/dashboard: fix cephfs forms validations (`pr#53831 <https://github.com/ceph/ceph/pull/53831>`_, Nizamudeen A)
* mgr/dashboard: fix image columns naming (`pr#53254 <https://github.com/ceph/ceph/pull/53254>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix progress bar color visibility (`pr#53209 <https://github.com/ceph/ceph/pull/53209>`_, Nizamudeen A)
* mgr/dashboard: fix prometheus queries subscriptions (`pr#53669 <https://github.com/ceph/ceph/pull/53669>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: fix rgw multi-site import form helper (`pr#54395 <https://github.com/ceph/ceph/pull/54395>`_, Aashish Sharma)
* mgr/dashboard: fix rgw multisite error when no rgw entity is present (`pr#54261 <https://github.com/ceph/ceph/pull/54261>`_, Aashish Sharma)
* mgr/dashboard: fix rgw page issues when hostname not resolvable (`pr#53214 <https://github.com/ceph/ceph/pull/53214>`_, Nizamudeen A)
* mgr/dashboard: fix rgw port manipulation error in dashboard (`pr#53392 <https://github.com/ceph/ceph/pull/53392>`_, Nizamudeen A)
* mgr/dashboard: fix the landing page layout issues (`issue#62961 <http://tracker.ceph.com/issues/62961>`_, `pr#53835 <https://github.com/ceph/ceph/pull/53835>`_, Nizamudeen A)
* mgr/dashboard: Fix user/bucket count in rgw overview dashboard (`pr#53818 <https://github.com/ceph/ceph/pull/53818>`_, Aashish Sharma)
* mgr/dashboard: fixed edit user quota form error (`pr#54223 <https://github.com/ceph/ceph/pull/54223>`_, Ivo Almeida)
* mgr/dashboard: images -> edit -> disable checkboxes for layering and deef-flatten (`pr#53388 <https://github.com/ceph/ceph/pull/53388>`_, avanthakkar)
* mgr/dashboard: minor usability improvements (`pr#53143 <https://github.com/ceph/ceph/pull/53143>`_, cloudbehl)
* mgr/dashboard: n/a entries behind primary snapshot mode (`pr#53223 <https://github.com/ceph/ceph/pull/53223>`_, Pere Diaz Bou)
* mgr/dashboard: Object gateway inventory card incorrect Buckets and user count (`pr#53382 <https://github.com/ceph/ceph/pull/53382>`_, Aashish Sharma)
* mgr/dashboard: Object gateway sync status cards keeps loading when multisite is not configured (`pr#53381 <https://github.com/ceph/ceph/pull/53381>`_, Aashish Sharma)
* mgr/dashboard: paginate hosts (`pr#52918 <https://github.com/ceph/ceph/pull/52918>`_, Pere Diaz Bou)
* mgr/dashboard: rbd image hide usage bar when disk usage is not provided (`pr#53810 <https://github.com/ceph/ceph/pull/53810>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: remove empty popover when there are no health warns (`pr#53652 <https://github.com/ceph/ceph/pull/53652>`_, Nizamudeen A)
* mgr/dashboard: remove green tick on old password field (`pr#53386 <https://github.com/ceph/ceph/pull/53386>`_, Nizamudeen A)
* mgr/dashboard: remove unnecessary failing hosts e2e (`pr#53458 <https://github.com/ceph/ceph/pull/53458>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: remove used and total used columns in favor of usage bar (`pr#53304 <https://github.com/ceph/ceph/pull/53304>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: replace sync progress bar with last synced timestamp in rgw multisite sync status card (`pr#53379 <https://github.com/ceph/ceph/pull/53379>`_, Aashish Sharma)
* mgr/dashboard: RGW Details card cleanup (`pr#53020 <https://github.com/ceph/ceph/pull/53020>`_, Nizamudeen A, cloudbehl)
* mgr/dashboard: Rgw Multi-site naming improvements (`pr#53806 <https://github.com/ceph/ceph/pull/53806>`_, Aashish Sharma)
* mgr/dashboard: rgw multisite topology view shows blank table for multisite entities (`pr#53380 <https://github.com/ceph/ceph/pull/53380>`_, Aashish Sharma)
* mgr/dashboard: set CORS header for unauthorized access (`pr#53201 <https://github.com/ceph/ceph/pull/53201>`_, Nizamudeen A)
* mgr/dashboard: show a message to restart the rgw daemons after moving from single-site to multi-site (`pr#53805 <https://github.com/ceph/ceph/pull/53805>`_, Aashish Sharma)
* mgr/dashboard: subvolume rm with snapshots (`pr#53233 <https://github.com/ceph/ceph/pull/53233>`_, Pedro Gonzalez Gomez)
* mgr/dashboard: update rgw multisite import form helper info (`pr#54253 <https://github.com/ceph/ceph/pull/54253>`_, Aashish Sharma)
* mgr/dashboard: upgrade angular v14 and v15 (`pr#52662 <https://github.com/ceph/ceph/pull/52662>`_, Nizamudeen A)
* mgr/rbd_support: fix recursive locking on CreateSnapshotRequests lock (`pr#54289 <https://github.com/ceph/ceph/pull/54289>`_, Ramana Raja)
* mgr/snap_schedule: allow retention spec 'n' to be user defined (`pr#52748 <https://github.com/ceph/ceph/pull/52748>`_, Milind Changire, Jakob Haufe)
* mgr/snap_schedule: make fs argument mandatory if more than one filesystem exists (`pr#54094 <https://github.com/ceph/ceph/pull/54094>`_, Milind Changire)
* mgr/volumes: Fix pending_subvolume_deletions in volume info (`pr#53572 <https://github.com/ceph/ceph/pull/53572>`_, Kotresh HR)
* mgr: register OSDs in ms_handle_accept (`pr#53187 <https://github.com/ceph/ceph/pull/53187>`_, Patrick Donnelly)
* mon, qa: issue pool application warning even if pool is empty (`pr#53041 <https://github.com/ceph/ceph/pull/53041>`_, Prashant D)
* mon/ConfigMonitor: update crush_location from osd entity (`pr#52466 <https://github.com/ceph/ceph/pull/52466>`_, Didier Gazen)
* mon/MDSMonitor: plug paxos when maybe manipulating osdmap (`pr#52246 <https://github.com/ceph/ceph/pull/52246>`_, Patrick Donnelly)
* mon/MonClient: resurrect original client_mount_timeout handling (`pr#52535 <https://github.com/ceph/ceph/pull/52535>`_, Ilya Dryomov)
* mon/OSDMonitor: do not propose on error in prepare_update (`pr#53186 <https://github.com/ceph/ceph/pull/53186>`_, Patrick Donnelly)
* mon: fix iterator mishandling in PGMap::apply_incremental (`pr#52554 <https://github.com/ceph/ceph/pull/52554>`_, Oliver Schmidt)
* msgr: AsyncMessenger add faulted connections metrics (`pr#53033 <https://github.com/ceph/ceph/pull/53033>`_, Pere Diaz Bou)
* os/bluestore: don't require bluestore_db_block_size when attaching new (`pr#52942 <https://github.com/ceph/ceph/pull/52942>`_, Igor Fedotov)
* os/bluestore: get rid off resulting lba alignment in allocators (`pr#54772 <https://github.com/ceph/ceph/pull/54772>`_, Igor Fedotov)
* osd/OpRequest: Add detail description for delayed op in osd log file (`pr#53688 <https://github.com/ceph/ceph/pull/53688>`_, Yite Gu)
* osd/OSDMap: Check for uneven weights & != 2 buckets post stretch mode (`pr#52457 <https://github.com/ceph/ceph/pull/52457>`_, Kamoltat)
* osd/scheduler/mClockScheduler: Use same profile and client ids for all clients to ensure allocated QoS limit consumption (`pr#53093 <https://github.com/ceph/ceph/pull/53093>`_, Sridhar Seshasayee)
* osd: fix logic in check_pg_upmaps (`pr#54276 <https://github.com/ceph/ceph/pull/54276>`_, Laura Flores)
* osd: fix read balancer logic to avoid redundant primary assignment (`pr#53820 <https://github.com/ceph/ceph/pull/53820>`_, Laura Flores)
* osd: fix use-after-move in build_incremental_map_msg() (`pr#54267 <https://github.com/ceph/ceph/pull/54267>`_, Ronen Friedman)
* osd: fix: slow scheduling when item_cost is large (`pr#53861 <https://github.com/ceph/ceph/pull/53861>`_, Jrchyang Yu)
* Overview graph improvements (`pr#53090 <https://github.com/ceph/ceph/pull/53090>`_, cloudbehl)
* pybind/mgr/devicehealth: do not crash if db not ready (`pr#52213 <https://github.com/ceph/ceph/pull/52213>`_, Patrick Donnelly)
* pybind/mgr/pg_autoscaler: Cut back osdmap.get_pools calls (`pr#52767 <https://github.com/ceph/ceph/pull/52767>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: fix warn when not too few pgs (`pr#53674 <https://github.com/ceph/ceph/pull/53674>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: noautoscale flag retains individual pool configs (`pr#53658 <https://github.com/ceph/ceph/pull/53658>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: Reorderd if statement for the func: _maybe_adjust (`pr#53429 <https://github.com/ceph/ceph/pull/53429>`_, Kamoltat)
* pybind/mgr/pg_autoscaler: Use bytes_used for actual_raw_used (`pr#53534 <https://github.com/ceph/ceph/pull/53534>`_, Kamoltat)
* pybind/mgr/volumes: log mutex locks to help debug deadlocks (`pr#53918 <https://github.com/ceph/ceph/pull/53918>`_, Kotresh HR)
* pybind/mgr: reopen database handle on blocklist (`pr#52460 <https://github.com/ceph/ceph/pull/52460>`_, Patrick Donnelly)
* pybind/rbd: don't produce info on errors in aio_mirror_image_get_info() (`pr#54055 <https://github.com/ceph/ceph/pull/54055>`_, Ilya Dryomov)
* python-common/drive_group: handle fields outside of 'spec' even when 'spec' is provided (`pr#53115 <https://github.com/ceph/ceph/pull/53115>`_, Adam King)
* python-common/drive_selection: lower log level of limit policy message (`pr#53114 <https://github.com/ceph/ceph/pull/53114>`_, Adam King)
* python-common: drive_selection: fix KeyError when osdspec_affinity is not set (`pr#53159 <https://github.com/ceph/ceph/pull/53159>`_, Guillaume Abrioux)
* qa/cephfs: fix build failure for mdtest project (`pr#53827 <https://github.com/ceph/ceph/pull/53827>`_, Rishabh Dave)
* qa/cephfs: fix ior project build failure (`pr#53825 <https://github.com/ceph/ceph/pull/53825>`_, Rishabh Dave)
* qa/cephfs: switch to python3 for centos stream 9 (`pr#53624 <https://github.com/ceph/ceph/pull/53624>`_, Xiubo Li)
* qa/rgw: add new POOL_APP_NOT_ENABLED failures to log-ignorelist (`pr#53896 <https://github.com/ceph/ceph/pull/53896>`_, Casey Bodley)
* qa/smoke,orch,perf-basic: add POOL_APP_NOT_ENABLED to ignorelist (`pr#54376 <https://github.com/ceph/ceph/pull/54376>`_, Prashant D)
* qa/standalone/osd/divergent-prior.sh: Divergent test 3 with pg_autoscale_mode on pick divergent osd (`pr#52721 <https://github.com/ceph/ceph/pull/52721>`_, Nitzan Mordechai)
* qa/suites/crimson-rados: add centos9 to supported distros (`pr#54020 <https://github.com/ceph/ceph/pull/54020>`_, Matan Breizman)
* qa/suites/crimson-rados: bring backfill testing (`pr#54021 <https://github.com/ceph/ceph/pull/54021>`_, Radoslaw Zarzynski, Matan Breizman)
* qa/suites/crimson-rados: Use centos8 for testing (`pr#54019 <https://github.com/ceph/ceph/pull/54019>`_, Matan Breizman)
* qa/suites/krbd: stress test for recovering from watch errors (`pr#53786 <https://github.com/ceph/ceph/pull/53786>`_, Ilya Dryomov)
* qa/suites/rbd: add test to check rbd_support module recovery (`pr#54291 <https://github.com/ceph/ceph/pull/54291>`_, Ramana Raja)
* qa/suites/rbd: drop cache tiering workload tests (`pr#53996 <https://github.com/ceph/ceph/pull/53996>`_, Ilya Dryomov)
* qa/suites/upgrade: enable default RBD image features (`pr#53352 <https://github.com/ceph/ceph/pull/53352>`_, Ilya Dryomov)
* qa/suites/upgrade: fix env indentation in stress-split upgrade tests (`pr#53921 <https://github.com/ceph/ceph/pull/53921>`_, Laura Flores)
* qa/suites/{rbd,krbd}: disable POOL_APP_NOT_ENABLED health check (`pr#53599 <https://github.com/ceph/ceph/pull/53599>`_, Ilya Dryomov)
* qa/tests: added - \(POOL_APP_NOT_ENABLED\) to the ignore list (`pr#54436 <https://github.com/ceph/ceph/pull/54436>`_, Yuri Weinstein)
* qa: add POOL_APP_NOT_ENABLED to ignorelist for cephfs tests (`issue#62482 <http://tracker.ceph.com/issues/62482>`_, `issue#62508 <http://tracker.ceph.com/issues/62508>`_, `pr#54380 <https://github.com/ceph/ceph/pull/54380>`_, Venky Shankar, Patrick Donnelly)
* qa: assign file system affinity for replaced MDS (`issue#61764 <http://tracker.ceph.com/issues/61764>`_, `pr#54037 <https://github.com/ceph/ceph/pull/54037>`_, Venky Shankar)
* qa: descrease pgbench scale factor to 32 for postgresql database test (`pr#53627 <https://github.com/ceph/ceph/pull/53627>`_, Xiubo Li)
* qa: fix cephfs-mirror unwinding and 'fs volume create/rm' order (`pr#52656 <https://github.com/ceph/ceph/pull/52656>`_, Jos Collin)
* qa: fix keystone in rgw/crypt/barbican.yaml (`pr#53412 <https://github.com/ceph/ceph/pull/53412>`_, Ali Maredia)
* qa: ignore expected cluster warning from damage tests (`pr#53484 <https://github.com/ceph/ceph/pull/53484>`_, Patrick Donnelly)
* qa: lengthen shutdown timeout for thrashed MDS (`pr#53553 <https://github.com/ceph/ceph/pull/53553>`_, Patrick Donnelly)
* qa: move nfs (mgr/nfs) related tests to fs suite (`pr#53906 <https://github.com/ceph/ceph/pull/53906>`_, Dhairya Parmar, Venky Shankar)
* qa: wait for file to have correct size (`pr#52742 <https://github.com/ceph/ceph/pull/52742>`_, Patrick Donnelly)
* qa: wait for MDSMonitor tick to replace daemons (`pr#52235 <https://github.com/ceph/ceph/pull/52235>`_, Patrick Donnelly)
* RadosGW API: incorrect bucket quota in response to HEAD /{bucket}/?usage (`pr#53437 <https://github.com/ceph/ceph/pull/53437>`_, shreyanshjain7174)
* rbd-mirror: fix image replayer shut down description on force promote (`pr#52880 <https://github.com/ceph/ceph/pull/52880>`_, Prasanna Kumar Kalever)
* rbd-mirror: fix race preventing local image deletion (`pr#52627 <https://github.com/ceph/ceph/pull/52627>`_, N Balachandran)
* rbd-nbd: fix stuck with disable request (`pr#54254 <https://github.com/ceph/ceph/pull/54254>`_, Prasanna Kumar Kalever)
* read balancer documentation (`pr#52777 <https://github.com/ceph/ceph/pull/52777>`_, Laura Flores)
* Rgw overview dashboard backport (`pr#53065 <https://github.com/ceph/ceph/pull/53065>`_, Aashish Sharma)
* rgw/amqp: remove possible race conditions with the amqp connections (`pr#53516 <https://github.com/ceph/ceph/pull/53516>`_, Yuval Lifshitz)
* rgw/amqp: skip idleness tests since it needs to sleep longer than 30s (`pr#53506 <https://github.com/ceph/ceph/pull/53506>`_, Yuval Lifshitz)
* rgw/crypt: apply rgw_crypt_default_encryption_key by default (`pr#52796 <https://github.com/ceph/ceph/pull/52796>`_, Casey Bodley)
* rgw/crypt: don't deref null manifest_bl (`pr#53590 <https://github.com/ceph/ceph/pull/53590>`_, Casey Bodley)
* rgw/kafka: failed to reconnect to broker after idle timeout (`pr#53513 <https://github.com/ceph/ceph/pull/53513>`_, Yuval Lifshitz)
* rgw/kafka: make sure that destroy is called after connection is removed (`pr#53515 <https://github.com/ceph/ceph/pull/53515>`_, Yuval Lifshitz)
* rgw/keystone: EC2Engine uses reject() for ERR_SIGNATURE_NO_MATCH (`pr#53762 <https://github.com/ceph/ceph/pull/53762>`_, Casey Bodley)
* rgw/multisite[archive zone]: fix storing of bucket instance info in the new bucket entrypoint (`pr#53466 <https://github.com/ceph/ceph/pull/53466>`_, Shilpa Jagannath)
* rgw/notification: pass in bytes_transferred to populate object_size in sync notification (`pr#53377 <https://github.com/ceph/ceph/pull/53377>`_, Juan Zhu)
* rgw/notification: remove non x-amz-meta-\* attributes from bucket notifications (`pr#53375 <https://github.com/ceph/ceph/pull/53375>`_, Juan Zhu)
* rgw/notifications: allow cross tenant notification management (`pr#53510 <https://github.com/ceph/ceph/pull/53510>`_, Yuval Lifshitz)
* rgw/s3: ListObjectsV2 returns correct object owners (`pr#54161 <https://github.com/ceph/ceph/pull/54161>`_, Casey Bodley)
* rgw/s3select: fix per QE defect (`pr#54163 <https://github.com/ceph/ceph/pull/54163>`_, galsalomon66)
* rgw/s3select: s3select fixes related to Trino/TPCDS benchmark and QE tests (`pr#53034 <https://github.com/ceph/ceph/pull/53034>`_, galsalomon66)
* rgw/sal: get_placement_target_names() returns void (`pr#53584 <https://github.com/ceph/ceph/pull/53584>`_, Casey Bodley)
* rgw/sync-policy: Correct "sync status" & "sync group" commands (`pr#53395 <https://github.com/ceph/ceph/pull/53395>`_, Soumya Koduri)
* rgw/upgrade: point upgrade suites to ragweed ceph-reef branch (`pr#53797 <https://github.com/ceph/ceph/pull/53797>`_, Shilpa Jagannath)
* RGW: add admin interfaces to get and delete notifications by bucket (`pr#53509 <https://github.com/ceph/ceph/pull/53509>`_, Ali Masarwa)
* rgw: add radosgw-admin bucket check olh/unlinked commands (`pr#53823 <https://github.com/ceph/ceph/pull/53823>`_, Cory Snyder)
* rgw: add versioning info to radosgw-admin bucket stats output (`pr#54191 <https://github.com/ceph/ceph/pull/54191>`_, Cory Snyder)
* RGW: bucket notification - hide auto generated topics when listing topics (`pr#53507 <https://github.com/ceph/ceph/pull/53507>`_, Ali Masarwa)
* rgw: don't dereference nullopt in DeleteMultiObj (`pr#54124 <https://github.com/ceph/ceph/pull/54124>`_, Casey Bodley)
* rgw: fetch_remote_obj() preserves original part lengths for BlockDecrypt (`pr#52816 <https://github.com/ceph/ceph/pull/52816>`_, Casey Bodley)
* rgw: fetch_remote_obj() uses uncompressed size for encrypted objects (`pr#54371 <https://github.com/ceph/ceph/pull/54371>`_, Casey Bodley)
* rgw: fix 2 null versionID after convert_plain_entry_to_versioned (`pr#53398 <https://github.com/ceph/ceph/pull/53398>`_, rui ma, zhuo li)
* rgw: fix multipart upload object leaks due to re-upload (`pr#52615 <https://github.com/ceph/ceph/pull/52615>`_, J. Eric Ivancich)
* rgw: fix rgw rate limiting RGWRateLimitInfo class decode_json max_rea… (`pr#53765 <https://github.com/ceph/ceph/pull/53765>`_, xiangrui meng)
* rgw: fix SignatureDoesNotMatch when extra headers start with 'x-amz' (`pr#53770 <https://github.com/ceph/ceph/pull/53770>`_, rui ma)
* rgw: fix unwatch crash at radosgw startup (`pr#53760 <https://github.com/ceph/ceph/pull/53760>`_, lichaochao)
* rgw: handle http options CORS with v4 auth (`pr#53413 <https://github.com/ceph/ceph/pull/53413>`_, Tobias Urdin)
* rgw: improve buffer list utilization in the chunkupload scenario (`pr#53773 <https://github.com/ceph/ceph/pull/53773>`_, liubingrun)
* rgw: pick http_date in case of http_x_amz_date absence (`pr#53440 <https://github.com/ceph/ceph/pull/53440>`_, Seena Fallah, Mohamed Awnallah)
* rgw: retry metadata cache notifications with INVALIDATE_OBJ (`pr#52798 <https://github.com/ceph/ceph/pull/52798>`_, Casey Bodley)
* rgw: s3 object lock avoids overflow in retention date (`pr#52604 <https://github.com/ceph/ceph/pull/52604>`_, Casey Bodley)
* rgw: s3website doesn't prefetch for web_dir() check (`pr#53767 <https://github.com/ceph/ceph/pull/53767>`_, Casey Bodley)
* RGW: Solving the issue of not populating etag in Multipart upload result (`pr#51447 <https://github.com/ceph/ceph/pull/51447>`_, Ali Masarwa)
* RGW:notifications: persistent topics are not deleted via radosgw-admin (`pr#53514 <https://github.com/ceph/ceph/pull/53514>`_, Ali Masarwa)
* src/mon/Monitor: Fix set_elector_disallowed_leaders (`pr#54003 <https://github.com/ceph/ceph/pull/54003>`_, Kamoltat)
* test/crimson/seastore/rbm: add sub-tests regarding RBM to the existing tests (`pr#53967 <https://github.com/ceph/ceph/pull/53967>`_, Myoungwon Oh)
* test/TestOSDMap: don't use the deprecated std::random_shuffle method (`pr#52737 <https://github.com/ceph/ceph/pull/52737>`_, Leonid Usov)
* valgrind: UninitCondition under __run_exit_handlers suppression (`pr#53681 <https://github.com/ceph/ceph/pull/53681>`_, Mark Kogan)
* xfstests_dev: install extra packages from powertools repo for xfsprogs (`pr#52843 <https://github.com/ceph/ceph/pull/52843>`_, Xiubo Li)

v18.2.0 Reef
============

This is the first stable release of Ceph Reef.

.. important::

   We are unable to build Ceph on Debian stable (bookworm) for the 18.2.0
   release because of Debian bug
   https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1030129. We will build as
   soon as this bug is resolved in Debian stable.

   *last updated 2023 Aug 04*

Release Date
------------

August 7, 2023

Major Changes from Quincy
--------------------------

Highlights
~~~~~~~~~~

See the relevant sections below for more details on these changes.

* **RADOS** FileStore is not supported in Reef.
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

* FileStore is not supported in Reef.
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
